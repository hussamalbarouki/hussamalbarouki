'use strict';
const http=require('node:http'),fs=require('node:fs'),path=require('node:path');
const crypto=require('node:crypto');
const {Store}=require('./store'),{FIELDS,defaults,validate}=require('./settings');
const {graphCycles,preview,simulate}=require('./math');
const auth=require('./auth');
const started=Date.now(),port=Number(process.env.PORT)||3000,dir=process.env.DATA_DIR||path.join(__dirname,'..','data');
const store=new Store(dir);
let settings={...defaults(),...store.get('settings')};
let account=store.get('account');
let setupCode=account?null:crypto.randomBytes(9).toString('base64url');
let setupSecret=null;
if(setupCode)console.log('\n=== مراقبة حسام | FIRST RUN SETUP CODE ===\n'+setupCode+'\nUse only on your HTTPS website, do not share it.\n=======================================\n');
let state={phase:'starting',connected:false,lastError:'',lastMessageAt:0,blockedUntil:0,universeAt:0,wsReconnects:0,
 symbols:0,routes:0,observed:0,scans:0,previews:0,validations:0,qualified:0,near:null,best:store.bestQualified(),requests:0};
let meta=new Map(),cycles=[],ticks=new Map(),socket=null,retryTimer=null,buildGeneration=0;
let inValidation=false,lastScan=0,verificationTimes=[],routeCooldown=new Map(),bootstrapRunning=false;
const requestAttempts=new Map();
const listen=new Set();
const stamp=()=>new Date().toISOString();
const log=(...v)=>console.log(stamp(),...v);
function rateLimit(key,max=7,windowMs=15*60*1000){const now=Date.now(),v=(requestAttempts.get(key)||[]).filter(t=>t>now-windowMs);v.push(now);requestAttempts.set(key,v);return v.length<=max;}
function respond(res,code,data,contentType='application/json; charset=utf-8',other={}){res.writeHead(code,{'Content-Type':contentType,'Cache-Control':'no-store','X-Content-Type-Options':'nosniff',...other});res.end(typeof data==='string'||Buffer.isBuffer(data)?data:JSON.stringify(data));}
function deny(res,code,message){respond(res,code,{error:message});}
function parseCookies(req){const raw=req.headers.cookie||'';return Object.fromEntries(raw.split(';').map(x=>x.trim()).filter(Boolean).map(s=>{const n=s.indexOf('=');return [s.slice(0,n),s.slice(n+1)];}));}
function getSession(req){const token=parseCookies(req).hussam_session;if(!token||!/^[a-f0-9]{64}$/.test(token))return null;const session=store.session(auth.hashToken(token));return session?{...session,token}:null;}
function secureConnection(req){return Boolean(req.socket.encrypted)||req.headers['x-forwarded-proto']==='https';}
function originAllowed(req){const origin=req.headers.origin;if(!origin)return true;try{return new URL(origin).host===req.headers.host;}catch{return false;}}
async function body(req,maxBytes=30000){return new Promise((resolve,reject)=>{let chunks=[],length=0,done=false;req.on('data',x=>{length+=x.length;if(length>maxBytes){if(!done){done=true;reject(Error('الطلب كبير جداً'));}return;}if(!done)chunks.push(x);});req.on('end',()=>{if(done)return;try{resolve(JSON.parse(Buffer.concat(chunks).toString()||'{}'));}catch{reject(Error('تنسيق JSON غير صحيح'));}});req.on('error',reject);});}
function newSession(req,res){const token=auth.newToken(),csrf=auth.newToken();store.addSession(auth.hashToken(token),csrf,Date.now()+12*3600000);return {token,csrf,header:{'Set-Cookie':auth.cookie(token,secureConnection(req))}};}
async function rest(relative,timeout=7500){if(Date.now()<state.blockedUntil)throw Error('Binance API cooldown');state.requests++;const ctl=new AbortController(),timer=setTimeout(()=>ctl.abort(),timeout);try{const response=await fetch('https://data-api.binance.vision'+relative,{signal:ctl.signal,headers:{'User-Agent':'HussamMonitor/2.0'}});if(response.status===429||response.status===418){const retry=Number(response.headers.get('retry-after'));state.blockedUntil=Date.now()+Math.max(60000,Number.isFinite(retry)?retry*1000:60000);}if(!response.ok)throw Error(`Binance HTTP ${response.status} ${relative.split('?')[0]}`);return response.json();}finally{clearTimeout(timer);}}
function tradeable(s){return s.status==='TRADING'&&s.isSpotTradingAllowed!==false&&(!s.orderTypes||s.orderTypes.includes('MARKET'))&&(!s.permissions||!s.permissions.length||s.permissions.includes('SPOT'));}
function consumeTick(s,b,a,B,A){if(!meta.has(s))return;const bid=Number(b),ask=Number(a),bidQty=Number(B),askQty=Number(A);if(!(bid>0&&ask>0&&bidQty>0&&askQty>0&&bid<=ask))return;ticks.set(s,{bid,ask,bidQty,askQty,at:Date.now()});state.lastMessageAt=Date.now();state.observed=ticks.size;}
function connect(symbolNames,generation){if(generation!==buildGeneration)return;
 if(socket){try{socket.close();}catch{}}
 const ws=new WebSocket('wss://data-stream.binance.vision/ws');socket=ws;state.connected=false;state.phase='connecting';
 ws.addEventListener('open',()=>{if(socket!==ws)return;state.connected=true;state.phase=settings.paused?'paused':'monitoring';state.lastError='';let id=1;for(let i=0;i<symbolNames.length;i+=150){ws.send(JSON.stringify({method:'SUBSCRIBE',params:symbolNames.slice(i,i+150).map(s=>s.toLowerCase()+'@bookTicker'),id:id++}));}log('Binance WebSocket connected, pairs:',symbolNames.length);});
 ws.addEventListener('message',event=>{if(ws!==socket)return;let d;try{d=JSON.parse(event.data);d=d.data||d;}catch{return;}if(d.s)consumeTick(d.s,d.b,d.a,d.B,d.A);});
 ws.addEventListener('error',()=>{if(ws===socket)state.lastError='WebSocket connection error';});
 ws.addEventListener('close',()=>{if(socket!==ws||generation!==buildGeneration)return;state.connected=false;state.phase='reconnecting';state.wsReconnects++;clearTimeout(retryTimer);retryTimer=setTimeout(()=>connect(symbolNames,generation),6500);});
}
async function bootstrap(){if(bootstrapRunning)return;bootstrapRunning=true;const generation=++buildGeneration;try{
 state.phase='loading Binance markets';const [exchange,volumes]=await Promise.all([rest('/api/v3/exchangeInfo',14000),rest('/api/v3/ticker/24hr',14000)]);
 const rows=exchange.symbols.filter(tradeable),volume=new Map(volumes.map(x=>[x.symbol,Number(x.quoteVolume)||0]));
 const base=rows.filter(x=>x.quoteAsset==='USDT'&&!/(UP|DOWN|BULL|BEAR)$/.test(x.baseAsset)).filter(x=>(volume.get(x.symbol)||0)>=settings.minVolume).sort((a,b)=>(volume.get(b.symbol)||0)-(volume.get(a.symbol)||0)).slice(0,settings.maxAssets).map(x=>x.baseAsset);
 const assets=new Set(['USDT','BTC','ETH','BNB',...base]),included=rows.filter(x=>assets.has(x.baseAsset)&&assets.has(x.quoteAsset)),newCycles=graphCycles(included,assets),symbols=new Set(newCycles.flatMap(c=>c.edges.map(e=>e.symbol)));
 if(!newCycles.length)throw Error('No available USDT triangular paths');meta=new Map(included.filter(s=>symbols.has(s.symbol)).map(s=>[s.symbol,s]));cycles=newCycles;ticks.clear();routeCooldown.clear();state.routes=cycles.length;state.symbols=symbols.size;state.observed=0;state.universeAt=Date.now();state.phase='connecting';
 clearTimeout(retryTimer);connect([...symbols],generation);log('Universe ready',cycles.length,'directed triangles and',symbols.size,'pairs');
 }catch(e){state.lastError=e.message;state.phase='Binance connection error';log('Bootstrap:',e.message);setTimeout(()=>bootstrap(),30000);}finally{bootstrapRunning=false;}}
async function refreshTop(){if(!meta.size||Date.now()<state.blockedUntil)return;try{const arr=await rest('/api/v3/ticker/bookTicker',9000);for(const x of arr||[]){const old=ticks.get(x.symbol);if(!old||Date.now()-old.at>3000)consumeTick(x.symbol,x.bidPrice,x.askPrice,x.bidQty,x.askQty);}}catch(e){state.lastError=e.message;}}
function candidate(){const now=Date.now();let winner=null,near=null;for(const cycle of cycles){const p=preview(cycle,ticks,settings.capital,settings.fee,settings.bufferBps,settings.quoteMaxAgeMs,now);if(!p)continue;state.scans++;if(!near||p.netUsd>near.netUsd)near={route:cycle.label,...p,at:now};if(p.netUsd>=settings.previewThreshold&&now>(routeCooldown.get(cycle.id)||0)&&(!winner||p.netUsd>winner.preview.netUsd))winner={cycle,preview:p};}state.near=near;return winner;}
async function depthSnapshot(cycle,config,metadata){const startedAt=Date.now(),symbols=[...new Set(cycle.edges.map(e=>e.symbol))];const raw=await Promise.all(symbols.map(s=>rest('/api/v3/depth?symbol='+encodeURIComponent(s)+'&limit='+config.depthLevels,6500)));
 const elapsed=Date.now()-startedAt;if(elapsed>config.maxDepthRttMs)return {ok:false,reason:'depth_latency_exceeded',elapsed};
 const map=new Map(raw.map((v,i)=>[symbols[i],{asks:v.asks,bids:v.bids,updateId:v.lastUpdateId}]));const result=simulate(cycle,map,metadata,config.capital,config.fee,config.bufferBps,config.liquiditySharePct);return {...result,elapsed};}
async function verify(c){const when=Date.now(),set={...settings},metadata=new Map(meta);state.validations++;routeCooldown.set(c.cycle.id,when+set.routeCooldownSeconds*1000);let one=null,two=null,outcome='rejected',reason='';try{
  one=await depthSnapshot(c.cycle,set,metadata);if(!one.ok)reason=one.reason;
  else if(one.netUsd<set.minProfit)reason='first_below_threshold';
  else{await new Promise(r=>setTimeout(r,set.recheckDelayMs));two=await depthSnapshot(c.cycle,set,metadata);if(!two.ok)reason='second_'+two.reason;else if(two.netUsd<set.minProfit)reason='second_below_threshold';else {outcome='qualified';state.qualified++;}}
 }catch(e){reason=e.message;state.lastError=e.message;}
 const event={at:Date.now(),route:c.cycle.label,outcome,previewUsd:+c.preview.netUsd.toFixed(7),firstUsd:one?.ok?+one.netUsd.toFixed(7):null,secondUsd:two?.ok?+two.netUsd.toFixed(7):null,netPct:two?.ok?+two.netPct.toFixed(6):null,elapsedMs:Date.now()-when,reason,detail:{quoteAgeMs:c.preview.maxQuoteAgeMs,firstElapsedMs:one?.elapsed??null,secondElapsedMs:two?.elapsed??null,legs:two?.ok?two.legs:one?.ok?one.legs:[],settings:{capital:set.capital,fee:set.fee,bufferBps:set.bufferBps,liquiditySharePct:set.liquiditySharePct,depthLevels:set.depthLevels},note:'Market snapshots are not atomic and no orders were executed.'}};
 try{event.id=store.insert(event);}catch(e){state.lastError='Unable to save event: '+e.message;log(state.lastError);}
 if(outcome==='qualified'&&(!state.best||event.secondUsd>state.best.secondUsd))state.best=event;
 broadcast('event',{id:event.id,outcome:event.outcome,route:event.route,secondUsd:event.secondUsd,at:event.at});
}
async function scanning(){if(settings.paused||inValidation||!state.connected||!cycles.length||Date.now()<state.blockedUntil||Date.now()-lastScan<settings.scanIntervalMs)return;lastScan=Date.now();if(!state.lastMessageAt||Date.now()-state.lastMessageAt>20000)return;const c=candidate();if(!c)return;state.previews++;verificationTimes=verificationTimes.filter(t=>t>Date.now()-60000);if(verificationTimes.length>=settings.maxVerifyPerMinute)return;inValidation=true;verificationTimes.push(Date.now());try{await verify(c);}finally{inValidation=false;}}
function dashboard(){const d=store.summary();return {...state,uptimeSeconds:Math.floor((Date.now()-started)/1000),lastMessageAgeMs:state.lastMessageAt?Date.now()-state.lastMessageAt:null,cooldownMs:Math.max(0,state.blockedUntil-Date.now()),settings:{capital:settings.capital,fee:settings.fee,bufferBps:settings.bufferBps,liquiditySharePct:settings.liquiditySharePct,paused:settings.paused},summary:d,warning:'Read-only research signals; zero executed orders or realized profits.'};}
function broadcast(type,data){for(const client of listen)try{client.write('event: '+type+'\ndata: '+JSON.stringify(data)+'\n\n');}catch{listen.delete(client);}}
const mime={'.html':'text/html; charset=utf-8','.css':'text/css; charset=utf-8','.js':'application/javascript; charset=utf-8','.svg':'image/svg+xml'};
const publicDir=path.resolve(__dirname,'..','public');
const server=http.createServer(async(req,res)=>{res.setHeader('X-Frame-Options','DENY');res.setHeader('X-Content-Type-Options','nosniff');res.setHeader('Referrer-Policy','no-referrer');res.setHeader('Permissions-Policy','camera=(), microphone=(), geolocation=()');res.setHeader('Content-Security-Policy',"default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'");
 let pathname;try{pathname=new URL(req.url,'http://localhost').pathname;}catch{return deny(res,400,'Invalid URL');}
 if(req.method==='GET'&&pathname==='/healthz')return respond(res,200,{ok:true});
 if(req.method==='GET'&&(pathname==='/'||pathname==='/index.html'||pathname==='/app.js'||pathname==='/style.css')){const filename=pathname==='/'?'/index.html':pathname;const file=path.resolve(publicDir,'.'+filename);if(!file.startsWith(publicDir+path.sep))return deny(res,404,'Not found');try{return respond(res,200,fs.readFileSync(file),mime[path.extname(file)]);}catch{return deny(res,404,'Not found');}}
 if(!pathname.startsWith('/api/'))return deny(res,404,'Not found');
 if(req.method==='POST'&&!originAllowed(req))return deny(res,403,'Invalid request origin');
 if(req.method==='GET'&&pathname==='/api/setup/status')return respond(res,200,{configured:!!account,requiresSetupCode:!account});
 if(!account&&(pathname==='/api/setup/prepare'||pathname==='/api/setup/complete')&&req.method==='POST'){
  try{const data=await body(req);if(!rateLimit('setup:'+req.socket.remoteAddress,12))return deny(res,429,'محاولات كثيرة، حاول لاحقاً');if(typeof data.setupCode!=='string'||data.setupCode!==setupCode)return deny(res,401,'رمز الإعداد غير صحيح');
   if(pathname.endsWith('prepare')){setupSecret=auth.randomSecret();const uri=`otpauth://totp/${encodeURIComponent('مراقبة حسام:admin')}?secret=${setupSecret}&issuer=${encodeURIComponent('مراقبة حسام')}&algorithm=SHA1&digits=6&period=30`;return respond(res,200,{secret:setupSecret,otpauthUri:uri});}
   if(!setupSecret)return deny(res,400,'ابدأ إعداد المصادقة أولاً');const pw=String(data.password||'');if(pw.length<12||Buffer.byteLength(pw)>200)return deny(res,400,'كلمة المرور يجب أن تكون 12 إلى 200 رمز');const step=auth.checkTotp(setupSecret,data.code);if(step<0)return deny(res,400,'رمز التحقق غير صحيح. تأكد من ضبط وقت الهاتف');
   const rec={...auth.passwordHash(pw),totpSecret:setupSecret,lastTotpStep:step,createdAt:Date.now()};store.set('account',rec);account=rec;setupCode=null;setupSecret=null;const session=newSession(req,res);return respond(res,200,{ok:true,csrf:session.csrf},undefined,session.header);
  }catch(e){return deny(res,400,e.message);}
 }
 if(!account)return deny(res,403,'أكمل التهيئة الأولى');
 if(pathname==='/api/auth/login'&&req.method==='POST'){try{const data=await body(req);if(!rateLimit('login:'+req.socket.remoteAddress,8))return deny(res,429,'محاولات كثيرة، حاول بعد 15 دقيقة');if(!auth.passwordMatches(String(data.password||''),account))return deny(res,401,'بيانات الدخول غير صحيحة');const step=auth.checkTotp(account.totpSecret,data.code,account.lastTotpStep);if(step<0)return deny(res,401,'رمز التحقق غير صحيح أو سبق استخدامه');account.lastTotpStep=step;store.set('account',account);const s=newSession(req,res);return respond(res,200,{ok:true,csrf:s.csrf},undefined,s.header);}catch(e){return deny(res,400,e.message);}}
 const session=getSession(req);if(!session)return deny(res,401,'تسجيل الدخول مطلوب');
 if(req.method==='POST'&&req.headers['x-csrf-token']!==session.csrf)return deny(res,403,'طلب غير آمن: CSRF');
 if(pathname==='/api/auth/me'&&req.method==='GET')return respond(res,200,{authenticated:true,csrf:session.csrf});
 if(pathname==='/api/auth/logout'&&req.method==='POST'){store.revoke(auth.hashToken(session.token));return respond(res,200,{ok:true},undefined,{'Set-Cookie':'hussam_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0'});}
 if(pathname==='/api/auth/change-password'&&req.method==='POST'){try{const d=await body(req);if(!auth.passwordMatches(d.currentPassword||'',account))return deny(res,401,'كلمة المرور الحالية خاطئة');if(String(d.newPassword||'').length<12||String(d.newPassword).length>200)return deny(res,400,'كلمة المرور الجديدة يجب أن تكون 12 رمزاً على الأقل');const step=auth.checkTotp(account.totpSecret,d.code,account.lastTotpStep);if(step<0)return deny(res,401,'رمز المصادقة خاطئ أو مستخدم');account={...account,...auth.passwordHash(d.newPassword),lastTotpStep:step};store.set('account',account);store.revokeAll();return respond(res,200,{ok:true,needLogin:true},undefined,{'Set-Cookie':'hussam_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0'});}catch(e){return deny(res,400,e.message);}}
 if(pathname==='/api/status'&&req.method==='GET')return respond(res,200,dashboard());
 if(pathname==='/api/live'&&req.method==='GET'){
  if(listen.size>=10)return deny(res,429,'Too many live connections');
  res.writeHead(200,{'Content-Type':'text/event-stream; charset=utf-8','Cache-Control':'no-cache, no-transform','Connection':'keep-alive','X-Accel-Buffering':'no'});res.write('retry: 3000\n\n');listen.add(res);const every=setInterval(()=>{if(!store.session(auth.hashToken(session.token))){clearInterval(every);listen.delete(res);res.end();return;}try{res.write('event: status\ndata: '+JSON.stringify(dashboard())+'\n\n');}catch{clearInterval(every);listen.delete(res);}},1500);req.on('close',()=>{clearInterval(every);listen.delete(res);});return;
 }
 if(pathname==='/api/settings'&&req.method==='GET')return respond(res,200,{fields:FIELDS,values:settings});
 if(pathname==='/api/settings'&&req.method==='POST'){try{const next=validate(await body(req));const before=settings;const updated={...settings,...next};store.set('settings',updated);settings=updated;if(before.maxAssets!==updated.maxAssets||before.minVolume!==updated.minVolume){bootstrap().catch(e=>log(e.message));}state.phase=settings.paused?'paused':state.connected?'monitoring':state.phase;return respond(res,200,{ok:true,values:settings});}catch(e){return deny(res,400,e.message);}}
 if(pathname==='/api/events'&&req.method==='GET'){const url=new URL(req.url,'http://localhost'),limit=Math.max(1,Math.min(200,Number(url.searchParams.get('limit'))||50)),offset=Math.max(0,Math.min(100000,Number(url.searchParams.get('offset'))||0)),filter=url.searchParams.get('filter')||'all';if(!['all','qualified','rejected'].includes(filter))return deny(res,400,'فلتر غير صحيح');return respond(res,200,{events:store.events(limit,offset,filter),limit,offset,filter});}
 if(pathname==='/api/export'&&req.method==='GET'){const url=new URL(req.url,'http://localhost'),filter=url.searchParams.get('filter')||'all';if(!['all','qualified','rejected'].includes(filter))return deny(res,400,'فلتر غير صحيح');res.writeHead(200,{'Content-Type':'text/csv; charset=utf-8','Content-Disposition':'attachment; filename="hussam-opportunities.csv"','Cache-Control':'no-store','X-Content-Type-Options':'nosniff'});res.write('\uFEFFid,utc,route,status,preview_usdt,first_usdt,second_usdt,net_pct,elapsed_ms,reason\n');let offset=0;const cell=x=>'"'+String(x??'').replace(/"/g,'""').replace(/^[=+@\-\t\r]/,'\u0027$&')+'"';while(true){const rows=store.events(500,offset,filter);for(const e of rows)res.write([e.id,new Date(e.at).toISOString(),e.route,e.outcome,e.previewUsd,e.firstUsd,e.secondUsd,e.netPct,e.elapsedMs,e.reason].map(cell).join(',')+'\n');if(rows.length<500)break;offset+=rows.length;if(offset>=100000)break;}res.end();return;}
 return deny(res,404,'Not found');
});
server.listen(port,'0.0.0.0',()=>log('مراقبة حسام listening at port',port));
setInterval(()=>{scanning().catch(e=>{state.lastError=e.message;inValidation=false;});},200);
setInterval(()=>{refreshTop().catch(e=>log(e.message));},15000);
setInterval(()=>{if(!bootstrapRunning&&state.universeAt&&Date.now()-state.universeAt>6*3600000)bootstrap().catch(e=>log(e.message));},60000);
setInterval(()=>{if(state.connected&&Date.now()-state.lastMessageAt>90000){state.lastError='WebSocket feed stale, reconnecting';try{socket.close();}catch{}}store.purge(settings.retentionDays);},60000);
process.on('SIGTERM',()=>{clearTimeout(retryTimer);if(socket)socket.close();server.close(()=>{store.close();process.exit(0);});setTimeout(()=>process.exit(0),4000).unref();});
bootstrap().catch(e=>log(e.message));
