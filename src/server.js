'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const WebSocket = require('ws');
const {graphCycles, simulate, topBooks} = require('./math');

const number = (key, fallback, min, max) => {
  const val = Number(process.env[key]);
  return Number.isFinite(val) && val >= min && val <= max ? val : fallback;
};
const cfg = {
  port: number('PORT',3000,1,65535),
  capital: number('CAPITAL_USDT',50,5,5000),
  fee: number('TAKER_FEE',0.001,0,0.02),
  maxAssets: Math.round(number('MAX_ASSETS',60,5,150)),
  minVolume: number('MIN_USDT_VOLUME',1000000,0,1e12),
  previewUsd: number('MIN_PREVIEW_USD',0.04,0,10),
  recheckUsd: number('MIN_RECHECK_USD',0.02,0,10),
  delay: number('RECHECK_DELAY_MS',800,100,5000),
  maxVerify: Math.round(number('MAX_VERIFY_PER_MIN',6,1,20)),
  dashboardToken: process.env.DASH_TOKEN || '',
  rest: process.env.BINANCE_REST || 'https://data-api.binance.vision',
  ws: process.env.BINANCE_WS || 'wss://data-stream.binance.vision/ws'
};
const log = (...args) => console.log(new Date().toISOString(), ...args);
const started=Date.now();
const state = {
  phase:'initializing', connected:false, lastMessageAt:0, lastError:'', symbols:0,
  routes:0, observed:0, scans:0, previews:0, verifications:0,
  qualified:0, blockUntil:0, lastVerificationAt:0, lastBootstrapAt:0,
  events:[], best:null, wsReconnects:0
};
const symbolMap = new Map();
const ticks = new Map();
let routes=[], socket=null, inProgress=false, validationTimes=[], activeRoute=null;
const routeCooldown = new Map();
const eventPath = path.join(__dirname, '..', 'data', 'events.jsonl');
fs.mkdirSync(path.dirname(eventPath),{recursive:true});
if (fs.existsSync(eventPath)) {
  try {
    const lines=fs.readFileSync(eventPath,'utf8').trim().split('\n').slice(-11000);
    state.events=lines.filter(Boolean).map(s=>JSON.parse(s)).reverse().slice(0,10000);
    for(const e of state.events) if(e.outcome==='qualified') state.qualified++;
  } catch(e) { log('Previous event log unreadable:',e.message); }
}
function addEvent(evt){
  state.events.unshift(evt);
  state.events=state.events.slice(0,10000);
  try {fs.appendFileSync(eventPath,JSON.stringify(evt)+'\n');} catch(e){log('Unable to append event',e.message);}
}
async function json(pathname, timeout=8500) {
  if(Date.now()<state.blockUntil) throw new Error('Binance REST backoff active');
  const ctl=new AbortController(); const t=setTimeout(()=>ctl.abort(),timeout);
  try {
    const r=await fetch(cfg.rest+pathname,{signal:ctl.signal,headers:{'User-Agent':'BinanceTriangleLab/1.0'}});
    if(r.status===418 || r.status===429) {
      const retry=Number(r.headers.get('retry-after'));
      state.blockUntil=Date.now()+Math.max(60000,Number.isFinite(retry)?retry*1000:60000);
    }
    if(!r.ok) throw new Error(`Binance REST HTTP ${r.status} ${pathname}`);
    return await r.json();
  } finally {clearTimeout(t);}
}
function tradable(s){
  if(s.status!=='TRADING' || s.isSpotTradingAllowed===false) return false;
  if(Array.isArray(s.orderTypes) && !s.orderTypes.includes('MARKET')) return false;
  if(Array.isArray(s.permissions) && s.permissions.length && !s.permissions.includes('SPOT')) return false;
  return true;
}
async function bootstrap(){
  state.phase='loading Binance metadata';
  const [metadata,volumes]=await Promise.all([
    json('/api/v3/exchangeInfo',12000),
    json('/api/v3/ticker/24hr',12000)
  ]);
  const all=metadata.symbols.filter(tradable);
  const usdtSymbols=new Map(all.filter(s=>s.quoteAsset==='USDT').map(s=>[s.symbol,s]));
  const volumeMap=new Map(volumes.map(t=>[t.symbol,Number(t.quoteVolume)||0]));
  const top=all.filter(s=>s.quoteAsset==='USDT' && !/(UP|DOWN|BULL|BEAR)$/.test(s.baseAsset))
    .filter(s=>(volumeMap.get(s.symbol)||0)>=cfg.minVolume)
    .sort((a,b)=>(volumeMap.get(b.symbol)||0)-(volumeMap.get(a.symbol)||0))
    .slice(0,cfg.maxAssets).map(s=>s.baseAsset);
  const assets=new Set(['USDT','BTC','ETH','BNB',...top]);
  const included=all.filter(s=>assets.has(s.baseAsset)&&assets.has(s.quoteAsset));
  const cycles=graphCycles(included,assets);
  if(!cycles.length) throw new Error('No eligible USDT triangles in current spot markets');
  routes=cycles;
  symbolMap.clear();
  const used=new Set(cycles.flatMap(c=>c.edges.map(e=>e.symbol)));
  for(const s of included) if(used.has(s.symbol)) symbolMap.set(s.symbol,s);
  ticks.clear();
  state.symbols=used.size;state.routes=cycles.length;state.lastBootstrapAt=Date.now();
  state.phase='connecting WebSocket';
  log(`Metadata: ${assets.size} assets, ${used.size} symbols, ${cycles.length} directed cycles`);
  return [...used];
}
function connect(symbolNames){
  if(socket) try{socket.terminate();}catch{}
  const sock=new WebSocket(cfg.ws,{perMessageDeflate:false,handshakeTimeout:12000});socket=sock;
  state.connected=false;
  sock.on('open',()=>{
    state.connected=true; state.phase='collecting live prices';state.lastError='';
    // Binance allows 1024 subscriptions per connection; batch to keep messages short.
    let id=1;
    for(let i=0;i<symbolNames.length;i+=90)
      sock.send(JSON.stringify({method:'SUBSCRIBE',params:symbolNames.slice(i,i+90).map(s=>s.toLowerCase()+'@bookTicker'),id:id++}));
    log('WebSocket subscribed to',symbolNames.length,'bookTicker streams');
  });
  sock.on('message',buf=>{
    let t;try{t=JSON.parse(buf.toString());}catch{return;}
    t=t.data||t;
    if(!t.s || !symbolMap.has(t.s)) return;
    const bid=Number(t.b),ask=Number(t.a),bidQty=Number(t.B),askQty=Number(t.A);
    if(!(bid>0&&ask>0&&bidQty>0&&askQty>0&&bid<=ask)) return;
    ticks.set(t.s,{bid,ask,bidQty,askQty,at:Date.now()});
    state.lastMessageAt=Date.now();state.observed=ticks.size;
    if(state.phase==='collecting live prices' && ticks.size>20) state.phase='scanning';
  });
  sock.on('error',err=>{state.lastError='WebSocket: '+err.message;log(state.lastError);});
  sock.on('close',()=>{if(socket!==sock)return;state.connected=false;state.phase='reconnecting';state.wsReconnects++;setTimeout(()=>connect(symbolNames),5000);});
}
function makeCandidate(){
  let best=null;
  const now=Date.now();
  for(const cycle of routes){
    if((routeCooldown.get(cycle.id)||0)>now) continue;
    const books=topBooks(cycle,ticks,2500,now);
    if(!books)continue;
    state.scans++;
    const result=simulate(cycle,books,symbolMap,cfg.capital,cfg.fee);
    if(result.ok && result.netUsd>=cfg.previewUsd && (!best || result.netUsd>best.preview.netUsd))
      best={cycle,preview:result};
  }
  return best;
}
async function snapshot(cycle){
  const began=Date.now();
  const symbols=[...new Set(cycle.edges.map(e=>e.symbol))];
  const raw=await Promise.all(symbols.map(s=>json('/api/v3/depth?symbol='+encodeURIComponent(s)+'&limit=20',7000)));
  const books=new Map(raw.map((b,i)=>[symbols[i],{bids:b.bids,asks:b.asks}]));
  return { result:simulate(cycle,books,symbolMap,cfg.capital,cfg.fee), elapsedMs:Date.now()-began };
}
async function verify(candidate){
  const cycle=candidate.cycle, first=Date.now();
  routeCooldown.set(cycle.id, first+30000);state.verifications++;
  let outcome='rejected', reason='',depthUsd=null,recheckUsd=null,latencyMs=0;
  try {
    const a=await snapshot(cycle);latencyMs+=a.elapsedMs;
    if(!a.result.ok){reason=a.result.reason;}
    else {
      depthUsd=a.result.netUsd;
      if(depthUsd < cfg.recheckUsd){reason='depth_no_longer_profitable';}
      else {
        await new Promise(resolve=>setTimeout(resolve,cfg.delay));
        const b=await snapshot(cycle);latencyMs+=b.elapsedMs;
        if(!b.result.ok){reason='recheck_'+b.result.reason;}
        else {recheckUsd=b.result.netUsd;
          if(b.result.netUsd >= cfg.recheckUsd){outcome='qualified';state.qualified++;}
          else reason='recheck_no_longer_profitable';
        }
      }
    }
  } catch(e){reason=e.message;state.lastError=e.message;}
  const event={at:new Date().toISOString(),route:cycle.label,previewUsd:+candidate.preview.netUsd.toFixed(5),
    depthUsd:depthUsd===null?null:+depthUsd.toFixed(5),
    recheckUsd:recheckUsd===null?null:+recheckUsd.toFixed(5),
    elapsedMs:Date.now()-first,networkMs:latencyMs,outcome,reason};
  state.lastVerificationAt=Date.now();addEvent(event);
  if(outcome==='qualified') {state.best=(!state.best || recheckUsd>state.best.recheckUsd)?event:state.best;log('Qualified theoretical opportunity:',event.route,event.recheckUsd);}
}
async function scan(){
  if(inProgress || !state.connected || !routes.length || Date.now()<state.blockUntil) return;
  if(Date.now()-state.lastMessageAt>20000)return;
  state.previews++;
  const candidate=makeCandidate();
  if(!candidate)return;
  validationTimes=validationTimes.filter(t=>Date.now()-t<60000);
  if(validationTimes.length>=cfg.maxVerify || Date.now()<(routeCooldown.get(candidate.cycle.id)||0))return;
  validationTimes.push(Date.now()); inProgress=true;
  try{await verify(candidate);}finally{inProgress=false;}
}
function snapshotStatus(){
  const lastDay=Date.now()-86400000;
  const daily=state.events.filter(e=>Date.parse(e.at)>=lastDay);
  return {phase:state.phase,connected:state.connected,uptimeSeconds:Math.round((Date.now()-started)/1000),
    lastMessageAgeMs:state.lastMessageAt?Date.now()-state.lastMessageAt:null,
    lastError:state.lastError,blockedForMs:Math.max(0,state.blockUntil-Date.now()),
    symbols:state.symbols,routes:state.routes,observed:state.observed,scans:state.scans,
    verifications:state.verifications,qualifiedRecorded:state.qualified,
    last24h:{inspected:daily.length,qualified:daily.filter(x=>x.outcome==='qualified').length},
    best:state.best,events:state.events.slice(0,30),
    assumptions:{capital:cfg.capital,takerFeePercent:cfg.fee*100,previewThresholdUsd:cfg.previewUsd,recheckThresholdUsd:cfg.recheckUsd,
      recheckDelayMs:cfg.delay,maxVerifyPerMinute:cfg.maxVerify},
    warning:'SIMULATION ONLY: quotes are not atomic; no executed fills, no guaranteed profits.'};
}
const publicHtml=fs.readFileSync(path.join(__dirname,'..','public','index.html'));
const server=http.createServer((req,res)=>{
  const pathname=new URL(req.url,'http://localhost').pathname;
  const send=(code,body,type='application/json; charset=utf-8')=>{res.writeHead(code,{'Content-Type':type,'Cache-Control':'no-store','X-Content-Type-Options':'nosniff'});res.end(body);};
  if(req.method!=='GET')return send(405,JSON.stringify({error:'Method not allowed'}));
  if(pathname==='/healthz')return send(200,JSON.stringify({alive:true,phase:state.phase}));
  if(pathname==='/' || pathname==='/index.html')return send(200,publicHtml,'text/html; charset=utf-8');
  if(pathname==='/api/status' || pathname==='/api/export'){
    if(cfg.dashboardToken && req.headers.authorization!=='Bearer '+cfg.dashboardToken)
      return send(401,JSON.stringify({error:'Unauthorized'}));
    if(pathname==='/api/export'){
      res.writeHead(200,{'Content-Type':'application/x-ndjson; charset=utf-8','Content-Disposition':'attachment; filename=triangle-events.jsonl','Cache-Control':'no-store'});
      return fs.createReadStream(eventPath).pipe(res);
    }
    return send(200,JSON.stringify(snapshotStatus()));
  }
  return send(404,JSON.stringify({error:'Not found'}));
});
server.listen(cfg.port,'0.0.0.0',()=>log('Dashboard listening on port',cfg.port));
async function start(){
  try {const symbols=await bootstrap();connect(symbols);}
  catch(e){state.lastError=e.message;state.phase='initialization error';log(e.message);setTimeout(start,30000);}
}
start();
setInterval(scan,700);
setInterval(()=>{
  if(socket && state.connected && Date.now()-state.lastMessageAt>90000) {log('WS feed stale, reconnecting');socket.terminate();}
},15000);
setInterval(()=>{
  if(state.lastBootstrapAt && Date.now()-state.lastBootstrapAt>6*3600000){
    // Rebuild universe for symbol changes without overlapping existing socket.
    start();
  }
},60000);
process.on('SIGTERM',()=>{if(socket)socket.terminate();server.close(()=>process.exit(0));});
