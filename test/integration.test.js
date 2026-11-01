'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path'),{spawn}=require('node:child_process');
const {totp}=require('../src/auth');
test('first run, 2FA, session, protected settings and persistent events API', {timeout:20000},async()=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'hussam-integ-'));const port=41000+Math.floor(Math.random()*20000),url=`http://127.0.0.1:${port}`;
 const child=spawn(process.execPath,[path.join(__dirname,'..','src/server.js')],{env:{...process.env,DATA_DIR:dir,PORT:String(port)},stdio:['ignore','pipe','pipe']});let stdout='',cookie='',csrf='';child.stdout.on('data',v=>stdout+=v.toString());child.stderr.on('data',v=>stdout+=v.toString());
 async function req(path,method='GET',data,authed=false){const r=await fetch(url+path,{method,headers:{...(data?{'Content-Type':'application/json'}:{}),...(authed?{'Cookie':cookie,'X-CSRF-Token':csrf}:{}),'Origin':url},body:data?JSON.stringify(data):undefined});const text=await r.text();return {status:r.status,body:text?JSON.parse(text):{},headers:r.headers};}
 try{
  let ok=false;for(let i=0;i<45;i++){try{const r=await req('/healthz');if(r.status===200){ok=true;break;}}catch{}await new Promise(r=>setTimeout(r,90));}assert.ok(ok,'server started');
  const code=stdout.match(/FIRST RUN SETUP CODE ===\s*([A-Za-z0-9_-]+)/)?.[1];assert.ok(code,'setup pairing code printed in logs');
  assert.equal((await req('/api/status')).status,403);
  assert.equal((await req('/api/setup/prepare','POST',{setupCode:'incorrect'})).status,401);
  const prepared=await req('/api/setup/prepare','POST',{setupCode:code});assert.equal(prepared.status,200);assert.equal(prepared.body.secret.length,32);
  const setup=await req('/api/setup/complete','POST',{setupCode:code,password:'Some-unpredictable-password#5678',code:totp(prepared.body.secret)});assert.equal(setup.status,200);cookie=setup.headers.get('set-cookie')?.split(';')[0]||'';csrf=setup.body.csrf;assert.ok(cookie.startsWith('hussam_session='));
  assert.equal((await req('/api/settings')).status,401);
  assert.equal((await req('/api/settings','POST',{capital:100},false)).status,401);
  const s=await req('/api/settings','GET',undefined,true);assert.equal(s.body.values.capital,50);
  assert.equal((await req('/api/settings','POST',{capital:75},true)).status,200);
  assert.equal((await req('/api/settings','GET',undefined,true)).body.values.capital,75);
  assert.equal((await req('/api/settings','POST',{capital:-100},true)).status,400);
  assert.equal((await req('/api/events','GET',undefined,true)).status,200);
  assert.equal((await req('/api/auth/logout','POST',{},true)).status,200);
  assert.equal((await req('/api/status','GET',undefined,true)).status,401);
  assert.equal((await req('/api/setup/prepare','POST',{setupCode:code})).status,401);
 }finally{child.kill('SIGTERM');await new Promise(r=>{if(child.exitCode!==null||child.signalCode!==null)return r();child.once('exit',r);setTimeout(r,1500).unref();});fs.rmSync(dir,{recursive:true,force:true});}
});
