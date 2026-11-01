'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {LocalBook,ShadowEngine,orderAllowed}=require('../src/shadow');
const {Store}=require('../src/store');
const {defaults}=require('../src/settings');
const market={symbol:'UUSDT',baseAsset:'U',quoteAsset:'USDT',status:'TRADING',isSpotTradingAllowed:true,filters:[
 {filterType:'LOT_SIZE',minQty:'1',maxQty:'999999',stepSize:'0.001'},
 {filterType:'PRICE_FILTER',tickSize:'0.001',minPrice:'0.001',maxPrice:'100'},
 {filterType:'MIN_NOTIONAL',minNotional:'5'}]};
function snapshot(){return {lastUpdateId:100,bids:[['1.000', '5']],asks:[['1.005','40']]};}
function fake(){const dir=fs.mkdtempSync(path.join(os.tmpdir(),'hussam-shadow-test-'));const store=new Store(dir);const settings={...defaults(),shadowMakerFee:0,shadowActivationMs:0,shadowMinSpreadBps:1,shadowQuoteTimeoutSec:30};const engine=new ShadowEngine({store,rest:async()=>snapshot(),onEvent:()=>{}});engine.meta=market;engine.settings=settings;engine.connected=true;engine.book.initialize(snapshot());engine.lastBook=Date.now();return {engine,store,dir};}
function destroy(f){f.engine.stop();f.store.close();fs.rmSync(f.dir,{recursive:true,force:true});}
test('snapshot buffers and applies spot depth updates in correct sequence',()=>{
 const b=new LocalBook();assert.equal(b.push({U:101,u:102,b:[['1.000','7']],a:[]}), 'buffering');b.initialize(snapshot());assert.equal(b.lastId,102);assert.equal(b.top().bidQty,7);
 assert.equal(b.push({U:105,u:105,b:[],a:[]}), 'gap');
});
test('stale updates never rewind the book, zero quantities delete levels',()=>{
 const b=new LocalBook();b.initialize(snapshot());assert.equal(b.push({U:100,u:100,b:[],a:[]}), 'old');
 assert.equal(b.push({U:101,u:101,b:[['1.000','0'],['0.999','7']],a:[]}), 'ok');assert.equal(b.top().bid,.999);
});
test('maker quantity and notional filters reject impossible small orders',()=>{
 assert.equal(orderAllowed(market,2,1),false);assert.equal(orderAllowed(market,7,1),true);
});
test('shadow starts with charged inventory and bid-marked equity, not a free coin allocation',()=>{
 const f=fake();try{f.engine.advance();const s=f.engine.status();assert.ok(s.trial);assert.equal(s.trial.capital,50);assert.ok(s.trial.equity<50);assert.ok(s.trial.base>0);assert.ok(s.trial.cash>0);assert.ok(f.store.shadowEvents(10).some(x=>x.type==='trial_start'));}finally{destroy(f);}
});
test('aggressive sells must exhaust queue ahead before a hypothetical BUY fills',()=>{
 const f=fake();try{f.engine.advance();const order=f.engine.ledger.orders.BUY;assert.ok(order);assert.equal(order.ahead,5);
 const at=Date.now();f.engine.trade({m:true,p:'1.000',q:'4',T:at,a:1});assert.equal(f.engine.ledger.buys,0);assert.equal(f.engine.ledger.orders.BUY.remaining,order.qty);
 f.engine.trade({m:true,p:'1.000',q:'0.8',T:at,a:2});assert.equal(f.engine.ledger.orders.BUY.remaining,order.qty);
 const cash=f.engine.ledger.cash;f.engine.trade({m:true,p:'1.000',q:'2',T:at,a:3});assert.ok(f.engine.ledger.cash<cash);assert.equal(f.engine.ledger.orders.BUY.filled,1.8);
 assert.ok(f.store.shadowEvents(30).some(x=>x.type==='fill'));
 }finally{destroy(f);}
});
test('the wrong trade aggressor or price never fills a passive order',()=>{
 const f=fake();try{f.engine.advance();const before=f.engine.ledger.cash;
 f.engine.trade({m:false,p:'1.000',q:'100',T:Date.now(),a:11});f.engine.trade({m:true,p:'0.999',q:'100',T:Date.now(),a:12});assert.equal(f.engine.ledger.cash,before);
 }finally{destroy(f);}
});
test('disconnect cancels paper orders without manufacturing fills, balances survive restore',()=>{
 const f=fake();try{f.engine.advance();const initial=f.engine.status().trial;f.engine.disconnect('feed_gap');assert.equal(f.engine.ledger.orders.BUY,null);
 const restored=new ShadowEngine({store:f.store,rest:async()=>snapshot()});assert.equal(restored.ledger.cash,initial.cash);assert.equal(restored.ledger.base,initial.base);assert.equal(restored.ledger.trial,initial.id);
 }finally{destroy(f);}
});
test('loss guard stops quoting on mark-to-bid equity loss',()=>{
 const f=fake();try{f.engine.advance();f.engine.book.bids=new Map([['0.8',5]]);f.engine.book.asks=new Map([['0.805',40]]);f.engine.advance();assert.equal(f.engine.ledger.stopped,true);assert.equal(f.engine.ledger.orders.BUY,null);
 }finally{destroy(f);}
});
test('events and equity history persist in SQLite',()=>{
 const f=fake();try{f.engine.advance();const trial=f.engine.ledger.trial;assert.ok(f.store.shadowEvents(20).length>0);assert.equal(f.store.equityHistory(trial).length,1);
 }finally{destroy(f);}
});
