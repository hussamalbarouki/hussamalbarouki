'use strict';
// READ ONLY SHADOW ENGINE. No authentication, API keys, POSTs or Binance orders.
const crypto=require('node:crypto');
const SPOT_WS='wss://data-stream.binance.vision/stream?streams=';
const EPS=1e-10;
function stepFloor(x,step){const s=Number(step); if(!(x>0))return 0;if(!(s>0))return x;const d=Math.min(12,(String(step).split('.')[1]||'').replace(/0+$/,'').length);return +((Math.floor(x/s+1e-8))*s).toFixed(d);}
function pFilter(meta,t){return (meta.filters||[]).find(x=>x.filterType===t)||{};}
function orderAllowed(meta,qty,price){const lot=pFilter(meta,'LOT_SIZE'),pf=pFilter(meta,'PRICE_FILTER'),min=pFilter(meta,'MIN_NOTIONAL'),not=pFilter(meta,'NOTIONAL');
 if(!Number.isFinite(qty)||!Number.isFinite(price)||qty<=0||price<=0)return false;
 if(Number(lot.minQty||0)>qty+EPS||Number(lot.maxQty||0)>0&&qty>Number(lot.maxQty)+EPS)return false;
 if(Number(pf.minPrice||0)>price+EPS||Number(pf.maxPrice||0)>0&&price>Number(pf.maxPrice)+EPS)return false;
 const value=qty*price;
 if(Number(min.minNotional||0)>value+EPS||Number(not.minNotional||0)>value+EPS||Number(not.maxNotional||0)>0&&value>Number(not.maxNotional)+EPS)return false;
 return true;
}
class LocalBook {
 constructor(){this.ready=false;this.lastId=0;this.bids=new Map();this.asks=new Map();this.buffer=[];this.at=0;this.lastEventAt=0;}
 push(event){if(!Number.isSafeInteger(event.U)||!Number.isSafeInteger(event.u)||!Array.isArray(event.b)||!Array.isArray(event.a))return 'bad';if(!this.ready){this.buffer.push(event);if(this.buffer.length>1500)return 'overflow';return 'buffering';}return this.apply(event);}
 initialize(snapshot){if(!Number.isSafeInteger(snapshot.lastUpdateId))throw Error('Snapshot without update ID');this.bids=new Map(snapshot.bids.map(([p,q])=>[p,Number(q)]).filter(([,q])=>q>0));this.asks=new Map(snapshot.asks.map(([p,q])=>[p,Number(q)]).filter(([,q])=>q>0));this.lastId=snapshot.lastUpdateId;this.ready=true;
 for(const ev of this.buffer){const result=this.apply(ev);if(result==='gap'){this.ready=false;throw Error('Depth stream discontinuity during snapshot');}}this.buffer=[];this.at=Date.now();}
 apply(ev){if(ev.u<=this.lastId)return 'old';if(ev.U>this.lastId+1)return 'gap';for(const [p,q] of ev.b){if(Number(q)===0)this.bids.delete(p);else this.bids.set(p,Number(q));}for(const [p,q] of ev.a){if(Number(q)===0)this.asks.delete(p);else this.asks.set(p,Number(q));}this.lastId=ev.u;this.at=Date.now();this.lastEventAt=Number(ev.E)||0;return 'ok';}
 top(){if(!this.ready)return null;let bid=0,ask=Infinity,bidQty=0,askQty=0;for(const [p,q] of this.bids)if(Number(p)>bid){bid=Number(p);bidQty=q;}for(const [p,q] of this.asks)if(Number(p)<ask){ask=Number(p);askQty=q;}if(!(bid>0&&ask>bid&&askQty>0&&bidQty>0))return null;return {bid,ask,bidQty,askQty,spreadBps:(ask/bid-1)*10000};}
 level(side,price){const m=side==='BUY'?this.bids:this.asks;for(const [p,q] of m)if(Math.abs(Number(p)-price)<=price*1e-10)return q;return 0;}
}
class ShadowEngine {
 constructor({store,rest,log=console.log,onEvent=()=>{},clock=()=>Date.now()}){
 this.store=store;this.rest=rest;this.log=log;this.onEvent=onEvent;this.clock=clock;this.settings=null;this.meta=null;this.socket=null;this.book=new LocalBook();this.connected=false;this.phase='waiting for market metadata';this.lastError='';this.lastTrade=0;this.lastBook=0;this.gaps=0;this.reconnects=0;this.generation=0;this.reconnectTimer=null;this.tickTimer=null;this.processing=false;this.closed=false;this.quotes=0;this.trades=0;this.pingAt=0;this.lastEquitySampleAt=0;this.lastSaveAt=0;
 this.ledger=this.store.get('shadow_ledger')||null;
 }
 event(type,detail={}){if(!this.ledger)return;const row={trial:this.ledger.trial,at:this.clock(),type,symbol:this.ledger.symbol,detail};try{row.id=this.store.insertShadow(row);}catch(e){this.lastError='Shadow log: '+e.message;}this.onEvent({id:row.id,type,trial:row.trial});}
 save(force=false){if(this.ledger&&(force||this.clock()-this.lastSaveAt>4000)){this.store.set('shadow_ledger',this.ledger);this.lastSaveAt=this.clock();}}
 configure(settings,meta){const before=this.settings;this.settings={...settings};if(!settings.shadowEnabled){this.disconnect('disabled');this.phase='disabled';return;}
 if(!meta||meta.symbol!==settings.shadowSymbol||meta.status!=='TRADING'||meta.quoteAsset!=='USDT'||meta.isSpotTradingAllowed===false||!(!meta.orderTypes||meta.orderTypes.includes('LIMIT')||meta.orderTypes.includes('LIMIT_MAKER'))){this.disconnect('unsupported_market');this.meta=null;this.phase='symbol not available for Binance Spot USDT';return;}
 const symbolChanged=this.meta?.symbol!==meta.symbol;const restart=symbolChanged||!this.socket;
 if(this.ledger&&this.ledger.symbol!==settings.shadowSymbol){this.cancelAll('symbol_changed');this.event('trial_closed',{reason:'symbol_changed',equity:this.ledger.lastEquity});this.ledger=null;this.save();this.store.set('shadow_ledger',null);}
 this.meta=meta;
 if(restart){this.disconnect('reconfiguring');this.start();}else if(before&&(before.shadowOrderUsdt!==settings.shadowOrderUsdt||before.shadowMakerFee!==settings.shadowMakerFee||before.shadowQuoteTimeoutSec!==settings.shadowQuoteTimeoutSec||before.shadowMinSpreadBps!==settings.shadowMinSpreadBps||before.shadowActivationMs!==settings.shadowActivationMs)){this.cancelAll('settings_changed');}
 }
 start(){if(this.closed||!this.meta||!this.settings.shadowEnabled)return;const generation=++this.generation;this.book=new LocalBook();this.lastBook=0;this.phase='connecting depth and trades';const symbol=this.meta.symbol.toLowerCase();const url=SPOT_WS+symbol+'@depth@100ms/'+symbol+'@aggTrade';
 try{const ws=new WebSocket(url);this.socket=ws;
 ws.addEventListener('open',()=>{if(this.socket!==ws)return;this.connected=true;this.phase='synchronizing order book';this.loadSnapshot(generation).catch(e=>this.retry(e.message,generation));});
 ws.addEventListener('message',e=>{if(this.socket!==ws)return;let evt;try{evt=JSON.parse(e.data).data;}catch{return;}if(!evt)return;
 if(evt.e==='depthUpdate'){const result=this.book.push(evt);if(result==='gap'||result==='overflow'){this.gaps++;this.retry('Depth sequence gap; resync required',generation);return;}if(result==='ok'){this.lastBook=this.clock();this.advance();}}
 if(evt.e==='aggTrade')this.trade(evt);
 });
 ws.addEventListener('error',()=>{if(this.socket===ws)this.lastError='Binance shadow WebSocket error';});
 ws.addEventListener('close',()=>{if(this.socket!==ws||this.closed)return;this.retry('WebSocket disconnected',generation);});
 }catch(e){this.retry(e.message,generation);}
 }
 async loadSnapshot(generation){const snap=await this.rest('/api/v3/depth?symbol='+encodeURIComponent(this.meta.symbol)+'&limit=1000',12000);if(this.generation!==generation||!this.socket)return;try{this.book.initialize(snap);}catch(e){this.gaps++;return this.retry(e.message,generation);}this.lastBook=this.clock();this.phase='shadow simulation live';this.lastError='';this.advance();}
 retry(message,generation){if(this.generation!==generation||this.closed)return;this.lastError=message;this.phase='reconnecting / data integrity';this.reconnects++;this.disconnect('feed_gap');clearTimeout(this.reconnectTimer);this.reconnectTimer=setTimeout(()=>this.start(),6000);this.reconnectTimer.unref?.();}
 disconnect(reason){++this.generation;this.connected=false;const ws=this.socket;this.socket=null;if(ws)try{ws.close();}catch{}this.cancelAll(reason);}
 stop(){this.closed=true;clearTimeout(this.reconnectTimer);clearInterval(this.tickTimer);this.disconnect('shutdown');}
 cancelAll(reason){if(!this.ledger)return;for(const side of ['BUY','SELL']){if(this.ledger.orders?.[side]){this.event('cancel',{side,reason,remaining:this.ledger.orders[side].remaining,price:this.ledger.orders[side].price});this.ledger.orders[side]=null;}}this.save(true);}
 fresh(){return this.settings?.shadowEnabled&&this.book.ready&&this.connected&&this.clock()-this.lastBook<3000;}
 trialStart(top){const s=this.settings;const budget=s.shadowCapital,part=budget*s.shadowInitialInventoryPct/100;
 const lot=pFilter(this.meta,'LOT_SIZE');const base=stepFloor(part/(top.ask*(1+s.shadowMakerFee)),lot.stepSize||'0');const spent=base*top.ask,fee=s.shadowMakerFee;
 // Simulated initial inventory is acquired at ask, NOT a free asset gift or assumed Maker fill.
 const received=base*(1-fee);const cash=budget-spent;
 this.ledger={trial:crypto.randomUUID(),symbol:this.meta.symbol,baseAsset:this.meta.baseAsset,started:this.clock(),capital:budget,cash,base:received,cost:spent,realized:0,fees:spent*fee,volume:0,buys:0,sells:0,partial:0,cancels:0,orders:{BUY:null,SELL:null},maxEquity:budget,drawdown:0,lastEquity:cash+received*top.bid,stopped:false,stopReason:'',firstPrice:top.bid, lastPrice:top.bid, lastUpdated:this.clock(), lastBookId:this.book.lastId};
 this.save(true);this.event('trial_start',{capital:budget,seedBaseQty:received,seedPrice:top.ask,seedCost:spent,seedFee:spent*fee,openingEquity:this.ledger.lastEquity,feeAssumptionVerified:false});
 }
 mark(top){if(!this.ledger)return;const l=this.ledger;const equity=l.cash+l.base*top.bid;l.maxEquity=Math.max(l.maxEquity,equity);l.drawdown=Math.max(l.drawdown,(l.maxEquity-equity)/l.maxEquity*100);l.lastEquity=equity;l.lastPrice=top.bid;l.lastUpdated=this.clock();l.lastBookId=this.book.lastId;
 if(this.clock()-this.lastEquitySampleAt>=60000){this.lastEquitySampleAt=this.clock();try{this.store.sampleEquity(l.trial,this.clock(),equity,l.cash,l.base,top.bid);}catch(e){this.lastError='Equity log: '+e.message;}}
 const loss=(l.capital-equity)/l.capital*100;
 if(!l.stopped&&loss>=this.settings.shadowMaxDailyLossPct){l.stopped=true;l.stopReason='equity_loss_guard';this.event('risk_stop',{lossPct:loss,equity});this.cancelAll('risk_stop');}
 }
 quote(side,top){const s=this.settings,l=this.ledger;const tick=Number(pFilter(this.meta,'PRICE_FILTER').tickSize||0),step=pFilter(this.meta,'LOT_SIZE').stepSize||'0';
 const price=side==='BUY'?top.bid:top.ask;const max=side==='BUY'?Math.min(s.shadowOrderUsdt,l.cash)/(price*(1+s.shadowMakerFee)):Math.min(l.base,s.shadowOrderUsdt/price);
 const qty=stepFloor(max,step);if(!(qty>0)||!orderAllowed(this.meta,qty,price))return;
 // Price must be strictly passive: the order cannot cross the other side.
 if(side==='BUY'&&price>=top.ask||side==='SELL'&&price<=top.bid)return;
 const now=this.clock();const o={side,price,qty,remaining:qty,ahead:this.book.level(side,price),created:now,activeAt:now+s.shadowActivationMs,tick,filled:0};l.orders[side]=o;this.quotes++;this.event('quote',{side,price,qty,ahead:o.ahead,activationMs:s.shadowActivationMs,spreadBps:top.spreadBps});
 }
 advance(){if(!this.fresh()||!this.meta)return;const top=this.book.top();if(!top)return;const now=this.clock();if(!this.ledger)this.trialStart(top);const l=this.ledger;
 this.mark(top);if(l.stopped){this.save();return;}
 for(const side of ['BUY','SELL']){const o=l.orders[side];if(!o)continue;
 const expired=now-o.created>=this.settings.shadowQuoteTimeoutSec*1000;
 const noLongerPassive=side==='BUY'?o.price>=top.ask:o.price<=top.bid;
 const stalePrice=side==='BUY'?top.bid>o.price+o.tick/2:top.ask<o.price-o.tick/2;
 if(expired||noLongerPassive||stalePrice){this.event('cancel',{side,price:o.price,remaining:o.remaining,reason:expired?'timeout':noLongerPassive?'would_cross':'price_moved'});l.orders[side]=null;l.cancels++;}
 }
 if(top.spreadBps>=this.settings.shadowMinSpreadBps){if(!l.orders.BUY)this.quote('BUY',top);if(!l.orders.SELL)this.quote('SELL',top);}
 this.save();
 }
 trade(evt){if(!this.fresh()||!this.ledger||this.ledger.stopped)return;
 const at=this.clock(),time=Number(evt.T)||0,price=Number(evt.p),qty=Number(evt.q);
 if(!(qty>0&&price>0)||!Number.isSafeInteger(time)||time>at+3000||time<at-15000)return;
 this.lastTrade=at;this.trades++;const side=evt.m===true?'BUY':'SELL';const o=this.ledger.orders[side];if(!o||at<o.activeAt||time<o.activeAt-250)return;
 // A passive bid fills on aggressive sells; passive ask fills on aggressive buys.
 if(Math.abs(price-o.price)>o.price*1e-10)return;
 const consumed=Math.min(o.ahead,qty);o.ahead=Math.max(0,o.ahead-consumed);
 const possible=qty-consumed;
 if(possible<=EPS)return;
 let fill=Math.min(o.remaining,possible);const l=this.ledger,fee=this.settings.shadowMakerFee;
 if(side==='BUY')fill=Math.min(fill,l.cash/(o.price*(1+fee)));
 else fill=Math.min(fill,l.base);
 if(fill<EPS)return;
 if(side==='BUY'){
  const value=fill*o.price,baseReceived=fill*(1-fee);l.cash-=value;l.base+=baseReceived;l.cost+=value;l.fees+=fill*fee*o.price;
 }else{
  const gross=fill*o.price,net=gross*(1-fee),cost=l.base>0?l.cost*fill/l.base:0;
  l.cash+=net;l.base-=fill;l.cost=Math.max(0,l.cost-cost);l.realized+=net-cost;l.fees+=gross*fee;
 }
 o.remaining-=fill;o.filled+=fill;l.volume+=fill*o.price;
 this.event('fill',{side,qty:fill,price:o.price,remaining:o.remaining,queueAheadAfter:o.ahead,tradeId:evt.a,tradeTime:time,feeEstimated:fill*o.price*fee});
 if(o.remaining<=EPS){l.orders[side]=null;if(side==='BUY')l.buys++;else l.sells++;}else l.partial++;
 const top=this.book.top();if(top)this.mark(top);this.save(true);
 }
 status(){const top=this.book.top(),l=this.ledger,s=this.settings||{};return {enabled:!!s.shadowEnabled,symbol:s.shadowSymbol||null,phase:this.phase,connected:this.connected,bookReady:this.book.ready,fresh:this.fresh(),bookAgeMs:this.lastBook?this.clock()-this.lastBook:null,lastTradeAgeMs:this.lastTrade?this.clock()-this.lastTrade:null,depthUpdateId:this.book.lastId,gaps:this.gaps,reconnects:this.reconnects,lastError:this.lastError,top,orders:l?.orders||{BUY:null,SELL:null},trial:l?{id:l.trial,started:l.started,capital:l.capital,cash:l.cash,base:l.base,cost:l.cost,realized:l.realized,fees:l.fees,volume:l.volume,buys:l.buys,sells:l.sells,partial:l.partial,cancels:l.cancels,equity:l.lastEquity,pnl:l.lastEquity-l.capital,pnlPct:(l.lastEquity/l.capital-1)*100,drawdownPct:l.drawdown,stopped:l.stopped,stopReason:l.stopReason}:null,assumptions:{fee:s.shadowMakerFee,feeNotVerified:true,orderUsdt:s.shadowOrderUsdt,initialInventoryPct:s.shadowInitialInventoryPct,activationMs:s.shadowActivationMs,minSpreadBps:s.shadowMinSpreadBps},warning:'RESEARCH ONLY: zero Binance orders. Maker queue and fills inferred conservatively, not provable. Fees are manual assumptions, not account verified.'};}
 reset(){this.cancelAll('manual_reset');if(this.ledger)this.event('trial_closed',{reason:'manual_reset',equity:this.ledger.lastEquity});this.ledger=null;this.lastEquitySampleAt=0;this.store.set('shadow_ledger',null);this.advance();}
}
module.exports={LocalBook,ShadowEngine,stepFloor,orderAllowed};
