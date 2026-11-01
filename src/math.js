'use strict';
// Research estimates, NOT executable orders. Each leg conservatively loses dust.
function floorStep(value,stepText){const step=Number(stepText);if(!(value>0)||!Number.isFinite(value))return 0;if(!(step>0))return value;const decimals=Math.min(14,(String(stepText).split('.')[1]||'').replace(/0+$/,'').length);return Number((Math.floor(value/step+1e-9)*step).toFixed(decimals));}
function lot(s){const filters=s.filters||[],market=filters.find(f=>f.filterType==='MARKET_LOT_SIZE'),normal=filters.find(f=>f.filterType==='LOT_SIZE');return market||normal||{minQty:'0',maxQty:'999999999',stepSize:'0'};}
function notionalAllowed(s,quote){for(const f of s.filters||[]){if(f.filterType==='MIN_NOTIONAL'&&f.applyToMarket!==false&&quote+1e-9<Number(f.minNotional))return false;if(f.filterType==='NOTIONAL'){if(f.applyMinToMarket!==false&&quote+1e-9<Number(f.minNotional))return false;if(f.applyMaxToMarket&&Number(f.maxNotional)>0&&quote>Number(f.maxNotional))return false;}}return true;}
function buyCapacity(asks,budget,buffer,share){let rest=budget,base=0;for(const [p,q] of asks){const price=Number(p)*(1+buffer),qty=Number(q)*share;if(!(price>0&&qty>0))continue;const spend=Math.min(rest,qty*price);base+=spend/price;rest-=spend;if(rest<=budget*1e-10)break;}return rest<=budget*0.00001?base:null;}
function quoteForBase(levels,base,buffer,share,side){let rest=base,quote=0;for(const [p,q] of levels){const price=Number(p)*(side==='BUY'?1+buffer:1-buffer),qty=Number(q)*share;if(!(price>0&&qty>0))continue;const used=Math.min(rest,qty);quote+=used*price;rest-=used;if(rest<=base*1e-10)break;}return rest<=base*1e-8?quote:null;}
function simulate(cycle,books,symbols,budget,fee,bufferBps=0,liquiditySharePct=100){let holding=Number(budget);const legs=[];if(!(holding>0)||!Number.isFinite(holding)||fee<0||fee>=0.05||bufferBps<0||bufferBps>500||liquiditySharePct<=0||liquiditySharePct>100)return {ok:false,reason:'invalid_input'};const buffer=bufferBps/10000,share=liquiditySharePct/100;
 for(const edge of cycle.edges){const meta=symbols.get(edge.symbol),book=books.get(edge.symbol);if(!meta||!book)return {ok:false,reason:'missing_book'};const limits=lot(meta);const step=limits.stepSize||'0';let qty,quote,received;
 if(edge.side==='BUY'){
  const available=buyCapacity(book.asks,holding,buffer,share);if(available===null)return {ok:false,reason:'insufficient_ask_depth'};
  qty=floorStep(available,step);if(!(qty>0))return {ok:false,reason:'qty_rounded_to_zero'};
  quote=quoteForBase(book.asks,qty,buffer,share,'BUY');if(quote===null||quote>holding*(1+1e-8))return {ok:false,reason:'insufficient_ask_depth'};
  received=qty*(1-fee);
 } else {
  qty=floorStep(holding,step);if(!(qty>0))return {ok:false,reason:'qty_rounded_to_zero'};
  quote=quoteForBase(book.bids,qty,buffer,share,'SELL');if(quote===null)return {ok:false,reason:'insufficient_bid_depth'};
  received=quote*(1-fee);
 }
 if(qty+1e-12<Number(limits.minQty)||Number(limits.maxQty)>0&&qty>Number(limits.maxQty))return {ok:false,reason:'lot_size_rejected'};
 if(!notionalAllowed(meta,quote))return {ok:false,reason:'notional_rejected'};
 legs.push({symbol:edge.symbol,side:edge.side,qty,quote,received,from:edge.from,to:edge.to});holding=received;
 }
 return {ok:true,netUsd:holding-budget,netPct:((holding/budget)-1)*100,finalUsdt:holding,legs};
}
function graphCycles(rows,allowed){const edges=new Map();for(const s of rows){if(!allowed.has(s.baseAsset)||!allowed.has(s.quoteAsset))continue;for(const e of [{from:s.baseAsset,to:s.quoteAsset,symbol:s.symbol,side:'SELL'},{from:s.quoteAsset,to:s.baseAsset,symbol:s.symbol,side:'BUY'}]){if(!edges.has(e.from))edges.set(e.from,[]);edges.get(e.from).push(e);}}const cycles=[];const found=new Set();for(const a of edges.get('USDT')||[])for(const b of edges.get(a.to)||[])if(b.to!=='USDT'&&b.to!==a.from)for(const c of edges.get(b.to)||[])if(c.to==='USDT'){const path=[a,b,c];const id=path.map(x=>x.symbol+':'+x.side).join('|');if(!found.has(id)){cycles.push({id,label:`USDT → ${a.to} → ${b.to} → USDT`,edges:path});found.add(id);}}return cycles;}
function preview(cycle,ticks,budget,fee,bufferBps,ageLimit,now=Date.now()){let amount=budget,maxAge=0;for(const e of cycle.edges){const tick=ticks.get(e.symbol);if(!tick||!Number.isFinite(tick.bid)||!Number.isFinite(tick.ask)||tick.bid<=0||tick.ask<=0||tick.bid>tick.ask)return null;const age=Math.max(0,now-tick.at);if(age>ageLimit)return null;maxAge=Math.max(maxAge,age);amount=(e.side==='BUY'?amount/(tick.ask*(1+bufferBps/10000)):amount*tick.bid*(1-bufferBps/10000))*(1-fee);}return {netUsd:amount-budget,netPct:(amount/budget-1)*100,maxQuoteAgeMs:maxAge};}
module.exports={floorStep,simulate,graphCycles,preview};
