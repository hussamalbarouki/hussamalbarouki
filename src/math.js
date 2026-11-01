'use strict';

// Conservative *non-executing* estimate for three market orders. Values are
// Number for screening only; never use this code for live order placement.
function floorStep(value, stepText) {
  const step = Number(stepText);
  if (!(value > 0) || !Number.isFinite(value)) return 0;
  if (!(step > 0)) return value;
  const decimals = Math.min(12, (String(stepText).split('.')[1] || '').replace(/0+$/, '').length);
  return Number((Math.floor((value / step) + 1e-9) * step).toFixed(decimals));
}
function lot(symbol) {
  const all = symbol.filters || [];
  const market = all.find(f => f.filterType === 'MARKET_LOT_SIZE');
  const normal = all.find(f => f.filterType === 'LOT_SIZE');
  // Some MARKET_LOT_SIZE filters have stepSize=0 (no step constraint).
  if (market && Number(market.stepSize) > 0) return market;
  return normal || market || { minQty: '0', maxQty: '999999999', stepSize: '0' };
}
function notionalAllowed(symbol, value) {
  const fs = symbol.filters || [];
  const min = fs.find(f => f.filterType === 'MIN_NOTIONAL');
  const range = fs.find(f => f.filterType === 'NOTIONAL');
  if (min && min.applyToMarket !== false && value + 1e-9 < Number(min.minNotional)) return false;
  if (range && range.applyMinToMarket !== false && value + 1e-9 < Number(range.minNotional)) return false;
  if (range && range.applyMaxToMarket && Number(range.maxNotional) > 0 && value > Number(range.maxNotional)) return false;
  return true;
}
function levelFill(levels, wantedBase, kind) {
  let pending = wantedBase, quote = 0;
  for (const [p, q] of levels) {
    const price = Number(p), qty = Number(q);
    if (!(price > 0) || !(qty > 0)) continue;
    const used = Math.min(pending, qty);
    quote += used * price;
    pending -= used;
    if (pending <= 1e-10 * Math.max(1, wantedBase)) break;
  }
  if (pending > 1e-10 * Math.max(1, wantedBase)) return null;
  return quote;
}
function buyCapacity(asks, budget) {
  let remaining = budget, base = 0;
  for (const [p, q] of asks) {
    const price = Number(p), qty = Number(q);
    if (!(price > 0) || !(qty > 0)) continue;
    const spent = Math.min(remaining, qty * price);
    base += spent / price;
    remaining -= spent;
    if (remaining <= 1e-9 * budget) break;
  }
  // We demand adequate displayed liquidity to use essentially all the budget.
  return remaining <= Math.max(1e-8, budget * 0.00001) ? base : null;
}
function simulate(cycle, books, symbols, budget, fee) {
  let holding = Number(budget);
  const legs = [];
  if (!(holding > 0) || !(fee >= 0 && fee < 0.05)) return { ok:false, reason:'invalid_input' };
  for (const edge of cycle.edges) {
    const meta = symbols.get(edge.symbol);
    const book = books.get(edge.symbol);
    if (!meta || !book) return { ok:false, reason:'missing_book' };
    const limits = lot(meta);
    const step = limits.stepSize || '0';
    let qty, value, received;
    if (edge.side === 'BUY') {
      const cap = buyCapacity(book.asks, holding);
      if (cap === null) return { ok:false, reason:'insufficient_ask_depth' };
      qty = floorStep(cap, step);
      if (!(qty > 0)) return { ok:false, reason:'qty_rounded_to_zero' };
      value = levelFill(book.asks, qty);
      if (value === null || value > holding * (1 + 1e-10)) return { ok:false, reason:'insufficient_ask_depth' };
      received = qty * (1 - fee); // conservatively fee deducted in received asset
    } else {
      qty = floorStep(holding, step);
      if (!(qty > 0)) return { ok:false, reason:'qty_rounded_to_zero' };
      value = levelFill(book.bids, qty);
      if (value === null) return { ok:false, reason:'insufficient_bid_depth' };
      received = value * (1 - fee);
    }
    if (Number(limits.minQty) > qty || (Number(limits.maxQty) > 0 && qty > Number(limits.maxQty))) {
      return { ok:false, reason:'lot_size_rejected' };
    }
    if (!notionalAllowed(meta, value)) return { ok:false, reason:'notional_rejected' };
    legs.push({ symbol:edge.symbol, side:edge.side, baseQty:qty, quoteValue:value, received });
    holding = received;
  }
  return { ok:true, netUsd:holding - budget, netPct:(holding / budget - 1) * 100, finalUsdt:holding, legs };
}
function graphCycles(symbolRows, allowedAssets) {
  const outgoing = new Map();
  const add = (from, to, sym, side) => {
    if (!outgoing.has(from)) outgoing.set(from, []);
    outgoing.get(from).push({ from,to,symbol:sym,side });
  };
  for (const s of symbolRows) {
    if (!allowedAssets.has(s.baseAsset) || !allowedAssets.has(s.quoteAsset)) continue;
    add(s.baseAsset, s.quoteAsset, s.symbol, 'SELL');
    add(s.quoteAsset, s.baseAsset, s.symbol, 'BUY');
  }
  const routes = [], seen = new Set();
  for (const a of outgoing.get('USDT') || []) {
    if (a.to === 'USDT') continue;
    for (const b of outgoing.get(a.to) || []) {
      if (b.to === 'USDT' || b.to === a.to) continue;
      for (const c of outgoing.get(b.to) || []) {
        if (c.to !== 'USDT') continue;
        const id = [a.symbol+':'+a.side,b.symbol+':'+b.side,c.symbol+':'+c.side].join('|');
        if (seen.has(id)) continue;
        seen.add(id);
        routes.push({ id, label:`USDT → ${a.to} → ${b.to} → USDT`, edges:[a,b,c] });
      }
    }
  }
  return routes;
}
function topBooks(cycle, tickerMap, maximumAgeMs, now=Date.now()) {
  const out = new Map();
  for (const {symbol} of cycle.edges) {
    const t = tickerMap.get(symbol);
    if (!t || now - t.at > maximumAgeMs) return null;
    if (!Number.isFinite(t.bid) || !Number.isFinite(t.ask) || t.bid<=0 || t.ask<=0) return null;
    out.set(symbol,{bids:[[t.bid,t.bidQty]],asks:[[t.ask,t.askQty]]});
  }
  return out;
}
module.exports = {floorStep,simulate,graphCycles,topBooks};
