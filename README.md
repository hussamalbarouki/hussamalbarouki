# Binance Triangle Lab (read-only)

A 24/7 **triangular arbitrage research tool**, designed for a hypothetical **50 USDT** on **Binance Spot only**. It never places trades, requests API credentials, or accesses balances. It is NOT a profit bot and cannot prove real order execution.

## What it measures

1. At startup calls Binance public REST `/api/v3/exchangeInfo` and `/api/v3/ticker/24hr` to select liquid Spot assets.
2. Builds both directed paths `USDT → A → B → USDT` where three trading pairs exist. Direction matters.
3. Subscribes to individual `<symbol>@bookTicker` WebSocket streams, checks top bid/ask and available quantities (not last-traded prices).
4. For promising routes, fetches all three depth snapshots (`/api/v3/depth?limit=20`), models buys at asks and sells at bids through order-book levels, subtracts **taker fee on each received asset**, rounds down quantities, and checks minimum quantity and minimum notional filters.
5. Waits 800 ms, refreshes all three snapshots, and records `qualified` only when **both** snapshots still show at least the configured estimated minimum net result.
6. Saves each validation attempt to `data/events.jsonl`, shows a mobile-friendly Arabic dashboard, and allows exporting the raw log.

### Significant limitations

- Three Binance market orders are **not atomic**; prices can disappear before or during execution. REST depth snapshots are captured at slightly different times. A `qualified` signal is only a research observation.
- We do not model the actual exchange matching engine, queue priority, unfilled legs, order rejections, deposits/withdrawals, tax, or personalized fee discounts. Binance market minimum-notional checks use Binance's reference/average price, so local checks are approximate.
- Residual dust is discarded from the modeled proceeds (conservative); fees are assumed in the **received asset**. Fee tier default 0.1% per leg, not asserted as your account's actual fee.
- There is **no realized, paper-executed, or guaranteed P&L**. Never sum independent signals as daily earnings. Signals can overlap.
- WebSocket quotes are validated for freshness on arrival time, not Binance exchange timestamp; individual bookTicker payloads may omit event time.
- Rates, supported symbols and Binance accessibility depend on VPS region. HTTP 418/429 invokes a cooldown. The code does not proxy around geographic restrictions.

## Coolify installation

**Recommended:** Use a dedicated repository (or the branch containing this project) and deploy as a **Dockerfile-based** application.

1. In Coolify: **New Resource → Application → Git repository**, choose the GitHub repository and the branch containing this project.
2. Set **Build Pack = Dockerfile**, **Dockerfile location = `./Dockerfile`**, **Port = `3000`**.
3. Configure environment variables (see `.env.example`); set a strong, random `DASH_TOKEN` if exposing it publicly.
4. Mount a persistent directory/volume to **`/app/data`**. This is required to keep the JSONL log across container recreation.
5. Deploy. Open the Coolify-assigned HTTPS domain, enter the dashboard token, and allow the system at least 24–72 hours to collect observations.
6. If the page shows `initialization error` / `WebSocket not connected`, inspect logs. Ensure the VPS network can directly reach `data-api.binance.vision` and `data-stream.binance.vision`.

No Binance API key or Binance account is required. Never paste private API keys into this monitor or commit secrets to GitHub.

### Environment variables

| Variable | Default | Purpose |
|---|---:|---|
| `PORT` | `3000` | HTTP port |
| `CAPITAL_USDT` | `50` | Test position size |
| `TAKER_FEE` | `0.001` | Per-leg taker fee fraction (0.001 = 0.1%) |
| `MAX_ASSETS` | `60` | Top assets with USDT volume |
| `MIN_USDT_VOLUME` | `1000000` | USDT 24h quote volume floor |
| `MIN_PREVIEW_USD` | `0.04` | Trigger depth validation at this preview net profit |
| `MIN_RECHECK_USD` | `0.02` | Minimum theoretical net at each depth check |
| `RECHECK_DELAY_MS` | `800` | Delay between depth snapshots |
| `MAX_VERIFY_PER_MIN` | `6` | REST depth-validation cap |
| `DASH_TOKEN` | unset | Protect `/api/status` and `/api/export` |

**Monitor all events** via dashboard and `GET /api/export` (auth Bearer token when set). **Liveness** via `GET /healthz`.

## Local tests

```sh
npm install
npm test
npm start
```

Unit tests cover path directions, three-leg fees, minimum notional, depth exhaustion, quantity rounding and stale quotations. To verify real connectivity, deploy to a machine permitted to access Binance and check WebSocket message age and market counts.

## How to interpret results

If 72h show 0 `qualified` opportunities, this particular sampling, fees and capital did not detect a repeatably positive theoretical opportunity; **it does not prove mathematical impossibility**. If positive opportunities appear, the next separate research step is to measure real end-to-end execution latency and quantify adverse price movements before considering very small real transactions. No live trading capability is included.

Data sources: https://developers.binance.com/en/docs/products/spot/web-socket-streams and https://developers.binance.com/en/docs/products/spot/filters
