# Phase 2.1 CI verification report

## Verified pull request run

- Pull request: #3 (merged)
- GitHub Actions workflow run: #36
- Run ID: 41580034767
- Tested branch head: 75ae592c195ea920e1557ece59995d10e0075675
- Python 3.12: compilation passed; **35 passed, 1 skipped in 0.32s**
- Python 3.13: compilation passed; **35 passed, 1 skipped in 0.56s**
- Skipped test: opt-in public Binance API smoke test; it is disabled unless BINANCE_PUBLIC_INTEGRATION=1.

Workflow URL:
https://github.com/bhuttasaifullah2-ui/binance-quant-arbitrage/actions/runs/41580034767

## Verification scope

The automated suite uses deterministic mocked market responses and local SQLite databases. It verifies parsing, validation, calculations, event integrity, pagination, API responses, schema upgrades and persistence behavior.

The run does not verify live Binance WebSocket connectivity, real-time feed stability, production rate-limit behavior, or profitability under actual execution. The optional public API smoke test was skipped. No trading credentials or order endpoints are used.

## Merge

Merged PR #3 into main with squash merge.

Merge commit:
04f03f16979af923ac420ffa93c70dd3961089b9

The system remains strictly read-only. WebSocket streaming and observation mode are opt-in.


## Follow-up scanner correctness run

A follow-up PR corrected the WebSocket bootstrap behavior so the scanner uses one REST bulk bootstrap, then consumes normalized per-symbol hub state and rejects stale/missing fields instead of repeatedly polling all tickers.

- Pull request: #4 (merged)
- CI run: #40
- Run ID: 41580273403
- Tested head: e46ec0d18ac8d99bd62e22c47884e0d585a60f14
- Python 3.12: **36 passed, 1 skipped in 0.54s**
- Python 3.13: **36 passed, 1 skipped in 0.44s**
- Additional regression test: verifies the scanner performs only one bulk REST bootstrap in WebSocket mode.

Follow-up merge commit:
e93cef7fa48dc904cf346c9cea701bb435d09beb

Workflow URL:
https://github.com/bhuttasaifullah2-ui/binance-quant-arbitrage/actions/runs/41580273403
