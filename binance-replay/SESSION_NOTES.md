# Binance Replay — Session Handoff (Apr 17 2026, 13:20 SGT)

## TL;DR
Built `binance-replay` Rust binary that drives real Binance aggTrades through
8 matching mechanisms and persists to Postgres `tick_data`. Two jobs running
now: crash (Oct 10 2025 21:00-22:00 UTC, BTCUSDT fut) and control (Sep 20
same clock hour). Results land in ~80 min and ~20 min respectively.

## Active runs (check with `ps -ef | grep delta-binance-replay`)
- Crash PID 171840  — log: `crash_20260417_051915.log`
- Control PID 171853 — log: `control_20260417_051915.log`
- Run IDs: `crash_btc_oct10_20260417_051915`, `control_btc_sep20_20260417_051915`

## Data on disk
- `/opt/delta-exchange/binance-ticks/raw/` — 4.58 GB Binance aggTrades
  spot + futures × BTC/ETH/SOL × Oct 10-12 crash + Sep 20-22 control
- `/opt/delta-exchange/hyperliquid-data/raw/` — HL REST context
  1h candles + funding, crash + control windows. 1m not available in REST.

## Project layout
- `binance-replay/src/types.rs`       — Order, Fill, Mechanism, Side, Participant
- `binance-replay/src/book.rs`        — OrderBook with price-ticks + matching
- `binance-replay/src/fair_value.rs`  — rolling VWAP estimator
- `binance-replay/src/queue_mechanisms.rs` — sort_queue for throttle buckets
- `binance-replay/src/main.rs`        — replay loop + Postgres writer
- `binance-replay/patch_throttle.py`  — first throttle patch (applied)
- `binance-replay/patch_throttle_fix.py` — passive/aggressor fix (applied)
- `binance-replay/final_analysis.sql` — core results query
- `binance-replay/results_20260417_051915.csv` — CSV export

## Preliminary directional findings (subject to is_aggressor fix)
- Control CLOB: mean slippage 1.66 (bps-scale on BTC ~$115K)
- Crash CLOB:   mean slippage 14.20 — 8x the calm regime
- Any batching increases slippage; effect magnifies under stress
- Max slippage roughly doubles under batching

## Known issues for next session
1. **Aggressor/passive determined by RNG participant type** (types.rs:80)
   → Causes between-run variance. Fix: add `is_aggressor: bool` to Order,
   set explicitly in main.rs passive/aggressor construction, change
   clear_fba_batch and throttle flushes to use that flag.
2. **Crossed book** on FBA/THROTTLED → negative avg_spread.
   Decide: fix book to resolve crosses, or report |spread| only.
3. **THROTTLED1MS ≈ CLOB** — bucket too small at crypto flow rates.
   Expected, not a bug. Consider dropping from paper.

## Paper angle
"Batched-matching interval effects on execution quality during
crypto market stress: evidence from the October 2025 crash"

Compare CLOB baseline against FBA at 10/100/200/500ms intervals using
real Binance BTCUSDT-PERP order flow for the Oct 10 21:00-22:00 UTC
crash hour, with Sep 20 same-hour as calm control. Reference HL's
HyperBFT ~200ms block cadence as the real-world anchor point.

## Quick commands
```bash
# Progress
tail -5 /opt/delta-exchange/binance-replay/crash_*.log
tail -5 /opt/delta-exchange/binance-replay/control_*.log

# Row counts
PGPASSWORD=delta_exchange_2026 psql -U delta -h localhost -d delta_exchange -t \
  -c "SELECT run_id, mechanism, COUNT(*) FROM tick_data
      WHERE run_id LIKE 'crash_btc_oct10_%' OR run_id LIKE 'control_btc_sep20_%'
      GROUP BY 1,2 ORDER BY 1,2;"

# Final analysis (run when both jobs done)
PGPASSWORD=delta_exchange_2026 psql -U delta -h localhost -d delta_exchange \
  -f /opt/delta-exchange/binance-replay/final_analysis.sql

# Are the jobs still running?
ps -ef | grep delta-binance-replay | grep -v grep
```

## Next session first moves (in order)
1. `cat SESSION_NOTES.md` (this file)
2. Check both jobs finished: `ls -la *.log` should show no growth in ~1 hour
3. Run `final_analysis.sql` and look at slippage mean/sd/p95/p99 cross-tab
4. If directional signal holds, do the is_aggressor refactor + re-run
5. Generate figures (matplotlib) from the CSV
6. Draft paper intro + methods using the taxonomy framework

## Known tech debt (deferred)
- `types.rs` + `book.rs` + `queue_mechanisms.rs` + `fba.rs` duplicated
  between `dual-engine/` and `binance-replay/`. Refactor to shared
  `delta-core` crate once both binaries are stable.
- `main.rs.bak.pre-throttle-fix` backup still in place; delete after
  next-session verification.
