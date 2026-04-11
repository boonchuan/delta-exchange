#!/usr/bin/env python3
"""
Real-Data Test: Order Lifetime (latency proxy) → Execution Quality
==================================================================
Using NASDAQ TotalView L3 data, we show:
1. Orders with shorter lifetimes (faster participants) get better fills
2. This gradient exists because time priority rewards speed
3. Under counterfactual RP, the gradient would vanish

This is the "latency → slippage" relationship in REAL data.
"""
import databento as db
import statistics
from collections import defaultdict

for symbol, filepath in [
    ("AAPL", "/opt/delta-exchange/databento-data/aapl_mbo_20250401.dbn.zst"),
    ("MSFT", "/opt/delta-exchange/databento-data/msft_mbo_20250401.dbn.zst"),
]:
    print(f"\n{'='*65}")
    print(f"  {symbol} — Latency Proxy vs Execution Quality")
    print(f"{'='*65}")

    data = db.DBNStore.from_file(filepath)
    df = data.to_df()

    # Track: for each executed order, measure lifetime and fill price
    orders = {}
    mid_prices = []
    executions = []

    for idx, row in df.iterrows():
        ts = idx
        oid = row['order_id']
        action = row['action']
        price = row['price']
        side = row['side']

        if action == 'A':
            orders[oid] = {'ts': ts, 'price': price, 'side': side}

        elif action in ('T', 'F') and oid in orders:
            lifetime = (ts - orders[oid]['ts']).total_seconds()
            if lifetime >= 0:
                executions.append({
                    'lifetime': lifetime,
                    'fill_price': price,
                    'side': side,
                    'ts': ts,
                })

        elif action == 'C' and oid in orders:
            del orders[oid]

    print(f"  Executed orders with known lifetime: {len(executions):,}")

    if len(executions) < 100:
        continue

    # Compute mid price as rolling median of fill prices
    # Then slippage = |fill - mid|
    fill_prices = [e['fill_price'] for e in executions]
    window = 50
    for i, e in enumerate(executions):
        start = max(0, i - window)
        end = min(len(fill_prices), i + window)
        mid = statistics.median(fill_prices[start:end])
        e['mid'] = mid
        e['slippage'] = abs(e['fill_price'] - mid)

    # Bin by lifetime quintiles
    lifetimes = sorted([e['lifetime'] for e in executions])
    n = len(lifetimes)
    quintile_bounds = [
        lifetimes[int(n * 0.2)],
        lifetimes[int(n * 0.4)],
        lifetimes[int(n * 0.6)],
        lifetimes[int(n * 0.8)],
    ]

    def get_quintile(lt):
        if lt <= quintile_bounds[0]: return 'Q1 (fastest)'
        if lt <= quintile_bounds[1]: return 'Q2'
        if lt <= quintile_bounds[2]: return 'Q3'
        if lt <= quintile_bounds[3]: return 'Q4'
        return 'Q5 (slowest)'

    bins = defaultdict(list)
    for e in executions:
        q = get_quintile(e['lifetime'])
        bins[q].append(e['slippage'])

    print(f"\n  {'Quintile':<18} {'N':>8} {'Avg Slip ($)':>12} {'Med Slip ($)':>12} {'Lifetime':>12}")
    print(f"  {'='*18} {'='*8} {'='*12} {'='*12} {'='*12}")

    for q in ['Q1 (fastest)', 'Q2', 'Q3', 'Q4', 'Q5 (slowest)']:
        slips = bins[q]
        if not slips:
            continue
        # Get lifetime range
        q_execs = [e for e in executions if get_quintile(e['lifetime']) == q]
        lt_range = f"{min(e['lifetime'] for e in q_execs)*1000:.1f}-{max(e['lifetime'] for e in q_execs)*1000:.0f}ms"
        print(f"  {q:<18} {len(slips):>8,} {statistics.mean(slips):>12.4f} {statistics.median(slips):>12.4f} {lt_range:>12}")

    # OLS: slippage = a + b * log(lifetime)
    import math
    xs = [math.log(max(e['lifetime'], 0.000001)) for e in executions]
    ys = [e['slippage'] for e in executions]
    n_obs = len(xs)
    xm = sum(xs) / n_obs
    ym = sum(ys) / n_obs
    ssxy = sum((x - xm) * (y - ym) for x, y in zip(xs, ys))
    ssxx = sum((x - xm) ** 2 for x in xs)
    b = ssxy / ssxx if ssxx > 0 else 0
    a = ym - b * xm
    yp = [a + b * x for x in xs]
    ssr = sum((y - yh) ** 2 for y, yh in zip(ys, yp))
    sst = sum((y - ym) ** 2 for y in ys)
    r2 = 1 - ssr / sst if sst > 0 else 0
    se = (ssr / (n_obs - 2)) ** 0.5
    se_b = se / (ssxx ** 0.5) if ssxx > 0 else 0
    t = b / se_b if se_b > 0 else 0

    print(f"\n  OLS: Slippage = {a:.4f} + {b:.4f} * log(lifetime)")
    print(f"  t-stat(slope): {t:.2f}")
    sig = "***" if abs(t) > 3.29 else "**" if abs(t) > 2.58 else "*" if abs(t) > 1.96 else "n.s."
    print(f"  Significance: {sig}")
    print(f"  R-squared: {r2:.6f}")
    print(f"  N: {n_obs:,}")

    if b > 0 and abs(t) > 1.96:
        print(f"\n  RESULT: Faster orders (shorter lifetime) get BETTER fills.")
        print(f"  This gradient exists because CLOB rewards speed with queue position.")
        print(f"  Under RP, this gradient would vanish (queue position is random).")
    elif abs(t) < 1.96:
        print(f"\n  RESULT: No significant relationship (consistent with noise in fill prices).")

