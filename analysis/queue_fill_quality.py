#!/usr/bin/env python3
"""
Queue Position → Fill Quality Test
====================================
For each execution at a contested price level:
1. Determine the order's queue position (by arrival time)
2. Measure fill quality (distance from mid-price)
3. Regress: fill_quality = a + b * queue_position

If b < 0: front-of-queue orders get better fills (CLOB rewards speed)
Under RP counterfactual: b should be zero
"""
import databento as db
import statistics
import math
from collections import defaultdict

for symbol, filepath in [
    ("AAPL", "/opt/delta-exchange/databento-data/aapl_mbo_20250401.dbn.zst"),
    ("MSFT", "/opt/delta-exchange/databento-data/msft_mbo_20250401.dbn.zst"),
]:
    print(f"\n{'='*65}")
    print(f"  {symbol} — Queue Position vs Fill Quality")
    print(f"{'='*65}")

    data = db.DBNStore.from_file(filepath)
    df = data.to_df()

    # Track orders
    orders = {}
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
            executions.append({
                'oid': oid,
                'submit_ts': orders[oid]['ts'],
                'exec_ts': ts,
                'price': price,
                'side': side,
                'age': (ts - orders[oid]['ts']).total_seconds(),
            })
        elif action == 'C' and oid in orders:
            del orders[oid]

    print(f"  Total executions: {len(executions):,}")

    # Group by price level + 1-second window
    buckets = defaultdict(list)
    for e in executions:
        key = (e['price'], int(e['exec_ts'].timestamp()))
        buckets[key].append(e)

    contested = {k: v for k, v in buckets.items() if len(v) >= 2}
    print(f"  Contested buckets: {len(contested):,}")

    # For each contested bucket, assign queue position by arrival time
    # Then measure: does earlier arrival → better execution?
    # "Better execution" = getting filled at all when others at same price also want fills
    # Use: normalized rank (0=first, 1=last) vs order age as quality proxy

    position_age_pairs = []
    position_slip_pairs = []

    # Compute rolling mid for slippage
    all_prices = sorted(set(e['price'] for e in executions))
    mid = statistics.median(e['price'] for e in executions)

    for key, execs in contested.items():
        n = len(execs)
        ranked = sorted(execs, key=lambda e: e['submit_ts'])

        bucket_mid = statistics.median(e['price'] for e in execs)

        for rank, e in enumerate(ranked):
            norm_rank = rank / (n - 1) if n > 1 else 0.5
            slip = abs(e['price'] - bucket_mid)
            position_slip_pairs.append((norm_rank, slip))
            position_age_pairs.append((norm_rank, e['age']))

    # OLS: slippage = a + b * queue_position
    def ols(pairs):
        xs = [p[0] for p in pairs]
        ys = [p[1] for p in pairs]
        n = len(xs)
        xm = sum(xs) / n
        ym = sum(ys) / n
        ssxy = sum((x - xm) * (y - ym) for x, y in zip(xs, ys))
        ssxx = sum((x - xm) ** 2 for x in xs)
        b = ssxy / ssxx if ssxx > 0 else 0
        a = ym - b * xm
        yp = [a + b * x for x in xs]
        ssr = sum((y - yh) ** 2 for y, yh in zip(ys, yp))
        sst = sum((y - ym) ** 2 for y in ys)
        r2 = 1 - ssr / sst if sst > 0 else 0
        se = (ssr / (n - 2)) ** 0.5 if n > 2 else 0
        se_b = se / (ssxx ** 0.5) if ssxx > 0 else 0
        t = b / se_b if se_b > 0 else 0
        return a, b, r2, t, n

    if position_slip_pairs:
        a, b, r2, t, n = ols(position_slip_pairs)
        sig = "***" if abs(t) > 3.29 else "**" if abs(t) > 2.58 else "*" if abs(t) > 1.96 else "n.s."
        print(f"\n  TEST 1: Queue Position -> Slippage")
        print(f"  Slippage = {a:.6f} + {b:.6f} * QueueRank")
        print(f"  t-stat: {t:.2f} ({sig})")
        print(f"  R2: {r2:.6f}")
        print(f"  N: {n:,}")

        # Front vs back comparison
        front = [p[1] for p in position_slip_pairs if p[0] < 0.33]
        back = [p[1] for p in position_slip_pairs if p[0] > 0.67]
        if front and back:
            print(f"\n  Front-of-queue (rank < 0.33): avg slip = ${statistics.mean(front):.4f}")
            print(f"  Back-of-queue  (rank > 0.67): avg slip = ${statistics.mean(back):.4f}")
            diff = statistics.mean(back) - statistics.mean(front)
            print(f"  Difference: ${diff:.4f} ({'back worse' if diff > 0 else 'front worse'})")

    if position_age_pairs:
        a2, b2, r2_2, t2, n2 = ols(position_age_pairs)
        sig2 = "***" if abs(t2) > 3.29 else "**" if abs(t2) > 2.58 else "*" if abs(t2) > 1.96 else "n.s."
        print(f"\n  TEST 2: Queue Position -> Order Age (confirmation)")
        print(f"  Age = {a2:.4f} + {b2:.4f} * QueueRank")
        print(f"  t-stat: {t2:.2f} ({sig2})")
        print(f"  N: {n2:,}")

print(f"\n{'='*65}")
print(f"  INTERPRETATION")
print(f"{'='*65}")
print(f"  If queue position predicts fill quality (Test 1), this confirms")
print(f"  that CLOB's time priority creates economically meaningful")
print(f"  differences in execution outcomes based on speed.")
print(f"  RP would eliminate this relationship by randomizing queue position.")
