#!/usr/bin/env python3
"""
Information Content of Queue Position
=======================================
Test: Do front-of-queue orders predict future price movements?

Method:
1. For each executed order, determine its queue position (by arrival time)
2. Measure the 1-second, 5-second, and 30-second price change after execution
3. Regress: future_return = a + b * queue_position

If b ≠ 0: queue position contains information (front-of-queue traders are informed)
If b = 0: queue position is purely mechanical (speed, not information)
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
    print(f"  {symbol} — Information Content of Queue Position")
    print(f"{'='*65}")

    data = db.DBNStore.from_file(filepath)
    df = data.to_df()

    # Build price series from trades
    trades = df[df['action'].isin(['T', 'F'])].copy()
    trade_prices = trades['price'].values
    trade_times = trades.index

    # Build a simple mid-price series (rolling median of recent trades)
    # For future price lookups
    price_at_time = list(zip(trade_times, trade_prices))

    def get_future_price(ts, horizon_sec):
        """Get the trade price closest to ts + horizon"""
        target = ts + __import__('pandas').Timedelta(seconds=horizon_sec)
        best = None
        best_dist = float('inf')
        for t, p in price_at_time:
            dist = abs((t - target).total_seconds())
            if dist < best_dist:
                best_dist = dist
                best = p
            if t > target and best is not None:
                break
        return best if best_dist < horizon_sec else None

    # Track orders
    orders = {}
    executions = []

    for idx, row in df.iterrows():
        oid = row['order_id']
        action = row['action']
        price = row['price']
        side = row['side']

        if action == 'A':
            orders[oid] = {'ts': idx, 'price': price, 'side': side}
        elif action in ('T', 'F') and oid in orders:
            executions.append({
                'oid': oid,
                'submit_ts': orders[oid]['ts'],
                'exec_ts': idx,
                'price': price,
                'side': orders[oid]['side'],
                'age': (idx - orders[oid]['ts']).total_seconds(),
            })
        elif action == 'C' and oid in orders:
            del orders[oid]

    print(f"  Total executions: {len(executions):,}")

    # Group by price level + 1-second window for queue position
    buckets = defaultdict(list)
    for e in executions:
        key = (e['price'], int(e['exec_ts'].timestamp()))
        buckets[key].append(e)

    contested = {k: v for k, v in buckets.items() if len(v) >= 2}
    print(f"  Contested buckets: {len(contested):,}")

    # For each contested bucket, compute queue position and future returns
    # Use 1s, 5s, 30s horizons
    for horizon in [1, 5, 30]:
        pairs = []  # (queue_rank, signed_return)

        for key, execs in contested.items():
            ranked = sorted(execs, key=lambda e: e['submit_ts'])
            n = len(ranked)

            for rank, e in enumerate(ranked):
                norm_rank = rank / (n - 1) if n > 1 else 0.5

                future_p = get_future_price(e['exec_ts'], horizon)
                if future_p is None:
                    continue

                # Signed return: positive if price moves in the direction of the trade
                if e['side'] == 'B':
                    ret = future_p - e['price']
                else:
                    ret = e['price'] - future_p

                pairs.append((norm_rank, ret))

            # Limit to first 2000 contested buckets for speed
            if len(pairs) > 8000:
                break

        if len(pairs) < 100:
            print(f"\n  Horizon {horizon}s: insufficient data")
            continue

        # OLS
        xs = [p[0] for p in pairs]
        ys = [p[1] for p in pairs]
        n_obs = len(xs)
        xm = sum(xs) / n_obs
        ym = sum(ys) / n_obs
        ssxy = sum((x - xm) * (y - ym) for x, y in zip(xs, ys))
        ssxx = sum((x - xm) ** 2 for x in xs)
        b = ssxy / ssxx if ssxx > 0 else 0
        a = ym - b * xm
        yp = [a + b * x for x in xs]
        ssr = sum((y - yh) ** 2 for y, yh in zip(ys, yp))
        se = (ssr / (n_obs - 2)) ** 0.5 if n_obs > 2 else 0
        se_b = se / (ssxx ** 0.5) if ssxx > 0 else 0
        t = b / se_b if se_b > 0 else 0
        sig = "***" if abs(t) > 3.29 else "**" if abs(t) > 2.58 else "*" if abs(t) > 1.96 else "n.s."

        # Front vs back comparison
        front = [p[1] for p in pairs if p[0] < 0.33]
        back = [p[1] for p in pairs if p[0] > 0.67]
        front_avg = statistics.mean(front) if front else 0
        back_avg = statistics.mean(back) if back else 0

        print(f"\n  Horizon: {horizon} second(s)")
        print(f"  Future Return = {a:.6f} + {b:.6f} * QueueRank")
        print(f"  t-stat: {t:.2f} ({sig})")
        print(f"  N: {n_obs:,}")
        print(f"  Front-of-queue avg return: ${front_avg:.6f}")
        print(f"  Back-of-queue avg return:  ${back_avg:.6f}")

        if abs(t) > 1.96:
            print(f"  → Queue position PREDICTS future returns ({horizon}s)")
        else:
            print(f"  → Queue position does NOT predict returns ({horizon}s)")

print(f"\n{'='*65}")
print(f"  INTERPRETATION")
print(f"{'='*65}")
print(f"  If queue position does not predict future price movements,")
print(f"  then front-of-queue orders are not more 'informed' than")
print(f"  back-of-queue orders. The queue advantage is purely")
print(f"  mechanical (speed), not informational.")
print(f"  This strengthens the case that time priority redistributes")
print(f"  value based on speed rather than rewarding superior information.")
