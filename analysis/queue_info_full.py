#!/usr/bin/env python3
"""
Information Content of Queue Position — Full Sample
=====================================================
Expanded test using ALL contested buckets (not limited to 2000)
Also adds:
- Buy vs sell decomposition
- Top vs bottom quintile comparison
- Signed and unsigned returns
"""
import databento as db
import statistics
import math
from collections import defaultdict
import pandas as pd

for symbol, filepath in [
    ("AAPL", "/opt/delta-exchange/databento-data/aapl_mbo_20250401.dbn.zst"),
    ("MSFT", "/opt/delta-exchange/databento-data/msft_mbo_20250401.dbn.zst"),
]:
    print(f"\n{'='*65}")
    print(f"  {symbol} — Information Content (Full Sample)")
    print(f"{'='*65}")

    data = db.DBNStore.from_file(filepath)
    df = data.to_df()

    # Build trade price series for future lookups
    trades = df[df['action'].isin(['T', 'F'])][['price']].copy()
    trades.columns = ['trade_price']

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
                'submit_ts': orders[oid]['ts'],
                'exec_ts': idx,
                'price': price,
                'side': orders[oid]['side'],
            })
        elif action == 'C' and oid in orders:
            del orders[oid]

    print(f"  Total executions: {len(executions):,}")

    # Group by price + 1-second window
    buckets = defaultdict(list)
    for e in executions:
        key = (e['price'], int(e['exec_ts'].timestamp()))
        buckets[key].append(e)

    contested = {k: v for k, v in buckets.items() if len(v) >= 2}
    print(f"  Contested buckets: {len(contested):,}")

    # For each horizon, compute queue position → future return
    for horizon in [1, 5, 30]:
        pairs = []

        for key, execs in contested.items():
            ranked = sorted(execs, key=lambda e: e['submit_ts'])
            n = len(ranked)

            for rank, e in enumerate(ranked):
                norm_rank = rank / (n - 1) if n > 1 else 0.5

                target_time = e['exec_ts'] + pd.Timedelta(seconds=horizon)

                # Find closest future trade
                future_trades = trades.loc[
                    (trades.index > e['exec_ts']) & 
                    (trades.index <= target_time)
                ]

                if len(future_trades) == 0:
                    continue

                future_p = future_trades.iloc[-1]['trade_price']

                if e['side'] == 'B':
                    ret = future_p - e['price']
                else:
                    ret = e['price'] - future_p

                pairs.append((norm_rank, ret, e['side']))

        if len(pairs) < 100:
            print(f"\n  Horizon {horizon}s: insufficient data ({len(pairs)} obs)")
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

        # Quintile comparison
        front = [p[1] for p in pairs if p[0] < 0.2]
        back = [p[1] for p in pairs if p[0] > 0.8]
        front_avg = statistics.mean(front) if front else 0
        back_avg = statistics.mean(back) if back else 0

        # Buy vs sell
        buys = [(p[0], p[1]) for p in pairs if p[2] == 'B']
        sells = [(p[0], p[1]) for p in pairs if p[2] == 'A']

        def quick_ols(data):
            if len(data) < 10:
                return 0, 0
            x = [d[0] for d in data]
            y = [d[1] for d in data]
            n = len(x)
            xm2 = sum(x)/n
            ym2 = sum(y)/n
            ssxy2 = sum((a-xm2)*(b-ym2) for a,b in zip(x,y))
            ssxx2 = sum((a-xm2)**2 for a in x)
            b2 = ssxy2/ssxx2 if ssxx2 > 0 else 0
            yp2 = [ym2 + b2*(a-xm2) for a in x]
            ssr2 = sum((a-b)**2 for a,b in zip(y,yp2))
            se2 = (ssr2/(n-2))**0.5 if n > 2 else 0
            seb2 = se2/(ssxx2**0.5) if ssxx2 > 0 else 0
            t2 = b2/seb2 if seb2 > 0 else 0
            return b2, t2

        b_buy, t_buy = quick_ols(buys)
        b_sell, t_sell = quick_ols(sells)

        print(f"\n  Horizon: {horizon}s | N = {n_obs:,}")
        print(f"  ALL:   slope = {b:.6f}, t = {t:.2f} ({sig})")
        print(f"  BUYS:  slope = {b_buy:.6f}, t = {t_buy:.2f} (n={len(buys):,})")
        print(f"  SELLS: slope = {b_sell:.6f}, t = {t_sell:.2f} (n={len(sells):,})")
        print(f"  Front Q1 avg return: ${front_avg:.6f}")
        print(f"  Back  Q5 avg return: ${back_avg:.6f}")

print(f"\n{'='*65}")
print(f"  SUMMARY")
print(f"{'='*65}")
print(f"  If ALL results are n.s., Paper 2 finding:")
print(f"  'Queue position is not informative — speed does not")
print(f"   proxy for information in real NASDAQ markets.'")
