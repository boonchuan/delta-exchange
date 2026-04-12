#!/usr/bin/env python3
"""
Queue Position → Fill Probability
===================================
The real value of queue position isn't price improvement — it's
getting filled vs not getting filled.

For each order at a contested price level:
1. Compute queue position (by arrival time)
2. Track whether the order gets filled or cancelled
3. Regress: P(fill) = f(queue_position)

Front-of-queue → high fill probability
Back-of-queue → low fill probability (likely cancelled before fill)
"""
import databento as db
import statistics
from collections import defaultdict

for symbol, filepath in [
    ("AAPL", "/opt/delta-exchange/databento-data/aapl_mbo_20250401.dbn.zst"),
    ("MSFT", "/opt/delta-exchange/databento-data/msft_mbo_20250401.dbn.zst"),
]:
    print(f"\n{'='*65}")
    print(f"  {symbol} — Queue Position vs Fill Probability")
    print(f"{'='*65}")

    data = db.DBNStore.from_file(filepath)
    df = data.to_df()

    # Track every order's fate: filled or cancelled
    orders = {}       # oid -> {ts, price, side}
    filled = set()    # oids that got filled
    cancelled = set() # oids that got cancelled

    for idx, row in df.iterrows():
        oid = row['order_id']
        action = row['action']
        price = row['price']

        if action == 'A':
            orders[oid] = {'ts': idx, 'price': price}
        elif action in ('T', 'F'):
            filled.add(oid)
        elif action == 'C':
            cancelled.add(oid)

    print(f"  Total orders: {len(orders):,}")
    print(f"  Filled: {len(filled):,} ({len(filled)/len(orders)*100:.1f}%)")
    print(f"  Cancelled: {len(cancelled):,} ({len(cancelled)/len(orders)*100:.1f}%)")

    # Group orders by price level
    price_queues = defaultdict(list)
    for oid, info in orders.items():
        price_queues[info['price']].append((info['ts'], oid))

    # For each price level with 3+ orders, compute queue position → fill rate
    results = []  # (normalized_queue_position, was_filled)

    for price, order_list in price_queues.items():
        if len(order_list) < 3:
            continue

        # Sort by arrival time → queue position
        sorted_orders = sorted(order_list, key=lambda x: x[0])
        n = len(sorted_orders)

        for rank, (ts, oid) in enumerate(sorted_orders):
            norm_rank = rank / (n - 1) if n > 1 else 0.5
            was_filled = 1 if oid in filled else 0
            results.append((norm_rank, was_filled))

    print(f"  Orders at contested prices: {len(results):,}")

    if not results:
        continue

    # Bin by decile
    results.sort(key=lambda x: x[0])
    n_total = len(results)
    decile_size = n_total // 10

    print(f"\n  {'Queue Decile':<20} {'N':>8} {'Fill Rate':>10} {'Description':>15}")
    print(f"  {'='*20} {'='*8} {'='*10} {'='*15}")

    decile_rates = []
    for d in range(10):
        start = d * decile_size
        end = (d + 1) * decile_size if d < 9 else n_total
        subset = results[start:end]
        fill_rate = sum(x[1] for x in subset) / len(subset) * 100
        decile_rates.append(fill_rate)
        label = 'front' if d == 0 else ('back' if d == 9 else '')
        print(f"  D{d+1:<19} {len(subset):>8,} {fill_rate:>9.2f}% {label:>15}")

    # Front vs back
    front_rate = decile_rates[0]
    back_rate = decile_rates[-1]
    ratio = front_rate / back_rate if back_rate > 0 else float('inf')

    print(f"\n  Front-of-queue fill rate: {front_rate:.2f}%")
    print(f"  Back-of-queue fill rate:  {back_rate:.2f}%")
    print(f"  Ratio (front/back):       {ratio:.2f}x")

    # OLS regression
    xs = [p[0] for p in results]
    ys = [p[1] for p in results]
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
    se = (ssr / (n_obs - 2)) ** 0.5 if n_obs > 2 else 0
    se_b = se / (ssxx ** 0.5) if ssxx > 0 else 0
    t = b / se_b if se_b > 0 else 0
    sig = "***" if abs(t) > 3.29 else "**" if abs(t) > 2.58 else "*" if abs(t) > 1.96 else "n.s."

    print(f"\n  OLS: P(fill) = {a:.4f} + {b:.4f} * QueueRank")
    print(f"  t-stat: {t:.2f} ({sig})")
    print(f"  R2: {r2:.6f}")
    print(f"  N: {n_obs:,}")

    if b < 0 and abs(t) > 1.96:
        print(f"\n  CONFIRMED: Front-of-queue orders are {ratio:.1f}x more likely to be filled.")
        print(f"  Under CLOB, speed → queue position → fill probability.")
        print(f"  Under RP, this chain would be broken.")

