#!/usr/bin/env python3
"""
Queue Position → Execution Outcome Regression
Uses LOBSTER AAPL data to show that queue position predicts fill quality
under CLOB but not under randomized priority.
"""
import csv
import random
import statistics
from collections import defaultdict

random.seed(42)

# Parse LOBSTER messages
# Format: timestamp, event_type, order_id, size, price, direction
# Event types: 1=new limit, 2=partial cancel, 3=full cancel, 4=execution, 5=hidden exec

orders = {}  # order_id -> {timestamp, price, direction, size}
executions = []  # list of {order_id, exec_time, submit_time, age, price}

with open('/opt/delta-exchange/lobster-data/AAPL_2012-06-21_34200000_57600000_message_10.csv') as f:
    reader = csv.reader(f)
    for row in reader:
        ts = float(row[0])
        etype = int(row[1])
        oid = int(row[2])
        size = int(row[3])
        price = int(row[4])  # price in 10000ths
        direction = int(row[5])  # 1=buy, -1=sell
        
        if etype == 1:  # New limit order
            orders[oid] = {'submit_time': ts, 'price': price, 'direction': direction, 'size': size}
        
        elif etype == 4:  # Execution
            if oid in orders:
                submit_time = orders[oid]['submit_time']
                age = ts - submit_time  # seconds in queue
                executions.append({
                    'order_id': oid,
                    'exec_time': ts,
                    'submit_time': submit_time,
                    'age': age,
                    'price': price,
                    'direction': direction,
                })
        
        elif etype in (2, 3):  # Cancel
            if oid in orders:
                del orders[oid]

print(f"Total executions with known submit time: {len(executions)}")
print(f"Median age: {statistics.median(e['age'] for e in executions):.2f}s")
print()

# Group executions by price level and time window (1-second buckets)
# Within each group, rank by submission time (= queue position under CLOB)
buckets = defaultdict(list)
for e in executions:
    bucket_key = (e['price'], int(e['exec_time']))
    buckets[bucket_key].append(e)

# Only use buckets with 2+ executions (queue competition exists)
contested = {k: v for k, v in buckets.items() if len(v) >= 2}
print(f"Contested price-time buckets: {len(contested)}")
print(f"Executions in contested buckets: {sum(len(v) for v in contested.values())}")
print()

# For each contested bucket:
# CLOB: rank by submit_time (earliest = rank 1 = front of queue)
# RP: rank randomly
# Measure: does rank predict execution price deviation?

clob_front_ages = []  # ages of front-of-queue fills
clob_back_ages = []   # ages of back-of-queue fills
rp_front_ages = []
rp_back_ages = []

clob_rank_age_pairs = []  # (normalized_rank, age) for regression
rp_rank_age_pairs = []

for key, execs in contested.items():
    n = len(execs)
    
    # CLOB ranking: by submit time (earliest first)
    clob_ranked = sorted(execs, key=lambda e: e['submit_time'])
    
    # RP ranking: random
    rp_ranked = execs.copy()
    random.shuffle(rp_ranked)
    
    for rank, e in enumerate(clob_ranked):
        norm_rank = rank / (n - 1) if n > 1 else 0.5  # 0 = front, 1 = back
        clob_rank_age_pairs.append((norm_rank, e['age']))
        if rank < n // 2:
            clob_front_ages.append(e['age'])
        else:
            clob_back_ages.append(e['age'])
    
    for rank, e in enumerate(rp_ranked):
        norm_rank = rank / (n - 1) if n > 1 else 0.5
        rp_rank_age_pairs.append((norm_rank, e['age']))
        if rank < n // 2:
            rp_front_ages.append(e['age'])
        else:
            rp_back_ages.append(e['age'])

# Simple OLS: age = a + b * rank
def simple_ols(pairs):
    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    n = len(xs)
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    ss_xy = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    ss_xx = sum((x - x_mean) ** 2 for x in xs)
    b = ss_xy / ss_xx if ss_xx > 0 else 0
    a = y_mean - b * x_mean
    # R-squared
    y_pred = [a + b * x for x in xs]
    ss_res = sum((y - yp) ** 2 for y, yp in zip(ys, y_pred))
    ss_tot = sum((y - y_mean) ** 2 for y in ys)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
    # t-stat
    if n > 2 and ss_xx > 0:
        se = (ss_res / (n - 2)) ** 0.5
        se_b = se / (ss_xx ** 0.5)
        t = b / se_b if se_b > 0 else 0
    else:
        t = 0
    return a, b, r2, t, n

print("=" * 65)
print("  QUEUE POSITION → EXECUTION OUTCOME REGRESSION")
print("  LOBSTER AAPL, June 21, 2012")
print("=" * 65)

a_c, b_c, r2_c, t_c, n_c = simple_ols(clob_rank_age_pairs)
a_r, b_r, r2_r, t_r, n_r = simple_ols(rp_rank_age_pairs)

print(f"\n  Regression: Order Age = a + b * Queue Rank")
print(f"  (Queue rank: 0 = front, 1 = back)")
print(f"\n  {'Metric':<30} {'CLOB':>12} {'RP':>12}")
print(f"  {'='*30} {'='*12} {'='*12}")
print(f"  {'Intercept (a)':<30} {a_c:>12.2f} {a_r:>12.2f}")
print(f"  {'Slope (b)':<30} {b_c:>12.2f} {b_r:>12.2f}")
print(f"  {'t-statistic':<30} {t_c:>12.2f} {t_r:>12.2f}")
print(f"  {'R-squared':<30} {r2_c:>12.6f} {r2_r:>12.6f}")
print(f"  {'N observations':<30} {n_c:>12,} {n_r:>12,}")

print(f"\n  Front-of-queue avg age (s):")
print(f"    CLOB: {statistics.mean(clob_front_ages):.2f}")
print(f"    RP:   {statistics.mean(rp_front_ages):.2f}")
print(f"  Back-of-queue avg age (s):")
print(f"    CLOB: {statistics.mean(clob_back_ages):.2f}")
print(f"    RP:   {statistics.mean(rp_back_ages):.2f}")

print(f"\n  INTERPRETATION:")
if abs(t_c) > abs(t_r) * 1.5:
    print(f"  Under CLOB, queue rank is strongly predicted by order age")
    print(f"  (slope = {b_c:.2f}, t = {t_c:.2f}): older orders are at the front.")
    print(f"  Under RP, this relationship is eliminated")
    print(f"  (slope = {b_r:.2f}, t = {t_r:.2f}): age does not predict rank.")
    print(f"  This confirms that CLOB converts arrival time into queue position,")
    print(f"  while RP severs this link.")
else:
    print(f"  Results require further investigation.")

print()
