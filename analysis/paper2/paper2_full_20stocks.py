#!/usr/bin/env python3
"""
Paper 2: Full 20-stock analysis
Process one file at a time to avoid memory issues
"""
import databento as db
import statistics
from collections import defaultdict
import pandas as pd
import glob, os, gc

files = sorted(glob.glob("/opt/delta-exchange/databento-data/*_mbo_*.dbn.zst"))

# Skip files that are too large (>5M records crashed before)
skip_files = ['googl_mbo_20250407', 'nvda_mbo_20250407', 'nvda_mbo_20250404']

results_5s = []

for filepath in files:
    basename = os.path.basename(filepath)
    if any(s in basename for s in skip_files):
        print(f"  SKIP {basename} (too large)")
        continue

    sym = basename.split("_mbo_")[0].upper()
    date = basename.split("_mbo_")[1].replace(".dbn.zst","")

    print(f"  {sym} {date}...", end=" ", flush=True)

    try:
        data = db.DBNStore.from_file(filepath)
        df = data.to_df()
        n_records = len(df)

        if n_records > 4000000:
            # Use only first 30 min to avoid memory issues
            cutoff = df.index[0] + pd.Timedelta(minutes=30)
            df = df.loc[:cutoff]

        trades = df[df['action'].isin(['T','F'])][['price']].copy()
        trades.columns = ['trade_price']

        orders = {}
        executions = []
        for idx, row in df.iterrows():
            oid = row['order_id']
            action = row['action']
            if action == 'A':
                orders[oid] = {'ts': idx, 'price': row['price'], 'side': row['side']}
            elif action in ('T','F') and oid in orders:
                executions.append({
                    'submit_ts': orders[oid]['ts'], 'exec_ts': idx,
                    'price': row['price'], 'side': orders[oid]['side'],
                })
            elif action == 'C' and oid in orders:
                del orders[oid]

        buckets = defaultdict(list)
        for e in executions:
            key = (e['price'], int(e['exec_ts'].timestamp()))
            buckets[key].append(e)
        contested = {k:v for k,v in buckets.items() if len(v) >= 2}

        # 5-second horizon only (main result)
        pairs = []
        for key, execs in contested.items():
            ranked = sorted(execs, key=lambda e: e['submit_ts'])
            n = len(ranked)
            for rank, e in enumerate(ranked):
                norm_rank = rank/(n-1) if n > 1 else 0.5
                target = e['exec_ts'] + pd.Timedelta(seconds=5)
                future = trades.loc[(trades.index > e['exec_ts']) & (trades.index <= target)]
                if len(future) == 0: continue
                fp = future.iloc[-1]['trade_price']
                ret = (fp - e['price']) if e['side'] == 'B' else (e['price'] - fp)
                pairs.append((norm_rank, ret))

        if len(pairs) < 30:
            print(f"skip (only {len(pairs)} pairs)")
            del df, data, orders, executions, buckets, contested, pairs
            gc.collect()
            continue

        xs = [p[0] for p in pairs]
        ys = [p[1] for p in pairs]
        n_obs = len(xs)
        xm = sum(xs)/n_obs; ym = sum(ys)/n_obs
        ssxy = sum((x-xm)*(y-ym) for x,y in zip(xs,ys))
        ssxx = sum((x-xm)**2 for x in xs)
        b = ssxy/ssxx if ssxx > 0 else 0
        a = ym - b*xm
        ssr = sum((y-(a+b*x))**2 for x,y in zip(xs,ys))
        se = (ssr/(n_obs-2))**0.5 if n_obs > 2 else 0
        seb = se/(ssxx**0.5) if ssxx > 0 else 0
        t = b/seb if seb > 0 else 0
        sig = "***" if abs(t) > 3.29 else "**" if abs(t) > 2.58 else "*" if abs(t) > 1.96 else ""

        results_5s.append({
            'symbol': sym, 'date': date, 'n': n_obs,
            'slope': b, 't': t, 'sig': sig, 'records': n_records
        })

        print(f"N={n_obs:,}, t={t:.2f}{sig}")

        del df, data, orders, executions, buckets, contested, pairs, xs, ys
        gc.collect()

    except Exception as e:
        print(f"Error: {e}")
        gc.collect()

# Summary
print(f"\n\n{'='*70}")
print(f"  PAPER 2: FULL 20-STOCK RESULTS (5-second horizon)")
print(f"{'='*70}")
print(f"\n  {'Stock':<8} {'Date':<10} {'N':>8} {'Slope':>12} {'t-stat':>8} {'Sig':>5}")
print(f"  {'-'*8} {'-'*10} {'-'*8} {'-'*12} {'-'*8} {'-'*5}")

for r in sorted(results_5s, key=lambda x: (x['symbol'], x['date'])):
    print(f"  {r['symbol']:<8} {r['date']:<10} {r['n']:>8,} {r['slope']:>12.6f} {r['t']:>8.2f} {r['sig']:>5}")

total_n = sum(r['n'] for r in results_5s)
sig_count = sum(1 for r in results_5s if r['sig'] != '')
total_tests = len(results_5s)

print(f"\n  Total N: {total_n:,}")
print(f"  Significant: {sig_count}/{total_tests} ({sig_count/total_tests*100:.1f}%)")
print(f"  Expected by chance (5%): {total_tests*0.05:.1f}")

# By stock summary
print(f"\n  {'Stock':<8} {'Tests':>6} {'Sig':>5} {'Avg t':>8} {'Verdict':<20}")
print(f"  {'-'*8} {'-'*6} {'-'*5} {'-'*8} {'-'*20}")
stock_groups = defaultdict(list)
for r in results_5s:
    stock_groups[r['symbol']].append(r)

for sym in sorted(stock_groups.keys()):
    rs = stock_groups[sym]
    n_sig = sum(1 for r in rs if r['sig'] != '')
    avg_t = statistics.mean(r['t'] for r in rs)
    if n_sig == 0:
        verdict = "MECHANICAL"
    elif n_sig >= len(rs) * 0.5:
        verdict = "INFORMATIVE"
    else:
        verdict = "MIXED"
    print(f"  {sym:<8} {len(rs):>6} {n_sig:>5} {avg_t:>8.2f} {verdict:<20}")

# Normal vs crisis
print(f"\n  NORMAL vs CRISIS")
normal = [r for r in results_5s if r['date'] == '20250401']
crisis = [r for r in results_5s if r['date'] in ['20250404','20250407']]
n_sig = sum(1 for r in normal if r['sig'] != '')
c_sig = sum(1 for r in crisis if r['sig'] != '')
print(f"  Normal (Apr 1): {n_sig}/{len(normal)} significant")
print(f"  Crisis (Apr 4,7): {c_sig}/{len(crisis)} significant")
