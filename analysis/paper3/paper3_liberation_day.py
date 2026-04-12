#!/usr/bin/env python3
"""
Paper 3: Order Book Dynamics During the Liberation Day Tariff Shock
====================================================================
Compare microstructure statistics between:
- April 1 (normal, pre-announcement)
- April 4 (first full crisis day)
- April 7 (peak crisis)

For 20 NASDAQ stocks with Level 3 data.
"""
import databento as db
import statistics
from collections import defaultdict
import pandas as pd
import glob, os, gc

files = sorted(glob.glob("/opt/delta-exchange/databento-data/*_mbo_*.dbn.zst"))
skip_files = ['googl_mbo_20250407', 'nvda_mbo_20250404', 'nvda_mbo_20250407']

# Only use Apr 1, Apr 4, Apr 7 (normal, crisis1, crisis2)
target_dates = ['20250401', '20250404', '20250407']

results = []

for filepath in files:
    basename = os.path.basename(filepath)
    if any(s in basename for s in skip_files):
        continue
    
    sym = basename.split("_mbo_")[0].upper()
    date = basename.split("_mbo_")[1].replace(".dbn.zst","")
    
    if date not in target_dates:
        continue
    
    print(f"  {sym} {date}...", end=" ", flush=True)
    
    try:
        data = db.DBNStore.from_file(filepath)
        df = data.to_df()
        
        if len(df) > 4000000:
            cutoff = df.index[0] + pd.Timedelta(minutes=30)
            df = df.loc[:cutoff]
        
        n_records = len(df)
        adds = df[df['action'] == 'A']
        cancels = df[df['action'] == 'C']
        trades = df[df['action'].isin(['T','F'])]
        
        n_adds = len(adds)
        n_cancels = len(cancels)
        n_trades = len(trades)
        ct_ratio = n_cancels / n_trades if n_trades > 0 else 0
        trade_pct = n_trades / n_records * 100 if n_records > 0 else 0
        
        # Order lifetime distribution
        orders = {}
        lifetimes = []
        for idx, row in df.iterrows():
            oid = row['order_id']
            if row['action'] == 'A':
                orders[oid] = idx
            elif row['action'] == 'C' and oid in orders:
                lt = (idx - orders[oid]).total_seconds() * 1000  # ms
                lifetimes.append(lt)
                del orders[oid]
            elif row['action'] in ('T','F') and oid in orders:
                lt = (idx - orders[oid]).total_seconds() * 1000
                lifetimes.append(lt)
        
        if lifetimes:
            lt_median = statistics.median(lifetimes)
            lt_p1 = sorted(lifetimes)[int(len(lifetimes) * 0.01)]
            pct_sub1ms = sum(1 for l in lifetimes if l < 1) / len(lifetimes) * 100
            pct_sub10ms = sum(1 for l in lifetimes if l < 10) / len(lifetimes) * 100
        else:
            lt_median = lt_p1 = pct_sub1ms = pct_sub10ms = 0
        
        # Queue competition
        buckets = defaultdict(int)
        for _, row in trades.iterrows():
            key = (row['price'], int(idx.timestamp()) if hasattr(idx, 'timestamp') else 0)
            buckets[key] = buckets.get(key, 0) + 1
        
        # Price range and volatility
        trade_prices = trades['price'].values
        if len(trade_prices) > 1:
            price_range = trade_prices.max() - trade_prices.min()
            price_range_pct = price_range / trade_prices.mean() * 100
            # Realized volatility (sum of squared returns)
            returns = pd.Series(trade_prices).pct_change().dropna()
            rv = (returns ** 2).sum() ** 0.5 * 100
        else:
            price_range = price_range_pct = rv = 0
        
        regime = 'Normal' if date == '20250401' else 'Crisis'
        
        results.append({
            'symbol': sym, 'date': date, 'regime': regime,
            'records': n_records, 'adds': n_adds, 'cancels': n_cancels,
            'trades': n_trades, 'ct_ratio': ct_ratio, 'trade_pct': trade_pct,
            'lt_median': lt_median, 'pct_sub1ms': pct_sub1ms,
            'pct_sub10ms': pct_sub10ms,
            'price_range_pct': price_range_pct, 'rv': rv,
        })
        
        print(f"OK ({n_records:,} records)")
        del df, data; gc.collect()
        
    except Exception as e:
        print(f"Error: {e}")
        gc.collect()

# ═══ SUMMARY ═══
print(f"\n\n{'='*70}")
print(f"  PAPER 3: Liberation Day Order Book Dynamics")
print(f"  {len(results)} stock-days across {len(set(r['symbol'] for r in results))} stocks")
print(f"{'='*70}")

# Aggregate by regime
normal = [r for r in results if r['regime'] == 'Normal']
crisis = [r for r in results if r['regime'] == 'Crisis']

def avg(data, key):
    vals = [d[key] for d in data if d[key] > 0]
    return statistics.mean(vals) if vals else 0

print(f"\n  {'Metric':<30} {'Normal (Apr 1)':>15} {'Crisis (Apr 4,7)':>15} {'Change':>10}")
print(f"  {'-'*30} {'-'*15} {'-'*15} {'-'*10}")

metrics = [
    ('Records per stock', 'records'),
    ('Trades per stock', 'trades'),
    ('Cancel/Trade ratio', 'ct_ratio'),
    ('Trade % of messages', 'trade_pct'),
    ('Median lifetime (ms)', 'lt_median'),
    ('Orders < 1ms (%)', 'pct_sub1ms'),
    ('Orders < 10ms (%)', 'pct_sub10ms'),
    ('Price range (%)', 'price_range_pct'),
    ('Realized volatility (%)', 'rv'),
]

for label, key in metrics:
    n_val = avg(normal, key)
    c_val = avg(crisis, key)
    if n_val > 0:
        change = (c_val / n_val - 1) * 100
        print(f"  {label:<30} {n_val:>15.2f} {c_val:>15.2f} {change:>+9.1f}%")
    else:
        print(f"  {label:<30} {n_val:>15.2f} {c_val:>15.2f} {'N/A':>10}")

# Per-stock detail
print(f"\n\n  DETAIL: Volume Explosion (Records)")
print(f"  {'Stock':<8} {'Apr 1':>10} {'Apr 4':>10} {'Apr 7':>10} {'Change':>10}")
print(f"  {'-'*8} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")

stocks = sorted(set(r['symbol'] for r in results))
for sym in stocks:
    apr1 = [r for r in results if r['symbol'] == sym and r['date'] == '20250401']
    apr4 = [r for r in results if r['symbol'] == sym and r['date'] == '20250404']
    apr7 = [r for r in results if r['symbol'] == sym and r['date'] == '20250407']
    
    v1 = apr1[0]['records'] if apr1 else 0
    v4 = apr4[0]['records'] if apr4 else 0
    v7 = apr7[0]['records'] if apr7 else 0
    
    if v1 > 0:
        max_crisis = max(v4, v7)
        change = (max_crisis / v1 - 1) * 100
        print(f"  {sym:<8} {v1:>10,} {v4:>10,} {v7:>10,} {change:>+9.0f}%")

print(f"\n{'='*70}")
print(f"  KEY FINDING: How did the order book change during Liberation Day?")
print(f"{'='*70}")
