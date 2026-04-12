#!/usr/bin/env python3
"""
Paper 2 Robustness Tests:
1. Bucket size sensitivity (500ms, 1s, 2s)
2. Order size control
3. Adverse selection proxy (spread)
Run on 4 representative stocks: MSFT (mechanical), GOOGL (informative), 
AAPL (mixed), INTC (informative)
"""
import databento as db
import numpy as np
from collections import defaultdict
import pandas as pd
import gc

test_files = [
    ("MSFT", "/opt/delta-exchange/databento-data/msft_mbo_20250401.dbn.zst"),
    ("GOOGL", "/opt/delta-exchange/databento-data/googl_mbo_20250401.dbn.zst"),
    ("AAPL", "/opt/delta-exchange/databento-data/aapl_mbo_20250401.dbn.zst"),
    ("INTC", "/opt/delta-exchange/databento-data/intc_mbo_20250401.dbn.zst"),
]

print(f"{'='*70}")
print(f"  ROBUSTNESS TEST 1: Bucket Size Sensitivity (Apr 1, 5s horizon)")
print(f"{'='*70}")

for bucket_ms in [500, 1000, 2000]:
    print(f"\n  Bucket size: {bucket_ms}ms")
    print(f"  {'Stock':<8} {'N':>8} {'Slope':>12} {'t-stat':>8}")
    print(f"  {'-'*8} {'-'*8} {'-'*12} {'-'*8}")
    
    for sym, filepath in test_files:
        data = db.DBNStore.from_file(filepath)
        df = data.to_df()
        trades = df[df['action'].isin(['T','F'])][['price']].copy()
        trades.columns = ['trade_price']
        
        orders = {}
        executions = []
        for idx, row in df.iterrows():
            oid = row['order_id']
            action = row['action']
            if action == 'A':
                orders[oid] = {'ts': idx, 'price': row['price'], 
                              'side': row['side'], 'size': row['size']}
            elif action in ('T','F') and oid in orders:
                executions.append({
                    'submit_ts': orders[oid]['ts'], 'exec_ts': idx,
                    'price': row['price'], 'side': orders[oid]['side'],
                    'size': orders[oid].get('size', 100),
                })
            elif action == 'C' and oid in orders:
                del orders[oid]
        
        # Bucket by price + time window
        buckets = defaultdict(list)
        for e in executions:
            ts_bucket = int(e['exec_ts'].timestamp() * 1000 / bucket_ms)
            key = (e['price'], ts_bucket)
            buckets[key].append(e)
        contested = {k:v for k,v in buckets.items() if len(v) >= 2}
        
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
            print(f"  {sym:<8} skip")
            del df, data; gc.collect()
            continue
        
        xs = np.array([p[0] for p in pairs])
        ys = np.array([p[1] for p in pairs])
        n_obs = len(xs)
        xm = xs.mean(); ym = ys.mean()
        ssxy = np.sum((xs-xm)*(ys-ym))
        ssxx = np.sum((xs-xm)**2)
        b = ssxy/ssxx if ssxx > 0 else 0
        resid = ys - (ym + b*(xs-xm))
        se = np.sqrt(np.sum(resid**2)/(n_obs-2))/np.sqrt(ssxx)
        t = b/se if se > 0 else 0
        sig = "***" if abs(t) > 3.29 else "**" if abs(t) > 2.58 else "*" if abs(t) > 1.96 else ""
        
        print(f"  {sym:<8} {n_obs:>8,} {b:>12.6f} {t:>7.2f}{sig}")
        del df, data; gc.collect()

# ═══ TEST 2: Order size control ═══
print(f"\n\n{'='*70}")
print(f"  ROBUSTNESS TEST 2: Controlling for Order Size (Apr 1, 1s bucket, 5s horizon)")
print(f"{'='*70}")

for sym, filepath in test_files:
    data = db.DBNStore.from_file(filepath)
    df = data.to_df()
    trades = df[df['action'].isin(['T','F'])][['price']].copy()
    trades.columns = ['trade_price']
    
    orders = {}
    executions = []
    for idx, row in df.iterrows():
        oid = row['order_id']
        action = row['action']
        if action == 'A':
            orders[oid] = {'ts': idx, 'price': row['price'], 
                          'side': row['side'], 'size': row['size']}
        elif action in ('T','F') and oid in orders:
            executions.append({
                'submit_ts': orders[oid]['ts'], 'exec_ts': idx,
                'price': row['price'], 'side': orders[oid]['side'],
                'size': orders[oid].get('size', 100),
            })
        elif action == 'C' and oid in orders:
            del orders[oid]
    
    buckets = defaultdict(list)
    for e in executions:
        key = (e['price'], int(e['exec_ts'].timestamp()))
        buckets[key].append(e)
    contested = {k:v for k,v in buckets.items() if len(v) >= 2}
    
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
            pairs.append((norm_rank, ret, np.log(e['size']+1), n))
    
    if len(pairs) < 30:
        del df, data; gc.collect()
        continue
    
    y = np.array([p[1] for p in pairs])
    rank = np.array([p[0] for p in pairs])
    log_size = np.array([p[2] for p in pairs])
    log_depth = np.array([np.log(p[3]+1) for p in pairs])
    n_obs = len(y)
    
    # Without controls
    xm = rank.mean(); ym = y.mean()
    ssxy = np.sum((rank-xm)*(y-ym))
    ssxx = np.sum((rank-xm)**2)
    b_raw = ssxy/ssxx if ssxx > 0 else 0
    resid_raw = y - (ym + b_raw*(rank-xm))
    se_raw = np.sqrt(np.sum(resid_raw**2)/(n_obs-2))/np.sqrt(ssxx)
    t_raw = b_raw/se_raw if se_raw > 0 else 0
    
    # With controls
    X = np.column_stack([np.ones(n_obs), rank, log_size, log_depth])
    b_ctrl = np.linalg.lstsq(X, y, rcond=None)[0]
    resid_ctrl = y - X @ b_ctrl
    mse = np.sum(resid_ctrl**2)/(n_obs - X.shape[1])
    var_b = mse * np.linalg.inv(X.T @ X)
    se_ctrl = np.sqrt(np.diag(var_b))
    t_ctrl = b_ctrl/se_ctrl
    
    sig_raw = "*" if abs(t_raw) > 1.96 else ""
    sig_ctrl = "*" if abs(t_ctrl[1]) > 1.96 else ""
    
    print(f"\n  {sym}:")
    print(f"    Without controls: beta={b_raw:.6f}, t={t_raw:.2f}{sig_raw}")
    print(f"    With size+depth:  beta={b_ctrl[1]:.6f}, t={t_ctrl[1]:.2f}{sig_ctrl}")
    print(f"    Size coef:        beta={b_ctrl[2]:.6f}, t={t_ctrl[2]:.2f}")
    
    del df, data; gc.collect()

# ═══ TEST 3: Adverse selection proxy ═══
print(f"\n\n{'='*70}")
print(f"  ROBUSTNESS TEST 3: Adverse Selection Proxy (Spread)")
print(f"{'='*70}")

for sym, filepath in test_files:
    data = db.DBNStore.from_file(filepath)
    df = data.to_df()
    
    # Compute average spread from order data
    bids = df[(df['action'] == 'A') & (df['side'] == 'B')]['price']
    asks = df[(df['action'] == 'A') & (df['side'] == 'A')]['price']
    
    if len(bids) > 0 and len(asks) > 0:
        avg_bid = bids.mean()
        avg_ask = asks.mean()
        avg_mid = (avg_bid + avg_ask) / 2
        spread = avg_ask - avg_bid
        spread_bps = spread / avg_mid * 10000
        
        # Cancel-to-trade ratio (proxy for HFT activity)
        n_cancels = len(df[df['action'] == 'C'])
        n_trades = len(df[df['action'].isin(['T','F'])])
        ct_ratio = n_cancels / n_trades if n_trades > 0 else 0
        
        print(f"  {sym}: spread={spread:.4f} ({spread_bps:.1f}bps), "
              f"cancel/trade={ct_ratio:.1f}, mid=${avg_mid:.2f}")
    
    del data, df; gc.collect()

print(f"\n  Prediction: Higher spread → more informed queue position")
print(f"  GOOGL/INTC should have wider spreads or higher cancel ratios")
