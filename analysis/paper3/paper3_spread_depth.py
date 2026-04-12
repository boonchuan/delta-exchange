#!/usr/bin/env python3
"""
Paper 3 Robustness: Spread and Depth Analysis
Compute bid-ask spread and top-of-book depth for normal vs crisis days
"""
import databento as db
import numpy as np
from collections import defaultdict
import pandas as pd
import gc

test_stocks = [
    ("AAPL", "20250401", "20250404", "20250407"),
    ("MSFT", "20250401", "20250404", "20250407"),
    ("GOOGL", "20250401", "20250404", None),
    ("AMZN", "20250401", "20250404", "20250407"),
    ("TSLA", "20250401", "20250404", "20250407"),
    ("INTC", "20250401", "20250404", "20250407"),
    ("CMCSA", "20250401", "20250404", "20250407"),
    ("META", "20250401", "20250404", "20250407"),
]

print(f"{'='*70}")
print(f"  SPREAD AND DEPTH ANALYSIS: Normal vs Crisis")
print(f"{'='*70}")

all_results = []

for stock_info in test_stocks:
    sym = stock_info[0]
    dates = [d for d in stock_info[1:] if d is not None]
    
    for date in dates:
        filepath = f"/opt/delta-exchange/databento-data/{sym.lower()}_mbo_{date}.dbn.zst"
        
        try:
            data = db.DBNStore.from_file(filepath)
            df = data.to_df()
            
            if len(df) > 4000000:
                cutoff = df.index[0] + pd.Timedelta(minutes=30)
                df = df.loc[:cutoff]
            
            # Track live orders to reconstruct book
            # Simpler approach: compute spread from trade prices
            trades = df[df['action'].isin(['T', 'F'])]
            adds = df[df['action'] == 'A']
            
            # Bid-ask spread proxy: use add orders
            bids = adds[adds['side'] == 'B']['price']
            asks = adds[adds['side'] == 'A']['price']
            
            if len(bids) > 100 and len(asks) > 100:
                # Use top percentiles as best bid/ask
                best_bid = bids.quantile(0.95)
                best_ask = asks.quantile(0.05)
                mid = (best_bid + best_ask) / 2
                spread = best_ask - best_bid
                spread_bps = spread / mid * 10000 if mid > 0 else 0
                
                # Trade-based spread (consecutive trades)
                trade_prices = trades['price'].values
                if len(trade_prices) > 10:
                    # Effective spread proxy: absolute return between consecutive trades
                    returns = np.abs(np.diff(trade_prices))
                    eff_spread = np.median(returns)
                    eff_spread_bps = eff_spread / np.mean(trade_prices) * 10000
                else:
                    eff_spread_bps = 0
                
                # Depth: number of resting orders (adds - cancels - trades)
                n_adds = len(adds)
                n_cancels = len(df[df['action'] == 'C'])
                n_trades = len(trades)
                
                regime = 'Normal' if date == '20250401' else 'Crisis'
                
                all_results.append({
                    'symbol': sym, 'date': date, 'regime': regime,
                    'spread_bps': spread_bps, 'eff_spread_bps': eff_spread_bps,
                    'n_adds': n_adds, 'mid': mid,
                })
                
                print(f"  {sym} {date}: spread={spread_bps:.1f}bps, eff_spread={eff_spread_bps:.2f}bps, mid=${mid:.2f}")
            
            del data, df; gc.collect()
            
        except Exception as e:
            print(f"  {sym} {date}: Error: {e}")
            gc.collect()

# Summary
print(f"\n{'='*70}")
print(f"  SUMMARY: Spread Changes During Crisis")
print(f"{'='*70}")

normal = [r for r in all_results if r['regime'] == 'Normal']
crisis = [r for r in all_results if r['regime'] == 'Crisis']

if normal and crisis:
    n_eff = np.mean([r['eff_spread_bps'] for r in normal])
    c_eff = np.mean([r['eff_spread_bps'] for r in crisis])
    change = (c_eff / n_eff - 1) * 100 if n_eff > 0 else 0
    
    print(f"\n  Effective spread (median consecutive trade diff):")
    print(f"    Normal:  {n_eff:.2f} bps")
    print(f"    Crisis:  {c_eff:.2f} bps")
    print(f"    Change:  {change:+.1f}%")
    
    # Per stock
    print(f"\n  {'Stock':<8} {'Normal (bps)':>12} {'Crisis (bps)':>12} {'Change':>10}")
    print(f"  {'-'*8} {'-'*12} {'-'*12} {'-'*10}")
    
    stocks = sorted(set(r['symbol'] for r in all_results))
    for sym in stocks:
        n = [r for r in normal if r['symbol'] == sym]
        c = [r for r in crisis if r['symbol'] == sym]
        if n and c:
            n_val = n[0]['eff_spread_bps']
            c_val = np.mean([r['eff_spread_bps'] for r in c])
            chg = (c_val / n_val - 1) * 100 if n_val > 0 else 0
            print(f"  {sym:<8} {n_val:>12.2f} {c_val:>12.2f} {chg:>+9.0f}%")

