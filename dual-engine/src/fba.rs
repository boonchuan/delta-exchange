// Frequent Batch Auction matching (Budish, Cramton, Shim 2015)
//
// Aggregates all buy and sell orders in a batch, finds the uniform clearing
// price where aggregate demand meets aggregate supply, and executes all
// matched orders at that clearing price. Pro-rata tie-breaking at the
// clearing price.

use crate::{Order, Fill, Side, Mechanism};
use rand::seq::SliceRandom;

/// Run FBA clearing on a batch of orders. Returns all fills executed at
/// the uniform clearing price. Does not modify the resting book — FBA
/// clears every batch atomically.
pub fn clear_batch(
    orders: &mut Vec<Order>,
    mechanism: Mechanism,
    batch_id: u64,
) -> Vec<Fill> {
    if orders.is_empty() {
        return Vec::new();
    }

    // Separate buys and sells
    let mut buys: Vec<Order> = orders.iter().filter(|o| o.side == Side::Buy).cloned().collect();
    let mut sells: Vec<Order> = orders.iter().filter(|o| o.side == Side::Sell).cloned().collect();

    if buys.is_empty() || sells.is_empty() {
        return Vec::new();
    }

    // Demand schedule: sort buys descending by price (highest bid first)
    buys.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));
    // Supply schedule: sort sells ascending by price (lowest ask first)
    sells.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));

    // Find the clearing price: walk both schedules, find intersection
    // Clearing price = price where cumulative buy qty >= cumulative sell qty
    // at the highest price where this holds for both sides
    let clearing_price = find_clearing_price(&buys, &sells);

    let clearing = match clearing_price {
        Some(p) => p,
        None => return Vec::new(), // No overlap — no trade
    };

    // Collect all eligible orders: buys with price >= clearing, sells with price <= clearing
    let mut eligible_buys: Vec<Order> = buys.into_iter()
        .filter(|o| o.price >= clearing - 1e-9)
        .collect();
    let mut eligible_sells: Vec<Order> = sells.into_iter()
        .filter(|o| o.price <= clearing + 1e-9)
        .collect();

    // Randomize within-price ties (BCS specify random tie-breaking at clearing price)
    let mut rng = rand::thread_rng();
    eligible_buys.shuffle(&mut rng);
    eligible_sells.shuffle(&mut rng);

    // Compute total supply and demand at clearing
    let total_buy: f64 = eligible_buys.iter().map(|o| o.remaining).sum();
    let total_sell: f64 = eligible_sells.iter().map(|o| o.remaining).sum();
    let matched_qty = total_buy.min(total_sell);

    if matched_qty <= 0.0 {
        return Vec::new();
    }

    // Pro-rata allocation on the long side (whichever has excess)
    let (long_side, short_side, buy_ratio, sell_ratio) = if total_buy >= total_sell {
        (&mut eligible_buys, &mut eligible_sells, matched_qty / total_buy, 1.0_f64)
    } else {
        (&mut eligible_buys, &mut eligible_sells, 1.0_f64, matched_qty / total_sell)
    };

    // Execute fills: pair buys against sells greedily (already shuffled for randomness)
    // Each order fills its pro-rata share at the clearing price
    let mut fills: Vec<Fill> = Vec::new();
    let mut buy_idx = 0;
    let mut sell_idx = 0;
    let mut buy_rem = if buy_idx < long_side.len() { long_side[buy_idx].remaining * buy_ratio } else { 0.0 };
    let mut sell_rem = if sell_idx < short_side.len() { short_side[sell_idx].remaining * sell_ratio } else { 0.0 };

    while buy_idx < long_side.len() && sell_idx < short_side.len() {
        let fill_qty = buy_rem.min(sell_rem);
        if fill_qty <= 1e-9 { break; }

        let buy = &long_side[buy_idx];
        let sell = &short_side[sell_idx];

        // Aggressor = the later-arriving order (by timestamp)
        let (aggressor_side, aggressor_ts, resting_ts) = if buy.timestamp_us > sell.timestamp_us {
            (Side::Buy, buy.timestamp_us, sell.timestamp_us)
        } else {
            (Side::Sell, sell.timestamp_us, buy.timestamp_us)
        };
        let latency_delta = aggressor_ts.saturating_sub(resting_ts);

        let aggressor = if aggressor_side == Side::Buy { buy.participant } else { sell.participant };
        let slippage = (clearing - buy.fair_value).abs().min((clearing - sell.fair_value).abs());

        fills.push(Fill {
            buy_order_id: buy.id,
            sell_order_id: sell.id,
            price: clearing,
            quantity: fill_qty,
            mechanism,
            aggressor_side,
            slippage,
            fair_value: buy.fair_value,
            latency_delta_us: latency_delta,
            aggressor_participant: aggressor,
            batch_id,
        });

        buy_rem -= fill_qty;
        sell_rem -= fill_qty;

        if buy_rem <= 1e-9 {
            buy_idx += 1;
            if buy_idx < long_side.len() {
                buy_rem = long_side[buy_idx].remaining * buy_ratio;
            }
        }
        if sell_rem <= 1e-9 {
            sell_idx += 1;
            if sell_idx < short_side.len() {
                sell_rem = short_side[sell_idx].remaining * sell_ratio;
            }
        }
    }

    // Clear the batch — FBA doesn't leave resting orders
    orders.clear();
    fills
}

/// Find the uniform clearing price: the price where demand meets supply.
/// Returns None if no overlap (lowest ask > highest bid).
fn find_clearing_price(buys: &[Order], sells: &[Order]) -> Option<f64> {
    // If no overlap, no trade
    if buys[0].price < sells[0].price {
        return None;
    }

    // The clearing price is the midpoint of the highest executable ask
    // and the lowest executable bid at the marginal unit.
    // Simpler and standard: midpoint of highest bid that trades and
    // lowest ask that trades.
    let mut cum_buy: f64 = 0.0;
    let mut cum_sell: f64 = 0.0;
    let mut bi = 0;
    let mut si = 0;
    let mut last_buy_price = buys[0].price;
    let mut last_sell_price = sells[0].price;

    while bi < buys.len() && si < sells.len() {
        if buys[bi].price < sells[si].price {
            break;
        }
        if cum_buy <= cum_sell {
            cum_buy += buys[bi].remaining;
            last_buy_price = buys[bi].price;
            bi += 1;
        } else {
            cum_sell += sells[si].remaining;
            last_sell_price = sells[si].price;
            si += 1;
        }
    }

    // Clearing price = midpoint of marginal bid and marginal ask
    Some((last_buy_price + last_sell_price) / 2.0)
}

/// Micro-Batch CLOB: accumulates orders for a time window, then matches
/// them against the resting book at a uniform clearing price.
/// Unlike FBA, unfilled orders remain in the resting book (preserving fill rates).
/// Unlike CLOB, all fills within the batch get the same price (eliminating
/// the speed advantage at the price level).
///
/// This is called from the main loop when mechanism is MicroBatch.
/// It works by:
/// 1. Taking all orders in the batch
/// 2. Finding the market-clearing price (midpoint of best bid/ask after sorting)
/// 3. Matching at that uniform price instead of individual resting prices
pub fn clear_microbatch(
    orders: &mut Vec<Order>,
    mechanism: Mechanism,
    batch_id: u64,
) -> Vec<Fill> {
    if orders.is_empty() {
        return Vec::new();
    }

    let mut buys: Vec<Order> = orders.iter().filter(|o| o.side == Side::Buy).cloned().collect();
    let mut sells: Vec<Order> = orders.iter().filter(|o| o.side == Side::Sell).cloned().collect();

    if buys.is_empty() || sells.is_empty() {
        // No two-sided market — return orders as-is for book insertion
        return Vec::new();
    }

    buys.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));
    sells.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));

    // Check for overlap
    if buys[0].price < sells[0].price {
        return Vec::new(); // No overlap, orders go to book
    }

    // Clearing price = midpoint of best bid and best ask
    let clearing = (buys[0].price + sells[0].price) / 2.0;

    // Filter to executable orders
    let mut exec_buys: Vec<Order> = buys.into_iter()
        .filter(|o| o.price >= clearing - 1e-9)
        .collect();
    let mut exec_sells: Vec<Order> = sells.into_iter()
        .filter(|o| o.price <= clearing + 1e-9)
        .collect();

    // Randomize for fair allocation
    let mut rng = rand::thread_rng();
    exec_buys.shuffle(&mut rng);
    exec_sells.shuffle(&mut rng);

    let total_buy: f64 = exec_buys.iter().map(|o| o.remaining).sum();
    let total_sell: f64 = exec_sells.iter().map(|o| o.remaining).sum();
    let matched_qty = total_buy.min(total_sell);

    if matched_qty <= 0.0 {
        return Vec::new();
    }

    let buy_ratio = if total_buy >= total_sell { matched_qty / total_buy } else { 1.0 };
    let sell_ratio = if total_sell >= total_buy { matched_qty / total_sell } else { 1.0 };

    let mut fills: Vec<Fill> = Vec::new();
    let mut buy_idx = 0;
    let mut sell_idx = 0;
    let mut buy_rem = if !exec_buys.is_empty() { exec_buys[0].remaining * buy_ratio } else { 0.0 };
    let mut sell_rem = if !exec_sells.is_empty() { exec_sells[0].remaining * sell_ratio } else { 0.0 };

    while buy_idx < exec_buys.len() && sell_idx < exec_sells.len() {
        let fill_qty = buy_rem.min(sell_rem);
        if fill_qty <= 1e-9 { break; }

        let buy = &exec_buys[buy_idx];
        let sell = &exec_sells[sell_idx];

        let (aggressor_side, aggressor_ts, resting_ts) = if buy.timestamp_us > sell.timestamp_us {
            (Side::Buy, buy.timestamp_us, sell.timestamp_us)
        } else {
            (Side::Sell, sell.timestamp_us, buy.timestamp_us)
        };
        let latency_delta = aggressor_ts.saturating_sub(resting_ts);
        let aggressor = if aggressor_side == Side::Buy { buy.participant } else { sell.participant };
        let slippage = (clearing - buy.fair_value).abs().min((clearing - sell.fair_value).abs());

        fills.push(Fill {
            buy_order_id: buy.id,
            sell_order_id: sell.id,
            price: clearing,
            quantity: fill_qty,
            mechanism,
            aggressor_side,
            slippage,
            fair_value: buy.fair_value,
            latency_delta_us: latency_delta,
            aggressor_participant: aggressor,
            batch_id,
        });

        buy_rem -= fill_qty;
        sell_rem -= fill_qty;

        if buy_rem <= 1e-9 {
            buy_idx += 1;
            if buy_idx < exec_buys.len() {
                buy_rem = exec_buys[buy_idx].remaining * buy_ratio;
            }
        }
        if sell_rem <= 1e-9 {
            sell_idx += 1;
            if sell_idx < exec_sells.len() {
                sell_rem = exec_sells[sell_idx].remaining * sell_ratio;
            }
        }
    }

    // KEY DIFFERENCE FROM FBA: don't clear the batch.
    // Unmatched orders remain in `orders` for book insertion by the caller.
    // Remove only the fully filled orders.
    let filled_buy_ids: std::collections::HashSet<u64> = exec_buys.iter().map(|o| o.id).collect();
    let filled_sell_ids: std::collections::HashSet<u64> = exec_sells.iter().map(|o| o.id).collect();
    orders.retain(|o| {
        !filled_buy_ids.contains(&o.id) && !filled_sell_ids.contains(&o.id)
    });

    fills
}
