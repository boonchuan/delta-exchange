use crate::types::*;
use std::collections::BTreeMap;

pub struct OrderBook {
    pub bids: BTreeMap<i64, Vec<Order>>,
    pub asks: BTreeMap<i64, Vec<Order>>,
    pub tick_size: f64,
    pub max_depth_levels: usize,
}

impl OrderBook {
    pub fn new(tick_size: f64) -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            tick_size,
            max_depth_levels: 200,
        }
    }

    pub fn price_to_ticks(&self, price: f64) -> i64 {
        (price / self.tick_size).round() as i64
    }
    pub fn ticks_to_price(&self, ticks: i64) -> f64 {
        ticks as f64 * self.tick_size
    }

    pub fn best_bid(&self) -> Option<f64> {
        self.bids.keys().next_back().map(|t| self.ticks_to_price(*t))
    }
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.keys().next().map(|t| self.ticks_to_price(*t))
    }

    pub fn depth_at_top(&self) -> (f64, f64) {
        let bid = self.bids.iter().next_back()
            .map(|(_, os)| os.iter().map(|o| o.remaining).sum())
            .unwrap_or(0.0);
        let ask = self.asks.iter().next()
            .map(|(_, os)| os.iter().map(|o| o.remaining).sum())
            .unwrap_or(0.0);
        (bid, ask)
    }

    pub fn spread(&self) -> f64 {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => a - b,
            _ => 0.0,
        }
    }

    pub fn add_limit(&mut self, order: Order) {
        let ticks = self.price_to_ticks(order.price);
        let side = order.side;
        let book = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        book.entry(ticks).or_default().push(order);
        // keep depth bounded
        while book.len() > self.max_depth_levels {
            let key = match side {
                Side::Buy => *book.keys().next().unwrap(),
                Side::Sell => *book.keys().next_back().unwrap(),
            };
            book.remove(&key);
        }
    }

    // Match an aggressive order against the book. Returns fills.
    pub fn execute_against(&mut self, aggr: &Order, mech: Mechanism, batch_id: u64)
        -> Vec<Fill>
    {
        let mut fills = Vec::new();
        let mut remaining = aggr.remaining;
        let tick_size = self.tick_size;
        let spread_now = self.spread();
        let (db, da) = self.depth_at_top();

        let price_ticks = self.price_to_ticks(aggr.price);
        let opposite = match aggr.side {
            Side::Buy => &mut self.asks,
            Side::Sell => &mut self.bids,
        };

        let price_levels: Vec<i64> = match aggr.side {
            Side::Buy => opposite.keys().copied().collect(),
            Side::Sell => opposite.keys().rev().copied().collect(),
        };

        for pl in price_levels {
            if remaining <= 1e-12 { break; }
            let can_match = match aggr.side {
                Side::Buy => price_ticks >= pl,
                Side::Sell => price_ticks <= pl,
            };
            if !can_match { break; }

            let mut resting = match opposite.remove(&pl) {
                Some(o) => o,
                None => continue,
            };

            let mut i = 0;
            while i < resting.len() && remaining > 1e-12 {
                let fill_qty = remaining.min(resting[i].remaining);
                let fill_px = pl as f64 * tick_size;
                let latency = aggr.timestamp_us.saturating_sub(resting[i].timestamp_us);

                fills.push(Fill {
                    price: fill_px,
                    quantity: fill_qty,
                    mechanism: mech,
                    aggressor_side: aggr.side,
                    slippage: (fill_px - aggr.fair_value).abs(),
                    fair_value: aggr.fair_value,
                    latency_delta_us: latency,
                    aggressor_participant: aggr.participant,
                    batch_id,
                    trade_time_ms: aggr.timestamp_us / 1000,
                    spread: spread_now,
                    depth_bid_1: db,
                    depth_ask_1: da,
                });

                resting[i].remaining -= fill_qty;
                remaining -= fill_qty;
                if resting[i].remaining <= 1e-12 {
                    resting.remove(i);
                } else {
                    i += 1;
                }
            }
            if !resting.is_empty() {
                opposite.insert(pl, resting);
            }
        }
        fills
    }
}
