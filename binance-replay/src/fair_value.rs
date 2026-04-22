use std::collections::VecDeque;

/// Rolling volume-weighted price over the last `window_ms` of trades.
pub struct FairValue {
    window_ms: u64,
    trades: VecDeque<(u64, f64, f64)>, // (time_ms, price, qty)
    sum_pq: f64,
    sum_q: f64,
}

impl FairValue {
    pub fn new(window_ms: u64) -> Self {
        Self { window_ms, trades: VecDeque::new(), sum_pq: 0.0, sum_q: 0.0 }
    }

    pub fn update(&mut self, time_ms: u64, price: f64, qty: f64) {
        self.trades.push_back((time_ms, price, qty));
        self.sum_pq += price * qty;
        self.sum_q += qty;
        let cutoff = time_ms.saturating_sub(self.window_ms);
        while let Some(&(t, p, q)) = self.trades.front() {
            if t < cutoff {
                self.sum_pq -= p * q;
                self.sum_q -= q;
                self.trades.pop_front();
            } else { break; }
        }
    }

    pub fn value(&self, fallback: f64) -> f64 {
        if self.sum_q > 1e-9 { self.sum_pq / self.sum_q } else { fallback }
    }
}
