use crate::{Order, MatchingMode};
use rand::seq::SliceRandom;
use rand::Rng;
use rust_decimal::Decimal;
use std::collections::HashMap;

pub fn sort_queue(
    orders: &mut Vec<Order>,
    mode: MatchingMode,
    rotation_counter: u64,
) {
    match mode {
        MatchingMode::Clob => {
            orders.sort_by_key(|o| o.timestamp);
        }
        MatchingMode::Deterministic => {
            let mut rng = rand::thread_rng();
            orders.shuffle(&mut rng);
        }
        MatchingMode::ThrottledTime1ms => {
            sort_throttled_time(orders, 1);
        }
        MatchingMode::ThrottledTime10ms => {
            sort_throttled_time(orders, 10);
        }
        MatchingMode::ThrottledTime100ms => {
            sort_throttled_time(orders, 100);
        }
        MatchingMode::AgeWeighted => {
            sort_age_weighted(orders);
        }
        MatchingMode::SizeWeighted => {
            sort_size_weighted(orders);
        }
        MatchingMode::RotatingPriority => {
            sort_rotating_priority(orders, rotation_counter);
        }
        MatchingMode::RandomPerParticipant => {
            sort_random_per_participant(orders);
        }
        MatchingMode::Batch => {
            orders.sort_by_key(|o| o.timestamp);
        }
    }
}

fn sort_throttled_time(orders: &mut Vec<Order>, bucket_width_ms: i64) {
    if orders.len() <= 1 { return; }
    let mut rng = rand::thread_rng();
    orders.sort_by_key(|o| o.timestamp.timestamp_millis() / bucket_width_ms);
    let mut i = 0;
    while i < orders.len() {
        let bucket = orders[i].timestamp.timestamp_millis() / bucket_width_ms;
        let mut j = i + 1;
        while j < orders.len()
            && orders[j].timestamp.timestamp_millis() / bucket_width_ms == bucket
        { j += 1; }
        orders[i..j].shuffle(&mut rng);
        i = j;
    }
}

fn sort_age_weighted(orders: &mut Vec<Order>) {
    if orders.len() <= 1 { return; }
    let mut rng = rand::thread_rng();
    let now = chrono::Utc::now();
    let weights: Vec<f64> = orders.iter()
        .map(|o| (now - o.timestamp).num_milliseconds().max(1) as f64)
        .collect();
    weighted_shuffle(orders, &weights, &mut rng);
}

fn sort_size_weighted(orders: &mut Vec<Order>) {
    if orders.len() <= 1 { return; }
    let mut rng = rand::thread_rng();
    let weights: Vec<f64> = orders.iter()
        .map(|o| {
            use rust_decimal::prelude::ToPrimitive;
            o.remaining().to_f64().unwrap_or(1.0).max(0.001)
        })
        .collect();
    weighted_shuffle(orders, &weights, &mut rng);
}

fn sort_rotating_priority(orders: &mut Vec<Order>, rotation_counter: u64) {
    if orders.len() <= 1 { return; }
    let mut account_ids: Vec<uuid::Uuid> = orders.iter().map(|o| o.account_id).collect();
    account_ids.sort();
    account_ids.dedup();
    let n = account_ids.len();
    if n == 0 { return; }
    let rotation_idx = (rotation_counter as usize) % n;
    let mut priority_map: HashMap<uuid::Uuid, usize> = HashMap::new();
    for (pos, &aid) in account_ids.iter().cycle().skip(rotation_idx).take(n).enumerate() {
        priority_map.insert(aid, pos);
    }
    orders.sort_by(|a, b| {
        let pa = priority_map.get(&a.account_id).copied().unwrap_or(usize::MAX);
        let pb = priority_map.get(&b.account_id).copied().unwrap_or(usize::MAX);
        pa.cmp(&pb).then(a.timestamp.cmp(&b.timestamp))
    });
}

fn sort_random_per_participant(orders: &mut Vec<Order>) {
    if orders.len() <= 1 { return; }
    let mut rng = rand::thread_rng();
    let mut account_ids: Vec<uuid::Uuid> = orders.iter().map(|o| o.account_id).collect();
    account_ids.sort();
    account_ids.dedup();
    account_ids.shuffle(&mut rng);
    let priority_map: HashMap<uuid::Uuid, usize> = account_ids.iter()
        .enumerate().map(|(pos, &aid)| (aid, pos)).collect();
    orders.sort_by(|a, b| {
        let pa = priority_map.get(&a.account_id).copied().unwrap_or(usize::MAX);
        let pb = priority_map.get(&b.account_id).copied().unwrap_or(usize::MAX);
        pa.cmp(&pb).then(a.timestamp.cmp(&b.timestamp))
    });
}

fn weighted_shuffle<T>(items: &mut Vec<T>, weights: &[f64], rng: &mut impl Rng) {
    let n = items.len();
    if n <= 1 { return; }
    let mut keys: Vec<(usize, f64)> = weights.iter().enumerate()
        .map(|(i, &w)| {
            let u: f64 = rng.gen_range(0.0001_f64..1.0);
            (-u.ln() / w.max(1e-10), i)
        })
        .map(|(k, i)| (i, k))
        .collect();
    keys.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let perm: Vec<usize> = keys.iter().map(|(i, _)| *i).collect();
    apply_permutation(items, &perm);
}

fn apply_permutation<T>(items: &mut Vec<T>, perm: &[usize]) {
    let n = items.len();
    let mut done = vec![false; n];
    for i in 0..n {
        if done[i] { continue; }
        let mut j = i;
        while !done[j] {
            done[j] = true;
            let k = perm[j];
            if k != j && !done[k] { items.swap(j, k); }
            j = k;
        }
    }
}

pub struct HybridFill {
    pub order_index: usize,
    pub fill_qty: Decimal,
}

pub fn compute_hybrid_fills(
    orders: &[Order],
    incoming_qty: Decimal,
    time_pct: u8,
) -> Vec<HybridFill> {
    if orders.is_empty() || incoming_qty <= Decimal::ZERO { return vec![]; }
    let time_share = incoming_qty * Decimal::from(time_pct) / Decimal::from(100);
    let prorata_share = incoming_qty - time_share;
    let mut fills: Vec<HybridFill> = Vec::with_capacity(orders.len());
    let mut remaining_time = time_share;
    for (idx, order) in orders.iter().enumerate() {
        if remaining_time <= Decimal::ZERO { break; }
        let fill = remaining_time.min(order.remaining());
        if fill > Decimal::ZERO {
            fills.push(HybridFill { order_index: idx, fill_qty: fill });
            remaining_time -= fill;
        }
    }
    if prorata_share > Decimal::ZERO {
        let total_resting: Decimal = orders.iter().map(|o| o.remaining()).sum();
        if total_resting > Decimal::ZERO {
            let mut remaining_prorata = prorata_share;
            for (idx, order) in orders.iter().enumerate() {
                let share = prorata_share * order.remaining() / total_resting;
                let fill = share.min(order.remaining()).min(remaining_prorata);
                if fill > Decimal::ZERO {
                    if let Some(entry) = fills.iter_mut().find(|f| f.order_index == idx) {
                        entry.fill_qty += fill;
                    } else {
                        fills.push(HybridFill { order_index: idx, fill_qty: fill });
                    }
                    remaining_prorata -= fill;
                }
            }
        }
    }
    fills
}
