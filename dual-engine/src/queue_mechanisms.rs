use crate::{Order, Mechanism};
use rand::seq::SliceRandom;
use rand::Rng;
use std::collections::HashMap;

pub fn sort_queue(orders: &mut Vec<Order>, mechanism: Mechanism) {
    match mechanism {
        Mechanism::Clob => { orders.sort_by_key(|o| o.timestamp_us); }
        Mechanism::Deterministic => { let mut rng = rand::thread_rng(); orders.shuffle(&mut rng); }
        Mechanism::ThrottledTime1ms => sort_throttled(orders, 1000),
        Mechanism::ThrottledTime10ms => sort_throttled(orders, 10000),
        Mechanism::ThrottledTime100ms => sort_throttled(orders, 100000),
        Mechanism::AgeWeighted => sort_age_weighted(orders),
        Mechanism::SizeWeighted => sort_size_weighted(orders),
        Mechanism::RotatingPriority => sort_rotating(orders),
        Mechanism::RandomPerParticipant => sort_per_participant(orders),
    }
}

fn sort_throttled(orders: &mut Vec<Order>, bucket_us: u64) {
    if orders.len() <= 1 { return; }
    let mut rng = rand::thread_rng();
    orders.sort_by_key(|o| o.timestamp_us / bucket_us);
    let mut i = 0;
    while i < orders.len() {
        let bucket = orders[i].timestamp_us / bucket_us;
        let mut j = i + 1;
        while j < orders.len() && orders[j].timestamp_us / bucket_us == bucket { j += 1; }
        orders[i..j].shuffle(&mut rng);
        i = j;
    }
}

fn sort_age_weighted(orders: &mut Vec<Order>) {
    if orders.len() <= 1 { return; }
    let mut rng = rand::thread_rng();
    let max_ts = orders.iter().map(|o| o.timestamp_us).max().unwrap_or(1);
    let weights: Vec<f64> = orders.iter().map(|o| ((max_ts - o.timestamp_us) as f64 + 1.0)).collect();
    weighted_shuffle(orders, &weights, &mut rng);
}

fn sort_size_weighted(orders: &mut Vec<Order>) {
    if orders.len() <= 1 { return; }
    let mut rng = rand::thread_rng();
    let weights: Vec<f64> = orders.iter().map(|o| o.remaining.max(0.001)).collect();
    weighted_shuffle(orders, &weights, &mut rng);
}

fn sort_rotating(orders: &mut Vec<Order>) {
    if orders.len() <= 1 { return; }
    let mut ids: Vec<u64> = orders.iter().map(|o| o.id).collect();
    ids.sort();
    ids.dedup();
    let n = ids.len();
    if n == 0 { return; }
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let rot = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as usize % n;
    let mut pmap: HashMap<u64, usize> = HashMap::new();
    for (pos, id) in ids.iter().cycle().skip(rot).take(n).enumerate() {
        pmap.insert(*id, pos);
    }
    orders.sort_by(|a, b| {
        let pa = pmap.get(&a.id).copied().unwrap_or(usize::MAX);
        let pb = pmap.get(&b.id).copied().unwrap_or(usize::MAX);
        pa.cmp(&pb).then(a.timestamp_us.cmp(&b.timestamp_us))
    });
}

fn sort_per_participant(orders: &mut Vec<Order>) {
    if orders.len() <= 1 { return; }
    let mut rng = rand::thread_rng();
    let ptype_key = |p: &crate::ParticipantType| -> u8 {
        match p {
            crate::ParticipantType::HFT => 0,
            crate::ParticipantType::MarketMaker => 1,
            crate::ParticipantType::Institutional => 2,
            crate::ParticipantType::Retail => 3,
        }
    };
    let mut ptypes: Vec<u8> = orders.iter().map(|o| ptype_key(&o.participant)).collect();
    ptypes.sort();
    ptypes.dedup();
    ptypes.shuffle(&mut rng);
    let pmap: HashMap<u8, usize> = ptypes.iter().enumerate().map(|(i, p)| (*p, i)).collect();
    orders.sort_by(|a, b| {
        let pa = pmap.get(&ptype_key(&a.participant)).copied().unwrap_or(usize::MAX);
        let pb = pmap.get(&ptype_key(&b.participant)).copied().unwrap_or(usize::MAX);
        pa.cmp(&pb).then(a.timestamp_us.cmp(&b.timestamp_us))
    });
}

fn weighted_shuffle<T>(items: &mut Vec<T>, weights: &[f64], rng: &mut impl Rng) {
    if items.len() <= 1 { return; }
    let mut keys: Vec<(usize, f64)> = weights.iter().enumerate()
        .map(|(i, w)| (i, -rng.gen_range(0.0001_f64..1.0_f64).ln() / w.max(1e-10)))
        .collect();
    keys.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let perm: Vec<usize> = keys.iter().map(|(i, _)| *i).collect();
    let n = items.len();
    let mut done = vec![false; n];
    for i in 0..n {
        if done[i] { continue; }
        let mut j = i;
        loop {
            done[j] = true;
            let k = perm[j];
            if k == j || done[k] { break; }
            items.swap(j, k);
            j = k;
        }
    }
}

/// Generic throttled time priority with arbitrary bucket width in microseconds
pub fn sort_throttled_custom(orders: &mut Vec<Order>, bucket_us: u64) {
    sort_throttled(orders, bucket_us);
}
