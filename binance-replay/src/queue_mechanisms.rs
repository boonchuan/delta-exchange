// Queue sorting mechanisms for FBA batch clearing.
// Adapted from dual-engine/src/queue_mechanisms.rs.

use crate::types::{Order, Mechanism};
use rand::seq::SliceRandom;

pub fn sort_queue(orders: &mut Vec<Order>, mechanism: Mechanism) {
    match mechanism {
        Mechanism::Clob => {
            orders.sort_by_key(|o| o.timestamp_us);
        }
        Mechanism::ThrottledTime1ms => sort_throttled(orders, 1_000),
        Mechanism::ThrottledTime10ms => sort_throttled(orders, 10_000),
        Mechanism::ThrottledTime100ms => sort_throttled(orders, 100_000),
        Mechanism::Fba10ms
        | Mechanism::Fba100ms
        | Mechanism::Fba200ms
        | Mechanism::Fba500ms => {
            // FBA batches don't need pre-sort; clear_batch handles it
        }
    }
}

fn sort_throttled(orders: &mut Vec<Order>, bucket_us: u64) {
    if orders.len() <= 1 {
        return;
    }
    let mut rng = rand::thread_rng();
    orders.sort_by_key(|o| o.timestamp_us / bucket_us);
    let mut i = 0;
    while i < orders.len() {
        let bucket = orders[i].timestamp_us / bucket_us;
        let mut j = i + 1;
        while j < orders.len() && orders[j].timestamp_us / bucket_us == bucket {
            j += 1;
        }
        orders[i..j].shuffle(&mut rng);
        i = j;
    }
}
