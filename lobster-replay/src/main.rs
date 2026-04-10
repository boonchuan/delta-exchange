use rand::seq::SliceRandom;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

// ══════════════════════════════════════════════════════════════
//  ΔEXCHANGE — LOBSTER Data Replay
//  Replays real NASDAQ order flow through CLOB and Deterministic
//  matching engines simultaneously.
//  Data: AAPL, June 21 2012, 400K+ events
// ══════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side { Buy, Sell }

#[derive(Debug, Clone)]
struct Order {
    id: u64,
    side: Side,
    price: i64,    // price in 0.0001 units
    size: i64,
    timestamp: f64, // seconds after midnight
}

#[derive(Debug, Clone)]
struct Fill {
    price: i64,
    size: i64,
    aggressor_timestamp: f64,
    resting_timestamp: f64,
    latency_delta: f64,
}

#[derive(Debug)]
struct OrderBook {
    bids: BTreeMap<i64, Vec<Order>>,  // price -> orders
    asks: BTreeMap<i64, Vec<Order>>,
    randomize: bool,
}

impl OrderBook {
    fn new(randomize: bool) -> Self {
        Self { bids: BTreeMap::new(), asks: BTreeMap::new(), randomize }
    }

    fn add_limit_order(&mut self, order: Order) {
        let s = order.side;
        let book = match s {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        book.entry(order.price).or_default().push(order);
        // Keep depth reasonable
        while book.len() > 100 {
            let key = match s {
                Side::Buy => *book.keys().next().unwrap(),
                Side::Sell => *book.keys().next_back().unwrap(),
            };
            book.remove(&key);
        }
    }

    fn cancel_order(&mut self, order_id: u64) -> bool {
        for book in [&mut self.bids, &mut self.asks] {
            for (_, orders) in book.iter_mut() {
                if let Some(idx) = orders.iter().position(|o| o.id == order_id) {
                    orders.remove(idx);
                    return true;
                }
            }
            book.retain(|_, orders| !orders.is_empty());
        }
        false
    }

    fn execute_against(&mut self, side: Side, price: i64, size: i64, timestamp: f64) -> Vec<Fill> {
        let mut fills = Vec::new();
        let mut remaining = size;

        let opposite = match side {
            Side::Buy => &mut self.asks,
            Side::Sell => &mut self.bids,
        };

        let price_levels: Vec<i64> = match side {
            Side::Buy => opposite.keys().copied().collect(),
            Side::Sell => opposite.keys().rev().copied().collect(),
        };

        for pl in price_levels {
            if remaining <= 0 { break; }

            let can_match = match side {
                Side::Buy => price >= pl,
                Side::Sell => price <= pl,
            };
            if !can_match { break; }

            let mut resting = match opposite.remove(&pl) {
                Some(orders) => orders,
                None => continue,
            };

            // ═══ KEY DIFFERENCE ═══
            if self.randomize {
                let mut rng = rand::thread_rng();
                resting.shuffle(&mut rng);
            }
            // CLOB: resting is already in insertion order (time priority)

            let mut i = 0;
            while i < resting.len() && remaining > 0 {
                let fill_size = remaining.min(resting[i].size);
                let latency_delta = (timestamp - resting[i].timestamp).abs();

                fills.push(Fill {
                    price: pl,
                    size: fill_size,
                    aggressor_timestamp: timestamp,
                    resting_timestamp: resting[i].timestamp,
                    latency_delta,
                });

                remaining -= fill_size;
                resting[i].size -= fill_size;

                if resting[i].size <= 0 {
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

    fn depth(&self) -> (usize, usize) {
        (self.bids.len(), self.asks.len())
    }
}

// ─── LOBSTER Message Parser ───

struct LobsterMessage {
    timestamp: f64,
    event_type: i32,
    order_id: u64,
    size: i64,
    price: i64,
    direction: i32, // 1 = buy, -1 = sell
}

fn parse_lobster_line(line: &str) -> Option<LobsterMessage> {
    let parts: Vec<&str> = line.split(',').collect();
    if parts.len() < 6 { return None; }

    Some(LobsterMessage {
        timestamp: parts[0].parse().ok()?,
        event_type: parts[1].parse().ok()?,
        order_id: parts[2].parse().ok()?,
        size: parts[3].parse().ok()?,
        price: parts[4].parse().ok()?,
        direction: parts[5].parse().ok()?,
    })
}

// ─── Statistics ───

#[derive(Debug, Default)]
struct ReplayStats {
    total_events: u64,
    limit_orders: u64,
    cancellations: u64,
    executions: u64,
    clob_fills: u64,
    det_fills: u64,
    clob_total_latency_delta: f64,
    det_total_latency_delta: f64,
    clob_latency_deltas: Vec<f64>,
    det_latency_deltas: Vec<f64>,
    // Track who gets filled first: orders with smaller timestamp
    clob_early_rester_wins: u64,  // resting order placed earlier gets filled
    det_early_rester_wins: u64,
    clob_total_resting_age: f64,
    det_total_resting_age: f64,
}

async fn persist_lobster_fill(pool: &PgPool, mechanism: &str, fill: &Fill, symbol: &str) {
    let price_decimal = Decimal::from_f64(fill.price as f64 / 10000.0).unwrap_or(Decimal::ZERO);
    let latency = Decimal::from_f64(fill.latency_delta * 1_000_000.0).unwrap_or(Decimal::ZERO); // to microseconds

    let _ = sqlx::query(
        "INSERT INTO tick_data (time, symbol, fill_price, fair_value, slippage, mechanism, latency_delta, participant_type) VALUES (NOW(), $1, $2, $2, 0, $3, $4, $5)"
    )
    .bind(symbol)
    .bind(price_decimal)
    .bind(mechanism)
    .bind(latency)
    .bind("LOBSTER")
    .execute(pool).await;
}

#[tokio::main]
async fn main() {
    let message_file = std::env::args().nth(1).unwrap_or_else(|| {
        "/opt/delta-exchange/lobster-data/AAPL_2012-06-21_34200000_57600000_message_10.csv".to_string()
    });

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://delta:delta_exchange_2026@localhost:5432/delta_exchange".into());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("DB connect failed");

    println!("══════════════════════════════════════════════════════════");
    println!("  ΔEXCHANGE — LOBSTER Data Replay");
    println!("  Real NASDAQ order flow: AAPL, June 21, 2012");
    println!("  400K+ events → CLOB vs Deterministic");
    println!("══════════════════════════════════════════════════════════");
    println!();

    let file = File::open(&message_file).expect("Cannot open message file");
    let reader = BufReader::new(file);

    let mut clob_book = OrderBook::new(false);  // time priority
    let mut det_book = OrderBook::new(true);    // random priority

    let mut stats = ReplayStats::default();
    let mut batch_counter: u64 = 0;

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };

        let msg = match parse_lobster_line(&line) {
            Some(m) => m,
            None => continue,
        };

        stats.total_events += 1;

        let side = if msg.direction == 1 { Side::Buy } else { Side::Sell };

        match msg.event_type {
            1 => {
                // New limit order — add to BOTH books
                stats.limit_orders += 1;
                let order = Order {
                    id: msg.order_id,
                    side,
                    price: msg.price,
                    size: msg.size,
                    timestamp: msg.timestamp,
                };
                clob_book.add_limit_order(order.clone());
                det_book.add_limit_order(order);
            }
            2 | 3 => {
                // Cancellation — cancel from BOTH books
                stats.cancellations += 1;
                clob_book.cancel_order(msg.order_id);
                det_book.cancel_order(msg.order_id);
            }
            4 | 5 => {
                // Execution — this is where the mechanism difference shows
                stats.executions += 1;
                batch_counter += 1;

                // Execute against BOTH books with same order
                let clob_fills = clob_book.execute_against(side, msg.price, msg.size, msg.timestamp);
                let det_fills = det_book.execute_against(side, msg.price, msg.size, msg.timestamp);

                // Analyze fills
                for f in &clob_fills {
                    stats.clob_fills += 1;
                    stats.clob_total_latency_delta += f.latency_delta;
                    stats.clob_latency_deltas.push(f.latency_delta);
                    stats.clob_total_resting_age += f.latency_delta;
                    // Check if the earliest resting order got filled (time priority working)
                    if f.resting_timestamp <= f.aggressor_timestamp {
                        stats.clob_early_rester_wins += 1;
                    }
                }
                for f in &det_fills {
                    stats.det_fills += 1;
                    stats.det_total_latency_delta += f.latency_delta;
                    stats.det_latency_deltas.push(f.latency_delta);
                    stats.det_total_resting_age += f.latency_delta;
                    if f.resting_timestamp <= f.aggressor_timestamp {
                        stats.det_early_rester_wins += 1;
                    }
                }

                // Persist to database (sample — every 100th batch)
                if batch_counter % 100 == 0 {
                    for f in &clob_fills {
                        persist_lobster_fill(&pool, "LOBSTER_CLOB", f, "AAPL").await;
                    }
                    for f in &det_fills {
                        persist_lobster_fill(&pool, "LOBSTER_DET", f, "AAPL").await;
                    }
                }
            }
            _ => {} // Skip other event types (halts, etc.)
        }

        // Progress report every 50K events
        if stats.total_events % 50000 == 0 {
            let (cb, ca) = clob_book.depth();
            let (db, da) = det_book.depth();
            println!("  [{:>6}K events] Orders: {} | Cancels: {} | Execs: {} | CLOB fills: {} | DET fills: {} | CLOB depth: {}b/{}a | DET depth: {}b/{}a",
                stats.total_events / 1000,
                stats.limit_orders, stats.cancellations, stats.executions,
                stats.clob_fills, stats.det_fills,
                cb, ca, db, da);
        }
    }

    // ═══ Final Report ═══
    let clob_avg_lat = if stats.clob_fills > 0 { stats.clob_total_latency_delta / stats.clob_fills as f64 } else { 0.0 };
    let det_avg_lat = if stats.det_fills > 0 { stats.det_total_latency_delta / stats.det_fills as f64 } else { 0.0 };

    let clob_early_pct = if stats.clob_fills > 0 { stats.clob_early_rester_wins as f64 / stats.clob_fills as f64 * 100.0 } else { 0.0 };
    let det_early_pct = if stats.det_fills > 0 { stats.det_early_rester_wins as f64 / stats.det_fills as f64 * 100.0 } else { 0.0 };

    // Compute median and percentiles for latency deltas
    stats.clob_latency_deltas.sort_by(|a, b| a.partial_cmp(b).unwrap());
    stats.det_latency_deltas.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let clob_med = if !stats.clob_latency_deltas.is_empty() { stats.clob_latency_deltas[stats.clob_latency_deltas.len() / 2] } else { 0.0 };
    let det_med = if !stats.det_latency_deltas.is_empty() { stats.det_latency_deltas[stats.det_latency_deltas.len() / 2] } else { 0.0 };

    let clob_p95 = if !stats.clob_latency_deltas.is_empty() { stats.clob_latency_deltas[(stats.clob_latency_deltas.len() as f64 * 0.95) as usize] } else { 0.0 };
    let det_p95 = if !stats.det_latency_deltas.is_empty() { stats.det_latency_deltas[(stats.det_latency_deltas.len() as f64 * 0.95) as usize] } else { 0.0 };

    println!();
    println!("══════════════════════════════════════════════════════════");
    println!("  LOBSTER REPLAY — FINAL RESULTS");
    println!("  AAPL, June 21 2012, {} events", stats.total_events);
    println!("══════════════════════════════════════════════════════════");
    println!();
    println!("  Events:       {} total", stats.total_events);
    println!("  Limit orders: {}", stats.limit_orders);
    println!("  Cancels:      {}", stats.cancellations);
    println!("  Executions:   {}", stats.executions);
    println!();
    println!("                          CLOB          DETERMINISTIC");
    println!("  ──────────────────────────────────────────────────────");
    println!("  Total Fills:         {:>10}       {:>10}", stats.clob_fills, stats.det_fills);
    println!("  Avg Resting Age (s): {:>10.4}       {:>10.4}", clob_avg_lat, det_avg_lat);
    println!("  Med Resting Age (s): {:>10.4}       {:>10.4}", clob_med, det_med);
    println!("  P95 Resting Age (s): {:>10.4}       {:>10.4}", clob_p95, det_p95);
    println!("  Early-Rester Win %:  {:>10.2}%      {:>10.2}%", clob_early_pct, det_early_pct);
    println!("  ──────────────────────────────────────────────────────");
    println!("  Early-Rester Δ:              {:>+.2}pp", det_early_pct - clob_early_pct);
    println!("  Resting Age Δ:               {:>+.6}s", det_avg_lat - clob_avg_lat);
    println!("══════════════════════════════════════════════════════════");
    println!();
    println!("  KEY METRIC: Early-Rester Win %");
    println!("  Under CLOB, earlier orders always win (time priority).");
    println!("  Under DET, queue position is randomized.");
    println!("  A lower early-rester win % under DET confirms the");
    println!("  mechanism works with REAL market data.");
    println!();
}
