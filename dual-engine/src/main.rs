use chrono::{DateTime, Utc};
use rand::seq::SliceRandom;
use rand::Rng;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::BTreeMap;
use std::time::Duration;
use uuid::Uuid;

// ══════════════════════════════════════════════════════════════
//  ΔEXCHANGE — Dual Engine Comparison Simulator
//  Runs IDENTICAL order flow through CLOB and Deterministic
//  matching simultaneously, measuring the difference.
//  This is the core empirical contribution for the paper.
// ══════════════════════════════════════════════════════════════

// ─── Types ───

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side { Buy, Sell }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mechanism { Clob, Deterministic }

#[derive(Debug, Clone)]
struct Order {
    id: u64,
    side: Side,
    price: f64,
    quantity: f64,
    remaining: f64,
    timestamp_us: u64, // microsecond arrival time — matters for CLOB
    participant: ParticipantType,
    fair_value: f64,
}

#[derive(Debug, Clone)]
struct Fill {
    buy_order_id: u64,
    sell_order_id: u64,
    price: f64,
    quantity: f64,
    mechanism: Mechanism,
    aggressor_side: Side,
    slippage: f64,       // |fill_price - fair_value|
    fair_value: f64,
    latency_delta_us: u64,
    aggressor_participant: ParticipantType,
    batch_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParticipantType { HFT, MarketMaker, Institutional, Retail }

impl ParticipantType {
    fn latency_us(&self) -> u64 {
        let mut rng = rand::thread_rng();
        match self {
            Self::HFT => rng.gen_range(1..10),
            Self::MarketMaker => rng.gen_range(10..200),
            Self::Institutional => rng.gen_range(500..5000),
            Self::Retail => rng.gen_range(1000..10000),
        }
    }

    fn spread_factor(&self) -> f64 {
        match self {
            Self::HFT => 0.02,
            Self::MarketMaker => 0.05,
            Self::Institutional => 0.15,
            Self::Retail => 0.25,
        }
    }

    fn name(&self) -> &str {
        match self {
            Self::HFT => "HFT",
            Self::MarketMaker => "MM",
            Self::Institutional => "INST",
            Self::Retail => "RETAIL",
        }
    }
}

// ─── Order Book (used for both engines) ───

#[derive(Debug, Clone)]
struct OrderBook {
    bids: BTreeMap<i64, Vec<Order>>, // price_ticks -> orders
    asks: BTreeMap<i64, Vec<Order>>,
    tick_size: f64,
}

impl OrderBook {
    fn new(tick_size: f64) -> Self {
        Self { bids: BTreeMap::new(), asks: BTreeMap::new(), tick_size }
    }

    fn price_to_ticks(&self, price: f64) -> i64 {
        (price / self.tick_size).round() as i64
    }

    fn ticks_to_price(&self, ticks: i64) -> f64 {
        ticks as f64 * self.tick_size
    }

    fn best_bid(&self) -> Option<f64> {
        self.bids.keys().next_back().map(|t| self.ticks_to_price(*t))
    }

    fn best_ask(&self) -> Option<f64> {
        self.asks.keys().next().map(|t| self.ticks_to_price(*t))
    }

    fn match_order(&mut self, order: &mut Order, mechanism: Mechanism, batch_id: u64) -> Vec<Fill> {
        let mut fills = Vec::new();

        let opposite = match order.side {
            Side::Buy => &mut self.asks,
            Side::Sell => &mut self.bids,
        };

        let price_levels: Vec<i64> = match order.side {
            Side::Buy => opposite.keys().copied().collect(),
            Side::Sell => opposite.keys().rev().copied().collect(),
        };

        for price_ticks in price_levels {
            if order.remaining <= 0.0 { break; }

            let price = price_ticks as f64 * self.tick_size;
            let can_match = match order.side {
                Side::Buy => order.price >= price,
                Side::Sell => order.price <= price,
            };
            if !can_match { break; }

            let mut resting = match opposite.remove(&price_ticks) {
                Some(orders) => orders,
                None => continue,
            };

            // ═══ KEY DIFFERENCE BETWEEN MECHANISMS ═══
            match mechanism {
                Mechanism::Clob => {
                    // CLOB: sort by arrival time (time priority)
                    resting.sort_by_key(|o| o.timestamp_us);
                }
                Mechanism::Deterministic => {
                    // DETERMINISTIC: random shuffle (eliminates time priority)
                    let mut rng = rand::thread_rng();
                    resting.shuffle(&mut rng);
                }
            }

            let mut i = 0;
            while i < resting.len() && order.remaining > 0.0 {
                let fill_qty = order.remaining.min(resting[i].remaining);

                // Fill price: resting order price for both (fair comparison)
                let fill_price = resting[i].price;

                // Slippage: deviation from fair value at time of fill
                let slippage = (fill_price - order.fair_value).abs();

                // Latency delta between the two participants
                let latency_delta = if order.timestamp_us > resting[i].timestamp_us {
                    order.timestamp_us - resting[i].timestamp_us
                } else {
                    resting[i].timestamp_us - order.timestamp_us
                };

                let fill = Fill {
                    buy_order_id: if order.side == Side::Buy { order.id } else { resting[i].id },
                    sell_order_id: if order.side == Side::Sell { order.id } else { resting[i].id },
                    price: fill_price,
                    quantity: fill_qty,
                    mechanism,
                    aggressor_side: order.side,
                    slippage,
                    fair_value: order.fair_value,
                    latency_delta_us: latency_delta,
                    batch_id,
                    aggressor_participant: order.participant,
                };

                order.remaining -= fill_qty;
                resting[i].remaining -= fill_qty;

                if resting[i].remaining <= 0.0 {
                    resting.remove(i);
                } else {
                    i += 1;
                }

                fills.push(fill);
            }

            if !resting.is_empty() {
                opposite.insert(price_ticks, resting);
            }
        }

        // Add unfilled remainder to book
        if order.remaining > 0.0 {
            let ticks = self.price_to_ticks(order.price);
            let book = match order.side {
                Side::Buy => &mut self.bids,
                Side::Sell => &mut self.asks,
            };
            book.entry(ticks).or_default().push(order.clone());
            // Keep book depth reasonable
            while book.len() > 50 {
                let key = match order.side {
                    Side::Buy => *book.keys().next().unwrap(),
                    Side::Sell => *book.keys().next_back().unwrap(),
                };
                book.remove(&key);
            }
        }

        fills
    }
}

// ─── Instrument ───

#[derive(Debug, Clone)]
struct Instrument {
    symbol: String,
    fair_price: f64,
    volatility: f64,
    tick_size: f64,
    current_price: f64,
}

impl Instrument {
    fn tick_price(&mut self) {
        let dt: f64 = 1.0 / (252.0 * 6.5 * 3600.0);
        let mut rng = rand::thread_rng();
        let drift = (rng.gen::<f64>() - 0.501) * self.volatility * dt.sqrt() * 3.0;
        let u: f64 = rng.gen_range(0.0001..1.0);
        let v: f64 = rng.gen_range(0.0001..1.0);
        let dw = (-2.0 * u.ln()).sqrt() * (2.0 * std::f64::consts::PI * v).cos();
        let change = self.current_price * (drift + self.volatility * dt.sqrt() * dw);
        self.current_price = ((self.current_price + change) / self.tick_size).round() * self.tick_size;
        self.current_price = self.current_price.max(self.tick_size);
    }
}

// ─── Comparison Results ───

#[derive(Debug, Default)]
struct ComparisonStats {
    clob_fills: u64,
    det_fills: u64,
    clob_total_slippage: f64,
    det_total_slippage: f64,
    clob_latency_rents: f64,
    det_latency_rents: f64,
    clob_hft_wins: u64,
    det_hft_wins: u64,
    clob_slippage_values: Vec<f64>,
    det_slippage_values: Vec<f64>,
}

impl ComparisonStats {
    fn clob_avg_slippage(&self) -> f64 {
        if self.clob_fills == 0 { 0.0 } else { self.clob_total_slippage / self.clob_fills as f64 }
    }

    fn det_avg_slippage(&self) -> f64 {
        if self.det_fills == 0 { 0.0 } else { self.det_total_slippage / self.det_fills as f64 }
    }

    fn slippage_reduction_pct(&self) -> f64 {
        let clob = self.clob_avg_slippage();
        if clob == 0.0 { 0.0 } else { (1.0 - self.det_avg_slippage() / clob) * 100.0 }
    }

    fn clob_slippage_variance(&self) -> f64 {
        variance(&self.clob_slippage_values)
    }

    fn det_slippage_variance(&self) -> f64 {
        variance(&self.det_slippage_values)
    }

    fn variance_reduction_pct(&self) -> f64 {
        let cv = self.clob_slippage_variance();
        if cv == 0.0 { 0.0 } else { (1.0 - self.det_slippage_variance() / cv) * 100.0 }
    }
}

fn variance(data: &[f64]) -> f64 {
    if data.len() < 2 { return 0.0; }
    let mean = data.iter().sum::<f64>() / data.len() as f64;
    data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (data.len() - 1) as f64
}

fn stddev(data: &[f64]) -> f64 {
    variance(data).sqrt()
}

// ─── Database Persistence ───

async fn persist_comparison(pool: &PgPool, symbol: &str, clob_fills: &[Fill], det_fills: &[Fill]) {
    for fill in clob_fills {
        let mech = "CLOB";
        let ptype = fill.aggressor_participant.name();
        let _ = sqlx::query(
            "INSERT INTO tick_data (time, symbol, fill_price, fair_value, slippage, mechanism, batch_id, latency_delta, participant_type) VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7, $8)"
        )
        .bind(symbol)
        .bind(Decimal::from_f64(fill.price).unwrap_or(Decimal::ZERO))
        .bind(Decimal::from_f64(fill.fair_value).unwrap_or(Decimal::ZERO))
        .bind(Decimal::from_f64(fill.slippage).unwrap_or(Decimal::ZERO))
        .bind(mech)
        .bind(fill.batch_id as i64)
        .bind(Decimal::from_f64(fill.latency_delta_us as f64).unwrap_or(Decimal::ZERO))
        .bind(ptype)
        .execute(pool).await.ok();
    }

    for fill in det_fills {
        let mech = "DETERMINISTIC";
        let ptype = fill.aggressor_participant.name();
        let _ = sqlx::query(
            "INSERT INTO tick_data (time, symbol, fill_price, fair_value, slippage, mechanism, batch_id, latency_delta, participant_type) VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7, $8)"
        )
        .bind(symbol)
        .bind(Decimal::from_f64(fill.price).unwrap_or(Decimal::ZERO))
        .bind(Decimal::from_f64(fill.fair_value).unwrap_or(Decimal::ZERO))
        .bind(Decimal::from_f64(fill.slippage).unwrap_or(Decimal::ZERO))
        .bind(mech)
        .bind(fill.batch_id as i64)
        .bind(Decimal::from_f64(fill.latency_delta_us as f64).unwrap_or(Decimal::ZERO))
        .bind(ptype)
        .execute(pool).await.ok();
    }
}

async fn persist_comparison_summary(pool: &PgPool, symbol: &str, stats: &ComparisonStats) {
    let _ = sqlx::query(
        "INSERT INTO engine_comparison (time_bucket, symbol, det_avg_slippage, clob_avg_slippage, det_fill_count, clob_fill_count, det_price_variance, clob_price_variance, slippage_improvement_pct) VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7, $8)"
    )
    .bind(symbol)
    .bind(Decimal::from_f64(stats.det_avg_slippage()).unwrap_or(Decimal::ZERO))
    .bind(Decimal::from_f64(stats.clob_avg_slippage()).unwrap_or(Decimal::ZERO))
    .bind(stats.det_fills as i32)
    .bind(stats.clob_fills as i32)
    .bind(Decimal::from_f64(stats.det_slippage_variance()).unwrap_or(Decimal::ZERO))
    .bind(Decimal::from_f64(stats.clob_slippage_variance()).unwrap_or(Decimal::ZERO))
    .bind(Decimal::from_f64(stats.slippage_reduction_pct()).unwrap_or(Decimal::ZERO))
    .execute(pool).await.ok();
}

// ─── Main Simulation ───

#[tokio::main]
async fn main() {
    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://delta:delta_exchange_2026@localhost:5432/delta_exchange".into());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("DB connect failed");

    println!("══════════════════════════════════════════════════════════");
    println!("  ΔEXCHANGE — Dual Engine Comparison Simulator");
    println!("  CLOB vs DETERMINISTIC — Side by Side");
    println!("══════════════════════════════════════════════════════════");
    println!("  Persisting to TimescaleDB for analysis");
    println!();

    let instruments = vec![
        Instrument { symbol: "DETX".into(), fair_price: 4285.5, volatility: 0.18, tick_size: 0.25, current_price: 4285.5 },
        Instrument { symbol: "DTEC".into(), fair_price: 890.25, volatility: 0.24, tick_size: 0.25, current_price: 890.25 },
        Instrument { symbol: "DFIN".into(), fair_price: 542.75, volatility: 0.16, tick_size: 0.25, current_price: 542.75 },
        Instrument { symbol: "DCRUDF".into(), fair_price: 78.42, volatility: 0.32, tick_size: 0.01, current_price: 78.42 },
        Instrument { symbol: "DGOLDF".into(), fair_price: 2342.8, volatility: 0.14, tick_size: 0.10, current_price: 2342.8 },
        Instrument { symbol: "DBTCP".into(), fair_price: 67480.0, volatility: 0.55, tick_size: 0.50, current_price: 67480.0 },
        Instrument { symbol: "DETHP".into(), fair_price: 3520.0, volatility: 0.52, tick_size: 0.10, current_price: 3520.0 },
        Instrument { symbol: "DEURUSD".into(), fair_price: 1.0852, volatility: 0.08, tick_size: 0.0001, current_price: 1.0852 },
    ];

    let cycle_ms: u64 = std::env::var("CYCLE_MS")
        .unwrap_or_else(|_| "200".into())
        .parse().unwrap_or(200);

    let mut order_id_counter: u64 = 1;
    let mut batch_counter: u64 = 1;
    let mut cycle_count: u64 = 0;

    // Separate orderbooks for each mechanism × instrument
    let mut clob_books: Vec<OrderBook> = instruments.iter()
        .map(|i| OrderBook::new(i.tick_size)).collect();
    let mut det_books: Vec<OrderBook> = instruments.iter()
        .map(|i| OrderBook::new(i.tick_size)).collect();

    let mut instruments = instruments;
    let mut global_stats = ComparisonStats::default();

    let participants = [
        ParticipantType::HFT,
        ParticipantType::HFT,
        ParticipantType::HFT,
        ParticipantType::HFT,
        ParticipantType::HFT,
        ParticipantType::HFT,
        ParticipantType::HFT,
        ParticipantType::HFT,
        ParticipantType::HFT,
        ParticipantType::HFT,
        ParticipantType::HFT,
        ParticipantType::Institutional,
        ParticipantType::Institutional,
        ParticipantType::Institutional,
        ParticipantType::Retail,
        ParticipantType::Retail,
        ParticipantType::Retail,
    ];

    println!("  Starting dual-engine simulation...");
    println!();

    loop {
        let mut rng = rand::thread_rng();

        // Tick all prices
        for inst in &mut instruments {
            inst.tick_price();
        }

        // Pick 2-5 instruments per cycle
        let num_active = rng.gen_range(2..=5).min(instruments.len());
        let mut indices: Vec<usize> = (0..instruments.len()).collect();
        indices.shuffle(&mut rng);
        indices.truncate(num_active);

        for &idx in &indices {
            let inst = &instruments[idx];
            let batch_id = batch_counter;
            batch_counter += 1;

            // Generate 2-6 orders per instrument
            let num_orders = rng.gen_range(6..=15);
            let mut orders: Vec<Order> = Vec::new();

            for _ in 0..num_orders {
                let participant = participants[rng.gen_range(0..participants.len())];
                let side = if rng.gen::<f64>() > 0.5 { Side::Buy } else { Side::Sell };
                let spread = inst.tick_size * (1.0 + participant.spread_factor() * 3.0);
                let u: f64 = rng.gen_range(0.0001..1.0);
                let v: f64 = rng.gen_range(0.0001..1.0);
                let noise = (-2.0 * u.ln()).sqrt() * (2.0 * std::f64::consts::PI * v).cos() * spread;

                let price = match side {
                    Side::Buy => inst.current_price - spread / 2.0 + noise,
                    Side::Sell => inst.current_price + spread / 2.0 + noise,
                };
                let price = ((price / inst.tick_size).round() * inst.tick_size).max(inst.tick_size);
                let qty = rng.gen_range(1.0_f64..5.0_f64).ceil();
                let timestamp_us = participant.latency_us();

                order_id_counter += 1;
                orders.push(Order {
                    id: order_id_counter,
                    side,
                    price,
                    quantity: qty,
                    remaining: qty,
                    timestamp_us,
                    participant,
                    fair_value: inst.current_price,
                });
            }

            // ═══ Run IDENTICAL orders through BOTH engines ═══

            let mut clob_orders = orders.clone();
            let mut det_orders = orders.clone();

            let mut clob_fills = Vec::new();
            let mut det_fills = Vec::new();

            for order in &mut clob_orders {
                let fills = clob_books[idx].match_order(order, Mechanism::Clob, batch_id);
                clob_fills.extend(fills);
            }

            for order in &mut det_orders {
                let fills = det_books[idx].match_order(order, Mechanism::Deterministic, batch_id);
                det_fills.extend(fills);
            }

            // Accumulate stats
            for f in &clob_fills {
                global_stats.clob_fills += 1;
                global_stats.clob_total_slippage += f.slippage;
                global_stats.clob_slippage_values.push(f.slippage);
                if f.latency_delta_us < 50 { global_stats.clob_hft_wins += 1; }
            }
            for f in &det_fills {
                global_stats.det_fills += 1;
                global_stats.det_total_slippage += f.slippage;
                global_stats.det_slippage_values.push(f.slippage);
                if f.latency_delta_us < 50 { global_stats.det_hft_wins += 1; }
            }

            // Persist to database (every batch)
            if !clob_fills.is_empty() || !det_fills.is_empty() {
                let p = pool.clone();
                let sym = inst.symbol.clone();
                let cf = clob_fills.clone();
                let df = det_fills.clone();
                tokio::spawn(async move {
                    persist_comparison(&p, &sym, &cf, &df).await;
                });
            }
        }

        cycle_count += 1;

        // Print comparison report every 60 seconds
        if cycle_count % (60_000 / cycle_ms) == 0 {
            // Keep memory bounded
            if global_stats.clob_slippage_values.len() > 100_000 {
                let drain = global_stats.clob_slippage_values.len() - 50_000;
                global_stats.clob_slippage_values.drain(0..drain);
            }
            if global_stats.det_slippage_values.len() > 100_000 {
                let drain = global_stats.det_slippage_values.len() - 50_000;
                global_stats.det_slippage_values.drain(0..drain);
            }

            // Persist summary
            let p = pool.clone();
            let gs = ComparisonStats {
                clob_fills: global_stats.clob_fills,
                det_fills: global_stats.det_fills,
                clob_total_slippage: global_stats.clob_total_slippage,
                det_total_slippage: global_stats.det_total_slippage,
                clob_latency_rents: global_stats.clob_latency_rents,
                det_latency_rents: global_stats.det_latency_rents,
                clob_hft_wins: global_stats.clob_hft_wins,
                det_hft_wins: global_stats.det_hft_wins,
                clob_slippage_values: Vec::new(),
                det_slippage_values: Vec::new(),
            };
            tokio::spawn(async move {
                persist_comparison_summary(&p, "ALL", &gs).await;
            });

            println!("══════════════════════════════════════════════════════════");
            println!("  DUAL ENGINE COMPARISON — Cycle {}", cycle_count);
            println!("══════════════════════════════════════════════════════════");
            println!("                          CLOB          DETERMINISTIC");
            println!("  ──────────────────────────────────────────────────────");
            println!("  Total Fills:         {:>10}       {:>10}",
                global_stats.clob_fills, global_stats.det_fills);
            println!("  Avg Slippage:        {:>10.6}       {:>10.6}",
                global_stats.clob_avg_slippage(), global_stats.det_avg_slippage());
            println!("  Slippage Variance:   {:>10.8}       {:>10.8}",
                global_stats.clob_slippage_variance(), global_stats.det_slippage_variance());
            println!("  Slippage StdDev:     {:>10.6}       {:>10.6}",
                stddev(&global_stats.clob_slippage_values), stddev(&global_stats.det_slippage_values));
            println!("  HFT Wins (<50μs):   {:>10}       {:>10}",
                global_stats.clob_hft_wins, global_stats.det_hft_wins);
            println!("  ──────────────────────────────────────────────────────");
            println!("  Slippage Reduction:           {:>+.2}%", global_stats.slippage_reduction_pct());
            println!("  Variance Reduction:           {:>+.2}%", global_stats.variance_reduction_pct());
            println!("  HFT Advantage Reduction:      {:>+.2}%",
                if global_stats.clob_hft_wins > 0 {
                    (1.0 - global_stats.det_hft_wins as f64 / global_stats.clob_hft_wins as f64) * 100.0
                } else { 0.0 });
            println!("══════════════════════════════════════════════════════════");

            // Per-instrument prices
            println!("  Prices:");
            for inst in &instruments {
                let pct = (inst.current_price - inst.fair_price) / inst.fair_price * 100.0;
                println!("    {:8} {:>12.4}  ({:+.2}%)", inst.symbol, inst.current_price, pct);
            }
            println!();
        }

        tokio::time::sleep(Duration::from_millis(cycle_ms)).await;
    }
}
