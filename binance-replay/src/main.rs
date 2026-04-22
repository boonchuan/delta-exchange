// ══════════════════════════════════════════════════════════════
//  ΔEXCHANGE — Binance aggTrades Replay
//  Feeds real Binance trade flow through matching mechanisms
//  for counterfactual microstructure analysis.
// ══════════════════════════════════════════════════════════════

mod types;
mod book;
mod fair_value;
mod queue_mechanisms;

use crate::types::*;
use crate::book::OrderBook;
use crate::fair_value::FairValue;

use chrono::{DateTime, Utc, TimeZone};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

// ─── Config ───

const DB_URL: &str = "postgresql://delta:delta_exchange_2026@localhost:5432/delta_exchange";
const BATCH_INSERT_SIZE: usize = 1000;
const FAIR_VALUE_WINDOW_MS: u64 = 500;

// All mechanisms to run in sequence
const MECHANISMS: &[Mechanism] = &[
    Mechanism::Clob,
    Mechanism::Fba10ms,
    Mechanism::Fba100ms,
    Mechanism::Fba200ms,   // Hyperliquid-faithful
    Mechanism::Fba500ms,   // Stressed consensus
    Mechanism::ThrottledTime1ms,
    Mechanism::ThrottledTime10ms,
    Mechanism::ThrottledTime100ms,
];

// ─── CSV row ───

#[derive(Debug, Clone)]
struct AggTrade {
    time_ms: u64,
    price: f64,
    qty: f64,
    is_buyer_maker: bool,
}

// ─── Run config ───

struct RunConfig {
    csv_path: String,
    symbol: String,
    tick_size: f64,
    start_ms: u64,
    end_ms: u64,
    run_id: String,
}

impl RunConfig {
    fn from_env() -> Self {
        let csv_path = std::env::var("CSV_PATH").unwrap_or_else(|_| {
            "/opt/delta-exchange/binance-ticks/raw/futures/BTCUSDT/\
             BTCUSDT-aggTrades-2025-10-10.csv".to_string()
        });
        let symbol = std::env::var("SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());
        let tick_size = std::env::var("TICK_SIZE")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(0.10);
        // Default: 21:00-22:00 UTC on Oct 10 2025 (crash hour)
        let start_ms = std::env::var("START_MS")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(1760130000000);
        let end_ms = std::env::var("END_MS")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(1760133600000);
        let run_id = std::env::var("RUN_ID").unwrap_or_else(|_| {
            format!("binance_{}_{}", symbol, Utc::now().format("%Y%m%d_%H%M%S"))
        });
        Self { csv_path, symbol, tick_size, start_ms, end_ms, run_id }
    }
}

// ─── CSV streaming ───

fn stream_window(cfg: &RunConfig) -> impl Iterator<Item = AggTrade> {
    let file = File::open(&cfg.csv_path).expect("open CSV");
    let reader = BufReader::with_capacity(1 << 20, file);
    let start = cfg.start_ms;
    let end = cfg.end_ms;
    reader.lines()
        .filter_map(Result::ok)
        .skip(1)  // header
        .filter_map(move |line| parse_aggtrade_line(&line))
        .skip_while(move |t| t.time_ms < start)
        .take_while(move |t| t.time_ms < end)
}

fn parse_aggtrade_line(line: &str) -> Option<AggTrade> {
    // Binance format: agg_id,price,quantity,first_id,last_id,transact_time,is_buyer_maker
    let parts: Vec<&str> = line.split(',').collect();
    if parts.len() < 7 { return None; }
    Some(AggTrade {
        time_ms: parts[5].parse().ok()?,
        price: parts[1].parse().ok()?,
        qty: parts[2].parse().ok()?,
        is_buyer_maker: parts[6].trim().eq_ignore_ascii_case("true"),
    })
}

// ─── Order ID generator ───

fn next_id(counter: &mut u64) -> u64 {
    *counter += 1;
    *counter
}

// ─── FBA uniform-clearing ───
// Price-time priority inside a batch, clearing at best-matched prices.
// Different from dual-engine's uniform-price FBA — closer to how
// Hyperliquid actually matches within a block.

fn clear_fba_batch(
    book: &mut OrderBook,
    pending: Vec<Order>,
    mech: Mechanism,
    batch_id: u64,
) -> Vec<Fill> {
    let mut fills = Vec::new();
    // Add all passive orders first, then execute aggressors in arrival order
    let mut aggressors = Vec::new();
    for o in pending {
        // Distinguish: in our synthetic flow, aggressors are taker orders
        // and passives are makers. We tag by participant heuristic:
        // aggressors have Retail/Institutional participant types mostly.
        let is_aggressor = matches!(
            o.participant,
            ParticipantType::Retail | ParticipantType::Institutional
        );
        if is_aggressor {
            aggressors.push(o);
        } else {
            book.add_limit(o);
        }
    }
    // Sort aggressors by timestamp (time priority within batch)
    aggressors.sort_by_key(|o| o.timestamp_us);
    for aggr in aggressors {
        fills.extend(book.execute_against(&aggr, mech, batch_id));
    }
    fills
}

// ─── Continuous matching ───

fn process_continuous(
    book: &mut OrderBook,
    passive: Order,
    aggressor: Order,
    mech: Mechanism,
    seq: u64,
) -> Vec<Fill> {
    book.add_limit(passive);
    book.execute_against(&aggressor, mech, seq)
}

// ─── Main replay driver for one mechanism ───

async fn replay_one(
    pool: &PgPool,
    cfg: &RunConfig,
    mech: Mechanism,
) -> anyhow::Result<usize> {
    let started = Instant::now();
    let mut book = OrderBook::new(cfg.tick_size);
    let mut fv = FairValue::new(FAIR_VALUE_WINDOW_MS);
    let mut order_id: u64 = 0;
    let mut batch_id: u64 = 0;
    let mut pending: Vec<Order> = Vec::new();
    let mut batch_start_us: u64 = 0;
    let mut fill_buffer: Vec<Fill> = Vec::new();
    let mut total_fills: usize = 0;

    let batch_us = mech.batch_us();
    let is_fba = mech.is_fba();

    for trade in stream_window(cfg) {
        let t_us = trade.time_ms * 1000;
        fv.update(trade.time_ms, trade.price, trade.qty);
        let fair = fv.value(trade.price);

        // Reconstruct the aggressor/passive pair from aggTrade flag
        // is_buyer_maker=true  → seller aggressed a resting buy
        // is_buyer_maker=false → buyer aggressed a resting sell
        let (passive_side, aggr_side) = if trade.is_buyer_maker {
            (Side::Buy, Side::Sell)
        } else {
            (Side::Sell, Side::Buy)
        };

        let passive = Order {
            id: next_id(&mut order_id),
            side: passive_side,
            price: trade.price,
            quantity: trade.qty,
            remaining: trade.qty,
            timestamp_us: t_us.saturating_sub(1),
            participant: ParticipantType::random_from_aggtrade(false),
            fair_value: fair,
        };
        let aggressor = Order {
            id: next_id(&mut order_id),
            side: aggr_side,
            price: trade.price,
            quantity: trade.qty,
            remaining: trade.qty,
            timestamp_us: t_us,
            participant: ParticipantType::random_from_aggtrade(true),
            fair_value: fair,
        };

        if is_fba {
            // Accumulate into current batch; flush when boundary crossed
            if batch_start_us == 0 { batch_start_us = t_us; }
            let window = batch_us.unwrap();
            if t_us - batch_start_us >= window {
                // flush
                let batch_orders = std::mem::take(&mut pending);
                let fills = clear_fba_batch(&mut book, batch_orders, mech, batch_id);
                fill_buffer.extend(fills);
                batch_id += 1;
                batch_start_us = t_us;
            }
            pending.push(passive);
            pending.push(aggressor);
        } else if batch_us.is_some() {
            // THROTTLED: accumulate within bucket, randomize at boundary, match CLOB-style.
            // This is the Budish/Cramton/Shim throttled-market model.
            if batch_start_us == 0 { batch_start_us = t_us; }
            let window = batch_us.unwrap();
            if t_us - batch_start_us >= window {
                let mut bucket_orders = std::mem::take(&mut pending);
                queue_mechanisms::sort_queue(&mut bucket_orders, mech);
                // passives rest, then aggressors hit the book in sorted order
                let mut aggressors: Vec<Order> = Vec::new();
                for o in bucket_orders.drain(..) {
                    let is_agg = matches!(o.participant,
                        ParticipantType::Retail | ParticipantType::Institutional);
                    if is_agg { aggressors.push(o); } else { book.add_limit(o); }
                }
                for aggr in aggressors {
                    let fills = book.execute_against(&aggr, mech, batch_id);
                    fill_buffer.extend(fills);
                }
                batch_id += 1;
                batch_start_us = t_us;
            }
            pending.push(passive);
            pending.push(aggressor);
        } else {
            // Pure CLOB: match immediately, no batching
            let fills = process_continuous(&mut book, passive, aggressor, mech, batch_id);
            fill_buffer.extend(fills);
        }

        // Flush to DB in chunks
        if fill_buffer.len() >= BATCH_INSERT_SIZE {
            total_fills += fill_buffer.len();
            persist_fills(pool, &cfg.symbol, &cfg.run_id, &fill_buffer).await?;
            fill_buffer.clear();
        }
    }

    // Final batch for FBA or THROTTLED
    if !pending.is_empty() {
        let batch_orders = std::mem::take(&mut pending);
        let fills = if is_fba {
            clear_fba_batch(&mut book, batch_orders, mech, batch_id)
        } else {
            // throttled final flush: passives rest, aggressors hit
            let mut ordered = batch_orders;
            queue_mechanisms::sort_queue(&mut ordered, mech);
            let mut aggressors: Vec<Order> = Vec::new();
            for o in ordered {
                let is_agg = matches!(o.participant,
                    ParticipantType::Retail | ParticipantType::Institutional);
                if is_agg { aggressors.push(o); } else { book.add_limit(o); }
            }
            let mut out = Vec::new();
            for aggr in aggressors {
                out.extend(book.execute_against(&aggr, mech, batch_id));
            }
            out
        };
        fill_buffer.extend(fills);
    }
    if !fill_buffer.is_empty() {
        total_fills += fill_buffer.len();
        persist_fills(pool, &cfg.symbol, &cfg.run_id, &fill_buffer).await?;
    }

    let elapsed = started.elapsed();
    println!("  {} → {} fills in {:.2}s", mech.name(), total_fills, elapsed.as_secs_f64());
    Ok(total_fills)
}

// ─── Postgres writer ───

async fn persist_fills(
    pool: &PgPool,
    symbol: &str,
    run_id: &str,
    fills: &[Fill],
) -> anyhow::Result<()> {
    // Use a transaction with multi-row insert for throughput
    let mut tx = pool.begin().await?;
    for f in fills {
        let ts: DateTime<Utc> = Utc.timestamp_millis_opt(f.trade_time_ms as i64)
            .single().unwrap_or_else(Utc::now);
        sqlx::query(
            "INSERT INTO tick_data \
             (time, symbol, fill_price, fair_value, slippage, spread, \
              depth_bid_1, depth_ask_1, volume, mechanism, latency_delta, \
              participant_type, batch_id, batch_size, run_id) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)"
        )
        .bind(ts)
        .bind(symbol)
        .bind(Decimal::from_f64(f.price).unwrap_or(Decimal::ZERO))
        .bind(Decimal::from_f64(f.fair_value).unwrap_or(Decimal::ZERO))
        .bind(Decimal::from_f64(f.slippage).unwrap_or(Decimal::ZERO))
        .bind(Decimal::from_f64(f.spread).unwrap_or(Decimal::ZERO))
        .bind(Decimal::from_f64(f.depth_bid_1).unwrap_or(Decimal::ZERO))
        .bind(Decimal::from_f64(f.depth_ask_1).unwrap_or(Decimal::ZERO))
        .bind(Decimal::from_f64(f.quantity).unwrap_or(Decimal::ZERO))
        .bind(f.mechanism.name())
        .bind(Decimal::from_f64(f.latency_delta_us as f64 / 1.0).unwrap_or(Decimal::ZERO))
        .bind(f.aggressor_participant.name())
        .bind(f.batch_id as i64)
        .bind(1_i32)
        .bind(run_id)
        .execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

// ─── Main ───

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = RunConfig::from_env();
    println!("══════════════════════════════════════════");
    println!("  Binance Replay");
    println!("  CSV:    {}", cfg.csv_path);
    println!("  Symbol: {}  (tick={})", cfg.symbol, cfg.tick_size);
    println!("  Window: {} → {}", cfg.start_ms, cfg.end_ms);
    println!("  RunID:  {}", cfg.run_id);
    println!("══════════════════════════════════════════");

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(DB_URL).await?;

    for mech in MECHANISMS {
        replay_one(&pool, &cfg, *mech).await?;
    }

    println!("\n✓ All mechanisms complete. Query:");
    println!("  psql -d delta_exchange -c \"SELECT mechanism, COUNT(*), AVG(slippage) \\");
    println!("                             FROM tick_data WHERE run_id='{}' \\", cfg.run_id);
    println!("                             GROUP BY mechanism ORDER BY mechanism;\"");
    Ok(())
}
