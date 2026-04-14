use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use rand::seq::SliceRandom;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tracing::{error, info};
use uuid::Uuid;
mod queue_mechanisms;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side { Buy, Sell }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderType { Limit, Market }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderStatus { New, Partial, Filled, Cancelled, Rejected }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MatchingMode { Deterministic, Clob, Batch, ThrottledTime1ms, ThrottledTime10ms, ThrottledTime100ms, AgeWeighted, SizeWeighted, RotatingPriority, RandomPerParticipant }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instrument {
    pub symbol: String, pub name: String, pub asset_class: String,
    pub instrument_type: String, pub tick_size: Decimal, pub lot_size: Decimal,
    pub initial_price: Decimal, pub volatility: Decimal, pub is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub order_id: Uuid, pub account_id: Uuid, pub symbol: String,
    pub side: Side, pub order_type: OrderType, pub price: Decimal,
    pub quantity: Decimal, pub filled_qty: Decimal, pub status: OrderStatus,
    pub timestamp: DateTime<Utc>, pub batch_id: Option<u64>,
}

impl Order {
    pub fn remaining(&self) -> Decimal { self.quantity - self.filled_qty }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub fill_id: Uuid, pub symbol: String, pub buy_order_id: Uuid,
    pub sell_order_id: Uuid, pub price: Decimal, pub quantity: Decimal,
    pub mechanism: MatchingMode, pub aggressor_side: Side,
    pub timestamp: DateTime<Utc>, pub batch_id: Option<u64>, pub sequence_num: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookLevel { pub price: Decimal, pub quantity: Decimal, pub order_count: u32 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub symbol: String, pub bids: Vec<OrderBookLevel>, pub asks: Vec<OrderBookLevel>,
    pub last_price: Option<Decimal>, pub timestamp: DateTime<Utc>,
}

#[derive(Debug)]
pub struct OrderBook {
    pub symbol: String,
    pub bids: BTreeMap<Decimal, Vec<Order>>,
    pub asks: BTreeMap<Decimal, Vec<Order>>,
    pub last_price: Option<Decimal>,
    pub sequence: u64,
}

impl OrderBook {
    pub fn new(symbol: String) -> Self {
        Self { symbol, bids: BTreeMap::new(), asks: BTreeMap::new(), last_price: None, sequence: 0 }
    }

    pub fn snapshot(&self, depth: usize) -> OrderBookSnapshot {
        let bids = self.bids.iter().rev().take(depth)
            .map(|(p, o)| OrderBookLevel { price: *p, quantity: o.iter().map(|x| x.remaining()).sum(), order_count: o.len() as u32 })
            .collect();
        let asks = self.asks.iter().take(depth)
            .map(|(p, o)| OrderBookLevel { price: *p, quantity: o.iter().map(|x| x.remaining()).sum(), order_count: o.len() as u32 })
            .collect();
        OrderBookSnapshot { symbol: self.symbol.clone(), bids, asks, last_price: self.last_price, timestamp: Utc::now() }
    }

    pub fn best_bid(&self) -> Option<Decimal> { self.bids.keys().next_back().copied() }
    pub fn best_ask(&self) -> Option<Decimal> { self.asks.keys().next().copied() }

    pub fn add_order(&mut self, order: Order) {
        let book = match order.side { Side::Buy => &mut self.bids, Side::Sell => &mut self.asks };
        book.entry(order.price).or_default().push(order);
    }

    pub fn cancel_order(&mut self, order_id: Uuid) -> Option<Order> {
        for book in [&mut self.bids, &mut self.asks] {
            for (_, orders) in book.iter_mut() {
                if let Some(idx) = orders.iter().position(|o| o.order_id == order_id) {
                    let mut order = orders.remove(idx);
                    order.status = OrderStatus::Cancelled;
                    return Some(order);
                }
            }
            book.retain(|_, orders| !orders.is_empty());
        }
        None
    }
}

pub struct MatchingEngine {
    pub orderbooks: DashMap<String, OrderBook>,
    pub instruments: DashMap<String, Instrument>,
    pub mode: MatchingMode,
    pub batch_counter: std::sync::atomic::AtomicU64,
    pub fill_tx: broadcast::Sender<Fill>,
    pub snapshot_tx: broadcast::Sender<OrderBookSnapshot>,
}

impl MatchingEngine {
    pub fn new(mode: MatchingMode) -> Self {
        let (fill_tx, _) = broadcast::channel(10000);
        let (snapshot_tx, _) = broadcast::channel(1000);
        Self { orderbooks: DashMap::new(), instruments: DashMap::new(), mode,
            batch_counter: std::sync::atomic::AtomicU64::new(1), fill_tx, snapshot_tx }
    }

    pub fn register_instrument(&self, instrument: Instrument) {
        let symbol = instrument.symbol.clone();
        self.orderbooks.insert(symbol.clone(), OrderBook::new(symbol.clone()));
        self.instruments.insert(symbol, instrument);
    }

    pub fn next_batch_id(&self) -> u64 {
        self.batch_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn process_order(&self, mut order: Order) -> (Order, Vec<Fill>) {
        let mut fills = Vec::new();
        let mut ob = match self.orderbooks.get_mut(&order.symbol) {
            Some(ob) => ob,
            None => { order.status = OrderStatus::Rejected; return (order, fills); }
        };

        let price_levels: Vec<Decimal> = match order.side {
            Side::Buy => ob.asks.keys().copied().collect(),
            Side::Sell => ob.bids.keys().rev().copied().collect(),
        };

        for price in price_levels {
            if order.remaining() <= Decimal::ZERO { break; }

            let can_match = match order.side {
                Side::Buy => order.price >= price || order.order_type == OrderType::Market,
                Side::Sell => order.price <= price || order.order_type == OrderType::Market,
            };
            if !can_match { break; }

            // Remove orders at this price to avoid borrow conflicts
            let opposite = match order.side {
                Side::Buy => &mut ob.asks,
                Side::Sell => &mut ob.bids,
            };
            let mut resting = match opposite.remove(&price) {
                Some(orders) => orders,
                None => continue,
            };

            // ═══ QUEUE MECHANISM DISPATCH ═══
            queue_mechanisms::sort_queue(&mut resting, self.mode, ob.sequence);

            let mut i = 0;
            while i < resting.len() && order.remaining() > Decimal::ZERO {
                let fill_qty = order.remaining().min(resting[i].remaining());
                let fill_price = match self.mode {
                    MatchingMode::Deterministic => if order.order_type == OrderType::Market { resting[i].price } else { (order.price + resting[i].price) / Decimal::from(2) },
                    _ => resting[i].price,
                };

                ob.sequence += 1;
                let seq = ob.sequence;

                let fill = Fill {
                    fill_id: Uuid::new_v4(), symbol: order.symbol.clone(),
                    buy_order_id: if order.side == Side::Buy { order.order_id } else { resting[i].order_id },
                    sell_order_id: if order.side == Side::Sell { order.order_id } else { resting[i].order_id },
                    price: fill_price, quantity: fill_qty, mechanism: self.mode,
                    aggressor_side: order.side, timestamp: Utc::now(),
                    batch_id: order.batch_id, sequence_num: seq,
                };

                order.filled_qty += fill_qty;
                resting[i].filled_qty += fill_qty;

                if resting[i].remaining() <= Decimal::ZERO {
                    resting[i].status = OrderStatus::Filled;
                    resting.remove(i);
                } else {
                    resting[i].status = OrderStatus::Partial;
                    i += 1;
                }

                ob.last_price = Some(fill_price);
                let _ = self.fill_tx.send(fill.clone());
                fills.push(fill);
            }

            if !resting.is_empty() {
                let opposite = match order.side {
                    Side::Buy => &mut ob.asks,
                    Side::Sell => &mut ob.bids,
                };
                opposite.insert(price, resting);
            }
        }

        if order.remaining() <= Decimal::ZERO {
            order.status = OrderStatus::Filled;
        } else if order.filled_qty > Decimal::ZERO {
            order.status = OrderStatus::Partial;
            if order.order_type == OrderType::Limit { ob.add_order(order.clone()); }
        } else if order.order_type == OrderType::Limit {
            order.status = OrderStatus::New;
            ob.add_order(order.clone());
        } else {
            order.status = OrderStatus::Rejected;
        }

        let snapshot = ob.snapshot(20);
        let _ = self.snapshot_tx.send(snapshot);
        (order, fills)
    }

    pub fn get_orderbook(&self, symbol: &str, depth: usize) -> Option<OrderBookSnapshot> {
        self.orderbooks.get(symbol).map(|ob| ob.snapshot(depth))
    }
}

#[derive(Debug, Deserialize)]
pub struct SubmitOrderRequest {
    pub symbol: String, pub side: Side, pub order_type: OrderType,
    pub price: Option<Decimal>, pub quantity: Decimal, pub account_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct SubmitOrderResponse {
    pub order_id: Uuid, pub status: OrderStatus, pub filled_qty: Decimal,
    pub avg_fill_price: Option<Decimal>, pub fills: Vec<FillResponse>,
}

#[derive(Debug, Serialize)]
pub struct FillResponse {
    pub fill_id: Uuid, pub price: Decimal, pub quantity: Decimal,
    pub mechanism: MatchingMode, pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String, pub engine: MatchingMode, pub version: String,
    pub instruments: usize, pub uptime_seconds: u64,
}

#[derive(Debug, Serialize)]
pub struct EngineStats {
    pub total_fills: u64, pub total_orders: u64, pub matching_mode: MatchingMode,
    pub instruments_active: usize, pub orderbooks: Vec<OrderBookSummary>,
}

#[derive(Debug, Serialize)]
pub struct OrderBookSummary {
    pub symbol: String, pub bid_levels: usize, pub ask_levels: usize,
    pub best_bid: Option<Decimal>, pub best_ask: Option<Decimal>,
    pub spread: Option<Decimal>, pub last_price: Option<Decimal>,
}

pub struct AppState {
    pub engine: Arc<MatchingEngine>, pub pool: PgPool, pub start_time: Instant,
    pub total_orders: std::sync::atomic::AtomicU64, pub total_fills: std::sync::atomic::AtomicU64,
}

async fn health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(), engine: state.engine.mode, version: "0.1.0".into(),
        instruments: state.engine.instruments.len(), uptime_seconds: state.start_time.elapsed().as_secs(),
    })
}

async fn list_instruments(State(state): State<Arc<AppState>>) -> Json<Vec<Instrument>> {
    Json(state.engine.instruments.iter().map(|r| r.value().clone()).collect())
}

async fn get_orderbook(State(state): State<Arc<AppState>>, Path(symbol): Path<String>) -> impl IntoResponse {
    match state.engine.get_orderbook(&symbol, 20) {
        Some(s) => (StatusCode::OK, Json(serde_json::to_value(s).unwrap())),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "not found"}))),
    }
}

async fn submit_order(State(state): State<Arc<AppState>>, Json(req): Json<SubmitOrderRequest>) -> impl IntoResponse {
    if !state.engine.instruments.contains_key(&req.symbol) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "unknown instrument"})));
    }
    let price = match req.order_type {
        OrderType::Limit => match req.price {
            Some(p) => p,
            None => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "price required"}))),
        },
        OrderType::Market => if req.side == Side::Buy { Decimal::from(999999999) } else { Decimal::ZERO },
    };

    let order = Order {
        order_id: Uuid::new_v4(), account_id: req.account_id.unwrap_or_else(Uuid::new_v4),
        symbol: req.symbol, side: req.side, order_type: req.order_type,
        price, quantity: req.quantity, filled_qty: Decimal::ZERO,
        status: OrderStatus::New, timestamp: Utc::now(), batch_id: Some(state.engine.next_batch_id()),
    };
    let oid = order.order_id;
    let (proc, fills) = state.engine.process_order(order);

    state.total_orders.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    state.total_fills.fetch_add(fills.len() as u64, std::sync::atomic::Ordering::Relaxed);

    let pool = state.pool.clone();
    let oc = proc.clone(); let fc = fills.clone();
    tokio::spawn(async move {
        persist_order(&pool, &oc).await;
        for f in &fc { persist_fill(&pool, f).await; persist_tick_data(&pool, f).await; }
    });

    let avg = if !fills.is_empty() {
        let tv: Decimal = fills.iter().map(|f| f.price * f.quantity).sum();
        let tq: Decimal = fills.iter().map(|f| f.quantity).sum();
        if tq > Decimal::ZERO { Some(tv / tq) } else { None }
    } else { None };

    (StatusCode::OK, Json(serde_json::to_value(SubmitOrderResponse {
        order_id: oid, status: proc.status, filled_qty: proc.filled_qty, avg_fill_price: avg,
        fills: fills.into_iter().map(|f| FillResponse {
            fill_id: f.fill_id, price: f.price, quantity: f.quantity, mechanism: f.mechanism, timestamp: f.timestamp,
        }).collect(),
    }).unwrap()))
}

async fn cancel_order(State(state): State<Arc<AppState>>, Path((sym, oid)): Path<(String, Uuid)>) -> impl IntoResponse {
    match state.engine.orderbooks.get_mut(&sym) {
        Some(mut ob) => match ob.cancel_order(oid) {
            Some(o) => (StatusCode::OK, Json(serde_json::json!({"order_id": o.order_id, "status": "CANCELLED"}))),
            None => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "order not found"}))),
        },
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "instrument not found"}))),
    }
}

async fn engine_stats(State(state): State<Arc<AppState>>) -> Json<EngineStats> {
    let obs = state.engine.orderbooks.iter().map(|r| {
        let ob = r.value(); let bb = ob.best_bid(); let ba = ob.best_ask();
        OrderBookSummary { symbol: ob.symbol.clone(), bid_levels: ob.bids.len(), ask_levels: ob.asks.len(),
            best_bid: bb, best_ask: ba, spread: bb.zip(ba).map(|(b,a)| a - b), last_price: ob.last_price }
    }).collect();
    Json(EngineStats {
        total_fills: state.total_fills.load(std::sync::atomic::Ordering::Relaxed),
        total_orders: state.total_orders.load(std::sync::atomic::Ordering::Relaxed),
        matching_mode: state.engine.mode, instruments_active: state.engine.instruments.len(), orderbooks: obs,
    })
}

async fn recent_fills(State(state): State<Arc<AppState>>, Path(sym): Path<String>) -> impl IntoResponse {
    match sqlx::query_as::<_, (Uuid, Decimal, Decimal, String, DateTime<Utc>)>(
        "SELECT fill_id, price, quantity, mechanism, created_at FROM fills WHERE symbol=$1 ORDER BY created_at DESC LIMIT 50"
    ).bind(&sym).fetch_all(&state.pool).await {
        Ok(rows) => (StatusCode::OK, Json(serde_json::json!(rows.iter().map(|(id,px,q,m,t)| serde_json::json!({"fill_id":id,"price":px,"quantity":q,"mechanism":m,"timestamp":t})).collect::<Vec<_>>()))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))),
    }
}

async fn persist_order(pool: &PgPool, o: &Order) {
    let _ = sqlx::query("INSERT INTO orders (order_id,account_id,symbol,side,order_type,price,quantity,filled_qty,status,batch_id,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (order_id) DO UPDATE SET filled_qty=$8,status=$9,updated_at=NOW()")
        .bind(o.order_id).bind(o.account_id).bind(&o.symbol)
        .bind(format!("{:?}",o.side).to_uppercase()).bind(format!("{:?}",o.order_type).to_uppercase())
        .bind(o.price).bind(o.quantity).bind(o.filled_qty).bind(format!("{:?}",o.status).to_uppercase())
        .bind(o.batch_id.map(|b| b as i64)).bind(o.timestamp).execute(pool).await.map_err(|e| error!("order: {}",e));
}

async fn persist_fill(pool: &PgPool, f: &Fill) {
    let _ = sqlx::query("INSERT INTO fills (fill_id,symbol,buy_order_id,sell_order_id,price,quantity,mechanism,aggressor_side,batch_id,sequence_num,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (fill_id) DO NOTHING")
        .bind(f.fill_id).bind(&f.symbol).bind(f.buy_order_id).bind(f.sell_order_id)
        .bind(f.price).bind(f.quantity).bind(format!("{:?}",f.mechanism).to_uppercase())
        .bind(format!("{:?}",f.aggressor_side).to_uppercase())
        .bind(f.batch_id.map(|b| b as i64)).bind(f.sequence_num as i64).bind(f.timestamp)
        .execute(pool).await.map_err(|e| error!("fill: {}",e));
}

async fn persist_tick_data(pool: &PgPool, f: &Fill) {
    let _ = sqlx::query("INSERT INTO tick_data (time,symbol,fill_price,fair_value,slippage,mechanism,batch_id) VALUES ($1,$2,$3,$4,$5,$6,$7)")
        .bind(f.timestamp).bind(&f.symbol).bind(f.price).bind(f.price).bind(Decimal::ZERO)
        .bind(format!("{:?}",f.mechanism).to_uppercase()).bind(f.batch_id.map(|b| b as i64))
        .execute(pool).await.map_err(|e| error!("tick: {}",e));
}

async fn load_instruments(pool: &PgPool) -> Vec<Instrument> {
    sqlx::query_as::<_,(String,String,String,String,Decimal,Decimal,Decimal,Decimal,bool)>(
        "SELECT symbol,name,asset_class,instrument_type,tick_size,lot_size,initial_price,volatility,is_active FROM instruments WHERE is_active=true"
    ).fetch_all(pool).await.unwrap_or_default().into_iter()
    .map(|(s,n,ac,it,ts,ls,ip,v,ia)| Instrument { symbol:s, name:n, asset_class:ac, instrument_type:it, tick_size:ts, lot_size:ls, initial_price:ip, volatility:v, is_active:ia })
    .collect()
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into())).init();

    info!("══════════════════════════════════════════");
    info!("  DELTA EXCHANGE — Deterministic Engine");
    info!("══════════════════════════════════════════");

    let pool = PgPoolOptions::new().max_connections(20)
        .connect(&std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql://delta:delta_exchange_2026@localhost:5432/delta_exchange".into()))
        .await.expect("DB connect failed");
    info!("Database connected");

    let mode_str = std::env::var("MATCHING_MODE").unwrap_or_else(|_| "DETERMINISTIC".into());
    let mode = match mode_str.to_uppercase().as_str() {
        "CLOB" => MatchingMode::Clob,
        "DETERMINISTIC" => MatchingMode::Deterministic,
        "THROTTLED1MS" => MatchingMode::ThrottledTime1ms,
        "THROTTLED10MS" => MatchingMode::ThrottledTime10ms,
        "THROTTLED100MS" => MatchingMode::ThrottledTime100ms,
        "AGEWEIGHTED" => MatchingMode::AgeWeighted,
        "SIZEWEIGHTED" => MatchingMode::SizeWeighted,
        "ROTATING" => MatchingMode::RotatingPriority,
        "PERPARTICIPANT" => MatchingMode::RandomPerParticipant,
        "BATCH" => MatchingMode::Batch,
        _ => { eprintln!("Unknown mode: {}, defaulting to DETERMINISTIC", mode_str); MatchingMode::Deterministic }
    };
    info!("Matching mode: {:?}", mode);
    let engine = Arc::new(MatchingEngine::new(mode));
    for inst in load_instruments(&pool).await {
        info!("  {} — {}", inst.symbol, inst.name);
        engine.register_instrument(inst);
    }

    let state = Arc::new(AppState { engine, pool, start_time: Instant::now(),
        total_orders: std::sync::atomic::AtomicU64::new(0), total_fills: std::sync::atomic::AtomicU64::new(0) });

    let app = Router::new()
        .route("/api/health", get(health))
        .route("/api/instruments", get(list_instruments))
        .route("/api/stats", get(engine_stats))
        .route("/api/order", post(submit_order))
        .route("/api/order/{symbol}/{order_id}", axum::routing::delete(cancel_order))
        .route("/api/orderbook/{symbol}", get(get_orderbook))
        .route("/api/fills/{symbol}", get(recent_fills))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    info!("Listening on {} — DETERMINISTIC MODE", addr);
    axum::serve(tokio::net::TcpListener::bind(addr).await.unwrap(), app).await.unwrap();
}
