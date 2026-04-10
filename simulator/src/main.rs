use chrono::Utc;
use rand::Rng;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

// ══════════════════════════════════════════════════════════════
//  ΔEXCHANGE — Market Simulator
//  Generates realistic order flow for research data collection
// ══════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
struct SimInstrument {
    symbol: String,
    fair_price: f64,
    volatility: f64,
    tick_size: f64,
    lot_size: f64,
    // Price state
    current_price: f64,
    high: f64,
    low: f64,
}

#[derive(Debug, Clone, Copy)]
enum ParticipantType {
    MarketMaker,
    Hft,
    Institutional,
    Retail,
}

impl ParticipantType {
    fn spread_factor(&self) -> f64 {
        match self {
            Self::MarketMaker => 0.3,
            Self::Hft => 0.15,
            Self::Institutional => 0.8,
            Self::Retail => 1.2,
        }
    }

    fn size_range(&self) -> (f64, f64) {
        match self {
            Self::MarketMaker => (1.0, 5.0),
            Self::Hft => (1.0, 3.0),
            Self::Institutional => (5.0, 20.0),
            Self::Retail => (1.0, 3.0),
        }
    }

    fn market_order_probability(&self) -> f64 {
        match self {
            Self::MarketMaker => 0.0,
            Self::Hft => 0.0,
            Self::Institutional => 0.0,
            Self::Retail => 0.0,
        }
    }

    fn name(&self) -> &str {
        match self {
            Self::MarketMaker => "MM",
            Self::Hft => "HFT",
            Self::Institutional => "INST",
            Self::Retail => "RETAIL",
        }
    }
}

#[derive(Debug, Serialize)]
struct OrderRequest {
    symbol: String,
    side: String,
    order_type: String,
    price: Option<String>,
    quantity: String,
}

#[derive(Debug, Deserialize)]
struct OrderResponse {
    order_id: String,
    status: String,
    filled_qty: String,
    avg_fill_price: Option<String>,
    #[serde(default)]
    fills: Vec<FillInfo>,
}

#[derive(Debug, Deserialize)]
struct FillInfo {
    fill_id: String,
    price: String,
    quantity: String,
}

fn gaussian_random() -> f64 {
    let mut rng = rand::thread_rng();
    let u: f64 = rng.gen_range(0.0001..1.0);
    let v: f64 = rng.gen_range(0.0001..1.0);
    (-2.0 * u.ln()).sqrt() * (2.0 * std::f64::consts::PI * v).cos()
}

fn round_to_tick(price: f64, tick: f64) -> f64 {
    (price / tick).round() * tick
}

impl SimInstrument {
    fn tick_price(&mut self) {
        let dt: f64 = 1.0 / (252.0 * 6.5 * 3600.0); // ~1 second
        let drift = (rand::thread_rng().gen::<f64>() - 0.501) * self.volatility * dt.sqrt() * 3.0;
        let dw = dt.sqrt() * gaussian_random();
        let change = self.current_price * (drift + self.volatility * dw);
        self.current_price = round_to_tick(
            (self.current_price + change).max(self.tick_size),
            self.tick_size,
        );
        self.high = self.high.max(self.current_price);
        self.low = self.low.min(self.current_price);
    }
}

struct Simulator {
    instruments: Vec<SimInstrument>,
    api_url: String,
    client: reqwest::Client,
    total_orders: u64,
    total_fills: u64,
    total_errors: u64,
}

impl Simulator {
    fn new(api_url: String) -> Self {
        let instruments = vec![
            // Equities
            SimInstrument { symbol: "DETX".into(), fair_price: 4285.5, volatility: 0.18, tick_size: 0.25, lot_size: 1.0, current_price: 4285.5, high: 4285.5, low: 4285.5 },
            SimInstrument { symbol: "DTEC".into(), fair_price: 890.25, volatility: 0.24, tick_size: 0.25, lot_size: 1.0, current_price: 890.25, high: 890.25, low: 890.25 },
            SimInstrument { symbol: "DFIN".into(), fair_price: 542.75, volatility: 0.16, tick_size: 0.25, lot_size: 1.0, current_price: 542.75, high: 542.75, low: 542.75 },
            SimInstrument { symbol: "DHLTH".into(), fair_price: 1205.0, volatility: 0.14, tick_size: 0.25, lot_size: 1.0, current_price: 1205.0, high: 1205.0, low: 1205.0 },
            // Commodities
            SimInstrument { symbol: "DCRUDF".into(), fair_price: 78.42, volatility: 0.32, tick_size: 0.01, lot_size: 1.0, current_price: 78.42, high: 78.42, low: 78.42 },
            SimInstrument { symbol: "DGOLDF".into(), fair_price: 2342.8, volatility: 0.14, tick_size: 0.1, lot_size: 1.0, current_price: 2342.8, high: 2342.8, low: 2342.8 },
            SimInstrument { symbol: "DSLVRF".into(), fair_price: 29.85, volatility: 0.26, tick_size: 0.005, lot_size: 1.0, current_price: 29.85, high: 29.85, low: 29.85 },
            SimInstrument { symbol: "DNATGF".into(), fair_price: 2.84, volatility: 0.45, tick_size: 0.001, lot_size: 1.0, current_price: 2.84, high: 2.84, low: 2.84 },
            SimInstrument { symbol: "DCORNF".into(), fair_price: 445.25, volatility: 0.22, tick_size: 0.25, lot_size: 1.0, current_price: 445.25, high: 445.25, low: 445.25 },
            SimInstrument { symbol: "DWHEAT".into(), fair_price: 582.5, volatility: 0.25, tick_size: 0.25, lot_size: 1.0, current_price: 582.5, high: 582.5, low: 582.5 },
            // Crypto
            SimInstrument { symbol: "DBTCP".into(), fair_price: 67480.0, volatility: 0.55, tick_size: 0.5, lot_size: 0.001, current_price: 67480.0, high: 67480.0, low: 67480.0 },
            SimInstrument { symbol: "DETHP".into(), fair_price: 3520.0, volatility: 0.52, tick_size: 0.1, lot_size: 0.01, current_price: 3520.0, high: 3520.0, low: 3520.0 },
            SimInstrument { symbol: "DSOLP".into(), fair_price: 178.5, volatility: 0.65, tick_size: 0.01, lot_size: 0.1, current_price: 178.5, high: 178.5, low: 178.5 },
            // FX
            SimInstrument { symbol: "DEURUSD".into(), fair_price: 1.0852, volatility: 0.08, tick_size: 0.0001, lot_size: 1.0, current_price: 1.0852, high: 1.0852, low: 1.0852 },
            SimInstrument { symbol: "DGBPUSD".into(), fair_price: 1.2645, volatility: 0.09, tick_size: 0.0001, lot_size: 1.0, current_price: 1.2645, high: 1.2645, low: 1.2645 },
            SimInstrument { symbol: "DUSDJPY".into(), fair_price: 154.82, volatility: 0.10, tick_size: 0.01, lot_size: 1.0, current_price: 154.82, high: 154.82, low: 154.82 },
            // Rates
            SimInstrument { symbol: "DUS10Y".into(), fair_price: 110.28, volatility: 0.06, tick_size: 0.03125, lot_size: 1.0, current_price: 110.28, high: 110.28, low: 110.28 },
            SimInstrument { symbol: "DUS2Y".into(), fair_price: 103.14, volatility: 0.03, tick_size: 0.015625, lot_size: 1.0, current_price: 103.14, high: 103.14, low: 103.14 },
        ];

        Self {
            instruments,
            api_url,
            client: reqwest::Client::new(),
            total_orders: 0,
            total_fills: 0,
            total_errors: 0,
        }
    }

    fn pick_participant() -> ParticipantType {
        let r: f64 = rand::thread_rng().gen();
        if r < 0.15 { ParticipantType::MarketMaker }
        else if r < 0.30 { ParticipantType::Hft }
        else if r < 0.60 { ParticipantType::Institutional }
        else { ParticipantType::Retail }
    }

    fn generate_order(&self, inst: &SimInstrument, participant: ParticipantType) -> OrderRequest {
        let mut rng = rand::thread_rng();
        let side = if rng.gen::<f64>() > 0.5 { "BUY" } else { "SELL" };
        let is_market = rng.gen::<f64>() < participant.market_order_probability();

        let spread = inst.current_price * 0.001 * participant.spread_factor();
        let offset = gaussian_random() * spread;

        let price = if side == "BUY" {
            round_to_tick(inst.current_price - spread / 2.0 + offset, inst.tick_size)
        } else {
            round_to_tick(inst.current_price + spread / 2.0 + offset, inst.tick_size)
        };
        let price = price.max(inst.tick_size); // never zero or negative

        let (min_size, max_size) = participant.size_range();
        let qty = rng.gen_range(min_size..max_size).ceil() * inst.lot_size;

        // Format price with enough decimals
        let price_str = if inst.tick_size < 0.001 {
            format!("{:.4}", price)
        } else if inst.tick_size < 0.01 {
            format!("{:.3}", price)
        } else if inst.tick_size < 0.1 {
            format!("{:.2}", price)
        } else {
            format!("{:.2}", price)
        };

        let qty_str = if inst.lot_size < 0.01 {
            format!("{:.3}", qty)
        } else if inst.lot_size < 1.0 {
            format!("{:.2}", qty)
        } else {
            format!("{:.0}", qty)
        };

        OrderRequest {
            symbol: inst.symbol.clone(),
            side: side.to_string(),
            order_type: if is_market { "MARKET".to_string() } else { "LIMIT".to_string() },
            price: if is_market { None } else { Some(price_str) },
            quantity: qty_str,
        }
    }

    async fn submit_order(&mut self, order: &OrderRequest) -> Result<OrderResponse, String> {
        let url = format!("{}/api/order", self.api_url);
        match self.client.post(&url)
            .json(order)
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp) => {
                if resp.status().is_success() {
                    match resp.json::<OrderResponse>().await {
                        Ok(r) => Ok(r),
                        Err(e) => Err(format!("parse: {}", e)),
                    }
                } else {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    Err(format!("HTTP {}: {}", status, body))
                }
            }
            Err(e) => Err(format!("request: {}", e)),
        }
    }

    async fn run_cycle(&mut self) {
        let mut rng = rand::thread_rng();

        // Each cycle: tick all prices, then generate orders for a random subset
        for inst in &mut self.instruments {
            inst.tick_price();
        }

        // Pick 3-8 instruments to generate orders for this cycle
        let num_active = rng.gen_range(3..=8).min(self.instruments.len());
        let mut indices: Vec<usize> = (0..self.instruments.len()).collect();
        indices.sort_by(|_, _| if rng.gen::<bool>() { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater });
        indices.truncate(num_active);

        for idx in indices {
            // 1-4 orders per instrument per cycle
            let num_orders = rng.gen_range(1..=4);
            for _ in 0..num_orders {
                let participant = Self::pick_participant();
                let inst = &self.instruments[idx];
                let order = self.generate_order(inst, participant);

                let sym = order.symbol.clone();
                let side = order.side.clone();
                let otype = order.order_type.clone();

                match self.submit_order(&order).await {
                    Ok(resp) => {
                        self.total_orders += 1;
                        let num_fills = resp.fills.len() as u64;
                        self.total_fills += num_fills;

                        if num_fills > 0 {
                            let fill_info = resp.fills.iter()
                                .map(|f| format!("{}@{}", f.quantity, f.price))
                                .collect::<Vec<_>>()
                                .join(", ");
                            println!("  FILL {} {} {} → {} [{}]",
                                sym, side, otype, resp.status, fill_info);
                        }
                    }
                    Err(e) => {
                        self.total_errors += 1;
                        if self.total_errors % 100 == 1 {
                            eprintln!("  ERR {}: {}", sym, e);
                        }
                    }
                }
            }
        }
    }

    fn print_status(&self) {
        println!("\n══════════════════════════════════════════");
        println!("  ΔEXCHANGE Simulator — Status Report");
        println!("══════════════════════════════════════════");
        println!("  Orders sent:  {}", self.total_orders);
        println!("  Fills:        {}", self.total_fills);
        println!("  Errors:       {}", self.total_errors);
        println!("  Fill rate:    {:.1}%",
            if self.total_orders > 0 { self.total_fills as f64 / self.total_orders as f64 * 100.0 } else { 0.0 });
        println!("──────────────────────────────────────────");
        for inst in &self.instruments {
            let change_pct = (inst.current_price - inst.fair_price) / inst.fair_price * 100.0;
            let arrow = if change_pct >= 0.0 { "▲" } else { "▼" };
            println!("  {:8} {:>12.4}  {} {:.2}%",
                inst.symbol, inst.current_price, arrow, change_pct.abs());
        }
        println!("══════════════════════════════════════════\n");
    }
}

#[tokio::main]
async fn main() {
    let api_url = std::env::var("API_URL")
        .unwrap_or_else(|_| "http://localhost:8080".to_string());

    let cycle_ms: u64 = std::env::var("CYCLE_MS")
        .unwrap_or_else(|_| "500".to_string())
        .parse()
        .unwrap_or(500);

    println!("══════════════════════════════════════════");
    println!("  ΔEXCHANGE — Market Simulator");
    println!("══════════════════════════════════════════");
    println!("  API:        {}", api_url);
    println!("  Cycle:      {}ms", cycle_ms);
    println!("  Instruments: 18");
    println!("  Participants: MM, HFT, INST, RETAIL");
    println!("══════════════════════════════════════════");
    println!();

    // Verify engine is up
    let client = reqwest::Client::new();
    match client.get(format!("{}/api/health", api_url)).send().await {
        Ok(resp) if resp.status().is_success() => {
            println!("  ✓ Engine is online");
        }
        _ => {
            eprintln!("  ✗ Cannot reach engine at {}. Is it running?", api_url);
            std::process::exit(1);
        }
    }

    let mut sim = Simulator::new(api_url);
    let mut cycle_count: u64 = 0;
    let status_interval = 60_000 / cycle_ms; // print status every ~60 seconds

    // Seed the orderbooks with initial liquidity
    println!("  Seeding initial liquidity...");
    for _ in 0..20 {
        sim.run_cycle().await;
    }
    println!("  ✓ Initial liquidity seeded");
    println!("  Starting continuous simulation...\n");

    loop {
        sim.run_cycle().await;
        cycle_count += 1;

        if cycle_count % status_interval == 0 {
            sim.print_status();
        }

        tokio::time::sleep(Duration::from_millis(cycle_ms)).await;
    }
}
