// Types copied from dual-engine. TECH DEBT: refactor to shared crate
// once both binaries are stable.

use rand::Rng;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side { Buy, Sell }

impl Side {
    pub fn as_str(&self) -> &'static str {
        match self { Side::Buy => "BUY", Side::Sell => "SELL" }
    }
    pub fn opposite(&self) -> Self {
        match self { Side::Buy => Side::Sell, Side::Sell => Side::Buy }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mechanism {
    Clob,
    Fba10ms,
    Fba100ms,
    Fba200ms,   // NEW — Hyperliquid HyperBFT block cadence
    Fba500ms,   // NEW — stressed-consensus regime
    ThrottledTime1ms,
    ThrottledTime10ms,
    ThrottledTime100ms,
}

impl Mechanism {
    pub fn name(&self) -> &'static str {
        match self {
            Mechanism::Clob => "CLOB",
            Mechanism::Fba10ms => "FBA10MS",
            Mechanism::Fba100ms => "FBA100MS",
            Mechanism::Fba200ms => "FBA200MS",
            Mechanism::Fba500ms => "FBA500MS",
            Mechanism::ThrottledTime1ms => "THROTTLED1MS",
            Mechanism::ThrottledTime10ms => "THROTTLED10MS",
            Mechanism::ThrottledTime100ms => "THROTTLED100MS",
        }
    }
    pub fn batch_us(&self) -> Option<u64> {
        match self {
            Mechanism::Clob => None,
            Mechanism::Fba10ms => Some(10_000),
            Mechanism::Fba100ms => Some(100_000),
            Mechanism::Fba200ms => Some(200_000),
            Mechanism::Fba500ms => Some(500_000),
            Mechanism::ThrottledTime1ms => Some(1_000),
            Mechanism::ThrottledTime10ms => Some(10_000),
            Mechanism::ThrottledTime100ms => Some(100_000),
        }
    }
    pub fn is_fba(&self) -> bool {
        matches!(self, Mechanism::Fba10ms | Mechanism::Fba100ms
                     | Mechanism::Fba200ms | Mechanism::Fba500ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParticipantType { Hft, MarketMaker, Institutional, Retail }

impl ParticipantType {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Hft => "HFT",
            Self::MarketMaker => "MM",
            Self::Institutional => "INST",
            Self::Retail => "RETAIL",
        }
    }
    pub fn random_from_aggtrade(aggressive: bool) -> Self {
        // Heuristic mapping: aggressors skew toward retail/institutional,
        // passive fills skew toward HFT/MM. Calibrated from Kyle-lambda
        // literature on crypto flow composition.
        let mut rng = rand::thread_rng();
        let r: f64 = rng.gen();
        if aggressive {
            if r < 0.35 { Self::Retail }
            else if r < 0.75 { Self::Institutional }
            else if r < 0.92 { Self::MarketMaker }
            else { Self::Hft }
        } else {
            if r < 0.55 { Self::Hft }
            else if r < 0.88 { Self::MarketMaker }
            else if r < 0.97 { Self::Institutional }
            else { Self::Retail }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Order {
    pub id: u64,
    pub side: Side,
    pub price: f64,
    pub quantity: f64,
    pub remaining: f64,
    pub timestamp_us: u64,
    pub participant: ParticipantType,
    pub fair_value: f64,
}

#[derive(Debug, Clone)]
pub struct Fill {
    pub price: f64,
    pub quantity: f64,
    pub mechanism: Mechanism,
    pub aggressor_side: Side,
    pub slippage: f64,
    pub fair_value: f64,
    pub latency_delta_us: u64,
    pub aggressor_participant: ParticipantType,
    pub batch_id: u64,
    pub trade_time_ms: u64,   // original Binance trade ts
    pub spread: f64,
    pub depth_bid_1: f64,
    pub depth_ask_1: f64,
}
