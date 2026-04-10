# ΔEXCHANGE — Random Queue Priority in Continuous Limit Order Books

A deterministic matching engine that replaces time priority with random priority at each price level.

**Live demo:** https://detmart.singapore.to

## Components

| Component | Language | Description |
|-----------|----------|-------------|
| `engine/` | Rust | Production matching engine with REST API |
| `dual-engine/` | Rust | CLOB vs Deterministic comparison simulator |
| `simulator/` | Rust | Market simulator for order flow generation |
| `frontend/` | HTML/JS | Trading UI |
| `research/` | SQL | Analysis queries and export tools |

## Key Finding

Random queue priority reduces HFT fast-fill rate by ~1.2pp in a controlled simulation, providing direct evidence of queue-priority rent redistribution.

## License

MIT
