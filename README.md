# ⚡ CryptoStream

Real-time crypto order book engine built in Rust.

## Tech Stack
Rust · Axum · Tokio · WebSockets · Serde

## Features
- REST API to place buy/sell orders
- Price-time priority matching engine (best bid meets best ask)
- WebSocket feed broadcasting live order book updates to all connected clients
- In-memory BTreeMap order book with O(log n) insertion and sorted price levels
- Arc<Mutex<>> shared state for safe concurrent access across async handlers

## Run Locally
```bash
cargo run
# Open http://localhost:3000
```

## Architecture
- `POST /order` — place a bid or ask
- `GET /book` — current order book snapshot
- `GET /ws` — WebSocket stream for live updates