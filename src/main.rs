use axum::{
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use axum::extract::ws::{Message, WebSocket};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use uuid::Uuid;
use tower_http::services::ServeDir;

// An individual order placed by a trader
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub side: String,      // "buy" or "sell"
    pub price: f64,
    pub quantity: f64,
    pub timestamp: i64,
}

// The full order book — bids and asks
#[derive(Debug)]
pub struct OrderBook {
    pub bids: BTreeMap<String, Order>,   // buy orders
    pub asks: BTreeMap<String, Order>,   // sell orders
    pub tx: broadcast::Sender<String>,   // broadcasts updates to WebSocket clients
}

impl OrderBook {
    pub fn new(tx: broadcast::Sender<String>) -> Self {
        OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            tx,
        }
    }

    pub fn add_order(&mut self, order: Order) -> Option<String> {
        let match_msg = self.try_match(&order);
        match order.side.as_str() {
            "buy" => { self.bids.insert(order.id.clone(), order); }
            "sell" => { self.asks.insert(order.id.clone(), order); }
            _ => {}
        }
        let snapshot = self.snapshot();
        let _ = self.tx.send(snapshot);
        match_msg
    }

    fn try_match(&self, incoming: &Order) -> Option<String> {
        if incoming.side == "buy" {
            for (_, ask) in &self.asks {
                if incoming.price >= ask.price {
                    return Some(format!(
                        "MATCH: buy {} units @ {} matched with ask @ {}",
                        incoming.quantity, incoming.price, ask.price
                    ));
                }
            }
        } else {
            for (_, bid) in &self.bids {
                if incoming.price <= bid.price {
                    return Some(format!(
                        "MATCH: sell {} units @ {} matched with bid @ {}",
                        incoming.quantity, incoming.price, bid.price
                    ));
                }
            }
        }
        None
    }

    pub fn snapshot(&self) -> String {
        let best_bid = self.bids.values()
            .map(|o| o.price)
            .fold(f64::NEG_INFINITY, f64::max);
        let best_ask = self.asks.values()
            .map(|o| o.price)
            .fold(f64::INFINITY, f64::min);
        serde_json::json!({
            "best_bid": if best_bid == f64::NEG_INFINITY { None } else { Some(best_bid) },
            "best_ask": if best_ask == f64::INFINITY { None } else { Some(best_ask) },
            "bid_count": self.bids.len(),
            "ask_count": self.asks.len(),
        }).to_string()
    }
}

// Shared state across all API handlers
type SharedBook = Arc<Mutex<OrderBook>>;

#[derive(Deserialize)]
struct PlaceOrder {
    side: String,
    price: f64,
    quantity: f64,
}

// POST /order — place a new order
async fn place_order(
    State(book): State<SharedBook>,
    Json(payload): Json<PlaceOrder>,
) -> Json<serde_json::Value> {
    let order = Order {
        id: Uuid::new_v4().to_string(),
        side: payload.side,
        price: payload.price,
        quantity: payload.quantity,
        timestamp: chrono::Utc::now().timestamp_millis(),
    };
    let mut book = book.lock().await;
    let match_result = book.add_order(order);
    Json(serde_json::json!({
        "status": "accepted",
        "match": match_result,
        "book": serde_json::from_str::<serde_json::Value>(&book.snapshot()).unwrap()
    }))
}

// GET /book — current order book snapshot
async fn get_book(State(book): State<SharedBook>) -> Json<serde_json::Value> {
    let book = book.lock().await;
    let snapshot: serde_json::Value = serde_json::from_str(&book.snapshot()).unwrap();
    Json(snapshot)
}

// GET /ws — WebSocket feed for live updates
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(book): State<SharedBook>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, book))
}

async fn handle_socket(mut socket: WebSocket, book: SharedBook) {
    let mut rx = {
        let book = book.lock().await;
        book.tx.subscribe()
    };
    while let Ok(msg) = rx.recv().await {
        if socket.send(Message::Text(msg.into())).await.is_err() {
            break;
        }
    }
}

#[tokio::main]
async fn main() {
    let (tx, _rx) = broadcast::channel(100);
    let book = Arc::new(Mutex::new(OrderBook::new(tx)));

    let app = Router::new()
        .route("/order", post(place_order))
        .route("/book", get(get_book))
        .route("/ws", get(ws_handler))
        .nest_service("/", ServeDir::new("static"))
        .with_state(book);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("CryptoStream running on http://localhost:3000");
    axum::serve(listener, app).await.unwrap();
}