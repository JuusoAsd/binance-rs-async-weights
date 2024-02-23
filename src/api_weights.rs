use crate::api::*;
use crate::futures::market::FuturesMarket;
use crate::market::Market;

use futures::Future;
use log::info;
use std::fmt::Debug;
use std::sync::Mutex;

use log::{Level, LevelFilter, Log, Metadata, Record};
use redis::Commands;
use serde_json::Value;

async fn execute_and_log<F, T, E>(future: F, message: &str)
where
    F: Future<Output = Result<T, E>>,
    T: Debug,
    E: Debug,
{
    info!("{}", message);
    let _ = future.await;
    wait().await;
}
async fn wait() { tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; }

async fn setup_rate_limiter_spot() {
    let symbol = "ETHUSDT";
    let market: Market = Binance::new(None, None);

    execute_and_log(market.get_depth(symbol), "spot_depth").await;
    execute_and_log(market.get_all_prices(), "spot_all_prices").await;
    execute_and_log(market.get_price(symbol), "spot_price").await;
    execute_and_log(market.get_average_price(symbol), "spot_average_price").await;
    execute_and_log(market.get_all_book_tickers(), "spot_all_tickers").await;
    execute_and_log(market.get_book_ticker(symbol), "spot_book_ticker").await;
    execute_and_log(market.get_24h_price_stats(symbol), "spot_24h_price_stats").await;
    execute_and_log(market.get_klines(symbol, "5m", 10, None, None), "spot_klines").await;
    execute_and_log(
        market.get_agg_trades(symbol, None, None, None, Some(10)),
        "spot_agg_trades",
    )
    .await;
}

async fn rate_limiter_setup_futures() {
    let market: FuturesMarket = Binance::new(None, None);
    let symbol = "btcusdt";
    execute_and_log(market.get_depth(symbol), "futures_depth").await;
    execute_and_log(market.get_trades(symbol), "futures_trades").await;
    execute_and_log(market.get_price(symbol), "futures_price").await;
    execute_and_log(market.get_all_book_tickers(), "futures_all_tickers").await;
    execute_and_log(market.get_book_ticker(symbol), "futures_book_ticker").await;
    execute_and_log(market.get_24h_price_stats(symbol), "futures_24h_price_stats").await;
    execute_and_log(market.get_klines(symbol, "5m", 10u16, None, None), "futures_klines").await;
    execute_and_log(
        market.get_agg_trades(symbol, None, None, None, 500u16),
        "futures_agg_trades",
    )
    .await;
    execute_and_log(market.get_mark_prices(None), "futures_mark_prices").await;
    execute_and_log(market.open_interest(symbol), "futures_open_interest").await;
    execute_and_log(
        market.get_funding_rate(symbol, None, None, 10u16),
        "futures_funding_rate",
    )
    .await;
}

pub async fn setup_rate_limiter() {
    setup_rate_limiter_spot().await;
    rate_limiter_setup_futures().await;
}

struct CustomLogHandler {
    current_usage_spot: Mutex<i32>,
    current_usage_fut: Mutex<i32>,
    prev_key: Mutex<String>,
    db_client: Mutex<redis::Connection>,
}
impl CustomLogHandler {
    fn new() -> CustomLogHandler {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let con = client.get_connection().unwrap();

        CustomLogHandler {
            current_usage_spot: Mutex::new(0),
            current_usage_fut: Mutex::new(0),
            prev_key: Mutex::new("empty".to_string()),
            db_client: Mutex::new(con),
        }
    }

    fn update_weight(&self, market_type: &str, new_weight: i32) {
        // Determine the correct current_usage based on market_type
        let current_usage = match market_type {
            "spot" => &self.current_usage_spot,
            _ => &self.current_usage_fut,
        };

        let old_weight = *current_usage.lock().expect("Lock current usage");
        if new_weight < old_weight {
            println!("{} - {}", *self.prev_key.lock().expect("Lock prev_key"), new_weight);
        } else {
            println!(
                "{} - {}",
                *self.prev_key.lock().expect("Lock prev_key"),
                new_weight - old_weight
            );
        }

        // Update Redis and the current usage
        let mut con = self.db_client.lock().expect("Lock db_client");
        let key = self.prev_key.lock().expect("Lock prev_key").clone();
        let _: Result<(), _> = con.set(&key, new_weight);

        *current_usage.lock().expect("Lock current usage") = new_weight;
    }

    fn process_log(&self, v: Value) {
        if let Some(new_weight_str) = v
            .get("x-mbx-used-weight")
            .and_then(|v| v.as_str())
            .or_else(|| v.get("x-mbx-used-weight-1m").and_then(|v| v.as_str()))
        {
            if let Ok(new_weight) = new_weight_str.parse::<i32>() {
                let market_type = if self.prev_key.lock().expect("Lock prev_key").starts_with("spot") {
                    "spot"
                } else {
                    "futures"
                };
                self.update_weight(market_type, new_weight);
            }
        }
    }
}
impl Log for CustomLogHandler {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info // Adjust this based on what levels you want to capture
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let val: Result<Value, serde_json::Error> =
                serde_json::from_str(record.clone().args().to_string().as_str());
            match val {
                Ok(v) => {
                    self.process_log(v);
                }
                Err(_) => {
                    let mut prev_val = self.prev_key.lock().unwrap();
                    *prev_val = record.args().to_string();
                }
            }
        }
    }

    fn flush(&self) {}
}

pub async fn setup_to_db() {
    log::set_boxed_logger(Box::new(CustomLogHandler::new()))
        .map(|()| log::set_max_level(LevelFilter::Info))
        .unwrap(); // Adjust level as needed

    setup_rate_limiter().await;
}
