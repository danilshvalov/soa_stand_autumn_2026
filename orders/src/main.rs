mod api;
mod events;
mod inventory;
mod outbox;
mod payment;
mod store;
mod worker;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;

use crate::events::EventsClient;
use crate::inventory::InventoryClient;
use crate::payment::PaymentClient;

const DB_CONNECT_TIMEOUT: Duration = Duration::from_secs(110);

pub struct AppState {
    pub pool: sqlx::PgPool,
    pub payment: PaymentClient,
    pub inventory: InventoryClient,
    pub events: EventsClient,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let database_url = require_env("DATABASE_URL")?;
    let payment_url = require_env("PAYMENT_URL")?
        .trim_end_matches('/')
        .to_string();
    let inventory_url = require_env("INVENTORY_URL")?
        .trim_end_matches('/')
        .to_string();
    let events_url = require_env("EVENTS_URL")?.trim_end_matches('/').to_string();
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse()
        .context("PORT must be a number")?;

    let pool = store::connect(&database_url).await?;

    let state = Arc::new(AppState {
        pool,
        payment: PaymentClient::new(payment_url),
        inventory: InventoryClient::new(inventory_url),
        events: EventsClient::new(events_url),
    });

    tokio::spawn(worker::run(state.clone()));
    tokio::spawn(outbox::run(state.clone()));

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .with_context(|| format!("cannot bind port {port}"))?;
    tracing::info!(port, "orders: listening");
    axum::serve(listener, api::router(state))
        .await
        .context("http server")?;
    Ok(())
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn require_env(name: &str) -> anyhow::Result<String> {
    std::env::var(name).map_err(|_| anyhow::anyhow!("environment variable {name} is not set"))
}
