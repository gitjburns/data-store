mod config;
mod error;
mod http;
mod inference;
mod state;
mod types;

use std::sync::Arc;

use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    config::{ServiceConfig, resolve_config_path_from_args},
    http::build_router,
    inference::InferenceRuntime,
    state::AppState,
};

/// Start the standalone Data Store service.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config_path = resolve_config_path_from_args()?;
    let config = ServiceConfig::load(config_path)?;
    let bind_address = config.bind_address();
    let inference = InferenceRuntime::initialize(&config);
    let state = Arc::new(AppState::new(config, inference));
    let app = build_router(state).layer(TraceLayer::new_for_http());
    let listener = TcpListener::bind(bind_address).await?;

    info!(%bind_address, "data store service listening");
    axum::serve(listener, app).await?;

    Ok(())
}

/// Initialize structured logging from RUST_LOG or a conservative default.
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("data_store_service=info,tower_http=info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
}
