mod config;
mod docling;
mod error;
mod http;
mod inference;
mod source;
mod state;
mod storage;
mod types;
mod units;

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    config::{CliOptions, ServiceConfig, resolve_cli_options_from_args},
    http::build_router,
    inference::InferenceRuntime,
    state::AppState,
    storage::{StorageRuntime, setup_storage},
};

/// Start the standalone Data Store service.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli_options = resolve_cli_options_from_args()?;
    let config = ServiceConfig::load(cli_options.config_path.clone())?;
    if cli_options.setup_storage {
        let db_path = setup_storage(&config.storage)?;
        println!("storage schema ready at {}", db_path.display());
        return Ok(());
    }
    if cli_options.smoke_dense {
        run_dense_smoke(&cli_options, &config)?;
        return Ok(());
    }

    let bind_address = config.bind_address();
    let inference = InferenceRuntime::initialize(&config);
    let storage = StorageRuntime::open(&config.storage, &config.models.dense);
    let admin_shutdown_token = generate_admin_shutdown_token()?;
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let state = Arc::new(AppState::new(
        config,
        inference,
        storage,
        admin_shutdown_token.clone(),
        shutdown_sender,
    ));
    let app = build_router(state).layer(TraceLayer::new_for_http());
    let listener = TcpListener::bind(bind_address).await?;

    println!("admin_shutdown_token={admin_shutdown_token}");
    info!(%bind_address, "data store service listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown_signal(shutdown_receiver))
        .await?;

    Ok(())
}

/// Initialize inference, report dense readiness, and exit without starting HTTP.
fn run_dense_smoke(cli_options: &CliOptions, config: &ServiceConfig) -> anyhow::Result<()> {
    let runtime = InferenceRuntime::initialize(config)?;
    println!(
        "dense smoke initialized from {}",
        cli_options.config_path.display()
    );
    for detail in runtime.health_details() {
        println!("{detail}");
    }

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

/// Generate one startup-scoped admin shutdown token from OS randomness.
fn generate_admin_shutdown_token() -> anyhow::Result<String> {
    let mut token_bytes = [0u8; 32];
    getrandom::fill(&mut token_bytes)?;
    Ok(hex_encode(&token_bytes))
}

/// Wait until the protected admin shutdown route signals service termination.
async fn wait_for_shutdown_signal(receiver: oneshot::Receiver<()>) {
    let _ = receiver.await;
}

/// Encode bytes as lowercase hexadecimal without adding another dependency.
fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for value in bytes {
        output.push_str(&format!("{value:02x}"));
    }

    output
}
