mod config;
mod docling;
mod error;
mod http;
mod inference;
mod logging;
mod source;
mod state;
mod storage;
mod types;
mod units;

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tower_http::trace::TraceLayer;
use tracing::{error, info};

use crate::{
    config::{CliOptions, ServiceConfig, resolve_cli_options_from_args},
    http::build_router,
    inference::InferenceRuntime,
    logging::init_file_logging,
    state::AppState,
    storage::{StorageRuntime, setup_storage},
};

/// Start the standalone Data Store service.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli_options = resolve_cli_options_from_args()?;
    // Bootstrap output stays on stdout so a launcher can find config/log
    // diagnostics before it backgrounds the service.
    println!(
        "data-store bootstrap config_path={}",
        cli_options.config_path.display()
    );
    let config = ServiceConfig::load(cli_options.config_path.clone())?;
    let resolved_log_path = config.logging.resolved_file_path();
    println!(
        "data-store bootstrap logging.file_path={} logging.resolved_file_path={} logging.level={}",
        config.logging.file_path.display(),
        resolved_log_path.display(),
        config.logging.level.as_str()
    );
    // Operational logs switch to the configured file here. Config/CLI failures
    // before this point still surface through stdout/stderr.
    let logging = init_file_logging(&config.logging)?;
    println!(
        "data-store bootstrap file_logging=initialized path={}",
        logging.resolved_file_path.display()
    );
    info!(
        event = "service.bootstrap",
        config_path = %cli_options.config_path.display(),
        log_file_path = %logging.resolved_file_path.display(),
        log_level = config.logging.level.as_str(),
        "service bootstrap completed"
    );
    if cli_options.setup_storage {
        match setup_storage(&config.storage) {
            Ok(db_path) => {
                println!("storage schema ready at {}", db_path.display());
                info!(
                    event = "storage.setup.completed",
                    db_path = %db_path.display(),
                    "storage setup completed"
                );
                return Ok(());
            }
            Err(source) => {
                error!(
                    event = "storage.setup.failed",
                    error = %source,
                    "storage setup failed"
                );
                return Err(source.into());
            }
        }
    }
    if cli_options.smoke_dense {
        run_dense_smoke(&cli_options, &config)?;
        return Ok(());
    }

    let bind_address = config.bind_address();
    // Model and storage initialization can be slow, so print the bind target
    // before those readiness checks begin.
    println!("data-store bootstrap bind_address={bind_address}");
    info!(
        event = "service.initializing",
        %bind_address,
        "initializing inference and storage"
    );
    let inference = InferenceRuntime::initialize(&config);
    let storage = StorageRuntime::open(
        &config.storage,
        &config.models.dense,
        &config.models.colbert,
    );
    match &inference {
        Ok(_) => info!(
            event = "inference.initialized",
            "inference initialized successfully"
        ),
        Err(source) => error!(
            event = "inference.initialization_failed",
            error = %source,
            "inference initialization failed"
        ),
    }
    match &storage {
        Ok(_) => info!(
            event = "storage.initialized",
            "storage initialized successfully"
        ),
        Err(source) => error!(
            event = "storage.initialization_failed",
            error = %source,
            "storage initialization failed"
        ),
    }
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

    // The admin token is intentionally stdout-only; it must not be persisted in
    // config, SQLite, or the service log file.
    println!("admin_shutdown_token={admin_shutdown_token}");
    info!(
        event = "service.listening",
        %bind_address,
        "data store service listening"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown_signal(shutdown_receiver))
        .await?;
    info!(event = "service.stopped", "data store service stopped");

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

    info!(
        event = "inference.smoke.completed",
        config_path = %cli_options.config_path.display(),
        "dense smoke completed"
    );
    Ok(())
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
