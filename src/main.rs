mod acquisition;
mod activation;
mod annotations;
mod artifact_store;
mod assembly;
mod canonical;
mod config;
mod connectors;
mod deletion;
mod docling;
mod docling_activity;
mod dry_run;
mod error;
mod events;
mod hot_plane;
mod http;
mod identity;
mod ids;
mod inference;
mod logging;
mod model;
mod operations;
mod parse;
mod policy;
mod primitives;
mod projections;
mod query;
mod restore;
mod scheduler;
mod snapshot;
mod source;
mod state;
mod types;
mod util;

use std::{
    env,
    fs::{self, OpenOptions},
    io::{self, BufRead, BufReader, Read, Write},
    os::unix::{
        fs::OpenOptionsExt,
        io::{AsRawFd, FromRawFd},
        net::UnixStream,
        process::CommandExt,
    },
    path::PathBuf,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::Instant,
};

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

use crate::{
    config::{CliOptions, ServiceConfig, resolve_cli_options_from_args},
    error::ApiError,
    hot_plane::setup_fabric_storage,
    http::build_router,
    inference::InferenceRuntime,
    logging::init_file_logging,
    state::{AnnotationHealth, AppState, FabricHealth, ShutdownSignal, SyncHealth},
};

enum ServiceProcessRole {
    ParentComplete,
    Service(StartupReporter),
}

enum StartupReporter {
    Stdout {
        active_progress_chars: usize,
        started_at: Instant,
    },
    Pipe {
        stream: UnixStream,
        started_at: Instant,
    },
}

struct AdminTokenFile {
    path: PathBuf,
    token: String,
}

struct StartupRelayOutcome {
    startup_failed: bool,
    last_status_line: Option<String>,
    last_progress_line: Option<String>,
}

const STARTUP_PROGRESS_PREFIX: &str = "__data_store_progress__";
const BACKGROUND_STARTUP_FD_ENV: &str = "DATA_STORE_BACKGROUND_STARTUP_FD";
const F_GETFD: i32 = 1;
const F_SETFD: i32 = 2;
const FD_CLOEXEC: i32 = 1;

// Direct POSIX declarations instead of a libc dependency: setsid detaches the
// daemonized child from its controlling terminal; fcntl clears FD_CLOEXEC on
// the inherited startup-status fd so it survives exec into the child.
unsafe extern "C" {
    fn setsid() -> i32;
    fn fcntl(fd: i32, cmd: i32, ...) -> i32;
}

impl StartupReporter {
    /// Return the human-readable execution mode for startup handoff output.
    fn mode_label(&self) -> &'static str {
        match self {
            Self::Stdout { .. } => "foreground",
            Self::Pipe { .. } => "background",
        }
    }

    /// Return elapsed startup time from the reporter's service-process boundary.
    fn elapsed_ms(&self) -> u64 {
        self.started_at().elapsed().as_millis() as u64
    }

    /// Return the monotonic clock captured when this service process began reporting startup.
    fn started_at(&self) -> Instant {
        match self {
            Self::Stdout { started_at, .. } | Self::Pipe { started_at, .. } => *started_at,
        }
    }

    /// Emit one operator-visible startup status line.
    fn report(&mut self, message: impl AsRef<str>) -> Result<(), ApiError> {
        let message = message.as_ref();
        if message.starts_with("admin_shutdown_token=") {
            self.write_status_line(message)?;
            info!(
                event = "startup.admin_token_reported",
                mode = self.mode_label(),
                token_present = true,
                elapsed_ms = self.elapsed_ms(),
                "startup admin token reported to operator channel"
            );
            return Ok(());
        }
        self.write_status_line(message)?;
        info!(
            event = "startup.status_reported",
            mode = self.mode_label(),
            stage = startup_message_stage(message),
            message,
            elapsed_ms = self.elapsed_ms(),
            "startup status reported"
        );

        Ok(())
    }

    /// Emit the admin token to the operator channel while logging only a sanitized durable event.
    fn report_admin_shutdown_token(&mut self, token: &str) -> Result<(), ApiError> {
        self.write_status_line(&format!("admin_shutdown_token={token}"))?;
        info!(
            event = "startup.admin_token_reported",
            mode = self.mode_label(),
            token_present = true,
            elapsed_ms = self.elapsed_ms(),
            "startup admin token reported to operator channel"
        );

        Ok(())
    }

    /// Write one startup status line to stdout or the background parent pipe.
    fn write_status_line(&mut self, message: &str) -> Result<(), ApiError> {
        match self {
            Self::Stdout {
                active_progress_chars,
                ..
            } => {
                if *active_progress_chars > 0 {
                    println!();
                    *active_progress_chars = 0;
                }
                println!("{message}");
                std::io::stdout()
                    .flush()
                    .map_err(|source| ApiError::InternalIo {
                        message: format!("failed to flush startup stdout: {source}"),
                    })?;
            }
            Self::Pipe { stream, .. } => {
                writeln!(stream, "{message}").map_err(|source| ApiError::InternalIo {
                    message: format!("failed to write startup status to parent: {source}"),
                })?;
                stream.flush().map_err(|source| ApiError::InternalIo {
                    message: format!("failed to flush startup status to parent: {source}"),
                })?;
            }
        }

        Ok(())
    }

    /// Emit a transient startup progress update that the terminal can overwrite in place.
    fn report_progress(&mut self, message: impl AsRef<str>) -> Result<(), ApiError> {
        let message = message.as_ref();
        match self {
            Self::Stdout {
                active_progress_chars,
                ..
            } => {
                *active_progress_chars =
                    write_active_terminal_line(message, *active_progress_chars)?;
            }
            Self::Pipe { stream, .. } => {
                writeln!(stream, "{STARTUP_PROGRESS_PREFIX}{message}").map_err(|source| {
                    ApiError::InternalIo {
                        message: format!("failed to write startup progress to parent: {source}"),
                    }
                })?;
                stream.flush().map_err(|source| ApiError::InternalIo {
                    message: format!("failed to flush startup progress to parent: {source}"),
                })?;
            }
        }
        info!(
            event = "startup.progress_reported",
            mode = self.mode_label(),
            stage = startup_message_stage(message),
            message,
            elapsed_ms = self.elapsed_ms(),
            "startup progress reported"
        );

        Ok(())
    }

    /// Close the startup handoff channel so the background parent can exit.
    fn close(self) {
        if let Self::Stdout {
            active_progress_chars,
            ..
        } = self
            && active_progress_chars > 0
        {
            println!();
        }
    }
}

/// Start the standalone Data Store service.
fn main() -> anyhow::Result<()> {
    let cli_options = resolve_cli_options_from_args()?;
    // Bootstrap output stays on stdout so a launcher can find config/log
    // diagnostics before it backgrounds the service.
    println!(
        "data-store bootstrap config_path={}",
        cli_options.config_path.display()
    );
    let config = ServiceConfig::load(cli_options.config_path.clone())?;
    let resolved_log_path = config.logging.resolved_file_path(config.config_root());
    println!(
        "data-store bootstrap logging.file_path={} logging.resolved_file_path={} logging.level={}",
        config.logging.file_path.display(),
        resolved_log_path.display(),
        config.logging.level.as_str()
    );
    // Operational logs switch to the configured file here. Config/CLI failures
    // before this point still surface through stdout/stderr.
    let logging = init_file_logging(&config.logging, config.config_root())?;
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
        // One deliberate operator action sets up the fabric hot plane. The
        // legacy schema plane was retired at cluster CR; the fabric plane is
        // the only durable store.
        match setup_fabric_storage(&config.storage.index_root) {
            Ok(db_path) => {
                println!("fabric storage schema ready at {}", db_path.display());
                info!(
                    event = "fabric_storage.setup.completed",
                    db_path = %db_path.display(),
                    "fabric storage setup completed"
                );
                return Ok(());
            }
            Err(source) => {
                error!(
                    event = "fabric_storage.setup.failed",
                    error = %source,
                    "fabric storage setup failed"
                );
                return Err(source.into());
            }
        }
    }
    if cli_options.smoke_dense {
        run_dense_smoke(&cli_options, &config)?;
        return Ok(());
    }
    if let Some(groups_per_source) = cli_options.annotation_dry_run {
        // CA2-P5 annotation dry-run mode: one deliberate operator pass —
        // scan → acquire → parse per source, sample-annotate the first N
        // entity/relation groups, then serve the vocabulary inspection
        // surface until POST /shutdown. Always foreground (an interactive
        // ruleset-authoring session, not a daemon); no scheduler, worker, or
        // inference runtime exists in this mode.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        return runtime.block_on(run_annotation_dry_run_mode(config, groups_per_source));
    }

    let bind_address = config.bind_address();
    println!("data-store bootstrap bind_address={bind_address}");

    let process_role = enter_service_process(cli_options.foreground)?;
    let reporter = match process_role {
        ServiceProcessRole::ParentComplete => return Ok(()),
        ServiceProcessRole::Service(reporter) => reporter,
    };
    let admin_shutdown_token = generate_admin_shutdown_token()?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_http_service(config, admin_shutdown_token, reporter))
}

/// Bind HTTP before lengthy dependency initialization, then report readiness and serve until shutdown.
async fn run_http_service(
    config: ServiceConfig,
    admin_shutdown_token: String,
    mut reporter: StartupReporter,
) -> anyhow::Result<()> {
    let startup_started_at = Instant::now();
    let bind_address = config.bind_address();
    reporter.report(format!(
        "data-store startup mode={} bind_address={bind_address}",
        reporter.mode_label()
    ))?;
    reporter.report(format!(
        "data-store startup http=binding bind_address={bind_address}"
    ))?;
    let http_bind_started_at = Instant::now();
    info!(
        event = "startup.http_bind_started",
        %bind_address,
        elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
        "startup HTTP bind started"
    );
    let listener = match TcpListener::bind(bind_address).await {
        Ok(listener) => {
            info!(
                event = "startup.http_bound",
                %bind_address,
                elapsed_ms = http_bind_started_at.elapsed().as_millis() as u64,
                "startup HTTP bind completed"
            );
            listener
        }
        Err(source) => {
            reporter.report(format!(
                "data-store startup http=bind_failed bind_address={bind_address} error=\"{source}\""
            ))?;
            error!(
                event = "startup.http_bind_failed",
                %bind_address,
                error = %source,
                elapsed_ms = http_bind_started_at.elapsed().as_millis() as u64,
                "startup HTTP bind failed"
            );
            error!(
                event = "startup.fatal",
                stage = "http_bind",
                %bind_address,
                error = %source,
                elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
                "startup failed during HTTP bind"
            );
            reporter.report(format!("data-store startup fatal=\"{source}\""))?;
            return Err(source.into());
        }
    };
    reporter.report_admin_shutdown_token(&admin_shutdown_token)?;
    let admin_token_file = match AdminTokenFile::write_current(&config, &admin_shutdown_token) {
        Ok(token_file) => token_file,
        Err(source) => {
            error!(
                event = "startup.fatal",
                stage = "admin_token_file_publish",
                %bind_address,
                error = %source,
                elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
                "startup failed while publishing admin token file"
            );
            reporter.report(format!("data-store startup fatal=\"{source}\""))?;
            return Err(source.into());
        }
    };
    reporter.report(format!(
        "data-store startup admin_token_file=ready path={}",
        admin_token_file.path.display()
    ))?;
    info!(
        event = "service.initializing",
        %bind_address,
        "initializing inference"
    );

    reporter.report("data-store startup inference=initializing")?;
    let inference_result = {
        let mut report_inference_progress = |message: &str| {
            let startup_message = format!("data-store startup inference={message}");
            if uses_count_progress(message) {
                reporter.report_progress(startup_message)
            } else {
                reporter.report(startup_message)
            }
        };
        InferenceRuntime::initialize_with_progress(&config, &mut report_inference_progress)
    };
    let inference = match inference_result {
        Ok(runtime) => {
            reporter.report(format!(
                "data-store startup inference=ready details=\"{}\"",
                runtime.health_details().join(" | ")
            ))?;
            info!(
                event = "inference.initialized",
                "inference initialized successfully"
            );
            runtime
        }
        Err(source) => {
            reporter.report(format!(
                "data-store startup inference=not_ready error=\"{source}\""
            ))?;
            error!(
                event = "inference.initialization_failed",
                %bind_address,
                error = %source,
                elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
                "inference initialization failed"
            );
            error!(
                event = "startup.fatal",
                stage = "inference_initialization",
                %bind_address,
                error = %source,
                elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
                "startup failed during inference initialization"
            );
            reporter.report(format!("data-store startup fatal=\"{source}\""))?;
            admin_token_file.cleanup_if_current();
            return Err(source.into());
        }
    };

    // Fabric hot-plane pre-check, reporting only: the scheduler thread owns
    // the runtime validation gate and re-validates before its first cycle;
    // this synchronous check gives the startup handoff a truthful prediction
    // of the sync component's readiness. It is a prediction, not the health
    // source of truth: /v1/health reads the scheduler-published sync slot,
    // which starts pending, so a probe in the brief window before the
    // scheduler's first publish reports ready=false even after this line
    // printed ready=true. A missing or invalid fabric plane is not fatal —
    // the service serves with ready=false and health explains why.
    reporter.report("data-store startup sync=validating")?;
    let fabric_ready_at_startup = match crate::hot_plane::open_read(&config.storage.index_root)
        .and_then(|connection| crate::hot_plane::validate_fabric_schema(&connection))
    {
        Ok(()) => {
            reporter.report("data-store startup sync=ready")?;
            true
        }
        Err(source) => {
            reporter.report(format!(
                "data-store startup sync=not_ready error=\"{source}\""
            ))?;
            warn!(
                event = "startup.fabric_not_ready",
                error = %source,
                "fabric hot plane is not ready; acquisition scheduler will report unready sync health (run --setup-storage)"
            );
            false
        }
    };
    // CA2 policy documents (D3 amendment): the two operator-editable external
    // policy documents load ONCE here — strict validation, fatal on failure,
    // matching the config posture (a service running under an unloadable
    // ruleset has no valid identity to record). Loaded BEFORE identity
    // capture below so both content hashes fold into the captured
    // ApplicationIdentity.
    let loaded_policies = policy::load_entity_match_policy(
        &config
            .policies
            .resolved_entity_match_file_path(config.config_root()),
    )
    .and_then(|entity_match| {
        let naming = policy::load_annotator_naming_policy(
            &config
                .policies
                .resolved_annotator_naming_file_path(config.config_root()),
        )?;
        Ok((entity_match, naming))
    });
    let (entity_match_policy, annotator_naming_policy) = match loaded_policies {
        Ok(policies) => policies,
        Err(source) => {
            error!(
                event = "startup.fatal",
                stage = "policy_document_load",
                %bind_address,
                error = %source,
                elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
                "startup failed loading an operator policy document"
            );
            reporter.report(format!("data-store startup fatal=\"{source}\""))?;
            admin_token_file.cleanup_if_current();
            return Err(source.into());
        }
    };
    // System-assigned policy versioning (CA2 ruling 5): hash-change detection
    // appends to the append-only policy_versions registry, atomically with its
    // policy.changed event, under one IMMEDIATE transaction. Runs ONLY when
    // the fabric pre-check passed: with the plane absent (commissioning
    // state), the load logs above already carry the hashes and registration
    // defers to the next valid-plane startup — versions are audit labels;
    // behavior keys on the hashes everywhere. A registration failure on a
    // VALID plane is fatal for the same reason identity-capture failure is:
    // the registry is the audit record of the ruleset the service runs under.
    if fabric_ready_at_startup {
        let registration = register_policy_versions(
            &config.storage.index_root,
            &entity_match_policy.content_hash,
            &annotator_naming_policy.content_hash,
        );
        if let Err(source) = registration {
            error!(
                event = "startup.fatal",
                stage = "policy_version_registration",
                %bind_address,
                error = %source,
                elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
                "startup failed registering policy versions"
            );
            reporter.report(format!("data-store startup fatal=\"{source}\""))?;
            admin_token_file.cleanup_if_current();
            return Err(source.into());
        }
    }
    // The scheduler owns clones of its inputs because `config` moves into
    // AppState next; the shared health slot is the only channel between the
    // scheduler thread and health reporting.
    let scheduler_corpus_root = config.storage.corpus_root.clone();
    let scheduler_index_root = config.storage.index_root.clone();
    let scheduler_governance_domain = config.connectors.filesystem.governance_domain.clone();
    let scheduler_docling = config.docling.clone();
    let annotation_index_root = config.storage.index_root.clone();
    let annotation_annotator = config.models.annotator.clone();
    let annotation_config_root = config.config_root().to_path_buf();
    // §30.2 application identity captured ONCE here, right after config load and
    // validation, then threaded explicitly into the scheduler (and from there to
    // every snapshot-minting site) per the 2026-07-16 ruling — no global, no
    // OnceLock. Computed before `config` moves into AppState below. Capture
    // failure is fatal: a service that cannot pin the identity its forensic
    // snapshots stamp has nothing valid to record, so it exits with the same
    // startup.fatal reporting as a failed scheduler spawn.
    let application_identity = match identity::ApplicationIdentity::capture(
        &config,
        &entity_match_policy.content_hash,
        &annotator_naming_policy.content_hash,
    ) {
        Ok(identity) => identity,
        Err(source) => {
            error!(
                event = "startup.fatal",
                stage = "application_identity_capture",
                %bind_address,
                error = %source,
                elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
                "startup failed capturing the application identity"
            );
            reporter.report(format!("data-store startup fatal=\"{source}\""))?;
            admin_token_file.cleanup_if_current();
            return Err(source.into());
        }
    };
    let sync_health = Arc::new(Mutex::new(SyncHealth::startup_pending()));
    // C10b diagnostic-only health slots: the scheduler publishes fabric counts,
    // the annotation worker its own state. Separate from `sync_health` so
    // neither slot gates readiness (the set stays {inference, sync}).
    let fabric_health = Arc::new(Mutex::new(FabricHealth::default()));
    let annotation_health = Arc::new(Mutex::new(AnnotationHealth::startup_pending()));
    let shutdown_signal = Arc::new(ShutdownSignal::default());
    // One cutover-barrier registry per process (§31.1): activation via the
    // scheduler and the future C10a accept disposition must serialize on the
    // SAME per-source barriers (discard deliberately takes none — no pointer
    // swap), so the registry is constructed here and cloned outward. The
    // C7/C8 query-side consumers reach it the same way.
    let cutover_registry = Arc::new(state::CutoverRegistry::new());
    // C6 projection-runtime inputs read BEFORE `config`/`inference` move into
    // AppState below. The dense/colbert runtimes are cheap Clone handles; the
    // expected vector widths come from config.models.{dense,colbert}.dimension.
    let scheduler_dense_runtime = inference.dense.clone();
    let scheduler_colbert_runtime = inference.colbert.clone();
    let scheduler_dense_dimension = config.models.dense.dimension as usize;
    let scheduler_colbert_dimension = config.models.colbert.dimension as usize;
    // The shared active dense cache (§1.6 successor). Phase 1 hands the scheduler
    // this clone; the second clone below rides AppState so the C7 dense
    // retrieval channel scores against the same swapped planes (C8d-1 seam).
    let dense_cache = Arc::new(projections::dense_cache::DenseCache::new());
    let state = Arc::new(AppState::new(
        config,
        state::InferenceSlot::Ready(inference),
        admin_shutdown_token.clone(),
        Arc::clone(&shutdown_signal),
        Arc::clone(&sync_health),
        Arc::clone(&fabric_health),
        Arc::clone(&annotation_health),
        Arc::clone(&dense_cache),
        Arc::clone(&cutover_registry),
        // Cloned onto AppState so HTTP snapshot/restore handlers stamp the same
        // identity the scheduler stamps; the original moves into scheduler::start
        // below (C10 resolution 7).
        application_identity.clone(),
        // CA2 entity-match policy: the loaded document moves into AppState (its
        // content hash was already folded into the identity capture above); the
        // query pipeline's graph channel is its only runtime consumer.
        entity_match_policy.document,
    ));
    // The projection runtime's model-call gate is the SAME process-global gate
    // AppState holds (`model_call_gate_handle`), so scheduler-thread model calls
    // and HTTP-path model calls serialize on one instance (§1.5). Constructed
    // here after AppState::new because the gate is created inside AppState.
    let scheduler_projection_runtime = scheduler::ProjectionRuntime {
        dense: scheduler_dense_runtime,
        colbert: scheduler_colbert_runtime,
        dense_dimension: scheduler_dense_dimension,
        colbert_dimension: scheduler_colbert_dimension,
        gate: state.model_call_gate_handle(),
        dense_cache: Arc::clone(&dense_cache),
    };
    // Spawn the acquisition scheduler once shared state exists. Spawn failure
    // is fatal: the sync component gates readiness, so a service whose
    // scheduler can never run would sit permanently unready with no operator
    // signal beyond this boundary.
    let scheduler_handle = match scheduler::start(
        scheduler_corpus_root,
        scheduler_index_root,
        scheduler_governance_domain,
        scheduler_docling,
        Arc::clone(&cutover_registry),
        scheduler_projection_runtime,
        application_identity,
        Arc::clone(&shutdown_signal),
        sync_health,
        fabric_health,
    ) {
        Ok(handle) => handle,
        Err(source) => {
            error!(
                event = "startup.fatal",
                stage = "scheduler_spawn",
                %bind_address,
                error = %source,
                elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
                "startup failed spawning the sync scheduler thread"
            );
            reporter.report(format!("data-store startup fatal=\"{source}\""))?;
            admin_token_file.cleanup_if_current();
            return Err(source.into());
        }
    };
    // Spawn the annotation worker (CA) beside the scheduler: discovery-based
    // post-activation annotation builds. Deliberately NOT readiness-critical —
    // an unreachable annotator endpoint or a parked worker degrades
    // annotations visibly (freshness rows, worker logs, and the diagnostic-only
    // annotation health slot) without gating service readiness.
    let annotation_worker_handle = annotations::worker::start(
        annotation_index_root,
        annotation_annotator,
        annotation_config_root,
        // CA2-P3: the operator-loaded naming rules compose into the entity/
        // relation producer prompts (identity-bearing); the document moves
        // here — its content hash was already folded into identity capture.
        annotator_naming_policy.document,
        Arc::clone(&shutdown_signal),
        annotation_health,
    );
    let app = build_router(state).layer(TraceLayer::new_for_http());
    reporter.report(format!(
        "data-store startup http=listening bind_address={bind_address}"
    ))?;
    info!(
        event = "startup.http_listening",
        %bind_address,
        elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
        "startup HTTP listener is ready"
    );
    let health_url = format!("http://{bind_address}/v1/health");
    // Top-level readiness is inference plus the sync component: inference is
    // true by construction here (its failure returns above), so the
    // pre-checked fabric state decides the reported flag.
    reporter.report(format!(
        "data-store startup ready={fabric_ready_at_startup} inference=true sync={fabric_ready_at_startup} health_url={health_url}"
    ))?;
    info!(
        event = "startup.ready",
        %bind_address,
        health_url = %health_url,
        ready = fabric_ready_at_startup,
        inference_ready = true,
        sync_ready = fabric_ready_at_startup,
        elapsed_ms = startup_started_at.elapsed().as_millis() as u64,
        "startup readiness completed"
    );
    reporter.close();
    info!(
        event = "service.listening",
        %bind_address,
        ready = fabric_ready_at_startup,
        inference_ready = true,
        sync_ready = fabric_ready_at_startup,
        "data store service listening"
    );
    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown_signal(Arc::clone(&shutdown_signal)))
        .await;
    admin_token_file.cleanup_if_current();
    // Serve has returned — gracefully or with a transport error. On the
    // error path nothing has tripped the shutdown signal yet, and the
    // scheduler and annotation worker may be mid-sleep; request shutdown
    // explicitly so the joins below are bounded by one wait_timeout wakeup
    // instead of a full idle sleep.
    if let Err(reason) = shutdown_signal.request() {
        error!(
            event = "scheduler.shutdown_request_failed",
            error = %reason,
            "failed to signal scheduler shutdown before join"
        );
    }
    if scheduler_handle.join().is_err() {
        error!(
            event = "scheduler.join_panicked",
            "sync scheduler thread panicked before shutdown"
        );
    }
    if annotation_worker_handle.join().is_err() {
        error!(
            event = "annotation_worker.join_panicked",
            "annotation worker thread panicked before shutdown"
        );
    }
    serve_result?;
    info!(event = "service.stopped", "data store service stopped");

    Ok(())
}

/// Register both operator policy documents' system-assigned versions in one
/// IMMEDIATE transaction (CA2 ruling 5): hash-change detection appends to the
/// append-only `policy_versions` registry atomically with its `policy.changed`
/// event. Shared by the normal startup path and the annotation dry-run mode so
/// both record versions through identical mechanics.
fn register_policy_versions(
    index_root: &std::path::Path,
    entity_match_hash: &str,
    annotator_naming_hash: &str,
) -> Result<(), ApiError> {
    let mut connection = hot_plane::open_write(index_root)?;
    let tx = hot_plane::begin_write_transaction(
        &mut connection,
        "policy",
        "policy_version_registration",
    )?;
    policy::register_policy_version(&tx, policy::POLICY_ID_ENTITY_MATCH, entity_match_hash)?;
    policy::register_policy_version(
        &tx,
        policy::POLICY_ID_ANNOTATOR_NAMING,
        annotator_naming_hash,
    )?;
    hot_plane::commit_transaction(tx, "policy", "policy_version_registration")
}

/// CA2-P5 annotation dry-run mode. One deliberate operator pass for the
/// ruleset-authoring loop: acquisition + parse across the corpus (parses left
/// READY for the next normal start's §13.5 GateExisting adoption — Docling is
/// paid once), entity/relation producers sampled over the first N section
/// groups per source, then the reduced inspection router served until
/// `POST /shutdown`.
///
/// Deliberate divergences from the normal startup, all mode-defining:
/// - NO inference runtime: `AppState` carries an explicit `Err`, so any
///   inference-touching path fails loudly; health honestly reports the
///   inference component not-ready with the mode message as its detail.
/// - NO scheduler or annotation-worker thread: the pass runs once on a
///   blocking task; the sampling driver owns all producer calls.
/// - A VALID fabric plane is REQUIRED (fatal otherwise): unlike the normal
///   serve path, this mode has no purpose without the plane — the remedy is
///   `--setup-storage` first.
/// - The reduced router serves only health, vocabulary inspection, Operation
///   reads, and shutdown; mutating admin routes would accept work nothing
///   drains here.
///
/// The HTTP listener serves DURING the pass (Docling over a corpus can take
/// a long time), so health and vocabulary are inspectable while sampling is
/// still running; pass completion or failure is logged, and a failed pass
/// keeps serving so whatever landed stays inspectable.
async fn run_annotation_dry_run_mode(
    config: ServiceConfig,
    groups_per_source: usize,
) -> anyhow::Result<()> {
    let started_at = Instant::now();
    let bind_address = config.bind_address();
    info!(
        event = "dry_run.mode_started",
        %bind_address,
        groups_per_source,
        corpus_root = %config.storage.corpus_root.display(),
        "annotation dry-run mode starting"
    );

    // Every fatal boundary in this mode logs `dry_run.fatal` with its stage
    // before returning: terminal/stderr output is not durable diagnostics
    // (DIAGNOSTICS-ONBOARDING), and these helpers return typed errors
    // expecting the caller to own the boundary — this function is that owner.
    let dry_run_fatal = |stage: &'static str, source: &ApiError| {
        error!(
            event = "dry_run.fatal",
            stage,
            error = %source,
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "annotation dry-run mode startup failed"
        );
    };

    // Operator policy documents: same strict load-or-die posture as the
    // normal path; the naming rules feed the sampled producers directly.
    let entity_match_policy = policy::load_entity_match_policy(
        &config
            .policies
            .resolved_entity_match_file_path(config.config_root()),
    )
    .inspect_err(|source| dry_run_fatal("policy_document_load", source))?;
    let annotator_naming_policy = policy::load_annotator_naming_policy(
        &config
            .policies
            .resolved_annotator_naming_file_path(config.config_root()),
    )
    .inspect_err(|source| dry_run_fatal("policy_document_load", source))?;

    // The fabric plane is REQUIRED here (fatal), unlike the normal serve
    // path's degrade-to-unready: the pass writes acquisition/parse/annotation
    // rows and the inspection surface reads them, so a missing plane leaves
    // nothing to do. Registration then runs unconditionally.
    crate::hot_plane::open_read(&config.storage.index_root)
        .and_then(|connection| crate::hot_plane::validate_fabric_schema(&connection))
        .inspect_err(|source| dry_run_fatal("fabric_plane_validation", source))?;
    register_policy_versions(
        &config.storage.index_root,
        &entity_match_policy.content_hash,
        &annotator_naming_policy.content_hash,
    )
    .inspect_err(|source| dry_run_fatal("policy_version_registration", source))?;

    let application_identity = identity::ApplicationIdentity::capture(
        &config,
        &entity_match_policy.content_hash,
        &annotator_naming_policy.content_hash,
    )
    .inspect_err(|source| dry_run_fatal("application_identity_capture", source))?;

    // Admin token: the vocabulary route and POST /shutdown are protected, so
    // the mode publishes the token file exactly like the normal path. The
    // token value goes to stdout only (foreground operator channel); the
    // durable log records presence, never the value.
    let admin_shutdown_token = generate_admin_shutdown_token()?;
    let admin_token_file = match AdminTokenFile::write_current(&config, &admin_shutdown_token) {
        Ok(token_file) => token_file,
        Err(source) => {
            error!(
                event = "dry_run.fatal",
                stage = "admin_token_file",
                error = %source,
                "dry-run mode failed publishing the admin token file"
            );
            return Err(source.into());
        }
    };
    println!("admin_shutdown_token={admin_shutdown_token}");
    info!(
        event = "dry_run.admin_token_published",
        token_present = true,
        "admin token published for the dry-run inspection surface"
    );

    let listener = TcpListener::bind(bind_address)
        .await
        .inspect_err(|source| {
            error!(
                event = "dry_run.fatal",
                stage = "http_bind",
                %bind_address,
                error = %source,
                "dry-run mode failed binding HTTP"
            );
            admin_token_file.cleanup_if_current();
        })?;

    // Pass inputs cloned out BEFORE `config` moves into AppState.
    let pass_inputs = dry_run::DryRunInputs {
        corpus_root: config.storage.corpus_root.clone(),
        index_root: config.storage.index_root.clone(),
        governance_domain: config.connectors.filesystem.governance_domain.clone(),
        docling: config.docling.clone(),
        annotator: config.models.annotator.clone(),
        config_root: config.config_root().to_path_buf(),
        naming_policy: annotator_naming_policy.document,
        groups_per_source,
    };

    let shutdown_signal = Arc::new(ShutdownSignal::default());
    let state = Arc::new(AppState::new(
        config,
        // No inference in this mode: deliberately not initialized, NOT a
        // failure. Any accidental inference-touching path still fails loudly
        // (the accessor turns this into an InferenceInit error), while health
        // reports the bare mode message instead of asserting an init failure
        // that never happened.
        state::InferenceSlot::NotInitialized,
        admin_shutdown_token.clone(),
        Arc::clone(&shutdown_signal),
        Arc::new(Mutex::new(SyncHealth::startup_pending())),
        Arc::new(Mutex::new(FabricHealth::default())),
        Arc::new(Mutex::new(AnnotationHealth::startup_pending())),
        Arc::new(projections::dense_cache::DenseCache::new()),
        Arc::new(state::CutoverRegistry::new()),
        application_identity,
        entity_match_policy.document,
    ));
    let app = http::build_dry_run_router(state).layer(TraceLayer::new_for_http());

    // The pass runs on a blocking task while the listener serves, so the
    // inspection surface answers during long Docling conversions. Completion
    // and failure are logged by the wrapper task; a failed pass deliberately
    // keeps the service up — partial vocabulary is still worth inspecting.
    let pass_shutdown = Arc::clone(&shutdown_signal);
    let pass_handle =
        tokio::task::spawn_blocking(move || dry_run::run(pass_inputs, &pass_shutdown));
    let pass_watcher = tokio::spawn(async move {
        match pass_handle.await {
            Ok(Ok(())) => info!(
                event = "dry_run.pass_completed",
                "dry-run pass complete; vocabulary is ready for inspection (POST /shutdown to end)"
            ),
            Ok(Err(source)) => error!(
                event = "dry_run.pass_failed",
                error = %source,
                "dry-run pass failed; serving whatever landed for inspection"
            ),
            Err(join_error) => error!(
                event = "dry_run.pass_panicked",
                error = %join_error,
                "dry-run pass panicked; serving whatever landed for inspection"
            ),
        }
    });

    let health_url = format!("http://{bind_address}/v1/health");
    println!("data-store dry-run serving inspection health_url={health_url}");
    info!(
        event = "dry_run.serving",
        %bind_address,
        health_url = %health_url,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "dry-run inspection surface serving"
    );

    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown_signal(Arc::clone(&shutdown_signal)))
        .await;
    admin_token_file.cleanup_if_current();
    // A shutdown mid-pass ends the pass at its next between-item probe; await
    // the watcher so the pass's terminal log lands before the process exits.
    if pass_watcher.await.is_err() {
        error!(
            event = "dry_run.pass_watcher_join_failed",
            "dry-run pass watcher task failed to join"
        );
    }
    serve_result?;
    info!(
        event = "dry_run.mode_stopped",
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "annotation dry-run mode stopped"
    );
    Ok(())
}

impl AdminTokenFile {
    /// Publish the current startup-scoped admin token to the configured owner-only runtime file.
    fn write_current(config: &ServiceConfig, token: &str) -> Result<Self, ApiError> {
        let started_at = Instant::now();
        let path = config.admin.resolved_token_file_path(config.config_root());
        info!(
            event = "admin_token_file.publish_started",
            path = %path.display(),
            stage = "start",
            permissions = "0600",
            "admin token file publication started"
        );
        if let Some(parent) = path.parent() {
            if let Err(source) = fs::create_dir_all(parent) {
                error!(
                    event = "admin_token_file.publish_directory_failed",
                    path = %path.display(),
                    directory = %parent.display(),
                    stage = "create_parent_directory",
                    error = %source,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "failed to create admin token file directory"
                );
                return Err(ApiError::InternalIo {
                    message: format!(
                        "failed to create admin token file directory {}: {source}",
                        parent.display()
                    ),
                });
            }
            info!(
                event = "admin_token_file.publish_directory_ready",
                path = %path.display(),
                directory = %parent.display(),
                stage = "create_parent_directory",
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "admin token file directory ready"
            );
        }
        match fs::remove_file(&path) {
            Ok(()) => {
                info!(
                    event = "admin_token_file.stale_file_removed",
                    path = %path.display(),
                    stage = "remove_stale_file",
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "stale admin token file removed"
                );
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                info!(
                    event = "admin_token_file.stale_file_absent",
                    path = %path.display(),
                    stage = "remove_stale_file",
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "no stale admin token file was present"
                );
            }
            Err(source) => {
                error!(
                    event = "admin_token_file.stale_file_remove_failed",
                    path = %path.display(),
                    stage = "remove_stale_file",
                    error = %source,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "failed to remove stale admin token file"
                );
                return Err(ApiError::InternalIo {
                    message: format!(
                        "failed to replace stale admin token file {}: {source}",
                        path.display()
                    ),
                });
            }
        }
        let mut file = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
        {
            Ok(file) => file,
            Err(source) => {
                error!(
                    event = "admin_token_file.publish_create_failed",
                    path = %path.display(),
                    stage = "create_token_file",
                    permissions = "0600",
                    error = %source,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "failed to create admin token file"
                );
                return Err(ApiError::InternalIo {
                    message: format!(
                        "failed to create admin token file {}: {source}",
                        path.display()
                    ),
                });
            }
        };
        if let Err(source) = writeln!(file, "{token}") {
            error!(
                event = "admin_token_file.publish_write_failed",
                path = %path.display(),
                stage = "write_token_file",
                error = %source,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "failed to write admin token file"
            );
            return Err(ApiError::InternalIo {
                message: format!(
                    "failed to write admin token file {}: {source}",
                    path.display()
                ),
            });
        }
        if let Err(source) = file.sync_all() {
            error!(
                event = "admin_token_file.publish_sync_failed",
                path = %path.display(),
                stage = "sync_token_file",
                error = %source,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "failed to flush admin token file"
            );
            return Err(ApiError::InternalIo {
                message: format!(
                    "failed to flush admin token file {}: {source}",
                    path.display()
                ),
            });
        }
        info!(
            event = "admin_token_file.publish_completed",
            path = %path.display(),
            stage = "complete",
            permissions = "0600",
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "admin token file publication completed"
        );

        Ok(Self {
            path,
            token: token.to_string(),
        })
    }

    /// Remove the runtime token file only when it still contains this service's current token.
    fn cleanup_if_current(&self) {
        info!(
            event = "admin_token_file.cleanup_started",
            path = %self.path.display(),
            "admin token file cleanup started"
        );
        let mut contents = String::new();
        let read_result = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .and_then(|mut file| {
                file.read_to_string(&mut contents)?;
                Ok(())
            });
        match read_result {
            Ok(()) => {
                if contents.trim_end_matches(['\r', '\n']) != self.token {
                    warn!(
                        event = "admin_token_file.cleanup_skipped",
                        path = %self.path.display(),
                        "admin token file did not contain the current service token"
                    );
                    return;
                }
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                info!(
                    event = "admin_token_file.cleanup_not_found",
                    path = %self.path.display(),
                    "admin token file already absent during cleanup"
                );
                return;
            }
            Err(source) => {
                warn!(
                    event = "admin_token_file.cleanup_failed",
                    path = %self.path.display(),
                    stage = "read_current_token",
                    error = %source,
                    "failed to read admin token file before cleanup"
                );
                return;
            }
        }
        if let Err(source) = fs::remove_file(&self.path) {
            warn!(
                event = "admin_token_file.cleanup_failed",
                path = %self.path.display(),
                stage = "remove_current_token_file",
                error = %source,
                "failed to remove admin token file"
            );
            return;
        }
        info!(
            event = "admin_token_file.cleanup_completed",
            path = %self.path.display(),
            "admin token file cleanup completed"
        );
    }
}

/// Decide whether this invocation should continue as the service or relay child startup output as the original parent.
fn enter_service_process(foreground: bool) -> Result<ServiceProcessRole, ApiError> {
    if let Some(fd_value) = env::var_os(BACKGROUND_STARTUP_FD_ENV) {
        let fd_text = fd_value.to_string_lossy();
        let fd = fd_text
            .parse::<i32>()
            .map_err(|source| ApiError::InternalIo {
                message: format!("invalid background startup fd {fd_text}: {source}"),
            })?;
        let stream = unsafe { UnixStream::from_raw_fd(fd) };
        return Ok(ServiceProcessRole::Service(StartupReporter::Pipe {
            stream,
            started_at: Instant::now(),
        }));
    }

    if foreground {
        println!("data-store bootstrap mode=foreground");
        return Ok(ServiceProcessRole::Service(StartupReporter::Stdout {
            active_progress_chars: 0,
            started_at: Instant::now(),
        }));
    }

    enter_background_process()
}

#[cfg(unix)]
/// Spawn a freshly exec'd detached service child while the parent relays startup status.
fn enter_background_process() -> Result<ServiceProcessRole, ApiError> {
    let started_at = Instant::now();
    let parent_pid = std::process::id();
    info!(
        event = "startup.background_spawn_started",
        parent_pid, "background service spawn started"
    );
    let (parent_stream, child_stream) = match UnixStream::pair() {
        Ok(streams) => streams,
        Err(source) => {
            error!(
                event = "startup.background_spawn_failed",
                parent_pid,
                stage = "create_startup_pipe",
                error = %source,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "failed to create background startup status pipe"
            );
            return Err(ApiError::InternalIo {
                message: format!("failed to create startup status pipe: {source}"),
            });
        }
    };
    let startup_fd = child_stream.as_raw_fd();
    if let Err(source) = clear_close_on_exec(startup_fd) {
        error!(
            event = "startup.background_spawn_failed",
            parent_pid,
            stage = "clear_close_on_exec",
            error = %source,
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "failed to prepare background startup status pipe"
        );
        return Err(source);
    }
    let executable = match env::current_exe() {
        Ok(executable) => executable,
        Err(source) => {
            error!(
                event = "startup.background_spawn_failed",
                parent_pid,
                stage = "resolve_current_executable",
                error = %source,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "failed to resolve executable for background spawn"
            );
            return Err(ApiError::InternalIo {
                message: format!(
                    "failed to resolve current executable for background spawn: {source}"
                ),
            });
        }
    };
    let mut command = Command::new(executable);
    command
        .args(env::args_os().skip(1))
        .env(BACKGROUND_STARTUP_FD_ENV, startup_fd.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    unsafe {
        command.pre_exec(|| {
            if setsid() < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(source) => {
            error!(
                event = "startup.background_spawn_failed",
                parent_pid,
                stage = "spawn_child",
                error = %source,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "failed to spawn background service process"
            );
            return Err(ApiError::InternalIo {
                message: format!("failed to spawn background service process: {source}"),
            });
        }
    };
    let child_pid = child.id();
    drop(child_stream);
    println!(
        "data-store bootstrap mode=background parent_pid={} child_pid={}",
        parent_pid, child_pid
    );
    info!(
        event = "startup.background_spawned",
        parent_pid,
        child_pid,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "background service process spawned"
    );
    let relay_outcome = relay_startup_status(parent_stream)?;
    info!(
        event = "startup.background_relay_completed",
        parent_pid,
        child_pid,
        startup_failed = relay_outcome.startup_failed,
        last_status_line = relay_outcome.last_status_line.as_deref().unwrap_or("none"),
        last_progress_line = relay_outcome
            .last_progress_line
            .as_deref()
            .unwrap_or("none"),
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "background startup relay completed"
    );
    if relay_outcome.startup_failed {
        match child.wait() {
            Ok(status) => {
                info!(
                    event = "startup.background_child_exit_observed",
                    parent_pid,
                    child_pid,
                    exit_status = %status,
                    success = status.success(),
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "background service child exit observed after startup failure"
                );
            }
            Err(source) => {
                warn!(
                    event = "startup.background_child_wait_failed",
                    parent_pid,
                    child_pid,
                    error = %source,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "failed to wait for background service child after startup failure"
                );
            }
        }
        error!(
            event = "startup.background_startup_failed",
            parent_pid,
            child_pid,
            last_status_line = relay_outcome.last_status_line.as_deref().unwrap_or("none"),
            last_progress_line = relay_outcome
                .last_progress_line
                .as_deref()
                .unwrap_or("none"),
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "background service failed during startup"
        );
        return Err(ApiError::InternalIo {
            message: "background service failed during startup".to_string(),
        });
    }
    let child_status = match child.try_wait() {
        Ok(status) => status,
        Err(source) => {
            error!(
                event = "startup.background_child_status_failed",
                parent_pid,
                child_pid,
                error = %source,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "failed to inspect background service status"
            );
            return Err(ApiError::InternalIo {
                message: format!("failed to inspect background service status: {source}"),
            });
        }
    };
    if let Some(status) = child_status {
        info!(
            event = "startup.background_child_exit_observed",
            parent_pid,
            child_pid,
            exit_status = %status,
            success = status.success(),
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "background service child exit observed during startup handoff"
        );
        if !status.success() {
            error!(
                event = "startup.background_startup_failed",
                parent_pid,
                child_pid,
                exit_status = %status,
                last_status_line = relay_outcome
                    .last_status_line
                    .as_deref()
                    .unwrap_or("none"),
                last_progress_line = relay_outcome
                    .last_progress_line
                    .as_deref()
                    .unwrap_or("none"),
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "background service exited during startup"
            );
            return Err(ApiError::InternalIo {
                message: format!("background service exited during startup with status {status}"),
            });
        }
    }

    Ok(ServiceProcessRole::ParentComplete)
}

#[cfg(not(unix))]
/// Report the unsupported background mode clearly on non-Unix targets.
fn enter_background_process() -> Result<ServiceProcessRole, ApiError> {
    Err(ApiError::InvalidCli {
        message: "--foreground is required on non-Unix targets".to_string(),
    })
}

/// Relay newline-delimited startup status from the background child to the invoking terminal.
fn relay_startup_status(stream: UnixStream) -> Result<StartupRelayOutcome, ApiError> {
    let started_at = Instant::now();
    info!(event = "startup.relay_started", "startup relay started");
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let mut startup_failed = false;
    let mut active_progress_chars = 0_usize;
    let mut last_status_line = None;
    let mut last_progress_line = None;
    loop {
        line.clear();
        let bytes_read = match reader.read_line(&mut line) {
            Ok(bytes_read) => bytes_read,
            Err(source) => {
                error!(
                    event = "startup.relay_read_failed",
                    error = %source,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "failed to read background startup status"
                );
                return Err(ApiError::InternalIo {
                    message: format!("failed to read background startup status: {source}"),
                });
            }
        };
        if bytes_read == 0 {
            break;
        }
        if let Some(progress) = line.strip_prefix(STARTUP_PROGRESS_PREFIX) {
            let progress = progress.trim_end_matches(['\r', '\n']);
            last_progress_line = Some(sanitize_startup_line(progress));
            info!(
                event = "startup.relay_progress_line",
                message = last_progress_line.as_deref().unwrap_or("none"),
                stage = startup_message_stage(progress),
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "startup progress relayed from background child"
            );
            active_progress_chars = write_active_terminal_line(progress, active_progress_chars)?;
            continue;
        }
        if active_progress_chars > 0 {
            println!();
            active_progress_chars = 0;
        }
        let status = line.trim_end_matches(['\r', '\n']);
        last_status_line = Some(sanitize_startup_line(status));
        if status.starts_with("admin_shutdown_token=") {
            info!(
                event = "startup.relay_admin_token_line",
                token_present = true,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "startup admin token line relayed from background child"
            );
        } else {
            info!(
                event = "startup.relay_status_line",
                message = last_status_line.as_deref().unwrap_or("none"),
                stage = startup_message_stage(status),
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "startup status relayed from background child"
            );
        }
        if line.contains("data-store startup fatal=")
            || line.contains("data-store startup http=bind_failed")
        {
            startup_failed = true;
            warn!(
                event = "startup.relay_failure_detected",
                message = last_status_line.as_deref().unwrap_or("none"),
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "startup relay detected background child startup failure"
            );
        }
        print!("{line}");
        std::io::stdout()
            .flush()
            .map_err(|source| ApiError::InternalIo {
                message: format!("failed to flush startup status stdout: {source}"),
            })?;
    }
    if active_progress_chars > 0 {
        println!();
    }
    info!(
        event = "startup.relay_eof",
        startup_failed,
        last_status_line = last_status_line.as_deref().unwrap_or("none"),
        last_progress_line = last_progress_line.as_deref().unwrap_or("none"),
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "startup relay reached EOF"
    );

    Ok(StartupRelayOutcome {
        startup_failed,
        last_status_line,
        last_progress_line,
    })
}

#[cfg(unix)]
/// Keep the startup-status pipe open across exec so the fresh child can report readiness.
fn clear_close_on_exec(fd: i32) -> Result<(), ApiError> {
    let flags = unsafe { fcntl(fd, F_GETFD) };
    if flags < 0 {
        return Err(ApiError::InternalIo {
            message: format!(
                "failed to inspect startup status fd flags: {}",
                io::Error::last_os_error()
            ),
        });
    }
    if unsafe { fcntl(fd, F_SETFD, flags & !FD_CLOEXEC) } < 0 {
        return Err(ApiError::InternalIo {
            message: format!(
                "failed to clear close-on-exec for startup status fd: {}",
                io::Error::last_os_error()
            ),
        });
    }

    Ok(())
}

/// Detect count-style progress fields (`key=current/total` with numeric
/// parts) in a startup status message, so the reporter treats the line as an
/// overwritable progress update rather than a discrete status line.
fn uses_count_progress(message: &str) -> bool {
    message.split_whitespace().any(|field| {
        let Some((_, value)) = field.split_once('=') else {
            return false;
        };
        let Some((current, total)) = value.split_once('/') else {
            return false;
        };
        !current.is_empty()
            && !total.is_empty()
            && current.chars().all(|value| value.is_ascii_digit())
            && total.chars().all(|value| value.is_ascii_digit())
    })
}

/// Write one terminal active line and pad over remnants from the previous active line.
fn write_active_terminal_line(message: &str, previous_chars: usize) -> Result<usize, ApiError> {
    let message_chars = message.chars().count();
    let padding = " ".repeat(previous_chars.saturating_sub(message_chars));
    print!("\r{message}{padding}");
    std::io::stdout()
        .flush()
        .map_err(|source| ApiError::InternalIo {
            message: format!("failed to flush active terminal line stdout: {source}"),
        })?;

    Ok(message_chars)
}

/// Extract a compact startup stage label from the existing operator-facing startup text.
fn startup_message_stage(message: &str) -> &str {
    let trimmed = message.trim();
    let startup_fields = trimmed
        .strip_prefix("data-store startup ")
        .or_else(|| trimmed.strip_prefix("data-store bootstrap "))
        .unwrap_or(trimmed);
    let first_field = startup_fields
        .split_whitespace()
        .next()
        .unwrap_or("unknown");
    first_field
        .split_once('=')
        .map(|(key, _)| key)
        .unwrap_or(first_field)
}

/// Return a log-safe version of one startup line without exposing startup-scoped secrets.
fn sanitize_startup_line(line: &str) -> String {
    if line.starts_with("admin_shutdown_token=") {
        return "admin_shutdown_token=<redacted>".to_string();
    }

    line.to_string()
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
async fn wait_for_shutdown_signal(signal: Arc<ShutdownSignal>) {
    let (shutdown_ready, shutdown_waiting) = oneshot::channel();
    info!(
        event = "shutdown.wait_started",
        stage = "shutdown_signal_waiting",
        "waiting for shutdown signal"
    );
    // The blocking domain signal stays on a standard thread; this async future
    // exists only to bridge that signal into Axum graceful shutdown.
    std::thread::spawn(move || {
        let bridge_started = Instant::now();
        info!(
            event = "shutdown.signal_bridge_thread_started",
            stage = "shutdown_signal_waiting",
            task = "shutdown_signal_bridge",
            "shutdown signal bridge thread started"
        );
        signal.wait();
        match shutdown_ready.send(()) {
            Ok(()) => {
                info!(
                    event = "shutdown.signal_bridge_thread_completed",
                    stage = "shutdown_signal_received",
                    task = "shutdown_signal_bridge",
                    elapsed_ms = bridge_started.elapsed().as_millis() as u64,
                    "shutdown signal bridge thread completed"
                );
            }
            Err(()) => {
                warn!(
                    event = "shutdown.signal_bridge_thread_failed",
                    stage = "shutdown_signal_forwarding",
                    task = "shutdown_signal_bridge",
                    elapsed_ms = bridge_started.elapsed().as_millis() as u64,
                    "shutdown signal bridge receiver closed before signal forwarding"
                );
            }
        }
    });

    match shutdown_waiting.await {
        Ok(()) => {
            info!(
                event = "shutdown.signal_received",
                stage = "shutdown_signal_received",
                "shutdown signal received"
            );
        }
        Err(source) => {
            warn!(
                event = "shutdown.signal_wait_failed",
                stage = "shutdown_signal_waiting",
                error = %source,
                "shutdown signal bridge closed before signal"
            );
        }
    }
}

/// Encode bytes as lowercase hexadecimal without adding another dependency.
fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for value in bytes {
        output.push_str(&format!("{value:02x}"));
    }

    output
}
