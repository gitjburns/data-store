use std::{
    env, fs,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::{StatusCode, blocking::Client};
use rustyline::{DefaultEditor, error::ReadlineError};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

const PROMPT: &str = "data-store> ";
const DEFAULT_CONFIG_PATH: &str = "config.toml";
const HISTORY_FILE_NAME: &str = ".data-store.history";
const HEALTH_PATH: &str = "/v1/health";

// Poll cadence for `GET /operations/{operationId}` (the §34.6 async-admin poll
// loop). This is an INTERNAL client behavior, not an operator tuning knob, so it
// is a code constant with a stated rationale (config records external facts
// only; an internal poll cadence is not an operator tuning knob). One second
// balances responsiveness against not hammering the operations store while an
// admin operation runs; the reqwest client's `[client].operation_timeout_seconds`
// (below) bounds each individual poll/transport request only. It does NOT bound
// total wait time: the poll loop runs unbounded on this interval until the
// Operation reaches a terminal status.
const OPERATION_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Deserialize)]
struct ClientConfig {
    server: ServerConfig,
    admin: AdminConfig,
    #[serde(default)]
    client: ClientRuntimeConfig,
}

#[derive(Debug, Deserialize)]
struct ServerConfig {
    bind_address: SocketAddr,
}

#[derive(Debug, Deserialize)]
struct AdminConfig {
    token_file_path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct ClientRuntimeConfig {
    #[serde(default = "default_operation_timeout_seconds")]
    operation_timeout_seconds: u64,
}

#[derive(Debug)]
struct ClientContext {
    base_url: String,
    token_file_path: PathBuf,
    http: Client,
}

// The typed command surface targets the finalized §34 REST admin routes. Every
// mutating admin verb resolves to an async Operation (202 + poll); the reads and
// `query` are direct request/response. `Exit`/`Help` are local REPL controls.
#[derive(Debug)]
enum Command {
    Health,
    Query {
        request: String,
    },
    IngestSource {
        source_system: String,
        native_uri: String,
    },
    ReparseSource {
        source_id: String,
        source_system: String,
        native_uri: String,
    },
    ActivateParse {
        source_id: String,
        parse_id: String,
    },
    AcceptParse {
        parse_id: String,
    },
    DiscardParse {
        parse_id: String,
    },
    Snapshot {
        request: String,
    },
    Restore {
        source_id: String,
        parse_id: String,
    },
    Shutdown,
    HeldParses,
    Operation {
        operation_id: String,
    },
    Unit {
        unit_id: String,
    },
    UnitRelationships {
        unit_id: String,
        direction: Option<String>,
        relationship_type: Option<String>,
    },
    Source {
        source_id: String,
    },
    SyncStatus,
    Help,
    Exit,
}

/// Construct a typed command from the registry-owned parsed argument vector. The
/// closure returns an error for argument shapes the registry entry cannot build,
/// so per-command arity/optionality rules live beside the command in the table.
type CommandBuilder = fn(&[String]) -> Result<Command>;

struct CommandSpec {
    /// Canonical token used at the interactive prompt.
    repl_name: &'static str,
    /// Additional REPL spellings that parse as this command but do not appear in help.
    repl_aliases: &'static [&'static str],
    /// Canonical process flag for one-shot invocation; absent for REPL-only commands.
    cli_flag: Option<&'static str>,
    /// Additional process flags that parse as this command but do not appear in help.
    cli_aliases: &'static [&'static str],
    /// Help line displayed by the interactive `help` command.
    repl_usage: &'static str,
    /// Help line displayed by executable-level `--help`; absent for REPL-only commands.
    cli_usage: Option<&'static str>,
    /// Registry-owned builder turning the positional argument vector into a typed command.
    build: CommandBuilder,
}

// Keep command metadata in one table so REPL parsing, CLI parsing, and help
// rendering cannot drift apart. Each entry pairs the command's names/help with a
// single builder that owns its argument shape; both front doors (REPL and CLI)
// collect the command's trailing positional arguments as a flat vector and hand
// it to `build`, so arity and optional-flag rules stay next to the command.
const COMMAND_SPECS: &[CommandSpec] = &[
    CommandSpec {
        repl_name: "health",
        repl_aliases: &[],
        cli_flag: Some("--health"),
        cli_aliases: &[],
        repl_usage: "health",
        cli_usage: Some("data-store [--config <path>] --health"),
        build: build_health_command,
    },
    CommandSpec {
        repl_name: "query",
        repl_aliases: &[],
        cli_flag: Some("--query"),
        cli_aliases: &[],
        repl_usage: "query <requestJson>",
        cli_usage: Some("data-store [--config <path>] --query <requestJson>"),
        build: build_query_command,
    },
    CommandSpec {
        repl_name: "ingest",
        repl_aliases: &[],
        cli_flag: Some("--ingest"),
        cli_aliases: &[],
        repl_usage: "ingest <sourceSystem> <nativeUri>",
        cli_usage: Some("data-store [--config <path>] --ingest <sourceSystem> <nativeUri>"),
        build: build_ingest_command,
    },
    CommandSpec {
        repl_name: "reparse",
        repl_aliases: &[],
        cli_flag: Some("--reparse"),
        cli_aliases: &[],
        repl_usage: "reparse <sourceId> <sourceSystem> <nativeUri>",
        cli_usage: Some(
            "data-store [--config <path>] --reparse <sourceId> <sourceSystem> <nativeUri>",
        ),
        build: build_reparse_command,
    },
    CommandSpec {
        repl_name: "activate",
        repl_aliases: &[],
        cli_flag: Some("--activate"),
        cli_aliases: &[],
        repl_usage: "activate <sourceId> <parseId>",
        cli_usage: Some("data-store [--config <path>] --activate <sourceId> <parseId>"),
        build: build_activate_command,
    },
    CommandSpec {
        repl_name: "accept",
        repl_aliases: &[],
        cli_flag: Some("--accept"),
        cli_aliases: &[],
        repl_usage: "accept <parseId>",
        cli_usage: Some("data-store [--config <path>] --accept <parseId>"),
        build: build_accept_command,
    },
    CommandSpec {
        repl_name: "discard",
        repl_aliases: &[],
        cli_flag: Some("--discard"),
        cli_aliases: &[],
        repl_usage: "discard <parseId>",
        cli_usage: Some("data-store [--config <path>] --discard <parseId>"),
        build: build_discard_command,
    },
    CommandSpec {
        repl_name: "snapshot",
        repl_aliases: &[],
        cli_flag: Some("--snapshot"),
        cli_aliases: &[],
        repl_usage: "snapshot [requestJson]",
        cli_usage: Some("data-store [--config <path>] --snapshot [requestJson]"),
        build: build_snapshot_command,
    },
    CommandSpec {
        repl_name: "restore",
        repl_aliases: &[],
        cli_flag: Some("--restore"),
        cli_aliases: &[],
        repl_usage: "restore <sourceId> <parseId>",
        cli_usage: Some("data-store [--config <path>] --restore <sourceId> <parseId>"),
        build: build_restore_command,
    },
    CommandSpec {
        repl_name: "shutdown",
        repl_aliases: &[],
        cli_flag: Some("--shutdown"),
        cli_aliases: &[],
        repl_usage: "shutdown",
        cli_usage: Some("data-store [--config <path>] --shutdown"),
        build: build_shutdown_command,
    },
    CommandSpec {
        repl_name: "held-parses",
        repl_aliases: &["held"],
        cli_flag: Some("--held-parses"),
        cli_aliases: &[],
        repl_usage: "held-parses",
        cli_usage: Some("data-store [--config <path>] --held-parses"),
        build: build_held_parses_command,
    },
    CommandSpec {
        repl_name: "operation",
        repl_aliases: &[],
        cli_flag: Some("--operation"),
        cli_aliases: &[],
        repl_usage: "operation <operationId>",
        cli_usage: Some("data-store [--config <path>] --operation <operationId>"),
        build: build_operation_command,
    },
    CommandSpec {
        repl_name: "unit",
        repl_aliases: &[],
        cli_flag: Some("--unit"),
        cli_aliases: &[],
        repl_usage: "unit <unitId>",
        cli_usage: Some("data-store [--config <path>] --unit <unitId>"),
        build: build_unit_command,
    },
    CommandSpec {
        repl_name: "relationships",
        repl_aliases: &[],
        cli_flag: Some("--relationships"),
        cli_aliases: &[],
        repl_usage: "relationships <unitId> [direction] [relationshipType]",
        cli_usage: Some(
            "data-store [--config <path>] --relationships <unitId> [direction] [relationshipType]",
        ),
        build: build_relationships_command,
    },
    CommandSpec {
        repl_name: "source",
        repl_aliases: &[],
        cli_flag: Some("--source"),
        cli_aliases: &[],
        repl_usage: "source <sourceId>",
        cli_usage: Some("data-store [--config <path>] --source <sourceId>"),
        build: build_source_command,
    },
    CommandSpec {
        repl_name: "sync-status",
        repl_aliases: &["sync"],
        cli_flag: Some("--sync-status"),
        cli_aliases: &[],
        repl_usage: "sync-status",
        cli_usage: Some("data-store [--config <path>] --sync-status"),
        build: build_sync_status_command,
    },
    CommandSpec {
        repl_name: "help",
        repl_aliases: &[],
        cli_flag: Some("--help"),
        cli_aliases: &["-h"],
        repl_usage: "help",
        cli_usage: Some("data-store --help"),
        build: build_help_command,
    },
    CommandSpec {
        repl_name: "exit",
        repl_aliases: &["quit"],
        cli_flag: None,
        cli_aliases: &[],
        repl_usage: "exit",
        cli_usage: None,
        build: build_exit_command,
    },
];

#[derive(Debug)]
enum StartupSelection {
    Help,
    Client {
        config_path: PathBuf,
        command: Option<Command>,
    },
}

/// Client mirror of `GET /v1/health` (the one non-camelCase response on the
/// surface): plain field names, matching the service `HealthResponse`. FINAL —
/// `render_health` renders it directly and is not a placeholder.
#[derive(Debug, Deserialize)]
struct HealthResponse {
    service: String,
    ready: bool,
    components: Vec<HealthComponent>,
}

#[derive(Debug, Deserialize)]
struct HealthComponent {
    name: String,
    ready: bool,
    details: Vec<String>,
    // Additive typed diagnostic counters (C10b). Defaulted so a component body
    // without the field still deserializes; each count carries its own `as_of`
    // marker so a value is never shown as current without saying when it was
    // measured (accuracy over convenience).
    #[serde(default)]
    counts: Vec<HealthCount>,
}

/// One additive diagnostic counter on a `HealthComponent` (C10b). Health is the
/// one non-camelCase response, and `source_system`/`as_of` are already
/// snake_case on the wire, so plain field names match. `source_system` is absent
/// for corpus-aggregate counters; `as_of` is the measurement time surfaced with
/// the value so the number is never presented as current without its marker.
#[derive(Debug, Deserialize)]
struct HealthCount {
    label: String,
    #[serde(default)]
    source_system: Option<String>,
    value: u64,
    as_of: String,
}

/// Client mirror of the §34 `POST /sources` and `POST /sources/{id}/parses`
/// request body: the connector-scoped identity of the content to ingest/reparse.
/// camelCase to match the service's `deny_unknown_fields` DTO.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IngestRequest {
    source_system: String,
    native_uri: String,
}

/// Client mirror of the §34 `POST /restore` request body.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RestoreRequest {
    source_id: String,
    parse_id: String,
}

/// Client mirror of the §34.6 immediate acceptance body returned under HTTP 202
/// by every async admin route. The transport depends on this shape to obtain the
/// operation id it then polls, so it is a typed mirror rather than raw JSON.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OperationAcceptedBody {
    operation_id: String,
}

/// Client mirror of the §34.6 `Operation` record polled at
/// `GET /operations/{operationId}`. The transport depends on `status` (to detect
/// terminal states) and `error` (to render a failure), so this is a typed mirror.
/// Wire is camelCase; `status` wire values are snake_case (`pending`/`running`/
/// `succeeded`/`failed`). The remaining fields are surfaced verbatim so nothing
/// the operator may need (target, timestamps) is dropped.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OperationRecord {
    id: String,
    operation_type: String,
    status: OperationStatus,
    target_object_type: String,
    target_object_id: String,
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    completed_at: Option<String>,
    #[serde(default)]
    error: Option<String>,
    created_at: String,
}

/// Client mirror of the §34.6 operation lifecycle status. `Succeeded`/`Failed`
/// are the terminal states the poll loop stops on. Wire form is snake_case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OperationStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
}

impl OperationStatus {
    /// Report whether this status is terminal, so the poll loop knows to stop.
    /// The terminal-state set (`succeeded`, `failed`) is the poll loop's sole
    /// exit contract; `pending`/`running` keep it polling.
    fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }
}

// --- Read/query response mirrors -------------------------------------------
//
// Every response mirror below is `Deserialize` WITHOUT `deny_unknown_fields`: a
// client must tolerate additive server fields (a new response field must never
// break decoding). Rich nested shapes the operator rarely needs field-by-field
// (`body`, the full conformance report, the assembly trace, per-hit pools) are
// kept as `serde_json::Value` so they survive losslessly and the renderer can
// print them in full rather than silently narrowing them (rendering honesty).

/// Client mirror of the §34.1 `POST /query` response: the §26 EvidencePack plus
/// optional debug diagnostics (present only when the request set `debug`).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryResponse {
    evidence_pack: EvidencePackView,
    #[serde(default)]
    diagnostics: Option<QueryDiagnostics>,
}

/// Client mirror of the operator-salient §26 EvidencePack fields. The rich
/// `relationships`/`annotations`/`assemblyTrace` shapes are kept as raw JSON so
/// nothing is dropped; the renderer summarizes the units and keeps the full
/// trace reachable.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EvidencePackView {
    query_id: String,
    query_text: String,
    evidence_units: Vec<EvidenceUnitView>,
    #[serde(default)]
    relationships: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    annotations: Option<Vec<serde_json::Value>>,
    assembly_trace: serde_json::Value,
    created_at: String,
}

/// Client mirror of one §26 EvidenceUnit. `body` is the arbitrary §18 payload,
/// kept as raw JSON; `textProjection` is the ranking/answering plain text the
/// renderer excerpts.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EvidenceUnitView {
    unit_id: String,
    source_id: String,
    parse_id: String,
    content_type: String,
    body: serde_json::Value,
    #[serde(default)]
    text_projection: Option<String>,
    #[serde(default)]
    locators: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    score: Option<f64>,
    #[serde(default)]
    reasons: Option<Vec<String>>,
}

/// Client mirror of the §24.3 debug diagnostics block. The fused candidate pool
/// is kept as raw JSON (a rich §24.4 hit shape the renderer counts and does not
/// narrow); the ranked views and latencies are typed for a readable summary.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryDiagnostics {
    fused_pool: Vec<serde_json::Value>,
    maxsim: Vec<RankedScore>,
    reranked: Vec<RerankedScore>,
    latencies: StageLatencies,
}

/// Client mirror of one ranked MaxSim score in the debug diagnostics.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RankedScore {
    unit_id: String,
    score: f64,
    rank: u64,
}

/// Client mirror of one final-reranker score; `logit`/`tokenCount` are present
/// only when the backend supplied them.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RerankedScore {
    unit_id: String,
    score: f64,
    rank: u64,
    #[serde(default)]
    logit: Option<f64>,
    #[serde(default)]
    token_count: Option<u64>,
}

/// Client mirror of the §24 per-stage wall-clock latencies (milliseconds).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StageLatencies {
    open_transaction_ms: u64,
    capture_ms: u64,
    query_embed_ms: u64,
    dense_lexical_fusion_ms: u64,
    graph_ms: u64,
    maxsim_ms: u64,
    rerank_ms: u64,
    assembly_ms: u64,
    snapshot_held_ms: u64,
}

/// Client mirror of the §13.4 `GET /parses?status=held` envelope.
#[derive(Debug, Deserialize)]
struct HeldParsesResponse {
    parses: Vec<ParseRunView>,
}

/// Client mirror of the operator-salient §12 ParseRun fields for the held
/// listing. The nested conformance report, warnings, and metrics are kept as raw
/// JSON so the renderer can surface the report's per-dimension regression detail
/// and keep the rest reachable without mirroring every §12.5 field.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ParseRunView {
    id: String,
    source_id: String,
    parser_name: String,
    parser_version: String,
    status: String,
    #[serde(default)]
    held_reason: Option<String>,
    #[serde(default)]
    conformance_report: Option<serde_json::Value>,
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    completed_at: Option<String>,
    #[serde(default)]
    warnings: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    metrics: Option<serde_json::Value>,
    created_at: String,
    #[serde(default)]
    error: Option<String>,
}

/// Client mirror of the §15 ContentUnit (`GET /units/{unitId}`). `body` is the
/// arbitrary §18 payload, kept as raw JSON and rendered losslessly.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContentUnitView {
    id: String,
    source_id: String,
    parse_id: String,
    content_type: String,
    body_hash: String,
    #[serde(default)]
    text_hash: Option<String>,
    #[serde(default)]
    structure_hash: Option<String>,
    #[serde(default)]
    primary_parent_id: Option<String>,
    #[serde(default)]
    sequence_index: Option<u64>,
    #[serde(default)]
    locators: Option<Vec<serde_json::Value>>,
    body: serde_json::Value,
    created_at: String,
    #[serde(default)]
    deleted_at: Option<String>,
}

/// Client mirror of the §19 relationships envelope
/// (`GET /units/{unitId}/relationships`).
#[derive(Debug, Deserialize)]
struct RelationshipsResponse {
    relationships: Vec<RelationshipView>,
}

/// Client mirror of one §19 UnitRelationship edge.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RelationshipView {
    id: String,
    source_id: String,
    parse_id: String,
    from_unit_id: String,
    to_unit_id: String,
    relationship_type: String,
    #[serde(default)]
    relationship_role: Option<String>,
    #[serde(default)]
    sequence_index: Option<u64>,
    #[serde(default)]
    confidence: Option<f64>,
    #[serde(default)]
    provenance: Option<serde_json::Value>,
    created_at: String,
    #[serde(default)]
    deleted_at: Option<String>,
}

/// Client mirror of the §10 SourceObject (`GET /sources/{sourceId}`). Locations
/// carry the freshness (`lastSeenAt`) and `status` the renderer surfaces
/// prominently.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SourceView {
    id: String,
    #[serde(default)]
    active_parse_id: Option<String>,
    mime_type: String,
    #[serde(default)]
    size_bytes: Option<u64>,
    source_hash: String,
    storage_uri: String,
    locations: Vec<SourceLocationView>,
    #[serde(default)]
    event_time: Option<String>,
    ingest_time: String,
    created_at: String,
    #[serde(default)]
    deactivated_at: Option<String>,
}

/// Client mirror of one §10 SourceLocation. `deletionEvidence`/`metadata` are
/// kept as raw JSON so their detail survives without mirroring every §11.1 field.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SourceLocationView {
    id: String,
    source_system: String,
    native_uri: String,
    #[serde(default)]
    native_id: Option<String>,
    governance_domain: String,
    first_seen_at: String,
    last_seen_at: String,
    status: String,
    #[serde(default)]
    deletion_evidence: Option<serde_json::Value>,
    #[serde(default)]
    metadata: Option<serde_json::Value>,
}

/// Client mirror of the §9.5–§9.6 sync scheduler health (`GET /sync/status`).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyncStatusView {
    fabric_ready: bool,
    #[serde(default)]
    detail: Option<String>,
    pending: u64,
    in_flight: u64,
    failed: u64,
    coalesced_total: u64,
    #[serde(default)]
    cadence_ms: Option<u64>,
    #[serde(default)]
    last_success_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    error: ErrorDetail,
}

/// Tolerant mirror of the service `{status, kind, message}` error detail. The
/// service always populates all three, but `status`/`kind` stay optional here so
/// a legacy or minimal error body still renders its message rather than failing
/// to deserialize (never replace a specific server error with a generic one).
#[derive(Debug, Deserialize)]
struct ErrorDetail {
    status: Option<u16>,
    kind: Option<String>,
    message: String,
}

/// Start either one command-line operation or the interactive client after resolving config.
fn main() -> Result<()> {
    let StartupSelection::Client {
        config_path,
        command,
    } = parse_startup_args()?
    else {
        render_cli_help();
        return Ok(());
    };
    let config = load_config(&config_path)?;
    let config_dir = config_parent_dir(&config_path)?;
    let context = ClientContext {
        base_url: base_url_for_bind_address(config.server.bind_address),
        token_file_path: resolve_config_relative_path(&config_dir, &config.admin.token_file_path),
        http: build_http_client(&config.client)?,
    };
    match command {
        Some(command) => {
            execute_command(&context, command)?;
            Ok(())
        }
        None => run_repl(
            context,
            resolve_config_relative_path(&config_dir, Path::new(HISTORY_FILE_NAME)),
        ),
    }
}

/// Parse process arguments into local help, one-shot command, or interactive mode.
fn parse_startup_args() -> Result<StartupSelection> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    parse_startup_arguments(&args)
}

/// Parse the supported non-interactive command surface without adding a new dependency.
fn parse_startup_arguments(args: &[String]) -> Result<StartupSelection> {
    let mut config_path = PathBuf::from(DEFAULT_CONFIG_PATH);
    let mut command = None;
    let mut help_requested = false;
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--config" {
            let value = take_startup_value(args, &mut index, "--config", "path")?;
            config_path = PathBuf::from(value);
            continue;
        }
        let Some(spec) = find_cli_command_spec(arg) else {
            bail!("unknown argument `{arg}`; run `data-store --help` for usage");
        };
        let next_command = parse_cli_command(spec, args, &mut index)?;
        if matches!(next_command, Command::Help) {
            help_requested = true;
        } else {
            set_startup_command(&mut command, next_command, arg)?;
        }
    }
    if help_requested {
        if command.is_some() {
            bail!("--help cannot be combined with operation flags");
        }
        return Ok(StartupSelection::Help);
    }

    Ok(StartupSelection::Client {
        config_path,
        command,
    })
}

/// Store the selected one-shot command while rejecting ambiguous multi-command invocations.
fn set_startup_command(
    command: &mut Option<Command>,
    next_command: Command,
    flag: &str,
) -> Result<()> {
    if command.is_some() {
        bail!("only one operation flag may be provided; got `{flag}` after another operation");
    }
    *command = Some(next_command);
    Ok(())
}

/// Take the value for a startup option that is not part of the command registry.
fn take_startup_value(
    args: &[String],
    index: &mut usize,
    flag: &str,
    value_name: &str,
) -> Result<String> {
    let value_index = *index + 1;
    let Some(value) = args.get(value_index) else {
        bail!("{flag} requires a {value_name}");
    };
    if is_startup_flag(value) {
        bail!("{flag} requires a {value_name}");
    }
    *index += 2;
    Ok(value.clone())
}

/// Locate a REPL command by its primary name or a documented alias such as `quit`.
fn find_repl_command_spec(value: &str) -> Option<&'static CommandSpec> {
    COMMAND_SPECS
        .iter()
        .find(|spec| spec.repl_name == value || spec.repl_aliases.contains(&value))
}

/// Locate a command-line operation flag by its primary flag or alias such as `-h`.
fn find_cli_command_spec(value: &str) -> Option<&'static CommandSpec> {
    COMMAND_SPECS
        .iter()
        .find(|spec| spec.cli_flag == Some(value) || spec.cli_aliases.contains(&value))
}

/// Identify startup flags so positional command parsing can stop before the next option.
fn is_startup_flag(value: &str) -> bool {
    value == "--config" || find_cli_command_spec(value).is_some()
}

/// Parse REPL arguments by handing the command's trailing tokens to its builder.
fn parse_repl_command(spec: &CommandSpec, args: &[String]) -> Result<Command> {
    (spec.build)(args)
}

/// Parse one CLI operation flag: collect its trailing non-flag tokens, advance the
/// argv cursor past them, and hand the collected vector to the command's builder.
fn parse_cli_command(spec: &CommandSpec, args: &[String], index: &mut usize) -> Result<Command> {
    *index += 1;
    let mut values = Vec::new();
    while let Some(value) = args.get(*index) {
        if is_startup_flag(value) {
            break;
        }
        values.push(value.clone());
        *index += 1;
    }
    (spec.build)(&values)
}

/// Build the typed health command from a no-argument registry entry.
fn build_health_command(args: &[String]) -> Result<Command> {
    require_arg_count(args, 0, "health")?;
    Ok(Command::Health)
}

/// Build the `query` command: one required raw JSON request body string.
fn build_query_command(args: &[String]) -> Result<Command> {
    require_arg_count(args, 1, "query <requestJson>")?;
    Ok(Command::Query {
        request: args[0].clone(),
    })
}

/// Build the `ingest` command targeting `POST /sources`.
fn build_ingest_command(args: &[String]) -> Result<Command> {
    require_arg_count(args, 2, "ingest <sourceSystem> <nativeUri>")?;
    Ok(Command::IngestSource {
        source_system: args[0].clone(),
        native_uri: args[1].clone(),
    })
}

/// Build the `reparse` command targeting `POST /sources/{sourceId}/parses`.
fn build_reparse_command(args: &[String]) -> Result<Command> {
    require_arg_count(args, 3, "reparse <sourceId> <sourceSystem> <nativeUri>")?;
    Ok(Command::ReparseSource {
        source_id: args[0].clone(),
        source_system: args[1].clone(),
        native_uri: args[2].clone(),
    })
}

/// Build the `activate` command targeting `POST /sources/{id}/parses/{id}/activate`.
fn build_activate_command(args: &[String]) -> Result<Command> {
    require_arg_count(args, 2, "activate <sourceId> <parseId>")?;
    Ok(Command::ActivateParse {
        source_id: args[0].clone(),
        parse_id: args[1].clone(),
    })
}

/// Build the `accept` command targeting `POST /parses/{parseId}/accept`.
fn build_accept_command(args: &[String]) -> Result<Command> {
    require_arg_count(args, 1, "accept <parseId>")?;
    Ok(Command::AcceptParse {
        parse_id: args[0].clone(),
    })
}

/// Build the `discard` command targeting `POST /parses/{parseId}/discard`.
fn build_discard_command(args: &[String]) -> Result<Command> {
    require_arg_count(args, 1, "discard <parseId>")?;
    Ok(Command::DiscardParse {
        parse_id: args[0].clone(),
    })
}

/// Build the `snapshot` command targeting `POST /snapshots`. The request body is
/// optional (all fields optional server-side); absent means an empty body.
fn build_snapshot_command(args: &[String]) -> Result<Command> {
    match args {
        [] => Ok(Command::Snapshot {
            request: "{}".to_string(),
        }),
        [request] => Ok(Command::Snapshot {
            request: request.clone(),
        }),
        _ => bail!("usage: snapshot [requestJson]"),
    }
}

/// Build the `restore` command targeting `POST /restore`.
fn build_restore_command(args: &[String]) -> Result<Command> {
    require_arg_count(args, 2, "restore <sourceId> <parseId>")?;
    Ok(Command::Restore {
        source_id: args[0].clone(),
        parse_id: args[1].clone(),
    })
}

/// Build the `shutdown` command targeting `POST /shutdown`.
fn build_shutdown_command(args: &[String]) -> Result<Command> {
    require_arg_count(args, 0, "shutdown")?;
    Ok(Command::Shutdown)
}

/// Build the `held-parses` command targeting `GET /parses?status=held`.
fn build_held_parses_command(args: &[String]) -> Result<Command> {
    require_arg_count(args, 0, "held-parses")?;
    Ok(Command::HeldParses)
}

/// Build the `operation` command targeting `GET /operations/{operationId}`.
fn build_operation_command(args: &[String]) -> Result<Command> {
    require_arg_count(args, 1, "operation <operationId>")?;
    Ok(Command::Operation {
        operation_id: args[0].clone(),
    })
}

/// Build the `unit` command targeting `GET /units/{unitId}`.
fn build_unit_command(args: &[String]) -> Result<Command> {
    require_arg_count(args, 1, "unit <unitId>")?;
    Ok(Command::Unit {
        unit_id: args[0].clone(),
    })
}

/// Build the `relationships` command targeting `GET /units/{unitId}/relationships`,
/// with the optional `direction` and `relationshipType` query filters.
fn build_relationships_command(args: &[String]) -> Result<Command> {
    match args {
        [unit_id] => Ok(Command::UnitRelationships {
            unit_id: unit_id.clone(),
            direction: None,
            relationship_type: None,
        }),
        [unit_id, direction] => Ok(Command::UnitRelationships {
            unit_id: unit_id.clone(),
            direction: Some(direction.clone()),
            relationship_type: None,
        }),
        [unit_id, direction, relationship_type] => Ok(Command::UnitRelationships {
            unit_id: unit_id.clone(),
            direction: Some(direction.clone()),
            relationship_type: Some(relationship_type.clone()),
        }),
        _ => bail!("usage: relationships <unitId> [direction] [relationshipType]"),
    }
}

/// Build the `source` command targeting `GET /sources/{sourceId}`.
fn build_source_command(args: &[String]) -> Result<Command> {
    require_arg_count(args, 1, "source <sourceId>")?;
    Ok(Command::Source {
        source_id: args[0].clone(),
    })
}

/// Build the `sync-status` command targeting `GET /sync/status`.
fn build_sync_status_command(args: &[String]) -> Result<Command> {
    require_arg_count(args, 0, "sync-status")?;
    Ok(Command::SyncStatus)
}

/// Build the local help command; CLI startup intercepts this before config loading.
fn build_help_command(args: &[String]) -> Result<Command> {
    require_arg_count(args, 0, "help")?;
    Ok(Command::Help)
}

/// Build the local exit command, which is intentionally REPL-only.
fn build_exit_command(args: &[String]) -> Result<Command> {
    require_arg_count(args, 0, "exit")?;
    Ok(Command::Exit)
}

/// Enforce exact positional arity for a command, reporting the command's usage
/// line so both front doors give the same message on a shape mismatch.
fn require_arg_count(args: &[String], expected: usize, usage: &str) -> Result<()> {
    if args.len() == expected {
        return Ok(());
    }

    bail!("usage: {usage}");
}

/// Load the subset of service config needed by the CLI client.
fn load_config(path: &Path) -> Result<ClientConfig> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read config at {}", path.display()))?;
    let config: ClientConfig = toml::from_str(&raw)
        .with_context(|| format!("failed to parse config at {}", path.display()))?;
    if config.admin.token_file_path.as_os_str().is_empty() {
        bail!("admin.token_file_path must be a non-empty path");
    }
    if config.client.operation_timeout_seconds == 0 {
        bail!("client.operation_timeout_seconds must be greater than zero");
    }

    Ok(config)
}

impl Default for ClientRuntimeConfig {
    /// Keep existing local configs usable while matching the documented one-hour operation cap.
    fn default() -> Self {
        Self {
            operation_timeout_seconds: default_operation_timeout_seconds(),
        }
    }
}

/// Return the default CLI operation timeout in seconds.
fn default_operation_timeout_seconds() -> u64 {
    3_600
}

/// Build the blocking HTTP client with the configured operation timeout. This
/// timeout bounds every request AND every individual poll of the operation loop
/// (`[client].operation_timeout_seconds`), consumed exactly as before the rework.
fn build_http_client(config: &ClientRuntimeConfig) -> Result<Client> {
    Client::builder()
        .timeout(Duration::from_secs(config.operation_timeout_seconds))
        .build()
        .context("failed to build HTTP client")
}

/// Return the canonicalized parent directory of the loaded config file: the
/// base every relative client-side path resolves against, matching the
/// service's resolution rule.
fn config_parent_dir(config_path: &Path) -> Result<PathBuf> {
    let canonical = config_path.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize config path {}",
            config_path.display()
        )
    })?;
    canonical.parent().map(Path::to_path_buf).with_context(|| {
        format!(
            "config path {} has no parent directory",
            canonical.display()
        )
    })
}

/// Resolve relative local paths against the config file's directory.
fn resolve_config_relative_path(config_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }

    config_dir.join(path)
}

/// Build the HTTP base URL while converting bind-all addresses to loopback clients can dial.
fn base_url_for_bind_address(bind_address: SocketAddr) -> String {
    let ip = match bind_address.ip() {
        IpAddr::V4(value) if value.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(value) if value.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        other => other,
    };
    format!("http://{}", SocketAddr::new(ip, bind_address.port()))
}

/// Run the readline loop until the user exits or input closes.
fn run_repl(context: ClientContext, history_path: PathBuf) -> Result<()> {
    println!("Data Store client connected to {}", context.base_url);
    println!("Type `help` for commands. Type `exit` to quit.");
    let mut editor = DefaultEditor::new().context("failed to initialize line editor")?;
    let _ = editor.load_history(&history_path);

    loop {
        match editor.readline(PROMPT) {
            Ok(line) => {
                if line.trim().is_empty() {
                    continue;
                }
                let _ = editor.add_history_entry(line.as_str());
                match parse_command_line(&line)
                    .and_then(|command| execute_command(&context, command))
                {
                    Ok(should_continue) => {
                        if !should_continue {
                            break;
                        }
                    }
                    Err(source) => print_error(&source),
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Use `exit` to quit.");
            }
            Err(ReadlineError::Eof) => break,
            Err(source) => return Err(source).context("failed to read input"),
        }
    }

    editor
        .save_history(&history_path)
        .with_context(|| format!("failed to save history at {}", history_path.display()))?;
    Ok(())
}

/// Print an error plus its cause chain without collapsing transport diagnostics.
fn print_error(error: &anyhow::Error) {
    eprintln!("error: {error}");
    for cause in error.chain().skip(1) {
        eprintln!("  caused by: {cause}");
    }
}

/// Convert one REPL line into a typed command before any HTTP request is sent.
fn parse_command_line(line: &str) -> Result<Command> {
    let args = split_shell_like(line)?;
    if args.is_empty() {
        bail!("empty command");
    }
    let command_name = &args[0];
    let Some(spec) = find_repl_command_spec(command_name) else {
        bail!("unknown command `{command_name}`; type `help` for commands");
    };
    parse_repl_command(spec, &args[1..])
}

/// Split one REPL line with minimal shell-like quotes but no shell behavior.
fn split_shell_like(line: &str) -> Result<Vec<String>> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut in_arg = false;
    let mut escaped = false;
    for value in line.chars() {
        if escaped {
            match value {
                '"' | '\\' => current.push(value),
                other => {
                    current.push('\\');
                    current.push(other);
                }
            }
            escaped = false;
            in_arg = true;
            continue;
        }
        if in_quotes && value == '\\' {
            escaped = true;
            continue;
        }
        if value == '"' {
            in_quotes = !in_quotes;
            in_arg = true;
            continue;
        }
        if !in_quotes && value.is_whitespace() {
            if in_arg {
                args.push(current);
                current = String::new();
                in_arg = false;
            }
            continue;
        }
        current.push(value);
        in_arg = true;
    }
    if escaped {
        current.push('\\');
    }
    if in_quotes {
        bail!("unterminated quoted string");
    }
    if in_arg {
        args.push(current);
    }

    Ok(args)
}

/// Execute one parsed command against the §34 REST surface and return whether the
/// REPL should continue. Each admin mutation goes through `run_admin_operation`
/// (202 + poll); each read/query goes through a GET/POST helper; renderers take
/// the decoded value so stage 2 can swap them without touching this dispatch.
fn execute_command(context: &ClientContext, command: Command) -> Result<bool> {
    match command {
        Command::Health => {
            let health: HealthResponse = get_public(context, HEALTH_PATH)?;
            render_health(health);
        }
        Command::Query { request } => {
            let body = parse_json_request("query", &request)?;
            let value = post_public_json(context, "/query", &body)?;
            // Decode the raw transport value into the typed mirror at the dispatch
            // seam so the transport layer stays byte-unchanged; a decode failure
            // still surfaces the full payload rather than dropping it.
            let response: QueryResponse = decode_value("/query", value)?;
            render_query(&response);
        }
        Command::IngestSource {
            source_system,
            native_uri,
        } => {
            let request = IngestRequest {
                source_system,
                native_uri,
            };
            run_admin_operation(context, "POST", "/sources", Some(&to_body(&request)?))?;
        }
        Command::ReparseSource {
            source_id,
            source_system,
            native_uri,
        } => {
            let request = IngestRequest {
                source_system,
                native_uri,
            };
            let path = format!("/sources/{}/parses", source_id);
            run_admin_operation(context, "POST", &path, Some(&to_body(&request)?))?;
        }
        Command::ActivateParse {
            source_id,
            parse_id,
        } => {
            let path = format!("/sources/{}/parses/{}/activate", source_id, parse_id);
            run_admin_operation(context, "POST", &path, None)?;
        }
        Command::AcceptParse { parse_id } => {
            let path = format!("/parses/{}/accept", parse_id);
            run_admin_operation(context, "POST", &path, None)?;
        }
        Command::DiscardParse { parse_id } => {
            let path = format!("/parses/{}/discard", parse_id);
            run_admin_operation(context, "POST", &path, None)?;
        }
        Command::Snapshot { request } => {
            let body = parse_json_request("snapshot", &request)?;
            run_admin_operation(context, "POST", "/snapshots", Some(&body))?;
        }
        Command::Restore {
            source_id,
            parse_id,
        } => {
            let request = RestoreRequest {
                source_id,
                parse_id,
            };
            run_admin_operation(context, "POST", "/restore", Some(&to_body(&request)?))?;
        }
        Command::Shutdown => {
            // §34: shutdown is NOT an Operation — 202 with no body, an immediate
            // signal. It must not be polled, so it uses the dedicated no-body path.
            send_shutdown(context)?;
        }
        Command::HeldParses => {
            let value = get_protected(context, "/parses?status=held")?;
            let response: HeldParsesResponse = decode_value("/parses?status=held", value)?;
            render_held_parses(&response);
        }
        Command::Operation { operation_id } => {
            let record = poll_operation_once(context, &operation_id)?;
            render_operation(&record);
        }
        Command::Unit { unit_id } => {
            let path = format!("/units/{}", unit_id);
            // §14: a 404 here means the unit is absent OR belongs to a
            // non-active parse — the server makes the two indistinguishable by
            // design, so annotate a not-found failure with that meaning.
            let unit: ContentUnitView = get_public(context, &path).map_err(annotate_unit_404)?;
            render_unit(&unit);
        }
        Command::UnitRelationships {
            unit_id,
            direction,
            relationship_type,
        } => {
            let path =
                relationships_path(&unit_id, direction.as_deref(), relationship_type.as_deref());
            // Same §14 404 semantics as the unit read (absent vs non-active parse).
            let response: RelationshipsResponse =
                get_public(context, &path).map_err(annotate_unit_404)?;
            render_relationships(&response);
        }
        Command::Source { source_id } => {
            let path = format!("/sources/{}", source_id);
            let source: SourceView = get_public(context, &path)?;
            render_source(&source);
        }
        Command::SyncStatus => {
            let status: SyncStatusView = get_public(context, "/sync/status")?;
            render_sync_status(&status);
        }
        Command::Help => render_help(),
        Command::Exit => return Ok(false),
    }

    Ok(true)
}

/// Build the `GET /units/{unitId}/relationships` path with its optional query
/// filters, encoding only the filters the operator supplied.
fn relationships_path(
    unit_id: &str,
    direction: Option<&str>,
    relationship_type: Option<&str>,
) -> String {
    let mut path = format!("/units/{}/relationships", unit_id);
    let mut params = Vec::new();
    if let Some(direction) = direction {
        params.push(format!("direction={}", direction));
    }
    if let Some(relationship_type) = relationship_type {
        params.push(format!("relationshipType={}", relationship_type));
    }
    if !params.is_empty() {
        path.push('?');
        path.push_str(&params.join("&"));
    }
    path
}

/// Read the current startup token immediately before an admin request uses it.
/// This per-request read is the auth-freshness contract: the token file is read,
/// trimmed, and validated non-empty on every protected call, never cached.
fn read_admin_token(context: &ClientContext) -> Result<String> {
    let token = fs::read_to_string(&context.token_file_path).with_context(|| {
        format!(
            "failed to read admin token file at {}",
            context.token_file_path.display()
        )
    })?;
    let token = token.trim().to_string();
    if token.is_empty() {
        bail!(
            "admin token file at {} is empty",
            context.token_file_path.display()
        );
    }

    Ok(token)
}

/// Parse a raw operator-supplied JSON request string into a value, surfacing a
/// parse failure with the command name rather than sending malformed JSON.
fn parse_json_request(command: &str, raw: &str) -> Result<serde_json::Value> {
    serde_json::from_str(raw)
        .with_context(|| format!("{command} request must be valid JSON, got `{raw}`"))
}

/// Encode a typed request DTO into a JSON value for the request body.
fn to_body<T: Serialize>(request: &T) -> Result<serde_json::Value> {
    serde_json::to_value(request).context("failed to encode request body")
}

/// Decode a raw transport JSON value into a typed response mirror at the dispatch
/// seam. Used for the protected/POST-public helpers, which return raw JSON; the
/// public GET helper is already generic and decodes directly. On failure the
/// error carries the route and the full payload so a shape drift is diagnosable
/// and no field is silently swallowed (rendering honesty).
fn decode_value<T: DeserializeOwned>(route: &str, value: serde_json::Value) -> Result<T> {
    serde_json::from_value(value.clone()).with_context(|| {
        format!(
            "{route} returned an unexpected response body: {}",
            serde_json::to_string(&value).unwrap_or_else(|_| value.to_string())
        )
    })
}

/// Annotate a not-found error from a `unit`/`relationships` read with the §14
/// meaning: the server returns 404 for BOTH an absent unit and a unit that
/// belongs to a non-active parse, and makes the two indistinguishable by design.
/// Non-404 errors pass through unchanged so their specific detail is preserved.
fn annotate_unit_404(error: anyhow::Error) -> anyhow::Error {
    if error.to_string().contains("HTTP 404") {
        return error.context(
            "§14: a 404 means the unit is absent OR belongs to a non-active parse — the \
             service makes these two cases indistinguishable by design",
        );
    }
    error
}

/// Build a service URL from the configured base URL and a protocol path.
fn url(context: &ClientContext, path: &str) -> String {
    format!("{}{}", context.base_url, path)
}

// --- Transport seam --------------------------------------------------------
//
// Three request primitives form the transport layer stage 2 must not touch:
//   * `get_public` / `post_public_json` / `get_protected` — one request, one
//     decoded value (protected variants read the admin token fresh per call).
//   * `run_admin_operation` — POST an async admin mutation, decode the 202
//     acceptance body, then `poll_operation` until terminal.
//   * `send_shutdown` — the one 202-with-no-body control action (never polled).
// Renderers consume the decoded values these return, so renderers are swappable
// without touching transport.

/// Perform a public GET and deserialize its success body into `T`. No bearer.
fn get_public<T: DeserializeOwned>(context: &ClientContext, path: &str) -> Result<T> {
    let target_url = url(context, path);
    let response = context
        .http
        .get(&target_url)
        .send()
        .with_context(|| format!("GET {target_url} failed to send HTTP request"))?;
    decode_success(response, "GET", &target_url)
}

/// Perform a protected GET and return its success body as raw JSON. Reads the
/// admin token immediately before sending (auth freshness).
fn get_protected(context: &ClientContext, path: &str) -> Result<serde_json::Value> {
    let target_url = url(context, path);
    let response = context
        .http
        .get(&target_url)
        .bearer_auth(read_admin_token(context)?)
        .send()
        .with_context(|| format!("GET {target_url} failed to send HTTP request"))?;
    decode_success(response, "GET", &target_url)
}

/// Perform a public POST with a JSON body and return its success body as raw JSON.
fn post_public_json(
    context: &ClientContext,
    path: &str,
    body: &serde_json::Value,
) -> Result<serde_json::Value> {
    let target_url = url(context, path);
    let response = context
        .http
        .post(&target_url)
        .json(body)
        .send()
        .with_context(|| format!("POST {target_url} failed to send HTTP request"))?;
    decode_success(response, "POST", &target_url)
}

/// Drive one async admin mutation to a terminal Operation state: POST the request
/// (bearer, optional JSON body), decode the 202 `{operationId}` acceptance body,
/// then poll `GET /operations/{operationId}` until terminal and render the result.
/// The POST and each poll read the admin token fresh (auth freshness).
fn run_admin_operation(
    context: &ClientContext,
    method: &str,
    path: &str,
    body: Option<&serde_json::Value>,
) -> Result<()> {
    let target_url = url(context, path);
    println!("{method} {target_url}");
    let mut builder = context
        .http
        .post(&target_url)
        .bearer_auth(read_admin_token(context)?);
    if let Some(body) = body {
        builder = builder.json(body);
    }
    let response = builder
        .send()
        .with_context(|| format!("{method} {target_url} failed to send HTTP request"))?;
    let accepted: OperationAcceptedBody = decode_success(response, method, &target_url)?;
    println!("Accepted: operationId={}", accepted.operation_id);
    let record = poll_operation(context, &accepted.operation_id)?;
    render_operation(&record);
    Ok(())
}

/// Poll `GET /operations/{operationId}` on the code-constant interval until the
/// Operation reaches a terminal state (`succeeded`/`failed`), then return the
/// terminal record. Each poll reads the admin token fresh (`/operations` is
/// protected). The reqwest client's `[client].operation_timeout_seconds` bounds
/// each poll request; the loop otherwise runs until a terminal status.
fn poll_operation(context: &ClientContext, operation_id: &str) -> Result<OperationRecord> {
    loop {
        let record = poll_operation_once(context, operation_id)?;
        if record.status.is_terminal() {
            return Ok(record);
        }
        thread::sleep(OPERATION_POLL_INTERVAL);
    }
}

/// Read one Operation record without waiting for a terminal state. Used both by
/// the poll loop and the standalone `operation` command (a single snapshot read).
fn poll_operation_once(context: &ClientContext, operation_id: &str) -> Result<OperationRecord> {
    let path = format!("/operations/{}", operation_id);
    let target_url = url(context, &path);
    let response = context
        .http
        .get(&target_url)
        .bearer_auth(read_admin_token(context)?)
        .send()
        .with_context(|| format!("GET {target_url} failed to send HTTP request"))?;
    decode_success(response, "GET", &target_url)
}

/// Signal shutdown via `POST /shutdown` (§34): protected, 202 with NO body, not
/// an Operation — so this reads only the status and never attempts to poll.
fn send_shutdown(context: &ClientContext) -> Result<()> {
    let target_url = url(context, "/shutdown");
    println!("POST {target_url}");
    let response = context
        .http
        .post(&target_url)
        .bearer_auth(read_admin_token(context)?)
        .send()
        .with_context(|| format!("POST {target_url} failed to send HTTP request"))?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().with_context(|| {
            format!("POST {target_url} failed to read HTTP error response body")
        })?;
        return Err(service_error("POST", &target_url, status, &text));
    }
    println!("Shutdown signalled (HTTP {})", status.as_u16());
    Ok(())
}

/// Decode a response body into `T` on success, or convert a non-2xx response into
/// an operator-facing error that preserves the server/protocol error detail.
fn decode_success<T: DeserializeOwned>(
    response: reqwest::blocking::Response,
    method: &str,
    target_url: &str,
) -> Result<T> {
    let status = response.status();
    if !status.is_success() {
        let text = response.text().with_context(|| {
            format!("{method} {target_url} failed to read HTTP error response body")
        })?;
        return Err(service_error(method, target_url, status, &text));
    }
    let text = response
        .text()
        .with_context(|| format!("{method} {target_url} failed to read HTTP response body"))?;
    serde_json::from_str(&text)
        .with_context(|| format!("{method} {target_url} returned an unexpected response body"))
}

/// Convert a non-success HTTP response into an operator-facing error, preserving
/// the structured `{error:{status,kind,message}}` detail when present and never
/// replacing a specific server message with a generic one.
fn service_error(method: &str, target_url: &str, status: StatusCode, text: &str) -> anyhow::Error {
    if let Ok(body) = serde_json::from_str::<ErrorBody>(text) {
        return anyhow!(
            "{} {} HTTP {}: {}",
            method,
            target_url,
            status.as_u16(),
            format_error_detail(&body.error)
        );
    }
    if text.trim().is_empty() {
        return anyhow!(
            "{} {} HTTP {} with empty response body",
            method,
            target_url,
            status.as_u16()
        );
    }

    anyhow!(
        "{} {} HTTP {}: {}",
        method,
        target_url,
        status.as_u16(),
        text.trim()
    )
}

/// Format structured service errors while tolerating minimal message-only bodies.
fn format_error_detail(error: &ErrorDetail) -> String {
    match (&error.status, &error.kind) {
        (Some(status), Some(kind)) => {
            format!("status={status} kind={kind} message={}", error.message)
        }
        (Some(status), None) => format!("status={status} message={}", error.message),
        (None, Some(kind)) => format!("kind={kind} message={}", error.message),
        (None, None) => error.message.clone(),
    }
}

// --- Renderers -------------------------------------------------------------
//
// `render_health` and `render_operation` are FINAL for the transport's terminal
// reporting — the latter carries the mandatory Operation-succeeded-vs-parse-
// outcome rule. The remaining renderers are typed per-route DTO renderers: each
// summarizes the operator-salient fields and keeps the rich/nested payload
// reachable in full (via `print_labeled_json`) so no field is silently dropped
// (rendering honesty). `excerpt_text` bounds long text projections; the full
// content stays visible because the unit `body` is always rendered alongside.

/// Print service readiness and component diagnostics in a compact form. FINAL —
/// `GET /v1/health` has a stable typed mirror.
fn render_health(response: HealthResponse) {
    println!("Service: {}", response.service);
    println!("Ready: {}", yes_no(response.ready));
    if response.components.is_empty() {
        println!("Components: none");
        return;
    }
    println!("Components:");
    for component in response.components {
        println!("  {}: {}", component.name, yes_no(component.ready));
        for detail in component.details {
            println!("    - {detail}");
        }
        // Additive typed counters (C10b). Each value is shown with its as-of
        // marker so a count is never presented as current without saying when it
        // was measured; `source_system` scopes fabric counts to their owner.
        for count in component.counts {
            let scope = count
                .source_system
                .as_deref()
                .map(|system| format!(" [{system}]"))
                .unwrap_or_default();
            println!(
                "    count {}{}: {} (as of {})",
                count.label, scope, count.value, count.as_of
            );
        }
    }
}

/// Render a terminal (or snapshot) Operation record.
///
/// OPERATION-SUCCEEDED vs PARSE-OUTCOME rule (§34.6, mandatory): an Operation
/// reaching `succeeded` means the pipeline LIFECYCLE completed — NOT that the
/// domain outcome was favorable. For a parse-producing operation the domain
/// verdict (a recorded parse failure, or a held disposition awaiting operator
/// action) lives in the parse run row, not in `Operation.status`. So for those
/// operation types a bare "succeeded" is misleading: this renderer surfaces the
/// lifecycle status AND directs the operator to the domain verdict (the held
/// listing via `held-parses`, or the parse run itself). Stage 2 builds on this
/// exact behavior; do not weaken the distinction.
fn render_operation(record: &OperationRecord) {
    println!("Operation: {}", record.id);
    println!("  type: {}", record.operation_type);
    println!("  status: {}", operation_status_label(record.status));
    println!(
        "  target: {} {}",
        record.target_object_type, record.target_object_id
    );
    if let Some(started_at) = &record.started_at {
        println!("  startedAt: {started_at}");
    }
    if let Some(completed_at) = &record.completed_at {
        println!("  completedAt: {completed_at}");
    }
    println!("  createdAt: {}", record.created_at);
    match record.status {
        OperationStatus::Failed => {
            // Preserve the server's specific failure detail rather than a generic line.
            let detail = record
                .error
                .as_deref()
                .unwrap_or("no error detail provided");
            println!("  error: {detail}");
        }
        OperationStatus::Succeeded => {
            if is_parse_producing_operation(&record.operation_type) {
                println!(
                    "  note: the operation lifecycle completed, but this does NOT confirm the \
                     domain outcome. Check the parse run row for the domain verdict; for a held \
                     result run `held-parses`."
                );
            }
        }
        OperationStatus::Pending | OperationStatus::Running => {}
    }
}

/// Whether an operation type produces a parse whose domain outcome lives in the
/// parse run row (not in `Operation.status`), so a `succeeded` lifecycle must be
/// paired with a pointer to the domain verdict. Matches the §34.6 operationType
/// wire names for the parse-producing operations.
fn is_parse_producing_operation(operation_type: &str) -> bool {
    matches!(
        operation_type,
        "source_ingest" | "parser_execution" | "parse_activation"
    )
}

/// Human label for an Operation lifecycle status.
fn operation_status_label(status: OperationStatus) -> &'static str {
    match status {
        OperationStatus::Pending => "pending",
        OperationStatus::Running => "running",
        OperationStatus::Succeeded => "succeeded",
        OperationStatus::Failed => "failed",
    }
}

// Character budget for excerpting long text projections in the query result.
// Bounds terminal output; the full text stays reachable because `body` is always
// rendered in full below the excerpt (nothing is hidden, only summarized).
const TEXT_EXCERPT_CHARS: usize = 800;

/// Render the §34.1 query result: the EvidencePack's units human-readably, with
/// the full pack detail (body, relationships, annotations, assembly trace) kept
/// reachable as pretty JSON, plus the optional debug diagnostics summary. The
/// per-unit `body` and the trace are rendered IN FULL so the rich pack is never
/// silently narrowed (rendering honesty); the summary supplements, not replaces.
fn render_query(response: &QueryResponse) {
    let pack = &response.evidence_pack;
    println!("Query: {}", pack.query_id);
    println!("  text: {}", pack.query_text);
    println!("  assembledAt: {}", pack.created_at);
    println!("  evidenceUnits: {}", pack.evidence_units.len());
    for (position, unit) in pack.evidence_units.iter().enumerate() {
        println!(
            "  [{}] unit {} ({})",
            position, unit.unit_id, unit.content_type
        );
        println!("      source: {} parse: {}", unit.source_id, unit.parse_id);
        if let Some(score) = unit.score {
            println!("      score: {score}");
        }
        if let Some(reasons) = &unit.reasons
            && !reasons.is_empty()
        {
            println!("      reasons: {}", reasons.join(", "));
        }
        if let Some(text) = &unit.text_projection {
            println!("      text: {}", excerpt_text(text, TEXT_EXCERPT_CHARS));
        }
        if let Some(locators) = &unit.locators {
            println!("      locators: {}", locators.len());
        }
        // The §18 body is arbitrary JSON; render it in full so nothing is lost.
        print_labeled_json("      body:", &unit.body);
    }
    if let Some(relationships) = &pack.relationships {
        println!("  relationships: {}", relationships.len());
    }
    if let Some(annotations) = &pack.annotations {
        println!("  annotations: {}", annotations.len());
    }
    // The §27 assembly trace makes selection auditable; keep it fully reachable.
    print_labeled_json("  assemblyTrace:", &pack.assembly_trace);
    if let Some(diagnostics) = &response.diagnostics {
        render_query_diagnostics(diagnostics);
    }
}

/// Render the §24.3 debug diagnostics: per-stage latencies and the ranked score
/// tables, with the fused candidate pool counted (its rich §24.4 hit shape stays
/// reachable in full rather than being narrowed to a summary line only).
fn render_query_diagnostics(diagnostics: &QueryDiagnostics) {
    println!("  diagnostics:");
    let latencies = &diagnostics.latencies;
    println!("    latencies (ms):");
    println!("      openTransaction: {}", latencies.open_transaction_ms);
    println!("      capture: {}", latencies.capture_ms);
    println!("      queryEmbed: {}", latencies.query_embed_ms);
    println!(
        "      denseLexicalFusion: {}",
        latencies.dense_lexical_fusion_ms
    );
    println!("      graph: {}", latencies.graph_ms);
    println!("      maxsim: {}", latencies.maxsim_ms);
    println!("      rerank: {}", latencies.rerank_ms);
    println!("      assembly: {}", latencies.assembly_ms);
    println!("      snapshotHeld: {}", latencies.snapshot_held_ms);
    println!("    fusedPool: {} candidates", diagnostics.fused_pool.len());
    if !diagnostics.fused_pool.is_empty() {
        // The pool carries the rich §24.4 hit shape; render it in full so the
        // debug surface is lossless, not just a count.
        print_labeled_json(
            "    fusedPoolDetail:",
            &serde_json::json!(diagnostics.fused_pool),
        );
    }
    if !diagnostics.maxsim.is_empty() {
        println!("    maxsim:");
        for score in &diagnostics.maxsim {
            println!(
                "      #{} unit {} score {}",
                score.rank, score.unit_id, score.score
            );
        }
    }
    if !diagnostics.reranked.is_empty() {
        println!("    reranked:");
        for score in &diagnostics.reranked {
            let mut line = format!(
                "      #{} unit {} score {}",
                score.rank, score.unit_id, score.score
            );
            if let Some(logit) = score.logit {
                line.push_str(&format!(" logit {logit}"));
            }
            if let Some(token_count) = score.token_count {
                line.push_str(&format!(" tokens {token_count}"));
            }
            println!("{line}");
        }
    }
}

/// Render the §13.4 held-parses listing. The operator-salient fields are shown
/// per parse; the conformance report's per-dimension regression detail (the
/// disposition cause named by `heldReason`) is rendered in full, and warnings/
/// metrics/report are kept reachable as pretty JSON rather than dropped.
fn render_held_parses(response: &HeldParsesResponse) {
    if response.parses.is_empty() {
        println!("Held parses: none");
        return;
    }
    println!("Held parses: {}", response.parses.len());
    for parse in &response.parses {
        println!("  parse {} (status {})", parse.id, parse.status);
        println!("    source: {}", parse.source_id);
        println!("    parser: {} {}", parse.parser_name, parse.parser_version);
        if let Some(held_reason) = &parse.held_reason {
            println!("    heldReason: {held_reason}");
        }
        println!("    createdAt: {}", parse.created_at);
        if let Some(started_at) = &parse.started_at {
            println!("    startedAt: {started_at}");
        }
        if let Some(completed_at) = &parse.completed_at {
            println!("    completedAt: {completed_at}");
        }
        if let Some(error) = &parse.error {
            println!("    error: {error}");
        }
        if let Some(warnings) = &parse.warnings {
            println!("    warnings: {}", warnings.len());
        }
        if let Some(metrics) = &parse.metrics {
            print_labeled_json("    metrics:", metrics);
        }
        if let Some(report) = &parse.conformance_report {
            // The report has no single verdict field; the per-dimension detail is
            // the regression cause. Surface `dimensions` prominently, then keep
            // the full report reachable.
            if let Some(dimensions) = report.get("dimensions").and_then(|value| value.as_object()) {
                println!("    conformance dimensions:");
                for (name, value) in dimensions {
                    println!("      {name}: {value}");
                }
            }
            print_labeled_json("    conformanceReport:", report);
        }
    }
}

/// Render one §15 ContentUnit. Hashes and structural convenience fields are
/// surfaced compactly; the arbitrary §18 `body` is rendered in full (lossless).
fn render_unit(unit: &ContentUnitView) {
    println!("Unit: {}", unit.id);
    println!("  source: {} parse: {}", unit.source_id, unit.parse_id);
    println!("  contentType: {}", unit.content_type);
    println!("  bodyHash: {}", unit.body_hash);
    if let Some(text_hash) = &unit.text_hash {
        println!("  textHash: {text_hash}");
    }
    if let Some(structure_hash) = &unit.structure_hash {
        println!("  structureHash: {structure_hash}");
    }
    if let Some(primary_parent_id) = &unit.primary_parent_id {
        println!("  primaryParentId: {primary_parent_id}");
    }
    if let Some(sequence_index) = unit.sequence_index {
        println!("  sequenceIndex: {sequence_index}");
    }
    if let Some(locators) = &unit.locators {
        println!("  locators: {}", locators.len());
        print_labeled_json("  locatorsDetail:", &serde_json::json!(locators));
    }
    println!("  createdAt: {}", unit.created_at);
    if let Some(deleted_at) = &unit.deleted_at {
        println!("  deletedAt: {deleted_at}");
    }
    print_labeled_json("  body:", &unit.body);
}

/// Render the §19 relationships listing for a unit.
fn render_relationships(response: &RelationshipsResponse) {
    if response.relationships.is_empty() {
        println!("Relationships: none");
        return;
    }
    println!("Relationships: {}", response.relationships.len());
    for relationship in &response.relationships {
        println!(
            "  {} {} -> {}",
            relationship.relationship_type, relationship.from_unit_id, relationship.to_unit_id
        );
        println!("    id: {}", relationship.id);
        println!(
            "    source: {} parse: {}",
            relationship.source_id, relationship.parse_id
        );
        if let Some(role) = &relationship.relationship_role {
            println!("    role: {role}");
        }
        if let Some(sequence_index) = relationship.sequence_index {
            println!("    sequenceIndex: {sequence_index}");
        }
        if let Some(confidence) = relationship.confidence {
            println!("    confidence: {confidence}");
        }
        if let Some(provenance) = &relationship.provenance {
            print_labeled_json("    provenance:", provenance);
        }
        println!("    createdAt: {}", relationship.created_at);
        if let Some(deleted_at) = &relationship.deleted_at {
            println!("    deletedAt: {deleted_at}");
        }
    }
}

/// Render one §10 SourceObject. Location freshness (`lastSeenAt`) and `status`
/// are surfaced prominently per location; deletion evidence and metadata stay
/// reachable in full.
fn render_source(source: &SourceView) {
    println!("Source: {}", source.id);
    match &source.active_parse_id {
        Some(active_parse_id) => println!("  activeParseId: {active_parse_id}"),
        None => println!("  activeParseId: none (no active parse)"),
    }
    println!("  mimeType: {}", source.mime_type);
    if let Some(size_bytes) = source.size_bytes {
        println!("  sizeBytes: {size_bytes}");
    }
    println!("  sourceHash: {}", source.source_hash);
    println!("  storageUri: {}", source.storage_uri);
    if let Some(event_time) = &source.event_time {
        println!("  eventTime: {event_time}");
    }
    println!("  ingestTime: {}", source.ingest_time);
    println!("  createdAt: {}", source.created_at);
    if let Some(deactivated_at) = &source.deactivated_at {
        println!("  deactivatedAt: {deactivated_at}");
    }
    if source.locations.is_empty() {
        println!("  locations: none");
        return;
    }
    println!("  locations: {}", source.locations.len());
    for location in &source.locations {
        println!(
            "    {}:{} [{}]",
            location.source_system, location.native_uri, location.status
        );
        println!("      id: {}", location.id);
        if let Some(native_id) = &location.native_id {
            println!("      nativeId: {native_id}");
        }
        println!("      governanceDomain: {}", location.governance_domain);
        println!("      firstSeenAt: {}", location.first_seen_at);
        println!("      lastSeenAt: {}", location.last_seen_at);
        if let Some(deletion_evidence) = &location.deletion_evidence {
            print_labeled_json("      deletionEvidence:", deletion_evidence);
        }
        if let Some(metadata) = &location.metadata {
            print_labeled_json("      metadata:", metadata);
        }
    }
}

/// Render the §9.5–§9.6 sync scheduler health snapshot.
fn render_sync_status(status: &SyncStatusView) {
    println!("Sync status:");
    println!("  fabricReady: {}", yes_no(status.fabric_ready));
    if let Some(detail) = &status.detail {
        println!("  detail: {detail}");
    }
    println!("  pending: {}", status.pending);
    println!("  inFlight: {}", status.in_flight);
    println!("  failed: {}", status.failed);
    println!("  coalescedTotal: {}", status.coalesced_total);
    if let Some(cadence_ms) = status.cadence_ms {
        println!("  cadenceMs: {cadence_ms}");
    }
    if let Some(last_success_at) = &status.last_success_at {
        println!("  lastSuccessAt: {last_success_at}");
    }
}

/// Excerpt long text to a character budget, appending an ellipsis when clipped,
/// so a long text projection does not flood the terminal. The excerpt is a
/// summary only — the full unit `body` is always rendered alongside it, so no
/// content is hidden by the clip.
fn excerpt_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let clipped: String = text.chars().take(max_chars).collect();
    format!("{clipped}…")
}

/// Print a labeled block of pretty-printed JSON so rich/nested payloads are
/// rendered in full (lossless). `to_string_pretty` on an already-decoded value
/// cannot fail in practice; fall back to the Display form rather than dropping
/// the payload.
fn print_labeled_json(label: &str, value: &serde_json::Value) {
    println!("{label}");
    let rendered = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    for line in rendered.lines() {
        println!("    {line}");
    }
}

/// Print command syntax without describing hidden or unsupported shell behavior.
fn render_help() {
    println!("Commands:");
    for spec in COMMAND_SPECS {
        println!("  {}", spec.repl_usage);
    }
}

/// Print executable-level usage for interactive and one-shot command modes.
fn render_cli_help() {
    println!("Usage:");
    println!("  data-store [--config <path>]");
    for spec in COMMAND_SPECS {
        if let Some(usage) = spec.cli_usage {
            println!("  {usage}");
        }
    }
    println!();
    println!("Options:");
    println!("  --config <path>  Service config path; defaults to config.toml");
    println!("  --help, -h       Print this help without reading config");
}

/// Render boolean readiness flags without adding presentation-only state to DTOs.
fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
