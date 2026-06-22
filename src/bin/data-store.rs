use std::{
    env,
    error::Error,
    fmt::{self, Display},
    fs,
    io::{self, BufRead, BufReader, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::{StatusCode, blocking::Client};
use rustyline::{DefaultEditor, error::ReadlineError};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

const PROMPT: &str = "data-store> ";
const DEFAULT_CONFIG_PATH: &str = "config.toml";
const HISTORY_FILE_NAME: &str = ".data-store.history";
const SEARCH_EXCERPT_CHARS: usize = 800;
const OPERATIONS_PATH: &str = "/v1/operations";
const HEALTH_PATH: &str = "/v1/health";
const HEALTH_PROBE_TIMEOUT_SECONDS: u64 = 5;
const SHUTDOWN_STATUS_COMPLETE: &str = "shutdown_complete";
const SOURCE_ALREADY_INGESTED_ERROR_KIND: &str = "source_already_ingested";
const INGEST_OPERATION_NAME: &str = "ingest";
const SEARCH_OPERATION_NAME: &str = "search";

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

#[derive(Debug)]
enum Command {
    Health,
    Limits,
    Sources,
    Ingest {
        source: String,
        force: bool,
    },
    Search {
        query: String,
        top_k: Option<u32>,
    },
    SearchFull {
        query: String,
        top_k: Option<u32>,
    },
    Versions,
    Rollback {
        source: String,
        version_label: String,
    },
    Shutdown,
    Help,
    Exit,
}

/// Construct a typed command when the registry says the command accepts no arguments.
type NoArgCommandBuilder = fn() -> Command;
/// Construct a typed ingest command after parsing the source and optional force flag.
type IngestCommandBuilder = fn(String, bool) -> Command;
/// Construct a typed command when the registry says the command accepts two values.
type TwoValueCommandBuilder = fn(String, String) -> Command;
/// Construct a typed search command after parsing the optional topK value.
type SearchCommandBuilder = fn(String, Option<u32>) -> Command;

#[derive(Clone, Copy)]
enum CommandArguments {
    /// The command accepts no positional values in either REPL or CLI form.
    NoArgs { build: NoArgCommandBuilder },
    /// Ingest accepts one required source plus an optional `--force` flag.
    Ingest { build: IngestCommandBuilder },
    /// The command accepts two required values, such as rollback source and version label.
    TwoValues {
        first_value_name: &'static str,
        second_value_name: &'static str,
        build: TwoValueCommandBuilder,
    },
    /// Search has one required query plus an optional topK value in both command surfaces.
    Search { build: SearchCommandBuilder },
}

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
    /// Argument parser shape plus constructor used to build the typed command.
    arguments: CommandArguments,
}

// Keep command metadata in one table so REPL parsing, CLI parsing, and help
// rendering cannot drift apart. This is the Rust equivalent of a C dispatch
// table: the command names and help text sit beside the constructor function
// pointer and the argument shape used by both front doors.
const COMMAND_SPECS: &[CommandSpec] = &[
    CommandSpec {
        repl_name: "health",
        repl_aliases: &[],
        cli_flag: Some("--health"),
        cli_aliases: &[],
        repl_usage: "health",
        cli_usage: Some("data-store [--config <path>] --health"),
        arguments: CommandArguments::NoArgs {
            build: build_health_command,
        },
    },
    CommandSpec {
        repl_name: "limits",
        repl_aliases: &[],
        cli_flag: Some("--limits"),
        cli_aliases: &[],
        repl_usage: "limits",
        cli_usage: Some("data-store [--config <path>] --limits"),
        arguments: CommandArguments::NoArgs {
            build: build_limits_command,
        },
    },
    CommandSpec {
        repl_name: "sources",
        repl_aliases: &[],
        cli_flag: Some("--sources"),
        cli_aliases: &[],
        repl_usage: "sources",
        cli_usage: Some("data-store [--config <path>] --sources"),
        arguments: CommandArguments::NoArgs {
            build: build_sources_command,
        },
    },
    CommandSpec {
        repl_name: "ingest",
        repl_aliases: &[],
        cli_flag: Some("--ingest"),
        cli_aliases: &[],
        repl_usage: "ingest <source> [--force]",
        cli_usage: Some("data-store [--config <path>] --ingest <source> [--force]"),
        arguments: CommandArguments::Ingest {
            build: build_ingest_command,
        },
    },
    CommandSpec {
        repl_name: "search",
        repl_aliases: &[],
        cli_flag: Some("--search"),
        cli_aliases: &[],
        repl_usage: "search <query> [topK]",
        cli_usage: Some("data-store [--config <path>] --search <query> [topK]"),
        arguments: CommandArguments::Search {
            build: build_search_command,
        },
    },
    CommandSpec {
        repl_name: "search-full",
        repl_aliases: &[],
        cli_flag: Some("--search-full"),
        cli_aliases: &[],
        repl_usage: "search-full <query> [topK]",
        cli_usage: Some("data-store [--config <path>] --search-full <query> [topK]"),
        arguments: CommandArguments::Search {
            build: build_search_full_command,
        },
    },
    CommandSpec {
        repl_name: "versions",
        repl_aliases: &[],
        cli_flag: Some("--versions"),
        cli_aliases: &[],
        repl_usage: "versions",
        cli_usage: Some("data-store [--config <path>] --versions"),
        arguments: CommandArguments::NoArgs {
            build: build_versions_command,
        },
    },
    CommandSpec {
        repl_name: "rollback",
        repl_aliases: &[],
        cli_flag: Some("--rollback"),
        cli_aliases: &[],
        repl_usage: "rollback <source> <versionLabel>",
        cli_usage: Some("data-store [--config <path>] --rollback <source> <versionLabel>"),
        arguments: CommandArguments::TwoValues {
            first_value_name: "source",
            second_value_name: "versionLabel",
            build: build_rollback_command,
        },
    },
    CommandSpec {
        repl_name: "shutdown",
        repl_aliases: &[],
        cli_flag: Some("--shutdown"),
        cli_aliases: &[],
        repl_usage: "shutdown",
        cli_usage: Some("data-store [--config <path>] --shutdown"),
        arguments: CommandArguments::NoArgs {
            build: build_shutdown_command,
        },
    },
    CommandSpec {
        repl_name: "help",
        repl_aliases: &[],
        cli_flag: Some("--help"),
        cli_aliases: &["-h"],
        repl_usage: "help",
        cli_usage: Some("data-store --help"),
        arguments: CommandArguments::NoArgs {
            build: build_help_command,
        },
    },
    CommandSpec {
        repl_name: "exit",
        repl_aliases: &["quit"],
        cli_flag: None,
        cli_aliases: &[],
        repl_usage: "exit",
        cli_usage: None,
        arguments: CommandArguments::NoArgs {
            build: build_exit_command,
        },
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
}

#[derive(Debug, Deserialize)]
struct LimitsResponse {
    request: RequestLimitsResponse,
    retrieval: RetrievalLimitsResponse,
}

#[derive(Debug, Deserialize)]
struct RequestLimitsResponse {
    #[serde(rename = "maxRequestBodyBytes")]
    max_request_body_bytes: usize,
    #[serde(rename = "maxIngestSourceChars")]
    max_ingest_source_chars: u32,
    #[serde(rename = "maxSearchQueryChars")]
    max_search_query_chars: u32,
}

#[derive(Debug, Deserialize)]
struct RetrievalLimitsResponse {
    #[serde(rename = "defaultTopK")]
    default_top_k: u32,
    #[serde(rename = "maxTopK")]
    max_top_k: u32,
}

#[derive(Debug, Serialize)]
struct IngestRequest {
    source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    force: Option<bool>,
}

/// Client mirror of the server's per-operation benchmark tree. The client
/// renders this as-is and performs no timing of its own.
#[derive(Debug, Deserialize)]
struct OperationBenchmarks {
    stages: Vec<BenchmarkStage>,
    #[serde(rename = "totalMs")]
    total_ms: u64,
}

/// One measured stage in a server benchmark tree; `children` is empty for leaves.
#[derive(Debug, Deserialize)]
struct BenchmarkStage {
    stage: String,
    #[serde(rename = "elapsedMs")]
    elapsed_ms: u64,
    children: Vec<BenchmarkStage>,
}

#[derive(Debug, Deserialize)]
struct IngestResponse {
    #[serde(rename = "documentId")]
    document_id: String,
    #[serde(rename = "versionLabel")]
    version_label: String,
    #[serde(rename = "unitsIngested")]
    units_ingested: u32,
    status: String,
    // Server-authoritative benchmarks; absent until the ingest server phase lands.
    #[serde(default)]
    benchmarks: Option<OperationBenchmarks>,
}

#[derive(Debug, Serialize)]
struct SearchRequest {
    query: String,
    #[serde(rename = "topK", skip_serializing_if = "Option::is_none")]
    top_k: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    results: Vec<SearchResult>,
    // Server-authoritative benchmarks; the server always emits these for search.
    #[serde(default)]
    benchmarks: Option<OperationBenchmarks>,
    raw: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct SearchResult {
    #[serde(rename = "unitId")]
    unit_id: String,
    score: f32,
    content: String,
    #[serde(rename = "headingPath")]
    heading_path: Vec<String>,
    #[serde(rename = "sourcePath")]
    source_path: String,
    #[serde(rename = "pageNumbers")]
    page_numbers: Vec<u32>,
}

#[derive(Debug, Deserialize)]
struct IngestedSourceListing {
    sources: Vec<IngestedSourceRecord>,
}

#[derive(Debug, Deserialize)]
struct IngestedSourceRecord {
    #[serde(rename = "sourcePath")]
    source_path: String,
    #[serde(rename = "activeVersionLabel")]
    active_version_label: String,
    #[serde(rename = "documentId")]
    document_id: String,
    #[serde(rename = "unitsIngested")]
    units_ingested: u32,
    status: String,
    #[serde(rename = "createdAtMs")]
    created_at_ms: u64,
    #[serde(rename = "updatedAtMs")]
    updated_at_ms: u64,
}

#[derive(Debug, Deserialize)]
struct DocumentVersionListing {
    sources: Vec<SourceDocumentVersionListing>,
}

#[derive(Debug, Deserialize)]
struct SourceDocumentVersionListing {
    #[serde(rename = "sourcePath")]
    source_path: String,
    #[serde(rename = "activeVersionLabel")]
    active_version_label: Option<String>,
    versions: Vec<DocumentVersionRecord>,
}

#[derive(Debug, Deserialize)]
struct DocumentVersionRecord {
    #[serde(rename = "versionLabel")]
    version_label: String,
    #[serde(rename = "documentId")]
    document_id: String,
    #[serde(rename = "isActive")]
    is_active: bool,
    #[serde(rename = "unitsIngested")]
    units_ingested: u32,
    status: String,
    #[serde(rename = "createdAtMs")]
    created_at_ms: u64,
    #[serde(rename = "updatedAtMs")]
    updated_at_ms: u64,
    #[serde(rename = "denseVectorMetadata")]
    dense_vector_metadata: Vec<DenseVectorMetadataRecord>,
    #[serde(rename = "colbertVectorMetadata")]
    colbert_vector_metadata: Vec<ColbertVectorMetadataRecord>,
}

#[derive(Debug, Deserialize)]
struct DenseVectorMetadataRecord {
    #[serde(rename = "modelDimension")]
    model_dimension: u32,
    #[serde(rename = "vectorCount")]
    vector_count: u32,
}

#[derive(Debug, Deserialize)]
struct ColbertVectorMetadataRecord {
    #[serde(rename = "modelDimension")]
    model_dimension: u32,
    #[serde(rename = "vectorCount")]
    vector_count: u32,
}

#[derive(Debug, Serialize)]
struct DocumentVersionRollbackRequest {
    source: String,
    #[serde(rename = "versionLabel")]
    version_label: String,
}

#[derive(Debug, Deserialize)]
struct DocumentVersionRollbackResponse {
    #[serde(rename = "sourcePath")]
    source_path: String,
    #[serde(rename = "activeVersionLabel")]
    active_version_label: String,
    #[serde(rename = "publishedAtMs")]
    published_at_ms: u64,
    #[serde(rename = "vectorCount")]
    vector_count: usize,
    status: String,
}

#[derive(Debug, Deserialize)]
struct ShutdownResponse {
    status: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct OperationRequest {
    #[serde(rename = "operationId", skip_serializing_if = "Option::is_none")]
    operation_id: Option<String>,
    operation: &'static str,
    payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum OperationEvent {
    #[serde(rename = "status")]
    Status {
        #[serde(rename = "operationId")]
        operation_id: String,
        sequence: u64,
        stage: Option<String>,
        message: Option<String>,
    },
    #[serde(rename = "progress")]
    Progress {
        #[serde(rename = "operationId")]
        operation_id: String,
        sequence: u64,
        stage: Option<String>,
        message: Option<String>,
        current: Option<u64>,
        total: Option<u64>,
    },
    #[serde(rename = "result")]
    Result {
        #[serde(rename = "operationId")]
        operation_id: String,
        sequence: u64,
        payload: serde_json::Value,
    },
    #[serde(rename = "error")]
    Error {
        #[serde(rename = "operationId")]
        operation_id: String,
        sequence: u64,
        stage: Option<String>,
        error: ErrorDetail,
    },
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Debug, Deserialize)]
struct ErrorDetail {
    status: Option<u16>,
    kind: Option<String>,
    message: String,
}

#[derive(Debug)]
struct LastStreamEvent {
    operation_id: String,
    sequence: u64,
    event_type: &'static str,
    stage: Option<String>,
    message: Option<String>,
}

#[derive(Debug)]
struct AmbiguousStreamLossError {
    report: String,
}

#[derive(Debug)]
enum HealthProbeOutcome {
    Ready(String),
    NotReady(String),
    Unreachable(String),
    Failed(String),
}

struct StreamRenderer {
    active_line_chars: usize,
    docling_wait_line_active: bool,
}

impl Display for AmbiguousStreamLossError {
    /// Render the complete ambiguous-outcome report as the user-facing error text.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.report)
    }
}

impl Error for AmbiguousStreamLossError {}

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
    let context = ClientContext {
        base_url: base_url_for_bind_address(config.server.bind_address),
        token_file_path: resolve_service_root_path(&config.admin.token_file_path),
        http: build_http_client(&config.client)?,
    };
    match command {
        Some(command) => {
            execute_command(&context, command)?;
            Ok(())
        }
        None => run_repl(context),
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

/// Parse REPL arguments using the command's registry-owned argument shape.
fn parse_repl_command(spec: &CommandSpec, args: &[String]) -> Result<Command> {
    match spec.arguments {
        CommandArguments::NoArgs { build } => {
            require_repl_arg_count(spec, args, 0)?;
            Ok(build())
        }
        CommandArguments::Ingest { build } => {
            if args.len() != 1 && args.len() != 2 {
                bail!("usage: {}", spec.repl_usage);
            }
            let force = parse_optional_ingest_force(args.get(1), spec.repl_usage)?;
            Ok(build(args[0].clone(), force))
        }
        CommandArguments::TwoValues { build, .. } => {
            require_repl_arg_count(spec, args, 2)?;
            Ok(build(args[0].clone(), args[1].clone()))
        }
        CommandArguments::Search { build } => {
            if args.len() != 1 && args.len() != 2 {
                bail!("usage: {}", spec.repl_usage);
            }
            let top_k = match args.get(1) {
                Some(value) => Some(parse_top_k(value)?),
                None => None,
            };
            Ok(build(args[0].clone(), top_k))
        }
    }
}

/// Parse one CLI operation flag and advance the argv cursor past its arguments.
fn parse_cli_command(spec: &CommandSpec, args: &[String], index: &mut usize) -> Result<Command> {
    *index += 1;
    match spec.arguments {
        CommandArguments::NoArgs { build } => Ok(build()),
        CommandArguments::Ingest { build } => {
            let source = take_cli_ingest_source_value(spec, args, index)?;
            let force = take_optional_cli_ingest_force(spec, args, index)?;
            Ok(build(source, force))
        }
        CommandArguments::TwoValues {
            first_value_name,
            second_value_name,
            build,
        } => {
            let first = take_cli_command_value(spec, args, index, first_value_name)?;
            let second = take_cli_command_value(spec, args, index, second_value_name)?;
            Ok(build(first, second))
        }
        CommandArguments::Search { build } => {
            let query = take_cli_command_value(spec, args, index, "query")?;
            let top_k = take_optional_cli_top_k(args, index)?;
            Ok(build(query, top_k))
        }
    }
}

/// Enforce exact REPL arity while reporting the usage line owned by the registry.
fn require_repl_arg_count(spec: &CommandSpec, args: &[String], expected: usize) -> Result<()> {
    if args.len() == expected {
        return Ok(());
    }

    bail!("usage: {}", spec.repl_usage);
}

/// Take one required command-line positional value for the current command.
fn take_cli_command_value(
    spec: &CommandSpec,
    args: &[String],
    index: &mut usize,
    value_name: &str,
) -> Result<String> {
    let Some(value) = args.get(*index) else {
        bail!(
            "{} requires a {value_name}",
            spec.cli_flag.unwrap_or(spec.repl_name)
        );
    };
    if is_startup_flag(value) {
        bail!(
            "{} requires a {value_name}",
            spec.cli_flag.unwrap_or(spec.repl_name)
        );
    }
    *index += 1;
    Ok(value.clone())
}

/// Take ingest's required source while reserving `--force` for the optional flag position.
fn take_cli_ingest_source_value(
    spec: &CommandSpec,
    args: &[String],
    index: &mut usize,
) -> Result<String> {
    let Some(value) = args.get(*index) else {
        bail!(
            "{} requires a source",
            spec.cli_flag.unwrap_or(spec.repl_name)
        );
    };
    if is_startup_flag(value) || value == "--force" {
        bail!(
            "{} requires a source",
            spec.cli_flag.unwrap_or(spec.repl_name)
        );
    }
    *index += 1;
    Ok(value.clone())
}

/// Parse search's optional command-line `topK` while leaving the next flag untouched.
fn take_optional_cli_top_k(args: &[String], index: &mut usize) -> Result<Option<u32>> {
    let Some(value) = args.get(*index) else {
        return Ok(None);
    };
    if is_startup_flag(value) {
        return Ok(None);
    }
    let top_k = parse_top_k(value)?;
    *index += 1;
    Ok(Some(top_k))
}

/// Parse ingest's optional REPL force flag without accepting unrelated trailing values.
fn parse_optional_ingest_force(value: Option<&String>, usage: &str) -> Result<bool> {
    let Some(value) = value else {
        return Ok(false);
    };
    if value == "--force" {
        return Ok(true);
    }

    bail!("usage: {usage}");
}

/// Parse ingest's optional command-line force flag while leaving later startup flags untouched.
fn take_optional_cli_ingest_force(
    spec: &CommandSpec,
    args: &[String],
    index: &mut usize,
) -> Result<bool> {
    let Some(value) = args.get(*index) else {
        return Ok(false);
    };
    if is_startup_flag(value) {
        return Ok(false);
    }
    if value == "--force" {
        *index += 1;
        return Ok(true);
    }

    bail!("usage: {}", spec.cli_usage.unwrap_or(spec.repl_usage));
}

/// Parse a user-provided `topK` value without imposing server-owned range validation.
fn parse_top_k(value: &str) -> Result<u32> {
    value
        .parse::<u32>()
        .with_context(|| format!("topK must be an integer, got `{value}`"))
}

/// Build the typed health command from a no-argument registry entry.
fn build_health_command() -> Command {
    Command::Health
}

/// Build the typed limits command from a no-argument registry entry.
fn build_limits_command() -> Command {
    Command::Limits
}

/// Build the typed sources command from a no-argument registry entry.
fn build_sources_command() -> Command {
    Command::Sources
}

/// Build the typed ingest command after shared parsing has captured the source and force flag.
fn build_ingest_command(source: String, force: bool) -> Command {
    Command::Ingest { source, force }
}

/// Build the excerpted search command after shared parsing has captured query and topK.
fn build_search_command(query: String, top_k: Option<u32>) -> Command {
    Command::Search { query, top_k }
}

/// Build the full-content search command after shared parsing has captured query and topK.
fn build_search_full_command(query: String, top_k: Option<u32>) -> Command {
    Command::SearchFull { query, top_k }
}

/// Build the typed versions command from a no-argument registry entry.
fn build_versions_command() -> Command {
    Command::Versions
}

/// Build the typed rollback command after shared parsing has captured both identifiers.
fn build_rollback_command(source: String, version_label: String) -> Command {
    Command::Rollback {
        source,
        version_label,
    }
}

/// Build the typed shutdown command from a no-argument registry entry.
fn build_shutdown_command() -> Command {
    Command::Shutdown
}

/// Build the local help command; CLI startup intercepts this before config loading.
fn build_help_command() -> Command {
    Command::Help
}

/// Build the local exit command, which is intentionally REPL-only.
fn build_exit_command() -> Command {
    Command::Exit
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

/// Build the blocking HTTP client with the configured operation timeout.
fn build_http_client(config: &ClientRuntimeConfig) -> Result<Client> {
    Client::builder()
        .timeout(Duration::from_secs(config.operation_timeout_seconds))
        .build()
        .context("failed to build HTTP client")
}

/// Resolve service-relative local paths through Cargo's manifest root.
fn resolve_service_root_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }

    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
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
fn run_repl(context: ClientContext) -> Result<()> {
    println!("Data Store client connected to {}", context.base_url);
    println!("Type `help` for commands. Type `exit` to quit.");
    let history_path = resolve_service_root_path(Path::new(HISTORY_FILE_NAME));
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

/// Execute one parsed command and return whether the REPL should continue.
fn execute_command(context: &ClientContext, command: Command) -> Result<bool> {
    match command {
        Command::Health => {
            render_health(send_operation(context, "health", empty_payload(), false)?)
        }
        Command::Limits => {
            render_limits(send_operation(context, "limits", empty_payload(), false)?)
        }
        Command::Sources => {
            render_sources(send_operation(context, "sources", empty_payload(), false)?);
        }
        Command::Ingest { source, force } => {
            let request = IngestRequest {
                source,
                force: force.then_some(true),
            };
            render_ingest(send_operation_output(
                context,
                INGEST_OPERATION_NAME,
                serde_json::to_value(request)
                    .context("failed to encode ingest operation payload")?,
                false,
                false,
            )?);
        }
        Command::Search { query, top_k } => {
            let request = SearchRequest { query, top_k };
            render_search(
                send_operation_output(
                    context,
                    SEARCH_OPERATION_NAME,
                    serde_json::to_value(request)
                        .context("failed to encode search operation payload")?,
                    false,
                    false,
                )?,
                false,
            );
        }
        Command::SearchFull { query, top_k } => {
            let request = SearchRequest { query, top_k };
            render_search(
                send_operation_output(
                    context,
                    SEARCH_OPERATION_NAME,
                    serde_json::to_value(request)
                        .context("failed to encode search operation payload")?,
                    false,
                    false,
                )?,
                true,
            );
        }
        Command::Versions => {
            render_versions(send_operation(context, "versions", empty_payload(), true)?);
        }
        Command::Rollback {
            source,
            version_label,
        } => {
            let request = DocumentVersionRollbackRequest {
                source,
                version_label,
            };
            render_rollback(send_operation(
                context,
                "rollback",
                serde_json::to_value(request)
                    .context("failed to encode rollback operation payload")?,
                true,
            )?);
        }
        Command::Shutdown => {
            render_shutdown(send_operation(context, "shutdown", empty_payload(), true)?)?;
        }
        Command::Help => render_help(),
        Command::Exit => return Ok(false),
    }

    Ok(true)
}

/// Read the current startup token immediately before an admin command uses it.
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

/// Build the operation payload for commands that do not accept arguments.
fn empty_payload() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

/// Send one operation request and read its NDJSON event stream to a terminal event.
fn send_operation<T>(
    context: &ClientContext,
    operation: &'static str,
    payload: serde_json::Value,
    protected: bool,
) -> Result<T>
where
    T: DeserializeOwned,
{
    send_operation_output(context, operation, payload, protected, true)
}

/// Send one operation and optionally print the generic whole-operation elapsed line.
fn send_operation_output<T>(
    context: &ClientContext,
    operation: &'static str,
    payload: serde_json::Value,
    protected: bool,
    print_elapsed: bool,
) -> Result<T>
where
    T: DeserializeOwned,
{
    let method = "POST";
    let target_url = url(context, OPERATIONS_PATH);
    let request = OperationRequest {
        operation_id: None,
        operation,
        payload,
    };
    let mut builder = context
        .http
        .post(&target_url)
        .header("Accept", "application/x-ndjson")
        .json(&request);
    if protected {
        builder = builder.bearer_auth(read_admin_token(context)?);
    }

    println!("Operation: {operation}");
    println!("{method} {target_url}");
    let started = Instant::now();
    let response = builder
        .send()
        .with_context(|| format!("{method} {target_url} failed to send HTTP request"))?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().with_context(|| {
            format!("{method} {target_url} failed to read HTTP error response body")
        })?;
        return Err(service_error(method, &target_url, status, &text));
    }

    let payload =
        match read_operation_stream(response, context, operation, method, &target_url, started) {
            Ok(payload) => payload,
            Err(source) => {
                if print_elapsed && source.downcast_ref::<AmbiguousStreamLossError>().is_none() {
                    println!("Elapsed: {} ms", started.elapsed().as_millis());
                }
                return Err(source);
            }
        };
    if print_elapsed {
        println!("Elapsed: {} ms", started.elapsed().as_millis());
    }
    serde_json::from_value(payload)
        .with_context(|| format!("failed to parse {operation} result payload"))
}

/// Read streamed operation events until the service emits a terminal result or error.
fn read_operation_stream(
    response: reqwest::blocking::Response,
    context: &ClientContext,
    operation: &str,
    method: &str,
    target_url: &str,
    started: Instant,
) -> Result<serde_json::Value> {
    let mut renderer = StreamRenderer::new();
    let mut reader = BufReader::new(response);
    let mut last_event: Option<LastStreamEvent> = None;
    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = match reader.read_line(&mut line) {
            Ok(bytes_read) => bytes_read,
            Err(source) => {
                renderer.finish_progress_line()?;
                let health_probe = probe_health_after_stream_loss(context);
                return Err(AmbiguousStreamLossError {
                    report: format_ambiguous_stream_loss_report(
                        operation,
                        started.elapsed(),
                        last_event.as_ref(),
                        &health_probe,
                        Some(format!("stream read failed: {source}")),
                    ),
                }
                .into());
            }
        };
        if bytes_read == 0 {
            renderer.finish_progress_line()?;
            let health_probe = probe_health_after_stream_loss(context);
            return Err(AmbiguousStreamLossError {
                report: format_ambiguous_stream_loss_report(
                    operation,
                    started.elapsed(),
                    last_event.as_ref(),
                    &health_probe,
                    None,
                ),
            }
            .into());
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        let event: OperationEvent = match serde_json::from_str(trimmed) {
            Ok(event) => event,
            Err(source) => {
                renderer.finish_progress_line()?;
                let health_probe = probe_health_after_stream_loss(context);
                return Err(AmbiguousStreamLossError {
                    report: format_ambiguous_stream_loss_report(
                        operation,
                        started.elapsed(),
                        last_event.as_ref(),
                        &health_probe,
                        Some(format!(
                            "{method} {target_url} returned invalid NDJSON before terminal event: {source}"
                        )),
                    ),
                }
                .into());
            }
        };
        match event {
            OperationEvent::Status {
                operation_id,
                sequence,
                stage,
                message,
            } => {
                last_event = Some(LastStreamEvent {
                    operation_id: operation_id.clone(),
                    sequence,
                    event_type: "status",
                    stage: stage.clone(),
                    message: message.clone(),
                });
                renderer.render_status(&operation_id, sequence, stage, message)?
            }
            OperationEvent::Progress {
                operation_id,
                sequence,
                stage,
                message,
                current,
                total,
            } => {
                last_event = Some(LastStreamEvent {
                    operation_id: operation_id.clone(),
                    sequence,
                    event_type: "progress",
                    stage: stage.clone(),
                    message: message.clone(),
                });
                renderer.render_progress(&operation_id, sequence, stage, message, current, total)?
            }
            OperationEvent::Result {
                operation_id,
                sequence,
                payload,
            } => {
                renderer.finish_progress_line()?;
                println!("Result: operationId={operation_id} sequence={sequence}");
                return Ok(payload);
            }
            OperationEvent::Error {
                operation_id,
                sequence,
                stage,
                error,
            } => {
                renderer.finish_progress_line()?;
                return Err(operation_error(
                    operation,
                    &operation_id,
                    sequence,
                    stage.as_deref(),
                    &error,
                ));
            }
        }
    }
}

/// Build the operator-facing report for a stream that ended before a terminal event.
fn format_ambiguous_stream_loss_report(
    operation: &str,
    elapsed: Duration,
    last_event: Option<&LastStreamEvent>,
    health_probe: &HealthProbeOutcome,
    reason: Option<String>,
) -> String {
    let mut lines = vec![
        format!("operation `{operation}` stream ended before a terminal result/error"),
        "outcome: unknown".to_string(),
        format!("operationId: {}", format_last_operation_id(last_event)),
        format!("last event: {}", format_last_event(last_event)),
        format!("elapsed: {} ms", elapsed.as_millis()),
        "server may have crashed, closed the stream, or continued without delivering the terminal event"
            .to_string(),
        "success must not be inferred".to_string(),
        format!("next: {}", recommended_follow_up(operation)),
        format!("health probe: {}", format_health_probe_outcome(health_probe)),
        "health probe does not prove operation success".to_string(),
    ];
    if let Some(reason) = reason {
        lines.insert(1, format!("reason: {reason}"));
    }

    lines.join("\n")
}

/// Return the operation ID from the latest stream event, or an explicit unknown marker.
fn format_last_operation_id(last_event: Option<&LastStreamEvent>) -> &str {
    match last_event {
        Some(event) => event.operation_id.as_str(),
        None => "unknown",
    }
}

/// Render the latest non-terminal stream event without dropping nullable protocol fields.
fn format_last_event(last_event: Option<&LastStreamEvent>) -> String {
    let Some(event) = last_event else {
        return "none".to_string();
    };
    let stage = event.stage.as_deref().unwrap_or("unknown");
    let message = match &event.message {
        Some(message) => format!("{message:?}"),
        None => "none".to_string(),
    };

    format!(
        "sequence={} type={} stage={} message={}",
        event.sequence, event.event_type, stage, message
    )
}

/// Choose the next operator action based on the operation whose stream became ambiguous.
fn recommended_follow_up(operation: &str) -> &'static str {
    match operation {
        "ingest" => "run `health`; for ingest, run `versions` if authorized",
        "rollback" => "run `health`; for rollback, run `versions` if authorized",
        "shutdown" => "run `health`; if unreachable, confirm the service process exited",
        _ => "run `health`",
    }
}

/// Run a short compatibility health request after ambiguous stream loss.
fn probe_health_after_stream_loss(context: &ClientContext) -> HealthProbeOutcome {
    let target_url = url(context, HEALTH_PATH);
    let probe_http = match Client::builder()
        .timeout(Duration::from_secs(HEALTH_PROBE_TIMEOUT_SECONDS))
        .build()
    {
        Ok(client) => client,
        Err(source) => {
            return HealthProbeOutcome::Failed(format!(
                "failed to build health probe client: {source}"
            ));
        }
    };
    let response = match probe_http.get(&target_url).send() {
        Ok(response) => response,
        Err(source) if source.is_connect() => {
            return HealthProbeOutcome::Unreachable(source.to_string());
        }
        Err(source) => return HealthProbeOutcome::Failed(source.to_string()),
    };
    let status = response.status();
    if !status.is_success() {
        let text = response
            .text()
            .unwrap_or_else(|source| format!("failed to read health response body: {source}"));
        return HealthProbeOutcome::Failed(format!(
            "GET {target_url} HTTP {}: {}",
            status.as_u16(),
            text.trim()
        ));
    }
    match response.json::<HealthResponse>() {
        Ok(health) if health.ready => {
            HealthProbeOutcome::Ready(format_health_probe_details(&health))
        }
        Ok(health) => HealthProbeOutcome::NotReady(format_health_probe_details(&health)),
        Err(source) => HealthProbeOutcome::Failed(format!(
            "GET {target_url} returned invalid health JSON: {source}"
        )),
    }
}

/// Render the post-loss health probe without implying the lost operation's outcome.
fn format_health_probe_outcome(outcome: &HealthProbeOutcome) -> String {
    match outcome {
        HealthProbeOutcome::Ready(detail) => format!("service reachable and ready ({detail})"),
        HealthProbeOutcome::NotReady(detail) => {
            format!("service reachable but not ready ({detail})")
        }
        HealthProbeOutcome::Unreachable(detail) => format!("service unreachable ({detail})"),
        HealthProbeOutcome::Failed(detail) => {
            format!("health probe failed with status/error detail ({detail})")
        }
    }
}

/// Keep the ambiguous-outcome health probe compact while preserving admission diagnostics.
fn format_health_probe_details(health: &HealthResponse) -> String {
    let mut details = vec![format!("service={}", health.service)];
    if let Some(admission) = health
        .components
        .iter()
        .find(|component| component.name == "admission")
    {
        details.push(format!(
            "admission_ready={} admission_details={}",
            admission.ready,
            admission.details.join("; ")
        ));
    }

    details.join("; ")
}

/// Build a service URL from the configured base URL and a protocol path.
fn url(context: &ClientContext, path: &str) -> String {
    format!("{}{}", context.base_url, path)
}

/// Convert a non-success HTTP response into an operator-facing error.
fn service_error(method: &str, target_url: &str, status: StatusCode, text: &str) -> anyhow::Error {
    if let Ok(body) = serde_json::from_str::<ErrorBody>(text) {
        if let Some(message) = source_already_ingested_message(&body.error) {
            return anyhow!("{message}");
        }
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

/// Convert a terminal operation error event into a complete operator-facing error.
fn operation_error(
    operation: &str,
    operation_id: &str,
    sequence: u64,
    stage: Option<&str>,
    error: &ErrorDetail,
) -> anyhow::Error {
    if let Some(message) = source_already_ingested_message(error) {
        return anyhow!("{message}");
    }
    let stage = stage.unwrap_or("unknown");
    anyhow!(
        "operation `{operation}` failed: operationId={operation_id} sequence={sequence} stage={stage} {}",
        format_error_detail(error)
    )
}

/// Format structured service errors while tolerating legacy message-only error bodies.
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

/// Return the duplicate-ingest message when the service marks the error with the stable kind.
fn source_already_ingested_message(error: &ErrorDetail) -> Option<&str> {
    if error.kind.as_deref() == Some(SOURCE_ALREADY_INGESTED_ERROR_KIND) {
        return Some(error.message.as_str());
    }

    None
}

/// Print protocol-surfaced BM25 diagnostics required by the search response contract.
fn render_bm25_diagnostics(raw: &serde_json::Value) {
    let bm25 = raw
        .pointer("/storage/retrieval/bm25")
        .expect("search response missing raw.storage.retrieval.bm25 diagnostics");
    println!();
    println!("BM25 diagnostics:");
    println!("  mode: {}", required_raw_str(bm25, "mode"));
    println!(
        "  fts query present: {}",
        yes_no(required_raw_bool(bm25, "ftsQueryPresent"))
    );
    render_required_raw_u64(bm25, "ftsTermCount", "  fts terms");
    render_required_raw_u64(bm25, "ftsQueryBytes", "  fts query bytes");
    render_required_raw_u64(bm25, "activeVersionCount", "  active versions");
    render_required_raw_u64(bm25, "candidateLimit", "  candidate limit");
    render_required_raw_u64(bm25, "unfilteredCandidates", "  unfiltered candidates");
    render_required_raw_u64(bm25, "returnedCandidates", "  returned candidates");
    render_required_raw_u64(
        bm25,
        "strictReturnedCandidates",
        "  strict returned candidates",
    );
    println!(
        "  fallback ran: {}",
        yes_no(required_raw_bool(bm25, "fallbackRan"))
    );
    render_required_raw_u64(
        bm25,
        "fallbackReturnedCandidates",
        "  fallback returned candidates",
    );
    render_required_raw_ms(bm25, "connectionOpenLatencyMs", "  connection open");
    render_required_raw_ms(bm25, "postFilterLatencyMs", "  post filter");
    render_required_raw_ms(bm25, "prepareLatencyMs", "  prepare");
    render_required_raw_ms(bm25, "queryExecutionLatencyMs", "  query execution");
    render_required_raw_ms(bm25, "rowIterationLatencyMs", "  row iteration");
    render_required_raw_ms(bm25, "totalLatencyMs", "  total");
}

/// Read a required raw string diagnostic from a protocol diagnostics object.
fn required_raw_str<'a>(raw: &'a serde_json::Value, field: &str) -> &'a str {
    raw.get(field)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("search response BM25 diagnostics missing string {field}"))
}

/// Read a required raw boolean diagnostic from a protocol diagnostics object.
fn required_raw_bool(raw: &serde_json::Value, field: &str) -> bool {
    raw.get(field)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or_else(|| panic!("search response BM25 diagnostics missing boolean {field}"))
}

/// Print a required raw unsigned integer diagnostic.
fn render_required_raw_u64(raw: &serde_json::Value, field: &str, label: &str) {
    let value = raw
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_else(|| panic!("search response BM25 diagnostics missing integer {field}"));
    println!("{label}: {value}");
}

/// Print a required millisecond diagnostic using the same seconds format as benchmarks.
fn render_required_raw_ms(raw: &serde_json::Value, field: &str, label: &str) {
    let value = raw
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_else(|| panic!("search response BM25 diagnostics missing integer {field}"));
    println!(
        "{label}: {}",
        format_benchmark_duration(Duration::from_millis(value))
    );
}

/// Format benchmark durations as seconds with millisecond precision.
fn format_benchmark_duration(duration: Duration) -> String {
    format!("{:.3}s", duration.as_secs_f64())
}

impl StreamRenderer {
    /// Create a renderer that tracks whether the terminal cursor is on an overwritten progress line.
    fn new() -> Self {
        Self {
            active_line_chars: 0,
            docling_wait_line_active: false,
        }
    }

    /// Render one status event as the active in-place stage line.
    fn render_status(
        &mut self,
        operation_id: &str,
        sequence: u64,
        stage: Option<String>,
        message: Option<String>,
    ) -> Result<()> {
        self.finish_progress_line()?;
        let stage = stage.unwrap_or_else(|| "status".to_string());
        let line = match message {
            Some(message) => {
                format!("[{operation_id} #{sequence}] {stage}: {message}")
            }
            None => {
                format!("[{operation_id} #{sequence}] {stage}")
            }
        };
        self.docling_wait_line_active = false;
        self.render_active_line(&line)?;
        Ok(())
    }

    /// Render one progress event by updating the active in-place stage line.
    fn render_progress(
        &mut self,
        operation_id: &str,
        sequence: u64,
        stage: Option<String>,
        message: Option<String>,
        current: Option<u64>,
        total: Option<u64>,
    ) -> Result<()> {
        let stage = stage.unwrap_or_else(|| "progress".to_string());
        let message = message.unwrap_or_else(|| "working".to_string());
        if stage == "docling_converting"
            && message.starts_with("docling_converting: processing and generating markdown: ")
        {
            if !self.docling_wait_line_active {
                self.finish_progress_line()?;
                self.docling_wait_line_active = true;
            }
            self.render_active_line(&message)?;
            return Ok(());
        }
        let line = match (current, total) {
            (Some(current), Some(total)) => {
                let percent = current.saturating_mul(100).checked_div(total).unwrap_or(0);
                format!(
                    "[{operation_id} #{sequence}] {stage}: {message} {current}/{total} ({percent}%)"
                )
            }
            _ => format!("[{operation_id} #{sequence}] {stage}: {message}"),
        };
        self.docling_wait_line_active = false;
        self.render_active_line(&line)?;
        Ok(())
    }

    /// Write one complete terminal line in place without advancing to the next line.
    fn render_active_line(&mut self, line: &str) -> Result<()> {
        let line_chars = line.chars().count();
        let padding = " ".repeat(self.active_line_chars.saturating_sub(line_chars));
        print!("\r{line}{padding}");
        io::stdout()
            .flush()
            .context("failed to flush progress line")?;
        self.active_line_chars = line_chars;
        Ok(())
    }

    /// Finish an overwritten progress line before printing normal output.
    fn finish_progress_line(&mut self) -> Result<()> {
        if self.active_line_chars > 0 {
            println!();
            io::stdout()
                .flush()
                .context("failed to flush completed progress line")?;
            self.active_line_chars = 0;
        }
        Ok(())
    }
}

/// Print service readiness and component diagnostics in a compact form.
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
    }
}

/// Print configured request and retrieval limits as labeled values.
fn render_limits(response: LimitsResponse) {
    println!("Request limits:");
    println!(
        "  max request body bytes: {}",
        response.request.max_request_body_bytes
    );
    println!(
        "  max ingest source chars: {}",
        response.request.max_ingest_source_chars
    );
    println!(
        "  max search query chars: {}",
        response.request.max_search_query_chars
    );
    println!("Retrieval limits:");
    println!("  default topK: {}", response.retrieval.default_top_k);
    println!("  max topK: {}", response.retrieval.max_top_k);
}

/// Print the durable ingest result, then append server-authoritative ingest stage benchmarks.
fn render_ingest(response: IngestResponse) {
    println!("Status: {}", response.status);
    println!("Document ID: {}", response.document_id);
    println!("Version label: {}", response.version_label);
    println!("Units ingested: {}", response.units_ingested);
    render_operation_benchmarks(response.benchmarks);
}

/// Print ranked search results, then append the server-authoritative benchmark report when available.
fn render_search(response: SearchResponse, full_content: bool) {
    if response.results.is_empty() {
        println!("No results");
        render_operation_benchmarks(response.benchmarks);
        render_bm25_diagnostics(&response.raw);
        return;
    }
    for (index, result) in response.results.iter().enumerate() {
        println!();
        println!("{}. score {:.6}", index + 1, result.score);
        println!("   source: {}", result.source_path);
        println!("   unit: {}", result.unit_id);
        if !result.page_numbers.is_empty() {
            println!("   pages: {}", join_numbers(&result.page_numbers));
        }
        if !result.heading_path.is_empty() {
            println!("   headings: {}", result.heading_path.join(" > "));
        }
        println!("   content:");
        print_indented_content(&render_content(&result.content, full_content));
    }
    render_operation_benchmarks(response.benchmarks);
    render_bm25_diagnostics(&response.raw);
}

/// Print the server's benchmark tree after operation results so the result remains the first payload users inspect.
fn render_operation_benchmarks(benchmarks: Option<OperationBenchmarks>) {
    let Some(benchmarks) = benchmarks else {
        return;
    };
    println!();
    println!("Benchmarks:");
    for stage in &benchmarks.stages {
        render_benchmark_stage(stage, 0);
    }
    println!();
    println!(
        "Total: {}",
        format_benchmark_duration(Duration::from_millis(benchmarks.total_ms))
    );
}

/// Print one server benchmark stage and its children, indented two spaces per depth level.
fn render_benchmark_stage(stage: &BenchmarkStage, depth: usize) {
    let indent = "  ".repeat(depth);
    println!(
        "{indent}{}: {}",
        stage.stage,
        format_benchmark_duration(Duration::from_millis(stage.elapsed_ms))
    );
    for child in &stage.children {
        render_benchmark_stage(child, depth + 1);
    }
}

/// Print active ingested source documents without retained-version diagnostics.
fn render_sources(response: IngestedSourceListing) {
    if response.sources.is_empty() {
        println!("No ingested sources");
        return;
    }
    for source in response.sources {
        println!("Source: {}", source.source_path);
        println!("  active version: {}", source.active_version_label);
        println!("  document ID: {}", source.document_id);
        println!("  status: {}", source.status);
        println!("  units ingested: {}", source.units_ingested);
        println!("  createdAtMs: {}", source.created_at_ms);
        println!("  updatedAtMs: {}", source.updated_at_ms);
        println!();
    }
}

/// Print retained document versions grouped by source path.
fn render_versions(response: DocumentVersionListing) {
    if response.sources.is_empty() {
        println!("No document versions");
        return;
    }
    for source in response.sources {
        println!("Source: {}", source.source_path);
        match &source.active_version_label {
            Some(label) => println!("  active version: {label}"),
            None => println!("  active version: none"),
        }
        for version in source.versions {
            let marker = if version.is_active {
                "active"
            } else {
                "retained"
            };
            println!("  - {} ({marker})", version.version_label);
            println!("    document ID: {}", version.document_id);
            println!("    status: {}", version.status);
            println!("    units ingested: {}", version.units_ingested);
            println!("    createdAtMs: {}", version.created_at_ms);
            println!("    updatedAtMs: {}", version.updated_at_ms);
            for metadata in version.dense_vector_metadata {
                println!(
                    "    dense vectors: count {} dimension {}",
                    metadata.vector_count, metadata.model_dimension
                );
            }
            for metadata in version.colbert_vector_metadata {
                println!(
                    "    colbert vectors: count {} dimension {}",
                    metadata.vector_count, metadata.model_dimension
                );
            }
        }
        println!();
    }
}

/// Print the active-version state returned after rollback publish.
fn render_rollback(response: DocumentVersionRollbackResponse) {
    println!("Status: {}", response.status);
    println!("Source: {}", response.source_path);
    println!("Active version: {}", response.active_version_label);
    println!("Published at ms: {}", response.published_at_ms);
    println!("Vector count: {}", response.vector_count);
}

/// Print the server-authored shutdown completion confirmation or fail on an ambiguous result.
fn render_shutdown(response: ShutdownResponse) -> Result<()> {
    if response.status != SHUTDOWN_STATUS_COMPLETE {
        bail!(
            "shutdown command returned ambiguous status `{}`: {}",
            response.status,
            response.message
        );
    }

    println!("Status: {}", response.status);
    println!("{}", response.message);
    Ok(())
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

/// Join page numbers without exposing JSON formatting in normal output.
fn join_numbers(values: &[u32]) -> String {
    values
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Choose between full content and a fixed terminal-friendly excerpt.
fn render_content(content: &str, full_content: bool) -> String {
    if full_content {
        return content.to_string();
    }
    let mut chars = content.chars();
    let excerpt: String = chars.by_ref().take(SEARCH_EXCERPT_CHARS).collect();
    if chars.next().is_none() {
        return content.to_string();
    }

    format!("{excerpt}...")
}

/// Print multiline content beneath result metadata with stable indentation.
fn print_indented_content(content: &str) {
    for line in content.lines() {
        println!("     {line}");
    }
    if content.is_empty() {
        println!("     ");
    }
}
