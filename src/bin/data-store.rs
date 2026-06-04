use std::{
    env, fs,
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
const SHUTDOWN_STATUS_COMPLETE: &str = "shutdown_complete";

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
    Ingest {
        source: String,
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
    #[serde(rename = "latencyMs")]
    latency_ms: u64,
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

struct StreamRenderer {
    active_line: bool,
}

/// Start the interactive client after resolving service config and local history.
fn main() -> Result<()> {
    let config_path = resolve_config_path()?;
    let config = load_config(&config_path)?;
    let context = ClientContext {
        base_url: base_url_for_bind_address(config.server.bind_address),
        token_file_path: resolve_service_root_path(&config.admin.token_file_path),
        http: build_http_client(&config.client)?,
    };
    run_repl(context)
}

/// Resolve the optional `--config` argument without accepting unrelated startup flags.
fn resolve_config_path() -> Result<PathBuf> {
    let mut args = env::args().skip(1);
    let mut config_path = PathBuf::from(DEFAULT_CONFIG_PATH);
    while let Some(arg) = args.next() {
        if arg != "--config" {
            bail!("unknown argument: {arg}");
        }
        let Some(value) = args.next() else {
            bail!("--config requires a path");
        };
        config_path = PathBuf::from(value);
    }

    Ok(config_path)
}

/// Load the subset of service config needed by the interactive client.
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
    match args[0].as_str() {
        "health" => {
            require_arg_count(&args, 1, "health")?;
            Ok(Command::Health)
        }
        "limits" => {
            require_arg_count(&args, 1, "limits")?;
            Ok(Command::Limits)
        }
        "ingest" => {
            require_arg_count(&args, 2, "ingest <source>")?;
            Ok(Command::Ingest {
                source: args[1].clone(),
            })
        }
        "search" => parse_search_command(&args, false),
        "search-full" => parse_search_command(&args, true),
        "versions" => {
            require_arg_count(&args, 1, "versions")?;
            Ok(Command::Versions)
        }
        "rollback" => {
            require_arg_count(&args, 3, "rollback <source> <versionLabel>")?;
            Ok(Command::Rollback {
                source: args[1].clone(),
                version_label: args[2].clone(),
            })
        }
        "shutdown" => {
            require_arg_count(&args, 1, "shutdown")?;
            Ok(Command::Shutdown)
        }
        "help" => {
            require_arg_count(&args, 1, "help")?;
            Ok(Command::Help)
        }
        "exit" | "quit" => {
            require_arg_count(&args, 1, "exit")?;
            Ok(Command::Exit)
        }
        command => bail!("unknown command `{command}`; type `help` for commands"),
    }
}

/// Parse search commands that share HTTP behavior but differ in rendering.
fn parse_search_command(args: &[String], full_content: bool) -> Result<Command> {
    if args.len() != 2 && args.len() != 3 {
        bail!("usage: {} <query> [topK]", args[0]);
    }
    let top_k = match args.get(2) {
        Some(value) => Some(
            value
                .parse::<u32>()
                .with_context(|| format!("topK must be an integer, got `{value}`"))?,
        ),
        None => None,
    };
    if full_content {
        return Ok(Command::SearchFull {
            query: args[1].clone(),
            top_k,
        });
    }

    Ok(Command::Search {
        query: args[1].clone(),
        top_k,
    })
}

/// Enforce exact command arity with a usage-oriented error.
fn require_arg_count(args: &[String], expected: usize, usage: &str) -> Result<()> {
    if args.len() == expected {
        return Ok(());
    }

    bail!("usage: {usage}");
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
        Command::Ingest { source } => {
            let request = IngestRequest { source };
            render_ingest(send_operation(
                context,
                "ingest",
                serde_json::to_value(request)
                    .context("failed to encode ingest operation payload")?,
                false,
            )?);
        }
        Command::Search { query, top_k } => {
            let request = SearchRequest { query, top_k };
            render_search(
                send_operation(
                    context,
                    "search",
                    serde_json::to_value(request)
                        .context("failed to encode search operation payload")?,
                    false,
                )?,
                false,
            );
        }
        Command::SearchFull { query, top_k } => {
            let request = SearchRequest { query, top_k };
            render_search(
                send_operation(
                    context,
                    "search",
                    serde_json::to_value(request)
                        .context("failed to encode search operation payload")?,
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

    let payload = match read_operation_stream(response, operation, method, &target_url) {
        Ok(payload) => payload,
        Err(source) => {
            println!("Elapsed: {} ms", started.elapsed().as_millis());
            return Err(source);
        }
    };
    println!("Elapsed: {} ms", started.elapsed().as_millis());
    serde_json::from_value(payload)
        .with_context(|| format!("failed to parse {operation} result payload"))
}

/// Read streamed operation events until the service emits a terminal result or error.
fn read_operation_stream(
    response: reqwest::blocking::Response,
    operation: &str,
    method: &str,
    target_url: &str,
) -> Result<serde_json::Value> {
    let mut renderer = StreamRenderer::new();
    let mut reader = BufReader::new(response);
    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = reader
            .read_line(&mut line)
            .with_context(|| format!("{method} {target_url} failed while reading NDJSON stream"))?;
        if bytes_read == 0 {
            renderer.finish_progress_line()?;
            bail!(
                "{method} {target_url} operation `{operation}` stream ended before a terminal event"
            );
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        let event: OperationEvent = serde_json::from_str(trimmed).with_context(|| {
            format!("{method} {target_url} operation `{operation}` returned invalid NDJSON")
        })?;
        match event {
            OperationEvent::Status {
                operation_id,
                sequence,
                stage,
                message,
            } => renderer.render_status(&operation_id, sequence, stage, message)?,
            OperationEvent::Progress {
                operation_id,
                sequence,
                stage,
                message,
                current,
                total,
            } => {
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

/// Build a service URL from the configured base URL and a protocol path.
fn url(context: &ClientContext, path: &str) -> String {
    format!("{}{}", context.base_url, path)
}

/// Convert a non-success HTTP response into an operator-facing error.
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

/// Convert a terminal operation error event into a complete operator-facing error.
fn operation_error(
    operation: &str,
    operation_id: &str,
    sequence: u64,
    stage: Option<&str>,
    error: &ErrorDetail,
) -> anyhow::Error {
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

impl StreamRenderer {
    /// Create a renderer that tracks whether the terminal cursor is on an overwritten progress line.
    fn new() -> Self {
        Self { active_line: false }
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
        let line = match (current, total) {
            (Some(current), Some(total)) => {
                let percent = if total > 0 {
                    (current.saturating_mul(100)) / total
                } else {
                    0
                };
                format!(
                    "[{operation_id} #{sequence}] {stage}: {message} {current}/{total} ({percent}%)"
                )
            }
            _ => {
                format!("[{operation_id} #{sequence}] {stage}: {message}")
            }
        };
        self.render_active_line(&line)?;
        Ok(())
    }

    /// Write one complete terminal line in place without advancing to the next line.
    fn render_active_line(&mut self, line: &str) -> Result<()> {
        print!("\r\x1b[2K{line}");
        io::stdout()
            .flush()
            .context("failed to flush progress line")?;
        self.active_line = true;
        Ok(())
    }

    /// Finish an overwritten progress line before printing normal output.
    fn finish_progress_line(&mut self) -> Result<()> {
        if self.active_line {
            println!();
            io::stdout()
                .flush()
                .context("failed to flush completed progress line")?;
            self.active_line = false;
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

/// Print the durable ingest result returned after storage and cache publish.
fn render_ingest(response: IngestResponse) {
    println!("Status: {}", response.status);
    println!("Document ID: {}", response.document_id);
    println!("Version label: {}", response.version_label);
    println!("Units ingested: {}", response.units_ingested);
}

/// Print ranked search results with either excerpts or full matched content.
fn render_search(response: SearchResponse, full_content: bool) {
    println!("Latency: {} ms", response.latency_ms);
    if response.results.is_empty() {
        println!("No results");
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
    println!("  health");
    println!("  limits");
    println!("  ingest <source>");
    println!("  search <query> [topK]");
    println!("  search-full <query> [topK]");
    println!("  versions");
    println!("  rollback <source> <versionLabel>");
    println!("  shutdown");
    println!("  help");
    println!("  exit");
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
