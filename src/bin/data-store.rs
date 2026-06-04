use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::{
    StatusCode,
    blocking::{Client, RequestBuilder},
};
use rustyline::{DefaultEditor, error::ReadlineError};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

const PROMPT: &str = "data-store> ";
const DEFAULT_CONFIG_PATH: &str = "config.toml";
const HISTORY_FILE_NAME: &str = ".data-store.history";
const SEARCH_EXCERPT_CHARS: usize = 800;

#[derive(Debug, Deserialize)]
struct ClientConfig {
    server: ServerConfig,
    admin: AdminConfig,
}

#[derive(Debug, Deserialize)]
struct ServerConfig {
    bind_address: SocketAddr,
}

#[derive(Debug, Deserialize)]
struct AdminConfig {
    token_file_path: PathBuf,
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
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Debug, Deserialize)]
struct ErrorDetail {
    message: String,
}

/// Start the interactive client after resolving service config and local history.
fn main() -> Result<()> {
    let config_path = resolve_config_path()?;
    let config = load_config(&config_path)?;
    let context = ClientContext {
        base_url: format!("http://{}", config.server.bind_address),
        token_file_path: resolve_service_root_path(&config.admin.token_file_path),
        http: Client::new(),
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

    Ok(config)
}

/// Resolve service-relative local paths through Cargo's manifest root.
fn resolve_service_root_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }

    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
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
                    Err(source) => eprintln!("error: {source}"),
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
        Command::Health => render_health(public_get(context, "/v1/health")?),
        Command::Limits => render_limits(public_get(context, "/v1/limits")?),
        Command::Ingest { source } => {
            let request = IngestRequest { source };
            render_ingest(public_post_json(context, "/v1/ingest", &request)?);
        }
        Command::Search { query, top_k } => {
            let request = SearchRequest { query, top_k };
            render_search(public_post_json(context, "/v1/search", &request)?, false);
        }
        Command::SearchFull { query, top_k } => {
            let request = SearchRequest { query, top_k };
            render_search(public_post_json(context, "/v1/search", &request)?, true);
        }
        Command::Versions => render_versions(admin_get(context, "/admin/document-versions")?),
        Command::Rollback {
            source,
            version_label,
        } => {
            let request = DocumentVersionRollbackRequest {
                source,
                version_label,
            };
            render_rollback(admin_post_json(
                context,
                "/admin/document-versions/rollback",
                &request,
            )?);
        }
        Command::Shutdown => {
            if confirm_shutdown()? {
                render_shutdown(admin_post_empty(context, "/admin/shutdown")?);
            } else {
                println!("shutdown cancelled");
            }
        }
        Command::Help => render_help(),
        Command::Exit => return Ok(false),
    }

    Ok(true)
}

/// Send an unauthenticated GET request to a versioned public endpoint.
fn public_get<T>(context: &ClientContext, path: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    send_json(context.http.get(url(context, path)))
}

/// Send an unauthenticated JSON POST request to a versioned public endpoint.
fn public_post_json<T, B>(context: &ClientContext, path: &str, body: &B) -> Result<T>
where
    T: DeserializeOwned,
    B: Serialize,
{
    send_json(context.http.post(url(context, path)).json(body))
}

/// Send an authenticated GET request to a protected admin endpoint.
fn admin_get<T>(context: &ClientContext, path: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    let token = read_admin_token(context)?;
    send_json(context.http.get(url(context, path)).bearer_auth(token))
}

/// Send an authenticated JSON POST request to a protected admin endpoint.
fn admin_post_json<T, B>(context: &ClientContext, path: &str, body: &B) -> Result<T>
where
    T: DeserializeOwned,
    B: Serialize,
{
    let token = read_admin_token(context)?;
    send_json(
        context
            .http
            .post(url(context, path))
            .bearer_auth(token)
            .json(body),
    )
}

/// Send an authenticated empty POST request to a protected admin endpoint.
fn admin_post_empty<T>(context: &ClientContext, path: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    let token = read_admin_token(context)?;
    send_json(context.http.post(url(context, path)).bearer_auth(token))
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

/// Build a service URL from the configured base URL and a protocol path.
fn url(context: &ClientContext, path: &str) -> String {
    format!("{}{}", context.base_url, path)
}

/// Send one HTTP request and parse either a success body or a service error body.
fn send_json<T>(request: RequestBuilder) -> Result<T>
where
    T: DeserializeOwned,
{
    let response = request.send().context("failed to send HTTP request")?;
    let status = response.status();
    let text = response
        .text()
        .context("failed to read HTTP response body")?;
    if status.is_success() {
        return serde_json::from_str(&text).context("failed to parse service response");
    }

    Err(service_error(status, &text))
}

/// Convert a non-success HTTP response into an operator-facing error.
fn service_error(status: StatusCode, text: &str) -> anyhow::Error {
    if let Ok(body) = serde_json::from_str::<ErrorBody>(text) {
        return anyhow!("HTTP {}: {}", status.as_u16(), body.error.message);
    }
    if text.trim().is_empty() {
        return anyhow!("HTTP {} with empty response body", status.as_u16());
    }

    anyhow!("HTTP {}: {}", status.as_u16(), text.trim())
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

/// Print the accepted shutdown status.
fn render_shutdown(response: ShutdownResponse) {
    println!("Status: {}", response.status);
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

/// Require an explicit typed confirmation before stopping the service.
fn confirm_shutdown() -> Result<bool> {
    println!("Type `shutdown` to stop the service.");
    let mut editor = DefaultEditor::new().context("failed to initialize confirmation prompt")?;
    match editor.readline("confirm> ") {
        Ok(value) => Ok(value.trim() == "shutdown"),
        Err(ReadlineError::Interrupted | ReadlineError::Eof) => Ok(false),
        Err(source) => Err(source).context("failed to read shutdown confirmation"),
    }
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
