//! Serve mode: the client-hosted, read-only web UI (SPEC-web-ui.md §1–§3).
//!
//! Two responsibilities only:
//!   * serve the three embedded static assets, and
//!   * proxy a fixed allowlist of `/api/*` routes to the service, returning the
//!     service's status code and body verbatim.
//!
//! The allowlist IS the read-only guarantee: there is no generic passthrough, so
//! no mutating service route is reachable through this proxy. Nothing here
//! reshapes, narrows, or summarizes a service response — presentation belongs to
//! the browser.
//!
//! Async is confined to this module. The rest of the client is blocking, and the
//! transport helpers below use blocking `reqwest`, so every one of them runs
//! inside `spawn_blocking` — blocking reqwest must never run on a runtime thread.

use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context as _, Result};
use axum::{
    Router,
    extract::{Path, RawQuery, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};

use super::{ClientContext, ClientRequestDiagnostic, read_admin_token, url};

// Assets are embedded at compile time (SPEC-web-ui.md §3): the serve binary must
// not depend on a working directory or an installed asset tree.
const INDEX_HTML: &str = include_str!("../../../assets/web/index.html");
const APP_JS: &str = include_str!("../../../assets/web/app.js");
const STYLE_CSS: &str = include_str!("../../../assets/web/style.css");

const CONTENT_TYPE_HTML: &str = "text/html; charset=utf-8";
const CONTENT_TYPE_JS: &str = "text/javascript; charset=utf-8";
const CONTENT_TYPE_CSS: &str = "text/css; charset=utf-8";
const CONTENT_TYPE_JSON: &str = "application/json";

/// Run serve mode in the foreground until the process is terminated.
///
/// Owns the client context for the process lifetime and shares it with every
/// handler through an `Arc`. There is no signal handling of its own (the `tokio`
/// dependency carries no `signal` feature): Ctrl-C terminates the process.
pub fn run(context: ClientContext, bind_addr: SocketAddr) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build the serve-mode tokio runtime")?;
    runtime.block_on(serve_web_ui(Arc::new(context), bind_addr))
}

/// Bind the requested address and serve the web UI router until the server stops.
/// The bound URL is printed to stdout as the operator's readiness signal, and a
/// non-loopback bind is called out first (`warn_if_exposed`).
async fn serve_web_ui(context: Arc<ClientContext>, bind_addr: SocketAddr) -> Result<()> {
    let app = build_router(context);
    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .with_context(|| format!("failed to bind the web UI on {bind_addr}"))?;
    warn_if_exposed(bind_addr);
    println!("data-store web UI listening on http://{bind_addr}/");
    axum::serve(listener, app)
        .await
        .context("the web UI server stopped with a transport error")
}

/// Warn on stderr when the web UI is bound anywhere other than loopback.
///
/// The UI carries no authentication of its own (SPEC-web-ui.md preamble), yet the
/// bearer routes of the §2 allowlist sign their upstream hop with the operator's
/// admin token. A non-loopback bind therefore hands the admin read surface
/// (held parses, operation records, full annotation vocabulary) to every client
/// that can reach the socket. `--serve <host>:<port>` accepts such an address by
/// spec, so this is a warning rather than a refusal; an unspecified address
/// (`0.0.0.0`, `::`) is non-loopback and is warned about too.
fn warn_if_exposed(bind_addr: SocketAddr) {
    if bind_addr.ip().is_loopback() {
        return;
    }
    eprintln!(
        "warning: the web UI is bound to {bind_addr}, which is not loopback. It has no \
         authentication of its own, and /api/held-parses, /api/operations/{{id}}, and \
         /api/vocabulary are served with this client's admin token — anyone who can reach \
         {bind_addr} gets that admin read access. Bind 127.0.0.1 unless the socket is \
         protected by other means."
    );
}

/// Build the serve-mode router: three static asset routes plus the fixed `/api/*`
/// proxy allowlist of SPEC-web-ui.md §2. Adding a route here widens the read-only
/// surface, so the table must stay in step with that spec section.
fn build_router(context: Arc<ClientContext>) -> Router {
    Router::new()
        .route("/", get(serve_index))
        .route("/app.js", get(serve_app_js))
        .route("/style.css", get(serve_style_css))
        // Public proxy routes (no bearer).
        .route("/api/query", post(proxy_query))
        .route("/api/health", get(proxy_health))
        .route("/api/units/{unitId}", get(proxy_unit))
        .route(
            "/api/units/{unitId}/relationships",
            get(proxy_unit_relationships),
        )
        .route("/api/sources/{sourceId}", get(proxy_source))
        .route("/api/sync-status", get(proxy_sync_status))
        // Bearer proxy routes: the admin token file is read fresh per request by
        // the protected transport helper (auth freshness, SPEC-CLIENT.md §1.5).
        .route("/api/held-parses", get(proxy_held_parses))
        .route("/api/operations/{operationId}", get(proxy_operation))
        .route("/api/vocabulary", get(proxy_vocabulary))
        .with_state(context)
}

/// Serve the embedded `index.html` shell.
async fn serve_index() -> Response {
    asset_response(CONTENT_TYPE_HTML, INDEX_HTML)
}

/// Serve the embedded application script.
async fn serve_app_js() -> Response {
    asset_response(CONTENT_TYPE_JS, APP_JS)
}

/// Serve the embedded stylesheet.
async fn serve_style_css() -> Response {
    asset_response(CONTENT_TYPE_CSS, STYLE_CSS)
}

/// Wrap an embedded asset in a 200 response with its declared content type.
fn asset_response(content_type: &'static str, body: &'static str) -> Response {
    ([(header::CONTENT_TYPE, content_type)], body).into_response()
}

/// Proxy `POST /api/query` to the service's `POST /query`, forwarding the full
/// request envelope. The body is parsed only to confirm it is JSON before it is
/// sent; the envelope's fields are never inspected or reshaped here.
async fn proxy_query(State(context): State<Arc<ClientContext>>, body: String) -> Response {
    let request_body = match serde_json::from_str::<serde_json::Value>(&body) {
        Ok(value) => value,
        Err(source) => {
            // A malformed body never reached the service, so this is the proxy's
            // own error rather than a passthrough of a service status.
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("POST /api/query requires a JSON request body: {source}"),
            );
        }
    };
    run_passthrough("POST", "/api/query".to_string(), move || {
        raw_post_public_json(&context, "/query", &request_body)
    })
    .await
}

/// Proxy `GET /api/health` to the service's `GET /v1/health`.
async fn proxy_health(State(context): State<Arc<ClientContext>>) -> Response {
    run_passthrough("GET", "/api/health".to_string(), move || {
        raw_get_public(&context, "/v1/health")
    })
    .await
}

/// Proxy `GET /api/units/{unitId}` to the service's `GET /units/{unitId}`.
async fn proxy_unit(
    State(context): State<Arc<ClientContext>>,
    Path(unit_id): Path<String>,
) -> Response {
    let service_path = format!("/units/{}", encode_path_segment(&unit_id));
    run_passthrough("GET", format!("/api/units/{unit_id}"), move || {
        raw_get_public(&context, &service_path)
    })
    .await
}

/// Proxy `GET /api/units/{unitId}/relationships`, forwarding the raw query string
/// (`direction`, `relationshipType`) so filter semantics stay the service's.
async fn proxy_unit_relationships(
    State(context): State<Arc<ClientContext>>,
    Path(unit_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    let service_path = with_query(
        &format!("/units/{}/relationships", encode_path_segment(&unit_id)),
        query.as_deref(),
    );
    run_passthrough(
        "GET",
        format!("/api/units/{unit_id}/relationships"),
        move || raw_get_public(&context, &service_path),
    )
    .await
}

/// Proxy `GET /api/sources/{sourceId}` to the service's `GET /sources/{sourceId}`.
async fn proxy_source(
    State(context): State<Arc<ClientContext>>,
    Path(source_id): Path<String>,
) -> Response {
    let service_path = format!("/sources/{}", encode_path_segment(&source_id));
    run_passthrough("GET", format!("/api/sources/{source_id}"), move || {
        raw_get_public(&context, &service_path)
    })
    .await
}

/// Proxy `GET /api/sync-status` to the service's `GET /sync/status`.
async fn proxy_sync_status(State(context): State<Arc<ClientContext>>) -> Response {
    run_passthrough("GET", "/api/sync-status".to_string(), move || {
        raw_get_public(&context, "/sync/status")
    })
    .await
}

/// Proxy `GET /api/held-parses` to the service's `GET /parses?status=held`
/// (bearer). The `status=held` filter is fixed by the allowlist, not forwarded.
async fn proxy_held_parses(State(context): State<Arc<ClientContext>>) -> Response {
    run_passthrough("GET", "/api/held-parses".to_string(), move || {
        raw_get_protected(&context, "/parses?status=held")
    })
    .await
}

/// Proxy `GET /api/operations/{operationId}` to the service's
/// `GET /operations/{operationId}` (bearer).
async fn proxy_operation(
    State(context): State<Arc<ClientContext>>,
    Path(operation_id): Path<String>,
) -> Response {
    let service_path = format!("/operations/{}", encode_path_segment(&operation_id));
    run_passthrough(
        "GET",
        format!("/api/operations/{operation_id}"),
        move || raw_get_protected(&context, &service_path),
    )
    .await
}

/// Proxy `GET /api/vocabulary` to the service's `GET /annotations/vocabulary`
/// (bearer), forwarding the raw query string (`annotationType`, `scope`).
async fn proxy_vocabulary(
    State(context): State<Arc<ClientContext>>,
    RawQuery(query): RawQuery,
) -> Response {
    let service_path = with_query("/annotations/vocabulary", query.as_deref());
    run_passthrough("GET", "/api/vocabulary".to_string(), move || {
        raw_get_protected(&context, &service_path)
    })
    .await
}

/// Run one proxied hop on a blocking thread and turn its outcome into the browser
/// response.
///
/// Two invariants live here: the upstream hop always runs under `spawn_blocking`
/// (blocking reqwest must never occupy a runtime thread), and a successful hop is
/// returned verbatim — the service's status code and body, unmodified. Only a
/// transport failure (or a panicked/cancelled blocking task) becomes a proxy-owned
/// 502 naming the hop that failed. Every request logs one line with the upstream
/// status, or the failure.
async fn run_passthrough<F>(method: &'static str, proxy_path: String, hop: F) -> Response
where
    F: FnOnce() -> Result<(StatusCode, String)> + Send + 'static,
{
    let diagnostic = ClientRequestDiagnostic::new();
    // The browser receives the upstream body unchanged; local correlation belongs
    // to console diagnostics, and elapsed time includes blocking-pool waiting.
    match tokio::task::spawn_blocking(hop).await {
        Ok(Ok((status, body))) => {
            println!(
                "serve proxy {method} {proxy_path} -> {} local_request_id={} elapsed_ms={} outcome=upstream_response",
                status.as_u16(),
                diagnostic.id,
                diagnostic.started.elapsed().as_millis()
            );
            (status, [(header::CONTENT_TYPE, CONTENT_TYPE_JSON)], body).into_response()
        }
        Ok(Err(source)) => {
            let message = format!("{method} {proxy_path} failed upstream: {source:#}");
            println!(
                "serve proxy {method} {proxy_path} -> 502 ({source:#}) local_request_id={} elapsed_ms={} stage=upstream_hop",
                diagnostic.id,
                diagnostic.started.elapsed().as_millis()
            );
            error_response(StatusCode::BAD_GATEWAY, "proxy_transport_failure", &message)
        }
        Err(source) => {
            let message =
                format!("{method} {proxy_path} upstream request did not complete: {source}");
            println!(
                "serve proxy {method} {proxy_path} -> 502 ({source}) local_request_id={} elapsed_ms={} stage=blocking_join outcome=unknown",
                diagnostic.id,
                diagnostic.started.elapsed().as_millis()
            );
            error_response(StatusCode::BAD_GATEWAY, "proxy_transport_failure", &message)
        }
    }
}

/// Build a proxy-owned JSON error body in the service's `{error:{...}}` envelope
/// shape so the browser has exactly one error shape to render. Serialization goes
/// through `serde_json::Value::to_string`, which cannot fail, keeping this path
/// free of fallible formatting.
fn error_response(status: StatusCode, kind: &str, message: &str) -> Response {
    let body = serde_json::json!({
        "error": { "status": status.as_u16(), "kind": kind, "message": message }
    })
    .to_string();
    (status, [(header::CONTENT_TYPE, CONTENT_TYPE_JSON)], body).into_response()
}

/// Perform a public GET and return the service's status and body verbatim.
///
/// This is deliberately NOT `super::get_public`: that helper decodes through
/// `decode_success`, which turns a non-2xx into an `Err` and loses the original
/// status. The proxy must pass a service error envelope through unchanged, so
/// only a transport failure may become an `Err` here.
fn raw_get_public(context: &ClientContext, path: &str) -> Result<(StatusCode, String)> {
    let target_url = url(context, path);
    let response = context
        .http
        .get(&target_url)
        .send()
        .with_context(|| format!("GET {target_url} failed to send HTTP request"))?;
    read_raw_response(response, "GET", &target_url)
}

/// Perform a protected GET and return the service's status and body verbatim.
/// The admin token is read immediately before sending, never cached (auth
/// freshness). A missing or empty token file fails here as a transport error
/// before any request is sent.
fn raw_get_protected(context: &ClientContext, path: &str) -> Result<(StatusCode, String)> {
    let target_url = url(context, path);
    let response = context
        .http
        .get(&target_url)
        .bearer_auth(read_admin_token(context)?)
        .send()
        .with_context(|| format!("GET {target_url} failed to send HTTP request"))?;
    read_raw_response(response, "GET", &target_url)
}

/// Perform a public POST with a JSON body and return the service's status and
/// body verbatim. The body is forwarded as received.
fn raw_post_public_json(
    context: &ClientContext,
    path: &str,
    body: &serde_json::Value,
) -> Result<(StatusCode, String)> {
    let target_url = url(context, path);
    let response = context
        .http
        .post(&target_url)
        .json(body)
        .send()
        .with_context(|| format!("POST {target_url} failed to send HTTP request"))?;
    read_raw_response(response, "POST", &target_url)
}

/// Read a service response into its status and raw body text without inspecting
/// either. Only a failure to read the body is an error; any HTTP status, success
/// or not, is a valid passthrough result.
fn read_raw_response(
    response: reqwest::blocking::Response,
    method: &str,
    target_url: &str,
) -> Result<(StatusCode, String)> {
    let status = response.status();
    let body = response
        .text()
        .with_context(|| format!("{method} {target_url} failed to read HTTP response body"))?;
    Ok((status, body))
}

/// Append a raw query string to a service path, verbatim and unparsed. Filter
/// semantics belong to the service, so the proxy neither validates nor re-encodes
/// the parameters it forwards. An empty query string is dropped rather than
/// producing a trailing `?`.
fn with_query(path: &str, query: Option<&str>) -> String {
    match query {
        Some(query) if !query.is_empty() => format!("{path}?{query}"),
        _ => path.to_string(),
    }
}

/// Percent-encode one path segment for the upstream URL.
///
/// Axum's `Path` extractor hands over an already percent-decoded id, so an id
/// containing `/`, `?`, `#`, or a space would otherwise change the meaning of the
/// upstream path. Re-encoding everything outside the RFC 3986 unreserved set
/// restores the segment exactly as the service should see it.
fn encode_path_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}
