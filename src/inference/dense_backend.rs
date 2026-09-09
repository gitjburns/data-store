use std::{
    fmt, fs,
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use reqwest::{StatusCode, blocking::Client};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::{
    config::DenseModelConfig,
    error::ApiError,
    inference::{
        DenseEmbeddingRuntime, InferenceProgress,
        dense::{DENSE_SMOKE_TEXT, format_dense_passage_text, format_dense_query_text},
    },
    util::MAX_BACKOFF_MS,
};

/// Stable adapter-mode label carried in this client's boundary logs, mirroring
/// the reranker's `adapter_mode`. Event names use a `model_call.` prefix under
/// `model_role="dense"`, so an operator greps dense calls the same way across
/// the local and HTTP backends.
const HTTP_DENSE_MODE: &str = "http_openai_embeddings";
const HTTP_FAILURE_EXCERPT_CHARS: usize = 2048;

// Bounded 429-only retry policy for the dense embeddings endpoint.
//
// Engineering fact this encodes: the OpenRouter provider intermittently returns
// HTTP 429 with an upstream `engine_overloaded` body under transient load, and
// those overload windows have been observed clearing within seconds. A short
// bounded retry rides through that window instead of failing the whole query or
// projection build on a blip.
//
// Invariant: retry is transient-class ONLY. We retry on HTTP status 429 and
// nothing else — every other status and every transport failure keeps the
// module's fail-immediately policy byte-for-byte (no alternate endpoints, no
// model switching, no queuing). The bound is deliberate: at most
// `DENSE_HTTP_RETRY_LIMIT` retries (so `DENSE_HTTP_RETRY_LIMIT + 1` attempts
// total) with the fixed backoff schedule below, which keeps a genuinely
// hard-down provider failing within ~15s of backoff (2+4+8) plus per-attempt
// request timeouts rather than hanging indefinitely.
const DENSE_HTTP_RETRY_LIMIT: usize = 3;

// Fixed backoff slept BEFORE each retry attempt, indexed by prior-attempt count:
// 2s before attempt 2, 4s before attempt 3, 8s before attempt 4. Length equals
// `DENSE_HTTP_RETRY_LIMIT`; the loop only ever indexes it for a retry it is
// allowed to make.
const DENSE_HTTP_RETRY_BACKOFF: [Duration; DENSE_HTTP_RETRY_LIMIT] = [
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
];

/// Config-selected dense-embedding backend. Exactly one backend is active per
/// service instance and there is NO fallback between variants (approved design):
/// the local Candle runtime and the HTTP OpenAI-compatible client are mutually
/// exclusive. Enum dispatch (not a trait object) keeps the concrete embedding
/// signatures intact; boxing only the larger local runtime keeps this selector
/// cheap to move and clone.
#[derive(Debug, Clone)]
pub enum DenseEmbeddingBackend {
    Local(Box<DenseEmbeddingRuntime>),
    Http(HttpDenseClient),
}

impl DenseEmbeddingBackend {
    /// Wrap the already-loaded local Candle dense runtime as the active backend.
    pub fn local(runtime: DenseEmbeddingRuntime) -> Self {
        DenseEmbeddingBackend::Local(Box::new(runtime))
    }

    /// Build the HTTP dense backend and run the startup smoke round-trip through
    /// the configured endpoint (dimension/finite/normalization validation).
    pub fn load_http_with_progress(
        config: &DenseModelConfig,
        config_root: &Path,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        HttpDenseClient::load_with_progress(config, config_root, progress).map(Self::Http)
    }

    /// Return the stable backend-kind label used in health and log output.
    pub fn backend_kind(&self) -> &'static str {
        match self {
            DenseEmbeddingBackend::Local(_) => "local",
            DenseEmbeddingBackend::Http(_) => "http",
        }
    }

    /// Return whether embedding uses the local accelerator model-call gate. The
    /// caller-side gate sites acquire ONLY when this is true; the HTTP backend
    /// is network I/O and must never hold the exclusive gate across a request.
    pub fn uses_local_model_gate(&self) -> bool {
        match self {
            DenseEmbeddingBackend::Local(_) => true,
            DenseEmbeddingBackend::Http(_) => false,
        }
    }

    /// Return the backend kind plus backend readiness details for health diagnostics.
    pub fn health_details(&self) -> Vec<String> {
        let mut details = vec![format!("dense backend: {}", self.backend_kind())];
        match self {
            DenseEmbeddingBackend::Local(runtime) => details.extend(runtime.health_details()),
            DenseEmbeddingBackend::Http(client) => details.extend(client.health_details()),
        }
        details
    }

    /// Embed one retrieval query, returning its unit-norm dense vector. The
    /// query prompt shape is identical across backends (both build the final
    /// text through `format_dense_query_text`).
    ///
    /// Passage embedding deliberately has NO enum-level per-text method: the
    /// dense builder matches on the variant itself, because the two backends'
    /// passage surfaces differ by design — the Local runtime embeds one passage
    /// at a time (`DenseEmbeddingRuntime::embed_passage_vector`, CPa batch-1
    /// ruling) while the Http client only exposes the batched
    /// `embed_passage_vectors` window call.
    pub fn embed_query_vector(&self, text: &str) -> Result<Vec<f32>, ApiError> {
        match self {
            DenseEmbeddingBackend::Local(runtime) => runtime.embed_query_vector(text),
            DenseEmbeddingBackend::Http(client) => client.embed_query_vector(text),
        }
    }
}

/// Blocking OpenAI-compatible embeddings client. Holds the configured
/// endpoint/model and an optional bearer key loaded ONCE at build time; the key
/// is never logged or shown in Debug output. Synchronous `reqwest::blocking`
/// exactly like the reranker and annotator clients — no async is introduced.
#[derive(Clone)]
pub struct HttpDenseClient {
    client: Client,
    endpoint: String,
    model: String,
    timeout_seconds: u64,
    dimension: usize,
    api_key: Option<String>,
    api_key_file_path: Option<PathBuf>,
    smoke: HttpDenseSmoke,
}

impl fmt::Debug for HttpDenseClient {
    /// Format debug output without exposing the loaded API-key secret.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpDenseClient")
            .field("endpoint", &self.endpoint)
            .field("model", &self.model)
            .field("timeout_seconds", &self.timeout_seconds)
            .field("dimension", &self.dimension)
            .field("api_key_file_path", &self.api_key_file_path)
            .field("has_api_key", &self.api_key.is_some())
            .field("smoke", &self.smoke)
            .finish()
    }
}

/// Startup smoke result retained for health diagnostics: how many vectors the
/// smoke round-trip returned and the client-normalized norm of the first, which
/// is ~1.0 after the client's L2 normalization.
#[derive(Debug, Clone)]
struct HttpDenseSmoke {
    vector_count: usize,
    first_norm: f32,
}

/// OpenAI-compatible embeddings request body. Only `model` and `input` are sent;
/// the endpoint's own defaults govern everything else. `input` is the batch of
/// already-formatted texts (query prefix / passage identity applied upstream).
#[derive(Debug, Serialize)]
struct EmbeddingsRequest<'request> {
    model: &'request str,
    input: Vec<&'request str>,
}

/// Minimal typed view of the OpenAI embeddings response. `data` carries one
/// entry per input; `index` lets the client restore input order regardless of
/// how the provider ordered its `data` array.
#[derive(Debug, Deserialize)]
struct EmbeddingsResponse {
    data: Vec<EmbeddingData>,
    // Optional metadata never changes vector validation or response acceptance.
    usage: Option<serde_json::Value>,
}

/// One embedding entry: its input `index` and the raw `embedding` values.
#[derive(Debug, Deserialize)]
struct EmbeddingData {
    index: usize,
    embedding: Vec<f32>,
}

/// What one text contributes to the batched model-call log: the call purpose
/// and the input kind (query vs passage vs smoke). No text or vector values.
#[derive(Clone, Copy)]
struct DenseCallShape {
    call_purpose: &'static str,
    input_kind: &'static str,
}

/// Outcome of a single HTTP attempt against the embeddings endpoint. Only a
/// transient HTTP 429 (`Overloaded`) is distinguished from success so the retry
/// loop owner can decide whether to back off and retry; every other status and
/// every transport/body/parse failure is already turned into a terminal
/// `Err(ApiError)` inside `send_request_once` (fail-immediately, unchanged).
/// `Overloaded` carries the bounded body excerpt so the retry log can surface
/// the upstream `engine_overloaded` reason without re-reading the body.
enum AttemptOutcome {
    Parsed(EmbeddingsResponse),
    Overloaded { body_excerpt: String },
}

impl HttpDenseClient {
    /// Build the blocking client, load the optional bearer key, and verify the
    /// endpoint with a smoke round-trip. Readiness-critical (unlike the
    /// annotator): a dense embedding failure fails every query and every
    /// projection build, so a misconfigured or unreachable endpoint must fail
    /// startup here rather than surface later per-request.
    fn load_with_progress(
        config: &DenseModelConfig,
        config_root: &Path,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        let endpoint = config.http_endpoint()?.to_string();
        let model = config.http_model()?.to_string();
        let timeout_seconds = config.http_timeout_seconds()?;
        // `dimension` is common to both backends; the HTTP path validates every
        // returned vector's length against it per call (the server owns the
        // model, so a width mismatch is a config/deployment error).
        let dimension = config.dimension as usize;
        let api_key_file_path = config.resolved_http_api_key_file_path(config_root);
        let api_key = match api_key_file_path.as_ref() {
            Some(path) => Some(read_api_key(path)?),
            None => None,
        };
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_seconds))
            .build()
            .map_err(|source| ApiError::InferenceInit {
                message: format!("failed to build HTTP dense client: {source}"),
            })?;

        let mut backend = Self {
            client,
            endpoint,
            model,
            timeout_seconds,
            dimension,
            api_key,
            api_key_file_path,
            smoke: HttpDenseSmoke {
                vector_count: 0,
                first_norm: 0.0,
            },
        };

        progress("dense_http_smoke_embedding")?;
        backend.run_smoke_check()?;
        progress("dense_http_smoke_ready")?;

        Ok(backend)
    }

    /// Return HTTP backend readiness details without exposing credentials.
    fn health_details(&self) -> Vec<String> {
        vec![format!(
            "dense HTTP backend ready: mode {}, endpoint {}, model {}, dimension {}, timeout_seconds {}, api_key_file {}, smoke_vectors {}, smoke_norm {:.6}",
            HTTP_DENSE_MODE,
            self.endpoint,
            self.model,
            self.dimension,
            self.timeout_seconds,
            self.api_key_file_path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "absent".to_string()),
            self.smoke.vector_count,
            self.smoke.first_norm
        )]
    }

    /// Run the startup smoke round-trip: embed the shared smoke text as a
    /// passage through the remote endpoint and validate/normalize it exactly as
    /// a real passage call would, so readiness proves the full receive path
    /// (dimension, finiteness, nonzero norm) and not merely connectivity.
    fn run_smoke_check(&mut self) -> Result<(), ApiError> {
        let vectors = self.embed_batch(
            &[DENSE_SMOKE_TEXT],
            DenseCallShape {
                call_purpose: "startup_smoke_embedding",
                input_kind: "smoke",
            },
        )?;
        let first = vectors.first().ok_or_else(|| ApiError::InferenceInit {
            message: "HTTP dense smoke round-trip produced no vectors".to_string(),
        })?;
        self.smoke = HttpDenseSmoke {
            vector_count: vectors.len(),
            first_norm: l2_norm(first),
        };

        Ok(())
    }

    /// Embed one retrieval query, formatting the final text through the shared
    /// query-prompt boundary so the HTTP path sends byte-identical text to what
    /// the local runtime tokenizes. Single-text on both backends (query-time
    /// embedding is not batched).
    fn embed_query_vector(&self, text: &str) -> Result<Vec<f32>, ApiError> {
        let formatted = format_dense_query_text(text);
        let mut vectors = self.embed_batch(
            &[formatted.as_str()],
            DenseCallShape {
                call_purpose: "query_embedding",
                input_kind: "query",
            },
        )?;
        single_vector(vectors.drain(..), "query_embedding")
    }

    /// Embed a batch of retrieval passages in one request, preserving input
    /// order. This is the HTTP client's ONLY passage surface (there is no
    /// single-passage method; a one-chunk trailing window is simply a batch of
    /// one). Each passage is routed through the shared passage-formatting
    /// boundary; the returned vectors align 1:1 with `texts`. The builder packs
    /// its own `DENSE_HTTP_BATCH_SIZE`-sized windows, so this method embeds
    /// exactly the window it is handed.
    pub fn embed_passage_vectors(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, ApiError> {
        let formatted: Vec<&str> = texts
            .iter()
            .map(|text| format_dense_passage_text(text))
            .collect();
        self.embed_batch(
            &formatted,
            DenseCallShape {
                call_purpose: "batched_passage_embedding",
                input_kind: "passage",
            },
        )
    }

    /// Send one embeddings request for the already-formatted `texts`, log the
    /// model-call boundaries, restore input order by `index`, then validate and
    /// L2-normalize every returned vector.
    ///
    /// Normalization boundary: the local runtime returns unit-norm vectors, so
    /// the HTTP path normalizes client-side to preserve that invariant. This is
    /// idempotent when the server already normalizes, and a zero/degenerate
    /// vector is rejected here — never silently passed through.
    fn embed_batch(
        &self,
        texts: &[&str],
        shape: DenseCallShape,
    ) -> Result<Vec<Vec<f32>>, ApiError> {
        let context = crate::util::model_call_context("dense", shape.call_purpose);
        let _entered = context.enter();
        let started_at = Instant::now();
        let text_count = texts.len();
        let text_chars = texts.iter().map(|text| text.chars().count()).sum::<usize>();
        info!(
            event = "model_call.started",
            model_role = "dense",
            adapter_mode = HTTP_DENSE_MODE,
            call_purpose = shape.call_purpose,
            input_kind = shape.input_kind,
            endpoint = %self.endpoint,
            model = %self.model,
            timeout_seconds = self.timeout_seconds,
            text_count,
            text_chars,
            expected_dimension = self.dimension,
            "HTTP dense embedding started"
        );

        // Retries performed by `send_request` for this call (0 when the first
        // attempt succeeded or the request was empty). Surfaced in the terminal
        // completed/failed logs. The loop updates this before each attempt, so
        // terminal HTTP errors cannot incorrectly report zero retries.
        let mut retried_attempts = 0usize;
        let result = (|| -> Result<Vec<Vec<f32>>, ApiError> {
            if texts.is_empty() {
                return Ok(Vec::new());
            }
            let response = self.send_request(texts, shape, &mut retried_attempts)?;
            self.map_response(texts.len(), response, shape)
        })();

        match &result {
            Ok(vectors) => {
                info!(
                    event = "model_call.completed",
                    model_role = "dense",
                    adapter_mode = HTTP_DENSE_MODE,
                    call_purpose = shape.call_purpose,
                    input_kind = shape.input_kind,
                    endpoint = %self.endpoint,
                    model = %self.model,
                    text_count,
                    text_chars,
                    vector_count = vectors.len(),
                    expected_dimension = self.dimension,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    retried_attempts,
                    "HTTP dense embedding completed"
                );
            }
            Err(source) => {
                error!(
                    event = "model_call.failed",
                    model_role = "dense",
                    adapter_mode = HTTP_DENSE_MODE,
                    call_purpose = shape.call_purpose,
                    input_kind = shape.input_kind,
                    endpoint = %self.endpoint,
                    model = %self.model,
                    text_count,
                    text_chars,
                    expected_dimension = self.dimension,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    retried_attempts,
                    error = %source,
                    "HTTP dense embedding failed"
                );
            }
        }

        result
    }

    /// Retry loop owner for the embeddings request. Delegates each attempt to
    /// `send_request_once`; on a transient HTTP 429 (`AttemptOutcome::Overloaded`)
    /// it backs off per `DENSE_HTTP_RETRY_BACKOFF` and retries up to
    /// `DENSE_HTTP_RETRY_LIMIT` times. Retry is 429-ONLY: any other status and any
    /// transport/body/parse failure is already a terminal `Err` from the attempt
    /// helper and returns immediately (fail-immediately, byte-for-byte). When the
    /// bound is exhausted the last 429 becomes a terminal failure whose error
    /// message and durable failed log carry the attempt count.
    fn send_request(
        &self,
        texts: &[&str],
        shape: DenseCallShape,
        retried_attempts: &mut usize,
    ) -> Result<EmbeddingsResponse, ApiError> {
        // `attempt` is 1-based; attempt 1 is the initial request, attempts
        // 2..=DENSE_HTTP_RETRY_LIMIT+1 are retries each preceded by the fixed
        // backoff for the just-failed attempt. The output count is the number
        // of retries performed (`attempt - 1`, so 0 on a first-attempt success).
        let mut attempt: usize = 1;
        loop {
            *retried_attempts = attempt - 1;
            let context = crate::util::LogContext::new("http_attempt", &attempt.to_string());
            let _entered = context.enter();
            match self.send_request_once(texts, shape)? {
                AttemptOutcome::Parsed(response) => return Ok(response),
                AttemptOutcome::Overloaded { body_excerpt } => {
                    // `attempt - 1` prior 429s indexes the backoff schedule; once
                    // it reaches DENSE_HTTP_RETRY_LIMIT the bound is exhausted.
                    if attempt <= DENSE_HTTP_RETRY_LIMIT {
                        // Preserve shorter scheduled waits while enforcing the
                        // shared ceiling before both logging and sleeping.
                        let delay = DENSE_HTTP_RETRY_BACKOFF[attempt - 1]
                            .min(Duration::from_millis(MAX_BACKOFF_MS));
                        warn!(
                            event = "model_call.http_retry",
                            model_role = "dense",
                            call_purpose = shape.call_purpose,
                            endpoint = %self.endpoint,
                            attempt,
                            delay_ms = delay.as_millis() as u64,
                            response_body_excerpt = %body_excerpt,
                            "HTTP dense request overloaded (429); backing off before retry"
                        );
                        thread::sleep(delay);
                        attempt += 1;
                        continue;
                    }
                    // Bound exhausted: the provider stayed overloaded across every
                    // allowed attempt. Emit the terminal durable failed log (the
                    // 429 status path, matching the non-retry failure shape) and
                    // return the terminal error with the attempt count folded in.
                    let attempts = attempt;
                    error!(
                        event = "model_call.http_request.failed",
                        model_role = "dense",
                        adapter_mode = HTTP_DENSE_MODE,
                        call_purpose = shape.call_purpose,
                        endpoint = %self.endpoint,
                        model = %self.model,
                        text_count = texts.len(),
                        http_status = StatusCode::TOO_MANY_REQUESTS.as_u16(),
                        phase = "http_status",
                        retried_attempts = attempts - 1,
                        response_body_excerpt = %body_excerpt,
                        "HTTP dense request failed after 429 retry limit"
                    );
                    return Err(ApiError::InferenceInit {
                        message: format!(
                            "HTTP dense request to {} failed with status {} after {attempts} attempts (429 retry limit reached); body_excerpt={}",
                            self.endpoint,
                            StatusCode::TOO_MANY_REQUESTS.as_u16(),
                            body_excerpt
                        ),
                    });
                }
            }
        }
    }

    /// Send the OpenAI-compatible embeddings request and parse the provider
    /// response, mapping every transport/status/body failure into an inference
    /// error carrying the endpoint, status, and a bounded body excerpt (never
    /// the payload text or vectors).
    ///
    /// Returns `AttemptOutcome::Overloaded` (not an `Err`) for a transient HTTP
    /// 429 so the `send_request` loop owner can back off and retry; that is the
    /// ONLY status diverted from the fail-immediately path. Every other
    /// non-success status still logs the terminal failed boundary here and
    /// returns a terminal `Err` unchanged.
    fn send_request_once(
        &self,
        texts: &[&str],
        shape: DenseCallShape,
    ) -> Result<AttemptOutcome, ApiError> {
        let http_started = Instant::now();
        let request = EmbeddingsRequest {
            model: &self.model,
            input: texts.to_vec(),
        };
        let mut request_builder = self.client.post(&self.endpoint).json(&request);
        if let Some(api_key) = &self.api_key {
            request_builder = request_builder.bearer_auth(api_key);
        }

        info!(
            event = "model_call.http_request.started",
            model_role = "dense",
            adapter_mode = HTTP_DENSE_MODE,
            call_purpose = shape.call_purpose,
            endpoint = %self.endpoint,
            model = %self.model,
            text_count = texts.len(),
            "HTTP dense request started"
        );
        // Transport-level failure (DNS, connect, TLS, timeout) surfaces before
        // any HTTP status exists.
        let response = match request_builder.send() {
            Ok(response) => response,
            Err(source) => {
                let error_detail = crate::util::error_chain(&source);
                error!(
                    event = "model_call.http_request.failed",
                    model_role = "dense",
                    adapter_mode = HTTP_DENSE_MODE,
                    call_purpose = shape.call_purpose,
                    endpoint = %self.endpoint,
                    model = %self.model,
                    text_count = texts.len(),
                    phase = "send_request",
                    elapsed_ms = http_started.elapsed().as_millis() as u64,
                    error = %error_detail,
                    "HTTP dense request failed"
                );
                return Err(ApiError::InferenceInit {
                    message: format!(
                        "HTTP dense request to {} failed before response: {error_detail}",
                        self.endpoint
                    ),
                });
            }
        };
        let status = response.status();
        let body = match response.text() {
            Ok(body) => body,
            Err(source) => {
                let error_detail = crate::util::error_chain(&source);
                error!(
                    event = "model_call.http_request.failed",
                    model_role = "dense",
                    adapter_mode = HTTP_DENSE_MODE,
                    call_purpose = shape.call_purpose,
                    endpoint = %self.endpoint,
                    model = %self.model,
                    text_count = texts.len(),
                    http_status = status.as_u16(),
                    phase = "read_response_body",
                    elapsed_ms = http_started.elapsed().as_millis() as u64,
                    error = %error_detail,
                    "HTTP dense request failed"
                );
                return Err(ApiError::InferenceInit {
                    message: format!(
                        "HTTP dense response body from {} could not be read: {error_detail}",
                        self.endpoint
                    ),
                });
            }
        };
        // Transient-class divert: a 429 is handed back to the retry loop owner
        // instead of failing here. This is the ONLY status treated as
        // recoverable; it carries the bounded excerpt so the retry log can show
        // the upstream `engine_overloaded` reason. All other non-success statuses
        // fall through to the unchanged terminal failure path below.
        if status == StatusCode::TOO_MANY_REQUESTS {
            return Ok(AttemptOutcome::Overloaded {
                body_excerpt: bounded_excerpt(&body),
            });
        }
        if !status.is_success() {
            let body_excerpt = bounded_excerpt(&body);
            error!(
                event = "model_call.http_request.failed",
                model_role = "dense",
                adapter_mode = HTTP_DENSE_MODE,
                call_purpose = shape.call_purpose,
                endpoint = %self.endpoint,
                model = %self.model,
                text_count = texts.len(),
                http_status = status.as_u16(),
                phase = "http_status",
                response_body_excerpt = %body_excerpt,
                elapsed_ms = http_started.elapsed().as_millis() as u64,
                "HTTP dense request failed"
            );
            return Err(http_status_error(&self.endpoint, status, &body));
        }

        let parsed = match serde_json::from_str::<EmbeddingsResponse>(&body) {
            Ok(parsed) => parsed,
            Err(source) => {
                let body_excerpt = bounded_excerpt(&body);
                error!(
                    event = "model_call.http_request.failed",
                    model_role = "dense",
                    adapter_mode = HTTP_DENSE_MODE,
                    call_purpose = shape.call_purpose,
                    endpoint = %self.endpoint,
                    model = %self.model,
                    text_count = texts.len(),
                    http_status = status.as_u16(),
                    phase = "parse_response_body",
                    response_body_excerpt = %body_excerpt,
                    elapsed_ms = http_started.elapsed().as_millis() as u64,
                    error = %source,
                    "HTTP dense request failed"
                );
                return Err(ApiError::InferenceInit {
                    message: format!(
                        "HTTP dense response from {} could not be parsed: {source}; body_excerpt={}",
                        self.endpoint, body_excerpt
                    ),
                });
            }
        };
        info!(
            event = "model_call.http_request.completed",
            model_role = "dense",
            adapter_mode = HTTP_DENSE_MODE,
            call_purpose = shape.call_purpose,
            endpoint = %self.endpoint,
            model = %self.model,
            text_count = texts.len(),
            http_status = status.as_u16(),
            vector_count = parsed.data.len(),
            prompt_tokens = parsed.usage.as_ref().and_then(|usage| usage.get("prompt_tokens")).and_then(serde_json::Value::as_u64),
            total_tokens = parsed.usage.as_ref().and_then(|usage| usage.get("total_tokens")).and_then(serde_json::Value::as_u64),
            response_body_chars = body.chars().count(),
            elapsed_ms = http_started.elapsed().as_millis() as u64,
            result_state = "response_received_unvalidated",
            "HTTP dense response received; vector validation pending"
        );

        Ok(AttemptOutcome::Parsed(parsed))
    }

    /// Restore input order by `index`, then validate and L2-normalize every
    /// vector. Rejects a wrong count, an out-of-range or duplicate index, a
    /// dimension mismatch, a non-finite value, or a zero/degenerate norm — a bad
    /// vector fails the whole call rather than reaching persistence or scoring.
    fn map_response(
        &self,
        expected_count: usize,
        response: EmbeddingsResponse,
        shape: DenseCallShape,
    ) -> Result<Vec<Vec<f32>>, ApiError> {
        if response.data.len() != expected_count {
            return Err(dense_http_error(
                shape,
                format!(
                    "returned {} embeddings for {expected_count} inputs",
                    response.data.len()
                ),
            ));
        }

        // Place each embedding at its declared input index so the returned order
        // matches the input regardless of the provider's `data` ordering.
        let mut ordered: Vec<Option<Vec<f32>>> = (0..expected_count).map(|_| None).collect();
        for entry in response.data {
            let Some(slot) = ordered.get_mut(entry.index) else {
                return Err(dense_http_error(
                    shape,
                    format!(
                        "returned out-of-range embedding index {} for {expected_count} inputs",
                        entry.index
                    ),
                ));
            };
            if slot.is_some() {
                return Err(dense_http_error(
                    shape,
                    format!("returned duplicate embedding index {}", entry.index),
                ));
            }
            *slot = Some(entry.embedding);
        }

        let mut vectors = Vec::with_capacity(expected_count);
        for (index, slot) in ordered.into_iter().enumerate() {
            let mut vector = slot.ok_or_else(|| {
                dense_http_error(shape, format!("missing embedding for input index {index}"))
            })?;
            // Validate then normalize at the receive boundary: length against the
            // configured dimension, all values finite, and a finite nonzero norm
            // BEFORE normalizing (division by a zero norm would manufacture
            // non-finite values). This preserves the local runtime's unit-norm
            // invariant for every downstream consumer.
            if vector.len() != self.dimension {
                return Err(dense_http_error(
                    shape,
                    format!(
                        "embedding {index} has dimension {}, expected {}",
                        vector.len(),
                        self.dimension
                    ),
                ));
            }
            if vector.iter().any(|value| !value.is_finite()) {
                return Err(dense_http_error(
                    shape,
                    format!("embedding {index} contains a non-finite value"),
                ));
            }
            let norm = l2_norm(&vector);
            if !norm.is_finite() || norm <= 0.0 {
                return Err(dense_http_error(
                    shape,
                    format!("embedding {index} has invalid norm {norm}; expected finite nonzero"),
                ));
            }
            for value in vector.iter_mut() {
                *value /= norm;
            }
            vectors.push(vector);
        }

        Ok(vectors)
    }
}

/// Take exactly one vector out of a single-text batch result. A single-text
/// request must yield exactly one vector; anything else is a client/protocol
/// error surfaced with the call purpose rather than silently indexing.
fn single_vector(
    mut vectors: std::vec::Drain<'_, Vec<f32>>,
    call_purpose: &'static str,
) -> Result<Vec<f32>, ApiError> {
    let vector = vectors.next().ok_or_else(|| ApiError::InferenceInit {
        message: format!("HTTP dense {call_purpose} produced no vector"),
    })?;
    if vectors.next().is_some() {
        return Err(ApiError::InferenceInit {
            message: format!("HTTP dense {call_purpose} produced more than one vector"),
        });
    }
    Ok(vector)
}

/// Compute an f32 vector L2 norm (used for the normalization boundary and the
/// smoke readiness norm).
fn l2_norm(vector: &[f32]) -> f32 {
    vector.iter().map(|value| value * value).sum::<f32>().sqrt()
}

/// Build an inference error naming the dense HTTP call purpose/input kind and
/// the local failure reason, so a bad response is attributable without the
/// payload.
fn dense_http_error(shape: DenseCallShape, reason: String) -> ApiError {
    ApiError::InferenceInit {
        message: format!(
            "HTTP dense embedding response invalid: call_purpose={} input_kind={} reason={reason}",
            shape.call_purpose, shape.input_kind
        ),
    }
}

/// Read a provider API key from an optional owner-only file without logging the
/// secret. Mirrors the reranker/annotator key handling exactly (permission
/// check, trim, non-empty) under a `dense_http.` event prefix so an operator
/// greps dense key reads distinctly.
fn read_api_key(path: &Path) -> Result<String, ApiError> {
    info!(
        event = "dense_http.api_key_file.read_started",
        path = %path.display(),
        "HTTP dense API-key file read started"
    );
    if let Err(source) = validate_api_key_file_permissions(path) {
        error!(
            event = "dense_http.api_key_file.read_failed",
            path = %path.display(),
            phase = "validate_permissions",
            error = %source,
            "HTTP dense API-key file read failed"
        );
        return Err(source);
    }
    let raw = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(source) => {
            error!(
                event = "dense_http.api_key_file.read_failed",
                path = %path.display(),
                error = %source,
                "HTTP dense API-key file read failed"
            );
            return Err(ApiError::InferenceInit {
                message: format!(
                    "failed to read HTTP dense API-key file at {}: {source}",
                    path.display()
                ),
            });
        }
    };
    let key = raw.trim().to_string();
    if key.is_empty() {
        error!(
            event = "dense_http.api_key_file.read_failed",
            path = %path.display(),
            error = "empty_api_key_file",
            "HTTP dense API-key file read failed"
        );
        return Err(ApiError::InferenceInit {
            message: format!("HTTP dense API-key file at {} is empty", path.display()),
        });
    }
    info!(
        event = "dense_http.api_key_file.read_completed",
        path = %path.display(),
        "HTTP dense API-key file read completed"
    );

    Ok(key)
}

/// Enforce owner-only API-key file permissions on Unix platforms.
#[cfg(unix)]
fn validate_api_key_file_permissions(path: &Path) -> Result<(), ApiError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = fs::metadata(path).map_err(|source| ApiError::InferenceInit {
        message: format!(
            "failed to inspect HTTP dense API-key file at {}: {source}",
            path.display()
        ),
    })?;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(ApiError::InferenceInit {
            message: format!(
                "HTTP dense API-key file at {} must be owner-only",
                path.display()
            ),
        });
    }

    Ok(())
}

/// Skip Unix permission checks on platforms without Unix file modes.
#[cfg(not(unix))]
fn validate_api_key_file_permissions(_path: &Path) -> Result<(), ApiError> {
    Ok(())
}

/// Build an inference error for an unsuccessful HTTP dense status response.
fn http_status_error(endpoint: &str, status: StatusCode, body: &str) -> ApiError {
    ApiError::InferenceInit {
        message: format!(
            "HTTP dense request to {endpoint} failed with status {}; body_excerpt={}",
            status.as_u16(),
            bounded_excerpt(body)
        ),
    }
}

/// Return a bounded single-line excerpt for provider failure diagnostics.
fn bounded_excerpt(value: &str) -> String {
    let excerpt = value
        .chars()
        .flat_map(|character| character.escape_default())
        .take(HTTP_FAILURE_EXCERPT_CHARS)
        .collect::<String>();
    if value.chars().count() > HTTP_FAILURE_EXCERPT_CHARS {
        return format!("{excerpt}...");
    }

    excerpt
}
