//! CAb: shared OpenAI-compatible chat-completions client for the annotation
//! producers (entity, relation, summary), mirroring the HTTP-reranker
//! discipline (explicit failure, bounded diagnostics, no fallback).
//!
//! One external endpoint serves all three producers. There is no fallback
//! model or endpoint: a call failure becomes `ApiError::AnnotationProducer`,
//! and the annotation worker parks the affected annotation as `failed` for a
//! later retry pass. Maintenance cancellation is a separate outcome and does
//! not consume a failure retry. Prompt content and model output are external-language
//! payloads and never enter the service log (DIAGNOSTICS forbidden-data
//! rules); logs carry only compact boundary facts.

use std::{
    fmt, fs,
    future::{Future, poll_fn},
    path::{Path, PathBuf},
    pin::pin,
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};

use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::config::AnnotatorModelConfig;
use crate::error::ApiError;
use crate::maintenance::{AnnotationCancelReason, AnnotationCancellation};
use crate::util::truncate_diagnostic_text;

/// Stable adapter-mode label carried in this client's boundary logs, mirroring
/// the reranker's `adapter_mode` field. The event names use an
/// `annotator_http.` prefix so an operator can grep annotator calls distinctly
/// from reranker calls.
const ANNOTATOR_HTTP_MODE: &str = "openai_chat_completions";

/// Thinking and the final answer share the tested provider output allowance.
pub(crate) const MAX_COMPLETION_TOKENS: u64 = 150_000;

/// Producer identity and live requests share the tested reasoning-mode contract.
pub(crate) const ENABLE_THINKING: bool = true;

/// Deterministic-leaning BASE temperature: annotation producers request the
/// most reproducible output the endpoint will give, so identical inputs tend
/// to yield identical annotations and memo reuse stays meaningful. It is a
/// code constant, not config, because it is a producer-contract decision, not
/// an operator tuning knob. First attempts always run at this base; the
/// worker's retry-escalation ladder (user-ruled 2026-07-21) passes higher
/// per-call temperatures for failed-row retries via `complete`.
pub(crate) const PRODUCER_TEMPERATURE: f64 = 0.0;

/// Synchronous producer interface over cancellable HTTP I/O. The shared runtime
/// keeps pooled connections alive between calls; only dedicated annotation
/// threads may call `complete`. The bearer key is excluded from Debug output.
#[derive(Clone)]
pub(crate) struct AnnotatorClient {
    client: Client,
    // Clones share one reactor for the full client lifetime, including idle
    // pooled connections. Dropping the client before its runtime closes the pool.
    runtime: Arc<tokio::runtime::Runtime>,
    cancellation: AnnotationCancellation,
    endpoint: String,
    model: String,
    timeout_seconds: u64,
    max_input_chars: usize,
    api_key: Option<String>,
    api_key_file_path: Option<PathBuf>,
}

/// Cancellation must reach the worker without being counted as a provider failure.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CompletionFailure {
    #[error("annotator request cancelled: {0:?}")]
    Cancelled(AnnotationCancelReason),
    #[error(transparent)]
    Request(#[from] ApiError),
}

impl fmt::Debug for AnnotatorClient {
    /// Format debug output without exposing the loaded API-key secret.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnnotatorClient")
            .field("endpoint", &self.endpoint)
            .field("model", &self.model)
            .field("timeout_seconds", &self.timeout_seconds)
            .field("api_key_file_path", &self.api_key_file_path)
            .field("has_api_key", &self.api_key.is_some())
            .finish()
    }
}

/// The tested annotation contract enables thinking and constrains each stage's
/// final answer with its own schema. Streaming exposes activity before completion.
#[derive(Debug, Serialize)]
struct ChatCompletionRequest<'request> {
    model: &'request str,
    messages: Vec<ChatMessage<'request>>,
    temperature: f64,
    max_completion_tokens: u64,
    stream: bool,
    stream_options: StreamOptions,
    response_format: ResponseFormat<'request>,
    // vLLM's request-local template option leaves other endpoint clients alone.
    chat_template_kwargs: ChatTemplateKwargs,
}

/// Request provider measurements in the terminal streaming usage event.
#[derive(Debug, Serialize)]
struct StreamOptions {
    include_usage: bool,
}

/// Standard structured-output envelope; the stage owns the schema contents.
#[derive(Debug, Serialize)]
struct ResponseFormat<'request> {
    r#type: &'static str,
    json_schema: OutputSchema<'request>,
}

/// Borrow the producer's schema without copying external-language artifacts.
#[derive(Debug, Serialize)]
struct OutputSchema<'request> {
    name: &'static str,
    strict: bool,
    schema: &'request serde_json::Value,
}

/// vLLM template control for the tested thinking-enabled annotation requests.
#[derive(Debug, Serialize)]
struct ChatTemplateKwargs {
    enable_thinking: bool,
}

/// One chat message; `role` is `"system"` or `"user"` for this client.
#[derive(Debug, Serialize)]
struct ChatMessage<'request> {
    role: &'request str,
    content: &'request str,
}

/// Retain measurements outside the cancellable future, including partial work
/// on timeout. Reasoning text is counted and discarded, never stored or logged.
#[derive(Default)]
struct CompletedResponse {
    content: String,
    content_chars: usize,
    reasoning_chars: usize,
    received_bytes: usize,
    events: usize,
    response_id: Option<String>,
    usage: Option<CompletionUsage>,
    finish_reason: Option<String>,
    done: bool,
}

/// Optional provider measurements remain absent when the endpoint omits them.
#[derive(Debug, Deserialize)]
struct CompletionUsage {
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
    total_tokens: Option<u64>,
    completion_tokens_details: Option<CompletionTokenDetails>,
}

/// Reasoning tokens are measured by the provider, never inferred from characters.
#[derive(Debug, Deserialize)]
struct CompletionTokenDetails {
    reasoning_tokens: Option<u64>,
}

impl CompletedResponse {
    /// Decode complete SSE events only; raw external fields are inspected before
    /// conversion so malformed values cannot leak model text through serde errors.
    fn accept_event(&mut self, data: &[u8]) -> Result<(), String> {
        self.events += 1;
        if data == b"[DONE]" {
            self.done = true;
            return Ok(());
        }
        let event: serde_json::Value = serde_json::from_slice(data).map_err(|source| {
            format!(
                "stream event JSON decoding failed: {source}; event_bytes={}",
                data.len()
            )
        })?;
        if event.get("error").is_some_and(|error| !error.is_null()) {
            return Err(format!(
                "provider stream error: {}",
                provider_error_detail(&event)
            ));
        }
        if let Some(id) = optional_stream_text(&event, "id")? {
            // Response IDs and termination labels are protocol metadata, never
            // free-form model text. Validate before exposing them to logging.
            validate_protocol_label(id, "id")?;
            if self
                .response_id
                .as_deref()
                .is_some_and(|previous| previous != id)
            {
                return Err("stream response id changed between events".to_string());
            }
            if self.response_id.is_none() {
                self.response_id = Some(id.to_string());
            }
        }
        if let Some(usage) = event.get("usage").filter(|usage| !usage.is_null()) {
            self.usage = Some(CompletionUsage::deserialize(usage).map_err(|source| {
                // Type errors may contain offending strings; log only structural
                // source context, never that externally supplied value.
                format!(
                    "stream usage decoding failed: category={:?}, line={}, column={}",
                    source.classify(),
                    source.line(),
                    source.column()
                )
            })?);
        }
        let choices = event
            .get("choices")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "stream event requires a choices array".to_string())?;
        for choice in choices {
            if choice.get("index").and_then(serde_json::Value::as_u64) != Some(0) {
                return Err(
                    "stream returned an unexpected choice index; requested one completion"
                        .to_string(),
                );
            }
            let delta = choice
                .get("delta")
                .filter(|delta| delta.is_object())
                .ok_or_else(|| "stream choice requires a delta object".to_string())?;
            if let Some(content) = optional_stream_text(delta, "content")? {
                if self.finish_reason.is_some() && !content.is_empty() {
                    return Err("stream appended content after finishing its choice".to_string());
                }
                self.content_chars += content.chars().count();
                self.content.push_str(content);
            }
            // vLLM deployments expose reasoning through either field name. A
            // single delta must not supply both, which would double-count it.
            let reasoning = optional_stream_text(delta, "reasoning")?;
            let reasoning_content = optional_stream_text(delta, "reasoning_content")?;
            if reasoning.is_some() && reasoning_content.is_some() {
                return Err("stream delta supplied both reasoning fields".to_string());
            }
            if let Some(reasoning) = reasoning.or(reasoning_content) {
                self.reasoning_chars += reasoning.chars().count();
            }
            if let Some(reason) = optional_stream_text(choice, "finish_reason")? {
                validate_protocol_label(reason, "finish_reason")?;
                if self.finish_reason.is_some() {
                    return Err("stream finished its choice more than once".to_string());
                }
                self.finish_reason = Some(reason.to_string());
            }
        }
        Ok(())
    }

    /// Report measured activity and terminal facts without reasoning or answer
    /// payloads; the caller's model-call context supplies stage and call identity.
    fn log_measurements(&self, checkpoint: &str, started_at: Instant) {
        info!(
            event = "annotator_http.stream.measurements",
            checkpoint,
            received_bytes = self.received_bytes,
            stream_events = self.events,
            output_chars = self.content_chars,
            reasoning_chars = self.reasoning_chars,
            provider_response_id = self.response_id.as_deref(),
            prompt_tokens = self.usage.as_ref().and_then(|usage| usage.prompt_tokens),
            completion_tokens = self
                .usage
                .as_ref()
                .and_then(|usage| usage.completion_tokens),
            total_tokens = self.usage.as_ref().and_then(|usage| usage.total_tokens),
            reasoning_tokens = self
                .usage
                .as_ref()
                .and_then(|usage| usage.completion_tokens_details.as_ref())
                .and_then(|details| details.reasoning_tokens),
            finish_reason = self.finish_reason.as_deref(),
            terminal_event_received = self.done,
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "annotator stream measurements"
        );
    }
}

/// Validate optional external text without echoing a malformed value into errors.
fn optional_stream_text<'event>(
    event: &'event serde_json::Value,
    key: &str,
) -> Result<Option<&'event str>, String> {
    match event.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(text)) => Ok(Some(text)),
        Some(_) => Err(format!("stream field {key} must be a string or null")),
    }
}

/// Only compact protocol identifiers may enter diagnostics as external strings.
fn validate_protocol_label(value: &str, field: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-.:/".contains(&byte))
    {
        return Err(format!(
            "stream field {field} is not a compact protocol identifier"
        ));
    }
    Ok(())
}

/// Retain bounded provider rejection detail without dumping a successful model
/// response. Only standard error fields enter this diagnostic representation.
fn provider_error_detail(envelope: &serde_json::Value) -> String {
    let error = envelope.get("error").unwrap_or(envelope);
    let fields = ["type", "code", "message"]
        .into_iter()
        .filter_map(|key| {
            error
                .get(key)
                .filter(|value| !value.is_null())
                .map(|value| {
                    let text = match value.as_str() {
                        Some(text) => text.to_string(),
                        None => value.to_string(),
                    };
                    format!("{key}={}", bounded_error_text(&text))
                })
        })
        .collect::<Vec<_>>();
    if fields.is_empty() {
        "provider error envelope has no type, code, or message".to_string()
    } else {
        fields.join("; ")
    }
}

/// Bound diagnostic text before escaping it so error bodies cannot inflate logs.
fn bounded_error_text(value: &str) -> String {
    let bounded = truncate_diagnostic_text(value);
    let escaped = bounded
        .chars()
        .flat_map(char::escape_default)
        .collect::<String>();
    truncate_diagnostic_text(&escaped)
}

impl AnnotatorClient {
    /// Build the shared HTTP reactor and client, and load the optional bearer key.
    ///
    /// There is deliberately NO startup smoke request here, and this loader is
    /// NOT readiness-critical: annotations build post-activation on a dedicated
    /// worker and never block activation or readiness. An unreachable or
    /// misconfigured endpoint therefore must not fail startup — it simply
    /// parks each attempted annotation as `failed` at call time for a later
    /// retry pass. Only local setup (runtime/client build, key-file read) can
    /// fail here. The owner must release the final client on a synchronous thread.
    pub(crate) fn load(
        config: &AnnotatorModelConfig,
        config_root: &Path,
        cancellation: AnnotationCancellation,
    ) -> Result<Self, ApiError> {
        let endpoint = config.endpoint.trim().to_string();
        let model = config.model.trim().to_string();
        let timeout_seconds = config.timeout_seconds;
        let api_key_file_path = config.resolved_api_key_file_path(config_root);
        let api_key = match api_key_file_path.as_ref() {
            Some(path) => Some(read_api_key(path)?),
            None => None,
        };
        // Scoped producer threads share this runtime, so pooled HTTP connections
        // never outlive the reactor that created them. SQLite stays on the
        // synchronous caller; the single runtime worker services only HTTP I/O.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|source| ApiError::AnnotationProducer {
                message: format!(
                    "failed to build annotator HTTP runtime for endpoint {endpoint}: {source}"
                ),
            })?;
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_seconds))
            .build()
            .map_err(|source| ApiError::AnnotationProducer {
                message: format!(
                    "failed to build annotator HTTP client for endpoint {endpoint}: {source}"
                ),
            })?;

        info!(
            event = "annotator_http.client_loaded",
            adapter_mode = ANNOTATOR_HTTP_MODE,
            endpoint = %endpoint,
            model = %model,
            timeout_seconds,
            has_api_key = api_key.is_some(),
            "annotator chat-completions client loaded"
        );

        Ok(Self {
            client,
            runtime: Arc::new(runtime),
            cancellation,
            endpoint,
            model,
            timeout_seconds,
            max_input_chars: config.max_input_chars,
            api_key,
            api_key_file_path,
        })
    }

    /// Expose the same maintenance signal used by HTTP calls to stop worker dispatch.
    pub(crate) fn cancellation(&self) -> &AnnotationCancellation {
        &self.cancellation
    }

    /// Intermediate batches use the configured excerpt cap, so a short passage
    /// still has room for JSON syntax and relationship field names.
    pub(crate) fn max_input_chars(&self) -> usize {
        self.max_input_chars
    }

    /// Run one chat-completions call and return the first choice's message
    /// content verbatim. `request_purpose` identifies the producer/invocation
    /// for logs; `temperature` is the per-call sampling temperature (the base
    /// `PRODUCER_TEMPERATURE` for first attempts, retry-escalated values from
    /// the worker's ladder), logged and sent verbatim so the audit trail
    /// records what actually ran. Every request failure — transport error,
    /// timeout, non-2xx status, unreadable or malformed body, or a missing
    /// choice/content — is an `ApiError::AnnotationProducer` carrying
    /// endpoint/model/status and protocol-boundary context. Prompt text and model
    /// output are never logged. Maintenance cancellation drops the HTTP future
    /// and returns a distinct outcome so the worker does not count a failed attempt.
    pub(crate) fn complete(
        &self,
        request_purpose: &str,
        system_prompt: &str,
        user_content: &str,
        output_schema: &serde_json::Value,
        temperature: f64,
    ) -> Result<String, CompletionFailure> {
        let context = crate::util::model_call_context("annotator", request_purpose);
        let entered = context.enter();
        let started_at = Instant::now();
        // Char count is a safe compact shape fact; the content itself is a
        // forbidden log payload.
        let input_chars = system_prompt.chars().count() + user_content.chars().count();
        // Snapshot the client's own receiver before polling HTTP. Comparing this
        // with the drain event distinguishes observed cancellation from intent;
        // this record does not assert that a request reached the remote server.
        let annotation_cancel_reason = self.cancellation.reason();
        info!(
            event = "annotator_http.call.started",
            annotation_cancel_requested = annotation_cancel_reason.is_some(),
            annotation_cancel_reason = ?annotation_cancel_reason.map(|reason| reason.label()),
            adapter_mode = ANNOTATOR_HTTP_MODE,
            request_purpose,
            endpoint = %self.endpoint,
            model = %self.model,
            timeout_seconds = self.timeout_seconds,
            temperature,
            input_chars,
            max_completion_tokens = MAX_COMPLETION_TOKENS,
            enable_thinking = ENABLE_THINKING,
            structured_output = true,
            streaming = true,
            "annotator chat-completions call started"
        );
        // Async polling enters the span only while running, rather than retaining
        // a thread-local guard through network waits.
        drop(entered);

        let mut response = CompletedResponse::default();
        let result = {
            let mut cancellation = self.cancellation.clone();
            let mut cancelled = pin!(cancellation.cancelled());
            let mut request = pin!(self.send_and_parse(
                system_prompt,
                user_content,
                output_schema,
                temperature,
                &mut response,
            ));
            // Cancellation wins a simultaneous response and covers both headers
            // and body reads. Leaving this scope drops the in-flight future;
            // vLLM may observe the disconnect, but remote completion is unknown.
            self.runtime.block_on(context.instrument(poll_fn(|cx| {
                if let Poll::Ready(reason) = cancelled.as_mut().poll(cx) {
                    return Poll::Ready(Err(CompletionFailure::Cancelled(reason)));
                }
                match request.as_mut().poll(cx) {
                    Poll::Ready(response) => Poll::Ready(match self.cancellation.reason() {
                        Some(reason) => Err(CompletionFailure::Cancelled(reason)),
                        None => response.map_err(CompletionFailure::Request),
                    }),
                    Poll::Pending => Poll::Pending,
                }
            })))
        };

        let _entered = context.enter();
        // This measurement record survives cancellation and decoding/transport
        // failures; unavailable usage stays absent rather than being estimated.
        response.log_measurements("terminal", started_at);
        match &result {
            Ok(()) => {
                info!(
                    event = "annotator_http.call.completed",
                    adapter_mode = ANNOTATOR_HTTP_MODE,
                    request_purpose,
                    endpoint = %self.endpoint,
                    model = %self.model,
                    temperature,
                    input_chars,
                    // Output length only; content is a forbidden log payload.
                    output_chars = response.content_chars,
                    provider_response_id = response.response_id.as_deref(),
                    prompt_tokens = response.usage.as_ref().and_then(|usage| usage.prompt_tokens),
                    completion_tokens = response.usage.as_ref().and_then(|usage| usage.completion_tokens),
                    total_tokens = response.usage.as_ref().and_then(|usage| usage.total_tokens),
                    finish_reason = response.finish_reason.as_deref(),
                    annotation_state = "awaiting_validation",
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "annotator response received; annotation validation pending"
                );
            }
            Err(CompletionFailure::Cancelled(reason)) => {
                info!(
                    event = "annotator_http.call.cancelled",
                    adapter_mode = ANNOTATOR_HTTP_MODE,
                    request_purpose,
                    endpoint = %self.endpoint,
                    model = %self.model,
                    temperature,
                    input_chars,
                    reason = reason.label(),
                    remote_outcome = "unknown",
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "annotator request cancelled locally; remote inference outcome unknown"
                );
            }
            Err(CompletionFailure::Request(source)) => {
                error!(
                    event = "annotator_http.call.failed",
                    adapter_mode = ANNOTATOR_HTTP_MODE,
                    request_purpose,
                    endpoint = %self.endpoint,
                    model = %self.model,
                    temperature,
                    input_chars,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    error = %source,
                    "annotator chat-completions call failed"
                );
            }
        }

        result.map(|()| response.content)
    }

    /// Build the request body, send it, and map every failure mode into a
    /// producer error. Split out from `complete` so the boundary logging in
    /// `complete` wraps exactly one fallible unit of work. `temperature` is
    /// the caller's per-call sampling temperature (see `complete`). Only network
    /// waits suspend; response decoding remains synchronous on the producer thread.
    async fn send_and_parse(
        &self,
        system_prompt: &str,
        user_content: &str,
        output_schema: &serde_json::Value,
        temperature: f64,
        completed: &mut CompletedResponse,
    ) -> Result<(), ApiError> {
        let request = ChatCompletionRequest {
            model: &self.model,
            messages: vec![
                ChatMessage {
                    role: "system",
                    content: system_prompt,
                },
                ChatMessage {
                    role: "user",
                    content: user_content,
                },
            ],
            temperature,
            max_completion_tokens: MAX_COMPLETION_TOKENS,
            stream: true,
            stream_options: StreamOptions {
                include_usage: true,
            },
            response_format: ResponseFormat {
                r#type: "json_schema",
                json_schema: OutputSchema {
                    name: "annotation_stage",
                    strict: true,
                    schema: output_schema,
                },
            },
            chat_template_kwargs: ChatTemplateKwargs {
                enable_thinking: ENABLE_THINKING,
            },
        };
        let mut request_builder = self.client.post(&self.endpoint).json(&request);
        if let Some(api_key) = &self.api_key {
            request_builder = request_builder.bearer_auth(api_key);
        }

        // Transport-level failure (DNS, connect, TLS, timeout) surfaces before
        // any HTTP status exists.
        let started_at = Instant::now();
        let mut response = request_builder.send().await.map_err(|source| {
            self.call_error(
                None,
                &format!(
                    "request failed before response: {}",
                    crate::util::error_chain(&source)
                ),
            )
        })?;
        let status = response.status();
        if !status.is_success() {
            let body = response.bytes().await.map_err(|source| {
                self.call_error(
                    Some(status),
                    &format!(
                        "provider rejection body read failed: {}",
                        crate::util::error_chain(&source)
                    ),
                )
            })?;
            completed.received_bytes += body.len();
            // Rejection messages are operational diagnostics. Successful stream
            // parse failures never use this body-excerpt path.
            let detail = match serde_json::from_slice::<serde_json::Value>(&body) {
                Ok(envelope) => provider_error_detail(&envelope),
                Err(source) => format!(
                    "error-body JSON decoding failed: {source}; body_excerpt={}",
                    bounded_error_text(&String::from_utf8_lossy(&body))
                ),
            };
            return Err(self.call_error(
                Some(status),
                &format!("non-success HTTP response; {detail}"),
            ));
        }
        info!(
            event = "annotator_http.response.started",
            status = status.as_u16(),
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "annotator streaming response headers received"
        );
        let mut pending = Vec::new();
        let mut event = Vec::new();
        let mut progress_at = Instant::now();
        while let Some(chunk) = response.chunk().await.map_err(|source| {
            self.call_error(
                Some(status),
                &format!(
                    "stream body read failed: {}",
                    crate::util::error_chain(&source)
                ),
            )
        })? {
            completed.received_bytes += chunk.len();
            pending.extend_from_slice(&chunk);
            // Buffer bytes until a full line arrives: UTF-8 characters and SSE
            // frames may both be divided across arbitrary HTTP chunks.
            let mut consumed = 0;
            while let Some(offset) = pending[consumed..].iter().position(|byte| *byte == b'\n') {
                let end = consumed + offset;
                let line = pending[consumed..end]
                    .strip_suffix(b"\r")
                    .unwrap_or(&pending[consumed..end]);
                if line.is_empty() {
                    if !event.is_empty() {
                        completed
                            .accept_event(&event)
                            .map_err(|detail| self.call_error(Some(status), &detail))?;
                        event.clear();
                    }
                } else if let Some(data) = line.strip_prefix(b"data:") {
                    let data = data.strip_prefix(b" ").unwrap_or(data);
                    if !event.is_empty() {
                        event.push(b'\n');
                    }
                    event.extend_from_slice(data);
                }
                consumed = end + 1;
                if completed.done {
                    break;
                }
            }
            pending.drain(..consumed);
            if completed.done {
                break;
            }
            if progress_at.elapsed() >= Duration::from_secs(10) {
                completed.log_measurements("progress", started_at);
                progress_at = Instant::now();
            }
        }
        // Neither EOF nor a token-limit finish is a complete annotation. Require
        // explicit protocol termination before exposing content to stage parsers.
        if !completed.done {
            return Err(self.call_error(Some(status), "stream ended without terminal [DONE] event"));
        }
        if completed.finish_reason.as_deref() != Some("stop") {
            return Err(self.call_error(
                Some(status),
                &format!(
                    "stream ended with finish_reason={}; expected stop",
                    completed.finish_reason.as_deref().unwrap_or("missing"),
                ),
            ));
        }
        if completed.content.trim().is_empty() {
            return Err(self.call_error(
                Some(status),
                "stream completed without final assistant content",
            ));
        }
        Ok(())
    }

    /// Construct a producer error carrying the endpoint/model/status context an
    /// operator needs to attribute the failure, without ever including prompt
    /// or output text. Provider rejection messages remain bounded diagnostics.
    fn call_error(&self, status: Option<StatusCode>, detail: &str) -> ApiError {
        let status_text = match status {
            Some(status) => status.as_u16().to_string(),
            None => "none".to_string(),
        };
        ApiError::AnnotationProducer {
            message: format!(
                "annotator call to {} for model {} failed (status {status_text}): {detail}",
                self.endpoint, self.model
            ),
        }
    }
}

/// Read a bearer API key from an optional owner-only file without logging the
/// secret. Reimplemented locally (rather than shared from the inference
/// reranker, whose copy is module-private) to mirror its permission
/// validation, trim, and non-empty behavior exactly, under this module's
/// `annotator_http.` event prefix.
fn read_api_key(path: &Path) -> Result<String, ApiError> {
    info!(
        event = "annotator_http.api_key_file.read_started",
        path = %path.display(),
        "annotator API-key file read started"
    );
    if let Err(source) = validate_api_key_file_permissions(path) {
        error!(
            event = "annotator_http.api_key_file.read_failed",
            path = %path.display(),
            phase = "validate_permissions",
            error = %source,
            "annotator API-key file read failed"
        );
        return Err(source);
    }
    let raw = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(source) => {
            error!(
                event = "annotator_http.api_key_file.read_failed",
                path = %path.display(),
                error = %source,
                "annotator API-key file read failed"
            );
            return Err(ApiError::AnnotationProducer {
                message: format!(
                    "failed to read annotator API-key file at {}: {source}",
                    path.display()
                ),
            });
        }
    };
    let key = raw.trim().to_string();
    if key.is_empty() {
        error!(
            event = "annotator_http.api_key_file.read_failed",
            path = %path.display(),
            error = "empty_api_key_file",
            "annotator API-key file read failed"
        );
        return Err(ApiError::AnnotationProducer {
            message: format!("annotator API-key file at {} is empty", path.display()),
        });
    }
    info!(
        event = "annotator_http.api_key_file.read_completed",
        path = %path.display(),
        "annotator API-key file read completed"
    );

    Ok(key)
}

/// Enforce owner-only API-key file permissions on Unix platforms.
#[cfg(unix)]
fn validate_api_key_file_permissions(path: &Path) -> Result<(), ApiError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = fs::metadata(path).map_err(|source| ApiError::AnnotationProducer {
        message: format!(
            "failed to inspect annotator API-key file at {}: {source}",
            path.display()
        ),
    })?;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(ApiError::AnnotationProducer {
            message: format!(
                "annotator API-key file at {} must be owner-only",
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
