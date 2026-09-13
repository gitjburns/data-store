//! CAb: shared OpenAI-compatible chat-completions client for the annotation
//! producers (entity, relation, summary), mirroring the HTTP-reranker
//! discipline (explicit failure, bounded diagnostics, no fallback).
//!
//! One external endpoint serves all three producers. There is no fallback
//! model or endpoint: a call failure becomes `ApiError::AnnotationProducer`,
//! and the annotation worker parks the affected annotation as `failed` for a
//! later retry pass. Maintenance cancellation is a separate outcome and does
//! not consume a failure retry. Selected exchange fields go to the annotator
//! transcript; the service log retains compact boundary facts.

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

use super::transcript::{Transcript, TranscriptCall};
use crate::config::AnnotatorModelConfig;
use crate::error::ApiError;
use crate::limits::DiagnosticLimits;
use crate::maintenance::{AnnotationCancelReason, AnnotationCancellation};
use crate::monitoring::{CallGuard, WorkHandle};
use crate::monitoring_types::TokenUsage;
use crate::types::AnnotationProgressCount;
use crate::util::{LogContext, truncate_diagnostic_text};

/// Stable adapter-mode label carried in this client's boundary logs, mirroring
/// the reranker's `adapter_mode` field. The event names use an
/// `annotator_http.` prefix so an operator can grep annotator calls distinctly
/// from reranker calls.
const ANNOTATOR_HTTP_MODE: &str = "openai_chat_completions";

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
    // Thinking and final output share this configured provider allowance.
    max_completion_tokens: u64,
    diagnostics: DiagnosticLimits,
    api_key: Option<String>,
    api_key_file_path: Option<PathBuf>,
    // Producer clones must share one writer lock for indivisible transcript blocks.
    transcript: Arc<Transcript>,
    // The owning source supplies identity; pooled client clones never infer it
    // from thread-local tracing or retain identities across unrelated documents.
    monitor: Option<WorkHandle>,
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
/// final answer with its own schema. Internal calls return one complete response.
#[derive(Debug, Serialize)]
struct ChatCompletionRequest<'request> {
    model: &'request str,
    messages: Vec<ChatMessage<'request>>,
    temperature: f64,
    max_completion_tokens: u64,
    stream: bool,
    response_format: ResponseFormat<'request>,
    // vLLM's request-local template option leaves other endpoint clients alone.
    chat_template_kwargs: ChatTemplateKwargs,
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

/// Retain the complete response before decoding so selected transcript fields
/// remain available on protocol failure. Cancellation may leave no body available.
#[derive(Default)]
struct CompletedResponse {
    content: String,
    body: Vec<u8>,
    body_received: bool,
    content_chars: usize,
    reasoning_chars: usize,
    response_id: Option<String>,
    usage: Option<CompletionUsage>,
    finish_reason: Option<String>,
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
    /// Decode one completed chat response. Retain its original body separately;
    /// protocol errors expose compact context to the service log, not payloads.
    fn accept_body(&mut self, diagnostics: &DiagnosticLimits) -> Result<(), String> {
        let event: serde_json::Value = serde_json::from_slice(&self.body)
            .map_err(|source| format!("response JSON decoding failed: {source}"))?;
        if event.get("error").is_some_and(|error| !error.is_null()) {
            return Err(format!(
                "provider response error: {}",
                provider_error_detail(&event, diagnostics)
            ));
        }
        if let Some(id) = optional_response_text(&event, "id")? {
            // Response IDs and termination labels are protocol metadata, never
            // free-form model text. Validate before exposing them to logging.
            validate_protocol_label(id, "id")?;
            self.response_id = Some(id.to_string());
        }
        if let Some(usage) = event.get("usage").filter(|usage| !usage.is_null()) {
            self.usage = Some(CompletionUsage::deserialize(usage).map_err(|source| {
                // Type errors may contain offending strings; log only structural
                // source context, never that externally supplied value.
                format!(
                    "response usage decoding failed: category={:?}, line={}, column={}",
                    source.classify(),
                    source.line(),
                    source.column()
                )
            })?);
        }
        let choices = event
            .get("choices")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "response requires a choices array".to_string())?;
        let [choice] = choices.as_slice() else {
            return Err("response must contain exactly one completion choice".to_string());
        };
        if choice.get("index").and_then(serde_json::Value::as_u64) != Some(0) {
            return Err("response returned an unexpected choice index; expected zero".to_string());
        }
        let message = choice
            .get("message")
            .filter(|message| message.is_object())
            .ok_or_else(|| "response choice requires a message object".to_string())?;
        if let Some(content) = optional_response_text(message, "content")? {
            self.content_chars = content.chars().count();
            self.content = content.to_string();
        }
        // Providers expose reasoning through either name; counting both would
        // misrepresent measured output. Supplying both remains a protocol error.
        let reasoning = optional_response_text(message, "reasoning")?;
        let reasoning_content = optional_response_text(message, "reasoning_content")?;
        if reasoning.is_some() && reasoning_content.is_some() {
            return Err("response message supplied both reasoning fields".to_string());
        }
        if let Some(reasoning) = reasoning.or(reasoning_content) {
            self.reasoning_chars = reasoning.chars().count();
        }
        if let Some(reason) = optional_response_text(choice, "finish_reason")? {
            validate_protocol_label(reason, "finish_reason")?;
            self.finish_reason = Some(reason.to_string());
        }
        Ok(())
    }

    /// Report completed-response measurements without reasoning or answer
    /// payloads; a failed whole-body receive leaves the byte count unknown.
    /// The caller's model-call context supplies stage and call identity.
    fn log_measurements(&self, started_at: Instant) {
        info!(
            event = "annotator_http.response.measurements",
            response_body_received = self.body_received,
            received_bytes = self.body_received.then_some(self.body.len()),
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
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "annotator response measurements"
        );
    }
}

/// Validate optional external text without echoing a malformed value into errors.
fn optional_response_text<'event>(
    event: &'event serde_json::Value,
    key: &str,
) -> Result<Option<&'event str>, String> {
    match event.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(text)) => Ok(Some(text)),
        Some(_) => Err(format!("response field {key} must be a string or null")),
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
            "response field {field} is not a compact protocol identifier"
        ));
    }
    Ok(())
}

/// Retain bounded provider rejection detail without dumping a successful model
/// response. Only standard error fields enter this diagnostic representation.
fn provider_error_detail(envelope: &serde_json::Value, diagnostics: &DiagnosticLimits) -> String {
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
                    format!("{key}={}", bounded_error_text(&text, diagnostics))
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
fn bounded_error_text(value: &str, diagnostics: &DiagnosticLimits) -> String {
    let bounded = truncate_diagnostic_text(value, diagnostics);
    let escaped = bounded
        .chars()
        .flat_map(char::escape_default)
        .collect::<String>();
    truncate_diagnostic_text(&escaped, diagnostics)
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
        diagnostics: DiagnosticLimits,
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
            max_completion_tokens: config.max_completion_tokens,
            diagnostics,
            api_key,
            api_key_file_path,
            transcript: Arc::new(Transcript::open(config_root)),
            monitor: None,
        })
    }

    /// Scope shared HTTP resources to one source's observer without changing the
    /// producer configuration or the memo identity derived from that configuration.
    pub(crate) fn with_monitor(&self, monitor: WorkHandle) -> Self {
        let mut client = self.clone();
        client.monitor = Some(monitor);
        client
    }

    /// Observe actual submission through the stage's terminal validation result;
    /// the stage owns the guard so response receipt cannot count twice.
    pub(crate) fn monitor_call(&self, stage: &str) -> Option<CallGuard> {
        self.monitor.as_ref().map(|monitor| {
            monitor.call(
                "annotator",
                &self.model,
                stage,
                Some(self.timeout_seconds.saturating_mul(1_000)),
            )
        })
    }

    /// Give the chain ownership of its transcript buffer and tracing context
    /// until output validation supplies the terminal result.
    pub(crate) fn start_call(
        &self,
        stage: &'static str,
        progress: Option<AnnotationProgressCount>,
    ) -> (TranscriptCall<'_>, LogContext) {
        self.transcript.call(stage, progress)
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
    /// content verbatim. `call` identifies the producer stage and transcript
    /// for logs; `temperature` is the per-call sampling temperature (the base
    /// `PRODUCER_TEMPERATURE` for first attempts, retry-escalated values from
    /// the worker's ladder), logged and sent verbatim so the audit trail
    /// records what actually ran. Every request failure — transport error,
    /// timeout, non-2xx status, unreadable or malformed body, or a missing
    /// choice/content — is an `ApiError::AnnotationProducer` carrying
    /// endpoint/model/status and protocol-boundary context. Selected payloads go only
    /// to the annotator transcript. Maintenance cancellation drops the HTTP future
    /// and returns a distinct outcome so the worker does not count a failed attempt.
    /// Reported usage accompanies both success and failure, independently of the
    /// result, so subsequent structural validation cannot discard that measurement.
    pub(crate) fn complete(
        &self,
        call: &mut TranscriptCall<'_>,
        context: &LogContext,
        system_prompt: &str,
        user_content: &str,
        output_schema: &serde_json::Value,
        temperature: f64,
    ) -> (Result<String, CompletionFailure>, TokenUsage) {
        let request_purpose = call.stage;
        let entered = context.enter();
        let started_at = Instant::now();
        // Keep service-log fields compact; the separate transcript owns payloads.
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
            max_completion_tokens = self.max_completion_tokens,
            enable_thinking = ENABLE_THINKING,
            structured_output = true,
            streaming = false,
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
                call,
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
        response.log_measurements(started_at);
        // Metadata survives request failure and cancellation when a response was
        // received. Reasoning remains a subset of completion, never extra tokens.
        let monitor_usage = response
            .usage
            .as_ref()
            .map_or_else(TokenUsage::default, |usage| TokenUsage {
                prompt: usage.prompt_tokens,
                completion: usage.completion_tokens,
                reasoning: usage
                    .completion_tokens_details
                    .as_ref()
                    .and_then(|details| details.reasoning_tokens),
                total: usage.total_tokens,
            });
        // Transcript field selection is presentation-only. Protocol validation and
        // the terminal failure reason remain authoritative in the caller's RESULT.
        call.response(response.body_received.then_some(response.body.as_slice()));
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
                    // Full output is retained in the separate transcript.
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

        (result.map(|()| response.content), monitor_usage)
    }

    /// Build the request body, send it, and map every failure mode into a
    /// producer error. Split out from `complete` so the boundary logging in
    /// `complete` wraps exactly one fallible unit of work. `temperature` is
    /// the caller's per-call sampling temperature (see `complete`). Only network
    /// waits suspend; response decoding remains synchronous on the producer thread.
    async fn send_and_parse(
        &self,
        call: &mut TranscriptCall<'_>,
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
            max_completion_tokens: self.max_completion_tokens,
            stream: false,
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
        // Capture the actual request shape before credentials are attached.
        call.request(&self.endpoint, &request);
        let mut request_builder = self.client.post(&self.endpoint).json(&request);
        if let Some(api_key) = &self.api_key {
            request_builder = request_builder.bearer_auth(api_key);
        }

        // Transport-level failure (DNS, connect, TLS, timeout) surfaces before
        // any HTTP status exists.
        let started_at = Instant::now();
        let response = request_builder.send().await.map_err(|source| {
            self.call_error(
                None,
                &format!(
                    "request failed before response: {}",
                    crate::util::error_chain(&source, &self.diagnostics)
                ),
            )
        })?;
        let status = response.status();
        // The existing cancellation race covers the complete send/body wait.
        // Publish bytes before validation so even rejected responses are readable.
        completed.body = response
            .bytes()
            .await
            .map_err(|source| {
                self.call_error(
                    Some(status),
                    &format!(
                        "response body read failed: {}",
                        crate::util::error_chain(&source, &self.diagnostics)
                    ),
                )
            })?
            .to_vec();
        completed.body_received = true;
        if !status.is_success() {
            let body = &completed.body;
            // Preserve provider rejection details in the bounded terminal error;
            // the transcript displays selected response fields and that RESULT.
            let detail = match serde_json::from_slice::<serde_json::Value>(body) {
                Ok(envelope) => provider_error_detail(&envelope, &self.diagnostics),
                Err(source) => format!(
                    "error-body JSON decoding failed: {source}; body_excerpt={}",
                    bounded_error_text(&String::from_utf8_lossy(body), &self.diagnostics)
                ),
            };
            return Err(self.call_error(
                Some(status),
                &format!("non-success HTTP response; {detail}"),
            ));
        }
        info!(
            event = "annotator_http.response.received",
            status = status.as_u16(),
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "annotator response body received"
        );
        completed
            .accept_body(&self.diagnostics)
            .map_err(|detail| self.call_error(Some(status), &detail))?;
        // A token-limit finish is still an execution failure. Only a complete,
        // nonempty answer proceeds to the stage's structural parser.
        if completed.finish_reason.as_deref() != Some("stop") {
            return Err(self.call_error(
                Some(status),
                &format!(
                    "response ended with finish_reason={}; expected stop",
                    completed.finish_reason.as_deref().unwrap_or("missing"),
                ),
            ));
        }
        if completed.content.trim().is_empty() {
            return Err(self.call_error(
                Some(status),
                "response completed without final assistant content",
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
