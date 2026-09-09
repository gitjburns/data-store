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

/// OpenAI chat-completions request body. Only the fields the producer contract
/// fixes are sent; the endpoint's own defaults govern everything else — except
/// for the one deliberate provider-specific extension below,
/// `chat_template_kwargs`, which is always sent to suppress the thinking trace
/// for these annotator calls.
#[derive(Debug, Serialize)]
struct ChatCompletionRequest<'request> {
    model: &'request str,
    messages: Vec<ChatMessage<'request>>,
    temperature: f64,
    // vLLM-specific extension, NOT a standard OpenAI chat-completions field.
    // OpenAI-compatible servers ignore unknown body fields, so sending this to a
    // stock OpenAI endpoint is harmless; on this vLLM deployment
    // `chat_template_kwargs.enable_thinking = false` suppresses the model's
    // thinking trace for THESE annotator calls only. The endpoint is shared and
    // stays thinking-enabled for every other client, because this flag rides on
    // the request body rather than any server-side setting (user ruling
    // 2026-07-17).
    chat_template_kwargs: ChatTemplateKwargs,
}

/// vLLM `chat_template_kwargs` payload. A typed serializable shape (per
/// PRINCIPLES "Rust Design Rules": application-owned request fields use typed
/// structs, not ad-hoc `serde_json::Value`) carrying the single flag this
/// client fixes. `enable_thinking` is always `false` for annotator calls; see
/// the provider-specific-extension note on `ChatCompletionRequest`.
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

/// Annotation parsing consumes first-choice text; optional provider metadata
/// supplies diagnostic facts without changing response acceptance.
#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
    // Metadata is optional provider data, not a condition for accepting content.
    id: Option<serde_json::Value>,
    usage: Option<serde_json::Value>,
}

/// One choice envelope with its optional provider termination reason.
#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatChoiceMessage,
    finish_reason: Option<serde_json::Value>,
}

/// Preserve provider metadata until the HTTP boundary records its measured facts.
struct CompletedResponse {
    content: String,
    response_id: Option<serde_json::Value>,
    usage: Option<serde_json::Value>,
    finish_reason: Option<serde_json::Value>,
}

/// The assistant message; only its textual content is consumed.
#[derive(Debug, Deserialize)]
struct ChatChoiceMessage {
    content: String,
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
            api_key,
            api_key_file_path,
        })
    }

    /// Expose the same maintenance signal used by HTTP calls to stop worker dispatch.
    pub(crate) fn cancellation(&self) -> &AnnotationCancellation {
        &self.cancellation
    }

    /// Run one chat-completions call and return the first choice's message
    /// content verbatim. `request_purpose` identifies the producer/invocation
    /// for logs; `temperature` is the per-call sampling temperature (the base
    /// `PRODUCER_TEMPERATURE` for first attempts, retry-escalated values from
    /// the worker's ladder), logged and sent verbatim so the audit trail
    /// records what actually ran. Every request failure — transport error,
    /// timeout, non-2xx status, unreadable or malformed body, or a missing
    /// choice/content — is an `ApiError::AnnotationProducer` carrying
    /// endpoint/model/status and a bounded body excerpt. Prompt text and model
    /// output are never logged. Maintenance cancellation drops the HTTP future
    /// and returns a distinct outcome so the worker does not count a failed attempt.
    pub(crate) fn complete(
        &self,
        request_purpose: &str,
        system_prompt: &str,
        user_content: &str,
        temperature: f64,
    ) -> Result<String, CompletionFailure> {
        let context = crate::util::model_call_context("annotator", request_purpose);
        let entered = context.enter();
        let started_at = Instant::now();
        // Char count is a safe compact shape fact; the content itself is a
        // forbidden log payload.
        let input_chars = system_prompt.chars().count() + user_content.chars().count();
        info!(
            event = "annotator_http.call.started",
            adapter_mode = ANNOTATOR_HTTP_MODE,
            request_purpose,
            endpoint = %self.endpoint,
            model = %self.model,
            timeout_seconds = self.timeout_seconds,
            temperature,
            input_chars,
            "annotator chat-completions call started"
        );
        // Async polling enters the span only while running, rather than retaining
        // a thread-local guard through network waits.
        drop(entered);

        let result = {
            let mut cancellation = self.cancellation.clone();
            let mut cancelled = pin!(cancellation.cancelled());
            let mut request = pin!(self.send_and_parse(system_prompt, user_content, temperature));
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
        match &result {
            Ok(response) => {
                info!(
                    event = "annotator_http.call.completed",
                    adapter_mode = ANNOTATOR_HTTP_MODE,
                    request_purpose,
                    endpoint = %self.endpoint,
                    model = %self.model,
                    temperature,
                    input_chars,
                    // Output length only; content is a forbidden log payload.
                    output_chars = response.content.chars().count(),
                    provider_response_id = response.response_id.as_ref().and_then(serde_json::Value::as_str),
                    prompt_tokens = response.usage.as_ref().and_then(|usage| usage.get("prompt_tokens")).and_then(serde_json::Value::as_u64),
                    completion_tokens = response.usage.as_ref().and_then(|usage| usage.get("completion_tokens")).and_then(serde_json::Value::as_u64),
                    total_tokens = response.usage.as_ref().and_then(|usage| usage.get("total_tokens")).and_then(serde_json::Value::as_u64),
                    finish_reason = response.finish_reason.as_ref().and_then(serde_json::Value::as_str),
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

        result.map(|response| response.content)
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
        temperature: f64,
    ) -> Result<CompletedResponse, ApiError> {
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
            // Constant for every annotator call: suppress the vLLM thinking
            // trace (see the provider-specific-extension note on
            // `ChatCompletionRequest`). Not threaded through `complete` or
            // config because it is a fixed producer-contract value.
            chat_template_kwargs: ChatTemplateKwargs {
                enable_thinking: false,
            },
        };
        let mut request_builder = self.client.post(&self.endpoint).json(&request);
        if let Some(api_key) = &self.api_key {
            request_builder = request_builder.bearer_auth(api_key);
        }

        // Transport-level failure (DNS, connect, TLS, timeout) surfaces before
        // any HTTP status exists.
        let response = request_builder.send().await.map_err(|source| {
            self.call_error(
                None,
                &format!(
                    "request failed before response: {}",
                    crate::util::error_chain(&source)
                ),
            )
        })?;
        let status = response.status();
        let body = response.text().await.map_err(|source| {
            self.call_error(
                Some(status),
                &format!(
                    "response body could not be read: {}",
                    crate::util::error_chain(&source)
                ),
            )
        })?;

        if !status.is_success() {
            return Err(self.call_error(
                Some(status),
                &format!(
                    "non-success status; body_excerpt={}",
                    bounded_excerpt(&body)
                ),
            ));
        }

        let parsed: ChatCompletionResponse = serde_json::from_str(&body).map_err(|source| {
            self.call_error(
                Some(status),
                &format!(
                    "response could not be parsed: {source}; body_excerpt={}",
                    bounded_excerpt(&body)
                ),
            )
        })?;

        // Shape violation: the endpoint returned 2xx but no usable assistant
        // message. This is a producer failure, not an empty result.
        let choice = parsed.choices.into_iter().next().ok_or_else(|| {
            self.call_error(
                Some(status),
                "response contained no choices[0].message.content",
            )
        })?;

        Ok(CompletedResponse {
            content: choice.message.content,
            response_id: parsed.id,
            usage: parsed.usage,
            finish_reason: choice.finish_reason,
        })
    }

    /// Construct a producer error carrying the endpoint/model/status context an
    /// operator needs to attribute the failure, without ever including prompt
    /// or output text beyond the caller-supplied bounded detail.
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

/// Return a bounded, escaped single-line excerpt of a failure body for
/// diagnostics, using the shared diagnostic cap so a pathological endpoint
/// response can never bloat an error or log line. Escaping keeps control
/// characters from corrupting the log line.
fn bounded_excerpt(value: &str) -> String {
    let escaped = value
        .chars()
        .flat_map(|character| character.escape_default())
        .collect::<String>();
    truncate_diagnostic_text(&escaped)
}
