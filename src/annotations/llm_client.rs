//! CAb: shared OpenAI-compatible chat-completions client for the annotation
//! producers (entity, relation, summary), mirroring the HTTP-reranker
//! discipline (explicit failure, bounded diagnostics, no fallback).
//!
//! One external endpoint serves all three producers. There is no fallback
//! model or endpoint: a call failure becomes `ApiError::AnnotationProducer`,
//! and the annotation worker parks the affected annotation as `failed` for a
//! later retry pass. Prompt content and model output are external-language
//! payloads and never enter the service log (DIAGNOSTICS forbidden-data
//! rules); logs carry only compact boundary facts.

use std::{
    fmt, fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use reqwest::{StatusCode, blocking::Client};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::config::AnnotatorModelConfig;
use crate::error::ApiError;
use crate::util::truncate_diagnostic_text;

/// Stable adapter-mode label carried in this client's boundary logs, mirroring
/// the reranker's `adapter_mode` field. The event names use an
/// `annotator_http.` prefix so an operator can grep annotator calls distinctly
/// from reranker calls.
const ANNOTATOR_HTTP_MODE: &str = "openai_chat_completions";

/// Deterministic-leaning temperature: annotation producers request the most
/// reproducible output the endpoint will give, so identical inputs tend to
/// yield identical annotations and memo reuse stays meaningful. It is a code
/// constant, not config, because it is a producer-contract decision, not an
/// operator tuning knob.
const PRODUCER_TEMPERATURE: f64 = 0.0;

/// Blocking OpenAI-compatible chat-completions client shared by the three
/// producers. Holds the configured endpoint/model and an optional bearer key;
/// the key is never logged or shown in Debug output.
#[derive(Clone)]
pub(crate) struct AnnotatorClient {
    client: Client,
    endpoint: String,
    model: String,
    timeout_seconds: u64,
    api_key: Option<String>,
    api_key_file_path: Option<PathBuf>,
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

/// Minimal typed view of the chat-completions response. Only the first
/// choice's message content is consumed; unknown fields are ignored because
/// the producer contract needs exactly the assistant text and nothing else.
#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
}

/// One choice envelope; only its message is read.
#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatChoiceMessage,
}

/// The assistant message; only its textual content is consumed.
#[derive(Debug, Deserialize)]
struct ChatChoiceMessage {
    content: String,
}

impl AnnotatorClient {
    /// Build the blocking client and load the optional bearer key.
    ///
    /// There is deliberately NO startup smoke request here, and this loader is
    /// NOT readiness-critical: annotations build post-activation on a dedicated
    /// worker and never block activation or readiness. An unreachable or
    /// misconfigured endpoint therefore must not fail startup — it simply
    /// parks each attempted annotation as `failed` at call time for a later
    /// retry pass. Only local, deterministic setup (client build, key-file
    /// read) can fail here.
    pub(crate) fn load(
        config: &AnnotatorModelConfig,
        config_root: &Path,
    ) -> Result<Self, ApiError> {
        let endpoint = config.endpoint.trim().to_string();
        let model = config.model.trim().to_string();
        let timeout_seconds = config.timeout_seconds;
        let api_key_file_path = config.resolved_api_key_file_path(config_root);
        let api_key = match api_key_file_path.as_ref() {
            Some(path) => Some(read_api_key(path)?),
            None => None,
        };
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
            endpoint,
            model,
            timeout_seconds,
            api_key,
            api_key_file_path,
        })
    }

    /// Run one chat-completions call and return the first choice's message
    /// content verbatim. `request_purpose` identifies the producer/invocation
    /// for logs. Every non-success outcome — transport error, timeout, non-2xx
    /// status, unreadable or malformed body, or a missing choice/content — is
    /// an `ApiError::AnnotationProducer` carrying endpoint/model/status and a
    /// bounded body excerpt. Prompt text and model output are never logged.
    pub(crate) fn complete(
        &self,
        request_purpose: &str,
        system_prompt: &str,
        user_content: &str,
    ) -> Result<String, ApiError> {
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
            input_chars,
            "annotator chat-completions call started"
        );

        let result = self.send_and_parse(system_prompt, user_content);

        match &result {
            Ok(content) => {
                info!(
                    event = "annotator_http.call.completed",
                    adapter_mode = ANNOTATOR_HTTP_MODE,
                    request_purpose,
                    endpoint = %self.endpoint,
                    model = %self.model,
                    input_chars,
                    // Output length only; content is a forbidden log payload.
                    output_chars = content.chars().count(),
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "annotator chat-completions call completed"
                );
            }
            Err(source) => {
                error!(
                    event = "annotator_http.call.failed",
                    adapter_mode = ANNOTATOR_HTTP_MODE,
                    request_purpose,
                    endpoint = %self.endpoint,
                    model = %self.model,
                    input_chars,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    error = %source,
                    "annotator chat-completions call failed"
                );
            }
        }

        result
    }

    /// Build the request body, send it, and map every failure mode into a
    /// producer error. Split out from `complete` so the boundary logging in
    /// `complete` wraps exactly one fallible unit of work.
    fn send_and_parse(&self, system_prompt: &str, user_content: &str) -> Result<String, ApiError> {
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
            temperature: PRODUCER_TEMPERATURE,
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
        let response = request_builder.send().map_err(|source| {
            self.call_error(None, &format!("request failed before response: {source}"))
        })?;
        let status = response.status();
        let body = response.text().map_err(|source| {
            self.call_error(
                Some(status),
                &format!("response body could not be read: {source}"),
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
        let content = parsed
            .choices
            .into_iter()
            .next()
            .map(|choice| choice.message.content)
            .ok_or_else(|| {
                self.call_error(
                    Some(status),
                    "response contained no choices[0].message.content",
                )
            })?;

        Ok(content)
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
