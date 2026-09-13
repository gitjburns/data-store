use std::{
    fmt, fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use reqwest::{StatusCode, blocking::Client};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

use crate::{
    config::RerankerModelConfig,
    error::ApiError,
    inference::{
        InferenceProgress, RerankerCandidateInput, RerankerCandidateScore, RerankerRuntime,
        http_models::{ServedModel, failure_excerpt, verify_capacity},
    },
    limits::{DiagnosticLimits, RuntimeLimits},
};

// Retained inference API (pinned contract); consumed at C7c.
#[allow(dead_code)]
const LOCAL_RERANKER_MODE: &str = "modernbert_sequence_classifier";
const HTTP_RERANKER_MODE: &str = "http_rerank";
const SMOKE_QUERY: &str = "clear writing style rules";
const SMOKE_DOCUMENT: &str = "Prefer specific words and direct sentences.";
const SMOKE_DISTRACTOR_DOCUMENT: &str = "A recipe lists ingredients and oven temperatures.";

/// Config-selected reranker backend. Exactly one backend is active per service
/// instance and there is no fallback between variants. Enum dispatch (not trait
/// objects) keeps the generic progress-closure scoring signature intact. Each
/// variant owns its provider behind a box, keeping the runtime handle compact
/// independently of the provider's retained metadata and configuration.
#[derive(Debug, Clone)]
pub enum RerankerBackend {
    Local(Box<RerankerRuntime>),
    Http(Box<HttpRerankerClient>),
}

#[derive(Clone)]
pub struct HttpRerankerClient {
    client: Client,
    endpoint: String,
    model: String,
    timeout_seconds: u64,
    served: ServedModel,
    diagnostics: DiagnosticLimits,
    api_key: Option<String>,
    api_key_file_path: Option<PathBuf>,
    smoke: HttpRerankerSmoke,
}

impl fmt::Debug for HttpRerankerClient {
    /// Format debug output without exposing the loaded API key secret.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpRerankerClient")
            .field("endpoint", &self.endpoint)
            .field("model", &self.model)
            .field("timeout_seconds", &self.timeout_seconds)
            .field("api_key_file_path", &self.api_key_file_path)
            .field("has_api_key", &self.api_key.is_some())
            .field("smoke", &self.smoke)
            .finish()
    }
}

#[derive(Debug, Clone)]
struct HttpRerankerSmoke {
    candidate_count: usize,
    first_score: f32,
}

/// Combined inputs must fit the verified serving capacity; no stage may clip their context.
#[derive(Debug, Serialize)]
struct HttpRerankRequest<'request> {
    model: &'request str,
    query: &'request str,
    documents: Vec<&'request str>,
    top_n: usize,
    truncate_prompt_tokens: Option<u32>,
    max_tokens_per_query: u32,
    max_tokens_per_doc: u32,
}

#[derive(Debug, Deserialize)]
struct HttpRerankResponse {
    results: Vec<HttpRerankResult>,
    // Provider metadata is diagnostic-only and may be absent or nonstandard.
    id: Option<serde_json::Value>,
    usage: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct HttpRerankResult {
    index: usize,
    relevance_score: f32,
}

impl RerankerBackend {
    /// Build the HTTP reranker backend and run the startup smoke check through the configured endpoint.
    pub fn load_http_with_progress(
        config: &RerankerModelConfig,
        config_root: &Path,
        limits: &RuntimeLimits,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        HttpRerankerClient::load_with_progress(config, config_root, limits, progress)
            .map(|client| Self::Http(Box::new(client)))
    }

    /// Return the stable backend-kind label used in health and log output.
    pub fn kind(&self) -> &'static str {
        match self {
            RerankerBackend::Local(_) => "local",
            RerankerBackend::Http(_) => "http",
        }
    }

    /// Return the raw-diagnostics reranker mode label for this backend.
    // Retained inference API (pinned contract); consumed at C7c.
    #[allow(dead_code)]
    pub fn mode(&self) -> &'static str {
        match self {
            RerankerBackend::Local(_) => LOCAL_RERANKER_MODE,
            RerankerBackend::Http(_) => HTTP_RERANKER_MODE,
        }
    }

    /// Return whether scoring uses the local accelerator model-call gate.
    // Retained inference API (pinned contract); consumed at C7c.
    #[allow(dead_code)]
    pub fn uses_local_model_gate(&self) -> bool {
        match self {
            RerankerBackend::Local(_) => true,
            RerankerBackend::Http(_) => false,
        }
    }

    /// Return the backend kind plus backend readiness details for health diagnostics.
    pub fn health_details(&self) -> Vec<String> {
        let mut details = vec![format!("reranker backend: {}", self.kind())];
        match self {
            RerankerBackend::Local(runtime) => details.extend(runtime.health_details()),
            RerankerBackend::Http(client) => details.extend(client.health_details()),
        }
        details
    }

    /// Score candidate documents without per-candidate progress reporting.
    // Retained inference API (pinned contract); consumed at C7c.
    #[allow(dead_code)]
    pub fn score_candidates(
        &self,
        query: &str,
        candidates: &[RerankerCandidateInput],
    ) -> Result<Vec<RerankerCandidateScore>, ApiError> {
        match self {
            RerankerBackend::Local(runtime) => runtime.score_candidates(query, candidates),
            RerankerBackend::Http(client) => client.score_candidates(query, candidates),
        }
    }

    /// Score candidate documents while reporting completed reranker candidates.
    // Retained inference API (pinned contract); consumed at C7c.
    #[allow(dead_code)]
    pub fn score_candidates_with_progress<F>(
        &self,
        query: &str,
        candidates: &[RerankerCandidateInput],
        progress: F,
    ) -> Result<Vec<RerankerCandidateScore>, ApiError>
    where
        F: FnMut(u64, u64) -> Result<(), ApiError>,
    {
        match self {
            RerankerBackend::Local(runtime) => {
                runtime.score_candidates_with_progress(query, candidates, progress)
            }
            RerankerBackend::Http(client) => {
                client.score_candidates_with_progress(query, candidates, progress)
            }
        }
    }
}

impl HttpRerankerClient {
    /// Build the blocking HTTP client, load optional credentials, and verify the endpoint with a smoke request.
    fn load_with_progress(
        config: &RerankerModelConfig,
        config_root: &Path,
        limits: &RuntimeLimits,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        let endpoint = config.http_endpoint()?.to_string();
        let model = config.http_model()?.to_string();
        let timeout_seconds = config.http_timeout_seconds()?;
        let api_key_file_path = config.resolved_http_api_key_file_path(config_root);
        let api_key = match api_key_file_path.as_ref() {
            Some(path) => Some(read_api_key(path)?),
            None => None,
        };
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_seconds))
            // Metadata and inference must stay on the configured serving boundary.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|source| ApiError::InferenceInit {
                message: format!("failed to build HTTP reranker client: {source}"),
            })?;

        progress("reranker_http_capacity_verifying")?;
        let served = verify_capacity(
            &client,
            &endpoint,
            "reranker",
            &model,
            config.max_tokens,
            api_key.as_deref(),
            limits,
        )?;
        let mut backend = Self {
            client,
            endpoint,
            model,
            timeout_seconds,
            served,
            diagnostics: limits.diagnostics,
            api_key,
            api_key_file_path,
            smoke: HttpRerankerSmoke {
                candidate_count: 0,
                first_score: 0.0,
            },
        };

        progress("reranker_http_smoke_scoring")?;
        backend.run_smoke_check()?;
        progress("reranker_http_smoke_ready")?;

        Ok(backend)
    }

    /// Return HTTP backend readiness details without exposing credentials.
    fn health_details(&self) -> Vec<String> {
        vec![format!(
            "reranker HTTP backend ready: mode {}, endpoint {}, model {}, timeout_seconds {}, api_key_file {}, smoke_candidates {}, smoke_score {:.6}, verified_max_tokens {}, checkpoint {}",
            HTTP_RERANKER_MODE,
            self.endpoint,
            self.model,
            self.timeout_seconds,
            self.api_key_file_path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "absent".to_string()),
            self.smoke.candidate_count,
            self.smoke.first_score,
            self.served.max_model_len,
            self.served.root
        )]
    }

    /// Run the startup smoke check through the configured remote reranker.
    fn run_smoke_check(&mut self) -> Result<(), ApiError> {
        let candidates = vec![
            RerankerCandidateInput {
                unit_id: "smoke-relevant".to_string(),
                content: SMOKE_DOCUMENT.to_string(),
            },
            RerankerCandidateInput {
                unit_id: "smoke-distractor".to_string(),
                content: SMOKE_DISTRACTOR_DOCUMENT.to_string(),
            },
        ];
        let scores = self.score_candidates_with_purpose(
            SMOKE_QUERY,
            &candidates,
            "startup_smoke_scoring",
            |_, _| Ok(()),
        )?;
        let first_score = scores
            .first()
            .ok_or_else(|| ApiError::InferenceInit {
                message: "HTTP reranker smoke candidate set produced no scores".to_string(),
            })?
            .score;
        self.smoke = HttpRerankerSmoke {
            candidate_count: scores.len(),
            first_score,
        };

        Ok(())
    }

    /// Score candidate documents without per-candidate progress reporting.
    // Retained inference API (pinned contract); consumed at C7c.
    #[allow(dead_code)]
    fn score_candidates(
        &self,
        query: &str,
        candidates: &[RerankerCandidateInput],
    ) -> Result<Vec<RerankerCandidateScore>, ApiError> {
        self.score_candidates_with_progress(query, candidates, |_, _| Ok(()))
    }

    /// Score candidate documents and report one completion step after the remote batch returns.
    // Retained inference API (pinned contract); consumed at C7c.
    #[allow(dead_code)]
    fn score_candidates_with_progress<F>(
        &self,
        query: &str,
        candidates: &[RerankerCandidateInput],
        progress: F,
    ) -> Result<Vec<RerankerCandidateScore>, ApiError>
    where
        F: FnMut(u64, u64) -> Result<(), ApiError>,
    {
        self.score_candidates_with_purpose(query, candidates, "candidate_batch_scoring", progress)
    }

    /// Score one HTTP rerank batch while logging provider-call boundaries and mapping provider indexes back to unit IDs.
    fn score_candidates_with_purpose<F>(
        &self,
        query: &str,
        candidates: &[RerankerCandidateInput],
        call_purpose: &'static str,
        mut progress: F,
    ) -> Result<Vec<RerankerCandidateScore>, ApiError>
    where
        F: FnMut(u64, u64) -> Result<(), ApiError>,
    {
        let context = crate::util::model_call_context("reranker", call_purpose);
        let _entered = context.enter();
        let started_at = Instant::now();
        let query_chars = query.chars().count();
        let document_chars = candidates
            .iter()
            .map(|candidate| candidate.content.chars().count())
            .sum::<usize>();
        info!(
            event = "model_call.started",
            model_role = "reranker",
            adapter_mode = HTTP_RERANKER_MODE,
            call_purpose,
            input_kind = "query_candidates",
            endpoint = %self.endpoint,
            model = %self.model,
            timeout_seconds = self.timeout_seconds,
            query_chars,
            candidates = candidates.len(),
            document_chars,
            "HTTP reranker candidate batch scoring started"
        );

        let result = (|| -> Result<Vec<RerankerCandidateScore>, ApiError> {
            debug!(
                event = "model_call.input_ready",
                model_role = "reranker",
                adapter_mode = HTTP_RERANKER_MODE,
                call_purpose,
                input_kind = "query_candidates",
                endpoint = %self.endpoint,
                model = %self.model,
                query_chars,
                candidates = candidates.len(),
                document_chars,
                "HTTP reranker candidate batch input ready"
            );
            if candidates.is_empty() {
                return Ok(Vec::new());
            }

            let response = self.send_request(query, candidates, call_purpose)?;
            let mut scores = self.map_response(candidates, response)?;
            scores.sort_by(|left, right| {
                right
                    .score
                    .total_cmp(&left.score)
                    .then_with(|| left.unit_id.cmp(&right.unit_id))
            });
            for (index, score) in scores.iter_mut().enumerate() {
                score.rank = index + 1;
            }
            progress(scores.len() as u64, candidates.len() as u64)?;

            Ok(scores)
        })();

        match &result {
            Ok(scores) => {
                info!(
                    event = "model_call.completed",
                    model_role = "reranker",
                    adapter_mode = HTTP_RERANKER_MODE,
                    call_purpose,
                    input_kind = "query_candidates",
                    endpoint = %self.endpoint,
                    model = %self.model,
                    query_chars,
                    candidates = candidates.len(),
                    scores = scores.len(),
                    document_chars,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "HTTP reranker candidate batch scoring completed"
                );
            }
            Err(source) => {
                error!(
                    event = "model_call.failed",
                    model_role = "reranker",
                    adapter_mode = HTTP_RERANKER_MODE,
                    call_purpose,
                    input_kind = "query_candidates",
                    endpoint = %self.endpoint,
                    model = %self.model,
                    query_chars,
                    candidates = candidates.len(),
                    document_chars,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    error = %source,
                    "HTTP reranker candidate batch scoring failed"
                );
            }
        }

        result
    }

    /// Send the Cohere-compatible rerank request and parse the provider response.
    fn send_request(
        &self,
        query: &str,
        candidates: &[RerankerCandidateInput],
        call_purpose: &'static str,
    ) -> Result<HttpRerankResponse, ApiError> {
        let http_started = Instant::now();
        let documents = candidates
            .iter()
            .map(|candidate| candidate.content.as_str())
            .collect::<Vec<_>>();
        let request = HttpRerankRequest {
            model: &self.model,
            query,
            top_n: documents.len(),
            documents,
            truncate_prompt_tokens: None,
            max_tokens_per_query: 0,
            max_tokens_per_doc: 0,
        };
        let mut request_builder = self.client.post(&self.endpoint).json(&request);
        if let Some(api_key) = &self.api_key {
            request_builder = request_builder.bearer_auth(api_key);
        }

        info!(
            event = "model_call.http_request.started",
            model_role = "reranker",
            adapter_mode = HTTP_RERANKER_MODE,
            call_purpose,
            endpoint = %self.endpoint,
            model = %self.model,
            candidates = candidates.len(),
            configured_max_tokens = self.served.max_model_len,
            "HTTP reranker request started"
        );
        let response = match request_builder.send() {
            Ok(response) => response,
            Err(source) => {
                let error_detail = crate::util::error_chain(&source, &self.diagnostics);
                error!(
                    event = "model_call.http_request.failed",
                    model_role = "reranker",
                    adapter_mode = HTTP_RERANKER_MODE,
                    call_purpose,
                    endpoint = %self.endpoint,
                    model = %self.model,
                    candidates = candidates.len(),
                    phase = "send_request",
                    elapsed_ms = http_started.elapsed().as_millis() as u64,
                    error = %error_detail,
                    "HTTP reranker request failed"
                );
                return Err(ApiError::InferenceInit {
                    message: format!(
                        "HTTP reranker request to {} failed before response: {error_detail}",
                        self.endpoint
                    ),
                });
            }
        };
        let status = response.status();
        let body = match response.text() {
            Ok(body) => body,
            Err(source) => {
                let error_detail = crate::util::error_chain(&source, &self.diagnostics);
                error!(
                    event = "model_call.http_request.failed",
                    model_role = "reranker",
                    adapter_mode = HTTP_RERANKER_MODE,
                    call_purpose,
                    endpoint = %self.endpoint,
                    model = %self.model,
                    candidates = candidates.len(),
                    http_status = status.as_u16(),
                    phase = "read_response_body",
                    elapsed_ms = http_started.elapsed().as_millis() as u64,
                    error = %error_detail,
                    "HTTP reranker request failed"
                );
                return Err(ApiError::InferenceInit {
                    message: format!(
                        "HTTP reranker response body from {} could not be read: {error_detail}",
                        self.endpoint
                    ),
                });
            }
        };
        if !status.is_success() {
            let body_excerpt = failure_excerpt(
                &body,
                self.api_key.as_deref(),
                self.diagnostics.model_error_excerpt_chars,
            );
            error!(
                event = "model_call.http_request.failed",
                model_role = "reranker",
                adapter_mode = HTTP_RERANKER_MODE,
                call_purpose,
                endpoint = %self.endpoint,
                model = %self.model,
                candidates = candidates.len(),
                http_status = status.as_u16(),
                phase = "http_status",
                response_body_excerpt = %body_excerpt,
                elapsed_ms = http_started.elapsed().as_millis() as u64,
                "HTTP reranker request failed"
            );
            return Err(http_status_error(&self.endpoint, status, &body_excerpt));
        }

        let parsed = match serde_json::from_str::<HttpRerankResponse>(&body) {
            Ok(parsed) => parsed,
            Err(source) => {
                let body_excerpt = failure_excerpt(
                    &body,
                    self.api_key.as_deref(),
                    self.diagnostics.model_error_excerpt_chars,
                );
                error!(
                    event = "model_call.http_request.failed",
                    model_role = "reranker",
                    adapter_mode = HTTP_RERANKER_MODE,
                    call_purpose,
                    endpoint = %self.endpoint,
                    model = %self.model,
                    candidates = candidates.len(),
                    http_status = status.as_u16(),
                    phase = "parse_response_body",
                    response_body_excerpt = %body_excerpt,
                    elapsed_ms = http_started.elapsed().as_millis() as u64,
                    error = %source,
                    "HTTP reranker request failed"
                );
                return Err(ApiError::InferenceInit {
                    message: format!(
                        "HTTP reranker response from {} could not be parsed: {source}; body_excerpt={}",
                        self.endpoint, body_excerpt
                    ),
                });
            }
        };
        info!(
            event = "model_call.http_request.completed",
            model_role = "reranker",
            adapter_mode = HTTP_RERANKER_MODE,
            call_purpose,
            endpoint = %self.endpoint,
            model = %self.model,
            candidates = candidates.len(),
            http_status = status.as_u16(),
            scores = parsed.results.len(),
            provider_response_id = parsed.id.as_ref().and_then(serde_json::Value::as_str),
            prompt_tokens = parsed.usage.as_ref().and_then(|usage| usage.get("prompt_tokens")).and_then(serde_json::Value::as_u64),
            total_tokens = parsed.usage.as_ref().and_then(|usage| usage.get("total_tokens")).and_then(serde_json::Value::as_u64),
            response_body_chars = body.chars().count(),
            elapsed_ms = http_started.elapsed().as_millis() as u64,
            result_state = "response_received_unvalidated",
            "HTTP reranker response received; score validation pending"
        );

        Ok(parsed)
    }

    /// Convert provider result indexes into service candidate scores while rejecting incomplete or malformed responses.
    fn map_response(
        &self,
        candidates: &[RerankerCandidateInput],
        response: HttpRerankResponse,
    ) -> Result<Vec<RerankerCandidateScore>, ApiError> {
        if response.results.len() != candidates.len() {
            return Err(ApiError::InferenceInit {
                message: format!(
                    "HTTP reranker returned {} scores for {} candidates",
                    response.results.len(),
                    candidates.len()
                ),
            });
        }

        let mut seen = vec![false; candidates.len()];
        let mut scores = Vec::with_capacity(response.results.len());
        for result in response.results {
            let Some(candidate) = candidates.get(result.index) else {
                return Err(ApiError::InferenceInit {
                    message: format!(
                        "HTTP reranker returned out-of-range result index {} for {} candidates",
                        result.index,
                        candidates.len()
                    ),
                });
            };
            if seen[result.index] {
                return Err(ApiError::InferenceInit {
                    message: format!(
                        "HTTP reranker returned duplicate result index {}",
                        result.index
                    ),
                });
            }
            if !result.relevance_score.is_finite() {
                return Err(ApiError::InferenceInit {
                    message: format!(
                        "HTTP reranker returned non-finite relevance_score for index {}",
                        result.index
                    ),
                });
            }
            seen[result.index] = true;
            scores.push(RerankerCandidateScore {
                unit_id: candidate.unit_id.clone(),
                score: result.relevance_score,
                rank: 0,
                logit: None,
                token_count: None,
            });
        }

        Ok(scores)
    }
}

/// Read a provider API key from an optional owner-only file without logging the secret.
fn read_api_key(path: &Path) -> Result<String, ApiError> {
    info!(
        event = "reranker_http.api_key_file.read_started",
        path = %path.display(),
        "HTTP reranker API-key file read started"
    );
    if let Err(source) = validate_api_key_file_permissions(path) {
        error!(
            event = "reranker_http.api_key_file.read_failed",
            path = %path.display(),
            phase = "validate_permissions",
            error = %source,
            "HTTP reranker API-key file read failed"
        );
        return Err(source);
    }
    let raw = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(source) => {
            error!(
                event = "reranker_http.api_key_file.read_failed",
                path = %path.display(),
                error = %source,
                "HTTP reranker API-key file read failed"
            );
            return Err(ApiError::InferenceInit {
                message: format!(
                    "failed to read HTTP reranker API-key file at {}: {source}",
                    path.display()
                ),
            });
        }
    };
    let key = raw.trim().to_string();
    if key.is_empty() {
        error!(
            event = "reranker_http.api_key_file.read_failed",
            path = %path.display(),
            error = "empty_api_key_file",
            "HTTP reranker API-key file read failed"
        );
        return Err(ApiError::InferenceInit {
            message: format!("HTTP reranker API-key file at {} is empty", path.display()),
        });
    }
    info!(
        event = "reranker_http.api_key_file.read_completed",
        path = %path.display(),
        "HTTP reranker API-key file read completed"
    );

    Ok(key)
}

/// Enforce owner-only API-key file permissions on Unix platforms.
#[cfg(unix)]
fn validate_api_key_file_permissions(path: &Path) -> Result<(), ApiError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = fs::metadata(path).map_err(|source| ApiError::InferenceInit {
        message: format!(
            "failed to inspect HTTP reranker API-key file at {}: {source}",
            path.display()
        ),
    })?;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(ApiError::InferenceInit {
            message: format!(
                "HTTP reranker API-key file at {} must be owner-only",
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

/// Build an inference error for an unsuccessful HTTP reranker status response.
fn http_status_error(endpoint: &str, status: StatusCode, body_excerpt: &str) -> ApiError {
    ApiError::InferenceInit {
        message: format!(
            "HTTP reranker request to {endpoint} failed with status {}; body_excerpt={}",
            status.as_u16(),
            body_excerpt
        ),
    }
}
