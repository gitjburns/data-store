use std::{
    fmt, fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use reqwest::{StatusCode, blocking::Client};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::{
    config::RerankerModelConfig,
    error::ApiError,
    inference::{
        InferenceProgress, RerankerCandidateInput, RerankerCandidateScore, RerankerRuntime,
    },
};

const LOCAL_RERANKER_MODE: &str = "modernbert_sequence_classifier";
const HTTP_RERANKER_MODE: &str = "http_rerank";
const SMOKE_QUERY: &str = "clear writing style rules";
const SMOKE_DOCUMENT: &str = "Prefer specific words and direct sentences.";
const SMOKE_DISTRACTOR_DOCUMENT: &str = "A recipe lists ingredients and oven temperatures.";
const HTTP_FAILURE_EXCERPT_CHARS: usize = 2048;

/// Config-selected reranker backend. Exactly one backend is active per service
/// instance and there is no fallback between variants. Enum dispatch (not trait
/// objects) keeps the generic progress-closure scoring signature intact.
#[derive(Debug, Clone)]
pub enum RerankerBackend {
    Local(RerankerRuntime),
    Http(HttpRerankerClient),
}

#[derive(Clone)]
pub struct HttpRerankerClient {
    client: Client,
    endpoint: String,
    model: String,
    timeout_seconds: u64,
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

#[derive(Debug, Serialize)]
struct HttpRerankRequest<'request> {
    model: &'request str,
    query: &'request str,
    documents: Vec<&'request str>,
    top_n: usize,
}

#[derive(Debug, Deserialize)]
struct HttpRerankResponse {
    results: Vec<HttpRerankResult>,
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
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        HttpRerankerClient::load_with_progress(config, progress).map(Self::Http)
    }

    /// Return the stable backend-kind label used in health and log output.
    pub fn kind(&self) -> &'static str {
        match self {
            RerankerBackend::Local(_) => "local",
            RerankerBackend::Http(_) => "http",
        }
    }

    /// Return the raw-diagnostics reranker mode label for this backend.
    pub fn mode(&self) -> &'static str {
        match self {
            RerankerBackend::Local(_) => LOCAL_RERANKER_MODE,
            RerankerBackend::Http(_) => HTTP_RERANKER_MODE,
        }
    }

    /// Return whether scoring uses the local accelerator model-call gate.
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
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        let endpoint = config.http_endpoint()?.to_string();
        let model = config.http_model()?.to_string();
        let timeout_seconds = config.http_timeout_seconds()?;
        let api_key_file_path = config.resolved_http_api_key_file_path();
        let api_key = match api_key_file_path.as_ref() {
            Some(path) => Some(read_api_key(path)?),
            None => None,
        };
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_seconds))
            .build()
            .map_err(|source| ApiError::InferenceInit {
                message: format!("failed to build HTTP reranker client: {source}"),
            })?;

        let mut backend = Self {
            client,
            endpoint,
            model,
            timeout_seconds,
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
            "reranker HTTP backend ready: mode {}, endpoint {}, model {}, timeout_seconds {}, api_key_file {}, smoke_candidates {}, smoke_score {:.6}",
            HTTP_RERANKER_MODE,
            self.endpoint,
            self.model,
            self.timeout_seconds,
            self.api_key_file_path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "absent".to_string()),
            self.smoke.candidate_count,
            self.smoke.first_score
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
    fn score_candidates(
        &self,
        query: &str,
        candidates: &[RerankerCandidateInput],
    ) -> Result<Vec<RerankerCandidateScore>, ApiError> {
        self.score_candidates_with_progress(query, candidates, |_, _| Ok(()))
    }

    /// Score candidate documents and report one completion step after the remote batch returns.
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
            info!(
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
            "HTTP reranker request started"
        );
        let response = match request_builder.send() {
            Ok(response) => response,
            Err(source) => {
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
                    error = %source,
                    "HTTP reranker request failed"
                );
                return Err(ApiError::InferenceInit {
                    message: format!(
                        "HTTP reranker request to {} failed before response: {source}",
                        self.endpoint
                    ),
                });
            }
        };
        let status = response.status();
        let body = match response.text() {
            Ok(body) => body,
            Err(source) => {
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
                    error = %source,
                    "HTTP reranker request failed"
                );
                return Err(ApiError::InferenceInit {
                    message: format!(
                        "HTTP reranker response body from {} could not be read: {source}",
                        self.endpoint
                    ),
                });
            }
        };
        if !status.is_success() {
            let body_excerpt = bounded_excerpt(&body);
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
            return Err(http_status_error(&self.endpoint, status, &body));
        }

        let parsed = match serde_json::from_str::<HttpRerankResponse>(&body) {
            Ok(parsed) => parsed,
            Err(source) => {
                let body_excerpt = bounded_excerpt(&body);
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
            response_body_chars = body.chars().count(),
            elapsed_ms = http_started.elapsed().as_millis() as u64,
            "HTTP reranker request completed"
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
fn http_status_error(endpoint: &str, status: StatusCode, body: &str) -> ApiError {
    ApiError::InferenceInit {
        message: format!(
            "HTTP reranker request to {endpoint} failed with status {}; body_excerpt={}",
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
