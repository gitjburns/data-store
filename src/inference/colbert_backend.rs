//! Remote token inference retains the local ColBERT formatting and CPU MaxSim contract.

use std::{
    fmt, fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use candle_core::{Device, Tensor};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use tokenizers::Tokenizer;
use tracing::{error, info};

use crate::{
    config::ColbertModelConfig,
    error::ApiError,
    inference::{
        InferenceProgress,
        colbert::{
            ColbertCandidateScore, ColbertDocumentEmbedding, ColbertRuntime, PreparedColbertQuery,
            SMOKE_BATCH_SHORT, SMOKE_DOCUMENT, SMOKE_QUERY, format_document, format_query,
            maxsim_score, tokenize_formatted, validate_colbert_config,
        },
    },
    util::{error_chain, model_call_context},
};

const HTTP_COLBERT_MODE: &str = "http_vllm_pooling";
const FAILURE_EXCERPT_CHARS: usize = 2048;

/// Exactly one inference provider is selected; neither variant falls back to the other.
/// Both are boxed because their tokenizer/model state is large and travels with runtime clones.
#[derive(Debug, Clone)]
pub enum ColbertBackend {
    Local(Box<ColbertRuntime>),
    Http(Box<HttpColbertClient>),
}

/// Complete source text fitting one formatted ColBERT document, with Unicode scalar offsets.
#[derive(Debug)]
pub struct ColbertTextWindow {
    pub text: String,
    /// Inclusive offset into the input supplied to `document_windows`.
    pub start_char: usize,
    /// Exclusive offset; adjacent windows preserve every scalar, including whitespace.
    pub end_char: usize,
}

impl ColbertBackend {
    /// Retain the loaded local runtime behind the common retrieval interface.
    pub fn local(runtime: ColbertRuntime) -> Self {
        Self::Local(Box::new(runtime))
    }

    /// Validate remote inference and CPU scoring before publishing runtime readiness.
    pub fn load_http_with_progress(
        config: &ColbertModelConfig,
        config_root: &Path,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        HttpColbertClient::load_with_progress(config, config_root, progress)
            .map(|client| Self::Http(Box::new(client)))
    }

    /// HTTP calls must not hold the exclusive local accelerator gate while waiting on I/O.
    pub fn uses_local_model_gate(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    /// Identify the configured provider in health and operator diagnostics.
    pub fn backend_kind(&self) -> &'static str {
        match self {
            Self::Local(_) => "local",
            Self::Http(_) => "http",
        }
    }

    /// Share the inference tokenizer with chunking and evidence token accounting.
    pub fn tokenizer(&self) -> &Tokenizer {
        match self {
            Self::Local(runtime) => runtime.tokenizer(),
            Self::Http(client) => &client.tokenizer,
        }
    }

    /// Partition source text to fit both raw passage and formatted model limits, including special tokens.
    pub fn document_windows(
        &self,
        text: &str,
        max_content_tokens: usize,
    ) -> Result<Vec<ColbertTextWindow>, ApiError> {
        if text.is_empty() {
            return Ok(Vec::new());
        }
        let max_tokens = match self {
            Self::Local(runtime) => runtime.document_max_tokens(),
            Self::Http(client) => client.document_max_tokens,
        };
        // Counting must see the whole formatted input; provider truncation or padding would hide tails.
        let mut counter = self.tokenizer().clone();
        counter.with_truncation(None).map_err(|source| {
            inference_error(format!(
                "failed to disable ColBERT document-window tokenizer truncation: {source}"
            ))
        })?;
        counter.with_padding(None);
        if !document_window_fits(&counter, "", max_tokens, max_content_tokens)? {
            return Err(inference_error(format!(
                "ColBERT document limit {max_tokens} or raw content limit {max_content_tokens} cannot fit formatting and special tokens"
            )));
        }
        let boundaries: Vec<usize> = text
            .char_indices()
            .map(|(offset, _)| offset)
            .chain(std::iter::once(text.len()))
            .collect();
        let mut start_char = 0;
        let mut windows = Vec::new();
        while start_char < boundaries.len() - 1 {
            let end_char = document_window_end(
                &counter,
                text,
                &boundaries,
                start_char,
                max_tokens,
                max_content_tokens,
            )?;
            windows.push(ColbertTextWindow {
                text: text[boundaries[start_char]..boundaries[end_char]].to_owned(),
                start_char,
                end_char,
            });
            start_char = end_char;
        }
        Ok(windows)
    }

    /// Expose readiness facts without revealing authentication material.
    pub fn health_details(&self) -> Vec<String> {
        let mut details = vec![format!("ColBERT backend: {}", self.backend_kind())];
        match self {
            Self::Local(runtime) => details.extend(runtime.health_details()),
            Self::Http(client) => details.extend(client.health_details()),
        }
        details
    }

    /// Produce one persistable token matrix with the caller's unit identity intact.
    pub fn embed_document(
        &self,
        unit_id: &str,
        text: &str,
    ) -> Result<ColbertDocumentEmbedding, ApiError> {
        match self {
            Self::Local(runtime) => runtime.embed_document(unit_id, text),
            Self::Http(client) => client
                .embed_documents(&[(unit_id, text)])?
                .pop()
                .ok_or_else(|| inference_error("HTTP ColBERT returned no document matrix")),
        }
    }

    /// Preserve input ordering while sending one remote request for the caller's batch.
    pub fn embed_documents(
        &self,
        documents: &[(&str, &str)],
    ) -> Result<Vec<ColbertDocumentEmbedding>, ApiError> {
        match self {
            Self::Local(runtime) => runtime.embed_documents(documents),
            Self::Http(client) => client.embed_documents(documents),
        }
    }

    /// Embed a query once for streamed scoring; local callers hold a model permit during this call.
    /// Retaining the prepared tensor between calls needs no permit; local scoring acquires one again.
    pub fn prepare_query(&self, query: &str) -> Result<PreparedColbertQuery, ApiError> {
        match self {
            Self::Local(runtime) => runtime.prepare_query(query),
            Self::Http(client) => client.prepare_query(query),
        }
    }

    /// Score one persisted matrix on the configured device without allocating an entire shortlist.
    /// The owning query stage records scoring diagnostics and supplies any required local model permit.
    pub fn score_matrix(
        &self,
        prepared: &PreparedColbertQuery,
        values: &[f32],
        rows: usize,
        dimension: usize,
    ) -> Result<f32, ApiError> {
        match self {
            Self::Local(runtime) => runtime.score_matrix(prepared, values, rows, dimension),
            Self::Http(client) => {
                prepared.score_matrix(values, rows, dimension, client.dimension, &Device::Cpu)
            }
        }
    }
}

/// Verify both passage-rendering and model-input bounds with the untruncated, unpadded tokenizer.
fn document_window_fits(
    counter: &Tokenizer,
    text: &str,
    max_tokens: usize,
    max_content_tokens: usize,
) -> Result<bool, ApiError> {
    // Raw passage accounting also includes special tokens, but omits the model's document prompt.
    let content_tokens = counter
        .encode(text, true)
        .map_err(|source| {
            inference_error(format!(
                "ColBERT source-window tokenization failed: {source}"
            ))
        })?
        .len();
    if content_tokens > max_content_tokens {
        return Ok(false);
    }
    counter
        .encode(format_document(text), true)
        .map(|encoding| encoding.len() <= max_tokens)
        .map_err(|source| {
            inference_error(format!(
                "ColBERT document-window tokenization failed: {source}"
            ))
        })
}

/// Find a verified fitting endpoint without tokenizing an entire long source for every window.
fn document_window_end(
    counter: &Tokenizer,
    text: &str,
    boundaries: &[usize],
    start_char: usize,
    max_tokens: usize,
    max_content_tokens: usize,
) -> Result<usize, ApiError> {
    let total_chars = boundaries.len() - 1;
    let start_byte = boundaries[start_char];
    let mut fitting_end = start_char;
    let mut probe_end = start_char
        .saturating_add(max_tokens.max(1))
        .min(total_chars);
    loop {
        let candidate = &text[start_byte..boundaries[probe_end]];
        if !document_window_fits(counter, candidate, max_tokens, max_content_tokens)? {
            break;
        }
        fitting_end = probe_end;
        if fitting_end == total_chars {
            return Ok(fitting_end);
        }
        let next_length = (fitting_end - start_char).saturating_mul(2);
        probe_end = start_char.saturating_add(next_length).min(total_chars);
    }
    // Token merges need not be monotonic. Keep only actually measured fitting endpoints;
    // a conservative window is acceptable, but an inferred fit could silently lose its tail.
    while probe_end - fitting_end > 1 {
        let middle = fitting_end + (probe_end - fitting_end) / 2;
        if document_window_fits(
            counter,
            &text[start_byte..boundaries[middle]],
            max_tokens,
            max_content_tokens,
        )? {
            fitting_end = middle;
        } else {
            probe_end = middle;
        }
    }
    if fitting_end == start_char {
        return Err(inference_error(format!(
            "ColBERT document limit {max_tokens} or raw content limit {max_content_tokens} cannot fit the source scalar at character {start_char}"
        )));
    }
    Ok(fitting_end)
}

/// Synchronous vLLM pooling client; it loads a tokenizer but no model weights or accelerator.
#[derive(Clone)]
pub struct HttpColbertClient {
    client: Client,
    endpoint: String,
    model: String,
    tokenizer: Tokenizer,
    tokenizer_path: PathBuf,
    timeout_seconds: u64,
    dimension: usize,
    query_max_tokens: usize,
    document_max_tokens: usize,
    api_key: Option<String>,
    smoke_score: Option<f32>,
}

impl fmt::Debug for HttpColbertClient {
    /// Keep the bearer secret and tokenizer internals out of diagnostic formatting.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpColbertClient")
            .field("endpoint", &self.endpoint)
            .field("model", &self.model)
            .field("tokenizer_path", &self.tokenizer_path)
            .field("timeout_seconds", &self.timeout_seconds)
            .field("dimension", &self.dimension)
            .field("query_max_tokens", &self.query_max_tokens)
            .field("document_max_tokens", &self.document_max_tokens)
            .field("has_api_key", &self.api_key.is_some())
            .field("smoke_score", &self.smoke_score)
            .finish()
    }
}

/// Token IDs bypass server tokenization so prompts, special tokens and truncation stay identical.
#[derive(Serialize)]
struct PoolingRequest<'request> {
    model: &'request str,
    task: &'static str,
    encoding_format: &'static str,
    use_activation: bool,
    add_special_tokens: bool,
    input: &'request [Vec<u32>],
}

/// vLLM supplies one indexed token matrix per input; array ordering is not assumed.
#[derive(Deserialize)]
struct PoolingResponse {
    data: Vec<PoolingData>,
    usage: Option<PoolingUsage>,
}

/// Optional provider accounting is observed, never inferred from the request's token count.
#[derive(Deserialize)]
struct PoolingUsage {
    prompt_tokens: Option<u64>,
    total_tokens: Option<u64>,
}

#[derive(Deserialize)]
struct PoolingData {
    index: usize,
    data: Vec<Vec<f32>>,
}

impl HttpColbertClient {
    /// Resolve local tokenizer/key inputs and make readiness depend on real remote inference.
    fn load_with_progress(
        config: &ColbertModelConfig,
        config_root: &Path,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        validate_colbert_config(config)?;
        let endpoint = config.http_endpoint()?.to_owned();
        let model = config.http_model()?.to_owned();
        let timeout_seconds = config.http_timeout_seconds()?;
        let tokenizer_path = config.http_tokenizer_file_path()?.to_path_buf();
        progress("colbert_http_tokenizer_loading")?;
        let tokenizer = Tokenizer::from_file(&tokenizer_path).map_err(|source| {
            inference_error(format!(
                "failed to load HTTP ColBERT tokenizer at {}: {source}",
                tokenizer_path.display()
            ))
        })?;
        progress("colbert_http_tokenizer_ready")?;
        let api_key = config
            .resolved_http_api_key_file_path(config_root)
            .as_deref()
            .map(read_api_key)
            .transpose()?;
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_seconds))
            // An inference request must use the configured boundary, never a redirect target.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|source| {
                inference_error(format!(
                    "failed to build HTTP ColBERT client: {}",
                    error_chain(&source)
                ))
            })?;
        let mut backend = Self {
            client,
            endpoint,
            model,
            tokenizer,
            tokenizer_path,
            timeout_seconds,
            dimension: config.dimension as usize,
            query_max_tokens: config.query_max_tokens as usize,
            document_max_tokens: config.document_max_tokens as usize,
            api_key,
            smoke_score: None,
        };
        progress("colbert_http_smoke_document_batch")?;
        let documents = backend.embed_documents(&[
            ("colbert-smoke-document", SMOKE_DOCUMENT),
            ("colbert-smoke-short", SMOKE_BATCH_SHORT),
        ])?;
        progress("colbert_http_smoke_query_and_cpu_maxsim")?;
        let scores = backend.score_persisted_candidates(SMOKE_QUERY, &documents)?;
        let first = scores
            .first()
            .ok_or_else(|| inference_error("HTTP ColBERT smoke produced no scores"))?;
        backend.smoke_score = Some(first.score);
        progress("colbert_http_smoke_ready")?;
        Ok(backend)
    }

    /// Report the deployed boundary and verified scoring path without credentials.
    fn health_details(&self) -> Vec<String> {
        vec![format!(
            "ColBERT HTTP backend ready: mode {HTTP_COLBERT_MODE}, endpoint {}, model {}, dimension {}, tokenizer {}, timeout_seconds {}, scoring CPU MaxSim, smoke_score {:?}",
            self.endpoint,
            self.model,
            self.dimension,
            self.tokenizer_path.display(),
            self.timeout_seconds,
            self.smoke_score
        )]
    }

    /// Map the caller's unit identities onto validated matrices without server-side text formatting.
    fn embed_documents(
        &self,
        documents: &[(&str, &str)],
    ) -> Result<Vec<ColbertDocumentEmbedding>, ApiError> {
        if documents.is_empty() {
            return Ok(Vec::new());
        }
        let input = documents
            .iter()
            .map(|(unit_id, text)| {
                tokenize_formatted(
                    &self.tokenizer,
                    &format_document(text),
                    self.document_max_tokens,
                    "HTTP document",
                )
                .map_err(|source| {
                    inference_error(format!(
                        "HTTP ColBERT document {unit_id} tokenization failed: {source}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let matrices = self.embed_token_batch(&input, "document_embedding")?;
        Ok(documents
            .iter()
            .zip(matrices)
            .zip(input)
            .map(|(((unit_id, _), vector), ids)| ColbertDocumentEmbedding {
                unit_id: (*unit_id).to_owned(),
                token_count: ids.len(),
                dimension: self.dimension,
                vector,
            })
            .collect())
    }

    /// Retain one remotely embedded query on CPU; the HTTP helper owns actual request diagnostics.
    fn prepare_query(&self, query: &str) -> Result<PreparedColbertQuery, ApiError> {
        let ids = tokenize_formatted(
            &self.tokenizer,
            &format_query(query),
            self.query_max_tokens,
            "HTTP query",
        )?;
        let query_tokens = ids.len();
        let vector = self
            .embed_token_batch(&[ids], "query_embedding")?
            .pop()
            .ok_or_else(|| inference_error("HTTP ColBERT returned no query matrix"))?;
        let matrix = cpu_matrix(&vector, query_tokens, self.dimension, "query")?;
        PreparedColbertQuery::from_matrix(matrix, self.dimension)
    }

    /// Keep scoring local and deterministic while remote inference supplies only the query matrix.
    fn score_persisted_candidates(
        &self,
        query: &str,
        candidates: &[ColbertDocumentEmbedding],
    ) -> Result<Vec<ColbertCandidateScore>, ApiError> {
        let context = model_call_context("colbert", "persisted_candidate_scoring");
        let _entered = context.enter();
        let started_at = Instant::now();
        info!(event = "model_call.started", model_role = "colbert",
            call_purpose = "persisted_candidate_scoring", adapter_mode = HTTP_COLBERT_MODE,
            endpoint = %self.endpoint, model = %self.model, candidates = candidates.len(),
            query_max_tokens = self.query_max_tokens, document_max_tokens = self.document_max_tokens,
            query_chars = query.chars().count(), "Remote ColBERT query and CPU scoring started");
        let result = (|| {
            let ids = tokenize_formatted(
                &self.tokenizer,
                &format_query(query),
                self.query_max_tokens,
                "HTTP query",
            )?;
            let query_tokens = ids.len();
            let vector = self
                .embed_token_batch(&[ids], "query_embedding")?
                .pop()
                .ok_or_else(|| inference_error("HTTP ColBERT returned no query matrix"))?;
            let query_matrix = cpu_matrix(&vector, query_tokens, self.dimension, "query")?;
            let mut scores = Vec::with_capacity(candidates.len());
            for candidate in candidates {
                if candidate.dimension != self.dimension {
                    return Err(inference_error(format!(
                        "persisted ColBERT document {} has dimension {}, expected {}",
                        candidate.unit_id, candidate.dimension, self.dimension
                    )));
                }
                let document_matrix = cpu_matrix(
                    &candidate.vector,
                    candidate.token_count,
                    candidate.dimension,
                    &candidate.unit_id,
                )?;
                scores.push(ColbertCandidateScore {
                    unit_id: candidate.unit_id.clone(),
                    score: maxsim_score(&query_matrix, &document_matrix)?,
                    rank: 0,
                    query_tokens,
                    document_tokens: candidate.token_count,
                });
            }
            scores.sort_by(|left, right| {
                right
                    .score
                    .total_cmp(&left.score)
                    .then_with(|| left.unit_id.cmp(&right.unit_id))
            });
            for (index, score) in scores.iter_mut().enumerate() {
                score.rank = index + 1;
            }
            Ok(scores)
        })();
        match &result {
            Ok(scores) => info!(event = "model_call.completed", model_role = "colbert",
                call_purpose = "persisted_candidate_scoring", adapter_mode = HTTP_COLBERT_MODE,
                endpoint = %self.endpoint, model = %self.model, scores = scores.len(),
                elapsed_ms = started_at.elapsed().as_millis() as u64, "Remote ColBERT query and CPU scoring completed"),
            Err(source) => error!(event = "model_call.failed", model_role = "colbert",
                call_purpose = "persisted_candidate_scoring", adapter_mode = HTTP_COLBERT_MODE,
                endpoint = %self.endpoint, model = %self.model, error = %source,
                elapsed_ms = started_at.elapsed().as_millis() as u64, "Remote ColBERT query and CPU scoring failed"),
        }
        result
    }

    /// Own the HTTP lifecycle and validate every returned token row before it can reach persistence.
    fn embed_token_batch(
        &self,
        input: &[Vec<u32>],
        purpose: &'static str,
    ) -> Result<Vec<Vec<f32>>, ApiError> {
        if input.is_empty() {
            return Ok(Vec::new());
        }
        let context = model_call_context("colbert", purpose);
        let _entered = context.enter();
        let started_at = Instant::now();
        let tokens = input
            .iter()
            .try_fold(0usize, |total, ids| total.checked_add(ids.len()))
            .ok_or_else(|| inference_error("HTTP ColBERT batch token count overflow"))?;
        info!(event = "model_call.started", model_role = "colbert", call_purpose = purpose,
            adapter_mode = HTTP_COLBERT_MODE, endpoint = %self.endpoint, model = %self.model,
            batch_size = input.len(), tokens, expected_dimension = self.dimension,
            query_max_tokens = self.query_max_tokens, document_max_tokens = self.document_max_tokens,
            timeout_seconds = self.timeout_seconds, "HTTP ColBERT token embedding started");
        let result = self.send_token_batch(input).map_err(|source| {
            inference_error(format!(
                "HTTP ColBERT request to {} model {} purpose {purpose} failed: {}",
                self.endpoint,
                self.model,
                self.redact(&error_chain(&source))
            ))
        });
        match &result {
            Ok(matrices) => info!(event = "model_call.completed", model_role = "colbert",
                call_purpose = purpose, adapter_mode = HTTP_COLBERT_MODE,
                endpoint = %self.endpoint, model = %self.model, batch_size = matrices.len(),
                tokens, dimension = self.dimension, elapsed_ms = started_at.elapsed().as_millis() as u64,
                "HTTP ColBERT token embedding completed"),
            Err(source) => error!(event = "model_call.failed", model_role = "colbert",
                call_purpose = purpose, adapter_mode = HTTP_COLBERT_MODE,
                endpoint = %self.endpoint, model = %self.model, batch_size = input.len(),
                tokens, expected_dimension = self.dimension, error = %source,
                elapsed_ms = started_at.elapsed().as_millis() as u64, "HTTP ColBERT token embedding failed"),
        }
        result
    }

    /// Make exactly one request: transport, status, schema, and matrix failures never retry implicitly.
    fn send_token_batch(&self, input: &[Vec<u32>]) -> Result<Vec<Vec<f32>>, ApiError> {
        if input.iter().any(Vec::is_empty) {
            return Err(inference_error(
                "HTTP ColBERT input contains an empty token sequence",
            ));
        }
        let started_at = Instant::now();
        // Keep vLLM's embedding activation enabled; IDs already include local special tokens.
        let body = PoolingRequest {
            model: &self.model,
            task: "token_embed",
            encoding_format: "float",
            use_activation: true,
            add_special_tokens: false,
            input,
        };
        let mut request = self.client.post(&self.endpoint).json(&body);
        if let Some(key) = &self.api_key {
            request = request.bearer_auth(key);
        }
        let response = request.send().map_err(|source| {
            inference_error(format!("request transport: {}", error_chain(&source)))
        })?;
        let status = response.status();
        // Retain the complete raw body through schema and matrix validation at this protocol boundary.
        // Payloads contain token matrices; operator diagnostics report shape and bounded failure facts.
        let raw = response.text().map_err(|source| {
            inference_error(format!(
                "response body read failed after status {status}: {}",
                error_chain(&source)
            ))
        })?;
        let parsed = serde_json::from_str::<PoolingResponse>(&raw);
        let usage = parsed
            .as_ref()
            .ok()
            .and_then(|response| response.usage.as_ref());
        // Receiving a response is not successful inference: schema and matrix validation still follow.
        info!(event = "model_call.http_request.completed", model_role = "colbert",
            adapter_mode = HTTP_COLBERT_MODE, endpoint = %self.endpoint, model = %self.model,
            status = status.as_u16(), response_bytes = raw.len(),
            prompt_tokens = ?usage.and_then(|usage| usage.prompt_tokens),
            total_tokens = ?usage.and_then(|usage| usage.total_tokens),
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "HTTP ColBERT response received; token matrices not yet validated");
        if !status.is_success() {
            let excerpt = self
                .redact(&raw)
                .chars()
                .flat_map(char::escape_default)
                .take(FAILURE_EXCERPT_CHARS)
                .collect::<String>();
            return Err(inference_error(format!(
                "HTTP status {status}; body_excerpt={excerpt}"
            )));
        }
        let response = parsed.map_err(|source| {
            inference_error(format!(
                "invalid pooling response JSON after status {status}, response_bytes={}: {source}",
                raw.len()
            ))
        })?;
        validate_response(response, input, self.dimension).map_err(|source| {
            inference_error(format!(
                "invalid pooling token matrices after status {status}, response_bytes={}: {source}",
                raw.len()
            ))
        })
    }

    /// Remove the configured bearer secret before provider errors enter durable diagnostics.
    fn redact(&self, text: &str) -> String {
        match &self.api_key {
            Some(key) => text.replace(key, "[REDACTED]"),
            None => text.to_owned(),
        }
    }
}

/// Restore input order and normalize finite nonzero rows only after validating their exact shape.
fn validate_response(
    response: PoolingResponse,
    input: &[Vec<u32>],
    dimension: usize,
) -> Result<Vec<Vec<f32>>, ApiError> {
    if dimension == 0 || input.iter().any(Vec::is_empty) {
        return Err(inference_error(
            "HTTP ColBERT matrix contract requires nonzero dimensions and token counts",
        ));
    }
    if response.data.len() != input.len() {
        return Err(inference_error(format!(
            "received {} matrices for {} inputs",
            response.data.len(),
            input.len()
        )));
    }
    let mut ordered: Vec<Option<Vec<f32>>> = vec![None; input.len()];
    for entry in response.data {
        let index = entry.index;
        if index >= input.len() {
            return Err(inference_error(format!(
                "matrix index {index} outside batch of {}",
                input.len()
            )));
        }
        if ordered[index].is_some() {
            return Err(inference_error(format!("duplicate matrix index {index}")));
        }
        if entry.data.len() != input[index].len() {
            return Err(inference_error(format!(
                "matrix {index} has {} token rows, expected {}",
                entry.data.len(),
                input[index].len()
            )));
        }
        let value_count = entry
            .data
            .len()
            .checked_mul(dimension)
            .ok_or_else(|| inference_error(format!("matrix {index} value count overflow")))?;
        let mut matrix = Vec::with_capacity(value_count);
        // Consume provider rows directly into their owned flattened result; reordering never clones matrices.
        for (row_index, row) in entry.data.into_iter().enumerate() {
            if row.len() != dimension || row.iter().any(|value| !value.is_finite()) {
                return Err(inference_error(format!(
                    "matrix {index} row {row_index} requires {dimension} finite values; received width {}",
                    row.len()
                )));
            }
            // f64 accumulation avoids overflow for finite f32 provider values and matches unit-vector semantics.
            let norm = row
                .iter()
                .map(|value| f64::from(*value).powi(2))
                .sum::<f64>()
                .sqrt();
            if !norm.is_finite() || norm <= 0.0 {
                return Err(inference_error(format!(
                    "matrix {index} row {row_index} has invalid zero/nonfinite norm"
                )));
            }
            matrix.extend(
                row.into_iter()
                    .map(|value| (f64::from(value) / norm) as f32),
            );
        }
        ordered[index] = Some(matrix);
    }
    ordered
        .into_iter()
        .enumerate()
        .map(|(index, matrix)| {
            matrix.ok_or_else(|| inference_error(format!("missing matrix index {index}")))
        })
        .collect()
}

/// Reject malformed persisted matrices before allocating the CPU tensor used by MaxSim.
fn cpu_matrix(
    values: &[f32],
    tokens: usize,
    dimension: usize,
    label: &str,
) -> Result<Tensor, ApiError> {
    if tokens == 0
        || dimension == 0
        || tokens.checked_mul(dimension) != Some(values.len())
        || values.iter().any(|value| !value.is_finite())
    {
        return Err(inference_error(format!(
            "ColBERT {label} has invalid matrix shape [{tokens}, {dimension}] or nonfinite values"
        )));
    }
    // Candle owns its tensor storage, so this copy leaves the caller's persisted matrix available for reuse.
    Tensor::from_slice(values, (tokens, dimension), &Device::Cpu).map_err(|source| {
        inference_error(format!(
            "failed to load ColBERT {label} matrix on CPU: {source}"
        ))
    })
}

/// Load optional bearer material once, enforcing the existing owner-only file contract on Unix.
fn read_api_key(path: &Path) -> Result<String, ApiError> {
    let started_at = Instant::now();
    info!(event = "colbert_http.api_key_file.read_started", path = %path.display(), "HTTP ColBERT API-key file read started");
    let result = (|| {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = fs::metadata(path).map_err(|source| {
                inference_error(format!(
                    "failed to inspect HTTP ColBERT API-key file {}: {source}",
                    path.display()
                ))
            })?;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(inference_error(format!(
                    "HTTP ColBERT API-key file {} must be owner-only",
                    path.display()
                )));
            }
        }
        let raw = fs::read_to_string(path).map_err(|source| {
            inference_error(format!(
                "failed to read HTTP ColBERT API-key file {}: {source}",
                path.display()
            ))
        })?;
        let key = raw.trim().to_owned();
        if key.is_empty() {
            return Err(inference_error(format!(
                "HTTP ColBERT API-key file {} is empty",
                path.display()
            )));
        }
        Ok(key)
    })();
    match &result {
        Ok(_) => info!(event = "colbert_http.api_key_file.read_completed", path = %path.display(),
            elapsed_ms = started_at.elapsed().as_millis() as u64, "HTTP ColBERT API-key file read completed"),
        Err(source) => {
            error!(event = "colbert_http.api_key_file.read_failed", path = %path.display(), error = %source,
            elapsed_ms = started_at.elapsed().as_millis() as u64, "HTTP ColBERT API-key file read failed")
        }
    }
    result
}

/// Use the existing inference error surface while retaining provider and shape context in messages.
fn inference_error(message: impl Into<String>) -> ApiError {
    ApiError::InferenceInit {
        message: message.into(),
    }
}
