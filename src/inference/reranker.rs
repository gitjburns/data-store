use std::{fs, path::Path, time::Instant};

use candle_core::Device;
use serde::Deserialize;
use tokenizers::Tokenizer;
use tracing::{error, info};

use crate::{
    config::RerankerModelConfig,
    error::ApiError,
    inference::{
        InferenceProgress,
        artifacts::{CONFIG_FILE_NAME, ModelArtifacts},
        qwen3::{Qwen3Model, TokenLogit, load_qwen3_config},
    },
};

const DEFAULT_INSTRUCTION: &str =
    "Given a web search query, retrieve relevant passages that answer the query";
const PROMPT_PREFIX: &str = "<|im_start|>system\nJudge whether the Document meets the requirements based on the Query and the Instruct provided. Note that the answer can only be \"yes\" or \"no\".<|im_end|>\n<|im_start|>user\n";
const PROMPT_SUFFIX: &str = "<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n";
const SMOKE_QUERY: &str = "clear writing style rules";
const SMOKE_DOCUMENT: &str = "Prefer specific words and direct sentences.";
const SMOKE_DISTRACTOR_DOCUMENT: &str = "A recipe lists ingredients and oven temperatures.";
const LOGIT_SCORE_DIR_NAME: &str = "1_LogitScore";
// Multi-row Qwen reranker passes produced non-finite logits on Metal during
// startup smoke scoring; keep scoring single-row until that path is proven.
const RERANKER_MICROBATCH_SIZE: usize = 1;

#[derive(Debug, Clone)]
pub struct RerankerRuntime {
    tokenizer: Tokenizer,
    model: Qwen3Model,
    device: Device,
    max_tokens: usize,
    true_token_id: u32,
    false_token_id: u32,
    smoke: RerankerSmoke,
}

#[derive(Debug, Clone)]
pub struct RerankerCandidateInput {
    pub unit_id: String,
    pub content: String,
}

#[derive(Debug, Clone)]
pub struct RerankerCandidateScore {
    pub unit_id: String,
    pub score: f32,
    pub rank: usize,
    pub true_logit: f32,
    pub false_logit: f32,
    pub token_count: usize,
}

#[derive(Debug, Clone)]
struct RerankerSmoke {
    score: f32,
    true_logit: f32,
    false_logit: f32,
    token_count: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct LogitScoreConfig {
    true_token_id: u32,
    false_token_id: u32,
}

#[derive(Debug, Clone)]
struct TokenizedRerankerCandidate {
    unit_id: String,
    input_ids: Vec<u32>,
    document_chars: usize,
}

#[derive(Debug, Clone)]
struct ScoredLogits {
    score: f32,
    true_logit: f32,
    false_logit: f32,
}

impl RerankerRuntime {
    /// Load the reranker runtime while reporting tokenizer, model, and smoke-check progress.
    pub fn load_with_progress(
        artifacts: &ModelArtifacts,
        config: &RerankerModelConfig,
        device: &Device,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        validate_reranker_config(config)?;

        progress("reranker_tokenizer_loading")?;
        let tokenizer = Tokenizer::from_file(&artifacts.tokenizer_path).map_err(|source| {
            inference_error(format!(
                "failed to load reranker tokenizer at {}: {source}",
                artifacts.tokenizer_path.display()
            ))
        })?;
        progress("reranker_tokenizer_ready")?;
        progress("reranker_config_loading")?;
        let qwen_config = load_qwen3_config("reranker", &artifacts.config_path)?;
        let logit_config = load_logit_score_config(
            &artifacts
                .root
                .join(LOGIT_SCORE_DIR_NAME)
                .join(CONFIG_FILE_NAME),
        )?;
        progress("reranker_config_ready")?;
        progress("reranker_model_loading")?;
        let model = Qwen3Model::load_with_progress(
            "reranker",
            &qwen_config,
            artifacts,
            device,
            Some("model"),
            progress,
        )?;
        progress("reranker_model_ready")?;
        let mut runtime = Self {
            tokenizer,
            model,
            device: device.clone(),
            max_tokens: config.max_tokens as usize,
            true_token_id: logit_config.true_token_id,
            false_token_id: logit_config.false_token_id,
            smoke: RerankerSmoke {
                score: 0.0,
                true_logit: 0.0,
                false_logit: 0.0,
                token_count: 0,
            },
        };
        progress("reranker_smoke_scoring")?;
        let smoke_candidates = vec![
            RerankerCandidateInput {
                unit_id: "smoke-relevant".to_string(),
                content: SMOKE_DOCUMENT.to_string(),
            },
            RerankerCandidateInput {
                unit_id: "smoke-distractor".to_string(),
                content: SMOKE_DISTRACTOR_DOCUMENT.to_string(),
            },
        ];
        let smoke_started_at = Instant::now();
        info!(
            event = "model_call.started",
            model_role = "reranker",
            call_purpose = "startup_smoke_scoring",
            candidate_count = smoke_candidates.len(),
            configured_max_tokens = runtime.max_tokens,
            selected_token_count = 2usize,
            true_token_id = runtime.true_token_id,
            false_token_id = runtime.false_token_id,
            "reranker startup smoke scoring started"
        );
        let smoke_scores_result = runtime.score_candidates(SMOKE_QUERY, &smoke_candidates);
        match &smoke_scores_result {
            Ok(scores) => {
                let max_token_count = scores
                    .iter()
                    .map(|score| score.token_count)
                    .max()
                    .unwrap_or(0);
                let first_score = scores.first();
                info!(
                    event = "model_call.completed",
                    model_role = "reranker",
                    call_purpose = "startup_smoke_scoring",
                    candidate_count = scores.len(),
                    configured_max_tokens = runtime.max_tokens,
                    selected_token_count = 2usize,
                    true_token_id = runtime.true_token_id,
                    false_token_id = runtime.false_token_id,
                    max_token_count,
                    smoke_score = ?first_score.map(|score| score.score),
                    smoke_true_logit = ?first_score.map(|score| score.true_logit),
                    smoke_false_logit = ?first_score.map(|score| score.false_logit),
                    smoke_token_count = ?first_score.map(|score| score.token_count),
                    elapsed_ms = smoke_started_at.elapsed().as_millis() as u64,
                    "reranker startup smoke scoring completed"
                );
            }
            Err(source) => {
                error!(
                    event = "model_call.failed",
                    model_role = "reranker",
                    call_purpose = "startup_smoke_scoring",
                    candidate_count = smoke_candidates.len(),
                    configured_max_tokens = runtime.max_tokens,
                    selected_token_count = 2usize,
                    true_token_id = runtime.true_token_id,
                    false_token_id = runtime.false_token_id,
                    elapsed_ms = smoke_started_at.elapsed().as_millis() as u64,
                    error = %source,
                    "reranker startup smoke scoring failed"
                );
            }
        }
        let smoke_scores = smoke_scores_result?;
        let smoke = smoke_scores.first().ok_or_else(|| {
            inference_error("reranker smoke candidate set produced no scores".to_string())
        })?;
        runtime.smoke = RerankerSmoke {
            score: smoke.score,
            true_logit: smoke.true_logit,
            false_logit: smoke.false_logit,
            token_count: smoke.token_count,
        };
        progress("reranker_smoke_ready")?;

        Ok(runtime)
    }

    /// Return reranker readiness details and the scoring-token contract.
    pub fn health_details(&self) -> Vec<String> {
        vec![format!(
            "reranker runtime ready: hidden {}, max_tokens {}, true_token {}, false_token {}, smoke_tokens {}, smoke_score {:.6}, smoke_true_logit {:.6}, smoke_false_logit {:.6}",
            self.model.hidden_size(),
            self.max_tokens,
            self.true_token_id,
            self.false_token_id,
            self.smoke.token_count,
            self.smoke.score,
            self.smoke.true_logit,
            self.smoke.false_logit
        )]
    }

    /// Score candidate documents without per-candidate progress reporting.
    pub fn score_candidates(
        &self,
        query: &str,
        candidates: &[RerankerCandidateInput],
    ) -> Result<Vec<RerankerCandidateScore>, ApiError> {
        self.score_candidates_with_progress(query, candidates, |_, _| Ok(()))
    }

    /// Score candidate documents while reporting completed reranker candidates.
    pub fn score_candidates_with_progress<F>(
        &self,
        query: &str,
        candidates: &[RerankerCandidateInput],
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
            call_purpose = "candidate_batch_scoring",
            input_kind = "query_candidates",
            query_chars,
            candidates = candidates.len(),
            document_chars,
            configured_max_tokens = self.max_tokens,
            "reranker candidate batch scoring started"
        );
        let result = (|| -> Result<Vec<RerankerCandidateScore>, ApiError> {
            let tokenized_candidates = self.tokenize_candidates(query, candidates)?;
            let total_token_count = tokenized_candidates
                .iter()
                .map(|candidate| candidate.input_ids.len())
                .sum::<usize>();
            let max_token_count = tokenized_candidates
                .iter()
                .map(|candidate| candidate.input_ids.len())
                .max()
                .unwrap_or(0);
            info!(
                event = "model_call.input_ready",
                model_role = "reranker",
                call_purpose = "candidate_batch_scoring",
                input_kind = "query_candidates",
                query_chars,
                candidates = candidates.len(),
                document_chars,
                configured_max_tokens = self.max_tokens,
                total_token_count,
                max_token_count,
                microbatch_size = RERANKER_MICROBATCH_SIZE,
                "reranker candidate batch input tokenized"
            );
            let total = candidates.len() as u64;
            let microbatch_count = if tokenized_candidates.is_empty() {
                0
            } else {
                (tokenized_candidates.len() + RERANKER_MICROBATCH_SIZE - 1)
                    / RERANKER_MICROBATCH_SIZE
            };
            let mut completed = 0u64;
            let mut scores = Vec::with_capacity(candidates.len());
            for (microbatch_index, microbatch) in tokenized_candidates
                .chunks(RERANKER_MICROBATCH_SIZE)
                .enumerate()
            {
                let mut microbatch_scores = self.score_candidate_microbatch(
                    microbatch_index + 1,
                    microbatch_count,
                    query_chars,
                    microbatch,
                )?;
                let completed_target = completed + microbatch_scores.len() as u64;
                scores.append(&mut microbatch_scores);
                while completed < completed_target {
                    completed += 1;
                    progress(completed, total)?;
                }
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
            Ok(scores) => {
                let max_token_count = scores
                    .iter()
                    .map(|score| score.token_count)
                    .max()
                    .unwrap_or(0);
                info!(
                    event = "model_call.completed",
                    model_role = "reranker",
                    call_purpose = "candidate_batch_scoring",
                    input_kind = "query_candidates",
                    query_chars,
                    candidates = candidates.len(),
                    scores = scores.len(),
                    document_chars,
                    configured_max_tokens = self.max_tokens,
                    max_token_count,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "reranker candidate batch scoring completed"
                );
            }
            Err(source) => {
                error!(
                    event = "model_call.failed",
                    model_role = "reranker",
                    call_purpose = "candidate_batch_scoring",
                    input_kind = "query_candidates",
                    query_chars,
                    candidates = candidates.len(),
                    document_chars,
                    configured_max_tokens = self.max_tokens,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    error = %source,
                    "reranker candidate batch scoring failed"
                );
            }
        }

        result
    }

    /// Render and tokenize reranker prompts while preserving candidate identity for later scores.
    fn tokenize_candidates(
        &self,
        query: &str,
        candidates: &[RerankerCandidateInput],
    ) -> Result<Vec<TokenizedRerankerCandidate>, ApiError> {
        candidates
            .iter()
            .map(|candidate| {
                let input_ids = build_reranker_input_ids(
                    &self.tokenizer,
                    DEFAULT_INSTRUCTION,
                    query,
                    &candidate.content,
                    self.max_tokens,
                )
                .map_err(|source| {
                    inference_error(format!(
                        "reranker candidate {} tokenization failed: {source}",
                        candidate.unit_id
                    ))
                })?;

                Ok(TokenizedRerankerCandidate {
                    unit_id: candidate.unit_id.clone(),
                    input_ids,
                    document_chars: candidate.content.chars().count(),
                })
            })
            .collect()
    }

    /// Score one tokenized candidate microbatch with a single Qwen forward pass.
    fn score_candidate_microbatch(
        &self,
        microbatch_index: usize,
        microbatch_count: usize,
        query_chars: usize,
        candidates: &[TokenizedRerankerCandidate],
    ) -> Result<Vec<RerankerCandidateScore>, ApiError> {
        let started_at = Instant::now();
        let document_chars = candidates
            .iter()
            .map(|candidate| candidate.document_chars)
            .sum::<usize>();
        let total_token_count = candidates
            .iter()
            .map(|candidate| candidate.input_ids.len())
            .sum::<usize>();
        let max_token_count = candidates
            .iter()
            .map(|candidate| candidate.input_ids.len())
            .max()
            .unwrap_or(0);
        info!(
            event = "model_call.started",
            model_role = "reranker",
            call_purpose = "candidate_microbatch_scoring",
            input_kind = "query_documents",
            microbatch_index,
            microbatch_count,
            query_chars,
            candidates = candidates.len(),
            document_chars,
            configured_max_tokens = self.max_tokens,
            total_token_count,
            max_token_count,
            selected_token_count = 2usize,
            "reranker candidate microbatch scoring started"
        );
        let result = (|| -> Result<Vec<RerankerCandidateScore>, ApiError> {
            let input_refs = candidates
                .iter()
                .map(|candidate| candidate.input_ids.as_slice())
                .collect::<Vec<_>>();
            let batch_logits = self.model.selected_token_logits_batch(
                &input_refs,
                &[self.false_token_id, self.true_token_id],
                &self.device,
                "reranker",
            )?;
            if batch_logits.len() != candidates.len() {
                return Err(inference_error(format!(
                    "reranker microbatch returned {} score rows for {} candidates",
                    batch_logits.len(),
                    candidates.len()
                )));
            }

            let mut scores = Vec::with_capacity(candidates.len());
            for (candidate, logits) in candidates.iter().zip(batch_logits.iter()) {
                let scored_logits =
                    score_selected_logits(logits, self.false_token_id, self.true_token_id)
                        .map_err(|source| {
                            inference_error(format!(
                                "reranker candidate {} logit scoring failed: {source}",
                                candidate.unit_id
                            ))
                        })?;
                scores.push(RerankerCandidateScore {
                    unit_id: candidate.unit_id.clone(),
                    score: scored_logits.score,
                    rank: 0,
                    true_logit: scored_logits.true_logit,
                    false_logit: scored_logits.false_logit,
                    token_count: candidate.input_ids.len(),
                });
            }

            Ok(scores)
        })();
        match &result {
            Ok(scores) => {
                info!(
                    event = "model_call.completed",
                    model_role = "reranker",
                    call_purpose = "candidate_microbatch_scoring",
                    input_kind = "query_documents",
                    microbatch_index,
                    microbatch_count,
                    query_chars,
                    candidates = candidates.len(),
                    scores = scores.len(),
                    document_chars,
                    configured_max_tokens = self.max_tokens,
                    total_token_count,
                    max_token_count,
                    selected_token_count = 2usize,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "reranker candidate microbatch scoring completed"
                );
            }
            Err(source) => {
                error!(
                    event = "model_call.failed",
                    model_role = "reranker",
                    call_purpose = "candidate_microbatch_scoring",
                    input_kind = "query_documents",
                    microbatch_index,
                    microbatch_count,
                    query_chars,
                    candidates = candidates.len(),
                    document_chars,
                    configured_max_tokens = self.max_tokens,
                    total_token_count,
                    max_token_count,
                    selected_token_count = 2usize,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    error = %source,
                    "reranker candidate microbatch scoring failed"
                );
            }
        }

        result
    }
}

/// Extract yes/no logits and convert them into the public reranker score.
fn score_selected_logits(
    logits: &[TokenLogit],
    false_token_id: u32,
    true_token_id: u32,
) -> Result<ScoredLogits, ApiError> {
    let false_logit = logits
        .iter()
        .find(|logit| logit.token_id == false_token_id)
        .map(|logit| logit.logit)
        .ok_or_else(|| inference_error("reranker false-token logit is missing".to_string()))?;
    let true_logit = logits
        .iter()
        .find(|logit| logit.token_id == true_token_id)
        .map(|logit| logit.logit)
        .ok_or_else(|| inference_error("reranker true-token logit is missing".to_string()))?;
    let score = yes_probability(false_logit, true_logit)?;

    Ok(ScoredLogits {
        score,
        true_logit,
        false_logit,
    })
}

/// Validate reranker config values that affect runtime prompt construction and memory use.
fn validate_reranker_config(config: &RerankerModelConfig) -> Result<(), ApiError> {
    if config.max_tokens == 0 {
        return Err(inference_error(
            "models.reranker.max_tokens must be greater than zero".to_string(),
        ));
    }

    Ok(())
}

/// Load the SentenceTransformers LogitScore token IDs used by the yes/no head.
fn load_logit_score_config(path: &Path) -> Result<LogitScoreConfig, ApiError> {
    let raw = fs::read_to_string(path).map_err(|source| ApiError::InferenceInit {
        message: format!(
            "failed to read reranker LogitScore config at {}: {source}",
            path.display()
        ),
    })?;
    serde_json::from_str(&raw).map_err(|source| ApiError::InferenceInit {
        message: format!(
            "failed to parse reranker LogitScore config at {}: {source}",
            path.display()
        ),
    })
}

/// Build a reranker prompt that preserves the assistant suffix used for next-token scoring.
fn build_reranker_input_ids(
    tokenizer: &Tokenizer,
    instruction: &str,
    query: &str,
    document: &str,
    max_tokens: usize,
) -> Result<Vec<u32>, ApiError> {
    let prefix_tokens = encode_without_special_tokens(tokenizer, PROMPT_PREFIX, "reranker prefix")?;
    let suffix_tokens = encode_without_special_tokens(tokenizer, PROMPT_SUFFIX, "reranker suffix")?;
    if prefix_tokens.len() + suffix_tokens.len() >= max_tokens {
        return Err(inference_error(format!(
            "models.reranker.max_tokens {max_tokens} leaves no room for query/document text after fixed prompt tokens"
        )));
    }
    let body = format!("<Instruct>: {instruction}\n<Query>: {query}\n<Document>: {document}");
    let mut body_tokens = encode_without_special_tokens(tokenizer, &body, "reranker body")?;
    let body_limit = max_tokens - prefix_tokens.len() - suffix_tokens.len();
    body_tokens.truncate(body_limit);

    let mut input_ids =
        Vec::with_capacity(prefix_tokens.len() + body_tokens.len() + suffix_tokens.len());
    input_ids.extend(prefix_tokens);
    input_ids.extend(body_tokens);
    input_ids.extend(suffix_tokens);

    Ok(input_ids)
}

/// Tokenize model-template text without adding extra special tokens around explicit chat markers.
fn encode_without_special_tokens(
    tokenizer: &Tokenizer,
    text: &str,
    label: &str,
) -> Result<Vec<u32>, ApiError> {
    tokenizer
        .encode(text, false)
        .map(|encoding| encoding.get_ids().to_vec())
        .map_err(|source| inference_error(format!("{label} tokenization failed: {source}")))
}

/// Convert no/yes logits into the public reranker score using a two-token softmax.
fn yes_probability(false_logit: f32, true_logit: f32) -> Result<f32, ApiError> {
    if !false_logit.is_finite() || !true_logit.is_finite() {
        return Err(inference_error(
            "reranker yes/no logits must be finite".to_string(),
        ));
    }
    let max_logit = false_logit.max(true_logit);
    let false_exp = (false_logit - max_logit).exp();
    let true_exp = (true_logit - max_logit).exp();
    let denominator = false_exp + true_exp;
    if !denominator.is_finite() || denominator <= 0.0 {
        return Err(inference_error(
            "reranker yes/no softmax denominator is invalid".to_string(),
        ));
    }

    Ok(true_exp / denominator)
}

/// Convert a Candle or tokenizer failure into the service inference error shape.
fn inference_error(message: String) -> ApiError {
    ApiError::InferenceInit { message }
}
