use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

use candle_core::{D, DType, Device, Tensor};
use candle_nn::{Embedding, Linear, Module, VarBuilder, embedding, linear, linear_no_bias};
use safetensors::{Dtype as SafeTensorDType, SafeTensors};
use serde::Deserialize;
use tokenizers::Tokenizer;
use tracing::{error, info};

use crate::{
    config::RerankerModelConfig,
    error::ApiError,
    inference::{
        InferenceProgress,
        artifacts::ModelArtifacts,
        tensor_ops::{apply_rope, softmax_last_dim_metal_safe},
    },
};

const EXPECTED_MODEL_TYPE: &str = "modernbert";
const EXPECTED_ARCHITECTURE: &str = "ModernBertForSequenceClassification";
const EXPECTED_HIDDEN_SIZE: usize = 768;
const EXPECTED_LABEL_COUNT: usize = 1;
const EXPECTED_CLASSIFIER_POOLING: &str = "mean";
const EXPECTED_CLASSIFIER_ACTIVATION: &str = "gelu";
const EXPECTED_HIDDEN_ACTIVATION: &str = "gelu";
const RERANKER_ADAPTER_MODE: &str = "modernbert_sequence_classifier";
const PAIR_SPECIAL_TOKEN_COUNT: usize = 3;
const SMOKE_QUERY: &str = "clear writing style rules";
const SMOKE_DOCUMENT: &str = "Prefer specific words and direct sentences.";
const SMOKE_DISTRACTOR_DOCUMENT: &str = "A recipe lists ingredients and oven temperatures.";

#[derive(Debug, Clone)]
pub struct RerankerRuntime {
    tokenizer: Tokenizer,
    model: ModernBertSequenceClassifier,
    device: Device,
    max_tokens: usize,
    cls_token_id: u32,
    sep_token_id: u32,
    classifier_pooling: String,
    classifier_activation: String,
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
    // Raw diagnostics that not every reranker backend can provide. The local
    // ModernBERT runtime always populates them; absent values must be omitted
    // from diagnostics output, never synthesized.
    pub logit: Option<f32>,
    pub token_count: Option<usize>,
}

#[derive(Debug, Clone)]
struct RerankerSmoke {
    candidate_count: usize,
    max_token_count: usize,
    first_score: f32,
    first_logit: Option<f32>,
}

#[derive(Debug, Clone)]
struct ModernBertSequenceClassifier {
    input_path: ModernBertInputPath,
    encoder: ModernBertEncoderRuntime,
    classifier_head: ModernBertClassifierHead,
    hidden_size: usize,
}

#[derive(Debug, Clone)]
struct ModernBertInputPath {
    embeddings: Embedding,
    norm: MetalSafeLayerNorm,
    vocab_size: usize,
    hidden_size: usize,
}

#[derive(Debug, Clone)]
struct ModernBertEncoderRuntime {
    layers: Vec<ModernBertLayerPrimitive>,
    final_norm: MetalSafeLayerNorm,
    hidden_size: usize,
}

#[derive(Debug, Clone)]
struct ModernBertLayerPrimitive {
    attention: ModernBertAttentionPrimitive,
    mlp_norm: MetalSafeLayerNorm,
    mlp: ModernBertMlpPrimitive,
    layer_index: usize,
    hidden_size: usize,
}

#[derive(Debug, Clone)]
struct ModernBertAttentionPrimitive {
    qkv_proj: Linear,
    out_proj: Linear,
    attn_norm: Option<MetalSafeLayerNorm>,
    layer_index: usize,
    attention_kind: ModernBertAttentionKind,
    num_attention_heads: usize,
    head_dim: usize,
    hidden_size: usize,
    local_attention: usize,
    max_position_embeddings: usize,
    rope_theta: f64,
}

#[derive(Debug, Clone)]
struct ModernBertMlpPrimitive {
    input_proj: Linear,
    output_proj: Linear,
    hidden_size: usize,
    intermediate_size: usize,
}

#[derive(Debug, Clone)]
struct ModernBertClassifierHead {
    dense: Linear,
    norm: MetalSafeLayerNorm,
    classifier: Linear,
    hidden_size: usize,
    label_count: usize,
}

#[derive(Debug, Clone, Copy)]
enum ModernBertAttentionKind {
    Global,
    Local,
}

#[derive(Debug, Clone)]
struct TokenizedRerankerCandidate {
    unit_id: String,
    input_ids: Vec<u32>,
    document_chars: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct ModernBertConfig {
    architectures: Vec<String>,
    attention_bias: bool,
    classifier_activation: String,
    classifier_pooling: String,
    cls_token_id: u32,
    global_attn_every_n_layers: usize,
    global_rope_theta: f64,
    hidden_activation: String,
    hidden_size: usize,
    id2label: HashMap<String, String>,
    intermediate_size: usize,
    label2id: HashMap<String, usize>,
    local_attention: usize,
    local_rope_theta: f64,
    max_position_embeddings: usize,
    mlp_bias: bool,
    model_type: String,
    norm_eps: f64,
    num_attention_heads: usize,
    num_hidden_layers: usize,
    pad_token_id: u32,
    sep_token_id: u32,
    vocab_size: usize,
}

#[derive(Debug, Clone)]
struct TensorInventory {
    tensors: HashMap<String, TensorMetadata>,
}

#[derive(Debug, Clone)]
struct TensorMetadata {
    dtype: SafeTensorDType,
    shape: Vec<usize>,
}

#[derive(Debug, Clone)]
struct MetalSafeLayerNorm {
    weight: Tensor,
    eps: f64,
}

impl RerankerRuntime {
    /// Load the reranker runtime while reporting tokenizer, model, and smoke-check progress.
    pub fn load_with_progress(
        artifacts: &ModelArtifacts,
        config: &RerankerModelConfig,
        device: &Device,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        // Validated present by config load for the local backend this runtime serves.
        let max_tokens = config.local_max_tokens()?;
        validate_reranker_config(max_tokens)?;

        progress("reranker_tokenizer_loading")?;
        let mut tokenizer = Tokenizer::from_file(&artifacts.tokenizer_path).map_err(|source| {
            inference_error(format!(
                "failed to load reranker tokenizer at {}: {source}",
                artifacts.tokenizer_path.display()
            ))
        })?;
        disable_serialized_tokenizer_limits(&mut tokenizer)?;
        progress("reranker_tokenizer_ready")?;
        progress("reranker_config_loading")?;
        let model_config = load_modernbert_config(&artifacts.config_path)?;
        validate_modernbert_config(&model_config, max_tokens)?;
        progress("reranker_config_ready")?;
        progress("reranker_safetensors_validating")?;
        validate_root_safetensors(artifacts, &model_config)?;
        progress("reranker_safetensors_ready")?;
        progress("reranker_model_loading")?;
        let model = ModernBertSequenceClassifier::load_with_progress(
            artifacts,
            &model_config,
            device,
            progress,
        )?;
        progress("reranker_model_ready")?;

        let mut runtime = Self {
            tokenizer,
            model,
            device: device.clone(),
            max_tokens: max_tokens as usize,
            cls_token_id: model_config.cls_token_id,
            sep_token_id: model_config.sep_token_id,
            classifier_pooling: model_config.classifier_pooling.clone(),
            classifier_activation: model_config.classifier_activation.clone(),
            smoke: RerankerSmoke {
                candidate_count: 0,
                max_token_count: 0,
                first_score: 0.0,
                first_logit: None,
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
        let context = crate::util::model_call_context("reranker", "startup_smoke_scoring");
        let _entered = context.enter();
        let smoke_started_at = Instant::now();
        info!(
            event = "model_call.started",
            model_role = "reranker",
            adapter_mode = RERANKER_ADAPTER_MODE,
            call_purpose = "startup_smoke_scoring",
            model_path = %artifacts.root.display(),
            hidden_size = runtime.model.hidden_size(),
            layer_count = runtime.model.layer_count(),
            configured_max_tokens = runtime.max_tokens,
            classifier_pooling = %runtime.classifier_pooling,
            classifier_activation = %runtime.classifier_activation,
            candidate_count = smoke_candidates.len(),
            "reranker startup smoke scoring started"
        );
        let smoke_scores_result = runtime.score_candidates(SMOKE_QUERY, &smoke_candidates);
        match &smoke_scores_result {
            Ok(scores) => {
                // Aggregate over present diagnostics; the local runtime
                // populates every score, so nothing is dropped here.
                let token_counts = scores
                    .iter()
                    .filter_map(|score| score.token_count)
                    .collect::<Vec<_>>();
                let logits = scores
                    .iter()
                    .filter_map(|score| score.logit)
                    .collect::<Vec<_>>();
                let public_scores = scores.iter().map(|score| score.score).collect::<Vec<_>>();
                info!(
                    event = "model_call.completed",
                    model_role = "reranker",
                    adapter_mode = RERANKER_ADAPTER_MODE,
                    call_purpose = "startup_smoke_scoring",
                    model_path = %artifacts.root.display(),
                    hidden_size = runtime.model.hidden_size(),
                    layer_count = runtime.model.layer_count(),
                    configured_max_tokens = runtime.max_tokens,
                    classifier_pooling = %runtime.classifier_pooling,
                    classifier_activation = %runtime.classifier_activation,
                    candidate_count = scores.len(),
                    smoke_token_counts = ?token_counts,
                    smoke_logits = ?logits,
                    smoke_scores = ?public_scores,
                    elapsed_ms = smoke_started_at.elapsed().as_millis() as u64,
                    "reranker startup smoke scoring completed"
                );
            }
            Err(source) => {
                error!(
                    event = "model_call.failed",
                    model_role = "reranker",
                    adapter_mode = RERANKER_ADAPTER_MODE,
                    call_purpose = "startup_smoke_scoring",
                    model_path = %artifacts.root.display(),
                    hidden_size = runtime.model.hidden_size(),
                    layer_count = runtime.model.layer_count(),
                    configured_max_tokens = runtime.max_tokens,
                    classifier_pooling = %runtime.classifier_pooling,
                    classifier_activation = %runtime.classifier_activation,
                    candidate_count = smoke_candidates.len(),
                    elapsed_ms = smoke_started_at.elapsed().as_millis() as u64,
                    error = %source,
                    "reranker startup smoke scoring failed"
                );
            }
        }
        let smoke_scores = smoke_scores_result?;
        let first_smoke = smoke_scores.first().ok_or_else(|| {
            inference_error("reranker smoke candidate set produced no scores".to_string())
        })?;
        let max_token_count = smoke_scores
            .iter()
            .filter_map(|score| score.token_count)
            .max()
            .unwrap_or(0);
        runtime.smoke = RerankerSmoke {
            candidate_count: smoke_scores.len(),
            max_token_count,
            first_score: first_smoke.score,
            first_logit: first_smoke.logit,
        };
        progress("reranker_smoke_ready")?;

        Ok(runtime)
    }

    /// Return reranker readiness details and the ModernBERT classification contract.
    pub fn health_details(&self) -> Vec<String> {
        vec![format!(
            "reranker runtime ready: mode {}, hidden {}, layers {}, max_tokens {}, classifier_pooling {}, classifier_activation {}, smoke_candidates {}, smoke_max_tokens {}, smoke_score {:.6}, smoke_logit {}",
            RERANKER_ADAPTER_MODE,
            self.model.hidden_size(),
            self.model.layer_count(),
            self.max_tokens,
            self.classifier_pooling,
            self.classifier_activation,
            self.smoke.candidate_count,
            self.smoke.max_token_count,
            self.smoke.first_score,
            // The local runtime always records a smoke logit; "absent" only
            // appears for backends that cannot provide one.
            self.smoke
                .first_logit
                .map(|logit| format!("{logit:.6}"))
                .unwrap_or_else(|| "absent".to_string())
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
        let context = crate::util::model_call_context("reranker", "candidate_batch_scoring");
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
            adapter_mode = RERANKER_ADAPTER_MODE,
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
                adapter_mode = RERANKER_ADAPTER_MODE,
                call_purpose = "candidate_batch_scoring",
                input_kind = "query_candidates",
                query_chars,
                candidates = candidates.len(),
                document_chars,
                configured_max_tokens = self.max_tokens,
                total_token_count,
                max_token_count,
                "reranker candidate batch input tokenized"
            );
            let total = tokenized_candidates.len() as u64;
            let mut scores = Vec::with_capacity(tokenized_candidates.len());
            for (index, candidate) in tokenized_candidates.iter().enumerate() {
                let score = self.score_tokenized_candidate(
                    index + 1,
                    tokenized_candidates.len(),
                    query_chars,
                    candidate,
                )?;
                scores.push(score);
                progress(scores.len() as u64, total)?;
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
                    .filter_map(|score| score.token_count)
                    .max()
                    .unwrap_or(0);
                info!(
                    event = "model_call.completed",
                    model_role = "reranker",
                    adapter_mode = RERANKER_ADAPTER_MODE,
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
                    adapter_mode = RERANKER_ADAPTER_MODE,
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

    /// Tokenize query/document pairs with explicit ModernBERT pair markers and no padding.
    fn tokenize_candidates(
        &self,
        query: &str,
        candidates: &[RerankerCandidateInput],
    ) -> Result<Vec<TokenizedRerankerCandidate>, ApiError> {
        let query_tokens = encode_without_special_tokens(&self.tokenizer, query, "reranker query")?;
        if query_tokens.is_empty() {
            return Err(inference_error(
                "reranker query tokenization produced no real tokens".to_string(),
            ));
        }

        candidates
            .iter()
            .map(|candidate| {
                let document_tokens = encode_without_special_tokens(
                    &self.tokenizer,
                    &candidate.content,
                    "reranker document",
                )
                .map_err(|source| {
                    inference_error(format!(
                        "reranker candidate {} tokenization failed: {source}",
                        candidate.unit_id
                    ))
                })?;
                if document_tokens.is_empty() {
                    return Err(inference_error(format!(
                        "reranker candidate {} document tokenization produced no real tokens",
                        candidate.unit_id
                    )));
                }
                let input_ids = build_pair_input_ids(
                    self.cls_token_id,
                    self.sep_token_id,
                    &query_tokens,
                    &document_tokens,
                    self.max_tokens,
                )?;

                Ok(TokenizedRerankerCandidate {
                    unit_id: candidate.unit_id.clone(),
                    input_ids,
                    document_chars: candidate.content.chars().count(),
                })
            })
            .collect()
    }

    /// Score one tokenized query/document pair through the ModernBERT classifier head.
    fn score_tokenized_candidate(
        &self,
        candidate_index: usize,
        candidate_count: usize,
        query_chars: usize,
        candidate: &TokenizedRerankerCandidate,
    ) -> Result<RerankerCandidateScore, ApiError> {
        let context = crate::util::model_call_context("reranker", "candidate_pair_scoring");
        let _entered = context.enter();
        let started_at = Instant::now();
        info!(
            event = "model_call.started",
            model_role = "reranker",
            adapter_mode = RERANKER_ADAPTER_MODE,
            call_purpose = "candidate_pair_scoring",
            input_kind = "query_document_pair",
            candidate_index,
            candidate_count,
            unit_id = %candidate.unit_id,
            query_chars,
            document_chars = candidate.document_chars,
            configured_max_tokens = self.max_tokens,
            token_count = candidate.input_ids.len(),
            "reranker candidate pair scoring started"
        );
        let result = (|| -> Result<RerankerCandidateScore, ApiError> {
            let logit =
                self.model
                    .forward_logit(&candidate.input_ids, &self.device, &candidate.unit_id)?;
            if !logit.is_finite() {
                return Err(inference_error(format!(
                    "reranker candidate {} produced non-finite logit",
                    candidate.unit_id
                )));
            }
            let score = sigmoid(logit)?;

            Ok(RerankerCandidateScore {
                unit_id: candidate.unit_id.clone(),
                score,
                rank: 0,
                logit: Some(logit),
                token_count: Some(candidate.input_ids.len()),
            })
        })();
        match &result {
            Ok(score) => {
                info!(
                    event = "model_call.completed",
                    model_role = "reranker",
                    adapter_mode = RERANKER_ADAPTER_MODE,
                    call_purpose = "candidate_pair_scoring",
                    input_kind = "query_document_pair",
                    candidate_index,
                    candidate_count,
                    unit_id = %candidate.unit_id,
                    query_chars,
                    document_chars = candidate.document_chars,
                    configured_max_tokens = self.max_tokens,
                    token_count = score.token_count,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "reranker candidate pair scoring completed"
                );
            }
            Err(source) => {
                error!(
                    event = "model_call.failed",
                    model_role = "reranker",
                    adapter_mode = RERANKER_ADAPTER_MODE,
                    call_purpose = "candidate_pair_scoring",
                    input_kind = "query_document_pair",
                    candidate_index,
                    candidate_count,
                    unit_id = %candidate.unit_id,
                    query_chars,
                    document_chars = candidate.document_chars,
                    configured_max_tokens = self.max_tokens,
                    token_count = candidate.input_ids.len(),
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    error = %source,
                    "reranker candidate pair scoring failed"
                );
            }
        }

        result
    }
}

impl ModernBertSequenceClassifier {
    /// Load the ModernBERT encoder and classifier tensors from the reranker root safetensors.
    fn load_with_progress(
        artifacts: &ModelArtifacts,
        config: &ModernBertConfig,
        device: &Device,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        progress("reranker_model_memory_mapping")?;
        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(&artifacts.safetensor_paths, DType::F32, device)
        }
        .map_err(|source| {
            inference_error(format!(
                "failed to memory-map reranker ModernBERT safetensors from {}: {source}",
                artifacts.root.display()
            ))
        })?;
        progress("reranker_model_memory_mapped")?;
        progress("reranker_input_path_loading")?;
        let input_path = ModernBertInputPath::load(config, vb.pp("model.embeddings"))?;
        progress("reranker_input_path_ready")?;
        let encoder =
            ModernBertEncoderRuntime::load_with_progress(config, vb.pp("model"), progress)?;
        progress("reranker_classifier_head_loading")?;
        let classifier_head = ModernBertClassifierHead::load(config, vb)?;
        progress("reranker_classifier_head_ready")?;

        Ok(Self {
            input_path,
            encoder,
            classifier_head,
            hidden_size: config.hidden_size,
        })
    }

    /// Return the hidden width for readiness and startup diagnostics.
    fn hidden_size(&self) -> usize {
        self.hidden_size
    }

    /// Return the loaded layer count for readiness and startup diagnostics.
    fn layer_count(&self) -> usize {
        self.encoder.layer_count()
    }

    /// Run one tokenized query/document pair through encoder, mean pooling, and classifier.
    fn forward_logit(
        &self,
        token_ids: &[u32],
        device: &Device,
        label: &str,
    ) -> Result<f32, ApiError> {
        let hidden = self.input_path.forward(token_ids, device, label)?;
        let encoded = self.encoder.encode(&hidden, label)?;
        let pooled = mean_pool_tokens(&encoded, label)?;
        self.classifier_head.forward_logit(&pooled, label)
    }
}

impl ModernBertInputPath {
    /// Load the ModernBERT token embedding and embedding norm tensors used before transformer layers.
    fn load(config: &ModernBertConfig, vb: VarBuilder) -> Result<Self, ApiError> {
        let embeddings = embedding(
            config.vocab_size,
            config.hidden_size,
            vb.pp("tok_embeddings"),
        )
        .map_err(|source| {
            inference_error(format!(
                "failed to load reranker ModernBERT token embeddings: {source}"
            ))
        })?;
        let norm = MetalSafeLayerNorm::load(config.hidden_size, config.norm_eps, vb.pp("norm"))
            .map_err(|source| {
                inference_error(format!(
                    "failed to load reranker ModernBERT embedding norm: {source}"
                ))
            })?;

        Ok(Self {
            embeddings,
            norm,
            vocab_size: config.vocab_size,
            hidden_size: config.hidden_size,
        })
    }

    /// Convert token IDs into normalized ModernBERT input hidden states without running encoder layers.
    fn forward(&self, token_ids: &[u32], device: &Device, label: &str) -> Result<Tensor, ApiError> {
        if token_ids.is_empty() {
            return Err(inference_error(format!(
                "reranker {label} input contains no tokens"
            )));
        }
        if let Some(token_id) = token_ids
            .iter()
            .find(|token_id| **token_id as usize >= self.vocab_size)
        {
            return Err(inference_error(format!(
                "reranker {label} input token {token_id} exceeds vocab size {}",
                self.vocab_size
            )));
        }

        let input = Tensor::new(token_ids, device)
            .map_err(|source| {
                inference_error(format!(
                    "failed to build reranker {label} input tensor: {source}"
                ))
            })?
            .unsqueeze(0)
            .map_err(|source| {
                inference_error(format!(
                    "failed to batch reranker {label} input tensor: {source}"
                ))
            })?;
        let hidden = self.embeddings.forward(&input).map_err(|source| {
            inference_error(format!(
                "reranker {label} token embedding lookup failed: {source}"
            ))
        })?;
        let normalized = self.norm.forward(&hidden).map_err(|source| {
            inference_error(format!("reranker {label} embedding norm failed: {source}"))
        })?;
        let (batch_size, token_count, hidden_size) = normalized.dims3().map_err(|source| {
            inference_error(format!(
                "reranker {label} embedding hidden-state shape error: {source}"
            ))
        })?;
        if batch_size != 1 || hidden_size != self.hidden_size {
            return Err(inference_error(format!(
                "reranker {label} embedding hidden states have shape [{batch_size}, {token_count}, {hidden_size}], expected [1, tokens, {}]",
                self.hidden_size
            )));
        }

        normalized
            .reshape((token_count, hidden_size))
            .map_err(|source| {
                inference_error(format!(
                    "failed to flatten reranker {label} hidden states for encoder: {source}"
                ))
            })
    }
}

impl ModernBertEncoderRuntime {
    /// Load the full ModernBERT encoder stack while reporting layer progress.
    fn load_with_progress(
        config: &ModernBertConfig,
        vb: VarBuilder,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        let mut layers = Vec::with_capacity(config.num_hidden_layers);
        for layer_index in 0..config.num_hidden_layers {
            progress(&format!(
                "reranker_encoder_layer_loading layer={}/{}",
                layer_index + 1,
                config.num_hidden_layers
            ))?;
            layers.push(ModernBertLayerPrimitive::load(
                config,
                vb.pp(format!("layers.{layer_index}")),
                layer_index,
            )?);
        }
        progress(&format!(
            "reranker_encoder_layers_ready count={}",
            config.num_hidden_layers
        ))?;
        progress("reranker_encoder_final_norm_loading")?;
        let final_norm =
            MetalSafeLayerNorm::load(config.hidden_size, config.norm_eps, vb.pp("final_norm"))
                .map_err(|source| {
                    inference_error(format!(
                        "failed to load reranker ModernBERT final norm: {source}"
                    ))
                })?;
        progress("reranker_encoder_final_norm_ready")?;

        Ok(Self {
            layers,
            final_norm,
            hidden_size: config.hidden_size,
        })
    }

    /// Return the number of loaded layers for health diagnostics.
    fn layer_count(&self) -> usize {
        self.layers.len()
    }

    /// Apply every ModernBERT layer followed by final normalization.
    fn encode(&self, hidden_states: &Tensor, label: &str) -> Result<Tensor, ApiError> {
        let (_, hidden_size) = hidden_states.dims2().map_err(|source| {
            inference_error(format!(
                "reranker {label} full-encoder input must be rank-2 hidden states: {source}"
            ))
        })?;
        if hidden_size != self.hidden_size {
            return Err(inference_error(format!(
                "reranker {label} full-encoder hidden size {hidden_size}, expected {}",
                self.hidden_size
            )));
        }

        let mut current = hidden_states.clone();
        for layer in &self.layers {
            current = layer.forward(&current, label)?;
        }
        self.final_norm.forward(&current).map_err(|source| {
            inference_error(format!("reranker {label} final norm failed: {source}"))
        })
    }
}

impl ModernBertLayerPrimitive {
    /// Load one complete ModernBERT layer with attention, MLP norm, and gated MLP projections.
    fn load(
        config: &ModernBertConfig,
        vb: VarBuilder,
        layer_index: usize,
    ) -> Result<Self, ApiError> {
        if layer_index >= config.num_hidden_layers {
            return Err(inference_error(format!(
                "reranker layer {layer_index} is outside {} configured layers",
                config.num_hidden_layers
            )));
        }

        let attn_norm_vb = if layer_index == 0 {
            None
        } else {
            Some(vb.pp("attn_norm"))
        };
        let attention =
            ModernBertAttentionPrimitive::load(config, vb.pp("attn"), attn_norm_vb, layer_index)?;
        let mlp_norm =
            MetalSafeLayerNorm::load(config.hidden_size, config.norm_eps, vb.pp("mlp_norm"))
                .map_err(|source| {
                    inference_error(format!(
                        "failed to load reranker layer {layer_index} MLP norm: {source}"
                    ))
                })?;
        let mlp = ModernBertMlpPrimitive::load(config, vb.pp("mlp"), layer_index)?;

        Ok(Self {
            attention,
            mlp_norm,
            mlp,
            layer_index,
            hidden_size: config.hidden_size,
        })
    }

    /// Apply one bidirectional ModernBERT encoder layer with attention and GELU-gated MLP residuals.
    fn forward(&self, hidden_states: &Tensor, label: &str) -> Result<Tensor, ApiError> {
        let (_, hidden_size) = hidden_states.dims2().map_err(|source| {
            inference_error(format!(
                "reranker {label} layer input must be rank-2 hidden states: {source}"
            ))
        })?;
        if hidden_size != self.hidden_size {
            return Err(inference_error(format!(
                "reranker {label} layer hidden size {hidden_size}, expected {}",
                self.hidden_size
            )));
        }

        let attention_output = self.attention.forward(hidden_states, label)?;
        let hidden_states = (attention_output + hidden_states).map_err(|source| {
            inference_error(format!(
                "reranker {label} attention residual failed for layer {}: {source}",
                self.layer_index
            ))
        })?;
        let mlp_input = self.mlp_norm.forward(&hidden_states).map_err(|source| {
            inference_error(format!(
                "reranker {label} MLP norm failed for layer {}: {source}",
                self.layer_index
            ))
        })?;
        let mlp_output = self.mlp.forward(&mlp_input, label, self.layer_index)?;
        (mlp_output + hidden_states).map_err(|source| {
            inference_error(format!(
                "reranker {label} MLP residual failed for layer {}: {source}",
                self.layer_index
            ))
        })
    }
}

impl ModernBertAttentionPrimitive {
    /// Load one ModernBERT attention block from the layer-local tensor namespace.
    fn load(
        config: &ModernBertConfig,
        vb: VarBuilder,
        attn_norm_vb: Option<VarBuilder>,
        layer_index: usize,
    ) -> Result<Self, ApiError> {
        let qkv_proj = linear_no_bias(config.hidden_size, config.hidden_size * 3, vb.pp("Wqkv"))
            .map_err(|source| {
                inference_error(format!(
                    "failed to load reranker layer {layer_index} Wqkv: {source}"
                ))
            })?;
        let out_proj = linear_no_bias(config.hidden_size, config.hidden_size, vb.pp("Wo"))
            .map_err(|source| {
                inference_error(format!(
                    "failed to load reranker layer {layer_index} Wo: {source}"
                ))
            })?;
        let attn_norm = match attn_norm_vb {
            Some(norm_vb) => Some(
                MetalSafeLayerNorm::load(config.hidden_size, config.norm_eps, norm_vb).map_err(
                    |source| {
                        inference_error(format!(
                            "failed to load reranker layer {layer_index} attention norm: {source}"
                        ))
                    },
                )?,
            ),
            None => None,
        };
        let attention_kind = ModernBertAttentionKind::for_layer(layer_index, config);
        let rope_theta = match attention_kind {
            ModernBertAttentionKind::Global => config.global_rope_theta,
            ModernBertAttentionKind::Local => config.local_rope_theta,
        };

        Ok(Self {
            qkv_proj,
            out_proj,
            attn_norm,
            layer_index,
            attention_kind,
            num_attention_heads: config.num_attention_heads,
            head_dim: config.hidden_size / config.num_attention_heads,
            hidden_size: config.hidden_size,
            local_attention: config.local_attention,
            max_position_embeddings: config.max_position_embeddings,
            rope_theta,
        })
    }

    /// Apply ModernBERT bidirectional attention for one configured layer without running the MLP block.
    fn forward(&self, hidden_states: &Tensor, label: &str) -> Result<Tensor, ApiError> {
        let (seq_len, hidden_size) = hidden_states.dims2().map_err(|source| {
            inference_error(format!(
                "reranker {label} attention input must be rank-2 hidden states: {source}"
            ))
        })?;
        if hidden_size != self.hidden_size {
            return Err(inference_error(format!(
                "reranker {label} attention hidden size {hidden_size}, expected {}",
                self.hidden_size
            )));
        }
        if seq_len > self.max_position_embeddings {
            return Err(inference_error(format!(
                "reranker {label} attention sequence length {seq_len} exceeds max_position_embeddings {}",
                self.max_position_embeddings
            )));
        }

        let attention_input = match &self.attn_norm {
            Some(norm) => {
                let normalized = norm.forward(hidden_states).map_err(|source| {
                    inference_error(format!(
                        "reranker {label} attention norm failed for layer {}: {source}",
                        self.layer_index
                    ))
                })?;
                normalized.reshape((seq_len, hidden_size)).map_err(|source| {
                    inference_error(format!(
                        "failed to flatten reranker {label} attention norm output for layer {}: {source}",
                        self.layer_index
                    ))
                })?
            }
            None => hidden_states.clone(),
        };
        let qkv = self.qkv_proj.forward(&attention_input).map_err(|source| {
            inference_error(format!(
                "reranker {label} fused QKV projection failed for layer {}: {source}",
                self.layer_index
            ))
        })?;
        let qkv = qkv
            .reshape((1, seq_len, hidden_size * 3))
            .map_err(|source| {
                inference_error(format!(
                    "failed to batch reranker {label} fused QKV projection for layer {}: {source}",
                    self.layer_index
                ))
            })?;
        let q = self.split_attention_projection(&qkv, 0, seq_len, label, "query")?;
        let k = self.split_attention_projection(&qkv, hidden_size, seq_len, label, "key")?;
        let v = self.split_attention_projection(&qkv, hidden_size * 2, seq_len, label, "value")?;
        let q = apply_rope(&q, self.rope_theta).map_err(|source| {
            inference_error(format!("reranker {label} query rope failed: {source}"))
        })?;
        let k = apply_rope(&k, self.rope_theta).map_err(|source| {
            inference_error(format!("reranker {label} key rope failed: {source}"))
        })?;
        let attention_output = self.attention_output_by_head(&q, &k, &v, seq_len, label)?;
        let attention_output = attention_output.reshape((seq_len, hidden_size)).map_err(|source| {
            inference_error(format!(
                "failed to flatten reranker {label} attention output before projection: {source}"
            ))
        })?;
        self.out_proj.forward(&attention_output).map_err(|source| {
            inference_error(format!(
                "reranker {label} attention output projection failed for layer {}: {source}",
                self.layer_index
            ))
        })
    }

    /// Compute attention per head with 2D matmuls to stay inside Candle Metal's supported operation shapes.
    fn attention_output_by_head(
        &self,
        q: &Tensor,
        k: &Tensor,
        v: &Tensor,
        seq_len: usize,
        label: &str,
    ) -> Result<Tensor, ApiError> {
        let mut head_outputs = Vec::with_capacity(self.num_attention_heads);
        for head_index in 0..self.num_attention_heads {
            let q_head = self.narrow_head(q, head_index, seq_len, label, "query")?;
            let k_head = self.narrow_head(k, head_index, seq_len, label, "key")?;
            let v_head = self.narrow_head(v, head_index, seq_len, label, "value")?;
            let attention_scores = q_head
                .matmul(&k_head.t().map_err(|source| {
                    inference_error(format!(
                        "reranker {label} key transpose failed for head {head_index}: {source}"
                    ))
                })?)
                .and_then(|tensor| tensor / (self.head_dim as f64).sqrt())
                .map_err(|source| {
                    inference_error(format!(
                        "reranker {label} attention scores failed for head {head_index}: {source}"
                    ))
                })?;
            let attention_scores = match self.attention_kind {
                ModernBertAttentionKind::Global => attention_scores,
                ModernBertAttentionKind::Local => {
                    apply_local_attention_mask(&attention_scores, seq_len, self.local_attention)
                        .map_err(|source| {
                            inference_error(format!(
                                "reranker {label} local attention mask failed for head {head_index}: {source}"
                            ))
                        })?
                }
            };
            let attention_probs =
                softmax_last_dim_metal_safe(&attention_scores).map_err(|source| {
                    inference_error(format!(
                        "reranker {label} attention softmax failed for head {head_index}: {source}"
                    ))
                })?;
            let head_output = attention_probs.matmul(&v_head).map_err(|source| {
                inference_error(format!(
                    "reranker {label} attention output failed for head {head_index}: {source}"
                ))
            })?;
            head_outputs.push(head_output);
        }
        let head_refs = head_outputs.iter().collect::<Vec<_>>();
        Tensor::cat(&head_refs, 1)
            .and_then(|tensor| tensor.reshape((1, seq_len, self.hidden_size)))
            .map_err(|source| {
                inference_error(format!(
                    "reranker {label} attention head merge failed: {source}"
                ))
            })
    }

    /// Extract one attention head as a `[tokens, head_dim]` matrix for the primitive attention path.
    fn narrow_head(
        &self,
        states: &Tensor,
        head_index: usize,
        seq_len: usize,
        label: &str,
        projection_label: &str,
    ) -> Result<Tensor, ApiError> {
        states
            .narrow(1, head_index, 1)
            .and_then(|tensor| tensor.reshape((seq_len, self.head_dim)))
            .map_err(|source| {
                inference_error(format!(
                    "reranker {label} {projection_label} head {head_index} extraction failed: {source}"
                ))
            })
    }

    /// Split one fused ModernBERT QKV projection into `[batch, heads, tokens, head_dim]`.
    fn split_attention_projection(
        &self,
        qkv: &Tensor,
        offset: usize,
        seq_len: usize,
        label: &str,
        projection_label: &str,
    ) -> Result<Tensor, ApiError> {
        qkv.narrow(2, offset, self.hidden_size)
            .and_then(|tensor| {
                tensor.reshape((1, seq_len, self.num_attention_heads, self.head_dim))
            })
            .and_then(|tensor| tensor.transpose(1, 2))
            .map_err(|source| {
                inference_error(format!(
                    "reranker {label} {projection_label} split failed: {source}"
                ))
            })
    }
}

impl ModernBertMlpPrimitive {
    /// Load the fused ModernBERT gated MLP projections for one layer.
    fn load(
        config: &ModernBertConfig,
        vb: VarBuilder,
        layer_index: usize,
    ) -> Result<Self, ApiError> {
        let input_proj = linear_no_bias(
            config.hidden_size,
            config.intermediate_size * 2,
            vb.pp("Wi"),
        )
        .map_err(|source| {
            inference_error(format!(
                "failed to load reranker layer {layer_index} MLP Wi: {source}"
            ))
        })?;
        let output_proj = linear_no_bias(config.intermediate_size, config.hidden_size, vb.pp("Wo"))
            .map_err(|source| {
                inference_error(format!(
                    "failed to load reranker layer {layer_index} MLP Wo: {source}"
                ))
            })?;

        Ok(Self {
            input_proj,
            output_proj,
            hidden_size: config.hidden_size,
            intermediate_size: config.intermediate_size,
        })
    }

    /// Apply ModernBERT's fused GELU-gated feed-forward block after MLP layer normalization.
    fn forward(
        &self,
        hidden_states: &Tensor,
        label: &str,
        layer_index: usize,
    ) -> Result<Tensor, ApiError> {
        let (_, hidden_size) = hidden_states.dims2().map_err(|source| {
            inference_error(format!(
                "reranker {label} MLP input must be rank-2 hidden states: {source}"
            ))
        })?;
        if hidden_size != self.hidden_size {
            return Err(inference_error(format!(
                "reranker {label} MLP hidden size {hidden_size}, expected {}",
                self.hidden_size
            )));
        }

        let projected = self.input_proj.forward(hidden_states).map_err(|source| {
            inference_error(format!(
                "reranker {label} MLP Wi failed for layer {layer_index}: {source}"
            ))
        })?;
        let input = projected
            .narrow(1, 0, self.intermediate_size)
            .map_err(|source| {
                inference_error(format!(
                    "reranker {label} MLP activation split failed for layer {layer_index}: {source}"
                ))
            })?;
        let gate = projected
            .narrow(1, self.intermediate_size, self.intermediate_size)
            .map_err(|source| {
                inference_error(format!(
                    "reranker {label} MLP gate split failed for layer {layer_index}: {source}"
                ))
            })?;
        let activated = input
            .gelu()
            .and_then(|tensor| tensor.mul(&gate))
            .map_err(|source| {
                inference_error(format!(
                    "reranker {label} MLP GELU gate failed for layer {layer_index}: {source}"
                ))
            })?;

        self.output_proj.forward(&activated).map_err(|source| {
            inference_error(format!(
                "reranker {label} MLP Wo failed for layer {layer_index}: {source}"
            ))
        })
    }
}

impl ModernBertClassifierHead {
    /// Load the mean-pooled sequence-classification head from root-level head/classifier tensors.
    fn load(config: &ModernBertConfig, vb: VarBuilder) -> Result<Self, ApiError> {
        let dense = linear_no_bias(config.hidden_size, config.hidden_size, vb.pp("head.dense"))
            .map_err(|source| {
                inference_error(format!(
                    "failed to load reranker classifier head dense layer: {source}"
                ))
            })?;
        let norm =
            MetalSafeLayerNorm::load(config.hidden_size, config.norm_eps, vb.pp("head.norm"))
                .map_err(|source| {
                    inference_error(format!(
                        "failed to load reranker classifier head norm: {source}"
                    ))
                })?;
        let classifier = linear(
            config.hidden_size,
            EXPECTED_LABEL_COUNT,
            vb.pp("classifier"),
        )
        .map_err(|source| {
            inference_error(format!(
                "failed to load reranker classifier output layer: {source}"
            ))
        })?;

        Ok(Self {
            dense,
            norm,
            classifier,
            hidden_size: config.hidden_size,
            label_count: EXPECTED_LABEL_COUNT,
        })
    }

    /// Convert a pooled ModernBERT hidden state into the raw single-label relevance logit.
    fn forward_logit(&self, pooled: &Tensor, label: &str) -> Result<f32, ApiError> {
        let (batch_size, hidden_size) = pooled.dims2().map_err(|source| {
            inference_error(format!(
                "reranker {label} pooled hidden state must be rank-2: {source}"
            ))
        })?;
        if batch_size != 1 || hidden_size != self.hidden_size {
            return Err(inference_error(format!(
                "reranker {label} pooled hidden state has shape [{batch_size}, {hidden_size}], expected [1, {}]",
                self.hidden_size
            )));
        }

        let hidden = self.dense.forward(pooled).map_err(|source| {
            inference_error(format!(
                "reranker {label} classifier dense forward failed: {source}"
            ))
        })?;
        let activated = hidden.gelu().map_err(|source| {
            inference_error(format!(
                "reranker {label} classifier GELU activation failed: {source}"
            ))
        })?;
        let normalized = self.norm.forward(&activated).map_err(|source| {
            inference_error(format!("reranker {label} classifier norm failed: {source}"))
        })?;
        let logits = self.classifier.forward(&normalized).map_err(|source| {
            inference_error(format!(
                "reranker {label} classifier output forward failed: {source}"
            ))
        })?;
        let (logit_batch, label_count) = logits.dims2().map_err(|source| {
            inference_error(format!(
                "reranker {label} classifier output must be rank-2: {source}"
            ))
        })?;
        if logit_batch != 1 || label_count != self.label_count {
            return Err(inference_error(format!(
                "reranker {label} classifier output has shape [{logit_batch}, {label_count}], expected [1, {}]",
                self.label_count
            )));
        }
        let rows = logits
            .to_device(&Device::Cpu)
            .and_then(|tensor| tensor.to_vec2::<f32>())
            .map_err(|source| {
                inference_error(format!(
                    "failed to read reranker {label} classifier output: {source}"
                ))
            })?;
        let Some(row) = rows.first() else {
            return Err(inference_error(format!(
                "reranker {label} classifier output produced no rows"
            )));
        };
        let Some(logit) = row.first() else {
            return Err(inference_error(format!(
                "reranker {label} classifier output produced no logits"
            )));
        };

        Ok(*logit)
    }
}

impl ModernBertAttentionKind {
    /// Resolve ModernBERT's layer policy so global and local attention stay explicit.
    fn for_layer(layer_index: usize, config: &ModernBertConfig) -> Self {
        if layer_index.is_multiple_of(config.global_attn_every_n_layers) {
            Self::Global
        } else {
            Self::Local
        }
    }
}

impl MetalSafeLayerNorm {
    /// Load a bias-free LayerNorm boundary while avoiding Candle fused ops that may be unavailable on Metal.
    fn load(size: usize, eps: f64, vb: VarBuilder) -> candle_core::Result<Self> {
        Ok(Self {
            weight: vb.get(size, "weight")?,
            eps,
        })
    }

    /// Apply bias-free LayerNorm with primitive tensor operations available across configured accelerators.
    fn forward(&self, hidden_states: &Tensor) -> candle_core::Result<Tensor> {
        let mean = hidden_states.mean_keepdim(D::Minus1)?;
        let centered = hidden_states.broadcast_sub(&mean)?;
        let variance = centered.sqr()?.mean_keepdim(D::Minus1)?;
        let normed = centered.broadcast_div(&(variance + self.eps)?.sqrt()?)?;
        normed.broadcast_mul(&self.weight)
    }
}

impl TensorInventory {
    /// Read safetensor headers from one or more files and keep only metadata needed for contract checks.
    fn load_many(paths: &[PathBuf], label: &str) -> Result<Self, ApiError> {
        let mut tensors = HashMap::new();
        for path in paths {
            let bytes = fs::read(path).map_err(|source| {
                inference_error(format!(
                    "failed to read {label} safetensor at {}: {source}",
                    path.display()
                ))
            })?;
            let safetensors = SafeTensors::deserialize(&bytes).map_err(|source| {
                inference_error(format!(
                    "failed to parse {label} safetensor at {}: {source}",
                    path.display()
                ))
            })?;
            for name in safetensors.names() {
                let tensor = safetensors.tensor(name).map_err(|source| {
                    inference_error(format!(
                        "failed to read {label} tensor {name} at {}: {source}",
                        path.display()
                    ))
                })?;
                tensors.insert(
                    name.to_string(),
                    TensorMetadata {
                        dtype: tensor.dtype(),
                        shape: tensor.shape().to_vec(),
                    },
                );
            }
        }

        Ok(Self { tensors })
    }

    /// Require one tensor to exist with the exact shape and dtype expected by the local model adapter.
    fn require_tensor(
        &self,
        name: &str,
        expected_shape: &[usize],
        expected_dtype: SafeTensorDType,
    ) -> Result<(), ApiError> {
        let Some(metadata) = self.tensors.get(name) else {
            return Err(inference_error(format!(
                "reranker safetensors are missing required tensor {name}"
            )));
        };
        if metadata.shape != expected_shape {
            return Err(inference_error(format!(
                "reranker tensor {name} has shape {:?}, expected {:?}",
                metadata.shape, expected_shape
            )));
        }
        if metadata.dtype != expected_dtype {
            return Err(inference_error(format!(
                "reranker tensor {name} has dtype {:?}, expected {:?}",
                metadata.dtype, expected_dtype
            )));
        }

        Ok(())
    }
}

/// Validate reranker service config values that affect pair construction and memory use.
fn validate_reranker_config(max_tokens: u32) -> Result<(), ApiError> {
    if max_tokens as usize <= PAIR_SPECIAL_TOKEN_COUNT + 1 {
        return Err(inference_error(format!(
            "models.reranker.max_tokens must leave room for [CLS] query [SEP] document [SEP], got {max_tokens}"
        )));
    }

    Ok(())
}

/// Disable tokenizer-level padding/truncation so this runtime owns pair truncation explicitly.
fn disable_serialized_tokenizer_limits(tokenizer: &mut Tokenizer) -> Result<(), ApiError> {
    tokenizer.with_padding(None);
    tokenizer.with_truncation(None).map_err(|source| {
        inference_error(format!(
            "failed to disable reranker tokenizer truncation: {source}"
        ))
    })?;

    Ok(())
}

/// Load the ModernBERT config subset needed before encoder implementation.
fn load_modernbert_config(path: &Path) -> Result<ModernBertConfig, ApiError> {
    let raw = fs::read_to_string(path).map_err(|source| {
        inference_error(format!(
            "failed to read reranker ModernBERT config at {}: {source}",
            path.display()
        ))
    })?;
    serde_json::from_str(&raw).map_err(|source| {
        inference_error(format!(
            "failed to parse reranker ModernBERT config at {}: {source}",
            path.display()
        ))
    })
}

/// Ensure the root model config is the expected ModernBERT sequence-classifier contract.
fn validate_modernbert_config(
    model_config: &ModernBertConfig,
    max_tokens: u32,
) -> Result<(), ApiError> {
    if model_config.model_type != EXPECTED_MODEL_TYPE {
        return Err(inference_error(format!(
            "reranker model_type must be {EXPECTED_MODEL_TYPE}, got {}",
            model_config.model_type
        )));
    }
    if !model_config
        .architectures
        .iter()
        .any(|architecture| architecture == EXPECTED_ARCHITECTURE)
    {
        return Err(inference_error(format!(
            "reranker architectures must include {EXPECTED_ARCHITECTURE}"
        )));
    }
    if model_config.hidden_size != EXPECTED_HIDDEN_SIZE {
        return Err(inference_error(format!(
            "reranker hidden_size must be {EXPECTED_HIDDEN_SIZE}, got {}",
            model_config.hidden_size
        )));
    }
    if !model_config
        .hidden_size
        .is_multiple_of(model_config.num_attention_heads)
    {
        return Err(inference_error(
            "reranker hidden_size must divide evenly by num_attention_heads".to_string(),
        ));
    }
    if model_config.attention_bias {
        return Err(inference_error(
            "reranker attention_bias must be false for the bias-free attention adapter".to_string(),
        ));
    }
    if model_config.mlp_bias {
        return Err(inference_error(
            "reranker mlp_bias must be false for the bias-free MLP adapter".to_string(),
        ));
    }
    if model_config.hidden_activation != EXPECTED_HIDDEN_ACTIVATION {
        return Err(inference_error(format!(
            "reranker hidden_activation must be {EXPECTED_HIDDEN_ACTIVATION}, got {}",
            model_config.hidden_activation
        )));
    }
    if model_config.classifier_pooling != EXPECTED_CLASSIFIER_POOLING {
        return Err(inference_error(format!(
            "reranker classifier_pooling must be {EXPECTED_CLASSIFIER_POOLING}, got {}",
            model_config.classifier_pooling
        )));
    }
    if model_config.classifier_activation != EXPECTED_CLASSIFIER_ACTIVATION {
        return Err(inference_error(format!(
            "reranker classifier_activation must be {EXPECTED_CLASSIFIER_ACTIVATION}, got {}",
            model_config.classifier_activation
        )));
    }
    if model_config.id2label.len() != EXPECTED_LABEL_COUNT
        || model_config.label2id.len() != EXPECTED_LABEL_COUNT
    {
        return Err(inference_error(format!(
            "reranker label metadata must contain {EXPECTED_LABEL_COUNT} label"
        )));
    }
    if model_config.vocab_size == 0
        || model_config.num_hidden_layers == 0
        || model_config.intermediate_size == 0
        || model_config.global_attn_every_n_layers == 0
        || model_config.local_attention == 0
        || model_config.max_position_embeddings == 0
    {
        return Err(inference_error(
            "reranker config must have positive vocab, layer, intermediate, attention cadence, local window, and position sizes".to_string(),
        ));
    }
    if !model_config.global_rope_theta.is_finite()
        || model_config.global_rope_theta <= 0.0
        || !model_config.local_rope_theta.is_finite()
        || model_config.local_rope_theta <= 0.0
    {
        return Err(inference_error(
            "reranker RoPE theta values must be finite and greater than zero".to_string(),
        ));
    }
    if max_tokens as usize > model_config.max_position_embeddings {
        return Err(inference_error(format!(
            "models.reranker.max_tokens {} exceeds reranker max_position_embeddings {}",
            max_tokens, model_config.max_position_embeddings
        )));
    }
    for (label, token_id) in [
        ("cls_token_id", model_config.cls_token_id),
        ("sep_token_id", model_config.sep_token_id),
        ("pad_token_id", model_config.pad_token_id),
    ] {
        if token_id as usize >= model_config.vocab_size {
            return Err(inference_error(format!(
                "reranker {label} {token_id} exceeds vocab size {}",
                model_config.vocab_size
            )));
        }
    }
    if model_config.cls_token_id == model_config.sep_token_id
        || model_config.cls_token_id == model_config.pad_token_id
        || model_config.sep_token_id == model_config.pad_token_id
    {
        return Err(inference_error(
            "reranker cls, sep, and pad token IDs must be distinct".to_string(),
        ));
    }

    Ok(())
}

/// Shape-check the ModernBERT sequence-classifier safetensor inventory before Candle maps it.
fn validate_root_safetensors(
    artifacts: &ModelArtifacts,
    config: &ModernBertConfig,
) -> Result<(), ApiError> {
    let inventory = TensorInventory::load_many(&artifacts.safetensor_paths, "reranker root")?;
    inventory.require_tensor(
        "model.embeddings.tok_embeddings.weight",
        &[config.vocab_size, config.hidden_size],
        SafeTensorDType::F32,
    )?;
    inventory.require_tensor(
        "model.embeddings.norm.weight",
        &[config.hidden_size],
        SafeTensorDType::F32,
    )?;
    inventory.require_tensor(
        "model.final_norm.weight",
        &[config.hidden_size],
        SafeTensorDType::F32,
    )?;

    for layer_index in 0..config.num_hidden_layers {
        let layer = format!("model.layers.{layer_index}");
        inventory.require_tensor(
            &format!("{layer}.attn.Wqkv.weight"),
            &[config.hidden_size * 3, config.hidden_size],
            SafeTensorDType::F32,
        )?;
        inventory.require_tensor(
            &format!("{layer}.attn.Wo.weight"),
            &[config.hidden_size, config.hidden_size],
            SafeTensorDType::F32,
        )?;
        inventory.require_tensor(
            &format!("{layer}.mlp.Wi.weight"),
            &[config.intermediate_size * 2, config.hidden_size],
            SafeTensorDType::F32,
        )?;
        inventory.require_tensor(
            &format!("{layer}.mlp.Wo.weight"),
            &[config.hidden_size, config.intermediate_size],
            SafeTensorDType::F32,
        )?;
        inventory.require_tensor(
            &format!("{layer}.mlp_norm.weight"),
            &[config.hidden_size],
            SafeTensorDType::F32,
        )?;
        if layer_index > 0 {
            inventory.require_tensor(
                &format!("{layer}.attn_norm.weight"),
                &[config.hidden_size],
                SafeTensorDType::F32,
            )?;
        }
    }

    inventory.require_tensor(
        "head.dense.weight",
        &[config.hidden_size, config.hidden_size],
        SafeTensorDType::F32,
    )?;
    inventory.require_tensor(
        "head.norm.weight",
        &[config.hidden_size],
        SafeTensorDType::F32,
    )?;
    inventory.require_tensor(
        "classifier.weight",
        &[EXPECTED_LABEL_COUNT, config.hidden_size],
        SafeTensorDType::F32,
    )?;
    inventory.require_tensor(
        "classifier.bias",
        &[EXPECTED_LABEL_COUNT],
        SafeTensorDType::F32,
    )
}

/// Tokenize text without adding special tokens and strip any serialized tokenizer padding defensively.
fn encode_without_special_tokens(
    tokenizer: &Tokenizer,
    text: &str,
    label: &str,
) -> Result<Vec<u32>, ApiError> {
    let encoding = tokenizer
        .encode(text, false)
        .map_err(|source| inference_error(format!("{label} tokenization failed: {source}")))?;
    let ids = encoding.get_ids();
    let mask = encoding.get_attention_mask();
    if ids.len() != mask.len() {
        return Err(inference_error(format!(
            "{label} tokenization produced {} IDs but {} attention-mask values",
            ids.len(),
            mask.len()
        )));
    }

    Ok(ids
        .iter()
        .zip(mask.iter())
        .filter_map(|(token_id, attention)| (*attention != 0).then_some(*token_id))
        .collect())
}

/// Build the explicit ModernBERT pair input `[CLS] query [SEP] document [SEP]`.
/// Preserve the local backend's query-first prefix allocation. The HTTP backend
/// sends complete pairs and surfaces an oversized-input rejection from the engine.
fn build_pair_input_ids(
    cls_token_id: u32,
    sep_token_id: u32,
    query_tokens: &[u32],
    document_tokens: &[u32],
    max_tokens: usize,
) -> Result<Vec<u32>, ApiError> {
    if max_tokens <= PAIR_SPECIAL_TOKEN_COUNT + 1 {
        return Err(inference_error(format!(
            "models.reranker.max_tokens {max_tokens} leaves no room for query/document text after pair special tokens"
        )));
    }
    let content_limit = max_tokens - PAIR_SPECIAL_TOKEN_COUNT;
    let query_limit = query_tokens.len().min(content_limit - 1);
    let document_limit = content_limit - query_limit;
    let mut input_ids = Vec::with_capacity(PAIR_SPECIAL_TOKEN_COUNT + query_limit + document_limit);
    input_ids.push(cls_token_id);
    input_ids.extend(query_tokens.iter().take(query_limit));
    input_ids.push(sep_token_id);
    input_ids.extend(document_tokens.iter().take(document_limit));
    input_ids.push(sep_token_id);

    Ok(input_ids)
}

/// Mean-pool encoded hidden states over the real token dimension for sequence classification.
fn mean_pool_tokens(hidden_states: &Tensor, label: &str) -> Result<Tensor, ApiError> {
    let (token_count, hidden_size) = hidden_states.dims2().map_err(|source| {
        inference_error(format!(
            "reranker {label} mean-pool input must be rank-2 hidden states: {source}"
        ))
    })?;
    if token_count == 0 {
        return Err(inference_error(format!(
            "reranker {label} mean-pool input contains no tokens"
        )));
    }
    let summed = hidden_states.sum_keepdim(D::Minus2).map_err(|source| {
        inference_error(format!("reranker {label} mean-pool sum failed: {source}"))
    })?;
    let pooled = (summed / token_count as f64).map_err(|source| {
        inference_error(format!(
            "reranker {label} mean-pool divide failed: {source}"
        ))
    })?;
    let (batch_size, pooled_hidden_size) = pooled.dims2().map_err(|source| {
        inference_error(format!(
            "reranker {label} mean-pool output must be rank-2: {source}"
        ))
    })?;
    if batch_size != 1 || pooled_hidden_size != hidden_size {
        return Err(inference_error(format!(
            "reranker {label} mean-pool output has shape [{batch_size}, {pooled_hidden_size}], expected [1, {hidden_size}]"
        )));
    }

    Ok(pooled)
}

/// Apply a numerically stable sigmoid to convert the raw classifier logit into the public score.
fn sigmoid(logit: f32) -> Result<f32, ApiError> {
    if !logit.is_finite() {
        return Err(inference_error(
            "reranker classifier logit must be finite".to_string(),
        ));
    }
    let value = logit as f64;
    let score = if value >= 0.0 {
        let denominator = 1.0 + (-value).exp();
        1.0 / denominator
    } else {
        let numerator = value.exp();
        numerator / (1.0 + numerator)
    } as f32;
    if !score.is_finite() {
        return Err(inference_error(
            "reranker sigmoid score must be finite".to_string(),
        ));
    }

    Ok(score)
}

/// Apply ModernBERT's fixed-width local attention mask to one attention-score matrix.
fn apply_local_attention_mask(
    scores: &Tensor,
    seq_len: usize,
    local_attention: usize,
) -> candle_core::Result<Tensor> {
    if local_attention >= seq_len {
        return Ok(scores.clone());
    }

    let device = scores.device();
    let radius = local_attention / 2;
    let mut values = Vec::with_capacity(seq_len * seq_len);
    for row in 0..seq_len {
        for col in 0..seq_len {
            let outside_left = col + radius < row;
            let outside_right = col > row + radius;
            values.push(if outside_left || outside_right {
                f32::NEG_INFINITY
            } else {
                0.0
            });
        }
    }
    let mask = Tensor::from_vec(values, (seq_len, seq_len), device)?.to_dtype(scores.dtype())?;
    scores.broadcast_add(&mask)
}

/// Convert a Candle or tokenizer failure into the service inference error shape.
fn inference_error(message: String) -> ApiError {
    ApiError::InferenceInit { message }
}
