use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

use candle_core::{D, DType, Device, Tensor};
use candle_nn::{Embedding, Linear, Module, VarBuilder, embedding, linear_no_bias};
use safetensors::{Dtype as SafeTensorDType, SafeTensors};
use serde::Deserialize;
use tokenizers::Tokenizer;
use tracing::{error, info};

use crate::{
    config::ColbertModelConfig,
    error::ApiError,
    inference::{
        InferenceProgress,
        artifacts::{CONFIG_FILE_NAME, ModelArtifacts, TOKENIZER_FILE_NAME},
        tensor_ops::{apply_rope, softmax_last_dim_metal_safe},
    },
};

const QUERY_PROMPT: &str = "search_query: ";
const DOCUMENT_PROMPT: &str = "search_document: ";
const QUERY_MARKER: &str = "[Q] ";
const DOCUMENT_MARKER: &str = "[D] ";
const EXPECTED_MODEL_TYPE: &str = "modernbert";
const EXPECTED_ARCHITECTURE: &str = "ModernBertModel";
const EXPECTED_HIDDEN_SIZE: usize = 768;
const EXPECTED_PROJECTION_DIMENSION: usize = 128;
const EXPECTED_TOKENIZER_MAX_LENGTH: usize = 518;
const SMOKE_QUERY: &str = "clear writing style rules";
const SMOKE_DOCUMENT: &str = "Prefer specific words and direct sentences.";
const PROJECTION_DIR_NAME: &str = "1_Dense";
const PROJECTION_MODEL_FILE_NAME: &str = "model.safetensors";
const TOKENIZER_CONFIG_FILE_NAME: &str = "tokenizer_config.json";
const SENTENCE_BERT_CONFIG_FILE_NAME: &str = "sentence_bert_config.json";
const TOKEN_VECTOR_NORM_EPS: f64 = 1e-12;

#[derive(Debug, Clone)]
pub struct ColbertRuntime {
    tokenizer: Tokenizer,
    device: Device,
    input_path: ColbertInputPath,
    projection: ColbertProjection,
    encoder: ColbertEncoderRuntime,
    query_max_tokens: usize,
    document_max_tokens: usize,
    attention_smoke: ColbertAttentionSmoke,
    single_layer_smoke: ColbertLayerSmoke,
    full_encoder_smoke: ColbertFullEncoderSmoke,
    document_capacity_smoke: ColbertDocumentCapacitySmoke,
    query_tokens: usize,
    document_tokens: usize,
    hidden_size: usize,
    projection_dimension: usize,
    num_hidden_layers: usize,
    maxsim_score: f32,
}

#[derive(Debug, Clone)]
pub struct ColbertCandidateScore {
    pub unit_id: String,
    pub score: f32,
    pub rank: usize,
    pub query_tokens: usize,
    pub document_tokens: usize,
}

#[derive(Debug, Clone)]
pub struct ColbertDocumentEmbedding {
    pub unit_id: String,
    pub token_count: usize,
    pub dimension: usize,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone)]
struct ColbertInputPath {
    embeddings: Embedding,
    norm: MetalSafeLayerNorm,
    vocab_size: usize,
    hidden_size: usize,
}

#[derive(Debug, Clone)]
struct ColbertProjection {
    linear: Linear,
    path: PathBuf,
    in_features: usize,
    out_features: usize,
}

#[derive(Debug, Clone)]
struct ColbertAttentionPrimitive {
    qkv_proj: Linear,
    out_proj: Linear,
    attn_norm: Option<MetalSafeLayerNorm>,
    layer_index: usize,
    attention_kind: ColbertAttentionKind,
    num_attention_heads: usize,
    head_dim: usize,
    hidden_size: usize,
    local_attention: usize,
    max_position_embeddings: usize,
    rope_theta: f64,
}

#[derive(Debug, Clone)]
struct ColbertLayerPrimitive {
    attention: ColbertAttentionPrimitive,
    mlp_norm: MetalSafeLayerNorm,
    mlp: ColbertMlpPrimitive,
    layer_index: usize,
    hidden_size: usize,
}

#[derive(Debug, Clone)]
struct ColbertEncoderRuntime {
    layers: Vec<ColbertLayerPrimitive>,
    final_norm: MetalSafeLayerNorm,
    hidden_size: usize,
}

#[derive(Debug, Clone)]
struct ColbertMlpPrimitive {
    input_proj: Linear,
    output_proj: Linear,
    hidden_size: usize,
    intermediate_size: usize,
}

#[derive(Debug, Clone)]
struct ColbertAttentionSmoke {
    layer_index: usize,
    attention_kind: ColbertAttentionKind,
    tokens: usize,
    hidden_size: usize,
    head_dim: usize,
    mean_abs: f32,
    max_abs: f32,
}

#[derive(Debug, Clone)]
struct ColbertLayerSmoke {
    layer_index: usize,
    tokens: usize,
    hidden_size: usize,
    mean_abs: f32,
    max_abs: f32,
}

#[derive(Debug, Clone)]
struct ColbertFullEncoderSmoke {
    query_tokens: usize,
    document_tokens: usize,
    hidden_size: usize,
    projection_dimension: usize,
    maxsim_score: f32,
    query_mean_abs: f32,
    query_max_abs: f32,
    document_mean_abs: f32,
    document_max_abs: f32,
}

#[derive(Debug, Clone)]
struct ColbertDocumentCapacitySmoke {
    document_tokens: usize,
    hidden_size: usize,
    projection_dimension: usize,
    projection_mean_abs: f32,
    projection_max_abs: f32,
}

#[derive(Debug, Clone, Copy)]
enum ColbertAttentionKind {
    Global,
    Local,
}

#[derive(Debug, Clone, Deserialize)]
struct ModernBertConfig {
    attention_bias: bool,
    architectures: Vec<String>,
    global_attn_every_n_layers: usize,
    global_rope_theta: f64,
    hidden_activation: String,
    hidden_size: usize,
    intermediate_size: usize,
    local_attention: usize,
    local_rope_theta: f64,
    max_position_embeddings: usize,
    mlp_bias: bool,
    model_type: String,
    norm_eps: f64,
    num_attention_heads: usize,
    num_hidden_layers: usize,
    vocab_size: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct PyLateProjectionConfig {
    in_features: usize,
    out_features: usize,
    bias: bool,
    activation_function: String,
    use_residual: bool,
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

#[derive(Debug, Clone, Copy)]
struct ColbertStartupCallDiagnostics {
    call_purpose: &'static str,
    query_tokens: Option<usize>,
    document_tokens: Option<usize>,
    document_capacity_tokens: Option<usize>,
    configured_query_max_tokens: usize,
    configured_document_max_tokens: usize,
    hidden_size: usize,
    projection_dimension: usize,
    layer_count: usize,
    attention_kind: Option<&'static str>,
}

impl ColbertRuntime {
    /// Load ColBERT while reporting tokenizer, encoder, projection, and smoke-check progress.
    pub fn load_with_progress(
        artifacts: &ModelArtifacts,
        config: &ColbertModelConfig,
        device: &Device,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        validate_colbert_config(config)?;

        progress("colbert_tokenizer_loading")?;
        let tokenizer = Tokenizer::from_file(&artifacts.tokenizer_path).map_err(|source| {
            inference_error(format!(
                "failed to load ColBERT tokenizer at {}: {source}",
                artifacts.tokenizer_path.display()
            ))
        })?;
        progress("colbert_tokenizer_ready")?;
        progress("colbert_config_loading")?;
        let model_config = load_modernbert_config(&artifacts.config_path)?;
        validate_modernbert_config(&model_config, config)?;
        progress("colbert_config_ready")?;
        progress("colbert_tokenizer_contract_validating")?;
        validate_tokenizer_contract(artifacts, config)?;
        progress("colbert_tokenizer_contract_ready")?;
        progress("colbert_safetensors_validating")?;
        validate_root_safetensors(artifacts, &model_config)?;
        progress("colbert_safetensors_ready")?;
        progress("colbert_input_path_loading")?;
        let input_path = ColbertInputPath::load(artifacts, &model_config, device)?;
        progress("colbert_input_path_ready")?;
        progress("colbert_projection_loading")?;
        let projection = ColbertProjection::load(artifacts, config, device)?;
        progress("colbert_projection_ready")?;
        progress("colbert_encoder_loading")?;
        let encoder =
            ColbertEncoderRuntime::load_with_progress(artifacts, &model_config, device, progress)?;
        progress("colbert_encoder_ready")?;

        let smoke_base = ColbertStartupCallDiagnostics {
            call_purpose: "startup_smoke",
            query_tokens: None,
            document_tokens: None,
            document_capacity_tokens: None,
            configured_query_max_tokens: config.query_max_tokens as usize,
            configured_document_max_tokens: config.document_max_tokens as usize,
            hidden_size: model_config.hidden_size,
            projection_dimension: config.dimension as usize,
            layer_count: model_config.num_hidden_layers,
            attention_kind: None,
        };
        progress("colbert_smoke_tokenizing")?;
        let tokenizing_started_at = Instant::now();
        log_colbert_startup_call_started(ColbertStartupCallDiagnostics {
            call_purpose: "startup_smoke_tokenizing",
            ..smoke_base
        });
        let tokenizing_result: Result<(Vec<u32>, Vec<u32>), ApiError> = (|| {
            let query_token_ids = tokenize_formatted(
                &tokenizer,
                &format_query(SMOKE_QUERY),
                config.query_max_tokens as usize,
                "query",
            )?;
            let document_token_ids = tokenize_formatted(
                &tokenizer,
                &format_document(SMOKE_DOCUMENT),
                config.document_max_tokens as usize,
                "document",
            )?;

            Ok((query_token_ids, document_token_ids))
        })();
        match &tokenizing_result {
            Ok((query_token_ids, document_token_ids)) => {
                log_colbert_startup_call_completed(
                    ColbertStartupCallDiagnostics {
                        call_purpose: "startup_smoke_tokenizing",
                        query_tokens: Some(query_token_ids.len()),
                        document_tokens: Some(document_token_ids.len()),
                        ..smoke_base
                    },
                    tokenizing_started_at,
                );
            }
            Err(source) => {
                log_colbert_startup_call_failed(
                    ColbertStartupCallDiagnostics {
                        call_purpose: "startup_smoke_tokenizing",
                        ..smoke_base
                    },
                    tokenizing_started_at,
                    source,
                );
            }
        }
        let (query_token_ids, document_token_ids) = tokenizing_result?;
        let smoke_tokens = ColbertStartupCallDiagnostics {
            query_tokens: Some(query_token_ids.len()),
            document_tokens: Some(document_token_ids.len()),
            ..smoke_base
        };
        progress("colbert_smoke_input_path")?;
        let (query_hidden, document_hidden) = run_colbert_startup_call(
            ColbertStartupCallDiagnostics {
                call_purpose: "startup_smoke_input_path",
                ..smoke_tokens
            },
            || {
                let query_hidden = input_path.forward(&query_token_ids, device, "query")?;
                let document_hidden =
                    input_path.forward(&document_token_ids, device, "document")?;

                Ok((query_hidden, document_hidden))
            },
        )?;
        progress("colbert_smoke_projection")?;
        let maxsim_score = run_colbert_startup_call(
            ColbertStartupCallDiagnostics {
                call_purpose: "startup_smoke_projection_maxsim",
                ..smoke_tokens
            },
            || {
                let query_projection = projection.project(&query_hidden)?;
                let document_projection = projection.project(&document_hidden)?;
                maxsim_score(&query_projection, &document_projection)
            },
        )?;
        let first_layer = encoder.first_layer()?;
        progress("colbert_smoke_attention")?;
        let attention_smoke = run_colbert_startup_call(
            ColbertStartupCallDiagnostics {
                call_purpose: "startup_smoke_attention",
                attention_kind: Some(first_layer.attention.attention_kind.label()),
                ..smoke_tokens
            },
            || first_layer.attention.smoke(&query_hidden, "query"),
        )?;
        progress("colbert_smoke_single_layer")?;
        let single_layer_smoke = run_colbert_startup_call(
            ColbertStartupCallDiagnostics {
                call_purpose: "startup_smoke_single_layer",
                attention_kind: Some(first_layer.attention.attention_kind.label()),
                ..smoke_tokens
            },
            || first_layer.smoke(&query_hidden, "query"),
        )?;
        progress("colbert_smoke_full_encoder")?;
        let full_encoder_smoke = run_colbert_startup_call(
            ColbertStartupCallDiagnostics {
                call_purpose: "startup_smoke_full_encoder",
                ..smoke_tokens
            },
            || encoder.smoke(&query_hidden, &document_hidden, &projection),
        )?;
        progress("colbert_smoke_document_capacity")?;
        let capacity_document_token_ids = repeat_token_ids_to_capacity(
            &document_token_ids,
            config.document_max_tokens as usize,
            "document capacity",
        )?;
        let document_capacity_smoke = run_colbert_startup_call(
            ColbertStartupCallDiagnostics {
                call_purpose: "startup_smoke_document_capacity",
                document_capacity_tokens: Some(capacity_document_token_ids.len()),
                ..smoke_tokens
            },
            || {
                let capacity_document_hidden = input_path.forward(
                    &capacity_document_token_ids,
                    device,
                    "document capacity",
                )?;
                encoder.document_capacity_smoke(&capacity_document_hidden, &projection)
            },
        )?;
        progress("colbert_smoke_ready")?;

        Ok(Self {
            tokenizer,
            device: device.clone(),
            input_path,
            projection,
            encoder,
            query_max_tokens: config.query_max_tokens as usize,
            document_max_tokens: config.document_max_tokens as usize,
            attention_smoke,
            single_layer_smoke,
            full_encoder_smoke,
            document_capacity_smoke,
            query_tokens: query_token_ids.len(),
            document_tokens: document_token_ids.len(),
            hidden_size: model_config.hidden_size,
            projection_dimension: config.dimension as usize,
            num_hidden_layers: model_config.num_hidden_layers,
            maxsim_score,
        })
    }

    /// Return ColBERT readiness details while distinguishing partial smoke checks from the full encoder runtime.
    pub fn health_details(&self) -> Vec<String> {
        vec![
            format!(
                "colbert input path ready: architecture {}, layers {}, vocab {}, hidden {}, embedding_norm_eps {}, projection {}->{}, query_tokens {}, document_tokens {}, embedding_path_maxsim {:.6}, projection_path {}",
                EXPECTED_ARCHITECTURE,
                self.num_hidden_layers,
                self.input_path.vocab_size,
                self.hidden_size,
                self.input_path.norm.eps,
                self.projection.in_features,
                self.projection_dimension,
                self.query_tokens,
                self.document_tokens,
                self.maxsim_score,
                self.projection.path.display()
            ),
            format!(
                "colbert attention primitive ready: layer {}, kind {}, tokens {}, hidden {}, head_dim {}, mean_abs {:.6}, max_abs {:.6}",
                self.attention_smoke.layer_index,
                self.attention_smoke.attention_kind.label(),
                self.attention_smoke.tokens,
                self.attention_smoke.hidden_size,
                self.attention_smoke.head_dim,
                self.attention_smoke.mean_abs,
                self.attention_smoke.max_abs
            ),
            format!(
                "colbert single layer ready: layer {}, tokens {}, hidden {}, mean_abs {:.6}, max_abs {:.6}",
                self.single_layer_smoke.layer_index,
                self.single_layer_smoke.tokens,
                self.single_layer_smoke.hidden_size,
                self.single_layer_smoke.mean_abs,
                self.single_layer_smoke.max_abs
            ),
            format!(
                "colbert full encoder ready: layers {}, query_tokens {}, document_tokens {}, hidden {}, projection {}, maxsim {:.6}, query_mean_abs {:.6}, query_max_abs {:.6}, document_mean_abs {:.6}, document_max_abs {:.6}",
                self.encoder.layer_count(),
                self.full_encoder_smoke.query_tokens,
                self.full_encoder_smoke.document_tokens,
                self.full_encoder_smoke.hidden_size,
                self.full_encoder_smoke.projection_dimension,
                self.full_encoder_smoke.maxsim_score,
                self.full_encoder_smoke.query_mean_abs,
                self.full_encoder_smoke.query_max_abs,
                self.full_encoder_smoke.document_mean_abs,
                self.full_encoder_smoke.document_max_abs
            ),
            format!(
                "colbert document capacity ready: document_tokens {}, hidden {}, projection {}, projection_mean_abs {:.6}, projection_max_abs {:.6}",
                self.document_capacity_smoke.document_tokens,
                self.document_capacity_smoke.hidden_size,
                self.document_capacity_smoke.projection_dimension,
                self.document_capacity_smoke.projection_mean_abs,
                self.document_capacity_smoke.projection_max_abs
            ),
        ]
    }

    /// Encode and flatten one unit's ColBERT document token matrix for durable storage.
    pub fn embed_document(
        &self,
        unit_id: &str,
        document: &str,
    ) -> Result<ColbertDocumentEmbedding, ApiError> {
        let started_at = Instant::now();
        let document_chars = document.chars().count();
        info!(
            event = "model_call.started",
            model_role = "colbert",
            call_purpose = "document_embedding",
            input_kind = "document",
            unit_id,
            text_count = 1usize,
            document_chars,
            configured_max_tokens = self.document_max_tokens,
            expected_dimension = self.projection_dimension,
            "ColBERT document embedding started"
        );
        let result = self
            .encode_projected_document(document)
            .and_then(|document_projection| {
                tensor_to_document_embedding(unit_id, document_projection)
            });
        match &result {
            Ok(embedding) => {
                info!(
                    event = "model_call.completed",
                    model_role = "colbert",
                    call_purpose = "document_embedding",
                    input_kind = "document",
                    unit_id,
                    text_count = 1usize,
                    document_chars,
                    token_count = embedding.token_count,
                    configured_max_tokens = self.document_max_tokens,
                    vector_dimension = embedding.dimension,
                    vector_values = embedding.vector.len(),
                    expected_dimension = self.projection_dimension,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "ColBERT document embedding completed"
                );
            }
            Err(source) => {
                error!(
                    event = "model_call.failed",
                    model_role = "colbert",
                    call_purpose = "document_embedding",
                    input_kind = "document",
                    unit_id,
                    text_count = 1usize,
                    document_chars,
                    configured_max_tokens = self.document_max_tokens,
                    expected_dimension = self.projection_dimension,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    error = %source,
                    "ColBERT document embedding failed"
                );
            }
        }

        result
    }

    /// Score persisted ColBERT document token vectors without per-candidate progress reporting.
    pub fn score_persisted_candidates(
        &self,
        query: &str,
        candidates: &[ColbertDocumentEmbedding],
    ) -> Result<Vec<ColbertCandidateScore>, ApiError> {
        self.score_persisted_candidates_with_progress(query, candidates, |_, _| Ok(()))
    }

    /// Score persisted ColBERT document token vectors while reporting completed candidates.
    pub fn score_persisted_candidates_with_progress<F>(
        &self,
        query: &str,
        candidates: &[ColbertDocumentEmbedding],
        mut progress: F,
    ) -> Result<Vec<ColbertCandidateScore>, ApiError>
    where
        F: FnMut(u64, u64) -> Result<(), ApiError>,
    {
        let started_at = Instant::now();
        let query_chars = query.chars().count();
        let document_tokens = candidates
            .iter()
            .map(|candidate| candidate.token_count)
            .sum::<usize>();
        let document_vector_values = candidates
            .iter()
            .map(|candidate| candidate.vector.len())
            .sum::<usize>();
        info!(
            event = "model_call.started",
            model_role = "colbert",
            call_purpose = "persisted_candidate_scoring",
            input_kind = "query_candidates",
            query_chars,
            candidates = candidates.len(),
            document_tokens,
            document_vector_values,
            query_max_tokens = self.query_max_tokens,
            document_max_tokens = self.document_max_tokens,
            expected_dimension = self.projection_dimension,
            "ColBERT persisted candidate scoring started"
        );
        let mut query_tokens_for_log = 0usize;
        let result = (|| -> Result<Vec<ColbertCandidateScore>, ApiError> {
            let query_projection = self.encode_projected_query(query)?;
            let (query_tokens, _) = query_projection.dims2().map_err(|source| {
                inference_error(format!(
                    "ColBERT search query projection shape error: {source}"
                ))
            })?;
            query_tokens_for_log = query_tokens;
            let total = candidates.len() as u64;
            let mut scores = Vec::with_capacity(candidates.len());
            for (index, candidate) in candidates.iter().enumerate() {
                let document_projection = Tensor::from_vec(
                    candidate.vector.clone(),
                    (candidate.token_count, candidate.dimension),
                    &self.device,
                )
                .map_err(|source| {
                    inference_error(format!(
                        "failed to load persisted ColBERT document vector {} onto device: {source}",
                        candidate.unit_id
                    ))
                })?;
                let score = maxsim_score(&query_projection, &document_projection)?;
                scores.push(ColbertCandidateScore {
                    unit_id: candidate.unit_id.clone(),
                    score,
                    rank: 0,
                    query_tokens,
                    document_tokens: candidate.token_count,
                });
                progress((index + 1) as u64, total)?;
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
                info!(
                    event = "model_call.completed",
                    model_role = "colbert",
                    call_purpose = "persisted_candidate_scoring",
                    input_kind = "query_candidates",
                    query_chars,
                    candidates = candidates.len(),
                    scores = scores.len(),
                    query_tokens = query_tokens_for_log,
                    document_tokens,
                    document_vector_values,
                    query_max_tokens = self.query_max_tokens,
                    document_max_tokens = self.document_max_tokens,
                    expected_dimension = self.projection_dimension,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "ColBERT persisted candidate scoring completed"
                );
            }
            Err(source) => {
                error!(
                    event = "model_call.failed",
                    model_role = "colbert",
                    call_purpose = "persisted_candidate_scoring",
                    input_kind = "query_candidates",
                    query_chars,
                    candidates = candidates.len(),
                    query_tokens = query_tokens_for_log,
                    document_tokens,
                    document_vector_values,
                    query_max_tokens = self.query_max_tokens,
                    document_max_tokens = self.document_max_tokens,
                    expected_dimension = self.projection_dimension,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    error = %source,
                    "ColBERT persisted candidate scoring failed"
                );
            }
        }

        result
    }

    /// Encode and project one search query using the ColBERT prompt contract.
    fn encode_projected_query(&self, query: &str) -> Result<Tensor, ApiError> {
        let token_ids = tokenize_formatted(
            &self.tokenizer,
            &format_query(query),
            self.query_max_tokens,
            "search query",
        )?;
        self.encode_projected_tokens(&token_ids, "search query")
    }

    /// Encode and project one candidate unit as a ColBERT document.
    fn encode_projected_document(&self, document: &str) -> Result<Tensor, ApiError> {
        let token_ids = tokenize_formatted(
            &self.tokenizer,
            &format_document(document),
            self.document_max_tokens,
            "search document",
        )?;
        self.encode_projected_tokens(&token_ids, "search document")
    }

    /// Run token IDs through input embeddings, the full encoder, and PyLate projection.
    fn encode_projected_tokens(&self, token_ids: &[u32], label: &str) -> Result<Tensor, ApiError> {
        let hidden = self.input_path.forward(token_ids, &self.device, label)?;
        let encoded = self.encoder.encode(&hidden, label)?;
        self.projection.project(&encoded)
    }
}

impl ColbertInputPath {
    /// Load the ModernBERT token embedding and embedding norm tensors used before transformer layers.
    fn load(
        artifacts: &ModelArtifacts,
        config: &ModernBertConfig,
        device: &Device,
    ) -> Result<Self, ApiError> {
        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(&artifacts.safetensor_paths, DType::F32, device)
        }
        .map_err(|source| {
            inference_error(format!(
                "failed to memory-map ColBERT root safetensors from {}: {source}",
                artifacts.root.display()
            ))
        })?;
        let embeddings = embedding(
            config.vocab_size,
            config.hidden_size,
            vb.pp("embeddings.tok_embeddings"),
        )
        .map_err(|source| {
            inference_error(format!("failed to load ColBERT token embeddings: {source}"))
        })?;
        let norm = MetalSafeLayerNorm::load(
            config.hidden_size,
            config.norm_eps,
            vb.pp("embeddings.norm"),
        )
        .map_err(|source| {
            inference_error(format!("failed to load ColBERT embedding norm: {source}"))
        })?;

        Ok(Self {
            embeddings,
            norm,
            vocab_size: config.vocab_size,
            hidden_size: config.hidden_size,
        })
    }

    /// Convert real token IDs into normalized ModernBERT input hidden states without running encoder layers.
    fn forward(&self, token_ids: &[u32], device: &Device, label: &str) -> Result<Tensor, ApiError> {
        let input = Tensor::new(token_ids, device)
            .map_err(|source| {
                inference_error(format!(
                    "failed to build ColBERT {label} input tensor: {source}"
                ))
            })?
            .unsqueeze(0)
            .map_err(|source| {
                inference_error(format!(
                    "failed to batch ColBERT {label} input tensor: {source}"
                ))
            })?;
        let hidden = self.embeddings.forward(&input).map_err(|source| {
            inference_error(format!(
                "ColBERT {label} token embedding lookup failed: {source}"
            ))
        })?;
        let normalized = self.norm.forward(&hidden).map_err(|source| {
            inference_error(format!("ColBERT {label} embedding norm failed: {source}"))
        })?;
        let (batch_size, token_count, hidden_size) = normalized.dims3().map_err(|source| {
            inference_error(format!(
                "ColBERT {label} embedding hidden-state shape error: {source}"
            ))
        })?;
        if batch_size != 1 || hidden_size != self.hidden_size {
            return Err(inference_error(format!(
                "ColBERT {label} embedding hidden states have shape [{batch_size}, {token_count}, {hidden_size}], expected [1, tokens, {}]",
                self.hidden_size
            )));
        }

        normalized
            .reshape((token_count, hidden_size))
            .map_err(|source| {
                inference_error(format!(
                    "failed to flatten ColBERT {label} hidden states for projection: {source}"
                ))
            })
    }
}

impl ColbertAttentionPrimitive {
    /// Load one ModernBERT attention block so startup can verify the real QKV/output projection path.
    fn load(
        artifacts: &ModelArtifacts,
        config: &ModernBertConfig,
        layer_index: usize,
        device: &Device,
    ) -> Result<Self, ApiError> {
        if layer_index >= config.num_hidden_layers {
            return Err(inference_error(format!(
                "ColBERT attention smoke layer {layer_index} is outside {} configured layers",
                config.num_hidden_layers
            )));
        }

        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(&artifacts.safetensor_paths, DType::F32, device)
        }
        .map_err(|source| {
            inference_error(format!(
                "failed to memory-map ColBERT attention tensors from {}: {source}",
                artifacts.root.display()
            ))
        })?;
        let layer_vb = vb.pp(format!("layers.{layer_index}"));
        let qkv_proj = linear_no_bias(
            config.hidden_size,
            config.hidden_size * 3,
            layer_vb.pp("attn.Wqkv"),
        )
        .map_err(|source| {
            inference_error(format!(
                "failed to load ColBERT layer {layer_index} Wqkv: {source}"
            ))
        })?;
        let out_proj = linear_no_bias(
            config.hidden_size,
            config.hidden_size,
            layer_vb.pp("attn.Wo"),
        )
        .map_err(|source| {
            inference_error(format!(
                "failed to load ColBERT layer {layer_index} Wo: {source}"
            ))
        })?;
        let attn_norm = if layer_index == 0 {
            None
        } else {
            Some(
                MetalSafeLayerNorm::load(
                    config.hidden_size,
                    config.norm_eps,
                    layer_vb.pp("attn_norm"),
                )
                .map_err(|source| {
                    inference_error(format!(
                        "failed to load ColBERT layer {layer_index} attention norm: {source}"
                    ))
                })?,
            )
        };
        let attention_kind = ColbertAttentionKind::for_layer(layer_index, config);
        let rope_theta = match attention_kind {
            ColbertAttentionKind::Global => config.global_rope_theta,
            ColbertAttentionKind::Local => config.local_rope_theta,
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

    /// Run the one-layer attention primitive and summarize shape plus finite-value diagnostics.
    fn smoke(
        &self,
        hidden_states: &Tensor,
        label: &str,
    ) -> Result<ColbertAttentionSmoke, ApiError> {
        let output = self.forward(hidden_states, label)?;
        let (tokens, hidden_size) = output.dims2().map_err(|source| {
            inference_error(format!(
                "ColBERT {label} attention smoke output shape error: {source}"
            ))
        })?;
        let (mean_abs, max_abs) = tensor_abs_summary(&output, label, "attention output")?;

        Ok(ColbertAttentionSmoke {
            layer_index: self.layer_index,
            attention_kind: self.attention_kind,
            tokens,
            hidden_size,
            head_dim: self.head_dim,
            mean_abs,
            max_abs,
        })
    }

    /// Apply ModernBERT bidirectional attention for one configured layer without running the MLP block.
    fn forward(&self, hidden_states: &Tensor, label: &str) -> Result<Tensor, ApiError> {
        let (seq_len, hidden_size) = hidden_states.dims2().map_err(|source| {
            inference_error(format!(
                "ColBERT {label} attention input must be rank-2 hidden states: {source}"
            ))
        })?;
        if hidden_size != self.hidden_size {
            return Err(inference_error(format!(
                "ColBERT {label} attention hidden size {hidden_size}, expected {}",
                self.hidden_size
            )));
        }
        if seq_len > self.max_position_embeddings {
            return Err(inference_error(format!(
                "ColBERT {label} attention sequence length {seq_len} exceeds max_position_embeddings {}",
                self.max_position_embeddings
            )));
        }

        let attention_input = match &self.attn_norm {
            Some(norm) => {
                let normalized = norm.forward(hidden_states).map_err(|source| {
                    inference_error(format!(
                        "ColBERT {label} attention norm failed for layer {}: {source}",
                        self.layer_index
                    ))
                })?;
                normalized.reshape((seq_len, hidden_size)).map_err(|source| {
                    inference_error(format!(
                        "failed to flatten ColBERT {label} attention norm output for layer {}: {source}",
                        self.layer_index
                    ))
                })?
            }
            None => hidden_states.clone(),
        };
        let qkv = self.qkv_proj.forward(&attention_input).map_err(|source| {
            inference_error(format!(
                "ColBERT {label} fused QKV projection failed for layer {}: {source}",
                self.layer_index
            ))
        })?;
        let qkv = qkv
            .reshape((1, seq_len, hidden_size * 3))
            .map_err(|source| {
                inference_error(format!(
                    "failed to batch ColBERT {label} fused QKV projection for layer {}: {source}",
                    self.layer_index
                ))
            })?;
        let q = self.split_attention_projection(&qkv, 0, seq_len, label, "query")?;
        let k = self.split_attention_projection(&qkv, hidden_size, seq_len, label, "key")?;
        let v = self.split_attention_projection(&qkv, hidden_size * 2, seq_len, label, "value")?;
        let q = apply_rope(&q, self.rope_theta).map_err(|source| {
            inference_error(format!("ColBERT {label} query rope failed: {source}"))
        })?;
        let k = apply_rope(&k, self.rope_theta).map_err(|source| {
            inference_error(format!("ColBERT {label} key rope failed: {source}"))
        })?;
        let attention_output = self.attention_output_by_head(&q, &k, &v, seq_len, label)?;
        let attention_output = attention_output.reshape((seq_len, hidden_size)).map_err(
            |source| {
                inference_error(format!(
                    "failed to flatten ColBERT {label} attention output before projection: {source}"
                ))
            },
        )?;
        self.out_proj.forward(&attention_output).map_err(|source| {
            inference_error(format!(
                "ColBERT {label} attention output projection failed for layer {}: {source}",
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
                        "ColBERT {label} key transpose failed for head {head_index}: {source}"
                    ))
                })?)
                .and_then(|tensor| tensor / (self.head_dim as f64).sqrt())
                .map_err(|source| {
                    inference_error(format!(
                        "ColBERT {label} attention scores failed for head {head_index}: {source}"
                    ))
                })?;
            let attention_scores = match self.attention_kind {
                ColbertAttentionKind::Global => attention_scores,
                ColbertAttentionKind::Local => {
                    apply_local_attention_mask(&attention_scores, seq_len, self.local_attention)
                        .map_err(|source| {
                            inference_error(format!(
                                "ColBERT {label} local attention mask failed for head {head_index}: {source}"
                            ))
                        })?
                }
            };
            let attention_probs =
                softmax_last_dim_metal_safe(&attention_scores).map_err(|source| {
                    inference_error(format!(
                        "ColBERT {label} attention softmax failed for head {head_index}: {source}"
                    ))
                })?;
            let head_output = attention_probs.matmul(&v_head).map_err(|source| {
                inference_error(format!(
                    "ColBERT {label} attention output failed for head {head_index}: {source}"
                ))
            })?;
            head_outputs.push(head_output);
        }
        let head_refs = head_outputs.iter().collect::<Vec<_>>();
        Tensor::cat(&head_refs, 1)
            .and_then(|tensor| tensor.reshape((1, seq_len, self.hidden_size)))
            .map_err(|source| {
                inference_error(format!(
                    "ColBERT {label} attention head merge failed: {source}"
                ))
            })
    }

    /// Extract one attention head as a `[tokens, head_dim]` matrix for the primitive smoke path.
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
                    "ColBERT {label} {projection_label} head {head_index} extraction failed: {source}"
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
                    "ColBERT {label} {projection_label} split failed: {source}"
                ))
            })
    }
}

impl ColbertLayerPrimitive {
    /// Load one complete ModernBERT layer so startup can verify attention, MLP, and residual boundaries together.
    fn load(
        artifacts: &ModelArtifacts,
        config: &ModernBertConfig,
        layer_index: usize,
        device: &Device,
    ) -> Result<Self, ApiError> {
        if layer_index >= config.num_hidden_layers {
            return Err(inference_error(format!(
                "ColBERT single-layer smoke layer {layer_index} is outside {} configured layers",
                config.num_hidden_layers
            )));
        }

        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(&artifacts.safetensor_paths, DType::F32, device)
        }
        .map_err(|source| {
            inference_error(format!(
                "failed to memory-map ColBERT layer tensors from {}: {source}",
                artifacts.root.display()
            ))
        })?;
        let layer_vb = vb.pp(format!("layers.{layer_index}"));
        let attention = ColbertAttentionPrimitive::load(artifacts, config, layer_index, device)?;
        let mlp_norm =
            MetalSafeLayerNorm::load(config.hidden_size, config.norm_eps, layer_vb.pp("mlp_norm"))
                .map_err(|source| {
                    inference_error(format!(
                        "failed to load ColBERT layer {layer_index} MLP norm: {source}"
                    ))
                })?;
        let mlp = ColbertMlpPrimitive::load(config, layer_vb.pp("mlp"), layer_index)?;

        Ok(Self {
            attention,
            mlp_norm,
            mlp,
            layer_index,
            hidden_size: config.hidden_size,
        })
    }

    /// Run one complete ModernBERT layer and summarize shape plus finite-value diagnostics.
    fn smoke(&self, hidden_states: &Tensor, label: &str) -> Result<ColbertLayerSmoke, ApiError> {
        let output = self.forward(hidden_states, label)?;
        let (tokens, hidden_size) = output.dims2().map_err(|source| {
            inference_error(format!(
                "ColBERT {label} single-layer smoke output shape error: {source}"
            ))
        })?;
        let (mean_abs, max_abs) = tensor_abs_summary(&output, label, "single-layer output")?;

        Ok(ColbertLayerSmoke {
            layer_index: self.layer_index,
            tokens,
            hidden_size,
            mean_abs,
            max_abs,
        })
    }

    /// Apply one bidirectional ModernBERT encoder layer with attention and GELU-gated MLP residuals.
    fn forward(&self, hidden_states: &Tensor, label: &str) -> Result<Tensor, ApiError> {
        let (_, hidden_size) = hidden_states.dims2().map_err(|source| {
            inference_error(format!(
                "ColBERT {label} single-layer input must be rank-2 hidden states: {source}"
            ))
        })?;
        if hidden_size != self.hidden_size {
            return Err(inference_error(format!(
                "ColBERT {label} single-layer hidden size {hidden_size}, expected {}",
                self.hidden_size
            )));
        }

        let attention_output = self.attention.forward(hidden_states, label)?;
        let hidden_states = (attention_output + hidden_states).map_err(|source| {
            inference_error(format!(
                "ColBERT {label} attention residual failed for layer {}: {source}",
                self.layer_index
            ))
        })?;
        let mlp_input = self.mlp_norm.forward(&hidden_states).map_err(|source| {
            inference_error(format!(
                "ColBERT {label} MLP norm failed for layer {}: {source}",
                self.layer_index
            ))
        })?;
        let mlp_output = self.mlp.forward(&mlp_input, label, self.layer_index)?;
        (mlp_output + hidden_states).map_err(|source| {
            inference_error(format!(
                "ColBERT {label} MLP residual failed for layer {}: {source}",
                self.layer_index
            ))
        })
    }
}

impl ColbertEncoderRuntime {
    /// Load the full ModernBERT encoder stack while reporting layer progress.
    fn load_with_progress(
        artifacts: &ModelArtifacts,
        config: &ModernBertConfig,
        device: &Device,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        progress("colbert_encoder_memory_mapping")?;
        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(&artifacts.safetensor_paths, DType::F32, device)
        }
        .map_err(|source| {
            inference_error(format!(
                "failed to memory-map ColBERT encoder tensors from {}: {source}",
                artifacts.root.display()
            ))
        })?;
        progress("colbert_encoder_memory_mapped")?;
        let mut layers = Vec::with_capacity(config.num_hidden_layers);
        for layer_index in 0..config.num_hidden_layers {
            progress(&format!(
                "colbert_encoder_layer_loading layer={}/{}",
                layer_index + 1,
                config.num_hidden_layers
            ))?;
            layers.push(ColbertLayerPrimitive::load(
                artifacts,
                config,
                layer_index,
                device,
            )?);
        }
        progress(&format!(
            "colbert_encoder_layers_ready count={}",
            config.num_hidden_layers
        ))?;
        progress("colbert_encoder_final_norm_loading")?;
        let final_norm =
            MetalSafeLayerNorm::load(config.hidden_size, config.norm_eps, vb.pp("final_norm"))
                .map_err(|source| {
                    inference_error(format!("failed to load ColBERT final norm: {source}"))
                })?;
        progress("colbert_encoder_final_norm_ready")?;

        Ok(Self {
            layers,
            final_norm,
            hidden_size: config.hidden_size,
        })
    }

    /// Return the first layer so startup can keep the narrower primitive smoke diagnostics.
    fn first_layer(&self) -> Result<&ColbertLayerPrimitive, ApiError> {
        self.layers.first().ok_or_else(|| {
            inference_error("ColBERT encoder has no layers after initialization".to_string())
        })
    }

    /// Return the number of loaded layers for health diagnostics.
    fn layer_count(&self) -> usize {
        self.layers.len()
    }

    /// Run query and document smoke texts through the full encoder, projection, and MaxSim path.
    fn smoke(
        &self,
        query_hidden: &Tensor,
        document_hidden: &Tensor,
        projection: &ColbertProjection,
    ) -> Result<ColbertFullEncoderSmoke, ApiError> {
        let query_encoded = self.encode(query_hidden, "query")?;
        let document_encoded = self.encode(document_hidden, "document")?;
        let query_projection = projection.project(&query_encoded)?;
        let document_projection = projection.project(&document_encoded)?;
        let maxsim_score = maxsim_score(&query_projection, &document_projection)?;
        let (query_tokens, query_projection_dimension) =
            query_projection.dims2().map_err(|source| {
                inference_error(format!(
                    "ColBERT full-encoder query projection shape error: {source}"
                ))
            })?;
        let (document_tokens, document_projection_dimension) =
            document_projection.dims2().map_err(|source| {
                inference_error(format!(
                    "ColBERT full-encoder document projection shape error: {source}"
                ))
            })?;
        if query_projection_dimension != projection.out_features
            || document_projection_dimension != projection.out_features
        {
            return Err(inference_error(format!(
                "ColBERT full-encoder projection dimensions were query={} document={}, expected {}",
                query_projection_dimension, document_projection_dimension, projection.out_features
            )));
        }
        let (query_mean_abs, query_max_abs) =
            tensor_abs_summary(&query_encoded, "query", "full-encoder output")?;
        let (document_mean_abs, document_max_abs) =
            tensor_abs_summary(&document_encoded, "document", "full-encoder output")?;

        Ok(ColbertFullEncoderSmoke {
            query_tokens,
            document_tokens,
            hidden_size: self.hidden_size,
            projection_dimension: projection.out_features,
            maxsim_score,
            query_mean_abs,
            query_max_abs,
            document_mean_abs,
            document_max_abs,
        })
    }

    /// Run a max-capacity document through the real encoder path so readiness covers long-sequence Metal kernels.
    fn document_capacity_smoke(
        &self,
        document_hidden: &Tensor,
        projection: &ColbertProjection,
    ) -> Result<ColbertDocumentCapacitySmoke, ApiError> {
        let document_encoded = self.encode(document_hidden, "document capacity")?;
        let document_projection = projection.project(&document_encoded)?;
        let (document_tokens, projection_dimension) =
            document_projection.dims2().map_err(|source| {
                inference_error(format!(
                    "ColBERT document capacity projection shape error: {source}"
                ))
            })?;
        if projection_dimension != projection.out_features {
            return Err(inference_error(format!(
                "ColBERT document capacity projection dimension was {projection_dimension}, expected {}",
                projection.out_features
            )));
        }
        let (projection_mean_abs, projection_max_abs) = tensor_abs_summary(
            &document_projection,
            "document capacity",
            "projection output",
        )?;

        Ok(ColbertDocumentCapacitySmoke {
            document_tokens,
            hidden_size: self.hidden_size,
            projection_dimension,
            projection_mean_abs,
            projection_max_abs,
        })
    }

    /// Apply every ModernBERT layer followed by final normalization, preserving the runtime's synchronous surface.
    fn encode(&self, hidden_states: &Tensor, label: &str) -> Result<Tensor, ApiError> {
        let (_, hidden_size) = hidden_states.dims2().map_err(|source| {
            inference_error(format!(
                "ColBERT {label} full-encoder input must be rank-2 hidden states: {source}"
            ))
        })?;
        if hidden_size != self.hidden_size {
            return Err(inference_error(format!(
                "ColBERT {label} full-encoder hidden size {hidden_size}, expected {}",
                self.hidden_size
            )));
        }

        let mut current = hidden_states.clone();
        for layer in &self.layers {
            current = layer.forward(&current, label)?;
        }
        self.final_norm.forward(&current).map_err(|source| {
            inference_error(format!("ColBERT {label} final norm failed: {source}"))
        })
    }
}

impl ColbertMlpPrimitive {
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
                "failed to load ColBERT layer {layer_index} MLP Wi: {source}"
            ))
        })?;
        let output_proj = linear_no_bias(config.intermediate_size, config.hidden_size, vb.pp("Wo"))
            .map_err(|source| {
                inference_error(format!(
                    "failed to load ColBERT layer {layer_index} MLP Wo: {source}"
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
                "ColBERT {label} MLP input must be rank-2 hidden states: {source}"
            ))
        })?;
        if hidden_size != self.hidden_size {
            return Err(inference_error(format!(
                "ColBERT {label} MLP hidden size {hidden_size}, expected {}",
                self.hidden_size
            )));
        }

        let projected = self.input_proj.forward(hidden_states).map_err(|source| {
            inference_error(format!(
                "ColBERT {label} MLP Wi failed for layer {layer_index}: {source}"
            ))
        })?;
        let input = projected
            .narrow(1, 0, self.intermediate_size)
            .map_err(|source| {
                inference_error(format!(
                    "ColBERT {label} MLP activation split failed for layer {layer_index}: {source}"
                ))
            })?;
        let gate = projected
            .narrow(1, self.intermediate_size, self.intermediate_size)
            .map_err(|source| {
                inference_error(format!(
                    "ColBERT {label} MLP gate split failed for layer {layer_index}: {source}"
                ))
            })?;
        let activated = input
            .gelu()
            .and_then(|tensor| tensor.mul(&gate))
            .map_err(|source| {
                inference_error(format!(
                    "ColBERT {label} MLP GELU gate failed for layer {layer_index}: {source}"
                ))
            })?;

        self.output_proj.forward(&activated).map_err(|source| {
            inference_error(format!(
                "ColBERT {label} MLP Wo failed for layer {layer_index}: {source}"
            ))
        })
    }
}

impl ColbertAttentionKind {
    /// Resolve ModernBERT's layer policy so global and local attention stay explicit.
    fn for_layer(layer_index: usize, config: &ModernBertConfig) -> Self {
        if layer_index.is_multiple_of(config.global_attn_every_n_layers) {
            Self::Global
        } else {
            Self::Local
        }
    }

    /// Return a stable health diagnostic label for the attention policy.
    fn label(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Local => "local",
        }
    }
}

impl ColbertProjection {
    /// Load the nested PyLate projection artifact that maps ModernBERT hidden states into ColBERT token vectors.
    fn load(
        artifacts: &ModelArtifacts,
        config: &ColbertModelConfig,
        device: &Device,
    ) -> Result<Self, ApiError> {
        let projection_dir = artifacts.root.join(PROJECTION_DIR_NAME);
        let projection_config_path = projection_dir.join(CONFIG_FILE_NAME);
        let projection_path = projection_dir.join(PROJECTION_MODEL_FILE_NAME);
        let projection_config = load_projection_config(&projection_config_path)?;
        validate_projection_config(&projection_config, config)?;
        validate_projection_safetensor(&projection_path, &projection_config)?;
        let paths = vec![projection_path.clone()];
        let vb = unsafe { VarBuilder::from_mmaped_safetensors(&paths, DType::F32, device) }
            .map_err(|source| {
                inference_error(format!(
                    "failed to memory-map ColBERT projection at {}: {source}",
                    projection_path.display()
                ))
            })?;
        let linear = linear_no_bias(
            projection_config.in_features,
            projection_config.out_features,
            vb.pp("linear"),
        )
        .map_err(|source| {
            inference_error(format!(
                "failed to load ColBERT projection linear weight: {source}"
            ))
        })?;

        Ok(Self {
            linear,
            path: projection_path,
            in_features: projection_config.in_features,
            out_features: projection_config.out_features,
        })
    }

    /// Apply the projection boundary and L2-normalize each token vector for ColBERT MaxSim.
    fn project(&self, hidden_states: &Tensor) -> Result<Tensor, ApiError> {
        let (_, hidden_size) = hidden_states.dims2().map_err(|source| {
            inference_error(format!(
                "ColBERT projection input must be rank-2 hidden states: {source}"
            ))
        })?;
        if hidden_size != self.in_features {
            return Err(inference_error(format!(
                "ColBERT projection input hidden size {hidden_size}, expected {}",
                self.in_features
            )));
        }

        let projected = self.linear.forward(hidden_states).map_err(|source| {
            inference_error(format!("ColBERT projection forward failed: {source}"))
        })?;
        let (_, projected_size) = projected.dims2().map_err(|source| {
            inference_error(format!(
                "ColBERT projection output must be rank-2 token vectors: {source}"
            ))
        })?;
        if projected_size != self.out_features {
            return Err(inference_error(format!(
                "ColBERT projection output dimension {projected_size}, expected {}",
                self.out_features
            )));
        }

        l2_normalize_last_dim(&projected, "projection output")
    }
}

/// Normalize vectors across the final dimension so MaxSim uses cosine-like ColBERT token scores.
fn l2_normalize_last_dim(tensor: &Tensor, label: &str) -> Result<Tensor, ApiError> {
    let squared_norm = tensor
        .sqr()
        .and_then(|values| values.sum_keepdim(D::Minus1));
    let denominator = squared_norm
        .and_then(|values| (values + TOKEN_VECTOR_NORM_EPS)?.sqrt())
        .map_err(|source| inference_error(format!("ColBERT {label} norm failed: {source}")))?;
    tensor.broadcast_div(&denominator).map_err(|source| {
        inference_error(format!("ColBERT {label} normalization failed: {source}"))
    })
}

#[derive(Debug, Clone)]
struct MetalSafeLayerNorm {
    weight: Tensor,
    eps: f64,
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

/// Validate ColBERT service config values that are tied to the local model contract.
fn validate_colbert_config(config: &ColbertModelConfig) -> Result<(), ApiError> {
    if config.dimension as usize != EXPECTED_PROJECTION_DIMENSION {
        return Err(inference_error(format!(
            "models.colbert.dimension must be {EXPECTED_PROJECTION_DIMENSION}, got {}",
            config.dimension
        )));
    }

    Ok(())
}

/// Load the ModernBERT config subset needed before encoder implementation.
fn load_modernbert_config(path: &Path) -> Result<ModernBertConfig, ApiError> {
    let raw = fs::read_to_string(path).map_err(|source| {
        inference_error(format!(
            "failed to read ColBERT config at {}: {source}",
            path.display()
        ))
    })?;
    serde_json::from_str(&raw).map_err(|source| {
        inference_error(format!(
            "failed to parse ColBERT config at {}: {source}",
            path.display()
        ))
    })
}

/// Ensure the root model config is the expected ModernBERT contract for ColBERT-Zero.
fn validate_modernbert_config(
    model_config: &ModernBertConfig,
    config: &ColbertModelConfig,
) -> Result<(), ApiError> {
    if model_config.model_type != EXPECTED_MODEL_TYPE {
        return Err(inference_error(format!(
            "ColBERT model_type must be {EXPECTED_MODEL_TYPE}, got {}",
            model_config.model_type
        )));
    }
    if !model_config
        .architectures
        .iter()
        .any(|architecture| architecture == EXPECTED_ARCHITECTURE)
    {
        return Err(inference_error(format!(
            "ColBERT architectures must include {EXPECTED_ARCHITECTURE}"
        )));
    }
    if model_config.hidden_size != EXPECTED_HIDDEN_SIZE {
        return Err(inference_error(format!(
            "ColBERT hidden_size must be {EXPECTED_HIDDEN_SIZE}, got {}",
            model_config.hidden_size
        )));
    }
    if !model_config
        .hidden_size
        .is_multiple_of(model_config.num_attention_heads)
    {
        return Err(inference_error(
            "ColBERT hidden_size must divide evenly by num_attention_heads".to_string(),
        ));
    }
    if model_config.attention_bias {
        return Err(inference_error(
            "ColBERT attention_bias must be false for the bias-free attention adapter".to_string(),
        ));
    }
    if model_config.mlp_bias {
        return Err(inference_error(
            "ColBERT mlp_bias must be false for the bias-free MLP adapter".to_string(),
        ));
    }
    if model_config.hidden_activation != "gelu" {
        return Err(inference_error(format!(
            "ColBERT hidden_activation must be gelu, got {}",
            model_config.hidden_activation
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
            "ColBERT config must have positive vocab, layer, intermediate, attention cadence, local window, and position sizes".to_string(),
        ));
    }
    if !model_config.global_rope_theta.is_finite()
        || model_config.global_rope_theta <= 0.0
        || !model_config.local_rope_theta.is_finite()
        || model_config.local_rope_theta <= 0.0
    {
        return Err(inference_error(
            "ColBERT RoPE theta values must be finite and greater than zero".to_string(),
        ));
    }
    if config.query_max_tokens as usize > EXPECTED_TOKENIZER_MAX_LENGTH
        || config.document_max_tokens as usize > EXPECTED_TOKENIZER_MAX_LENGTH
    {
        return Err(inference_error(format!(
            "models.colbert query/document max tokens must be <= {EXPECTED_TOKENIZER_MAX_LENGTH}"
        )));
    }

    Ok(())
}

/// Validate tokenizer metadata so later encoder phases share the same truncation contract.
fn validate_tokenizer_contract(
    artifacts: &ModelArtifacts,
    config: &ColbertModelConfig,
) -> Result<(), ApiError> {
    let tokenizer_label = format!("ColBERT {TOKENIZER_FILE_NAME}");
    let tokenizer_json = read_json_file(&artifacts.tokenizer_path, &tokenizer_label)?;
    let tokenizer_max = json_usize_at(
        &tokenizer_json,
        &["truncation", "max_length"],
        "tokenizer.json truncation.max_length",
    )?;
    let tokenizer_config_path = artifacts.root.join(TOKENIZER_CONFIG_FILE_NAME);
    let tokenizer_config = read_json_file(&tokenizer_config_path, "ColBERT tokenizer_config.json")?;
    let tokenizer_config_max = json_usize_at(
        &tokenizer_config,
        &["model_max_length"],
        "tokenizer_config.json model_max_length",
    )?;
    let sentence_config_path = artifacts.root.join(SENTENCE_BERT_CONFIG_FILE_NAME);
    let sentence_config =
        read_json_file(&sentence_config_path, "ColBERT sentence_bert_config.json")?;
    let sentence_max = json_usize_at(
        &sentence_config,
        &["max_seq_length"],
        "sentence_bert_config.json max_seq_length",
    )?;

    for (label, value) in [
        ("tokenizer.json truncation.max_length", tokenizer_max),
        (
            "tokenizer_config.json model_max_length",
            tokenizer_config_max,
        ),
        ("sentence_bert_config.json max_seq_length", sentence_max),
    ] {
        if value != EXPECTED_TOKENIZER_MAX_LENGTH {
            return Err(inference_error(format!(
                "ColBERT {label} must be {EXPECTED_TOKENIZER_MAX_LENGTH}, got {value}"
            )));
        }
    }
    if config.query_max_tokens as usize > tokenizer_max
        || config.document_max_tokens as usize > tokenizer_max
    {
        return Err(inference_error(format!(
            "models.colbert query/document max tokens must be <= tokenizer max length {tokenizer_max}"
        )));
    }

    Ok(())
}

/// Shape-check the root ModernBERT safetensor inventory without loading the encoder graph.
fn validate_root_safetensors(
    artifacts: &ModelArtifacts,
    config: &ModernBertConfig,
) -> Result<(), ApiError> {
    let inventory = TensorInventory::load_many(&artifacts.safetensor_paths, "ColBERT root")?;
    inventory.require_tensor(
        "embeddings.tok_embeddings.weight",
        &[config.vocab_size, config.hidden_size],
        SafeTensorDType::F32,
    )?;
    inventory.require_tensor(
        "embeddings.norm.weight",
        &[config.hidden_size],
        SafeTensorDType::F32,
    )?;
    inventory.require_tensor(
        "final_norm.weight",
        &[config.hidden_size],
        SafeTensorDType::F32,
    )?;

    for layer_index in 0..config.num_hidden_layers {
        let layer = format!("layers.{layer_index}");
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

    Ok(())
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
                "ColBERT safetensors are missing required tensor {name}"
            )));
        };
        if metadata.shape != expected_shape {
            return Err(inference_error(format!(
                "ColBERT tensor {name} has shape {:?}, expected {:?}",
                metadata.shape, expected_shape
            )));
        }
        if metadata.dtype != expected_dtype {
            return Err(inference_error(format!(
                "ColBERT tensor {name} has dtype {:?}, expected {:?}",
                metadata.dtype, expected_dtype
            )));
        }

        Ok(())
    }
}

/// Load the PyLate projection config that defines the public ColBERT vector boundary.
fn load_projection_config(path: &Path) -> Result<PyLateProjectionConfig, ApiError> {
    let raw = fs::read_to_string(path).map_err(|source| {
        inference_error(format!(
            "failed to read ColBERT projection config at {}: {source}",
            path.display()
        ))
    })?;
    serde_json::from_str(&raw).map_err(|source| {
        inference_error(format!(
            "failed to parse ColBERT projection config at {}: {source}",
            path.display()
        ))
    })
}

/// Validate that the nested PyLate projection is the expected bias-free identity linear layer.
fn validate_projection_config(
    projection_config: &PyLateProjectionConfig,
    config: &ColbertModelConfig,
) -> Result<(), ApiError> {
    if projection_config.in_features != EXPECTED_HIDDEN_SIZE {
        return Err(inference_error(format!(
            "ColBERT projection in_features must be {EXPECTED_HIDDEN_SIZE}, got {}",
            projection_config.in_features
        )));
    }
    if projection_config.out_features != config.dimension as usize {
        return Err(inference_error(format!(
            "ColBERT projection out_features must match models.colbert.dimension {}, got {}",
            config.dimension, projection_config.out_features
        )));
    }
    if projection_config.bias {
        return Err(inference_error(
            "ColBERT projection bias must be false".to_string(),
        ));
    }
    if projection_config.activation_function != "torch.nn.modules.linear.Identity" {
        return Err(inference_error(format!(
            "ColBERT projection activation must be Identity, got {}",
            projection_config.activation_function
        )));
    }
    if projection_config.use_residual {
        return Err(inference_error(
            "ColBERT projection use_residual must be false".to_string(),
        ));
    }

    Ok(())
}

/// Validate the nested projection safetensor before Candle maps it onto the accelerator.
fn validate_projection_safetensor(
    path: &Path,
    config: &PyLateProjectionConfig,
) -> Result<(), ApiError> {
    let inventory = TensorInventory::load_many(&[path.to_path_buf()], "ColBERT projection")?;
    inventory.require_tensor(
        "linear.weight",
        &[config.out_features, config.in_features],
        SafeTensorDType::F32,
    )
}

/// Read a JSON file as a generic value for small metadata assertions.
fn read_json_file(path: &Path, label: &str) -> Result<serde_json::Value, ApiError> {
    let raw = fs::read_to_string(path).map_err(|source| {
        inference_error(format!(
            "failed to read {label} at {}: {source}",
            path.display()
        ))
    })?;
    serde_json::from_str(&raw).map_err(|source| {
        inference_error(format!(
            "failed to parse {label} at {}: {source}",
            path.display()
        ))
    })
}

/// Extract a positive integer from a nested JSON path used by tokenizer metadata.
fn json_usize_at(value: &serde_json::Value, path: &[&str], label: &str) -> Result<usize, ApiError> {
    let mut current = value;
    for segment in path {
        current = current
            .get(segment)
            .ok_or_else(|| inference_error(format!("ColBERT metadata is missing {label}")))?;
    }
    let Some(number) = current.as_u64() else {
        return Err(inference_error(format!(
            "ColBERT metadata {label} must be an integer"
        )));
    };
    if number == 0 {
        return Err(inference_error(format!(
            "ColBERT metadata {label} must be greater than zero"
        )));
    }

    Ok(number as usize)
}

/// Apply the ColBERT-Zero query prompt and marker inside the runtime boundary.
fn format_query(text: &str) -> String {
    format!("{QUERY_PROMPT}{QUERY_MARKER}{text}")
}

/// Apply the ColBERT-Zero document prompt and marker inside the runtime boundary.
fn format_document(text: &str) -> String {
    format!("{DOCUMENT_PROMPT}{DOCUMENT_MARKER}{text}")
}

/// Tokenize one formatted ColBERT text and apply the configured service truncation.
fn tokenize_formatted(
    tokenizer: &Tokenizer,
    text: &str,
    max_tokens: usize,
    label: &str,
) -> Result<Vec<u32>, ApiError> {
    let encoding = tokenizer.encode(text, true).map_err(|source| {
        inference_error(format!("ColBERT {label} tokenization failed: {source}"))
    })?;
    let mut ids = encoding.get_ids().to_vec();
    ids.truncate(max_tokens);
    if ids.is_empty() {
        return Err(inference_error(format!(
            "ColBERT {label} tokenization produced no tokens"
        )));
    }

    Ok(ids)
}

/// Repeat valid smoke token IDs to the configured capacity so startup tests the longest supported sequence shape.
fn repeat_token_ids_to_capacity(
    seed: &[u32],
    capacity: usize,
    label: &str,
) -> Result<Vec<u32>, ApiError> {
    if capacity == 0 {
        return Err(inference_error(format!(
            "ColBERT {label} token capacity must be greater than zero"
        )));
    }
    if seed.is_empty() {
        return Err(inference_error(format!(
            "ColBERT {label} seed tokenization produced no tokens"
        )));
    }

    let mut ids = Vec::with_capacity(capacity);
    while ids.len() < capacity {
        let remaining = capacity - ids.len();
        ids.extend(seed.iter().copied().take(remaining));
    }

    Ok(ids)
}

/// Apply ModernBERT's local bidirectional window mask to attention scores.
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

/// Verify that one smoke tensor is finite and return compact magnitude diagnostics.
fn tensor_abs_summary(
    tensor: &Tensor,
    label: &str,
    tensor_label: &str,
) -> Result<(f32, f32), ApiError> {
    let rows = tensor
        .to_device(&Device::Cpu)
        .and_then(|tensor| tensor.to_vec2::<f32>())
        .map_err(|source| {
            inference_error(format!(
                "failed to read ColBERT {label} {tensor_label} for diagnostics: {source}"
            ))
        })?;
    let mut count = 0usize;
    let mut total_abs = 0.0f32;
    let mut max_abs = 0.0f32;
    for row in rows {
        for value in row {
            if !value.is_finite() {
                return Err(inference_error(format!(
                    "ColBERT {label} {tensor_label} produced a non-finite value"
                )));
            }
            let abs = value.abs();
            total_abs += abs;
            max_abs = max_abs.max(abs);
            count += 1;
        }
    }
    if count == 0 {
        return Err(inference_error(format!(
            "ColBERT {label} {tensor_label} produced no values"
        )));
    }

    Ok((total_abs / count as f32, max_abs))
}

/// Move a ColBERT document token matrix to CPU and flatten it for SQLite persistence.
fn tensor_to_document_embedding(
    unit_id: &str,
    tensor: Tensor,
) -> Result<ColbertDocumentEmbedding, ApiError> {
    let (token_count, dimension) = tensor.dims2().map_err(|source| {
        inference_error(format!(
            "ColBERT document projection shape error for {unit_id}: {source}"
        ))
    })?;
    if token_count == 0 || dimension == 0 {
        return Err(inference_error(format!(
            "ColBERT document projection for {unit_id} has invalid shape [{token_count}, {dimension}]"
        )));
    }
    let rows = tensor
        .to_device(&Device::Cpu)
        .and_then(|value| value.to_vec2::<f32>())
        .map_err(|source| {
            inference_error(format!(
                "failed to materialize ColBERT document projection for {unit_id}: {source}"
            ))
        })?;
    let mut vector = Vec::with_capacity(token_count * dimension);
    for row in rows {
        if row.len() != dimension {
            return Err(inference_error(format!(
                "ColBERT document projection for {unit_id} has ragged row length {}, expected {dimension}",
                row.len()
            )));
        }
        for value in row {
            if !value.is_finite() {
                return Err(inference_error(format!(
                    "ColBERT document projection for {unit_id} contains non-finite values"
                )));
            }
            vector.push(value);
        }
    }

    Ok(ColbertDocumentEmbedding {
        unit_id: unit_id.to_string(),
        token_count,
        dimension,
        vector,
    })
}

/// Compute ColBERT MaxSim by summing each query token's best document-token dot product.
fn maxsim_score(query_vectors: &Tensor, document_vectors: &Tensor) -> Result<f32, ApiError> {
    let (query_tokens, _) = query_vectors.dims2().map_err(|source| {
        inference_error(format!("ColBERT query projection shape error: {source}"))
    })?;
    let (document_tokens, _) = document_vectors.dims2().map_err(|source| {
        inference_error(format!("ColBERT document projection shape error: {source}"))
    })?;
    if query_tokens == 0 || document_tokens == 0 {
        return Err(inference_error(
            "ColBERT MaxSim requires non-empty query and document token vectors".to_string(),
        ));
    }

    let scores = query_vectors
        .matmul(&document_vectors.t().map_err(|source| {
            inference_error(format!(
                "ColBERT document projection transpose failed: {source}"
            ))
        })?)
        .and_then(|tensor| tensor.to_device(&Device::Cpu))
        .and_then(|tensor| tensor.to_vec2::<f32>())
        .map_err(|source| inference_error(format!("ColBERT MaxSim matrix failed: {source}")))?;
    let mut total = 0.0f32;
    for row in scores {
        let best = row.into_iter().fold(f32::NEG_INFINITY, f32::max);
        if !best.is_finite() {
            return Err(inference_error(
                "ColBERT MaxSim produced a non-finite token score".to_string(),
            ));
        }
        total += best;
    }
    if !total.is_finite() {
        return Err(inference_error(
            "ColBERT MaxSim produced a non-finite total score".to_string(),
        ));
    }

    Ok(total)
}

/// Run one ColBERT startup smoke boundary with durable R5-shaped lifecycle logs.
fn run_colbert_startup_call<T, F>(
    diagnostics: ColbertStartupCallDiagnostics,
    call: F,
) -> Result<T, ApiError>
where
    F: FnOnce() -> Result<T, ApiError>,
{
    let started_at = Instant::now();
    log_colbert_startup_call_started(diagnostics);
    let result = call();
    match &result {
        Ok(_) => log_colbert_startup_call_completed(diagnostics, started_at),
        Err(source) => log_colbert_startup_call_failed(diagnostics, started_at, source),
    }

    result
}

/// Log the start of a ColBERT startup smoke boundary without document or token payloads.
fn log_colbert_startup_call_started(diagnostics: ColbertStartupCallDiagnostics) {
    info!(
        event = "model_call.started",
        model_role = "colbert",
        call_purpose = diagnostics.call_purpose,
        query_tokens = ?diagnostics.query_tokens,
        document_tokens = ?diagnostics.document_tokens,
        document_capacity_tokens = ?diagnostics.document_capacity_tokens,
        configured_query_max_tokens = diagnostics.configured_query_max_tokens,
        configured_document_max_tokens = diagnostics.configured_document_max_tokens,
        hidden_size = diagnostics.hidden_size,
        projection_dimension = diagnostics.projection_dimension,
        layer_count = diagnostics.layer_count,
        attention_kind = ?diagnostics.attention_kind,
        "ColBERT startup smoke boundary started"
    );
}

/// Log successful completion of a ColBERT startup smoke boundary with compact shape facts.
fn log_colbert_startup_call_completed(
    diagnostics: ColbertStartupCallDiagnostics,
    started_at: Instant,
) {
    info!(
        event = "model_call.completed",
        model_role = "colbert",
        call_purpose = diagnostics.call_purpose,
        query_tokens = ?diagnostics.query_tokens,
        document_tokens = ?diagnostics.document_tokens,
        document_capacity_tokens = ?diagnostics.document_capacity_tokens,
        configured_query_max_tokens = diagnostics.configured_query_max_tokens,
        configured_document_max_tokens = diagnostics.configured_document_max_tokens,
        hidden_size = diagnostics.hidden_size,
        projection_dimension = diagnostics.projection_dimension,
        layer_count = diagnostics.layer_count,
        attention_kind = ?diagnostics.attention_kind,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "ColBERT startup smoke boundary completed"
    );
}

/// Log failure of a ColBERT startup smoke boundary with local model shape context.
fn log_colbert_startup_call_failed(
    diagnostics: ColbertStartupCallDiagnostics,
    started_at: Instant,
    source: &ApiError,
) {
    error!(
        event = "model_call.failed",
        model_role = "colbert",
        call_purpose = diagnostics.call_purpose,
        query_tokens = ?diagnostics.query_tokens,
        document_tokens = ?diagnostics.document_tokens,
        document_capacity_tokens = ?diagnostics.document_capacity_tokens,
        configured_query_max_tokens = diagnostics.configured_query_max_tokens,
        configured_document_max_tokens = diagnostics.configured_document_max_tokens,
        hidden_size = diagnostics.hidden_size,
        projection_dimension = diagnostics.projection_dimension,
        layer_count = diagnostics.layer_count,
        attention_kind = ?diagnostics.attention_kind,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        error = %source,
        "ColBERT startup smoke boundary failed"
    );
}

/// Convert ColBERT runtime failures into the service inference error shape.
fn inference_error(message: String) -> ApiError {
    ApiError::InferenceInit { message }
}
