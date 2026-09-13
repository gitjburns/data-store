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
pub(super) const SMOKE_QUERY: &str = "clear writing style rules";
pub(super) const SMOKE_DOCUMENT: &str = "Prefer specific words and direct sentences.";
const PROJECTION_DIR_NAME: &str = "1_Dense";
const PROJECTION_MODEL_FILE_NAME: &str = "model.safetensors";
const TOKENIZER_CONFIG_FILE_NAME: &str = "tokenizer_config.json";
const SENTENCE_BERT_CONFIG_FILE_NAME: &str = "sentence_bert_config.json";
const TOKEN_VECTOR_NORM_EPS: f64 = 1e-12;
/// Fixed different-length smoke texts (distinct token lengths) embedded both
/// batched and singly to prove the CPd batched DOCUMENT path is numerically
/// equivalent to the retained singular path. The longest is sized against the
/// runtime-loaded `local_attention` so it crosses a local-window boundary and
/// forces a masked padded region relative to the shorter two.
pub(super) const SMOKE_BATCH_SHORT: &str = "Clear rules.";
const SMOKE_BATCH_MEDIUM: &str =
    "Prefer specific words and direct sentences when explaining technical changes to a reader.";
/// Per-token-vector cosine floor between the batched and singular embeddings of
/// the SAME text. The two paths differ only in F32 accumulation order (batched
/// rank-3 matmuls vs. singular per-head 2D matmuls), so agreement must be near
/// exact; anything below this indicates a real formulation defect (a mask leak,
/// a stride miscompute) rather than floating-point noise.
const SMOKE_BATCH_COSINE_FLOOR: f32 = 0.9999;

#[derive(Debug, Clone)]
pub struct ColbertRuntime {
    tokenizer: Tokenizer,
    device: Device,
    input_path: ColbertInputPath,
    projection: ColbertProjection,
    encoder: ColbertEncoderRuntime,
    query_max_tokens: usize,
    document_max_tokens: usize,
    document_batch_size: usize,
    local_batch_max_tokens: usize,
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
    // Retained inference API (pinned contract); consumed at C6e/C7c.
    #[allow(dead_code)]
    pub query_tokens: usize,
    // Retained inference API (pinned contract); consumed at C6e/C7c.
    #[allow(dead_code)]
    pub document_tokens: usize,
}

#[derive(Debug, Clone)]
pub struct ColbertDocumentEmbedding {
    pub unit_id: String,
    pub token_count: usize,
    pub dimension: usize,
    pub vector: Vec<f32>,
}

/// One query matrix retained on its inference backend's scoring device for a retrieval stage.
/// The caller owns a local model permit during preparation and scoring calls;
/// retaining the tensor between those calls performs no model work and needs no permit.
#[derive(Debug)]
pub struct PreparedColbertQuery {
    matrix: Tensor,
    token_count: usize,
    dimension: usize,
}

impl PreparedColbertQuery {
    /// Preserve actual query token accounting without exposing the device tensor to retrieval code.
    pub fn token_count(&self) -> usize {
        self.token_count
    }

    /// Validate the small query once, before it can be reused across persisted document matrices.
    pub(super) fn from_matrix(matrix: Tensor, expected_dimension: usize) -> Result<Self, ApiError> {
        let (token_count, dimension) = matrix.dims2().map_err(|source| {
            inference_error(format!("ColBERT prepared query shape error: {source}"))
        })?;
        if token_count == 0 || dimension == 0 || dimension != expected_dimension {
            return Err(inference_error(format!(
                "ColBERT prepared query has shape [{token_count}, {dimension}], expected nonempty rows of dimension {expected_dimension}"
            )));
        }
        // A single query-sized CPU validation prevents NaNs from being hidden by MaxSim's maximum.
        let values = matrix
            .flatten_all()
            .and_then(|tensor| tensor.to_device(&Device::Cpu))
            .and_then(|tensor| tensor.to_vec1::<f32>())
            .map_err(|source| {
                inference_error(format!(
                    "ColBERT prepared query validation failed: {source}"
                ))
            })?;
        if values.iter().any(|value| !value.is_finite()) {
            return Err(inference_error(
                "ColBERT prepared query contains nonfinite values".to_string(),
            ));
        }
        Ok(Self {
            matrix,
            token_count,
            dimension,
        })
    }

    /// Score one borrowed matrix with temporary device storage; the caller owns stage diagnostics.
    pub(super) fn score_matrix(
        &self,
        values: &[f32],
        rows: usize,
        dimension: usize,
        expected_dimension: usize,
        device: &Device,
    ) -> Result<f32, ApiError> {
        if rows == 0
            || dimension == 0
            || dimension != expected_dimension
            || dimension != self.dimension
            || rows.checked_mul(dimension) != Some(values.len())
            || values.iter().any(|value| !value.is_finite())
        {
            return Err(inference_error(format!(
                "persisted ColBERT matrix has invalid shape [{rows}, {dimension}] or nonfinite values; backend dimension {expected_dimension}, query dimension {}",
                self.dimension
            )));
        }
        // Candle owns one document allocation at a time; the persisted payload remains borrowed.
        let document = Tensor::from_slice(values, (rows, dimension), device).map_err(|source| {
            inference_error(format!(
                "failed to load persisted ColBERT matrix onto scoring device: {source}"
            ))
        })?;
        maxsim_score(&self.matrix, &document)
    }
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
        validate_modernbert_config(&model_config)?;
        if config.max_tokens as usize > model_config.max_position_embeddings {
            return Err(inference_error(format!(
                "models.colbert.max_tokens {} exceeds checkpoint max_position_embeddings {}",
                config.max_tokens, model_config.max_position_embeddings
            )));
        }
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
        // CPd batch-consistency smoke (additive): prove the batched DOCUMENT path
        // matches the singular reference path per token vector on fixed
        // different-length texts BEFORE readiness. Reuses the shared smoke
        // logging boundary with its own `call_purpose`; `document_capacity_tokens`
        // is not a meaningful field here, so it is left at its default.
        progress("colbert_smoke_batch_consistency")?;
        let (batch_consistency_texts, crosses_local_boundary) =
            build_batch_consistency_smoke_texts(
                &tokenizer,
                model_config.local_attention,
                config.document_max_tokens as usize,
            )?;
        run_colbert_startup_call(
            ColbertStartupCallDiagnostics {
                call_purpose: "startup_smoke_batch_consistency",
                ..smoke_tokens
            },
            || {
                run_colbert_batch_consistency_smoke(
                    &tokenizer,
                    &input_path,
                    &encoder,
                    &projection,
                    config.document_max_tokens as usize,
                    device,
                    &batch_consistency_texts,
                )
            },
        )?;
        info!(
            event = "colbert_smoke_batch_consistency_ready",
            model_role = "colbert",
            text_count = batch_consistency_texts.len(),
            local_attention = model_config.local_attention,
            configured_document_max_tokens = config.document_max_tokens as usize,
            // When false, the loaded config's local window spans the document
            // token cap, so no smoke text can cross a local-window boundary; the
            // masked-padding case is still exercised, the boundary-crossing case
            // is not.
            crosses_local_boundary,
            "ColBERT batched/singular batch-consistency smoke passed"
        );
        progress("colbert_smoke_ready")?;

        Ok(Self {
            tokenizer,
            device: device.clone(),
            input_path,
            projection,
            encoder,
            query_max_tokens: config.query_max_tokens as usize,
            document_max_tokens: config.document_max_tokens as usize,
            document_batch_size: config.document_batch_size,
            local_batch_max_tokens: config.local_batch_max_tokens,
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

    /// Borrow the ColBERT tokenizer the runtime encodes with. The C6b chunk
    /// builder measures its per-chunk token cap against THIS tokenizer (§C6b
    /// contract) so a chunk's token count is the same measurement the
    /// multivector channel embeds under; the integration wiring passes this
    /// borrow into `chunk::build_chunks`. Read-only: the tokenizer is not
    /// mutated, and the borrow lives no longer than the runtime handle.
    pub(crate) fn tokenizer(&self) -> &Tokenizer {
        &self.tokenizer
    }

    /// Share the actual formatted-document capacity with source-window construction.
    pub(super) fn document_max_tokens(&self) -> usize {
        self.document_max_tokens
    }

    /// Bound padded document batches at the builder's admission boundary.
    pub(super) fn document_batch_size(&self) -> usize {
        self.document_batch_size
    }

    /// Route long inputs to batch-one inference where padding would multiply attention work.
    pub(super) fn local_batch_max_tokens(&self) -> usize {
        self.local_batch_max_tokens
    }

    /// Encode and flatten one unit's ColBERT document token matrix. Production
    /// path for LONG units under the CPd length-threshold hybrid (2026-07-18):
    /// the C6e builder routes units above configured `local_batch_max_tokens`
    /// here — padded quadratic attention makes the batched
    /// path slower past the ~130-token crossover — and packs the rest through
    /// `embed_documents`. Also consumed by the colbert-diagnostic bin, and
    /// remains the pinned byte-compatibility reference the batched path is
    /// defined against.
    pub fn embed_document(
        &self,
        unit_id: &str,
        document: &str,
    ) -> Result<ColbertDocumentEmbedding, ApiError> {
        let context = crate::util::model_call_context("colbert", "document_embedding");
        let _entered = context.enter();
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

    /// Embed a window of documents in ONE batched forward pass, returning one
    /// entry per input in input order, each already truncated to its own true
    /// token length so its matrix is byte-identical to the singular
    /// `embed_document` path.
    ///
    /// CPd batched DOCUMENT path (additive; the singular `embed_document` above
    /// stays intact for the colbert-diagnostic bin and as the byte-compatibility
    /// reference this batched path is defined against — the smoke's reference side
    /// reuses the same singular primitives directly, and the query path uses
    /// `encode_projected_query`). Documents are tokenized, padded to the window's
    /// longest true length, and run through the FLATTENED batched encoder
    /// (`encode_batched`): rank-2 linears over `(B·S, 768)`, rank-3 attention
    /// over `(B·heads, S, head_dim)`. A true key-padding mask composed with the
    /// existing local-window mask keeps padded keys from leaking into real
    /// tokens; each document's output rows `0..L_b` are extracted so padding
    /// never reaches storage.
    ///
    /// Gate: this method acquires NOTHING; the caller (the multi-vector builder)
    /// holds one `ModelCallPermit` across the whole per-parse batch (D-fact 9).
    /// One `model_call.*` pair is emitted per batch with batch-level fields.
    pub fn embed_documents(
        &self,
        units: &[(&str, &str)],
    ) -> Result<Vec<ColbertDocumentEmbedding>, ApiError> {
        let context = crate::util::model_call_context("colbert", "document_embedding_batched");
        let _entered = context.enter();
        let started_at = Instant::now();
        let text_count = units.len();
        let document_chars: usize = units.iter().map(|(_, text)| text.chars().count()).sum();
        info!(
            event = "model_call.started",
            model_role = "colbert",
            call_purpose = "document_embedding_batched",
            input_kind = "document",
            // Batch-level fields replace the singular path's per-unit `unit_id`:
            // `text_count` = batch size, `document_chars` = summed chars. The
            // per-unit `unit_id` is intentionally absent here.
            text_count,
            document_chars,
            configured_max_tokens = self.document_max_tokens,
            expected_dimension = self.projection_dimension,
            "ColBERT batched document embedding started"
        );

        let result = self.embed_documents_inner(units);
        match &result {
            Ok(embeddings) => {
                // `token_count` is the summed TRUE token count across the batch
                // (a safe aggregate), never per-unit and never vector values.
                let token_count: usize = embeddings
                    .iter()
                    .map(|embedding| embedding.token_count)
                    .sum();
                info!(
                    event = "model_call.completed",
                    model_role = "colbert",
                    call_purpose = "document_embedding_batched",
                    input_kind = "document",
                    text_count,
                    document_chars,
                    token_count,
                    configured_max_tokens = self.document_max_tokens,
                    expected_dimension = self.projection_dimension,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "ColBERT batched document embedding completed"
                );
            }
            Err(source) => {
                error!(
                    event = "model_call.failed",
                    model_role = "colbert",
                    call_purpose = "document_embedding_batched",
                    input_kind = "document",
                    text_count,
                    document_chars,
                    configured_max_tokens = self.document_max_tokens,
                    expected_dimension = self.projection_dimension,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    error = %source,
                    "ColBERT batched document embedding failed"
                );
            }
        }

        result
    }

    /// Tokenize, pad, run the flattened batched encoder, extract each document's
    /// true-length projected matrix, and materialize it to a `ColbertDocumentEmbedding`
    /// in input order. Split from `embed_documents` so the batch-level logging
    /// boundary wraps exactly this fallible work.
    fn embed_documents_inner(
        &self,
        units: &[(&str, &str)],
    ) -> Result<Vec<ColbertDocumentEmbedding>, ApiError> {
        if units.is_empty() {
            return Ok(Vec::new());
        }

        let texts: Vec<&str> = units.iter().map(|(_, text)| *text).collect();
        // Shared flattened batched pipeline; returns each document's true-length
        // projected matrix indexed by original input position.
        let matrices = batched_document_matrices(
            &self.tokenizer,
            &self.input_path,
            &self.encoder,
            &self.projection,
            self.document_max_tokens,
            &self.device,
            &texts,
        )?;

        // Materialize in input order. Padded rows were already dropped at
        // true-length extraction, so `tensor_to_document_embedding`'s non-finite
        // check only ever sees real-token values.
        let mut results: Vec<Option<ColbertDocumentEmbedding>> =
            (0..units.len()).map(|_| None).collect();
        for (input_index, matrix) in matrices {
            let unit_id = units[input_index].0;
            results[input_index] = Some(tensor_to_document_embedding(unit_id, matrix)?);
        }
        results
            .into_iter()
            .enumerate()
            .map(|(input_index, entry)| {
                entry.ok_or_else(|| {
                    inference_error(format!(
                        "ColBERT batched document embedding lost input index {input_index}"
                    ))
                })
            })
            .collect()
    }

    /// Embed one query on the existing device while the caller holds the local model permit.
    /// The permit may be released afterward and reacquired for each subsequent scoring scope.
    pub(super) fn prepare_query(&self, query: &str) -> Result<PreparedColbertQuery, ApiError> {
        let context = crate::util::model_call_context("colbert", "query_embedding");
        let _entered = context.enter();
        let started_at = Instant::now();
        let query_chars = query.chars().count();
        info!(
            event = "model_call.started",
            model_role = "colbert",
            call_purpose = "query_embedding",
            input_kind = "query",
            text_count = 1usize,
            query_chars,
            configured_max_tokens = self.query_max_tokens,
            expected_dimension = self.projection_dimension,
            "ColBERT query embedding started"
        );
        let result = self.encode_projected_query(query).and_then(|matrix| {
            PreparedColbertQuery::from_matrix(matrix, self.projection_dimension)
        });
        match &result {
            Ok(prepared) => info!(
                event = "model_call.completed",
                model_role = "colbert",
                call_purpose = "query_embedding",
                input_kind = "query",
                text_count = 1usize,
                query_chars,
                token_count = prepared.token_count(),
                configured_max_tokens = self.query_max_tokens,
                vector_dimension = self.projection_dimension,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "ColBERT query embedding completed"
            ),
            Err(source) => error!(
                event = "model_call.failed", model_role = "colbert", call_purpose = "query_embedding",
                input_kind = "query", text_count = 1usize, query_chars,
                configured_max_tokens = self.query_max_tokens, expected_dimension = self.projection_dimension,
                elapsed_ms = started_at.elapsed().as_millis() as u64, error = %source,
                "ColBERT query embedding failed"
            ),
        }
        result
    }

    /// Reuse a prepared query while loading only the current document onto the existing device.
    pub(super) fn score_matrix(
        &self,
        prepared: &PreparedColbertQuery,
        values: &[f32],
        rows: usize,
        dimension: usize,
    ) -> Result<f32, ApiError> {
        prepared.score_matrix(
            values,
            rows,
            dimension,
            self.projection_dimension,
            &self.device,
        )
    }

    /// Score persisted ColBERT document token vectors without per-candidate progress reporting.
    // Retained inference API (pinned contract); consumed at C6e/C7c.
    #[allow(dead_code)]
    pub fn score_persisted_candidates(
        &self,
        query: &str,
        candidates: &[ColbertDocumentEmbedding],
    ) -> Result<Vec<ColbertCandidateScore>, ApiError> {
        self.score_persisted_candidates_with_progress(query, candidates, |_, _| Ok(()))
    }

    /// Score persisted ColBERT document token vectors while reporting completed candidates.
    // Retained inference API (pinned contract); consumed at C6e/C7c.
    #[allow(dead_code)]
    pub fn score_persisted_candidates_with_progress<F>(
        &self,
        query: &str,
        candidates: &[ColbertDocumentEmbedding],
        mut progress: F,
    ) -> Result<Vec<ColbertCandidateScore>, ApiError>
    where
        F: FnMut(u64, u64) -> Result<(), ApiError>,
    {
        let context = crate::util::model_call_context("colbert", "persisted_candidate_scoring");
        let _entered = context.enter();
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
    // Retained inference API (pinned contract); consumed at C6e/C7c.
    #[allow(dead_code)]
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
    // Retained inference API (pinned contract); consumed at C6e/C7c.
    #[allow(dead_code)]
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
    // Retained inference API (pinned contract); consumed at C6e/C7c.
    #[allow(dead_code)]
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

    /// Batched twin of `forward`: turn a padded `(batch_size, padded_len)`
    /// token-id matrix into FLATTENED `(batch_size * padded_len, 768)` normalized
    /// hidden states. LayerNorm reduces only within a token (`D::Minus1`), so
    /// padded rows never contaminate real rows here; the key-padding mask in
    /// attention is what keeps padded KEYS out of real tokens downstream.
    fn forward_batched(
        &self,
        padded_ids: &[u32],
        batch_size: usize,
        padded_len: usize,
        device: &Device,
    ) -> Result<Tensor, ApiError> {
        let input = Tensor::from_vec(padded_ids.to_vec(), (batch_size, padded_len), device)
            .map_err(|source| {
                inference_error(format!(
                    "failed to build ColBERT batched document input tensor: {source}"
                ))
            })?;
        let hidden = self.embeddings.forward(&input).map_err(|source| {
            inference_error(format!(
                "ColBERT batched document token embedding lookup failed: {source}"
            ))
        })?;
        let normalized = self.norm.forward(&hidden).map_err(|source| {
            inference_error(format!(
                "ColBERT batched document embedding norm failed: {source}"
            ))
        })?;
        normalized
            .reshape((batch_size * padded_len, self.hidden_size))
            .map_err(|source| {
                inference_error(format!(
                    "failed to flatten ColBERT batched document hidden states for encoder: {source}"
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

    /// FLATTENED batched twin of `forward`: run ModernBERT attention over a whole
    /// padded window. Input/output are flattened `(batch_size * seq_len, hidden)`;
    /// `pad_mask` is the shared additive `(batch_size, seq_len)` key-padding mask
    /// (`0.0` for real keys, `-inf` for padded keys `j >= L_b`), computed once per
    /// encode and reused for every layer. RoPE, QKV split, per-head scores, mask
    /// composition, softmax, and `probs·V` all run rank-3 over `(B·heads, S, D)`.
    fn forward_batched(
        &self,
        hidden_states: &Tensor,
        batch_size: usize,
        seq_len: usize,
        pad_mask: &Tensor,
        label: &str,
    ) -> Result<Tensor, ApiError> {
        let (rows, hidden_size) = hidden_states.dims2().map_err(|source| {
            inference_error(format!(
                "ColBERT {label} batched attention input must be rank-2 hidden states: {source}"
            ))
        })?;
        if hidden_size != self.hidden_size || rows != batch_size * seq_len {
            return Err(inference_error(format!(
                "ColBERT {label} batched attention shape [{rows}, {hidden_size}], expected [{}, {}]",
                batch_size * seq_len,
                self.hidden_size
            )));
        }
        if seq_len > self.max_position_embeddings {
            return Err(inference_error(format!(
                "ColBERT {label} batched attention sequence length {seq_len} exceeds max_position_embeddings {}",
                self.max_position_embeddings
            )));
        }

        // Attention pre-norm (skipped for layer 0, mirroring the singular path).
        // LayerNorm reduces within a token, so it is padding-safe on flattened
        // rows.
        let attention_input = match &self.attn_norm {
            Some(norm) => norm.forward(hidden_states).map_err(|source| {
                inference_error(format!(
                    "ColBERT {label} batched attention norm failed for layer {}: {source}",
                    self.layer_index
                ))
            })?,
            None => hidden_states.clone(),
        };
        let qkv = self.qkv_proj.forward(&attention_input).map_err(|source| {
            inference_error(format!(
                "ColBERT {label} batched fused QKV projection failed for layer {}: {source}",
                self.layer_index
            ))
        })?;
        // Re-batch the fused projection to `(batch_size, seq_len, 3*hidden)` so
        // each projection can be split per head with a known batch axis.
        let qkv = qkv
            .reshape((batch_size, seq_len, hidden_size * 3))
            .map_err(|source| {
                inference_error(format!(
                    "failed to batch ColBERT {label} fused QKV projection for layer {}: {source}",
                    self.layer_index
                ))
            })?;
        let q =
            self.split_attention_projection_batched(&qkv, 0, batch_size, seq_len, label, "query")?;
        let k = self.split_attention_projection_batched(
            &qkv,
            hidden_size,
            batch_size,
            seq_len,
            label,
            "key",
        )?;
        let v = self.split_attention_projection_batched(
            &qkv,
            hidden_size * 2,
            batch_size,
            seq_len,
            label,
            "value",
        )?;
        let q = apply_rope(&q, self.rope_theta).map_err(|source| {
            inference_error(format!(
                "ColBERT {label} batched query rope failed: {source}"
            ))
        })?;
        let k = apply_rope(&k, self.rope_theta).map_err(|source| {
            inference_error(format!("ColBERT {label} batched key rope failed: {source}"))
        })?;
        let attention_output =
            self.attention_output_by_head_batched(&q, &k, &v, pad_mask, label)?;
        // Flatten `(batch_size, seq_len, hidden)` back to `(B·S, hidden)` for the
        // output projection, which reduces within a token.
        let attention_output = attention_output
            .reshape((batch_size * seq_len, hidden_size))
            .map_err(|source| {
                inference_error(format!(
                    "failed to flatten ColBERT {label} batched attention output before projection: {source}"
                ))
            })?;
        self.out_proj.forward(&attention_output).map_err(|source| {
            inference_error(format!(
                "ColBERT {label} batched attention output projection failed for layer {}: {source}",
                self.layer_index
            ))
        })
    }

    /// Rank-3 batched attention over `(batch_size * heads, seq_len, head_dim)`.
    ///
    /// q/k/v arrive as `(batch_size, heads, seq_len, head_dim)`. They are
    /// reshaped to `(B·heads, S, D)` (`.contiguous()` after every stride-permuting
    /// reshape/transpose — the candle-Metal defect below). Scores are
    /// `q·kᵀ / sqrt(D)` giving `(B·heads, S, S)`; the additive local-window mask
    /// (Local layers only) and the key-padding mask are composed onto them; a
    /// single softmax and `probs·V` finish the head.
    ///
    /// candle-Metal defect (cited per CPd contract): `Tensor::cat`/`narrow` over
    /// strided views returns a stride-permuted view CPU matmul rejects
    /// (MatMulUnexpectedStriding) but Metal silently miscomputes past batch row 0,
    /// so every reshape that permutes strides is forced contiguous.
    fn attention_output_by_head_batched(
        &self,
        q: &Tensor,
        k: &Tensor,
        v: &Tensor,
        pad_mask: &Tensor,
        label: &str,
    ) -> Result<Tensor, ApiError> {
        // q/k/v arrive as `(batch_size, heads, seq_len, head_dim)`; derive the
        // batch and sequence axes here rather than threading them as parameters.
        let (batch_size, _, seq_len, _) = q.dims4().map_err(|source| {
            inference_error(format!(
                "ColBERT {label} batched attention query must be rank-4: {source}"
            ))
        })?;
        let flat = batch_size * self.num_attention_heads;
        let q = self.flatten_heads(q, flat, seq_len, label, "query")?;
        let k = self.flatten_heads(k, flat, seq_len, label, "key")?;
        let v = self.flatten_heads(v, flat, seq_len, label, "value")?;

        let scores = q
            .matmul(
                &k.transpose(1, 2)
                    .and_then(|tensor| tensor.contiguous())
                    .map_err(|source| {
                        inference_error(format!(
                            "ColBERT {label} batched key transpose failed: {source}"
                        ))
                    })?,
            )
            .and_then(|tensor| tensor / (self.head_dim as f64).sqrt())
            .map_err(|source| {
                inference_error(format!(
                    "ColBERT {label} batched attention scores failed: {source}"
                ))
            })?;

        // Compose masks additively onto `(B·heads, S, S)`. The local-window mask
        // (Global layers have none) is positional and identical across batch
        // rows, so it broadcasts over the flat leading axis. The key-padding mask
        // is `(batch_size, 1, seq_len)` and must be expanded per head so key
        // column `j` is masked in the SAME document's rows only; padded keys are
        // masked even when the local window covers the whole sequence (the
        // `local_attention >= seq_len` early return does NOT skip padding).
        // Additive `-inf` is idempotent, so composition order is irrelevant.
        let scores = match self.attention_kind {
            ColbertAttentionKind::Global => scores,
            ColbertAttentionKind::Local => {
                let window = local_attention_mask(seq_len, self.local_attention, scores.device())
                    .map_err(|source| {
                    inference_error(format!(
                        "ColBERT {label} batched local attention mask failed: {source}"
                    ))
                })?;
                match window {
                    Some(window) => scores.broadcast_add(&window).map_err(|source| {
                        inference_error(format!(
                            "ColBERT {label} batched local mask add failed: {source}"
                        ))
                    })?,
                    None => scores,
                }
            }
        };
        let scores = self
            .add_key_padding_mask(&scores, pad_mask, batch_size, seq_len)
            .map_err(|source| {
                inference_error(format!(
                    "ColBERT {label} batched key-padding mask failed: {source}"
                ))
            })?;

        // NaN / fully-masked-row hazard: a padded QUERY row (row index
        // `i >= L_b`) can be masked against EVERY key (when its position falls
        // outside every real key's local window), so its softmax is NaN. That NaN
        // stays inside this layer's padded row here, but it IS a cross-LAYER
        // contamination hazard: at the next layer the NaN row feeds `qkv_proj`,
        // its K/V rows go NaN, and `finite · NaN = NaN` would poison the score
        // COLUMN of that key for every real query row of the same document,
        // defeating both additive masks (`NaN + -inf = NaN`). It is confined per
        // layer by the post-layer row-validity multiply in `encode_batched`,
        // which zeros padded rows at each layer exit so the next layer's qkv sees
        // a finite (zero-input) padded row. REAL query rows always keep their own
        // diagonal key unmasked (a token attends to itself within its window and
        // is never a padded key), so no real row goes fully `-inf`.
        let probs = softmax_last_dim_metal_safe(&scores).map_err(|source| {
            inference_error(format!(
                "ColBERT {label} batched attention softmax failed: {source}"
            ))
        })?;
        let context = probs.matmul(&v).map_err(|source| {
            inference_error(format!(
                "ColBERT {label} batched attention output failed: {source}"
            ))
        })?;

        // `(B·heads, S, D)` -> `(batch_size, heads, S, D)` -> `(batch_size, S,
        // heads, D)` -> `(batch_size, S, hidden)`. `.contiguous()` after the
        // head/seq transpose (stride-permuting) before the final merge reshape.
        context
            .reshape((batch_size, self.num_attention_heads, seq_len, self.head_dim))
            .and_then(|tensor| tensor.transpose(1, 2))
            .and_then(|tensor| tensor.contiguous())
            .and_then(|tensor| tensor.reshape((batch_size, seq_len, self.hidden_size)))
            .map_err(|source| {
                inference_error(format!(
                    "ColBERT {label} batched attention head merge failed: {source}"
                ))
            })
    }

    /// Reshape `(batch_size, heads, seq_len, head_dim)` into the rank-3
    /// `(batch_size * heads, seq_len, head_dim)` matmul shape, forcing contiguity
    /// (the head/batch axes were produced by a stride-permuting transpose) (the
    /// candle-Metal strided-view defect).
    fn flatten_heads(
        &self,
        states: &Tensor,
        flat: usize,
        seq_len: usize,
        label: &str,
        projection_label: &str,
    ) -> Result<Tensor, ApiError> {
        states
            .contiguous()
            .and_then(|tensor| tensor.reshape((flat, seq_len, self.head_dim)))
            .map_err(|source| {
                inference_error(format!(
                    "ColBERT {label} {projection_label} head flatten failed: {source}"
                ))
            })
    }

    /// Expand the shared `(batch_size, seq_len)` additive key-padding mask to
    /// `(batch_size * heads, 1, seq_len)` and add it to `(B·heads, S, S)` scores,
    /// masking padded KEY columns across every query row of the same document.
    fn add_key_padding_mask(
        &self,
        scores: &Tensor,
        pad_mask: &Tensor,
        batch_size: usize,
        seq_len: usize,
    ) -> candle_core::Result<Tensor> {
        // `(batch_size, seq_len)` -> `(batch_size, 1, 1, seq_len)` -> repeat over
        // heads -> `(batch_size * heads, 1, seq_len)`. The middle `1` broadcasts
        // over the query axis so every query row of a document sees the same
        // masked key columns. `broadcast_as` yields a stride-0 view; force
        // contiguity before the flatten reshape (the candle-Metal strided-view
        // defect — see attention_output_by_head_batched).
        let expanded = pad_mask
            .reshape((batch_size, 1, 1, seq_len))?
            .broadcast_as((batch_size, self.num_attention_heads, 1, seq_len))?
            .contiguous()?
            .reshape((batch_size * self.num_attention_heads, 1, seq_len))?;
        scores.broadcast_add(&expanded)
    }

    /// FLATTENED-batched fused-QKV split into `(batch_size, heads, seq_len,
    /// head_dim)`. `.contiguous()` after the head/seq transpose so the downstream
    /// flatten sees a packed buffer (the candle-Metal strided-view defect).
    fn split_attention_projection_batched(
        &self,
        qkv: &Tensor,
        offset: usize,
        batch_size: usize,
        seq_len: usize,
        label: &str,
        projection_label: &str,
    ) -> Result<Tensor, ApiError> {
        qkv.narrow(2, offset, self.hidden_size)
            .and_then(|tensor| {
                tensor.reshape((batch_size, seq_len, self.num_attention_heads, self.head_dim))
            })
            .and_then(|tensor| tensor.transpose(1, 2))
            .and_then(|tensor| tensor.contiguous())
            .map_err(|source| {
                inference_error(format!(
                    "ColBERT {label} {projection_label} batched split failed: {source}"
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

    /// FLATTENED batched twin of `forward`: one ModernBERT layer over a padded
    /// window. Hidden states are flattened `(batch_size * seq_len, hidden)`; the
    /// shared `pad_mask` threads into batched attention. Residuals and the
    /// GELU-gated MLP are elementwise / within-token, so they operate unchanged on
    /// the flattened rows — padded rows never leak into real rows.
    fn forward_batched(
        &self,
        hidden_states: &Tensor,
        batch_size: usize,
        seq_len: usize,
        pad_mask: &Tensor,
        label: &str,
    ) -> Result<Tensor, ApiError> {
        let (rows, hidden_size) = hidden_states.dims2().map_err(|source| {
            inference_error(format!(
                "ColBERT {label} batched single-layer input must be rank-2 hidden states: {source}"
            ))
        })?;
        if hidden_size != self.hidden_size || rows != batch_size * seq_len {
            return Err(inference_error(format!(
                "ColBERT {label} batched single-layer shape [{rows}, {hidden_size}], expected [{}, {}]",
                batch_size * seq_len,
                self.hidden_size
            )));
        }

        let attention_output =
            self.attention
                .forward_batched(hidden_states, batch_size, seq_len, pad_mask, label)?;
        let hidden_states = (attention_output + hidden_states).map_err(|source| {
            inference_error(format!(
                "ColBERT {label} batched attention residual failed for layer {}: {source}",
                self.layer_index
            ))
        })?;
        let mlp_input = self.mlp_norm.forward(&hidden_states).map_err(|source| {
            inference_error(format!(
                "ColBERT {label} batched MLP norm failed for layer {}: {source}",
                self.layer_index
            ))
        })?;
        let mlp_output = self.mlp.forward(&mlp_input, label, self.layer_index)?;
        (mlp_output + hidden_states).map_err(|source| {
            inference_error(format!(
                "ColBERT {label} batched MLP residual failed for layer {}: {source}",
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

    /// FLATTENED batched twin of `encode`: run the full ModernBERT stack over one
    /// padded window of `batch_size` documents. Input/output are flattened
    /// `(batch_size * seq_len, hidden)`. The key-padding mask is built ONCE from
    /// `true_lengths` and reused for every layer; final LayerNorm reduces within a
    /// token, so it is padding-safe on the flattened rows.
    fn encode_batched(
        &self,
        hidden_states: &Tensor,
        batch_size: usize,
        seq_len: usize,
        true_lengths: &[usize],
    ) -> Result<Tensor, ApiError> {
        let (rows, hidden_size) = hidden_states.dims2().map_err(|source| {
            inference_error(format!(
                "ColBERT batched full-encoder input must be rank-2 hidden states: {source}"
            ))
        })?;
        if hidden_size != self.hidden_size || rows != batch_size * seq_len {
            return Err(inference_error(format!(
                "ColBERT batched full-encoder shape [{rows}, {hidden_size}], expected [{}, {}]",
                batch_size * seq_len,
                self.hidden_size
            )));
        }
        if true_lengths.len() != batch_size {
            return Err(inference_error(format!(
                "ColBERT batched full-encoder received {} true lengths, expected {batch_size}",
                true_lengths.len()
            )));
        }

        let pad_mask =
            key_padding_mask(true_lengths, seq_len, hidden_states.device()).map_err(|source| {
                inference_error(format!(
                    "ColBERT batched key-padding mask build failed: {source}"
                ))
            })?;

        // Row-validity mask over the SAME packed row order as the flattened hidden
        // states: 1.0 for real rows (`0..L_b`), 0.0 for padded rows. Scrubbing each
        // layer's padded-row OUTPUT confines a fully-masked-softmax NaN (born when
        // a padded query row falls outside every real key's local window, so all
        // its scores are `-inf`) to the layer that produced it: the next layer's
        // row-wise qkv projection then sees a zero-input padded row and emits
        // exactly-zero K/V (the projections are bias-free), whose score COLUMNS
        // the key-padding mask still pins to `-inf`, so every REAL row's output is
        // governed bit-for-bit by real tokens alone and batched-vs-singular
        // agreement holds.
        //
        // The scrub MUST be a `where_cond` SELECT, never multiplication: IEEE 754
        // makes `NaN * 0.0 = NaN`, so a zero-mask multiply preserves exactly the
        // NaN it exists to remove, and one layer later that NaN spreads through
        // the shared key columns into every real row of the same document
        // (probe-verified on both backends 2026-07-18: dense-batch-diagnostic
        // `--probe-batched-sanitize`). The select copies real rows bit-exactly and
        // forces padded rows to literal zeros regardless of their contents;
        // zeroed rows stay finite through LayerNorm/RmsNorm (zero mean,
        // eps-floored variance) and the final norm, and are discarded at
        // true-length extraction.
        let row_validity = row_validity_mask(true_lengths, seq_len, hidden_states.device())
            .map_err(|source| {
                inference_error(format!(
                    "ColBERT batched row-validity mask build failed: {source}"
                ))
            })?;
        // The U8 condition is materialized `.contiguous()` once before the loop:
        // `broadcast_as` yields a stride-0 view, the candle-Metal strided-view
        // defect class (see attention_output_by_head_batched).
        let validity_condition = row_validity
            .broadcast_as((rows, hidden_size))
            .and_then(|mask| mask.contiguous())
            .and_then(|mask| mask.to_dtype(DType::U8))
            .map_err(|source| {
                inference_error(format!(
                    "ColBERT batched row-validity condition build failed: {source}"
                ))
            })?;
        let zero_rows = Tensor::zeros((rows, hidden_size), DType::F32, hidden_states.device())
            .map_err(|source| {
                inference_error(format!(
                    "ColBERT batched zero-row template build failed: {source}"
                ))
            })?;

        let mut current = hidden_states.clone();
        for layer in &self.layers {
            current =
                layer.forward_batched(&current, batch_size, seq_len, &pad_mask, "document")?;
            // Sanitize every layer exit uniformly so no padded-row NaN ever
            // reaches the next layer's qkv projection or the final norm.
            current = validity_condition
                .where_cond(&current, &zero_rows)
                .map_err(|source| {
                    inference_error(format!(
                        "ColBERT batched row-validity sanitize failed: {source}"
                    ))
                })?;
        }
        self.final_norm.forward(&current).map_err(|source| {
            inference_error(format!(
                "ColBERT batched document final norm failed: {source}"
            ))
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

/// Validate the ColBERT-Zero projection contract shared by local and HTTP backends.
pub(super) fn validate_colbert_config(config: &ColbertModelConfig) -> Result<(), ApiError> {
    if config.dimension as usize != EXPECTED_PROJECTION_DIMENSION {
        return Err(inference_error(format!(
            "models.colbert.dimension must be {EXPECTED_PROJECTION_DIMENSION}, got {}",
            config.dimension
        )));
    }

    // The checkpoint's positional contract is a physical constraint, not an operator override.
    if config.max_tokens as usize > EXPECTED_TOKENIZER_MAX_LENGTH
        || config.query_max_tokens > config.max_tokens
        || config.document_max_tokens > config.max_tokens
    {
        return Err(inference_error(format!(
            "models.colbert query/document max tokens must fit max_tokens {}, within ColBERT-Zero capacity {EXPECTED_TOKENIZER_MAX_LENGTH}",
            config.max_tokens
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
fn validate_modernbert_config(model_config: &ModernBertConfig) -> Result<(), ApiError> {
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
pub(super) fn format_query(text: &str) -> String {
    format!("{QUERY_PROMPT}{QUERY_MARKER}{text}")
}

/// Apply the ColBERT-Zero document prompt and marker inside the runtime boundary.
pub(super) fn format_document(text: &str) -> String {
    format!("{DOCUMENT_PROMPT}{DOCUMENT_MARKER}{text}")
}

/// Tokenize one formatted ColBERT text and apply the configured service truncation.
/// Legacy whole-unit inputs and queries retain their existing prefix policy;
/// annotation representations arrive through complete, verified document windows.
pub(super) fn tokenize_formatted(
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
    match local_attention_mask(seq_len, local_attention, scores.device())? {
        Some(mask) => scores.broadcast_add(&mask.to_dtype(scores.dtype())?),
        None => Ok(scores.clone()),
    }
}

/// Build ModernBERT's additive local bidirectional window mask as an
/// `(seq_len, seq_len)` F32 tensor (`0.0` inside the window, `-inf` outside), or
/// `None` when the window covers the whole sequence (nothing to mask). Shared by
/// the singular and the flattened batched paths; the mask is positional only, so
/// the batched path broadcasts one copy across every `(batch·heads)` row. The
/// window is purely a QUERY-KEY DISTANCE mask — it never masks padded keys, which
/// is the separate key-padding concern the batched path composes on top.
fn local_attention_mask(
    seq_len: usize,
    local_attention: usize,
    device: &Device,
) -> candle_core::Result<Option<Tensor>> {
    if local_attention >= seq_len {
        return Ok(None);
    }

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
    Ok(Some(Tensor::from_vec(values, (seq_len, seq_len), device)?))
}

/// Build the shared additive key-padding mask for one padded window:
/// `(batch_size, seq_len)` F32 with `0.0` at real key positions `j < L_b` and
/// `-inf` at padded positions `j >= L_b`. Computed ONCE per encode and reused for
/// every layer; the batched attention expands it per head. A padded KEY column is
/// forced to zero attention weight after softmax regardless of the local window,
/// which is what makes padding safe under ModernBERT's bidirectional attention.
fn key_padding_mask(
    true_lengths: &[usize],
    seq_len: usize,
    device: &Device,
) -> candle_core::Result<Tensor> {
    let batch_size = true_lengths.len();
    let mut values = Vec::with_capacity(batch_size * seq_len);
    for &true_len in true_lengths {
        for col in 0..seq_len {
            values.push(if col < true_len {
                0.0f32
            } else {
                f32::NEG_INFINITY
            });
        }
    }
    Tensor::from_vec(values, (batch_size, seq_len), device)
}

/// Build the `(batch_size * seq_len, 1)` row-validity mask in packed row order:
/// 1.0 for rows whose within-document position is below that document's true
/// length, 0.0 for padded rows. `encode_batched` broadcasts this into a
/// `where_cond` SELECT condition that zeroes padded rows at every layer exit —
/// a select, never a multiply, because `NaN * 0.0 = NaN` (see the sanitize
/// comment in `encode_batched`).
fn row_validity_mask(
    true_lengths: &[usize],
    seq_len: usize,
    device: &Device,
) -> candle_core::Result<Tensor> {
    let batch_size = true_lengths.len();
    let mut values = Vec::with_capacity(batch_size * seq_len);
    for &true_len in true_lengths {
        for pos in 0..seq_len {
            values.push(if pos < true_len { 1.0f32 } else { 0.0f32 });
        }
    }
    Tensor::from_vec(values, (batch_size * seq_len, 1), device)
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

/// Run the FLATTENED batched DOCUMENT pipeline over a window of texts and return
/// each document's true-length projected `(true_len, dimension)` matrix paired
/// with its original input index.
///
/// The batched formulation (per the CPd contract): tokenize each text (same
/// formatter and cap as the singular path, so a one-text window is byte-identical
/// to `embed_document`), length-sort locally to bound padding waste, pad to the
/// window's longest true length, run `forward_batched` → `encode_batched` →
/// `project` over flattened rows, then extract each document's rows `0..L_b`.
/// Padded rows are dropped here BEFORE any non-finite check. This is a free
/// function so the startup batch-consistency smoke can call it on the same loaded
/// components before the `ColbertRuntime` handle exists.
fn batched_document_matrices(
    tokenizer: &Tokenizer,
    input_path: &ColbertInputPath,
    encoder: &ColbertEncoderRuntime,
    projection: &ColbertProjection,
    document_max_tokens: usize,
    device: &Device,
    texts: &[&str],
) -> Result<Vec<(usize, Tensor)>, ApiError> {
    if texts.is_empty() {
        return Ok(Vec::new());
    }

    // Tokenize up front so true lengths are known before padding. Attribute a
    // failure to its window-local input index (the identifier available at this
    // seam — the smoke calls this with texts only, no unit ids; callers map index
    // -> unit id), mirroring the extraction-error convention below.
    let mut tokenized: Vec<Vec<u32>> = Vec::with_capacity(texts.len());
    for (input_index, text) in texts.iter().enumerate() {
        tokenized.push(
            tokenize_formatted(
                tokenizer,
                &format_document(text),
                document_max_tokens,
                "search document",
            )
            .map_err(|source| {
                inference_error(format!(
                    "ColBERT batched document tokenization failed for input {input_index}: {source}"
                ))
            })?,
        );
    }

    // Length-sort input positions (longest first, id tiebreak) ONLY to minimize
    // padding waste inside this packed forward; each result carries its original
    // input index so the caller restores input order.
    let mut order: Vec<usize> = (0..tokenized.len()).collect();
    order.sort_by(|&left, &right| {
        tokenized[right]
            .len()
            .cmp(&tokenized[left].len())
            .then_with(|| left.cmp(&right))
    });

    let batch_size = order.len();
    let padded_len = order
        .iter()
        .map(|&slot| tokenized[slot].len())
        .max()
        .unwrap_or(0);
    if padded_len == 0 {
        return Err(inference_error(
            "ColBERT batched document embedding produced no tokens".to_string(),
        ));
    }

    // True lengths in packed row order drive the key-padding mask and the
    // per-document true-length extraction below.
    let true_lengths: Vec<usize> = order.iter().map(|&slot| tokenized[slot].len()).collect();

    // Padded `(batch_size, padded_len)` token-id matrix. Pad id 0 never
    // contributes: its key columns are masked to -inf before softmax and its
    // query rows are discarded at extraction.
    let mut padded_ids: Vec<u32> = vec![0; batch_size * padded_len];
    for (row, &slot) in order.iter().enumerate() {
        let ids = &tokenized[slot];
        padded_ids[row * padded_len..row * padded_len + ids.len()].copy_from_slice(ids);
    }

    let hidden = input_path.forward_batched(&padded_ids, batch_size, padded_len, device)?;
    let encoded = encoder.encode_batched(&hidden, batch_size, padded_len, &true_lengths)?;
    let projected = projection.project(&encoded)?;

    // Extract each document's rows `0..L_b` from the packed projection, restoring
    // input order via the packed-slot -> input-index map.
    let mut matrices: Vec<(usize, Tensor)> = Vec::with_capacity(batch_size);
    for (row, &input_index) in order.iter().enumerate() {
        let true_len = true_lengths[row];
        // `narrow` returns a strided view of the packed projection; force
        // contiguity so downstream consumers never see it (the candle-Metal
        // strided-view defect — see attention_output_by_head_batched).
        let document_rows = projected
            .narrow(0, row * padded_len, true_len)
            .and_then(|tensor| tensor.contiguous())
            .map_err(|source| {
                inference_error(format!(
                    "failed to extract ColBERT batched document rows for input {input_index}: {source}"
                ))
            })?;
        matrices.push((input_index, document_rows));
    }

    Ok(matrices)
}

/// Compute ColBERT MaxSim by summing each query token's best document-token dot product.
pub(super) fn maxsim_score(
    query_vectors: &Tensor,
    document_vectors: &Tensor,
) -> Result<f32, ApiError> {
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

/// Build the three fixed different-length smoke texts, sizing the longest
/// against the runtime-loaded `local_attention` so its token count exceeds the
/// local window (forcing at least one length crossing a window boundary and a
/// masked padded region relative to the shorter texts) while staying under
/// `document_max_tokens`. Returns the texts plus whether the local-boundary case
/// is actually exercised. When the loaded `local_attention >= document_max_tokens`
/// makes a boundary crossing impossible, the longest text targets
/// `document_max_tokens` and `false` is returned so the caller can note it.
fn build_batch_consistency_smoke_texts(
    tokenizer: &Tokenizer,
    local_attention: usize,
    document_max_tokens: usize,
) -> Result<(Vec<String>, bool), ApiError> {
    // A repeated sentence whose length we grow until its true (post-truncation)
    // token count clears the target, so the longest text reliably spans a padded
    // region relative to the two shorter fixed texts.
    let unit = "Prefer specific direct sentences when explaining technical changes to a reader. ";
    let crosses_boundary = local_attention < document_max_tokens;
    let target_tokens = if crosses_boundary {
        // One token past the window guarantees a crossing while staying bounded.
        (local_attention + 1).min(document_max_tokens)
    } else {
        document_max_tokens
    };

    let mut long_text = String::new();
    // Grow until the tokenized-and-truncated length reaches the target or the
    // text is already at capacity (repeats add at least one token each round).
    loop {
        long_text.push_str(unit);
        let token_len = tokenize_formatted(
            tokenizer,
            &format_document(&long_text),
            document_max_tokens,
            "batch-consistency smoke",
        )?
        .len();
        if token_len >= target_tokens || token_len >= document_max_tokens {
            break;
        }
    }

    Ok((
        vec![
            SMOKE_BATCH_SHORT.to_string(),
            SMOKE_BATCH_MEDIUM.to_string(),
            long_text,
        ],
        crosses_boundary,
    ))
}

/// Batch-consistency startup smoke: embed fixed different-length texts BOTH
/// batched (the CPd path) AND singly (the retained singular reference path), then
/// gate each text's every token vector on per-token cosine similarity between the
/// two. A failure raises `ApiError::InferenceInit` (same class as every other
/// smoke) because a divergence means the batched formulation — mask composition,
/// padding, or a stride miscompute — is wrong and must block readiness.
fn run_colbert_batch_consistency_smoke(
    tokenizer: &Tokenizer,
    input_path: &ColbertInputPath,
    encoder: &ColbertEncoderRuntime,
    projection: &ColbertProjection,
    document_max_tokens: usize,
    device: &Device,
    texts: &[String],
) -> Result<(), ApiError> {
    let text_refs: Vec<&str> = texts.iter().map(String::as_str).collect();

    // Batched side (CPd path), returned in input order.
    let mut batched: Vec<Option<Tensor>> = (0..text_refs.len()).map(|_| None).collect();
    for (input_index, matrix) in batched_document_matrices(
        tokenizer,
        input_path,
        encoder,
        projection,
        document_max_tokens,
        device,
        &text_refs,
    )? {
        batched[input_index] = Some(matrix);
    }

    for (index, text) in text_refs.iter().enumerate() {
        // Singular reference side: the exact retained per-doc path. Every failure
        // carries the text index so a divergence names its input; `project` takes
        // no label, so its error is wrapped to carry the same index.
        let reference_label = format!("batch-consistency smoke text {index}");
        let token_ids = tokenize_formatted(
            tokenizer,
            &format_document(text),
            document_max_tokens,
            &reference_label,
        )?;
        let reference_hidden = input_path.forward(&token_ids, device, &reference_label)?;
        let reference_encoded = encoder.encode(&reference_hidden, &reference_label)?;
        let reference = projection.project(&reference_encoded).map_err(|source| {
            inference_error(format!(
                "ColBERT batch-consistency smoke text {index} reference projection failed: {source}"
            ))
        })?;

        let batched_matrix = batched[index].as_ref().ok_or_else(|| {
            inference_error(format!(
                "ColBERT batch-consistency smoke lost batched text index {index}"
            ))
        })?;
        assert_token_matrices_agree(batched_matrix, &reference, index)?;
    }

    Ok(())
}

/// Gate every token vector of two `(tokens, dimension)` matrices on cosine
/// similarity >= `SMOKE_BATCH_COSINE_FLOOR`. Both matrices are already
/// L2-normalized by `project`, so the per-token dot product IS the cosine; the
/// floor allows only F32 accumulation-order drift, not a formulation defect.
fn assert_token_matrices_agree(
    batched: &Tensor,
    reference: &Tensor,
    text_index: usize,
) -> Result<(), ApiError> {
    let batched_rows = batched
        .to_device(&Device::Cpu)
        .and_then(|tensor| tensor.to_vec2::<f32>())
        .map_err(|source| {
            inference_error(format!(
                "ColBERT batch-consistency smoke could not read batched matrix for text {text_index}: {source}"
            ))
        })?;
    let reference_rows = reference
        .to_device(&Device::Cpu)
        .and_then(|tensor| tensor.to_vec2::<f32>())
        .map_err(|source| {
            inference_error(format!(
                "ColBERT batch-consistency smoke could not read reference matrix for text {text_index}: {source}"
            ))
        })?;
    if batched_rows.len() != reference_rows.len() {
        return Err(inference_error(format!(
            "ColBERT batch-consistency smoke text {text_index} token counts differ: batched {}, singular {}",
            batched_rows.len(),
            reference_rows.len()
        )));
    }

    for (token_index, (batched_row, reference_row)) in
        batched_rows.iter().zip(reference_rows.iter()).enumerate()
    {
        if batched_row.len() != reference_row.len() {
            return Err(inference_error(format!(
                "ColBERT batch-consistency smoke text {text_index} token {token_index} dimensions differ: batched {}, singular {}",
                batched_row.len(),
                reference_row.len()
            )));
        }
        // Both rows are unit-norm from `project`, so their dot product is the
        // cosine similarity directly.
        let cosine: f32 = batched_row
            .iter()
            .zip(reference_row.iter())
            .map(|(left, right)| left * right)
            .sum();
        if !cosine.is_finite() || cosine < SMOKE_BATCH_COSINE_FLOOR {
            return Err(inference_error(format!(
                "ColBERT batch-consistency smoke text {text_index} token {token_index} cosine {cosine:.6} below floor {SMOKE_BATCH_COSINE_FLOOR}"
            )));
        }
    }

    Ok(())
}

/// Run one ColBERT startup smoke boundary with durable R5-shaped lifecycle logs.
fn run_colbert_startup_call<T, F>(
    diagnostics: ColbertStartupCallDiagnostics,
    call: F,
) -> Result<T, ApiError>
where
    F: FnOnce() -> Result<T, ApiError>,
{
    let context = crate::util::model_call_context("colbert", diagnostics.call_purpose);
    let _entered = context.enter();
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
