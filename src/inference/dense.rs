use std::time::Instant;

use candle_core::{DType, Device, Tensor};
use tokenizers::Tokenizer;
use tracing::{error, info};

use crate::{
    config::DenseModelConfig,
    error::ApiError,
    inference::{
        InferenceProgress,
        artifacts::ModelArtifacts,
        qwen3::{Qwen3Model, load_qwen3_config},
    },
};

const QUERY_INSTRUCTION_PREFIX: &str =
    "Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery:";
/// Smoke text embedded once per backend at startup. `pub(crate)` so the HTTP
/// backend's startup smoke round-trips the SAME text the local runtime does,
/// keeping the readiness check identical across backends.
pub(crate) const DENSE_SMOKE_TEXT: &str = "dense embedding readiness smoke check";
const SMOKE_TEXT: &str = DENSE_SMOKE_TEXT;
const DENSE_POOLING_LAST_TOKEN: &str = "last_token";

/// Format a retrieval query into the exact text the dense model tokenizes: the
/// Qwen3 instruction prefix followed by the raw query. This is the SINGLE source
/// of truth for the query prompt shape — both the local runtime (`embed_query`)
/// and the HTTP backend build the final query text through this function, so a
/// backend switch can never fork the embedded semantics. The prefix string is
/// defined once here (`QUERY_INSTRUCTION_PREFIX`) and never duplicated.
pub(crate) fn format_dense_query_text(text: &str) -> String {
    format!("{QUERY_INSTRUCTION_PREFIX} {text}")
}

/// Format a retrieval passage into the exact text the dense model tokenizes.
/// Qwen3 passages carry no instruction prefix, so this is the identity; it
/// exists as the passage-side twin of `format_dense_query_text` so both the
/// local runtime and the HTTP backend route passage text through one named
/// boundary and a future prefix change lands in exactly one place per kind.
pub(crate) fn format_dense_passage_text(text: &str) -> &str {
    text
}

/// On-device compute dtype for the dense model: safetensors weights are
/// converted to this at load and the whole forward runs in it. BF16 matches
/// the shipped checkpoint and is a deliberate, validated choice: F16 was
/// benchmarked ~17-25% faster on the dominant Metal matmuls but REJECTED
/// 2026-07-18 (user-ruled) — the real-model cross-dtype validation
/// (`dense-batch-diagnostic --validate-dense-dtypes`) produced non-finite
/// activations under F16 (Qwen-family outlier activations overflow F16's
/// exponent range, e.g. in the RmsNorm square). BF16 keeps F32's exponent
/// range and has no such cliff. Re-run that validation before ever changing
/// this constant.
const DENSE_COMPUTE_DTYPE: DType = DType::BF16;

#[derive(Debug, Clone)]
pub struct DenseEmbeddingRuntime {
    tokenizer: Tokenizer,
    model: Qwen3Model,
    device: Device,
    max_tokens: usize,
    dimension: usize,
    smoke_norm: f32,
}

#[derive(Debug, Clone)]
struct DenseEmbeddingOutput {
    vector: Vec<f32>,
    token_count: usize,
}

impl DenseEmbeddingRuntime {
    /// Load the dense runtime while reporting tokenizer, model, and smoke-check
    /// progress. The service path always computes in `DENSE_COMPUTE_DTYPE`.
    pub fn load_with_progress(
        artifacts: &ModelArtifacts,
        config: &DenseModelConfig,
        device: &Device,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        Self::load_with_dtype(artifacts, config, device, DENSE_COMPUTE_DTYPE, progress)
    }

    /// Validation-only load at an explicit compute dtype, consumed by the
    /// dense-batch-diagnostic cross-dtype check (which embeds the same passages
    /// under two dtypes sequentially and compares cosines). The service startup
    /// path never calls this.
    #[allow(dead_code)]
    pub fn load_with_dtype_for_validation(
        artifacts: &ModelArtifacts,
        config: &DenseModelConfig,
        device: &Device,
        compute_dtype: DType,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        Self::load_with_dtype(artifacts, config, device, compute_dtype, progress)
    }

    /// Shared load body behind the public entry points; `compute_dtype` is
    /// threaded to the Qwen3 weight load and governs the whole forward.
    fn load_with_dtype(
        artifacts: &ModelArtifacts,
        config: &DenseModelConfig,
        device: &Device,
        compute_dtype: DType,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        validate_dense_config(config)?;
        // This runtime is the LOCAL backend; `local_max_tokens()` returns the
        // config-guaranteed local token cap and fails clearly if config selected
        // the HTTP backend (which has no local runtime to load).
        let max_tokens = config.local_max_tokens()? as usize;

        progress("dense_tokenizer_loading")?;
        let tokenizer = Tokenizer::from_file(&artifacts.tokenizer_path).map_err(|source| {
            ApiError::InferenceInit {
                message: format!(
                    "failed to load dense tokenizer at {}: {source}",
                    artifacts.tokenizer_path.display()
                ),
            }
        })?;
        progress("dense_tokenizer_ready")?;
        progress("dense_config_loading")?;
        let qwen_config = load_qwen3_config("dense", &artifacts.config_path)?;
        // A configurable input ceiling must never extend the checkpoint's positional capacity.
        if max_tokens > qwen_config.max_position_embeddings {
            return Err(inference_error(format!(
                "models.dense.max_tokens {max_tokens} exceeds dense max_position_embeddings {}",
                qwen_config.max_position_embeddings
            )));
        }
        progress("dense_config_ready")?;
        progress("dense_model_loading")?;
        let model = Qwen3Model::load_with_progress(
            "dense",
            &qwen_config,
            artifacts,
            device,
            compute_dtype,
            None,
            progress,
        )?;
        progress("dense_model_ready")?;
        let mut runtime = Self {
            tokenizer,
            model,
            device: device.clone(),
            max_tokens,
            dimension: config.dimension as usize,
            smoke_norm: 0.0,
        };
        progress("dense_smoke_passage")?;
        let passage_smoke =
            runtime.startup_smoke_embedding("startup_smoke_passage_embedding", "passage")?;
        progress("dense_smoke_query")?;
        let _query_smoke =
            runtime.startup_smoke_embedding("startup_smoke_query_embedding", "query")?;
        runtime.smoke_norm = l2_norm(&passage_smoke.vector);
        progress("dense_smoke_ready")?;

        Ok(runtime)
    }

    /// Return dense runtime readiness details for health diagnostics.
    pub fn health_details(&self) -> Vec<String> {
        vec![format!(
            "dense runtime ready: dim {}, max_tokens {}, smoke_norm {:.6}",
            self.dimension, self.max_tokens, self.smoke_norm
        )]
    }

    /// Section artifacts record complete model input. Reject text that the local
    /// dense tokenizer would truncate; existing fine-passage behavior is unchanged.
    pub(crate) fn embed_complete_passage_vector(&self, text: &str) -> Result<Vec<f32>, ApiError> {
        let mut tokenizer = self.tokenizer.clone();
        tokenizer.with_truncation(None).map_err(|source| {
            inference_error(format!(
                "failed to disable dense token counter truncation: {source}"
            ))
        })?;
        tokenizer.with_padding(None);
        let tokens = tokenizer
            .encode(format_dense_passage_text(text), true)
            .map_err(|source| {
                inference_error(format!("dense section tokenization failed: {source}"))
            })?
            .len();
        let limit = self
            .tokenizer
            .get_truncation()
            .map_or(self.max_tokens, |truncation| {
                truncation.max_length.min(self.max_tokens)
            });
        if tokens > limit {
            return Err(inference_error(format!(
                "dense section input requires {tokens} dense-model tokens, exceeding local limit {limit}; refusing truncated section embedding"
            )));
        }
        self.embed_passage_vector(text)
    }

    /// Embed a retrieval unit as a passage and return its dense vector.
    // Consumed by the dense builder's Local arm (projections/dense.rs
    // build_all_chunks) and the dense-batch-diagnostic bin's dtype validation.
    pub fn embed_passage_vector(&self, text: &str) -> Result<Vec<f32>, ApiError> {
        let context = crate::util::model_call_context("dense", "passage_embedding");
        let _entered = context.enter();
        let started_at = Instant::now();
        let text_chars = text.chars().count();
        info!(
            event = "model_call.started",
            model_role = "dense",
            call_purpose = "passage_embedding",
            input_kind = "passage",
            text_count = 1usize,
            text_chars,
            configured_max_tokens = self.max_tokens,
            expected_dimension = self.dimension,
            "dense passage embedding started"
        );
        let result = self.embed_passage(text).and_then(|output| {
            validate_dense_embedding_output(
                output,
                "passage_embedding",
                "passage",
                self.dimension,
                started_at.elapsed().as_millis() as u64,
            )
        });
        match &result {
            Ok(output) => {
                info!(
                    event = "model_call.completed",
                    model_role = "dense",
                    call_purpose = "passage_embedding",
                    input_kind = "passage",
                    text_count = 1usize,
                    text_chars,
                    token_count = output.token_count,
                    configured_max_tokens = self.max_tokens,
                    vector_dimension = output.vector.len(),
                    expected_dimension = self.dimension,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "dense passage embedding completed"
                );
            }
            Err(source) => {
                error!(
                    event = "model_call.failed",
                    model_role = "dense",
                    call_purpose = "passage_embedding",
                    input_kind = "passage",
                    text_count = 1usize,
                    text_chars,
                    configured_max_tokens = self.max_tokens,
                    expected_dimension = self.dimension,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    error = %source,
                    "dense passage embedding failed"
                );
            }
        }

        result.map(|output| output.vector)
    }

    /// Embed a retrieval query with the configured instruction prefix and return its dense vector.
    // Consumed by the query path (query/execute.rs dense query embedding, via
    // DenseEmbeddingBackend::embed_query_vector's Local arm).
    pub fn embed_query_vector(&self, text: &str) -> Result<Vec<f32>, ApiError> {
        let context = crate::util::model_call_context("dense", "query_embedding");
        let _entered = context.enter();
        let started_at = Instant::now();
        let text_chars = text.chars().count();
        info!(
            event = "model_call.started",
            model_role = "dense",
            call_purpose = "query_embedding",
            input_kind = "query",
            text_count = 1usize,
            text_chars,
            configured_max_tokens = self.max_tokens,
            expected_dimension = self.dimension,
            "dense query embedding started"
        );
        let result = self.embed_query(text).and_then(|output| {
            validate_dense_embedding_output(
                output,
                "query_embedding",
                "query",
                self.dimension,
                started_at.elapsed().as_millis() as u64,
            )
        });
        match &result {
            Ok(output) => {
                info!(
                    event = "model_call.completed",
                    model_role = "dense",
                    call_purpose = "query_embedding",
                    input_kind = "query",
                    text_count = 1usize,
                    text_chars,
                    token_count = output.token_count,
                    configured_max_tokens = self.max_tokens,
                    vector_dimension = output.vector.len(),
                    expected_dimension = self.dimension,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "dense query embedding completed"
                );
            }
            Err(source) => {
                error!(
                    event = "model_call.failed",
                    model_role = "dense",
                    call_purpose = "query_embedding",
                    input_kind = "query",
                    text_count = 1usize,
                    text_chars,
                    configured_max_tokens = self.max_tokens,
                    expected_dimension = self.dimension,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    error = %source,
                    "dense query embedding failed"
                );
            }
        }

        result.map(|output| output.vector)
    }

    /// Embed a retrieval query with the Qwen3 instruction prefix. The prompt
    /// text is built through the shared `format_dense_query_text` boundary so
    /// the local and HTTP backends tokenize/send byte-identical query text.
    fn embed_query(&self, text: &str) -> Result<DenseEmbeddingOutput, ApiError> {
        self.embed_text(&format_dense_query_text(text))
    }

    /// Embed a retrieval passage without an instruction prefix, routed through
    /// the shared `format_dense_passage_text` boundary (the identity for
    /// passages) so passage text has one formatting source of truth too.
    fn embed_passage(&self, text: &str) -> Result<DenseEmbeddingOutput, ApiError> {
        self.embed_text(format_dense_passage_text(text))
    }

    /// Run one startup smoke embedding and log the model-call boundary without exposing smoke text.
    fn startup_smoke_embedding(
        &self,
        call_purpose: &'static str,
        input_kind: &'static str,
    ) -> Result<DenseEmbeddingOutput, ApiError> {
        let context = crate::util::model_call_context("dense", call_purpose);
        let _entered = context.enter();
        let started_at = Instant::now();
        info!(
            event = "model_call.started",
            model_role = "dense",
            call_purpose,
            input_kind,
            text_count = 1usize,
            configured_max_tokens = self.max_tokens,
            expected_dimension = self.dimension,
            "dense startup smoke embedding started"
        );
        let result = match input_kind {
            "passage" => self.embed_passage(SMOKE_TEXT),
            "query" => self.embed_query(SMOKE_TEXT),
            _ => Err(ApiError::InferenceInit {
                message: format!("unsupported dense startup smoke input kind {input_kind}"),
            }),
        }
        .and_then(|output| {
            validate_dense_embedding_output(
                output,
                call_purpose,
                input_kind,
                self.dimension,
                started_at.elapsed().as_millis() as u64,
            )
        });
        match &result {
            Ok(output) => {
                info!(
                    event = "model_call.completed",
                    model_role = "dense",
                    call_purpose,
                    input_kind,
                    text_count = 1usize,
                    token_count = output.token_count,
                    configured_max_tokens = self.max_tokens,
                    vector_dimension = output.vector.len(),
                    expected_dimension = self.dimension,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "dense startup smoke embedding completed"
                );
            }
            Err(source) => {
                error!(
                    event = "model_call.failed",
                    model_role = "dense",
                    call_purpose,
                    input_kind,
                    text_count = 1usize,
                    configured_max_tokens = self.max_tokens,
                    expected_dimension = self.dimension,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    error = %source,
                    "dense startup smoke embedding failed"
                );
            }
        }

        result
    }

    /// Tokenize, truncate, run the model, last-token pool, and L2-normalize one
    /// text. This is the single boundary applying embedding truncation, pooling,
    /// and normalization for every dense entry point.
    fn embed_text(&self, text: &str) -> Result<DenseEmbeddingOutput, ApiError> {
        let ids = tokenize_truncated(&self.tokenizer, text, self.max_tokens)?;
        let token_count = ids.len();
        if token_count == 0 {
            return Err(ApiError::InferenceInit {
                message: "dense tokenizer produced no tokens for non-empty input".to_string(),
            });
        }

        let input = Tensor::new(ids.as_slice(), &self.device)
            .map_err(|source| {
                inference_error(format!("failed to build dense input tensor: {source}"))
            })?
            .unsqueeze(0)
            .map_err(|source| {
                inference_error(format!("failed to batch dense input tensor: {source}"))
            })?;
        let hidden = self.model.forward_hidden(&input, "dense")?;
        let (_, seq_len, hidden_size) = hidden.dims3().map_err(|source| {
            inference_error(format!("dense hidden state shape error: {source}"))
        })?;
        let embedding = hidden
            .narrow(1, seq_len - 1, 1)
            .and_then(|tensor| tensor.reshape((hidden_size,)))
            .and_then(|tensor| l2_normalize_tensor(&tensor))
            .and_then(|tensor| tensor.to_device(&Device::Cpu))
            .and_then(|tensor| tensor.to_vec1::<f32>())
            .map_err(|source| {
                inference_error(format!("failed to pool dense embedding: {source}"))
            })?;

        Ok(DenseEmbeddingOutput {
            vector: embedding,
            token_count,
        })
    }
}

/// Validate dense-model config values that are semantic, not only syntactic.
fn validate_dense_config(config: &DenseModelConfig) -> Result<(), ApiError> {
    if config.pooling != DENSE_POOLING_LAST_TOKEN {
        return Err(ApiError::InferenceInit {
            message: format!(
                "models.dense.pooling must be {DENSE_POOLING_LAST_TOKEN} for Qwen3 embeddings, got {}",
                config.pooling
            ),
        });
    }

    Ok(())
}

/// Validate dense model output before callers can persist or rank with it.
fn validate_dense_embedding_output(
    output: DenseEmbeddingOutput,
    call_purpose: &'static str,
    input_kind: &'static str,
    expected_dimension: usize,
    elapsed_ms: u64,
) -> Result<DenseEmbeddingOutput, ApiError> {
    let actual_dimension = output.vector.len();
    if actual_dimension != expected_dimension {
        return Err(dense_output_validation_error(
            call_purpose,
            input_kind,
            output.token_count,
            expected_dimension,
            actual_dimension,
            elapsed_ms,
            format!("dimension mismatch: got {actual_dimension}, expected {expected_dimension}"),
        ));
    }
    if output.vector.iter().any(|value| !value.is_finite()) {
        return Err(dense_output_validation_error(
            call_purpose,
            input_kind,
            output.token_count,
            expected_dimension,
            actual_dimension,
            elapsed_ms,
            "non-finite vector value".to_string(),
        ));
    }
    let norm = l2_norm(&output.vector);
    if !norm.is_finite() || norm <= 0.0 {
        return Err(dense_output_validation_error(
            call_purpose,
            input_kind,
            output.token_count,
            expected_dimension,
            actual_dimension,
            elapsed_ms,
            format!("invalid norm {norm}; expected finite nonzero norm"),
        ));
    }

    Ok(output)
}

/// Build an inference error with the local dense output facts needed to diagnose invalid model output.
fn dense_output_validation_error(
    call_purpose: &'static str,
    input_kind: &'static str,
    token_count: usize,
    expected_dimension: usize,
    actual_dimension: usize,
    elapsed_ms: u64,
    reason: String,
) -> ApiError {
    inference_error(format!(
        "dense model output validation failed: model_role=dense call_purpose={call_purpose} input_kind={input_kind} token_count={token_count} expected_dimension={expected_dimension} actual_dimension={actual_dimension} elapsed_ms={elapsed_ms} reason={reason}"
    ))
}

/// Tokenize one string and apply explicit service-owned truncation.
/// This local-only prefix policy is distinct from HTTP rejection of oversized input.
/// Complete annotation and section representations validate their full input before this call.
fn tokenize_truncated(
    tokenizer: &Tokenizer,
    text: &str,
    max_tokens: usize,
) -> Result<Vec<u32>, ApiError> {
    let encoding = tokenizer
        .encode(text, true)
        .map_err(|source| inference_error(format!("dense tokenization failed: {source}")))?;
    let mut ids = encoding.get_ids().to_vec();
    ids.truncate(max_tokens);
    Ok(ids)
}

/// Normalize one embedding tensor to unit length in f32.
fn l2_normalize_tensor(tensor: &Tensor) -> candle_core::Result<Tensor> {
    let tensor = tensor.to_dtype(DType::F32)?;
    let norm = tensor.sqr()?.sum_all()?.sqrt()?;
    tensor.broadcast_div(&norm)
}

/// Compute an f32 vector norm for readiness diagnostics.
fn l2_norm(vector: &[f32]) -> f32 {
    vector.iter().map(|value| value * value).sum::<f32>().sqrt()
}

/// Convert a Candle or tokenizer failure into the service inference error shape.
fn inference_error(message: String) -> ApiError {
    ApiError::InferenceInit { message }
}
