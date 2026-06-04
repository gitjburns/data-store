use candle_core::{DType, Device, Tensor};
use tokenizers::Tokenizer;

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
const SMOKE_TEXT: &str = "dense embedding readiness smoke check";
const DENSE_POOLING_LAST_TOKEN: &str = "last_token";

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
}

impl DenseEmbeddingRuntime {
    /// Load the dense runtime while reporting tokenizer, model, and smoke-check progress.
    pub fn load_with_progress(
        artifacts: &ModelArtifacts,
        config: &DenseModelConfig,
        device: &Device,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        validate_dense_config(config)?;

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
        progress("dense_config_ready")?;
        progress("dense_model_loading")?;
        let model = Qwen3Model::load_with_progress(
            "dense",
            &qwen_config,
            artifacts,
            device,
            None,
            progress,
        )?;
        progress("dense_model_ready")?;
        let mut runtime = Self {
            tokenizer,
            model,
            device: device.clone(),
            max_tokens: config.max_tokens as usize,
            dimension: config.dimension as usize,
            smoke_norm: 0.0,
        };
        progress("dense_smoke_passage")?;
        let passage_smoke = runtime.embed_passage(SMOKE_TEXT)?;
        progress("dense_smoke_query")?;
        let query_smoke = runtime.embed_query(SMOKE_TEXT)?;
        if passage_smoke.vector.len() != runtime.dimension {
            return Err(ApiError::InferenceInit {
                message: format!(
                    "dense passage smoke embedding returned dimension {}, expected {}",
                    passage_smoke.vector.len(),
                    runtime.dimension
                ),
            });
        }
        if query_smoke.vector.len() != runtime.dimension {
            return Err(ApiError::InferenceInit {
                message: format!(
                    "dense query smoke embedding returned dimension {}, expected {}",
                    query_smoke.vector.len(),
                    runtime.dimension
                ),
            });
        }
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

    /// Embed a retrieval unit as a passage and return its dense vector.
    pub fn embed_passage_vector(&self, text: &str) -> Result<Vec<f32>, ApiError> {
        Ok(self.embed_passage(text)?.vector)
    }

    /// Embed a retrieval query with the configured instruction prefix and return its dense vector.
    pub fn embed_query_vector(&self, text: &str) -> Result<Vec<f32>, ApiError> {
        Ok(self.embed_query(text)?.vector)
    }

    /// Embed a retrieval query with the Qwen3 instruction prefix.
    fn embed_query(&self, text: &str) -> Result<DenseEmbeddingOutput, ApiError> {
        self.embed_text(&format!("{QUERY_INSTRUCTION_PREFIX} {text}"))
    }

    /// Embed a retrieval passage without an instruction prefix.
    fn embed_passage(&self, text: &str) -> Result<DenseEmbeddingOutput, ApiError> {
        self.embed_text(text)
    }

    /// Tokenize, truncate, run the model, last-token pool, and L2-normalize one text.
    ///
    /// This is the single boundary that applies embedding truncation, pooling, and normalization.
    fn embed_text(&self, text: &str) -> Result<DenseEmbeddingOutput, ApiError> {
        let ids = tokenize_truncated(&self.tokenizer, text, self.max_tokens)?;
        if ids.is_empty() {
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

        Ok(DenseEmbeddingOutput { vector: embedding })
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

/// Tokenize one string and apply explicit service-owned truncation.
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
