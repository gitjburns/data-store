use std::{fs, path::Path};

use candle_core::{D, DType, Device, Tensor};
use candle_nn::{Embedding, Linear, Module, VarBuilder, embedding, linear_no_bias};
use serde::Deserialize;
use tokenizers::Tokenizer;

use crate::{config::DenseModelConfig, error::ApiError, inference::artifacts::ModelArtifacts};

const QUERY_INSTRUCTION_PREFIX: &str =
    "Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery:";
const SMOKE_TEXT: &str = "dense embedding readiness smoke check";

#[derive(Debug, Clone)]
pub struct DenseEmbeddingRuntime {
    tokenizer: Tokenizer,
    model: Qwen3EmbeddingModel,
    device: Device,
    max_tokens: usize,
    dimension: usize,
    smoke_norm: f32,
}

#[derive(Debug, Clone)]
struct DenseEmbeddingOutput {
    vector: Vec<f32>,
}

#[derive(Debug, Clone, Deserialize)]
struct Qwen3Config {
    hidden_size: usize,
    intermediate_size: usize,
    num_attention_heads: usize,
    num_hidden_layers: usize,
    num_key_value_heads: usize,
    rms_norm_eps: f64,
    rope_theta: f64,
    vocab_size: usize,
}

#[derive(Debug, Clone)]
struct Qwen3EmbeddingModel {
    embeddings: Embedding,
    layers: Vec<Qwen3Layer>,
    norm: MetalSafeRmsNorm,
    config: Qwen3Config,
}

#[derive(Debug, Clone)]
struct Qwen3Layer {
    input_layernorm: MetalSafeRmsNorm,
    self_attn: Qwen3Attention,
    post_attention_layernorm: MetalSafeRmsNorm,
    mlp: Qwen3Mlp,
}

#[derive(Debug, Clone)]
struct Qwen3Attention {
    q_proj: Linear,
    k_proj: Linear,
    v_proj: Linear,
    o_proj: Linear,
    q_norm: MetalSafeRmsNorm,
    k_norm: MetalSafeRmsNorm,
    num_attention_heads: usize,
    num_key_value_heads: usize,
    head_dim: usize,
    rope_theta: f64,
}

#[derive(Debug, Clone)]
struct Qwen3Mlp {
    gate_proj: Linear,
    up_proj: Linear,
    down_proj: Linear,
}

#[derive(Debug, Clone)]
struct MetalSafeRmsNorm {
    weight: Tensor,
    eps: f64,
}

impl DenseEmbeddingRuntime {
    /// Load the dense tokenizer and Qwen3 embedding graph, then verify query and passage formatting paths.
    pub fn load(
        artifacts: &ModelArtifacts,
        config: &DenseModelConfig,
        device: &Device,
    ) -> Result<Self, ApiError> {
        validate_dense_config(config)?;

        let tokenizer = Tokenizer::from_file(&artifacts.tokenizer_path).map_err(|source| {
            ApiError::InferenceInit {
                message: format!(
                    "failed to load dense tokenizer at {}: {source}",
                    artifacts.tokenizer_path.display()
                ),
            }
        })?;
        let qwen_config = load_qwen3_config(&artifacts.config_path)?;
        let model = Qwen3EmbeddingModel::load(&qwen_config, artifacts, device)?;
        let mut runtime = Self {
            tokenizer,
            model,
            device: device.clone(),
            max_tokens: config.max_tokens as usize,
            dimension: config.dimension as usize,
            smoke_norm: 0.0,
        };
        let passage_smoke = runtime.embed_passage(SMOKE_TEXT)?;
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
        let hidden = self.model.forward(&input)?;
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

impl Qwen3EmbeddingModel {
    /// Load the service-local Qwen3 transformer blocks needed for embedding hidden states.
    fn load(
        config: &Qwen3Config,
        artifacts: &ModelArtifacts,
        device: &Device,
    ) -> Result<Self, ApiError> {
        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(&artifacts.safetensor_paths, DType::BF16, device)
        }
        .map_err(|source| {
            inference_error(format!(
                "failed to memory-map dense safetensors from {}: {source}",
                artifacts.root.display()
            ))
        })?;
        let embeddings = embedding(config.vocab_size, config.hidden_size, vb.pp("embed_tokens"))
            .map_err(|source| {
                inference_error(format!("failed to load dense embeddings: {source}"))
            })?;
        let mut layers = Vec::with_capacity(config.num_hidden_layers);
        for index in 0..config.num_hidden_layers {
            layers.push(Qwen3Layer::load(config, vb.pp(format!("layers.{index}")))?);
        }
        let norm = MetalSafeRmsNorm::load(config.hidden_size, config.rms_norm_eps, vb.pp("norm"))
            .map_err(|source| {
            inference_error(format!("failed to load dense final norm: {source}"))
        })?;

        Ok(Self {
            embeddings,
            layers,
            norm,
            config: config.clone(),
        })
    }

    /// Run token IDs through Qwen3 and return final hidden states.
    fn forward(&self, input_ids: &Tensor) -> Result<Tensor, ApiError> {
        let mut hidden_states = self.embeddings.forward(input_ids).map_err(|source| {
            inference_error(format!("dense embedding lookup failed: {source}"))
        })?;
        for layer in &self.layers {
            hidden_states = layer.forward(&hidden_states, &self.config)?;
        }
        self.norm
            .forward(&hidden_states)
            .map_err(|source| inference_error(format!("dense final norm failed: {source}")))
    }
}

impl Qwen3Layer {
    /// Load one Qwen3 decoder layer from Hugging Face tensor names.
    fn load(config: &Qwen3Config, vb: VarBuilder) -> Result<Self, ApiError> {
        let input_layernorm = MetalSafeRmsNorm::load(
            config.hidden_size,
            config.rms_norm_eps,
            vb.pp("input_layernorm"),
        )
        .map_err(|source| inference_error(format!("failed to load input layernorm: {source}")))?;
        let self_attn = Qwen3Attention::load(config, vb.pp("self_attn"))?;
        let post_attention_layernorm = MetalSafeRmsNorm::load(
            config.hidden_size,
            config.rms_norm_eps,
            vb.pp("post_attention_layernorm"),
        )
        .map_err(|source| {
            inference_error(format!("failed to load post-attention layernorm: {source}"))
        })?;
        let mlp = Qwen3Mlp::load(config, vb.pp("mlp"))?;

        Ok(Self {
            input_layernorm,
            self_attn,
            post_attention_layernorm,
            mlp,
        })
    }

    /// Apply one causal decoder layer and preserve the hidden-state contract.
    fn forward(&self, hidden_states: &Tensor, config: &Qwen3Config) -> Result<Tensor, ApiError> {
        let residual = hidden_states;
        let attention_input = self
            .input_layernorm
            .forward(hidden_states)
            .map_err(|source| inference_error(format!("dense input layernorm failed: {source}")))?;
        let attention_output = self.self_attn.forward(&attention_input, config)?;
        let hidden_states = (attention_output + residual).map_err(|source| {
            inference_error(format!("dense attention residual failed: {source}"))
        })?;

        let residual = &hidden_states;
        let mlp_input = self
            .post_attention_layernorm
            .forward(&hidden_states)
            .map_err(|source| {
                inference_error(format!("dense post-attention layernorm failed: {source}"))
            })?;
        let mlp_output = self.mlp.forward(&mlp_input)?;
        (mlp_output + residual)
            .map_err(|source| inference_error(format!("dense mlp residual failed: {source}")))
    }
}

impl Qwen3Attention {
    /// Load Qwen3 grouped-query attention projections and per-head norms.
    fn load(config: &Qwen3Config, vb: VarBuilder) -> Result<Self, ApiError> {
        let head_dim = config.hidden_size / config.num_attention_heads;
        let q_proj = linear_no_bias(
            config.hidden_size,
            config.num_attention_heads * head_dim,
            vb.pp("q_proj"),
        )
        .map_err(|source| inference_error(format!("failed to load q_proj: {source}")))?;
        let k_proj = linear_no_bias(
            config.hidden_size,
            config.num_key_value_heads * head_dim,
            vb.pp("k_proj"),
        )
        .map_err(|source| inference_error(format!("failed to load k_proj: {source}")))?;
        let v_proj = linear_no_bias(
            config.hidden_size,
            config.num_key_value_heads * head_dim,
            vb.pp("v_proj"),
        )
        .map_err(|source| inference_error(format!("failed to load v_proj: {source}")))?;
        let o_proj = linear_no_bias(
            config.num_attention_heads * head_dim,
            config.hidden_size,
            vb.pp("o_proj"),
        )
        .map_err(|source| inference_error(format!("failed to load o_proj: {source}")))?;
        let q_norm = MetalSafeRmsNorm::load(head_dim, config.rms_norm_eps, vb.pp("q_norm"))
            .map_err(|source| inference_error(format!("failed to load q_norm: {source}")))?;
        let k_norm = MetalSafeRmsNorm::load(head_dim, config.rms_norm_eps, vb.pp("k_norm"))
            .map_err(|source| inference_error(format!("failed to load k_norm: {source}")))?;

        Ok(Self {
            q_proj,
            k_proj,
            v_proj,
            o_proj,
            q_norm,
            k_norm,
            num_attention_heads: config.num_attention_heads,
            num_key_value_heads: config.num_key_value_heads,
            head_dim,
            rope_theta: config.rope_theta,
        })
    }

    /// Apply causal grouped-query attention for a full prompt.
    fn forward(&self, hidden_states: &Tensor, config: &Qwen3Config) -> Result<Tensor, ApiError> {
        let (batch_size, seq_len, _) = hidden_states.dims3().map_err(|source| {
            inference_error(format!("dense attention input shape error: {source}"))
        })?;
        let q = self
            .q_proj
            .forward(hidden_states)
            .and_then(|tensor| {
                tensor.reshape((batch_size, seq_len, self.num_attention_heads, self.head_dim))
            })
            .and_then(|tensor| tensor.transpose(1, 2))
            .and_then(|tensor| self.q_norm.forward(&tensor))
            .map_err(|source| inference_error(format!("dense q projection failed: {source}")))?;
        let k = self
            .k_proj
            .forward(hidden_states)
            .and_then(|tensor| {
                tensor.reshape((batch_size, seq_len, self.num_key_value_heads, self.head_dim))
            })
            .and_then(|tensor| tensor.transpose(1, 2))
            .and_then(|tensor| self.k_norm.forward(&tensor))
            .map_err(|source| inference_error(format!("dense k projection failed: {source}")))?;
        let v = self
            .v_proj
            .forward(hidden_states)
            .and_then(|tensor| {
                tensor.reshape((batch_size, seq_len, self.num_key_value_heads, self.head_dim))
            })
            .and_then(|tensor| tensor.transpose(1, 2))
            .map_err(|source| inference_error(format!("dense v projection failed: {source}")))?;

        let q = apply_rope(&q, self.rope_theta)?;
        let k = apply_rope(&k, self.rope_theta)?;
        let k = repeat_kv_heads(&k, self.num_attention_heads, self.num_key_value_heads)?;
        let v = repeat_kv_heads(&v, self.num_attention_heads, self.num_key_value_heads)?;
        let attention_scores = q
            .matmul(&k.t().map_err(|source| {
                inference_error(format!("dense key transpose failed: {source}"))
            })?)
            .and_then(|tensor| {
                (tensor / (self.head_dim as f64).sqrt()).map_err(candle_core::Error::from)
            })
            .and_then(|tensor| apply_causal_mask(&tensor, seq_len))
            .map_err(|source| {
                inference_error(format!("dense attention scores failed: {source}"))
            })?;
        let attention_probs = softmax_last_dim_metal_safe(&attention_scores).map_err(|source| {
            inference_error(format!("dense attention softmax failed: {source}"))
        })?;
        let attention_output = attention_probs
            .matmul(&v)
            .and_then(|tensor| tensor.transpose(1, 2))
            .and_then(|tensor| tensor.reshape((batch_size, seq_len, config.hidden_size)))
            .map_err(|source| {
                inference_error(format!("dense attention output failed: {source}"))
            })?;

        self.o_proj
            .forward(&attention_output)
            .map_err(|source| inference_error(format!("dense o projection failed: {source}")))
    }
}

impl Qwen3Mlp {
    /// Load Qwen3 gated MLP projections.
    fn load(config: &Qwen3Config, vb: VarBuilder) -> Result<Self, ApiError> {
        let gate_proj = linear_no_bias(
            config.hidden_size,
            config.intermediate_size,
            vb.pp("gate_proj"),
        )
        .map_err(|source| inference_error(format!("failed to load gate_proj: {source}")))?;
        let up_proj = linear_no_bias(
            config.hidden_size,
            config.intermediate_size,
            vb.pp("up_proj"),
        )
        .map_err(|source| inference_error(format!("failed to load up_proj: {source}")))?;
        let down_proj = linear_no_bias(
            config.intermediate_size,
            config.hidden_size,
            vb.pp("down_proj"),
        )
        .map_err(|source| inference_error(format!("failed to load down_proj: {source}")))?;

        Ok(Self {
            gate_proj,
            up_proj,
            down_proj,
        })
    }

    /// Apply the Qwen3 SiLU-gated feed-forward block.
    fn forward(&self, hidden_states: &Tensor) -> Result<Tensor, ApiError> {
        let gate = self
            .gate_proj
            .forward(hidden_states)
            .map_err(|source| inference_error(format!("dense gate projection failed: {source}")))?;
        let up = self
            .up_proj
            .forward(hidden_states)
            .map_err(|source| inference_error(format!("dense up projection failed: {source}")))?;
        let activated = candle_nn::ops::silu(&gate)
            .and_then(|tensor| tensor.mul(&up))
            .map_err(|source| {
                inference_error(format!("dense gated activation failed: {source}"))
            })?;

        self.down_proj
            .forward(&activated)
            .map_err(|source| inference_error(format!("dense down projection failed: {source}")))
    }
}

impl MetalSafeRmsNorm {
    /// Load RMSNorm weights while avoiding Candle's Metal-unsupported fused op.
    fn load(size: usize, eps: f64, vb: VarBuilder) -> candle_core::Result<Self> {
        Ok(Self {
            weight: vb.get(size, "weight")?,
            eps,
        })
    }

    /// Apply RMSNorm with primitive tensor operations that are available on Metal.
    fn forward(&self, hidden_states: &Tensor) -> candle_core::Result<Tensor> {
        let variance = hidden_states.sqr()?.mean_keepdim(D::Minus1)?;
        let normed = hidden_states.broadcast_div(&(variance + self.eps)?.sqrt()?)?;
        normed.broadcast_mul(&self.weight)
    }
}

/// Validate dense-model config values that are semantic, not only syntactic.
fn validate_dense_config(config: &DenseModelConfig) -> Result<(), ApiError> {
    if config.pooling != "last_token" {
        return Err(ApiError::InferenceInit {
            message: format!(
                "models.dense.pooling must be last_token for Qwen3 embeddings, got {}",
                config.pooling
            ),
        });
    }

    Ok(())
}

/// Load the Hugging Face Qwen3 config subset used by the dense adapter.
fn load_qwen3_config(path: &Path) -> Result<Qwen3Config, ApiError> {
    let raw = fs::read_to_string(path).map_err(|source| ApiError::InferenceInit {
        message: format!(
            "failed to read dense config at {}: {source}",
            path.display()
        ),
    })?;
    serde_json::from_str(&raw).map_err(|source| ApiError::InferenceInit {
        message: format!(
            "failed to parse dense config at {}: {source}",
            path.display()
        ),
    })
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

/// Apply Qwen/Llama rotary position embeddings to query or key states.
fn apply_rope(states: &Tensor, theta: f64) -> Result<Tensor, ApiError> {
    let (_, _, seq_len, head_dim) = states
        .dims4()
        .map_err(|source| inference_error(format!("dense rope shape error: {source}")))?;
    let device = states.device();
    let dtype = states.dtype();
    let half_dim = head_dim / 2;
    let mut cos = Vec::with_capacity(seq_len * head_dim);
    let mut sin = Vec::with_capacity(seq_len * head_dim);
    for position in 0..seq_len {
        for dim in 0..head_dim {
            let freq_index = dim % half_dim;
            let inv_freq = theta.powf(-(2.0 * freq_index as f64) / head_dim as f64);
            let angle = position as f64 * inv_freq;
            cos.push(angle.cos() as f32);
            sin.push(angle.sin() as f32);
        }
    }

    let cos = Tensor::from_vec(cos, (1, 1, seq_len, head_dim), device)
        .and_then(|tensor| tensor.to_dtype(dtype))
        .map_err(|source| {
            inference_error(format!("failed to build dense rope cosines: {source}"))
        })?;
    let sin = Tensor::from_vec(sin, (1, 1, seq_len, head_dim), device)
        .and_then(|tensor| tensor.to_dtype(dtype))
        .map_err(|source| inference_error(format!("failed to build dense rope sines: {source}")))?;
    let rotated = rotate_half(states)?;
    states
        .broadcast_mul(&cos)
        .and_then(|left| rotated.broadcast_mul(&sin).and_then(|right| left + right))
        .map_err(|source| inference_error(format!("failed to apply dense rope: {source}")))
}

/// Rotate the final dimension as `[-x2, x1]` for rotary embedding.
fn rotate_half(states: &Tensor) -> Result<Tensor, ApiError> {
    let (_, _, _, head_dim) = states
        .dims4()
        .map_err(|source| inference_error(format!("dense rotate-half shape error: {source}")))?;
    let half_dim = head_dim / 2;
    let first = states.narrow(3, 0, half_dim).map_err(|source| {
        inference_error(format!("dense rotate-half first split failed: {source}"))
    })?;
    let second = states
        .narrow(3, half_dim, half_dim)
        .and_then(|tensor| tensor.neg())
        .map_err(|source| {
            inference_error(format!("dense rotate-half second split failed: {source}"))
        })?;

    Tensor::cat(&[&second, &first], 3)
        .map_err(|source| inference_error(format!("dense rotate-half concat failed: {source}")))
}

/// Repeat grouped key/value heads so attention heads can consume them directly.
fn repeat_kv_heads(
    states: &Tensor,
    num_attention_heads: usize,
    num_key_value_heads: usize,
) -> Result<Tensor, ApiError> {
    if num_attention_heads == num_key_value_heads {
        return Ok(states.clone());
    }

    let repeat_count = num_attention_heads / num_key_value_heads;
    let mut heads = Vec::with_capacity(num_attention_heads);
    for head_index in 0..num_key_value_heads {
        let head = states
            .narrow(1, head_index, 1)
            .map_err(|source| inference_error(format!("dense kv head split failed: {source}")))?;
        for _ in 0..repeat_count {
            heads.push(head.clone());
        }
    }
    let head_refs = heads.iter().collect::<Vec<_>>();
    Tensor::cat(&head_refs, 1)
        .map_err(|source| inference_error(format!("dense kv head repeat failed: {source}")))
}

/// Add an upper-triangular causal mask to attention scores.
fn apply_causal_mask(scores: &Tensor, seq_len: usize) -> candle_core::Result<Tensor> {
    let device = scores.device();
    let mut values = Vec::with_capacity(seq_len * seq_len);
    for row in 0..seq_len {
        for col in 0..seq_len {
            values.push(if col > row { f32::NEG_INFINITY } else { 0.0 });
        }
    }
    let mask =
        Tensor::from_vec(values, (1, 1, seq_len, seq_len), device)?.to_dtype(scores.dtype())?;
    scores.broadcast_add(&mask)
}

/// Apply softmax over the final dimension without Candle's Metal-unsupported fused op.
fn softmax_last_dim_metal_safe(scores: &Tensor) -> candle_core::Result<Tensor> {
    let output_dtype = scores.dtype();
    let scores = scores.to_dtype(DType::F32)?;
    let exp = scores.exp()?;
    let denominator = exp.sum_keepdim(D::Minus1)?;
    exp.broadcast_div(&denominator)?.to_dtype(output_dtype)
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
