use std::{fs, path::Path, time::Instant};

use candle_core::{D, DType, Device, Tensor};
use candle_nn::{Embedding, Linear, Module, VarBuilder, embedding, linear_no_bias};
use serde::Deserialize;
use tracing::{error, info};

use crate::{
    error::ApiError,
    inference::{
        InferenceProgress,
        artifacts::ModelArtifacts,
        tensor_ops::{apply_rope, softmax_last_dim_metal_safe},
    },
};

#[derive(Debug, Clone, Deserialize)]
pub struct Qwen3Config {
    pub architectures: Vec<String>,
    pub head_dim: usize,
    pub hidden_act: String,
    pub hidden_size: usize,
    pub intermediate_size: usize,
    pub model_type: String,
    pub num_attention_heads: usize,
    pub num_hidden_layers: usize,
    pub num_key_value_heads: usize,
    pub rms_norm_eps: f64,
    pub rope_theta: f64,
    pub vocab_size: usize,
}

#[derive(Debug, Clone)]
pub struct Qwen3Model {
    embeddings: Embedding,
    layers: Vec<Qwen3Layer>,
    norm: MetalSafeRmsNorm,
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

impl Qwen3Model {
    /// Load a Qwen3 graph while reporting long model-load substeps to startup.
    ///
    /// `compute_dtype` is the on-device weight/activation dtype the whole
    /// forward runs in (the caller owns the choice — see the dense runtime's
    /// `DENSE_COMPUTE_DTYPE` rationale); safetensors weights are converted to
    /// it at memory-map time.
    pub fn load_with_progress(
        label: &str,
        config: &Qwen3Config,
        artifacts: &ModelArtifacts,
        device: &Device,
        compute_dtype: DType,
        tensor_prefix: Option<&str>,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        let context =
            crate::util::LogContext::new("model_call", &crate::util::diagnostic_id("call"));
        context.record("model_role", label);
        context.record("call_purpose", "startup_model_load");
        let _entered = context.enter();
        let started_at = Instant::now();
        info!(
            event = "model_call.started",
            model_role = label,
            call_purpose = "startup_model_load",
            layer_count = config.num_hidden_layers,
            hidden_size = config.hidden_size,
            head_count = config.num_attention_heads,
            kv_head_count = config.num_key_value_heads,
            vocab_size = config.vocab_size,
            safetensor_count = artifacts.safetensor_paths.len(),
            "Qwen3 startup model load started"
        );
        let load_result: Result<Self, ApiError> = (|| {
            validate_qwen3_config(label, config)?;
            progress(&format!("{label}_model_memory_mapping"))?;
            let vb = unsafe {
                VarBuilder::from_mmaped_safetensors(
                    &artifacts.safetensor_paths,
                    compute_dtype,
                    device,
                )
            }
            .map_err(|source| {
                inference_error(format!(
                    "failed to memory-map {label} safetensors from {}: {source}",
                    artifacts.root.display()
                ))
            })?;
            progress(&format!("{label}_model_memory_mapped"))?;
            let model_vb = tensor_prefix.map_or_else(|| vb.clone(), |prefix| vb.pp(prefix));
            progress(&format!("{label}_model_embeddings_loading"))?;
            let embeddings = embedding(
                config.vocab_size,
                config.hidden_size,
                model_vb.pp("embed_tokens"),
            )
            .map_err(|source| {
                inference_error(format!("failed to load {label} token embeddings: {source}"))
            })?;
            progress(&format!("{label}_model_embeddings_ready"))?;
            let mut layers = Vec::with_capacity(config.num_hidden_layers);
            for index in 0..config.num_hidden_layers {
                progress(&format!(
                    "{label}_model_layer_loading layer={}/{}",
                    index + 1,
                    config.num_hidden_layers
                ))?;
                layers.push(Qwen3Layer::load(
                    label,
                    config,
                    model_vb.pp(format!("layers.{index}")),
                )?);
            }
            progress(&format!(
                "{label}_model_layers_ready count={}",
                config.num_hidden_layers
            ))?;
            progress(&format!("{label}_model_final_norm_loading"))?;
            let norm = MetalSafeRmsNorm::load(
                config.hidden_size,
                config.rms_norm_eps,
                model_vb.pp("norm"),
            )
            .map_err(|source| {
                inference_error(format!("failed to load {label} final norm: {source}"))
            })?;
            progress(&format!("{label}_model_final_norm_ready"))?;

            Ok(Self {
                embeddings,
                layers,
                norm,
            })
        })();
        match &load_result {
            Ok(_) => {
                info!(
                    event = "model_call.completed",
                    model_role = label,
                    call_purpose = "startup_model_load",
                    layer_count = config.num_hidden_layers,
                    hidden_size = config.hidden_size,
                    head_count = config.num_attention_heads,
                    kv_head_count = config.num_key_value_heads,
                    vocab_size = config.vocab_size,
                    safetensor_count = artifacts.safetensor_paths.len(),
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "Qwen3 startup model load completed"
                );
            }
            Err(source) => {
                error!(
                    event = "model_call.failed",
                    model_role = label,
                    call_purpose = "startup_model_load",
                    layer_count = config.num_hidden_layers,
                    hidden_size = config.hidden_size,
                    head_count = config.num_attention_heads,
                    kv_head_count = config.num_key_value_heads,
                    vocab_size = config.vocab_size,
                    safetensor_count = artifacts.safetensor_paths.len(),
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    error = %source,
                    "Qwen3 startup model load failed"
                );
            }
        }

        load_result
    }

    /// Run token IDs through Qwen3 and return final hidden states.
    pub fn forward_hidden(&self, input_ids: &Tensor, label: &str) -> Result<Tensor, ApiError> {
        let mut hidden_states = self.embeddings.forward(input_ids).map_err(|source| {
            inference_error(format!("{label} embedding lookup failed: {source}"))
        })?;
        for layer in &self.layers {
            hidden_states = layer.forward(label, &hidden_states)?;
        }
        self.norm
            .forward(&hidden_states)
            .map_err(|source| inference_error(format!("{label} final norm failed: {source}")))
    }
}

impl Qwen3Layer {
    /// Load one Qwen3 decoder layer from Hugging Face tensor names.
    fn load(label: &str, config: &Qwen3Config, vb: VarBuilder) -> Result<Self, ApiError> {
        let input_layernorm = MetalSafeRmsNorm::load(
            config.hidden_size,
            config.rms_norm_eps,
            vb.pp("input_layernorm"),
        )
        .map_err(|source| {
            inference_error(format!("failed to load {label} input layernorm: {source}"))
        })?;
        let self_attn = Qwen3Attention::load(label, config, vb.pp("self_attn"))?;
        let post_attention_layernorm = MetalSafeRmsNorm::load(
            config.hidden_size,
            config.rms_norm_eps,
            vb.pp("post_attention_layernorm"),
        )
        .map_err(|source| {
            inference_error(format!(
                "failed to load {label} post-attention layernorm: {source}"
            ))
        })?;
        let mlp = Qwen3Mlp::load(label, config, vb.pp("mlp"))?;

        Ok(Self {
            input_layernorm,
            self_attn,
            post_attention_layernorm,
            mlp,
        })
    }

    /// Apply one causal decoder layer and preserve the hidden-state contract.
    fn forward(&self, label: &str, hidden_states: &Tensor) -> Result<Tensor, ApiError> {
        let residual = hidden_states;
        let attention_input = self
            .input_layernorm
            .forward(hidden_states)
            .map_err(|source| {
                inference_error(format!("{label} input layernorm failed: {source}"))
            })?;
        let attention_output = self.self_attn.forward(label, &attention_input)?;
        let hidden_states = (attention_output + residual).map_err(|source| {
            inference_error(format!("{label} attention residual failed: {source}"))
        })?;

        let residual = &hidden_states;
        let mlp_input = self
            .post_attention_layernorm
            .forward(&hidden_states)
            .map_err(|source| {
                inference_error(format!("{label} post-attention layernorm failed: {source}"))
            })?;
        let mlp_output = self.mlp.forward(label, &mlp_input)?;
        (mlp_output + residual)
            .map_err(|source| inference_error(format!("{label} mlp residual failed: {source}")))
    }
}

impl Qwen3Attention {
    /// Load Qwen3 grouped-query attention projections and per-head norms.
    fn load(label: &str, config: &Qwen3Config, vb: VarBuilder) -> Result<Self, ApiError> {
        let q_proj = linear_no_bias(
            config.hidden_size,
            config.num_attention_heads * config.head_dim,
            vb.pp("q_proj"),
        )
        .map_err(|source| inference_error(format!("failed to load {label} q_proj: {source}")))?;
        let k_proj = linear_no_bias(
            config.hidden_size,
            config.num_key_value_heads * config.head_dim,
            vb.pp("k_proj"),
        )
        .map_err(|source| inference_error(format!("failed to load {label} k_proj: {source}")))?;
        let v_proj = linear_no_bias(
            config.hidden_size,
            config.num_key_value_heads * config.head_dim,
            vb.pp("v_proj"),
        )
        .map_err(|source| inference_error(format!("failed to load {label} v_proj: {source}")))?;
        let o_proj = linear_no_bias(
            config.num_attention_heads * config.head_dim,
            config.hidden_size,
            vb.pp("o_proj"),
        )
        .map_err(|source| inference_error(format!("failed to load {label} o_proj: {source}")))?;
        let q_norm = MetalSafeRmsNorm::load(config.head_dim, config.rms_norm_eps, vb.pp("q_norm"))
            .map_err(|source| {
                inference_error(format!("failed to load {label} q_norm: {source}"))
            })?;
        let k_norm = MetalSafeRmsNorm::load(config.head_dim, config.rms_norm_eps, vb.pp("k_norm"))
            .map_err(|source| {
                inference_error(format!("failed to load {label} k_norm: {source}"))
            })?;

        Ok(Self {
            q_proj,
            k_proj,
            v_proj,
            o_proj,
            q_norm,
            k_norm,
            num_attention_heads: config.num_attention_heads,
            num_key_value_heads: config.num_key_value_heads,
            head_dim: config.head_dim,
            rope_theta: config.rope_theta,
        })
    }

    /// Apply causal grouped-query attention for a full prompt.
    fn forward(&self, label: &str, hidden_states: &Tensor) -> Result<Tensor, ApiError> {
        let (batch_size, seq_len, _) = hidden_states.dims3().map_err(|source| {
            inference_error(format!("{label} attention input shape error: {source}"))
        })?;
        let q = self
            .q_proj
            .forward(hidden_states)
            .and_then(|tensor| {
                tensor.reshape((batch_size, seq_len, self.num_attention_heads, self.head_dim))
            })
            .and_then(|tensor| tensor.transpose(1, 2))
            .and_then(|tensor| self.q_norm.forward(&tensor))
            .map_err(|source| inference_error(format!("{label} q projection failed: {source}")))?;
        let k = self
            .k_proj
            .forward(hidden_states)
            .and_then(|tensor| {
                tensor.reshape((batch_size, seq_len, self.num_key_value_heads, self.head_dim))
            })
            .and_then(|tensor| tensor.transpose(1, 2))
            .and_then(|tensor| self.k_norm.forward(&tensor))
            .map_err(|source| inference_error(format!("{label} k projection failed: {source}")))?;
        let v = self
            .v_proj
            .forward(hidden_states)
            .and_then(|tensor| {
                tensor.reshape((batch_size, seq_len, self.num_key_value_heads, self.head_dim))
            })
            .and_then(|tensor| tensor.transpose(1, 2))
            .map_err(|source| inference_error(format!("{label} v projection failed: {source}")))?;

        let q = apply_rope(&q, self.rope_theta)
            .map_err(|source| inference_error(format!("{label} query rope failed: {source}")))?;
        let k = apply_rope(&k, self.rope_theta)
            .map_err(|source| inference_error(format!("{label} key rope failed: {source}")))?;
        let k = repeat_kv_heads(
            &k,
            self.num_attention_heads,
            self.num_key_value_heads,
            label,
        )?;
        let v = repeat_kv_heads(
            &v,
            self.num_attention_heads,
            self.num_key_value_heads,
            label,
        )?;
        let attention_scores = q
            .matmul(&k.t().map_err(|source| {
                inference_error(format!("{label} key transpose failed: {source}"))
            })?)
            .and_then(|tensor| tensor / (self.head_dim as f64).sqrt())
            .and_then(|tensor| apply_causal_mask(&tensor, seq_len))
            .map_err(|source| {
                inference_error(format!("{label} attention scores failed: {source}"))
            })?;
        let attention_probs = softmax_last_dim_metal_safe(&attention_scores).map_err(|source| {
            inference_error(format!("{label} attention softmax failed: {source}"))
        })?;
        let attention_output = attention_probs
            .matmul(&v)
            .and_then(|tensor| tensor.transpose(1, 2))
            .and_then(|tensor| {
                tensor.reshape((
                    batch_size,
                    seq_len,
                    self.num_attention_heads * self.head_dim,
                ))
            })
            .map_err(|source| {
                inference_error(format!("{label} attention output failed: {source}"))
            })?;

        self.o_proj
            .forward(&attention_output)
            .map_err(|source| inference_error(format!("{label} o projection failed: {source}")))
    }
}

impl Qwen3Mlp {
    /// Load Qwen3 gated MLP projections.
    fn load(label: &str, config: &Qwen3Config, vb: VarBuilder) -> Result<Self, ApiError> {
        let gate_proj = linear_no_bias(
            config.hidden_size,
            config.intermediate_size,
            vb.pp("gate_proj"),
        )
        .map_err(|source| inference_error(format!("failed to load {label} gate_proj: {source}")))?;
        let up_proj = linear_no_bias(
            config.hidden_size,
            config.intermediate_size,
            vb.pp("up_proj"),
        )
        .map_err(|source| inference_error(format!("failed to load {label} up_proj: {source}")))?;
        let down_proj = linear_no_bias(
            config.intermediate_size,
            config.hidden_size,
            vb.pp("down_proj"),
        )
        .map_err(|source| inference_error(format!("failed to load {label} down_proj: {source}")))?;

        Ok(Self {
            gate_proj,
            up_proj,
            down_proj,
        })
    }

    /// Apply the Qwen3 SiLU-gated feed-forward block.
    fn forward(&self, label: &str, hidden_states: &Tensor) -> Result<Tensor, ApiError> {
        let gate = self.gate_proj.forward(hidden_states).map_err(|source| {
            inference_error(format!("{label} gate projection failed: {source}"))
        })?;
        let up = self
            .up_proj
            .forward(hidden_states)
            .map_err(|source| inference_error(format!("{label} up projection failed: {source}")))?;
        let activated = candle_nn::ops::silu(&gate)
            .and_then(|tensor| tensor.mul(&up))
            .map_err(|source| {
                inference_error(format!("{label} gated activation failed: {source}"))
            })?;

        self.down_proj
            .forward(&activated)
            .map_err(|source| inference_error(format!("{label} down projection failed: {source}")))
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

/// Load the Hugging Face Qwen3 config subset used by service-local adapters.
pub fn load_qwen3_config(label: &str, path: &Path) -> Result<Qwen3Config, ApiError> {
    let raw = fs::read_to_string(path).map_err(|source| ApiError::InferenceInit {
        message: format!(
            "failed to read {label} config at {}: {source}",
            path.display()
        ),
    })?;
    serde_json::from_str(&raw).map_err(|source| ApiError::InferenceInit {
        message: format!(
            "failed to parse {label} config at {}: {source}",
            path.display()
        ),
    })
}

/// Validate Qwen3 model metadata before graph loading relies on local adapter assumptions.
fn validate_qwen3_config(label: &str, config: &Qwen3Config) -> Result<(), ApiError> {
    if config.model_type != "qwen3" {
        return Err(inference_error(format!(
            "{label} model_type must be qwen3, got {}",
            config.model_type
        )));
    }
    if !config
        .architectures
        .iter()
        .any(|architecture| architecture == "Qwen3ForCausalLM")
    {
        return Err(inference_error(format!(
            "{label} architectures must include Qwen3ForCausalLM"
        )));
    }
    if config.hidden_act != "silu" {
        return Err(inference_error(format!(
            "{label} hidden_act must be silu, got {}",
            config.hidden_act
        )));
    }
    if !config
        .num_attention_heads
        .is_multiple_of(config.num_key_value_heads)
    {
        return Err(inference_error(format!(
            "{label} num_attention_heads must be divisible by num_key_value_heads"
        )));
    }
    if config.head_dim == 0 {
        return Err(inference_error(format!(
            "{label} head_dim must be positive"
        )));
    }
    Ok(())
}

/// Repeat grouped key/value heads so attention heads can consume them directly.
fn repeat_kv_heads(
    states: &Tensor,
    num_attention_heads: usize,
    num_key_value_heads: usize,
    label: &str,
) -> Result<Tensor, ApiError> {
    if num_attention_heads == num_key_value_heads {
        return Ok(states.clone());
    }

    let repeat_count = num_attention_heads / num_key_value_heads;
    let mut heads = Vec::with_capacity(num_attention_heads);
    for head_index in 0..num_key_value_heads {
        let head = states
            .narrow(1, head_index, 1)
            .map_err(|source| inference_error(format!("{label} kv head split failed: {source}")))?;
        for _ in 0..repeat_count {
            heads.push(head.clone());
        }
    }
    let head_refs = heads.iter().collect::<Vec<_>>();
    // The cat over narrowed head views returns a stride-PERMUTED view (buffer
    // ordered [heads, batch, seq, dim]), and candle 0.10.2's Metal matmul
    // silently miscomputes that layout for batch rows >= 1 (the CPU backend
    // rejects the same layout as MatMulUnexpectedStriding; verified by the
    // dense-batch-diagnostic reproducer, 2026-07-18). At batch 1 the layout is
    // degenerate-equivalent to contiguous, which is why singular embedding was
    // always correct. Materialize to standard layout so both attention matmuls
    // (q @ k^T and probs @ v) consume safe inputs; a no-op copy if a future
    // candle returns contiguous cats.
    Tensor::cat(&head_refs, 1)
        .and_then(|repeated| repeated.contiguous())
        .map_err(|source| inference_error(format!("{label} kv head repeat failed: {source}")))
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

/// Convert a Candle or tokenizer failure into the service inference error shape.
fn inference_error(message: String) -> ApiError {
    ApiError::InferenceInit { message }
}
