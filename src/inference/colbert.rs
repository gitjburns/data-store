use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use candle_core::{DType, Device, Tensor};
use candle_nn::{Linear, Module, VarBuilder, linear_no_bias};
use safetensors::{Dtype as SafeTensorDType, SafeTensors};
use serde::Deserialize;
use tokenizers::Tokenizer;

use crate::{config::ColbertModelConfig, error::ApiError, inference::artifacts::ModelArtifacts};

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

#[derive(Debug, Clone)]
pub struct ColbertRuntime {
    projection: ColbertProjection,
    query_tokens: usize,
    document_tokens: usize,
    hidden_size: usize,
    projection_dimension: usize,
    num_hidden_layers: usize,
    maxsim_score: f32,
}

#[derive(Debug, Clone)]
struct ColbertProjection {
    linear: Linear,
    path: PathBuf,
    in_features: usize,
    out_features: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct ModernBertConfig {
    architectures: Vec<String>,
    hidden_size: usize,
    intermediate_size: usize,
    model_type: String,
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

impl ColbertRuntime {
    /// Validate the ColBERT artifact contract and run projection-only smoke checks without executing ModernBERT.
    pub fn load(
        artifacts: &ModelArtifacts,
        config: &ColbertModelConfig,
        device: &Device,
    ) -> Result<Self, ApiError> {
        validate_colbert_config(config)?;

        let tokenizer = Tokenizer::from_file(&artifacts.tokenizer_path).map_err(|source| {
            inference_error(format!(
                "failed to load ColBERT tokenizer at {}: {source}",
                artifacts.tokenizer_path.display()
            ))
        })?;
        let model_config = load_modernbert_config(&artifacts.config_path)?;
        validate_modernbert_config(&model_config, config)?;
        validate_tokenizer_contract(artifacts, config)?;
        validate_root_safetensors(artifacts, &model_config)?;
        let projection = ColbertProjection::load(artifacts, config, device)?;

        let query_tokens = tokenize_formatted(
            &tokenizer,
            &format_query(SMOKE_QUERY),
            config.query_max_tokens as usize,
            "query",
        )?
        .len();
        let document_tokens = tokenize_formatted(
            &tokenizer,
            &format_document(SMOKE_DOCUMENT),
            config.document_max_tokens as usize,
            "document",
        )?
        .len();
        let query_hidden = synthetic_hidden_states(query_tokens, model_config.hidden_size, device)?;
        let document_hidden =
            synthetic_hidden_states(document_tokens, model_config.hidden_size, device)?;
        let query_projection = projection.project(&query_hidden)?;
        let document_projection = projection.project(&document_hidden)?;
        let maxsim_score = maxsim_score(&query_projection, &document_projection)?;

        Ok(Self {
            projection,
            query_tokens,
            document_tokens,
            hidden_size: model_config.hidden_size,
            projection_dimension: config.dimension as usize,
            num_hidden_layers: model_config.num_hidden_layers,
            maxsim_score,
        })
    }

    /// Return ColBERT loader/projection readiness details while clearly excluding encoder execution.
    pub fn health_details(&self) -> Vec<String> {
        vec![format!(
            "colbert contract/projection ready: architecture {}, layers {}, hidden {}, projection {}->{}, query_tokens {}, document_tokens {}, synthetic_maxsim {:.6}, projection_path {}, encoder_runtime not yet executed",
            EXPECTED_ARCHITECTURE,
            self.num_hidden_layers,
            self.hidden_size,
            self.projection.in_features,
            self.projection_dimension,
            self.query_tokens,
            self.document_tokens,
            self.maxsim_score,
            self.projection.path.display()
        )]
    }
}

impl ColbertProjection {
    /// Load the nested PyLate projection artifact that maps ModernBERT hidden states into ColBERT token vectors.
    fn load(
        artifacts: &ModelArtifacts,
        config: &ColbertModelConfig,
        device: &Device,
    ) -> Result<Self, ApiError> {
        let projection_dir = artifacts.root.join("1_Dense");
        let projection_config_path = projection_dir.join("config.json");
        let projection_path = projection_dir.join("model.safetensors");
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

    /// Apply the projection boundary from `[tokens, 768]` hidden states to `[tokens, 128]` ColBERT vectors.
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

        Ok(projected)
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
    if model_config.hidden_size % model_config.num_attention_heads != 0 {
        return Err(inference_error(
            "ColBERT hidden_size must divide evenly by num_attention_heads".to_string(),
        ));
    }
    if model_config.vocab_size == 0
        || model_config.num_hidden_layers == 0
        || model_config.intermediate_size == 0
    {
        return Err(inference_error(
            "ColBERT config must have positive vocab, layer, and intermediate sizes".to_string(),
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
    let tokenizer_json = read_json_file(&artifacts.tokenizer_path, "ColBERT tokenizer.json")?;
    let tokenizer_max = json_usize_at(
        &tokenizer_json,
        &["truncation", "max_length"],
        "tokenizer.json truncation.max_length",
    )?;
    let tokenizer_config_path = artifacts.root.join("tokenizer_config.json");
    let tokenizer_config = read_json_file(&tokenizer_config_path, "ColBERT tokenizer_config.json")?;
    let tokenizer_config_max = json_usize_at(
        &tokenizer_config,
        &["model_max_length"],
        "tokenizer_config.json model_max_length",
    )?;
    let sentence_config_path = artifacts.root.join("sentence_bert_config.json");
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

/// Build deterministic hidden states for projection smoke without invoking ModernBERT.
fn synthetic_hidden_states(
    tokens: usize,
    hidden_size: usize,
    device: &Device,
) -> Result<Tensor, ApiError> {
    let mut values = Vec::with_capacity(tokens * hidden_size);
    for token_index in 0..tokens {
        for hidden_index in 0..hidden_size {
            let value = (((token_index + 1) * (hidden_index + 3)) % 97) as f32 / 97.0;
            values.push(value);
        }
    }

    Tensor::from_vec(values, (tokens, hidden_size), device).map_err(|source| {
        inference_error(format!(
            "failed to build ColBERT synthetic hidden states: {source}"
        ))
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

/// Convert ColBERT runtime failures into the service inference error shape.
fn inference_error(message: String) -> ApiError {
    ApiError::InferenceInit { message }
}
