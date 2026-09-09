use std::{
    fs,
    path::{Path, PathBuf},
};

use tokenizers::Tokenizer;

use crate::{
    config::{ColbertBackendKind, DenseBackendKind, ModelConfig, RerankerBackendKind},
    error::ApiError,
};

pub const CONFIG_FILE_NAME: &str = "config.json";
pub const TOKENIZER_FILE_NAME: &str = "tokenizer.json";
pub const SAFETENSORS_EXTENSION: &str = "safetensors";

#[derive(Debug, Clone)]
pub struct ModelArtifactSet {
    /// Dense local artifacts, present only for the local dense backend. The HTTP
    /// backend has no local model to validate, so this is `None` there (mirror
    /// of the reranker's optional artifacts).
    pub dense: Option<ModelArtifacts>,
    /// HTTP ColBERT loads only its tokenizer, never local inference weights.
    pub colbert: Option<ModelArtifacts>,
    pub reranker: Option<ModelArtifacts>,
}

#[derive(Debug, Clone)]
pub struct ModelArtifacts {
    pub name: &'static str,
    pub root: PathBuf,
    pub config_path: PathBuf,
    pub tokenizer_path: PathBuf,
    pub safetensor_paths: Vec<PathBuf>,
}

impl ModelArtifactSet {
    /// Validate all configured model directories and tokenizer files. All
    /// local artifacts are validated only when their backend is local;
    /// an HTTP backend has no local model directory to check.
    pub fn load(config: &ModelConfig) -> Result<Self, ApiError> {
        Ok(Self {
            dense: match config.dense.backend {
                DenseBackendKind::Local => {
                    Some(ModelArtifacts::load("dense", config.dense.local_path()?)?)
                }
                DenseBackendKind::Http => None,
            },
            colbert: match config.colbert.backend {
                ColbertBackendKind::Local => Some(ModelArtifacts::load(
                    "colbert",
                    config.colbert.local_path()?,
                )?),
                ColbertBackendKind::Http => None,
            },
            reranker: match config.reranker.backend {
                RerankerBackendKind::Local => Some(ModelArtifacts::load(
                    "reranker",
                    config.reranker.local_path()?,
                )?),
                RerankerBackendKind::Http => None,
            },
        })
    }

    /// Return readiness details for all configured model artifact groups.
    pub fn health_details(&self) -> Vec<String> {
        let mut details = Vec::new();
        match &self.dense {
            Some(dense) => details.push(dense.health_detail()),
            None => details.push("dense artifacts ready: remote HTTP backend".to_string()),
        }
        match &self.colbert {
            Some(colbert) => details.push(colbert.health_detail()),
            None => details
                .push("colbert artifacts ready: remote HTTP backend (tokenizer only)".to_string()),
        }
        match &self.reranker {
            Some(reranker) => details.push(reranker.health_detail()),
            None => details.push("reranker artifacts ready: remote HTTP backend".to_string()),
        }
        details
    }
}

impl ModelArtifacts {
    /// Validate the minimum Hugging Face-style files required before graph loading.
    pub(super) fn load(name: &'static str, root: &Path) -> Result<Self, ApiError> {
        if !root.is_dir() {
            return Err(ApiError::InferenceInit {
                message: format!("models.{name}.path is not a directory: {}", root.display()),
            });
        }

        let config_path = root.join(CONFIG_FILE_NAME);
        require_file(name, CONFIG_FILE_NAME, &config_path)?;

        let tokenizer_path = root.join(TOKENIZER_FILE_NAME);
        require_file(name, TOKENIZER_FILE_NAME, &tokenizer_path)?;
        Tokenizer::from_file(&tokenizer_path).map_err(|source| ApiError::InferenceInit {
            message: format!(
                "failed to load {name} tokenizer at {}: {source}",
                tokenizer_path.display()
            ),
        })?;

        let safetensor_paths =
            find_safetensors(root).map_err(|source| ApiError::InferenceInit {
                message: format!(
                    "failed to inspect {name} model directory at {}: {source}",
                    root.display()
                ),
            })?;
        if safetensor_paths.is_empty() {
            return Err(ApiError::InferenceInit {
                message: format!("{name} model directory contains no .safetensors files"),
            });
        }

        Ok(Self {
            name,
            root: root.to_path_buf(),
            config_path,
            tokenizer_path,
            safetensor_paths,
        })
    }

    /// Summarize validated artifacts for health diagnostics.
    fn health_detail(&self) -> String {
        format!(
            "{} artifacts ready: {} safetensors, root {}, config {}, tokenizer {}",
            self.name,
            self.safetensor_paths.len(),
            self.root.display(),
            self.config_path.display(),
            self.tokenizer_path.display()
        )
    }
}

/// Ensure a required model artifact path exists as a file.
fn require_file(model_name: &str, artifact_name: &str, path: &Path) -> Result<(), ApiError> {
    if path.is_file() {
        return Ok(());
    }

    Err(ApiError::InferenceInit {
        message: format!(
            "{model_name} model is missing required {artifact_name} at {}",
            path.display()
        ),
    })
}

/// List safetensors shards directly inside one model directory.
fn find_safetensors(root: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut paths = Vec::new();

    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.extension().and_then(|extension| extension.to_str()) == Some(SAFETENSORS_EXTENSION)
        {
            paths.push(path);
        }
    }

    paths.sort();
    Ok(paths)
}
