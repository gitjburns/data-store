mod artifacts;
mod colbert;
mod dense;
mod device;
mod qwen3;
mod reranker;
mod reranker_backend;
mod tensor_ops;

use crate::{config::ServiceConfig, error::ApiError};

pub use artifacts::ModelArtifactSet;
pub use colbert::{ColbertCandidateScore, ColbertDocumentEmbedding, ColbertRuntime};
pub use dense::DenseEmbeddingRuntime;
pub use device::SelectedDevice;
pub use reranker::{RerankerCandidateInput, RerankerCandidateScore, RerankerRuntime};
pub use reranker_backend::RerankerBackend;

pub type InferenceProgress<'progress> = &'progress mut dyn FnMut(&str) -> Result<(), ApiError>;

#[derive(Debug, Clone)]
pub struct InferenceRuntime {
    pub device: SelectedDevice,
    pub artifacts: ModelArtifactSet,
    pub dense: DenseEmbeddingRuntime,
    pub colbert: ColbertRuntime,
    pub reranker: RerankerBackend,
}

impl InferenceRuntime {
    /// Initialize the configured accelerator and validate model artifacts.
    pub fn initialize(config: &ServiceConfig) -> Result<Self, ApiError> {
        let mut progress = ignore_inference_progress;
        Self::initialize_with_progress(config, &mut progress)
    }

    /// Initialize only the configured accelerator and ColBERT runtime for isolated diagnostics.
    #[allow(dead_code)]
    pub fn initialize_colbert_only_with_progress(
        config: &ServiceConfig,
        progress: InferenceProgress<'_>,
    ) -> Result<ColbertRuntime, ApiError> {
        progress("device_initializing")?;
        let device = device::initialize_device(&config.inference)?;
        progress(&format!("device_ready details=\"{}\"", device.label()))?;
        progress("colbert_artifacts_validating")?;
        let artifacts = artifacts::ModelArtifacts::load("colbert", &config.models.colbert.path)?;
        progress("colbert_artifacts_ready")?;
        progress("colbert_loading")?;
        let colbert = ColbertRuntime::load_with_progress(
            &artifacts,
            &config.models.colbert,
            &device.candle,
            progress,
        )?;
        progress("colbert_ready")?;

        Ok(colbert)
    }

    /// Initialize inference while emitting operator-visible startup progress.
    pub fn initialize_with_progress(
        config: &ServiceConfig,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        progress("device_initializing")?;
        let device = device::initialize_device(&config.inference)?;
        progress(&format!("device_ready details=\"{}\"", device.label()))?;
        progress("artifacts_validating")?;
        let artifacts = ModelArtifactSet::load(&config.models)?;
        progress("artifacts_ready")?;
        progress("dense_loading")?;
        let dense = DenseEmbeddingRuntime::load_with_progress(
            &artifacts.dense,
            &config.models.dense,
            &device.candle,
            progress,
        )?;
        progress("dense_ready")?;
        progress("colbert_loading")?;
        let colbert = ColbertRuntime::load_with_progress(
            &artifacts.colbert,
            &config.models.colbert,
            &device.candle,
            progress,
        )?;
        progress("colbert_ready")?;
        progress("reranker_loading")?;
        let reranker = RerankerBackend::Local(RerankerRuntime::load_with_progress(
            &artifacts.reranker,
            &config.models.reranker,
            &device.candle,
            progress,
        )?);
        progress("reranker_ready")?;

        Ok(Self {
            device,
            artifacts,
            dense,
            colbert,
            reranker,
        })
    }

    /// Return human-readable readiness details for health diagnostics.
    pub fn health_details(&self) -> Vec<String> {
        let mut details = vec![format!("device ready: {}", self.device.label())];
        details.extend(self.artifacts.health_details());
        details.extend(self.dense.health_details());
        details.extend(self.colbert.health_details());
        details.extend(self.reranker.health_details());
        details
    }
}

/// Accept startup progress messages without emitting them for non-interactive inference callers.
pub(crate) fn ignore_inference_progress(_message: &str) -> Result<(), ApiError> {
    Ok(())
}
