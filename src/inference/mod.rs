mod artifacts;
mod colbert;
mod dense;
mod device;
mod qwen3;
mod reranker;
mod tensor_ops;

use crate::{config::ServiceConfig, error::ApiError};

pub use artifacts::ModelArtifactSet;
pub use colbert::{ColbertCandidateScore, ColbertDocumentEmbedding, ColbertRuntime};
pub use dense::DenseEmbeddingRuntime;
pub use device::SelectedDevice;
pub use reranker::{RerankerCandidateInput, RerankerCandidateScore, RerankerRuntime};

#[derive(Debug, Clone)]
pub struct InferenceRuntime {
    pub device: SelectedDevice,
    pub artifacts: ModelArtifactSet,
    pub dense: DenseEmbeddingRuntime,
    pub colbert: ColbertRuntime,
    pub reranker: RerankerRuntime,
}

impl InferenceRuntime {
    /// Initialize the configured accelerator and validate model artifacts.
    pub fn initialize(config: &ServiceConfig) -> Result<Self, ApiError> {
        let device = device::initialize_device(&config.inference)?;
        let artifacts = ModelArtifactSet::load(&config.models)?;
        let dense =
            DenseEmbeddingRuntime::load(&artifacts.dense, &config.models.dense, &device.candle)?;
        let colbert =
            ColbertRuntime::load(&artifacts.colbert, &config.models.colbert, &device.candle)?;
        let reranker =
            RerankerRuntime::load(&artifacts.reranker, &config.models.reranker, &device.candle)?;

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
