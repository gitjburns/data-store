mod artifacts;
mod dense;
mod device;

use crate::{config::ServiceConfig, error::ApiError};

pub use artifacts::ModelArtifactSet;
pub use dense::DenseEmbeddingRuntime;
pub use device::SelectedDevice;

#[derive(Debug, Clone)]
pub struct InferenceRuntime {
    pub device: SelectedDevice,
    pub artifacts: ModelArtifactSet,
    pub dense: DenseEmbeddingRuntime,
}

impl InferenceRuntime {
    /// Initialize the configured accelerator and validate model artifacts.
    pub fn initialize(config: &ServiceConfig) -> Result<Self, ApiError> {
        let device = device::initialize_device(&config.inference)?;
        let artifacts = ModelArtifactSet::load(&config.models)?;
        let dense =
            DenseEmbeddingRuntime::load(&artifacts.dense, &config.models.dense, &device.candle)?;

        Ok(Self {
            device,
            artifacts,
            dense,
        })
    }

    /// Return human-readable readiness details for health diagnostics.
    pub fn health_details(&self) -> Vec<String> {
        let mut details = vec![format!("device ready: {}", self.device.label())];
        details.extend(self.artifacts.health_details());
        details.extend(self.dense.health_details());
        details
    }
}
