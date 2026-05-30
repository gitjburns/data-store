mod artifacts;
mod device;

use crate::{config::ServiceConfig, error::ApiError};

pub use artifacts::ModelArtifactSet;
pub use device::SelectedDevice;

#[derive(Debug, Clone)]
pub struct InferenceRuntime {
    pub device: SelectedDevice,
    pub artifacts: ModelArtifactSet,
}

impl InferenceRuntime {
    /// Initialize the configured accelerator and validate model artifacts.
    pub fn initialize(config: &ServiceConfig) -> Result<Self, ApiError> {
        let device = device::initialize_device(&config.inference)?;
        let artifacts = ModelArtifactSet::load(&config.models)?;

        Ok(Self { device, artifacts })
    }

    /// Return human-readable readiness details for health diagnostics.
    pub fn health_details(&self) -> Vec<String> {
        let mut details = vec![format!("device ready: {}", self.device.label())];
        details.extend(self.artifacts.health_details());
        details
    }
}
