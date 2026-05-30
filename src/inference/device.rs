#[cfg(any(feature = "cuda", feature = "metal"))]
use candle_core::Device;

use crate::{
    config::{InferenceConfig, InferenceDeviceKind},
    error::ApiError,
};

#[derive(Debug, Clone)]
pub struct SelectedDevice {
    pub kind: InferenceDeviceKind,
    pub index: usize,
}

impl SelectedDevice {
    /// Return a stable display name for health diagnostics.
    pub fn label(&self) -> String {
        match self.kind {
            InferenceDeviceKind::Cuda => format!("cuda:{}", self.index),
            InferenceDeviceKind::Metal => format!("metal:{}", self.index),
        }
    }
}

/// Create the configured Candle device, failing clearly when the backend is unavailable.
pub fn initialize_device(config: &InferenceConfig) -> Result<SelectedDevice, ApiError> {
    match config.device {
        InferenceDeviceKind::Cuda => initialize_cuda(config.device_index),
        InferenceDeviceKind::Metal => initialize_metal(config.device_index),
    }
}

/// Initialize a CUDA device when the binary was compiled with CUDA support.
#[cfg(feature = "cuda")]
fn initialize_cuda(index: usize) -> Result<SelectedDevice, ApiError> {
    Device::new_cuda(index).map_err(|source| ApiError::InferenceInit {
        message: format!("failed to initialize CUDA device {index}: {source}"),
    })?;

    Ok(SelectedDevice {
        kind: InferenceDeviceKind::Cuda,
        index,
    })
}

/// Report an explicit build-feature error for CUDA when support is not compiled in.
#[cfg(not(feature = "cuda"))]
fn initialize_cuda(index: usize) -> Result<SelectedDevice, ApiError> {
    Err(ApiError::InferenceInit {
        message: format!(
            "config requested cuda:{index}, but this binary was not built with --features cuda"
        ),
    })
}

/// Initialize a Metal device when the binary was compiled with Metal support.
#[cfg(feature = "metal")]
fn initialize_metal(index: usize) -> Result<SelectedDevice, ApiError> {
    Device::new_metal(index).map_err(|source| ApiError::InferenceInit {
        message: format!("failed to initialize Metal device {index}: {source}"),
    })?;

    Ok(SelectedDevice {
        kind: InferenceDeviceKind::Metal,
        index,
    })
}

/// Report an explicit build-feature error for Metal when support is not compiled in.
#[cfg(not(feature = "metal"))]
fn initialize_metal(index: usize) -> Result<SelectedDevice, ApiError> {
    Err(ApiError::InferenceInit {
        message: format!(
            "config requested metal:{index}, but this binary was not built with --features metal"
        ),
    })
}
