#[cfg(any(feature = "cuda", feature = "metal"))]
use std::panic::{AssertUnwindSafe, catch_unwind};

use candle_core::Device;
#[cfg(any(feature = "cuda", feature = "metal"))]
use candle_core::Error as CandleError;

use crate::{
    config::{InferenceConfig, InferenceDeviceKind},
    error::ApiError,
    limits::DiagnosticLimits,
};

#[derive(Debug, Clone)]
pub struct SelectedDevice {
    pub kind: InferenceDeviceKind,
    pub index: usize,
    pub candle: Device,
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
/// Initialization failures use the caller's diagnostic bounds before a runtime exists.
pub fn initialize_device(
    config: &InferenceConfig,
    diagnostics: &DiagnosticLimits,
) -> Result<SelectedDevice, ApiError> {
    match config.device {
        InferenceDeviceKind::Cuda => initialize_cuda(config.device_index, diagnostics),
        InferenceDeviceKind::Metal => initialize_metal(config.device_index, diagnostics),
    }
}

/// Initialize a CUDA device when the binary was compiled with CUDA support.
#[cfg(feature = "cuda")]
fn initialize_cuda(
    index: usize,
    diagnostics: &DiagnosticLimits,
) -> Result<SelectedDevice, ApiError> {
    let candle = create_candle_device(format!("CUDA device {index}"), diagnostics, || {
        Device::new_cuda(index)
    })?;

    Ok(SelectedDevice {
        kind: InferenceDeviceKind::Cuda,
        index,
        candle,
    })
}

/// Report an explicit build-feature error for CUDA when support is not compiled in.
#[cfg(not(feature = "cuda"))]
fn initialize_cuda(
    index: usize,
    _diagnostics: &DiagnosticLimits,
) -> Result<SelectedDevice, ApiError> {
    Err(ApiError::InferenceInit {
        message: format!(
            "config requested cuda:{index}, but this binary was not built with --features cuda"
        ),
    })
}

/// Initialize a Metal device when the binary was compiled with Metal support.
#[cfg(feature = "metal")]
fn initialize_metal(
    index: usize,
    diagnostics: &DiagnosticLimits,
) -> Result<SelectedDevice, ApiError> {
    let candle = create_candle_device(format!("Metal device {index}"), diagnostics, || {
        Device::new_metal(index)
    })?;

    Ok(SelectedDevice {
        kind: InferenceDeviceKind::Metal,
        index,
        candle,
    })
}

/// Report an explicit build-feature error for Metal when support is not compiled in.
#[cfg(not(feature = "metal"))]
fn initialize_metal(
    index: usize,
    _diagnostics: &DiagnosticLimits,
) -> Result<SelectedDevice, ApiError> {
    Err(ApiError::InferenceInit {
        message: format!(
            "config requested metal:{index}, but this binary was not built with --features metal"
        ),
    })
}

/// Create a Candle accelerator device and convert backend panics into readiness diagnostics.
#[cfg(any(feature = "cuda", feature = "metal"))]
fn create_candle_device<F>(
    label: String,
    diagnostics: &DiagnosticLimits,
    create: F,
) -> Result<Device, ApiError>
where
    F: FnOnce() -> Result<Device, CandleError>,
{
    match catch_unwind(AssertUnwindSafe(create)) {
        Ok(Ok(device)) => Ok(device),
        Ok(Err(source)) => Err(ApiError::InferenceInit {
            message: format!("failed to initialize {label}: {source}"),
        }),
        Err(payload) => Err(ApiError::InferenceInit {
            message: format!(
                "failed to initialize {label}: Candle backend panicked: {}",
                crate::util::panic_payload_message(payload.as_ref(), diagnostics)
            ),
        }),
    }
}
