mod artifacts;
mod colbert;
mod colbert_backend;
mod dense;
mod dense_backend;
mod device;
mod qwen3;
mod reranker;
mod reranker_backend;
mod tensor_ops;

use crate::{
    config::{ColbertBackendKind, DenseBackendKind, RerankerBackendKind, ServiceConfig},
    error::ApiError,
};

pub use artifacts::ModelArtifactSet;
// Retained inference API (pinned contract); consumed at C6c/C6e/C7c.
#[allow(unused_imports)]
pub use colbert::{ColbertCandidateScore, ColbertDocumentEmbedding, ColbertRuntime};
pub use colbert_backend::ColbertBackend;
pub use dense::DenseEmbeddingRuntime;
pub use dense_backend::{DenseEmbeddingBackend, HttpDenseClient};
pub use device::SelectedDevice;
pub use reranker::{RerankerCandidateInput, RerankerCandidateScore, RerankerRuntime};
pub use reranker_backend::RerankerBackend;

pub type InferenceProgress<'progress> = &'progress mut dyn FnMut(&str) -> Result<(), ApiError>;

#[derive(Debug, Clone)]
pub struct InferenceRuntime {
    /// Absent when all retrieval model inference runs on HTTP backends.
    pub device: Option<SelectedDevice>,
    pub artifacts: ModelArtifactSet,
    pub dense: DenseEmbeddingBackend,
    pub colbert: ColbertBackend,
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
        // This diagnostic is local-only. Reject HTTP selection before touching
        // accelerator APIs, including on builds with no accelerator features.
        let model_path = config.models.colbert.local_path()?;
        progress("device_initializing")?;
        let device = device::initialize_device(&config.inference)?;
        progress(&format!("device_ready details=\"{}\"", device.label()))?;
        progress("colbert_artifacts_validating")?;
        let artifacts = artifacts::ModelArtifacts::load("colbert", model_path)?;
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

    /// Initialize only the configured accelerator and dense runtime at an
    /// explicit compute dtype, for the dense-batch-diagnostic cross-dtype
    /// validation (`--validate-dense-dtypes`). The service startup path always
    /// goes through `initialize_with_progress`.
    #[allow(dead_code)]
    pub fn initialize_dense_for_dtype_validation(
        config: &ServiceConfig,
        compute_dtype: candle_core::DType,
        progress: InferenceProgress<'_>,
    ) -> Result<DenseEmbeddingRuntime, ApiError> {
        progress("device_initializing")?;
        let device = device::initialize_device(&config.inference)?;
        progress(&format!("device_ready details=\"{}\"", device.label()))?;
        progress("dense_artifacts_validating")?;
        // Cross-dtype validation is a LOCAL-backend-only diagnostic (it loads
        // the real Candle weights twice). `local_path()` fails clearly if config
        // selects the HTTP backend, which has no local artifacts to load.
        let artifacts =
            artifacts::ModelArtifacts::load("dense", config.models.dense.local_path()?)?;
        progress("dense_artifacts_ready")?;
        progress("dense_loading")?;
        let dense = DenseEmbeddingRuntime::load_with_dtype_for_validation(
            &artifacts,
            &config.models.dense,
            &device.candle,
            compute_dtype,
            progress,
        )?;
        progress("dense_ready")?;

        Ok(dense)
    }

    /// Initialize inference while emitting operator-visible startup progress.
    pub fn initialize_with_progress(
        config: &ServiceConfig,
        progress: InferenceProgress<'_>,
    ) -> Result<Self, ApiError> {
        // Remote model selection is explicit; a CPU-only host must never probe
        // CUDA/Metal or load weights when no local model was selected.
        let needs_accelerator = config.models.dense.backend == DenseBackendKind::Local
            || config.models.colbert.backend == ColbertBackendKind::Local
            || config.models.reranker.backend == RerankerBackendKind::Local;
        let device = if needs_accelerator {
            progress("device_initializing")?;
            let device = device::initialize_device(&config.inference)?;
            progress(&format!("device_ready details=\"{}\"", device.label()))?;
            Some(device)
        } else {
            progress(
                "device_not_required details=\"all retrieval models use HTTP; MaxSim uses CPU\"",
            )?;
            None
        };
        progress("artifacts_validating")?;
        let artifacts = ModelArtifactSet::load(&config.models)?;
        progress("artifacts_ready")?;
        // Dense backend selection (approved design): the local backend loads the
        // validated 8B artifacts and the Candle runtime (bit-identical to the
        // pre-split path); the HTTP backend skips local artifact validation and
        // model load entirely and runs an HTTP smoke round-trip instead. There
        // is no fallback between them.
        progress("dense_loading")?;
        let dense = match config.models.dense.backend {
            DenseBackendKind::Local => {
                let dense_artifacts =
                    artifacts
                        .dense
                        .as_ref()
                        .ok_or_else(|| ApiError::InferenceInit {
                            message: "local dense backend has no validated artifacts".to_string(),
                        })?;
                DenseEmbeddingBackend::local(DenseEmbeddingRuntime::load_with_progress(
                    dense_artifacts,
                    &config.models.dense,
                    selected_local_device(&device)?,
                    progress,
                )?)
            }
            DenseBackendKind::Http => DenseEmbeddingBackend::load_http_with_progress(
                &config.models.dense,
                config.config_root(),
                progress,
            )?,
        };
        progress("dense_ready")?;
        progress("colbert_loading")?;
        let colbert = match config.models.colbert.backend {
            ColbertBackendKind::Local => {
                let colbert_artifacts =
                    artifacts
                        .colbert
                        .as_ref()
                        .ok_or_else(|| ApiError::InferenceInit {
                            message: "local ColBERT backend has no validated artifacts".to_string(),
                        })?;
                ColbertBackend::local(ColbertRuntime::load_with_progress(
                    colbert_artifacts,
                    &config.models.colbert,
                    selected_local_device(&device)?,
                    progress,
                )?)
            }
            ColbertBackendKind::Http => ColbertBackend::load_http_with_progress(
                &config.models.colbert,
                config.config_root(),
                progress,
            )?,
        };
        progress("colbert_ready")?;
        progress("reranker_loading")?;
        let reranker = match config.models.reranker.backend {
            RerankerBackendKind::Local => {
                let reranker_artifacts =
                    artifacts
                        .reranker
                        .as_ref()
                        .ok_or_else(|| ApiError::InferenceInit {
                            message: "local reranker backend has no validated artifacts"
                                .to_string(),
                        })?;
                RerankerBackend::Local(Box::new(RerankerRuntime::load_with_progress(
                    reranker_artifacts,
                    &config.models.reranker,
                    selected_local_device(&device)?,
                    progress,
                )?))
            }
            RerankerBackendKind::Http => RerankerBackend::load_http_with_progress(
                &config.models.reranker,
                config.config_root(),
                progress,
            )?,
        };
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
        let mut details = vec![match &self.device {
            Some(device) => format!("device ready: {}", device.label()),
            None => "device: no local accelerator required; MaxSim uses CPU".to_string(),
        }];
        details.extend(self.artifacts.health_details());
        details.extend(self.dense.health_details());
        details.extend(self.colbert.health_details());
        details.extend(self.reranker.health_details());
        details
    }
}

/// Local model branches must have a deliberately initialized accelerator;
/// absence is a startup invariant failure, never an implicit CPU fallback.
fn selected_local_device(
    device: &Option<SelectedDevice>,
) -> Result<&candle_core::Device, ApiError> {
    device
        .as_ref()
        .map(|device| &device.candle)
        .ok_or_else(|| ApiError::InferenceInit {
            message: "local model selected without an initialized accelerator".to_string(),
        })
}

/// Accept startup progress messages without emitting them for non-interactive inference callers.
pub(crate) fn ignore_inference_progress(_message: &str) -> Result<(), ApiError> {
    Ok(())
}
