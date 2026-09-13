//! Selected PDF producer shared by ordinary ingestion and annotation dry runs.
//! Identity lookup and execution use the same variant, so the importer's expected
//! capability profile cannot disagree with the worker that produced its bundle.

use crate::runtime::StorageContext;
use std::path::PathBuf;

use crate::{
    config::{DoclingConfig, PdfConfig, PdfEngine},
    error::ApiError,
    model::ParserCapabilityProfile,
    source::ResolvedSource,
};

use super::{mupdf_worker, pdf_worker};

/// Startup-selected producer with only its applicable execution settings.
#[derive(Clone)]
pub(crate) enum PdfParser {
    Docling {
        config: DoclingConfig,
        document_timeout_seconds: u64,
    },
    MuPdf {
        document_timeout_seconds: u64,
    },
}

impl PdfParser {
    /// Capture validated startup settings once; no worker selects a fallback.
    pub(crate) fn from_config(
        pdf: &PdfConfig,
        docling: Option<&DoclingConfig>,
    ) -> Result<Self, ApiError> {
        match pdf.engine {
            PdfEngine::Docling => Ok(Self::Docling {
                config: docling
                    .ok_or_else(|| ApiError::InvalidConfig {
                        message: "[docling] is required when pdf.engine is docling".to_owned(),
                    })?
                    .clone(),
                document_timeout_seconds: pdf.document_timeout_seconds,
            }),
            PdfEngine::MuPdf => Ok(Self::MuPdf {
                document_timeout_seconds: pdf.document_timeout_seconds,
            }),
        }
    }

    /// Derive the exact identity the selected producer stamps into its bundles.
    pub(crate) fn effective_capability_profile(&self) -> Result<ParserCapabilityProfile, ApiError> {
        match self {
            Self::Docling {
                config,
                document_timeout_seconds,
            } => pdf_worker::effective_capability_profile(config, *document_timeout_seconds),
            Self::MuPdf {
                document_timeout_seconds,
            } => mupdf_worker::capability_profile(*document_timeout_seconds),
        }
    }

    /// Stage one successful or failed candidate bundle for the canonical importer.
    pub(crate) fn run(
        &self,
        index_root: &StorageContext,
        source: ResolvedSource,
        source_id: &str,
        source_hash: &str,
    ) -> Result<PathBuf, ApiError> {
        match self {
            Self::Docling {
                config,
                document_timeout_seconds,
            } => pdf_worker::run_pdf_parse(
                config,
                *document_timeout_seconds,
                index_root,
                source,
                source_id,
                source_hash,
            ),
            Self::MuPdf {
                document_timeout_seconds,
            } => mupdf_worker::run_pdf_parse(
                *document_timeout_seconds,
                index_root,
                &source.absolute_path,
                source_id,
                source_hash,
            ),
        }
    }
}
