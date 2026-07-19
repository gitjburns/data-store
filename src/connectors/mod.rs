//! Acquisition connectors (spec §9.1): untrusted producers that interrogate
//! external source systems and emit staged acquisition bundles. Connectors
//! never write canonical storage or hot retrieval state; the acquisition
//! importer (`crate::acquisition`) validates and imports their staged output.
//!
//! This module also defines the staged-bundle contract shared by the
//! connector (producer), the importer (consumer), and the scheduler
//! (orchestrator). The contract is deliberately claims-based: every manifest
//! field is a connector claim, and the importer independently recomputes the
//! byte hash before trusting content identity.

pub(crate) mod filesystem;

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::model::AcquisitionFailureClass;

/// File name of the raw source bytes inside one staged bundle directory.
pub(crate) const BUNDLE_SOURCE_FILE_NAME: &str = "source.bin";

/// File name of the bundle manifest inside one staged bundle directory.
pub(crate) const BUNDLE_MANIFEST_FILE_NAME: &str = "manifest.json";

/// Staging root for acquisition bundles under the service-owned index root.
/// Staging is not canonical storage: bundles here are untrusted until
/// validated and imported by `crate::acquisition`.
pub(crate) fn acquisition_staging_root(index_root: &Path) -> PathBuf {
    index_root
        .join("fabric")
        .join("staging")
        .join("acquisition")
}

/// Deterministic bundle directory for one source location: the SHA-256 hex
/// of its native URI. One directory per location gives latest-state
/// coalescing (spec §9.4 rule 2) replace semantics for free: re-staging a
/// location atomically replaces its previous, not-yet-imported bundle.
pub(crate) fn bundle_dir_for(staging_root: &Path, native_uri: &str) -> PathBuf {
    staging_root.join(crate::canonical::sha256_hex_bytes(native_uri.as_bytes()))
}

/// Manifest describing one staged acquisition bundle (spec §9.1 rule 2).
/// Written by connectors, validated by the importer. All fields are
/// connector claims about one acquisition attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AcquisitionBundleManifest {
    pub(crate) connector_name: String,
    pub(crate) connector_version: String,
    pub(crate) connector_config_hash: String,
    pub(crate) capability_profile_hash: String,
    pub(crate) source_system: String,
    /// Absolute native identifier of the acquired item in its source system.
    pub(crate) native_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) native_id: Option<String>,
    /// Source-system-claimed modification time (RFC3339 UTC), when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) native_modified_at: Option<String>,
    pub(crate) governance_domain: String,
    /// Connector-claimed SHA-256 hex of the staged source bytes. The importer
    /// recomputes this from `source.bin`; a mismatch fails the import.
    pub(crate) claimed_source_hash: String,
    pub(crate) size_bytes: u64,
    /// When the connector read the source bytes (RFC3339 UTC).
    pub(crate) acquired_at: String,
    /// Wall-clock cost of acquiring this one item.
    pub(crate) elapsed_ms: u64,
}

/// Last-known per-location state the scheduler hands a connector so a full
/// scan can prescreen unchanged files instead of re-staging the whole
/// corpus. Claims only: content identity is still verified at import time by
/// hashing the staged bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct KnownLocationState {
    /// Native modified time in unix milliseconds from the last successful
    /// acquisition of this location.
    pub(crate) native_modified_at_ms: i64,
    /// Source byte size from the last successful acquisition.
    pub(crate) size_bytes: u64,
}

/// Why a full scan failed as a unit, discriminated by which side of the
/// trust boundary faulted. The scheduler routes the two arms differently:
/// a source-side failure is a meaningful acquisition outcome recorded as a
/// durable failed AcquisitionRecord at scope level (spec §9.2), while an
/// internal fault is a canonical-side error that propagates with no record.
#[derive(Debug)]
pub(crate) enum ScanError {
    /// The source system itself was unreadable at scan-precondition level
    /// (e.g. the corpus root cannot be statted or read): classified for the
    /// failure record, with local detail preserved.
    SourceSide {
        failure_class: AcquisitionFailureClass,
        detail: String,
    },
    /// Local infrastructure of the canonical side failed (e.g. the staging
    /// root cannot be created); not a statement about the source system.
    Internal(ApiError),
}

/// Per-item failure observed during a scan. The scheduler turns each one
/// into a durable failed AcquisitionRecord via the importer.
#[derive(Debug, Clone)]
pub(crate) struct ScanFailure {
    pub(crate) native_uri: String,
    pub(crate) failure_class: AcquisitionFailureClass,
    pub(crate) detail: String,
}

/// Outcome of one connector scan cycle. `enumeration_complete` gates
/// deletion inference: a failed or partial enumeration asserts nothing
/// about absent items (spec §11.1).
#[derive(Debug)]
pub(crate) struct FullScanOutcome {
    /// Bundle directories staged this cycle (new or changed items only).
    pub(crate) staged_bundle_dirs: Vec<PathBuf>,
    /// Every native URI observed by the scan, staged or not.
    pub(crate) enumerated_native_uris: BTreeSet<String>,
    /// True only when the scan enumerated its whole scope without error.
    pub(crate) enumeration_complete: bool,
    /// Items skipped because their known state matched the prescreen claims.
    pub(crate) skipped_unchanged: u64,
    /// Per-item failures; the scan continues past them.
    pub(crate) failures: Vec<ScanFailure>,
    pub(crate) elapsed_ms: u64,
}
