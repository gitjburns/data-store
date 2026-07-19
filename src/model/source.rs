//! SourceObject and location model (spec §10) plus evidence-based deletion
//! inference (spec §11.1). Identity is content (`sourceHash`); presence is
//! location.

use serde::{Deserialize, Serialize};

/// Spec §10. One immutable content identity per `sourceHash`; raw bytes live
/// in the artifact store at `storageUri`. Acquiring identical content from a
/// new place appends or refreshes a `SourceLocation`, never a duplicate
/// SourceObject.
// Typed source rows are consumed when later clusters read them back (C9
// deletion propagation, C10a inspection); C3 writes these tables through
// SQL constants only. Remove the allow when first wired.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SourceObject {
    pub(crate) id: String,

    /// The only parse considered production truth (§10 rule 5); absent while
    /// no parse has activated or after deactivation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) active_parse_id: Option<String>,

    pub(crate) mime_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) size_bytes: Option<u64>,

    pub(crate) source_hash: String,
    pub(crate) storage_uri: String,

    pub(crate) locations: Vec<SourceLocation>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) event_time: Option<String>,
    pub(crate) ingest_time: String,
    pub(crate) created_at: String,

    /// Set when the source left the queryable plane because zero `current`
    /// locations remain (§11.3 step 4).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) deactivated_at: Option<String>,
}

/// Spec §10. One place a SourceObject's content has been observed. A rename
/// or move is one location ending and another beginning on the same content.
// See SourceObject above: consumed when later clusters read typed source
// rows; remove the allow when first wired.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SourceLocation {
    pub(crate) id: String,

    pub(crate) source_system: String,
    pub(crate) native_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) native_id: Option<String>,

    pub(crate) governance_domain: String,

    pub(crate) first_seen_at: String,
    pub(crate) last_seen_at: String,

    pub(crate) status: SourceLocationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) deletion_evidence: Option<DeletionEvidence>,

    /// Descriptive metadata is retained per location, losslessly, even when
    /// locations disagree (§10 rule 2).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metadata: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Spec §10 `status`. `access_lost` is not deletion (§11.2): the content
/// presumably still exists but can no longer be observed.
// See SourceObject above: consumed when later clusters read typed source
// rows; remove the allow when first wired.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SourceLocationStatus {
    Current,
    Deleted,
    AccessLost,
}

/// Spec §11.1. Deletion is inferred only from qualifying evidence, never
/// from absence counting; the record links back to the acquisition attempt
/// that observed the signal.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DeletionEvidence {
    pub(crate) signal: DeletionSignal,

    pub(crate) observed_at: String,
    pub(crate) acquisition_record_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) detail: Option<String>,
}

/// Spec §11.1 `signal`. The closed set of qualifying deletion signals; a
/// failed or partial enumeration asserts nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DeletionSignal {
    ExplicitDeleteEvent,
    AbsentFromCompleteEnumeration,
    SourceReportedGone,
}
