//! ForensicSnapshot model (spec §30): the content-addressed snapshot header,
//! its replay-capability profile, and the manifest of hash-referenced
//! artifacts a snapshot immutably points at.
//!
//! Every heavy forensic artifact is immutable and content-hashed (§30.1); the
//! `ForensicSnapshot` header carries the queryable metadata (audits find a
//! snapshot by id/type/time/subject), while `ForensicSnapshotManifest` is the
//! archived object listing every referenced artifact by hash. The header row
//! lives in the `forensic_snapshots` hot-plane table; the manifest lives in
//! the artifact store at `manifestUri`/`manifestHash`.

// Consumed from C9a onward (snapshot minting, verification, restore); the
// header/profile/manifest types are declared here so C9a–C9d compile against
// one stable §30 shape. Remove this allow when C9a wires construction.
#![allow(dead_code)]

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Spec §30.3 `ForensicSnapshot`. The queryable header of a content-addressed
/// snapshot: audits locate a snapshot by `id`, `snapshotType`, `createdAt`,
/// and (for lifecycle snapshots) its subject source/parse, then fetch and
/// verify the archived manifest at `manifestUri`/`manifestHash`. The heavy
/// artifact set never lives here — only the hash reference to the manifest
/// that enumerates it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ForensicSnapshot {
    pub(crate) id: String,

    pub(crate) snapshot_type: SnapshotType,

    pub(crate) created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) created_by: Option<String>,

    /// Source objects in scope for this snapshot. Serialized as a JSON string
    /// array both on the wire and in the `source_object_ids_json` hot column.
    pub(crate) source_object_ids: Vec<String>,
    /// Active parse ids in scope for this snapshot. Serialized as a JSON string
    /// array both on the wire and in the `active_parse_ids_json` hot column.
    pub(crate) active_parse_ids: Vec<String>,

    pub(crate) manifest_uri: String,
    pub(crate) manifest_hash: String,

    pub(crate) system_version: String,
    pub(crate) spec_version: String,

    pub(crate) replay_profile: ReplayProfile,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) notes: Option<String>,
}

/// Spec §30.3 `snapshotType`. The lifecycle moment a snapshot was taken at.
/// The full closed set is exhaustive here so every trigger and audit path
/// matches it without a catch-all; `Scheduled` and `PreDeployment` are inert
/// at MVP (no trigger constructs them) but MUST exist as variants because the
/// spec enum is closed and C9b/audits match against it. There is no
/// pre-superseded-deletion variant (user ruling 2026-07-16): the §31.2
/// deletion gate verifies over the immediately-preceding lifecycle snapshot
/// (`PostActivation` in the activation flow, `PreDeactivation` in the
/// deactivation flow), never a snapshot re-taken for deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SnapshotType {
    Scheduled,
    PreActivation,
    PostActivation,
    PreDeactivation,
    PreDeployment,
    Manual,
    Incident,
}

/// Spec §30.3 `ReplayProfile`. The declared replay capability of a snapshot,
/// per evidence/retrieval/generation dimension. The enums are spec-complete;
/// which values a real snapshot may carry is a construction-time constraint
/// C9a enforces (at MVP: `evidenceReplayMode = bit_exact`, retrieval and
/// generation = `not_supported`, `record_replay` never emitted), NOT an enum
/// narrowing — the closed sets stay complete so future tiers need no schema
/// change. `channelReplayModes`/`declaredTolerances` are spec-optional; per
/// §30.3 `declaredTolerances` is present only when a `rank_stable` claim is
/// made (§29.4), and the aggregate `retrievalReplayMode` is the weakest
/// channel's mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ReplayProfile {
    pub(crate) evidence_replay_mode: EvidenceReplayMode,
    pub(crate) retrieval_replay_mode: RetrievalReplayMode,
    pub(crate) generation_replay_mode: GenerationReplayMode,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) channel_replay_modes: Option<BTreeMap<String, ChannelReplayMode>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) declared_tolerances: Option<BTreeMap<String, f64>>,
}

/// Spec §30.3 `evidenceReplayMode`. Whether recorded evidence replays
/// bit-for-bit. At MVP only `BitExact` is emitted; `NotSupported` completes
/// the closed set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EvidenceReplayMode {
    BitExact,
    NotSupported,
}

/// Spec §30.3 `retrievalReplayMode`. Strength of retrieval reproducibility;
/// the aggregate is the weakest channel's mode. `RecordReplay` is
/// undemonstrable until the QER tier lands and is never emitted at MVP, but
/// stays in the closed set so the enum needs no later widening.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RetrievalReplayMode {
    BitExact,
    RankStable,
    RecordReplay,
    NotSupported,
}

/// Spec §30.3 `generationReplayMode`. Strength of generation reproducibility.
/// `RecordReplay` is never emitted at MVP but stays in the closed set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GenerationReplayMode {
    Deterministic,
    RecordReplay,
    NotSupported,
}

/// Spec §30.3 per-channel replay mode (`channelReplayModes` values): the same
/// closed set the retrieval aggregate is drawn from, keyed by channel name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChannelReplayMode {
    BitExact,
    RankStable,
    RecordReplay,
    NotSupported,
}

/// Manifest layout written by every new snapshot; see
/// `ForensicSnapshotManifest::format_version` for what each version means.
pub(crate) const MANIFEST_FORMAT_VERSION: u32 = 2;

/// Spec §30.4 `ForensicSnapshotManifest`. The archived object a
/// `ForensicSnapshot` references by hash: one list of `SnapshotArtifactRef`
/// per artifact category the snapshot immutably captures (§30.2). Optional
/// list fields (`parserOutputBundles`, `deletionRecords`, `modelArtifacts`)
/// are absent when the interval produced none. `manifestHash` is the
/// manifest's own self-hash: computed over every field except itself via
/// `canonical::canonical_sha256_hex_without_field` (C9a), so tampering with
/// any referenced artifact set is detectable at verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ForensicSnapshotManifest {
    pub(crate) snapshot_id: String,
    pub(crate) created_at: String,
    /// Layout of the archived binary planes. Absent on manifests minted before
    /// the field existed: those archive one blob per vector row, addressed by a
    /// `<blob_column>Hash` key on each metadata record. `MANIFEST_FORMAT_VERSION`
    /// archives one blob per plane, addressed by `<blob_column>Offset` and
    /// `<blob_column>Length` keys. Readers dispatch on this field so every
    /// existing snapshot stays verifiable and restorable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) format_version: Option<u32>,

    pub(crate) source_objects: Vec<SnapshotArtifactRef>,
    pub(crate) acquisition_records: Vec<SnapshotArtifactRef>,
    pub(crate) parse_runs: Vec<SnapshotArtifactRef>,
    pub(crate) canonical_parse_bundles: Vec<SnapshotArtifactRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) parser_output_bundles: Option<Vec<SnapshotArtifactRef>>,
    pub(crate) content_units: Vec<SnapshotArtifactRef>,
    pub(crate) unit_relationships: Vec<SnapshotArtifactRef>,
    pub(crate) semantic_annotations: Vec<SnapshotArtifactRef>,
    pub(crate) retrieval_projections: Vec<SnapshotArtifactRef>,
    pub(crate) retrieval_indexes: Vec<SnapshotArtifactRef>,
    pub(crate) assembly_policies: Vec<SnapshotArtifactRef>,
    pub(crate) retrieval_profiles: Vec<SnapshotArtifactRef>,
    pub(crate) capability_profiles: Vec<SnapshotArtifactRef>,
    pub(crate) query_execution_records: Vec<SnapshotArtifactRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) deletion_records: Option<Vec<SnapshotArtifactRef>>,
    pub(crate) runtime_artifacts: Vec<SnapshotArtifactRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) model_artifacts: Option<Vec<SnapshotArtifactRef>>,

    pub(crate) manifest_hash: String,
}

/// Spec §30.4 `SnapshotArtifactRef`. One typed reference from a manifest to a
/// content-addressed artifact: `artifactType` names the category, `uri`/`hash`
/// locate and pin the bytes. This is distinct from `artifact_store::ArtifactRef`
/// (which carries `size_bytes` but no type/format/metadata): the manifest ref
/// is the §30.4 wire shape, not the store's internal blob handle, so the two
/// are deliberately not unified.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SnapshotArtifactRef {
    pub(crate) artifact_type: String,
    pub(crate) uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) format: Option<String>,
    pub(crate) hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metadata: Option<BTreeMap<String, serde_json::Value>>,
}
