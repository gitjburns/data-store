//! Typed fabric data model (spec §7–§20, §33): sources, acquisition,
//! parses, content units and bodies, locators, relationships, provenance,
//! conformance, deletion evidence, and system events.
//!
//! Every type mirrors its spec schema field-for-field. Shared wire
//! conventions across the module:
//!
//! - `#[serde(rename_all = "camelCase")]` so serialized names match the spec
//!   schemas exactly.
//! - Spec-optional (`?`) fields are `Option` with
//!   `skip_serializing_if = "Option::is_none"` so absent fields stay absent
//!   (§16.2: omission is distinct from explicit null).
//! - `#[serde(deny_unknown_fields)]` on every struct so import validation
//!   (§13.1 hard gate, consumed by C4b) rejects shapes the spec does not
//!   define.
//! - Closed string enums are Rust enums matched exhaustively at every
//!   contract point, so adding a variant is a compile error there.
//! - Spec `number` fields are `u64` where they denote counts, indexes,
//!   offsets, sizes, or line/page numbers, and `f64` where they denote
//!   measurements, rates, or scores.

pub(crate) mod acquisition;
pub(crate) mod annotation;
pub(crate) mod body;
pub(crate) mod event;
pub(crate) mod locator;
pub(crate) mod operation;
pub(crate) mod parse;
pub(crate) mod provenance;
pub(crate) mod relationship;
pub(crate) mod snapshot;
pub(crate) mod source;
pub(crate) mod sync;
pub(crate) mod unit;

// Flat re-exports so consumers write `crate::model::SourceObject` (same
// pattern as `crate::primitives`), split into two `use` items:
//
// Wired re-exports, consumed by the acquisition (C3), parsing (C4),
// activation (C5), and annotation (CA) clusters today — no lint allow, so a
// name falling out of use is a visible warning.
pub(crate) use {
    acquisition::{
        AcquisitionFailureClass, AcquisitionOutcome, AcquisitionRecord, ConnectorCapabilityProfile,
        DetectionMode,
    },
    annotation::{AnnotationFreshnessStatus, SemanticAnnotation, SemanticAnnotationType},
    body::{
        CaptionBody, CodeBlockBody, TableCellBody, TextBlockBody, TextBlockRole,
        content_type_body_matches,
    },
    event::{SystemEvent, SystemEventType},
    locator::{CharRangeLocator, Locator},
    parse::{
        ConformanceReport, ParseHeldReason, ParseMetrics, ParseRun, ParseRunStatus, ParseWarning,
        ParseWarningSeverity, ParserCapabilityProfile,
    },
    provenance::{ProducerType, Provenance, ProvenanceInputRef, ProvenanceObjectType},
    relationship::{UnitRelationship, UnitRelationshipType},
    // §30 forensic snapshot shapes wired by the C9 lifecycle-forensics cluster
    // (minting, verification, triggering, restore) — no lint allow, so a name
    // falling out of use is a visible warning.
    snapshot::{
        EvidenceReplayMode, ForensicSnapshot, ForensicSnapshotManifest, GenerationReplayMode,
        ReplayProfile, RetrievalReplayMode, SnapshotArtifactRef, SnapshotType,
    },
    source::{DeletionEvidence, DeletionSignal},
    sync::{SyncQueueEntry, SyncQueueState},
    unit::{ContentType, ContentUnit},
};
// §34.6 Operation shapes, wired by the C10a admin HTTP surface: the operations
// store (src/operations.rs) reads/writes them and GET /operations/{operationId}
// serves them. No lint allow, so a name falling out of use is a visible warning.
pub(crate) use operation::{Operation, OperationStatus, OperationType};
// Typed source rows, wired by the C10a inspection surface: GET /sources/{id}
// reads them back into these shapes (SourceObject carrying its SourceLocation
// list and the DeletionEvidence/DeletionSignal already above). No lint allow.
pub(crate) use source::{SourceLocation, SourceLocationStatus, SourceObject};
// Not-yet-wired re-exports: the SPEC-epub §2 body types, closed-set enums,
// locator, and relationship-role set that only the EPUB worker (Phase 4) and
// its importer/conformance consumers emit or inspect. Remove each name from
// this allow block when its consumer wires it.
#[allow(unused_imports)]
pub(crate) use {
    body::{
        AsideBody, AsideKind, DocumentBody, FigureBody, ListBody, ListItemBody, ListKind, PageBody,
        SectionKind, TableBody, TableHeader, TableRowBody, TableRowRole, TextSectionBody,
    },
    locator::DomPathLocator,
    relationship::RELATIONSHIP_ROLES_REFERENCES,
    // `ChannelReplayMode` is the `ForensicSnapshotManifest.channel_replay_modes`
    // field type; at MVP no per-channel replay claim is emitted, so it has no
    // external consumer. Its consumer is the post-MVP verified-recompute tier
    // (§29.4 rank_stable claims). Remove it from this allow block when that tier
    // wires it.
    snapshot::ChannelReplayMode,
};
