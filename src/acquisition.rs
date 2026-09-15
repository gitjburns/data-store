//! Acquisition importer (spec §9.2, §10, §11.1): validates staged connector
//! bundles, computes source hashes, and owns every canonical acquisition
//! write (source objects, locations, acquisition records, deletion
//! evidence, events). Implemented by work package C3b.
//!
//! Trust boundary: connectors are untrusted producers (spec §9.1). Every
//! manifest field is a claim; content identity is only ever the SHA-256 this
//! module recomputes over the staged bytes. A malformed bundle is a recorded
//! acquisition outcome (failed AcquisitionRecord + event), never an importer
//! `Err` — importer errors are reserved for faults of the canonical side
//! (SQL, artifact store, staging filesystem).
//!
//! Write policy (D1): each operation opens a fresh hot-plane connection via
//! `hot_plane::open_write`/`open_read` and performs all canonical row writes
//! plus their audit events inside one transaction so state changes and their
//! events commit or roll back together (the `crate::events::append_event`
//! invariant).

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    time::Instant,
};

use crate::sqlite::{Connection, Transaction};
use rusqlite::{OptionalExtension, params};
use serde::Serialize;
use serde_json::{Map, Value};
use tracing::{debug, info, warn};

use crate::artifact_store::{ArtifactRef, ArtifactStore};
use crate::connectors::{
    AcquisitionBundleManifest, BUNDLE_MANIFEST_FILE_NAME, BUNDLE_SOURCE_FILE_NAME,
    KnownLocationState,
};
use crate::error::ApiError;
use crate::events::{append_event, entry, new_system_event};
use crate::hot_plane;
use crate::ids::{new_source_location_id, new_source_object_id};
use crate::model::{
    AcquisitionFailureClass, AcquisitionOutcome, AcquisitionRecord, DeletionEvidence,
    DeletionSignal, SystemEventType,
};
use crate::primitives::{parse_utc_timestamp_ms, utc_now};
use crate::util::truncate_persisted_detail;

/// Looks up the content identity for dedup (spec §10 rule 2): one
/// SourceObject exists per source_hash.
const SELECT_SOURCE_OBJECT_BY_HASH_SQL: &str = "
SELECT id FROM source_objects WHERE source_hash = ?1";

/// Inserts a new SourceObject row; active_parse_id, event_time, and
/// deactivated_at start NULL (no parse yet, no source event time claimed,
/// not deactivated).
const INSERT_SOURCE_OBJECT_SQL: &str = "
INSERT INTO source_objects (
  id, source_hash, mime_type, size_bytes, storage_uri, ingest_time, created_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

/// Looks up the location row owning a (source_system, native_uri) pair —
/// the UNIQUE presence key of spec §10.
const SELECT_LOCATION_BY_SYSTEM_AND_URI_SQL: &str = "
SELECT id, source_id, status FROM source_locations
WHERE source_system = ?1 AND native_uri = ?2";

/// Inserts a new current SourceLocation; deletion_evidence_json and
/// metadata_json start NULL.
const INSERT_SOURCE_LOCATION_SQL: &str = "
INSERT INTO source_locations (
  id, source_id, source_system, native_uri, native_id, governance_domain,
  first_seen_at, last_seen_at, status
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'current')";

/// Refreshes an existing location that still points at the same content:
/// presence re-confirmed, so the location is current and any stale deletion
/// evidence on the row is cleared (the durable evidence survives in the
/// system_events audit trail; the row reflects current state only).
const REFRESH_SOURCE_LOCATION_SQL: &str = "
UPDATE source_locations
SET last_seen_at = ?2, status = 'current', deletion_evidence_json = NULL
WHERE id = ?1";

/// Repoints an existing location at new content (spec §10 rule 7: content
/// changed at the location, so a new SourceObject supersedes the old one at
/// this presence key). first_seen_at is reset to the rebind time: a content
/// change is one location ending and another beginning (spec §10 rules 3/7),
/// so the reused row models the NEW binding, whose observation history
/// starts now — the old binding's history survives in acquisition_records
/// and system_events, not in this row.
const REBIND_SOURCE_LOCATION_SQL: &str = "
UPDATE source_locations
SET source_id = ?2, first_seen_at = ?3, last_seen_at = ?3, status = 'current',
    deletion_evidence_json = NULL
WHERE id = ?1";

/// Marks one location deleted with its qualifying evidence (spec §11.1);
/// deletion_evidence_json is a canonical model::DeletionEvidence.
const MARK_LOCATION_DELETED_SQL: &str = "
UPDATE source_locations
SET status = 'deleted', deletion_evidence_json = ?2
WHERE id = ?1";

/// Lists every current location of one source system; ordered so
/// enumeration-deletion decisions are deterministic and auditable.
/// Deliberately uncapped (a documented exemption from the row-cap policy):
/// §11.1 deletion inference requires the COMPLETE current-location set — a
/// capped read would treat rows beyond the cap as absent and fabricate
/// deletion evidence. The result is bounded by corpus size.
const SELECT_CURRENT_LOCATIONS_FOR_SYSTEM_SQL: &str = "
SELECT id, native_uri, source_id FROM source_locations
WHERE source_system = ?1 AND status = 'current'
ORDER BY native_uri";

/// Inserts one AcquisitionRecord row (spec §9.2), success and failure paths
/// alike; the source_* linkage parameters are NULL on the failure path.
const INSERT_ACQUISITION_RECORD_SQL: &str = "
INSERT INTO acquisition_records (
  id, connector_name, connector_version, connector_config_hash,
  source_system, native_uri, native_id, native_version, native_modified_at,
  governance_domain, outcome, failure_class, failure_detail,
  source_hash, source_object_id, source_location_id, acquired_at, elapsed_ms
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)";

/// Per-current-location prescreen facts for one source system: the byte size
/// of the location's content and the native modified time claimed by the
/// latest succeeded acquisition of that native_uri. Enumeration records
/// (scope-level, no per-item claims) never carry native_modified_at, so the
/// IS NOT NULL filter keeps them out of the subquery naturally.
/// Deliberately uncapped (a documented exemption from the row-cap policy):
/// the prescreen map must be complete — a truncated map would silently
/// re-stage or skip the locations it dropped, with no truncation signal to
/// the consumer. The result is bounded by corpus size.
const SELECT_KNOWN_LOCATION_STATE_SQL: &str = "
SELECT
  locations.native_uri,
  objects.size_bytes,
  (SELECT records.native_modified_at
     FROM acquisition_records AS records
    WHERE records.source_system = locations.source_system
      AND records.native_uri = locations.native_uri
      AND records.outcome = 'succeeded'
      AND records.native_modified_at IS NOT NULL
    ORDER BY records.acquired_at DESC, records.id DESC
    LIMIT 1) AS native_modified_at
FROM source_locations AS locations
JOIN source_objects AS objects ON objects.id = locations.source_id
WHERE locations.source_system = ?1 AND locations.status = 'current'";

/// SystemEvent object_type for acquisition_records rows.
const OBJECT_TYPE_ACQUISITION_RECORD: &str = "acquisition_record";

/// SystemEvent object_type for source_objects rows.
const OBJECT_TYPE_SOURCE_OBJECT: &str = "source_object";

/// SystemEvent object_type for source_locations rows.
const OBJECT_TYPE_SOURCE_LOCATION: &str = "source_location";

/// Placeholder recorded for connector-identity claims when the staged
/// manifest itself is missing or unparseable: the attempt is still durable
/// operational state (spec §9.2), but no identity claims exist to copy. The
/// literal is deliberately visible in records rather than a silent guess.
const UNKNOWN_CLAIM: &str = "unknown";

/// Log-event namespace this module passes to the shared hot-plane
/// transaction helpers, so boundary logs stay attributable to acquisition.
const TX_LOG_NAMESPACE: &str = "acquisition";

/// Connector identity the scheduler passes when there is no readable bundle
/// to carry the claims: failed acquisition attempts (`record_failed_acquisition`)
/// and enumeration evidence (`record_enumeration`). Field meanings match the
/// same-named AcquisitionRecord claims (spec §9.2).
#[derive(Debug, Clone)]
pub(crate) struct AcquisitionContext {
    pub(crate) connector_name: String,
    pub(crate) connector_version: String,
    pub(crate) connector_config_hash: String,
    pub(crate) source_system: String,
    pub(crate) governance_domain: String,
}

/// Result of importing one staged bundle. Both arms are recorded outcomes:
/// a rejected bundle produced a durable failed AcquisitionRecord exactly
/// like a valid one produced a succeeded record.
#[derive(Debug, Clone)]
pub(crate) struct ImportOutcome {
    /// True when the bundle validated and canonical state committed; false
    /// when the bundle was rejected as malformed (recorded, bundle kept for
    /// diagnostics, nothing canonical mutated beyond the failure record).
    pub(crate) imported: bool,
    /// The durable AcquisitionRecord written for this attempt (succeeded or
    /// failed).
    pub(crate) acquisition_record_id: String,
    /// Content identity the bundle resolved to; None on rejection.
    pub(crate) source_object_id: Option<String>,
    /// Recomputed SHA-256 of the staged bytes; None on rejection (claims
    /// stay unverified).
    pub(crate) source_hash: Option<String>,
    /// Why the bundle was rejected; None when imported.
    pub(crate) rejection_detail: Option<String>,
}

/// A bundle that passed validation: manifest claims, the staged bytes, and
/// the recomputed (trusted) content hash.
struct ValidatedBundle {
    manifest: AcquisitionBundleManifest,
    source_bytes: Vec<u8>,
    source_hash: String,
}

/// Why a staged bundle was rejected. `claims` carries the manifest when it
/// was at least readable, so the failure record preserves the connector's
/// claims; None means even the manifest could not be read. The manifest is
/// boxed only to keep this Err variant small (clippy::result_large_err).
struct BundleRejection {
    claims: Option<Box<AcquisitionBundleManifest>>,
    detail: String,
}

/// Row linkage produced by the import transaction body, echoed into the
/// ImportOutcome after commit.
struct ImportLinkage {
    acquisition_record_id: String,
    source_object_id: String,
    source_location_id: String,
    new_source_object: bool,
    new_source_location: bool,
}

/// Validate and import one staged acquisition bundle (spec §9.1 rule 3):
/// recompute the content hash, write the raw bytes to the artifact store,
/// dedup by source_hash, maintain the (source_system, native_uri) location,
/// and record the attempt plus its events in one transaction. A malformed
/// bundle is a recorded failed outcome (bundle kept for diagnostics), not an
/// `Err`; `Err` means the canonical side itself failed.
pub(crate) fn import_staged_bundle(
    index_root: &crate::runtime::StorageContext,
    bundle_dir: &Path,
) -> Result<ImportOutcome, ApiError> {
    let started = Instant::now();
    // Loading/hashing a staged source can be lengthy before canonical identity
    // exists. Keep that work visible and attach validated identity only when known.
    let monitoring = index_root.monitoring();
    let work = monitoring.work(
        crate::monitoring_types::WorkIdentity::new(
            "ingestion",
            &format!("staged bundle {}", bundle_dir.display()),
            None,
            None,
        ),
        "validating staged acquisition",
        None,
        "bytes",
    );
    let monitor = work.handle();
    info!(
        event = "acquisition.import_started",
        bundle_dir = %bundle_dir.display(),
        "staged bundle import starting"
    );
    // Issue keys retain the actual input identity; display sanitization must
    // never prevent retirement when a later complete scan removes that source.
    let bundle_issue_key = format!("acquisition-bundle:{}", bundle_dir.display());
    let mut issue_key = bundle_issue_key.clone();
    let outcome = match load_staged_bundle(bundle_dir) {
        Ok(bundle) => {
            issue_key = format!("acquisition:{}", bundle.manifest.native_uri);
            monitor.document(&bundle.manifest.native_uri);
            monitor.stage("archiving and committing acquisition", None, "acquisitions");
            import_validated_bundle(index_root, bundle_dir, bundle, started)
        }
        Err(rejection) => {
            if let Some(claims) = &rejection.claims {
                issue_key = format!("acquisition:{}", claims.native_uri);
                monitor.document(&claims.native_uri);
            }
            reject_staged_bundle(index_root, bundle_dir, rejection, started)
        }
    };
    let (state, message) = match &outcome {
        Ok(outcome) if outcome.imported => {
            monitor.identify(outcome.source_object_id.as_deref(), None);
            monitoring.record_completed(
                monitor.identity(),
                "acquisition",
                "Acquisition committed",
                "acquisitions committed",
                1,
            );
            (
                crate::monitoring_types::MonitorState::Complete,
                "Acquisition committed".to_string(),
            )
        }
        Ok(outcome) => (
            crate::monitoring_types::MonitorState::Failed,
            outcome
                .rejection_detail
                .clone()
                .unwrap_or_else(|| "Acquisition rejected without a detail".to_string()),
        ),
        Err(source) => (
            crate::monitoring_types::MonitorState::Failed,
            source.to_string(),
        ),
    };
    if state == crate::monitoring_types::MonitorState::Failed {
        monitoring.set_issue(
            issue_key,
            crate::monitoring_types::MonitorIssue {
                identity: monitor.identity(),
                stage: "acquisition import".to_string(),
                state,
                message: message.clone(),
                affected: 1,
                retry_in_ms: None,
                attempt: None,
                retry_limit: None,
                observed_since: None,
                elapsed_ms: 0,
            },
        );
    } else {
        monitoring.clear_issue(&issue_key);
        monitoring.clear_issue(&bundle_issue_key);
    }
    work.finish(state, &message);
    outcome
}

/// Read and validate one staged bundle: manifest present and well-formed,
/// source bytes present, recomputed hash equal to the claimed hash, claimed
/// size equal to the actual byte count. Any violation is a rejection carrying
/// whatever claims were readable — never an importer error, because staged
/// input is untrusted connector output (spec §9.1 rule 4).
fn load_staged_bundle(bundle_dir: &Path) -> Result<ValidatedBundle, BundleRejection> {
    let manifest_path = bundle_dir.join(BUNDLE_MANIFEST_FILE_NAME);
    let manifest_bytes = fs::read(&manifest_path).map_err(|source| BundleRejection {
        claims: None,
        detail: format!(
            "unreadable {BUNDLE_MANIFEST_FILE_NAME} at {}: {source}",
            manifest_path.display()
        ),
    })?;
    let manifest: AcquisitionBundleManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|source| BundleRejection {
            claims: None,
            detail: format!("invalid {BUNDLE_MANIFEST_FILE_NAME}: {source}"),
        })?;

    let source_path = bundle_dir.join(BUNDLE_SOURCE_FILE_NAME);
    let source_bytes = fs::read(&source_path).map_err(|source| BundleRejection {
        claims: Some(Box::new(manifest.clone())),
        detail: format!(
            "unreadable {BUNDLE_SOURCE_FILE_NAME} at {}: {source}",
            source_path.display()
        ),
    })?;

    // Trust boundary: the recomputed hash is the only content identity ever
    // persisted; the manifest hash is a claim checked against it.
    let source_hash = crate::canonical::sha256_hex_bytes(&source_bytes);
    if source_hash != manifest.claimed_source_hash {
        return Err(BundleRejection {
            detail: format!(
                "source hash mismatch: manifest claims {}, staged bytes hash to {source_hash}",
                manifest.claimed_source_hash
            ),
            claims: Some(Box::new(manifest)),
        });
    }
    if source_bytes.len() as u64 != manifest.size_bytes {
        return Err(BundleRejection {
            detail: format!(
                "size mismatch: manifest claims {} bytes, {BUNDLE_SOURCE_FILE_NAME} is {} bytes",
                manifest.size_bytes,
                source_bytes.len()
            ),
            claims: Some(Box::new(manifest)),
        });
    }
    // elapsed_ms is an untrusted connector claim persisted into an SQLite
    // INTEGER column; a value beyond i64 range is unrepresentable and
    // rejected here at the trust boundary rather than failing the insert.
    if i64::try_from(manifest.elapsed_ms).is_err() {
        return Err(BundleRejection {
            detail: format!(
                "elapsed_ms claim {} exceeds the SQLite integer range",
                manifest.elapsed_ms
            ),
            claims: Some(Box::new(manifest)),
        });
    }

    Ok(ValidatedBundle {
        manifest,
        source_bytes,
        source_hash,
    })
}

/// Record one rejected bundle as a durable failed acquisition (failed
/// AcquisitionRecord + acquisition.failed event in one transaction) and
/// return the rejected ImportOutcome. The staged bundle directory is
/// deliberately KEPT so the operator can inspect the malformed input.
fn reject_staged_bundle(
    index_root: &crate::runtime::StorageContext,
    bundle_dir: &Path,
    rejection: BundleRejection,
    started: Instant,
) -> Result<ImportOutcome, ApiError> {
    let detail = truncate_persisted_detail(&rejection.detail, &index_root.limits().diagnostics);
    let record = failed_record_from_rejection(bundle_dir, rejection.claims, &detail)?;

    let mut connection = open_bounded_write(index_root)?;
    let tx = hot_plane::begin_write_transaction(
        &mut connection,
        TX_LOG_NAMESPACE,
        "reject_staged_bundle",
    )?;
    let body = (|| -> Result<(), ApiError> {
        insert_acquisition_record(&tx, &record)?;
        append_acquisition_failed_event(&tx, &record)?;
        Ok(())
    })();
    if let Err(source) = body {
        return Err(hot_plane::abort_transaction(
            tx,
            TX_LOG_NAMESPACE,
            "reject_staged_bundle",
            source,
        ));
    }
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "reject_staged_bundle")?;

    warn!(
        event = "acquisition.import_rejected",
        bundle_dir = %bundle_dir.display(),
        acquisition_record_id = record.id,
        source_system = record.source_system,
        native_uri = record.native_uri,
        detail,
        bundle_kept = true,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "staged bundle rejected as malformed; failure recorded, bundle kept for diagnostics"
    );
    Ok(ImportOutcome {
        imported: false,
        acquisition_record_id: record.id,
        source_object_id: None,
        source_hash: None,
        rejection_detail: Some(detail),
    })
}

/// Build the failed AcquisitionRecord for a rejected bundle. When the
/// manifest was readable its claims are preserved verbatim (including its
/// acquired_at/elapsed_ms); when it was not, identity claims are the
/// explicit UNKNOWN_CLAIM literal and native_uri is the bundle directory
/// path — the only real local fact — so the record never fabricates claims.
fn failed_record_from_rejection(
    bundle_dir: &Path,
    claims: Option<Box<AcquisitionBundleManifest>>,
    detail: &str,
) -> Result<AcquisitionRecord, ApiError> {
    let id = crate::ids::new_acquisition_record_id()?;
    let record = match claims.map(|manifest| *manifest) {
        Some(manifest) => AcquisitionRecord {
            id,
            connector_name: manifest.connector_name,
            connector_version: manifest.connector_version,
            connector_config_hash: manifest.connector_config_hash,
            source_system: manifest.source_system,
            native_uri: manifest.native_uri,
            native_id: manifest.native_id,
            native_version: None,
            native_modified_at: manifest.native_modified_at,
            governance_domain: manifest.governance_domain,
            outcome: AcquisitionOutcome::Failed,
            failure_class: Some(AcquisitionFailureClass::Malformed),
            failure_detail: Some(detail.to_owned()),
            source_hash: None,
            source_object_id: None,
            source_location_id: None,
            acquired_at: manifest.acquired_at,
            // The elapsed_ms claim is preserved only when it is
            // i64-representable (the SQLite INTEGER limit); an
            // unrepresentable claim is dropped from the failure record —
            // the rejection detail preserves the value when the claim
            // itself was the rejection cause.
            elapsed_ms: i64::try_from(manifest.elapsed_ms)
                .is_ok()
                .then_some(manifest.elapsed_ms),
        },
        None => AcquisitionRecord {
            id,
            connector_name: UNKNOWN_CLAIM.to_owned(),
            connector_version: UNKNOWN_CLAIM.to_owned(),
            connector_config_hash: UNKNOWN_CLAIM.to_owned(),
            source_system: UNKNOWN_CLAIM.to_owned(),
            native_uri: bundle_dir.display().to_string(),
            native_id: None,
            native_version: None,
            native_modified_at: None,
            governance_domain: UNKNOWN_CLAIM.to_owned(),
            outcome: AcquisitionOutcome::Failed,
            failure_class: Some(AcquisitionFailureClass::Malformed),
            failure_detail: Some(detail.to_owned()),
            source_hash: None,
            source_object_id: None,
            source_location_id: None,
            // No readable claim exists, so the import time is the only
            // honest acquired_at.
            acquired_at: utc_now()?,
            elapsed_ms: None,
        },
    };
    Ok(record)
}

/// Import one validated bundle: artifact-store write first, then one write
/// transaction covering dedup, location maintenance, the succeeded record,
/// and all events.
///
/// The artifact-store put deliberately happens BEFORE (outside) the SQL
/// transaction: the store is content-addressed and write-once idempotent, so
/// a crash between the blob write and the commit leaves only an orphan blob
/// a later import of the same content reuses — never a committed row whose
/// storage_uri dangles.
///
/// The consumed bundle directory is NOT removed here: the scheduler owns
/// that cleanup and performs it only after the entry's whole unit of work
/// (import → parse → gate → activate → queue completion) has finished, so a
/// crash mid-chain replays against the still-present bundle — this import is
/// idempotent for identical content (dedup by source_hash refreshes rather
/// than duplicates).
fn import_validated_bundle(
    index_root: &crate::runtime::StorageContext,
    bundle_dir: &Path,
    bundle: ValidatedBundle,
    started: Instant,
) -> Result<ImportOutcome, ApiError> {
    let store = ArtifactStore::open(index_root)?;
    let artifact = store.put_bytes(&bundle.source_bytes)?;

    let mut connection = open_bounded_write(index_root)?;
    let tx = hot_plane::begin_write_transaction(
        &mut connection,
        TX_LOG_NAMESPACE,
        "import_staged_bundle",
    )?;
    let body = import_transaction_body(&tx, &bundle, &artifact);
    let linkage = match body {
        Ok(linkage) => linkage,
        Err(source) => {
            return Err(hot_plane::abort_transaction(
                tx,
                TX_LOG_NAMESPACE,
                "import_staged_bundle",
                source,
            ));
        }
    };
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "import_staged_bundle")?;

    info!(
        event = "acquisition.import_succeeded",
        bundle_dir = %bundle_dir.display(),
        acquisition_record_id = linkage.acquisition_record_id,
        source_object_id = linkage.source_object_id,
        source_location_id = linkage.source_location_id,
        source_hash = bundle.source_hash,
        new_source_object = linkage.new_source_object,
        new_source_location = linkage.new_source_location,
        size_bytes = artifact.size_bytes,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "staged bundle imported"
    );
    Ok(ImportOutcome {
        imported: true,
        acquisition_record_id: linkage.acquisition_record_id,
        source_object_id: Some(linkage.source_object_id),
        source_hash: Some(bundle.source_hash),
        rejection_detail: None,
    })
}

/// The canonical writes of one successful import, all on the caller's
/// transaction: content dedup (spec §10 rule 2), location maintenance on the
/// UNIQUE (source_system, native_uri) key, the succeeded AcquisitionRecord,
/// and the events that record exactly what changed.
fn import_transaction_body(
    tx: &Transaction<'_>,
    bundle: &ValidatedBundle,
    artifact: &ArtifactRef,
) -> Result<ImportLinkage, ApiError> {
    let manifest = &bundle.manifest;
    let now = utc_now()?;

    let (source_object_id, new_source_object) = ensure_source_object(tx, bundle, artifact, &now)?;
    let (source_location_id, new_source_location) =
        maintain_source_location(tx, manifest, &source_object_id, &now)?;

    let record = AcquisitionRecord {
        id: crate::ids::new_acquisition_record_id()?,
        connector_name: manifest.connector_name.clone(),
        connector_version: manifest.connector_version.clone(),
        connector_config_hash: manifest.connector_config_hash.clone(),
        source_system: manifest.source_system.clone(),
        native_uri: manifest.native_uri.clone(),
        native_id: manifest.native_id.clone(),
        native_version: None,
        native_modified_at: manifest.native_modified_at.clone(),
        governance_domain: manifest.governance_domain.clone(),
        outcome: AcquisitionOutcome::Succeeded,
        failure_class: None,
        failure_detail: None,
        source_hash: Some(bundle.source_hash.clone()),
        source_object_id: Some(source_object_id.clone()),
        source_location_id: Some(source_location_id.clone()),
        acquired_at: manifest.acquired_at.clone(),
        elapsed_ms: Some(manifest.elapsed_ms),
    };
    insert_acquisition_record(tx, &record)?;

    // Events commit atomically with the rows they describe (append_event
    // uses this transaction's connection): acquisition.succeeded always,
    // the source.* events only when this import actually created the row
    // they announce.
    let succeeded_payload = Map::from_iter([
        entry("sourceSystem", &manifest.source_system),
        entry("nativeUri", &manifest.native_uri),
        entry("sourceObjectId", &source_object_id),
        entry("sourceLocationId", &source_location_id),
        entry("sourceHash", &bundle.source_hash),
    ]);
    let succeeded_event = new_system_event(
        SystemEventType::AcquisitionSucceeded,
        OBJECT_TYPE_ACQUISITION_RECORD,
        &record.id,
        Some(succeeded_payload),
    )?;
    append_event(tx, &succeeded_event)?;

    if new_source_object {
        let mut payload = Map::from_iter([
            entry("sourceHash", &bundle.source_hash),
            entry("mimeType", mime_type_for_native_uri(&manifest.native_uri)),
        ]);
        payload.insert(
            "sizeBytes".to_owned(),
            Value::Number(artifact.size_bytes.into()),
        );
        let event = new_system_event(
            SystemEventType::SourceIngested,
            OBJECT_TYPE_SOURCE_OBJECT,
            &source_object_id,
            Some(payload),
        )?;
        append_event(tx, &event)?;
    }
    if new_source_location {
        let payload = Map::from_iter([
            entry("sourceSystem", &manifest.source_system),
            entry("nativeUri", &manifest.native_uri),
            entry("sourceObjectId", &source_object_id),
        ]);
        let event = new_system_event(
            SystemEventType::SourceLocationAdded,
            OBJECT_TYPE_SOURCE_LOCATION,
            &source_location_id,
            Some(payload),
        )?;
        append_event(tx, &event)?;
    }

    Ok(ImportLinkage {
        acquisition_record_id: record.id,
        source_object_id,
        source_location_id,
        new_source_object,
        new_source_location,
    })
}

/// Content-based dedup (spec §10 rule 2): reuse the existing SourceObject
/// for this source_hash or insert a new one referencing the already-written
/// artifact blob. Returns the object id and whether it was newly created.
fn ensure_source_object(
    tx: &Transaction<'_>,
    bundle: &ValidatedBundle,
    artifact: &ArtifactRef,
    now: &str,
) -> Result<(String, bool), ApiError> {
    let existing: Option<String> = tx
        .query_row(
            SELECT_SOURCE_OBJECT_BY_HASH_SQL,
            params![bundle.source_hash],
            |row| row.get(0),
        )
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to look up source object for hash {}: {source}",
                bundle.source_hash
            ),
        })?;
    if let Some(id) = existing {
        debug!(
            event = "acquisition.source_object_deduplicated",
            source_object_id = id,
            source_hash = bundle.source_hash,
            "identical content already known; reusing source object"
        );
        return Ok((id, false));
    }

    let id = new_source_object_id()?;
    let size_bytes = sql_integer(artifact.size_bytes, "size_bytes")?;
    tx.execute(
        INSERT_SOURCE_OBJECT_SQL,
        params![
            id,
            bundle.source_hash,
            mime_type_for_native_uri(&bundle.manifest.native_uri),
            size_bytes,
            artifact.uri,
            now,
            now,
        ],
    )
    .map_err(|source| ApiError::StorageOperation {
        message: format!(
            "failed to insert source object {id} for hash {}: {source}",
            bundle.source_hash
        ),
    })?;
    info!(
        event = "acquisition.source_object_inserted",
        committed = false,
        source_object_id = id,
        source_hash = bundle.source_hash,
        size_bytes = artifact.size_bytes,
        "new source object staged in import transaction"
    );
    Ok((id, true))
}

/// Location maintenance on the UNIQUE (source_system, native_uri) presence
/// key (spec §10): insert a new current location, refresh a location that
/// still points at the same content (which restores an evidenced-deleted
/// location to current on reappearance, spec §11.4), or rebind a location
/// whose content changed to the new SourceObject (spec §10 rule 7). The old
/// object may thereby lose its last current location; deactivation is C9
/// deletion propagation (spec §11.3) and deliberately does NOT happen here.
/// Returns the location id and whether the row was newly inserted.
fn maintain_source_location(
    tx: &Transaction<'_>,
    manifest: &AcquisitionBundleManifest,
    source_object_id: &str,
    now: &str,
) -> Result<(String, bool), ApiError> {
    let existing: Option<(String, String, String)> = tx
        .query_row(
            SELECT_LOCATION_BY_SYSTEM_AND_URI_SQL,
            params![manifest.source_system, manifest.native_uri],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to look up source location for {} {}: {source}",
                manifest.source_system, manifest.native_uri
            ),
        })?;

    let Some((location_id, current_source_id, prior_status)) = existing else {
        let location_id = new_source_location_id()?;
        tx.execute(
            INSERT_SOURCE_LOCATION_SQL,
            params![
                location_id,
                source_object_id,
                manifest.source_system,
                manifest.native_uri,
                manifest.native_id,
                manifest.governance_domain,
                now,
                now,
            ],
        )
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to insert source location {location_id} for {} {}: {source}",
                manifest.source_system, manifest.native_uri
            ),
        })?;
        info!(
            event = "acquisition.source_location_inserted",
            committed = false,
            source_location_id = location_id,
            source_system = manifest.source_system,
            native_uri = manifest.native_uri,
            source_object_id,
            "new source location staged in import transaction"
        );
        return Ok((location_id, true));
    };

    if current_source_id == source_object_id {
        // Same content re-observed at the same place. If the location was
        // evidenced-deleted or access-lost, its reappearance restores it to
        // current (spec §11.4) — an operationally meaningful transition.
        if prior_status != "current" {
            info!(
                event = "acquisition.source_location_restored",
                committed = false,
                source_location_id = location_id,
                source_system = manifest.source_system,
                native_uri = manifest.native_uri,
                prior_status,
                "location selected for restoration by reappearance of its content"
            );
        }
        tx.execute(REFRESH_SOURCE_LOCATION_SQL, params![location_id, now])
            .map_err(|source| ApiError::StorageOperation {
                message: format!("failed to refresh source location {location_id}: {source}"),
            })?;
        // §11.2 recovery — a location coming back from `access_lost` completes the
        // audit pair: `deletion::mark_scope_access_lost` minted `source.access_lost`
        // (object_type "source_location") when observation was lost, so the return
        // to `current` mints its `source.access_restored` counterpart. On the SAME
        // transaction as the refresh above, so the status flip and its audit event
        // are atomic. Only `access_lost` recovery mints this: a `deleted`→`current`
        // reappearance is the §11.4 SourceReactivated path, evented elsewhere.
        if prior_status == "access_lost" {
            let payload = Map::from_iter([
                entry("sourceSystem", &manifest.source_system),
                entry("nativeUri", &manifest.native_uri),
                entry("sourceObjectId", source_object_id),
            ]);
            let event = new_system_event(
                SystemEventType::SourceAccessRestored,
                OBJECT_TYPE_SOURCE_LOCATION,
                &location_id,
                Some(payload),
            )?;
            append_event(tx, &event)?;
        }
        debug!(
            event = "acquisition.source_location_refreshed",
            committed = false,
            source_location_id = location_id,
            source_system = manifest.source_system,
            native_uri = manifest.native_uri,
            "source location refresh staged in import transaction"
        );
        return Ok((location_id, false));
    }

    // Content changed at the location: the new SourceObject supersedes the
    // old one at this presence key (spec §10 rule 7). The row is reused but
    // models the NEW binding — first_seen_at resets to now, because rule 3
    // treats a content change as one location ending and another beginning.
    // The superseded object is left untouched here even if this was its
    // last current location — deactivation is C9 propagation, not an
    // import concern.
    tx.execute(
        REBIND_SOURCE_LOCATION_SQL,
        params![location_id, source_object_id, now],
    )
    .map_err(|source| ApiError::StorageOperation {
        message: format!(
            "failed to rebind source location {location_id} to {source_object_id}: {source}"
        ),
    })?;
    info!(
        event = "acquisition.source_location_rebound",
        committed = false,
        source_location_id = location_id,
        source_system = manifest.source_system,
        native_uri = manifest.native_uri,
        previous_source_object_id = current_source_id,
        source_object_id,
        prior_status,
        "content changed at location; source rebinding staged in import transaction"
    );
    Ok((location_id, false))
}

/// Record one failed acquisition attempt (spec §9.2: a source that cannot be
/// read is operationally meaningful state): failed AcquisitionRecord plus
/// acquisition.failed event in one transaction. Returns the record id.
pub(crate) fn record_failed_acquisition(
    index_root: &crate::runtime::StorageContext,
    context: &AcquisitionContext,
    native_uri: &str,
    failure_class: AcquisitionFailureClass,
    detail: &str,
) -> Result<String, ApiError> {
    let started = Instant::now();
    let record = AcquisitionRecord {
        id: crate::ids::new_acquisition_record_id()?,
        connector_name: context.connector_name.clone(),
        connector_version: context.connector_version.clone(),
        connector_config_hash: context.connector_config_hash.clone(),
        source_system: context.source_system.clone(),
        native_uri: native_uri.to_owned(),
        native_id: None,
        native_version: None,
        native_modified_at: None,
        governance_domain: context.governance_domain.clone(),
        outcome: AcquisitionOutcome::Failed,
        failure_class: Some(failure_class),
        failure_detail: Some(truncate_persisted_detail(
            detail,
            &index_root.limits().diagnostics,
        )),
        source_hash: None,
        source_object_id: None,
        source_location_id: None,
        acquired_at: utc_now()?,
        elapsed_ms: None,
    };

    let mut connection = open_bounded_write(index_root)?;
    let tx = hot_plane::begin_write_transaction(
        &mut connection,
        TX_LOG_NAMESPACE,
        "record_failed_acquisition",
    )?;
    let body = (|| -> Result<(), ApiError> {
        insert_acquisition_record(&tx, &record)?;
        append_acquisition_failed_event(&tx, &record)?;
        Ok(())
    })();
    if let Err(source) = body {
        return Err(hot_plane::abort_transaction(
            tx,
            TX_LOG_NAMESPACE,
            "record_failed_acquisition",
            source,
        ));
    }
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "record_failed_acquisition")?;

    info!(
        event = "acquisition.failure_recorded",
        acquisition_record_id = record.id,
        source_system = record.source_system,
        native_uri = record.native_uri,
        failure_class = ?failure_class,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "failed acquisition attempt recorded"
    );
    Ok(record.id)
}

/// Record one completed full enumeration of a scope as a SUCCEEDED
/// AcquisitionRecord (native_uri = scope_uri, no source linkage). This
/// record is the acquisition attempt that absent_from_complete_enumeration
/// DeletionEvidence references (spec §11.1 requires each deletion signal to
/// name the attempt that observed it, and a complete enumeration is itself
/// an acquisition-layer observation); giving it a durable record id is what
/// makes that evidence traceable. No event is emitted: enumeration evidence
/// anchors deletions, and the deletions themselves carry the events.
pub(crate) fn record_enumeration(
    index_root: &crate::runtime::StorageContext,
    context: &AcquisitionContext,
    scope_uri: &str,
    elapsed_ms: u64,
) -> Result<String, ApiError> {
    let started = Instant::now();
    let record = AcquisitionRecord {
        id: crate::ids::new_acquisition_record_id()?,
        connector_name: context.connector_name.clone(),
        connector_version: context.connector_version.clone(),
        connector_config_hash: context.connector_config_hash.clone(),
        source_system: context.source_system.clone(),
        native_uri: scope_uri.to_owned(),
        native_id: None,
        native_version: None,
        native_modified_at: None,
        governance_domain: context.governance_domain.clone(),
        outcome: AcquisitionOutcome::Succeeded,
        failure_class: None,
        failure_detail: None,
        source_hash: None,
        source_object_id: None,
        source_location_id: None,
        acquired_at: utc_now()?,
        elapsed_ms: Some(elapsed_ms),
    };

    let mut connection = open_bounded_write(index_root)?;
    let tx = hot_plane::begin_write_transaction(
        &mut connection,
        TX_LOG_NAMESPACE,
        "record_enumeration",
    )?;
    if let Err(source) = insert_acquisition_record(&tx, &record) {
        return Err(hot_plane::abort_transaction(
            tx,
            TX_LOG_NAMESPACE,
            "record_enumeration",
            source,
        ));
    }
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "record_enumeration")?;

    debug!(
        event = "acquisition.enumeration_recorded",
        acquisition_record_id = record.id,
        source_system = record.source_system,
        scope_uri,
        scan_elapsed_ms = elapsed_ms,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "completed full enumeration recorded as deletion-evidence anchor"
    );
    Ok(record.id)
}

/// Apply deletion inference after one successful complete enumeration (spec
/// §11.1 signal 2): every current location of `source_system` UNDER
/// `scope_uri` whose native_uri the enumeration did not include is marked
/// deleted with absent_from_complete_enumeration evidence referencing the
/// enumeration's acquisition record, and a source.location_deleted event is
/// appended — all in one transaction. The enumeration asserts absence only
/// within its own scope, so locations outside `scope_uri` stay current.
/// Callers must only invoke this for a COMPLETE enumeration (a failed or
/// partial one asserts nothing). Source objects are never deactivated here:
/// that is C9 deletion propagation (spec §11.3). Returns the native_uris of
/// the deleted locations.
pub(crate) fn apply_enumeration_deletions(
    index_root: &crate::runtime::StorageContext,
    source_system: &str,
    scope_uri: &str,
    enumerated: &BTreeSet<String>,
    enumeration_record_id: &str,
) -> Result<Vec<String>, ApiError> {
    let started = Instant::now();
    debug!(
        event = "acquisition.enumeration_deletions_started",
        source_system,
        scope_uri,
        enumerated_count = enumerated.len(),
        enumeration_record_id,
        "applying deletion inference from complete enumeration"
    );

    let mut connection = open_bounded_write(index_root)?;
    let tx = hot_plane::begin_write_transaction(
        &mut connection,
        TX_LOG_NAMESPACE,
        "apply_enumeration_deletions",
    )?;
    let body = enumeration_deletions_body(
        &tx,
        source_system,
        scope_uri,
        enumerated,
        enumeration_record_id,
    );
    let deleted = match body {
        Ok(deleted) => deleted,
        Err(source) => {
            return Err(hot_plane::abort_transaction(
                tx,
                TX_LOG_NAMESPACE,
                "apply_enumeration_deletions",
                source,
            ));
        }
    };
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "apply_enumeration_deletions")?;

    // Keep actual deletion commits visible even if a later scheduler phase fails.
    if deleted.is_empty() {
        debug!(
            event = "acquisition.enumeration_deletions_applied",
            source_system,
            deleted_count = deleted.len(),
            enumeration_record_id,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "enumeration deletion inference applied"
        );
    } else {
        info!(
            event = "acquisition.enumeration_deletions_applied",
            source_system,
            deleted_count = deleted.len(),
            enumeration_record_id,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "enumeration deletion inference applied"
        );
    }
    Ok(deleted)
}

/// The transactional body of `apply_enumeration_deletions`: select the
/// current locations, mark each in-scope absent one deleted with its
/// evidence, and append its event on the same transaction.
fn enumeration_deletions_body(
    tx: &Transaction<'_>,
    source_system: &str,
    scope_uri: &str,
    enumerated: &BTreeSet<String>,
    enumeration_record_id: &str,
) -> Result<Vec<String>, ApiError> {
    let mut statement = tx
        .prepare(SELECT_CURRENT_LOCATIONS_FOR_SYSTEM_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare current-location listing: {source}"),
        })?;
    let rows = statement
        .query_map(params![source_system], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to list current locations of {source_system}: {source}"),
        })?;
    // Materialize the current-location snapshot before writing so the
    // UPDATEs below never race the SELECT cursor over the same table.
    let mut current_locations = Vec::new();
    for row in rows {
        current_locations.push(row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read current-location row of {source_system}: {source}"),
        })?);
    }
    drop(statement);

    let mut deleted_native_uris = Vec::new();
    for (location_id, native_uri, source_object_id) in current_locations {
        // The enumeration asserts absence only within its own scope (spec
        // §11.1): a location outside `scope_uri` was never examined, so its
        // absence from the result set is not evidence and it stays current.
        // The match is path-component-aware (exact scope or under
        // "{scope}/") and done in Rust rather than SQL LIKE, which would
        // treat % and _ in the scope as wildcards.
        let in_scope = native_uri == scope_uri
            || native_uri
                .strip_prefix(scope_uri)
                .is_some_and(|rest| rest.starts_with('/'));
        if !in_scope {
            continue;
        }
        // Presence in the complete enumeration means the location survives;
        // only proven absence is deletion evidence (spec §11.1).
        if enumerated.contains(&native_uri) {
            continue;
        }

        let evidence = DeletionEvidence {
            signal: DeletionSignal::AbsentFromCompleteEnumeration,
            observed_at: utc_now()?,
            acquisition_record_id: enumeration_record_id.to_owned(),
            detail: None,
        };
        let evidence_json = canonical_json_string_of(
            &evidence,
            &format!("deletion evidence for location {location_id}"),
        )?;
        tx.execute(
            MARK_LOCATION_DELETED_SQL,
            params![location_id, evidence_json],
        )
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to mark source location {location_id} deleted: {source}"),
        })?;

        // The event payload embeds the full evidence, so the audit trail
        // holds the durable deletion record independently of the row.
        let evidence_value =
            serde_json::to_value(&evidence).map_err(|source| ApiError::InternalIo {
                message: format!(
                    "deletion evidence for location {location_id} is not representable \
                     as JSON: {source}"
                ),
            })?;
        let mut payload = Map::from_iter([
            entry("sourceSystem", source_system),
            entry("nativeUri", &native_uri),
            entry("sourceObjectId", &source_object_id),
        ]);
        payload.insert("evidence".to_owned(), evidence_value);
        let event = new_system_event(
            SystemEventType::SourceLocationDeleted,
            OBJECT_TYPE_SOURCE_LOCATION,
            &location_id,
            Some(payload),
        )?;
        append_event(tx, &event)?;

        // Each deletion decision is individually logged with its evidence
        // facts so the operator can audit inference without the database.
        info!(
            event = "acquisition.location_deleted",
            committed = false,
            source_location_id = location_id,
            source_system,
            native_uri,
            source_object_id,
            signal = "absent_from_complete_enumeration",
            enumeration_record_id,
            "location deletion staged: absent from complete enumeration"
        );
        deleted_native_uris.push(native_uri);
    }
    Ok(deleted_native_uris)
}

/// Load the prescreen claims for every current location of one source
/// system: native_uri → (native modified time of the latest succeeded
/// acquisition, in unix ms; byte size of the location's content). Locations
/// lacking either fact — or whose recorded timestamp does not parse as the
/// fabric UTC-millisecond format — are deliberately NOT mapped, so the
/// connector treats them as unknown and re-stages them instead of trusting a
/// hole. Read-only connection; no transaction needed for one SELECT.
pub(crate) fn known_location_state(
    index_root: &crate::runtime::StorageContext,
    source_system: &str,
) -> Result<BTreeMap<String, KnownLocationState>, ApiError> {
    let started = Instant::now();
    let connection = open_bounded_read(index_root)?;
    let mut statement = connection
        .prepare(SELECT_KNOWN_LOCATION_STATE_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare known-location-state query: {source}"),
        })?;
    let rows = statement
        .query_map(params![source_system], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query known location state of {source_system}: {source}"),
        })?;

    let mut known = BTreeMap::new();
    let mut skipped: u64 = 0;
    for row in rows {
        let (native_uri, size_bytes, native_modified_at) =
            row.map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to read known-location-state row of {source_system}: {source}"
                ),
            })?;
        // Missing size, missing timestamp, unparsable timestamp, or a
        // corrupt negative size all mean "state unknown": skip so the
        // connector re-stages rather than prescreening on a guess.
        let usable = match (size_bytes, &native_modified_at) {
            (Some(size), Some(modified)) if size >= 0 => {
                parse_utc_timestamp_ms(modified).map(|native_modified_at_ms| KnownLocationState {
                    native_modified_at_ms,
                    size_bytes: size as u64,
                })
            }
            _ => None,
        };
        match usable {
            Some(state) => {
                known.insert(native_uri, state);
            }
            None => {
                skipped += 1;
                info!(
                    event = "acquisition.known_state_skipped",
                    source_system,
                    native_uri,
                    has_size = size_bytes.is_some(),
                    has_modified_at = native_modified_at.is_some(),
                    "location lacks usable prescreen facts; connector will re-stage it"
                );
            }
        }
    }

    debug!(
        event = "acquisition.known_state_loaded",
        source_system,
        known_count = known.len() as u64,
        skipped_count = skipped,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "known location state loaded"
    );
    Ok(known)
}

/// Insert one AcquisitionRecord row on the caller's transaction; enum
/// columns are rendered through their serde wire names so the persisted
/// values always match the schema CHECK sets without a second name table.
fn insert_acquisition_record(
    tx: &Transaction<'_>,
    record: &AcquisitionRecord,
) -> Result<(), ApiError> {
    let outcome = enum_wire_name(&record.outcome, "acquisition outcome")?;
    let failure_class = record
        .failure_class
        .as_ref()
        .map(|class| enum_wire_name(class, "acquisition failure class"))
        .transpose()?;
    let elapsed_ms = record
        .elapsed_ms
        .map(|value| sql_integer(value, "elapsed_ms"))
        .transpose()?;
    tx.execute(
        INSERT_ACQUISITION_RECORD_SQL,
        params![
            record.id,
            record.connector_name,
            record.connector_version,
            record.connector_config_hash,
            record.source_system,
            record.native_uri,
            record.native_id,
            record.native_version,
            record.native_modified_at,
            record.governance_domain,
            outcome,
            failure_class,
            record.failure_detail,
            record.source_hash,
            record.source_object_id,
            record.source_location_id,
            record.acquired_at,
            elapsed_ms,
        ],
    )
    .map_err(|source| ApiError::StorageOperation {
        message: format!(
            "failed to insert acquisition record {} ({} {}): {source}",
            record.id, record.source_system, record.native_uri
        ),
    })?;
    debug!(
        event = "acquisition.record_inserted",
        committed = false,
        acquisition_record_id = record.id,
        source_system = record.source_system,
        native_uri = record.native_uri,
        outcome,
        "acquisition record staged in transaction"
    );
    Ok(())
}

/// Append the acquisition.failed event for one failed record on the caller's
/// transaction; shared by the malformed-bundle and connector-failure paths.
fn append_acquisition_failed_event(
    tx: &Transaction<'_>,
    record: &AcquisitionRecord,
) -> Result<(), ApiError> {
    let mut payload = Map::from_iter([
        entry("sourceSystem", &record.source_system),
        entry("nativeUri", &record.native_uri),
    ]);
    if let Some(class) = &record.failure_class {
        payload.insert(
            "failureClass".to_owned(),
            Value::String(enum_wire_name(class, "acquisition failure class")?),
        );
    }
    let event = new_system_event(
        SystemEventType::AcquisitionFailed,
        OBJECT_TYPE_ACQUISITION_RECORD,
        &record.id,
        Some(payload),
    )?;
    append_event(tx, &event)
}

/// Open a writer with configured lock waiting and execution-scoped deadlines.
/// Idle transactions do not consume the SQL budget while external work proceeds.
fn open_bounded_write(index_root: &crate::runtime::StorageContext) -> Result<Connection, ApiError> {
    hot_plane::open_write(index_root)
}

/// Open the hot-plane reader for one acquisition operation; same statement
/// bounding status as `open_bounded_write`.
fn open_bounded_read(index_root: &crate::runtime::StorageContext) -> Result<Connection, ApiError> {
    hot_plane::open_read(index_root)
}

/// Render one closed model enum through its serde wire name (same pattern as
/// `crate::events`), so persisted column values can never drift from the
/// Rust enum or the schema CHECK set.
fn enum_wire_name<T: Serialize>(value: &T, what: &'static str) -> Result<String, ApiError> {
    match serde_json::to_value(value) {
        Ok(Value::String(name)) => Ok(name),
        // Unreachable for plain renamed enums; kept explicit so a future
        // representation change fails loudly instead of persisting garbage.
        other => Err(ApiError::InternalIo {
            message: format!("{what} did not serialize to a string: {other:?}"),
        }),
    }
}

/// Render any model shape as a canonical JSON string for a *_json column
/// (deterministic bytes per spec §16.2, same policy as the event appender).
fn canonical_json_string_of<T: Serialize>(value: &T, what: &str) -> Result<String, ApiError> {
    let bytes = crate::canonical::canonical_json_bytes_of(value)?;
    // Canonical bytes are valid UTF-8 by construction (spec §16.2); the
    // error arm keeps the panic-free Result policy instead of unwrapping.
    String::from_utf8(bytes).map_err(|source| ApiError::InternalIo {
        message: format!("canonical bytes for {what} are not UTF-8: {source}"),
    })
}

/// Convert an unsigned count into an SQLite INTEGER, rejecting values beyond
/// i64 range as a shape violation instead of panicking or wrapping.
fn sql_integer(value: u64, what: &'static str) -> Result<i64, ApiError> {
    i64::try_from(value).map_err(|_| ApiError::BadRequest {
        message: format!("{what} value {value} exceeds the SQLite integer range"),
    })
}

/// Stored mime type for plain-text sources. Single source of the routing
/// vocabulary: the scheduler's parse dispatch routes on exactly the MIME
/// constants defined here, so acquisition-time typing and dispatch-time
/// routing cannot drift apart.
pub(crate) const MIME_TYPE_PLAIN_TEXT: &str = "text/plain";

/// Stored mime type for EPUB sources (SPEC-epub §3.1); the second routed
/// MIME, sharing the single-source rule of `MIME_TYPE_PLAIN_TEXT`.
pub(crate) const MIME_TYPE_EPUB: &str = "application/epub+zip";

/// Infer the stored mime type from the native URI's filename extension. The
/// closed .txt/.epub mapping (case-insensitive) mirrors the routed parser
/// set; everything else is honestly opaque bytes rather than a guessed type.
fn mime_type_for_native_uri(native_uri: &str) -> &'static str {
    let lowered = native_uri.to_ascii_lowercase();
    if lowered.ends_with(".txt") {
        MIME_TYPE_PLAIN_TEXT
    } else if lowered.ends_with(".epub") {
        MIME_TYPE_EPUB
    } else {
        "application/octet-stream"
    }
}
