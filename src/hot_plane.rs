//! Fabric hot-plane access (decision D1): fabric.sqlite3 setup, schema
//! validation, the explicit connection/deadline policy (WAL, busy_timeout,
//! read-only opens, statement deadlines), and the shared IMMEDIATE
//! write-transaction lifecycle helpers used by every hot-plane writer.
//!
//! The hot plane is the relational half of the D1 storage layout (spec §32):
//! envelopes and pipeline state live in `{index_root}/fabric/fabric.sqlite3`;
//! heavy payloads live next door in the content-addressed artifact store.
//! Schema creation happens only through the explicit operator setup path
//! (`setup_fabric_storage`); every other entry point validates and fails
//! fatally on any mismatch — nothing here repairs or migrates at runtime.

use std::{
    collections::BTreeMap,
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use rusqlite::{OpenFlags, TransactionBehavior};
use tracing::{debug, error, info, warn};

use crate::error::ApiError;
use crate::runtime::{RuntimeSettings, StorageContext};
use crate::sqlite::{Connection, Transaction};

/// Directory under `storage.index_root` holding all fabric state (hot plane
/// and artifact store); part of the persisted D1 layout contract.
const FABRIC_DIR_NAME: &str = "fabric";

/// Filename of the fabric hot-plane SQLite database inside the fabric
/// directory; part of the persisted D1 layout contract.
const FABRIC_DATABASE_FILE_NAME: &str = "fabric.sqlite3";

/// Fabric hot-plane DDL. Kept in a dedicated schema file per the
/// External-Language Artifacts rule and applied only by
/// `setup_fabric_storage`, never as a runtime migration.
const FABRIC_SCHEMA_SQL: &str = include_str!("../sql/fabric/schema.sql");

/// `PRAGMA user_version` stamped at the end of sql/fabric/schema.sql; the
/// single source the runtime and the setup path compare against. Any other
/// value means the database was created by a different schema revision: the
/// runtime must stop rather than guess, and `--setup-storage` recreates it.
/// Bump together with every change to the schema file.
const FABRIC_SCHEMA_VERSION: i64 = 2;

/// One column of the fabric schema contract: declared name and type, and
/// whether NULL must be rejected (satisfied by NOT NULL or by PRIMARY KEY
/// membership, which SQLite reports with `pk > 0` instead of `notnull`).
struct FabricColumn {
    name: &'static str,
    declared_type: &'static str,
    not_null: bool,
}

/// Shorthand constructor keeping `FABRIC_TABLE_CONTRACTS` readable.
const fn col(name: &'static str, declared_type: &'static str, not_null: bool) -> FabricColumn {
    FabricColumn {
        name,
        declared_type,
        not_null,
    }
}

/// The full fabric hot-plane column contract, table by table. This is the
/// Rust-side mirror of sql/fabric/schema.sql: `validate_fabric_schema`
/// compares it against `PRAGMA table_info`, so any drift between DDL and
/// this list is a fatal startup error, not a silent divergence.
const FABRIC_TABLE_CONTRACTS: &[(&str, &[FabricColumn])] = &[
    (
        "source_objects",
        &[
            col("id", "TEXT", true),
            col("source_hash", "TEXT", true),
            col("active_parse_id", "TEXT", false),
            col("mime_type", "TEXT", true),
            col("size_bytes", "INTEGER", false),
            col("storage_uri", "TEXT", true),
            col("event_time", "TEXT", false),
            col("ingest_time", "TEXT", true),
            col("created_at", "TEXT", true),
            col("deactivated_at", "TEXT", false),
        ],
    ),
    (
        "source_locations",
        &[
            col("id", "TEXT", true),
            col("source_id", "TEXT", true),
            col("source_system", "TEXT", true),
            col("native_uri", "TEXT", true),
            col("native_id", "TEXT", false),
            col("governance_domain", "TEXT", true),
            col("first_seen_at", "TEXT", true),
            col("last_seen_at", "TEXT", true),
            col("status", "TEXT", true),
            col("deletion_evidence_json", "TEXT", false),
            col("metadata_json", "TEXT", false),
        ],
    ),
    (
        "acquisition_records",
        &[
            col("id", "TEXT", true),
            col("connector_name", "TEXT", true),
            col("connector_version", "TEXT", true),
            col("connector_config_hash", "TEXT", true),
            col("source_system", "TEXT", true),
            col("native_uri", "TEXT", true),
            col("native_id", "TEXT", false),
            col("native_version", "TEXT", false),
            col("native_modified_at", "TEXT", false),
            col("governance_domain", "TEXT", true),
            col("outcome", "TEXT", true),
            col("failure_class", "TEXT", false),
            col("failure_detail", "TEXT", false),
            col("source_hash", "TEXT", false),
            col("source_object_id", "TEXT", false),
            col("source_location_id", "TEXT", false),
            col("acquired_at", "TEXT", true),
            col("elapsed_ms", "INTEGER", false),
        ],
    ),
    (
        "sync_queue",
        &[
            col("id", "TEXT", true),
            col("source_key", "TEXT", true),
            col("source_system", "TEXT", true),
            col("native_uri", "TEXT", true),
            col("detected_at", "TEXT", true),
            col("reason", "TEXT", true),
            col("state", "TEXT", true),
            col("attempt_count", "INTEGER", true),
            col("last_attempt_at", "TEXT", false),
            col("last_error", "TEXT", false),
            col("coalesced_count", "INTEGER", true),
            col("created_at", "TEXT", true),
            // Nullable §34.6 Operation link: set only on HTTP-enqueued
            // (queue-coupled) rows so the drain can complete their Operation;
            // NULL for autonomous scheduler detections.
            col("operation_id", "TEXT", false),
        ],
    ),
    (
        "parse_runs",
        &[
            col("id", "TEXT", true),
            col("source_id", "TEXT", true),
            col("parser_name", "TEXT", true),
            col("parser_version", "TEXT", true),
            col("parser_config_hash", "TEXT", true),
            col("capability_profile_hash", "TEXT", true),
            col("status", "TEXT", true),
            col("held_reason", "TEXT", false),
            col("conformance_report_json", "TEXT", false),
            col("started_at", "TEXT", false),
            col("completed_at", "TEXT", false),
            col("activated_at", "TEXT", false),
            col("archived_at", "TEXT", false),
            col("artifact_bundle_uri", "TEXT", false),
            col("artifact_bundle_hash", "TEXT", false),
            col("parser_raw_output_uri", "TEXT", false),
            col("warnings_json", "TEXT", false),
            col("metrics_json", "TEXT", false),
            col("error", "TEXT", false),
            col("created_at", "TEXT", true),
        ],
    ),
    (
        "content_units",
        &[
            col("id", "TEXT", true),
            col("source_id", "TEXT", true),
            col("parse_id", "TEXT", true),
            col("content_type", "TEXT", true),
            col("body_hash", "TEXT", true),
            col("text_hash", "TEXT", false),
            col("structure_hash", "TEXT", false),
            col("primary_parent_id", "TEXT", false),
            col("sequence_index", "INTEGER", false),
            col("locators_json", "TEXT", false),
            col("body_json", "TEXT", true),
            col("created_at", "TEXT", true),
        ],
    ),
    (
        "unit_relationships",
        &[
            col("id", "TEXT", true),
            col("source_id", "TEXT", true),
            col("parse_id", "TEXT", true),
            col("from_unit_id", "TEXT", true),
            col("to_unit_id", "TEXT", true),
            col("relationship_type", "TEXT", true),
            col("relationship_role", "TEXT", false),
            col("sequence_index", "INTEGER", false),
            col("confidence", "REAL", false),
            col("provenance_json", "TEXT", false),
            col("created_at", "TEXT", true),
        ],
    ),
    (
        "retrieval_projections",
        &[
            col("id", "TEXT", true),
            col("source_id", "TEXT", false),
            col("parse_id", "TEXT", false),
            col("projection_type", "TEXT", true),
            col("input_unit_ids_json", "TEXT", false),
            // Nullable: §22 inputAnnotationIds is optional, set only by
            // annotation-derived projections (C6d summary, C6f graph).
            col("input_annotation_ids_json", "TEXT", false),
            col("producer_json", "TEXT", true),
            col("index_name", "TEXT", false),
            col("index_partition", "TEXT", false),
            col("payload_uri", "TEXT", false),
            col("freshness_status", "TEXT", true),
            col("created_at", "TEXT", true),
            col("valid_from", "TEXT", false),
            col("valid_to", "TEXT", false),
            col("deleted_at", "TEXT", false),
        ],
    ),
    (
        "query_execution_records",
        &[
            col("id", "TEXT", true),
            col("query_hash", "TEXT", true),
            col("executed_at", "TEXT", true),
            col("plan_hash", "TEXT", true),
            col("evidence_pack_hash", "TEXT", true),
            col("archive_uri", "TEXT", true),
            col("archive_hash", "TEXT", true),
            col("created_at", "TEXT", true),
        ],
    ),
    (
        // §30 ForensicSnapshot metadata rows. Mirror of the forensic_snapshots
        // DDL in sql/fabric/schema.sql; subject_source_id/subject_parse_id are
        // nullable (only lifecycle snapshots carry a subject), the *_ids_json
        // columns are canonical JSON string arrays.
        "forensic_snapshots",
        &[
            col("id", "TEXT", true),
            col("snapshot_type", "TEXT", true),
            col("subject_source_id", "TEXT", false),
            col("subject_parse_id", "TEXT", false),
            col("source_object_ids_json", "TEXT", true),
            col("active_parse_ids_json", "TEXT", true),
            col("manifest_uri", "TEXT", true),
            col("manifest_hash", "TEXT", true),
            col("system_version", "TEXT", true),
            col("spec_version", "TEXT", true),
            col("created_at", "TEXT", true),
        ],
    ),
    (
        // §34.6 Operation metadata rows. Mirror of the operations DDL in
        // sql/fabric/schema.sql; started_at/completed_at/error are nullable
        // (a pending/running Operation has not reached those milestones).
        "operations",
        &[
            col("id", "TEXT", true),
            col("operation_type", "TEXT", true),
            col("status", "TEXT", true),
            col("target_object_type", "TEXT", true),
            col("target_object_id", "TEXT", true),
            col("started_at", "TEXT", false),
            col("completed_at", "TEXT", false),
            col("error", "TEXT", false),
            col("created_at", "TEXT", true),
        ],
    ),
    (
        "semantic_annotations",
        &[
            col("id", "TEXT", true),
            col("source_id", "TEXT", true),
            col("parse_id", "TEXT", true),
            col("target_unit_ids_json", "TEXT", true),
            col("annotation_type", "TEXT", true),
            // body_json is nullable: a building or failed annotation has no
            // body yet (schema §21 envelope), so NOT NULL would be wrong.
            col("body_json", "TEXT", false),
            col("provenance_json", "TEXT", true),
            col("confidence", "REAL", false),
            col("freshness_status", "TEXT", true),
            col("memoization_key_hash", "TEXT", true),
            // CA2 content-scoped satisfaction key; NOT NULL, populated by
            // CA2-P1. Distinct from memoization_key_hash (which also folds in
            // producer identity).
            col("content_key_hash", "TEXT", true),
            col("created_at", "TEXT", true),
            col("deleted_at", "TEXT", false),
        ],
    ),
    (
        "annotation_memo",
        &[
            // memoization_key_hash is the PRIMARY KEY, so its NOT NULL
            // requirement is satisfied by PK membership (pk > 0), matching
            // how every other table's id column is declared here.
            col("memoization_key_hash", "TEXT", true),
            col("annotation_type", "TEXT", true),
            col("producer_identity_hash", "TEXT", true),
            col("body_json", "TEXT", true),
            col("confidence", "REAL", false),
            col("original_annotation_id", "TEXT", true),
            col("created_at", "TEXT", true),
        ],
    ),
    (
        "system_events",
        &[
            col("id", "TEXT", true),
            col("event_type", "TEXT", true),
            col("object_type", "TEXT", true),
            col("object_id", "TEXT", true),
            col("payload_json", "TEXT", false),
            col("created_at", "TEXT", true),
        ],
    ),
    (
        // CA2 policy substrate: append-only version log. policy_id and version
        // together form the PRIMARY KEY, so both satisfy their NOT NULL
        // requirement by PK membership (pk > 0), matching the annotation_memo
        // and chunk_dense_vectors PK-column declarations above.
        "policy_versions",
        &[
            col("policy_id", "TEXT", true),
            col("version", "INTEGER", true),
            col("content_hash", "TEXT", true),
            col("observed_at", "TEXT", true),
        ],
    ),
    // C6 retrieval-projection payload tables (spec §22–§23). The FTS5 lexical
    // index `chunk_text_index` is intentionally absent from this list: a
    // virtual table reports empty column types through PRAGMA table_info, so
    // it fails the type contract and is validated by existence instead
    // (`FABRIC_VIRTUAL_TABLES` / `validate_fabric_virtual_table`).
    (
        "chunk_projections",
        &[
            col("id", "TEXT", true),
            col("projection_id", "TEXT", true),
            col("source_id", "TEXT", true),
            col("parse_id", "TEXT", true),
            col("input_unit_ids_json", "TEXT", true),
            col("targeting_text", "TEXT", true),
            // token_count is nullable: §22 ChunkPayload.tokenCount is optional.
            col("token_count", "INTEGER", false),
            // Fine-grain membership (PLAN-grains.md Section 2): per-unit scalar
            // ranges and the section path used only as the model-input prefix.
            col("fragments_json", "TEXT", true),
            col("section_path_json", "TEXT", true),
            col("chunker_name", "TEXT", true),
            col("chunker_version", "TEXT", true),
            col("chunker_config_hash", "TEXT", true),
            col("created_at", "TEXT", true),
            // 0-based reading-order position within the parse: the only
            // reading-order authority for chunks (schema.sql chunk_projections).
            col("chunk_index", "INTEGER", true),
        ],
    ),
    (
        "chunk_dense_vectors",
        &[
            // chunk_id is the PRIMARY KEY (one dense vector per chunk), so its
            // NOT NULL requirement is satisfied by PK membership.
            col("chunk_id", "TEXT", true),
            col("source_id", "TEXT", true),
            col("parse_id", "TEXT", true),
            col("dimension", "INTEGER", true),
            col("norm", "REAL", true),
            col("vector_blob", "BLOB", true),
            col("created_at", "TEXT", true),
        ],
    ),
    (
        "colbert_windows",
        &[
            col("id", "TEXT", true),
            col("projection_id", "TEXT", true),
            col("source_id", "TEXT", true),
            col("parse_id", "TEXT", true),
            // ColBERT-grain membership (PLAN-grains.md Section 2): reading-order
            // position, member chunk ids in order, and their concatenated
            // scalar-offset fragments (schema.sql colbert_windows).
            col("window_index", "INTEGER", true),
            col("chunk_ids_json", "TEXT", true),
            col("fragments_json", "TEXT", true),
            col("token_count", "INTEGER", true),
            col("dimension", "INTEGER", true),
            col("matrix_blob", "BLOB", true),
            col("created_at", "TEXT", true),
        ],
    ),
    (
        "graph_entity_mentions",
        &[
            col("id", "TEXT", true),
            col("projection_id", "TEXT", true),
            col("source_id", "TEXT", true),
            col("parse_id", "TEXT", true),
            col("normalized_name", "TEXT", true),
            // entity_type is nullable: D9 makes it node metadata, not identity.
            col("entity_type", "TEXT", false),
            col("unit_ids_json", "TEXT", true),
            col("created_at", "TEXT", true),
        ],
    ),
    (
        "graph_entity_edges",
        &[
            col("id", "TEXT", true),
            col("projection_id", "TEXT", true),
            col("source_id", "TEXT", true),
            col("parse_id", "TEXT", true),
            col("from_normalized_name", "TEXT", true),
            col("to_normalized_name", "TEXT", true),
            col("relation_type", "TEXT", true),
            col("target_unit_ids_json", "TEXT", true),
            col("created_at", "TEXT", true),
        ],
    ),
];

/// Fabric virtual tables validated by existence rather than by the column
/// contract. FTS5 virtual tables report empty column types through
/// PRAGMA table_info, so the ordinary type/nullability battery cannot cover
/// them; presence in sqlite_master (with the expected module) is the
/// strongest check available and matches the never-repair startup policy.
const FABRIC_VIRTUAL_TABLES: &[&str] = &["chunk_text_index"];

/// Absolute path of the fabric hot-plane database under the
/// operator-configured index root (absoluteness is guaranteed by config
/// validation of `storage.index_root`).
fn fabric_database_path(index_root: &Path) -> PathBuf {
    index_root
        .join(FABRIC_DIR_NAME)
        .join(FABRIC_DATABASE_FILE_NAME)
}

/// Tables whose row counts are logged before an incompatible database is
/// deleted: the ones holding operator-visible work (parsed units, produced
/// annotations, cached annotator responses) that a recreate throws away.
const LOST_ROW_COUNT_TABLES: [&str; 3] =
    ["content_units", "semantic_annotations", "annotation_memo"];

/// Why an existing fabric database cannot be kept, captured together with the
/// row counts of `LOST_ROW_COUNT_TABLES` while the database is still open, so
/// the operator log describes the loss before the directory is deleted.
struct FabricIncompatibility {
    reason: String,
    /// Parallel to `LOST_ROW_COUNT_TABLES`; a count is the decimal number or
    /// `unavailable` when the table cannot be counted (typically missing).
    row_counts: [String; 3],
}

/// Create the fabric hot-plane database through the explicit operator setup
/// action — the only schema-creation path. Re-runs keep a compatible existing
/// database (same schema version, every contract valid) and only validate it;
/// an incompatible one is never migrated — the whole fabric directory is
/// deleted after its loss is logged, and a fresh database is created. This
/// function owns the setup diagnostic boundary and logs start, terminal
/// success, and terminal failure with elapsed time.
pub(crate) fn setup_fabric_storage(index_root: &StorageContext) -> Result<PathBuf, ApiError> {
    let started = Instant::now();
    let db_path = fabric_database_path(index_root);
    let already_exists = db_path.exists();
    info!(
        event = "hot_plane.setup_started",
        db_path = %db_path.display(),
        already_exists,
        "fabric hot-plane setup starting"
    );
    let outcome = if already_exists {
        setup_existing_fabric_database(index_root, &db_path)
    } else {
        setup_fabric_database(&db_path, index_root.shared_settings()).map(|()| "created")
    };
    match outcome {
        Ok(mode) => {
            info!(
                event = "hot_plane.setup_completed",
                db_path = %db_path.display(),
                mode,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "fabric hot-plane setup completed"
            );
            Ok(db_path)
        }
        Err(source) => {
            error!(
                event = "hot_plane.setup_failed",
                db_path = %db_path.display(),
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "fabric hot-plane setup failed"
            );
            Err(source)
        }
    }
}

/// Setup re-run against an existing database: keep and report it when
/// compatible, otherwise log the reason and the rows about to be lost, delete
/// the fabric directory, and create fresh. Returns the completion mode.
fn setup_existing_fabric_database(
    index_root: &StorageContext,
    db_path: &Path,
) -> Result<&'static str, ApiError> {
    let incompatibility =
        match inspect_existing_fabric_database(db_path, index_root.shared_settings())? {
            None => return Ok("validated_existing"),
            Some(incompatibility) => incompatibility,
        };
    warn!(
        event = "storage_setup.incompatible_database",
        db_path = %db_path.display(),
        reason = %incompatibility.reason,
        "existing fabric database is incompatible with this schema; it will be deleted and recreated"
    );
    let [content_units, semantic_annotations, annotation_memo] = &incompatibility.row_counts;
    warn!(
        event = "storage_setup.rows_to_be_lost",
        db_path = %db_path.display(),
        content_units = %content_units,
        semantic_annotations = %semantic_annotations,
        annotation_memo = %annotation_memo,
        "row counts in the incompatible fabric database about to be deleted"
    );
    let deleted_path = remove_fabric_directory(index_root)?;
    setup_fabric_database(db_path, index_root.shared_settings())?;
    warn!(
        event = "storage_setup.recreated",
        deleted_path = %deleted_path.display(),
        db_path = %db_path.display(),
        "fabric directory deleted and fabric database recreated"
    );
    Ok("recreated")
}

/// Decide whether an existing database is compatible: `Ok(None)` when it
/// passes, `Ok(Some(..))` with the reason and pre-deletion row counts when it
/// does not. Failing to open the file read-write (locked, unreadable, not
/// WAL) is an ordinary error, not grounds for deletion. The connection is
/// dropped before returning so nothing holds the file open during deletion.
fn inspect_existing_fabric_database(
    db_path: &Path,
    settings: Arc<RuntimeSettings>,
) -> Result<Option<FabricIncompatibility>, ApiError> {
    let connection = open_write_at(db_path, settings)?;
    // Version first: a stamp from another revision is reported as such even
    // when the column contracts also differ, and only a matching stamp earns
    // the full contract battery.
    let reason = match read_fabric_schema_version(&connection) {
        Ok(version) if version == FABRIC_SCHEMA_VERSION => validate_fabric_schema(&connection)
            .err()
            .map(|source| source.to_string()),
        Ok(version) => Some(format!(
            "fabric schema version is {version}, expected {FABRIC_SCHEMA_VERSION}"
        )),
        Err(source) => Some(source.to_string()),
    };
    Ok(reason.map(|reason| FabricIncompatibility {
        reason,
        row_counts: LOST_ROW_COUNT_TABLES
            .map(|table_name| fabric_row_count_display(&connection, table_name)),
    }))
}

/// Count one table's rows for the pre-deletion log; a table that cannot be
/// counted (missing in the old revision, or unreadable) reads `unavailable`
/// rather than failing setup, since the database is being discarded anyway.
fn fabric_row_count_display(connection: &Connection, table_name: &str) -> String {
    // The table name comes from the static LOST_ROW_COUNT_TABLES list, never
    // from input, so interpolating it into the statement is safe.
    connection
        .query_row(&format!("SELECT COUNT(*) FROM {table_name};"), [], |row| {
            row.get::<_, i64>(0)
        })
        .map_or_else(|_| "unavailable".to_string(), |count| count.to_string())
}

/// Delete the whole `{index_root}/fabric` tree (hot plane and artifact
/// store) ahead of a recreate, returning the deleted path. This is the only
/// deletion in the module and it is guarded to exactly that directory.
fn remove_fabric_directory(index_root: &StorageContext) -> Result<PathBuf, ApiError> {
    let fabric_dir = index_root.join(FABRIC_DIR_NAME);
    // Deletion guard: refuse unless the target is a real directory (a symlink
    // signals a layout setup does not own, so it is refused rather than
    // unlinked) named `fabric` whose parent is the index root itself. The
    // path is built from constants, so a failure here means the layout on
    // disk is not the one setup owns.
    let is_real_directory = fs::symlink_metadata(&fabric_dir)
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false);
    let is_fabric_under_index_root = fabric_dir.file_name() == Some(OsStr::new(FABRIC_DIR_NAME))
        && fabric_dir.parent() == Some(index_root.as_ref());
    if !(is_real_directory && is_fabric_under_index_root) {
        return Err(ApiError::StorageInit {
            message: format!(
                "refusing to delete {}: not a directory named {FABRIC_DIR_NAME} directly under \
                 index root {}",
                fabric_dir.display(),
                index_root.display()
            ),
        });
    }
    fs::remove_dir_all(&fabric_dir).map_err(|source| ApiError::StorageInit {
        message: format!(
            "failed to delete fabric directory {}: {source}",
            fabric_dir.display()
        ),
    })?;
    Ok(fabric_dir)
}

/// Create a fresh, stamped fabric database at `db_path` for
/// `setup_fabric_storage`: create the fabric directory, build at a temp path,
/// publish by rename. The caller guarantees no database exists at `db_path`.
fn setup_fabric_database(db_path: &Path, settings: Arc<RuntimeSettings>) -> Result<(), ApiError> {
    // The fabric directory is shared with the artifact store; creating it
    // here keeps setup self-sufficient on a fresh index root.
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent).map_err(|source| ApiError::StorageInit {
            message: format!(
                "failed to create fabric directory {}: {source}",
                parent.display()
            ),
        })?;
    }

    // Build at a temp path in the same directory, then publish with one
    // atomic rename. A crash mid-setup therefore never leaves a partial
    // database at the real path, only an inert temp file the next setup run
    // overwrites.
    let temp_path = db_path.with_extension("sqlite3.setup-tmp");
    let build_result = build_fresh_fabric_database(&temp_path, settings);
    if let Err(source) = build_result {
        // Best-effort cleanup keeps failed setups re-runnable; the build
        // error stays the primary diagnostic.
        let _ = fs::remove_file(&temp_path);
        return Err(source);
    }
    fs::rename(&temp_path, db_path).map_err(|source| ApiError::StorageInit {
        message: format!(
            "failed to publish fabric database from {} to {}: {source}",
            temp_path.display(),
            db_path.display()
        ),
    })
}

/// Create and stamp a fresh fabric database at `build_path` (the setup temp
/// path): the only code path allowed to create the file and apply DDL, so it
/// opens with the CREATE flag directly instead of going through `open_write`
/// (which refuses to create). The connection is closed before the caller
/// renames the finished file into place, so no WAL sidecar files survive.
fn build_fresh_fabric_database(
    build_path: &Path,
    settings: Arc<RuntimeSettings>,
) -> Result<(), ApiError> {
    let connection =
        Connection::open(build_path, settings).map_err(|source| ApiError::StorageInit {
            message: format!(
                "failed to create fabric database at {}: {source}",
                build_path.display()
            ),
        })?;
    apply_connection_policy(&connection, build_path)?;
    // journal_mode=WAL is persistent database state (stored in the file
    // header); it is set once here and only verified everywhere else.
    let journal_mode = connection
        .query_row("PRAGMA journal_mode = WAL;", [], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|source| ApiError::StorageInit {
            message: format!(
                "failed to set fabric journal_mode to WAL at {}: {source}",
                build_path.display()
            ),
        })?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(ApiError::StorageInit {
            message: format!("fabric journal_mode after setup is {journal_mode}, expected wal"),
        });
    }
    apply_full_synchronous(&connection, build_path)?;
    connection
        .execute_batch(FABRIC_SCHEMA_SQL)
        .map_err(|source| ApiError::StorageInit {
            message: format!("failed to create fabric schema: {source}"),
        })?;
    validate_fabric_schema(&connection)?;
    // Checkpoint and close so the finished database is a single file the
    // caller can atomically rename; a lingering -wal/-shm pair would not
    // follow the rename.
    connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .map_err(|source| ApiError::StorageInit {
            message: format!("failed to checkpoint fresh fabric database: {source}"),
        })?;
    connection.close().map_err(|source| ApiError::StorageInit {
        message: format!("failed to close fresh fabric database: {source}"),
    })
}

/// Open a read-only connection to the fabric hot plane with the D1
/// per-connection policy applied and the WAL journal mode verified. A
/// missing or non-WAL database is a fatal error pointing at setup — the
/// read path never creates or repairs anything.
pub(crate) fn open_read(index_root: &StorageContext) -> Result<Connection, ApiError> {
    let db_path = fabric_database_path(index_root);
    let connection = Connection::open_with_flags(
        &db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        index_root.shared_settings(),
    )
    .map_err(|source| {
        open_failed(&db_path, "read", &source);
        ApiError::StorageInit {
            message: format!(
                "failed to open fabric database read-only at {}: {source}; run --setup-storage",
                db_path.display()
            ),
        }
    })?;
    apply_connection_policy(&connection, &db_path)?;
    verify_wal_journal_mode(&connection, &db_path)?;
    Ok(connection)
}

/// Open a read-write connection to the fabric hot plane. Write access is
/// explicit and narrowly scoped: the connection never creates the database
/// (setup owns creation), applies the D1 per-connection policy plus FULL
/// synchronous durability, and fails fatally if the journal mode is not WAL.
pub(crate) fn open_write(index_root: &StorageContext) -> Result<Connection, ApiError> {
    let db_path = fabric_database_path(index_root);
    open_write_at(&db_path, index_root.shared_settings())
}

/// `open_write` body shared with the setup validation path, which already
/// holds the resolved database path.
fn open_write_at(db_path: &Path, settings: Arc<RuntimeSettings>) -> Result<Connection, ApiError> {
    let connection = Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        settings,
    )
    .map_err(|source| {
        open_failed(db_path, "write", &source);
        ApiError::StorageInit {
            message: format!(
                "failed to open fabric database read-write at {}: {source}; run --setup-storage",
                db_path.display()
            ),
        }
    })?;
    apply_connection_policy(&connection, db_path)?;
    apply_full_synchronous(&connection, db_path)?;
    verify_wal_journal_mode(&connection, db_path)?;
    Ok(connection)
}

/// Begin one IMMEDIATE write transaction on a hot-plane connection, with the
/// begin boundary logged under `{log_namespace}.transaction_begin`. IMMEDIATE
/// takes the write lock up front so a mid-transaction lock upgrade can never
/// deadlock against a competing writer. This module owns the D1 connection
/// policy, so it also owns the shared transaction lifecycle helpers; callers
/// (`crate::acquisition`, `crate::scheduler`) pass their log-event namespace
/// so boundary logs stay attributable to the owning subsystem.
pub(crate) fn begin_write_transaction<'c>(
    connection: &'c mut Connection,
    log_namespace: &'static str,
    operation: &'static str,
) -> Result<Transaction<'c>, ApiError> {
    let started = Instant::now();
    debug!(
        event = %format!("{log_namespace}.transaction_begin"),
        operation, "write transaction beginning"
    );
    connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .inspect(|_| {
            debug!(event = %format!("{log_namespace}.transaction_lock_acquired"),
                operation, lock_wait_ms = started.elapsed().as_millis() as u64,
                "write transaction acquired SQLite writer lock");
        })
        .map_err(|source| {
            error!(
                event = %format!("{log_namespace}.transaction_begin_failed"),
                operation,
                lock_wait_ms = started.elapsed().as_millis() as u64,
                error = %source,
                "write transaction begin failed"
            );
            ApiError::StorageOperation {
                message: format!("failed to begin {operation} transaction: {source}"),
            }
        })
}

/// Outcome of a contention-aware write-transaction begin: either the IMMEDIATE
/// transaction was taken (`Begun`) or the writer lock was held by a competing
/// writer past the connection's `busy_timeout` (`Busy`). `Busy` after the
/// busy_timeout expiry is the authoritative writer-contention signal, not a
/// storage fault: the scheduler's projection-build transaction holds the writer
/// lock across a source's entire dense/ColBERT embed (tens of minutes), so a
/// competing writer legitimately cannot begin. Callers use `Busy` to defer or
/// wait rather than treating contention as an error.
pub(crate) enum WriteTransactionAttempt<'c> {
    Begun(Transaction<'c>),
    Busy,
}

/// Contention-aware sibling of `begin_write_transaction`: attempt one IMMEDIATE
/// write transaction, but classify a post-busy_timeout `SQLITE_BUSY` as writer
/// contention (`Busy`) instead of a storage fault. On `Busy` the begin boundary
/// is logged at DEBUG (`{log_namespace}.transaction_begin_busy`) because
/// contention is normal operation here — the scheduler holds the writer lock
/// across a source's whole embed — and the caller decides whether to defer or
/// wait. Every other rusqlite error takes the identical ERROR log and
/// `ApiError::StorageOperation` mapping as `begin_write_transaction`, so true
/// faults stay exactly as loud. This exists so the annotation worker can stop
/// logging an ERROR pair every idle cycle on ordinary writer contention.
pub(crate) fn begin_write_transaction_if_free<'c>(
    connection: &'c mut Connection,
    log_namespace: &'static str,
    operation: &'static str,
) -> Result<WriteTransactionAttempt<'c>, ApiError> {
    let started = Instant::now();
    debug!(
        event = %format!("{log_namespace}.transaction_begin"),
        operation, "write transaction beginning"
    );
    match connection.transaction_with_behavior(TransactionBehavior::Immediate) {
        Ok(tx) => {
            debug!(event = %format!("{log_namespace}.transaction_lock_acquired"),
                operation, lock_wait_ms = started.elapsed().as_millis() as u64,
                "write transaction acquired SQLite writer lock");
            Ok(WriteTransactionAttempt::Begun(tx))
        }
        Err(rusqlite::Error::SqliteFailure(err, _))
            if err.code == rusqlite::ErrorCode::DatabaseBusy =>
        {
            debug!(
                event = %format!("{log_namespace}.transaction_begin_busy"),
                operation,
                lock_wait_ms = started.elapsed().as_millis() as u64,
                "write transaction begin deferred; writer lock held (busy_timeout expired)"
            );
            Ok(WriteTransactionAttempt::Busy)
        }
        Err(source) => {
            error!(
                event = %format!("{log_namespace}.transaction_begin_failed"),
                operation,
                lock_wait_ms = started.elapsed().as_millis() as u64,
                error = %source,
                "write transaction begin failed"
            );
            Err(ApiError::StorageOperation {
                message: format!("failed to begin {operation} transaction: {source}"),
            })
        }
    }
}

/// Begin one DEFERRED read transaction on a hot-plane connection — the
/// read-only twin of `begin_write_transaction`. It exists to open the single
/// per-query WAL read snapshot the DP1 pipeline requires: every hot-plane read
/// for one query runs inside this one transaction on one connection, so the
/// scope-filtered active-set capture and all subsequent channel reads share
/// one consistent snapshot (§31.1 — in-flight queries execute entirely against
/// their captured pre-cutover view). DEFERRED (not IMMEDIATE) is the whole
/// point: a read-only query must not take a write lock. Recorded tradeoff: the
/// pinned WAL read snapshot blocks checkpointing past it for the query's
/// duration, bounded by the C8d single-search admission window. Callers pass
/// their log-event namespace so the begin boundary stays attributable.
pub(crate) fn begin_read_transaction<'c>(
    connection: &'c mut Connection,
    log_namespace: &'static str,
    operation: &'static str,
) -> Result<Transaction<'c>, ApiError> {
    debug!(
        event = %format!("{log_namespace}.transaction_begin"),
        operation, "read transaction beginning"
    );
    connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(|source| {
            error!(
                event = %format!("{log_namespace}.transaction_begin_failed"),
                operation,
                error = %source,
                "read transaction begin failed"
            );
            ApiError::StorageOperation {
                message: format!("failed to begin {operation} transaction: {source}"),
            }
        })
}

/// Commit one hot-plane transaction with DEBUG attempt/success and ERROR failure
/// logging under the caller's namespace. Callers own INFO outcomes for meaningful
/// durable changes; after success every row written on the transaction is durable.
pub(crate) fn commit_transaction(
    tx: Transaction<'_>,
    log_namespace: &'static str,
    operation: &'static str,
) -> Result<(), ApiError> {
    let started = Instant::now();
    debug!(
        event = %format!("{log_namespace}.transaction_commit"),
        operation, "transaction commit attempt"
    );
    tx.commit().map_err(|source| {
        error!(
            event = %format!("{log_namespace}.transaction_commit_failed"),
            operation,
            commit_elapsed_ms = started.elapsed().as_millis() as u64,
            error = %source,
            durable_outcome = "unknown",
            "transaction commit failed; durable outcome is unconfirmed"
        );
        ApiError::StorageOperation {
            message: format!("failed to commit {operation} transaction: {source}"),
        }
    })?;
    debug!(
        event = %format!("{log_namespace}.transaction_committed"),
        commit_elapsed_ms = started.elapsed().as_millis() as u64,
        operation, "transaction committed"
    );
    Ok(())
}

/// Drop a failed transaction and preserve its original error. Rusqlite attempts
/// rollback on drop but does not expose that result, so logs cannot confirm it.
pub(crate) fn abort_transaction(
    tx: Transaction<'_>,
    log_namespace: &'static str,
    operation: &'static str,
    source: ApiError,
) -> ApiError {
    error!(
        event = %format!("{log_namespace}.transaction_rolled_back"),
        operation,
        error = %source,
        rollback_state = "requested_on_drop",
        durable_outcome = "unconfirmed",
        "transaction body failed; rollback will be attempted on drop"
    );
    drop(tx);
    source
}

/// Log one failed fabric connection open with its local facts; the typed
/// error carries the same context to the caller.
fn open_failed(db_path: &Path, mode: &'static str, source: &rusqlite::Error) {
    error!(
        event = "hot_plane.open_failed",
        db_path = %db_path.display(),
        mode,
        error = %source,
        "fabric database open failed"
    );
}

/// Enable foreign keys on every connection. The bounded SQLite owner already
/// installed configured lock waiting and execution deadlines before this call.
fn apply_connection_policy(connection: &Connection, db_path: &Path) -> Result<(), ApiError> {
    connection
        .execute_batch("PRAGMA foreign_keys = ON;")
        .map_err(|source| ApiError::StorageInit {
            message: format!(
                "failed to enable fabric foreign keys on {}: {source}",
                db_path.display()
            ),
        })
}

/// Set `synchronous = FULL` on a write-capable connection. Unlike
/// journal_mode, synchronous is per-connection state, so the D1 durability
/// policy must be re-applied on every writer, not just at setup.
fn apply_full_synchronous(connection: &Connection, db_path: &Path) -> Result<(), ApiError> {
    connection
        .execute_batch("PRAGMA synchronous = FULL;")
        .map_err(|source| ApiError::StorageInit {
            message: format!(
                "failed to set fabric synchronous to FULL on {}: {source}",
                db_path.display()
            ),
        })
}

/// Verify the persistent journal mode is WAL and fail fatally otherwise.
/// A non-WAL fabric database was not produced by setup; repairing it at
/// runtime would hide the corruption of the operational contract, so every
/// open refuses instead.
fn verify_wal_journal_mode(connection: &Connection, db_path: &Path) -> Result<(), ApiError> {
    let journal_mode = connection
        .query_row("PRAGMA journal_mode;", [], |row| row.get::<_, String>(0))
        .map_err(|source| ApiError::StorageInit {
            message: format!("failed to read fabric journal_mode: {source}"),
        })?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(ApiError::StorageInit {
            message: format!(
                "fabric database at {} has journal_mode {journal_mode}, expected wal; \
                 it was not created by --setup-storage and will not be repaired at runtime",
                db_path.display()
            ),
        });
    }
    Ok(())
}

/// Validate the fabric hot-plane contract on an open connection: schema
/// version, every required table with its exact column set, and the WAL
/// journal mode. Any missing or mismatched element is a fatal error naming
/// the precise table/column, never a repair or a warning.
pub(crate) fn validate_fabric_schema(connection: &Connection) -> Result<(), ApiError> {
    validate_fabric_schema_version(connection)?;
    for (table_name, expected_columns) in FABRIC_TABLE_CONTRACTS {
        validate_fabric_table(connection, table_name, expected_columns)?;
    }
    for virtual_table_name in FABRIC_VIRTUAL_TABLES {
        validate_fabric_virtual_table(connection, virtual_table_name)?;
    }
    // Journal mode is part of the operational contract (D1), so schema
    // validation re-checks it even though the openers already did: this
    // function is also the whole battery for the idempotent setup re-run.
    let journal_mode = connection
        .query_row("PRAGMA journal_mode;", [], |row| row.get::<_, String>(0))
        .map_err(|source| ApiError::StorageInit {
            message: format!("failed to read fabric journal_mode: {source}"),
        })?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(ApiError::StorageInit {
            message: format!("fabric journal_mode is {journal_mode}, expected wal"),
        });
    }
    Ok(())
}

/// Read the `PRAGMA user_version` stamp without judging it, so the setup path
/// can compare versions before running the column contracts.
fn read_fabric_schema_version(connection: &Connection) -> Result<i64, ApiError> {
    connection
        .query_row("PRAGMA user_version;", [], |row| row.get::<_, i64>(0))
        .map_err(|source| ApiError::StorageInit {
            message: format!("failed to read fabric schema version: {source}"),
        })
}

/// Check the stamped schema version so a database from another schema
/// revision stops the process instead of being silently reinterpreted.
fn validate_fabric_schema_version(connection: &Connection) -> Result<(), ApiError> {
    let version = read_fabric_schema_version(connection)?;
    if version != FABRIC_SCHEMA_VERSION {
        return Err(ApiError::StorageInit {
            message: format!(
                "fabric schema version is {version}, expected \
                 {FABRIC_SCHEMA_VERSION}; run --setup-storage"
            ),
        });
    }
    Ok(())
}

/// One `PRAGMA table_info` row reduced to the facts the contract checks.
struct ActualColumn {
    declared_type: String,
    not_null: bool,
    in_primary_key: bool,
}

/// Validate one fabric table against its contract entry: the table exists,
/// every expected column is present with the declared type and nullability
/// (PRIMARY KEY membership satisfies a NOT NULL requirement), and no
/// unexpected columns exist.
fn validate_fabric_table(
    connection: &Connection,
    table_name: &str,
    expected: &[FabricColumn],
) -> Result<(), ApiError> {
    // PRAGMA table_info takes an identifier, not a bind parameter; the name
    // comes exclusively from the static FABRIC_TABLE_CONTRACTS list.
    let table_info_sql = format!("PRAGMA table_info({table_name});");
    let mut statement =
        connection
            .prepare(&table_info_sql)
            .map_err(|source| ApiError::StorageInit {
                message: format!("failed to prepare column inspection for {table_name}: {source}"),
            })?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                ActualColumn {
                    declared_type: row.get::<_, String>(2)?,
                    not_null: row.get::<_, i64>(3)? != 0,
                    in_primary_key: row.get::<_, i64>(5)? != 0,
                },
            ))
        })
        .map_err(|source| ApiError::StorageInit {
            message: format!("failed to inspect fabric table {table_name}: {source}"),
        })?;
    let mut actual = BTreeMap::new();
    for row in rows {
        let (name, column) = row.map_err(|source| ApiError::StorageInit {
            message: format!("failed to read table_info row for {table_name}: {source}"),
        })?;
        actual.insert(name, column);
    }
    // table_info yields zero rows for a missing table.
    if actual.is_empty() {
        return Err(ApiError::StorageInit {
            message: format!(
                "fabric schema is missing required table {table_name}; run --setup-storage"
            ),
        });
    }
    for spec in expected {
        let Some(column) = actual.get(spec.name) else {
            return Err(ApiError::StorageInit {
                message: format!(
                    "fabric table {table_name} is missing required column {}",
                    spec.name
                ),
            });
        };
        if !column
            .declared_type
            .eq_ignore_ascii_case(spec.declared_type)
        {
            return Err(ApiError::StorageInit {
                message: format!(
                    "fabric column {table_name}.{} has type {}, expected {}",
                    spec.name, column.declared_type, spec.declared_type
                ),
            });
        }
        if spec.not_null && !column.not_null && !column.in_primary_key {
            return Err(ApiError::StorageInit {
                message: format!(
                    "fabric column {table_name}.{} is nullable, expected NOT NULL or PRIMARY KEY",
                    spec.name
                ),
            });
        }
    }
    for column_name in actual.keys() {
        if !expected.iter().any(|spec| spec.name == column_name) {
            return Err(ApiError::StorageInit {
                message: format!("fabric table {table_name} has unexpected column {column_name}"),
            });
        }
    }
    Ok(())
}

/// Validate one fabric virtual table by existence: it must appear in
/// sqlite_master as a `table` entry. FTS5 virtual tables report empty column
/// types through PRAGMA table_info, so they cannot go through the column
/// contract; a missing entry is a fatal setup error pointing at
/// `--setup-storage`, never a repair.
fn validate_fabric_virtual_table(
    connection: &Connection,
    table_name: &str,
) -> Result<(), ApiError> {
    let exists: bool = connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table_name],
            |_row| Ok(true),
        )
        .or_else(|source| match source {
            rusqlite::Error::QueryReturnedNoRows => Ok(false),
            other => Err(ApiError::StorageInit {
                message: format!("failed to inspect fabric virtual table {table_name}: {other}"),
            }),
        })?;
    if !exists {
        return Err(ApiError::StorageInit {
            message: format!(
                "fabric schema is missing required virtual table {table_name}; run --setup-storage"
            ),
        });
    }
    Ok(())
}
