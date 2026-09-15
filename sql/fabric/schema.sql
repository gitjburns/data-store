-- Fabric hot-plane schema (spec §32): the relational half of the D1 storage
-- layout, living at {index_root}/fabric/fabric.sqlite3 next to the
-- content-addressed artifact store. Rows here are envelopes and pipeline
-- state; heavy payloads (raw sources, parse bundles, projection payloads,
-- full QueryExecutionRecords) live in the artifact store and are referenced
-- by URI plus hash.
--
-- Conventions:
--   * Timestamps are RFC 3339 TEXT, matching the string timestamps of the
--     typed model in src/model (spec §16.2 canonical serialization).
--   * *_json columns hold canonical JSON (crate::canonical) for nested model
--     shapes where a full typed column set is not warranted.
--   * CHECK value sets on model-backed columns (source_locations.status,
--     acquisition_records.outcome, parse_runs.status) mirror the closed
--     snake_case Rust enums in src/model; a new enum variant requires a
--     coordinated schema version bump here. sync_queue.state mirrors the
--     `SyncQueueState` enum in src/model/sync.rs (defined against this CHECK
--     since C3c); its values come from spec §9.4.
--   * PRAGMA foreign_keys is per-connection state and is enabled by every
--     connection opener in src/hot_plane.rs, not in this DDL.
--
-- This file is applied only by the explicit setup path
-- (hot_plane::setup_fabric_storage) and is never re-applied as a migration.

-- Spec §10. One immutable content identity per source_hash; where the content
-- was seen lives in source_locations. active_parse_id names the single parse
-- considered production truth (§14); it is not a foreign key because
-- parse_runs itself references this table and activation updates the pointer
-- inside the cutover transaction (§31.1).
CREATE TABLE IF NOT EXISTS source_objects (
  id TEXT PRIMARY KEY,
  source_hash TEXT NOT NULL UNIQUE,
  active_parse_id TEXT,
  mime_type TEXT NOT NULL,
  size_bytes INTEGER,
  storage_uri TEXT NOT NULL,
  event_time TEXT,
  ingest_time TEXT NOT NULL,
  created_at TEXT NOT NULL,
  deactivated_at TEXT
);

-- Spec §10/§11. One place a SourceObject's content has been observed. A
-- (source_system, native_uri) pair points at one content identity at a time;
-- a rename or content change is one location ending and another beginning.
-- deletion_evidence_json is a canonical model::DeletionEvidence (§11.1);
-- metadata_json retains per-location descriptive metadata losslessly.
CREATE TABLE IF NOT EXISTS source_locations (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES source_objects(id),
  source_system TEXT NOT NULL,
  native_uri TEXT NOT NULL,
  native_id TEXT,
  governance_domain TEXT NOT NULL,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('current', 'deleted', 'access_lost')),
  deletion_evidence_json TEXT,
  metadata_json TEXT,
  UNIQUE (source_system, native_uri)
);

-- Spec §9.2. Durable record of every acquisition attempt, failed attempts
-- included. The source_* linkage columns are set only on the success path
-- (bundle validated, SourceObject/SourceLocation created or refreshed); they
-- are plain TEXT because failed and success rows are written before any
-- linkage exists and records outlive hot cleanup of their targets.
CREATE TABLE IF NOT EXISTS acquisition_records (
  id TEXT PRIMARY KEY,
  connector_name TEXT NOT NULL,
  connector_version TEXT NOT NULL,
  connector_config_hash TEXT NOT NULL,
  source_system TEXT NOT NULL,
  native_uri TEXT NOT NULL,
  native_id TEXT,
  native_version TEXT,
  native_modified_at TEXT,
  governance_domain TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('succeeded', 'failed')),
  failure_class TEXT,
  failure_detail TEXT,
  source_hash TEXT,
  source_object_id TEXT,
  source_location_id TEXT,
  acquired_at TEXT NOT NULL,
  elapsed_ms INTEGER
);

-- Spec §9.4. The one explicit, durable work queue driving acquisition →
-- parse → gate → activation. Pending work coalesces latest-state per source:
-- the UNIQUE source_key (connector-scoped source identity, composed by the
-- enqueuer from source_system plus native identity) holds at most one row
-- per pending source. On coalescing, detected_at advances to the latest
-- observation and coalesced_count increments (§9.5 health visibility);
-- created_at stays at the first detection of the currently-pending change so
-- per-source-system lag remains answerable (§9.4 rule 1).
CREATE TABLE IF NOT EXISTS sync_queue (
  id TEXT PRIMARY KEY,
  source_key TEXT NOT NULL UNIQUE,
  source_system TEXT NOT NULL,
  native_uri TEXT NOT NULL,
  detected_at TEXT NOT NULL,
  reason TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending', 'in_flight', 'failed')),
  attempt_count INTEGER NOT NULL,
  last_attempt_at TEXT,
  last_error TEXT,
  coalesced_count INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  -- Nullable §34.6 Operation link. Queue rows the autonomous scheduler
  -- enqueues on its own detection carry no Operation, so this is NULL for
  -- them; only HTTP-enqueued (queue-coupled) ingest/re-parse rows set it, so
  -- the drain can complete the paired Operation when the work finishes.
  operation_id TEXT
);

-- Spec §12. One parse attempt over a SourceObject; at most one active parse
-- per source (§14) and at most one held candidate (status 'ready' with
-- held_reason set, §13.4). conformance_report_json, warnings_json, and
-- metrics_json are canonical model::{ConformanceReport, Vec<ParseWarning>,
-- ParseMetrics}.
CREATE TABLE IF NOT EXISTS parse_runs (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES source_objects(id),
  parser_name TEXT NOT NULL,
  parser_version TEXT NOT NULL,
  parser_config_hash TEXT NOT NULL,
  capability_profile_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK (
    status IN ('building', 'ready', 'active', 'archiving', 'archived', 'failed')
  ),
  held_reason TEXT,
  conformance_report_json TEXT,
  started_at TEXT,
  completed_at TEXT,
  activated_at TEXT,
  archived_at TEXT,
  artifact_bundle_uri TEXT,
  artifact_bundle_hash TEXT,
  parser_raw_output_uri TEXT,
  warnings_json TEXT,
  metrics_json TEXT,
  error TEXT,
  created_at TEXT NOT NULL
);

-- Spec §15. Canonical ContentUnit envelopes for parses resident in the hot
-- plane. body_json is the canonical typed body (§18), validated against
-- content_type at creation (§15.2 hard gate in Rust); locators_json is a
-- canonical Vec<model::Locator>. Rows for superseded parses are removed by
-- archive-verify-delete hot cleanup (§31.2), never soft-deleted here.
CREATE TABLE IF NOT EXISTS content_units (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,
  parse_id TEXT NOT NULL,
  content_type TEXT NOT NULL,
  body_hash TEXT NOT NULL,
  text_hash TEXT,
  structure_hash TEXT,
  primary_parent_id TEXT,
  sequence_index INTEGER,
  locators_json TEXT,
  body_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

-- Reading-order and per-parse scans: activation, hot cleanup, and assembly
-- walk a parse's units in sequence (§25).
CREATE INDEX IF NOT EXISTS idx_content_units_parse_sequence
ON content_units(parse_id, sequence_index);

-- Spec §19. Canonical structural edges between ContentUnits of the same
-- source and parse; this graph is the canonical structure, with
-- content_units.primary_parent_id/sequence_index as convenience copies.
-- provenance_json is a canonical model::Provenance (§20).
CREATE TABLE IF NOT EXISTS unit_relationships (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,
  parse_id TEXT NOT NULL,
  from_unit_id TEXT NOT NULL,
  to_unit_id TEXT NOT NULL,
  relationship_type TEXT NOT NULL,
  relationship_role TEXT,
  sequence_index INTEGER,
  confidence REAL,
  provenance_json TEXT,
  created_at TEXT NOT NULL
);

-- Parse-scoped edge traversal from a unit in both directions: the §25 assembly
-- operators walk structural relationships out of and into an anchor unit within
-- a single active parse, so both lookups key on (parse_id, unit_id).
CREATE INDEX IF NOT EXISTS idx_unit_relationships_parse_from
ON unit_relationships(parse_id, from_unit_id);
CREATE INDEX IF NOT EXISTS idx_unit_relationships_parse_to
ON unit_relationships(parse_id, to_unit_id);

-- Spec §22. RetrievalProjection metadata envelopes; projection payloads and
-- index state live outside the hot plane (payload_uri into the artifact
-- store or a named index/partition). producer_json is a canonical
-- model::Provenance; input_unit_ids_json and input_annotation_ids_json are
-- canonical JSON string arrays — the latter is spec §22 inputAnnotationIds,
-- optional, set only by annotation-derived projections (C6d summary, C6f
-- graph) to link the envelope back to its SemanticAnnotation inputs.
-- Projections are rebuildable, so unlike canonical rows they carry
-- freshness_status and an explicit validity window plus deleted_at.
CREATE TABLE IF NOT EXISTS retrieval_projections (
  id TEXT PRIMARY KEY,
  source_id TEXT,
  parse_id TEXT,
  projection_type TEXT NOT NULL,
  input_unit_ids_json TEXT,
  input_annotation_ids_json TEXT,
  producer_json TEXT NOT NULL,
  index_name TEXT,
  index_partition TEXT,
  payload_uri TEXT,
  freshness_status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  valid_from TEXT,
  valid_to TEXT,
  deleted_at TEXT
);

-- Spec §28/§32. QER metadata only: the full immutable QueryExecutionRecord
-- (embedded EvidencePack included) is a compressed archive in the artifact
-- store at archive_uri/archive_hash; this row exists so audits can find and
-- verify it by query identity and time.
CREATE TABLE IF NOT EXISTS query_execution_records (
  id TEXT PRIMARY KEY,
  query_hash TEXT NOT NULL,
  executed_at TEXT NOT NULL,
  plan_hash TEXT NOT NULL,
  evidence_pack_hash TEXT NOT NULL,
  archive_uri TEXT NOT NULL,
  archive_hash TEXT NOT NULL,
  created_at TEXT NOT NULL
);

-- Spec §30/§32. ForensicSnapshot metadata only: the full manifest (the
-- ForensicSnapshotManifest listing every referenced content-addressed
-- artifact) is archived in the artifact store at manifest_uri/manifest_hash;
-- this row exists so audits and the lifecycle machinery can find and verify a
-- snapshot by identity, time, and subject without opening the manifest. Mirrors
-- the query_execution_records pattern (metadata row + archived heavy object).
--
-- snapshot_type holds the snake_case SnapshotType wire name (mirrors the closed
-- Rust enum in src/model/snapshot.rs). source_object_ids_json and
-- active_parse_ids_json are canonical JSON string arrays (the repo list-column
-- convention, e.g. target_unit_ids_json), the serialized ForensicSnapshot
-- sourceObjectIds/activeParseIds.
--
-- subject_source_id/subject_parse_id are the lifecycle-snapshot lookup keys: a
-- pre_activation/post_activation/pre_deactivation snapshot is per-source, so the
-- §31.2 deletion gate and §11.4 restore locate the right snapshot by
-- (subject_source_id, subject_parse_id, snapshot_type). They are NULLABLE
-- because corpus-wide snapshots (manual, incident, scheduled, pre_deployment)
-- have no single subject; a lifecycle snapshot always sets both.
CREATE TABLE IF NOT EXISTS forensic_snapshots (
  id TEXT PRIMARY KEY,
  snapshot_type TEXT NOT NULL,
  subject_source_id TEXT,
  subject_parse_id TEXT,
  source_object_ids_json TEXT NOT NULL,
  active_parse_ids_json TEXT NOT NULL,
  manifest_uri TEXT NOT NULL,
  manifest_hash TEXT NOT NULL,
  system_version TEXT NOT NULL,
  spec_version TEXT NOT NULL,
  created_at TEXT NOT NULL
);

-- Lifecycle-snapshot lookup: the deletion gate and restore resolve the subject
-- snapshot by (source, parse, type). Partial index over rows that carry a
-- subject so corpus-wide snapshots do not bloat it.
CREATE INDEX IF NOT EXISTS idx_forensic_snapshots_subject
ON forensic_snapshots(subject_source_id, subject_parse_id, snapshot_type)
WHERE subject_source_id IS NOT NULL;

-- Spec §34.6. One metadata row per asynchronous administrative Operation: the
-- durable status handle operators poll through GET /operations/{operationId}.
-- Mirrors the forensic_snapshots metadata-row pattern (a queryable header row,
-- no heavy payload). The Operation row IS the audit record for admin async
-- work — there is no operation.* SystemEvent in the §33 closed vocabulary.
--
-- operation_type holds the snake_case wire name mirroring the closed Rust enum
-- in src/model/operation.rs (like forensic_snapshots.snapshot_type). status is
-- CHECK-constrained to the closed set because it is a fixed lifecycle vocabulary
-- the transitions guard on; operation_type carries no CHECK (the Rust enum +
-- wire-name round-trip is the authority, matching forensic_snapshots.snapshot_type).
-- started_at/completed_at/error are nullable because a pending/running Operation
-- has not reached those milestones.
CREATE TABLE IF NOT EXISTS operations (
  id TEXT PRIMARY KEY,
  operation_type TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
  target_object_type TEXT NOT NULL,
  target_object_id TEXT NOT NULL,
  started_at TEXT,
  completed_at TEXT,
  error TEXT,
  created_at TEXT NOT NULL
);

-- Spec §21. SemanticAnnotation envelopes: derived, parse-scoped semantic
-- artifacts over one or more target ContentUnits, queryable only while their
-- parse is active (§21 rule 1) and rebuilt within the parse lifecycle rather
-- than migrated across parser versions. target_unit_ids_json is a canonical
-- JSON string array; annotation_type holds the snake_case
-- SemanticAnnotationType wire name (mirrors the closed Rust enum in
-- src/model/annotation.rs, extended only by a coordinated schema version
-- bump). body_json is NULL while freshness_status is 'building' or 'failed'
-- and holds the canonical typed body once fresh; provenance_json is a
-- canonical model::Provenance (§20), always present because the planned
-- producer identity is known before invocation. memoization_key_hash is the
-- §21.2 content key denormalized out of provenance so discovery and memo
-- lookup can scan it via an index. freshness_status makes post-activation
-- annotation builds visible truth rather than silent absence (§21 rule 3);
-- its CHECK set mirrors AnnotationFreshnessStatus.
CREATE TABLE IF NOT EXISTS semantic_annotations (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,
  parse_id TEXT NOT NULL,
  target_unit_ids_json TEXT NOT NULL,
  annotation_type TEXT NOT NULL,
  body_json TEXT,
  provenance_json TEXT NOT NULL,
  confidence REAL,
  freshness_status TEXT NOT NULL CHECK (
    freshness_status IN ('fresh', 'stale', 'building', 'failed')
  ),
  memoization_key_hash TEXT NOT NULL,
  -- CA2 content-scoped satisfaction key: annotation type crossed with the
  -- ordered target ContentUnit content hashes, so frontier application can
  -- decide satisfaction from target content identity alone. Distinct from
  -- memoization_key_hash, which also folds in producer identity. CA2-P1
  -- populates it; NOT NULL because every annotation row is content-scoped.
  content_key_hash TEXT NOT NULL,
  created_at TEXT NOT NULL,
  deleted_at TEXT
);

-- Per-parse annotation discovery and freshness scans (§21): the worker's
-- set-difference input and projection readers filter by (parse_id,
-- annotation_type).
CREATE INDEX IF NOT EXISTS idx_semantic_annotations_parse
ON semantic_annotations(parse_id, annotation_type);

-- Memoization lookup (§21.2): reuse discovery matches an annotation to its
-- content key through this index.
CREATE INDEX IF NOT EXISTS idx_semantic_annotations_memo_key
ON semantic_annotations(memoization_key_hash);

-- Content-scoped satisfaction lookup (CA2): frontier application within a parse
-- resolves whether an annotation type is already satisfied for a target content
-- key, so this keys on (parse_id, content_key_hash).
CREATE INDEX IF NOT EXISTS idx_semantic_annotations_content_key
ON semantic_annotations(parse_id, content_key_hash);

-- Spec §21.2. Content-keyed memoization cache: a producer output keyed by the
-- content-derived memoization_key_hash, so an identical (annotation_type,
-- producer_identity, target content) request reuses the prior result instead
-- of re-invoking the producer. This table DELIBERATELY SURVIVES parse
-- archival and hot cleanup — reuse ACROSS parses is its entire purpose, so
-- unlike semantic_annotations rows it is never removed when a parse is
-- superseded. original_annotation_id is the memoizedFrom target recorded in
-- reusing rows' provenance; it is plain TEXT, not a foreign key, because a
-- cache row outlives the semantic_annotations row it originated from.
CREATE TABLE IF NOT EXISTS annotation_memo (
  memoization_key_hash TEXT PRIMARY KEY,
  annotation_type TEXT NOT NULL,
  producer_identity_hash TEXT NOT NULL,
  body_json TEXT NOT NULL,
  confidence REAL,
  original_annotation_id TEXT NOT NULL,
  created_at TEXT NOT NULL
);

-- Spec §33. Durable pipeline/audit events (model::SystemEvent); event_type
-- is the closed dotted set enforced by the Rust enum. payload_json is a
-- canonical JSON object.
CREATE TABLE IF NOT EXISTS system_events (
  id TEXT PRIMARY KEY,
  event_type TEXT NOT NULL,
  object_type TEXT NOT NULL,
  object_id TEXT NOT NULL,
  payload_json TEXT,
  created_at TEXT NOT NULL
);

-- The event log is read as an ordered audit trail.
CREATE INDEX IF NOT EXISTS idx_system_events_created_at
ON system_events(created_at);

-- CA2 policy substrate. One row per observed content of an operator-editable
-- policy document (entity_match, annotator_naming): version is a SYSTEM-ASSIGNED
-- change-event counter, not an operator-authored field. content_hash is the
-- SHA-256 over the PARSED document's canonical serialization (crate::canonical),
-- so whitespace/comment-only edits do not append a version. observed_at is when
-- registration first saw this content.
--
-- APPEND-ONLY by convention: rows are only ever INSERTed. No UPDATE or DELETE is
-- ever issued against this table. A revert to previously seen content still
-- appends a NEW version (the counter records change events, not distinct
-- contents), so history is a faithful timeline of when each content took effect.
CREATE TABLE IF NOT EXISTS policy_versions (
  policy_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  content_hash TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  PRIMARY KEY (policy_id, version)
);

-- Spec §22–§23 (C6 retrieval projections). The typed projection payloads
-- below are parse-scoped and rebuildable, mirroring the content_units /
-- semantic_annotations conventions: (source_id, parse_id) are plain TEXT
-- scope columns (not foreign keys — payload rows are hot cleanup targets on
-- parse supersession, §31.2, and their freshness envelope already lives on
-- retrieval_projections). Every payload row references its
-- retrieval_projections envelope through projection_id so freshness_status
-- and the §22 validity window stay single-sourced on the envelope; the
-- reference is plain TEXT (not a foreign key) because a payload may be
-- rebuilt and re-linked to a fresh envelope within one parse, and because
-- hot cleanup deletes envelopes and payloads independently.

-- Spec §22 ChunkPayload; PLAN-grains Section 2 fine grain. One chunk
-- projection: a retrieval targeting artifact over a run of evidence members
-- from one or more canonical ContentUnits (§23 rule 2), never evidence.
-- input_unit_ids_json is a canonical JSON string array of the ContentUnit IDs
-- the chunk targets; targeting_text is the chunk's canonical text (member
-- texts joined by one blank line), indexed as-is by the lexical channel and
-- embedded behind the section-path prefix by the dense channel.
-- fragments_json is the canonical JSON array of { unitId, startChar, endChar }
-- membership records in Unicode scalar offsets over each unit's evidence
-- text, end exclusive (never byte offsets); section_path_json is the
-- canonical JSON string array of the first member's section path, used only
-- for the model-input prefix. token_count is nullable because it is measured
-- by the caller-supplied tokenizer and may be absent (§22
-- ChunkPayload.tokenCount is optional). The chunker_* columns pin the
-- producing chunker identity and its config hash so a chunk's provenance and
-- rebuild determinism are answerable from the row. chunk_index is the
-- chunk's 0-based position in reading order within its parse, assigned by the
-- chunker at insert; it is the ONLY reading-order authority for chunks (ids
-- and created_at carry no order), and every higher grain that packs runs of
-- consecutive chunks orders by it.
CREATE TABLE IF NOT EXISTS chunk_projections (
  id TEXT PRIMARY KEY,
  projection_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  parse_id TEXT NOT NULL,
  input_unit_ids_json TEXT NOT NULL,
  targeting_text TEXT NOT NULL,
  token_count INTEGER,
  chunker_name TEXT NOT NULL,
  chunker_version TEXT NOT NULL,
  chunker_config_hash TEXT NOT NULL,
  created_at TEXT NOT NULL,
  fragments_json TEXT NOT NULL,
  section_path_json TEXT NOT NULL,
  chunk_index INTEGER NOT NULL
);

-- Per-parse chunk scans: the lexical/dense builders and hot cleanup walk a
-- parse's chunks, and chunk-hit resolution maps a chunk back to its parse.
CREATE INDEX IF NOT EXISTS idx_chunk_projections_parse
ON chunk_projections(parse_id);

-- Reading order is total within a parse: one chunk per position, and ordered
-- per-parse reads (section windows, ColBERT windows) walk this index.
CREATE UNIQUE INDEX IF NOT EXISTS idx_chunk_projections_parse_order
ON chunk_projections(parse_id, chunk_index);

-- Spec §22/§36. FTS5 lexical index over chunk targeting text — the C6a
-- lexical channel's candidate generation surface. chunk_id is UNINDEXED
-- (stored, not tokenized) so a full-text MATCH returns the owning
-- chunk_projections.id without a join; targeting_text is the sole tokenized
-- column. This is a standalone FTS5 table the lexical builder populates and
-- rebuilds from chunk_projections, not an external-content mirror: the
-- projection identity is the TEXT chunk id, which does not map onto FTS5's
-- integer rowid contract. Virtual tables take no CHECK/foreign-key clauses,
-- so the schema-validator battery checks this table by existence, not by the
-- column contract used for ordinary tables (FTS5 reports empty column types).
CREATE VIRTUAL TABLE IF NOT EXISTS chunk_text_index USING fts5(
  chunk_id UNINDEXED,
  targeting_text
);

-- Spec §8.3/§22 dense_vector projection, per chunk. The dense retrieval
-- channel shares chunk targeting granularity (C6 design), so one dense vector
-- persists per chunk_projections row. dimension and norm are stored
-- alongside the little-endian f32 vector_blob so the C6c decoder can validate
-- length and reuse the precomputed L2 norm for cosine scoring without
-- rescanning the blob (mirrors the legacy StoredDenseVector contract in
-- src/primitives/{validate,codec}.rs).
CREATE TABLE IF NOT EXISTS chunk_dense_vectors (
  chunk_id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,
  parse_id TEXT NOT NULL,
  dimension INTEGER NOT NULL,
  norm REAL NOT NULL,
  vector_blob BLOB NOT NULL,
  created_at TEXT NOT NULL
);

-- Per-parse dense scans and hot cleanup over a parse's chunk vectors.
CREATE INDEX IF NOT EXISTS idx_chunk_dense_vectors_parse
ON chunk_dense_vectors(parse_id);

-- Spec §22 multi_vector projection at the ColBERT grain (PLAN-grains.md
-- Section 2): one ColBERT token matrix per WINDOW, a run of consecutive fine
-- chunks packed under indexing.colbert_max_tokens in chunk_index order.
-- window_index is the window's 0-based reading-order position within the
-- parse; chunk_ids_json is the canonical JSON array of member chunk ids in
-- order; fragments_json is the concatenation of the members' fragments
-- ({unitId, startChar, endChar} in Unicode scalar offsets). token_count and
-- dimension bound the row-major f32 matrix_blob (token_count * dimension
-- values), letting the decoder validate the matrix without a separate shape
-- record (mirrors UnitColbertDocumentVector / StoredColbertDocumentVector in
-- src/primitives/{codec,validate}.rs); token_count is the matrix row count.
-- projection_id links the shared retrieval_projections envelope.
CREATE TABLE IF NOT EXISTS colbert_windows (
  id TEXT PRIMARY KEY,
  projection_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  parse_id TEXT NOT NULL,
  window_index INTEGER NOT NULL,
  chunk_ids_json TEXT NOT NULL,
  fragments_json TEXT NOT NULL,
  token_count INTEGER NOT NULL,
  dimension INTEGER NOT NULL,
  matrix_blob BLOB NOT NULL,
  created_at TEXT NOT NULL
);

-- One window per (parse, reading-order position).
CREATE UNIQUE INDEX IF NOT EXISTS idx_colbert_windows_parse_window
ON colbert_windows(parse_id, window_index);

-- Per-parse scan key for the builder's delete-first rebuild, the query-side
-- membership lookups, and hot cleanup.
CREATE INDEX IF NOT EXISTS idx_colbert_windows_parse
ON colbert_windows(parse_id);

-- Spec §22 graph_projection, D9 resolution: entity mentions keyed by
-- normalized entity name. Entity node identity IS the normalized name
-- (normalized_name); entity_type is node metadata, not identity (D9). One row
-- per (parse, normalized entity name): unit_ids_json is the canonical JSON
-- string array of the ContentUnit IDs the CA entity annotation targets. The
-- query-time graph channel (C7b) does a lexical match of query text against
-- normalized_name at candidate generation, then reads unit_ids_json — no LLM
-- call, no materialized closures (D9). Parse-scoped and rebuilt from CA
-- entity annotations, so these rows are hot cleanup targets on supersession.
CREATE TABLE IF NOT EXISTS graph_entity_mentions (
  id TEXT PRIMARY KEY,
  projection_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  parse_id TEXT NOT NULL,
  normalized_name TEXT NOT NULL,
  entity_type TEXT,
  unit_ids_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

-- Entity lookup at query time (D9 entry) and per-parse rebuild/cleanup: the
-- graph channel matches query text against normalized_name within a parse.
CREATE INDEX IF NOT EXISTS idx_graph_entity_mentions_parse_name
ON graph_entity_mentions(parse_id, normalized_name);

-- Spec §22 graph_projection, D9 resolution: entity-to-entity relation edges.
-- One row per CA relation annotation, keyed by the normalized name pair
-- (from_normalized_name, to_normalized_name) — both are entity-node
-- identities, matching graph_entity_mentions.normalized_name. relation_type
-- carries the CA relation annotation's semantic type. target_unit_ids_json is
-- the canonical JSON string array of the ContentUnit IDs the relation
-- annotation targets (the edge's supporting units). D9 traversal is
-- semantic-only and one relational hop: structural UnitRelationships are never
-- walked at query time. Parse-scoped; rebuilt from CA relation annotations.
CREATE TABLE IF NOT EXISTS graph_entity_edges (
  id TEXT PRIMARY KEY,
  projection_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  parse_id TEXT NOT NULL,
  from_normalized_name TEXT NOT NULL,
  to_normalized_name TEXT NOT NULL,
  relation_type TEXT NOT NULL,
  target_unit_ids_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

-- One-hop traversal from a matched entity (D9 edges): the graph channel finds
-- edges touching a normalized entity name within a parse, in both directions.
CREATE INDEX IF NOT EXISTS idx_graph_entity_edges_parse_from
ON graph_entity_edges(parse_id, from_normalized_name);
CREATE INDEX IF NOT EXISTS idx_graph_entity_edges_parse_to
ON graph_entity_edges(parse_id, to_normalized_name);

-- Schema revision stamp compared against hot_plane::FABRIC_SCHEMA_VERSION.
-- Every change to this file bumps it (and the Rust constant); --setup-storage
-- recreates any existing database whose stamp differs.
PRAGMA user_version = 2;
