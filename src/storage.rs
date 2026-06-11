use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, params, params_from_iter, types::Value};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tracing::{error, info, warn};

use crate::{
    config::{ColbertModelConfig, DenseModelConfig, RetrievalConfig, StorageConfig},
    docling::DoclingConversionResult,
    error::ApiError,
    units::{RetrievalUnit, build_document_id},
};

#[derive(Debug, Clone)]
pub struct StorageRuntime {
    db_path: PathBuf,
    dense_dimension: usize,
    colbert_dimension: usize,
    cache: Arc<Mutex<DenseVectorCache>>,
}

#[derive(Debug, Clone)]
pub struct UnitDenseVector {
    pub unit_id: String,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone)]
pub struct UnitColbertDocumentVector {
    pub unit_id: String,
    pub token_count: usize,
    pub dimension: usize,
    pub vector: Vec<f32>,
}

#[derive(Debug)]
pub struct SearchCandidatePoolOutput {
    pub candidates: Vec<SearchCandidate>,
    pub raw: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct SearchSnapshot {
    cache: DenseVectorCache,
}

#[derive(Debug, Clone)]
pub struct SearchCandidate {
    pub unit_id: String,
    pub rrf_score: f64,
    pub rrf_rank: usize,
    pub dense_rank: Option<usize>,
    pub dense_similarity: Option<f32>,
    pub bm25_rank: Option<usize>,
    pub bm25_score: Option<f64>,
    pub content: String,
    pub heading_path: Vec<String>,
    pub source_path: String,
    pub page_numbers: Vec<u32>,
    pub colbert_token_count: usize,
    pub colbert_dimension: usize,
    pub colbert_vector: Vec<f32>,
}

#[derive(Debug, Serialize)]
pub struct IngestedSourceListing {
    pub sources: Vec<IngestedSourceRecord>,
}

#[derive(Debug, Serialize)]
pub struct IngestedSourceRecord {
    #[serde(rename = "sourcePath")]
    pub source_path: String,

    #[serde(rename = "activeVersionLabel")]
    pub active_version_label: String,

    #[serde(rename = "documentId")]
    pub document_id: String,

    #[serde(rename = "unitsIngested")]
    pub units_ingested: u32,

    pub status: String,

    #[serde(rename = "createdAtMs")]
    pub created_at_ms: u64,

    #[serde(rename = "updatedAtMs")]
    pub updated_at_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct DocumentVersionListing {
    pub sources: Vec<SourceDocumentVersionListing>,
}

#[derive(Debug, Serialize)]
pub struct SourceDocumentVersionListing {
    #[serde(rename = "sourcePath")]
    pub source_path: String,

    #[serde(rename = "activeVersionLabel")]
    pub active_version_label: Option<String>,

    pub versions: Vec<DocumentVersionRecord>,
}

#[derive(Debug, Serialize)]
pub struct DocumentVersionRecord {
    #[serde(rename = "versionLabel")]
    pub version_label: String,

    #[serde(rename = "documentId")]
    pub document_id: String,

    #[serde(rename = "isActive")]
    pub is_active: bool,

    #[serde(rename = "sourceSha256")]
    pub source_sha256: Option<String>,

    #[serde(rename = "markdownPath")]
    pub markdown_path: String,

    #[serde(rename = "markdownSha256")]
    pub markdown_sha256: Option<String>,

    #[serde(rename = "pdfBackend")]
    pub pdf_backend: String,

    #[serde(rename = "ocrMode")]
    pub ocr_mode: String,

    #[serde(rename = "pageBatchSize")]
    pub page_batch_size: Option<u32>,

    #[serde(rename = "unitsIngested")]
    pub units_ingested: u32,

    pub status: String,
    pub diagnostics: serde_json::Value,

    #[serde(rename = "createdAtMs")]
    pub created_at_ms: u64,

    #[serde(rename = "updatedAtMs")]
    pub updated_at_ms: u64,

    #[serde(rename = "denseVectorMetadata")]
    pub dense_vector_metadata: Vec<DenseVectorMetadataRecord>,

    #[serde(rename = "colbertVectorMetadata")]
    pub colbert_vector_metadata: Vec<ColbertVectorMetadataRecord>,
}

#[derive(Debug, Serialize)]
pub struct DenseVectorMetadataRecord {
    #[serde(rename = "modelPath")]
    pub model_path: String,

    #[serde(rename = "modelDimension")]
    pub model_dimension: u32,

    pub pooling: String,
    pub format: String,

    #[serde(rename = "vectorCount")]
    pub vector_count: u32,
}

#[derive(Debug, Serialize)]
pub struct ColbertVectorMetadataRecord {
    #[serde(rename = "modelPath")]
    pub model_path: String,

    #[serde(rename = "modelDimension")]
    pub model_dimension: u32,

    pub format: String,

    #[serde(rename = "vectorCount")]
    pub vector_count: u32,
}

#[derive(Debug)]
pub struct DocumentVersionRollbackResult {
    pub source_path: String,
    pub active_version_label: String,
    pub published_at_ms: u64,
    pub vector_count: usize,
}

#[derive(Debug, Clone)]
struct DenseVectorCache {
    dimension: usize,
    vectors: Vec<f32>,
    unit_ids: Vec<String>,
    source_paths: Vec<String>,
    version_labels: Vec<String>,
    norms: Vec<f32>,
    active_versions: Vec<ActiveDocumentVersion>,
    loaded_at_ms: u64,
    load_duration_ms: u64,
    memory_bytes: usize,
}

#[derive(Debug, Clone)]
struct ActiveDocumentVersion {
    source_path: String,
    version_label: String,
}

#[derive(Debug, Clone)]
struct DenseCacheVector {
    source_path: String,
    version_label: String,
    dense: StoredDenseVector,
}

#[derive(Debug, Clone)]
struct StoredDenseVector {
    unit_id: String,
    vector: Vec<f32>,
    norm: f32,
}

#[derive(Debug, Clone)]
struct StoredColbertDocumentVector {
    unit_id: String,
    token_count: usize,
    dimension: usize,
    vector: Vec<f32>,
}

#[derive(Debug)]
struct QueriedIngestedSource {
    source_path: String,
    active_version_label: String,
    document_id: String,
    units_ingested: i64,
    status: String,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Debug)]
struct QueriedDocumentVersion {
    source_path: String,
    version_label: String,
    document_id: String,
    source_sha256: Option<String>,
    markdown_path: String,
    markdown_sha256: Option<String>,
    pdf_backend: String,
    ocr_mode: String,
    page_batch_size: Option<i64>,
    units_ingested: i64,
    status: String,
    diagnostics_json: String,
    created_at_ms: i64,
    updated_at_ms: i64,
    is_active: bool,
}

#[derive(Debug)]
struct DenseMatch {
    unit_id: String,
    similarity: f32,
    rank: usize,
}

#[derive(Debug)]
struct Bm25Match {
    unit_id: String,
    score: f64,
    rank: usize,
}

#[derive(Debug)]
struct FusedMatch {
    unit_id: String,
    score: f64,
    rank: usize,
    dense_rank: Option<usize>,
    dense_similarity: Option<f32>,
    bm25_rank: Option<usize>,
    bm25_score: Option<f64>,
}

#[derive(Debug)]
struct StoredUnit {
    unit_id: String,
    source_path: String,
    heading_path: Vec<String>,
    page_numbers: Vec<u32>,
    content: String,
    colbert_token_count: usize,
    colbert_dimension: usize,
    colbert_vector: Vec<f32>,
}

struct SearchRawInput<'a> {
    cache: &'a DenseVectorCache,
    query_vector: &'a StoredDenseVector,
    dense_matches: &'a [DenseMatch],
    bm25_matches: &'a [Bm25Match],
    fused_matches: &'a [FusedMatch],
    bm25_query: Option<&'a str>,
    candidate_limit: usize,
    colbert_candidate_pool_size: u32,
    top_k: u32,
    rrf_k: u32,
    overfetch_multiplier: u32,
    query_vector_validation_latency_ms: u64,
    dense_latency_ms: u64,
    bm25_latency_ms: u64,
    rrf_fusion_latency_ms: u64,
    candidate_materialization_latency_ms: u64,
    latency_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct ColumnSpec {
    table_name: &'static str,
    name: &'static str,
    declared_type: &'static str,
    required: bool,
    primary_key_position: i64,
}

#[derive(Debug)]
struct ColumnInfo {
    name: String,
    declared_type: String,
    not_null: bool,
    primary_key_position: i64,
}

#[derive(Debug, Clone, Copy)]
struct ForeignKeySpec {
    table_name: &'static str,
    from_column: &'static str,
    referenced_table: &'static str,
    referenced_column: &'static str,
    on_delete: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct CompositeForeignKeySpec {
    table_name: &'static str,
    from_columns: &'static [&'static str],
    referenced_table: &'static str,
    referenced_columns: &'static [&'static str],
}

const DATABASE_FILE_NAME: &str = "data-store.sqlite3";
const EXPECTED_SCHEMA_VERSION: i64 = 3;
const DENSE_VECTOR_FORMAT: &str = "little_endian_f32";
const COLBERT_DOCUMENT_VECTOR_FORMAT: &str = "little_endian_f32_row_major";
const DOCUMENT_STATUS_INGESTED: &str = "ingested";
const RETRIEVAL_MODE_DENSE_BM25_RRF_POOL: &str = "dense_bm25_rrf_candidate_pool";
const F32_BYTE_WIDTH: usize = std::mem::size_of::<f32>();
const STORAGE_SCHEMA_SQL: &str = include_str!("../sql/schema.sql");
const ENABLE_FOREIGN_KEYS_SQL: &str = "PRAGMA foreign_keys = ON;";
const GET_SCHEMA_VERSION_SQL: &str = "PRAGMA user_version;";
const BM25_SEARCH_SQL_PREFIX: &str = "
SELECT units.unit_id, bm25(units_fts) AS bm25_score
FROM units_fts
JOIN units ON units.rowid = units_fts.rowid
WHERE units_fts MATCH ?
  AND (";
const BM25_SEARCH_SQL_SUFFIX: &str = ")
ORDER BY bm25_score ASC, units.unit_id ASC
LIMIT ?";
const LOAD_DENSE_VECTORS_SQL: &str = "
SELECT
  dense_vectors.unit_id,
  dense_vectors.dimension,
  dense_vectors.vector_blob,
  dense_vectors.vector_norm,
  units.source_path,
  units.version_label
FROM dense_vectors
JOIN units ON units.unit_id = dense_vectors.unit_id
JOIN active_document_versions AS active
  ON active.source_path = units.source_path
 AND active.version_label = units.version_label
ORDER BY dense_vectors.unit_id ASC";
const LOAD_ACTIVE_DOCUMENT_VERSIONS_SQL: &str = "
SELECT source_path, version_label
FROM active_document_versions
ORDER BY source_path ASC";
const LIST_INGESTED_SOURCES_SQL: &str = "
SELECT
  document_versions.source_path,
  active_document_versions.version_label,
  document_versions.document_id,
  document_versions.units_ingested,
  document_versions.status,
  document_versions.created_at_ms,
  document_versions.updated_at_ms
FROM active_document_versions
JOIN document_versions
  ON document_versions.source_path = active_document_versions.source_path
 AND document_versions.version_label = active_document_versions.version_label
ORDER BY document_versions.source_path ASC";
const LIST_DOCUMENT_VERSIONS_SQL: &str = "
SELECT
  document_versions.source_path,
  document_versions.version_label,
  document_versions.document_id,
  document_versions.source_sha256,
  document_versions.markdown_path,
  document_versions.markdown_sha256,
  document_versions.pdf_backend,
  document_versions.ocr_mode,
  document_versions.page_batch_size,
  document_versions.units_ingested,
  document_versions.status,
  document_versions.diagnostics_json,
  document_versions.created_at_ms,
  document_versions.updated_at_ms,
  active_document_versions.version_label IS NOT NULL AS is_active
FROM document_versions
LEFT JOIN active_document_versions
  ON active_document_versions.source_path = document_versions.source_path
 AND active_document_versions.version_label = document_versions.version_label
ORDER BY document_versions.source_path ASC, document_versions.created_at_ms DESC, document_versions.version_label DESC";
const DOCUMENT_VERSION_EXISTS_SQL: &str = "
SELECT 1
FROM document_versions
WHERE source_path = ?1
  AND version_label = ?2
LIMIT 1";
const LOAD_DENSE_VECTORS_FOR_VERSION_SQL: &str = "
SELECT
  dense_vectors.unit_id,
  dense_vectors.dimension,
  dense_vectors.vector_blob,
  dense_vectors.vector_norm
FROM dense_vectors
JOIN units ON units.unit_id = dense_vectors.unit_id
WHERE units.source_path = ?1
  AND units.version_label = ?2
ORDER BY dense_vectors.unit_id ASC";
const LOAD_DENSE_VECTOR_METADATA_FOR_VERSION_SQL: &str = "
SELECT dense_vectors.model_path, dense_vectors.model_dimension, dense_vectors.pooling,
       dense_vectors.format, COUNT(*)
FROM dense_vectors
JOIN units ON units.unit_id = dense_vectors.unit_id
WHERE units.source_path = ?1
  AND units.version_label = ?2
GROUP BY dense_vectors.model_path, dense_vectors.model_dimension, dense_vectors.pooling,
         dense_vectors.format
ORDER BY dense_vectors.model_path ASC, dense_vectors.model_dimension ASC,
         dense_vectors.pooling ASC, dense_vectors.format ASC";
const LOAD_COLBERT_VECTOR_METADATA_FOR_VERSION_SQL: &str = "
SELECT colbert_document_vectors.model_path, colbert_document_vectors.model_dimension,
       colbert_document_vectors.format, COUNT(*)
FROM colbert_document_vectors
JOIN units ON units.unit_id = colbert_document_vectors.unit_id
WHERE units.source_path = ?1
  AND units.version_label = ?2
GROUP BY colbert_document_vectors.model_path, colbert_document_vectors.model_dimension,
         colbert_document_vectors.format
ORDER BY colbert_document_vectors.model_path ASC, colbert_document_vectors.model_dimension ASC,
         colbert_document_vectors.format ASC";
const SCHEMA_OBJECT_EXISTS_SQL: &str = "
SELECT 1
FROM sqlite_master
WHERE name = ?1
LIMIT 1";
const SCHEMA_OBJECT_SQL_SQL: &str = "
SELECT sql
FROM sqlite_master
WHERE name = ?1
LIMIT 1";
const DOCUMENT_VERSIONS_TABLE_INFO_SQL: &str = "PRAGMA table_info(document_versions);";
const ACTIVE_DOCUMENT_VERSIONS_TABLE_INFO_SQL: &str =
    "PRAGMA table_info(active_document_versions);";
const UNITS_TABLE_INFO_SQL: &str = "PRAGMA table_info(units);";
const DENSE_VECTORS_TABLE_INFO_SQL: &str = "PRAGMA table_info(dense_vectors);";
const COLBERT_DOCUMENT_VECTORS_TABLE_INFO_SQL: &str =
    "PRAGMA table_info(colbert_document_vectors);";
const ACTIVE_DOCUMENT_VERSIONS_FOREIGN_KEYS_SQL: &str =
    "PRAGMA foreign_key_list(active_document_versions);";
const UNITS_FOREIGN_KEYS_SQL: &str = "PRAGMA foreign_key_list(units);";
const DENSE_VECTORS_FOREIGN_KEYS_SQL: &str = "PRAGMA foreign_key_list(dense_vectors);";
const COLBERT_DOCUMENT_VECTORS_FOREIGN_KEYS_SQL: &str =
    "PRAGMA foreign_key_list(colbert_document_vectors);";
const DOCUMENT_VERSIONS_INDEX_LIST_SQL: &str = "PRAGMA index_list(document_versions);";
const UNITS_INDEX_LIST_SQL: &str = "PRAGMA index_list(units);";
const DOCUMENT_VERSIONS_DOCUMENT_ID_INDEX_INFO_SQL: &str =
    "PRAGMA index_info(idx_document_versions_document_id);";
const DOCUMENT_VERSIONS_SOURCE_VERSION_INDEX_INFO_SQL: &str =
    "PRAGMA index_info(idx_document_versions_source_version);";
const UNITS_DOCUMENT_SEQUENCE_INDEX_INFO_SQL: &str =
    "PRAGMA index_info(idx_units_document_sequence);";
const LOAD_UNIT_SQL: &str = "
SELECT unit_id, source_path, heading_path_json, page_numbers_json, content
FROM units
WHERE unit_id = ?1";
const INSERT_DOCUMENT_SQL: &str = "
INSERT INTO document_versions (
  source_path, version_label, document_id, source_sha256, markdown_path, markdown_sha256,
  pdf_backend, ocr_mode, page_batch_size, units_ingested, status,
  diagnostics_json, created_at_ms, updated_at_ms
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)";
const INSERT_UNIT_SQL: &str = "
INSERT INTO units (
  unit_id, document_id, source_path, version_label, sequence,
  heading_path_json, page_numbers_json, token_count, content, content_chars
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)";
const INSERT_UNIT_FTS_SQL: &str = "INSERT INTO units_fts(rowid, content) VALUES (?1, ?2)";
const INSERT_DENSE_VECTOR_SQL: &str = "
INSERT INTO dense_vectors (
  unit_id, dimension, vector_blob, vector_norm, model_path,
  model_dimension, pooling, format, created_at_ms, updated_at_ms
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)";
const INSERT_COLBERT_DOCUMENT_VECTOR_SQL: &str = "
INSERT INTO colbert_document_vectors (
  unit_id, token_count, dimension, vector_blob, model_path,
  model_dimension, format, created_at_ms, updated_at_ms
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)";
const LOAD_COLBERT_DOCUMENT_VECTOR_SQL: &str = "
SELECT token_count, dimension, vector_blob, model_dimension, format
FROM colbert_document_vectors
WHERE unit_id = ?1";
const PUBLISH_ACTIVE_DOCUMENT_VERSION_SQL: &str = "
INSERT INTO active_document_versions (source_path, version_label, published_at_ms)
VALUES (?1, ?2, ?3)
ON CONFLICT(source_path) DO UPDATE SET
  version_label = excluded.version_label,
  published_at_ms = excluded.published_at_ms";
const EXPECTED_UNITS_FTS_SQL: &str =
    "CREATE VIRTUAL TABLE units_fts USING fts5(content, content='units', content_rowid='rowid')";
const DOCUMENT_VERSIONS_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        table_name: "document_versions",
        name: "source_path",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 1,
    },
    ColumnSpec {
        table_name: "document_versions",
        name: "version_label",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 2,
    },
    ColumnSpec {
        table_name: "document_versions",
        name: "document_id",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "document_versions",
        name: "source_sha256",
        declared_type: "TEXT",
        required: false,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "document_versions",
        name: "markdown_path",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "document_versions",
        name: "markdown_sha256",
        declared_type: "TEXT",
        required: false,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "document_versions",
        name: "pdf_backend",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "document_versions",
        name: "ocr_mode",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "document_versions",
        name: "page_batch_size",
        declared_type: "INTEGER",
        required: false,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "document_versions",
        name: "units_ingested",
        declared_type: "INTEGER",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "document_versions",
        name: "status",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "document_versions",
        name: "diagnostics_json",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "document_versions",
        name: "created_at_ms",
        declared_type: "INTEGER",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "document_versions",
        name: "updated_at_ms",
        declared_type: "INTEGER",
        required: true,
        primary_key_position: 0,
    },
];
const ACTIVE_DOCUMENT_VERSIONS_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        table_name: "active_document_versions",
        name: "source_path",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 1,
    },
    ColumnSpec {
        table_name: "active_document_versions",
        name: "version_label",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "active_document_versions",
        name: "published_at_ms",
        declared_type: "INTEGER",
        required: true,
        primary_key_position: 0,
    },
];
const UNITS_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        table_name: "units",
        name: "unit_id",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 1,
    },
    ColumnSpec {
        table_name: "units",
        name: "document_id",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "units",
        name: "source_path",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "units",
        name: "version_label",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "units",
        name: "sequence",
        declared_type: "INTEGER",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "units",
        name: "heading_path_json",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "units",
        name: "page_numbers_json",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "units",
        name: "token_count",
        declared_type: "INTEGER",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "units",
        name: "content",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "units",
        name: "content_chars",
        declared_type: "INTEGER",
        required: true,
        primary_key_position: 0,
    },
];
const DENSE_VECTORS_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        table_name: "dense_vectors",
        name: "unit_id",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 1,
    },
    ColumnSpec {
        table_name: "dense_vectors",
        name: "dimension",
        declared_type: "INTEGER",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "dense_vectors",
        name: "vector_blob",
        declared_type: "BLOB",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "dense_vectors",
        name: "vector_norm",
        declared_type: "REAL",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "dense_vectors",
        name: "model_path",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "dense_vectors",
        name: "model_dimension",
        declared_type: "INTEGER",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "dense_vectors",
        name: "pooling",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "dense_vectors",
        name: "format",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "dense_vectors",
        name: "created_at_ms",
        declared_type: "INTEGER",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "dense_vectors",
        name: "updated_at_ms",
        declared_type: "INTEGER",
        required: true,
        primary_key_position: 0,
    },
];
const COLBERT_DOCUMENT_VECTORS_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        table_name: "colbert_document_vectors",
        name: "unit_id",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 1,
    },
    ColumnSpec {
        table_name: "colbert_document_vectors",
        name: "token_count",
        declared_type: "INTEGER",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "colbert_document_vectors",
        name: "dimension",
        declared_type: "INTEGER",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "colbert_document_vectors",
        name: "vector_blob",
        declared_type: "BLOB",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "colbert_document_vectors",
        name: "model_path",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "colbert_document_vectors",
        name: "model_dimension",
        declared_type: "INTEGER",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "colbert_document_vectors",
        name: "format",
        declared_type: "TEXT",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "colbert_document_vectors",
        name: "created_at_ms",
        declared_type: "INTEGER",
        required: true,
        primary_key_position: 0,
    },
    ColumnSpec {
        table_name: "colbert_document_vectors",
        name: "updated_at_ms",
        declared_type: "INTEGER",
        required: true,
        primary_key_position: 0,
    },
];
const UNITS_DOCUMENT_FOREIGN_KEY: ForeignKeySpec = ForeignKeySpec {
    table_name: "units",
    from_column: "document_id",
    referenced_table: "document_versions",
    referenced_column: "document_id",
    on_delete: "CASCADE",
};
const ACTIVE_DOCUMENT_VERSION_FOREIGN_KEY: CompositeForeignKeySpec = CompositeForeignKeySpec {
    table_name: "active_document_versions",
    from_columns: &["source_path", "version_label"],
    referenced_table: "document_versions",
    referenced_columns: &["source_path", "version_label"],
};
const DENSE_VECTORS_UNIT_FOREIGN_KEY: ForeignKeySpec = ForeignKeySpec {
    table_name: "dense_vectors",
    from_column: "unit_id",
    referenced_table: "units",
    referenced_column: "unit_id",
    on_delete: "CASCADE",
};
const COLBERT_DOCUMENT_VECTORS_UNIT_FOREIGN_KEY: ForeignKeySpec = ForeignKeySpec {
    table_name: "colbert_document_vectors",
    from_column: "unit_id",
    referenced_table: "units",
    referenced_column: "unit_id",
    on_delete: "CASCADE",
};

impl StorageRuntime {
    /// Open existing storage, validate schema, and load the dense vector cache without runtime schema changes.
    pub fn open(
        storage: &StorageConfig,
        dense: &DenseModelConfig,
        colbert: &ColbertModelConfig,
    ) -> Result<Self, ApiError> {
        let db_path = database_path(storage);
        if !db_path.is_file() {
            return Err(ApiError::StorageInit {
                message: format!(
                    "SQLite database is missing at {}; run --setup-storage first",
                    db_path.display()
                ),
            });
        }

        let connection = open_connection(&db_path)?;
        validate_schema(&connection)?;
        let dense_dimension = dense.dimension as usize;
        let cache = DenseVectorCache::load(&connection, dense_dimension)?;

        // This startup event is the durable/cache readiness boundary for
        // background operators: schema validation succeeded and active vectors
        // are resident in memory.
        info!(
            event = "storage.opened",
            db_path = %db_path.display(),
            vectors = cache.unit_ids.len(),
            dimension = cache.dimension,
            memory_bytes = cache.memory_bytes,
            load_duration_ms = cache.load_duration_ms,
            "SQLite storage opened and dense cache loaded"
        );

        Ok(Self {
            db_path,
            dense_dimension,
            colbert_dimension: colbert.dimension as usize,
            cache: Arc::new(Mutex::new(cache)),
        })
    }

    /// Return storage/cache readiness details for health diagnostics.
    pub fn health_details(&self) -> Vec<String> {
        match self.cache.lock() {
            Ok(cache) => vec![
                format!("sqlite database ready: {}", self.db_path.display()),
                format!(
                    "dense cache ready: active_sources {}, vectors {}, dim {}, memory_bytes {}, loaded_at_ms {}, load_duration_ms {}",
                    cache.active_versions.len(),
                    cache.unit_ids.len(),
                    cache.dimension,
                    cache.memory_bytes,
                    cache.loaded_at_ms,
                    cache.load_duration_ms
                ),
            ],
            Err(source) => vec![format!("dense cache lock is poisoned: {source}")],
        }
    }

    /// Return the active version label for a source path when the source is currently searchable.
    pub fn active_version_for_source(&self, source_path: &str) -> Result<Option<String>, ApiError> {
        let cache = self
            .cache
            .lock()
            .map_err(|source| ApiError::StorageOperation {
                message: format!("dense cache lock is poisoned: {source}"),
            })?;

        Ok(cache
            .active_versions
            .iter()
            .find(|active| active.source_path == source_path)
            .map(|active| active.version_label.clone()))
    }

    /// Capture the active search-visible cache immediately after request admission.
    pub fn capture_search_snapshot(
        &self,
        operation_id: &str,
        query_chars: usize,
        top_k: u32,
    ) -> Result<SearchSnapshot, ApiError> {
        let snapshot_started = Instant::now();
        info!(
            event = "storage.search_snapshot.capture_started",
            operation_id, query_chars, top_k, "search snapshot capture started"
        );
        let cache = match self.cache.lock().map_err(|source| {
            storage_operation_error(format!("dense cache lock is poisoned: {source}"))
        }) {
            Ok(cache) => cache,
            Err(source) => {
                error!(
                    event = "storage.search_snapshot.capture_failed",
                    operation_id,
                    query_chars,
                    top_k,
                    phase = "cache_lock",
                    error = %source,
                    elapsed_ms = snapshot_started.elapsed().as_millis() as u64,
                    "search snapshot capture failed"
                );
                return Err(source);
            }
        };
        let snapshot = cache.clone();
        info!(
            event = "storage.search_snapshot.capture_completed",
            operation_id,
            query_chars,
            top_k,
            active_sources = snapshot.active_versions.len(),
            vectors = snapshot.unit_ids.len(),
            dimension = snapshot.dimension,
            memory_bytes = snapshot.memory_bytes,
            elapsed_ms = snapshot_started.elapsed().as_millis() as u64,
            "search snapshot capture completed"
        );

        Ok(SearchSnapshot { cache: snapshot })
    }

    /// Return active ingested source documents without admin-only version diagnostics.
    pub fn list_ingested_sources(&self) -> Result<IngestedSourceListing, ApiError> {
        let started = Instant::now();
        info!(
            event = "storage.ingested_sources.listing_started",
            db_path = %self.db_path.display(),
            "ingested-source listing started"
        );
        let connection = match open_connection(&self.db_path) {
            Ok(connection) => connection,
            Err(source) => {
                error!(
                    event = "storage.ingested_sources.listing_failed",
                    db_path = %self.db_path.display(),
                    phase = "connection_open",
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "ingested-source listing failed"
                );
                return Err(source);
            }
        };
        let mut statement = match connection
            .prepare(LIST_INGESTED_SOURCES_SQL)
            .map_err(|source| {
                storage_operation_error(format!(
                    "failed to prepare ingested-source listing: {source}"
                ))
            }) {
            Ok(statement) => statement,
            Err(source) => {
                error!(
                    event = "storage.ingested_sources.listing_failed",
                    db_path = %self.db_path.display(),
                    phase = "statement_prepare",
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "ingested-source listing failed"
                );
                return Err(source);
            }
        };
        let rows = match statement
            .query_map([], |row| {
                Ok(QueriedIngestedSource {
                    source_path: row.get::<_, String>(0)?,
                    active_version_label: row.get::<_, String>(1)?,
                    document_id: row.get::<_, String>(2)?,
                    units_ingested: row.get::<_, i64>(3)?,
                    status: row.get::<_, String>(4)?,
                    created_at_ms: row.get::<_, i64>(5)?,
                    updated_at_ms: row.get::<_, i64>(6)?,
                })
            })
            .map_err(|source| {
                storage_operation_error(format!(
                    "failed to execute ingested-source listing: {source}"
                ))
            }) {
            Ok(rows) => rows,
            Err(source) => {
                error!(
                    event = "storage.ingested_sources.listing_failed",
                    db_path = %self.db_path.display(),
                    phase = "query_execute",
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "ingested-source listing failed"
                );
                return Err(source);
            }
        };
        let mut sources = Vec::new();
        for row in rows {
            let source = match row.map_err(|source| {
                storage_operation_error(format!("failed to read ingested-source row: {source}"))
            }) {
                Ok(source) => source,
                Err(source) => {
                    error!(
                        event = "storage.ingested_sources.listing_failed",
                        db_path = %self.db_path.display(),
                        phase = "row_read",
                        error = %source,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "ingested-source listing failed"
                    );
                    return Err(source);
                }
            };
            let record = IngestedSourceRecord {
                source_path: source.source_path,
                active_version_label: source.active_version_label,
                document_id: source.document_id,
                units_ingested: match i64_to_u32(
                    "document_versions.units_ingested",
                    source.units_ingested,
                ) {
                    Ok(units_ingested) => units_ingested,
                    Err(source) => {
                        error!(
                            event = "storage.ingested_sources.listing_failed",
                            db_path = %self.db_path.display(),
                            phase = "record_materialization",
                            error = %source,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "ingested-source listing failed"
                        );
                        return Err(source);
                    }
                },
                status: source.status,
                created_at_ms: match i64_to_u64(
                    "document_versions.created_at_ms",
                    source.created_at_ms,
                ) {
                    Ok(created_at_ms) => created_at_ms,
                    Err(source) => {
                        error!(
                            event = "storage.ingested_sources.listing_failed",
                            db_path = %self.db_path.display(),
                            phase = "record_materialization",
                            error = %source,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "ingested-source listing failed"
                        );
                        return Err(source);
                    }
                },
                updated_at_ms: match i64_to_u64(
                    "document_versions.updated_at_ms",
                    source.updated_at_ms,
                ) {
                    Ok(updated_at_ms) => updated_at_ms,
                    Err(source) => {
                        error!(
                            event = "storage.ingested_sources.listing_failed",
                            db_path = %self.db_path.display(),
                            phase = "record_materialization",
                            error = %source,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "ingested-source listing failed"
                        );
                        return Err(source);
                    }
                },
            };
            sources.push(record);
        }
        let listing = IngestedSourceListing { sources };
        info!(
            event = "storage.ingested_sources.listing_completed",
            db_path = %self.db_path.display(),
            sources = listing.sources.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "ingested-source listing completed"
        );

        Ok(listing)
    }

    /// Return retained source-document versions and active-version diagnostics for admin inspection.
    pub fn list_document_versions(&self) -> Result<DocumentVersionListing, ApiError> {
        let started = Instant::now();
        info!(
            event = "storage.document_versions.listing_started",
            db_path = %self.db_path.display(),
            "document-version listing started"
        );
        let connection = match open_connection(&self.db_path) {
            Ok(connection) => connection,
            Err(source) => {
                error!(
                    event = "storage.document_versions.listing_failed",
                    db_path = %self.db_path.display(),
                    phase = "connection_open",
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "document-version listing failed"
                );
                return Err(source);
            }
        };
        let mut statement = match connection
            .prepare(LIST_DOCUMENT_VERSIONS_SQL)
            .map_err(|source| {
                storage_operation_error(format!(
                    "failed to prepare document-version listing: {source}"
                ))
            }) {
            Ok(statement) => statement,
            Err(source) => {
                error!(
                    event = "storage.document_versions.listing_failed",
                    db_path = %self.db_path.display(),
                    phase = "statement_prepare",
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "document-version listing failed"
                );
                return Err(source);
            }
        };
        let rows = match statement
            .query_map([], |row| {
                Ok(QueriedDocumentVersion {
                    source_path: row.get::<_, String>(0)?,
                    version_label: row.get::<_, String>(1)?,
                    document_id: row.get::<_, String>(2)?,
                    source_sha256: row.get::<_, Option<String>>(3)?,
                    markdown_path: row.get::<_, String>(4)?,
                    markdown_sha256: row.get::<_, Option<String>>(5)?,
                    pdf_backend: row.get::<_, String>(6)?,
                    ocr_mode: row.get::<_, String>(7)?,
                    page_batch_size: row.get::<_, Option<i64>>(8)?,
                    units_ingested: row.get::<_, i64>(9)?,
                    status: row.get::<_, String>(10)?,
                    diagnostics_json: row.get::<_, String>(11)?,
                    created_at_ms: row.get::<_, i64>(12)?,
                    updated_at_ms: row.get::<_, i64>(13)?,
                    is_active: row.get::<_, i64>(14)? != 0,
                })
            })
            .map_err(|source| {
                storage_operation_error(format!(
                    "failed to execute document-version listing: {source}"
                ))
            }) {
            Ok(rows) => rows,
            Err(source) => {
                error!(
                    event = "storage.document_versions.listing_failed",
                    db_path = %self.db_path.display(),
                    phase = "query_execute",
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "document-version listing failed"
                );
                return Err(source);
            }
        };
        let mut sources = BTreeMap::<String, SourceDocumentVersionListing>::new();
        for row in rows {
            let version = match row.map_err(|source| {
                storage_operation_error(format!("failed to read document-version row: {source}"))
            }) {
                Ok(version) => version,
                Err(source) => {
                    error!(
                        event = "storage.document_versions.listing_failed",
                        db_path = %self.db_path.display(),
                        phase = "row_read",
                        error = %source,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "document-version listing failed"
                    );
                    return Err(source);
                }
            };
            let dense_metadata = match load_dense_vector_metadata_for_version(
                &connection,
                &version.source_path,
                &version.version_label,
            ) {
                Ok(metadata) => metadata,
                Err(source) => {
                    error!(
                        event = "storage.document_versions.listing_failed",
                        db_path = %self.db_path.display(),
                        source_path = %version.source_path,
                        version_label = %version.version_label,
                        phase = "dense_metadata_load",
                        error = %source,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "document-version listing failed"
                    );
                    return Err(source);
                }
            };
            let colbert_metadata = match load_colbert_vector_metadata_for_version(
                &connection,
                &version.source_path,
                &version.version_label,
            ) {
                Ok(metadata) => metadata,
                Err(source) => {
                    error!(
                        event = "storage.document_versions.listing_failed",
                        db_path = %self.db_path.display(),
                        source_path = %version.source_path,
                        version_label = %version.version_label,
                        phase = "colbert_metadata_load",
                        error = %source,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "document-version listing failed"
                    );
                    return Err(source);
                }
            };
            let record = match version.to_record(dense_metadata, colbert_metadata) {
                Ok(record) => record,
                Err(source) => {
                    error!(
                        event = "storage.document_versions.listing_failed",
                        db_path = %self.db_path.display(),
                        source_path = %version.source_path,
                        version_label = %version.version_label,
                        phase = "record_materialization",
                        error = %source,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "document-version listing failed"
                    );
                    return Err(source);
                }
            };
            let entry = sources
                .entry(version.source_path.clone())
                .or_insert_with(|| SourceDocumentVersionListing {
                    source_path: version.source_path.clone(),
                    active_version_label: None,
                    versions: Vec::new(),
                });
            if record.is_active {
                entry.active_version_label = Some(record.version_label.clone());
            }
            entry.versions.push(record);
        }
        let listing = DocumentVersionListing {
            sources: sources.into_values().collect(),
        };
        let version_count = listing
            .sources
            .iter()
            .map(|source| source.versions.len())
            .sum::<usize>();
        info!(
            event = "storage.document_versions.listing_completed",
            db_path = %self.db_path.display(),
            sources = listing.sources.len(),
            versions = version_count,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "document-version listing completed"
        );

        Ok(listing)
    }

    /// Publish an older retained source-document version as active without rebuilding or deleting data.
    pub fn rollback_document_version(
        &self,
        source_path: &str,
        version_label: &str,
    ) -> Result<DocumentVersionRollbackResult, ApiError> {
        let started = Instant::now();
        info!(
            event = "storage.document_version_rollback.started",
            source_path,
            version_label,
            db_path = %self.db_path.display(),
            expected_dense_dimension = self.dense_dimension,
            "document-version rollback started"
        );
        let connection = match open_connection(&self.db_path) {
            Ok(connection) => connection,
            Err(source) => {
                error!(
                    event = "storage.document_version_rollback.failed",
                    source_path,
                    version_label,
                    db_path = %self.db_path.display(),
                    phase = "connection_open",
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "document-version rollback failed"
                );
                return Err(source);
            }
        };
        let version_exists = match document_version_exists(&connection, source_path, version_label)
        {
            Ok(version_exists) => version_exists,
            Err(source) => {
                error!(
                    event = "storage.document_version_rollback.failed",
                    source_path,
                    version_label,
                    db_path = %self.db_path.display(),
                    phase = "version_lookup",
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "document-version rollback failed"
                );
                return Err(source);
            }
        };
        if !version_exists {
            let error = ApiError::BadRequest {
                message: format!(
                    "document version not found for source {source_path} and versionLabel {version_label}"
                ),
            };
            warn!(
                event = "storage.document_version_rollback.failed",
                source_path,
                version_label,
                db_path = %self.db_path.display(),
                phase = "version_lookup",
                status = error.status_u16(),
                error_kind = error.error_kind(),
                error = %error,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "document-version rollback failed"
            );
            return Err(error);
        }
        let stored_vectors = match load_dense_vectors_for_version(
            &connection,
            source_path,
            version_label,
            self.dense_dimension,
        ) {
            Ok(vectors) => vectors,
            Err(source) => {
                error!(
                    event = "storage.document_version_rollback.failed",
                    source_path,
                    version_label,
                    db_path = %self.db_path.display(),
                    phase = "dense_vector_load",
                    expected_dense_dimension = self.dense_dimension,
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "document-version rollback failed"
                );
                return Err(source);
            }
        };
        let vector_count = stored_vectors.len();
        let published_at_ms = match self.publish_source_version_with_vectors(
            source_path,
            version_label,
            stored_vectors,
        ) {
            Ok(published_at_ms) => published_at_ms,
            Err(source) => {
                error!(
                    event = "storage.document_version_rollback.failed",
                    source_path,
                    version_label,
                    db_path = %self.db_path.display(),
                    phase = "active_publish",
                    vector_count,
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "document-version rollback failed"
                );
                return Err(source);
            }
        };

        // Rollback publishes an already-retained immutable version; it never
        // rewrites embeddings or deletes inactive versions.
        info!(
            event = "storage.document_version_rollback.published",
            source_path,
            version_label,
            vector_count,
            published_at_ms,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "document version rolled back"
        );

        Ok(DocumentVersionRollbackResult {
            source_path: source_path.to_string(),
            active_version_label: version_label.to_string(),
            published_at_ms,
            vector_count,
        })
    }

    /// Persist one immutable document version, active map, and cache publish as one ingest boundary.
    pub fn ingest_document<F>(
        &self,
        conversion: &DoclingConversionResult,
        version_label: &str,
        units: &[RetrievalUnit],
        vectors: Vec<UnitDenseVector>,
        colbert_vectors: Vec<UnitColbertDocumentVector>,
        dense: &DenseModelConfig,
        colbert: &ColbertModelConfig,
        mut progress: Option<F>,
    ) -> Result<(), ApiError>
    where
        F: FnMut(&'static str, u64, u64) -> Result<(), ApiError>,
    {
        if units.len() != vectors.len() {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "unit/vector count mismatch during ingest: units={}, vectors={}",
                    units.len(),
                    vectors.len()
                ),
            });
        }
        if units.len() != colbert_vectors.len() {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "unit/ColBERT vector count mismatch during ingest: units={}, colbert_vectors={}",
                    units.len(),
                    colbert_vectors.len()
                ),
            });
        }

        info!(
            event = "storage.ingest_vectors.validation_started",
            source_path = %conversion.source.relative_path.display(),
            version_label,
            units = units.len(),
            dense_vectors = vectors.len(),
            colbert_document_vectors = colbert_vectors.len(),
            "ingest vector validation started"
        );
        let stored_vectors = match vectors
            .into_iter()
            .map(|value| {
                validate_vector(value.unit_id, value.vector, self.dense_dimension)
                    .map_err(storage_operation_error)
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(vectors) => vectors,
            Err(source) => {
                error!(
                    event = "storage.ingest_vectors.validation_failed",
                    source_path = %conversion.source.relative_path.display(),
                    version_label,
                    vector_type = "dense",
                    error = %source,
                    "ingest dense vector validation failed"
                );
                return Err(source);
            }
        };
        let stored_colbert_vectors = match colbert_vectors
            .into_iter()
            .map(|value| validate_colbert_document_vector(value, self.colbert_dimension))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(vectors) => vectors,
            Err(source) => {
                error!(
                    event = "storage.ingest_vectors.validation_failed",
                    source_path = %conversion.source.relative_path.display(),
                    version_label,
                    vector_type = "colbert",
                    error = %source,
                    "ingest ColBERT vector validation failed"
                );
                return Err(source);
            }
        };
        info!(
            event = "storage.ingest_vectors.validation_completed",
            source_path = %conversion.source.relative_path.display(),
            version_label,
            dense_vectors = stored_vectors.len(),
            colbert_document_vectors = stored_colbert_vectors.len(),
            "ingest vector validation completed"
        );
        let source_bytes =
            fs::read(&conversion.source.absolute_path).map_err(|source| ApiError::InternalIo {
                message: format!(
                    "failed to read source for checksum at {}: {source}",
                    conversion.source.absolute_path.display()
                ),
            })?;
        let source_sha256 = sha256_hex(&source_bytes);
        let markdown_sha256 = sha256_hex(conversion.markdown.as_bytes());
        let now_ms = current_time_ms()?;
        let diagnostics = build_ingest_diagnostics(conversion)?;
        let document_id = units
            .first()
            .map(|unit| unit.document_id.clone())
            .unwrap_or_else(|| {
                build_versioned_document_id(&conversion.source.relative_path, version_label)
            });
        if document_id.is_empty() {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "could not derive document id from source path {}",
                    conversion.source.relative_path.display()
                ),
            });
        }
        let source_path = conversion.source.relative_path.display().to_string();
        let vector_count = stored_vectors.len();

        info!(
            event = "storage.ingest_transaction.starting",
            source_path,
            version_label,
            document_id,
            units = units.len(),
            "ingest SQLite transaction starting"
        );
        let mut connection = match open_connection(&self.db_path) {
            Ok(connection) => connection,
            Err(source) => {
                error!(
                    event = "storage.ingest_transaction.failed",
                    source_path,
                    version_label,
                    document_id,
                    phase = "connection_open",
                    error = %source,
                    "ingest SQLite transaction failed"
                );
                return Err(source);
            }
        };
        let tx = match connection.transaction().map_err(|source| {
            storage_operation_error(format!("failed to begin ingest transaction: {source}"))
        }) {
            Ok(tx) => tx,
            Err(source) => {
                error!(
                    event = "storage.ingest_transaction.failed",
                    source_path,
                    version_label,
                    document_id,
                    phase = "transaction_begin",
                    error = %source,
                    "ingest SQLite transaction failed"
                );
                return Err(source);
            }
        };
        info!(
            event = "storage.ingest_transaction.started",
            source_path, version_label, document_id, "ingest SQLite transaction started"
        );
        // The SQLite transaction rolls back by drop; this makes each intentional pre-commit abort durable.
        let log_ingest_transaction_aborting =
            |phase: &'static str,
             vector_count: usize,
             published_at_ms: Option<u64>,
             error: &ApiError| {
                error!(
                    event = "storage.ingest_transaction.aborting",
                    source_path,
                    version_label,
                    document_id,
                    phase,
                    units = units.len(),
                    vectors = vector_count,
                    published_at_ms = ?published_at_ms,
                    durable_commit_completed = false,
                    error = %error,
                    "ingest SQLite transaction aborting before commit"
                );
            };

        if let Err(source) = insert_document(
            &tx,
            conversion,
            version_label,
            units.len(),
            &document_id,
            &source_sha256,
            &markdown_sha256,
            &diagnostics,
            now_ms,
        ) {
            error!(
                event = "storage.ingest_document_metadata.failed",
                source_path,
                version_label,
                document_id,
                error = %source,
                "ingest document metadata persistence failed"
            );
            log_ingest_transaction_aborting("document_metadata", vector_count, None, &source);
            return Err(source);
        }
        info!(
            event = "storage.ingest_document_metadata.persisted",
            source_path,
            version_label,
            document_id,
            units = units.len(),
            "ingest document metadata persisted"
        );
        if let Err(source) =
            emit_ingest_storage_progress(&mut progress, "persisting document metadata", 1, 1)
        {
            log_ingest_transaction_aborting(
                "document_metadata_progress",
                vector_count,
                None,
                &source,
            );
            return Err(source);
        }
        for (index, ((unit, vector), colbert_vector)) in units
            .iter()
            .zip(stored_vectors.iter())
            .zip(stored_colbert_vectors.iter())
            .enumerate()
        {
            if unit.unit_id != vector.unit_id || unit.unit_id != colbert_vector.unit_id {
                let error = storage_operation_error(format!(
                    "unit/vector id mismatch during ingest: unit={}, dense={}, colbert={}",
                    unit.unit_id, vector.unit_id, colbert_vector.unit_id
                ));
                error!(
                    event = "storage.ingest_units.failed",
                    source_path,
                    version_label,
                    document_id,
                    unit_id = %unit.unit_id,
                    current = index + 1,
                    total = units.len(),
                    phase = "id_validation",
                    error = %error,
                    "ingest unit persistence failed"
                );
                log_ingest_transaction_aborting("id_validation", vector_count, None, &error);
                return Err(error);
            }
            if let Err(source) = insert_unit(&tx, unit, version_label) {
                error!(
                    event = "storage.ingest_units.failed",
                    source_path,
                    version_label,
                    document_id,
                    unit_id = %unit.unit_id,
                    current = index + 1,
                    total = units.len(),
                    phase = "unit_row",
                    error = %source,
                    "ingest unit persistence failed"
                );
                log_ingest_transaction_aborting("unit_row", vector_count, None, &source);
                return Err(source);
            }
            if let Err(source) = insert_dense_vector(&tx, vector, dense, now_ms) {
                error!(
                    event = "storage.ingest_units.failed",
                    source_path,
                    version_label,
                    document_id,
                    unit_id = %unit.unit_id,
                    current = index + 1,
                    total = units.len(),
                    phase = "dense_vector_row",
                    error = %source,
                    "ingest unit persistence failed"
                );
                log_ingest_transaction_aborting("dense_vector_row", vector_count, None, &source);
                return Err(source);
            }
            if let Err(source) =
                insert_colbert_document_vector(&tx, colbert_vector, colbert, now_ms)
            {
                error!(
                    event = "storage.ingest_units.failed",
                    source_path,
                    version_label,
                    document_id,
                    unit_id = %unit.unit_id,
                    current = index + 1,
                    total = units.len(),
                    phase = "colbert_vector_row",
                    error = %source,
                    "ingest unit persistence failed"
                );
                log_ingest_transaction_aborting("colbert_vector_row", vector_count, None, &source);
                return Err(source);
            }
            if should_log_storage_checkpoint(index + 1, units.len()) {
                info!(
                    event = "storage.ingest_units.persisted_checkpoint",
                    source_path,
                    version_label,
                    document_id,
                    current = index + 1,
                    total = units.len(),
                    "ingest units and vectors persisted checkpoint"
                );
            }
            if let Err(source) = emit_ingest_storage_progress(
                &mut progress,
                "persisting units and vectors",
                (index + 1) as u64,
                units.len() as u64,
            ) {
                log_ingest_transaction_aborting("units_progress", vector_count, None, &source);
                return Err(source);
            }
        }

        let published_at_ms = match current_time_ms() {
            Ok(value) => value,
            Err(source) => {
                error!(
                    event = "storage.ingest_publish.failed",
                    source_path,
                    version_label,
                    document_id,
                    vectors = vector_count,
                    phase = "publish_timestamp",
                    error = %source,
                    "ingest active-version publish failed"
                );
                log_ingest_transaction_aborting("publish_timestamp", vector_count, None, &source);
                return Err(source);
            }
        };
        info!(
            event = "storage.ingest_publish.starting",
            source_path,
            version_label,
            document_id,
            vectors = vector_count,
            published_at_ms,
            "ingest active-version publish starting"
        );
        let mut cache = match self.cache.lock().map_err(|source| {
            storage_operation_error(format!("dense cache lock is poisoned: {source}"))
        }) {
            Ok(cache) => cache,
            Err(source) => {
                error!(
                    event = "storage.ingest_publish.failed",
                    source_path,
                    version_label,
                    document_id,
                    vectors = vector_count,
                    published_at_ms,
                    phase = "cache_lock",
                    error = %source,
                    "ingest active-version publish failed"
                );
                log_ingest_transaction_aborting(
                    "cache_lock",
                    vector_count,
                    Some(published_at_ms),
                    &source,
                );
                return Err(source);
            }
        };
        info!(
            event = "storage.ingest_publish.cache_prepare_starting",
            source_path,
            version_label,
            document_id,
            vectors = vector_count,
            published_at_ms,
            current_active_sources = cache.active_versions.len(),
            current_cache_vectors = cache.unit_ids.len(),
            "ingest active-version cache preparation starting"
        );
        let published_cache = match cache.with_published_source_version(
            &source_path,
            version_label,
            stored_vectors,
            published_at_ms,
        ) {
            Ok(cache) => cache,
            Err(source) => {
                error!(
                    event = "storage.ingest_publish.failed",
                    source_path,
                    version_label,
                    document_id,
                    vectors = vector_count,
                    published_at_ms,
                    phase = "cache_prepare",
                    error = %source,
                    "ingest active-version publish failed"
                );
                log_ingest_transaction_aborting(
                    "cache_prepare",
                    vector_count,
                    Some(published_at_ms),
                    &source,
                );
                return Err(source);
            }
        };
        let published_cache_vectors = published_cache.unit_ids.len();
        let published_cache_active_sources = published_cache.active_versions.len();
        info!(
            event = "storage.ingest_publish.cache_prepared",
            source_path,
            version_label,
            document_id,
            vectors = vector_count,
            published_at_ms,
            cache_vectors = published_cache_vectors,
            active_sources = published_cache_active_sources,
            "ingest active-version cache prepared"
        );
        info!(
            event = "storage.ingest_publish.active_row_writing",
            source_path,
            version_label,
            document_id,
            vectors = vector_count,
            published_at_ms,
            phase = "active_row_write",
            "ingest active-version row writing"
        );
        if let Err(source) = tx
            .execute(
                PUBLISH_ACTIVE_DOCUMENT_VERSION_SQL,
                params![&source_path, version_label, published_at_ms as i64],
            )
            .map_err(|source| {
                storage_operation_error(format!(
                    "failed to publish active document version during ingest: {source}"
                ))
            })
        {
            error!(
                event = "storage.ingest_publish.failed",
                source_path,
                version_label,
                document_id,
                vectors = vector_count,
                published_at_ms,
                phase = "active_row_write",
                error = %source,
                "ingest active-version publish failed"
            );
            log_ingest_transaction_aborting(
                "active_row_write",
                vector_count,
                Some(published_at_ms),
                &source,
            );
            return Err(source);
        }
        info!(
            event = "storage.ingest_publish.active_row_written",
            source_path,
            version_label,
            document_id,
            vectors = vector_count,
            published_at_ms,
            phase = "active_row_write",
            "ingest active-version row written"
        );
        if let Err(source) =
            emit_ingest_storage_progress(&mut progress, "committing ingest transaction", 1, 1)
        {
            log_ingest_transaction_aborting(
                "commit_progress",
                vector_count,
                Some(published_at_ms),
                &source,
            );
            return Err(source);
        }
        info!(
            event = "storage.ingest_transaction.commit_starting",
            source_path,
            version_label,
            document_id,
            units = units.len(),
            vectors = vector_count,
            published_at_ms,
            "ingest SQLite transaction commit starting"
        );
        if let Err(source) = tx.commit().map_err(|source| {
            storage_operation_error(format!("failed to commit ingest transaction: {source}"))
        }) {
            error!(
                event = "storage.ingest_transaction.commit_failed",
                source_path,
                version_label,
                document_id,
                vectors = vector_count,
                published_at_ms,
                error = %source,
                "ingest SQLite transaction commit failed"
            );
            return Err(source);
        }
        info!(
            event = "storage.ingest_transaction.committed",
            source_path,
            version_label,
            document_id,
            units = units.len(),
            vectors = vector_count,
            published_at_ms,
            "ingest SQLite transaction committed"
        );
        info!(
            event = "storage.ingest_publish.cache_swap_starting",
            source_path,
            version_label,
            document_id,
            vectors = vector_count,
            published_at_ms,
            phase = "cache_swap",
            "ingest active-version dense cache swap starting"
        );
        *cache = published_cache;
        info!(
            event = "storage.ingest_publish.cache_swap_completed",
            source_path,
            version_label,
            document_id,
            vectors = vector_count,
            published_at_ms,
            cache_vectors = published_cache_vectors,
            active_sources = published_cache_active_sources,
            phase = "cache_swap",
            "ingest active-version dense cache swap completed"
        );
        info!(
            event = "storage.ingest_version.published",
            source_path,
            version_label,
            document_id,
            units = units.len(),
            vectors = vector_count,
            published_at_ms,
            "ingested document version published"
        );

        Ok(())
    }

    /// Build the bounded first-stage candidate pool with dense exact scan, SQLite FTS5 BM25, and RRF fusion.
    ///
    /// ColBERT and later rerankers consume this pool before final public top-K truncation.
    pub fn build_search_candidate_pool(
        &self,
        operation_id: &str,
        query: &str,
        query_vector: Vec<f32>,
        snapshot: SearchSnapshot,
        top_k: u32,
        retrieval: &RetrievalConfig,
    ) -> Result<SearchCandidatePoolOutput, ApiError> {
        let started = Instant::now();
        let query_chars = query.chars().count();
        let requested_query_vector_values = query_vector.len();
        let candidate_limit = first_stage_candidate_limit(top_k, retrieval);
        info!(
            event = "storage.search_candidate_pool.started",
            operation_id,
            query_chars,
            top_k,
            requested_query_vector_values,
            expected_dense_dimension = self.dense_dimension,
            candidate_limit,
            colbert_candidate_pool_size = retrieval.colbert_candidate_pool_size,
            "search candidate pool construction started"
        );
        let validation_started = Instant::now();
        let query_vector = match validate_vector(
            "search-query".to_string(),
            query_vector,
            self.dense_dimension,
        )
        .map_err(storage_operation_error)
        {
            Ok(vector) => vector,
            Err(source) => {
                error!(
                    event = "storage.search_query_vector.validation_failed",
                    operation_id,
                    query_chars,
                    top_k,
                    requested_query_vector_values,
                    expected_dense_dimension = self.dense_dimension,
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "search query vector validation failed"
                );
                return Err(source);
            }
        };
        let query_vector_validation_latency_ms = validation_started.elapsed().as_millis() as u64;
        info!(
            event = "storage.search_query_vector.validation_completed",
            operation_id,
            query_chars,
            top_k,
            vector_dimension = query_vector.vector.len(),
            vector_norm = query_vector.norm,
            elapsed_ms = query_vector_validation_latency_ms,
            "search query vector validation completed"
        );
        let cache_snapshot = snapshot.cache;

        let dense_started = Instant::now();
        info!(
            event = "storage.search_dense_scan.started",
            operation_id,
            query_chars,
            top_k,
            candidate_limit,
            cache_vectors = cache_snapshot.unit_ids.len(),
            vector_dimension = query_vector.vector.len(),
            "search dense scan started"
        );
        let dense_matches = match cache_snapshot.search(&query_vector, candidate_limit) {
            Ok(matches) => matches,
            Err(source) => {
                error!(
                    event = "storage.search_dense_scan.failed",
                    operation_id,
                    query_chars,
                    top_k,
                    candidate_limit,
                    cache_vectors = cache_snapshot.unit_ids.len(),
                    error = %source,
                    elapsed_ms = dense_started.elapsed().as_millis() as u64,
                    "search dense scan failed"
                );
                return Err(source);
            }
        };
        let dense_latency_ms = dense_started.elapsed().as_millis() as u64;
        info!(
            event = "storage.search_dense_scan.completed",
            operation_id,
            query_chars,
            top_k,
            candidate_limit,
            matches = dense_matches.len(),
            elapsed_ms = dense_latency_ms,
            "search dense scan completed"
        );

        let bm25_started = Instant::now();
        // Treat user text as plain search terms, not FTS syntax, so operators cannot alter the query language boundary.
        let bm25_query = build_fts_query(query);
        info!(
            event = "storage.search_bm25.started",
            operation_id,
            query_chars,
            top_k,
            candidate_limit,
            fts_query_present = bm25_query.is_some(),
            active_sources = cache_snapshot.active_versions.len(),
            "search BM25 started"
        );
        let bm25_matches = match self.search_bm25(
            bm25_query.as_deref(),
            candidate_limit,
            &cache_snapshot.active_versions,
        ) {
            Ok(matches) => matches,
            Err(source) => {
                error!(
                    event = "storage.search_bm25.failed",
                    operation_id,
                    query_chars,
                    top_k,
                    candidate_limit,
                    fts_query_present = bm25_query.is_some(),
                    active_sources = cache_snapshot.active_versions.len(),
                    error = %source,
                    elapsed_ms = bm25_started.elapsed().as_millis() as u64,
                    "search BM25 failed"
                );
                return Err(source);
            }
        };
        let bm25_latency_ms = bm25_started.elapsed().as_millis() as u64;
        info!(
            event = "storage.search_bm25.completed",
            operation_id,
            query_chars,
            top_k,
            candidate_limit,
            matches = bm25_matches.len(),
            elapsed_ms = bm25_latency_ms,
            "search BM25 completed"
        );

        let fusion_started = Instant::now();
        info!(
            event = "storage.search_rrf_fusion.started",
            operation_id,
            query_chars,
            top_k,
            dense_matches = dense_matches.len(),
            bm25_matches = bm25_matches.len(),
            colbert_candidate_pool_size = retrieval.colbert_candidate_pool_size,
            rrf_k = retrieval.rrf_k,
            "search RRF fusion started"
        );
        let fused_matches = fuse_matches(
            &dense_matches,
            &bm25_matches,
            retrieval.colbert_candidate_pool_size as usize,
            retrieval.rrf_k,
        );
        let rrf_fusion_latency_ms = fusion_started.elapsed().as_millis() as u64;
        info!(
            event = "storage.search_rrf_fusion.completed",
            operation_id,
            query_chars,
            top_k,
            fused_matches = fused_matches.len(),
            elapsed_ms = rrf_fusion_latency_ms,
            "search RRF fusion completed"
        );
        let materialization_started = Instant::now();
        info!(
            event = "storage.search_candidate_materialization.started",
            operation_id,
            query_chars,
            top_k,
            fused_matches = fused_matches.len(),
            "search candidate materialization started"
        );
        let units = match self.load_units_for_fused_matches(&fused_matches) {
            Ok(units) => units,
            Err(source) => {
                error!(
                    event = "storage.search_candidate_materialization.failed",
                    operation_id,
                    query_chars,
                    top_k,
                    fused_matches = fused_matches.len(),
                    phase = "unit_load",
                    error = %source,
                    elapsed_ms = materialization_started.elapsed().as_millis() as u64,
                    "search candidate materialization failed"
                );
                return Err(source);
            }
        };
        let candidates = fused_matches
            .iter()
            .filter_map(|matched| {
                let unit = units.iter().find(|unit| unit.unit_id == matched.unit_id)?;
                Some(SearchCandidate {
                    unit_id: unit.unit_id.clone(),
                    rrf_score: matched.score,
                    rrf_rank: matched.rank,
                    dense_rank: matched.dense_rank,
                    dense_similarity: matched.dense_similarity,
                    bm25_rank: matched.bm25_rank,
                    bm25_score: matched.bm25_score,
                    content: unit.content.clone(),
                    heading_path: unit.heading_path.clone(),
                    source_path: unit.source_path.clone(),
                    page_numbers: unit.page_numbers.clone(),
                    colbert_token_count: unit.colbert_token_count,
                    colbert_dimension: unit.colbert_dimension,
                    colbert_vector: unit.colbert_vector.clone(),
                })
            })
            .collect::<Vec<_>>();
        if candidates.len() != fused_matches.len() {
            let source = storage_operation_error(
                "fused search result materialization missed one or more unit rows".to_string(),
            );
            error!(
                event = "storage.search_candidate_materialization.failed",
                operation_id,
                query_chars,
                top_k,
                fused_matches = fused_matches.len(),
                loaded_units = units.len(),
                candidates = candidates.len(),
                phase = "candidate_join",
                error = %source,
                elapsed_ms = materialization_started.elapsed().as_millis() as u64,
                "search candidate materialization failed"
            );
            return Err(source);
        }
        let candidate_materialization_latency_ms =
            materialization_started.elapsed().as_millis() as u64;
        info!(
            event = "storage.search_candidate_materialization.completed",
            operation_id,
            query_chars,
            top_k,
            fused_matches = fused_matches.len(),
            loaded_units = units.len(),
            candidates = candidates.len(),
            elapsed_ms = candidate_materialization_latency_ms,
            "search candidate materialization completed"
        );
        let raw_started = Instant::now();
        info!(
            event = "storage.search_raw_diagnostics.started",
            operation_id,
            query_chars,
            top_k,
            dense_matches = dense_matches.len(),
            bm25_matches = bm25_matches.len(),
            fused_matches = fused_matches.len(),
            "search raw diagnostics assembly started"
        );
        let mut raw = match self.build_search_raw(SearchRawInput {
            cache: &cache_snapshot,
            query_vector: &query_vector,
            dense_matches: &dense_matches,
            bm25_matches: &bm25_matches,
            fused_matches: &fused_matches,
            bm25_query: bm25_query.as_deref(),
            candidate_limit,
            colbert_candidate_pool_size: retrieval.colbert_candidate_pool_size,
            top_k,
            rrf_k: retrieval.rrf_k,
            overfetch_multiplier: retrieval.candidate_overfetch_multiplier,
            query_vector_validation_latency_ms,
            dense_latency_ms,
            bm25_latency_ms,
            rrf_fusion_latency_ms,
            candidate_materialization_latency_ms,
            latency_ms: 0,
        }) {
            Ok(raw) => raw,
            Err(source) => {
                error!(
                    event = "storage.search_raw_diagnostics.failed",
                    operation_id,
                    query_chars,
                    top_k,
                    dense_matches = dense_matches.len(),
                    bm25_matches = bm25_matches.len(),
                    fused_matches = fused_matches.len(),
                    error = %source,
                    elapsed_ms = raw_started.elapsed().as_millis() as u64,
                    "search raw diagnostics assembly failed"
                );
                return Err(source);
            }
        };
        let raw_diagnostics_latency_ms = raw_started.elapsed().as_millis() as u64;
        let latency_ms = started.elapsed().as_millis() as u64;
        raw["retrieval"]["latencyMs"] = serde_json::json!(latency_ms);
        raw["retrieval"]["rawDiagnosticsLatencyMs"] = serde_json::json!(raw_diagnostics_latency_ms);
        info!(
            event = "storage.search_raw_diagnostics.completed",
            operation_id,
            query_chars,
            top_k,
            elapsed_ms = raw_diagnostics_latency_ms,
            "search raw diagnostics assembly completed"
        );
        info!(
            event = "storage.search_candidate_pool.completed",
            operation_id,
            query_chars,
            top_k,
            candidate_limit,
            candidates = candidates.len(),
            dense_matches = dense_matches.len(),
            bm25_matches = bm25_matches.len(),
            fused_matches = fused_matches.len(),
            elapsed_ms = latency_ms,
            "search candidate pool construction completed"
        );

        Ok(SearchCandidatePoolOutput { candidates, raw })
    }

    /// Retrieve BM25-ranked candidates from the SQLite FTS5 index.
    ///
    /// SQLite bm25() returns better matches as lower, usually negative, values.
    fn search_bm25(
        &self,
        fts_query: Option<&str>,
        limit: usize,
        active_versions: &[ActiveDocumentVersion],
    ) -> Result<Vec<Bm25Match>, ApiError> {
        let Some(fts_query) = fts_query else {
            return Ok(Vec::new());
        };
        if limit == 0 || active_versions.is_empty() {
            return Ok(Vec::new());
        }

        let connection = open_connection(&self.db_path)?;
        let active_filter = active_versions
            .iter()
            .map(|_| "(units.source_path = ? AND units.version_label = ?)")
            .collect::<Vec<_>>()
            .join(" OR ");
        let sql = format!("{BM25_SEARCH_SQL_PREFIX}{active_filter}{BM25_SEARCH_SQL_SUFFIX}");
        let mut query_params = Vec::<Value>::with_capacity(2 + active_versions.len() * 2);
        query_params.push(Value::from(fts_query.to_string()));
        for active in active_versions {
            query_params.push(Value::from(active.source_path.clone()));
            query_params.push(Value::from(active.version_label.clone()));
        }
        query_params.push(Value::from(limit as i64));
        let mut statement = connection
            // ASC preserves SQLite FTS5's lower-is-better bm25() ordering.
            .prepare(&sql)
            .map_err(|source| {
                storage_operation_error(format!("failed to prepare BM25 search: {source}"))
            })?;
        let rows = statement
            .query_map(params_from_iter(query_params), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
            })
            .map_err(|source| {
                storage_operation_error(format!("failed to execute BM25 search: {source}"))
            })?;
        let mut matches = Vec::new();
        for row in rows {
            let (unit_id, score) = row.map_err(|source| {
                storage_operation_error(format!("failed to read BM25 candidate: {source}"))
            })?;
            if !score.is_finite() {
                return Err(storage_operation_error(format!(
                    "BM25 score for {unit_id} is non-finite"
                )));
            }
            matches.push(Bm25Match {
                unit_id,
                score,
                rank: matches.len() + 1,
            });
        }

        Ok(matches)
    }

    /// Load durable unit metadata and content for already ranked fused matches.
    fn load_units_for_fused_matches(
        &self,
        matches: &[FusedMatch],
    ) -> Result<Vec<StoredUnit>, ApiError> {
        let connection = open_connection(&self.db_path)?;
        let mut units = Vec::with_capacity(matches.len());
        for matched in matches {
            units.push(load_unit(
                &connection,
                &matched.unit_id,
                self.colbert_dimension,
            )?);
        }

        Ok(units)
    }

    /// Build raw search diagnostics without hiding per-stage rank inputs.
    fn build_search_raw(&self, input: SearchRawInput<'_>) -> Result<serde_json::Value, ApiError> {
        Ok(serde_json::json!({
            "retrieval": {
                "mode": RETRIEVAL_MODE_DENSE_BM25_RRF_POOL,
                "latencyMs": input.latency_ms,
                "queryVectorValidationLatencyMs": input.query_vector_validation_latency_ms,
                "denseLatencyMs": input.dense_latency_ms,
                "bm25LatencyMs": input.bm25_latency_ms,
                "rrfFusionLatencyMs": input.rrf_fusion_latency_ms,
                "candidateMaterializationLatencyMs": input.candidate_materialization_latency_ms,
                "topK": input.top_k,
                "firstStageCandidateLimit": input.candidate_limit,
                "colbertCandidatePoolSize": input.colbert_candidate_pool_size,
                "rrfK": input.rrf_k,
                "candidateOverfetchMultiplier": input.overfetch_multiplier,
                "query": {
                    "dimension": input.query_vector.vector.len(),
                    "norm": input.query_vector.norm,
                    "fts": input.bm25_query
                },
                "cache": {
                    "vectorCount": input.cache.unit_ids.len(),
                    "dimension": input.cache.dimension,
                    "memoryBytes": input.cache.memory_bytes,
                    "loadedAtMs": input.cache.loaded_at_ms,
                    "loadDurationMs": input.cache.load_duration_ms
                },
                "activeVersions": input.cache.active_versions.iter().map(|active| {
                    serde_json::json!({
                        "sourcePath": active.source_path,
                        "versionLabel": active.version_label
                    })
                }).collect::<Vec<_>>(),
                "denseCandidates": input.dense_matches.iter().map(|matched| {
                    serde_json::json!({
                        "unitId": matched.unit_id,
                        "similarity": matched.similarity,
                        "rank": matched.rank
                    })
                }).collect::<Vec<_>>(),
                "bm25Candidates": input.bm25_matches.iter().map(|matched| {
                    serde_json::json!({
                        "unitId": matched.unit_id,
                        "score": matched.score,
                        "rank": matched.rank
                    })
                }).collect::<Vec<_>>(),
                "fusedCandidates": input.fused_matches.iter().map(|matched| {
                    serde_json::json!({
                        "unitId": matched.unit_id,
                        "score": matched.score,
                        "rank": matched.rank,
                        "denseRank": matched.dense_rank,
                        "denseSimilarity": matched.dense_similarity,
                        "bm25Rank": matched.bm25_rank,
                        "bm25Score": matched.bm25_score
                    })
                }).collect::<Vec<_>>()
            }
        }))
    }

    /// Publish one source version by committing the active map and swapping the cache under one lock.
    fn publish_source_version_with_vectors(
        &self,
        source_path: &str,
        version_label: &str,
        vectors: Vec<StoredDenseVector>,
    ) -> Result<u64, ApiError> {
        let started = Instant::now();
        let published_at_ms = current_time_ms()?;
        let vector_count = vectors.len();
        info!(
            event = "storage.active_version_publish.started",
            source_path,
            version_label,
            vectors = vector_count,
            published_at_ms,
            "active-version publish started"
        );
        let mut cache = match self.cache.lock().map_err(|source| {
            storage_operation_error(format!("dense cache lock is poisoned: {source}"))
        }) {
            Ok(cache) => cache,
            Err(source) => {
                error!(
                    event = "storage.active_version_publish.failed",
                    source_path,
                    version_label,
                    vectors = vector_count,
                    published_at_ms,
                    phase = "cache_lock",
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "active-version publish failed"
                );
                return Err(source);
            }
        };
        let published_cache = match cache.with_published_source_version(
            source_path,
            version_label,
            vectors,
            published_at_ms,
        ) {
            Ok(cache) => cache,
            Err(source) => {
                error!(
                    event = "storage.active_version_publish.failed",
                    source_path,
                    version_label,
                    vectors = vector_count,
                    published_at_ms,
                    phase = "cache_prepare",
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "active-version publish failed"
                );
                return Err(source);
            }
        };
        info!(
            event = "storage.active_version_publish.connection_opening",
            source_path,
            version_label,
            vectors = vector_count,
            published_at_ms,
            db_path = %self.db_path.display(),
            phase = "connection_open",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "active-version publish SQLite connection opening"
        );
        let mut connection = match open_connection(&self.db_path) {
            Ok(connection) => connection,
            Err(source) => {
                error!(
                    event = "storage.active_version_publish.failed",
                    source_path,
                    version_label,
                    vectors = vector_count,
                    published_at_ms,
                    db_path = %self.db_path.display(),
                    phase = "connection_open",
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "active-version publish failed"
                );
                return Err(source);
            }
        };
        info!(
            event = "storage.active_version_publish.connection_opened",
            source_path,
            version_label,
            vectors = vector_count,
            published_at_ms,
            db_path = %self.db_path.display(),
            phase = "connection_open",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "active-version publish SQLite connection opened"
        );
        info!(
            event = "storage.active_version_publish.transaction_starting",
            source_path,
            version_label,
            vectors = vector_count,
            published_at_ms,
            phase = "transaction_begin",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "active-version publish SQLite transaction starting"
        );
        let tx = match connection.transaction().map_err(|source| {
            storage_operation_error(format!("failed to begin active-version publish: {source}"))
        }) {
            Ok(tx) => tx,
            Err(source) => {
                error!(
                    event = "storage.active_version_publish.failed",
                    source_path,
                    version_label,
                    vectors = vector_count,
                    published_at_ms,
                    phase = "transaction_begin",
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "active-version publish failed"
                );
                return Err(source);
            }
        };
        info!(
            event = "storage.active_version_publish.transaction_started",
            source_path,
            version_label,
            vectors = vector_count,
            published_at_ms,
            phase = "transaction_begin",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "active-version publish SQLite transaction started"
        );
        // The SQLite transaction rolls back by drop; this makes each intentional pre-commit abort durable.
        let log_active_version_publish_aborting = |phase: &'static str, error: &ApiError| {
            error!(
                event = "storage.active_version_publish.aborting",
                source_path,
                version_label,
                vectors = vector_count,
                published_at_ms,
                phase,
                durable_commit_completed = false,
                error = %error,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "active-version publish transaction aborting before commit"
            );
        };
        info!(
            event = "storage.active_version_publish.active_row_writing",
            source_path,
            version_label,
            vectors = vector_count,
            published_at_ms,
            phase = "active_row_write",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "active-version row writing"
        );
        if let Err(source) = tx
            .execute(
                PUBLISH_ACTIVE_DOCUMENT_VERSION_SQL,
                params![source_path, version_label, published_at_ms as i64],
            )
            .map_err(|source| {
                storage_operation_error(format!(
                    "failed to publish active document version: {source}"
                ))
            })
        {
            error!(
                event = "storage.active_version_publish.failed",
                source_path,
                version_label,
                vectors = vector_count,
                published_at_ms,
                phase = "active_row_write",
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "active-version publish failed"
            );
            log_active_version_publish_aborting("active_row_write", &source);
            return Err(source);
        }
        info!(
            event = "storage.active_version_publish.active_row_written",
            source_path,
            version_label,
            vectors = vector_count,
            published_at_ms,
            phase = "active_row_write",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "active-version row written"
        );
        info!(
            event = "storage.active_version_publish.commit_starting",
            source_path,
            version_label,
            vectors = vector_count,
            published_at_ms,
            phase = "transaction_commit",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "active-version publish SQLite transaction commit starting"
        );
        if let Err(source) = tx.commit().map_err(|source| {
            storage_operation_error(format!("failed to commit active-version publish: {source}"))
        }) {
            error!(
                event = "storage.active_version_publish.failed",
                source_path,
                version_label,
                vectors = vector_count,
                published_at_ms,
                phase = "transaction_commit",
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "active-version publish failed"
            );
            return Err(source);
        }
        info!(
            event = "storage.active_version_publish.committed",
            source_path,
            version_label,
            vectors = vector_count,
            published_at_ms,
            phase = "transaction_commit",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "active-version publish SQLite transaction committed"
        );
        info!(
            event = "storage.active_version_publish.cache_swap_starting",
            source_path,
            version_label,
            vectors = vector_count,
            published_at_ms,
            phase = "cache_swap",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "active-version dense cache swap starting"
        );
        *cache = published_cache;
        info!(
            event = "storage.active_version_publish.cache_swap_completed",
            source_path,
            version_label,
            vectors = vector_count,
            published_at_ms,
            phase = "cache_swap",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "active-version dense cache swap completed"
        );
        info!(
            event = "storage.active_version_publish.completed",
            source_path,
            version_label,
            vectors = vector_count,
            published_at_ms,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "active-version publish completed"
        );

        Ok(published_at_ms)
    }
}

impl DenseVectorCache {
    /// Load and validate active dense vectors from SQLite into one flat row-major snapshot.
    fn load(connection: &Connection, dimension: usize) -> Result<Self, ApiError> {
        let started = Instant::now();
        let active_versions = load_active_document_versions(connection)?;
        let mut statement = connection
            .prepare(LOAD_DENSE_VECTORS_SQL)
            .map_err(|source| {
                storage_init_error(format!("failed to prepare dense cache load: {source}"))
            })?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, f64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })
            .map_err(|source| {
                storage_init_error(format!("failed to read dense vectors: {source}"))
            })?;
        let mut stored = Vec::new();
        for row in rows {
            let (unit_id, row_dimension, blob, norm, source_path, version_label) =
                row.map_err(|source| {
                    storage_init_error(format!("failed to read dense vector row: {source}"))
                })?;
            if row_dimension < 0 {
                return Err(storage_init_error(format!(
                    "dense vector {unit_id} has negative dimension {row_dimension}"
                )));
            }
            let vector = decode_vector_blob(&unit_id, &blob, row_dimension as usize, dimension)
                .map_err(storage_init_error)?;
            let norm = validate_norm(&unit_id, norm as f32).map_err(storage_init_error)?;
            stored.push(DenseCacheVector {
                source_path,
                version_label,
                dense: StoredDenseVector {
                    unit_id,
                    vector,
                    norm,
                },
            });
        }

        Ok(Self::from_vectors(
            dimension,
            stored,
            active_versions,
            current_time_ms()?,
            started.elapsed().as_millis() as u64,
        ))
    }

    /// Build a cache from already validated vectors and metadata.
    fn from_vectors(
        dimension: usize,
        stored: Vec<DenseCacheVector>,
        active_versions: Vec<ActiveDocumentVersion>,
        loaded_at_ms: u64,
        load_duration_ms: u64,
    ) -> Self {
        let mut vectors = Vec::with_capacity(stored.len() * dimension);
        let mut unit_ids = Vec::with_capacity(stored.len());
        let mut source_paths = Vec::with_capacity(stored.len());
        let mut version_labels = Vec::with_capacity(stored.len());
        let mut norms = Vec::with_capacity(stored.len());
        for value in stored {
            source_paths.push(value.source_path);
            version_labels.push(value.version_label);
            unit_ids.push(value.dense.unit_id);
            norms.push(value.dense.norm);
            vectors.extend(value.dense.vector);
        }
        let memory_bytes = vectors.len() * std::mem::size_of::<f32>()
            + norms.len() * std::mem::size_of::<f32>()
            + unit_ids.iter().map(String::len).sum::<usize>()
            + source_paths.iter().map(String::len).sum::<usize>()
            + version_labels.iter().map(String::len).sum::<usize>();

        Self {
            dimension,
            vectors,
            unit_ids,
            source_paths,
            version_labels,
            norms,
            active_versions,
            loaded_at_ms,
            load_duration_ms,
            memory_bytes,
        }
    }

    /// Return a new active snapshot where one source path points at the newly ingested version.
    fn with_published_source_version(
        &self,
        source_path: &str,
        version_label: &str,
        replacement: Vec<StoredDenseVector>,
        published_at_ms: u64,
    ) -> Result<Self, ApiError> {
        let started = Instant::now();
        let mut retained = self
            .unit_ids
            .iter()
            .enumerate()
            .filter_map(|(index, unit_id)| {
                if self.source_paths[index] == source_path {
                    return None;
                }

                let start = index * self.dimension;
                let end = start + self.dimension;
                Some(DenseCacheVector {
                    source_path: self.source_paths[index].clone(),
                    version_label: self.version_labels[index].clone(),
                    dense: StoredDenseVector {
                        unit_id: unit_id.clone(),
                        vector: self.vectors[start..end].to_vec(),
                        norm: self.norms[index],
                    },
                })
            })
            .collect::<Vec<_>>();

        retained.extend(replacement.into_iter().map(|dense| DenseCacheVector {
            source_path: source_path.to_string(),
            version_label: version_label.to_string(),
            dense,
        }));
        retained.sort_by(|left, right| left.dense.unit_id.cmp(&right.dense.unit_id));
        let mut active_versions = self
            .active_versions
            .iter()
            .filter(|active| active.source_path != source_path)
            .cloned()
            .collect::<Vec<_>>();
        active_versions.push(ActiveDocumentVersion {
            source_path: source_path.to_string(),
            version_label: version_label.to_string(),
        });
        active_versions.sort_by(|left, right| left.source_path.cmp(&right.source_path));

        Ok(Self::from_vectors(
            self.dimension,
            retained,
            active_versions,
            published_at_ms,
            started.elapsed().as_millis() as u64,
        ))
    }

    /// Rank cached dense vectors by exact cosine similarity with deterministic tie-breaking.
    fn search(&self, query: &StoredDenseVector, top_k: usize) -> Result<Vec<DenseMatch>, ApiError> {
        let mut matches = self
            .unit_ids
            .iter()
            .enumerate()
            .map(|(index, unit_id)| {
                let start = index * self.dimension;
                let end = start + self.dimension;
                let dot = self.vectors[start..end]
                    .iter()
                    .zip(query.vector.iter())
                    .map(|(left, right)| left * right)
                    .sum::<f32>();
                let similarity = dot / (query.norm * self.norms[index]);
                if !similarity.is_finite() {
                    return Err(storage_operation_error(format!(
                        "dense similarity for {unit_id} is non-finite"
                    )));
                }

                Ok(DenseMatch {
                    unit_id: unit_id.clone(),
                    similarity,
                    rank: 0,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        matches.sort_by(|left, right| {
            right
                .similarity
                .total_cmp(&left.similarity)
                .then_with(|| left.unit_id.cmp(&right.unit_id))
        });
        matches.truncate(top_k);
        for (index, matched) in matches.iter_mut().enumerate() {
            matched.rank = index + 1;
        }

        Ok(matches)
    }
}

/// Return the first-stage retrieval limit needed to fill the configured ColBERT pool.
fn first_stage_candidate_limit(top_k: u32, retrieval: &RetrievalConfig) -> usize {
    candidate_limit(top_k, retrieval.candidate_overfetch_multiplier)
        .max(retrieval.colbert_candidate_pool_size as usize)
}

/// Return the top-K overfetch size used before ColBERT pool expansion.
fn candidate_limit(top_k: u32, overfetch_multiplier: u32) -> usize {
    top_k.saturating_mul(overfetch_multiplier) as usize
}

/// Build a conservative FTS5 query from user text by discarding operators and quoting each term.
fn build_fts_query(query: &str) -> Option<String> {
    let terms = query
        .split(|value: char| !value.is_alphanumeric())
        .filter_map(|value| {
            let term = value.trim().to_lowercase();
            if term.is_empty() { None } else { Some(term) }
        })
        .collect::<Vec<_>>();
    if terms.is_empty() {
        return None;
    }

    Some(
        terms
            .into_iter()
            .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(" OR "),
    )
}

/// Fuse dense and BM25 candidates with reciprocal rank fusion.
///
/// The output score is only a fused rank signal, not a semantic similarity score.
fn fuse_matches(
    dense_matches: &[DenseMatch],
    bm25_matches: &[Bm25Match],
    top_k: usize,
    rrf_k: u32,
) -> Vec<FusedMatch> {
    let mut values = BTreeMap::<String, FusedMatch>::new();
    for matched in dense_matches {
        let entry = values
            .entry(matched.unit_id.clone())
            .or_insert_with(|| empty_fused_match(&matched.unit_id));
        entry.score += reciprocal_rank_score(rrf_k, matched.rank);
        entry.dense_rank = Some(matched.rank);
        entry.dense_similarity = Some(matched.similarity);
    }
    for matched in bm25_matches {
        let entry = values
            .entry(matched.unit_id.clone())
            .or_insert_with(|| empty_fused_match(&matched.unit_id));
        entry.score += reciprocal_rank_score(rrf_k, matched.rank);
        entry.bm25_rank = Some(matched.rank);
        entry.bm25_score = Some(matched.score);
    }

    let mut fused = values.into_values().collect::<Vec<_>>();
    fused.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.unit_id.cmp(&right.unit_id))
    });
    fused.truncate(top_k);
    for (index, matched) in fused.iter_mut().enumerate() {
        matched.rank = index + 1;
    }

    fused
}

/// Return the initial fused-candidate record for one unit id.
fn empty_fused_match(unit_id: &str) -> FusedMatch {
    FusedMatch {
        unit_id: unit_id.to_string(),
        score: 0.0,
        rank: 0,
        dense_rank: None,
        dense_similarity: None,
        bm25_rank: None,
        bm25_score: None,
    }
}

/// Return the reciprocal-rank contribution for one candidate rank using the configured RRF K constant.
fn reciprocal_rank_score(rrf_k: u32, rank: usize) -> f64 {
    1.0 / (rrf_k as f64 + rank as f64)
}

/// Create the SQLite schema through the explicit setup command, which is the only schema-creation path.
pub fn setup_storage(storage: &StorageConfig) -> Result<PathBuf, ApiError> {
    fs::create_dir_all(&storage.index_root).map_err(|source| ApiError::InternalIo {
        message: format!(
            "failed to create storage index root at {}: {source}",
            storage.index_root.display()
        ),
    })?;

    let db_path = database_path(storage);
    let connection = open_connection(&db_path)?;
    connection
        .execute_batch(STORAGE_SCHEMA_SQL)
        .map_err(|source| {
            storage_operation_error(format!("failed to create SQLite schema: {source}"))
        })?;
    validate_schema(&connection)?;

    Ok(db_path)
}

/// Return the service-owned SQLite database path from storage config.
fn database_path(storage: &StorageConfig) -> PathBuf {
    storage.index_root.join(DATABASE_FILE_NAME)
}

/// Open a SQLite connection with required pragmas enabled.
fn open_connection(db_path: &Path) -> Result<Connection, ApiError> {
    let connection = Connection::open(db_path).map_err(|source| {
        storage_init_error(format!(
            "failed to open SQLite database at {}: {source}",
            db_path.display()
        ))
    })?;
    connection
        .execute_batch(ENABLE_FOREIGN_KEYS_SQL)
        .map_err(|source| {
            storage_init_error(format!("failed to enable SQLite foreign keys: {source}"))
        })?;

    Ok(connection)
}

/// Load the durable active-version map that defines the search-visible corpus.
fn load_active_document_versions(
    connection: &Connection,
) -> Result<Vec<ActiveDocumentVersion>, ApiError> {
    let mut statement = connection
        .prepare(LOAD_ACTIVE_DOCUMENT_VERSIONS_SQL)
        .map_err(|source| {
            storage_init_error(format!("failed to prepare active-version load: {source}"))
        })?;
    let rows = statement
        .query_map([], |row| {
            Ok(ActiveDocumentVersion {
                source_path: row.get::<_, String>(0)?,
                version_label: row.get::<_, String>(1)?,
            })
        })
        .map_err(|source| {
            storage_init_error(format!("failed to read active document versions: {source}"))
        })?;
    let mut active_versions = Vec::new();
    for row in rows {
        let active = row.map_err(|source| {
            storage_init_error(format!("failed to read active-version row: {source}"))
        })?;
        if active.source_path.trim().is_empty() || active.version_label.trim().is_empty() {
            return Err(storage_init_error(
                "active document version contains empty source_path or version_label".to_string(),
            ));
        }
        active_versions.push(active);
    }

    Ok(active_versions)
}

impl QueriedDocumentVersion {
    /// Convert one SQLite document-version row into the admin-facing diagnostic record.
    fn to_record(
        &self,
        dense_vector_metadata: Vec<DenseVectorMetadataRecord>,
        colbert_vector_metadata: Vec<ColbertVectorMetadataRecord>,
    ) -> Result<DocumentVersionRecord, ApiError> {
        let diagnostics = serde_json::from_str(&self.diagnostics_json).map_err(|source| {
            storage_operation_error(format!(
                "document version {} {} has invalid diagnostics_json: {source}",
                self.source_path, self.version_label
            ))
        })?;

        Ok(DocumentVersionRecord {
            version_label: self.version_label.clone(),
            document_id: self.document_id.clone(),
            is_active: self.is_active,
            source_sha256: self.source_sha256.clone(),
            markdown_path: self.markdown_path.clone(),
            markdown_sha256: self.markdown_sha256.clone(),
            pdf_backend: self.pdf_backend.clone(),
            ocr_mode: self.ocr_mode.clone(),
            page_batch_size: optional_i64_to_u32(
                "document_versions.page_batch_size",
                self.page_batch_size,
            )?,
            units_ingested: i64_to_u32("document_versions.units_ingested", self.units_ingested)?,
            status: self.status.clone(),
            diagnostics,
            created_at_ms: i64_to_u64("document_versions.created_at_ms", self.created_at_ms)?,
            updated_at_ms: i64_to_u64("document_versions.updated_at_ms", self.updated_at_ms)?,
            dense_vector_metadata,
            colbert_vector_metadata,
        })
    }
}

/// Return whether one retained source-document version exists in durable storage.
fn document_version_exists(
    connection: &Connection,
    source_path: &str,
    version_label: &str,
) -> Result<bool, ApiError> {
    connection
        .query_row(
            DOCUMENT_VERSION_EXISTS_SQL,
            params![source_path, version_label],
            |_| Ok(()),
        )
        .optional()
        .map(|value| value.is_some())
        .map_err(|source| {
            storage_operation_error(format!(
                "failed to check document version {source_path} {version_label}: {source}"
            ))
        })
}

/// Load and validate dense vectors for one retained source-document version.
fn load_dense_vectors_for_version(
    connection: &Connection,
    source_path: &str,
    version_label: &str,
    dimension: usize,
) -> Result<Vec<StoredDenseVector>, ApiError> {
    let mut statement = connection
        .prepare(LOAD_DENSE_VECTORS_FOR_VERSION_SQL)
        .map_err(|source| {
            storage_operation_error(format!(
                "failed to prepare dense vectors for version {source_path} {version_label}: {source}"
            ))
        })?;
    let rows = statement
        .query_map(params![source_path, version_label], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, f64>(3)?,
            ))
        })
        .map_err(|source| {
            storage_operation_error(format!(
                "failed to read dense vectors for version {source_path} {version_label}: {source}"
            ))
        })?;
    let mut vectors = Vec::new();
    for row in rows {
        let (unit_id, row_dimension, blob, norm) = row.map_err(|source| {
            storage_operation_error(format!(
                "failed to read dense vector row for version {source_path} {version_label}: {source}"
            ))
        })?;
        if row_dimension < 0 {
            return Err(storage_operation_error(format!(
                "dense vector {unit_id} has negative dimension {row_dimension}"
            )));
        }
        let vector = decode_vector_blob(&unit_id, &blob, row_dimension as usize, dimension)
            .map_err(storage_operation_error)?;
        let norm = validate_norm(&unit_id, norm as f32).map_err(storage_operation_error)?;
        vectors.push(StoredDenseVector {
            unit_id,
            vector,
            norm,
        });
    }

    Ok(vectors)
}

/// Load distinct dense vector model metadata for one source-document version.
fn load_dense_vector_metadata_for_version(
    connection: &Connection,
    source_path: &str,
    version_label: &str,
) -> Result<Vec<DenseVectorMetadataRecord>, ApiError> {
    let mut statement = connection
        .prepare(LOAD_DENSE_VECTOR_METADATA_FOR_VERSION_SQL)
        .map_err(|source| {
            storage_operation_error(format!(
                "failed to prepare dense vector metadata for {source_path} {version_label}: {source}"
            ))
        })?;
    let rows = statement
        .query_map(params![source_path, version_label], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .map_err(|source| {
            storage_operation_error(format!(
                "failed to query dense vector metadata for {source_path} {version_label}: {source}"
            ))
        })?;
    let mut metadata = Vec::new();
    for row in rows {
        let (model_path, model_dimension, pooling, format, vector_count) =
            row.map_err(|source| {
                storage_operation_error(format!(
                    "failed to read dense vector metadata for {source_path} {version_label}: {source}"
                ))
            })?;
        metadata.push(DenseVectorMetadataRecord {
            model_path,
            model_dimension: i64_to_u32("dense_vectors.model_dimension", model_dimension)?,
            pooling,
            format,
            vector_count: i64_to_u32("dense_vectors vector count", vector_count)?,
        });
    }

    Ok(metadata)
}

/// Load distinct ColBERT vector model metadata for one source-document version.
fn load_colbert_vector_metadata_for_version(
    connection: &Connection,
    source_path: &str,
    version_label: &str,
) -> Result<Vec<ColbertVectorMetadataRecord>, ApiError> {
    let mut statement = connection
        .prepare(LOAD_COLBERT_VECTOR_METADATA_FOR_VERSION_SQL)
        .map_err(|source| {
            storage_operation_error(format!(
                "failed to prepare ColBERT vector metadata for {source_path} {version_label}: {source}"
            ))
        })?;
    let rows = statement
        .query_map(params![source_path, version_label], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .map_err(|source| {
            storage_operation_error(format!(
                "failed to query ColBERT vector metadata for {source_path} {version_label}: {source}"
            ))
        })?;
    let mut metadata = Vec::new();
    for row in rows {
        let (model_path, model_dimension, format, vector_count) = row.map_err(|source| {
            storage_operation_error(format!(
                "failed to read ColBERT vector metadata for {source_path} {version_label}: {source}"
            ))
        })?;
        metadata.push(ColbertVectorMetadataRecord {
            model_path,
            model_dimension: i64_to_u32(
                "colbert_document_vectors.model_dimension",
                model_dimension,
            )?,
            format,
            vector_count: i64_to_u32("colbert_document_vectors vector count", vector_count)?,
        });
    }

    Ok(metadata)
}

/// Convert a non-negative SQLite integer into an API u32 diagnostic value.
fn i64_to_u32(label: &str, value: i64) -> Result<u32, ApiError> {
    u32::try_from(value).map_err(|_| {
        storage_operation_error(format!(
            "{label} value {value} cannot be represented as u32"
        ))
    })
}

/// Convert a non-negative SQLite integer into an API u64 timestamp value.
fn i64_to_u64(label: &str, value: i64) -> Result<u64, ApiError> {
    u64::try_from(value).map_err(|_| {
        storage_operation_error(format!(
            "{label} value {value} cannot be represented as u64"
        ))
    })
}

/// Convert an optional SQLite integer into an optional API u32 diagnostic value.
fn optional_i64_to_u32(label: &str, value: Option<i64>) -> Result<Option<u32>, ApiError> {
    value.map(|inner| i64_to_u32(label, inner)).transpose()
}

/// Validate the durable SQLite contract before runtime operations proceed.
fn validate_schema(connection: &Connection) -> Result<(), ApiError> {
    validate_schema_version(connection)?;
    for table_name in [
        "document_versions",
        "active_document_versions",
        "units",
        "dense_vectors",
        "colbert_document_vectors",
        "units_fts",
    ] {
        validate_schema_object_exists(connection, table_name)?;
    }
    validate_table_columns(
        connection,
        DOCUMENT_VERSIONS_TABLE_INFO_SQL,
        DOCUMENT_VERSIONS_COLUMNS,
    )?;
    validate_table_columns(
        connection,
        ACTIVE_DOCUMENT_VERSIONS_TABLE_INFO_SQL,
        ACTIVE_DOCUMENT_VERSIONS_COLUMNS,
    )?;
    validate_table_columns(connection, UNITS_TABLE_INFO_SQL, UNITS_COLUMNS)?;
    validate_table_columns(
        connection,
        DENSE_VECTORS_TABLE_INFO_SQL,
        DENSE_VECTORS_COLUMNS,
    )?;
    validate_table_columns(
        connection,
        COLBERT_DOCUMENT_VECTORS_TABLE_INFO_SQL,
        COLBERT_DOCUMENT_VECTORS_COLUMNS,
    )?;
    validate_composite_foreign_key(
        connection,
        ACTIVE_DOCUMENT_VERSIONS_FOREIGN_KEYS_SQL,
        ACTIVE_DOCUMENT_VERSION_FOREIGN_KEY,
    )?;
    validate_foreign_key(
        connection,
        UNITS_FOREIGN_KEYS_SQL,
        UNITS_DOCUMENT_FOREIGN_KEY,
    )?;
    validate_foreign_key(
        connection,
        DENSE_VECTORS_FOREIGN_KEYS_SQL,
        DENSE_VECTORS_UNIT_FOREIGN_KEY,
    )?;
    validate_foreign_key(
        connection,
        COLBERT_DOCUMENT_VECTORS_FOREIGN_KEYS_SQL,
        COLBERT_DOCUMENT_VECTORS_UNIT_FOREIGN_KEY,
    )?;
    validate_named_unique_index(
        connection,
        DOCUMENT_VERSIONS_INDEX_LIST_SQL,
        DOCUMENT_VERSIONS_DOCUMENT_ID_INDEX_INFO_SQL,
        "document_versions",
        "idx_document_versions_document_id",
        &["document_id"],
    )?;
    validate_named_unique_index(
        connection,
        DOCUMENT_VERSIONS_INDEX_LIST_SQL,
        DOCUMENT_VERSIONS_SOURCE_VERSION_INDEX_INFO_SQL,
        "document_versions",
        "idx_document_versions_source_version",
        &["source_path", "version_label"],
    )?;
    validate_units_document_sequence_index(connection)?;
    validate_units_fts_definition(connection)?;

    Ok(())
}

/// Validate that the database was created by the current explicit setup schema.
fn validate_schema_version(connection: &Connection) -> Result<(), ApiError> {
    let version = connection
        .query_row(GET_SCHEMA_VERSION_SQL, [], |row| row.get::<_, i64>(0))
        .map_err(|source| {
            storage_init_error(format!("failed to read SQLite schema version: {source}"))
        })?;
    if version != EXPECTED_SCHEMA_VERSION {
        return Err(storage_init_error(format!(
            "SQLite schema version is {version}, expected {EXPECTED_SCHEMA_VERSION}; run --setup-storage"
        )));
    }

    Ok(())
}

/// Validate that one named schema object exists before deeper contract checks run.
fn validate_schema_object_exists(
    connection: &Connection,
    table_name: &str,
) -> Result<(), ApiError> {
    let exists = connection
        .query_row(SCHEMA_OBJECT_EXISTS_SQL, [table_name], |_| Ok(()))
        .optional()
        .map_err(|source| storage_init_error(format!("failed to inspect SQLite schema: {source}")))?
        .is_some();
    if !exists {
        return Err(ApiError::StorageInit {
            message: format!(
                "SQLite schema is missing required table {table_name}; run --setup-storage"
            ),
        });
    }

    Ok(())
}

/// Validate one table's declared columns against the service-owned schema contract.
fn validate_table_columns(
    connection: &Connection,
    table_info_sql: &str,
    expected: &[ColumnSpec],
) -> Result<(), ApiError> {
    let table_name = expected
        .first()
        .map(|value| value.table_name)
        .unwrap_or("unknown");
    let actual = load_table_columns(connection, table_info_sql, table_name)?;
    if actual.len() != expected.len() {
        return Err(storage_init_error(format!(
            "SQLite table {table_name} has {} columns, expected {}",
            actual.len(),
            expected.len()
        )));
    }
    for spec in expected {
        let Some(column) = actual.get(spec.name) else {
            return Err(storage_init_error(format!(
                "SQLite table {} is missing required column {}",
                spec.table_name, spec.name
            )));
        };
        validate_column_contract(spec, column)?;
    }
    for column_name in actual.keys() {
        if !expected.iter().any(|spec| spec.name == column_name) {
            return Err(storage_init_error(format!(
                "SQLite table {table_name} has unexpected column {column_name}"
            )));
        }
    }

    Ok(())
}

/// Load SQLite table_info rows into a name-keyed map for contract validation.
fn load_table_columns(
    connection: &Connection,
    table_info_sql: &str,
    table_name: &str,
) -> Result<BTreeMap<String, ColumnInfo>, ApiError> {
    let mut statement = connection.prepare(table_info_sql).map_err(|source| {
        storage_init_error(format!(
            "failed to prepare column inspection for {table_name}: {source}"
        ))
    })?;
    let rows = statement
        .query_map([], |row| {
            Ok(ColumnInfo {
                name: row.get::<_, String>(1)?,
                declared_type: row.get::<_, String>(2)?,
                not_null: row.get::<_, i64>(3)? != 0,
                primary_key_position: row.get::<_, i64>(5)?,
            })
        })
        .map_err(|source| {
            storage_init_error(format!(
                "failed to inspect SQLite table {table_name}: {source}"
            ))
        })?;
    let mut columns = BTreeMap::new();
    for row in rows {
        let column = row.map_err(|source| {
            storage_init_error(format!(
                "failed to read SQLite table_info row for {table_name}: {source}"
            ))
        })?;
        columns.insert(column.name.clone(), column);
    }

    Ok(columns)
}

/// Validate one column's type, nullability, and primary-key position.
fn validate_column_contract(spec: &ColumnSpec, column: &ColumnInfo) -> Result<(), ApiError> {
    if !column
        .declared_type
        .eq_ignore_ascii_case(spec.declared_type)
    {
        return Err(storage_init_error(format!(
            "SQLite column {}.{} has type {}, expected {}",
            spec.table_name, spec.name, column.declared_type, spec.declared_type
        )));
    }
    if spec.required && !column.not_null && column.primary_key_position == 0 {
        return Err(storage_init_error(format!(
            "SQLite column {}.{} is nullable, expected NOT NULL or PRIMARY KEY",
            spec.table_name, spec.name
        )));
    }
    if column.primary_key_position != spec.primary_key_position {
        return Err(storage_init_error(format!(
            "SQLite column {}.{} has primary-key position {}, expected {}",
            spec.table_name, spec.name, column.primary_key_position, spec.primary_key_position
        )));
    }

    Ok(())
}

/// Validate one expected single-column foreign key and its delete behavior.
fn validate_foreign_key(
    connection: &Connection,
    foreign_keys_sql: &str,
    expected: ForeignKeySpec,
) -> Result<(), ApiError> {
    let mut statement = connection.prepare(foreign_keys_sql).map_err(|source| {
        storage_init_error(format!(
            "failed to prepare foreign-key inspection for {}: {source}",
            expected.table_name
        ))
    })?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(6)?,
            ))
        })
        .map_err(|source| {
            storage_init_error(format!(
                "failed to inspect foreign keys for {}: {source}",
                expected.table_name
            ))
        })?;
    for row in rows {
        let (referenced_table, from_column, referenced_column, on_delete) =
            row.map_err(|source| {
                storage_init_error(format!(
                    "failed to read foreign-key row for {}: {source}",
                    expected.table_name
                ))
            })?;
        if referenced_table == expected.referenced_table
            && from_column == expected.from_column
            && referenced_column == expected.referenced_column
            && on_delete.eq_ignore_ascii_case(expected.on_delete)
        {
            return Ok(());
        }
    }

    Err(storage_init_error(format!(
        "SQLite table {} is missing foreign key {} -> {}.{} ON DELETE {}",
        expected.table_name,
        expected.from_column,
        expected.referenced_table,
        expected.referenced_column,
        expected.on_delete
    )))
}

/// Validate one expected composite foreign key and its ordered column mapping.
fn validate_composite_foreign_key(
    connection: &Connection,
    foreign_keys_sql: &str,
    expected: CompositeForeignKeySpec,
) -> Result<(), ApiError> {
    let mut statement = connection.prepare(foreign_keys_sql).map_err(|source| {
        storage_init_error(format!(
            "failed to prepare foreign-key inspection for {}: {source}",
            expected.table_name
        ))
    })?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .map_err(|source| {
            storage_init_error(format!(
                "failed to inspect foreign keys for {}: {source}",
                expected.table_name
            ))
        })?;
    let mut grouped = BTreeMap::<i64, Vec<(i64, String, String, String)>>::new();
    for row in rows {
        let (id, sequence, referenced_table, from_column, referenced_column) =
            row.map_err(|source| {
                storage_init_error(format!(
                    "failed to read foreign-key row for {}: {source}",
                    expected.table_name
                ))
            })?;
        grouped.entry(id).or_default().push((
            sequence,
            referenced_table,
            from_column,
            referenced_column,
        ));
    }

    for mut rows in grouped.into_values() {
        rows.sort_by_key(|(sequence, _, _, _)| *sequence);
        let referenced_table = rows
            .first()
            .map(|(_, table, _, _)| table.as_str())
            .unwrap_or_default();
        let from_columns = rows
            .iter()
            .map(|(_, _, from, _)| from.as_str())
            .collect::<Vec<_>>();
        let referenced_columns = rows
            .iter()
            .map(|(_, _, _, column)| column.as_str())
            .collect::<Vec<_>>();
        if referenced_table == expected.referenced_table
            && from_columns == expected.from_columns
            && referenced_columns == expected.referenced_columns
        {
            return Ok(());
        }
    }

    Err(storage_init_error(format!(
        "SQLite table {} is missing composite foreign key {:?} -> {}.{:?}",
        expected.table_name,
        expected.from_columns,
        expected.referenced_table,
        expected.referenced_columns
    )))
}

/// Validate one named unique index and its ordered column list.
fn validate_named_unique_index(
    connection: &Connection,
    index_list_sql: &str,
    index_info_sql: &str,
    table_name: &str,
    index_name: &str,
    expected_columns: &[&str],
) -> Result<(), ApiError> {
    let mut statement = connection.prepare(index_list_sql).map_err(|source| {
        storage_init_error(format!(
            "failed to prepare {table_name} index inspection: {source}"
        ))
    })?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(2)? != 0))
        })
        .map_err(|source| {
            storage_init_error(format!("failed to inspect {table_name} indexes: {source}"))
        })?;
    let mut found_unique_index = false;
    for row in rows {
        let (name, unique) = row.map_err(|source| {
            storage_init_error(format!("failed to read {table_name} index row: {source}"))
        })?;
        if name == index_name && unique {
            found_unique_index = true;
        }
    }
    if !found_unique_index {
        return Err(storage_init_error(format!(
            "SQLite table {table_name} is missing unique index {index_name}"
        )));
    }

    validate_index_columns(connection, index_info_sql, index_name, expected_columns)
}

/// Validate the unique index that preserves deterministic unit sequence per document.
fn validate_units_document_sequence_index(connection: &Connection) -> Result<(), ApiError> {
    validate_named_unique_index(
        connection,
        UNITS_INDEX_LIST_SQL,
        UNITS_DOCUMENT_SEQUENCE_INDEX_INFO_SQL,
        "units",
        "idx_units_document_sequence",
        &["document_id", "sequence"],
    )
}

/// Validate one index's ordered column list.
fn validate_index_columns(
    connection: &Connection,
    index_info_sql: &str,
    index_name: &str,
    expected_columns: &[&str],
) -> Result<(), ApiError> {
    let mut statement = connection.prepare(index_info_sql).map_err(|source| {
        storage_init_error(format!(
            "failed to prepare index inspection for {index_name}: {source}"
        ))
    })?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(2)?))
        })
        .map_err(|source| {
            storage_init_error(format!("failed to inspect index {index_name}: {source}"))
        })?;
    let mut columns = Vec::new();
    for row in rows {
        let (sequence, name) = row.map_err(|source| {
            storage_init_error(format!(
                "failed to read index row for {index_name}: {source}"
            ))
        })?;
        columns.push((sequence, name));
    }
    columns.sort_by_key(|(sequence, _)| *sequence);
    let actual_columns = columns
        .into_iter()
        .map(|(_, name)| name)
        .collect::<Vec<_>>();
    let expected = expected_columns
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    if actual_columns != expected {
        return Err(storage_init_error(format!(
            "SQLite index {index_name} has columns {:?}, expected {:?}",
            actual_columns, expected
        )));
    }

    Ok(())
}

/// Validate that units_fts is the expected external-content FTS5 table.
fn validate_units_fts_definition(connection: &Connection) -> Result<(), ApiError> {
    let sql = connection
        .query_row(SCHEMA_OBJECT_SQL_SQL, ["units_fts"], |row| {
            row.get::<_, String>(0)
        })
        .optional()
        .map_err(|source| {
            storage_init_error(format!("failed to inspect units_fts definition: {source}"))
        })?
        .ok_or_else(|| {
            storage_init_error("SQLite schema is missing units_fts definition".to_string())
        })?;
    if normalize_schema_sql(&sql) != normalize_schema_sql(EXPECTED_UNITS_FTS_SQL) {
        return Err(storage_init_error(format!(
            "SQLite units_fts definition is incompatible: {sql}"
        )));
    }

    Ok(())
}

/// Normalize SQLite DDL text for stable comparison across formatting differences.
fn normalize_schema_sql(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

/// Load one durable unit row, its metadata, and its persisted ColBERT document vectors.
fn load_unit(
    connection: &Connection,
    unit_id: &str,
    colbert_dimension: usize,
) -> Result<StoredUnit, ApiError> {
    let row = connection
        .query_row(LOAD_UNIT_SQL, [unit_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .optional()
        .map_err(|source| {
            storage_operation_error(format!("failed to load unit {unit_id}: {source}"))
        })?;
    let Some((unit_id, source_path, heading_path_json, page_numbers_json, content)) = row else {
        return Err(storage_operation_error(format!(
            "dense search matched missing unit row {unit_id}"
        )));
    };
    let colbert_vector = load_colbert_document_vector(connection, &unit_id, colbert_dimension)?;

    Ok(StoredUnit {
        unit_id,
        source_path,
        heading_path: decode_json_array("heading_path_json", &heading_path_json)?,
        page_numbers: decode_json_array("page_numbers_json", &page_numbers_json)?,
        content,
        colbert_token_count: colbert_vector.token_count,
        colbert_dimension: colbert_vector.dimension,
        colbert_vector: colbert_vector.vector,
    })
}

/// Load and validate one persisted ColBERT document token matrix for search-time MaxSim.
fn load_colbert_document_vector(
    connection: &Connection,
    unit_id: &str,
    expected_dimension: usize,
) -> Result<StoredColbertDocumentVector, ApiError> {
    let row = connection
        .query_row(LOAD_COLBERT_DOCUMENT_VECTOR_SQL, [unit_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .optional()
        .map_err(|source| {
            storage_operation_error(format!(
                "failed to load ColBERT document vector {unit_id}: {source}"
            ))
        })?;
    let Some((token_count, dimension, blob, model_dimension, format)) = row else {
        return Err(storage_operation_error(format!(
            "missing persisted ColBERT document vector for {unit_id}; re-ingest the source with the current schema"
        )));
    };
    if token_count <= 0 {
        return Err(storage_operation_error(format!(
            "ColBERT document vector {unit_id} has invalid token_count {token_count}"
        )));
    }
    if dimension <= 0 {
        return Err(storage_operation_error(format!(
            "ColBERT document vector {unit_id} has invalid dimension {dimension}"
        )));
    }
    if model_dimension <= 0 {
        return Err(storage_operation_error(format!(
            "ColBERT document vector {unit_id} has invalid model_dimension {model_dimension}"
        )));
    }

    let token_count = token_count as usize;
    let dimension = dimension as usize;
    let model_dimension = model_dimension as usize;
    if model_dimension != expected_dimension {
        return Err(storage_operation_error(format!(
            "ColBERT document vector {unit_id} has model_dimension {model_dimension}, expected {expected_dimension}"
        )));
    }
    if format != COLBERT_DOCUMENT_VECTOR_FORMAT {
        return Err(storage_operation_error(format!(
            "ColBERT document vector {unit_id} has format {format}, expected {COLBERT_DOCUMENT_VECTOR_FORMAT}"
        )));
    }
    let vector = decode_colbert_document_vector_blob(
        unit_id,
        &blob,
        token_count,
        dimension,
        expected_dimension,
    )
    .map_err(storage_operation_error)?;

    Ok(StoredColbertDocumentVector {
        unit_id: unit_id.to_string(),
        token_count,
        dimension,
        vector,
    })
}

/// Decode JSON array metadata from SQLite and treat malformed data as storage corruption.
fn decode_json_array<T>(label: &str, value: &str) -> Result<Vec<T>, ApiError>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_str(value).map_err(|source| {
        storage_operation_error(format!("invalid {label} metadata in unit row: {source}"))
    })
}

/// Insert the immutable durable document-version row for one successful ingest.
fn insert_document(
    tx: &rusqlite::Transaction<'_>,
    conversion: &DoclingConversionResult,
    version_label: &str,
    units_ingested: usize,
    document_id: &str,
    source_sha256: &str,
    markdown_sha256: &str,
    diagnostics_json: &str,
    timestamp_ms: u64,
) -> Result<(), ApiError> {
    tx.execute(
        INSERT_DOCUMENT_SQL,
        params![
            conversion.source.relative_path.display().to_string(),
            version_label,
            document_id,
            source_sha256,
            conversion.markdown_path.display().to_string(),
            markdown_sha256,
            &conversion.options.pdf_backend,
            &conversion.options.ocr_mode,
            Some(i64::from(conversion.options.page_batch_size)),
            units_ingested as i64,
            DOCUMENT_STATUS_INGESTED,
            diagnostics_json,
            timestamp_ms as i64,
            timestamp_ms as i64,
        ],
    )
    .map_err(|source| storage_operation_error(format!("failed to insert document: {source}")))?;

    Ok(())
}

/// Report one storage progress checkpoint when the operation stream requested it.
fn emit_ingest_storage_progress<F>(
    progress: &mut Option<F>,
    message: &'static str,
    current: u64,
    total: u64,
) -> Result<(), ApiError>
where
    F: FnMut(&'static str, u64, u64) -> Result<(), ApiError>,
{
    if let Some(progress) = progress {
        progress(message, current, total)?;
    }

    Ok(())
}

/// Return whether a unit persistence count should be written as a durable service-log checkpoint.
fn should_log_storage_checkpoint(current: usize, total: usize) -> bool {
    current == 1 || current == total || current % 10 == 0
}

/// Insert one retrieval unit and its external-content FTS row.
fn insert_unit(
    tx: &rusqlite::Transaction<'_>,
    unit: &RetrievalUnit,
    version_label: &str,
) -> Result<(), ApiError> {
    let heading_path_json = serde_json::to_string(&unit.heading_path).map_err(|source| {
        storage_operation_error(format!("failed to encode heading path: {source}"))
    })?;
    let page_numbers_json = serde_json::to_string(&unit.page_numbers).map_err(|source| {
        storage_operation_error(format!("failed to encode page numbers: {source}"))
    })?;
    tx.execute(
        INSERT_UNIT_SQL,
        params![
            &unit.unit_id,
            &unit.document_id,
            &unit.source_path,
            version_label,
            unit.sequence as i64,
            heading_path_json,
            page_numbers_json,
            unit.token_count as i64,
            &unit.content,
            unit.content.chars().count() as i64,
        ],
    )
    .map_err(|source| {
        storage_operation_error(format!("failed to insert unit {}: {source}", unit.unit_id))
    })?;
    let rowid = tx.last_insert_rowid();
    tx.execute(INSERT_UNIT_FTS_SQL, params![rowid, &unit.content])
        .map_err(|source| {
            storage_operation_error(format!(
                "failed to insert FTS row for {}: {source}",
                unit.unit_id
            ))
        })?;

    Ok(())
}

/// Insert one dense vector row using the approved blob and metadata contract.
fn insert_dense_vector(
    tx: &rusqlite::Transaction<'_>,
    vector: &StoredDenseVector,
    dense: &DenseModelConfig,
    timestamp_ms: u64,
) -> Result<(), ApiError> {
    tx.execute(
        INSERT_DENSE_VECTOR_SQL,
        params![
            &vector.unit_id,
            dense.dimension as i64,
            encode_vector_blob(&vector.vector),
            vector.norm as f64,
            dense.path.display().to_string(),
            dense.dimension as i64,
            &dense.pooling,
            DENSE_VECTOR_FORMAT,
            timestamp_ms as i64,
            timestamp_ms as i64,
        ],
    )
    .map_err(|source| {
        storage_operation_error(format!(
            "failed to insert dense vector {}: {source}",
            vector.unit_id
        ))
    })?;

    Ok(())
}

/// Insert one persisted ColBERT document token matrix for later search-time MaxSim.
fn insert_colbert_document_vector(
    tx: &rusqlite::Transaction<'_>,
    vector: &StoredColbertDocumentVector,
    colbert: &ColbertModelConfig,
    timestamp_ms: u64,
) -> Result<(), ApiError> {
    tx.execute(
        INSERT_COLBERT_DOCUMENT_VECTOR_SQL,
        params![
            &vector.unit_id,
            vector.token_count as i64,
            vector.dimension as i64,
            encode_vector_blob(&vector.vector),
            colbert.path.display().to_string(),
            colbert.dimension as i64,
            COLBERT_DOCUMENT_VECTOR_FORMAT,
            timestamp_ms as i64,
            timestamp_ms as i64,
        ],
    )
    .map_err(|source| {
        storage_operation_error(format!(
            "failed to insert ColBERT document vector {}: {source}",
            vector.unit_id
        ))
    })?;

    Ok(())
}

/// Build bounded JSON diagnostics for the document row.
fn build_ingest_diagnostics(conversion: &DoclingConversionResult) -> Result<String, ApiError> {
    serde_json::to_string(&serde_json::json!({
        "docling": {
            "args": conversion.args,
            "stdout": conversion.stdout,
            "stderr": conversion.stderr,
            "outputDir": conversion.output_dir.display().to_string(),
            "markdownPath": conversion.markdown_path.display().to_string()
        }
    }))
    .map_err(|source| {
        storage_operation_error(format!("failed to encode ingest diagnostics: {source}"))
    })
}

/// Validate one dense vector and compute the stored norm from raw values.
fn validate_vector(
    unit_id: String,
    vector: Vec<f32>,
    dimension: usize,
) -> Result<StoredDenseVector, String> {
    if vector.len() != dimension {
        return Err(format!(
            "dense vector {unit_id} has dimension {}, expected {dimension}",
            vector.len()
        ));
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(format!("dense vector {unit_id} contains non-finite values"));
    }

    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    let norm = validate_norm(&unit_id, norm)?;
    Ok(StoredDenseVector {
        unit_id,
        vector,
        norm,
    })
}

/// Validate a ColBERT document token matrix and preserve its row-major token layout.
fn validate_colbert_document_vector(
    vector: UnitColbertDocumentVector,
    expected_dimension: usize,
) -> Result<StoredColbertDocumentVector, ApiError> {
    if vector.token_count == 0 {
        return Err(storage_operation_error(format!(
            "ColBERT document vector {} has zero tokens",
            vector.unit_id
        )));
    }
    if vector.dimension != expected_dimension {
        return Err(storage_operation_error(format!(
            "ColBERT document vector {} has dimension {}, expected {}",
            vector.unit_id, vector.dimension, expected_dimension
        )));
    }
    let expected_values = vector
        .token_count
        .checked_mul(vector.dimension)
        .ok_or_else(|| {
            storage_operation_error(format!(
                "ColBERT document vector {} token matrix size overflow",
                vector.unit_id
            ))
        })?;
    if vector.vector.len() != expected_values {
        return Err(storage_operation_error(format!(
            "ColBERT document vector {} has {} values, expected {}",
            vector.unit_id,
            vector.vector.len(),
            expected_values
        )));
    }
    if vector.vector.iter().any(|value| !value.is_finite()) {
        return Err(storage_operation_error(format!(
            "ColBERT document vector {} contains non-finite values",
            vector.unit_id
        )));
    }

    Ok(StoredColbertDocumentVector {
        unit_id: vector.unit_id,
        token_count: vector.token_count,
        dimension: vector.dimension,
        vector: vector.vector,
    })
}

/// Validate a stored norm and return it in f32 form.
fn validate_norm(unit_id: &str, norm: f32) -> Result<f32, String> {
    if !norm.is_finite() || norm <= 0.0 {
        return Err(format!(
            "dense vector {unit_id} has invalid norm {norm}; expected finite nonzero norm"
        ));
    }

    Ok(norm)
}

/// Encode one vector as contiguous little-endian f32 bytes.
fn encode_vector_blob(vector: &[f32]) -> Vec<u8> {
    encode_f32_blob(vector)
}

/// Encode contiguous f32 values using the SQLite little-endian blob contract.
fn encode_f32_blob(vector: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(vector.len() * F32_BYTE_WIDTH);
    for value in vector {
        bytes.extend(value.to_le_bytes());
    }
    bytes
}

/// Decode and validate a little-endian f32 vector blob from SQLite.
fn decode_vector_blob(
    unit_id: &str,
    blob: &[u8],
    row_dimension: usize,
    expected_dimension: usize,
) -> Result<Vec<f32>, String> {
    if row_dimension != expected_dimension {
        return Err(format!(
            "dense vector {unit_id} has dimension {row_dimension}, expected {expected_dimension}"
        ));
    }
    let vector = decode_f32_blob(blob, expected_dimension, &format!("dense vector {unit_id}"))?;
    validate_vector(unit_id.to_string(), vector, expected_dimension).map(|value| value.vector)
}

/// Decode and validate a persisted ColBERT token-vector matrix blob from SQLite.
fn decode_colbert_document_vector_blob(
    unit_id: &str,
    blob: &[u8],
    token_count: usize,
    row_dimension: usize,
    expected_dimension: usize,
) -> Result<Vec<f32>, String> {
    if row_dimension != expected_dimension {
        return Err(format!(
            "ColBERT document vector {unit_id} has dimension {row_dimension}, expected {expected_dimension}"
        ));
    }
    let expected_values = token_count
        .checked_mul(expected_dimension)
        .ok_or_else(|| format!("ColBERT document vector {unit_id} token matrix size overflow"))?;
    let vector = decode_f32_blob(
        blob,
        expected_values,
        &format!("ColBERT document vector {unit_id}"),
    )?;
    validate_colbert_document_vector(
        UnitColbertDocumentVector {
            unit_id: unit_id.to_string(),
            token_count,
            dimension: expected_dimension,
            vector,
        },
        expected_dimension,
    )
    .map(|value| value.vector)
    .map_err(|source| source.to_string())
}

/// Decode contiguous little-endian f32 values before domain-specific vector validation.
fn decode_f32_blob(blob: &[u8], expected_values: usize, label: &str) -> Result<Vec<f32>, String> {
    let expected_bytes = expected_values
        .checked_mul(F32_BYTE_WIDTH)
        .ok_or_else(|| format!("{label} byte length overflow"))?;
    if blob.len() != expected_bytes {
        return Err(format!(
            "{label} has byte length {}, expected {expected_bytes}",
            blob.len()
        ));
    }

    let mut vector = Vec::with_capacity(expected_values);
    for bytes in blob.chunks_exact(F32_BYTE_WIDTH) {
        vector.push(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
    }
    Ok(vector)
}

/// Return a lowercase SHA-256 hex digest for durable document metadata.
fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut result = String::with_capacity(digest.len() * 2);
    for value in digest {
        result.push_str(&format!("{value:02x}"));
    }
    result
}

/// Allocate the source-document-scoped version label used for immutable ingest rows.
pub fn allocate_version_label() -> Result<String, ApiError> {
    format_utc_timestamp_ms(current_time_ms()?)
}

/// Build a durable document id that keeps old versions addressable after re-ingest.
pub fn build_versioned_document_id(source_path: &Path, version_label: &str) -> String {
    format!(
        "{}:version:{}",
        build_document_id(source_path),
        version_label.replace(':', "-")
    )
}

/// Format epoch milliseconds as an ISO-like UTC timestamp without adding a time dependency.
fn format_utc_timestamp_ms(epoch_ms: u64) -> Result<String, ApiError> {
    let total_seconds = epoch_ms / 1_000;
    let millis = epoch_ms % 1_000;
    let days = (total_seconds / 86_400) as i64;
    let seconds_of_day = total_seconds % 86_400;
    let (year, month, day) = civil_from_epoch_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;

    Ok(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z"
    ))
}

/// Convert days since the Unix epoch to the Gregorian UTC date components.
fn civil_from_epoch_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let shifted_days = days_since_epoch + 719_468;
    let era = if shifted_days >= 0 {
        shifted_days
    } else {
        shifted_days - 146_096
    } / 146_097;
    let day_of_era = shifted_days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year_of_era + era * 400 + if month <= 2 { 1 } else { 0 };

    (year, month as u32, day as u32)
}

/// Return current epoch milliseconds for durable timestamps and cache diagnostics.
fn current_time_ms() -> Result<u64, ApiError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as u64)
        .map_err(|source| ApiError::InternalIo {
            message: format!("system clock is before UNIX epoch: {source}"),
        })
}

/// Convert a storage initialization failure into the service error shape.
fn storage_init_error(message: String) -> ApiError {
    ApiError::StorageInit { message }
}

/// Convert a storage operation failure into the service error shape.
fn storage_operation_error(message: String) -> ApiError {
    ApiError::StorageOperation { message }
}
