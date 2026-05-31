use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use tracing::info;

use crate::{
    config::{DenseModelConfig, StorageConfig},
    docling::DoclingConversionResult,
    error::ApiError,
    units::{RetrievalUnit, build_document_id},
};

#[derive(Debug, Clone)]
pub struct StorageRuntime {
    db_path: PathBuf,
    dense_dimension: usize,
    cache: Arc<Mutex<DenseVectorCache>>,
}

#[derive(Debug, Clone)]
pub struct UnitDenseVector {
    pub unit_id: String,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone)]
struct DenseVectorCache {
    dimension: usize,
    vectors: Vec<f32>,
    unit_ids: Vec<String>,
    norms: Vec<f32>,
    loaded_at_ms: u64,
    load_duration_ms: u64,
    memory_bytes: usize,
}

#[derive(Debug, Clone)]
struct StoredDenseVector {
    unit_id: String,
    vector: Vec<f32>,
    norm: f32,
}

const DATABASE_FILE_NAME: &str = "data-store.sqlite3";
const DENSE_VECTOR_FORMAT: &str = "little_endian_f32";

impl StorageRuntime {
    /// Open existing storage, validate schema, and load the dense vector cache.
    pub fn open(storage: &StorageConfig, dense: &DenseModelConfig) -> Result<Self, ApiError> {
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

        info!(
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
            cache: Arc::new(Mutex::new(cache)),
        })
    }

    /// Return storage/cache readiness details for health diagnostics.
    pub fn health_details(&self) -> Vec<String> {
        match self.cache.lock() {
            Ok(cache) => vec![
                format!("sqlite database ready: {}", self.db_path.display()),
                format!(
                    "dense cache ready: vectors {}, dim {}, memory_bytes {}, loaded_at_ms {}, load_duration_ms {}",
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

    /// Persist one converted document and replace its entries in the dense cache.
    pub fn ingest_document(
        &self,
        conversion: &DoclingConversionResult,
        units: &[RetrievalUnit],
        vectors: Vec<UnitDenseVector>,
        dense: &DenseModelConfig,
    ) -> Result<(), ApiError> {
        if units.len() != vectors.len() {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "unit/vector count mismatch during ingest: units={}, vectors={}",
                    units.len(),
                    vectors.len()
                ),
            });
        }

        let stored_vectors = vectors
            .into_iter()
            .map(|value| {
                validate_vector(value.unit_id, value.vector, self.dense_dimension)
                    .map_err(storage_operation_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
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
        let document_id = build_document_id(&conversion.source.relative_path);
        if document_id.is_empty() {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "could not derive document id from source path {}",
                    conversion.source.relative_path.display()
                ),
            });
        }

        let mut connection = open_connection(&self.db_path)?;
        let tx = connection.transaction().map_err(|source| {
            storage_operation_error(format!("failed to begin ingest transaction: {source}"))
        })?;

        delete_existing_document(&tx, &conversion.source.relative_path.display().to_string())?;
        insert_document(
            &tx,
            conversion,
            units.len(),
            &document_id,
            &source_sha256,
            &markdown_sha256,
            &diagnostics,
            now_ms,
        )?;
        for (unit, vector) in units.iter().zip(stored_vectors.iter()) {
            insert_unit(&tx, unit)?;
            insert_dense_vector(&tx, vector, dense, now_ms)?;
        }

        tx.commit().map_err(|source| {
            storage_operation_error(format!("failed to commit ingest transaction: {source}"))
        })?;
        self.replace_document_cache(&document_id, stored_vectors)?;

        Ok(())
    }

    /// Replace all cache rows for one document prefix after SQLite commit succeeds.
    fn replace_document_cache(
        &self,
        document_id: &str,
        vectors: Vec<StoredDenseVector>,
    ) -> Result<(), ApiError> {
        let prefix = format!("{document_id}:unit:");
        let mut cache = self.cache.lock().map_err(|source| {
            storage_operation_error(format!("dense cache lock is poisoned: {source}"))
        })?;
        cache.replace_document_vectors(&prefix, vectors)
    }
}

impl DenseVectorCache {
    /// Load and validate all dense vectors from SQLite into a flat row-major cache.
    fn load(connection: &Connection, dimension: usize) -> Result<Self, ApiError> {
        let started = Instant::now();
        let mut statement = connection
            .prepare(
                "SELECT unit_id, dimension, vector_blob, vector_norm
                 FROM dense_vectors
                 ORDER BY unit_id ASC",
            )
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
                ))
            })
            .map_err(|source| {
                storage_init_error(format!("failed to read dense vectors: {source}"))
            })?;
        let mut stored = Vec::new();
        for row in rows {
            let (unit_id, row_dimension, blob, norm) = row.map_err(|source| {
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
            stored.push(StoredDenseVector {
                unit_id,
                vector,
                norm,
            });
        }

        Ok(Self::from_vectors(
            dimension,
            stored,
            current_time_ms()?,
            started.elapsed().as_millis() as u64,
        ))
    }

    /// Build a cache from already validated vectors and metadata.
    fn from_vectors(
        dimension: usize,
        stored: Vec<StoredDenseVector>,
        loaded_at_ms: u64,
        load_duration_ms: u64,
    ) -> Self {
        let mut vectors = Vec::with_capacity(stored.len() * dimension);
        let mut unit_ids = Vec::with_capacity(stored.len());
        let mut norms = Vec::with_capacity(stored.len());
        for value in stored {
            unit_ids.push(value.unit_id);
            norms.push(value.norm);
            vectors.extend(value.vector);
        }
        let memory_bytes = vectors.len() * std::mem::size_of::<f32>()
            + norms.len() * std::mem::size_of::<f32>()
            + unit_ids.iter().map(String::len).sum::<usize>();

        Self {
            dimension,
            vectors,
            unit_ids,
            norms,
            loaded_at_ms,
            load_duration_ms,
            memory_bytes,
        }
    }

    /// Replace all vectors matching a document unit-id prefix and keep deterministic ordering.
    fn replace_document_vectors(
        &mut self,
        prefix: &str,
        mut replacement: Vec<StoredDenseVector>,
    ) -> Result<(), ApiError> {
        let started = Instant::now();
        let mut retained = self
            .unit_ids
            .iter()
            .enumerate()
            .filter_map(|(index, unit_id)| {
                if unit_id.starts_with(prefix) {
                    return None;
                }

                let start = index * self.dimension;
                let end = start + self.dimension;
                Some(StoredDenseVector {
                    unit_id: unit_id.clone(),
                    vector: self.vectors[start..end].to_vec(),
                    norm: self.norms[index],
                })
            })
            .collect::<Vec<_>>();

        retained.append(&mut replacement);
        retained.sort_by(|left, right| left.unit_id.cmp(&right.unit_id));
        *self = Self::from_vectors(
            self.dimension,
            retained,
            current_time_ms()?,
            started.elapsed().as_millis() as u64,
        );

        Ok(())
    }
}

/// Create the SQLite schema through the explicit setup command.
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
        .execute_batch(
            "
            CREATE TABLE IF NOT EXISTS documents (
              document_id TEXT PRIMARY KEY,
              source_path TEXT NOT NULL UNIQUE,
              source_sha256 TEXT,
              markdown_path TEXT NOT NULL,
              markdown_sha256 TEXT,
              pdf_backend TEXT NOT NULL,
              ocr_mode TEXT NOT NULL,
              page_batch_size INTEGER,
              units_ingested INTEGER NOT NULL,
              status TEXT NOT NULL,
              diagnostics_json TEXT NOT NULL,
              created_at_ms INTEGER NOT NULL,
              updated_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS units (
              unit_id TEXT PRIMARY KEY,
              document_id TEXT NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
              source_path TEXT NOT NULL,
              sequence INTEGER NOT NULL,
              heading_path_json TEXT NOT NULL,
              page_numbers_json TEXT NOT NULL,
              token_count INTEGER NOT NULL,
              content TEXT NOT NULL,
              content_chars INTEGER NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_units_document_sequence
            ON units(document_id, sequence);

            CREATE TABLE IF NOT EXISTS dense_vectors (
              unit_id TEXT PRIMARY KEY REFERENCES units(unit_id) ON DELETE CASCADE,
              dimension INTEGER NOT NULL,
              vector_blob BLOB NOT NULL,
              vector_norm REAL NOT NULL,
              model_path TEXT NOT NULL,
              model_dimension INTEGER NOT NULL,
              pooling TEXT NOT NULL,
              format TEXT NOT NULL,
              created_at_ms INTEGER NOT NULL,
              updated_at_ms INTEGER NOT NULL
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS units_fts
            USING fts5(content, content='units', content_rowid='rowid');
            ",
        )
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
        .execute_batch("PRAGMA foreign_keys = ON;")
        .map_err(|source| {
            storage_init_error(format!("failed to enable SQLite foreign keys: {source}"))
        })?;

    Ok(connection)
}

/// Validate that all Phase 8 tables exist before runtime operations proceed.
fn validate_schema(connection: &Connection) -> Result<(), ApiError> {
    for table_name in ["documents", "units", "dense_vectors", "units_fts"] {
        let exists = connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE name = ?1 LIMIT 1",
                [table_name],
                |_| Ok(()),
            )
            .optional()
            .map_err(|source| {
                storage_init_error(format!("failed to inspect SQLite schema: {source}"))
            })?
            .is_some();
        if !exists {
            return Err(ApiError::StorageInit {
                message: format!(
                    "SQLite schema is missing required table {table_name}; run --setup-storage"
                ),
            });
        }
    }

    Ok(())
}

/// Delete any existing document rows and corresponding external-content FTS rows.
fn delete_existing_document(
    tx: &rusqlite::Transaction<'_>,
    source_path: &str,
) -> Result<(), ApiError> {
    tx.execute(
        "DELETE FROM units_fts
         WHERE rowid IN (
           SELECT units.rowid
           FROM units
           JOIN documents ON documents.document_id = units.document_id
           WHERE documents.source_path = ?1
         )",
        [source_path],
    )
    .map_err(|source| {
        storage_operation_error(format!("failed to delete existing FTS rows: {source}"))
    })?;
    tx.execute(
        "DELETE FROM documents WHERE source_path = ?1",
        [source_path],
    )
    .map_err(|source| {
        storage_operation_error(format!("failed to delete existing document: {source}"))
    })?;

    Ok(())
}

/// Insert the durable document row for one successful ingest.
fn insert_document(
    tx: &rusqlite::Transaction<'_>,
    conversion: &DoclingConversionResult,
    units_ingested: usize,
    document_id: &str,
    source_sha256: &str,
    markdown_sha256: &str,
    diagnostics_json: &str,
    timestamp_ms: u64,
) -> Result<(), ApiError> {
    tx.execute(
        "INSERT INTO documents (
           document_id, source_path, source_sha256, markdown_path, markdown_sha256,
           pdf_backend, ocr_mode, page_batch_size, units_ingested, status,
           diagnostics_json, created_at_ms, updated_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'ingested', ?10, ?11, ?12)",
        params![
            document_id,
            conversion.source.relative_path.display().to_string(),
            source_sha256,
            conversion.markdown_path.display().to_string(),
            markdown_sha256,
            &conversion.options.pdf_backend,
            &conversion.options.ocr_mode,
            conversion.options.page_batch_size.map(i64::from),
            units_ingested as i64,
            diagnostics_json,
            timestamp_ms as i64,
            timestamp_ms as i64,
        ],
    )
    .map_err(|source| storage_operation_error(format!("failed to insert document: {source}")))?;

    Ok(())
}

/// Insert one retrieval unit and its external-content FTS row.
fn insert_unit(tx: &rusqlite::Transaction<'_>, unit: &RetrievalUnit) -> Result<(), ApiError> {
    let heading_path_json = serde_json::to_string(&unit.heading_path).map_err(|source| {
        storage_operation_error(format!("failed to encode heading path: {source}"))
    })?;
    let page_numbers_json = serde_json::to_string(&unit.page_numbers).map_err(|source| {
        storage_operation_error(format!("failed to encode page numbers: {source}"))
    })?;
    tx.execute(
        "INSERT INTO units (
           unit_id, document_id, source_path, sequence, heading_path_json,
           page_numbers_json, token_count, content, content_chars
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            &unit.unit_id,
            &unit.document_id,
            &unit.source_path,
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
    tx.execute(
        "INSERT INTO units_fts(rowid, content) VALUES (?1, ?2)",
        params![rowid, &unit.content],
    )
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
        "INSERT INTO dense_vectors (
           unit_id, dimension, vector_blob, vector_norm, model_path,
           model_dimension, pooling, format, created_at_ms, updated_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
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
    let mut bytes = Vec::with_capacity(vector.len() * std::mem::size_of::<f32>());
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
    let expected_bytes = expected_dimension * std::mem::size_of::<f32>();
    if blob.len() != expected_bytes {
        return Err(format!(
            "dense vector {unit_id} has byte length {}, expected {expected_bytes}",
            blob.len()
        ));
    }

    let mut vector = Vec::with_capacity(expected_dimension);
    for bytes in blob.chunks_exact(std::mem::size_of::<f32>()) {
        vector.push(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
    }
    validate_vector(unit_id.to_string(), vector, expected_dimension).map(|value| value.vector)
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
