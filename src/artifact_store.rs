//! Write-once content-addressed artifact store (spec §16.3, §30.1, §32)
//! rooted at {index_root}/fabric/artifacts per decision D1.
//!
//! Blobs live at `artifacts/sha256/<first-2-hex>/<full-64-hex-hash>` and are
//! immutable once written: a second put of identical bytes is a no-op that
//! returns the existing reference, and a same-hash put whose on-disk size
//! disagrees is an explicit error, never a silent overwrite. New blobs are
//! written to a temp file in the destination shard directory and published
//! with an atomic rename, so a crash can never leave a partial blob at a
//! hashed path. Reads re-hash the returned bytes so corruption surfaces as an
//! explicit error instead of propagating downstream.
//!
//! JSON/JSONL helpers route through `crate::canonical` so the bytes stored on
//! disk are byte-for-byte the bytes that were hashed: a blob's hash can always
//! be re-verified directly against its file.

// Consumed from C4 onward; remove this allow when first wired.
#![allow(dead_code)]

use std::{
    fs,
    io::{self, BufReader, Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracing::{error, info};

use crate::error::ApiError;

/// Directory under `{index_root}/fabric/artifacts` naming the hash algorithm;
/// part of the persisted D1 layout contract and must never change for
/// already-written stores.
const HASH_ALGORITHM_DIR: &str = "sha256";

/// Length of a lowercase-hex SHA-256 digest; every blob filename has exactly
/// this length and the shard directory is its first two characters.
const HASH_HEX_LEN: usize = 64;

/// Process-wide counter making concurrent temp filenames unique within one
/// process; combined with the pid it is unique across processes too.
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Reference to one stored blob: the content hash that keys it, the absolute
/// filesystem path it lives at, and its size. This is the shape manifests
/// embed when they reference heavy artifacts by URI plus hash (spec §16.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ArtifactRef {
    /// Lowercase-hex SHA-256 of the stored bytes; the store key.
    pub(crate) hash: String,
    /// Absolute filesystem path of the blob as a string. Absoluteness derives
    /// from config validation: `storage.index_root` is required absolute.
    pub(crate) uri: String,
    /// Exact byte length of the stored blob.
    pub(crate) size_bytes: u64,
}

/// Hash bytes as they are consumed without retaining a second payload-sized copy.
struct HashingReader<R> {
    inner: R,
    digest: Sha256,
    bytes_read: u64,
    max_bytes: Option<u64>,
}

impl<R: Read> Read for HashingReader<R> {
    /// Enforce the declared byte ceiling even if a file grows after metadata inspection.
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let count = self.inner.read(buffer)?;
        self.bytes_read = self
            .bytes_read
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::other("artifact byte count overflow"))?;
        if self.max_bytes.is_some_and(|limit| self.bytes_read > limit) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "artifact exceeds its byte ceiling",
            ));
        }
        self.digest.update(&buffer[..count]);
        Ok(count)
    }
}

/// Write-once content-addressed blob store on the local filesystem (D1).
/// One instance per store root; all methods take `&self` and are safe under
/// concurrent use because publication is an atomic rename of immutable
/// content — two racing writers of the same hash produce identical bytes.
#[derive(Debug)]
pub(crate) struct ArtifactStore {
    /// `{index_root}/fabric/artifacts` — the root of the hash-sharded tree.
    root: PathBuf,
}

impl ArtifactStore {
    /// Query readers require an existing store and never create filesystem state.
    pub(crate) fn open_existing(index_root: &Path) -> Result<Self, ApiError> {
        let root = index_root.join("fabric").join("artifacts");
        let directory = root.join(HASH_ALGORITHM_DIR);
        let metadata = fs::metadata(&directory).map_err(|source| ApiError::StorageOperation {
            message: format!(
                "inspect artifact store directory {}: {source}",
                directory.display()
            ),
        })?;
        if !metadata.is_dir() {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "artifact store path is not a directory: {}",
                    directory.display()
                ),
            });
        }
        Ok(Self { root })
    }

    /// Open (creating if absent) the artifact store under
    /// `{index_root}/fabric/artifacts`. Callers pass `config.storage.index_root`;
    /// this constructor owns root creation and logs that boundary. It does not
    /// scan or validate existing blobs — reads verify integrity per blob.
    pub(crate) fn open(index_root: &Path) -> Result<Self, ApiError> {
        let root = index_root.join("fabric").join("artifacts");
        // Shard directories are created on demand at write time; only the
        // algorithm root is created here so an empty store is recognizable.
        let algorithm_root = root.join(HASH_ALGORITHM_DIR);
        fs::create_dir_all(&algorithm_root).map_err(|source| {
            error!(
                event = "artifact_store.open_failed",
                root = %root.display(),
                error = %source,
                "artifact store root creation failed"
            );
            ApiError::StorageInit {
                message: format!(
                    "failed to create artifact store root {}: {source}",
                    algorithm_root.display()
                ),
            }
        })?;
        tracing::debug!(
            event = "artifact_store.opened",
            root = %root.display(),
            "artifact store root ready"
        );
        Ok(Self { root })
    }

    /// Store raw bytes under their SHA-256 hash. Write-once semantics: if the
    /// blob already exists with the expected size the existing reference is
    /// returned without rewriting; an existing blob whose size disagrees with
    /// the incoming bytes is an explicit error (hash-collision suspect or
    /// on-disk corruption), never an overwrite.
    pub(crate) fn put_bytes(&self, bytes: &[u8]) -> Result<ArtifactRef, ApiError> {
        let started = Instant::now();
        let hash = crate::canonical::sha256_hex_bytes(bytes);
        let path = self.blob_path(&hash);
        let incoming_size = bytes.len() as u64;

        // Write-once fast path: the hash already has a blob. Size is the
        // cheap integrity proxy here; full byte verification happens on read.
        if let Ok(existing) = fs::metadata(&path) {
            if existing.len() == incoming_size {
                tracing::debug!(
                    event = "artifact_store.blob_deduplicated",
                    hash,
                    path = %path.display(),
                    size_bytes = incoming_size,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "blob already stored; write-once no-op"
                );
                return Ok(self.artifact_ref(hash, &path, incoming_size));
            }
            // Same hash, different size: either a SHA-256 collision (all but
            // impossible) or a corrupted/foreign file at the hashed path.
            // Either way the store must refuse rather than pick a winner.
            error!(
                event = "artifact_store.size_mismatch",
                hash,
                path = %path.display(),
                existing_size_bytes = existing.len(),
                incoming_size_bytes = incoming_size,
                "existing blob size disagrees with incoming bytes for the same hash"
            );
            return Err(ApiError::StorageOperation {
                message: format!(
                    "artifact store write-once violation for hash {hash} at {}: \
                     existing blob is {} bytes, incoming content is {} bytes",
                    path.display(),
                    existing.len(),
                    incoming_size
                ),
            });
        }

        // Start-boundary log before the fallible write: a crash mid-write
        // must leave durable evidence that this blob write was in flight
        // (the orphaned temp file alone is mute). The dedup fast path above
        // needs no start log — it performs no fallible write work.
        info!(
            event = "artifact_store.blob_write_started",
            hash,
            path = %path.display(),
            size_bytes = incoming_size,
            "blob write starting"
        );
        self.write_new_blob(&hash, &path, bytes)?;
        info!(
            event = "artifact_store.blob_written",
            hash,
            path = %path.display(),
            size_bytes = incoming_size,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "blob written"
        );
        Ok(self.artifact_ref(hash, &path, incoming_size))
    }

    /// Read a blob by hash and verify its integrity: the returned bytes are
    /// re-hashed and must reproduce the requested hash, so silent on-disk
    /// corruption becomes an explicit error at the read boundary.
    pub(crate) fn get_bytes(&self, hash: &str) -> Result<Vec<u8>, ApiError> {
        let started = Instant::now();
        self.validate_hash(hash)?;
        let path = self.blob_path(hash);
        // Start-boundary log so a read that hangs or dies (filesystem stall,
        // process kill) is attributable to this blob from the log alone.
        tracing::debug!(
            event = "artifact_store.blob_read_started",
            hash,
            path = %path.display(),
            "blob read starting"
        );
        let bytes = fs::read(&path).map_err(|source| {
            error!(
                event = "artifact_store.read_failed",
                hash,
                path = %path.display(),
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "blob read failed"
            );
            ApiError::StorageOperation {
                message: format!(
                    "failed to read artifact {hash} at {}: {source}",
                    path.display()
                ),
            }
        })?;
        // Integrity gate: content-addressed reads are only trustworthy if the
        // bytes still hash to their address.
        let computed = crate::canonical::sha256_hex_bytes(&bytes);
        if computed != hash {
            error!(
                event = "artifact_store.corruption_detected",
                expected_hash = hash,
                computed_hash = computed,
                path = %path.display(),
                size_bytes = bytes.len() as u64,
                "blob bytes no longer hash to their address"
            );
            return Err(ApiError::StorageOperation {
                message: format!(
                    "artifact store corruption: blob at {} was stored as {hash} \
                     but its {} bytes hash to {computed}",
                    path.display(),
                    bytes.len()
                ),
            });
        }
        tracing::debug!(
            event = "artifact_store.blob_read",
            hash,
            path = %path.display(),
            size_bytes = bytes.len() as u64,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "blob read and verified"
        );
        Ok(bytes)
    }

    /// Resolve a recorded artifact location by content identity, always reading
    /// from this store. A restored envelope may retain its original root path;
    /// that path must never become an arbitrary filesystem read authority.
    pub(crate) fn get_bytes_by_uri(&self, uri: &str) -> Result<Vec<u8>, ApiError> {
        let hash = self.hash_from_uri(uri)?;
        self.get_bytes(hash)
    }

    /// Consume a payload with bounded buffers, releasing the result only after
    /// all consumed bytes reproduce its content address. Callback side effects
    /// must remain provisional until this method succeeds; early parser returns
    /// still drain and verify the rest of the file.
    pub(crate) fn with_verified_reader<T>(
        &self,
        uri: &str,
        max_bytes: Option<u64>,
        read: impl FnOnce(&mut dyn Read) -> Result<T, ApiError>,
    ) -> Result<T, ApiError> {
        let hash = self.hash_from_uri(uri)?;
        let path = self.blob_path(hash);
        let started = Instant::now();
        tracing::debug!(event = "artifact_store.blob_read_started", hash,
            path = %path.display(), max_bytes, "streaming blob read starting");
        let result = (|| {
            let file = fs::File::open(&path).map_err(|source| ApiError::StorageOperation {
                message: format!("open artifact {hash}: {source}"),
            })?;
            let size = file
                .metadata()
                .map_err(|source| ApiError::StorageOperation {
                    message: format!("inspect artifact {hash}: {source}"),
                })?
                .len();
            if max_bytes.is_some_and(|limit| size > limit) {
                return Err(ApiError::StorageOperation {
                    message: format!(
                        "artifact {hash} has {size} bytes, exceeding ceiling {max_bytes:?}"
                    ),
                });
            }
            let mut reader = HashingReader {
                inner: file,
                digest: Sha256::new(),
                bytes_read: 0,
                max_bytes,
            };
            // Buffer above the digest so parsers requesting one byte at a time
            // still hash/read in blocks. Read-ahead bytes already enter the hash.
            let value = read(&mut BufReader::new(&mut reader))?;
            io::copy(&mut reader, &mut io::sink()).map_err(|source| {
                ApiError::StorageOperation {
                    message: format!("finish reading artifact {hash}: {source}"),
                }
            })?;
            let bytes_read = reader.bytes_read;
            let actual = format!("{:x}", reader.digest.finalize());
            if actual != hash {
                return Err(ApiError::StorageOperation {
                    message: format!("artifact store corruption: {hash} hashes to {actual}"),
                });
            }
            tracing::debug!(
                event = "artifact_store.blob_read",
                hash,
                size_bytes = bytes_read,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "streamed blob read and verified"
            );
            Ok(value)
        })();
        result.inspect_err(|source| {
            error!(event = "artifact_store.read_failed", hash,
            path = %path.display(), error = %source,
            elapsed_ms = started.elapsed().as_millis() as u64, "streamed artifact read failed")
        })
    }

    /// Pin a payload by streaming its integrity check instead of retaining its bytes.
    pub(crate) fn reference_for_uri(&self, uri: &str) -> Result<ArtifactRef, ApiError> {
        let hash = self.hash_from_uri(uri)?;
        let size = self.with_verified_reader(uri, None, |reader| {
            io::copy(reader, &mut io::sink()).map_err(|source| ApiError::StorageOperation {
                message: format!("read artifact reference {hash}: {source}"),
            })
        })?;
        Ok(self.artifact_ref(hash.to_owned(), &self.blob_path(hash), size))
    }

    /// Accept only the store's canonical sha256/shard/hash URI suffix. The
    /// historical root is metadata; only the validated hash selects local bytes.
    fn hash_from_uri<'uri>(&self, uri: &'uri str) -> Result<&'uri str, ApiError> {
        let path = Path::new(uri);
        let invalid = || ApiError::StorageOperation {
            message: format!("invalid content-addressed artifact URI: {uri:?}"),
        };
        if !path.is_absolute()
            || path
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir))
        {
            return Err(invalid());
        }
        let hash = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(invalid)?;
        self.validate_hash(hash)?;
        let shard = path.parent().ok_or_else(invalid)?;
        if shard.file_name().and_then(|name| name.to_str()) != Some(&hash[..2])
            || shard
                .parent()
                .and_then(Path::file_name)
                .and_then(|name| name.to_str())
                != Some(HASH_ALGORITHM_DIR)
        {
            return Err(invalid());
        }
        Ok(hash)
    }

    /// Report whether a blob exists for the given hash. A malformed hash is
    /// simply "not stored" — existence is a query, not a validation boundary.
    pub(crate) fn exists(&self, hash: &str) -> bool {
        if self.validate_hash(hash).is_err() {
            return false;
        }
        self.blob_path(hash).is_file()
    }

    /// Store one canonical JSON record (spec §16.3 "JSON for singular
    /// records"). Bytes come from `crate::canonical`, so the blob's hash is
    /// exactly the record's canonical content hash.
    pub(crate) fn put_json(&self, record: &Value) -> Result<ArtifactRef, ApiError> {
        let bytes = crate::canonical::canonical_json_bytes(record)?;
        self.put_bytes(&bytes)
    }

    /// Read one JSON record blob by hash. Integrity is byte-level (via
    /// `get_bytes`); a stored-but-unparseable blob is an explicit error.
    pub(crate) fn get_json(&self, hash: &str) -> Result<Value, ApiError> {
        let bytes = self.get_bytes(hash)?;
        serde_json::from_slice(&bytes).map_err(|source| ApiError::StorageOperation {
            message: format!("artifact {hash} is not valid JSON: {source}"),
        })
    }

    /// Store an ordered record set as a canonical JSONL blob (spec §16.3):
    /// canonical lines in the given order joined by single LF, no trailing
    /// LF. The blob's hash therefore equals
    /// `crate::canonical::jsonl_record_set_hash` over the same records, and
    /// record order is significant — it must match the ordering rule the
    /// manifest records for this artifact.
    pub(crate) fn put_jsonl(&self, records: &[Value]) -> Result<ArtifactRef, ApiError> {
        let bytes = crate::canonical::canonical_jsonl_bytes(records)?;
        self.put_bytes(&bytes)
    }

    /// Read a JSONL record-set blob by hash, returning records in stored
    /// order. The empty blob is the empty record set (mirror of
    /// `canonical_jsonl_bytes`); any unparseable line is an explicit error
    /// naming the offending line number.
    pub(crate) fn get_jsonl(&self, hash: &str) -> Result<Vec<Value>, ApiError> {
        let bytes = self.get_bytes(hash)?;
        // Canonical JSONL is defined over UTF-8 text; non-UTF-8 content means
        // this hash does not address a JSONL artifact.
        let text = std::str::from_utf8(&bytes).map_err(|source| ApiError::StorageOperation {
            message: format!("artifact {hash} is not valid UTF-8 JSONL: {source}"),
        })?;
        // Empty bytes are the canonical empty record set; splitting "" would
        // otherwise yield one empty line and a parse error.
        if text.is_empty() {
            return Ok(Vec::new());
        }
        text.split('\n')
            .enumerate()
            .map(|(index, line)| {
                serde_json::from_str(line).map_err(|source| ApiError::StorageOperation {
                    message: format!(
                        "artifact {hash} line {} is not valid JSON: {source}",
                        index + 1
                    ),
                })
            })
            .collect()
    }

    /// Compute the sharded blob path for a hash:
    /// `{root}/sha256/<first-2-hex>/<full-hash>` (D1 layout).
    fn blob_path(&self, hash: &str) -> PathBuf {
        self.root
            .join(HASH_ALGORITHM_DIR)
            .join(&hash[..2])
            .join(hash)
    }

    /// Reject hashes that are not exactly 64 lowercase-hex characters before
    /// they are used as path components. This is both a correctness check
    /// (self-produced hashes always pass) and a path-safety guard: no caller
    /// input can ever escape the shard tree via separators or `..`.
    fn validate_hash(&self, hash: &str) -> Result<(), ApiError> {
        let well_formed = hash.len() == HASH_HEX_LEN
            && hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        if well_formed {
            Ok(())
        } else {
            Err(ApiError::StorageOperation {
                message: format!(
                    "artifact hash is not a 64-character lowercase-hex SHA-256 digest: {hash:?}"
                ),
            })
        }
    }

    /// Build the reference for a stored blob. `blob_path` roots every path at
    /// the store root, which is absolute because config validation requires
    /// `storage.index_root` to be absolute.
    fn artifact_ref(&self, hash: String, path: &Path, size_bytes: u64) -> ArtifactRef {
        ArtifactRef {
            hash,
            uri: path.display().to_string(),
            size_bytes,
        }
    }

    /// Write a new blob crash-safely: bytes go to a uniquely named temp file
    /// in the destination shard directory, are fsynced, and are then published
    /// with an atomic same-directory rename. A crash at any point leaves at
    /// worst an orphan temp file, never a partial blob at a hashed path. Two
    /// racing writers of the same hash both rename identical content, so the
    /// last rename winning is harmless.
    fn write_new_blob(&self, hash: &str, path: &Path, bytes: &[u8]) -> Result<(), ApiError> {
        // Shard directories are created on demand so the tree only contains
        // shards that hold blobs.
        let shard_dir = path.parent().ok_or_else(|| ApiError::StorageOperation {
            // blob_path always produces root/sha256/<shard>/<hash>, so a
            // missing parent is an internal invariant violation, not an
            // expected filesystem condition.
            message: format!("artifact path {} has no parent directory", path.display()),
        })?;
        fs::create_dir_all(shard_dir)
            .map_err(|source| self.write_error(hash, path, "shard directory creation", &source))?;

        // pid + process-wide counter makes the temp name unique across
        // concurrent writers in this and other processes; the leading dot
        // keeps orphaned temp files visually distinct from published blobs.
        let temp_path = shard_dir.join(format!(
            ".{hash}.tmp-{}-{}",
            std::process::id(),
            TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let write_result = (|| -> Result<(), std::io::Error> {
            let mut file = fs::File::create(&temp_path)?;
            file.write_all(bytes)?;
            // Durability before visibility: the content is flushed to disk
            // before the rename makes the hashed path exist, so a published
            // blob can never be an empty or partial file after a crash.
            file.sync_all()?;
            Ok(())
        })();
        if let Err(source) = write_result {
            // Best-effort temp cleanup; the write error is what matters and
            // an orphaned dot-file is harmless.
            let _ = fs::remove_file(&temp_path);
            return Err(self.write_error(hash, path, "temp file write", &source));
        }
        if let Err(source) = fs::rename(&temp_path, path) {
            let _ = fs::remove_file(&temp_path);
            return Err(self.write_error(hash, path, "atomic rename", &source));
        }
        Ok(())
    }

    /// Log and wrap one blob-write failure with the boundary facts (hash,
    /// path, failed phase); never logs content.
    fn write_error(
        &self,
        hash: &str,
        path: &Path,
        phase: &str,
        source: &std::io::Error,
    ) -> ApiError {
        error!(
            event = "artifact_store.write_failed",
            hash,
            path = %path.display(),
            phase,
            error = %source,
            "blob write failed"
        );
        ApiError::StorageOperation {
            message: format!(
                "artifact store write of {hash} failed during {phase} at {}: {source}",
                path.display()
            ),
        }
    }
}
