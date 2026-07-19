//! Full-scan filesystem connector over the configured corpus root
//! (spec §9.1, §9.3): complete enumeration, staged acquisition bundles,
//! and a declared capability profile.
//!
//! The connector is an untrusted producer outside the canonical trust
//! boundary (§9.1): it never touches the hot plane, canonical storage, the
//! artifact store, or the event log, and its only writes are staged bundle
//! directories under its staging root. Everything it reports — hashes,
//! timestamps, sizes — is a claim the acquisition importer re-verifies.

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsStr,
    fs, io,
    path::{Path, PathBuf},
    time::{Instant, UNIX_EPOCH},
};

use tracing::{debug, error, info, warn};

use crate::{
    canonical,
    error::ApiError,
    model::{AcquisitionFailureClass, ConnectorCapabilityProfile, DetectionMode},
    primitives::{current_time_ms, format_utc_timestamp_ms},
};

use super::{
    AcquisitionBundleManifest, BUNDLE_MANIFEST_FILE_NAME, BUNDLE_SOURCE_FILE_NAME, FullScanOutcome,
    KnownLocationState, ScanError, ScanFailure, bundle_dir_for,
};

/// Connector identity name recorded in every manifest and capability
/// profile this connector emits.
const FILESYSTEM_CONNECTOR_NAME: &str = "filesystem";

/// Connector implementation version. Bump when scan or staging behavior
/// changes in a way downstream consumers can observe.
const FILESYSTEM_CONNECTOR_VERSION: &str = "1";

/// Source-system tag stamped on acquired locations. Distinct from the
/// connector name even though the values coincide today: the name
/// identifies this producer implementation, the source system classifies
/// where the bytes live.
const FILESYSTEM_SOURCE_SYSTEM: &str = "filesystem";

/// Full-scan connector that enumerates regular files under one corpus root
/// and stages new or changed files as acquisition bundles (spec §9.1).
///
/// Trust boundary: this type performs no canonical writes. Its only side
/// effects are bundle directories under `staging_root`; the importer
/// validates them before anything durable happens.
pub(crate) struct FilesystemConnector {
    /// Absolute directory whose regular files form this connector's scope.
    corpus_root: PathBuf,
    /// Absolute directory receiving staged bundles; disjoint from
    /// `corpus_root` so the scan never enumerates its own output.
    staging_root: PathBuf,
    /// Governance domain claimed on every staged manifest (external fact
    /// assigned at acquisition, spec §6).
    governance_domain: String,
    /// Canonical hash of the connector's effective configuration
    /// (corpus root path + governance domain), computed once at
    /// construction. Ties every AcquisitionRecord to the exact
    /// configuration that produced it.
    connector_config_hash: String,
    /// Statically declared capability profile (spec §9.3) with its
    /// `profile_hash` precomputed once at construction.
    capability_profile: ConnectorCapabilityProfile,
}

impl FilesystemConnector {
    /// Build a connector from its effective configuration, computing the
    /// config and capability-profile hashes once. Rejects parameter shapes
    /// that would break scan invariants: relative roots (native URIs must
    /// be absolute paths), a non-UTF-8 corpus root (unhashable and
    /// unrepresentable as a native URI prefix), an empty governance
    /// domain, and a staging root inside the corpus root (the scan would
    /// enumerate its own staged output).
    pub(crate) fn new(
        corpus_root: PathBuf,
        staging_root: PathBuf,
        governance_domain: String,
    ) -> Result<Self, ApiError> {
        if !corpus_root.is_absolute() {
            return Err(invalid_config(format!(
                "filesystem connector corpus root must be absolute: {}",
                corpus_root.display()
            )));
        }
        if !staging_root.is_absolute() {
            return Err(invalid_config(format!(
                "filesystem connector staging root must be absolute: {}",
                staging_root.display()
            )));
        }
        if staging_root.starts_with(&corpus_root) {
            return Err(invalid_config(format!(
                "filesystem connector staging root {} must not be inside the corpus root {}",
                staging_root.display(),
                corpus_root.display()
            )));
        }
        if governance_domain.is_empty() {
            return Err(invalid_config(
                "filesystem connector governance domain must not be empty".to_string(),
            ));
        }
        // The corpus root string participates in the config hash and
        // prefixes every native URI, so it must be valid UTF-8.
        let Some(corpus_root_str) = corpus_root.to_str() else {
            return Err(invalid_config(format!(
                "filesystem connector corpus root is not valid UTF-8: {}",
                corpus_root.display()
            )));
        };
        // Effective configuration hash: everything that changes what this
        // connector would acquire or claim. Field names are stable wire
        // names for the hash input, not a persisted shape.
        let connector_config_hash = canonical::canonical_sha256_hex(&serde_json::json!({
            "corpusRoot": corpus_root_str,
            "governanceDomain": governance_domain,
        }))?;
        Ok(Self {
            corpus_root,
            staging_root,
            governance_domain,
            connector_config_hash,
            capability_profile: build_capability_profile()?,
        })
    }

    /// Connector identity name for manifests and acquisition records.
    pub(crate) fn connector_name(&self) -> &'static str {
        FILESYSTEM_CONNECTOR_NAME
    }

    /// Connector implementation version claimed on every manifest.
    pub(crate) fn connector_version(&self) -> &'static str {
        FILESYSTEM_CONNECTOR_VERSION
    }

    /// Canonical hash of the connector's effective configuration.
    pub(crate) fn connector_config_hash(&self) -> &str {
        &self.connector_config_hash
    }

    /// Hash of the declared capability profile (spec §9.3), covering every
    /// profile field except the hash itself.
    // Manifests carry the hash via the connector's own staging path; this
    // accessor and capability_profile() below exist for the consumers that
    // persist or surface declared profiles (forensic snapshots at C9,
    // health/inspection at C10b). Remove the allows when first wired.
    #[allow(dead_code)]
    pub(crate) fn capability_profile_hash(&self) -> &str {
        &self.capability_profile.profile_hash
    }

    /// Source-system tag claimed on every staged manifest.
    pub(crate) fn source_system(&self) -> &'static str {
        FILESYSTEM_SOURCE_SYSTEM
    }

    /// Governance domain claimed on every staged manifest.
    pub(crate) fn governance_domain(&self) -> &str {
        &self.governance_domain
    }

    /// The connector's statically declared change-detection capability
    /// (spec §9.3). Cloned because callers persist and serialize the
    /// profile independently of the connector's lifetime.
    // See capability_profile_hash above: consumed when declared profiles are
    // persisted or surfaced (C9 snapshots, C10b health/inspection).
    #[allow(dead_code)]
    pub(crate) fn capability_profile(&self) -> ConnectorCapabilityProfile {
        self.capability_profile.clone()
    }

    /// Run one full scan of the corpus root (spec §9.3 `full_scan` mode):
    /// enumerate every regular file, stage a bundle for each new or
    /// changed file, and report the outcome.
    ///
    /// Scope and error policy:
    /// - Symlinks are never followed — every symlink (file or directory)
    ///   is skipped. Hidden (dot-prefixed) entries and non-regular files
    ///   are skipped.
    /// - Per-item read/stat/stage failures become `ScanFailure` entries
    ///   and the scan continues. A failed item is still enumerated: the
    ///   file was observed to exist, so its absence must not be inferred.
    /// - An unreadable subdirectory clears `enumeration_complete` (a
    ///   partial enumeration asserts nothing for deletion inference, spec
    ///   §11.1) but the scan continues elsewhere. Item-level failures do
    ///   NOT clear it: the scope was still fully enumerated.
    /// - A corpus-root precondition failure (unstattable, symlinked, or
    ///   unreadable root) fails the scan as a unit as
    ///   `ScanError::SourceSide` — the source itself was unreadable, which
    ///   the scheduler records durably. Local infrastructure failures
    ///   (staging setup) fail as `ScanError::Internal`.
    ///
    /// This function owns the scan diagnostic boundary: it logs start,
    /// terminal completion with counts, and terminal failure with elapsed
    /// time; per-item and per-directory problems are logged where they
    /// occur.
    pub(crate) fn full_scan(
        &self,
        known: &BTreeMap<String, KnownLocationState>,
    ) -> Result<FullScanOutcome, ScanError> {
        let scan_started = Instant::now();
        info!(
            event = "connector.filesystem.scan_started",
            corpus_root = %self.corpus_root.display(),
            known_locations = known.len(),
            "filesystem full scan starting"
        );

        match self.run_full_scan(known, scan_started) {
            Ok(outcome) => {
                info!(
                    event = "connector.filesystem.scan_completed",
                    corpus_root = %self.corpus_root.display(),
                    enumerated = outcome.enumerated_native_uris.len(),
                    staged = outcome.staged_bundle_dirs.len(),
                    skipped_unchanged = outcome.skipped_unchanged,
                    failures = outcome.failures.len(),
                    enumeration_complete = outcome.enumeration_complete,
                    elapsed_ms = outcome.elapsed_ms,
                    "filesystem full scan completed"
                );
                Ok(outcome)
            }
            Err(source) => {
                // Both arms are logged here at the terminal boundary; the
                // side that faulted is part of the diagnostic record.
                let (side, message) = match &source {
                    ScanError::SourceSide { detail, .. } => ("source", detail.clone()),
                    ScanError::Internal(error) => ("internal", error.to_string()),
                };
                error!(
                    event = "connector.filesystem.scan_failed",
                    corpus_root = %self.corpus_root.display(),
                    side,
                    error = %message,
                    elapsed_ms = scan_started.elapsed().as_millis() as u64,
                    "filesystem full scan failed"
                );
                Err(source)
            }
        }
    }

    /// Perform the scan work behind `full_scan`, which owns the terminal
    /// diagnostic boundary: walk the corpus root iteratively and
    /// accumulate the outcome.
    fn run_full_scan(
        &self,
        known: &BTreeMap<String, KnownLocationState>,
        scan_started: Instant,
    ) -> Result<FullScanOutcome, ScanError> {
        // Corpus-root precondition failures are SOURCE-side: the scope
        // itself could not be read, which is a recordable acquisition
        // outcome (spec §9.2), not an internal fault. A symlinked corpus
        // root would make the whole scan a symlink traversal; refuse it
        // rather than silently following.
        let root_metadata =
            fs::symlink_metadata(&self.corpus_root).map_err(|source| ScanError::SourceSide {
                failure_class: io_failure_class(source.kind()),
                detail: format!(
                    "filesystem connector cannot stat corpus root {}: {source}",
                    self.corpus_root.display()
                ),
            })?;
        if root_metadata.file_type().is_symlink() {
            return Err(ScanError::SourceSide {
                // Not an IO error kind, so the closed taxonomy's `Other`
                // is the honest class for a refused symlinked root.
                failure_class: AcquisitionFailureClass::Other,
                detail: format!(
                    "filesystem connector corpus root must not be a symlink: {}",
                    self.corpus_root.display()
                ),
            });
        }
        // The staging root must exist before any bundle write; creating it
        // here keeps the connector self-sufficient on a fresh index root.
        // Failure is INTERNAL: local staging infrastructure, not the source.
        fs::create_dir_all(&self.staging_root).map_err(|source| {
            ScanError::Internal(ApiError::InternalIo {
                message: format!(
                    "filesystem connector cannot create staging root {}: {source}",
                    self.staging_root.display()
                ),
            })
        })?;

        let mut state = ScanState {
            known,
            pending_dirs: Vec::new(),
            staged_bundle_dirs: Vec::new(),
            enumerated_native_uris: BTreeSet::new(),
            enumeration_complete: true,
            skipped_unchanged: 0,
            failures: Vec::new(),
            hidden_skipped: 0,
            symlink_skipped: 0,
            temp_seq: 0,
        };

        // Root readability is the scan's precondition: nothing has been
        // enumerated yet, so failure here fails the scan as a unit (as a
        // source-side outcome) instead of reporting an empty "partial"
        // enumeration.
        let root_reader =
            fs::read_dir(&self.corpus_root).map_err(|source| ScanError::SourceSide {
                failure_class: io_failure_class(source.kind()),
                detail: format!(
                    "filesystem connector cannot read corpus root {}: {source}",
                    self.corpus_root.display()
                ),
            })?;
        self.scan_directory(&self.corpus_root, root_reader, &mut state);

        // Iterative walk: subdirectory read failures degrade completeness
        // but never abort the scan.
        while let Some(dir) = state.pending_dirs.pop() {
            match fs::read_dir(&dir) {
                Ok(reader) => self.scan_directory(&dir, reader, &mut state),
                Err(source) => {
                    warn!(
                        event = "connector.filesystem.dir_unreadable",
                        path = %dir.display(),
                        error = %source,
                        "directory unreadable; enumeration is partial"
                    );
                    state.enumeration_complete = false;
                }
            }
        }

        // Hidden-entry skips are logged once per scan as a count so a
        // dot-file-heavy corpus does not flood the log.
        debug!(
            event = "connector.filesystem.hidden_skipped",
            count = state.hidden_skipped,
            "hidden (dot-prefixed) entries skipped this scan"
        );
        // Symlink skips are likewise summarized once per scan; the warn
        // level is kept (skipping is safety behavior worth surfacing) but
        // guarded so symlink-free scans stay silent. Individual paths are
        // logged at debug level where each skip occurs.
        if state.symlink_skipped > 0 {
            warn!(
                event = "connector.filesystem.symlink_skipped",
                count = state.symlink_skipped,
                "symlinks skipped this scan; the filesystem connector never follows symlinks"
            );
        }

        Ok(FullScanOutcome {
            staged_bundle_dirs: state.staged_bundle_dirs,
            enumerated_native_uris: state.enumerated_native_uris,
            enumeration_complete: state.enumeration_complete,
            skipped_unchanged: state.skipped_unchanged,
            failures: state.failures,
            elapsed_ms: scan_started.elapsed().as_millis() as u64,
        })
    }

    /// Classify and process every entry of one readable directory:
    /// queue subdirectories, skip hidden entries, symlinks, and
    /// non-regular files, and scan regular files.
    fn scan_directory(&self, dir: &Path, reader: fs::ReadDir, state: &mut ScanState<'_>) {
        for entry_result in reader {
            let entry = match entry_result {
                Ok(entry) => entry,
                Err(source) => {
                    // An entry we could not even name may hide an entire
                    // subtree, so completeness is lost.
                    warn!(
                        event = "connector.filesystem.dir_entry_unreadable",
                        dir = %dir.display(),
                        error = %source,
                        "directory entry unreadable; enumeration is partial"
                    );
                    state.enumeration_complete = false;
                    continue;
                }
            };
            // Hidden check precedes the symlink check so hidden symlinks
            // count as hidden skips instead of producing warn noise.
            if entry.file_name().as_encoded_bytes().starts_with(b".") {
                state.hidden_skipped += 1;
                continue;
            }
            let path = entry.path();
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(source) => {
                    // Unclassifiable entry: it might have been a directory
                    // whose subtree we now miss, so completeness is lost;
                    // the entry itself is also a per-item failure.
                    warn!(
                        event = "connector.filesystem.entry_type_unreadable",
                        path = %path.display(),
                        error = %source,
                        "entry type unreadable; enumeration is partial"
                    );
                    state.enumeration_complete = false;
                    state.failures.push(ScanFailure {
                        // Lossy rendering is for failure reporting only; it
                        // never names a staged bundle.
                        native_uri: path.to_string_lossy().into_owned(),
                        failure_class: io_failure_class(source.kind()),
                        detail: format!("cannot determine entry type: {source}"),
                    });
                    continue;
                }
            };
            if file_type.is_symlink() {
                // Never followed: a symlink could escape the corpus root or
                // create traversal cycles, and its target is not this
                // connector's claim to make. Counted per scan (summarized
                // once at warn level) so a symlink-heavy corpus does not
                // flood the log; the individual path stays visible at debug.
                debug!(
                    event = "connector.filesystem.symlink_skipped",
                    path = %path.display(),
                    "symlink skipped; the filesystem connector never follows symlinks"
                );
                state.symlink_skipped += 1;
                continue;
            }
            if file_type.is_dir() {
                state.pending_dirs.push(path);
                continue;
            }
            if !file_type.is_file() {
                // FIFOs, sockets, and devices are out of scope: reading
                // them can block forever and they are not documents.
                debug!(
                    event = "connector.filesystem.non_regular_skipped",
                    path = %path.display(),
                    "non-regular file skipped"
                );
                continue;
            }
            self.scan_regular_file(&entry, &path, state);
        }
    }

    /// Prescreen one regular file against its known state and stage a
    /// bundle when it is new or changed, recording the result in the scan
    /// state. Every observed file — skipped, staged, or failed — is
    /// enumerated, because it was seen to exist and its absence must not
    /// be inferred by deletion inference (spec §11.1).
    fn scan_regular_file(&self, entry: &fs::DirEntry, path: &Path, state: &mut ScanState<'_>) {
        // Item timing starts here so failure logs carry the measured cost
        // of the failed attempt (prescreen + read + stage, as far as it got).
        let item_started = Instant::now();
        // The native URI is the file's absolute path; a non-UTF-8 path has
        // no faithful string form, so the file is reported as failed under
        // a lossy rendering rather than silently mangled.
        let Some(native_uri) = path.to_str() else {
            let lossy_uri = path.to_string_lossy().into_owned();
            warn!(
                event = "connector.filesystem.item_failed",
                native_uri = %lossy_uri,
                failure_class = "other",
                elapsed_ms = item_started.elapsed().as_millis() as u64,
                "file path is not valid UTF-8; item failed"
            );
            state.enumerated_native_uris.insert(lossy_uri.clone());
            state.failures.push(ScanFailure {
                native_uri: lossy_uri,
                failure_class: AcquisitionFailureClass::Other,
                detail: "file path is not valid UTF-8".to_string(),
            });
            return;
        };
        let native_uri = native_uri.to_string();

        match self.process_regular_file(entry, path, &native_uri, state.known, &mut state.temp_seq)
        {
            Ok(FileScanAction::Unchanged) => {
                state.skipped_unchanged += 1;
                state.enumerated_native_uris.insert(native_uri);
            }
            Ok(FileScanAction::Staged(bundle_dir)) => {
                state.staged_bundle_dirs.push(bundle_dir);
                state.enumerated_native_uris.insert(native_uri);
            }
            Err(item_error) => {
                warn!(
                    event = "connector.filesystem.item_failed",
                    native_uri = %native_uri,
                    failure_class = ?item_error.class,
                    error = %item_error.detail,
                    elapsed_ms = item_started.elapsed().as_millis() as u64,
                    "item acquisition failed; scan continues"
                );
                state.enumerated_native_uris.insert(native_uri.clone());
                state.failures.push(ScanFailure {
                    native_uri,
                    failure_class: item_error.class,
                    detail: item_error.detail,
                });
            }
        }
    }

    /// Decide one regular file's fate: skip it when its (mtime, size)
    /// claims match the known state — a prescreen only, since content
    /// identity is verified at import time by hashing the staged bytes —
    /// or stage an acquisition bundle for it.
    fn process_regular_file(
        &self,
        entry: &fs::DirEntry,
        path: &Path,
        native_uri: &str,
        known: &BTreeMap<String, KnownLocationState>,
        temp_seq: &mut u64,
    ) -> Result<FileScanAction, ItemError> {
        // DirEntry::metadata never traverses symlinks, and the entry was
        // already classified as a regular file.
        let metadata = entry
            .metadata()
            .map_err(|source| ItemError::io("stat failed", &source))?;
        let size_bytes = metadata.len();
        let modified = metadata
            .modified()
            .map_err(|source| ItemError::io("modification time unavailable", &source))?;
        // Pre-epoch mtimes cannot be rendered by the fabric's UTC
        // formatter and indicate a pathological file; fail the item rather
        // than inventing a timestamp.
        let modified_since_epoch = modified
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ItemError::other("modification time is before the UNIX epoch"))?;
        let mtime_ms = u64::try_from(modified_since_epoch.as_millis())
            .map_err(|_| ItemError::other("modification time overflows u64 milliseconds"))?;
        let mtime_ms_signed = i64::try_from(mtime_ms)
            .map_err(|_| ItemError::other("modification time overflows i64 milliseconds"))?;

        // Prescreen: an exact (mtime, size) match against the last
        // successful acquisition means the file is claimed unchanged and
        // only enumerated, not re-staged.
        if let Some(known_state) = known.get(native_uri)
            && known_state.native_modified_at_ms == mtime_ms_signed
            && known_state.size_bytes == size_bytes
        {
            return Ok(FileScanAction::Unchanged);
        }

        let bundle_dir = self.stage_bundle(path, native_uri, mtime_ms, temp_seq)?;
        Ok(FileScanAction::Staged(bundle_dir))
    }

    /// Stage one acquisition bundle: read the file, build its manifest
    /// from the bytes actually staged, write both into a sibling temp
    /// directory, then atomically promote the temp directory to the
    /// contract bundle name. A partially written bundle is therefore never
    /// visible under the contract name, and re-staging a coalesced
    /// location replaces its previous, not-yet-imported bundle.
    fn stage_bundle(
        &self,
        path: &Path,
        native_uri: &str,
        mtime_ms: u64,
        temp_seq: &mut u64,
    ) -> Result<PathBuf, ItemError> {
        let item_started = Instant::now();
        let bytes = fs::read(path).map_err(|source| ItemError::io("read failed", &source))?;

        // Manifest claims describe the bytes actually staged, not the stat
        // results: the file may change between stat and read, and the
        // importer verifies the hash against source.bin.
        let claimed_source_hash = canonical::sha256_hex_bytes(&bytes);
        let acquired_at = format_utc_timestamp_ms(
            current_time_ms()
                .map_err(|source| ItemError::internal("clock read failed", &source))?,
        )
        .map_err(|source| ItemError::internal("timestamp formatting failed", &source))?;
        let native_modified_at = format_utc_timestamp_ms(mtime_ms)
            .map_err(|source| ItemError::internal("mtime formatting failed", &source))?;

        // native_id is the corpus-relative path; the walk started at the
        // corpus root, so the prefix always strips.
        let native_id = path
            .strip_prefix(&self.corpus_root)
            .map_err(|_| ItemError::other("file path escaped the corpus root"))?
            .to_str()
            .ok_or_else(|| ItemError::other("corpus-relative path is not valid UTF-8"))?
            .to_string();

        let manifest = AcquisitionBundleManifest {
            connector_name: FILESYSTEM_CONNECTOR_NAME.to_string(),
            connector_version: FILESYSTEM_CONNECTOR_VERSION.to_string(),
            connector_config_hash: self.connector_config_hash.clone(),
            capability_profile_hash: self.capability_profile.profile_hash.clone(),
            source_system: FILESYSTEM_SOURCE_SYSTEM.to_string(),
            native_uri: native_uri.to_string(),
            native_id: Some(native_id),
            native_modified_at: Some(native_modified_at),
            governance_domain: self.governance_domain.clone(),
            claimed_source_hash,
            size_bytes: bytes.len() as u64,
            acquired_at,
            // Acquisition cost of this one item: read plus hash; staging
            // writes happen after the manifest is sealed.
            elapsed_ms: item_started.elapsed().as_millis() as u64,
        };
        // Plain (non-canonical) serialization is the staged-manifest
        // contract: the manifest is an untrusted claim, not hash input.
        let manifest_bytes = serde_json::to_vec(&manifest).map_err(|source| {
            ItemError::other(format!("manifest serialization failed: {source}"))
        })?;

        let bundle_dir = bundle_dir_for(&self.staging_root, native_uri);
        // Bundle names are exactly 64 hex chars, so a suffixed temp name
        // can never collide with a contract bundle name. The pid+sequence
        // suffix keeps concurrent or crashed scans from sharing a temp dir.
        let bundle_name = bundle_dir
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(|| ItemError::other("bundle directory has no valid name"))?;
        *temp_seq += 1;
        let temp_dir = self.staging_root.join(format!(
            "{bundle_name}.tmp-{}-{temp_seq}",
            std::process::id()
        ));

        if let Err(item_error) =
            write_and_promote_bundle(&temp_dir, &bundle_dir, &bytes, &manifest_bytes)
        {
            // Best-effort cleanup so a failed staging attempt does not
            // leave temp litter; the failure being reported is the write
            // error, not the cleanup.
            if temp_dir.exists()
                && let Err(cleanup_error) = fs::remove_dir_all(&temp_dir)
            {
                warn!(
                    event = "connector.filesystem.temp_cleanup_failed",
                    path = %temp_dir.display(),
                    error = %cleanup_error,
                    "temp bundle directory cleanup failed after staging error"
                );
            }
            return Err(item_error);
        }
        // Per-item staging success checkpoint. Debug level bounds the
        // volume on large corpora; the scan-level summary stays the
        // operator's primary signal.
        debug!(
            event = "connector.filesystem.item_staged",
            native_uri,
            bundle_dir = %bundle_dir.display(),
            size_bytes = bytes.len() as u64,
            elapsed_ms = item_started.elapsed().as_millis() as u64,
            "acquisition bundle staged"
        );
        Ok(bundle_dir)
    }
}

/// Write the bundle files into `temp_dir`, then replace `bundle_dir` with
/// it. The remove-then-rename pair is not a single atomic step, but the
/// invariant it guarantees is the one the contract needs: the contract
/// name is only ever absent or a complete bundle, never partial.
fn write_and_promote_bundle(
    temp_dir: &Path,
    bundle_dir: &Path,
    source_bytes: &[u8],
    manifest_bytes: &[u8],
) -> Result<(), ItemError> {
    // A stale temp dir can only exist after a pid+sequence collision with
    // a crashed scan; remove it so leftover files cannot leak into this
    // bundle.
    if temp_dir.exists() {
        fs::remove_dir_all(temp_dir)
            .map_err(|source| ItemError::io("stale temp bundle dir removal failed", &source))?;
    }
    fs::create_dir(temp_dir)
        .map_err(|source| ItemError::io("temp bundle dir creation failed", &source))?;
    fs::write(temp_dir.join(BUNDLE_SOURCE_FILE_NAME), source_bytes)
        .map_err(|source| ItemError::io("source.bin write failed", &source))?;
    fs::write(temp_dir.join(BUNDLE_MANIFEST_FILE_NAME), manifest_bytes)
        .map_err(|source| ItemError::io("manifest.json write failed", &source))?;
    // Latest-state coalescing (spec §9.4 rule 2): a previous bundle for
    // this location is superseded, so it is removed before the rename —
    // renaming onto a non-empty directory would fail.
    if bundle_dir.exists() {
        fs::remove_dir_all(bundle_dir)
            .map_err(|source| ItemError::io("stale bundle dir removal failed", &source))?;
    }
    fs::rename(temp_dir, bundle_dir)
        .map_err(|source| ItemError::io("bundle dir rename failed", &source))?;
    Ok(())
}

/// Build the connector's statically declared capability profile (spec
/// §9.3) with its hash computed over every field except the hash itself.
///
/// Declared facts: full-scan detection over a walkable tree; no change
/// feed, so no explicit delete events; the walk observes every regular
/// file in scope, so enumeration is complete; and NO native versioning —
/// filesystems have no version API, and mtime is claimed metadata used
/// only for change prescreening, never as a version.
fn build_capability_profile() -> Result<ConnectorCapabilityProfile, ApiError> {
    let mut profile = ConnectorCapabilityProfile {
        connector_name: FILESYSTEM_CONNECTOR_NAME.to_string(),
        connector_version: FILESYSTEM_CONNECTOR_VERSION.to_string(),
        detection_mode: DetectionMode::FullScan,
        supports_explicit_delete_events: false,
        supports_complete_enumeration: true,
        supports_native_versioning: false,
        provider_constraints: None,
        profile_hash: String::new(),
    };
    profile.profile_hash = capability_profile_hash_of(&profile)?;
    Ok(profile)
}

/// Canonical hash over a capability profile's fields excluding
/// `profile_hash` itself (the shared self-hash pattern in
/// `crate::canonical`); the model struct stays the single source of the
/// hashed shape.
fn capability_profile_hash_of(profile: &ConnectorCapabilityProfile) -> Result<String, ApiError> {
    canonical::canonical_sha256_hex_without_field(profile, canonical::PROFILE_HASH_JSON_KEY)
}

/// Mutable accumulator for one scan pass, threaded through the walk so the
/// per-directory and per-file helpers stay free of long parameter lists.
struct ScanState<'a> {
    /// Last-known per-location claims used for the unchanged prescreen.
    known: &'a BTreeMap<String, KnownLocationState>,
    /// Directories discovered but not yet read (LIFO walk order).
    pending_dirs: Vec<PathBuf>,
    staged_bundle_dirs: Vec<PathBuf>,
    enumerated_native_uris: BTreeSet<String>,
    /// Cleared by any directory-level failure; item-level failures leave
    /// it set because the scope was still fully enumerated.
    enumeration_complete: bool,
    skipped_unchanged: u64,
    failures: Vec<ScanFailure>,
    /// Hidden-entry skips, logged once per scan as a count.
    hidden_skipped: u64,
    /// Symlink skips (never followed), summarized once per scan.
    symlink_skipped: u64,
    /// Monotonic suffix making temp bundle directory names unique within
    /// this scan.
    temp_seq: u64,
}

/// What happened to one regular file that was successfully processed.
enum FileScanAction {
    /// Prescreen matched the known (mtime, size) claims; enumerate only.
    Unchanged,
    /// A bundle was staged at this contract directory.
    Staged(PathBuf),
}

/// One per-item failure: classified for the durable acquisition record and
/// carrying local detail. The scan continues past every `ItemError`.
struct ItemError {
    class: AcquisitionFailureClass,
    detail: String,
}

impl ItemError {
    /// Classify an IO failure at a named stage of item processing.
    fn io(stage: &str, source: &io::Error) -> Self {
        Self {
            class: io_failure_class(source.kind()),
            detail: format!("{stage}: {source}"),
        }
    }

    /// Wrap an internal (non-IO) failure such as a clock or formatting
    /// error; classified `Other` because the source system did not fail.
    fn internal(stage: &str, source: &ApiError) -> Self {
        Self {
            class: AcquisitionFailureClass::Other,
            detail: format!("{stage}: {source}"),
        }
    }

    /// A failure with no external error source, described directly.
    fn other(detail: impl Into<String>) -> Self {
        Self {
            class: AcquisitionFailureClass::Other,
            detail: detail.into(),
        }
    }
}

/// Map an IO error kind onto the closed acquisition failure taxonomy
/// (spec §9.2): missing and forbidden are meaningful classes; everything
/// else is `Other`.
fn io_failure_class(kind: io::ErrorKind) -> AcquisitionFailureClass {
    match kind {
        io::ErrorKind::NotFound => AcquisitionFailureClass::NotFound,
        io::ErrorKind::PermissionDenied => AcquisitionFailureClass::AccessDenied,
        _ => AcquisitionFailureClass::Other,
    }
}

/// Wrap a connector parameter problem as the config-error variant used for
/// configuration-shaped failures across the service.
fn invalid_config(message: String) -> ApiError {
    ApiError::InvalidConfig { message }
}
