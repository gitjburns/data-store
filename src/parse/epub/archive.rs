//! Capped zip member reading (SPEC-epub §5.1): member count, per-member and
//! total decompressed byte caps, accepted compression methods, and
//! normalized member-name lookup. Nothing is extracted to disk.
//!
//! Every failure here is a recorded outcome at stage `Archive`; the caller
//! that knows the §11.2 boundary re-stages it (`EpubFailure::with_stage`).

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::Path;

use zip::CompressionMethod;
use zip::ZipArchive;

use crate::limits::EpubLimits;
use crate::parse::epub::{EpubFailure, EpubStage};

/// One open EPUB archive: the zip reader, the normalized-name index used
/// for every lookup, and the running total of distinct member bytes read.
pub(crate) struct Archive {
    zip: ZipArchive<File>,
    /// Normalized member name to zip index. Directory entries and names
    /// that escape the archive root are not indexed; they still count
    /// toward `max_members` because they are directory entries.
    names: BTreeMap<String, usize>,
    /// Zip indexes already charged to `total_bytes_read`, so the two-pass
    /// walk of §11.2 charges each member once (§5.1).
    charged: BTreeSet<usize>,
    /// Decompressed bytes of every distinct member read so far.
    total_bytes_read: u64,
    /// Sum of the central directory's declared uncompressed sizes, taken
    /// at `open`. Diagnostic only: declared sizes are untrusted (§5.1) and
    /// never feed a cap.
    declared_total_bytes: u64,
    /// Decompressed bytes accepted for one member.
    max_member_bytes: usize,
    /// Decompressed bytes accepted across all distinct members.
    max_total_member_bytes: usize,
}

/// Open the archive at `path`, enforcing `max_members` and rejecting
/// encrypted members, unsupported compression methods, and non-zip files.
/// Only the central directory is read; no member is decompressed here.
pub(crate) fn open(path: &Path, limits: &EpubLimits) -> Result<Archive, EpubFailure> {
    let file = File::open(path)
        .map_err(|error| archive_failure(format!("cannot open archive file: {error}")))?;
    let mut zip = ZipArchive::new(file)
        .map_err(|error| archive_failure(format!("not a readable zip archive: {error}")))?;

    // Member count is checked before any per-member inspection so an
    // oversized directory fails on the cap, not on whatever entry happens
    // to come first.
    let member_count = zip.len();
    if member_count > limits.max_members {
        return Err(archive_failure(format!(
            "archive has {member_count} members, exceeding epub.max_members = {}",
            limits.max_members
        )));
    }

    let mut names = BTreeMap::new();
    let mut declared_total_bytes: u64 = 0;
    for index in 0..member_count {
        // `by_index_raw` yields the entry without decrypting or
        // decompressing, so metadata can be inspected for every member,
        // including ones that `by_index` would refuse to open.
        let entry = zip.by_index_raw(index).map_err(|error| {
            archive_failure(format!("cannot read directory entry {index}: {error}"))
        })?;
        let name = entry.name().to_owned();
        // Summed before any rejection below so the diagnostic covers the
        // whole directory; saturating because the values are untrusted.
        declared_total_bytes = declared_total_bytes.saturating_add(entry.size());
        if entry.encrypted() {
            return Err(archive_failure(format!(
                "member '{name}' is encrypted; encrypted members are not supported"
            )));
        }
        let method = entry.compression();
        if !matches!(
            method,
            CompressionMethod::Stored | CompressionMethod::Deflated
        ) {
            return Err(archive_failure(format!(
                "member '{name}' uses compression method {method}; only stored and deflate are supported"
            )));
        }
        if entry.is_dir() {
            continue;
        }
        // The first entry claiming a normalized name wins; a later duplicate
        // in a malformed directory never shadows an already indexed member.
        if let Some(normalized) = normalize_member_name(&name) {
            names.entry(normalized).or_insert(index);
        }
    }

    Ok(Archive {
        zip,
        names,
        charged: BTreeSet::new(),
        total_bytes_read: 0,
        declared_total_bytes,
        max_member_bytes: limits.max_member_bytes,
        max_total_member_bytes: limits.max_total_member_bytes,
    })
}

impl Archive {
    /// Whether a member with this normalized name exists.
    pub(crate) fn contains(&self, member: &str) -> bool {
        self.names.contains_key(member)
    }

    /// Read one member fully into memory under the per-member cap and the
    /// total cap (each member counted once); `Ok(None)` when absent.
    /// The declared uncompressed size is never trusted: the reader is
    /// bounded at one byte past the cap so an overrun is detected without
    /// decompressing further.
    pub(crate) fn read(&mut self, member: &str) -> Result<Option<Vec<u8>>, EpubFailure> {
        let Some(&index) = self.names.get(member) else {
            return Ok(None);
        };
        let cap = self.max_member_bytes;
        let entry = self
            .zip
            .by_index(index)
            .map_err(|error| archive_failure(format!("cannot open member '{member}': {error}")))?;
        // Capacity is bounded by the cap so a lying declared size cannot
        // drive allocation; the declared size only avoids regrowth when
        // honest.
        let declared = usize::try_from(entry.size()).unwrap_or(usize::MAX);
        let mut bytes = Vec::with_capacity(declared.min(cap));
        // One byte past the cap is enough to prove the member exceeds it.
        let bound = u64::try_from(cap).unwrap_or(u64::MAX).saturating_add(1);
        entry
            .take(bound)
            .read_to_end(&mut bytes)
            .map_err(|error| archive_failure(format!("cannot read member '{member}': {error}")))?;
        if bytes.len() > cap {
            return Err(archive_failure(format!(
                "member '{member}' exceeds epub.max_member_bytes = {cap}"
            )));
        }

        // Charge the total once per distinct member (§5.1): the second
        // read of a content document in the two-pass walk is free.
        if self.charged.insert(index) {
            let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            let total = self.total_bytes_read.saturating_add(len);
            let total_cap = u64::try_from(self.max_total_member_bytes).unwrap_or(u64::MAX);
            if total > total_cap {
                return Err(archive_failure(format!(
                    "reading member '{member}' brings total member bytes to {total}, \
                     exceeding epub.max_total_member_bytes = {total_cap}"
                )));
            }
            self.total_bytes_read = total;
        }
        Ok(Some(bytes))
    }

    /// Number of members in the archive directory, including directory
    /// entries and names that were not indexed.
    pub(crate) fn member_count(&self) -> usize {
        self.zip.len()
    }

    /// Sum of the central directory's declared uncompressed sizes over every
    /// entry, as read at `open`. Diagnostic only (logged by
    /// `epub.archive.opened`): declared sizes are untrusted (§5.1) and never
    /// feed a cap; `read` measures the real bytes.
    pub(crate) fn declared_total_bytes(&self) -> u64 {
        self.declared_total_bytes
    }
}

/// Build a failure at stage `Archive`; the calling boundary re-stages it.
fn archive_failure(detail: String) -> EpubFailure {
    EpubFailure::new(EpubStage::Archive, detail)
}

/// Normalize a zip entry name into the lookup key that §5.4 href
/// resolution produces on the archive side: segments split on `/`, empty
/// and `.` segments dropped, `..` popping the previous segment. Returns
/// `None` when the name escapes the archive root, so such an entry can
/// never be addressed. No percent-decoding: zip names are literal.
fn normalize_member_name(name: &str) -> Option<String> {
    let mut segments: Vec<&str> = Vec::new();
    for segment in name.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop()?;
            }
            other => segments.push(other),
        }
    }
    if segments.is_empty() {
        return None;
    }
    Some(segments.join("/"))
}
