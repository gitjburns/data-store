//! Active dense handles retain publication identity, never resident vector planes.
//! Passage rows remain in SQLite and section vectors remain in immutable artifacts;
//! each query scans them through its captured read snapshot with bounded buffers.
//!
//! Successor identity: the legacy `ingest_document` path held a dense-cache
//! Mutex across the SQLite commit so the durable active-row write and the
//! in-memory swap could not interleave with another publish (§1.6). This module
//! is that cache's replacement under the new fabric shape: the per-source
//! cutover barrier (`state::CutoverRegistry`) now owns the atomicity, and a
//! plane swap here is one of the two operations that barrier covers (the
//! active-pointer swap and this in-memory publish together — see the barrier
//! doc on `CutoverRegistry`). This module does NOT own the barrier; the
//! activation caller (C5/integration) holds the barrier guard across both the
//! durable write and the `load_parse`/`evict_parse` call, so the swap here
//! never interleaves with another publish of the same source.
//!
//! Ephemeral vs durable (spec §8.4): the durable source of truth is the
//! `chunk_dense_vectors` table and immutable section artifacts. The cache is
//! derived from both and reconstructed without inference at startup and restore.
//!
//! File pages can stay warm in operating-system caches and be reclaimed under
//! memory pressure without evicting a required service-owned vector buffer.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rusqlite::Connection;
use tracing::{debug, error, info};

use crate::artifact_store::ArtifactStore;
use crate::error::ApiError;
use crate::projections::dense::{StoredDenseVectorRow, visit_dense_vectors_for_parse};
use crate::projections::section_dense::{
    SectionDenseReference, SectionDenseWindow, load_section_dense_reference, visit_section_dense,
};

/// An immutable handle to one parse's persisted vectors. Captured handles survive
/// cutover; callers supply their captured SQLite connection for every scan.
#[derive(Debug)]
pub(crate) struct DensePlane {
    parse_id: String,
    row_count: usize,
    dimension: usize,
    // Both representations share one immutable publication. None identifies a
    // legacy parse explicitly; query execution must require an operator rebuild.
    sections: Option<SectionDenseReference>,
}

impl DensePlane {
    /// Expected model dimension is checked before any row is offered for scoring.
    pub(crate) fn dimension(&self) -> usize {
        self.dimension
    }

    /// Visit one validated passage row at a time in the query's read snapshot.
    /// A disappeared persisted row is corruption, not an ordinary cache miss.
    pub(crate) fn visit_vectors(
        &self,
        conn: &Connection,
        visit: impl FnMut(&StoredDenseVectorRow) -> Result<(), ApiError>,
    ) -> Result<(), ApiError> {
        let count = visit_dense_vectors_for_parse(conn, &self.parse_id, self.dimension, visit)?;
        if count != self.row_count {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "dense row count changed for parse {}: expected {}, found {count}",
                    self.parse_id, self.row_count
                ),
            });
        }
        Ok(())
    }

    /// Return the section representation captured in the same cache publication.
    pub(crate) fn sections(&self) -> Option<&SectionDenseReference> {
        self.sections.as_ref()
    }

    /// Stream captured section vectors without retaining the full artifact. Any
    /// provisional scores must be discarded if final integrity validation fails.
    pub(crate) fn visit_sections(
        &self,
        conn: &Connection,
        store: &ArtifactStore,
        visit: impl FnMut(&SectionDenseWindow) -> Result<(), ApiError>,
    ) -> Result<(), ApiError> {
        let reference = self
            .sections
            .as_ref()
            .ok_or_else(|| ApiError::ServiceUnavailable {
                message: format!(
                    "section dense projection missing for parse {}",
                    self.parse_id
                ),
            })?;
        visit_section_dense(conn, store, reference, visit)
    }
}

/// The active dense cache: one loaded `DensePlane` per active parse, shared
/// process state per PRINCIPLES.md ("shared process state should be small,
/// explicit, stable"). Keyed by parse id — the active parse of each source — so
/// a source's supersession evicts exactly its prior parse's plane.
///
/// Concurrency model (mirrors `state::CutoverRegistry`): the map lock is held
/// ONLY for the single map operation (get / insert / remove), NEVER across a
/// plane build or a scan. A new plane is loaded and constructed BEFORE the lock
/// is taken; the swap is one map insert under the lock; a snapshot read is one
/// map get that clones an `Arc` and releases the lock immediately. Because the
/// lock is never held across the durable SQLite work or the C7 scan, it never
/// serializes reads against a swap for longer than a pointer move.
///
/// Poison recovery (same sticky-poison rule as `ExclusiveGate`/`CutoverRegistry`
/// in state.rs): the `HashMap` behind the lock stays structurally valid after a
/// holder panics — every critical section is a single map mutation that cannot
/// leave the map torn — so a poisoned lock is recovered (`into_inner()`) with a
/// logged event rather than propagated, so one panic cannot wedge every later
/// activation or query.
#[derive(Debug)]
pub(crate) struct DenseCache {
    // parse_id -> that parse's loaded plane. Entries are added by `load_parse`
    // on activation and removed by `evict_parse` on supersession/deactivation,
    // so the map's size tracks the number of currently active parses (small,
    // explicit, stable — one plane per active parse).
    planes: Mutex<HashMap<String, Arc<DensePlane>>>,
}

impl DenseCache {
    /// Create an empty cache; planes are installed lazily by `load_parse` as
    /// parses activate.
    pub(crate) fn new() -> Self {
        Self {
            planes: Mutex::new(HashMap::new()),
        }
    }

    /// Drop all planes after rebuild-all has drained every query and publisher.
    pub(crate) fn clear(&self) {
        self.with_planes(|planes| planes.clear());
        info!(
            event = "dense_cache.cleared",
            "all active dense planes cleared for rebuild-all"
        );
    }

    /// Load (or reload) the active dense plane for `parse_id` from the durable
    /// `chunk_dense_vectors` rows through `visit_dense_vectors_for_parse`
    /// reader, then atomically swap it into the cache map.
    ///
    /// Publish invariant (successor to the §1.6 ingest publish invariant): the
    /// CALLER holds the source's cutover barrier (`state::CutoverRegistry`)
    /// across the durable active-row write AND this call, so the durable write
    /// and this in-memory swap cannot interleave with another publish of the
    /// same source. This method does not touch the barrier itself; it assumes
    /// the caller's barrier discipline and only performs the derived-plane
    /// swap.
    ///
    /// Atomic swap: the plane is fully loaded and constructed BEFORE the map
    /// lock is taken. Under the lock a single `insert` replaces the prior
    /// `Arc<DensePlane>` (if any) with the new one; the old `Arc` is dropped
    /// only when its last in-flight reader releases its captured clone. In-flight
    /// C7 scans that captured the prior `Arc` (via `snapshot_for_parse`) keep
    /// scoring against it undisturbed — a swap is tear-free because a plane is
    /// immutable and readers hold their own `Arc` (see `snapshot_for_parse`).
    ///
    /// Load persisted vectors and section artifacts without model calls. Both
    /// loaders validate their representation before the single cache swap.
    pub(crate) fn load_parse(
        &self,
        conn: &Connection,
        store: &ArtifactStore,
        parse_id: &str,
        expected_dimension: usize,
    ) -> Result<(), ApiError> {
        let started = Instant::now();
        info!(
            event = "dense_cache.load.started",
            parse_id, expected_dimension, "active dense plane load started"
        );

        // Durable read + plane construction happen OUTSIDE the map lock, so the
        // lock is never held across the SQLite work — only across the swap.
        let row_count =
            match visit_dense_vectors_for_parse(conn, parse_id, expected_dimension, |_| Ok(())) {
                Ok(count) => count,
                Err(error) => {
                    error!(
                        event = "dense_cache.load.failed",
                        parse_id,
                        expected_dimension,
                        error = %error,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "active dense plane load failed"
                    );
                    return Err(error);
                }
            };
        let sections = load_section_dense_reference(conn, store, parse_id, expected_dimension)
            .map_err(|error| {
                error!(event = "dense_cache.load.failed", parse_id, expected_dimension,
                error = %error, elapsed_ms = started.elapsed().as_millis() as u64,
                "active section dense plane load failed; cache not published");
                error
            })?;
        let section_window_count = sections.as_ref().map_or(0, |plane| plane.window_count);
        let section_projection_present = sections.is_some();
        let plane = Arc::new(DensePlane {
            parse_id: parse_id.to_owned(),
            row_count,
            dimension: expected_dimension,
            sections,
        });

        // Swap under the lock: a single map insert replacing any prior plane.
        // Held only for this pointer move, never across the load above.
        self.with_planes(|planes| {
            planes.insert(parse_id.to_string(), Arc::clone(&plane));
        });

        info!(
            event = "dense_cache.load.completed",
            parse_id,
            vector_count = row_count,
            section_window_count,
            section_projection_present,
            dimension = expected_dimension,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "active dense plane loaded and swapped in"
        );
        Ok(())
    }

    /// Capture the current plane for `parse_id` as a cheap `Arc` clone — the
    /// snapshot the C7 dense channel scores against. Returns `None` when no
    /// plane is loaded for the parse (never activated, or already evicted).
    ///
    /// Captured-snapshot invariant: scoring MUST read this returned `Arc`, never
    /// the live map. The map lock is taken only long enough to clone the `Arc`
    /// and is released before scoring begins, so a concurrent `load_parse` swap
    /// (which replaces the map entry) cannot tear an in-flight scan — the scan
    /// keeps the immutable plane it captured, and the swapped-out plane is freed
    /// only after the last captured `Arc` drops. This mirrors the §31.1 rule
    /// that in-flight queries execute against their captured pre-cutover
    /// snapshots.
    pub(crate) fn snapshot_for_parse(&self, parse_id: &str) -> Option<Arc<DensePlane>> {
        // Clone the Arc under the lock, then release: the caller scores against
        // the returned clone with the lock long dropped.
        self.with_planes(|planes| planes.get(parse_id).map(Arc::clone))
    }

    /// Evict the plane for a superseded or deactivated parse, dropping its
    /// `Arc` from the map. Keyed by parse id to match the active-parse keying:
    /// when a source's active parse is superseded, its prior parse's plane is
    /// dropped here.
    ///
    /// Same publish discipline as `load_parse`: on a supersession that swaps in
    /// a new active parse, the caller holds the source's cutover barrier across
    /// both the new parse's `load_parse` and the old parse's `evict_parse`, so
    /// the two map operations and the durable pointer swap form one publish.
    ///
    /// Tear-free like the swap: removing the map entry drops the cache's `Arc`,
    /// but any C7 scan that already captured the plane via `snapshot_for_parse`
    /// keeps its own `Arc` and finishes undisturbed; the plane's memory is freed
    /// only when that last captured clone drops.
    pub(crate) fn evict_parse(&self, parse_id: &str) {
        let removed = self.with_planes(|planes| planes.remove(parse_id).is_some());
        if removed {
            info!(
                event = "dense_cache.evicted",
                parse_id, "active dense plane evicted"
            );
        } else {
            // Evicting an absent parse is a benign no-op (e.g. a source with no
            // dense plane), logged so an unexpected miss is still visible.
            debug!(
                event = "dense_cache.evict.absent",
                parse_id, "dense plane eviction requested for a parse with no loaded plane"
            );
        }
    }

    /// Run `body` while holding the planes map lock, recovering lock poison the
    /// same way `state::ExclusiveGate`/`CutoverRegistry` do: the map stays
    /// structurally valid after a holder panic (every critical section is a
    /// single map mutation), so a poisoned lock is recovered with a logged event
    /// rather than propagated — one panic must not wedge every later activation
    /// or query. The lock is held only for `body`, which is always one bounded
    /// map operation, never a plane build or a scan.
    fn with_planes<T>(&self, body: impl FnOnce(&mut HashMap<String, Arc<DensePlane>>) -> T) -> T {
        let mut planes = match self.planes.lock() {
            Ok(planes) => planes,
            Err(poisoned) => {
                error!(
                    event = "dense_cache.lock_poisoned",
                    "dense cache map lock was poisoned; recovering last valid map"
                );
                poisoned.into_inner()
            }
        };
        body(&mut planes)
    }
}
