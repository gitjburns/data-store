//! C6c active dense cache: the in-memory successor to the retired legacy dense
//! cache (§1.6 "ingest publish invariant"; §2 state.rs/units.rs rows). It holds
//! one loaded dense plane per ACTIVE parse and hands the C7 dense retrieval
//! channel a captured snapshot to score against.
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
//! `chunk_dense_vectors` table; a plane in this cache is DERIVED from those rows
//! and is fully rebuildable from them. Caches are excluded from snapshots (spec
//! §8.4), so this state is never persisted and never replayed — it is rebuilt
//! by re-reading the durable rows through C6c-1's loader.
//!
//! Parallel-array layout (approved design fact): a plane stores its vectors as
//! PARALLEL ARRAYS — one contiguous row-major `Vec<f32>` of length n*dim, a
//! parallel `Vec<String>` of chunk ids, and a parallel `Vec<f32>` of L2 norms —
//! not a `Vec` of per-row structs. This is the layout the C7 dense channel
//! scans: cosine scoring walks the contiguous `vectors` buffer dim-strided
//! while indexing `chunk_ids`/`norms` by the same row index, which keeps the
//! scan cache-friendly and avoids per-row indirection. The three arrays are
//! always the same length in rows (`chunk_ids.len() == norms.len() ==
//! vectors.len() / dimension`); that invariant is established once at plane
//! construction and never mutated afterwards (planes are immutable behind an
//! `Arc` — see the swap invariant below).

// The cache is consumed by the C7 dense retrieval channel (`snapshot_for_parse`
// scoring) and by the C5/C6 integration activation wiring (`load_parse`,
// `evict_parse`). None are wired yet, so this module-level allow names those
// pending consumers; remove it as each seam attaches.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rusqlite::Connection;
use tracing::{error, info};

use crate::error::ApiError;
use crate::projections::dense::load_dense_vectors_for_parse;

/// One immutable loaded dense plane for a single active parse, stored as
/// parallel arrays (see the module-level layout note). Once built the plane is
/// never mutated; a "swap" installs a brand-new `Arc<DensePlane>` in the cache
/// map and drops the old one, so any reader holding a captured `Arc` keeps a
/// consistent plane for the whole scan.
///
/// Layout invariant (established in `from_rows`, never broken afterwards):
/// `chunk_ids.len() == norms.len()` and `vectors.len() == chunk_ids.len() *
/// dimension`. The C7 dense channel relies on this to index the three arrays by
/// a shared row index and to stride `vectors` by `dimension`.
#[derive(Debug)]
pub(crate) struct DensePlane {
    /// Row-major contiguous embeddings: row `i` occupies
    /// `vectors[i*dimension .. (i+1)*dimension]`. Contiguous (not a Vec of
    /// vectors) so the C7 cosine scan walks one buffer.
    vectors: Vec<f32>,
    /// Chunk id of row `i`, parallel to `vectors`' row `i`. Carried so a scan
    /// can report which chunk each score belongs to without a second lookup.
    chunk_ids: Vec<String>,
    /// Precomputed L2 norm of row `i`, parallel to `vectors`' row `i`. Reused by
    /// the query-time cosine scorer; every norm is finite and strictly positive
    /// (guaranteed by C6c-1's loader — the cache does not re-validate).
    norms: Vec<f32>,
    /// Embedding dimension: the stride into `vectors` and the length of every
    /// row. Uniform across the plane (a mismatch fails in C6c-1's loader).
    dimension: usize,
}

impl DensePlane {
    /// Build a plane from C6c-1's already-validated loader rows. The rows arrive
    /// in `chunk_id` order (C6c-1's handoff guarantee) and each satisfies
    /// `vector.len() == dimension`, finite values, and finite strictly-positive
    /// `norm`, so this constructor does NOT re-validate — it only flattens the
    /// row-of-structs shape into the parallel-array layout the C7 channel scans.
    /// It appends each row's `vector` into one contiguous buffer, establishing
    /// the layout invariant (`vectors.len() == chunk_ids.len() * dimension`) by
    /// construction.
    fn from_rows(
        rows: Vec<crate::projections::dense::StoredDenseVectorRow>,
        dimension: usize,
    ) -> Self {
        let mut vectors = Vec::with_capacity(rows.len() * dimension);
        let mut chunk_ids = Vec::with_capacity(rows.len());
        let mut norms = Vec::with_capacity(rows.len());
        for row in rows {
            // Each row's vector is length `dimension` (loader guarantee), so
            // extending the flat buffer preserves the row-major stride.
            vectors.extend_from_slice(&row.vector);
            chunk_ids.push(row.chunk_id);
            norms.push(row.norm);
        }
        Self {
            vectors,
            chunk_ids,
            norms,
            dimension,
        }
    }

    /// Number of vector rows in the plane (chunks, not f32s). Bounded by the
    /// parse's chunk count.
    pub(crate) fn row_count(&self) -> usize {
        self.chunk_ids.len()
    }

    /// The embedding dimension (stride into `vectors`, length of each row).
    pub(crate) fn dimension(&self) -> usize {
        self.dimension
    }

    /// The contiguous row-major embedding buffer the C7 dense channel scans.
    /// Row `i` is `&self.vectors()[i*dimension .. (i+1)*dimension]`.
    pub(crate) fn vectors(&self) -> &[f32] {
        &self.vectors
    }

    /// Chunk ids parallel to the vector rows: `chunk_ids()[i]` names row `i`.
    pub(crate) fn chunk_ids(&self) -> &[String] {
        &self.chunk_ids
    }

    /// Precomputed L2 norms parallel to the vector rows: `norms()[i]` is the
    /// norm of row `i`, reused by the cosine scorer.
    pub(crate) fn norms(&self) -> &[f32] {
        &self.norms
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
    /// `chunk_dense_vectors` rows through C6c-1's `load_dense_vectors_for_parse`
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
    /// No model calls and no gate: this LOADS persisted vectors (a pure SQLite
    /// read via C6c-1's loader, which takes no model gate). The rows are already
    /// validated by the loader, so no re-validation happens here.
    pub(crate) fn load_parse(
        &self,
        conn: &Connection,
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
        let rows = match load_dense_vectors_for_parse(conn, parse_id, expected_dimension) {
            Ok(rows) => rows,
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
        let row_count = rows.len();
        let plane = Arc::new(DensePlane::from_rows(rows, expected_dimension));

        // Swap under the lock: a single map insert replacing any prior plane.
        // Held only for this pointer move, never across the load above.
        self.with_planes(|planes| {
            planes.insert(parse_id.to_string(), Arc::clone(&plane));
        });

        info!(
            event = "dense_cache.load.completed",
            parse_id,
            vector_count = row_count,
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
            info!(
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
