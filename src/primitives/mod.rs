//! Pure, dependency-free primitives extracted from `storage.rs` (C1b seam
//! cut): f32 blob codecs, vector validators, BM25 query construction, RRF
//! fusion, hashing, and timestamp formatting. Nothing here may depend on
//! SQLite, Axum, or Tokio; these functions must stay independently
//! reviewable.

pub(crate) mod bm25;
pub(crate) mod codec;
pub(crate) mod fusion;
pub(crate) mod hash;
pub(crate) mod time;
pub(crate) mod validate;

// Re-exports exist only for items with current cross-module consumers. The
// retrieval primitives (bm25, codec, fusion, validate) lost their legacy
// consumer at cluster CR; their fabric consumers (C6/C7) import via direct
// submodule paths. (The legacy-shaped `latency` module was deleted at C7d;
// `query::execute::QueryStageLatencies` is its fabric successor.)
pub(crate) use hash::sha256_hex;
pub(crate) use time::{current_time_ms, format_utc_timestamp_ms, parse_utc_timestamp_ms, utc_now};
