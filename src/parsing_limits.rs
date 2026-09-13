//! Parser admission and subprocess controls shared with the offline PDF diagnostic.

use serde::{Deserialize, Serialize};

/// Parser acceptance, bounded process-output capture, and child-process observation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParsingLimits {
    pub max_candidate_units: usize,
    pub max_candidate_relationships: usize,
    pub max_candidate_warnings: usize,
    pub max_unit_body_bytes: usize,
    pub process_log_bytes: usize,
    pub process_read_chunk_bytes: usize,
    pub mupdf_poll_ms: u64,
    pub docling_poll_ms: u64,
    pub docling_feedback_initial_ms: u64,
    pub docling_feedback_interval_ms: u64,
    /// Zero disables native process sampling; parsing itself remains active.
    pub docling_sample_seconds: u64,
    pub activity_sample_grace_ms: u64,
    pub activity_poll_ms: u64,
    pub cleanup_regex_backtrack_limit: usize,
}
