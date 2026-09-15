//! Parser admission controls for in-process parse workers.

use serde::{Deserialize, Serialize};

/// Parser acceptance bounds on candidate counts and unit body size.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParsingLimits {
    pub max_candidate_units: usize,
    pub max_candidate_relationships: usize,
    pub max_candidate_warnings: usize,
    pub max_unit_body_bytes: usize,
}
