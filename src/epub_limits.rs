//! EPUB admission controls for the in-process EPUB parse worker.

use serde::{Deserialize, Serialize};

/// EPUB admission budgets. Every value is a required positive bound; the
/// worker records exceeding one as a parse outcome (or a warning for images)
/// rather than continuing past it.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EpubLimits {
    /// Archive members accepted before the parse fails as a recorded outcome.
    pub max_members: usize,
    /// Decompressed bytes accepted for any single member.
    pub max_member_bytes: usize,
    /// Decompressed bytes accepted across all members read.
    pub max_total_member_bytes: usize,
    /// Decoded bytes accepted for any XML member (package, navigation, content).
    pub max_document_bytes: usize,
    /// Bytes accepted for one image; larger images are not archived (warning).
    pub max_image_bytes: usize,
    /// Element nesting depth accepted in any XML member.
    pub max_element_depth: usize,
}
