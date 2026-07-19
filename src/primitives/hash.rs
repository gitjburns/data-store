//! Content hashing for durable document metadata.

use sha2::{Digest, Sha256};

/// Return a lowercase SHA-256 hex digest for durable document metadata.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut result = String::with_capacity(digest.len() * 2);
    for value in digest {
        result.push_str(&format!("{value:02x}"));
    }
    result
}
