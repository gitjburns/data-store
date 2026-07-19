//! Retrieval projections (spec §22–§23; C6 cluster). Projections are
//! rebuildable retrieval-targeting and ranking artifacts over the active
//! parse — never canonical evidence (§8.3). Their freshness envelope lives on
//! the shared `retrieval_projections` metadata table (spec §22); the typed
//! payloads live in the per-type C6 tables added to sql/fabric/schema.sql.
//!
//! Layout, one module per C6 work package plus the shared envelope store:
//! - `envelope` (substrate): the ONE persistence path for RetrievalProjection
//!   metadata rows (`retrieval_projections`) and their freshness lifecycle,
//!   consumed by every builder below.
//! - `lexical` (C6a): FTS5 lexical channel over `chunk_text_index`.
//! - `chunk` (C6b): the chunk builder — splits (text, unit-id context) into
//!   `chunk_projections` rows carrying the chunker identity and config hash.
//! - `dense` (C6c): per-chunk dense vectors (`chunk_dense_vectors`).
//! - `view` (C6d): derived-view and summary projections.
//! - `multivector` (C6e): per-unit ColBERT matrices
//!   (`unit_multivector_projections`, D4).
//! - `graph` (C6f): entity-mention and entity-edge projections (D9).
//!
//! This module owns the SHARED contract the packages consume: the banked
//! chunker constants, the chunker identity and its config-hash derivation
//! (routed through `crate::canonical` so the hash follows the one §16.2
//! canonical-serialization rule), and the stored-chunk row shape the lexical
//! and dense builders read chunks back through.

// Consumed by the C6a–C6f projection builders; remove this allow as each
// package wires the shared contract.
#![allow(dead_code)]

pub(crate) mod chunk;
pub(crate) mod dense;
pub(crate) mod dense_cache;
pub(crate) mod envelope;
pub(crate) mod graph;
pub(crate) mod lexical;
pub(crate) mod multivector;
pub(crate) mod view;

use serde::Serialize;

use crate::error::ApiError;

/// Minimum character length a chunk must reach to be indexed as a search
/// target (spec §23 / D3). Banked at CRc from the retired
/// `[search].min_search_unit_chars` config key so C6b no longer reads it from
/// config; it is now identity-bearing, folded into `chunkerConfigHash`.
pub(crate) const MIN_SEARCH_UNIT_CHARS: u32 = 400;

/// Maximum token count a chunk may hold before it is split (spec §23 / D3),
/// the ColBERT token cap. Banked at CRc from the retired
/// `[search].max_unit_tokens` config key; like `MIN_SEARCH_UNIT_CHARS` it is
/// now part of the hashed chunker configuration, not runtime config.
pub(crate) const MAX_UNIT_TOKENS: u32 = 512;

/// Chunker producer name recorded on every `chunk_projections` row and in the
/// projection's Provenance (spec §22 ChunkPayload.chunkerName). Stable across
/// rebuilds so a chunk's producer identity is answerable from its row.
pub(crate) const CHUNKER_NAME: &str = "fabric-chunker";

/// Chunker producer version (spec §22 ChunkPayload.chunkerVersion). Bumped
/// when the splitting algorithm changes in a way that alters chunk boundaries,
/// so a version change is a visible rebuild trigger rather than a silent
/// change in what got indexed.
pub(crate) const CHUNKER_VERSION: &str = "1";

/// The identity-bearing chunker configuration whose canonical hash is the
/// `chunkerConfigHash` stamped on every chunk (spec §22). Serializing the real
/// struct keeps it the single source of the hashed shape; camelCase matches
/// the §16.2 wire convention used across the model. Two chunkers that would
/// produce different chunk boundaries must never share this hash, so every
/// boundary-affecting parameter belongs here.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ChunkerConfig {
    pub(crate) chunker_name: &'static str,
    pub(crate) chunker_version: &'static str,
    pub(crate) min_search_unit_chars: u32,
    pub(crate) max_unit_tokens: u32,
}

impl ChunkerConfig {
    /// The active chunker configuration: the banked constants under the
    /// current chunker identity. This is the one configuration C6b builds
    /// chunks with, so its hash is the `chunkerConfigHash` every chunk carries.
    pub(crate) fn active() -> Self {
        ChunkerConfig {
            chunker_name: CHUNKER_NAME,
            chunker_version: CHUNKER_VERSION,
            min_search_unit_chars: MIN_SEARCH_UNIT_CHARS,
            max_unit_tokens: MAX_UNIT_TOKENS,
        }
    }

    /// Derive this configuration's `chunkerConfigHash` (spec §22): lowercase
    /// hex SHA-256 over the canonical serialization, routed through
    /// `crate::canonical` so it follows the same §16.2 rule as every other
    /// content-derived hash in the fabric.
    pub(crate) fn config_hash(&self) -> Result<String, ApiError> {
        crate::canonical::canonical_sha256_hex_of(self)
    }
}

/// One `chunk_projections` row read back by the builders that consume chunks
/// (C6a lexical indexing, C6c dense embedding). Fields mirror the schema
/// columns: `input_unit_ids` is the decoded `input_unit_ids_json` array (the
/// canonical ContentUnit IDs the chunk targets, §23 rule 2), `targeting_text`
/// is the exact text indexed by both the lexical and dense channels, and the
/// chunker identity/config hash pin the producing chunker (§22). `token_count`
/// is optional because it is measured by a caller-supplied tokenizer and may
/// be absent (§22 ChunkPayload.tokenCount).
#[derive(Debug, Clone)]
pub(crate) struct StoredChunk {
    pub(crate) id: String,
    pub(crate) projection_id: String,
    pub(crate) source_id: String,
    pub(crate) parse_id: String,
    pub(crate) input_unit_ids: Vec<String>,
    pub(crate) targeting_text: String,
    pub(crate) token_count: Option<u64>,
    pub(crate) chunker_name: String,
    pub(crate) chunker_version: String,
    pub(crate) chunker_config_hash: String,
}
