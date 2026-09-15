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
//! - `chunk` (C6b): the fine chunker — packs evidence members into
//!   `chunk_projections` rows (PLAN-grains Section 2 fine grain) carrying the
//!   chunker identity, config hash, fragments, and section path.
//! - `dense` (C6c): per-chunk dense vectors (`chunk_dense_vectors`).
//! - `view` (C6d): derived-view and summary projections.
//! - `multivector` (C6e): ColBERT matrices per window of consecutive fine
//!   chunks (`colbert_windows`, PLAN-grains Section 2 ColBERT grain).
//! - `graph` (C6f): entity-mention and entity-edge projections (D9).
//!
//! This module owns the SHARED contract the packages consume: the banked
//! archived chunker configuration, its identity and config-hash derivation
//! (routed through `crate::canonical` so the hash follows the one §16.2
//! canonical-serialization rule), and the stored-chunk row shape the lexical
//! and dense builders read chunks back through.

// Consumed by the C6a–C6f projection builders; remove this allow as each
// package wires the shared contract.
#![allow(dead_code)]

pub(crate) mod annotation;
pub(crate) mod annotation_io;
pub(crate) mod chunk;
pub(crate) mod dense;
pub(crate) mod dense_cache;
pub(crate) mod envelope;
pub(crate) mod graph;
pub(crate) mod lexical;
pub(crate) mod multivector;
pub(crate) mod section_dense;
pub(crate) mod view;
pub(crate) mod worker;

use serde::{Deserialize, Serialize};

use crate::error::ApiError;

/// Chunker producer name recorded on every `chunk_projections` row and in the
/// projection's Provenance (spec §22 ChunkPayload.chunkerName). Stable across
/// rebuilds so a chunk's producer identity is answerable from its row.
pub(crate) const CHUNKER_NAME: &str = "fabric-chunker";

/// Chunker producer version (spec §22 ChunkPayload.chunkerVersion). Bumped
/// when the splitting algorithm changes in a way that alters chunk boundaries,
/// so a version change is a visible rebuild trigger rather than a silent
/// change in what got indexed.
pub(crate) const CHUNKER_VERSION: &str = "3";
pub(crate) const CHUNK_CONFIG_PAYLOAD_TYPE: &str = "chunk_construction_policy";

/// The identity-bearing chunker configuration whose canonical hash is the
/// `chunkerConfigHash` stamped on every chunk (spec §22). Serializing the real
/// struct keeps it the single source of the hashed shape; camelCase matches
/// the §16.2 wire convention used across the model. Two chunkers that would
/// produce different chunk boundaries must never share this hash, so every
/// boundary-affecting parameter belongs here: the token cap and, since
/// version 3, the PLAN-grains Section 2 minimum-fill ratio.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ChunkerConfig {
    pub(crate) chunker_name: String,
    pub(crate) chunker_version: String,
    pub(crate) max_unit_tokens: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) tokenizer_hash: Option<String>,
    /// Minimum fill of a run as a fraction of `max_unit_tokens`; absent only
    /// in the frozen pre-version-3 shapes, which had no minimum-fill rule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) min_fill_ratio: Option<f64>,
}

impl ChunkerConfig {
    /// Seal current construction limits and tokenizer bytes before producing any chunks.
    pub(crate) fn active(
        limits: &crate::limits::IndexingLimits,
        tokenizer: &tokenizers::Tokenizer,
    ) -> Result<Self, ApiError> {
        let tokenizer =
            tokenizer
                .to_string(false)
                .map_err(|source| ApiError::StorageOperation {
                    message: format!("serialize chunk tokenizer identity: {source}"),
                })?;
        Ok(Self {
            chunker_name: CHUNKER_NAME.to_owned(),
            chunker_version: CHUNKER_VERSION.to_owned(),
            max_unit_tokens: limits.fine_max_tokens,
            tokenizer_hash: Some(crate::canonical::sha256_hex_bytes(tokenizer.as_bytes())),
            min_fill_ratio: Some(limits.min_fill_ratio),
        })
    }

    /// Legacy artifacts have no descriptor and must match this exact frozen v1 shape.
    /// The v1 character floor (400) is no longer part of the descriptor; v1
    /// artifacts are matched by name and version only.
    pub(crate) fn legacy() -> Self {
        ChunkerConfig {
            chunker_name: CHUNKER_NAME.to_owned(),
            chunker_version: "1".to_owned(),
            max_unit_tokens: 512,
            tokenizer_hash: None,
            min_fill_ratio: None,
        }
    }

    /// Reject unknown construction versions without comparing history to today's settings.
    pub(crate) fn validate(&self) -> Result<(), ApiError> {
        let valid = self.chunker_name == CHUNKER_NAME
            && self.chunker_version == CHUNKER_VERSION
            && self.max_unit_tokens > 0
            && self.tokenizer_hash.as_ref().is_some_and(|hash| {
                hash.len() == 64
                    && hash
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
            && self
                .min_fill_ratio
                .is_some_and(|ratio| ratio.is_finite() && ratio > 0.0 && ratio < 1.0);
        if !valid {
            return Err(ApiError::StorageOperation {
                message: "invalid or unsupported chunk construction policy".to_owned(),
            });
        }
        Ok(())
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
/// is the canonical text the lexical channel indexes as-is and the dense
/// channel embeds behind `chunk::model_input`'s section-path prefix,
/// `fragments` and `section_path` are the decoded membership records and
/// first-member section path (PLAN-grains Section 2), and the chunker
/// identity/config hash pin the producing chunker (§22). `token_count` is
/// optional because it is measured by a caller-supplied tokenizer and may be
/// absent (§22 ChunkPayload.tokenCount). `chunk_index` is the chunk's 0-based
/// reading-order position within its parse, the only reading-order authority
/// for chunks; readers return chunks ordered by it.
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
    pub(crate) fragments: Vec<chunk::Fragment>,
    pub(crate) section_path: Vec<String>,
    pub(crate) chunk_index: usize,
}
