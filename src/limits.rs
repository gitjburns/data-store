//! Immutable operational limits shared by the service and bundled clients.
//! Missing fields are errors; model capacities and historical artifact formats
//! remain separate from these operator-selected work and allocation budgets.

pub use crate::client_limits::ClientLimits;
use serde::{Deserialize, Serialize};
// Diagnostic crates can share parsing controls without importing server runtime state.
#[path = "parsing_limits.rs"]
mod parsing;
pub use parsing::ParsingLimits;

/// Query work, presentation, and evidence budgets captured for each retrieval profile.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetrievalLimits {
    pub default_results: u32,
    pub max_results: u32,
    pub max_concurrent_queries: u32,
    pub max_candidates_per_channel: u32,
    pub colbert_candidate_pool_size: u32,
    pub reranker_candidate_pool_size: u32,
    pub rrf_k: u32,
    pub section_candidate_limit: u32,
    pub section_passages_per_window: u32,
    pub graph_hop_budget: u32,
    pub max_entity_name_tokens: usize,
    /// Shorter normalized terms remain eligible only for the broad lexical query.
    pub min_fts_term_chars: usize,
    pub passage_max_tokens: u32,
    pub max_passage_units: usize,
    pub max_source_locations: usize,
    pub max_section_ancestry: usize,
    pub raw_evidence_max_units: u32,
    pub raw_evidence_max_tokens: u32,
    pub entity_matching: FuzzyLimits,
}

/// Numeric fuzzy-match bounds; the policy document separately enables match classes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FuzzyLimits {
    pub max_fuzzy_candidates: u32,
    pub acronym_min_name_tokens: u32,
    pub prefix_min_token_chars: u32,
}

/// New projection construction settings; persisted artifacts retain their own settings.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexingLimits {
    pub min_search_unit_chars: usize,
    pub chunk_max_tokens: u32,
    pub annotation_window_max_tokens: u32,
    pub section_max_tokens: u32,
}

/// Allocation and scan guards; exceeding one is an explicit resource-limit failure.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLimits {
    pub max_source_body_bytes: usize,
    pub max_json_cell_bytes: usize,
    pub max_manifest_bytes: usize,
    /// Bound one HTTP model-list response before JSON decoding.
    pub model_metadata_max_bytes: usize,
    pub max_embedding_values: usize,
    pub max_embedding_rows: usize,
    pub max_annotations_per_cohort: usize,
    pub max_cohorts_per_parse: usize,
    pub max_sources: usize,
    pub max_annotation_inputs: usize,
    pub max_annotation_dependents: usize,
    pub max_parse_units: usize,
    pub max_startup_parses: usize,
    pub vocabulary_scan_rows: usize,
    pub vocabulary_groups: usize,
    pub embedding_read_buffer_bytes: usize,
}

/// Worker admission and cadence; bounded cycles leave queued work for later cycles.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerLimits {
    pub annotation_idle_interval_ms: u64,
    pub annotation_concurrent_calls: usize,
    pub annotation_commit_wait_log_attempts: usize,
    pub projection_interval_ms: u64,
    pub projection_commit_wait_ms: u64,
    pub projection_batch_size: usize,
    pub projection_cohorts_per_cycle: usize,
    pub projection_cohorts_per_source: usize,
}

/// Independent SQLite lock waiting and cooperative statement execution deadlines.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteLimits {
    pub busy_timeout_ms: u64,
    pub execution_timeout_ms: u64,
    pub progress_operations: i32,
}

/// Adaptive filesystem scheduling bounds, growth factors, and shutdown polling.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulingLimits {
    pub min_interval_ms: u64,
    pub max_backoff_ms: u64,
    pub error_retry_base_ms: u64,
    pub quiet_growth: f64,
    pub backpressure_growth: f64,
    pub error_growth: f64,
    pub inter_change_ema_weight: f64,
    pub maintenance_poll_ms: u64,
}

/// Bounded operational summaries; authoritative records keep their existing ownership.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticLimits {
    pub max_error_chars: usize,
    pub persisted_detail_chars: usize,
    pub error_chain_depth: usize,
    pub model_error_excerpt_chars: usize,
    pub progress_log_chars: usize,
    pub activity_process_name_chars: usize,
    pub activity_error_chars: usize,
    pub identifier_preview_chars: usize,
    pub hash_prefix_chars: usize,
    pub sweep_example_paths: usize,
}

/// Validated operational settings copied once from the loaded configuration.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeLimits {
    pub retrieval: RetrievalLimits,
    pub indexing: IndexingLimits,
    pub resources: ResourceLimits,
    pub workers: WorkerLimits,
    pub sqlite: SqliteLimits,
    pub parsing: ParsingLimits,
    pub scheduling: SchedulingLimits,
    pub diagnostics: DiagnosticLimits,
    pub client: ClientLimits,
}

impl FuzzyLimits {
    /// Require non-vacuous name matching; enable flags govern disabled classes.
    pub fn validate(&self) -> Result<(), String> {
        positive_values(&[
            (
                "retrieval.entity_matching.max_fuzzy_candidates",
                self.max_fuzzy_candidates as u128,
            ),
            (
                "retrieval.entity_matching.prefix_min_token_chars",
                self.prefix_min_token_chars as u128,
            ),
        ])?;
        if self.acronym_min_name_tokens < 2 {
            return Err(
                "retrieval.entity_matching.acronym_min_name_tokens must be at least 2".into(),
            );
        }
        Ok(())
    }
}

impl RuntimeLimits {
    /// Validate allocation, ordering, and cadence invariants before any runtime work.
    pub fn validate(&self) -> Result<(), String> {
        self.client.validate()?;
        self.retrieval.entity_matching.validate()?;
        let r = self.retrieval;
        let i = self.indexing;
        let m = self.resources;
        let w = self.workers;
        let p = self.parsing;
        let s = self.scheduling;
        let d = self.diagnostics;
        positive_values(&[
            ("retrieval.default_results", r.default_results as u128),
            ("retrieval.max_results", r.max_results as u128),
            (
                "retrieval.max_concurrent_queries",
                r.max_concurrent_queries as u128,
            ),
            (
                "retrieval.max_candidates_per_channel",
                r.max_candidates_per_channel as u128,
            ),
            (
                "retrieval.colbert_candidate_pool_size",
                r.colbert_candidate_pool_size as u128,
            ),
            (
                "retrieval.reranker_candidate_pool_size",
                r.reranker_candidate_pool_size as u128,
            ),
            ("retrieval.rrf_k", r.rrf_k as u128),
            (
                "retrieval.section_candidate_limit",
                r.section_candidate_limit as u128,
            ),
            (
                "retrieval.section_passages_per_window",
                r.section_passages_per_window as u128,
            ),
            (
                "retrieval.max_entity_name_tokens",
                r.max_entity_name_tokens as u128,
            ),
            ("retrieval.min_fts_term_chars", r.min_fts_term_chars as u128),
            ("retrieval.passage_max_tokens", r.passage_max_tokens as u128),
            ("retrieval.max_passage_units", r.max_passage_units as u128),
            (
                "retrieval.max_source_locations",
                r.max_source_locations as u128,
            ),
            (
                "retrieval.max_section_ancestry",
                r.max_section_ancestry as u128,
            ),
            (
                "retrieval.raw_evidence_max_units",
                r.raw_evidence_max_units as u128,
            ),
            (
                "retrieval.raw_evidence_max_tokens",
                r.raw_evidence_max_tokens as u128,
            ),
            (
                "indexing.min_search_unit_chars",
                i.min_search_unit_chars as u128,
            ),
            ("indexing.chunk_max_tokens", i.chunk_max_tokens as u128),
            (
                "indexing.annotation_window_max_tokens",
                i.annotation_window_max_tokens as u128,
            ),
            ("indexing.section_max_tokens", i.section_max_tokens as u128),
            (
                "resources.max_source_body_bytes",
                m.max_source_body_bytes as u128,
            ),
            (
                "resources.max_json_cell_bytes",
                m.max_json_cell_bytes as u128,
            ),
            ("resources.max_manifest_bytes", m.max_manifest_bytes as u128),
            (
                "resources.model_metadata_max_bytes",
                m.model_metadata_max_bytes as u128,
            ),
            (
                "resources.max_embedding_values",
                m.max_embedding_values as u128,
            ),
            ("resources.max_embedding_rows", m.max_embedding_rows as u128),
            (
                "resources.max_annotations_per_cohort",
                m.max_annotations_per_cohort as u128,
            ),
            (
                "resources.max_cohorts_per_parse",
                m.max_cohorts_per_parse as u128,
            ),
            ("resources.max_sources", m.max_sources as u128),
            (
                "resources.max_annotation_inputs",
                m.max_annotation_inputs as u128,
            ),
            (
                "resources.max_annotation_dependents",
                m.max_annotation_dependents as u128,
            ),
            ("resources.max_parse_units", m.max_parse_units as u128),
            ("resources.max_startup_parses", m.max_startup_parses as u128),
            (
                "resources.vocabulary_scan_rows",
                m.vocabulary_scan_rows as u128,
            ),
            ("resources.vocabulary_groups", m.vocabulary_groups as u128),
            (
                "resources.embedding_read_buffer_bytes",
                m.embedding_read_buffer_bytes as u128,
            ),
            (
                "workers.annotation_idle_interval_ms",
                w.annotation_idle_interval_ms as u128,
            ),
            (
                "workers.annotation_concurrent_calls",
                w.annotation_concurrent_calls as u128,
            ),
            (
                "workers.annotation_commit_wait_log_attempts",
                w.annotation_commit_wait_log_attempts as u128,
            ),
            (
                "workers.projection_interval_ms",
                w.projection_interval_ms as u128,
            ),
            (
                "workers.projection_commit_wait_ms",
                w.projection_commit_wait_ms as u128,
            ),
            (
                "workers.projection_batch_size",
                w.projection_batch_size as u128,
            ),
            (
                "workers.projection_cohorts_per_cycle",
                w.projection_cohorts_per_cycle as u128,
            ),
            (
                "workers.projection_cohorts_per_source",
                w.projection_cohorts_per_source as u128,
            ),
            (
                "sqlite.busy_timeout_ms",
                self.sqlite.busy_timeout_ms as u128,
            ),
            (
                "sqlite.execution_timeout_ms",
                self.sqlite.execution_timeout_ms as u128,
            ),
            ("parsing.max_candidate_units", p.max_candidate_units as u128),
            (
                "parsing.max_candidate_relationships",
                p.max_candidate_relationships as u128,
            ),
            (
                "parsing.max_candidate_warnings",
                p.max_candidate_warnings as u128,
            ),
            ("parsing.max_unit_body_bytes", p.max_unit_body_bytes as u128),
            ("parsing.process_log_bytes", p.process_log_bytes as u128),
            (
                "parsing.process_read_chunk_bytes",
                p.process_read_chunk_bytes as u128,
            ),
            ("parsing.mupdf_poll_ms", p.mupdf_poll_ms as u128),
            ("parsing.docling_poll_ms", p.docling_poll_ms as u128),
            (
                "parsing.docling_feedback_initial_ms",
                p.docling_feedback_initial_ms as u128,
            ),
            (
                "parsing.docling_feedback_interval_ms",
                p.docling_feedback_interval_ms as u128,
            ),
            (
                "parsing.activity_sample_grace_ms",
                p.activity_sample_grace_ms as u128,
            ),
            ("parsing.activity_poll_ms", p.activity_poll_ms as u128),
            (
                "parsing.cleanup_regex_backtrack_limit",
                p.cleanup_regex_backtrack_limit as u128,
            ),
            ("scheduling.min_interval_ms", s.min_interval_ms as u128),
            ("scheduling.max_backoff_ms", s.max_backoff_ms as u128),
            (
                "scheduling.error_retry_base_ms",
                s.error_retry_base_ms as u128,
            ),
            (
                "scheduling.maintenance_poll_ms",
                s.maintenance_poll_ms as u128,
            ),
            ("diagnostics.max_error_chars", d.max_error_chars as u128),
            (
                "diagnostics.persisted_detail_chars",
                d.persisted_detail_chars as u128,
            ),
            ("diagnostics.error_chain_depth", d.error_chain_depth as u128),
            (
                "diagnostics.model_error_excerpt_chars",
                d.model_error_excerpt_chars as u128,
            ),
            (
                "diagnostics.progress_log_chars",
                d.progress_log_chars as u128,
            ),
            (
                "diagnostics.activity_process_name_chars",
                d.activity_process_name_chars as u128,
            ),
            (
                "diagnostics.activity_error_chars",
                d.activity_error_chars as u128,
            ),
            (
                "diagnostics.identifier_preview_chars",
                d.identifier_preview_chars as u128,
            ),
            ("diagnostics.hash_prefix_chars", d.hash_prefix_chars as u128),
            (
                "diagnostics.sweep_example_paths",
                d.sweep_example_paths as u128,
            ),
        ])?;
        if r.graph_hop_budget > 1 {
            return Err("retrieval.graph_hop_budget supports only 0 or 1".into());
        }
        if r.default_results > r.max_results
            || r.max_results > r.colbert_candidate_pool_size
            || r.reranker_candidate_pool_size > r.colbert_candidate_pool_size
        {
            return Err("retrieval requires default_results <= max_results <= colbert_candidate_pool_size and reranker_candidate_pool_size <= colbert_candidate_pool_size".into());
        }
        if self.sqlite.progress_operations <= 0 {
            return Err("sqlite.progress_operations must be a positive i32".into());
        }
        // sqlite3_busy_timeout takes a signed 32-bit millisecond count; reject
        // narrowing overflow before rusqlite constructs the native timeout.
        if self.sqlite.busy_timeout_ms > i32::MAX as u64 {
            return Err(
                "sqlite.busy_timeout_ms must fit SQLite's signed 32-bit millisecond timeout".into(),
            );
        }
        // Canonical unit identifiers use six-digit ordering; acceptance settings
        // cannot expand that historical identifier format.
        if p.max_candidate_units > 1_000_000 {
            return Err("parsing.max_candidate_units cannot exceed the canonical identifier capacity of 1000000".into());
        }
        // Paired projection rows use 2 * cohorts + 1 for bounded overflow
        // detection. Every other count guard reserves its own sentinel row.
        if m.max_cohorts_per_parse as u128
            > ((i64::MAX as u128 - 1) / 2).min((usize::MAX as u128 - 1) / 2)
        {
            return Err(
                "resources.max_cohorts_per_parse exceeds the SQLite paired-row limit".into(),
            );
        }
        // These guards bind LIMIT n + 1 through usize before SQLite converts
        // to i64. Reserve the sentinel on either supported machine word size.
        for (name, count) in [
            ("resources.max_sources", m.max_sources),
            ("resources.max_annotation_inputs", m.max_annotation_inputs),
            (
                "resources.max_annotation_dependents",
                m.max_annotation_dependents,
            ),
            ("resources.max_parse_units", m.max_parse_units),
            ("resources.max_startup_parses", m.max_startup_parses),
            ("resources.vocabulary_scan_rows", m.vocabulary_scan_rows),
            ("retrieval.max_source_locations", r.max_source_locations),
        ] {
            if count == usize::MAX {
                return Err(format!(
                    "{name} must leave room for one overflow-detection row"
                ));
            }
        }
        // Embedding values are stored as four-byte f32s. Vec byte lengths must
        // fit isize as well as avoiding usize multiplication overflow.
        if m.max_embedding_values
            .checked_mul(std::mem::size_of::<f32>())
            .is_none_or(|bytes| bytes > isize::MAX as usize)
        {
            return Err(
                "resources.max_embedding_values exceeds the addressable f32 byte-buffer limit"
                    .into(),
            );
        }
        for (name, bytes) in [
            ("resources.max_source_body_bytes", m.max_source_body_bytes),
            ("resources.max_json_cell_bytes", m.max_json_cell_bytes),
            ("resources.max_manifest_bytes", m.max_manifest_bytes),
            (
                "resources.model_metadata_max_bytes",
                m.model_metadata_max_bytes,
            ),
            (
                "resources.embedding_read_buffer_bytes",
                m.embedding_read_buffer_bytes,
            ),
            ("parsing.max_unit_body_bytes", p.max_unit_body_bytes),
            ("parsing.process_log_bytes", p.process_log_bytes),
            (
                "parsing.process_read_chunk_bytes",
                p.process_read_chunk_bytes,
            ),
        ] {
            // Bounded readers may retain one extra byte to detect overflow.
            if bytes >= isize::MAX as usize {
                return Err(format!(
                    "{name} must fit an addressable byte buffer plus one sentinel"
                ));
            }
        }
        if m.embedding_read_buffer_bytes < std::mem::size_of::<f32>()
            || !m
                .embedding_read_buffer_bytes
                .is_multiple_of(std::mem::size_of::<f32>())
        {
            return Err(
                "resources.embedding_read_buffer_bytes must be a positive multiple of 4".into(),
            );
        }
        if s.min_interval_ms > s.max_backoff_ms || s.error_retry_base_ms > s.max_backoff_ms {
            return Err(
                "scheduling.min_interval_ms and error_retry_base_ms must not exceed max_backoff_ms"
                    .into(),
            );
        }
        for (name, value) in [
            ("quiet_growth", s.quiet_growth),
            ("backpressure_growth", s.backpressure_growth),
            ("error_growth", s.error_growth),
        ] {
            if !value.is_finite() || value < 1.0 {
                return Err(format!("scheduling.{name} must be finite and at least 1"));
            }
        }
        if !s.inter_change_ema_weight.is_finite()
            || !(0.0..=1.0).contains(&s.inter_change_ema_weight)
            || s.inter_change_ema_weight == 0.0
        {
            return Err(
                "scheduling.inter_change_ema_weight must be finite, greater than 0, and at most 1"
                    .into(),
            );
        }
        Ok(())
    }
}

/// Reserve a sentinel row so bounded SQL readers can detect excess without overflow.
fn positive_values(values: &[(&str, u128)]) -> Result<(), String> {
    for (name, value) in values {
        if *value == 0 || *value >= i64::MAX as u128 {
            return Err(format!("{name} must be between 1 and {}", i64::MAX - 1));
        }
    }
    Ok(())
}
