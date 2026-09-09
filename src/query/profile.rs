//! Retrieval profile (§24.2): the versioned, self-hashed `RetrievalProfile`
//! with fixed MVP retrieval knobs, plus scope resolution (query constraints →
//! `ResolvedScope`). Content landed with package C7a; the `/query` route
//! (C8d-2) is now its live consumer — `resolve_scope` runs per request and the
//! profile's knobs size the pipeline.
//!
//! §24.2 makes the query planner's defaults come from a *visible, versioned*
//! `RetrievalProfile`, not from operational config — so the MVP profile is a
//! code document sealed by a self-hash over its canonical serialization (§16.2),
//! exactly as declared capability profiles and the §21.4 policy are. No
//! retrieval knob is ever a config key: the profile's identity (id + version +
//! hash) must be stable and auditable across processes, which a mutable config
//! surface cannot guarantee. A future §35 config path may point at an external
//! profile *document* without changing any consumer, because consumers read
//! `active_profile()` and never a config field.
//!
//! The abstract §24.2 shape (`defaultChannels`, `defaultMaxCandidatesPerChannel`,
//! `defaultMaxFinalEvidenceUnits`, `defaultRerank`, `defaultFusionStrategy?`)
//! is preserved field-for-field; the concrete C7 MVP tuning knobs the spec's
//! abstract shape does not name (`rrf_k`,
//! `colbert_candidate_pool_size`, `reranker_candidate_pool_size`,
//! `graph_hop_budget`) are added as additional profile fields and covered by
//! the same self-hash. Candidate budgets and passage ranking use the sealed v3
//! values; the graph hop budget remains one.

use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::assembly::model::MAX_QUERY_RESULTS;
use crate::error::ApiError;
use crate::query::model::{ResolvedScope, ResolvedScopeKind, RetrievalChannel};

/// Spec §24.2 `RetrievalProfile`: the versioned, self-hashed document the query
/// planner draws its defaults from. The abstract §24.2 fields (`id`, `version`,
/// `default_channels`, `default_max_candidates_per_channel`,
/// `default_max_final_evidence_units`, `default_rerank`,
/// `default_fusion_strategy?`, `created_at`, `profile_hash`) are carried
/// verbatim; the MVP tuning knobs the spec shape does not name are added as
/// further fields and are covered by the same self-hash.
///
/// `deny_unknown_fields` so a future externally supplied profile document
/// (a §35 config path) cannot smuggle unread fields past this consumer;
/// `profile_hash` (spec §16.2) covers every field except itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RetrievalProfile {
    /// Stable profile identity (§24.2 `id`).
    pub(crate) id: String,
    /// Profile version; part of the profile's audited identity (§24.2 `version`).
    pub(crate) version: String,

    /// Channels retrieved by default (§24.2 `defaultChannels`). MVP set is
    /// exactly dense, lexical, graph (`multi_vector` channel deferred post-MVP,
    /// 2026-07-15 rescope).
    pub(crate) default_channels: Vec<RetrievalChannel>,
    /// Per-channel candidate cap before fusion (§24.2
    /// `defaultMaxCandidatesPerChannel`). MVP maps this to the dense/lexical
    /// candidate pool size (= `colbert_candidate_pool_size`), the fused pool
    /// MaxSim then re-scores.
    pub(crate) default_max_candidates_per_channel: u32,
    /// Final ranked-evidence-unit cap returned to context assembly (§24.2
    /// `defaultMaxFinalEvidenceUnits`). MVP maps this to `default_top_k`.
    pub(crate) default_max_final_evidence_units: u32,
    /// Whether the final reranker runs by default (§24.2 `defaultRerank`).
    pub(crate) default_rerank: bool,
    /// Fusion strategy tag (§24.2 `defaultFusionStrategy?`). MVP fuses with RRF.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) default_fusion_strategy: Option<String>,

    // --- MVP tuning knobs (not named by the abstract §24.2 shape; approved
    // plan values 2026-07-15, D9 for the hop budget). Covered by the self-hash.
    /// Default result count when a query does not request its own top-k.
    pub(crate) default_top_k: u32,
    /// Hard ceiling on a query's requested top-k.
    pub(crate) max_top_k: u32,
    /// Reciprocal-rank-fusion rank constant `k` (RRF `1/(k+rank)`).
    pub(crate) rrf_k: u32,
    /// Global section-window shortlist before nomination of canonical units.
    pub(crate) section_candidate_limit: u32,
    /// Eligible units nominated per section window by their best fine-chunk cosine.
    pub(crate) section_passages_per_window: u32,
    /// Fused-pool size fed to ColBERT MaxSim re-scoring.
    pub(crate) colbert_candidate_pool_size: u32,
    /// Minimum passage candidate depth for final reranking; requests for more
    /// results raise it within the fused-candidate ceiling.
    pub(crate) reranker_candidate_pool_size: u32,
    /// Graph channel one-hop traversal budget (D9: hop budget = 1).
    pub(crate) graph_hop_budget: u32,

    /// Fixed authoring timestamp of this versioned document (§24.2 `createdAt`);
    /// never the runtime clock — see `seal_mvp_profile`.
    pub(crate) created_at: String,
    /// Self-hash over the canonical document minus this field (§16.2, §24.2
    /// `profileHash`).
    pub(crate) profile_hash: String,
}

/// The active retrieval-profile document (spec §24.2), lazily sealed once and
/// reused. Fallible because sealing computes the self-hash, which serializes
/// the document and can fail loudly on a serde rename; the crate's
/// explicit-`Result` policy forbids panicking in the `OnceLock` initializer, so
/// the resolved `Result` is stored and each caller re-observes any sealing
/// failure. Consumers (the C7 pipeline) read the profile here rather than by
/// construction, so a future profile version changes behavior without changing
/// them.
pub(crate) fn active_profile() -> Result<&'static RetrievalProfile, ApiError> {
    static PROFILE: OnceLock<Result<RetrievalProfile, ApiError>> = OnceLock::new();
    // `ApiError` is not `Clone`, so a stored sealing failure is re-surfaced by
    // reconstructing an `InternalIo` from its rendered message. Sealing only
    // ever fails with `InternalIo` (canonicalization), so the rendered text is
    // faithful and the source context is preserved. This mirrors
    // `annotations/policy.rs::active_policy`.
    match PROFILE.get_or_init(seal_mvp_profile) {
        Ok(profile) => Ok(profile),
        Err(source) => Err(ApiError::InternalIo {
            message: source.to_string(),
        }),
    }
}

/// Seal retrieval v3: passage and section lists fuse inside the dense channel,
/// which retains one vote alongside lexical and graph in outer RRF.
///
/// `created_at` is a FIXED authoring timestamp, not runtime state: this is a
/// versioned document whose identity (id + version + hash) must be stable
/// across processes, so it must never be stamped with the current clock —
/// mirroring `annotations/policy.rs::seal_mvp_policy`.
fn seal_mvp_profile() -> Result<RetrievalProfile, ApiError> {
    let default_results = 10;
    let candidate_pool = 100;
    let mut profile = RetrievalProfile {
        id: "retrieval-profile".to_string(),
        version: "3".to_string(),

        // MVP channel set: dense, lexical, graph (multi_vector channel deferred
        // post-MVP, 2026-07-15 rescope).
        default_channels: vec![
            RetrievalChannel::Dense,
            RetrievalChannel::Lexical,
            RetrievalChannel::Graph,
        ],
        // Per-channel candidate cap = the ColBERT-fed pool size (100).
        default_max_candidates_per_channel: candidate_pool,
        // The public legacy field now caps final passages, not seed units.
        default_max_final_evidence_units: default_results,
        default_rerank: true,
        default_fusion_strategy: Some("rrf".to_string()),

        default_top_k: default_results,
        max_top_k: MAX_QUERY_RESULTS as u32,
        rrf_k: 60,
        section_candidate_limit: 20,
        section_passages_per_window: 5,
        colbert_candidate_pool_size: candidate_pool,
        reranker_candidate_pool_size: 30,
        graph_hop_budget: 1,

        // Fixed authoring timestamp of this versioned document; see fn comment.
        created_at: "2026-09-08T00:00:00.000Z".to_string(),
        profile_hash: String::new(),
    };
    // Self-hash over the document minus its own hash field (spec §16.2 rules);
    // the shared helper fails loudly if a serde rename ever drops the field.
    profile.profile_hash = crate::canonical::canonical_sha256_hex_without_field(
        &profile,
        crate::canonical::PROFILE_HASH_JSON_KEY,
    )?;
    Ok(profile)
}

/// Scope-relevant inputs a query carries into scope resolution. This is the
/// minimal shape `resolve_scope` needs — the full §24.3 `QueryRequest` /
/// `QueryConstraints` envelope is C8d's (deferred, 2026-07-15); C8d passes the
/// scope-relevant constraint fields through this borrow so C7a does not depend
/// on the envelope's shape. `source_ids` and `governance_domains` correspond to
/// the identically named §24.3 `QueryConstraints` fields.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ScopeInput<'a> {
    /// Explicit source ids requested by the query (§24.3 `sourceIds?`).
    pub(crate) source_ids: Option<&'a [String]>,
    /// Governance domains requested by the query (§24.3 `governanceDomains?`).
    pub(crate) governance_domains: Option<&'a [String]>,
}

/// Resolve a query's scope-relevant constraints to a `ResolvedScope` (§24.2).
///
/// Default (no scope constraints) resolves to `kind: All` — §6 reservation 1:
/// the default scope is all sources. An explicit `source_ids` set alone resolves
/// to `SourceSet`; a `governance_domains` set alone resolves to `DomainSet`.
///
/// CONJUNCTION (R3, §24.3): when BOTH `source_ids` AND `governance_domains` are
/// present and non-empty the constraints CONJOIN — the query reduces to
/// source-set scoping (§6 "reduce to source-set scoping") narrowed to sources
/// that ALSO have a current location in one of the domains. The result is a
/// `SourceSet` scope carrying BOTH fields populated; the capture layer
/// (`execute.rs`) then applies both predicates (`id IN (…)` AND a `current`
/// location in the domains). This SUPERSEDES the earlier precedence rule where
/// `source_ids` alone won — both-present is now an intersection, not a pick.
///
/// The resolved scope bounds the *captured active set* the channels read; it is
/// enforced at candidate generation and is NEVER applied as a post-filter over
/// ranked hits (§6, §38, §24.2). This function only classifies intent; the
/// pipeline (C7d) uses the result to confine the captured parses. An empty
/// intersection is a legitimate empty result at capture, never an error here.
pub(crate) fn resolve_scope(input: ScopeInput<'_>) -> ResolvedScope {
    // Treat an explicitly empty set the same as absent: an empty selection is
    // not a request to scan nothing, it is no constraint (default = all).
    let source_ids = input.source_ids.filter(|ids| !ids.is_empty());
    let governance_domains = input.governance_domains.filter(|d| !d.is_empty());

    match (source_ids, governance_domains) {
        // Both present: CONJOIN into a source-set scope carrying both predicates
        // (R3). Capture applies `id IN (source_ids)` AND a current-location
        // domain filter; the empty intersection is an empty capture, not an error.
        (Some(ids), Some(domains)) => ResolvedScope {
            kind: ResolvedScopeKind::SourceSet,
            governance_domains: Some(domains.to_vec()),
            source_ids: Some(ids.to_vec()),
        },
        // Source set alone: capture by id only.
        (Some(ids), None) => ResolvedScope {
            kind: ResolvedScopeKind::SourceSet,
            governance_domains: None,
            source_ids: Some(ids.to_vec()),
        },
        (None, Some(domains)) => ResolvedScope {
            kind: ResolvedScopeKind::DomainSet,
            governance_domains: Some(domains.to_vec()),
            source_ids: None,
        },
        // §6 reservation 1: default scope is all sources.
        (None, None) => ResolvedScope {
            kind: ResolvedScopeKind::All,
            governance_domains: None,
            source_ids: None,
        },
    }
}
