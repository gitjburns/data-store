//! Configuration-backed retrieval profiles retain deterministic, self-hashed
//! identities. A runtime owns its sealed document; queries never read TOML or
//! a mutable global profile. Historical snapshots retain their original documents.

use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::limits::RetrievalLimits;
use crate::query::model::{ResolvedScope, ResolvedScopeKind, RetrievalChannel};

/// Version five carries one authoritative configured limits object. Algorithm
/// labels describe implemented behavior; they are not unused operator switches.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RetrievalProfile {
    /// Stable profile identity (§24.2 `id`).
    pub(crate) id: String,
    /// Profile version; part of the profile's audited identity (§24.2 `version`).
    pub(crate) version: String,

    /// Discovery mechanisms retained in attribution. Graph and semantic share
    /// one annotation contribution in final fusion; ColBERT scores the fused pool.
    pub(crate) default_channels: Vec<RetrievalChannel>,
    /// Whether the final reranker runs by default (§24.2 `defaultRerank`).
    pub(crate) default_rerank: bool,
    /// Fusion strategy tag (§24.2 `defaultFusionStrategy?`), including grouping.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) default_fusion_strategy: Option<String>,

    pub(crate) limits: RetrievalLimits,

    /// Fixed authoring timestamp of this versioned document (§24.2 `createdAt`);
    /// never the runtime clock, so identical configuration has identical identity.
    pub(crate) created_at: String,
    /// Self-hash over the canonical document minus this field (§16.2, §24.2
    /// `profileHash`).
    pub(crate) profile_hash: String,
}

/// Seal the validated configuration alongside the fixed three-contribution RRF
/// and best-representation MaxSim algorithms. Construction occurs once per runtime.
pub(crate) fn from_limits(limits: &RetrievalLimits) -> Result<RetrievalProfile, ApiError> {
    let mut profile = RetrievalProfile {
        id: "retrieval-profile".to_string(),
        version: "5".to_string(),

        // Attribution distinguishes discovery mechanisms from final fusion groups.
        default_channels: vec![
            RetrievalChannel::Dense,
            RetrievalChannel::Lexical,
            RetrievalChannel::Graph,
            RetrievalChannel::Semantic,
        ],
        default_rerank: true,
        default_fusion_strategy: Some("grouped_rrf".to_string()),

        limits: *limits,

        // Fixed authoring timestamp of this versioned document; see fn comment.
        created_at: "2026-09-12T00:00:00.000Z".to_string(),
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
