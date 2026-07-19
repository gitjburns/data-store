//! §24.3 `QueryRequest` envelope, MVP-narrowed (R6). This is the request DTO
//! the `/query` route deserializes and validates before the synchronous
//! retrieval pipeline runs. C8d-1 owns the DTOs + validation; the HTTP boundary
//! logging and the pipeline threading are C8d-2's.
//!
//! Wire conventions mirror `crate::query::model` (§16.2): `camelCase` field
//! names, `deny_unknown_fields` on every struct, `skip_serializing_if` on the
//! spec-optional (`?`) fields so omission stays distinct from an explicit null.
//!
//! MVP NARROWING (R6): the §24.3 `QueryRequest` spec fields `contentTypes`,
//! `timeRange`, `metadataFilters`, `sourceSystems`, `freshness`, `channels`,
//! `rerank`, `includeContradictions`, and `includeFreshnessMetadata` are
//! DELIBERATELY OMITTED from this envelope. Because every struct carries
//! `deny_unknown_fields`, a request naming any of them is REJECTED as an
//! unknown field. This is an additive-later narrowing (those fields can be
//! added back without breaking existing callers), not a spec gap — the MVP
//! query surface intentionally accepts only what the C7 pipeline honors.

use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::query::profile::RetrievalProfile;

/// §24.3 `QueryRequest`: the MVP query envelope. `query_text` is the only
/// required field; everything else defaults per the validation rules below.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct QueryRequest {
    /// The natural-language query (§24.3 `queryText`). Required; validated
    /// non-empty-after-trim and length-capped in `validate`.
    pub(crate) query_text: String,

    /// Opaque §6 caller-context passthrough (§24.3 `callerContext?`). Accepted
    /// per the §6 normative reservation and passed through UNMODIFIED; the MVP
    /// does not interpret or persist it. It is currently UNRECORDED — recording
    /// belongs to the deferred QueryExecutionRecord (QER) audit tier — so it is
    /// validated only for well-formed JSON, never read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) caller_context: Option<serde_json::Value>,

    /// Scope constraints (§24.3 `constraints?`); resolved to a `ResolvedScope`
    /// by C8d-2's `resolve_scope`, never here.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) constraints: Option<QueryConstraints>,

    /// Per-request retrieval-policy overrides (§24.3 `retrievalPolicy?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) retrieval_policy: Option<RetrievalPolicyRequest>,

    /// Per-request evidence-policy overrides (§24.3 `evidencePolicy?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) evidence_policy: Option<EvidencePolicyRequest>,

    /// When true, the response carries raw stage diagnostics (§24.3 `debug?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) debug: Option<bool>,
}

/// §24.3 `QueryConstraints` (MVP subset): the scope-relevant constraint fields
/// the C7 pipeline honors. `resolve_scope` (C8d-2) conjoins these into a
/// `ResolvedScope`; both empty ⇒ default all-sources scope.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct QueryConstraints {
    /// Explicit source ids to scope to (§24.3 `sourceIds?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_ids: Option<Vec<String>>,
    /// Governance domains to scope to (§24.3 `governanceDomains?`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) governance_domains: Option<Vec<String>>,
}

/// §24.3 `RetrievalPolicy` (MVP subset): the one per-request retrieval override
/// the MVP honors. Bounded against the active `RetrievalProfile` in `validate`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RetrievalPolicyRequest {
    /// Requested final evidence-unit count (§24.3 `maxFinalEvidenceUnits?`).
    /// When present must be `1..=profile.max_top_k`; absent ⇒
    /// `profile.default_top_k`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_final_evidence_units: Option<u32>,
}

/// §24.3 `EvidencePolicy` (MVP subset): the three assembly output toggles the
/// EvidencePack builder (C8b) honors. Defaults are resolved in `validate`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct EvidencePolicyRequest {
    /// Include per-unit source locators (§24.3 `includeSourceLocators?`);
    /// default `true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) include_source_locators: Option<bool>,
    /// Include the traversed relationships (§24.3 `includeRelationships?`);
    /// default `false`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) include_relationships: Option<bool>,
    /// Include semantic annotations for selected units (§24.3
    /// `includeAnnotations?`); default `false`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) include_annotations: Option<bool>,
}

/// A validated, defaults-resolved query ready for the pipeline. Validation
/// produces this OWNED struct rather than mutating the request in place so the
/// handler threads one coherent set of resolved values (caps applied, defaults
/// filled) into `execute_query` without re-deriving them or re-reading the
/// profile. The raw `QueryRequest` stays the wire contract; `ValidatedQuery` is
/// the internal, already-checked shape.
#[derive(Debug, Clone)]
pub(crate) struct ValidatedQuery {
    /// Trimmed, length-checked query text.
    pub(crate) query_text: String,
    /// Opaque caller context, passed through unmodified (currently unrecorded).
    // Genuinely dead at C8d-2: the §6 passthrough is validated as well-formed
    // JSON but has no consumer this cluster — recording it belongs to the
    // deferred QueryExecutionRecord (QER) audit tier. Kept on the validated
    // shape (not dropped) so the QER tier reads it here without re-plumbing the
    // request. Exposed when the mod-level query allow was removed at C8d-2.
    #[allow(dead_code)]
    pub(crate) caller_context: Option<serde_json::Value>,
    /// Explicit source-id scope constraint (may be empty; scope resolution
    /// treats empty as no-constraint). Threaded to C8d-2's `resolve_scope`.
    pub(crate) source_ids: Option<Vec<String>>,
    /// Governance-domain scope constraint. Threaded to `resolve_scope`.
    pub(crate) governance_domains: Option<Vec<String>>,
    /// Final evidence-unit count, resolved against the profile
    /// (`1..=max_top_k`, default `default_top_k`).
    pub(crate) max_final_evidence_units: u32,
    /// Resolved evidence-output policy handed to the assembly stage.
    pub(crate) evidence_options: EvidenceOptions,
    /// Whether to attach raw stage diagnostics to the response.
    pub(crate) debug: bool,
}

/// Resolved §24.3 `EvidencePolicy` toggles with MVP defaults applied. Handed to
/// C8b's `build_evidence_pack` to gate locators/relationships/annotations.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EvidenceOptions {
    /// Include per-unit source locators in the pack (default `true`).
    pub(crate) include_source_locators: bool,
    /// Include the operator-traversed relationships (default `false`).
    pub(crate) include_relationships: bool,
    /// Include selected-unit semantic annotations (default `false`).
    pub(crate) include_annotations: bool,
}

impl QueryRequest {
    /// Validate the request against the active retrieval profile and the server
    /// query-length cap, producing a defaults-resolved `ValidatedQuery`.
    ///
    /// Validation is a request boundary: every rejection is a typed
    /// `ApiError::BadRequest` naming the rejected field and the safe cap/limit
    /// (never the offending value's contents), so the HTTP layer (C8d-2) can
    /// log field/cap/status/error_kind without leaking payload.
    ///
    /// `max_search_query_chars` is `config.server.max_search_query_chars`; this
    /// validator is its FIRST functional reader.
    pub(crate) fn validate(
        &self,
        profile: &RetrievalProfile,
        max_search_query_chars: u32,
    ) -> Result<ValidatedQuery, ApiError> {
        // query_text: required, non-empty after trim. An all-whitespace query
        // carries no retrievable signal, so it is a client error, not an empty
        // result. The trimmed form is what the pipeline retrieves against.
        let query_text = self.query_text.trim();
        if query_text.is_empty() {
            return Err(ApiError::BadRequest {
                message: "queryText must not be empty".to_string(),
            });
        }
        // Length cap is a validation limit, not a transport body limit, so it
        // surfaces as `BadRequest` naming the field and cap (the honest choice:
        // `json_rejection_to_api_error` reserves `PayloadTooLarge` for the axum
        // body-size rejection, i.e. bytes over the wire; a post-parse character
        // cap on one field is a field-validation failure). Count characters,
        // not bytes, so the cap is stable across multibyte input.
        let query_len = query_text.chars().count();
        if query_len as u64 > u64::from(max_search_query_chars) {
            return Err(ApiError::BadRequest {
                message: format!(
                    "queryText exceeds maximum length of {max_search_query_chars} characters"
                ),
            });
        }

        // max_final_evidence_units: when present, must fall in
        // `1..=profile.max_top_k`; absent defaults to `profile.default_top_k`.
        let max_final_evidence_units = match self
            .retrieval_policy
            .as_ref()
            .and_then(|policy| policy.max_final_evidence_units)
        {
            Some(requested) => {
                if requested < 1 || requested > profile.max_top_k {
                    return Err(ApiError::BadRequest {
                        message: format!(
                            "maxFinalEvidenceUnits must be between 1 and {}",
                            profile.max_top_k
                        ),
                    });
                }
                requested
            }
            None => profile.default_top_k,
        };

        // Evidence-policy defaults (R6): locators on, relationships/annotations
        // off. Scope constraints are surfaced verbatim for C8d-2's
        // `resolve_scope` — this validator never resolves scope.
        let evidence_options = EvidenceOptions {
            include_source_locators: self
                .evidence_policy
                .as_ref()
                .and_then(|policy| policy.include_source_locators)
                .unwrap_or(true),
            include_relationships: self
                .evidence_policy
                .as_ref()
                .and_then(|policy| policy.include_relationships)
                .unwrap_or(false),
            include_annotations: self
                .evidence_policy
                .as_ref()
                .and_then(|policy| policy.include_annotations)
                .unwrap_or(false),
        };

        let (source_ids, governance_domains) = match &self.constraints {
            Some(constraints) => (
                constraints.source_ids.clone(),
                constraints.governance_domains.clone(),
            ),
            None => (None, None),
        };

        Ok(ValidatedQuery {
            query_text: query_text.to_string(),
            caller_context: self.caller_context.clone(),
            source_ids,
            governance_domains,
            max_final_evidence_units,
            evidence_options,
            debug: self.debug.unwrap_or(false),
        })
    }
}
