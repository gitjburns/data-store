//! Compile-sealed, self-hashed policy for retaining final passage evidence.
//! Snapshot capture retains this document's identity; runtime never mutates it.

use std::sync::OnceLock;

use crate::error::ApiError;

use crate::assembly::model::{
    AssemblyBudget, AssemblyCondition, AssemblyOperation, AssemblyOperator, AssemblyPolicy,
    AssemblyReason, AssemblyRule, MAX_PASSAGE_UNITS, MAX_QUERY_RESULTS,
};

/// JSON field name the policy stores its self-hash under. Named beside the
/// producer so the `seal_mvp_policy` builder and the
/// `canonical_sha256_hex_without_field` call cannot drift onto differently
/// named hash fields (mirrors `crate::annotations::policy`).
const POLICY_HASH_JSON_KEY: &str = "policyHash";

/// Fixed authoring timestamp of the sealed v2 policy document. A versioned
/// document's identity (id + version + hash) must be stable across processes,
/// so `createdAt` is a FIXED string and never the runtime clock — otherwise the
/// self-hash would change every boot. Mirrors `seal_mvp_policy` in
/// `crate::annotations::policy` and `seal_mvp_profile` in `crate::query::profile`.
const POLICY_CREATED_AT: &str = "2026-09-06T00:00:00.000Z";

/// The active MVP `AssemblyPolicy` document (§25), lazily sealed once and
/// reused. Fallible because sealing computes the self-hash, which serializes the
/// document and can fail loudly on a serde rename; the crate's explicit-`Result`
/// policy forbids panicking in the `OnceLock` initializer, so the resolved
/// `Result` is stored and each caller re-observes any sealing failure.
/// Capture and assembly share this one immutable document and its safety bounds.
pub(crate) fn active_policy() -> Result<&'static AssemblyPolicy, ApiError> {
    static POLICY: OnceLock<Result<AssemblyPolicy, ApiError>> = OnceLock::new();
    // `ApiError` is not `Clone`, so a stored sealing failure is re-surfaced by
    // reconstructing an `InternalIo` from its rendered message. Sealing only
    // ever fails with `InternalIo` (canonicalization), so the rendered text is
    // faithful and the source context is preserved. Mirrors
    // `crate::annotations::policy::active_policy`.
    match POLICY.get_or_init(seal_mvp_policy) {
        Ok(policy) => Ok(policy),
        Err(source) => Err(ApiError::InternalIo {
            message: source.to_string(),
        }),
    }
}

/// Stable trace rule identity shared by the sealed document and raw assembler.
pub(crate) const SELECTED_PASSAGE_RULE_ID: &str = "selected-passage";

/// Seal the passage-retention policy with deterministic identity and safety limits.
fn seal_mvp_policy() -> Result<AssemblyPolicy, ApiError> {
    let max_raw_units = (MAX_QUERY_RESULTS * MAX_PASSAGE_UNITS) as u32;
    let mut policy = AssemblyPolicy {
        id: "assembly-policy".to_string(),
        version: "2".to_string(),
        description: Some(
            "Retain full canonical members of final ranked passages, in passage rank and reading order; no automatic expansion. Raw safety limits fail visibly."
                .to_string(),
        ),
        budgets: AssemblyBudget {
            // Raw bodies are retained independently of the displayed 512-token
            // passage and requested result count. Never silently trim this pack.
            max_evidence_units: max_raw_units,
            max_tokens: max_raw_units * crate::projections::MAX_UNIT_TOKENS,
            max_expansion_depth: 0,
            max_referenced_units: None,
        },
        rules: vec![selected_passage_rule()],
        requires_relationship_types: None,
        created_at: POLICY_CREATED_AT.to_string(),
        created_by: None,
        policy_hash: String::new(),
    };
    policy.policy_hash =
        crate::canonical::canonical_sha256_hex_without_field(&policy, POLICY_HASH_JSON_KEY)?;
    Ok(policy)
}

/// Attribute every retained canonical member to the already selected passage.
fn selected_passage_rule() -> AssemblyRule {
    AssemblyRule {
        id: SELECTED_PASSAGE_RULE_ID.to_string(),
        description: Some("Retain final passage members without graph expansion.".to_string()),
        when: AssemblyCondition {
            hit_type: None,
            content_type: None,
            has_outgoing_relationships: None,
            has_incoming_relationships: None,
        },
        apply: vec![AssemblyOperation {
            operator: AssemblyOperator::IncludeSelectedPassage,
            parameters: None,
        }],
        reason: AssemblyReason::SelectedPassage,
    }
}

/// Captured source/parse identity used to validate raw evidence membership.
pub(crate) struct CapturedParseRef<'a> {
    pub(crate) source_id: &'a str,
    pub(crate) parse_id: &'a str,
}
