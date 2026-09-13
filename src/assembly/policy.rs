//! Configuration-backed, self-hashed policy for retaining final passage evidence.
//! Snapshot capture retains this document's identity; runtime never mutates it.

use crate::error::ApiError;
use crate::limits::RetrievalLimits;

use crate::assembly::model::{
    AssemblyBudget, AssemblyCondition, AssemblyOperation, AssemblyOperator, AssemblyPolicy,
    AssemblyReason, AssemblyRule,
};

/// JSON field name the policy stores its self-hash under. Named beside the
/// producer so the `seal_mvp_policy` builder and the
/// `canonical_sha256_hex_without_field` call cannot drift onto differently
/// named hash fields (mirrors `crate::annotations::policy`).
const POLICY_HASH_JSON_KEY: &str = "policyHash";

/// Fixed authoring timestamp of the sealed v3 policy document. A versioned
/// document's identity (id + version + hash) must be stable across processes,
/// so `createdAt` is a FIXED string and never the runtime clock — otherwise the
/// self-hash would change every boot. Mirrors `seal_mvp_policy` in
/// `crate::annotations::policy` and `seal_mvp_profile` in `crate::query::profile`.
const POLICY_CREATED_AT: &str = "2026-09-12T00:00:00.000Z";

/// Stable trace rule identity shared by the sealed document and raw assembler.
pub(crate) const SELECTED_PASSAGE_RULE_ID: &str = "selected-passage";

/// Seal only effective retention budgets; unused graph-expansion controls are
/// absent from v3. Historical snapshot JSON is preserved in its original version.
pub(crate) fn from_limits(limits: &RetrievalLimits) -> Result<AssemblyPolicy, ApiError> {
    let mut policy = AssemblyPolicy {
        id: "assembly-policy".to_string(),
        version: "3".to_string(),
        description: Some(
            "Retain full canonical members of final ranked passages, in passage rank and reading order; no automatic expansion. Raw safety limits fail visibly."
                .to_string(),
        ),
        budgets: AssemblyBudget {
            // Complete bodies use independent configured budgets; display-window
            // tuning must not silently change raw evidence admission.
            max_evidence_units: limits.raw_evidence_max_units,
            max_tokens: limits.raw_evidence_max_tokens,
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
