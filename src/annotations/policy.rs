//! CAd: required-annotation-set policy (spec §21.4) — versioned, hashed,
//! MVP content "nothing blocks activation".
//!
//! §21.4 rules which annotation types must be fresh BEFORE activation versus
//! which build AFTER activation with visible freshness. The spec makes this a
//! visible, versioned policy DOCUMENT (like the C7a RetrievalProfile), not a
//! runtime tuning knob — so it is defined here as code and self-hashed over
//! its canonical serialization, exactly as declared capability profiles are.
//! §35 keeps policy out of operational config; config may later hold a PATH to
//! an external policy document without changing any consumer, because
//! consumers read `active_policy()` and never a config field.

use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::model::SemanticAnnotationType;

/// JSON field name the policy stores its self-hash under. Named beside the
/// document so the producer and the `canonical_sha256_hex_without_field` call
/// cannot drift onto differently named hash fields.
const POLICY_HASH_JSON_KEY: &str = "policyHash";

/// Spec §21.4 required-annotation-set policy: the versioned, hashed ruling on
/// which annotation types gate activation. `blocking_types` must be fresh
/// BEFORE a parse activates; `post_activation_types` build AFTER activation
/// and expose their build state through annotation freshness (§21 rule 3).
/// `deny_unknown_fields` so an externally supplied document (a future §35
/// config path) cannot smuggle unread fields past this consumer.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RequiredAnnotationSetPolicy {
    pub(crate) id: String,
    pub(crate) version: String,
    pub(crate) description: String,

    /// Annotation types that must be fresh before activation. Empty under the
    /// MVP content: nothing blocks activation.
    pub(crate) blocking_types: Vec<SemanticAnnotationType>,

    /// Annotation types built after activation with visible freshness.
    pub(crate) post_activation_types: Vec<SemanticAnnotationType>,

    pub(crate) created_at: String,
    pub(crate) policy_hash: String,
}

/// The active required-annotation-set policy document (spec §21.4), lazily
/// sealed once and reused. Fallible because sealing computes the self-hash,
/// which serializes the document and can fail loudly on a serde rename; the
/// crate's explicit-`Result` policy forbids panicking in the `OnceLock`
/// initializer, so the resolved `Result` is stored and each caller re-observes
/// any sealing failure. Consumers (the activation prerequisite seam) read the
/// document here rather than by construction, so a future policy version with
/// a non-empty `blocking_types` set changes behavior without changing them.
pub(crate) fn active_policy() -> Result<&'static RequiredAnnotationSetPolicy, ApiError> {
    static POLICY: OnceLock<Result<RequiredAnnotationSetPolicy, ApiError>> = OnceLock::new();
    // `ApiError` is not `Clone`, so a stored sealing failure is re-surfaced by
    // reconstructing an `InternalIo` from its rendered message. Sealing only
    // ever fails with `InternalIo` (canonicalization), so the rendered text is
    // faithful and the source context is preserved.
    match POLICY.get_or_init(seal_mvp_policy) {
        Ok(policy) => Ok(policy),
        Err(source) => Err(ApiError::InternalIo {
            message: source.to_string(),
        }),
    }
}

/// Build and seal the MVP §21.4 policy content. The MVP ruling: NOTHING blocks
/// activation — the three MVP annotation types (entity, relation, summary) all
/// build post-activation with visible freshness, so `blocking_types` is empty.
/// `created_at` is a FIXED authoring timestamp, not runtime state: this is a
/// versioned document whose identity (id + version + hash) must be stable
/// across processes, so it must never be stamped with the current clock.
fn seal_mvp_policy() -> Result<RequiredAnnotationSetPolicy, ApiError> {
    let mut policy = RequiredAnnotationSetPolicy {
        id: "required-annotation-set".to_string(),
        version: "1".to_string(),
        description: "MVP required-annotation-set policy (spec §21.4): nothing blocks \
                      activation; the entity, relation, and summary annotation types all \
                      build post-activation with visible freshness."
            .to_string(),
        blocking_types: Vec::new(),
        post_activation_types: vec![
            SemanticAnnotationType::Entity,
            SemanticAnnotationType::Relation,
            SemanticAnnotationType::Summary,
        ],
        // Fixed authoring timestamp of this versioned document; see fn comment.
        created_at: "2026-07-13T00:00:00.000Z".to_string(),
        policy_hash: String::new(),
    };
    // Self-hash over the document minus its own hash field (spec §16.2 rules);
    // the shared helper fails loudly if a serde rename ever drops the field.
    policy.policy_hash =
        crate::canonical::canonical_sha256_hex_without_field(&policy, POLICY_HASH_JSON_KEY)?;
    Ok(policy)
}
