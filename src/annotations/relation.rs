//! CAb: relation producer — per-section-group relation extraction (spec §21).
//!
//! Prompt and strict output parsing for the relation producer. The system
//! prompt is a named external-language constant (PRINCIPLES); the model must
//! return a bare JSON object of the exact shape parsed below, no prose and no
//! code fences. Parsing is strict: `deny_unknown_fields`, non-empty subject /
//! predicate / object strings, and confidence (when present) within [0,1] —
//! out of range is an error, never clamped. An empty `relations` array is a
//! valid result; a malformed response is `ApiError::AnnotationProducer` with
//! bounded diagnostics only.

use serde::Deserialize;
use serde_json::json;

use crate::annotations::producer::{
    ProducedAnnotation, strict_from_str, strip_optional_code_fence, validate_confidence,
    validate_non_empty,
};
use crate::error::ApiError;

/// System prompt for the relation producer. Fixes the task and the exact
/// bare-JSON response contract the parser below enforces.
///
/// This is the BASE prompt: the EFFECTIVE (sent and hashed) prompt is this
/// text composed with the operator's naming rules by `ProducerKind::prompt`
/// (CA2-P3, user-ruled 2026-07-19); an empty naming document composes to
/// exactly these bytes. Editing this constant — like editing the naming
/// document — is a producer-identity change (promptHash) that invalidates
/// memo reuse.
pub(crate) const SYSTEM_PROMPT: &str = "You are a relation extraction component. \
Read the provided text and extract the subject-predicate-object relations it states. \
Respond with a single bare JSON object and nothing else: no prose, no explanation, and no Markdown code fences. \
The object must have exactly this shape: \
{\"relations\":[{\"subject\":\"<subject>\",\"predicate\":\"<predicate>\",\"object\":\"<object>\",\"confidence\":<number 0..1, optional>}]}. \
Each relation's subject, predicate, and object must be non-empty strings. \
Include the confidence field only when you can report a calibrated value between 0 and 1 inclusive. \
If the text states no relations, return {\"relations\":[]}.";

/// Strict deserialization target for the relation response.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RelationResponse {
    relations: Vec<RelationItem>,
}

/// One extracted relation as returned by the model.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RelationItem {
    subject: String,
    predicate: String,
    object: String,
    #[serde(default)]
    confidence: Option<f64>,
}

/// Parse the relation producer's raw output into one `ProducedAnnotation` per
/// relation. Empty `relations` yields an empty vec. The per-relation body is
/// `{"subject","predicate","object"}` in camelCase.
pub(crate) fn parse_output(raw: &str) -> Result<Vec<ProducedAnnotation>, ApiError> {
    let json_text = strip_optional_code_fence(raw);
    let response: RelationResponse = strict_from_str(json_text, "relation", raw)?;

    let mut produced = Vec::with_capacity(response.relations.len());
    for relation in response.relations {
        validate_non_empty(&relation.subject, "relation.subject", raw)?;
        validate_non_empty(&relation.predicate, "relation.predicate", raw)?;
        validate_non_empty(&relation.object, "relation.object", raw)?;
        validate_confidence(relation.confidence, "relation.confidence", raw)?;
        produced.push(ProducedAnnotation {
            body: json!({
                "subject": relation.subject,
                "predicate": relation.predicate,
                "object": relation.object,
            }),
            confidence: relation.confidence,
        });
    }
    Ok(produced)
}
