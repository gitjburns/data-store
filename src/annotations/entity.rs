//! CAb: entity producer — per-section-group entity extraction (spec §21).
//!
//! Prompt and strict output parsing for the entity producer. The system prompt
//! is a named external-language constant (PRINCIPLES); the model is required to
//! return a bare JSON object of the exact shape parsed below, no prose and no
//! code fences. Parsing is strict: `deny_unknown_fields`, non-empty required
//! strings, and confidence (when present) within [0,1] — an out-of-range
//! confidence is an error, never clamped (accuracy principle). An empty
//! `entities` array is a valid result (no entities found is a result, not a
//! failure); a malformed response is `ApiError::AnnotationProducer` carrying
//! only bounded diagnostics, never the full model output.

use serde::Deserialize;
use serde_json::json;

use crate::annotations::producer::{
    ProducedAnnotation, strict_from_str, strip_optional_code_fence, validate_confidence,
    validate_non_empty,
};
use crate::error::ApiError;

/// System prompt for the entity producer. Fixes the task and the exact
/// bare-JSON response contract the parser below enforces.
///
/// This is the BASE prompt: the EFFECTIVE (sent and hashed) prompt is this
/// text composed with the operator's naming rules by `ProducerKind::prompt`
/// (CA2-P3, user-ruled 2026-07-19); an empty naming document composes to
/// exactly these bytes. Editing this constant — like editing the naming
/// document — is a producer-identity change (promptHash) that invalidates
/// memo reuse.
pub(crate) const SYSTEM_PROMPT: &str = "You are an entity extraction component. \
Read the provided text and extract the named entities it mentions. \
Respond with a single bare JSON object and nothing else: no prose, no explanation, and no Markdown code fences. \
The object must have exactly this shape: \
{\"entities\":[{\"name\":\"<entity name>\",\"entityType\":\"<entity type>\",\"confidence\":<number 0..1, optional>}]}. \
Each entity's name and entityType must be non-empty strings. \
Include the confidence field only when you can report a calibrated value between 0 and 1 inclusive. \
If the text mentions no entities, return {\"entities\":[]}.";

/// Strict deserialization target for the entity response.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EntityResponse {
    entities: Vec<EntityItem>,
}

/// One extracted entity as returned by the model.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EntityItem {
    name: String,
    #[serde(rename = "entityType")]
    entity_type: String,
    #[serde(default)]
    confidence: Option<f64>,
}

/// Parse the entity producer's raw output into one `ProducedAnnotation` per
/// entity. Empty `entities` yields an empty vec (a valid "no entities" result).
/// The per-entity body is `{"name","entityType"}` in camelCase.
pub(crate) fn parse_output(raw: &str) -> Result<Vec<ProducedAnnotation>, ApiError> {
    let json_text = strip_optional_code_fence(raw);
    let response: EntityResponse = strict_from_str(json_text, "entity", raw)?;

    let mut produced = Vec::with_capacity(response.entities.len());
    for entity in response.entities {
        validate_non_empty(&entity.name, "entity.name", raw)?;
        validate_non_empty(&entity.entity_type, "entity.entityType", raw)?;
        validate_confidence(entity.confidence, "entity.confidence", raw)?;
        produced.push(ProducedAnnotation {
            body: json!({ "name": entity.name, "entityType": entity.entity_type }),
            confidence: entity.confidence,
        });
    }
    Ok(produced)
}
