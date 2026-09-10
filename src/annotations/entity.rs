//! Shape validation for entity annotations from bounded, single-goal chains.
//! Stage prompts and response schemas are defined together in `stages`.

use serde::Deserialize;
use serde_json::json;

use crate::annotations::producer::{
    ProducedAnnotation, strict_from_str, strip_optional_code_fence, validate_non_empty,
};
use crate::error::ApiError;

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
        produced.push(ProducedAnnotation {
            body: json!({ "name": entity.name, "entityType": entity.entity_type }),
            // These stages do not request a calibrated confidence estimate.
            confidence: None,
        });
    }
    Ok(produced)
}
