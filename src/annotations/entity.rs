//! Shape validation for entity annotations from bounded, single-goal chains.
//! Stage prompts and response schemas are defined together in `stages`.

use serde::Deserialize;
use serde_json::json;

use crate::annotations::producer::{
    ProducedAnnotation, strict_from_str, strip_optional_code_fence, validate_non_empty,
};
use crate::error::ApiError;

/// The entity stage's whole answer: names with types read from the passage.
/// Shape-valid items still await the chain's grounding check before becoming rows.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EntityResponse {
    pub(crate) entities: Vec<EntityItem>,
}

/// One extracted entity as returned by the model.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EntityItem {
    pub(crate) name: String,
    #[serde(rename = "entityType")]
    pub(crate) entity_type: String,
}

/// Validate the response shape and required text before the chain grounds names.
/// The array is required and may be empty.
pub(crate) fn parse_output(raw: &str) -> Result<EntityResponse, ApiError> {
    let json_text = strip_optional_code_fence(raw);
    let response: EntityResponse = strict_from_str(json_text, "entity", raw)?;

    for entity in &response.entities {
        validate_non_empty(&entity.name, "entity.name", raw)?;
        validate_non_empty(&entity.entity_type, "entity.entityType", raw)?;
        // A previously observed refusal sentinel cannot become graph metadata.
        if entity
            .entity_type
            .trim()
            .eq_ignore_ascii_case("NOT_AN_ENTITY")
        {
            return Err(ApiError::AnnotationProducer {
                message: "entity stage returned NOT_AN_ENTITY as an entityType; omit non-entities instead".to_string(),
            });
        }
    }
    Ok(response)
}

impl EntityResponse {
    /// Convert the grounded, collapsed items the chain kept. An empty vector
    /// uses the worker's existing fresh empty-coverage marker.
    pub(crate) fn into_annotations(self) -> Vec<ProducedAnnotation> {
        self.entities
            .into_iter()
            .map(|entity| ProducedAnnotation {
                body: json!({ "name": entity.name, "entityType": entity.entity_type }),
                // These stages do not request a calibrated confidence estimate.
                confidence: None,
            })
            .collect()
    }
}
