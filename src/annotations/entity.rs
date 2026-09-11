//! Shape validation for entity annotations from bounded, single-goal chains.
//! Stage prompts and response schemas are defined together in `stages`.

use serde::Deserialize;
use serde_json::json;

use crate::annotations::producer::{
    ProducedAnnotation, strict_from_str, strip_optional_code_fence, validate_non_empty,
};
use crate::error::ApiError;

/// Typed decisions remain separate until the chain verifies that accepted and
/// rejected candidates together account for its entire input batch.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EntityResponse {
    pub(crate) entities: Vec<EntityItem>,
    pub(crate) rejected: Vec<RejectedEntity>,
}

/// One extracted entity as returned by the model.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EntityItem {
    pub(crate) name: String,
    #[serde(rename = "entityType")]
    entity_type: String,
}

/// An explicit negative decision is diagnostic evidence, never an entity row.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RejectedEntity {
    pub(crate) name: String,
    reason: String,
}

/// Validate the decision shape and required text before the chain checks names.
/// Both arrays are required; either may be empty, including all-rejected output.
pub(crate) fn parse_output(raw: &str) -> Result<EntityResponse, ApiError> {
    let json_text = strip_optional_code_fence(raw);
    let response: EntityResponse = strict_from_str(json_text, "entity", raw)?;

    for entity in &response.entities {
        validate_non_empty(&entity.name, "entity.name", raw)?;
        validate_non_empty(&entity.entity_type, "entity.entityType", raw)?;
        // The previously observed rejection sentinel cannot become graph metadata.
        if entity
            .entity_type
            .trim()
            .eq_ignore_ascii_case("NOT_AN_ENTITY")
        {
            return Err(ApiError::AnnotationProducer {
                message: "entity typing used a rejection label as an accepted entityType; use rejected instead".to_string(),
            });
        }
    }
    for rejected in &response.rejected {
        validate_non_empty(&rejected.name, "rejected.name", raw)?;
        validate_non_empty(&rejected.reason, "rejected.reason", raw)?;
    }
    Ok(response)
}

impl EntityResponse {
    /// Convert accepted decisions only, after complete candidate accounting. An
    /// empty vector uses the worker's existing fresh empty-coverage marker.
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
