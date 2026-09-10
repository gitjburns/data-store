//! Shape validation for relation annotations from bounded, single-goal chains.
//! Stage prompts and response schemas are defined together in `stages`.

use serde::Deserialize;
use serde_json::json;

use crate::annotations::producer::{
    ProducedAnnotation, strict_from_str, strip_optional_code_fence, validate_non_empty,
};
use crate::error::ApiError;

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
        produced.push(ProducedAnnotation {
            body: json!({
                "subject": relation.subject,
                "predicate": relation.predicate,
                "object": relation.object,
            }),
            // These stages do not request a calibrated confidence estimate.
            confidence: None,
        });
    }
    Ok(produced)
}
