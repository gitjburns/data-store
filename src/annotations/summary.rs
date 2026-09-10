//! Shape validation for summary annotations from bounded, single-goal chains.
//! Stage prompts and response schemas are defined together in `stages`.

use serde::Deserialize;
use serde_json::json;

use crate::annotations::producer::{
    ProducedAnnotation, strict_from_str, strip_optional_code_fence, validate_non_empty,
};
use crate::error::ApiError;

/// Strict deserialization target for the summary response.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SummaryResponse {
    summary: String,
}

/// Parse the summary producer's raw output into exactly one
/// `ProducedAnnotation` whose body is `{"text"}` in camelCase. The summary
/// producer reports no confidence, so `confidence` is always `None`.
pub(crate) fn parse_output(raw: &str) -> Result<Vec<ProducedAnnotation>, ApiError> {
    let json_text = strip_optional_code_fence(raw);
    let response: SummaryResponse = strict_from_str(json_text, "summary", raw)?;
    validate_non_empty(&response.summary, "summary.summary", raw)?;

    Ok(vec![ProducedAnnotation {
        body: json!({ "text": response.summary }),
        confidence: None,
    }])
}
