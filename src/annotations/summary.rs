//! CAb: summary producer — per-document summarization over the ordered
//! composite of evidence-bearing units (spec §21).
//!
//! Prompt and strict output parsing for the summary producer. The system
//! prompt is a named external-language constant (PRINCIPLES); the model must
//! return a bare JSON object of the exact shape parsed below, no prose and no
//! code fences. Parsing is strict: `deny_unknown_fields` and a non-empty
//! `summary` string. Unlike entity/relation, the summary result is exactly one
//! annotation — there is no "empty" valid result: a document composite always
//! summarizes to one text. A malformed or empty-summary response is
//! `ApiError::AnnotationProducer` with bounded diagnostics only.

use serde::Deserialize;
use serde_json::json;

use crate::annotations::producer::{
    ProducedAnnotation, strict_from_str, strip_optional_code_fence, validate_non_empty,
};
use crate::error::ApiError;

/// System prompt for the summary producer. Fixes the task and the exact
/// bare-JSON response contract the parser below enforces.
pub(crate) const SYSTEM_PROMPT: &str = "You are a summarization component. \
Read the provided text and write one concise summary of it. \
Respond with a single bare JSON object and nothing else: no prose, no explanation, and no Markdown code fences. \
The object must have exactly this shape: {\"summary\":\"<summary text>\"}. \
The summary must be a single non-empty string that captures the text's main points.";

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
