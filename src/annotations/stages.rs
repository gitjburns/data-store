//! Single-goal annotation contracts shared by live requests and producer identity.

use crate::error::ApiError;

// One entity stage: names and types come straight from the passage, and the
// chain grounds each returned name in the passage.
const ENTITIES_SCHEMA: &str = r#"{
  "type":"object","properties":{"entities":{"type":"array","items":{
    "type":"object","properties":{"name":{"type":"string","minLength":1},"entityType":{"type":"string","minLength":1}},
    "required":["name","entityType"],"additionalProperties":false
  }}},"required":["entities"],"additionalProperties":false
}"#;
const STATEMENTS_SCHEMA: &str = r#"{
  "type":"object","properties":{"sentences":{"type":"array","items":{"type":"string","minLength":1}}},
  "required":["sentences"],"additionalProperties":false
}"#;
const RELATIONS_SCHEMA: &str = r#"{
  "type":"object","properties":{"relations":{"type":"array","items":{
    "type":"object","properties":{"subject":{"type":"string","minLength":1},"predicate":{"type":"string","minLength":1},"object":{"type":"string","minLength":1}},
    "required":["subject","predicate","object"],"additionalProperties":false
  }}},"required":["relations"],"additionalProperties":false
}"#;
const EVIDENCE_SCHEMA: &str = r#"{
  "type":"object","properties":{"evidence":{"type":"array","items":{
    "type":"object","properties":{"relationship_index":{"type":"integer","minimum":0},"quotes":{"type":"array","items":{"type":"string","minLength":1}}},
    "required":["relationship_index","quotes"],"additionalProperties":false
  }}},"required":["evidence"],"additionalProperties":false
}"#;
const SUMMARY_SCHEMA: &str = r#"{
  "type":"object","properties":{"summary":{"type":"string","minLength":1}},
  "required":["summary"],"additionalProperties":false
}"#;

// Prose makes each existing schema's final-answer shape explicit to the model;
// these instructions do not add generation goals or alter schema validation.
const ENTITIES_FORMAT: &str = r#"Return a JSON object with exactly one field, "entities", containing an array of objects. Each object must have exactly "name" and "entityType", both nonempty strings. If no named entities are present, return {"entities":[]}."#;
const STATEMENTS_FORMAT: &str = r#"Return a JSON object with exactly one field, "sentences", containing an array of nonempty strings. If no sentence qualifies, return {"sentences":[]}."#;
const RELATIONS_FORMAT: &str = r#"Return a JSON object with exactly one field, "relations", containing an array of objects. Each object must have exactly "subject", "predicate", and "object", all nonempty strings. If there are no relationships, return {"relations":[]}."#;
const EVIDENCE_FORMAT: &str = r#"Return a JSON object with exactly one field, "evidence", containing an array of objects. Each object must have exactly "relationship_index", a nonnegative integer identifying the supplied relationship, and "quotes", an array of nonempty strings. Include an entry for every supplied relationship index; use an empty quotes array when no supporting quotation is found."#;
const SUMMARY_FORMAT: &str =
    r#"Return a JSON object with exactly one field, "summary", containing a nonempty string."#;
const FINAL_ANSWER_FORMAT: &str = "Your final answer must contain only the specified JSON object, without Markdown fences or commentary.";
// Every stage message may open with the excerpt's section path. The line is
// context for reading the passage and is never source text: nothing may be
// extracted, quoted, or summarized from it. Composed into every prompt so a
// change here is a visible prompt-hash change.
const SECTION_CONTEXT_NOTE: &str = "The input may begin with a line starting \"Section:\" followed by a blank line. That line names where the passage sits in the document and is context only. Only the passage is source text: never extract names, sentences, quotations, or summary content from the Section line.";

/// Each stage has one semantic goal; schemas constrain representation only.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Stage {
    Entities,
    Statements,
    Relations,
    Evidence,
    Summary,
}

impl Stage {
    /// Stable stage labels correlate HTTP activity and intermediate validation.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Entities => "entities",
            Self::Statements => "statements",
            Self::Relations => "relations",
            Self::Evidence => "evidence",
            Self::Summary => "summary",
        }
    }

    /// Compose task, section-context, and final-answer instructions identically
    /// for live requests and producer hashing, so cached output cannot cross a
    /// prompt change.
    pub(crate) fn prompt(self) -> String {
        let task = match self {
            Self::Entities => {
                "List the named entities mentioned in the passage with the type of each, using the names as written in the passage. Return an empty entities array if no named entities are present."
            }
            Self::Statements => {
                "Copy only declarative sentences that explicitly assert a relationship between entities in the passage. Exclude questions. Return an empty list if no sentence qualifies."
            }
            Self::Relations => {
                "Express the selected sentence's relationships as subject-predicate-object triples, preserving its meaning in the passage."
            }
            Self::Evidence => {
                "Copy verbatim source quotations that support each supplied relationship. Return an empty quotes array where no supporting quotation can be found."
            }
            Self::Summary => "Write a concise summary of the passage.",
        };
        let output_format = match self {
            Self::Entities => ENTITIES_FORMAT,
            Self::Statements => STATEMENTS_FORMAT,
            Self::Relations => RELATIONS_FORMAT,
            Self::Evidence => EVIDENCE_FORMAT,
            Self::Summary => SUMMARY_FORMAT,
        };
        format!("{task}\n\n{SECTION_CONTEXT_NOTE}\n\n{output_format}\n\n{FINAL_ANSWER_FORMAT}")
    }

    /// Decode owned contract data explicitly; a broken schema is a local error,
    /// never a rejected model output or reason to retry inference.
    pub(crate) fn schema(self) -> Result<serde_json::Value, ApiError> {
        let source = match self {
            Self::Entities => ENTITIES_SCHEMA,
            Self::Statements => STATEMENTS_SCHEMA,
            Self::Relations => RELATIONS_SCHEMA,
            Self::Evidence => EVIDENCE_SCHEMA,
            Self::Summary => SUMMARY_SCHEMA,
        };
        serde_json::from_str(source).map_err(|error| ApiError::AnnotationProducer {
            message: format!(
                "annotation stage {} has invalid output schema: {error}",
                self.name()
            ),
        })
    }
}
