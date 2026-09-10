//! Single-goal annotation contracts shared by live requests and producer identity.

use crate::error::ApiError;

const ENTITY_NAMES_SCHEMA: &str = r#"{
  "type":"object","properties":{"names":{"type":"array","items":{"type":"string","minLength":1}}},
  "required":["names"],"additionalProperties":false
}"#;
const ENTITY_TYPES_SCHEMA: &str = r#"{
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

/// Each stage has one semantic goal; schemas constrain representation only.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Stage {
    EntityNames,
    EntityTypes,
    Statements,
    Relations,
    Evidence,
    Summary,
}

impl Stage {
    /// Stable stage labels correlate HTTP activity and intermediate validation.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::EntityNames => "entity_names",
            Self::EntityTypes => "entity_types",
            Self::Statements => "statements",
            Self::Relations => "relations",
            Self::Evidence => "evidence",
            Self::Summary => "summary",
        }
    }

    /// Keep the tested instructions identical in hashing and live generation;
    /// naming policy and verification instructions are not appended to a stage.
    pub(crate) fn prompt(self) -> &'static str {
        match self {
            Self::EntityNames => {
                "List the named entities mentioned in the passage, using the names as written."
            }
            Self::EntityTypes => {
                "Assign an entity type to each supplied name in the context of the passage."
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
        }
    }

    /// Decode owned contract data explicitly; a broken schema is a local error,
    /// never a rejected model output or reason to retry inference.
    pub(crate) fn schema(self) -> Result<serde_json::Value, ApiError> {
        let source = match self {
            Self::EntityNames => ENTITY_NAMES_SCHEMA,
            Self::EntityTypes => ENTITY_TYPES_SCHEMA,
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
