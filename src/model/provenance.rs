//! Provenance model (spec §20): who produced a derived artifact, with what
//! configuration and inputs, and — for annotation memoization (§21.1–§21.3,
//! deferred implementation) — whether the producer actually ran.

// Consumed from C4 onward; remove when C4 wires it.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

/// Spec §20. Producer identity, configuration, and input lineage for a
/// derived artifact. The memoization fields are normative hooks reserved by
/// §21.1 and inert in this revision: when populated, `memoized: true` plus
/// `memoizedFrom` state honestly that the producer was not re-invoked.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Provenance {
    pub(crate) producer_type: ProducerType,
    pub(crate) producer_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) producer_version: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) config_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) model_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) model_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) prompt_hash: Option<String>,

    /// Sampling temperature actually sent on the model call that minted this
    /// artifact (user-ruled 2026-07-21: retry-escalated annotator calls must
    /// be distinguishable in the audit record, since temperature is not
    /// identity-bearing). None for non-sampling producers (parsers, rules,
    /// embeddings), memo reuses (no call ran), and rows minted before
    /// temperature recording existed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) temperature: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) confidence: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) memoized: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) memoized_from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) memoization_key_hash: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) input_refs: Option<Vec<ProvenanceInputRef>>,
}

/// Spec §20 `producerType`. What class of actor produced the artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProducerType {
    Parser,
    Rule,
    Model,
    Human,
    System,
}

/// Spec §20. One typed reference to an input object, preserving lineage from
/// derived outputs back to their inputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ProvenanceInputRef {
    pub(crate) object_type: ProvenanceObjectType,
    pub(crate) id: String,
}

/// Spec §20 `ProvenanceInputRef.objectType`. Closed set of referenceable
/// fabric object types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProvenanceObjectType {
    SourceObject,
    AcquisitionRecord,
    ParseRun,
    ContentUnit,
    UnitRelationship,
    SemanticAnnotation,
    RetrievalProjection,
    AssemblyPolicy,
}
