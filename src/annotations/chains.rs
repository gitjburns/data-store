//! Single-goal annotation chains over one excerpt's canonical text. The section
//! path rides along as separate prompt context (`Section: a / b / c`) and never
//! enters the passage the stages match against. Intermediate work remains
//! local to one invocation; only a fully completed chain reaches persistence.

use std::{borrow::Cow, collections::BTreeMap};

use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::annotations::{
    entity,
    llm_client::{AnnotatorClient, CompletionFailure},
    producer::{
        InvocationFailure, ProducedAnnotation, ProducerKind, source_text_matches, strict_from_str,
        validate_non_empty,
    },
    relation,
    stages::Stage,
    summary,
};
use crate::error::ApiError;
use crate::monitoring_types::{MonitorState, TokenUsage};
use crate::types::AnnotationProgressCount;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NamesResponse {
    names: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StatementsResponse {
    sentences: Vec<String>,
}

#[derive(Serialize)]
struct EntityTypesInput<'input> {
    passage: &'input str,
    names: &'input [String],
}

#[derive(Serialize)]
struct RelationsInput<'input> {
    passage: &'input str,
    selected_sentence: &'input str,
}

#[derive(Serialize)]
struct IndexedRelationship<'input> {
    relationship_index: usize,
    subject: &'input str,
    predicate: &'input str,
    object: &'input str,
}

#[derive(Serialize)]
struct EvidenceInput<'input> {
    passage: &'input str,
    relationships: &'input [IndexedRelationship<'input>],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceResponse {
    evidence: Vec<EvidenceItem>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceItem {
    relationship_index: usize,
    quotes: Vec<String>,
}

/// Run dependencies sequentially inside the worker's existing bounded invocation
/// concurrency. A later failure discards all local results, never partial writes.
/// Every stage retains the wave's committed coverage snapshot until persistence.
/// `section` is the excerpt's section path already joined for display; it is
/// prepended to every stage message as context and is never source text.
pub(crate) fn run(
    kind: ProducerKind,
    client: &AnnotatorClient,
    passage: &str,
    section: Option<&str>,
    temperature: f64,
    progress: Option<AnnotationProgressCount>,
) -> Result<Vec<ProducedAnnotation>, InvocationFailure> {
    match kind {
        ProducerKind::Entity => entities(client, passage, section, temperature, progress),
        ProducerKind::Relation => relations(client, passage, section, temperature, progress),
        ProducerKind::Summary => run_stage(
            Stage::Summary,
            client,
            passage,
            section,
            temperature,
            progress,
            summary::parse_output,
            Vec::len,
        ),
    }
}

/// Find candidates, then classify or explicitly reject them. Both decision lists
/// must account for the exact input multiset before any entity is produced.
fn entities(
    client: &AnnotatorClient,
    passage: &str,
    section: Option<&str>,
    temperature: f64,
    progress: Option<AnnotationProgressCount>,
) -> Result<Vec<ProducedAnnotation>, InvocationFailure> {
    let names = run_stage(
        Stage::EntityNames,
        client,
        passage,
        section,
        temperature,
        progress,
        |raw| {
            let response: NamesResponse = strict_from_str(raw, Stage::EntityNames.name(), raw)?;
            for name in &response.names {
                validate_non_empty(name, "names[]", raw)?;
            }
            Ok(response.names)
        },
        Vec::len,
    )?;
    let mut annotations = Vec::new();
    for batch in bounded_batches(&names, batch_limit(passage), Stage::EntityTypes)? {
        let input = serialize_input(&EntityTypesInput {
            passage,
            names: batch,
        })?;
        let mut typed = run_stage(
            Stage::EntityTypes,
            client,
            &input,
            section,
            temperature,
            progress,
            |raw| {
                let typed = entity::parse_output(raw)?;
                let mut remaining = BTreeMap::<&str, usize>::new();
                for name in batch {
                    *remaining.entry(name.as_str()).or_default() += 1;
                }
                // Rejections satisfy candidate accounting but never become
                // annotations. Duplicated input names still require one decision
                // per occurrence, across both lists rather than independently.
                let decided_names = typed
                    .entities
                    .iter()
                    .map(|entity| entity.name.as_str())
                    .chain(typed.rejected.iter().map(|rejected| rejected.name.as_str()));
                for name in decided_names {
                    let count = remaining.get_mut(name).ok_or_else(|| {
                        output_error(
                            Stage::EntityTypes,
                            "returned a name absent from its input batch",
                        )
                    })?;
                    if *count == 0 {
                        return Err(output_error(
                            Stage::EntityTypes,
                            "returned an extra duplicate name",
                        ));
                    }
                    *count -= 1;
                }
                if remaining.values().any(|count| *count != 0) {
                    return Err(output_error(Stage::EntityTypes, "omitted a supplied name"));
                }
                info!(
                    event = "annotation_stage.entity_decisions",
                    candidates = batch.len(),
                    accepted = typed.entities.len(),
                    rejected = typed.rejected.len(),
                    "entity candidate accounting passed; rejected candidates produce no annotations"
                );
                Ok(typed.into_annotations())
            },
            Vec::len,
        )?;
        annotations.append(&mut typed);
    }
    Ok(annotations)
}

/// Select statements before forming triples, then attach receipts separately.
/// Statements and quotations share forgiving source matching that tolerates
/// text repairs and omissions. This does not establish semantic correctness.
fn relations(
    client: &AnnotatorClient,
    passage: &str,
    section: Option<&str>,
    temperature: f64,
    progress: Option<AnnotationProgressCount>,
) -> Result<Vec<ProducedAnnotation>, InvocationFailure> {
    let statements = run_stage(
        Stage::Statements,
        client,
        passage,
        section,
        temperature,
        progress,
        |raw| {
            let response: StatementsResponse = strict_from_str(raw, Stage::Statements.name(), raw)?;
            // Validate source support without undoing the model's text repairs;
            // downstream requests retain both its statement and the original source.
            for statement in &response.sentences {
                validate_non_empty(statement, "sentences[]", raw)?;
                if !source_text_matches(passage, statement) {
                    return Err(output_error(
                        Stage::Statements,
                        "selected statement does not match the source within the fuzzy repair and omission limits",
                    ));
                }
            }
            Ok(response.sentences)
        },
        Vec::len,
    )?;
    let mut annotations = Vec::new();
    for statement in statements {
        let input = serialize_input(&RelationsInput {
            passage,
            selected_sentence: &statement,
        })?;
        let mut formed = run_stage(
            Stage::Relations,
            client,
            &input,
            section,
            temperature,
            progress,
            relation::parse_output,
            Vec::len,
        )?;
        attach_evidence(client, passage, section, temperature, &mut formed, progress)?;
        annotations.append(&mut formed);
    }
    Ok(annotations)
}

/// Keep candidate payloads bounded independently of source text. Receipt indexes
/// refer to the explicit supplied indexes, including when a batch starts above zero.
fn attach_evidence(
    client: &AnnotatorClient,
    passage: &str,
    section: Option<&str>,
    temperature: f64,
    annotations: &mut [ProducedAnnotation],
    progress: Option<AnnotationProgressCount>,
) -> Result<(), InvocationFailure> {
    let relationships = annotations
        .iter()
        .enumerate()
        .map(|(index, annotation)| {
            Ok(IndexedRelationship {
                relationship_index: index,
                subject: annotation_text(annotation, "subject")?,
                predicate: annotation_text(annotation, "predicate")?,
                object: annotation_text(annotation, "object")?,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()
        .map_err(InvocationFailure::Internal)?;
    let mut receipts = BTreeMap::new();
    for batch in bounded_batches(&relationships, batch_limit(passage), Stage::Evidence)? {
        let input = serialize_input(&EvidenceInput {
            passage,
            relationships: batch,
        })?;
        let evidence = run_stage(
            Stage::Evidence,
            client,
            &input,
            section,
            temperature,
            progress,
            |raw| {
                let response: EvidenceResponse = strict_from_str(raw, Stage::Evidence.name(), raw)?;
                let mut expected = batch
                    .iter()
                    .map(|item| (item.relationship_index, false))
                    .collect::<BTreeMap<_, _>>();
                for item in &response.evidence {
                    let seen = expected.get_mut(&item.relationship_index).ok_or_else(|| {
                        output_error(
                            Stage::Evidence,
                            "returned a relationship index outside its input batch",
                        )
                    })?;
                    if *seen {
                        return Err(output_error(
                            Stage::Evidence,
                            "returned a duplicate relationship index",
                        ));
                    }
                    *seen = true;
                    for quote in &item.quotes {
                        validate_non_empty(quote, "evidence[].quotes[]", raw)?;
                        // Use the same comparison as statement selection so model
                        // text repairs and bounded omissions retain their quotations.
                        if !source_text_matches(passage, quote) {
                            return Err(output_error(
                                Stage::Evidence,
                                "quotation does not match the source within the fuzzy repair and omission limits",
                            ));
                        }
                    }
                }
                if expected.values().any(|seen| !seen) {
                    return Err(output_error(
                        Stage::Evidence,
                        "omitted a supplied relationship index",
                    ));
                }
                Ok(response.evidence)
            },
            Vec::len,
        )?;
        receipts.extend(
            evidence
                .into_iter()
                .map(|item| (item.relationship_index, item.quotes)),
        );
    }
    // All receipt batches have passed structural checks before any local body is
    // changed. Empty quotes remain explicit candidate data for future verification.
    for (index, annotation) in annotations.iter_mut().enumerate() {
        let quotes = receipts.remove(&index).ok_or_else(|| {
            InvocationFailure::Internal(output_error(
                Stage::Evidence,
                "validated receipt mapping is incomplete",
            ))
        })?;
        let body = annotation.body.as_object_mut().ok_or_else(|| {
            InvocationFailure::Internal(output_error(
                Stage::Evidence,
                "relation parser returned a non-object annotation body",
            ))
        })?;
        body.insert(
            "evidenceQuotes".to_string(),
            serde_json::Value::Array(quotes.into_iter().map(serde_json::Value::String).collect()),
        );
        annotation.confidence = None;
    }
    Ok(())
}

/// Own each request/validation boundary with its single stage identity. The
/// maintenance signal wins before submission and before consuming a response.
/// When `section` is present the user message is the `Section:` line, one
/// blank line, then `input`; the passage and every validator still see `input`
/// alone, so the section line can never be quoted or matched as source text.
// The section context joins the stage's existing explicit inputs; a wrapper
// struct would only hide which of them the model message is built from.
#[allow(clippy::too_many_arguments)]
fn run_stage<T>(
    stage: Stage,
    client: &AnnotatorClient,
    input: &str,
    section: Option<&str>,
    temperature: f64,
    progress: Option<AnnotationProgressCount>,
    parse: impl FnOnce(&str) -> Result<T, ApiError>,
    item_count: impl FnOnce(&T) -> usize,
) -> Result<T, InvocationFailure> {
    let message: Cow<'_, str> = match section {
        Some(section) => Cow::Owned(format!("Section: {section}\n\n{input}")),
        None => Cow::Borrowed(input),
    };
    let (mut call, context) = client.start_call(stage.name(), progress);
    // The synchronous stage owns this context through HTTP and validation. The
    // HTTP future is polled on this same producer thread, never on a Tokio worker.
    // Context and payload buffer have distinct owners: HTTP can fill the buffer
    // while the stage retains its tracing scope through validation and one flush.
    let _entered = context.enter();
    let mut monitor_call = None;
    let mut usage = TokenUsage::default();
    let result = (|| {
        check_cancellation(client)?;
        let schema = stage.schema().map_err(InvocationFailure::Internal)?;
        let prompt = stage.prompt();
        // Submission starts after local preparation; one terminal observation
        // includes structural validation without claiming annotation persistence.
        monitor_call = client.monitor_call(stage.name());
        let (completion, received_usage) =
            client.complete(&mut call, &context, &prompt, &message, &schema, temperature);
        // Usage belongs to the received exchange even when HTTP or subsequent
        // validation fails; preserve it before propagating either result.
        usage = received_usage;
        let raw = completion.map_err(|failure| match failure {
            CompletionFailure::Cancelled(reason) => InvocationFailure::Cancelled(reason),
            CompletionFailure::Request(error) => InvocationFailure::Call(error),
        })?;
        check_cancellation(client)?;
        parse(&raw).map_err(InvocationFailure::InvalidOutput)
    })();
    if let Some(monitor_call) = monitor_call {
        match &result {
            Ok(_) => monitor_call.finish(MonitorState::Complete, usage, None),
            Err(InvocationFailure::Cancelled(reason)) => {
                monitor_call.finish(MonitorState::Cancelled, usage, Some(reason.label()));
            }
            Err(failure) => {
                monitor_call.finish(MonitorState::Failed, usage, Some(&failure.to_string()));
            }
        }
    }
    match &result {
        Ok(value) => {
            let output_items = item_count(value);
            call.result("SUCCESS", &format!(
                "Structural validation passed; {output_items} output items.\nSemantic verification was not performed.\nDatabase persistence is a separate annotation outcome."
            ));
            info!(
                event = "annotation_stage.validated",
                stage = stage.name(),
                output_items,
                verification = "not_performed",
                "annotation stage passed structural validation",
            );
        }
        Err(InvocationFailure::InvalidOutput(failure)) => {
            call.result("FAILURE — structural validation", &failure.to_string());
            error!(
            event = "annotation_stage.validation_failed", stage = stage.name(),
            error = %failure, "annotation stage failed structural validation",
            );
        }
        Err(InvocationFailure::Cancelled(reason)) => {
            call.result(
                "CANCELLED",
                &format!(
                    "Reason: {}\nRemote inference outcome is unknown.",
                    reason.label()
                ),
            );
            info!(
                event = "annotation_stage.cancelled",
                stage = stage.name(),
                reason = reason.label(),
                "annotation stage cancelled"
            );
        }
        Err(failure) => {
            call.result(
                "FAILURE",
                &format!("Class: {}\nReason: {failure}", failure.class()),
            );
            error!(event = "annotation_stage.failed", stage = stage.name(),
                error = %failure, "annotation stage failed before structural validation");
        }
    }
    result
}

/// Cancellation is an operator outcome, never an output retry or a local fault.
fn check_cancellation(client: &AnnotatorClient) -> Result<(), InvocationFailure> {
    match client.cancellation().reason() {
        Some(reason) => Err(InvocationFailure::Cancelled(reason)),
        None => Ok(()),
    }
}

/// Preserve typed application payloads until the existing text-message boundary.
fn serialize_input<T: Serialize + ?Sized>(input: &T) -> Result<String, InvocationFailure> {
    serde_json::to_string(input).map_err(|error| {
        InvocationFailure::Internal(ApiError::AnnotationProducer {
            message: format!("annotation stage input serialization failed: {error}"),
        })
    })
}

/// The intermediate batch bound is the passage's own character count
/// (PLAN-grains Section 4): candidate payloads never outgrow the source text
/// they were drawn from, whatever the configured grain sizes.
fn batch_limit(passage: &str) -> usize {
    passage.chars().count()
}

/// Bound serialized candidate arrays, including escaping and separators, to the
/// passage's character count. Oversized individual items fail explicitly;
/// truncating a model-produced name or relationship would change its meaning.
fn bounded_batches<T: Serialize>(
    items: &[T],
    limit: usize,
    stage: Stage,
) -> Result<Vec<&[T]>, InvocationFailure> {
    let mut batches = Vec::new();
    let mut start = 0;
    let mut used = 2;
    for (index, item) in items.iter().enumerate() {
        let size = serialize_input(item)?.chars().count();
        if size.saturating_add(2) > limit {
            return Err(InvocationFailure::InvalidOutput(output_error(
                stage,
                &format!(
                    "one intermediate item needs {} serialized characters; batch limit is {limit}",
                    size.saturating_add(2),
                ),
            )));
        }
        let separator = usize::from(index > start);
        if used + separator + size > limit {
            batches.push(&items[start..index]);
            start = index;
            used = 2;
        }
        used += usize::from(index > start) + size;
    }
    if start < items.len() {
        batches.push(&items[start..]);
    }
    Ok(batches)
}

/// Read fields guaranteed by the existing annotation parser without panic paths.
fn annotation_text<'body>(
    annotation: &'body ProducedAnnotation,
    field: &str,
) -> Result<&'body str, ApiError> {
    annotation
        .body
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ApiError::AnnotationProducer {
            message: format!("annotation parser omitted required body field {field}"),
        })
}

/// Keep structural rejection errors attributable without emitting model payloads.
fn output_error(stage: Stage, detail: &str) -> ApiError {
    ApiError::AnnotationProducer {
        message: format!("annotation stage {}: {detail}", stage.name()),
    }
}
