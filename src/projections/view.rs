//! C6d derived-view builder: `derived_view` projections and the `summary`
//! projection that materializes CA summary annotations (per D8's selected
//! types) rather than choosing its own producer (spec §22, §36).
//!
//! Two builders live here, both PURE functions invoked by integration wiring
//! (design fact 8): they take the caller's `&Transaction`, the identifiers,
//! and the shared handles (`ArtifactStore`, and for the summary a read
//! `&Connection`); they touch NO scheduler/worker/main and open NO transaction
//! of their own.
//!
//! Canonical-boundary invariant (design fact 5, spec §22 rule 3): a
//! `derived_view` renders EXCLUSIVELY from canonical `content_units` — never
//! from Docling output or any parser-native artifact — and its Markdown-ish
//! payload maps back to those canonical records; it is never itself canonical
//! parsed state. The summary projection likewise reads only the durable CA
//! `summary` annotations (already canonical-boundary-clean by construction),
//! materializing them into a `Summary` projection; it invents no content.
//!
//! No-canonical-write invariant (design fact 6): neither builder writes the
//! parse bundle or bumps any schema version. The derived_view payload is
//! archived to the content-addressed artifact store; the projection envelope
//! records only its `payload_uri`. The summary projection's content already
//! lives durably on its source annotations, so the projection is pure
//! linkage — see `build_summary` for why its payload is NOT re-archived.
//!
//! Envelope single-source invariant (design fact 7): every
//! `retrieval_projections` row change goes through `super::envelope`; this
//! module hand-writes NO projection SQL. It DOES read `content_units`
//! directly (that table is not envelope-owned; the CA producer reads it the
//! same way via its own ordered SELECT), because rendering needs the ordered
//! unit tree and no shared reader exposes it to this module.

use std::time::Instant;

use rusqlite::{Connection, Transaction, params};
use serde_json::Value;
use tracing::{debug, error, info};

use super::envelope::{self, NewProjection, ProjectionType};
use crate::error::ApiError;
use crate::model::{
    ContentType, ProducerType, Provenance, SemanticAnnotation, SemanticAnnotationType,
};

/// Stable producer name recorded in the `derived_view` projection's Provenance
/// (§20). The renderer is a deterministic in-process transform, not a model,
/// so its provenance is a `System` producer with a version that bumps when the
/// rendering rule changes in a way that alters the archived bytes.
const DERIVED_VIEW_PRODUCER_NAME: &str = "fabric_derived_view";

/// Stable producer name recorded in the `summary` projection's Provenance
/// (§20). The summary projection runs no producer of its own — it MATERIALIZES
/// CA summary annotations (D7 dissolved into D8) — so its provenance is a
/// `System` materializer, distinct from the `annotator_summary` model producer
/// that actually authored the summary text.
const SUMMARY_PRODUCER_NAME: &str = "fabric_summary_materializer";

/// Producer version for both projections; bumped when the rendering rule (view)
/// or the materialization shape (summary) changes in a way that must invalidate
/// prior payloads.
const PRODUCER_VERSION: &str = "1";

/// Ordered SELECT of a parse's content units in reading order, mirroring the CA
/// producer's `SELECT_PARSE_UNITS_SQL` ordering discipline exactly: NULL
/// `sequence_index` sorts last, then by `sequence_index`, with an `id`
/// tiebreak so the reading order is total and deterministic across runs. The
/// rendered document's block order IS this order, so any ordering drift here
/// would silently change the archived bytes. `content_units` rows are
/// hard-deleted by hot cleanup (§31.2), never soft-deleted, so there is no
/// `deleted_at` filter (the table has no such column).
const SELECT_PARSE_UNITS_SQL: &str = "
SELECT id, content_type, primary_parent_id, sequence_index, body_json
FROM content_units
WHERE parse_id = ?1
ORDER BY sequence_index IS NULL, sequence_index, id";

/// One canonical unit read for rendering: the subset of `content_units` columns
/// the renderer needs. `content_type` drives which body field renders and at
/// what structural level; `id` is recorded in the projection's
/// `input_unit_ids` so the archived view maps back to its canonical inputs
/// (spec §22 rule 3).
struct RenderUnit {
    id: String,
    content_type: ContentType,
    body: Value,
}

/// Build the `derived_view` projection for a parse: render the parse's ordered
/// canonical units into a Markdown-ish document, archive it to the artifact
/// store, and open+complete a `DerivedView` envelope recording the archived
/// `payload_uri`.
///
/// Lifecycle phase (reported for integration): this builder is CONTENT-derived
/// — it reads only `content_units`, never fresh annotations — so it may run
/// PRE-activation (as soon as the parse's units are durable) OR post-activation.
/// Recommendation: run it PRE-activation alongside the other content-derived
/// projections (lexical/dense/chunk/multivector), so a parse's derived view is
/// ready at cutover rather than lagging behind it.
///
/// REBUILD idempotence: this builder does NOT delete prior `DerivedView`
/// envelopes for the parse — deleting `retrieval_projections` rows is an
/// envelope operation and `super::envelope` exposes none (design fact 7 forbids
/// hand-writing that SQL here). Integration MUST call the envelope
/// delete-for-parse operation (reported as missing) before invoking this
/// builder so a rebuild replaces rather than accumulates. The artifact-store
/// payload is content-addressed and write-once, so re-archiving identical
/// rendered bytes is a harmless dedup no-op.
///
/// On any failure AFTER the envelope is opened, the envelope is marked failed
/// so the failed build is visible truth (spec §22), not a dangling `building`
/// row; a failure BEFORE the envelope opens surfaces the error with no row to
/// clean up.
pub(crate) fn build_derived_view(
    tx: &Transaction<'_>,
    store: &crate::artifact_store::ArtifactStore,
    source_id: &str,
    parse_id: &str,
) -> Result<String, ApiError> {
    let started = Instant::now();
    info!(
        event = "derived_view.build_started",
        source_id, parse_id, "derived view build starting"
    );

    // Read the ordered canonical units and render BEFORE opening the envelope,
    // so a read/render failure never leaves a dangling `building` row. The
    // rendered document is derived from canonical units ONLY (design fact 5).
    let units = read_parse_units(tx, parse_id)?;
    let rendered = render_document(&units);
    let rendered_unit_ids: Vec<String> = rendered.unit_ids;

    // Archive the rendered payload to the content-addressed store: this
    // projection's payload IS archived (unlike lexical/dense, whose payload
    // lives in a hot-plane payload table), so its bytes are preserved for
    // forensic snapshots (spec §22 rule 1) and the envelope records their URI.
    let artifact = store
        .put_bytes(rendered.markdown.as_bytes())
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to archive derived view for parse {parse_id}: {source}"),
        })?;

    // The producer is a deterministic System renderer; input_refs name the
    // rendered ContentUnits so the projection's lineage points back at its
    // canonical inputs (§20).
    let producer = derived_view_producer(&rendered_unit_ids);
    let request = NewProjection {
        source_id: source_id.to_owned(),
        parse_id: parse_id.to_owned(),
        projection_type: ProjectionType::DerivedView,
        input_unit_ids: Some(rendered_unit_ids.clone()),
        // A derived view is unit-derived, not annotation-derived: no
        // inputAnnotationIds (spec §22 sets that only for summary/graph).
        input_annotation_ids: None,
        producer,
        index_name: None,
        index_partition: None,
    };

    let projection_id = envelope::insert_building(tx, &request)?;

    // Record the archived payload URI at complete_fresh (spec §22 / envelope
    // COMPLETE_FRESH_SQL): the payload_uri arrives now, not at insert. Any
    // failure completing marks the envelope failed so the build is visible.
    if let Err(complete_error) = envelope::complete_fresh(tx, &projection_id, Some(&artifact.uri)) {
        record_build_failure(
            tx,
            &projection_id,
            "derived_view",
            source_id,
            parse_id,
            &complete_error,
        );
        return Err(complete_error);
    }

    info!(
        event = "derived_view.build_succeeded",
        // The blob exists; its envelope is not durable until the caller commits.
        persistence = "payload_archived_envelope_pending_commit",
        source_id,
        parse_id,
        projection_id = %projection_id,
        unit_count = rendered_unit_ids.len(),
        payload_hash = %artifact.hash,
        payload_size_bytes = artifact.size_bytes,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "derived view build succeeded"
    );
    Ok(projection_id)
}

/// Build the `summary` projection for a parse: read the parse's FRESH summary
/// annotations, materialize their linkage into a `Summary` envelope, and link
/// the source annotations via `input_annotation_ids` (spec §22
/// inputAnnotationIds).
///
/// Lifecycle phase (reported for integration): this builder MUST run
/// POST-activation. It reads fresh annotations through
/// `annotations::store::fresh_for_active_parse`, whose active-parse subselect
/// returns rows only once the parse is the source's active parse; running it
/// pre-activation would read zero annotations and materialize an empty summary.
/// Recommendation: wire it on the annotation-worker completion hook, after the
/// summary annotations are fresh.
///
/// Payload archival decision (reported): the summary payload is NOT re-archived
/// to the artifact store. The summary text already lives durably on each source
/// `summary` annotation (`semantic_annotations.body_json`), preserved in
/// forensic snapshots via the annotation, and the projection's job is
/// MATERIALIZATION/LINKAGE — it points at those annotations through
/// `input_annotation_ids` rather than copying their bytes. So the envelope
/// completes with `payload_uri = None`: re-archiving would duplicate durable
/// content and create a second thing to keep consistent. (This is the same
/// "payload lives elsewhere" shape envelope::complete_fresh documents for a
/// None URI.)
///
/// A parse with no fresh summary annotations still opens+completes an empty
/// `Summary` projection (empty `input_annotation_ids`): an empty materialized
/// summary is visible truth (spec §22), not silent absence.
///
/// EMPTY-MARKER SKIP: a summary annotation whose body is EXACTLY `[]` is the
/// annotation worker's by-design empty-producer marker (no summary was produced),
/// not a corrupt body. It is SKIPPED before the `summary_text` extraction and the
/// skip is COUNTED (`skipped_summary_markers` in the success log) — a visible,
/// counted skip, distinct from the loud failure a genuinely malformed body still
/// gets. Such a marker still appears in `input_annotation_ids` (it was consumed).
///
/// REBUILD idempotence: like the view builder, this does NOT delete prior
/// `Summary` envelopes — that is an envelope operation this module must not
/// hand-write (design fact 7). Integration MUST delete-for-parse (reported as
/// missing) before invoking.
pub(crate) fn build_summary(
    tx: &Transaction<'_>,
    conn: &Connection,
    source_id: &str,
    parse_id: &str,
) -> Result<String, ApiError> {
    let started = Instant::now();
    debug!(
        event = "summary.build_started",
        source_id, parse_id, "summary projection build starting"
    );

    // Read fresh annotations for the source's active parse and keep only the
    // Summary type; freshness AND active-parse scoping are enforced inside the
    // store query, so this builder trusts the returned set. Filter to the
    // requested parse defensively so a caller passing a non-active parse_id
    // materializes an empty summary rather than another parse's annotations.
    let summaries: Vec<SemanticAnnotation> = envelope_free_summary_annotations(conn, source_id)?
        .into_iter()
        .filter(|annotation| {
            annotation.annotation_type == SemanticAnnotationType::Summary
                && annotation.parse_id == parse_id
        })
        .collect();

    // DELIBERATE: skipped empty-marker rows REMAIN in this lineage — the builder
    // consumed them (it read and classified each), and the skip is made visible
    // via the logged `skipped_summary_markers` count below, not by omitting them
    // from lineage.
    let input_annotation_ids: Vec<String> = summaries
        .iter()
        .map(|annotation| annotation.id.clone())
        .collect();

    // Extract each summary's text from its `{ "text": <string> }` body. A body
    // missing the `text` field is a corrupt/malformed summary annotation and is
    // surfaced loudly: the materialization must be an honest reflection of its
    // inputs. The one body shape NOT surfaced loudly is the by-design empty
    // marker (body EXACTLY `[]`): the annotation worker records an empty producer
    // result as a fresh `[]`-bodied annotation so the freshness key stays
    // satisfied and the work is not rediscovered each cycle. That marker carries
    // no summary, so it is skipped here and the skip is COUNTED
    // (`skipped_summary_markers`), never fed to `summary_text`. `extracted_texts`
    // counts only REAL summaries. Both counts are logged for diagnostics; neither
    // the text nor the body is ever persisted here (the text lives on the
    // annotation) or logged (document contents are forbidden in logs).
    // MUST STAY IN STEP with the worker's empty-marker write site
    // (`crate::annotations::worker::complete_build`) and the two sibling consumer
    // skips (`crate::projections::graph::accumulate_mentions` / `derive_edges`);
    // the worker's must-stay-in-step banner names all three.
    let mut extracted_texts = 0usize;
    let mut skipped_summary_markers = 0usize;
    for annotation in &summaries {
        // Skip the empty-marker row (body is EXACTLY `[]`) before extraction; it
        // encodes "no summary produced", not a corrupt summary (see above).
        if annotation.body.as_array().is_some_and(Vec::is_empty) {
            skipped_summary_markers += 1;
            continue;
        }
        summary_text(annotation)?;
        extracted_texts += 1;
    }

    let producer = summary_producer(&input_annotation_ids);
    let request = NewProjection {
        source_id: source_id.to_owned(),
        parse_id: parse_id.to_owned(),
        projection_type: ProjectionType::Summary,
        // A summary is annotation-derived: no input UNIT ids; its lineage is
        // the source annotations (spec §22 inputAnnotationIds).
        input_unit_ids: None,
        input_annotation_ids: Some(input_annotation_ids.clone()),
        producer,
        index_name: None,
        index_partition: None,
    };

    let projection_id = envelope::insert_building(tx, &request)?;

    // The summary payload lives on its source annotations, so the envelope
    // completes with payload_uri = None (see the function-level decision note).
    if let Err(complete_error) = envelope::complete_fresh(tx, &projection_id, None) {
        record_build_failure(
            tx,
            &projection_id,
            "summary",
            source_id,
            parse_id,
            &complete_error,
        );
        return Err(complete_error);
    }

    debug!(
        event = "summary.build_succeeded",
        persistence = "pending_commit",
        source_id,
        parse_id,
        projection_id = %projection_id,
        summary_annotation_count = input_annotation_ids.len(),
        extracted_texts,
        skipped_summary_markers,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "summary projection build succeeded"
    );
    Ok(projection_id)
}

/// The rendered document plus the ordered ids of the units that contributed to
/// it. `unit_ids` becomes the projection's `input_unit_ids`, so it lists
/// exactly the units whose content is reflected in `markdown`.
struct RenderedDocument {
    markdown: String,
    unit_ids: Vec<String>,
}

/// Render the ordered canonical units into a Markdown-ish document (spec §22
/// rule 3). This is the ONLY place canonical content becomes rendered bytes,
/// and it reads ONLY the units passed in (design fact 5 — never Docling).
///
/// Which unit types render, and how (the renderable carriers named in the
/// brief; every other type is a structural container carrying no direct
/// rendered text and is skipped, mirroring the CA producer's evidence-text
/// selection):
///   - text_section -> a Markdown heading from `headingText` at
///     `headingLevel` (clamped to 1..=6); a section with no heading text
///     emits no line but still scopes the blocks that follow it in reading
///     order. Sections carry the document's structure, so they render at the
///     structural (heading) level, not as body text.
///   - text_block   -> `body.text` as a paragraph.
///   - caption      -> `body.text` as an italic caption line.
///   - table_cell   -> `body.text`, else `body.normalizedText`, as a
///     paragraph. (Full table reconstruction is out of scope; each cell's
///     text renders in reading order so its content is not dropped.)
///
/// The tree is walked in READING ORDER (the SELECT ordering), not by
/// re-deriving structure from `primary_parent_id`: reading order already
/// linearizes the unit tree, and rendering in that order reproduces the
/// document's natural top-to-bottom flow. `primary_parent_id` is read but not
/// needed for the linear render; it is retained on `RenderUnit` so a future
/// nested renderer can use it without another read.
///
/// Only units that actually emit a line contribute their id to `unit_ids`, so
/// the projection's `input_unit_ids` names exactly the units reflected in the
/// bytes (an empty-text block that renders nothing is not claimed as an input).
fn render_document(units: &[RenderUnit]) -> RenderedDocument {
    let mut lines: Vec<String> = Vec::new();
    let mut unit_ids: Vec<String> = Vec::new();

    for unit in units {
        let rendered_line = match unit.content_type {
            ContentType::TextSection => render_section_heading(&unit.body),
            ContentType::TextBlock => text_field(&unit.body, "text").map(str::to_owned),
            ContentType::Caption => text_field(&unit.body, "text").map(|text| format!("*{text}*")),
            ContentType::TableCell => text_field(&unit.body, "text")
                .or_else(|| text_field(&unit.body, "normalizedText"))
                .map(str::to_owned),
            // Structural / non-text carriers: pages, tables, rows, figures,
            // image regions, and code blocks are not rendered by this view
            // (code blocks carry `code`, not `text`; a fenced-code renderer is
            // out of the approved scope). They scope no rendered line.
            ContentType::Page
            | ContentType::Table
            | ContentType::TableRow
            | ContentType::Figure
            | ContentType::ImageRegion
            | ContentType::CodeBlock => None,
        };

        if let Some(line) = rendered_line {
            if line.trim().is_empty() {
                // An empty-text renderable carrier contributes nothing and is
                // not claimed as an input (see function doc).
                continue;
            }
            lines.push(line);
            unit_ids.push(unit.id.clone());
        }
    }

    // Blocks are separated by a blank line so the Markdown reads as distinct
    // paragraphs/headings; a trailing newline is omitted so identical unit sets
    // render byte-identical documents (stable content-addressed hash).
    RenderedDocument {
        markdown: lines.join("\n\n"),
        unit_ids,
    }
}

/// Render a `text_section` body as a Markdown heading. Returns `None` when the
/// section carries no `headingText` (a heading-less structural section emits no
/// line). `headingLevel` selects `#` depth, clamped to Markdown's 1..=6; an
/// absent level defaults to 1 (top-level heading).
fn render_section_heading(body: &Value) -> Option<String> {
    let heading = text_field(body, "headingText")?;
    let level = body
        .get("headingLevel")
        .and_then(Value::as_u64)
        .unwrap_or(1)
        .clamp(1, 6) as usize;
    Some(format!("{} {heading}", "#".repeat(level)))
}

/// Read a non-empty string field from a unit body, returning `None` for a
/// missing, non-string, or whitespace-only value. Whitespace-only text carries
/// no rendered content, so it is treated as absent.
fn text_field<'body>(body: &'body Value, field: &str) -> Option<&'body str> {
    body.get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
}

/// Extract a summary annotation's text from its `{ "text": <string> }` body
/// (the CA summary body shape). A missing or non-string `text` is a corrupt
/// summary annotation surfaced as an error with the annotation id — the
/// materialization reflects its inputs honestly and fails loudly on a malformed
/// body. The by-design empty-marker (`[]`) body never reaches here — `build_
/// summary` skips and counts it BEFORE calling this; that visible, counted skip
/// is not a malformed body.
fn summary_text(annotation: &SemanticAnnotation) -> Result<String, ApiError> {
    annotation
        .body
        .get("text")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| ApiError::StorageOperation {
            message: format!(
                "summary annotation {} has no string `text` body field",
                annotation.id
            ),
        })
}

/// Assemble the `derived_view` producer Provenance (§20): a deterministic
/// `System` renderer naming the rendered ContentUnits as its inputs. No model
/// fields — the render is not a model call.
fn derived_view_producer(rendered_unit_ids: &[String]) -> Provenance {
    Provenance {
        producer_type: ProducerType::System,
        producer_name: DERIVED_VIEW_PRODUCER_NAME.to_owned(),
        producer_version: Some(PRODUCER_VERSION.to_owned()),
        config_hash: None,
        model_name: None,
        model_version: None,
        prompt_hash: None,
        temperature: None,
        confidence: None,
        memoized: None,
        memoized_from: None,
        memoization_key_hash: None,
        input_refs: Some(input_refs(
            crate::model::ProvenanceObjectType::ContentUnit,
            rendered_unit_ids,
        )),
    }
}

/// Assemble the `summary` producer Provenance (§20): a `System` materializer
/// naming the source summary annotations as its inputs. Distinct from the
/// `annotator_summary` model producer that authored the text — this producer
/// only materializes/links, so it carries no model or prompt fields.
fn summary_producer(input_annotation_ids: &[String]) -> Provenance {
    Provenance {
        producer_type: ProducerType::System,
        producer_name: SUMMARY_PRODUCER_NAME.to_owned(),
        producer_version: Some(PRODUCER_VERSION.to_owned()),
        config_hash: None,
        model_name: None,
        model_version: None,
        prompt_hash: None,
        temperature: None,
        confidence: None,
        memoized: None,
        memoized_from: None,
        memoization_key_hash: None,
        input_refs: Some(input_refs(
            crate::model::ProvenanceObjectType::SemanticAnnotation,
            input_annotation_ids,
        )),
    }
}

/// Build the `ProvenanceInputRef` list for a set of input ids of one object
/// type (§20 lineage), shared by both producers so the two provenance builders
/// do not duplicate the mapping.
fn input_refs(
    object_type: crate::model::ProvenanceObjectType,
    ids: &[String],
) -> Vec<crate::model::ProvenanceInputRef> {
    ids.iter()
        .map(|id| crate::model::ProvenanceInputRef {
            object_type,
            id: id.clone(),
        })
        .collect()
}

/// Mark an already-opened envelope failed on a post-open build failure, so the
/// failed build is visible truth (spec §22) rather than a dangling `building`
/// row. A secondary failure marking-failed is logged (the row may already be
/// gone) but never masks the original error, which the caller still returns.
fn record_build_failure(
    tx: &Transaction<'_>,
    projection_id: &str,
    kind: &str,
    source_id: &str,
    parse_id: &str,
    cause: &ApiError,
) {
    error!(
        event = "projection_view.build_failed",
        kind,
        source_id,
        parse_id,
        projection_id,
        error = %cause,
        "projection build failed after envelope opened; marking failed"
    );
    if let Err(mark_error) = envelope::mark_failed(tx, projection_id, &cause.to_string()) {
        error!(
            event = "projection_view.mark_failed_failed",
            kind,
            projection_id,
            error = %mark_error,
            "failed to mark projection failed after a build failure"
        );
    }
}

/// Read one parse's fresh summary-eligible annotations for a source's active
/// parse via the shared annotation store reader (active-parse + freshness
/// scoping enforced in that query). Named to make clear this builder owns NO
/// annotation SQL: it delegates entirely to `annotations::store`.
fn envelope_free_summary_annotations(
    conn: &Connection,
    source_id: &str,
) -> Result<Vec<SemanticAnnotation>, ApiError> {
    crate::annotations::store::fresh_for_active_parse(conn, source_id)
}

/// Read a parse's content units in reading order for rendering. Directly reads
/// `content_units` (not an envelope-owned table; the CA producer reads it the
/// same way) because the ordered unit tree has no shared reader exposed to this
/// module. SQL failures surface as `StorageOperation` naming the parse.
fn read_parse_units(tx: &Transaction<'_>, parse_id: &str) -> Result<Vec<RenderUnit>, ApiError> {
    let mut statement =
        tx.prepare(SELECT_PARSE_UNITS_SQL)
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to prepare parse-units query for parse {parse_id}: {source}"
                ),
            })?;
    let rows = statement
        .query_map(params![parse_id], |row| {
            Ok(RenderUnitRow {
                id: row.get(0)?,
                content_type: row.get(1)?,
                primary_parent_id: row.get(2)?,
                sequence_index: row.get(3)?,
                body_json: row.get(4)?,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query units for parse {parse_id}: {source}"),
        })?;

    let mut units = Vec::new();
    for row in rows {
        let row = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read unit row for parse {parse_id}: {source}"),
        })?;
        units.push(render_unit(row)?);
    }
    Ok(units)
}

/// One `content_units` row as read for rendering, before its wire-string type
/// and JSON body are re-typed. `primary_parent_id` and `sequence_index` are
/// read (they define the tree and reading order the query orders by) even
/// though the linear renderer does not currently branch on them.
struct RenderUnitRow {
    id: String,
    content_type: String,
    primary_parent_id: Option<String>,
    sequence_index: Option<u64>,
    body_json: String,
}

/// Re-type one persisted unit row into a `RenderUnit`. `content_type` re-types
/// through the model enum (a value outside the schema CHECK set fails loudly)
/// and `body_json` parses back to JSON so the renderer can select its text
/// field; a corrupt body is surfaced with the unit id, never silently rendered
/// as blank.
fn render_unit(row: RenderUnitRow) -> Result<RenderUnit, ApiError> {
    let content_type: ContentType = serde_json::from_value(Value::String(row.content_type.clone()))
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "persisted content unit {} type {:?} is not a known variant: {source}",
                row.id, row.content_type
            ),
        })?;
    let body: Value =
        serde_json::from_str(&row.body_json).map_err(|source| ApiError::StorageOperation {
            message: format!(
                "persisted body of content unit {} is unparseable: {source}",
                row.id
            ),
        })?;
    // Touch the read-but-unused structural fields so the query columns stay
    // wired to the row shape (a future nested renderer consumes them); this
    // keeps the columns meaningful without a dead-field warning masking a real
    // drop later.
    let _ = (&row.primary_parent_id, &row.sequence_index);
    Ok(RenderUnit {
        id: row.id,
        content_type,
        body,
    })
}
