//! Query-time passages built from canonical units in the caller's read snapshot.
//! Persisted embeddings still rank seed units; the final reranker sees exactly
//! the bounded passage text returned to the reader, together with its heading.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;
use tokenizers::Tokenizer;
use tracing::{error, info};

use crate::assembly::evidence::evidence_text;
use crate::assembly::model::MAX_PASSAGE_UNITS;
use crate::error::ApiError;
use crate::inference::ColbertCandidateScore;
use crate::model::{ContentType, Locator, SourceLocationStatus};
use crate::projections::MAX_UNIT_TOKENS;
use crate::query::model::RetrievalHit;
use crate::query::provenance::{
    AnnotationContribution, RetrievalChannel, RetrievalProvenance, UnitRetrievalMatch,
};

// Bound cells before bringing them into Rust. Oversized authoritative data is
// an explicit error, never a silently shortened raw evidence record.
const MAX_CELL_BYTES: usize = 1_048_576;
const MAX_SOURCE_LOCATIONS: usize = 256;
const UNIT_SQL: &str = "
SELECT source_id, content_type,
       CASE WHEN length(CAST(body_json AS BLOB)) <= ?3 THEN body_json END,
       CASE WHEN length(CAST(COALESCE(locators_json, '[]') AS BLOB)) <= ?3
            THEN COALESCE(locators_json, '[]') END
FROM content_units WHERE parse_id = ?1 AND id = ?2";
const SECTION_SQL: &str = "
SELECT DISTINCT p.id
FROM unit_relationships r JOIN content_units p
  ON p.parse_id = r.parse_id AND p.id = r.from_unit_id
WHERE r.parse_id = ?1 AND r.to_unit_id = ?2
  AND r.relationship_type IN ('logically_contains', 'contains')
  AND p.content_type != 'page'
LIMIT 2";
const PREVIOUS_SQL: &str = "
SELECT DISTINCT CASE WHEN from_unit_id = ?2 THEN to_unit_id ELSE from_unit_id END
FROM unit_relationships WHERE parse_id = ?1 AND
 ((relationship_type = 'precedes' AND to_unit_id = ?2) OR
  (relationship_type = 'follows' AND from_unit_id = ?2)) LIMIT 2";
const NEXT_SQL: &str = "
SELECT DISTINCT CASE WHEN from_unit_id = ?2 THEN to_unit_id ELSE from_unit_id END
FROM unit_relationships WHERE parse_id = ?1 AND
 ((relationship_type = 'precedes' AND from_unit_id = ?2) OR
  (relationship_type = 'follows' AND to_unit_id = ?2)) LIMIT 2";
const LOCATIONS_SQL: &str = "
SELECT CASE WHEN length(CAST(native_uri AS BLOB)) <= ?2 THEN native_uri END, status
FROM source_locations WHERE source_id = ?1
ORDER BY CASE status WHEN 'current' THEN 0 WHEN 'access_lost' THEN 1 ELSE 2 END,
         source_system, native_uri LIMIT ?3";

/// A recorded location, including availability so a historical path cannot be
/// mistaken for a currently accessible source.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SourceCitation {
    pub(crate) native_uri: String,
    pub(crate) status: SourceLocationStatus,
}

/// Reader-facing result, with canonical identities retained for inspection.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SearchResult {
    pub(crate) text: String,
    pub(crate) source_id: String,
    pub(crate) parse_id: String,
    pub(crate) unit_ids: Vec<String>,
    pub(crate) source_locations: Vec<SourceCitation>,
    pub(crate) section_path: Vec<String>,
    /// Physical PDF page positions, not printed page labels inferred from text.
    pub(crate) page_numbers: Vec<u64>,
    pub(crate) score: f64,
    /// True only when a single oversized canonical unit had to be excerpted.
    pub(crate) truncated: bool,
    pub(crate) retrieval_provenance: RetrievalProvenance,
}

/// One canonical text contribution; the raw body is retained by evidence assembly.
#[derive(Debug, Clone)]
struct PassagePart {
    id: String,
    text: String,
    pages: Vec<u64>,
}

/// Bounded candidate keyed by its strongest seed's canonical unit identity.
/// The same key follows it through the existing reranker protocol and diagnostics.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PassageCandidate {
    pub(crate) anchor_unit_id: String,
    pub(crate) source_id: String,
    pub(crate) parse_id: String,
    pub(crate) unit_ids: Vec<String>,
    pub(crate) text: String,
    pub(crate) section_path: Vec<String>,
    pub(crate) truncated: bool,
    #[serde(skip)]
    section_id: Option<String>,
    #[serde(skip)]
    parts: Vec<PassagePart>,
}

impl PassageCandidate {
    /// Include the structural heading for scoring without inventing source prose.
    pub(crate) fn ranking_text(&self) -> String {
        if self.section_path.is_empty() {
            self.text.clone()
        } else {
            format!("{}\n\n{}", self.section_path.join(" / "), self.text)
        }
    }

    /// Attach source locations only for final results, on the same read snapshot.
    pub(crate) fn into_result(
        self,
        conn: &Connection,
        score: f64,
        pool: &[RetrievalHit],
        channel_hits: &[RetrievalHit],
    ) -> Result<SearchResult, ApiError> {
        let retrieval_provenance = self.retrieval_provenance(pool, channel_hits)?;
        let source_locations = read_locations(conn, &self.source_id)?;
        let page_numbers = self
            .parts
            .iter()
            .flat_map(|p| p.pages.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Ok(SearchResult {
            text: self.text,
            source_id: self.source_id,
            parse_id: self.parse_id,
            unit_ids: self.unit_ids,
            source_locations,
            section_path: self.section_path,
            page_numbers,
            score,
            truncated: self.truncated,
            retrieval_provenance,
        })
    }

    /// Attribute final membership after all merges. Only admitted candidate units
    /// count as retrieved matches; expanded context must not acquire a channel
    /// merely because another unit in the passage matched. Covered candidates do
    /// retain their own matches, without claiming they caused passage formation.
    fn retrieval_provenance(
        &self,
        pool: &[RetrievalHit],
        channel_hits: &[RetrievalHit],
    ) -> Result<RetrievalProvenance, ApiError> {
        let mut matched_units = Vec::new();
        let mut context_unit_ids = Vec::new();
        let mut channels = Vec::new();
        let mut has_graph = false;
        let mut has_graph_only = false;
        for unit_id in &self.unit_ids {
            let owns_unit = |hit: &&RetrievalHit| {
                hit.source_id == self.source_id
                    && hit.parse_id == self.parse_id
                    && hit.unit_ids.contains(unit_id)
            };
            if !pool.iter().any(|hit| owns_unit(&hit)) {
                context_unit_ids.push(unit_id.clone());
                continue;
            }
            let mut unit_channels = Vec::new();
            let mut graph_matches = BTreeSet::new();
            for hit in channel_hits.iter().filter(owns_unit) {
                if !unit_channels.contains(&hit.channel) {
                    unit_channels.push(hit.channel);
                }
                if !channels.contains(&hit.channel) {
                    channels.push(hit.channel);
                }
                graph_matches.extend(hit.graph_matches.iter().cloned());
            }
            if unit_channels.is_empty() {
                return Err(failure(format!(
                    "admitted passage unit {unit_id} in {} has no channel provenance",
                    self.parse_id
                )));
            }
            let graph = unit_channels.contains(&RetrievalChannel::Graph);
            has_graph |= graph;
            has_graph_only |= graph && unit_channels.len() == 1;
            matched_units.push(UnitRetrievalMatch {
                unit_id: unit_id.clone(),
                channels: unit_channels,
                graph_matches: graph_matches.into_iter().collect(),
            });
        }
        // Exclusivity is per unit against this query's capped channel lists,
        // never a counterfactual claim about the whole passage or answer quality.
        let annotation_contribution = if has_graph_only {
            AnnotationContribution::AdditionalMatches
        } else if has_graph {
            AnnotationContribution::Overlap
        } else {
            AnnotationContribution::None
        };
        Ok(RetrievalProvenance {
            channels,
            annotation_contribution,
            matched_units,
            context_unit_ids,
        })
    }
}

/// Loaded canonical body and provenance; all lookups are scoped by captured parse.
struct Unit {
    id: String,
    source_id: String,
    content_type: ContentType,
    body: Value,
    locators: Vec<Locator>,
}

impl Unit {
    /// Recognize page furniture by the parser's explicit role, never by guessing
    /// from numeric text (numbers can be legitimate evidence).
    fn is_furniture(&self) -> bool {
        self.content_type == ContentType::TextBlock
            && matches!(
                self.body.get("blockRole").and_then(Value::as_str),
                Some("header" | "footer")
            )
    }

    /// Only ordinary prose is expanded; lists, formulas, captions, cells and code
    /// retain their own structural boundaries instead of being mixed into prose.
    fn is_prose(&self) -> bool {
        self.content_type == ContentType::TextBlock
            && matches!(
                self.body.get("blockRole").and_then(Value::as_str),
                None | Some("paragraph" | "unknown")
            )
    }

    /// Resolve the shared evidence-text contract and its physical page citations.
    fn part(&self) -> Option<PassagePart> {
        let text = evidence_text(self.content_type, &self.body)?;
        if text.trim().is_empty() {
            return None;
        }
        let pages = self
            .locators
            .iter()
            .filter_map(|locator| match locator {
                Locator::PageBbox(page) => Some(page.page_number),
                _ => None,
            })
            .collect();
        Some(PassagePart {
            id: self.id.clone(),
            text,
            pages,
        })
    }
}

/// Build distinct passages in ColBERT seed order and retain every candidate for
/// diagnostics before the caller applies the final passage reranker.
pub(crate) fn build_passages(
    conn: &Connection,
    ranked: &[ColbertCandidateScore],
    parse_of_unit: &BTreeMap<String, String>,
    tokenizer: &Tokenizer,
    candidate_limit: usize,
    query_id: &str,
) -> Result<Vec<PassageCandidate>, ApiError> {
    let started = Instant::now();
    info!(
        event = "query.passages.started",
        query_id,
        seeds = ranked.len(),
        candidate_limit,
        max_tokens = MAX_UNIT_TOKENS,
        max_units = MAX_PASSAGE_UNITS,
        "constructing passage candidates"
    );
    let mut counts = PassageBuildCounts::default();
    let outcome = build_passages_body(
        conn,
        ranked,
        parse_of_unit,
        tokenizer,
        candidate_limit,
        &mut counts,
    );
    match &outcome {
        Ok(passages) => info!(
            event = "query.passages.completed",
            query_id,
            seeds = ranked.len(),
            passages = passages.len(),
            truncated = passages.iter().filter(|p| p.truncated).count(),
            covered_seeds = counts.covered_seeds,
            unrenderable_seeds = counts.unrenderable_seeds,
            merged_windows = counts.merged_windows,
            overlap_rejections = counts.overlap_rejections,
            candidate_limit_exclusions = counts.candidate_limit_exclusions,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "passage candidates ready"
        ),
        Err(failure) => error!(event = "query.passages.failed", query_id,
            error = %failure, elapsed_ms = started.elapsed().as_millis() as u64,
            error_chain = %crate::util::error_chain(failure),
            stage = "passage_construction", seeds = ranked.len(), candidate_limit,
            "passage construction failed"),
    }
    outcome
}

/// Selection accounting distinguishes deduplication from eligibility and budget loss.
#[derive(Default)]
struct PassageBuildCounts {
    covered_seeds: usize,
    unrenderable_seeds: usize,
    merged_windows: usize,
    overlap_rejections: usize,
    candidate_limit_exclusions: usize,
}

/// Keep higher-ranked seeds when a bounded overlap cannot be merged; never
/// return two candidates sharing canonical text. Disjoint windows remain separate.
fn build_passages_body(
    conn: &Connection,
    ranked: &[ColbertCandidateScore],
    parse_of_unit: &BTreeMap<String, String>,
    tokenizer: &Tokenizer,
    candidate_limit: usize,
    counts: &mut PassageBuildCounts,
) -> Result<Vec<PassageCandidate>, ApiError> {
    let mut passages: Vec<PassageCandidate> = Vec::new();
    for seed in ranked {
        if passages.iter().any(|p| p.unit_ids.contains(&seed.unit_id)) {
            counts.covered_seeds += 1;
            continue;
        }
        let parse_id = parse_of_unit.get(&seed.unit_id).ok_or_else(|| {
            failure(format!(
                "passage seed {} has no captured parse",
                seed.unit_id
            ))
        })?;
        let Some(candidate) =
            build_passage(conn, parse_id, &seed.unit_id, tokenizer).map_err(|source| {
                failure(format!(
                    "passage seed {} in {parse_id}: {source}",
                    seed.unit_id
                ))
            })?
        else {
            counts.unrenderable_seeds += 1;
            continue;
        };
        let overlapping: Vec<usize> = passages
            .iter()
            .enumerate()
            .filter_map(|(i, existing)| {
                (existing.parse_id == candidate.parse_id
                    && existing
                        .unit_ids
                        .iter()
                        .any(|id| candidate.unit_ids.contains(id)))
                .then_some(i)
            })
            .collect();
        if overlapping.is_empty() {
            passages.push(candidate);
            continue;
        }
        // Work on an owned trial so a cap failure leaves stronger candidates intact.
        let first = overlapping[0];
        let mut merged = passages[first].clone();
        let mut fits = merge_passage(&mut merged, &candidate, tokenizer)?;
        for &index in overlapping.iter().skip(1) {
            if fits {
                fits = merge_passage(&mut merged, &passages[index], tokenizer)?;
            }
        }
        if fits {
            counts.merged_windows += overlapping.len();
            passages[first] = merged;
            for &index in overlapping.iter().skip(1).rev() {
                passages.remove(index);
            }
        } else {
            counts.overlap_rejections += 1;
        }
    }
    counts.candidate_limit_exclusions = passages.len().saturating_sub(candidate_limit);
    passages.truncate(candidate_limit);
    Ok(passages)
}

/// Follow canonical reading-order edges around a seed, stopping at section or
/// content boundaries. Alternating directions avoids spending the whole budget
/// on just the preceding context. Traversal itself is bounded even across furniture.
fn build_passage(
    conn: &Connection,
    parse_id: &str,
    anchor: &str,
    tokenizer: &Tokenizer,
) -> Result<Option<PassageCandidate>, ApiError> {
    let unit = read_unit(conn, parse_id, anchor)?;
    if unit.is_furniture() {
        return Ok(None);
    }
    let Some(mut part) = unit.part() else {
        return Ok(None);
    };
    let (section_id, section_path) = read_section(conn, parse_id, anchor)?;
    let (text, truncated) = bounded_text(&part.text, tokenizer)?;
    part.text = text;
    let mut parts = vec![part];
    if unit.is_prose() && !truncated {
        let mut cursors = [Some(anchor.to_string()), Some(anchor.to_string())];
        let mut visited = BTreeSet::from([anchor.to_string()]);
        for step in 0..MAX_PASSAGE_UNITS - 1 {
            let direction = step % 2;
            let Some(current) = cursors[direction].as_deref() else {
                if cursors.iter().all(Option::is_none) {
                    break;
                }
                continue;
            };
            let sql = if direction == 0 {
                PREVIOUS_SQL
            } else {
                NEXT_SQL
            };
            let next = unique_link(conn, sql, parse_id, current, "reading order")?;
            cursors[direction] = next.clone();
            let Some(next) = next else {
                continue;
            };
            if !visited.insert(next.clone()) {
                return Err(failure(format!(
                    "reading-order cycle at unit {next} in parse {parse_id}"
                )));
            }
            let neighbor = read_unit(conn, parse_id, &next)?;
            if neighbor.source_id != unit.source_id {
                return Err(failure(format!(
                    "passage crosses source at unit {next} in parse {parse_id}"
                )));
            }
            if neighbor.is_furniture() {
                continue;
            }
            if !neighbor.is_prose() || read_section(conn, parse_id, &next)?.0 != section_id {
                cursors[direction] = None;
                continue;
            }
            let Some(part) = neighbor.part() else {
                continue;
            };
            let joined = if direction == 0 {
                format!("{}\n\n{}", part.text, join_parts(&parts))
            } else {
                format!("{}\n\n{}", join_parts(&parts), part.text)
            };
            if token_count(tokenizer, &joined)? > MAX_UNIT_TOKENS as usize {
                cursors[direction] = None;
            } else if direction == 0 {
                parts.insert(0, part);
            } else {
                parts.push(part);
            }
        }
    }
    Ok(Some(PassageCandidate {
        anchor_unit_id: anchor.to_string(),
        source_id: unit.source_id,
        parse_id: parse_id.to_string(),
        unit_ids: parts.iter().map(|p| p.id.clone()).collect(),
        text: join_parts(&parts),
        section_id,
        section_path,
        truncated,
        parts,
    }))
}

/// Stitch two overlapping ordered windows without sorting opaque unit IDs.
/// A failed trial leaves the destination untouched and preserves its stronger seed.
fn merge_passage(
    destination: &mut PassageCandidate,
    other: &PassageCandidate,
    tokenizer: &Tokenizer,
) -> Result<bool, ApiError> {
    if destination.section_id != other.section_id || destination.truncated || other.truncated {
        return Ok(false);
    }
    let Some((left_index, right_index)) =
        destination.parts.iter().enumerate().find_map(|(i, part)| {
            other
                .parts
                .iter()
                .position(|p| p.id == part.id)
                .map(|j| (i, j))
        })
    else {
        return Ok(false);
    };
    let (earlier, later, offset) = if left_index >= right_index {
        (&destination.parts, &other.parts, left_index - right_index)
    } else {
        (&other.parts, &destination.parts, right_index - left_index)
    };
    let mut combined = earlier.clone();
    for (index, part) in later.iter().enumerate() {
        let position = offset + index;
        if let Some(existing) = combined.get(position) {
            if existing.id != part.id {
                return Err(failure("inconsistent passage reading order"));
            }
        } else {
            combined.push(part.clone());
        }
    }
    let text = join_parts(&combined);
    if combined.len() > MAX_PASSAGE_UNITS
        || token_count(tokenizer, &text)? > MAX_UNIT_TOKENS as usize
    {
        return Ok(false);
    }
    destination.unit_ids = combined.iter().map(|p| p.id.clone()).collect();
    destination.text = text;
    destination.parts = combined;
    Ok(true)
}

/// Preserve paragraph separation when assembling verbatim source contributions.
fn join_parts(parts: &[PassagePart]) -> String {
    parts
        .iter()
        .map(|p| p.text.as_str())
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Use the same tokenizer as persisted chunking for the approved passage bound.
fn token_count(tokenizer: &Tokenizer, text: &str) -> Result<usize, ApiError> {
    tokenizer
        .encode(text, true)
        .map(|encoded| encoded.len())
        .map_err(|source| failure(format!("passage tokenization failed: {source}")))
}

/// Excerpt only an oversized single unit, retaining original UTF-8 bytes and
/// exposing truncation. Its complete canonical body still appears in evidencePack.
fn bounded_text(text: &str, tokenizer: &Tokenizer) -> Result<(String, bool), ApiError> {
    let encoded = tokenizer
        .encode(text, true)
        .map_err(|source| failure(format!("passage tokenization failed: {source}")))?;
    if encoded.len() <= MAX_UNIT_TOKENS as usize {
        return Ok((text.to_string(), false));
    }
    let mut end = encoded
        .get_offsets()
        .iter()
        .take(MAX_UNIT_TOKENS as usize)
        .map(|(_, end)| *end)
        .max()
        .unwrap_or(0)
        .min(text.len());
    while end > 0
        && (!text.is_char_boundary(end)
            || token_count(tokenizer, &text[..end])? > MAX_UNIT_TOKENS as usize)
    {
        end -= 1;
    }
    if end == 0 {
        return Err(failure(
            "passage tokenizer produced no usable excerpt boundary",
        ));
    }
    Ok((text[..end].to_string(), true))
}

/// Read a bounded canonical unit, failing on missing or oversized authoritative data.
fn read_unit(conn: &Connection, parse_id: &str, unit_id: &str) -> Result<Unit, ApiError> {
    let row = conn
        .query_row(
            UNIT_SQL,
            params![parse_id, unit_id, MAX_CELL_BYTES],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(|source| {
            failure(format!(
                "read passage unit {unit_id} in {parse_id}: {source}"
            ))
        })?
        .ok_or_else(|| failure(format!("missing passage unit {unit_id} in {parse_id}")))?;
    let body = row
        .2
        .ok_or_else(|| failure(format!("body of {unit_id} exceeds {MAX_CELL_BYTES} bytes")))?;
    let locators = row.3.ok_or_else(|| {
        failure(format!(
            "locators of {unit_id} exceed {MAX_CELL_BYTES} bytes"
        ))
    })?;
    Ok(Unit {
        id: unit_id.to_string(),
        source_id: row.0,
        content_type: serde_json::from_value(Value::String(row.1))
            .map_err(|source| failure(format!("content type of {unit_id}: {source}")))?,
        body: serde_json::from_str(&body)
            .map_err(|source| failure(format!("body of {unit_id}: {source}")))?,
        locators: serde_json::from_str(&locators)
            .map_err(|source| failure(format!("locators of {unit_id}: {source}")))?,
    })
}

/// Find the nearest logical section through nested tables/figures. Physical page
/// parents never mask logical ancestry; cycles and exhausted traversal fail visibly.
fn read_section(
    conn: &Connection,
    parse_id: &str,
    unit_id: &str,
) -> Result<(Option<String>, Vec<String>), ApiError> {
    let mut current = unit_id.to_string();
    let mut visited = BTreeSet::from([current.clone()]);
    for _ in 0..MAX_PASSAGE_UNITS {
        let Some(id) = unique_link(conn, SECTION_SQL, parse_id, &current, "logical parent")? else {
            return Ok((None, Vec::new()));
        };
        if !visited.insert(id.clone()) {
            return Err(failure(format!(
                "logical containment cycle at {id} in {parse_id}"
            )));
        }
        let parent = read_unit(conn, parse_id, &id)?;
        if parent.content_type == ContentType::TextSection {
            let path = if let Some(value) = parent.body.get("sectionPath") {
                serde_json::from_value::<Vec<String>>(value.clone())
                    .map_err(|source| failure(format!("section path of {id}: {source}")))?
            } else {
                parent
                    .body
                    .get("headingText")
                    .and_then(Value::as_str)
                    .map(|heading| vec![heading.to_string()])
                    .unwrap_or_default()
            };
            return Ok((Some(id), path));
        }
        current = id;
    }
    Err(failure(format!(
        "logical section ancestry of {unit_id} exceeds {MAX_PASSAGE_UNITS} hops"
    )))
}

/// Detect ambiguous graph boundaries explicitly instead of choosing an arbitrary
/// branch and presenting it as a coherent passage.
fn unique_link(
    conn: &Connection,
    sql: &str,
    parse_id: &str,
    unit_id: &str,
    purpose: &str,
) -> Result<Option<String>, ApiError> {
    let mut statement = conn
        .prepare(sql)
        .map_err(|source| failure(format!("prepare {purpose} for {unit_id}: {source}")))?;
    let values = statement
        .query_map(params![parse_id, unit_id], |row| row.get::<_, String>(0))
        .map_err(|source| failure(format!("read {purpose} for {unit_id}: {source}")))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| failure(format!("decode {purpose} for {unit_id}: {source}")))?;
    if values.len() > 1 {
        return Err(failure(format!(
            "ambiguous {purpose} for {unit_id} in {parse_id}"
        )));
    }
    Ok(values.into_iter().next())
}

/// Preserve recorded locations and their states; an absent location is reported
/// as absent by clients rather than replaced with an invented title or path.
fn read_locations(conn: &Connection, source_id: &str) -> Result<Vec<SourceCitation>, ApiError> {
    let mut statement = conn.prepare(LOCATIONS_SQL).map_err(|source| {
        failure(format!(
            "prepare source citations for {source_id}: {source}"
        ))
    })?;
    let rows = statement
        .query_map(
            params![source_id, MAX_CELL_BYTES, MAX_SOURCE_LOCATIONS + 1],
            |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?)),
        )
        .map_err(|source| failure(format!("read source citations for {source_id}: {source}")))?;
    let mut locations = Vec::new();
    for row in rows {
        let (uri, status) =
            row.map_err(|source| failure(format!("source citation for {source_id}: {source}")))?;
        let native_uri = uri.ok_or_else(|| {
            failure(format!(
                "source URI for {source_id} exceeds {MAX_CELL_BYTES} bytes"
            ))
        })?;
        locations.push(SourceCitation {
            native_uri,
            status: serde_json::from_value(Value::String(status))
                .map_err(|source| failure(format!("source status for {source_id}: {source}")))?,
        });
        if locations.len() > MAX_SOURCE_LOCATIONS {
            return Err(failure(format!(
                "source {source_id} exceeds {MAX_SOURCE_LOCATIONS} citation locations"
            )));
        }
    }
    Ok(locations)
}

/// Keep the local graph/storage/tokenizer failure context in the normal API error flow.
fn failure(message: impl Into<String>) -> ApiError {
    ApiError::StorageOperation {
        message: message.into(),
    }
}
