//! Query-time passages built from canonical units in the caller's read snapshot.
//! Persisted embeddings rank source units and exact excerpts; the final reranker
//! receives their bounded canonical text, headings, and separately labeled matched annotations.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;
use tokenizers::Tokenizer;
use tracing::{error, info};

use crate::assembly::evidence::evidence_text;
use crate::assembly::model::MAX_PASSAGE_UNITS;
use crate::canonical::{canonical_sha256_hex_of, sha256_hex_bytes};
use crate::error::ApiError;
use crate::inference::ColbertCandidateScore;
use crate::model::{ContentType, Locator, SourceLocationStatus};
use crate::projections::{MAX_UNIT_TOKENS, annotation::slice_chars};
use crate::query::annotation::ScoredExcerpt;
use crate::query::model::RetrievalHit;
use crate::query::provenance::{
    AnnotationContribution, AnnotationMatch, AnnotationRepresentation, GraphReach,
    RetrievalChannel, RetrievalProvenance, SourceExcerpt, UnitRetrievalMatch,
};
use crate::sections::read_section;

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
    /// Exact Unicode-scalar ranges contributing the displayed text, in passage order.
    pub(crate) source_excerpts: Vec<SourceExcerpt>,
    pub(crate) source_locations: Vec<SourceCitation>,
    pub(crate) section_path: Vec<String>,
    /// Physical PDF page positions, not printed page labels inferred from text.
    pub(crate) page_numbers: Vec<u64>,
    pub(crate) score: f64,
    /// A legacy unit seed was clipped by the passage budget; exact retrieved windows remain complete.
    pub(crate) truncated: bool,
    pub(crate) retrieval_provenance: RetrievalProvenance,
}

/// One canonical text contribution; the raw body is retained by evidence assembly.
#[derive(Debug, Clone)]
struct PassagePart {
    id: String,
    start_char: usize,
    end_char: usize,
    text: String,
    pages: Vec<u64>,
}

impl PassagePart {
    /// Hash the displayed canonical bytes, including the exact range left by any legacy prefix selection.
    fn source_excerpt(&self) -> SourceExcerpt {
        SourceExcerpt {
            unit_id: self.id.clone(),
            start_char: self.start_char,
            end_char: self.end_char,
            text_hash: sha256_hex_bytes(self.text.as_bytes()),
        }
    }
}

/// An annotation context is useful only while its complete supporting range remains in the passage.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ScopedAnnotationContext {
    excerpt: SourceExcerpt,
    text: String,
}

/// Candidate identity describes its final source ranges; the anchor remains a canonical unit ID.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PassageCandidate {
    pub(crate) candidate_id: String,
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
    // Legacy unit nominations belong only to the anchor text actually constructed, never another slice of that unit.
    #[serde(skip)]
    legacy_seed_ranges: Vec<SourceExcerpt>,
    // Matched annotation bodies affect model input, but are not serialized into ordinary diagnostics/results.
    #[serde(skip)]
    annotation_context: Vec<ScopedAnnotationContext>,
    #[serde(skip)]
    graph_context: Vec<String>,
    #[serde(skip)]
    annotation_matches: Vec<AnnotationMatch>,
}

impl PassageCandidate {
    /// Distinguish canonical evidence from derived context so annotations cannot masquerade as source text.
    pub(crate) fn ranking_text(&self) -> String {
        let mut text = String::new();
        if !self.section_path.is_empty() {
            text.push_str(&format!(
                "Source headings:\n{}\n\n",
                self.section_path.join(" / ")
            ));
        }
        text.push_str("Canonical source text:\n");
        text.push_str(&self.text);
        let annotations: BTreeSet<&str> = self
            .annotation_context
            .iter()
            .map(|context| context.text.as_str())
            .collect();
        if !annotations.is_empty() {
            text.push_str("\n\nMatched annotation context (derived from source):\n");
            text.push_str(&annotations.into_iter().collect::<Vec<_>>().join("\n\n"));
        }
        if !self.graph_context.is_empty() {
            text.push_str("\n\nMatched graph context (bounded; whole-unit targeting):\n");
            text.push_str(&self.graph_context.join("\n"));
        }
        text
    }

    /// Expose the source slices actually rendered, never the broader annotation extraction target.
    pub(crate) fn source_excerpts(&self) -> Vec<SourceExcerpt> {
        self.parts.iter().map(PassagePart::source_excerpt).collect()
    }

    /// Include retained graph paths within the existing passage token budget, preserving full provenance separately.
    pub(crate) fn attach_graph_context(
        &mut self,
        channel_hits: &[RetrievalHit],
        tokenizer: &Tokenizer,
    ) -> Result<(), ApiError> {
        let counter = untruncated_tokenizer(tokenizer)?;
        let mut context = Vec::new();
        for hit in channel_hits {
            if !self
                .unit_ids
                .iter()
                .any(|unit_id| self.contains_hit(hit, unit_id))
            {
                continue;
            }
            for matched in &hit.graph_matches {
                let text = match &matched.reach {
                    GraphReach::DirectMention => format!("Entity: {}", matched.matched_entity),
                    GraphReach::RelationSupport { relationship }
                    | GraphReach::RelatedEntityMention { relationship } => {
                        format!(
                            "Entity: {}\nRelationship: {} → {} → {}",
                            matched.matched_entity,
                            relationship.subject,
                            relationship.predicate,
                            relationship.object
                        )
                    }
                };
                if context.contains(&text) {
                    continue;
                }
                let proposed = if context.is_empty() {
                    text.clone()
                } else {
                    format!("{}\n{text}", context.join("\n"))
                };
                if token_count(&counter, &proposed)? <= MAX_UNIT_TOKENS as usize {
                    context.push(text);
                }
            }
        }
        self.graph_context = context;
        Ok(())
    }

    /// Coordinate containment prevents a later match in the same unit from attaching to its prefix.
    fn covers_excerpt(&self, excerpt: &SourceExcerpt) -> bool {
        self.parts.iter().any(|part| {
            part.id == excerpt.unit_id
                && part.start_char <= excerpt.start_char
                && excerpt.start_char < excerpt.end_char
                && part.end_char >= excerpt.end_char
        })
    }

    /// Range overlap, rather than unit equality, lets disjoint source excerpts remain separate candidates.
    fn overlaps(&self, other: &Self) -> bool {
        self.source_id == other.source_id
            && self.parse_id == other.parse_id
            && self.parts.iter().any(|left| {
                other.parts.iter().any(|right| {
                    left.id == right.id
                        && left.start_char < right.end_char
                        && right.start_char < left.end_char
                })
            })
    }

    /// Whole candidate containment permits metadata transfer without expanding its stronger source passage.
    fn covers_candidate(&self, other: &Self) -> bool {
        self.source_id == other.source_id
            && self.parse_id == other.parse_id
            && other.parts.iter().all(|right| {
                self.parts.iter().any(|left| {
                    left.id == right.id
                        && left.start_char <= right.start_char
                        && left.end_char >= right.end_char
                })
            })
    }

    /// Exact hits require their full source range; legacy hits require the retained anchor range they actually seeded.
    fn contains_hit(&self, hit: &RetrievalHit, unit_id: &str) -> bool {
        if hit.source_id != self.source_id
            || hit.parse_id != self.parse_id
            || !hit.unit_ids.iter().any(|id| id == unit_id)
        {
            return false;
        }
        match &hit.source_excerpt {
            Some(excerpt) => excerpt.unit_id == unit_id && self.covers_excerpt(excerpt),
            // This locates the displayed legacy seed, not a newly invented exact annotation match.
            None => self
                .legacy_seed_ranges
                .iter()
                .any(|excerpt| excerpt.unit_id == unit_id && self.covers_excerpt(excerpt)),
        }
    }

    /// Covered or merged candidates transfer only match lineage whose complete support remains displayed.
    fn retain_supported_matches(&mut self, other: &Self) {
        for excerpt in &other.legacy_seed_ranges {
            if self.covers_excerpt(excerpt) && !self.legacy_seed_ranges.contains(excerpt) {
                self.legacy_seed_ranges.push(excerpt.clone());
            }
        }
        for context in &other.annotation_context {
            if self.covers_excerpt(&context.excerpt) && !self.annotation_context.contains(context) {
                self.annotation_context.push(context.clone());
            }
        }
        for matched in &other.annotation_matches {
            if self.covers_excerpt(&matched.excerpt) && !self.annotation_matches.contains(matched) {
                self.annotation_matches.push(matched.clone());
            }
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
        let source_excerpts = self.source_excerpts();
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
            source_excerpts,
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
        let mut has_annotations = false;
        let mut has_annotation_only = false;
        for unit_id in &self.unit_ids {
            let owns_unit = |hit: &&RetrievalHit| self.contains_hit(hit, unit_id);
            if !pool.iter().any(|hit| owns_unit(&hit)) {
                context_unit_ids.push(unit_id.clone());
                continue;
            }
            let mut unit_channels = Vec::new();
            let mut graph_matches = BTreeSet::new();
            let mut dense_matches = BTreeSet::new();
            let mut annotation_matches = BTreeSet::new();
            for hit in channel_hits.iter().filter(owns_unit) {
                // Raw source windows remain dense evidence even when carried by annotation storage.
                let channel = if hit.channel == RetrievalChannel::Semantic
                    && !hit.annotation_matches.is_empty()
                    && hit
                        .annotation_matches
                        .iter()
                        .all(|matched| matched.representation == AnnotationRepresentation::Source)
                {
                    RetrievalChannel::Dense
                } else {
                    hit.channel
                };
                if !unit_channels.contains(&channel) {
                    unit_channels.push(channel);
                }
                if !channels.contains(&channel) {
                    channels.push(channel);
                }
                graph_matches.extend(hit.graph_matches.iter().cloned());
                dense_matches.extend(hit.dense_matches.iter().cloned());
                annotation_matches.extend(
                    hit.annotation_matches
                        .iter()
                        .filter(|matched| {
                            matched.excerpt.unit_id == *unit_id
                                && self.covers_excerpt(&matched.excerpt)
                        })
                        .cloned(),
                );
            }
            annotation_matches.extend(
                self.annotation_matches
                    .iter()
                    .filter(|matched| {
                        matched.excerpt.unit_id == *unit_id && self.covers_excerpt(&matched.excerpt)
                    })
                    .cloned(),
            );
            if unit_channels.is_empty() {
                return Err(failure(format!(
                    "admitted passage unit {unit_id} in {} has no channel provenance",
                    self.parse_id
                )));
            }
            let annotations = unit_channels.iter().any(|channel| {
                matches!(
                    channel,
                    RetrievalChannel::Graph | RetrievalChannel::Semantic
                )
            });
            let source = unit_channels.iter().any(|channel| {
                matches!(channel, RetrievalChannel::Dense | RetrievalChannel::Lexical)
            });
            has_annotations |= annotations;
            has_annotation_only |= annotations && !source;
            matched_units.push(UnitRetrievalMatch {
                unit_id: unit_id.clone(),
                channels: unit_channels,
                graph_matches: graph_matches.into_iter().collect(),
                dense_matches: dense_matches.into_iter().collect(),
                annotation_matches: annotation_matches.into_iter().collect(),
            });
        }
        // Exclusivity is per unit against this query's capped channel lists,
        // never a counterfactual claim about the whole passage or answer quality.
        let annotation_contribution = if has_annotation_only {
            AnnotationContribution::AdditionalMatches
        } else if has_annotations {
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
            start_char: 0,
            end_char: text.chars().count(),
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
    excerpts: &[ScoredExcerpt],
    parse_of_unit: &BTreeMap<String, String>,
    tokenizer: &Tokenizer,
    candidate_limit: usize,
    query_id: &str,
) -> Result<Vec<PassageCandidate>, ApiError> {
    let started = Instant::now();
    info!(
        event = "query.passages.started",
        query_id,
        seeds = ranked.len() + excerpts.len(),
        candidate_limit,
        max_tokens = MAX_UNIT_TOKENS,
        max_units = MAX_PASSAGE_UNITS,
        "constructing passage candidates"
    );
    let mut counts = PassageBuildCounts::default();
    let outcome = untruncated_tokenizer(tokenizer).and_then(|counter| {
        build_passages_body(
            conn,
            ranked,
            excerpts,
            parse_of_unit,
            &counter,
            candidate_limit,
            &mut counts,
        )
    });
    match &outcome {
        Ok(passages) => info!(
            event = "query.passages.completed",
            query_id,
            seeds = ranked.len() + excerpts.len(),
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
            stage = "passage_construction", seeds = ranked.len() + excerpts.len(), candidate_limit,
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
    excerpts: &[ScoredExcerpt],
    parse_of_unit: &BTreeMap<String, String>,
    tokenizer: &Tokenizer,
    candidate_limit: usize,
    counts: &mut PassageBuildCounts,
) -> Result<Vec<PassageCandidate>, ApiError> {
    let mut passages: Vec<PassageCandidate> = Vec::new();
    let mut seeds: Vec<_> = ranked
        .iter()
        .map(PassageSeed::Unit)
        .chain(excerpts.iter().map(PassageSeed::Excerpt))
        .collect();
    seeds.sort_by(|left, right| {
        let (left_score, left_key) = left.score_and_key();
        let (right_score, right_key) = right.score_and_key();
        right_score
            .total_cmp(&left_score)
            .then_with(|| left_key.cmp(right_key))
    });
    for seed in seeds {
        let Some(candidate) = build_seed(conn, &seed, parse_of_unit, tokenizer)? else {
            counts.unrenderable_seeds += 1;
            continue;
        };
        if let Some(existing) = passages
            .iter_mut()
            .find(|existing| existing.covers_candidate(&candidate))
        {
            existing.retain_supported_matches(&candidate);
            counts.covered_seeds += 1;
            continue;
        }
        let overlapping: Vec<usize> = passages
            .iter()
            .enumerate()
            .filter_map(|(i, existing)| existing.overlaps(&candidate).then_some(i))
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
            // A rejected expansion can still carry match lineage wholly supported by a stronger passage.
            for &index in &overlapping {
                passages[index].retain_supported_matches(&candidate);
            }
            counts.overlap_rejections += 1;
        }
    }
    counts.candidate_limit_exclusions = passages.len().saturating_sub(candidate_limit);
    passages.truncate(candidate_limit);
    Ok(passages)
}

/// Source-unit and exact-window seeds share MaxSim ordering without losing their distinct targeting.
enum PassageSeed<'a> {
    Unit(&'a ColbertCandidateScore),
    Excerpt(&'a ScoredExcerpt),
}

impl PassageSeed<'_> {
    /// Use the actual MaxSim score and a stable key to compare both seed kinds in one ordering.
    fn score_and_key(&self) -> (f32, &str) {
        match self {
            Self::Unit(seed) => (seed.score, &seed.unit_id),
            Self::Excerpt(seed) => (seed.score, &seed.candidate_id),
        }
    }
}

/// Retain legacy unit expansion while exact-window seeds bypass all prefix selection.
fn build_seed(
    conn: &Connection,
    seed: &PassageSeed<'_>,
    parse_of_unit: &BTreeMap<String, String>,
    tokenizer: &Tokenizer,
) -> Result<Option<PassageCandidate>, ApiError> {
    match seed {
        PassageSeed::Unit(seed) => {
            let parse_id = parse_of_unit.get(&seed.unit_id).ok_or_else(|| {
                failure(format!(
                    "passage seed {} has no captured parse",
                    seed.unit_id
                ))
            })?;
            build_passage(conn, parse_id, &seed.unit_id, tokenizer).map_err(|source| {
                failure(format!(
                    "passage seed {} in {parse_id}: {source}",
                    seed.unit_id
                ))
            })
        }
        PassageSeed::Excerpt(seed) => {
            build_excerpt_passage(conn, seed, tokenizer).map_err(|source| {
                failure(format!(
                    "passage excerpt {} in {}: {source}",
                    seed.candidate_id, seed.parse_id
                ))
            })
        }
    }
}

/// Verify the immutable source window against canonical evidence before it can become cited text.
fn build_excerpt_passage(
    conn: &Connection,
    seed: &ScoredExcerpt,
    tokenizer: &Tokenizer,
) -> Result<Option<PassageCandidate>, ApiError> {
    let unit = read_unit(conn, &seed.parse_id, &seed.excerpt.unit_id)?;
    if unit.source_id != seed.source_id {
        return Err(failure(
            "exact passage seed and canonical unit have different sources",
        ));
    }
    if unit.is_furniture() {
        return Ok(None);
    }
    let Some(mut part) = unit.part() else {
        return Ok(None);
    };
    let text = slice_chars(&part.text, seed.excerpt.start_char, seed.excerpt.end_char)?;
    if text != seed.text || sha256_hex_bytes(text.as_bytes()) != seed.excerpt.text_hash {
        return Err(failure(
            "exact passage text or hash differs from its canonical source range",
        ));
    }
    if text.trim().is_empty() {
        return Ok(None);
    }
    if token_count(tokenizer, text)? > MAX_UNIT_TOKENS as usize {
        return Err(failure(
            "exact source window exceeds the passage token budget",
        ));
    }
    if seed
        .annotation_matches
        .iter()
        .any(|matched| matched.excerpt != seed.excerpt)
    {
        return Err(failure(
            "exact passage annotation attribution has a different supporting range",
        ));
    }
    // Recorded unit locators remain authoritative; character offsets cannot invent finer page positions.
    part.text = text.to_owned();
    part.start_char = seed.excerpt.start_char;
    part.end_char = seed.excerpt.end_char;
    let (section_id, section_path) = read_section(conn, &seed.parse_id, &seed.excerpt.unit_id)?;
    let parts = vec![part];
    Ok(Some(PassageCandidate {
        candidate_id: passage_candidate_id(&seed.source_id, &seed.parse_id, &parts)?,
        anchor_unit_id: seed.excerpt.unit_id.clone(),
        source_id: seed.source_id.clone(),
        parse_id: seed.parse_id.clone(),
        unit_ids: vec![seed.excerpt.unit_id.clone()],
        text: join_parts(&parts),
        section_path,
        truncated: false,
        section_id,
        parts,
        legacy_seed_ranges: Vec::new(),
        annotation_context: seed
            .annotation_context
            .iter()
            .map(|text| ScopedAnnotationContext {
                excerpt: seed.excerpt.clone(),
                text: text.clone(),
            })
            .collect(),
        graph_context: Vec::new(),
        annotation_matches: seed.annotation_matches.clone(),
    }))
}

/// Bind scoring identity to complete displayed source ranges, independently of which seed was strongest.
fn passage_candidate_id(
    source_id: &str,
    parse_id: &str,
    parts: &[PassagePart],
) -> Result<String, ApiError> {
    let excerpts: Vec<_> = parts.iter().map(PassagePart::source_excerpt).collect();
    canonical_sha256_hex_of(&(source_id, parse_id, excerpts))
        .map(|hash| format!("passage:{hash}"))
        .map_err(|source| failure(format!("hash passage candidate identity: {source}")))
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
    part.end_char = part.start_char + text.chars().count();
    part.text = text;
    let legacy_seed_range = part.source_excerpt();
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
        candidate_id: passage_candidate_id(&unit.source_id, parse_id, &parts)?,
        anchor_unit_id: anchor.to_string(),
        source_id: unit.source_id,
        parse_id: parse_id.to_string(),
        unit_ids: parts.iter().map(|p| p.id.clone()).collect(),
        text: join_parts(&parts),
        section_id,
        section_path,
        truncated,
        parts,
        legacy_seed_ranges: vec![legacy_seed_range],
        annotation_context: Vec::new(),
        graph_context: Vec::new(),
        annotation_matches: Vec::new(),
    }))
}

/// Stitch two overlapping ordered windows without sorting opaque unit IDs.
/// A failed trial leaves the destination untouched and preserves its stronger seed.
fn merge_passage(
    destination: &mut PassageCandidate,
    other: &PassageCandidate,
    tokenizer: &Tokenizer,
) -> Result<bool, ApiError> {
    if destination.source_id != other.source_id
        || destination.parse_id != other.parse_id
        || destination.section_id != other.section_id
    {
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
        if let Some(existing) = combined.get_mut(position) {
            if existing.id != part.id {
                return Err(failure("inconsistent passage reading order"));
            }
            let Some(union) = merge_part(existing, part)? else {
                return Ok(false);
            };
            *existing = union;
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
    let candidate_id =
        passage_candidate_id(&destination.source_id, &destination.parse_id, &combined)?;
    destination.unit_ids = combined.iter().map(|p| p.id.clone()).collect();
    destination.candidate_id = candidate_id;
    destination.text = text;
    destination.parts = combined;
    destination.truncated |= other.truncated;
    destination.retain_supported_matches(other);
    Ok(true)
}

/// Union overlapping slices of one canonical unit without introducing text from an unobserved gap.
fn merge_part(left: &PassagePart, right: &PassagePart) -> Result<Option<PassagePart>, ApiError> {
    if left.id != right.id || left.start_char >= right.end_char || right.start_char >= left.end_char
    {
        return Ok(None);
    }
    let (earlier, later) = if left.start_char <= right.start_char {
        (left, right)
    } else {
        (right, left)
    };
    let overlap_end = earlier.end_char.min(later.end_char);
    let overlap = slice_chars(
        &earlier.text,
        later.start_char - earlier.start_char,
        overlap_end - earlier.start_char,
    )?;
    let other_overlap = slice_chars(&later.text, 0, overlap_end - later.start_char)?;
    if overlap != other_overlap {
        return Err(failure(format!(
            "overlapping passage source bytes disagree for unit {}",
            left.id
        )));
    }
    let mut text = earlier.text.clone();
    if later.end_char > earlier.end_char {
        text.push_str(slice_chars(
            &later.text,
            earlier.end_char - later.start_char,
            later.end_char - later.start_char,
        )?);
    }
    Ok(Some(PassagePart {
        id: earlier.id.clone(),
        start_char: earlier.start_char,
        end_char: earlier.end_char.max(later.end_char),
        text,
        pages: earlier
            .pages
            .iter()
            .chain(&later.pages)
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    }))
}

/// Preserve paragraph separation when assembling verbatim source contributions.
fn join_parts(parts: &[PassagePart]) -> String {
    parts
        .iter()
        .map(|p| p.text.as_str())
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Disable hidden tokenizer limits once so passage and graph-context accounting cannot conceal oversized text.
fn untruncated_tokenizer(tokenizer: &Tokenizer) -> Result<Tokenizer, ApiError> {
    let mut counter = tokenizer.clone();
    counter
        .with_truncation(None)
        .map_err(|source| failure(format!("disable passage tokenizer truncation: {source}")))?;
    counter.with_padding(None);
    Ok(counter)
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
