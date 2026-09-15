//! Query-time passages built from scored ColBERT windows and exact excerpts in
//! the caller's read snapshot. Both seed kinds carry fragments (PLAN-grains
//! Section 2), so every passage is sliced from canonical units by the same
//! rule; the final reranker receives the bounded canonical text, headings, and
//! separately labeled matched annotations.

use std::collections::BTreeSet;
use std::time::Instant;

use crate::sqlite::Connection;
use rusqlite::{OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;
use tokenizers::Tokenizer;
use tracing::{error, info};

use crate::assembly::evidence::evidence_text;
use crate::canonical::{canonical_sha256_hex_of, sha256_hex_bytes};
use crate::error::ApiError;
use crate::limits::RetrievalLimits;
use crate::model::{ContentType, SourceLocationStatus};
use crate::projections::annotation::slice_chars;
use crate::query::annotation::{ScoredExcerpt, excerpt_cites_unit, excerpt_unit_ids};
use crate::query::model::RetrievalHit;
use crate::query::provenance::{
    AnnotationContribution, AnnotationMatch, AnnotationRepresentation, GraphReach,
    RetrievalChannel, RetrievalProvenance, SourceExcerpt, SourceFragment, UnitRetrievalMatch,
};
use crate::query::rerank::ColbertWindowScore;
use crate::sections::read_section;

// Bound cells before bringing them into Rust. Oversized authoritative data is
// an explicit error, never a silently shortened raw evidence record.
const UNIT_SQL: &str = "
SELECT source_id, content_type,
       CASE WHEN length(CAST(body_json AS BLOB)) <= ?3 THEN body_json END
FROM content_units WHERE parse_id = ?1 AND id = ?2";
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
    pub(crate) score: f64,
    /// A ColBERT window seed was clipped by the passage budget; exact retrieved windows remain complete.
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
}

impl PassagePart {
    /// Hash the displayed canonical bytes, including the exact range left by any budget clip.
    fn source_excerpt(&self) -> SourceExcerpt {
        SourceExcerpt {
            fragments: vec![SourceFragment {
                unit_id: self.id.clone(),
                start_char: self.start_char,
                end_char: self.end_char,
            }],
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
    // Window seeds attach unit hits only to the ranges actually displayed, never another slice of that unit.
    #[serde(skip)]
    seed_ranges: Vec<SourceExcerpt>,
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
            text.push_str("\n\nMatched graph context (whole-unit targeting):\n");
            text.push_str(&self.graph_context.join("\n"));
        }
        text
    }

    /// Expose the source slices actually rendered, never the broader annotation extraction target.
    pub(crate) fn source_excerpts(&self) -> Vec<SourceExcerpt> {
        self.parts.iter().map(PassagePart::source_excerpt).collect()
    }

    /// Include every retained graph path supporting the passage; the reranker enforces its total input capacity.
    pub(crate) fn attach_graph_context(&mut self, channel_hits: &[RetrievalHit]) {
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
                // A component-specific cutoff would hide expensive annotations from final scoring.
                context.push(text);
            }
        }
        self.graph_context = context;
    }

    /// Coordinate containment of every fragment prevents a later match in the
    /// same unit from attaching to its prefix; an excerpt is covered only whole.
    fn covers_excerpt(&self, excerpt: &SourceExcerpt) -> bool {
        !excerpt.fragments.is_empty()
            && excerpt.fragments.iter().all(|fragment| {
                self.parts.iter().any(|part| {
                    part.id == fragment.unit_id
                        && part.start_char <= fragment.start_char
                        && fragment.start_char < fragment.end_char
                        && part.end_char >= fragment.end_char
                })
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

    /// Exact hits require their full source range; unit hits require a displayed window range citing the unit.
    fn contains_hit(&self, hit: &RetrievalHit, unit_id: &str) -> bool {
        if hit.source_id != self.source_id
            || hit.parse_id != self.parse_id
            || !hit.unit_ids.iter().any(|id| id == unit_id)
        {
            return false;
        }
        match &hit.source_excerpt {
            Some(excerpt) => excerpt_cites_unit(excerpt, unit_id) && self.covers_excerpt(excerpt),
            // This locates the displayed window range, not a newly invented exact annotation match.
            None => self.seed_ranges.iter().any(|excerpt| {
                excerpt_cites_unit(excerpt, unit_id) && self.covers_excerpt(excerpt)
            }),
        }
    }

    /// Covered or merged candidates transfer only match lineage whose complete support remains displayed.
    fn retain_supported_matches(&mut self, other: &Self) {
        for excerpt in &other.seed_ranges {
            if self.covers_excerpt(excerpt) && !self.seed_ranges.contains(excerpt) {
                self.seed_ranges.push(excerpt.clone());
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
        Ok(SearchResult {
            text: self.text,
            source_id: self.source_id,
            parse_id: self.parse_id,
            unit_ids: self.unit_ids,
            source_excerpts,
            source_locations,
            section_path: self.section_path,
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
                            excerpt_cites_unit(&matched.excerpt, unit_id)
                                && self.covers_excerpt(&matched.excerpt)
                        })
                        .cloned(),
                );
            }
            annotation_matches.extend(
                self.annotation_matches
                    .iter()
                    .filter(|matched| {
                        excerpt_cites_unit(&matched.excerpt, unit_id)
                            && self.covers_excerpt(&matched.excerpt)
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
}

impl Unit {
    /// Resolve the shared evidence-text contract for this unit's whole text.
    fn part(&self) -> Option<PassagePart> {
        let text = evidence_text(self.content_type, &self.body)?;
        if text.trim().is_empty() {
            return None;
        }
        Some(PassagePart {
            id: self.id.clone(),
            start_char: 0,
            end_char: text.chars().count(),
            text,
        })
    }
}

/// Build distinct passages in ColBERT seed order and retain every candidate for
/// diagnostics before the caller applies the final passage reranker.
pub(crate) fn build_passages(
    conn: &Connection,
    ranked: &[ColbertWindowScore],
    excerpts: &[ScoredExcerpt],
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
        max_tokens = conn.limits().retrieval.passage_max_tokens,
        max_units = conn.limits().retrieval.max_passage_units,
        "constructing passage candidates"
    );
    let mut counts = PassageBuildCounts::default();
    let outcome = untruncated_tokenizer(tokenizer).and_then(|counter| {
        build_passages_body(
            conn,
            ranked,
            excerpts,
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
            error_chain = %crate::util::error_chain(failure, &conn.limits().diagnostics),
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
    ranked: &[ColbertWindowScore],
    excerpts: &[ScoredExcerpt],
    tokenizer: &Tokenizer,
    candidate_limit: usize,
    counts: &mut PassageBuildCounts,
) -> Result<Vec<PassageCandidate>, ApiError> {
    let mut passages: Vec<PassageCandidate> = Vec::new();
    let mut seeds: Vec<_> = ranked
        .iter()
        .map(PassageSeed::Window)
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
        let candidates = build_seed(conn, &seed, tokenizer)?;
        if candidates.is_empty() {
            counts.unrenderable_seeds += 1;
            continue;
        }
        // A window's section groups are admitted in reading order under the
        // window's one score; each is a distinct candidate to the merge rules.
        for candidate in candidates {
            admit_candidate(
                &mut passages,
                candidate,
                tokenizer,
                &conn.limits().retrieval,
                counts,
            )?;
        }
    }
    counts.candidate_limit_exclusions = passages.len().saturating_sub(candidate_limit);
    passages.truncate(candidate_limit);
    Ok(passages)
}

/// Admit one built candidate against the passages already kept: a candidate
/// wholly covered transfers its lineage; a disjoint one is appended; an
/// overlapping one merges with every overlapping passage when the merged
/// result fits the budget, and otherwise only transfers supported lineage.
fn admit_candidate(
    passages: &mut Vec<PassageCandidate>,
    candidate: PassageCandidate,
    tokenizer: &Tokenizer,
    limits: &RetrievalLimits,
    counts: &mut PassageBuildCounts,
) -> Result<(), ApiError> {
    if let Some(existing) = passages
        .iter_mut()
        .find(|existing| existing.covers_candidate(&candidate))
    {
        existing.retain_supported_matches(&candidate);
        counts.covered_seeds += 1;
        return Ok(());
    }
    let overlapping: Vec<usize> = passages
        .iter()
        .enumerate()
        .filter_map(|(i, existing)| existing.overlaps(&candidate).then_some(i))
        .collect();
    let Some(&first) = overlapping.first() else {
        passages.push(candidate);
        return Ok(());
    };
    // Work on an owned trial so a cap failure leaves stronger candidates intact.
    let mut merged = passages[first].clone();
    let mut fits = merge_passage(&mut merged, &candidate, tokenizer, limits)?;
    for &index in overlapping.iter().skip(1) {
        if fits {
            fits = merge_passage(&mut merged, &passages[index], tokenizer, limits)?;
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
    Ok(())
}

/// ColBERT-window and exact-window seeds share MaxSim ordering without losing
/// their distinct targeting: both carry fragments, but only an exact window
/// carries archived text to verify and annotation attribution to attach.
enum PassageSeed<'a> {
    Window(&'a ColbertWindowScore),
    Excerpt(&'a ScoredExcerpt),
}

impl PassageSeed<'_> {
    /// Use the actual MaxSim score and a stable key to compare both seed kinds in one ordering.
    fn score_and_key(&self) -> (f32, &str) {
        match self {
            Self::Window(seed) => (seed.score, &seed.window_id),
            Self::Excerpt(seed) => (seed.score, &seed.candidate_id),
        }
    }
}

/// Build a seed's passages: a ColBERT window yields one per section group, an
/// exact window at most one. An empty result is an unrenderable seed.
fn build_seed(
    conn: &Connection,
    seed: &PassageSeed<'_>,
    tokenizer: &Tokenizer,
) -> Result<Vec<PassageCandidate>, ApiError> {
    match seed {
        PassageSeed::Window(seed) => {
            build_window_passages(conn, seed, tokenizer).map_err(|source| {
                failure(format!(
                    "passage window {} in {}: {source}",
                    seed.window_id, seed.parse_id
                ))
            })
        }
        PassageSeed::Excerpt(seed) => build_excerpt_passage(conn, seed, tokenizer)
            .map(|passage| passage.into_iter().collect())
            .map_err(|source| {
                failure(format!(
                    "passage excerpt {} in {}: {source}",
                    seed.candidate_id, seed.parse_id
                ))
            }),
    }
}

/// Verify the immutable source window against canonical evidence before it can
/// become cited text: every fragment is sliced from its unit under this
/// snapshot, and the archived window text must be those slices in order with
/// only whitespace between, before, or after them (the grain's join, or the
/// remnant of one at a partition boundary). Abutting fragments of one unit
/// become one part, so a unit split across member chunks is one displayed
/// range and reading-order merges see one part per unit.
fn build_excerpt_passage(
    conn: &Connection,
    seed: &ScoredExcerpt,
    tokenizer: &Tokenizer,
) -> Result<Option<PassageCandidate>, ApiError> {
    if sha256_hex_bytes(seed.text.as_bytes()) != seed.excerpt.text_hash {
        return Err(failure(
            "exact passage text differs from its archived window hash",
        ));
    }
    let mut parts: Vec<PassagePart> = Vec::new();
    let mut remaining = seed.text.as_str();
    for fragment in &seed.excerpt.fragments {
        let Some((unit_source, part)) = read_fragment_part(conn, &seed.parse_id, fragment)? else {
            return Ok(None);
        };
        if unit_source != seed.source_id {
            return Err(failure(
                "exact passage seed and canonical unit have different sources",
            ));
        }
        remaining = strip_after_whitespace(remaining, &part.text).ok_or_else(|| {
            failure(format!(
                "exact passage text differs from canonical unit {} [{}, {})",
                fragment.unit_id, fragment.start_char, fragment.end_char
            ))
        })?;
        push_part(&mut parts, part);
    }
    if !remaining.trim().is_empty() {
        return Err(failure(
            "exact passage text holds characters outside its fragments",
        ));
    }
    let Some(anchor) = parts.first().map(|part| part.id.clone()) else {
        return Ok(None);
    };
    // The archived partition was bounded by the ColBERT cap, but the displayed
    // passage re-joins parts with blank lines where the archive used tabs, so a
    // cap-filling partition can re-tokenize above `passage_max_tokens`. Clip
    // exactly as window passages do (trailing parts first, then a lone part's
    // text) and mark the passage truncated; never fail the query over display.
    let max_tokens = conn.limits().retrieval.passage_max_tokens as usize;
    let mut truncated = false;
    while parts.len() > 1 && token_count(tokenizer, &join_parts(&parts))? > max_tokens {
        parts.pop();
        truncated = true;
    }
    if let [part] = parts.as_mut_slice()
        && token_count(tokenizer, &part.text)? > max_tokens
    {
        let (text, _) = bounded_text(&part.text, tokenizer, max_tokens)?;
        part.end_char = part.start_char + text.chars().count();
        part.text = text;
        truncated = true;
    }
    let text = join_parts(&parts);
    if text.trim().is_empty() {
        return Ok(None);
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
    let (section_id, section_path) = read_section(conn, &seed.parse_id, &anchor)?;
    Ok(Some(PassageCandidate {
        candidate_id: passage_candidate_id(&seed.source_id, &seed.parse_id, &parts)?,
        anchor_unit_id: anchor,
        source_id: seed.source_id.clone(),
        parse_id: seed.parse_id.clone(),
        unit_ids: excerpt_unit_ids(&seed.excerpt),
        text,
        section_path,
        truncated,
        section_id,
        parts,
        seed_ranges: Vec::new(),
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

/// The text after `slice`, where `slice` may be preceded only by whitespace.
/// Every whitespace-prefix depth is tried so a slice that itself begins with
/// whitespace is not lost to trimming.
fn strip_after_whitespace<'a>(text: &'a str, slice: &str) -> Option<&'a str> {
    let boundaries = text
        .char_indices()
        .take_while(|(_, character)| character.is_whitespace())
        .map(|(offset, character)| offset + character.len_utf8());
    std::iter::once(0)
        .chain(boundaries)
        .find_map(|offset| text[offset..].strip_prefix(slice))
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

/// Build the passages one scored ColBERT window seeds. The window's fragments
/// are sliced from their canonical units under this snapshot (abutting
/// fragments of one unit become one part); consecutive parts in one section
/// form one passage, because the window itself never consulted section
/// boundaries; each passage is then held to `max_passage_units` and
/// `passage_max_tokens` by dropping trailing parts and, for a lone part still
/// over budget, clipping its text — either clip sets `truncated`. Every unit
/// must belong to one source. A unit without evidence text makes the window
/// unrenderable (an empty result), as for exact excerpts. Each passage's
/// seed ranges are exactly its displayed parts, so a unit hit attaches only
/// to the range shown.
fn build_window_passages(
    conn: &Connection,
    seed: &ColbertWindowScore,
    tokenizer: &Tokenizer,
) -> Result<Vec<PassageCandidate>, ApiError> {
    let mut parts: Vec<PassagePart> = Vec::new();
    let mut source_id: Option<String> = None;
    for fragment in &seed.fragments {
        let Some((unit_source, part)) = read_fragment_part(conn, &seed.parse_id, fragment)? else {
            return Ok(Vec::new());
        };
        match &source_id {
            Some(known) if *known != unit_source => {
                return Err(failure(format!(
                    "window spans sources {known} and {unit_source}"
                )));
            }
            Some(_) => {}
            None => source_id = Some(unit_source),
        }
        push_part(&mut parts, part);
    }
    let Some(source_id) = source_id else {
        return Ok(Vec::new());
    };

    // Section groups: consecutive parts whose units share a section id.
    let mut groups: Vec<(Option<String>, Vec<String>, Vec<PassagePart>)> = Vec::new();
    for part in parts {
        let (section_id, section_path) = read_section(conn, &seed.parse_id, &part.id)?;
        match groups.last_mut() {
            Some((known, _, members)) if *known == section_id => members.push(part),
            _ => groups.push((section_id, section_path, vec![part])),
        }
    }

    let limits = &conn.limits().retrieval;
    let max_tokens = limits.passage_max_tokens as usize;
    let mut passages = Vec::with_capacity(groups.len());
    for (section_id, section_path, mut parts) in groups {
        let mut truncated = false;
        if parts.len() > limits.max_passage_units {
            parts.truncate(limits.max_passage_units);
            truncated = true;
        }
        // Trailing parts go first; only a lone over-budget part is clipped
        // inside its text, keeping the displayed range a prefix of the unit range.
        while parts.len() > 1 && token_count(tokenizer, &join_parts(&parts))? > max_tokens {
            parts.pop();
            truncated = true;
        }
        if let [part] = parts.as_mut_slice()
            && token_count(tokenizer, &part.text)? > max_tokens
        {
            let (text, _) = bounded_text(&part.text, tokenizer, max_tokens)?;
            part.end_char = part.start_char + text.chars().count();
            part.text = text;
            truncated = true;
        }
        let Some(anchor) = parts.first().map(|part| part.id.clone()) else {
            continue;
        };
        let text = join_parts(&parts);
        if text.trim().is_empty() {
            continue;
        }
        passages.push(PassageCandidate {
            candidate_id: passage_candidate_id(&source_id, &seed.parse_id, &parts)?,
            anchor_unit_id: anchor,
            source_id: source_id.clone(),
            parse_id: seed.parse_id.clone(),
            unit_ids: parts.iter().map(|part| part.id.clone()).collect(),
            text,
            section_path,
            truncated,
            section_id,
            seed_ranges: parts.iter().map(PassagePart::source_excerpt).collect(),
            parts,
            annotation_context: Vec::new(),
            graph_context: Vec::new(),
            annotation_matches: Vec::new(),
        });
    }
    Ok(passages)
}

/// Slice one fragment from its canonical unit under this snapshot, returning
/// the unit's source id with the part. `None` when the unit has no evidence
/// text, which no persisted fragment should cite.
fn read_fragment_part(
    conn: &Connection,
    parse_id: &str,
    fragment: &SourceFragment,
) -> Result<Option<(String, PassagePart)>, ApiError> {
    let unit = read_unit(conn, parse_id, &fragment.unit_id)?;
    let Some(whole) = unit.part() else {
        return Ok(None);
    };
    let slice = slice_chars(&whole.text, fragment.start_char, fragment.end_char)?;
    let part = PassagePart {
        id: fragment.unit_id.clone(),
        start_char: fragment.start_char,
        end_char: fragment.end_char,
        text: slice.to_owned(),
    };
    Ok(Some((unit.source_id, part)))
}

/// Append a part, merging it into the previous one when both slice the same
/// unit and abut, so a unit split across member chunks is one displayed range
/// and reading-order merges see one part per unit.
fn push_part(parts: &mut Vec<PassagePart>, part: PassagePart) {
    match parts.last_mut() {
        Some(last) if last.id == part.id && last.end_char == part.start_char => {
            last.text.push_str(&part.text);
            last.end_char = part.end_char;
        }
        _ => parts.push(part),
    }
}

/// Stitch two overlapping ordered windows without sorting opaque unit IDs.
/// A failed trial leaves the destination untouched and preserves its stronger seed.
fn merge_passage(
    destination: &mut PassageCandidate,
    other: &PassageCandidate,
    tokenizer: &Tokenizer,
    limits: &RetrievalLimits,
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
    if combined.len() > limits.max_passage_units
        || token_count(tokenizer, &text)? > limits.passage_max_tokens as usize
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

/// Clip an oversized lone part to a prefix, retaining original UTF-8 bytes and
/// exposing truncation. Its complete canonical body still appears in evidencePack.
fn bounded_text(
    text: &str,
    tokenizer: &Tokenizer,
    max_tokens: usize,
) -> Result<(String, bool), ApiError> {
    let encoded = tokenizer
        .encode(text, true)
        .map_err(|source| failure(format!("passage tokenization failed: {source}")))?;
    if encoded.len() <= max_tokens {
        return Ok((text.to_string(), false));
    }
    let mut end = encoded
        .get_offsets()
        .iter()
        .take(max_tokens)
        .map(|(_, end)| *end)
        .max()
        .unwrap_or(0)
        .min(text.len());
    while end > 0
        && (!text.is_char_boundary(end) || token_count(tokenizer, &text[..end])? > max_tokens)
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
    let body_limit = conn.limits().resources.max_source_body_bytes;
    let row = conn
        .query_row(UNIT_SQL, params![parse_id, unit_id, body_limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .optional()
        .map_err(|source| {
            failure(format!(
                "read passage unit {unit_id} in {parse_id}: {source}"
            ))
        })?
        .ok_or_else(|| failure(format!("missing passage unit {unit_id} in {parse_id}")))?;
    let body = row.2.ok_or_else(|| {
        failure(format!(
            "resource limit: body of {unit_id} exceeds {body_limit} bytes"
        ))
    })?;
    Ok(Unit {
        id: unit_id.to_string(),
        source_id: row.0,
        content_type: serde_json::from_value(Value::String(row.1))
            .map_err(|source| failure(format!("content type of {unit_id}: {source}")))?,
        body: serde_json::from_str(&body)
            .map_err(|source| failure(format!("body of {unit_id}: {source}")))?,
    })
}

/// Preserve recorded locations and their states; an absent location is reported
/// as absent by clients rather than replaced with an invented title or path.
fn read_locations(conn: &Connection, source_id: &str) -> Result<Vec<SourceCitation>, ApiError> {
    let cell_limit = conn.limits().resources.max_json_cell_bytes;
    let location_limit = conn.limits().retrieval.max_source_locations;
    let mut statement = conn.prepare(LOCATIONS_SQL).map_err(|source| {
        failure(format!(
            "prepare source citations for {source_id}: {source}"
        ))
    })?;
    let rows = statement
        .query_map(params![source_id, cell_limit, location_limit + 1], |row| {
            Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|source| failure(format!("read source citations for {source_id}: {source}")))?;
    let mut locations = Vec::new();
    for row in rows {
        let (uri, status) =
            row.map_err(|source| failure(format!("source citation for {source_id}: {source}")))?;
        let native_uri = uri.ok_or_else(|| {
            failure(format!(
                "resource limit: source URI for {source_id} exceeds {cell_limit} bytes"
            ))
        })?;
        locations.push(SourceCitation {
            native_uri,
            status: serde_json::from_value(Value::String(status))
                .map_err(|source| failure(format!("source status for {source_id}: {source}")))?,
        });
        if locations.len() > location_limit {
            return Err(failure(format!(
                "resource limit: source {source_id} exceeds {location_limit} citation locations"
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
