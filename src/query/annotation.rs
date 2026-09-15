//! Semantic annotation discovery, grouped fusion, and bounded source-window scoring.
//! Candidates are keyed by the cohort's context window; a candidate displays
//! the best-scoring ColBERT partition of that window's canonical text.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};

use crate::sqlite::Connection;
use serde::Serialize;
use tracing::{error, info};

use crate::{
    artifact_store::ArtifactStore,
    canonical::sha256_hex_bytes,
    error::ApiError,
    inference::{InferenceRuntime, PreparedColbertQuery},
    primitives::fusion::fuse_ranked_lists,
    projections::{annotation, annotation_io, graph::normalize_entity_name},
    state::{ExclusiveGate, acquire_model_call_gate_on},
};

use super::{
    channels::{CapturedParse, ChannelCandidates},
    model::{RetrievalHit, RetrievalHitType},
    profile::RetrievalProfile,
    provenance::{AnnotationMatch, AnnotationRepresentation, RetrievalChannel, SourceExcerpt},
};

/// Shared immutable lineage keeps a combined annotation ID list from being copied per source window.
#[derive(Debug, Clone)]
struct MatchPointer {
    projection_id: String,
    manifest_uri: String,
    representation_id: String,
    representation: AnnotationRepresentation,
    annotation_ids: Vec<String>,
    exact_annotation_range: bool,
}

/// One bounded dense nomination of a cohort window, displayed through the
/// source partition `excerpt`. Text and matrices remain in verified artifact files.
#[derive(Debug, Clone)]
struct WindowMatch {
    key: String,
    source_id: String,
    parse_id: String,
    excerpt: SourceExcerpt,
    source_representation_id: String,
    matched: Arc<MatchPointer>,
    score: f32,
    source_score: f32,
    entity_name: Option<String>,
}

/// One parse's context-window membership, read once per query from the
/// section-dense reference captured in the query transaction.
#[derive(Default)]
struct WindowIndex {
    /// Member chunk ids per window id, for checking a cohort's window still exists unchanged.
    chunk_ids_by_window: BTreeMap<String, Vec<String>>,
    /// The single window citing a unit; `None` when the unit is split across windows.
    window_of_unit: BTreeMap<String, Option<String>>,
}

/// Per-representation lists are independently capped before their ranks are grouped.
pub(crate) struct AnnotationScan {
    lists: BTreeMap<AnnotationRepresentation, Vec<WindowMatch>>,
    pub(crate) semantic_names: Vec<(String, String)>,
    /// Membership of every parse that has annotation publications, keyed by parse id.
    windows: BTreeMap<String, WindowIndex>,
}

/// The scoring handoff keeps file references only for candidates admitted by final fusion.
pub(crate) struct AnnotationFusion {
    pub(crate) pool: Vec<RetrievalHit>,
    pub(crate) channel_hits: Vec<RetrievalHit>,
    selected: BTreeMap<String, SelectedWindow>,
}

/// Source scoring is required even when only a lexical/graph nomination survives fusion.
struct SelectedWindow {
    source: WindowMatch,
    matches: Vec<WindowMatch>,
}

/// A scored, exact canonical excerpt plus the annotation context used to evaluate it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ScoredExcerpt {
    pub(crate) candidate_id: String,
    pub(crate) source_id: String,
    pub(crate) parse_id: String,
    pub(crate) excerpt: SourceExcerpt,
    pub(crate) text: String,
    /// Best MaxSim across the source and its matched annotation representations, never their sum.
    pub(crate) score: f32,
    pub(crate) source_score: f32,
    #[serde(skip)]
    pub(crate) annotation_context: Vec<String>,
    pub(crate) annotation_matches: Vec<AnnotationMatch>,
}

/// Search every eligible published input while retaining only distinct bounded window nominations.
pub(crate) fn scan(
    conn: &Connection,
    store: &ArtifactStore,
    parses: &[CapturedParse],
    runtime: &InferenceRuntime,
    query: &[f32],
    profile: &RetrievalProfile,
    query_id: &str,
) -> Result<AnnotationScan, ApiError> {
    let started = Instant::now();
    let limit = profile.limits.max_candidates_per_channel as usize;
    let mut lists: BTreeMap<AnnotationRepresentation, Vec<WindowMatch>> = BTreeMap::new();
    let mut publications = 0_usize;
    let mut vectors = 0_usize;
    let mut incompatible = 0_usize;
    let mut stale = 0_usize;
    info!(
        event = "query.annotation.started",
        query_id,
        parse_count = parses.len(),
        limit,
        "searching published annotation and source representations"
    );
    let result = (|| {
        let mut windows: BTreeMap<String, WindowIndex> = BTreeMap::new();
        for parse in parses {
            let published =
                annotation::published_for_parse(conn, &parse.source_id, &parse.parse_id)?;
            if published.is_empty() {
                continue;
            }
            let index = window_index(conn, store, parse)?;
            for publication in published {
                let manifest = annotation::read_manifest(store, &publication.payload_uri)?;
                annotation::validate_publication_lineage(&publication, &manifest.plan)?;
                if manifest.plan.source_id != parse.source_id
                    || manifest.plan.parse_id != parse.parse_id
                    || manifest.plan.cohort_id != publication.cohort_id
                    || manifest.plan.input_hash != publication.input_hash
                {
                    return Err(failure(
                        "annotation envelope and manifest identities differ",
                    ));
                }
                publications += 1;
                if manifest.plan.model_identity != runtime.embedding_identity {
                    incompatible += 1;
                    continue;
                }
                let Some(inputs) = annotation::current_inputs(conn, &manifest.plan)? else {
                    stale += 1;
                    continue;
                };
                // The cohort window must still exist with the same members in
                // the captured section plane; a rebuilt plane can reuse an id.
                let target = &manifest.plan.target;
                if index.chunk_ids_by_window.get(&target.window_id) != Some(&target.chunk_ids) {
                    stale += 1;
                    continue;
                }
                let mut best: BTreeMap<AnnotationRepresentation, (f32, usize)> = BTreeMap::new();
                for (index, representation) in manifest.representations.iter().enumerate() {
                    if representation.input.representation == AnnotationRepresentation::Source {
                        continue;
                    }
                    let score = annotation_io::cosine(store, &representation.dense, query)?;
                    vectors += 1;
                    let entry = best
                        .entry(representation.input.representation)
                        .or_insert((score, index));
                    if score.total_cmp(&entry.0).is_gt()
                        || (score.to_bits() == entry.0.to_bits()
                            && representation.input.id < manifest.representations[entry.1].input.id)
                    {
                        *entry = (score, index);
                    }
                }
                let pointers: Vec<_> = best
                    .into_iter()
                    .map(|(kind, (score, index))| {
                        let input = &manifest.representations[index].input;
                        let entity_name = if kind == AnnotationRepresentation::Entity {
                            inputs
                                .iter()
                                .find(|annotation| input.annotation_ids.contains(&annotation.id))
                                .and_then(|annotation| annotation.body.get("name"))
                                .and_then(serde_json::Value::as_str)
                                .map(normalize_entity_name)
                        } else {
                            None
                        };
                        (
                            kind,
                            score,
                            entity_name,
                            Arc::new(MatchPointer {
                                projection_id: publication.projection_id.clone(),
                                manifest_uri: publication.payload_uri.clone(),
                                representation_id: input.id.clone(),
                                representation: kind,
                                annotation_ids: input.annotation_ids.clone(),
                                exact_annotation_range: inputs
                                    .iter()
                                    .filter(|annotation| {
                                        input.annotation_ids.contains(&annotation.id)
                                    })
                                    .all(annotation::has_exact_ranges),
                            }),
                        )
                    })
                    .collect();
                for representation in &manifest.representations {
                    let Some(excerpt) = &representation.input.source_excerpt else {
                        continue;
                    };
                    let source_score = annotation_io::cosine(store, &representation.dense, query)?;
                    vectors += 1;
                    let key = window_key(&parse.parse_id, &target.window_id);
                    let source = WindowMatch {
                        key,
                        source_id: parse.source_id.clone(),
                        parse_id: parse.parse_id.clone(),
                        excerpt: excerpt.clone(),
                        source_representation_id: representation.input.id.clone(),
                        matched: Arc::new(MatchPointer {
                            projection_id: publication.projection_id.clone(),
                            manifest_uri: publication.payload_uri.clone(),
                            representation_id: representation.input.id.clone(),
                            representation: AnnotationRepresentation::Source,
                            annotation_ids: Vec::new(),
                            // A source partition is always an exact canonical range.
                            exact_annotation_range: true,
                        }),
                        score: source_score,
                        source_score,
                        entity_name: None,
                    };
                    retain_match(
                        lists.entry(AnnotationRepresentation::Source).or_default(),
                        source.clone(),
                        limit,
                    );
                    for (kind, score, entity_name, pointer) in &pointers {
                        let mut nomination = source.clone();
                        nomination.score = *score;
                        nomination.matched = Arc::clone(pointer);
                        nomination.entity_name = entity_name.clone();
                        retain_match(lists.entry(*kind).or_default(), nomination, limit);
                    }
                }
            }
            windows.insert(parse.parse_id.clone(), index);
        }
        let semantic_names = lists
            .get(&AnnotationRepresentation::Entity)
            .into_iter()
            .flatten()
            .filter_map(|hit| {
                hit.entity_name
                    .as_ref()
                    .map(|name| (hit.parse_id.clone(), name.clone()))
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Ok(AnnotationScan {
            lists,
            semantic_names,
            windows,
        })
    })();
    match &result {
        Ok(scan) => info!(
            event = "query.annotation.completed",
            query_id,
            publications,
            vectors_scored = vectors,
            incompatible_publications = incompatible,
            stale_publications = stale,
            retained_matches = scan.lists.values().map(Vec::len).sum::<usize>(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "annotation discovery completed"
        ),
        Err(source) => error!(event = "query.annotation.failed", query_id, error = %source,
            elapsed_ms = started.elapsed().as_millis() as u64, "annotation discovery failed"),
    }
    result
}

/// Own final fusion diagnostics after every source and annotation shortlist is available.
pub(crate) fn fuse(
    scan: AnnotationScan,
    source: ChannelCandidates,
    profile: &RetrievalProfile,
    query_id: &str,
) -> Result<AnnotationFusion, ApiError> {
    let started = Instant::now();
    info!(
        event = "query.fusion.started",
        query_id,
        source_hits = source.channel_hits.len(),
        representation_matches = scan.lists.values().map(Vec::len).sum::<usize>(),
        candidate_limit = profile.limits.colbert_candidate_pool_size,
        channel_limit = profile.limits.max_candidates_per_channel,
        rrf_k = profile.limits.rrf_k,
        "fusing source dense, lexical, and grouped annotation rankings"
    );
    let result = fuse_candidates(scan, source, profile);
    match &result {
        Ok(fusion) => info!(
            event = "query.fusion.completed",
            query_id,
            fused_hits = fusion.pool.len(),
            channel_hits = fusion.channel_hits.len(),
            exact_excerpts = fusion
                .pool
                .iter()
                .filter(|hit| hit.source_excerpt.is_some())
                .count(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "three-contribution rank fusion completed"
        ),
        Err(source) => error!(event = "query.fusion.failed", query_id, error = %source,
            elapsed_ms = started.elapsed().as_millis() as u64, "rank fusion failed"),
    }
    result
}

/// Combine graph and semantic annotation lists into one vote before the three-way fusion.
fn fuse_candidates(
    scan: AnnotationScan,
    source: ChannelCandidates,
    profile: &RetrievalProfile,
) -> Result<AnnotationFusion, ApiError> {
    let pool_limit = profile.limits.colbert_candidate_pool_size as usize;
    let channel_limit = profile.limits.max_candidates_per_channel as usize;
    let mut records: BTreeMap<String, RetrievalHit> = BTreeMap::new();
    let mut pointers: BTreeMap<String, Vec<WindowMatch>> = BTreeMap::new();
    let mut representation_lists = Vec::new();
    let mut source_windows = Vec::new();
    for (kind, matches) in scan.lists {
        let mut keys = Vec::new();
        for matched in matches {
            let key = matched.key.clone();
            keys.push(key.clone());
            records
                .entry(key.clone())
                .or_insert_with(|| window_hit(&matched));
            pointers.entry(key).or_default().push(matched);
        }
        if kind == AnnotationRepresentation::Source {
            source_windows = keys;
        } else {
            representation_lists.push(keys);
        }
    }
    // Consolidation rule: a source channel hit on unit `u` identifies the same
    // evidence as the annotation window whose member chunks hold every chunk
    // of `u`, that is, the single window citing `u` in the parse's membership.
    // A unit split across windows stays a distinct unit candidate; it must not
    // inherit a window's match for text the window only partly holds. Only
    // windows nominated by annotation retrieval are consolidation targets.
    let mut dense = Vec::new();
    let mut lexical = Vec::new();
    let mut graph = Vec::new();
    let mut base_hits = Vec::new();
    for mut hit in source.channel_hits {
        let key = scan
            .windows
            .get(&hit.parse_id)
            .and_then(|index| index.window_of_unit.get(&hit.hit_id))
            .and_then(|window| window.as_deref())
            .map(|window_id| window_key(&hit.parse_id, window_id))
            .filter(|key| records.contains_key(key))
            .unwrap_or_else(|| unit_key(&hit.parse_id, &hit.hit_id));
        if let Some(existing) = records.get(&key) {
            // The hit adopts the window's identity so later same-target joins
            // and passage attribution see one candidate, not a unit and a window.
            hit.hit_id = existing.hit_id.clone();
            hit.unit_ids = existing.unit_ids.clone();
            hit.source_excerpt = existing.source_excerpt.clone();
        }
        match hit.channel {
            RetrievalChannel::Dense => dense.push(key.clone()),
            RetrievalChannel::Lexical => lexical.push(key.clone()),
            RetrievalChannel::Graph => graph.push(key.clone()),
            RetrievalChannel::Semantic => {
                return Err(failure(
                    "source channel unexpectedly emitted semantic annotation hits",
                ));
            }
        }
        merge_record(&mut records, &key, &hit);
        base_hits.push((key, hit));
    }
    // Multiple representations share one discovery channel's budget. The larger
    // final pool cannot expand either dense or semantic channel admission.
    let semantic = fuse_ranked_lists(&representation_lists, channel_limit, profile.limits.rrf_k);
    let semantic_keys: Vec<String> = semantic.iter().map(|(key, _)| key.clone()).collect();
    let dense = fuse_ranked_lists(
        &[dense, source_windows],
        channel_limit,
        profile.limits.rrf_k,
    );
    // Graph and semantic are separate discoveries but one final fusion contribution.
    let annotation = fuse_ranked_lists(
        &[graph.clone(), semantic_keys.clone()],
        pool_limit,
        profile.limits.rrf_k,
    );
    let final_lists = vec![
        dense.iter().map(|(key, _)| key.clone()).collect(),
        lexical.clone(),
        annotation.iter().map(|(key, _)| key.clone()).collect(),
    ];
    let fused = fuse_ranked_lists(&final_lists, pool_limit, profile.limits.rrf_k);
    let mut channel_hits = Vec::new();
    for (index, (key, score)) in dense.iter().enumerate() {
        let mut hit = records[key].clone();
        hit.channel = RetrievalChannel::Dense;
        hit.score = *score;
        hit.rank = Some((index + 1) as u32);
        hit.annotation_matches =
            pointer_matches(pointers.get(key), Some(AnnotationRepresentation::Source));
        // Shared target records are only an identity join. A dense nomination
        // cannot inherit annotation or graph evidence from another mechanism.
        hit.graph_matches.clear();
        hit.matched_annotation_id = None;
        hit.matched_projection_id = hit
            .annotation_matches
            .first()
            .map(|matched| matched.projection_id.clone())
            .or_else(|| {
                base_hits
                    .iter()
                    .find(|(base_key, base)| {
                        base_key == key && base.channel == RetrievalChannel::Dense
                    })
                    .and_then(|(_, base)| base.matched_projection_id.clone())
            });
        hit.explanation = None;
        channel_hits.push(hit);
    }
    channel_hits.extend(
        base_hits
            .iter()
            .filter(|(_, hit)| {
                matches!(
                    hit.channel,
                    RetrievalChannel::Lexical | RetrievalChannel::Graph
                )
            })
            .map(|(_, hit)| hit.clone()),
    );
    for (index, (key, score)) in semantic.iter().enumerate() {
        let mut hit = records[key].clone();
        hit.channel = RetrievalChannel::Semantic;
        hit.score = *score;
        hit.rank = Some((index + 1) as u32);
        hit.annotation_matches = pointer_matches(pointers.get(key), None);
        hit.graph_matches.clear();
        hit.dense_matches.clear();
        hit.matched_projection_id = hit
            .annotation_matches
            .first()
            .map(|matched| matched.projection_id.clone());
        hit.matched_annotation_id = hit
            .annotation_matches
            .iter()
            .flat_map(|matched| &matched.annotation_ids)
            .next()
            .cloned();
        hit.explanation = None;
        channel_hits.push(hit);
    }
    let mut selected = BTreeMap::new();
    let mut pool = Vec::new();
    let dense_keys: BTreeSet<_> = dense.iter().map(|(key, _)| key.as_str()).collect();
    let semantic_keys: BTreeSet<_> = semantic.iter().map(|(key, _)| key.as_str()).collect();
    for (index, (key, score)) in fused.into_iter().enumerate() {
        let mut hit = records
            .remove(&key)
            .ok_or_else(|| failure("fused candidate has no source record"))?;
        hit.score = score;
        hit.rank = Some((index + 1) as u32);
        let retained: Vec<_> = channel_hits
            .iter()
            .filter(|item| same_target(item, &hit))
            .collect();
        // Rebuild the final union after mechanism caps; intermediate records may
        // contain discoveries that were excluded from a retained channel list.
        hit.annotation_matches = retained
            .iter()
            .flat_map(|item| item.annotation_matches.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        hit.graph_matches = retained
            .iter()
            .flat_map(|item| item.graph_matches.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        hit.dense_matches = retained
            .iter()
            .flat_map(|item| item.dense_matches.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let origin = retained
            .first()
            .ok_or_else(|| failure("fused candidate has no retained discovery mechanism"))?;
        hit.channel = origin.channel;
        hit.matched_projection_id = origin.matched_projection_id.clone();
        hit.matched_annotation_id = origin.matched_annotation_id.clone();
        hit.explanation = origin.explanation.clone();
        // Attribution and annotation scoring follow retained mechanism lists,
        // not intermediate representation matches excluded by their own cap.
        if let Some(mut matches) = pointers.remove(&key)
            && let Some(source) = matches.first().cloned()
        {
            matches.retain(|matched| {
                if matched.matched.representation == AnnotationRepresentation::Source {
                    dense_keys.contains(key.as_str())
                } else {
                    semantic_keys.contains(key.as_str())
                }
            });
            selected.insert(key, SelectedWindow { source, matches });
        }
        pool.push(hit);
    }
    Ok(AnnotationFusion {
        pool,
        channel_hits,
        selected,
    })
}

/// Score only admitted exact windows, loading one bounded cohort/matrix at a time.
pub(crate) fn score_excerpts(
    store: &ArtifactStore,
    fusion: &AnnotationFusion,
    runtime: &InferenceRuntime,
    query: &PreparedColbertQuery,
    gate: &Arc<ExclusiveGate>,
    query_id: &str,
) -> Result<Vec<ScoredExcerpt>, ApiError> {
    let started = Instant::now();
    info!(
        event = "query.annotation_maxsim.started",
        query_id,
        candidates = fusion.selected.len(),
        "scoring annotation and source windows"
    );
    let result = (|| {
        let mut output = Vec::new();
        for (key, selected) in &fusion.selected {
            let first = &selected.source;
            // The displayed text is the archived partition text; the passage
            // builder verifies it against canonical units under its snapshot.
            let mut text = None;
            let mut score = f32::NEG_INFINITY;
            let mut source_score = f32::NEG_INFINITY;
            let mut context = BTreeSet::new();
            let mut matches = BTreeSet::new();
            let mut scored = BTreeSet::new();
            for (pointer, attributed) in std::iter::once((first, false))
                .chain(selected.matches.iter().map(|pointer| (pointer, true)))
            {
                let manifest = annotation::read_manifest(store, &pointer.matched.manifest_uri)?;
                if manifest.plan.model_identity != runtime.embedding_identity {
                    return Err(failure("annotation model identity changed within a query"));
                }
                for (id, is_source) in [
                    (&pointer.source_representation_id, true),
                    (&pointer.matched.representation_id, false),
                ] {
                    if !is_source && !attributed {
                        continue;
                    }
                    if !scored.insert((pointer.matched.projection_id.clone(), id.clone())) {
                        continue;
                    }
                    let representation = manifest
                        .representations
                        .iter()
                        .find(|item| item.input.id == *id)
                        .ok_or_else(|| {
                            failure(
                                "retained annotation representation is missing from its manifest",
                            )
                        })?;
                    if is_source && text.is_none() {
                        if sha256_hex_bytes(representation.input.text.as_bytes())
                            != first.excerpt.text_hash
                        {
                            return Err(failure(
                                "retrieved source window differs from its archived text",
                            ));
                        }
                        text = Some(representation.input.text.clone());
                    }
                    let values = annotation_io::load_embedding(store, &representation.colbert)?;
                    // Storage I/O is finished before a local accelerator permit is taken.
                    let permit = if runtime.colbert.uses_local_model_gate() {
                        Some(acquire_model_call_gate_on(
                            gate,
                            query_id,
                            "colbert",
                            "annotation_scoring",
                        )?)
                    } else {
                        None
                    };
                    let value = runtime.colbert.score_matrix(
                        query,
                        &values,
                        representation.colbert.rows,
                        representation.colbert.dimension,
                    )?;
                    drop(permit);
                    score = score.max(value);
                    if is_source
                        || representation.input.representation == AnnotationRepresentation::Source
                    {
                        source_score = source_score.max(value);
                    } else {
                        context.insert(representation.input.text.clone());
                    }
                }
                if attributed {
                    matches.insert(match_provenance(pointer));
                }
            }
            if !score.is_finite() || !source_score.is_finite() {
                return Err(failure("annotation candidate has no finite source score"));
            }
            let text =
                text.ok_or_else(|| failure("annotation candidate scored no source partition"))?;
            output.push(ScoredExcerpt {
                candidate_id: key.clone(),
                source_id: first.source_id.clone(),
                parse_id: first.parse_id.clone(),
                excerpt: first.excerpt.clone(),
                text,
                score,
                source_score,
                annotation_context: context.into_iter().collect(),
                annotation_matches: matches.into_iter().collect(),
            });
        }
        output.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| right.source_score.total_cmp(&left.source_score))
                .then_with(|| left.candidate_id.cmp(&right.candidate_id))
        });
        Ok(output)
    })();
    match &result {
        Ok(scores) => info!(
            event = "query.annotation_maxsim.completed",
            query_id,
            scored = scores.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "annotation MaxSim completed"
        ),
        Err(source) => error!(event = "query.annotation_maxsim.failed", query_id, error = %source,
            elapsed_ms = started.elapsed().as_millis() as u64, "annotation MaxSim failed"),
    }
    result
}

/// Retain at most one nomination of an excerpt in a representation's ranked list.
fn retain_match(list: &mut Vec<WindowMatch>, candidate: WindowMatch, limit: usize) {
    if let Some(index) = list.iter().position(|item| item.key == candidate.key) {
        if match_order(&candidate, &list[index]).is_lt() {
            list[index] = candidate;
        }
    } else {
        list.push(candidate);
    }
    list.sort_by(match_order);
    list.truncate(limit);
}

/// Source similarity only breaks equal annotation scores, without adding another annotation vote.
fn match_order(left: &WindowMatch, right: &WindowMatch) -> std::cmp::Ordering {
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| right.source_score.total_cmp(&left.source_score))
        .then_with(|| left.key.cmp(&right.key))
}

/// Unit ids an excerpt cites, in first-occurrence order without duplicates:
/// the rule every grain derives its unit ids by (`chunk::ordered_unit_ids`).
pub(crate) fn excerpt_unit_ids(excerpt: &SourceExcerpt) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    for fragment in &excerpt.fragments {
        if !ids.iter().any(|id| id == &fragment.unit_id) {
            ids.push(fragment.unit_id.clone());
        }
    }
    ids
}

/// Whether any fragment of the excerpt cites the unit.
pub(crate) fn excerpt_cites_unit(excerpt: &SourceExcerpt, unit_id: &str) -> bool {
    excerpt
        .fragments
        .iter()
        .any(|fragment| fragment.unit_id == unit_id)
}

/// Address a cohort window independently of which annotation representation found it.
fn window_key(parse_id: &str, window_id: &str) -> String {
    format!("window:{parse_id}:{window_id}")
}

/// Whole-unit candidates remain distinct from window excerpts until passage overlap resolution.
fn unit_key(parse_id: &str, unit_id: &str) -> String {
    format!("unit:{parse_id}:{unit_id}")
}

/// Read one parse's window membership from its captured section reference.
/// Retains chunk ids and unit-to-window membership only; vectors and text are
/// streamed past. The dense plane is required for every captured parse.
fn window_index(
    conn: &Connection,
    store: &ArtifactStore,
    parse: &CapturedParse,
) -> Result<WindowIndex, ApiError> {
    let plane = parse.dense_plane.as_ref().ok_or_else(|| {
        failure(format!(
            "captured parse {} has no dense plane for annotation windows",
            parse.parse_id
        ))
    })?;
    let mut index = WindowIndex::default();
    plane.visit_sections(conn, store, |window| {
        for unit_id in &window.input_unit_ids {
            index
                .window_of_unit
                .entry(unit_id.clone())
                .and_modify(|owner| *owner = None)
                .or_insert_with(|| Some(window.window_id.clone()));
        }
        // The visitor lends each streamed window; the index outlives the stream.
        index
            .chunk_ids_by_window
            .insert(window.window_id.clone(), window.chunk_ids.clone());
        Ok(())
    })?;
    Ok(index)
}

/// The hit anchors on the excerpt's first cited unit and resolves to every
/// cited unit, with the exact source target recorded separately.
fn window_hit(matched: &WindowMatch) -> RetrievalHit {
    let unit_ids = excerpt_unit_ids(&matched.excerpt);
    RetrievalHit {
        hit_type: RetrievalHitType::ContentUnit,
        hit_id: unit_ids.first().cloned().unwrap_or_default(),
        source_id: matched.source_id.clone(),
        parse_id: matched.parse_id.clone(),
        unit_ids,
        channel: if matched.matched.representation == AnnotationRepresentation::Source {
            RetrievalChannel::Dense
        } else {
            RetrievalChannel::Semantic
        },
        score: f64::from(matched.score),
        rank: None,
        matched_projection_id: Some(matched.matched.projection_id.clone()),
        matched_annotation_id: matched.matched.annotation_ids.first().cloned(),
        explanation: None,
        graph_matches: Vec::new(),
        dense_matches: Vec::new(),
        source_excerpt: Some(matched.excerpt.clone()),
        annotation_matches: Vec::new(),
    }
}

/// Union source channel attribution when whole-unit and full-window identities coincide.
fn merge_record(records: &mut BTreeMap<String, RetrievalHit>, key: &str, hit: &RetrievalHit) {
    let record = records.entry(key.to_owned()).or_insert_with(|| hit.clone());
    record
        .graph_matches
        .extend(hit.graph_matches.iter().cloned());
    record.graph_matches.sort();
    record.graph_matches.dedup();
    record
        .dense_matches
        .extend(hit.dense_matches.iter().cloned());
    record.dense_matches.sort();
    record.dense_matches.dedup();
    if hit.channel == RetrievalChannel::Dense {
        record.channel = RetrievalChannel::Dense;
    }
}

/// Keep actual mechanism attribution even though semantic and graph ranks share one outer group.
fn pointer_matches(
    pointers: Option<&Vec<WindowMatch>>,
    only: Option<AnnotationRepresentation>,
) -> Vec<AnnotationMatch> {
    pointers
        .into_iter()
        .flatten()
        .filter(|matched| match only {
            Some(kind) => matched.matched.representation == kind,
            None => matched.matched.representation != AnnotationRepresentation::Source,
        })
        .map(match_provenance)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Link a scored model representation to its exact source window and original annotations.
fn match_provenance(matched: &WindowMatch) -> AnnotationMatch {
    AnnotationMatch {
        projection_id: matched.matched.projection_id.clone(),
        representation_id: matched.matched.representation_id.clone(),
        representation: matched.matched.representation,
        annotation_ids: matched.matched.annotation_ids.clone(),
        excerpt: matched.excerpt.clone(),
        exact_annotation_range: matched.matched.exact_annotation_range,
    }
}

/// Distinct offsets in one canonical unit must not acquire each other's channel membership.
fn same_target(left: &RetrievalHit, right: &RetrievalHit) -> bool {
    left.parse_id == right.parse_id
        && left.hit_id == right.hit_id
        && left.source_excerpt == right.source_excerpt
}

/// Preserve source context through the existing query error envelope.
fn failure(message: impl Into<String>) -> ApiError {
    ApiError::StorageOperation {
        message: message.into(),
    }
}
