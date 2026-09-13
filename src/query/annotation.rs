//! Semantic annotation discovery, grouped fusion, and bounded source-window scoring.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};

use crate::sqlite::Connection;
use rusqlite::params;
use serde::Serialize;
use tracing::{error, info};

use crate::{
    artifact_store::ArtifactStore,
    canonical::{canonical_sha256_hex_of, sha256_hex_bytes},
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

/// One bounded dense nomination. Text and matrices remain in verified artifact files.
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

/// Per-representation lists are independently capped before their ranks are grouped.
pub(crate) struct AnnotationScan {
    lists: BTreeMap<AnnotationRepresentation, Vec<WindowMatch>>,
    pub(crate) semantic_names: Vec<(String, String)>,
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
        for parse in parses {
            for publication in
                annotation::published_for_parse(conn, &parse.source_id, &parse.parse_id)?
            {
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
                if !eligible_unit(
                    conn,
                    &parse.source_id,
                    &parse.parse_id,
                    &manifest.plan.target.unit_id,
                )? {
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
                                exact_annotation_range: manifest.plan.target.range.is_some(),
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
                    let key = excerpt_key(&parse.parse_id, excerpt)?;
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
                            exact_annotation_range: manifest.plan.target.range.is_some(),
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
    conn: &Connection,
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
    let result = fuse_candidates(conn, scan, source, profile);
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
    conn: &Connection,
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
    // A whole-unit hit and an exact window covering that entire unit identify
    // the same evidence. Partial windows remain distinct; they must not inherit
    // an unrelated chunk's match merely because they share a parent unit.
    let mut whole_units = BTreeMap::new();
    for matches in pointers.values() {
        let Some(first) = matches.first() else {
            continue;
        };
        if first.excerpt.start_char != 0 {
            continue;
        }
        let text = annotation::source_text(
            conn,
            &first.source_id,
            &first.parse_id,
            &first.excerpt.unit_id,
        )?;
        if text.chars().count() == first.excerpt.end_char
            && sha256_hex_bytes(text.as_bytes()) == first.excerpt.text_hash
        {
            whole_units.insert(
                (first.parse_id.clone(), first.excerpt.unit_id.clone()),
                first.key.clone(),
            );
        }
    }
    let mut dense = Vec::new();
    let mut lexical = Vec::new();
    let mut graph = Vec::new();
    let mut base_hits = Vec::new();
    for mut hit in source.channel_hits {
        let key = whole_units
            .get(&(hit.parse_id.clone(), hit.hit_id.clone()))
            .cloned()
            .unwrap_or_else(|| unit_key(&hit.parse_id, &hit.hit_id));
        if let Some(existing) = records.get(&key) {
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
    conn: &Connection,
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
            let canonical = annotation::source_text(
                conn,
                &first.source_id,
                &first.parse_id,
                &first.excerpt.unit_id,
            )?;
            let text = annotation::slice_chars(
                &canonical,
                first.excerpt.start_char,
                first.excerpt.end_char,
            )?;
            if sha256_hex_bytes(text.as_bytes()) != first.excerpt.text_hash {
                return Err(failure(
                    "retrieved source window differs from canonical evidence",
                ));
            }
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
            output.push(ScoredExcerpt {
                candidate_id: key.clone(),
                source_id: first.source_id.clone(),
                parse_id: first.parse_id.clone(),
                excerpt: first.excerpt.clone(),
                text: text.to_owned(),
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

/// Address a canonical source slice independently of which annotation representation found it.
pub(crate) fn excerpt_key(parse_id: &str, excerpt: &SourceExcerpt) -> Result<String, ApiError> {
    canonical_sha256_hex_of(&(parse_id, excerpt))
}

/// Whole-unit candidates remain distinct from partial excerpts until passage overlap resolution.
fn unit_key(parse_id: &str, unit_id: &str) -> String {
    format!("unit:{parse_id}:{unit_id}")
}

/// Preserve the canonical hit ID while recording the more precise source target separately.
fn window_hit(matched: &WindowMatch) -> RetrievalHit {
    RetrievalHit {
        hit_type: RetrievalHitType::ContentUnit,
        hit_id: matched.excerpt.unit_id.clone(),
        source_id: matched.source_id.clone(),
        parse_id: matched.parse_id.clone(),
        unit_ids: vec![matched.excerpt.unit_id.clone()],
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

/// Enforce the same canonical role exclusions as ordinary source retrieval.
fn eligible_unit(
    conn: &Connection,
    source_id: &str,
    parse_id: &str,
    unit_id: &str,
) -> Result<bool, ApiError> {
    let (kind, role): (String, Option<String>) = conn.query_row(
        "SELECT content_type, json_extract(body_json, '$.blockRole') FROM content_units WHERE id=?1 AND source_id=?2 AND parse_id=?3",
        params![unit_id, source_id, parse_id], |row| Ok((row.get(0)?, row.get(1)?)),
    ).map_err(|source| failure(format!("validate annotation target {unit_id} in {parse_id}: {source}")))?;
    Ok(!(kind == "text_block" && matches!(role.as_deref(), Some("header" | "footer"))))
}

/// Preserve source context through the existing query error envelope.
fn failure(message: impl Into<String>) -> ApiError {
    ApiError::StorageOperation {
        message: message.into(),
    }
}
