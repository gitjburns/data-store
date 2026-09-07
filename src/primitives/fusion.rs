//! Reciprocal rank fusion of dense, BM25, and graph candidate lists with a
//! deterministic tie-break (score `total_cmp`, then unit id ordering). The
//! dense, BM25, and fused match records live here with the fusion algorithm
//! that consumes and produces them.

// Retrieval fusion substrate consumed by the query fusion stage
// (query::channels). `empty_fused_match` and `reciprocal_rank_score` are
// internal helpers called by `fuse_matches`.

use std::collections::BTreeMap;

/// One dense-retrieval candidate: cosine similarity plus its 1-based rank in the dense list.
#[derive(Debug)]
pub(crate) struct DenseMatch {
    pub(crate) unit_id: String,
    pub(crate) similarity: f32,
    pub(crate) rank: usize,
}

/// One BM25 candidate: FTS5 bm25 score plus its 1-based rank in the BM25 list.
#[derive(Debug)]
pub(crate) struct Bm25Match {
    pub(crate) unit_id: String,
    pub(crate) score: f64,
    pub(crate) rank: usize,
}

/// One fused candidate. Per-signal rank/score fields are `None` when that
/// signal did not return the unit; `score` is the combined RRF rank signal.
#[derive(Debug)]
pub(crate) struct FusedMatch {
    pub(crate) unit_id: String,
    pub(crate) score: f64,
    pub(crate) rank: usize,
    pub(crate) dense_rank: Option<usize>,
    pub(crate) dense_similarity: Option<f32>,
    pub(crate) bm25_rank: Option<usize>,
    pub(crate) bm25_score: Option<f64>,
    pub(crate) graph_rank: Option<usize>,
}

/// Fuse unit-deduplicated channel lists; graph scores carry only ordinal meaning.
///
/// The output score is only a fused rank signal, not a semantic similarity score.
pub(crate) fn fuse_matches(
    dense_matches: &[DenseMatch],
    bm25_matches: &[Bm25Match],
    graph_matches: &[(&str, usize)],
    top_k: usize,
    rrf_k: u32,
) -> Vec<FusedMatch> {
    let mut values = BTreeMap::<String, FusedMatch>::new();
    for matched in dense_matches {
        let entry = values
            .entry(matched.unit_id.clone())
            .or_insert_with(|| empty_fused_match(&matched.unit_id));
        entry.score += reciprocal_rank_score(rrf_k, matched.rank);
        entry.dense_rank = Some(matched.rank);
        entry.dense_similarity = Some(matched.similarity);
    }
    for matched in bm25_matches {
        let entry = values
            .entry(matched.unit_id.clone())
            .or_insert_with(|| empty_fused_match(&matched.unit_id));
        entry.score += reciprocal_rank_score(rrf_k, matched.rank);
        entry.bm25_rank = Some(matched.rank);
        entry.bm25_score = Some(matched.score);
    }
    for &(unit_id, rank) in graph_matches {
        let entry = values
            .entry(unit_id.to_owned())
            .or_insert_with(|| empty_fused_match(unit_id));
        entry.score += reciprocal_rank_score(rrf_k, rank);
        entry.graph_rank = Some(rank);
    }

    let mut fused = values.into_values().collect::<Vec<_>>();
    fused.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.unit_id.cmp(&right.unit_id))
    });
    fused.truncate(top_k);
    for (index, matched) in fused.iter_mut().enumerate() {
        matched.rank = index + 1;
    }

    fused
}

/// Return the initial fused-candidate record for one unit id.
fn empty_fused_match(unit_id: &str) -> FusedMatch {
    FusedMatch {
        unit_id: unit_id.to_string(),
        score: 0.0,
        rank: 0,
        dense_rank: None,
        dense_similarity: None,
        bm25_rank: None,
        bm25_score: None,
        graph_rank: None,
    }
}

/// Return the reciprocal-rank contribution for one candidate rank using the configured RRF K constant.
fn reciprocal_rank_score(rrf_k: u32, rank: usize) -> f64 {
    1.0 / (rrf_k as f64 + rank as f64)
}
