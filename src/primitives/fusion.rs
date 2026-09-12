//! Ranked-list fusion with one vote per distinct key in each input list.
//! Callers own channel grouping and attribution; ties use canonical key order.

use std::collections::{BTreeMap, BTreeSet};

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

/// Fuse ranked keys with one vote per list, compressing duplicate entries before assigning ranks.
pub(crate) fn fuse_ranked_lists(
    lists: &[Vec<String>],
    top_k: usize,
    rrf_k: u32,
) -> Vec<(String, f64)> {
    let mut values = BTreeMap::<&str, f64>::new();
    for list in lists {
        let mut seen = BTreeSet::<&str>::new();
        let mut rank = 0;
        for key in list {
            if !seen.insert(key.as_str()) {
                continue;
            }
            // A repeated representation must neither add a vote nor displace another key's rank.
            rank += 1;
            *values.entry(key.as_str()).or_default() += reciprocal_rank_score(rrf_k, rank);
        }
    }
    let mut fused: Vec<_> = values.into_iter().collect();
    fused.sort_by(|left, right| right.1.total_cmp(&left.1).then_with(|| left.0.cmp(right.0)));
    fused.truncate(top_k);
    // Only returned keys need owned storage; all fusion accounting borrows the caller's lists.
    fused
        .into_iter()
        .map(|(key, score)| (key.to_owned(), score))
        .collect()
}

/// Return the reciprocal-rank contribution for one candidate rank using the configured RRF K constant.
fn reciprocal_rank_score(rrf_k: u32, rank: usize) -> f64 {
    1.0 / (rrf_k as f64 + rank as f64)
}
