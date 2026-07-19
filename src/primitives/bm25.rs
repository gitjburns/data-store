//! BM25 FTS5 query construction: normalizes user text into strict (AND) and
//! broad (OR) quoted term queries while preserving the plain-text boundary.

// Retained retrieval substrate; its sole legacy consumer (storage.rs) is
// deleted at cluster CR. Consumed by the C6a lexical builder and the C7b
// lexical channel; remove when wired.
#![allow(dead_code)]

/// The strict (AND) and broad (OR) FTS5 query strings built from one user
/// query; either side is `None` when no eligible terms survived filtering.
#[derive(Debug)]
pub(crate) struct Bm25Queries {
    pub(crate) strict_query: Option<String>,
    pub(crate) broad_query: Option<String>,
}

const MIN_FTS_TERM_CHARS: usize = 3;
const FTS_STOPWORDS: &[&str] = &[
    "about",
    "above",
    "after",
    "again",
    "against",
    "also",
    "among",
    "because",
    "before",
    "being",
    "between",
    "both",
    "but",
    "cannot",
    "could",
    "does",
    "doing",
    "during",
    "each",
    "from",
    "further",
    "had",
    "has",
    "have",
    "having",
    "here",
    "hers",
    "herself",
    "him",
    "himself",
    "his",
    "how",
    "into",
    "its",
    "itself",
    "more",
    "most",
    "nor",
    "not",
    "off",
    "once",
    "only",
    "other",
    "our",
    "ours",
    "ourselves",
    "out",
    "over",
    "own",
    "same",
    "she",
    "should",
    "some",
    "such",
    "than",
    "that",
    "the",
    "their",
    "theirs",
    "them",
    "themselves",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "too",
    "under",
    "until",
    "very",
    "was",
    "were",
    "what",
    "when",
    "where",
    "which",
    "while",
    "who",
    "whom",
    "why",
    "will",
    "with",
    "would",
    "you",
    "your",
    "yours",
    "yourself",
    "yourselves",
];

/// Build strict and broad BM25 query strings from user text while preserving the plain-text boundary.
pub(crate) fn build_bm25_queries(query: &str) -> Option<Bm25Queries> {
    let mut broad_terms = Vec::<String>::new();
    let mut strict_terms = Vec::<String>::new();
    for value in query.split(|value: char| !value.is_alphanumeric()) {
        let term = value.trim().to_lowercase();
        if term.is_empty() || broad_terms.iter().any(|existing| existing == &term) {
            continue;
        }
        if term.chars().count() >= MIN_FTS_TERM_CHARS && !is_fts_stopword(&term) {
            strict_terms.push(term.clone());
        }
        broad_terms.push(term);
    }
    if broad_terms.is_empty() {
        return None;
    }

    Some(Bm25Queries {
        strict_query: build_joined_fts_query(strict_terms, " AND "),
        broad_query: build_joined_fts_query(broad_terms, " OR "),
    })
}

/// Build one quoted FTS5 query string from already-normalized terms.
fn build_joined_fts_query(terms: Vec<String>, separator: &str) -> Option<String> {
    if terms.is_empty() {
        return None;
    }

    Some(
        terms
            .into_iter()
            .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(separator),
    )
}

/// Return whether a normalized query token is too common to help BM25 narrow the candidate set.
fn is_fts_stopword(term: &str) -> bool {
    FTS_STOPWORDS.contains(&term)
}
