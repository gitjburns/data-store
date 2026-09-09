//! Retrieval fabric query pipeline (spec §24, §31, §38): the synchronous
//! per-query pipeline that opens one WAL read snapshot, generates candidates
//! across the MVP channels (dense, lexical, graph), fuses and reranks them,
//! and returns ranked `RetrievalHit`s for context assembly.
//!
//! This module is filled across the C7 cluster: C7s stands up the module root
//! and the shared §24 contract types (`model`); the parallel and serial C7
//! packages fill the remaining submodules declared below. The whole surface is
//! now LIVE: C8d-2 wired the `/query` route through `execute::execute_query`,
//! so the pipeline, its DTOs, and the profile are reachable from the request
//! path — the former module-level `dead_code` allow was removed at C8d-2.

pub(crate) mod channels; // C7b: dense/lexical/graph candidate generation + RRF fusion.
pub(crate) mod execute; // C7d: the synchronous execute_query pipeline.
pub(crate) mod model; // C7s: shared §24 contract types (this package).
pub(crate) mod passages; // Canonical passage construction before final reranking.
pub(crate) mod profile; // C7a: sealed RetrievalProfile + scope resolution.
pub(crate) mod provenance; // Attribution wire types shared with the CLI.
pub(crate) mod request; // C8d-1: §24.3 QueryRequest MVP envelope + validation.
pub(crate) mod rerank; // C7c: ColBERT MaxSim over the fused pool + final reranker.
