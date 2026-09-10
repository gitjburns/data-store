//! Semantic annotations and memoization (spec §20–§21; CA cluster).
//!
//! Layout, one module per CA work package:
//! - `store` (CAa): hot-plane persistence and freshness lifecycle for
//!   SemanticAnnotation rows, with atomic `annotation.*` events.
//! - `excerpt` + `producer`: lossless source slices, identity, and provenance.
//! - `stages` + `chains` + `llm_client`: single-goal requests over each excerpt.
//! - `entity`/`relation`/`summary`: final annotation shape validation.
//! - `memo` (CAc): §21.2 content-keyed memoization cache; reuse is recorded
//!   honestly through the Provenance memoization fields.
//! - `policy` (CAd): the versioned, hashed required-annotation-set policy
//!   document read by the activation prerequisite seam.
//! - `worker` (CAd): the dedicated discovery-based build thread; annotations
//!   build post-activation and never block activation or the sync pipeline.

pub(crate) mod chains;
pub(crate) mod entity;
pub(crate) mod excerpt;
pub(crate) mod llm_client;
pub(crate) mod memo;
pub(crate) mod policy;
pub(crate) mod producer;
pub(crate) mod relation;
pub(crate) mod stages;
pub(crate) mod store;
pub(crate) mod summary;
pub(crate) mod vocabulary;
pub(crate) mod worker;
