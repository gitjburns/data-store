//! Context assembly (§25–§27): converts reranked RetrievalHits into an
//! `EvidencePack` of canonical ContentUnits. Assembly is deterministic,
//! visible, versioned, and traceable — a query with the same hits and the same
//! sealed policy always produces the same pack, and every non-anchor inclusion
//! is attributed to a policy rule in the `ContextAssemblyTrace` (§27).
//!
//! DECOUPLING INVARIANT (mirrors `query/`-vs-`projections/`): nothing under
//! `src/assembly/` imports axum/tokio/http types, and no function here opens
//! its own database connection. Every assembly function takes
//! `&rusqlite::Connection` (the caller's query read transaction) plus explicit
//! inputs. The transport shell (`http.rs`) and the pipeline stage that owns the
//! transaction (`query/execute.rs`) live outside this boundary and thread the
//! connection in; assembly stays a pure, synchronous graph-and-unit reader.
//!
//! Submodule ownership (declared here in full so later packages never contend
//! on this file): `model` — the §25–§27 wire types (C8s); `policy` — the sealed
//! MVP `AssemblyPolicy` and the graph operators' policy surface (C8a);
//! `operators` — the seven §25 graph operators (C8a); `evidence` — the
//! `build_evidence_pack` entry point (C8b).

pub(crate) mod evidence;
pub(crate) mod model;
pub(crate) mod operators;
pub(crate) mod policy;
