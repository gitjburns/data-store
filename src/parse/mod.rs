//! Parsing layer (spec §12–§13 build side): staged parser output bundles,
//! the core importer that validates and canonicalizes them, conformance
//! measurement, and the parser workers. Parsers are untrusted producers
//! outside the hot retrieval trust boundary (§12.1): workers write staged
//! bundles only; the importer is the only writer of canonical parse state.
//! Activation and dispatch wiring are C5.

pub(crate) mod bundle;
pub(crate) mod cleanup;
pub(crate) mod conformance;
pub(crate) mod importer;
pub(crate) mod mupdf_worker;
pub(crate) mod native_pdf;
pub(crate) mod pdf;
pub(crate) mod pdf_worker;
pub(crate) mod text_worker;
