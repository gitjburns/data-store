# Defects and limitations

Recorded from source inspection on 2026-09-08; D04–D06 descriptions reconciled
with annotation source on 2026-09-10. This register tracks defects,
limitations, and proposed enhancements. Proposed remedies are directions for future
design approval, not approved implementation plans. Their retrieval-quality and
performance benefits have not been measured.

| ID | Finding | Status |
| --- | --- | --- |
| D04 | Annotation input sizing lacks a complete-request token budget | Open |
| D05 | Annotation requests lacked structured-output schemas | Resolved in code; endpoint enforcement unverified |
| D06 | Annotation selection is exhaustive rather than selective | Open |
| D07 | Graph traversal cannot follow connections across documents | Open |
| D08 | Derived-data rebuilding is too coarse | Open |
| D09 | Graph entry lacks semantic matching through entity embeddings | Open |

## D04 — Character-based annotation budgets

**Current behavior.** `build_invocation_plan` in
[src/annotations/producer.rs](src/annotations/producer.rs) uses `split_text` in
[src/annotations/excerpt.rs](src/annotations/excerpt.rs) to partition source text
without dropping oversized-unit tails. Each invocation contains one excerpt
bounded by `max_input_chars`, with exact fragment ranges recorded in provenance.
The cap measures source characters, not the complete tokenized request;
system prompts and prior-stage output are additional input.

**Impact.** Character counts do not establish model token counts or predict
request cost reliably. Lossless splitting resolves omitted tails but does not
establish a token budget for the complete request.

**Proposed remedy.** Budget requests using the annotator's actual tokenizer,
including prompts and prior-stage output. Preserve exact fragment identity and
coverage in planning, provenance, memoization, and satisfaction accounting.

**Resolution criteria.** Complete requests obey the real token budget, measured
with the annotator's tokenizer. Budgeting preserves source coverage and cannot
let partial output satisfy an entire unit or conflate distinct fragments.

## D05 — Structured-output schemas for annotation requests

**Current behavior.** `ChatCompletionRequest` in
[src/annotations/llm_client.rs](src/annotations/llm_client.rs) sends
`response_format.type = "json_schema"` with `strict: true` and the stage-specific
schema defined in [src/annotations/stages.rs](src/annotations/stages.rs).
Application-side output validation remains required.

**Status.** The missing request-schema defect is resolved in code. Enforcement
by the deployed endpoint, compatibility of generated outputs with application
validation, and effects on malformed-output retry rates remain unverified.
Schema conformance does not establish factual correctness or source support
for an extracted relationship.

## D06 — Annotation work is not selected by retrieval value

**Current behavior.** The policy in
[src/annotations/policy.rs](src/annotations/policy.rs) requires entity, relation,
and summary annotations after activation. The producer plan assigns each type
one source excerpt per invocation. Section/document labels organize routing and
discovery; they do not combine excerpts into larger requests. Discovery
in [src/annotations/worker.rs](src/annotations/worker.rs) schedules the matching
work across eligible active sources. Satisfaction checks and memoization avoid
repeating completed inputs, but do not select which new inputs merit annotation.

**Impact.** Annotation can dominate ingestion even when dense and lexical
retrieval already cover much of the material. The implementation has no policy
for spending annotation effort selectively. The cost is observable; the marginal
retrieval value of exhaustive annotation has not been measured.

**Proposed remedy.** Design selective annotation using explicit criteria and a
versioned policy. Preserve visibility of intentionally unannotated coverage,
failures, pending work, and exhausted retries as different states. This is the
previously discussed follow-up to richer dense embeddings, not implemented work.

**Resolution criteria.** The policy explains which inputs receive each type of
annotation; omission does not masquerade as complete graph coverage; pending work
and memo reuse follow the selected policy; reduced inference cost is considered
alongside observed retrieval changes. Selection criteria still require approval.

## D07 — Graph connections remain within one document parse

**Current behavior.** `graph_channel` in
[src/query/channels.rs](src/query/channels.rs) iterates captured parses and calls
`mentions_for_name` and `one_hop_edges` using the current parse ID. Far-end entity
mentions are looked up within that same parse. The active retrieval profile also
limits graph traversal to one hop.

**Impact.** A query can independently match several documents, but a relationship
in document A cannot lead graph traversal into mentions in document B. For
example, a connection from a project to a subsystem in one document cannot lead
directly to that subsystem's supporting discussion in another document through
the graph path. Dense or lexical search may still find that discussion.

**Proposed remedy.** Introduce scope-aware cross-document entity linking and
traversal. Entity disambiguation is a prerequisite: equal or similar names do not
prove that two mentions identify the same entity. Semantic entity-name lookup
helps graph entry but does not, by itself, establish cross-document identity.
Additional traversal hops would need a separately defined bounded policy.

**Resolution criteria.** Cross-document paths preserve source evidence,
relationship direction, active-parse identity, and governance constraints;
ambiguous names do not create asserted equivalence; traversal remains bounded;
returned provenance identifies the actual path used.

## D08 — Rebuilding derived data requires overly broad work

**Current behavior.** There is no dedicated public operation to regenerate a
selected embedding or annotation layer while preserving unrelated results.
`evaluate_no_retry_guard` in [src/scheduler.rs](src/scheduler.rs) can skip a
reparse for an unchanged source/parser identity as `already_parsed`. Thus the
existing reparse route is not a general projection-refresh command.

The available full reset in [src/reset.rs](src/reset.rs) clears indexed state
and artifacts and resumes automatic ingestion. Its route and CLI contracts are
documented in [PROTOCOL.md](PROTOCOL.md) and [SPEC-CLIENT.md](SPEC-CLIENT.md).
Rebuild cancellation now stops unnecessary annotation requests during draining;
it does not make the rebuild selective.

**Impact.** Experiments with embeddings or annotation policy can force repeated
parsing and model work that the actual change did not require. This increases
iteration cost and discards otherwise useful derived results and history.

**Proposed remedy.** Add explicit source/layer rebuild operations that reuse
canonical parsed content and regenerate the selected projections plus their
true dependents. Model or policy identity, freshness, activation, cache
publication, snapshots, and failure recovery must agree on the refreshed state.

**Resolution criteria.** Operators can target the intended layer and source;
unrelated canonical and derived data is retained; stale dependent projections
cannot be served as fresh; operations report accepted, progress, and terminal
outcomes; interrupted work has an explicit recovery path. This requires a
separate design and operational contract before implementation.

## D09 — Entity embeddings for semantic graph entry

**Current behavior.** `candidate_entity_names` and `graph_channel` in
[src/query/channels.rs](src/query/channels.rs) find graph entry points using
normalized query text and stored entity names. Exact matching is supplemented
by optional acronym and token-prefix matching. Entity annotations do not have
a semantic embedding lookup. Passage and section embeddings search document
content, but do not select graph entities through semantic similarity.

**Impact.** A query can describe a concept without naming it. For example,
"sticking to daily habits" may be relevant to an annotated entity named
"self-discipline", yet fail to enter the graph through that entity. Dense
passage retrieval may still find relevant text, but the entity's relationships
are not explored through that semantic connection.

**Proposed remedy.** Embed distinct entity names using the configured dense
model and retain their canonical mention mappings. At query time, compare the
query embedding with entity embeddings eligible within the requested scope,
then use selected entities as additional entry points for the existing graph
traversal. Reuse embeddings for identical inputs rather than embedding every
mention. Keep existing name matching and preserve the source passages as
evidence. Whether embedding inputs should include entity type or source context,
and how semantic matches are bounded and ranked, require detailed design.

**Resolution criteria.** Queries using different wording can nominate relevant
entities and reach their source passages; exact matches retain an intentional
ranking policy; semantic candidates are bounded and scoped before selection;
provenance distinguishes semantic entity matching from exact/acronym/prefix
matching. Embedding identity, freshness, cache publication, and snapshot/restore
coverage must follow the existing projection lifecycle. Evaluate additional
useful matches and false positives rather than treating vector similarity as
proof of relevance.

**Relationship to D07.** Semantic graph entry does not establish that similarly
named entities in different documents are identical. Cross-document entity
linking remains a separate capability with its own disambiguation requirements.
