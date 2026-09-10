# Defects and limitations

Recorded from source inspection on 2026-09-08. This register tracks defects,
limitations, and proposed enhancements. Proposed remedies are directions for future
design approval, not approved implementation plans. Their retrieval-quality and
performance benefits have not been measured.

| ID | Finding | Status |
| --- | --- | --- |
| D04 | Annotation input sizing uses characters and truncates oversized units | Open |
| D05 | Annotation output structure is enforced after generation | Open |
| D06 | Annotation selection is exhaustive rather than selective | Open |
| D07 | Graph traversal cannot follow connections across documents | Open |
| D08 | Derived-data rebuilding is too coarse | Open |
| D09 | Graph entry lacks semantic matching through entity embeddings | Open |

## D04 — Character-based annotation budgets and incomplete oversized-unit coverage

**Current behavior.** `split_targets` in
[src/annotations/producer.rs](src/annotations/producer.rs) groups input using
`max_input_chars`. If one unit exceeds the cap, its text is truncated to the
first allowed characters and `annotator_plan.unit_truncated` is logged. The
remainder is not emitted as another invocation. The budget applies to target
text rather than the complete tokenized request, including its system prompt.

**Impact.** Character counts do not establish model token counts or predict
request cost reliably. Large requests can still be expensive, and annotations
from a truncated unit do not cover its omitted tail. Cancellation improves
control of unfinished work but does not fix its input size or coverage.

**Proposed remedy.** Budget requests using the annotator's actual tokenizer,
including prompt overhead. Split oversized units into explicit fragments instead
of dropping the tail. Fragment identity and coverage must participate in
invocation planning, provenance, memoization, and satisfaction accounting; simply
reusing the same unit ID for several partial inputs is insufficient.

**Resolution criteria.** Every intended character has recorded coverage;
requests obey the real token budget; partial output cannot mark an entire unit
complete; memo entries distinguish different fragments; token counts are measured
rather than estimated from characters.

## D05 — Annotation schema validation occurs after generation

**Current behavior.** `ChatCompletionRequest` in
[src/annotations/llm_client.rs](src/annotations/llm_client.rs) sends the model,
messages, temperature, and thinking-control extension, but no structured-output
schema. `ProducerKind::parse_output` in
[src/annotations/producer.rs](src/annotations/producer.rs) dispatches to the
strict entity, relation, and summary parsers after the response arrives.

**Impact.** A request can consume inference time and then fail because its output
contains missing fields, invalid types, or malformed JSON. Such rejected outputs
consume the worker's output-retry budget. Missing relation `object` fields were
observed during the session; those observations do not establish a current
failure rate for a different model or server configuration.

**Proposed remedy.** Supply a producer-specific structured-output schema where
the deployed endpoint supports it. Verify its compatibility with each existing
parser, including empty results and optional values. Keep application-side
validation and full error context.

**Resolution criteria.** Generated shapes agree with the strict parsers;
unsupported schema behavior fails visibly; malformed-output retry rates can be
compared. Schema conformance must not be represented as factual correctness or
evidence that an extracted relationship is supported by the source.

## D06 — Annotation work is not selected by retrieval value

**Current behavior.** The policy in
[src/annotations/policy.rs](src/annotations/policy.rs) requires entity, relation,
and summary annotations after activation. The producer plan assigns entity and
relation work to section groups and summary work to document groups. Discovery
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
