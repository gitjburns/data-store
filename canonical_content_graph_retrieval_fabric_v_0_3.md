# Canonical Content Graph and Retrieval Fabric

Version: 0.4
Status: Draft Specification
Supersedes: 0.3

## Revision Summary (0.3 → 0.4)

This revision is the content-model revision that accompanies the EPUB parser
worker (SPEC-epub.md). The operating model, acquisition layer, security
reservations, replay model, audit SLA, forensic architecture, activation
gating, and deletion lifecycle of v0.3 are retained. The content model is:

1. **ContentType** is a closed set of thirteen types with `document` as the
   single root unit of a parse and `list`, `list_item`, and `aside` as
   containers; exactly four types carry evidence text (§15).
2. **Locators** are a closed union of `dom_path` and `char_range` (§17).
3. **Typed bodies** reject unknown fields, hold no reference to another unit,
   and carry no normalized-text or rendering fields (§18).
4. **UnitRelationship** is a closed set of six types; `references` is the only
   type with roles (§19).
5. **ParseMetrics** counts every container type (§12).
6. **Conformance dimensions** are the six named keys of §12.5.
7. **Parser output bundles** archive image bytes under `artifacts/` by
   SHA-256, and the importer lists them in the canonical bundle manifest
   (§12.2, §12.3).
8. **Parsers** are plain text and EPUB 2/3; every other format is converted to
   plain text outside this system (§4, §5, §36).

## 1. Purpose

This specification defines a canonical content graph and retrieval fabric over
unstructured and semi-structured data drawn from external source systems. The
system continuously acquires documents from disparate, independently governed
silos into a centralized index repository, keeps that repository as close to
the sources' current state as observed reality and capacity allow, and answers
search queries with full provenance and audit-grade evidence.

The system must be able to answer two questions at any later time:

```text
1. What specific source data is this search result based on?

2. How was a search result obtained in the past constructed, so that
   responsibility for any inaccuracy it contains can be determined?
```

The core design separates durable source-derived truth from
retrieval-optimized projections. Source acquisition, parsed content units,
structural relationships, semantic annotations, retrieval projections, query
execution records, and forensic snapshots are modeled explicitly so that the
system supports both near-real-time retrieval and post-event investigation.

The system is intended for workloads where generated outputs may have
operational, legal, financial, reputational, or compliance impact.

### 1.1 Specification Notation

Schema blocks in this document describe a language-neutral data model. They
are normative with respect to field names, field meanings, required/optional
status, allowed values, and structural relationships; they are not a
requirement to use any particular programming language, type system,
framework, serialization library, or runtime.

```text
field?: T
  Optional field. Omitted means absent. Explicit null has semantic meaning
  only when the field definition allows it.

Record<string, unknown>
  Map from string keys to JSON-compatible values.

"a" | "b" | "c"
  Closed enumeration of allowed string values.

T[]
  Ordered array of T.

TBody or other generic placeholders
  The field body is governed by the associated typed schema, not by a
  language-specific generic type.
```

Implementations may publish these schemas as JSON Schema, OpenAPI, Protocol
Buffers, database schemas, or any other representation that preserves the
normative data contract.

Unless otherwise stated, JSON examples are transport examples. Canonical
persisted artifacts are defined by the artifact and serialization rules in
this specification.

## 2. Operating Model

The following operating facts are normative and correct v0.2's assumptions:

```text
The corpus is external and uncontrolled. Documents appear, change, are
renamed, and disappear without notice and outside this system's authority.

Acquisition, parsing, activation, deactivation, and snapshotting are
autonomous. No step of normal operation requires a human present.

Parse activation is a routine, frequent, system-initiated event, not an
operator-initiated infrastructure operation.

There is exactly one search executor. Every production query executes
through this system's query fabric and produces a QueryExecutionRecord.

Humans participate asynchronously and optionally: they review surfaced
truth (logs, health, held-parse dispositions) on their own schedule. A
deployment whose operators never look remains correct, merely degraded in
ways the system reports truthfully.
```

## 3. Design Principles

```text
The hot system contains only active production truth.

Raw sources, acquisition provenance, and canonical parses are durable.

Canonical parsed state is typed content graph state, not Markdown.

Chunks are targeting artifacts, not evidence.

Final evidence is composed from canonical ContentUnits.

Parser upgrades create net-new canonical graphs.

Parser workers and connectors are untrusted producers; the core system
validates and imports.

Superseded production state is snapshotted before deletion from hot storage.

Every production query creates an immutable QueryExecutionRecord.

Context assembly behavior must be visible, deterministic, versioned, and
traceable.

Forensic replay claims are graded and honest; the system never claims a
replay fidelity it cannot demonstrate.

The system adapts to load and reports truth; it does not enforce guessed
thresholds. Configuration records external facts, not internal guesses.

Quality gating is by demonstrable fact (invariant violation, measured
regression on identical input), never by guessed threshold.

Deletion is inferred from evidence, never from absence counting.

Degradation is always visible: logged, health-surfaced, and recorded in
query execution records. Silent degradation is prohibited.
```

## 4. Scope

This specification describes acquisition from external source systems, the
internal data model, parsing lifecycle, retrieval fabric, context assembly
model, query execution logging, freshness reporting, deletion lifecycle, and
forensic snapshot requirements for a centralized index repository serving a
single search executor.

The system supports:

- Autonomous acquisition from heterogeneous external source systems through
  connectors with declared capabilities.
- Immutable source object storage with content-based identity and
  location-scoped presence.
- Versioned parse runs with measured conformance and unattended activation.
- Parsing of plain-text (`text/plain`) and EPUB 2 and 3
  (`application/epub+zip`) sources; every other format is converted to plain
  text outside this system.
- Typed canonical content units, durable structural relationships, and
  versioned semantic annotations.
- Disposable but snapshot-preserved retrieval projections.
- Human-readable canonical artifact bundles using typed JSON/JSONL.
- Isolated parser and connector execution through explicit artifact contracts.
- Chunk-based retrieval targeting and deterministic EvidencePack construction.
- QueryExecutionRecords for every production query, embedding served evidence.
- Content-addressed forensic snapshots and verified restore.
- Evidence-based deletion propagation and restore-based reappearance.
- Adaptive, knob-free acquisition scheduling with truthful freshness
  reporting.

## 5. Out of Scope

- Entitlement resolution and permission enforcement (reserved for a future
  layer; the retrieval-side contract is normative now — see §6).
- User authentication, tenant routing, administrative access policy.
- Compliance-driven hard erasure of audit artifacts (named deferral — see
  §11.5).
- Verified counterfactual replay (rank-stable recompute) — documented future
  tier, not committed (see §29.4).
- Multi-executor search deployment.
- Parsing of any format other than plain text and EPUB; a source of any other
  MIME type is counted as unparseable.
- DRM-protected EPUB archives, fixed-layout rendering, media overlays,
  scripting, multiple renditions, remote resources, inline markup inside unit
  text, and print-page geometry (a `page` carries an ordinal and a label
  only).
- Specific implementation languages, frameworks, parser products, container
  runtimes, queue products, or deployment platforms.

## 6. Security and Permissions Model

Permissions are a critical requirement whose implementation is deliberately
deferred. This section replaces v0.2 §5 in full.

The system is designed so that an entitlement layer can be added later
without surgery to acquisition, indexing, or search. Five reservations are
normative from day one:

```text
1. Retrieval scope is a first-class query-plan concept. Every retrieval
   channel accepts a source-scope predicate and enforces it at candidate
   generation. Ranked results are never post-filtered for scope. The
   default scope is all sources.

2. QueryRequest carries an optional, opaque callerContext field. It is
   passed through unmodified and recorded in the QueryExecutionRecord.

3. Every SourceLocation carries a governanceDomain, assigned at
   acquisition from connector configuration. Re-tagging a domain is a
   metadata operation and requires no re-parse or re-index.

4. Every QueryExecutionRecord records the resolved scope in force, and
   planHash covers it.

5. Invariant: no query path computes over out-of-scope sources.
```

A future entitlement layer resolves a caller to an allowed source set
(domain-granular or item-granular; both reduce to source-set scoping because
units, relationships, and assembly never cross a source boundary) and
intersects it into the resolved scope. Nothing else changes.

Known accepted caveat: lexical index statistics (e.g., BM25 document
frequencies) are corpus-global; scoped queries never leak out-of-scope
candidates, but scores carry a statistical shadow of the wider corpus.
Per-domain index partitioning is the future remedy if required; indexes
rebuild deterministically from canonical state.

There are no per-unit ACL fields in the canonical data model.

## 7. System Layers

```text
Connector
  An untrusted producer that interrogates one external source system and
  emits acquisition bundles.

AcquisitionRecord
  Durable provenance for one acquisition attempt (success or failure).

SourceObject
  Immutable raw content identified by hash, present at one or more
  SourceLocations.

ParseRun
  A parser execution against a SourceObject, with a measured conformance
  report.

ContentUnit
  A stable, addressable canonical unit derived from a ParseRun.

UnitRelationship
  A durable structural relationship between ContentUnits.

SemanticAnnotation
  A model-, rule-, or human-derived annotation over one or more ContentUnits.

RetrievalProjection
  A retrieval-optimized projection envelope with a typed payload.

AssemblyPolicy
  A visible, versioned policy governing deterministic context assembly.

EvidencePack
  A query-time set of canonical ContentUnits assembled for a caller.

QueryExecutionRecord
  An immutable, self-contained record of a production query execution.

ForensicSnapshot
  A content-addressed manifest capturing replayable corpus-serving state.
```

## 8. Durability Classes

### 8.1 Durable Canonical State

Preserved as production truth while its parse is active; preserved in the
artifact store thereafter:

```text
SourceObject and SourceLocation records
AcquisitionRecords (including failures)
ParseRun records, capability profiles, conformance reports
ContentUnit, UnitRelationship
Locators, hashes, parser provenance
Canonical parse artifact bundles (typed JSON/JSONL plus manifests)
DeletionEvidence records
QueryExecutionRecords
```

Markdown, HTML, prompt renderings, and other display/export views are not
durable canonical state unless explicitly modeled as typed records. They may
be regenerated from canonical state.

### 8.2 Semi-Durable Semantic State

Persisted, versioned, audit-relevant, derived: entities, claims, topics,
summaries, classifications, extracted relations, table/figure
interpretations, question-answer annotations. Not source truth; carries
provenance.

### 8.3 Retrieval Projection State

Generated targeting/ranking artifacts: chunk projections, dense vector
payloads, learned sparse payloads, multi-vector payloads, lexical index
documents, graph/temporal/summary projections, reranker features, derived
views. Not canonical evidence. Projection payloads and replayable index state
(or deterministic rebuild artifacts) must be preserved in forensic snapshots.

### 8.4 Ephemeral Runtime State

Caches, in-memory request state, transient scheduling state. Excluded from
snapshots unless required for exact replay.

## 9. Acquisition Layer

### 9.1 Connectors Are Untrusted Producers

A connector is any component that interrogates an external source system —
filesystem scanner, change-feed client, API crawler. Connectors are outside
the canonical trust boundary.

Rules:

1. Connectors must not write canonical storage or hot retrieval indexes.
2. Connectors emit acquisition bundles into an implementation-defined staging
   area: raw source bytes plus a manifest (byte hash, native identifiers,
   native version/etag/modified-time as claimed by the source system,
   acquisition timestamp, connector identity and configuration hash,
   governance domain).
3. The core system validates bundles, computes `sourceHash`, performs
   content-based deduplication and location maintenance (§10), creates or
   updates SourceObjects, writes AcquisitionRecords, and emits events.
4. Connector failure, timeout, malformed output, or resource exhaustion must
   not mutate canonical or serving state.
5. Process isolation for connectors is per-connector implementation policy;
   the staged-bundle contract is normative regardless.

### 9.2 AcquisitionRecord

Every acquisition attempt — successful or failed — produces a durable record.
A source system that cannot be read is operationally meaningful state.

```ts
type AcquisitionRecord = {
  id: string

  connectorName: string
  connectorVersion: string
  connectorConfigHash: string

  sourceSystem: string
  nativeUri: string
  nativeId?: string
  nativeVersion?: string
  nativeModifiedAt?: string

  governanceDomain: string

  outcome: "succeeded" | "failed"
  failureClass?: "unreachable" | "access_denied" | "not_found" | "timeout"
    | "malformed" | "resource_limit" | "other"
  failureDetail?: string

  sourceHash?: string
  sourceObjectId?: string
  sourceLocationId?: string

  acquiredAt: string
  elapsedMs?: number
}
```

### 9.3 Declared Change-Detection Capability

Each connector declares, statically and versioned:

```ts
type ConnectorCapabilityProfile = {
  connectorName: string
  connectorVersion: string

  detectionMode: "change_feed" | "incremental_poll" | "full_scan"

  supportsExplicitDeleteEvents: boolean
  supportsCompleteEnumeration: boolean
  supportsNativeVersioning: boolean

  providerConstraints?: Record<string, unknown>

  profileHash: string
}
```

`providerConstraints` records externally imposed facts (documented API rate
limits, contractual quotas). Recording external facts in configuration is
legitimate; encoding internal guesses is prohibited (§35).

### 9.4 The Sync Queue

Detected changes enter one explicit, durable, inspectable work queue driving
acquisition → parse → gate → activation per source.

Rules:

1. The queue is visible: an operator can always answer what is pending, what
   is in flight, what has failed, and how far behind each source system is.
2. Pending work coalesces latest-state per source: at most one pending change
   per source. Intermediate states never sampled are not evidence and are not
   owed to the audit trail; search serves observed states only.
3. There are no hidden or secondary queues.

### 9.5 Adaptive Scheduling (Knob-Free)

Detection cadence per source adapts continuously from observed signals. There
are no configured cadence floors, ceilings, or backlog thresholds.

Signal classes:

```text
Pipeline backpressure
  Backlog depth and drain rate throttle detection when the shared
  parse/embed/activate pipeline saturates. Coalescing sheds load: slower
  sampling fetches fewer intermediate states.

Observed change frequency
  Sampling rate per source tracks its measured churn. Cold sources are
  sampled rarely; hot sources often, subject to backpressure. Idle capacity
  does not accelerate sampling beyond observed change rates.

Source-system pushback
  Rate-limit responses, errors, and observed operation cost (a scan cannot
  run more often than its own duration) throttle from the source side.
```

Every adaptation is logged with its cause. Current effective cadence,
backlog depth, drain rate, and coalescing counts per source system are
health-visible. Alerting thresholds are the operator's monitoring concern,
outside this system.

### 9.6 Freshness Is Measured Truth

The system holds itself to no freshness target. It achieves what source
physics and capacity allow, adapts to keep pending work bounded, and reports
achieved freshness truthfully:

1. Boundary timestamps are recorded per source per cycle: change observed,
   acquired, parse complete, activated.
2. Observed lag per source system is surfaced in health and logs.
3. Every QueryExecutionRecord carries a freshness record for in-scope
   sources (§28).
4. Detection bounds that exist by construction (a poll interval, a scan
   duration) are reportable facts, not promises.

## 10. SourceObject

Identity is content. Presence is location.

```ts
type SourceObject = {
  id: string

  activeParseId?: string

  mimeType: string
  sizeBytes?: number

  sourceHash: string
  storageUri: string

  locations: SourceLocation[]

  eventTime?: string
  ingestTime: string
  createdAt: string

  deactivatedAt?: string
}
```

```ts
type SourceLocation = {
  id: string

  sourceSystem: string
  nativeUri: string
  nativeId?: string

  governanceDomain: string

  firstSeenAt: string
  lastSeenAt: string

  status: "current" | "deleted" | "access_lost"
  deletionEvidence?: DeletionEvidence

  metadata?: Record<string, unknown>
}
```

Rules:

1. `SourceObject` is immutable with respect to raw content. `sourceHash` is
   a cryptographic hash of the raw source bytes; raw bytes are stored in the
   artifact store and referenced by `storageUri`.
2. One SourceObject exists per `sourceHash`. Acquisition of identical content
   from a new place appends or refreshes a SourceLocation; it never creates a
   duplicate SourceObject and never overwrites existing location records.
   Conflicting descriptive metadata is retained per location, losslessly.
3. A rename or move is one location ending and another beginning on the same
   content.
4. Scope filtering (§6) treats a source as visible in a governance domain if
   any `current` location is in that domain.
5. `activeParseId` identifies the only parse considered production truth.
   Only active-parse objects are queryable.
6. A source with zero `current` locations is deactivated from search (§11).
7. Content changes at a location produce a new SourceObject (new hash) whose
   location supersedes the old object's location; parse and activation follow
   the normal lifecycle.

## 11. Deletion Lifecycle

### 11.1 Evidence-Based Deletion Inference

Deletion is inferred only from evidence, never from absence counting. There
is no "missed N scans" rule. Qualifying signals:

```ts
type DeletionEvidence = {
  signal:
    | "explicit_delete_event"
    | "absent_from_complete_enumeration"
    | "source_reported_gone"

  observedAt: string
  acquisitionRecordId: string
  detail?: string
}
```

1. `explicit_delete_event`: the source system's change feed reported the
   deletion.
2. `absent_from_complete_enumeration`: a successful, complete enumeration of
   the scope that previously contained the location no longer includes it. A
   failed or partial enumeration asserts nothing.
3. `source_reported_gone`: a healthy source system answered "gone" (e.g.,
   404) for the specific item.

### 11.2 Access-Lost Is Not Deletion

Persistent unreachability or revoked access (connection failure, 403) sets
the location to `access_lost`: the document presumably still exists; the
system has lost the ability to observe it. Serving continues; the freshness
clock for that location stops advancing; the state is logged and
health-visible; query freshness records report last-verified time.

### 11.3 Deletion Propagation

Deletion is location-scoped. When a location's deletion is evidenced:

```text
1. Write the durable deletion record (evidence, provenance, timestamps).
2. Set the location status to deleted.
3. If current locations remain, stop: the source remains searchable via its
   surviving locations.
4. If no current locations remain: create the pre-deactivation forensic
   snapshot manifest, atomically deactivate the source from the queryable
   plane through the per-source cutover barrier, then perform
   archive-verify-delete hot cleanup (§31). Set deactivatedAt.
```

Nothing is erased: canonical state, parse bundles, acquisition and deletion
records, and QueryExecutionRecords that cited the source remain in the
artifact store permanently.

### 11.4 Reappearance

If evidenced-deleted content reappears (same `sourceHash`), reactivation is a
restore from the artifact store — no re-parse, no re-embedding — followed by
normal activation. Different content at the old location is an ordinary new
SourceObject.

### 11.5 Named Deferral: Compliance-Driven Erasure

Hard erasure demands (e.g., statutory right-to-be-forgotten) conflict with
immutable QueryExecutionRecords that embed evidence. A designed, explicit,
audited purge operation is required before this system can honor such
demands. It is deliberately out of scope for this revision and must not be
improvised.

## 12. ParseRun

```ts
type ParseRun = {
  id: string
  sourceId: string

  parserName: string
  parserVersion: string
  parserConfigHash: string
  capabilityProfileHash: string

  status:
    | "building"
    | "ready"
    | "active"
    | "archiving"
    | "archived"
    | "failed"

  heldReason?: "conformance_regression"
  conformanceReport?: ConformanceReport

  startedAt?: string
  completedAt?: string
  activatedAt?: string
  archivedAt?: string

  artifactBundleUri?: string
  artifactBundleHash?: string
  parserRawOutputUri?: string

  warnings?: ParseWarning[]
  metrics?: ParseMetrics

  createdAt: string
  error?: string
}
```

```ts
type ParseWarning = {
  code: string
  message: string
  severity: "info" | "warning" | "error"
  locator?: Locator
}

type ParseMetrics = {
  unitCount?: number
  relationshipCount?: number
  pageCount?: number
  sectionCount?: number
  listCount?: number
  asideCount?: number
  tableCount?: number
  figureCount?: number
  codeBlockCount?: number
  annotationCount?: number
  projectionCount?: number
}
```

Rules:

1. A SourceObject may have multiple ParseRuns over time; only one may be
   active at a time.
2. A non-active ParseRun may be built, annotated, indexed, and validated
   without becoming query-visible.
3. Parser upgrades create net-new canonical graphs. The system does not
   preserve unit-level structural lineage across parser versions.
4. A held ParseRun (status `ready` with `heldReason`) is retained pending
   explicit disposition (§13.4). At most one held candidate exists per
   source; a newer candidate supersedes it.

### 12.1 Parser Execution Boundary

Parsers are untrusted producers of candidate structure, outside the hot
retrieval trust boundary. The core content fabric owns canonical IDs,
validation, hashing, persistence, activation, retrieval projections, query
execution records, and forensic snapshots.

Rules (unchanged in substance from v0.2):

1. Parser workers must not write canonical storage or hot indexes directly.
2. Parser workers emit output bundles into staging; output is not canonical
   until validated and imported by the core.
3. Validation covers the typed content model, locator model, relationship
   model, provenance requirements, resource limits, canonical serialization
   rules, and the parser's own capability profile.
4. Parser failure, timeout, cancellation, crash, excessive or malformed
   output must not mutate active serving state.
5. Parser execution has explicit timeout, cancellation, and resource bounds;
   uses a disposable workspace; and captures identity, configuration hash,
   timings, warnings, metrics, and failure diagnostics.
6. Parser raw output may be preserved for diagnostics but is not canonical
   evidence unless imported.

### 12.2 Parser Output Bundle

Staged, untrusted output of one parser execution. Recommended layout:

```text
parser_output_bundle/
  manifest.json
  parser_result.json
  candidate_content_units.jsonl
  candidate_unit_relationships.jsonl
  candidate_semantic_annotations.jsonl
  warnings.jsonl
  metrics.json
  stderr.log
  stdout.log
  parser_raw/
  artifacts/
```

Rules: `manifest.json` records file hashes, parser identity, configuration
hash, source hash, schema version, creation time. Candidate records may use
parser-local references; the core replaces them with canonical IDs at import.
Candidate ContentUnits must be typed; Markdown-only output is insufficient.
Large binaries are referenced as hashed artifacts. `artifacts/` holds image
bytes, one file per distinct image, named by the lowercase SHA-256 hex of its
bytes; `FigureBody.imageHash` is that name, and no body field carries a
storage URI. `stdout.log` and `stderr.log` are empty files for in-process
workers. Failure bundles may be preserved for diagnostics. Bundles are not
queryable.

### 12.3 Canonical Parse Artifact Bundle

The validated, durable artifact set produced by the core after import. Part
of durable canonical state; referenced by forensic snapshots.

```text
canonical_parse_bundle/
  manifest.json
  source_object.json
  parse_run.json
  conformance_report.json
  content_units.jsonl
  unit_relationships.jsonl
  semantic_annotations.jsonl
  retrieval_projections.jsonl
  warnings.jsonl
  metrics.json
  artifacts/
```

Rules: created by the core, never the parser; all records use canonical IDs
and canonical serialization; the manifest records every file path, artifact
type, hash, byte size, schema version, source ID, parse ID, parser identity,
configuration hash, creation time, and manifest hash. At import the core
recomputes the hash of every file under the staged bundle's `artifacts/`; a
name that does not equal its hash is a contract violation (recorded parse
failure). Each verified file is written to the artifact store and listed in
the manifest with `artifactType = "image"`; bodies are not rewritten, and the
store resolves `imageHash` to the blob. Renderings may be included as
derived-view artifacts but never replace typed records.

### 12.4 Parser Capability Profile

Each parser declares, statically and versioned, what it attempts to emit:

```ts
type ParserCapabilityProfile = {
  parserName: string
  parserVersion: string
  parserConfigHash: string

  emitsContentTypes: ContentType[]
  emitsRelationshipTypes: UnitRelationshipType[]
  emitsLocatorKinds: string[]
  emitsBodyFields?: Record<string, string[]>

  profileHash: string
}
```

A parse whose output omits a capability its profile declares, where the
input plausibly contains it, is measured by the conformance report; a parse
that emits structure violating its declaration fails validation.

### 12.5 Conformance Report

At import, the core measures what the parse actually contains:

```ts
type ConformanceReport = {
  parseId: string

  unitTypeCounts: Record<string, number>
  relationshipTypeCounts: Record<string, number>

  locatorCoverage: number
  captionPairingRate?: number
  tableDecompositionRate?: number

  dimensions: Record<string, number>

  measuredAt: string
  reportHash: string
}
```

`dimensions` is the set of measured conformance metrics used by the
activation dominance rule (§13.3), every one oriented so that higher is
better. Keys:

```text
locator_coverage
relationship_coverage
caption_pairing_rate     fraction of caption units with at least one
                         caption_of edge
table_decomposition_rate fraction of table units with a table_cell reachable
                         through contains
list_decomposition_rate  fraction of list units with a list_item reachable
                         through contains
section_kind_coverage    fraction of text_section units whose kind is not
                         unknown
```

`list_decomposition_rate` and `section_kind_coverage` are present only when
their subject population is non-empty. The report is persisted in the
canonical parse bundle and logged. Conformance is always measured and always
reported; it gates activation only as defined in §13.

## 13. Parse Activation

Activation is unattended. There are no absolute quality thresholds anywhere
in the activation path.

### 13.1 Binary Structural Invariants (Hard Gate)

A parse fails validation — and is never activated — if any of the following
demonstrable faults hold:

```text
A ContentUnit body does not match its contentType (§15.2).
A declared-capability emission violates the parser's own profile.
Bundle hashes, canonical serialization, or ID assignment rules fail.
Required provenance is missing.
Resource or structural limits are exceeded.
```

These are definitional truths with no thresholds to tune.

### 13.2 Changed Content Activates

For a parse of new content (a `sourceHash` not previously parsed for this
lifecycle), passing the binary invariants is sufficient: the parse activates
automatically. Conformance is measured and logged truthfully; with an
uncontrolled corpus, refusing changed content over a structure-quality dip is
itself an inaccuracy.

### 13.3 Unchanged Content: Dominance Rule

For a re-parse of identical bytes (same `sourceHash`; parser or configuration
changed), the predecessor parse is not stale — keeping it costs nothing in
accuracy. The new parse's conformance report is compared to the active
parse's, dimension by dimension:

```text
Equal or better on every dimension → activate automatically.

Worse on any dimension → hold: the new ParseRun remains ready and
non-queryable with heldReason = conformance_regression; the predecessor
keeps serving; the deltas are logged durably; health surfaces a held count.
```

This is a dominance rule over a partial order, not a threshold. Mixed
results are undecidable by machine and hold by default, safely, because the
fallback is current and correct.

### 13.4 Held-Parse Disposition

An explicit administrative operation accepts (force-activates) or discards a
held parse. Disposition is optional and asynchronous; at most one held
candidate exists per source (newer candidates supersede). A deployment that
never dispositions held parses remains fully correct and merely un-upgraded,
visibly.

### 13.5 Parse Failure Disposition

When a parse of changed content fails the binary invariants, the disposition
is forced: there is no valid new data to serve.

1. The system keeps serving the last valid version.
2. Every failed attempt writes a durable failure record.
3. Health carries a "serving stale due to parse failure" count.
4. Query freshness records state that a change was detected at T and is not
   yet active (§28), so results built on known-superseded content are
   identifiable weeks later.
5. Determinism forbids blind retry: identical bytes through an identical
   parser fail identically. Re-parse occurs only on new `sourceHash` or new
   parser/configuration. Failures coalesce; a churning source holds one
   pending state.

### 13.6 Activation Mechanics

A ParseRun may activate only after required ContentUnits, UnitRelationships,
required SemanticAnnotations (per the required-annotation-set policy, §21.4),
RetrievalProjections, and indexes are built and validated. Activation updates
`SourceObject.activeParseId` as a single logical operation behind the
per-source cutover barrier (§31.1).

## 14. Active Parse Invariants

The hot retrieval system is active-parse-only.

```text
A query path must not read ContentUnits, UnitRelationships,
SemanticAnnotations, or RetrievalProjections unless their parseId equals the
owning SourceObject's activeParseId.
```

All queryable state is parse-scoped. The only exceptions are explicitly
non-production: administrative validation of a non-active ParseRun, and held
parses awaiting disposition — both marked non-queryable build/validation
state.

## 15. Canonical ContentUnit Model

```ts
type ContentUnit<TBody = unknown> = {
  id: string

  sourceId: string
  parseId: string

  contentType: ContentType

  bodyHash: string
  textHash?: string
  structureHash?: string

  primaryParentId?: string
  sequenceIndex?: number

  locators?: Locator[]

  body: TBody

  createdAt: string
  deletedAt?: string
}
```

### 15.1 ContentType

Closed set:

```ts
type ContentType =
  | "document"
  | "page"
  | "text_section"
  | "text_block"
  | "list"
  | "list_item"
  | "aside"
  | "table"
  | "table_row"
  | "table_cell"
  | "figure"
  | "caption"
  | "code_block"
```

Interpretation: `document` is the single root unit of a parse and carries
source metadata; `page` is a print-page marker; `text_section` is a logical
container with a kind and an optional heading; `text_block` is the atomic
textual evidence unit; `list`, `list_item`, and `aside` are containers;
`table`, `table_row`, and `table_cell` are the tabular decomposition;
`figure` is a visual object; `caption` is an independent caption unit;
`code_block` is a code fragment.

Evidence-bearing types, whose text feeds chunking, multi-vector projections,
annotation, and passages: `text_block`, `caption`, `table_cell`,
`code_block`. All other types carry no evidence text.

Text projection for `textHash`: `text` for `text_block`, `caption`, and
`table_cell`; `code` for `code_block`; absent for every other type.

### 15.2 ContentType-to-Body Mapping

Each `contentType` requires its specific body type; a mismatch is invalid and
rejected at creation (a §13.1 invariant):

```text
document → DocumentBody          table → TableBody
page → PageBody                  table_row → TableRowBody
text_section → TextSectionBody   table_cell → TableCellBody
text_block → TextBlockBody       figure → FigureBody
list → ListBody                  caption → CaptionBody
list_item → ListItemBody         code_block → CodeBlockBody
aside → AsideBody
```

Rules: `text_section` is a container, not a paragraph; `text_block` is the
preferred atomic evidence unit; captions are independent ContentUnits; table
cells are first-class; ContentUnits are not chunks; typed bodies preserve
source-derived structure — Markdown renderings are not substitutes.

## 16. Hashing Model and Canonical IDs

### 16.1 Hashes

```ts
type Hashes = {
  sourceHash: string
  bodyHash: string
  textHash?: string
  structureHash?: string
}
```

Rules: `sourceHash` over raw source bytes; `bodyHash` over the canonical
typed body; `textHash` over the normalized textual projection;
`structureHash` over structure excluding volatile metadata. All hashes are
computed from deterministic canonical serialization and exclude volatile
fields (`createdAt`, generated IDs, runtime metrics, storage URIs) unless
explicitly intended.

### 16.2 Canonical Serialization

All structured hash inputs use:

```text
Encoding: UTF-8. Strings NFC-normalized before serialization.
Format: JSON, lexicographically sorted object keys, no extra whitespace.
Arrays: order significant and preserved.
Numbers: canonical JSON representation; no NaN/Infinity/trailing zeros.
Timestamps: UTC RFC3339 with explicit Z.
Optional fields: omitted when absent; explicit null distinct from omission.
Hash algorithm: SHA-256 unless otherwise specified.
```

These rules apply to every content-derived hash in the system, including
`policyHash`, `queryHash`, `planHash`, `manifestHash`, and `reportHash`.

### 16.3 Canonical Artifact Serialization

Canonical parsed state must be exportable as human-readable typed JSON/JSONL
artifact bundles. JSON for singular records; JSONL (UTF-8, LF-separated, one
canonical object per line) for large ordered record sets. Record-set hashes
are computed over canonical lines in declared order, joined by LF; ordering
rules are recorded in the manifest. Large binaries are referenced by URI plus
hash from a human-readable manifest. Markdown/HTML/plain-text are derived
views, never canonical parsed state.

### 16.4 Canonical ID Scheme

IDs are assigned by the core system, never by producers.

```text
SourceObject, ParseRun, AcquisitionRecord, QueryExecutionRecord,
ForensicSnapshot: typed prefix plus time-ordered unique identifier
(e.g., src_..., parse_..., acq_..., qer_..., snap_...). Unique, immutable,
opaque.

ContentUnit and UnitRelationship: parse-scoped deterministic IDs:
parseId + type discriminator + sequence index. Canonical bundles are
therefore bit-reproducible from parser output plus the assignment rule, and
unitId-ascending ordering is stable and meaningful.
```

Rebuilds always re-import from stored artifacts, preserving IDs; IDs are
never re-derived.

## 17. Locators

Locators map ContentUnits back to source positions. Closed union:

```ts
type Locator = DomPathLocator | CharRangeLocator

type DomPathLocator = {
  kind: "dom_path"
  document: string         // package-relative href of the content document
  path: string             // element path, e.g. /html[1]/body[1]/div[1]/p[5]
  elementId?: string       // the element's id attribute when present
  nodeRange?: [number, number]  // inclusive 0-based child-node index range
                                // within the element, counting every node kind
}

type CharRangeLocator = { kind: "char_range"; start: number; end: number }
```

`path` is the sequence of steps from the document element to the target
element, each `/<localname>[<n>]` where `n` is the 1-based index of the
element among siblings with the same local name.

Rules: every evidence-bearing ContentUnit should have at least one locator
when possible; locators are durable canonical provenance; retrieval
projections must not be the only path back to source evidence.

## 18. Typed Bodies

Every body rejects unknown fields. Optional fields are omitted when absent.
No body field may reference another unit; pairing and containment are
relationships only (§16.1). The closed sets `SectionKind`, `TextBlockRole`,
`ListBody.kind`, `AsideBody.kind`, and `TableRowBody.role` are defined once
here; producers declare no parallel copies.

```ts
type DocumentBody = {
  title?: string
  creators?: string[]
  publisher?: string
  language?: string
  identifiers?: string[]
  date?: string
  description?: string
}

type PageBody = {
  ordinal: number          // 1-based position among the parse's page markers
  label?: string           // printed folio as declared, e.g. "xiv", "218"
}

type TextSectionBody = {
  kind: SectionKind
  headingText?: string
  headingLevel: number     // depth in the section tree; children of document = 1
  label?: string           // declared number, e.g. "Chapter 1."
  sectionPath: string[]    // heading trail from level 1 to this section, inclusive
}

type SectionKind =
  | "part" | "chapter" | "section"
  | "preface" | "foreword" | "introduction" | "prologue"
  | "epilogue" | "afterword" | "conclusion"
  | "appendix" | "glossary" | "bibliography" | "index" | "notes"
  | "acknowledgments" | "dedication" | "epigraph"
  | "titlepage" | "copyright_page" | "cover" | "toc" | "colophon"
  | "unknown"

type TextBlockBody = {
  text: string
  role: TextBlockRole
  label?: string           // declared marker inside the block
  language?: string        // BCP 47 tag from the nearest declared language
}

type TextBlockRole =
  | "paragraph" | "heading" | "title" | "subtitle"
  | "term" | "definition"
  | "footnote" | "quote" | "attribution" | "formula" | "unknown"

type ListBody = {
  kind: "ordered" | "unordered" | "definition"
  start?: number           // declared start for ordered lists
}

type ListItemBody = {
  ordinal: number          // 1-based position within the list
  label?: string           // declared marker text when the source renders one
}

type AsideBody = {
  kind: "note" | "tip" | "warning" | "caution" | "important"
      | "sidebar" | "epigraph" | "example" | "unknown"
  title?: string
}

type TableBody = {
  caption?: string
  rowCount: number
  columnCount: number
  headers?: TableHeader[]
}

type TableHeader = {
  rowIndex: number
  columnIndex: number
  text: string
  rowSpan?: number
  columnSpan?: number
}

type TableRowBody = {
  rowIndex: number
  role?: "header" | "body" | "footer"
}

type TableCellBody = {
  rowIndex: number
  columnIndex: number
  rowSpan?: number
  columnSpan?: number
  text?: string
}

type FigureBody = {
  imageHash?: string       // SHA-256 hex of the archived image bytes (§12.2)
  imageMediaType?: string
  imageSizeBytes?: number
  altText?: string
  caption?: string
}

type CaptionBody = {
  text: string
  label?: string           // declared marker inside the caption, e.g. "Figure 1-1."
}

type CodeBlockBody = {
  code: string
  language?: string
  label?: string           // from the paired caption's label, e.g. "Example 2-1."
  title?: string
}
```

`FigureBody.caption`, `TableBody.caption`, and `CodeBlockBody.title` hold the
text of the paired `caption` unit; the pairing itself is the `caption_of` /
`has_caption` relationship pair (§19). The prose roles a passage builder
treats as body text are `paragraph`, `quote`, `definition`, and `unknown`.

## 19. UnitRelationship Model

The canonical structure is a graph; `primaryParentId`/`sequenceIndex` are
convenience fields.

```ts
type UnitRelationship = {
  id: string

  sourceId: string
  parseId: string

  fromUnitId: string
  toUnitId: string

  relationshipType: UnitRelationshipType
  relationshipRole?: string

  sequenceIndex?: number
  confidence?: number

  provenance?: Provenance

  createdAt: string
  deletedAt?: string
}
```

Closed set:

```ts
type UnitRelationshipType =
  | "contains"
  | "precedes"
  | "appears_on"
  | "caption_of"
  | "has_caption"
  | "references"
```

`relationshipRole` values:

```text
references    footnote | cross_reference | index_locator
all others    none
```

Durable canonical relationships are structural: every unit except `document`
has exactly one parent and one `contains` edge from it, and `primaryParentId`
names that parent; siblings under one parent are chained by `precedes` in
reading order; a `caption` unit is `caption_of` its subject and the subject
`has_caption` it; leaf evidence units and figures are `appears_on` each page
whose range they intersect; `references` runs from the evidence unit
containing a link to its target unit. Section resolution walks `contains`
upward to the nearest `text_section`. Semantic or retrieval relationships
(supports-claim, similar-to, relevant-to) must not be canonical
UnitRelationships; they belong in SemanticAnnotation or RetrievalProjection
state. Relationships never cross a source boundary.

## 20. Provenance

```ts
type Provenance = {
  producerType: "parser" | "rule" | "model" | "human" | "system"
  producerName: string
  producerVersion?: string

  configHash?: string
  modelName?: string
  modelVersion?: string
  promptHash?: string

  confidence?: number

  memoized?: boolean
  memoizedFrom?: string
  memoizationKeyHash?: string

  inputRefs?: ProvenanceInputRef[]
}
```

```ts
type ProvenanceInputRef = {
  objectType:
    | "source_object"
    | "acquisition_record"
    | "parse_run"
    | "content_unit"
    | "unit_relationship"
    | "semantic_annotation"
    | "retrieval_projection"
    | "assembly_policy"
  id: string
}
```

Rules: parser-derived structure references parser identity and
configuration; model-derived annotations reference model, prompt/config, and
input units; derived outputs preserve input lineage. The memoization fields
(§21.3) state honestly when a producer was not re-invoked: `memoized: true`
plus `memoizedFrom` referencing the original invocation record. An auditor
always knows whether the model actually ran.

## 21. SemanticAnnotation Model

```ts
type SemanticAnnotation<TBody = unknown> = {
  id: string

  sourceId: string
  parseId: string

  targetUnitIds: string[]

  annotationType: SemanticAnnotationType
  body: TBody

  provenance: Provenance
  confidence?: number

  freshnessStatus: "fresh" | "stale" | "building" | "failed"

  createdAt: string
  deletedAt?: string
}
```

```ts
type SemanticAnnotationType =
  | "entity"
  | "claim"
  | "topic"
  | "summary"
  | "keyword"
  | "classification"
  | "relation"
  | "question_answer"
  | "table_interpretation"
  | "figure_interpretation"
```

Rules:

1. SemanticAnnotations are derived artifacts with provenance, parse-scoped,
   queryable only while their parse is active, snapshotted as forensic state.
2. They are rebuilt within the parse lifecycle, never migrated or re-targeted
   across parser versions.
3. `freshnessStatus` makes post-activation annotation builds visible truth,
   never silent absence.

### 21.1 Memoization Reservations (Deferred Implementation)

Annotation production may be memoized by content, not by unit lineage. The
data-model hooks are normative now; the implementation is deferred.

### 21.2 Memoization Key

```text
memoizationKey = hash(textHash-or-bodyHash of target content (composite,
ordered, for multi-unit targets) + annotationType + producer identity hash)
```

Defined here so any future implementation memoizes identically and cache
entries are portable artifacts.

### 21.3 Eligibility and Honesty

Producers declare memoization eligibility: pure-function-of-target-content
producers are eligible; context-dependent producers (corpus-level entity
linking, whole-document-context summaries) are not, and memoizing them would
be silently wrong. Reused results carry the Provenance memoization fields
(§20) — record-replay honesty for annotation production.

### 21.4 Required-Annotation-Set Policy

Which annotation types block activation versus build after activation with
declared freshness is a visible, versioned policy document (like
AssemblyPolicy), not a tuning knob. Expensive producers should generally be
post-activation; their absence is visible via `freshnessStatus` and health.

## 22. RetrievalProjection Model

`RetrievalProjection` is the envelope; each `projectionType` requires a typed
payload, mirroring the ContentUnit body mapping.

```ts
type RetrievalProjection<TPayload = unknown> = {
  id: string

  sourceId?: string
  parseId?: string

  projectionType: RetrievalProjectionType

  inputUnitIds?: string[]
  inputAnnotationIds?: string[]

  producer: Provenance

  indexName?: string
  indexPartition?: string
  payloadUri?: string
  payload?: TPayload

  freshnessStatus: "fresh" | "stale" | "building" | "failed" | "superseded"

  createdAt: string
  validFrom?: string
  validTo?: string
  deletedAt?: string
}
```

```ts
type RetrievalProjectionType =
  | "lexical_document"
  | "learned_sparse_vector"
  | "dense_vector"
  | "multi_vector"
  | "chunk"
  | "summary"
  | "graph_projection"
  | "temporal_projection"
  | "reranker_feature"
  | "derived_view"
```

The `"chunk"` payload schema:

```ts
type ChunkPayload = {
  inputUnitIds: string[]

  targetingText: string
  tokenCount?: number

  chunkerName: string
  chunkerVersion: string
  chunkerConfigHash: string
}
```

Rules:

1. RetrievalProjections are not canonical evidence; they reference their
   source ContentUnits or SemanticAnnotations; they may be rebuilt, replaced,
   or deleted from hot storage; payloads and replayable index state (or
   deterministic rebuild artifacts) are preserved in forensic snapshots.
2. Retrieval responses ultimately cite ContentUnits, not projections.
3. `derived_view` payloads (Markdown/HTML/prompt renderings) map back to
   canonical records and are never canonical parsed state.

## 23. Chunking Policy

Chunks are retrieval targeting artifacts only.

Rules:

1. Chunk projections may be indexed by any retrieval channel and returned
   internally as hits.
2. A chunk must map back to one or more canonical ContentUnit IDs.
3. Chunk hits are resolved to canonical ContentUnits before EvidencePack
   construction. Chunks never appear in a final EvidencePack. Query answers
   cite canonical ContentUnits.

```text
Chunks help the system aim. Chunks do not define what the model is allowed
to rely on as evidence.
```

## 24. Retrieval Architecture

There is exactly one search executor. The retrieval fabric may use multiple
channels over the active parse: lexical, learned sparse, dense, multi-vector,
graph, semantic, temporal, metadata filtering.

### 24.1 Query Flow

```text
QueryRequest
  ↓
QueryPlanner (deterministic plan compilation; scope resolution)
  ↓
QueryPlan
  ↓
Parallel candidate generation over active parses, scope-filtered at the
candidate source
  ↓
Candidate fusion
  ↓
Reranking
  ↓
Deterministic Context Assembly
  ↓
EvidencePack
  ↓
Optional generation
  ↓
QueryExecutionRecord (written before or atomically with the response)
```

### 24.2 QueryPlanner

The QueryPlanner is a deterministic plan compiler. Defaults come from a
visible, versioned `RetrievalProfile`; the planner makes no autonomous
strategy decisions.

```ts
type RetrievalProfile = {
  id: string
  version: string

  defaultChannels: RetrievalChannel[]
  defaultMaxCandidatesPerChannel: number
  defaultMaxFinalEvidenceUnits: number
  defaultRerank: boolean
  defaultFusionStrategy?: string

  createdAt: string
  profileHash: string
}
```

```ts
type QueryPlan = {
  queryId: string
  queryHash: string

  resolvedChannels: RetrievalChannel[]
  resolvedMaxCandidatesPerChannel: number
  resolvedMaxFinalEvidenceUnits: number
  resolvedRerank: boolean
  resolvedFusionStrategy?: string

  resolvedConstraints?: QueryConstraints
  resolvedFreshness?: FreshnessPolicy
  resolvedScope: ResolvedScope

  sourceRetrievalProfileId?: string
  sourceRetrievalProfileVersion?: string

  planHash: string
}
```

```ts
type ResolvedScope = {
  kind: "all" | "source_set" | "domain_set"
  governanceDomains?: string[]
  sourceIds?: string[]
}
```

`planHash` normatively covers the complete resolved plan: channels, limits,
fusion strategy, rerank flag, resolved constraints, resolved freshness
policy, resolved scope, and profile identity — under §16.2 canonical
serialization. Two executions with different effective parameters can never
share a `planHash`.

Rules: the planner is deterministic (same request + same active profile →
same plan); the resolved plan is recorded in the QueryExecutionRecord; the
active default profile is snapshot state and visible through administrative
APIs.

### 24.3 QueryRequest

```ts
type QueryRequest = {
  queryText: string

  callerContext?: unknown

  constraints?: QueryConstraints
  freshness?: FreshnessPolicy
  retrievalPolicy?: RetrievalPolicy
  evidencePolicy?: EvidencePolicy

  debug?: boolean
}
```

```ts
type QueryConstraints = {
  sourceIds?: string[]
  sourceSystems?: string[]
  governanceDomains?: string[]
  contentTypes?: ContentType[]

  timeRange?: {
    start?: string
    end?: string
    field?: "eventTime" | "ingestTime" | "createdAt"
  }

  metadataFilters?: Record<string, unknown>
}
```

```ts
type FreshnessPolicy = {
  maxIndexLagMs?: number
  allowStaleProjections?: boolean
  requireHotIndex?: boolean
}

type RetrievalPolicy = {
  channels?: RetrievalChannel[]
  maxCandidatesPerChannel?: number
  maxFinalEvidenceUnits?: number
  rerank?: boolean
}

type RetrievalChannel =
  | "lexical" | "learned_sparse" | "dense" | "multi_vector"
  | "graph" | "semantic" | "temporal"

type EvidencePolicy = {
  includeSourceLocators?: boolean
  includeRelationships?: boolean
  includeAnnotations?: boolean
  includeFreshnessMetadata?: boolean
  includeContradictions?: boolean
}
```

`callerContext` is opaque to this system: passed through, recorded in the
QueryExecutionRecord, and reserved as the input to future entitlement
resolution (§6).

### 24.4 RetrievalHit

```ts
type RetrievalHit = {
  hitType: "chunk" | "content_unit" | "semantic_annotation" | "retrieval_projection"

  hitId: string

  sourceId: string
  parseId: string
  unitIds: string[]

  channel: RetrievalChannel
  score: number
  rank?: number

  matchedProjectionId?: string
  matchedAnnotationId?: string

  explanation?: string
}
```

Hits are internal targeting/ranking artifacts, resolved to canonical
ContentUnits before EvidencePack construction. Scope filtering (§6) is
applied at candidate generation in every channel; ranked hits are never
post-filtered for scope.

## 25. AssemblyPolicy

Context assembly converts RetrievalHits into an EvidencePack of canonical
ContentUnits. It must be deterministic, visible, versioned, and traceable.
Generic graph operators may be implemented in code; the policy deciding when
they apply is externalized.

```ts
type AssemblyPolicy = {
  id: string
  version: string
  description?: string

  budgets: AssemblyBudget
  rules: AssemblyRule[]

  requiresRelationshipTypes?: UnitRelationshipType[]

  createdAt: string
  createdBy?: string

  policyHash: string
}
```

```ts
type AssemblyBudget = {
  maxEvidenceUnits: number
  maxTokens: number
  maxExpansionDepth: number
  maxReferencedUnits?: number
}

type AssemblyRule = {
  id: string
  description?: string
  when: AssemblyCondition
  apply: AssemblyOperation[]
  reason:
    | "anchor"
    | "required_completion"
    | "structural_context"
    | "explicit_reference"
    | "local_continuity"
}

type AssemblyCondition = {
  hitType?: "any" | "chunk" | "content_unit" | "semantic_annotation" | "retrieval_projection"
  contentType?: ContentType[]
  hasOutgoingRelationships?: UnitRelationshipType[]
  hasIncomingRelationships?: UnitRelationshipType[]
}

type AssemblyOperation = {
  operator:
    | "include_anchor"
    | "include_parent_container"
    | "include_heading_path"
    | "include_caption_pair"
    | "include_explicit_references"
    | "include_continuation_chain"
    | "include_text_neighbors"
  parameters?: Record<string, unknown>
}
```

Assembly budgets are policy choices reflecting external facts (model context
limits), reviewed with the policy version — they are not internal capacity
guesses.

### 25.1 Policy Dependency Checking

`requiresRelationshipTypes` declares the edge types the policy's rules
operate over. When the active policy requires relationship types that active
parses (per their conformance reports) do not contain, the system emits an
explicit, logged, health-visible warning: the policy is visibly inert in
those respects, never silently no-op.

### 25.2 Assembly Invariants

```text
All EvidencePack construction decisions are attributable to a RetrievalHit
or an AssemblyPolicy rule.

The active AssemblyPolicy is visible through administrative APIs; its id,
version, and hash are recorded in every QueryExecutionRecord; it is
included in ForensicSnapshots together with the operator implementations
required to replay it.

Context assembly must not rely on unconstrained agentic graph traversal.
An agent may propose assembly intent only if final EvidencePack
construction remains governed by AssemblyPolicy and generic operators.
```

## 26. EvidencePack

```ts
type EvidencePack = {
  queryId: string
  queryText: string

  evidenceUnits: EvidenceUnit[]

  relationships?: UnitRelationship[]
  annotations?: SemanticAnnotation[]

  assemblyTrace: ContextAssemblyTrace
  freshness?: EvidenceFreshness

  createdAt: string
}
```

```ts
type EvidenceUnit = {
  unitId: string

  sourceId: string
  parseId: string

  contentType: ContentType
  body: unknown

  textProjection?: string
  locators?: Locator[]

  score?: number
  reasons?: string[]
}

type EvidenceFreshness = {
  maxIndexLagMs?: number
  staleProjectionCount?: number
  hotIndexUsed?: boolean
  lastVerifiedAt?: Record<string, string>
  pendingChangeSources?: string[]
}
```

Rules:

1. `evidenceUnits` contains only canonical ContentUnits from active parses.
2. No ChunkPayloads, raw vectors, or index-native documents as evidence.
3. Relationships and annotations may accompany as supporting context.
4. Every EvidencePack includes its ContextAssemblyTrace.

## 27. ContextAssemblyTrace

```ts
type ContextAssemblyTrace = {
  assemblyPolicyId: string
  assemblyPolicyVersion: string
  assemblyPolicyHash: string

  inputHitIds: string[]

  appliedRules: AppliedAssemblyRule[]

  selectedUnitIds: string[]
  rejectedHitIds?: string[]
  rejectedUnitIds?: string[]

  budget: AssemblyBudget
}

type AppliedAssemblyRule = {
  ruleId: string
  anchorUnitId?: string
  anchorHitId?: string
  addedUnitIds: string[]
  reason:
    | "anchor"
    | "required_completion"
    | "structural_context"
    | "explicit_reference"
    | "local_continuity"
}
```

Every added EvidenceUnit is traceable to a hit or a rule; the trace explains
every non-anchor inclusion; the trace is included in the
QueryExecutionRecord.

## 28. QueryExecutionRecord

Every production query creates an immutable QueryExecutionRecord, written
before or atomically with returning the response. A served response without
a QueryExecutionRecord is a breach of Guarantee 1 (§29.2).

The persisted record embeds the full served EvidencePack, including evidence
bodies. This is deliberate: the record remains self-answering after sources
are deleted from silos and superseded parses leave hot storage — the most
likely audit scenario in an uncontrolled corpus. Storage-layer compression
of the QER store is recommended. If sustained query volume ever makes
embedding uneconomical, reference-style records can be derived from embedded
ones (never the reverse); that migration is mechanical and out of scope.

```ts
type QueryExecutionRecord = {
  id: string

  queryText: string
  queryHash: string

  callerContext?: unknown
  resolvedScope: ResolvedScope

  sourceObjectIds: string[]
  activeParseIds: string[]

  executedAt: string

  queryPlan: QueryPlan

  retrievalTrace: RetrievalTrace
  rankingTrace?: RankingTrace
  contextAssemblyTrace: ContextAssemblyTrace

  evidencePack: EvidencePack
  evidencePackHash: string

  freshnessRecord: QueryFreshnessRecord

  generationTrace?: GenerationTrace
  finalResponse?: string
  finalResponseHash?: string

  retrievalReplayMode: "bit_exact" | "rank_stable" | "record_replay" | "not_supported"

  systemVersion: string
  specVersion: string

  forensicSnapshotId?: string

  createdAt: string
}
```

### 28.1 QueryFreshnessRecord

The truth about index currency at execution time, per in-scope source
system:

```ts
type QueryFreshnessRecord = {
  perSourceSystem: {
    sourceSystem: string
    lastSuccessfulSyncAt?: string
    adaptiveState?: "normal" | "backpressure" | "provider_throttled"
    observedLagMs?: number
  }[]

  pendingDetectedChanges?: {
    sourceId: string
    detectedAt: string
    reason: "queued" | "parse_failed" | "held"
  }[]

  accessLostSources?: {
    sourceId: string
    lastVerifiedAt: string
  }[]
}
```

This record makes staleness attributable weeks later: an inaccuracy caused
by a silo change the index had not yet absorbed — within adaptive operation
or during a parse failure — is distinguishable from a retrieval or assembly
fault, from the record alone.

### 28.2 RetrievalTrace

```ts
type RetrievalTrace = {
  channels: RetrievalChannelTrace[]

  fusionStrategy?: string
  fusedHitIds: string[]
  fusedHitScores?: Record<string, number>
}

type RetrievalChannelTrace = {
  channel: RetrievalChannel

  indexName: string
  indexVersion: string
  projectionVersion: string

  queryPayloadHash: string

  returnedHits: {
    hitId: string
    hitType: "chunk" | "content_unit" | "semantic_annotation" | "retrieval_projection"
    projectionId?: string
    unitIds: string[]
    score: number
    rank: number
  }[]
}
```

### 28.3 RankingTrace

```ts
type RankingTrace = {
  rankerName?: string
  rankerVersion?: string
  rerankerName?: string
  rerankerVersion?: string

  inputHitIds: string[]
  outputHitIds: string[]

  scores?: Record<string, number>
  rankingConfigHash?: string
}
```

### 28.4 GenerationTrace

```ts
type GenerationTrace = {
  modelName: string
  modelVersion?: string

  promptTemplateHash: string
  inputContextHash: string

  parameters: {
    temperature?: number
    topP?: number
    seed?: number
    maxTokens?: number
  }

  requestHash: string
  outputHash: string
}
```

## 29. Replay Model and Audit SLA

### 29.1 Replay Fidelity Modes

Replay claims are graded and per-stage. The vocabulary:

```text
bit_exact
  Identical hits, ordering, and scores under canonical serialization.

rank_stable
  Identical candidate sets; every recomputed score within a declared,
  measured per-channel tolerance ε of the recorded score; ordering
  identical except among near-tie pairs (recorded score gap < ε);
  deterministic tie-breaking within the replay. Requires measured
  tolerances, probe sets, and a conforming numeric environment.

record_replay
  Recorded stage outputs are substituted; no live-recompute claim is made.

not_supported
```

The system must never claim a mode it cannot demonstrate. Rank-stable claims
require the probe/tolerance machinery described in §29.4; absent that
machinery, model-scored stages are declared `record_replay`.

### 29.2 Committed Guarantees

The committed, breach-defined SLA. Each guarantee names its conditions;
"unconditional" means no environment or tolerance conditions.

```text
Guarantee 1 — Execution record. Unconditional.
  Every production query response is backed by an immutable
  QueryExecutionRecord written before or atomically with the response,
  containing the resolved plan, per-channel hits with scores and ranks,
  fusion, assembly trace, embedded EvidencePack, freshness record, and
  content hashes. Breach: a served response without a QER.

Guarantee 2 — Evidence and provenance integrity. Bit-exact; conditional
only on artifact integrity (manifest hashes verify).
  Re-running context assembly from recorded hits reproduces the
  EvidencePack byte-identically (evidencePackHash). Every evidence unit
  hash-chains through its canonical ContentUnit and parse bundle manifest
  to raw source bytes. Re-executing the recorded AssemblyPolicy over
  recorded hits yields identical selected/rejected sets. Breach: any hash
  mismatch.

Guarantee 4 — External record completeness. Unconditional.
  Every external model call (rerank, generation) captures full request and
  response payloads (or hashes per retention policy), provider/model
  identity, and parameters, hash-verified in snapshots. No claim is made
  that the provider reproduces its output; the record of what it said is
  complete and tamper-evident. Breach: missing or incomplete records.
```

The committed retrieval replay mode is `record_replay`: replay substitutes
recorded per-stage outputs. Divergence clause: if any future recompute
diverges from records, the QueryExecutionRecord remains authoritative —
divergence is a finding about environment drift, never an invalidation of
the record.

### 29.3 What the Guarantees Answer

```text
"What specific source data is this search result based on?"
  Guarantee 2: evidence units, locators, hash chain to source bytes,
  acquisition provenance identifying the silo, plus the location and
  deletion records proving where the content lived and when.

"Explain how a weeks-old result was constructed; who or what is to blame?"
  Guarantee 1 + 2 + 4: the blame chain is a record walk — acquisition
  record (was the silo wrong?), conformance report (did the parser mangle
  it?), retrieval and ranking traces (did targeting aim wrong?), assembly
  trace and policy version (did assembly include/exclude wrongly?),
  generation trace (did the model hallucinate beyond its evidence?),
  freshness record (was the index behind the silo, within adaptive
  operation or during a visible failure?).
```

### 29.4 Future Tier: Verified Recompute (Guarantee 3)

Rank-stable live recompute — verified counterfactual replay — is a
documented, optional future tier, not committed. Adopting it requires:
versioned probe query sets; per-channel tolerance ε measured (not invented)
at snapshot verification; numeric-environment capture (accelerator class,
precision, library identities) in snapshot manifests; a divergence
comparator classifying every deviation as near-tie substitution (flagged),
environment nonconformance (claim void), or breach. Until adopted,
counterfactual queries against restored snapshot state are available
best-effort and are explicitly non-evidentiary.

### 29.5 External Model Replay

Exact replay of externally hosted models may be impossible without provider
version pinning and deterministic execution. When not guaranteed, the system
preserves full request/response payloads or hashes per retention policy,
model identity and version when available, provider metadata, generation
parameters, prompt template hash, input context hash, and final output hash
— and declares `generationReplayMode: "record_replay"`. The system must not
claim deterministic generation replay it does not have.

## 30. ForensicSnapshot

### 30.1 Content-Addressed Snapshots

All heavy forensic artifacts are immutable and content-hashed, stored once
in a content-addressed artifact store keyed by hash. A ForensicSnapshot is a
manifest of hash references plus whatever artifacts are new since the last
snapshot. Model weights and runtime artifacts are archived once per version
ever used and referenced thereafter. Frequent automatic snapshots are
therefore cheap by construction.

### 30.2 Snapshot Requirements

A ForensicSnapshot must include or immutably reference:

```text
Raw source objects and acquisition records
Canonical parse artifact bundles (including conformance reports)
Parser output bundles and failure bundles retained by policy
Hot relational state for active parses (or its exact artifact projection)
ContentUnits, UnitRelationships, SemanticAnnotations
RetrievalProjection payloads and projection-to-unit mappings
Lexical/vector/graph index state or deterministic rebuild artifacts
AssemblyPolicy, RetrievalProfile (active defaults), required-annotation-set
  policy, parser and connector capability profiles
Query planner, fusion, ranking, reranking configuration
Prompt templates and tool definitions
Application identity (version/commit, build features, configuration hash)
Model identifiers, versions, and immutable weight references
External model request/response records
QueryExecutionRecords associated with the snapshot interval
Deletion evidence records for the interval
```

### 30.3 Snapshot Schema

```ts
type ForensicSnapshot = {
  id: string

  snapshotType:
    | "scheduled" | "pre_activation" | "post_activation"
    | "pre_deactivation" | "pre_deployment" | "manual" | "incident"

  createdAt: string
  createdBy?: string

  sourceObjectIds: string[]
  activeParseIds: string[]

  manifestUri: string
  manifestHash: string

  systemVersion: string
  specVersion: string

  replayProfile: ReplayProfile

  notes?: string
}
```

```ts
type ReplayProfile = {
  evidenceReplayMode: "bit_exact" | "not_supported"
  retrievalReplayMode: "bit_exact" | "rank_stable" | "record_replay" | "not_supported"
  generationReplayMode: "deterministic" | "record_replay" | "not_supported"

  channelReplayModes?: Record<string, "bit_exact" | "rank_stable" | "record_replay" | "not_supported">
  declaredTolerances?: Record<string, number>
}
```

`declaredTolerances` is present only when a `rank_stable` claim is made
(§29.4). The aggregate `retrievalReplayMode` is the weakest channel's mode.

### 30.4 Snapshot Manifest

```ts
type ForensicSnapshotManifest = {
  snapshotId: string
  createdAt: string

  sourceObjects: SnapshotArtifactRef[]
  acquisitionRecords: SnapshotArtifactRef[]
  parseRuns: SnapshotArtifactRef[]
  canonicalParseBundles: SnapshotArtifactRef[]
  parserOutputBundles?: SnapshotArtifactRef[]
  contentUnits: SnapshotArtifactRef[]
  unitRelationships: SnapshotArtifactRef[]
  semanticAnnotations: SnapshotArtifactRef[]
  retrievalProjections: SnapshotArtifactRef[]
  retrievalIndexes: SnapshotArtifactRef[]
  assemblyPolicies: SnapshotArtifactRef[]
  retrievalProfiles: SnapshotArtifactRef[]
  capabilityProfiles: SnapshotArtifactRef[]
  queryExecutionRecords: SnapshotArtifactRef[]
  deletionRecords?: SnapshotArtifactRef[]
  runtimeArtifacts: SnapshotArtifactRef[]
  modelArtifacts?: SnapshotArtifactRef[]

  manifestHash: string
}

type SnapshotArtifactRef = {
  artifactType: string
  uri: string
  format?: string
  hash: string
  createdAt?: string
  metadata?: Record<string, unknown>
}
```

### 30.5 Verification Tiers

```text
Every snapshot: mechanical verification — manifest completeness and hash
verification of referenced artifacts.

Deletion gate (before superseded hot state is removed): mechanical
verification plus index-rebuild verification (indexes rebuild
deterministically from referenced artifacts).

Scheduled restore drills: full environment restore on a clean workspace —
application identity, restored artifact store, rebuilt indexes — plus
evidence-replay verification (Guarantee 2) over sampled
QueryExecutionRecords. Drills prove restorability on a cadence; they also
rehearse the rollback-restore path (§31).
```

All verification is unattended-capable. A verification failure halts the
affected source's lifecycle only (superseded state is retained until
resolved), surfaces in health, and never silently proceeds to deletion.

### 30.6 Snapshot Triggers

Snapshot creation is an unattended step inside the lifecycles it protects:

```text
Before and after activating a ParseRun.
Before deactivating a source (deletion propagation).
Before deleting superseded parse state from hot storage.
Before material retrieval index rebuilds.
Before model, prompt, ranking, policy, or profile changes.
Before major application deployment.
On manual or incident request.
Scheduled.
```

### 30.7 Replay Environment

An isolated replay environment is: the recorded application identity
(version/commit, build features, configuration hash) plus the restored
artifact store plus deterministically rebuilt indexes. Container or VM
imaging is not required for replay by this specification.

## 31. Activation Cutover, Deletion of Superseded State, and Rollback

### 31.1 Cutover Barrier

Parse activation and source deactivation swap a per-source pointer behind a
brief cutover barrier:

```text
Queries targeting a source whose barrier is active are rejected with a
retryable error and are never partially executed. The barrier covers a
single pointer write and lasts milliseconds. Activation is frequent and
system-initiated; the barrier's non-disruptiveness rests on brevity and
per-source scope, not on frequency or operator timing. Standard client
retry logic is sufficient.
```

In-flight queries execute entirely against their captured pre-cutover
snapshot view.

### 31.2 Superseded-State Lifecycle

When a new ParseRun activates:

```text
1. New parse built, validated, gated (§13), snapshot-referenced.
2. Cutover: activeParseId updated atomically.
3. Post-activation snapshot manifest created.
4. Deletion-gate verification runs (§30.5, mechanical + rebuild check).
5. Superseded hot records and index entries for the previous parseId are
   deleted from live stores. The previous ParseRun becomes archived.
```

The sequence is failure-gated: any verification failure halts before
deletion, retains superseded state, and surfaces in health. There is no
grace window and no auto-retry past a failed gate.

### 31.3 Rollback Is Restore

Rollback repoints a source to a previously archived parse by restoring it
from the artifact store: re-import of canonical rows and projection payloads
(no re-parse, no re-embedding), deterministic index rebuild, then normal
activation through the cutover barrier. Rollback of a parse that reflects a
*source content* change is a deliberate administrative decision to serve
stale reality and is recorded as such. Scheduled restore drills (§30.5) keep
this path exercised.

## 32. Storage Model

Recommended physical storage:

```text
Content-addressed artifact store (object/blob):
  raw sources, acquisition bundles, canonical parse bundles, parser output
  and failure bundles, rendered pages, projection payload archives,
  snapshot manifests, model/runtime artifacts, QueryExecutionRecord
  archives (compressed)

Relational or document database (hot plane):
  SourceObject/SourceLocation, AcquisitionRecord, ParseRun,
  ContentUnit envelopes, UnitRelationship, SemanticAnnotation metadata,
  RetrievalProjection metadata, policies/profiles, QER metadata,
  sync queue state

Search index: lexical and learned sparse projections
Vector index: dense and multi-vectors
Graph index (optional): traversal projection only

Event log:
  acquisition, ingest, parse, gate, activation, deactivation, deletion,
  projection, snapshot, drill, policy change, query execution
```

Storage technology is non-normative; the data contract and exportable
artifact bundles are. The graph database, if used, is a projection; the
canonical relationship source of truth is UnitRelationship storage.
Human-readable canonical artifacts are preferred for source-derived
structured state; binary formats are appropriate for inherently binary data,
referenced from human-readable manifests with hash, size, type, and
provenance.

## 33. Event Model

```ts
type SystemEvent = {
  id: string

  eventType: SystemEventType

  objectType: string
  objectId: string

  payload?: Record<string, unknown>

  createdAt: string
}
```

```ts
type SystemEventType =
  | "acquisition.succeeded"
  | "acquisition.failed"
  | "source.ingested"
  | "source.location_added"
  | "source.location_deleted"
  | "source.access_lost"
  | "source.access_restored"
  | "source.deactivated"
  | "source.reactivated"
  | "parse.started"
  | "parse.ready"
  | "parse.held"
  | "parse.hold_superseded"
  | "parse.accepted"
  | "parse.discarded"
  | "parse.activated"
  | "parse.failed"
  | "parse.archived"
  | "sync.backpressure_entered"
  | "sync.backpressure_exited"
  | "projection.requested"
  | "projection.completed"
  | "projection.failed"
  | "projection.stale"
  | "assembly_policy.changed"
  | "snapshot.started"
  | "snapshot.completed"
  | "snapshot.failed"
  | "drill.completed"
  | "drill.failed"
  | "query.executed"
```

Events drive the autonomous pipeline and the audit trail. They are internal
and audit-facing; operators poll Operations (§34.6) and health for status.

## 34. APIs

The autonomous pipeline is the primary driver of this system. The APIs below
are administrative overrides, integration points, and the query surface —
not the operating model.

### 34.1 Query

```http
POST /query
```

Body: `QueryRequest` (§24.3). Response: the EvidencePack plus the
`queryExecutionRecordId`. Every call produces a QueryExecutionRecord.

### 34.2 Administrative Ingest and Parse Overrides

```http
POST /sources                      # manual registration (integration point)
POST /sources/{sourceId}/parses    # force re-parse (e.g., parser rollout)
POST /sources/{sourceId}/parses/{parseId}/activate   # explicit activation
```

### 34.3 Held-Parse Disposition

```http
GET  /parses?status=held
POST /parses/{parseId}/accept
POST /parses/{parseId}/discard
```

### 34.4 Inspection

```http
GET /units/{unitId}
GET /units/{unitId}/relationships?direction=out&type=contains
GET /sources/{sourceId}                 # includes locations and freshness
GET /query-executions/{qerId}
GET /sync/status                        # queue, cadence, backlog, lag truth
```

### 34.5 Snapshots and Restore

```http
POST /snapshots            # manual/incident snapshot
POST /restore              # rollback/reactivation restore (administrative)
```

### 34.6 Operations

Asynchronous operations (acquisition, parse build, import validation,
activation, projection build, snapshot, restore, drill) create an Operation
record; async calls return an `operationId` for polling.

```ts
type Operation = {
  id: string

  operationType:
    | "acquisition"
    | "parser_execution"
    | "parse_build"
    | "parse_import_validation"
    | "parse_activation"
    | "projection_build"
    | "snapshot_creation"
    | "restore"
    | "drill"
    | "source_ingest"

  status: "pending" | "running" | "succeeded" | "failed"

  targetObjectType: string
  targetObjectId: string

  startedAt?: string
  completedAt?: string

  error?: string

  createdAt: string
}
```

```http
GET /operations/{operationId}
```

## 35. Configuration Principle

```text
Configuration records external facts. It must not encode internal guesses.
```

Legitimate configuration: provider-documented rate limits and quotas,
credentials, endpoints and paths, governance-mandated retention rules,
governance domain assignments, policy documents (assembly, retrieval
profile, required-annotation-set) that are deliberate versioned product
choices.

Prohibited configuration: guessed capacity thresholds, backlog limits,
adaptive-cadence floors/ceilings, retry counts for deterministic failures,
absolute conformance-quality thresholds, deletion-inference scan counts.
Where v0.2 or an implementation would reach for such a knob, the required
design is: adapt from observed signals, log what is true, surface it in
health, and leave alerting thresholds to the operator's monitoring layer.

## 36. MVP Scope

Recommended MVP content types: the full §15.1 set — document, page,
text_section, text_block, list, list_item, aside, table, table_row,
table_cell, figure, caption, code_block.

Recommended MVP relationships: the full §19 set — contains, precedes,
appears_on, caption_of, has_caption, references.

Recommended MVP retrieval projections: lexical_document, chunk,
dense_vector, summary, derived_view. Forward-compatible:
learned_sparse_vector, multi_vector, graph_projection, temporal_projection.

Recommended MVP connectors: filesystem (full_scan), one API-based connector
with incremental detection. Recommended MVP parsers: plain text and EPUB 2/3;
every other format is converted to plain text outside the system.

Recommended MVP behavior:

```text
Autonomous acquisition with coalescing sync queue and adaptive cadence
Isolated parser execution with output-bundle validation
Canonical JSON/JSONL parse bundles with manifest hashing
Typed ContentUnits instead of Markdown-as-truth
Conformance measurement and dominance-gated unattended activation
Parallel lexical + dense retrieval with chunk targeting
Canonical-unit EvidencePack construction under a visible AssemblyPolicy
QueryExecutionRecord (embedded evidence) for every query
Content-addressed snapshot manifests with mechanical verification
Evidence-based deletion propagation
Active-parse-only querying with per-source cutover
```

Do not block the MVP on graph retrieval, multi-vector retrieval, semantic
annotations, agentic assembly, or the verified-recompute replay tier. The
canonical model supports them without requiring them initially.

## 37. Acceptance Criteria

The system is architecturally compliant if:

1. Connectors acquire from external systems as untrusted producers; the core
   validates, hashes, and imports; connector failure cannot mutate serving
   state.
2. Every acquisition attempt, including failures, leaves a durable
   AcquisitionRecord with provenance.
3. SourceObject identity is content-based; presence is location-scoped;
   duplicate content appends locations losslessly; renames are location
   transitions.
4. Detection cadence adapts from observed signals with no configured cadence
   or backlog knobs; pending work is bounded by latest-state coalescing; all
   adaptation is logged with cause.
5. Achieved freshness is measured at named boundaries, health-surfaced, and
   recorded per query in the QueryExecutionRecord.
6. A source has at most one active ParseRun; hot retrieval paths use only
   active-parse data; parse activation and source deactivation are atomic
   behind per-source barriers.
7. Parser upgrades create net-new canonical graphs without unit-level
   lineage mapping.
8. Parses are gated by binary structural invariants and, for identical
   input, by conformance dominance against the predecessor; no absolute
   quality thresholds exist; held parses are visible with explicit
   asynchronous disposition; a never-dispositioned deployment remains
   correct.
9. Parse failure on changed content serves the last valid version with
   durable failure records, health visibility, and per-query staleness
   attribution; deterministic failures are not blindly retried.
10. Deletion is inferred only from qualifying evidence; access-lost is a
    distinct reported state; deletion propagates location-scoped with
    snapshot-before-deactivation; reappearance restores without recompute.
11. ContentUnits are addressable, typed, hashed, and source-locatable;
    structural relationships are durable graph edges; SemanticAnnotations
    carry provenance and freshness status.
12. RetrievalProjections are typed envelope+payload records referencing
    canonical units or annotations; chunks are never returned as final
    evidence; EvidencePacks contain canonical ContentUnits only and include
    ContextAssemblyTrace.
13. Context assembly uses a visible, versioned AssemblyPolicy whose
    relationship-type dependencies are checked against active-parse
    conformance, with unmet dependencies loudly surfaced.
14. Every production query creates an immutable QueryExecutionRecord,
    written before or atomically with the response, embedding the served
    EvidencePack, the resolved plan and scope, all stage traces, and the
    freshness record (SLA Guarantee 1).
15. Evidence replay is bit-exact from records and artifacts alone (SLA
    Guarantee 2); external model calls have complete tamper-evident records
    (SLA Guarantee 4).
16. Replay claims are graded; the committed retrieval replay mode is
    record_replay; no fidelity is claimed that is not demonstrated.
17. ForensicSnapshots are content-addressed manifests over an immutable
    artifact store; every snapshot passes mechanical verification; deletion
    of superseded state is gated on verification including deterministic
    index-rebuild checks; scheduled restore drills demonstrate isolated
    restore plus evidence replay.
18. Rollback is restore-from-store through the normal activation path.
19. Retrieval scope is enforced at candidate generation in every channel;
    ranked results are never post-filtered for scope; callerContext and
    resolved scope are recorded in every QueryExecutionRecord; no per-unit
    ACL logic exists in the hot path.
20. Configuration contains external facts and versioned policy documents
    only; no internal-guess knobs exist.

## 38. Key Prohibitions

```text
Do not make Markdown canonical.

Do not make chunks canonical. Do not allow chunk payloads in EvidencePacks.

Do not make embeddings canonical evidence.

Do not let connectors or parser workers write canonical storage or hot
indexes directly. Do not trust their output without validation and import.

Do not let connector or parser failure affect active serving state.

Do not infer deletion from absence counting or failed scans.

Do not treat access-lost as deletion.

Do not preserve superseded parses in hot storage after verified snapshot
and deletion gating.

Do not attempt unit-level lineage mapping across parser versions.

Do not gate activation on guessed quality thresholds; gate on invariants
and measured dominance only.

Do not retry deterministic failures on unchanged input.

Do not hide context assembly policy inside source code. Do not allow
unconstrained agentic traversal to determine final evidence.

Do not post-filter ranked results for scope; enforce scope at candidate
generation.

Do not add per-unit ACL logic to the hot retrieval path.

Do not encode internal capacity guesses in configuration. Do not add
cadence floors/ceilings or backlog thresholds.

Do not degrade silently. Every adaptation, hold, failure, staleness, and
access loss is logged, health-visible, and query-attributable.

Do not claim replay fidelity that is not demonstrated. Do not rely on
future replay to prove what happened; record QueryExecutionRecords at
execution time.

Do not improvise hard erasure of audit artifacts; it requires a designed,
explicit, audited operation (deferred).
```

## 39. Core Summary

This system continuously and autonomously draws unstructured data from
external, uncontrolled, independently governed silos into one centralized
canonical content graph, and serves a single search executor from
active-parse-only hot state. Raw sources, acquisition provenance, and typed
canonical parses form the durable evidence substrate. Chunks, embeddings,
indexes, and graph projections are retrieval machinery, not evidence. Final
EvidencePacks are assembled from canonical ContentUnits by visible,
deterministic, versioned policy.

The system adapts to source physics and its own capacity without guessed
thresholds, and reports the truth about what it achieved — in logs, health,
and every QueryExecutionRecord. Quality gating is by demonstrable invariant
and measured dominance. Deletion is by evidence. Permissions have a reserved,
normative slot and a deferred implementation.

For audit-grade workloads, every production query leaves an immutable,
self-contained QueryExecutionRecord, and content-addressed forensic
snapshots preserve restorable serving state with honestly graded replay
claims: records prove what happened; snapshots and drills prove the evidence
can be re-derived; nothing is claimed that cannot be demonstrated.
