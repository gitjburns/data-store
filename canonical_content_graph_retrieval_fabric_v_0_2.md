# Canonical Content Graph and Retrieval Fabric

Version: 0.2  
Status: Draft Specification

## 1. Purpose

This specification defines a canonical content graph and retrieval fabric for unstructured and semi-structured data. The system is designed to support high-quality retrieval over large, frequently updated corpora while preserving source provenance, deterministic evidence assembly, and audit-grade forensic replay.

The core design separates durable source-derived truth from retrieval-optimized projections. Source files, parsed content units, structural relationships, semantic annotations, retrieval projections, query execution records, and forensic snapshots are modeled explicitly so that the system can support both near-real-time retrieval and post-event investigation.

The system is intended for workloads where generated outputs may have operational, legal, financial, reputational, or compliance impact.

### 1.1 Specification Notation

Schema blocks in this document describe a language-neutral data model. They are
normative with respect to field names, field meanings, required/optional status,
allowed values, and structural relationships; they are not a requirement to use
any particular programming language, type system, framework, serialization
library, or runtime.

Existing schema blocks use a compact TypeScript-like notation only for
readability. Interpret that notation as follows:

```text
field?: T
  Optional field. Omitted means absent. Explicit null has semantic meaning only
  when the field definition allows it.

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
Buffers, database schemas, C structs, Go structs, Rust structs, or any other
representation that preserves the normative data contract.

Unless otherwise stated, JSON examples in this document are transport examples.
Canonical persisted artifacts are defined by the artifact and serialization
rules in this specification, not by any incidental example formatting.

## 2. Design Principles

The system is governed by the following principles:

```text
A corpus is the security boundary.

The hot system contains only active production truth.

Raw sources and canonical parses are durable.

Canonical parsed state is typed content graph state, not Markdown.

Chunks are targeting artifacts, not evidence.

Final evidence is composed from canonical ContentUnits.

Parser upgrades create net-new canonical graphs.

Parser workers are untrusted producers; the core system validates and imports.

Superseded production state is snapshotted before deletion from hot storage.

Every production query creates an immutable QueryExecutionRecord.

Context assembly behavior must be visible, deterministic, versioned, and traceable.

Forensic replay requires full replayable system state, not merely source documents.
```

## 3. Scope

This specification describes the internal data model, parsing lifecycle, retrieval fabric, context assembly model, query execution logging, and forensic snapshot requirements for a single authorized corpus.

The system supports:

- Immutable source object storage.
- Versioned parse runs.
- Typed canonical content units.
- Durable structural relationships.
- Versioned semantic annotations.
- Disposable but snapshot-preserved retrieval projections.
- Human-readable canonical artifact bundles using typed JSON/JSONL.
- Isolated parser execution through an explicit artifact contract.
- Chunk-based retrieval targeting.
- Deterministic EvidencePack construction.
- QueryExecutionRecords for every production query.
- Full forensic snapshots for audit-grade replay.

## 4. Out of Scope

The following are outside the scope of this specification:

- User authentication.
- User authorization.
- Tenant routing.
- Multi-corpus permission resolution.
- Per-user or per-document ACLs.
- Row-level or unit-level access control.
- Administrative policy for deciding which users may query the corpus.
- Specific implementation languages, web frameworks, parser products,
  container runtimes, queue systems, or deployment platforms.

This specification assumes that any request reaching the corpus has already been authorized by an external control plane.

## 5. Security Model

A corpus is the security boundary.

All objects inside a corpus are assumed visible to any principal authorized to query that corpus. The retrieval system does not perform row-level, document-level, unit-level, or vector-hit-level authorization filtering.

The system therefore does not define `aclId`, `principalId`, `tenantId`, `corpusId`, `securityDomainId`, or `aclContext` fields in the canonical data model.

Security invariants:

```text
Every request reaching this system is assumed authorized for the corpus.

Every object in the corpus is assumed retrievable by an authorized caller.

Retrieval results are not post-filtered for authorization.

No EvidencePack construction step performs ACL filtering.
```

If an implementation requires multiple permission domains, each permission domain should be modeled as a separate corpus outside this specification.

## 6. System Layers

The system consists of the following conceptual layers:

```text
SourceObject
  Raw immutable source object: PDF, DOCX, HTML, image, transcript, code file, etc.

ParseRun
  A parser execution against a SourceObject.

ContentUnit
  A stable, addressable canonical unit derived from a ParseRun.

UnitRelationship
  A durable structural relationship between ContentUnits.

SemanticAnnotation
  A model-, rule-, or human-derived annotation over one or more ContentUnits.

RetrievalProjection
  A retrieval-optimized projection over ContentUnits or SemanticAnnotations.

AssemblyPolicy
  A visible, versioned policy governing deterministic context assembly.

EvidencePack
  A query-time set of canonical ContentUnits assembled for use by a downstream model or caller.

QueryExecutionRecord
  An immutable record of a production query execution.

ForensicSnapshot
  A replayable point-in-time snapshot of the corpus-serving environment.
```

## 7. Durability Classes

### 7.1 Durable Canonical State

Durable canonical state is source-derived and must be preserved as production truth while its parse is active.

Durable canonical state includes:

```text
SourceObject
ParseRun
ContentUnit
UnitRelationship
Locators
Hashes
Parser provenance
Canonical parse artifact manifests
Validated typed JSON/JSONL parse artifacts
```

Markdown, HTML renderings, prompt renderings, and other display/export views are
not durable canonical state unless explicitly modeled as typed ContentUnits or
other canonical records. They may be regenerated from canonical state and may be
preserved for diagnostics, replay, or operator inspection, but they do not define
source-derived truth.

### 7.2 Semi-Durable Semantic State

Semantic state is derived from canonical content, but may depend on models, rules, prompts, extraction policies, or human review. It is persisted, versioned, and audit-relevant.

Semantic state includes:

```text
Entities
Claims
Topics
Summaries
Classifications
Extracted semantic relationships
Table interpretations
Figure interpretations
Question-answer annotations
```

Semantic annotations are not source truth. They are derived artifacts with explicit provenance.

### 7.3 Retrieval Projection State

Retrieval projections are generated artifacts used to target and rank relevant canonical units.

Retrieval projections include:

```text
ChunkProjection
Dense vector payloads
Learned sparse vector payloads
Multi-vector payloads
Lexical index documents
Graph projections
Temporal projections
Summary projections
Reranker features
Generated Markdown/display/search views
```

Retrieval projections are not canonical evidence. However, because this system supports audit-grade replay, retrieval projection payloads and replayable index state must be included in forensic snapshots.

### 7.4 Ephemeral Runtime State

Ephemeral runtime state includes temporary caches, in-memory request state, transient queues, and other artifacts not required to reconstruct query behavior. These may be excluded from forensic snapshots unless required for exact replay by the implementation.

## 8. SourceObject

A SourceObject represents an immutable raw source.

```ts
type SourceObject = {
  id: string

  activeParseId?: string

  sourceSystem?: string
  sourceUri?: string
  sourceVersion?: string

  mimeType: string
  fileName?: string
  sizeBytes?: number

  sourceHash: string
  storageUri: string

  metadata?: Record<string, unknown>

  eventTime?: string
  ingestTime: string
  createdAt: string

  deletedAt?: string
}
```

Rules:

1. `SourceObject` is immutable with respect to raw content.
2. `sourceHash` is a cryptographic hash of the raw source bytes.
3. Raw source bytes are stored in object/blob storage and referenced by `storageUri`.
4. `activeParseId` identifies the only parse considered production truth for this source.
5. Only objects associated with `activeParseId` are queryable through normal retrieval paths.
6. A source update creates a new `SourceObject` or source version according to implementation policy. It does not mutate the raw content of an existing `SourceObject`.
7. Ingest is idempotent. If a source with an identical `sourceHash` already exists, the system must reject the duplicate and return a reference to the existing `SourceObject`.

## 9. ParseRun

A ParseRun represents one parser execution against a SourceObject.

```ts
type ParseRun = {
  id: string
  sourceId: string

  parserName: string
  parserVersion: string
  parserConfigHash: string

  status:
    | "building"
    | "ready"
    | "active"
    | "archiving"
    | "archived"
    | "failed"

  startedAt?: string
  completedAt?: string
  activatedAt?: string
  archivedAt?: string

  snapshotUri?: string
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
  tableCount?: number
  figureCount?: number
  ocrRegionCount?: number
  annotationCount?: number
  projectionCount?: number
}
```

Rules:

1. A SourceObject may have multiple ParseRuns over time.
2. Only one ParseRun may be active for a SourceObject at a time.
3. A non-active ParseRun may be built, annotated, indexed, and validated, but it is not visible to normal query execution.
4. Parser upgrades create net-new canonical graphs.
5. The system does not attempt to preserve unit-level structural lineage across parser versions.
6. A ParseRun may be activated only after required ContentUnits, UnitRelationships, SemanticAnnotations, RetrievalProjections, and indexes have been built and validated.
7. Activation updates `SourceObject.activeParseId` as a single logical operation.
8. Once a new parse is activated, the previous active parse must be forensically snapshotted and removed from hot storage.

### 9.1 Parser Execution Boundary

A parser is any implementation component that converts raw source bytes into
candidate structure. A parser may be a library, CLI, subprocess, sandboxed
program, containerized worker, remote worker, or hosted service. The parser's
implementation language and runtime are outside this specification.

Parsers are outside the hot retrieval trust boundary. A parser worker is an
untrusted producer of candidate artifacts. The core content fabric owns
canonical IDs, validation, hashing, persistence, activation, retrieval
projections, query execution records, and forensic snapshots.

Parser worker rules:

1. Parser workers must not write canonical storage or hot retrieval indexes directly.
2. Parser workers emit artifact bundles into an implementation-defined staging area.
3. Parser output is not canonical until the core system validates and imports it.
4. The core system must validate parser output against the typed content model, locator model, relationship model, provenance requirements, resource limits, and canonical serialization rules before accepting it.
5. Parser failure, timeout, cancellation, crash, excessive output, malformed output, or resource exhaustion must not mutate the active parse or serving state.
6. Parser execution must have explicit timeout and cancellation behavior.
7. Parser execution must run with bounded CPU, memory, disk, output size, page count, unit count, and relationship count according to implementation policy.
8. Parser execution must use a temporary workspace that can be atomically accepted, preserved for diagnostics, or discarded.
9. Parser execution must capture parser name, parser version, parser configuration, parser configuration hash, start time, completion time, warnings, metrics, and failure diagnostics.
10. Parser raw output may be preserved for replay, diagnostics, or counterfactual analysis, but it is not canonical evidence unless imported into validated ContentUnits and UnitRelationships.
11. Implementations should prefer isolated parser execution for complex or failure-prone document formats. Isolation may be provided by subprocesses, containers, sandboxes, worker VMs, process pools with bounded lifetime, or equivalent mechanisms.

### 9.2 Parser Output Bundle

A Parser Output Bundle is the staged, untrusted output of one parser execution.
Its exact physical layout is implementation-defined, but it must contain enough
information for the core system to validate and import a ParseRun without
calling back into parser process memory.

Recommended parser output bundle layout:

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

Rules:

1. `manifest.json` records file hashes, parser identity, parser configuration hash, source hash, output schema version, created time, and bundle hash inputs.
2. Candidate records may use parser-local references. The core system may replace them with canonical IDs during import.
3. Candidate ContentUnits must already be typed; Markdown-only output is insufficient for canonical import.
4. Large binary outputs such as rendered page images, extracted figures, and OCR image regions must be referenced as artifacts with hashes.
5. Parser stdout/stderr and partial artifacts may be preserved in failure bundles for diagnostics.
6. Parser output bundles are not queryable.

### 9.3 Canonical Parse Artifact Bundle

A Canonical Parse Artifact Bundle is the validated, durable, source-derived
artifact set produced by the core system after parser output validation and
canonical import. It is part of durable canonical state and must be included or
immutably referenced by forensic snapshots.

Recommended canonical parse artifact bundle layout:

```text
canonical_parse_bundle/
  manifest.json
  source_object.json
  parse_run.json
  content_units.jsonl
  unit_relationships.jsonl
  semantic_annotations.jsonl
  retrieval_projections.jsonl
  warnings.jsonl
  metrics.json
  artifacts/
```

Rules:

1. The core system, not the parser worker, creates the canonical parse bundle.
2. All records in the canonical parse bundle use canonical IDs.
3. All records are serialized using the canonical JSON/JSONL rules in this specification.
4. `manifest.json` records every file path, artifact type, hash, byte size, schema version, source ID, parse ID, parser identity, parser configuration hash, bundle creation time, and bundle manifest hash.
5. Markdown renderings, HTML renderings, prompt renderings, and plain-text exports may be included as derived view artifacts, but they must not replace typed ContentUnits, UnitRelationships, locators, or provenance.
6. The canonical parse bundle is a replay and audit artifact. Hot relational, lexical, vector, or graph indexes may project from it, but they do not replace it.

## 10. Active Parse Invariants

The hot retrieval system is active-parse-only.

```text
A query path must not read ContentUnits unless ContentUnit.parseId == SourceObject.activeParseId.

A query path must not read UnitRelationships unless UnitRelationship.parseId == SourceObject.activeParseId.

A query path must not read SemanticAnnotations unless SemanticAnnotation.parseId == SourceObject.activeParseId.

A query path must not read RetrievalProjections unless RetrievalProjection.parseId == SourceObject.activeParseId.
```

There are no exceptions for parse-independent objects. All queryable state — ContentUnits, UnitRelationships, SemanticAnnotations, and RetrievalProjections — is parse-scoped.

The only normal exception is administrative validation of a non-active ParseRun before activation. Such validation must be explicitly marked as non-production.

## 11. Canonical ContentUnit Model

A ContentUnit is a stable, addressable canonical unit derived from a ParseRun.

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

### 11.1 ContentType

MVP content types:

```ts
type ContentType =
  | "page"
  | "text_section"
  | "text_block"
  | "table"
  | "table_row"
  | "table_cell"
  | "figure"
  | "caption"
  | "image_region"
  | "code_block"
```

Interpretation:

```text
page:
  Physical page container.

text_section:
  Logical section container.

text_block:
  Evidence-bearing text block such as paragraph, list item, heading, quote, or footnote.

table:
  Tabular container.

table_row:
  Row inside a table.

table_cell:
  Evidence-bearing table cell.

figure:
  Visual object such as chart, diagram, photo, screenshot, or drawing.

caption:
  Textual caption associated with a table or figure.

image_region:
  Region inside an image or figure.

code_block:
  Code fragment or code-like source unit.
```

### 11.2 ContentType-to-Body Mapping

Each `contentType` requires a specific body type. Implementations must enforce this pairing.

```text
page             → PageBody
text_section     → TextSectionBody
text_block       → TextBlockBody
table            → TableBody
table_row        → TableRowBody
table_cell       → TableCellBody
figure           → FigureBody
caption          → CaptionBody
image_region     → ImageRegionBody
code_block       → CodeBlockBody
```

A ContentUnit with a `contentType` that does not match its body schema is invalid and must be rejected at creation time.

Rules:

1. `text_section` is a container and should not be overloaded to mean paragraph.
2. `text_block` is the preferred atomic textual evidence unit.
3. Captions should be independent ContentUnits, not merely fields on figures or tables.
4. Table cells should be first-class ContentUnits when table retrieval matters.
5. ContentUnits are not chunks. Chunks are RetrievalProjections.
6. ContentUnit bodies must preserve typed source-derived structure. A Markdown
   rendering of a page, section, table, figure, or document is not a substitute
   for the corresponding typed body and locator data.

## 12. Hashing Model

The system uses multiple hashes because deduplication, integrity, and change detection operate at different layers.

```ts
type Hashes = {
  sourceHash: string
  bodyHash: string
  textHash?: string
  structureHash?: string
}
```

Rules:

1. `sourceHash` is computed over raw source bytes.
2. `bodyHash` is computed over the canonical typed body.
3. `textHash` is computed over normalized textual projection, when available.
4. `structureHash` is computed over structural representation excluding volatile metadata.
5. Hashes must be computed from deterministic canonical serialization.
6. Hash inputs must exclude volatile fields such as `createdAt`, generated IDs, runtime metrics, and storage URIs unless explicitly intended.

### 12.1 Canonical Serialization

All structured hash inputs must use the following canonical serialization:

```text
Encoding: UTF-8.
Format: JSON with lexicographically sorted object keys and no extraneous whitespace.
Objects: key order is lexicographic by Unicode code point.
Arrays: array order is significant and preserved.
Numbers: canonical JSON number representation; no NaN, Infinity, trailing zeros, or leading plus sign.
Strings: UTF-8 NFC normalization before serialization.
Booleans/null: standard JSON literals.
Timestamps: UTC RFC3339 with explicit `Z` offset.
Optional fields: omitted when absent. Explicit null is distinct from omission.
Hash algorithm: SHA-256 unless otherwise specified.
```

These requirements apply to all hashes in the system: `sourceHash`, `bodyHash`, `textHash`, `structureHash`, `policyHash`, `queryHash`, `manifestHash`, and any other content-derived hash.

### 12.2 Canonical Artifact Serialization

Canonical parsed state must be representable as human-readable typed JSON or
JSON Lines artifacts. Implementations may also store records in relational,
document, columnar, or index-native systems, but the canonical parse state must
be exportable as the artifact bundle defined in this specification.

Rules:

1. Use JSON for singular records such as manifests, SourceObjects, ParseRuns, metrics, policies, query records, and snapshot records.
2. Use JSON Lines for large ordered record sets such as ContentUnits, UnitRelationships, SemanticAnnotations, RetrievalProjections, events, warnings, and query trace entries.
3. Each JSONL line contains exactly one complete canonical JSON object.
4. JSONL files are UTF-8 text. Lines are separated by `LF`.
5. Record-set artifact hashes are computed over the canonical JSON object on each line, in the declared artifact order, joined by `LF`.
6. If an artifact order is semantically meaningful, preserve that order and record the ordering rule in the manifest.
7. If an artifact order is not semantically meaningful, sort records by stable canonical key before hashing and record that key in the manifest.
8. Large binary artifacts such as raw sources, rendered pages, extracted figures, image regions, and index-native files are referenced by URI plus cryptographic hash.
9. Markdown, HTML, plain text, prompt renderings, and other display/export formats are derived views. They may be stored and hashed, but they are not canonical parsed state.
10. Binary canonical formats may be used only where binary representation is materially superior, such as images, model artifacts, vector index files, or columnar exports. Such artifacts must still be referenced from a human-readable manifest.

## 13. Locators

Locators map ContentUnits back to source positions, offsets, paths, or time ranges.

```ts
type Locator =
  | PageBBoxLocator
  | CharRangeLocator
  | ByteRangeLocator
  | TimeRangeLocator
  | DomPathLocator
  | XmlPathLocator
  | TableCellLocator
  | RepoPathLocator
```

```ts
type PageBBoxLocator = {
  kind: "page_bbox"
  pageNumber: number
  bbox: [number, number, number, number]
  coordinateSystem?: "pdf_points" | "pixels" | "normalized"
}

type CharRangeLocator = {
  kind: "char_range"
  start: number
  end: number
}

type ByteRangeLocator = {
  kind: "byte_range"
  start: number
  end: number
}

type TimeRangeLocator = {
  kind: "time_range"
  startMs: number
  endMs: number
}

type DomPathLocator = {
  kind: "dom_path"
  path: string
}

type XmlPathLocator = {
  kind: "xml_path"
  path: string
}

type TableCellLocator = {
  kind: "table_cell"
  rowIndex: number
  columnIndex: number
  rowSpan?: number
  columnSpan?: number
}

type RepoPathLocator = {
  kind: "repo_path"
  path: string
  startLine?: number
  endLine?: number
  commit?: string
}
```

Rules:

1. Every evidence-bearing ContentUnit should have at least one locator when possible.
2. Locators are durable canonical provenance.
3. Locators should support source rendering, highlighting, citation, and audit.
4. Retrieval projections must not be the only path back to source evidence.

## 14. Typed Bodies

### 14.1 PageBody

```ts
type PageBody = {
  pageNumber: number
  width: number
  height: number
  rotation?: number
  renderedImageUri?: string
}
```

### 14.2 TextSectionBody

```ts
type TextSectionBody = {
  headingText?: string
  headingLevel?: number
  sectionPath?: string[]
  normalizedText?: string
}
```

### 14.3 TextBlockBody

```ts
type TextBlockBody = {
  text: string
  normalizedText?: string

  blockRole?:
    | "paragraph"
    | "heading"
    | "list_item"
    | "footnote"
    | "header"
    | "footer"
    | "quote"
    | "formula"
    | "unknown"

  language?: string
}
```

### 14.4 TableBody

```ts
type TableBody = {
  caption?: string
  rowCount: number
  columnCount: number

  headers?: TableHeader[]
  normalizedMarkdown?: string
  normalizedCsvUri?: string
  normalizedHtmlUri?: string
}
```

```ts
type TableHeader = {
  rowIndex?: number
  columnIndex?: number
  text: string
  span?: {
    rowSpan?: number
    columnSpan?: number
  }
}
```

### 14.5 TableRowBody

```ts
type TableRowBody = {
  rowIndex: number
  role?: "header" | "body" | "footer"
}
```

### 14.6 TableCellBody

```ts
type TableCellBody = {
  rowIndex: number
  columnIndex: number
  rowSpan?: number
  columnSpan?: number

  text?: string
  normalizedText?: string

  value?: string | number | boolean | null
  valueType?: "string" | "number" | "date" | "boolean" | "currency" | "unknown"

  headerRefs?: string[]
}
```

### 14.7 FigureBody

```ts
type FigureBody = {
  imageUri?: string
  caption?: string
  altText?: string

  figureType?:
    | "chart"
    | "diagram"
    | "photo"
    | "screenshot"
    | "drawing"
    | "unknown"

  ocrText?: string
}
```

### 14.8 CaptionBody

```ts
type CaptionBody = {
  text: string
  normalizedText?: string
  captionForUnitIds?: string[]
}
```

### 14.9 ImageRegionBody

```ts
type ImageRegionBody = {
  imageUri?: string
  label?: string
  ocrText?: string
  confidence?: number
}
```

### 14.10 CodeBlockBody

```ts
type CodeBlockBody = {
  language?: string
  code: string
  normalizedCode?: string
  startLine?: number
  endLine?: number
}
```

## 15. UnitRelationship Model

The canonical structure is a graph. The `primaryParentId` and `sequenceIndex` fields on ContentUnit are convenience fields, not the complete structural model.

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

```ts
type UnitRelationshipType =
  | "contains"
  | "physically_contains"
  | "logically_contains"
  | "precedes"
  | "follows"
  | "appears_on"
  | "caption_of"
  | "has_caption"
  | "references"
  | "continues_on"
  | "derived_from"
```

Durable canonical relationships include:

```text
page contains block
section contains paragraph
table contains row
row contains cell
figure has_caption caption
caption caption_of figure
paragraph precedes paragraph
unit appears_on page
paragraph references figure
block continues_on block
```

Semantic or retrieval relationships should not be stored as canonical UnitRelationships.

Examples that should not be canonical UnitRelationships:

```text
paragraph supports claim
unit discusses topic
unit is similar_to unit
unit is relevant_to query
unit belongs_to cluster
```

These belong in SemanticAnnotation or RetrievalProjection state.

## 16. Provenance

All derived objects should carry provenance.

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

  inputRefs?: ProvenanceInputRef[]
}
```

```ts
type ProvenanceInputRef = {
  objectType:
    | "source_object"
    | "parse_run"
    | "content_unit"
    | "unit_relationship"
    | "semantic_annotation"
    | "retrieval_projection"
    | "assembly_policy"

  id: string
}
```

Rules:

1. Parser-derived structure must reference parser name, version, and configuration.
2. Model-derived annotations must reference model, prompt/config, and input units.
3. Human-reviewed annotations should include human review provenance.
4. Derived outputs must preserve input lineage.

## 17. SemanticAnnotation Model

SemanticAnnotation records model-, rule-, or human-derived meaning over one or more ContentUnits.

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

1. SemanticAnnotations are derived artifacts, not canonical source truth.
2. SemanticAnnotations must include provenance.
3. SemanticAnnotations must be snapshotted as part of forensic state.
4. SemanticAnnotations are parse-scoped and queryable only when their parse is active.
5. SemanticAnnotations are rebuilt as part of the parse build lifecycle. They are not migrated or re-targeted across parser versions.

## 18. RetrievalProjection Model

RetrievalProjection records generated artifacts used for retrieval targeting, ranking, or query-time assembly.

```ts
type RetrievalProjection = {
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

Rules:

1. RetrievalProjections are not canonical evidence.
2. RetrievalProjections must reference their source ContentUnits or SemanticAnnotations.
3. RetrievalProjections may be rebuilt, replaced, or deleted from hot storage.
4. RetrievalProjection payloads and sufficient replayable index state must be preserved in ForensicSnapshots.
5. Retrieval responses should ultimately cite ContentUnits, not projection records.
6. A `derived_view` projection may contain Markdown, HTML, plain text, or prompt-oriented renderings for inspection, search, or downstream formatting. It is not canonical parsed state and must map back to ContentUnits or SemanticAnnotations.

## 19. Chunking Policy

Chunks are retrieval targeting artifacts only.

```ts
type ChunkProjection = {
  id: string

  sourceId: string
  parseId: string

  inputUnitIds: string[]

  targetingText: string
  tokenCount?: number

  chunkerName: string
  chunkerVersion: string
  chunkerConfigHash: string

  createdAt: string
}
```

Rules:

1. A ChunkProjection may be indexed by lexical, sparse, dense, or multi-vector retrieval systems.
2. A ChunkProjection may be returned internally as a retrieval hit.
3. A ChunkProjection must not appear in the final EvidencePack.
4. Every ChunkProjection must map back to one or more canonical ContentUnit IDs.
5. After candidate retrieval and ranking, chunk hits must be resolved to canonical ContentUnits before EvidencePack construction.
6. Query answers should cite canonical ContentUnits, not chunks.

Invariant:

```text
Chunks help the system aim. Chunks do not define what the model is allowed to rely on as evidence.
```

## 20. Retrieval Architecture

The retrieval fabric may use multiple channels over the active parse:

```text
lexical retrieval
learned sparse retrieval
dense vector retrieval
multi-vector retrieval
graph projection retrieval
temporal filtering
semantic annotation retrieval
metadata filtering
```

### 20.1 Query Flow

```text
QueryRequest
  ↓
QueryPlanner (deterministic plan compilation)
  ↓
QueryPlan
  ↓
Parallel candidate generation over active parse
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
QueryExecutionRecord
```

### 20.1.1 QueryPlanner

The QueryPlanner is a deterministic plan compiler. It compiles a `QueryRequest` and its `RetrievalPolicy` into a concrete `QueryPlan`.

The QueryPlanner does not make autonomous retrieval strategy decisions. Any default channel selection, query rewriting, fusion strategy, reranking behavior, candidate limits, or fallback behavior must be defined by the caller-provided `RetrievalPolicy` or by a visible, versioned default `RetrievalProfile`.

If the caller omits `RetrievalPolicy`, the QueryPlanner applies the active default `RetrievalProfile`. The active default profile must be visible through administrative APIs.

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

  sourceRetrievalProfileId?: string
  sourceRetrievalProfileVersion?: string

  planHash: string
}
```

Rules:

1. The QueryPlanner is deterministic. Given the same `QueryRequest` and the same active `RetrievalProfile`, it must produce the same `QueryPlan`.
2. The resolved `QueryPlan` must be recorded in the `QueryExecutionRecord`.
3. The active default `RetrievalProfile` must be included in `ForensicSnapshot`.

### 20.2 QueryRequest

```ts
type QueryRequest = {
  queryText: string

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
  sourceTypes?: string[]
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
```

```ts
type RetrievalPolicy = {
  channels?: RetrievalChannel[]
  maxCandidatesPerChannel?: number
  maxFinalEvidenceUnits?: number
  rerank?: boolean
}
```

```ts
type RetrievalChannel =
  | "lexical"
  | "learned_sparse"
  | "dense"
  | "multi_vector"
  | "graph"
  | "semantic"
  | "temporal"
```

```ts
type EvidencePolicy = {
  includeSourceLocators?: boolean
  includeRelationships?: boolean
  includeAnnotations?: boolean
  includeFreshnessMetadata?: boolean
  includeContradictions?: boolean
}
```

### 20.3 RetrievalHit

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

Rules:

1. RetrievalHits may point to chunks, units, annotations, or other projection records.
2. RetrievalHits are internal targeting/ranking artifacts.
3. RetrievalHits must be resolved into canonical ContentUnits before EvidencePack construction.

## 21. AssemblyPolicy

Context assembly converts RetrievalHits into an EvidencePack of canonical ContentUnits.

Context assembly must be deterministic, visible, versioned, and traceable. Behavior that affects EvidencePack contents must not be hidden only in source code.

The implementation may hard-code generic graph operators, but the policy deciding when those operators apply must be externalized as a visible AssemblyPolicy.

### 21.1 AssemblyPolicy Schema

```ts
type AssemblyPolicy = {
  id: string
  version: string
  description?: string

  budgets: AssemblyBudget
  rules: AssemblyRule[]

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
```

```ts
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
```

```ts
type AssemblyCondition = {
  hitType?: "any" | "chunk" | "content_unit" | "semantic_annotation" | "retrieval_projection"
  contentType?: ContentType[]
  hasOutgoingRelationships?: UnitRelationshipType[]
  hasIncomingRelationships?: UnitRelationshipType[]
}
```

```ts
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

### 21.2 Generic Graph Operators

Generic operators may be implemented in source code. Their use must be controlled by AssemblyPolicy.

Operators:

```text
include_anchor:
  Include the canonical ContentUnit or ContentUnits targeted by the RetrievalHit.

include_parent_container:
  Include the nearest structurally complete parent container, such as table for table_cell or figure/table for caption.

include_heading_path:
  Include section heading path or logical heading context for textual units.

include_caption_pair:
  Include caption associated with figure/table or figure/table associated with caption.

include_explicit_references:
  Include units connected by explicit reference relationships.

include_continuation_chain:
  Include units connected by continues_on relationships.

include_text_neighbors:
  Include preceding or succeeding text blocks according to policy limits.
```

### 21.3 Example AssemblyPolicy

```yaml
id: default_context_assembly
version: 0.2.0
description: Default deterministic small-to-big context assembly policy.

budgets:
  maxEvidenceUnits: 24
  maxTokens: 12000
  maxExpansionDepth: 2
  maxReferencedUnits: 5

rules:
  - id: include_anchor
    when:
      hitType: any
    apply:
      - operator: include_anchor
    reason: anchor

  - id: complete_table_child
    when:
      contentType:
        - table_cell
        - table_row
    apply:
      - operator: include_parent_container
      - operator: include_caption_pair
    reason: required_completion

  - id: complete_caption
    when:
      contentType:
        - caption
    apply:
      - operator: include_parent_container
    reason: required_completion

  - id: complete_figure
    when:
      contentType:
        - figure
    apply:
      - operator: include_caption_pair
    reason: required_completion

  - id: text_block_heading_context
    when:
      contentType:
        - text_block
    apply:
      - operator: include_heading_path
    reason: structural_context

  - id: explicit_references
    when:
      hasOutgoingRelationships:
        - references
        - continues_on
    apply:
      - operator: include_explicit_references
      - operator: include_continuation_chain
    reason: explicit_reference
```

### 21.4 Assembly Invariants

```text
All EvidencePack construction decisions must be attributable to a RetrievalHit or an AssemblyPolicy rule.

The active AssemblyPolicy must be visible through administrative APIs.

The active AssemblyPolicy id, version, and hash must be recorded in every QueryExecutionRecord.

A ForensicSnapshot must include the active AssemblyPolicy and operator implementations required to replay it.

Context assembly must not rely on unconstrained agentic graph traversal.

An agent may propose assembly intent only if final EvidencePack construction remains governed by AssemblyPolicy and generic operators.
```

## 22. EvidencePack

An EvidencePack is the final assembled evidence payload produced by retrieval and context assembly.

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
```

```ts
type EvidenceFreshness = {
  maxIndexLagMs?: number
  staleProjectionCount?: number
  hotIndexUsed?: boolean
}
```

Rules:

1. `EvidencePack.evidenceUnits` must contain only canonical ContentUnits from the active parse.
2. EvidencePack must not contain ChunkProjection payloads.
3. EvidencePack must not contain raw embedding vectors, sparse vectors, ANN records, or index-native documents as evidence.
4. EvidencePack may include relationships and annotations as supporting context, but canonical ContentUnits remain the evidence substrate.
5. EvidencePack must include ContextAssemblyTrace.

## 23. ContextAssemblyTrace

ContextAssemblyTrace records how retrieval hits became evidence.

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
```

```ts
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

Rules:

1. Every added EvidenceUnit must be traceable to a retrieval hit or AssemblyPolicy rule.
2. The trace must explain why each non-anchor unit was included.
3. The trace must be included in QueryExecutionRecord.

## 24. QueryExecutionRecord

Every production query must create an immutable QueryExecutionRecord.

The QueryExecutionRecord proves what happened during a production query execution. It is not a substitute for forensic replay; it is the authoritative event record used with forensic snapshots to support investigation.

```ts
type QueryExecutionRecord = {
  id: string

  queryText: string
  queryHash: string

  sourceObjectIds: string[]
  activeParseIds: string[]

  executedAt: string

  queryPlan: QueryPlan

  retrievalTrace: RetrievalTrace
  rankingTrace?: RankingTrace
  contextAssemblyTrace: ContextAssemblyTrace

  evidencePack: EvidencePack
  evidencePackHash: string

  generationTrace?: GenerationTrace
  finalResponse?: string
  finalResponseHash?: string

  systemVersion: string
  specVersion: string

  forensicSnapshotId?: string

  createdAt: string
}
```

### 24.1 RetrievalTrace

```ts
type RetrievalTrace = {
  channels: RetrievalChannelTrace[]

  fusionStrategy?: string
  fusedHitIds: string[]
  fusedHitScores?: Record<string, number>
}
```

```ts
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

### 24.2 RankingTrace

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

### 24.3 GenerationTrace

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

Rules:

1. QueryExecutionRecord is immutable.
2. QueryExecutionRecord must include retrieval hits, ranks, scores, context assembly trace, final EvidencePack, and hashes.
3. If generation occurs, QueryExecutionRecord must include generation trace and output hash.
4. QueryExecutionRecord must record enough version metadata to correlate the execution with a ForensicSnapshot.
5. QueryExecutionRecord should be written before or atomically with returning the production response where feasible.

## 25. ForensicSnapshot

A ForensicSnapshot is an immutable point-in-time snapshot of the complete replayable corpus-serving environment.

The ForensicSnapshot enables investigation, replay, and counterfactual analysis. It must preserve enough state to restore an isolated replay environment capable of executing historical queries against the same corpus-serving state.

### 25.1 Snapshot Requirements

A ForensicSnapshot must include or immutably reference:

```text
Raw source objects
Canonical parse artifact bundles
Parser output bundles and failure bundles retained by policy
Hot relational database state for active parses
Canonical ContentUnits
UnitRelationships
SemanticAnnotations
RetrievalProjection payloads
ChunkProjection records
Dense vector payloads
Learned sparse vector payloads
Multi-vector payloads
Lexical/search index state or deterministic rebuild artifacts
Vector index state or deterministic rebuild artifacts
Graph index state or deterministic rebuild artifacts, if used
Projection-to-ContentUnit mappings
AssemblyPolicy
RetrievalProfile (active default)
Graph operator implementations
Query planner configuration
Fusion/ranking/reranking configuration
Prompt templates
Tool definitions
Application version
Parser worker artifacts, versions, configurations, and isolation metadata required for replay
Container/runtime/deployment artifacts where required for replay
Model identifiers and version metadata
Self-hosted model weights or immutable model artifact references, if applicable
External model request/response records where exact model replay is not guaranteed
QueryExecutionRecords associated with the snapshot interval or state
```

### 25.2 Snapshot Schema

```ts
type ForensicSnapshot = {
  id: string

  snapshotType: "scheduled" | "pre_activation" | "post_activation" | "pre_deployment" | "manual" | "incident"

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
  supportsEvidenceReplay: boolean
  supportsRetrievalReplay: boolean
  supportsGenerationReplay: boolean
  generationReplayMode?: "deterministic" | "record_replay" | "not_supported"
}
```

### 25.3 Snapshot Manifest

```ts
type ForensicSnapshotManifest = {
  snapshotId: string

  createdAt: string

  sourceObjects: SnapshotArtifactRef[]
  parseRuns: SnapshotArtifactRef[]
  canonicalParseBundles: SnapshotArtifactRef[]
  parserOutputBundles?: SnapshotArtifactRef[]
  contentUnits: SnapshotArtifactRef[]
  unitRelationships: SnapshotArtifactRef[]
  semanticAnnotations: SnapshotArtifactRef[]
  retrievalProjections: SnapshotArtifactRef[]
  retrievalIndexes: SnapshotArtifactRef[]
  assemblyPolicies: SnapshotArtifactRef[]
  queryExecutionRecords: SnapshotArtifactRef[]
  runtimeArtifacts: SnapshotArtifactRef[]
  modelArtifacts?: SnapshotArtifactRef[]

  manifestHash: string
}
```

```ts
type SnapshotArtifactRef = {
  artifactType: string
  uri: string
  format?: string
  hash: string
  createdAt?: string
  metadata?: Record<string, unknown>
}
```

### 25.4 Snapshot Triggers

The system must support forensic snapshot creation at minimum for:

```text
Before activating a new ParseRun.
After activating a new ParseRun.
Before deleting superseded parse state from hot storage.
Before material retrieval index rebuilds.
Before model, prompt, ranking, or AssemblyPolicy changes.
Before major application deployment.
On manual operator request.
On incident response request.
```

Implementations may also create scheduled snapshots.

### 25.5 Snapshot and Deletion Lifecycle

When a new ParseRun is activated for a SourceObject:

```text
1. The new ParseRun is built, annotated, indexed, and validated.
2. A pre-activation ForensicSnapshot is created if required by policy.
3. The system enters a brief cutover barrier. New query admission is paused or rejected, and any in-flight queries that cannot be guaranteed to execute entirely against either the pre-cutover or post-cutover state are rejected with a retryable error. `SourceObject.activeParseId` is then atomically updated to the new ParseRun, and query admission resumes only after the cutover is complete.
4. A ForensicSnapshot preserving the previous replayable serving state is created and verified.
5. Superseded hot records and indexes associated with the previous parseId are deleted from live relational, lexical, vector, graph, and cache stores.
6. The previous ParseRun status becomes archived.
```

#### Cutover Barrier Characteristics

Parse activation is an infrequent, operator-initiated infrastructure operation — comparable in frequency to a database schema migration. The cutover barrier (step 3) covers only the atomic pointer swap of `SourceObject.activeParseId`, which is a single database write. The expensive work — building the new parse, indexing projections, creating forensic snapshots — occurs before and after the barrier, not during it.

Because the barrier is infrequent, brief, and operator-timed, rejecting queries with a retryable error during the barrier is non-disruptive for any production profile. Standard client retry logic is sufficient.

#### Multi-Source Query Atomicity

A query that targets any source whose activation barrier is active is rejected with a retryable error. The query is not partially executed. This applies regardless of how many sources the query targets.

#### Failure Gating

The activation lifecycle is sequential and failure-gated. Each step requires verified success of the prior step. A failure at any point halts the sequence. There is no auto-retry and no step skipping.

If the forensic snapshot (step 4) fails, superseded parse data remains in hot storage until an operator resolves the failure. Step 5 (deletion) must not execute until step 4 has succeeded and been verified.

Rules:

1. Superseded parses are not queryable through normal retrieval paths.
2. Cold snapshots are outside the hot retrieval plane.
3. Hot storage should contain only active production truth plus explicitly non-queryable build/validation state.
4. Full-system snapshotting is the primary audit mechanism.
5. Logical export manifests may be used as an implementation detail, but are not a substitute for replayable forensic state.
6. Retention, archival, and tiering of ForensicSnapshots and QueryExecutionRecords are infrastructure-level concerns outside the scope of this specification. This specification defines creation and immutability requirements; lifecycle management beyond that is implementation-defined.

## 26. External Model Replay

Exact replay of externally hosted models may be impossible unless the provider guarantees strict version pinning and deterministic execution.

When exact model replay is not guaranteed, the system must preserve:

```text
Full model request payload or request hash according to data retention policy.
Full model response payload or response hash according to data retention policy.
Model identifier.
Model version, if available.
Provider metadata.
Generation parameters.
Prompt template hash.
Input context hash.
Final output hash.
```

In this case, generation replay mode should be marked as `record_replay` rather than `deterministic`.

The system must not falsely claim deterministic generation replay when it only preserves request/response records.

## 27. Storage Model

Recommended physical storage:

```text
Object/blob storage:
  raw source files
  canonical parse artifact bundles (typed JSON/JSONL plus manifest)
  parser output bundles and failure bundles retained by policy
  rendered page images
  large canonical bodies
  normalized table exports
  derived Markdown/HTML/plain-text views
  vector payload archives
  forensic snapshots
  model/runtime artifacts

Relational or document database:
  SourceObject
  ParseRun
  ContentUnit envelopes
  UnitRelationship
  SemanticAnnotation metadata
  RetrievalProjection metadata
  AssemblyPolicy metadata
  QueryExecutionRecord metadata

Search index:
  lexical and learned sparse projections

Vector index:
  dense vectors and multi-vectors

Graph index:
  optional projection for traversal

Event log:
  ingest, parse, annotation, projection, activation, snapshot, deletion, query execution, policy change
```

The graph database, if used, is a projection. The canonical relationship source of truth remains UnitRelationship storage.

Human-readable canonical artifacts are preferred for source-derived structured
state. Binary artifact formats are appropriate for inherently binary data,
large numeric/index payloads, and runtime artifacts where text representation is
materially inferior. Every binary artifact must be referenced from a
human-readable manifest with hash, size, type, and provenance metadata.

Markdown is a derived view/export format. It may be useful for operator
inspection, prompt rendering, simple lexical indexing, or debugging, but it must
not be the canonical parse artifact and must not be the only path back to source
evidence.

## 28. Event Model

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
  | "source.ingested"
  | "source.deleted"
  | "parse.started"
  | "parse.ready"
  | "parse.activated"
  | "parse.failed"
  | "parse.archived"
  | "unit.created"
  | "relationship.created"
  | "annotation.created"
  | "projection.requested"
  | "projection.completed"
  | "projection.failed"
  | "projection.stale"
  | "assembly_policy.changed"
  | "snapshot.started"
  | "snapshot.completed"
  | "snapshot.failed"
  | "query.executed"
```

Events drive asynchronous parsing, annotation, indexing, activation, snapshotting, deletion, and audit workflows.

## 29. APIs

### 29.1 Ingest Source

```http
POST /sources
```

```json
{
  "sourceUri": "s3://bucket/file.pdf",
  "mimeType": "application/pdf",
  "fileName": "contract.pdf",
  "metadata": {
    "customer": "Acme"
  }
}
```

Response:

```json
{
  "sourceId": "src_123",
  "sourceHash": "sha256:...",
  "status": "accepted"
}
```

### 29.2 Create ParseRun

```http
POST /sources/{sourceId}/parses
```

```json
{
  "parserName": "pdf_parser",
  "parserVersion": "2.4.1",
  "parserConfigHash": "sha256:..."
}
```

Response:

```json
{
  "parseId": "parse_123",
  "status": "building"
}
```

### 29.3 Activate ParseRun

```http
POST /sources/{sourceId}/parses/{parseId}/activate
```

Response:

```json
{
  "sourceId": "src_123",
  "activeParseId": "parse_123",
  "previousParseId": "parse_122",
  "status": "active"
}
```

### 29.4 Get ContentUnit

```http
GET /units/{unitId}
```

### 29.5 Traverse Relationships

```http
GET /units/{unitId}/relationships?direction=out&type=contains
```

### 29.6 Query

```http
POST /query
```

```json
{
  "queryText": "Who owns customer data under this agreement?",
  "constraints": {
    "sourceIds": ["src_123"],
    "contentTypes": ["text_block", "table_cell", "caption"]
  },
  "freshness": {
    "maxIndexLagMs": 30000,
    "allowStaleProjections": false
  },
  "retrievalPolicy": {
    "channels": ["learned_sparse", "dense", "multi_vector"],
    "maxCandidatesPerChannel": 100,
    "maxFinalEvidenceUnits": 12,
    "rerank": true
  },
  "evidencePolicy": {
    "includeSourceLocators": true,
    "includeRelationships": true,
    "includeAnnotations": true,
    "includeFreshnessMetadata": true
  }
}
```

Response:

```json
{
  "queryExecutionRecordId": "qer_123",
  "evidencePack": {
    "queryId": "q_123",
    "queryText": "Who owns customer data under this agreement?",
    "evidenceUnits": [],
    "assemblyTrace": {},
    "createdAt": "2026-05-19T00:00:00Z"
  }
}
```

### 29.7 Create ForensicSnapshot

```http
POST /snapshots
```

```json
{
  "snapshotType": "manual",
  "notes": "Pre-deployment forensic snapshot"
}
```

Response:

```json
{
  "snapshotId": "snap_123",
  "status": "started"
}
```

### 29.8 Get QueryExecutionRecord

```http
GET /query-executions/{queryExecutionRecordId}
```

### 29.9 Get Operation Status

Asynchronous infrastructure operations (parse building, projection generation, snapshot creation) must create an `Operation` record. Async API calls must return an `operationId`. Callers may poll for status.

```ts
type Operation = {
  id: string

  operationType:
    | "parser_execution"
    | "parse_build"
    | "parse_import_validation"
    | "parse_activation"
    | "projection_build"
    | "snapshot_creation"
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

Response:

```json
{
  "operationId": "op_123",
  "operationType": "parse_build",
  "status": "running",
  "targetObjectType": "parse_run",
  "targetObjectId": "parse_123",
  "startedAt": "2026-05-19T00:00:00Z",
  "createdAt": "2026-05-19T00:00:00Z"
}
```

The event model (§28) is internal and audit-facing. It is not the primary operator status API. Operators poll `GET /operations/{operationId}` for status of asynchronous operations.

## 30. MVP Scope

Recommended MVP content types:

```text
page
text_section
text_block
table
table_cell
figure
caption
```

Recommended MVP relationships:

```text
contains
logically_contains
physically_contains
precedes
appears_on
caption_of
has_caption
references
continues_on
```

Recommended MVP retrieval projections:

```text
lexical_document
chunk
dense_vector
summary
derived_view
```

Forward-compatible projections:

```text
learned_sparse_vector
multi_vector
graph_projection
temporal_projection
```

Recommended MVP parsers:

```text
PDF
DOCX
HTML
plain text
image with OCR
```

Recommended MVP canonical artifact behavior:

```text
isolated parser execution
parser output bundle validation
canonical JSON/JSONL parse bundles
typed ContentUnits instead of Markdown-as-truth
manifest hashing for parse bundles and referenced artifacts
failure bundle preservation for parser diagnostics
```

Recommended MVP query behavior:

```text
parallel lexical + dense retrieval
chunk targeting
canonical-unit EvidencePack construction
visible deterministic AssemblyPolicy
QueryExecutionRecord creation
forensic snapshot support
active-parse-only querying
```

Do not block the MVP on graph retrieval, multi-vector retrieval, or agentic assembly. The canonical model should support them without requiring them initially.

## 31. Acceptance Criteria

The system is architecturally compliant if:

1. A raw source can be ingested as an immutable SourceObject.
2. A source can have exactly one active ParseRun at query time.
3. A new ParseRun can be built without becoming query-visible.
4. Parse activation is atomic.
5. Parser upgrades create net-new canonical graphs without unit-level lineage mapping.
6. Superseded production state is forensically snapshotted before hot deletion.
7. Hot retrieval paths use only active parse data.
8. ContentUnits are addressable, typed, hashed, and source-locatable.
9. Structural relationships are represented as durable graph edges.
10. SemanticAnnotations carry provenance.
11. RetrievalProjections reference canonical units or annotations.
12. Chunks are never returned as final evidence.
13. EvidencePacks contain canonical ContentUnits only.
14. Context assembly uses a visible, versioned AssemblyPolicy.
15. Every EvidencePack includes ContextAssemblyTrace.
16. Every production query creates an immutable QueryExecutionRecord.
17. ForensicSnapshots preserve replayable corpus-serving state.
18. The system can restore an isolated replay environment from a ForensicSnapshot, subject to external model replay limitations.
19. External model replay limitations are explicitly represented as deterministic replay or record-replay.
20. No row-level or unit-level ACL logic exists in the hot retrieval path.
21. Parser workers are isolated from canonical storage and hot retrieval indexes.
22. Parser output is validated before canonical import.
23. The core system owns canonical IDs, hashes, artifact manifests, persistence, and activation.
24. Canonical parsed state is available as typed JSON/JSONL artifact bundles with manifest hashes.
25. Markdown and other renderings are treated as derived views, not canonical parsed state.
26. Parser failure, timeout, crash, cancellation, or malformed output cannot mutate active serving state.

## 32. Key Prohibitions

```text
Do not make Markdown canonical.

Do not make chunks canonical.

Do not make embeddings canonical evidence.

Do not allow ChunkProjection payloads in EvidencePack.

Do not perform row-level authorization filtering inside retrieval.

Do not preserve old parses in hot storage after snapshotting and deletion.

Do not attempt brittle unit-level lineage mapping across parser versions.

Do not hide context assembly policy inside source code.

Do not allow unconstrained agentic traversal to determine final evidence.

Do not allow parser workers to write hot canonical storage or retrieval indexes directly.

Do not trust parser output without validation and canonical import.

Do not let parser failure affect active serving state.

Do not claim deterministic replay for external models unless deterministic replay is actually available.

Do not rely on future replay alone to prove what happened; record QueryExecutionRecords at execution time.
```

## 33. Core Summary

This system treats the corpus as an authorized security boundary and keeps the hot retrieval plane focused on active production truth. Raw sources and canonical parses form the durable evidence substrate. Chunks, embeddings, indexes, and graph projections are retrieval machinery, not evidence. Final EvidencePacks are assembled from canonical ContentUnits using visible, deterministic, versioned policy.

For audit-grade workloads, the system preserves both immutable QueryExecutionRecords and full replayable ForensicSnapshots. QueryExecutionRecords prove what happened. ForensicSnapshots enable investigation, replay, and counterfactual analysis.

The resulting architecture is not a conventional chunk-and-embed RAG stack. It
is a canonical content graph with isolated parser execution, typed
human-readable canonical artifacts, active-parse-only retrieval, deterministic
evidence assembly, and forensic replay as first-class system capabilities.
