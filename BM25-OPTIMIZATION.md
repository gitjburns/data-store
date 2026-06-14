# BM25 Optimization Session Log

## Starting State

Before this optimization work, search used a single SQLite FTS5 BM25 query for
lexical retrieval. User query text was normalized into quoted FTS terms joined
with `OR`, for example:

```text
"term1" OR "term2" OR "term3"
```

The BM25 SQL selected `bm25(units_fts)` as the candidate score, joined
`units_fts` to `units`, filtered to the captured active versions with a large
`OR` predicate over `(source_path, version_label)` pairs, ordered by BM25 score,
and limited to the configured candidate limit.

The important starting benchmark showed BM25 dominating retrieval latency:

```text
retrieving_candidates: 4.708s
  retrieving_candidates.dense_scan: 0.306s
  retrieving_candidates.bm25: 4.212s
```

After BM25 diagnostics were added, the slow boundary was clearly row iteration:

```text
BM25 diagnostics:
  fts query present: yes
  fts terms: 5
  fts query bytes: 68
  active versions: 118
  candidate limit: 100
  sql parameters: 238
  returned candidates: 100
  connection open: 0.001s
  filter build: 0.000s
  prepare: 0.001s
  query execution: 0.000s
  row iteration: 4.224s
  total: 4.227s
```

This indicated SQLite was not spending time opening the database, preparing the
statement, or starting execution. The cost was stepping/ranking FTS rows.

## Problem

The broad `OR` FTS query can match a large fraction of the FTS table when the
query contains common natural-language terms. `LIMIT 100` does not avoid the
cost, because SQLite still has to evaluate and rank enough matching rows to know
which 100 are best.

The active-version filter also covers 118 active documents, and retained
inactive versions remain in `units_fts`. However, later experiments suggest the
dominant cost is broad FTS matching/ranking itself, not SQL construction,
statement preparation, parameter count, or the active filter predicate shape.

## Change 1: Selective `build_fts_query`

Change:

- Replaced broad `OR` query construction with a selective query builder.
- Dropped tokens shorter than 3 characters.
- Dropped common English stopwords.
- Deduplicated terms.
- Joined retained terms with `AND` instead of `OR`.
- Skipped BM25 when no meaningful terms remained.

Rationale:

Dense retrieval already provides high-recall semantic matching. BM25 can act as
a higher-precision lexical retriever. Requiring all meaningful lexical terms
should dramatically reduce FTS match volume and row-iteration cost.

Result:

```text
retrieving_candidates: 0.526s
  retrieving_candidates.dense_scan: 0.353s
  retrieving_candidates.bm25: 0.022s

BM25 diagnostics:
  fts terms: 4
  returned candidates: 27
  row iteration: 0.018s
```

The latency improvement was large. BM25 dropped from roughly 4.2s to 0.022s.
However, result quality risk increased because BM25 returned only 27 candidates
against a candidate limit of 100. Strict `AND` can miss relevant passages that
contain only some query terms.

## Change 2: Quality-Preserving Strict/Broad Fallback

Change:

- Refactored BM25 query construction to produce both:
  - a strict filtered `AND` query for the fast path;
  - a broad deduplicated `OR` query preserving the old lexical recall behavior.
- Ran strict BM25 first.
- If strict BM25 filled `candidate_limit`, used strict results.
- If strict BM25 underfilled, ran broad fallback and used the broad fallback
  result as the BM25 candidate list.
- Added raw and CLI diagnostics:
  - `mode`;
  - `strictReturnedCandidates`;
  - `fallbackRan`;
  - `fallbackReturnedCandidates`.

Rationale:

Search quality was the priority. The fallback preserves old broad-BM25 recall
whenever strict matching might starve BM25 candidates. This gives fast behavior
for selective queries and old-quality behavior for underfilled queries.

Result:

```text
BM25 diagnostics:
  mode: strict_broad_fallback
  returned candidates: 100
  strict returned candidates: 27
  fallback ran: yes
  fallback returned candidates: 100
  row iteration: 4.180s
  total: 4.186s
```

Quality risk was addressed for underfilled queries, but latency returned to the
old broad-query cost when fallback ran.

## Change 3: Use FTS5 `rank` Instead Of `bm25(units_fts)`

Change:

- Changed the BM25 SQL to select and order by `units_fts.rank` instead of
  explicitly calling `bm25(units_fts)`.
- Preserved the downstream field name `bm25_score`.

Rationale:

SQLite FTS5 exposes a hidden `rank` column intended for ranked FTS queries.
Using `rank` can be faster than repeatedly invoking the `bm25()` auxiliary
function, while preserving default BM25 ranking semantics.

Result:

```text
BM25 diagnostics:
  mode: strict_broad_fallback
  returned candidates: 100
  strict returned candidates: 27
  fallback ran: yes
  row iteration: 4.193s
  total: 4.197s
```

No meaningful improvement. The bottleneck is not the explicit `bm25()` function
call.

## Change 4: Replace Large Active-Version `OR` Predicate With Snapshot CTE

Change:

- Replaced the generated 118-way active-version `OR` predicate with:

```sql
WITH active_snapshot(source_path, version_label) AS (VALUES ...)
...
JOIN active_snapshot active
  ON active.source_path = units.source_path
 AND active.version_label = units.version_label
```

- Bound the captured active-version snapshot into the `VALUES` CTE.
- Did not join the live `active_document_versions` table, because search must
  use the captured active snapshot for the whole request.

Rationale:

The large active-version `OR` predicate produced 238 SQL parameters. A CTE join
might let SQLite evaluate active-version membership more efficiently while
preserving captured-snapshot semantics.

Result:

```text
BM25 diagnostics:
  mode: strict_broad_fallback
  returned candidates: 100
  strict returned candidates: 27
  fallback ran: yes
  row iteration: 4.426s
  total: 4.430s
```

This was slightly worse than the original `OR` predicate. It did not address
the bottleneck.

## Current Code State

At the end of this session, the code still contains the experimental changes:

- strict/broad BM25 fallback and diagnostics;
- FTS5 `rank` selection/order instead of explicit `bm25(units_fts)`;
- active-snapshot `VALUES` CTE join instead of the large active-version `OR`
  predicate;
- CLI BM25 diagnostics for mode/fallback counts.

The CTE experiment regressed the benchmark and should probably be reverted. The
`rank` experiment appeared neutral, but it did not improve the measured slow
case. The strict/broad fallback is quality-preserving, but still slow whenever
fallback runs.

## Current Hypothesis

The slow case is broad FTS matching/ranking over too many rows. The expensive
part is not:

- database open;
- SQL filter string construction;
- SQL preparation;
- query start;
- explicit `bm25()` versus FTS5 `rank`;
- 118-way `OR` predicate versus active snapshot CTE.

The likely structural cause is that broad fallback searches `units_fts` across
all retained versions, while search only needs active versions. Because inactive
retained versions remain in the FTS table, broad queries can match and rank far
more rows than the active corpus needs.

## Recommended Next Steps

1. Revert the active-snapshot CTE change.

   It made the benchmark worse and adds complexity. If preserving the current
   code state for comparison is useful, record the benchmark first, then revert.

2. Consider reverting the `rank` change.

   It did not improve the benchmark. Keeping it may still be acceptable if the
   team prefers FTS5's recommended rank path, but it should not be treated as a
   latency fix.

3. Keep the strict/broad fallback diagnostics for now.

   They clearly show when the fast strict path succeeds versus when the slow
   quality-preserving fallback runs.

4. Design an active-only lexical index.

   The most promising quality-preserving latency fix is to make broad BM25
   operate only over active document versions. That likely means a separate
   active-only FTS table or equivalent indexed lexical structure maintained at
   active-version publish/rollback time.

   Design constraints:

   - normal runtime must not migrate schema;
   - storage setup must be explicit;
   - ingest publish must atomically update durable version state, active-version
     state, active dense cache, and active lexical index visibility;
   - rollback must update the active lexical index without rebuilding embeddings;
   - search must continue using one captured active snapshot for the whole
     request;
   - raw diagnostics should distinguish strict path, fallback path, active-only
     FTS row counts, and row-iteration timing.

5. If an active-only FTS table is too large for the next step, add an operator
   diagnostic query first.

   Measure total `units` rows, active `units` rows, retained inactive `units`
   rows, and broad FTS match counts for representative slow queries. This will
   confirm whether retained inactive versions are multiplying fallback work.

---

## Session 2 Assessment

A new agent (this session) reviewed the previous session's work and BM25
benchmarks. The initial assessment was:

### What the previous agent got right

- Instrumentation-first approach correctly isolated the cost to FTS row
  iteration and ruled out connection open, filter build, prepare, and query
  start.
- Methodical falsification: Changes 3 and 4 cheaply eliminated micro-causes.
- Recognizing 27/100 strict candidates as a recall risk was correct.

### What the previous agent got wrong (per this assessment)

1. The central hypothesis (retained inactive versions multiplying work) was
   never measured before the active-only FTS table was recommended.
2. The strict/broad fallback is "always slow" in practice because strict AND
   over natural-language terms almost never fills the candidate limit.
3. High-document-frequency terms were identified as the cost driver but never
   exploited via IDF-aware pruning.
4. No quality measurement existed on either side of the BM25 changes.

### New ideas proposed

1. Measure first: active vs inactive row counts, per-term document
   frequencies, broad match counts.
2. IDF-aware term pruning in the broad query.
3. "At least k of n" FTS middle ground.
4. Active-only FTS index (only if measurement supports it).
5. Check connection/cache handling and SQLite pragmas.

## Measurements

Read-only diagnostic queries were run against the production database.

### Row counts

| Metric | Value |
| --- | --- |
| Total `units` rows | 65,123 |
| Active units | 63,525 |
| Retained inactive units | 1,598 (2.5%) |
| Active documents | 118 |
| Total versions with units | 120 |
| Total FTS rows | 65,123 |

### Per-term match counts

| Term | Matching rows | % of corpus |
| --- | --- | --- |
| `"clear"` | 3,331 | 5.1% |
| `"rules"` | 2,328 | 3.6% |
| `"writing"` | 1,856 | 2.9% |
| `"style"` | 1,009 | 1.5% |
| Broad OR union | 7,942 | 12.2% |
| Strict AND intersection | 2 | 0.003% |

### EXPLAIN QUERY PLAN (CTE-based query)

```text
SCAN active_snapshot
SCAN units_fts VIRTUAL TABLE INDEX 0:M1
SEARCH units USING INTEGER PRIMARY KEY (rowid=?)
USE TEMP B-TREE FOR ORDER BY
```

### Indexes on `units`

Only `idx_units_document_sequence(document_id, sequence)` and the PK on
`unit_id`. No index on `(source_path, version_label)`.

### Key findings from measurements

- The active-only FTS table idea from session 1 was invalidated: only 2.5% of
  rows are inactive. Eliminating them would save almost nothing.
- Strict AND finds only 2 rows, confirming fallback fires on virtually every
  real query.

## Change 5: Add Index On `units(source_path, version_label)`

Hypothesis:

The EXPLAIN QUERY PLAN showed the active-snapshot CTE as the outermost scan
and `units` being accessed by rowid. There was no index on
`(source_path, version_label)`, so the active-version join had to scan the
CTE linearly for each of the 7,942 FTS match rows. Adding the index would
let SQLite do an indexed probe instead of a linear scan for membership
checking: O(log n) per match instead of O(n).

Change:

- Added `CREATE INDEX IF NOT EXISTS idx_units_source_version ON
  units(source_path, version_label)` to `sql/schema.sql`.
- Bumped `PRAGMA user_version` from 3 to 4.
- Updated `EXPECTED_SCHEMA_VERSION` in `storage.rs` from 3 to 4.
- Re-ran `--setup-storage` to build the index.

Result:

```text
BM25 diagnostics:
  mode: strict_broad_fallback
  returned candidates: 100
  strict returned candidates: 27
  fallback ran: yes
  row iteration: 4.458s
  total: 4.459s
```

No improvement. The hypothesis was wrong. SQLite accesses `units` rows via
rowid from the FTS join, not by `(source_path, version_label)`. Once a row is
loaded by rowid, checking column values against the active set is a value
comparison on the already-loaded row. The new index is never consulted because
the access path is rowid-first.

## Change 6: Move Active-Version Filter From SQL To Rust

Hypothesis:

The EXPLAIN QUERY PLAN showed `SCAN active_snapshot` as the outermost loop
with 118 rows. The theory was that SQLite was re-executing the FTS MATCH for
each of the 118 CTE rows, creating 118 × 7,942 = 937K virtual row
evaluations. Moving the filter to Rust with a `HashSet` would reduce this
from O(N×M) to O(N).

Change:

- Replaced the CTE-based SQL with a simple unfiltered query:
  ```sql
  SELECT units.unit_id, units.source_path, units.version_label,
         units_fts.rank AS bm25_score
  FROM units_fts
  JOIN units ON units.rowid = units_fts.rowid
  WHERE units_fts MATCH ?
  ORDER BY units_fts.rank ASC, units.unit_id ASC
  LIMIT ?
  ```
- Removed all active-version parameters from the SQL (2 params instead of
  238).
- Added `Bm25RawMatch` struct carrying `source_path` and `version_label`.
- Post-filtered in Rust using `HashSet<(&str, &str)>` built from the captured
  active versions.
- Used `limit * 2` as the SQL LIMIT to overfetch for post-filter losses.
- Updated diagnostics: replaced `sqlParameterCount`/`filterBuildLatencyMs`
  with `unfilteredCandidates`/`postFilterLatencyMs`.
- Updated CLI rendering to match.

Result:

```text
BM25 diagnostics:
  mode: strict_broad_fallback
  unfiltered candidates: 227
  returned candidates: 100
  strict returned candidates: 27
  fallback ran: yes
  post filter: 0.000s
  row iteration: 4.174s
  total: 4.178s
```

Marginal improvement (~6%): 4.426s → 4.174s. The N×M multiplication
hypothesis was wrong, or at least greatly overstated. Removing the
active-version filter from SQL entirely barely changed the row iteration
cost.

## Diagnostic: Isolating FTS5 Cost Layers

After two failed hypotheses, ran timed `sqlite3` queries to isolate which
layer of the FTS pipeline is expensive.

| Test | Description | Time |
| --- | --- | --- |
| A | FTS MATCH only, no rank, no sort, no join, LIMIT 200 | 0.006s |
| B | + rank computation | 0.001s |
| C | + ORDER BY rank (forces scoring all 7,942 rows) | 0.016s |
| D | + JOIN units (full production query shape) | ~0.04s |

### Key finding

The full BM25 query — matching, scoring, sorting, joining 7,942 rows —
takes approximately 16–40ms in sqlite3. The Rust service takes 4,174ms for
the same query. That is a ~100–250× slowdown.

This means the bottleneck is not in SQLite's FTS5 engine, query shape, match
volume, active-version filtering, index coverage, or SQL structure. It is
somewhere in the Rust execution environment: connection handling, rusqlite row
deserialization, page cache behavior, or system-level I/O interaction.

### Database characteristics

| Property | Value |
| --- | --- |
| Database file size | 11 GB |
| Page size | 4,096 bytes |
| Page count | 2,839,957 |
| Default cache_size | 2,000 pages (8 MB) |
| mmap_size | 0 (disabled) |
| journal_mode | delete |

### What was ruled out across sessions 1 and 2

- SQL filter string construction
- SQL preparation
- Query start / query_execution timing
- Explicit `bm25()` vs FTS5 `rank`
- 118-way `OR` predicate vs active-snapshot CTE
- Missing index on `units(source_path, version_label)`
- Active-version filter in SQL vs post-filter in Rust
- Retained inactive versions inflating FTS match volume (only 2.5% inactive)
- FTS5 match volume, scoring, or sorting (confirmed fast at ~16ms in sqlite3)

## Current Code State

The code contains all experimental changes from both sessions:

- Strict/broad BM25 fallback and diagnostics.
- FTS5 `rank` selection/order instead of explicit `bm25(units_fts)`.
- Active-version filter moved from SQL to Rust `HashSet` post-filter
  (Change 6 replaced Change 4's CTE).
- `Bm25RawMatch` struct for pre-filter rows.
- Schema version bumped to 4 with `idx_units_source_version` index (Change 5,
  had no effect but is harmless).
- CLI diagnostics updated for `unfilteredCandidates`/`postFilterLatencyMs`.

## Current Hypothesis

The bottleneck is not FTS5. The same query runs in 16–40ms via the `sqlite3`
CLI against the same database.

The ~100–250× slowdown in the Rust service is likely caused by the
interaction between rusqlite row iteration and SQLite page access on an 11 GB
database with default pragmas:

- `cache_size = 2000` (8 MB) against an 11 GB file.
- `mmap_size = 0` (disabled).
- `journal_mode = delete` (not WAL).
- A fresh `Connection::open` per BM25 query with no pragma tuning beyond
  `PRAGMA foreign_keys = ON`.

The sqlite3 CLI was fast because the OS page cache was warm from prior runs
and sqlite3 may handle page access differently. The Rust service opens a new
connection per query, starting with a cold 8 MB SQLite page cache. Iterating
7,942 FTS matches plus rowid joins scattered across an 11 GB file with no
mmap forces repeated OS page cache lookups through SQLite's buffer pool.

This hypothesis has not been tested. Previous hypotheses in this session were
also plausible-sounding and wrong. The correct next step is to measure,
not to implement. Possible measurements:

1. Add mmap and cache_size pragmas to the Rust `open_connection` function and
   benchmark to see if the slowdown disappears.
2. Add a persistent read connection to `StorageRuntime` and benchmark to see
   if page cache reuse across queries eliminates the cost.
3. Profile the Rust service during a BM25 query to identify where wall-clock
   time is actually spent (system calls, page faults, rusqlite internals).
