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
