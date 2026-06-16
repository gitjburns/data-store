# Benchmark Policy

This document records the benchmark policy for deciding when the standalone
Data Store service should reconsider exact dense flat scan and evaluate an ANN
or dedicated vector index.

It does not record completed benchmark results. Add measured results only after
running the procedure on named hardware with a known corpus and config.

## Dense Flat-Scan Scope

The current dense stage uses the active in-memory dense cache and exact cosine
similarity. The measured code path is `DenseVectorCache::search`:

- vectors are stored in one row-major `Vec<f32>`
- each vector is expected to have dimension `models.dense.dimension`
- current dense dimension is `4096`
- every cached vector is scanned for each query
- cosine similarity uses the query norm and stored vector norm
- results sort by similarity descending, then `unitId` ascending for
  deterministic tie-breaking

The benchmark policy is concerned with this dense scan stage, not with dense
query embedding, BM25, RRF, ColBERT MaxSim, Qwen3 reranking, or response
serialization.

## Required Corpus Sizes

Measure at these active-unit counts before considering an ANN/indexing change:

- 10k active units
- 50k active units
- 100k active units

Use active source-document versions only. Inactive retained versions must not be
counted unless a benchmark is explicitly measuring storage retention overhead.

## Metrics To Record

For each run, record:

- hardware model, CPU, memory, accelerator, and OS
- build profile and Cargo feature set
- service config values that affect retrieval limits
- active unit count
- dense vector dimension
- dense cache memory bytes
- dense cache load duration
- query count and query set description
- p50, p95, and max `denseLatencyMs`
- p50, p95, and max end-to-end `/v1/search` latency
- notes about concurrent ingest/search load, if any

The current `/v1/search` response exposes the relevant dense-stage fields under
`raw.retrieval`:

- `denseLatencyMs`
- `cache.vectorCount`
- `cache.memoryBytes`
- `cache.loadDurationMs`

## Methodology

1. Build and run the service with the target accelerator feature.
2. Ensure storage is already set up; do not include schema setup in benchmark
   timings.
3. Start the service and wait until `/v1/health` reports ready.
4. Run one warm-up search for each benchmark query and discard those timings.
5. Run the fixed query set against a stable active corpus.
6. Record `raw.retrieval.denseLatencyMs` and end-to-end `latencyMs` from every
   response.
7. Report p50, p95, and max values.
8. Repeat for 10k, 50k, and 100k active units.

Do not benchmark cold startup as dense search latency. Startup cache load is a
separate operational metric and should be recorded from
`raw.retrieval.cache.loadDurationMs` or `/v1/health` storage diagnostics after
startup.

Use representative queries for the target corpus. Keep the query set stable
between corpus sizes so scaling behavior is comparable.

## Initial Decision Thresholds

Exact dense flat scan remains acceptable while these p95 dense-stage thresholds
hold on target hardware:

| Active units | p95 dense scan target |
|---:|---:|
| 10k | <= 25 ms |
| 50k | <= 125 ms |
| 100k | <= 250 ms |

These thresholds are intentionally linear because the implementation performs a
full scan over every active vector.

Reconsider ANN or a dedicated vector index when any of the following are true:

- 100k active-unit p95 `denseLatencyMs` exceeds 250 ms on target hardware.
- Dense scan remains above 20% of end-to-end search latency after model-stage
  latency has been separately optimized.
- Dense cache memory becomes operationally unacceptable for expected corpus
  size.
- Ingest publish time becomes unacceptable because cache rebuild/swap work grows
  too large for active-version updates.

Do not add ANN only because it is conventional. The current exact scan is
simple, deterministic, and preserves straightforward observability. Replace it
only when measured behavior shows a real operational need.

## Result Template

Use this table shape when recording measured results:

| Date | Hardware | Feature | Active units | Dimension | Cache memory | Cache load | Queries | Dense p50 | Dense p95 | Dense max | Search p95 | Decision |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| YYYY-MM-DD | example CPU/GPU/RAM | metal/cuda | 10000 | 4096 | TBD | TBD | TBD | TBD | TBD | TBD | TBD | keep flat scan / revisit ANN |

## Current Limitation

The production API exposes dense-stage latency as part of full `/v1/search`.
Full search also runs BM25, RRF, ColBERT MaxSim, and Qwen3 reranking, so large
benchmark suites may be slow.

If full-pipeline benchmark time becomes impractical, add a dedicated internal
dense benchmark command or admin-only diagnostic endpoint as a separately
approved scope. Do not add that benchmark harness through normal runtime paths
without approval.
