# How The Data Store Works

The data store is a standalone service that turns source documents into
searchable retrieval units, then answers search queries by combining lexical
search, vector search, ColBERT reranking, and a final reranker.

Source documents do not get uploaded through the API. Instead, the service is
configured with a corpus directory, and ingest requests name a file inside that
corpus. The service owns the conversion, splitting, embedding, storage,
indexing, and search pipeline.

## Ingestion

Ingestion is the process of making a document searchable.

```text
+------------------------------------------------------------------------------+
| 1. Request ingest for a corpus-relative source file                          |
+------------------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------------------+
| 2. Validate request, resolve source, enforce limits/admission                |
+------------------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------------------+
| 3. Convert PDF to text with configured Docling settings                      |
+------------------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------------------+
| 4. Split text into retrieval units with source/version/page metadata         |
+------------------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------------------+
| 5. Generate dense embeddings for each unit for semantic vector search        |
+------------------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------------------+
| 6. Generate ColBERT token vectors for late-interaction scoring               |
+------------------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------------------+
| 7. Write SQLite transaction: version, units, vectors, and FTS index          |
+------------------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------------------+
| 8. Publish active snapshot and swap in-memory dense cache                    |
+------------------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------------------+
| 9. Searches can now see the new active document version                      |
+------------------------------------------------------------------------------+
```

1. **The user requests ingest**

   A client sends an ingest operation with a corpus-relative source path, for
   example:

   ```json
   {
     "operation": "ingest",
     "payload": {
       "source": "The_Elements_of_Style.pdf"
     }
   }
   ```

   The service resolves that path inside its configured corpus root. File bytes
   are never sent through the HTTP request.

2. **The service checks admission and source validity**

   The service validates the request shape and field limits, then checks whether
   ingest capacity is available. If too many ingests are already running, it
   fails clearly with `503 Service Unavailable` instead of hiding the work in an
   internal queue.

   It also checks whether the document already has an active version. By
   default, ingest refuses to overwrite an existing active document. A caller
   must explicitly use `force: true` to create and publish a replacement
   version.

3. **PDF conversion runs through Docling**

   For PDFs, the service launches the configured Docling executable as a
   separate process. Docling converts the PDF into markdown-like text.

   Conversion behavior is controlled by service config, not by the request. That
   includes PDF backend, OCR mode, Docling device, timeout, thread count, and
   page batch size. The service does not silently switch parser backends or OCR
   modes if conversion fails.

4. **The converted text is split into retrieval units**

   The service breaks the converted document into deterministic searchable
   units. These are the chunks that search can return later.

   Each unit keeps useful metadata such as source path, version label, sequence,
   headings, page numbers, token counts, and content.

5. **The service creates a new immutable document version**

   Every successful ingest creates a new version for that source document. Older
   versions are retained.

   This matters because search visibility is versioned. A new ingest does not
   gradually leak into search while it is being processed. The new version
   becomes searchable only after all required storage, indexes, and in-memory
   cache updates are ready.

   ```text
   Before ingest:           During ingest:             After publish:
   +------------------+     +------------------+       +------------------+
   | Search sees      |     | Search still sees|       | Search sees      |
   | active version A |     | active version A |       | active version B |
   +------------------+     +------------------+       +------------------+
                                            |
                                            v
                                   +------------------+
                                   | Build version B  |
                                   | off to the side  |
                                   +------------------+

   Older version A is retained for rollback after B becomes active.
   ```

6. **Dense vectors are generated**

   Each retrieval unit is embedded with the local dense embedding model. These
   dense vectors are used later for semantic similarity search.

   The service validates vector dimensions, values, and norms before storing or
   using them.

7. **ColBERT document vectors are generated**

   Each retrieval unit also gets ColBERT document-token vectors. These are
   persisted in SQLite and used during search for late-interaction scoring.

   Search does not recompute ColBERT document vectors as a fallback. If the
   stored vectors are missing or invalid, that is an explicit data/runtime
   problem.

8. **Durable storage and indexes are written**

   The service writes the new document version, units, dense vectors, ColBERT
   vectors, and FTS5 lexical index rows to SQLite.

   Publishing the active document version happens as part of the same durable
   operation. If the new version cannot be fully stored and prepared, it does
   not become searchable.

9. **The active search cache is swapped**

   The service keeps an in-memory dense-vector cache for active document
   versions. After the new version is durable and cache-ready, the active cache
   is swapped.

   From that point on, new searches can see the newly ingested version.

## Search

Search is the process of taking a user query and finding the best matching
retrieval units.

```text
+--------------+     +-----------------+     +-------------------------+
| Search query |---->| Capture active  |---->| Embed query once        |
|              |     | snapshot        |     | for retrieval pipeline  |
+--------------+     +-----------------+     +-----------+-------------+
                                                          |
                         +--------------------------------+----------------+
                         |                                                 |
                         v                                                 v
             +---------------------+     +---------------------+
             | Dense vector search |     | BM25 lexical search |
             | semantic matches    |     | keyword matches     |
             +----------+----------+     +----------+----------+
                         |                                                 |
                         +------------------------+------------------------+
                                                  |
                                                  v
+--------------+     +-----------------+     +-------------------------+
| Top-K result |<----| Final reranker  |<----| RRF fusion + ColBERT    |
| + raw diag   |     | local or HTTP   |     | MaxSim reranking        |
+--------------+     +-----------------+     +-------------------------+
```

1. **The user sends a query**

   A client sends a search operation:

   ```json
   {
     "operation": "search",
     "payload": {
       "query": "clear writing style rules",
       "topK": 3
     }
   }
   ```

   `topK` controls how many final results the user wants.

2. **The service captures one active snapshot**

   At request admission, the service captures the active document-version map
   and dense-vector cache.

   This snapshot is used for the entire search. Even if another ingest finishes
   while the search is running, this search continues using the same captured
   corpus view. That prevents mixed-version or timing-dependent results.

3. **The query is embedded with the dense model**

   The service embeds the query using the dense embedding model, then validates
   the query vector.

4. **Dense semantic retrieval runs**

   The query vector is compared against the active dense-vector cache using
   exact cosine similarity.

   This finds units that are semantically similar to the query, even if they do
   not share exact words.

5. **BM25 lexical retrieval runs**

   In parallel conceptually, the service also searches SQLite FTS5 using BM25.

   This finds units with strong keyword or phrase overlap.

6. **Dense and BM25 candidates are fused**

   The service combines dense results and BM25 results using Reciprocal Rank
   Fusion, or RRF.

   This gives the pipeline a broader candidate pool: semantic matches, lexical
   matches, and items that score well in both.

7. **ColBERT reranking runs**

   The service takes the fused candidate pool, embeds the query with ColBERT,
   loads the stored ColBERT document vectors for the candidate units, and
   computes MaxSim scores.

   ColBERT is more precise than the first-stage dense/BM25 retrieval because it
   compares query-token and document-token representations rather than relying
   on a single vector per unit.

8. **The final reranker scores candidates**

   The ColBERT-ranked candidates are passed to the final reranker.

   In v2, this reranker is config-selected:

   - `backend = "local"` uses the local Candle ModernBERT reranker.
   - `backend = "http"` sends candidates to a Cohere-compatible HTTP rerank
     endpoint, such as vLLM, Cohere, or Jina-style APIs.

   ```text
   +-------------------------+     +-------------------------+
   | ColBERT-ranked pool     |---->| models.reranker.backend |
   | candidate units         |     | selects one backend     |
   +-------------------------+     +------------+------------+
                                                |
                            +-------------------+-------------------+
                            |                                       |
                            v                                       v
                +----------------------+              +----------------------+
                | local                |              | http                 |
                | Candle ModernBERT    |              | Cohere-compatible    |
                +----------+-----------+              +----------+-----------+
                           |                                     |
                           +------------------+------------------+
                                              |
                                              v
                                 +-------------------------+
                                 | Final ranked top-K      |
                                 | no backend fallback     |
                                 +-------------------------+
   ```

   The reranker candidate pool is separate from `topK`. For example, the user
   may request 10 results, while the reranker evaluates 40 candidates and
   chooses the best 10. This gives the strongest ranking model room to improve
   the final result set.

9. **Final top-K results are returned**

   The public results are ordered by final reranker score. Each result includes
   the unit ID, score, content, heading path, source path, and page numbers.

   The response also includes raw diagnostic data for the retrieval stages. That
   raw data preserves how dense search, BM25, RRF, ColBERT, and the final
   reranker contributed to the result. For the HTTP reranker, fields like raw
   logits and token counts are omitted because the remote API does not provide
   them.

## Important Guarantees

- Runtime never creates or migrates the SQLite schema. Storage setup is
  explicit.
- Ingested versions are immutable.
- Search uses one consistent active snapshot for the whole request.
- The service does not silently fall back between models, devices, OCR modes,
  parser backends, or reranker backends.
- Long-running operations stream progress to clients and also write durable
  service logs.
- HTTP reranker failures are explicit; they do not silently fall back to the
  local reranker.
- Public search results are concise, while raw diagnostics remain available for
  debugging and evaluation.
