# ModernBERT Reranker Implementation Plan

## Objective

Replace the current Qwen3 yes/no reranker path with support for
`Alibaba-NLP/gte-reranker-modernbert-base`, installed locally at:

```text
service/data-store/models/gte-reranker-modernbert-base
```

The goal is to preserve the benefits of a cross-encoder reranker while reducing
search latency substantially. The current Qwen3 reranker is a 4B causal LM and
takes roughly 0.8s-2.2s per candidate on Metal. The new model is a 149M
ModernBERT sequence-classification reranker with max input length 8192.

## Current State

- Current runtime path: `service/data-store/src/inference/reranker.rs`
- Current model: `service/data-store/models/Qwen3-Reranker-4B`
- Current scoring: Qwen3 causal LM prompt forward pass, then yes/no next-token
  logit softmax.
- Current search behavior: ColBERT ranks the candidate pool, then the reranker
  scores only the top `topK` ColBERT candidates.
- Qwen batching is disabled by `RERANKER_MICROBATCH_SIZE = 1` because multi-row
  Qwen passes produced non-finite logits on Metal during startup smoke scoring.

## New Model Facts

From `service/data-store/models/gte-reranker-modernbert-base/config.json`:

- `architectures = ["ModernBertForSequenceClassification"]`
- `model_type = "modernbert"`
- `hidden_size = 768`
- `num_hidden_layers = 22`
- `num_attention_heads = 12`
- `intermediate_size = 1152`
- `max_position_embeddings = 8192`
- `classifier_pooling = "mean"`
- `classifier_activation = "gelu"`
- `num_labels = 1` implied by classifier tensor shape
- `pad_token_id = 50283`

Observed safetensor head tensors:

- `head.dense.weight`
- `head.norm.weight`
- `classifier.weight`
- `classifier.bias`

The model returns one raw relevance logit per query/document pair. Public
reranker score should be `sigmoid(logit)`.

## Design

### Adapter Selection

Auto-detect the reranker adapter from `config.json`:

- `Qwen3ForCausalLM` keeps the existing Qwen3 yes/no implementation.
- `ModernBertForSequenceClassification` uses the new ModernBERT classifier
  implementation.
- Any other architecture fails readiness explicitly.

This avoids adding a config selector and keeps model identity authoritative.

### Runtime Structure

Refactor `RerankerRuntime` into an enum-like dispatcher:

```text
RerankerRuntime
- Qwen3 adapter
- ModernBERT sequence-classifier adapter
```

Keep the public API stable:

- `RerankerRuntime::load_with_progress`
- `RerankerRuntime::health_details`
- `score_candidates`
- `score_candidates_with_progress`

The HTTP/search code should not need to know which adapter is loaded except for
raw diagnostic mode strings and score metadata.

### ModernBERT Scoring Pipeline

For each candidate pair:

1. Tokenize `(query, candidate.content)` using tokenizer pair encoding.
2. Truncate to `models.reranker.max_tokens`.
3. Run the ModernBERT encoder.
4. Mean-pool hidden states according to `classifier_pooling = "mean"`.
5. Apply classifier head:
   - dense projection
   - GELU activation
   - head norm
   - classifier linear layer plus bias
6. Produce:
   - raw logit
   - public score = sigmoid(logit)
   - token count
7. Sort descending by score, tie-break by `unit_id` ascending.

### ModernBERT Implementation Approach

Reuse or extract the existing ModernBERT encoder implementation currently in:

```text
service/data-store/src/inference/colbert.rs
```

The ColBERT path already implements ModernBERT pieces and Metal-ready startup
smoke checks. Prefer sharing core ModernBERT code rather than copying a second
encoder by hand. If extracting becomes too broad, keep the first implementation
local but do not change ColBERT behavior.

Do not add an ONNX runtime dependency. Use Candle and `model.safetensors`.

### Diagnostics

Startup logs must include:

- adapter name
- model path
- hidden size
- layer count
- max tokens
- classifier pooling
- classifier activation
- startup smoke candidate count
- smoke token counts
- smoke raw logits
- smoke sigmoid scores
- elapsed milliseconds

Search logs must preserve existing lifecycle boundaries:

- reranker batch started/completed/failed
- per microbatch or per candidate model-call boundaries as appropriate
- candidate count
- total and max token counts
- elapsed milliseconds

Do not log document contents, prompts, token dumps, vectors, or large payloads.

### API Raw Diagnostics

Update reranker raw metadata for adapter-specific fields.

For ModernBERT:

```json
{
  "mode": "modernbert_sequence_classifier",
  "candidateSource": "colbert_ranked_candidate_pool",
  "colbertCandidateCount": 67,
  "candidateLimit": 10,
  "candidateCount": 10,
  "scores": [
    {
      "unitId": "...",
      "score": 0.91,
      "rank": 1,
      "logit": 2.31,
      "tokenCount": 512
    }
  ]
}
```

Existing public results can continue using `rerankerScore`,
`rerankerRank`, and `rerankerTokenCount`. Qwen-only true/false logit fields
should become optional or adapter-specific raw details.

## Config Changes

Config changes require explicit approval before editing.

Target file:

```text
service/data-store/config.toml
```

Planned changes:

```toml
[models.reranker]
path = "/Users/goon/project/playground/service/data-store/models/gte-reranker-modernbert-base"
max_tokens = 8192
```

Also update local comments from Qwen3-specific wording to generic reranker
wording or ModernBERT-specific wording.

Consider updating `config.example.toml` only after user approval, since it is a
configuration file too.

## Likely Files To Touch

- `service/data-store/src/inference/reranker.rs`
- `service/data-store/src/inference/mod.rs`
- `service/data-store/src/inference/colbert.rs` or a new shared ModernBERT
  module if extraction is approved
- `service/data-store/src/http.rs`
- `service/data-store/src/config.rs`
- `service/data-store/config.toml` after explicit config approval
- Possibly `service/data-store/config.example.toml` after explicit config
  approval
- Possibly `service/data-store/README.md` after implementation is working

## Verification

Required checks:

```bash
cargo fmt
cargo check
cargo check --features metal
```

Runtime verification:

1. Start service with `--features metal`.
2. Confirm startup smoke loads the ModernBERT reranker and scores two pairs.
3. Run one representative search.
4. Confirm search raw diagnostics show:
   - `mode = modernbert_sequence_classifier`
   - `candidateLimit = topK`
   - one logit per reranked candidate
   - reranker latency significantly below the previous Qwen path
5. Inspect `service/data-store/logs/data-store.log` for complete startup,
   model-call, search, stream-delivery, and terminal-result boundaries.

## Risks

- Extracting ModernBERT from ColBERT may be larger than expected because
  ColBERT has projection-specific and startup-smoke-specific code nearby.
- Tokenizer pair encoding behavior must match the model README. The README
  uses `tokenizer(pairs, padding=True, truncation=True, max_length=512)` in
  Python. The service should use equivalent pair encoding and truncation.
- The classification head implementation must match Hugging Face's ModernBERT
  sequence-classification head. Tensor names confirm the needed weights, but
  implementation should be validated with startup smoke scores.
- Metal behavior should be smoke-tested at max or representative sequence
  length before claiming readiness.

## Resume Instructions

Start by reading this file, then inspect the current `reranker.rs` and
`colbert.rs` ModernBERT implementation. Do not edit config until explicit
approval is given for the exact config path and values.

Recommended first implementation step:

1. Build the ModernBERT sequence-classifier adapter while leaving Qwen support
   intact.
2. Compile-check.
3. Request explicit approval for `config.toml`.
4. Switch the local reranker config to the GTE model.
5. Run startup and search verification.
