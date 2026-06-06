# ModernBERT Reranker Implementation Plan

## Handoff Note

All necessary code exploration for the ModernBERT reranker replacement has
already been performed. Do not repeat broad exploration at the start of the
next session. The code has not been modified.

The next session should begin implementation after confirming approval for the
scoped code edits. Configuration edits still require separate explicit approval
for the exact config file and values.

## Objective

Replace the current Qwen3 yes/no reranker implementation with a
ModernBERT-only sequence-classification reranker for
`Alibaba-NLP/gte-reranker-modernbert-base`, installed locally at:

```text
service/data-store/models/gte-reranker-modernbert-base
```

Qwen reranker support is no longer required. Do not build an adapter dispatcher
and do not preserve the Qwen reranker path. The dense embedding runtime still
uses `qwen3.rs`, so do not remove the shared Qwen module.

The goal is to preserve cross-encoder final ranking while reducing search
latency. The old reranker is a Qwen3 4B causal LM that scored yes/no next-token
logits. The new reranker is a 149M ModernBERT sequence classifier with long
context support.

## Current Runtime State

Current reranker runtime:

- `service/data-store/src/inference/reranker.rs`
- hard-wires `Qwen3Model`
- loads `1_LogitScore/config.json` for `true_token_id` and `false_token_id`
- builds a Qwen chat prompt with fixed prefix/suffix constants
- calls `selected_token_logits_batch`
- converts true/false logits to a public score with two-token softmax
- uses `RERANKER_MICROBATCH_SIZE = 1` because multi-row Qwen reranker passes
  produced non-finite logits on Metal

Current search integration:

- `service/data-store/src/http.rs`
- ColBERT ranks the candidate pool first
- `build_reranker_candidates` takes only the top `topK` ColBERT candidates
- search calls only:
  - `inference.reranker.score_candidates(...)`
  - `inference.reranker.score_candidates_with_progress(...)`
- public search result `score` is the final reranker score
- raw diagnostics currently expose Qwen-specific `trueLogit`/`falseLogit`
  fields and `rerankerTrueLogit`/`rerankerFalseLogit`

Search behavior should remain:

- Dense/BM25/RRF produce the first-stage candidate pool.
- ColBERT MaxSim ranks the bounded candidate pool.
- The reranker scores the top `topK` ColBERT-ranked candidates.
- Final public ordering is descending reranker score, tie-break by `unit_id`
  ascending.

## Model Artifact Facts

Read from `service/data-store/models/gte-reranker-modernbert-base/config.json`:

- `architectures = ["ModernBertForSequenceClassification"]`
- `model_type = "modernbert"`
- `hidden_size = 768`
- `num_hidden_layers = 22`
- `num_attention_heads = 12`
- `intermediate_size = 1152`
- `max_position_embeddings = 8192`
- `classifier_pooling = "mean"`
- `classifier_activation = "gelu"`
- `classifier_bias = false`
- `num_labels = 1` by label metadata and classifier tensor shape
- `pad_token_id = 50283`
- `cls_token_id = 50281`
- `sep_token_id = 50282`
- `norm_eps = 1e-05`
- `attention_bias = false`
- `mlp_bias = false`
- `global_attn_every_n_layers = 3`
- `global_rope_theta = 160000.0`
- `local_attention = 128`
- `local_rope_theta = 10000.0`
- `vocab_size = 50368`

Observed root safetensor names from
`service/data-store/models/gte-reranker-modernbert-base/model.safetensors`:

- `model.embeddings.tok_embeddings.weight`: `[50368, 768]`
- `model.embeddings.norm.weight`: `[768]`
- `model.final_norm.weight`: `[768]`
- `model.layers.{0..21}.attn.Wqkv.weight`: `[2304, 768]`
- `model.layers.{0..21}.attn.Wo.weight`: `[768, 768]`
- `model.layers.{0..21}.mlp.Wi.weight`: `[2304, 768]`
- `model.layers.{0..21}.mlp.Wo.weight`: `[768, 1152]`
- `model.layers.{0..21}.mlp_norm.weight`: `[768]`
- `model.layers.{1..21}.attn_norm.weight`: `[768]`
- `head.dense.weight`: `[768, 768]`
- `head.norm.weight`: `[768]`
- `classifier.weight`: `[1, 768]`
- `classifier.bias`: `[1]`

Note: `classifier_bias = false` in config, but the safetensor contains
`classifier.bias`. Implement against the actual loaded tensor inventory and
shape-check it explicitly.

The local model README shows raw transformer usage:

```python
scores = model(**inputs, return_dict=True).logits.view(-1, ).float()
```

It also shows `sentence-transformers` returning normalized scores in `[0, 1]`.
For this service, preserve both values:

- raw diagnostic value: `logit`
- public ranking score: `sigmoid(logit)`

## Tokenizer Facts

Read from `service/data-store/models/gte-reranker-modernbert-base/tokenizer.json`
and `tokenizer_config.json`:

- pair post-processor is:
  - `[CLS]`
  - sequence A
  - `[SEP]`
  - sequence B
  - `[SEP]`
- special token IDs:
  - `[CLS] = 50281`
  - `[SEP] = 50282`
  - `[PAD] = 50283`
  - `[MASK] = 50284`
- `tokenizer_config.json` has `model_max_length = 8192`
- `tokenizer_config.json` has `max_length = 512`
- `tokenizer.json` has truncation `max_length = 8000`
- `tokenizer.json` has fixed right padding to 8000
- model input names include `input_ids` and `attention_mask`

Important implementation consequence:

- Do not blindly use the serialized tokenizer padding behavior if it pads every
  pair to 8000 tokens. That would waste inference time and corrupt mean pooling
  unless attention masks are applied.
- Preferred first implementation: disable tokenizer padding for reranker
  scoring, use pair encoding, truncate to the service max token count, and mean
  pool over the actual emitted tokens.
- If the tokenizer API makes disabling serialized padding awkward, use the
  encoding attention mask for pooling and log actual non-pad token count.
- Do not claim a runtime max of 8192 if the loaded tokenizer is still
  truncating to 8000. Either override tokenizer truncation deliberately or set
  the local config to 8000.

## Implementation Design

Implement a ModernBERT sequence-classifier runtime directly in
`service/data-store/src/inference/reranker.rs`.

Do not extract shared ModernBERT code from `colbert.rs` for the first
implementation. Exploration showed the ColBERT code is useful as a reference,
but it is tightly coupled to ColBERT:

- ColBERT validates architecture `ModernBertModel`; reranker architecture is
  `ModernBertForSequenceClassification`.
- ColBERT safetensor names are rooted at `embeddings.*`, `layers.*`, and
  `final_norm.*`; reranker safetensor names are rooted at `model.*` plus
  `head.*` and `classifier.*`.
- ColBERT includes PyLate projection loading and MaxSim diagnostics.
- ColBERT tokenizer limits are hard-coded around 518/512-token ColBERT use.
- ColBERT health text, validation errors, and log labels are ColBERT-specific.

Use the ColBERT implementation as the local reference for these mechanics:

- `ColbertInputPath`
- `ColbertAttentionPrimitive`
- `ColbertLayerPrimitive`
- `ColbertEncoderRuntime`
- `ColbertMlpPrimitive`
- `MetalSafeLayerNorm`
- `TensorInventory`
- `apply_local_attention_mask`
- `tensor_abs_summary`
- startup model-call log helper pattern

Existing shared helpers available in `service/data-store/src/inference/tensor_ops.rs`:

- `apply_rope`
- `softmax_last_dim_metal_safe`

No new dependency is expected. Existing Cargo dependencies include:

- `candle-core = "0.10.2"`
- `candle-nn = "0.10.2"`
- `candle-transformers = "0.10.2"`
- `safetensors = "0.7.0"`
- `tokenizers = "0.23.1"`

## Reranker Runtime Shape

Keep this public API stable:

- `RerankerRuntime::load_with_progress`
- `RerankerRuntime::health_details`
- `score_candidates`
- `score_candidates_with_progress`
- `RerankerCandidateInput`
- `RerankerCandidateScore`

Update `RerankerCandidateScore` to ModernBERT fields:

```text
unit_id: String
score: f32
rank: usize
logit: f32
token_count: usize
```

Remove Qwen-only fields:

- `true_logit`
- `false_logit`

Suggested internal structs:

- `RerankerRuntime`
  - tokenizer
  - ModernBERT classifier model
  - device
  - max_tokens
  - hidden size, layer count, classifier metadata
  - smoke result
- `ModernBertSequenceClassifier`
  - input path
  - encoder
  - classification head
- `ModernBertInputPath`
  - token embeddings
  - embedding norm
- `ModernBertEncoderRuntime`
  - layers
  - final norm
- `ModernBertLayerPrimitive`
  - attention
  - mlp norm
  - mlp
- `ModernBertAttentionPrimitive`
  - Wqkv
  - Wo
  - optional attention norm
  - global/local attention metadata
- `ModernBertClassifierHead`
  - `head.dense` no-bias linear
  - GELU
  - `head.norm`
  - `classifier` linear using weight and observed bias
- `TokenizedRerankerCandidate`
  - unit ID
  - input IDs
  - token count or non-pad token count
  - document character count

Every new function must have a useful comment immediately before it, following
repo rules.

## Scoring Pipeline

For each candidate pair:

1. Tokenize `(query, candidate.content)` using pair encoding so tokenizer
   post-processing creates `[CLS] query [SEP] document [SEP]`.
2. Disable padding or honor the attention mask for pooling.
3. Truncate according to `models.reranker.max_tokens`.
4. Build token tensor on the selected accelerator.
5. Run embeddings and embedding norm.
6. Run all 22 ModernBERT layers with the same attention mechanics as ColBERT:
   - layer 0 has no `attn_norm`
   - later layers have `attn_norm`
   - layer index divisible by `global_attn_every_n_layers` uses global RoPE
   - other layers use local attention mask with `local_attention`
7. Run final norm.
8. Mean-pool hidden states over the valid token dimension.
9. Apply classifier head:
   - `head.dense`
   - GELU
   - `head.norm`
   - `classifier.weight` plus `classifier.bias`
10. Validate finite raw logit.
11. Convert public score with sigmoid.
12. Sort descending by score and tie-break by `unit_id` ascending.
13. Assign one-based ranks after sorting.

Score formula:

```text
score = 1 / (1 + exp(-logit))
```

Use a numerically stable sigmoid branch for large positive/negative logits.

## Diagnostics Requirements

Startup logs must include compact safe facts:

- model role `reranker`
- adapter/mode `modernbert_sequence_classifier`
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

Search logs must preserve the current lifecycle boundaries:

- candidate batch scoring started/completed/failed
- input tokenization ready
- per-candidate or microbatch model call started/completed/failed
- candidate count
- total and max token counts
- elapsed milliseconds

Do not log document contents, prompts, token dumps, vectors, full payloads, or
admin tokens.

## HTTP Raw Diagnostics

Update `service/data-store/src/http.rs`.

Current constant:

```text
RERANKER_MODE_QWEN3_YES_NO = "qwen3_yes_no_candidate_rerank"
```

Replace with:

```text
RERANKER_MODE_MODERNBERT_SEQUENCE_CLASSIFIER = "modernbert_sequence_classifier"
```

Current raw reranker score objects contain:

```json
{
  "unitId": "...",
  "score": 0.91,
  "rank": 1,
  "trueLogit": 1.0,
  "falseLogit": -1.0,
  "tokenCount": 512
}
```

ModernBERT raw score objects should contain:

```json
{
  "unitId": "...",
  "score": 0.91,
  "rank": 1,
  "logit": 2.31,
  "tokenCount": 512
}
```

Current `finalResults` raw entries contain:

- `rerankerTrueLogit`
- `rerankerFalseLogit`

Replace with:

- `rerankerLogit`

Keep:

- `rerankerScore`
- `rerankerRank`
- `rerankerTokenCount`
- ColBERT, RRF, dense, and BM25 fields

Update the current public-result comment in `build_reranker_results`; it still
says the score is a Qwen3 yes/no probability.

## Config Notes

Config edits require explicit separate approval before changing any config
file.

Current local config:

```toml
[models.reranker]
path = "/Users/goon/project/playground/service/data-store/models/Qwen3-Reranker-4B"
max_tokens = 32768
```

Target local model path:

```toml
path = "/Users/goon/project/playground/service/data-store/models/gte-reranker-modernbert-base"
```

Open config decision:

- `max_tokens = 8192` matches `config.json` and `tokenizer_config.json`
  `model_max_length`.
- `max_tokens = 8000` matches `tokenizer.json` serialized truncation and fixed
  padding.

Recommendation for first implementation:

- If the runtime deliberately overrides tokenizer truncation and disables
  padding, use `8192`.
- If the runtime leaves tokenizer truncation metadata in place, use `8000`.

Also update Qwen-specific comments in config only after explicit approval:

- `service/data-store/config.toml`
- possibly `service/data-store/config.example.toml`

`service/data-store/src/config.rs` has Qwen-specific comments for the reranker
config struct. Those are code comments, not config files, and should be updated
with the code change.

## Files Expected To Change

Likely code files:

- `service/data-store/src/inference/reranker.rs`
- `service/data-store/src/http.rs`
- `service/data-store/src/config.rs`

Possibly code files:

- `service/data-store/src/inference/mod.rs` only if public export names change;
  this is probably not needed.

Do not change unless explicitly approved or necessary after implementation:

- `service/data-store/src/inference/colbert.rs`
- `service/data-store/src/inference/qwen3.rs`
- `service/data-store/config.toml`
- `service/data-store/config.example.toml`
- README or architecture docs

Reference docs that still mention Qwen reranker:

- `service/data-store/README.md`
- `service/data-store/ARCHITECTURE.md`
- `service/data-store/SPEC-SERVER.md`
- `service/data-store/PLAN-SERVER.md`

Do not treat those older mentions as blockers for implementation. Update docs
only with explicit user approval after the runtime is working.

## Verification

Required checks after Rust/TS-style code changes in this Rust service:

```bash
cargo fmt
cargo check
cargo check --features metal
```

Runtime verification after code compiles and config approval is granted:

1. Switch local reranker config to the ModernBERT model path and approved
   `max_tokens`.
2. Start service with Metal.
3. Confirm startup smoke loads the ModernBERT sequence classifier and scores
   two pairs.
4. Run one representative search.
5. Confirm search raw diagnostics show:
   - `mode = modernbert_sequence_classifier`
   - `candidateLimit = topK`
   - one `logit` per reranked candidate
   - no Qwen true/false logit fields
6. Inspect `service/data-store/logs/data-store.log` for startup, model-call,
   search, stream-delivery, and terminal-result boundaries.

## Risks And Decisions

- Tokenizer padding/truncation is the highest-risk implementation detail. Avoid
  fixed 8000-token padding unless attention-mask pooling is implemented.
- The classifier head must use the observed `classifier.bias` tensor even
  though config says `classifier_bias = false`.
- A local ModernBERT classifier implementation is preferable to extracting from
  ColBERT for the first pass because ColBERT tensor names, validation, logging,
  and projection behavior are not generic.
- Metal behavior should be smoke-tested at representative sequence length
  before claiming readiness.
- The old Qwen reranker microbatch limitation should be deleted with the Qwen
  reranker path; do not carry that limitation forward unless ModernBERT shows a
  real batching issue.

## Next Session Resume Instructions

Start by reading this file only, then request approval for the scoped code
implementation if approval has not already been granted. Do not redo broad code
exploration; it has already been completed and summarized here.

Recommended implementation order:

1. Replace `service/data-store/src/inference/reranker.rs` with a ModernBERT-only
   sequence-classifier runtime.
2. Update `service/data-store/src/http.rs` to use ModernBERT mode and `logit`
   raw fields.
3. Update reranker comments in `service/data-store/src/config.rs`.
4. Run `cargo fmt`.
5. Run `cargo check`.
6. Run `cargo check --features metal`.
7. Request explicit config approval for `service/data-store/config.toml`.
8. Switch the local reranker config.
9. Run startup and search verification.
