#![allow(dead_code, unused_imports)]

//! Candle/Metal diagnostics and throughput benchmarks. Pure tensor work — no
//! model weights, no config, no service code — so it runs in seconds.
//!
//! Part 1 — correctness probes: Metal-vs-CPU comparison of the batched tensor
//! ops the qwen3 forward performs, including the 2026-07-18 permuted-stride
//! reproducer (`Tensor::cat` over narrowed head views yields a stride-permuted
//! view that candle 0.10.2's Metal matmul silently miscomputes for every batch
//! row past the first; the CPU backend rejects the same layout). Re-run these
//! after any candle upgrade to decide whether the `.contiguous()` workaround in
//! `qwen3.rs::repeat_kv_heads` can be retired.
//!
//! Part 1b (`--probe-batched-sanitize`) — CPd batch-consistency smoke failure
//! diagnosis: NaN*0 sanitize semantics, stride-0 broadcast_mul correctness,
//! and the two-layer NaN-propagation chain under both sanitizer mechanisms.
//!
//! Part 2 — dense dtype throughput benchmark: the dominant per-layer linear
//! matmuls of Qwen3-Embedding-8B (hidden 4096, intermediate 12288, 32/8 heads,
//! head_dim 128) timed on Metal in BF16 vs F16 vs F32 at the measured average
//! passage length. Decision input for switching the model load dtype: M1-class
//! GPUs have native F16 but not native bfloat.
//!
//! Part 3 — ColBERT batching throughput benchmark: ModernBERT-shaped layer work
//! (hidden 768, 12 heads, head_dim 64, GLU intermediate 1152, F32 — the
//! runtime's load dtype) comparing the current per-document rank-2 per-head
//! style against batched rank-3/rank-4 execution. Decision input for the
//! ColBERT batching rework.
//!
//! Part 4 (`--validate-dense-dtypes [--config path]`) — REAL-MODEL cross-dtype
//! validation: loads the actual dense model at BF16, embeds fixed passages,
//! drops it (32 GB cannot hold two 8B loads at once), reloads at F16, embeds
//! the same passages, and reports per-passage cross-dtype cosine plus embed
//! timings. The `DENSE_COMPUTE_DTYPE` flip to F16 is gated on every cosine
//! clearing 0.99. Flag-gated because it loads 16 GB of weights twice.

use std::{env, path::PathBuf, time::Instant};

// Included inference computes the same canonical embedding identity as the service.
#[path = "../canonical.rs"]
mod canonical;
#[path = "../client_limits.rs"]
mod client_limits;
#[path = "../config.rs"]
mod config;
#[path = "../error.rs"]
mod error;
#[path = "../inference/mod.rs"]
mod inference;
#[path = "../limits.rs"]
mod limits;
#[path = "../primitives/mod.rs"]
mod primitives;
// Required by the included inference sources: device.rs renders panic
// payloads through crate::util, which this bin crate must therefore declare.
#[path = "../util.rs"]
mod util;

use candle_core::{D, DType, Device, Tensor};
use candle_nn::Module;

/// Dispatch on the flag: default runs the light tensor-only parts 1–3;
/// `--validate-dense-dtypes` runs only the heavy real-model part 4.
fn main() {
    let mut validate_dtypes = false;
    let mut probe_batched_sanitize = false;
    let mut config_path = PathBuf::from("config.toml");
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--validate-dense-dtypes" => validate_dtypes = true,
            "--probe-batched-sanitize" => probe_batched_sanitize = true,
            "--config" => {
                let Some(value) = args.next() else {
                    println!("--config requires a path");
                    return;
                };
                config_path = PathBuf::from(value);
            }
            other => {
                println!("unknown argument: {other}");
                return;
            }
        }
    }

    if validate_dtypes {
        if let Err(source) = validate_dense_dtypes(config_path) {
            println!("dtype_validation FAILED error={source}");
        }
        return;
    }

    let metal = match Device::new_metal(0) {
        Ok(device) => device,
        Err(source) => {
            println!("SKIPPED metal device init failed: {source}");
            return;
        }
    };

    if probe_batched_sanitize {
        run_batched_sanitize_probes(&metal);
        return;
    }

    run_metal_op_probes(&metal);
    bench_dense_dtypes(&metal);
    bench_colbert_batching(&metal);
}

/// The gate every cross-dtype cosine must clear before `DENSE_COMPUTE_DTYPE`
/// flips to F16.
const DTYPE_VALIDATION_COSINE_MIN: f32 = 0.99;

/// Load the real dense model at BF16 then F16 (sequentially — memory), embed
/// the same fixed passages under each, and print per-passage cross-dtype
/// cosines plus per-dtype embed timings. The final verdict line states whether
/// the F16 flip gate is met.
fn validate_dense_dtypes(config_path: PathBuf) -> Result<(), error::ApiError> {
    let service_config = config::ServiceConfig::load(config_path.clone())?;
    println!("dtype_validation config={}", config_path.display());
    let passages = validation_passages();

    let mut embeddings_per_dtype: Vec<Vec<Vec<f32>>> = Vec::with_capacity(2);
    for dtype in [DType::BF16, DType::F16] {
        let load_started = Instant::now();
        let mut progress = |_message: &str| Ok(());
        let dense = inference::InferenceRuntime::initialize_dense_for_dtype_validation(
            &service_config,
            dtype,
            &mut progress,
        )?;
        println!(
            "dtype_validation dtype={dtype:?} load_ms={}",
            load_started.elapsed().as_millis()
        );

        // One untimed warmup embed so first-dispatch kernel compilation does
        // not pollute the timing comparison.
        dense.embed_passage_vector(&passages[0])?;
        let embed_started = Instant::now();
        let vectors = passages
            .iter()
            .map(|passage| dense.embed_passage_vector(passage))
            .collect::<Result<Vec<_>, error::ApiError>>()?;
        println!(
            "dtype_validation dtype={dtype:?} passages={} total_embed_ms={}",
            passages.len(),
            embed_started.elapsed().as_millis()
        );
        embeddings_per_dtype.push(vectors);
        // `dense` drops here, releasing this dtype's 16 GB before the next load.
    }

    let mut min_cosine = f32::INFINITY;
    for (index, (bf16, f16)) in embeddings_per_dtype[0]
        .iter()
        .zip(embeddings_per_dtype[1].iter())
        .enumerate()
    {
        let cosine = cosine(bf16, f16);
        min_cosine = min_cosine.min(cosine);
        println!(
            "dtype_validation passage={index} chars={} cosine_bf16_f16={cosine:.6}",
            validation_passages()[index].chars().count()
        );
    }
    println!(
        "dtype_validation verdict min_cosine={min_cosine:.6} gate={DTYPE_VALIDATION_COSINE_MIN} pass={}",
        min_cosine >= DTYPE_VALIDATION_COSINE_MIN
    );

    Ok(())
}

/// Fixed validation passages spanning the corpus's length range: short, two
/// mid-length prose bodies, numeric/tabular-flavored text, unicode-heavy text,
/// and one near-chunk-cap body (~500 tokens via repetition) to stress
/// accumulated F16 error at the chunker's 512-token cap.
fn validation_passages() -> Vec<String> {
    let long_sentence = "The activation gate admits at most one active parse per source, \
        holding non-dominant candidates for explicit disposition while the predecessor \
        keeps serving canonical evidence to the retrieval fabric. ";
    vec![
        "short passage".to_string(),
        "The importer owns every canonical write: it dedups by content hash, writes raw \
         bytes to the artifact store before the SQL transaction, and records provenance \
         for every acquisition attempt including failures."
            .to_string(),
        "Retrieval projections are disposable targeting artifacts; canonical content \
         units are the only admissible evidence, and every evidence pack traces each \
         inclusion to a retrieval hit or an assembly-policy rule."
            .to_string(),
        "Q3 revenue was $14,203,551.07 — up 12.4% year-over-year; margin compressed 310 \
         basis points to 41.2% while headcount grew from 1,847 to 2,215."
            .to_string(),
        "Übersetzungsqualität hängt von präziser Terminologie ab; 自然言語処理は難しい; \
         résumé naïve façade — mixed-script content with combining marks."
            .to_string(),
        long_sentence.repeat(18),
    ]
}

/// Cosine similarity over two equal-length vectors, 0.0 when either norm is
/// zero (a zero norm cannot pass the 0.99 gate, which is the honest outcome).
fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a = a.iter().map(|v| v * v).sum::<f32>().sqrt();
    let norm_b = b.iter().map(|v| v * v).sum::<f32>().sqrt();
    let denom = norm_a * norm_b;
    if denom == 0.0 { 0.0 } else { dot / denom }
}

// ---------------------------------------------------------------------------
// Part 1 — correctness probes (Metal vs CPU)
// ---------------------------------------------------------------------------

/// Compare individual candle ops on Metal against a CPU reference at batch > 1,
/// per batch row, in F32 and BF16. The op set mirrors every batched-tensor
/// operation the qwen3 forward performs; the repeat_kv probes are the minimal
/// reproducer for the permuted-stride Metal matmul defect.
fn run_metal_op_probes(metal: &Device) {
    for dtype in [DType::F32, DType::BF16] {
        compare_op(metal, dtype, "matmul4_qk_transposed", 3, |device, dtype| {
            let q = seeded_tensor(&[3, 16, 16, 128], 1, device, dtype)?;
            let k = seeded_tensor(&[3, 16, 16, 128], 2, device, dtype)?;
            q.matmul(&k.t()?)
        });
        compare_op(
            metal,
            dtype,
            "matmul4_contiguous_rhs",
            3,
            |device, dtype| {
                let q = seeded_tensor(&[3, 16, 16, 128], 1, device, dtype)?;
                let k = seeded_tensor(&[3, 16, 16, 128], 2, device, dtype)?;
                q.matmul(&k.t()?.contiguous()?)
            },
        );
        compare_op(metal, dtype, "matmul3_flattened", 3, |device, dtype| {
            let q = seeded_tensor(&[48, 16, 128], 1, device, dtype)?;
            let k = seeded_tensor(&[48, 16, 128], 2, device, dtype)?;
            q.matmul(&k.t()?)
        });
        compare_op(metal, dtype, "linear_forward", 3, |device, dtype| {
            let input = seeded_tensor(&[3, 16, 1024], 1, device, dtype)?;
            let weight = seeded_tensor(&[512, 1024], 2, device, dtype)?;
            candle_nn::Linear::new(weight, None).forward(&input)
        });
        compare_op(metal, dtype, "broadcast_add_mask", 3, |device, dtype| {
            let scores = seeded_tensor(&[3, 16, 16, 16], 1, device, dtype)?;
            let mask = seeded_tensor(&[1, 1, 16, 16], 2, device, dtype)?;
            scores.broadcast_add(&mask)
        });
        compare_op(metal, dtype, "softmax_composite", 3, |device, dtype| {
            let scores = seeded_tensor(&[3, 16, 13, 13], 1, device, dtype)?;
            let scores = scores.to_dtype(DType::F32)?;
            let max = scores.max_keepdim(D::Minus1)?;
            let exp = scores.broadcast_sub(&max)?.exp()?;
            let denominator = exp.sum_keepdim(D::Minus1)?;
            exp.broadcast_div(&denominator)?.to_dtype(dtype)
        });
        compare_op(metal, dtype, "rmsnorm_composite", 3, |device, dtype| {
            let hidden = seeded_tensor(&[3, 13, 1024], 1, device, dtype)?;
            let weight = seeded_tensor(&[1024], 2, device, dtype)?;
            let variance = hidden.sqr()?.mean_keepdim(D::Minus1)?;
            let normed = hidden.broadcast_div(&(variance + 1e-6)?.sqrt()?)?;
            normed.broadcast_mul(&weight)
        });
        // SMOKING-GUN probe: matmul whose rhs is the repeat_kv cat output — a
        // stride-permuted view (buffer ordered [heads, batch, seq, dim]) that
        // CPU matmul REJECTS as non-contiguous but Metal accepts. The CPU
        // reference gets an explicit .contiguous() (value-preserving) so the
        // comparison can run; a Metal mismatch on rows >= 1 convicts Metal's
        // handling of the permuted batch stride.
        compare_op(metal, dtype, "matmul4_repeat_kv_rhs", 3, |device, dtype| {
            let q = seeded_tensor(&[3, 16, 13, 128], 1, device, dtype)?;
            let k = repeat_kv_cat(&seeded_tensor(&[3, 8, 13, 128], 2, device, dtype)?)?;
            let k_t = k.t()?;
            let k_t = if matches!(device, Device::Cpu) {
                k_t.contiguous()?
            } else {
                k_t
            };
            q.matmul(&k_t)
        });
        // Fix-candidate probe: same computation with .contiguous() on BOTH
        // devices — passing where the raw probe fails validates the
        // repeat_kv_heads materialization workaround.
        compare_op(
            metal,
            dtype,
            "matmul4_repeat_kv_rhs_contiguous",
            3,
            |device, dtype| {
                let q = seeded_tensor(&[3, 16, 13, 128], 1, device, dtype)?;
                let k = repeat_kv_cat(&seeded_tensor(&[3, 8, 13, 128], 2, device, dtype)?)?
                    .contiguous()?;
                q.matmul(&k.t()?.contiguous()?)
            },
        );
        // The second production consumer shape: probs @ v with v as the
        // UN-transposed permuted-stride rhs.
        compare_op(
            metal,
            dtype,
            "matmul4_probs_repeat_kv_v",
            3,
            |device, dtype| {
                let probs = seeded_tensor(&[3, 16, 13, 13], 1, device, dtype)?;
                let v = repeat_kv_cat(&seeded_tensor(&[3, 8, 13, 128], 3, device, dtype)?)?;
                let v = if matches!(device, Device::Cpu) {
                    v.contiguous()?
                } else {
                    v
                };
                probs.matmul(&v)
            },
        );
        // Layout introspection: the strides candle actually gives the
        // repeat_kv cat output and its transpose.
        if dtype == DType::F32 {
            match seeded_tensor(&[3, 8, 13, 128], 2, metal, dtype).and_then(|k| repeat_kv_cat(&k)) {
                Ok(repeated) => {
                    println!(
                        "op_probe=layout repeat_kv_cat layout={:?}",
                        repeated.layout()
                    );
                    if let Ok(transposed) = repeated.t() {
                        println!(
                            "op_probe=layout repeat_kv_cat_t layout={:?}",
                            transposed.layout()
                        );
                    }
                }
                Err(source) => println!("op_probe=layout FAILED error={source}"),
            }
        }
    }
}

/// The qwen3 `repeat_kv_heads` body WITHOUT the `.contiguous()` workaround:
/// narrow each of 8 kv heads, duplicate, cat along dim 1 — deliberately
/// reproducing the stride-permuted layout the workaround neutralizes.
fn repeat_kv_cat(states: &Tensor) -> candle_core::Result<Tensor> {
    let mut heads = Vec::with_capacity(16);
    for head_index in 0..8 {
        let head = states.narrow(1, head_index, 1)?;
        heads.push(head.clone());
        heads.push(head);
    }
    let head_refs = heads.iter().collect::<Vec<_>>();
    Tensor::cat(&head_refs, 1)
}

/// Run `op` on Metal at the probed dtype and on CPU at F32 (the always-supported
/// reference — CPU lacks some BF16 kernels), with identical deterministic
/// inputs, then print per-batch-row max-abs-diff and non-finite counts. A
/// correct kernel shows only dtype-level rounding drift (BF16 ~1e-2) on every
/// row; a defective kernel shows garbage or NaN on rows >= 1. Failures are
/// labeled with the side that threw so an unsupported-dtype rejection on one
/// backend is not misread as the other's.
fn compare_op(
    metal: &Device,
    dtype: DType,
    name: &str,
    rows: usize,
    op: impl Fn(&Device, DType) -> candle_core::Result<Tensor>,
) {
    let metal_outcome = op(metal, dtype).and_then(|tensor| {
        tensor
            .to_dtype(DType::F32)?
            .to_device(&Device::Cpu)?
            .flatten_all()?
            .to_vec1::<f32>()
    });
    let metal_out = match metal_outcome {
        Ok(values) => values,
        Err(source) => {
            println!("op_probe={name} dtype={dtype:?} FAILED side=metal error={source}");
            return;
        }
    };
    let cpu_outcome =
        op(&Device::Cpu, DType::F32).and_then(|tensor| tensor.flatten_all()?.to_vec1::<f32>());
    let cpu_out = match cpu_outcome {
        Ok(values) => values,
        Err(source) => {
            println!(
                "op_probe={name} dtype={dtype:?} FAILED side=cpu_f32_reference error={source}"
            );
            return;
        }
    };

    let row_len = metal_out.len() / rows;
    for row in 0..rows {
        let metal_row = &metal_out[row * row_len..(row + 1) * row_len];
        let cpu_row = &cpu_out[row * row_len..(row + 1) * row_len];
        let max_abs_diff = metal_row
            .iter()
            .zip(cpu_row.iter())
            .map(|(m, c)| (m - c).abs())
            .fold(0.0f32, f32::max);
        let nonfinite = metal_row.iter().filter(|v| !v.is_finite()).count();
        println!(
            "op_probe={name} dtype={dtype:?} row={row} max_abs_diff={max_abs_diff:.6} nonfinite={nonfinite}"
        );
    }
}

// ---------------------------------------------------------------------------
// Part 1b (`--probe-batched-sanitize`) — CPd batch-consistency smoke failure
// diagnosis (2026-07-18). Three hypotheses for the observed NaN on a REAL
// token of the most-padded document:
//   H1: candle-Metal miscomputes the stride-0 `broadcast_mul` sanitize.
//   H2: batched mask misalignment across the (B*heads) slab ordering.
//   H3: IEEE semantics — NaN * 0.0 = NaN, so multiplying a fully-masked
//       softmax NaN row by a 0/1 validity mask cannot clear it, and the NaN
//       re-enters the next layer's K/V and spreads to real rows.
// The probes are pure tensor work (no model weights) and reproduce the
// production chain shape-faithfully in miniature.
// ---------------------------------------------------------------------------

/// Run the four sanitize probes: NaN*0 semantics (H3, per device), finite
/// stride-0 broadcast_mul correctness (H1), and the full two-layer sanitize
/// chain under both the shipped `broadcast_mul` sanitizer and the
/// `where_cond` fix candidate.
fn run_batched_sanitize_probes(metal: &Device) {
    probe_nan_times_zero(metal);
    // H1 probe: finite hidden-state rows times a (rows, 1) 0/1 validity mask,
    // Metal vs CPU. A correct kernel zeroes rows 2 and 4 exactly and leaves
    // the rest bit-identical to the CPU reference.
    compare_op(
        metal,
        DType::F32,
        "broadcast_mul_row_validity_finite",
        6,
        |device, dtype| {
            let hidden = seeded_tensor(&[6, 768], 1, device, dtype)?;
            let mask = Tensor::from_vec(vec![1.0f32, 1.0, 0.0, 1.0, 0.0, 1.0], (6, 1), device)?
                .to_dtype(dtype)?;
            hidden.broadcast_mul(&mask)
        },
    );
    probe_sanitize_chain(metal);
}

/// H3 probe: rows 2 and 5 of a finite matrix are set to NaN (emulating
/// fully-masked softmax output rows), then multiplied by a (rows, 1) mask
/// that is 0.0 exactly on those rows. IEEE 754 predicts the NaN rows stay
/// NaN on EVERY backend; a zero count would refute H3.
fn probe_nan_times_zero(metal: &Device) {
    for (device_label, device) in [("metal", metal), ("cpu", &Device::Cpu)] {
        match nan_times_zero_row_counts(device) {
            Ok(per_row_nonfinite) => println!(
                "sanitize_probe=nan_times_zero device={device_label} per_row_nonfinite={per_row_nonfinite:?} (rows 2 and 5 were NaN, mask zeroes them)"
            ),
            Err(source) => {
                println!(
                    "sanitize_probe=nan_times_zero device={device_label} FAILED error={source}"
                )
            }
        }
    }
}

/// Build the NaN-rows-times-zero-mask product on one device and return the
/// per-row non-finite value counts of the result.
fn nan_times_zero_row_counts(device: &Device) -> candle_core::Result<Vec<usize>> {
    let mut values = vec![0.5f32; 6 * 8];
    for column in 0..8 {
        values[2 * 8 + column] = f32::NAN;
        values[5 * 8 + column] = f32::NAN;
    }
    let hidden = Tensor::from_vec(values, (6, 8), device)?;
    let mask = Tensor::from_vec(vec![1.0f32, 1.0, 0.0, 1.0, 1.0, 0.0], (6, 1), device)?;
    let product = hidden
        .broadcast_mul(&mask)?
        .to_device(&Device::Cpu)?
        .to_vec2::<f32>()?;
    Ok(product
        .iter()
        .map(|row| row.iter().filter(|value| !value.is_finite()).count())
        .collect())
}

/// Miniature of the production failure chain, run under both sanitizer
/// modes on both devices. One padded document slab: seq 8, true length 3.
/// Layer 1 is a "local" layer whose padded query rows 5..8 fall outside
/// every real key's window (all scores -inf -> softmax NaN). The sanitizer
/// then zeroes rows >= 3, layer 2 projects K from the sanitized output, and
/// the probe reports whether the REAL query rows (0..3) of layer 2's
/// softmax are still finite — the production smoke's exact failure surface.
fn probe_sanitize_chain(metal: &Device) {
    for (device_label, device) in [("metal", metal), ("cpu", &Device::Cpu)] {
        for (mode, use_where_cond) in [("broadcast_mul", false), ("where_cond", true)] {
            match sanitize_chain_real_row_nonfinite(device, use_where_cond) {
                Ok((layer1_nan_rows, real_row_nonfinite)) => println!(
                    "sanitize_probe=chain device={device_label} sanitizer={mode} layer1_nan_rows={layer1_nan_rows} layer2_real_row_nonfinite={real_row_nonfinite:?}"
                ),
                Err(source) => println!(
                    "sanitize_probe=chain device={device_label} sanitizer={mode} FAILED error={source}"
                ),
            }
        }
    }
}

/// Execute the two-layer chain with the selected sanitizer and return
/// (count of NaN rows after layer 1's softmax*V, per-real-row non-finite
/// counts in layer 2's softmax output).
fn sanitize_chain_real_row_nonfinite(
    device: &Device,
    use_where_cond: bool,
) -> candle_core::Result<(usize, Vec<usize>)> {
    const SEQ: usize = 8;
    const TRUE_LEN: usize = 3;
    const HIDDEN: usize = 16;

    // Layer 1 scores with the production mask composition: key-padding
    // (-inf on padded key columns >= TRUE_LEN for every row) plus a local
    // window that fully masks padded query rows 5..8 (every key -inf).
    let mut score_values = Vec::with_capacity(SEQ * SEQ);
    for row in 0..SEQ {
        for column in 0..SEQ {
            let seeded = ((row * SEQ + column) as f32 * 0.37).sin();
            let masked = column >= TRUE_LEN || row >= 5;
            score_values.push(if masked { f32::NEG_INFINITY } else { seeded });
        }
    }
    let scores = Tensor::from_vec(score_values, (SEQ, SEQ), device)?;
    let probs = softmax_last_dim(&scores)?;
    let v = seeded_tensor(&[SEQ, HIDDEN], 3, device, DType::F32)?;
    let attention_out = probs.matmul(&v)?;
    let layer1_rows = attention_out.to_device(&Device::Cpu)?.to_vec2::<f32>()?;
    let layer1_nan_rows = layer1_rows
        .iter()
        .filter(|row| row.iter().any(|value| !value.is_finite()))
        .count();

    // Sanitize rows >= TRUE_LEN with the probed mechanism.
    let mut validity_values = Vec::with_capacity(SEQ);
    for row in 0..SEQ {
        validity_values.push(if row < TRUE_LEN { 1.0f32 } else { 0.0f32 });
    }
    let validity = Tensor::from_vec(validity_values, (SEQ, 1), device)?;
    let sanitized = if use_where_cond {
        // Fix candidate: a select never performs arithmetic with the NaN,
        // so masked rows become exactly zero regardless of their contents.
        // The condition is materialized contiguous before the select to stay
        // clear of the candle-Metal strided-view defect class.
        let condition = validity
            .broadcast_as((SEQ, HIDDEN))?
            .contiguous()?
            .to_dtype(DType::U8)?;
        let zeros = Tensor::zeros((SEQ, HIDDEN), DType::F32, device)?;
        condition.where_cond(&attention_out, &zeros)?
    } else {
        // Shipped sanitizer: encode_batched's exact mechanism.
        attention_out.broadcast_mul(&validity)?
    };

    // Layer 2: K projected from the sanitized layer-1 output (bias-free, as
    // in production), scores against a fresh finite Q, key-padding re-applied,
    // softmax. If any sanitized row carried NaN into K, every real row's
    // score column for that key is NaN and the row-wise softmax spreads it
    // across the whole real row.
    let k_weight = seeded_tensor(&[HIDDEN, HIDDEN], 5, device, DType::F32)?;
    let k = sanitized.matmul(&k_weight.t()?)?;
    let q = seeded_tensor(&[SEQ, HIDDEN], 7, device, DType::F32)?;
    let raw_scores = q.matmul(&k.t()?)?;
    let mut key_mask_values = Vec::with_capacity(SEQ);
    for column in 0..SEQ {
        key_mask_values.push(if column < TRUE_LEN {
            0.0f32
        } else {
            f32::NEG_INFINITY
        });
    }
    let key_mask = Tensor::from_vec(key_mask_values, (1, SEQ), device)?;
    let masked_scores = raw_scores.broadcast_add(&key_mask)?;
    let probs2 = softmax_last_dim(&masked_scores)?;

    let rows = probs2.to_device(&Device::Cpu)?.to_vec2::<f32>()?;
    let real_row_nonfinite = rows
        .iter()
        .take(TRUE_LEN)
        .map(|row| row.iter().filter(|value| !value.is_finite()).count())
        .collect();
    Ok((layer1_nan_rows, real_row_nonfinite))
}

// The chain probe reuses the shared `softmax_last_dim` helper defined with
// the part 3 benchmarks below. Note for probe reading: a fully -inf row
// yields NaN by construction there (max = -inf, x - max = NaN) — that is
// the behavior under probe, not a helper defect.

// ---------------------------------------------------------------------------
// Part 2 — dense dtype throughput benchmark (Qwen3-8B layer shapes)
// ---------------------------------------------------------------------------

/// Qwen3-Embedding-8B dims from models/Qwen3-Embedding-8B/config.json.
const DENSE_HIDDEN: usize = 4096;
const DENSE_INTERMEDIATE: usize = 12288;
const DENSE_KV_PROJ: usize = 8 * 128; // num_key_value_heads * head_dim

/// Measured average passage length from the commissioned corpus (291 tokens).
const DENSE_BENCH_SEQ: usize = 291;

const BENCH_WARMUP: usize = 3;
const BENCH_ITERS: usize = 10;

/// Time one synthetic Qwen3 layer's dominant linear matmuls (q/k/v/o + gated
/// MLP) at B=1 on Metal per dtype. Attention-core matmuls, norms, and RoPE are
/// omitted: they are a small fraction of per-layer FLOPs, and the decision
/// datum is the relative dtype cost of the dominant matmul kernels. Each
/// iteration ends in a scalar readback, mirroring the real per-call readback
/// and forcing Metal completion so wall-clock is honest.
fn bench_dense_dtypes(metal: &Device) {
    for dtype in [DType::BF16, DType::F16, DType::F32] {
        let outcome = (|| -> candle_core::Result<f64> {
            let x = seeded_tensor(&[1, DENSE_BENCH_SEQ, DENSE_HIDDEN], 1, metal, dtype)?;
            let wq = seeded_weight(&[DENSE_HIDDEN, DENSE_HIDDEN], 2, metal, dtype)?;
            let wk = seeded_weight(&[DENSE_KV_PROJ, DENSE_HIDDEN], 3, metal, dtype)?;
            let wv = seeded_weight(&[DENSE_KV_PROJ, DENSE_HIDDEN], 4, metal, dtype)?;
            let wo = seeded_weight(&[DENSE_HIDDEN, DENSE_HIDDEN], 5, metal, dtype)?;
            let wgate = seeded_weight(&[DENSE_INTERMEDIATE, DENSE_HIDDEN], 6, metal, dtype)?;
            let wup = seeded_weight(&[DENSE_INTERMEDIATE, DENSE_HIDDEN], 7, metal, dtype)?;
            let wdown = seeded_weight(&[DENSE_HIDDEN, DENSE_INTERMEDIATE], 8, metal, dtype)?;
            let linears = [&wq, &wk, &wv, &wo, &wgate, &wup, &wdown]
                .map(|weight| candle_nn::Linear::new(weight.clone(), None));
            let [lq, lk, lv, lo, lgate, lup, ldown] = linears;

            time_iterations(|| {
                let q = lq.forward(&x)?;
                let k = lk.forward(&x)?;
                let v = lv.forward(&x)?;
                let o = lo.forward(&q)?;
                let h = (lgate.forward(&x)? * lup.forward(&x)?)?;
                let mlp = ldown.forward(&h)?;
                // Fold every output into one scalar so no matmul is dead code
                // to the device and one readback synchronizes the iteration.
                ((o.sum_all()? + mlp.sum_all()?)? + (k.sum_all()? + v.sum_all()?)?)?
                    .to_dtype(DType::F32)
            })
        })();
        match outcome {
            Ok(ms) => println!(
                "bench=dense_dtype dtype={dtype:?} seq={DENSE_BENCH_SEQ} ms_per_layer_linears={ms:.2}"
            ),
            Err(source) => println!("bench=dense_dtype dtype={dtype:?} FAILED error={source}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Part 3 — ColBERT batching throughput benchmark (ModernBERT layer shapes)
// ---------------------------------------------------------------------------

/// ModernBERT dims from models/ColBERT-Zero/config.json (GLU MLP: Wi projects
/// to 2*intermediate, halves are gated then Wo projects back).
const CB_HIDDEN: usize = 768;
const CB_HEADS: usize = 12;
const CB_HEAD_DIM: usize = 64;
const CB_INTERMEDIATE: usize = 1152;
const CB_SEQ: usize = 128;
const CB_DOCS: usize = 16;

/// Compare one synthetic ModernBERT layer's work for CB_DOCS documents in the
/// runtime's current shape (per-document rank-2 tensors, per-head 2D attention
/// matmuls, per-document readback) against batched execution (one rank-3/rank-4
/// pass over all documents, one readback). F32 — the ColBERT load dtype.
/// Masks, embeddings, and the output projection are omitted from BOTH modes;
/// the single mode's per-head mask ops would only widen the batched win, so the
/// measured ratio is a conservative lower bound.
fn bench_colbert_batching(metal: &Device) {
    let outcome = (|| -> candle_core::Result<(f64, f64)> {
        let dtype = DType::F32;
        let wqkv = seeded_weight(&[3 * CB_HIDDEN, CB_HIDDEN], 2, metal, dtype)?;
        let wo = seeded_weight(&[CB_HIDDEN, CB_HIDDEN], 3, metal, dtype)?;
        let wi = seeded_weight(&[2 * CB_INTERMEDIATE, CB_HIDDEN], 4, metal, dtype)?;
        let wo_mlp = seeded_weight(&[CB_HIDDEN, CB_INTERMEDIATE], 5, metal, dtype)?;

        // Current-runtime shape: rank-2 per document, per-head 2D matmuls,
        // one readback per document (the runtime reads each document's
        // embedding matrix back to CPU as it is produced).
        let docs: Vec<Tensor> = (0..CB_DOCS)
            .map(|doc| seeded_tensor(&[CB_SEQ, CB_HIDDEN], 10 + doc as u64, metal, dtype))
            .collect::<candle_core::Result<Vec<_>>>()?;
        let single_ms = time_iterations(|| {
            let mut total = 0f32;
            for x in &docs {
                let qkv = x.matmul(&wqkv.t()?)?;
                let q = qkv.narrow(1, 0, CB_HIDDEN)?;
                let k = qkv.narrow(1, CB_HIDDEN, CB_HIDDEN)?;
                let v = qkv.narrow(1, 2 * CB_HIDDEN, CB_HIDDEN)?;
                let mut head_outputs = Vec::with_capacity(CB_HEADS);
                for head in 0..CB_HEADS {
                    // The real runtime extracts each head via narrow+reshape,
                    // which materializes a contiguous copy (Metal rejects the
                    // strided narrow directly); pay the same copy here.
                    let q_h = q.narrow(1, head * CB_HEAD_DIM, CB_HEAD_DIM)?.contiguous()?;
                    let k_h = k.narrow(1, head * CB_HEAD_DIM, CB_HEAD_DIM)?.contiguous()?;
                    let v_h = v.narrow(1, head * CB_HEAD_DIM, CB_HEAD_DIM)?.contiguous()?;
                    let scores = (q_h.matmul(&k_h.t()?)? / (CB_HEAD_DIM as f64).sqrt())?;
                    let probs = softmax_last_dim(&scores)?;
                    head_outputs.push(probs.matmul(&v_h)?);
                }
                let head_refs = head_outputs.iter().collect::<Vec<_>>();
                let attention = Tensor::cat(&head_refs, 1)?;
                let projected = attention.matmul(&wo.t()?)?;
                let glu = projected.matmul(&wi.t()?)?;
                let gated = (glu.narrow(1, 0, CB_INTERMEDIATE)?
                    * glu.narrow(1, CB_INTERMEDIATE, CB_INTERMEDIATE)?)?;
                let out = gated.matmul(&wo_mlp.t()?)?;
                total += out.sum_all()?.to_scalar::<f32>()?;
            }
            Tensor::new(total, &Device::Cpu)
        })?;

        // Batched shape: linears flattened to one rank-2 matmul over all
        // documents' tokens, attention flattened to rank-3 over (docs*heads) —
        // the layouts the correctness probes showed Metal computes exactly.
        // `.contiguous()` after each stride-permuting reshape (the lesson of
        // the repeat_kv defect). One readback for the whole batch.
        let batch = Tensor::stack(&docs.iter().collect::<Vec<_>>(), 0)?;
        let flat_tokens = CB_DOCS * CB_SEQ;
        let batched_ms = time_iterations(|| {
            let flat = batch.reshape((flat_tokens, CB_HIDDEN))?;
            let qkv = flat.matmul(&wqkv.t()?)?;
            // (docs*seq, 3*hidden) -> per-projection (docs*heads, seq, head_dim)
            let split_heads = |offset: usize| -> candle_core::Result<Tensor> {
                qkv.narrow(1, offset, CB_HIDDEN)?
                    .reshape((CB_DOCS, CB_SEQ, CB_HEADS, CB_HEAD_DIM))?
                    .transpose(1, 2)?
                    .contiguous()?
                    .reshape((CB_DOCS * CB_HEADS, CB_SEQ, CB_HEAD_DIM))
            };
            let q = split_heads(0)?;
            let k = split_heads(CB_HIDDEN)?;
            let v = split_heads(2 * CB_HIDDEN)?;
            let scores = (q.matmul(&k.t()?)? / (CB_HEAD_DIM as f64).sqrt())?;
            let probs = softmax_last_dim(&scores)?;
            let attention = probs
                .matmul(&v)?
                .reshape((CB_DOCS, CB_HEADS, CB_SEQ, CB_HEAD_DIM))?
                .transpose(1, 2)?
                .contiguous()?
                .reshape((flat_tokens, CB_HIDDEN))?;
            let projected = attention.matmul(&wo.t()?)?;
            let glu = projected.matmul(&wi.t()?)?;
            let gated = (glu.narrow(1, 0, CB_INTERMEDIATE)?
                * glu.narrow(1, CB_INTERMEDIATE, CB_INTERMEDIATE)?)?;
            let out = gated.matmul(&wo_mlp.t()?)?;
            out.sum_all()?.to_dtype(DType::F32)
        })?;

        Ok((single_ms, batched_ms))
    })();

    match outcome {
        Ok((single_ms, batched_ms)) => {
            println!(
                "bench=colbert_batch mode=single_per_doc docs={CB_DOCS} seq={CB_SEQ} ms_per_layer={single_ms:.2}"
            );
            println!(
                "bench=colbert_batch mode=batched docs={CB_DOCS} seq={CB_SEQ} ms_per_layer={batched_ms:.2} speedup={:.2}x",
                single_ms / batched_ms
            );
        }
        Err(source) => println!("bench=colbert_batch FAILED error={source}"),
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Numerically stable softmax over the last dim using only primitive ops (the
/// same shape as the service's metal-safe softmax; inputs here are already F32).
fn softmax_last_dim(scores: &Tensor) -> candle_core::Result<Tensor> {
    let max = scores.max_keepdim(D::Minus1)?;
    let exp = scores.broadcast_sub(&max)?.exp()?;
    let denominator = exp.sum_keepdim(D::Minus1)?;
    exp.broadcast_div(&denominator)
}

/// Run `op` BENCH_WARMUP times untimed, then BENCH_ITERS times timed, and
/// return the mean milliseconds per iteration. `op` must end in a small
/// CPU-visible tensor (its construction forces Metal completion); the returned
/// tensor is read back to a vec here as the synchronization point.
fn time_iterations(op: impl Fn() -> candle_core::Result<Tensor>) -> candle_core::Result<f64> {
    for _ in 0..BENCH_WARMUP {
        sync_readback(&op()?)?;
    }
    let started = Instant::now();
    for _ in 0..BENCH_ITERS {
        sync_readback(&op()?)?;
    }
    Ok(started.elapsed().as_secs_f64() * 1000.0 / BENCH_ITERS as f64)
}

/// Force device completion by copying the (small) tensor to CPU memory.
fn sync_readback(tensor: &Tensor) -> candle_core::Result<()> {
    tensor
        .to_device(&Device::Cpu)?
        .flatten_all()?
        .to_vec1::<f32>()?;
    Ok(())
}

/// Deterministic pseudo-random tensor in [-0.5, 0.5), reproducible across
/// devices so Metal and CPU compute over identical inputs.
fn seeded_tensor(
    shape: &[usize],
    seed: u64,
    device: &Device,
    dtype: DType,
) -> candle_core::Result<Tensor> {
    seeded_tensor_scaled(shape, seed, 1.0, device, dtype)
}

/// Weight-scaled variant (values in [-0.01, 0.01)) so chained benchmark matmuls
/// keep activations O(1) — necessary for F16, whose exponent range overflows
/// under unscaled 4096-term dot products.
fn seeded_weight(
    shape: &[usize],
    seed: u64,
    device: &Device,
    dtype: DType,
) -> candle_core::Result<Tensor> {
    seeded_tensor_scaled(shape, seed, 0.02, device, dtype)
}

/// Shared deterministic value generator behind `seeded_tensor`/`seeded_weight`.
fn seeded_tensor_scaled(
    shape: &[usize],
    seed: u64,
    scale: f32,
    device: &Device,
    dtype: DType,
) -> candle_core::Result<Tensor> {
    let count: usize = shape.iter().product();
    let values: Vec<f32> = (0..count)
        .map(|index| {
            let mixed = (index as u64)
                .wrapping_mul(1103515245)
                .wrapping_add(seed.wrapping_mul(12345));
            ((mixed % 1000) as f32 / 1000.0 - 0.5) * scale
        })
        .collect();
    Tensor::from_vec(values, shape, device)?.to_dtype(dtype)
}
