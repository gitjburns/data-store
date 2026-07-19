#![allow(dead_code, unused_imports)]

use std::{env, path::PathBuf, time::Instant};

#[path = "../config.rs"]
mod config;
#[path = "../error.rs"]
mod error;
#[path = "../inference/mod.rs"]
mod inference;
// Required by the included inference sources: device.rs renders panic
// payloads through crate::util, which this bin crate must therefore declare.
#[path = "../util.rs"]
mod util;

use config::ServiceConfig;
use error::ApiError;
use inference::InferenceRuntime;

/// Run the isolated ColBERT runtime initialization path without loading dense or reranker models.
fn main() -> Result<(), ApiError> {
    let config_path = resolve_config_path()?;
    let config = ServiceConfig::load(config_path.clone())?;
    println!("colbert diagnostic config={}", config_path.display());

    let started = Instant::now();
    let mut progress = |message: &str| {
        println!("colbert diagnostic progress={message}");
        Ok(())
    };
    let colbert = InferenceRuntime::initialize_colbert_only_with_progress(&config, &mut progress)?;

    println!(
        "colbert diagnostic ready elapsed_ms={}",
        started.elapsed().as_millis()
    );
    for detail in colbert.health_details() {
        println!("{detail}");
    }

    let document = diagnostic_document();
    let embedding = colbert.embed_document("diagnostic-unit", &document)?;
    println!(
        "colbert diagnostic document_embedding unit_id={} tokens={} dimension={} values={} norm_min={:.6} norm_max={:.6}",
        embedding.unit_id,
        embedding.token_count,
        embedding.dimension,
        embedding.vector.len(),
        min_token_norm(&embedding.vector, embedding.dimension),
        max_token_norm(&embedding.vector, embedding.dimension)
    );
    let scores = colbert.score_persisted_candidates(
        "clear writing style rules",
        std::slice::from_ref(&embedding),
    )?;
    let score = scores.first().ok_or_else(|| ApiError::InferenceInit {
        message: "ColBERT diagnostic produced no candidate scores".to_string(),
    })?;
    println!(
        "colbert diagnostic maxsim unit_id={} score={:.6} rank={} query_tokens={} document_tokens={}",
        score.unit_id, score.score, score.rank, score.query_tokens, score.document_tokens
    );

    Ok(())
}

/// Resolve the optional `--config` path while keeping the diagnostic command surface narrow.
fn resolve_config_path() -> Result<PathBuf, ApiError> {
    let mut args = env::args().skip(1);
    let mut config_path = PathBuf::from("config.toml");

    while let Some(arg) = args.next() {
        if arg == "--config" {
            let Some(value) = args.next() else {
                return Err(ApiError::InvalidCli {
                    message: "--config requires a path".to_string(),
                });
            };
            config_path = PathBuf::from(value);
            continue;
        }

        return Err(ApiError::InvalidCli {
            message: format!("unknown argument: {arg}"),
        });
    }

    Ok(config_path)
}

/// Build a long document that forces the public ColBERT document path to use configured truncation.
fn diagnostic_document() -> String {
    let sentence = "Prefer specific words and direct sentences when explaining technical changes. ";
    sentence.repeat(200)
}

/// Return the smallest per-token vector norm in a flattened row-major embedding matrix.
fn min_token_norm(values: &[f32], dimension: usize) -> f32 {
    values
        .chunks(dimension)
        .map(token_norm)
        .fold(f32::INFINITY, f32::min)
}

/// Return the largest per-token vector norm in a flattened row-major embedding matrix.
fn max_token_norm(values: &[f32], dimension: usize) -> f32 {
    values
        .chunks(dimension)
        .map(token_norm)
        .fold(f32::NEG_INFINITY, f32::max)
}

/// Compute the Euclidean norm for one token vector.
fn token_norm(values: &[f32]) -> f32 {
    values.iter().map(|value| value * value).sum::<f32>().sqrt()
}
