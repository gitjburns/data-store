//! Readiness metadata binds configured capacities to the actual HTTP serving engine.

use std::{io::Read, time::Instant};

use reqwest::{Url, blocking::Client};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::{error::ApiError, limits::RuntimeLimits};

/// Stable, advertised model facts; timestamps and permissions cannot invalidate embeddings.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct ServedModel {
    pub id: String,
    pub root: String,
    pub max_model_len: u32,
}

/// Only advertised model identities and capacities are needed from the provider payload.
#[derive(Deserialize)]
struct ModelList {
    data: Vec<ServedModel>,
}

/// Reject missing, ambiguous, or mismatched serving capacity before inference can become ready.
pub(super) fn verify_capacity(
    client: &Client,
    endpoint: &str,
    role: &'static str,
    model: &str,
    expected: u32,
    api_key: Option<&str>,
    limits: &RuntimeLimits,
) -> Result<ServedModel, ApiError> {
    let started = Instant::now();
    info!(
        event = "model_capacity.started",
        model_role = role,
        endpoint,
        model,
        expected_max_tokens = expected,
        "HTTP model capacity verification started"
    );
    let result = read_capacity(client, endpoint, model, expected, api_key, limits);
    match &result {
        Ok(served) => info!(event = "model_capacity.completed", model_role = role, endpoint, model,
            expected_max_tokens = expected, advertised_max_tokens = served.max_model_len,
            served_checkpoint = %served.root, elapsed_ms = started.elapsed().as_millis() as u64,
            "HTTP model capacity matches configured capacity"),
        Err(source) => error!(event = "model_capacity.failed", model_role = role, endpoint, model,
            expected_max_tokens = expected, elapsed_ms = started.elapsed().as_millis() as u64,
            error = %source, "HTTP model capacity verification failed"),
    }
    result
}

/// Use the same configured origin and timeout, with a bounded metadata body and no alternate route.
fn read_capacity(
    client: &Client,
    endpoint: &str,
    model: &str,
    expected: u32,
    api_key: Option<&str>,
    limits: &RuntimeLimits,
) -> Result<ServedModel, ApiError> {
    let fail = |message: String| ApiError::InferenceInit {
        message: format!("HTTP model metadata for {endpoint} model {model}: {message}"),
    };
    let mut url =
        Url::parse(endpoint).map_err(|source| fail(format!("invalid endpoint URL: {source}")))?;
    url.set_path("/v1/models");
    url.set_query(None);
    url.set_fragment(None);
    let mut request = client.get(url);
    if let Some(key) = api_key {
        request = request.bearer_auth(key);
    }
    let response = request.send().map_err(|source| {
        fail(format!(
            "metadata request failed: {}",
            crate::util::error_chain(&source, &limits.diagnostics)
        ))
    })?;
    let status = response.status();
    let max_bytes = limits.resources.model_metadata_max_bytes;
    if response
        .content_length()
        .is_some_and(|bytes| bytes > max_bytes as u64)
    {
        return Err(fail(format!(
            "metadata body after status {status} exceeds resources.model_metadata_max_bytes {max_bytes}"
        )));
    }
    // Read one sentinel byte beyond the ceiling to detect a missing or inaccurate Content-Length.
    let mut raw = Vec::new();
    response
        .take((max_bytes as u64).saturating_add(1))
        .read_to_end(&mut raw)
        .map_err(|source| {
            fail(format!(
                "metadata body read after status {status}: {}",
                crate::util::error_chain(&source, &limits.diagnostics)
            ))
        })?;
    if raw.len() > max_bytes {
        return Err(fail(format!(
            "metadata body after status {status} exceeds resources.model_metadata_max_bytes {max_bytes}"
        )));
    }
    if !status.is_success() {
        let excerpt = failure_excerpt(
            &String::from_utf8_lossy(&raw),
            api_key,
            limits.diagnostics.model_error_excerpt_chars,
        );
        return Err(fail(format!(
            "metadata HTTP status {status}; body_excerpt={excerpt}"
        )));
    }
    let payload: ModelList = serde_json::from_slice(&raw).map_err(|source| {
        fail(format!(
            "invalid metadata JSON after status {status}: {source}"
        ))
    })?;
    let mut matching = payload.data.into_iter().filter(|item| item.id == model);
    let served = matching
        .next()
        .ok_or_else(|| fail("configured model is absent from /v1/models".to_owned()))?;
    if matching.next().is_some() {
        return Err(fail(
            "configured model occurs more than once in /v1/models".to_owned(),
        ));
    }
    if served.root.is_empty() || served.max_model_len != expected {
        return Err(fail(format!(
            "expected max_tokens {expected}, advertised max_model_len {} and checkpoint {:?}; align configuration with the serving engine",
            served.max_model_len, served.root
        )));
    }
    Ok(served)
}

/// Redact the loaded bearer secret before escaping and bounding provider failure diagnostics.
pub(super) fn failure_excerpt(value: &str, api_key: Option<&str>, max_chars: usize) -> String {
    let redacted = match api_key {
        Some(key) => value.replace(key, "[REDACTED]"),
        None => value.to_owned(),
    };
    redacted
        .chars()
        .flat_map(char::escape_default)
        .take(max_chars)
        .collect()
}
