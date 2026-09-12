//! Immutable embedding blobs use bounded reads; the operating system owns caching.

use std::io::Read;

use serde::{Deserialize, Serialize};

use crate::{
    artifact_store::{ArtifactRef, ArtifactStore},
    error::ApiError,
};

// These are corruption/allocation ceilings, not retrieval truncation policies.
const MAX_VALUES: usize = 1_048_576;
const MAX_ROWS: usize = 518;
pub(crate) const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;

/// Shape and norm are verified against the immutable little-endian f32 payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct EmbeddingRef {
    pub(crate) artifact: ArtifactRef,
    pub(crate) rows: usize,
    pub(crate) dimension: usize,
    pub(crate) norm: f32,
}

/// Archive one bounded dense vector or ColBERT matrix after validating every row.
pub(crate) fn store_embedding(
    store: &ArtifactStore,
    values: &[f32],
    rows: usize,
    dimension: usize,
) -> Result<EmbeddingRef, ApiError> {
    let count = value_count(rows, dimension)?;
    if values.len() != count {
        return Err(failure("embedding value count disagrees with its shape"));
    }
    let norm = checked_norm(values, dimension)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(count * 4)
        .map_err(|source| failure(format!("allocate embedding write buffer: {source}")))?;
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    Ok(EmbeddingRef {
        artifact: store.put_bytes(&bytes)?,
        rows,
        dimension,
        norm,
    })
}

/// Decode only the requested matrix, never the corpus's collection of matrices.
pub(crate) fn load_embedding(
    store: &ArtifactStore,
    reference: &EmbeddingRef,
) -> Result<Vec<f32>, ApiError> {
    validate_ref(reference)?;
    let count = value_count(reference.rows, reference.dimension)?;
    store.with_verified_reader(
        &reference.artifact.uri,
        Some(reference.artifact.size_bytes),
        |reader| {
            let mut values = Vec::new();
            values
                .try_reserve_exact(count)
                .map_err(|source| failure(format!("allocate embedding read buffer: {source}")))?;
            visit_values(reader, count, |value| {
                values.push(value);
                Ok(())
            })?;
            let norm = checked_norm(&values, reference.dimension)?;
            if norm.to_bits() != reference.norm.to_bits() {
                return Err(failure(format!(
                    "embedding norm differs from manifest for {}",
                    reference.artifact.hash
                )));
            }
            Ok(values)
        },
    )
}

/// Score a dense vector while streaming bytes; retain no decoded index-sized array.
pub(crate) fn cosine(
    store: &ArtifactStore,
    reference: &EmbeddingRef,
    query: &[f32],
) -> Result<f32, ApiError> {
    validate_ref(reference)?;
    if reference.rows != 1 || reference.dimension != query.len() {
        return Err(failure("dense query and stored embedding shapes differ"));
    }
    let query_norm = checked_norm(query, query.len())?;
    store.with_verified_reader(
        &reference.artifact.uri,
        Some(reference.artifact.size_bytes),
        |reader| {
            let mut index = 0;
            let mut dot = 0.0_f32;
            let mut norm_squared = 0.0_f32;
            visit_values(reader, query.len(), |value| {
                dot += value * query[index];
                norm_squared += value * value;
                index += 1;
                Ok(())
            })?;
            let norm = norm_squared.sqrt();
            if norm.to_bits() != reference.norm.to_bits() || !norm.is_finite() || norm <= 0.0 {
                return Err(failure(format!(
                    "dense embedding norm differs for {}",
                    reference.artifact.hash
                )));
            }
            let score = dot / (norm * query_norm);
            if !score.is_finite() {
                return Err(failure("dense annotation cosine is non-finite"));
            }
            Ok(score)
        },
    )
}

/// Bound shape arithmetic before either allocation or byte-count multiplication.
pub(crate) fn validate_ref(reference: &EmbeddingRef) -> Result<(), ApiError> {
    let count = value_count(reference.rows, reference.dimension)?;
    if reference.artifact.size_bytes != (count * 4) as u64
        || !reference.norm.is_finite()
        || reference.norm <= 0.0
        || reference.artifact.hash.len() != 64
        || !reference
            .artifact
            .hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || !reference.artifact.uri.ends_with(&reference.artifact.hash)
    {
        return Err(failure(
            "embedding reference has invalid shape, norm, size, or hash identity",
        ));
    }
    Ok(())
}

/// Reject malformed dimensions before they become a working-memory requirement.
fn value_count(rows: usize, dimension: usize) -> Result<usize, ApiError> {
    rows.checked_mul(dimension)
        .filter(|count| rows > 0 && rows <= MAX_ROWS && dimension > 0 && *count <= MAX_VALUES)
        .ok_or_else(|| failure(format!("invalid embedding shape {rows} x {dimension}")))
}

/// Validate nonzero finite rows and retain the deterministic flattened L2 norm.
fn checked_norm(values: &[f32], dimension: usize) -> Result<f32, ApiError> {
    if dimension == 0 || values.is_empty() || !values.len().is_multiple_of(dimension) {
        return Err(failure("embedding has no complete rows"));
    }
    for row in values.chunks_exact(dimension) {
        let squared: f32 = row.iter().map(|value| value * value).sum();
        if row.iter().any(|value| !value.is_finite()) || !squared.is_finite() || squared <= 0.0 {
            return Err(failure("embedding contains a non-finite or zero row"));
        }
    }
    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    if !norm.is_finite() || norm <= 0.0 {
        return Err(failure("embedding norm is invalid"));
    }
    Ok(norm)
}

/// Read fixed-size blocks and reject trailing bytes instead of silently accepting another format.
fn visit_values(
    reader: &mut dyn Read,
    count: usize,
    mut visit: impl FnMut(f32) -> Result<(), ApiError>,
) -> Result<(), ApiError> {
    let mut buffer = [0_u8; 16 * 1024];
    let mut remaining = count;
    while remaining > 0 {
        let values = remaining.min(buffer.len() / 4);
        reader
            .read_exact(&mut buffer[..values * 4])
            .map_err(|source| failure(format!("read embedding values: {source}")))?;
        for bytes in buffer[..values * 4].chunks_exact(4) {
            let value = f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            if !value.is_finite() {
                return Err(failure("embedding contains non-finite values"));
            }
            visit(value)?;
        }
        remaining -= values;
    }
    if reader
        .read(&mut buffer[..1])
        .map_err(|source| failure(format!("read embedding end: {source}")))?
        != 0
    {
        return Err(failure("embedding contains trailing bytes"));
    }
    Ok(())
}

/// Keep storage boundary failures specific through the existing API error envelope.
fn failure(message: impl Into<String>) -> ApiError {
    ApiError::StorageOperation {
        message: message.into(),
    }
}
