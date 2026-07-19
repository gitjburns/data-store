//! Little-endian f32 blob codecs for the SQLite vector storage contract:
//! dense vectors and row-major ColBERT document token matrices.

// Retained vector-persistence substrate; its sole legacy consumer (storage.rs)
// is deleted at cluster CR. Consumed by the C6c dense and C6e multi-vector
// projection builders; remove when wired.
#![allow(dead_code)]

use crate::primitives::validate::{validate_colbert_document_vector, validate_vector};

/// Row-major ColBERT token-vector matrix for one retrieval unit, before
/// validation: `vector` holds `token_count * dimension` f32 values.
#[derive(Debug, Clone)]
pub struct UnitColbertDocumentVector {
    pub unit_id: String,
    pub token_count: usize,
    pub dimension: usize,
    pub vector: Vec<f32>,
}

const F32_BYTE_WIDTH: usize = std::mem::size_of::<f32>();

/// Encode one vector as contiguous little-endian f32 bytes.
pub(crate) fn encode_vector_blob(vector: &[f32]) -> Vec<u8> {
    encode_f32_blob(vector)
}

/// Encode a validated row-major ColBERT token matrix as the `matrix_blob`
/// persisted to `unit_multivector_projections` (C6e). `vector` holds
/// `token_count * dimension` contiguous f32 values in row (token) major order;
/// the byte layout is the SAME little-endian f32 contract as
/// `encode_vector_blob`, so this reuses `encode_f32_blob` — the matrix is one
/// flat run of tokens, not a separate blob primitive. The (token_count,
/// dimension) shape is NOT stored in the blob: it lives in its own columns and
/// is re-supplied to `decode_colbert_document_vector_blob` on read, which
/// re-derives the expected byte length (token_count * dimension * 4) and
/// rejects any mismatch. Callers pass values already validated by
/// `validate::validate_colbert_document_vector`, so this function only serializes.
pub(crate) fn encode_colbert_matrix_blob(vector: &[f32]) -> Vec<u8> {
    encode_f32_blob(vector)
}

/// Encode contiguous f32 values using the SQLite little-endian blob contract.
fn encode_f32_blob(vector: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(vector.len() * F32_BYTE_WIDTH);
    for value in vector {
        bytes.extend(value.to_le_bytes());
    }
    bytes
}

/// Decode and validate a little-endian f32 vector blob from SQLite.
pub(crate) fn decode_vector_blob(
    unit_id: &str,
    blob: &[u8],
    row_dimension: usize,
    expected_dimension: usize,
) -> Result<Vec<f32>, String> {
    if row_dimension != expected_dimension {
        return Err(format!(
            "dense vector {unit_id} has dimension {row_dimension}, expected {expected_dimension}"
        ));
    }
    let vector = decode_f32_blob(blob, expected_dimension, &format!("dense vector {unit_id}"))?;
    validate_vector(unit_id.to_string(), vector, expected_dimension).map(|value| value.vector)
}

/// Decode and validate a persisted ColBERT token-vector matrix blob from SQLite.
pub(crate) fn decode_colbert_document_vector_blob(
    unit_id: &str,
    blob: &[u8],
    token_count: usize,
    row_dimension: usize,
    expected_dimension: usize,
) -> Result<Vec<f32>, String> {
    if row_dimension != expected_dimension {
        return Err(format!(
            "ColBERT document vector {unit_id} has dimension {row_dimension}, expected {expected_dimension}"
        ));
    }
    let expected_values = token_count
        .checked_mul(expected_dimension)
        .ok_or_else(|| format!("ColBERT document vector {unit_id} token matrix size overflow"))?;
    let vector = decode_f32_blob(
        blob,
        expected_values,
        &format!("ColBERT document vector {unit_id}"),
    )?;
    validate_colbert_document_vector(
        UnitColbertDocumentVector {
            unit_id: unit_id.to_string(),
            token_count,
            dimension: expected_dimension,
            vector,
        },
        expected_dimension,
    )
    .map(|value| value.vector)
    .map_err(|source| source.to_string())
}

/// Decode contiguous little-endian f32 values before domain-specific vector validation.
fn decode_f32_blob(blob: &[u8], expected_values: usize, label: &str) -> Result<Vec<f32>, String> {
    let expected_bytes = expected_values
        .checked_mul(F32_BYTE_WIDTH)
        .ok_or_else(|| format!("{label} byte length overflow"))?;
    if blob.len() != expected_bytes {
        return Err(format!(
            "{label} has byte length {}, expected {expected_bytes}",
            blob.len()
        ));
    }

    let mut vector = Vec::with_capacity(expected_values);
    for bytes in blob.chunks_exact(F32_BYTE_WIDTH) {
        vector.push(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
    }
    Ok(vector)
}
