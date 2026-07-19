//! Vector validators for dense vectors, ColBERT document token matrices, and
//! stored norms. The validated output types and the `ApiError::StorageOperation`
//! constructor live here with the validators that produce them.

// Vector-validation substrate consumed by the dense and multi-vector
// projection builders (projections::dense, projections::multivector) and by
// primitives::codec. `storage_operation_error` and `validate_norm` are
// internal helpers called by the validators in this module.

use crate::{error::ApiError, primitives::codec::UnitColbertDocumentVector};

/// A dense vector that passed validation, carrying the precomputed L2 norm
/// used for cosine similarity at query time.
#[derive(Debug, Clone)]
pub(crate) struct StoredDenseVector {
    // Carried as validated identity but not yet read by the dense projection
    // builder, which consumes only `vector` and `norm`. Recorded-but-unread.
    #[allow(dead_code)]
    pub(crate) unit_id: String,
    pub(crate) vector: Vec<f32>,
    pub(crate) norm: f32,
}

/// A ColBERT token matrix that passed validation: `token_count`, `dimension`,
/// and the row-major value count are confirmed consistent.
#[derive(Debug, Clone)]
pub(crate) struct StoredColbertDocumentVector {
    pub(crate) unit_id: String,
    pub(crate) token_count: usize,
    pub(crate) dimension: usize,
    pub(crate) vector: Vec<f32>,
}

/// Convert a storage operation failure into the service error shape.
pub(crate) fn storage_operation_error(message: String) -> ApiError {
    ApiError::StorageOperation { message }
}

/// Validate one dense vector and compute the stored norm from raw values.
pub(crate) fn validate_vector(
    unit_id: String,
    vector: Vec<f32>,
    dimension: usize,
) -> Result<StoredDenseVector, String> {
    if vector.len() != dimension {
        return Err(format!(
            "dense vector {unit_id} has dimension {}, expected {dimension}",
            vector.len()
        ));
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(format!("dense vector {unit_id} contains non-finite values"));
    }

    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    let norm = validate_norm(&unit_id, norm)?;
    Ok(StoredDenseVector {
        unit_id,
        vector,
        norm,
    })
}

/// Validate a ColBERT document token matrix and preserve its row-major token layout.
pub(crate) fn validate_colbert_document_vector(
    vector: UnitColbertDocumentVector,
    expected_dimension: usize,
) -> Result<StoredColbertDocumentVector, ApiError> {
    if vector.token_count == 0 {
        return Err(storage_operation_error(format!(
            "ColBERT document vector {} has zero tokens",
            vector.unit_id
        )));
    }
    if vector.dimension != expected_dimension {
        return Err(storage_operation_error(format!(
            "ColBERT document vector {} has dimension {}, expected {}",
            vector.unit_id, vector.dimension, expected_dimension
        )));
    }
    let expected_values = vector
        .token_count
        .checked_mul(vector.dimension)
        .ok_or_else(|| {
            storage_operation_error(format!(
                "ColBERT document vector {} token matrix size overflow",
                vector.unit_id
            ))
        })?;
    if vector.vector.len() != expected_values {
        return Err(storage_operation_error(format!(
            "ColBERT document vector {} has {} values, expected {}",
            vector.unit_id,
            vector.vector.len(),
            expected_values
        )));
    }
    if vector.vector.iter().any(|value| !value.is_finite()) {
        return Err(storage_operation_error(format!(
            "ColBERT document vector {} contains non-finite values",
            vector.unit_id
        )));
    }

    Ok(StoredColbertDocumentVector {
        unit_id: vector.unit_id,
        token_count: vector.token_count,
        dimension: vector.dimension,
        vector: vector.vector,
    })
}

/// Validate a stored norm and return it in f32 form.
pub(crate) fn validate_norm(unit_id: &str, norm: f32) -> Result<f32, String> {
    if !norm.is_finite() || norm <= 0.0 {
        return Err(format!(
            "dense vector {unit_id} has invalid norm {norm}; expected finite nonzero norm"
        ));
    }

    Ok(norm)
}
