use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use crate::{config::StorageConfig, error::ApiError};

#[derive(Debug, Clone)]
pub struct ResolvedSource {
    pub requested: String,
    pub relative_path: PathBuf,
    pub absolute_path: PathBuf,
}

/// Resolve a corpus-relative source reference to one contained PDF file.
pub fn resolve_source_reference(
    storage: &StorageConfig,
    source: &str,
) -> Result<ResolvedSource, ApiError> {
    let trimmed = source.trim();
    let relative_path = validate_relative_source(trimmed)?;
    let corpus_root = canonicalize_existing_directory("storage.corpus_root", &storage.corpus_root)?;
    let candidate_path = corpus_root.join(&relative_path);
    let absolute_path = canonicalize_existing_file("source", &candidate_path)?;

    if !absolute_path.starts_with(&corpus_root) {
        return Err(ApiError::SourceResolution {
            message: "source must stay within the configured corpus root".to_string(),
        });
    }
    if absolute_path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_none_or(|extension| !extension.eq_ignore_ascii_case("pdf"))
    {
        return Err(ApiError::SourceResolution {
            message: "source must reference a PDF file for Docling conversion".to_string(),
        });
    }

    Ok(ResolvedSource {
        requested: trimmed.to_string(),
        relative_path,
        absolute_path,
    })
}

/// Validate that a request source is relative and contains no traversal components.
fn validate_relative_source(source: &str) -> Result<PathBuf, ApiError> {
    if source.is_empty() {
        return Err(ApiError::SourceResolution {
            message: "source must be a non-empty corpus-relative reference".to_string(),
        });
    }

    let path = PathBuf::from(source);
    if path.is_absolute() {
        return Err(ApiError::SourceResolution {
            message: "source must be relative to the configured corpus root".to_string(),
        });
    }
    if path.components().any(is_forbidden_component) {
        return Err(ApiError::SourceResolution {
            message: "source must not contain parent traversal or root components".to_string(),
        });
    }

    Ok(path)
}

/// Return whether a path component can escape or ambiguously address the corpus root.
fn is_forbidden_component(component: Component<'_>) -> bool {
    matches!(
        component,
        Component::ParentDir | Component::RootDir | Component::Prefix(_)
    )
}

/// Canonicalize an existing directory with a field-specific diagnostic.
fn canonicalize_existing_directory(label: &str, path: &Path) -> Result<PathBuf, ApiError> {
    let canonical = fs::canonicalize(path).map_err(|source| ApiError::SourceResolution {
        message: format!("{label} is not readable at {}: {source}", path.display()),
    })?;
    if !canonical.is_dir() {
        return Err(ApiError::SourceResolution {
            message: format!("{label} is not a directory: {}", canonical.display()),
        });
    }

    Ok(canonical)
}

/// Canonicalize an existing file with a field-specific diagnostic.
fn canonicalize_existing_file(label: &str, path: &Path) -> Result<PathBuf, ApiError> {
    let canonical = fs::canonicalize(path).map_err(|source| ApiError::SourceResolution {
        message: format!("{label} is not readable at {}: {source}", path.display()),
    })?;
    if !canonical.is_file() {
        return Err(ApiError::SourceResolution {
            message: format!("{label} is not a file: {}", canonical.display()),
        });
    }

    Ok(canonical)
}
