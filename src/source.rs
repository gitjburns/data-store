// Fabric substrate: `ResolvedSource` and the corpus path-safety resolution
// functions are consumed by the scheduler's parse dispatch (C5c), and the
// lexical URI-mapping/prescreen helpers additionally by the HTTP ingest
// route (ruled 2026-07-17).

use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use crate::{config::StorageConfig, error::ApiError};

/// A source reference proven to resolve to one file inside the corpus root
/// (see `resolve_contained_source`); `absolute_path` is canonicalized.
#[derive(Debug, Clone)]
pub struct ResolvedSource {
    pub absolute_path: PathBuf,
}

/// Resolve a corpus-relative source reference to one contained file of any
/// type. This is the single containment authority for parser input paths:
/// traversal/root components are rejected lexically, then BOTH the corpus
/// root and the candidate are canonicalized (resolving symlinks) before the
/// containment check, so a symlink escaping the corpus root cannot pass.
pub fn resolve_contained_source(
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

    Ok(ResolvedSource { absolute_path })
}

/// Derive the corpus-relative form of a native URI. The filesystem
/// connector builds native URIs as absolute UTF-8 paths under the corpus
/// root; the scheduler's parse dispatch and the HTTP ingest prescreen share
/// this one mapping rule. The strip is LEXICAL only — it provides no
/// containment guarantee (a symlink after the prefix passes): every
/// dispatch route must feed the result through the resolvers above, whose
/// canonicalized containment check is the single path-safety authority.
pub fn corpus_relative_source(corpus_root: &Path, native_uri: &str) -> Result<String, ApiError> {
    let relative = Path::new(native_uri)
        .strip_prefix(corpus_root)
        .map_err(|_| ApiError::SourceResolution {
            message: format!(
                "native URI {native_uri} is not under the corpus root {}",
                corpus_root.display()
            ),
        })?;
    relative
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| ApiError::SourceResolution {
            // Unreachable for a str-derived path; the error arm keeps the
            // panic-free Result policy instead of unwrapping.
            message: format!("corpus-relative form of {native_uri} is not valid UTF-8"),
        })
}

/// Lexical containment prescreen for an operator-supplied ingest URI at the
/// HTTP boundary (ruled 2026-07-17): the URI must be an absolute path
/// lexically under the corpus root, with no traversal components in its
/// corpus-relative remainder — otherwise no scan enumeration can ever stage
/// it and the request is rejected up front (400) instead of minting a
/// pending Operation whose failure would wait on the next scan cycle.
/// Advisory only — deliberately no filesystem I/O and no existence check
/// (the file may legitimately land before the next scan): the drain's
/// missing-bundle policy and the parse-dispatch containment resolvers above
/// remain the authoritative checks.
pub fn prescreen_operator_native_uri(corpus_root: &Path, native_uri: &str) -> Result<(), ApiError> {
    let relative = corpus_relative_source(corpus_root, native_uri)?;
    validate_relative_source(&relative)?;
    Ok(())
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
