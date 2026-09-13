//! CA2 policy substrate: operator-editable external policy documents and their
//! system-assigned version log.
//!
//! Two policy documents are authored by operators as TOML files referenced by
//! path from the `[policies]` config section: the entity-match policy (query →
//! stored-name matching classes) and the annotator-naming policy (ordered
//! naming-rule lines composed into producer prompts by CA2-P3). Both are loaded
//! ONCE at startup and STRICTLY validated: a missing file, unparseable TOML, an
//! unknown key, or an out-of-range value is a fatal startup error, never a
//! silently-defaulted value. This mirrors the config principle in
//! `crate::config` (every field required, `deny_unknown_fields`), because a
//! policy that silently loses an operator's intent is worse than a hard failure.
//!
//! Each loaded policy carries a SHA-256 over its effective canonical document.
//! Entity matching composes file enable flags with config numeric limits in the
//! historical shape before hashing. Whitespace and comment edits preserve identity.
//!
//! Versions are SYSTEM-ASSIGNED change-event counters tracked in the append-only
//! `policy_versions` hot-plane table (see `sql/fabric/schema.sql`). Registration
//! reads the latest row for a policy id and, when the content hash differs (or
//! no row exists), appends the next version and a `policy.changed` SystemEvent
//! atomically on the caller's connection. A revert to previously seen content
//! still appends a new version: the counter records change events, not distinct
//! contents.
//!
//! Error classification follows the config surface this module parallels:
//! read/parse failures reuse `ApiError::ConfigRead`/`ConfigParse` and semantic
//! validation reuses `ApiError::InvalidConfig`, since a policy document is
//! operator-authored startup configuration. Version-log reads and writes reuse
//! `ApiError::StorageOperation`, matching every other hot-plane writer.

use std::path::{Path, PathBuf};

use crate::sqlite::Connection;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::error::ApiError;
use crate::events::{append_event, entry, new_system_event};
use crate::model::SystemEventType;
use crate::primitives::utc_now;

/// Stable policy id recorded in `policy_versions.policy_id` and in
/// `policy.changed` events for the entity-match policy. System-owned, never
/// operator-authored.
pub(crate) const POLICY_ID_ENTITY_MATCH: &str = "entity_match";

/// Stable policy id recorded in `policy_versions.policy_id` and in
/// `policy.changed` events for the annotator-naming policy. System-owned, never
/// operator-authored.
pub(crate) const POLICY_ID_ANNOTATOR_NAMING: &str = "annotator_naming";

/// Object type recorded on `policy.changed` SystemEvents, matching the
/// `OBJECT_TYPE_*` convention used by the other event emitters (e.g.
/// `semantic_annotation` in `annotations::store`). The event's `object_id` is
/// the policy id, so an audit reader can trace a policy's whole version history.
const OBJECT_TYPE_POLICY: &str = "policy";

/// Effective entity-match policy: file enable flags plus configured numeric
/// limits. Its serialized shape stays stable so relocating unchanged settings
/// does not manufacture a policy version or invalidate historical hashes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EntityMatchPolicy {
    /// Acronym match class: a single query token compared against the
    /// first-letter acronym of a multi-token stored name.
    pub(crate) acronym: AcronymClass,

    /// Token-prefix match class: each query token a prefix of the corresponding
    /// stored-name token, in order.
    pub(crate) token_prefix: TokenPrefixClass,

    /// Cap on the number of fuzzy-matched stored names considered per query.
    /// Must be > 0: a zero cap would disable fuzzy matching silently, which the
    /// operator should express by disabling the classes, not by a zero here.
    pub(crate) max_fuzzy_candidates: u32,
}

/// On-disk entity policy contains behavior switches; numeric work lives in config.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EntityMatchDocument {
    acronym: MatchClassSwitch,
    token_prefix: MatchClassSwitch,
}

/// A required enable switch prevents missing values from silently disabling a class.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MatchClassSwitch {
    enabled: bool,
}

/// The `[acronym]` match class of the entity-match policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcronymClass {
    /// Whether this match class participates at query time.
    pub(crate) enabled: bool,

    /// Minimum stored-name token count from which to derive an acronym. Must be
    /// >= 2: a single-token name has no multi-token acronym to match against.
    pub(crate) min_name_tokens: u32,
}

/// The `[token_prefix]` match class of the entity-match policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TokenPrefixClass {
    /// Whether this match class participates at query time.
    pub(crate) enabled: bool,

    /// Minimum query-token length for that token to participate as a prefix.
    /// Must be >= 1: a zero-length token participates vacuously against every
    /// stored token, which is never the operator's intent.
    pub(crate) min_token_len: u32,
}

/// The annotator-naming policy document (CA2): ordered naming-rule lines that
/// CA2-P3 composes into producer prompts. An EMPTY `rules` vector is the valid
/// neutral document (no additional naming guidance) and MUST be representable;
/// it is not an error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AnnotatorNamingPolicy {
    /// Ordered naming-rule lines. Order is significant (it is preserved into the
    /// composed prompt) and an empty vector is the neutral document.
    pub(crate) rules: Vec<String>,
}

/// An effective policy paired with its canonical content hash; file formatting
/// cannot change identity, and entity matching includes configured numeric limits.
#[derive(Debug, Clone)]
pub(crate) struct LoadedPolicy<T> {
    /// The strictly-validated policy document.
    pub(crate) document: T,

    /// Lowercase-hex SHA-256 over the parsed document's canonical serialization.
    pub(crate) content_hash: String,
}

/// The outcome of registering a policy content hash against the version log:
/// the resolved version and whether this registration advanced it (a real
/// change) or matched the latest recorded content (no write performed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RegisteredVersion {
    /// The version now in effect for this policy id.
    pub(crate) version: i64,

    /// True when this call inserted a new version row (and appended a
    /// `policy.changed` event); false when the content matched the latest
    /// recorded version and nothing was written.
    pub(crate) advanced: bool,
}

/// Compose file switches and validated config limits into the historical
/// effective-policy shape before hashing and version registration.
pub(crate) fn load_entity_match_policy(
    path: &Path,
    limits: &crate::limits::FuzzyLimits,
) -> Result<LoadedPolicy<EntityMatchPolicy>, ApiError> {
    limits
        .validate()
        .map_err(|message| ApiError::InvalidConfig { message })?;
    let switches: EntityMatchDocument = parse_policy_file(path)?;
    let document = EntityMatchPolicy {
        acronym: AcronymClass {
            enabled: switches.acronym.enabled,
            min_name_tokens: limits.acronym_min_name_tokens,
        },
        token_prefix: TokenPrefixClass {
            enabled: switches.token_prefix.enabled,
            min_token_len: limits.prefix_min_token_chars,
        },
        max_fuzzy_candidates: limits.max_fuzzy_candidates,
    };
    let content_hash = crate::canonical::canonical_sha256_hex_of(&document)?;

    // Log the load boundary with bounded metadata only: never rule text or any
    // operator content (DIAGNOSTICS-ONBOARDING); field counts and flags are
    // safe and let an operator confirm what took effect.
    info!(
        event = "policy.loaded",
        policy_id = POLICY_ID_ENTITY_MATCH,
        path = %path.display(),
        content_hash = %content_hash,
        acronym_enabled = document.acronym.enabled,
        token_prefix_enabled = document.token_prefix.enabled,
        max_fuzzy_candidates = document.max_fuzzy_candidates,
        "entity-match policy loaded"
    );

    Ok(LoadedPolicy {
        document,
        content_hash,
    })
}

/// Load and strictly validate the annotator-naming policy from `path`. A missing
/// file, unparseable TOML, or an unknown key is a fatal error; an empty rule
/// list is valid (the neutral document). Returns the parsed document plus its
/// parsed-shape content hash.
pub(crate) fn load_annotator_naming_policy(
    path: &Path,
) -> Result<LoadedPolicy<AnnotatorNamingPolicy>, ApiError> {
    let document: AnnotatorNamingPolicy = parse_policy_file(path)?;
    // No semantic validation beyond deserialization: any ordered list of rule
    // lines, including the empty list, is a valid naming policy.
    let content_hash = crate::canonical::canonical_sha256_hex_of(&document)?;

    // Bounded metadata only: the rule COUNT, never the rule text.
    info!(
        event = "policy.loaded",
        policy_id = POLICY_ID_ANNOTATOR_NAMING,
        path = %path.display(),
        content_hash = %content_hash,
        rule_count = document.rules.len(),
        "annotator-naming policy loaded"
    );

    Ok(LoadedPolicy {
        document,
        content_hash,
    })
}

/// Register `content_hash` for `policy_id` against the append-only
/// `policy_versions` table on the CALLER's connection. Reads the latest row for
/// the id: absent → insert version 1; hash differs → insert latest + 1; hash
/// matches → no write. On every INSERT a `policy.changed` SystemEvent is
/// appended on the same connection, so (when the caller holds a transaction) the
/// version row and its audit event commit or roll back together — the append-log
/// invariant of `crate::events`. This function never opens its own connection or
/// transaction; the main-loop wiring calls it only when the fabric plane is
/// valid.
pub(crate) fn register_policy_version(
    connection: &Connection,
    policy_id: &str,
    content_hash: &str,
) -> Result<RegisteredVersion, ApiError> {
    let latest = read_latest_policy_version(connection, policy_id)?;

    if let Some((latest_version, latest_hash)) = &latest
        && latest_hash == content_hash
    {
        // Unchanged: the latest recorded content already matches, so the
        // append-only log gets no new row and no event. Report the standing
        // version without advancing it.
        tracing::debug!(
            event = "policy.version_unchanged",
            policy_id = %policy_id,
            version = *latest_version,
            content_hash = %content_hash,
            "policy content matches latest recorded version; no change"
        );
        return Ok(RegisteredVersion {
            version: *latest_version,
            advanced: false,
        });
    }

    // Absent → 1; changed → latest + 1. The counter records change events, so a
    // revert to previously seen content still lands here and advances.
    let next_version = latest.map(|(version, _)| version + 1).unwrap_or(1);
    let observed_at = utc_now()?;

    connection
        .execute(
            INSERT_POLICY_VERSION_SQL,
            params![policy_id, next_version, content_hash, observed_at],
        )
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to insert policy version {next_version} for {policy_id}: {source}"
            ),
        })?;

    // Append the audit event on the caller's connection so it is atomic with the
    // version row it records (crate::events invariant). The payload carries only
    // safe identifiers: policy id, resolved version, and content hash.
    let payload = serde_json::Map::from_iter([
        entry("policyId", policy_id),
        entry("version", &next_version.to_string()),
        entry("contentHash", content_hash),
    ]);
    let event = new_system_event(
        SystemEventType::PolicyChanged,
        OBJECT_TYPE_POLICY,
        policy_id,
        Some(payload),
    )?;
    append_event(connection, &event)?;

    info!(
        event = "policy.version_advanced",
        committed = connection.is_autocommit(),
        policy_id = %policy_id,
        version = next_version,
        content_hash = %content_hash,
        "policy version and audit event written; committed field reflects transaction ownership"
    );

    Ok(RegisteredVersion {
        version: next_version,
        advanced: true,
    })
}

/// Insert one append-only `policy_versions` row. The table is never UPDATEd or
/// DELETEd (schema convention), so INSERT is the only statement this module
/// issues against it.
const INSERT_POLICY_VERSION_SQL: &str = "
INSERT INTO policy_versions (
  policy_id, version, content_hash, observed_at
) VALUES (?1, ?2, ?3, ?4)";

/// Read the highest-versioned row for `policy_id`, returning its
/// `(version, content_hash)` or `None` when the policy has never been recorded.
/// ORDER BY version DESC LIMIT 1 selects the latest change event.
fn read_latest_policy_version(
    connection: &Connection,
    policy_id: &str,
) -> Result<Option<(i64, String)>, ApiError> {
    connection
        .query_row(
            SELECT_LATEST_POLICY_VERSION_SQL,
            params![policy_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read latest policy version for {policy_id}: {source}"),
        })
}

/// Select the latest version and its content hash for one policy id.
const SELECT_LATEST_POLICY_VERSION_SQL: &str = "
SELECT version, content_hash
FROM policy_versions
WHERE policy_id = ?1
ORDER BY version DESC
LIMIT 1";

/// Read and strictly deserialize a policy TOML file into `T`. Read failures map
/// to `ConfigRead` and parse/unknown-key failures to `ConfigParse`, matching
/// `ServiceConfig::load` — a policy document is operator-authored startup
/// configuration, so it shares the config error vocabulary.
fn parse_policy_file<T>(path: &Path) -> Result<T, ApiError>
where
    T: for<'de> Deserialize<'de>,
{
    let raw = std::fs::read_to_string(path).map_err(|source| ApiError::ConfigRead {
        path: PathBuf::from(path),
        source,
    })?;
    toml::from_str(&raw).map_err(|source| ApiError::ConfigParse {
        path: PathBuf::from(path),
        source,
    })
}
