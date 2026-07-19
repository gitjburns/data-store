//! Canonical serialization and content hashing (spec §16.1–16.3): NFC-
//! normalized, sorted-key canonical JSON, JSONL record-set hashing, and
//! SHA-256 digests over canonical bytes. Every content-derived hash in the
//! fabric routes through this module.
//!
//! Canonical JSON rules (spec §16.2):
//! - Encoding: UTF-8; every string (keys and values) is NFC-normalized
//!   before serialization.
//! - Objects: keys sorted lexicographically by the UTF-8 byte order of the
//!   NFC-normalized key; two distinct raw keys that normalize to the same
//!   key are rejected as ambiguous content.
//! - Arrays: order significant and preserved.
//! - Numbers: integers as-is; floats in serde_json's shortest round-trip
//!   (ryu) form; NaN/Infinity are errors.
//! - No whitespace; explicit `null` is preserved. Absence-vs-null is the
//!   producer's concern: fabric shapes use `skip_serializing_if` on every
//!   `Option` so absent fields never reach this module.
//!
//! Timestamp contract: this module does not format or validate timestamps.
//! Producers must supply timestamps already rendered as RFC3339 UTC with an
//! explicit `Z` at millisecond precision via
//! `crate::primitives::format_utc_timestamp_ms`, whose
//! `YYYY-MM-DDTHH:MM:SS.mmmZ` output satisfies that contract.

// Consumed from C2c/C2d onward; remove this allow when first wired.
#![allow(dead_code)]

use std::borrow::Cow;

use serde::Serialize;
use serde_json::Value;
use unicode_normalization::{UnicodeNormalization, is_nfc};

use crate::error::ApiError;

/// Serialize a JSON value to canonical UTF-8 bytes per spec §16.2; these
/// bytes are the only valid input for structured content hashes.
pub(crate) fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>, ApiError> {
    let mut out = Vec::new();
    write_canonical_value(value, &mut out)?;
    Ok(out)
}

/// Hash a JSON value: lowercase-hex SHA-256 over its canonical bytes. This
/// is the digest used for `bodyHash`, `policyHash`, `queryHash`, and every
/// other structured content hash in the fabric.
pub(crate) fn canonical_sha256_hex(value: &Value) -> Result<String, ApiError> {
    Ok(crate::primitives::sha256_hex(&canonical_json_bytes(value)?))
}

/// Hash raw artifact bytes (e.g. `sourceHash` over source bytes) as
/// lowercase-hex SHA-256; no canonicalization applies to opaque bytes.
pub(crate) fn sha256_hex_bytes(bytes: &[u8]) -> String {
    // Delegates to the existing primitive so the codebase keeps exactly one
    // SHA-256 hex implementation.
    crate::primitives::sha256_hex(bytes)
}

/// Serialize an ordered record set to canonical JSONL artifact bytes (spec
/// §16.3): each record's canonical JSON on its own line, in the given
/// order, joined by a single LF, with NO trailing LF. An empty record set
/// yields empty bytes. These are byte-for-byte the bytes hashed by
/// `jsonl_record_set_hash`, so a record-set hash can be re-verified
/// directly against the written artifact file.
pub(crate) fn canonical_jsonl_bytes(records: &[Value]) -> Result<Vec<u8>, ApiError> {
    let mut out = Vec::new();
    for (index, record) in records.iter().enumerate() {
        // LF joins lines; it never terminates the final line, so the byte
        // stream is unambiguous for any record count.
        if index > 0 {
            out.push(b'\n');
        }
        write_canonical_value(record, &mut out)?;
    }
    Ok(out)
}

/// Hash an ordered record set (spec §16.3): lowercase-hex SHA-256 over the
/// exact bytes produced by `canonical_jsonl_bytes` — canonical lines in the
/// given order joined by a single LF with no trailing LF; the empty record
/// set hashes the empty byte string. Record order is significant and must
/// match the ordering rule recorded in the artifact manifest.
pub(crate) fn jsonl_record_set_hash(records: &[Value]) -> Result<String, ApiError> {
    Ok(crate::primitives::sha256_hex(&canonical_jsonl_bytes(
        records,
    )?))
}

/// Serialize any `Serialize` shape to canonical bytes by routing through
/// `serde_json::Value`, so typed fabric structs and raw values hash
/// identically under one rule set.
pub(crate) fn canonical_json_bytes_of<T: Serialize>(value: &T) -> Result<Vec<u8>, ApiError> {
    let json = serde_json::to_value(value).map_err(|source| {
        canonical_error(format!("shape is not representable as JSON: {source}"))
    })?;
    canonical_json_bytes(&json)
}

/// Hash any `Serialize` shape: lowercase-hex SHA-256 over its canonical
/// bytes via `canonical_json_bytes_of`.
pub(crate) fn canonical_sha256_hex_of<T: Serialize>(value: &T) -> Result<String, ApiError> {
    Ok(crate::primitives::sha256_hex(&canonical_json_bytes_of(
        value,
    )?))
}

/// JSON field name every self-hashed capability profile stores its hash
/// under (spec §9.3, §12.4). Defined once beside the self-hash helper so
/// producers cannot drift onto differently named hash fields.
pub(crate) const PROFILE_HASH_JSON_KEY: &str = "profileHash";

/// Hash any `Serialize` shape with one named top-level field removed before
/// hashing — the self-hash pattern of declared profiles (spec §9.3, §12.4):
/// the stored hash covers every field except the hash field itself.
/// Serializing the real struct and removing the field keeps the struct the
/// single source of the hashed shape, and both failure paths are loud so a
/// serde rename can never silently change what the hash covers.
pub(crate) fn canonical_sha256_hex_without_field<T: Serialize>(
    value: &T,
    field: &str,
) -> Result<String, ApiError> {
    let mut json = serde_json::to_value(value).map_err(|source| {
        canonical_error(format!("shape is not representable as JSON: {source}"))
    })?;
    let Some(fields) = json.as_object_mut() else {
        return Err(canonical_error(
            "self-hashed shape did not serialize to a JSON object".to_string(),
        ));
    };
    if fields.remove(field).is_none() {
        return Err(canonical_error(format!(
            "self-hashed shape is missing the {field} field; \
             hashing would silently cover the wrong shape"
        )));
    }
    canonical_sha256_hex(&json)
}

/// Recursively write one JSON value in canonical form; the single walker
/// that enforces every §16.2 rule (NFC strings, sorted keys, canonical
/// numbers, no whitespace, preserved array order and explicit null).
fn write_canonical_value(value: &Value, out: &mut Vec<u8>) -> Result<(), ApiError> {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(true) => out.extend_from_slice(b"true"),
        Value::Bool(false) => out.extend_from_slice(b"false"),
        Value::Number(number) => write_canonical_number(number, out)?,
        Value::String(raw) => write_json_escaped(&nfc_normalized(raw), out)?,
        Value::Array(items) => {
            // Array order is significant and preserved exactly as given.
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical_value(item, out)?;
            }
            out.push(b']');
        }
        Value::Object(map) => {
            // Keys are NFC-normalized BEFORE sorting so the sort order is
            // defined over canonical bytes, not raw producer bytes.
            let mut entries: Vec<(Cow<'_, str>, &Value)> = map
                .iter()
                .map(|(key, entry)| (nfc_normalized(key), entry))
                .collect();
            entries.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
            // Distinct raw keys can collide after NFC normalization; that
            // content has no single canonical form, so it must be rejected
            // rather than silently dropping one entry.
            for pair in entries.windows(2) {
                if pair[0].0 == pair[1].0 {
                    return Err(canonical_error(format!(
                        "object keys collide after NFC normalization: {:?}",
                        pair[0].0
                    )));
                }
            }
            out.push(b'{');
            for (index, (key, entry)) in entries.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_json_escaped(key, out)?;
                out.push(b':');
                write_canonical_value(entry, out)?;
            }
            out.push(b'}');
        }
    }
    Ok(())
}

/// Write one number in canonical form: integers verbatim, finite floats in
/// serde_json's shortest round-trip (ryu) rendering, NaN/Infinity rejected.
fn write_canonical_number(number: &serde_json::Number, out: &mut Vec<u8>) -> Result<(), ApiError> {
    // Integer path first: i64/u64 render digit-for-digit and must not go
    // through f64, which is lossy above 2^53.
    if number.is_i64() || number.is_u64() {
        out.extend_from_slice(number.to_string().as_bytes());
        return Ok(());
    }
    // serde_json numbers cannot hold NaN/Infinity today, but this guard is
    // deliberate defense at the hashing boundary: a non-finite value must
    // never be silently rendered into hash input.
    match number.as_f64() {
        Some(float) if float.is_finite() => {
            // Number's Display is serde_json's ryu path: the shortest string
            // that round-trips to the same f64.
            out.extend_from_slice(number.to_string().as_bytes());
            Ok(())
        }
        _ => Err(canonical_error(format!(
            "non-finite number cannot be canonically serialized: {number}"
        ))),
    }
}

/// Write an already-NFC-normalized string as a JSON string literal using
/// serde_json's deterministic escaping (quote, backslash, and control
/// characters only), keeping escape policy identical to ordinary
/// serde_json output.
fn write_json_escaped(normalized: &str, out: &mut Vec<u8>) -> Result<(), ApiError> {
    // Serializing a &str to an in-memory string cannot fail for valid UTF-8;
    // the error arm exists to keep the panic-free Result policy.
    let escaped = serde_json::to_string(normalized)
        .map_err(|source| canonical_error(format!("string escaping failed: {source}")))?;
    out.extend_from_slice(escaped.as_bytes());
    Ok(())
}

/// NFC-normalize a string, borrowing when it is already normalized so the
/// common ASCII/normalized case allocates nothing.
fn nfc_normalized(raw: &str) -> Cow<'_, str> {
    if is_nfc(raw) {
        Cow::Borrowed(raw)
    } else {
        Cow::Owned(raw.nfc().collect())
    }
}

/// Wrap a canonicalization failure as an `ApiError`. `InternalIo` is the
/// existing catch-all internal-failure variant (already used for non-IO
/// failures such as clock errors); canonicalization failures are internal
/// invariant violations, never client-attributable 4xx conditions.
fn canonical_error(message: String) -> ApiError {
    ApiError::InternalIo {
        message: format!("canonical serialization: {message}"),
    }
}
