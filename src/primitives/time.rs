//! Epoch-millisecond timestamps and dependency-free UTC formatting and
//! parsing for durable storage metadata and cache diagnostics. The fabric
//! timestamp format (`YYYY-MM-DDTHH:MM:SS.mmmZ`) round-trips exactly through
//! `format_utc_timestamp_ms`/`parse_utc_timestamp_ms`.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::ApiError;

/// Format epoch milliseconds as an ISO-like UTC timestamp without adding a time dependency.
pub(crate) fn format_utc_timestamp_ms(epoch_ms: u64) -> Result<String, ApiError> {
    let total_seconds = epoch_ms / 1_000;
    let millis = epoch_ms % 1_000;
    let days = (total_seconds / 86_400) as i64;
    let seconds_of_day = total_seconds % 86_400;
    let (year, month, day) = civil_from_epoch_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;

    Ok(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z"
    ))
}

/// Convert days since the Unix epoch to the Gregorian UTC date components.
fn civil_from_epoch_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let shifted_days = days_since_epoch + 719_468;
    let era = if shifted_days >= 0 {
        shifted_days
    } else {
        shifted_days - 146_096
    } / 146_097;
    let day_of_era = shifted_days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year_of_era + era * 400 + if month <= 2 { 1 } else { 0 };

    (year, month as u32, day as u32)
}

/// Return current epoch milliseconds for durable timestamps and cache diagnostics.
pub(crate) fn current_time_ms() -> Result<u64, ApiError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as u64)
        .map_err(|source| ApiError::InternalIo {
            message: format!("system clock is before UNIX epoch: {source}"),
        })
}

/// Current time as the fabric RFC3339 UTC millisecond timestamp string used
/// by every persisted timestamp column.
pub(crate) fn utc_now() -> Result<String, ApiError> {
    format_utc_timestamp_ms(current_time_ms()?)
}

/// Parse one fabric timestamp (`YYYY-MM-DDTHH:MM:SS.mmmZ`, the exact output
/// of `format_utc_timestamp_ms`) back to unix milliseconds — the formatter's
/// inverse. Strictness is deliberate: a timestamp is only trusted when it
/// round-trips bit-for-bit through the fabric formatter, which also rejects
/// impossible dates; anything else returns None and the caller treats the
/// value as unknown.
pub(crate) fn parse_utc_timestamp_ms(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    if bytes.len() != 24
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'.'
        || bytes[23] != b'Z'
    {
        return None;
    }
    let year = parse_decimal(&text[0..4])?;
    let month = parse_decimal(&text[5..7])?;
    let day = parse_decimal(&text[8..10])?;
    let hour = parse_decimal(&text[11..13])?;
    let minute = parse_decimal(&text[14..16])?;
    let second = parse_decimal(&text[17..19])?;
    let millis = parse_decimal(&text[20..23])?;

    let days = days_from_civil(year as i64, month, day);
    // Pre-epoch timestamps cannot round-trip through the u64-based fabric
    // formatter, so they are out of contract here.
    if days < 0 {
        return None;
    }
    let epoch_ms =
        (days * 86_400 + (hour * 3_600 + minute * 60 + second) as i64) * 1_000 + millis as i64;

    // Round-trip gate: reformatting must reproduce the input exactly, which
    // rejects out-of-range fields and impossible dates (e.g. Feb 30) without
    // duplicating calendar validation logic.
    match format_utc_timestamp_ms(epoch_ms as u64) {
        Ok(formatted) if formatted == text => Some(epoch_ms),
        _ => None,
    }
}

/// Parse a fixed-width all-digit decimal field of a fabric timestamp.
fn parse_decimal(field: &str) -> Option<u64> {
    if !field.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    field.parse().ok()
}

/// Convert a Gregorian UTC date to days since the Unix epoch (Howard
/// Hinnant's civil-days algorithm; the exact inverse of the formatter's
/// `civil_from_epoch_days`).
fn days_from_civil(year: i64, month: u64, day: u64) -> i64 {
    let adjusted_year = year - if month <= 2 { 1 } else { 0 };
    let era = if adjusted_year >= 0 {
        adjusted_year
    } else {
        adjusted_year - 399
    } / 400;
    let year_of_era = adjusted_year - era * 400;
    let month_prime = if month > 2 { month - 3 } else { month + 9 } as i64;
    let day_of_year = (153 * month_prime + 2) / 5 + day as i64 - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}
