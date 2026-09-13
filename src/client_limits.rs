//! Client request and presentation limits shared without importing server policy.

use serde::{Deserialize, Serialize};

/// Shared client request and presentation controls, excluded from server identity.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientLimits {
    /// Whole HTTP-request deadline, not a deadline for an operation's poll loop.
    pub operation_timeout_seconds: u64,
    pub operation_poll_interval_ms: u64,
    pub provenance_preview_items: usize,
}

impl ClientLimits {
    /// Reject zero intervals and deadlines before constructing a client or timer.
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            (
                "client.operation_timeout_seconds",
                self.operation_timeout_seconds as u128,
            ),
            (
                "client.operation_poll_interval_ms",
                self.operation_poll_interval_ms as u128,
            ),
            (
                "client.provenance_preview_items",
                self.provenance_preview_items as u128,
            ),
        ] {
            if value == 0 || value >= i64::MAX as u128 {
                return Err(format!("{name} must be between 1 and {}", i64::MAX - 1));
            }
        }
        Ok(())
    }
}
