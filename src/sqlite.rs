//! Synchronous SQLite ownership with cooperative, execution-scoped deadlines.
//! Native connections never escape this module: every SQL entry point arms a
//! budget, and row iterators retain it until exhaustion or drop. Transactions
//! retain their connection/settings but do not themselves run a timeout clock.
//! Iterator budgets are wall-clock budgets: caller row processing and nested
//! reads count until the iterator ends, even when SQLite's VM is temporarily idle.

use std::{
    ops::Deref,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use rusqlite::{DropBehavior, OpenFlags, Params, Row, TransactionBehavior};
use tracing::{debug, error};

use crate::{
    canonical::sha256_hex_bytes,
    limits::{RuntimeLimits, SqliteLimits},
    runtime::RuntimeSettings,
};

/// Per-connection callbacks and RAII scopes share only a small deadline stack.
/// The mutex satisfies the driver's Send callback contract and keeps nested
/// reads on this same connection from replacing their parent's deadline.
#[derive(Debug)]
struct DeadlineControl {
    limits: SqliteLimits,
    state: Mutex<DeadlineState>,
}

#[derive(Debug, Default)]
struct DeadlineState {
    next_id: u64,
    active: Vec<ActiveDeadline>,
    failure: Option<String>,
}

#[derive(Debug)]
struct ActiveDeadline {
    id: u64,
    deadline: Instant,
    operation: &'static str,
    statement_hash: String,
}

impl DeadlineControl {
    /// Start a fresh execution budget, not a timer tied to connection age.
    fn start(
        self: &Arc<Self>,
        operation: &'static str,
        statement_hash: &str,
    ) -> rusqlite::Result<Deadline> {
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(self.limits.execution_timeout_ms))
            .ok_or_else(|| {
                policy_error("SQL execution timeout exceeds the monotonic clock range")
            })?;
        let mut state = self
            .state
            .lock()
            .map_err(|source| policy_error(format!("SQL deadline state poisoned: {source}")))?;
        if state.active.is_empty() {
            state.failure = None;
        }
        let id = state
            .next_id
            .checked_add(1)
            .ok_or_else(|| policy_error("SQL deadline identity overflow"))?;
        state.next_id = id;
        state.active.push(ActiveDeadline {
            id,
            deadline,
            operation,
            statement_hash: statement_hash.to_owned(),
        });
        Ok(Deadline {
            control: Arc::clone(self),
            id,
        })
    }

    /// SQLite calls this between VM work batches; lock waits retain their own busy timeout.
    fn should_interrupt(&self) -> bool {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(source) => {
                error!(event = "sql.deadline_state_failed", error = %source,
                    "SQLite execution interrupted because its deadline state is unavailable");
                return true;
            }
        };
        let now = Instant::now();
        let Some(expired) = state
            .active
            .iter()
            .filter(|active| active.deadline <= now)
            .min_by_key(|active| active.deadline)
        else {
            return false;
        };
        let message = format!(
            "SQL {} exceeded its {} ms execution budget (statement {})",
            expired.operation, self.limits.execution_timeout_ms, expired.statement_hash
        );
        if state.failure.is_none() {
            error!(event = "sql.execution_timed_out", operation = expired.operation,
                statement_hash = %expired.statement_hash, limit_ms = self.limits.execution_timeout_ms,
                "SQLite progress callback requested interruption");
            state.failure = Some(message);
        }
        true
    }
}

/// Each iterator/call owns exactly one stack entry; drop disarms all exit paths.
struct Deadline {
    control: Arc<DeadlineControl>,
    id: u64,
}

impl Deadline {
    /// Attach the known timeout reason while retaining SQLite's original error/code.
    fn explain(&self, error: rusqlite::Error) -> rusqlite::Error {
        let rusqlite::Error::SqliteFailure(code, detail) = error else {
            return error;
        };
        if code.code != rusqlite::ErrorCode::OperationInterrupted {
            return rusqlite::Error::SqliteFailure(code, detail);
        }
        let reason = match self.control.state.lock() {
            Ok(state) => state.failure.clone(),
            Err(source) => Some(format!("SQL deadline state poisoned: {source}")),
        };
        let detail = match (detail, reason) {
            (Some(detail), Some(reason)) => Some(format!("{detail}; {reason}")),
            (detail, None) => detail,
            (None, reason) => reason,
        };
        rusqlite::Error::SqliteFailure(code, detail)
    }
}

impl Drop for Deadline {
    /// Remove only this scope; an outer streaming query remains timed during nested reads.
    fn drop(&mut self) {
        match self.control.state.lock() {
            Ok(mut state) => {
                if let Some(index) = state.active.iter().position(|entry| entry.id == self.id) {
                    state.active.remove(index);
                } else {
                    error!(
                        event = "sql.deadline_scope_missing",
                        scope_id = self.id,
                        "SQL deadline scope was already absent during cleanup"
                    );
                }
            }
            Err(source) => error!(event = "sql.deadline_cleanup_failed", error = %source,
                scope_id = self.id, "SQL deadline cleanup could not access its state"),
        }
    }
}

/// The only native-connection owner; consumers cannot bypass timed SQL methods.
#[derive(Debug)]
pub(crate) struct Connection {
    native: rusqlite::Connection,
    settings: Arc<RuntimeSettings>,
    control: Arc<DeadlineControl>,
}

impl Connection {
    /// Explicit setup alone uses CREATE; ordinary opens provide their own read/write flags.
    pub(crate) fn open(path: &Path, settings: Arc<RuntimeSettings>) -> rusqlite::Result<Self> {
        Self::wrap(rusqlite::Connection::open(path)?, settings)
    }

    /// Install the same deadline policy for every read-only and write connection.
    pub(crate) fn open_with_flags(
        path: &Path,
        flags: OpenFlags,
        settings: Arc<RuntimeSettings>,
    ) -> rusqlite::Result<Self> {
        Self::wrap(
            rusqlite::Connection::open_with_flags(path, flags)?,
            settings,
        )
    }

    /// Keep the callback alive exactly as long as its owning native connection.
    fn wrap(
        native: rusqlite::Connection,
        settings: Arc<RuntimeSettings>,
    ) -> rusqlite::Result<Self> {
        let limits = settings.limits.sqlite;
        let control = Arc::new(DeadlineControl {
            limits,
            state: Mutex::new(DeadlineState::default()),
        });
        let callback = Arc::clone(&control);
        native.progress_handler(
            limits.progress_operations,
            Some(move || callback.should_interrupt()),
        );
        native.busy_timeout(Duration::from_millis(limits.busy_timeout_ms))?;
        Ok(Self {
            native,
            settings,
            control,
        })
    }

    /// Captured settings are immutable for this connection and every transaction it creates.
    pub(crate) fn settings(&self) -> &RuntimeSettings {
        &self.settings
    }

    /// Data readers use the same resource admission policy as the owning operation.
    pub(crate) fn limits(&self) -> &RuntimeLimits {
        &self.settings.limits
    }

    /// Parsing/compilation has its own budget; later executions receive a fresh one.
    pub(crate) fn prepare(&self, sql: &str) -> rusqlite::Result<Statement<'_>> {
        let hash = sha256_hex_bytes(sql.as_bytes());
        let budget = self.control.start("prepare", &hash)?;
        let native = self
            .native
            .prepare(sql)
            .map_err(|source| budget.explain(source))?;
        Ok(Statement {
            native: StatementKind::Owned(native),
            control: Arc::clone(&self.control),
            statement_hash: hash,
        })
    }

    /// Preserve the native prepared-statement cache while timing every reuse independently.
    pub(crate) fn prepare_cached(&self, sql: &str) -> rusqlite::Result<Statement<'_>> {
        let hash = sha256_hex_bytes(sql.as_bytes());
        let budget = self.control.start("prepare_cached", &hash)?;
        let native = self
            .native
            .prepare_cached(sql)
            .map_err(|source| budget.explain(source))?;
        Ok(Statement {
            native: StatementKind::Cached(native),
            control: Arc::clone(&self.control),
            statement_hash: hash,
        })
    }

    /// Direct SQL calls share the same statement wrapper as explicitly prepared work.
    pub(crate) fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.prepare(sql)?.execute(params)
    }

    /// Bound an explicit SQL batch as one execution call without changing its transaction semantics.
    pub(crate) fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        let budget = self
            .control
            .start("execute_batch", &sha256_hex_bytes(sql.as_bytes()))?;
        self.native
            .execute_batch(sql)
            .map_err(|source| budget.explain(source))
    }

    /// Keep row conversion inside the SQL read scope so nested reads preserve the parent's timer.
    pub(crate) fn query_row<T, P: Params, F: FnOnce(&Row<'_>) -> rusqlite::Result<T>>(
        &self,
        sql: &str,
        params: P,
        map: F,
    ) -> rusqlite::Result<T> {
        self.prepare(sql)?.query_row(params, map)
    }

    /// The mutable public borrow prevents nested transactions; the native unchecked
    /// constructor merely lets the wrapper retain an immutable connection reference.
    pub(crate) fn transaction_with_behavior(
        &mut self,
        behavior: TransactionBehavior,
    ) -> rusqlite::Result<Transaction<'_>> {
        let budget = self
            .control
            .start("begin_transaction", "transaction_begin")?;
        let native = rusqlite::Transaction::new_unchecked(&self.native, behavior)
            .map_err(|source| budget.explain(source))?;
        Ok(Transaction {
            native: Some(native),
            connection: self,
        })
    }

    /// Report actual SQLite transaction state for existing durable-outcome diagnostics.
    pub(crate) fn is_autocommit(&self) -> bool {
        self.native.is_autocommit()
    }

    /// Setup treats close failure as fatal and discards the connection; retain
    /// the driver's original error without returning an unused recovery handle.
    pub(crate) fn close(self) -> rusqlite::Result<()> {
        self.native.close().map_err(|(_, source)| source)
    }
}

/// Preserve rollback-on-drop through the bounded connection, never the driver's silent cleanup.
pub(crate) struct Transaction<'connection> {
    // The native borrow proves transaction ownership; Option distinguishes completed control SQL.
    native: Option<rusqlite::Transaction<'connection>>,
    connection: &'connection Connection,
}

impl Transaction<'_> {
    /// Commit errors retain SQLite context; callers continue to report unconfirmed durable outcomes.
    pub(crate) fn commit(mut self) -> rusqlite::Result<()> {
        // The execution guard ends before self drops on error, so cleanup gets
        // a fresh budget instead of inheriting an expired COMMIT deadline.
        let result = self.connection.execute_batch("COMMIT;");
        if result.is_ok() {
            self.release_native();
        }
        result
    }

    /// Explicit cancellation rollback has the same bounded SQL execution policy as writes.
    pub(crate) fn rollback(mut self) -> rusqlite::Result<()> {
        let result = self.connection.execute_batch("ROLLBACK;");
        if result.is_ok() {
            self.release_native();
        }
        result
    }

    /// Consume only the driver's borrow marker and prohibit its unbounded, error-suppressing Drop SQL.
    fn release_native(&mut self) -> bool {
        let Some(mut native) = self.native.take() else {
            return false;
        };
        native.set_drop_behavior(DropBehavior::Ignore);
        true
    }
}

impl Drop for Transaction<'_> {
    /// Early returns and failed explicit control statements receive one observable bounded cleanup attempt.
    fn drop(&mut self) {
        if !self.release_native() {
            return;
        }
        let started = Instant::now();
        let already_closed = self.connection.is_autocommit();
        debug!(
            event = "sql.drop_rollback.started",
            already_closed,
            limit_ms = self.connection.limits().sqlite.execution_timeout_ms,
            "transaction cleanup started"
        );
        // SQLite may already have rolled back after a statement error. Its
        // actual autocommit flag prevents a misleading second ROLLBACK failure.
        if already_closed {
            debug!(
                event = "sql.drop_rollback.completed",
                already_closed,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "transaction was already closed before cleanup"
            );
            return;
        }
        let result = self.connection.execute_batch("ROLLBACK;");
        let transaction_open = !self.connection.is_autocommit();
        match result {
            Ok(()) if !transaction_open => debug!(
                event = "sql.drop_rollback.completed",
                already_closed = false,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "transaction rollback completed during cleanup"
            ),
            Ok(()) => error!(
                event = "sql.drop_rollback.unconfirmed",
                transaction_open,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "ROLLBACK returned success but SQLite still reports an open transaction"
            ),
            Err(source) => error!(event = "sql.drop_rollback.failed", transaction_open,
                error = %source,
                error_chain = %crate::util::error_chain(&source, &self.connection.limits().diagnostics),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "bounded transaction cleanup failed; original caller error is preserved"),
        }
    }
}

impl Deref for Transaction<'_> {
    type Target = Connection;
    /// All transaction queries route through the bounded connection rather than native deref.
    fn deref(&self) -> &Connection {
        self.connection
    }
}

enum StatementKind<'connection> {
    Owned(rusqlite::Statement<'connection>),
    Cached(rusqlite::CachedStatement<'connection>),
}

impl<'connection> StatementKind<'connection> {
    /// The private bridge preserves cached ownership without exposing unchecked SQL to consumers.
    fn get(&self) -> &rusqlite::Statement<'connection> {
        match self {
            Self::Owned(statement) => statement,
            Self::Cached(statement) => statement,
        }
    }

    /// Statement mutation stays behind execution-scoped wrapper methods.
    fn get_mut(&mut self) -> &mut rusqlite::Statement<'connection> {
        match self {
            Self::Owned(statement) => statement,
            Self::Cached(statement) => statement,
        }
    }
}

/// Prepared SQL retains only a fingerprint for diagnostics, never expanded parameter values.
pub(crate) struct Statement<'connection> {
    native: StatementKind<'connection>,
    control: Arc<DeadlineControl>,
    statement_hash: String,
}

impl Statement<'_> {
    /// Schema introspection needs column names without executing the statement.
    pub(crate) fn column_names(&self) -> Vec<&str> {
        self.native.get().column_names()
    }

    /// A reused write statement starts a new budget for each invocation.
    pub(crate) fn execute<P: Params>(&mut self, params: P) -> rusqlite::Result<usize> {
        let budget = self.control.start("execute", &self.statement_hash)?;
        self.native
            .get_mut()
            .execute(params)
            .map_err(|source| budget.explain(source))
    }

    /// A one-row query drops its budget when row conversion succeeds or fails.
    pub(crate) fn query_row<T, P: Params, F: FnOnce(&Row<'_>) -> rusqlite::Result<T>>(
        &mut self,
        params: P,
        map: F,
    ) -> rusqlite::Result<T> {
        let budget = self.control.start("query_row", &self.statement_hash)?;
        self.native
            .get_mut()
            .query_row(params, map)
            .map_err(|source| budget.explain(source))
    }

    /// Streaming consumers retain their deadline across all rows, including nested reads.
    pub(crate) fn query<P: Params>(&mut self, params: P) -> rusqlite::Result<Rows<'_>> {
        let budget = self.control.start("query", &self.statement_hash)?;
        let native = self
            .native
            .get_mut()
            .query(params)
            .map_err(|source| budget.explain(source))?;
        Ok(Rows {
            native,
            budget: Some(budget),
        })
    }

    /// Mapped iteration has the same lifetime/budget contract as explicit row iteration.
    pub(crate) fn query_map<T, P: Params, F: FnMut(&Row<'_>) -> rusqlite::Result<T>>(
        &mut self,
        params: P,
        map: F,
    ) -> rusqlite::Result<MappedRows<'_, F>> {
        let budget = self.control.start("query_map", &self.statement_hash)?;
        let native = self
            .native
            .get_mut()
            .query_map(params, map)
            .map_err(|source| budget.explain(source))?;
        Ok(MappedRows {
            native,
            budget: Some(budget),
        })
    }
}

/// Native rows reset before the guard drops; no active query timer survives exhaustion.
pub(crate) struct Rows<'statement> {
    native: rusqlite::Rows<'statement>,
    budget: Option<Deadline>,
}

impl Rows<'_> {
    /// Preserve borrowed Row lifetimes while enriching only known timeout failures.
    pub(crate) fn next(&mut self) -> rusqlite::Result<Option<&Row<'_>>> {
        match self.native.next() {
            Ok(None) => {
                self.budget.take();
                Ok(None)
            }
            Ok(Some(row)) => Ok(Some(row)),
            Err(source) => Err(match &self.budget {
                Some(budget) => budget.explain(source),
                None => source,
            }),
        }
    }
}

pub(crate) struct MappedRows<'statement, F> {
    native: rusqlite::MappedRows<'statement, F>,
    budget: Option<Deadline>,
}

impl<T, F: FnMut(&Row<'_>) -> rusqlite::Result<T>> Iterator for MappedRows<'_, F> {
    type Item = rusqlite::Result<T>;
    /// The guard ends on exhaustion; conversion errors do not silently disarm later rows.
    fn next(&mut self) -> Option<Self::Item> {
        match self.native.next() {
            Some(result) => Some(result.map_err(|source| match &self.budget {
                Some(budget) => budget.explain(source),
                None => source,
            })),
            None => {
                self.budget.take();
                None
            }
        }
    }
}

/// Internal deadline-state faults use SQLite's abort family and retain specific context.
fn policy_error(message: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ABORT),
        Some(message.into()),
    )
}
