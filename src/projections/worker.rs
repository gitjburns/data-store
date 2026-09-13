//! One synchronous owner publishes committed annotations into graph, summary,
//! dense, and ColBERT retrieval projections. Model calls never hold SQLite
//! transactions; a maintenance lease covers each cycle through its last result
//! and health update so rebuild cannot remove storage beneath an in-flight call.

use std::collections::{BTreeSet, HashMap};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::sqlite::{Connection, Transaction};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use tracing::{debug, error, info};

use crate::artifact_store::{ArtifactRef, ArtifactStore};
use crate::error::ApiError;
use crate::hot_plane::{self, WriteTransactionAttempt};
use crate::inference::InferenceRuntime;
use crate::limits::DiagnosticLimits;
use crate::maintenance::{AnnotationCancelReason, AnnotationCancellation, MaintenanceGate};
use crate::model::{ProducerType, Provenance};
use crate::monitoring::WorkHandle;
use crate::monitoring_types::{MonitorIssue, MonitorState, WorkIdentity};
use crate::primitives::utc_now;
use crate::state::{ExclusiveGate, ProjectionHealth, ShutdownSignal, acquire_model_call_gate_on};
use crate::types::{ProjectionActivity, ProjectionDocumentProgress, ProjectionPublicationCounts};
use crate::util::{LogContext, panic_payload_message, truncate_persisted_detail};

use super::annotation::{self, CohortPlan, EmbeddedRepresentation, PreparedProjection};
use super::annotation_io;
use super::envelope::{self, NewProjection, ProjectionType};
use super::{dense, graph, view};

const TX_NAMESPACE: &str = "projection_worker";
const ACTIVE_SOURCES_SQL: &str = "
WITH active AS (
SELECT id, active_parse_id,
       (SELECT json_group_array(native_uri) FROM source_locations
        WHERE source_id = source_objects.id AND status = 'current') AS paths
FROM source_objects WHERE active_parse_id IS NOT NULL AND deactivated_at IS NULL
ORDER BY id LIMIT ?1)
SELECT id, active_parse_id,
 CASE WHEN length(CAST(paths AS BLOB)) <= ?2 THEN paths END FROM active ORDER BY id";
const ACTIVE_SQL: &str = "SELECT EXISTS(SELECT 1 FROM source_objects
 WHERE id = ?1 AND active_parse_id = ?2 AND deactivated_at IS NULL)";
const INPUT_IDS_SQL: &str = "SELECT id FROM semantic_annotations
 WHERE source_id = ?1 AND parse_id = ?2 AND freshness_status = 'fresh'
 AND deleted_at IS NULL AND (annotation_type = ?3 OR annotation_type = ?4)
 ORDER BY id LIMIT ?5";
const PUBLISHED_SQL: &str = "SELECT id,
 CASE WHEN length(CAST(input_annotation_ids_json AS BLOB)) <= ?4
 THEN input_annotation_ids_json END
 FROM retrieval_projections WHERE source_id = ?1 AND parse_id = ?2
 AND projection_type = ?3 AND index_name IS NULL AND index_partition IS NULL
 AND freshness_status = 'fresh' AND deleted_at IS NULL LIMIT 2";
const GRAPH_OWNERSHIP_SQL: &str = "SELECT
 EXISTS(SELECT 1 FROM graph_entity_mentions WHERE parse_id = ?1
        AND (projection_id <> ?2 OR source_id <> ?3))
 OR EXISTS(SELECT 1 FROM graph_entity_edges WHERE parse_id = ?1
        AND (projection_id <> ?2 OR source_id <> ?3))";
const COHORT_INPUT_STATE_SQL: &str = "SELECT p.freshness_status,
 EXISTS(SELECT 1 FROM json_each(p.input_annotation_ids_json) AS input
   LEFT JOIN semantic_annotations AS a ON a.id = input.value
   WHERE a.id IS NULL OR a.freshness_status <> 'fresh' OR a.deleted_at IS NOT NULL),
 EXISTS(SELECT 1 FROM json_each(p.input_annotation_ids_json) AS input
   JOIN semantic_annotations AS a ON a.id = input.value
   WHERE a.source_id <> p.source_id OR a.parse_id <> p.parse_id)
 FROM retrieval_projections AS p WHERE p.id = ?1 AND p.source_id = ?2
 AND p.parse_id = ?3 AND p.deleted_at IS NULL";
const CAPTURED_PAIR_SQL: &str = "SELECT id FROM retrieval_projections
 WHERE source_id = ?1 AND parse_id = ?2 AND index_name = ?3 AND index_partition = ?4
 AND payload_uri = ?5 AND projection_type IN ('dense_vector', 'multi_vector')
 AND freshness_status = 'fresh' AND deleted_at IS NULL ORDER BY id LIMIT 3";

/// Owned process handles are explicit; only the runtime and small health slot
/// cross thread boundaries. SQLite connections stay on the publication thread.
pub(crate) struct WorkerInputs {
    pub(crate) index_root: crate::runtime::StorageContext,
    pub(crate) runtime: Option<Arc<InferenceRuntime>>,
    pub(crate) dense_dimension: usize,
    pub(crate) colbert_dimension: usize,
    pub(crate) model_gate: Arc<ExclusiveGate>,
    pub(crate) shutdown: Arc<ShutdownSignal>,
    pub(crate) maintenance: Arc<MaintenanceGate>,
    pub(crate) health: Arc<Mutex<ProjectionHealth>>,
}

/// The cycle captures active identity and current display locations together.
struct ActiveSource {
    source_id: String,
    parse_id: String,
    source_paths: Vec<String>,
}

impl ActiveSource {
    /// Keep canonical identity stable while locations remain presentation fields.
    fn monitor_identity(&self) -> WorkIdentity {
        WorkIdentity {
            worker: crate::monitoring::PUBLICATION_WORKER.to_owned(),
            document: self.source_paths.join(", "),
            source_id: Some(self.source_id.clone()),
            parse_id: Some(self.parse_id.clone()),
        }
    }
}

/// Ephemeral scheduling positions carry no durable work state; discovery remains
/// authoritative after restart, while source/cohort rotation prevents starvation.
#[derive(Default)]
struct PublicationCursor {
    last_source: Option<String>,
    last_cohort: HashMap<String, String>,
}

/// One cycle shares a fixed model allowance across independently measured sources.
struct EmbeddingBudget<'a> {
    remaining: usize,
    cursor: &'a mut PublicationCursor,
}

/// Compact current publication identities exclude retired inputs; pending retirement remains
/// visible even when no current annotations produce a replacement cohort plan.
#[derive(Default)]
struct PublicationInventory {
    publications: HashMap<String, annotation::PublishedCohort>,
    pending_retirements: BTreeSet<String>,
    cancelled: Option<AnnotationCancelReason>,
}

/// Cancellation and ordinary contention do not create failed projection records.
enum PublicationOutcome {
    Current,
    Published,
    Deferred,
    InputsChanged,
    Inactive,
    Cancelled(AnnotationCancelReason),
}

/// Cancellation carries the observed reason through archival instead of rereading
/// a control signal and inventing a reason when the result crosses a boundary.
enum EmbeddingOutcome {
    Archived(ArtifactRef),
    Cancelled(AnnotationCancelReason),
}

/// The two cheap materializations have independent inputs, failure, and commit boundaries.
#[derive(Clone, Copy)]
enum Materialization {
    Graph,
    Summary,
}

impl Materialization {
    /// Name both the persisted projection kind and its compact lifecycle records.
    fn name(self) -> &'static str {
        match self {
            Self::Graph => "graph_projection",
            Self::Summary => "summary",
        }
    }

    /// Select only the envelope type owned by this materialization.
    fn projection_type(self) -> ProjectionType {
        match self {
            Self::Graph => ProjectionType::GraphProjection,
            Self::Summary => ProjectionType::Summary,
        }
    }

    /// Ignore unfinished unrelated types while identifying consumed fresh inputs.
    fn annotation_types(self) -> (&'static str, &'static str) {
        match self {
            Self::Graph => ("entity", "relation"),
            Self::Summary => ("summary", "summary"),
        }
    }
}

/// Retain worker-level failures independently of document retirement and coverage.
fn publication_worker_issue(
    monitoring: &crate::monitoring::Monitoring,
    stage: &str,
    message: &str,
) {
    monitoring.set_issue(
        "projection-worker".to_owned(),
        MonitorIssue {
            observed_since: None,
            elapsed_ms: 0,
            identity: WorkIdentity::new(
                crate::monitoring::PUBLICATION_WORKER,
                "worker",
                None,
                None,
            ),
            stage: stage.to_owned(),
            state: MonitorState::Unavailable,
            message: message.to_owned(),
            affected: 1,
            retry_in_ms: None,
            attempt: None,
            retry_limit: None,
        },
    );
}

/// Start a diagnostic-only publication owner with observable spawn/panic outcomes.
/// The caller retains and joins the handle during common shutdown cleanup.
pub(crate) fn start(inputs: WorkerInputs) -> Result<JoinHandle<()>, ApiError> {
    let failed_health = Arc::clone(&inputs.health);
    let failure_monitor = Arc::clone(inputs.index_root.monitoring());
    // Spawn failures occur after the closure has taken ownership of inputs.
    let diagnostics = inputs.index_root.limits().diagnostics;
    thread::Builder::new()
        .name("projection-worker".to_owned())
        .spawn(move || {
            let context = LogContext::new("worker", "projections");
            let _entered = context.enter();
            info!(
                event = "projection_worker.thread_started",
                embeddings_available = inputs.runtime.is_some(),
                "annotation projection worker started"
            );
            match catch_unwind(AssertUnwindSafe(|| run_worker(&inputs))) {
                Ok(Ok(())) => info!(
                    event = "projection_worker.thread_stopped",
                    reason = "shutdown",
                    "annotation projection worker stopped cleanly"
                ),
                Ok(Err(source)) => {
                    publication_worker_issue(
                        inputs.index_root.monitoring(),
                        "worker lifecycle",
                        &source.to_string(),
                    );
                    error!(event = "projection_worker.thread_failed", error = %source,
                    "annotation projection worker stopped after a lifecycle failure");
                    unavailable(&inputs.health, &source.to_string(), &diagnostics);
                }
                Err(payload) => {
                    let detail = panic_payload_message(payload.as_ref(), &diagnostics);
                    publication_worker_issue(
                        inputs.index_root.monitoring(),
                        "worker panic",
                        &detail,
                    );
                    error!(event = "projection_worker.thread_panicked", is_panic = true,
                    panic_message = %detail, "annotation projection worker panicked");
                    unavailable(&inputs.health, &detail, &diagnostics);
                }
            }
        })
        .map_err(|source| {
            let error = failure(format!(
                "failed to spawn annotation projection worker: {source}"
            ));
            error!(event = "projection_worker.spawn_failed", error = %error,
            "annotation projection publication unavailable");
            unavailable(&failed_health, &error.to_string(), &diagnostics);
            publication_worker_issue(&failure_monitor, "worker spawn", &error.to_string());
            error
        })
}

/// Retain maintenance admission through in-flight blocking calls, artifact writes,
/// publication, and health. A reset generation discards the prior inventory.
fn run_worker(inputs: &WorkerInputs) -> Result<(), ApiError> {
    // Both workers observe the same rebuild/shutdown signal; this worker polls
    // it between blocking calls instead of introducing an asynchronous model path.
    let cancellation = inputs.maintenance.annotation_cancellation();
    let mut generation = 0;
    let mut corpus_generation = 0;
    let mut delay = Duration::ZERO;
    let mut cursor = PublicationCursor::default();
    loop {
        let Some(permit) =
            inputs
                .maintenance
                .worker_permit("projections", delay, generation, &inputs.shutdown)?
        else {
            break;
        };
        if permit.generation() != generation {
            generation = permit.generation();
            cursor = PublicationCursor::default();
        }
        if permit.corpus_generation() != corpus_generation {
            corpus_generation = permit.corpus_generation();
            update_health(&inputs.health, |health| {
                health.documents = None;
                health.measured_at = None;
            });
        }
        match run_cycle(inputs, &cancellation, &mut cursor) {
            Ok(()) => inputs
                .index_root
                .monitoring()
                .clear_issue("projection-worker"),
            Err(source) => {
                publication_worker_issue(
                    inputs.index_root.monitoring(),
                    "discovery",
                    &source.to_string(),
                );
                error!(event = "projection_worker.cycle_failed", error = %source,
                    "projection discovery failed; previous document measurements retained");
                worker_activity(
                    &inputs.health,
                    ProjectionActivity::RetryWait,
                    Some(source.to_string()),
                    &inputs.index_root.limits().diagnostics,
                )?;
            }
        }
        if let Some(reason) = cancelled(inputs, &cancellation) {
            stop_activity(&inputs.health, reason.label())?;
        }
        // Existing HTTP clients finish or reach configured timeouts before this
        // point. Dropping the lease earlier would race rebuild artifact cleanup.
        drop(permit);
        delay = Duration::from_millis(inputs.index_root.limits().workers.projection_interval_ms);
    }
    stop_activity(&inputs.health, "shutdown")
}

/// Publish cheap graph/summary work for the entire inventory before any embedding
/// request, so an embedding failure cannot delay those independent publications.
fn run_cycle(
    inputs: &WorkerInputs,
    cancellation: &AnnotationCancellation,
    cursor: &mut PublicationCursor,
) -> Result<(), ApiError> {
    let started = Instant::now();
    let sources = discover_sources(inputs)?;
    let active_ids: BTreeSet<&str> = sources
        .iter()
        .map(|source| source.source_id.as_str())
        .collect();
    cursor
        .last_cohort
        .retain(|source_id, _| active_ids.contains(source_id.as_str()));
    // Preserve last valid publication counts during discovery; a routine check
    // must not turn completed embeddings back into an unmeasured inventory.
    let mut documents = publish_inventory(inputs, &sources)?;
    for (source, document) in sources.iter().zip(&mut documents) {
        for family in [Materialization::Graph, Materialization::Summary] {
            if cancelled(inputs, cancellation).is_some() || cancellation.dispatch_paused() {
                return Ok(());
            }
            let result = publish_materialization(inputs, cancellation, source, family);
            apply_materialization_result(inputs, cancellation, source, document, family, result);
        }
        publish_document(inputs, document)?;
    }
    // Runtime absence is explicit while cheap materialization remains usable.
    if let Some(runtime) = &inputs.runtime {
        let start = cursor.last_source.as_deref().map_or(0, |last| {
            sources.partition_point(|source| source.source_id.as_str() <= last)
        });
        let mut budget = EmbeddingBudget {
            // Revisit cheap graph/summary publications between bounded embedding phases.
            remaining: inputs
                .index_root
                .limits()
                .workers
                .projection_cohorts_per_cycle,
            cursor,
        };
        for offset in 0..sources.len() {
            // The body is never reached for an empty inventory, so the modulus
            // has a nonzero denominator without an artificial fallback source.
            let index = (start + offset) % sources.len();
            let source = &sources[index];
            let document = &mut documents[index];
            if cancelled(inputs, cancellation).is_some() || cancellation.dispatch_paused() {
                return Ok(());
            }
            if let Err(source_error) = publish_document_embeddings(
                inputs,
                cancellation,
                runtime,
                source,
                document,
                &mut budget,
            ) {
                inputs.index_root.monitoring().set_issue(
                    format!("projection-source:{}", source.parse_id),
                    MonitorIssue {
                        observed_since: None,
                        elapsed_ms: 0,
                        identity: source.monitor_identity(),
                        stage: "embedding discovery".to_owned(),
                        state: MonitorState::Unavailable,
                        message: source_error.to_string(),
                        affected: 1,
                        retry_in_ms: None,
                        attempt: None,
                        retry_limit: None,
                    },
                );
                error!(event = "projection_worker.document_failed", source_id = %source.source_id,
                    parse_id = %source.parse_id, error = %source_error, error_kind = source_error.error_kind(),
                    "annotation embedding discovery failed for one source");
                document.activity = ProjectionActivity::RetryWait;
                document.detail = Some(truncate_persisted_detail(
                    &source_error.to_string(),
                    &inputs.index_root.limits().diagnostics,
                ));
                publish_document(inputs, document)?;
            } else {
                inputs
                    .index_root
                    .monitoring()
                    .clear_issue(&format!("projection-source:{}", source.parse_id));
            }
        }
    } else {
        inputs.index_root.monitoring().set_issue("projection-runtime".to_owned(), MonitorIssue {
            observed_since: None, elapsed_ms: 0,
            identity: WorkIdentity::new(crate::monitoring::PUBLICATION_WORKER, "worker", None, None),
            stage: "embedding runtime".to_owned(), state: MonitorState::Unavailable,
            message: "Annotation embedding runtime unavailable; graph and summary publication remain independent.".to_owned(),
            affected: sources.len() as u64, retry_in_ms: None, attempt: None, retry_limit: None,
        });
        for document in &mut documents {
            document.activity = ProjectionActivity::Unavailable;
            document.detail = Some("Annotation embedding runtime is unavailable; graph and summary publication remain independent.".to_owned());
            publish_document(inputs, document)?;
        }
    }
    let failed: u64 = documents
        .iter()
        .map(|document| {
            document.graph.failed
                + document.summary.failed
                + document.embeddings.map_or(0, |counts| counts.failed)
        })
        .sum();
    let pending: u64 = documents
        .iter()
        .map(|document| {
            document.graph.pending
                + document.summary.pending
                + document.embeddings.map_or(0, |counts| counts.pending)
        })
        .sum();
    let activity = if inputs.runtime.is_none() {
        ProjectionActivity::Unavailable
    } else if failed > 0
        || documents
            .iter()
            .any(|document| document.activity == ProjectionActivity::RetryWait)
    {
        ProjectionActivity::RetryWait
    } else if pending > 0 {
        ProjectionActivity::Pending
    } else {
        ProjectionActivity::Complete
    };
    worker_activity(
        &inputs.health,
        activity,
        inputs
            .runtime
            .is_none()
            .then(|| "Annotation embedding runtime is unavailable.".to_owned()),
        &inputs.index_root.limits().diagnostics,
    )?;
    debug!(
        event = "projection_worker.cycle_completed",
        source_count = sources.len(),
        failed,
        pending,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "annotation projection discovery completed"
    );
    Ok(())
}

/// Read an explicitly bounded active inventory on one read-only connection.
fn discover_sources(inputs: &WorkerInputs) -> Result<Vec<ActiveSource>, ApiError> {
    let connection = hot_plane::open_read(&inputs.index_root)?;
    let mut statement = connection
        .prepare(ACTIVE_SOURCES_SQL)
        .map_err(|source| failure(format!("prepare projection source inventory: {source}")))?;
    let rows = statement
        .query_map(
            params![
                connection.limits().resources.max_sources + 1,
                connection.limits().resources.max_json_cell_bytes
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .map_err(|source| failure(format!("read projection source inventory: {source}")))?;
    let mut sources = Vec::new();
    for row in rows {
        let (source_id, parse_id, paths) =
            row.map_err(|source| failure(format!("decode projection source inventory: {source}")))?;
        if sources.len() == connection.limits().resources.max_sources {
            return Err(failure(
                "resource limit: projection source inventory exceeds resources.max_sources",
            ));
        }
        let paths = paths.ok_or_else(|| {
            failure(format!(
                "resource limit: projection source {source_id} locations exceed resources.max_json_cell_bytes"
            ))
        })?;
        let source_paths = serde_json::from_str(&paths).map_err(|source| {
            failure(format!(
                "decode projection locations for {source_id}: {source}"
            ))
        })?;
        sources.push(ActiveSource {
            source_id,
            parse_id,
            source_paths,
        });
    }
    Ok(sources)
}

/// Every publication transaction rechecks both active identity and deactivation.
fn is_active(conn: &Connection, source_id: &str, parse_id: &str) -> Result<bool, ApiError> {
    conn.query_row(ACTIVE_SQL, params![source_id, parse_id], |row| row.get(0))
        .map_err(|source| {
            failure(format!(
                "check active projection target {source_id}/{parse_id}: {source}"
            ))
        })
}

/// Compare consumed fresh IDs to the existing envelope without requiring any
/// unfinished annotation type. Legacy graph rows also need matching envelope ownership.
fn materialization_current(
    conn: &Connection,
    source: &ActiveSource,
    family: Materialization,
) -> Result<bool, ApiError> {
    let (first, second) = family.annotation_types();
    let mut statement = conn
        .prepare(INPUT_IDS_SQL)
        .map_err(|error| failure(format!("prepare {} annotation IDs: {error}", family.name())))?;
    let rows = statement
        .query_map(
            params![
                source.source_id,
                source.parse_id,
                first,
                second,
                conn.limits().resources.max_annotation_inputs + 1
            ],
            |row| row.get::<_, String>(0),
        )
        .map_err(|error| failure(format!("read {} annotation IDs: {error}", family.name())))?;
    let mut current = BTreeSet::new();
    for row in rows {
        if current.len() == conn.limits().resources.max_annotation_inputs {
            return Err(failure(
                "resource limit: materialization input membership exceeds resources.max_annotation_inputs",
            ));
        }
        current.insert(
            row.map_err(|error| failure(format!("decode materialization annotation ID: {error}")))?,
        );
    }
    let mut statement = conn
        .prepare(PUBLISHED_SQL)
        .map_err(|error| failure(format!("prepare {} publication: {error}", family.name())))?;
    let rows = statement
        .query_map(
            params![
                source.source_id,
                source.parse_id,
                family.name(),
                conn.limits().resources.max_json_cell_bytes
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .map_err(|error| failure(format!("read {} publication: {error}", family.name())))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| failure(format!("decode {} publication: {error}", family.name())))?;
    if rows.len() != 1 {
        return Ok(false);
    }
    let (id, raw) = &rows[0];
    let Some(raw) = raw else { return Ok(false) };
    let declared: Vec<String> = serde_json::from_str(raw).map_err(|error| {
        failure(format!(
            "decode input membership of projection {id}: {error}"
        ))
    })?;
    let declared_set: BTreeSet<_> = declared.iter().cloned().collect();
    if declared_set.len() != declared.len() || declared_set != current {
        return Ok(false);
    }
    if matches!(family, Materialization::Graph) {
        let mismatched: bool = conn
            .query_row(
                GRAPH_OWNERSHIP_SQL,
                params![source.parse_id, id, source.source_id],
                |row| row.get(0),
            )
            .map_err(|error| {
                failure(format!(
                    "verify graph payload ownership for {}: {error}",
                    source.parse_id
                ))
            })?;
        if mismatched {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Each cheap representation commits independently and rechecks freshness while
/// holding the writer lock, so discovery races cannot publish a retired parse.
fn publish_materialization(
    inputs: &WorkerInputs,
    cancellation: &AnnotationCancellation,
    source: &ActiveSource,
    family: Materialization,
) -> Result<PublicationOutcome, ApiError> {
    if let Some(reason) = cancelled(inputs, cancellation) {
        return Ok(PublicationOutcome::Cancelled(reason));
    }
    {
        let mut connection = hot_plane::open_read(&inputs.index_root)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| {
                failure(format!("begin materialization discovery snapshot: {error}"))
            })?;
        if !is_active(&tx, &source.source_id, &source.parse_id)? {
            return Ok(PublicationOutcome::Inactive);
        }
        if materialization_current(&tx, source, family)? {
            return Ok(PublicationOutcome::Current);
        }
    }
    let mut connection = hot_plane::open_write(&inputs.index_root)?;
    let tx = match hot_plane::begin_write_transaction_if_free(
        &mut connection,
        TX_NAMESPACE,
        "materialize",
    )? {
        WriteTransactionAttempt::Begun(tx) => tx,
        WriteTransactionAttempt::Busy => return Ok(PublicationOutcome::Deferred),
    };
    if let Some(reason) = cancelled(inputs, cancellation) {
        rollback_cancelled(tx, family.name(), reason)?;
        return Ok(PublicationOutcome::Cancelled(reason));
    }
    let started = Instant::now();
    let mut work = None;
    let body = (|| {
        if !is_active(&tx, &source.source_id, &source.parse_id)? {
            return Ok(PublicationOutcome::Inactive);
        }
        if materialization_current(&tx, source, family)? {
            return Ok(PublicationOutcome::Current);
        }
        // Only real materialization owns an active-work row. Freshness checks
        // remain quiet, including the second check under the writer lock.
        work = Some(inputs.index_root.monitoring().work(
            source.monitor_identity(),
            family.name(),
            Some(1),
            "publication",
        ));
        info!(event = "projection_worker.materialization_started", source_id = %source.source_id,
            parse_id = %source.parse_id, projection_type = family.name(), "annotation materialization started");
        envelope::delete_for_index_partition(
            &tx,
            &source.parse_id,
            family.projection_type(),
            None,
            None,
        )?;
        match family {
            Materialization::Graph => {
                graph::build_graph_projection(&tx, &tx, &source.source_id, &source.parse_id)?
            }
            Materialization::Summary => {
                view::build_summary(&tx, &tx, &source.source_id, &source.parse_id)?
            }
        };
        Ok(PublicationOutcome::Published)
    })();
    let outcome = match body {
        Ok(outcome) => outcome,
        Err(error) => {
            let result = Err(hot_plane::abort_transaction(
                tx,
                TX_NAMESPACE,
                "materialize",
                error,
            ));
            if let Some(work) = work {
                finish_publication_work(work, &result);
            }
            return result;
        }
    };
    if let Some(reason) = cancelled(inputs, cancellation) {
        rollback_cancelled(tx, family.name(), reason)?;
        if let Some(work) = work {
            finish_publication_work(work, &Ok(PublicationOutcome::Cancelled(reason)));
        }
        return Ok(PublicationOutcome::Cancelled(reason));
    }
    let committed =
        hot_plane::commit_transaction(tx, TX_NAMESPACE, "materialize").map(|()| outcome);
    if let Some(work) = work {
        finish_publication_work(work, &committed);
    }
    let outcome = committed?;
    if matches!(outcome, PublicationOutcome::Published) {
        info!(event = "projection_worker.materialization_published", source_id = %source.source_id,
            parse_id = %source.parse_id, projection_type = family.name(), committed = true,
            elapsed_ms = started.elapsed().as_millis() as u64, "annotation materialization committed");
    }
    Ok(outcome)
}

/// Freshness changes are distinct from corrupt ownership or changed immutable content.
enum CohortInputState {
    Fresh,
    Invalid,
    Unavailable,
}

/// Retire captured publications with missing/stale declared inputs, including
/// cohorts that vanished from current discovery. Additional fresh annotations
/// leave an older valid subset available until its replacement is published.
fn retire_invalid_cohorts(
    inputs: &WorkerInputs,
    cancellation: &AnnotationCancellation,
    source: &ActiveSource,
    published: Vec<annotation::PublishedCohort>,
) -> Result<PublicationInventory, ApiError> {
    let connection = hot_plane::open_read(&inputs.index_root)?;
    let mut inventory = PublicationInventory::default();
    for publication in published {
        if let Some(reason) = cancelled(inputs, cancellation) {
            inventory.cancelled = Some(reason);
            break;
        }
        match cohort_input_state(&connection, source, &publication)? {
            CohortInputState::Fresh => {
                inventory
                    .publications
                    .insert(publication.cohort_id.clone(), publication);
            }
            CohortInputState::Unavailable => {}
            CohortInputState::Invalid => {
                match retire_cohort(inputs, cancellation, source, &publication)? {
                    PublicationOutcome::Deferred => {
                        inventory.pending_retirements.insert(publication.cohort_id);
                    }
                    PublicationOutcome::Cancelled(reason) => {
                        inventory.cancelled = Some(reason);
                        break;
                    }
                    PublicationOutcome::Current => {
                        if matches!(
                            cohort_input_state(&connection, source, &publication)?,
                            CohortInputState::Fresh
                        ) {
                            inventory
                                .publications
                                .insert(publication.cohort_id.clone(), publication);
                        }
                    }
                    PublicationOutcome::Published
                    | PublicationOutcome::Inactive
                    | PublicationOutcome::InputsChanged => {}
                }
            }
        }
    }
    Ok(inventory)
}

/// Inspect only declared input identity/freshness in SQL; no model or full vector
/// payload read is needed to distinguish valid older subsets from invalid inputs.
fn cohort_input_state(
    conn: &Connection,
    source: &ActiveSource,
    publication: &annotation::PublishedCohort,
) -> Result<CohortInputState, ApiError> {
    let state = conn
        .query_row(
            COHORT_INPUT_STATE_SQL,
            params![publication.projection_id, source.source_id, source.parse_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, bool>(1)?,
                    row.get::<_, bool>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|error| {
            failure(format!(
                "check annotation cohort {} input freshness: {error}",
                publication.cohort_id
            ))
        })?;
    let Some((status, invalid, wrong_owner)) = state else {
        return Ok(CohortInputState::Unavailable);
    };
    if wrong_owner {
        return Err(failure(format!(
            "annotation cohort {} declares an input owned by another source/parse",
            publication.cohort_id
        )));
    }
    match status.as_str() {
        "fresh" if invalid => Ok(CohortInputState::Invalid),
        "fresh" => Ok(CohortInputState::Fresh),
        "stale" | "superseded" | "failed" | "building" => Ok(CohortInputState::Unavailable),
        _ => Err(failure(format!(
            "annotation cohort {} has invalid projection freshness {status}",
            publication.cohort_id
        ))),
    }
}

/// Stale both captured halves in one writer transaction. Envelope identity and
/// payload URI prevent a later replacement from being retired by an older scan.
fn retire_cohort(
    inputs: &WorkerInputs,
    cancellation: &AnnotationCancellation,
    source: &ActiveSource,
    publication: &annotation::PublishedCohort,
) -> Result<PublicationOutcome, ApiError> {
    if let Some(reason) = cancelled(inputs, cancellation) {
        return Ok(PublicationOutcome::Cancelled(reason));
    }
    let mut connection = hot_plane::open_write(&inputs.index_root)?;
    let tx = match hot_plane::begin_write_transaction_if_free(
        &mut connection,
        TX_NAMESPACE,
        "retire_cohort",
    )? {
        WriteTransactionAttempt::Begun(tx) => tx,
        WriteTransactionAttempt::Busy => return Ok(PublicationOutcome::Deferred),
    };
    let body = (|| {
        if !is_active(&tx, &source.source_id, &source.parse_id)? {
            return Ok(PublicationOutcome::Inactive);
        }
        if !matches!(
            cohort_input_state(&tx, source, publication)?,
            CohortInputState::Invalid
        ) {
            return Ok(PublicationOutcome::Current);
        }
        let mut statement = tx
            .prepare(CAPTURED_PAIR_SQL)
            .map_err(|error| failure(format!("prepare annotation cohort retirement: {error}")))?;
        let ids = statement
            .query_map(
                params![
                    source.source_id,
                    source.parse_id,
                    annotation::INDEX_NAME,
                    publication.cohort_id,
                    publication.payload_uri
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(|error| failure(format!("read annotation cohort retirement pair: {error}")))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                failure(format!("decode annotation cohort retirement pair: {error}"))
            })?;
        if ids.len() != 2 || !ids.contains(&publication.projection_id) {
            return Err(failure(format!(
                "annotation cohort {} cannot retire an unpaired captured publication",
                publication.cohort_id
            )));
        }
        drop(statement);
        for id in ids {
            envelope::mark_stale(&tx, &id)?;
        }
        Ok(PublicationOutcome::Published)
    })();
    let outcome = match body {
        Ok(outcome) => outcome,
        Err(error) => {
            return Err(hot_plane::abort_transaction(
                tx,
                TX_NAMESPACE,
                "retire_cohort",
                error,
            ));
        }
    };
    if let Some(reason) = cancelled(inputs, cancellation) {
        rollback_cancelled(tx, "retire_cohort", reason)?;
        return Ok(PublicationOutcome::Cancelled(reason));
    }
    hot_plane::commit_transaction(tx, TX_NAMESPACE, "retire_cohort")?;
    if matches!(outcome, PublicationOutcome::Published) {
        info!(event = "projection_worker.cohort_retired", source_id = %source.source_id,
            parse_id = %source.parse_id, cohort_id = %publication.cohort_id, committed = true,
            reason = "declared_inputs_no_longer_fresh", "annotation cohort projections retired together");
    }
    Ok(outcome)
}

/// Discover one source's cohort versions together, then release SQLite before
/// tokenization or model calls. New independent annotations are picked up next cycle.
fn publish_document_embeddings(
    inputs: &WorkerInputs,
    cancellation: &AnnotationCancellation,
    runtime: &InferenceRuntime,
    source: &ActiveSource,
    document: &mut ProjectionDocumentProgress,
    budget: &mut EmbeddingBudget<'_>,
) -> Result<(), ApiError> {
    let (plans, published) = {
        let mut connection = hot_plane::open_read(&inputs.index_root)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| {
                failure(format!(
                    "begin annotation projection discovery snapshot: {error}"
                ))
            })?;
        if !is_active(&tx, &source.source_id, &source.parse_id)? {
            document.activity = ProjectionActivity::Stopped;
            document.detail = Some("Source no longer has this active parse.".to_owned());
            return publish_document(inputs, document);
        }
        (
            annotation::plan_for_parse(
                &tx,
                &source.source_id,
                &source.parse_id,
                &runtime.embedding_identity,
            )?,
            annotation::published_for_parse(&tx, &source.source_id, &source.parse_id)?,
        )
    };
    let inventory = retire_invalid_cohorts(inputs, cancellation, source, published)?;
    if let Some(reason) = inventory.cancelled {
        document.activity = ProjectionActivity::Stopped;
        document.detail = Some(format!("Publication cancelled: {}", reason.label()));
        return publish_document(inputs, document);
    }
    let mut pending = Vec::new();
    let issue_prefix = format!("projection-cohort:{}:", source.parse_id);
    let current_issue_keys: BTreeSet<_> = plans
        .iter()
        .map(|plan| format!("{issue_prefix}{}", plan.cohort_id))
        .collect();
    inputs
        .index_root
        .monitoring()
        .retain_issues(&issue_prefix, |key| current_issue_keys.contains(key));
    let mut retirement_only = inventory.pending_retirements;
    let mut counts = ProjectionPublicationCounts::default();
    for plan in plans {
        // A replacement retires its previous pair in the same transaction, so
        // it is one pending cohort rather than a second retirement obligation.
        retirement_only.remove(&plan.cohort_id);
        if let Some(publication) = inventory.publications.get(&plan.cohort_id)
            && publication.input_hash == plan.input_hash
        {
            // A matching input hash cannot hide different declared envelope
            // lineage. Validate against this plan without loading artifact bodies.
            annotation::validate_publication_lineage(publication, &plan)?;
            inputs
                .index_root
                .monitoring()
                .clear_issue(&format!("{issue_prefix}{}", plan.cohort_id));
            counts.published += 1;
        } else {
            counts.pending += 1;
            pending.push(plan);
        }
    }
    counts.pending += retirement_only.len() as u64;
    document.embeddings = Some(counts);
    document.activity = if counts.pending > 0 {
        ProjectionActivity::Pending
    } else {
        document_activity(document)
    };
    publish_document(inputs, document)?;
    let store = ArtifactStore::open_existing(&inputs.index_root)?;
    if let Some(last) = budget.cursor.last_cohort.get(&source.source_id) {
        let start = pending.partition_point(|plan| plan.cohort_id.as_str() <= last.as_str());
        pending.rotate_left(start);
    }
    let allowance = budget.remaining.min(
        inputs
            .index_root
            .limits()
            .workers
            .projection_cohorts_per_source,
    );
    for plan in pending.into_iter().take(allowance) {
        // Finish the previous cohort, including its publication commit, before
        // yielding to non-destructive recovery. No new model call is dispatched.
        if cancellation.dispatch_paused() {
            break;
        }
        if let Some(reason) = cancelled(inputs, cancellation) {
            document.activity = ProjectionActivity::Stopped;
            document.detail = Some(format!("Publication cancelled: {}", reason.label()));
            publish_document(inputs, document)?;
            break;
        }
        budget.remaining -= 1;
        budget.cursor.last_source = Some(source.source_id.clone());
        budget
            .cursor
            .last_cohort
            .insert(source.source_id.clone(), plan.cohort_id.clone());
        document.activity = ProjectionActivity::Building;
        publish_document(inputs, document)?;
        let started = Instant::now();
        info!(event = "projection_worker.embedding_started", source_id = %plan.source_id,
            parse_id = %plan.parse_id, cohort_id = %plan.cohort_id, input_hash = %plan.input_hash,
            annotation_count = plan.inputs.len(), "annotation retrieval embedding started");
        let work = inputs.index_root.monitoring().work(
            source.monitor_identity(),
            "annotation representations",
            None,
            "representations",
        );
        let monitor = work.handle();
        let result = build_and_publish_cohort(
            inputs,
            cancellation,
            runtime,
            &store,
            &plan,
            document,
            &monitor,
        );
        finish_publication_work(work, &result);
        let issue_key = format!("{issue_prefix}{}", plan.cohort_id);
        if matches!(result, Ok(PublicationOutcome::Published)) {
            inputs.index_root.monitoring().record_completed(
                source.monitor_identity(),
                "annotation embeddings",
                "Dense and ColBERT publication committed",
                "embedding cohorts published",
                1,
            );
        }
        match result {
            Ok(PublicationOutcome::Published | PublicationOutcome::Current) => {
                inputs.index_root.monitoring().clear_issue(&issue_key);
                counts.pending -= 1;
                counts.published += 1;
            }
            Ok(PublicationOutcome::Cancelled(reason)) => {
                document.activity = ProjectionActivity::Stopped;
                document.detail = Some(format!("Publication cancelled: {}", reason.label()));
                document.embeddings = Some(counts);
                publish_document(inputs, document)?;
                break;
            }
            Ok(PublicationOutcome::Inactive) => {
                document.activity = ProjectionActivity::Stopped;
                document.detail =
                    Some("Source changed active parse before publication.".to_owned());
                document.embeddings = Some(counts);
                publish_document(inputs, document)?;
                break;
            }
            Ok(PublicationOutcome::InputsChanged | PublicationOutcome::Deferred) => {
                debug!(event = "projection_worker.embedding_deferred", source_id = %plan.source_id,
                    parse_id = %plan.parse_id, cohort_id = %plan.cohort_id,
                    "captured annotation inputs changed; next cycle will rediscover them");
            }
            Err(source_error) => {
                counts.pending -= 1;
                counts.failed += 1;
                inputs.index_root.monitoring().set_issue(issue_key, MonitorIssue {
                    observed_since: None, elapsed_ms: 0,
                    identity: source.monitor_identity(),
                    stage: "annotation embedding publication".to_owned(),
                    state: MonitorState::Failed,
                    message: format!("{source_error}; committed annotations and previous publication retained"),
                    affected: 1,
                    retry_in_ms: Some(inputs.index_root.limits().workers.projection_interval_ms),
                    attempt: None,
                    retry_limit: None,
                });
                document.detail = Some(truncate_persisted_detail(
                    &source_error.to_string(),
                    &inputs.index_root.limits().diagnostics,
                ));
                error!(event = "projection_worker.embedding_failed", source_id = %plan.source_id,
                    parse_id = %plan.parse_id, cohort_id = %plan.cohort_id, error = %source_error,
                    error_kind = source_error.error_kind(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "annotation embedding failed; committed annotations and previous publication retained");
                record_failure(
                    inputs,
                    cancellation,
                    &embedding_request(&plan, ProjectionType::DenseVector),
                    &source_error,
                );
                // One endpoint failure must not multiply requests across this
                // document's remaining cohorts. Other cheap families ran first.
                document.embeddings = Some(counts);
                document.activity = ProjectionActivity::RetryWait;
                publish_document(inputs, document)?;
                break;
            }
        }
        document.embeddings = Some(counts);
        document.activity = document_activity(document);
        publish_document(inputs, document)?;
    }
    Ok(())
}

/// Prepare against one read snapshot, archive bounded model batches, then publish
/// both model forms in one transaction only if the declared inputs still match.
fn build_and_publish_cohort(
    inputs: &WorkerInputs,
    cancellation: &AnnotationCancellation,
    runtime: &InferenceRuntime,
    store: &ArtifactStore,
    plan: &CohortPlan,
    document: &mut ProjectionDocumentProgress,
    monitor: &WorkHandle,
) -> Result<PublicationOutcome, ApiError> {
    let prepared = {
        let mut connection = hot_plane::open_read(&inputs.index_root)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| {
                failure(format!("begin annotation representation snapshot: {error}"))
            })?;
        if !is_active(&tx, &plan.source_id, &plan.parse_id)? {
            return Ok(PublicationOutcome::Inactive);
        }
        annotation::prepare(&tx, plan, &runtime.colbert)?
    };
    let Some(prepared) = prepared else {
        return Ok(PublicationOutcome::InputsChanged);
    };
    let artifact = match embed_and_archive(inputs, cancellation, runtime, store, prepared, monitor)?
    {
        EmbeddingOutcome::Archived(artifact) => artifact,
        EmbeddingOutcome::Cancelled(reason) => return Ok(PublicationOutcome::Cancelled(reason)),
    };
    document.activity = ProjectionActivity::AwaitingCommit;
    monitor.waiting("embeddings archived; awaiting publication commit");
    publish_document(inputs, document)?;
    publish_cohort(inputs, cancellation, plan, &artifact, monitor)
}

/// Keep at most one small response batch of matrices/vectors resident. Inputs use
/// their actual canonical unit identity, while returned order maps representations.
fn embed_and_archive(
    inputs: &WorkerInputs,
    cancellation: &AnnotationCancellation,
    runtime: &InferenceRuntime,
    store: &ArtifactStore,
    prepared: PreparedProjection,
    monitor: &WorkHandle,
) -> Result<EmbeddingOutcome, ApiError> {
    let PreparedProjection { plan, texts } = prepared;
    let total_representations = texts.len() as u64;
    let mut representations = Vec::new();
    let mut texts = texts.into_iter();
    loop {
        // Both the worker's working set and the model's document-call bound apply.
        let batch_size = inputs
            .index_root
            .limits()
            .workers
            .projection_batch_size
            .min(runtime.colbert.document_batch_size());
        let batch: Vec<_> = texts.by_ref().take(batch_size).collect();
        if batch.is_empty() {
            break;
        }
        if let Some(reason) = cancelled(inputs, cancellation) {
            return Ok(EmbeddingOutcome::Cancelled(reason));
        }
        let passages: Vec<_> = batch.iter().map(|item| item.text.as_str()).collect();
        monitor.stage(
            "annotation dense embedding batch",
            Some(batch.len() as u64),
            "representations embedded",
        );
        let dense_vectors = {
            let _permit = if runtime.dense.uses_local_model_gate() {
                monitor.waiting("waiting for local dense model gate");
                Some(acquire_model_call_gate_on(
                    &inputs.model_gate,
                    &plan.cohort_id,
                    "dense",
                    "annotation_projection",
                )?)
            } else {
                None
            };
            if let Some(reason) = cancelled(inputs, cancellation) {
                return Ok(EmbeddingOutcome::Cancelled(reason));
            }
            monitor.running();
            dense::embed_texts(
                &runtime.dense,
                &passages,
                &plan.parse_id,
                Some(monitor),
                "annotation embedding",
            )?
        };
        if let Some(reason) = cancelled(inputs, cancellation) {
            return Ok(EmbeddingOutcome::Cancelled(reason));
        }
        let documents: Vec<_> = passages
            .iter()
            .map(|text| (plan.target.unit_id.as_str(), *text))
            .collect();
        let matrices = {
            monitor.stage(
                "annotation ColBERT embedding batch",
                Some(documents.len() as u64),
                "representations embedded",
            );
            let _permit = if runtime.colbert.uses_local_model_gate() {
                monitor.waiting("waiting for local ColBERT model gate");
                Some(acquire_model_call_gate_on(
                    &inputs.model_gate,
                    &plan.cohort_id,
                    "colbert",
                    "annotation_projection",
                )?)
            } else {
                None
            };
            if let Some(reason) = cancelled(inputs, cancellation) {
                return Ok(EmbeddingOutcome::Cancelled(reason));
            }
            monitor.running();
            let call = runtime
                .colbert
                .monitor_call(Some(monitor), "annotation embedding");
            let result = runtime.colbert.embed_documents(&documents, call.as_ref());
            if let Some(call) = call {
                call.finish_result(&result);
            }
            let matrices = result?;
            monitor.progress(documents.len() as u64, Some(documents.len() as u64));
            matrices
        };
        if let Some(reason) = cancelled(inputs, cancellation) {
            return Ok(EmbeddingOutcome::Cancelled(reason));
        }
        if dense_vectors.len() != batch.len() || matrices.len() != batch.len() {
            return Err(ApiError::InferenceInit {
                message: format!(
                    "annotation embedding response count mismatch for cohort {}",
                    plan.cohort_id
                ),
            });
        }
        // Archival measures prepared inputs only; the publication denominator
        // advances later, after both envelopes commit together.
        monitor.stage(
            "archive annotation representations",
            Some(total_representations),
            "representations archived",
        );
        for ((input, vector), matrix) in batch.into_iter().zip(dense_vectors).zip(matrices) {
            if matrix.unit_id != plan.target.unit_id || matrix.dimension != inputs.colbert_dimension
            {
                return Err(ApiError::InferenceInit {
                    message: format!(
                        "ColBERT identity/dimension mismatch for annotation representation {}",
                        input.id
                    ),
                });
            }
            let dense = annotation_io::store_embedding(store, &vector, 1, inputs.dense_dimension)?;
            let colbert = annotation_io::store_embedding(
                store,
                &matrix.vector,
                matrix.token_count,
                matrix.dimension,
            )?;
            representations.push(EmbeddedRepresentation {
                input,
                dense,
                colbert,
            });
        }
        debug!(event = "projection_worker.embedding_batch_completed", source_id = %plan.source_id,
            parse_id = %plan.parse_id, cohort_id = %plan.cohort_id,
            completed_representations = representations.len(), "annotation embedding batch archived");
        monitor.progress(representations.len() as u64, Some(total_representations));
    }
    if let Some(reason) = cancelled(inputs, cancellation) {
        return Ok(EmbeddingOutcome::Cancelled(reason));
    }
    annotation::archive(store, plan, representations).map(EmbeddingOutcome::Archived)
}

/// Hold only immutable artifact references while waiting for SQLite. Retain the
/// paid result instead of repeating model calls, but leave on rebuild/shutdown.
fn publish_cohort(
    inputs: &WorkerInputs,
    cancellation: &AnnotationCancellation,
    plan: &CohortPlan,
    artifact: &ArtifactRef,
    monitor: &WorkHandle,
) -> Result<PublicationOutcome, ApiError> {
    let started = Instant::now();
    let mut connection = hot_plane::open_write(&inputs.index_root)?;
    let mut waited = false;
    loop {
        if let Some(reason) = cancelled(inputs, cancellation) {
            return Ok(PublicationOutcome::Cancelled(reason));
        }
        let tx = match hot_plane::begin_write_transaction_if_free(
            &mut connection,
            TX_NAMESPACE,
            "embedding_publish",
        )? {
            WriteTransactionAttempt::Begun(tx) => tx,
            WriteTransactionAttempt::Busy => {
                monitor.waiting("embeddings archived; waiting for publication writer lock");
                if !waited {
                    info!(event = "projection_worker.publication_waiting", source_id = %plan.source_id,
                        parse_id = %plan.parse_id, cohort_id = %plan.cohort_id,
                        "annotation embeddings archived; waiting for publication writer lock");
                    waited = true;
                }
                inputs.shutdown.wait_timeout(Duration::from_millis(
                    inputs.index_root.limits().workers.projection_commit_wait_ms,
                ));
                continue;
            }
        };
        let body = (|| {
            if !is_active(&tx, &plan.source_id, &plan.parse_id)? {
                return Ok(PublicationOutcome::Inactive);
            }
            // New annotations outside this declared version do not starve a
            // completed cohort. Only changed/deleted declared inputs invalidate it.
            if annotation::current_inputs(&tx, plan)?.is_none() {
                return Ok(PublicationOutcome::InputsChanged);
            }
            for kind in [ProjectionType::DenseVector, ProjectionType::MultiVector] {
                envelope::delete_for_index_partition(
                    &tx,
                    &plan.parse_id,
                    kind,
                    Some(annotation::INDEX_NAME),
                    Some(&plan.cohort_id),
                )?;
                let id = envelope::insert_building(&tx, &embedding_request(plan, kind))?;
                envelope::complete_fresh(&tx, &id, Some(&artifact.uri))?;
            }
            Ok(PublicationOutcome::Published)
        })();
        let outcome = match body {
            Ok(outcome) => outcome,
            Err(error) => {
                return Err(hot_plane::abort_transaction(
                    tx,
                    TX_NAMESPACE,
                    "embedding_publish",
                    error,
                ));
            }
        };
        if let Some(reason) = cancelled(inputs, cancellation) {
            rollback_cancelled(tx, "embedding_publish", reason)?;
            return Ok(PublicationOutcome::Cancelled(reason));
        }
        hot_plane::commit_transaction(tx, TX_NAMESPACE, "embedding_publish")?;
        if matches!(outcome, PublicationOutcome::Published) {
            info!(event = "projection_worker.embedding_published", source_id = %plan.source_id,
                parse_id = %plan.parse_id, cohort_id = %plan.cohort_id, input_hash = %plan.input_hash,
                annotation_count = plan.inputs.len(), committed = true,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "dense and ColBERT annotation projections committed together");
        }
        return Ok(outcome);
    }
}

/// Finish at the publication owner's boundary; model completion alone cannot
/// imply a durable projection, and source changes are observed deferrals.
fn finish_publication_work(
    work: crate::monitoring::WorkGuard,
    result: &Result<PublicationOutcome, ApiError>,
) {
    match result {
        Ok(PublicationOutcome::Published) => {
            work.finish(MonitorState::Complete, "Projection publication committed")
        }
        Ok(PublicationOutcome::Current) => {
            work.finish(MonitorState::Idle, "Projection inputs already published")
        }
        Ok(PublicationOutcome::Deferred) => work.finish(
            MonitorState::Waiting,
            "Publication deferred by writer contention",
        ),
        Ok(PublicationOutcome::InputsChanged | PublicationOutcome::Inactive) => work.finish(
            MonitorState::Cancelled,
            "Captured inputs or active source changed before publication",
        ),
        Ok(PublicationOutcome::Cancelled(reason)) => {
            work.finish(MonitorState::Cancelled, reason.label())
        }
        Err(error) => work.finish(MonitorState::Failed, &error.to_string()),
    }
}

/// Both envelopes name canonical source evidence as well as their annotation inputs.
fn embedding_request(plan: &CohortPlan, kind: ProjectionType) -> NewProjection {
    NewProjection {
        source_id: plan.source_id.clone(),
        parse_id: plan.parse_id.clone(),
        projection_type: kind,
        input_unit_ids: Some(vec![plan.target.unit_id.clone()]),
        input_annotation_ids: Some(
            plan.inputs
                .iter()
                .map(|input| input.annotation_id.clone())
                .collect(),
        ),
        producer: annotation::producer(plan),
        index_name: Some(annotation::INDEX_NAME.to_owned()),
        index_partition: Some(plan.cohort_id.clone()),
    }
}

/// Keep a family's failure independent of its sibling and preserve a durable
/// failure event even when its builder transaction was rolled back.
fn apply_materialization_result(
    inputs: &WorkerInputs,
    cancellation: &AnnotationCancellation,
    source: &ActiveSource,
    document: &mut ProjectionDocumentProgress,
    family: Materialization,
    result: Result<PublicationOutcome, ApiError>,
) {
    let mut counts = ProjectionPublicationCounts::default();
    let issue_key = format!("projection:{}:{}", source.parse_id, family.name());
    if matches!(result, Ok(PublicationOutcome::Published)) {
        inputs.index_root.monitoring().record_completed(
            source.monitor_identity(),
            family.name(),
            "Materialization publication committed",
            "materializations published",
            1,
        );
    }
    match result {
        Ok(PublicationOutcome::Current | PublicationOutcome::Published) => {
            counts.published = 1;
            inputs.index_root.monitoring().clear_issue(&issue_key);
        }
        Ok(PublicationOutcome::Deferred | PublicationOutcome::InputsChanged) => counts.pending = 1,
        Ok(PublicationOutcome::Inactive) => {
            counts.pending = 1;
            document.activity = ProjectionActivity::Stopped;
            document.detail =
                Some("Source changed active parse before materialization.".to_owned());
        }
        Ok(PublicationOutcome::Cancelled(reason)) => {
            counts.pending = 1;
            document.activity = ProjectionActivity::Stopped;
            document.detail = Some(format!("Publication cancelled: {}", reason.label()));
        }
        Err(source_error) => {
            counts.failed = 1;
            inputs.index_root.monitoring().set_issue(
                issue_key,
                MonitorIssue {
                    observed_since: None,
                    elapsed_ms: 0,
                    identity: source.monitor_identity(),
                    stage: family.name().to_owned(),
                    state: MonitorState::Failed,
                    message: source_error.to_string(),
                    affected: 1,
                    retry_in_ms: None,
                    attempt: None,
                    retry_limit: None,
                },
            );
            document.detail = Some(truncate_persisted_detail(
                &source_error.to_string(),
                &inputs.index_root.limits().diagnostics,
            ));
            error!(event = "projection_worker.materialization_failed", source_id = %source.source_id,
                parse_id = %source.parse_id, projection_type = family.name(), error = %source_error,
                "annotation materialization failed; independent publications continue");
            record_failure(
                inputs,
                cancellation,
                &NewProjection {
                    source_id: source.source_id.clone(),
                    parse_id: source.parse_id.clone(),
                    projection_type: family.projection_type(),
                    input_unit_ids: None,
                    input_annotation_ids: None,
                    producer: failure_producer(),
                    index_name: None,
                    index_partition: None,
                },
                &source_error,
            );
        }
    }
    match family {
        Materialization::Graph => document.graph = counts,
        Materialization::Summary => document.summary = counts,
    }
    if document.activity != ProjectionActivity::Stopped {
        document.activity = document_activity(document);
    }
}

/// Preserve failed publication evidence outside its rolled-back transaction.
/// Audit errors are logged separately and never replace the original model/SQL error.
fn record_failure(
    inputs: &WorkerInputs,
    cancellation: &AnnotationCancellation,
    request: &NewProjection,
    original: &ApiError,
) {
    let result = (|| -> Result<bool, ApiError> {
        if cancelled(inputs, cancellation).is_some() {
            return Ok(false);
        }
        let mut connection = hot_plane::open_write(&inputs.index_root)?;
        let tx =
            hot_plane::begin_write_transaction(&mut connection, TX_NAMESPACE, "failure_audit")?;
        if let Some(reason) = cancelled(inputs, cancellation) {
            rollback_cancelled(tx, "failure_audit", reason)?;
            return Ok(false);
        }
        let body = (|| {
            if !is_active(&tx, &request.source_id, &request.parse_id)? {
                return Ok(false);
            }
            let id = envelope::insert_building(&tx, request)?;
            envelope::mark_failed(
                &tx,
                &id,
                &truncate_persisted_detail(
                    &original.to_string(),
                    &inputs.index_root.limits().diagnostics,
                ),
            )?;
            Ok(true)
        })();
        let recorded = match body {
            Ok(recorded) => recorded,
            Err(error) => {
                return Err(hot_plane::abort_transaction(
                    tx,
                    TX_NAMESPACE,
                    "failure_audit",
                    error,
                ));
            }
        };
        if let Some(reason) = cancelled(inputs, cancellation) {
            rollback_cancelled(tx, "failure_audit", reason)?;
            return Ok(false);
        }
        hot_plane::commit_transaction(tx, TX_NAMESPACE, "failure_audit")?;
        Ok(recorded)
    })();
    match result {
        Ok(true) => info!(event = "projection_worker.failure_audit_committed",
            source_id = %request.source_id, parse_id = %request.parse_id, committed = true,
            "projection.failed audit committed independently"),
        Ok(false) => info!(event = "projection_worker.failure_audit_skipped",
            source_id = %request.source_id, parse_id = %request.parse_id,
            "projection failure audit skipped after cancellation or source cutover; original error remains logged"),
        Err(error) => error!(event = "projection_worker.failure_audit_failed",
            source_id = %request.source_id, parse_id = %request.parse_id, error = %error,
            "projection failure audit failed; original error remains logged"),
    }
}

/// Materialization failures belong to integration wiring, not to annotation generation.
fn failure_producer() -> Provenance {
    Provenance {
        producer_type: ProducerType::System,
        producer_name: "fabric-projection-worker".to_owned(),
        producer_version: Some("1".to_owned()),
        config_hash: None,
        model_name: None,
        model_version: None,
        prompt_hash: None,
        temperature: None,
        confidence: None,
        memoized: None,
        memoized_from: None,
        memoization_key_hash: None,
        input_refs: None,
    }
}

/// Rebuild and shutdown share cancellation; the shutdown latch also covers a
/// maintenance-owner failure before its watch notification could be published.
fn cancelled(
    inputs: &WorkerInputs,
    cancellation: &AnnotationCancellation,
) -> Option<AnnotationCancelReason> {
    cancellation.reason().or_else(|| {
        inputs
            .shutdown
            .wait_timeout(Duration::ZERO)
            .then_some(AnnotationCancelReason::Shutdown)
    })
}

/// An explicit rollback distinguishes confirmed cancellation cleanup from an
/// unconfirmed rollback error and never turns cancellation into retry debt.
fn rollback_cancelled(
    tx: Transaction<'_>,
    operation: &str,
    reason: AnnotationCancelReason,
) -> Result<(), ApiError> {
    tx.rollback().map_err(|source| {
        error!(event = "projection_worker.cancellation_rollback_failed", operation,
            reason = reason.label(), error = %source, durable_outcome = "unconfirmed",
            "cancelled publication transaction rollback failed");
        failure(format!(
            "rollback cancelled {operation} publication: {source}"
        ))
    })?;
    info!(
        event = "projection_worker.transaction_cancelled",
        operation,
        reason = reason.label(),
        durable_outcome = "rolled_back",
        "cancelled publication transaction rolled back"
    );
    Ok(())
}

/// A new active parse is unmeasured until its owning worker completes discovery.
fn initial_document(source: &ActiveSource) -> ProjectionDocumentProgress {
    ProjectionDocumentProgress {
        source_id: source.source_id.clone(),
        parse_id: source.parse_id.clone(),
        source_paths: source.source_paths.clone(),
        measured_at: None,
        graph: ProjectionPublicationCounts {
            pending: 1,
            ..Default::default()
        },
        summary: ProjectionPublicationCounts {
            pending: 1,
            ..Default::default()
        },
        embeddings: None,
        activity: ProjectionActivity::Discovering,
        detail: None,
    }
}

/// Replace inventory by captured source/parse identity while retaining older
/// measurements for still-active documents until their new pass completes.
fn publish_inventory(
    inputs: &WorkerInputs,
    sources: &[ActiveSource],
) -> Result<Vec<ProjectionDocumentProgress>, ApiError> {
    let now = utc_now()?;
    let mut monitored_documents = Vec::new();
    update_health(&inputs.health, |health| {
        let previous: HashMap<_, _> = health
            .documents
            .take()
            .unwrap_or_default()
            .into_iter()
            .map(|document| {
                (
                    (document.source_id.clone(), document.parse_id.clone()),
                    document,
                )
            })
            .collect();
        health.documents = Some(
            sources
                .iter()
                .map(|source| {
                    previous
                        .get(&(source.source_id.clone(), source.parse_id.clone()))
                        .cloned()
                        .unwrap_or_else(|| initial_document(source))
                })
                .collect(),
        );
        health.measured_at = Some(now);
        monitored_documents = health.documents.clone().unwrap_or_default();
    });
    // Publish outside the health lock so monitoring never creates nested lock ownership.
    inputs
        .index_root
        .monitoring()
        .replace_publications(&monitored_documents);
    Ok(monitored_documents)
}

/// Classification follows measured publication work; missing embedding inventory
/// remains discovering instead of being reported as fully published.
fn document_activity(document: &ProjectionDocumentProgress) -> ProjectionActivity {
    if document.graph.failed
        + document.summary.failed
        + document.embeddings.map_or(0, |counts| counts.failed)
        > 0
    {
        ProjectionActivity::RetryWait
    } else if document.graph.pending
        + document.summary.pending
        + document.embeddings.map_or(0, |counts| counts.pending)
        > 0
    {
        ProjectionActivity::Pending
    } else if document.embeddings.is_none() {
        ProjectionActivity::Discovering
    } else {
        ProjectionActivity::Complete
    }
}

/// Publish one source's observation atomically; clocks and compact progress facts
/// belong to the owning worker, while health handlers only copy the last snapshot.
fn publish_document(
    inputs: &WorkerInputs,
    document: &mut ProjectionDocumentProgress,
) -> Result<(), ApiError> {
    document.measured_at = Some(utc_now()?);
    inputs
        .index_root
        .monitoring()
        .publication_document(document);
    update_health(&inputs.health, |health| {
        if let Some(current) = health.documents.as_mut().and_then(|documents| {
            documents.iter_mut().find(|current| {
                current.source_id == document.source_id && current.parse_id == document.parse_id
            })
        }) {
            *current = document.clone();
        }
        health.activity = document.activity;
        health.measured_at = document.measured_at.clone();
    });
    debug!(event = "projection_worker.document_progress", source_id = %document.source_id,
        parse_id = %document.parse_id, activity = %document.activity,
        graph_published = document.graph.published, graph_pending = document.graph.pending, graph_failed = document.graph.failed,
        summary_published = document.summary.published, summary_pending = document.summary.pending, summary_failed = document.summary.failed,
        embedding_published = document.embeddings.map(|counts| counts.published),
        embedding_pending = document.embeddings.map(|counts| counts.pending),
        embedding_failed = document.embeddings.map(|counts| counts.failed),
        measured_at = document.measured_at.as_deref(), "annotation projection coverage measured");
    Ok(())
}

/// Change lifecycle state without recasting older per-document measurements as current.
fn worker_activity(
    slot: &Mutex<ProjectionHealth>,
    activity: ProjectionActivity,
    detail: Option<String>,
    diagnostics: &DiagnosticLimits,
) -> Result<(), ApiError> {
    let now = utc_now()?;
    update_health(slot, |health| {
        health.activity = activity;
        health.detail = detail.map(|detail| truncate_persisted_detail(&detail, diagnostics));
        health.measured_at = Some(now);
    });
    Ok(())
}

/// A parked or stopped publisher must never leave an in-flight activity on display.
fn stop_activity(slot: &Mutex<ProjectionHealth>, reason: &str) -> Result<(), ApiError> {
    let now = utc_now()?;
    update_health(slot, |health| {
        health.activity = ProjectionActivity::Stopped;
        health.detail = Some(format!("Projection work stopped: {reason}"));
        health.measured_at = Some(now.clone());
        for document in health.documents.iter_mut().flatten() {
            if matches!(
                document.activity,
                ProjectionActivity::Discovering
                    | ProjectionActivity::Building
                    | ProjectionActivity::AwaitingCommit
                    | ProjectionActivity::Pending
            ) {
                document.activity = ProjectionActivity::Stopped;
                document.detail = health.detail.clone();
            }
        }
    });
    Ok(())
}

/// Publish an unavailable lifecycle even if timestamp acquisition also fails.
fn unavailable(slot: &Mutex<ProjectionHealth>, detail: &str, diagnostics: &DiagnosticLimits) {
    let now = match utc_now() {
        Ok(now) => Some(now),
        Err(source) => {
            error!(event = "projection_worker.health_time_failed", error = %source,
                "projection worker failure could not be timestamped");
            None
        }
    };
    update_health(slot, |health| {
        health.activity = ProjectionActivity::Unavailable;
        health.detail = Some(truncate_persisted_detail(detail, diagnostics));
        health.measured_at = now;
        for document in health.documents.iter_mut().flatten() {
            if matches!(
                document.activity,
                ProjectionActivity::Building
                    | ProjectionActivity::AwaitingCommit
                    | ProjectionActivity::Discovering
            ) {
                document.activity = ProjectionActivity::Unavailable;
                document.detail = health.detail.clone();
            }
        }
    });
}

/// Health mutations are bounded snapshot operations; poison recovery is observable
/// and never keeps the data-plane worker wedged on a previous reporting panic.
fn update_health(slot: &Mutex<ProjectionHealth>, update: impl FnOnce(&mut ProjectionHealth)) {
    let mut health = match slot.lock() {
        Ok(health) => health,
        Err(poisoned) => {
            error!(
                event = "projection_worker.health_slot_poisoned",
                "projection health slot poisoned; recovering the last observation"
            );
            poisoned.into_inner()
        }
    };
    update(&mut health);
}

/// Preserve concrete operation context through the service's storage error path.
fn failure(message: impl Into<String>) -> ApiError {
    ApiError::StorageOperation {
        message: message.into(),
    }
}
