//! CA2-P5 annotation dry-run mode driver (ruling 9).
//!
//! `data-store-service --annotation-dry-run <groups-per-source>` is a deliberate
//! one-shot operator mode for the ruleset-authoring loop: run acquisition + parse
//! across the corpus, sample-annotate the first N section groups per source per
//! producer type, and leave a state the NEXT normal start adopts and completes.
//!
//! This module owns the mode's DRIVER — the sampling + summary logic that runs
//! ABOVE the scheduler pass. Responsibilities:
//!
//!   1. Run the scheduler's one-pass scan/parse (`scheduler::run_annotation_dry_run_pass`),
//!      which leaves every ready parse un-held for §13.5 GateExisting adoption
//!      (mechanic 1) and every queue row `in_flight` for normal-cycle reclamation.
//!   2. For each source that now has a READY parse (from the pass outcomes),
//!      build the invocation plan and enumerate the ENTITY and RELATION work items
//!      only — summary is deliberately EXCLUDED, because sampling exists to surface
//!      naming/predicate vocabulary (ruling 9). Take the FIRST N section groups per
//!      source per type (N = the CLI's groups-per-source), deterministic by the
//!      plan's unit order, and run each through the NORMAL annotation machinery:
//!      memo lookup → memo re-mint OR producer call → store writes + memo record.
//!   3. Log a bounded per-source and total summary.
//!
//! SCOPE NOTE — these parses are NOT active. Annotations are parse-scoped: the
//! store's insert path keys on `parse_id`/`source_id` from the request
//! (`store::insert_building`/`insert_fresh`/`complete_fresh`), NOT on active
//! status. The active-parse gate is read-side only (`store::fresh_for_active_parse`,
//! and the vocabulary route's `scope=all`), so writing annotations against a
//! never-activated ready parse is exactly what the schema supports — and the
//! vocabulary route's `scope=all` is what reads them back for inspection.
//!
//! FIDELITY — the per-item build sequence mirrors `annotations::worker`'s
//! `build_source`/`prepare_work_item`/`complete_build` for a SINGLE item: the
//! CA2-P1 two-key stamping (memo key + content key on `NewAnnotation`), the
//! single-goal producer contracts (through the identity hash / memo key), and the
//! empty-result `[]` marker convention (a producer that found nothing still
//! completes its building row with an empty JSON-array body so the key is
//! satisfied and not rebuilt). Execution is SERIAL (no waves — sampling is small);
//! cancellation interrupts HTTP sampling and prevents unfinished output committing.
//! The worker's dedicated discovery loop is NOT reused (its
//! item enumeration is private); this driver mirrors the call sequence directly.
//!
//! Sampling requires published context windows (the section-dense artifact);
//! the dry-run pass does not build them yet, so its plans are empty until it does.

use std::path::PathBuf;

use tracing::{info, warn};

use crate::annotations::llm_client::{self, AnnotatorClient};
use crate::annotations::memo::{self, MemoItem};
use crate::annotations::producer::{
    self, Invocation, InvocationTarget, ProducedAnnotation, ProducerKind,
};
use crate::annotations::store::{self, NewAnnotation, ReopenableRow, ReopenableStatus};
use crate::artifact_store::ArtifactStore;
use crate::config::AnnotatorModelConfig;
use crate::error::ApiError;
use crate::hot_plane;
use crate::maintenance::AnnotationCancellation;
use crate::model::Provenance;
use crate::scheduler::{self, DryRunReadyParse};
use crate::state::ShutdownSignal;

/// Log-event namespace for this driver's hot-plane transaction boundaries.
const TX_LOG_NAMESPACE: &str = "dry_run";

/// The producer kinds the dry run samples, in a FIXED order (ruling 9). Summary
/// is deliberately excluded: sampling exists to surface entity naming and
/// relation predicate vocabulary, and the summary producer emits neither.
const SAMPLED_KINDS: [ProducerKind; 2] = [ProducerKind::Entity, ProducerKind::Relation];

/// Everything the mode driver needs. Grouped so the entry `run` keeps a short
/// signature; each field is threaded straight from the main-loop config wiring.
/// Deliberately carries NO `ProjectionRuntime`,
/// `CutoverRegistry`, or `ApplicationIdentity`: the dry-run pass truncates
/// before every gate-continuation step that consumes them, and a
/// `ProjectionRuntime` is constructible only from an initialized
/// `InferenceRuntime` — which this mode never initializes by design.
pub(crate) struct DryRunInputs {
    pub(crate) corpus_root: PathBuf,
    pub(crate) index_root: crate::runtime::StorageContext,
    pub(crate) governance_domain: String,
    /// The annotator model config (endpoint, model, max_completion_tokens, …).
    pub(crate) annotator: AnnotatorModelConfig,
    /// Config-file parent directory, for resolving the annotator's api-key file.
    pub(crate) config_root: PathBuf,
    /// Configured dense model dimension, needed to load the section-dense
    /// artifact the planner reads context windows from.
    pub(crate) dense_dimension: usize,
    /// Excerpts per source per type to sample (the CLI's argument).
    pub(crate) groups_per_source: usize,
}

/// Per-source sampling accounting, folded into the total for the summary log.
#[derive(Debug, Clone, Copy, Default)]
struct SampleCounts {
    /// Section groups sampled (attempted) for this source, across both types.
    groups_sampled: u64,
    /// Producer invocations attempted, including cancellation before HTTP dispatch.
    /// HTTP lifecycle logs determine which attempts actually reached the network.
    producer_calls: u64,
    /// Memo hits re-minted without a model call.
    memo_hits: u64,
    /// Items already satisfied (a fresh row existed for the content key) — skipped.
    already_satisfied: u64,
    /// Items whose producer call failed (parked failed, retried by a later run).
    failures: u64,
}

impl SampleCounts {
    fn add(&mut self, other: &SampleCounts) {
        self.groups_sampled += other.groups_sampled;
        self.producer_calls += other.producer_calls;
        self.memo_hits += other.memo_hits;
        self.already_satisfied += other.already_satisfied;
        self.failures += other.failures;
    }
}

/// Run the annotation dry-run mode's scan/parse pass and sampling. Returns `Ok`
/// once sampling completes (or shutdown ended it early); `Err` is a cycle-wide
/// canonical-side fault from the pass, which the caller surfaces as a fatal.
///
/// The caller (main.rs) is responsible for the surrounding mode lifecycle: bind,
/// serve the reduced router (`http::build_dry_run_router`), invoke this, log the
/// mode-ready-for-inspection line, and serve until shutdown. This function owns
/// only the pass + sampling; it does NOT bind or serve.
pub(crate) fn run(
    inputs: DryRunInputs,
    shutdown: &ShutdownSignal,
    cancellation: AnnotationCancellation,
) -> Result<(), ApiError> {
    // Driver-scoped start boundary. main.rs emits `dry_run.mode_started` at the
    // outer mode boundary (bind/serve lifecycle); this distinct name marks the
    // inner pass+sampling driver so the two boundaries are distinguishable.
    info!(
        event = "dry_run.driver_started",
        corpus_root = %inputs.corpus_root.display(),
        groups_per_source = inputs.groups_per_source,
        "annotation dry-run mode driver started (pass + sampling)"
    );

    // Phase 1: the scheduler pass (scan → acquire → parse, chain truncated at the
    // importer's ready boundary; queue rows left in_flight, ready parses un-held).
    let pass = scheduler::run_annotation_dry_run_pass(
        inputs.corpus_root.clone(),
        inputs.index_root.clone(),
        inputs.governance_domain,
    )?;
    info!(
        event = "dry_run.pass_boundary",
        boundary = "pass_complete",
        sources_parsed = pass.ready.len(),
        claimed = pass.claimed,
        parsed_ready = pass.parsed_ready,
        skipped = pass.skipped,
        parse_failed = pass.parse_failed,
        faulted = pass.faulted,
        "annotation dry-run pass complete; beginning sampling"
    );

    // The annotator client loads once here (not inside the pass): a bad key file
    // or an unbuildable client fails the mode loudly, because sampling IS the
    // mode's purpose (unlike the normal worker, where annotations are
    // non-critical and a client load failure only parks the worker).
    let client = AnnotatorClient::load(
        &inputs.annotator,
        &inputs.config_root,
        inputs.index_root.limits().diagnostics,
        cancellation,
    )?;

    // Phase 2: sample the first N excerpts per source per type. Serial —
    // sampling is small (bounded by groups_per_source), so no wave machinery.
    let mut total = SampleCounts::default();
    let mut sources_sampled: u64 = 0;
    for ready in &pass.ready {
        if client.cancellation().reason().is_some()
            || shutdown.wait_timeout(std::time::Duration::ZERO)
        {
            // A shutdown request ends sampling promptly. Already-sampled sources
            // are durable; the rest are covered by the next normal start (their
            // parses are ready and un-held). Deliberately not counted as failure.
            info!(
                event = "dry_run.sampling_cancelled",
                reason = client
                    .cancellation()
                    .reason()
                    .map(|reason| reason.label())
                    .unwrap_or("shutdown"),
                sources_sampled,
                "cancellation requested during sampling; ending dry-run sampling early"
            );
            break;
        }
        let counts = sample_source(
            &inputs.index_root,
            &inputs.annotator,
            inputs.dense_dimension,
            &client,
            ready,
            inputs.groups_per_source,
            shutdown,
        )?;
        info!(
            event = "dry_run.source_sampled",
            source_id = %ready.source_id,
            parse_id = %ready.parse_run_id,
            groups_sampled = counts.groups_sampled,
            producer_calls = counts.producer_calls,
            producer_calls_scope = "invocations_attempted",
            memo_hits = counts.memo_hits,
            already_satisfied = counts.already_satisfied,
            failures = counts.failures,
            "annotation dry-run source sample summary"
        );
        total.add(&counts);
        sources_sampled += 1;
    }

    info!(
        event = "dry_run.sampling_complete",
        sources_parsed = pass.ready.len(),
        sources_sampled,
        groups_sampled = total.groups_sampled,
        producer_calls = total.producer_calls,
        producer_calls_scope = "invocations_attempted",
        memo_hits = total.memo_hits,
        already_satisfied = total.already_satisfied,
        failures = total.failures,
        "annotation dry-run sampling complete"
    );
    Ok(())
}

/// Sample one source's ready parse: build the invocation plan, take the first N
/// excerpt invocations per sampled kind (Entity, Relation) in plan order, and
/// build each through the normal machinery. Satisfaction/reopen classify
/// against the parse's existing annotation rows (content-scoped, CA2), exactly as
/// the worker does, so a re-run of the mode does not duplicate work already done.
fn sample_source(
    index_root: &crate::runtime::StorageContext,
    config: &AnnotatorModelConfig,
    dense_dimension: usize,
    client: &AnnotatorClient,
    ready: &DryRunReadyParse,
    groups_per_source: usize,
    shutdown: &ShutdownSignal,
) -> Result<SampleCounts, ApiError> {
    let mut counts = SampleCounts::default();

    // Build the plan and the existing-annotation classification maps on ONE read
    // connection, dropped before any producer call or write (worker discipline).
    let (plan, present_keys, reopenable) = {
        let store = ArtifactStore::open_existing(index_root)?;
        let connection = hot_plane::open_read(index_root)?;
        let plan = producer::build_invocation_plan(
            &connection,
            &store,
            &ready.parse_run_id,
            dense_dimension,
            &index_root.limits().indexing,
        )?;
        let present_keys =
            store::fresh_content_key_hashes_for_parse(&connection, &ready.parse_run_id)?;
        let reopenable = store::reopenable_rows_for_parse(&connection, &ready.parse_run_id)?;
        (plan, present_keys, reopenable)
    };

    // For each sampled kind, take the FIRST `groups_per_source` excerpt
    // invocations in plan order (deterministic: `build_invocation_plan` emits
    // excerpts in context-window order).
    for kind in SAMPLED_KINDS {
        let sampled_groups: Vec<&Invocation> = plan
            .iter()
            .filter(|invocation| producer::invocation_matches_kind(kind, invocation))
            .take(groups_per_source)
            .collect();

        for invocation in sampled_groups {
            if client.cancellation().reason().is_some()
                || shutdown.wait_timeout(std::time::Duration::ZERO)
            {
                // No NEW paid producer call may start after a shutdown request;
                // return what is done so far (already-built rows are durable).
                info!(
                    event = "dry_run.sampling_cancelled",
                    reason = client.cancellation().reason().map(|reason| reason.label()).unwrap_or("shutdown"),
                    source_id = %ready.source_id,
                    parse_id = %ready.parse_run_id,
                    "cancellation requested mid-source; ending this source's sampling"
                );
                return Ok(counts);
            }
            counts.groups_sampled += 1;
            sample_item(
                index_root,
                config,
                client,
                ready,
                kind,
                invocation,
                &present_keys,
                &reopenable,
                &mut counts,
            )?;
        }
    }

    Ok(counts)
}

/// Build one sampled work item through the normal machinery (mirrors the worker's
/// per-item PRE-PAID + POST-PAID sequence, serial and without wave buffering):
///   - compute the memo key + content key (CA2-P1 two-key derivation);
///   - classify against the parse's existing rows (content-scoped satisfaction);
///   - memo HIT → re-mint the cached output with no model call;
///   - memo MISS → insert the building row, run the producer, complete it (or
///     park it failed on a producer error), and record the memo entry.
///
/// A producer error parks the row failed and counts a failure, never aborting the
/// pass — a later run (this mode or the normal worker) retries the failed row.
#[allow(clippy::too_many_arguments)]
fn sample_item(
    index_root: &crate::runtime::StorageContext,
    config: &AnnotatorModelConfig,
    client: &AnnotatorClient,
    ready: &DryRunReadyParse,
    kind: ProducerKind,
    invocation: &Invocation,
    present_keys: &std::collections::HashSet<String>,
    reopenable: &std::collections::HashMap<String, ReopenableRow>,
    counts: &mut SampleCounts,
) -> Result<(), ApiError> {
    // Two-key derivation (CA2-P1): the memo key folds producer identity (cache
    // scope — includes the single-goal prompt contracts); the content key does not
    // (satisfaction scope). Both read on a short-lived connection.
    let (memo_key, content_key) = {
        let connection = hot_plane::open_read(index_root)?;
        let memo_key = memo::memoization_key_hash(&connection, kind, config, invocation)?;
        let content_key = memo::content_key_hash(&connection, kind, invocation)?;
        (memo_key, content_key)
    };

    // Satisfaction/reopen classification on the CONTENT key (CA2): a content key
    // already fresh under ANY producer identity is satisfied. A reopenable row is
    // a prior failed row or a crash-orphaned building row for this content key.
    let reopened = reopenable.get(&content_key);
    let is_unsatisfied = reopened.is_some() || !present_keys.contains(&content_key);
    if !is_unsatisfied {
        counts.already_satisfied += 1;
        return Ok(());
    }

    // The building row opens over the whole excerpt; completed items narrow to
    // their attributed fragments (`attributed_request`).
    let request = new_annotation_request(
        config,
        &index_root.limits().indexing,
        ready,
        kind,
        &invocation.targets,
        &memo_key,
        &content_key,
    )?;

    // Memo lookup on a read connection dropped before any write (worker discipline).
    let cached = {
        let connection = hot_plane::open_read(index_root)?;
        memo::lookup(&connection, &memo_key)?
    };

    let cancellation = client.cancellation();
    if sampling_cancelled(cancellation, "before_persistence") {
        return Ok(());
    }
    if let Some(entry) = cached {
        // MEMO HIT: re-mint the cached invocation output as annotation rows in ONE
        // transaction, with NO model call. Mirrors `worker::remint_from_memo`.
        if !remint_from_memo(
            index_root,
            config,
            ready,
            kind,
            invocation,
            &request,
            reopened,
            &entry,
            &memo_key,
            cancellation,
        )? {
            return Ok(());
        }
        counts.memo_hits += 1;
        info!(
            event = "dry_run.memo_hit",
            source_id = %ready.source_id,
            parse_id = %ready.parse_run_id,
            producer = kind.producer_name(),
            reminted = entry.items.len(),
            "dry-run memo hit: cached producer output re-minted without a model call"
        );
        return Ok(());
    }

    // MEMO MISS: insert the visible building row (or reopen the chosen row) in a
    // committed transaction FIRST (durable in-flight truth before any model call),
    // then run the producer, then complete-or-fail. Serial single-thread, so a
    // plain committed begin is used (no writer contention to wait out).
    let building_id = {
        let mut connection = hot_plane::open_write(index_root)?;
        let tx =
            hot_plane::begin_write_transaction(&mut connection, TX_LOG_NAMESPACE, "build_open")?;
        let result = reopen_or_insert_building(&tx, reopened, &request);
        match result {
            Ok(id) => {
                if !commit_unless_cancelled(tx, "build_open", cancellation)? {
                    return Ok(());
                }
                id
            }
            Err(source) => {
                return Err(hot_plane::abort_transaction(
                    tx,
                    TX_LOG_NAMESPACE,
                    "build_open",
                    source,
                ));
            }
        }
    };

    // Producer call (the only external dependency in this mode). A failure parks
    // the row failed and counts a failure; it is NOT fatal. Dry-run sampling
    // has no retry ladder, so every call runs at the base temperature.
    let context = invocation.log_context();
    context.record("trigger", "dry_run");
    context.record("source_id", ready.source_id.as_str());
    context.record("parse_id", ready.parse_run_id.as_str());
    context.record("annotation_id", building_id.as_str());
    let _entered = context.enter();
    if sampling_cancelled(cancellation, "before_producer") {
        return Ok(());
    }
    counts.producer_calls += 1;
    // Sampling does not measure the normal document plan; its progress remains
    // explicitly unavailable rather than treating this sample as the whole plan.
    let result = producer::invoke(
        kind,
        client,
        invocation,
        llm_client::PRODUCER_TEMPERATURE,
        None,
    );
    // Cancellation can arrive after the response or during validation. Its output
    // and provider errors must not be persisted as fresh annotations or failures,
    // but an already-observed error remains part of the diagnostic record.
    if sampling_cancelled(cancellation, "after_producer") {
        if let Err(source) = &result
            && source.error().is_some()
        {
            warn!(
                event = "dry_run.build_error_discarded",
                source_id = %ready.source_id,
                parse_id = %ready.parse_run_id,
                producer = kind.producer_name(),
                annotation_id = %building_id,
                failure_class = source.class(),
                error = %source,
                "observed producer error discarded after cancellation; no failure persisted"
            );
        }
        return Ok(());
    }
    match result {
        Ok(produced) => {
            if !complete_build(
                index_root,
                config,
                ready,
                kind,
                invocation,
                &request,
                &building_id,
                &produced,
                &memo_key,
                cancellation,
            )? {
                return Ok(());
            }
            info!(
                event = "dry_run.build_completed",
                source_id = %ready.source_id,
                parse_id = %ready.parse_run_id,
                producer = kind.producer_name(),
                produced = produced.len(),
                annotation_state = "fresh_committed",
                "dry-run validated annotation output committed"
            );
        }
        Err(producer::InvocationFailure::Cancelled(reason)) => {
            info!(
                event = "dry_run.build_cancelled",
                reason = reason.label(),
                annotation_id = %building_id,
                "dry-run annotation cancelled; building row left for recovery"
            );
        }
        Err(source) => {
            let Some(error) = source.error() else {
                return Ok(());
            };
            // Observe the producer boundary before SQL so a rollback or persistence
            // error cannot obscure the failure that led to this write attempt.
            warn!(
                event = "dry_run.producer_failure_observed",
                failure_class = source.class(),
                error = %source,
                producer = kind.producer_name(),
                annotation_state = "failure_not_yet_persisted",
                "dry-run producer failed; recording its result is pending"
            );
            if !fail_build(index_root, &building_id, error, cancellation)? {
                // The transaction may observe cancellation after the earlier probe;
                // retain the known error under this invocation's source/build span.
                warn!(
                    event = "dry_run.build_error_discarded",
                    failure_class = source.class(),
                    error = %source,
                    producer = kind.producer_name(),
                    "observed producer error discarded during cancellation-aware persistence"
                );
                return Ok(());
            }
            counts.failures += 1;
            warn!(
                event = "dry_run.build_failed",
                source_id = %ready.source_id,
                parse_id = %ready.parse_run_id,
                producer = kind.producer_name(),
                annotation_id = %building_id,
                failure_class = source.class(),
                error = %source,
                "dry-run producer invocation failed; annotation parked failed"
            );
        }
    }
    Ok(())
}

/// Assemble the `NewAnnotation` request for one sampled item over `targets`: the
/// targets' unit ids (deduplicated in order) are exactly the resulting
/// annotation's `targetUnitIds`, the planned provenance's input refs are the same
/// targets, and BOTH keys are carried up front (CA2-P1 two-key stamping; prompt
/// contracts feed the provenance identity). Mirrors `worker::new_annotation_request`.
fn new_annotation_request(
    config: &AnnotatorModelConfig,
    indexing: &crate::limits::IndexingLimits,
    ready: &DryRunReadyParse,
    kind: ProducerKind,
    targets: &[InvocationTarget],
    memo_key: &str,
    content_key: &str,
) -> Result<NewAnnotation, ApiError> {
    let target_unit_ids = producer::target_unit_ids(targets);
    let provenance = producer::planned_provenance(kind, config, indexing, targets)?;
    Ok(NewAnnotation {
        source_id: ready.source_id.clone(),
        parse_id: ready.parse_run_id.clone(),
        target_unit_ids,
        annotation_type: kind.annotation_type(),
        provenance,
        memoization_key_hash: memo_key.to_string(),
        content_key_hash: content_key.to_string(),
    })
}

/// The request for one produced item: its targets are the invocation fragments
/// `producer::attribute` keeps for the body, so the row's `targetUnitIds` and
/// provenance input refs name only supporting fragments. Both keys are copied
/// from the invocation-wide request. Mirrors `worker::attributed_request`.
fn attributed_request(
    config: &AnnotatorModelConfig,
    indexing: &crate::limits::IndexingLimits,
    ready: &DryRunReadyParse,
    kind: ProducerKind,
    invocation: &Invocation,
    request: &NewAnnotation,
    body: &serde_json::Value,
) -> Result<NewAnnotation, ApiError> {
    let targets = producer::attribute(kind, body, &invocation.targets);
    new_annotation_request(
        config,
        indexing,
        ready,
        kind,
        &targets,
        &request.memoization_key_hash,
        &request.content_key_hash,
    )
}

/// Produce the build's visible `building` row inside the caller's transaction:
/// reuse the reopened row (flipping a `failed` row back to building via the
/// guarded transition, adopting a crash-orphaned `building` row as-is) or insert
/// a new one. Mirrors `worker::reopen_or_insert_building`.
fn reopen_or_insert_building(
    tx: &crate::sqlite::Transaction<'_>,
    reopened: Option<&ReopenableRow>,
    request: &NewAnnotation,
) -> Result<String, ApiError> {
    match reopened {
        Some(row) => {
            if row.status == ReopenableStatus::Failed {
                store::retry_failed(tx, &row.annotation_id)?;
            }
            Ok(row.annotation_id.clone())
        }
        None => store::insert_building(tx, request),
    }
}

/// MEMO HIT re-mint: complete the building row to cached item 1 and insert items
/// 2..N directly fresh, all in ONE transaction, each stamped `memoized: true`
/// with its per-item `memoizedFrom` (§21.3 honesty). Mirrors
/// `worker::remint_from_memo` (minus the writer-contention defer — the dry run is
/// single-threaded, so a plain committed begin is correct).
/// Each re-minted item is attributed against THIS invocation's targets, since the
/// cached output may have been produced at another location of the same text.
/// Returns false when cancellation discards the re-mint without a durable memo hit.
#[allow(clippy::too_many_arguments)]
fn remint_from_memo(
    index_root: &crate::runtime::StorageContext,
    config: &AnnotatorModelConfig,
    ready: &DryRunReadyParse,
    kind: ProducerKind,
    invocation: &Invocation,
    request: &NewAnnotation,
    reopened: Option<&ReopenableRow>,
    entry: &memo::MemoEntry,
    memo_key: &str,
    cancellation: &AnnotationCancellation,
) -> Result<bool, ApiError> {
    if sampling_cancelled(cancellation, "memo_remint") {
        return Ok(false);
    }
    let mut connection = hot_plane::open_write(index_root)?;
    let tx = hot_plane::begin_write_transaction(&mut connection, TX_LOG_NAMESPACE, "memo_remint")?;
    let body = (|| -> Result<(), ApiError> {
        let building_id = reopen_or_insert_building(&tx, reopened, request)?;
        let mut items = entry.items.iter();
        let Some(first) = items.next() else {
            // A cached entry always holds at least one item (`memo::record` refuses
            // an empty array); an empty entry is corruption.
            return Err(ApiError::StorageOperation {
                message: format!("memo entry for key {memo_key} is empty on re-mint"),
            });
        };
        let first_request = attributed_request(
            config,
            &index_root.limits().indexing,
            ready,
            kind,
            invocation,
            request,
            &first.body,
        )?;
        let first_provenance = memoized_provenance(&first_request.provenance, memo_key, first);
        // Re-stamp the memo key to THIS invocation's identity (CA2): a
        // content-scoped reopen may have adopted a row minted under a different
        // producer identity.
        store::complete_fresh(
            &tx,
            &building_id,
            &first.body,
            first.confidence,
            &first_provenance,
            memo_key,
            &first_request.target_unit_ids,
        )?;
        for extra in items {
            let extra_request = attributed_request(
                config,
                &index_root.limits().indexing,
                ready,
                kind,
                invocation,
                request,
                &extra.body,
            )?;
            let extra_provenance = memoized_provenance(&extra_request.provenance, memo_key, extra);
            store::insert_fresh(
                &tx,
                &extra_request,
                &extra.body,
                extra.confidence,
                &extra_provenance,
            )?;
        }
        Ok(())
    })();
    if let Err(source) = body {
        return Err(hot_plane::abort_transaction(
            tx,
            TX_LOG_NAMESPACE,
            "memo_remint",
            source,
        ));
    }
    commit_unless_cancelled(tx, "memo_remint", cancellation)
}

/// MEMO MISS completion: complete item 1 into the building row, insert items 2..N
/// directly fresh, and record the memo entry — all in ONE transaction, so the
/// cache row and the annotation rows it caches commit together. An EMPTY producer
/// result still completes the building row with an empty `[]` JSON-array body: the
/// BY-DESIGN "no annotations" marker for ALL producer kinds (the same marker the
/// annotation-derived projection builders skip), so the key is satisfied and not
/// rebuilt on a re-run. Each row's target units and provenance input refs are
/// the item's attributed fragments (`producer::attribute`), replacing the
/// whole-excerpt set the building row was opened with. Mirrors
/// `worker::complete_build`.
/// Returns false when cancellation discards output before its completion commits.
#[allow(clippy::too_many_arguments)]
fn complete_build(
    index_root: &crate::runtime::StorageContext,
    config: &AnnotatorModelConfig,
    ready: &DryRunReadyParse,
    kind: ProducerKind,
    invocation: &Invocation,
    request: &NewAnnotation,
    building_id: &str,
    produced: &[ProducedAnnotation],
    memo_key: &str,
    cancellation: &AnnotationCancellation,
) -> Result<bool, ApiError> {
    if sampling_cancelled(cancellation, "build_complete") {
        return Ok(false);
    }
    let mut connection = hot_plane::open_write(index_root)?;
    let tx =
        hot_plane::begin_write_transaction(&mut connection, TX_LOG_NAMESPACE, "build_complete")?;
    let body = (|| -> Result<(), ApiError> {
        let Some((first, rest)) = produced.split_first() else {
            // Empty producer result: complete with the empty `[]` marker body (see
            // the module banner and `worker::complete_build`'s three-consumer note).
            // Nothing to cache — no reusable items — so no memo row is written.
            let empty_body = serde_json::Value::Array(Vec::new());
            // An empty result has nothing to attribute; the marker row keeps
            // the whole excerpt as its coverage.
            let provenance = completed_provenance(&request.provenance, None);
            store::complete_fresh(
                &tx,
                building_id,
                &empty_body,
                None,
                &provenance,
                memo_key,
                &request.target_unit_ids,
            )?;
            return Ok(());
        };
        // Item 1 completes the building row; memo items collect every produced item
        // paired with the annotation id it was minted as (per-item memoizedFrom).
        let first_request = attributed_request(
            config,
            &index_root.limits().indexing,
            ready,
            kind,
            invocation,
            request,
            &first.body,
        )?;
        let first_provenance = completed_provenance(&first_request.provenance, first.confidence);
        store::complete_fresh(
            &tx,
            building_id,
            &first.body,
            first.confidence,
            &first_provenance,
            memo_key,
            &first_request.target_unit_ids,
        )?;
        let mut memo_items = vec![MemoItem {
            body: first.body.clone(),
            confidence: first.confidence,
            original_annotation_id: building_id.to_string(),
        }];
        for extra in rest {
            let extra_request = attributed_request(
                config,
                &index_root.limits().indexing,
                ready,
                kind,
                invocation,
                request,
                &extra.body,
            )?;
            let extra_provenance =
                completed_provenance(&extra_request.provenance, extra.confidence);
            let extra_id = store::insert_fresh(
                &tx,
                &extra_request,
                &extra.body,
                extra.confidence,
                &extra_provenance,
            )?;
            memo_items.push(MemoItem {
                body: extra.body.clone(),
                confidence: extra.confidence,
                original_annotation_id: extra_id,
            });
        }
        // Cache write atomic with the truth it caches (ruling D.2). The producer
        // identity uses the same single-goal prompt contracts as the memo key.
        let identity_hash = kind.identity_hash(config, &index_root.limits().indexing)?;
        memo::record(
            &tx,
            memo_key,
            kind.annotation_type(),
            &identity_hash,
            &memo_items,
        )?;
        Ok(())
    })();
    if let Err(source) = body {
        return Err(hot_plane::abort_transaction(
            tx,
            TX_LOG_NAMESPACE,
            "build_complete",
            source,
        ));
    }
    commit_unless_cancelled(tx, "build_complete", cancellation)
}

/// Park the building row failed with bounded detail in one transaction. The
/// failed row stays visible (§21 rule 3) and is retried by a later run. Mirrors
/// `worker::fail_build`.
/// Returns false when cancellation prevents the failure from being persisted.
fn fail_build(
    index_root: &crate::runtime::StorageContext,
    building_id: &str,
    producer_error: &ApiError,
    cancellation: &AnnotationCancellation,
) -> Result<bool, ApiError> {
    if sampling_cancelled(cancellation, "build_fail") {
        return Ok(false);
    }
    let detail = crate::util::truncate_persisted_detail(
        &producer_error.to_string(),
        &index_root.limits().diagnostics,
    );
    let mut connection = hot_plane::open_write(index_root)?;
    let tx = hot_plane::begin_write_transaction(&mut connection, TX_LOG_NAMESPACE, "build_fail")?;
    let result = store::mark_failed(&tx, building_id, &detail);
    if let Err(source) = result {
        return Err(hot_plane::abort_transaction(
            tx,
            TX_LOG_NAMESPACE,
            "build_fail",
            source,
        ));
    }
    commit_unless_cancelled(tx, "build_fail", cancellation)
}

/// Report discarded sampling work without counting cancellation as a provider failure.
fn sampling_cancelled(cancellation: &AnnotationCancellation, phase: &'static str) -> bool {
    let Some(reason) = cancellation.reason() else {
        return false;
    };
    info!(
        event = "dry_run.build_cancelled",
        reason = reason.label(),
        phase,
        "dry-run sampling cancelled; unfinished results discarded"
    );
    true
}

/// Only a committed transaction may contribute to sampling success counters.
/// Cancellation rolls back explicitly so the operator sees a confirmed outcome.
fn commit_unless_cancelled(
    tx: crate::sqlite::Transaction<'_>,
    phase: &'static str,
    cancellation: &AnnotationCancellation,
) -> Result<bool, ApiError> {
    if let Some(reason) = cancellation.reason() {
        let started = std::time::Instant::now();
        tx.rollback().map_err(|source| {
            warn!(
                event = "dry_run.cancellation_rollback_failed",
                reason = reason.label(),
                phase,
                error = %source,
                durable_outcome = "unconfirmed",
                "could not confirm rollback of cancelled annotation work"
            );
            ApiError::StorageOperation {
                message: format!("dry-run {phase} cancellation rollback failed: {source}"),
            }
        })?;
        info!(
            event = "dry_run.cancellation_rolled_back",
            reason = reason.label(),
            phase,
            elapsed_ms = started.elapsed().as_millis() as u64,
            durable_outcome = "rolled_back",
            "cancelled annotation transaction rolled back"
        );
        return Ok(false);
    }
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, phase)?;
    Ok(true)
}

/// Final provenance of a freshly-produced (model-run) annotation: the model ran,
/// so the memoization fields stay absent, the concrete confidence is filled in,
/// and the base sampling temperature is stamped (dry-run sampling has no retry
/// ladder, so every call runs at the base). Mirrors `worker::completed_provenance`.
fn completed_provenance(planned: &Provenance, confidence: Option<f64>) -> Provenance {
    let mut provenance = planned.clone();
    provenance.confidence = confidence;
    provenance.temperature = Some(llm_client::PRODUCER_TEMPERATURE);
    provenance
}

/// Final provenance of a MEMO-REUSED annotation: `memoized: true`, `memoizedFrom`
/// set to the cached item's original annotation id, and the memoization key
/// recorded, so an auditor sees the model did not run (§21.3). Mirrors
/// `worker::memoized_provenance`.
fn memoized_provenance(planned: &Provenance, memo_key: &str, memo_item: &MemoItem) -> Provenance {
    let mut provenance = planned.clone();
    provenance.confidence = memo_item.confidence;
    provenance.memoized = Some(true);
    provenance.memoized_from = Some(memo_item.original_annotation_id.clone());
    provenance.memoization_key_hash = Some(memo_key.to_string());
    provenance
}
