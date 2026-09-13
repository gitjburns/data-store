//! A single-screen view of server-owned measurements. Polling is blocking work
//! on one dedicated thread; terminal input never waits for a network response.

use std::{
    io::{self, IsTerminal, Read, Stdout},
    panic::{self, PanicHookInfo},
    sync::{Arc, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use crossterm::{
    cursor::{Hide, Show},
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui_core::{
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    terminal::{Frame, Terminal},
    text::{Line, Span},
};
use ratatui_crossterm::CrosstermBackend;
use ratatui_widgets::{block::Block, borders::Borders, gauge::Gauge, paragraph::Paragraph};

use super::{ClientContext, ClientRequestDiagnostic, service_error, url};
use crate::{
    monitoring_types::{
        CALL_HISTORY_LIMIT, MonitorSnapshot, MonitorState, PUBLICATION_WORKER, RATE_WINDOW_SECONDS,
        RECENT_EVENT_LIMIT, REFRESH_INTERVAL_MS, WorkIdentity, WorkObservation,
    },
    types::AnnotationProgressCount,
};

/// Bound a failed poll so quitting cannot wait on the ordinary admin request deadline.
const REQUEST_TIMEOUT_MS: u64 = 2_000;
const MONITOR_PATH: &str = "/v1/monitor";
const INPUT_INTERVAL: Duration = Duration::from_millis(25);
const REFRESH_INTERVAL: Duration = Duration::from_millis(REFRESH_INTERVAL_MS);
const REQUEST_TIMEOUT: Duration = Duration::from_millis(REQUEST_TIMEOUT_MS);
// A monitor response is compact metadata. Oversized responses are a visible
// transport failure, never a partially accepted corpus observation.
const MAX_SNAPSHOT_BYTES: u64 = 8 * 1024 * 1024;
const MAX_ERROR_BYTES: u64 = 64 * 1024;
// This fits eight grouped calls, three worker rows, three model totals, and
// all summary categories without treating hidden panels as a complete dashboard.
const MIN_TERMINAL_WIDTH: u16 = 120;
const MIN_TERMINAL_HEIGHT: u16 = 42;
const RUNNING: Color = Color::Rgb(86, 180, 233);
const WAITING: Color = Color::Rgb(230, 159, 0);
const PROBLEM: Color = Color::Rgb(204, 121, 167);
const NORMAL: Color = Color::Rgb(220, 225, 230);
const MUTED: Color = Color::Rgb(145, 154, 164);
const BORDER: Color = Color::Rgb(70, 80, 94);

type MonitorTerminal = Terminal<CrosstermBackend<Stdout>>;
type PanicHook = dyn Fn(&PanicHookInfo<'_>) + Send + Sync + 'static;

/// Own terminal restoration even when initialization, rendering, or polling fails.
struct TerminalSession {
    previous_hook: Arc<PanicHook>,
    restored: bool,
}

impl TerminalSession {
    /// Install restoration before the first terminal mutation, including partial setup.
    fn enter() -> Result<(Self, MonitorTerminal)> {
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            bail!("--monitor requires an interactive terminal on stdin and stdout");
        }
        let previous_hook: Arc<PanicHook> = Arc::from(panic::take_hook());
        let panic_previous = Arc::clone(&previous_hook);
        panic::set_hook(Box::new(move |info| {
            if let Err(error) = restore_terminal() {
                eprintln!("monitor terminal restoration during panic failed: {error:#}");
            }
            panic_previous(info);
        }));
        let session = Self {
            previous_hook,
            restored: false,
        };
        enable_raw_mode().context("failed to enable monitor terminal raw mode")?;
        execute!(io::stdout(), EnterAlternateScreen, Hide)
            .context("failed to enter monitor terminal screen")?;
        let terminal = Terminal::new(CrosstermBackend::new(io::stdout()))
            .context("failed to initialize monitor terminal")?;
        Ok((session, terminal))
    }

    /// Restore before returning errors or waiting for the bounded outstanding poll.
    fn restore(&mut self) -> Result<()> {
        if self.restored {
            return Ok(());
        }
        restore_terminal()?;
        self.restored = true;
        Ok(())
    }
}

impl Drop for TerminalSession {
    /// Preserve the original panic report while avoiding hook changes during unwind.
    fn drop(&mut self) {
        if let Err(error) = self.restore() {
            eprintln!("monitor terminal restoration failed: {error:#}");
        }
        if !thread::panicking() {
            let previous_hook = Arc::clone(&self.previous_hook);
            panic::set_hook(Box::new(move |info| previous_hook(info)));
        }
    }
}

/// Attempt both cleanup operations even when one fails, preserving both errors.
fn restore_terminal() -> Result<()> {
    let raw = disable_raw_mode();
    let screen = execute!(io::stdout(), LeaveAlternateScreen, Show);
    match (raw, screen) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(raw), Err(screen)) => bail!("raw-mode cleanup: {raw}; screen cleanup: {screen}"),
        (Err(error), _) => Err(error).context("failed to disable terminal raw mode"),
        (_, Err(error)) => Err(error).context("failed to restore terminal screen and cursor"),
    }
}

/// Capacity-one responses and explicit requests prevent poll backlog and overlap.
struct Poller {
    requests: Option<mpsc::Sender<()>>,
    responses: mpsc::Receiver<Result<MonitorSnapshot>>,
    worker: Option<JoinHandle<()>>,
}

impl Poller {
    /// Transfer the HTTP context to one worker without sharing mutable transport state.
    fn start(context: ClientContext) -> Result<Self> {
        let (requests, request_rx) = mpsc::channel();
        let (responses_tx, responses) = mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("monitor-poll".to_owned())
            .spawn(move || {
                while request_rx.recv().is_ok() {
                    if responses_tx.send(fetch_snapshot(&context)).is_err() {
                        break;
                    }
                }
            })
            .context("failed to start monitor polling worker")?;
        Ok(Self {
            requests: Some(requests),
            responses,
            worker: Some(worker),
        })
    }

    /// The UI calls this only after consuming the previous response.
    fn request(&self) -> Result<()> {
        self.requests
            .as_ref()
            .context("monitor polling worker is stopped")?
            .send(())
            .context("monitor polling worker disconnected")
    }

    /// Closing the request stream wakes an idle worker; an active request has a 2s bound.
    fn stop(&mut self) -> Result<()> {
        self.requests.take();
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| anyhow!("monitor polling worker panicked"))?;
        }
        Ok(())
    }
}

impl Drop for Poller {
    /// Every exit, including unwind, joins the worker rather than detaching its request.
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            eprintln!("monitor polling cleanup failed: {error:#}");
        }
    }
}

/// Retain structured server errors while overriding only this request's timeout.
fn fetch_snapshot(context: &ClientContext) -> Result<MonitorSnapshot> {
    let diagnostic = ClientRequestDiagnostic::new();
    let target = url(context, MONITOR_PATH);
    let response = context
        .http
        .get(&target)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .with_context(|| diagnostic.failure("GET", &target, "monitor snapshot request"))?;
    decode_snapshot(response, &target)
        .with_context(|| diagnostic.failure("GET", &target, "monitor snapshot decoding"))
}

/// Bound body allocation on both success and error paths while retaining source errors.
fn decode_snapshot(response: reqwest::blocking::Response, target: &str) -> Result<MonitorSnapshot> {
    let status = response.status();
    let limit = if status.is_success() {
        MAX_SNAPSHOT_BYTES
    } else {
        MAX_ERROR_BYTES
    };
    let mut body = Vec::new();
    response
        .take(limit + 1)
        .read_to_end(&mut body)
        .with_context(|| {
            format!(
                "GET {target} HTTP {}: failed to read monitor response",
                status.as_u16()
            )
        })?;
    if !status.is_success() {
        let truncated = body.len() as u64 > limit;
        if truncated {
            body.truncate(limit as usize);
        }
        let detail = String::from_utf8_lossy(&body);
        let error = service_error("GET", target, status, &detail);
        return Err(if truncated {
            error.context(format!(
                "HTTP error detail exceeds {limit} bytes; displayed prefix only"
            ))
        } else {
            error
        });
    }
    if body.len() as u64 > limit {
        bail!(
            "GET {target}: monitor snapshot exceeds {limit} bytes; last complete observation retained"
        );
    }
    serde_json::from_slice(&body)
        .with_context(|| format!("GET {target}: unexpected monitor snapshot contract"))
}

/// Client state records receipt freshness only; percentages, rates, and work states
/// always come from the service and are never reconstructed by the dashboard.
#[derive(Default)]
struct ViewState {
    snapshot: Option<MonitorSnapshot>,
    received_at: Option<Instant>,
    transport_error: Option<String>,
    reset_notice: Option<String>,
    in_flight: bool,
}

impl ViewState {
    /// Replace a snapshot atomically so counts from different runs never mix.
    fn receive(&mut self, response: Result<MonitorSnapshot>) {
        self.in_flight = false;
        match response {
            Ok(snapshot) => {
                if let Some(previous) = &self.snapshot {
                    if previous.run_id != snapshot.run_id {
                        self.reset_notice = Some("SERVICE RESTARTED: run totals reset".to_owned());
                    } else if previous.generation != snapshot.generation {
                        self.reset_notice =
                            Some("MONITOR RESET: observations and statistics reset".to_owned());
                    }
                }
                self.snapshot = Some(snapshot);
                self.received_at = Some(Instant::now());
                self.transport_error = None;
            }
            Err(error) => self.transport_error = Some(format!("{error:#}")),
        }
    }
}

/// Run a read-only full-screen session and restore the terminal before surfacing errors.
pub(super) fn run(context: ClientContext) -> Result<()> {
    let (mut session, mut terminal) = TerminalSession::enter()?;
    let mut poller = Poller::start(context)?;
    let result = run_screen(&mut terminal, &poller);
    // Restoration precedes join so the operator regains the normal terminal even
    // when an outstanding HTTP request uses its remaining timeout allowance.
    let restoration = session.restore();
    let stopped = poller.stop();
    for failure in [&result, &restoration, &stopped] {
        if let Err(error) = failure {
            eprintln!("monitor session: {error:#}");
        }
    }
    result.and(restoration).and(stopped)
}

/// Keep input responsive while a single worker polls; resizing redraws the same screen.
fn run_screen(terminal: &mut MonitorTerminal, poller: &Poller) -> Result<()> {
    let mut state = ViewState::default();
    let mut next_poll = Instant::now();
    let mut next_draw = Instant::now();
    loop {
        match poller.responses.try_recv() {
            Ok(response) => {
                state.receive(response);
                next_draw = Instant::now();
            }
            Err(mpsc::TryRecvError::Empty) => {}
            Err(mpsc::TryRecvError::Disconnected) => {
                bail!("monitor polling worker stopped before the session ended");
            }
        }
        let now = Instant::now();
        if now >= next_poll && !state.in_flight {
            poller.request()?;
            state.in_flight = true;
            next_poll = now + REFRESH_INTERVAL;
        }
        if now >= next_draw {
            terminal
                .draw(|frame| render_screen(frame, &state))
                .context("failed to draw monitor screen")?;
            next_draw = now + REFRESH_INTERVAL;
        }
        if event::poll(INPUT_INTERVAL).context("failed to poll monitor terminal input")? {
            match event::read().context("failed to read monitor terminal input")? {
                Event::Key(key) if key.kind != KeyEventKind::Release => {
                    if matches!(key.code, KeyCode::Char('q' | 'Q') | KeyCode::Esc)
                        || (key.code == KeyCode::Char('c')
                            && key.modifiers.contains(KeyModifiers::CONTROL))
                    {
                        return Ok(());
                    }
                }
                Event::Resize(_, _) => next_draw = Instant::now(),
                _ => {}
            }
        }
    }
}

/// A row is text or measured progress; the renderer reserves an explicit overflow row.
enum DisplayRow {
    Text(Line<'static>),
    Progress {
        label: String,
        counts: AnnotationProgressCount,
    },
}

/// Fit all panel categories onto one screen, with more detail as the terminal grows.
fn render_screen(frame: &mut Frame<'_>, state: &ViewState) {
    let area = frame.area();
    if area.width < MIN_TERMINAL_WIDTH || area.height < MIN_TERMINAL_HEIGHT {
        render_size_notice(frame, area, state);
        return;
    }
    // Model totals receive their own full-width table; squeezing them into a
    // narrow activity column would hide most models at ordinary terminal sizes.
    let statistics = state.snapshot.as_ref().map_or_else(Vec::new, |snapshot| {
        statistics_rows(snapshot, area.width.saturating_sub(2))
    });
    // Reserve activity capacity from terminal geometry alone: arrivals and
    // completions must never move neighboring panels. At minimum size, work
    // has two rows and each call table has a heading plus three data rows.
    // Share extra height with these panels and the attention/history band.
    let extra_rows = area.height.saturating_sub(MIN_TERMINAL_HEIGHT) / 4;
    let work_height = 4 + extra_rows;
    let calls_height = 6 + extra_rows;
    let statistics_height = 6 + extra_rows;
    let bands = Layout::vertical([
        Constraint::Length(4),
        Constraint::Length(11),
        Constraint::Length(work_height),
        Constraint::Length(calls_height),
        Constraint::Length(statistics_height),
        Constraint::Fill(1),
        Constraint::Length(1),
    ])
    .split(area);
    render_header(frame, bands[0], state);
    let Some(snapshot) = &state.snapshot else {
        let pending =
            Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).split(Rect::new(
                area.x,
                bands[0].bottom(),
                area.width,
                area.height.saturating_sub(4),
            ));
        render_panel(
            frame,
            pending[0],
            "CONNECTING",
            vec![text_row(
                state
                    .transport_error
                    .as_deref()
                    .unwrap_or("Waiting for first service snapshot; Q or Ctrl-C exits."),
                if state.transport_error.is_some() {
                    PROBLEM
                } else {
                    WAITING
                },
            )],
        );
        return;
    };
    let summaries = Layout::horizontal([
        Constraint::Ratio(1, 3),
        Constraint::Ratio(1, 3),
        Constraint::Ratio(1, 3),
    ])
    .split(bands[1]);
    let outcomes = Layout::horizontal([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(bands[5]);
    render_panel(frame, summaries[0], "INGESTION", ingestion_rows(snapshot));
    render_panel(
        frame,
        summaries[1],
        "ANNOTATIONS · COMMITTED",
        annotation_rows(snapshot),
    );
    render_panel(
        frame,
        summaries[2],
        "RETRIEVAL · PUBLISHED",
        publication_rows(snapshot),
    );
    render_panel(
        frame,
        bands[2],
        &format!("CURRENT WORK · {} groups", current_work(snapshot).count()),
        work_rows(snapshot),
    );
    let running: u64 = snapshot.calls.iter().map(|call| call.running).sum();
    render_panel(
        frame,
        bands[3],
        &format!(
            "MODEL CALLS · {running} running · {} / {CALL_HISTORY_LIMIT} finished retained",
            snapshot
                .call_log
                .iter()
                .filter(|call| call.state != MonitorState::Running)
                .count()
        ),
        call_rows(snapshot, bands[3].width.saturating_sub(2)),
    );
    render_panel(
        frame,
        bands[4],
        &format!(
            "COMPLETED CALLS · observed {}",
            duration(snapshot.generation_elapsed_ms)
        ),
        statistics,
    );
    render_panel(
        frame,
        outcomes[0],
        "NEEDS ATTENTION",
        attention_rows(snapshot, state, outcomes[0].width.saturating_sub(2)),
    );
    render_panel(
        frame,
        outcomes[1],
        &format!(
            "RECENT · UTC · {} / {RECENT_EVENT_LIMIT} retained",
            snapshot.recent.len()
        ),
        recent_rows(snapshot),
    );
    let footer = "Q/Ctrl-C quit · +N omitted rows · Tokens(k): reported, rounded; reasoning in OUT; MISS: partial/missing usage";
    frame.render_widget(
        Paragraph::new(footer).style(Style::default().fg(MUTED)),
        bands[6],
    );
}

/// A reduced terminal gets an explicit resize state, never silently missing dashboard panels.
fn render_size_notice(frame: &mut Frame<'_>, area: Rect, state: &ViewState) {
    let mut rows = vec![
        text_row(
            format!(
                "Resize to at least {MIN_TERMINAL_WIDTH} columns × {MIN_TERMINAL_HEIGHT} rows."
            ),
            WAITING,
        ),
        text_row(
            format!(
                "Current size: {} × {}. Q or Ctrl-C exits.",
                area.width, area.height
            ),
            NORMAL,
        ),
        text_row(
            "Only aggregate coverage is shown below; dashboard panels need more space.",
            MUTED,
        ),
    ];
    if let Some(error) = &state.transport_error {
        rows.push(text_row(format!("STALE · {}", safe(error)), PROBLEM));
    }
    if let Some(snapshot) = &state.snapshot {
        rows.extend([
            progress_row("Active known sources", snapshot.ingestion.active_sources),
            progress_row("Committed annotations", snapshot.annotations.progress),
            progress_row("Graph published", snapshot.publication.graph_progress),
            progress_row("Summary published", snapshot.publication.summary_progress),
        ]);
        if let Some(embeddings) = snapshot.publication.embeddings_progress {
            rows.push(progress_row("Embedding cohorts published", embeddings));
        }
    }
    render_panel(frame, area, "RESIZE TERMINAL · LIMITED VIEW", rows);
}

/// Lead with server-owned ingestion status; transport freshness changes only when stale.
fn render_header(frame: &mut Frame<'_>, area: Rect, state: &ViewState) {
    let age = state.received_at.map(|at| at.elapsed());
    let stale = state.transport_error.is_some() || age.is_some_and(|age| age >= REQUEST_TIMEOUT);
    let (status, color) = match (&state.snapshot, stale) {
        (Some(_), true) => ("STALE", PROBLEM),
        (Some(snapshot), false) if snapshot.ready => ("Service ready · connected", RUNNING),
        (Some(_), false) => ("Service not ready · connected", WAITING),
        (None, _) => ("CONNECTING", WAITING),
    };
    let mut first = Line::from(vec![
        Span::styled(
            " DATA STORE  ",
            Style::default().fg(NORMAL).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            status,
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ),
    ]);
    if let Some(snapshot) = &state.snapshot {
        first.spans.push(Span::raw(format!(
            " · up {} · queries {}/{}",
            duration(snapshot.uptime_ms),
            snapshot.queries_in_flight,
            snapshot.query_limit
        )));
    }
    frame.render_widget(
        Paragraph::new(first).style(Style::default().fg(NORMAL)),
        Rect::new(area.x, area.y, area.width, 1),
    );
    if let Some(snapshot) = &state.snapshot {
        let notice = state
            .reset_notice
            .as_ref()
            .map_or_else(String::new, |notice| format!("{notice} · "));
        let second = Line::from(format!(
            " {notice}{}",
            if snapshot.headline.is_empty() {
                "Ingestion summary unavailable from this server".to_owned()
            } else {
                safe(&snapshot.headline)
            }
        ));
        frame.render_widget(
            Paragraph::new(fit_line(&second, area.width)).style(Style::default().fg(
                if state.reset_notice.is_some() {
                    WAITING
                } else {
                    NORMAL
                },
            )),
            Rect::new(area.x, area.y + 1, area.width, 1),
        );
        frame.render_widget(
            Paragraph::new(if snapshot.activity.is_empty() {
                " Current activity explanation unavailable".to_owned()
            } else {
                format!(" {}", safe(&snapshot.activity))
            })
            .style(Style::default().fg(NORMAL)),
            Rect::new(area.x, area.y + 2, area.width, 1),
        );
        render_rates(
            frame,
            Rect::new(area.x, area.y + 3, area.width, 1),
            snapshot,
        );
    }
}

/// Give every rate its own cell so long labels cannot push another rate off screen.
fn render_rates(frame: &mut Frame<'_>, area: Rect, snapshot: &MonitorSnapshot) {
    if snapshot.rates.is_empty() {
        frame.render_widget(
            Paragraph::new(format!(
                " Rates ≤{RATE_WINDOW_SECONDS}s · awaiting measured completions"
            ))
            .style(Style::default().fg(MUTED)),
            area,
        );
        return;
    }
    let columns = Layout::horizontal(vec![Constraint::Fill(1); snapshot.rates.len()]).split(area);
    for (rate, column) in snapshot.rates.iter().zip(columns.iter()) {
        let counts = format!(
            "{:.1}/m ({}/{})",
            rate.per_minute,
            rate.completed,
            duration(rate.window_ms)
        );
        let width = usize::from(column.width)
            .saturating_sub(Line::from(counts.as_str()).width() + 2) as u16;
        let label = fit_line(&Line::from(safe(&rate.label)), width);
        frame.render_widget(
            Paragraph::new(format!(" {counts} {label}")).style(Style::default().fg(RUNNING)),
            *column,
        );
    }
}

/// Preserve file/source population boundaries and explicit discovery incompleteness.
fn ingestion_rows(snapshot: &MonitorSnapshot) -> Vec<DisplayRow> {
    let data = &snapshot.ingestion;
    vec![
        progress_row("Searchable / known sources", data.active_sources),
        text_row(
            format!(
                "Blocked by failed parse: {}",
                optional(data.blocked_sources)
            ),
            MUTED,
        ),
        text_row(
            format!(
                "Queued {} · working {} · failed jobs {}",
                optional(data.pending),
                optional(data.in_flight),
                optional(data.failed)
            ),
            NORMAL,
        ),
        text_row(
            format!(
                "Last scan: {} files · {} queued",
                optional(data.enumerated_files),
                data.staged_files
            ),
            NORMAL,
        ),
        text_row(
            format!(
                "Not queued {} · scan errors {}",
                data.skipped_files, data.scan_failures
            ),
            if data.scan_failures > 0 {
                PROBLEM
            } else {
                NORMAL
            },
        ),
        text_row(
            format!(
                "Discovery {}",
                if data.enumeration_complete {
                    "complete"
                } else {
                    "not yet complete"
                }
            ),
            MUTED,
        ),
    ]
}

/// Coverage and unfinished states remain distinct; memoized work is not added to committed totals.
fn annotation_rows(snapshot: &MonitorSnapshot) -> Vec<DisplayRow> {
    let data = &snapshot.annotations;
    let mut rows = vec![
        text_row(
            format!("Scope: {} active documents only", optional(data.documents)),
            NORMAL,
        ),
        progress_row("Committed annotation work", data.progress),
    ];
    rows.extend(
        data.by_type
            .iter()
            .map(|kind| progress_row(&kind.annotation_type, kind.progress)),
    );
    rows.push(text_row(
        format!(
            "Pending {} · running {} · retry {}",
            data.work.pending, data.work.running, data.work.retry_waiting
        ),
        NORMAL,
    ));
    rows.push(text_row(
        format!(
            "Failed {} · exhausted {} · reused {}",
            data.work.failed, data.work.exhausted, data.memoized
        ),
        if data.work.exhausted > 0 {
            PROBLEM
        } else {
            NORMAL
        },
    ));
    rows.push(text_row(
        format!(
            "Documents awaiting measurement: {}",
            data.unmeasured_documents
        ),
        NORMAL,
    ));
    if let Some(reason) = &data.parked_reason {
        rows.push(text_row(format!("WAIT {}", safe(reason)), WAITING));
    }
    rows
}

/// Publication bars use server percentages; pending and failed current inputs stay visible.
fn publication_rows(snapshot: &MonitorSnapshot) -> Vec<DisplayRow> {
    let data = &snapshot.publication;
    let mut rows = vec![
        text_row(
            format!("Scope: {} active documents only", optional(data.documents)),
            NORMAL,
        ),
        progress_row("Graph", data.graph_progress),
        text_row(
            format!(
                "Pending {} · failed {}",
                data.graph.pending, data.graph.failed
            ),
            NORMAL,
        ),
        progress_row("Summary", data.summary_progress),
        text_row(
            format!(
                "Pending {} · failed {}",
                data.summary.pending, data.summary.failed
            ),
            NORMAL,
        ),
    ];
    if let (Some(coverage), Some(counts)) = (&data.embeddings_progress, &data.embeddings) {
        rows.push(progress_row("Embedding cohorts", *coverage));
        rows.push(text_row(
            format!("Pending {} · failed {}", counts.pending, counts.failed),
            NORMAL,
        ));
    } else {
        rows.push(text_row("Embedding cohorts: unmeasured", WAITING));
    }
    rows.push(text_row(
        format!(
            "Documents awaiting embedding counts: {}",
            data.unmeasured_embedding_documents
        ),
        MUTED,
    ));
    rows
}

/// Publication stages belong to retrieval metrics and recent outcomes, not the
/// current-work list. Share this selection with its count and empty-state label.
fn current_work(snapshot: &MonitorSnapshot) -> impl Iterator<Item = &WorkObservation> {
    snapshot
        .work
        .iter()
        .filter(|work| work.identity.worker != PUBLICATION_WORKER)
}

/// Keep worker identity, stage, measured progress, and specific waits adjacent on screen.
fn work_rows(snapshot: &MonitorSnapshot) -> Vec<DisplayRow> {
    if current_work(snapshot).next().is_none() {
        return vec![text_row(
            "No ingestion or annotation work. See retrieval and recent activity.",
            MUTED,
        )];
    }
    let mut rows = Vec::new();
    // Brief work remains observable through completion history and counters;
    // displaying it here for a single poll produces unreadable flashes. This
    // presentation filter does not delay failures in the attention panel.
    for work in current_work(snapshot).filter(|work| work.elapsed_ms >= 1_000) {
        let detail = work
            .detail
            .as_ref()
            .map_or_else(String::new, |detail| format!(" · {}", safe(detail)));
        let label = format!(
            "{} {} {} · {}{detail} · {}",
            state_label(work.state),
            duration(work.elapsed_ms),
            safe(&work.identity.worker),
            safe(&work.stage),
            document_label(&work.identity.document)
        );
        if let Some(progress) = &work.progress {
            rows.push(progress_row(
                &format!("{} · {label}", safe(&progress.unit)),
                progress.counts,
            ));
        } else {
            rows.push(text_row(label, state_color(work.state)));
        }
    }
    rows
}

/// Server-retained outcomes survive polls; active calls precede completion history
/// in the same fixed-height table, with unknown token counts kept explicit.
fn call_rows(snapshot: &MonitorSnapshot, width: u16) -> Vec<DisplayRow> {
    if snapshot.call_log.is_empty() {
        return vec![text_row("No model call records available", MUTED)];
    }
    // Reserve numeric and status columns before distributing name space so
    // long model/document names cannot clip the outcome or reported usage.
    let name_width = width.saturating_sub(64) / 3;
    let mut rows = vec![text_row(
        format!(
            "{:>7} {:>8} {:>6} {:>6} {:>6} {:>6} {:>6} {} {} {} {}",
            "STATE",
            "UTC",
            "TIME",
            "LIMIT",
            "IN(k)",
            "OUT(k)",
            "TOT(k)",
            fit_cell("ROLE", 9),
            fit_cell("STAGE", name_width),
            fit_cell("DOCUMENT", name_width),
            fit_cell("MODEL", name_width)
        ),
        MUTED,
    )];
    for call in &snapshot.call_log {
        rows.push(text_row(
            format!(
                "{:>7} {:>8} {:>6} {:>6} {:>6} {:>6} {:>6} {} {} {} {}",
                state_label(call.state),
                fit_cell(&utc_clock(call.started_at.as_deref()), 8),
                fit_cell(&duration(call.elapsed_ms), 6),
                fit_cell(&optional_duration(call.timeout_ms), 6),
                fit_cell(&token_thousands(call.usage.prompt), 6),
                fit_cell(&token_thousands(call.usage.completion), 6),
                fit_cell(&token_thousands(call.usage.total), 6),
                fit_cell(&call.role, 9),
                fit_cell(&call.stage, name_width),
                fit_cell(&document_label(&call.identity.document), name_width),
                fit_cell(&call.model, name_width),
            ),
            state_color(call.state),
        ));
    }
    rows
}

/// Pad by terminal cells, keeping Unicode names aligned with adjacent numeric columns.
fn fit_cell(value: &str, width: u16) -> String {
    let line = fit_line(&Line::from(safe(value)), width);
    format!(
        "{line}{}",
        " ".repeat(usize::from(width).saturating_sub(line.width()))
    )
}

/// Calls use UTC clock time from the service timestamp; no local-clock conversion is inferred.
fn utc_clock(value: Option<&str>) -> String {
    value.map_or_else(
        || "?".to_owned(),
        |value| {
            value
                .get(11..19)
                .filter(|_| value.ends_with('Z'))
                .map_or_else(|| safe(value), safe)
        },
    )
}

/// Keep every model's outcomes, timings, and reported usage on one comparable row.
fn statistics_rows(snapshot: &MonitorSnapshot, width: u16) -> Vec<DisplayRow> {
    if snapshot.model_stats.is_empty() {
        return vec![text_row(
            "No model calls completed since monitoring began",
            MUTED,
        )];
    }
    let mut rows = Vec::new();
    let name_width = usize::from(width.saturating_sub(72)).max(8);
    rows.push(text_row(
        format!(
            "{:<name_width$} {:>5} {:>4} {:>4} {:>6} {:>6} {:>8} {:>8} {:>8} {:>8} {:>4}",
            "ROLE / MODEL",
            "OK",
            "ERR",
            "CAN",
            "MEAN",
            "MAX",
            "IN(k)",
            "OUT(k)",
            "REASN(k)",
            "TOTAL(k)",
            "MISS"
        ),
        MUTED,
    ));
    for model in &snapshot.model_stats {
        let name = fit_line(
            &Line::from(format!("{} / {}", safe(&model.role), safe(&model.model))),
            name_width as u16,
        )
        .to_string();
        rows.push(text_row(
            format!(
                "{name:<name_width$} {:>5} {:>4} {:>4} {:>6} {:>6} {:>8} {:>8} {:>8} {:>8} {:>4}",
                model.succeeded,
                model.failed,
                model.cancelled,
                optional_duration(model.mean_duration_ms),
                optional_duration(model.max_duration_ms),
                token_thousands(model.usage.prompt),
                token_thousands(model.usage.completion),
                token_thousands(model.usage.reasoning),
                token_thousands(model.usage.total),
                model.usage_unavailable,
            ),
            if model.failed > 0 { PROBLEM } else { NORMAL },
        ));
    }
    rows
}

/// Scale provider totals for the compact table while marking missing metadata explicitly.
fn token_thousands(value: Option<u64>) -> String {
    value.map_or_else(
        || "?".to_owned(),
        |value| format!("{:.1}", value as f64 / 1_000.0),
    )
}

/// Outstanding problems do not disappear when the bounded recent history rolls over.
fn attention_rows(snapshot: &MonitorSnapshot, state: &ViewState, width: u16) -> Vec<DisplayRow> {
    let mut rows = Vec::new();
    if let Some(error) = &state.transport_error {
        rows.push(text_row(format!("STALE · {}", safe(error)), PROBLEM));
    }
    for issue in &snapshot.issues {
        append_wrapped_text(
            &mut rows,
            format!(
                "{} ×{} · {} · {}",
                state_label(issue.state),
                issue.affected,
                identity(&issue.identity),
                safe(&issue.stage)
            ),
            state_color(issue.state),
            width,
        );
        append_wrapped_text(
            &mut rows,
            format!("  {}", safe(&issue.message)),
            state_color(issue.state),
            width,
        );
        if issue.retry_in_ms.is_some() || issue.attempt.is_some() {
            // The allowance counts additional retries; an observed attempt is
            // not a completion numerator and can legitimately exceed that value.
            rows.push(text_row(
                format!(
                    "  retry in {} · attempt {} · retry allowance {}",
                    optional_duration(issue.retry_in_ms),
                    optional(issue.attempt),
                    optional(issue.retry_limit)
                ),
                WAITING,
            ));
        }
    }
    // Zero-valued exception categories are not problems; nonzero counts remain
    // visible even when no per-document observation was supplied by their owner.
    for count in snapshot.corpus.iter().filter(|count| count.value > 0) {
        rows.push(text_row(
            format!(
                "{} / {}: {}",
                safe(&count.source_system),
                safe(&count.label),
                count.value
            ),
            PROBLEM,
        ));
    }
    if rows.is_empty() {
        rows.push(text_row("No outstanding issues reported", MUTED));
    }
    rows
}

/// Failure explanations wrap onto adjacent rows instead of hiding the cause or
/// recovery instruction behind horizontal truncation. Panel overflow stays explicit.
fn append_wrapped_text(rows: &mut Vec<DisplayRow>, text: String, color: Color, width: u16) {
    let width = usize::from(width.max(1));
    let mut line = String::new();
    let mut cells = 0;
    for character in text.chars() {
        let character_cells = Line::from(character.to_string()).width();
        if cells + character_cells > width && !line.is_empty() {
            rows.push(text_row(std::mem::take(&mut line), color));
            cells = 0;
        }
        line.push(character);
        cells += character_cells;
    }
    if !line.is_empty() {
        rows.push(text_row(line, color));
    }
}

/// Recent outcomes retain their actual boundary label rather than implying durable success.
fn recent_rows(snapshot: &MonitorSnapshot) -> Vec<DisplayRow> {
    if snapshot.recent.is_empty() {
        return vec![text_row("No recent outcomes observed", MUTED)];
    }
    let mut rows = Vec::new();
    for event in &snapshot.recent {
        rows.push(text_row(
            format!(
                "{} {} {}",
                utc_clock(event.at.as_deref()),
                state_label(event.state),
                document_label(&event.identity.document)
            ),
            state_color(event.state),
        ));
        rows.push(text_row(
            format!("  {} · {}", safe(&event.stage), safe(&event.message)),
            NORMAL,
        ));
    }
    rows
}

/// Draw a bounded panel without silently discarding overflow or clipping long text.
fn render_panel(frame: &mut Frame<'_>, area: Rect, title: &str, rows: Vec<DisplayRow>) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BORDER))
        .title(
            Line::from(format!(" {title} "))
                .style(Style::default().fg(NORMAL).add_modifier(Modifier::BOLD)),
        );
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height == 0 || inner.width == 0 {
        return;
    }
    let capacity = usize::from(inner.height);
    let visible = if rows.len() > capacity {
        capacity.saturating_sub(1)
    } else {
        rows.len()
    };
    for (index, row) in rows.iter().take(visible).enumerate() {
        let line_area = Rect::new(inner.x, inner.y + index as u16, inner.width, 1);
        match row {
            DisplayRow::Text(line) => {
                frame.render_widget(Paragraph::new(fit_line(line, inner.width)), line_area)
            }
            DisplayRow::Progress { label, counts } => {
                render_progress(frame, line_area, label, counts)
            }
        }
    }
    if rows.len() > visible {
        let message = format!("+{} more rows · enlarge terminal", rows.len() - visible);
        frame.render_widget(
            Paragraph::new(fit_line(&Line::from(message), inner.width))
                .style(Style::default().fg(WAITING)),
            Rect::new(inner.x, inner.bottom() - 1, inner.width, 1),
        );
    }
}

/// A percentage comes from the server; clamping only protects the terminal gauge API.
fn render_progress(
    frame: &mut Frame<'_>,
    area: Rect,
    label: &str,
    counts: &AnnotationProgressCount,
) {
    let counts_text = match (counts.total, counts.percentage) {
        (Some(0), _) => format!("{}/0 · no work", counts.completed),
        (Some(total), Some(percent)) if percent.is_finite() => {
            format!("{}/{} {:.1}%", counts.completed, total, percent)
        }
        (Some(total), _) => format!("{}/{} · ?%", counts.completed, total),
        (None, _) => format!("{} / ?", counts.completed),
    };
    // Long stage names lose display space before the measured numerator and
    // denominator do; the progress bar must never hide what its percentage counts.
    let label_width =
        usize::from(area.width).saturating_sub(Line::from(counts_text.as_str()).width() + 1);
    let short_label = fit_line(&Line::from(safe(label)), label_width as u16);
    let text = format!("{counts_text} {short_label}");
    let fitted = fit_line(&Line::from(text), area.width);
    if let Some(percent) = counts.percentage.filter(|value| value.is_finite()) {
        frame.render_widget(
            Gauge::default()
                .ratio((percent / 100.0).clamp(0.0, 1.0))
                .gauge_style(Style::default().fg(RUNNING).bg(Color::Rgb(25, 34, 45)))
                .label(fitted.to_string())
                .use_unicode(true),
            area,
        );
    } else {
        frame.render_widget(
            Paragraph::new(fitted).style(Style::default().fg(MUTED)),
            area,
        );
    }
}

/// Construct owned text because snapshots can be replaced between terminal frames.
fn text_row(text: impl AsRef<str>, color: Color) -> DisplayRow {
    DisplayRow::Text(Line::from(safe(text.as_ref())).style(Style::default().fg(color)))
}

/// Preserve the shared count record unchanged until its final presentation step.
fn progress_row(label: &str, counts: AnnotationProgressCount) -> DisplayRow {
    DisplayRow::Progress {
        label: safe(label),
        counts,
    }
}

/// Truncate by terminal cell width, preserving Unicode and making horizontal loss explicit.
fn fit_line(line: &Line<'_>, width: u16) -> Line<'static> {
    if line.width() <= usize::from(width) {
        return Line::from(line.to_string()).style(line.style);
    }
    if width == 0 {
        return Line::default();
    }
    let budget = usize::from(width.saturating_sub(1));
    let mut text = String::new();
    for character in line.to_string().chars() {
        text.push(character);
        if Line::from(text.as_str()).width() > budget {
            text.pop();
            break;
        }
    }
    text.push('…');
    Line::from(text).style(line.style)
}

/// Source names and remote errors are data, never terminal escape sequences or new rows.
fn safe(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect()
}

/// Keep worker ownership visible for unnamed or shared operations as well as documents.
fn identity(value: &WorkIdentity) -> String {
    format!("{} · {}", safe(&value.worker), safe(&value.document))
}

/// Prefer the identifying path suffix over a shared directory prefix when space is tight.
fn document_label(value: &str) -> String {
    let sanitized = safe(value);
    if Line::from(sanitized.as_str()).width() <= 24 {
        return sanitized;
    }
    let mut suffix = Vec::new();
    let mut cells = 0;
    for character in sanitized.chars().rev() {
        let character_cells = Line::from(character.to_string()).width();
        if cells + character_cells > 23 {
            break;
        }
        suffix.push(character);
        cells += character_cells;
    }
    format!("…{}", suffix.into_iter().rev().collect::<String>())
}

/// Unknown counts remain visibly unknown instead of becoming invented zeroes.
fn optional(value: Option<u64>) -> String {
    value.map_or_else(|| "?".to_owned(), |value| value.to_string())
}

/// Missing timing observations are distinct from measured zero-duration boundaries.
fn optional_duration(value: Option<u64>) -> String {
    value.map_or_else(|| "?".to_owned(), duration)
}

/// Format measured elapsed time without changing its source or estimating remaining work.
fn duration(milliseconds: u64) -> String {
    if milliseconds < 1_000 {
        format!("{milliseconds}ms")
    } else if milliseconds < 60_000 {
        format!("{:.1}s", milliseconds as f64 / 1_000.0)
    } else if milliseconds < 3_600_000 {
        format!(
            "{}m{:02}s",
            milliseconds / 60_000,
            milliseconds / 1_000 % 60
        )
    } else {
        format!(
            "{}h{:02}m",
            milliseconds / 3_600_000,
            milliseconds / 60_000 % 60
        )
    }
}

/// Every color has a corresponding text state, including cancellation and unavailable data.
fn state_label(state: MonitorState) -> &'static str {
    match state {
        MonitorState::Running => "RUN",
        MonitorState::Waiting => "WAIT",
        MonitorState::Complete => "DONE",
        MonitorState::Failed => "ERROR",
        MonitorState::Cancelled => "CANCEL",
        MonitorState::Unavailable => "UNKNOWN",
        MonitorState::Idle => "IDLE",
    }
}

/// Blue, amber, and magenta separate activity without relying on red/green perception.
fn state_color(state: MonitorState) -> Color {
    match state {
        MonitorState::Running => RUNNING,
        MonitorState::Waiting => WAITING,
        MonitorState::Failed | MonitorState::Unavailable => PROBLEM,
        MonitorState::Complete => NORMAL,
        MonitorState::Cancelled | MonitorState::Idle => MUTED,
    }
}
