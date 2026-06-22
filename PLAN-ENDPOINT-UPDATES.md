# Endpoint Updates Implementation Plan

## Goal

Consolidate the HTTP API surface around the canonical streamed operation API
while keeping the dedicated readiness route that still serves a distinct
operator and CLI diagnostic purpose.

Keep:

- `GET /v1/health`
- `POST /v1/operations`
- `POST /v1/operations/{operationId}/control`

Remove:

- `GET /v1/limits`
- `GET /v1/sources`
- `POST /v1/ingest`
- `POST /v1/search`
- `POST /admin/shutdown`
- `GET /admin/document-versions`
- `POST /admin/document-versions/rollback`

The app is expected to be offline during all phases. Phases optimize for
accuracy and development efficiency, not between-phase runtime functionality.

## Verified Current State

These findings were verified before this plan was written so implementation
sessions should not need another codebase exploration pass.

- `src/http.rs` registers all current HTTP routes in `build_router` at the top
  of the file.
- `GET /v1/health` is registered at `src/http.rs:71` and handled by
  `get_health` at `src/http.rs:92`.
- `GET /v1/health` is a live supported route, not a removable compatibility
  shim:
  - `src/main.rs:486` builds `http://{bind_address}/v1/health` for startup
    readiness reporting.
  - `src/bin/data-store.rs:22` defines `HEALTH_PATH`.
  - `src/bin/data-store.rs:1450` uses `probe_health_after_stream_loss` to send a
    short `GET /v1/health` probe after ambiguous operation-stream loss.
- The seven removable route handlers are thin transport adapters over shared
  execution functions:
  - `get_limits` calls `build_limits_response`.
  - `get_sources` calls `execute_ingested_sources`.
  - `post_ingest` decodes `IngestRequest` and calls `execute_ingest`.
  - `post_search` decodes `SearchRequest` and calls `execute_search`.
  - `post_admin_shutdown` authorizes and calls `execute_shutdown`.
  - `get_admin_document_versions` authorizes and calls
    `execute_document_versions`.
  - `post_admin_document_version_rollback` authorizes, decodes
    `DocumentVersionRollbackRequest`, and calls
    `execute_document_version_rollback`.
- The shared execution functions are used by `execute_operation` for
  `/v1/operations` and must stay:
  - `build_limits_response`
  - `execute_ingested_sources`
  - `execute_ingest`
  - `execute_search`
  - `execute_shutdown`
  - `execute_document_versions`
  - `execute_document_version_rollback`
- `log_route_started` must stay. It is still used by `get_health` and
  `post_operation_control`.
- `log_route_failed` must stay. It is still used by `post_operation_control`.
- `src/main.rs` does not need code changes for endpoint removal because its
  `/v1/health` readiness URL remains valid.
- `src/bin/data-store.rs` does not need code changes for endpoint removal
  because normal CLI commands already use `/v1/operations`, and the health probe
  remains valid.
- `SPEC-SERVER.md`, `SPEC-CLIENT.md`, and `PROTOCOL.md` were checked for the
  removed route-specific endpoints. They only matched `/v1/operations` and
  `/v1/operations/{operationId}/control`, so no spec edits are planned.

## Global Guardrails

- Ask before each phase's file writes unless the user has explicitly approved
  that phase.
- Any edit to `config.example.toml` or `config.toml` requires explicit config
  approval, even though the intended changes are comment-only.
- Do not change config shape, values, parsing, defaults, or operational
  behavior.
- Do not start, stop, or restart the service.
- Do not run `cargo test`.
- After any approved Rust source edit, run:
  - `cargo fmt`
  - `cargo check`
  - `cargo check --features metal`
  - `cargo clippy`
- If Cargo verification fails because of the approved Rust change, fix only
  compile/lint issues within the approved scope unless the fix would change
  behavior or require a design decision.

## Progress Tracker

- [x] Phase 1: Remove public legacy routes
- [x] Phase 2: Remove admin legacy routes
- [x] Phase 3: Update operator and architecture docs
- [x] Phase 4: Update config comments
- [x] Phase 5: Reference sweep and plan status update
- [ ] Phase 6: Run clippy cleanup and update agent instructions

## Phase 1: Remove Public Legacy Routes

Status: Completed.

Completed summary:

- Removed the public legacy route registrations for `/v1/limits`,
  `/v1/sources`, `/v1/ingest`, and `/v1/search` from `src/http.rs`.
- Deleted the thin public legacy handler functions `get_limits`, `get_sources`,
  `post_ingest`, and `post_search`.
- Kept the shared operation-stream execution paths intact, including
  `build_limits_response`, `execute_ingested_sources`, `execute_ingest`, and
  `execute_search`.
- Updated adjacent `execute_ingest` and `execute_search` comments so they no
  longer describe route-specific sharing.
- Verified with `cargo fmt`, `cargo check`, `cargo check --features metal`,
  `cargo clippy`, and the planned `rg` checks for removed public route strings
  and handler names.

Estimated effort: 10k-14k tokens.

Confidence: 92%.

Scope:

- Edit `src/http.rs` only.
- Remove public legacy route registrations from `build_router`.
- Delete public legacy handler functions.
- Keep all shared execution functions and operation-stream dispatch behavior.

Route registrations to remove from `build_router`:

- `.route("/v1/limits", get(get_limits))`
- `.route("/v1/sources", get(get_sources))`
- `.route("/v1/ingest", post(post_ingest))`
- `.route("/v1/search", post(post_search))`

Handler functions to delete:

- `get_limits`, currently at `src/http.rs:110`.
- `get_sources`, currently at `src/http.rs:146`.
- `post_ingest`, currently at `src/http.rs:268`.
- `post_search`, currently at `src/http.rs:914`.

Functions and helpers to keep:

- `get_health`
- `build_limits_response`
- `execute_ingested_sources`
- `execute_ingest`
- `execute_search`
- `post_operation`
- `post_operation_control`
- `execute_operation`
- `log_route_started`
- `log_route_failed`
- `json_rejection_to_api_error`
- `bearer_token_from_headers`

Expected import impact:

- No import removal is expected to be required solely from this phase. Types
  such as `IngestRequest`, `IngestResponse`, `SearchRequest`, `SearchResponse`,
  `LimitsResponse`, `Json`, and `JsonRejection` remain used by shared execution
  functions or operation-stream handlers.
- Let Cargo and clippy identify any unexpected unused imports after deletion.

Verification:

- `cargo fmt`
- `cargo check`
- `cargo check --features metal`
- `cargo clippy`
- `rg -n '\b(get_limits|get_sources|post_ingest|post_search)\b' src/http.rs`
  should only show no matches after the phase.
- `rg -n '/v1/(limits|sources|ingest|search)' src/http.rs` should show no
  matches after the phase.

Completion criteria:

- Public legacy endpoints are no longer registered.
- Public legacy handler functions are gone.
- `/v1/health`, `/v1/operations`, and operation control still compile.
- Shared operation names `limits`, `sources`, `ingest`, and `search` still work
  through `execute_operation`.

## Phase 2: Remove Admin Legacy Routes

Status: Completed.

Completed summary:

- Removed the admin legacy route registrations for `/admin/shutdown`,
  `/admin/document-versions`, and `/admin/document-versions/rollback` from
  `src/http.rs`.
- Deleted the thin admin legacy handler functions `post_admin_shutdown`,
  `get_admin_document_versions`, and
  `post_admin_document_version_rollback`.
- Kept protected operation-stream execution intact, including
  `execute_shutdown`, `execute_document_versions`,
  `execute_document_version_rollback`, `OperationName::is_protected`, and
  protected-operation dispatch in `execute_operation`.
- Updated the `build_router` comment so it describes the supported health,
  operation, and operation-control routes.
- Verified with `cargo fmt`, `cargo check`, `cargo check --features metal`,
  `cargo clippy`, and the planned `rg` checks for removed admin route strings
  and handler names.

Estimated effort: 8k-12k tokens.

Confidence: 93%.

Scope:

- Edit `src/http.rs` only.
- Remove admin legacy route registrations from `build_router`.
- Delete admin legacy handler functions.
- Keep protected operations through `/v1/operations`.
- Keep admin authorization helpers and shared admin execution functions.

Route registrations to remove from `build_router`:

- `.route("/admin/shutdown", post(post_admin_shutdown))`
- `.route("/admin/document-versions", get(get_admin_document_versions))`
- `.route("/admin/document-versions/rollback", post(post_admin_document_version_rollback))`

Handler functions to delete:

- `post_admin_shutdown`, currently at `src/http.rs:2842`.
- `get_admin_document_versions`, currently at `src/http.rs:2955`.
- `post_admin_document_version_rollback`, currently at `src/http.rs:3071`.

Functions and helpers to keep:

- `bearer_token_from_headers`
- `state.authorize_admin_token` call path in `post_operation`
- `execute_shutdown`
- `execute_document_versions`
- `execute_document_version_rollback`
- `OperationName::is_protected`
- protected-operation dispatch in `execute_operation`

Expected import impact:

- `HeaderMap` still remains used by `post_operation`.
- `DocumentVersionRollbackRequest`, `DocumentVersionRollbackResponse`, and
  `ShutdownResponse` still remain used by shared execution functions.
- Let Cargo and clippy identify any unexpected unused imports after deletion.

Verification:

- `cargo fmt`
- `cargo check`
- `cargo check --features metal`
- `cargo clippy`
- `rg -n '\b(post_admin_shutdown|get_admin_document_versions|post_admin_document_version_rollback)\b' src/http.rs`
  should show no matches after the phase.
- `rg -n '/admin/(shutdown|document-versions)' src/http.rs` should show no
  matches after the phase.
- `rg -n 'OperationName::(Versions|Rollback|Shutdown)|Self::(Versions|Rollback|Shutdown)' src/http.rs`
  should still show protected operation support.

Completion criteria:

- Admin legacy endpoints are no longer registered.
- Admin legacy handler functions are gone.
- Protected operations `versions`, `rollback`, and `shutdown` still compile
  through `/v1/operations`.

## Phase 3: Update Operator and Architecture Docs

Status: Completed.

Completed summary:

- Updated `README.md` to describe `GET /v1/health` as the supported
  readiness/liveness route and `POST /v1/operations` as the operation endpoint
  for all other consumer operations, including protected operations with bearer
  authorization.
- Updated `INSTALL.md` so startup handoff describes `/v1/health` as a readiness
  URL rather than a compatibility URL.
- Updated `ARCHITECTURE.md` to remove route-specific migration/compatibility
  wording and document the supported `/v1/health` and `/v1/operations` route
  surface.
- Verified with the planned `rg` checks that the edited docs no longer contain
  the stale compatibility wording or removed endpoint references, and that
  supported `/v1/health` and `/v1/operations` references remain.

Estimated effort: 7k-10k tokens.

Confidence: 95%.

Scope:

- Edit `README.md`, `INSTALL.md`, and `ARCHITECTURE.md`.
- Remove wording that says route-specific endpoints remain available during
  migration or as compatibility routes.
- Document `GET /v1/health` as a supported readiness/liveness route.
- Document `/v1/operations` as the canonical operation API for all operations.

Known stale doc locations:

- `README.md:466` through `README.md:468` list `/v1/health`, `/v1/limits`,
  `/v1/sources`, `/v1/ingest`, `/v1/search`, and `/admin/...` as compatibility
  routes.
- `INSTALL.md:105` calls `/v1/health` a compatibility URL.
- `ARCHITECTURE.md:81` through `ARCHITECTURE.md:83` call retained
  `/v1/health` migration compatibility.
- `ARCHITECTURE.md:127` through `ARCHITECTURE.md:129` say route-specific
  `/v1/...` and `/admin/...` endpoints remain available during migration.

Recommended README change:

- In the Operation API section, keep the statement that
  `POST /v1/operations` is the documented consumer operation API.
- Replace the compatibility paragraph with wording equivalent to:
  - `GET /v1/health` is a supported readiness/liveness route for operators,
    startup handoff, and the CLI's post-stream-loss probe.
  - All other consumer operations use `POST /v1/operations`.
  - Protected operations use `POST /v1/operations` with bearer authorization.

Recommended INSTALL change:

- Change "`/v1/health` compatibility URL for readiness checks" to
  "`/v1/health` readiness URL" or equivalent.

Recommended ARCHITECTURE changes:

- In Model Runtime, describe `/v1/health` as a supported readiness route that
  reports the same readiness data as the `health` operation.
- In Operation Protocol, remove route-specific compatibility wording and state
  that operations are exposed through `/v1/operations`.
- Avoid the phrase "during migration" for this API cleanup so it cannot be
  confused with the hard no-runtime-SQLite-migration rule.

Verification:

- `rg -n 'during migration|compatibility routes|compatibility URL' README.md INSTALL.md ARCHITECTURE.md`
  should show no stale matches after the phase.
- `rg -n '/v1/(limits|sources|ingest|search)|/admin/' README.md INSTALL.md ARCHITECTURE.md`
  should show no references to removed endpoints after the phase.
- `rg -n '/v1/health|/v1/operations' README.md INSTALL.md ARCHITECTURE.md`
  should show the intended supported routes.

Completion criteria:

- Operator docs no longer advertise removed routes.
- Docs clearly state that `/v1/health` remains supported.
- Docs no longer use "migration" wording for the old route-specific API
  surface.

## Phase 4: Update Config Comments

Status: Completed.

Completed summary:

- Updated stale route-specific comments in `config.example.toml` and
  `config.toml` so they refer to ingest/search operations instead of removed
  `/v1/ingest` and `/v1/search` endpoints.
- Kept all config values, config shape, parsing, defaults, and runtime behavior
  unchanged.
- Verified with the planned `rg` checks that the stale route strings are gone
  from the two config files and the replacement comments are present.

Estimated effort: 4k-6k tokens.

Confidence: 98%.

Scope:

- Edit `config.example.toml` and `config.toml`.
- Comment-only changes.
- No config values, shape, parsing, defaults, or runtime behavior changes.

Required explicit approval:

- This phase edits configuration files. Approval for code or documentation work
  does not authorize this phase.

Known stale comment locations:

- `config.example.toml:6`
  - Current: `# Maximum corpus-relative source reference length accepted by /v1/ingest.`
  - Change to: `# Maximum corpus-relative source reference length accepted by ingest operations.`
- `config.example.toml:8`
  - Current: `# Maximum query text length accepted by /v1/search.`
  - Change to: `# Maximum query text length accepted by search operations.`
- `config.example.toml:165`
  - Current: `# Default result count when /v1/search omits topK.`
  - Change to: `# Default result count when a search operation omits topK.`
- `config.toml:9`
  - Current: `# Maximum corpus-relative source reference length accepted by /v1/ingest.`
  - Change to: `# Maximum corpus-relative source reference length accepted by ingest operations.`
- `config.toml:11`
  - Current: `# Maximum query text length accepted by /v1/search.`
  - Change to: `# Maximum query text length accepted by search operations.`
- `config.toml:138`
  - Current: `# Default result count when /v1/search omits topK.`
  - Change to: `# Default result count when a search operation omits topK.`

Comments not in scope:

- Existing comments that use "legacy" or "compatibility" for Docling or the
  prior Node-backed Data Store are not part of this endpoint cleanup.

Verification:

- `rg -n '/v1/(ingest|search)' config.example.toml config.toml` should show no
  matches after the phase.
- `rg -n 'accepted by ingest operations|accepted by search operations|search operation omits topK' config.example.toml config.toml`
  should show the new comments.

Completion criteria:

- Config comments no longer point operators at removed route-specific endpoints.
- Config values are unchanged.

## Phase 5: Reference Sweep and Plan Status Update

Status: Completed.

Completed summary:

- Ran the planned reference sweeps across live code, operator docs,
  architecture docs, protocol specs, and config files.
- Confirmed no removed public endpoint strings remain for `/v1/limits`,
  `/v1/sources`, `/v1/ingest`, or `/v1/search`.
- Confirmed no removed admin endpoint strings remain for `/admin/shutdown` or
  `/admin/document-versions`.
- Confirmed stale compatibility wording is gone from the planned docs and kept
  route references still describe `/v1/health` and `/v1/operations`.
- Resolved the discovered operation-control path-template naming inconsistency
  by standardizing public references and the Rust route string on
  `/v1/operations/{operationId}/control`.
- Verified the Rust route-template edit with `cargo fmt`, `cargo check`,
  `cargo check --features metal`, and `cargo clippy`. Clippy completed with
  pre-existing warnings that remain Phase 6 cleanup scope.

Estimated effort: 3k-5k tokens.

Confidence: 96%.

Scope:

- Perform a read-only reference sweep over the approved in-scope project files.
- Update this plan's progress tracker and phase statuses only after explicit
  approval for that plan-file write.
- Do not make unplanned code, config, or documentation edits in this phase. If
  new live references are found, report them and propose a follow-up phase.

Recommended sweeps:

- Removed public routes:
  - `rg -n '/v1/(limits|sources|ingest|search)' src README.md INSTALL.md ARCHITECTURE.md config.example.toml config.toml SPEC-SERVER.md SPEC-CLIENT.md PROTOCOL.md`
- Removed admin routes:
  - `rg -n '/admin/(shutdown|document-versions)' src README.md INSTALL.md ARCHITECTURE.md config.example.toml config.toml SPEC-SERVER.md SPEC-CLIENT.md PROTOCOL.md`
- Stale compatibility wording:
  - `rg -n 'during migration|compatibility routes|compatibility URL' README.md INSTALL.md ARCHITECTURE.md`
- Kept route sanity check:
  - `rg -n '/v1/health|/v1/operations' src README.md INSTALL.md ARCHITECTURE.md PROTOCOL.md SPEC-SERVER.md SPEC-CLIENT.md`

Archival handoff note:

- `endpoint-updates.txt` contains historical discussion of the old endpoints.
  Treat it as archival unless the user explicitly asks to update or remove it.

Completion criteria:

- No removed endpoint strings remain in live code, operator docs, architecture
  docs, protocol specs, or config comments.
- Remaining `/v1/health` references describe the supported readiness route.
- Remaining `/v1/operations` references describe the canonical operation API.
- This plan file accurately reflects completed phase status after user approval
  to update it.

## Phase 6: Run Clippy Cleanup and Update Agent Instructions

Status: Not started.

Estimated effort: 25k-40k tokens.

Confidence: 80%.

To get confidence to at least 90%, first run `cargo clippy` in the current
workspace state and classify every warning by fix type, affected ownership
boundary, and whether the fix is mechanical or design-affecting.

Scope:

- Run `cargo clippy` and capture the complete warning set produced by the
  current workspace state.
- Resolve all clippy warnings produced by that run.
- Keep fixes local and mechanical where the warning clearly identifies a
  behavior-preserving change.
- Stop and seek explicit approval before any warning fix that changes
  architecture, public API, ownership boundaries, async/blocking behavior,
  configuration, persistence, protocol contracts, or diagnostic semantics.
- Do not use `#[allow(...)]` to silence warnings unless a warning is genuinely
  intentional and the reason is documented at the call site.
- After warning fixes, run the required Rust verification:
  - `cargo fmt`
  - `cargo check`
  - `cargo check --features metal`
  - `cargo clippy`
- At the end of the phase, update the agent instructions so future agents must
  address clippy warnings when those warnings are originally surfaced during
  approved verification, instead of leaving them for a later cleanup phase.

Expected agent-instruction update:

- Edit `AGENTS.md` to state that when `cargo clippy` is run as mandatory
  verification and surfaces warnings, warnings caused by the approved change
  must be fixed immediately, and pre-existing warnings surfaced during that run
  must be reported with a proposed cleanup path unless the user has approved
  resolving them in the current scope.
- Preserve the existing rule that fixes requiring design, architecture, config,
  behavior, diagnostics, async/blocking, protocol, or persistence decisions need
  explicit approval before implementation.

Verification:

- Final `cargo clippy` should complete with no warnings.
- `cargo fmt`
- `cargo check`
- `cargo check --features metal`
- Re-read the edited `AGENTS.md` section and verify it addresses warnings when
  they are originally surfaced.

Completion criteria:

- The workspace produces no clippy warnings.
- Any non-mechanical clippy fix has explicit approval before implementation.
- Agent instructions prevent newly surfaced clippy warnings from being deferred
  silently in future sessions.
