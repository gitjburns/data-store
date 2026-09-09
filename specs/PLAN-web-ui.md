# PLAN-web-ui — workflow-orchestrated implementation of SPEC-web-ui.md

Execution model for the implementing session: the main thread acts ONLY as a
workflow orchestrator. It invokes the Workflow tool with the script in §4,
relays results, and makes no file edits and no exploratory reads itself. All
implementation, verification, and fixing is done by subagents. The purpose of
subagent use is main-thread context preservation, not wall-clock speed —
phases are deliberately sequential where files are shared.

The design contract is `SPEC-web-ui.md` (approved). The app is fully offline
during implementation: no server may be started, no network verification.
Functionality between phases is not required; correctness is verified by Cargo
checks and static review only.

## 1. Approvals already granted (do not re-ask)

- Implementation of SPEC-web-ui.md as specified, including all file writes in
  §2 scope.
- Async expansion in the client binary, confined to the serve module.
- Cargo checks (`cargo fmt` / `cargo check` / `cargo clippy`) and fixing any
  errors/warnings the change introduces.
- Applying fixes for review findings confirmed by the adversarial verify phase,
  within §2 scope.

Not approved (stop and ask the user): any `Cargo.toml` change, any config file
change, any file outside §2 scope, tests, git, starting servers or the app.

## 2. File scope

| File | Action |
| --- | --- |
| `src/bin/data-store.rs` | Edit: `StartupSelection::Serve` variant, `--serve` startup parsing, `main` match arm, `mod serve;` declaration. The command registry (`COMMAND_SPECS`) is NOT touched — serve has no REPL spelling. |
| `src/bin/data-store/serve.rs` | Create: runtime + router + static-asset routes + proxy handlers + raw passthrough transport helpers. |
| `assets/web/index.html`, `assets/web/app.js`, `assets/web/style.css` | Create (new directory `assets/web/`). |
| `README.md` | Edit: short Web UI subsection only; the CLI verb table is unchanged. |
| `SPEC-CLIENT.md` | Edit: §1.2 invocation modes (serve as a third mode of the binary) + a new serve-mode section (proxy surface, asset routes). Command table (§3) unchanged. |
| `SPEC-web-ui.md` | Edit only if implementation deviates; flag deviations to the user instead where possible. |

## 3. Verified code anchors (verified 2026-08-10; line numbers are hints — re-locate by symbol/heading search before editing)

- `src/bin/data-store.rs`:
  - `ClientContext` (line ~55): `base_url: String`, `token_file_path: PathBuf`, `http: reqwest::blocking::Client`.
  - `main` (~798): matches `StartupSelection`; builds `ClientContext` from
    `load_config`, `base_url_for_bind_address`, `resolve_config_relative_path`,
    `build_http_client`.
  - `parse_startup_arguments` (~833): handles `--config` outside the registry —
    `--serve` is parsed here the same way (via `take_startup_value`), and
    `is_startup_flag` (~914) must learn `--serve`.
  - `StartupSelection` enum: today `Help` and `Client { config_path, command }`;
    gains `Serve { config_path, bind_addr }` (serve still needs `--config` for
    server address and token file path).
  - Transport helpers reachable from a submodule as `super::*`: `url`,
    `read_admin_token`, `get_public` (~1569), `get_protected` (~1581),
    `post_public_json` (~1593). All decode via `decode_success` (~1690), which
    converts non-2xx to `Err` — the proxy CANNOT reuse them and needs new raw
    variants returning `(reqwest::StatusCode, String)` verbatim for both 2xx
    and error responses (transport failures remain `Err`).
- Service patterns to mirror: `src/main.rs` ~330
  (`tokio::runtime::Builder::new_multi_thread().enable_all()` + `block_on`),
  ~905 (`tokio::net::TcpListener::bind`), ~734 (`axum::serve(listener, app)`);
  `src/http.rs` 57–86 (axum 0.8 `Router::new().route(...)`, `{param}` path
  syntax, `get`/`post` handlers).
- Dependencies (`Cargo.toml`, MUST NOT change): `axum 0.8.9`,
  `tokio 1.52.3` features `rt-multi-thread, net, sync` — NO `signal` feature,
  so serve mode does no signal handling (process terminates on Ctrl-C),
  `reqwest 0.12` blocking+json+rustls.
- Service routes (verified against `src/http.rs` and client call sites):
  `POST /query`, `GET /v1/health`, `GET /units/{unitId}`,
  `GET /units/{unitId}/relationships?direction=&relationshipType=`,
  `GET /sources/{sourceId}`, `GET /sync/status`, `GET /parses?status=held`,
  `GET /operations/{operationId}`,
  `GET /annotations/vocabulary?annotationType=&scope=`.
- `PROTOCOL.md` response-shape sections (for UI field names): `/query` line
  ~247, `/units` ~435, relationships ~490, sources ~541, sync ~595, health
  ~183, parses ~846, operations ~922, vocabulary ~964. Health is the one
  non-camelCase response.
- `SPEC-CLIENT.md`: §1.2 "Two modes" heading ~48; command table §3 ~135
  (unchanged by this work).
- `README.md`: CLI verb table ~244–268 (unchanged); add the Web UI subsection
  after the "Getting started" CLI material.
- Asset embedding: `include_str!("../../../assets/web/<file>")` from
  `src/bin/data-store/serve.rs`.

## 4. Workflow script

Invoke via the Workflow tool with this script verbatim. If a phase returns
`blocked`, stop the workflow, report to the user, and wait.

```js
export const meta = {
  name: 'web-ui-implementation',
  description: 'Implement SPEC-web-ui.md serve mode, proxy, and web assets with adversarial verification',
  phases: [
    { title: 'Rust', detail: 'serve mode + proxy in the client binary' },
    { title: 'Scaffold', detail: 'asset skeleton: html, css, app.js core' },
    { title: 'Views', detail: 'sequential view agents over app.js' },
    { title: 'Docs', detail: 'README + SPEC-CLIENT updates' },
    { title: 'Gate', detail: 'cargo fmt/check/clippy' },
    { title: 'Review', detail: 'parallel reviewers incl. adversarial' },
    { title: 'Verify', detail: 'per-finding adversarial votes' },
    { title: 'Fix', detail: 'apply confirmed findings per file' },
    { title: 'Final', detail: 'cargo gate + conformance sweep' },
  ],
}

// Repo rules digest prepended to every implementing agent.
const RULES = `
You are implementing approved scope in /Users/goon/project/service/data-store.
Read PLAN-web-ui.md sections 1-3 and SPEC-web-ui.md FIRST; they are the
contract. Hard rules: stay inside the repo; never touch Cargo.toml, config
files, or files outside PLAN-web-ui.md section 2; no tests; no git; never start
the server or the app; no new dependencies. Rust: every function gets a
purpose/invariant comment (doc comments for public items); comment non-obvious
invariants in the same patch; no unwrap/expect in runtime paths; explicit
Result handling preserving source context; cargo fmt/check/clippy are allowed
and expected after Rust edits. Patch hygiene: read the exact region before
editing; re-read after each edit to the same file; anchor edits on unique
context. Your final message is data for the orchestrator, not prose for a
human: report files touched, what was built, and any deviation from spec.
If you cannot proceed within these rules, return the word BLOCKED plus why.`

const FINDINGS = {
  type: 'object', required: ['findings'],
  properties: { findings: { type: 'array', items: {
    type: 'object', required: ['file', 'summary', 'failureScenario', 'severity'],
    properties: {
      file: { type: 'string' }, line: { type: 'integer' },
      summary: { type: 'string' }, failureScenario: { type: 'string' },
      severity: { enum: ['critical', 'major', 'minor'] },
    } } } } }

const VERDICT = {
  type: 'object', required: ['refuted', 'reason'],
  properties: { refuted: { type: 'boolean' }, reason: { type: 'string' } } }

phase('Rust')
const rust = await agent(`${RULES}
Implement SPEC-web-ui.md section 1 and 2 (serve mode + proxy) exactly, using
the anchors in PLAN-web-ui.md section 3:
1. src/bin/data-store.rs: add StartupSelection::Serve { config_path, bind_addr }
   (bind_addr: std::net::SocketAddr); parse --serve <host>:<port> in
   parse_startup_arguments beside --config via take_startup_value; teach
   is_startup_flag the flag; reject combining --serve with an operation flag;
   match the new variant in main, building ClientContext exactly as the Client
   arm does, then calling the serve module. Add "mod serve;" and cli help text
   for --serve in render_cli_help. Do NOT touch COMMAND_SPECS.
2. Create src/bin/data-store/serve.rs: build a multi-thread tokio runtime
   (mirror src/main.rs ~330) and block_on an axum server (mirror ~905/~734).
   Router: GET / -> embedded index.html, GET /app.js, GET /style.css (correct
   content types, include_str! from assets/web/ — the Scaffold phase creates
   those files after you; the include paths must match PLAN section 3).
   Proxy routes per the SPEC section 2 table under /api/*, forwarding to the
   service via NEW raw passthrough helpers (blocking reqwest inside
   spawn_blocking, never on a runtime thread): public GET, public POST (json
   body passthrough), protected GET (bearer via super::read_admin_token, token
   read fresh per call). Passthrough returns the service status code and body
   verbatim (application/json); transport failure maps to 502 with a JSON
   error body naming the failed hop. Forward query parameters verbatim on the
   routes that take them. Log one line per proxied request (method, path,
   upstream status) and a startup line with the bound URL.
Then run cargo fmt, cargo check, cargo clippy and fix what your change caused.
Return: files touched, helper names created, router paths, any deviation.`,
  { label: 'rust:serve-mode' })
if (String(rust).includes('BLOCKED')) return { blocked: 'Rust', detail: rust }

phase('Scaffold')
const scaffold = await agent(`${RULES}
Create assets/web/index.html, assets/web/style.css, and the SKELETON of
assets/web/app.js per SPEC-web-ui.md sections 3-4. Vanilla JS, no framework,
no build step. index.html: nav shell with links to the views (#/query,
#/unit/<id>, #/source/<id>, #/health, #/sync, #/held, #/operation, #/vocab)
and a #main mount point. app.js skeleton: hash router with a view registry,
an api() fetch wrapper over /api/* that surfaces non-2xx service error bodies
to the caller, an esc() HTML-escaping helper (ALL service-derived text must
pass through it before DOM insertion), shared renderers (key/value table,
collapsible JSON tree, unitId/sourceId link helpers), and stub registrations
for every view so routing works end to end. style.css: clean readable
defaults, anchor-vs-context evidence styling hooks.
Prior phase built the Rust side; route paths are the SPEC section 2 proxy
table. Return: the view-registration contract (exact function signature and
registry call a later agent must use to add a view), helper names, and file
sizes.`, { label: 'scaffold:assets' })
if (String(scaffold).includes('BLOCKED')) return { blocked: 'Scaffold', detail: scaffold }

phase('Views')
// Sequential on purpose: all four edit assets/web/app.js. Each prompt carries
// the scaffold's registration contract; PROTOCOL.md sections give field names.
const VIEWS = [
  ['views:query', `the query console, assembly-trace panel, and debug
   diagnostics panel per SPEC-web-ui.md section 4 bullets 1-3. PROTOCOL.md
   POST /query (~line 247) defines every field. Anchor units (score present)
   must be visually distinct from context units; every unitId/sourceId
   rendered is a navigation link; the trace and debug panels render only when
   present. Request builder must support constraints, maxFinalEvidenceUnits,
   evidence toggles, and debug.`],
  ['views:explorer', `the unit explorer (unit detail + relationships with
   direction/type filters, related units clickable) and the source view
   (locations, freshness, active parse). PROTOCOL.md ~435/~490/~541 define
   the shapes.`],
  ['views:dashboards', `the health dashboard (readiness, per-component typed
   details, counts each labeled with its as_of; note /v1/health is the one
   non-camelCase response, PROTOCOL.md ~183) and the sync status view
   (~595).`],
  ['views:admin-reads', `the held-parses list (~846), the operation viewer
   (single fetch of one operationId with a manual refresh control, ~922), and
   the vocabulary explorer (entity/relation toggle, scope active/all, ~964).
   These call the bearer-backed proxy routes; render a service 401/403 error
   body readably rather than treating it as a transport fault.`],
]
for (const [label, task] of VIEWS) {
  const done = await agent(`${RULES}
Edit assets/web/app.js (and style.css if needed) to implement ${task}
Follow the existing scaffold contract exactly as reported by the previous
agents: ${JSON.stringify(scaffold)}. Read the current app.js fully before
editing; replace the relevant stub registration; do not restructure other
views. All service text goes through esc(). Return: functions added, stub
replaced, any spec deviation.`, { label, phase: 'Views' })
  if (String(done).includes('BLOCKED')) return { blocked: label, detail: done }
}

phase('Docs')
const docs = await parallel([
  () => agent(`${RULES}
Edit README.md only: add a short "Web UI" subsection after the Getting
started CLI material describing data-store --serve <host>:<port> (startup
mode, not a verb — the verb table is UNCHANGED), what the UI offers, and that
v1 is read-only. Durable-doc rule: contract only, no process commentary.`,
    { label: 'docs:readme', phase: 'Docs' }),
  () => agent(`${RULES}
Edit SPEC-CLIENT.md only: update section 1.2 so serve is a third invocation
mode of the binary (no REPL spelling, no command-table entry), and add a
serve-mode section specifying the proxy surface and asset routes, consistent
with SPEC-web-ui.md section 2-3. Locate headings by search, not memory.`,
    { label: 'docs:spec-client', phase: 'Docs' }),
])
if (docs.some(d => String(d).includes('BLOCKED'))) return { blocked: 'Docs', detail: docs }

phase('Gate')
const gate = await agent(`${RULES}
Run cargo fmt, cargo check, cargo clippy for this repo. Fix only issues caused
by the web-ui change (src/bin/data-store.rs, src/bin/data-store/serve.rs).
Pre-existing warnings: report, do not fix. Return: PASS or the remaining
errors, plus anything you fixed.`, { label: 'gate:cargo' })
if (!String(gate).includes('PASS')) return { blocked: 'Gate', detail: gate }

phase('Review')
const REVIEWERS = [
  ['review:adherence', `Review the web-ui implementation (git-free: read the
   files in PLAN-web-ui.md section 2) for adherence to AGENTS.md and
   PRINCIPLES.md: function comments present and non-restating, no
   unwrap/expect in runtime paths, error context preserved, no scope creep
   beyond SPEC-web-ui.md, external artifacts in dedicated files, COMMAND_SPECS
   untouched.`],
  ['review:spec-conformance', `Verify the implementation against SPEC-web-ui.md
   clause by clause: exact proxy route table (paths, params, auth class),
   lossless status+body passthrough, asset routes and embedding, all section-4
   views present and wired, startup-mode (not registry) integration, docs
   obligations met in README.md and SPEC-CLIENT.md.`],
  ['review:adversarial-rust', `Adversarially hunt for real defects in
   src/bin/data-store.rs (startup parsing changes) and
   src/bin/data-store/serve.rs: blocking reqwest reachable on a runtime
   thread, panics/unwraps, error paths that drop the service status or body,
   query params not forwarded, wrong content types, include_str! path
   mismatches, --serve arg-parsing edge cases (missing value, combined with
   operation flags, invalid socket addr). Report only defects with a concrete
   failure scenario.`],
  ['review:adversarial-js', `Adversarially review assets/web/*.js/html against
   PROTOCOL.md response shapes: wrong/missing field names (camelCase
   everywhere EXCEPT /v1/health), fields documented as omitted-when-absent
   dereferenced unconditionally, service-derived text inserted into the DOM
   without esc() (XSS), broken hash routes or dead links, fetch error paths
   that swallow the service error envelope. Report only defects with a
   concrete failure scenario.`],
]
const reviews = await parallel(REVIEWERS.map(([label, task]) => () =>
  agent(`${RULES}\nYou are a read-only reviewer; make NO edits. ${task}`,
    { label, phase: 'Review', schema: FINDINGS })))
const allFindings = reviews.filter(Boolean).flatMap(r => r.findings)
// Barrier + dedup is deliberate: verification is per-finding and expensive.
const seen = new Set()
const unique = allFindings.filter(f => {
  const key = `${f.file}:${f.summary.toLowerCase().slice(0, 60)}`
  if (seen.has(key)) return false
  seen.add(key); return true
})
log(`${allFindings.length} raw findings, ${unique.length} unique`)

phase('Verify')
const verified = await parallel(unique.map(f => () =>
  parallel([0, 1, 2].map(i => () =>
    agent(`Read-only skeptic pass ${i} in /Users/goon/project/service/data-store.
Try to REFUTE this finding against the actual code; default refuted=true if
you cannot demonstrate the failure scenario from the code as written.
Finding: ${JSON.stringify(f)}`,
      { label: `verify:${f.file.split('/').pop()}`, phase: 'Verify', schema: VERDICT, effort: 'low' })))
    .then(votes => ({ ...f, confirmed: votes.filter(Boolean).filter(v => !v.refuted).length >= 2 }))))
const confirmed = verified.filter(Boolean).filter(f => f.confirmed)
log(`${confirmed.length}/${unique.length} findings confirmed`)

phase('Fix')
// Group by file; sequential to avoid same-file collisions.
const byFile = new Map()
for (const f of confirmed) {
  if (!byFile.has(f.file)) byFile.set(f.file, [])
  byFile.get(f.file).push(f)
}
for (const [file, findings] of byFile) {
  const fixed = await agent(`${RULES}
Fix these CONFIRMED review findings in ${file}. Read the file region first;
smallest correct fix per finding; add/update comments where the fix embodies a
non-obvious rule. Findings: ${JSON.stringify(findings)}
Return per finding: fixed | no_change_needed (with justification).`,
    { label: `fix:${file.split('/').pop()}`, phase: 'Fix' })
  if (String(fixed).includes('BLOCKED')) return { blocked: 'Fix', detail: fixed }
}

phase('Final')
const finalGate = await agent(`${RULES}
Run cargo fmt, cargo check, cargo clippy; fix only what the web-ui change
caused. Then do a short closing sweep: SPEC-web-ui.md clause list vs the
implementation, and confirm no file outside PLAN-web-ui.md section 2 changed.
Return PASS plus a one-paragraph completion summary, or the failures.`,
  { label: 'final:gate' })
return {
  rust, scaffoldContract: scaffold,
  findings: { raw: allFindings.length, unique: unique.length, confirmed: confirmed.length },
  confirmedFindings: confirmed, finalGate,
}
```

## 5. Residual risk (report to user at completion)

Browser-side behavior has no offline verification path: field-name and
escaping review is static. First `data-store --serve` run by the user is the
real verification; expect polish findings there.
