# SPEC-WEB-UI — the client-served read-only web UI

Status: approved design, v1 not yet implemented. The web UI is a presentation
feature of the bundled `data-store` client (`SPEC-CLIENT.md`); the server and
`PROTOCOL.md` are unchanged by it. v1 is read-only: the UI exposes no mutating
route and carries no UI-level authentication.

## 1. Serve mode

- `--serve <host>:<port>` is a startup mode of the client binary, parsed
  alongside `--config` as a `StartupSelection` variant. It is NOT a command
  registry entry and has no REPL spelling; the REPL never touches serve. The
  argument must resolve to a socket address: an IP literal or a resolvable
  hostname; the first resolved address is bound.
- Serve mode runs a local HTTP server in the foreground until the process is
  terminated (Ctrl-C; no signal handling of its own), logging the bound URL to
  stdout.
- Implementation lives in a client submodule (`src/bin/data-store/serve.rs`)
  hosting an axum `Router` on a tokio runtime. Async is confined to this module
  (user-approved async expansion, serve mode only). All existing client
  functions remain blocking; proxy handlers reach the blocking `reqwest`
  transport through `spawn_blocking` — blocking reqwest must never run on a
  runtime thread.
- Serve mode shares the client's `ClientContext` (base URL, admin token file
  path, HTTP client). No configuration additions.

## 2. Proxy API

Browser JS calls same-origin `/api/*`; the proxy forwards to the service using
the client's existing transport and returns the service's JSON body and status
code verbatim. The proxy never narrows, reshapes, or summarizes a service
response — presentation is the browser's job.

The route set is a fixed allowlist; there is no generic passthrough. The
allowlist is the read-only guarantee: no mutating service route is reachable
through the proxy in v1.

| Proxy route | Service route | Auth |
| --- | --- | --- |
| `POST /api/query` | `POST /query` (full request envelope forwarded) | public |
| `GET /api/health` | `GET /v1/health` | public |
| `GET /api/units/{unitId}` | `GET /units/{unitId}` | public |
| `GET /api/units/{unitId}/relationships` (`direction`, `relationshipType` params) | `GET /units/{unitId}/relationships` | public |
| `GET /api/sources/{sourceId}` | `GET /sources/{sourceId}` | public |
| `GET /api/sync-status` | `GET /sync/status` | public |
| `GET /api/held-parses` | `GET /parses?status=held` | bearer |
| `GET /api/operations/{operationId}` | `GET /operations/{operationId}` | bearer |
| `GET /api/vocabulary` (`annotationType`, `scope` params) | `GET /annotations/vocabulary` | bearer |

Bearer routes authenticate with the admin token file the client already reads
(`SPEC-CLIENT.md` §1.5). Service error envelopes pass through with their
original status codes.

## 3. Static assets

- Files: `assets/web/index.html`, `assets/web/app.js`, `assets/web/style.css` —
  dedicated files per the external-language-artifacts rule, embedded into the
  binary with `include_str!`.
- Routes: `GET /` serves `index.html`; `GET /app.js` and `GET /style.css` serve
  their files with correct content types.
- Vanilla JS + `fetch`, hash-based routing, no framework, no build step, no
  Node toolchain.

## 4. UI views

- **Query console** (the primary view): query text, constraints (`sourceIds`,
  `governanceDomains`), `maxFinalEvidenceUnits`, evidence-policy toggles, debug
  toggle. Primary results are ranked server-supplied passages (`results`), with
  source locations and status, section path, full passage text, and an
  explicit marker when the server truncated the passage. Missing source
  information is labelled unavailable.
- **Retrieval provenance** appears on each passage without requiring debug.
  Server-supplied channels are labelled Semantic search, Keyword search, and
  Annotations. Annotation contribution distinguishes no annotation matches,
  overlap with other candidate lists, and additional candidate matches; it does
  not claim annotations improved relevance. Previews show up to three distinct
  annotation explanations and three dense-match descriptions. Annotation paths
  distinguish direct entity matches, relationship evidence, and related entity
  mentions while preserving relationship direction. Dense descriptions
  distinguish direct passage, section-guided, and document-scoped context matches.
  Missing provenance is explicitly unavailable.
- **Retrieval details** expands each passage's complete per-unit channels,
  graph and dense matches, relationship-support links, context-unit links, and
  provenance JSON. Context units receive no inferred retrieval match. Canonical
  section IDs link to the unit explorer; section-window and chunk IDs remain
  text because they have no detail endpoint.
- **Evidence and diagnostics** expands the evidence pack, assembly trace, debug
  diagnostics when requested, and complete response JSON. Evidence units retain
  role/content-type badges, scores for anchors, context inclusion reasons,
  text projections or labelled summaries derived from the typed bodies
  (SPEC-epub.md §2.2), and raw body/provenance details. Unit and source IDs
  link to their respective views. The assembly trace shows policy
  id/version/hash, rules grouped by anchor, rejected hits/units, and budget.
  Debug diagnostics show per-stage latency, complete eligible channel
  membership (`channelHits`), the fused candidate pool labelled
  with its single **representative channel**, MaxSim, and reranker scores.
- **Unit explorer**: unit detail (typed body per SPEC-epub.md §2.2, `dom_path`
  or `char_range` locators) plus relationships with direction/type filters;
  related units are click-through links.
- **Source view**: locations, freshness, active parse.
- **Health dashboard**: readiness, per-component details, and counts each
  shown with its `as_of` label.
- **Sync status**, **held-parses list**, **operation viewer** (manual-refresh
  poll), **vocabulary explorer** (entity/relation, scope toggle).

## 5. Documentation obligations

Implementation must update: a short README Web UI subsection (serve is not a
CLI verb, so the verb table is unchanged); SPEC-CLIENT.md §1.2 (invocation modes — serve is a third mode of
the binary, not a command-table entry) and a section covering serve mode's
proxy surface.

## 6. Verification

Rust code is covered by the mandatory Cargo checks. The HTML/JS has no
compile-time verification; browser-side behavior is verified by running
`data-store --serve` and exercising each view.
