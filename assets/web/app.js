// data-store web UI (SPEC-web-ui.md §3-§4).
//
// Vanilla ES2020, no framework and no build step: this file is embedded in the
// client binary with include_str! and served verbatim at /app.js.
//
// Structure, in order: escaping, API access, shared renderers, the hash router
// and its view registry, then one registration per view. The hard rule for every
// view is that ALL service-derived text passes through esc() before it reaches
// the DOM — the service stores arbitrary document text, so unescaped
// interpolation is an XSS hole, not a style question.

'use strict';

// ---------------------------------------------------------------------------
// Escaping
// ---------------------------------------------------------------------------

/**
 * HTML-escape any value for insertion into element content or a quoted
 * attribute. null/undefined render as the empty string so callers can inline
 * omitted-when-absent protocol fields without a guard; non-strings are
 * stringified first. Both quote characters are escaped because the return value
 * is also used inside attributes.
 */
function esc(value) {
  if (value === null || value === undefined) return '';
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

/**
 * Render a value that may be absent, substituting a visible placeholder rather
 * than blank space. Protocol fields documented as "omitted when absent" are the
 * intended callers. The result is escaped.
 */
function escOr(value, placeholder = '—') {
  if (value === null || value === undefined || value === '') {
    return `<span class="absent">${esc(placeholder)}</span>`;
  }
  return esc(value);
}

// ---------------------------------------------------------------------------
// API access
// ---------------------------------------------------------------------------

/**
 * A failed /api/* call. Carries the service's own error envelope
 * (PROTOCOL.md "Error body": status/kind/message) when one was returned, so
 * views can render a 401/403/404 from the service as data instead of treating it
 * as a transport fault. `status` is 0 only when the browser could not reach the
 * local proxy at all.
 */
class ApiError extends Error {
  constructor(status, kind, message, body) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.kind = kind;
    this.body = body;
  }
}

/**
 * Call a same-origin proxy route and return the parsed JSON body.
 *
 * Invariant: a non-2xx response NEVER resolves. It throws an ApiError carrying
 * the service's verbatim error envelope, because the proxy passes service status
 * codes and bodies through unchanged (SPEC-web-ui.md §2) and the UI must show
 * what the service actually said. A 2xx with an empty body resolves to null.
 */
async function api(path, options = {}) {
  let response;
  try {
    response = await fetch(path, options);
  } catch (cause) {
    throw new ApiError(0, 'proxy_unreachable', `${path}: ${cause.message}`, null);
  }
  const raw = await response.text();
  let body = null;
  if (raw !== '') {
    try {
      body = JSON.parse(raw);
    } catch (cause) {
      body = null;
      if (response.ok) {
        throw new ApiError(response.status, 'invalid_response', `${path}: response was not JSON`, raw);
      }
    }
  }
  if (!response.ok) {
    const envelope = body && typeof body === 'object' ? body.error : null;
    const kind = (envelope && envelope.kind) || 'unknown_error';
    const message = (envelope && envelope.message) || raw || response.statusText;
    throw new ApiError(response.status, kind, message, body);
  }
  return body;
}

/** GET a proxy route, appending `params` as a query string when non-empty. */
function apiGet(path, params) {
  return api(path + queryString(params));
}

/** POST a JSON body to a proxy route; the envelope is forwarded as-is. */
function apiPost(path, body) {
  return api(path, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
}

/**
 * Build a `?a=b` string from a plain object, dropping entries whose value is
 * null, undefined, or empty — an omitted filter must not become an empty-string
 * filter on the service side.
 */
function queryString(params) {
  if (!params) return '';
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined || value === '') continue;
    search.set(key, String(value));
  }
  const rendered = search.toString();
  return rendered === '' ? '' : `?${rendered}`;
}

// ---------------------------------------------------------------------------
// Shared renderers
//
// Every renderer returns an HTML string rather than a node: views compose one
// string and hand it to setMain(), which is the only place that assigns
// innerHTML. Renderers escape the values they are given; a parameter documented
// as "markup" is trusted and must already be built from escaped parts.
// ---------------------------------------------------------------------------

/**
 * Two-column detail table.
 *
 * Rows are `{ label, value }` (value is escaped text, absent values become the
 * placeholder) or `{ label, html }` (already-built markup, e.g. a link or a
 * nested table). Rows that are null/undefined are skipped so callers can inline
 * conditional fields.
 */
function kvTable(rows) {
  const cells = (rows || [])
    .filter(Boolean)
    .map((row) => {
      const value = 'html' in row ? row.html : escOr(row.value);
      return `<tr><th scope="row">${esc(row.label)}</th><td>${value}</td></tr>`;
    })
    .join('');
  if (cells === '') return '';
  return `<table class="kv">${cells}</table>`;
}

/**
 * Collapsible JSON tree.
 *
 * Uses <details>/<summary> so expansion needs no JavaScript state and survives
 * a re-render. `open` controls the depth that starts expanded (default: the root
 * only). Keys and scalars are escaped; cycles are impossible because the input
 * is always a freshly parsed service response.
 */
function jsonTree(value, label = 'JSON', openDepth = 1) {
  return `<div class="json-tree">${jsonNode(label, value, 0, openDepth)}</div>`;
}

/** Render one JSON node; recursion helper for jsonTree(). */
function jsonNode(key, value, depth, openDepth) {
  const isArray = Array.isArray(value);
  const isObject = value !== null && typeof value === 'object' && !isArray;
  if (!isArray && !isObject) {
    return `<div class="json-leaf"><span class="json-key">${esc(key)}</span>: ${jsonScalar(value)}</div>`;
  }
  const entries = isArray
    ? value.map((item, index) => [String(index), item])
    : Object.entries(value);
  const summary = isArray ? `${esc(key)} [${entries.length}]` : `${esc(key)} {${entries.length}}`;
  const children = entries
    .map(([childKey, childValue]) => jsonNode(childKey, childValue, depth + 1, openDepth))
    .join('');
  const open = depth < openDepth ? ' open' : '';
  return `<details class="json-branch"${open}><summary>${summary}</summary>${children}</details>`;
}

/** Render a JSON scalar with a type class for styling; always escaped. */
function jsonScalar(value) {
  if (value === null) return '<span class="json-null">null</span>';
  const type = typeof value;
  if (type === 'number' || type === 'boolean') {
    return `<span class="json-${type}">${esc(value)}</span>`;
  }
  return `<span class="json-string">${esc(value)}</span>`;
}

/** Link to any in-app route; `hash` must already include its leading `#/`. */
function routeLink(hash, text, className = '') {
  const cls = className === '' ? '' : ` class="${esc(className)}"`;
  return `<a${cls} href="${esc(hash)}">${esc(text)}</a>`;
}

/**
 * Link to the unit explorer. The id is percent-encoded into the hash so ids
 * containing `/` or `#` cannot break the route; the router decodes it back.
 */
function unitLink(unitId, text) {
  if (unitId === null || unitId === undefined || unitId === '') return escOr(unitId);
  return routeLink(`#/unit/${encodeURIComponent(unitId)}`, text || unitId, 'id-link');
}

/** Link to the source view; ids are percent-encoded exactly as unitLink(). */
function sourceLink(sourceId, text) {
  if (sourceId === null || sourceId === undefined || sourceId === '') return escOr(sourceId);
  return routeLink(`#/source/${encodeURIComponent(sourceId)}`, text || sourceId, 'id-link');
}

/** Link to the operation viewer; ids are percent-encoded exactly as unitLink(). */
function operationLink(operationId, text) {
  if (operationId === null || operationId === undefined || operationId === '') return escOr(operationId);
  return routeLink(`#/operation/${encodeURIComponent(operationId)}`, text || operationId, 'id-link');
}

/** Titled content block; `markup` is trusted and must already be escaped. */
function panel(title, markup, className = '') {
  const cls = className === '' ? 'panel' : `panel ${esc(className)}`;
  return `<section class="${cls}"><h2>${esc(title)}</h2>${markup}</section>`;
}

/** Small labelled tag, e.g. an evidence role or a readiness state. */
function badge(text, className = '') {
  const cls = className === '' ? 'badge' : `badge ${esc(className)}`;
  return `<span class="${cls}">${esc(text)}</span>`;
}

/**
 * Render a failure for the user.
 *
 * An ApiError from the service is shown as its envelope (status/kind/message)
 * because that is real service data, not a client fault; anything else is shown
 * as a client-side error. Used by the router for uncaught view failures and by
 * views that catch their own.
 */
function errorPanel(error) {
  if (error instanceof ApiError) {
    const statusLabel = error.status === 0 ? 'no response' : String(error.status);
    return panel(
      'Request failed',
      kvTable([
        { label: 'status', value: statusLabel },
        { label: 'kind', value: error.kind },
        { label: 'message', value: error.message },
      ]),
      'panel-error'
    );
  }
  return panel('Error', `<p class="error-message">${esc(error && error.message ? error.message : error)}</p>`, 'panel-error');
}

/** Placeholder shown while a view's requests are in flight. */
function loadingMarkup(what = 'Loading') {
  return `<p class="loading">${esc(what)}…</p>`;
}

/** Single-field lookup form markup for views reachable without an id. */
function lookupForm(formId, label, placeholder) {
  return `<form class="lookup" id="${esc(formId)}">
    <label for="${esc(formId)}-input">${esc(label)}</label>
    <input id="${esc(formId)}-input" name="id" type="text" placeholder="${esc(placeholder)}" autocomplete="off" />
    <button type="submit">Open</button>
  </form>`;
}

// ---------------------------------------------------------------------------
// Router and view registry
// ---------------------------------------------------------------------------

/** Registered views keyed by their first hash segment. */
const views = new Map();

/**
 * Register a view under its route name (the first hash segment, e.g. `unit` for
 * `#/unit/<id>`).
 *
 * `view` is `{ title, params, render }`:
 *   - `title`  — document/page title for the route.
 *   - `params` — ordered names bound to the hash segments after the route name;
 *                a segment the URL omits binds to undefined, so a view reachable
 *                both with and without an id handles the undefined case.
 *   - `render` — `async (mount, params) => void`. It owns `mount`'s content and
 *                must write only inside it. Throwing is allowed: the router
 *                renders the error. All service text must pass through esc().
 *
 * Registering the same name twice replaces the earlier view; the last
 * registration for a name wins.
 */
function registerView(name, view) {
  views.set(name, view);
}

/**
 * Split `location.hash` into a route name and its decoded segments.
 * An empty or malformed hash resolves to the default route.
 */
function parseHash(hash) {
  const raw = String(hash || '').replace(/^#\/?/, '');
  const segments = raw.split('/').filter((segment) => segment !== '');
  const decoded = segments.map((segment) => {
    try {
      return decodeURIComponent(segment);
    } catch (cause) {
      // A hand-edited hash can hold an invalid escape; the raw segment is the
      // most faithful thing left to show.
      return segment;
    }
  });
  if (decoded.length === 0) return { name: DEFAULT_ROUTE, args: [] };
  return { name: decoded[0], args: decoded.slice(1) };
}

const DEFAULT_ROUTE = 'query';

/**
 * Renders that are superseded before their awaits finish must not write to the
 * visible page. The token orders renders so the router can discard a stale
 * render's error; stale success writes are neutralised by setMain() detaching
 * the mount each render owns.
 */
let renderToken = 0;

/**
 * Install `markup` as the page's main content and return the element that owns
 * it — the mount every view writes into.
 *
 * Invariant (the stale-render guard views rely on): each call builds a *new*
 * <main> element and swaps it for the one in the document, so the element a
 * render captured stops being part of the page the moment a later render
 * starts. Views therefore write `mount.innerHTML` after their awaits without a
 * token check: a superseded render updates a detached element nobody sees, and
 * the handlers it binds are bound inside that dead subtree. Views that write to
 * a container looked up by id (document.getElementById) do not get this
 * protection — those call sites carry their own post-await route check.
 *
 * The replacement copies id and class so the document keeps the structure
 * index.html declares (`<main id="main" class="app-main">`) and the stylesheet
 * targets.
 */
function setMain(markup) {
  const current = document.getElementById('main');
  if (!current) return null;
  const fresh = document.createElement('main');
  fresh.id = current.id;
  fresh.className = current.className;
  fresh.innerHTML = markup;
  current.replaceWith(fresh);
  return fresh;
}

/** Mark the nav link whose data-route matches the active route. */
function markActiveNav(name) {
  for (const link of document.querySelectorAll('.app-nav a[data-route]')) {
    link.classList.toggle('active', link.dataset.route === name);
  }
}

/** Navigate to another in-app route; the hashchange listener does the render. */
function navigate(hash) {
  if (window.location.hash === hash) {
    renderCurrentRoute();
    return;
  }
  window.location.hash = hash;
}

/** Resolve the current hash to a view and render it into the mount. */
async function renderCurrentRoute() {
  const { name, args } = parseHash(window.location.hash);
  const view = views.get(name);
  markActiveNav(view ? name : '');
  if (!view) {
    document.title = 'data-store — not found';
    setMain(panel('Unknown route', `<p>No view is registered for <code>${esc(name)}</code>.</p>`, 'panel-error'));
    return;
  }
  document.title = `data-store — ${view.title}`;
  const params = {};
  (view.params || []).forEach((paramName, index) => {
    params[paramName] = args[index];
  });
  const token = ++renderToken;
  const mount = setMain(loadingMarkup());
  if (!mount) return;
  try {
    await view.render(mount, params);
  } catch (error) {
    // The error panel goes through setMain(), which writes to whatever <main>
    // is currently installed — so unlike a view's own mount write it is not
    // self-neutralising and must be gated on the token.
    if (token !== renderToken) return;
    setMain(errorPanel(error));
  }
}

/** Wire the router to the address bar and perform the first render. */
function startRouter() {
  window.addEventListener('hashchange', () => {
    renderCurrentRoute();
  });
  if (window.location.hash === '') {
    window.location.hash = `#/${DEFAULT_ROUTE}`;
    return; // The hash assignment fires hashchange, which renders.
  }
  renderCurrentRoute();
}

// ---------------------------------------------------------------------------
// Views
//
// One registerView() call per route, each preceded by the section comment naming
// the SPEC-web-ui.md §4 bullet it implements and the PROTOCOL.md response shape
// it reads.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Query console (SPEC-web-ui.md §4 bullets 1-3; PROTOCOL.md "POST /query")
//
// Three panels, all fed by one POST /api/query response: the evidence pack, the
// assembly trace, and — only when the request set debug — the raw stage
// diagnostics. The anchor/context discriminator is score presence: PROTOCOL.md
// omits `score` for units the assembler added as context, so a unit carrying a
// score is an anchor and everything else was pulled in by an assembly rule.
// ---------------------------------------------------------------------------

/**
 * Request-builder state, kept at module scope because the router rebuilds the
 * view from scratch on every hash change: without this, following a unit link
 * out of a result and coming back would lose both the form and the answer.
 * Field values are the raw string/boolean form inputs, not the request envelope;
 * `includeSourceLocators` starts at the protocol default (true), the other two
 * evidence toggles at false.
 */
const queryForm = {
  queryText: '',
  sourceIds: '',
  governanceDomains: '',
  maxFinalEvidenceUnits: '',
  includeSourceLocators: true,
  includeRelationships: false,
  includeAnnotations: false,
  debug: false,
};

/**
 * Outcome of the most recent submission: `idle` (never run), `running`,
 * `done` (`response` retains the complete parsed body, including results), or
 * `error` (`error` holds an ApiError from the service or a local validation
 * Error). Updated even when the user has navigated away, so returning to the
 * console shows the finished result.
 */
let queryRun = { status: 'idle', response: null, error: null };

/** Render the console into `mount` and bind the form's submit handler. */
function renderQueryConsole(mount) {
  mount.innerHTML = queryFormMarkup() + queryResultMarkup();
  const form = mount.querySelector('#query-form');
  if (!form) return;
  form.addEventListener('submit', (event) => {
    // The form has no action: submission is an API call, never a page load.
    event.preventDefault();
    submitQuery(mount);
  });
}

/**
 * Read the form back into `queryForm`, build the request envelope, run the
 * query, and re-render. A local validation failure is stored as the run's error
 * so it renders in the same place a service error would.
 *
 * Invariant: the state is updated before the route check, so an answer that
 * arrives after the user navigated away is still there on return; only the DOM
 * write is skipped when the console is no longer the active view.
 */
async function submitQuery(mount) {
  const form = mount.querySelector('#query-form');
  if (form) readQueryForm(form);
  let request;
  try {
    request = buildQueryRequest(queryForm);
  } catch (error) {
    queryRun = { status: 'error', response: null, error };
    renderQueryConsole(mount);
    return;
  }
  queryRun = { status: 'running', response: null, error: null };
  renderQueryConsole(mount);
  try {
    const response = await apiPost('/api/query', request);
    queryRun = { status: 'done', response, error: null };
  } catch (error) {
    queryRun = { status: 'error', response: null, error };
  }
  if (parseHash(window.location.hash).name !== 'query') return;
  const current = document.getElementById('main');
  if (current) renderQueryConsole(current);
}

/** Copy the live form controls into `queryForm`; missing controls keep their prior value. */
function readQueryForm(form) {
  const text = (name) => {
    const field = form.elements.namedItem(name);
    return field && typeof field.value === 'string' ? field.value : queryForm[name];
  };
  const checked = (name) => {
    const field = form.elements.namedItem(name);
    return field && 'checked' in field ? field.checked : queryForm[name];
  };
  queryForm.queryText = text('queryText');
  queryForm.sourceIds = text('sourceIds');
  queryForm.governanceDomains = text('governanceDomains');
  queryForm.maxFinalEvidenceUnits = text('maxFinalEvidenceUnits');
  queryForm.includeSourceLocators = checked('includeSourceLocators');
  queryForm.includeRelationships = checked('includeRelationships');
  queryForm.includeAnnotations = checked('includeAnnotations');
  queryForm.debug = checked('debug');
}

/**
 * Build the `QueryRequest` envelope from form state.
 *
 * The envelope is `deny_unknown_fields` on every nested object, so only the
 * documented fields are emitted and an unused group is omitted entirely rather
 * than sent empty: no `constraints` when both id lists are blank (that is
 * all-sources scope), no `retrievalPolicy` when the unit cap is blank (the
 * retrieval profile default applies), and `debug` only when it is on.
 * `evidencePolicy` is always sent because all three toggles are user-visible.
 * Throws a plain Error for input the service would reject as `bad_request`.
 */
function buildQueryRequest(state) {
  const queryText = String(state.queryText || '').trim();
  if (queryText === '') throw new Error('queryText must not be empty.');
  const request = { queryText };

  const sourceIds = splitIdList(state.sourceIds);
  const governanceDomains = splitIdList(state.governanceDomains);
  if (sourceIds.length > 0 || governanceDomains.length > 0) {
    const constraints = {};
    if (sourceIds.length > 0) constraints.sourceIds = sourceIds;
    if (governanceDomains.length > 0) constraints.governanceDomains = governanceDomains;
    request.constraints = constraints;
  }

  const cap = String(state.maxFinalEvidenceUnits || '').trim();
  if (cap !== '') {
    if (!/^\d+$/.test(cap) || Number(cap) < 1) {
      throw new Error('Maximum results must be a positive integer.');
    }
    request.retrievalPolicy = { maxFinalEvidenceUnits: Number(cap) };
  }

  request.evidencePolicy = {
    includeSourceLocators: state.includeSourceLocators === true,
    includeRelationships: state.includeRelationships === true,
    includeAnnotations: state.includeAnnotations === true,
  };
  if (state.debug === true) request.debug = true;
  return request;
}

/**
 * Split a free-text id field into a list. Commas and newlines separate entries
 * so ids containing spaces survive; blanks are dropped so a trailing comma does
 * not become an empty constraint the service would match nothing against.
 */
function splitIdList(raw) {
  return String(raw || '')
    .split(/[\n,]+/)
    .map((entry) => entry.trim())
    .filter((entry) => entry !== '');
}

/** The request-builder form, re-rendered from `queryForm` on every render. */
function queryFormMarkup() {
  const running = queryRun.status === 'running';
  const disabled = running ? ' disabled' : '';
  const body = `<form id="query-form" class="query-form">
    <label for="query-text">Query text</label>
    <textarea id="query-text" name="queryText" placeholder="natural-language query"${disabled}>${esc(queryForm.queryText)}</textarea>
    <fieldset>
      <legend>Constraints</legend>
      <div class="field-row">
        <label for="query-source-ids">sourceIds</label>
        <input id="query-source-ids" name="sourceIds" type="text" autocomplete="off"
          placeholder="comma-separated source ids" value="${esc(queryForm.sourceIds)}"${disabled} />
      </div>
      <div class="field-row">
        <label for="query-domains">governanceDomains</label>
        <input id="query-domains" name="governanceDomains" type="text" autocomplete="off"
          placeholder="comma-separated domains" value="${esc(queryForm.governanceDomains)}"${disabled} />
      </div>
      <p class="field-note">Both present intersect; both blank is all-sources scope.</p>
    </fieldset>
    <fieldset>
      <legend>Policy</legend>
      <div class="field-row">
        <label for="query-max-units">Maximum results</label>
        <input id="query-max-units" name="maxFinalEvidenceUnits" type="number" min="1" step="1"
          placeholder="profile default" value="${esc(queryForm.maxFinalEvidenceUnits)}"${disabled} />
      </div>
      <div class="field-row">
        ${checkboxMarkup('includeSourceLocators', 'includeSourceLocators', queryForm.includeSourceLocators, running)}
        ${checkboxMarkup('includeRelationships', 'includeRelationships', queryForm.includeRelationships, running)}
        ${checkboxMarkup('includeAnnotations', 'includeAnnotations', queryForm.includeAnnotations, running)}
        ${checkboxMarkup('debug', 'debug', queryForm.debug, running)}
      </div>
    </fieldset>
    <div class="form-actions">
      <button type="submit"${disabled}>Run query</button>
      <span class="field-note">POST /api/query</span>
    </div>
  </form>`;
  return panel('Query console', body);
}

/** One labelled checkbox for the policy fieldset; `name` is also the control id stem. */
function checkboxMarkup(name, label, on, running) {
  const id = `query-${name}`;
  return `<span class="field-check">
    <input id="${esc(id)}" name="${esc(name)}" type="checkbox"${on ? ' checked' : ''}${running ? ' disabled' : ''} />
    <label for="${esc(id)}">${esc(label)}</label>
  </span>`;
}

/**
 * Show server-ranked passages by default, retaining the original response and
 * detailed evidence behind disclosure. Missing results is a protocol error,
 * never a reason to reconstruct passages from the evidence pack in the browser.
 */
function queryResultMarkup() {
  if (queryRun.status === 'running') return loadingMarkup('Running query');
  if (queryRun.status === 'error') return errorPanel(queryRun.error);
  if (queryRun.status !== 'done') return '';
  const body = queryRun.response;
  const pack = body && typeof body === 'object' ? body.evidencePack : null;
  if (!pack || typeof pack !== 'object' || !Array.isArray(body.results)) {
    return panel('Response', '<p>Query response is missing results or evidence pack.</p>' + jsonTree(body, 'response'), 'panel-error');
  }
  const diagnostics = body && typeof body === 'object' ? body.diagnostics : null;
  const results = body.results.length === 0
    ? '<p class="absent">No results.</p>'
    : body.results.map((result, index) => queryPassageMarkup(result, index + 1)).join('');
  const details = `<details class="evidence-detail">
    <summary>Evidence and diagnostics</summary>
    ${evidencePackMarkup(pack)}${assemblyTraceMarkup(pack.assemblyTrace)}${debugPanelMarkup(diagnostics)}
    ${jsonTree(body, 'Complete response JSON', 0)}
  </details>`;
  return panel(`Results (${body.results.length})`, `<p>${esc(pack.queryText)}</p>${results}${details}`);
}

/** Render one complete server passage; citation metadata never becomes a separate hit. */
function queryPassageMarkup(result, rank) {
  const locations = Array.isArray(result.sourceLocations) ? result.sourceLocations : [];
  const source = locations.length === 0
    ? '<div class="absent">Source location unavailable</div>'
    : locations.map((location) => {
      // Display locations as text: local paths need not be browser-accessible URLs.
      const status = location.status === 'current' ? '' : ` (${esc(location.status)})`;
      return `<div>${esc(location.nativeUri)}${status}</div>`;
    }).join('');
  const section = Array.isArray(result.sectionPath) && result.sectionPath.length > 0
    ? `<div>${result.sectionPath.map(esc).join(' › ')}</div>`
    : '';
  // Physical PDF page numbers are authoritative; printed page labels can differ.
  const pages = Array.isArray(result.pageNumbers) && result.pageNumbers.length > 0
    ? `<div>PDF pages: ${result.pageNumbers.map(esc).join(', ')}</div>`
    : '<div class="absent">Page information unavailable</div>';
  const truncated = result.truncated === true ? '<p class="field-note">[Passage truncated]</p>' : '';
  return `<article class="evidence">
    <div class="evidence-head"><span class="evidence-rank">#${esc(rank)}</span>${source}</div>
    ${section}${pages}
    <div class="evidence-text">${esc(result.text)}</div>${truncated}
    ${retrievalProvenanceMarkup(result.retrievalProvenance)}
  </article>`;
}

/** Translate channel identifiers for readers while preserving unknown values. */
function retrievalChannelsMarkup(channels) {
  if (!Array.isArray(channels) || channels.length === 0) return escOr(null);
  const labels = { dense: 'Semantic search', lexical: 'Keyword search', graph: 'Annotations' };
  return channels.map((channel) => badge(labels[channel] || channel)).join(' ');
}

/**
 * Explain the recorded traversal in its original subject/object direction.
 * The query-matched entity can be either endpoint; it is named separately so
 * walking an incoming edge does not reverse the annotation's meaning.
 */
function graphMatchMarkup(match) {
  const matchLabels = { acronym: 'acronym match', token_prefix: 'token-prefix match' };
  const matchClass = match.matchClass && match.matchClass !== 'exact'
    ? ` (${esc(matchLabels[match.matchClass] || match.matchClass)})` : '';
  const entity = `“${esc(match.matchedEntity)}”${matchClass}`;
  if (match.kind === 'direct_mention') return `Direct entity match: ${entity}`;
  const relationship = match.relationship;
  const path = relationship
    ? `“${esc(relationship.subject)}” → ${esc(relationship.predicate)} → “${esc(relationship.object)}”`
    : '<span class="absent">Relationship unavailable</span>';
  const kinds = {
    relation_support: 'Reached relationship evidence',
    related_entity_mention: 'Reached a related entity mention',
  };
  return `${path} — ${esc(kinds[match.kind] || match.kind)} via ${entity}`;
}

/**
 * Present server-owned candidate attribution without deriving contribution from
 * merged passage channels. Preview deduplication affects display only: the unit
 * table and JSON retain every match and its relationship-support references.
 */
function retrievalProvenanceMarkup(provenance) {
  if (!provenance || typeof provenance !== 'object') {
    return '<div class="retrieval-provenance absent">Retrieval provenance unavailable</div>';
  }
  const contributionLabels = {
    none: 'No annotation-based candidate matches.',
    overlap: 'All annotation-matched units also matched keyword or semantic search.',
    additional_matches: 'Annotations matched units absent from keyword and semantic candidate lists.',
  };
  const contribution = contributionLabels[provenance.annotationContribution]
    || 'Annotation contribution unavailable.';
  const matchedUnits = Array.isArray(provenance.matchedUnits) ? provenance.matchedUnits : [];
  const contextUnits = Array.isArray(provenance.contextUnitIds) ? provenance.contextUnitIds : [];
  const explanations = new Set();
  const unitRows = matchedUnits.map((unit) => {
    const matches = Array.isArray(unit.graphMatches) ? unit.graphMatches : [];
    const matchMarkup = matches.map((match) => {
      const explanation = graphMatchMarkup(match);
      explanations.add(explanation);
      const support = match.relationship && Array.isArray(match.relationship.supportingUnitIds)
        ? `<div>Relationship evidence: ${idListMarkup(match.relationship.supportingUnitIds, unitLink)}</div>`
        : '';
      return `<li>${explanation}${support}</li>`;
    }).join('');
    return [
      unitLink(unit.unitId),
      retrievalChannelsMarkup(unit.channels),
      matchMarkup ? `<ul class="retrieval-matches">${matchMarkup}</ul>` : escOr(null),
    ];
  });
  const preview = Array.from(explanations).slice(0, 3)
    .map((explanation) => `<li>${explanation}</li>`).join('');
  const additional = explanations.size > 3
    ? `<p>${esc(explanations.size - 3)} more annotation matches in retrieval details.</p>` : '';
  const contextNote = contextUnits.length > 0
    ? `<p>${esc(contextUnits.length)} passage units added as context; no retrieval match is attributed to them.</p>`
    : '';
  return `<div class="retrieval-provenance">
    <div><strong>Matched by:</strong> ${retrievalChannelsMarkup(provenance.channels)}</div>
    <p><strong>Annotation contribution:</strong> ${esc(contribution)}</p>
    ${preview ? `<ul class="retrieval-matches">${preview}</ul>` : ''}${additional}${contextNote}
    <details class="evidence-detail">
      <summary>Retrieval details</summary>
      <p>Candidate matches describe how units entered retrieval, not whether annotations improved the result.</p>
      ${dataTable(['Matched unit', 'Matched by', 'Annotation matches'], unitRows)}
      ${contextUnits.length > 0 ? `<p>Context units: ${idListMarkup(contextUnits, unitLink)}</p>` : ''}
      ${jsonTree(provenance, 'Complete retrieval provenance', 0)}
    </details>
  </div>`;
}

/**
 * Score presence is the anchor discriminator: PROTOCOL.md omits `score` for
 * every unit the assembler added as context.
 */
function isAnchorUnit(unit) {
  return unit && unit.score !== null && unit.score !== undefined;
}

/** The evidence pack: header counts, then one block per unit in pack order. */
function evidencePackMarkup(pack) {
  const units = Array.isArray(pack.evidenceUnits) ? pack.evidenceUnits : [];
  const anchors = units.filter(isAnchorUnit).length;
  const header = kvTable([
    { label: 'queryId', value: pack.queryId },
    { label: 'queryText', value: pack.queryText },
    { label: 'createdAt', value: pack.createdAt },
    {
      label: 'evidenceUnits',
      html: `${esc(units.length)} — ${esc(anchors)} ${badge('anchor', 'anchor')} · ${esc(units.length - anchors)} ${badge('context', 'context')}`,
    },
  ]);
  const list =
    units.length === 0
      ? '<p class="absent">No evidence units.</p>'
      : units.map((unit, index) => evidenceUnitMarkup(unit, index + 1)).join('');
  // relationships/annotations are omitted unless their evidence toggle was set,
  // so their absence here is a request choice, not missing data.
  const extras =
    (Array.isArray(pack.relationships) ? `<h3 class="subhead">relationships</h3>${jsonTree(pack.relationships, 'relationships', 0)}` : '') +
    (Array.isArray(pack.annotations) ? `<h3 class="subhead">annotations</h3>${jsonTree(pack.annotations, 'annotations', 0)}` : '');
  return panel('Evidence pack', header + list + extras);
}

/**
 * One evidence unit: simple-first head (rank, role/content-type/reason badges,
 * score) over the text projection — or, for projection-less container units, a
 * labelled body-derived summary — with full provenance behind a <details> so
 * the pack stays readable. Every id in the block is a navigation link.
 */
function evidenceUnitMarkup(unit, rank) {
  const anchor = isAnchorUnit(unit);
  const role = anchor ? 'anchor' : 'context';
  const scoreCell = anchor ? `<span class="evidence-score">score ${formatScore(unit.score)}</span>` : '';
  // Context units badge their inclusion reasons in the head: "why is this card
  // here" must be visible without expanding Provenance. Anchors skip the
  // reason badges — the role badge already says "anchor".
  const reasonBadges =
    !anchor && Array.isArray(unit.reasons)
      ? unit.reasons.map((reason) => badge(reason, 'reason')).join('')
      : '';
  const head = `<div class="evidence-head">
    <span class="evidence-rank">#${esc(rank)}</span>
    ${badge(role, role)}
    ${unit.contentType ? badge(unit.contentType, 'content-type') : ''}
    ${reasonBadges}
    ${scoreCell}
    <span class="evidence-ids">${unitLink(unit.unitId)} · ${sourceLink(unit.sourceId)}</span>
  </div>`;
  const projection =
    unit.textProjection === null || unit.textProjection === undefined
      ? derivedBodyMarkup(unit)
      : `<div class="evidence-text">${esc(unit.textProjection)}</div>`;
  const reasons =
    Array.isArray(unit.reasons) && unit.reasons.length > 0
      ? { label: 'reasons', html: unit.reasons.map((reason) => `<div>${esc(reason)}</div>`).join('') }
      : null;
  const provenance = kvTable([
    { label: 'unitId', html: unitLink(unit.unitId) },
    { label: 'sourceId', html: sourceLink(unit.sourceId) },
    { label: 'parseId', value: unit.parseId },
    { label: 'contentType', value: unit.contentType },
    { label: 'score', html: anchor ? formatScore(unit.score) : escOr(null, 'omitted (context unit)') },
    reasons,
  ]);
  const locators = Array.isArray(unit.locators) ? jsonTree(unit.locators, 'locators', 0) : '';
  const body = unit.body === undefined ? '' : jsonTree(unit.body, 'body', 0);
  const detail = `<details class="evidence-detail">
    <summary>Provenance</summary>
    ${provenance}${locators}${body}
  </details>`;
  return `<article class="evidence ${role}">${head}${projection}${detail}</article>`;
}

/**
 * Stand-in shown where the text projection would render, for units that have
 * none (the container/structural content types). Everything shown is read from
 * the unit's own `body` in the response — a derived display supplementing the
 * raw body JSON still in Provenance, never replacing it — and the block is
 * labelled "derived from body" so it cannot be mistaken for server-provided
 * text. A body with none of the expected fields falls back to an absence line
 * that at least names the content type.
 */
function derivedBodyMarkup(unit) {
  const body = unit.body;
  const parts =
    body !== null && typeof body === 'object' && !Array.isArray(body)
      ? derivedBodyParts(unit.contentType, body)
      : [];
  if (parts.length === 0) {
    return `<p class="absent">${esc(unit.contentType || 'unit')} — no text content.</p>`;
  }
  return `<div class="evidence-derived"><span class="derived-label">derived from body</span>${parts.join('')}</div>`;
}

/**
 * Per-content-type extraction behind derivedBodyMarkup(): pick the
 * human-useful fields out of a §18 body (PROTOCOL.md body shapes). Types with
 * a server-side text projection never reach this switch in practice; they and
 * unknown types return no parts, triggering the caller's fallback line.
 */
function derivedBodyParts(contentType, body) {
  const parts = [];
  switch (contentType) {
    case 'text_section':
      // sectionPath is the full heading ancestry and subsumes the bare
      // heading, so prefer it when present.
      if (Array.isArray(body.sectionPath) && body.sectionPath.length > 0) {
        parts.push(derivedLine('section', body.sectionPath.map(esc).join(' › ')));
      } else if (body.headingText) {
        const level = body.headingLevel === undefined ? '' : ` (h${esc(body.headingLevel)})`;
        parts.push(derivedLine('heading', `${esc(body.headingText)}${level}`));
      }
      if (body.normalizedText) parts.push(derivedText(body.normalizedText));
      break;
    case 'table':
      if (body.caption) parts.push(derivedLine('caption', esc(body.caption)));
      parts.push(derivedLine('shape', `${esc(body.rowCount)} rows × ${esc(body.columnCount)} columns`));
      if (Array.isArray(body.headers) && body.headers.length > 0) {
        parts.push(derivedLine('headers', body.headers.map((header) => esc(header.text)).join(' · ')));
      }
      if (body.normalizedMarkdown) {
        parts.push(`<pre class="derived-pre">${esc(body.normalizedMarkdown)}</pre>`);
      }
      break;
    case 'table_row':
      parts.push(derivedLine('row', `${esc(body.rowIndex)}${body.role ? ` (${esc(body.role)})` : ''}`));
      break;
    case 'figure':
      if (body.figureType) parts.push(derivedLine('type', esc(body.figureType)));
      if (body.caption) parts.push(derivedLine('caption', esc(body.caption)));
      if (body.altText) parts.push(derivedLine('alt text', esc(body.altText)));
      if (body.ocrText) parts.push(derivedText(body.ocrText));
      break;
    case 'image_region':
      if (body.label) parts.push(derivedLine('label', esc(body.label)));
      if (body.confidence !== undefined) parts.push(derivedLine('confidence', esc(body.confidence)));
      if (body.ocrText) parts.push(derivedText(body.ocrText));
      break;
    case 'page':
      parts.push(derivedLine('page', esc(body.pageNumber)));
      break;
    default:
      break;
  }
  return parts;
}

/** One "key: value" line of a derived-body summary; `html` is pre-escaped. */
function derivedLine(label, html) {
  return `<div class="derived-line"><span class="derived-key">${esc(label)}</span> ${html}</div>`;
}

/** Free-form derived text (OCR, normalized text), whitespace preserved. */
function derivedText(text) {
  return `<div class="evidence-text">${esc(text)}</div>`;
}

/**
 * Assembly trace (SPEC-web-ui.md §4 bullet 2): policy identity, applied rules
 * grouped by their anchor, rejected hits/units, and the budget in force.
 * Returns '' when the response carried no trace.
 */
function assemblyTraceMarkup(trace) {
  if (!trace || typeof trace !== 'object') return '';
  const identity = kvTable([
    { label: 'assemblyPolicyId', value: trace.assemblyPolicyId },
    { label: 'assemblyPolicyVersion', value: trace.assemblyPolicyVersion },
    { label: 'assemblyPolicyHash', value: trace.assemblyPolicyHash },
    { label: 'inputHitIds', html: idListMarkup(trace.inputHitIds, null) },
    { label: 'selectedUnitIds', html: idListMarkup(trace.selectedUnitIds, unitLink) },
    // Rejections are omitted when absent, so only render the row when present.
    trace.rejectedHitIds ? { label: 'rejectedHitIds', html: idListMarkup(trace.rejectedHitIds, null) } : null,
    trace.rejectedUnitIds ? { label: 'rejectedUnitIds', html: idListMarkup(trace.rejectedUnitIds, unitLink) } : null,
  ]);
  const budget = budgetMarkup(trace.budget);
  const rules = appliedRulesMarkup(trace.appliedRules);
  return panel('Assembly trace', identity + budget + rules);
}

/** The budget in force; `maxReferencedUnits` is omitted when absent. */
function budgetMarkup(budget) {
  if (!budget || typeof budget !== 'object') return '';
  return (
    '<h3 class="subhead">budget</h3>' +
    kvTable([
      { label: 'maxEvidenceUnits', value: budget.maxEvidenceUnits },
      { label: 'maxTokens', value: budget.maxTokens },
      { label: 'maxExpansionDepth', value: budget.maxExpansionDepth },
      budget.maxReferencedUnits === null || budget.maxReferencedUnits === undefined
        ? null
        : { label: 'maxReferencedUnits', value: budget.maxReferencedUnits },
    ])
  );
}

/**
 * Applied rules grouped by anchor, because the trace's story is "this anchor
 * pulled in these units under this rule". A rule may be unit-grained
 * (`anchorUnitId`), hit-grained (`anchorHitId`), or neither.
 */
function appliedRulesMarkup(rules) {
  if (!Array.isArray(rules) || rules.length === 0) return '';
  const groups = new Map();
  for (const rule of rules) {
    const unitAnchor = rule && rule.anchorUnitId;
    const hitAnchor = rule && rule.anchorHitId;
    const key = unitAnchor ? `unit:${unitAnchor}` : hitAnchor ? `hit:${hitAnchor}` : 'none';
    let group = groups.get(key);
    if (!group) {
      const heading = unitAnchor
        ? `Anchor unit ${unitLink(unitAnchor)}`
        : hitAnchor
          ? `Anchor hit <code>${esc(hitAnchor)}</code>`
          : 'No anchor';
      group = { heading, rules: [] };
      groups.set(key, group);
    }
    group.rules.push(rule);
  }
  const sections = [...groups.values()]
    .map((group) => {
      const rows = group.rules.map((rule) => [
        `<code>${escOr(rule.ruleId)}</code>`,
        badge(rule.reason || 'unknown', 'context'),
        idListMarkup(rule.addedUnitIds, unitLink),
      ]);
      return `<div class="trace-group"><h3>${group.heading}</h3>${dataTable(['ruleId', 'reason', 'addedUnitIds'], rows)}</div>`;
    })
    .join('');
  return `<h3 class="subhead">appliedRules</h3>${sections}`;
}

/**
 * Debug diagnostics (SPEC-web-ui.md §4 bullet 3): rendered only when the
 * response carried `diagnostics`, which the service attaches only when the
 * request set `debug: true`.
 */
function debugPanelMarkup(diagnostics) {
  if (!diagnostics || typeof diagnostics !== 'object') return '';
  return panel(
    'Debug diagnostics',
    latencyMarkup(diagnostics.latencies) +
      retrievalHitsMarkup(diagnostics.channelHits, 'channelHits', 'channel') +
      retrievalHitsMarkup(diagnostics.fusedPool, 'fusedPool', 'representative channel') +
      maxsimMarkup(diagnostics.maxsim) +
      rerankedMarkup(diagnostics.reranked)
  );
}

/** Per-stage wall-clock latencies; iterated from the object so new stages still show. */
function latencyMarkup(latencies) {
  if (!latencies || typeof latencies !== 'object') return '';
  const rows = Object.entries(latencies).map(([stage, ms]) => [esc(stage), `${escOr(ms)} ms`]);
  return `<h3 class="subhead">latencies</h3>${dataTable(['stage', 'wall clock'], rows)}`;
}

/**
 * Preserve each candidate record's fields in both retrieval diagnostic tables.
 * A fused record keeps one representative channel; it cannot describe overlap.
 * Optional graph paths supplement the original notes rather than replacing them.
 */
function retrievalHitsMarkup(pool, title, channelHeading) {
  if (!Array.isArray(pool) || pool.length === 0) return '';
  const rows = pool.map((hit) => {
    const notes = [
      hit.matchedProjectionId ? `projection ${hit.matchedProjectionId}` : null,
      hit.matchedAnnotationId ? `annotation ${hit.matchedAnnotationId}` : null,
      hit.explanation || null,
    ]
      .filter(Boolean)
      .map((note) => esc(note))
      .join('<br />');
    const matches = Array.isArray(hit.graphMatches) && hit.graphMatches.length > 0
      ? jsonTree(hit.graphMatches, 'Graph matches', 0) : '';
    return [
      escOr(hit.rank),
      badge(hit.channel || 'unknown'),
      escOr(hit.hitType),
      `<code>${escOr(hit.hitId)}</code>`,
      formatScore(hit.score),
      sourceLink(hit.sourceId),
      idListMarkup(hit.unitIds, unitLink),
      notes === '' && matches === '' ? escOr(null) : notes + matches,
    ];
  });
  return `<h3 class="subhead">${esc(title)}</h3>${dataTable(
    ['rank', channelHeading, 'hitType', 'hitId', 'score', 'sourceId', 'unitIds', 'notes'],
    rows
  )}`;
}

/** MaxSim scores over the fused pool, best-first as the service ordered them. */
function maxsimMarkup(entries) {
  if (!Array.isArray(entries) || entries.length === 0) return '';
  const rows = entries.map((entry) => [escOr(entry.rank), unitLink(entry.unitId), formatScore(entry.score)]);
  return `<h3 class="subhead">maxsim</h3>${dataTable(['rank', 'unitId', 'score'], rows)}`;
}

/** Final reranker scores; `logit` and `tokenCount` are omitted when absent. */
function rerankedMarkup(entries) {
  if (!Array.isArray(entries) || entries.length === 0) return '';
  const rows = entries.map((entry) => [
    escOr(entry.rank),
    unitLink(entry.unitId),
    formatScore(entry.score),
    entry.logit === null || entry.logit === undefined ? escOr(null) : formatScore(entry.logit),
    escOr(entry.tokenCount),
  ]);
  return `<h3 class="subhead">reranked</h3>${dataTable(['rank', 'unitId', 'score', 'logit', 'tokenCount'], rows)}`;
}

/**
 * Render an id array as links (or as code when `linkFn` is null, e.g. hit ids,
 * which have no view). Absent or empty arrays get the placeholder.
 */
function idListMarkup(ids, linkFn) {
  if (!Array.isArray(ids) || ids.length === 0) return escOr(null);
  const items = ids.map((id) => (linkFn ? linkFn(id) : `<code>${esc(id)}</code>`));
  return `<span class="id-list">${items.join(' ')}</span>`;
}

/**
 * Table from a header list plus rows of already-escaped cell markup. Cells are
 * trusted: every caller builds them from esc()/escOr()/link helpers.
 */
function dataTable(headers, rows) {
  if (!Array.isArray(rows) || rows.length === 0) return '';
  const head = headers.map((header) => `<th scope="col">${esc(header)}</th>`).join('');
  const body = rows.map((cells) => `<tr>${cells.map((cell) => `<td>${cell}</td>`).join('')}</tr>`).join('');
  return `<table class="data"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

/** Fixed-precision score markup; a non-numeric or absent score falls back to escOr(). */
function formatScore(value) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return escOr(value);
  return esc(value.toFixed(4));
}

registerView('query', {
  title: 'Query',
  params: [],
  render: async (mount) => {
    renderQueryConsole(mount);
  },
});

// ---------------------------------------------------------------------------
// Unit explorer (SPEC-web-ui.md §4 bullet 4; PROTOCOL.md "GET /units/{unitId}"
// and "GET /units/{unitId}/relationships")
//
// Two fetches per unit: the ContentUnit itself and the structural edges touching
// it. The unit read is the view's spine — if it fails (404 for a unit whose parse
// is not active is the common case) the router renders the service envelope and
// nothing else is shown. The relationship read is subordinate: its failure and
// its filters are confined to the relationships panel.
// ---------------------------------------------------------------------------

/**
 * Relationship-filter state. Module-scoped so the router's full re-render (and
 * following a related-unit link) keeps the filter the user selected; both fields
 * hold the raw form values, where '' means "no filter" and is therefore dropped
 * by queryString() instead of being sent as an empty parameter.
 */
const unitRelationshipFilters = { direction: '', relationshipType: '' };

/** The closed `relationshipType` wire set (PROTOCOL.md); offered as suggestions,
 *  not enforced, so a type added by a later service version is still filterable. */
const RELATIONSHIP_TYPES = [
  'contains',
  'physically_contains',
  'logically_contains',
  'precedes',
  'follows',
  'appears_on',
  'caption_of',
  'has_caption',
  'references',
  'continues_on',
  'derived_from',
];

/**
 * Render the unit detail plus its relationships panel, then bind the filter
 * form. Writes `mount` once, after both fetches, so a render superseded mid-flight
 * cannot interleave with the next one.
 */
async function renderUnitExplorer(mount, unitId) {
  // A unit read failure is fatal for the view: let it propagate so the router
  // renders the service's own error envelope (404 not_found, etc.).
  const unit = await apiGet(`/api/units/${encodeURIComponent(unitId)}`);
  const relationships = await fetchRelationshipsMarkup(unitId);
  mount.innerHTML =
    unitDetailMarkup(unit, unitId) +
    panel(
      'Relationships',
      relationshipFilterFormMarkup() + `<div id="unit-relationships">${relationships}</div>`
    );
  bindRelationshipFilters(mount, unitId);
}

/**
 * Fetch the filtered relationships and return the panel body markup. A service
 * error is returned as a rendered error panel rather than thrown: the unit detail
 * is still worth showing when only the edge read failed.
 */
async function fetchRelationshipsMarkup(unitId) {
  try {
    const body = await apiGet(`/api/units/${encodeURIComponent(unitId)}/relationships`, {
      direction: unitRelationshipFilters.direction,
      relationshipType: unitRelationshipFilters.relationshipType,
    });
    const edges = body && Array.isArray(body.relationships) ? body.relationships : [];
    return relationshipsMarkup(edges, unitId);
  } catch (error) {
    return errorPanel(error);
  }
}

/**
 * Bind the filter form to an in-place refresh of the relationships container.
 * Re-fetching without navigating keeps the unit detail and the page position; the
 * post-await route check prevents a slow response from writing into a view the
 * user has already left.
 */
function bindRelationshipFilters(mount, unitId) {
  const form = mount.querySelector('#unit-rel-filters');
  if (!form) return;
  form.addEventListener('submit', async (event) => {
    // No action attribute: applying a filter is an API call, never a page load.
    event.preventDefault();
    const direction = form.elements.namedItem('direction');
    const relationshipType = form.elements.namedItem('relationshipType');
    if (direction && typeof direction.value === 'string') unitRelationshipFilters.direction = direction.value;
    if (relationshipType && typeof relationshipType.value === 'string') {
      unitRelationshipFilters.relationshipType = relationshipType.value.trim();
    }
    const container = document.getElementById('unit-relationships');
    if (container) container.innerHTML = loadingMarkup('Loading relationships');
    const markup = await fetchRelationshipsMarkup(unitId);
    const route = parseHash(window.location.hash);
    if (route.name !== 'unit' || route.args[0] !== unitId) return;
    const target = document.getElementById('unit-relationships');
    if (target) target.innerHTML = markup;
  });
}

/**
 * The ContentUnit itself. `id` is the unit's own field name (the route parameter
 * is `unitId`); `primaryParentId` is a convenience field and is linkable because
 * it names another unit. Locators and body render as collapsible JSON because
 * both are typed unions the UI does not interpret.
 */
function unitDetailMarkup(unit, unitId) {
  if (!unit || typeof unit !== 'object') {
    return panel('Unit', `<p class="absent">No unit body for ${esc(unitId)}.</p>`, 'panel-error');
  }
  const rows = kvTable([
    { label: 'id', value: unit.id },
    { label: 'sourceId', html: sourceLink(unit.sourceId) },
    { label: 'parseId', value: unit.parseId },
    { label: 'contentType', html: badge(unit.contentType || 'unknown') },
    { label: 'bodyHash', value: unit.bodyHash },
    // Hashes, structural convenience fields and deletedAt are omitted when
    // absent; a row is rendered only when the field came back.
    unit.textHash === undefined ? null : { label: 'textHash', value: unit.textHash },
    unit.structureHash === undefined ? null : { label: 'structureHash', value: unit.structureHash },
    unit.primaryParentId === undefined ? null : { label: 'primaryParentId', html: unitLink(unit.primaryParentId) },
    unit.sequenceIndex === undefined ? null : { label: 'sequenceIndex', value: unit.sequenceIndex },
    { label: 'createdAt', value: unit.createdAt },
    unit.deletedAt === undefined ? null : { label: 'deletedAt', value: unit.deletedAt },
  ]);
  const locators = Array.isArray(unit.locators) ? jsonTree(unit.locators, 'locators', 0) : '';
  const body = unit.body === undefined ? '' : jsonTree(unit.body, 'body', 0);
  return panel('Unit', rows + locators + body);
}

/** Direction and relationship-type filter controls, seeded from the live state. */
function relationshipFilterFormMarkup() {
  const selected = (value) => (unitRelationshipFilters.direction === value ? ' selected' : '');
  const options = RELATIONSHIP_TYPES.map((type) => `<option value="${esc(type)}"></option>`).join('');
  return `<form id="unit-rel-filters" class="filters">
    <div class="field-row">
      <label for="unit-rel-direction">direction</label>
      <select id="unit-rel-direction" name="direction">
        <option value=""${selected('')}>both</option>
        <option value="out"${selected('out')}>out</option>
        <option value="in"${selected('in')}>in</option>
      </select>
      <label for="unit-rel-type">relationshipType</label>
      <input id="unit-rel-type" name="relationshipType" type="text" list="relationship-types"
        autocomplete="off" placeholder="any" value="${esc(unitRelationshipFilters.relationshipType)}" />
      <datalist id="relationship-types">${options}</datalist>
      <button type="submit">Apply</button>
    </div>
  </form>`;
}

/**
 * The edge table, service order preserved (sequenceIndex then id). Each row shows
 * the unit at the other end as a link, which is the click-through path through
 * the graph; the raw array follows so omitted-when-absent fields the table does
 * not columnise (provenance) remain inspectable.
 */
function relationshipsMarkup(edges, unitId) {
  if (edges.length === 0) return '<p class="absent">No relationships match this filter.</p>';
  const rows = edges.map((edge) => [
    badge(relationshipDirection(edge, unitId)),
    `<code>${escOr(edge.relationshipType)}</code>`,
    escOr(edge.relationshipRole),
    unitLink(relationshipOtherUnitId(edge, unitId)),
    escOr(edge.sequenceIndex),
    edge.confidence === null || edge.confidence === undefined ? escOr(null) : formatScore(edge.confidence),
    escOr(edge.createdAt),
  ]);
  const table = dataTable(
    ['direction', 'type', 'role', 'other unit', 'sequenceIndex', 'confidence', 'createdAt'],
    rows
  );
  return `${table}${jsonTree(edges, 'relationships', 0)}`;
}

/**
 * Direction relative to the anchor unit. A self-edge (both endpoints the anchor)
 * is reported as `self` rather than silently counted as outgoing.
 */
function relationshipDirection(edge, unitId) {
  const from = edge && edge.fromUnitId === unitId;
  const to = edge && edge.toUnitId === unitId;
  if (from && to) return 'self';
  if (from) return 'out';
  if (to) return 'in';
  return 'unrelated';
}

/**
 * The endpoint that is not the anchor unit — the click-through target. A
 * self-edge has no other end, so the anchor itself is returned.
 */
function relationshipOtherUnitId(edge, unitId) {
  if (!edge) return null;
  return edge.fromUnitId === unitId ? edge.toUnitId : edge.fromUnitId;
}

registerView('unit', {
  title: 'Unit',
  params: ['unitId'],
  render: async (mount, params) => {
    if (!params.unitId) {
      mount.innerHTML = panel(
        'Unit explorer',
        `<p>Open a unit by id, or follow a <code>unitId</code> link from a query result.</p>
         ${lookupForm('unit-lookup', 'Unit id', 'unit id')}`
      );
      return;
    }
    await renderUnitExplorer(mount, params.unitId);
  },
});

// ---------------------------------------------------------------------------
// Source view (SPEC-web-ui.md §4 bullet 5; PROTOCOL.md "GET /sources/{sourceId}")
//
// One fetch. The SourceObject carries its own location set, so identity/active
// parse, freshness timestamps, and locations are three presentations of a single
// response rather than three reads.
// ---------------------------------------------------------------------------

/** Render the source panels; one write after the single fetch. */
async function renderSourceView(mount, sourceId) {
  const source = await apiGet(`/api/sources/${encodeURIComponent(sourceId)}`);
  if (!source || typeof source !== 'object') {
    mount.innerHTML = panel('Source', `<p class="absent">No source body for ${esc(sourceId)}.</p>`, 'panel-error');
    return;
  }
  const locations = Array.isArray(source.locations) ? source.locations : [];
  mount.innerHTML =
    panel('Source', sourceIdentityMarkup(source)) +
    panel('Freshness', sourceFreshnessMarkup(source)) +
    // panel() escapes its title, so the count is interpolated raw here.
    panel(`Locations (${locations.length})`, sourceLocationsMarkup(locations));
}

/**
 * Identity and the active parse. `activeParseId` is omitted while no parse has
 * activated or after deactivation, and that absence is the reason a unit read of
 * this source's units would 404 — so it is shown as an explicit state, not a
 * blank.
 */
function sourceIdentityMarkup(source) {
  return kvTable([
    { label: 'id', value: source.id },
    {
      label: 'activeParseId',
      html: source.activeParseId === undefined || source.activeParseId === null
        ? escOr(null, 'no active parse')
        : `<code>${esc(source.activeParseId)}</code>`,
    },
    { label: 'mimeType', value: source.mimeType },
    { label: 'sizeBytes', value: source.sizeBytes },
    { label: 'sourceHash', value: source.sourceHash },
    { label: 'storageUri', value: source.storageUri },
  ]);
}

/**
 * Freshness timestamps. `eventTime` is the source system's own time and is
 * omitted when absent; `deactivatedAt` is set only when zero `current` locations
 * remain, so its presence is the corpus-level "gone" signal.
 */
function sourceFreshnessMarkup(source) {
  return kvTable([
    { label: 'eventTime', value: source.eventTime },
    { label: 'ingestTime', value: source.ingestTime },
    { label: 'createdAt', value: source.createdAt },
    source.deactivatedAt === undefined || source.deactivatedAt === null
      ? null
      : { label: 'deactivatedAt', html: `${esc(source.deactivatedAt)} ${badge('deactivated', 'error')}` },
  ]);
}

/**
 * The location set. Per-location `deletionEvidence` and `metadata` are omitted
 * when absent and are rendered as JSON below the table only for the locations
 * that carry them, keeping the common case a plain table.
 */
function sourceLocationsMarkup(locations) {
  if (locations.length === 0) return '<p class="absent">No locations.</p>';
  const rows = locations.map((location) => [
    `<code>${escOr(location.sourceSystem)}</code>`,
    escOr(location.nativeUri),
    escOr(location.nativeId),
    escOr(location.governanceDomain),
    locationStatusBadge(location.status),
    escOr(location.firstSeenAt),
    escOr(location.lastSeenAt),
  ]);
  const table = dataTable(
    ['sourceSystem', 'nativeUri', 'nativeId', 'governanceDomain', 'status', 'firstSeenAt', 'lastSeenAt'],
    rows
  );
  const details = locations
    .filter((location) => location.deletionEvidence !== undefined || location.metadata !== undefined)
    .map((location) => {
      const label = location.id === undefined || location.id === null ? 'location' : String(location.id);
      // Only present fields are put in the tree. An `undefined` value would keep
      // its key in Object.entries() and render as an empty json-string, which
      // reads as "the service reported an empty value" — the opposite of
      // omitted-when-absent, and most misleading on deletionEvidence, the field
      // that distinguishes `deleted` from `access_lost`.
      const detail = {};
      if (location.deletionEvidence !== undefined) detail.deletionEvidence = location.deletionEvidence;
      if (location.metadata !== undefined) detail.metadata = location.metadata;
      return jsonTree(detail, label, 0);
    })
    .join('');
  return details === '' ? table : `${table}<h3 class="subhead">location detail</h3>${details}`;
}

/**
 * Status badge for one location. `access_lost` is explicitly not deletion
 * (PROTOCOL.md), so it gets the warning styling, not the error styling.
 */
function locationStatusBadge(status) {
  if (status === 'current') return badge('current', 'ok');
  if (status === 'deleted') return badge('deleted', 'error');
  if (status === 'access_lost') return badge('access_lost', 'context');
  return badge(status || 'unknown');
}

registerView('source', {
  title: 'Source',
  params: ['sourceId'],
  render: async (mount, params) => {
    if (!params.sourceId) {
      mount.innerHTML = panel(
        'Source',
        `<p>Open a source by id, or follow a <code>sourceId</code> link from a query result.</p>
         ${lookupForm('source-lookup', 'Source id', 'source id')}`
      );
      return;
    }
    await renderSourceView(mount, params.sourceId);
  },
});

// ---------------------------------------------------------------------------
// Health dashboard (SPEC-web-ui.md §4 bullet 6; PROTOCOL.md "GET /v1/health")
//
// One fetch. `/v1/health` is the ONE service response whose field names are
// snake_case on the wire (`source_system`, `as_of`); every other view in this
// file reads camelCase. Readiness is gated by the `inference` and `sync`
// components only — the rest are diagnostic-only, so a not-ready diagnostic
// component must not be shown as if it were holding the service down.
// ---------------------------------------------------------------------------

/** Ready/not-ready badge; a missing `ready` field is reported as unknown rather
 *  than silently rendered as not ready. */
function readinessBadge(ready) {
  if (ready === true) return badge('ready', 'ok');
  if (ready === false) return badge('not ready', 'error');
  return badge('unknown');
}

/** The component names that gate top-level readiness (PROTOCOL.md); every other
 *  component is diagnostic-only. */
const READINESS_GATING_COMPONENTS = ['inference', 'sync'];

/** Render the dashboard; one write after the single fetch. */
async function renderHealthDashboard(mount) {
  const health = await apiGet('/api/health');
  if (!health || typeof health !== 'object') {
    mount.innerHTML = panel('Health', '<p class="absent">No health body.</p>', 'panel-error');
    return;
  }
  const components = Array.isArray(health.components) ? health.components : [];
  mount.innerHTML =
    panel('Health', healthSummaryMarkup(health, components)) +
    components.map((component) => healthComponentPanel(component)).join('');
}

/** Service identity plus top-level readiness and the component roster. */
function healthSummaryMarkup(health, components) {
  const roster =
    components.length === 0
      ? escOr(null)
      : components
          .map((component) => `${esc(component && component.name)} ${readinessBadge(component && component.ready)}`)
          .join(' · ');
  return kvTable([
    { label: 'service', value: health.service },
    { label: 'ready', html: readinessBadge(health.ready) },
    { label: 'components', html: roster },
  ]);
}

/**
 * One component: its readiness, its free-form diagnostic lines, and its typed
 * counters. `counts` is an empty array for components that publish none, so an
 * empty counts table is expected data, not a failure.
 */
function healthComponentPanel(component) {
  if (!component || typeof component !== 'object') return '';
  const name = component.name || 'unnamed component';
  const gating = READINESS_GATING_COMPONENTS.includes(component.name);
  const header = kvTable([
    { label: 'ready', html: `${readinessBadge(component.ready)} ${gating ? badge('gates readiness', 'anchor') : badge('diagnostic only')}` },
  ]);
  const details = healthDetailsMarkup(component.details);
  const counts = healthCountsMarkup(component.counts);
  // panel() escapes its title, so the component name is passed as text.
  return panel(`Component: ${name}`, header + details + counts);
}

/** The component's free-form diagnostic lines, preserved in service order. */
function healthDetailsMarkup(details) {
  if (!Array.isArray(details) || details.length === 0) return '';
  const lines = details.map((line) => `<li>${esc(line)}</li>`).join('');
  return `<h3 class="subhead">details</h3><ul class="health-details">${lines}</ul>`;
}

/**
 * The component's typed counters. Field names here are snake_case because
 * `/v1/health` is the one snake_case response; `source_system` is omitted for
 * corpus-aggregate counts, and every count carries its own `as_of`, which is why
 * the timestamp is a per-row column rather than a panel-level label.
 */
function healthCountsMarkup(counts) {
  if (!Array.isArray(counts) || counts.length === 0) return '';
  const rows = counts.map((count) => [
    `<code>${escOr(count.label)}</code>`,
    count.source_system === undefined || count.source_system === null
      ? escOr(null, 'corpus aggregate')
      : `<code>${esc(count.source_system)}</code>`,
    escOr(count.value),
    escOr(count.as_of),
  ]);
  return `<h3 class="subhead">counts</h3>${dataTable(['label', 'source_system', 'value', 'as_of'], rows)}`;
}

registerView('health', {
  title: 'Health',
  params: [],
  render: async (mount) => {
    await renderHealthDashboard(mount);
  },
});

// ---------------------------------------------------------------------------
// Sync status (SPEC-web-ui.md §4 bullet 7; PROTOCOL.md "GET /sync/status")
//
// One fetch of the last-published SyncHealth snapshot (camelCase). This route
// deliberately carries no fabric-counts projection — those live on the health
// dashboard, the single aggregation surface — so the view links there instead of
// implying the counts are missing.
// ---------------------------------------------------------------------------

/** Render the sync snapshot; one write after the single fetch. */
async function renderSyncStatus(mount) {
  const status = await apiGet('/api/sync-status');
  if (!status || typeof status !== 'object') {
    mount.innerHTML = panel('Sync status', '<p class="absent">No sync status body.</p>', 'panel-error');
    return;
  }
  mount.innerHTML =
    panel('Sync status', syncSummaryMarkup(status)) +
    panel('Queue backlog', syncBacklogMarkup(status));
}

/**
 * Readiness and cycle timing. `detail` (why the subsystem is not ready, or the
 * last cycle-level error), `cadenceMs`, and `lastSuccessAt` are omitted when
 * absent; `detail` is rendered only when present so its absence is not read as an
 * empty error message.
 */
function syncSummaryMarkup(status) {
  return kvTable([
    { label: 'fabricReady', html: readinessBadge(status.fabricReady) },
    status.detail === undefined || status.detail === null ? null : { label: 'detail', value: status.detail },
    { label: 'cadenceMs', value: status.cadenceMs },
    { label: 'lastSuccessAt', value: status.lastSuccessAt },
    { label: 'fabric counts', html: routeLink('#/health', 'health dashboard', 'id-link') },
  ]);
}

/**
 * The queue backlog counters, always present. `coalescedTotal` counts later
 * detections folded into an already-queued row, so it is a cumulative total
 * rather than a current depth and is labelled apart from the three depths.
 */
function syncBacklogMarkup(status) {
  const rows = [[escOr(status.pending), escOr(status.inFlight), escOr(status.failed)]];
  return (
    dataTable(['pending', 'inFlight', 'failed'], rows) +
    kvTable([{ label: 'coalescedTotal (cumulative)', value: status.coalescedTotal }])
  );
}

registerView('sync', {
  title: 'Sync status',
  params: [],
  render: async (mount) => {
    await renderSyncStatus(mount);
  },
});

// ---------------------------------------------------------------------------
// Admin reads: held parses, operation viewer, vocabulary explorer
// (SPEC-web-ui.md §4 bullet 8; PROTOCOL.md "GET /parses", "GET
// /operations/{operationId}", "GET /annotations/vocabulary")
//
// These three views are the only ones whose proxy routes carry the admin bearer
// token (SPEC-web-ui.md §2). The token lives in the client's token file, not in
// the browser, so a 401/403 here is a service answer about that file — real data
// to show the operator, never a transport fault. Each view therefore catches its
// own ApiError and renders the envelope in place instead of letting the router
// replace the whole view, so the controls (refresh, filters) stay usable.
// ---------------------------------------------------------------------------

/**
 * Render a failed admin read: the service's own envelope, plus an explanation
 * when the failure is an authorization one. 401/403 from a bearer route means
 * the client's admin token file was missing, unreadable, or rejected — the fix
 * is on the serving host, which the panel alone would not tell the operator.
 */
function adminErrorMarkup(error) {
  return errorPanel(error) + adminAuthNote(error);
}

/** The token-file note for 401/403 only; '' for every other failure. */
function adminAuthNote(error) {
  if (!(error instanceof ApiError)) return '';
  if (error.status !== 401 && error.status !== 403) return '';
  return `<p class="auth-note">The service rejected the admin bearer token. This route is proxied with the
    token file configured for the <code>data-store</code> client that is serving this page; the browser holds
    no credential of its own.</p>`;
}

// ---------------------------------------------------------------------------
// Held-parses list (PROTOCOL.md "GET /parses")
// ---------------------------------------------------------------------------

/**
 * Render the held-parses list; one write after the single fetch. A held parse is
 * `status: "ready"` carrying `heldReason`, so the disposition-relevant fields are
 * the conformance report and the parser identity, not the status alone.
 */
async function renderHeldParses(mount) {
  let body;
  try {
    body = await apiGet('/api/held-parses');
  } catch (error) {
    mount.innerHTML = adminErrorMarkup(error);
    return;
  }
  mount.innerHTML = heldParsesMarkup(body);
}

/** Summary table over the held parses, then one detail panel each. */
function heldParsesMarkup(body) {
  const parses = body && Array.isArray(body.parses) ? body.parses : [];
  if (parses.length === 0) {
    return panel('Held parses', '<p class="absent">No parses are held for disposition.</p>');
  }
  const rows = parses.map((parse) => [
    `<code>${escOr(parse.id)}</code>`,
    sourceLink(parse.sourceId),
    `${escOr(parse.parserName)} <span class="mono">${escOr(parse.parserVersion)}</span>`,
    parseStatusBadge(parse.status),
    // heldReason is omitted when absent; its only value is conformance_regression.
    escOr(parse.heldReason),
    escOr(parse.createdAt),
    escOr(parse.completedAt),
  ]);
  const table = dataTable(
    ['id', 'sourceId', 'parser', 'status', 'heldReason', 'createdAt', 'completedAt'],
    rows
  );
  // panel() escapes its title, so the count is interpolated as text.
  return panel(`Held parses (${parses.length})`, table) + parses.map(heldParseDetailMarkup).join('');
}

/**
 * One held parse in full: identity and artifact hashes, its conformance report
 * (the activation-dominance evidence), warnings, metrics, and the raw row. Every
 * field below `status` is omitted when absent, so each block renders only when
 * the service sent it.
 */
function heldParseDetailMarkup(parse) {
  if (!parse || typeof parse !== 'object') return '';
  const identity = kvTable([
    { label: 'id', value: parse.id },
    { label: 'sourceId', html: sourceLink(parse.sourceId) },
    { label: 'status', html: `${parseStatusBadge(parse.status)} ${escOr(parse.heldReason, 'no heldReason')}` },
    { label: 'parserName', value: parse.parserName },
    { label: 'parserVersion', value: parse.parserVersion },
    { label: 'parserConfigHash', value: parse.parserConfigHash },
    { label: 'capabilityProfileHash', value: parse.capabilityProfileHash },
    { label: 'createdAt', value: parse.createdAt },
    parse.startedAt === undefined ? null : { label: 'startedAt', value: parse.startedAt },
    parse.completedAt === undefined ? null : { label: 'completedAt', value: parse.completedAt },
    parse.activatedAt === undefined ? null : { label: 'activatedAt', value: parse.activatedAt },
    parse.archivedAt === undefined ? null : { label: 'archivedAt', value: parse.archivedAt },
    parse.artifactBundleUri === undefined ? null : { label: 'artifactBundleUri', value: parse.artifactBundleUri },
    parse.artifactBundleHash === undefined ? null : { label: 'artifactBundleHash', value: parse.artifactBundleHash },
    parse.parserRawOutputUri === undefined ? null : { label: 'parserRawOutputUri', value: parse.parserRawOutputUri },
    parse.error === undefined ? null : { label: 'error', value: parse.error },
  ]);
  const label = parse.id === undefined || parse.id === null ? 'parse' : String(parse.id);
  return panel(
    `Parse ${label}`,
    identity +
      conformanceReportMarkup(parse.conformanceReport) +
      parseWarningsMarkup(parse.warnings) +
      parseMetricsMarkup(parse.metrics)
  );
}

/**
 * Status badge for a parse run. `failed` is the only error state; `ready` is
 * styled as context because a held parse sits in `ready` awaiting a human
 * decision rather than having succeeded.
 */
function parseStatusBadge(status) {
  if (status === 'active') return badge('active', 'ok');
  if (status === 'failed') return badge('failed', 'error');
  if (status === 'ready') return badge('ready', 'context');
  return badge(status || 'unknown');
}

/**
 * The conformance report: the scalar coverage metrics, the `dimensions` map the
 * activation dominance rule compares, and the per-type counts. Absent entirely
 * for a parse held without one, and `captionPairingRate` /
 * `tableDecompositionRate` are individually omitted when absent.
 */
function conformanceReportMarkup(report) {
  if (!report || typeof report !== 'object') return '';
  const scalars = kvTable([
    { label: 'parseId', value: report.parseId },
    { label: 'locatorCoverage', html: formatScore(report.locatorCoverage) },
    report.captionPairingRate === undefined
      ? null
      : { label: 'captionPairingRate', html: formatScore(report.captionPairingRate) },
    report.tableDecompositionRate === undefined
      ? null
      : { label: 'tableDecompositionRate', html: formatScore(report.tableDecompositionRate) },
    { label: 'measuredAt', value: report.measuredAt },
    { label: 'reportHash', value: report.reportHash },
  ]);
  return (
    '<h3 class="subhead">conformanceReport</h3>' +
    scalars +
    numberMapMarkup('dimensions', report.dimensions, formatScore) +
    numberMapMarkup('unitTypeCounts', report.unitTypeCounts, escOr) +
    numberMapMarkup('relationshipTypeCounts', report.relationshipTypeCounts, escOr)
  );
}

/**
 * Render a `string -> number` map as a two-column table under its own subhead.
 * `format` renders the value cell (already-escaped markup), which is how the same
 * helper serves both integer counts and fractional dimension scores.
 */
function numberMapMarkup(title, map, format) {
  if (!map || typeof map !== 'object' || Array.isArray(map)) return '';
  const entries = Object.entries(map);
  if (entries.length === 0) return '';
  const rows = entries.map(([key, value]) => [`<code>${esc(key)}</code>`, format(value)]);
  return `<h3 class="subhead">${esc(title)}</h3>${dataTable([title, 'value'], rows)}`;
}

/**
 * Parse warnings, service order preserved. `locator` is omitted when absent and
 * is a typed union the UI does not interpret, so the raw array follows the table
 * for the warnings that carry one.
 */
function parseWarningsMarkup(warnings) {
  if (!Array.isArray(warnings) || warnings.length === 0) return '';
  const rows = warnings.map((warning) => [
    warningSeverityBadge(warning && warning.severity),
    `<code>${escOr(warning && warning.code)}</code>`,
    escOr(warning && warning.message),
    warning && warning.locator !== undefined ? badge('locator') : escOr(null),
  ]);
  return (
    '<h3 class="subhead">warnings</h3>' +
    dataTable(['severity', 'code', 'message', 'locator'], rows) +
    jsonTree(warnings, 'warnings', 0)
  );
}

/** Severity badge for a parse warning; `info` keeps the neutral styling. */
function warningSeverityBadge(severity) {
  if (severity === 'error') return badge('error', 'error');
  if (severity === 'warning') return badge('warning', 'context');
  return badge(severity || 'info');
}

/** The parse's counters. Every field is individually omitted when absent, so the
 *  present keys are iterated rather than listed. */
function parseMetricsMarkup(metrics) {
  if (!metrics || typeof metrics !== 'object' || Array.isArray(metrics)) return '';
  const rows = Object.entries(metrics).map(([name, value]) => ({ label: name, value }));
  if (rows.length === 0) return '';
  return `<h3 class="subhead">metrics</h3>${kvTable(rows)}`;
}

registerView('held', {
  title: 'Held parses',
  params: [],
  render: async (mount) => {
    await renderHeldParses(mount);
  },
});

// ---------------------------------------------------------------------------
// Operation viewer (PROTOCOL.md "GET /operations/{operationId}")
//
// One operation, one fetch, refreshed only when the operator asks: the service
// has no streaming surface and an automatic poll would keep hitting a bearer
// route after the operator stopped looking.
// ---------------------------------------------------------------------------

/** Render the operation panel and bind its refresh control; one write per fetch. */
async function renderOperationViewer(mount, operationId) {
  mount.innerHTML = panel(
    'Operation',
    operationToolbarMarkup(operationId) + `<div id="operation-body">${loadingMarkup('Loading operation')}</div>`
  );
  bindOperationRefresh(mount, operationId);
  await refreshOperation(operationId);
}

/** The manual-refresh control plus the id being viewed. */
function operationToolbarMarkup(operationId) {
  return `<div class="toolbar">
    <code>${esc(operationId)}</code>
    <button type="button" id="operation-refresh" class="secondary">Refresh</button>
    <span class="field-note" id="operation-fetched-at"></span>
  </div>`;
}

/** Bind the refresh button to an in-place re-fetch of the operation body. */
function bindOperationRefresh(mount, operationId) {
  const button = mount.querySelector('#operation-refresh');
  if (!button) return;
  button.addEventListener('click', () => {
    refreshOperation(operationId);
  });
}

/**
 * Fetch the operation and write it into the body container.
 *
 * Invariant: nothing is written unless the operation viewer for this same id is
 * still the active route — a slow response (or a click just before navigating
 * away) must not overwrite the next view's mount.
 */
async function refreshOperation(operationId) {
  const container = document.getElementById('operation-body');
  if (container) container.innerHTML = loadingMarkup('Loading operation');
  let markup;
  try {
    const operation = await apiGet(`/api/operations/${encodeURIComponent(operationId)}`);
    markup = operationMarkup(operation, operationId);
  } catch (error) {
    markup = adminErrorMarkup(error);
  }
  const route = parseHash(window.location.hash);
  if (route.name !== 'operation' || route.args[0] !== operationId) return;
  const target = document.getElementById('operation-body');
  if (target) target.innerHTML = markup;
  const stamp = document.getElementById('operation-fetched-at');
  if (stamp) stamp.textContent = `fetched ${new Date().toLocaleTimeString()}`;
}

/**
 * The Operation row. `startedAt` is omitted while pending, `completedAt` until
 * terminal, and `error` unless failed, so each is rendered only when present.
 * `succeeded` is a lifecycle outcome, not a parse verdict (PROTOCOL.md), which is
 * why the terminal-success note points at the held-parses list.
 */
function operationMarkup(operation, operationId) {
  if (!operation || typeof operation !== 'object') {
    return panel('Operation', `<p class="absent">No operation body for ${esc(operationId)}.</p>`, 'panel-error');
  }
  const rows = kvTable([
    { label: 'id', value: operation.id },
    { label: 'operationType', html: `<code>${escOr(operation.operationType)}</code>` },
    { label: 'status', html: operationStatusBadge(operation.status) },
    { label: 'targetObjectType', value: operation.targetObjectType },
    { label: 'targetObjectId', html: operationTargetMarkup(operation) },
    { label: 'createdAt', value: operation.createdAt },
    operation.startedAt === undefined ? null : { label: 'startedAt', value: operation.startedAt },
    operation.completedAt === undefined ? null : { label: 'completedAt', value: operation.completedAt },
    operation.error === undefined ? null : { label: 'error', value: operation.error },
  ]);
  const note =
    operation.status === 'succeeded'
      ? `<p class="field-note">A succeeded operation reports the pipeline lifecycle, not the domain verdict:
         check the ${routeLink('#/held', 'held parses', 'id-link')} for the parse outcome.</p>`
      : '';
  return rows + note;
}

/** Status badge for an Operation; `pending`/`running` are in-flight, not failures. */
function operationStatusBadge(status) {
  if (status === 'succeeded') return badge('succeeded', 'ok');
  if (status === 'failed') return badge('failed', 'error');
  if (status === 'running') return badge('running', 'anchor');
  if (status === 'pending') return badge('pending', 'context');
  return badge(status || 'unknown');
}

/**
 * The acted-on object. Only a `source` target has a view of its own, so every
 * other target type renders as a plain id rather than a link that would 404.
 */
function operationTargetMarkup(operation) {
  if (operation.targetObjectType === 'source') return sourceLink(operation.targetObjectId);
  return `<code>${escOr(operation.targetObjectId)}</code>`;
}

registerView('operation', {
  title: 'Operation',
  params: ['operationId'],
  render: async (mount, params) => {
    if (!params.operationId) {
      mount.innerHTML = panel(
        'Operation viewer',
        `<p>Open an operation by its <code>op_</code> id.</p>
         ${lookupForm('operation-lookup', 'Operation id', 'operation id')}`
      );
      return;
    }
    await renderOperationViewer(mount, params.operationId);
  },
});

// ---------------------------------------------------------------------------
// Vocabulary explorer (PROTOCOL.md "GET /annotations/vocabulary")
//
// `annotationType` is required by the service and selects between two response
// shapes that share one counter frame; `scope` defaults to `active` (active-parse
// rows only) and `all` additionally includes non-active-parse rows.
// ---------------------------------------------------------------------------

/**
 * Explorer state. Module-scoped so the toggle survives the router's full
 * re-render; `annotationType` is never blank because the service rejects a
 * missing one with 400.
 */
const vocabularyFilters = { annotationType: 'entity', scope: 'active' };

/** Render the explorer: filter form plus the current result, then bind the form. */
async function renderVocabularyExplorer(mount) {
  const results = await fetchVocabularyMarkup();
  mount.innerHTML = panel(
    'Vocabulary',
    vocabularyFilterFormMarkup() + `<div id="vocab-results">${results}</div>`
  );
  bindVocabularyFilters(mount);
}

/**
 * Fetch the selected vocabulary and return the result-container markup. A service
 * failure is returned as a rendered panel rather than thrown so the toggle stays
 * on screen — the 400 for an unrecognized scope and the 401/403 for the bearer
 * token are both fixed by acting on this same view.
 */
async function fetchVocabularyMarkup() {
  try {
    const body = await apiGet('/api/vocabulary', {
      annotationType: vocabularyFilters.annotationType,
      scope: vocabularyFilters.scope,
    });
    return vocabularyResultsMarkup(body);
  } catch (error) {
    return adminErrorMarkup(error);
  }
}

/** The entity/relation and active/all toggles, seeded from the live state. */
function vocabularyFilterFormMarkup() {
  const typeSelected = (value) => (vocabularyFilters.annotationType === value ? ' selected' : '');
  const scopeSelected = (value) => (vocabularyFilters.scope === value ? ' selected' : '');
  return `<form id="vocab-filters" class="filters">
    <div class="field-row">
      <label for="vocab-type">annotationType</label>
      <select id="vocab-type" name="annotationType">
        <option value="entity"${typeSelected('entity')}>entity</option>
        <option value="relation"${typeSelected('relation')}>relation</option>
      </select>
      <label for="vocab-scope">scope</label>
      <select id="vocab-scope" name="scope">
        <option value="active"${scopeSelected('active')}>active</option>
        <option value="all"${scopeSelected('all')}>all</option>
      </select>
      <button type="submit">Load</button>
    </div>
  </form>`;
}

/**
 * Bind the toggles to an in-place refresh of the result container. The
 * post-await route check prevents a slow response from writing into a view the
 * user has already left.
 */
function bindVocabularyFilters(mount) {
  const form = mount.querySelector('#vocab-filters');
  if (!form) return;
  form.addEventListener('submit', async (event) => {
    // No action attribute: applying the toggle is an API call, never a page load.
    event.preventDefault();
    const annotationType = form.elements.namedItem('annotationType');
    const scope = form.elements.namedItem('scope');
    if (annotationType && typeof annotationType.value === 'string') {
      vocabularyFilters.annotationType = annotationType.value;
    }
    if (scope && typeof scope.value === 'string') vocabularyFilters.scope = scope.value;
    const container = document.getElementById('vocab-results');
    if (container) container.innerHTML = loadingMarkup('Loading vocabulary');
    const markup = await fetchVocabularyMarkup();
    if (parseHash(window.location.hash).name !== 'vocab') return;
    const target = document.getElementById('vocab-results');
    if (target) target.innerHTML = markup;
  });
}

/**
 * The counter frame both shapes share, then the groups table for whichever shape
 * the response echoed. The response's own `annotationType` selects the columns,
 * not the local toggle, so a result rendered before a toggle change still matches
 * its own data.
 */
function vocabularyResultsMarkup(body) {
  if (!body || typeof body !== 'object') return '<p class="absent">No vocabulary body.</p>';
  const groups = Array.isArray(body.groups) ? body.groups : [];
  const frame = kvTable([
    { label: 'annotationType', html: badge(body.annotationType || 'unknown') },
    { label: 'scope', html: badge(body.scope || 'unknown') },
    { label: 'groupCount', value: body.groupCount },
    { label: 'rowsRead', value: body.rowsRead },
    { label: 'skippedMarkerCount', value: body.skippedMarkerCount },
    { label: 'malformedRowCount', value: body.malformedRowCount },
    {
      label: 'truncated',
      // truncated means a row or group cap was hit, so the groups below are a
      // partial view of the corpus — an error-styled badge, not a plain boolean.
      html: body.truncated === true ? badge('truncated', 'error') : badge('complete', 'ok'),
    },
  ]);
  if (groups.length === 0) return frame + '<p class="absent">No groups.</p>';
  const table =
    body.annotationType === 'relation' ? relationGroupsMarkup(groups) : entityGroupsMarkup(groups);
  return frame + table;
}

/** Entity groups, keyed by `normalizedName` and sorted by the service. */
function entityGroupsMarkup(groups) {
  const rows = groups.map((group) => [
    esc(group && group.normalizedName),
    stringListMarkup(group && group.entityTypes),
    escOr(group && group.totalCount),
    escOr(group && group.sourceCount),
    countedListMarkup(group && group.rawForms, 'rawForm'),
    countedListMarkup(group && group.modelCounts, 'modelName'),
  ]);
  return dataTable(
    ['normalizedName', 'entityTypes', 'totalCount', 'sourceCount', 'rawForms', 'modelCounts'],
    rows
  );
}

/** Relation groups, keyed by the normalized `predicate` and sorted by the service. */
function relationGroupsMarkup(groups) {
  const rows = groups.map((group) => [
    esc(group && group.predicate),
    escOr(group && group.totalCount),
    escOr(group && group.sourceCount),
    countedListMarkup(group && group.rawForms, 'rawForm'),
    countedListMarkup(group && group.modelCounts, 'modelName'),
  ]);
  return dataTable(['predicate', 'totalCount', 'sourceCount', 'rawForms', 'modelCounts'], rows);
}

/** A plain string array as inline code chips; an empty array gets the placeholder. */
function stringListMarkup(values) {
  if (!Array.isArray(values) || values.length === 0) return escOr(null);
  return values.map((value) => `<code>${escOr(value)}</code>`).join(' ');
}

/**
 * A `{ <nameKey>, count }` array as `name ×count` chips. Shared by `rawForms` and
 * `modelCounts`, which differ only in the name field; `(unknown)` is a real
 * `modelName` the service emits for rows without one.
 */
function countedListMarkup(entries, nameKey) {
  if (!Array.isArray(entries) || entries.length === 0) return escOr(null);
  return entries
    .map(
      (entry) =>
        `<span class="count-chip"><code>${escOr(entry && entry[nameKey])}</code> ×${escOr(entry && entry.count)}</span>`
    )
    .join(' ');
}

registerView('vocab', {
  title: 'Vocabulary',
  params: [],
  render: async (mount) => {
    await renderVocabularyExplorer(mount);
  },
});

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

/**
 * Delegated submit handler for the shared lookup forms. Registered once on the
 * document so it survives every re-render of the mount; the form id names the
 * route the entered id belongs to.
 */
document.addEventListener('submit', (event) => {
  const form = event.target;
  if (!(form instanceof HTMLFormElement) || !form.classList.contains('lookup')) return;
  const routeByForm = {
    'unit-lookup': 'unit',
    'source-lookup': 'source',
    'operation-lookup': 'operation',
  };
  const route = routeByForm[form.id];
  if (!route) return;
  event.preventDefault();
  const value = new FormData(form).get('id');
  const id = typeof value === 'string' ? value.trim() : '';
  if (id === '') return;
  navigate(`#/${route}/${encodeURIComponent(id)}`);
});

startRouter();
