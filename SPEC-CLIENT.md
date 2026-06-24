# Data Store CLI Client Specification

## Purpose

Build a simple command-line client for operating the standalone Data Store
service. The client supports both interactive REPL use and non-interactive
one-shot operation invocation. Both modes are operator/developer interfaces over
the same documented operation protocol used by all consumers.

The client must not introduce a second authentication path, service-side
backdoor, hidden client state, or alternate domain behavior. The service remains
the source of truth for validation, persistence, retrieval, version management,
progress reporting, and shutdown behavior.

## Binary

The client binary name is:

```bash
data-store
```

The service binary remains:

```bash
data-store-service
```

The client is a separate binary target from the service so launching the client
cannot accidentally start, fork, initialize, smoke-test, or set up the service.

Expected development run command:

```bash
cargo run --bin data-store -- --config config.toml
```

Expected non-interactive development command:

```bash
cargo run --bin data-store -- --config config.toml --health
```

## Startup Configuration

The client starts from the same service config file used by the service:

```bash
data-store --config config.toml
```

If `--config` is omitted, the client uses `config.toml` in the Rust service
working directory.

The client reads:

- `server.bind_address` to construct the base HTTP URL.
- `admin.token_file_path` to locate the current startup-scoped admin bearer
  token for protected operations.
- `client.operation_timeout_seconds` to bound one operation-stream request.

The client must not require operators to manually provide the admin token during
normal use.

The local usage command does not require config or a running service:

```bash
data-store --help
```

## Admin Token File

The service config must contain a required admin section:

```toml
[admin]
# Runtime admin bearer token file. Relative paths resolve from the Rust service root.
token_file_path = ".data-store-admin-token"
```

`admin.token_file_path` is required. Missing or empty values are fatal service
configuration errors.

Relative token file paths resolve from the Rust service root. The example config
must include the recommended explicit relative value shown above so new
installers can use the example as-is while still seeing and controlling the
credential handoff path.

The token remains the only admin authentication mechanism. The token file is
only a local credential handoff mechanism for the client.

Service token-file lifecycle:

- On startup, the service generates its startup-scoped admin token.
- The service writes that token to the configured token file.
- The service replaces any stale token file from an earlier run.
- The token file must be created with owner-only permissions.
- Token-file creation is startup-critical; if the service cannot write the file
  securely, startup fails clearly.
- The service still prints `admin_shutdown_token=<token>` to stdout.
- On graceful shutdown, the service removes the token file if it still contains
  the current token.
- If the service crashes, a stale token file may remain. That stale token is not
  accepted by any later service process and is replaced on the next startup.

The recommended token file must be gitignored.

## Service Protocol

The client uses the same operation protocol documented in `PROTOCOL.md`:

```http
POST /v1/operations
Accept: application/x-ndjson
Content-Type: application/json
```

Every client command sends one operation request and reads the streamed NDJSON
operation events until the service emits a terminal `result` or `error` event.

Protected operations use:

```http
Authorization: Bearer <token>
```

The token is read from the configured token file immediately before each
protected operation. The client must not cache the token for the whole REPL
session.

The control endpoint is reserved for cancellation or future client-to-server
operation messages:

```http
POST /v1/operations/{operationId}/control
```

The first client version does not need an interactive cancel command unless the
service implementation supports cancellation.

## Interaction Model

The client supports two interaction modes:

- With no operation flag, it starts a REPL-style interactive CLI.
- With exactly one operation flag, it sends that operation once and exits after
  the terminal result or error.

It is not a menu-driven TUI.

The prompt should be concise and stable:

```text
data-store>
```

The REPL supports readline-like editing and history through `rustyline`.

History behavior:

- Command history should persist across client runs if supported directly by
  `rustyline` with low implementation effort.
- The history file path is fixed at:

```text
./.data-store.history
```

- The history file is not configured in the service config.
- The history file must be gitignored.

## Non-Interactive Invocation

Non-interactive invocation uses the same command semantics and renderers as the
REPL commands, but receives arguments from process argv instead of the REPL
line parser.

Supported operation flags:

```bash
data-store [--config <path>] --health
data-store [--config <path>] --limits
data-store [--config <path>] --sources
data-store [--config <path>] --ingest <source> [--force]
data-store [--config <path>] --search <query> [topK]
data-store [--config <path>] --search-full <query> [topK]
data-store [--config <path>] --versions
data-store [--config <path>] --rollback <source> <versionLabel>
data-store [--config <path>] --shutdown
```

Only one operation flag may be provided per process invocation. Public flags
send unauthenticated operations. Protected flags read the configured token file
immediately before the request, exactly like their REPL equivalents.

Shells perform argument splitting before the client receives argv, so
multi-word queries and paths containing spaces must be quoted by the operator:

```bash
data-store --config config.toml --search "clear writing style rules" 3
```

## Input Parsing

The REPL uses simple shell-like parsing:

- Arguments are separated by whitespace.
- Double quotes allow spaces inside one argument.
- Backslash escapes are supported inside quoted strings for at least `\"` and
  `\\`.
- No shell execution, variables, redirects, pipes, globbing, command
  substitution, or multiline input.

Examples:

```text
search "clear writing style rules" 3
ingest The_Elements_of_Style.pdf
ingest The_Elements_of_Style.pdf --force
rollback The_Elements_of_Style.pdf 2026-06-01T21:37:22.184Z
```

Invalid parsing must produce a clear client-side error without sending an HTTP
request.

## Commands

The client exposes the service operations plus basic REPL controls. Each service
operation has both a REPL command and a matching non-interactive flag.

### `health` / `--health`

Operation: `health`

Authentication: none.

Payload:

```json
{}
```

Output: human-readable readiness summary and component details.

### `limits` / `--limits`

Operation: `limits`

Authentication: none.

Payload:

```json
{}
```

Output: labeled request and retrieval limits.

### `sources` / `--sources`

Operation: `sources`

Authentication: none.

Payload:

```json
{}
```

Output: active ingested source listing. Include source path, active version
label, document ID, status, units ingested, and timestamps.

### `ingest <source> [--force]` / `--ingest <source> [--force]`

Operation: `ingest`

Authentication: none.

Payload:

```json
{
  "source": "<source>",
  "force": true
}
```

Output: streamed operation status and progress, followed by document ID,
version label, units ingested, and status.

The client sends `force: true` only when `--force` is provided.
When `--force` is omitted and the service returns
`kind: "source_already_ingested"`, output the service message:
`Source <source> is already ingested. Use --force to override.`

### `search <query> [topK]` / `--search <query> [topK]`

Operation: `search`

Authentication: none.

Payload:

```json
{
  "query": "<query>",
  "topK": 3
}
```

Output: streamed operation status and progress, followed by ranked results with
score, source path, unit ID, page numbers, heading path when present, and a
bounded excerpt of matched content.

After search results, the client prints a `Benchmarks:` summary. The
`retrieving_candidates` row is the client-observed duration for that streamed
operation stage. When the response includes `raw.storage.retrieval` timing
fields, the client must print indented child rows beneath
`retrieving_candidates` for:

- `retrieving_candidates.query_vector_validation`
- `retrieving_candidates.dense_scan`
- `retrieving_candidates.bm25`
- `retrieving_candidates.rrf_fusion`
- `retrieving_candidates.candidate_materialization`
- `retrieving_candidates.raw_diagnostics`

The child rows are server-reported diagnostic timings, not independent stream
stages.

The client rendering is excerpted only; service search behavior is unchanged.

### `search-full <query> [topK]` / `--search-full <query> [topK]`

Operation: `search`

Authentication: none.

Payload is the same as `search`.

Output: same metadata as `search`, but prints the full matched unit content for
each result.

The client rendering is full-content only; service search behavior is
unchanged.

### `versions` / `--versions`

Operation: `versions`

Authentication: bearer token read from `admin.token_file_path`.

Payload:

```json
{}
```

Output: grouped source-document version listing. Active versions must be clearly
marked. Include version label, document ID, status, units ingested, timestamps,
and vector metadata counts/dimensions.

### `rollback <source> <versionLabel>` / `--rollback <source> <versionLabel>`

Operation: `rollback`

Authentication: bearer token read from `admin.token_file_path`.

Payload:

```json
{
  "source": "<source>",
  "versionLabel": "<versionLabel>"
}
```

Output: streamed operation status, followed by source path, active version
label, publish timestamp, vector count, and status.

### `shutdown` / `--shutdown`

Operation: `shutdown`

Authentication: bearer token read from `admin.token_file_path`.

Payload:

```json
{}
```

Output: streamed operation status followed by the server-authored
`shutdown_complete` terminal result.

### `help`

Prints the available REPL commands and syntax.

### `--help`

Prints executable-level usage without reading config or contacting the service.

### `exit`

Exits the REPL cleanly.

Aliases such as `quit` may be accepted if they do not complicate the parser.

## Output Requirements

Output is human-readable only. The client must not include a JSON output mode in
the first version.

Client output should be concise but complete enough for operation:

- Show operation start, status, progress, terminal result, and elapsed time.
- Render status and counted progress events compactly, including `current` and
  `total` when the service provides them.
- Finalize any overwritten progress line before printing the next status,
  result, or error.
- Show HTTP method and URL for transport failures.
- Show operation, stage, server error kind, status, and message for operation
  errors.
- Print the full client-side cause chain for transport, response parsing, and
  stream parsing failures.
- Do not print the bearer token.
- Do not log the bearer token.
- Do not print raw JSON responses as the normal interface.
- Use raw search diagnostics only to render documented human-readable summaries,
  such as retrieval benchmark child rows.

Search result excerpts should be long enough to be useful in a terminal while
preventing accidental output floods. The exact excerpt length is an
implementation detail, but it should be a fixed constant in the client.

## HTTP Behavior

The client uses the operation protocol as documented in `PROTOCOL.md`.

Base URL construction:

- Use `server.bind_address` from config.
- Construct `http://<bind_address>`.
- If the configured bind host is an unspecified address such as `0.0.0.0` or
  `::`, connect to the equivalent loopback address on the same port.
- HTTPS support is not required for the first version.

The client should use a real HTTP client dependency rather than hand-rolled
`TcpStream` HTTP.

Approved dependency:

- `reqwest`

## Error Handling

Client-side errors must be clear and must not look like successful service
responses.

Examples:

- Config file missing or invalid.
- Missing required config fields.
- Token file missing for a protected operation.
- Token file unreadable.
- Invalid command syntax.
- HTTP connection failure.
- Operation stream line is not valid JSON.
- Operation stream ends before a terminal event.
- Terminal operation error event.

For terminal operation errors, print:

- Operation name.
- Stage when available.
- Error kind.
- Error status.
- Error message.

For transport and stream errors, print:

- HTTP method.
- URL.
- Top-level error.
- Cause chain.

## Documentation Updates

Implementation should update the operator documentation to describe:

- The `data-store` client binary.
- How to run it with `--config config.toml`.
- How to invoke one-shot commands such as `--health`, `--ingest`, `--search`,
  and `--shutdown`.
- The required `[admin] token_file_path` config.
- Token-file lifecycle and security behavior.
- The operation-stream protocol at a user-facing level.
- The REPL commands.
- The `.data-store.history` and `.data-store-admin-token` gitignored local
  files.

## Non-Goals

The first version must not include:

- Menu-driven TUI.
- JSON output mode.
- Persistent client config.
- Client-side saved base URLs or saved tokens.
- Alternate admin authentication.
- Unix socket admin channel.
- External consumer integration.
