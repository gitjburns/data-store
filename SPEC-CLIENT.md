# Data Store CLI Client Specification

## Purpose

Build a simple interactive command-line client for operating the standalone Data
Store service. The client is an operator/developer interface over the service's
documented HTTP API. It must not introduce a second authentication path,
service-side backdoor, hidden client state, or alternate domain behavior.

The service remains the source of truth for validation, persistence, retrieval,
version management, and shutdown behavior. The client only sends HTTP requests,
renders responses for humans, and reads the configured runtime admin token file
when protected admin commands require authentication.

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
  token for protected admin commands.

The client must not require operators to manually provide the admin token during
normal use.

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

- On startup, the service generates its startup-scoped admin token as it does
  today.
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

## Interaction Model

The client is a REPL-style interactive CLI, not a menu-driven TUI.

The prompt should be concise and stable, for example:

```text
data-store>
```

The REPL supports readline-like editing and history through `rustyline`.

History behavior:

- Command history should persist across client runs if supported directly by
  `rustyline` with low implementation effort.
- The history file path is fixed at:

```text
service/data-store/.data-store.history
```

- The history file is not configured in the service config.
- The history file must be gitignored.

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
rollback The_Elements_of_Style.pdf 2026-06-01T21:37:22.184Z
```

Invalid parsing must produce a clear client-side error without sending an HTTP
request.

## Commands

The first client version exposes only the current documented service API plus
basic REPL controls.

### `health`

Calls:

```http
GET /v1/health
```

Authentication: none.

Output: human-readable readiness summary and component details.

### `limits`

Calls:

```http
GET /v1/limits
```

Authentication: none.

Output: labeled request and retrieval limits.

### `ingest <source>`

Calls:

```http
POST /v1/ingest
```

Authentication: none.

Output: document ID, version label, units ingested, and status.

### `search <query> [topK]`

Calls:

```http
POST /v1/search
```

Authentication: none.

Output: ranked results with score, source path, unit ID, page numbers, heading
path when present, and a bounded excerpt of matched content.

The client rendering is excerpted only; service search behavior is unchanged.

### `search-full <query> [topK]`

Calls:

```http
POST /v1/search
```

Authentication: none.

Output: same metadata as `search`, but prints the full matched unit content for
each result.

The client rendering is full-content only; service search behavior is unchanged.

### `versions`

Calls:

```http
GET /admin/document-versions
```

Authentication: bearer token read from `admin.token_file_path`.

Output: grouped source-document version listing. Active versions must be clearly
marked. Include version label, document ID, status, units ingested, timestamps,
and vector metadata counts/dimensions.

### `rollback <source> <versionLabel>`

Calls:

```http
POST /admin/document-versions/rollback
```

Authentication: bearer token read from `admin.token_file_path`.

Output: source path, active version label, publish timestamp, vector count, and
status.

### `shutdown`

Calls:

```http
POST /admin/shutdown
```

Authentication: bearer token read from `admin.token_file_path`.

Because this command stops the running service, the REPL must ask for typed
confirmation before sending the request. Confirmation should require an explicit
word such as:

```text
shutdown
```

Output: shutdown status.

### `help`

Prints the available commands, syntax, and a short description of each command.

### `exit`

Exits the REPL cleanly.

Aliases such as `quit` may be accepted if they do not complicate the parser, but
they are not required.

## Output Requirements

Output is human-readable only. The client must not include a JSON output mode in
the first version.

Client output should be concise but complete enough for operation:

- Show HTTP status and service error message for failed API calls.
- Do not hide service errors behind generic client messages.
- Do not print the bearer token.
- Do not log the bearer token.
- Do not print raw JSON responses as the normal interface.

Search result excerpts should be long enough to be useful in a terminal while
preventing accidental output floods. The exact excerpt length is an
implementation detail, but it should be a fixed constant in the client.

## HTTP Behavior

The client uses the service HTTP protocol as documented in `PROTOCOL.md`.

Base URL construction:

- Use `server.bind_address` from config.
- Construct `http://<bind_address>`.
- HTTPS support is not required for the first version.

Protected admin requests use:

```http
Authorization: Bearer <token>
```

The token is read from the configured token file immediately before admin
commands that need it. This allows the client to handle service restarts during
a long client session without caching a stale token.

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
- Token file missing for an admin command.
- Token file unreadable.
- Invalid REPL command syntax.
- HTTP connection failure.
- Non-success HTTP response.

For non-success HTTP responses, print:

- HTTP status code.
- Service error message when the response body contains the documented error
  shape.
- Raw response text only as a fallback when the body cannot be parsed into a
  known error shape.

## Documentation Updates

Implementation should update the operator documentation to describe:

- The `data-store` client binary.
- How to run it with `--config config.toml`.
- The required `[admin] token_file_path` config.
- Token-file lifecycle and security behavior.
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
- Long-lived static admin secret.
- Shell command execution.
- Pipes, redirects, globbing, variables, or scripting language features.
- Hidden fallback behavior if token-file creation fails.
- Frontend or Node backend integration.

## Verification

After implementation, run from `service/data-store/`:

```bash
cargo fmt
cargo check
cargo check --features metal
```

Manual verification should include starting the service, running the client,
checking public commands, checking admin commands through the token file, and
confirming graceful shutdown removes the current token file.
