# Data Store CLI Client Implementation Plan

## Goal

Implement the `data-store` interactive CLI client described in
`SPEC-CLIENT.md`, plus the service-side token-file support required for the
client to authenticate admin API calls without manual token entry.

The work must preserve the service as the single source of truth. The client
only reads config, reads the runtime token file for admin commands, sends HTTP
requests, and renders responses for humans.

The next approved design direction migrates the CLI from route-specific HTTP
calls to the universal streamed operation protocol documented in `PROTOCOL.md`.

## Status

Initial implementation complete. Operation-stream migration is the next planned
scope.

Completed:

- Required `[admin].token_file_path` config and example value.
- Service runtime admin token-file write, stale replacement, and graceful
  shutdown cleanup.
- Separate `data-store` REPL client binary.
- Readline-style editing with persistent `.data-store.history`.
- Public and admin HTTP commands listed in this plan.
- Human-readable response rendering.
- Documentation and local gitignore updates.
- Cargo dependency and lockfile updates.

Verified:

```bash
cargo fmt
cargo check
cargo check --bin data-store
cargo check --features metal
```

Not yet manually verified against a running service.

Next scope:

- Replace route-specific client calls with `POST /v1/operations`.
- Render operation-scoped NDJSON `status`, `progress`, `result`, and `error`
  events.
- Improve transport, stream, and service-error reporting.
- Keep protected operations on the startup bearer token read from the runtime
  token file.

## Historical Scope

Included:

- Required `[admin] token_file_path` service config.
- Startup-critical service token-file creation with owner-only permissions.
- Graceful-shutdown token-file cleanup.
- Separate `data-store` client binary.
- REPL with readline-style editing and persistent local history.
- API-only command set from the spec.
- Human-readable output only.
- Documentation and gitignore updates.

Excluded:

- JSON output mode.
- Menu-driven TUI.
- Persistent client config.
- Saved client base URL or saved client token.
- Alternate admin authentication.
- Unix socket admin channel.
- External consumer integration.

The following phases document the completed initial client implementation. They
are retained as implementation history; the next target contract is the
operation-stream migration below.

## Historical Phase 1: Config And Local Files

Files:

- `service/data-store/src/config.rs`
- `service/data-store/config.example.toml`
- `service/data-store/.gitignore`

Changes:

1. Add required `AdminConfig` to `ServiceConfig`.
2. Add required `admin.token_file_path: PathBuf`.
3. Validate `admin.token_file_path` as non-empty.
4. Resolve relative token-file paths from the Rust service root.
5. Add `[admin] token_file_path = ".data-store-admin-token"` to
   `config.example.toml`.
6. Add `.data-store-admin-token` and `.data-store.history` to
   `service/data-store/.gitignore`.

Design notes:

- The token file path is explicit in config and fatal if missing.
- Relative resolution matches the existing service-root convention for
  `logging.file_path`.
- The history file is intentionally not configurable.

## Historical Phase 2: Service Token-File Lifecycle

Files:

- `service/data-store/src/main.rs`

Changes:

1. After generating the startup-scoped admin token, write it to the configured
   token file before the service becomes ready.
2. Create parent directories when needed.
3. Write the token file with owner-only permissions.
4. Replace stale token files from earlier runs.
5. Keep the existing stdout line:

   ```text
   admin_shutdown_token=<token>
   ```

6. Add startup handoff output that reports the resolved token-file path and
   successful token-file creation without printing the token again.
7. On graceful shutdown, remove the token file only if it still contains the
   current token.

Design notes:

- Token-file creation is startup-critical.
- Token-file cleanup must not delete a token file created by a newer service
  process.
- The token remains the only admin authentication mechanism.
- The token must not be written to service logs.

## Historical Phase 3: Dependencies And Binary Target

Files:

- `service/data-store/Cargo.toml`
- `service/data-store/Cargo.lock`

Changes:

1. Add a separate binary target:

   ```toml
   [[bin]]
   name = "data-store"
   path = "src/bin/data-store.rs"
   ```

2. Add `reqwest` for HTTP requests.
3. Add `rustyline` for interactive input, editing, and history.

Design notes:

- The service binary remains `data-store-service`.
- The client binary does not call service startup code.
- `reqwest` should be configured for blocking JSON HTTP to keep the REPL
  implementation straightforward.

## Historical Phase 4: Client Config Loading

Files:

- `service/data-store/src/bin/data-store.rs`

Changes:

1. Parse client startup arguments:

   ```bash
   data-store --config config.toml
   ```

2. Default `--config` to `config.toml`.
3. Load the TOML config.
4. Read only the client-needed config fields:

   - `server.bind_address`
   - `admin.token_file_path`

5. Resolve relative `admin.token_file_path` from the Rust service root.
6. Construct the base URL as:

   ```text
   http://<server.bind_address>
   ```

Design notes:

- The client can define small local config structs for the fields it needs.
- Config errors should be clear and should exit before entering the REPL.
- HTTPS is not required in the first version.

## Historical Phase 5: REPL And Parser

Files:

- `service/data-store/src/bin/data-store.rs`

Changes:

1. Start a `rustyline` REPL with prompt:

   ```text
   data-store>
   ```

2. Load history from:

   ```text
   .data-store.history
   ```

   resolved from the Rust service root.

3. Save history on clean exit.
4. Implement simple shell-like parsing:

   - whitespace-separated arguments;
   - double-quoted arguments;
   - `\"` and `\\` escapes inside quoted arguments;
   - no shell execution or expansion.

5. Print clear client-side errors for invalid input.

Design notes:

- Invalid input must not send HTTP requests.
- The parser should stay small and command-focused.
- `exit` exits cleanly. `help` prints command syntax.

## Historical Phase 6: HTTP Client Layer

Files:

- `service/data-store/src/bin/data-store.rs`

Changes:

1. Create a blocking `reqwest` client.
2. Implement helpers for:

   - public `GET`;
   - public `POST` JSON;
   - admin `GET`;
   - admin `POST` JSON;
   - admin `POST` with no body.

3. Read the bearer token from `admin.token_file_path` immediately before each
   admin command.
4. Add `Authorization: Bearer <token>` only for admin commands.
5. Parse success responses into client DTOs.
6. Parse error responses into the documented service error shape when possible.

Design notes:

- The client must not cache the token for the whole REPL session.
- The client must not print or log the token.
- Non-success responses should print status and service error details.

## Historical Phase 7: Commands And Rendering

Files:

- `service/data-store/src/bin/data-store.rs`

Commands:

1. `health`
   - `GET /v1/health`
   - Render readiness and component details.

2. `limits`
   - `GET /v1/limits`
   - Render request and retrieval limits as labeled rows.

3. `ingest <source>`
   - `POST /v1/ingest`
   - Render document ID, version label, units ingested, and status.

4. `search <query> [topK]`
   - `POST /v1/search`
   - Render ranked results with metadata and fixed-length excerpts.

5. `search-full <query> [topK]`
   - `POST /v1/search`
   - Render ranked results with metadata and full content.

6. `versions`
   - `GET /admin/document-versions`
   - Render sources grouped by source path.
   - Clearly mark active versions.
   - Include version label, document ID, status, units ingested, timestamps,
     and vector metadata counts/dimensions.

7. `rollback <source> <versionLabel>`
   - `POST /admin/document-versions/rollback`
   - Render source path, active version label, published timestamp, vector
     count, and status.

8. `shutdown`
   - Ask for typed confirmation.
   - `POST /admin/shutdown`
   - Render shutdown status.

9. `help`
   - Render command list and syntax.

10. `exit`
    - Save history and exit.

Design notes:

- Output is human-readable only.
- No JSON output command or flag.
- `search` and `search-full` differ only in client rendering.

## Historical Phase 8: Documentation

Files:

- `service/data-store/README.md`
- `service/data-store/ARCHITECTURE.md`
- `service/data-store/PROTOCOL.md`

Changes:

1. Document the required `[admin] token_file_path` config.
2. Document token-file lifecycle and security behavior.
3. Document the `data-store` client binary and run command.
4. Document the REPL commands.
5. Document the local ignored files:

   - `.data-store-admin-token`
   - `.data-store.history`

6. Update startup documentation to mention both stdout token printing and
   token-file writing.

Design notes:

- `PROTOCOL.md` should stay focused on HTTP protocol. Only update it if the
  token-file behavior needs to be mentioned as an operator/client integration
  note.
- `README.md` should be the main operator runbook for the client.
- `ARCHITECTURE.md` should capture the token-file invariant and client boundary.

## Historical Phase 9: Verification

Run from `service/data-store/`:

```bash
cargo fmt
cargo check
cargo check --features metal
```

Manual verification:

1. Confirm `config.example.toml` contains `[admin] token_file_path`.
2. Start the service in foreground with a config that includes the admin token
   file.
3. Confirm startup prints `admin_shutdown_token=<token>`.
4. Confirm the configured token file exists and is owner-only.
5. Start the client:

   ```bash
   cargo run --bin data-store -- --config config.toml
   ```

6. Run public commands:

   ```text
   health
   limits
   ```

7. Run admin command:

   ```text
   versions
   ```

8. Run `shutdown`, confirm the typed confirmation works, and verify graceful
   shutdown removes the current token file.
9. Restart the service and verify a stale token file is replaced.
10. Restart the client and confirm command history is available.

## Next Scope: Operation-Stream Protocol Migration

Migrate the client to the universal streamed operation protocol documented in
`PROTOCOL.md` and `SPEC-CLIENT.md`.

This is a protocol migration, not a new client-specific API. The CLI must use
the same `/v1/operations` NDJSON stream protocol available to all consumers.

### Included

- `POST /v1/operations` request envelope.
- Operation-scoped NDJSON response stream.
- `status`, `progress`, `result`, and `error` events.
- Structured operation errors with `status`, `kind`, and `message`.
- Operations: `health`, `limits`, `ingest`, `search`, `versions`, `rollback`,
  and `shutdown`.
- Bearer-token authentication for protected operations only.
- CLI rendering of status/progress/result/error events.
- Better client transport, stream, and service-error reporting.
- Loopback connection mapping for unspecified bind addresses.
- Reserved operation control endpoint shape.

### Excluded

- JSON output mode.
- Menu-driven TUI.
- Persistent client config.
- Saved client base URL or saved client token.
- Alternate admin authentication.
- Unix socket admin channel.
- External consumer integration.
- Guaranteed cancellation support in the first implementation.

### Phase 10: Protocol Types

Files:

- `service/data-store/src/types.rs`
- `service/data-store/src/error.rs`

Changes:

1. Add the operation request envelope with optional `operationId`, operation
   name, and payload.
2. Add operation event DTOs for `status`, `progress`, `result`, and `error`.
3. Add structured error fields: `status`, `kind`, and `message`.
4. Expose an `ApiError` conversion path for terminal operation errors and
   pre-stream HTTP failures.

Design notes:

- The event stream is NDJSON, one event per line.
- `sequence` must be monotonic per operation.
- Terminal events are exactly `result` or `error`.
- Operation error messages must not expose bearer tokens.

### Phase 11: Operation Dispatcher

Files:

- `service/data-store/src/http.rs`

Changes:

1. Add `POST /v1/operations`.
2. Parse the operation envelope.
3. Dispatch by operation name.
4. Validate protected operation auth before starting protected work.
5. Emit initial status events after the stream opens.
6. Emit terminal result events for success.
7. Emit terminal error events for operation failures.

Operations:

- `health`
- `limits`
- `ingest`
- `search`
- `versions`
- `rollback`
- `shutdown`

Design notes:

- Authentication is operation-specific, not endpoint-specific.
- Public operations must remain usable without a bearer token.
- Protected operations must use the same startup bearer token as current admin
  controls.

### Phase 12: Stream Implementation

Files:

- `service/data-store/src/http.rs`
- `service/data-store/Cargo.toml`
- `service/data-store/Cargo.lock`

Changes:

1. Implement streaming HTTP responses with `application/x-ndjson`.
2. Add a small streaming helper dependency only if the existing stack cannot do
   this cleanly.
3. Ensure every emitted event is flushed promptly.
4. Ensure operation errors after stream open are written as terminal error
   events.
5. Ensure errors before stream open still return structured HTTP error bodies.

Design notes:

- Use a bounded internal channel if needed to bridge blocking operation work and
  async response streaming.
- Do not hold global locks while writing stream events.
- Every event line must be complete JSON plus newline.

### Phase 13: Progress Instrumentation

Files:

- `service/data-store/src/http.rs`
- `service/data-store/src/storage.rs`
- `service/data-store/src/inference/*.rs` as needed

Changes:

1. Add status/progress callbacks to operation pipelines.
2. Instrument ingest:
   - source resolution
   - Docling conversion
   - unit splitting
   - dense embedding per unit
   - ColBERT embedding per unit
   - storage publish
3. Instrument search:
   - query embedding
   - storage candidate retrieval
   - ColBERT candidate scoring
   - reranker scoring
   - result assembly
4. Instrument versions, rollback, shutdown, health, and limits with at least
   start/completion status events.

Design notes:

- Progress must reflect real server-side stages.
- Counted progress should include `current` and `total`.
- Do not invent fake percentages.
- Keep progress messages stable and concise.

### Phase 14: Client Operation Transport

Files:

- `service/data-store/src/bin/data-store.rs`

Changes:

1. Replace route-specific request helpers with one operation request helper.
2. Send `POST /v1/operations` with `Accept: application/x-ndjson`.
3. Add `Authorization: Bearer <token>` only for protected operations.
4. Read protected-operation tokens immediately before the request.
5. Parse the streamed response line by line.
6. Require a terminal `result` or `error` event.
7. Deserialize result payloads into existing command-specific DTOs.

Design notes:

- The CLI must not cache the admin token across commands.
- The CLI must not print or log the admin token.
- The CLI should continue using blocking `reqwest` unless streaming support
  requires an async client.

### Phase 15: Client Rendering

Files:

- `service/data-store/src/bin/data-store.rs`

Changes:

1. Render operation start with operation name and target URL.
2. Render `status` events as normal lines.
3. Render counted `progress` events by overwriting the current line.
4. Finalize any active progress line before printing status, result, or error.
5. Render terminal `result` payloads with existing human-readable command
   renderers.
6. Render terminal `error` payloads with operation, stage, status, kind, and
   message.
7. Print elapsed time after terminal result or error.

Design notes:

- Output is human-readable only.
- No JSON output mode.
- `search` and `search-full` differ only in result rendering.

### Phase 16: Client Error Reporting

Files:

- `service/data-store/src/bin/data-store.rs`

Changes:

1. Include HTTP method and URL in transport errors.
2. Include the full cause chain for client-side failures.
3. Detect and report:
   - connection refused
   - invalid URL
   - response status failures before stream parsing
   - invalid NDJSON line
   - missing terminal event
   - unexpected event type
   - result payload deserialization failure
4. Map configured bind addresses `0.0.0.0:<port>` and `[::]:<port>` to
   loopback connection URLs.

Design notes:

- The REPL should not collapse errors to a single vague line.
- Error output must remain concise enough for operators.

### Phase 17: Control Endpoint Placeholder

Files:

- `service/data-store/src/http.rs`
- `service/data-store/src/types.rs`

Changes:

1. Add `POST /v1/operations/{operationId}/control`.
2. Parse a control envelope with at least `{ "type": "cancel" }`.
3. Return an explicit error for unsupported control messages or unknown
   operation IDs if cancellation is not implemented yet.

Design notes:

- This preserves the bidirectional protocol shape without forcing cancellation
  support into the first implementation.
- Future client-to-server operation messages should use this endpoint.

### Phase 18: Documentation

Files:

- `service/data-store/PROTOCOL.md`
- `service/data-store/SPEC-CLIENT.md`
- `service/data-store/PLAN-CLIENT.md`
- `service/data-store/README.md`
- `service/data-store/ARCHITECTURE.md`

Changes:

1. Document `/v1/operations`.
2. Document NDJSON event types.
3. Document protected operation auth.
4. Document CLI streamed status/progress behavior.
5. Keep CLI instructions focused on operation-stream behavior.
6. Keep README concise and operator-focused.

### Phase 19: Operation-Stream Verification

Run from `service/data-store/`:

```bash
cargo fmt
cargo check
cargo check --bin data-store
cargo check --features metal
```

Manual verification:

1. Start the service with a config that includes the admin token file.
2. Confirm startup writes the configured token file and still prints the token.
3. Start the client:

   ```bash
   cargo run --bin data-store -- --config config.toml
   ```

4. Run:

   ```text
   help
   health
   limits
   ingest The_Elements_of_Style.pdf
   search "clear writing style rules" 3
   versions
   shutdown
   ```

5. Confirm long operations stream real status/progress events.
6. Confirm counted progress overwrites the current line.
7. Confirm non-counted status lines end with newlines.
8. Confirm operation errors show status, kind, message, operation, and stage.
9. Confirm transport failures show method, URL, and cause chain.
10. Confirm protected operations read the current token file and do not print
    the token.

## Implementation Order

Historical implementation order:

1. Config and example config.
2. Token-file lifecycle.
3. Dependency and binary setup.
4. Client config loading.
5. REPL and parser.
6. HTTP helpers.
7. Commands and renderers.
8. Docs.
9. Verification.

This order brings up the service credential handoff before the client depends on
it, then builds the client from startup through rendering.

Next implementation order:

1. Protocol DTOs and structured errors.
2. Operation dispatcher.
3. NDJSON stream transport.
4. Ingest progress instrumentation.
5. Search progress instrumentation.
6. Protected operation dispatch.
7. CLI operation transport.
8. CLI event rendering.
9. CLI error reporting.
10. Control endpoint placeholder.
11. Docs and verification.
