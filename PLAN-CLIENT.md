# Data Store CLI Client Implementation Plan

## Goal

Implement the `data-store` interactive CLI client described in
`SPEC-CLIENT.md`, plus the service-side token-file support required for the
client to authenticate admin API calls without manual token entry.

The work must preserve the service as the single source of truth. The client
only reads config, reads the runtime token file for admin commands, sends HTTP
requests, and renders responses for humans.

## Status

Implementation complete.

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

## Scope

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
- Frontend or Node backend integration.

## Phase 1: Config And Local Files

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

## Phase 2: Service Token-File Lifecycle

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

## Phase 3: Dependencies And Binary Target

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

## Phase 4: Client Config Loading

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

## Phase 5: REPL And Parser

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

## Phase 6: HTTP Client Layer

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

## Phase 7: Commands And Rendering

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

## Phase 8: Documentation

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

## Phase 9: Verification

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

## Implementation Order

Recommended order:

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
