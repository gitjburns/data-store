# Using the interactive Data Store client (REPL)

The `data-store` binary runs an interactive read-eval-print loop (REPL) when
invoked with **no operation flag**. It is the CLI client binary, separate from
the `data-store-service` binary that runs the service; running it without a
one-shot command (such as `--health` or `--query`) starts the REPL instead.

This guide covers the REPL. For install and first-run setup, see `INSTALL.md`.
For the full request/response shapes and rendering detail see `SPEC-CLIENT.md`,
and for the HTTP wire contract see `PROTOCOL.md`.

## Launching

```sh
data-store                            # uses ./config.toml
data-store --config /path/to/config.toml
```

The REPL shares the same `config.toml` as the service. From it, the client
resolves:

- the service base URL from `[server].bind_address` (a bind-all address is
  dialed as loopback),
- the admin token file from `[admin].token_file_path`, and
- the request/poll timeout from `[client].operation_timeout_seconds`, which
  bounds each individual HTTP request (including each poll); the poll loop
  itself has no total-time cap.

On launch it prints the connected base URL and a hint to type `help`. Line
editing and history are provided by rustyline; command history persists to
`.data-store.history` (resolved against the config file's directory) across
sessions.

Type `exit` (or `quit`), or send EOF (Ctrl-D), to leave. Ctrl-C interrupts the
current line and prints a reminder to use `exit`.

## Command set

Commands and their usage strings (from the client's command table):

| Command | Usage | Notes |
| --- | --- | --- |
| `health` | `health` | Service health and per-component readiness. |
| `query` | `query <requestJson>` | Search; request body is raw JSON. |
| `ingest` | `ingest <sourceSystem> <nativeUri>` | Admin; async operation. |
| `reparse` | `reparse <sourceId> <sourceSystem> <nativeUri>` | Admin; async operation. |
| `activate` | `activate <sourceId> <parseId>` | Admin; async operation. |
| `accept` | `accept <parseId>` | Admin; async operation. |
| `discard` | `discard <parseId>` | Admin; async operation. |
| `snapshot` | `snapshot [requestJson]` | Admin; async operation. Optional JSON request body. |
| `restore` | `restore <sourceId> <parseId>` | Admin; async operation. |
| `shutdown` | `shutdown` | Admin; control action (not polled). |
| `held-parses` (alias `held`) | `held-parses` | List held parses awaiting disposition. |
| `operation` | `operation <operationId>` | Read one operation record (single snapshot). |
| `unit` | `unit <unitId>` | Read a unit. |
| `relationships` | `relationships <unitId> [direction] [relationshipType]` | Optional filters. |
| `source` | `source <sourceId>` | Read a source. |
| `sync-status` (alias `sync`) | `sync-status` | Acquisition/sync status. |
| `help` | `help` | List commands. |
| `exit` (alias `quit`) | `exit` | Leave the REPL. |

Arguments are split with minimal shell-like quoting: double quotes group
tokens; inside quotes a backslash escapes only `"` and `\` (any other escaped
character keeps its backslash); outside quotes a backslash is literal; an
unterminated quote is an error. There is no shell expansion of any kind. JSON
arguments (`query`, and the optional `snapshot` body) are parsed and validated
before any request is sent.

## Polling model

There is **no streaming**. Mutating admin commands (`ingest`, `reparse`,
`activate`, `accept`, `discard`, `snapshot`, `restore`) resolve to an
asynchronous operation: the client receives
an operation id (HTTP 202) and then **polls `GET /operations/{operationId}` on a
fixed one-second interval** until the operation reaches a terminal state
(`succeeded` or `failed`), rendering the terminal record. Each poll — like every
protected request — reads the admin token from the token file **fresh, per
request**; the token is never cached. `[client].operation_timeout_seconds`
bounds each individual poll request only; the loop itself runs until a
terminal status with no total-time cap.

`operation <operationId>` performs a single snapshot read of an operation record
without polling. `shutdown` is the one control action that returns 202 with no
body and is never polled.

### Operation succeeded is not the parse outcome

For a parse-producing operation (`source_ingest`, `parser_execution`,
`parse_activation`), an operation reaching `succeeded` means the pipeline
**lifecycle** completed — it does **not** confirm a favorable domain outcome. The
domain verdict (a recorded parse failure, or a held disposition awaiting your
action) lives in the parse run row, not in the operation status. When the client
renders such a `succeeded` operation, it prints a note directing you to the
domain verdict: run `held-parses` for a held result, or inspect the parse run
itself.

## Reference

- `SPEC-CLIENT.md` — full command surface, request/response shapes, and
  rendering detail.
- `PROTOCOL.md` — the HTTP wire contract.
