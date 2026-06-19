# Agent Rules

## Rule Precedence

When instructions conflict, follow this order:

1. Explicit user instruction in the current conversation.
2. Agent conduct rules in this file, including safety, scope, approval,
   filesystem, command, and patch process rules.
3. Repository technical policy in `PRINCIPLES.md` and diagnostics policy in
   `DIAGNOSTICS-ONBOARDING.md`.
4. Existing local code patterns, only when they do not conflict with the above.

## Scope And Design Approval

Exact scope wins by default. Do only what the user explicitly requested or
approved.

Implementation details inside the approved design may be handled independently.
Design and architecture decisions must be proposed first and approved by the user
before implementation.

This user has extensive technology and software development experience, strong
opinions about software design and architecture, and high standards. Do not make
design choices on the user's behalf. Do not treat common framework patterns,
"best practices", or model preference as approval to override the user's design
judgment.

Ask before any file write except the verification writes explicitly allowed by
`Verification Behavior` after approved Rust source edits. This includes
creating, modifying, deleting, moving, or generating code, config,
documentation, plans, notes, scratch files, or artifacts. Before writing, state
the target path and intended change, then wait for explicit approval.

Any change to any configuration file requires explicit approval with no
exceptions. Do not treat approval for related code, schemas, prompts, docs,
tests, type fixes, mechanical consistency, or implementation follow-through as
approval to edit config. State the exact config path and intended config change,
then wait for explicit approval before modifying it.

Also ask before:

- Changing architecture, ownership boundaries, data flow, persistence, or API
  contracts.
- Introducing a new abstraction, dependency, framework pattern, fallback path, or
  state-management mechanism.
- Refactoring beyond the approved change.
- Adding behavior, UI, logging, tests, or documentation not explicitly requested.
- Choosing between viable design alternatives where tradeoffs exist.

Allowed without separate approval:

- Low-level implementation details required by an approved design.
- Rust compiler fixes caused by the approved change, if they do not change
  behavior beyond the approved intent.
- Local mechanical edits needed to keep the approved change coherent.

Approval to implement a specific plan includes the file writes needed for that
approved scope, except configuration changes and any newly discovered design,
scope, behavior, or side-effect decisions. Those still require explicit
approval.

Approval is not permission to guess. If scope, behavior, side effects, or user
intent is ambiguous, stop and ask before acting. Approval covers only explicitly
described behavior, not unstated assumptions or newly discovered implications.

When in doubt, stop and ask one focused question.

## Collaboration With This User

The user is new to Rust. When a detail involves Rust-specific issues, explain it
so the user can be informed and assist.

Questions are requests for answers, never requests for action.

Questions containing action language such as "can we", "should we", or "what if
we" are not implicit approval. Respond with a proposal and ask before making
changes.

When proposing changes, explain:

- What the change does.
- How it works within the codebase.
- Why you chose this approach.

Effort estimates and confidence percentages are planning-session review
metadata. Show them to the user before approval when required, but do not write
them into repository plan files, specs, documentation, or implementation
artifacts unless the user explicitly asks.

After 2-3 failed attempts, stop and discuss rather than continuing to iterate.

When genuinely uncertain, involve the user rather than guessing.

When you discover something that changes your understanding of what needs to be
done, pause and share that discovery before acting on it.

If the user is right, say so. If they are wrong, say so.

Do not ask whether the user restarted the server when they report that changes
did not take effect. Assume the user already knows when restart is needed and
investigate the root cause.

## Asking Questions

Ask exactly one focused question at a time.

Do not ask multiple questions in one turn. Do not ask a list of questions and
then ask which to tackle first. Do not bundle unrelated decisions together. The
user should always know exactly what answer is needed next.

Every question must include enough context for a high-quality answer:

- Why the question matters.
- What code, behavior, or design decision it affects.
- The viable options.
- The pros and cons of each option.
- Your recommendation and why you recommend it.

For questions about variables, functions, types, or calls, explain what they do
and where they are used.

Ask questions in the logical order needed to move the work forward. If one answer
could change the next question, ask only the first question and wait.

## Verification Behavior

Follow `PRINCIPLES.md` for repository verification policy. This section defines
which verification steps agents run by default and which require explicit user
approval. For agent work, do not create or run automated tests unless the user
explicitly asks to re-enable testing:

- No Rust test modules, test functions, test fixture files, or integration tests.
- No `cargo test` unless explicitly requested.

Cargo checks are mandatory verification, not automated tests. After Rust
changes, run all appropriate Cargo checks:

- `cargo fmt`
- `cargo check`
- `cargo clippy`

Approval for Rust source edits includes permission to run these mandatory Cargo
verification commands and their normal formatting/build-artifact writes. This
does not authorize unrelated file writes or configuration changes.

Cargo checks remain mandatory after Rust changes and do not require separate
approval once the Rust change itself is approved.

`PRINCIPLES.md` describes the full verification policy. In agent work, Cargo
checks are the default required verification after Rust changes. Additional
verification from that policy that starts the server, depends on network access,
requires live external services, or otherwise changes runtime state requires
explicit user approval. When those checks are relevant but not approved, explain
the remaining risk instead.
If broader verification from `PRINCIPLES.md` is relevant but not approved or not
possible, report it as residual risk rather than treating Cargo checks as full
verification.

Do not start, stop, or restart the server unless the user explicitly asks.

Do not run network-dependent verification unless the user explicitly approves it.

If compile, format, or lint checks fail because of the approved change, fix those
errors without asking again unless the fix changes behavior beyond the approved
intent, expands scope, or requires a design decision.

If verification cannot be run, explain exactly why and what risk remains.

## Code Rules

Rust code must use the project's Cargo edition and be formatted with
`cargo fmt`.

Every function must have a useful comment immediately before it explaining its
purpose or key invariant. Public Rust items should use doc comments. Comments
are not limited to functions; include them anywhere the code is not completely
intuitive or clear to someone unfamiliar with the codebase. Follow
`PRINCIPLES.md` `Code Comments`; do not write comments that merely restate
names, types, parameters, or obvious syntax.

Comment requirements are active review criteria, not passive style guidance.
Before final verification, inspect every edited region and decide whether the
code is locally understandable to a future maintainer who did not participate in
the current conversation. If not, add a concise comment explaining the intent,
invariant, ownership rule, lifecycle, or failure mode. Use Rust doc comments for
public items and ordinary comments for internal implementation notes. If non-function code
is obvious from names and structure, do not add a comment. Functions are the
deliberate exception: every function always carries a comment describing its
utility, even one that is currently obvious, because functions grow and a
once-obvious function can become non-obvious over time. The rule against
comments that merely restate names, types, or syntax still governs the content
of that function comment — it must describe purpose or invariant, not echo the
signature.

For every behavioral Rust edit, agents must decide before patching whether the
changed code introduces or relies on a non-obvious intent, invariant,
ownership/lifetime constraint, borrowing strategy, async/blocking boundary,
cancellation or shutdown rule, UI layout invariant, cache invalidation rule,
persistence/refresh coupling, ordering rule, error propagation policy, or
cross-module contract. If yes, include the explanatory comment in the same patch
as the code. Do not wait for the user to request comments, and do not leave the
explanation only in chat. Use Rust doc comments for public items and ordinary
comments for internal implementation notes. Comments should state why the code
exists or what must remain true, not restate the code mechanics.

Follow `PRINCIPLES.md` `External-Language Artifacts`: do not embed large SQL,
prompts, templates, scripts, HTML, JavaScript, CSS, or similar external-language
artifacts inside function bodies. Use dedicated files, named constants, or query
files as appropriate.

Follow `PRINCIPLES.md` for explicit error-handling policy. Do not introduce
`unwrap` or `expect` unless the use fits the allowed exceptions there and the
reason is clear at the call site.

Do not use compiler-silencing patterns to avoid ownership clarity. Clones,
`Arc`, `Mutex`, `RwLock`, boxed dynamic errors, and `allow` attributes must each
have a concrete reason.

Follow `PRINCIPLES.md` for async and blocking-work policy. Before introducing
or expanding async behavior, stop, explain why the change is needed under that
policy, and get explicit approval before implementing it.

SQLite access through `rusqlite` is synchronous. Keep database work synchronous;
when async runtime code reaches SQLite, follow `PRINCIPLES.md` for the required
blocking boundary and do not convert the database work itself into async code.

Before applying a mechanical pattern, ask:

- Is this the right fix, or just a fix?
- Does the error reveal a deeper issue?
- Would a different approach be better?

If copying a code block 3+ times, extract to a loop or function.

Shared types/constants must be defined once and imported everywhere. If adding a
new case requires edits in multiple files, look for a single-source-of-truth
refactor.

## Patch Hygiene

When editing files:

- Before every edit, read the exact current file region you intend to edit with
  line numbers.
- When editing a named section, first locate the heading with
  `rg -n '^#+ .*Section Name' <file>` or an equivalent exact heading search,
  then read the contiguous region around that located line. Do not guess line
  ranges from memory or nearby context.
- When reading multiple non-contiguous regions, prefer separate focused reads
  over compound `sed` ranges. If output skips line numbers or appears truncated,
  re-read each target region as one contiguous range before editing.
- If a previous edit in this turn touched the same file, re-read the target
  region before the next edit.
- After an interrupted or aborted turn, inspect the current edited regions or
  relevant diff before continuing. Treat partially applied edits as untrusted
  until re-read, and remove or revise any stale partial design before adding new
  changes.
- Prefer one conceptual edit per patch. Do not combine distant or unrelated
  edits unless each one is trivial and independently anchored.
- Prefer small, targeted edits over large verbatim block replacements.
- Anchor each edit on the smallest stable context that uniquely identifies it.
- Do not reconstruct long existing code blocks from memory.
- For large functions, edit imports, helper additions, and small internal edits
  separately.
- After a successful edit, re-read the changed region before making another
  edit in the same file.
- After re-reading each edited region, perform a comment sufficiency check before
  moving on. If the edited code contains a non-obvious invariant, ownership
  boundary, lifecycle rule, accounting rule, ordering rule, error-handling
  policy, external-system contract, or intentionally preserved edge case, add or
  update a nearby comment in the same edit sequence. Do not wait for the user to
  ask for comments.
- If an edit fails to match, stop and re-read the relevant file region before
  retrying. Do not retry from memory.
- Do not combine unrelated files in one edit unless the changes are trivial.
- When running shell searches that include Markdown backticks, `$`, `*`,
  brackets, parentheses, or other shell-active characters, wrap the search
  pattern in single quotes.

## Implementation Rules

- **Function signature changes**: When changing parameters, use
  `rg -n "function_name\\(" src static` or an equivalent search and update every
  caller.
- **Handler and event changes**: When changing `/api/chat`, streamed NDJSON
  event fields, or event ordering, check both the Rust emitter and
  `static/index.html` consumer.
- **Tool-call changes**: When changing tool definitions, tool result shape, SQL
  execution, or model-visible content, check the full path from provider response
  to tool execution to model-fed tool message to browser event.
- **Config changes**: Any config shape change requires explicit approval,
  updates to `config.example.toml`, and corresponding config parsing behavior.
- **Prompt changes**: Treat system prompt changes as behavior changes. Check
  schema generation, curated domain notes, and the model/tool assumptions
  affected by the prompt.
- **Async paths**: Follow `PRINCIPLES.md` for async and blocking-work policy.
  Before introducing or expanding async behavior, stop, explain why the change is
  needed under that policy, and get explicit approval before implementing it.
- **SQLite work**: SQLite access through `rusqlite` is synchronous. Keep
  database work synchronous; when async runtime code reaches SQLite, follow
  `PRINCIPLES.md` for the required blocking boundary and do not convert the
  database work itself into async code.
- **Resource cleanup**: Any long-lived operation, connection, stream, timeout, or
  closeable resource must have an explicit lifecycle and observable
  failure/completion behavior.

## Migration Rule (Hard, No Exceptions)

Never implement DB migrations in live runtime paths. This includes startup,
config loading, server setup, request handlers, tool execution, schema prompt
generation, and any code that runs as part of normal app execution.

All schema and data migrations must be explicit one-time scripts run deliberately
by the user/developer.

Do not add "safe", "idempotent", "temporary", or "compatibility" migrations to
runtime code. If existing runtime code appears to need a migration, stop and
propose a script instead.

## High-Risk Areas

These areas are high-risk. Before changing them, read the relevant code and
explain the intended change before editing.

- **Observability**: Raw request/response payloads are authoritative audit
  material. Never filter, narrow, reconstruct, rename, or omit observability
  fields except for explicit secret redaction. Derived views are additions, not
  replacements.
- **Protocol round-trip**: OpenAI-compatible messages, assistant tool calls, tool
  call IDs, and tool result messages must preserve round-trip correctness. Do not
  narrow or rebuild protocol payloads in a way that loses unknown fields or
  breaks provider compatibility.
- **Tool execution**: `query_database` is the only model-facing data tool.
  Changes to tool definitions, tool arguments, SQL execution, result caps,
  truncation, or error shape affect correctness and auditability.
- **SQLite access**: Database access must remain read-only, synchronous, bounded,
  and explicit. Do not add write-capable connections, hidden fallback data
  sources, or async wrappers.
- **Configuration**: Config is strict and operationally significant. Missing
  files, missing keys, unknown keys, and missing required secrets are fatal
  errors.
- **System prompt**: The generated schema summary and curated domain instructions
  affect model correctness. Treat prompt changes as behavior changes.
- **NDJSON event contract**: Browser-visible events must have explicit types and
  terminal state. Changes require checking both Rust emission and browser
  consumption.

## Debugging And User Feedback

The user and operator must be able to tell what happened. Do not leave
user-triggered work in an apparently idle or ambiguous state.

Use targeted operator logs as the primary debugging tool. Add logs when existing
logs do not explain operation start, boundary transitions, errors, completions,
or elapsed time. Do not add noisy logging.

User-facing feedback must appear inline in the browser UI or streamed chat
events. Do not add modals, toasts, alerts, or tooltips unless the user
specifically asks for them.

Errors must preserve source context. Do not replace a specific provider, SQL,
config, or protocol error with a generic message.

## Diagnostic Hygiene

- Follow `DIAGNOSTICS.md` for diagnostics policy and implementation standards.
- Before relying on diagnostics for a feature, identify the authoritative log or
  audit path from the applicable configuration, documentation, or
  implementation. If no authoritative path is documented or discoverable, treat
  the diagnostic record as unresolved and ask before proceeding.
- Before changing diagnostic behavior, identify the affected lifecycle or
  boundary logs and explain the intended change.
- When diagnostics policy requires adding or changing logs, include those log
  changes in the proposed plan and get approval before editing.
- Do not omit, narrow, reconstruct, or replace authoritative diagnostic records
  except for explicit secret redaction.
- If a likely failure cannot be diagnosed from durable logs after a change,
  treat the change as incomplete.

## Filesystem Boundary (Hard, No Exceptions)

This is a shared machine. The agent must not read from or write to anything
outside this repository.

All work must stay inside the project root. Do not inspect parent directories,
home directories, system directories, `/tmp`, or unrelated projects. Do not use
external paths for scratch files, temporary files, backups, exports, diagnostics,
or any other purpose.

The `specs/` directory is also off-limits unless the user explicitly asks for it.
Do not read from or write to `specs/`; it contains archived material that must
not be treated as current context.

Always run commands from the project root. If a command must target a
subdirectory, use an explicit working directory or `cd dir && command` for that
command only. Do not leave the repo root as the operating context.

## Commands And Git

Do not perform these actions unless the user explicitly instructs you:

- Kill processes.
- Start, stop, or restart servers.
- Run package/dependency management commands such as `cargo add`,
  `cargo update`, or dependency installation.
- Delete files.

Do not use git unless the user explicitly requests a git operation. If git is
explicitly requested, run it from the project root only and never commit secrets.
