# AGENTS.md

This file provides guidance to AI agents working with code in this repository.
The shorter `CLAUDE.md` at the repo root is a symlink to this file — both names
get the same source of truth.

**CRITICAL: File Size Limit** - AGENTS.md must not exceed 40,000 characters.
When editing this file, check the character count (`wc -c AGENTS.md`). If over
the limit:

- **NEVER remove guidelines** - only condense verbose explanations
- **Condense code examples first** - examples are illustrative, rules are not
- **Remove redundancy** - if a guideline duplicates another, keep one reference
- **Shorten explanations** - preserve the rule, reduce the elaboration

## Documentation

**Before doing any work**, read these documents:

1. **[SPEC.md](SPEC.md)** — the north star. Describes what this library should
   be. All new features must be spec'ed here first. If your change contradicts
   the spec, either update the spec first (with user approval) or change your
   approach. Implementation is downstream from the spec.
2. **[docs/domain.md](docs/domain.md)** — naming conventions and domain
   terminology. All code must use the names defined here. If a name isn't in
   this doc, check existing code for precedent before inventing one.

**Read when relevant** to your task:

- [docs/cqrs.md](docs/cqrs.md) - Event sourcing patterns: the `EventSourced`
  trait, `Lifecycle` adapter, projections, services, schema registry.
- [docs/sqlx.md](docs/sqlx.md) - `SQLX_OFFLINE`, `query!` vs runtime queries,
  regenerating the `.sqlx/` cache, common pitfalls.
- [docs/ttdd.md](docs/ttdd.md) - Type-driven TDD workflow: scientific method
  applied to software, failing tests before implementation.

**Update at the end:**

- **README.md** — if project structure, features, commands, or architecture
  changed
- **`docs/`** — when research or trial-and-error reveals non-obvious patterns,
  pitfalls, or framework behavior, document it in the relevant `docs/` file (or
  create a new one) to prevent rediscovery. Prioritize documenting:
  framework-specific idioms, integration gotchas, and "X doesn't work because Y"
  findings.

## Ownership Principles

**CRITICAL**: Fix all problems immediately regardless of origin. Meet ALL
constraints (file size limits apply to entire file). No warnings/errors pass
through. Work until all tasks complete unless blocked needing user input.

**CRITICAL: If you know you caused a problem and know how to fix it, fix it
immediately.** Do not restate the request, ask for confirmation, or wait for
permission to do the obvious corrective work.

## Communication

- **Do not run commands to "show" output to the user.** The CLI truncates
  output. If you need the user to review something, explicitly ask them to look
  at it. Do not run `git diff` expecting the user to see output.

## Planning Hierarchy

The project uses a strict document hierarchy:

1. **SPEC.md** - Source of truth for system behavior. Features documented here
   before implementation.
2. **ROADMAP.md / GitHub Issues** - Downstream from spec. Describe problems, not
   solutions.
3. **Planning** - Downstream from issues. Implementation plans before coding.
4. **Tests** - Downstream from plan. Written before implementation (TDD).
5. **Implementation** - Makes the tests pass.

**Before implementing:** Ensure feature is in SPEC.md -> has GitHub issue ->
plan the implementation.

### Goal-Oriented Planning

Organize around the **goal**, not implementation streams. Start from the desired
end state and work backwards. Implementation details (crate, branch) are
downstream from the goal.

### Epic Decomposition for Parallel Execution

Decompose epics to maximize independent parallel execution:

1. **Identify coupling boundaries.** Work touching disjoint code areas can
   proceed in parallel.
2. **Sequence only where necessary.** A branch depends on another only when it
   needs types/traits/behavior introduced by that branch.
3. **Every branch must be independently valid.** Each PR must pass CI and make
   sense on its own -- never leave a broken intermediate state.
4. **Defer integration.** Push integration PRs to the end, stacked on both
   parallel branches.
5. **Shared dependencies go first.** Extract shared types/traits/schemas into a
   base PR to unblock all downstream branches.
6. **Conflict-prone work goes last.** Schedule after parallel branches merge.
7. **Converge to a single terminal node.** One final PR depends on all parallel
   streams for integration.

### Managing Epics in the Roadmap

An epic is a roadmap subsection grouping related issues toward a single goal.

- **Lead with motivation**: One or two sentences explaining why this work
  matters and what the end state looks like.
- **Show the dependency structure**: Use a Mermaid diagram (GitHub renders them
  natively) to make the execution order and parallelism obvious at a glance.
- **Reference issues, not solutions**: Each item links to a GitHub issue. The
  issue describes the desired outcome; the PR (added later) describes the
  solution.
- **Mark progress inline**: `[x]` with PR link as branches merge. When all items
  complete, move the section to "Completed."

## Plan & Review

### Task management

- Keep a granular task list for the current request and update it as work
  progresses.
- Clear completed tasks from the active list so the remaining work is always
  obvious.

### While implementing

- **CRITICAL: All new or modified logic MUST have corresponding test coverage.**
  Do not move on from a piece of code until tests are written. This is
  non-negotiable.

### Before deciding on an implementation

Before making changes, deeply understand the task: read the relevant repo docs,
read the relevant source code, form an initial approach, criticize it, and
improve it until the plan is coherent with the repo architecture. Keep the
resulting diff as small and reviewable as possible.

### Handling questions and approach changes

Answer the question first. Don't silently change approach - ask confirmation. If
new approach fails, state what went wrong and ask before reverting. Explicit
confirmation required before changing direction.

When the user has already clearly told you what to do, start doing it
immediately. Do not paraphrase the request back as a confirmation step.

### When issues are pointed out

When the user points out an issue, bug, or problem - fix it immediately. Do not
ask "Want me to fix this?" or "Should I address this?". The user never sends
messages just for the sake of it; when they point out issues, they expect action
(usually a fix, sometimes reproducing, opening a GitHub issue, etc. based on
context).

**CRITICAL: Re-evaluate all work when a pattern is identified.** When the user
points out a mistake, immediately: (1) fix it, (2) re-evaluate ALL session work
for similar issues, (3) proactively fix all instances without being asked.

### When user action is required

**CRITICAL**: The user is not reading every word of your output - they are
monitoring your actions. When you need the user to do something (run a command,
check output, provide input), you must ensure they see the request:

- If you are **blocked** and cannot proceed without user action, STOP after
  stating what you need. Do not continue working on other tasks.
- If you are **not blocked**, you can continue working, but when you're ready to
  stop, clearly state what you need from the user at the end of your response.

The user checks your output when they see you've stopped. If you give them a
command mid-response and keep working, they will miss it.

Significant architectural decisions are a special case. When existing docs do
not already answer an architectural choice and the decision is important enough
to record, write an ADR under `adrs/$INDEX-$PROPOSAL_NAME.md`, give the user a
brief summary, and stop for review before continuing with that direction. Once
the ADR is approved, treat it as the standing decision and do not ask the same
question again.

### Before handing over

After implementation is complete and verification passes (tests, lints, fmt),
perform a self-review:

1. **Review the diff** - examine all changes and ask: can I justify each chunk?
2. **Revert unjustified changes** - if you can't articulate why a change is
   necessary, revert it
3. **Check for scope creep** - did you change things unrelated to the task?

**Justified changes:** explicitly requested by user, required to make the
requested change work, fixes a bug/warning encountered during the task, improves
readability of code being modified, enforces stricter domain boundaries.

**Unjustified changes:** renaming unrelated things, reformatting outside the
change area, LLM-initiated "while I'm here" improvements, changing terminology
without request, adding comments to unchanged code.

This step exists because LLMs are not naturally aware of diff size while
generating, but can effectively review diffs after the fact. When context is
ambiguous (after compaction), if you cannot point to an explicit user request in
visible conversation, treat the change as unjustified and revert it.

## Project Overview

`event-sorcery` is an event-sourcing library on top of `cqrs-es`. The Cargo
workspace has two crates and no application binaries:

- **`crates/sqlite-es`** — SQLite implementation of `cqrs-es`'s event/view
  repository traits. Standalone; usable wherever a `cqrs-es` backend is needed.
- **`crates/event-sorcery`** — higher-level ergonomics on top of `sqlite-es`:
  the `EventSourced` trait, `Lifecycle` adapter, typed `Store`, projections,
  schema registry, reactor.

The `migrations/` directory at the workspace root holds the canonical SQLite
schema (events + snapshots tables) that `sqlite_es::testing::create_test_pool`
applies to in-memory test databases. Consumers run the same migration in their
application database.

## Key Development Commands

### Building & Running

**CRITICAL: NEVER use `cargo build` for verification.** It's slower than
`cargo check` and less useful than `cargo nextest run` or `cargo clippy`. Use:

- `cargo check` for fast compilation verification
- `cargo nextest run` for verification with test coverage
- `cargo clippy` for verification with linting

Only use `cargo build` when you actually need the build artifacts (e.g., final
verification before a release).

### Testing

- `cargo nextest run --workspace` - Run all tests in both crates
- `cargo nextest run --lib` - Run library tests only
- `cargo nextest run -p event-sorcery` - Run event-sorcery crate tests
- `cargo nextest run -p sqlite-es` - Run sqlite-es crate tests
- `cargo nextest run <test_name>` - Run specific test

### Database Management

This library doesn't run a long-lived database itself; tests use in-memory
SQLite pools via `sqlite_es::testing::create_test_pool()`. The single workspace
migration (`migrations/20251016210348_init.sql`) is the canonical
event/snapshots schema; consumers apply it (and any of their own view
migrations) in their app.

- `sqlx migrate add <migration_name>` - Create a new migration file
- **CRITICAL: NEVER manually create migration files.** Always use
  `sqlx migrate add <migration_name>` to ensure proper timestamping and
  sequencing.
- Migrations to the `events` table after the initial one are forbidden — the
  schema is part of the library's public contract.

### Dependency Management

**CRITICAL: NEVER manually edit `Cargo.toml` to add dependencies.** Always use
`cargo add <crate_name>` to add dependencies. This ensures proper version
resolution and feature selection.

- `cargo add <crate_name>` - Add a dependency to the current crate
- `cargo add <crate_name> --dev` - Add a dev-dependency
- `cargo add <crate_name> --build` - Add a build-dependency
- `cargo add <crate_name> -F <feature>` - Add a dependency with specific
  features

Workspace deps live in `[workspace.dependencies]`; per-crate `Cargo.toml` files
use `<dep>.workspace = true` (with optional per-crate features added on top).
`Cargo.lock` is committed.

### Development Tools

- `cargo clippy --workspace --all-targets --all-features` - Run Clippy for
  linting
- `cargo fmt` - Format code

### Nix Development Environment

- `nix develop` - Enter development shell. Provides the rust toolchain,
  `sqlx-cli`, `cargo-expand`, `cargo-nextest`, and the pre-commit hooks listed
  in `.pre-commit-config.yaml`. Built on top of `rainix` for shared Rust
  tooling.

### Generated paths

Anything generated by tooling goes under `.tmp/` (gitignored). Do **not** add
ad-hoc top-level entries like `.foo-bar.json` to `.gitignore`. If a tool needs a
stable on-disk path, point it at `.tmp/<name>` and let the existing `.tmp/`
ignore rule cover it.

### Configuration Files

| File                  | Purpose                                                                    |
| --------------------- | -------------------------------------------------------------------------- |
| `Cargo.toml`          | Workspace definition, `[workspace.lints]` lint config, shared dependencies |
| `clippy.toml`         | Clippy behavior settings (thresholds, test permissions)                    |
| `flake.nix`           | Nix flake: dev shell                                                       |
| `crates/*/Cargo.toml` | Per-crate dependencies and `[lints] workspace = true`                      |
| `migrations/`         | Canonical SQLite event/snapshots schema applied by `create_test_pool`      |

## Development Workflow Notes

- When running `git diff`, make sure to add `--no-pager` to avoid opening it in
  the interactive view, e.g. `git --no-pager diff`
- **CRITICAL: NEVER run a binary speculatively.** If you want to understand what
  code does, read it. If you want to test functionality, write proper tests.
- When handling clippy errors about function lengths or cognitive complexity,
  don't split up the functions more than necessary to get below the limit.
  Instead ask the user if we can add a clippy allow for that error.

## Architecture Overview

### Library Layering

`event-sorcery` sits on top of `sqlite-es`, which sits on top of `cqrs-es`. Each
layer narrows the API the layer above sees. The `Lifecycle` enum is `pub(crate)`
deliberately — it's an implementation detail of the `EventSourced` ↔ cqrs-es
bridge and must not leak through any public bound. See the `ViewBackend` GAT in
`view_backend.rs` for how to express
`(Lifecycle<Entity>, Lifecycle<Entity>)`-shaped repository requirements without
naming the private type in any public bound.

### Naming Conventions

Code names must be consistent with **[docs/domain.md](docs/domain.md)**, which
is the source of truth for terminology and naming conventions. cqrs-es names
(`Aggregate`, `Query`, `View`, `DomainEvent`) are deliberately avoided in our
public API — consumers interact through `EventSourced`, `Store`, `Projection`,
etc., so it's immediately obvious whether code belongs to this crate or to
cqrs-es.

### Code Quality & Best Practices

- **CRITICAL: Package by Feature, Not by Layer**: Organize by business domain,
  not technical layers. **FORBIDDEN** catch-all modules: `types.rs`, `error.rs`,
  `models.rs`, `utils.rs`, `helpers.rs`, `http.rs`, `dto.rs`, `entities.rs`,
  `services.rs`, `domain.rs`. **CORRECT**: `lifecycle.rs`, `projection.rs`,
  `reactor.rs`, `view_backend.rs`. Each feature module contains ALL related
  code. Shared types import from the owning feature. **Flat by default**: single
  file per feature, split into directory only when business logic boundaries
  emerge
- **CRITICAL: CQRS/Event Sourcing Architecture**: **NEVER write directly to the
  `events` table** — no direct INSERTs, no manual sequence numbers, no bypassing
  `CqrsFramework`. Always use `CqrsFramework::execute()` or
  `execute_with_metadata()` to emit events through aggregate commands. The
  framework handles persistence, sequence numbers, and consistency
- **CRITICAL: Single CQRS Framework Instance Per Aggregate**: In any consuming
  application, each aggregate must have exactly ONE `SqliteCqrs<A>`, constructed
  once at startup. Never call `sqlite_cqrs()` or `CqrsFramework::new()` per
  request. Direct construction is fine in test/CLI/migration code
- **CQRS Aggregate Services Pattern**: Use cqrs-es Services for side-effects in
  `handle()` to ensure atomicity with events. **Naming:** `{Action}er` trait ->
  `{Domain}Service` implements -> `{Domain}Manager` orchestrates
- **Log in command handlers, not callers**: All logging for command execution
  belongs in the aggregate's `handle()` method, not at the call site. The
  handler has full aggregate state making log messages rich without the caller
  needing to load or pass extra context. This keeps logging consistent and
  centralized — one place per command, not scattered across every caller
- **Type Modeling**: Make invalid states unrepresentable through the type
  system. Use ADTs and enums to encode business rules and state transitions
  directly in types rather than runtime validation. See "Type modeling" in Code
  Style for details
- **SDK Boundary Conversion**: Accept domain newtypes and convert to SDK
  primitives inside the callee. Exception: cross-crate boundaries where the
  callee can't depend on caller's domain types -- destructure at the call site
- **Schema Design**: No contradictory columns. Use constraints and
  normalization. Align schemas with type modeling principles
- **No Denormalized Columns**: Never store values computable from other columns.
  Compute on-demand; if caching needed, use views or generated columns
- **Functional Programming**: Favor FP/ADT patterns over OOP. Use pattern
  matching, combinators, type-driven design. Prefer iterators over imperative
  loops unless it increases complexity. Use itertools for richer iterator chains
- **Comments**: Follow comprehensive commenting guidelines (see detailed section
  below)
- **Spacing**: Leave an empty line in between code blocks to allow vim curly
  braces jumping between blocks and for easier reading
- **CRITICAL: Import Organization**: Follow a consistent three-group import
  pattern throughout the codebase:
  - **Group 1 - External imports**: All imports from external crates including
    `std`, `cqrs_es`, `serde`, `tokio`, `sqlx`, `tracing`, etc. No empty lines
    within.
  - **Empty line**
  - **Group 2 - Workspace imports**: Imports from other workspace crates
    (`sqlite_es`). No empty lines within.
  - **Empty line**
  - **Group 3 - Crate-internal imports**: Imports using `crate::` and `super::`.
    No empty lines within.
  - Groups 2 or 3 may be absent if unused; never add an empty group
  - **FORBIDDEN**: Empty lines within a group, imports out of group order
  - **FORBIDDEN**: Function-level imports. Always use top-of-module imports.
    **Sole exception**: enum variant imports (`use MyEnum::*` or
    `use MyEnum::{A, B, C}`) inside function bodies to avoid repetitive
    qualification. Enum variant imports are never allowed at module level.
  - Module declarations (`mod foo;`) can appear between imports if needed
  - This pattern applies to ALL modules including test modules
    (`#[cfg(test)] mod tests`)
- **Import Conventions**: Qualify imports only to prevent ambiguity (e.g.
  `cqrs_es::Aggregate`), not when the module is clear (e.g. `info!` not
  `tracing::info!`). Top-of-module imports only (not top-of-file -- test module
  imports go in the test module, not behind `#[cfg(test)]` at file top)
- **Error Handling**: No `unwrap()`/`.expect()` in production code (validation
  logic may change, leaving panics). **Exception**: fine in test code
  (`#[cfg(test)]` modules); enabled via `clippy.toml` (`allow-unwrap-in-tests`,
  `allow-expect-in-tests`)
- **CRITICAL: Error Type Design**: **NEVER create error variants with opaque
  String values.** No `SomeError(String)`, no `.to_string()` or `format!()`
  conversions, no unpacking newtypes (store domain types not `String`). Prefer
  `#[from]` + `?` for error conversion; preserve error chains with `#[source]`;
  discover variants during implementation not preemptively. `.map_err` is
  permitted when adding call-site context or adapting a source error type that
  cannot implement `From`/`#[from]` - do not reach for it as the default when
  `#[from]` + `?` suffices. To log before converting:
  `.inspect_err(|error| error!(?error, "ctx"))` before `?`
- **Silent Early Returns**: Always log a warning/error before early returns in
  `let-else` or similar patterns. Silent failures hide bugs
- **No Duplicate Values in Debug Output**: Log actual runtime values, never
  hardcoded copies (they drift from the real implementation)
- **Visibility Levels**: Keep visibility as restrictive as possible (private >
  `pub(crate)` > `pub`) for better dead code detection and clearer scope
- **Type Aliases**: Only add when clippy complains about type complexity. If
  clippy doesn't flag it, the full type is clearer. Use newtypes (not aliases)
  to distinguish types with the same representation.

### CRITICAL: Numeric Integrity

**NEVER** silently mask failures on numeric values: no defensive capping,
fallback defaults, precision truncation, `unwrap_or()`, `unwrap_or_default()`.
ALL arithmetic in event handlers and projections must use explicit error
handling.

**Must fail fast**: numeric conversions (`try_into()`), precision loss, range
violations (error, not clamp), parse failures, arithmetic (checked), database
constraints. This library is consumed by financial systems where silent data
corruption leads to massive losses — the same standard applies here.

### CRITICAL: Security and Secrets Management

**NEVER read files containing secrets, credentials, or sensitive configuration
without explicit user permission.**

**Protected files** (require explicit permission): `.env*`, `credentials.json`,
`*.key`, `*.pem`, `*.p12`, `*.pfx`, database files with sensitive data. Ask
permission, explain why, wait for confirmation. Prefer `.env.example` or
reviewing code that uses configuration instead of reading secrets directly.

### Testing Strategy

- **Database Isolation**: In-memory SQLite databases for test isolation via
  `sqlite_es::testing::create_test_pool()`
- **Edge Case Coverage**: Comprehensive error scenario testing for event
  application and projection logic
- **Testing Principle**: Follow the testing pyramid — most coverage in unit
  tests, fewer integration tests, fewest e2e tests. Integration tests may cover
  failure scenarios when those failures can only be triggered by wiring multiple
  components together
- **CRITICAL: Tests must assert CORRECT behavior, never "document gaps"**: If
  code is broken, tests MUST assert correct behavior and FAIL until fixed. NEVER
  assert incorrect behavior with "will fix later" comments. A failing test is
  better than a passing test that asserts wrong behavior.
- **CRITICAL: NEVER delete, skip, or bypass existing tests to ease a refactor.**
  Either: (1) adapt tests to the new design preserving coverage, (2) find a
  design that keeps tests passing, or (3) stop and ask. Tests are correctness
  constraints -- if your change can't satisfy them, the change is wrong.
- **Debugging failing tests**: Add context to the assert! macro, not temporary
  println! statements
- **No ad-hoc debugging scripts**: Debug via test functions, not scripts or temp
  files
- **Test Quality**: Tests must verify business logic, not language features or
  struct field assignments
- **Property-Based Testing**: Use `proptest` for property-based tests whenever
  there are clear invariants to verify. Property tests are excellent for:
  - Parsing/serialization roundtrips
  - Boundary conditions (e.g., message length validation)
  - Invariants that should hold for all inputs (e.g., extracted data matches
    input regardless of surrounding bytes)
  - Numeric operations where edge cases are hard to enumerate manually

#### Writing Meaningful Tests

Tests must verify application logic, not language features. Testing struct field
assignments is useless; test actual behavior like
`projection.load(&id).await.unwrap()` returning expected values.

### Workflow Best Practices

Scope checks to the active package during development; run full workspace checks
before handover. Clippy is a polish step -- run it (and full-workspace nextest)
only after substantive edits are done. Skip the handover pass for doc-only
changes. CI blocks merging on any failure -- fix every warning, error, or
failure regardless of origin.

**During development:**

1. `cargo check -p <crate>`
2. `cargo nextest run -p <crate>`
3. `cargo clippy -p <crate>`

**Before handover:**

1. `cargo check --workspace`
2. `cargo nextest run --workspace --all-features`
3. `cargo clippy --workspace --all-targets --all-features`
4. `cargo fmt`
5. **Diff review** -- revert any chunks without clear justification (see "Before
   handing over" section)

#### CRITICAL: Quality Control Policy

**NEVER bypass, disable, or suppress ANY quality control mechanism without
explicit permission being granted.** This applies to ALL checks including but
not limited to:

- Clippy lints (`#[allow(clippy::*)]`)
- Compiler warnings (`#[allow(deprecated)]`, `#[allow(dead_code)]`, etc.)
- Deadnix, rustfmt, denofmt, yamlfmt, taplo, or any other linting/formatting
  tools
- Test assertions or validation logic
- Any other strictness or quality enforcement

Clippy lints often indicate poor design worth reconsidering. Upon a lint
violation:

1. **Re-evaluate the design** in the context of what was flagged. If the lint
   reveals a flaw in the broader design or architecture, fix that
2. **Refactor the code** to address the root cause of the lint violation
3. **Break down large functions** into smaller, more focused functions
4. **Improve code structure** to meet clippy's standards
5. **Use proper error handling** instead of suppressing warnings
6. If the violation is intentional and makes perfect sense in context, **stop
   and request explicit permission** from the user before suppressing

**FORBIDDEN: Obscure workarounds that silence the linter without fixing the
problem.** Either fix the underlying design issue or request permission to
suppress. No third option.

### Commenting Guidelines

Code should be self-documenting. Comments only when they add context that code
structure cannot express.

**DO comment**: complex business logic, algorithm rationale, external system
behavior, non-obvious constraints, test data context, workarounds.

**DON'T comment**: self-explanatory code, restating what code does, function
signature descriptions (use `///`), obvious test setup, section markers. **NEVER
reference task numbers or issue trackers** in comments.

Use `///` for public APIs. Keep comments focused on "why" not "what".

### Code style

#### ASCII in code, unicode in user-facing output

Use ASCII characters only in identifiers, comments, log messages, and config
keys. For arrows in comments, use `->` not `→`. Unicode breaks vim navigation
and grep workflows.

In user-facing string literals (CLI display, rendered text), prefer unicode
characters (`←`, `→`, `·`, `▲`, `▼`, etc.) for readability and polish.

#### No single-letter variables or arguments

Single-letter names are **FORBIDDEN** everywhere -- variables, arguments,
closure params, generic type params. Always use descriptive names. Exception:
short closures where the type is unambiguous (e.g., `|event| event.payload`).

**Generic type parameters**: Single-letter type vars forbidden when multiple
type vars exist. Use descriptive names (`Entity`, `View`, `Agg`, `Backend`). A
lone type var is acceptable when unambiguous.

#### Module Organization

Order by importance: public API first, private implementation, then tests. Every
module should have a `//!` docstring. What a module _does_ (consumer-facing
types/functions) goes before what _supports_ it (error types, helpers).

#### Line width in docstrings and macros

All doc comments (`//!` and `///`) and long strings inside attribute macros
(e.g., `#[error(...)]`) must not exceed 100 characters per line. `cargo fmt`
does not enforce this (without nightly rustfmt), so be careful and check
manually.

For multi-line `#[error]` strings, use `\` continuation.

#### Never use `is_err()`/`is_ok()` assertions in tests

**FORBIDDEN**: `assert!(x.is_err())`, `assert!(x.is_ok())`. For errors, unwrap
and assert the exact variant with `matches!`. For ok, just `.unwrap()`.

#### Prefer exhaustive `match` over `matches!` in production code

Exhaustive `match` forces handling new variants; `matches!` hides them behind
`_ => false`. **Test code**: `matches!` is fine for assertions. **Production
code**: always exhaustive `match` so the compiler flags new variants.

#### Assertions must be specific

Use `assert_eq!` with exact values, not `assert!(result.is_some())`. Never use
`||` in assertions unless outcomes are genuinely equivalent.

#### Serialization test assertions must use literals

When testing serialized output (JSON, etc.), assert against `json!()` literals,
never against re-serialized domain types. Comparing
`serde_json::to_value(field)` against the parent's serialized output tests
serde's Serialize derive against itself — if the derive is wrong, both sides are
wrong and the test still passes. Use `assert_eq!(parsed["field"], json!("10"))`
so the expected value is independent of the code under test.

#### Type modeling

Use enums (not optional fields) for mutually exclusive states, newtypes for
domain concepts, and typestate for protocol enforcement. Make invalid states
unrepresentable.

#### Avoid deep nesting

Prefer flat code over deeply nested blocks to improve readability and
maintainability. This includes test modules - do NOT nest submodules inside
`mod tests`. Put all tests directly in the `tests` module.

##### Techniques for flat code:

- **Early returns** with `?` and `return Err(...)` instead of nested `if let`
- **let-else** for guard clauses:
  `let Some(value) = expr else { return Err(...); };`
- **Pattern matching with guards** instead of nested `if let` chains

#### Struct field access

Use struct literal syntax and direct field access. Don't create `fn new()`
constructors or getters unless they add logic beyond setting/getting values.

#### Prefer destructuring over `.0` access

For newtypes, prefer `let TypeName(inner) = value` over `value.0` -- names the
type explicitly.

#### No one-liner helpers

If a helper's body is a single expression, inline it. Wrapping one function call
in another adds indirection without reducing complexity. Helpers must
encapsulate multi-step logic.

#### Don't split simple-but-long pattern matches

A single `match` with many trivial arms (state transitions, event mapping)
should stay as one function even if it exceeds line count lints. Request
permission to suppress `too_many_lines` rather than extracting pointless
helpers.
