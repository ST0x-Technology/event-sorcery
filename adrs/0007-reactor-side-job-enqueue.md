# 0007. Reactor-side durable-job enqueue, defer, dedup, fail-stop, and cancel

- Status: Accepted
- Date: 2026-06-28
- Issue: none yet — warrants a Linear issue (the durable-jobs enqueue-model
  extension / liquidity migration epic). Extends
  [ADR-0006](0006-cqrs-native-durable-jobs.md).

## Context

[ADR-0006](0006-cqrs-native-durable-jobs.md) established cqrs-native durable
jobs with **handler-only enqueue**: a `JobQueue<Self::Jobs>` buffers pushes into
the `PENDING_JOBS` scope that `Store::send` opens around a command, so the
`Enqueued` event commits atomically with the triggering command's events. A push
outside that scope is dropped with a warning. That model is correct for jobs
_born from a command_ — but it is the only enqueue path event-sorcery has.

A full audit of the primary consumer, `st0x.liquidity` (17 durable job kinds
over its own apalis-sqlite framework, `conductor::job`), found that **every one
of those jobs is enqueued from a reactor (in response to a committed domain
event), a polling loop, a sibling job (a chain), a self-reschedule, or startup
recovery — and zero from command handlers.** So ADR-0006's `JobQueue` cannot
enqueue a single liquidity job, and the migration off the manual apalis
framework is blocked. The analysis is recorded in
[`docs/migrating-to-durable-jobs.md`](../docs/migrating-to-durable-jobs.md).

event-sorcery already covers the _execution_ machinery well, often better than
the apalis framework: durable at-least-once execution, retry with backoff, claim
/ lease / `renew` orphan recovery (which sidesteps the apalis
deterministic-worker- name heartbeat bug that made `requeue_orphaned`
necessary), dead-letter with audit retention, claim-budget crash-loop
protection, execution timeout, sub-second poll. The gap is entirely on the
**enqueue and lifecycle-control side**, plus a few operational hooks.

The desired end state: a consumer can enqueue durable jobs from anywhere (not
just command handlers), snooze a job without burning its retry budget, dedupe
enqueues, escalate a terminal job failure to a process-level fail-stop, and
cancel stale pending work — so a manual / apalis job system can be fully
replaced. Without these the migration is not just inconvenient but **unsafe**:
the self-rescheduling jobs would land on the failure-only retry path and
crash-loop, and the money-movement jobs could re-burn funds without dedup (see
the migration guide's safety section).

## Decision

Keep ADR-0006 in force (handler-push stays the default for command-born jobs)
and **extend** the durable-jobs model with the following, each additive and a
no-op until used:

1. **Standalone durable-enqueue API.** A method on `JobRuntime` (e.g.
   `enqueue<J: Job>(&self, job: J)` + `enqueue_with_delay`) that appends the
   `Enqueued` event and seeds the `job_queue` projection row in its _own_
   transaction, not bound to a command commit. This is the central unblocker for
   reactor / poller / chain / startup enqueues. It is explicitly _not_ atomic
   with any triggering event (the triggering event has already committed) — the
   same non-atomicity apalis has, and acceptable.

2. **Defer / snooze success outcome.** `perform` gains a way to return a
   "defer(delay)" success that emits a new `JobEvent::Rescheduled` re-arming
   `run_at` _without_ incrementing `attempt` or recording a failure — distinct
   from `RetryScheduled` (a failure). This serves the self-rescheduling pollers
   (poll-while-pending, CCTP attestation waits, recovery guard contention) and
   lets a long external wait be modelled as defer rather than a long-held lease.

3. **Enqueue-time dedup + in-flight queries.** An optional `Job::dedup_key`; the
   standalone enqueue returns an "already in flight" result instead of enqueuing
   when a non-terminal job with the same `(kind, dedup_key)` exists. Plus
   `has_in_flight(kind)` / pending-count queries on `JobRuntime`. This is
   _additive_ defense: consumers keep their own event-reconstructed fail-closed
   guards for money-critical paths, keyed on **domain identity, never
   `job_id`**.

4. **Per-kind terminal-failure (fail-stop) hook.** A callback fired when a job
   reaches `Dead` (including `DeadReason::Abandoned` from claim-budget
   exhaustion). The consumer wires it to crash the process so a supervisor
   restarts clean. A kind that does _not_ install the hook is the best-effort
   tier — one mechanism covers both fail-stop and best-effort. The hook must
   abort fast: with `max_concurrency = 1` the ES poll loop does **not** halt
   after a dead-letter (it proceeds to the next candidate), so correctness
   depends on the hook stopping the process before the next claim.

5. **Cancel-by-kind / cancel-by-dedup-key.** A command emitting a terminal
   `Cancelled` outcome (a `DeadReason::Cancelled` variant) for discarding stale
   pending work (e.g. rebalancing checks captured against pre-rebalance
   inventory). Lease-expiry re-claim must not race a just-cancelled job back to
   runnable.

6. **Operational docs + cleanup.** `lease_duration` / `execution_timeout` /
   `poll_interval` sizing guidance (steer long external waits to _defer_, not
   long-held leases), and `prune_terminal_jobs` (the ADR-0006 follow-up) to
   bound `job_queue` growth while retaining `Dead` rows for audit.

The additions ship as separate PRs in dependency order (enqueue → defer → dedup
→ fail-stop → cancel). New `JobEvent` / `JobCommand` variants (`Rescheduled`,
`Cancelled`) are additive — `SCHEMA_VERSION` is unchanged; existing streams fold
unaffected.

## Alternatives Considered

### A. Reshape every reactor/poller/chain enqueue into command -> handler -> push (keep handler-only)

- Pros: no new enqueue API; every enqueue stays atomic with events; a single
  enqueue path.
- Cons: artificial or incorrect for pollers (a poll tick has no domain event to
  emit) and for job-chains (a job would invent an event + handler purely to
  satisfy the enqueue weld); a large unnatural reshape of all 17 liquidity jobs.
- Rejected because: it forces a fake command+event+handler triad onto enqueues
  that have no domain command behind them, inventing aggregates/events just to
  enqueue — more complexity than a standalone API, and it still would not
  provide defer, dedup, or fail-stop.

### B. Keep liquidity on apalis; do not migrate

- Pros: zero event-sorcery work; liquidity's framework is battle-tested.
- Cons: two job systems in the org; the manual apalis framework (SQL dedup,
  `requeue_orphaned`, manual guards) is exactly the duplication event-sorcery
  was built to remove; liquidity carries a second sqlx/apalis stack
  indefinitely.
- Rejected because: the org goal is one shared event-sourcing library; leaving
  liquidity on a parallel job framework defeats event-sorcery's purpose and
  keeps the manual job-state the migration is meant to eliminate.

### C. Ship the standalone enqueue only; defer the rest

- Pros: smallest unblocker; one PR.
- Cons: the 6 self-rescheduling jobs would land on the failure-only retry path,
  exhaust `max_attempts`, dead-letter, and (with the fail-stop hook) crash-loop;
  the money-movement jobs without dedup risk CCTP re-burn.
- Rejected because: defer, dedup, and the fail-stop hook are co-requisites for a
  _safe_ migration (per the migration analysis) — shipping enqueue alone invites
  the exact crash-loop and re-burn failures the analysis flagged. (Standalone
  enqueue is still the first PR; "alone" means shipping it as the whole
  migration enabler.)

## Consequences

- The job model carries two enqueue paths: handler-push (command-born, atomic
  with events) and standalone (everything else). The docs must make the choice
  obvious.
- `perform`'s return shape grows a defer signal; the core fold and worker handle
  a new `Rescheduled` event. A bug here affects all durable jobs — mitigated by
  the existing worker test suite plus new tests per addition.
- The at-least-once-per-`perform` reality combined with standalone (non-atomic)
  enqueue means consumers MUST audit each job's idempotency on domain identity,
  not `job_id` (ADR-0006's idempotency contract is refined accordingly in the
  migration guide).
- The subtlest risk is the fail-stop-vs-next-poll race: the hook must stop the
  process before the next claim, or a stale-state next job can run between
  `Dead` and exit. Implementation must pin this down (synchronous abort).
- Each addition is independently shippable and reversible until consumed; the
  liquidity migration then proceeds per `docs/migrating-to-durable-jobs.md`
  (both backends coexisting on one SQLite file, atomic per-kind enqueuer flip,
  leaf-first, drain-before-decommission, `AccountForDexTrade` as the one
  cutover-critical queue).
- Blast radius if wrong: the enqueue + defer paths touch the core job lifecycle,
  so a defect affects every consumer's jobs, not one feature. This is why it is
  an ADR and ships incrementally with per-addition tests rather than as one
  change.
