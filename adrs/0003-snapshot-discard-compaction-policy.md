# ADR-0003: Snapshot discard-on-deserialize must be compaction-policy-aware

## Status

Accepted.

## Context

PR #29 (`fix/snapshot-discard-on-deserialize`) made aggregate loading tolerant
of a stored snapshot whose payload no longer deserializes into the current
aggregate type (a non-versioned schema/wire-format change, or corruption).
Instead of failing the load hard, the repository's `get_snapshot` discards the
incompatible snapshot and returns `Ok(None)`, so `cqrs-es` rebuilds the
aggregate by replaying events.

That is safe **only when the full event history needed to reconstruct the
aggregate still exists**. For `CompactionPolicy::CompactAfterSnapshot`
aggregates it does not: `compact_events` deletes events with
`sequence <= snapshots.last_sequence`, so after compaction the snapshot can be
the _only_ durable record of pre-compaction state. Discarding it and "rebuilding
from events" then reconstructs partial or `Default` state — silent data loss.
`docs/cqrs.md` already forbids manually deleting snapshots for compactable
aggregates, and the startup `Reconciler` refuses to clear them
(`ReconcileError::CompactedSnapshotClear`). The read-path discard added in PR
#29 bypassed that guard.

We attempted to make the read-path discard safe by **inferring, from the event
rows, whether the covering history survives**. Two predicates were tried and
both are unsound:

- `COUNT(events WHERE sequence <= boundary) == boundary` — assumes per-aggregate
  sequences are contiguous from 1. It correctly rejects compacted histories, but
  it also rejects **sparse** sequences (stores that assign sequence numbers from
  a counter shared across aggregates, e.g. migrated/federated data), reporting a
  complete history as "compacted" and failing the load.
- `EXISTS(event at sequence = boundary)` — contiguity-agnostic, so it accepts
  sparse histories. But it only proves the _latest_ event survives. Under
  **multi-generation compaction** (compaction ran at an earlier boundary `N`,
  deleting events `1..N`; the snapshot has since advanced to `M`; events
  `N+1..M` and the boundary event `M` remain) it reports "replayable" while the
  `1..N` prefix is gone — silent partial rebuild.

No data-only predicate can distinguish "a compacted prefix (unsafe to discard)"
from "a sparse-but-complete history (safe to discard)", because in sparse data
the origination event need not be sequence 1, so "is the prefix present?" is not
answerable from the rows alone. The missing information — _whether this
aggregate's events can ever be deleted_ — is a domain property
(`EventSourced::COMPACTION_POLICY`), not something recoverable from the event
table.

## Decision

Decide discard-on-deserialize by the aggregate's `COMPACTION_POLICY`, not by
inspecting event data:

- **`CompactionPolicy::Retain`** — events are never deleted, so the full history
  is always present (contiguous or sparse). An incompatible snapshot is always
  safe to discard; delete it and let `cqrs-es` rebuild from events.
- **`CompactionPolicy::CompactAfterSnapshot`** — a prefix may have been
  compacted away and the snapshot may be the only durable state. **Never
  discard**; surface a deserialization error on load. This makes the failure
  loud and consistent with the `Reconciler`'s `CompactedSnapshotClear` guard and
  `docs/cqrs.md`: recovery requires an explicit migration / `SCHEMA_VERSION`
  bump, never a silent rebuild from incomplete history.

The discard decision is owned by the layer that can see
`Entity::COMPACTION_POLICY` (the `event-sorcery` wiring that constructs the
repository), since the generic `sqlite-es` repository — parameterised over
`cqrs_es::Aggregate` — cannot see the policy. The policy is supplied to the
snapshot-load path at construction; exact plumbing is an implementation detail
of this change.

The data-inference primitive `sqlite_es::discard_snapshot_if_replayable` (and
its `COUNT`/`EXISTS`/`last_sequence`-pin/re-query logic) is **removed**.

## Consequences

- Eliminates the silent-partial-rebuild class entirely. The crate-level claim
  that incompatible snapshots are "rebuilt from events ... state is never
  silently lost" becomes true rather than aspirational.
- Removes, rather than fixes, the edge cases the SQL inference introduced: the
  load-vs-delete TOCTOU between concurrent loaders, the sparse-sequence false
  rejection, and the `last_sequence = 0` boundary. None can exist once there is
  no inference.
- `Retain` aggregates — including every projected entity, which the
  `StoreBuilder` `const`-asserts to be `Retain` — keep self-healing on a
  non-versioned shape change, exactly as PR #29 intended.
- `CompactAfterSnapshot` aggregates with an incompatible snapshot now **fail
  loudly on load** instead of silently rebuilding wrong state. This is the
  intended behaviour for genuinely irrecoverable state; recovery is an operator
  action (migration / `SCHEMA_VERSION` bump), the same contract the `Reconciler`
  already enforces at startup.
- Requires threading `COMPACTION_POLICY` into the snapshot-load path — a small,
  contained surface change confined to the `event-sorcery` repository and its
  construction sites.

## Alternatives considered

- **Data-inference predicates** (`COUNT == boundary`; `EXISTS(boundary)`):
  rejected. Provably cannot distinguish compaction from sparse sequences; one or
  the other always misfires, and the `EXISTS` variant silently loses state under
  multi-generation compaction.
- **Verify the minimum surviving event is the origination event**: rejected. The
  generic repository cannot evaluate `originate()`, and in sparse data the
  origination event is not identifiable by sequence number.
- **Accept the narrow window and document an operator contract** ("always bump
  `SCHEMA_VERSION` on a shape change"): rejected for this library. The window is
  narrow in reachability but its consequence is silent financial-state
  corruption in precisely the failure mode this safety net exists to handle;
  leaving a documented sharp edge is not acceptable here.
