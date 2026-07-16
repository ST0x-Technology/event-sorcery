# ADR-0005: Bounded per-aggregate lock table via weak entries

## Status

Accepted. Amends [ADR-0004](0004-per-aggregate-command-serialization.md): it
supersedes that ADR's "No eviction" tradeoff only. The decision to serialize
commands per aggregate is unchanged.

## Context

ADR-0004 accepted unbounded growth of the per-aggregate lock map on the premise
that "this library's consumers have bounded business-entity cardinality (not
per-request keys)". Auditing both production consumers falsified that premise on
their highest-volume aggregates:

- `st0x.liquidity` keys `OnChainTrade` by `{tx_hash, log_index}` -- one fresh
  aggregate ID per on-chain fill -- and `OffchainOrder` by a `Uuid` minted per
  hedge order.
- `st0x.issuance` keys `Mint` by a `Uuid` minted per `POST /mints` request, and
  `Redemption` by the burn transaction's hash.

Both processes run for weeks between deploys. One never-released
`Arc<tokio::sync::Mutex<()>>` plus its `String` key per aggregate ID is a memory
leak growing linearly with trading volume. The premise has to be retracted, not
worked around downstream -- and the aggregates that leak hardest are precisely
the ones that need serialization least, since each ID is commanded a handful of
times and then never again.

The obvious repair -- remove the entry when the last guard drops -- silently
breaks ADR-0004's ordering guarantee:

```text
T1: acquire("K") -> lock map -> clone the mutex out -> unlock map  [preempted]
T2: (holding K's guard) -> drops guard -> evicts "K" from the map
T3: acquire("K") -> lock map -> miss -> insert a FRESH mutex -> locks it
T1: [resumes] -> locks the OLD mutex, which is now free
```

T1 and T3 then run their whole `execute()` for `K` concurrently. No error, no
panic -- reactors and projections simply observe events out of commit order
again, which is the exact bug ADR-0004 exists to prevent. The root cause is that
a task which has cloned the mutex out of the map but has not yet acquired it
holds lock interest that the map cannot see.

## Decision

Store `Weak<tokio::sync::Mutex<()>>` in the lock table instead of `Arc`. Strong
references then exist only along the acquire path -- the clone taken under the
map lock, the `lock_owned()` future it is moved into, and the resulting
`OwnedMutexGuard` -- so a strong reference exists **iff** some task holds or
awaits the lock. Lock interest _is_ a strong reference.

`acquire` resolves an entry with `Weak::upgrade` under the table's outer
`std::sync::Mutex`, and inserts a fresh mutex only when that upgrade fails.

This makes the race above unrepresentable rather than unlikely:

1. A replacement mutex for `K` is created only when `upgrade()` returns `None`
   under the map lock.
2. `upgrade()` returns `None` iff the strong count is zero, and the strong count
   of a table mutex only ever _increases_ inside that same map lock -- the
   upgrade is the only count-increasing site; everything downstream moves the
   `Arc` rather than cloning it. So "nobody holds this" observed under the lock
   cannot be invalidated by a concurrent acquirer.
3. Every task in the lookup-to-acquisition window holds a strong reference by
   construction. So an in-window, queued, or holding task implies the upgrade
   succeeds, which implies the newcomer joins the _same_ mutex.
4. Once the strong count reaches zero the `Weak` is permanently dead and the
   mutex is already deallocated, so a stale mutex can never be acquired late.

No eviction threshold, no `Drop` impl, and no release-path bookkeeping -- the
mutex frees itself the moment its last holder or waiter goes away, including
under cancellation, where a cancelled waiter simply drops its `Arc`.

What remains in the table after that is a dead entry: a `String` key and a dead
`Weak` pointer, with no mutex behind it. That is pure garbage, carrying zero
correctness weight, and it is collected by an amortized sweep on the insert
path: sweep when the table reaches a watermark, retaining only entries with a
live strong count, then set the watermark to
`max(2 * live, MIN_SWEEP_WATERMARK)` and shrink the map back to it.

The shrink is not incidental. `HashMap::retain` walks the bucket array, so a
sweep costs O(capacity), not O(len), and a `std` `HashMap` never shrinks itself
on `retain`/`remove`. Retaining alone would therefore bound the table by _peak_
concurrency rather than current: a burst of `P` concurrent sends grows capacity
to ~`2P` and leaves it there for the life of the process, and once that burst
drains the watermark falls back to `MIN_SWEEP_WATERMARK` while every subsequent
sweep still pays O(`P`) -- triggered by as few as `MIN_SWEEP_WATERMARK / 2`
inserts. Shrinking to the new watermark after the retain re-ties capacity to the
live count.

With it, table size is bounded by
`max(2 * concurrent in-flight sends, MIN_SWEEP_WATERMARK)` -- current
concurrency, not the historical peak -- and sweep cost is amortized O(1) per
acquire, since each O(capacity) sweep is preceded by at least as many inserts as
it costs.

## Alternatives considered

- **Evict on last release via `strong_count`.** Sound, but fragile in exactly
  the way ADR-0004 anticipated when it rejected this: correctness depends on the
  threshold (`== 2`, not `<= 1`), on drop _order_ within a wrapper guard, and on
  every count-decreasing path being paired with an eviction check -- including a
  cancelled waiter, which produces no guard at all and would otherwise orphan
  its entry forever. It also takes the map lock again on every release, doubling
  hot-path traffic on it. The chosen design needs none of that: it tests lock
  interest directly instead of counting proxies for it.
- **Consumer-declared opt-out** (an `EventSourced` associated const marking an
  entity as not needing serialization). Rejected: the declaration is
  unverifiable by the library, and getting it wrong silently reintroduces the
  ordering bug with no compile-time or runtime signal. It also rests on a false
  premise -- the "write-once" aggregates do receive multiple commands on one ID
  (`OnChainTrade` is witnessed then acknowledged; `OffchainOrder` is placed then
  polled repeatedly), so they genuinely want serialization, just briefly.
- **Periodic background sweep.** `Store` has no task lifecycle or shutdown story
  to own a background task, the interval would be an unprincipled knob, and
  memory would still grow unbounded _between_ ticks under load. The inline
  amortized sweep dominates it.
- **Sharded lock array** (fixed N mutexes, `hash(id) % N`). Bounded by
  construction, but two unrelated aggregates colliding on a shard would
  serialize against each other -- and because reactors run _under_ the lock, a
  reactor commanding aggregate `B` from inside a command on a same-shard
  aggregate `A` would self-deadlock with no cycle present, which the reentrancy
  guard cannot catch because the keys differ. Fatal.

## Consequences

- Lock-table memory is bounded by concurrency rather than by aggregate-ID
  cardinality. Per-event and per-request ID schemes are supported, so ADR-0004's
  "Unbounded lock-table growth" consequence, and the bounded-cardinality
  requirement its `Store` doc imposed on consumers, are both retracted.
- The steady-state hit path swaps an `Arc` clone for a `Weak::upgrade` (one
  atomic compare-exchange loop). The release path is untouched -- no additional
  map locking.
- A dead entry can linger until the next sweep. It is garbage only: never a
  stale mutex that some task could still acquire.
- The lock for an aggregate is now created fresh per burst of activity rather
  than once per process. This is behaviourally invisible -- a mutex with no
  holders and no waiters carries no state worth preserving.
