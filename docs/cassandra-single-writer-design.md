---
id: cassandra-single-writer-design
title: Cassandra single-writer design
sidebar_label: Cassandra single-writer design
---

Design notes for the compare-and-set snapshot mode of `kafka-flow-persistence-cassandra`
(`CassandraSnapshots.withSchema(compareAndSet = true)`) — the mechanism and its subtleties. The Kafka
backend solves the same problem differently — see [Kafka single-writer design](kafka-single-writer-design.md).

## Problem

[kafka-flow#732](https://github.com/evolution-gaming/kafka-flow/issues/732): during a rebalance a
previous owner that has not yet observed the revocation keeps flushing snapshots alongside the new
owner, and the last-write-wins snapshots table lets a stale write overwrite a newer one — the next
recovery then loads stale state and skips the events in between. The
[Kafka design doc](kafka-single-writer-design.md) covers the failure in full. The Cassandra-specific
point: Cassandra has a conditional write (a Paxos lightweight transaction) but no transaction to bind
the input-offset commit to, so the fence is **per write**, not a transaction.

## Mechanism: compare-and-set

The stored offset is a per-key [fencing token](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html):
every persist asserts the stored offset is not greater than the one being written, so the
newest-by-offset writer wins regardless of who it is. The write is a Paxos lightweight transaction,
linearizable per partition key, so concurrent writers to one key are ordered without relying on clock
synchronisation:

```sql
UPDATE snapshots_v2 SET ... , offset = :offset WHERE <key> IF offset <= :offset
```

The first write of a key finds no row, so the conditional `UPDATE` does not apply; it falls back to
`INSERT ... IF NOT EXISTS`. If that loses a race to a concurrent insert, the conditional `UPDATE` is
retried once, so the newest snapshot still wins a first-write race. A rejected write raises
`SnapshotWriteConflict`. A not-applied result reports the stored `offset` when Cassandra returns it
and treats its absence (or a null) as "row absent".

The guard is per **key**, the right granularity: #732 corruption is per key and keys are independent,
so per-key monotonic durability is exactly what prevents it.

## Scope: persist-only — and what the full fence would cost

## Delete: offset-carrying tombstone

A delete cannot be a plain `DELETE`. Removing the row removes the `offset` guard with it, so a lagging
zombie's `INSERT ... IF NOT EXISTS` at a lower offset would then succeed — resurrecting a stale
snapshot. A later recovery would fold new events onto that resurrected base, silently losing the
delete: #732 reintroduced for that key.

So the compare-and-set delete (`deleteCompareAndSet`) writes an offset-carrying logical **tombstone**

```sql
UPDATE snapshots_v2 SET value = null, offset = :offset WHERE <key> IF offset <= :offset
```

keeping the row (and its `offset`). A stale lower-offset writer is then rejected, not resurrected; a
replayed delete is a no-op (equal offset) or a conflict (a newer write exists), never a resurrection;
a read surfaces the null `value` as a `Stored.Tombstone` (no live value). The tombstone is reaped by the TTL, if configured. Keeping
the row also routes the delete through Paxos, avoiding the well-known hazard of mixing lightweight
transactions and regular mutations on the same row.

**A breaking API.** Gating deletes means a delete must carry an offset: an offset-carrying logical
*tombstone* (`SET value = null`, keeping the row and its `offset`) instead of a row removal, so a
lower-offset writer is rejected rather than allowed to resurrect. But that offset has to reach the store
through `SnapshotWriteDatabase.delete`, forcing a source/binary-breaking `delete(key, offset)` signature
and offset plumbing through every delete path. Persist-only makes **no public API change** and needs no
major-version bump.

A delete and a re-persist are fenced on an offset that, just after recovery, can legitimately trail
the key's own stored snapshot. The partition resumes from the committed offset `C` (the minimum offset
still held across all of its keys); a single slow key can hold `C` well below a fast key's durable
snapshot offset `X` (the offset-lags-state invariant only guarantees `C <= X`). On recovery the
partition's processing offset starts at `C`, while the recovered snapshot's offset is `X`.

```mermaid
sequenceDiagram
    participant F as Owner<br/>(just recovered)
    participant B as Snapshot buffer
    participant DB as snapshots_v2<br/>(IF offset <= :offset)

    Note over DB,F: key recovered at snapshot offset X.<br/>partition resumes at committed offset C < X (a slow key held C back)
    F->>B: replay event at offset < X
    Note over B: buffer kept monotonic — holds high-water X,<br/>does not regress to the replayed offset
    F->>B: tick deletes the key (processing offset < X)
    B->>DB: delete gated on max(processing offset, X) = X
    Note over DB: IF offset <= X holds, the legitimate owner applies.<br/>Gating on the processing offset (< X) would be rejected<br/>as stale — the owner fencing itself.
```

If, in this window, the owner issues a write for that key:

- a **time-driven tick** that deletes the key would gate the delete on the processing offset `C` —
  `IF offset <= C` against the stored `X > C` rejects it, **crashing the legitimate owner** with a
  `SnapshotWriteConflict` that reads as if another writer owned the key;
- a **periodic flush** during replay would re-derive the snapshot and try to persist it at the
  replayed offset (`< X`) — the same rejection.

Neither is a safety problem (the durable snapshot stays at `X`), but both are a liveness problem: the
owner fences *itself*. The cause is that the in-memory snapshot buffer's offset *regresses* while
replaying events below `X` (a naive buffer `append` would overwrite it with the replayed, lower offset).

The fix keeps the buffered snapshot **monotonic in offset**: `Snapshots.append` drops a lower-offset
append rather than regressing the buffer (sound because, under the determinism the design already
assumes, re-folding events `<= X` reproduces the same state — see below). The buffer therefore stays
at the key's high-water `X`, so

- a delete is fenced on `max(currentOffset, highWater)` — the legitimate owner presents `X` and
  applies, while a genuinely stale writer (which only ever *reached* its own lower offset, never `X`)
  still presents that lower offset and stays fenced;
- a re-derived snapshot is not re-persisted below `X` (the buffer stays `persisted`), so the flush is
  a no-op.

Of these two, only the **delete** is irreducible *for a live recovered snapshot*. `SnapshotFold` drops
replayed events (`record.offset > snapshot.offset`), so a re-derived snapshot below `X` is never even
appended — the monotonic `append` is belt-and-suspenders for the persist case. A **tick-delete**
(`TickToState`) is timer-driven and bypasses that filter, so only the monotonic buffer lets a legitimate
tick-delete apply during replay.

The fence is live only for the offset-carrying `KafkaSnapshot` / compare-and-set wiring, which passes
`Some(_.offset)`; other stores pass `None` (unfenced, last-write-wins). It is surgical even when live:
`max(currentOffset, highWater)` differs from `currentOffset` only inside this replay window.

This fence and the tombstone above are independent and complementary: presenting the higher `highWater`
for a delete makes the tombstone it writes *more* protective against a lower-offset revive, never less.

### Recovering a deleted key

Both protections above are keyed on the high-water `X`, which a recovery establishes from the *recovered
snapshot's* offset. But a deleted key's row is a tombstone — its `value` is null, read back as absent — so
a read that surfaced only the value would establish no floor: the buffer would start empty and
`Snapshots.append` would climb from the replayed offsets (all `< X`), re-persisting below `X`. The
offset-`X` tombstone then rejects that write, fencing the *legitimate* owner; the flow tears down,
re-recovers the same tombstone (again no floor), and loops — a livelock. (`SnapshotFold`'s filter is keyed
on the same recovered offset, so it too has no floor for a tombstone; the monotonic buffer is what carries
the deleted-key case. The tick-**delete** path does not arise here: after an absent recovery nothing is yet
persisted, so a replay-window tick-delete is dispatched `persist = false` — buffer-only, never reaching the
store.)

So recovery must surface the tombstone's offset as the floor, not collapse it to "nothing there".
`SnapshotDatabase.read` returns `Some(Stored(None, X))` for a tombstone — value-less but carrying the
offset — distinct from `None` for a reaped or never-written key (and from `Some(Stored(Some(v), X))` for a
live snapshot); `Snapshots` holds `X` as the buffer high-water even with no buffered value, so a re-derived
snapshot below `X` is dropped exactly as for a live snapshot and the owner makes progress. The deleted-key
recovery is thus symmetric with the live one — same floor, only the value is absent — and the floor is
re-established on every recovery, so the livelock cannot form. (Because `read` returns one `Stored`, a
wrapper such as the metrics one has a single read to delegate — there is no separate value-only path that
could silently drop the tombstone's offset.)

The same hazard reaches the **events-recovery** mode (`restoreEvents`), where state is restored by folding
the journal rather than reading the snapshot. A delete clears the key's journal, so the fold yields `None`
and the high-water `X` survives only on the snapshot tombstone — the buffer would again start with no floor.
So events-recovery reads the snapshot store for its tombstone floor (`ReadState` runs `Snapshots.read` for
that side-effect) before folding the journal; the recovered state still comes from the journal. A *live* key
needs no floor here — its journal is intact, so the fold reconstructs `X` itself. Note compare-and-set
protects the snapshot store, which events-recovery does not read for state, so pairing the two buys no
stale-writer safety; seeding the floor only removes the deleted-key livelock for setups that nonetheless
enable both.

## Equal-offset writes and determinism

`IF offset <= :offset` admits an *equal* offset, so a stale writer holding exactly the stored offset is
not detected. This is deliberate. The legitimate owner can act at an offset it has already stored: a
tick can change state — or delete — without consuming a new record, and in the replay window above a
tick-delete is fenced on the high-water `X`, which equals the stored offset; a strict `<` would reject
these and fence the owner against itself. Admitting equal is safe not because the new value is identical
but because a same-offset write does not move the recovery point — unlike a lower-offset write it cannot
drop committed events (#732). The *records* folded into any two snapshots at the same offset are the
same, so a same-offset re-persist differs at most in time-driven tick state, never in event data; a
replayed delete is a no-op. Deterministic, replayable folds are therefore a precondition of the CAS
mode (as they already are of recovery generally) — the same property that lets the monotonic buffer
drop a lower-offset replay as a no-op.

## Consistency

The lightweight transaction reaches consensus at the *serial* consistency level, which is distinct
from the read/write levels in `ConsistencyOverrides` and defaults to `SERIAL` (a cross-datacenter
quorum). `ConsistencyOverrides.write` governs only the transaction's commit phase, not its consensus.
For single-datacenter partition ownership (the common case) set the scassandra client's
`query.serial-consistency = LOCAL_SERIAL` to keep the Paxos rounds in-DC; otherwise every conditional
write and delete pays a cross-datacenter round-trip. Recovery reads at `ConsistencyOverrides.read`;
a serial read is not required, because `R + W > N` makes a non-serial read see every committed
snapshot, and a still-in-flight write — one whose `persist` has not completed — is safe to miss
(recovery re-folds from the committed offset).

Compare-and-set does require read and write consistency at `QUORUM` or stronger (so `R + W > N`).
For single-datacenter ownership, `LOCAL_QUORUM` at both levels satisfies `R + W > N` within the
local DC and pairs with `LOCAL_SERIAL`. What matters is access locality — a key's writers and its
recovery read all in one DC — not the replication footprint, which may still span DCs for DR. The
conditional write reaches consensus on a serial quorum but *materialises* at `ConsistencyOverrides.write`;
with a weaker write level a (non-serial) `QUORUM` recovery read can miss the newest committed snapshot
even with no in-flight Paxos, reintroducing #732 on the read side — which the write-side fence does not
heal. This is not a default: `ConsistencyOverrides` is empty unless you set it, so the snapshot table
inherits the session's default level (often `LOCAL_ONE`) — you must configure read and write to a
quorum. Keep `R + W > N` within one consistency domain and a key's Paxos in one DC: the `LOCAL_*` set
is for single-DC ownership, while ownership that can fail over between DCs (or write a key from two
DCs) needs the cross-DC `SERIAL` / `QUORUM` levels.

## TTL and rollout

The `offset` guard lives in the snapshot row, so it expires with the row's TTL. After a row's TTL
lapses the guard is gone and a stale write can land a fresh `INSERT`; this is harmless when the TTL far
exceeds the rebalance/zombie overlap window (the usual case — a zombie outliving the TTL is not
realistic), but the monotonicity guarantee only holds within the TTL.

Without a TTL the offset-carrying tombstone is never reaped, so a deleted key leaves its row behind
indefinitely and the table grows by one row per deleted key. Configure a TTL (comfortably above the
overlap window) for workloads that delete keys.

Enabling on a running system needs no migration (the condition reads the `offset` column every version
already writes). The one rolling-deploy caveat: a lightweight transaction uses a coordinator-generated
write timestamp while a regular write uses a client-side one, so during a mixed deploy an application
clock running ahead of the coordinators can let an old (plain-write) instance's snapshot shadow a newer
conditional one. Negligible with NTP-synced clocks, and gone once every instance writes conditionally.

## Implementation

Entry point: `CassandraSnapshots.withSchema(compareAndSet = true)` (or
`CassandraPersistence.withSchema(snapshotCompareAndSet = true)`). In the current code:

- **Unified write / read** — `CassandraSnapshots.write` routes a `Stored`: a present value to
  `persistCompareAndSet`, an absent value (a delete) to `deleteCompareAndSet`, both issuing the offset-gated
  `UPDATE`/`INSERT`; `resolveConditional` classifies the result (`applied` / newer-stored-offset /
  row-absent), shared by both. `read` returns the stored unit — `Some(Stored(value, offset))`, with an
  absent value for a tombstone — or `None` for no row.
- **Monotonic buffer** — `Snapshots` holds one `Stored` cell (a live snapshot or an offset-carrying
  tombstone floor); `append` and `delete` both flow through one monotonic `put` that drops a lower-offset
  re-persist and lifts a lower-offset delete to the high-water. Recovery seeds the cell (and thus the floor)
  from `SnapshotDatabase.read`: `Some(Stored(None, X))` is a tombstone at `X`, `None` is a reaped or
  never-written key.
- **Replay filter** — `SnapshotFold` drops replayed events above the recovered offset; a timer-driven
  `TickToState` delete bypasses that filter, which is why the monotonic buffer is what carries the
  tick-delete case.
- **Events-recovery floor** — `ReadState` runs `Snapshots.read` for its tombstone-floor side effect
  before folding the journal.
- **Offset accessor** — the offset-carrying wiring passes `Some(_.offset)` (live fence); other stores
  pass `None` (unfenced, last-write-wins).

## Testing

- Store-level against real Cassandra (`SnapshotSpec`, persistence-cassandra-it-tests) — monotonic
  writes, stale-write rejection, the tombstone (no resurrection, replayed/stale delete, equal-offset),
  idempotent delete, TTL on both the insert and update paths, and concurrent first-writers racing on a
  fresh key (the highest offset wins, no corruption — exercising the first-write retry path).
- Through the real PartitionFlow / eager-recovery / flush-on-revoke machinery (`FlowSpec`,
  persistence-cassandra-it-tests) — the #732 reproduction asserts corruption under last-write-wins;
  the prevention asserts the stale flush is rejected; and a delete during replay below the recovered
  snapshot offset applies (fenced on the high-water), not rejected.
- In core — the monotonic buffer and the delete fence: `SnapshotsSpec` (the buffer directly) and
  `SnapshotsOfSpec` (the offset-of fencing wiring), with `SnapshotReplayFencingSpec` driving the
  replay-window cases flow-level against an offset-gated in-memory store.

### Formal models

The mechanisms above are model-checked in `models/` (TLA+), as a refinement tower: one abstract
`SingleWriterStore` spec (the durable cell always equals the correct fold) that each design refines.

- **`Cassandra`** — the offset compare-and-set, the tombstone, and the replay-window monotone buffer,
  all three load-bearing negative controls: `cassandra_unguarded` (no offset guard) and
  `cassandra_notomb` (plain delete) each break the refinement / `INV_NoCorruptDurable`, and
  `cassandra_replay_fixoff` (no monotone buffer) makes the legitimate owner livelock mid-replay
  (conflict→recover→retry, never committing; `RefLive` violated), while `cassandra_refines` holds with
  all three. Modelled *with the zombie present*, so a genuinely stale writer is still correctly
  rejected; only the legitimate owner livelocks. (`models/README.md` covers how Kafka protects the same
  shared replay window differently — atomic offset binding rather than the monotone buffer.)
- **`CasFirstWrite`** — the non-atomic first-write `UPDATE`/`INSERT`/retry compound, checked under
  every interleaving against the atomic CAS it stands in for (`CasFirstWriteAtomic`). Its one deviation,
  a spurious conflict (the retry finds the row gone), is fed into the conflict/recover loop in
  `cassandra_firstwrite_spurious`: it leaves the row absent, so no replay window arises and it converges
  (it recovers on the next flush) — in contrast to the replay-window livelock.

The models verify behaviour *under* the assumptions below; they do not prove them. See `models/README.md`
for the full config catalogue and the rejected designs that must *fail* checking.

## Assumptions

This design takes three things as given:

- **Per-key linearizable compare-and-set.** Each Cassandra lightweight transaction on a row is an atomic,
  linearizable operation (Paxos). The first-write `UPDATE`/`INSERT`/retry compound is *not* atomic.
- **Deterministic, replayable folds (a user contract).** Re-folding a recovered base over the same events
  reproduces the same state. This is what makes a lower-offset replay a no-op (so the buffer can stay
  monotonic) and what makes equal-offset writes idempotent. A non-deterministic fold breaks both.
- **Per-key independence.** #732 corruption is per key; keys are independent, so per-key monotonic
  durability is the whole guarantee.

## Rejected alternatives

- **Offset-as-write-timestamp (LWW register)**: write each snapshot `USING TIMESTAMP <offset>` and let
  Cassandra's last-write-wins reconciliation keep the highest-offset cell — a plain quorum write, much
  cheaper than a Paxos round, and a delete becomes a tombstone ordered by offset. Rejected as the
  default: equal-offset replacement breaks (at equal timestamps Cassandra breaks ties by value, not
  write order), a rolling deploy inverts catastrophically (old instances write wall-clock timestamps
  that dominate every offset-as-timestamp value), and it discards the real write timestamps.
- **Lease / ownership table**: a per-partition lease acquired with one LWT, then cheap writes. The
  lease alone does not stop a paused leaseholder's plain writes (last-write-wins still applies), so a
  per-write fencing token is still required — at which point the lease only adds liveness/expiry
  concerns on top of the per-write CAS.
- **Composite `(offset, generation)` token**: gate on the consumer generation as well as the offset,
  closing the equal-offset gap and giving per-partition (not just per-key) ownership. Couples the
  self-contained Cassandra module to the live consumer generation; reasonable as a future strict mode,
  not a default.
- **Recovery-side reconciliation** (store the offset, recover from the lowest): does not prevent the
  stale overwrite (last-write-wins still corrupts), so strictly weaker than fencing the write.

## Forward-looking

[KIP-939 (participation in 2PC)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-939:+Support+Participation+in+2PC)
could extend a Kafka generation fence to this Cassandra store: a transactional producer in an
externally-coordinated two-phase commit could bind the Cassandra snapshot write to a generation-fenced
Kafka input-offset commit, giving Cassandra per-partition ownership without the per-key compare-and-set.
Not actionable now; see the Kafka design doc's forward-looking note.
