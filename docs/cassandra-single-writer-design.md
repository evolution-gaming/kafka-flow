---
id: cassandra-single-writer-design
title: Cassandra single-writer design
sidebar_label: Cassandra single-writer design
---

Design notes for the compare-and-set snapshot mode of `kafka-flow-persistence-cassandra`
(`CassandraSnapshots.withSchema(compareAndSet = true)`). User-facing guarantees, costs and rollout
guidance are in [Persistence](persistence.md#protecting-against-stale-snapshot-writes); this page
records the mechanism and its subtleties. The Kafka backend solves the same problem differently — see
[Kafka single-writer design](kafka-single-writer-design.md). Formal models are in `models/` in the repo.

## Problem

[kafka-flow#732](https://github.com/evolution-gaming/kafka-flow/issues/732): consumer-group ownership
of the input topic does not extend to the snapshot store. During a rebalance a previous owner that has
not yet observed the revocation (network issue, GC pause, slow poll loop) keeps folding events and
flushing snapshots alongside the new owner. The snapshots table is last-write-wins, so a stale
snapshot can overwrite a newer one; the next recovery loads stale state and loses the events between
the two snapshots, even though their input offsets were committed. Overlaps of tens of seconds have
been observed in production.

```mermaid
sequenceDiagram
    participant A as Owner A<br/>(previous)
    participant DB as snapshots_v2<br/>(last-write-wins)
    participant B as Owner B<br/>(new)

    Note over A: folds input up to offset 100,<br/>state buffered, not yet flushed
    Note over A,B: rebalance — partition revoked from A and assigned to B,<br/>but A has not observed it yet
    B->>DB: recover: read snapshot (none / older)
    B->>B: fold input up to offset 150
    B->>DB: persist snapshot @150 ✓
    A-->>DB: flush buffered snapshot @100<br/>(stale: A no longer owns the partition)
    Note over DB: last write wins — @100 overwrites @150
    Note over DB,B: next recovery loads @100<br/>(events 101 to 150 lost)
```

## Mechanism: compare-and-set

The stored offset is a per-key [fencing token](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html):
every write asserts that the stored offset is not greater than the one being written, so the
newest-by-offset writer wins regardless of who it is. Unlike Kafka, Cassandra offers a conditional
write (a Paxos lightweight transaction), so the fence is per write rather than a transaction binding
the input-offset commit.

A snapshot write (`CassandraSnapshots.persistCompareAndSet`) is

```sql
UPDATE snapshots_v2 SET ... , offset = :offset WHERE <key> IF offset <= :offset
```

The first write of a key finds no row, so the conditional `UPDATE` does not apply; it falls back to
`INSERT ... IF NOT EXISTS`. If that loses a race to a concurrent insert, the conditional `UPDATE` is
retried once, so the newest snapshot still wins a first-write race. A rejected write raises
`SnapshotWriteConflict` (handled uniformly, see [Persistence](persistence.md#protecting-against-stale-snapshot-writes)).
Result classification (`applied` / newer-stored-offset / row-absent) is shared by persist and delete
in `resolveConditional`; a not-applied result reports the stored `offset` when Cassandra returns it
and treats its absence (or a null) as "row absent".

This first-write path is the one place a persist is **not** a single atomic compare-and-set — it is a
compound of separate Paxos transactions with interleaving gaps. It is still safe by construction: both
`UPDATE`s are offset-gated and `INSERT ... IF NOT EXISTS` only ever writes to an absent cell (nothing to
overwrite), so no interleaving can produce a stale overwrite. The only deviation from the atomic
abstraction is a *spurious* conflict — the retry finding the row gone because a TTL reap (or a
last-write-wins hard delete) removed it between the `INSERT` and the retry — which is benign: the flow
recovers on its next flush. Model: `CasFirstWrite` checks the compound under every interleaving
(`casfw_guarded` holds; `casfw_unguarded` shows the offset guard is load-bearing; `casfw_reap` keeps
safety under a mid-protocol reap; `casfw_spurious` shows the spurious path is reachable but liveness-only).

The guard is per **key** (per row), which is the right granularity: #732 corruption is per key, keys
are independent, and per-key monotonic durability is exactly what prevents it. The conditional write
is linearizable per partition key (Paxos), so concurrent writers to one key are correctly ordered
without relying on clock synchronisation. Models: `CasCompareAndSet` (`unguarded_gap` shows the
unguarded path corrupts; `guarded_holds` shows the guard holds).

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
`get` reads a null `value` back as `None`. The tombstone is reaped by the TTL, if configured. Keeping
the row also routes the delete through Paxos, avoiding the well-known hazard of mixing lightweight
transactions and regular mutations on the same row. Model: `CasDeleteRevive` — `revive_r1`
(`Tombstone=FALSE`, plain delete) corrupts on delete-then-revive; `revive_r2` (`Tombstone=TRUE`) holds.

## The replay window

A delete and a re-persist are fenced on an offset that, just after recovery, can legitimately trail
the key's own stored snapshot. The partition resumes from the committed offset `C` (the minimum offset
still held across all of its keys); a single slow key can hold `C` well below a fast key's durable
snapshot offset `X` (the offset-lags-state invariant only guarantees `C <= X`). On recovery the
partition's processing offset starts at `C`, while the recovered snapshot's offset is `X`.

If, in this window, the owner issues a write for that key:

- a **time-driven tick** that deletes the key would gate the delete on the processing offset `C` —
  `IF offset <= C` against the stored `X > C` rejects it, **crashing the legitimate owner** with a
  `SnapshotWriteConflict` that reads as if another writer owned the key;
- a **periodic flush** during replay would re-derive the snapshot and try to persist it at the
  replayed offset (`< X`) — the same rejection.

Neither is a safety problem (the durable snapshot stays at `X`), but both are a liveness problem: the
owner fences *itself*. The cause is that the in-memory snapshot buffer's offset *regresses* while
replaying events below `X` (`Snapshot.updateValue` treats the offset-only change as a value change).

The fix keeps the buffered snapshot **monotonic in offset**: `Snapshots.append` drops a lower-offset
append rather than regressing the buffer (sound because, under the determinism the recovery model
already assumes, re-folding events `<= X` reproduces the same state — see below). The buffer therefore
stays at the key's high-water `X`, so

- a delete is fenced on `max(currentOffset, highWater)` — the legitimate owner presents `X` and
  applies, while a genuinely stale writer (which only ever *reached* its own lower offset, never `X`)
  still presents that lower offset and stays fenced;
- a re-derived snapshot is not re-persisted below `X` (the buffer stays `persisted`), so the flush is
  a no-op.

The `offset` extractor is threaded as `Snapshots.of(..., offsetOf = _.offset)` from the
`KafkaSnapshot`-specialised `SnapshotDatabase.snapshotsOf`; other snapshot types and the Kafka backend
get a neutral default, so the change is inert for them. It is also surgical for Cassandra:
`max(currentOffset, highWater)` differs from `currentOffset` only when `highWater > currentOffset`,
i.e. only in this replay window. Model: `ReplayFence` — `Fix=TRUE` holds (safety and liveness);
`Fix=FALSE` violates `INV_NoSelfFence` while `INV_NoStaleApply` still holds, confirming the bug is
liveness-only and the fix never lets a stale writer through.

This fence and the tombstone above are independent mechanisms; their *interaction* over one key's
lifetime is checked by `CasReplayTombstone` (not just argued): with both on, the legitimate owner is
never self-fenced and a zombie never revives; turning off either one violates exactly its own invariant.
The two are complementary — presenting the higher `highWater` for a delete makes the tombstone it writes
*more* protective against a lower-offset revive, never less.

## Equal-offset writes and determinism

`IF offset <= :offset` admits an *equal* offset, so a stale writer holding exactly the stored offset is
not detected. This is deliberate: a snapshot may legitimately be replaced at the same offset (a timer
producing new state without consuming a new record). It is safe under deterministic, replayable folds:
two snapshots at the same offset for a key fold the same events, so they are equal — a same-offset
replacement cannot diverge. The same determinism underpins the replay window above: recovery folds new
events onto the *recovered* base (state at `X`), so re-applying events `<= X` during replay must be a
no-op. Deterministic processing is therefore a precondition of the CAS mode, as it already is of
snapshot recovery generally.

## Consistency

The lightweight transaction reaches consensus at the *serial* consistency level, which is distinct
from the read/write levels in `ConsistencyOverrides` and defaults to `SERIAL` (a cross-datacenter
quorum). `ConsistencyOverrides.write` governs only the transaction's commit phase, not its consensus.
For single-datacenter partition ownership (the common case) set the scassandra client's
`query.serial-consistency = LOCAL_SERIAL` to keep the Paxos rounds in-DC; otherwise every conditional
write and delete pays a cross-datacenter round-trip. Recovery reads at `ConsistencyOverrides.read`
(`QUORUM` by default): #732 recovery happens after the stale writer is gone, so there is no in-flight
Paxos for the read to race, and a serial read is not required.

## TTL and rollout

The `offset` guard lives in the snapshot row, so it expires with the row's TTL. After a row's TTL
lapses the guard is gone and a stale write can land a fresh `INSERT`; this is harmless when the TTL far
exceeds the rebalance/zombie overlap window (the usual case — a zombie outliving the TTL is not
realistic), but the monotonicity guarantee only holds within the TTL.

Enabling on a running system needs no migration (the condition reads the `offset` column every version
already writes). The one rolling-deploy caveat: a lightweight transaction uses a coordinator-generated
write timestamp while a regular write uses a client-side one, so during a mixed deploy an application
clock running ahead of the coordinators can let an old (plain-write) instance's snapshot shadow a newer
conditional one. Negligible with NTP-synced clocks, and gone once every instance writes conditionally.

## Testing

- `SnapshotSpec` (persistence-cassandra-it-tests) — store-level against real Cassandra: monotonic
  writes, stale-write rejection, the tombstone (no resurrection, replayed/stale delete, equal-offset),
  idempotent delete, TTL on both the insert and update paths.
- `FlowSpec` (persistence-cassandra-it-tests) — through the real PartitionFlow / eager-recovery /
  flush-on-revoke machinery: the #732 reproduction asserts corruption under last-write-wins; the
  prevention asserts the stale flush is rejected; and a delete during replay below the recovered
  snapshot offset applies (fenced on the high-water), not rejected.
- `SnapshotsSpec` / `SnapshotReplayFencingSpec` (core) — the monotonic buffer and the delete fence:
  the unit spec directly, the flow spec under a mocked clock (`TestControl`) with an offset-gated
  in-memory store.
- Formal models in `models/`: `CasCompareAndSet` (offset monotonicity), `CasDeleteRevive` (tombstone /
  delete-then-revive), `CasFirstWrite` (the non-atomic first-write race refines the atomic CAS),
  `ReplayFence` (the replay-window fence), and `CasReplayTombstone` (the replay fence and the tombstone
  composed over one key). The models verify behaviour *under* their stated assumptions (below); they do
  not prove those assumptions.

## Assumptions

The models and this design take three things as given:

- **Per-key linearizable compare-and-set.** Each Cassandra lightweight transaction on a row is an atomic,
  linearizable operation (Paxos). The first-write `UPDATE`/`INSERT`/retry compound is *not* atomic — that
  is the one place modelled explicitly (`CasFirstWrite`).
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
is the convergence point for the two backends: a transactional Kafka producer in an
externally-coordinated two-phase commit could bind a Cassandra snapshot write to the same
generation-fenced input-offset commit the Kafka backend already uses, giving Cassandra per-partition
ownership without the per-key compare-and-set. Not actionable now; see the Kafka design doc's
forward-looking note.
