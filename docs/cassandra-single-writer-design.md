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
[Kafka design doc](kafka-single-writer-design.md) covers the failure in full.

## Mechanism: compare-and-set

Cassandra has no transaction to bind the input-offset commit to, the way the Kafka backend does — but
it does offer a conditional write (a Paxos lightweight transaction), so the fence is **per write**. The
stored offset is the per-key [fencing token](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html):
every persist asserts the stored offset is not greater than the one being written, so the
newest-by-offset write wins, whoever issued it. The write is linearizable per partition key, so
concurrent writers to one key are ordered without relying on clock synchronisation:

```sql
UPDATE snapshots_v2 SET ... , offset = :offset WHERE <key> IF offset <= :offset
```

A key's first write finds no row, so the `UPDATE` does not apply and it falls back to
`INSERT ... IF NOT EXISTS` (retried once via the `UPDATE` if it loses an insert race). This is the one
non-atomic path — a compound of separate Paxos transactions — but it is safe by construction: both
`UPDATE`s are offset-gated and the `INSERT` only writes an absent cell, so no interleaving overwrites a
newer snapshot. A rejected write raises `SnapshotWriteConflict` — including a spurious one if a delete
slips between the first-write `INSERT` and its retry (an over-rejection, never corruption, cleared on
the next flush).

The guard is per **key**, the right granularity: #732 corruption is per key and keys are independent,
so per-key monotonic durability is exactly what prevents it.

## Scope: persist-only — and fencing a deletion without gating delete

This mode fences **persists** only. A delete is an ordinary last-write-wins `DELETE`; the
`IF offset <= :offset` guard does not apply to it. The residual gap, for anyone who hard-deletes: a
lagging zombie can resurrect a just-deleted key by inserting at a lower offset, and a later recovery then
folds new events onto that revived base — #732 for that one key.

Fencing a deletion, though, needs no gated `delete`. The obvious way to gate one — a logical *tombstone*
that keeps the row and its `offset` instead of removing it — is just a persisted null:

```sql
-- gating delete (the rejected route): a value-less logical tombstone
UPDATE snapshots_v2 SET value = null,   offset = :offset WHERE <key> IF offset <= :offset
-- the equivalent that already works: persist an empty state
UPDATE snapshots_v2 SET value = :empty, offset = :offset WHERE <key> IF offset <= :offset
```

It is the same gated write, and the row survives until its TTL either way — the guard has to outlive the
rebalance overlap window, so neither reclaims sooner. So model a deletion as an offset-carrying *empty*
snapshot and persist it through the already-fenced persist path: a lower-offset zombie write is rejected
exactly as it is for any persist. Routing the same thing through `delete` instead would cost on two
fronts to buy nothing.

**A breaking API.** The offset has to reach the store through `SnapshotWriteDatabase.delete`, forcing a
source/binary-breaking `delete(key, offset)` signature and offset plumbing through every delete path.
Persisting an empty snapshot makes **no public API change** and needs no major-version bump.

**A replay-window livelock.** A value-less tombstone recovers with no offset *floor*. A partition resumes
from the committed offset `C` (the minimum still held across its keys), which a slow key can hold well
below a fast key's durable snapshot offset `X`; the buffer then climbs from the replayed offsets (all
`< X`), a tick-delete or flush mid-replay writes below `X`, and `IF offset <= X` rejects the **legitimate
owner**, which tears down, re-recovers the same floorless tombstone, and loops — safety holds (`X` never
regresses), but it is a pure **liveness** failure. Closing it needs a snapshot buffer kept *monotonic in
offset* with a floor seeded from the tombstone's offset (a dedicated recovery type), plus the same floor
on the journal (events-recovery) path. A persisted snapshot has all of this already: `SnapshotFold`
deduplicates replayed records by offset (`record.offset > snapshot.offset`), so a key recovered at `X`
drops events `<= X` before the fold and never re-derives a snapshot below `X` to persist — the floor for
free.

Gating `delete` would therefore rebuild, behind a breaking API, the offset floor the persist path already
gives — which is why offset-gated deletes are **unnecessary**, not merely deferred (see *Rejected
alternatives*). The plain last-write-wins `delete` stays for callers who want a real row removal and have
no need to fence it.

## Equal-offset writes and determinism

`IF offset <= :offset` admits an *equal* offset, so a stale writer holding exactly the stored offset is
not detected. This is deliberate. The legitimate owner can act at an offset it has already stored — a
re-persist of the high-water `X` equals the stored offset, and a strict `<` would reject it and fence the
owner against itself. Admitting equal is safe not because the new value is identical but because a
same-offset write does not move the recovery point — unlike a lower-offset write it cannot drop committed
events (#732). The *records* folded into any two snapshots at the same offset are the same, so a
same-offset re-persist differs at most in time-driven tick state, never in event data. Deterministic,
replayable folds are therefore a precondition of the compare-and-set mode (as they already are of
recovery generally).

## Consistency

A lightweight transaction has two consistency levels. The *serial* level (`SERIAL` by default — a
quorum across all replicas, which spans datacenters under multi-DC replication) runs the Paxos
consensus; `ConsistencyOverrides.write` governs only the commit that materialises the agreed value. So
the serial level decides *agreement*, not *visibility*: the value becomes readable to a normal read
only once that commit lands at the write level. Recovery uses a non-serial read
(`ConsistencyOverrides.read`), so it sees the latest committed snapshot only when **`R + W > N`** on the
regular levels — the write side must be a quorum, not just the serial consensus. (A non-serial read can
also miss an *accepted-but-not-yet-committed* LWT left by a failed coordinator — but that coincides with
an incomplete `persist`, which never held its offset, so recovery just re-folds those events. A serial
read would avoid both cases; the design forgoes that cost.)

So configure `ConsistencyOverrides` read **and** write to a quorum — `LOCAL_QUORUM` for single-DC,
paired with the scassandra client's `query.serial-consistency = LOCAL_SERIAL` to keep Paxos in-DC
(otherwise each conditional write pays a cross-DC round-trip; a plain delete is unaffected). This is
**not** a default: `ConsistencyOverrides` is empty unless you set it, so the table inherits the session
default (often `LOCAL_ONE`), which reintroduces #732 on the read side — the write-side fence does not
heal that. What matters is access locality (a key's writers and its recovery read in one DC), not the
replication footprint, which may still span DCs for DR; ownership that fails over between DCs needs the
cross-DC `SERIAL` / `QUORUM` levels.

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

## Implementation

Entry point: `CassandraSnapshots.withSchema(compareAndSet = true)` (or
`CassandraPersistence.withSchema(snapshotCompareAndSet = true)`). `persistCompareAndSet` issues the
offset-gated `UPDATE` with the `INSERT ... IF NOT EXISTS` first-write fallback, and `resolveConditional`
classifies the result; `delete` stays an unguarded last-write-wins `DELETE`. `WriteMode.CompareAndSet`
carries the extra `INSERT` statement, so it exists exactly in compare-and-set mode.

## Testing

- Store-level against real Cassandra (`SnapshotSpec`, persistence-cassandra-it-tests) — monotonic
  persists applied (including an equal-offset re-persist), stale-write rejection, TTL on both the insert
  and update paths, and concurrent first-writers racing on a fresh key (the highest offset wins, no
  corruption — exercising the first-write retry path). The persist-only delete gap is pinned by a
  characterization test: a lower-offset write after a delete resurrects the key — the residual of the
  unfenced last-write-wins delete (a caller who needs the deletion fenced persists an empty snapshot
  instead; see *Scope*).
- Through the real PartitionFlow / eager-recovery / flush-on-revoke machinery (`FlowSpec`,
  persistence-cassandra-it-tests) — the #732 reproduction asserts corruption under last-write-wins;
  the prevention asserts the stale flush is rejected.

## Assumptions

This design takes three things as given:

- **Per-key linearizable compare-and-set.** Each Cassandra lightweight transaction on a row is an atomic,
  linearizable operation (Paxos). The first-write `UPDATE`/`INSERT`/retry compound is *not* atomic.
- **Deterministic, replayable folds (a user contract).** Re-folding a recovered base over the same events
  reproduces the same state. This is what makes equal-offset writes idempotent. A non-deterministic fold
  breaks it.
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
- **Offset-gated deletes (logical tombstone)**: gate `delete` like `persist` by keeping a value-less
  tombstone (`SET value = null`) that carries the offset, closing the resurrection gap. Rejected as
  redundant — a tombstone is a persisted null, so the same fencing comes from persisting an
  offset-carrying *empty* snapshot through the already-fenced persist path, without a breaking
  `delete(key, offset)` API and without the replay-window livelock a value-less tombstone reintroduces
  (see *Scope*).

## Forward-looking

- **Per-partition ownership** — the equal-offset gap and per-key (rather than per-partition) granularity
  could be closed by a composite `(offset, generation)` token (see Rejected alternatives) or, further out, by
  [KIP-939 (participation in 2PC)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-939:+Support+Participation+in+2PC):
  a transactional producer in an externally-coordinated two-phase commit could bind the Cassandra
  snapshot write to a generation-fenced Kafka input-offset commit, giving Cassandra per-partition
  ownership without the per-key compare-and-set. Not actionable now; see the Kafka design doc's
  forward-looking note.
