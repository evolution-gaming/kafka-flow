---
id: cassandra-single-writer-design
title: "Cassandra single-writer design: persist only"
sidebar_label: "Cassandra single-writer design: persist only"
---

Design notes for the compare-and-set snapshot mode of `kafka-flow-persistence-cassandra`
(`CassandraSnapshots.withSchema(compareAndSet = true)`). What ships is small — one conditional
predicate on each `persist` — so these notes spend little time on mechanics. Their weight is on the
design choices: where the persist-only line is drawn and why, and the full solution (offset-gated
deletes) that was designed and then deferred — because the reasons it was deferred are
the main thing a future author needs before reopening it.

Operational guidance (enabling, consistency levels, TTL, rollout) is in
[Persistence](persistence.md). The Kafka backend solves the same problem with a different mechanism —
see [Kafka single-writer design](kafka-single-writer-design.md).

## Problem

[kafka-flow#732](https://github.com/evolution-gaming/kafka-flow/issues/732): consumer-group ownership
of the input topic does not extend to the snapshot store. During a rebalance a previous owner that has
not yet observed the revocation keeps flushing snapshots alongside the new owner; the snapshots table
is last-write-wins, so a stale snapshot can overwrite a newer one, and the next recovery loads stale
state while resuming from the committed offset — the events in between are lost from the state. The
[Kafka design doc](kafka-single-writer-design.md) covers the failure in full, with a diagram.

Kafka's fix does not transfer. There, the snapshot write is bound to the input-offset commit in one
broker transaction, and the consumer generation — authoritative for ownership — fences the commit.
Cassandra offers no transaction to bind a Kafka offset commit into, and holds no notion of who owns a
partition. What it does offer is a conditional write: a lightweight transaction (LWT), linearizable
per partition key. So the fence must be built from something the snapshot itself carries, and checked
per write, per key.

## Design

The stored offset is the per-key
[fencing token](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html). Every
snapshot already carries the offset it was folded up to, and #732 corruption is stale *by offset* — a
lower-offset snapshot replacing a higher-offset one. So each persist asserts the stored offset is not
greater than the one being written, and the newest-by-offset snapshot wins, whoever writes it:

```sql
UPDATE snapshots_v2 SET ..., offset = :offset WHERE <key> IF offset <= :offset
```

A key's first persist finds no row, so the `UPDATE` does not apply and falls back to
`INSERT ... IF NOT EXISTS`, retried once through the gated `UPDATE` if it loses an insert race. This
compound of separate LWTs is the one non-atomic path, and it is safe by construction: both `UPDATE`s
are offset-gated and the `INSERT` only writes an absent cell, so no interleaving overwrites a newer
snapshot. Its worst case is an over-rejection, never corruption: a delete slipping between the
`INSERT` and its retry surfaces as a spurious conflict, cleared on the next flush. A rejected write
raises `SnapshotWriteConflict`.

Three choices in the predicate carry most of the design:

- **Per key, not per partition.** #732 corruption is per key and keys are independent, so per-key
  monotonic durability is exactly the guarantee needed. Anything per-partition would require an
  ownership authority Cassandra does not have (see *Rejected alternatives* for what importing one
  would cost).
- **`<=`, not `<`.** The legitimate owner can write at an offset it has already stored — a
  timer-driven re-flush of the buffered high-water snapshot — and a strict `<` would fence the owner
  against itself. Admitting an equal offset is safe because a same-offset write cannot move the
  recovery point, so it cannot drop committed events. It also means a stale writer holding *exactly*
  the stored offset goes undetected — accepted for the same reason.
- **Ordered by data, not by identity.** The token orders writes by what they contain, not by who sent
  them, so the fence needs no clock synchronisation, no lease and no liveness protocol — but it can
  never say "you are not the owner", only "you are behind". Closing that gap needs a generation in
  the token (see *Rejected alternatives* and *Forward-looking*).

Two snapshots at the same offset have folded the same records, so admitting equal offsets is sound
only when folds are deterministic and replayable — already a precondition of recovery generally, but
this mode leans on it harder.

### Why the store is the whole change

The mode deliberately changes nothing outside `CassandraSnapshots`: no buffer change, no recovery
change, no new read. That is possible because core already guarantees the legitimate owner never
presents an offset below what it recovered: `SnapshotFold` drops replayed records at or below the
recovered snapshot's offset before they reach the fold, so nothing is re-derived — or re-persisted —
below a key's recovered high-water even while the partition replays from a lower committed offset.
The fence therefore only ever rejects genuinely stale writers, and liveness comes for free.

That property is load-bearing, not incidental. The deferred design below is the story of what happens
when it stops holding: fencing deletes breaks it, and repairing it pulls the buffer and both recovery
paths into the change.

## Scope: deletes are not fenced

`delete` stays a plain last-write-wins row `DELETE`. During an ownership overlap a stale writer can
therefore still erase a newer snapshot, or resurrect a just-deleted key by writing at a lower
offset — #732 residual for exactly the keys a fold deletes concurrently with re-writes.

Drawing the line here is a choice, not an oversight:

- **A fenced deletion is still expressible.** Instead of folding to `None`, fold to an
  offset-carrying *empty* state (`Some(empty)`): the deletion then travels the fenced persist path
  and a lower-offset zombie is rejected exactly as for any persist. The store's row lives until its
  TTL — the cost of the workaround.
- **Fencing `delete` itself costs out of all proportion to the predicate above** — a breaking API and
  new recovery machinery, with a liveness trap on each recovery path. That is the deferred design
  below.

## The expired guard (TTL reconfiguration)

Cassandra TTLs are per **cell** and fixed at write time, and a row stays visible while any live cell —
or the first write's `INSERT` row marker (immortal only when that first write ran without a `ttl` and
the table has no `default_time_to_live`) — survives. Under a uniform `ttl` from the first deployment
it is unreachable: the delete co-writes `offset` with `value`, so a visible row always carries a live
guard. It is a **reconfiguration / pre-TTL-legacy** hazard — enabling (or shortening) the `ttl` between writes of a key can leave a
visible row whose `offset` guard cell has expired while an earlier, longer-lived cell or marker
survives: e.g. a no-TTL deployment's first write (immortal marker), TTL'd re-persists, then idleness
past the TTL — every column null, nothing to fence. `get` already reads such a row as absent (its `value` cell is expired)
and a plain delete removes it entirely, but no regular persist could ever claim it: the null guard
fails `IF offset <= :offset`, the not-applied result looks like "row absent" (it is not — Cassandra
returns the condition column, null, exactly when the row exists), `INSERT ... IF NOT EXISTS` loses to
the still-visible row, and the retry conflicts — every write of the key raising
`SnapshotWriteConflict`, forever. So the write path distinguishes the guard-expired result and repairs
it with a Paxos-safe `IF offset = null` persist that reinstates the guard
(`Statements.prepareRepairPersist`; racing repairs serialize, the loser conflicts as usual). The
repair re-arms the guard but (being an `UPDATE`) cannot remove an immortal marker, so such a row
re-poisons after each `ttl` until an owner deletes/reaps it — palliative, not curative. `SnapshotTtlEdgeSpec` pins the state and the repair against real
Cassandra. Prefer configuring the `ttl` from the first deployment anyway.

## Compatibility

- **No public API change**, hence no major version: the mode is an opt-in flag
  (`compareAndSet = true`, default `false`) on the existing entry points.
- **No schema migration in either direction**: the condition reads the `offset` column every version
  already writes. Rolling deploys are safe; the mixed-mode clock-skew caveat is in
  [Persistence](persistence.md).
- **The fence is write-side only.** Recovery reads at the regular (non-serial) consistency level, so
  the guarantee reaches the reader only when `R + W > N` — with weaker levels a recovery read can
  miss the newest committed snapshot and reintroduce #732 on the read side. Required levels and the
  serial-consistency setting are in [Persistence](persistence.md).

## Testing

Store-level tests against real Cassandra (`SnapshotSpec`) cover monotonic persists, stale-write
rejection, equal-offset re-persist, TTL on both write paths, and concurrent first-writers racing a
fresh key (exercising the insert-retry compound). Flow-level tests (`FlowSpec`) run through the real
recovery and flush machinery: the #732 reproduction shows corruption under last-write-wins, and its
counterpart shows the stale flush rejected under compare-and-set.

## The full solution, and why it was deferred

Persist-only was carved out of a full design that also fenced deletes. That full design was
implemented and reviewed, and none of it ships. It is summarised here because its trajectory is
the main learning of this work: **a fence that looked like one more predicate turned into a chain of
core changes, each forced by the previous one.**

1. **A fenced delete cannot remove the row.** The offset guard *is* the row: remove it and a lagging
   zombie's `INSERT ... IF NOT EXISTS` at a lower offset succeeds, resurrecting a stale snapshot. So
   a fenced delete must be a logical **tombstone** — the row kept, value nulled, offset gated on the
   same predicate as a persist (`SET value = null ... IF offset <= :offset`).

2. **The tombstone forces a breaking API.** The gate needs the delete's offset, but
   `SnapshotWriteDatabase.delete(key)` does not carry one — fencing deletes means a source- and
   binary-breaking `delete(key, offset)` through every delete path and every custom store. A major
   version, for this alone.

3. **The buffer must become monotonic, or the owner fences itself.** After recovery a key can hold a
   durable snapshot at offset `X` while the partition replays from a lower committed offset `C` (a
   slow *other* key held `C` back). A timer-driven delete in that window would be gated on the
   processing offset `< X` and rejected — the fence crashing the *legitimate* owner. The fix keeps
   the per-key buffer monotonic in offset and gates a delete on the key's high-water `X`, which the
   true owner presents and a genuinely stale writer never reached. Note the persist case needs no
   such fix (`SnapshotFold` filters replayed records); it is the timer-driven *delete*, which
   bypasses that filter, that forces the buffer change.

4. **Recovery must learn to see tombstones, or the owner livelocks.** The base read is `get`, typed
   `Option[S]`: a value or nothing. A tombstone comes back as `None`, indistinguishable from a
   never-written key — so recovery seeds no high-water, the buffer climbs from replayed offsets
   `< X`, and the offset-`X` tombstone rejects the owner's own flush. The flow tears down,
   re-recovers the same floorless tombstone, and repeats:

   ```mermaid
   sequenceDiagram
       participant O as Owner (recovering)
       participant DB as snapshots table
       Note over DB: tombstone: value = null, offset = X
       O->>DB: recover: get(key) → None (offset X invisible)
       O->>O: replay events from committed offset C < X
       O->>DB: flush re-derived snapshot at offset < X
       DB-->>O: IF offset <= X fails: SnapshotWriteConflict
       Note over O: tear down, re-recover the same None,<br/>repeat — livelock
   ```

   The fix is a new recovery read that surfaces `Deleted(offset)` where `get` says `None`, seeding
   the buffer's floor.

5. **Events-recovery re-opens the livelock on its own.** `restoreEvents` rebuilds state by folding
   the *journal*, not by reading the snapshot store — and a delete clears the journal, so the fold
   yields nothing and the floor is lost again, untouched by the fix above. Events-recovery must read
   the snapshot store purely for the tombstone floor before folding. A delete fence that seeds the
   floor only on snapshot recovery still livelocks here; the two seedings are independent.

**What the analysis showed.** Safety was never at risk: a rejected write changes nothing and the
durable offset never regresses, in every configuration. Every defect was a **liveness** failure — the
self-fence and the livelocks above — and each fix proved necessary: with the monotonic buffer or the
tombstone floor removed, liveness fails while safety still holds, reached through a live snapshot in
one configuration and through a deleted key in another. With both in place, a single writer's durable
offsets stay monotonic for both safety and liveness.

**The deferral decision.** On the benefit side, fencing `delete` protects only folds that return
`None` — a need already coverable by the empty-state persist above. On the cost side: a major version
for every custom snapshot store; invasive changes to the buffer and both recovery paths — the hottest
code in core; and a liveness surface subtle enough to be a maintenance risk in its own right, not
just implementation effort. Timing sharpened the call:
[KIP-939](https://cwiki.apache.org/confluence/display/KAFKA/KIP-939:+Support+Participation+in+2PC)
(participation in 2PC) may eventually let a Cassandra snapshot write bind to a generation-fenced
Kafka offset commit, giving per-partition ownership that supersedes a per-key delete fence — making
the deferred machinery potentially throwaway. So the full design was parked where it can be resumed,
and the persist-only subset shipped.

The learning generalises: in a last-write-wins store, fencing *writes* is cheap — the token rides the
data. It is *deletion* that is expensive, because deletion removes the very cell that carries the
fence, and every fix for that (tombstones, floors, monotonic buffers) leaks upward into recovery.

## Rejected alternatives

- **Offset-as-write-timestamp (LWW register)**: write each snapshot `USING TIMESTAMP <offset>` and
  let Cassandra's own last-write-wins reconciliation keep the highest-offset cell — a plain quorum
  write, much cheaper than Paxos, and a delete becomes a tombstone ordered by offset for free.
  Rejected: equal-offset replacement breaks (at equal timestamps Cassandra breaks ties by value, not
  write order), a rolling deploy inverts catastrophically (old instances' wall-clock timestamps
  dominate every offset-as-timestamp value), and it discards the real write timestamps.
- **Lease / ownership table**: a per-partition lease acquired with one LWT, then cheap plain writes.
  The lease alone does not stop a paused leaseholder's in-flight plain writes (last-write-wins still
  applies), so a per-write fencing token is still required — at which point the lease adds only
  liveness and expiry concerns on top of the per-write CAS.
- **Composite `(offset, generation)` token**: gate on the consumer generation as well as the offset,
  closing the equal-offset gap and giving per-partition ownership. Rejected as the default because it
  couples the self-contained Cassandra module to the live consumer generation; reasonable as a future
  strict mode.
- **Recovery-side reconciliation** (store the offset, reconcile on read): does not prevent the stale
  overwrite — last-write-wins still corrupts the store — so strictly weaker than fencing the write.

## Forward-looking

- **Offset-gated deletes** — the deferred design above; a candidate for a future major
  version if the empty-state workaround proves insufficient in practice.
- **Per-partition ownership** — a composite `(offset, generation)` strict mode, or, further out,
  [KIP-939](https://cwiki.apache.org/confluence/display/KAFKA/KIP-939:+Support+Participation+in+2PC):
  an externally-coordinated two-phase commit binding the Cassandra snapshot write to a
  generation-fenced Kafka offset commit — per-partition ownership without per-key CAS. Not actionable
  now; see the Kafka design doc's forward-looking note.
