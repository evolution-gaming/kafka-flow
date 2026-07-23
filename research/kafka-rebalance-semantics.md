# Study: consumer-group rebalance semantics (primary-source verification)

*Narrative (supporting). Kafka arm of the #732 single-writer study — primary-source pin of rebalance mechanics: classic-protocol verdicts, the KIP-848 addendum, and the `group.protocol=consumer` guide. Corpus index: [`README.md`](README.md).*

The fencing arguments in the Kafka single-writer design lean on precise claims about kafka-clients
rebalance mechanics — where the generation moves, when each callback fires, and what the broker
validates. Those claims had been re-derived from memory more than once during editing, which risks
drift. This study pins each one against primary sources, once, so future doc/comment edits cite this
record instead of re-deriving.

Method: deep-research fan-out — 5 search angles, 20 primary sources fetched (apache/kafka trunk and
branch 3.9, tags 2.5.0/3.9.0, published 2.4/3.7/3.9 javadoc, KIPs 266/429/447/848), 93 claims
extracted, 25 surviving 3-vote adversarial verification (3 killed on wording or source quality),
synthesized to the verdicts below. Line numbers cite trunk (fetched 2026-07-06); verifiers found the
cited structure identical between branch 3.9 and trunk for every classic-protocol assertion.

## Verdicts

| # | Claim | Verdict | Evidence |
|---|---|---|---|
| 1 | Classic protocol: join, sync, generation adoption and all listener callbacks run on the application thread inside `poll()`; the heartbeat thread never advances the generation and never invokes callbacks | **CONFIRMED, one qualification** | `AbstractCoordinator.joinGroupIfNeeded` invokes `onJoinPrepare`/`onJoinComplete` only from the poll path; the heartbeat thread is explicitly fenced off during the join. Qualification: the heartbeat thread CAN *reset* the internal generation to `NO_GENERATION` (−1/empty) on `ILLEGAL_GENERATION`/`UNKNOWN_MEMBER_ID`/session timeout — a reset, never an advance, and it never touches the public `groupMetadata()`. Non-poll callback sites: `close(Duration)` and `unsubscribe()` (via `onLeavePrepare`, still the user thread; lost instead of revoked when the generation is reset). |
| 2 | Public `groupMetadata()` is assigned in exactly two places: constructor (−1/empty) and `onJoinComplete` | **CONFIRMED** (3-0) | Field is private to `ConsumerCoordinator`; constructor (line ~244) and `onJoinComplete` (line ~395). `resetStateAndGeneration` resets only `AbstractCoordinator`'s internal generation. Consequence: −1 is reported only until the first join completes; after falling out, the client keeps reporting the last joined generation. |
| 3 | Eager ordering: revoked fires in `onJoinPrepare`, before the JoinGroup round, with the still-current generation — a commit inside the callback is accepted | **CONFIRMED, one qualification** (3-0) | Revoked fires for ALL owned partitions strictly before `initiateJoinGroup`, with the previous generation passed in; an in-source comment keeps the assignment accessible "to commit offsets etc.". Qualification: acceptance holds for a *graceful* rebalance — client-side ordering cannot prevent a broker-side generation bump racing the commit. If the heartbeat already reset the generation, the lost branch runs instead, so whenever eager revoked fires the generation is valid. |
| 4 | Cooperative ordering in `onJoinComplete`: generation stamped into `groupMetadata` BEFORE the revoked callback | **CONFIRMED** (3-0) | Exact order: (1) stamp generation+memberId (line ~395); (2) cooperative branch computes revoked = owned − assigned and, only if non-empty, invokes revoked (line ~446); (3) `requestRejoin` inside that guard; (4) `assignor.onAssignment`; (5) `assignFromSubscribed`; (6) `invokePartitionsAssigned(addedPartitions)` UNCONDITIONALLY (line ~469). Same structure on 3.9. So the cooperative revoke callback always runs with the member already on the new generation — still inside the poll, on the app thread. |
| 5 | Cooperative revoke-time commit carrying the captured (previous) generation is always fenced; one reading `groupMetadata` live would carry the new generation and be ACCEPTED, because the broker validates member+generation, never partition ownership | **CONFIRMED for the classic coordinator** (3-0) | `validateOffsetCommit(group, generationId, memberId, groupInstanceId, isTransactional)` — no partition argument; `doCommitOffsets`/`doTxnCommitOffsets` pass the offsets map to `storeOffsets` with no ownership filtering (assignments stored as opaque bytes). The KRaft-native `ConsumerGroup` validator was NOT verified (the one supporting source was a mailing-list mirror, killed 1-2); trunk `ClassicGroup.validateOffsetCommit` independently confirmed to preserve classic semantics for classic-type groups in 4.x. |
| 6 | The −1/empty sentinel skips validation on the classic path | **CONFIRMED, and stronger than we claimed** (3-0) | For non-transactional commits the sentinel is accepted only when the group is Empty; for TRANSACTIONAL commits the `UNKNOWN_MEMBER_ID` rejection is gated on `!isTransactional` — the sentinel lands unfenced even against a NON-empty group ("older versions of this protocol do not require memberId and generationId"). The gate survives into the 4.x Java coordinator for classic-type groups. Gate shape: the classic skip keys on any `generationId < 0` with the member id unset — the whole negative family, not only −1; the consumer-type gate is `== −1` exactly (full span table: [`external-semantics.md`](external-semantics.md) ext(K3)). This strengthens the publish-guard rationale in `Consumer.publish`. |
| 7 | `onPartitionsAssigned` fires on every completed rebalance, empty delta included; revoked/lost fire only non-empty | **HALF CONFIRMED / HALF REFUTED** (3-0) | Assigned: unconditional in `onJoinComplete`, javadoc-guaranteed since 2.4/KIP-429 ("always be triggered exactly once ... even if there is no newly assigned partitions ... with an empty collection"), wording unchanged through 3.9. Revoked: the EAGER branch of `onJoinPrepare` has NO `isEmpty` guard — an already-joined eager member owning zero partitions gets an empty revoked callback on rejoin (not on first join, where generation −1 routes to the guarded lost branch). Non-empty holds only for cooperative revoked and all lost paths. The blanket "revoked/lost are empty-gated" in this study's earlier wording was wrong; corrected. |
| 8 | KIP-266: the join round can span multiple `poll()` calls; `groupMetadata()` reflects the last COMPLETED join throughout | **CONFIRMED, medium confidence** | `onJoinPrepare` takes a Timer and returns boolean; `joinGroupIfNeeded` returns `false` mid-rebalance and resumes on a later poll (KAFKA-13310 structure, 3.2+). Combined with verdict 2, `groupMetadata()` cannot observe an incomplete join. Inferred from the verified return-false/resume structure rather than end-to-end timeout tracing. |
| 9 | KIP-848 (`group.protocol=consumer`): epoch advances on the background thread; callbacks still on the app thread; sentinel handled differently; KIP-447 support unclear | **LARGELY UNVERIFIED at first pass — since pinned** | Only the callback-threading half survived the first pass (source quality). A dedicated second pass — decompiled client bytecode plus broker/coordinator primary sources — has since pinned every question this row left open; see the KIP-848 addendum below. |

**Runtime pin of verdicts 3–5 (2026-07-12).** The composed consequence of the source-verified ordering
— the revoke-time transactional flush is **always fenced** under classic cooperative-sticky (the
generation is stamped before the revoked callback, so the flush carries the held token and its
transaction aborts) and **commits** under classic eager-sticky (revoked fires before the join round) —
is now IT-confirmed against a real broker by `RevokeTimeFlushSpec` (persistence-kafka-it-tests, routine
suite): a second member joins, the first member's `onPartitionsRevoked` runs the production-shaped
flush with the token the production path reads, and the test additionally observes the mechanism, not
only the outcome — inside the cooperative callback the live `groupMetadata()` generation is already
past the joined one while the flush's token is not (verdict 4 end to end); inside the eager callback
the generation is unmoved (verdict 3). The eager case is the paired control: same machinery, same held
token, opposite outcome — the fence is caused by the generation-adoption ordering. This closes the gap
that verdicts 3–5's consequence was pinned at source level only; before this spec, the fenced/landed
outcomes were exercised solely with a *simulated* stale generation (`generationId − 1`).

## KIP-848 addendum (second pass)

Verdict 9 left KIP-848 unpinned. This pass closed it with two independent tracks (2026-07-07):

- **Client**: direct decompilation (`javap -c -p`) of **kafka-clients 4.3.0** — the jar skafka 20.2.0
  resolves — enumerating every write path of the public `groupMetadata()` and the full
  `TransactionManager` error switch. Direct bytecode evidence, not web-sourced.
- **Broker/ecosystem**: deep-research fan-out — 5 angles, 17 primary sources (apache/kafka
  group-coordinator commit diffs, PRs #17914/#14845/#16145, KAFKA-18060, the KIP-848 text), 81 claims,
  25 sent to 3-vote adversarial panels: 17 confirmed 3-0, 7 refuted (all describing the pre-4.0 state
  of the sentinel path as if current), 1 lost to verifier infrastructure (listed under residuals).

### Client (kafka-clients 4.3.0, decompiled)

| # | Claim | Verdict | Evidence |
|---|---|---|---|
| C1 | Public `groupMetadata()` can advance BETWEEN `poll()` calls — and even mid-callback | **CONFIRMED** (bytecode) | `AsyncKafkaConsumer.groupMetadata` is an `AtomicReference` read; the writer chain is `ConsumerMembershipManager.onHeartbeatSuccess` (background network thread) → `updateMemberEpoch` → `notifyEpochChange` → `MemberStateListener.onMemberEpochUpdated` → `updateGroupMetadata`. Heartbeating continues during RECONCILING, so the value can also move while `onPartitionsRevoked` is running on the app thread. The classic poll-boundary reasoning (verdicts 1–2) does not carry over. |
| C2 | An epoch bump with UNCHANGED assignment is adopted silently — no rebalance-listener callback of any kind | **CONFIRMED** (bytecode) | In `onHeartbeatSuccess`, `updateMemberEpoch(data.memberEpoch())` runs unconditionally before the assignment null-check; a null (unchanged) assignment then returns immediately. `notifyEpochChange` touches only `MemberStateListener`s, never the `ConsumerRebalanceListener` and never a state transition. The empty-delta callback (and with it skafka issue 581's trigger) does not exist under KIP-848 — no callback observes such a bump; only a `groupMetadata()` *read* does (post-poll, as the design does, or a live read — the value is a thread-safe `AtomicReference` here), and even that cannot close the read-to-commit window (C1). |
| C3 | Rebalance callbacks run on the application thread; reconciliation cannot finalize until the callback completes | **CONFIRMED** (bytecode) | Background→app handoff via `PartitionsRemovedEvent`/`PartitionsAssignedEvent`, drained in `poll()`, `updateAssignmentMetadataIfNeeded`, and `unsubscribe()`; `close()` invokes revoked/lost directly on the closing thread. `AbstractMembershipManager.revokeAndAssign` only sets `currentAssignment` and transitions to ACKNOWLEDGING in the completion of the revocation future, which is completed by the callback-completed event from the app thread — the teardown-inside-callback coupling survives. |
| C4 | A stale-epoch **transactional** commit surfaces to the producer as `ILLEGAL_GENERATION` → abortable `CommitFailedException` — a graceful abort, **same as classic**, not the fatal error an earlier reading of this row claimed | **CORRECTED (source-verified)** | The client switch is as the bytecode shows — `TxnOffsetCommitHandler.handleResponse` maps `UNKNOWN_MEMBER_ID`/`ILLEGAL_GENERATION` → `abortableError(CommitFailedException)`, has no `STALE_MEMBER_EPOCH` case, and would send an unmapped code to the fatal default. **But that default is unreachable on the txn path:** the broker never puts `STALE_MEMBER_EPOCH` on a `TxnOffsetCommitResponse` — `OffsetMetadataManager.validateTransactionalOffsetCommit` (4.0.0 lines 360-370, identical 4.1.0) wraps `group.validateOffsetCommit(..., isTransactional=true, ...)` in `catch (StaleMemberEpochException ex) { throw Errors.ILLEGAL_GENERATION.exception(); }` (the API has no `STALE_MEMBER_EPOCH` code — "to preserve backward compatibility"). So a KIP-848 member's stale transactional commit is thrown as `StaleMemberEpochException`, caught, and re-emitted as `ILLEGAL_GENERATION`, which the producer aborts gracefully. The prior "fatal/harsher" verdict was a true-but-vacuous bytecode observation (the fatal branch exists but nothing reaches it here); corrected. |
| C5 | The leave/fence sentinels (−1/−2/0) cannot reach the public `groupMetadata()`; after fencing or leaving it freezes at the last positive epoch; only `unsubscribe()` resets to −1 | **CONFIRMED** (bytecode) | `updateMemberEpoch` fires `notifyEpochChange(Optional.empty())` for any epoch ≤ 0, and `updateGroupMetadata` no-ops on an empty optional. `resetGroupMetadata()` (back to −1/empty) is called only from `unsubscribe()`. So −1 is visible only before the first heartbeat — the classic verdict-2 consequence has a KIP-848 analogue: the token can be stale, never a leave sentinel. |

### Broker (Kafka 4.x KRaft group coordinator, 3-0 adversarial)

| # | Claim | Verdict | Evidence |
|---|---|---|---|
| B1 | `ConsumerGroup#validateOffsetCommit` checks member existence and member epoch ONLY — no per-partition ownership validation, despite the coordinator holding server-side assignments (**≤4.2.0**; see the version note in the residuals) | **CONFIRMED** (3-0) | Signature: `validateOffsetCommit(String memberId, String groupInstanceId, int memberEpoch, boolean isTransactional, short apiVersion)` — no topic/partition parameters (PR #16145 diff). Body: member lookup (`UnknownMemberIdException` if absent — a fenced member is removed from the group, so its commits fail there) then `validateMemberEpoch`. KIP-848 text agrees: "The MemberId and the GenerationIdOrMemberEpoch are verified. STALE_MEMBER_EPOCH or UNKNOWN_MEMBER_ID is returned accordingly." **Version bound: from 4.3.0, KIP-1251 adds a per-partition assignment-epoch check on the *lagging* path** (`createAssignmentEpochValidator`) — so "no per-partition check" holds only ≤4.2.0. |
| B2 | Epoch validation is EXACT equality; any mismatch (stale or ahead) is rejected — `StaleMemberEpochException` for 848-protocol members, `IllegalGenerationException` for classic-protocol members of the same group (**≤4.2.0**) | **CONFIRMED** (3-0) | `if (receivedMemberEpoch != expectedMemberEpoch) { if (useClassicProtocol) throw new IllegalGenerationException(...) else throw new StaleMemberEpochException(...) }`. The fail direction for a lagging token is fenced, never accepted. **From 4.3.0 (KIP-1251) equality is relaxed on the lagging side only**: equal → accept, *ahead* → still fenced, *lagging* → per-partition check (accept if the partition is still assigned to the member, else fence). Safety holds on both sides of the boundary — a commit for a partition the member no longer owns is fenced either way. |
| B3 | The pre-KIP-447-shaped transactional commit (epoch −1 + empty memberId + null groupInstanceId) is accepted UNFENCED by consumer-type groups in 4.0 GA; identity fields are validated whenever any is provided | **CONFIRMED** (3-0) | Initially the new coordinator rejected it (`UnknownMemberIdException`) — KAFKA-18060, a 4.0 Blocker; PR #17914 (merged 2024-12-03) added the early return `if (isTransactional && memberEpoch == UNKNOWN_GENERATION_ID && memberId.equals(UNKNOWN_MEMBER_ID) && groupInstanceId == null) return;`. Classic verdict 6's "stronger than we claimed" carries over intact: `Consumer.publish`'s `generationId >= 0` guard stays load-bearing under KIP-848. |
| B4 | `TxnOffsetCommit` (and hence `sendOffsetsToTransaction`) is implemented for the new coordinator — transactional commits to consumer-type groups work, epoch-fenced when real metadata is supplied | **CONFIRMED** (3-0) | PR #14845 (KAFKA-14505, merged 2023-12-07, in 3.7+): transactional offset commits held in a pending-transactional-offsets structure until the transaction commits or aborts. Together with B2/B3: fenced with real metadata, unfenced only via the pre-KIP-447 compatibility shape (B3). |
| B5 | During revocation the coordinator keeps the member on its CURRENT epoch until the client acknowledges — a revoke-time commit at that epoch is ACCEPTED, and the partition is not reassigned until the ack | **CONFIRMED** (3-0) | KIP-848 reconciliation: "1. The group coordinator revokes the partitions … 2. When the group coordinator receives the acknowledgement of the revocation, it updates the member current assignment to its target assignment (and target epoch). 3. The group coordinator assigns the new partitions…". The coordinator lead rejected validation that would block commits in this window: "Let's imagine that a member must rebalance … The first thing that it will do is committing its offsets. With this logic, the commits will be rejected." (PR #16145 review). So the revoke-time flush direction flips versus classic-cooperative (verdict 5): under KIP-848 it is accepted like classic-eager — and safely, since ownership stays exclusive until the ack. |
| B6 | The silent bump is real broker-side: when the group epoch bumps (another member joins/leaves) and a member's assignment is UNCHANGED, the coordinator advances THAT member's epoch straight to the bumped target epoch — no revocation, no callback — and a commit still carrying the old epoch is fenced (`STALE_MEMBER_EPOCH`) | **CONFIRMED (source-quoted)** | This is the premise C2 depends on, now pinned on the broker side. `CurrentAssignmentBuilder.computeNextAssignment` (4.0.0, identical 4.1.0): with `partitionsPendingRevocation` and `partitionsPendingAssignment` both empty, execution reaches the final `else` — `new ConsumerGroupMember.Builder(member).setState(STABLE).updateMemberEpoch(targetAssignmentEpoch).setAssignedPartitions(newAssignedPartitions)…` (comment: "the member transitions to the target epoch and to the STABLE state") — `STABLE → STABLE` at the new epoch, never entering `UNREVOKED_PARTITIONS`. Corroborated by **KAFKA-19779** (a JIRA proposing to *relax* the strict check precisely because it fences this case): "This is not a zombie commit request … (because P1 was not reassigned to a different member), but it will still be rejected"; current check is "`Client-Side Member Epoch == Broker-Side Member Epoch` … returns a `STALE_MEMBER_EPOCH` error for regular offset commits". Closes the loop with C2 (client adopts the bump with no rebalance callback): the silent bump the client absorbs is one the broker actually emits — so the post-poll refresh is genuinely required under KIP-848 **on brokers ≤4.2.0**, not defensively. **Version bound: from 4.3.0 (KIP-1251) this commit is *accepted*** — the assignment is unchanged, so the partition is still owned and the lagging epoch passes the per-partition check; the spurious fence, and with it the refresh's role here, moves into the broker. Fenced ≤4.2.0, accepted ≥4.3.0; safe both ways. |

### Consequences for the captured-generation design

- **The token still lags, never leads** (C1 + C5): the captured value is only ever an epoch the member
  actually held, and leave sentinels are filtered before the public surface. Any lag lands as a fence
  (B2), so safety is preserved; the cost of lag is availability.
- **The failure is a graceful abort, same as classic** (C4, corrected): the transactional fence surfaces
  as `ILLEGAL_GENERATION` → abortable `CommitFailedException`, because the broker translates
  `StaleMemberEpochException` on the TxnOffsetCommit path. The earlier "harsher/fatal" framing was wrong
  for the path kafka-flow uses; the fatal client branch exists but is unreachable there.
- **The silent bump can only be tracked by *reading* `groupMetadata()`, and no read fully closes the
  window** (C2 + B6 + C1). Both sides of the bump are real — the broker advances an unchanged-assignment
  member's epoch (B6) and the client adopts it with no callback (C2) — so no callback-based scheme sees
  it; only a read does. Two honest qualifications the "only way / the refresh solves it" shorthand
  dropped: (1) *which* read is not unique — the design uses a post-poll read into a `Ref` because a live
  off-thread read of the classic consumer is unsafe, but under KIP-848 `groupMetadata()` is a thread-safe
  `AtomicReference`, so a live read at commit time is viable and strictly fresher; (2) neither read is
  *sufficient* — the epoch can advance between the read (post-poll or live) and the broker validating the
  commit (C1), so the read narrows the spurious-fence window but cannot close it. Net under KIP-848: a
  partial availability mitigation, not a guarantee; safety is unaffected regardless (a lagging token only
  fences).
- **The flows-alive coupling survives** (C3 + B5): no per-partition broker check exists (B1), so the
  awaited revoke-time teardown remains the sole cross-partition support — and both halves hold: the
  client cannot acknowledge until the callback completes, and the broker does not reassign until the
  acknowledgement. Both hold within the rebalance timeout only: a teardown stalling past it gets the
  member evicted and the partition reassigned over live flows — where the broker's rejection takes over:
  an evicted member is removed from the group, so its commits fail member validation
  (`UNKNOWN_MEMBER_ID`, before any epoch comparison; B1) — an availability loss, never a safety one.
- **The publish guard stays load-bearing** (B3): the transactional sentinel skip survived into the new
  coordinator, deliberately, for pre-KIP-447 compatibility.
- **Group commit amplifies a fence to the whole batch** (B1 + KF6/KF7): because validation is member+epoch
  with no per-partition check (B1), a single stale epoch aborts the *entire* group-committed batch of
  retained partitions, not one partition — a KIP-848-specific amplification of the C1 window. Availability
  only (replay), never a safety issue.
- **Static membership is out of scope.** `KafkaModule` does not itself set `group.instance.id`, so this
  analysis assumes dynamic members; a deployment that enables static membership through its own
  `ConsumerConfig` would need a separate pass on the static-member restart path (same instance,
  epoch/assignment continuity).

### Practical guide: how to reason under `group.protocol=consumer`

The classic mental model is one plane: generation movement, listener callbacks, and the public
`groupMetadata()` all happen on the application thread inside `poll` (verdicts 1–2), so poll-to-poll
reasoning covers everything. Under KIP-848 split the world into two planes, and be precise about which
guarantee lives on which:

**The callback plane stays poll-synchronous.** `onPartitionsAssigned`/`Revoked`/`Lost` still execute
only on the application thread, inside a consumer call — never on the background thread. Pinned in the
4.3.0 bytecode: `poll(Duration)` is the *only* production caller of the background-event drain
(`checkInflightPoll`); `unsubscribe()` (reachable also from `assign()`) runs the blocking drain;
`close()` invokes revoked/lost directly on the closing thread; `updateAssignmentMetadataIfNeeded`
contains a drain but has no caller within the consumer, and `commitSync` does not drain at all. So the
classic rule — callbacks only inside `poll`, plus the `close`/`unsubscribe` leave paths — survives
verbatim, and so does everything built on it: the reconciliation cannot finalize until the callback
returns (C3), and the coordinator does not reassign a revoked partition until the client acknowledges
(B5), so a teardown awaited inside `onPartitionsRevoked` still completes before anyone else can own
the partition.

**The epoch plane is never poll-synchronous.** What does *not* survive is "the generation only moves
inside `poll`". The member epoch — and with it the public `groupMetadata()` — advances on the
background heartbeat thread: between polls, during a poll, even while a callback is running (C1). And
it moves silently: a bump that changes no partitions fires no callback of any kind (C2), so no
callback-based scheme can track the epoch under this protocol — it must be read. Rules that follow:

1. Treat any `groupMetadata()` value as possibly stale the moment it is held; design so a stale epoch
   fails safe. Here it does: commit validation is exact-match (B2), so lag can only fence. Reading
   more often (e.g. after each poll) narrows the staleness window; nothing can close it.
2. The **awaited revoked-partition flush** needs no special epoch handling: the member keeps its epoch
   until it acknowledges the revocation (B5), and that flush runs inside `onPartitionsRevoked` before the
   ack, so it commits at the still-current epoch — the classic-cooperative always-fenced caveat
   (verdict 5) does not carry over. Read this narrowly: it is *not* "no commit is fenced in a rebalance
   window." A *retained*-partition commit issued while a co-tenant rebalance bumps the epoch is an
   instance of the read-to-commit window (rule 1 / C1) and can still be fenced — safely, as a graceful
   abort; that is the KAFKA-19779 case.
3. The transactional fence is an abortable `CommitFailedException`, same as classic (C4): the broker
   translates the stale-epoch error to `ILLEGAL_GENERATION` on the TxnOffsetCommit path, so recovery is
   transaction-abort-and-retry, not a fatal producer restart. (Only a *non*-transactional commit would
   see `STALE_MEMBER_EPOCH` and the client's unmapped fatal branch — not a path this design takes:
   `KafkaModule` sets `autoCommit = false` and the transactional mode commits offsets solely through
   `producer.sendOffsetsToTransaction`, so every fenced commit is transactional.)
4. The leave sentinels absorb themselves client-side — `groupMetadata()` never shows an epoch ≤ 0
   (C5) — but the broker still accepts the `−1` + empty-member-id transactional commit unfenced (B3), so
   a guard refusing negative epochs on the transactional path stays load-bearing.

The one-line summary: **callbacks tell you where the partitions are; only a read tells you what the
epoch is.** Classic lets the two coincide; KIP-848 does not.

### When the post-poll refresh can be retired (581 unwinding), from first principles

If skafka fixes issue 581 — so `onPartitionsAssigned` forwards the empty delta — capturing in that
callback could replace the post-poll refresh outright. Whether that is safe reduces to one condition:

> callback-capture is complete **iff** every change to `groupMetadata()` is accompanied by an
> `onPartitionsAssigned` invocation.

If it holds, the callback sees exactly the set of generation changes the post-poll read sees, so nothing
is missed. Safety (the token can only lag, never lead) holds either way, since both mechanisms only ever
write a generation the member actually joined — so the whole question is *completeness*, not safety.

**Classic protocol: the condition holds — the swap is sound, and a net win.** `groupMetadata()` is
assigned only in the constructor (−1, guard-filtered) and `onJoinComplete` (verdict 2), and
`onJoinComplete` invokes `onPartitionsAssigned` unconditionally on every completed rebalance (verdict 7),
stamping the generation *before* the callback (verdict 4). So generation-change ⟺ assigned-callback:
581-fixed capture observes precisely the changes the post-poll read would. It is if anything fresher
(captured inside the completing poll, not after it) and it removes a `groupMetadata()` read on every
poll; during a multi-poll join both mechanisms hold the last-completed-join value (verdict 8, the safe
lag). No safety or liveness cost, a simplification. The unit canary (`ConsumerSpec`) is the
tripwire that marks the moment 581 is fixed.

**Consumer protocol (KIP-848): the condition fails — the swap is unsafe for availability.** The
equivalence rests on generation-change ⟺ assigned-callback, and that is a classic-only fact. Under
KIP-848 the epoch bumps on the background thread with no callback at all when this member's partitions
are unchanged (addendum C2/B6), so callback-capture misses those bumps and the Ref lags until the member's
next real assignment change — reintroducing the spurious fence (an abortable commit failure, not fatal —
addendum C4). Only a *read* of `groupMetadata()` (post-poll, as the design does, or live) observes a
callback-less bump, so a read must stay wherever the consumer protocol is reachable **on a broker ≤4.2.0**
— though no read fully closes the window (C1), so it is a mitigation, not a cure. **On a 4.3.0+ broker
(KIP-1251) this changes**: the silent bump leaves the partition still owned, so its lagging commit is
accepted at the broker and the spurious fence disappears without any read — the refresh becomes largely
redundant there (relevant to a future consumer-protocol solution: a 4.3.0+ broker floor could drop it).

The unwinding is therefore gated on the **protocol**, not merely on the skafka fix: sound under classic,
unsafe under consumer. This is why the canary's note and the review dispositions (the appendix in
[`kafka-generation-study.md`](kafka-generation-study.md)) both scope the "refresh can be reconsidered"
conclusion to the classic protocol.

### Residual unknowns from this pass (do not cite as fact)

- The KIP-848 text appears to contain no transactions/EOS section (its silence is itself the finding) —
  the claim's verifier panel was lost to infrastructure; unverified.
- The consumer-group offset-commit relaxation is **settled and released** (an earlier pass here framed it
  as unmerged/future — wrong). Version boundary, verified by direct fetch of the release tags:
  `ConsumerGroup.validateOffsetCommit` is exact-equality through **4.2.0** (returns `NO_OP`); from
  **4.3.0** (released 2026-05-22) it does a three-way check where a *lagging* epoch triggers a
  per-partition assignment-epoch check (`createAssignmentEpochValidator`), attributed in-code to
  **KIP-1251** ("Assignment epochs for consumer groups") — accept if the partition is still assigned to
  the member, fence if reassigned. So on 4.3.0+ a still-owned partition's lagging commit is accepted,
  which only *removes* spurious fences; a reassigned partition stays fenced — more forgiving, never less
  safe. **KAFKA-19779 is the StreamsGroup-only sibling** of this idea (its PRs modify only
  `StreamsGroup.java`; a `group.protocol=consumer` group never hits it) — it is cited above only as
  documentation of the pre-relaxation *fencing* behavior (B6) and of the txn-path `ILLEGAL_GENERATION`
  translation (C4), both of which still hold. Correction trail: an advisory reviewer read the relaxation
  as shipped in 4.2.0 (wrong version) via KAFKA-19779 (wrong KIP); a later pass over-corrected to
  "unreleased trunk" (also wrong). The tag fetches settle it: exact-equality ≤4.2.0, KIP-1251 relaxation
  ≥4.3.0.
- The stale-**transactional**-commit error code is **settled** (was a residual): the coordinator
  translates `StaleMemberEpochException` → `ILLEGAL_GENERATION` on the TxnOffsetCommit path
  (`OffsetMetadataManager.validateTransactionalOffsetCommit`, source-quoted), which the producer aborts
  gracefully (`CommitFailedException`). The earlier "harsher/fatal" framing is corrected in C4, the
  consequences above, and (now) [`docs/kafka-single-writer-design.md`](../docs/kafka-single-writer-design.md).
- The **co-tenant-join epoch advance is settled** — B6 pins it at source. The **retained-partition
  revocation-window race** (a *retained*-partition commit issued while the broker bumps the epoch through
  a rebalance) is a concrete instance of the C1 read-to-commit window: on **≤4.2.0** it is fenced
  (`ILLEGAL_GENERATION`), availability-only (it aborts safely); on **4.3.0+** KIP-1251 accepts it for a
  still-owned partition, so the race largely resolves at the broker. Kafka-dev acknowledged this exact
  case (it is what KIP-1251 / the KAFKA-19779 Streams sibling were written to relax), not merely an
  empirical unknown.
- **The addendum was analytical; it is now partly runtime-confirmed.** It rests on decompiled client
  bytecode (4.3.0) and read broker source (4.0–4.2). The realized experiment
  ([`kafka-generation-study.md`](kafka-generation-study.md)) then exercised
  `group.protocol=consumer` against a live Kafka **4.3.0** broker and confirmed the load-bearing predictions:
  the member epoch advances **on the background thread with no rebalance callback** on a silent bump (only a
  read observes it), the epoch can be **read before the initial assign callback drains** on the poll thread,
  and the stale-epoch transactional commit is fenced → `CommitFailedException` → abort under both protocols.
  A code corollary fell out: since nothing reads the generation `Ref` between the assign callback and the
  end-of-poll refresh, **capture-on-assign is redundant** — the post-poll read is the whole mechanism (F-8
  corollary), which `TokenSync.tla` then proved at the model level (refresh subsumes capture: equivalent when
  every bump fires a callback, strictly better on a silent bump). The background-thread epoch advance is
  modelled too (`Kafka.tla`'s `GenBump` is a free action; an earlier note here calling the models
  "synchronous" was wrong). What stays genuinely open is only the read-to-commit window (C1), inherent to any
  read-based currency mechanism.

## Corrections this study forced

- **`Consumer.publish` comment**: claimed −1 is reported "before the first join and after falling out
  of the group". The fall-out half is wrong (verdict 2) — and had already been corrected once in an
  earlier pass, then lost in a history squash; re-applied. Only the pre-first-join state reports −1.
- **Eager revoke-time flush**: "the flush lands" now reads "lands in a graceful rebalance"
  (verdict 3's qualification).
- **This study's own earlier wording** ("revoked/lost are empty-gated") corrected per verdict 7; the
  filed skafka issue's framing shares the imprecision (harmless there — an empty eager revoked dropped
  by the typed layer loses nothing — but not repeated here).
- **KIP-848 sentinel claim downgraded**: this record previously stated `ConsumerGroup` rejects the
  sentinel against a non-empty group as if verified; per verdict 9 it is unverified. Recorded as an
  open question, not a fact.

## Open questions (unpinned; do not cite as fact)

Three of the four questions this section originally held were KIP-848 questions, now pinned by the
addendum (B1–B3 the validator, B4 transactional support, C1/C3 the drain points and the
mid-callback epoch advance). What remains open:

- Broker-side conditions under which an eager graceful revoke-commit still loses the race
  (rebalance-timeout expiry, member kicked; `REBALANCE_IN_PROGRESS` vs `ILLEGAL_GENERATION` for the
  transactional path) — classic protocol.
- The addendum's own residuals, listed there.

## Primary sources

- `clients/.../internals/ConsumerCoordinator.java`, `AbstractCoordinator.java` (trunk; branch 3.9; tags 2.5.0, 3.9.0)
- `core/src/main/scala/kafka/coordinator/group/GroupCoordinator.scala` (3.9 lineage); trunk `ClassicGroup.validateOffsetCommit`
- `ConsumerRebalanceListener` javadoc, 2.4 / 3.7 / 3.9
- KIP-266, KIP-429, KIP-447, KIP-848 (cwiki)

Addendum sources:

- kafka-clients **4.3.0** jar, decompiled (`AsyncKafkaConsumer`, `ConsumerMembershipManager`,
  `AbstractMembershipManager`, `TransactionManager`, `TxnOffsetCommitRequest`)
- `group-coordinator/.../modern/consumer/ConsumerGroup.java` `validateOffsetCommit` via the
  apache/kafka commit diffs for PR #16145 (mail-archive commits mirror) and PR #17914
- KAFKA-18060 (JIRA) + PR #17914; KAFKA-14505 + PR #14845 (TxnOffsetCommit in the new coordinator)
- KIP-848 text (offset-commit handling, reconciliation phases); PR #16145 review discussion
  (revocation-window commits); KAFKA-19779 (dev-list announcement — REPORTED only)
