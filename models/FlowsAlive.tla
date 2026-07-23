------------------------------- MODULE FlowsAlive -------------------------------
(*****************************************************************************)
(* The cross-partition "flows-alive" invariant for the Kafka transactional   *)
(* writer (docs/kafka-single-writer-design.md).                              *)
(*                                                                          *)
(* KIP-447 fences an offset commit by consumer GENERATION, but               *)
(* `sendOffsetsToTransaction` validates only member + generation -- there is  *)
(* NO per-partition ownership check on the broker. So the only thing keeping  *)
(* a lingering flow for a no-longer-owned partition from committing under a   *)
(* fresh token is that the flow is torn down before the node acts in the new  *)
(* generation. That teardown is `TopicFlow.remove` awaiting the cache release *)
(* (`cache.remove(_).flatten`, `parTraverse_`) inside the revoked/lost        *)
(* callback, which the client runs synchronously before                       *)
(* `onPartitionsAssigned` and before `poll` returns.                         *)
(*                                                                          *)
(* Safety, not liveness: a single commit from an un-owned flow corrupts, so   *)
(* `live \subseteq owned` must hold at every state, not just eventually. One  *)
(* knob:                                                                      *)
(*   AwaitTeardown  TRUE  -- revoke awaits teardown before the node moves to   *)
(*                          the new generation; no un-owned flow is ever live. *)
(*                  FALSE -- fire-and-forget: the revoked flow lingers as a     *)
(*                          separate interleavable `Teardown` while the node    *)
(*                          already owns the new generation -- the exact race   *)
(*                          the awaited coupling prevents.                      *)
(*****************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Partitions, MaxGen, AwaitTeardown

VARIABLES
  owned,   \* partitions assigned to this node in the CURRENT generation
  live,    \* partitions that have a live flow in the cache
  gen      \* generation counter (bumped on each rebalance)

vars == << owned, live, gen >>

Init ==
  /\ owned = {}
  /\ live  = {}
  /\ gen   = 0

\* a rebalance: the node's assignment changes to `newOwned`. Partitions leaving
\* (`owned \ newOwned`) are revoked; entering ones get fresh flows. The
\* generation is refreshed. Whether the revoked flows are gone by the time the
\* node owns the new generation is the AwaitTeardown knob.
Rebalance(newOwned) ==
  /\ gen < MaxGen
  /\ newOwned \subseteq Partitions
  /\ newOwned # owned
  /\ gen'   = gen + 1
  /\ owned' = newOwned
  /\ IF AwaitTeardown
       \* revoke awaits teardown (cache.remove(_).flatten) before the node
       \* proceeds: revoked flows are gone, new flows created, atomically.
       THEN live' = (live \ (owned \ newOwned)) \cup (newOwned \ live)
       \* fire-and-forget: new flows created now, revoked flows NOT yet removed
       \* -- they linger, to be reaped by the separate Teardown action below.
       ELSE live' = live \cup (newOwned \ live)

\* the deferred teardown finally completing on some other fiber -- only ever
\* enabled when a flow outlived its ownership (the fire-and-forget case).
Teardown(p) ==
  /\ p \in live
  /\ p \notin owned
  /\ live' = live \ {p}
  /\ UNCHANGED << owned, gen >>

Next ==
  \/ \E s \in SUBSET Partitions : Rebalance(s)
  \/ \E p \in Partitions : Teardown(p)

SafeSpec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* THE flows-alive invariant: every live flow is owned in the current
\* generation. A live-but-un-owned flow is precisely the lingering foreign-
\* partition owner that could commit under a fresh token (no broker per-
\* partition fence). Must hold at ALL times -- it is safety, not liveness.
INV_FlowsAlive == live \subseteq owned

TypeOK ==
  /\ owned \subseteq Partitions
  /\ live  \subseteq Partitions
  /\ gen \in 0 .. MaxGen
=============================================================================
