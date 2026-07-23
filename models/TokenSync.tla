--------------------------- MODULE TokenSync ---------------------------
(*******************************************************************************)
(* Capture vs. refresh: are they equivalent ways to keep the owner's published   *)
(* generation token current, and where do they differ? A focused model of just    *)
(* the owner token, isolated from the refinement tower (Kafka.tla), answering the  *)
(* question the capture-removal experiment raised.                                 *)
(*                                                                             *)
(* The coordinator's `liveGen` advances by two kinds of bump, each a free action   *)
(* (so an advance can interleave anywhere -- the background-thread epoch advance,   *)
(* not a poll-synchronous one):                                                    *)
(*   - AssigningBump : a rebalance that fires an assignment callback for this       *)
(*     member (an eager rebalance, or -- under classic -- the empty cooperative      *)
(*     callback a 581-fixed skafka would forward). It leaves a callback `owed`.       *)
(*   - SilentBump    : a bump that fires NO callback at all -- the KIP-848 consumer   *)
(*     protocol when this member keeps its partitions (or classic-cooperative under   *)
(*     skafka 581). It owes nothing.                                                   *)
(*                                                                             *)
(* Two mechanisms sync the token to `liveGen`, each a knob:                        *)
(*   - OwnerCapture (Capture): the assignment-callback capture. Can fire only when    *)
(*     a callback is owed -- capture is driven by callbacks.                          *)
(*   - OwnerRefresh (Refresh): the post-poll `groupMetadata()` read. A read sees the  *)
(*     current generation regardless of callbacks, so it needs nothing owed.          *)
(*                                                                             *)
(* Result (checked by the five configs). Note this model DEMONSTRATES the consequences  *)
(* of an assumed capture/refresh asymmetry (capture is callback-gated, refresh is not); *)
(* it does not *prove* that asymmetry -- that is a fact about the code and the broker    *)
(* (Consumer.scala; the KIP-848 audit), evidenced by the IT suite. Here it is a bounded  *)
(* consistency check of the reasoning:                                                   *)
(*   - Under callback-firing bumps only (AllowSilent=FALSE), capture-on-finished-        *)
(*     rebalance and refresh are EQUIVALENT: each alone keeps the token current          *)
(*     (Synced holds) -- tokensync_capture_assigning and tokensync_refresh both HOLD.     *)
(*   - Refresh SUBSUMES capture: with a silent bump reachable, refresh still holds        *)
(*     (tokensync_refresh, tokensync_both), but capture cannot close the lag (no callback *)
(*     to drive it) -- tokensync_capture VIOLATES Synced. This is the KIP-848 case, and    *)
(*     the reason capture-on-assign was safe to remove: the read subsumes it.             *)
(*   - `token` never LEADS `liveGen` (TokenNeverLeads). This holds by construction --      *)
(*     `token` is only ever assigned the current `liveGen` -- so it is a structural        *)
(*     sanity check of this model, NOT a checked safety result; the real stale-write        *)
(*     safety is modelled in Kafka.tla (a zombie can actually land a write there).          *)
(*******************************************************************************)
EXTENDS Naturals

CONSTANTS MaxGen, Capture, Refresh, AllowSilent

VARIABLES liveGen, token, owed
  \* liveGen : the coordinator's current generation (its truth)
  \* token   : the owner's published generation -- what its transactional commits carry
  \* owed    : an assignment callback is available for the owner to deliver (an AssigningBump happened)
vars == <<liveGen, token, owed>>

Init ==
  /\ liveGen = 1
  /\ token = 1
  /\ owed = FALSE

\* a rebalance that fires an assignment callback for this member (leaves a callback owed).
AssigningBump ==
  /\ liveGen < MaxGen
  /\ liveGen' = liveGen + 1
  /\ owed' = TRUE
  /\ UNCHANGED token

\* a generation bump with no callback for this member (the KIP-848 silent bump).
SilentBump ==
  /\ AllowSilent
  /\ liveGen < MaxGen
  /\ liveGen' = liveGen + 1
  /\ UNCHANGED <<token, owed>>

\* callback-driven capture: delivers an owed callback, reading the CURRENT generation.
OwnerCapture ==
  /\ Capture
  /\ owed
  /\ token' = liveGen
  /\ owed' = FALSE
  /\ UNCHANGED liveGen

\* the post-poll read: syncs the token to the current generation, callback or none.
OwnerRefresh ==
  /\ Refresh
  /\ token /= liveGen
  /\ token' = liveGen
  /\ owed' = FALSE
  /\ UNCHANGED liveGen

Next ==
  \/ AssigningBump
  \/ SilentBump
  \/ OwnerCapture
  \/ OwnerRefresh

Spec == Init /\ [][Next]_vars /\ WF_vars(OwnerCapture) /\ WF_vars(OwnerRefresh)

----------------------------------------------------------------------------
\* safety: the token is only ever a generation the member actually holds -- it never leads.
TokenNeverLeads == token <= liveGen
\* liveness: the token eventually stays current (after bumps stop, the sync mechanism catches up).
Synced == <>[](token = liveGen)

TypeOK ==
  /\ liveGen \in 1 .. MaxGen
  /\ token \in 1 .. MaxGen
  /\ owed \in BOOLEAN
=============================================================================
