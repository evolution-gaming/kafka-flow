------------------------ MODULE MC_flowsalive_holds ------------------------
\* expect: HOLDS
\* flags:  -deadlock
\* Positive control for the cross-partition flows-alive invariant. With
\* AwaitTeardown=TRUE (the code: `TopicFlow.remove` awaits
\* `cache.remove(_).flatten` on the revoke callback, run synchronously before
\* assign and before poll returns), a revoked flow is torn down before the
\* node owns the new generation, so no live flow is ever un-owned:
\* INV_FlowsAlive holds. Non-vacuous -- the paired negative flowsalive_race
\* reaches, at this same bound, the un-owned-flow state this config keeps
\* empty. -deadlock: once gen = MaxGen with all flows owned, no action is
\* enabled -- a benign terminal state.
EXTENDS FlowsAlive
=============================================================================
