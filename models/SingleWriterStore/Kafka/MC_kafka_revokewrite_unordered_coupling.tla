-------------- MODULE MC_kafka_revokewrite_unordered_coupling --------------
\* expect: HOLDS
\* flags: -deadlock
\* The same unordered run at the coupling invariant: INV_CaptureCoupled
\* HOLDS, because capture, write and teardown are one action and no state
\* shows a zombie alive with the live generation -- the invariant that
\* catches the decoupling refactor (kafka_decoupled_coupling) is blind to
\* the revoke-write hazard. The hazard is in the write's effect, and only
\* the step simulation sees it (kafka_revokewrite_unordered).
EXTENDS Kafka
=============================================================================
