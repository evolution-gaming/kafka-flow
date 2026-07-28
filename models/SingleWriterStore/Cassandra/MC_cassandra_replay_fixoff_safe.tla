------------------ MODULE MC_cassandra_replay_fixoff_safe ------------------
\* expect: HOLDS
\* flags: -deadlock
\* The safety half of the replay-window claim: with the monotone buffer
\* REMOVED (Fix=FALSE) the legitimate owner livelocks
\* (cassandra_replay_fixoff, the paired liveness failure) but nothing unsafe
\* becomes durable -- a conflicted flush writes nothing, the zombie stays
\* offset-gated, and the durable cell remains the correct fold. Verifies the
\* "safety still holds; this is purely a liveness failure" claim instead of
\* asserting it. RefLive is deliberately NOT checked here.
EXTENDS Cassandra
=============================================================================
