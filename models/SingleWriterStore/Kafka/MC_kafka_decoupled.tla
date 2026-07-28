------------------------- MODULE MC_kafka_decoupled -------------------------
\* expect: VIOLATES-REFINEMENT RefSafeSpec
\* flags: -deadlock
\* Capture decoupled from teardown: a revoked flow captures the current
\* generation yet keeps flushing -> its stale write lands -> Kafka no longer
\* refines (#732 reopens).
EXTENDS Kafka
=============================================================================
