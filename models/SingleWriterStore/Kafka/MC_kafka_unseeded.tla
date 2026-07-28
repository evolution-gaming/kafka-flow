------------------------- MODULE MC_kafka_unseeded -------------------------
\* expect: VIOLATES-REFINEMENT RefSafeSpec
\* flags: -deadlock
\* The offset-to-commit left unseeded: the first flush carries no offset, so
\* it is ungated and a stale write lands -> Kafka no longer refines.
EXTENDS Kafka
=============================================================================
