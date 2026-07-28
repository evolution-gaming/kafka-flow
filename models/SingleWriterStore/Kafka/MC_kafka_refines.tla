-------------------------- MODULE MC_kafka_refines --------------------------
\* expect: HOLDS
\* flags: -deadlock
\* THEOREM Kafka => SingleWriterStore: capture coupled to teardown + every
\* flush seeded.
EXTENDS Kafka
=============================================================================
