-------------------- MODULE MC_kafka_decoupled_coupling --------------------
\* expect: VIOLATES INV_CaptureCoupled
\* flags: -deadlock
\* The same cause at the coupling invariant: decoupling lets an alive zombie
\* hold the current captured generation -- which is what then lets the
\* refinement (kafka_decoupled) break.
EXTENDS Kafka
=============================================================================
