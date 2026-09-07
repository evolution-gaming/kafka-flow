------------------- MODULE MC_kafka_revokewrite_unordered -------------------
\* expect: VIOLATES-REFINEMENT RefSafeSpec
\* flags: -deadlock
\* The ordering removed (RevokeBeforeHandover=FALSE): the revoke callback
\* may run after the new owner has taken over and written. It captures the
\* same live generation, so the fence accepts the flush, and the revoked
\* flow's frozen buffer regresses the snapshot -> Kafka no longer refines
\* (#732 through an ACCEPTED write). The generation check does not encode
\* partition ownership; the withholding does. Paired positive:
\* kafka_revokewrite_refines.
EXTENDS Kafka
=============================================================================
