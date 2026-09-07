-------------------- MODULE MC_kafka_revokewrite_refines --------------------
\* expect: HOLDS
\* flags: -deadlock
\* THEOREM Kafka => SingleWriterStore with the revoke-time write: the
\* revoking member reads the client's already-moved generation inside the
\* revoke callback and lands its flush and offset commit under it, then
\* tears down (RevokeWrite) -- under the cooperative ordering fact
\* (RevokeBeforeHandover: the revoked partition is withheld from its next
\* owner, and no round completes, until the revoker's callback has returned
\* and it rejoined). Nothing else owns the partition while the callback
\* runs, so the write is an idempotent rewrite and the commit closes the
\* one-round lag. All else as kafka_refines.
EXTENDS Kafka
=============================================================================
