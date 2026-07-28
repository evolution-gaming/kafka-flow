-------------------------- MODULE MC_epoch_refines --------------------------
\* expect: VIOLATES-REFINEMENT RefSafeSpec
\* flags: -deadlock
\* THEOREM Epoch => SingleWriterStore is FALSE (the rejected design): the
\* stale write lands.
EXTENDS Epoch
=============================================================================
