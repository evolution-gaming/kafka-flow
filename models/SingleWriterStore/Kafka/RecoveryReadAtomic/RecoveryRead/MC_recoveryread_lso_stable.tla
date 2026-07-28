--------------------- MODULE MC_recoveryread_lso_stable ---------------------
\* expect: HOLDS
\* flags: -deadlock
\* Remedy (2), alternative B (NOT adopted; A's HW bound is the open
\* alternative): a stable per-partition transactional.id. B's MANDATORY
\* initTransactions aborts A's open transaction before B writes S3, so within
\* the lineage a committed record above an open transaction is unreachable
\* (INV_LineageSerialized) -- the plain read_committed endOffsets bound
\* linearizes (RefAtomic) with NO wait and NO reader-side ordering
\* assumption: the read completes at a dangling transaction without waiting
\* it out, missing nothing.
EXTENDS RecoveryRead
=============================================================================
