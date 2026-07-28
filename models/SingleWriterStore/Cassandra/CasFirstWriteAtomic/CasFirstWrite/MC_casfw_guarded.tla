-------------------------- MODULE MC_casfw_guarded --------------------------
\* expect: HOLDS
\* flags: -deadlock
\* The non-atomic compound with the offset guard on: no stale overwrite under
\* any interleaving of two concurrent first-writers.
EXTENDS CasFirstWrite
=============================================================================
