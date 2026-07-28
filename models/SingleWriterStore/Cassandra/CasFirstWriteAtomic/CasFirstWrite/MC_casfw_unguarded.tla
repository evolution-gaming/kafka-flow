------------------------- MODULE MC_casfw_unguarded -------------------------
\* expect: VIOLATES INV_NoStaleOverwrite
\* flags: -deadlock
\* The offset guard is load-bearing even inside the compound: ungated UPDATEs
\* let a stale writer overwrite a strictly-higher cell.
EXTENDS CasFirstWrite
=============================================================================
