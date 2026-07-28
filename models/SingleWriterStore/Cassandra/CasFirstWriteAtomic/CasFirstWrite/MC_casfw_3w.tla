---------------------------- MODULE MC_casfw_3w ----------------------------
\* expect: HOLDS
\* flags: -deadlock
\* Three concurrent first-writers: a third can act between a loser's
\* INSERT-fail and its retry; stresses "one retry is enough".
EXTENDS CasFirstWrite
=============================================================================
