--------------------------- MODULE MC_casfw_reap ---------------------------
\* expect: HOLDS
\* flags: -deadlock
\* Safety survives a TTL reap mid-protocol: even with a row removed between
\* the INSERT and the retry, no stale overwrite occurs.
EXTENDS CasFirstWrite
=============================================================================
