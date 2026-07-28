------------------- MODULE MC_tokensync_capture_assigning -------------------
\* expect: HOLDS
\* flags: -deadlock
\* Capture-on-finished-rebalance alone (no refresh), when EVERY bump fires a
\* callback (AllowSilent=FALSE -- an eager rebalance, or a 581-fixed classic
\* cooperative one): EQUIVALENT to refresh, the token stays current.
EXTENDS TokenSync
=============================================================================
