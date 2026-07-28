--------------------- MODULE MC_recoverydeadline_notrip ---------------------
\* expect: VIOLATES INV_NoSilentEviction
\* flags: -deadlock
\* Issue #849 itself: the read loop as shipped has NO tripwire (Tripwire =
\* FALSE), so a hang (HealthyProgress = FALSE -- target outlives the log end)
\* stalls all the way to the eviction deadline and the member is silently
\* evicted. This is the defect the tripwire must fix.
EXTENDS RecoveryDeadline
=============================================================================
