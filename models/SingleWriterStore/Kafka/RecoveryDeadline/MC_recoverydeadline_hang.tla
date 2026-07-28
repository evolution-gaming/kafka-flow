---------------------- MODULE MC_recoverydeadline_hang ----------------------
\* expect: HOLDS
\* flags: -deadlock
\* The R-849 remedy working: a no-progress tripwire (Tripwire = TRUE,
\* NoProgressTrip = TRUE) with TripAt < Deadline catches the #849 hang
\* (HealthyProgress = FALSE) and fails loudly BEFORE the eviction deadline --
\* the member is never silently evicted.
EXTENDS RecoveryDeadline
=============================================================================
