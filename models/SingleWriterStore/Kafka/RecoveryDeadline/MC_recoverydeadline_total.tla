--------------------- MODULE MC_recoverydeadline_total ---------------------
\* expect: VIOLATES INV_OnlyStalledFails
\* flags: -deadlock
\* R-849.1 is load-bearing: a tripwire keyed on TOTAL elapsed time
\* (NoProgressTrip = FALSE) instead of consecutive no-progress fails a read
\* that is still MAKING PROGRESS (HealthyProgress = TRUE) -- the loud failure
\* fires with noprog = 0. A large-but-progressing recovery must not be
\* killed; the tripwire must measure no-progress, not duration.
EXTENDS RecoveryDeadline
=============================================================================
