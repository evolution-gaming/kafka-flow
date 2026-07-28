-------------------- MODULE MC_recoveryread_lso_foreign --------------------
\* expect: VIOLATES-REFINEMENT RefAtomic
\* flags: -deadlock
\* The residual of remedy (2), as a negative control: a producer OUTSIDE the
\* partition's id lineage (a second application misconfigured onto the same
\* snapshot topic) leaves an open transaction no takeover can abort,
\* re-pinning the LSO below committed records -- Capture again excludes a
\* committed record and the step simulation fails. What the
\* one-topic-one-flow discipline carries; a deployment that shares a snapshot
\* topic already mixes state on recovery, so the discipline is required
\* independently of the read bound (the HW-wait would only turn this silent
\* under-read into a slow read).
EXTENDS RecoveryRead
=============================================================================
