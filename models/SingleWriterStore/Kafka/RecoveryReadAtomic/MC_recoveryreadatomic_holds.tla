-------------------- MODULE MC_recoveryreadatomic_holds --------------------
\* expect: HOLDS
\* flags: -deadlock
\* The spec is self-consistent (cf. sws_holds): the atomic read -- one
\* linearization point observing exactly the committed set, then a response
\* -- terminates and returns only committed records, under the full
\* double-handover cast with the broker's timeout abort.
EXTENDS RecoveryReadAtomic
=============================================================================
