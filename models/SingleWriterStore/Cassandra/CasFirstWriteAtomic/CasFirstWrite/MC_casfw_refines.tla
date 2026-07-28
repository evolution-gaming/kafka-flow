-------------------------- MODULE MC_casfw_refines --------------------------
\* expect: HOLDS
\* flags: -deadlock
\* Grain of atomicity: the non-atomic first-write compound refines one atomic
\* CAS.
EXTENDS CasFirstWrite
=============================================================================
