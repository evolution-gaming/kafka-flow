---------------------------- MODULE MC_gc_nofair ----------------------------
\* expect: VIOLATES-TEMPORAL Termination
\* Liveness control (Sec. 14.3.5): without fairness the orchestration need
\* not terminate -- a writer may never take the lock. Confirms the fairness
\* is load-bearing and TLC's liveness engine reports the violation.
EXTENDS GroupCommit
=============================================================================
