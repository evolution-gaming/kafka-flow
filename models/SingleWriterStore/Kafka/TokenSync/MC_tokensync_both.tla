------------------------- MODULE MC_tokensync_both -------------------------
\* expect: HOLDS
\* flags: -deadlock
\* 2x2 cell [both]. Capture + refresh together, silent bump reachable: the
\* refresh closes every lag and the capture is harmless belt-and-suspenders
\* -- so it holds, same as refresh-only (capture adds nothing).
EXTENDS TokenSync
=============================================================================
