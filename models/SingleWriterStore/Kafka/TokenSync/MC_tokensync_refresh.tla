------------------------ MODULE MC_tokensync_refresh ------------------------
\* expect: HOLDS
\* flags: -deadlock
\* Refresh alone (no capture) keeps the token current under BOTH bump kinds
\* (silent + assigning): the post-poll read sees the current generation
\* regardless of callbacks. The refresh-only (capture-removed) design.
EXTENDS TokenSync
=============================================================================
