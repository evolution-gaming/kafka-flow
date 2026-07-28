------------------------ MODULE MC_tokensync_neither ------------------------
\* expect: VIOLATES-TEMPORAL Synced
\* flags: -deadlock
\* Negative control: with neither mechanism the token never re-syncs after
\* any bump.
EXTENDS TokenSync
=============================================================================
