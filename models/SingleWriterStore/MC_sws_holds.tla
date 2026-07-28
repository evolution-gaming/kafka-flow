---------------------------- MODULE MC_sws_holds ----------------------------
\* expect: HOLDS
\* flags: -deadlock
\* The abstract spec is self-consistent: durable is always the correct fold,
\* and every key's whole log eventually becomes durable.
EXTENDS SingleWriterStore
=============================================================================
