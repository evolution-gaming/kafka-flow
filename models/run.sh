#!/usr/bin/env bash
#
# Run every TLC config and assert its expected outcome (holds, or a specific
# invariant violation). This table is the authoritative index of the suite:
# each row pairs a config with the model it checks and what it must demonstrate.
#
#   ./run.sh           run everything, print a PASS/FAIL line per config
#   ./run.sh genfence  run only configs whose name matches the filter
#
# Needs a JRE and tla2tools.jar (v1.8.0+) next to this script:
#   https://github.com/tlaplus/tlaplus/releases
#
set -u
cd "$(dirname "$0")" || exit 2

JAR=tla2tools.jar
if [[ ! -f $JAR ]]; then
  echo "tla2tools.jar not found. Download v1.8.0+ from" >&2
  echo "  https://github.com/tlaplus/tlaplus/releases  (place it next to this script)" >&2
  exit 2
fi

# config | model | flags | expected
#   expected is HOLDS, or VIOLATES:<InvariantName> (the invariant TLC reports first).
#   flags carries -deadlock only where a run reaches a terminal state and would
#   otherwise be flagged as a (benign) deadlock.
CASES=(
  # Cassandra compare-and-set (store layer; persist treated as atomic)
  "guarded_holds|CasCompareAndSet|-deadlock|HOLDS"
  "unguarded_gap|CasCompareAndSet||VIOLATES:INV_NoStaleOverwrite"          # ungated DELETE erases a newer snapshot
  "guarded_residual|CasCompareAndSet||VIOLATES:INV_OwnerDoneImpliesCorrect" # R1 residual the tombstone closes
  "revive_r1|CasDeleteRevive||VIOLATES:INV_NoCorruptDurable"               # row-removing delete -> stale revive
  "revive_r2|CasDeleteRevive||HOLDS"                                        # offset-carrying tombstone
  # first-write compound (the only store model where persist is NOT atomic)
  "casfw_guarded|CasFirstWrite|-deadlock|HOLDS"
  "casfw_unguarded|CasFirstWrite|-deadlock|VIOLATES:INV_NoStaleOverwrite"  # the offset guard is load-bearing
  "casfw_reap|CasFirstWrite|-deadlock|HOLDS"                                # safety survives a TTL reap mid-protocol
  "casfw_spurious|CasFirstWrite|-deadlock|VIOLATES:INV_NeverSpurious"      # spurious conflict reachable (liveness-only)
  "casfw_3w|CasFirstWrite|-deadlock|HOLDS"                                  # three first-writers: one retry is enough
  # in-memory buffer / processing-offset layer
  "replay_fix_on|ReplayFence||HOLDS"
  "replay_fix_off|ReplayFence||VIOLATES:INV_NoSelfFence"                   # the replay-window self-fence bug
  "replay_fix_off_safety|ReplayFence||HOLDS"                                # bug is liveness-only: INV_NoStaleApply still holds

  # Kafka generation fencing
  "genfence_coupled|GenerationFencing||HOLDS"                               # live capture coupled to teardown, seeded
  "genfence_decoupled|GenerationFencing||VIOLATES:INV_NoStaleDurable"      # refactor hazard: capture decoupled (#732 reopens)
  "genfence_decoupled_F|GenerationFencing||VIOLATES:INV_F"                 # same cause, checked at the coupling invariant
  "genfence_unseeded|GenerationFencing||VIOLATES:INV_NoStaleDurable"       # unseeded offset-to-commit: first flush ungated
  "genfence_batch|GenerationFencing||HOLDS"                                 # real group commit: a fenced batch lands nothing
  # group commit orchestration (PlusCal)
  "gc_3_1|GroupCommitConc||HOLDS"   # Cap=1: no batching
  "gc_3_2|GroupCommitConc||HOLDS"   # Cap=2: partial batch with a leftover
  "gc_3_3|GroupCommitConc||HOLDS"   # Cap=3=N: one holder drains the whole queue

  # Cross-model compositions
  "crt_holds|CasReplayTombstone||HOLDS"                                     # replay fence + tombstone, one key
  "crt_nofix|CasReplayTombstone||VIOLATES:INV_NoSelfFence"                 # tombstone on, replay fix off
  "crt_notomb|CasReplayTombstone||VIOLATES:INV_NoCorruptDurable"           # replay fix on, tombstone off

  # Rejected / early designs (holes demonstrated)
  "epochfence|EpochFencing||VIOLATES:INV_OwnerNeverFenced"                 # producer-epoch fencing: true owner fenced
  "epochfence_stale|EpochFencing||VIOLATES:INV_NoStaleDurable"            # ...and the stale write lands (separate config: TLC halts at the first violated invariant)
)

filter="${1:-}"
pass=0
fail=0
for row in "${CASES[@]}"; do
  IFS='|' read -r cfg model flags expected <<<"$row"
  [[ -n $filter && $cfg != *"$filter"* ]] && continue

  out=$(java -cp "$JAR" tlc2.TLC $flags -config "$cfg.cfg" "$model.tla" 2>&1)

  if [[ $expected == HOLDS ]]; then
    if grep -q "Model checking completed. No error has been found." <<<"$out"; then
      result=PASS
    else
      result=FAIL
    fi
  else
    want=${expected#VIOLATES:}
    if grep -q "Invariant $want is violated" <<<"$out"; then
      result=PASS
    else
      result=FAIL
    fi
  fi

  if [[ $result == PASS ]]; then ((pass++)); else ((fail++)); fi
  printf '%-7s %-22s %-18s %s\n' "$result" "$cfg" "$model" "$expected"
done

echo "----"
echo "$pass passed, $fail failed"
[[ $fail -eq 0 ]]
