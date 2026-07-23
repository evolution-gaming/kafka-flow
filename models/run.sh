#!/usr/bin/env bash
#
# Discover and run every TLC config in this folder, asserting the outcome each config declares
# inline. The models form a refinement tower (one abstract spec, everything else a refinement), so
# the layout is flat and each config names its module:
#
#   \* spec: Cassandra                       -- the module to run (required)
#   \* expect: HOLDS                          -- model checking completes with no error
#   \* expect: VIOLATES INV_Some               -- that state invariant is violated (a safety control)
#   \* expect: VIOLATES-TEMPORAL Prop          -- that temporal property is violated (a liveness control)
#   \* expect: VIOLATES-REFINEMENT Prop        -- the refinement (step simulation) fails: a rejected
#                                                design, or a removed fence -- Impl does NOT imply the spec
#   \* flags:  -deadlock                       -- optional; for a HOLDS run that reaches a terminal state
#
#   ./run.sh            check every config; one PASS/FAIL line each, non-zero exit on any failure
#   ./run.sh refines    only configs whose name contains the filter
#
# Needs a JRE. tla2tools.jar is downloaded to this folder on first run if missing (git-ignored);
# override the version/URL with the JAR_VERSION/JAR_URL vars below.
#
# Pinned to tlaplus release v1.7.0 (JAR_URL below) -- the reproducible artifact the suite is verified
# against. Its jar self-reports "TLC2 Version 2.15 ... rev: eb3ff99" (verify with `java -cp tla2tools.jar
# tlc2.TLC` -- the banner prints on any invocation). The matchers target this pre-2.17 output: it emits
# the unnamed "Temporal properties were violated." report (no property name), which the
# VIOLATES-TEMPORAL matcher accepts for a single-property config. Newer TLC (e.g. v1.8.0 / 2.18) is NOT a drop-in:
# its output tripped every matcher (0/N). Bump only with a full suite re-run and any matcher changes it
# forces.
set -u
cd "$(dirname "$0")" || exit 2

JAR="$PWD/tla2tools.jar"
JAR_VERSION="v1.7.0"
JAR_URL="https://github.com/tlaplus/tlaplus/releases/download/${JAR_VERSION}/tla2tools.jar"
if [[ ! -f $JAR ]]; then
  echo "tla2tools.jar not found; downloading ${JAR_VERSION} -> $JAR" >&2
  tmp="$JAR.tmp.$$"
  if command -v curl >/dev/null 2>&1; then
    dl=(curl -fSL -o "$tmp" "$JAR_URL")
  elif command -v wget >/dev/null 2>&1; then
    dl=(wget -qO "$tmp" "$JAR_URL")
  else
    echo "neither curl nor wget found; download manually from" >&2
    echo "  $JAR_URL  (place it at this folder's root)" >&2
    exit 2
  fi
  # "PK" = the zip/jar magic; guards against an HTML error page or a partial download
  if "${dl[@]}" && [[ -s $tmp && "$(head -c2 "$tmp")" == "PK" ]]; then
    mv "$tmp" "$JAR"
  else
    rm -f "$tmp"
    echo "download failed; fetch it manually from" >&2
    echo "  $JAR_URL  (place it at this folder's root)" >&2
    exit 2
  fi
fi

directive() {  # value after a leading "\* <keyword>:" directive line, trimmed; empty if absent.
  # Anchored to the directive form (keyword is the first token after the comment marker), so a prose
  # comment that merely mentions "expect:"/"spec:" cannot be mistaken for the directive.
  awk -v k="$1" '
    /^[[:space:]]*\\\*[[:space:]]*/ {
      s = $0; sub(/^[[:space:]]*\\\*[[:space:]]*/, "", s)
      if (index(s, k ":") == 1) {
        v = substr(s, length(k) + 2)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", v)
        print v; exit
      }
    }' "$2"
}

filter="${1:-}"
pass=0; fail=0

for cfg in *.cfg; do
  [[ -n $filter && $cfg != *"$filter"* ]] && continue
  name="${cfg%.cfg}"
  spec="$(directive spec "$cfg")"
  expect="$(directive expect "$cfg")"
  read -r -a flags <<<"$(directive flags "$cfg")"   # optional; word-split into argv (empty -> no arg)

  if [[ -z $spec || -z $expect ]]; then
    printf '%-7s %-26s %s\n' FAIL "$name" "missing '\\* spec:' or '\\* expect:' directive"
    ((fail++)); continue
  fi

  # Reproducibility (both found by the fresh-context model review):
  #  - Unique metadir per config: TLC's default scratch dir is states/<spec>/<timestamp-to-the-second>,
  #    so two configs of the SAME spec finishing within one second collide ("directory already exists").
  #  - Single worker by default: multi-worker (`-workers auto`) liveness checking crashes in some
  #    environments with `FileNotFoundException .../nodes_0` during implied-temporal checking, falsely
  #    failing genuinely-HOLDS temporal configs. The suite is tiny, so serial is the reproducible
  #    default; override with WORKERS=auto for speed once you've confirmed it is stable locally.
  meta="states/$name.$$"
  out="$(java -cp "$JAR" tlc2.TLC -workers "${WORKERS:-1}" -metadir "$meta" "${flags[@]+"${flags[@]}"}" -config "$cfg" "$spec.tla" 2>&1)"

  case "$expect" in
    HOLDS)
      grep -q "Model checking completed. No error has been found." <<<"$out" && r=PASS || r=FAIL ;;
    VIOLATES-TEMPORAL\ *)
      prop="${expect#VIOLATES-TEMPORAL }"
      # TLC >= 2.17 names the violated property; the pinned pre-2.17 TLC reports only "Temporal
      # properties were violated." -- accept the unnamed form ONLY when the config declares exactly
      # this one temporal property, which makes the unnamed report unambiguous.
      if grep -q "Temporal property ${prop} was violated" <<<"$out"; then r=PASS
      elif grep -q "Temporal properties were violated" <<<"$out" \
        && [[ "$(grep -cE '^[[:space:]]*PROPERT(Y|IES)[[:space:]]' "$cfg")" == 1 ]] \
        && grep -qE "^[[:space:]]*PROPERT(Y|IES)[[:space:]]+${prop}[[:space:]]*$" "$cfg"; then r=PASS
      else r=FAIL; fi ;;
    VIOLATES-REFINEMENT\ *)
      # a refinement (step-simulation) failure: TLC reports the violated abstract module + location,
      # not the property's alias -- so match the abstract module the declared alias instantiates
      # (RefinesAtomic -> CasFirstWriteAtomic; the Ref* tower aliases -> SingleWriterStore).
      case "${expect#VIOLATES-REFINEMENT }" in
        RefinesAtomic) mod="CasFirstWriteAtomic" ;;
        RefAtomic)     mod="RecoveryReadAtomic" ;;
        Ref*)          mod="SingleWriterStore" ;;
        *)             mod="[A-Za-z0-9_]+" ;;
      esac
      grep -Eq "Action property .* of module ${mod} is violated" <<<"$out" && r=PASS || r=FAIL ;;
    VIOLATES\ *)
      grep -q "Invariant ${expect#VIOLATES } is violated" <<<"$out" && r=PASS || r=FAIL ;;
    *) r=FAIL ;;
  esac

  if [[ $r == PASS ]]; then ((pass++)); else ((fail++)); fi
  printf '%-7s %-26s %-18s %s\n' "$r" "$name" "$spec" "$expect"
  # On an UNEXPECTED result, dump the raw TLC output so a CI failure is diagnosable (the loop otherwise
  # swallows it). Silent on a green run; bounded to the tail, where TLC's completion/violation/error lines are.
  if [[ $r == FAIL ]]; then
    { echo "----- $name: raw TLC output (expected $expect) -----"; tail -n 30 <<<"$out"; echo "----- end $name -----"; } >&2
  fi
  rm -rf "$meta" states "${spec}_TTrace_"*.tla "${spec}_TTrace_"*.bin 2>/dev/null
done

echo "----"
if (( pass + fail == 0 )); then
  echo "no configs matched${filter:+ for filter \"$filter\"}; nothing verified" >&2
  exit 2
fi
echo "$pass passed, $fail failed"
[[ $fail -eq 0 ]]
