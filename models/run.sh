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
set -u
cd "$(dirname "$0")" || exit 2

JAR="$PWD/tla2tools.jar"
JAR_VERSION="v1.8.0"
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

  out="$(java -cp "$JAR" tlc2.TLC -workers "${WORKERS:-auto}" "${flags[@]+"${flags[@]}"}" -config "$cfg" "$spec.tla" 2>&1)"

  case "$expect" in
    HOLDS)
      grep -q "Model checking completed. No error has been found." <<<"$out" && r=PASS || r=FAIL ;;
    VIOLATES-TEMPORAL\ *)
      grep -q "Temporal property ${expect#VIOLATES-TEMPORAL } was violated" <<<"$out" && r=PASS || r=FAIL ;;
    VIOLATES-REFINEMENT\ *)
      # a refinement (step-simulation) failure: TLC reports the violated abstract module + location,
      # not the property's alias, so we match that signature rather than the declared name.
      grep -Eq "Action property .* of module .* is violated" <<<"$out" && r=PASS || r=FAIL ;;
    VIOLATES\ *)
      grep -q "Invariant ${expect#VIOLATES } is violated" <<<"$out" && r=PASS || r=FAIL ;;
    *) r=FAIL ;;
  esac

  if [[ $r == PASS ]]; then ((pass++)); else ((fail++)); fi
  printf '%-7s %-26s %-18s %s\n' "$r" "$name" "$spec" "$expect"
  rm -rf states "${spec}_TTrace_"*.tla "${spec}_TTrace_"*.bin 2>/dev/null
done

echo "----"
if (( pass + fail == 0 )); then
  echo "no configs matched${filter:+ for filter \"$filter\"}; nothing verified" >&2
  exit 2
fi
echo "$pass passed, $fail failed"
[[ $fail -eq 0 ]]
