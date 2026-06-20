#!/usr/bin/env bash
#
# Discover and run every TLC config under models/<Model>/, asserting the outcome each config
# declares inline. Add a config to a model's directory and it is picked up automatically.
#
# Each .cfg declares its expected outcome (and any TLC flags) in a comment (keep it the leading
# comment: the first line containing "expect:" / "flags:" wins):
#   \* expect: HOLDS                         -- model checking completes with no error
#   \* expect: VIOLATES INV_SomeInvariant    -- that invariant is violated (a negative control)
#   \* flags: -deadlock                      -- optional; for "holds" runs that reach a terminal state
#
# The model is the parent directory; its single .tla is the spec. Runs happen from that directory.
#
#   ./run.sh            check every config; one PASS/FAIL line each, non-zero exit on any failure
#   ./run.sh genfence   only configs whose path contains the filter
#
# Needs a JRE and tla2tools.jar (v1.8.0+) at this folder's root:
#   https://github.com/tlaplus/tlaplus/releases
#
set -u
cd "$(dirname "$0")" || exit 2

JAR="$PWD/tla2tools.jar"
if [[ ! -f $JAR ]]; then
  echo "tla2tools.jar not found. Download v1.8.0+ from" >&2
  echo "  https://github.com/tlaplus/tlaplus/releases  (place it at this folder's root)" >&2
  exit 2
fi

# value after "<keyword>:" on the first comment line that has it, trimmed; empty if absent
directive() {
  local v
  v=$(grep -E "$1:" "$2" 2>/dev/null | head -1) || true
  [[ -z $v ]] && return
  v=${v#*:}
  v="${v#"${v%%[![:space:]]*}"}"   # ltrim
  v="${v%"${v##*[![:space:]]}"}"   # rtrim
  printf '%s' "$v"
}

filter="${1:-}"
pass=0
fail=0

for cfg in */*.cfg; do
  [[ -n $filter && $cfg != *"$filter"* ]] && continue
  model="${cfg%%/*}"
  base="$(basename "$cfg")"
  name="${base%.cfg}"

  expect="$(directive expect "$cfg")"
  flags="$(directive flags "$cfg")"
  tla="$(cd "$model" && ls -1 *.tla 2>/dev/null | head -1)"

  if [[ -z $expect ]]; then
    printf '%-7s %-22s %-18s %s\n' FAIL "$name" "$model" "no '\\* expect:' directive"
    ((fail++)); continue
  fi

  out="$( (cd "$model" && java -cp "$JAR" tlc2.TLC $flags -config "$base" "$tla") 2>&1 )"

  case "$expect" in
    HOLDS)
      grep -q "Model checking completed. No error has been found." <<<"$out" && result=PASS || result=FAIL ;;
    VIOLATES\ *)
      want="${expect#VIOLATES }"
      grep -q "Invariant $want is violated" <<<"$out" && result=PASS || result=FAIL ;;
    *)
      result=FAIL ;;
  esac

  if [[ $result == PASS ]]; then ((pass++)); else ((fail++)); fi
  printf '%-7s %-22s %-18s %s\n' "$result" "$name" "$model" "$expect"
done

echo "----"
echo "$pass passed, $fail failed"
[[ $fail -eq 0 ]]
