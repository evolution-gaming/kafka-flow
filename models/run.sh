#!/usr/bin/env bash
#
# Discover and run every MC_*.tla model-check wrapper under models/, asserting the outcome each
# wrapper declares inline. One folder per spec, NESTED after the refinement tower (a refining
# model lives under the spec it refines: Cassandra/ under SingleWriterStore/, CasFirstWrite/
# under CasFirstWriteAtomic/, ...). A wrapper is the standard MC pair beside the module it
# EXTENDS -- MC_<name>.tla (the module + the declared expectation) and MC_<name>.cfg (its TLC
# configuration) -- so the VS Code TLA+ extension model-checks any wrapper out of the box:
#
#   \* expect: HOLDS                          -- model checking completes with no error
#   \* expect: VIOLATES INV_Some               -- that state invariant is violated (a safety control)
#   \* expect: VIOLATES-TEMPORAL Prop          -- that temporal property is violated (a liveness control)
#   \* expect: VIOLATES-REFINEMENT Prop        -- the refinement (step simulation) fails: a rejected
#                                                design, or a removed fence -- Impl does NOT imply the spec
#   \* flags:  -deadlock                       -- optional; for a HOLDS run that reaches a terminal state
#
#   ./run.sh                check every wrapper; one PASS/FAIL line each, non-zero exit on any failure
#   ./run.sh refines        only wrappers whose name contains the filter
#
# Outcomes are asserted by TLC EXIT CODE alone (tlc2.output.EC$ExitStatus -- stable API since 2.15);
# TLC's output text is never parsed, only dumped raw when a run FAILs. Expected codes:
#
#   HOLDS -> 0    VIOLATES -> 12 (safety)    VIOLATES-TEMPORAL / VIOLATES-REFINEMENT -> 13
#   (an action-property/step-simulation violation exits 13, verified on the pinned TLC;
#    11 = unexpected deadlock, 10 = ASSUME failure, 150/151 = parse error -- all FAIL any expectation)
#
# The exit code carries the violation's CLASS, not its name -- the property name in the directive is
# documentation. Identity is structural: an expected-violation config declares exactly ONE NAME of
# the expected class (one invariant for VIOLATES, one property for VIOLATES-TEMPORAL/-REFINEMENT),
# so the exit code can only mean that one. Enforced below by counting names, not declaration lines
# -- TLC's plural forms take several names on a line. A co-check of the OTHER class may ride along:
# it exits with a different code, which fails the run rather than being mistaken for the expected
# violation. That is why the negative controls carry no TypeOK -- their paired positive does.
#
# Needs a JRE. tla2tools.jar is downloaded to this folder on first run if missing (git-ignored);
# override the version/URL with the JAR_VERSION/JAR_URL vars below. Pinned to tlaplus release v1.7.4
# (self-reports "TLC2 Version 2.19 ... rev: 5a47802") for reproducibility; exit codes are stable
# across TLC versions, so a bump needs only a full suite re-run, no matcher surgery.
set -u
cd "$(dirname "$0")" || exit 2
# TLC scratch lives under states/; reap it however we exit (Ctrl-C included). Per-run metadirs
# beneath it are removed as each wrapper finishes, so a concurrent run is unaffected.
trap 'rm -rf "$PWD/states" 2>/dev/null' EXIT

# every folder holding a spec goes on TLC's module path, so cross-folder EXTENDS/INSTANCE
# resolve. NOTE: TLC honors -DTLA-Library only for a spec given by ABSOLUTE path, hence $PWD
# everywhere below.
TLA_LIBRARY_PATH="$(find "$PWD" -name '*.tla' ! -name 'MC_*' -exec dirname {} \; | sort -u | paste -sd ':' -)"

wrappers() {  # every MC_*.tla under models/, absolute paths, stable order
  find "$PWD" -name 'MC_*.tla' | sort
}

JAR="$PWD/tla2tools.jar"
JAR_VERSION="v1.7.4"
JAR_URL="https://github.com/tlaplus/tlaplus/releases/download/${JAR_VERSION}/tla2tools.jar"

directive() {  # value after a leading "\* <keyword>:" directive line, trimmed; empty if absent.
  # Anchored to the directive form (keyword is the first token after the comment marker), so a prose
  # comment that merely mentions "expect:"/"flags:" cannot be mistaken for the directive.
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

declared_names() {  # names declared in $2's sections whose keyword matches $1
  # Section-scoped, not line-scoped: TLC's cfg grammar is token-based, so names may follow the
  # keyword on continuation lines (`INVARIANTS A\n  B` declares both). Collect tokens until the
  # next cfg keyword, ignoring `\*` comments.
  awk -v want="$1" '
    function isKeyword(t) {
      return t ~ /^(SPECIFICATION|INIT|NEXT|INVARIANTS?|PROPERT(Y|IES)|CONSTANTS?|CONSTRAINTS?|ACTION_CONSTRAINTS?|SYMMETRY|VIEW|ALIAS|POSTCONDITION|CHECK_DEADLOCK|TYPE|TYPE_CONSTRAINT)$/
    }
    { sub(/\\\*.*/, "") }
    { for (i = 1; i <= NF; i++) {
        if (isKeyword($i)) { cur = $i; continue }
        if (cur ~ want) print $i } }' "$2"
}

filter="${1:-}"

# Lamport margin: the Toolbox right-margin default (EDITOR_RIGHT_MARGIN_DEFAULT = 77) is the
# suite's line-width cap; ASCII-only keeps that width unambiguous (a multi-byte glyph makes
# "column" depend on the locale). Enforced here so neither convention erodes.
# LC_ALL=C: count bytes, and make [:print:] mean ASCII-printable so any byte >= 0x80 is caught
# (awk has no portable \xNN escape -- hence grep for the ASCII check).
offenders="$( { find "$PWD" \( -name '*.tla' -o -name '*.cfg' \) ! -path '*/states/*' \
                  -exec env LC_ALL=C awk 'length > 77 {print FILENAME ":" FNR ": over 77 columns"}' {} +
                env LC_ALL=C grep -rn '[^[:print:][:space:]]' --include='*.tla' --include='*.cfg' \
                  --exclude-dir=states "$PWD" | cut -d: -f1,2 | sed 's/$/: non-ASCII character/'
              } )"
if [[ -n $offenders ]]; then
  echo "formatting violations (77-column margin, ASCII only):" >&2
  echo "$offenders" >&2
  exit 2
fi

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

pass=0; fail=0

failure() {  # print a FAIL line + the raw TLC output (diagnosis only, never assertion)
  printf '%-7s %-38s %s\n' FAIL "$1" "$2"
  ((fail++))
  if [[ -n "${3:-}" ]]; then
    { echo "----- $1: raw TLC output -----"; tail -n 30 <<<"$3"; echo "----- end $1 -----"; } >&2
  fi
}

while IFS= read -r mc; do
  name="$(basename "$mc" .tla)"
  [[ -n $filter && $name != *"$filter"* ]] && continue
  dir="$(dirname "$mc")"
  expect="$(directive expect "$mc")"
  read -r -a flags <<<"$(directive flags "$mc")"   # optional; word-split into argv (empty -> no arg)

  if [[ -z $expect ]]; then
    failure "$name" "missing '\\* expect:' directive"; continue
  fi

  cfg="$dir/$name.cfg"
  if [[ ! -s $cfg ]]; then
    failure "$name" "missing or empty sibling $name.cfg"; continue
  fi

  case "$expect" in
    HOLDS) want=0; class="" ;;
    VIOLATES-TEMPORAL\ *|VIOLATES-REFINEMENT\ *) want=13; class='^PROPERT'; kind=property ;;
    VIOLATES\ *) want=12; class='^INVARIANTS?$'; kind=invariant ;;
    *) failure "$name" "unknown expect '$expect'"; continue ;;
  esac

  # Only -deadlock is a supported flag. Anything else (-simulate, -depth, -continue, ...) would
  # change what "checked" means while the run still reported PASS.
  for f in ${flags[@]+"${flags[@]}"}; do
    [[ $f == -deadlock ]] || { failure "$name" "unsupported flag '$f' (only -deadlock)"; continue 2; }
  done

  if [[ -z $class ]]; then
    # A HOLDS config that declares nothing checks nothing, and exits 0 either way.
    if [[ -z "$(declared_names '^(INVARIANTS?|PROPERT(Y|IES))$' "$cfg")" ]]; then
      failure "$name" "HOLDS but declares no invariant or property -- it would pass vacuously"
      continue
    fi
  else
    # The structural-identity guard (see header). Compare the declared NAMES of the expected class
    # against the directive: exactly one, and the one named. Names are collected per SECTION, not
    # per line -- TLC's cfg grammar lets names follow the keyword on continuation lines, so a
    # line- or keyword-line-scoped count would miss a second same-class check riding underneath
    # and let the exit code be attributed to the wrong property.
    declared="$(declared_names "$class" "$cfg" | tr '\n' ' ')"
    if [[ "${declared% }" != "${expect#* }" ]]; then
      failure "$name" "declares ${kind}s [${declared% }]; expected exactly '${expect#* }'"
      continue
    fi
  fi

  # Unique metadir per run: TLC's default scratch dir is states/<spec>/<timestamp-to-the-second>,
  # so two runs of the SAME spec finishing within one second collide ("directory already exists").
  # Single worker by default: multi-worker liveness checking has crashed in some environments
  # (FileNotFoundException .../nodes_0), falsely failing genuinely-HOLDS temporal configs; the
  # suite is tiny, so serial is the reproducible default -- override with WORKERS=auto locally
  # (passes on the pinned v1.7.4 here, but serial stays the default for reproducibility).
  # TLC_JAVA_OPTS: optional JVM tuning (heap etc.); default none so CI runners are not over-asked.
  meta="$PWD/states/$name.$$"
  # shellcheck disable=SC2086  # TLC_JAVA_OPTS is intentionally word-split
  out="$(java ${TLC_JAVA_OPTS:-} -DTLA-Library="$TLA_LIBRARY_PATH" -cp "$JAR" tlc2.TLC \
          -workers "${WORKERS:-1}" -metadir "$meta" "${flags[@]+"${flags[@]}"}" \
          -config "$cfg" "$mc" 2>&1)"
  code=$?

  if [[ $code -eq $want ]]; then
    ((pass++))
    printf '%-7s %-38s %s\n' PASS "$name" "$expect"
  else
    failure "$name" "$expect (want exit $want, got $code)" "$out"
  fi
  # only this run's metadir -- a concurrent ./run.sh owns its own under the shared states/ tree,
  # which the EXIT trap reaps
  rm -rf "$meta" 2>/dev/null
done < <(wrappers)

echo "----"
if (( pass + fail == 0 )); then
  echo "no wrappers matched${filter:+ for filter \"$filter\"}; nothing verified" >&2
  exit 2
fi
echo "$pass passed, $fail failed"
[[ $fail -eq 0 ]]
