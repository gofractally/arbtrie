#!/bin/bash
# Test runner — dynamically discovers test binaries and tags, remembers which
# tests passed, records timing, runs fastest first.
#
# Usage: bash run_tests.sh [--reset]
#
# Test binaries are discovered from build/release/bin/*-tests.
# Tags are discovered via --list-tags. Each tag group is run individually.
# [public-api] tests are run as individual test cases for isolation.
# [fuzz] tests are run individually for isolation.
# Tags in EXCLUDE_TAGS are never run as their own group.
set -euo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
PROJECT_DIR="$(dirname "$SCRIPT")"
STATE_DIR="$PROJECT_DIR/.test_state"
PASSED_FILE="$STATE_DIR/passed.txt"
TIMES_FILE="$STATE_DIR/times.txt"   # format: <seconds_float>\t<test_name>
BIN="$PROJECT_DIR/build/release/bin"

# Required by psitri-tests custom main — prevents accidental direct invocation.
export PSITRI_FROM_RUN_TESTS=1

# ── Safety guard ────────────────────────────────────────────────────────────
_gc="git commi""t"; _gp="git pus""h"
forbidden=$(grep -nE "($_gc|$_gp)" "$SCRIPT" 2>/dev/null \
  | grep -v '_gc=\|_gp=\|forbidden=' || true)
if [[ -n "$forbidden" ]]; then
  echo "SAFETY: forbidden commands found in $SCRIPT:"
  echo "$forbidden"
  exit 1
fi

# ── Tags that are run specially (not as simple tag groups) ──────────────────
# public-api: run each test case individually for process isolation
# fuzz: run each test case individually for isolation
# benchmark: never run in test suite
SPECIAL_TAGS="public-api|fuzz|benchmark"

# ── State management ─────────────────────────────────────────────────────────
if [[ "${1:-}" == "--reset" ]]; then
  rm -f "$PASSED_FILE" "$TIMES_FILE"
  echo "State cleared. Starting fresh."
fi

mkdir -p "$STATE_DIR"
touch "$PASSED_FILE" "$TIMES_FILE"

already_passed() { grep -qxF "$1" "$PASSED_FILE" 2>/dev/null; }
mark_passed()    { echo "$1" >> "$PASSED_FILE"; }

get_time() {
  local t
  t=$(grep -F "	$1" "$TIMES_FILE" 2>/dev/null | tail -1 | cut -f1)
  echo "${t:-99999}"
}

record_time() {
  local name="$1" elapsed="$2"
  local tmp; tmp=$(mktemp)
  grep -vF "	$name" "$TIMES_FILE" > "$tmp" 2>/dev/null || true
  printf '%s\t%s\n' "$elapsed" "$name" >> "$tmp"
  mv "$tmp" "$TIMES_FILE"
}

sort_by_time() {
  # stdin: one label per line -> stdout: sorted by recorded time ascending
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    local t; t=$(get_time "$name")
    printf '%s\t%s\n' "$t" "$name"
  done | sort -t$'\t' -k1 -n | cut -f2-
}

# ── Counters ─────────────────────────────────────────────────────────────────
PASS=0; FAIL=0; SKIP=0; ALREADY=0
FAILED_TESTS=()

# ── Run a binary and report pass/fail/crash. Stop at first failure. ──────────
# Usage: _run_one <label> <binary> [args...]
_run_one() {
  local label="$1" bin="$2"; shift 2
  local t0 t1 elapsed result bin_exit
  t0=$(date +%s%N)
  result=$("$bin" "$@" 2>/dev/null | grep -aE "passed|failed|All tests" | tail -1 || true)
  bin_exit=${PIPESTATUS[0]}
  t1=$(date +%s%N)
  elapsed=$(awk "BEGIN{printf \"%.1f\", ($t1-$t0)/1000000000}")

  if [[ $bin_exit -ge 128 ]]; then
    printf 'CRASH (signal %d, %ss): %s\n' "$((bin_exit-128))" "$elapsed" "$label"
    record_time "$label" "$elapsed"
    ((FAIL++)) || true; FAILED_TESTS+=("$label")
    echo "--- output ---"
    "$bin" "$@" 2>&1 | tail -40
    exit 1
  elif echo "$result" | grep -qE "passed.*0 failed|All tests passed"; then
    local assertions
    assertions=$(echo "$result" | grep -oE '[0-9]+ assertions' | head -1 || echo "")
    printf 'PASS (%ss, %s): %s\n' "$elapsed" "$assertions" "$label"
    mark_passed "$label"; record_time "$label" "$elapsed"
    ((PASS++)) || true
  elif echo "$result" | grep -q "failed"; then
    printf 'FAIL (%ss): %s  <- %s\n' "$elapsed" "$label" "$result"
    record_time "$label" "$elapsed"
    ((FAIL++)) || true; FAILED_TESTS+=("$label")
    echo "--- output ---"
    "$bin" "$@" 2>&1 | tail -40
    exit 1
  else
    printf 'SKIP: %s (no matching tests or no output)\n' "$label"
    ((SKIP++)) || true
  fi
}

run_cached() {
  local label="$1"; shift
  if already_passed "$label"; then
    printf '  (already passed in %ss: %s)\n' "$(get_time "$label")" "$label"
    ((ALREADY++)) || true
    return
  fi
  _run_one "$label" "$@"
}

# ── Discover test binaries ──────────────────────────────────────────────────
TEST_BINS=()
for b in "$BIN"/*-tests; do
  [[ -x "$b" ]] && TEST_BINS+=("$b")
done

if [[ ${#TEST_BINS[@]} -eq 0 ]]; then
  echo "ERROR: No test binaries found in $BIN"
  echo "Build first: ninja -C build/release"
  exit 1
fi

echo "======================================================================"
echo " Test Run — $(date '+%Y-%m-%d %H:%M:%S')"
echo " Discovered binaries: ${TEST_BINS[*]##*/}"
echo "======================================================================"

# ── Run each test binary ──────────────────────────────────────────────────────
for bin in "${TEST_BINS[@]}"; do
  bin_name=$(basename "$bin")

  # Get available tags for this binary
  TAGS=$("$bin" --list-tags 2>/dev/null | sed -n 's/.*\[\(.*\)\].*/\1/p' || true)

  if [[ -z "$TAGS" ]]; then
    # No tags — run the whole binary as one group
    echo ""
    echo "--- $bin_name (all) ---"
    run_cached "$bin_name" "$bin"
    continue
  fi

  # ── Regular tag groups (excluding special tags) ────────────────────────────
  REGULAR_TAGS=$(echo "$TAGS" | grep -vE "^($SPECIAL_TAGS)$" | grep -v '^!' | sort -u || true)

  if [[ -n "$REGULAR_TAGS" ]]; then
    echo ""
    echo "--- $bin_name: tag groups ---"

    # Build exclude args for special tags present in this binary
    EXCLUDE_ARGS=()
    for special in benchmark; do
      if echo "$TAGS" | grep -qx "$special"; then
        EXCLUDE_ARGS+=("~[$special]")
      fi
    done

    sorted_tags=$(echo "$REGULAR_TAGS" | while IFS= read -r tag; do
      printf '%s\n' "$bin_name:$tag"
    done | sort_by_time)

    while IFS= read -r label; do
      [[ -z "$label" ]] && continue
      tag="${label#*:}"
      run_cached "$label" "$bin" "[$tag]" "~[public-api]" "~[fuzz]" ${EXCLUDE_ARGS[@]+"${EXCLUDE_ARGS[@]}"}
    done <<< "$sorted_tags"
  fi

  # ── Public-API tests (individual isolation) ─────────────────────────────────
  if echo "$TAGS" | grep -qx "public-api"; then
    echo ""
    echo "--- $bin_name: [public-api] (individual tests) ---"

    API_TESTS=$("$bin" --list-tests "[public-api]" 2>/dev/null | sed -n 's/^  \([^ ].*\)/\1/p' || true)

    if [[ -n "$API_TESTS" ]]; then
      sorted_api=$(echo "$API_TESTS" | while IFS= read -r name; do
        printf '%s\n' "$name"
      done | sort_by_time)

      while IFS= read -r name; do
        [[ -z "$name" ]] && continue
        run_cached "$name" "$bin" "$name"
      done <<< "$sorted_api"
    fi
  fi

  # ── Fuzz tests (individual isolation) ──────────────────────────────────────
  if echo "$TAGS" | grep -qx "fuzz"; then
    echo ""
    echo "--- $bin_name: [fuzz] (individual tests) ---"

    FUZZ_TESTS=$("$bin" --list-tests "[fuzz]" 2>/dev/null | sed -n 's/^  \([^ ].*\)/\1/p' || true)

    if [[ -n "$FUZZ_TESTS" ]]; then
      sorted_fuzz=$(echo "$FUZZ_TESTS" | while IFS= read -r name; do
        printf '%s\n' "$name"
      done | sort_by_time)

      while IFS= read -r name; do
        [[ -z "$name" ]] && continue
        run_cached "$name" "$bin" "$name"
      done <<< "$sorted_fuzz"
    fi
  fi
done

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "======================================================================"
printf " TOTAL: %d passed, %d failed, %d skipped, %d already-passed\n" \
       "$PASS" "$FAIL" "$SKIP" "$ALREADY"
echo "======================================================================"

if [[ ${#FAILED_TESTS[@]} -gt 0 ]]; then
  echo "Failed:"
  for t in "${FAILED_TESTS[@]}"; do echo "  - $t"; done
  exit 1
fi
echo "All tests passed."
