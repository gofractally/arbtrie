#!/bin/bash
# Test runner — remembers which tests passed, records timing, runs fastest first.
# Usage: bash run_tests.sh [--reset]
set -euo pipefail

SCRIPT="$(readlink -f "$0")"
PROJECT_DIR="$(dirname "$SCRIPT")"
STATE_DIR="$PROJECT_DIR/.test_state"
PASSED_FILE="$STATE_DIR/passed.txt"
TIMES_FILE="$STATE_DIR/times.txt"   # format: <seconds_float>\t<test_name>
BIN="$PROJECT_DIR/build/release/bin"

# ── Safety guard ────────────────────────────────────────────────────────────
_gc="git commi""t"; _gp="git pus""h"
forbidden=$(grep -nE "($_gc|$_gp)" "$SCRIPT" 2>/dev/null \
  | grep -v '_gc=\|_gp=\|forbidden=' || true)
if [[ -n "$forbidden" ]]; then
  echo "SAFETY: forbidden commands found in $SCRIPT:"
  echo "$forbidden"
  exit 1
fi

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
  # stdin: one label per line → stdout: sorted by recorded time ascending
  while IFS= read -r name; do
    local t; t=$(get_time "$name")
    printf '%s\t%s\n' "$t" "$name"
  done | sort -t$'\t' -k1 -n | cut -f2-
}

# ── Counters ─────────────────────────────────────────────────────────────────
PASS=0; FAIL=0; SKIP=0; ALREADY=0
FAILED_TESTS=()

# ── Time a command and set $ELAPSED (seconds, 1 decimal) ─────────────────────
time_cmd() {
  local t0 t1
  t0=$(date +%s%N)
  "$@"
  t1=$(date +%s%N)
  ELAPSED=$(awk "BEGIN{printf \"%.1f\", ($t1-$t0)/1000000000}")
}

# ── Run a binary and report pass/fail/crash. Stop at first failure. ──────────
# Usage: _run_one <label> <binary> [args...]
_run_one() {
  local label="$1" bin="$2"; shift 2
  local t0 t1 elapsed result bin_exit
  t0=$(date +%s%N)
  result=$("$bin" "$@" 2>/dev/null | grep -aE "passed|failed" | tail -1 || true)
  bin_exit=${PIPESTATUS[0]}
  t1=$(date +%s%N)
  elapsed=$(awk "BEGIN{printf \"%.1f\", ($t1-$t0)/1000000000}")

  if [[ $bin_exit -ge 128 ]]; then
    printf 'CRASH (signal %d, %.1fs): %s\n' "$((bin_exit-128))" "$elapsed" "$label"
    record_time "$label" "$elapsed"
    ((FAIL++)) || true; FAILED_TESTS+=("$label")
    echo "--- output ---"
    "$bin" "$@" 2>&1 | tail -40
    exit 1
  elif echo "$result" | grep -qE "passed.*0 failed|All tests passed"; then
    printf 'PASS (%.1fs): %s\n' "$elapsed" "$label"
    mark_passed "$label"; record_time "$label" "$elapsed"
    ((PASS++)) || true
  elif echo "$result" | grep -q "failed"; then
    printf 'FAIL (%.1fs): %s  ← %s\n' "$elapsed" "$label" "$result"
    record_time "$label" "$elapsed"
    ((FAIL++)) || true; FAILED_TESTS+=("$label")
    echo "--- output ---"
    "$bin" "$@" 2>&1 | tail -40
    exit 1
  else
    printf 'SKIP: %s (no matching tests)\n' "$label"
    ((SKIP++)) || true
  fi
}

# ── Run sal-tests (no name filter, just run all) ──────────────────────────────
run_sal() {
  local label="sal-tests (all)"
  if already_passed "$label"; then
    printf '  (already passed in %.1fs: %s)\n' "$(get_time "$label")" "$label"
    ((ALREADY++)) || true
    return
  fi
  _run_one "$label" "$BIN/sal-tests"
}

# ── Run a psitri tag group (args are separate Catch2 tokens) ─────────────────
run_tag() {
  local label="$1"; shift   # remaining args go to psitri-tests
  if already_passed "$label"; then
    printf '  (already passed in %.1fs: %s)\n' "$(get_time "$label")" "$label"
    ((ALREADY++)) || true
    return
  fi
  _run_one "$label" "$BIN/psitri-tests" "$@"
}

# ── Run a single named psitri test case ──────────────────────────────────────
run_test() {
  local name="$1"
  if already_passed "$name"; then
    printf '  (already passed in %.1fs: %s)\n' "$(get_time "$name")" "$name"
    ((ALREADY++)) || true
    return
  fi
  _run_one "$name" "$BIN/psitri-tests" "$name"
}

# ════════════════════════════════════════════════════════════════════════════
echo "======================================================================"
echo " sal-tests"
echo "======================================================================"
run_sal

# ── psitri non-database tag groups ───────────────────────────────────────────
echo ""
echo "======================================================================"
echo " psitri-tests: non-database tag groups (fastest first)"
echo "======================================================================"

declare -A TAG_MAP
TAG_MAP["psitri [leaf_node]"]="[leaf_node]"
TAG_MAP["psitri [inner_node]"]="[inner_node]"
TAG_MAP["psitri [inner_node_util]"]="[inner_node_util]"
TAG_MAP["psitri [inner_prefix_node]"]="[inner_prefix_node]"
TAG_MAP["psitri [find_clines]"]="[find_clines]"
TAG_MAP["psitri [cursor]"]="[cursor]"
TAG_MAP["psitri [trie]"]="[trie]"
TAG_MAP["psitri [tree_context]"]="[tree_context]"
TAG_MAP["psitri [remove]"]="[remove]"
TAG_MAP["psitri [collapse]"]="[collapse]"
TAG_MAP["psitri [smart_ptr]"]="[smart_ptr]"
TAG_MAP["psitri [subtree]"]="[subtree]"
TAG_MAP["psitri [recovery]"]="[recovery]"
TAG_MAP["psitri [truncate]"]="[truncate]"
TAG_MAP["psitri [coverage]"]="[coverage]"
TAG_MAP["psitri [fuzz]"]="[fuzz]"
TAG_MAP["psitri [multi_writer]"]="[multi_writer]"
TAG_MAP["psitri [count_keys]"]="[count_keys]"
TAG_MAP["psitri [integrity]"]="[integrity]"
TAG_MAP["psitri [range_remove]"]="[range_remove]"
TAG_MAP["psitri [update_value]"]="[update_value]"

sorted_tag_labels=$(printf '%s\n' "${!TAG_MAP[@]}" | sort_by_time)
while IFS= read -r label; do
  tag="${TAG_MAP[$label]}"
  run_tag "$label" "$tag" "~[public-api]" "~[benchmark]"
done <<< "$sorted_tag_labels"

# ── psitri public-api tests (one process each) ────────────────────────────────
echo ""
echo "======================================================================"
echo " psitri-tests: public-api (fastest first, each in its own process)"
echo "======================================================================"

PUBLIC_API_TESTS=(
  "write_cursor basic CRUD"
  "write_cursor read_cursor iteration"
  "write_cursor get into buffer"
  "transaction commit persists root"
  "transaction abort discards changes"
  "transaction destructor aborts"
  "transaction sub_transaction commit"
  "transaction sub_transaction abort"
  "transaction read_cursor snapshot"
  "session get_root empty returns null"
  "session set_root and get_root round-trip"
  "multiple independent roots"
  "database reopen preserves data"
  "bulk insert and verify"
  "bulk insert then remove all"
  "remove does not leak references - repeated insert/remove cycles"
  "remove nonexistent keys does not leak references"
  "batched remove across multiple transactions"
  "remove same key repeatedly does not leak references"
  "high-frequency insert/remove on overlapping keys"
  "insert-removeall-reinsert 3 keys"
  "insert-removeall-reinsert 50 keys"
  "insert-removeall-reinsert 500 keys"
  "insert-removeall-reinsert 500 keys with large values"
  "insert-removeall-reinsert 5000 keys"
  "50 cycles of insert-removeall 500 keys"
  "5 cycles insert-removeall 500 keys with large values"
  "interleaved insert and remove in same transaction"
  "remove half then reinsert"
  "batched remove 100 at a time from 5000"
  "leak: insert then release root leaves zero allocated objects"
  "leak: insert large values then release root leaves zero allocated"
  "leak: insert-release-reinsert cycles via root release"
  "leak: insert-remove-reinsert cycles have no cumulative leaks"
  "leak: interleaved insert/remove in same tx - no orphans"
  "leak: interleaved insert and range_remove - no orphans"
  "leak: remove-all sequential leaves zero allocated objects"
  "leak: remove-all reverse order leaves zero allocated objects"
  "leak: remove-all random order leaves zero allocated objects"
  "leak: remove half - reachable equals allocated"
  "leak: large values - remove-all leaves zero allocated"
  "leak: mixed key sizes - remove-all leaves zero allocated"
  "leak: batched remove across transactions - no orphans"
  "leak: range_remove all keys leaves zero allocated"
  "leak: range_remove subset leaves no orphans"
  "leak: range_remove subset scaling diagnosis"
  "leak: range_remove with large values leaves no orphans"
  "leak: repeated range_remove cycles leave zero allocated"
  "leak: snapshot held during remove - no orphans after release"
  "leak: diagnose remove leak scaling"
)

sorted_tests=$(printf '%s\n' "${PUBLIC_API_TESTS[@]}" | sort_by_time)
while IFS= read -r name; do
  run_test "$name"
done <<< "$sorted_tests"

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
