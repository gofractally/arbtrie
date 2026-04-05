#!/bin/bash
# Coverage runner — dynamically discovers test binaries and tags, runs each
# group individually, tracks results, generates coverage report with a summary
# of what was included/excluded.
#
# Usage: bash run_coverage.sh [--reset]
#   --reset  Clear cached pass state and re-run everything
#
# Requires: debug build with ENABLE_COVERAGE=ON
#   cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DENABLE_COVERAGE=ON -B build/debug
#   ninja -C build/debug
set -uo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
PROJECT_DIR="$(dirname "$SCRIPT")"
BUILD_DIR="$PROJECT_DIR/build/debug"
BIN="$BUILD_DIR/bin"
STATE_DIR="$PROJECT_DIR/.coverage_state"
RESULTS_FILE="$STATE_DIR/results.txt"     # format: STATUS\tTIME\tLABEL
COVERAGE_DIR="$BUILD_DIR/coverage"

# ── Safety guard ────────────────────────────────────────────────────────────
_gc="git commi""t"; _gp="git pus""h"
forbidden=$(grep -nE "($_gc|$_gp)" "$SCRIPT" 2>/dev/null \
  | grep -v '_gc=\|_gp=\|forbidden=' || true)
if [[ -n "$forbidden" ]]; then
  echo "SAFETY: forbidden commands found in $SCRIPT:"
  echo "$forbidden"
  exit 1
fi

# ── Tags that get special handling ─────────────────────────────────────────
# fuzz: run each test individually for isolation (crash containment)
# public-api: run each test individually for isolation
# multi_writer/multi-writer: run each individually (thread-sensitive)
# benchmark: never run
INDIVIDUAL_TAGS="fuzz|public-api|multi_writer|multi-writer"
SKIP_TAGS="benchmark"

# ── Verify build exists ────────────────────────────────────────────────────
TEST_BINS=()
for b in "$BIN"/*-tests; do
  [[ -x "$b" ]] && TEST_BINS+=("$b")
done

if [[ ${#TEST_BINS[@]} -eq 0 ]]; then
  echo "ERROR: No test binaries found in $BIN"
  echo "Build first with ENABLE_COVERAGE=ON:"
  echo "  cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DENABLE_COVERAGE=ON -B build/debug"
  echo "  ninja -C build/debug"
  exit 1
fi

# ── State management ─────────────────────────────────────────────────────────
if [[ "${1:-}" == "--reset" ]]; then
  rm -rf "$STATE_DIR"
  echo "Coverage state cleared."
fi
mkdir -p "$STATE_DIR"
touch "$RESULTS_FILE"

already_done() { grep -qP "^(PASS|FAIL|CRASH|SKIP)\t" "$RESULTS_FILE" 2>/dev/null && grep -qF "$1" "$RESULTS_FILE" 2>/dev/null; }

record_result() {
  local status="$1" elapsed="$2" label="$3"
  local tmp; tmp=$(mktemp)
  grep -vF "$label" "$RESULTS_FILE" > "$tmp" 2>/dev/null || true
  printf '%s\t%s\t%s\n' "$status" "$elapsed" "$label" >> "$tmp"
  mv "$tmp" "$RESULTS_FILE"
}

# ── Counters ─────────────────────────────────────────────────────────────────
PASS=0; FAIL=0; CRASH=0; SKIP=0; CACHED=0

# ── Run a single test group and record result ────────────────────────────────
run_group() {
  local label="$1" bin="$2"; shift 2

  if already_done "$label"; then
    local prev_status prev_time
    prev_status=$(grep -F "$label" "$RESULTS_FILE" | tail -1 | cut -f1)
    prev_time=$(grep -F "$label" "$RESULTS_FILE" | tail -1 | cut -f2)
    printf '  (cached %s in %ss: %s)\n' "$prev_status" "$prev_time" "$label"
    case "$prev_status" in
      PASS)  ((PASS++)) || true ;;
      FAIL)  ((FAIL++)) || true ;;
      CRASH) ((CRASH++)) || true ;;
      SKIP)  ((SKIP++)) || true ;;
    esac
    ((CACHED++)) || true
    return
  fi

  local t0 t1 elapsed bin_exit result
  t0=$(date +%s%N)
  result=$("$bin" "$@" 2>/dev/null | grep -aE "passed|failed|All tests" | tail -1 || true)
  bin_exit=${PIPESTATUS[0]}
  t1=$(date +%s%N)
  elapsed=$(awk "BEGIN{printf \"%.1f\", ($t1-$t0)/1000000000}")

  if [[ $bin_exit -ge 128 ]]; then
    local sig=$((bin_exit - 128))
    printf 'CRASH (signal %d, %ss): %s\n' "$sig" "$elapsed" "$label"
    record_result "CRASH" "$elapsed" "$label"
    ((CRASH++)) || true
  elif echo "$result" | grep -qE "passed.*0 failed|All tests passed"; then
    local assertions
    assertions=$(echo "$result" | grep -oE '[0-9]+ assertions' | head -1 || echo "")
    printf 'PASS (%ss, %s): %s\n' "$elapsed" "$assertions" "$label"
    record_result "PASS" "$elapsed" "$label"
    ((PASS++)) || true
  elif echo "$result" | grep -q "failed"; then
    printf 'FAIL (%ss): %s  <- %s\n' "$elapsed" "$label" "$result"
    record_result "FAIL" "$elapsed" "$label"
    ((FAIL++)) || true
  else
    printf 'SKIP: %s (no matching tests or no output)\n' "$label"
    record_result "SKIP" "$elapsed" "$label"
    ((SKIP++)) || true
  fi
}

# ════════════════════════════════════════════════════════════════════════════
echo "======================================================================"
echo " Coverage Run — $(date '+%Y-%m-%d %H:%M:%S')"
echo " Discovered binaries: ${TEST_BINS[*]##*/}"
echo "======================================================================"

# Zero counters only on first run (not cached)
if [[ "$CACHED" -eq 0 ]] && ! grep -q "PASS" "$RESULTS_FILE" 2>/dev/null; then
  echo "Zeroing gcda counters..."
  lcov --directory "$BUILD_DIR" --zerocounters --quiet 2>/dev/null || true
fi

# ── Run each test binary ──────────────────────────────────────────────────────
for bin in "${TEST_BINS[@]}"; do
  bin_name=$(basename "$bin")

  # Get available tags for this binary
  TAGS=$("$bin" --list-tags 2>/dev/null | sed -n 's/.*\[\(.*\)\].*/\1/p' || true)

  if [[ -z "$TAGS" ]]; then
    # No tags — run the whole binary as one group
    echo ""
    echo "--- $bin_name ---"
    run_group "$bin_name" "$bin"
    continue
  fi

  # ── Regular tag groups ──────────────────────────────────────────────────────
  REGULAR_TAGS=$(echo "$TAGS" | grep -vE "^($INDIVIDUAL_TAGS|$SKIP_TAGS)$" | grep -v '^!' | sort -u || true)

  if [[ -n "$REGULAR_TAGS" ]]; then
    echo ""
    echo "--- $bin_name: tag groups ---"

    # Build exclude args for tags we handle individually or skip
    EXCLUDE_ARGS=()
    for pattern in fuzz public-api multi_writer multi-writer benchmark; do
      if echo "$TAGS" | grep -qx "$pattern"; then
        EXCLUDE_ARGS+=("~[$pattern]")
      fi
    done

    while IFS= read -r tag; do
      [[ -z "$tag" ]] && continue
      run_group "$bin_name:$tag" "$bin" "[$tag]" ${EXCLUDE_ARGS[@]+"${EXCLUDE_ARGS[@]}"}
    done <<< "$REGULAR_TAGS"
  fi

  # ── Individual tags (fuzz, public-api, multi_writer, multi-writer) ──────────
  for itag_pattern in fuzz public-api multi_writer multi-writer; do
    if echo "$TAGS" | grep -qx "$itag_pattern"; then
      echo ""
      echo "--- $bin_name: [$itag_pattern] (individual tests) ---"

      ITESTS=$("$bin" --list-tests "[$itag_pattern]" 2>/dev/null \
        | sed -n 's/^  \([^ ].*\)/\1/p' || true)

      while IFS= read -r test_name; do
        [[ -z "$test_name" ]] && continue
        run_group "$bin_name:$itag_pattern:$test_name" "$bin" "$test_name"
      done <<< "$ITESTS"
    fi
  done
done

# ════════════════════════════════════════════════════════════════════════════
echo ""
echo "======================================================================"
echo " Generating coverage report..."
echo "======================================================================"

mkdir -p "$COVERAGE_DIR"

# Capture
lcov --directory "$BUILD_DIR" --capture \
  --output-file "$COVERAGE_DIR/coverage.info" \
  --rc branch_coverage=1 --rc function_coverage=1 \
  --ignore-errors inconsistent,inconsistent \
  --ignore-errors range,range --quiet 2>/dev/null

# Remove system/test/third-party paths
lcov --remove "$COVERAGE_DIR/coverage.info" \
  '*/third_party/*' '*/tests/*' '/usr/*' '*/catch2/*' '*/build/*' \
  --output-file "$COVERAGE_DIR/coverage.info" \
  --rc branch_coverage=1 --rc function_coverage=1 \
  --ignore-errors inconsistent,inconsistent --quiet 2>/dev/null

# Generate HTML
genhtml "$COVERAGE_DIR/coverage.info" \
  --output-directory "$COVERAGE_DIR/html" \
  --branch-coverage --function-coverage \
  --ignore-errors category,category \
  --ignore-errors inconsistent,inconsistent --quiet 2>/dev/null

# Extract overall numbers
COVERAGE_SUMMARY=$(lcov --summary "$COVERAGE_DIR/coverage.info" \
  --rc branch_coverage=1 --rc function_coverage=1 2>&1 \
  | grep -E "lines|functions|branches" || true)

# ── Generate summary file ──────────────────────────────────────────────────
SUMMARY_FILE="$COVERAGE_DIR/html/test_summary.txt"
{
  echo "Coverage Test Summary — $(date '+%Y-%m-%d %H:%M:%S')"
  echo "======================================================================"
  echo ""
  echo "Overall Coverage:"
  echo "$COVERAGE_SUMMARY"
  echo ""
  echo "======================================================================"
  echo "Test Results:"
  echo "======================================================================"
  echo ""
  printf '%-8s  %8s  %s\n' "STATUS" "TIME" "TEST"
  printf '%-8s  %8s  %s\n' "------" "----" "----"
  sort -t$'\t' -k1,1 -k3,3 "$RESULTS_FILE" | while IFS=$'\t' read -r status elapsed label; do
    printf '%-8s  %7ss  %s\n' "$status" "$elapsed" "$label"
  done
  echo ""
  echo "======================================================================"
  echo "Summary: $PASS passed, $FAIL failed, $CRASH crashed, $SKIP skipped ($CACHED cached)"
  echo "======================================================================"

  # List tests that crashed or failed (excluded from coverage)
  PROBLEM_TESTS=$(grep -E "^(CRASH|FAIL)" "$RESULTS_FILE" 2>/dev/null || true)
  if [[ -n "$PROBLEM_TESTS" ]]; then
    echo ""
    echo "Tests NOT fully contributing to coverage:"
    echo "$PROBLEM_TESTS" | while IFS=$'\t' read -r status elapsed label; do
      case "$status" in
        CRASH) echo "  [CRASH] $label — partial coverage (up to crash point)" ;;
        FAIL)  echo "  [FAIL]  $label — partial coverage (assertions failed)" ;;
      esac
    done
  fi
} > "$SUMMARY_FILE"

# ── Print final summary ───────────────────────────────────────────────────
echo ""
echo "======================================================================"
echo " Results"
echo "======================================================================"
echo ""
echo "$COVERAGE_SUMMARY"
echo ""
printf 'Tests: %d passed, %d failed, %d crashed, %d skipped' "$PASS" "$FAIL" "$CRASH" "$SKIP"
[[ "$CACHED" -gt 0 ]] && printf ' (%d cached)' "$CACHED"
echo ""

if [[ "$CRASH" -gt 0 || "$FAIL" -gt 0 ]]; then
  echo ""
  echo "Tests with issues (may have incomplete coverage):"
  grep -E "^(CRASH|FAIL)" "$RESULTS_FILE" | while IFS=$'\t' read -r status elapsed label; do
    printf '  [%s] %s\n' "$status" "$label"
  done
fi

echo ""
echo "HTML report: $COVERAGE_DIR/html/index.html"
echo "Test summary: $SUMMARY_FILE"
