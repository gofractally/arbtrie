#!/bin/bash
# PsiTri Benchmark Runner
#
# Usage:
#   bench/run_all.sh [OPTIONS] [SUITE...]
#
# Options:
#   --machine SLUG    Machine name for data directory (default: hostname)
#   --date DATE       Date stamp (default: today YYYY-MM-DD)
#   --build-dir DIR   Build directory (default: build/release)
#   --data-dir DIR    Output base directory (default: docs/data)
#   --force           Re-run even if results exist
#   --dry-run         Show what would run without executing
#
# Suites: bank_tpcb  random_upsert  rocksdb_api  tatp
#   (default: all suites whose executables are available)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Defaults
MACHINE=$(hostname -s 2>/dev/null || hostname)
DATE=$(date +%Y-%m-%d)
BUILD_DIR="build/release"
DATA_DIR="docs/data"
FORCE=0
DRY_RUN=0
SUITES=()

# Parse args
while [ $# -gt 0 ]; do
    case "$1" in
        --machine)   MACHINE="$2"; shift 2 ;;
        --date)      DATE="$2"; shift 2 ;;
        --build-dir) BUILD_DIR="$2"; shift 2 ;;
        --data-dir)  DATA_DIR="$2"; shift 2 ;;
        --force)     FORCE=1; shift ;;
        --dry-run)   DRY_RUN=1; shift ;;
        -*)          echo "Unknown option: $1" >&2; exit 1 ;;
        *)           SUITES+=("$1"); shift ;;
    esac
done

export BIN="$BUILD_DIR/bin"
export FORCE

# Detect engines
source "$SCRIPT_DIR/lib/detect_engines.sh" "$BUILD_DIR"
echo ""

# Default: run all available suites
if [ ${#SUITES[@]} -eq 0 ]; then
    SUITES=(bank_tpcb rocksdb_api kv_suite random_upsert tatp)
fi

echo "=== PsiTri Benchmark Runner ==="
echo "  Machine:   $MACHINE"
echo "  Date:      $DATE"
echo "  Build:     $BUILD_DIR"
echo "  Data:      $DATA_DIR"
echo "  Suites:    ${SUITES[*]}"
echo ""

for suite in "${SUITES[@]}"; do
    OUTDIR="$DATA_DIR/${suite}/${DATE}/${MACHINE}"
    export OUTDIR

    echo "--- Suite: $suite ---"
    echo "  Output: $OUTDIR"

    if [ "$DRY_RUN" = 1 ]; then
        echo "  [dry-run] Would run: $SCRIPT_DIR/suites/${suite}.sh"
        continue
    fi

    mkdir -p "$OUTDIR"

    # Capture machine info
    bash "$SCRIPT_DIR/lib/machine_info.sh" "$BUILD_DIR" > "$OUTDIR/machine.json"

    # Run suite
    suite_script="$SCRIPT_DIR/suites/${suite}.sh"
    if [ -f "$suite_script" ]; then
        bash "$suite_script"
    else
        echo "  WARN: Suite script not found: $suite_script"
    fi

    echo ""
done

echo "=== Complete ==="
