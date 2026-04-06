#!/bin/bash
# Check freshness of benchmark data vs current git HEAD.
#
# Usage: bench/check_freshness.sh [DATA_DIR]
#
# Reports each workload's most recent run, git distance, and staleness status.
set -euo pipefail

DATA_DIR="${1:-docs/data}"
HEAD=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
HEAD_FULL=$(git rev-parse HEAD 2>/dev/null || echo "unknown")

printf "%-25s %-12s %-20s %-10s %s\n" "WORKLOAD" "DATE" "MACHINE" "COMMIT" "STATUS"
printf "%-25s %-12s %-20s %-10s %s\n" "--------" "----" "-------" "------" "------"

found=0
for machine_json in $(find "$DATA_DIR" -name "machine.json" 2>/dev/null | sort -r); do
    found=1
    # Parse path: docs/data/<workload>/<date>/<machine>/machine.json
    rel=${machine_json#$DATA_DIR/}
    workload=$(echo "$rel" | cut -d/ -f1)
    date=$(echo "$rel" | cut -d/ -f2)
    machine=$(echo "$rel" | cut -d/ -f3)

    # Read git commit from machine.json
    commit=$(grep -oP '"git_commit"\s*:\s*"\K[^"]+' "$machine_json" 2>/dev/null || echo "?")

    # Count commits between benchmark and HEAD
    if [ "$commit" != "?" ] && [ "$HEAD_FULL" != "unknown" ]; then
        distance=$(git rev-list --count "${commit}..HEAD" 2>/dev/null || echo "?")
    else
        distance="?"
    fi

    # Determine status
    if [ "$distance" = "?" ]; then
        status="UNKNOWN"
    elif [ "$distance" -eq 0 ]; then
        status="CURRENT"
    elif [ "$distance" -le 10 ]; then
        status="OK ($distance commits)"
    elif [ "$distance" -le 50 ]; then
        status="AGING ($distance commits)"
    else
        status="STALE ($distance commits)"
    fi

    printf "%-25s %-12s %-20s %-10s %s\n" "$workload" "$date" "$machine" "$commit" "$status"
done

if [ "$found" = 0 ]; then
    echo "No benchmark data found in $DATA_DIR"
    echo "Run: bench/run_all.sh"
fi
