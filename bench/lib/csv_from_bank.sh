#!/bin/bash
# Parse bank-bench text output into CSV.
# Usage: bash bench/lib/csv_from_bank.sh < bank-bench.log > results.csv
#
# Extracts key metrics from printf-formatted output lines like:
#   "  Bulk Load                    0.326 sec    3,060,276 ops/sec"
#   "  Transfer Phase (WO)         26.547 sec      376,691 ops/sec"
#   "  Size: file=1088.0 MB  live=976.0 MB  free=112.0 MB  reachable=303.5 MB"
set -euo pipefail

echo "metric,duration_sec,ops_per_sec,file_mb,live_mb,free_mb,reachable_mb"

# State
file_mb="" live_mb="" free_mb="" reachable_mb=""

while IFS= read -r line; do
    # Match performance lines: "  <Name>       <sec> sec    <ops> ops/sec"
    if echo "$line" | grep -q 'ops/sec'; then
        metric=$(echo "$line" | sed 's/^\s*//' | awk -F'  +' '{print $1}' | sed 's/\s*$//')
        secs=$(echo "$line" | grep -oP '[\d.]+(?=\s+sec)' || echo "")
        ops=$(echo "$line" | grep -oP '[\d,]+(?=\s+ops/sec)' | tr -d ',' || echo "")
        if [ -n "$secs" ] && [ -n "$ops" ]; then
            echo "$metric,$secs,$ops,,,,"
        fi
    fi

    # Match size lines: "  Size: file=1088.0 MB  live=976.0 MB ..."
    if echo "$line" | grep -q 'Size: file='; then
        file_mb=$(echo "$line" | grep -oP 'file=[\d.]+' | cut -d= -f2 || echo "")
        live_mb=$(echo "$line" | grep -oP 'live=[\d.]+' | cut -d= -f2 || echo "")
        free_mb=$(echo "$line" | grep -oP 'free=[\d.]+' | cut -d= -f2 || echo "")
        reachable_mb=$(echo "$line" | grep -oP 'reachable=[\d.]+' | cut -d= -f2 || echo "")
        echo "db_size,,,$file_mb,$live_mb,$free_mb,$reachable_mb"
    fi

    # Match summary TPS lines: "  Write-only: 376,691 tx/sec" or "  Write+Read: 250,564 tx/sec"
    # Skip per-round progress lines (contain " / " like "1,000,000 / 10,000,000")
    if echo "$line" | grep -q 'tx/sec' && ! echo "$line" | grep -q ' / '; then
        metric=$(echo "$line" | sed 's/^\s*//' | awk -F: '{print $1}')
        ops=$(echo "$line" | grep -oP '[\d,]+(?=\s+tx/sec)' | tr -d ',' || echo "")
        if [ -n "$ops" ]; then
            echo "$metric,,$ops,,,,"
        fi
    fi

    # Match reader lines: "  Reader:     1,719,249 reads/sec"
    if echo "$line" | grep -q 'reads/sec'; then
        ops=$(echo "$line" | grep -oP '[\d,]+(?=\s+reads/sec)' | tr -d ',' || echo "")
        if [ -n "$ops" ]; then
            echo "Reader,,$ops,,,,"
        fi
    fi
done
