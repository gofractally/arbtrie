#!/bin/bash
# TATP telecom benchmark: PsiTri-SQLite vs system SQLite, PsiTri-DuckDB vs DuckDB.
# Called by run_all.sh with BIN, OUTDIR, FORCE set.
set -euo pipefail

SUBSCRIBERS=${BENCH_SUBSCRIBERS:-10000}
DURATION=${BENCH_DURATION:-10}

run_tatp() {
    local name=$1
    local binary=$2
    shift 2
    local csv="$OUTDIR/${name}.csv"
    local log="$OUTDIR/${name}.log"

    if [ -f "$csv" ] && [ "${FORCE:-0}" != 1 ]; then
        echo "  SKIP $name (exists: $csv)"
        return
    fi

    if [ ! -x "$binary" ]; then
        echo "  SKIP $name (binary not found: $binary)"
        return
    fi

    echo "  RUN  $name -> $log"

    "$binary" "$@" \
        --subscribers "$SUBSCRIBERS" \
        --duration "$DURATION" \
        2>&1 | tee "$log"

    # Parse TATP output into CSV
    # Header line:  "Transaction                  Count      TPS  Avg(us) ..."
    # Data lines:   "GetSubscriberData         35000   3500.0     1234 ..."
    echo "transaction,count,tps,avg_us,p50_us,p99_us" > "$csv"
    # Extract total TPS
    total_tps=$(grep 'Total TPS' "$log" | grep -oP '[\d.]+' | tail -1 || echo "")
    [ -n "$total_tps" ] && echo "TOTAL,,$total_tps,,," >> "$csv"
    # Extract per-transaction lines (skip header, match lines with numbers)
    awk '/^[A-Z][a-zA-Z]+\s/ && /[0-9]/ {
        name=$1; count=$2; tps=$3; avg=$4; p50=$5; p99=$6
        printf "%s,%s,%s,%s,%s,%s\n", name, count, tps, avg, p50, p99
    }' "$log" >> "$csv"

    echo "  DONE $name"
    sleep 5
}

# PsiTri-SQLite (sync=off)
if [ "${HAVE_TATP:-0}" = 1 ]; then
    run_tatp psitri_sqlite_off "$BIN/tatp-bench" --engine sqlite --sync off
    run_tatp psitri_sqlite_full "$BIN/tatp-bench" --engine sqlite --sync full
    run_tatp psitri_duckdb "$BIN/tatp-bench" --engine psitri
    run_tatp duckdb_native "$BIN/tatp-bench" --engine duckdb
fi

# System SQLite
if [ "${HAVE_TATP_SQLITE:-0}" = 1 ]; then
    run_tatp system_sqlite_off "$BIN/tatp-bench-system-sqlite" --engine sqlite --sync off
    run_tatp system_sqlite_full "$BIN/tatp-bench-system-sqlite" --engine sqlite --sync full
fi
