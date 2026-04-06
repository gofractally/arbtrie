#!/bin/bash
# KV operations benchmark: all available engines.
# Called by run_all.sh with BIN, OUTDIR, FORCE set.
set -euo pipefail

NUM=${BENCH_NUM:-1000000}
BATCH=${BENCH_BATCH:-512}
VALUE_SIZE=${BENCH_VALUE_SIZE:-8}

run_kv_bench() {
    local engine=$1
    local binary="$BIN/kv-bench-${engine}"
    local csv="$OUTDIR/${engine}.csv"
    local log="$OUTDIR/${engine}.log"

    if [ -f "$csv" ] && [ "${FORCE:-0}" != 1 ]; then
        echo "  SKIP $engine (exists: $csv)"
        return
    fi

    if [ ! -x "$binary" ]; then
        echo "  SKIP $engine (binary not found)"
        return
    fi

    local db_path="/tmp/bench_kv_${engine}_$$"
    echo "  RUN  kv-bench-$engine -> $log"

    "$binary" \
        --items="$NUM" \
        --batch="$BATCH" \
        --value-size="$VALUE_SIZE" \
        --db-dir="$db_path" \
        2>&1 | tee "$log"

    # Parse kv-bench output into CSV (same format as bank)
    bash "$(dirname "$0")/../lib/csv_from_bank.sh" < "$log" > "$csv"

    rm -rf "$db_path"
    echo "  DONE $engine"
    sleep 5
}

[ "${HAVE_KV_PSITRI:-0}" = 1 ] && run_kv_bench psitri
[ "${HAVE_PSITRIROCKS:-0}" = 1 ] && [ -x "$BIN/kv-bench-psitrirocks" ] && run_kv_bench psitrirocks
[ "${HAVE_KV_ROCKS:-0}" = 1 ]  && run_kv_bench rocksdb
[ "${HAVE_KV_MDBX:-0}" = 1 ]   && run_kv_bench mdbx
[ -x "$BIN/kv-bench-tidesdb" ]  && run_kv_bench tidesdb
