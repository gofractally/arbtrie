#!/bin/bash
# RocksDB API benchmark: PsiTriRocks vs native RocksDB (same db_bench workloads).
# Called by run_all.sh with BIN, OUTDIR, FORCE set.
set -euo pipefail

NUM=${BENCH_NUM:-1000000}
BATCH=${BENCH_BATCH_ROCKS:-1}
WORKLOADS="fillrandom,readrandom,readseq,overwrite,seekrandom,deleteseq,deleterandom"

run_rocks_bench() {
    local engine=$1
    local binary=$2
    local csv="$OUTDIR/${engine}.csv"
    local log="$OUTDIR/${engine}.log"

    if [ -f "$csv" ] && [ "${FORCE:-0}" != 1 ]; then
        echo "  SKIP $engine (exists: $csv)"
        return
    fi

    if [ ! -x "$binary" ]; then
        echo "  SKIP $engine (binary not found: $binary)"
        return
    fi

    local db_path="/tmp/bench_rocksapi_${engine}_$$"
    echo "  RUN  $engine -> $log"

    "$binary" \
        --db="$db_path" \
        --num="$NUM" \
        --batch_size="$BATCH" \
        --benchmarks="$WORKLOADS" \
        2>&1 | tee "$log"

    # Parse rocks-bench output into CSV
    # Format: "fillrandom    :    1234567.890 micros/op;  810.5 MB/s"
    echo "benchmark,micros_per_op,mb_per_sec,ops_per_sec" > "$csv"
    grep -E '^\w+\s+:' "$log" | while IFS= read -r line; do
        bench=$(echo "$line" | awk '{print $1}')
        micros=$(echo "$line" | grep -oP '[\d.]+(?=\s+micros/op)' || echo "")
        mbps=$(echo "$line" | grep -oP '[\d.]+(?=\s+MB/s)' || echo "")
        if [ -n "$micros" ] && [ "$micros" != "0" ]; then
            ops=$(awk "BEGIN{printf \"%.0f\", 1000000/$micros}")
        else
            ops=""
        fi
        echo "$bench,$micros,$mbps,$ops" >> "$csv"
    done

    rm -rf "$db_path"
    echo "  DONE $engine"
    sleep 5
}

run_rocks_bench psitrirocks "$BIN/psitrirocks-bench"
run_rocks_bench rocksdb     "$BIN/rocksdb-bench"
run_rocks_bench mdbx        "$BIN/mdbx-bench"
