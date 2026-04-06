#!/bin/bash
# Random upsert benchmark: DWAL vs RocksDB vs MDBX scaling test.
# Called by run_all.sh with BIN, OUTDIR, FORCE set.
set -euo pipefail

ROUNDS=${BENCH_ROUNDS:-200}
BATCH=${BENCH_BATCH:-100}
VALUE_SIZE=${BENCH_VALUE_SIZE:-256}
PINNED_CACHE=${BENCH_PINNED_CACHE:-256}
TIMEOUT=${BENCH_TIMEOUT:-1800}  # 30 minute cap per engine

if [ "${HAVE_DWAL_BENCH:-0}" != 1 ]; then
    echo "  SKIP random_upsert: dwal-bench not available"
    exit 0
fi

# PsiTri DWAL
csv="$OUTDIR/psitri_dwal.csv"
log="$OUTDIR/psitri_dwal.log"
if [ ! -f "$csv" ] || [ "${FORCE:-0}" = 1 ]; then
    db_path="/tmp/bench_upsert_dwal_$$"
    echo "  RUN  dwal-bench (psitri dwal) -> $log"
    "$BIN/dwal-bench" \
        --rounds "$ROUNDS" --batch "$BATCH" --value-size "$VALUE_SIZE" \
        --mode upsert-rand --dwal --pinned-cache-mb "$PINNED_CACHE" \
        --db-dir "$db_path" --csv-log "$csv" \
        2>&1 | tee "$log"
    rm -rf "$db_path"
    echo "  DONE psitri_dwal"
    sleep 10
fi

# RocksDB (if available via dwal-bench --engine flag or separate binary)
if [ "${HAVE_ROCKSDB:-0}" = 1 ]; then
    csv="$OUTDIR/rocksdb.csv"
    log="$OUTDIR/rocksdb.log"
    if [ ! -f "$csv" ] || [ "${FORCE:-0}" = 1 ]; then
        db_path="/tmp/bench_upsert_rocks_$$"
        echo "  RUN  dwal-bench (rocksdb) -> $log"
        "$BIN/dwal-bench" \
            --rounds "$ROUNDS" --batch "$BATCH" --value-size "$VALUE_SIZE" \
            --mode upsert-rand --engine rocksdb \
            --db-dir "$db_path" --csv-log "$csv" \
            2>&1 | tee "$log"
        rm -rf "$db_path"
        echo "  DONE rocksdb"
        sleep 10
    fi
fi

# MDBX (if available)
if [ "${HAVE_MDBX:-0}" = 1 ]; then
    csv="$OUTDIR/mdbx.csv"
    log="$OUTDIR/mdbx.log"
    if [ ! -f "$csv" ] || [ "${FORCE:-0}" = 1 ]; then
        db_path="/tmp/bench_upsert_mdbx_$$"
        echo "  RUN  dwal-bench (mdbx) -> $log"
        "$BIN/dwal-bench" \
            --rounds "$ROUNDS" --batch "$BATCH" --value-size "$VALUE_SIZE" \
            --mode upsert-rand --engine mdbx \
            --db-dir "$db_path" --csv-log "$csv" \
            2>&1 | tee "$log" || true  # MDBX may fail with MAP_FULL
        rm -rf "$db_path"
        echo "  DONE mdbx"
        sleep 10
    fi
fi
