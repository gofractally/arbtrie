#!/bin/bash
# Bank transaction benchmark (TPC-B style): all available engines.
# Called by run_all.sh with BIN, OUTDIR, FORCE set.
set -euo pipefail

ACCOUNTS=${BENCH_ACCOUNTS:-1000000}
TRANSACTIONS=${BENCH_TRANSACTIONS:-10000000}
BATCH=${BENCH_BATCH:-100}
SYNC_EVERY=${BENCH_SYNC_EVERY:-100}

ENGINES=""
[ "${HAVE_PSITRI:-0}" = 1 ]      && ENGINES="$ENGINES psitri"
[ "${HAVE_DWAL:-0}" = 1 ]        && ENGINES="$ENGINES dwal"
[ "${HAVE_PSITRIROCKS:-0}" = 1 ] && ENGINES="$ENGINES psitrirocks"
[ "${HAVE_ROCKSDB:-0}" = 1 ]     && ENGINES="$ENGINES rocksdb"
[ "${HAVE_PSITRIMDBX:-0}" = 1 ]  && ENGINES="$ENGINES psitrimdbx"
[ "${HAVE_MDBX:-0}" = 1 ]        && ENGINES="$ENGINES mdbx"
[ "${HAVE_TIDESDB:-0}" = 1 ]     && ENGINES="$ENGINES tidesdb"

if [ -z "$ENGINES" ]; then
    echo "  SKIP bank_tpcb: no engines available"
    exit 0
fi

for engine in $ENGINES; do
    csv="$OUTDIR/${engine}.csv"
    log="$OUTDIR/${engine}.log"

    if [ -f "$csv" ] && [ "${FORCE:-0}" != 1 ]; then
        echo "  SKIP $engine (exists: $csv)"
        continue
    fi

    db_path="/tmp/bench_bank_${engine}_$$"
    echo "  RUN  bank-bench-$engine -> $log"

    "$BIN/bank-bench-${engine}" \
        --num-accounts="$ACCOUNTS" \
        --num-transactions="$TRANSACTIONS" \
        --batch-size="$BATCH" \
        --sync-every="$SYNC_EVERY" \
        --db-path="$db_path" \
        2>&1 | tee "$log"

    bash "$(dirname "$0")/../lib/csv_from_bank.sh" < "$log" > "$csv"

    rm -rf "$db_path"
    echo "  DONE $engine"
    sleep 10
done
