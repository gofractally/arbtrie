#!/bin/bash
# Bank transaction benchmark (TPC-B style): all available engines.
# Called by run_all.sh with BIN, OUTDIR, FORCE set.
set -euo pipefail

ACCOUNTS=${BENCH_ACCOUNTS:-1000000}
TRANSACTIONS=${BENCH_TRANSACTIONS:-1000000}
BATCH=${BENCH_BATCH:-1}
SYNC_EVERY=${BENCH_SYNC_EVERY:-0}
TIMEOUT=${BENCH_TIMEOUT:-1800}  # 30 minute cap per engine

ENGINES=""
[ "${HAVE_PSITRI:-0}" = 1 ]         && ENGINES="$ENGINES psitri"
[ "${HAVE_DWAL:-0}" = 1 ]           && ENGINES="$ENGINES dwal"
[ "${HAVE_PSITRIROCKS:-0}" = 1 ]    && ENGINES="$ENGINES psitrirocks"
[ "${HAVE_ROCKSDB:-0}" = 1 ]        && ENGINES="$ENGINES rocksdb"
[ "${HAVE_PSITRIMDBX:-0}" = 1 ]     && ENGINES="$ENGINES psitrimdbx"
[ "${HAVE_MDBX:-0}" = 1 ]           && ENGINES="$ENGINES mdbx"
[ "${HAVE_PSITRI_SQLITE:-0}" = 1 ]  && ENGINES="$ENGINES psitri-sqlite"
[ "${HAVE_SYSTEM_SQLITE:-0}" = 1 ]  && ENGINES="$ENGINES system-sqlite"
[ "${HAVE_TIDESDB:-0}" = 1 ]        && ENGINES="$ENGINES tidesdb"

if [ -z "$ENGINES" ]; then
    echo "  SKIP bank_tpcb: no engines available"
    exit 0
fi

echo "  Engines: $ENGINES"
echo "  Accounts: $ACCOUNTS  Transactions: $TRANSACTIONS  Batch: $BATCH  Timeout: ${TIMEOUT}s"

for engine in $ENGINES; do
    csv="$OUTDIR/${engine}.csv"
    log="$OUTDIR/${engine}.log"

    if [ -f "$csv" ] && [ "${FORCE:-0}" != 1 ]; then
        echo "  SKIP $engine (exists: $csv)"
        continue
    fi

    db_path="/tmp/bench_bank_${engine}_$$"
    echo "  RUN  bank-bench-$engine -> $log"

    timeout "$TIMEOUT" "$BIN/bank-bench-${engine}" \
        --num-accounts="$ACCOUNTS" \
        --num-transactions="$TRANSACTIONS" \
        --batch-size="$BATCH" \
        --sync-every="$SYNC_EVERY" \
        --db-path="$db_path" \
        2>&1 | tee "$log"
    exit_code=${PIPESTATUS[0]}

    if [ "$exit_code" -eq 124 ]; then
        echo "  TIMEOUT after ${TIMEOUT}s — partial results in $log"
    fi

    bash "$(dirname "$0")/../lib/csv_from_bank.sh" < "$log" > "$csv"

    rm -rf "$db_path"
    echo "  DONE $engine (exit=$exit_code)"
    sleep 5
done
