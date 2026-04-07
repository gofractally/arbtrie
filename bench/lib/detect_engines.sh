#!/bin/bash
# Detect which benchmark engines are available in the build.
# Usage: source bench/lib/detect_engines.sh [BUILD_DIR]
# Sets HAVE_* variables and BIN path.

BIN="${1:-build/release}/bin"

HAVE_PSITRI=0;     [ -x "$BIN/bank-bench-psitri" ]      && HAVE_PSITRI=1
HAVE_DWAL=0;       [ -x "$BIN/bank-bench-dwal" ]        && HAVE_DWAL=1
HAVE_PSITRIROCKS=0;[ -x "$BIN/psitrirocks-bench" ]      && HAVE_PSITRIROCKS=1
HAVE_ROCKSDB=0;    [ -x "$BIN/rocksdb-bench" ]           && HAVE_ROCKSDB=1
HAVE_PSITRIMDBX=0; [ -x "$BIN/bank-bench-psitrimdbx" ]  && HAVE_PSITRIMDBX=1
HAVE_MDBX=0;       [ -x "$BIN/bank-bench-mdbx" ]        && HAVE_MDBX=1
HAVE_TIDESDB=0;    [ -x "$BIN/bank-bench-tidesdb" ]      && HAVE_TIDESDB=1
HAVE_TATP=0;       [ -x "$BIN/tatp-bench" ]              && HAVE_TATP=1
HAVE_TATP_SQLITE=0;[ -x "$BIN/tatp-bench-system-sqlite" ] && HAVE_TATP_SQLITE=1
HAVE_DWAL_BENCH=0; [ -x "$BIN/dwal-bench" ]             && HAVE_DWAL_BENCH=1
HAVE_PSITRI_SQLITE=0; [ -x "$BIN/bank-bench-psitri-sqlite" ] && HAVE_PSITRI_SQLITE=1
HAVE_SYSTEM_SQLITE=0; [ -x "$BIN/bank-bench-system-sqlite" ] && HAVE_SYSTEM_SQLITE=1
HAVE_KV_PSITRI=0;  [ -x "$BIN/kv-bench-psitri" ]        && HAVE_KV_PSITRI=1
HAVE_KV_DWAL=0;    [ -x "$BIN/kv-bench-dwal" ]          && HAVE_KV_DWAL=1
HAVE_KV_ROCKS=0;   [ -x "$BIN/kv-bench-rocksdb" ]       && HAVE_KV_ROCKS=1
HAVE_KV_MDBX=0;    [ -x "$BIN/kv-bench-mdbx" ]          && HAVE_KV_MDBX=1

echo "=== Available Engines ==="
echo "  PsiTri (native):  $HAVE_PSITRI"
echo "  DWAL:             $HAVE_DWAL"
echo "  PsiTriRocks:      $HAVE_PSITRIROCKS"
echo "  RocksDB:          $HAVE_ROCKSDB"
echo "  PsiTriMDBX:       $HAVE_PSITRIMDBX"
echo "  MDBX:             $HAVE_MDBX"
echo "  TidesDB:          $HAVE_TIDESDB"
echo "  PsiTri-SQLite:    $HAVE_PSITRI_SQLITE"
echo "  System SQLite:    $HAVE_SYSTEM_SQLITE"
echo "  TATP:             $HAVE_TATP"
echo "  TATP (sys SQLite):$HAVE_TATP_SQLITE"
echo "  DWAL bench:       $HAVE_DWAL_BENCH"
echo "  KV (psitri):      $HAVE_KV_PSITRI"
echo "  KV (rocksdb):     $HAVE_KV_ROCKS"
echo "  KV (mdbx):        $HAVE_KV_MDBX"
