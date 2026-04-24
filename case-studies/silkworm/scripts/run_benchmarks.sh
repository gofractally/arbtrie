#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build/Release"
RESULTS_DIR="${SCRIPT_DIR}/../results"

echo "=== Building eth-state-bench ==="
cmake -B "$BUILD_DIR" -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SILKWORM_CASE_STUDY=ON \
    "$ROOT_DIR/external/psitri"
cmake --build "$BUILD_DIR" --target eth-state-bench

echo ""
echo "=== Running psitrimdbx benchmark ==="
"$BUILD_DIR/bin/eth-state-bench" \
    --blocks 500 --ops 2000 \
    --dir /tmp/eth_bench_psitri \
    | tee "$RESULTS_DIR/psitrimdbx_results.txt"

echo ""
echo "Results saved to $RESULTS_DIR/"
