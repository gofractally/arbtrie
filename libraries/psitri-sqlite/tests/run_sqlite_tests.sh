#!/bin/bash
# Run SQLite's TCL test suite against psitri-sqlite testfixture.
#
# Usage:
#   bash run_sqlite_tests.sh                    # Run default test set
#   bash run_sqlite_tests.sh select1 expr func  # Run specific tests
#
# Prerequisites:
#   - Build: ninja -C build/release psitri-testfixture
#   - SQLite source: git clone --depth 1 --branch version-3.51.3 \
#       https://github.com/sqlite/sqlite.git external/sqlite-src
#   - Generated headers: cd external/sqlite-src/bld && ../configure \
#       && make parse.h opcodes.h keywordhash.h

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
TESTFIXTURE="$REPO_ROOT/build/release/libraries/psitri-sqlite/tests/psitri-testfixture"
TESTDIR="$REPO_ROOT/external/sqlite-src/test"

if [ ! -x "$TESTFIXTURE" ]; then
    echo "Error: testfixture not found. Build with: ninja -C build/release psitri-testfixture"
    exit 1
fi

if [ ! -d "$TESTDIR" ]; then
    echo "Error: SQLite test directory not found at $TESTDIR"
    exit 1
fi

# Default test list — core SQL functionality tests
DEFAULT_TESTS="
    select1 select2 select3 select4 select5 select6 select7
    insert insert2 insert3
    update delete delete2
    where where2 where3
    expr func func2
    subquery
    join join2 join3 join4 join5
    sort cast coalesce null
    types types2 types3
    alter alter2 alter3 alter4
    trigger1 trigger2 trigger3
    view
    index index2 index3
    unique check
    autoincr limit like between in
    collate1 collate2 collate3
    distinct
    misc1 misc2 misc3 misc4 misc5
"

if [ $# -gt 0 ]; then
    TESTS="$@"
else
    TESTS="$DEFAULT_TESTS"
fi

total_pass=0
total_fail=0
total_crash=0
failed_tests=""

for test in $TESTS; do
    testfile="$TESTDIR/${test}.test"
    [ -f "$testfile" ] || { printf "%-20s MISSING\n" "$test"; continue; }

    # Each test gets its own temp directory to avoid contamination
    WORKDIR=$(mktemp -d /tmp/psitri_sqltest.XXXXXX)
    output=$(cd "$WORKDIR" && "$TESTFIXTURE" "$testfile" 2>/dev/null) || true

    # Try to parse "N errors out of M tests" summary line
    summary=$(echo "$output" | grep -o '[0-9]* errors out of [0-9]* tests' | head -1)
    if [ -n "$summary" ]; then
        errors=$(echo "$summary" | awk '{print $1}')
        total_tests=$(echo "$summary" | awk '{print $5}')
        pass=$((total_tests - errors))
    else
        # Fall back to counting individual Ok/! lines
        pass=$(echo "$output" | grep -c '^\S.*\.\.\. Ok$' || true)
        errors=$(echo "$output" | grep -c '^! ' || true)
    fi

    total_pass=$((total_pass + pass))
    total_fail=$((total_fail + errors))

    if [ "$pass" -eq 0 ] && [ "$errors" -eq 0 ]; then
        total_crash=$((total_crash + 1))
        printf "%-20s CRASH\n" "$test"
        failed_tests="$failed_tests $test"
    elif [ "$errors" -gt 0 ]; then
        printf "%-20s %4d pass, %3d FAIL\n" "$test" "$pass" "$errors"
        failed_tests="$failed_tests $test"
    else
        printf "%-20s %4d pass\n" "$test" "$pass"
    fi

    rm -rf "$WORKDIR" 2>/dev/null
done

echo "========================================"
printf "TOTAL: %d passed, %d failed, %d crashed\n" "$total_pass" "$total_fail" "$total_crash"
if [ $((total_pass + total_fail)) -gt 0 ]; then
    pct=$((total_pass * 100 / (total_pass + total_fail)))
    printf "Pass rate: %d%%\n" "$pct"
fi
