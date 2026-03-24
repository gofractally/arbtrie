# Testing & Benchmarking Framework Improvement Plan

## Context

psitri has ~170 TEST_CASE entries. TidesDB has 350+ test functions with deeper coverage in several areas. libmdbx uses intensive stress/stochastic tests plus full sanitizer integration. Another agent is building a fuzz tester with oracle baseline comparison, and another is working on code coverage reports. This plan covers the remaining gaps.

## Remaining Gaps (after fuzz + coverage work)

### 1. Cursor/Iterator Robustness Tests (HIGH PRIORITY)
**psitri gap**: 6 basic cursor tests vs TidesDB's 30+ iterator tests

New file: `libraries/psitri/tests/cursor_tests.cpp`
- Bidirectional iteration: forward then reverse, verify consistency
- Seek to non-existent keys, seek past end, seek before begin
- Lower bound edge cases: boundary keys, empty ranges, single-element trees
- Cursor after reopen: write data, close DB, reopen, verify cursor works
- Cursor snapshot isolation: writer mutates while reader iterates
- Large value iteration: cursor over entries with large inline and value_node values
- Empty tree cursor: all cursor operations on empty tree
- Prefix key iteration: keys that are prefixes of each other
- Single-key tree: all cursor ops on tree with exactly 1 entry

### 2. Data Integrity Verification Tests (HIGH PRIORITY)
**psitri gap**: Tests operations don't crash, but rarely verifies data correctness explicitly

New file: `libraries/psitri/tests/integrity_tests.cpp`
- Write-verify cycles: insert N keys, verify all N readable with correct values
- Overwrite integrity: overwrite keys, verify only latest value visible
- Delete integrity: delete subset, verify deleted gone + remaining intact
- Mixed operation integrity: random insert/update/delete sequence, verify final state
- Transaction isolation: concurrent transactions see correct snapshots
- Persistence integrity: write/close/reopen/verify (multiple patterns - small/large datasets)
- Large dataset integrity: 100K+ keys, verify all survive correctly
- Subtree value integrity: write subtrees, verify they survive operations + reopen

### 3. Edge Case Tests (MEDIUM)
**psitri gap**: Sporadic edge cases vs TidesDB's systematic boundary testing

New file: `libraries/psitri/tests/edge_case_tests.cpp`
- Empty/null values: insert, retrieve, update, remove empty values
- Maximum key sizes: keys at size limits
- Single-byte keys: minimal keys
- Duplicate operations: double-insert, double-delete, insert-after-delete same key
- Keys as prefixes of each other ("a", "ab", "abc")
- Extreme key skew: all keys sharing long common prefix
- Single-key tree: all CRUD operations
- Rapid insert-delete same key (churn)

### 4. Sanitizer Build Targets (MEDIUM)
**psitri gap**: Only TSAN; libmdbx has ASan, UBSan, leak, memcheck

Modify `CMakeLists.txt`:
- Add `ENABLE_ASAN` option → `-fsanitize=address -fno-omit-frame-pointer`
- Add `ENABLE_UBSAN` option → `-fsanitize=undefined`
- Apply to psitri library + all test targets (same pattern as existing `ENABLE_SANITIZER`)
- Ensure mutual exclusivity with TSAN (can't combine ASan + TSAN)

### 5. Concurrency Stress Tests (LOWER - may overlap with fuzz tester)
Depending on what the fuzz tester covers, may need:

New file: `libraries/psitri/tests/stress_tests.cpp`
- High thread count (8-16 threads) mixed insert/remove/read
- Writer-reader stress: writers mutating while readers iterate
- Concurrent subtree operations across threads
- Long-running configurable-duration stress test

## Files to Modify
- `libraries/psitri/tests/CMakeLists.txt` - Add new test source files
- `CMakeLists.txt` - Add ASan/UBSan options
- New files:
  - `libraries/psitri/tests/cursor_tests.cpp`
  - `libraries/psitri/tests/integrity_tests.cpp`
  - `libraries/psitri/tests/edge_case_tests.cpp`
  - `libraries/psitri/tests/stress_tests.cpp` (if not covered by fuzz tester)

## Verification
1. `cmake --build build/debug --target psitri-tests`
2. `./bin/psitri-tests [cursor]`
3. `./bin/psitri-tests [integrity]`
4. `./bin/psitri-tests [edge_case]`
5. `./bin/psitri-tests [stress]`
6. ASan build: `cmake -DENABLE_ASAN=ON -DCMAKE_BUILD_TYPE=Debug ..` then run tests
7. UBSan build: `cmake -DENABLE_UBSAN=ON -DCMAKE_BUILD_TYPE=Debug ..` then run tests
