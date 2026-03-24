# Code Coverage

## Quick Start

```bash
# 1. Configure coverage build (one-time)
mkdir -p build/coverage
cd build/coverage
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DENABLE_COVERAGE=ON ../..

# 2. Build
ninja

# 3. Generate report (runs tests + lcov + genhtml)
ninja coverage
open coverage/html/index.html
```

## Manual Report Generation

The `ninja coverage` target may fail if any test calls `abort()` (e.g. a C++
`assert()` fires in debug mode), because the gcov runtime never gets a chance
to flush `.gcda` files. When this happens, generate the report manually:

```bash
cd build/coverage

# Delete stale gcda files
find . -name "*.gcda" -delete

# Run tests, excluding tags that cause abort()
./bin/psitri-tests "~[crash]" "~[!mayfail]"

# Capture coverage
lcov --directory . --capture --output-file coverage/coverage_raw.info \
  --rc branch_coverage=1 --rc function_coverage=1 \
  --ignore-errors graph,empty,corrupt,inconsistent,category,deprecated,format,unused,range \
  --filter range

# Remove external/test files from report
lcov --remove coverage/coverage_raw.info \
  '/opt/homebrew/*' '/usr/include/*' '/usr/local/*' '/Library/*' \
  '*/catch2/*' '*/test/*' \
  --output-file coverage/coverage.info \
  --rc branch_coverage=1 --rc function_coverage=1 \
  --ignore-errors graph,empty,corrupt,inconsistent,category,deprecated,format,unused,range \
  --filter range

# Generate HTML
genhtml coverage/coverage.info --output-directory coverage/html \
  --ignore-errors corrupt,inconsistent,category,range \
  --branch-coverage --function-coverage

open coverage/html/index.html
```

## Common Issues

### No `.gcda` files generated (0% coverage)

**Cause:** A test triggered a C++ `assert()` or `SIGABRT`, killing the process
before the gcov runtime could write `.gcda` files. Unlike Catch2 `REQUIRE`
failures (which are exceptions), a raw `assert()` failure calls `abort()` and
destroys all pending coverage data for the entire run.

**Fix:** Exclude the offending test tags:
```bash
./bin/psitri-tests "~[crash]" "~[!mayfail]"
```

Known tags that can cause `abort()` in debug builds:
- `[crash]` — tests with known SIGABRT behavior (hidden by default via `[.]`)
- `[!mayfail]` — tests that exercise code paths which hit internal assertions

### Coverage numbers look too low

**Cause:** Multiple test runs without clearing `.gcda` files can produce
corrupted/stale data. The gcov counters accumulate across runs, and if a
later run aborts, it can zero out previously-collected data.

**Fix:** Always delete `.gcda` files before a fresh run:
```bash
find . -name "*.gcda" -delete
```

### lcov reports "no .gcda files found"

**Cause:** Either the tests weren't run, or they aborted (see above).
Verify files exist before running lcov:
```bash
find . -name "*.gcda" | wc -l   # should be > 0
```

### `--coverage` not working with Homebrew LLVM/Clang

The project uses Homebrew clang (`/opt/homebrew/opt/llvm/bin/clang++`).
With `--coverage`, this compiler generates gcov-compatible `.gcda`/`.gcno`
files (not LLVM `.profraw` files), so standard `lcov`/`genhtml` work fine.

If you switch compilers, verify coverage instrumentation is present:
```bash
nm bin/psitri-tests | grep gcov   # should show ___gcov_dump etc.
```

## Build Configurations

| Directory | Type | Flags | Use |
|-----------|------|-------|-----|
| `build/release` | Release | `-O3`, LTO | Performance testing |
| `build/coverage` | Debug | `-O0 -g --coverage` | Coverage reports |
| `build/debug` | Debug | `-O0 -g` | Development/debugging |
