# Contributing

## Building from Source

```bash
git clone https://github.com/gofractally/arbtrie.git
cd arbtrie
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -B build/release
cmake --build build/release -j8
```

### Debug Build

```bash
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -B build/debug
cmake --build build/debug -j8
```

### With Sanitizers

```bash
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DENABLE_SANITIZER=ON -B build/tsan
cmake --build build/tsan -j8
```

This enables ThreadSanitizer for detecting data races.

## Running Tests

```bash
# All tests
./build/release/bin/psitri-tests

# Specific test by name
./build/release/bin/psitri-tests "tree_ops: shared-mode*"

# By tag
./build/release/bin/psitri-tests "[public-api]"

# Exclude slow/crash tests
./build/release/bin/psitri-tests "~[crash]" "~[!mayfail]"
```

Tests use [Catch2](https://github.com/catchorg/Catch2) as the test framework.

## Project Structure

```
libraries/
  hash/include/hash/       # xxhash, lehmer64 (header-only)
  ucc/include/ucc/         # SIMD lower_bound, hierarchical bitmap (header-only)
  sal/
    include/sal/           # allocator.hpp, smart_ptr.hpp, control_block.hpp
    src/                   # allocator.cpp, block_allocator.cpp
  psitri/
    include/psitri/        # database.hpp, tree_ops.hpp, cursor.hpp,
                           # transaction.hpp, write_session.hpp
    include/psitri/node/   # leaf.hpp, inner.hpp, value_node.hpp
    src/                   # database.cpp, node/leaf.cpp
    tests/                 # All test files
```

## Code Style

- C++20 with Clang 15+ or GCC 12+
- No exceptions in hot paths (COW, tree traversal)
- `snake_case` for functions and variables, `PascalCase` for types
- Header-only where practical (hash, ucc libraries)
- Prefer `std::string_view` over `const std::string&` for key/value parameters

## Running Benchmarks

```bash
# Build and run the bank transaction benchmark
cmake --build build/release -j8 --target bank-bench-psitri
./build/release/bin/bank-bench-psitri \
    --num-accounts=1000000 \
    --num-transactions=10000000 \
    --batch-size=100
```

See [Bank Transaction Benchmark](../benchmarks/bank.md) for full details.

## Code Coverage

See [Code Coverage](coverage.md) for instructions on generating coverage reports.
