cmake_minimum_required(VERSION 3.16)

# Set CMake policies
if(POLICY CMP0144)
    cmake_policy(SET CMP0144 NEW)
endif()
if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()

# Enforce build type if not specified - place before project()
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Choose the type of build" FORCE)
endif()

# Valid build types
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
    "Debug" "Release" "MinSizeRel" "RelWithDebInfo")

# Print build type for debugging
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# Common compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-vla-extension -fdiagnostics-color=always")

# Set debug flags explicitly - must be before project()
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -UNDEBUG -fno-omit-frame-pointer" CACHE STRING "Flags used by the compiler in Debug builds" FORCE)
    message(STATUS "Setting Debug flags: ${CMAKE_CXX_FLAGS_DEBUG}")
endif()

# Set the C++ compiler to LLVM clang (required — GCC is not supported)
if(APPLE)
    set(CMAKE_CXX_COMPILER "/opt/homebrew/opt/llvm/bin/clang++")
elseif(UNIX AND NOT CMAKE_CXX_COMPILER)
    find_program(CLANGPP clang++ clang++-20 clang++-19 clang++-18 REQUIRED)
    set(CMAKE_CXX_COMPILER "${CLANGPP}")
endif()

# Enable compile commands for IDE support
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Create symlink to compile_commands.json in project root, for IDE support
if(CMAKE_EXPORT_COMPILE_COMMANDS)
    execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink
        ${CMAKE_BINARY_DIR}/compile_commands.json
        ${CMAKE_SOURCE_DIR}/compile_commands.json)
endif()

project(arbtrie VERSION 0.1.0 LANGUAGES CXX C)

# Set global properties
set(CMAKE_CXX_STANDARD 20 CACHE STRING "C++ standard" FORCE)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable Link-Time Optimization (LTO) for release builds
if(CMAKE_BUILD_TYPE MATCHES "^(Release|MinSizeRel|RelWithDebInfo)$")
  message(STATUS "Enabling Link-Time Optimization (LTO)")
  set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
else()
  set(CMAKE_INTERPROCEDURAL_OPTIMIZATION FALSE)
endif()

# Define build options
option(ENABLE_SANITIZER "Enable Thread Sanitizer" OFF)
option(ENABLE_COVERAGE "Enable coverage reporting" OFF)
option(BUILD_TESTS "Build tests" ON)
option(DISABLE_NEON "Disable all ARM NEON SIMD optimizations (for debugging)" OFF)
option(ENABLE_PLATFORM_OPTIMIZATIONS
    "Enable platform-specific optimizations that rely on hardware-defined behavior \
beyond what the C++ standard guarantees. When ON (default), the following techniques \
are used for performance: \
(1) Unaligned memory access — leaf nodes pack uint16_t offsets after variable-length \
byte arrays, producing addresses that may not be 2-byte aligned. On x86-64 and ARM64 \
this is handled transparently by hardware with no performance penalty. \
(2) Pointer arithmetic wraparound — the NEON branch-copy routine uses unsigned \
underflow (N-16 when N<16) to compute a negative offset into caller-provided padding, \
avoiding a branch for the small-N case. \
(3) typed_int<uint16_t> is marked __attribute__((packed)) so the compiler knows these \
values may reside at any alignment. On a single-member struct this changes nothing \
about code generation on modern architectures. \
When OFF, all code paths use standards-conforming alternatives (memcpy for unaligned \
access, scalar fallbacks for small-N NEON). This is fully UBSAN-clean but measurably \
slower on hot paths. Use OFF when running sanitizers to get clean reports, or when \
targeting architectures where unaligned access traps (e.g. some MIPS, older ARM)."
    ON)

if(ENABLE_PLATFORM_OPTIMIZATIONS)
    message(STATUS "Platform optimizations ENABLED (hardware-defined unaligned access, pointer tricks)")
    add_compile_definitions(PSITRI_PLATFORM_OPTIMIZATIONS=1)
else()
    message(STATUS "Platform optimizations DISABLED (strict C++ conformance, UBSAN-clean)")
    add_compile_definitions(PSITRI_PLATFORM_OPTIMIZATIONS=0)
endif()

option(ENABLE_DEEP_INVARIANTS "Enable expensive invariant checks in debug builds" OFF)

if(ENABLE_DEEP_INVARIANTS)
    message(STATUS "Deep invariant checks ENABLED")
    add_compile_definitions(PSITRI_DEEP_INVARIANTS=1)
endif()

option(ENABLE_DEBUG_CURSOR "Enable temporary cursor debug instrumentation" OFF)
if(ENABLE_DEBUG_CURSOR)
    message(STATUS "Cursor debug instrumentation ENABLED")
    add_compile_definitions(PSITRI_DEBUG_CURSOR=1)
endif()

if(DISABLE_NEON)
    message(STATUS "NEON SIMD optimizations DISABLED")
    # Force-include a header that undefs __ARM_NEON before any source code runs
    add_compile_options(-include ${CMAKE_SOURCE_DIR}/libraries/ucc/include/ucc/disable_neon.hpp)
endif()

option(ENABLE_NATIVE_ARCH
    "Compile for the native CPU architecture (-march=native). Enables SSE4.1, SSSE3, AVX2, etc. \
on x86 and Apple Silicon tuning on ARM. Produces binaries that may not run on older CPUs."
    ON)
if(ENABLE_NATIVE_ARCH)
    message(STATUS "Native arch optimizations ENABLED (-march=native)")
    add_compile_options(-march=native)
else()
    message(STATUS "Native arch optimizations DISABLED (portable binary; use -DENABLE_NATIVE_ARCH=ON for best performance)")
endif()

# Find required packages
find_package(Threads REQUIRED)
find_package(SQLite3)
# CMake's FindSQLite3 creates SQLite::SQLite3 on Linux but some installations
# (Homebrew on macOS) provide SQLite3::SQLite3. Normalise to SQLite3::SQLite3.
if(TARGET SQLite::SQLite3 AND NOT TARGET SQLite3::SQLite3)
    add_library(SQLite3::SQLite3 ALIAS SQLite::SQLite3)
endif()
if(SQLite3_FOUND)
    message(STATUS "SQLite3 found -- sqlite-arb-test benchmark will be built")
else()
    message(STATUS "SQLite3 not found -- sqlite-arb-test benchmark will be skipped")
endif()

# Find Boost
set(Boost_NO_WARN_NEW_VERSIONS ON)
find_package(Boost REQUIRED COMPONENTS program_options)

# Only find Catch2 if we're building tests
if(BUILD_TESTS)
    find_package(Catch2 3 REQUIRED)
    enable_testing()
endif()

# Abseil (needed by cow-amort-bench for btree_map comparison)
include(FetchContent)
FetchContent_Declare(
    abseil
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG        20240722.0
    GIT_SHALLOW    TRUE
)
set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "" FORCE)
set(ABSL_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(abseil)

# DuckDB (query engine for psitri-duckdb)
option(BUILD_DUCKDB "Build DuckDB storage extension" OFF)
option(BUILD_SQLITE "Build psitri-sqlite (SQLite API backed by psitri DWAL)" OFF)
if(BUILD_DUCKDB)
    FetchContent_Declare(
        duckdb
        GIT_REPOSITORY https://github.com/duckdb/duckdb.git
        GIT_TAG        v1.2.2
        GIT_SHALLOW    TRUE
    )
    set(BUILD_UNITTESTS OFF CACHE BOOL "" FORCE)
    set(BUILD_SHELL OFF CACHE BOOL "" FORCE)
    set(BUILD_PARQUET_EXTENSION OFF CACHE BOOL "" FORCE)
    set(SKIP_EXTENSIONS "parquet" CACHE STRING "" FORCE)
    set(BUILD_JEMALLOC_EXTENSION OFF CACHE BOOL "" FORCE)
    set(BUILD_AUTOCOMPLETE_EXTENSION OFF CACHE BOOL "" FORCE)
    set(DISABLE_BUILTIN_EXTENSIONS ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(duckdb)

    # Patch DuckDB v1.2.2 for LLVM 22+ / libc++ C++23 compatibility.
    #
    # Problem: circular include chain select_statement.hpp -> query_node.hpp ->
    # common_table_expression_info.hpp -> select_statement.hpp (blocked by
    # #pragma once). This leaves SelectStatement incomplete when unique_ptr
    # destructor is instantiated, triggering static_assert(sizeof(T) >= 0)
    # in LLVM 22's libc++ under C++23.
    #
    # Fix: move constructors/destructors that trigger unique_ptr<incomplete>
    # instantiation out-of-line into .cpp files.

    # 1) SelectStatement: move inline ctor out-of-line
    set(_ss_file "${duckdb_SOURCE_DIR}/src/include/duckdb/parser/statement/select_statement.hpp")
    file(READ "${_ss_file}" _ss_content)
    string(FIND "${_ss_content}" "SelectStatement() : SQLStatement" _ss_ctor_pos)
    if(_ss_ctor_pos GREATER -1)
        string(REPLACE
            "SelectStatement() : SQLStatement(StatementType::SELECT_STATEMENT) {\n\t}"
            "SelectStatement();"
            _ss_content "${_ss_content}")
        file(WRITE "${_ss_file}" "${_ss_content}")
        file(APPEND "${duckdb_SOURCE_DIR}/src/parser/statement/select_statement.cpp"
            "\n// Patched: out-of-line ctor for llvm 22+ compatibility\n"
            "namespace duckdb {\n"
            "SelectStatement::SelectStatement() : SQLStatement(StatementType::SELECT_STATEMENT) {}\n"
            "} // namespace duckdb\n")
        message(STATUS "Patched DuckDB SelectStatement ctor for llvm 22+ compat")
    endif()

    # 2) CommonTableExpressionInfo: add out-of-line destructor so the implicit
    #    dtor doesn't instantiate unique_ptr<SelectStatement>::~unique_ptr()
    #    while SelectStatement is still incomplete.
    set(_cte_file "${duckdb_SOURCE_DIR}/src/include/duckdb/parser/common_table_expression_info.hpp")
    file(READ "${_cte_file}" _cte_content)
    string(FIND "${_cte_content}" "~CommonTableExpressionInfo" _cte_dtor_pos)
    if(_cte_dtor_pos EQUAL -1)
        string(REPLACE
            "unique_ptr<CommonTableExpressionInfo> Copy();"
            "unique_ptr<CommonTableExpressionInfo> Copy();\n\t~CommonTableExpressionInfo();"
            _cte_content "${_cte_content}")
        file(WRITE "${_cte_file}" "${_cte_content}")
        file(APPEND "${duckdb_SOURCE_DIR}/src/parser/query_node.cpp"
            "\n// Patched: out-of-line dtor for llvm 22+ compatibility\n"
            "#include \"duckdb/parser/common_table_expression_info.hpp\"\n"
            "namespace duckdb {\n"
            "CommonTableExpressionInfo::~CommonTableExpressionInfo() = default;\n"
            "} // namespace duckdb\n")
        message(STATUS "Patched DuckDB CommonTableExpressionInfo dtor for llvm 22+ compat")
    endif()

    # 3) GetDatabaseType: skip autoload if storage extension is already registered.
    #    DuckDB v1.2.2 tries to autoload/install a file-based extension for TYPE X
    #    before checking config.storage_extensions, breaking in-process registration.
    set(_dbm_file "${duckdb_SOURCE_DIR}/src/main/database_manager.cpp")
    file(READ "${_dbm_file}" _dbm_content)
    string(FIND "${_dbm_content}" "// Patched: skip autoload" _dbm_patch_pos)
    if(_dbm_patch_pos EQUAL -1)
        string(REPLACE
            "// If we are loading a database type from an extension, then we need to check if that extension is loaded.\n\tif (!options.db_type.empty()) {\n\t\tif (!Catalog::TryAutoLoad(context, options.db_type)) {"
            "// If we are loading a database type from an extension, then we need to check if that extension is loaded.\n\tif (!options.db_type.empty()) {\n\t\t// Patched: skip autoload if storage extension already registered in-process\n\t\tif (config.storage_extensions.find(options.db_type) != config.storage_extensions.end()) {\n\t\t\treturn;\n\t\t}\n\t\tif (!Catalog::TryAutoLoad(context, options.db_type)) {"
            _dbm_content "${_dbm_content}")
        file(WRITE "${_dbm_file}" "${_dbm_content}")
        message(STATUS "Patched DuckDB GetDatabaseType to check storage_extensions first")
    endif()
endif()

# SAL library setup
option(SAL_DEBUG "Enable debug logging for SAL library" ON)
option(SAL_BUILD_TESTS "Build SAL test suite" ${BUILD_TESTS})

add_subdirectory(libraries/sal)
add_subdirectory(libraries/psitri)
add_subdirectory(libraries/hash)
add_subdirectory(libraries/ucc)
add_subdirectory(libraries/art)

# PsiTriRocks: RocksDB-compatible wrapper around psitri
add_subdirectory(libraries/psitrirocks)

# PsiTri SQL layers (independently optional)
if(BUILD_SQLITE)
    add_subdirectory(libraries/psitri-sqlite)
endif()
if(BUILD_DUCKDB)
    add_subdirectory(libraries/psitri-duckdb)
endif()

# PsiTriMDBX: MDBX-compatible wrapper around psitri
add_subdirectory(libraries/psitrimdbx)

# Debug specific settings at the global level
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-O0 -g -fno-omit-frame-pointer)
endif()

# Sanitizer configuration
if(ENABLE_SANITIZER)
    message(STATUS "Enabling Thread Sanitizer")
    target_compile_options(psitri PUBLIC -fsanitize=thread -g)
    target_link_options(psitri PUBLIC -fsanitize=thread -g)
endif()

# Coverage configuration
if(ENABLE_COVERAGE)
    message(STATUS "Enabling coverage reporting (SCALE override: SCALE=1 for full coverage)")

    # Use -O1 and NDEBUG for faster coverage runs — asserts and -O0 are unnecessary overhead
    target_compile_options(psitri PUBLIC
        -fprofile-arcs -ftest-coverage -g -O1 -DNDEBUG)
    target_link_options(psitri PUBLIC
        -fprofile-arcs -ftest-coverage)

    # Find required tools
    find_program(LCOV lcov REQUIRED)
    find_program(GENHTML genhtml REQUIRED)
    
    # Define common arguments
    set(LCOV_ARGS
        --rc branch_coverage=1
        --rc function_coverage=1
        --ignore-errors graph,empty,corrupt,inconsistent,category,deprecated,format,unused,range
        --filter range
        --verbose
    )

    set(GENHTML_ARGS
        --ignore-errors corrupt,inconsistent,category,range
        --branch-coverage
        --function-coverage
        --verbose
    )

    # Define unwanted files list
    set(COVERAGE_EXCLUDES
        '/opt/homebrew/*'
        '/usr/include/*'
        '/usr/local/*'
        '/v1/*'
        '/Library/*'
        '*/catch2/*'
        '*/test/*'
        '*/tests/*'
        '*/debug.hpp'
    )
    
    # Run test groups in separate processes so a crash doesn't lose other groups' gcda data.
    # gcda files accumulate across runs — lcov captures the union.
    set(PSITRI_TESTS ${CMAKE_BINARY_DIR}/bin/psitri-tests)
    set(COVERAGE_EXCLUDE_TAGS "~[crash]" "~[!mayfail]" "~[benchmark]")

    # Crash-prone groups run isolated; everything else runs as one batch first.
    set(COVERAGE_RUN_COMMANDS
        COMMAND ${CMAKE_COMMAND} -E make_directory coverage
        COMMAND ${LCOV} --directory ${CMAKE_BINARY_DIR} --zerocounters
        # SAL tests
        COMMAND ${CMAKE_BINARY_DIR}/bin/sal-tests ${COVERAGE_EXCLUDE_TAGS} || true
        # ART tests
        COMMAND ${CMAKE_BINARY_DIR}/bin/art-tests ${COVERAGE_EXCLUDE_TAGS} || true
        # psitri batch: all tests except crash-prone groups (fast, covers most code)
        COMMAND ${PSITRI_TESTS} "~[fuzz]" "~[multi-writer]" "~[power_loss]" "~[recovery]" "~[truncate]" ${COVERAGE_EXCLUDE_TAGS} || true
        # Isolated groups that may crash or hang
        COMMAND ${PSITRI_TESTS} "[truncate]" ${COVERAGE_EXCLUDE_TAGS} || true
        COMMAND ${PSITRI_TESTS} "[recovery]" ${COVERAGE_EXCLUDE_TAGS} || true
        COMMAND ${PSITRI_TESTS} "[power_loss]" ${COVERAGE_EXCLUDE_TAGS} || true
        COMMAND ${PSITRI_TESTS} "[multi-writer]" ${COVERAGE_EXCLUDE_TAGS} || true
        COMMAND ${PSITRI_TESTS} "[fuzz]" "~[multi-writer]" ${COVERAGE_EXCLUDE_TAGS} || true
    )

    # coverage-run: just run the tests (no report generation)
    add_custom_target(coverage-run
        ${COVERAGE_RUN_COMMANDS}
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        DEPENDS sal-tests psitri-tests art-tests
        COMMENT "Running coverage test groups..."
    )

    # coverage-report: generate report from existing gcda data
    add_custom_target(coverage-report
        COMMAND ${LCOV}
            --directory ${CMAKE_BINARY_DIR}
            --capture
            --output-file coverage/coverage.info
            ${LCOV_ARGS}
        COMMAND ${LCOV}
            --remove coverage/coverage.info
            ${COVERAGE_EXCLUDES}
            --output-file coverage/coverage.info
            ${LCOV_ARGS}
        COMMAND ${GENHTML}
            coverage/coverage.info
            --output-directory coverage/html
            ${GENHTML_ARGS}
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating coverage report from existing data..."
    )

    # coverage: run all test groups then generate report
    add_custom_target(coverage
        DEPENDS sal-tests psitri-tests art-tests
        ${COVERAGE_RUN_COMMANDS}
        COMMAND ${LCOV}
            --directory ${CMAKE_BINARY_DIR}
            --capture
            --output-file coverage/coverage.info
            ${LCOV_ARGS}
        COMMAND ${LCOV}
            --remove coverage/coverage.info
            ${COVERAGE_EXCLUDES}
            --output-file coverage/coverage.info
            ${LCOV_ARGS}
        COMMAND ${GENHTML}
            coverage/coverage.info
            --output-directory coverage/html
            ${GENHTML_ARGS}
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating coverage report..."
    )

    # coverage-reset: just zero the counters
    add_custom_target(coverage-reset
        COMMAND ${CMAKE_COMMAND} -E make_directory coverage
        COMMAND ${LCOV} --directory ${CMAKE_BINARY_DIR} --zerocounters
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Resetting coverage counters..."
    )
endif()

# MDBX benchmark test (optional)
option(BUILD_MDBX_TEST "Build the lmdbx benchmark test" OFF)
# mdbx-benchmark is created later, after FetchContent_MakeAvailable(libmdbx)

# Psitri benchmark
add_executable(psitri-benchmark programs/psitri_bench.cpp)
target_link_libraries(psitri-benchmark PUBLIC Boost::program_options psitri)

# COW amortization benchmark
add_executable(cow-amort-bench programs/cow_amort_bench.cpp third_party/libart/art.c)
target_link_libraries(cow-amort-bench PRIVATE psitri absl::btree art)
target_include_directories(cow-amort-bench PRIVATE ${CMAKE_SOURCE_DIR}/third_party/libart)
target_compile_definitions(cow-amort-bench PRIVATE XXH_INLINE_ALL)
set_target_properties(cow-amort-bench PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
target_include_directories(psitri-benchmark PUBLIC ${Boost_INCLUDE_DIRS})
set_target_properties(psitri-benchmark
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# DWAL benchmark (DWAL buffered vs direct COW)
add_executable(dwal-bench programs/dwal_bench.cpp)
target_link_libraries(dwal-bench PRIVATE psitri)
target_include_directories(dwal-bench PRIVATE ${CMAKE_SOURCE_DIR}/programs)
set_target_properties(dwal-bench PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# Psitri database inspection & defrag tool
add_executable(psitri-tool programs/psitri_tool.cpp)
target_link_libraries(psitri-tool PUBLIC Boost::program_options psitri)
target_include_directories(psitri-tool PUBLIC ${Boost_INCLUDE_DIRS})
set_target_properties(psitri-tool PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# psitri benchmark in TidesDB-style format (same workload/output for direct comparison)
# Configurable via BENCH_* cmake variables (same defaults as TidesDB bench)
if(NOT DEFINED BENCH_NUM_OPERATIONS)
    set(BENCH_NUM_OPERATIONS 100000000)
endif()
if(NOT DEFINED BENCH_NUM_SEEK_OPS)
    set(BENCH_NUM_SEEK_OPS 1000)
endif()
if(NOT DEFINED BENCH_KEY_SIZE)
    set(BENCH_KEY_SIZE 16)
endif()
if(NOT DEFINED BENCH_VALUE_SIZE)
    set(BENCH_VALUE_SIZE 100)
endif()
if(NOT DEFINED BENCH_NUM_THREADS)
    set(BENCH_NUM_THREADS 8)
endif()
if(NOT DEFINED BENCH_KEY_PATTERN)
    set(BENCH_KEY_PATTERN "random")
endif()
if(NOT DEFINED BENCH_DB_PATH)
    set(BENCH_DB_PATH "psitri_benchmark_db")
endif()
if(NOT DEFINED BENCH_BATCH_SIZE)
    set(BENCH_BATCH_SIZE 1000)
endif()

add_executable(psitri-tidesdb-bench bench/psitri__bench.cpp)
target_compile_definitions(psitri-tidesdb-bench PRIVATE
    BENCH_NUM_OPERATIONS=${BENCH_NUM_OPERATIONS}
    BENCH_NUM_SEEK_OPS=${BENCH_NUM_SEEK_OPS}
    BENCH_KEY_SIZE=${BENCH_KEY_SIZE}
    BENCH_VALUE_SIZE=${BENCH_VALUE_SIZE}
    BENCH_NUM_THREADS=${BENCH_NUM_THREADS}
    BENCH_KEY_PATTERN="${BENCH_KEY_PATTERN}"
    BENCH_DB_PATH="${BENCH_DB_PATH}"
    BENCH_BATCH_SIZE=${BENCH_BATCH_SIZE}
)
target_link_libraries(psitri-tidesdb-bench PRIVATE psitri Threads::Threads)
set_target_properties(psitri-tidesdb-bench PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# PsiTriRocks benchmark (uses rocksdb::DB API backed by psitri)
add_executable(psitrirocks-bench programs/rocks_bench.cpp)
target_link_libraries(psitrirocks-bench PRIVATE psitrirocks)
target_compile_definitions(psitrirocks-bench PRIVATE PSITRIROCKS_BACKEND=1)
set_target_properties(psitrirocks-bench PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# Real RocksDB benchmark (optional — fetches RocksDB for comparison)
option(BUILD_ROCKSDB_BENCH "Build RocksDB comparison benchmark (fetches RocksDB)" OFF)
if(BUILD_ROCKSDB_BENCH)
    message(STATUS "Fetching RocksDB for comparison benchmark")
    include(FetchContent)

    # Minimal RocksDB build — disable tests, tools, benchmarks, Java
    set(WITH_TESTS OFF CACHE BOOL "" FORCE)
    set(WITH_BENCHMARK_TOOLS OFF CACHE BOOL "" FORCE)
    set(WITH_TOOLS OFF CACHE BOOL "" FORCE)
    set(WITH_ALL_TESTS OFF CACHE BOOL "" FORCE)
    set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)
    set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(FAIL_ON_WARNINGS OFF CACHE BOOL "" FORCE)
    set(WITH_JNI OFF CACHE BOOL "" FORCE)
    set(WITH_SNAPPY OFF CACHE BOOL "" FORCE)
    set(WITH_LZ4 OFF CACHE BOOL "" FORCE)
    set(WITH_ZLIB OFF CACHE BOOL "" FORCE)
    set(WITH_ZSTD OFF CACHE BOOL "" FORCE)
    set(WITH_BZ2 OFF CACHE BOOL "" FORCE)

    FetchContent_Declare(
        rocksdb
        GIT_REPOSITORY https://github.com/facebook/rocksdb.git
        GIT_TAG        v9.9.3
        GIT_SHALLOW    TRUE
    )
    FetchContent_MakeAvailable(rocksdb)

    # Same benchmark code, linked against real RocksDB
    add_executable(rocksdb-bench programs/rocks_bench.cpp)
    target_link_libraries(rocksdb-bench PRIVATE rocksdb)
    target_include_directories(rocksdb-bench PRIVATE ${rocksdb_SOURCE_DIR}/include)
    set_target_properties(rocksdb-bench PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

    # Scale benchmark (mirrors psitri_bench / tidesdb_bench pattern)
    add_executable(rocksdb-scale-bench programs/rocksdb_scale_bench.cpp)
    target_compile_definitions(rocksdb-scale-bench PRIVATE XXH_INLINE_ALL)
    target_link_libraries(rocksdb-scale-bench PRIVATE rocksdb Boost::program_options hash)
    target_include_directories(rocksdb-scale-bench PRIVATE
        ${rocksdb_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/programs)
    set_target_properties(rocksdb-scale-bench PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

    # ── libmdbx for MDBX comparison benchmark ──
    message(STATUS "Fetching libmdbx for comparison benchmark")

    set(MDBX_BUILD_SHARED_LIBRARY OFF CACHE BOOL "" FORCE)
    set(MDBX_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
    set(MDBX_BUILD_CXX OFF CACHE BOOL "" FORCE)

    FetchContent_Declare(
        libmdbx
        GIT_REPOSITORY https://github.com/erthink/libmdbx.git
        GIT_TAG        v0.13.11
        GIT_SHALLOW    TRUE
    )
    FetchContent_MakeAvailable(libmdbx)

    # MDBX scale benchmark (mirrors psitri-benchmark), now that mdbx-static target exists
    if(BUILD_MDBX_TEST AND TARGET mdbx-static)
        message(STATUS "Building mdbx-benchmark (scale test)")
        add_executable(mdbx-benchmark programs/mdbx_bench.cpp)
        target_compile_definitions(mdbx-benchmark PRIVATE XXH_INLINE_ALL)
        target_link_libraries(mdbx-benchmark PRIVATE
            mdbx-static Boost::program_options hash Threads::Threads)
        set_target_properties(mdbx-benchmark PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
        if(ENABLE_SANITIZER)
            target_compile_options(mdbx-benchmark PRIVATE -fsanitize=thread -g)
            target_link_options(mdbx-benchmark PRIVATE -fsanitize=thread -g)
        endif()
    endif()

    add_subdirectory(libraries/mdbxrocks)

    # MDBX benchmark — same bench code, linked against mdbxrocks
    add_executable(mdbx-bench programs/rocks_bench.cpp)
    target_link_libraries(mdbx-bench PRIVATE mdbxrocks)
    target_compile_definitions(mdbx-bench PRIVATE MDBX_BACKEND=1)
    set_target_properties(mdbx-bench PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

    # Convenience target: build all three and run them side by side
    set(BENCH_NUM "1000000" CACHE STRING "Number of keys for bench-compare")
    set(BENCH_BATCH "1" CACHE STRING "Batch size for bench-compare")
    set(BENCH_WORKLOADS "fillseq,fillrandom,overwrite,readrandom,readseq,readreverse,seekrandom,deleteseq,deleterandom,fillrandom,readwhilewriting"
        CACHE STRING "Comma-separated benchmark workloads for bench-compare")

    add_custom_target(bench-compare
        COMMAND ${CMAKE_COMMAND} -E echo "======================================================"
        COMMAND ${CMAKE_COMMAND} -E echo "  PsiTriRocks (psitri backend) — ${BENCH_NUM} keys"
        COMMAND ${CMAKE_COMMAND} -E echo "======================================================"
        COMMAND ${CMAKE_BINARY_DIR}/bin/psitrirocks-bench
            --num=${BENCH_NUM} --batch_size=${BENCH_BATCH}
            --benchmarks=${BENCH_WORKLOADS}
            --db=${CMAKE_BINARY_DIR}/bench_psitri
        COMMAND ${CMAKE_COMMAND} -E echo ""
        COMMAND ${CMAKE_COMMAND} -E echo "======================================================"
        COMMAND ${CMAKE_COMMAND} -E echo "  RocksDB (native) — ${BENCH_NUM} keys"
        COMMAND ${CMAKE_COMMAND} -E echo "======================================================"
        COMMAND ${CMAKE_BINARY_DIR}/bin/rocksdb-bench
            --num=${BENCH_NUM} --batch_size=${BENCH_BATCH}
            --benchmarks=${BENCH_WORKLOADS}
            --db=${CMAKE_BINARY_DIR}/bench_rocksdb
        COMMAND ${CMAKE_COMMAND} -E echo ""
        COMMAND ${CMAKE_COMMAND} -E echo "======================================================"
        COMMAND ${CMAKE_COMMAND} -E echo "  MDBX (libmdbx backend) — ${BENCH_NUM} keys"
        COMMAND ${CMAKE_COMMAND} -E echo "======================================================"
        COMMAND ${CMAKE_BINARY_DIR}/bin/mdbx-bench
            --num=${BENCH_NUM} --batch_size=${BENCH_BATCH}
            --benchmarks=${BENCH_WORKLOADS}
            --db=${CMAKE_BINARY_DIR}/bench_mdbx
        DEPENDS psitrirocks-bench rocksdb-bench mdbx-bench
        COMMENT "Running PsiTriRocks vs RocksDB vs MDBX comparison benchmark"
        VERBATIM
    )
endif()

# TidesDB benchmark (optional — fetched automatically like RocksDB/MDBX)
option(BUILD_TIDESDB_BENCH "Build the TidesDB comparison benchmark" OFF)
if(BUILD_TIDESDB_BENCH)
    message(STATUS "Fetching TidesDB for comparison benchmark")

    # Fetch source only — we build it ourselves to avoid TidesDB's complex
    # CMakeLists.txt pulling in platform-specific allocator detection etc.
    FetchContent_Declare(
        tidesdb
        GIT_REPOSITORY https://github.com/tidesdb/tidesdb.git
        GIT_TAG        v8.9.4
        GIT_SHALLOW    TRUE
    )
    # Populate source only — we build TidesDB ourselves to avoid its complex
    # CMakeLists.txt (platform-specific allocator detection, etc.)
    FetchContent_GetProperties(tidesdb)
    if(NOT tidesdb_POPULATED)
        FetchContent_Populate(tidesdb)
    endif()

    # Build TidesDB as a static C library from its source files
    add_library(tidesdb_lib STATIC
        ${tidesdb_SOURCE_DIR}/src/tidesdb.c
        ${tidesdb_SOURCE_DIR}/src/block_manager.c
        ${tidesdb_SOURCE_DIR}/src/skip_list.c
        ${tidesdb_SOURCE_DIR}/src/compress.c
        ${tidesdb_SOURCE_DIR}/src/bloom_filter.c
        ${tidesdb_SOURCE_DIR}/src/manifest.c
        ${tidesdb_SOURCE_DIR}/src/clock_cache.c
        ${tidesdb_SOURCE_DIR}/src/queue.c
        ${tidesdb_SOURCE_DIR}/src/buffer.c
        ${tidesdb_SOURCE_DIR}/src/btree.c
        ${tidesdb_SOURCE_DIR}/src/alloc.c
        ${tidesdb_SOURCE_DIR}/external/xxhash.c
        ${tidesdb_SOURCE_DIR}/external/ini.c)
    # Generate tidesdb_version.h into the binary dir
    configure_file(
        "${tidesdb_SOURCE_DIR}/src/tidesdb_version.h.in"
        "${tidesdb_BINARY_DIR}/tidesdb_version.h")
    find_path(LZ4_INCLUDE_DIR lz4.h)
    target_include_directories(tidesdb_lib PRIVATE
        ${tidesdb_SOURCE_DIR}/src
        ${tidesdb_SOURCE_DIR}/external
        ${tidesdb_BINARY_DIR}
        ${LZ4_INCLUDE_DIR})
    # TidesDB needs lz4, zstd, snappy for compression
    find_library(LZ4_LIBRARY lz4)
    find_library(ZSTD_LIBRARY zstd)
    find_library(SNAPPY_LIBRARY snappy)
    target_link_libraries(tidesdb_lib PRIVATE
        ${LZ4_LIBRARY} ${ZSTD_LIBRARY} ${SNAPPY_LIBRARY} Threads::Threads)
    set_target_properties(tidesdb_lib PROPERTIES C_STANDARD 11)

    set(TIDESDB_INCLUDE_DIR ${tidesdb_SOURCE_DIR}/src)

    # The C wrapper is compiled as C to avoid C++ type-checking issues
    # with TidesDB's headers (implicit void* casts).
    add_executable(tidesdb-benchmark
        programs/tidesdb_bench.cpp
        programs/tidesdb_c_wrapper.c)
    target_compile_definitions(tidesdb-benchmark PRIVATE XXH_INLINE_ALL)
    target_include_directories(tidesdb-benchmark PRIVATE
        ${TIDESDB_INCLUDE_DIR} ${tidesdb_SOURCE_DIR}/external
        ${tidesdb_BINARY_DIR} ${CMAKE_SOURCE_DIR}/programs ${Boost_INCLUDE_DIRS})
    target_link_libraries(tidesdb-benchmark PRIVATE
        tidesdb_lib Boost::program_options hash Threads::Threads)
    set_target_properties(tidesdb-benchmark PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

    if(ENABLE_SANITIZER)
        target_compile_options(tidesdb-benchmark PRIVATE -fsanitize=thread -g)
        target_link_options(tidesdb-benchmark PRIVATE -fsanitize=thread -g)
    endif()
endif()

# ── Bank Transaction Benchmark ──
# PsiTri bank bench (always available)
add_executable(bank-bench-psitri programs/bank_bench.cpp)
target_compile_definitions(bank-bench-psitri PRIVATE BANK_ENGINE_PSITRI=1)
target_link_libraries(bank-bench-psitri PRIVATE psitri Boost::program_options)
set_target_properties(bank-bench-psitri PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# PsiTri DWAL bank bench (always available)
add_executable(bank-bench-dwal programs/bank_bench.cpp)
target_compile_definitions(bank-bench-dwal PRIVATE BANK_ENGINE_DWAL=1)
target_link_libraries(bank-bench-dwal PRIVATE psitri Boost::program_options)
set_target_properties(bank-bench-dwal PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# PsiTriMDBX bank bench — uses the same MDBX C API but backed by psitrimdbx
add_executable(bank-bench-psitrimdbx programs/bank_bench.cpp)
target_compile_definitions(bank-bench-psitrimdbx PRIVATE BANK_ENGINE_MDBX=1)
target_link_libraries(bank-bench-psitrimdbx PRIVATE psitrimdbx Boost::program_options)
set_target_properties(bank-bench-psitrimdbx PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# PsiTri-SQLite bank bench — uses sqlite3 API backed by psitri-sqlite
if(BUILD_SQLITE)
    add_executable(bank-bench-psitri-sqlite programs/bank_bench.cpp)
    target_compile_definitions(bank-bench-psitri-sqlite PRIVATE BANK_ENGINE_SQLITE=1 PSITRI_SQLITE=1)
    target_link_libraries(bank-bench-psitri-sqlite PRIVATE psitri-sqlite Boost::program_options)
    target_include_directories(bank-bench-psitri-sqlite PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/libraries/psitri-sqlite/sqlite3)
    set_target_properties(bank-bench-psitri-sqlite PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
endif()

# System SQLite bank bench — for comparison against psitri-sqlite
if(SQLite3_FOUND)
    add_executable(bank-bench-system-sqlite programs/bank_bench.cpp)
    target_compile_definitions(bank-bench-system-sqlite PRIVATE BANK_ENGINE_SQLITE=1)
    target_link_libraries(bank-bench-system-sqlite PRIVATE SQLite3::SQLite3 Boost::program_options)
    set_target_properties(bank-bench-system-sqlite PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
endif()

if(BUILD_ROCKSDB_BENCH)
    # PsiTriRocks bank bench
    add_executable(bank-bench-psitrirocks programs/bank_bench.cpp)
    target_compile_definitions(bank-bench-psitrirocks PRIVATE BANK_ENGINE_ROCKSDB=1 PSITRIROCKS_BACKEND=1)
    target_link_libraries(bank-bench-psitrirocks PRIVATE psitrirocks Boost::program_options)
    set_target_properties(bank-bench-psitrirocks PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

    # Real RocksDB bank bench
    add_executable(bank-bench-rocksdb programs/bank_bench.cpp)
    target_compile_definitions(bank-bench-rocksdb PRIVATE BANK_ENGINE_ROCKSDB=1)
    target_link_libraries(bank-bench-rocksdb PRIVATE rocksdb Boost::program_options)
    target_include_directories(bank-bench-rocksdb PRIVATE ${rocksdb_SOURCE_DIR}/include)
    set_target_properties(bank-bench-rocksdb PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

    # MDBX bank bench (native libmdbx)
    add_executable(bank-bench-mdbx programs/bank_bench.cpp)
    target_compile_definitions(bank-bench-mdbx PRIVATE BANK_ENGINE_MDBX=1)
    target_link_libraries(bank-bench-mdbx PRIVATE mdbx-static Boost::program_options)
    set_target_properties(bank-bench-mdbx PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
endif()

# Standalone native libmdbx bank bench (when BUILD_ROCKSDB_BENCH is off)
if(NOT BUILD_ROCKSDB_BENCH AND EXISTS "${CMAKE_BINARY_DIR}/_deps/libmdbx-src/CMakeLists.txt")
    set(MDBX_BUILD_SHARED_LIBRARY OFF CACHE BOOL "" FORCE)
    set(MDBX_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
    set(MDBX_BUILD_CXX OFF CACHE BOOL "" FORCE)
    add_subdirectory("${CMAKE_BINARY_DIR}/_deps/libmdbx-src"
                     "${CMAKE_BINARY_DIR}/_deps/libmdbx-build"
                     EXCLUDE_FROM_ALL)

    if(TARGET mdbx-static)
        add_executable(bank-bench-mdbx programs/bank_bench.cpp)
        target_compile_definitions(bank-bench-mdbx PRIVATE BANK_ENGINE_MDBX=1)
        target_link_libraries(bank-bench-mdbx PRIVATE mdbx-static Boost::program_options)
        set_target_properties(bank-bench-mdbx PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
    endif()
endif()

if(BUILD_TIDESDB_BENCH AND TARGET tidesdb_lib)
    add_executable(bank-bench-tidesdb
        programs/bank_bench.cpp
        programs/tidesdb_c_wrapper.c)
    target_compile_definitions(bank-bench-tidesdb PRIVATE BANK_ENGINE_TIDESDB=1)
    target_include_directories(bank-bench-tidesdb PRIVATE
        ${TIDESDB_INCLUDE_DIR} ${tidesdb_SOURCE_DIR}/external
        ${tidesdb_BINARY_DIR} ${CMAKE_SOURCE_DIR}/programs ${Boost_INCLUDE_DIRS})
    target_link_libraries(bank-bench-tidesdb PRIVATE
        tidesdb_lib Boost::program_options Threads::Threads)
    set_target_properties(bank-bench-tidesdb PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
endif()

# ── KV Benchmark ──
# PsiTri kv bench (always available)
add_executable(kv-bench-psitri programs/kv_bench.cpp)
target_compile_definitions(kv-bench-psitri PRIVATE KV_ENGINE_PSITRI=1)
target_link_libraries(kv-bench-psitri PRIVATE psitri hash Boost::program_options)
set_target_properties(kv-bench-psitri PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

add_executable(kv-bench-dwal programs/kv_bench.cpp)
target_compile_definitions(kv-bench-dwal PRIVATE KV_ENGINE_DWAL=1)
target_link_libraries(kv-bench-dwal PRIVATE psitri hash Boost::program_options)
set_target_properties(kv-bench-dwal PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

if(BUILD_ROCKSDB_BENCH)
    # PsiTriRocks kv bench
    add_executable(kv-bench-psitrirocks programs/kv_bench.cpp)
    target_compile_definitions(kv-bench-psitrirocks PRIVATE KV_ENGINE_ROCKSDB=1 PSITRIROCKS_BACKEND=1)
    target_link_libraries(kv-bench-psitrirocks PRIVATE psitrirocks hash Boost::program_options)
    set_target_properties(kv-bench-psitrirocks PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

    # Real RocksDB kv bench
    add_executable(kv-bench-rocksdb programs/kv_bench.cpp)
    target_compile_definitions(kv-bench-rocksdb PRIVATE KV_ENGINE_ROCKSDB=1 XXH_INLINE_ALL)
    target_link_libraries(kv-bench-rocksdb PRIVATE rocksdb hash Boost::program_options)
    target_include_directories(kv-bench-rocksdb PRIVATE ${rocksdb_SOURCE_DIR}/include)
    set_target_properties(kv-bench-rocksdb PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

    # MDBX kv bench
    add_executable(kv-bench-mdbx programs/kv_bench.cpp)
    target_compile_definitions(kv-bench-mdbx PRIVATE KV_ENGINE_MDBX=1 XXH_INLINE_ALL)
    target_link_libraries(kv-bench-mdbx PRIVATE mdbx-static hash Boost::program_options)
    set_target_properties(kv-bench-mdbx PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
endif()

if(BUILD_TIDESDB_BENCH AND TARGET tidesdb_lib)
    add_executable(kv-bench-tidesdb
        programs/kv_bench.cpp
        programs/tidesdb_c_wrapper.c)
    target_compile_definitions(kv-bench-tidesdb PRIVATE KV_ENGINE_TIDESDB=1 XXH_INLINE_ALL)
    target_include_directories(kv-bench-tidesdb PRIVATE
        ${TIDESDB_INCLUDE_DIR} ${tidesdb_SOURCE_DIR}/external
        ${tidesdb_BINARY_DIR} ${CMAKE_SOURCE_DIR}/programs ${Boost_INCLUDE_DIRS})
    target_link_libraries(kv-bench-tidesdb PRIVATE
        tidesdb_lib hash Boost::program_options Threads::Threads)
    set_target_properties(kv-bench-tidesdb PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
endif()

# ── Examples ──
option(BUILD_EXAMPLES "Build example programs" ON)
if(BUILD_EXAMPLES)
    set(EXAMPLE_SOURCES
        basic_crud
        cursor_iteration
        range_operations
        snapshots
        subtrees
    )
    foreach(example ${EXAMPLE_SOURCES})
        add_executable(example-${example} examples/${example}.cpp)
        target_link_libraries(example-${example} PRIVATE psitri)
        set_target_properties(example-${example} PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/examples")
    endforeach()
endif()

# ── Installation ──
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Install psitri and all dependencies as a single package
install(TARGETS psitri psitrirocks sal hash ucc art
    EXPORT psitri-targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# Install all headers
install(DIRECTORY libraries/psitri/include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY libraries/psitrirocks/include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY libraries/sal/include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY libraries/hash/include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY libraries/ucc/include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# Export targets
install(EXPORT psitri-targets
    FILE psitri-targets.cmake
    NAMESPACE psitri::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/psitri
)

# Config file
configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/psitri-config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/psitri-config.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/psitri
)

# Version file
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/psitri-config-version.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/psitri-config.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/psitri-config-version.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/psitri
)

# Only add test directory if testing is enabled
if(BUILD_TESTS)
    add_subdirectory(test)
endif()

