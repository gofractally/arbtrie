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

# Set the C++ compiler to LLVM from Homebrew on macOS
if(APPLE)
    set(CMAKE_CXX_COMPILER "/opt/homebrew/opt/llvm/bin/clang++")
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
set(CMAKE_CXX_STANDARD 20)
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

# Find required packages
find_package(Threads REQUIRED)
find_package(SQLite3 REQUIRED)

# Find Boost
set(Boost_NO_WARN_NEW_VERSIONS ON)
find_package(Boost REQUIRED COMPONENTS program_options)

# Only find Catch2 if we're building tests
if(BUILD_TESTS)
    find_package(Catch2 3 REQUIRED)
    enable_testing()
endif()

# SAL library setup
option(SAL_DEBUG "Enable debug logging for SAL library" ON)
option(SAL_BUILD_TESTS "Build SAL test suite" ${BUILD_TESTS})

add_subdirectory(libraries/sal)
add_subdirectory(libraries/psitri)
add_subdirectory(libraries/hash)
add_subdirectory(libraries/ucc)

# PsiTriRocks: RocksDB-compatible wrapper around psitri
add_subdirectory(libraries/psitrirocks)

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
        # psitri batch: all tests except crash-prone groups (fast, covers most code)
        COMMAND ${PSITRI_TESTS} "~[fuzz]" "~[multi-writer]" "~[power_loss]" "~[recovery]" ${COVERAGE_EXCLUDE_TAGS} || true
        # Isolated groups that may crash or hang
        COMMAND ${PSITRI_TESTS} "[recovery]" ${COVERAGE_EXCLUDE_TAGS} || true
        COMMAND ${PSITRI_TESTS} "[power_loss]" ${COVERAGE_EXCLUDE_TAGS} || true
        COMMAND ${PSITRI_TESTS} "[multi-writer]" ${COVERAGE_EXCLUDE_TAGS} || true
        COMMAND ${PSITRI_TESTS} "[fuzz]" "~[multi-writer]" ${COVERAGE_EXCLUDE_TAGS} || true
    )

    # coverage-run: just run the tests (no report generation)
    add_custom_target(coverage-run
        ${COVERAGE_RUN_COMMANDS}
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
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
if(BUILD_MDBX_TEST)
    message(STATUS "Building MDBX benchmark test")
    # Look for header directly in external/libmdbx
    find_path(MDBX_INCLUDE_DIR mdbx.h PATHS external/libmdbx HINTS "${CMAKE_SOURCE_DIR}/external/libmdbx" NO_CACHE)
    
    # Look for library directly in external/libmdbx or its build dir, prefer static
    find_library(MDBX_LIBRARY NAMES libmdbx-static.a libmdbx.a mdbx
                 PATHS external/libmdbx external/libmdbx/build
                 HINTS "${CMAKE_SOURCE_DIR}/external/libmdbx" "${CMAKE_SOURCE_DIR}/external/libmdbx/build"
                 NO_DEFAULT_PATH NO_CACHE)

    # If static wasn't found, maybe dynamic exists (less preferred but okay for testing)
    if(NOT MDBX_LIBRARY)
         find_library(MDBX_LIBRARY NAMES mdbx
                      PATHS external/libmdbx external/libmdbx/build
                      HINTS "${CMAKE_SOURCE_DIR}/external/libmdbx" "${CMAKE_SOURCE_DIR}/external/libmdbx/build"
                      NO_DEFAULT_PATH NO_CACHE)
    endif()


    if(MDBX_INCLUDE_DIR AND MDBX_LIBRARY)
        message(STATUS "Found MDBX include: ${MDBX_INCLUDE_DIR}")
        message(STATUS "Found MDBX library: ${MDBX_LIBRARY}")

        # Standardized benchmark (mirrors psitri-benchmark)
        add_executable(mdbx-benchmark programs/mdbx_bench.cpp)
        target_compile_definitions(mdbx-benchmark PRIVATE XXH_INLINE_ALL)
        target_include_directories(mdbx-benchmark PRIVATE ${MDBX_INCLUDE_DIR} ${Boost_INCLUDE_DIRS})
        target_link_libraries(mdbx-benchmark PRIVATE
            ${MDBX_LIBRARY} Boost::program_options hash Threads::Threads)
        set_target_properties(mdbx-benchmark PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

        if(ENABLE_SANITIZER)
            target_compile_options(mdbx-benchmark PRIVATE -fsanitize=thread -g)
            target_link_options(mdbx-benchmark PRIVATE -fsanitize=thread -g)
        endif()

    else()
        message(WARNING "MDBX library or includes not found. Skipping lmdbx_arb_test build. Looked in external/libmdbx")
        if(NOT MDBX_INCLUDE_DIR)
            message(WARNING "MDBX_INCLUDE_DIR not found.")
        endif()
        if(NOT MDBX_LIBRARY)
            message(WARNING "MDBX_LIBRARY not found.")
        endif()
    endif()
endif()

# Psitri benchmark
add_executable(psitri-benchmark programs/psitri_bench.cpp)
target_link_libraries(psitri-benchmark PUBLIC Boost::program_options psitri)
target_include_directories(psitri-benchmark PUBLIC ${Boost_INCLUDE_DIRS})
set_target_properties(psitri-benchmark
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

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

    # MDBX bank bench
    add_executable(bank-bench-mdbx programs/bank_bench.cpp)
    target_compile_definitions(bank-bench-mdbx PRIVATE BANK_ENGINE_MDBX=1)
    target_link_libraries(bank-bench-mdbx PRIVATE mdbx-static Boost::program_options)
    set_target_properties(bank-bench-mdbx PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
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

# Only add test directory if testing is enabled
if(BUILD_TESTS)
    add_subdirectory(test)
endif()

