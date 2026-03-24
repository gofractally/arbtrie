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
    message(STATUS "Enabling coverage reporting")

    target_compile_options(psitri PUBLIC
        $<$<CONFIG:Debug>:-fprofile-arcs -ftest-coverage -g -O0>)
    target_link_options(psitri PUBLIC
        $<$<CONFIG:Debug>:-fprofile-arcs -ftest-coverage>)

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
    )
    
    # Add custom target for generating coverage report
    add_custom_target(coverage
        # Clear counters
        COMMAND ${CMAKE_COMMAND} -E make_directory coverage
        COMMAND ${LCOV} --directory ${CMAKE_BINARY_DIR} --zerocounters
        
        # Run tests (continue even if they fail)
        COMMAND ${CMAKE_BINARY_DIR}/bin/psitri-tests || true
        
        # Capture coverage data
        COMMAND ${LCOV} 
            --directory ${CMAKE_BINARY_DIR}
            --capture 
            --output-file coverage/coverage.info 
            ${LCOV_ARGS}

        # Remove unwanted files from coverage
        COMMAND ${LCOV} 
            --remove coverage/coverage.info 
            ${COVERAGE_EXCLUDES}
            --output-file coverage/coverage.info
            ${LCOV_ARGS}
        
        # Generate HTML report
        COMMAND ${GENHTML} 
            coverage/coverage.info 
            --output-directory coverage/html 
            ${GENHTML_ARGS}
        
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating coverage report..."
    )

    # Add target that only generates report from existing data
    add_custom_target(coverage-report
        # Capture coverage data
        COMMAND ${LCOV} 
            --directory ${CMAKE_BINARY_DIR}
            --capture 
            --output-file coverage/coverage.info 
            ${LCOV_ARGS}

        # Remove unwanted files from coverage
        COMMAND ${LCOV} 
            --remove coverage/coverage.info 
            ${COVERAGE_EXCLUDES}
            --output-file coverage/coverage.info
            ${LCOV_ARGS}
        
        # Generate HTML report
        COMMAND ${GENHTML} 
            coverage/coverage.info 
            --output-directory coverage/html 
            ${GENHTML_ARGS}
        
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating coverage report from existing data..."
    )

    # Rename the full coverage target to be more descriptive
    add_custom_target(coverage-all
        COMMAND ${CMAKE_COMMAND} -E env TEST_PATTERN="" ${CMAKE_MAKE_PROGRAM} coverage
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating coverage report for all tests..."
    )

    # Add target to just reset coverage data
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

# TidesDB benchmark (optional — requires tidesdb installed or in external/)
option(BUILD_TIDESDB_BENCH "Build the TidesDB comparison benchmark" OFF)
if(BUILD_TIDESDB_BENCH)
    message(STATUS "Building TidesDB benchmark")

    # Look for tidesdb header — prefer the build/include/tidesdb/ directory
    # which has all headers (including ini.h, uthash.h, etc.)
    find_path(TIDESDB_INCLUDE_DIR tidesdb.h
        PATHS
            "${CMAKE_SOURCE_DIR}/external/tidesdb/build/include/tidesdb"
            "${CMAKE_SOURCE_DIR}/external/tidesdb/src"
        PATH_SUFFIXES include/tidesdb src
        NO_CACHE)

    # Look for tidesdb library (shared or static)
    find_library(TIDESDB_LIBRARY NAMES tidesdb libtidesdb
        PATHS "${CMAKE_SOURCE_DIR}/external/tidesdb/build"
        NO_CACHE)

    # Also try system paths if external/ didn't work
    if(NOT TIDESDB_INCLUDE_DIR)
        find_path(TIDESDB_INCLUDE_DIR tidesdb.h PATH_SUFFIXES include/tidesdb include src NO_CACHE)
    endif()
    if(NOT TIDESDB_LIBRARY)
        find_library(TIDESDB_LIBRARY NAMES tidesdb libtidesdb NO_CACHE)
    endif()

    if(TIDESDB_INCLUDE_DIR AND TIDESDB_LIBRARY)
        message(STATUS "Found TidesDB include: ${TIDESDB_INCLUDE_DIR}")
        message(STATUS "Found TidesDB library: ${TIDESDB_LIBRARY}")

        # The C wrapper is compiled as C to avoid C++ type-checking issues
        # with TidesDB's headers (implicit void* casts).
        add_executable(tidesdb-benchmark
            programs/tidesdb_bench.cpp
            programs/tidesdb_c_wrapper.c)
        target_compile_definitions(tidesdb-benchmark PRIVATE XXH_INLINE_ALL)
        target_include_directories(tidesdb-benchmark PRIVATE
            ${TIDESDB_INCLUDE_DIR} ${CMAKE_SOURCE_DIR}/programs ${Boost_INCLUDE_DIRS})
        target_link_libraries(tidesdb-benchmark PRIVATE
            ${TIDESDB_LIBRARY} Boost::program_options hash Threads::Threads)
        set_target_properties(tidesdb-benchmark PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

        if(ENABLE_SANITIZER)
            target_compile_options(tidesdb-benchmark PRIVATE -fsanitize=thread -g)
            target_link_options(tidesdb-benchmark PRIVATE -fsanitize=thread -g)
        endif()
    else()
        message(WARNING "TidesDB not found. Skipping tidesdb-benchmark build.")
        if(NOT TIDESDB_INCLUDE_DIR)
            message(WARNING "  tidesdb.h not found — try: git clone https://github.com/tidesdb/tidesdb external/tidesdb && cd external/tidesdb && cmake -B build && cmake --build build")
        endif()
        if(NOT TIDESDB_LIBRARY)
            message(WARNING "  tidesdb library not found.")
        endif()
    endif()
endif()

# Only add test directory if testing is enabled
if(BUILD_TESTS)
    add_subdirectory(test)
endif()

