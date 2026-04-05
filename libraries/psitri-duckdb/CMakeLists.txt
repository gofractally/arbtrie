# psitri-duckdb: DuckDB storage extension backed by psitri
add_library(psitri-duckdb
    src/row_encoding.cpp
    src/psitri_extension.cpp
    src/psitri_catalog.cpp
    src/psitri_transaction.cpp
    src/psitri_scanner.cpp
    src/psitri_insert.cpp
    src/psitri_delete.cpp
    src/psitri_update.cpp
)

target_include_directories(psitri-duckdb PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

# Link against psitri (storage) and duckdb (query engine)
target_link_libraries(psitri-duckdb PUBLIC psitri duckdb)

# DuckDB headers
target_include_directories(psitri-duckdb SYSTEM PUBLIC
    ${duckdb_SOURCE_DIR}/src/include
)

# TATP Benchmark — uses psitri-sqlite for all SQLite API calls.
# For comparison against system SQLite, build a separate binary or
# link against SQLite3 instead (flip HAS_SYSTEM_SQLITE3).
add_executable(tatp-bench benchmarks/tatp_bench.cpp)
target_link_libraries(tatp-bench PRIVATE psitri-duckdb psitri-sqlite)

# psitri-sqlite provides the sqlite3 API for tatp-bench
target_compile_definitions(tatp-bench PRIVATE HAS_SQLITE3=1)

# Separate binary linked against system SQLite3 for comparison benchmarks.
# Uses the exact same source but with real SQLite instead of psitri-sqlite.
find_package(SQLite3 QUIET)
if(SQLite3_FOUND)
    add_executable(tatp-bench-system-sqlite benchmarks/tatp_bench.cpp)
    target_link_libraries(tatp-bench-system-sqlite PRIVATE psitri-duckdb ${SQLite3_LIBRARIES})
    target_include_directories(tatp-bench-system-sqlite PRIVATE ${SQLite3_INCLUDE_DIRS})
    target_compile_definitions(tatp-bench-system-sqlite PRIVATE HAS_SQLITE3=1)
endif()
