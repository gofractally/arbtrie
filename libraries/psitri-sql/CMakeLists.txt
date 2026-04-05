# psitri-sql: DuckDB storage extension backed by psitri
add_library(psitri-sql
    src/row_encoding.cpp
    src/psitri_extension.cpp
    src/psitri_catalog.cpp
    src/psitri_transaction.cpp
    src/psitri_scanner.cpp
    src/psitri_insert.cpp
    src/psitri_delete.cpp
    src/psitri_update.cpp
)

target_include_directories(psitri-sql PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

# Link against psitri (storage) and duckdb (query engine)
target_link_libraries(psitri-sql PUBLIC psitri duckdb)

# DuckDB headers
target_include_directories(psitri-sql SYSTEM PUBLIC
    ${duckdb_SOURCE_DIR}/src/include
)

