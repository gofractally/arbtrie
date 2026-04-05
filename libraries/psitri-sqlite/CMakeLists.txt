# psitri-sqlite: SQLite with psitri DWAL replacing btree.c
#
# Build strategy:
#   1. sqlite3.c (C) — the amalgamation with btree.c/btmutex.c #if 0'd out
#   2. btree_psitri.cpp (C++) — our replacement, provides all sqlite3Btree* symbols
#   3. Link together with psitri for storage

add_library(psitri-sqlite
    src/btree_helpers.c      # Includes sqlite3.c amalgamation + C helpers (C)
    src/btree_psitri.cpp     # Psitri btree replacement (C++)
)

# btree_helpers.c must compile as C
set_source_files_properties(src/btree_helpers.c PROPERTIES LANGUAGE C)

# Disable LTO for this library — the amalgamation triggers LLVM LTO bugs
# and LTO mixing between C amalgamation and C++ btree replacement causes issues
set_target_properties(psitri-sqlite PROPERTIES
    INTERPROCEDURAL_OPTIMIZATION OFF
)
target_compile_options(psitri-sqlite PRIVATE -fno-lto)

# Define SQLITE_CORE so the amalgamation exports internal symbols.
# SQLITE_PRIVATE must be empty (not "static") so btree_psitri.cpp can call
# internal functions like sqlite3VdbeRecordCompare, sqlite3PutVarint, etc.
target_compile_definitions(psitri-sqlite PRIVATE
    SQLITE_CORE=1
    SQLITE_PRIVATE=
    SQLITE_THREADSAFE=1
    SQLITE_OMIT_SHARED_CACHE=1
    SQLITE_DEFAULT_MEMSTATUS=0
    SQLITE_DQS=0
)

# Public headers (sqlite3.h + our compat header)
target_include_directories(psitri-sqlite PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/sqlite3>
)

# Link against psitri for DWAL storage
target_link_libraries(psitri-sqlite PUBLIC psitri)

# Smoke test
add_executable(psitri-sqlite-smoke tests/smoke_test.cpp)
target_link_libraries(psitri-sqlite-smoke PRIVATE psitri-sqlite)
target_compile_options(psitri-sqlite-smoke PRIVATE -fno-lto)

# SQLite testfixture and test infrastructure
add_subdirectory(tests)
