# Build SQLite's testfixture linked against psitri-sqlite
#
# This lets us run SQLite's own TCL test suite against our btree replacement.
# Tests that probe btree/pager internals will fail (different struct layout),
# but the vast majority of SQL-level tests should pass.

set(SQLITE_SRC_DIR "${CMAKE_SOURCE_DIR}/external/sqlite-src")
set(SQLITE_BLD_DIR "${SQLITE_SRC_DIR}/bld")

if(NOT EXISTS "${SQLITE_SRC_DIR}/src/test1.c")
    message(STATUS "SQLite source not found at ${SQLITE_SRC_DIR} — skipping testfixture")
    return()
endif()

# Generated headers (parse.h, opcodes.h, etc.) must exist.
# Run: cd external/sqlite-src/bld && ../configure && make parse.h opcodes.h keywordhash.h
if(NOT EXISTS "${SQLITE_BLD_DIR}/parse.h")
    message(STATUS "SQLite generated headers not found at ${SQLITE_BLD_DIR} — skipping testfixture")
    message(STATUS "  Run: cd external/sqlite-src && mkdir -p bld && cd bld && ../configure && make parse.h opcodes.h keywordhash.h")
    return()
endif()

# Find TCL
find_library(TCL_LIBRARY tcl8.5 tcl)
find_path(TCL_INCLUDE_DIR tcl.h
    PATHS
        /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include
        /Library/Developer/CommandLineTools/SDKs/MacOSX15.4.sdk/usr/include
        /Library/Developer/CommandLineTools/SDKs/MacOSX26.2.sdk/usr/include
        /usr/include
)

if(NOT TCL_LIBRARY OR NOT TCL_INCLUDE_DIR)
    message(STATUS "TCL not found — skipping testfixture (need tcl.h and libtcl)")
    return()
endif()

message(STATUS "Building psitri-sqlite testfixture (TCL: ${TCL_LIBRARY})")

# Test source files from SQLite
set(TESTSRC
    ${SQLITE_SRC_DIR}/src/test1.c
    ${SQLITE_SRC_DIR}/src/test2.c
    ${SQLITE_SRC_DIR}/src/test3.c
    ${SQLITE_SRC_DIR}/src/test4.c
    ${SQLITE_SRC_DIR}/src/test5.c
    ${SQLITE_SRC_DIR}/src/test6.c
    ${SQLITE_SRC_DIR}/src/test8.c
    ${SQLITE_SRC_DIR}/src/test9.c
    ${SQLITE_SRC_DIR}/src/test_autoext.c
    ${SQLITE_SRC_DIR}/src/test_backup.c
    ${SQLITE_SRC_DIR}/src/test_bestindex.c
    ${SQLITE_SRC_DIR}/src/test_blob.c
    ${SQLITE_SRC_DIR}/src/test_btree.c
    ${SQLITE_SRC_DIR}/src/test_config.c
    ${SQLITE_SRC_DIR}/src/test_delete.c
    ${SQLITE_SRC_DIR}/src/test_demovfs.c
    ${SQLITE_SRC_DIR}/src/test_devsym.c
    ${SQLITE_SRC_DIR}/src/test_fs.c
    ${SQLITE_SRC_DIR}/src/test_func.c
    ${SQLITE_SRC_DIR}/src/test_hexio.c
    ${SQLITE_SRC_DIR}/src/test_init.c
    ${SQLITE_SRC_DIR}/src/test_intarray.c
    ${SQLITE_SRC_DIR}/src/test_journal.c
    ${SQLITE_SRC_DIR}/src/test_malloc.c
    ${SQLITE_SRC_DIR}/src/test_md5.c
    ${SQLITE_SRC_DIR}/src/test_multiplex.c
    ${SQLITE_SRC_DIR}/src/test_mutex.c
    ${SQLITE_SRC_DIR}/src/test_onefile.c
    ${SQLITE_SRC_DIR}/src/test_osinst.c
    ${SQLITE_SRC_DIR}/src/test_pcache.c
    ${SQLITE_SRC_DIR}/src/test_quota.c
    ${SQLITE_SRC_DIR}/src/test_rtree.c
    ${SQLITE_SRC_DIR}/src/test_schema.c
    ${SQLITE_SRC_DIR}/src/test_superlock.c
    ${SQLITE_SRC_DIR}/src/test_syscall.c
    ${SQLITE_SRC_DIR}/src/test_tclsh.c
    ${SQLITE_SRC_DIR}/src/test_tclvar.c
    ${SQLITE_SRC_DIR}/src/test_thread.c
    ${SQLITE_SRC_DIR}/src/test_vdbecov.c
    ${SQLITE_SRC_DIR}/src/test_vfs.c
    ${SQLITE_SRC_DIR}/src/test_window.c
    ${SQLITE_SRC_DIR}/src/test_wsd.c
    ${SQLITE_SRC_DIR}/src/tclsqlite.c
    # Extensions needed by tests
    ${SQLITE_SRC_DIR}/ext/fts3/fts3_term.c
    ${SQLITE_SRC_DIR}/ext/fts3/fts3_test.c
    ${SQLITE_SRC_DIR}/ext/session/test_session.c
    ${SQLITE_SRC_DIR}/ext/recover/sqlite3recover.c
    ${SQLITE_SRC_DIR}/ext/recover/dbdata.c
    ${SQLITE_SRC_DIR}/ext/recover/test_recover.c
    ${SQLITE_SRC_DIR}/ext/intck/test_intck.c
    ${SQLITE_SRC_DIR}/ext/intck/sqlite3intck.c
    ${SQLITE_SRC_DIR}/ext/rbu/test_rbu.c
    ${SQLITE_SRC_DIR}/ext/expert/sqlite3expert.c
    ${SQLITE_SRC_DIR}/ext/expert/test_expert.c
    ${SQLITE_SRC_DIR}/ext/misc/amatch.c
    ${SQLITE_SRC_DIR}/ext/misc/appendvfs.c
    ${SQLITE_SRC_DIR}/ext/misc/basexx.c
    ${SQLITE_SRC_DIR}/ext/misc/cksumvfs.c
    ${SQLITE_SRC_DIR}/ext/misc/closure.c
    ${SQLITE_SRC_DIR}/ext/misc/csv.c
    ${SQLITE_SRC_DIR}/ext/misc/decimal.c
    ${SQLITE_SRC_DIR}/ext/misc/eval.c
    ${SQLITE_SRC_DIR}/ext/misc/explain.c
    ${SQLITE_SRC_DIR}/ext/misc/fileio.c
    ${SQLITE_SRC_DIR}/ext/misc/fuzzer.c
    ${SQLITE_SRC_DIR}/ext/fts5/fts5_tcl.c
    ${SQLITE_SRC_DIR}/ext/fts5/fts5_test_mi.c
    ${SQLITE_SRC_DIR}/ext/fts5/fts5_test_tok.c
    ${SQLITE_SRC_DIR}/ext/misc/ieee754.c
    ${SQLITE_SRC_DIR}/ext/misc/mmapwarm.c
    ${SQLITE_SRC_DIR}/ext/misc/nextchar.c
    ${SQLITE_SRC_DIR}/ext/misc/normalize.c
    ${SQLITE_SRC_DIR}/ext/misc/prefixes.c
    ${SQLITE_SRC_DIR}/ext/misc/qpvtab.c
    ${SQLITE_SRC_DIR}/ext/misc/randomjson.c
    ${SQLITE_SRC_DIR}/ext/misc/regexp.c
    ${SQLITE_SRC_DIR}/ext/misc/remember.c
    ${SQLITE_SRC_DIR}/ext/misc/series.c
    ${SQLITE_SRC_DIR}/ext/misc/spellfix.c
    ${SQLITE_SRC_DIR}/ext/misc/stmtrand.c
    ${SQLITE_SRC_DIR}/ext/misc/totype.c
    ${SQLITE_SRC_DIR}/ext/misc/unionvtab.c
    ${SQLITE_SRC_DIR}/ext/misc/wholenumber.c
    ${SQLITE_SRC_DIR}/ext/misc/zipfile.c
    ${SQLITE_SRC_DIR}/ext/rtree/test_rtreedoc.c
)

add_executable(psitri-testfixture ${TESTSRC} ${CMAKE_CURRENT_SOURCE_DIR}/test_stubs.c)

# Disable LTO — amalgamation + test sources trigger LLVM bugs
set_target_properties(psitri-testfixture PROPERTIES
    INTERPROCEDURAL_OPTIMIZATION OFF
)
target_compile_options(psitri-testfixture PRIVATE -fno-lto)

target_compile_definitions(psitri-testfixture PRIVATE
    SQLITE_TEST=1
    SQLITE_CRASH_TEST=1
    TCLSH_INIT_PROC=sqlite3TestInit
    SQLITE_SERVER=1
    SQLITE_PRIVATE=
    SQLITE_CORE=1
    BUILD_sqlite
    SQLITE_SERIES_CONSTRAINT_VERIFY=1
    SQLITE_DEFAULT_PAGE_SIZE=1024
    SQLITE_ENABLE_STMTVTAB=1
    SQLITE_ENABLE_DBPAGE_VTAB=1
    SQLITE_ENABLE_BYTECODE_VTAB=1
    SQLITE_ENABLE_CARRAY=1
    SQLITE_ENABLE_PERCENTILE=1
    SQLITE_CKSUMVFS_STATIC=1
    SQLITE_STATIC_RANDOMJSON=1
    SQLITE_STRICT_SUBTYPE=1
    SQLITE_NO_SYNC=1
    SQLITE_THREADSAFE=1
    SQLITE_OMIT_SHARED_CACHE=1
    SQLITE_DEFAULT_MEMSTATUS=0
    SQLITE_DQS=0
    SQLITE_OMIT_LOAD_EXTENSION=1
)

target_include_directories(psitri-testfixture PRIVATE
    ${TCL_INCLUDE_DIR}
    ${SQLITE_BLD_DIR}
    ${SQLITE_SRC_DIR}/src
    ${SQLITE_SRC_DIR}/ext/fts3
    ${SQLITE_SRC_DIR}/ext/rtree
    ${SQLITE_SRC_DIR}/ext/recover
    ${SQLITE_SRC_DIR}/ext/session
    ${SQLITE_SRC_DIR}/ext/intck
    ${SQLITE_SRC_DIR}/ext/rbu
    ${SQLITE_SRC_DIR}/ext/fts5
    ${SQLITE_SRC_DIR}/ext/expert
)

# Link against psitri-sqlite (our btree replacement) + TCL
target_link_libraries(psitri-testfixture PRIVATE
    psitri-sqlite
    ${TCL_LIBRARY}
)

# Suppress warnings from SQLite test code (it's upstream, not ours)
target_compile_options(psitri-testfixture PRIVATE
    -Wno-implicit-function-declaration
    -Wno-int-conversion
    -Wno-deprecated-declarations
    -Wno-incompatible-pointer-types
    -Wno-unused-variable
    -Wno-unused-function
)

# Ad-hoc sign on macOS to prevent com.apple.provenance issues
if(APPLE)
    add_custom_command(TARGET psitri-testfixture POST_BUILD
        COMMAND codesign -s - $<TARGET_FILE:psitri-testfixture>
        COMMENT "Ad-hoc signing psitri-testfixture"
    )
endif()
