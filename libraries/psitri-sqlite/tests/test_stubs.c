/*
** Stubs for SQLite test-instrumentation globals and functions.
**
** These symbols are normally defined inside the SQLite source when compiled
** with SQLITE_TEST=1. Since psitri-sqlite compiles the amalgamation without
** SQLITE_TEST, we provide zero-initialized stubs so the testfixture links.
**
** Test-internal counters will read as 0 — tests that check exact counter
** values may fail, but SQL-level correctness tests will pass.
*/
#include "sqlite3.h"
#include <stddef.h>

/* Test counters (normally in vdbe.c, pager.c, btree.c, etc.) */
int sqlite3_search_count = 0;
int sqlite3_sort_count = 0;
int sqlite3_sync_count = 0;
int sqlite3_fullsync_count = 0;
int sqlite3_found_count = 0;
int sqlite3_like_count = 0;
int sqlite3_xferopt_count = 0;
int sqlite3_open_file_count = 0;
int sqlite3_opentemp_count = 0;
int sqlite3_max_blobsize = 0;
int sqlite3_interrupt_count = 0;
int sqlite3_current_time = 0;

/* Pager counters */
int sqlite3_pager_readdb_count = 0;
int sqlite3_pager_writedb_count = 0;
int sqlite3_pager_writej_count = 0;

/* I/O error injection (test_vfs.c and os_unix.c use these) */
int sqlite3_io_error_hit = 0;
int sqlite3_io_error_hardhit = 0;
int sqlite3_io_error_pending = 0;
int sqlite3_io_error_persist = 0;

/* Disk full simulation */
int sqlite3_diskfull_pending = 0;
int sqlite3_diskfull = 0;

/* VFS OOM testing */
int sqlite3_memdebug_vfs_oom_test = 1;

/* Host ID for cluster testing */
int sqlite3_hostid_num = 0;

/* --- Function stubs --- */

/* sqlite3ErrName: normally behind SQLITE_NEED_ERR_NAME / SQLITE_TEST */
const char *sqlite3ErrName(int rc) {
    switch (rc & 0xff) {
        case 0:  return "SQLITE_OK";
        case 1:  return "SQLITE_ERROR";
        case 2:  return "SQLITE_INTERNAL";
        case 3:  return "SQLITE_PERM";
        case 4:  return "SQLITE_ABORT";
        case 5:  return "SQLITE_BUSY";
        case 6:  return "SQLITE_LOCKED";
        case 7:  return "SQLITE_NOMEM";
        case 8:  return "SQLITE_READONLY";
        case 9:  return "SQLITE_INTERRUPT";
        case 10: return "SQLITE_IOERR";
        case 11: return "SQLITE_CORRUPT";
        case 12: return "SQLITE_NOTFOUND";
        case 13: return "SQLITE_FULL";
        case 14: return "SQLITE_CANTOPEN";
        case 15: return "SQLITE_PROTOCOL";
        case 16: return "SQLITE_EMPTY";
        case 17: return "SQLITE_SCHEMA";
        case 18: return "SQLITE_TOOBIG";
        case 19: return "SQLITE_CONSTRAINT";
        case 20: return "SQLITE_MISMATCH";
        case 21: return "SQLITE_MISUSE";
        case 100: return "SQLITE_ROW";
        case 101: return "SQLITE_DONE";
        default: return "SQLITE_UNKNOWN";
    }
}

/* sqlite3PagerPagenumber: dead-code-eliminated because only btree.c called it */
unsigned int sqlite3PagerPagenumber(void *pPg) {
    (void)pPg;
    return 0;
}

/* sqlite3PagerStats: returns pointer to static array of pager counters */
static int dummy_pager_stats[11];
int *sqlite3PagerStats(void *pPager) {
    (void)pPager;
    return dummy_pager_stats;
}

/* sqlite3PcacheStats: return zeros for cache hit/miss/etc */
void sqlite3PcacheStats(int *pCurrent, int *pMax, int *pMin, int *pRecycle) {
    if (pCurrent) *pCurrent = 0;
    if (pMax) *pMax = 0;
    if (pMin) *pMin = 0;
    if (pRecycle) *pRecycle = 0;
}

/* sqlite3UtfSelfTest: self-test for UTF encoding, no-op stub */
void sqlite3UtfSelfTest(void) {}

/* sqlite3DbstatRegister: registers the dbstat virtual table */
int sqlite3DbstatRegister(sqlite3 *db) {
    (void)db;
    return 0;  /* SQLITE_OK */
}

/* sqlite3_carray_bind: normally in ext/misc/carray.c with SQLITE_ENABLE_CARRAY */
int sqlite3_carray_bind(
    sqlite3_stmt *pStmt,
    int idx,
    void *aData,
    int nData,
    int mFlags,
    void (*xDestroy)(void*)
) {
    (void)pStmt; (void)idx; (void)aData; (void)nData; (void)mFlags; (void)xDestroy;
    return 21;  /* SQLITE_MISUSE */
}
