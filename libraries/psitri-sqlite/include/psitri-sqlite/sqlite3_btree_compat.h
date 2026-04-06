#pragma once
/**
 * sqlite3_btree_compat.h — Minimal declarations from SQLite internals
 * needed by btree_psitri.cpp without including the full amalgamation.
 *
 * These types mirror definitions in sqliteInt.h, btree.h, and btreeInt.h.
 * They must stay in sync with the amalgamation version used.
 */

#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ---- basic integer types (from sqliteInt.h) ---- */
typedef unsigned char      u8;
typedef signed char        i8;
typedef unsigned short     u16;
typedef short              i16;
typedef unsigned int       u32;
typedef sqlite3_int64      i64;
typedef sqlite3_uint64     u64;
typedef u32                Pgno;

/* ---- visibility ---- */
#ifndef SQLITE_PRIVATE
# define SQLITE_PRIVATE
#endif

/* Match amalgamation defaults for conditional compilation */
#ifndef SQLITE_MAX_MMAP_SIZE
# define SQLITE_MAX_MMAP_SIZE 0x7fff0000
#endif

/* ---- btree.h constants ---- */
#define SQLITE_N_BTREE_META     16

#define BTREE_AUTOVACUUM_NONE   0
#define BTREE_AUTOVACUUM_FULL   1
#define BTREE_AUTOVACUUM_INCR   2

#define BTREE_OMIT_JOURNAL  1
#define BTREE_MEMORY        2
#define BTREE_SINGLE        4
#define BTREE_UNORDERED     8

#define BTREE_INTKEY     1
#define BTREE_BLOBKEY    2

#define BTREE_FREE_PAGE_COUNT     0
#define BTREE_SCHEMA_VERSION      1
#define BTREE_FILE_FORMAT         2
#define BTREE_DEFAULT_CACHE_SIZE  3
#define BTREE_LARGEST_ROOT_PAGE   4
#define BTREE_TEXT_ENCODING       5
#define BTREE_USER_VERSION        6
#define BTREE_INCR_VACUUM         7
#define BTREE_APPLICATION_ID      8
#define BTREE_DATA_VERSION        15

#define BTREE_HINT_RANGE    0
#define BTREE_BULKLOAD      0x00000001
#define BTREE_SEEK_EQ       0x00000002
#define BTREE_WRCSR         0x00000004
#define BTREE_FORDELETE     0x00000008
#define BTREE_SAVEPOSITION  0x02
#define BTREE_AUXDELETE     0x04
#define BTREE_APPEND        0x08
#define BTREE_PREFORMAT     0x80

/* ---- transaction states (from btreeInt.h) ---- */
#define TRANS_NONE  0
#define TRANS_READ  1
#define TRANS_WRITE 2

/* ---- forward declarations ---- */
typedef struct Btree Btree;
typedef struct BtCursor BtCursor;
typedef struct BtShared BtShared;
typedef struct BtreePayload BtreePayload;
struct KeyInfo;
struct Schema;
struct Pager;

/* Mem == sqlite3_value (public typedef) */
typedef sqlite3_value Mem;

/* ---- BtreePayload (from btree.h) ---- */
struct BtreePayload {
   const void *pKey;
   sqlite3_int64 nKey;
   const void *pData;
   sqlite3_value *aMem;
   u16 nMem;
   int nData;
   int nZero;
};

/* ---- UnpackedRecord (from sqliteInt.h) ---- */
typedef struct UnpackedRecord UnpackedRecord;
struct UnpackedRecord {
   struct KeyInfo *pKeyInfo;
   Mem *aMem;
   union {
      char *z;
      i64 i;
   } u;
   int n;
   u16 nField;
   i8 default_rc;
   u8 errCode;
   i8 r1;
   i8 r2;
   u8 eqSeen;
};

/* ---- Internal SQLite functions we call ---- */
SQLITE_PRIVATE int sqlite3VdbeRecordCompare(int nKey1, const void *pKey1,
                                            UnpackedRecord *pPKey2);
SQLITE_PRIVATE int sqlite3PutVarint(unsigned char*, u64);
SQLITE_PRIVATE int sqlite3VarintLen(u64 v);

#ifdef __cplusplus
}
#endif
