/**
 * btree_helpers.c — C helper functions compiled with access to SQLite internals.
 *
 * These are called from btree_psitri.cpp but need the Mem struct definition
 * (sqlite3_value) which is incomplete in the public header.
 *
 * Must be compiled as C, not C++.
 */
#define SQLITE_CORE 1
#include "../sqlite3/sqlite3.c"  /* Get all internal definitions (with btree.c #if 0'd) */

#include <string.h>

/*
 * Pack an UnpackedRecord into a binary key blob suitable for B-tree index
 * lookup. This replicates what OP_MakeRecord does, but from an UnpackedRecord.
 *
 * buf must be pre-allocated. Returns the number of bytes written, or -1 on error.
 * If buf is NULL, returns the required buffer size.
 */
int psitri_pack_record(UnpackedRecord *pRec, unsigned char *buf, int bufSize) {
   int nField = pRec->nField;
   Mem *aMem = pRec->aMem;
   int i;

   /* Pass 1: compute serial types and sizes */
   u32 nHdr = 0;
   u32 nData = 0;
   u32 serialTypes[128]; /* max fields we support */
   if (nField > 128) nField = 128;

   for (i = 0; i < nField; i++) {
      Mem *pMem = &aMem[i];
      u32 len;
      u32 st;
      int flags = pMem->flags;

      if (flags & MEM_Null) {
         st = 0; len = 0;
      } else if (flags & (MEM_Int | MEM_IntReal)) {
         i64 iv = pMem->u.i;
         u64 u = iv < 0 ? ~(u64)iv : (u64)iv;
         if (u <= 127) {
            if ((iv & 1) == iv) { st = 8 + (u32)u; len = 0; }
            else { st = 1; len = 1; }
         } else if (u <= 32767)      { st = 2; len = 2; }
           else if (u <= 8388607)    { st = 3; len = 3; }
           else if (u <= 2147483647) { st = 4; len = 4; }
           else if (u <= 0x7FFFFFFFFFFFLL) { st = 5; len = 6; }
           else { st = 6; len = 8; }
      } else if (flags & MEM_Real) {
         st = 7; len = 8;
      } else if (flags & MEM_Str) {
         u32 n = (u32)pMem->n;
         if (flags & MEM_Zero) n += pMem->u.nZero;
         st = n * 2 + 13; len = n;
      } else if (flags & MEM_Blob) {
         u32 n = (u32)pMem->n;
         if (flags & MEM_Zero) n += pMem->u.nZero;
         st = n * 2 + 12; len = n;
      } else {
         st = 0; len = 0;
      }
      serialTypes[i] = st;
      nData += len;
      nHdr += sqlite3VarintLen(st);
   }

   /* Header size includes its own varint length */
   nHdr += sqlite3VarintLen(nHdr + 1); /* +1 for minimum self-length */
   /* Recheck if the header size varint grew */
   {
      int hdrLen = sqlite3VarintLen(nHdr);
      /* Recalculate: the header consists of hdrLen + sum of serial type varints */
      u32 hdrContent = 0;
      for (i = 0; i < nField; i++) {
         hdrContent += sqlite3VarintLen(serialTypes[i]);
      }
      nHdr = hdrLen + hdrContent;
      if ((u32)sqlite3VarintLen(nHdr) > (u32)hdrLen) {
         nHdr++;
      }
   }

   int total = (int)(nHdr + nData);
   if (!buf) return total;
   if (total > bufSize) return -1;

   /* Write header */
   int idx = sqlite3PutVarint(buf, nHdr);
   for (i = 0; i < nField; i++) {
      idx += sqlite3PutVarint(buf + idx, serialTypes[i]);
   }

   /* Write data */
   for (i = 0; i < nField; i++) {
      Mem *pMem = &aMem[i];
      u32 st = serialTypes[i];

      if (st == 0 || (st >= 8 && st <= 9)) {
         /* NULL, integer 0, or integer 1 — no body bytes */
         continue;
      }
      if (st == 7) {
         /* float — 8 bytes big-endian */
         u64 v;
         memcpy(&v, &pMem->u.r, 8);
         buf[idx++] = (unsigned char)(v >> 56);
         buf[idx++] = (unsigned char)(v >> 48);
         buf[idx++] = (unsigned char)(v >> 40);
         buf[idx++] = (unsigned char)(v >> 32);
         buf[idx++] = (unsigned char)(v >> 24);
         buf[idx++] = (unsigned char)(v >> 16);
         buf[idx++] = (unsigned char)(v >> 8);
         buf[idx++] = (unsigned char)(v);
         continue;
      }
      if (st >= 12) {
         /* blob or text */
         u32 n = (st - 12) / 2;
         if (n > 0 && pMem->z) {
            memcpy(buf + idx, pMem->z, n);
            idx += n;
         }
         continue;
      }
      /* integer: 1,2,3,4,5,6 → 1,2,3,4,6,8 bytes */
      {
         static const int sizes[] = {0, 1, 2, 3, 4, 6, 8};
         int n = sizes[st];
         i64 iv = pMem->u.i;
         u64 v = (u64)iv;
         int j;
         for (j = n - 1; j >= 0; j--) {
            buf[idx + j] = (unsigned char)(v & 0xFF);
            v >>= 8;
         }
         idx += n;
      }
   }

   return idx;
}

/*
 * Return sizeof(sqlite3_value) so C++ code can do pointer arithmetic.
 */
int psitri_sizeof_sqlite3_value(void) {
   return (int)sizeof(sqlite3_value);
}

/*
 * Return a pointer to a static Pager configured as an in-memory no-op.
 * psitri doesn't use SQLite's pager, but many amalgamation functions call
 * sqlite3BtreePager() and dereference fields. Key settings:
 *   - noLock=1: prevents file locking calls
 *   - memDb=1: inhibits all file I/O
 *   - tempFile=1: marks as temporary
 *   - journalMode=OFF: prevents journal operations
 *   - eState=PAGER_OPEN: safe default state
 *   - fd points to a static sqlite3_file with pMethods=NULL (isOpen returns false)
 */
static Pager g_dummy_pager;
static sqlite3_file g_dummy_fd;
static int g_dummy_pager_initialized = 0;

struct Pager* psitri_get_dummy_pager(void) {
   if (!g_dummy_pager_initialized) {
      memset(&g_dummy_pager, 0, sizeof(g_dummy_pager));
      memset(&g_dummy_fd, 0, sizeof(g_dummy_fd));
      /* g_dummy_fd.pMethods = NULL → isOpen() returns false */
      g_dummy_pager.fd = &g_dummy_fd;
      g_dummy_pager.jfd = &g_dummy_fd;
      g_dummy_pager.sjfd = &g_dummy_fd;
      g_dummy_pager.noLock = 1;
      g_dummy_pager.memDb = 1;
      g_dummy_pager.tempFile = 1;
      g_dummy_pager.journalMode = PAGER_JOURNALMODE_OFF;
      g_dummy_pager.eState = PAGER_OPEN;
      g_dummy_pager.eLock = EXCLUSIVE_LOCK;  /* Already "locked" */
      g_dummy_pager.hasHeldSharedLock = 1;
      g_dummy_pager_initialized = 1;
   }
   return &g_dummy_pager;
}
