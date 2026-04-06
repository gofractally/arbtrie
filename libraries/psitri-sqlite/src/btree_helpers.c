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
 * Encode a packed SQLite record into a byte-comparable key.
 *
 * Each field is encoded as a type tag followed by field-specific comparable
 * bytes.  Within a given index all records have the same field types, so
 * the type tags are constant and the ordering is determined by the data.
 *
 *   NULL:     0x05
 *   INTEGER:  0x15 + 8 byte big-endian with sign flip (XOR 0x80 on MSB)
 *   REAL:     0x25 + 8 byte comparable float
 *   TEXT:     0x35 + raw bytes + 0x00
 *   BLOB:     0x45 + raw bytes + 0x00
 *
 * If buf is NULL, returns the required buffer size.
 * Returns bytes written, or -1 on error.
 */
int psitri_make_comparable_key(const unsigned char *pKey, int nKey,
                               unsigned char *buf, int bufSize) {
   if (nKey < 1) return 0;

   u32 szHdr;
   u32 idx;
   idx = getVarint32(pKey, szHdr);
   if (szHdr > (u32)nKey) return -1;

   u32 d = szHdr; /* data offset */
   int out = 0;

   while (idx < szHdr) {
      u32 serial_type;
      idx += getVarint32(&pKey[idx], serial_type);
      u32 data_len = sqlite3VdbeSerialTypeLen(serial_type);

      /* Bounds check: ensure data fits within the record */
      if (d + data_len > (u32)nKey) break;

      if (serial_type == 0) {
         /* NULL */
         if (buf) {
            if (out >= bufSize) return -1;
            buf[out] = 0x05;
         }
         out++;
      } else if (serial_type >= 1 && serial_type <= 6) {
         /* Integer: decode, then encode as big-endian with sign flip */
         i64 val = vdbeRecordDecodeInt(serial_type, &pKey[d]);
         u64 u = (u64)val ^ ((u64)1 << 63);
         if (buf) {
            if (out + 9 > bufSize) return -1;
            buf[out] = 0x15;
            int j;
            for (j = 0; j < 8; j++)
               buf[out+1+j] = (unsigned char)(u >> (56 - j*8));
         }
         out += 9;
      } else if (serial_type == 7) {
         /* Real: read as big-endian double, apply sign-magnitude transform */
         u64 v = 0;
         int j;
         for (j = 0; j < 8; j++)
            v = (v << 8) | pKey[d + j];
         if (v & ((u64)1 << 63)) {
            v = ~v;               /* negative: invert all bits */
         } else {
            v ^= ((u64)1 << 63);  /* positive: flip sign bit only */
         }
         if (buf) {
            if (out + 9 > bufSize) return -1;
            buf[out] = 0x25;
            for (j = 0; j < 8; j++)
               buf[out+1+j] = (unsigned char)(v >> (56 - j*8));
         }
         out += 9;
      } else if (serial_type >= 8 && serial_type <= 9) {
         /* Integer 0 or 1 (in-header, no data bytes) */
         u64 u = (u64)(serial_type - 8) ^ ((u64)1 << 63);
         if (buf) {
            if (out + 9 > bufSize) return -1;
            buf[out] = 0x15;
            int j;
            for (j = 0; j < 8; j++)
               buf[out+1+j] = (unsigned char)(u >> (56 - j*8));
         }
         out += 9;
         /* data_len is 0 for serial types 8 and 9 */
      } else if (serial_type >= 12) {
         int n = (int)data_len;
         int is_text = serial_type & 1;
         u8 tag = is_text ? 0x35 : 0x45;
         if (buf) {
            if (out + 1 + n + 1 > bufSize) return -1;
            buf[out] = tag;
            if (n > 0) memcpy(buf + out + 1, &pKey[d], n);
            buf[out + 1 + n] = 0x00; /* terminator */
         }
         out += 1 + n + 1;
      }

      d += data_len;
   }

   return out;
}

/*
 * Encode an UnpackedRecord into a byte-comparable key.
 * Same format as psitri_make_comparable_key but from an UnpackedRecord.
 *
 * If buf is NULL, returns the required buffer size.
 */
int psitri_make_comparable_key_from_unpacked(UnpackedRecord *pRec,
                                             unsigned char *buf, int bufSize) {
   int nField = pRec->nField;
   Mem *aMem = pRec->aMem;
   int out = 0;
   int i;

   for (i = 0; i < nField; i++) {
      Mem *pMem = &aMem[i];
      int flags = pMem->flags;

      if (flags & MEM_Null) {
         if (buf) { if (out >= bufSize) return -1; buf[out] = 0x05; }
         out++;
      } else if (flags & (MEM_Int | MEM_IntReal)) {
         u64 u = (u64)pMem->u.i ^ ((u64)1 << 63);
         if (buf) {
            int j;
            if (out + 9 > bufSize) return -1;
            buf[out] = 0x15;
            for (j = 0; j < 8; j++)
               buf[out+1+j] = (unsigned char)(u >> (56 - j*8));
         }
         out += 9;
      } else if (flags & MEM_Real) {
         /* Convert host-endian double to big-endian comparable */
         u64 v;
         int j;
         memcpy(&v, &pMem->u.r, 8);
         /* Byte-swap to big-endian */
         u64 be = 0;
         for (j = 0; j < 8; j++)
            be = (be << 8) | ((v >> (j*8)) & 0xFF);
         if (be & ((u64)1 << 63)) {
            be = ~be;
         } else {
            be ^= ((u64)1 << 63);
         }
         if (buf) {
            if (out + 9 > bufSize) return -1;
            buf[out] = 0x25;
            for (j = 0; j < 8; j++)
               buf[out+1+j] = (unsigned char)(be >> (56 - j*8));
         }
         out += 9;
      } else if (flags & MEM_Str) {
         int n = pMem->n;
         if (buf) {
            if (out + 1 + n + 1 > bufSize) return -1;
            buf[out] = 0x35;
            if (n > 0 && pMem->z) memcpy(buf + out + 1, pMem->z, n);
            buf[out + 1 + n] = 0x00;
         }
         out += 1 + n + 1;
      } else if (flags & MEM_Blob) {
         int n = pMem->n;
         if (buf) {
            if (out + 1 + n + 1 > bufSize) return -1;
            buf[out] = 0x45;
            if (n > 0 && pMem->z) memcpy(buf + out + 1, pMem->z, n);
            buf[out + 1 + n] = 0x00;
         }
         out += 1 + n + 1;
      } else {
         if (buf) { if (out >= bufSize) return -1; buf[out] = 0x05; }
         out++;
      }
   }

   return out;
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
