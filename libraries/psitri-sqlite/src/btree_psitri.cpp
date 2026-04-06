/**
 * btree_psitri.cpp — Drop-in replacement for SQLite's btree.c
 *
 * Implements the btree.h interface using psitri's DWAL as the storage engine.
 * SQLite's parser, code generator, and VDBE remain unchanged.
 *
 * Architecture:
 *   Btree*     → shared dwal_database (singleton per path)
 *   Root page  → DWAL root index
 *   BtCursor   → merge_cursor on a specific DWAL root
 *
 * DWAL root 0 is reserved for database metadata (schema version, etc.).
 * Roots 1+ are allocated by sqlite3BtreeCreateTable() for tables/indexes.
 *
 * Table B-trees (BTREE_INTKEY): key = big-endian int64 rowid, value = record blob
 * Index B-trees (BTREE_BLOBKEY): key = raw index key blob, value = empty
 */

#include <psitri-sqlite/sqlite3_btree_compat.h>

// C helper functions defined in btree_helpers.c (compiled with full Mem access)
extern "C" {
int psitri_pack_record(UnpackedRecord *pRec, unsigned char *buf, int bufSize);
int psitri_make_comparable_key(const unsigned char *pKey, int nKey,
                               unsigned char *buf, int bufSize);
int psitri_make_comparable_key_from_unpacked(UnpackedRecord *pRec,
                                             unsigned char *buf, int bufSize);
int psitri_sizeof_sqlite3_value(void);
struct Pager* psitri_get_dummy_pager(void);
}

#include <psitri/dwal/dwal_database.hpp>
#include <psitri/dwal/dwal_transaction.hpp>
#include <psitri/dwal/merge_cursor.hpp>
#include <psitri/database.hpp>
#include <sal/config.hpp>

#include <atomic>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>
#include <filesystem>
#include <unistd.h>

// ============================================================================
// Global singleton registry
// ============================================================================

struct PsitriDb {
   std::shared_ptr<psitri::database>            psi_db;
   std::shared_ptr<psitri::dwal::dwal_database> dwal_db;
   uint32_t next_root = 2;  // 0 = metadata, 1 = sqlite_schema (reserved)
   u32 meta[SQLITE_N_BTREE_META] = {};
   int ref_count = 0;
   std::atomic<u32> data_version{1};
   sal::sync_type sync_mode = sal::sync_type::none;

   // Open DWAL transactions for the current SQLite write transaction.
   // Lazily created per root on first Insert/Delete, committed together
   // during sqlite3BtreeCommitPhaseOne, aborted on Rollback.
   std::map<uint32_t, psitri::dwal::dwal_transaction> open_tx;

   psitri::dwal::dwal_transaction& get_tx(uint32_t root_index) {
      auto it = open_tx.find(root_index);
      if (it != open_tx.end())
         return it->second;
      auto [inserted, ok] = open_tx.try_emplace(
         root_index, dwal_db->start_write_transaction(root_index));
      return inserted->second;
   }

   void commit_all_tx() {
      for (auto& [root, tx] : open_tx)
         tx.commit();
      open_tx.clear();
   }

   void abort_all_tx() {
      for (auto& [root, tx] : open_tx)
         tx.abort();
      open_tx.clear();
   }
};

static std::mutex g_db_mutex;
static std::map<std::string, std::shared_ptr<PsitriDb>> g_databases;

// ============================================================================
// Btree and BtShared structures
// ============================================================================

struct BtShared {
   PsitriDb*    psitri = nullptr;
   sqlite3*     db = nullptr;
   BtCursor*    pCursor = nullptr;   // linked list of open cursors
   u8           inTransaction = 0;   // TRANS_NONE/READ/WRITE
   u32          pageSize = 4096;
   u32          nPage = 100;
   void*        pSchema = nullptr;
   void(*xFreeSchema)(void*) = nullptr;
};

struct Btree {
   sqlite3*     db = nullptr;
   BtShared*    pBt = nullptr;
   u8           inTrans = 0;
   u8           sharable = 0;
   u8           locked = 0;
   u8           hasIncrblobCur = 0;
   int          wantToLock = 0;
   int          nBackup = 0;
   u32          iBDataVersion = 0;
   Btree*       pNext = nullptr;
   Btree*       pPrev = nullptr;
#ifdef SQLITE_DEBUG
   u64          nSeek = 0;
#endif
};

// ============================================================================
// Key encoding: rowid ↔ big-endian 8-byte key
// ============================================================================

static std::string encode_rowid(i64 rowid) {
   u64 v = (u64)rowid ^ ((u64)1 << 63);
   char buf[8];
   for (int i = 7; i >= 0; i--) { buf[i] = (char)(v & 0xFF); v >>= 8; }
   return std::string(buf, 8);
}

static i64 decode_rowid(const char* data) {
   u64 v = 0;
   for (int i = 0; i < 8; i++) v = (v << 8) | (u8)data[i];
   return (i64)(v ^ ((u64)1 << 63));
}

// ============================================================================
// BtCursor — wraps a psitri merge cursor
// ============================================================================

#define CURSOR_INVALID  0
#define CURSOR_VALID    1
#define CURSOR_FAULT    4

struct BtCursor {
   /* Fields zeroed by CursorZero (up to pBt) */
   u8           eState = CURSOR_INVALID;
   u8           curFlags = 0;
   u8           curPagerFlags = 0;
   u8           hints = 0;
   int          skipNext = 0;
   Btree*       pBtree = nullptr;
   void*        aOverflow = nullptr;
   void*        pKey = nullptr;
   /* --- end of zeroed region --- */
   BtShared*    pBt = nullptr;
   BtCursor*    pNext = nullptr;
   i64          nKey = 0;
   Pgno         pgnoRoot = 0;
   u8           curIntKey = 0;
   struct KeyInfo* pKeyInfo = nullptr;

   /* psitri-specific state */
   PsitriDb*                                    psitri_db = nullptr;
   std::optional<psitri::dwal::owned_merge_cursor> cursor;
   std::string                                  cur_value;
   bool                                         is_valid = false;
   int                                          wrFlag = 0;
};

// ============================================================================
// Helper: read value from merge cursor (RW/RO/Tri)
// ============================================================================

static std::string read_mc_value(psitri::dwal::merge_cursor& mc) {
   auto src = mc.current_source();
   if (src == psitri::dwal::merge_cursor::source::rw ||
       src == psitri::dwal::merge_cursor::source::ro) {
      return std::string(mc.current_value().data);
   }
   std::string result;
   auto* tri = mc.tri_cursor();
   if (tri) {
      tri->get_value([&](psitri::value_view vv) {
         result.assign(reinterpret_cast<const char*>(vv.data()), vv.size());
      });
   }
   return result;
}

static void cache_cursor_value(BtCursor* pCur) {
   if (!pCur->cursor || pCur->cursor->cursor().is_end()) {
      pCur->is_valid = false;
      pCur->eState = CURSOR_INVALID;
      return;
   }
   auto& mc = pCur->cursor->cursor();
   auto key_sv = mc.key();

   if (pCur->curIntKey) {
      if (key_sv.size() >= 8) {
         pCur->nKey = decode_rowid(key_sv.data());
      }
      pCur->cur_value = read_mc_value(mc);
   } else {
      // BLOBKEY: the DWAL key is a byte-comparable encoding.
      // The DWAL value contains the original packed record that SQLite
      // expects to read back via sqlite3BtreePayload().
      auto val = read_mc_value(mc);
      pCur->nKey = (i64)val.size();
      pCur->cur_value = std::move(val);
   }
   pCur->is_valid = true;
   pCur->eState = CURSOR_VALID;
}

// ============================================================================
// Index key packing (delegates to C helper with full Mem access)
// ============================================================================

static std::string pack_index_key(UnpackedRecord* pRec) {
   // First call to get required size
   int sz = psitri_pack_record(pRec, nullptr, 0);
   if (sz <= 0) return {};
   std::string result(sz, '\0');
   int actual = psitri_pack_record(pRec, (unsigned char*)result.data(), sz);
   if (actual > 0) result.resize(actual);
   return result;
}

// Convert a packed SQLite record to a byte-comparable key
static std::string make_comparable_from_packed(const char* data, int len) {
   int sz = psitri_make_comparable_key(
      (const unsigned char*)data, len, nullptr, 0);
   if (sz <= 0) return {};
   std::string result(sz, '\0');
   int written = psitri_make_comparable_key(
      (const unsigned char*)data, len,
      (unsigned char*)result.data(), sz);
   if (written > 0) result.resize(written);
   return result;
}

// Convert an UnpackedRecord to a byte-comparable key
static std::string make_comparable_from_unpacked(UnpackedRecord* pRec) {
   int sz = psitri_make_comparable_key_from_unpacked(pRec, nullptr, 0);
   if (sz <= 0) return {};
   std::string result(sz, '\0');
   psitri_make_comparable_key_from_unpacked(
      pRec, (unsigned char*)result.data(), sz);
   return result;
}

// ============================================================================
// Btree lifecycle
// ============================================================================

#define PSITRI_TRACE 0
#if PSITRI_TRACE
#define PTRACE(...) std::fprintf(stderr, "[psitri-btree] " __VA_ARGS__)
#else
#define PTRACE(...) ((void)0)
#endif

extern "C" {

// Forward declarations for functions used before they're defined
int sqlite3BtreeCloseCursor(BtCursor*);
int sqlite3BtreeTableMoveto(BtCursor*, i64, int, int*);

int sqlite3BtreeOpen(
   sqlite3_vfs* pVfs, const char* zFilename, sqlite3* db,
   Btree** ppBtree, int flags, int vfsFlags
) {
   auto* pBtree = new Btree;
   pBtree->db = db;

   auto* pBt = new BtShared;
   pBt->db = db;
   pBtree->pBt = pBt;

   bool is_memory = (flags & BTREE_MEMORY) || !zFilename || zFilename[0] == 0;
   std::string path;

   if (is_memory) {
      static int mem_counter = 0;
      path = (std::filesystem::temp_directory_path() /
              ("psitri_mem_" + std::to_string(getpid()) + "_" +
               std::to_string(mem_counter++))).string();
   } else {
      path = zFilename;
   }

   std::lock_guard<std::mutex> lock(g_db_mutex);
   auto it = g_databases.find(path);
   if (it != g_databases.end()) {
      pBt->psitri = it->second.get();
      it->second->ref_count++;
   } else {
      auto pdb = std::make_shared<PsitriDb>();
      try {
         std::filesystem::create_directories(path);
         pdb->psi_db = psitri::database::open(
            path + "/data", psitri::open_mode::create_or_open);
         psitri::dwal::dwal_config cfg;
         cfg.max_rw_entries = 200000;
         pdb->dwal_db = std::make_shared<psitri::dwal::dwal_database>(
            pdb->psi_db, path + "/wal", cfg);

         // Load metadata from root 0
         auto meta_r = pdb->dwal_db->get_latest(0, "meta");
         if (meta_r.found && meta_r.value.is_data() &&
             meta_r.value.data.size() >= sizeof(pdb->meta)) {
            std::memcpy(pdb->meta, meta_r.value.data.data(), sizeof(pdb->meta));
            PTRACE("BtreeOpen: loaded metadata from root 0 (schema_version=%d)\n",
                   pdb->meta[1]);
         } else {
            pdb->meta[BTREE_FILE_FORMAT] = 4;
            pdb->meta[BTREE_TEXT_ENCODING] = SQLITE_UTF8;
            PTRACE("BtreeOpen: no metadata found — using defaults\n");
         }

         auto root_r = pdb->dwal_db->get_latest(0, "next_root");
         if (root_r.found && root_r.value.is_data() &&
             root_r.value.data.size() >= 4) {
            std::memcpy(&pdb->next_root, root_r.value.data.data(), 4);
            PTRACE("BtreeOpen: loaded next_root=%d\n", pdb->next_root);
         } else {
            PTRACE("BtreeOpen: no next_root found — starting at 2\n");
         }
      } catch (const std::exception&) {
         delete pBt;
         delete pBtree;
         return SQLITE_CANTOPEN;
      }
      pdb->ref_count = 1;
      g_databases[path] = pdb;
      pBt->psitri = pdb.get();
   }

   PTRACE("BtreeOpen: path=%s pBtree=%p pBt=%p psitri=%p\n",
          path.c_str(), (void*)pBtree, (void*)pBt, (void*)pBt->psitri);
   *ppBtree = pBtree;
   return SQLITE_OK;
}

int sqlite3BtreeClose(Btree* p) {
   if (!p) return SQLITE_OK;
   PTRACE("BtreeClose: pBtree=%p\n", (void*)p);

   auto* pBt = p->pBt;
   while (pBt->pCursor) {
      sqlite3BtreeCloseCursor(pBt->pCursor);
   }

   // Persist metadata and release global reference
   if (pBt->psitri && pBt->psitri->dwal_db) {
      try {
         auto tx = pBt->psitri->dwal_db->start_write_transaction(0);
         tx.upsert("meta", std::string_view(
            reinterpret_cast<const char*>(pBt->psitri->meta),
            sizeof(pBt->psitri->meta)));
         std::string nr(4, '\0');
         std::memcpy(nr.data(), &pBt->psitri->next_root, 4);
         tx.upsert("next_root", nr);
         tx.commit();
      } catch (...) {}

      std::lock_guard<std::mutex> lock(g_db_mutex);
      pBt->psitri->ref_count--;
      PTRACE("BtreeClose: ref_count now %d for psitri=%p\n",
             pBt->psitri->ref_count, (void*)pBt->psitri);
      if (pBt->psitri->ref_count <= 0) {
         PTRACE("BtreeClose: last ref — destroying PsitriDb\n");
         // Last reference — remove from global registry (destroys PsitriDb)
         for (auto it = g_databases.begin(); it != g_databases.end(); ++it) {
            if (it->second.get() == pBt->psitri) {
               PTRACE("BtreeClose: psi_db use_count=%ld dwal_db use_count=%ld\n",
                      it->second->psi_db.use_count(), it->second->dwal_db.use_count());
               // Clear thread-local caches that hold shared_ptr refs to the database.
               // Must be done before destruction so the shared_ptrs can reach zero.
               it->second->dwal_db->clear_thread_local_caches();
               // Reset dwal_db first (stops merge threads, releases database ref)
               it->second->dwal_db.reset();
               PTRACE("BtreeClose: dwal_db reset, psi_db use_count=%ld\n",
                      it->second->psi_db.use_count());
               // Now reset psi_db (releases file locks)
               it->second->psi_db.reset();
               PTRACE("BtreeClose: psi_db reset — locks released\n");
               g_databases.erase(it);
               break;
            }
         }
      }
      pBt->psitri = nullptr;
   }

   if (pBt->pSchema && pBt->xFreeSchema) {
      pBt->xFreeSchema(pBt->pSchema);
   }

   delete pBt;
   delete p;
   return SQLITE_OK;
}

// ============================================================================
// Configuration (mostly no-ops for psitri)
// ============================================================================

int sqlite3BtreeSetCacheSize(Btree*, int) { return SQLITE_OK; }
int sqlite3BtreeSetSpillSize(Btree*, int) { return SQLITE_OK; }
#if SQLITE_MAX_MMAP_SIZE>0
int sqlite3BtreeSetMmapLimit(Btree*, sqlite3_int64) { return SQLITE_OK; }
#endif
int sqlite3BtreeSetPagerFlags(Btree* p, unsigned pgFlags) {
   // Maps PRAGMA synchronous + PRAGMA fullfsync to psitri sync modes.
   //
   // Safety level (low 3 bits, PAGER_SYNCHRONOUS_MASK):
   //   OFF(1)    → sync_type::none        — no flush
   //   NORMAL(2) → sync_type::msync_async — write buffer flushed, no fsync
   //   FULL(3)   → sync_type::fsync       — fsync WAL per commit
   //   EXTRA(4)  → sync_type::fsync       — fsync + directory sync
   //
   // PAGER_FULLFSYNC (0x08) from PRAGMA fullfsync=ON:
   //   Upgrades fsync → F_FULLFSYNC (data to physical media)
   if (!p || !p->pBt || !p->pBt->psitri) return SQLITE_OK;
   unsigned level = pgFlags & 0x07;
   bool fullfsync = (pgFlags & 0x08) != 0;  // PAGER_FULLFSYNC
   switch (level) {
      case 0x01: p->pBt->psitri->sync_mode = sal::sync_type::none; break;
      case 0x02: p->pBt->psitri->sync_mode = sal::sync_type::msync_async; break;
      case 0x03: // fall through
      case 0x04:
         p->pBt->psitri->sync_mode = fullfsync ? sal::sync_type::full
                                                : sal::sync_type::fsync;
         break;
      default:   break;
   }
   return SQLITE_OK;
}
int sqlite3BtreeSetPageSize(Btree*, int, int, int) { return SQLITE_OK; }
int sqlite3BtreeGetPageSize(Btree*) { return 4096; }
Pgno sqlite3BtreeMaxPageCount(Btree*, Pgno) { return 0xFFFFFFFF; }
Pgno sqlite3BtreeLastPage(Btree*) { return 100; }
int sqlite3BtreeSecureDelete(Btree*, int) { return 0; }
int sqlite3BtreeGetRequestedReserve(Btree*) { return 0; }
int sqlite3BtreeGetReserveNoMutex(Btree*) { return 0; }
int sqlite3BtreeSetAutoVacuum(Btree*, int) { return SQLITE_OK; }
int sqlite3BtreeGetAutoVacuum(Btree*) { return BTREE_AUTOVACUUM_NONE; }
int sqlite3BtreeIsReadonly(Btree*) { return 0; }
int sqlite3BtreeSetVersion(Btree*, int) { return SQLITE_OK; }
int sqlite3BtreeIsInBackup(Btree*) { return 0; }
const char* sqlite3BtreeGetFilename(Btree*) { return ""; }
const char* sqlite3BtreeGetJournalname(Btree*) { return ""; }
// sqlite3BtreeCopyFile is defined in backup.c (part of amalgamation)
int sqlite3BtreeIncrVacuum(Btree*) { return SQLITE_DONE; }
int sqlite3HeaderSizeBtree(void) { return 0; }
int sqlite3BtreeNewDb(Btree*) { return SQLITE_OK; }
void sqlite3BtreeClearCache(Btree*) {}
struct Pager* sqlite3BtreePager(Btree*) { return psitri_get_dummy_pager(); }

// ============================================================================
// Transactions
// ============================================================================

int sqlite3BtreeTxnState(Btree* p) {
   return p ? p->inTrans : 0;
}

int sqlite3BtreeBeginTrans(Btree* p, int wrflag, int* pSchemaVersion) {
   PTRACE("BeginTrans: wrflag=%d inTrans=%d open_tx=%zu\n",
          wrflag, (int)p->inTrans, p->pBt->psitri->open_tx.size());
   if (wrflag) {
      p->inTrans = TRANS_WRITE;
   } else if (p->inTrans == TRANS_NONE) {
      p->inTrans = TRANS_READ;
   }
   p->pBt->inTransaction = p->inTrans;

   if (pSchemaVersion) {
      *pSchemaVersion = (int)p->pBt->psitri->meta[BTREE_SCHEMA_VERSION];
   }
   return SQLITE_OK;
}

int sqlite3BtreeCommitPhaseOne(Btree* p, const char*) {
   PTRACE("CommitPhaseOne: open_tx=%zu\n",
          (p && p->pBt && p->pBt->psitri) ? p->pBt->psitri->open_tx.size() : 0);
   if (p && p->pBt && p->pBt->psitri) {
      auto* psitri = p->pBt->psitri;
      try {
         psitri->commit_all_tx();
      } catch (const std::exception& e) {
         PTRACE("CommitPhaseOne: commit_all_tx THREW: %s\n", e.what());
         return SQLITE_ERROR;
      }
      // Sync WAL if sync mode requires it
      if (psitri->sync_mode >= sal::sync_type::fsync) {
         psitri->dwal_db->flush_wal(psitri->sync_mode);
      }
   }
   return SQLITE_OK;
}

int sqlite3BtreeCommitPhaseTwo(Btree* p, int) {
   PTRACE("CommitPhaseTwo: inTrans=%d\n", p ? (int)p->inTrans : -1);
   if (p) {
      p->inTrans = TRANS_NONE;
      p->pBt->inTransaction = TRANS_NONE;
   }
   return SQLITE_OK;
}

int sqlite3BtreeCommit(Btree* p) {
   sqlite3BtreeCommitPhaseOne(p, nullptr);
   return sqlite3BtreeCommitPhaseTwo(p, 0);
}

int sqlite3BtreeRollback(Btree* p, int, int) {
   if (p && p->pBt && p->pBt->psitri) {
      p->pBt->psitri->abort_all_tx();
   }
   if (p) {
      p->inTrans = TRANS_NONE;
      p->pBt->inTransaction = TRANS_NONE;
   }
   return SQLITE_OK;
}

int sqlite3BtreeBeginStmt(Btree*, int) { return SQLITE_OK; }
int sqlite3BtreeSavepoint(Btree*, int, int) { return SQLITE_OK; }

#ifndef SQLITE_OMIT_WAL
int sqlite3BtreeCheckpoint(Btree*, int, int* pnLog, int* pnCkpt) {
   if (pnLog) *pnLog = 0;
   if (pnCkpt) *pnCkpt = 0;
   return SQLITE_OK;
}
#endif

// ============================================================================
// Schema and metadata
// ============================================================================

void* sqlite3BtreeSchema(Btree* p, int nBytes, void(*xFree)(void*)) {
   auto* pBt = p->pBt;
   if (!pBt->pSchema && nBytes > 0) {
      pBt->pSchema = sqlite3_malloc64(nBytes);
      if (pBt->pSchema) std::memset(pBt->pSchema, 0, nBytes);
      pBt->xFreeSchema = xFree;
   }
   return pBt->pSchema;
}

int sqlite3BtreeSchemaLocked(Btree*) { return 0; }

#ifndef SQLITE_OMIT_SHARED_CACHE
int sqlite3BtreeLockTable(Btree*, int, u8) { return SQLITE_OK; }
#endif

void sqlite3BtreeGetMeta(Btree* p, int idx, u32* pValue) {
   PTRACE("GetMeta: idx=%d\n", idx);
   if (idx >= 0 && idx < SQLITE_N_BTREE_META) {
      if (idx == BTREE_DATA_VERSION) {
         *pValue = p->pBt->psitri->data_version.load();
      } else {
         *pValue = p->pBt->psitri->meta[idx];
      }
   } else {
      *pValue = 0;
   }
}

int sqlite3BtreeUpdateMeta(Btree* p, int idx, u32 value) {
   if (idx >= 0 && idx < SQLITE_N_BTREE_META) {
      p->pBt->psitri->meta[idx] = value;
      p->pBt->psitri->data_version.fetch_add(1);
   }
   return SQLITE_OK;
}

// ============================================================================
// Table/index creation and deletion
// ============================================================================

int sqlite3BtreeCreateTable(Btree* p, Pgno* piTable, int flags) {
   PTRACE("CreateTable: flags=%d\n", flags);
   auto* psitri = p->pBt->psitri;
   *piTable = psitri->next_root++;

   try {
      auto tx = psitri->dwal_db->start_write_transaction(0);
      std::string nr(4, '\0');
      std::memcpy(nr.data(), &psitri->next_root, 4);
      tx.upsert("next_root", nr);
      std::string key = "tflags_" + std::to_string(*piTable);
      char f = (char)flags;
      tx.upsert(key, std::string_view(&f, 1));
      tx.commit();
   } catch (...) {}

   return SQLITE_OK;
}

int sqlite3BtreeDropTable(Btree*, int, int* piMoved) {
   if (piMoved) *piMoved = 0;
   return SQLITE_OK;
}

int sqlite3BtreeClearTable(Btree* p, int iTable, i64* pnChange) {
   auto* psitri = p->pBt->psitri;
   try {
      auto cursor = psitri->dwal_db->create_cursor(
         (uint32_t)iTable, psitri::dwal::read_mode::latest);
      auto& mc = cursor.cursor();
      mc.seek_begin();
      std::vector<std::string> keys;
      i64 count = 0;
      while (!mc.is_end()) {
         keys.push_back(std::string(mc.key()));
         mc.next();
         count++;
      }
      if (!keys.empty()) {
         auto tx = psitri->dwal_db->start_write_transaction((uint32_t)iTable);
         for (auto& k : keys) tx.remove(k);
         tx.commit();
      }
      if (pnChange) *pnChange = count;
   } catch (...) {
      if (pnChange) *pnChange = 0;
   }
   return SQLITE_OK;
}

int sqlite3BtreeClearTableOfCursor(BtCursor* pCur) {
   return sqlite3BtreeClearTable(pCur->pBtree, pCur->pgnoRoot, nullptr);
}

int sqlite3BtreeTripAllCursors(Btree* p, int errCode, int) {
   auto* pBt = p->pBt;
   for (auto* cur = pBt->pCursor; cur; cur = cur->pNext) {
      cur->eState = CURSOR_FAULT;
      cur->skipNext = errCode;
      cur->is_valid = false;
   }
   return SQLITE_OK;
}

// ============================================================================
// Cursors
// ============================================================================

int sqlite3BtreeCursorSize(void) {
   return (int)sizeof(BtCursor);
}

void sqlite3BtreeCursorZero(BtCursor* p) {
   size_t offset = offsetof(BtCursor, pBt);
   std::memset((void*)p, 0, offset);
}

BtCursor* sqlite3BtreeFakeValidCursor(void) {
   // Static storage — C++ members are properly constructed here
   static BtCursor fake{};
   fake.eState = CURSOR_VALID;
   return &fake;
}

int sqlite3BtreeCursor(
   Btree* p, Pgno iTable, int wrFlag, struct KeyInfo* pKeyInfo,
   BtCursor* pCur
) {
   PTRACE("BtreeCursor: pCur=%p root=%u wrFlag=%d intkey=%d\n",
          (void*)pCur, iTable, wrFlag, pKeyInfo == nullptr);
   // BtCursor was allocated by SQLite via malloc+memset, so C++ members
   // (std::optional, std::string) need placement-new construction.
   new (&pCur->cursor) std::optional<psitri::dwal::owned_merge_cursor>();
   new (&pCur->cur_value) std::string();

   pCur->pBtree = p;
   pCur->pBt = p->pBt;
   pCur->pgnoRoot = iTable;
   pCur->pKeyInfo = pKeyInfo;
   pCur->curIntKey = (pKeyInfo == nullptr) ? 1 : 0;
   pCur->eState = CURSOR_INVALID;
   pCur->is_valid = false;
   pCur->wrFlag = wrFlag;
   pCur->psitri_db = p->pBt->psitri;

   pCur->pNext = p->pBt->pCursor;
   p->pBt->pCursor = pCur;

   return SQLITE_OK;
}

int sqlite3BtreeCloseCursor(BtCursor* pCur) {
   if (!pCur) return SQLITE_OK;

   auto* pBt = pCur->pBt;
   if (pBt) {
      BtCursor** pp = &pBt->pCursor;
      while (*pp && *pp != pCur) pp = &(*pp)->pNext;
      if (*pp == pCur) *pp = pCur->pNext;
   }

   if (pCur->pKey) { sqlite3_free(pCur->pKey); pCur->pKey = nullptr; }

   // Explicitly destruct C++ members (allocated via placement new)
   pCur->cursor.~optional();
   pCur->cur_value.~basic_string();

   pCur->eState = CURSOR_INVALID;
   pCur->is_valid = false;
   return SQLITE_OK;
}

void sqlite3BtreeClearCursor(BtCursor* pCur) {
   pCur->cursor.reset();
   pCur->is_valid = false;
   pCur->eState = CURSOR_INVALID;
   if (pCur->pKey) { sqlite3_free(pCur->pKey); pCur->pKey = nullptr; }
}

void sqlite3BtreeCursorPin(BtCursor*) {}
void sqlite3BtreeCursorUnpin(BtCursor*) {}
void sqlite3BtreeCursorHintFlags(BtCursor* pCur, unsigned flags) {
   pCur->hints = (u8)flags;
}

#ifdef SQLITE_ENABLE_CURSOR_HINTS
void sqlite3BtreeCursorHint(BtCursor*, int, ...) {}
#endif

int sqlite3BtreeCursorHasMoved(BtCursor* pCur) {
   return pCur->eState != CURSOR_VALID;
}

int sqlite3BtreeCursorRestore(BtCursor* pCur, int* pDifferentRow) {
   if (pCur->eState == CURSOR_VALID) {
      *pDifferentRow = 0;
      return SQLITE_OK;
   }
   if (pCur->pKey && pCur->curIntKey) {
      int res = 0;
      int rc = sqlite3BtreeTableMoveto(pCur, pCur->nKey, 0, &res);
      *pDifferentRow = (res != 0);
      return rc;
   }
   *pDifferentRow = 1;
   pCur->eState = CURSOR_INVALID;
   return SQLITE_OK;
}

int sqlite3BtreeCursorIsValidNN(BtCursor* pCur) {
   return pCur->eState == CURSOR_VALID;
}

#ifndef NDEBUG
int sqlite3BtreeCursorIsValid(BtCursor* pCur) {
   return pCur && pCur->eState == CURSOR_VALID;
}
#endif

int sqlite3BtreeCursorHasHint(BtCursor* pCur, unsigned mask) {
   return pCur->hints & mask;
}

static void ensure_cursor(BtCursor* pCur) {
   if (!pCur->cursor) {
      PTRACE("  ensure_cursor: creating for root=%u wrFlag=%d\n",
             pCur->pgnoRoot, pCur->wrFlag);
      try {
         // If there's an open write transaction on this root, use its cursor
         // so we see uncommitted writes within the same SQLite transaction.
         auto it = pCur->psitri_db->open_tx.find(pCur->pgnoRoot);
         if (it != pCur->psitri_db->open_tx.end()) {
            pCur->cursor.emplace(it->second.create_cursor());
         } else {
            pCur->cursor.emplace(pCur->psitri_db->dwal_db->create_cursor(
               pCur->pgnoRoot, psitri::dwal::read_mode::latest));
         }
         pCur->cursor->cursor().seek_begin();
      } catch (const std::exception& e) {
         PTRACE("  ensure_cursor: exception: %s\n", e.what());
         pCur->cursor.reset();
      } catch (...) {
         PTRACE("  ensure_cursor: unknown exception\n");
         pCur->cursor.reset();
      }
   }
}

// ============================================================================
// Cursor movement
// ============================================================================

int sqlite3BtreeTableMoveto(BtCursor* pCur, i64 intKey, int bias, int* pRes) {
   ensure_cursor(pCur);
   if (!pCur->cursor) {
      *pRes = -1;
      pCur->eState = CURSOR_INVALID;
      pCur->is_valid = false;
      return SQLITE_OK;
   }
   auto& mc = pCur->cursor->cursor();

   auto key = encode_rowid(intKey);
   mc.seek_begin();
   mc.lower_bound(key);

   if (mc.is_end()) {
      *pRes = -1;
      pCur->eState = CURSOR_INVALID;
      pCur->is_valid = false;
      return SQLITE_OK;
   }

   auto cur_key = mc.key();
   if (cur_key.size() >= 8) {
      i64 found = decode_rowid(cur_key.data());
      if (found == intKey) {
         *pRes = 0;
      } else if (found > intKey) {
         *pRes = -1;
      } else {
         *pRes = 1;
      }
   } else {
      *pRes = -1;
   }

   cache_cursor_value(pCur);
   return SQLITE_OK;
}

int sqlite3BtreeIndexMoveto(BtCursor* pCur, UnpackedRecord* pUnKey, int* pRes) {
   PTRACE("IndexMoveto: root=%u nField=%d default_rc=%d\n",
          pCur->pgnoRoot, pUnKey->nField, pUnKey->default_rc);
   ensure_cursor(pCur);
   if (!pCur->cursor) {
      *pRes = -1;
      pCur->eState = CURSOR_INVALID;
      pCur->is_valid = false;
      return SQLITE_OK;
   }
   auto& mc = pCur->cursor->cursor();

   // Build a byte-comparable search key from the UnpackedRecord.
   // Index entries are stored with comparable keys (see sqlite3BtreeInsert),
   // so lower_bound works correctly with byte comparison.
   auto comp_key = make_comparable_from_unpacked(pUnKey);
   mc.lower_bound(std::string_view(comp_key.data(), comp_key.size()));

   if (mc.is_end()) {
      *pRes = -1;
      pCur->eState = CURSOR_INVALID;
      pCur->is_valid = false;
      return SQLITE_OK;
   }

   cache_cursor_value(pCur);
   PTRACE("IndexMoveto: is_valid=%d cur_value.size=%zu\n",
          (int)pCur->is_valid, pCur->cur_value.size());
   if (pCur->is_valid && !pCur->cur_value.empty()) {
      // Compare the stored packed record (in cur_value) against the search key
      // using SQLite's record comparator, which handles collation and
      // differing field counts correctly.
      *pRes = sqlite3VdbeRecordCompare(
         (int)pCur->cur_value.size(),
         pCur->cur_value.data(),
         pUnKey);
      PTRACE("IndexMoveto: *pRes=%d\n", *pRes);
   } else {
      *pRes = -1;
   }

   return SQLITE_OK;
}

int sqlite3BtreeFirst(BtCursor* pCur, int* pRes) {
   PTRACE("BtreeFirst: root=%u\n", pCur->pgnoRoot);
   ensure_cursor(pCur);
   if (!pCur->cursor) {
      // Root doesn't exist yet — empty table
      *pRes = 1;
      pCur->eState = CURSOR_INVALID;
      pCur->is_valid = false;
      PTRACE("BtreeFirst: root=%u -> empty (no cursor)\n", pCur->pgnoRoot);
      return SQLITE_OK;
   }
   auto& mc = pCur->cursor->cursor();
   mc.seek_begin();

   if (mc.is_end()) {
      *pRes = 1;
      pCur->eState = CURSOR_INVALID;
      pCur->is_valid = false;
      PTRACE("BtreeFirst: root=%u -> empty\n", pCur->pgnoRoot);
   } else {
      *pRes = 0;
      cache_cursor_value(pCur);
      PTRACE("BtreeFirst: root=%u -> found\n", pCur->pgnoRoot);
   }
   return SQLITE_OK;
}

int sqlite3BtreeIsEmpty(BtCursor* pCur, int* pRes) {
   ensure_cursor(pCur);
   if (!pCur->cursor) {
      *pRes = 1;
      return SQLITE_OK;
   }
   auto& mc = pCur->cursor->cursor();
   mc.seek_begin();
   *pRes = mc.is_end() ? 1 : 0;
   return SQLITE_OK;
}

int sqlite3BtreeLast(BtCursor* pCur, int* pRes) {
   ensure_cursor(pCur);
   if (!pCur->cursor) {
      *pRes = 1;
      pCur->eState = CURSOR_INVALID;
      pCur->is_valid = false;
      return SQLITE_OK;
   }
   auto& mc = pCur->cursor->cursor();
   mc.seek_last();

   if (mc.is_rend()) {
      *pRes = 1;
      pCur->eState = CURSOR_INVALID;
      pCur->is_valid = false;
   } else {
      *pRes = 0;
      cache_cursor_value(pCur);
   }
   return SQLITE_OK;
}

int sqlite3BtreeNext(BtCursor* pCur, int) {
   if (!pCur->cursor || !pCur->is_valid) return SQLITE_DONE;
   auto& mc = pCur->cursor->cursor();
   mc.next();
   if (mc.is_end()) {
      pCur->eState = CURSOR_INVALID;
      pCur->is_valid = false;
      return SQLITE_DONE;
   }
   cache_cursor_value(pCur);
   return SQLITE_OK;
}

int sqlite3BtreeEof(BtCursor* pCur) {
   return pCur->eState != CURSOR_VALID;
}

int sqlite3BtreePrevious(BtCursor* pCur, int) {
   if (!pCur->cursor || !pCur->is_valid) return SQLITE_DONE;
   auto& mc = pCur->cursor->cursor();
   mc.prev();
   if (mc.is_rend()) {
      pCur->eState = CURSOR_INVALID;
      pCur->is_valid = false;
      return SQLITE_DONE;
   }
   cache_cursor_value(pCur);
   return SQLITE_OK;
}

// ============================================================================
// Data access
// ============================================================================

i64 sqlite3BtreeIntegerKey(BtCursor* pCur) {
   return pCur->nKey;
}

i64 sqlite3BtreeOffset(BtCursor*) {
   return 0;
}

u32 sqlite3BtreePayloadSize(BtCursor* pCur) {
   return (u32)pCur->cur_value.size();
}

sqlite3_int64 sqlite3BtreeMaxRecordSize(BtCursor*) {
   return 0x7FFFFFFF;
}

int sqlite3BtreePayload(BtCursor* pCur, u32 offset, u32 amt, void* pBuf) {
   if (offset + amt > pCur->cur_value.size()) return SQLITE_CORRUPT;
   std::memcpy(pBuf, pCur->cur_value.data() + offset, amt);
   return SQLITE_OK;
}

const void* sqlite3BtreePayloadFetch(BtCursor* pCur, u32* pAmt) {
   *pAmt = (u32)pCur->cur_value.size();
   return pCur->cur_value.data();
}

#ifndef SQLITE_OMIT_INCRBLOB
int sqlite3BtreePayloadChecked(BtCursor* pCur, u32 offset, u32 amt, void* pBuf) {
   return sqlite3BtreePayload(pCur, offset, amt, pBuf);
}
int sqlite3BtreePutData(BtCursor*, u32, u32, void*) {
   return SQLITE_READONLY;
}
void sqlite3BtreeIncrblobCursor(BtCursor*) {}
#endif

// ============================================================================
// Mutations
// ============================================================================

int sqlite3BtreeInsert(
   BtCursor* pCur, const BtreePayload* pPayload, int flags, int seekResult
) {
   PTRACE("Insert: root=%u intkey=%d nKey=%lld nData=%d\n",
          pCur->pgnoRoot, pCur->curIntKey, (long long)pPayload->nKey, pPayload->nData);
   auto* psitri = pCur->psitri_db;
   std::string key, value;

   if (pCur->curIntKey) {
      key = encode_rowid(pPayload->nKey);
      if (pPayload->pData && pPayload->nData > 0) {
         value.assign((const char*)pPayload->pData, pPayload->nData);
         if (pPayload->nZero > 0) {
            value.append(pPayload->nZero, '\0');
         }
      }
   } else {
      // BLOBKEY (index): store a byte-comparable encoding as the DWAL key
      // and the original packed record as the value, so that cache_cursor_value
      // can return the packed record to SQLite for sqlite3BtreePayload().
      if (pPayload->pKey) {
         key = make_comparable_from_packed(
            (const char*)pPayload->pKey, (int)pPayload->nKey);
         value.assign((const char*)pPayload->pKey, pPayload->nKey);
      }
   }

   try {
      auto& tx = psitri->get_tx(pCur->pgnoRoot);
      tx.upsert(key, value);
   } catch (...) {
      return SQLITE_ERROR;
   }

   psitri->data_version.fetch_add(1);

   pCur->cursor.reset();
   pCur->is_valid = false;
   pCur->eState = CURSOR_INVALID;

   return SQLITE_OK;
}

int sqlite3BtreeDelete(BtCursor* pCur, u8 flags) {
   if (!pCur->is_valid) return SQLITE_ERROR;

   auto* psitri = pCur->psitri_db;
   auto& mc = pCur->cursor->cursor();
   auto key = std::string(mc.key());

   try {
      auto& tx = psitri->get_tx(pCur->pgnoRoot);
      tx.remove(key);
   } catch (...) {
      return SQLITE_ERROR;
   }

   psitri->data_version.fetch_add(1);
   pCur->is_valid = false;

   if (flags & BTREE_SAVEPOSITION) {
      // Reposition cursor at the deleted key's location
      if (!pCur->cursor) {
         pCur->eState = CURSOR_INVALID;
      } else {
         auto& newmc = pCur->cursor->cursor();
         newmc.seek_begin();
         newmc.lower_bound(key);
         cache_cursor_value(pCur);
         pCur->skipNext = 1;
      }
   } else {
      pCur->eState = CURSOR_INVALID;
   }

   return SQLITE_OK;
}

// ============================================================================
// Row count
// ============================================================================

int sqlite3BtreeCount(sqlite3*, BtCursor* pCur, i64* pnEntry) {
   ensure_cursor(pCur);
   if (!pCur->cursor) {
      *pnEntry = 0;
      return SQLITE_OK;
   }
   *pnEntry = (i64)pCur->cursor->cursor().count_keys();
   return SQLITE_OK;
}

i64 sqlite3BtreeRowCountEst(BtCursor* pCur) {
   if (!pCur->psitri_db || !pCur->psitri_db->dwal_db) return 1000;
   try {
      auto it = pCur->psitri_db->open_tx.find(pCur->pgnoRoot);
      if (it != pCur->psitri_db->open_tx.end()) {
         auto cursor = it->second.create_cursor();
         return (i64)cursor.cursor().count_keys();
      }
      auto cursor = pCur->psitri_db->dwal_db->create_cursor(
         pCur->pgnoRoot, psitri::dwal::read_mode::latest);
      return (i64)cursor.cursor().count_keys();
   } catch (...) {
      return 1000;
   }
}

int sqlite3BtreeTransferRow(BtCursor* pDest, BtCursor* pSrc, i64 iKey) {
   if (!pSrc->is_valid) return SQLITE_ERROR;

   BtreePayload payload = {};
   if (pDest->curIntKey) {
      payload.nKey = iKey;
      payload.pData = pSrc->cur_value.data();
      payload.nData = (int)pSrc->cur_value.size();
   } else {
      payload.pKey = pSrc->cur_value.data();
      payload.nKey = (sqlite3_int64)pSrc->cur_value.size();
   }
   return sqlite3BtreeInsert(pDest, &payload, 0, 0);
}

// ============================================================================
// Integrity check (stub)
// ============================================================================

int sqlite3BtreeIntegrityCheck(
   sqlite3*, Btree*, Pgno*, sqlite3_value*,
   int, int, int* pnErr, char** pzOut
) {
   *pnErr = 0;
   *pzOut = nullptr;
   return SQLITE_OK;
}

// ============================================================================
// Mutex stubs (no shared cache)
// ============================================================================

#ifndef SQLITE_OMIT_SHARED_CACHE
void sqlite3BtreeEnter(Btree*) {}
void sqlite3BtreeEnterAll(sqlite3*) {}
int sqlite3BtreeSharable(Btree*) { return 0; }
void sqlite3BtreeEnterCursor(BtCursor*) {}
int sqlite3BtreeConnectionCount(Btree*) { return 1; }
#endif

#if !defined(SQLITE_OMIT_SHARED_CACHE) && SQLITE_THREADSAFE
void sqlite3BtreeLeave(Btree*) {}
void sqlite3BtreeLeaveCursor(BtCursor*) {}
void sqlite3BtreeLeaveAll(sqlite3*) {}
#ifndef NDEBUG
int sqlite3BtreeHoldsMutex(Btree*) { return 1; }
int sqlite3BtreeHoldsAllMutexes(sqlite3*) { return 1; }
int sqlite3SchemaMutexHeld(sqlite3*, int, Schema*) { return 1; }
#endif
#endif

#ifdef SQLITE_DEBUG
sqlite3_uint64 sqlite3BtreeSeekCount(Btree* p) { return p->nSeek; }
int sqlite3BtreeClosesWithCursor(Btree*, BtCursor*) { return 0; }
#endif

} // extern "C"
