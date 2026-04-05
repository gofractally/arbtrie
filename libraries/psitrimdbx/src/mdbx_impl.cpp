/** @file mdbx_impl.cpp
 *  @brief PsiTri-backed MDBX C and C++ API implementation.
 */
#include <mdbx.h>
#include <mdbx.h++>

#include <psitri/database.hpp>
#include <psitri/dwal/dwal_database.hpp>
#include <psitri/dwal/dwal_transaction.hpp>
#include <psitri/dwal/merge_cursor.hpp>
#include <psitri/dwal/transaction.hpp>
#include <psitri/read_session_impl.hpp>

#include <atomic>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

// ════════════════════════════════════════════════════════════════════
// Internal types (hidden behind opaque C handles)
// ════════════════════════════════════════════════════════════════════

/// Per-DBI metadata stored in the environment.
struct dbi_info
{
   std::string name;           // empty for unnamed default DB
   uint32_t    root_index;     // psitri root index
   unsigned    flags;          // MDBX_db_flags_t used at creation
   bool        is_dupsort;     // (flags & MDBX_DUPSORT) != 0
};

struct MDBX_env
{
   // ── PsiTri database ───────────────────────────────────────────
   std::shared_ptr<psitri::database>              db;
   std::unique_ptr<psitri::dwal::dwal_database>   dwal_db;
   std::filesystem::path                          path;

   // ── Configuration (set before open) ───────────────────────────
   unsigned          max_dbs     = 16;
   unsigned          max_readers = 126;
   MDBX_env_flags_t  env_flags   = MDBX_ENV_DEFAULTS;
   bool              opened      = false;
   void*             userctx     = nullptr;

   // Geometry (stored but ignored — psitri manages its own sizing)
   intptr_t geo_lower = -1, geo_now = -1, geo_upper = -1;
   intptr_t geo_growth = -1, geo_shrink = -1, geo_pagesize = -1;

   // ── DBI registry ─────────────────────────────────────────────
   mutable std::shared_mutex             dbi_mutex;
   std::vector<dbi_info>                 dbis;        // indexed by MDBX_dbi
   std::unordered_map<std::string, MDBX_dbi> name_to_dbi;
   // DBI 0 = reserved (metadata), DBI 1 = unnamed default
   // Named DBIs start at 2.

   // ── Sequence counter for txn IDs ──────────────────────────────
   std::atomic<uint64_t> next_txn_id{1};

   void init_default_dbi()
   {
      // DBI 0: metadata root (stores name→dbi mappings)
      dbis.push_back({"__meta__", 0, 0, false});
      // DBI 1: unnamed default database (root 1)
      dbis.push_back({"", 1, MDBX_DB_DEFAULTS, false});
   }

   MDBX_dbi allocate_dbi(const std::string& name, unsigned flags)
   {
      MDBX_dbi dbi = static_cast<MDBX_dbi>(dbis.size());
      uint32_t root_idx = dbi + 1;
      dbis.push_back({name, root_idx, flags, (flags & MDBX_DUPSORT) != 0});
      // Use dbi+1 as root_index (root 0 = meta, root 1 = default, root 2+ = named)
      if (!name.empty())
         name_to_dbi[name] = dbi;

      // Enable RW-layer locking so MDBX-style readers (which call get_latest
      // and latest-mode cursors from non-writer threads) never see torn txns.
      if (dwal_db)
         dwal_db->ensure_root_public(root_idx).enable_rw_locking = true;

      return dbi;
   }
};

// ── DUPSORT composite key encoding ──────────────────────────────
//
// For DUPSORT tables, we store (key, value) pairs as a single psitri
// key using an escaped-separator encoding:
//
//   composite = escape(key) + \x00\x00 + value
//
// where escape() replaces each \x00 byte with \x00\x01.
// This preserves lexicographic ordering: entries sort by key first,
// then by value within the same key. The \x00\x00 separator cannot
// appear in an escaped key, so decoding is unambiguous.

static std::string dupsort_encode(std::string_view key, std::string_view value)
{
   std::string result;
   result.reserve(key.size() + 2 + value.size());
   for (char c : key)
   {
      result += c;
      if (c == '\0')
         result += '\x01';
   }
   result += '\0';
   result += '\0';
   result.append(value.data(), value.size());
   return result;
}

static std::string dupsort_key_prefix(std::string_view key)
{
   std::string result;
   result.reserve(key.size() + 2);
   for (char c : key)
   {
      result += c;
      if (c == '\0')
         result += '\x01';
   }
   result += '\0';
   result += '\0';
   return result;
}

static std::string dupsort_key_upper(std::string_view key)
{
   std::string result;
   result.reserve(key.size() + 2);
   for (char c : key)
   {
      result += c;
      if (c == '\0')
         result += '\x01';
   }
   result += '\0';
   result += '\x01';
   return result;
}

static bool dupsort_decode(std::string_view composite,
                           std::string& key_out,
                           std::string& val_out)
{
   key_out.clear();
   val_out.clear();

   for (size_t i = 0; i < composite.size(); ++i)
   {
      if (composite[i] == '\0')
      {
         if (i + 1 < composite.size() && composite[i + 1] == '\x01')
         {
            key_out += '\0';
            ++i;
         }
         else if (i + 1 < composite.size() && composite[i + 1] == '\x00')
         {
            val_out.assign(composite.data() + i + 2, composite.size() - i - 2);
            return true;
         }
         else
         {
            return false;
         }
      }
      else
      {
         key_out += composite[i];
      }
   }
   return false;
}

/// Cursor state for a single DBI — either merge cursor or direct.
struct cursor_state
{
   psitri::dwal::owned_merge_cursor mc;
   bool                             valid     = false;
   bool                             dupsort   = false;
   std::string                      key_buf;   // exposed key
   std::string                      val_buf;   // exposed value
   std::string                      raw_key;   // raw composite (DUPSORT only)

   explicit cursor_state(psitri::dwal::owned_merge_cursor m, bool ds = false)
       : mc(std::move(m)), dupsort(ds)
   {
   }

   /// Read the raw value from the merge cursor into val_buf (non-DUPSORT).
   void read_value()
   {
      if (mc->current_source() == psitri::dwal::merge_cursor::source::tri)
      {
         auto* tc = mc->tri_cursor();
         tc->get_value([this](psitri::value_view vv) {
            val_buf.assign(vv.data(), vv.size());
         });
      }
      else
      {
         auto& bv = mc->current_value();
         val_buf.assign(bv.data.data(), bv.data.size());
      }
   }

   void sync_key_val()
   {
      if (mc->is_end() || mc->is_rend())
      {
         valid = false;
         return;
      }

      valid = true;
      if (!dupsort)
      {
         key_buf.assign(mc->key().data(), mc->key().size());
         read_value();
      }
      else
      {
         // Composite key — decode into key + value
         raw_key.assign(mc->key().data(), mc->key().size());
         if (!dupsort_decode(raw_key, key_buf, val_buf))
         {
            valid = false;
            return;
         }
         // For DUPSORT, the psitri "value" is empty (all data is in the key).
         // But we read it anyway in case of non-dupsort entries mixed in.
      }
   }

   MDBX_val key_val() const
   {
      return {const_cast<char*>(key_buf.data()), key_buf.size()};
   }

   MDBX_val data_val() const
   {
      return {const_cast<char*>(val_buf.data()), val_buf.size()};
   }
};

struct MDBX_txn
{
   MDBX_env*         env       = nullptr;
   MDBX_txn_flags_t  txn_flags = MDBX_TXN_READWRITE;
   void*             context   = nullptr;
   uint64_t          id        = 0;

   // RW transaction (multi-root — routes DBI→root internally)
   std::unique_ptr<psitri::dwal::transaction>      write_tx;
   std::vector<uint32_t>                           write_roots; // roots opened for writing

   // RO transaction (mutable: lazily created even from const mdbx_get)
   mutable std::unique_ptr<psitri::dwal::dwal_read_session> read_session;

   // Per-DBI lookup cache (populated lazily)
   // For reads within a RW txn, we use the transaction's get() method.
   // For RO txns, we use the read session.

   // Value buffer for get() return — kept alive until next get()
   std::string get_buf;

   bool committed = false;
   bool aborted   = false;

   bool is_readonly() const { return (txn_flags & MDBX_TXN_RDONLY) != 0; }
};

struct MDBX_cursor
{
   MDBX_txn* txn  = nullptr;
   MDBX_dbi  dbi  = 0;
   bool      is_dupsort = false;

   std::unique_ptr<cursor_state> state;
};

// ════════════════════════════════════════════════════════════════════
// Helpers
// ════════════════════════════════════════════════════════════════════

static std::string_view to_sv(const MDBX_val* v)
{
   return v ? std::string_view(static_cast<const char*>(v->iov_base), v->iov_len)
            : std::string_view{};
}

static uint32_t dbi_root_index(MDBX_env* env, MDBX_dbi dbi)
{
   std::shared_lock lk(env->dbi_mutex);
   if (dbi >= env->dbis.size())
      return UINT32_MAX;
   return env->dbis[dbi].root_index;
}

static bool dbi_is_dupsort(MDBX_env* env, MDBX_dbi dbi)
{
   std::shared_lock lk(env->dbi_mutex);
   if (dbi >= env->dbis.size())
      return false;
   return env->dbis[dbi].is_dupsort;
}

// ════════════════════════════════════════════════════════════════════
// C API implementation
// ════════════════════════════════════════════════════════════════════

// ── Version ──────────────────────────────────────────────────────

const MDBX_version_info mdbx_version = {
   0, 13, 11, 0,
   {"psitri-compat", "", "", "psitrimdbx-0.1"},
   "psitrimdbx"
};

// ── Error handling ───────────────────────────────────────────────

const char* mdbx_strerror(int errnum)
{
   switch (errnum)
   {
      case MDBX_SUCCESS:        return "MDBX_SUCCESS: Successful";
      case MDBX_RESULT_TRUE:    return "MDBX_RESULT_TRUE";
      case MDBX_KEYEXIST:       return "MDBX_KEYEXIST: Key already exists";
      case MDBX_NOTFOUND:       return "MDBX_NOTFOUND: No matching key/data pair found";
      case MDBX_CORRUPTED:      return "MDBX_CORRUPTED: Database is corrupted";
      case MDBX_PANIC:          return "MDBX_PANIC: Environment had fatal error";
      case MDBX_VERSION_MISMATCH: return "MDBX_VERSION_MISMATCH: Library version mismatch";
      case MDBX_INVALID:        return "MDBX_INVALID: Invalid parameter";
      case MDBX_MAP_FULL:       return "MDBX_MAP_FULL: Database map full";
      case MDBX_DBS_FULL:       return "MDBX_DBS_FULL: Maximum databases reached";
      case MDBX_READERS_FULL:   return "MDBX_READERS_FULL: Maximum readers reached";
      case MDBX_TXN_FULL:       return "MDBX_TXN_FULL: Transaction has too many dirty pages";
      case MDBX_BAD_TXN:        return "MDBX_BAD_TXN: Transaction is invalid";
      case MDBX_BAD_VALSIZE:    return "MDBX_BAD_VALSIZE: Invalid value size";
      case MDBX_BAD_DBI:        return "MDBX_BAD_DBI: Invalid database handle";
      case MDBX_PROBLEM:        return "MDBX_PROBLEM: Unexpected internal error";
      case MDBX_BUSY:           return "MDBX_BUSY: Resource is busy";
      case MDBX_EMULTIVAL:      return "MDBX_EMULTIVAL: Multiple values for a key";
      case MDBX_ENOSYS:         return "MDBX_ENOSYS: Feature not implemented";
      case MDBX_EINVAL:         return "MDBX_EINVAL: Invalid argument";
      case MDBX_ENOMEM:         return "MDBX_ENOMEM: Out of memory";
      case MDBX_ENOFILE:        return "MDBX_ENOFILE: No such file or directory";
      case MDBX_PAGE_NOTFOUND:  return "MDBX_PAGE_NOTFOUND: Page not found";
      case MDBX_INCOMPATIBLE:   return "MDBX_INCOMPATIBLE: Incompatible operation";
      case MDBX_UNABLE_EXTEND_MAPSIZE: return "MDBX_UNABLE_EXTEND_MAPSIZE: Unable to extend map size";
      default:                  return "Unknown MDBX error";
   }
}

const char* mdbx_strerror_r(int errnum, char* buf, size_t buflen)
{
   const char* msg = mdbx_strerror(errnum);
   if (buf && buflen > 0)
   {
      std::strncpy(buf, msg, buflen - 1);
      buf[buflen - 1] = '\0';
   }
   return buf ? buf : msg;
}

// ── Environment ──────────────────────────────────────────────────

int mdbx_env_create(MDBX_env** penv)
{
   if (!penv)
      return MDBX_EINVAL;
   *penv = new MDBX_env();
   return MDBX_SUCCESS;
}

int mdbx_env_open(MDBX_env* env, const char* pathname,
                  MDBX_env_flags_t flags, mdbx_mode_t /*mode*/)
{
   if (!env || !pathname)
      return MDBX_EINVAL;
   if (env->opened)
      return MDBX_EINVAL;

   try
   {
      env->path      = pathname;
      env->env_flags = flags;

      // Determine open mode
      auto open_mode = psitri::open_mode::create_or_open;
      if (flags & MDBX_RDONLY)
         open_mode = psitri::open_mode::open_existing;

      env->db = psitri::database::open(env->path, open_mode);

      // Create WAL directory
      auto wal_dir = env->path / "wal";
      std::filesystem::create_directories(wal_dir);

      psitri::dwal::dwal_config cfg;
      cfg.merge_threads = 2;

      env->dwal_db = std::make_unique<psitri::dwal::dwal_database>(
         env->db, wal_dir, cfg);

      env->init_default_dbi();

      // Enable RW-layer locking on default roots so MDBX-style readers
      // (which use get_latest from non-writer threads) see consistent state.
      env->dwal_db->ensure_root_public(0).enable_rw_locking = true;
      env->dwal_db->ensure_root_public(1).enable_rw_locking = true;

      env->opened = true;

      return MDBX_SUCCESS;
   }
   catch (const std::exception&)
   {
      return MDBX_PANIC;
   }
}

int mdbx_env_close(MDBX_env* env)
{
   return mdbx_env_close_ex(env, 0);
}

int mdbx_env_close_ex(MDBX_env* env, int /*dont_sync*/)
{
   if (!env)
      return MDBX_EINVAL;

   env->dwal_db.reset();
   env->db.reset();
   delete env;
   return MDBX_SUCCESS;
}

int mdbx_env_set_geometry(MDBX_env* env,
                          intptr_t size_lower, intptr_t size_now,
                          intptr_t size_upper, intptr_t growth_step,
                          intptr_t shrink_threshold, intptr_t pagesize)
{
   if (!env)
      return MDBX_EINVAL;
   // Store but don't enforce — psitri manages its own storage
   env->geo_lower    = size_lower;
   env->geo_now      = size_now;
   env->geo_upper    = size_upper;
   env->geo_growth   = growth_step;
   env->geo_shrink   = shrink_threshold;
   env->geo_pagesize = pagesize;
   return MDBX_SUCCESS;
}

int mdbx_env_set_maxdbs(MDBX_env* env, MDBX_dbi dbs)
{
   if (!env || env->opened)
      return MDBX_EINVAL;
   env->max_dbs = dbs;
   return MDBX_SUCCESS;
}

int mdbx_env_set_maxreaders(MDBX_env* env, unsigned readers)
{
   if (!env || env->opened)
      return MDBX_EINVAL;
   env->max_readers = readers;
   return MDBX_SUCCESS;
}

int mdbx_env_sync_ex(MDBX_env* env, int /*force*/, int /*nonblock*/)
{
   if (!env || !env->opened)
      return MDBX_EINVAL;
   try
   {
      env->dwal_db->flush_wal();
      return MDBX_SUCCESS;
   }
   catch (...)
   {
      return MDBX_PANIC;
   }
}

int mdbx_env_get_flags(MDBX_env* env, unsigned* flags)
{
   if (!env || !flags)
      return MDBX_EINVAL;
   *flags = env->env_flags;
   return MDBX_SUCCESS;
}

int mdbx_env_stat_ex(const MDBX_env* env, const MDBX_txn* /*txn*/,
                     MDBX_stat* stat, size_t bytes)
{
   if (!env || !stat || bytes < sizeof(MDBX_stat))
      return MDBX_EINVAL;
   std::memset(stat, 0, sizeof(MDBX_stat));
   stat->ms_psize = 4096; // Synthetic page size
   return MDBX_SUCCESS;
}

int mdbx_env_info_ex(const MDBX_env* env, const MDBX_txn* /*txn*/,
                     MDBX_envinfo* info, size_t bytes)
{
   if (!env || !info || bytes < sizeof(MDBX_envinfo))
      return MDBX_EINVAL;
   std::memset(info, 0, sizeof(MDBX_envinfo));
   info->mi_dxb_pagesize = 4096;
   info->mi_sys_pagesize = 4096;
   return MDBX_SUCCESS;
}

void* mdbx_env_get_userctx(const MDBX_env* env)
{
   return env ? env->userctx : nullptr;
}

int mdbx_env_set_userctx(MDBX_env* env, void* ctx)
{
   if (!env)
      return MDBX_EINVAL;
   env->userctx = ctx;
   return MDBX_SUCCESS;
}

// ── Transactions ─────────────────────────────────────────────────

int mdbx_txn_begin_ex(MDBX_env* env, MDBX_txn* parent,
                      MDBX_txn_flags_t flags, MDBX_txn** txn,
                      void* context)
{
   if (!env || !env->opened || !txn)
      return MDBX_EINVAL;
   if (parent)
      return MDBX_ENOSYS; // Nested transactions not yet supported

   try
   {
      auto* t      = new MDBX_txn();
      t->env       = env;
      t->txn_flags = flags;
      t->context   = context;
      t->id        = env->next_txn_id.fetch_add(1);

      if (flags & MDBX_TXN_RDONLY)
      {
         t->read_session = std::make_unique<psitri::dwal::dwal_read_session>(
            env->dwal_db->start_read_session());
      }
      // RW transaction created lazily when first DBI is used,
      // because we need to know which roots to lock.

      *txn = t;
      return MDBX_SUCCESS;
   }
   catch (...)
   {
      return MDBX_ENOMEM;
   }
}

/// Ensure the write transaction is created with the right roots.
/// Called lazily before the first mutation or get in an RW txn.
static int ensure_write_tx(MDBX_txn* txn, uint32_t root_index)
{
   if (!txn || txn->is_readonly())
      return MDBX_BAD_TXN;

   if (txn->write_tx)
   {
      // Check if this root is already part of the transaction
      for (auto ri : txn->write_roots)
         if (ri == root_index)
            return MDBX_SUCCESS;

      // Root not yet included — we need to commit-and-recreate
      // or just add it. For simplicity, we use single-root transactions
      // per operation for now, but the multi-root transaction approach
      // is better. Let's use single-root dwal_transactions instead.
      // Actually, since MDBX serializes writers, we hold the global
      // write lock anyway. We can use per-operation transactions.
   }

   return MDBX_SUCCESS;
}

int mdbx_txn_commit_ex(MDBX_txn* txn, MDBX_commit_latency* latency)
{
   if (!txn)
      return MDBX_EINVAL;
   if (txn->committed || txn->aborted)
      return MDBX_BAD_TXN;

   if (latency)
      std::memset(latency, 0, sizeof(MDBX_commit_latency));

   try
   {
      if (txn->write_tx)
      {
         txn->write_tx->commit();
      }

      txn->committed = true;
      delete txn;
      return MDBX_SUCCESS;
   }
   catch (...)
   {
      txn->aborted = true;
      delete txn;
      return MDBX_PANIC;
   }
}

int mdbx_txn_abort(MDBX_txn* txn)
{
   if (!txn)
      return MDBX_EINVAL;
   if (txn->committed || txn->aborted)
   {
      delete txn;
      return MDBX_SUCCESS;
   }

   try
   {
      if (txn->write_tx)
         txn->write_tx->abort();

      txn->aborted = true;
      delete txn;
      return MDBX_SUCCESS;
   }
   catch (...)
   {
      delete txn;
      return MDBX_PANIC;
   }
}

int mdbx_txn_reset(MDBX_txn* txn)
{
   if (!txn || !txn->is_readonly())
      return MDBX_BAD_TXN;
   txn->read_session.reset();
   return MDBX_SUCCESS;
}

int mdbx_txn_renew(MDBX_txn* txn)
{
   if (!txn || !txn->is_readonly())
      return MDBX_BAD_TXN;
   try
   {
      txn->read_session = std::make_unique<psitri::dwal::dwal_read_session>(
         txn->env->dwal_db->start_read_session());
      return MDBX_SUCCESS;
   }
   catch (...)
   {
      return MDBX_PANIC;
   }
}

MDBX_env* mdbx_txn_env(const MDBX_txn* txn)
{
   return txn ? txn->env : nullptr;
}

uint64_t mdbx_txn_id(const MDBX_txn* txn)
{
   return txn ? txn->id : 0;
}

int mdbx_txn_flags(const MDBX_txn* txn)
{
   return txn ? static_cast<int>(txn->txn_flags) : MDBX_EINVAL;
}

// ── DBI operations ───────────────────────────────────────────────

int mdbx_dbi_open(MDBX_txn* txn, const char* name,
                  MDBX_db_flags_t flags, MDBX_dbi* dbi)
{
   if (!txn || !dbi)
      return MDBX_EINVAL;

   MDBX_env* env = txn->env;

   // Unnamed default database
   if (!name || name[0] == '\0')
   {
      *dbi = 1; // DBI 1 = default unnamed DB
      return MDBX_SUCCESS;
   }

   std::string sname(name);
   {
      std::shared_lock lk(env->dbi_mutex);
      auto it = env->name_to_dbi.find(sname);
      if (it != env->name_to_dbi.end())
      {
         *dbi = it->second;
         return MDBX_SUCCESS;
      }
   }

   // Not found — create if requested
   if (!(flags & MDBX_CREATE))
      return MDBX_NOTFOUND;

   if (txn->is_readonly())
      return MDBX_EACCESS;

   {
      std::unique_lock lk(env->dbi_mutex);
      // Double-check
      auto it = env->name_to_dbi.find(sname);
      if (it != env->name_to_dbi.end())
      {
         *dbi = it->second;
         return MDBX_SUCCESS;
      }

      if (env->dbis.size() >= env->max_dbs + 2)
         return MDBX_DBS_FULL;

      *dbi = env->allocate_dbi(sname, flags);
      return MDBX_SUCCESS;
   }
}

int mdbx_dbi_close(MDBX_env* /*env*/, MDBX_dbi /*dbi*/)
{
   // No-op: psitri roots are persistent
   return MDBX_SUCCESS;
}

int mdbx_dbi_stat(const MDBX_txn* txn, MDBX_dbi dbi,
                  MDBX_stat* stat, size_t bytes)
{
   if (!txn || !stat || bytes < sizeof(MDBX_stat))
      return MDBX_EINVAL;

   uint32_t root_idx = dbi_root_index(const_cast<MDBX_env*>(txn->env), dbi);
   if (root_idx == UINT32_MAX)
      return MDBX_BAD_DBI;

   std::memset(stat, 0, sizeof(MDBX_stat));
   stat->ms_psize = 4096;

   try
   {
      // Count entries by iterating a cursor.
      auto mode = txn->is_readonly() ? psitri::dwal::read_mode::buffered
                                     : psitri::dwal::read_mode::latest;
      auto mc = txn->env->dwal_db->create_cursor(root_idx, mode,
                                                  /*skip_rw_lock=*/!txn->is_readonly());
      uint64_t count = 0;
      if (mc->seek_begin())
      {
         do { ++count; } while (mc->next());
      }
      stat->ms_entries = count;
   }
   catch (...)
   {
      // If counting fails, return zeroed stats rather than failing.
   }

   return MDBX_SUCCESS;
}

static int ensure_rw_root(MDBX_txn* txn, uint32_t root_idx);

int mdbx_drop(MDBX_txn* txn, MDBX_dbi dbi, int del)
{
   if (!txn || txn->is_readonly())
      return MDBX_BAD_TXN;

   uint32_t root_idx = dbi_root_index(txn->env, dbi);
   if (root_idx == UINT32_MAX)
      return MDBX_BAD_DBI;

   // Ensure the data root is writable; also ensure the catalog root (1) if deleting.
   if (del)
   {
      int rc = ensure_rw_root(txn, 1);  // catalog root first
      if (rc != MDBX_SUCCESS)
         return rc;
      rc = ensure_rw_root(txn, root_idx);
      if (rc != MDBX_SUCCESS)
         return rc;
   }
   else
   {
      int rc = ensure_rw_root(txn, root_idx);
      if (rc != MDBX_SUCCESS)
         return rc;
   }

   try
   {
      // Clear: remove all entries by iterating with a cursor.
      auto mc = txn->env->dwal_db->create_cursor(root_idx, psitri::dwal::read_mode::latest,
                                               /*skip_rw_lock=*/true);
      std::vector<std::string> keys_to_remove;
      if (mc->seek_begin())
      {
         do
         {
            keys_to_remove.emplace_back(mc->key());
         } while (mc->next());
      }
      for (auto& k : keys_to_remove)
         txn->write_tx->remove(root_idx, k);

      if (del)
      {
         // Remove the DBI entry from the catalog (root 1).
         std::shared_lock lk(txn->env->dbi_mutex);
         if (dbi < txn->env->dbis.size() && !txn->env->dbis[dbi].name.empty())
         {
            auto name = txn->env->dbis[dbi].name;
            lk.unlock();
            txn->write_tx->remove(1, name);
            std::unique_lock wlk(txn->env->dbi_mutex);
            txn->env->name_to_dbi.erase(name);
            txn->env->dbis[dbi].name.clear();
            txn->env->dbis[dbi].root_index = UINT32_MAX;
         }
      }

      return MDBX_SUCCESS;
   }
   catch (...)
   {
      return MDBX_PANIC;
   }
}

int mdbx_dbi_flags_ex(const MDBX_txn* txn, MDBX_dbi dbi,
                      unsigned* flags, unsigned* state)
{
   if (!txn || !flags)
      return MDBX_EINVAL;
   std::shared_lock lk(txn->env->dbi_mutex);
   if (dbi >= txn->env->dbis.size())
      return MDBX_BAD_DBI;
   *flags = txn->env->dbis[dbi].flags;
   if (state)
      *state = 0;
   return MDBX_SUCCESS;
}

// ── Key-value operations ─────────────────────────────────────────

int mdbx_get(const MDBX_txn* txn, MDBX_dbi dbi,
             const MDBX_val* key, MDBX_val* data)
{
   if (!txn || !key || !data)
      return MDBX_EINVAL;

   uint32_t root_idx = dbi_root_index(txn->env, dbi);
   if (root_idx == UINT32_MAX)
      return MDBX_BAD_DBI;

   auto    key_sv = to_sv(key);
   auto*   mtxn   = const_cast<MDBX_txn*>(txn);
   bool    is_ds  = dbi_is_dupsort(txn->env, dbi);

   try
   {
      if (is_ds)
      {
         // DUPSORT: find the first composite key with this key prefix.
         auto prefix = dupsort_key_prefix(key_sv);
         bool writer = !txn->is_readonly() && txn->write_tx;
         auto mc = txn->env->dwal_db->create_cursor(
            root_idx, psitri::dwal::read_mode::latest, writer);
         if (!mc->lower_bound(prefix) || mc->is_end())
            return MDBX_NOTFOUND;

         auto found = mc->key();
         if (found.size() < prefix.size()
             || found.substr(0, prefix.size()) != std::string_view(prefix))
            return MDBX_NOTFOUND;

         // Decode the composite to get the value part
         std::string dk, dv;
         if (!dupsort_decode(found, dk, dv))
            return MDBX_NOTFOUND;

         mtxn->get_buf = std::move(dv);
         data->iov_base = mtxn->get_buf.data();
         data->iov_len  = mtxn->get_buf.size();
         return MDBX_SUCCESS;
      }

      // Non-DUPSORT path
      if (!txn->is_readonly() && txn->write_tx)
      {
         auto result = txn->write_tx->get(root_idx, key_sv);
         if (!result.found)
            return MDBX_NOTFOUND;

         mtxn->get_buf.assign(result.value.data.data(), result.value.data.size());
         data->iov_base = mtxn->get_buf.data();
         data->iov_len  = mtxn->get_buf.size();
         return MDBX_SUCCESS;
      }
      else
      {
         auto result = txn->env->dwal_db->get_latest(root_idx, key_sv);
         if (!result.found)
            return MDBX_NOTFOUND;

         if (!result.owned_data.empty())
            mtxn->get_buf = std::move(result.owned_data);
         else
            mtxn->get_buf.assign(result.value.data.data(), result.value.data.size());
         data->iov_base = mtxn->get_buf.data();
         data->iov_len  = mtxn->get_buf.size();
         return MDBX_SUCCESS;
      }
   }
   catch (...)
   {
      return MDBX_PANIC;
   }
}

int mdbx_get_ex(const MDBX_txn* txn, MDBX_dbi dbi,
                MDBX_val* key, MDBX_val* data, size_t* values_count)
{
   int rc = mdbx_get(txn, dbi, key, data);
   if (values_count)
   {
      *values_count = 1;
      if (rc == MDBX_SUCCESS && dbi_is_dupsort(txn->env, dbi))
      {
         // Count all entries with this key
         auto key_sv = to_sv(key);
         auto prefix = dupsort_key_prefix(key_sv);
         auto upper  = dupsort_key_upper(key_sv);
         bool writer = !txn->is_readonly() && txn->write_tx;
         auto mc = txn->env->dwal_db->create_cursor(
            dbi_root_index(txn->env, dbi), psitri::dwal::read_mode::latest, writer);
         size_t n = 0;
         if (mc->lower_bound(prefix))
         {
            while (!mc->is_end() && mc->key() < std::string_view(upper))
            {
               ++n;
               if (!mc->next())
                  break;
            }
         }
         *values_count = n;
      }
   }
   return rc;
}

/// Ensure a write transaction exists that covers the given root.
/// Uses a single-root dwal::transaction for simplicity — MDBX serializes
/// writers anyway (only one RW txn at a time).
static int ensure_rw_root(MDBX_txn* txn, uint32_t root_idx)
{
   if (!txn || txn->is_readonly())
      return MDBX_BAD_TXN;

   if (!txn->write_tx)
   {
      // Create a single-root write transaction
      txn->write_tx = std::make_unique<psitri::dwal::transaction>(
         *txn->env->dwal_db, std::vector<uint32_t>{root_idx});
      txn->write_roots.push_back(root_idx);
      return MDBX_SUCCESS;
   }

   // Check if root is already in the transaction
   for (auto ri : txn->write_roots)
      if (ri == root_idx)
         return MDBX_SUCCESS;

   // Need to add a new root — commit existing and start fresh with both
   // This is a limitation: MDBX_txn maps to a single dwal::transaction,
   // but dwal::transaction requires roots declared upfront.
   // Solution: commit current, start new with expanded root set.
   try
   {
      txn->write_tx->commit();
      txn->write_roots.push_back(root_idx);
      txn->write_tx = std::make_unique<psitri::dwal::transaction>(
         *txn->env->dwal_db, txn->write_roots);
      return MDBX_SUCCESS;
   }
   catch (...)
   {
      return MDBX_PANIC;
   }
}

int mdbx_put(MDBX_txn* txn, MDBX_dbi dbi,
             const MDBX_val* key, MDBX_val* data,
             MDBX_put_flags_t flags)
{
   if (!txn || !key || !data || txn->is_readonly())
      return MDBX_EINVAL;

   uint32_t root_idx = dbi_root_index(txn->env, dbi);
   if (root_idx == UINT32_MAX)
      return MDBX_BAD_DBI;

   int rc = ensure_rw_root(txn, root_idx);
   if (rc != MDBX_SUCCESS)
      return rc;

   auto key_sv = to_sv(key);
   auto val_sv = to_sv(data);
   bool is_ds  = dbi_is_dupsort(txn->env, dbi);

   try
   {
      if (is_ds)
      {
         // DUPSORT: store as composite key, empty value
         auto composite = dupsort_encode(key_sv, val_sv);

         if (flags & MDBX_NODUPDATA)
         {
            // Check if this exact (key, value) pair exists
            auto result = txn->write_tx->get(root_idx, composite);
            if (result.found)
               return MDBX_KEYEXIST;
         }
         if (flags & MDBX_NOOVERWRITE)
         {
            // Check if any entry with this key exists
            auto prefix = dupsort_key_prefix(key_sv);
            auto upper  = dupsort_key_upper(key_sv);
            // Use lower_bound to check if any composite starts with this key
            // We need a cursor for this — use get_latest range check
            auto result = txn->write_tx->get(root_idx, prefix);
            // A prefix match means: result key starts with our prefix
            // Actually, we need a range scan. Simpler: just lower_bound.
            // For now, do a cursor-based check.
            auto mc = txn->env->dwal_db->create_cursor(
               root_idx, psitri::dwal::read_mode::latest, /*skip_rw_lock=*/true);
            if (mc->lower_bound(prefix) && !mc->is_end())
            {
               auto found_key = mc->key();
               if (found_key.size() >= prefix.size()
                   && found_key.substr(0, prefix.size()) == prefix)
                  return MDBX_KEYEXIST;
            }
         }

         txn->write_tx->upsert(root_idx, composite, std::string_view{});
         return MDBX_SUCCESS;
      }

      // Non-DUPSORT path
      if (flags & MDBX_NOOVERWRITE)
      {
         auto result = txn->write_tx->get(root_idx, key_sv);
         if (result.found)
         {
            data->iov_base = const_cast<char*>(result.value.data.data());
            data->iov_len  = result.value.data.size();
            return MDBX_KEYEXIST;
         }
      }

      txn->write_tx->upsert(root_idx, key_sv, val_sv);
      return MDBX_SUCCESS;
   }
   catch (...)
   {
      return MDBX_PANIC;
   }
}

int mdbx_del(MDBX_txn* txn, MDBX_dbi dbi,
             const MDBX_val* key, const MDBX_val* data)
{
   if (!txn || !key || txn->is_readonly())
      return MDBX_EINVAL;

   uint32_t root_idx = dbi_root_index(txn->env, dbi);
   if (root_idx == UINT32_MAX)
      return MDBX_BAD_DBI;

   int rc = ensure_rw_root(txn, root_idx);
   if (rc != MDBX_SUCCESS)
      return rc;

   auto key_sv = to_sv(key);
   bool is_ds  = dbi_is_dupsort(txn->env, dbi);

   try
   {
      if (is_ds)
      {
         if (data)
         {
            // Delete specific (key, value) pair
            auto composite = dupsort_encode(key_sv, to_sv(data));
            bool removed = txn->write_tx->remove(root_idx, composite);
            return removed ? MDBX_SUCCESS : MDBX_NOTFOUND;
         }
         else
         {
            // Delete ALL values for this key — range remove
            auto lo = dupsort_key_prefix(key_sv);
            auto hi = dupsort_key_upper(key_sv);
            txn->write_tx->remove_range(root_idx, lo, hi);
            return MDBX_SUCCESS;
         }
      }

      // Non-DUPSORT
      bool removed = txn->write_tx->remove(root_idx, key_sv);
      return removed ? MDBX_SUCCESS : MDBX_NOTFOUND;
   }
   catch (...)
   {
      return MDBX_PANIC;
   }
}

int mdbx_replace(MDBX_txn* txn, MDBX_dbi dbi,
                 const MDBX_val* key, MDBX_val* new_data,
                 MDBX_val* old_data, MDBX_put_flags_t flags)
{
   if (!txn || !key)
      return MDBX_EINVAL;

   // Get old value first
   if (old_data)
   {
      int rc = mdbx_get(txn, dbi, key, old_data);
      if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND)
         return rc;
   }

   // Put new value
   if (new_data)
      return mdbx_put(txn, dbi, key, new_data, flags);

   // No new data = delete
   return mdbx_del(txn, dbi, key, nullptr);
}

// ── Cursor operations ────────────────────────────────────────────

int mdbx_cursor_open(MDBX_txn* txn, MDBX_dbi dbi, MDBX_cursor** cursor)
{
   if (!txn || !cursor)
      return MDBX_EINVAL;

   uint32_t root_idx = dbi_root_index(txn->env, dbi);
   if (root_idx == UINT32_MAX)
      return MDBX_BAD_DBI;

   try
   {
      auto* c = new MDBX_cursor();
      c->txn  = txn;
      c->dbi  = dbi;
      c->is_dupsort = dbi_is_dupsort(txn->env, dbi);

      // Always use latest mode — committed data lives in the RW layer
      // until a swap happens (which may not occur for small transactions).
      // MDBX serializes writers, so only one writer exists at a time,
      // making it safe for RO reads to see the RW layer.
      auto mode = psitri::dwal::read_mode::latest;

      // Ensure root is writable if RW
      if (!txn->is_readonly())
      {
         int rc = ensure_rw_root(txn, root_idx);
         if (rc != MDBX_SUCCESS)
         {
            delete c;
            return rc;
         }
      }

      // Skip the RW lock if the writer thread already holds it.
      bool writer = !txn->is_readonly() && txn->write_tx;
      auto mc = txn->env->dwal_db->create_cursor(root_idx, mode, writer);
      c->state = std::make_unique<cursor_state>(std::move(mc), c->is_dupsort);

      *cursor = c;
      return MDBX_SUCCESS;
   }
   catch (...)
   {
      return MDBX_PANIC;
   }
}

void mdbx_cursor_close(MDBX_cursor* cursor)
{
   delete cursor;
}

int mdbx_cursor_get(MDBX_cursor* cursor, MDBX_val* key,
                    MDBX_val* data, MDBX_cursor_op op)
{
   if (!cursor || !cursor->state)
      return MDBX_EINVAL;

   auto& st = *cursor->state;
   auto& mc = st.mc;

   try
   {
      bool ok = false;
      switch (op)
      {
         case MDBX_FIRST:
            ok = mc->seek_begin();
            break;
         case MDBX_LAST:
            ok = mc->seek_last();
            break;
         case MDBX_NEXT:
            if (mc->is_end() && mc->is_rend())
               ok = mc->seek_begin(); // First call after open
            else
               ok = mc->next();
            break;
         case MDBX_PREV:
            ok = mc->prev();
            break;
         case MDBX_GET_CURRENT:
            ok = !mc->is_end() && !mc->is_rend();
            break;
         case MDBX_SET:
         case MDBX_SET_KEY:
         {
            if (!key)
               return MDBX_EINVAL;
            auto key_sv = to_sv(key);
            if (st.dupsort)
            {
               // Seek to first entry with this key
               auto prefix = dupsort_key_prefix(key_sv);
               ok = mc->lower_bound(prefix);
               if (ok && !mc->is_end())
               {
                  auto found = mc->key();
                  if (found.size() < prefix.size()
                      || found.substr(0, prefix.size()) != std::string_view(prefix))
                     ok = false;
               }
            }
            else
            {
               ok = mc->seek(key_sv);
            }
            break;
         }
         case MDBX_SET_RANGE:
         case MDBX_SET_LOWERBOUND:
         {
            if (!key)
               return MDBX_EINVAL;
            auto key_sv = to_sv(key);
            if (st.dupsort)
            {
               auto prefix = dupsort_key_prefix(key_sv);
               ok = mc->lower_bound(prefix);
            }
            else
            {
               ok = mc->lower_bound(key_sv);
            }
            break;
         }
         case MDBX_SET_UPPERBOUND:
         {
            if (!key)
               return MDBX_EINVAL;
            auto key_sv = to_sv(key);
            if (st.dupsort)
            {
               auto upper = dupsort_key_upper(key_sv);
               ok = mc->lower_bound(upper);
            }
            else
            {
               ok = mc->upper_bound(key_sv);
            }
            break;
         }

         case MDBX_NEXT_NODUP:
            if (st.dupsort && st.valid)
            {
               // Skip past all entries with the current key
               auto upper = dupsort_key_upper(st.key_buf);
               ok = mc->lower_bound(upper);
            }
            else
            {
               ok = mc->next();
            }
            break;

         case MDBX_PREV_NODUP:
            if (st.dupsort && st.valid)
            {
               // Seek to first entry of current key, then prev
               auto prefix = dupsort_key_prefix(st.key_buf);
               ok = mc->lower_bound(prefix);
               if (ok && !mc->is_end())
                  ok = mc->prev();
               // Now we're at the last entry of the previous key — sync will decode it
            }
            else
            {
               ok = mc->prev();
            }
            break;

         // ── DUPSORT duplicate navigation ────────────────────────────
         case MDBX_FIRST_DUP:
         {
            if (!st.dupsort || !st.valid)
               return MDBX_NOTFOUND;
            auto prefix = dupsort_key_prefix(st.key_buf);
            ok = mc->lower_bound(prefix);
            if (ok && !mc->is_end())
            {
               auto found = mc->key();
               if (found.size() < prefix.size()
                   || found.substr(0, prefix.size()) != std::string_view(prefix))
                  ok = false;
            }
            break;
         }
         case MDBX_LAST_DUP:
         {
            if (!st.dupsort || !st.valid)
               return MDBX_NOTFOUND;
            // Seek to upper bound, then prev
            auto upper = dupsort_key_upper(st.key_buf);
            ok = mc->lower_bound(upper);
            if (mc->is_end())
               ok = mc->seek_last();
            else
               ok = mc->prev();
            // Verify still in same key
            if (ok && !mc->is_end() && !mc->is_rend())
            {
               auto prefix = dupsort_key_prefix(st.key_buf);
               auto found  = mc->key();
               if (found.size() < prefix.size()
                   || found.substr(0, prefix.size()) != std::string_view(prefix))
                  ok = false;
            }
            break;
         }
         case MDBX_NEXT_DUP:
         {
            if (!st.dupsort || !st.valid)
               return MDBX_NOTFOUND;
            std::string saved_key = st.key_buf;
            std::string saved_raw = st.raw_key;
            ok = mc->next();
            if (ok && !mc->is_end())
            {
               st.sync_key_val();
               if (!st.valid || st.key_buf != saved_key)
               {
                  // Moved past current key — restore position
                  mc->seek(saved_raw);
                  st.sync_key_val();
                  return MDBX_NOTFOUND;
               }
               if (key)
                  *key = st.key_val();
               if (data)
                  *data = st.data_val();
               return MDBX_SUCCESS;
            }
            // Hit end — restore
            mc->seek(saved_raw);
            st.sync_key_val();
            return MDBX_NOTFOUND;
         }
         case MDBX_PREV_DUP:
         {
            if (!st.dupsort || !st.valid)
               return MDBX_NOTFOUND;
            std::string saved_key = st.key_buf;
            std::string saved_raw = st.raw_key;
            ok = mc->prev();
            if (ok && !mc->is_rend())
            {
               st.sync_key_val();
               if (!st.valid || st.key_buf != saved_key)
               {
                  mc->seek(saved_raw);
                  st.sync_key_val();
                  return MDBX_NOTFOUND;
               }
               if (key)
                  *key = st.key_val();
               if (data)
                  *data = st.data_val();
               return MDBX_SUCCESS;
            }
            mc->seek(saved_raw);
            st.sync_key_val();
            return MDBX_NOTFOUND;
         }
         case MDBX_GET_BOTH:
         {
            if (!key || !data)
               return MDBX_EINVAL;
            if (!st.dupsort)
               return MDBX_EINVAL;
            auto composite = dupsort_encode(to_sv(key), to_sv(data));
            ok = mc->seek(composite);
            break;
         }
         case MDBX_GET_BOTH_RANGE:
         {
            if (!key || !data)
               return MDBX_EINVAL;
            if (!st.dupsort)
               return MDBX_EINVAL;
            // lower_bound on composite, verify same key
            auto composite = dupsort_encode(to_sv(key), to_sv(data));
            ok = mc->lower_bound(composite);
            if (ok && !mc->is_end())
            {
               auto prefix = dupsort_key_prefix(to_sv(key));
               auto found  = mc->key();
               if (found.size() < prefix.size()
                   || found.substr(0, prefix.size()) != std::string_view(prefix))
                  ok = false;
            }
            break;
         }

         default:
            return MDBX_EINVAL;
      }

      if (!ok || mc->is_end() || mc->is_rend())
         return MDBX_NOTFOUND;

      st.sync_key_val();
      if (!st.valid)
         return MDBX_NOTFOUND;

      if (key)
         *key = st.key_val();
      if (data)
         *data = st.data_val();

      return MDBX_SUCCESS;
   }
   catch (...)
   {
      return MDBX_PANIC;
   }
}

int mdbx_cursor_put(MDBX_cursor* cursor, const MDBX_val* key,
                    MDBX_val* data, MDBX_put_flags_t flags)
{
   if (!cursor || !cursor->txn || cursor->txn->is_readonly())
      return MDBX_BAD_TXN;
   return mdbx_put(cursor->txn, cursor->dbi, key, data, flags);
}

int mdbx_cursor_del(MDBX_cursor* cursor, MDBX_put_flags_t flags)
{
   if (!cursor || !cursor->state || !cursor->state->valid)
      return MDBX_EINVAL;
   if (!cursor->txn || cursor->txn->is_readonly())
      return MDBX_BAD_TXN;

   auto& st = *cursor->state;

   if (st.dupsort)
   {
      if (flags & MDBX_ALLDUPS)
      {
         // Delete all duplicates for the current key
         MDBX_val key = st.key_val();
         return mdbx_del(cursor->txn, cursor->dbi, &key, nullptr);
      }
      else
      {
         // Delete current (key, value) pair — use the raw composite key
         uint32_t root_idx = dbi_root_index(cursor->txn->env, cursor->dbi);
         int rc = ensure_rw_root(cursor->txn, root_idx);
         if (rc != MDBX_SUCCESS)
            return rc;
         auto composite = dupsort_encode(st.key_buf, st.val_buf);
         cursor->txn->write_tx->remove(root_idx, composite);
         return MDBX_SUCCESS;
      }
   }

   MDBX_val key = st.key_val();
   return mdbx_del(cursor->txn, cursor->dbi, &key, nullptr);
}

int mdbx_cursor_count(const MDBX_cursor* cursor, size_t* count)
{
   if (!cursor || !count)
      return MDBX_EINVAL;

   auto& st = *cursor->state;
   if (!st.valid)
      return MDBX_EINVAL;

   if (!st.dupsort)
   {
      *count = 1;
      return MDBX_SUCCESS;
   }

   // DUPSORT: count entries with same key prefix
   auto prefix = dupsort_key_prefix(st.key_buf);
   auto upper  = dupsort_key_upper(st.key_buf);

   // Save cursor position, create a temp cursor to count
   bool writer = !cursor->txn->is_readonly() && cursor->txn->write_tx;
   auto temp_mc = cursor->txn->env->dwal_db->create_cursor(
      dbi_root_index(cursor->txn->env, cursor->dbi),
      psitri::dwal::read_mode::latest, writer);

   size_t n = 0;
   if (temp_mc->lower_bound(prefix))
   {
      while (!temp_mc->is_end() && temp_mc->key() < std::string_view(upper))
      {
         ++n;
         if (!temp_mc->next())
            break;
      }
   }
   *count = n;
   return MDBX_SUCCESS;
}

int mdbx_cursor_renew(MDBX_txn* txn, MDBX_cursor* cursor)
{
   if (!txn || !cursor)
      return MDBX_EINVAL;

   uint32_t root_idx = dbi_root_index(txn->env, cursor->dbi);
   if (root_idx == UINT32_MAX)
      return MDBX_BAD_DBI;

   try
   {
      cursor->txn = txn;
      auto mode = txn->is_readonly()
                     ? psitri::dwal::read_mode::buffered
                     : psitri::dwal::read_mode::latest;
      bool writer = !txn->is_readonly() && txn->write_tx;
      auto mc = txn->env->dwal_db->create_cursor(root_idx, mode, writer);
      cursor->state = std::make_unique<cursor_state>(std::move(mc), cursor->is_dupsort);
      return MDBX_SUCCESS;
   }
   catch (...)
   {
      return MDBX_PANIC;
   }
}

MDBX_dbi mdbx_cursor_dbi(const MDBX_cursor* cursor)
{
   return cursor ? cursor->dbi : 0;
}

MDBX_txn* mdbx_cursor_txn(const MDBX_cursor* cursor)
{
   return cursor ? cursor->txn : nullptr;
}

// ════════════════════════════════════════════════════════════════════
// C++ API implementation
// ════════════════════════════════════════════════════════════════════

namespace mdbx
{
   // ── env methods ───────────────────────────────────────────────────

   MDBX_env_flags_t env::operate_parameters::make_flags() const noexcept
   {
      unsigned f = MDBX_ENV_DEFAULTS;
      if (mode == env::readonly)
         f |= MDBX_RDONLY;
      if (mode == env::write_mapped_io)
         f |= MDBX_WRITEMAP;
      switch (durability)
      {
         case env::robust_synchronous:       break;
         case env::half_synchronous_weak_last: f |= MDBX_NOMETASYNC; break;
         case env::lazy_weak_tail:           f |= MDBX_SAFE_NOSYNC; break;
         case env::whole_fragile:            f |= MDBX_UTTERLY_NOSYNC; break;
      }
      return static_cast<MDBX_env_flags_t>(f);
   }

   env& env::set_geometry(const geometry& geo)
   {
      error::success_or_throw(
         mdbx_env_set_geometry(handle_, geo.size_lower, geo.size_now,
                               geo.size_upper, geo.growth_step,
                               geo.shrink_threshold, geo.pagesize));
      return *this;
   }

   unsigned env::max_maps() const
   {
      return handle_ ? static_cast<const MDBX_env*>(handle_)->max_dbs : 0;
   }

   unsigned env::max_readers() const
   {
      return handle_ ? static_cast<const MDBX_env*>(handle_)->max_readers : 0;
   }

   env& env::set_context(void* ctx)
   {
      error::success_or_throw(mdbx_env_set_userctx(handle_, ctx));
      return *this;
   }

   bool env::sync_to_disk(bool force, bool nonblock)
   {
      return mdbx_env_sync_ex(handle_, force ? 1 : 0, nonblock ? 1 : 0) == MDBX_SUCCESS;
   }

   void env::close_map(const map_handle& map)
   {
      mdbx_dbi_close(handle_, map.dbi);
   }

   txn_managed env::start_read() const
   {
      MDBX_txn* t = nullptr;
      error::success_or_throw(
         mdbx_txn_begin(const_cast<MDBX_env*>(handle_), nullptr, MDBX_TXN_RDONLY, &t));
      return txn_managed(t);
   }

   txn_managed env::start_write(bool dont_wait)
   {
      MDBX_txn* t     = nullptr;
      auto      flags = dont_wait
                           ? static_cast<MDBX_txn_flags_t>(MDBX_TXN_READWRITE | MDBX_TXN_TRY)
                           : MDBX_TXN_READWRITE;
      error::success_or_throw(mdbx_txn_begin(handle_, nullptr, flags, &t));
      return txn_managed(t);
   }

   // ── env_managed ───────────────────────────────────────────────────

   env_managed::env_managed(const char* pathname,
                            const create_parameters& cp,
                            const operate_parameters& op,
                            bool /*accede*/)
   {
      MDBX_env* e = nullptr;
      error::success_or_throw(mdbx_env_create(&e));
      handle_ = e;

      if (op.max_maps)
         error::success_or_throw(mdbx_env_set_maxdbs(e, op.max_maps));
      if (op.max_readers)
         error::success_or_throw(mdbx_env_set_maxreaders(e, op.max_readers));

      error::success_or_throw(mdbx_env_set_geometry(
         e, cp.geometry.size_lower, cp.geometry.size_now,
         cp.geometry.size_upper, cp.geometry.growth_step,
         cp.geometry.shrink_threshold, cp.geometry.pagesize));

      auto flags = op.make_flags();
      if (!cp.use_subdirectory)
         flags = static_cast<MDBX_env_flags_t>(flags | MDBX_NOSUBDIR);

      error::success_or_throw(mdbx_env_open(e, pathname, flags, cp.file_mode_bits));
   }

   env_managed::~env_managed() noexcept
   {
      if (handle_)
      {
         mdbx_env_close(handle_);
         handle_ = nullptr;
      }
   }

   void env_managed::close(bool dont_sync)
   {
      if (handle_)
      {
         mdbx_env_close_ex(handle_, dont_sync ? 1 : 0);
         handle_ = nullptr;
      }
   }

   env_managed& env_managed::operator=(env_managed&& o) noexcept
   {
      if (handle_)
         mdbx_env_close(handle_);
      handle_   = o.handle_;
      o.handle_ = nullptr;
      return *this;
   }

   // ── txn methods ───────────────────────────────────────────────────

   ::mdbx::env txn::env() const noexcept
   {
      auto* e = mdbx_txn_env(handle_);
      return ::mdbx::env(reinterpret_cast<MDBX_env*>(e));
   }

   void* txn::get_context() const noexcept
   {
      return handle_ ? handle_->context : nullptr;
   }

   txn& txn::set_context(void* ctx)
   {
      if (handle_)
         handle_->context = ctx;
      return *this;
   }

   map_handle txn::open_map(const char* name, key_mode /*km*/, value_mode vm) const
   {
      MDBX_dbi   dbi   = 0;
      unsigned   flags = 0;
      if (vm != value_mode::single)
         flags |= static_cast<unsigned>(vm);
      int rc = mdbx_dbi_open(const_cast<MDBX_txn*>(handle_), name,
                             static_cast<MDBX_db_flags_t>(flags), &dbi);
      if (rc == MDBX_NOTFOUND)
         throw not_found();
      error::success_or_throw(rc);
      return map_handle(dbi);
   }

   map_handle txn::create_map(const char* name, key_mode /*km*/, value_mode vm)
   {
      MDBX_dbi   dbi   = 0;
      unsigned   flags = MDBX_CREATE;
      if (vm != value_mode::single)
         flags |= static_cast<unsigned>(vm);
      error::success_or_throw(
         mdbx_dbi_open(handle_, name, static_cast<MDBX_db_flags_t>(flags), &dbi));
      return map_handle(dbi);
   }

   void txn::drop_map(map_handle map)
   {
      error::success_or_throw(mdbx_drop(handle_, map.dbi, 1));
   }

   void txn::clear_map(map_handle map)
   {
      error::success_or_throw(mdbx_drop(handle_, map.dbi, 0));
   }

   cursor_managed txn::open_cursor(map_handle map) const
   {
      MDBX_cursor* c = nullptr;
      error::success_or_throw(
         mdbx_cursor_open(const_cast<MDBX_txn*>(handle_), map.dbi, &c));
      return cursor_managed(c);
   }

   slice txn::get(map_handle map, const slice& key) const
   {
      MDBX_val k = key;
      MDBX_val v;
      int rc = mdbx_get(handle_, map.dbi, &k, &v);
      if (rc == MDBX_NOTFOUND)
         throw not_found();
      error::success_or_throw(rc);
      return slice(v);
   }

   slice txn::get(map_handle map, const slice& key,
                  const slice& value_at_absence) const
   {
      MDBX_val k = key;
      MDBX_val v;
      int rc = mdbx_get(handle_, map.dbi, &k, &v);
      if (rc == MDBX_NOTFOUND)
         return value_at_absence;
      error::success_or_throw(rc);
      return slice(v);
   }

   void txn::upsert(map_handle map, const slice& key, const slice& value)
   {
      MDBX_val k = key;
      MDBX_val v = value;
      error::success_or_throw(mdbx_put(handle_, map.dbi, &k, &v, MDBX_UPSERT));
   }

   void txn::insert(map_handle map, const slice& key, slice value)
   {
      MDBX_val k = key;
      MDBX_val v = value;
      error::success_or_throw(mdbx_put(handle_, map.dbi, &k, &v, MDBX_NOOVERWRITE));
   }

   value_result txn::try_insert(map_handle map, const slice& key, slice value)
   {
      MDBX_val k = key;
      MDBX_val v = value;
      int rc = mdbx_put(handle_, map.dbi, &k, &v, MDBX_NOOVERWRITE);
      if (rc == MDBX_KEYEXIST)
         return {slice(v), false};
      error::success_or_throw(rc);
      return {value, true};
   }

   void txn::update(map_handle map, const slice& key, const slice& value)
   {
      MDBX_val k = key;
      MDBX_val v = value;
      error::success_or_throw(mdbx_put(handle_, map.dbi, &k, &v, MDBX_CURRENT));
   }

   bool txn::try_update(map_handle map, const slice& key, const slice& value)
   {
      MDBX_val k = key;
      MDBX_val v = value;
      int rc = mdbx_put(handle_, map.dbi, &k, &v, MDBX_CURRENT);
      if (rc == MDBX_NOTFOUND)
         return false;
      error::success_or_throw(rc);
      return true;
   }

   MDBX_error_t txn::put(map_handle map, const slice& key, slice* value,
                          MDBX_put_flags_t flags) noexcept
   {
      MDBX_val k = key;
      MDBX_val v = *value;
      int rc = mdbx_put(handle_, map.dbi, &k, &v, flags);
      if (rc == MDBX_SUCCESS || rc == MDBX_KEYEXIST)
         *value = slice(v);
      return static_cast<MDBX_error_t>(rc);
   }

   bool txn::erase(map_handle map, const slice& key)
   {
      MDBX_val k = key;
      int rc = mdbx_del(handle_, map.dbi, &k, nullptr);
      if (rc == MDBX_NOTFOUND)
         return false;
      error::success_or_throw(rc);
      return true;
   }

   bool txn::erase(map_handle map, const slice& key, const slice& value)
   {
      MDBX_val k = key;
      MDBX_val v = value;
      int rc = mdbx_del(handle_, map.dbi, &k, &v);
      if (rc == MDBX_NOTFOUND)
         return false;
      error::success_or_throw(rc);
      return true;
   }

   // ── txn_managed ───────────────────────────────────────────────────

   txn_managed::~txn_managed() noexcept
   {
      if (handle_)
      {
         mdbx_txn_abort(handle_);
         handle_ = nullptr;
      }
   }

   void txn_managed::abort()
   {
      if (handle_)
      {
         error::success_or_throw(mdbx_txn_abort(handle_));
         handle_ = nullptr;
      }
   }

   void txn_managed::commit()
   {
      if (handle_)
      {
         error::success_or_throw(mdbx_txn_commit(handle_));
         handle_ = nullptr;
      }
   }

   txn_managed& txn_managed::operator=(txn_managed&& o) noexcept
   {
      if (handle_)
         mdbx_txn_abort(handle_);
      handle_   = o.handle_;
      o.handle_ = nullptr;
      return *this;
   }

   // ── cursor methods ────────────────────────────────────────────────

   cursor::move_result cursor::do_get(MDBX_cursor_op op, MDBX_val* key,
                                      MDBX_val* data, bool throw_notfound) const
   {
      MDBX_val k{}, d{};
      if (key)
         k = *key;
      int rc = mdbx_cursor_get(const_cast<MDBX_cursor*>(handle_), &k, &d, op);
      if (rc == MDBX_NOTFOUND)
      {
         if (throw_notfound)
            throw not_found();
         return move_result({}, {}, false);
      }
      error::success_or_throw(rc);
      return move_result(slice(k), slice(d), true);
   }

   cursor::move_result cursor::to_first(bool throw_notfound)
   {
      return do_get(MDBX_FIRST, nullptr, nullptr, throw_notfound);
   }

   cursor::move_result cursor::to_last(bool throw_notfound)
   {
      return do_get(MDBX_LAST, nullptr, nullptr, throw_notfound);
   }

   cursor::move_result cursor::to_next(bool throw_notfound)
   {
      return do_get(MDBX_NEXT, nullptr, nullptr, throw_notfound);
   }

   cursor::move_result cursor::to_previous(bool throw_notfound)
   {
      return do_get(MDBX_PREV, nullptr, nullptr, throw_notfound);
   }

   cursor::move_result cursor::current(bool throw_notfound) const
   {
      return do_get(MDBX_GET_CURRENT, nullptr, nullptr, throw_notfound);
   }

   cursor::move_result cursor::to_key_equal(const slice& key, bool throw_notfound)
   {
      MDBX_val k = key;
      return do_get(MDBX_SET_KEY, &k, nullptr, throw_notfound);
   }

   bool cursor::seek(const slice& key)
   {
      MDBX_val k = key;
      MDBX_val d;
      int rc = mdbx_cursor_get(handle_, &k, &d, MDBX_SET_RANGE);
      return rc == MDBX_SUCCESS;
   }

   cursor::move_result cursor::find(const slice& key, bool throw_notfound)
   {
      MDBX_val k = key;
      return do_get(MDBX_SET_KEY, &k, nullptr, throw_notfound);
   }

   cursor::move_result cursor::lower_bound(const slice& key, bool throw_notfound)
   {
      MDBX_val k = key;
      return do_get(MDBX_SET_RANGE, &k, nullptr, throw_notfound);
   }

   cursor::move_result cursor::upper_bound(const slice& key, bool throw_notfound)
   {
      MDBX_val k = key;
      return do_get(MDBX_SET_UPPERBOUND, &k, nullptr, throw_notfound);
   }

   // DUPSORT multi-value stubs
   cursor::move_result cursor::to_current_first_multi(bool throw_notfound)
   {
      return do_get(MDBX_FIRST_DUP, nullptr, nullptr, throw_notfound);
   }

   cursor::move_result cursor::to_current_last_multi(bool throw_notfound)
   {
      return do_get(MDBX_LAST_DUP, nullptr, nullptr, throw_notfound);
   }

   cursor::move_result cursor::to_next_dup(bool throw_notfound)
   {
      return do_get(MDBX_NEXT_DUP, nullptr, nullptr, throw_notfound);
   }

   cursor::move_result cursor::to_prev_dup(bool throw_notfound)
   {
      return do_get(MDBX_PREV_DUP, nullptr, nullptr, throw_notfound);
   }

   cursor::move_result cursor::to_next_nodup(bool throw_notfound)
   {
      return do_get(MDBX_NEXT_NODUP, nullptr, nullptr, throw_notfound);
   }

   cursor::move_result cursor::to_prev_nodup(bool throw_notfound)
   {
      return do_get(MDBX_PREV_NODUP, nullptr, nullptr, throw_notfound);
   }

   cursor::move_result cursor::find_multivalue(const slice& key, const slice& value,
                                                bool throw_notfound)
   {
      MDBX_val k = key;
      MDBX_val v = value;
      return do_get(MDBX_GET_BOTH, &k, &v, throw_notfound);
   }

   cursor::move_result cursor::lower_bound_multivalue(const slice& key, const slice& value,
                                                       bool throw_notfound)
   {
      MDBX_val k = key;
      MDBX_val v = value;
      return do_get(MDBX_GET_BOTH_RANGE, &k, &v, throw_notfound);
   }

   size_t cursor::count_multivalue() const
   {
      size_t cnt = 0;
      mdbx_cursor_count(handle_, &cnt);
      return cnt;
   }

   bool cursor::eof() const
   {
      if (!handle_ || !handle_->state)
         return true;
      return handle_->state->mc->is_end();
   }

   bool cursor::on_first() const
   {
      if (!handle_ || !handle_->state || handle_->state->mc->is_end())
         return false;
      auto current_key = handle_->state->mc->key();
      auto root_idx    = dbi_root_index(handle_->txn->env, handle_->dbi);
      auto mode        = handle_->txn->is_readonly() ? psitri::dwal::read_mode::buffered
                                                     : psitri::dwal::read_mode::latest;
      auto tmp = handle_->txn->env->dwal_db->create_cursor(
          root_idx, mode, /*skip_rw_lock=*/!handle_->txn->is_readonly());
      return tmp->seek_begin() && tmp->key() == current_key;
   }

   bool cursor::on_last() const
   {
      if (!handle_ || !handle_->state || handle_->state->mc->is_end())
         return false;
      auto current_key = handle_->state->mc->key();
      auto root_idx    = dbi_root_index(handle_->txn->env, handle_->dbi);
      auto mode        = handle_->txn->is_readonly() ? psitri::dwal::read_mode::buffered
                                                     : psitri::dwal::read_mode::latest;
      auto tmp = handle_->txn->env->dwal_db->create_cursor(
          root_idx, mode, /*skip_rw_lock=*/!handle_->txn->is_readonly());
      return tmp->seek_last() && tmp->key() == current_key;
   }

   void cursor::upsert(const slice& key, const slice& value)
   {
      MDBX_val k = key;
      MDBX_val v = value;
      error::success_or_throw(mdbx_cursor_put(handle_, &k, &v, MDBX_UPSERT));
   }

   void cursor::insert(const slice& key, slice value)
   {
      MDBX_val k = key;
      MDBX_val v = value;
      error::success_or_throw(mdbx_cursor_put(handle_, &k, &v, MDBX_NOOVERWRITE));
   }

   value_result cursor::try_insert(const slice& key, slice value)
   {
      MDBX_val k = key;
      MDBX_val v = value;
      int rc = mdbx_cursor_put(handle_, &k, &v, MDBX_NOOVERWRITE);
      if (rc == MDBX_KEYEXIST)
         return {slice(v), false};
      error::success_or_throw(rc);
      return {value, true};
   }

   void cursor::update(const slice& key, const slice& value)
   {
      MDBX_val k = key;
      MDBX_val v = value;
      error::success_or_throw(mdbx_cursor_put(handle_, &k, &v, MDBX_CURRENT));
   }

   bool cursor::try_update(const slice& key, const slice& value)
   {
      MDBX_val k = key;
      MDBX_val v = value;
      int rc = mdbx_cursor_put(handle_, &k, &v, MDBX_CURRENT);
      if (rc == MDBX_NOTFOUND)
         return false;
      error::success_or_throw(rc);
      return true;
   }

   bool cursor::erase(bool /*whole_multivalue*/)
   {
      int rc = mdbx_cursor_del(handle_, MDBX_UPSERT);
      if (rc == MDBX_NOTFOUND)
         return false;
      error::success_or_throw(rc);
      return true;
   }

   bool cursor::erase(const slice& key, bool /*whole_multivalue*/)
   {
      MDBX_val k = key;
      int rc = mdbx_del(mdbx_cursor_txn(handle_), mdbx_cursor_dbi(handle_), &k, nullptr);
      if (rc == MDBX_NOTFOUND)
         return false;
      error::success_or_throw(rc);
      return true;
   }

   void cursor::renew(::mdbx::txn& t)
   {
      error::success_or_throw(mdbx_cursor_renew(t, handle_));
   }

   void cursor::bind(::mdbx::txn& t, map_handle map)
   {
      // Rebind = close + reopen
      mdbx_cursor_close(handle_);
      handle_ = nullptr;
      MDBX_cursor* c = nullptr;
      error::success_or_throw(mdbx_cursor_open(t, map.dbi, &c));
      handle_ = c;
   }

   ::mdbx::txn cursor::txn() const
   {
      return ::mdbx::txn(mdbx_cursor_txn(handle_));
   }

   map_handle cursor::map() const
   {
      return map_handle(mdbx_cursor_dbi(handle_));
   }

   // ── cursor_managed ────────────────────────────────────────────────

   cursor_managed::~cursor_managed() noexcept
   {
      if (handle_)
      {
         mdbx_cursor_close(handle_);
         handle_ = nullptr;
      }
   }

   void cursor_managed::close()
   {
      if (handle_)
      {
         mdbx_cursor_close(handle_);
         handle_ = nullptr;
      }
   }

   cursor_managed& cursor_managed::operator=(cursor_managed&& o) noexcept
   {
      if (handle_)
         mdbx_cursor_close(handle_);
      handle_   = o.handle_;
      o.handle_ = nullptr;
      return *this;
   }

}  // namespace mdbx
