/** @file mdbx_impl.cpp
 *  @brief PsiTri-backed MDBX C and C++ API implementation.
 */
#include <mdbx.h>
#include <mdbx.h++>

#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/dwal/dwal_database.hpp>
#include <psitri/dwal/dwal_transaction.hpp>
#include <psitri/dwal/merge_cursor.hpp>
#include <psitri/dwal/transaction.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session.hpp>
#include <psitri/write_session_impl.hpp>

#include <atomic>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <map>
#include <mutex>
#include <set>
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
   bool        reverse_dup;    // (flags & MDBX_REVERSEDUP) != 0
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

   // Read mode for RO transactions: 0=buffered, 1=latest, 2=direct(get_latest)
   int               read_mode   = 1;  // default: latest (sees all committed data)

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
      dbis.push_back({"__meta__", 0, 0, false, false});
      // DBI 1: unnamed default database (root 1)
      dbis.push_back({"", 1, MDBX_DB_DEFAULTS, false, false});
   }

   MDBX_dbi allocate_dbi(const std::string& name, unsigned flags)
   {
      MDBX_dbi dbi = static_cast<MDBX_dbi>(dbis.size());
      uint32_t root_idx = dbi + 1;
      dbis.push_back({name, root_idx, flags, (flags & MDBX_DUPSORT) != 0,
                      (flags & MDBX_REVERSEDUP) != 0});
      // Use dbi+1 as root_index (root 0 = meta, root 1 = default, root 2+ = named)
      if (!name.empty())
         name_to_dbi[name] = dbi;

      // Under COWART, latest reads are lock-free via cow_coordinator.
      // No explicit locking setup needed.

      return dbi;
   }

   /// Like allocate_dbi but binds a specific root_idx. Used during catalog
   /// restore so reopens see the same name → root_idx mapping that was
   /// established when the DBIs were first created — the dbi number itself
   /// is sequential (assigned in catalog-iteration order) but root_idx is
   /// taken from the catalog entry, decoupling DBI numbering from root
   /// addressing.
   MDBX_dbi allocate_dbi_with_root(const std::string& name, unsigned flags,
                                   uint32_t root_idx)
   {
      MDBX_dbi dbi = static_cast<MDBX_dbi>(dbis.size());
      dbis.push_back({name, root_idx, flags, (flags & MDBX_DUPSORT) != 0,
                      (flags & MDBX_REVERSEDUP) != 0});
      if (!name.empty())
         name_to_dbi[name] = dbi;
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

static std::string dupsort_encode(std::string_view key, std::string_view value,
                                  bool reverse_dup = false)
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
   if (reverse_dup)
   {
      for (char c : value)
         result += static_cast<char>(~static_cast<unsigned char>(c));
   }
   else
   {
      result.append(value.data(), value.size());
   }
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
                           std::string& val_out,
                           bool reverse_dup = false)
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
            if (reverse_dup)
            {
               val_out.resize(composite.size() - i - 2);
               for (size_t j = i + 2; j < composite.size(); ++j)
                  val_out[j - i - 2] = static_cast<char>(
                     ~static_cast<unsigned char>(composite[j]));
            }
            else
            {
               val_out.assign(composite.data() + i + 2, composite.size() - i - 2);
            }
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
   bool                             valid      = false;
   bool                             dupsort    = false;
   bool                             positioned = false;
   std::string                      key_buf;   // exposed key
   std::string                      val_buf;   // exposed value
   std::string                      raw_key;   // raw composite (DUPSORT only)

   bool                             rev_dup    = false;

   explicit cursor_state(psitri::dwal::owned_merge_cursor m, bool ds = false, bool rd = false)
       : mc(std::move(m)), dupsort(ds), rev_dup(rd)
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
         if (!dupsort_decode(raw_key, key_buf, val_buf, rev_dup))
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

// Write operation buffered before flush to DWAL transaction
struct write_op
{
   enum class type { upsert, remove, remove_range };
   type             op;
   std::string      key;
   std::string      value;   // for upsert; high bound for remove_range
};

struct MDBX_txn
{
   MDBX_env*         env       = nullptr;
   MDBX_txn_flags_t  txn_flags = MDBX_TXN_READWRITE;
   void*             context   = nullptr;
   uint64_t          id        = 0;

   /// True when MDBX_TXN_USE_DWAL was passed at txn_begin. Default
   /// (false) routes writes through the direct COW path (no buffer cap).
   bool              use_dwal  = false;

   // ── Direct COW path (default) ──────────────────────────────────────
   // One write_session per RW txn, plus one per-root psitri::transaction
   // lazily created on first write. Each per-root transaction owns the
   // root's write mutex until commit() or abort(). MDBX_txn destruction
   // aborts each by ~transaction(). No write_buffer, no replay, no cap.
   std::shared_ptr<psitri::write_session>          direct_session;
   std::map<uint32_t, psitri::transaction>         direct_root_txns;

   // ── DWAL path (opt-in via MDBX_TXN_USE_DWAL) ───────────────────────
   // RW transaction — lazily created at flush time with all touched roots
   std::unique_ptr<psitri::dwal::transaction>      write_tx;
   std::vector<uint32_t>                           write_roots;

   // Write buffer: accumulates mutations per root until flush
   std::map<uint32_t, std::vector<write_op>>       write_buffer;
   // Index for read-your-writes: key -> index of last op in write_buffer
   std::map<uint32_t, std::map<std::string, size_t, std::less<>>> write_index;
   bool                                            buffer_flushed = false;

   // RO transaction (mutable: lazily created even from const mdbx_get)
   mutable std::unique_ptr<psitri::dwal::dwal_read_session> read_session;

   // Value buffer for get() return — kept alive until next get()
   std::string get_buf;

   bool committed = false;
   bool aborted   = false;

   uint64_t write_gen = 0;

   bool is_readonly() const { return (txn_flags & MDBX_TXN_RDONLY) != 0; }
};

struct MDBX_cursor
{
   MDBX_txn* txn  = nullptr;
   MDBX_dbi  dbi  = 0;
   bool      is_dupsort    = false;
   bool      is_reverse_dup = false;
   void*     context = nullptr;

   std::unique_ptr<cursor_state> state;
   uint64_t cursor_gen = 0;
};

// ════════════════════════════════════════════════════════════════════
// Helpers
// ════════════════════════════════════════════════════════════════════

static std::string_view to_sv(const MDBX_val* v)
{
   return v ? std::string_view(static_cast<const char*>(v->iov_base), v->iov_len)
            : std::string_view{};
}

/// Lazily get-or-create a psitri::transaction for the given root_idx in
/// direct COW mode. Acquires the per-root write mutex on first call.
/// Throws if the txn is read-only or DWAL mode.
static psitri::transaction& ensure_direct_root_txn(MDBX_txn* txn, uint32_t root_idx)
{
   assert(!txn->is_readonly() && !txn->use_dwal && txn->direct_session);
   auto it = txn->direct_root_txns.find(root_idx);
   if (it != txn->direct_root_txns.end())
      return it->second;
   // start_transaction returns by value (move-only). Use try_emplace to
   // construct in place via piecewise/forward_as_tuple semantics.
   auto [ins_it, inserted] = txn->direct_root_txns.try_emplace(
       root_idx,
       txn->direct_session->start_transaction(root_idx, psitri::tx_mode::batch));
   return ins_it->second;
}

static int flush_write_buffer(MDBX_txn* txn);
static int ensure_rw_root(MDBX_txn* txn, uint32_t root_idx);

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

static bool dbi_is_reverse_dup(MDBX_env* env, MDBX_dbi dbi)
{
   std::shared_lock lk(env->dbi_mutex);
   if (dbi >= env->dbis.size())
      return false;
   return env->dbis[dbi].reverse_dup;
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

const MDBX_build_info mdbx_build = {
   __DATE__ " " __TIME__,
   "psitrimdbx",
   "",
#if defined(__clang__)
   "clang " __clang_version__,
#elif defined(__GNUC__)
   "gcc " __VERSION__,
#else
   "unknown",
#endif
   ""
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
      case MDBX_EKEYMISMATCH:   return "MDBX_EKEYMISMATCH: Key mismatch (APPEND)";
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

      // Write a small mdbx.dat marker so callers that probe for the
      // canonical libmdbx file layout (e.g. Silkworm's open precheck,
      // which requires file_size > 0) see a non-empty regular file.
      if (!(flags & MDBX_RDONLY))
      {
         auto marker = env->path / "mdbx.dat";
         if (!std::filesystem::exists(marker) ||
             std::filesystem::file_size(marker) == 0)
         {
            std::ofstream out{marker, std::ios::binary | std::ios::trunc};
            out.write("psitrimdbx", 10);
         }
      }

      // Create WAL directory
      auto wal_dir = env->path / "wal";
      std::filesystem::create_directories(wal_dir);

      psitri::dwal::dwal_config cfg;
      cfg.merge_threads = 2;

      env->dwal_db = std::make_unique<psitri::dwal::dwal_database>(
         env->db, wal_dir, cfg);

      env->init_default_dbi();

      // Under COWART, latest reads are lock-free via cow_coordinator.

      // Restore named DBIs from catalog (PsiTri root 1 = unnamed default / MAIN_DBI).
      // Catalog value layout: legacy = 4-byte flags only; current = 4-byte
      // flags + 4-byte root_idx. When root_idx is present we must use it
      // instead of letting allocate_dbi assign one — see write path comment.
      try
      {
         auto rs = env->db->start_read_session();
         auto cur = rs->create_cursor(1);
         if (cur.seek_begin())
         {
            do
            {
               auto name = std::string(cur.key().data(), cur.key().size());
               auto val = cur.value<std::string>();
               unsigned f = 0;
               uint32_t r = 0;
               if (val && val->size() >= sizeof(uint32_t))
                  std::memcpy(&f, val->data(), sizeof(uint32_t));
               if (val && val->size() >= 2 * sizeof(uint32_t))
                  std::memcpy(&r, val->data() + sizeof(uint32_t), sizeof(uint32_t));
               if (r != 0)
                  env->allocate_dbi_with_root(name, f, r);
               else
                  env->allocate_dbi(name, f);
            } while (cur.next());
         }
      }
      catch (...)
      {
         // If catalog read fails on first open, no named DBIs to restore.
      }

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

int mdbx_env_close_ex(MDBX_env* env, int dont_sync)
{
   if (!env)
      return MDBX_EINVAL;

   // Drain all DWAL RW btrees into PsiTri before closing.
   // This ensures all committed data survives across restarts.
   if (env->dwal_db && env->db && !dont_sync)
   {
      try
      {
         std::shared_lock lk(env->dbi_mutex);
         std::vector<uint32_t> active_roots;
         for (auto& di : env->dbis)
         {
            if (di.root_index != UINT32_MAX)
               active_roots.push_back(di.root_index);
         }
         lk.unlock();

         // Flush all DWAL data to persistent storage first
         for (auto root_idx : active_roots)
            env->dwal_db->flush_wal(root_idx);

         auto ws = env->db->start_write_session();
         for (auto root_idx : active_roots)
         {
            // Use a merge cursor that shows live keys (tombstones filtered)
            auto mc = env->dwal_db->create_cursor(root_idx, psitri::dwal::read_mode::latest);

            // Build set of live keys from DWAL merge view
            std::vector<std::pair<std::string, std::string>> live_entries;
            if (mc->seek_begin())
            {
               do
               {
                  std::string key(mc->key());
                  std::string val;
                  if (mc->current_source() == psitri::dwal::merge_cursor::source::tri)
                  {
                     auto* tc = mc->tri_cursor();
                     tc->get_value([&val](psitri::value_view vv) {
                        val.assign(vv.data(), vv.size());
                     });
                  }
                  else
                  {
                     auto& bv = mc->current_value();
                     val.assign(bv.data.data(), bv.data.size());
                  }
                  live_entries.emplace_back(std::move(key), std::move(val));
               } while (mc->next());
            }

            // Write live entries to PsiTri; also remove stale PsiTri keys
            auto wt = ws->start_transaction(root_idx);
            auto tri_cursor = wt.read_cursor();

            // Remove keys that exist in PsiTri but are not in the live set
            std::set<std::string> live_keys;
            for (auto& [k, v] : live_entries)
               live_keys.insert(k);

            if (tri_cursor.seek_begin())
            {
               std::vector<std::string> stale;
               do
               {
                  std::string tk(tri_cursor.key().data(), tri_cursor.key().size());
                  if (live_keys.find(tk) == live_keys.end())
                     stale.push_back(std::move(tk));
               } while (tri_cursor.next());
               for (auto& k : stale)
                  wt.remove(k);
            }

            for (auto& [k, v] : live_entries)
               wt.upsert(k, v);
            wt.commit();
         }
      }
      catch (...)
      {
         // Best effort — if drain fails, data may be lost.
      }
   }

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

int mdbx_env_set_read_mode(MDBX_env* env, int mode)
{
   if (!env || mode < 0 || mode > 2)
      return MDBX_EINVAL;
   env->read_mode = mode;
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
      t->use_dwal  = (flags & MDBX_TXN_USE_DWAL) != 0;

      if (flags & MDBX_TXN_RDONLY)
      {
         t->read_session = std::make_unique<psitri::dwal::dwal_read_session>(
            env->dwal_db->start_read_session());
      }
      else if (!t->use_dwal)
      {
         // Direct COW path: open a write_session up front. Per-root
         // psitri::transaction objects are created lazily on first
         // touch via ensure_direct_root_txn().
         t->direct_session = env->db->start_write_session();
      }
      // DWAL path: write_tx created lazily at first flush_write_buffer.

      *txn = t;
      return MDBX_SUCCESS;
   }
   catch (...)
   {
      return MDBX_ENOMEM;
   }
}

/// Ensure the write transaction is created with the right roots.
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
      // ALWAYS re-flush from write_buffer at commit. Direct upserts on
      // write_tx after a flush (the buffer_flushed=true path in mdbx_put)
      // are not durable for roots whose initial replay phase had 0 ops —
      // psitri::dwal::transaction does not pick them up. write_buffer is
      // the source of truth, so abort any in-flight write_tx and replay
      // everything fresh.
      if (txn->write_tx)
      {
         txn->write_tx->abort();
         txn->write_tx.reset();
         txn->buffer_flushed = false;
      }

      if (!txn->write_buffer.empty())
      {
         int rc = flush_write_buffer(txn);
         if (rc != MDBX_SUCCESS)
         {
            txn->aborted = true;
            delete txn;
            return rc;
         }
      }

      if (txn->write_tx)
         txn->write_tx->commit();

      // Force WAL flush for all roots so subsequent read sessions see
      // newly committed data. Without this, reads on roots first written
      // in this txn may return NOTFOUND because the WAL entry isn't
      // visible to the next read_session.
      for (auto root_idx : txn->write_roots)
         txn->env->dwal_db->flush_wal(root_idx);

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
      // Discard write buffer — no DWAL state was touched if not flushed
      txn->write_buffer.clear();
      txn->write_index.clear();

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
      if (!txn->read_session)
         txn->read_session = std::make_unique<psitri::dwal::dwal_read_session>(
            txn->env->dwal_db->start_read_session());
      // Session auto-refreshes on generation change — no explicit refresh needed.
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
   }

   // Write catalog entry to root 1 (MAIN_DBI) through the write buffer.
   // Format: 4 bytes flags, then 4 bytes root_idx. Storing root_idx is
   // essential because allocate_dbi assigns DBI numbers sequentially —
   // on reopen the catalog is iterated in ART (sorted) order which differs
   // from the original creation order, so without persisting root_idx the
   // restored DBI → root mapping would be wrong and reads would hit the
   // wrong root.
   {
      int rc = ensure_rw_root(txn, 1);
      if (rc != MDBX_SUCCESS)
         return rc;

      uint32_t f = flags;
      uint32_t r = env->dbis[*dbi].root_index;
      std::string catalog_value;
      catalog_value.resize(sizeof(uint32_t) * 2);
      std::memcpy(catalog_value.data(), &f, sizeof(uint32_t));
      std::memcpy(catalog_value.data() + sizeof(uint32_t), &r, sizeof(uint32_t));

      auto& ops = txn->write_buffer[1];
      txn->write_index[1][sname] = ops.size();
      ops.push_back({write_op::type::upsert, sname, catalog_value});
      if (txn->buffer_flushed && txn->write_tx)
         txn->write_tx->upsert(1, sname, std::string_view(catalog_value));
   }

   return MDBX_SUCCESS;
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
      // Flush write buffer so cursor sees all pending writes
      if (!txn->is_readonly() && !txn->buffer_flushed)
      {
         auto* mtxn = const_cast<MDBX_txn*>(txn);
         int frc = flush_write_buffer(mtxn);
         if (frc != MDBX_SUCCESS)
            return frc;
      }

      static constexpr psitri::dwal::read_mode modes[] = {
         psitri::dwal::read_mode::buffered,
         psitri::dwal::read_mode::latest,
         psitri::dwal::read_mode::trie,
      };
      auto mode = txn->is_readonly() ? modes[txn->env->read_mode]
                                     : psitri::dwal::read_mode::latest;
      auto mc = txn->env->dwal_db->create_cursor(root_idx, mode,
                                                  /*skip_rw_lock=*/!txn->is_readonly() && txn->write_tx != nullptr);
      stat->ms_entries = mc->count_keys();
   }
   catch (...)
   {
      // If counting fails, return zeroed stats rather than failing.
   }

   return MDBX_SUCCESS;
}

int mdbx_drop(MDBX_txn* txn, MDBX_dbi dbi, int del)
{
   if (!txn || txn->is_readonly())
      return MDBX_BAD_TXN;

   uint32_t root_idx = dbi_root_index(txn->env, dbi);
   if (root_idx == UINT32_MAX)
      return MDBX_BAD_DBI;

   {
      int rc = ensure_rw_root(txn, root_idx);
      if (rc != MDBX_SUCCESS)
         return rc;
   }

   try
   {
      // Flush buffer so we can iterate current state
      if (!txn->buffer_flushed && !txn->write_buffer.empty())
      {
         int frc = flush_write_buffer(txn);
         if (frc != MDBX_SUCCESS)
            return frc;
      }

      // Clear: remove all entries by iterating with a cursor.
      bool writer = txn->write_tx != nullptr;
      auto mc = txn->env->dwal_db->create_cursor(root_idx, psitri::dwal::read_mode::latest,
                                               /*skip_rw_lock=*/writer);
      std::vector<std::string> keys_to_remove;
      if (mc->seek_begin())
      {
         do
         {
            keys_to_remove.emplace_back(mc->key());
         } while (mc->next());
      }
      if (txn->buffer_flushed && txn->write_tx)
      {
         for (auto& k : keys_to_remove)
         {
            txn->write_tx->remove(root_idx, k);
            // Also buffer for potential re-flush on root expansion
            txn->write_buffer[root_idx].push_back({write_op::type::remove, k, {}});
         }
      }
      else
      {
         for (auto& k : keys_to_remove)
         {
            auto& ops = txn->write_buffer[root_idx];
            txn->write_index[root_idx][k] = ops.size();
            ops.push_back({write_op::type::remove, k, {}});
         }
      }

      if (del)
      {
         // Remove the DBI entry from root 1 (MAIN_DBI) through write buffer.
         std::shared_lock lk(txn->env->dbi_mutex);
         if (dbi < txn->env->dbis.size() && !txn->env->dbis[dbi].name.empty())
         {
            auto name = txn->env->dbis[dbi].name;
            lk.unlock();

            int rc = ensure_rw_root(txn, 1);
            if (rc != MDBX_SUCCESS)
               return rc;
            auto& ops = txn->write_buffer[1];
            txn->write_index[1][name] = ops.size();
            ops.push_back({write_op::type::remove, name, {}});
            if (txn->buffer_flushed && txn->write_tx)
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
         // DUPSORT: flush buffer in RW txn so cursor sees all writes
         if (!txn->is_readonly() && !txn->buffer_flushed)
         {
            int frc = flush_write_buffer(mtxn);
            if (frc != MDBX_SUCCESS)
               return frc;
         }

         // Find the first composite key with this key prefix.
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
         bool rev = dbi_is_reverse_dup(txn->env, dbi);
         std::string dk, dv;
         if (!dupsort_decode(found, dk, dv, rev))
            return MDBX_NOTFOUND;

         mtxn->get_buf = std::move(dv);
         data->iov_base = mtxn->get_buf.data();
         data->iov_len  = mtxn->get_buf.size();
         return MDBX_SUCCESS;
      }

      // Non-DUPSORT RW path: check write buffer / write_tx first
      if (!txn->is_readonly())
      {
         if (!txn->buffer_flushed)
         {
            auto root_it = txn->write_index.find(root_idx);
            if (root_it != txn->write_index.end())
            {
               auto key_it = root_it->second.find(key_sv);
               if (key_it != root_it->second.end())
               {
                  auto& op = mtxn->write_buffer[root_idx][key_it->second];
                  if (op.op == write_op::type::remove)
                     return MDBX_NOTFOUND;
                  if (op.op == write_op::type::upsert)
                  {
                     mtxn->get_buf = op.value;
                     data->iov_base = mtxn->get_buf.data();
                     data->iov_len  = mtxn->get_buf.size();
                     return MDBX_SUCCESS;
                  }
               }
            }
         }
         else if (txn->write_tx)
         {
            auto result = txn->write_tx->get(root_idx, key_sv);
            if (!result.found)
               return MDBX_NOTFOUND;

            mtxn->get_buf.assign(result.value.data.data(), result.value.data.size());
            data->iov_base = mtxn->get_buf.data();
            data->iov_len  = mtxn->get_buf.size();
            return MDBX_SUCCESS;
         }
      }

      // Fall through: read from DWAL read session (both RO and RW unbuffered keys)
      if (!txn->read_session)
      {
         mtxn->read_session = std::make_unique<psitri::dwal::dwal_read_session>(
            txn->env->dwal_db->start_read_session());
      }

      static constexpr psitri::dwal::read_mode modes[] = {
         psitri::dwal::read_mode::buffered,
         psitri::dwal::read_mode::latest,
         psitri::dwal::read_mode::trie,
      };
      auto dwal_mode = modes[txn->env->read_mode];
      auto result = txn->read_session->get(root_idx, key_sv, dwal_mode);
      if (!result.found)
         return MDBX_NOTFOUND;

      mtxn->get_buf = std::move(result.value);
      data->iov_base = mtxn->get_buf.data();
      data->iov_len  = mtxn->get_buf.size();
      return MDBX_SUCCESS;
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

/// Flush the write buffer into a real multi-root DWAL transaction.
/// After this call, write_tx is live and all buffered ops have been replayed.
static int flush_write_buffer(MDBX_txn* txn)
{
   if (!txn || txn->is_readonly())
      return MDBX_BAD_TXN;
   if (txn->buffer_flushed)
      return MDBX_SUCCESS;
   if (txn->write_buffer.empty())
      return MDBX_SUCCESS;

   try
   {
      // Collect all touched roots
      std::vector<uint32_t> roots;
      for (auto& [root_idx, ops] : txn->write_buffer)
         roots.push_back(root_idx);

      txn->write_tx = std::make_unique<psitri::dwal::transaction>(
         *txn->env->dwal_db, roots);
      txn->write_roots = roots;

      // Replay buffered operations
      for (auto& [root_idx, ops] : txn->write_buffer)
      {
         for (auto& op : ops)
         {
            switch (op.op)
            {
               case write_op::type::upsert:
                  txn->write_tx->upsert(root_idx, op.key, op.value);
                  break;
               case write_op::type::remove:
                  txn->write_tx->remove(root_idx, op.key);
                  break;
               case write_op::type::remove_range:
                  txn->write_tx->remove_range(root_idx, op.key, op.value);
                  break;
            }
         }
      }

      txn->buffer_flushed = true;
      return MDBX_SUCCESS;
   }
   catch (...)
   {
      return MDBX_PANIC;
   }
}

/// Ensure the given root is tracked for writing. If the buffer has already
/// been flushed to a live DWAL transaction and this root wasn't included,
/// we need to flush again (abort old tx, replay with expanded root set).
static int ensure_rw_root(MDBX_txn* txn, uint32_t root_idx)
{
   if (!txn || txn->is_readonly())
      return MDBX_BAD_TXN;

   // If we haven't flushed yet, just ensure the root is in the buffer map
   if (!txn->buffer_flushed)
   {
      // Touch the root in the buffer (creates empty vector if new)
      txn->write_buffer[root_idx];
      return MDBX_SUCCESS;
   }

   // Already flushed — check if root is in the live transaction
   for (auto ri : txn->write_roots)
      if (ri == root_idx)
         return MDBX_SUCCESS;

   // Need to expand: abort current tx, add root, re-flush
   try
   {
      txn->write_tx->abort();
      txn->write_tx.reset();
      txn->buffer_flushed = false;
      txn->write_buffer[root_idx]; // ensure root present
      return flush_write_buffer(txn);
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
   bool rev    = dbi_is_reverse_dup(txn->env, dbi);

   try
   {
      // For flag checks that need reads, flush the buffer first so we can
      // use the live transaction for lookups. Flags like NOOVERWRITE, CURRENT,
      // NODUPDATA, APPEND all need to check existing data.
      bool needs_read = (flags & (MDBX_NOOVERWRITE | MDBX_NODUPDATA |
                                  MDBX_CURRENT | MDBX_APPEND | MDBX_APPENDDUP)) != 0;
      if (needs_read && !txn->buffer_flushed)
      {
         int frc = flush_write_buffer(txn);
         if (frc != MDBX_SUCCESS)
            return frc;
      }

      // Helper: check if a key prefix exists in a DUPSORT table
      auto dupsort_key_exists = [&]() -> bool {
         auto prefix = dupsort_key_prefix(key_sv);
         auto mc = txn->env->dwal_db->create_cursor(
            root_idx, psitri::dwal::read_mode::latest, /*skip_rw_lock=*/true);
         if (mc->lower_bound(prefix) && !mc->is_end())
         {
            auto found_key = mc->key();
            return found_key.size() >= prefix.size()
                && found_key.substr(0, prefix.size()) == std::string_view(prefix);
         }
         return false;
      };

      if (is_ds)
      {
         auto composite = dupsort_encode(key_sv, val_sv, rev);

         if (flags & MDBX_NODUPDATA)
         {
            auto result = txn->write_tx->get(root_idx, composite);
            if (result.found)
               return MDBX_KEYEXIST;
         }
         if (flags & MDBX_NOOVERWRITE)
         {
            if (dupsort_key_exists())
            {
               auto prefix = dupsort_key_prefix(key_sv);
               auto mc = txn->env->dwal_db->create_cursor(
                  root_idx, psitri::dwal::read_mode::latest, /*skip_rw_lock=*/true);
               if (mc->lower_bound(prefix) && !mc->is_end())
               {
                  std::string dk, dv;
                  if (dupsort_decode(mc->key(), dk, dv, rev))
                  {
                     txn->get_buf = std::move(dv);
                     data->iov_base = txn->get_buf.data();
                     data->iov_len  = txn->get_buf.size();
                  }
               }
               return MDBX_KEYEXIST;
            }
         }
         if (flags & MDBX_CURRENT)
         {
            if (!dupsort_key_exists())
               return MDBX_NOTFOUND;
         }
         if (flags & MDBX_APPEND)
         {
            auto mc = txn->env->dwal_db->create_cursor(
               root_idx, psitri::dwal::read_mode::latest, /*skip_rw_lock=*/true);
            if (mc->seek_last() && !mc->is_end())
            {
               if (composite <= std::string(mc->key()))
                  return MDBX_EKEYMISMATCH;
            }
         }

         // Always buffer for potential re-flush on root expansion
         {
            auto& ops = txn->write_buffer[root_idx];
            txn->write_index[root_idx][composite] = ops.size();
            ops.push_back({write_op::type::upsert, composite, {}});
         }
         // Also write directly to live transaction if already flushed
         if (txn->buffer_flushed)
            txn->write_tx->upsert(root_idx, composite, std::string_view{});
         ++txn->write_gen;
         return MDBX_SUCCESS;
      }

      // Non-DUPSORT path
      if (flags & MDBX_NOOVERWRITE)
      {
         auto result = txn->write_tx->get(root_idx, key_sv);
         if (result.found)
         {
            txn->get_buf.assign(result.value.data.data(), result.value.data.size());
            data->iov_base = txn->get_buf.data();
            data->iov_len  = txn->get_buf.size();
            return MDBX_KEYEXIST;
         }
      }
      if (flags & MDBX_CURRENT)
      {
         auto result = txn->write_tx->get(root_idx, key_sv);
         if (!result.found)
            return MDBX_NOTFOUND;
      }
      if (flags & MDBX_APPEND)
      {
         auto mc = txn->env->dwal_db->create_cursor(
            root_idx, psitri::dwal::read_mode::latest, /*skip_rw_lock=*/true);
         if (mc->seek_last() && !mc->is_end())
         {
            if (key_sv <= mc->key())
               return MDBX_EKEYMISMATCH;
         }
      }

      // Always buffer for potential re-flush on root expansion
      {
         auto& ops = txn->write_buffer[root_idx];
         std::string k(key_sv);
         txn->write_index[root_idx][k] = ops.size();
         ops.push_back({write_op::type::upsert, std::string(k), std::string(val_sv)});
      }
      // Also write directly to live transaction if already flushed
      if (txn->buffer_flushed)
         txn->write_tx->upsert(root_idx, key_sv, val_sv);
      ++txn->write_gen;
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
   bool rev    = dbi_is_reverse_dup(txn->env, dbi);

   try
   {
      if (is_ds)
      {
         // Flush buffer to get accurate found/not-found from DWAL
         if (!txn->buffer_flushed)
         {
            int frc = flush_write_buffer(txn);
            if (frc != MDBX_SUCCESS)
               return frc;
            if (!txn->write_tx)
            {
               txn->write_tx = std::make_unique<psitri::dwal::transaction>(
                  *txn->env->dwal_db, std::vector<uint32_t>{root_idx});
               txn->write_roots = {root_idx};
               txn->buffer_flushed = true;
            }
         }

         if (data)
         {
            auto composite = dupsort_encode(key_sv, to_sv(data), rev);
            // Buffer for potential re-flush
            txn->write_buffer[root_idx].push_back(
               {write_op::type::remove, composite, {}});
            bool removed = txn->write_tx->remove(root_idx, composite);
            ++txn->write_gen;
            return removed ? MDBX_SUCCESS : MDBX_NOTFOUND;
         }
         else
         {
            auto prefix = dupsort_key_prefix(key_sv);
            auto upper  = dupsort_key_upper(key_sv);
            // Buffer for potential re-flush
            txn->write_buffer[root_idx].push_back(
               {write_op::type::remove_range, prefix, upper});
            txn->write_tx->remove_range(root_idx, prefix, upper);
            ++txn->write_gen;
            return MDBX_SUCCESS;
         }
      }

      // Non-DUPSORT: flush buffer so we can get accurate found/not-found
      if (!txn->buffer_flushed)
      {
         int frc = flush_write_buffer(txn);
         if (frc != MDBX_SUCCESS)
            return frc;
         if (!txn->write_tx)
         {
            txn->write_tx = std::make_unique<psitri::dwal::transaction>(
               *txn->env->dwal_db, std::vector<uint32_t>{root_idx});
            txn->write_roots = {root_idx};
            txn->buffer_flushed = true;
         }
      }
      {
         // Buffer for potential re-flush
         txn->write_buffer[root_idx].push_back(
            {write_op::type::remove, std::string(key_sv), {}});
         bool removed = txn->write_tx->remove(root_idx, key_sv);
         ++txn->write_gen;
         return removed ? MDBX_SUCCESS : MDBX_NOTFOUND;
      }
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
      c->is_dupsort     = dbi_is_dupsort(txn->env, dbi);
      c->is_reverse_dup = dbi_is_reverse_dup(txn->env, dbi);

      auto mode = psitri::dwal::read_mode::latest;

      // For RW transactions, flush write buffer so cursor sees all writes
      if (!txn->is_readonly())
      {
         int rc = ensure_rw_root(txn, root_idx);
         if (rc != MDBX_SUCCESS)
         {
            delete c;
            return rc;
         }
         // Only flush if there are actual buffered operations (not just touched roots)
         if (!txn->buffer_flushed)
         {
            bool has_ops = false;
            for (auto& [ri, ops] : txn->write_buffer)
            {
               if (!ops.empty()) { has_ops = true; break; }
            }
            if (has_ops)
            {
               rc = flush_write_buffer(txn);
               if (rc != MDBX_SUCCESS)
               {
                  delete c;
                  return rc;
               }
            }
         }
      }

      // Skip the RW lock if the writer thread already holds it.
      bool writer = !txn->is_readonly() && txn->write_tx;
      auto mc = txn->env->dwal_db->create_cursor(root_idx, mode, writer);
      c->state = std::make_unique<cursor_state>(std::move(mc), c->is_dupsort, c->is_reverse_dup);
      c->cursor_gen = txn->write_gen;

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

   auto* txn_ptr = cursor->txn;
   if (txn_ptr && !txn_ptr->is_readonly() && cursor->cursor_gen != txn_ptr->write_gen)
   {
      if (!txn_ptr->buffer_flushed)
      {
         bool has_ops = false;
         for (auto& [ri, ops] : txn_ptr->write_buffer)
            if (!ops.empty()) { has_ops = true; break; }
         if (has_ops)
         {
            int rc = flush_write_buffer(txn_ptr);
            if (rc != MDBX_SUCCESS)
               return rc;
         }
      }
      uint32_t root_idx = dbi_root_index(txn_ptr->env, cursor->dbi);
      bool writer = txn_ptr->write_tx != nullptr;
      cursor->state->mc = txn_ptr->env->dwal_db->create_cursor(
         root_idx, psitri::dwal::read_mode::latest, writer);
      cursor->state->positioned = false;
      cursor->state->valid = false;
      cursor->cursor_gen = txn_ptr->write_gen;
   }

   auto& st = *cursor->state;
   auto& mc = st.mc;

   try
   {
      bool ok = false;
      switch (op)
      {
         case MDBX_FIRST:
            ok = mc->seek_begin();
            st.positioned = ok;
            break;
         case MDBX_LAST:
            ok = mc->seek_last();
            st.positioned = ok;
            break;
         case MDBX_NEXT:
            if (!st.positioned)
               ok = mc->seek_begin();
            else
               ok = mc->next();
            st.positioned = st.positioned || ok;
            break;
         case MDBX_PREV:
            if (!st.positioned)
               ok = mc->seek_last();
            else
               ok = mc->prev();
            st.positioned = st.positioned || ok;
            break;
         case MDBX_GET_CURRENT:
            ok = st.positioned && !mc->is_end() && !mc->is_rend();
            break;
         case MDBX_SET:
         case MDBX_SET_KEY:
         {
            if (!key)
               return MDBX_EINVAL;
            auto key_sv = to_sv(key);
            if (st.dupsort)
            {
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
            if (ok) st.positioned = true;
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
            if (ok) st.positioned = true;
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
            if (ok) st.positioned = true;
            break;
         }

         case MDBX_NEXT_NODUP:
            if (st.dupsort && st.valid)
            {
               auto upper = dupsort_key_upper(st.key_buf);
               ok = mc->lower_bound(upper);
            }
            else
            {
               ok = mc->next();
            }
            if (ok) st.positioned = true;
            break;

         case MDBX_PREV_NODUP:
            if (st.dupsort && st.valid)
            {
               auto prefix = dupsort_key_prefix(st.key_buf);
               ok = mc->lower_bound(prefix);
               if (ok && !mc->is_end())
                  ok = mc->prev();
            }
            else
            {
               ok = mc->prev();
            }
            if (ok) st.positioned = true;
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
               return MDBX_INCOMPATIBLE;
            auto composite = dupsort_encode(to_sv(key), to_sv(data), st.rev_dup);
            ok = mc->seek(composite);
            break;
         }
         case MDBX_GET_BOTH_RANGE:
         {
            if (!key || !data)
               return MDBX_EINVAL;
            if (!st.dupsort)
               return MDBX_INCOMPATIBLE;
            // lower_bound on composite, verify same key
            auto composite = dupsort_encode(to_sv(key), to_sv(data), st.rev_dup);
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
         MDBX_val k = {const_cast<char*>(st.key_buf.data()), st.key_buf.size()};
         MDBX_val v = {const_cast<char*>(st.val_buf.data()), st.val_buf.size()};
         return mdbx_del(cursor->txn, cursor->dbi, &k, &v);
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

int mdbx_cursor_on_first(const MDBX_cursor* cursor)
{
   if (!cursor || !cursor->state || !cursor->state->valid)
      return MDBX_NOTFOUND;
   if (cursor->state->mc->is_end())
      return MDBX_NOTFOUND;

   auto current_key = cursor->state->mc->key();
   auto root_idx    = dbi_root_index(cursor->txn->env, cursor->dbi);
   static constexpr psitri::dwal::read_mode modes[] = {
      psitri::dwal::read_mode::buffered,
      psitri::dwal::read_mode::latest,
      psitri::dwal::read_mode::trie,
   };
   auto mode = cursor->txn->is_readonly() ? modes[cursor->txn->env->read_mode]
                                           : psitri::dwal::read_mode::latest;
   auto tmp = cursor->txn->env->dwal_db->create_cursor(
       root_idx, mode, /*skip_rw_lock=*/!cursor->txn->is_readonly());
   if (tmp->seek_begin() && tmp->key() == current_key)
      return MDBX_RESULT_TRUE;
   return MDBX_RESULT_FALSE;
}

int mdbx_cursor_on_last(const MDBX_cursor* cursor)
{
   if (!cursor || !cursor->state || !cursor->state->valid)
      return MDBX_NOTFOUND;
   if (cursor->state->mc->is_end())
      return MDBX_NOTFOUND;

   auto current_key = cursor->state->mc->key();
   auto root_idx    = dbi_root_index(cursor->txn->env, cursor->dbi);
   static constexpr psitri::dwal::read_mode modes[] = {
      psitri::dwal::read_mode::buffered,
      psitri::dwal::read_mode::latest,
      psitri::dwal::read_mode::trie,
   };
   auto mode = cursor->txn->is_readonly() ? modes[cursor->txn->env->read_mode]
                                           : psitri::dwal::read_mode::latest;
   auto tmp = cursor->txn->env->dwal_db->create_cursor(
       root_idx, mode, /*skip_rw_lock=*/!cursor->txn->is_readonly());
   if (tmp->seek_last() && tmp->key() == current_key)
      return MDBX_RESULT_TRUE;
   return MDBX_RESULT_FALSE;
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
      static constexpr psitri::dwal::read_mode modes[] = {
         psitri::dwal::read_mode::buffered,
         psitri::dwal::read_mode::latest,
         psitri::dwal::read_mode::trie,
      };
      auto mode = txn->is_readonly() ? modes[txn->env->read_mode]
                                     : psitri::dwal::read_mode::latest;
      bool writer = !txn->is_readonly() && txn->write_tx;
      auto mc = txn->env->dwal_db->create_cursor(root_idx, mode, writer);
      cursor->state = std::make_unique<cursor_state>(std::move(mc), cursor->is_dupsort, cursor->is_reverse_dup);
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

// ── Cursor create/copy (unbound) ─────────────────────────────────

MDBX_cursor* mdbx_cursor_create(void* context)
{
   auto* c = new MDBX_cursor();
   c->context = context;
   return c;
}

int mdbx_cursor_copy(const MDBX_cursor* src, MDBX_cursor* dst)
{
   if (!src || !dst)
      return MDBX_EINVAL;
   if (!src->state || !src->txn)
      return MDBX_EINVAL;

   // Copy DBI/flags, then rebind to same txn
   dst->dbi            = src->dbi;
   dst->is_dupsort     = src->is_dupsort;
   dst->is_reverse_dup = src->is_reverse_dup;
   int rc = mdbx_cursor_renew(src->txn, dst);
   if (rc != MDBX_SUCCESS)
      return rc;

   // Seek to same position
   if (src->state->valid)
   {
      if (src->is_dupsort)
         dst->state->mc->seek(src->state->raw_key);
      else
         dst->state->mc->seek(src->state->key_buf);
      dst->state->sync_key_val();
      dst->state->positioned = src->state->positioned;
   }
   return MDBX_SUCCESS;
}

// ── Environment options ─────────────────────────────────────────

int mdbx_env_set_option(MDBX_env* env, MDBX_option_t option, uint64_t value)
{
   if (!env)
      return MDBX_EINVAL;
   // Most options are no-ops — psitri self-tunes
   switch (option)
   {
      case MDBX_opt_max_db:     return mdbx_env_set_maxdbs(env, static_cast<MDBX_dbi>(value));
      case MDBX_opt_max_readers: return mdbx_env_set_maxreaders(env, static_cast<unsigned>(value));
      default: break;
   }
   return MDBX_SUCCESS;
}

int mdbx_env_get_option(const MDBX_env* env, MDBX_option_t option, uint64_t* value)
{
   if (!env || !value)
      return MDBX_EINVAL;
   switch (option)
   {
      case MDBX_opt_max_db:      *value = env->max_dbs; return MDBX_SUCCESS;
      case MDBX_opt_max_readers: *value = env->max_readers; return MDBX_SUCCESS;
      default: *value = 0; return MDBX_SUCCESS;
   }
}

// ── Database copy ───────────────────────────────────────────────

int mdbx_env_copy(MDBX_env* env, const char* dest, unsigned /*flags*/)
{
   if (!env || !dest || !env->opened)
      return MDBX_EINVAL;
   try
   {
      env->dwal_db->flush_wal();
      std::filesystem::copy(env->path, dest,
         std::filesystem::copy_options::recursive | std::filesystem::copy_options::overwrite_existing);
      return MDBX_SUCCESS;
   }
   catch (...)
   {
      return MDBX_PANIC;
   }
}

// ── MDBX_EKEYMISMATCH error string ──────────────────────────────

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

   MDBX_stat env::get_stat() const
   {
      MDBX_stat s{};
      mdbx_env_stat_ex(handle_, nullptr, &s, sizeof(s));
      return s;
   }

   MDBX_envinfo env::get_info() const
   {
      MDBX_envinfo i{};
      mdbx_env_info_ex(handle_, nullptr, &i, sizeof(i));
      return i;
   }

   void env::copy(const char* dest, bool compactify, bool /*force_dynamic*/)
   {
      error::success_or_throw(mdbx_env_copy(handle_, dest, compactify ? 1 : 0));
   }

   std::filesystem::path env::get_path() const
   {
      if (!handle_)
         error::success_or_throw(MDBX_EINVAL);
      return static_cast<const MDBX_env*>(handle_)->path;
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

   bool txn::drop_map(const std::string& name, bool throw_if_absent)
   {
      MDBX_dbi dbi = 0;
      int rc = mdbx_dbi_open(handle_, name.c_str(), MDBX_DB_DEFAULTS, &dbi);
      if (rc == MDBX_NOTFOUND)
      {
         if (throw_if_absent)
            throw not_found();
         return false;
      }
      error::success_or_throw(rc);
      error::success_or_throw(mdbx_drop(handle_, dbi, 1));
      return true;
   }

   bool txn::clear_map(const std::string& name, bool throw_if_absent)
   {
      MDBX_dbi dbi = 0;
      int rc = mdbx_dbi_open(handle_, name.c_str(), MDBX_DB_DEFAULTS, &dbi);
      if (rc == MDBX_NOTFOUND)
      {
         if (throw_if_absent)
            throw not_found();
         return false;
      }
      error::success_or_throw(rc);
      error::success_or_throw(mdbx_drop(handle_, dbi, 0));
      return true;
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

   MDBX_stat txn::get_map_stat(map_handle map) const
   {
      MDBX_stat s{};
      mdbx_dbi_stat(handle_, map.dbi, &s, sizeof(s));
      return s;
   }

   map_handle::info txn::get_handle_info(map_handle map) const
   {
      map_handle::info inf{};
      inf.dbi = map.dbi;
      unsigned raw_flags = 0;
      mdbx_dbi_flags_ex(handle_, map.dbi, &raw_flags, &inf.state);
      inf.flags = static_cast<MDBX_db_flags_t>(raw_flags);
      return inf;
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
      if (data)
         d = *data;
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
      int rc = mdbx_cursor_get(handle_, &k, &d, MDBX_SET_KEY);
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

   cursor::move_result cursor::move(move_operation op, MDBX_val* key,
                                     MDBX_val* value, bool throw_notfound)
   {
      return do_get(op, key, value, throw_notfound);
   }

   cursor::move_result cursor::move(move_operation op, bool throw_notfound)
   {
      return do_get(op, nullptr, nullptr, throw_notfound);
   }

   cursor::move_result cursor::move(move_operation op, const slice& key,
                                     bool throw_notfound)
   {
      MDBX_val k = key;
      return do_get(op, &k, nullptr, throw_notfound);
   }

   cursor::move_result cursor::move(move_operation op, const slice& key,
                                     const slice& value, bool throw_notfound)
   {
      MDBX_val k = key;
      MDBX_val v = value;
      return do_get(op, &k, &v, throw_notfound);
   }

   // DUPSORT multi-value navigation
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

   cursor::move_result cursor::to_current_prev_multi(bool throw_notfound)
   {
      if (!handle_->is_dupsort)
         return do_get(MDBX_PREV, nullptr, nullptr, throw_notfound);
      return do_get(MDBX_PREV_DUP, nullptr, nullptr, throw_notfound);
   }

   cursor::move_result cursor::to_current_next_multi(bool throw_notfound)
   {
      if (!handle_->is_dupsort)
         return do_get(MDBX_NEXT, nullptr, nullptr, throw_notfound);
      return do_get(MDBX_NEXT_DUP, nullptr, nullptr, throw_notfound);
   }

   cursor::move_result cursor::to_previous_last_multi(bool throw_notfound)
   {
      return do_get(MDBX_PREV_NODUP, nullptr, nullptr, throw_notfound);
   }

   cursor::move_result cursor::to_next_first_multi(bool throw_notfound)
   {
      return do_get(MDBX_NEXT_NODUP, nullptr, nullptr, throw_notfound);
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
      static constexpr psitri::dwal::read_mode modes[] = {
         psitri::dwal::read_mode::buffered,
         psitri::dwal::read_mode::latest,
         psitri::dwal::read_mode::trie,
      };
      auto mode = handle_->txn->is_readonly() ? modes[handle_->txn->env->read_mode]
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
      static constexpr psitri::dwal::read_mode modes[] = {
         psitri::dwal::read_mode::buffered,
         psitri::dwal::read_mode::latest,
         psitri::dwal::read_mode::trie,
      };
      auto mode = handle_->txn->is_readonly() ? modes[handle_->txn->env->read_mode]
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

   void cursor::append(const slice& key, const slice& value)
   {
      MDBX_val k = key;
      MDBX_val v = value;
      error::success_or_throw(mdbx_cursor_put(handle_, &k, &v, MDBX_APPEND));
   }

   MDBX_error_t cursor::put(const slice& key, slice* value, MDBX_put_flags_t flags) noexcept
   {
      MDBX_val k = key;
      MDBX_val v;
      if (value)
         v = *value;
      else
         v = MDBX_val{nullptr, 0};
      int rc = mdbx_cursor_put(handle_, &k, &v, flags);
      if (value && rc == MDBX_SUCCESS)
         *value = slice(v.iov_base, v.iov_len);
      return static_cast<MDBX_error_t>(rc);
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

   bool cursor::erase(bool whole_multivalue)
   {
      auto flags = whole_multivalue ? MDBX_ALLDUPS : MDBX_UPSERT;
      int rc = mdbx_cursor_del(handle_, static_cast<MDBX_put_flags_t>(flags));
      if (rc == MDBX_NOTFOUND)
         return false;
      error::success_or_throw(rc);
      return true;
   }

   bool cursor::erase(const slice& key, bool whole_multivalue)
   {
      MDBX_val k = key;
      if (whole_multivalue)
      {
         int rc = mdbx_del(mdbx_cursor_txn(handle_), mdbx_cursor_dbi(handle_), &k, nullptr);
         if (rc == MDBX_NOTFOUND)
            return false;
         error::success_or_throw(rc);
         return true;
      }
      // Single dup: need the current value to delete just this one
      MDBX_val v{};
      int rc = mdbx_cursor_get(handle_, &k, &v, MDBX_SET);
      if (rc == MDBX_NOTFOUND)
         return false;
      error::success_or_throw(rc);
      rc = mdbx_del(mdbx_cursor_txn(handle_), mdbx_cursor_dbi(handle_), &k, &v);
      if (rc == MDBX_NOTFOUND)
         return false;
      error::success_or_throw(rc);
      return true;
   }

   bool cursor::erase(const slice& key, const slice& value)
   {
      MDBX_val k = key;
      MDBX_val v = value;
      int rc = mdbx_del(mdbx_cursor_txn(handle_), mdbx_cursor_dbi(handle_), &k, &v);
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
      // Native libmdbx returns slices that point into the mmapped DB and
      // remain valid for the entire transaction. Callers (e.g. Silkworm's
      // read_chain_config) save a slice from one find() and reuse it as a
      // lookup key after rebinding the cursor to a different map. Preserve
      // that contract by reusing the existing cursor object — the state
      // buffers (key_buf, val_buf) that back returned slices stay alive.
      // Fresh cursor (no prior state): use mdbx_cursor_open so it runs the
      // RW txn setup (ensure_rw_root, flush_write_buffer if dirty) which
      // makes pending writes visible to subsequent reads. The reuse path
      // below is only valid for cursors that already have state (have done
      // at least one operation) — those preserve their key_buf/val_buf so
      // slices returned from a prior find() remain valid across rebinds.
      if (!handle_ || !handle_->state)
      {
         if (handle_)
         {
            mdbx_cursor_close(handle_);
            handle_ = nullptr;
         }
         MDBX_cursor* c = nullptr;
         error::success_or_throw(mdbx_cursor_open(t, map.dbi, &c));
         handle_ = c;
         return;
      }
      MDBX_txn* txn = t;
      uint32_t root_idx = dbi_root_index(txn->env, map.dbi);
      if (root_idx == UINT32_MAX)
         error::success_or_throw(MDBX_BAD_DBI);

      // Reuse path: must still ensure the txn's write state is consistent
      // with what mdbx_cursor_open would have done. Specifically, if there
      // are buffered writes that haven't been flushed, flushing them now
      // makes them visible to reads through the cursor we're about to
      // rebind.
      if (!txn->is_readonly())
      {
         int rc = ensure_rw_root(txn, root_idx);
         if (rc != MDBX_SUCCESS)
            error::success_or_throw(rc);
         if (!txn->buffer_flushed)
         {
            bool has_ops = false;
            for (auto& [ri, ops] : txn->write_buffer)
            {
               if (!ops.empty()) { has_ops = true; break; }
            }
            if (has_ops)
            {
               rc = flush_write_buffer(txn);
               if (rc != MDBX_SUCCESS)
                  error::success_or_throw(rc);
            }
         }
      }

      handle_->txn = txn;
      handle_->dbi = map.dbi;
      handle_->is_dupsort     = dbi_is_dupsort(txn->env, map.dbi);
      handle_->is_reverse_dup = dbi_is_reverse_dup(txn->env, map.dbi);

      static constexpr psitri::dwal::read_mode modes[] = {
         psitri::dwal::read_mode::buffered,
         psitri::dwal::read_mode::latest,
         psitri::dwal::read_mode::trie,
      };
      auto mode = txn->is_readonly() ? modes[txn->env->read_mode]
                                     : psitri::dwal::read_mode::latest;
      bool writer = !txn->is_readonly() && txn->write_tx;
      auto new_mc = txn->env->dwal_db->create_cursor(root_idx, mode, writer);

      handle_->state->mc         = std::move(new_mc);
      handle_->state->dupsort    = handle_->is_dupsort;
      handle_->state->rev_dup    = handle_->is_reverse_dup;
      handle_->state->positioned = false;
      handle_->state->valid      = false;
      handle_->cursor_gen = txn->write_gen;
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
