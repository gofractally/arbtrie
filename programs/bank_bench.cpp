/**
 * Banking Transaction Benchmark
 *
 * Simulates a bank processing transfers between accounts.
 * Compares PsiTri, RocksDB (real + PsiTriRocks shim), MDBX, and TidesDB.
 *
 * Compiled into separate binaries per engine via #ifdef:
 *   BANK_ENGINE_PSITRI, BANK_ENGINE_ROCKSDB, BANK_ENGINE_MDBX, BANK_ENGINE_TIDESDB
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#if defined(BANK_ENGINE_PSITRI)
#include <psitri/cursor.hpp>
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>
#include <psitri/tree_ops.hpp>
#include <psitri/value_type.hpp>
#include <psitri/write_session_impl.hpp>
#endif

#if defined(BANK_ENGINE_ROCKSDB)
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>
#endif

#if defined(BANK_ENGINE_MDBX)
#include <mdbx.h>
#endif

#if defined(BANK_ENGINE_TIDESDB)
extern "C"
{
#include "tidesdb_c_wrapper.h"
}
#endif

namespace po = boost::program_options;

// ============================================================
// Configuration
// ============================================================

struct BankConfig
{
   uint64_t    num_accounts     = 1'000'000;
   uint64_t    num_transactions = 10'000'000;
   std::string db_path          = "/tmp/bank_bench_db";
   std::string sync_mode        = "none";
   uint64_t    seed             = 12345;
   uint64_t    initial_balance  = 1'000'000;
   uint64_t    batch_size       = 1;
   uint64_t    sync_every       = 0;  // sync every N commits (0 = never)
};

// ============================================================
// Utilities
// ============================================================

using Clock = std::chrono::steady_clock;

static std::string format_comma(uint64_t v)
{
   auto s = std::to_string(v);
   int  n = (int)s.size();
   if (n <= 3)
      return s;
   std::string result;
   result.reserve(n + (n - 1) / 3);
   for (int i = 0; i < n; ++i)
   {
      if (i > 0 && (n - i) % 3 == 0)
         result += ',';
      result += s[i];
   }
   return result;
}

static double elapsed_sec(Clock::time_point start)
{
   return std::chrono::duration<double>(Clock::now() - start).count();
}

static uint64_t measure_db_size(const std::string& path)
{
   uint64_t total = 0;
   try
   {
      auto status = std::filesystem::status(path);
      if (std::filesystem::is_regular_file(status))
      {
         // Single file (e.g. MDBX with NOSUBDIR)
         total = std::filesystem::file_size(path);
         // Also check for lock file
         if (std::filesystem::exists(path + "-lck"))
            total += std::filesystem::file_size(path + "-lck");
      }
      else if (std::filesystem::is_directory(status))
      {
         for (auto& entry : std::filesystem::recursive_directory_iterator(path))
            if (entry.is_regular_file())
               total += entry.file_size();
      }
   }
   catch (...)
   {
   }
   return total;
}

static void print_phase(const char* name, double secs, uint64_t ops = 0, const char* extra = nullptr)
{
   printf("  %-28s %8.3f sec", name, secs);
   if (ops > 0)
      printf("  %14s ops/sec", format_comma((uint64_t)(ops / secs)).c_str());
   if (extra)
      printf("  %s", extra);
   printf("\n");
}

struct SizeReport
{
   uint64_t file_size      = 0;  // raw on-disk footprint
   uint64_t live_size      = 0;  // actual data in use
   uint64_t free_size      = 0;  // reclaimable space
   uint64_t reachable_size = 0;  // bytes occupied by reachable objects (0 = not available)

   void print() const
   {
      printf("  Size: file=%.1f MB  live=%.1f MB  free=%.1f MB",
             file_size / (1024.0 * 1024.0),
             live_size / (1024.0 * 1024.0),
             free_size / (1024.0 * 1024.0));
      if (reachable_size > 0)
         printf("  reachable=%.1f MB", reachable_size / (1024.0 * 1024.0));
      printf("\n");
   }
};

// ============================================================
// Balance encoding (8-byte native uint64_t)
// ============================================================

static std::string encode_balance(uint64_t b)
{
   return std::string(reinterpret_cast<const char*>(&b), sizeof(b));
}

static uint64_t decode_balance(const char* data, size_t len)
{
   uint64_t b = 0;
   std::memcpy(&b, data, std::min(len, sizeof(b)));
   return b;
}

static uint64_t decode_balance(const std::string& s)
{
   return decode_balance(s.data(), s.size());
}

static uint64_t to_big_endian(uint64_t v)
{
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
   return __builtin_bswap64(v);
#else
   return v;
#endif
}

// ============================================================
// Transaction log key/value encoding
// ============================================================

// Log key: "\x00TX" prefix + 8-byte big-endian sequence number
// The NUL byte prefix ensures no collision with dictionary words (all printable)
// or binary account keys (which are 8 bytes, not 11).
static const std::string LOG_PREFIX = std::string("\x00TX", 3);

static std::string encode_log_key(uint64_t seq)
{
   std::string key;
   key.reserve(11);
   key.append(LOG_PREFIX);
   uint64_t be = to_big_endian(seq);
   key.append(reinterpret_cast<const char*>(&be), sizeof(be));
   return key;
}

// Log value: src_len(2 bytes) + src + dst_len(2 bytes) + dst + amount(8 bytes)
static std::string encode_log_value(const std::string& src,
                                    const std::string& dst,
                                    uint64_t           amount)
{
   std::string val;
   val.reserve(2 + src.size() + 2 + dst.size() + 8);
   uint16_t slen = (uint16_t)src.size();
   uint16_t dlen = (uint16_t)dst.size();
   val.append(reinterpret_cast<const char*>(&slen), 2);
   val.append(src);
   val.append(reinterpret_cast<const char*>(&dlen), 2);
   val.append(dst);
   val.append(reinterpret_cast<const char*>(&amount), 8);
   return val;
}

// ============================================================
// Account name generation
// ============================================================

static std::vector<std::string> generate_account_names(uint64_t num_accounts, uint64_t seed)
{
   std::vector<std::string> names;
   names.reserve(num_accounts);

   // Read dictionary words
   std::ifstream dict("/usr/share/dict/words");
   if (dict.is_open())
   {
      std::string word;
      while (std::getline(dict, word) && names.size() < num_accounts)
         if (!word.empty())
            names.push_back(std::move(word));
   }
   printf("  Loaded %s dictionary words\n", format_comma(names.size()).c_str());

   // Generate remaining names in round-robin: big-endian, little-endian, decimal
   uint64_t counter = 0;
   while (names.size() < num_accounts)
   {
      int      type = counter % 3;
      uint64_t idx  = counter / 3 + 1;  // +1 to avoid BE/LE collision at 0
      switch (type)
      {
         case 0:
         {  // big-endian 8-byte binary
            uint64_t be = to_big_endian(idx);
            names.push_back(std::string(reinterpret_cast<const char*>(&be), 8));
            break;
         }
         case 1:
         {  // little-endian 8-byte binary
            names.push_back(std::string(reinterpret_cast<const char*>(&idx), 8));
            break;
         }
         case 2:
         {  // zero-padded 8-digit decimal
            char buf[16];
            snprintf(buf, sizeof(buf), "%08lu", (unsigned long)idx);
            names.push_back(buf);
            break;
         }
      }
      ++counter;
   }

   // Deterministic shuffle
   std::mt19937_64 rng(seed);
   std::shuffle(names.begin(), names.end(), rng);

   return names;
}

// ============================================================
// Account selection (triangular distribution)
// ============================================================

static uint64_t pick_account(std::mt19937_64& rng, uint64_t num_accounts)
{
   std::uniform_int_distribution<uint64_t> d(0, num_accounts / 2 - 1);
   return d(rng) + d(rng);
}

// ============================================================
// BankEngine interface
// ============================================================

class BankEngine
{
  public:
   virtual ~BankEngine() = default;

   virtual const char* name() const                                              = 0;
   virtual void open(const std::string& path, const std::string& sync_mode)      = 0;
   virtual void close()                                                          = 0;
   virtual void bulk_load(const std::vector<std::string>& accounts, uint64_t balance) = 0;

   /// Begin a batch transaction. All transfer() calls until commit_batch() are
   /// part of a single atomic commit.
   virtual void begin_batch() = 0;

   /// Attempt a single transfer within the current batch. Returns true if the
   /// transfer succeeded (sufficient balance), false if skipped. Reads within
   /// the batch must see writes from prior transfers in the same batch.
   /// On success, also inserts a transaction log entry keyed by big-endian seq.
   virtual bool transfer(const std::string& src,
                         const std::string& dst,
                         uint64_t           amount,
                         uint64_t           seq) = 0;

   /// Commit the current batch transaction.
   virtual void commit_batch() = 0;

   /// Flush/sync data to disk. Called periodically (every N commits) equally
   /// across all engines to ensure fair comparison.
   virtual void sync() = 0;

   /// Report detailed size breakdown: file, live, and free/reclaimable.
   virtual SizeReport report_size(const std::string& db_path) = 0;

   virtual uint64_t scan_all(
       std::function<void(const std::string& key, uint64_t bal)> visitor) = 0;
};

// ============================================================
// PsiTri Engine
// ============================================================

#if defined(BANK_ENGINE_PSITRI)
class PsiTriEngine : public BankEngine
{
   std::shared_ptr<psitri::database>      _db;
   std::shared_ptr<psitri::write_session> _ses;
   std::optional<psitri::transaction>     _tx;

  public:
   const char* name() const override { return "PsiTri"; }

   void open(const std::string& path, const std::string& sync_mode) override
   {
      std::filesystem::remove_all(path);
      psitri::runtime_config cfg;
      if (sync_mode == "async")
         cfg.sync_mode = sal::sync_type::msync_async;
      else if (sync_mode == "sync")
         cfg.sync_mode = sal::sync_type::msync_sync;
      else
         cfg.sync_mode = sal::sync_type::none;
      _db  = psitri::database::create(path, cfg);
      _ses = _db->start_write_session();
   }

   void close() override
   {
      _tx.reset();
      _ses.reset();
      _db->compact_and_truncate();
      _db.reset();
   }

   void bulk_load(const std::vector<std::string>& accounts, uint64_t balance) override
   {
      auto bal = encode_balance(balance);
      auto tx  = _ses->start_transaction(0);
      for (auto& acct : accounts)
      {
         tx.upsert(psitri::to_key_view(acct), psitri::to_value_view(bal));
      }
      tx.commit();
   }

   void begin_batch() override
   {
      _tx.emplace(_ses->start_transaction(0));
   }

   bool transfer(const std::string& src,
                 const std::string& dst,
                 uint64_t           amount,
                 uint64_t           seq) override
   {
      auto src_val = _tx->get<std::string>(psitri::to_key_view(src));
      if (!src_val)
         return false;
      uint64_t src_bal = decode_balance(*src_val);
      if (src_bal < amount)
         return false;

      auto dst_val = _tx->get<std::string>(psitri::to_key_view(dst));
      if (!dst_val)
         return false;
      uint64_t dst_bal = decode_balance(*dst_val);

      auto new_src = encode_balance(src_bal - amount);
      auto new_dst = encode_balance(dst_bal + amount);
      _tx->upsert(psitri::to_key_view(src), psitri::to_value_view(new_src));
      _tx->upsert(psitri::to_key_view(dst), psitri::to_value_view(new_dst));

      // Transaction log entry
      auto log_key = encode_log_key(seq);
      auto log_val = encode_log_value(src, dst, amount);
      _tx->upsert(psitri::to_key_view(log_key), psitri::to_value_view(log_val));
      return true;
   }

   void commit_batch() override
   {
      _tx->commit();
      _tx.reset();
   }

   void sync() override { _db->sync(); }

   SizeReport report_size(const std::string&) override
   {
      auto d = _db->dump();
      SizeReport r;
      r.file_size = d.total_segments * sal::segment_size;
      // Live = allocated minus freed across all non-free segments
      uint64_t live = 0;
      for (auto& seg : d.segments)
         if (!seg.is_free && seg.alloc_pos > 0)
            live += seg.alloc_pos - seg.freed_bytes;
      r.live_size = live;
      r.free_size = r.file_size - live;
      // Walk reachable objects from roots to get true data footprint
      r.reachable_size = _db->reachable_size();
      return r;
   }

   uint64_t scan_all(std::function<void(const std::string&, uint64_t)> visitor) override
   {
      auto     tx  = _ses->start_transaction(0);
      auto     cur = tx.read_cursor();
      uint64_t count = 0;
      cur.seek_begin();
      while (!cur.is_end())
      {
         auto val = cur.value<std::string>();
         if (val)
         {
            auto k = cur.key();
            visitor(std::string(k.data(), k.size()), decode_balance(*val));
            ++count;
         }
         cur.next();
      }
      return count;
   }
};
#endif

// ============================================================
// RocksDB Engine (works with both PsiTriRocks shim and real RocksDB)
// ============================================================

#if defined(BANK_ENGINE_ROCKSDB)
class RocksDbEngine : public BankEngine
{
   rocksdb::DB*           _db = nullptr;
   rocksdb::WriteOptions  _wo;
   rocksdb::ReadOptions   _ro;

   // Batch state: pending writes buffered in a map so reads see prior writes
   rocksdb::WriteBatch                          _batch;
   std::unordered_map<std::string, std::string> _pending;

  public:
   const char* name() const override
   {
#if defined(PSITRIROCKS_BACKEND)
      return "PsiTriRocks";
#else
      return "RocksDB";
#endif
   }

   void open(const std::string& path, const std::string& sync_mode) override
   {
      rocksdb::DestroyDB(path, {});
      rocksdb::Options options;
      options.create_if_missing = true;
      auto s = rocksdb::DB::Open(options, path, &_db);
      if (!s.ok())
      {
         fprintf(stderr, "RocksDB open failed: %s\n", s.ToString().c_str());
         std::exit(1);
      }
      _wo.sync = (sync_mode == "sync");
   }

   void close() override
   {
      delete _db;
      _db = nullptr;
   }

   void bulk_load(const std::vector<std::string>& accounts, uint64_t balance) override
   {
      auto bal = encode_balance(balance);
      for (size_t i = 0; i < accounts.size(); i += 10000)
      {
         rocksdb::WriteBatch batch;
         size_t              end = std::min(i + 10000, accounts.size());
         for (size_t j = i; j < end; ++j)
            batch.Put(accounts[j], bal);
         _db->Write(_wo, &batch);
      }
   }

   void begin_batch() override
   {
      _batch.Clear();
      _pending.clear();
   }

   bool transfer(const std::string& src,
                 const std::string& dst,
                 uint64_t           amount,
                 uint64_t           seq) override
   {
      // Read source — check pending writes first, then DB
      std::string src_val;
      auto        it = _pending.find(src);
      if (it != _pending.end())
         src_val = it->second;
      else if (!_db->Get(_ro, src, &src_val).ok())
         return false;

      uint64_t src_bal = decode_balance(src_val);
      if (src_bal < amount)
         return false;

      // Read dest
      std::string dst_val;
      it = _pending.find(dst);
      if (it != _pending.end())
         dst_val = it->second;
      else if (!_db->Get(_ro, dst, &dst_val).ok())
         return false;

      uint64_t dst_bal = decode_balance(dst_val);

      auto new_src = encode_balance(src_bal - amount);
      auto new_dst = encode_balance(dst_bal + amount);
      _batch.Put(src, new_src);
      _batch.Put(dst, new_dst);
      _pending[src] = std::move(new_src);
      _pending[dst] = std::move(new_dst);

      // Transaction log entry
      _batch.Put(encode_log_key(seq), encode_log_value(src, dst, amount));
      return true;
   }

   void commit_batch() override
   {
      _db->Write(_wo, &_batch);
      _batch.Clear();
      _pending.clear();
   }

   void sync() override { _db->Flush(rocksdb::FlushOptions()); }

   SizeReport report_size(const std::string& db_path) override
   {
      SizeReport r;
      r.file_size = measure_db_size(db_path);
      std::string val;
      if (_db->GetProperty("rocksdb.live-sst-files-size", &val))
         r.live_size = std::stoull(val);
      else
         r.live_size = r.file_size;
      r.free_size = r.file_size > r.live_size ? r.file_size - r.live_size : 0;
      // PsiTriRocks exposes reachable object size
      if (_db->GetProperty("psitri.reachable-size", &val))
         r.reachable_size = std::stoull(val);
      return r;
   }

   uint64_t scan_all(std::function<void(const std::string&, uint64_t)> visitor) override
   {
      uint64_t count = 0;
      auto*    iter  = _db->NewIterator(_ro);
      for (iter->SeekToFirst(); iter->Valid(); iter->Next())
      {
         auto k = iter->key();
         auto v = iter->value();
         visitor(k.ToString(), decode_balance(v.data(), v.size()));
         ++count;
      }
      delete iter;
      return count;
   }
};
#endif

// ============================================================
// MDBX Engine
// ============================================================

#if defined(BANK_ENGINE_MDBX)

#define MDBX_CHECK(expr)                                                                  \
   do                                                                                     \
   {                                                                                      \
      int rc_ = (expr);                                                                   \
      if (rc_ != MDBX_SUCCESS)                                                            \
      {                                                                                   \
         fprintf(stderr, "mdbx error %d (%s) at %s:%d: %s\n", rc_, mdbx_strerror(rc_),   \
                 __FILE__, __LINE__, #expr);                                               \
         std::exit(1);                                                                    \
      }                                                                                   \
   } while (0)

class MdbxEngine : public BankEngine
{
   MDBX_env* _env = nullptr;
   MDBX_dbi  _dbi = 0;
   MDBX_txn* _txn = nullptr;  // current batch transaction

  public:
   const char* name() const override { return "MDBX"; }

   void open(const std::string& path, const std::string& sync_mode) override
   {
      std::filesystem::remove_all(path);
      MDBX_CHECK(mdbx_env_create(&_env));
      MDBX_CHECK(mdbx_env_set_geometry(_env, 1024 * 1024,       // lower
                                        1024 * 1024,              // initial
                                        64ULL * 1024 * 1024 * 1024,  // upper (64 GB)
                                        -1, -1, -1));
      MDBX_CHECK(mdbx_env_set_maxreaders(_env, 4));

      unsigned sync_flags = 0;
      if (sync_mode == "none")
         sync_flags = MDBX_SAFE_NOSYNC;
      else if (sync_mode == "async")
         sync_flags = MDBX_SAFE_NOSYNC;
      else
         sync_flags = MDBX_SYNC_DURABLE;

      unsigned flags = sync_flags | MDBX_LIFORECLAIM | MDBX_NOSUBDIR;
      MDBX_CHECK(mdbx_env_open(_env, path.c_str(), (MDBX_env_flags_t)flags, 0664));

      MDBX_txn* txn = nullptr;
      MDBX_CHECK(mdbx_txn_begin(_env, nullptr, (MDBX_txn_flags_t)0, &txn));
      MDBX_CHECK(mdbx_dbi_open(txn, nullptr, MDBX_CREATE, &_dbi));
      MDBX_CHECK(mdbx_txn_commit(txn));
   }

   void close() override
   {
      if (_env)
      {
         mdbx_dbi_close(_env, _dbi);
         mdbx_env_close(_env);
         _env = nullptr;
      }
   }

   void bulk_load(const std::vector<std::string>& accounts, uint64_t balance) override
   {
      MDBX_txn* txn = nullptr;
      MDBX_CHECK(mdbx_txn_begin(_env, nullptr, (MDBX_txn_flags_t)0, &txn));
      for (auto& acct : accounts)
      {
         MDBX_val k{const_cast<char*>(acct.data()), acct.size()};
         MDBX_val v{&balance, sizeof(balance)};
         MDBX_CHECK(mdbx_put(txn, _dbi, &k, &v, MDBX_UPSERT));
      }
      MDBX_CHECK(mdbx_txn_commit(txn));
   }

   void begin_batch() override
   {
      MDBX_CHECK(mdbx_txn_begin(_env, nullptr, (MDBX_txn_flags_t)0, &_txn));
   }

   bool transfer(const std::string& src,
                 const std::string& dst,
                 uint64_t           amount,
                 uint64_t           seq) override
   {
      MDBX_val sk{const_cast<char*>(src.data()), src.size()};
      MDBX_val sv;
      if (mdbx_get(_txn, _dbi, &sk, &sv) != MDBX_SUCCESS)
         return false;
      uint64_t src_bal;
      std::memcpy(&src_bal, sv.iov_base, sizeof(src_bal));

      if (src_bal < amount)
         return false;

      MDBX_val dk{const_cast<char*>(dst.data()), dst.size()};
      MDBX_val dv;
      if (mdbx_get(_txn, _dbi, &dk, &dv) != MDBX_SUCCESS)
         return false;
      uint64_t dst_bal;
      std::memcpy(&dst_bal, dv.iov_base, sizeof(dst_bal));

      uint64_t new_src = src_bal - amount;
      uint64_t new_dst = dst_bal + amount;
      MDBX_val nsv{&new_src, sizeof(new_src)};
      MDBX_val ndv{&new_dst, sizeof(new_dst)};
      MDBX_CHECK(mdbx_put(_txn, _dbi, &sk, &nsv, MDBX_UPSERT));
      MDBX_CHECK(mdbx_put(_txn, _dbi, &dk, &ndv, MDBX_UPSERT));

      // Transaction log entry
      auto     lk = encode_log_key(seq);
      auto     lv = encode_log_value(src, dst, amount);
      MDBX_val lkv{lk.data(), lk.size()};
      MDBX_val lvv{lv.data(), lv.size()};
      MDBX_CHECK(mdbx_put(_txn, _dbi, &lkv, &lvv, MDBX_UPSERT));
      return true;
   }

   void commit_batch() override
   {
      MDBX_CHECK(mdbx_txn_commit(_txn));
      _txn = nullptr;
   }

   void sync() override { mdbx_env_sync_ex(_env, true, false); }

   SizeReport report_size(const std::string&) override
   {
      SizeReport r;
      MDBX_envinfo info;
      MDBX_stat    stat;
      MDBX_CHECK(mdbx_env_info_ex(_env, nullptr, &info, sizeof(info)));
      MDBX_CHECK(mdbx_env_stat_ex(_env, nullptr, &stat, sizeof(stat)));
      r.file_size = info.mi_geo.current;
      uint64_t live_pages = stat.ms_branch_pages + stat.ms_leaf_pages + stat.ms_overflow_pages;
      r.live_size = live_pages * stat.ms_psize;
      r.free_size = r.file_size > r.live_size ? r.file_size - r.live_size : 0;
      return r;
   }

   uint64_t scan_all(std::function<void(const std::string&, uint64_t)> visitor) override
   {
      MDBX_txn* txn = nullptr;
      MDBX_CHECK(mdbx_txn_begin(_env, nullptr, (MDBX_txn_flags_t)MDBX_TXN_RDONLY, &txn));
      MDBX_cursor* cur = nullptr;
      MDBX_CHECK(mdbx_cursor_open(txn, _dbi, &cur));

      MDBX_val k, v;
      uint64_t count = 0;
      int      rc    = mdbx_cursor_get(cur, &k, &v, MDBX_FIRST);
      while (rc == MDBX_SUCCESS)
      {
         uint64_t bal;
         std::memcpy(&bal, v.iov_base, sizeof(bal));
         visitor(std::string((const char*)k.iov_base, k.iov_len), bal);
         ++count;
         rc = mdbx_cursor_get(cur, &k, &v, MDBX_NEXT);
      }
      mdbx_cursor_close(cur);
      mdbx_txn_abort(txn);
      return count;
   }
};
#endif

// ============================================================
// TidesDB Engine
// ============================================================

#if defined(BANK_ENGINE_TIDESDB)
class TidesDbEngine : public BankEngine
{
   tdb_wrapper_t*                  _db = nullptr;
   static constexpr const char*    CF  = "bank";
   const std::vector<std::string>* _account_names = nullptr;
   tdb_wrapper_txn_t*              _txn = nullptr;

  public:
   void set_account_names(const std::vector<std::string>* names) { _account_names = names; }

   const char* name() const override { return "TidesDB"; }

   void open(const std::string& path, const std::string& sync_mode) override
   {
      std::filesystem::remove_all(path);
      int sm = TDB_WRAP_SYNC_NONE;
      if (sync_mode == "async")
         sm = TDB_WRAP_SYNC_SAFE;
      else if (sync_mode == "sync")
         sm = TDB_WRAP_SYNC_FULL;
      _db = tdb_open(path.c_str(), sm);
      if (!_db)
      {
         fprintf(stderr, "TidesDB open failed\n");
         std::exit(1);
      }
      tdb_ensure_cf(_db, CF);
   }

   void close() override
   {
      if (_db)
      {
         tdb_close(_db);
         _db = nullptr;
      }
   }

   void bulk_load(const std::vector<std::string>& accounts, uint64_t balance) override
   {
      // TidesDB limits transactions to 100,000 ops; chunk accordingly
      constexpr size_t chunk = 90'000;
      for (size_t i = 0; i < accounts.size(); i += chunk)
      {
         auto* txn  = tdb_txn_begin(_db);
         size_t end = std::min(i + chunk, accounts.size());
         for (size_t j = i; j < end; ++j)
         {
            tdb_put(txn, CF, (const uint8_t*)accounts[j].data(), accounts[j].size(),
                    (const uint8_t*)&balance, sizeof(balance));
         }
         tdb_txn_commit(txn);
         tdb_txn_free(txn);
      }
   }

   void begin_batch() override
   {
      _txn = tdb_txn_begin(_db);
   }

   bool transfer(const std::string& src,
                 const std::string& dst,
                 uint64_t           amount,
                 uint64_t           seq) override
   {
      uint8_t* sv   = nullptr;
      size_t   slen = 0;
      if (tdb_get(_txn, CF, (const uint8_t*)src.data(), src.size(), &sv, &slen) !=
          TDB_WRAP_SUCCESS)
         return false;
      uint64_t src_bal;
      std::memcpy(&src_bal, sv, sizeof(src_bal));
      free(sv);

      if (src_bal < amount)
         return false;

      uint8_t* dv   = nullptr;
      size_t   dlen = 0;
      if (tdb_get(_txn, CF, (const uint8_t*)dst.data(), dst.size(), &dv, &dlen) !=
          TDB_WRAP_SUCCESS)
         return false;
      uint64_t dst_bal;
      std::memcpy(&dst_bal, dv, sizeof(dst_bal));
      free(dv);

      uint64_t new_src = src_bal - amount;
      uint64_t new_dst = dst_bal + amount;
      tdb_put(_txn, CF, (const uint8_t*)src.data(), src.size(),
              (const uint8_t*)&new_src, sizeof(new_src));
      tdb_put(_txn, CF, (const uint8_t*)dst.data(), dst.size(),
              (const uint8_t*)&new_dst, sizeof(new_dst));

      // Transaction log entry
      auto lk = encode_log_key(seq);
      auto lv = encode_log_value(src, dst, amount);
      tdb_put(_txn, CF, (const uint8_t*)lk.data(), lk.size(),
              (const uint8_t*)lv.data(), lv.size());
      return true;
   }

   void commit_batch() override
   {
      tdb_txn_commit(_txn);
      tdb_txn_free(_txn);
      _txn = nullptr;
   }

   void sync() override
   {
      // TidesDB doesn't expose a standalone sync API; commits are durable
   }

   SizeReport report_size(const std::string& db_path) override
   {
      SizeReport r;
      r.file_size = measure_db_size(db_path);
      r.live_size = r.file_size;  // TidesDB doesn't expose detailed stats
      return r;
   }

   uint64_t scan_all(std::function<void(const std::string&, uint64_t)> visitor) override
   {
      // TidesDB C wrapper iterator doesn't expose key/value accessors,
      // so we do point lookups on all known account names.
      if (!_account_names)
         return 0;
      uint64_t count = 0;
      auto*    txn   = tdb_txn_begin(_db);
      for (auto& acct : *_account_names)
      {
         uint8_t* val  = nullptr;
         size_t   vlen = 0;
         if (tdb_get(txn, CF, (const uint8_t*)acct.data(), acct.size(), &val, &vlen) ==
             TDB_WRAP_SUCCESS)
         {
            uint64_t bal;
            std::memcpy(&bal, val, sizeof(bal));
            free(val);
            visitor(acct, bal);
            ++count;
         }
      }
      // Also scan transaction log entries by probing sequential keys
      for (uint64_t seq = 1; ; ++seq)
      {
         auto     lk  = encode_log_key(seq);
         uint8_t* val = nullptr;
         size_t   vlen = 0;
         if (tdb_get(txn, CF, (const uint8_t*)lk.data(), lk.size(), &val, &vlen) !=
             TDB_WRAP_SUCCESS)
            break;
         free(val);
         visitor(lk, 0);  // balance field unused for log entries
         ++count;
      }
      tdb_txn_free(txn);
      return count;
   }
};
#endif

// ============================================================
// Main
// ============================================================

int main(int argc, char** argv)
{
   BankConfig cfg;

   po::options_description desc("Bank Benchmark Options");
   auto                    opt = desc.add_options();
   opt("num-accounts", po::value(&cfg.num_accounts)->default_value(1'000'000),
       "Number of accounts");
   opt("num-transactions", po::value(&cfg.num_transactions)->default_value(10'000'000),
       "Number of transfer transactions");
   opt("db-path", po::value(&cfg.db_path)->default_value("/tmp/bank_bench_db"),
       "Database directory path");
   opt("sync-mode", po::value(&cfg.sync_mode)->default_value("none"),
       "Sync mode: none, async, sync");
   opt("seed", po::value(&cfg.seed)->default_value(12345), "RNG seed");
   opt("initial-balance", po::value(&cfg.initial_balance)->default_value(1'000'000),
       "Initial balance per account");
   opt("batch-size", po::value(&cfg.batch_size)->default_value(1),
       "Number of transfers per commit batch");
   opt("sync-every", po::value(&cfg.sync_every)->default_value(0),
       "Sync to disk every N batch commits (0 = no periodic sync)");
   opt("help,h", "Show help");

   po::variables_map vm;
   po::store(po::parse_command_line(argc, argv, desc), vm);
   po::notify(vm);

   if (vm.count("help"))
   {
      std::cout << desc << "\n";
      return 0;
   }

   // Create engine
   std::unique_ptr<BankEngine> engine;
#if defined(BANK_ENGINE_PSITRI)
   engine = std::make_unique<PsiTriEngine>();
#elif defined(BANK_ENGINE_ROCKSDB)
   engine = std::make_unique<RocksDbEngine>();
#elif defined(BANK_ENGINE_MDBX)
   engine = std::make_unique<MdbxEngine>();
#elif defined(BANK_ENGINE_TIDESDB)
   engine = std::make_unique<TidesDbEngine>();
#else
#error "No BANK_ENGINE_* defined"
#endif

   printf("=== Bank Benchmark: %s ===\n", engine->name());
   printf("  Accounts:     %s\n", format_comma(cfg.num_accounts).c_str());
   printf("  Transactions: %s\n", format_comma(cfg.num_transactions).c_str());
   printf("  Batch size:   %s\n", format_comma(cfg.batch_size).c_str());
   printf("  Sync every:   %s commits\n",
          cfg.sync_every > 0 ? format_comma(cfg.sync_every).c_str() : "never");
   printf("  Sync mode:    %s\n", cfg.sync_mode.c_str());
   printf("  Seed:         %llu\n", (unsigned long long)cfg.seed);
   printf("  DB path:      %s\n", cfg.db_path.c_str());
   printf("\n");

   auto wall_start = Clock::now();

   // ── Phase 1: Account Generation ──
   printf("Phase 1: Account Generation\n");
   auto p1_start = Clock::now();
   auto accounts = generate_account_names(cfg.num_accounts, cfg.seed);
   print_phase("Account generation", elapsed_sec(p1_start), cfg.num_accounts);

#if defined(BANK_ENGINE_TIDESDB)
   static_cast<TidesDbEngine*>(engine.get())->set_account_names(&accounts);
#endif

   // ── Phase 2: Bulk Load ──
   printf("\nPhase 2: Bulk Load\n");
   engine->open(cfg.db_path, cfg.sync_mode);
   auto p2_start = Clock::now();
   engine->bulk_load(accounts, cfg.initial_balance);
   auto p2_secs = elapsed_sec(p2_start);
   print_phase("Bulk load", p2_secs, cfg.num_accounts);
   engine->report_size(cfg.db_path).print();

   // ── Phase 3: Transaction Processing ──
   printf("\nPhase 3: Transaction Processing\n");
   std::mt19937_64 rng(cfg.seed);
   std::uniform_int_distribution<uint64_t> amt_dist(1, cfg.initial_balance);

   uint64_t successful     = 0;
   uint64_t skipped        = 0;
   uint64_t batch_counter  = 0;
   uint64_t commit_counter = 0;
   auto     p3_start       = Clock::now();

   engine->begin_batch();
   for (uint64_t i = 0; i < cfg.num_transactions; ++i)
   {
      uint64_t src_idx = pick_account(rng, cfg.num_accounts);
      uint64_t dst_idx = pick_account(rng, cfg.num_accounts);
      uint64_t amount  = amt_dist(rng);

      // Deterministic fallback if same account
      if (dst_idx == src_idx)
         dst_idx = (src_idx + 1) % cfg.num_accounts;

      if (engine->transfer(accounts[src_idx], accounts[dst_idx], amount, successful + 1))
         ++successful;
      else
         ++skipped;

      if (++batch_counter >= cfg.batch_size)
      {
         engine->commit_batch();
         batch_counter = 0;
         ++commit_counter;

         // Periodic sync — applied equally to all engines
         if (cfg.sync_every > 0 && commit_counter % cfg.sync_every == 0)
            engine->sync();
         if (i + 1 < cfg.num_transactions)
            engine->begin_batch();
      }

      if ((i + 1) % 1'000'000 == 0)
      {
         double secs = elapsed_sec(p3_start);
         printf("  %s / %s  (%s tx/sec, %s ok, %s skip)\n",
                format_comma(i + 1).c_str(),
                format_comma(cfg.num_transactions).c_str(),
                format_comma((uint64_t)((i + 1) / secs)).c_str(),
                format_comma(successful).c_str(),
                format_comma(skipped).c_str());
      }
   }
   // Commit any remaining transfers in the last partial batch
   if (batch_counter > 0)
      engine->commit_batch();

   auto p3_secs = elapsed_sec(p3_start);
   print_phase("Transactions", p3_secs, cfg.num_transactions);
   printf("  Successful: %s  Skipped: %s\n",
          format_comma(successful).c_str(), format_comma(skipped).c_str());
   engine->report_size(cfg.db_path).print();

   // ── Phase 4: Validation ──
   printf("\nPhase 4: Validation\n");
   auto     p4_start      = Clock::now();
   uint64_t total_balance  = 0;
   uint64_t account_count  = 0;
   uint64_t log_count      = 0;
   engine->scan_all([&](const std::string& key, uint64_t bal)
   {
      // Log entries start with "\x00TX" prefix and are 11 bytes (3 + 8)
      if (key.size() == 11 && key.substr(0, 3) == LOG_PREFIX)
         ++log_count;
      else
      {
         total_balance += bal;
         ++account_count;
      }
   });
   auto p4_secs = elapsed_sec(p4_start);

   uint64_t expected = cfg.initial_balance * cfg.num_accounts;
   bool     pass     = (total_balance == expected && account_count == cfg.num_accounts
                        && log_count == successful);

   print_phase("Validation scan", p4_secs, account_count + log_count);
   printf("  Accounts found: %s (expected %s)\n",
          format_comma(account_count).c_str(), format_comma(cfg.num_accounts).c_str());
   printf("  Log entries:    %s (expected %s)\n",
          format_comma(log_count).c_str(), format_comma(successful).c_str());
   printf("  Total balance:  %s (expected %s)  %s\n",
          format_comma(total_balance).c_str(), format_comma(expected).c_str(),
          pass ? "PASS" : "*** FAIL ***");
   engine->report_size(cfg.db_path).print();

   // ── Cleanup ──
   engine->close();

   printf("\n%s  Total wall clock: %.3f sec\n",
          pass ? "PASS" : "FAIL", elapsed_sec(wall_start));

   return pass ? 0 : 1;
}
