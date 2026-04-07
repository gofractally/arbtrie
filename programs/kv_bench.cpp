/**
 * Multi-Engine KV Benchmark
 *
 * Replicates the psitri_bench test suite across PsiTri, RocksDB, MDBX, and TidesDB.
 * Tests: insert, upsert, get, get-rand, lower-bound, iterate, remove, remove-rand,
 *        multiwriter, multithread-rw.
 *
 * Compiled into separate binaries per engine via #ifdef:
 *   KV_ENGINE_PSITRI, KV_ENGINE_ROCKSDB, KV_ENGINE_MDBX, KV_ENGINE_TIDESDB
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include <hash/xxhash.h>

#if defined(KV_ENGINE_PSITRI)
#include <psitri/cursor.hpp>
#include <psitri/database.hpp>
#include <psitri/read_session.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/tree_ops.hpp>
#include <psitri/write_session_impl.hpp>
#endif

#if defined(KV_ENGINE_DWAL)
#include <psitri/database.hpp>
#include <psitri/dwal/dwal_database.hpp>
#include <psitri/dwal/dwal_transaction.hpp>
#include <psitri/dwal/merge_cursor.hpp>
#endif

#if defined(KV_ENGINE_ROCKSDB)
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>
#endif

#if defined(KV_ENGINE_MDBX)
#include <mdbx.h>
#endif

#if defined(KV_ENGINE_TIDESDB)
extern "C"
{
#include "tidesdb_c_wrapper.h"
}
#endif

namespace po = boost::program_options;

// ============================================================
// Configuration
// ============================================================

struct benchmark_config
{
   uint32_t    rounds     = 3;
   uint32_t    items      = 1'000'000;
   uint32_t    batch_size = 512;
   uint32_t    value_size = 8;
   uint32_t    threads    = 4;
   std::string sync_mode  = "none";
   bool        checksum   = false;  // PsiTri: checksum_on_commit (compactor checksums independently)
   bool        wp_commit  = true;   // PsiTri: write_protect_on_commit
};

// ============================================================
// Utilities
// ============================================================

static int64_t rand_from_seq(uint64_t seq)
{
   return XXH3_64bits((char*)&seq, sizeof(seq));
}

static uint64_t to_big_endian(uint64_t x)
{
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
   return __builtin_bswap64(x);
#else
   return x;
#endif
}

static void to_key(uint64_t val, std::vector<char>& v)
{
   v.resize(sizeof(val));
   memcpy(v.data(), &val, sizeof(val));
}

static void to_key(const std::string& val, std::vector<char>& v)
{
   v.resize(val.size());
   memcpy(v.data(), val.data(), val.size());
}

static std::string format_comma(uint64_t s)
{
   if (s < 1000)
      return std::to_string(s);
   if (s < 1000000)
      return std::to_string(s / 1000) + ',' + std::to_string((s % 1000) + 1000).substr(1);
   if (s < 1000000000)
      return std::to_string(s / 1000000) + ',' +
             std::to_string(((s % 1000000) / 1000) + 1000).substr(1) + "," +
             std::to_string((s % 1000) + 1000).substr(1);
   return std::to_string(s / 1000000000) + ',' +
          std::to_string(((s % 1000000000) / 1000000) + 1000).substr(1) + "," +
          std::to_string(((s % 1000000) / 1000) + 1000).substr(1) + "," +
          std::to_string((s % 1000) + 1000).substr(1);
}

static void print_header(const std::string& name, const benchmark_config& cfg)
{
   std::cout << "---------------------  " << name
             << "  --------------------------------------------------\n";
   std::cout << "rounds: " << cfg.rounds << "  items: " << format_comma(cfg.items)
             << " batch: " << format_comma(cfg.batch_size) << "\n";
   std::cout << "-----------------------------------------------------------------------\n";
}

// ============================================================
// Abstract KV Interface
// ============================================================

struct KVIterator
{
   virtual ~KVIterator()                             = default;
   virtual void seek_first()                         = 0;
   virtual bool seek(const char* key, size_t klen)   = 0;  // lower_bound semantics
   virtual void next()                               = 0;
   virtual bool valid()                              = 0;
   virtual std::pair<const char*, size_t> key()      = 0;
   virtual std::pair<const char*, size_t> value()    = 0;
};

struct KVEngine
{
   virtual ~KVEngine() = default;
   virtual const char* name() const = 0;

   virtual void open(const std::string& path, const benchmark_config& cfg) = 0;
   virtual void close() = 0;

   // Batch write (= one transaction / commit unit)
   virtual void begin_batch() = 0;
   virtual void commit_batch() = 0;
   virtual void sync() = 0;

   // KV operations
   virtual void put(const char* key, size_t klen, const char* val, size_t vlen) = 0;
   virtual void insert(const char* k, size_t kl, const char* v, size_t vl) { put(k, kl, v, vl); }
   virtual bool get(const char* key, size_t klen, std::string& val_out) = 0;
   virtual void del(const char* key, size_t klen) = 0;
   virtual std::unique_ptr<KVIterator> new_iterator() = 0;

   // Concurrent reader support
   virtual void reader_thread_init() {}
   virtual void reader_thread_teardown() {}
   virtual bool snapshot_get(const char* key, size_t klen, std::string& val_out) = 0;
   virtual std::unique_ptr<KVIterator> snapshot_iterator() = 0;
   virtual void refresh_snapshot() = 0;

   // Multi-writer: create a clone that shares the DB but has its own write state
   virtual std::unique_ptr<KVEngine> clone_for_writer(uint32_t writer_id) { return nullptr; }
   // Multi-reader: create a clone that shares the DB for read-only access
   virtual std::unique_ptr<KVEngine> clone_for_reader() { return nullptr; }

   virtual void print_stats() {}
};

// ============================================================
// PsiTri Engine
// ============================================================

#if defined(KV_ENGINE_PSITRI)

class PsiTriIterator : public KVIterator
{
   psitri::cursor _cur;

  public:
   PsiTriIterator(psitri::cursor c) : _cur(std::move(c)) {}
   void seek_first() override { _cur.seek_begin(); }
   bool seek(const char* key, size_t klen) override
   {
      return _cur.lower_bound(psitri::key_view(key, klen));
   }
   void next() override { _cur.next(); }
   bool valid() override { return !_cur.is_end(); }
   std::pair<const char*, size_t> key() override
   {
      auto k = _cur.key();
      return {k.data(), k.size()};
   }
   std::pair<const char*, size_t> value() override
   {
      std::pair<const char*, size_t> result{nullptr, 0};
      _cur.get_value([&](psitri::value_view vv) { result = {vv.data(), vv.size()}; });
      return result;
   }
};

class PsiTriEngine : public KVEngine
{
   std::shared_ptr<psitri::database>      _db;
   std::shared_ptr<psitri::write_session> _ses;
   std::optional<psitri::transaction>     _tx;
   uint32_t                               _root_index = 0;
   bool                                   _owns_db    = true;

   // Reader state
   std::shared_ptr<psitri::read_session> _reader_rs;

  public:
   const char* name() const override { return "PsiTri"; }

   void open(const std::string& path, const benchmark_config& bcfg) override
   {
      psitri::runtime_config cfg;
      if (bcfg.sync_mode == "safe")
         cfg.sync_mode = sal::sync_type::fsync;
      else if (bcfg.sync_mode == "full")
         cfg.sync_mode = sal::sync_type::full;
      else
         cfg.sync_mode = sal::sync_type::none;
      cfg.checksum_on_commit      = bcfg.checksum;
      cfg.write_protect_on_commit = bcfg.wp_commit;
      _db  = psitri::database::create(path, cfg);
      _ses = _db->start_write_session();
   }

   void close() override
   {
      _tx.reset();
      _ses.reset();
      if (_db && _owns_db)
      {
         _db->compact_and_truncate();
      }
      _db.reset();
   }

   void begin_batch() override
   {
      if (!_ses)
         _ses = _db->start_write_session();
      _tx.emplace(_ses->start_transaction(_root_index));
   }
   void commit_batch() override
   {
      _tx->commit();
      _tx.reset();
   }
   void sync() override { _db->sync(); }

   void put(const char* key, size_t klen, const char* val, size_t vlen) override
   {
      _tx->upsert(psitri::key_view(key, klen), psitri::value_view(val, vlen));
   }

   void insert(const char* key, size_t klen, const char* val, size_t vlen) override
   {
      _tx->insert(psitri::key_view(key, klen), psitri::value_view(val, vlen));
   }

   bool get(const char* key, size_t klen, std::string& val_out) override
   {
      auto root = _ses->get_root(_root_index);
      psitri::cursor cur(root);
      auto result = cur.get(psitri::key_view(key, klen), &val_out);
      return result >= 0;
   }

   void del(const char* key, size_t klen) override
   {
      _tx->remove(psitri::key_view(key, klen));
   }

   std::unique_ptr<KVIterator> new_iterator() override
   {
      auto root = _ses->get_root(_root_index);
      return std::make_unique<PsiTriIterator>(psitri::cursor(root));
   }

   void reader_thread_init() override { _reader_rs = _db->start_read_session(); }
   void reader_thread_teardown() override { _reader_rs.reset(); }

   bool snapshot_get(const char* key, size_t klen, std::string& val_out) override
   {
      auto cur = _reader_rs->create_cursor(_root_index);
      auto result = cur.get(psitri::key_view(key, klen), &val_out);
      return result >= 0;
   }

   std::unique_ptr<KVIterator> snapshot_iterator() override
   {
      auto cur = _reader_rs->create_cursor(_root_index);
      return std::make_unique<PsiTriIterator>(std::move(cur));
   }

   void refresh_snapshot() override
   {
      // read_session automatically sees latest committed state on next cursor creation
   }

   std::unique_ptr<KVEngine> clone_for_writer(uint32_t writer_id) override
   {
      auto clone = std::make_unique<PsiTriEngine>();
      clone->_db         = _db;
      // Don't create session here — get_session() is thread_local,
      // so it must be created on the writer thread (lazy init in begin_batch)
      clone->_root_index = writer_id + 1;
      clone->_owns_db    = false;
      return clone;
   }

   std::unique_ptr<KVEngine> clone_for_reader() override
   {
      auto clone = std::make_unique<PsiTriEngine>();
      clone->_db         = _db;
      clone->_root_index = _root_index;
      clone->_owns_db    = false;
      return clone;
   }

   void print_stats() override
   {
      auto root = _ses->get_root(_root_index);
      if (!root)
      {
         std::cout << "(empty tree)\n";
         return;
      }
      auto start = std::chrono::steady_clock::now();
      auto cur   = _ses->create_write_cursor(root);
      auto s     = cur->get_stats();
      auto end   = std::chrono::steady_clock::now();
      double elapsed = std::chrono::duration<double>(end - start).count();

      std::cout << "  inner_nodes: " << format_comma(s.inner_nodes)
                << "  inner_prefix_nodes: " << format_comma(s.inner_prefix_nodes)
                << "  leaf_nodes: " << format_comma(s.leaf_nodes)
                << "  value_nodes: " << format_comma(s.value_nodes) << "\n";
      std::cout << "  total_keys: " << format_comma(s.total_keys)
                << "  branches: " << format_comma(s.branches)
                << "  clines: " << format_comma(s.clines)
                << "  max_depth: " << s.max_depth << "\n";
      std::cout << "  avg_inner_size: " << s.average_inner_node_size()
                << "  avg_clines/inner: " << std::fixed << std::setprecision(2)
                << s.average_clines_per_inner_node()
                << "  avg_branches/inner: " << s.average_branch_per_inner_node() << "\n";
      std::cout << "  stats took " << std::fixed << std::setprecision(3) << elapsed << " sec\n";
   }
};
#endif  // KV_ENGINE_PSITRI

// ============================================================
// PsiTri DWAL Engine (buffered writes via ART + WAL)
// ============================================================

#if defined(KV_ENGINE_DWAL)

class DwalIterator : public KVIterator
{
   psitri::dwal::owned_merge_cursor _mc;

  public:
   DwalIterator(psitri::dwal::owned_merge_cursor mc) : _mc(std::move(mc)) {}
   void seek_first() override { _mc.cursor().seek_begin(); }
   bool seek(const char* key, size_t klen) override
   {
      return _mc.cursor().lower_bound(std::string_view(key, klen));
   }
   void next() override { _mc.cursor().next(); }
   bool valid() override { return !_mc.cursor().is_end(); }
   std::pair<const char*, size_t> key() override
   {
      auto k = _mc.cursor().key();
      return {k.data(), k.size()};
   }
   std::pair<const char*, size_t> value() override
   {
      auto& v = _mc.cursor().current_value();
      return {v.data.data(), v.data.size()};
   }
};

class DwalEngine : public KVEngine
{
   std::shared_ptr<psitri::database>            _db;
   std::shared_ptr<psitri::dwal::dwal_database> _dwal;
   std::optional<psitri::dwal::dwal_transaction> _tx;
   uint32_t _root_index = 0;
   bool _owns_db = true;

  public:
   const char* name() const override { return "PsiTri-DWAL"; }

   void open(const std::string& path, const benchmark_config& bcfg) override
   {
      psitri::runtime_config rcfg;
      if (bcfg.sync_mode == "safe")
         rcfg.sync_mode = sal::sync_type::fsync;
      else if (bcfg.sync_mode == "full")
         rcfg.sync_mode = sal::sync_type::full;
      else
         rcfg.sync_mode = sal::sync_type::none;
      _db = psitri::database::create(path + "/data", rcfg);

      psitri::dwal::dwal_config dcfg;
      dcfg.max_rw_entries = 100000;
      dcfg.merge_threads  = 2;
      _dwal = std::make_shared<psitri::dwal::dwal_database>(
          _db, path + "/wal", dcfg);
   }

   void close() override
   {
      _tx.reset();
      if (_dwal)
      {
         _dwal->request_shutdown();
         _dwal.reset();
      }
      if (_db && _owns_db)
      {
         _db->compact_and_truncate();
         _db.reset();
      }
   }

   void begin_batch() override
   {
      _tx.emplace(_dwal->start_write_transaction(_root_index));
   }

   void commit_batch() override
   {
      _tx->commit();
      _tx.reset();
   }

   void sync() override
   {
      _dwal->flush_wal(sal::sync_type::fsync);
   }

   void put(const char* key, size_t klen, const char* val, size_t vlen) override
   {
      _tx->upsert(std::string_view(key, klen), std::string_view(val, vlen));
   }

   void insert(const char* key, size_t klen, const char* val, size_t vlen) override
   {
      _tx->upsert(std::string_view(key, klen), std::string_view(val, vlen));
   }

   bool get(const char* key, size_t klen, std::string& val_out) override
   {
      auto r = _dwal->get_latest(_root_index, std::string_view(key, klen));
      if (!r.found)
         return false;
      val_out.assign(r.value.data.data(), r.value.data.size());
      return true;
   }

   void del(const char* key, size_t klen) override
   {
      _tx->remove(std::string_view(key, klen));
   }

   std::unique_ptr<KVIterator> new_iterator() override
   {
      return std::make_unique<DwalIterator>(
          _dwal->create_cursor(_root_index, psitri::dwal::read_mode::latest,
                               /*skip_rw_lock=*/true));
   }

   void reader_thread_init() override {}
   void reader_thread_teardown() override {}

   bool snapshot_get(const char* key, size_t klen, std::string& val_out) override
   {
      auto r = _dwal->get(_root_index, std::string_view(key, klen),
                          psitri::dwal::read_mode::buffered);
      if (!r.found)
         return false;
      val_out.assign(r.value.data.data(), r.value.data.size());
      return true;
   }

   std::unique_ptr<KVIterator> snapshot_iterator() override
   {
      return std::make_unique<DwalIterator>(
          _dwal->create_cursor(_root_index, psitri::dwal::read_mode::buffered));
   }

   void refresh_snapshot() override {}

   std::unique_ptr<KVEngine> clone_for_writer(uint32_t writer_id) override
   {
      auto clone = std::make_unique<DwalEngine>();
      clone->_db         = _db;
      clone->_dwal       = _dwal;
      clone->_root_index = writer_id + 1;
      clone->_owns_db    = false;
      return clone;
   }

   std::unique_ptr<KVEngine> clone_for_reader() override
   {
      auto clone = std::make_unique<DwalEngine>();
      clone->_db         = _db;
      clone->_dwal       = _dwal;
      clone->_root_index = _root_index;
      clone->_owns_db    = false;
      return clone;
   }

   void print_stats() override
   {
      std::cout << "(DWAL — stats not available without direct trie access)\n";
   }
};
#endif  // KV_ENGINE_DWAL

// ============================================================
// RocksDB Engine (works with both PsiTriRocks shim and real RocksDB)
// ============================================================

#if defined(KV_ENGINE_ROCKSDB)

class RocksIterator : public KVIterator
{
   rocksdb::Iterator* _it;

  public:
   RocksIterator(rocksdb::Iterator* it) : _it(it) {}
   ~RocksIterator() { delete _it; }
   void seek_first() override { _it->SeekToFirst(); }
   bool seek(const char* key, size_t klen) override
   {
      _it->Seek(rocksdb::Slice(key, klen));
      return _it->Valid();
   }
   void next() override { _it->Next(); }
   bool valid() override { return _it->Valid(); }
   std::pair<const char*, size_t> key() override
   {
      auto k = _it->key();
      return {k.data(), k.size()};
   }
   std::pair<const char*, size_t> value() override
   {
      auto v = _it->value();
      return {v.data(), v.size()};
   }
};

class RocksDbEngine : public KVEngine
{
   rocksdb::DB*                    _db = nullptr;
   rocksdb::WriteOptions           _wo;
   rocksdb::ReadOptions            _ro;
   std::string                     _sync_mode;
   rocksdb::WriteBatch             _batch;
   bool                            _owns_db = true;
   const rocksdb::Snapshot*        _snapshot = nullptr;

  public:
   const char* name() const override
   {
#if defined(PSITRIROCKS_BACKEND)
      return "PsiTriRocks";
#else
      return "RocksDB";
#endif
   }

   void open(const std::string& path, const benchmark_config& bcfg) override
   {
      _sync_mode = bcfg.sync_mode;
      rocksdb::DestroyDB(path, {});
      rocksdb::Options options;
      options.create_if_missing = true;
      auto s = rocksdb::DB::Open(options, path, &_db);
      if (!s.ok())
      {
         fprintf(stderr, "RocksDB open failed: %s\n", s.ToString().c_str());
         std::exit(1);
      }
      _wo.sync = (bcfg.sync_mode == "full");
   }

   void close() override
   {
      if (_snapshot)
      {
         _db->ReleaseSnapshot(_snapshot);
         _snapshot = nullptr;
      }
      if (_owns_db)
         delete _db;
      _db = nullptr;
   }

   void begin_batch() override { _batch.Clear(); }
   void commit_batch() override
   {
      _db->Write(_wo, &_batch);
      _batch.Clear();
   }

   void sync() override
   {
      if (_sync_mode == "none")
         return;
      rocksdb::FlushOptions fo;
      fo.wait = (_sync_mode == "full");
      _db->Flush(fo);
   }

   void put(const char* key, size_t klen, const char* val, size_t vlen) override
   {
      _batch.Put(rocksdb::Slice(key, klen), rocksdb::Slice(val, vlen));
   }

   bool get(const char* key, size_t klen, std::string& val_out) override
   {
      return _db->Get(_ro, rocksdb::Slice(key, klen), &val_out).ok();
   }

   void del(const char* key, size_t klen) override
   {
      _batch.Delete(rocksdb::Slice(key, klen));
   }

   std::unique_ptr<KVIterator> new_iterator() override
   {
      return std::make_unique<RocksIterator>(_db->NewIterator(_ro));
   }

   void reader_thread_init() override
   {
      _snapshot = _db->GetSnapshot();
   }

   void reader_thread_teardown() override
   {
      if (_snapshot)
      {
         _db->ReleaseSnapshot(_snapshot);
         _snapshot = nullptr;
      }
   }

   bool snapshot_get(const char* key, size_t klen, std::string& val_out) override
   {
      rocksdb::ReadOptions ro;
      ro.snapshot = _snapshot;
      return _db->Get(ro, rocksdb::Slice(key, klen), &val_out).ok();
   }

   std::unique_ptr<KVIterator> snapshot_iterator() override
   {
      rocksdb::ReadOptions ro;
      ro.snapshot = _snapshot;
      return std::make_unique<RocksIterator>(_db->NewIterator(ro));
   }

   void refresh_snapshot() override
   {
      if (_snapshot)
         _db->ReleaseSnapshot(_snapshot);
      _snapshot = _db->GetSnapshot();
   }

   std::unique_ptr<KVEngine> clone_for_writer(uint32_t /*writer_id*/) override
   {
      auto clone = std::make_unique<RocksDbEngine>();
      clone->_db       = _db;
      clone->_owns_db  = false;
      clone->_wo       = _wo;
      clone->_ro       = _ro;
      clone->_sync_mode = _sync_mode;
      return clone;
   }

   std::unique_ptr<KVEngine> clone_for_reader() override
   {
      auto clone = std::make_unique<RocksDbEngine>();
      clone->_db       = _db;
      clone->_owns_db  = false;
      clone->_wo       = _wo;
      clone->_ro       = _ro;
      clone->_sync_mode = _sync_mode;
      return clone;
   }
};
#endif  // KV_ENGINE_ROCKSDB

// ============================================================
// MDBX Engine
// ============================================================

#if defined(KV_ENGINE_MDBX)

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

class MdbxIterator : public KVIterator
{
   MDBX_txn*    _txn = nullptr;
   MDBX_cursor* _cur = nullptr;
   MDBX_val     _k{}, _v{};
   bool         _valid = false;
   bool         _owns_txn;

  public:
   MdbxIterator(MDBX_env* env, MDBX_dbi dbi, bool owns_txn = true)
       : _owns_txn(owns_txn)
   {
      if (owns_txn)
         MDBX_CHECK(mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)MDBX_TXN_RDONLY, &_txn));
      MDBX_CHECK(mdbx_cursor_open(_txn, dbi, &_cur));
   }

   MdbxIterator(MDBX_txn* txn, MDBX_dbi dbi) : _txn(txn), _owns_txn(false)
   {
      MDBX_CHECK(mdbx_cursor_open(_txn, dbi, &_cur));
   }

   ~MdbxIterator()
   {
      if (_cur)
         mdbx_cursor_close(_cur);
      if (_owns_txn && _txn)
         mdbx_txn_abort(_txn);
   }

   void seek_first() override
   {
      _valid = (mdbx_cursor_get(_cur, &_k, &_v, MDBX_FIRST) == MDBX_SUCCESS);
   }
   bool seek(const char* key, size_t klen) override
   {
      _k = {const_cast<char*>(key), klen};
      _valid = (mdbx_cursor_get(_cur, &_k, &_v, MDBX_SET_RANGE) == MDBX_SUCCESS);
      return _valid;
   }
   void next() override
   {
      _valid = (mdbx_cursor_get(_cur, &_k, &_v, MDBX_NEXT) == MDBX_SUCCESS);
   }
   bool valid() override { return _valid; }
   std::pair<const char*, size_t> key() override
   {
      return {(const char*)_k.iov_base, _k.iov_len};
   }
   std::pair<const char*, size_t> value() override
   {
      return {(const char*)_v.iov_base, _v.iov_len};
   }
};

class MdbxEngine : public KVEngine
{
   MDBX_env*   _env = nullptr;
   MDBX_dbi    _dbi = 0;
   MDBX_txn*   _txn = nullptr;
   std::string _sync_mode;
   std::mutex  _write_mutex;  // MDBX is single-writer
   bool        _owns_env = true;

   // Reader state
   MDBX_txn* _read_txn = nullptr;

  public:
   const char* name() const override { return "MDBX"; }

   void open(const std::string& path, const benchmark_config& bcfg) override
   {
      _sync_mode = bcfg.sync_mode;
      MDBX_CHECK(mdbx_env_create(&_env));
      MDBX_CHECK(mdbx_env_set_geometry(_env, 1024 * 1024, 1024 * 1024,
                                        64ULL * 1024 * 1024 * 1024, -1, -1, -1));
      MDBX_CHECK(mdbx_env_set_maxreaders(_env, 8));

      unsigned flags = MDBX_SAFE_NOSYNC | MDBX_LIFORECLAIM | MDBX_NOSUBDIR;
      MDBX_CHECK(mdbx_env_open(_env, path.c_str(), (MDBX_env_flags_t)flags, 0664));

      MDBX_txn* txn = nullptr;
      MDBX_CHECK(mdbx_txn_begin(_env, nullptr, (MDBX_txn_flags_t)0, &txn));
      MDBX_CHECK(mdbx_dbi_open(txn, nullptr, MDBX_CREATE, &_dbi));
      MDBX_CHECK(mdbx_txn_commit(txn));
   }

   void close() override
   {
      if (_read_txn)
      {
         mdbx_txn_abort(_read_txn);
         _read_txn = nullptr;
      }
      if (_env && _owns_env)
      {
         mdbx_dbi_close(_env, _dbi);
         mdbx_env_close(_env);
      }
      _env = nullptr;
   }

   void begin_batch() override
   {
      MDBX_CHECK(mdbx_txn_begin(_env, nullptr, (MDBX_txn_flags_t)0, &_txn));
   }

   void commit_batch() override
   {
      MDBX_CHECK(mdbx_txn_commit(_txn));
      _txn = nullptr;
   }

   void sync() override
   {
      if (_sync_mode == "none")
         return;
      bool nonblock = (_sync_mode == "safe");
      mdbx_env_sync_ex(_env, true, nonblock);
   }

   void put(const char* key, size_t klen, const char* val, size_t vlen) override
   {
      MDBX_val k{const_cast<char*>(key), klen};
      MDBX_val v{const_cast<char*>(val), vlen};
      MDBX_CHECK(mdbx_put(_txn, _dbi, &k, &v, MDBX_UPSERT));
   }

   bool get(const char* key, size_t klen, std::string& val_out) override
   {
      MDBX_val k{const_cast<char*>(key), klen};
      MDBX_val v;
      // If we have a write txn open, read from it; otherwise open a read-only txn
      MDBX_txn* txn = _txn;
      bool own_txn = false;
      if (!txn)
      {
         MDBX_CHECK(mdbx_txn_begin(_env, nullptr, (MDBX_txn_flags_t)MDBX_TXN_RDONLY, &txn));
         own_txn = true;
      }
      int rc = mdbx_get(txn, _dbi, &k, &v);
      if (own_txn)
         mdbx_txn_abort(txn);
      if (rc != MDBX_SUCCESS)
         return false;
      val_out.assign((const char*)v.iov_base, v.iov_len);
      return true;
   }

   void del(const char* key, size_t klen) override
   {
      MDBX_val k{const_cast<char*>(key), klen};
      mdbx_del(_txn, _dbi, &k, nullptr);
   }

   std::unique_ptr<KVIterator> new_iterator() override
   {
      return std::make_unique<MdbxIterator>(_env, _dbi);
   }

   void reader_thread_init() override
   {
      MDBX_CHECK(mdbx_txn_begin(_env, nullptr, (MDBX_txn_flags_t)MDBX_TXN_RDONLY, &_read_txn));
   }

   void reader_thread_teardown() override
   {
      if (_read_txn)
      {
         mdbx_txn_abort(_read_txn);
         _read_txn = nullptr;
      }
   }

   bool snapshot_get(const char* key, size_t klen, std::string& val_out) override
   {
      MDBX_CHECK(mdbx_txn_renew(_read_txn));
      MDBX_val k{const_cast<char*>(key), klen};
      MDBX_val v;
      int rc = mdbx_get(_read_txn, _dbi, &k, &v);
      mdbx_txn_reset(_read_txn);
      if (rc != MDBX_SUCCESS)
         return false;
      val_out.assign((const char*)v.iov_base, v.iov_len);
      return true;
   }

   std::unique_ptr<KVIterator> snapshot_iterator() override
   {
      // Create a fresh read-only txn for the iterator
      return std::make_unique<MdbxIterator>(_env, _dbi);
   }

   void refresh_snapshot() override
   {
      // renew/reset is done per-call in snapshot_get
   }

   std::unique_ptr<KVEngine> clone_for_writer(uint32_t /*writer_id*/) override
   {
      // MDBX is single-writer — clones share env but serialize writes
      auto clone = std::make_unique<MdbxEngine>();
      clone->_env       = _env;
      clone->_dbi       = _dbi;
      clone->_sync_mode = _sync_mode;
      clone->_owns_env  = false;
      return clone;
   }

   std::unique_ptr<KVEngine> clone_for_reader() override
   {
      auto clone = std::make_unique<MdbxEngine>();
      clone->_env       = _env;
      clone->_dbi       = _dbi;
      clone->_sync_mode = _sync_mode;
      clone->_owns_env  = false;
      return clone;
   }
};
#endif  // KV_ENGINE_MDBX

// ============================================================
// TidesDB Engine
// ============================================================

#if defined(KV_ENGINE_TIDESDB)

class TidesIterator : public KVIterator
{
   tdb_wrapper_iter_t* _it;

  public:
   TidesIterator(tdb_wrapper_iter_t* it) : _it(it) {}
   ~TidesIterator()
   {
      if (_it)
         tdb_iter_free(_it);
   }
   void seek_first() override { tdb_iter_seek_first(_it); }
   bool seek(const char* key, size_t klen) override
   {
      return tdb_iter_seek(_it, (const uint8_t*)key, klen) == TDB_WRAP_SUCCESS;
   }
   void next() override { tdb_iter_next(_it); }
   bool valid() override { return tdb_iter_valid(_it); }
   std::pair<const char*, size_t> key() override
   {
      uint8_t* k   = nullptr;
      size_t   len = 0;
      tdb_iter_key(_it, &k, &len);
      return {(const char*)k, len};
   }
   std::pair<const char*, size_t> value() override
   {
      uint8_t* v   = nullptr;
      size_t   len = 0;
      tdb_iter_value(_it, &v, &len);
      return {(const char*)v, len};
   }
};

class TidesDbEngine : public KVEngine
{
   tdb_wrapper_t*     _db  = nullptr;
   tdb_wrapper_txn_t* _txn = nullptr;
   uint32_t           _op_count = 0;
   bool               _owns_db = true;
   static constexpr const char* CF = "bench";

   // Reader state
   tdb_wrapper_txn_t* _read_txn = nullptr;

  public:
   const char* name() const override { return "TidesDB"; }

   void open(const std::string& path, const benchmark_config& bcfg) override
   {
      int sm = TDB_WRAP_SYNC_NONE;
      if (bcfg.sync_mode == "safe")
         sm = TDB_WRAP_SYNC_SAFE;
      else if (bcfg.sync_mode == "full")
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
      if (_read_txn)
      {
         tdb_txn_free(_read_txn);
         _read_txn = nullptr;
      }
      if (_db && _owns_db)
      {
         tdb_close(_db);
      }
      _db = nullptr;
   }

   void begin_batch() override
   {
      _txn = tdb_txn_begin(_db);
      _op_count = 0;
   }

   void commit_batch() override
   {
      tdb_txn_commit(_txn);
      tdb_txn_free(_txn);
      _txn = nullptr;
      _op_count = 0;
   }

   void sync() override { /* TidesDB sync is commit-integrated */ }

   void auto_split()
   {
      if (_op_count >= 90000)
      {
         tdb_txn_commit(_txn);
         tdb_txn_free(_txn);
         _txn = tdb_txn_begin(_db);
         _op_count = 0;
      }
   }

   void put(const char* key, size_t klen, const char* val, size_t vlen) override
   {
      auto_split();
      tdb_put(_txn, CF, (const uint8_t*)key, klen, (const uint8_t*)val, vlen);
      ++_op_count;
   }

   bool get(const char* key, size_t klen, std::string& val_out) override
   {
      uint8_t* v   = nullptr;
      size_t   vlen = 0;
      auto* txn = _txn ? _txn : tdb_txn_begin(_db);
      int rc = tdb_get(txn, CF, (const uint8_t*)key, klen, &v, &vlen);
      if (!_txn)
         tdb_txn_free(txn);
      if (rc != TDB_WRAP_SUCCESS)
         return false;
      val_out.assign((const char*)v, vlen);
      free(v);
      return true;
   }

   void del(const char* key, size_t klen) override
   {
      auto_split();
      tdb_delete(_txn, CF, (const uint8_t*)key, klen);
      ++_op_count;
   }

   std::unique_ptr<KVIterator> new_iterator() override
   {
      auto* txn = tdb_txn_begin(_db);
      auto* it  = tdb_iter_new(txn, CF);
      // Note: txn lifetime managed by iterator (leaked for now — TidesDB limitation)
      return std::make_unique<TidesIterator>(it);
   }

   void reader_thread_init() override
   {
      _read_txn = tdb_txn_begin(_db);
   }

   void reader_thread_teardown() override
   {
      if (_read_txn)
      {
         tdb_txn_free(_read_txn);
         _read_txn = nullptr;
      }
   }

   bool snapshot_get(const char* key, size_t klen, std::string& val_out) override
   {
      uint8_t* v   = nullptr;
      size_t   vlen = 0;
      int rc = tdb_get(_read_txn, CF, (const uint8_t*)key, klen, &v, &vlen);
      if (rc != TDB_WRAP_SUCCESS)
         return false;
      val_out.assign((const char*)v, vlen);
      free(v);
      return true;
   }

   std::unique_ptr<KVIterator> snapshot_iterator() override
   {
      auto* it = tdb_iter_new(_read_txn, CF);
      return std::make_unique<TidesIterator>(it);
   }

   void refresh_snapshot() override
   {
      if (_read_txn)
         tdb_txn_free(_read_txn);
      _read_txn = tdb_txn_begin(_db);
   }

   std::unique_ptr<KVEngine> clone_for_writer(uint32_t /*writer_id*/) override
   {
      auto clone       = std::make_unique<TidesDbEngine>();
      clone->_db       = _db;
      clone->_owns_db  = false;
      return clone;
   }

   std::unique_ptr<KVEngine> clone_for_reader() override
   {
      auto clone       = std::make_unique<TidesDbEngine>();
      clone->_db       = _db;
      clone->_owns_db  = false;
      return clone;
   }
};
#endif  // KV_ENGINE_TIDESDB

// ============================================================
// Engine factory
// ============================================================

static std::unique_ptr<KVEngine> make_engine()
{
#if defined(KV_ENGINE_PSITRI)
   return std::make_unique<PsiTriEngine>();
#elif defined(KV_ENGINE_DWAL)
   return std::make_unique<DwalEngine>();
#elif defined(KV_ENGINE_ROCKSDB)
   return std::make_unique<RocksDbEngine>();
#elif defined(KV_ENGINE_MDBX)
   return std::make_unique<MdbxEngine>();
#elif defined(KV_ENGINE_TIDESDB)
   return std::make_unique<TidesDbEngine>();
#else
#error "No KV_ENGINE_* defined"
#endif
}

// ============================================================
// Benchmark functions
// ============================================================

void insert_test(benchmark_config cfg, KVEngine& eng, const std::string& name, auto make_key)
{
   print_header(name, cfg);

   std::vector<char> key;
   std::vector<char> value(cfg.value_size, 'v');
   uint64_t          seq = 0;

   for (uint32_t r = 0; r < cfg.rounds; ++r)
   {
      eng.begin_batch();
      auto     start    = std::chrono::steady_clock::now();
      uint32_t inserted = 0;
      while (inserted < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq++, key);
            eng.insert(key.data(), key.size(), value.data(), value.size());
            ++inserted;
         }
      }
      eng.commit_batch();

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   ips  = uint64_t(inserted / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(ips)
                << "  inserts/sec\n";
   }
   eng.print_stats();
}

void upsert_test(benchmark_config cfg, KVEngine& eng, const std::string& name, auto make_key)
{
   print_header(name, cfg);

   std::vector<char> key;
   std::vector<char> value(cfg.value_size, 'u');
   uint64_t          seq = 0;

   for (uint32_t r = 0; r < cfg.rounds; ++r)
   {
      eng.begin_batch();
      auto     start = std::chrono::steady_clock::now();
      uint32_t count = 0;
      while (count < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - count);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq++, key);
            eng.put(key.data(), key.size(), value.data(), value.size());
            ++count;
         }
      }
      eng.commit_batch();

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   ups  = uint64_t(count / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(ups)
                << "  upserts/sec\n";
   }
   eng.print_stats();
}

void get_test(benchmark_config cfg, KVEngine& eng, const std::string& name, auto make_key)
{
   print_header(name, cfg);

   std::vector<char> key;
   std::string       buf;

   auto     start = std::chrono::steady_clock::now();
   uint64_t found = 0;
   for (uint64_t i = 0; i < uint64_t(cfg.items) * cfg.rounds; ++i)
   {
      make_key(i, key);
      if (eng.get(key.data(), key.size(), buf))
         ++found;
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   gps  = uint64_t(found / secs);
   std::cout << format_comma(gps) << " gets/sec  (" << format_comma(found) << " found)\n";
}

void get_rand_test(benchmark_config cfg, KVEngine& eng, const std::string& name, auto make_key)
{
   print_header(name, cfg);

   std::vector<char> key;
   std::string       buf;

   auto     start = std::chrono::steady_clock::now();
   uint64_t count = 0;
   uint64_t found = 0;
   for (uint64_t i = 0; i < uint64_t(cfg.items) * cfg.rounds; ++i)
   {
      make_key(i, key);
      if (eng.get(key.data(), key.size(), buf))
         ++found;
      ++count;
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   gps  = uint64_t(count / secs);
   std::cout << format_comma(gps) << " gets/sec  (" << format_comma(found) << " found / "
             << format_comma(count) << " ops)\n";
}

void lower_bound_test(benchmark_config cfg, KVEngine& eng, const std::string& name, auto make_key)
{
   print_header(name, cfg);

   std::vector<char> key;
   auto              iter = eng.new_iterator();

   auto     start = std::chrono::steady_clock::now();
   uint64_t count = 0;
   for (uint64_t i = 0; i < uint64_t(cfg.items) * cfg.rounds; ++i)
   {
      make_key(i, key);
      iter->seek(key.data(), key.size());
      ++count;
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   lps  = uint64_t(count / secs);
   std::cout << format_comma(lps) << " lower_bounds/sec  (" << format_comma(count) << " ops)\n";
}

void iterate_test(benchmark_config cfg, KVEngine& eng)
{
   std::cout << "---------------------  iterate  "
                "--------------------------------------------------\n";

   auto iter = eng.new_iterator();

   auto     start = std::chrono::steady_clock::now();
   uint64_t count = 0;
   iter->seek_first();
   while (iter->valid())
   {
      ++count;
      iter->next();
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   kps  = uint64_t(count / secs);
   std::cout << format_comma(count) << " keys iterated in " << std::fixed << std::setprecision(3)
             << secs << " sec  (" << format_comma(kps) << " keys/sec)\n";
}

void remove_test(benchmark_config cfg, KVEngine& eng, const std::string& name, auto make_key)
{
   print_header(name, cfg);

   std::vector<char> key;
   uint64_t          seq = 0;

   for (uint32_t r = 0; r < cfg.rounds; ++r)
   {
      eng.begin_batch();
      auto     start   = std::chrono::steady_clock::now();
      uint32_t removed = 0;
      while (removed < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - removed);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq++, key);
            eng.del(key.data(), key.size());
            ++removed;
         }
      }
      eng.commit_batch();

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   rps  = uint64_t(removed / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(rps)
                << "  removes/sec\n";
   }
   eng.print_stats();
}

void remove_rand_test(benchmark_config cfg, KVEngine& eng, const std::string& name, auto make_key)
{
   print_header(name, cfg);

   uint64_t total = uint64_t(cfg.items) * cfg.rounds;
   std::vector<uint64_t> indices(total);
   for (uint64_t i = 0; i < total; ++i)
      indices[i] = i;
   for (uint64_t i = total - 1; i > 0; --i)
   {
      uint64_t j = rand_from_seq(i) % (i + 1);
      std::swap(indices[i], indices[j]);
   }

   std::vector<char> key;
   uint64_t          pos = 0;

   for (uint32_t r = 0; r < cfg.rounds; ++r)
   {
      eng.begin_batch();
      auto     start   = std::chrono::steady_clock::now();
      uint32_t removed = 0;
      while (removed < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - removed);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(indices[pos++], key);
            eng.del(key.data(), key.size());
            ++removed;
         }
      }
      eng.commit_batch();

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   rps  = uint64_t(removed / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(pos) << "  " << std::setw(12) << std::right << format_comma(rps)
                << "  removes/sec\n";
   }
   eng.print_stats();
}

// -- Multi-writer benchmark --

void multiwriter_test(benchmark_config cfg,
                      KVEngine&        eng,
                      uint32_t         num_writers,
                      auto             make_key,
                      const std::string& name)
{
   std::cout << "---------------------  " << name << "  "
             << std::string(std::max(0, int(52 - int(name.size()))), '-') << "\n";
   std::cout << "write rounds: " << cfg.rounds << "  items: " << format_comma(cfg.items)
             << "  writers: " << num_writers << "\n";
   std::cout << "-----------------------------------------------------------------------\n";

   struct alignas(128) padded_counter
   {
      std::atomic<int64_t> count{0};
   };

   std::atomic<bool>           start_flag{false};
   std::atomic<bool>           done{false};
   std::vector<padded_counter> counters(num_writers);

   // Create per-writer engine clones
   std::vector<std::unique_ptr<KVEngine>> writer_engines;
   for (uint32_t t = 0; t < num_writers; ++t)
      writer_engines.push_back(eng.clone_for_writer(t));

   std::vector<std::thread> writers;
   writers.reserve(num_writers);
   for (uint32_t t = 0; t < num_writers; ++t)
   {
      writers.emplace_back(
          [&start_flag, &done, &counters, &cfg, t, &make_key, num_writers,
           eng_ptr = writer_engines[t].get()]()
          {
             std::vector<char> key;
             std::vector<char> value(cfg.value_size, 'v');
             int64_t           total_inserted = 0;

             while (!start_flag.load(std::memory_order_relaxed))
                ;

             for (uint32_t r = 0; r < cfg.rounds && !done.load(std::memory_order_relaxed); ++r)
             {
                eng_ptr->begin_batch();
                uint32_t inserted = 0;
                while (inserted < cfg.items)
                {
                   uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
                   for (uint32_t i = 0; i < batch; ++i)
                   {
                      uint64_t seq = uint64_t(r) * cfg.items + inserted + i;
                      seq          = seq * num_writers + t;
                      make_key(seq, key);
                      eng_ptr->put(key.data(), key.size(), value.data(), value.size());
                      ++inserted;
                   }
                }
                eng_ptr->commit_batch();
                total_inserted += inserted;
                counters[t].count.store(total_inserted, std::memory_order_relaxed);
             }
          });
   }

   auto sum_inserts = [&]()
   {
      int64_t total = 0;
      for (uint32_t i = 0; i < num_writers; ++i)
         total += counters[i].count.load(std::memory_order_relaxed);
      return total;
   };

   auto overall_start = std::chrono::steady_clock::now();
   start_flag.store(true, std::memory_order_relaxed);

   int64_t  prev_inserts   = 0;
   auto     prev_time      = overall_start;
   int64_t  target_inserts = int64_t(cfg.items) * cfg.rounds * num_writers;
   uint32_t report_num     = 0;
   while (true)
   {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      auto    now   = std::chrono::steady_clock::now();
      double  secs  = std::chrono::duration<double>(now - prev_time).count();
      int64_t cur   = sum_inserts();
      int64_t delta = cur - prev_inserts;
      prev_inserts  = cur;
      prev_time     = now;
      auto ips      = uint64_t(delta / secs);
      std::cout << std::setw(4) << std::left << report_num++ << " " << std::setw(12) << std::right
                << format_comma(cur) << "  " << std::setw(12) << std::right << format_comma(ips)
                << "  inserts/sec (aggregate)\n";
      if (cur >= target_inserts)
         break;
   }

   done.store(true, std::memory_order_relaxed);
   for (auto& t : writers)
      t.join();

   auto   overall_end    = std::chrono::steady_clock::now();
   double overall_secs   = std::chrono::duration<double>(overall_end - overall_start).count();
   auto   final_inserts  = sum_inserts();
   std::cout << "total: " << format_comma(final_inserts) << " inserts across " << num_writers
             << " writers in " << std::fixed << std::setprecision(3) << overall_secs << " sec\n";
   std::cout << "  aggregate: " << format_comma(uint64_t(final_inserts / overall_secs))
             << " inserts/sec  per-writer: "
             << format_comma(uint64_t(final_inserts / overall_secs / num_writers))
             << " inserts/sec\n";
}

// -- Multithread read+write --

void multithread_rw_test(benchmark_config cfg,
                         KVEngine&        eng,
                         uint32_t         num_threads,
                         auto             make_key,
                         const std::string& read_op,
                         const std::string& key_mode)
{
   std::string label = "multithread " + read_op + " (" + key_mode + " keys)";
   std::cout << "---------------------  " << label << "  "
             << std::string(std::max(0, int(52 - (int)label.size())), '-') << "\n";
   std::cout << "write rounds: " << cfg.rounds << "  items: " << format_comma(cfg.items)
             << "  read threads: " << num_threads << "\n";
   std::cout << "-----------------------------------------------------------------------\n";

   bool use_get   = (read_op == "get");
   bool use_known = (key_mode == "known");

   // Seed the tree so readers start with data
   {
      std::vector<char> key;
      std::vector<char> value(cfg.value_size, 'v');
      eng.begin_batch();
      for (uint32_t i = 0; i < cfg.items; ++i)
      {
         make_key(i, key);
         eng.put(key.data(), key.size(), value.data(), value.size());
      }
      eng.commit_batch();
   }
   std::cout << "seeded " << format_comma(cfg.items) << " keys\n";
   eng.print_stats();

   struct alignas(128) padded_counters
   {
      std::atomic<int64_t> ops{0};
      std::atomic<int64_t> found{0};
   };

   std::atomic<uint64_t>        committed_seq{cfg.items};
   std::atomic<bool>            done{false};
   std::vector<padded_counters> counters(num_threads);

   // Create per-thread reader clones so each thread has independent state
   std::vector<std::unique_ptr<KVEngine>> reader_engines;
   for (uint32_t t = 0; t < num_threads; ++t)
      reader_engines.push_back(eng.clone_for_reader());

   // Launch reader threads
   std::vector<std::thread> readers;
   readers.reserve(num_threads);
   for (uint32_t t = 0; t < num_threads; ++t)
   {
      readers.emplace_back(
          [&done, &counters, &committed_seq, t, &make_key, use_get, use_known,
           reng = reader_engines[t].get()]()
          {
             reng->reader_thread_init();

             std::vector<char> key;
             std::string       buf;
             int64_t           local_ops     = 0;
             int64_t           local_found   = 0;
             uint32_t          refresh_counter = 0;
             const uint64_t    salt = rand_from_seq(t * 999983ULL + 1);

             while (!done.load(std::memory_order_relaxed))
             {
                if (++refresh_counter >= 10)
                {
                   reng->refresh_snapshot();
                   refresh_counter = 0;
                }
                uint64_t max_seq = committed_seq.load(std::memory_order_relaxed);
                for (uint32_t i = 0; i < 1000; ++i)
                {
                   uint64_t seq = rand_from_seq(local_ops + salt);
                   if (use_known)
                      seq = seq % max_seq;
                   make_key(seq, key);

                   if (use_get)
                   {
                      if (reng->snapshot_get(key.data(), key.size(), buf))
                         ++local_found;
                   }
                   else
                   {
                      auto iter = reng->snapshot_iterator();
                      if (iter->seek(key.data(), key.size()))
                         ++local_found;
                   }
                   ++local_ops;
                }
                counters[t].ops.store(local_ops, std::memory_order_relaxed);
                counters[t].found.store(local_found, std::memory_order_relaxed);
             }

             reng->reader_thread_teardown();
          });
   }

   auto sum_ops = [&]()
   {
      int64_t total = 0;
      for (uint32_t i = 0; i < num_threads; ++i)
         total += counters[i].ops.load(std::memory_order_relaxed);
      return total;
   };
   auto sum_found = [&]()
   {
      int64_t total = 0;
      for (uint32_t i = 0; i < num_threads; ++i)
         total += counters[i].found.load(std::memory_order_relaxed);
      return total;
   };

   // Writer: insert new keys while readers are running
   auto overall_start = std::chrono::steady_clock::now();

   std::vector<char> key;
   std::vector<char> value(cfg.value_size, 'v');
   uint64_t          seq = cfg.items;

   int64_t prev_ops = 0;
   for (uint32_t r = 0; r < cfg.rounds; ++r)
   {
      eng.begin_batch();
      auto     start    = std::chrono::steady_clock::now();
      uint32_t inserted = 0;
      while (inserted < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq++, key);
            eng.put(key.data(), key.size(), value.data(), value.size());
            ++inserted;
         }
      }
      eng.commit_batch();
      committed_seq.store(seq, std::memory_order_relaxed);

      auto   end       = std::chrono::steady_clock::now();
      double secs      = std::chrono::duration<double>(end - start).count();
      auto   ips       = uint64_t(inserted / secs);
      auto   cur_ops   = sum_ops();
      auto   round_ops = cur_ops - prev_ops;
      auto   rps       = uint64_t(round_ops / secs);
      prev_ops         = cur_ops;
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(ips)
                << "  inserts/sec  " << std::setw(12) << std::right << format_comma(rps)
                << "  " << read_op << "s/sec\n";
   }

   done.store(true, std::memory_order_relaxed);
   for (auto& t : readers)
      t.join();

   auto   overall_end  = std::chrono::steady_clock::now();
   double overall_secs = std::chrono::duration<double>(overall_end - overall_start).count();
   auto   final_ops    = sum_ops();
   auto   final_found  = sum_found();
   auto   written      = seq - cfg.items;
   std::cout << "total: " << format_comma(written) << " inserts, " << format_comma(final_ops)
             << " " << read_op << "s (" << format_comma(final_found) << " found) in "
             << std::fixed << std::setprecision(3) << overall_secs << " sec\n";
   std::cout << "  write: " << format_comma(uint64_t(written / overall_secs))
             << "/sec  " << read_op << ": "
             << format_comma(uint64_t(final_ops / overall_secs)) << "/sec\n";
}

// ============================================================
// Main
// ============================================================

int main(int argc, char** argv)
{
   uint32_t    rounds;
   uint32_t    batch;
   uint32_t    items;
   uint32_t    value_size;
   uint32_t    threads;
   bool        reset    = false;
   std::string db_dir   = "/tmp/kv_bench_db";
   std::string bench    = "all";
   std::string sync_str = "none";

   po::options_description desc("kv-benchmark options");
   auto                    opt = desc.add_options();
   opt("help,h", "print this message");
   opt("round,r", po::value<uint32_t>(&rounds)->default_value(3), "number of rounds");
   opt("batch,b", po::value<uint32_t>(&batch)->default_value(512), "batch size");
   opt("items,i", po::value<uint32_t>(&items)->default_value(1000000), "number of items per round");
   opt("value-size,s", po::value<uint32_t>(&value_size)->default_value(8), "value size in bytes");
   opt("threads,t", po::value<uint32_t>(&threads)->default_value(4), "reader/writer threads");
   opt("db-dir,d", po::value<std::string>(&db_dir)->default_value("/tmp/kv_bench_db"), "database dir");
   opt("bench", po::value<std::string>(&bench)->default_value("all"),
       "benchmark: all, insert, upsert, get, iterate, remove, remove-rand, lower-bound, get-rand, "
       "multiwriter-rand, multiwriter-seq, "
       "multithread-lowerbound-rand, multithread-lowerbound-known, "
       "multithread-get-rand, multithread-get-known");
   opt("sync", po::value<std::string>(&sync_str)->default_value("none"),
       "sync mode: none, safe, full");
   opt("reset", po::bool_switch(&reset), "reset database before running");
   opt("checksum-on-commit", po::bool_switch(), "PsiTri: enable per-commit segment checksum (default: off, compactor checksums independently)");
   opt("no-write-protect", po::bool_switch(), "PsiTri: disable write_protect_on_commit (default: on)");

   po::variables_map vm;
   po::store(po::parse_command_line(argc, argv, desc), vm);
   po::notify(vm);

   if (vm.count("help"))
   {
      std::cout << desc << "\n";
      return 0;
   }

   bool checksum  = vm["checksum-on-commit"].as<bool>();
   bool wp_commit = !vm["no-write-protect"].as<bool>();

   auto eng = make_engine();

   if (reset)
      std::filesystem::remove_all(db_dir);

   benchmark_config cfg = {rounds, items, batch, value_size, threads, sync_str, checksum, wp_commit};

   eng->open(db_dir, cfg);

   std::cout << "kv-benchmark [" << eng->name() << "]: db=" << db_dir << "\n";
   std::cout << "rounds=" << rounds << " items=" << format_comma(items)
             << " batch=" << batch << " value_size=" << value_size
             << " threads=" << threads << " sync=" << sync_str
             << " checksum=" << (checksum ? "on" : "off")
             << " write_protect=" << (wp_commit ? "on" : "off") << "\n\n";

   auto run_all    = (bench == "all");
   auto be_seq_key = [](uint64_t seq, auto& v) { to_key(to_big_endian(seq), v); };
   auto rand_key   = [](uint64_t seq, auto& v) { to_key(rand_from_seq(seq), v); };
   auto str_rand_key = [](uint64_t seq, auto& v) { to_key(std::to_string(rand_from_seq(seq)), v); };

   // -- Insert --
   if (run_all || bench == "insert")
   {
      insert_test(cfg, *eng, "big endian seq insert", be_seq_key);
      insert_test(cfg, *eng, "dense random insert", rand_key);
      insert_test(cfg, *eng, "string number rand insert", str_rand_key);
   }

   // -- Get --
   if (run_all || bench == "get")
   {
      get_test(cfg, *eng, "big endian seq get", be_seq_key);
      get_test(cfg, *eng, "dense random get", rand_key);
   }

   // -- Upsert --
   if (run_all || bench == "upsert")
   {
      upsert_test(cfg, *eng, "big endian seq upsert", be_seq_key);
   }

   // -- Iterate --
   if (run_all || bench == "iterate")
   {
      iterate_test(cfg, *eng);
   }

   // -- Lower-bound --
   if (run_all || bench == "lower-bound")
   {
      lower_bound_test(cfg, *eng, "random lower_bound", rand_key);
   }

   // -- Random get --
   if (run_all || bench == "get-rand")
   {
      get_rand_test(cfg, *eng, "random get", rand_key);
   }

   // -- Remove --
   if (run_all || bench == "remove")
   {
      remove_test(cfg, *eng, "big endian seq remove", be_seq_key);
   }

   // -- Random remove --
   if (run_all || bench == "remove-rand")
   {
      remove_rand_test(cfg, *eng, "random remove (known keys)", rand_key);
   }

   // -- Multi-writer --
   if (run_all || bench == "multiwriter-rand")
   {
      multiwriter_test(cfg, *eng, threads, rand_key, "multi-writer rand insert");
   }
   if (run_all || bench == "multiwriter-seq")
   {
      multiwriter_test(cfg, *eng, threads, be_seq_key, "multi-writer seq insert");
   }

   // -- Multithread read+write --
   if (run_all || bench == "multithread-get-known")
   {
      multithread_rw_test(cfg, *eng, threads, rand_key, "get", "known");
      eng->print_stats();
   }
   if (run_all || bench == "multithread-get-rand")
   {
      multithread_rw_test(cfg, *eng, threads, rand_key, "get", "rand");
      eng->print_stats();
   }

   eng->close();
   std::cout << "\ndone.\n";
   return 0;
}
