/// rocks_bench — db_bench-compatible benchmark for PsiTriRocks (and optionally real RocksDB)
///
/// Runs the standard workloads: fillseq, fillrandom, overwrite, readrandom,
/// readseq, readreverse, seekrandom, deleteseq, deleterandom, readwhilewriting
///
/// Build against PsiTriRocks:  links psitrirocks
/// Build against real RocksDB: links rocksdb (when BUILD_ROCKSDB_BENCH=ON)
///
/// Output format matches db_bench for easy comparison.

#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <thread>

#include "bench_signal.hpp"
#include <vector>

// ── Helpers ────────────────────────────────────────────────────────────────

namespace
{

   std::string comma_format(uint64_t n)
   {
      auto s = std::to_string(n);
      int  insertPos = static_cast<int>(s.length()) - 3;
      while (insertPos > 0)
      {
         s.insert(insertPos, ",");
         insertPos -= 3;
      }
      return s;
   }

   // Generate a key from an integer, zero-padded to key_size bytes
   std::string make_key(uint64_t k, int key_size)
   {
      std::string s(key_size, '0');
      auto        num = std::to_string(k);
      if ((int)num.size() < key_size)
         std::copy(num.rbegin(), num.rend(), s.rbegin());
      else
         s = num.substr(0, key_size);
      return s;
   }

   std::string make_value(int value_size, uint64_t seed)
   {
      std::string v(value_size, 'x');
      // Mix in some variation
      for (int i = 0; i < value_size && i < 8; i++)
         v[i] = 'a' + ((seed >> (i * 4)) & 0xf) % 26;
      return v;
   }

   struct BenchmarkResult
   {
      std::string name;
      uint64_t    ops;
      double      elapsed_sec;
      uint64_t    bytes;

      void print() const
      {
         double ops_per_sec   = ops / elapsed_sec;
         double mb_per_sec    = bytes / elapsed_sec / (1024.0 * 1024.0);
         printf("%-24s : %12s ops/sec    %8.1f MB/s    (%7.3f sec)\n",
                name.c_str(), comma_format((uint64_t)ops_per_sec).c_str(),
                mb_per_sec, elapsed_sec);
      }
   };

   using Clock = std::chrono::high_resolution_clock;

   double elapsed(Clock::time_point start)
   {
      return std::chrono::duration<double>(Clock::now() - start).count();
   }

   // ── Config ──

   struct Config
   {
      std::string db_path     = "/tmp/rocks_bench_db";
      int         num         = 1000000;
      int         key_size    = 16;
      int         value_size  = 100;
      int         batch_size  = 1;
      bool        use_sync    = false;
      bool        use_fsync   = false;
      std::string benchmarks  = "fillseq,fillrandom,overwrite,readrandom,"
                                "readseq,readreverse,seekrandom,"
                                "deleteseq,deleterandom";
      int         threads     = 1;
      bool        fresh_db    = true;

      void parse(int argc, char** argv)
      {
         for (int i = 1; i < argc; i++)
         {
            std::string arg = argv[i];
            auto        eq  = arg.find('=');
            if (eq == std::string::npos)
               continue;
            auto flag  = arg.substr(0, eq);
            auto value = arg.substr(eq + 1);

            if (flag == "--db")
               db_path = value;
            else if (flag == "--num")
               num = std::stoi(value);
            else if (flag == "--key_size")
               key_size = std::stoi(value);
            else if (flag == "--value_size")
               value_size = std::stoi(value);
            else if (flag == "--batch_size")
               batch_size = std::stoi(value);
            else if (flag == "--sync")
               use_sync = (value == "1" || value == "true");
            else if (flag == "--use_fsync")
               use_fsync = (value == "1" || value == "true");
            else if (flag == "--benchmarks")
               benchmarks = value;
            else if (flag == "--threads")
               threads = std::stoi(value);
            else if (flag == "--use_existing_db")
               fresh_db = (value != "1" && value != "true");
         }
      }
   };

   // ── Benchmark runners ──

   rocksdb::DB* open_db(const Config& cfg)
   {
      if (cfg.fresh_db)
         rocksdb::DestroyDB(cfg.db_path, {});

      rocksdb::Options options;
      options.create_if_missing = true;
      options.use_fsync         = cfg.use_fsync;

      rocksdb::DB* db = nullptr;
      auto         s  = rocksdb::DB::Open(options, cfg.db_path, &db);
      if (!s.ok())
      {
         fprintf(stderr, "Open failed: %s\n", s.ToString().c_str());
         exit(1);
      }
      return db;
   }

   BenchmarkResult run_fillseq(rocksdb::DB* db, const Config& cfg)
   {
      rocksdb::WriteOptions wo;
      wo.sync = cfg.use_sync;

      auto start = Clock::now();

      if (cfg.batch_size <= 1)
      {
         for (int i = 0; i < cfg.num && !bench::interrupted(); i++)
         {
            auto key = make_key(i, cfg.key_size);
            auto val = make_value(cfg.value_size, i);
            db->Put(wo, key, val);
         }
      }
      else
      {
         for (int i = 0; i < cfg.num; i += cfg.batch_size)
         {
            rocksdb::WriteBatch batch;
            int end = std::min(i + cfg.batch_size, cfg.num);
            for (int j = i; j < end; j++)
            {
               batch.Put(make_key(j, cfg.key_size), make_value(cfg.value_size, j));
            }
            db->Write(wo, &batch);
         }
      }

      return {"fillseq", (uint64_t)cfg.num, elapsed(start),
              (uint64_t)cfg.num * (cfg.key_size + cfg.value_size)};
   }

   BenchmarkResult run_fillrandom(rocksdb::DB* db, const Config& cfg)
   {
      rocksdb::WriteOptions wo;
      wo.sync = cfg.use_sync;

      std::mt19937_64 rng(42);

      // Generate shuffled indices
      std::vector<uint64_t> indices(cfg.num);
      for (int i = 0; i < cfg.num && !bench::interrupted(); i++)
         indices[i] = i;
      std::shuffle(indices.begin(), indices.end(), rng);

      auto start = Clock::now();

      if (cfg.batch_size <= 1)
      {
         for (int i = 0; i < cfg.num && !bench::interrupted(); i++)
         {
            auto key = make_key(indices[i], cfg.key_size);
            auto val = make_value(cfg.value_size, indices[i]);
            db->Put(wo, key, val);
         }
      }
      else
      {
         for (int i = 0; i < cfg.num; i += cfg.batch_size)
         {
            rocksdb::WriteBatch batch;
            int end = std::min(i + cfg.batch_size, cfg.num);
            for (int j = i; j < end; j++)
            {
               batch.Put(make_key(indices[j], cfg.key_size),
                         make_value(cfg.value_size, indices[j]));
            }
            db->Write(wo, &batch);
         }
      }

      return {"fillrandom", (uint64_t)cfg.num, elapsed(start),
              (uint64_t)cfg.num * (cfg.key_size + cfg.value_size)};
   }

   BenchmarkResult run_overwrite(rocksdb::DB* db, const Config& cfg)
   {
      rocksdb::WriteOptions wo;
      wo.sync = cfg.use_sync;

      std::mt19937_64 rng(123);

      auto start = Clock::now();

      for (int i = 0; i < cfg.num && !bench::interrupted(); i++)
      {
         uint64_t k   = rng() % cfg.num;
         auto     key = make_key(k, cfg.key_size);
         auto     val = make_value(cfg.value_size, k + cfg.num);
         db->Put(wo, key, val);
      }

      return {"overwrite", (uint64_t)cfg.num, elapsed(start),
              (uint64_t)cfg.num * (cfg.key_size + cfg.value_size)};
   }

   BenchmarkResult run_readrandom(rocksdb::DB* db, const Config& cfg)
   {
      rocksdb::ReadOptions ro;
      std::mt19937_64      rng(99);
      std::string          value;

      uint64_t found    = 0;
      auto     start    = Clock::now();

      for (int i = 0; i < cfg.num && !bench::interrupted(); i++)
      {
         uint64_t k   = rng() % cfg.num;
         auto     key = make_key(k, cfg.key_size);
         if (db->Get(ro, key, &value).ok())
            found++;
      }

      auto result = BenchmarkResult{"readrandom", (uint64_t)cfg.num, elapsed(start),
                                    found * (cfg.key_size + cfg.value_size)};
      printf("  (%s of %s found)\n", comma_format(found).c_str(),
             comma_format(cfg.num).c_str());
      return result;
   }

   BenchmarkResult run_readseq(rocksdb::DB* db, const Config& cfg)
   {
      rocksdb::ReadOptions ro;

      uint64_t count = 0;
      uint64_t bytes = 0;
      auto     start = Clock::now();

      auto* iter = db->NewIterator(ro);
      for (iter->SeekToFirst(); iter->Valid(); iter->Next())
      {
         count++;
         bytes += iter->key().size() + iter->value().size();
      }
      delete iter;

      return {"readseq", count, elapsed(start), bytes};
   }

   BenchmarkResult run_readreverse(rocksdb::DB* db, const Config& cfg)
   {
      rocksdb::ReadOptions ro;

      uint64_t count = 0;
      uint64_t bytes = 0;
      auto     start = Clock::now();

      auto* iter = db->NewIterator(ro);
      for (iter->SeekToLast(); iter->Valid(); iter->Prev())
      {
         count++;
         bytes += iter->key().size() + iter->value().size();
      }
      delete iter;

      return {"readreverse", count, elapsed(start), bytes};
   }

   BenchmarkResult run_seekrandom(rocksdb::DB* db, const Config& cfg)
   {
      rocksdb::ReadOptions ro;
      std::mt19937_64      rng(77);

      uint64_t count = 0;
      auto     start = Clock::now();

      auto* iter = db->NewIterator(ro);
      for (int i = 0; i < cfg.num && !bench::interrupted(); i++)
      {
         uint64_t k   = rng() % cfg.num;
         auto     key = make_key(k, cfg.key_size);
         iter->Seek(key);
         if (iter->Valid())
            count++;
      }
      delete iter;

      return {"seekrandom", (uint64_t)cfg.num, elapsed(start),
              count * (cfg.key_size + cfg.value_size)};
   }

   BenchmarkResult run_deleteseq(rocksdb::DB* db, const Config& cfg)
   {
      rocksdb::WriteOptions wo;
      wo.sync = cfg.use_sync;

      auto start = Clock::now();

      for (int i = 0; i < cfg.num && !bench::interrupted(); i++)
      {
         auto key = make_key(i, cfg.key_size);
         db->Delete(wo, key);
      }

      return {"deleteseq", (uint64_t)cfg.num, elapsed(start),
              (uint64_t)cfg.num * cfg.key_size};
   }

   BenchmarkResult run_deleterandom(rocksdb::DB* db, const Config& cfg)
   {
      rocksdb::WriteOptions wo;
      wo.sync = cfg.use_sync;
      std::mt19937_64 rng(55);

      auto start = Clock::now();

      for (int i = 0; i < cfg.num && !bench::interrupted(); i++)
      {
         uint64_t k   = rng() % cfg.num;
         auto     key = make_key(k, cfg.key_size);
         db->Delete(wo, key);
      }

      return {"deleterandom", (uint64_t)cfg.num, elapsed(start),
              (uint64_t)cfg.num * cfg.key_size};
   }

   BenchmarkResult run_readwhilewriting(rocksdb::DB* db, const Config& cfg)
   {
      rocksdb::ReadOptions  ro;
      rocksdb::WriteOptions wo;
      wo.sync = cfg.use_sync;

      std::atomic<bool>    stop{false};
      std::atomic<uint64_t> writes{0};

      // Background writer thread
      auto writer = std::thread([&]()
      {
         std::mt19937_64 rng(333);
         while (!stop.load(std::memory_order_relaxed) && !bench::interrupted())
         {
            uint64_t k   = rng() % cfg.num;
            auto     key = make_key(k, cfg.key_size);
            auto     val = make_value(cfg.value_size, k);
            db->Put(wo, key, val);
            writes.fetch_add(1, std::memory_order_relaxed);
         }
      });

      // Reader
      std::mt19937_64 rng(444);
      std::string     value;
      uint64_t        found = 0;

      auto start = Clock::now();

      for (int i = 0; i < cfg.num && !bench::interrupted(); i++)
      {
         uint64_t k   = rng() % cfg.num;
         auto     key = make_key(k, cfg.key_size);
         if (db->Get(ro, key, &value).ok())
            found++;
      }

      stop.store(true);
      writer.join();

      auto result = BenchmarkResult{"readwhilewriting", (uint64_t)cfg.num, elapsed(start),
                                    found * (cfg.key_size + cfg.value_size)};
      printf("  (%s reads found, %s background writes)\n",
             comma_format(found).c_str(), comma_format(writes.load()).c_str());
      return result;
   }

   // Multi-threaded random reads from periodic snapshots while a writer hammers the DB.
   // Each reader thread takes its own snapshot, refreshes every 1000 reads.
   BenchmarkResult run_readwhilewriting_snap(rocksdb::DB* db, const Config& cfg)
   {
      rocksdb::WriteOptions wo;
      wo.sync = cfg.use_sync;

      const int read_threads = std::max(1, cfg.threads);

      std::atomic<bool>     stop{false};
      std::atomic<uint64_t> writes{0};

      // Background writer thread
      auto writer = std::thread([&]()
      {
         std::mt19937_64 rng(333);
         while (!stop.load(std::memory_order_relaxed) && !bench::interrupted())
         {
            uint64_t k   = rng() % cfg.num;
            auto     key = make_key(k, cfg.key_size);
            auto     val = make_value(cfg.value_size, k);
            db->Put(wo, key, val);
            writes.fetch_add(1, std::memory_order_relaxed);
         }
      });

      static constexpr int kSnapRefresh = 1000;
      const int reads_per_thread = cfg.num / read_threads;

      std::atomic<uint64_t> total_found{0};

      auto start = Clock::now();

      std::vector<std::thread> readers;
      for (int t = 0; t < read_threads; t++)
      {
         readers.emplace_back([&, t]()
         {
            std::mt19937_64 rng(444 + t);
            std::string     value;
            uint64_t        found = 0;

            const rocksdb::Snapshot* snap = db->GetSnapshot();
            rocksdb::ReadOptions     ro;
            ro.snapshot = snap;

            for (int i = 0; i < reads_per_thread; i++)
            {
               if (i % kSnapRefresh == 0 && i > 0)
               {
                  db->ReleaseSnapshot(snap);
                  snap        = db->GetSnapshot();
                  ro.snapshot = snap;
               }
               uint64_t k   = rng() % cfg.num;
               auto     key = make_key(k, cfg.key_size);
               if (db->Get(ro, key, &value).ok())
                  found++;
            }

            db->ReleaseSnapshot(snap);
            total_found.fetch_add(found, std::memory_order_relaxed);
         });
      }

      for (auto& r : readers)
         r.join();

      stop.store(true);
      writer.join();

      uint64_t total_reads = (uint64_t)reads_per_thread * read_threads;
      auto result = BenchmarkResult{"readwhilewriting_snap", total_reads, elapsed(start),
                                    total_found.load() * (cfg.key_size + cfg.value_size)};
      printf("  (%s reads found, %s background writes, %d reader threads, snapshot refresh every %d)\n",
             comma_format(total_found.load()).c_str(), comma_format(writes.load()).c_str(),
             read_threads, kSnapRefresh);
      return result;
   }

   // Multi-threaded sequential scan from snapshot while a writer hammers the DB.
   // Each reader thread scans the full keyspace from its own snapshot.
   BenchmarkResult run_scanwhilewriting_snap(rocksdb::DB* db, const Config& cfg)
   {
      rocksdb::WriteOptions wo;
      wo.sync = cfg.use_sync;

      const int scan_threads = std::max(1, cfg.threads);

      std::atomic<bool>     stop{false};
      std::atomic<uint64_t> writes{0};

      // Background writer thread
      auto writer = std::thread([&]()
      {
         std::mt19937_64 rng(333);
         while (!stop.load(std::memory_order_relaxed) && !bench::interrupted())
         {
            uint64_t k   = rng() % cfg.num;
            auto     key = make_key(k, cfg.key_size);
            auto     val = make_value(cfg.value_size, k);
            db->Put(wo, key, val);
            writes.fetch_add(1, std::memory_order_relaxed);
         }
      });

      std::atomic<uint64_t> total_scanned{0};

      auto start = Clock::now();

      std::vector<std::thread> scanners;
      for (int t = 0; t < scan_threads; t++)
      {
         scanners.emplace_back([&]()
         {
            const rocksdb::Snapshot* snap = db->GetSnapshot();
            rocksdb::ReadOptions     ro;
            ro.snapshot = snap;

            auto*    it    = db->NewIterator(ro);
            uint64_t count = 0;
            for (it->SeekToFirst(); it->Valid(); it->Next())
            {
               auto k = it->key();
               auto v = it->value();
               (void)k;
               (void)v;
               count++;
            }
            delete it;

            db->ReleaseSnapshot(snap);
            total_scanned.fetch_add(count, std::memory_order_relaxed);
         });
      }

      for (auto& s : scanners)
         s.join();

      stop.store(true);
      writer.join();

      auto result = BenchmarkResult{"scanwhilewriting_snap", total_scanned.load(), elapsed(start),
                                    total_scanned.load() * (cfg.key_size + cfg.value_size)};
      printf("  (%s keys scanned, %s background writes, %d scanner threads)\n",
             comma_format(total_scanned.load()).c_str(), comma_format(writes.load()).c_str(),
             scan_threads);
      return result;
   }

}  // anonymous namespace

// ── Main ───────────────────────────────────────────────────────────────────

int main(int argc, char** argv)
{
   bench::install_interrupt_handler();
   Config cfg;
   cfg.parse(argc, argv);

#ifdef PSITRIROCKS_BACKEND
   printf("Backend: PsiTriRocks (psitri)\n");
#elif defined(MDBX_BACKEND)
   printf("Backend: MDBX (libmdbx)\n");
#else
   printf("Backend: RocksDB\n");
#endif
   printf("Keys:       %s (%d bytes each)\n", comma_format(cfg.num).c_str(), cfg.key_size);
   printf("Values:     %d bytes each\n", cfg.value_size);
   printf("Batch size: %d\n", cfg.batch_size);
   printf("Sync:       %s\n", cfg.use_sync ? "ON" : "OFF");
   printf("DB path:    %s\n", cfg.db_path.c_str());
   printf("-------------------------------------------------------\n");

   // Parse comma-separated benchmark names
   std::vector<std::string> bench_names;
   {
      std::istringstream ss(cfg.benchmarks);
      std::string        name;
      while (std::getline(ss, name, ','))
         if (!name.empty())
            bench_names.push_back(name);
   }

   rocksdb::DB* db = nullptr;
   std::vector<BenchmarkResult> results;

   for (auto& name : bench_names)
   {
      // Some benchmarks need a fresh DB
      bool needs_fresh = (name == "fillseq" || name == "fillrandom");
      bool needs_data  = (name == "readrandom" || name == "readseq" ||
                         name == "readreverse" || name == "seekrandom" ||
                         name == "overwrite" || name == "deleteseq" ||
                         name == "deleterandom" || name == "readwhilewriting" ||
                         name == "readwhilewriting_snap" || name == "scanwhilewriting_snap");

      if (needs_fresh || db == nullptr)
      {
         if (db)
         {
            delete db;
            db = nullptr;
         }
         db = open_db(cfg);
      }

      // If the benchmark needs data but DB is fresh (from fillrandom/fillseq),
      // and the previous benchmark didn't fill it, prefill
      if (needs_data && results.empty())
      {
         printf("(prefilling %s keys...)\n", comma_format(cfg.num).c_str());
         auto r = run_fillseq(db, cfg);
         r.name = "fillseq (prefill)";
         r.print();
         printf("\n");
      }

      BenchmarkResult r{};
      if (name == "fillseq")
         r = run_fillseq(db, cfg);
      else if (name == "fillrandom")
         r = run_fillrandom(db, cfg);
      else if (name == "overwrite")
         r = run_overwrite(db, cfg);
      else if (name == "readrandom")
         r = run_readrandom(db, cfg);
      else if (name == "readseq")
         r = run_readseq(db, cfg);
      else if (name == "readreverse")
         r = run_readreverse(db, cfg);
      else if (name == "seekrandom")
         r = run_seekrandom(db, cfg);
      else if (name == "deleteseq")
         r = run_deleteseq(db, cfg);
      else if (name == "deleterandom")
         r = run_deleterandom(db, cfg);
      else if (name == "readwhilewriting")
         r = run_readwhilewriting(db, cfg);
      else if (name == "readwhilewriting_snap")
         r = run_readwhilewriting_snap(db, cfg);
      else if (name == "scanwhilewriting_snap")
         r = run_scanwhilewriting_snap(db, cfg);
      else
      {
         fprintf(stderr, "Unknown benchmark: %s\n", name.c_str());
         continue;
      }

      r.print();
      results.push_back(r);
   }

   // Print engine-specific stats before closing
   if (db)
   {
      std::string stats;
      if (db->GetProperty("psitri.stats", &stats))
         printf("PsiTri stats: %s\n", stats.c_str());

      // Compact and truncate to reclaim disk space before measuring DB size
      if (db->GetProperty("psitri.compact_and_truncate", &stats))
         printf("Truncate: %s\n", stats.c_str());
   }

   if (db)
      delete db;

   // Report database size on disk
   {
      uint64_t total_bytes = 0;
      try
      {
         for (auto& entry : std::filesystem::recursive_directory_iterator(cfg.db_path))
         {
            if (entry.is_regular_file())
               total_bytes += entry.file_size();
         }
      }
      catch (...) {}

      if (total_bytes > 0)
      {
         double mb = total_bytes / (1024.0 * 1024.0);
         uint64_t data_bytes = (uint64_t)cfg.num * (cfg.key_size + cfg.value_size);
         double ratio = data_bytes > 0 ? (double)total_bytes / data_bytes : 0;
         printf("DB size:     %.1f MB (%.1fx raw data)\n", mb, ratio);
      }
   }

   printf("-------------------------------------------------------\n");
   printf("Done.\n");

   return 0;
}
