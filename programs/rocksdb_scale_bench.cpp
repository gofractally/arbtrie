/**
 * RocksDB scale benchmark — mirrors psitri_bench.cpp / tidesdb_bench.cpp
 * for apples-to-apples comparison.
 *
 * Covers: insert (seq + random), get (seq + random), upsert, iterate, remove,
 *         remove-rand, lower-bound, get-rand, multithread read+write.
 *
 * Uses the native RocksDB C++ API.
 */

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <thread>
#include <vector>

#include "bench_signal.hpp"

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include <hash/xxhash.h>

#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

namespace po = boost::program_options;

// ---------------------------------------------------------------------------
// helpers shared with psitri_bench / tidesdb_bench
// ---------------------------------------------------------------------------

struct benchmark_config
{
   uint32_t rounds;
   uint32_t items      = 1000000;
   uint32_t batch_size = 512;
   uint32_t value_size = 8;
};

static int64_t rand_from_seq(uint64_t seq)
{
   return XXH3_64bits((char*)&seq, sizeof(seq));
}

/// 1MB buffer of pseudo-random bytes. Values are sliced from random offsets.
static constexpr size_t RANDOM_BUF_SIZE = 1 << 20;  // 1 MB
static char             g_random_buf[RANDOM_BUF_SIZE];

static uint64_t g_random_seed = 0xdeadbeefcafebabe;

static void reshuffle_random_buf()
{
   auto* p = reinterpret_cast<uint64_t*>(g_random_buf);
   for (size_t i = 0; i < RANDOM_BUF_SIZE / 8; ++i)
   {
      g_random_seed ^= g_random_seed << 13;
      g_random_seed ^= g_random_seed >> 7;
      g_random_seed ^= g_random_seed << 17;
      p[i] = g_random_seed;
   }
}

static rocksdb::Slice random_value(uint64_t seq, uint32_t value_size)
{
   size_t offset = rand_from_seq(seq) % (RANDOM_BUF_SIZE - value_size);
   return rocksdb::Slice(g_random_buf + offset, value_size);
}

static void to_key(uint64_t val, std::string& v)
{
   v.resize(sizeof(val));
   memcpy(v.data(), &val, sizeof(val));
}

static void to_key(const std::string& val, std::string& v)
{
   v = val;
}

static uint64_t to_big_endian(uint64_t x)
{
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
   return __builtin_bswap64(x);
#else
   return x;
#endif
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

// ---------------------------------------------------------------------------
// RocksDB helpers
// ---------------------------------------------------------------------------

static uint64_t db_size_on_disk(const std::string& path)
{
   uint64_t total = 0;
   try
   {
      for (auto& entry : std::filesystem::recursive_directory_iterator(path))
         if (entry.is_regular_file())
            total += entry.file_size();
   }
   catch (...) {}
   return total;
}

// ---------------------------------------------------------------------------
// benchmarks
// ---------------------------------------------------------------------------

static void insert_test(benchmark_config    cfg,
                        rocksdb::DB*        db,
                        const std::string&  name,
                        auto                make_key)
{
   print_header(name, cfg);

   std::string key;
   uint64_t    seq = 0;

   rocksdb::WriteOptions wo;

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      reshuffle_random_buf();
      auto     start    = std::chrono::steady_clock::now();
      uint32_t inserted = 0;

      while (inserted < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
         rocksdb::WriteBatch wb;
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq, key);
            wb.Put(key, random_value(seq, cfg.value_size));
            ++seq;
            ++inserted;
         }
         db->Write(wo, &wb);
      }

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   ips  = uint64_t(inserted / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(ips)
                << "  inserts/sec" << std::endl;
   }
}

static void upsert_test(benchmark_config    cfg,
                        rocksdb::DB*        db,
                        const std::string&  name,
                        auto                make_key)
{
   print_header(name, cfg);

   std::string key;
   uint64_t    seq = 0;

   rocksdb::WriteOptions wo;

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      reshuffle_random_buf();
      auto     start = std::chrono::steady_clock::now();
      uint32_t count = 0;

      while (count < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - count);
         rocksdb::WriteBatch wb;
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq, key);
            wb.Put(key, random_value(seq, cfg.value_size));
            ++seq;
            ++count;
         }
         db->Write(wo, &wb);
      }

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   ips  = uint64_t(count / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(ips)
                << "  upserts/sec" << std::endl;
   }
}

static void get_test(benchmark_config    cfg,
                     rocksdb::DB*        db,
                     const std::string&  name,
                     auto                make_key)
{
   print_header(name, cfg);

   std::string key;
   std::string value;

   rocksdb::ReadOptions ro;

   auto     start = std::chrono::steady_clock::now();
   uint64_t found = 0;
   for (uint64_t i = 0; i < uint64_t(cfg.items) * cfg.rounds && !bench::interrupted(); ++i)
   {
      make_key(i, key);
      if (db->Get(ro, key, &value).ok())
         ++found;
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   gps  = uint64_t(found / secs);
   std::cout << format_comma(gps) << " gets/sec  (" << format_comma(found) << " found)" << std::endl;
}

static void iterate_test(benchmark_config cfg, rocksdb::DB* db)
{
   std::cout << "---------------------  iterate  "
                "--------------------------------------------------\n";

   rocksdb::ReadOptions ro;
   auto* iter = db->NewIterator(ro);

   auto     start = std::chrono::steady_clock::now();
   uint64_t count = 0;
   for (iter->SeekToFirst(); iter->Valid(); iter->Next())
   {
      ++count;
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   kps  = uint64_t(count / secs);
   std::cout << format_comma(count) << " keys iterated in " << std::fixed << std::setprecision(3)
             << secs << " sec  (" << format_comma(kps) << " keys/sec)" << std::endl;

   delete iter;
}

static void remove_test(benchmark_config    cfg,
                        rocksdb::DB*        db,
                        const std::string&  name,
                        auto                make_key)
{
   print_header(name, cfg);

   std::string key;
   uint64_t    seq = 0;

   rocksdb::WriteOptions wo;

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      auto     start   = std::chrono::steady_clock::now();
      uint32_t removed = 0;

      while (removed < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - removed);
         rocksdb::WriteBatch wb;
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq++, key);
            wb.Delete(key);
            ++removed;
         }
         db->Write(wo, &wb);
      }

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   rps  = uint64_t(removed / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(rps)
                << "  removes/sec" << std::endl;
   }
}

static void remove_rand_test(benchmark_config    cfg,
                             rocksdb::DB*        db,
                             const std::string&  name,
                             auto                make_key)
{
   print_header(name, cfg);

   uint64_t              total = uint64_t(cfg.items) * cfg.rounds;
   std::vector<uint64_t> indices(total);
   for (uint64_t i = 0; i < total && !bench::interrupted(); ++i)
      indices[i] = i;
   for (uint64_t i = total - 1; i > 0; --i)
   {
      uint64_t j = rand_from_seq(i) % (i + 1);
      std::swap(indices[i], indices[j]);
   }

   std::string key;
   uint64_t    pos = 0;

   rocksdb::WriteOptions wo;

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      auto     start   = std::chrono::steady_clock::now();
      uint32_t removed = 0;

      while (removed < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - removed);
         rocksdb::WriteBatch wb;
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(indices[pos++], key);
            wb.Delete(key);
            ++removed;
         }
         db->Write(wo, &wb);
      }

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   rps  = uint64_t(removed / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(pos) << "  " << std::setw(12) << std::right << format_comma(rps)
                << "  removes/sec" << std::endl;
   }
}

static void lower_bound_test(benchmark_config    cfg,
                             rocksdb::DB*        db,
                             const std::string&  name,
                             auto                make_key)
{
   print_header(name, cfg);

   std::string key;

   rocksdb::ReadOptions ro;
   auto* iter = db->NewIterator(ro);

   auto     start = std::chrono::steady_clock::now();
   uint64_t count = 0;
   for (uint64_t i = 0; i < uint64_t(cfg.items) * cfg.rounds && !bench::interrupted(); ++i)
   {
      make_key(i, key);
      iter->Seek(key);
      ++count;
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   lps  = uint64_t(count / secs);
   std::cout << format_comma(lps) << " lower_bounds/sec  (" << format_comma(count) << " ops)"
             << std::endl;

   delete iter;
}

static void get_rand_test(benchmark_config    cfg,
                          rocksdb::DB*        db,
                          const std::string&  name,
                          auto                make_key)
{
   print_header(name, cfg);

   std::string key;
   std::string value;

   rocksdb::ReadOptions ro;

   auto     start = std::chrono::steady_clock::now();
   uint64_t count = 0;
   uint64_t found = 0;
   for (uint64_t i = 0; i < uint64_t(cfg.items) * cfg.rounds && !bench::interrupted(); ++i)
   {
      make_key(i, key);
      if (db->Get(ro, key, &value).ok())
         ++found;
      ++count;
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   gps  = uint64_t(count / secs);
   std::cout << format_comma(gps) << " gets/sec  (" << format_comma(found) << " found / "
             << format_comma(count) << " ops)" << std::endl;
}

// -- Multi-threaded stress test: concurrent reads while writing --

static void multithread_rw_test(benchmark_config    cfg,
                                rocksdb::DB*        db,
                                uint32_t            num_threads,
                                auto                make_key,
                                const std::string&  read_op,
                                const std::string&  key_mode,
                                const std::string&  db_path)
{
   std::string label = "multithread " + read_op + " (" + key_mode + " keys)";
   std::cout << "---------------------  " << label << "  "
             << std::string(std::max(0, int(52 - int(label.size()))), '-') << "\n";
   std::cout << "write rounds: " << cfg.rounds << "  items: " << format_comma(cfg.items)
             << "  read threads: " << num_threads << "\n";
   std::cout << "-----------------------------------------------------------------------" << std::endl;

   bool use_get   = (read_op == "get");
   bool use_known = (key_mode == "known");

   rocksdb::WriteOptions wo;

   // Seed the database so readers start with data
   {
      reshuffle_random_buf();
      std::string key;
      for (uint32_t i = 0; i < cfg.items;)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - i);
         rocksdb::WriteBatch wb;
         for (uint32_t j = 0; j < batch; ++j)
         {
            make_key(i + j, key);
            wb.Put(key, random_value(i + j, cfg.value_size));
         }
         db->Write(wo, &wb);
         i += batch;
      }
   }
   std::cout << "seeded " << format_comma(cfg.items) << " keys" << std::endl;

   struct alignas(128) padded_counters
   {
      std::atomic<int64_t> ops{0};
      std::atomic<int64_t> found{0};
   };

   std::atomic<uint64_t>        committed_seq{cfg.items};
   std::atomic<bool>            done{false};
   std::vector<padded_counters> counters(num_threads);

   // Launch reader threads
   std::vector<std::thread> readers;
   readers.reserve(num_threads);
   for (uint32_t t = 0; t < num_threads; ++t)
   {
      readers.emplace_back(
          [db, &done, &counters, &committed_seq, t, &make_key, use_get, use_known]()
          {
             std::string key;
             std::string value;
             int64_t     local_ops   = 0;
             int64_t     local_found = 0;
             uint32_t    snap_counter = 0;
             const uint64_t salt = rand_from_seq(t * 999983ULL + 1);

             // Use snapshots refreshed periodically
             const rocksdb::Snapshot* snap = db->GetSnapshot();
             rocksdb::ReadOptions ro;
             ro.snapshot = snap;

             rocksdb::Iterator* iter = nullptr;
             if (!use_get)
                iter = db->NewIterator(ro);

             while (!done.load(std::memory_order_relaxed))
             {
                // Refresh snapshot every 10 batches
                if (++snap_counter >= 10)
                {
                   if (iter)
                   {
                      delete iter;
                      iter = nullptr;
                   }
                   db->ReleaseSnapshot(snap);
                   snap = db->GetSnapshot();
                   ro.snapshot = snap;
                   if (!use_get)
                      iter = db->NewIterator(ro);
                   snap_counter = 0;
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
                      if (db->Get(ro, key, &value).ok())
                         ++local_found;
                   }
                   else
                   {
                      iter->Seek(key);
                      if (iter->Valid())
                         ++local_found;
                   }
                   ++local_ops;
                }
                counters[t].ops.store(local_ops, std::memory_order_relaxed);
                counters[t].found.store(local_found, std::memory_order_relaxed);
             }

             if (iter)
                delete iter;
             db->ReleaseSnapshot(snap);
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

   std::string key;
   uint64_t    seq = cfg.items;

   int64_t prev_ops = 0;
   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      reshuffle_random_buf();
      auto     start    = std::chrono::steady_clock::now();
      uint32_t inserted = 0;

      while (inserted < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
         rocksdb::WriteBatch wb;
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq, key);
            wb.Put(key, random_value(seq, cfg.value_size));
            ++seq;
            ++inserted;
         }
         db->Write(wo, &wb);
      }
      committed_seq.store(seq, std::memory_order_relaxed);

      auto   end       = std::chrono::steady_clock::now();
      double secs      = std::chrono::duration<double>(end - start).count();
      auto   ips       = uint64_t(inserted / secs);
      auto   cur_ops   = sum_ops();
      auto   round_ops = cur_ops - prev_ops;
      auto   rps       = uint64_t(round_ops / secs);
      prev_ops         = cur_ops;

      // DB size
      auto   dbsz    = db_size_on_disk(db_path);
      double dbsz_gb = dbsz / (1024.0 * 1024.0 * 1024.0);

      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(ips)
                << "  inserts/sec  " << std::setw(12) << std::right << format_comma(rps)
                << "  " << read_op << "s/sec  "
                << std::fixed << std::setprecision(2) << dbsz_gb << " GB"
                << std::endl;
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
             << format_comma(uint64_t(final_ops / overall_secs)) << "/sec" << std::endl;
}

// ---------------------------------------------------------------------------

int main(int argc, char** argv)
{
   bench::install_interrupt_handler();
   reshuffle_random_buf();

   uint32_t    rounds;
   uint32_t    batch;
   uint32_t    items;
   uint32_t    value_size;
   uint32_t    threads;
   bool        reset  = false;
   std::string db_dir = "./rocksdb_benchdb";
   std::string bench  = "all";

   po::options_description desc("rocksdb-scale-benchmark options");
   auto                    opt = desc.add_options();
   opt("help,h", "print this message");
   opt("round,r", po::value<uint32_t>(&rounds)->default_value(3), "number of rounds");
   opt("batch,b", po::value<uint32_t>(&batch)->default_value(512), "batch size");
   opt("items,i", po::value<uint32_t>(&items)->default_value(1000000), "number of items per round");
   opt("value-size,s", po::value<uint32_t>(&value_size)->default_value(8), "value size in bytes");
   opt("threads,t", po::value<uint32_t>(&threads)->default_value(4), "number of read threads for multithread test");
   opt("db-dir,d", po::value<std::string>(&db_dir)->default_value("./rocksdb_benchdb"), "database dir");
   opt("bench", po::value<std::string>(&bench)->default_value("all"),
       "benchmark: all, insert, upsert, get, iterate, remove, remove-rand, lower-bound, get-rand, "
       "multithread-lowerbound-rand, multithread-lowerbound-known, "
       "multithread-get-rand, multithread-get-known");
   opt("reset", po::bool_switch(&reset), "reset database before running");

   po::variables_map vm;
   po::store(po::parse_command_line(argc, argv, desc), vm);
   po::notify(vm);

   if (vm.count("help"))
   {
      std::cout << desc << "\n";
      return 0;
   }

   if (reset)
      std::filesystem::remove_all(db_dir);

   // Open RocksDB
   rocksdb::Options options;
   options.create_if_missing = true;

   rocksdb::DB* db = nullptr;
   auto         s  = rocksdb::DB::Open(options, db_dir, &db);
   if (!s.ok())
   {
      std::cerr << "RocksDB open failed: " << s.ToString() << "\n";
      return 1;
   }

   benchmark_config cfg = {rounds, items, batch, value_size};

   std::cout << "rocksdb-scale-benchmark: db=" << db_dir << "\n";
   std::cout << "rounds=" << rounds << " items=" << format_comma(items)
             << " batch=" << batch << " value_size=" << value_size << "\n\n";

   auto run_all      = (bench == "all");
   auto be_seq_key   = [](uint64_t seq, auto& v) { to_key(to_big_endian(seq), v); };
   auto rand_key     = [](uint64_t seq, auto& v) { to_key(rand_from_seq(seq), v); };
   auto str_rand_key = [](uint64_t seq, auto& v) { to_key(std::to_string(rand_from_seq(seq)), v); };

   auto should_run = [&](const std::string& name) {
      return !bench::interrupted() && (run_all || bench == name);
   };

   // -- Insert --
   if (should_run("insert"))
   {
      insert_test(cfg, db, "big endian seq insert", be_seq_key);
      insert_test(cfg, db, "dense random insert", rand_key);
      insert_test(cfg, db, "string number rand insert", str_rand_key);
   }
   if (should_run("insert-rand"))
      insert_test(cfg, db, "dense random insert", rand_key);

   // -- Get --
   if (should_run("get"))
   {
      get_test(cfg, db, "big endian seq get", be_seq_key);
      get_test(cfg, db, "dense random get", rand_key);
   }

   // -- Upsert --
   if (should_run("upsert"))
      upsert_test(cfg, db, "big endian seq upsert", be_seq_key);
   if (should_run("upsert-rand"))
      upsert_test(cfg, db, "dense random upsert", rand_key);

   // -- Iterate --
   if (should_run("iterate"))
      iterate_test(cfg, db);

   // -- Lower-bound --
   if (should_run("lower-bound"))
      lower_bound_test(cfg, db, "random lower_bound", rand_key);

   // -- Random get --
   if (should_run("get-rand"))
      get_rand_test(cfg, db, "random get", rand_key);

   // -- Remove --
   if (should_run("remove"))
      remove_test(cfg, db, "big endian seq remove", be_seq_key);

   // -- Random remove of known keys --
   if (should_run("remove-rand"))
      remove_rand_test(cfg, db, "random remove (known keys)", rand_key);

   // -- Multithread read+write variants --
   if (should_run("multithread-lowerbound-rand"))
      multithread_rw_test(cfg, db, threads, rand_key, "lower_bound", "rand", db_dir);
   if (should_run("multithread-lowerbound-known"))
      multithread_rw_test(cfg, db, threads, rand_key, "lower_bound", "known", db_dir);
   if (should_run("multithread-get-rand"))
      multithread_rw_test(cfg, db, threads, rand_key, "get", "rand", db_dir);
   if (should_run("multithread-get-known"))
      multithread_rw_test(cfg, db, threads, rand_key, "get", "known", db_dir);

   // Report final DB size
   auto dbsz = db_size_on_disk(db_dir);
   std::cout << "\nDB size on disk: " << std::fixed << std::setprecision(2)
             << dbsz / (1024.0 * 1024.0 * 1024.0) << " GB" << std::endl;

   delete db;

   if (bench::interrupted())
      std::cout << "\nInterrupted — exiting gracefully.\n";
   else
      std::cout << "\ndone.\n";
   return 0;
}
