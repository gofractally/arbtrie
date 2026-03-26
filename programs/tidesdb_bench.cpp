/**
 * TidesDB benchmark — mirrors psitri_bench.cpp for apples-to-apples comparison.
 *
 * Covers: insert (seq + random), get (seq + random), upsert, iterate, remove,
 *         remove-rand, lower-bound.
 *
 * Uses tidesdb_c_wrapper.h to avoid C++ compilation issues with TidesDB's C headers.
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
#include "tidesdb_c_wrapper.h"

namespace po = boost::program_options;

// ---------------------------------------------------------------------------
// helpers shared with psitri_bench
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
static uint8_t          g_random_buf[RANDOM_BUF_SIZE];

static uint64_t g_random_seed = 0xdeadbeefcafebabe;

/// Reshuffle the random buffer with a new seed. Call once per round
/// outside the timing loop to ensure every round has unique values.
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

static const uint8_t* random_value_ptr(uint64_t seq, uint32_t value_size)
{
   size_t offset = rand_from_seq(seq) % (RANDOM_BUF_SIZE - value_size);
   return g_random_buf + offset;
}

static void to_key(uint64_t val, std::vector<uint8_t>& v)
{
   v.resize(sizeof(val));
   memcpy(v.data(), &val, sizeof(val));
}

static void to_key(const std::string& val, std::vector<uint8_t>& v)
{
   v.resize(val.size());
   memcpy(v.data(), val.data(), val.size());
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
// TidesDB RAII guard
// ---------------------------------------------------------------------------

static const char* CF_NAME = "bench";

/// TidesDB limits transactions to 100K ops. Auto-commit helper commits
/// the current txn and opens a new one when the op count reaches the limit.
static constexpr uint32_t TDB_TXN_OP_LIMIT = 90000;  // leave headroom below 100K

static void tdb_auto_commit(tdb_wrapper_txn_t*& txn, tdb_wrapper_t* db, uint32_t& op_count)
{
   if (op_count >= TDB_TXN_OP_LIMIT)
   {
      tdb_txn_commit(txn);
      tdb_txn_free(txn);
      txn = tdb_txn_begin(db);
      op_count = 0;
   }
}

struct tdb_guard
{
   tdb_wrapper_t* db = nullptr;

   tdb_guard(const std::string& path, int sync_mode = TDB_WRAP_SYNC_NONE)
   {
      db = tdb_open(path.c_str(), sync_mode);
      if (!db)
      {
         std::cerr << "tidesdb: failed to open " << path << "\n";
         std::exit(1);
      }
      if (tdb_ensure_cf(db, CF_NAME) != TDB_WRAP_SUCCESS)
      {
         std::cerr << "tidesdb: failed to create column family\n";
         std::exit(1);
      }
   }

   ~tdb_guard()
   {
      if (db)
         tdb_close(db);
   }
};

// ---------------------------------------------------------------------------
// benchmarks
// ---------------------------------------------------------------------------

static void insert_test(benchmark_config   cfg,
                        tdb_guard&         tdb,
                        const std::string& name,
                        auto               make_key)
{
   print_header(name, cfg);

   std::vector<uint8_t> key;
   uint64_t             seq = 0;

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      reshuffle_random_buf();
      auto     start    = std::chrono::steady_clock::now();
      uint32_t inserted = 0;

      auto* txn = tdb_txn_begin(tdb.db);
      if (!txn)
      {
         std::cerr << "txn_begin failed\n";
         return;
      }
      uint32_t op_count = 0;

      while (inserted < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq, key);
            auto* vp = random_value_ptr(seq, cfg.value_size);
            tdb_put(txn, CF_NAME, key.data(), key.size(), vp, cfg.value_size);
            ++seq;
            ++inserted;
            ++op_count;
            tdb_auto_commit(txn, tdb.db, op_count);
         }
      }

      tdb_txn_commit(txn);
      tdb_txn_free(txn);

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   ips  = uint64_t(inserted / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(ips)
                << "  inserts/sec\n";
   }
}

static void upsert_test(benchmark_config   cfg,
                        tdb_guard&         tdb,
                        const std::string& name,
                        auto               make_key)
{
   print_header(name, cfg);

   std::vector<uint8_t> key;
   uint64_t             seq = 0;

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      reshuffle_random_buf();
      auto     start = std::chrono::steady_clock::now();
      uint32_t count = 0;

      auto* txn = tdb_txn_begin(tdb.db);
      if (!txn)
         return;
      uint32_t op_count = 0;

      while (count < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - count);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq, key);
            auto* vp = random_value_ptr(seq, cfg.value_size);
            tdb_put(txn, CF_NAME, key.data(), key.size(), vp, cfg.value_size);
            ++seq;
            ++count;
            ++op_count;
            tdb_auto_commit(txn, tdb.db, op_count);
         }
      }

      tdb_txn_commit(txn);
      tdb_txn_free(txn);

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   ips  = uint64_t(count / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(ips)
                << "  upserts/sec\n";
   }
}

static void get_test(benchmark_config   cfg,
                     tdb_guard&         tdb,
                     const std::string& name,
                     auto               make_key)
{
   print_header(name, cfg);

   std::vector<uint8_t> key;

   auto* txn = tdb_txn_begin(tdb.db);
   if (!txn)
      return;

   auto     start = std::chrono::steady_clock::now();
   uint64_t found = 0;
   for (uint64_t i = 0; i < uint64_t(cfg.items) * cfg.rounds && !bench::interrupted(); ++i)
   {
      make_key(i, key);
      uint8_t* val_out  = nullptr;
      size_t   val_size = 0;
      if (tdb_get(txn, CF_NAME, key.data(), key.size(), &val_out, &val_size) == TDB_WRAP_SUCCESS)
      {
         free(val_out);
         ++found;
      }
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   gps  = uint64_t(found / secs);
   std::cout << format_comma(gps) << " gets/sec  (" << format_comma(found) << " found)\n";

   tdb_txn_free(txn);
}

static void iterate_test(benchmark_config cfg, tdb_guard& tdb)
{
   std::cout << "---------------------  iterate  "
                "--------------------------------------------------\n";

   auto* txn = tdb_txn_begin(tdb.db);
   if (!txn)
      return;

   auto* iter = tdb_iter_new(txn, CF_NAME);
   if (!iter)
   {
      tdb_txn_free(txn);
      return;
   }
   tdb_iter_seek_first(iter);

   auto     start = std::chrono::steady_clock::now();
   uint64_t count = 0;
   while (tdb_iter_valid(iter))
   {
      ++count;
      tdb_iter_next(iter);
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   kps  = uint64_t(count / secs);
   std::cout << format_comma(count) << " keys iterated in " << std::fixed << std::setprecision(3)
             << secs << " sec  (" << format_comma(kps) << " keys/sec)\n";

   tdb_iter_free(iter);
   tdb_txn_free(txn);
}

static void remove_test(benchmark_config   cfg,
                        tdb_guard&         tdb,
                        const std::string& name,
                        auto               make_key)
{
   print_header(name, cfg);

   std::vector<uint8_t> key;
   uint64_t             seq = 0;

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      auto     start   = std::chrono::steady_clock::now();
      uint32_t removed = 0;

      auto* txn = tdb_txn_begin(tdb.db);
      if (!txn)
         return;

      while (removed < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - removed);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq++, key);
            tdb_delete(txn, CF_NAME, key.data(), key.size());
            ++removed;
         }
      }

      tdb_txn_commit(txn);
      tdb_txn_free(txn);

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   rps  = uint64_t(removed / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(rps)
                << "  removes/sec\n";
   }
}

static void remove_rand_test(benchmark_config   cfg,
                             tdb_guard&         tdb,
                             const std::string& name,
                             auto               make_key)
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

   std::vector<uint8_t> key;
   uint64_t             pos = 0;

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      auto     start   = std::chrono::steady_clock::now();
      uint32_t removed = 0;

      auto* txn = tdb_txn_begin(tdb.db);
      if (!txn)
         return;

      while (removed < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - removed);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(indices[pos++], key);
            tdb_delete(txn, CF_NAME, key.data(), key.size());
            ++removed;
         }
      }

      tdb_txn_commit(txn);
      tdb_txn_free(txn);

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   rps  = uint64_t(removed / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(pos) << "  " << std::setw(12) << std::right << format_comma(rps)
                << "  removes/sec\n";
   }
}

static void lower_bound_test(benchmark_config   cfg,
                             tdb_guard&         tdb,
                             const std::string& name,
                             auto               make_key)
{
   print_header(name, cfg);

   std::vector<uint8_t> key;

   auto* txn = tdb_txn_begin(tdb.db);
   if (!txn)
      return;

   auto* iter = tdb_iter_new(txn, CF_NAME);
   if (!iter)
   {
      tdb_txn_free(txn);
      return;
   }

   auto     start = std::chrono::steady_clock::now();
   uint64_t count = 0;
   for (uint64_t i = 0; i < uint64_t(cfg.items) * cfg.rounds && !bench::interrupted(); ++i)
   {
      make_key(i, key);
      tdb_iter_seek(iter, key.data(), key.size());
      ++count;
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   lps  = uint64_t(count / secs);
   std::cout << format_comma(lps) << " lower_bounds/sec  (" << format_comma(count) << " ops)\n";

   tdb_iter_free(iter);
   tdb_txn_free(txn);
}

static void get_rand_test(benchmark_config   cfg,
                          tdb_guard&         tdb,
                          const std::string& name,
                          auto               make_key)
{
   print_header(name, cfg);

   std::vector<uint8_t> key;

   auto* txn = tdb_txn_begin(tdb.db);
   if (!txn)
      return;

   auto     start = std::chrono::steady_clock::now();
   uint64_t count = 0;
   uint64_t found = 0;
   for (uint64_t i = 0; i < uint64_t(cfg.items) * cfg.rounds && !bench::interrupted(); ++i)
   {
      make_key(i, key);
      uint8_t* val_out  = nullptr;
      size_t   val_size = 0;
      if (tdb_get(txn, CF_NAME, key.data(), key.size(), &val_out, &val_size) == TDB_WRAP_SUCCESS)
      {
         free(val_out);
         ++found;
      }
      ++count;
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   gps  = uint64_t(count / secs);
   std::cout << format_comma(gps) << " gets/sec  (" << format_comma(found) << " found / "
             << format_comma(count) << " ops)\n";

   tdb_txn_free(txn);
}

// -- Multi-threaded stress test: concurrent reads while writing --
// read_op: "lower_bound" or "get"
// key_mode: "rand" (random keys, may not exist) or "known" (keys guaranteed to exist)

static void multithread_rw_test(benchmark_config   cfg,
                                tdb_guard&         tdb,
                                uint32_t           num_threads,
                                auto               make_key,
                                const std::string& read_op,
                                const std::string& key_mode)
{
   std::string label = "multithread " + read_op + " (" + key_mode + " keys)";
   std::cout << "---------------------  " << label << "  "
             << std::string(std::max(0, int(52 - int(label.size()))), '-') << "\n";
   std::cout << "write rounds: " << cfg.rounds << "  items: " << format_comma(cfg.items)
             << "  read threads: " << num_threads << "\n";
   std::cout << "-----------------------------------------------------------------------" << std::endl;

   bool use_get   = (read_op == "get");
   bool use_known = (key_mode == "known");

   // Seed the database so readers start with data
   {
      auto* txn = tdb_txn_begin(tdb.db);
      if (!txn)
         return;
      uint32_t op_count = 0;
      std::vector<uint8_t> key;
      for (uint32_t i = 0; i < cfg.items; ++i)
      {
         make_key(i, key);
         auto* vp = random_value_ptr(i, cfg.value_size);
         tdb_put(txn, CF_NAME, key.data(), key.size(), vp, cfg.value_size);
         ++op_count;
         tdb_auto_commit(txn, tdb.db, op_count);
      }
      tdb_txn_commit(txn);
      tdb_txn_free(txn);
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
          [&tdb, &done, &counters, &committed_seq, t, &make_key, use_get, use_known]()
          {
             std::vector<uint8_t> key;
             int64_t              local_ops       = 0;
             int64_t              local_found     = 0;
             uint32_t             refresh_counter = 0;
             // Per-thread salt so threads probe different keys
             const uint64_t       salt = rand_from_seq(t * 999983ULL + 1);

             // Keep transaction (and iterator for seek) alive across batches,
             // refresh every 10 batches to pick up newly committed writes.
             tdb_wrapper_txn_t*  txn  = tdb_txn_begin(tdb.db);
             tdb_wrapper_iter_t* iter = nullptr;
             if (!use_get && txn)
                iter = tdb_iter_new(txn, CF_NAME);

             while (!done.load(std::memory_order_relaxed))
             {
                if (!txn)
                   break;

                // Refresh transaction every 10 batches
                if (++refresh_counter >= 10)
                {
                   if (iter)
                      tdb_iter_free(iter);
                   tdb_txn_free(txn);
                   txn = tdb_txn_begin(tdb.db);
                   iter = nullptr;
                   if (!use_get && txn)
                      iter = tdb_iter_new(txn, CF_NAME);
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
                      uint8_t* val_out  = nullptr;
                      size_t   val_size = 0;
                      if (tdb_get(txn, CF_NAME, key.data(), key.size(),
                                  &val_out, &val_size) == TDB_WRAP_SUCCESS)
                      {
                         free(val_out);
                         ++local_found;
                      }
                   }
                   else
                   {
                      if (iter && tdb_iter_seek(iter, key.data(), key.size()) == TDB_WRAP_SUCCESS)
                         ++local_found;
                   }
                   ++local_ops;
                }
                counters[t].ops.store(local_ops, std::memory_order_relaxed);
                counters[t].found.store(local_found, std::memory_order_relaxed);
             }

             if (iter)
                tdb_iter_free(iter);
             if (txn)
                tdb_txn_free(txn);
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

   std::vector<uint8_t> key;
   uint64_t             seq = cfg.items;

   int64_t prev_ops = 0;
   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      reshuffle_random_buf();
      auto* txn = tdb_txn_begin(tdb.db);
      if (!txn)
         break;
      uint32_t op_count = 0;

      auto     start    = std::chrono::steady_clock::now();
      uint32_t inserted = 0;
      while (inserted < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq, key);
            auto* vp = random_value_ptr(seq, cfg.value_size);
            tdb_put(txn, CF_NAME, key.data(), key.size(), vp, cfg.value_size);
            ++seq;
            ++inserted;
            ++op_count;
            tdb_auto_commit(txn, tdb.db, op_count);
         }
      }
      tdb_txn_commit(txn);
      tdb_txn_free(txn);
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
                << "  " << read_op << "s/sec" << std::endl;
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
   std::string db_dir = "./tidesdb_benchdb";
   std::string bench  = "all";
   std::string sync_str = "none";

   po::options_description desc("tidesdb-benchmark options");
   auto                    opt = desc.add_options();
   opt("help,h", "print this message");
   opt("round,r", po::value<uint32_t>(&rounds)->default_value(3), "number of rounds");
   opt("batch,b", po::value<uint32_t>(&batch)->default_value(512), "batch size");
   opt("items,i", po::value<uint32_t>(&items)->default_value(1000000), "number of items per round");
   opt("value-size,s", po::value<uint32_t>(&value_size)->default_value(8), "value size in bytes");
   opt("threads,t", po::value<uint32_t>(&threads)->default_value(4), "number of read threads for multithread test");
   opt("db-dir,d", po::value<std::string>(&db_dir)->default_value("./tidesdb_benchdb"), "database dir");
   opt("bench", po::value<std::string>(&bench)->default_value("all"),
       "benchmark: all, insert, upsert, get, iterate, remove, remove-rand, lower-bound, get-rand, "
       "multithread-lowerbound-rand, multithread-lowerbound-known, "
       "multithread-get-rand, multithread-get-known");
   opt("reset", po::bool_switch(&reset), "reset database before running");
   opt("sync", po::value<std::string>(&sync_str)->default_value("none"),
       "sync mode: none, safe, full");

   po::variables_map vm;
   po::store(po::parse_command_line(argc, argv, desc), vm);
   po::notify(vm);

   if (vm.count("help"))
   {
      std::cout << desc << "\n";
      return 0;
   }

   if (reset)
   {
      std::filesystem::remove_all(db_dir);
   }

   std::filesystem::create_directories(db_dir);

   int sync_mode = TDB_WRAP_SYNC_NONE;
   if (sync_str == "safe")
      sync_mode = TDB_WRAP_SYNC_SAFE;
   else if (sync_str == "full")
      sync_mode = TDB_WRAP_SYNC_FULL;
   else if (sync_str != "none")
   {
      std::cerr << "invalid --sync mode: " << sync_str << " (use none, safe, full)\n";
      return 1;
   }

   tdb_guard tdb(db_dir, sync_mode);

   benchmark_config cfg = {rounds, items, batch, value_size};

   std::cout << "tidesdb-benchmark: db=" << db_dir << "\n";
   std::cout << "rounds=" << rounds << " items=" << format_comma(items)
             << " batch=" << batch << " value_size=" << value_size
             << " sync=" << sync_str << "\n\n";

   auto run_all      = (bench == "all");
   auto be_seq_key   = [](uint64_t seq, auto& v) { to_key(to_big_endian(seq), v); };
   auto rand_key     = [](uint64_t seq, auto& v) { to_key(rand_from_seq(seq), v); };
   auto str_rand_key = [](uint64_t seq, auto& v) { to_key(std::to_string(rand_from_seq(seq)), v); };

   // -- Insert --
   if (run_all || bench == "insert")
   {
      insert_test(cfg, tdb, "big endian seq insert", be_seq_key);
      insert_test(cfg, tdb, "dense random insert", rand_key);
      insert_test(cfg, tdb, "string number rand insert", str_rand_key);
   }

   // -- Get --
   if (run_all || bench == "get")
   {
      get_test(cfg, tdb, "big endian seq get", be_seq_key);
      get_test(cfg, tdb, "dense random get", rand_key);
   }

   // -- Upsert --
   if (run_all || bench == "upsert")
   {
      upsert_test(cfg, tdb, "big endian seq upsert", be_seq_key);
   }

   // -- Iterate --
   if (run_all || bench == "iterate")
   {
      iterate_test(cfg, tdb);
   }

   // -- Lower-bound --
   if (run_all || bench == "lower-bound")
   {
      lower_bound_test(cfg, tdb, "random lower_bound", rand_key);
   }

   // -- Random get --
   if (run_all || bench == "get-rand")
   {
      get_rand_test(cfg, tdb, "random get", rand_key);
   }

   // -- Remove --
   if (run_all || bench == "remove")
   {
      remove_test(cfg, tdb, "big endian seq remove", be_seq_key);
   }

   // -- Random remove of known keys --
   if (run_all || bench == "remove-rand")
   {
      remove_rand_test(cfg, tdb, "random remove (known keys)", rand_key);
   }

   // -- Multithread read+write variants --
   if (run_all || bench == "multithread-lowerbound-rand")
   {
      multithread_rw_test(cfg, tdb, threads, rand_key, "lower_bound", "rand");
   }
   if (run_all || bench == "multithread-lowerbound-known")
   {
      multithread_rw_test(cfg, tdb, threads, rand_key, "lower_bound", "known");
   }
   if (run_all || bench == "multithread-get-rand")
   {
      multithread_rw_test(cfg, tdb, threads, rand_key, "get", "rand");
   }
   if (run_all || bench == "multithread-get-known")
   {
      multithread_rw_test(cfg, tdb, threads, rand_key, "get", "known");
   }

   std::cout << "\ndone.\n";
   return 0;
}
