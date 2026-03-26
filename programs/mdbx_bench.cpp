/**
 * MDBX benchmark — mirrors psitri_bench.cpp for apples-to-apples comparison.
 *
 * Covers: insert (seq + random), get (seq + random), upsert, iterate, remove,
 *         remove-rand, lower-bound, get-rand, multithread read+write variants.
 *
 * MDBX is a B+tree with single-writer / multi-reader concurrency.
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
#include <mdbx.h>

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
// MDBX helpers
// ---------------------------------------------------------------------------

#define MDBX_CHECK(expr)                                                        \
   do                                                                           \
   {                                                                            \
      int rc_ = (expr);                                                         \
      if (rc_ != MDBX_SUCCESS)                                                  \
      {                                                                         \
         std::cerr << "mdbx error " << rc_ << " (" << mdbx_strerror(rc_)       \
                   << ") at " << __FILE__ << ":" << __LINE__ << ": " #expr "\n";\
         std::exit(1);                                                          \
      }                                                                         \
   } while (0)

// Sync modes for mdbx
enum class mdbx_sync_mode { none, safe, full };

struct mdbx_guard
{
   MDBX_env* env = nullptr;
   MDBX_dbi  dbi = 0;

   mdbx_guard(const std::string& path, uint64_t map_size_mb, uint32_t max_readers,
              mdbx_sync_mode sync)
   {
      MDBX_CHECK(mdbx_env_create(&env));
      MDBX_CHECK(mdbx_env_set_geometry(env,
                                       1024 * 1024,                     // lower
                                       1024 * 1024,                     // initial
                                       map_size_mb * 1024ULL * 1024ULL, // upper
                                       -1, -1, -1));
      MDBX_CHECK(mdbx_env_set_maxreaders(env, max_readers + 2));

      unsigned sync_flags = 0;
      switch (sync)
      {
         case mdbx_sync_mode::none: sync_flags = MDBX_UTTERLY_NOSYNC; break;
         case mdbx_sync_mode::safe: sync_flags = MDBX_SAFE_NOSYNC | MDBX_NOMETASYNC; break;
         case mdbx_sync_mode::full: sync_flags = MDBX_SYNC_DURABLE; break;
      }
      unsigned flags = sync_flags | MDBX_LIFORECLAIM | MDBX_NOSUBDIR;
      MDBX_CHECK(mdbx_env_open(env, path.c_str(), (MDBX_env_flags_t)flags, 0664));

      // Open default DBI
      MDBX_txn* txn = nullptr;
      MDBX_CHECK(mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)0, &txn));
      MDBX_CHECK(mdbx_dbi_open(txn, nullptr, MDBX_CREATE, &dbi));
      MDBX_CHECK(mdbx_txn_commit(txn));
   }

   ~mdbx_guard()
   {
      if (env)
      {
         mdbx_dbi_close(env, dbi);
         mdbx_env_close(env);
      }
   }
};

// ---------------------------------------------------------------------------
// benchmarks
// ---------------------------------------------------------------------------

static void insert_test(benchmark_config   cfg,
                        mdbx_guard&        mdb,
                        const std::string& name,
                        auto               make_key)
{
   print_header(name, cfg);

   std::vector<char> key;
   std::vector<char> value(cfg.value_size, 'v');
   uint64_t          seq = 0;

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      auto     start    = std::chrono::steady_clock::now();
      uint32_t inserted = 0;

      MDBX_txn* txn = nullptr;
      MDBX_CHECK(mdbx_txn_begin(mdb.env, nullptr, (MDBX_txn_flags_t)0, &txn));

      while (inserted < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq++, key);
            MDBX_val k{key.data(), key.size()};
            MDBX_val v{value.data(), value.size()};
            MDBX_CHECK(mdbx_put(txn, mdb.dbi, &k, &v, MDBX_NOOVERWRITE));
            ++inserted;
         }
      }

      MDBX_CHECK(mdbx_txn_commit(txn));

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   ips  = uint64_t(inserted / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(ips)
                << "  inserts/sec\n";
   }
}

static void upsert_test(benchmark_config   cfg,
                        mdbx_guard&        mdb,
                        const std::string& name,
                        auto               make_key)
{
   print_header(name, cfg);

   std::vector<char> key;
   std::vector<char> value(cfg.value_size, 'u');
   uint64_t          seq = 0;

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      auto     start = std::chrono::steady_clock::now();
      uint32_t count = 0;

      MDBX_txn* txn = nullptr;
      MDBX_CHECK(mdbx_txn_begin(mdb.env, nullptr, (MDBX_txn_flags_t)0, &txn));

      while (count < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - count);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq++, key);
            MDBX_val k{key.data(), key.size()};
            MDBX_val v{value.data(), value.size()};
            MDBX_CHECK(mdbx_put(txn, mdb.dbi, &k, &v, MDBX_UPSERT));
            ++count;
         }
      }

      MDBX_CHECK(mdbx_txn_commit(txn));

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   ips  = uint64_t(count / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(ips)
                << "  upserts/sec\n";
   }
}

static void get_test(benchmark_config   cfg,
                     mdbx_guard&        mdb,
                     const std::string& name,
                     auto               make_key)
{
   print_header(name, cfg);

   std::vector<char> key;

   MDBX_txn* txn = nullptr;
   MDBX_CHECK(mdbx_txn_begin(mdb.env, nullptr, MDBX_TXN_RDONLY, &txn));

   auto     start = std::chrono::steady_clock::now();
   uint64_t found = 0;
   for (uint64_t i = 0; i < uint64_t(cfg.items) * cfg.rounds && !bench::interrupted(); ++i)
   {
      make_key(i, key);
      MDBX_val k{key.data(), key.size()};
      MDBX_val v;
      int      rc = mdbx_get(txn, mdb.dbi, &k, &v);
      if (rc == MDBX_SUCCESS)
         ++found;
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   gps  = uint64_t(found / secs);
   std::cout << format_comma(gps) << " gets/sec  (" << format_comma(found) << " found)\n";

   mdbx_txn_abort(txn);
}

static void iterate_test(benchmark_config cfg, mdbx_guard& mdb)
{
   std::cout << "---------------------  iterate  "
                "--------------------------------------------------\n";

   MDBX_txn* txn = nullptr;
   MDBX_CHECK(mdbx_txn_begin(mdb.env, nullptr, MDBX_TXN_RDONLY, &txn));

   MDBX_cursor* cursor = nullptr;
   MDBX_CHECK(mdbx_cursor_open(txn, mdb.dbi, &cursor));

   MDBX_val key, data;

   auto     start = std::chrono::steady_clock::now();
   uint64_t count = 0;
   int      rc    = mdbx_cursor_get(cursor, &key, &data, MDBX_FIRST);
   while (rc == MDBX_SUCCESS)
   {
      ++count;
      rc = mdbx_cursor_get(cursor, &key, &data, MDBX_NEXT);
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   kps  = uint64_t(count / secs);
   std::cout << format_comma(count) << " keys iterated in " << std::fixed << std::setprecision(3)
             << secs << " sec  (" << format_comma(kps) << " keys/sec)\n";

   mdbx_cursor_close(cursor);
   mdbx_txn_abort(txn);
}

static void remove_test(benchmark_config   cfg,
                        mdbx_guard&        mdb,
                        const std::string& name,
                        auto               make_key)
{
   print_header(name, cfg);

   std::vector<char> key;
   uint64_t          seq = 0;

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      auto     start   = std::chrono::steady_clock::now();
      uint32_t removed = 0;

      MDBX_txn* txn = nullptr;
      MDBX_CHECK(mdbx_txn_begin(mdb.env, nullptr, (MDBX_txn_flags_t)0, &txn));

      while (removed < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - removed);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq++, key);
            MDBX_val k{key.data(), key.size()};
            int      rc = mdbx_del(txn, mdb.dbi, &k, nullptr);
            if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND)
            {
               MDBX_CHECK(rc);
            }
            ++removed;
         }
      }

      MDBX_CHECK(mdbx_txn_commit(txn));

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   rps  = uint64_t(removed / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(rps)
                << "  removes/sec\n";
   }
}

static void remove_rand_test(benchmark_config   cfg,
                             mdbx_guard&        mdb,
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

   std::vector<char> key;
   uint64_t          pos = 0;

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      auto     start   = std::chrono::steady_clock::now();
      uint32_t removed = 0;

      MDBX_txn* txn = nullptr;
      MDBX_CHECK(mdbx_txn_begin(mdb.env, nullptr, (MDBX_txn_flags_t)0, &txn));

      while (removed < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - removed);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(indices[pos++], key);
            MDBX_val k{key.data(), key.size()};
            int      rc = mdbx_del(txn, mdb.dbi, &k, nullptr);
            if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND)
            {
               MDBX_CHECK(rc);
            }
            ++removed;
         }
      }

      MDBX_CHECK(mdbx_txn_commit(txn));

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   rps  = uint64_t(removed / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(pos) << "  " << std::setw(12) << std::right << format_comma(rps)
                << "  removes/sec\n";
   }
}

static void lower_bound_test(benchmark_config   cfg,
                             mdbx_guard&        mdb,
                             const std::string& name,
                             auto               make_key)
{
   print_header(name, cfg);

   std::vector<char> key;

   MDBX_txn* txn = nullptr;
   MDBX_CHECK(mdbx_txn_begin(mdb.env, nullptr, MDBX_TXN_RDONLY, &txn));

   MDBX_cursor* cursor = nullptr;
   MDBX_CHECK(mdbx_cursor_open(txn, mdb.dbi, &cursor));

   auto     start = std::chrono::steady_clock::now();
   uint64_t count = 0;
   for (uint64_t i = 0; i < uint64_t(cfg.items) * cfg.rounds && !bench::interrupted(); ++i)
   {
      make_key(i, key);
      MDBX_val k{key.data(), key.size()};
      MDBX_val v;
      mdbx_cursor_get(cursor, &k, &v, MDBX_SET_RANGE);
      ++count;
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   lps  = uint64_t(count / secs);
   std::cout << format_comma(lps) << " lower_bounds/sec  (" << format_comma(count) << " ops)\n";

   mdbx_cursor_close(cursor);
   mdbx_txn_abort(txn);
}

static void get_rand_test(benchmark_config   cfg,
                          mdbx_guard&        mdb,
                          const std::string& name,
                          auto               make_key)
{
   print_header(name, cfg);

   std::vector<char> key;

   MDBX_txn* txn = nullptr;
   MDBX_CHECK(mdbx_txn_begin(mdb.env, nullptr, MDBX_TXN_RDONLY, &txn));

   auto     start = std::chrono::steady_clock::now();
   uint64_t count = 0;
   uint64_t found = 0;
   for (uint64_t i = 0; i < uint64_t(cfg.items) * cfg.rounds && !bench::interrupted(); ++i)
   {
      make_key(i, key);
      MDBX_val k{key.data(), key.size()};
      MDBX_val v;
      if (mdbx_get(txn, mdb.dbi, &k, &v) == MDBX_SUCCESS)
         ++found;
      ++count;
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   gps  = uint64_t(count / secs);
   std::cout << format_comma(gps) << " gets/sec  (" << format_comma(found) << " found / "
             << format_comma(count) << " ops)\n";

   mdbx_txn_abort(txn);
}

// -- Multi-threaded stress test: concurrent reads while writing --
// read_op: "lower_bound" or "get"
// key_mode: "rand" (random keys, may not exist) or "known" (keys guaranteed to exist)

static void multithread_rw_test(benchmark_config   cfg,
                                mdbx_guard&        mdb,
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
   std::cout << "-----------------------------------------------------------------------\n";

   bool use_get   = (read_op == "get");
   bool use_known = (key_mode == "known");

   // Seed the database so readers start with data
   {
      MDBX_txn* txn = nullptr;
      MDBX_CHECK(mdbx_txn_begin(mdb.env, nullptr, (MDBX_txn_flags_t)0, &txn));
      std::vector<char> key;
      std::vector<char> value(cfg.value_size, 'v');
      for (uint32_t i = 0; i < cfg.items; ++i)
      {
         make_key(i, key);
         MDBX_val k{key.data(), key.size()};
         MDBX_val v{value.data(), value.size()};
         MDBX_CHECK(mdbx_put(txn, mdb.dbi, &k, &v, MDBX_UPSERT));
      }
      MDBX_CHECK(mdbx_txn_commit(txn));
   }
   std::cout << "seeded " << format_comma(cfg.items) << " keys\n";

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
          [&mdb, &done, &counters, &committed_seq, t, &make_key, use_get, use_known]()
          {
             std::vector<char> key;
             int64_t           local_ops       = 0;
             int64_t           local_found     = 0;
             uint32_t          refresh_counter = 0;
             const uint64_t    salt = rand_from_seq(t * 999983ULL + 1);

             // MDBX read-only transactions can be renewed efficiently
             MDBX_txn* txn = nullptr;
             MDBX_CHECK(mdbx_txn_begin(mdb.env, nullptr, MDBX_TXN_RDONLY, &txn));

             MDBX_cursor* cursor = nullptr;
             if (!use_get)
             {
                MDBX_CHECK(mdbx_cursor_open(txn, mdb.dbi, &cursor));
             }

             while (!done.load(std::memory_order_relaxed) && !bench::interrupted())
             {
                // Refresh read snapshot every 10 batches via txn_renew
                if (++refresh_counter >= 10)
                {
                   MDBX_CHECK(mdbx_txn_renew(txn));
                   if (cursor)
                      MDBX_CHECK(mdbx_cursor_renew(txn, cursor));
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
                      MDBX_val k{key.data(), key.size()};
                      MDBX_val v;
                      if (mdbx_get(txn, mdb.dbi, &k, &v) == MDBX_SUCCESS)
                         ++local_found;
                   }
                   else
                   {
                      MDBX_val k{key.data(), key.size()};
                      MDBX_val v;
                      if (mdbx_cursor_get(cursor, &k, &v, MDBX_SET_RANGE) == MDBX_SUCCESS)
                         ++local_found;
                   }
                   ++local_ops;
                }
                counters[t].ops.store(local_ops, std::memory_order_relaxed);
                counters[t].found.store(local_found, std::memory_order_relaxed);
             }

             if (cursor)
                mdbx_cursor_close(cursor);
             mdbx_txn_abort(txn);
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
   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      MDBX_txn* txn = nullptr;
      MDBX_CHECK(mdbx_txn_begin(mdb.env, nullptr, (MDBX_txn_flags_t)0, &txn));

      auto     start    = std::chrono::steady_clock::now();
      uint32_t inserted = 0;
      while (inserted < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq++, key);
            MDBX_val k{key.data(), key.size()};
            MDBX_val v{value.data(), value.size()};
            MDBX_CHECK(mdbx_put(txn, mdb.dbi, &k, &v, MDBX_NOOVERWRITE));
            ++inserted;
         }
      }
      MDBX_CHECK(mdbx_txn_commit(txn));
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

// ---------------------------------------------------------------------------

int main(int argc, char** argv)
{
   bench::install_interrupt_handler();
   uint32_t    rounds;
   uint32_t    batch;
   uint32_t    items;
   uint32_t    value_size;
   uint32_t    threads;
   uint64_t    map_size_mb;
   bool        reset    = false;
   std::string db_dir   = "./mdbxdb";
   std::string bench    = "all";
   std::string sync_str = "none";

   po::options_description desc("mdbx-benchmark options");
   auto                    opt = desc.add_options();
   opt("help,h", "print this message");
   opt("round,r", po::value<uint32_t>(&rounds)->default_value(3), "number of rounds");
   opt("batch,b", po::value<uint32_t>(&batch)->default_value(512), "batch size");
   opt("items,i", po::value<uint32_t>(&items)->default_value(1000000), "number of items per round");
   opt("value-size,s", po::value<uint32_t>(&value_size)->default_value(8), "value size in bytes");
   opt("threads,t", po::value<uint32_t>(&threads)->default_value(4), "number of read threads for multithread test");
   opt("map-size-mb", po::value<uint64_t>(&map_size_mb)->default_value(4096), "max map size in MB");
   opt("db-dir,d", po::value<std::string>(&db_dir)->default_value("./mdbxdb"), "database path");
   opt("bench", po::value<std::string>(&bench)->default_value("all"),
       "benchmark: all, insert, upsert, get, iterate, remove, remove-rand, lower-bound, get-rand, "
       "multithread-lowerbound-rand, multithread-lowerbound-known, "
       "multithread-get-rand, multithread-get-known");
   opt("sync", po::value<std::string>(&sync_str)->default_value("none"),
       "sync mode: none (UTTERLY_NOSYNC), safe (SAFE_NOSYNC), full (SYNC_DURABLE)");
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
   {
      // MDBX with NOSUBDIR uses db_dir as the file itself + lock file
      std::filesystem::remove(db_dir);
      std::filesystem::remove(db_dir + "-lck");
   }

   // Parent directory must exist for MDBX_NOSUBDIR
   auto parent = std::filesystem::path(db_dir).parent_path();
   if (!parent.empty())
      std::filesystem::create_directories(parent);

   mdbx_sync_mode sync = mdbx_sync_mode::none;
   if (sync_str == "safe")
      sync = mdbx_sync_mode::safe;
   else if (sync_str == "full")
      sync = mdbx_sync_mode::full;
   else if (sync_str != "none")
   {
      std::cerr << "invalid --sync mode: " << sync_str << " (use none, safe, full)\n";
      return 1;
   }

   mdbx_guard mdb(db_dir, map_size_mb, threads, sync);

   benchmark_config cfg = {rounds, items, batch, value_size};

   std::cout << "mdbx-benchmark: db=" << db_dir << "\n";
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
      insert_test(cfg, mdb, "big endian seq insert", be_seq_key);
      insert_test(cfg, mdb, "dense random insert", rand_key);
      insert_test(cfg, mdb, "string number rand insert", str_rand_key);
   }

   // -- Get --
   if (run_all || bench == "get")
   {
      get_test(cfg, mdb, "big endian seq get", be_seq_key);
      get_test(cfg, mdb, "dense random get", rand_key);
   }

   // -- Upsert --
   if (run_all || bench == "upsert")
   {
      upsert_test(cfg, mdb, "big endian seq upsert", be_seq_key);
   }

   // -- Iterate --
   if (run_all || bench == "iterate")
   {
      iterate_test(cfg, mdb);
   }

   // -- Lower-bound --
   if (run_all || bench == "lower-bound")
   {
      lower_bound_test(cfg, mdb, "random lower_bound", rand_key);
   }

   // -- Random get --
   if (run_all || bench == "get-rand")
   {
      get_rand_test(cfg, mdb, "random get", rand_key);
   }

   // -- Remove --
   if (run_all || bench == "remove")
   {
      remove_test(cfg, mdb, "big endian seq remove", be_seq_key);
   }

   // -- Random remove of known keys --
   if (run_all || bench == "remove-rand")
   {
      remove_rand_test(cfg, mdb, "random remove (known keys)", rand_key);
   }

   // -- Multithread read+write variants --
   if (run_all || bench == "multithread-lowerbound-rand")
   {
      multithread_rw_test(cfg, mdb, threads, rand_key, "lower_bound", "rand");
   }
   if (run_all || bench == "multithread-lowerbound-known")
   {
      multithread_rw_test(cfg, mdb, threads, rand_key, "lower_bound", "known");
   }
   if (run_all || bench == "multithread-get-rand")
   {
      multithread_rw_test(cfg, mdb, threads, rand_key, "get", "rand");
   }
   if (run_all || bench == "multithread-get-known")
   {
      multithread_rw_test(cfg, mdb, threads, rand_key, "get", "known");
   }

   std::cout << "\ndone.\n";
   return 0;
}
