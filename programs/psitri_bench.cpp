#include <atomic>
#include <chrono>
#include <csignal>
#include <execinfo.h>
#include <iomanip>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <vector>

#include "bench_signal.hpp"

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include <hash/xxhash.h>
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>
#include <psitri/tree_ops.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>

namespace po = boost::program_options;
using namespace psitri;

struct benchmark_config
{
   uint32_t       rounds;
   uint32_t       items      = 1000000;
   uint32_t       batch_size = 512;
   uint32_t       value_size = 8;
   sal::sync_type sync       = sal::sync_type::none;
   bool           validate   = false;
};

int64_t rand_from_seq(uint64_t seq)
{
   return XXH3_64bits((char*)&seq, sizeof(seq));
}

void to_key(uint64_t val, std::vector<char>& v)
{
   v.resize(sizeof(val));
   memcpy(v.data(), &val, sizeof(val));
}

void to_key(const std::string& val, std::vector<char>& v)
{
   v.resize(val.size());
   memcpy(v.data(), val.data(), val.size());
}

uint64_t to_big_endian(uint64_t x)
{
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
   return __builtin_bswap64(x);
#else
   return x;
#endif
}

std::string format_comma(uint64_t s)
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

void print_header(const std::string& name, const benchmark_config& cfg)
{
   std::cout << "---------------------  " << name
             << "  --------------------------------------------------\n";
   std::cout << "rounds: " << cfg.rounds << "  items: " << format_comma(cfg.items)
             << " batch: " << format_comma(cfg.batch_size) << "\n";
   std::cout << "-----------------------------------------------------------------------\n";
}

void print_stats(write_session& ses, uint32_t root_index = 0)
{
   auto root = ses.get_root(root_index);
   if (!root)
   {
      std::cout << "(empty tree)\n";
      return;
   }
   auto  start = std::chrono::steady_clock::now();
   auto  cur   = ses.create_write_cursor(root);
   auto  s     = cur->get_stats();
   auto  end   = std::chrono::steady_clock::now();
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

void validate_tree(write_session& ses, uint32_t root_index = 0)
{
   auto root = ses.get_root(root_index);
   if (!root)
      return;
   tree_context ctx(root);
   auto         s = ctx.get_stats();
   std::cout << "  validate: " << format_comma(s.total_nodes()) << " nodes, "
             << format_comma(s.total_keys) << " keys OK\n";
}

// -- Mutation benchmarks --

void insert_test(benchmark_config cfg,
                 write_session&   ses,
                 const std::string& name,
                 auto             make_key)
{
   print_header(name, cfg);

   std::vector<char> key;
   std::vector<char> value(cfg.value_size, 'v');
   value_view        vv(value.data(), value.size());
   uint64_t          seq = 0;

   auto tx = ses.start_transaction(0);

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      auto     start    = std::chrono::steady_clock::now();
      uint32_t inserted = 0;
      while (inserted < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq++, key);
            tx.insert(key_view(key.data(), key.size()), vv);
            ++inserted;
         }
      }

      auto   end    = std::chrono::steady_clock::now();
      double secs   = std::chrono::duration<double>(end - start).count();
      auto   ips    = uint64_t(inserted / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(ips)
                << "  inserts/sec\n";
      if (cfg.validate)
         validate_tree(ses);
   }
   if (!bench::interrupted())
      tx.commit();
}

void upsert_test(benchmark_config   cfg,
                 write_session&     ses,
                 const std::string& name,
                 auto               make_key)
{
   print_header(name, cfg);

   std::vector<char> key;
   std::vector<char> value(cfg.value_size, 'u');
   value_view        vv(value.data(), value.size());
   uint64_t          seq = 0;

   auto tx = ses.start_transaction(0);

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      auto     start    = std::chrono::steady_clock::now();
      uint32_t count    = 0;
      while (count < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - count);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq++, key);
            tx.upsert(key_view(key.data(), key.size()), vv);
            ++count;
         }
      }

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   ips  = uint64_t(count / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(ips)
                << "  upserts/sec\n";
   }
   if (!bench::interrupted())
      tx.commit();
}

// -- Get benchmark --

void get_test(benchmark_config   cfg,
              write_session&     ses,
              const std::string& name,
              auto               make_key)
{
   print_header(name, cfg);

   std::vector<char> key;
   std::string       buf;
   auto              root = ses.get_root(0);
   cursor            cur(root);

   auto     start = std::chrono::steady_clock::now();
   uint64_t found = 0;
   for (uint64_t i = 0; i < uint64_t(cfg.items) * cfg.rounds && !bench::interrupted(); ++i)
   {
      make_key(i, key);
      auto result = cur.get(key_view(key.data(), key.size()), &buf);
      if (result >= 0)
         ++found;
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   gps  = uint64_t(found / secs);
   std::cout << format_comma(gps) << " gets/sec  (" << format_comma(found) << " found)\n";
}

// -- Iterate benchmark --

void iterate_test(benchmark_config cfg, write_session& ses)
{
   std::cout << "---------------------  iterate  "
                "--------------------------------------------------\n";

   auto     root  = ses.get_root(0);
   cursor   cur(root);

   auto     start = std::chrono::steady_clock::now();
   uint64_t count = 0;
   cur.seek_begin();
   while (!cur.is_end() && !bench::interrupted())
   {
      ++count;
      cur.next();
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   kps  = uint64_t(count / secs);
   std::cout << format_comma(count) << " keys iterated in " << std::fixed << std::setprecision(3)
             << secs << " sec  (" << format_comma(kps) << " keys/sec)\n";
}

// -- Remove benchmark --

void remove_test(benchmark_config   cfg,
                 write_session&     ses,
                 const std::string& name,
                 auto               make_key)
{
   print_header(name, cfg);

   std::vector<char> key;
   uint64_t          seq = 0;

   auto tx = ses.start_transaction(0);

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      auto     start   = std::chrono::steady_clock::now();
      uint32_t removed = 0;
      while (removed < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - removed);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq++, key);
            tx.remove(key_view(key.data(), key.size()));
            ++removed;
         }
      }

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   rps  = uint64_t(removed / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(rps)
                << "  removes/sec\n";
   }
   if (!bench::interrupted())
      tx.commit();
}

// -- Random remove of known keys --
// Removes keys that were previously inserted with make_key(0..total_inserted-1)
// in random order using a Fisher-Yates-like permutation via rand_from_seq

void remove_rand_test(benchmark_config   cfg,
                      write_session&     ses,
                      const std::string& name,
                      auto               make_key)
{
   print_header(name, cfg);

   // Build a shuffled index of all inserted keys
   uint64_t total = uint64_t(cfg.items) * cfg.rounds;
   std::vector<uint64_t> indices(total);
   for (uint64_t i = 0; i < total; ++i)
      indices[i] = i;
   // Fisher-Yates shuffle with deterministic RNG
   for (uint64_t i = total - 1; i > 0; --i)
   {
      uint64_t j = rand_from_seq(i) % (i + 1);
      std::swap(indices[i], indices[j]);
   }

   std::vector<char> key;
   uint64_t          pos = 0;

   auto tx = ses.start_transaction(0);

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      auto     start   = std::chrono::steady_clock::now();
      uint32_t removed = 0;
      while (removed < cfg.items)
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - removed);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(indices[pos++], key);
            tx.remove(key_view(key.data(), key.size()));
            ++removed;
         }
      }

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   rps  = uint64_t(removed / secs);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(pos) << "  " << std::setw(12) << std::right << format_comma(rps)
                << "  removes/sec\n";
   }
   if (!bench::interrupted())
      tx.commit();
}

// -- Lower-bound benchmark --

void lower_bound_test(benchmark_config   cfg,
                      write_session&     ses,
                      const std::string& name,
                      auto               make_key)
{
   print_header(name, cfg);

   std::vector<char> key;
   auto              root = ses.get_root(0);
   cursor            cur(root);

   auto     start = std::chrono::steady_clock::now();
   uint64_t count = 0;
   for (uint64_t i = 0; i < uint64_t(cfg.items) * cfg.rounds && !bench::interrupted(); ++i)
   {
      make_key(i, key);
      cur.lower_bound(key_view(key.data(), key.size()));
      ++count;
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   lps  = uint64_t(count / secs);
   std::cout << format_comma(lps) << " lower_bounds/sec  (" << format_comma(count) << " ops)\n";
}

// -- Random get benchmark (point lookups, mix of found/not-found) --

void get_rand_test(benchmark_config   cfg,
                   write_session&     ses,
                   const std::string& name,
                   auto               make_key)
{
   print_header(name, cfg);

   std::vector<char> key;
   std::string       buf;
   auto              root = ses.get_root(0);
   cursor            cur(root);

   auto     start = std::chrono::steady_clock::now();
   uint64_t count = 0;
   uint64_t found = 0;
   for (uint64_t i = 0; i < uint64_t(cfg.items) * cfg.rounds && !bench::interrupted(); ++i)
   {
      make_key(i, key);
      auto result = cur.get(key_view(key.data(), key.size()), &buf);
      if (result >= 0)
         ++found;
      ++count;
   }
   auto   end  = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(end - start).count();
   auto   gps  = uint64_t(count / secs);
   std::cout << format_comma(gps) << " gets/sec  (" << format_comma(found) << " found / "
             << format_comma(count) << " ops)\n";
}

// -- Multi-writer benchmark: N writers each on their own tree --

void multiwriter_test(benchmark_config            cfg,
                      std::shared_ptr<database>&   db,
                      uint32_t                     num_writers,
                      auto                         make_key,
                      const std::string&           name)
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

   std::vector<std::thread> writers;
   writers.reserve(num_writers);
   for (uint32_t t = 0; t < num_writers; ++t)
   {
      // Each writer uses root_index = t+1 (reserve 0 for other tests)
      writers.emplace_back(
          [&db, &start_flag, &done, &counters, &cfg, t, &make_key, num_writers]()
          {
             sal::set_current_thread_name("writer_thread");
             auto              ws  = db->start_write_session();
             ws->set_sync(cfg.sync);
             std::vector<char> key;
             std::vector<char> value(cfg.value_size, 'v');
             value_view        vv(value.data(), value.size());
             uint32_t          root_index = t + 1;  // each writer owns its own tree
             int64_t           total_inserted = 0;

             // Wait for all threads to be ready
             while (!start_flag.load(std::memory_order_relaxed))
                ;

             for (uint32_t r = 0; r < cfg.rounds && !done.load(std::memory_order_relaxed) && !bench::interrupted(); ++r)
             {
                auto     tx       = ws->start_transaction(root_index);
                uint32_t inserted = 0;
                while (inserted < cfg.items && !bench::interrupted())
                {
                   uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
                   for (uint32_t i = 0; i < batch; ++i)
                   {
                      // Salt keys by writer index so each tree has unique keys
                      uint64_t seq = uint64_t(r) * cfg.items + inserted + i;
                      seq = seq * num_writers + t;
                      make_key(seq, key);
                      tx.insert(key_view(key.data(), key.size()), vv);
                      ++inserted;
                   }
                }
                if (bench::interrupted()) { tx.abort(); break; }
                tx.commit();
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

   // Report progress while writers are running
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
      if (cur >= target_inserts || bench::interrupted())
         break;
   }

   done.store(true, std::memory_order_relaxed);
   for (auto& t : writers)
      t.join();

   auto   overall_end  = std::chrono::steady_clock::now();
   double overall_secs = std::chrono::duration<double>(overall_end - overall_start).count();
   auto   final_inserts = sum_inserts();
   std::cout << "total: " << format_comma(final_inserts) << " inserts across " << num_writers
             << " writers in " << std::fixed << std::setprecision(3) << overall_secs << " sec\n";
   std::cout << "  aggregate: " << format_comma(uint64_t(final_inserts / overall_secs))
             << " inserts/sec  per-writer: "
             << format_comma(uint64_t(final_inserts / overall_secs / num_writers))
             << " inserts/sec\n";
}

// -- Multi-threaded stress test: concurrent reads while writing --
// read_op: "lower_bound" or "get"
// key_mode: "rand" (random keys, may not exist) or "known" (keys guaranteed to exist)

void multithread_rw_test(benchmark_config            cfg,
                         std::shared_ptr<database>&   db,
                         write_session&               ses,
                         uint32_t                     num_threads,
                         auto                         make_key,
                         const std::string&           read_op,
                         const std::string&           key_mode)
{
   std::string label = "multithread " + read_op + " (" + key_mode + " keys)";
   std::cout << "---------------------  " << label << "  "
             << std::string(std::max(0, int(52 - label.size())), '-') << "\n";
   std::cout << "write rounds: " << cfg.rounds << "  items: " << format_comma(cfg.items)
             << "  read threads: " << num_threads << "\n";
   std::cout << "-----------------------------------------------------------------------\n";

   bool use_get   = (read_op == "get");
   bool use_known = (key_mode == "known");

   // Seed the tree so readers start with data
   {
      std::vector<char> key;
      std::vector<char> value(cfg.value_size, 'v');
      value_view        vv(value.data(), value.size());
      auto              tx = ses.start_transaction(0);
      for (uint32_t i = 0; i < cfg.items; ++i)
      {
         make_key(i, key);
         tx.insert(key_view(key.data(), key.size()), vv);
      }
      tx.commit();
   }
   std::cout << "seeded " << format_comma(cfg.items) << " keys\n";
   print_stats(ses);

   struct alignas(128) padded_counters
   {
      std::atomic<int64_t> ops{0};
      std::atomic<int64_t> found{0};
   };

   std::atomic<uint64_t>           committed_seq{cfg.items};
   std::atomic<bool>               done{false};
   std::vector<padded_counters>    counters(num_threads);

   // Launch reader threads
   std::vector<std::thread> readers;
   readers.reserve(num_threads);
   for (uint32_t t = 0; t < num_threads; ++t)
   {
      readers.emplace_back(
          [&db, &done, &counters, &committed_seq, t, &make_key, use_get, use_known]()
          {
             sal::set_current_thread_name("read_thread");
             auto              rs  = db->start_read_session();
             auto              cur = rs->create_cursor(0);
             std::vector<char> key;
             std::string       buf;
             int64_t           local_ops   = 0;
             int64_t           local_found = 0;
             uint32_t          refresh_counter = 0;
             // Per-thread salt so threads probe different keys
             const uint64_t    salt = rand_from_seq(t * 999983ULL + 1);

             while (!done.load(std::memory_order_relaxed) && !bench::interrupted())
             {
                if (++refresh_counter >= 10)
                {
                   cur = rs->create_cursor(0);
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
                      if (cur.get(key_view(key.data(), key.size()), &buf) >= 0)
                         ++local_found;
                   }
                   else
                   {
                      if (cur.lower_bound(key_view(key.data(), key.size())))
                         ++local_found;
                   }
                   ++local_ops;
                }
                counters[t].ops.store(local_ops, std::memory_order_relaxed);
                counters[t].found.store(local_found, std::memory_order_relaxed);
             }
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
   value_view        vv(value.data(), value.size());
   uint64_t          seq = cfg.items;

   int64_t prev_ops = 0;
   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      auto     tx       = ses.start_transaction(0);
      auto     start    = std::chrono::steady_clock::now();
      uint32_t inserted = 0;
      while (inserted < cfg.items && !bench::interrupted())
      {
         uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
         for (uint32_t i = 0; i < batch; ++i)
         {
            make_key(seq++, key);
            tx.insert(key_view(key.data(), key.size()), vv);
            ++inserted;
         }
      }
      if (bench::interrupted()) break;
      tx.commit();
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

static void crash_handler(int sig, siginfo_t* info, void* /*ctx*/)
{
   const char* name = (sig == SIGBUS) ? "SIGBUS" : (sig == SIGSEGV) ? "SIGSEGV" : "SIGILL";
   fprintf(stderr, "\n=== %s at addr %p ===\n", name, info->si_addr);
   void* bt[64];
   int n = backtrace(bt, 64);
   backtrace_symbols_fd(bt, n, STDERR_FILENO);
   _exit(128 + sig);
}

int main(int argc, char** argv)
{
   struct sigaction sa{};
   sa.sa_sigaction = crash_handler;
   sa.sa_flags = SA_SIGINFO;
   sigaction(SIGBUS, &sa, nullptr);
   sigaction(SIGSEGV, &sa, nullptr);
   sigaction(SIGILL, &sa, nullptr);
   bench::install_interrupt_handler();

   sal::set_current_thread_name("main");
   uint32_t    rounds;
   uint32_t    batch;
   uint32_t    items;
   uint32_t    value_size;
   uint32_t    threads;
   bool        reset    = false;
   bool        stat     = false;
   bool        validate = false;
   std::string db_dir   = "./psitridb";
   std::string bench    = "all";
   std::string sync_str = "none";

   po::options_description desc("psitri-benchmark options");
   auto                    opt = desc.add_options();
   opt("help,h", "print this message");
   opt("round,r", po::value<uint32_t>(&rounds)->default_value(3), "number of rounds");
   opt("batch,b", po::value<uint32_t>(&batch)->default_value(512), "batch size");
   opt("items,i", po::value<uint32_t>(&items)->default_value(1000000), "number of items per round");
   opt("value-size,s", po::value<uint32_t>(&value_size)->default_value(8), "value size in bytes");
   opt("threads,t", po::value<uint32_t>(&threads)->default_value(4), "number of read threads for multithread test");
   opt("db-dir,d", po::value<std::string>(&db_dir)->default_value("./psitridb"), "database dir");
   opt("bench", po::value<std::string>(&bench)->default_value("all"),
       "benchmark: all, insert, upsert, get, iterate, remove, remove-rand, lower-bound, get-rand, "
       "multiwriter-rand, multiwriter-seq, "
       "multithread-lowerbound-rand, multithread-lowerbound-known, "
       "multithread-get-rand, multithread-get-known");
   opt("sync", po::value<std::string>(&sync_str)->default_value("none"),
       "sync mode: none (no sync), safe (msync async), full (msync sync)");
   opt("reset", po::bool_switch(&reset), "reset database before running");
   opt("stat", po::bool_switch(&stat)->default_value(false), "print database stats and exit");
   opt("validate", po::bool_switch(&validate)->default_value(false), "validate tree after each round");

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

   // Open or create
   bool created = false;
   if (!std::filesystem::exists(db_dir / std::filesystem::path("data")))
   {
      std::filesystem::create_directories(db_dir / std::filesystem::path("data"));
      created = true;
   }

   auto db  = std::make_shared<database>(db_dir, runtime_config());
   auto ses = db->start_write_session();

   if (stat)
   {
      print_stats(*ses);
      return 0;
   }

   sal::sync_type sync = sal::sync_type::none;
   if (sync_str == "safe")
      sync = sal::sync_type::fsync;
   else if (sync_str == "full")
      sync = sal::sync_type::full;
   else if (sync_str != "none")
   {
      std::cerr << "invalid --sync mode: " << sync_str << " (use none, safe, full)\n";
      return 1;
   }

   ses->set_sync(sync);

   benchmark_config cfg = {rounds, items, batch, value_size, sync, validate};

   std::cout << "psitri-benchmark: db=" << db_dir << (created ? " (new)" : " (existing)") << "\n";
   std::cout << "rounds=" << rounds << " items=" << format_comma(items)
             << " batch=" << batch << " value_size=" << value_size
             << " sync=" << sync_str << "\n\n";

   auto run_all    = (bench == "all");
   auto be_seq_key = [](uint64_t seq, auto& v) { to_key(to_big_endian(seq), v); };
   auto le_seq_key = [](uint64_t seq, auto& v) { to_key(seq, v); };
   auto rand_key   = [](uint64_t seq, auto& v) { to_key(rand_from_seq(seq), v); };
   auto str_rand_key = [](uint64_t seq, auto& v) { to_key(std::to_string(rand_from_seq(seq)), v); };

   auto should_run = [&](const std::string& name) {
      return !bench::interrupted() && (run_all || bench == name);
   };

   // -- Insert --
   if (should_run("insert"))
   {
      insert_test(cfg, *ses, "big endian seq insert", be_seq_key);
      print_stats(*ses);

      if (!bench::interrupted())
      {
         insert_test(cfg, *ses, "dense random insert", rand_key);
         print_stats(*ses);
      }
      if (!bench::interrupted())
      {
         insert_test(cfg, *ses, "string number rand insert", str_rand_key);
         print_stats(*ses);
      }
   }

   // -- Get --
   if (should_run("get"))
   {
      get_test(cfg, *ses, "big endian seq get", be_seq_key);
      if (!bench::interrupted())
         get_test(cfg, *ses, "dense random get", rand_key);
   }

   // -- Upsert --
   if (should_run("upsert"))
   {
      upsert_test(cfg, *ses, "big endian seq upsert", be_seq_key);
      print_stats(*ses);
   }

   // -- Iterate --
   if (should_run("iterate"))
      iterate_test(cfg, *ses);

   // -- Lower-bound --
   if (should_run("lower-bound"))
      lower_bound_test(cfg, *ses, "random lower_bound", rand_key);

   // -- Random get (point lookups, mix of found/not-found) --
   if (should_run("get-rand"))
      get_rand_test(cfg, *ses, "random get", rand_key);

   // -- Remove --
   if (should_run("remove"))
   {
      remove_test(cfg, *ses, "big endian seq remove", be_seq_key);
      print_stats(*ses);
   }

   // -- Random remove of known keys (run after random insert) --
   if (should_run("remove-rand"))
   {
      remove_rand_test(cfg, *ses, "random remove (known keys)", rand_key);
      print_stats(*ses);
   }

   // -- Multi-writer --
   if (should_run("multiwriter-rand"))
      multiwriter_test(cfg, db, threads, rand_key, "multi-writer rand insert");
   if (should_run("multiwriter-seq"))
      multiwriter_test(cfg, db, threads, be_seq_key, "multi-writer seq insert");

   // -- Multithread read+write variants --
   if (should_run("multithread-lowerbound-rand"))
   {
      multithread_rw_test(cfg, db, *ses, threads, rand_key, "lower_bound", "rand");
      print_stats(*ses);
   }
   if (should_run("multithread-lowerbound-known"))
   {
      multithread_rw_test(cfg, db, *ses, threads, rand_key, "lower_bound", "known");
      print_stats(*ses);
   }
   if (should_run("multithread-get-rand"))
   {
      multithread_rw_test(cfg, db, *ses, threads, rand_key, "get", "rand");
      print_stats(*ses);
   }
   if (should_run("multithread-get-known"))
   {
      multithread_rw_test(cfg, db, *ses, threads, rand_key, "get", "known");
      print_stats(*ses);
   }

   if (bench::interrupted())
      std::cout << "\nInterrupted — exiting gracefully.\n";
   else
      std::cout << "\ndone.\n";
   return 0;
}
