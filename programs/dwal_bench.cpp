#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <thread>
#include <vector>

#include "bench_signal.hpp"

#include <hash/xxhash.h>
#include <psitri/database.hpp>
#include <psitri/dwal/dwal_database.hpp>
#include <psitri/dwal/merge_cursor.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/write_session_impl.hpp>

using namespace psitri;

/// Sum the size of all files in a directory (non-recursive for speed).
static uint64_t dir_size_bytes(const std::filesystem::path& dir)
{
   uint64_t total = 0;
   std::error_code ec;
   for (auto& entry : std::filesystem::recursive_directory_iterator(dir, ec))
   {
      if (entry.is_regular_file(ec))
         total += entry.file_size(ec);
   }
   return total;
}

static std::string format_size_mb(uint64_t bytes)
{
   double mb = bytes / (1024.0 * 1024.0);
   char buf[32];
   snprintf(buf, sizeof(buf), "%.1fMB", mb);
   return buf;
}

int64_t rand_from_seq(uint64_t seq)
{
   return XXH3_64bits((char*)&seq, sizeof(seq));
}

static constexpr size_t RANDOM_BUF_SIZE = 1 << 20;
static char             g_random_buf[RANDOM_BUF_SIZE];
static uint64_t         g_random_seed = 0xdeadbeefcafebabe;

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

static value_view random_value(uint64_t seq, uint32_t value_size)
{
   size_t offset = rand_from_seq(seq) % (RANDOM_BUF_SIZE - value_size);
   return value_view(g_random_buf + offset, value_size);
}

void to_key(uint64_t val, std::vector<char>& v)
{
   v.resize(sizeof(val));
   memcpy(v.data(), &val, sizeof(val));
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

struct bench_config
{
   uint32_t rounds         = 10;
   uint32_t items          = 1000000;
   uint32_t batch_size     = 512;
   uint32_t value_size     = 100;
   uint32_t readers        = 4;
   uint32_t merge_threads  = 2;
   uint32_t max_rw_entries = 100000;
   uint32_t pause_ms       = 0;
   bool     use_dwal       = true;
};

// ── Reader thread counters ──────────────────────────────────────

struct padded_counters
{
   alignas(128) std::atomic<int64_t> ops{0};
   std::atomic<int64_t> found{0};
};

// ── Write-only benchmark ────────────────────────────────────────

void write_only_bench(bench_config cfg, const std::filesystem::path& db_dir)
{
   auto db = database::open(db_dir);

   std::string mode_label = cfg.use_dwal ? "DWAL buffered" : "direct COW";
   std::cout << "═══════════════════════════════════════════════════════════════\n";
   std::cout << "  " << mode_label << " write-only throughput\n";
   std::cout << "  rounds=" << cfg.rounds << " items=" << format_comma(cfg.items)
             << " batch=" << cfg.batch_size << " val_size=" << cfg.value_size << "\n";
   if (cfg.use_dwal)
      std::cout << "  merge_threads=" << cfg.merge_threads
                << " max_rw_entries=" << format_comma(cfg.max_rw_entries) << "\n";
   std::cout << "═══════════════════════════════════════════════════════════════\n";

   std::unique_ptr<dwal::dwal_database> dwal_db;
   if (cfg.use_dwal)
   {
      dwal::dwal_config dcfg;
      dcfg.merge_threads  = cfg.merge_threads;
      dcfg.max_rw_entries = cfg.max_rw_entries;
      dwal_db = std::make_unique<dwal::dwal_database>(db, db_dir / "wal", dcfg);
   }

   auto ws = db->start_write_session();

   std::vector<char> key;
   uint64_t          seq = 0;
   auto rand_key = [](uint64_t s, std::vector<char>& v) { to_key(rand_from_seq(s), v); };

   auto overall_start = std::chrono::steady_clock::now();

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      reshuffle_random_buf();
      auto     start    = std::chrono::steady_clock::now();
      uint32_t inserted = 0;

      if (cfg.use_dwal)
      {
         while (inserted < cfg.items && !bench::interrupted())
         {
            auto     tx    = dwal_db->start_write_transaction(0);
            uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
            for (uint32_t i = 0; i < batch; ++i)
            {
               rand_key(seq, key);
               tx.upsert(std::string_view(key.data(), key.size()),
                         std::string_view(random_value(seq, cfg.value_size).data(),
                                          cfg.value_size));
               ++seq;
               ++inserted;
            }
            tx.commit();
         }
      }
      else
      {
         while (inserted < cfg.items && !bench::interrupted())
         {
            auto     tx    = ws->start_transaction(0);
            uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
            for (uint32_t i = 0; i < batch; ++i)
            {
               rand_key(seq, key);
               tx.upsert(key_view(key.data(), key.size()), random_value(seq, cfg.value_size));
               ++seq;
               ++inserted;
            }
            tx.commit();
         }
      }

      auto   end  = std::chrono::steady_clock::now();
      double secs = std::chrono::duration<double>(end - start).count();
      auto   ips  = uint64_t(inserted / secs);
      auto db_bytes = dir_size_bytes(db_dir);
      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(seq) << "  " << std::setw(12) << std::right << format_comma(ips)
                << "  upserts/sec  db=" << format_size_mb(db_bytes)
                << "  alloc=" << format_comma(ws->get_total_allocated_objects())
                << "  pending=" << format_comma(ws->get_pending_release_count())
                << std::endl;
   }

   auto   overall_end  = std::chrono::steady_clock::now();
   double overall_secs = std::chrono::duration<double>(overall_end - overall_start).count();
   std::cout << "───────────────────────────────────────────────────────────────\n";
   std::cout << "total: " << format_comma(seq) << " upserts in " << std::fixed
             << std::setprecision(3) << overall_secs << " sec  ("
             << format_comma(uint64_t(seq / overall_secs)) << " upserts/sec)\n";
}

// ── Write + Concurrent Read benchmark ───────────────────────────
//
// Runs each read mode as a separate sequential phase so they don't
// interfere with each other. Each phase: writer + N readers of one mode.
//
// For DWAL: 3 phases (persistent, buffered, latest).
// For direct COW: 1 phase (persistent via PsiTri cursor).

void rw_bench(bench_config cfg, const std::filesystem::path& db_dir)
{
   auto db = database::open(db_dir);

   std::string mode_label = cfg.use_dwal ? "DWAL buffered" : "direct COW";
   std::cout << "═══════════════════════════════════════════════════════════════\n";
   std::cout << "  " << mode_label << " write + concurrent reads (sequential phases)\n";
   std::cout << "  rounds=" << cfg.rounds << " items=" << format_comma(cfg.items)
             << " batch=" << cfg.batch_size << " val_size=" << cfg.value_size << "\n";
   if (cfg.use_dwal)
      std::cout << "  merge_threads=" << cfg.merge_threads
                << " max_rw_entries=" << format_comma(cfg.max_rw_entries) << "\n";
   std::cout << "  readers per phase: " << cfg.readers << "\n";
   std::cout << "═══════════════════════════════════════════════════════════════\n";

   std::unique_ptr<dwal::dwal_database> dwal_db;
   if (cfg.use_dwal)
   {
      dwal::dwal_config dcfg;
      dcfg.merge_threads  = cfg.merge_threads;
      dcfg.max_rw_entries = cfg.max_rw_entries;
      dwal_db = std::make_unique<dwal::dwal_database>(db, db_dir / "wal", dcfg);
   }

   auto ws = db->start_write_session();

   // Seed with initial data so readers have something to hit immediately.
   auto rand_key = [](uint64_t s, std::vector<char>& v) { to_key(rand_from_seq(s), v); };
   uint64_t seq = 0;
   {
      std::vector<char> key;
      uint32_t seeded = 0;
      uint32_t seed_count = std::min(cfg.items, uint32_t(100000));
      if (cfg.use_dwal)
      {
         while (seeded < seed_count)
         {
            auto     tx    = dwal_db->start_write_transaction(0);
            uint32_t batch = std::min(cfg.batch_size, seed_count - seeded);
            for (uint32_t i = 0; i < batch; ++i)
            {
               rand_key(seq, key);
               tx.upsert(std::string_view(key.data(), key.size()),
                         std::string_view(random_value(seq, cfg.value_size).data(),
                                          cfg.value_size));
               ++seq;
               ++seeded;
            }
            tx.commit();
         }
         dwal_db->swap_rw_to_ro(0);
      }
      else
      {
         while (seeded < seed_count)
         {
            auto     tx    = ws->start_transaction(0);
            uint32_t batch = std::min(cfg.batch_size, seed_count - seeded);
            for (uint32_t i = 0; i < batch; ++i)
            {
               rand_key(seq, key);
               tx.upsert(key_view(key.data(), key.size()), random_value(seq, cfg.value_size));
               ++seq;
               ++seeded;
            }
            tx.commit();
         }
      }
      std::cout << "seeded " << format_comma(seq) << " keys\n";
   }

   // Read modes to test sequentially.
   uint32_t    num_phases = cfg.use_dwal ? 3 : 1;
   const char* phase_names[] = {"persistent", "buffered", "latest"};
   dwal::read_mode phase_modes[] = {
       dwal::read_mode::persistent, dwal::read_mode::buffered, dwal::read_mode::latest};

   for (uint32_t phase = 0; phase < num_phases && !bench::interrupted(); ++phase)
   {
      std::cout << "\n── " << phase_names[phase] << " readers ──\n";

      std::atomic<uint64_t> committed_seq{seq};
      std::atomic<bool>     done{false};

      // Per-thread counters.
      std::vector<std::unique_ptr<padded_counters>> counters;
      counters.reserve(cfg.readers);
      for (uint32_t t = 0; t < cfg.readers; ++t)
         counters.push_back(std::make_unique<padded_counters>());

      // Launch reader threads.
      std::vector<std::thread> readers;
      for (uint32_t t = 0; t < cfg.readers; ++t)
      {
         readers.emplace_back(
             [&, t, phase]()
             {
                sal::set_current_thread_name("reader");
                std::vector<char> key;
                int64_t           local_ops   = 0;
                int64_t           local_found = 0;
                const uint64_t    salt = rand_from_seq(t * 999983ULL + phase * 777773ULL + 1);

                std::optional<dwal::dwal_read_session> dwal_reader;
                std::shared_ptr<read_session>          tri_rs;
                psitri::cursor                         tri_cur{sal::smart_ptr<sal::alloc_header>{}};

                if (dwal_db)
                {
                   dwal_reader.emplace(*dwal_db);
                }
                else
                {
                   tri_rs  = db->start_read_session();
                   tri_cur = tri_rs->create_cursor(0);
                }

                auto mode = phase_modes[phase];

                while (!done.load(std::memory_order_relaxed) && !bench::interrupted())
                {
                   if (!dwal_db && (local_ops % 10000) == 0 && tri_rs)
                      tri_cur.refresh(0);

                   uint64_t max_seq = committed_seq.load(std::memory_order_relaxed);
                   for (uint32_t i = 0; i < 1000; ++i)
                   {
                      uint64_t s = rand_from_seq(local_ops + salt) % max_seq;
                      to_key(rand_from_seq(s), key);

                      bool found = false;
                      if (dwal_reader)
                      {
                         auto result = dwal_reader->get(
                             0, std::string_view(key.data(), key.size()), mode);
                         found = result.found;
                      }
                      else
                      {
                         std::string buf;
                         found = tri_cur.get(key_view(key.data(), key.size()), &buf) >= 0;
                      }

                      if (found)
                         ++local_found;
                      ++local_ops;
                   }
                   counters[t]->ops.store(local_ops, std::memory_order_relaxed);
                   counters[t]->found.store(local_found, std::memory_order_relaxed);
                }
             });
      }

      auto sum_reader_ops = [&]() -> int64_t
      {
         int64_t total = 0;
         for (uint32_t i = 0; i < cfg.readers; ++i)
            total += counters[i]->ops.load(std::memory_order_relaxed);
         return total;
      };

      // Writer loop for this phase.
      std::vector<char> key;
      uint64_t          phase_start_seq = seq;
      int64_t           prev_ops        = 0;
      auto              phase_start     = std::chrono::steady_clock::now();

      for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
      {
         reshuffle_random_buf();
         auto     start    = std::chrono::steady_clock::now();
         uint32_t inserted = 0;

         if (cfg.use_dwal)
         {
            while (inserted < cfg.items && !bench::interrupted())
            {
               auto     tx    = dwal_db->start_write_transaction(0);
               uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
               for (uint32_t i = 0; i < batch; ++i)
               {
                  rand_key(seq, key);
                  tx.upsert(std::string_view(key.data(), key.size()),
                            std::string_view(random_value(seq, cfg.value_size).data(),
                                             cfg.value_size));
                  ++seq;
                  ++inserted;
               }
               tx.commit();
               committed_seq.store(seq, std::memory_order_relaxed);
            }
         }
         else
         {
            while (inserted < cfg.items && !bench::interrupted())
            {
               auto     tx    = ws->start_transaction(0);
               uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
               for (uint32_t i = 0; i < batch; ++i)
               {
                  rand_key(seq, key);
                  tx.upsert(key_view(key.data(), key.size()), random_value(seq, cfg.value_size));
                  ++seq;
                  ++inserted;
               }
               tx.commit();
               committed_seq.store(seq, std::memory_order_relaxed);
            }
         }

         auto   end  = std::chrono::steady_clock::now();
         double secs = std::chrono::duration<double>(end - start).count();
         auto   ips  = uint64_t(inserted / secs);

         auto cur_ops  = sum_reader_ops();
         auto delta    = cur_ops - prev_ops;
         auto rps      = uint64_t(delta / secs);
         prev_ops      = cur_ops;

         auto db_bytes = dir_size_bytes(db_dir);
         std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                   << format_comma(ips) << "  writes/sec  " << std::setw(12) << std::right
                   << format_comma(rps) << "  " << phase_names[phase] << "/sec"
                   << "  db=" << format_size_mb(db_bytes) << std::endl;

         if (cfg.pause_ms > 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(cfg.pause_ms));
      }

      done.store(true, std::memory_order_relaxed);
      for (auto& t : readers)
         t.join();

      auto     phase_end  = std::chrono::steady_clock::now();
      double   phase_secs = std::chrono::duration<double>(phase_end - phase_start).count();
      uint64_t written    = seq - phase_start_seq;

      int64_t total_ops   = sum_reader_ops();
      int64_t total_found = 0;
      for (uint32_t i = 0; i < cfg.readers; ++i)
         total_found += counters[i]->found.load(std::memory_order_relaxed);

      std::cout << "  " << phase_names[phase] << " summary: "
                << format_comma(uint64_t(written / phase_secs)) << " writes/sec, "
                << format_comma(uint64_t(total_ops / phase_secs)) << " reads/sec  ("
                << format_comma(total_found) << " found / " << format_comma(total_ops) << " ops)\n";
   }

   auto db_bytes = dir_size_bytes(db_dir);
   std::cout << "───────────────────────────────────────────────────────────────\n";
   std::cout << "total keys written: " << format_comma(seq)
             << "  db=" << format_size_mb(db_bytes) << "\n";
}

// ── Crash handler ───────────────────────────────────────────────

static void crash_handler(int sig, siginfo_t* info, void* /*ctx*/)
{
   const char* name = (sig == SIGBUS) ? "SIGBUS" : (sig == SIGSEGV) ? "SIGSEGV" : "SIGILL";
   fprintf(stderr, "\n=== %s at addr %p ===\n", name, info->si_addr);
   _exit(128 + sig);
}

int main(int argc, char** argv)
{
   struct sigaction sa{};
   sa.sa_sigaction = crash_handler;
   sa.sa_flags = SA_SIGINFO;
   sigaction(SIGBUS, &sa, nullptr);
   sigaction(SIGSEGV, &sa, nullptr);
   bench::install_interrupt_handler();

   sal::set_current_thread_name("main");
   reshuffle_random_buf();

   bench_config cfg;
   std::string  db_base = "./dwal_bench_db";

   for (int i = 1; i < argc; ++i)
   {
      std::string arg = argv[i];
      auto next = [&]() -> std::string { return (i + 1 < argc) ? argv[++i] : ""; };
      if (arg == "--rounds" || arg == "-r")         cfg.rounds = std::stoi(next());
      else if (arg == "--items" || arg == "-i")      cfg.items = std::stoi(next());
      else if (arg == "--batch" || arg == "-b")      cfg.batch_size = std::stoi(next());
      else if (arg == "--value-size" || arg == "-s") cfg.value_size = std::stoi(next());
      else if (arg == "--readers" || arg == "-t")    cfg.readers = std::stoi(next());
      else if (arg == "--merge-threads")             cfg.merge_threads = std::stoi(next());
      else if (arg == "--max-rw")                    cfg.max_rw_entries = std::stoi(next());
      else if (arg == "--pause")                     cfg.pause_ms = std::stoi(next());
      else if (arg == "--db-dir" || arg == "-d")     db_base = next();
      else if (arg == "--help" || arg == "-h")
      {
         std::cout << "dwal-bench: Compare DWAL buffered vs direct COW write throughput\n\n"
                   << "Options:\n"
                   << "  -r, --rounds N        rounds (default 10)\n"
                   << "  -i, --items N         items per round (default 1000000)\n"
                   << "  -b, --batch N         batch size (default 512)\n"
                   << "  -s, --value-size N    value bytes (default 100)\n"
                   << "  -t, --readers N       reader threads per mode (default 4)\n"
                   << "  --merge-threads N     DWAL merge pool threads (default 2)\n"
                   << "  --max-rw N            DWAL RW btree swap threshold (default 100000)\n"
                   << "  -d, --db-dir PATH     database dir prefix (default ./dwal_bench_db)\n";
         return 0;
      }
   }

   std::cout << "\n╔═══════════════════════════════════════════════════════════════╗\n"
             << "║              DWAL vs Direct COW Benchmark                    ║\n"
             << "╚═══════════════════════════════════════════════════════════════╝\n\n";

   // --- Phase 1: Write-only throughput ---
   std::cout << "Phase 1: Write-only throughput\n\n";

   {
      auto dir = std::filesystem::path(db_base + "_direct_w");
      std::filesystem::remove_all(dir);
      cfg.use_dwal = false;
      write_only_bench(cfg, dir);
      std::filesystem::remove_all(dir);
   }
   std::cout << "\n";
   {
      auto dir = std::filesystem::path(db_base + "_dwal_w");
      std::filesystem::remove_all(dir);
      cfg.use_dwal = true;
      write_only_bench(cfg, dir);
      std::filesystem::remove_all(dir);
   }

   // --- Phase 2: Write + concurrent readers ---
   std::cout << "\n\nPhase 2: Write + concurrent readers (" << cfg.readers << " per mode)\n\n";

   {
      auto dir = std::filesystem::path(db_base + "_direct_rw");
      std::filesystem::remove_all(dir);
      cfg.use_dwal = false;
      rw_bench(cfg, dir);
      std::filesystem::remove_all(dir);
   }
   std::cout << "\n";
   {
      auto dir = std::filesystem::path(db_base + "_dwal_rw");
      std::filesystem::remove_all(dir);
      cfg.use_dwal = true;
      rw_bench(cfg, dir);
      std::filesystem::remove_all(dir);
   }

   std::cout << "\ndone.\n";
   return 0;
}
