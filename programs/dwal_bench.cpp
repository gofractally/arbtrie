#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <thread>
#include <vector>

#include "bench_signal.hpp"

#include <hash/xxhash.h>
#include <psitri/database.hpp>
#include <psitri/dwal/dwal_database.hpp>

// Global pointer for signal handler to trigger graceful DWAL shutdown.
// Set when a DWAL benchmark phase is active; cleared on exit.
static std::atomic<psitri::dwal::dwal_database*> g_active_dwal{nullptr};
#include <psitri/dwal/merge_cursor.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/write_session_impl.hpp>

using namespace psitri;

// ── Utilities ───────────────────────────────────────────────────

static uint64_t dir_size_bytes(const std::filesystem::path& dir)
{
   uint64_t        total = 0;
   std::error_code ec;
   for (auto& entry : std::filesystem::recursive_directory_iterator(dir, ec))
   {
      if (entry.is_regular_file(ec))
         total += entry.file_size(ec);
   }
   return total;
}

static std::string format_size(uint64_t bytes)
{
   char buf[32];
   if (bytes >= uint64_t(1) << 30)
      snprintf(buf, sizeof(buf), "%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
   else
      snprintf(buf, sizeof(buf), "%.1f MB", bytes / (1024.0 * 1024.0));
   return buf;
}

static int64_t rand_from_seq(uint64_t seq)
{
   return XXH3_64bits(&seq, sizeof(seq));
}

static void to_key(uint64_t val, std::vector<char>& v)
{
   v.resize(sizeof(val));
   memcpy(v.data(), &val, sizeof(val));
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

// ── Random value buffer (non-validation mode) ──────────────────

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

// ── Deterministic value for validation mode ────────────────────
//
// value = repeating XXH3_64bits(key) so every write of the same key
// produces the same value.  Readers verify the first 8 bytes.

static thread_local char t_val_buf[4096];

static value_view validation_value(const char* key, size_t key_len, uint32_t value_size)
{
   uint64_t h   = XXH3_64bits(key, key_len);
   size_t   pos = 0;
   while (pos + 8 <= value_size)
   {
      memcpy(t_val_buf + pos, &h, 8);
      pos += 8;
   }
   if (pos < value_size)
      memcpy(t_val_buf + pos, &h, value_size - pos);
   return value_view(t_val_buf, value_size);
}

static bool verify_value(const char* key, size_t key_len, const std::string& value)
{
   if (value.size() < 8)
      return false;
   uint64_t expected = XXH3_64bits(key, key_len);
   uint64_t actual;
   memcpy(&actual, value.data(), 8);
   return expected == actual;
}

// ── Configuration ──────────────────────────────────────────────

struct bench_config
{
   uint32_t rounds          = 10;
   uint32_t items           = 1000000;
   uint32_t batch_size      = 10;
   uint32_t value_size      = 32;
   uint32_t readers         = 6;
   uint32_t merge_threads   = 2;
   uint32_t max_rw_entries  = 100000;
   uint64_t pinned_cache_mb = 1024 * 8;

   bool reset_db = false;
   bool no_mlock = false;
   bool validate = false;

   uint32_t reopen_every = 0;  ///< Close and reopen DB every N rounds (0 = disabled).

   // Mode selection (default: dwal + rw)
   bool run_write_only = false;
   bool run_rw         = false;
   bool run_direct     = false;
   bool run_dwal       = false;

   std::string db_dir   = "./dwal_bench_db";
   std::string csv_path = "./dwal_bench_results.csv";
};

// ── CSV Logger ──────────────────────────────────────────────────
//
// Append-only CSV for Excel import.  Config params are written once on
// the "config" marker row at the start of each invocation.  Data rows
// have only metric columns — linked to their config by run_id.

struct csv_logger
{
   std::string path;
   std::string run_id;

   csv_logger() = default;

   void init(const std::string& p, const bench_config& cfg)
   {
      path = p;
      if (path.empty())
         return;

      auto now = std::chrono::system_clock::now();
      auto tt  = std::chrono::system_clock::to_time_t(now);
      char buf[32];
      std::strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", std::gmtime(&tt));
      run_id = buf;

      // Write header if file is new / empty.
      {
         std::ifstream probe(path);
         if (!probe.good() || probe.peek() == EOF)
         {
            std::ofstream f(path, std::ios::app);
            f << "timestamp,run_id,phase,round,total_keys,"
                 "inserts_per_sec,reads_per_sec,read_found_pct,validation_errors,"
                 "db_size_bytes,free_bytes,alloc_objects,pending_releases,"
                 "pinned_bytes,pinned_segments,recycle_queue_depth\n";
         }
      }

      // Write a config marker row.  We abuse the "phase" column
      // with "config" and put all config values in the numeric columns.
      {
         std::ofstream f(path, std::ios::app);
         f << now_str() << ',' << run_id << ",config,-1,"
           << (cfg.reset_db ? 1 : 0)            // total_keys → reset flag
           << ',' << cfg.rounds                  // inserts_per_sec → rounds
           << ',' << cfg.items                   // reads_per_sec → items
           << ',' << cfg.batch_size              // read_found_pct → batch
           << ',' << cfg.value_size              // validation_errors → value_size
           << ',' << cfg.readers                 // db_size_bytes → readers
           << ',' << cfg.merge_threads           // free_bytes → merge_threads
           << ',' << cfg.max_rw_entries          // alloc_objects → max_rw_entries
           << ',' << cfg.pinned_cache_mb         // pending_releases → pinned_cache_mb
           << ',' << (cfg.validate ? 1 : 0)      // pinned_bytes → validate
           << ',' << 0                            // pinned_segments → reserved
           << '\n';
         f.flush();
      }
   }

   void log_round(const char* phase,
                  uint32_t    round,
                  uint64_t    total_keys,
                  uint64_t    inserts_per_sec,
                  uint64_t    reads_per_sec,
                  double      read_found_pct,
                  uint64_t    validation_errors,
                  uint64_t    db_size_bytes,
                  uint64_t    free_bytes,
                  uint64_t    alloc_objects,
                  uint64_t    pending_releases,
                  uint64_t    pinned_bytes,
                  uint64_t    pinned_segments,
                  uint64_t    recycle_queue_depth)
   {
      if (path.empty())
         return;
      std::ofstream f(path, std::ios::app);
      f << now_str() << ',' << run_id << ',' << phase << ',' << round << ',' << total_keys << ','
        << inserts_per_sec << ',' << reads_per_sec << ',' << std::fixed << std::setprecision(1)
        << read_found_pct << ',' << validation_errors << ',' << db_size_bytes << ',' << free_bytes
        << ',' << alloc_objects << ',' << pending_releases << ',' << pinned_bytes << ','
        << pinned_segments << ',' << recycle_queue_depth << '\n';
      f.flush();
   }

   void log_marker(const char* event)
   {
      if (path.empty())
         return;
      std::ofstream f(path, std::ios::app);
      f << now_str() << ',' << run_id << ',' << event << ",-1,,,,,,,,,,,\n";
      f.flush();
   }

private:
   std::string now_str() const
   {
      auto now = std::chrono::system_clock::now();
      auto tt  = std::chrono::system_clock::to_time_t(now);
      char buf[32];
      std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", std::gmtime(&tt));
      return buf;
   }
};

// ── Reader thread counters ──────────────────────────────────────

struct alignas(128) padded_counters
{
   std::atomic<int64_t>  ops{0};
   std::atomic<int64_t>  found{0};
   std::atomic<uint64_t> validation_errors{0};
};

// ── DB stats dump ───────────────────────────────────────────────

static void print_db_stats(const char*                       label,
                           const std::shared_ptr<database>&  db,
                           const std::shared_ptr<write_session>& ws)
{
   auto stats = db->get_stats();
   std::cout << "── " << label << " ──\n"
             << "  file size:     " << format_size(stats.database_file_bytes) << "\n"
             << "  free space:    " << format_size(stats.total_free_bytes) << "\n"
             << "  alloc objects: " << format_comma(ws->get_total_allocated_objects()) << "\n"
             << "  pinned:        " << format_size(stats.pinned_bytes) << " ("
             << stats.pinned_segments << " segs)\n"
             << "  pending rel:   " << format_comma(stats.pending_releases) << "\n"
             << "  recycle queue: " << stats.recycled_queue_depth << "/"
             << stats.recycled_queue_capacity << "\n";
}

// ── Safety limits ───────────────────────────────────────────────

static constexpr uint64_t KEY_SIZE = 8;

static bool check_safety_limits(const std::filesystem::path& db_dir,
                                uint64_t                     db_bytes,
                                uint64_t                     free_bytes,
                                uint64_t                     total_keys,
                                uint32_t                     value_size)
{
   if (total_keys > 0)
   {
      // Compare actual used space (excluding reclaimable free space)
      // against theoretical minimum.  Use a generous 20x multiplier
      // because the trie creates many internal nodes per key-value
      // pair and total_keys is an overcount on resumed runs.
      uint64_t used            = (db_bytes > free_bytes) ? db_bytes - free_bytes : 0;
      uint64_t theoretical_min = total_keys * (KEY_SIZE + value_size);
      if (theoretical_min > (10ULL << 30) && used > 20 * theoretical_min)
      {
         fprintf(stderr,
                 "\n!!! SAFETY: Used space (%s) exceeds 20x theoretical minimum (%s) "
                 "for %llu keys — possible leak. Stopping.\n",
                 format_size(used).c_str(), format_size(theoretical_min).c_str(),
                 (unsigned long long)total_keys);
         bench::g_interrupted.store(true, std::memory_order_relaxed);
         return true;
      }
   }

   std::error_code ec;
   auto            space = std::filesystem::space(db_dir, ec);
   if (!ec && space.capacity > 0)
   {
      double pct_used = 100.0 * (space.capacity - space.available) / space.capacity;
      if (pct_used > 75.0)
      {
         fprintf(stderr,
                 "\n!!! SAFETY: Disk %.1f%% full (%.1f GB avail). Stopping.\n",
                 pct_used, space.available / (1024.0 * 1024.0 * 1024.0));
         bench::g_interrupted.store(true, std::memory_order_relaxed);
         return true;
      }
   }
   return false;
}

// ── Watchdog ────────────────────────────────────────────────────
//
// Prints progress every 10 seconds so we can see exactly where a hang
// occurs (which operation in which round).

static std::atomic<uint64_t> g_watchdog_ops{0};
static std::atomic<uint32_t> g_watchdog_round{0};
static const char*           g_watchdog_phase = "";

static void watchdog_loop(std::atomic<bool>& stop,
                          const std::shared_ptr<database>& db,
                          const std::shared_ptr<write_session>& ws)
{
   uint64_t prev_ops = 0;
   while (!stop.load(std::memory_order_relaxed))
   {
      std::this_thread::sleep_for(std::chrono::seconds(10));
      if (stop.load(std::memory_order_relaxed))
         break;
      uint64_t cur = g_watchdog_ops.load(std::memory_order_relaxed);
      uint32_t r   = g_watchdog_round.load(std::memory_order_relaxed);
      auto     stats = db->get_stats();
      if (cur == prev_ops)
      {
         fprintf(stderr,
                 "[WATCHDOG] STALL in %s round %u: %llu ops, "
                 "alloc=%llu free=%s pending=%lld recycle=%llu/%llu db=%s\n",
                 g_watchdog_phase, r, (unsigned long long)cur,
                 (unsigned long long)ws->get_total_allocated_objects(),
                 format_size(stats.total_free_bytes).c_str(),
                 (long long)stats.pending_releases,
                 (unsigned long long)stats.recycled_queue_depth,
                 (unsigned long long)stats.recycled_queue_capacity,
                 format_size(stats.database_file_bytes).c_str());
      }
      prev_ops = cur;
   }
}

// ── Write-only benchmark ────────────────────────────────────────

static void write_only_bench(const bench_config& cfg,
                             bool                use_dwal,
                             csv_logger&         csv,
                             const std::filesystem::path& db_dir)
{
   const char* phase_name = use_dwal ? "write_dwal" : "write_direct";

   sal::runtime_config rcfg;
   rcfg.max_pinned_cache_size_mb = cfg.no_mlock ? 0 : cfg.pinned_cache_mb;
   // Lower compaction thresholds to prevent deadlock when free space is
   // fragmented across many segments (each below the per-segment threshold).
   // Default pinned=4MB means compaction stops when segments avg <4MB freed,
   // even if total free space is gigabytes.
   rcfg.compact_pinned_unused_threshold_mb   = 1;
   rcfg.compact_unpinned_unused_threshold_mb = 2;

   auto db = database::open(db_dir, psitri::open_mode::create_or_open, rcfg);
   auto ws = db->start_write_session();
   print_db_stats("open", db, ws);
   uint64_t initial_alloc = ws->get_total_allocated_objects();

   // Start watchdog thread.
   g_watchdog_phase = phase_name;
   g_watchdog_ops.store(0, std::memory_order_relaxed);
   g_watchdog_round.store(0, std::memory_order_relaxed);
   std::atomic<bool> watchdog_stop{false};
   std::thread       watchdog_thread(watchdog_loop, std::ref(watchdog_stop),
                                     std::cref(db), std::cref(ws));

   std::cout << "═══════════════════════════════════════════════════════════════\n"
             << "  " << phase_name << " — write-only throughput\n"
             << "  rounds=" << cfg.rounds << " items=" << format_comma(cfg.items)
             << " batch=" << cfg.batch_size << " val_size=" << cfg.value_size
             << " validate=" << (cfg.validate ? "yes" : "no") << "\n";
   if (use_dwal)
      std::cout << "  merge_threads=" << cfg.merge_threads
                << " max_rw_entries=" << format_comma(cfg.max_rw_entries) << "\n";
   std::cout << "═══════════════════════════════════════════════════════════════\n";

   std::unique_ptr<dwal::dwal_database> dwal_db;
   if (use_dwal)
   {
      dwal::dwal_config dcfg;
      dcfg.merge_threads  = cfg.merge_threads;
      dcfg.max_rw_entries = cfg.max_rw_entries;
      dwal_db = std::make_unique<dwal::dwal_database>(db, db_dir / "wal", dcfg);
      g_active_dwal.store(dwal_db.get(), std::memory_order_relaxed);
   }

   std::vector<char> key;
   uint64_t          seq = 0;
   auto rand_key = [](uint64_t s, std::vector<char>& v) { to_key(rand_from_seq(s), v); };

   auto overall_start = std::chrono::steady_clock::now();

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      g_watchdog_round.store(r, std::memory_order_relaxed);
      reshuffle_random_buf();
      auto     start    = std::chrono::steady_clock::now();
      uint32_t inserted = 0;

      if (use_dwal)
      {
         while (inserted < cfg.items && !bench::interrupted())
         {
            auto     tx    = dwal_db->start_write_transaction(0);
            uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
            for (uint32_t i = 0; i < batch; ++i)
            {
               rand_key(seq, key);
               auto val = cfg.validate
                              ? std::string_view(validation_value(key.data(), key.size(), cfg.value_size).data(), cfg.value_size)
                              : std::string_view(random_value(seq, cfg.value_size).data(), cfg.value_size);
               tx.upsert(std::string_view(key.data(), key.size()), val);
               ++seq;
               ++inserted;
            }
            tx.commit();
            g_watchdog_ops.store(seq, std::memory_order_relaxed);
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
               auto val = cfg.validate ? validation_value(key.data(), key.size(), cfg.value_size)
                                       : random_value(seq, cfg.value_size);
               tx.upsert(key_view(key.data(), key.size()), val);
               ++seq;
               ++inserted;
            }
            tx.commit();
            g_watchdog_ops.store(seq, std::memory_order_relaxed);
         }
      }

      auto     end      = std::chrono::steady_clock::now();
      double   secs     = std::chrono::duration<double>(end - start).count();
      uint64_t ips      = uint64_t(inserted / secs);
      auto     db_bytes = dir_size_bytes(db_dir);
      auto     stats    = db->get_stats();

      std::cout << std::setw(4) << std::left << r << " " << std::setw(14) << std::right
                << format_comma(seq) << "  " << std::setw(12) << format_comma(ips)
                << "  upserts/sec  db=" << format_size(db_bytes)
                << "  free=" << format_size(stats.total_free_bytes)
                << "  alloc=" << format_comma(ws->get_total_allocated_objects())
                << "  pending=" << format_comma(ws->get_pending_release_count())
                << "  rq=" << stats.recycled_queue_depth << "/" << stats.recycled_queue_capacity
                << std::endl;

      csv.log_round(phase_name, r, seq, ips, 0, 0.0, 0, db_bytes, stats.total_free_bytes,
                    ws->get_total_allocated_objects(), ws->get_pending_release_count(),
                    stats.pinned_bytes, stats.pinned_segments, stats.recycled_queue_depth);

      check_safety_limits(db_dir, db_bytes, stats.total_free_bytes, initial_alloc + seq, cfg.value_size);
   }

   if (dwal_db)
      dwal_db->request_shutdown();
   watchdog_stop.store(true, std::memory_order_relaxed);
   watchdog_thread.join();

   auto   overall_end  = std::chrono::steady_clock::now();
   double overall_secs = std::chrono::duration<double>(overall_end - overall_start).count();
   std::cout << "───────────────────────────────────────────────────────────────\n"
             << "total: " << format_comma(seq) << " upserts in " << std::fixed
             << std::setprecision(3) << overall_secs << " sec  ("
             << format_comma(uint64_t(seq / overall_secs)) << " upserts/sec)\n";
   g_active_dwal.store(nullptr, std::memory_order_relaxed);
   print_db_stats("close", db, ws);
}

// ── Write + concurrent read benchmark ──────────────────────────
//
// 1 writer thread (main), N reader threads, trie read mode only.

static void rw_bench(const bench_config& cfg,
                     bool                use_dwal,
                     csv_logger&         csv,
                     const std::filesystem::path& db_dir)
{
   const char* phase_name = use_dwal ? "rw_dwal" : "rw_direct";

   sal::runtime_config rcfg;
   rcfg.max_pinned_cache_size_mb = cfg.no_mlock ? 0 : cfg.pinned_cache_mb;
   // Lower compaction thresholds to prevent deadlock when free space is
   // fragmented across many segments (each below the per-segment threshold).
   // Default pinned=4MB means compaction stops when segments avg <4MB freed,
   // even if total free space is gigabytes.
   rcfg.compact_pinned_unused_threshold_mb   = 1;
   rcfg.compact_unpinned_unused_threshold_mb = 2;

   auto db = database::open(db_dir, psitri::open_mode::create_or_open, rcfg);
   auto ws = db->start_write_session();
   print_db_stats("open", db, ws);
   uint64_t initial_alloc = ws->get_total_allocated_objects();

   // Start watchdog thread.
   g_watchdog_phase = phase_name;
   g_watchdog_ops.store(0, std::memory_order_relaxed);
   g_watchdog_round.store(0, std::memory_order_relaxed);
   std::atomic<bool> watchdog_stop{false};
   std::thread       watchdog_thread(watchdog_loop, std::ref(watchdog_stop),
                                     std::cref(db), std::cref(ws));

   std::cout << "═══════════════════════════════════════════════════════════════\n"
             << "  " << phase_name << " — write + " << cfg.readers
             << " trie readers (concurrent)\n"
             << "  rounds=" << cfg.rounds << " items=" << format_comma(cfg.items)
             << " batch=" << cfg.batch_size << " val_size=" << cfg.value_size
             << " validate=" << (cfg.validate ? "yes" : "no") << "\n";
   if (use_dwal)
      std::cout << "  merge_threads=" << cfg.merge_threads
                << " max_rw_entries=" << format_comma(cfg.max_rw_entries) << "\n";
   std::cout << "═══════════════════════════════════════════════════════════════\n";

   std::unique_ptr<dwal::dwal_database> dwal_db;
   if (use_dwal)
   {
      dwal::dwal_config dcfg;
      dcfg.merge_threads  = cfg.merge_threads;
      dcfg.max_rw_entries = cfg.max_rw_entries;
      dwal_db = std::make_unique<dwal::dwal_database>(db, db_dir / "wal", dcfg);
      g_active_dwal.store(dwal_db.get(), std::memory_order_relaxed);
   }

   // Seed initial data so readers have something to hit.
   auto rand_key = [](uint64_t s, std::vector<char>& v) { to_key(rand_from_seq(s), v); };
   uint64_t seq = 0;
   {
      std::vector<char> key;
      uint32_t          seed_count = std::min(cfg.items, uint32_t(100000));
      uint32_t          seeded     = 0;

      if (use_dwal)
      {
         while (seeded < seed_count)
         {
            auto     tx    = dwal_db->start_write_transaction(0);
            uint32_t batch = std::min(cfg.batch_size, seed_count - seeded);
            for (uint32_t i = 0; i < batch; ++i)
            {
               rand_key(seq, key);
               auto val = cfg.validate
                              ? std::string_view(validation_value(key.data(), key.size(), cfg.value_size).data(), cfg.value_size)
                              : std::string_view(random_value(seq, cfg.value_size).data(), cfg.value_size);
               tx.upsert(std::string_view(key.data(), key.size()), val);
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
               auto val = cfg.validate ? validation_value(key.data(), key.size(), cfg.value_size)
                                       : random_value(seq, cfg.value_size);
               tx.upsert(key_view(key.data(), key.size()), val);
               ++seq;
               ++seeded;
            }
            tx.commit();
         }
      }
      std::cout << "seeded " << format_comma(seq) << " keys\n";
   }

   // ── Launch reader threads ────────────────────────────────────
   std::atomic<uint64_t> committed_seq{seq};
   std::atomic<bool>     done{false};

   std::vector<std::unique_ptr<padded_counters>> counters;
   counters.reserve(cfg.readers);
   for (uint32_t t = 0; t < cfg.readers; ++t)
      counters.push_back(std::make_unique<padded_counters>());

   std::vector<std::thread> readers;
   for (uint32_t t = 0; t < cfg.readers; ++t)
   {
      readers.emplace_back(
          [&, t]()
          {
             sal::set_current_thread_name("reader");
             std::vector<char> key;
             int64_t           local_ops     = 0;
             int64_t           local_found   = 0;
             uint64_t          local_val_err = 0;
             const uint64_t    salt          = rand_from_seq(t * 999983ULL + 1);

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

             while (!done.load(std::memory_order_relaxed) && !bench::interrupted())
             {
                // Refresh direct-COW cursor periodically to pick up new writes.
                if (!dwal_db && (local_ops % 10000) == 0 && tri_rs)
                   tri_cur.refresh(0);

                uint64_t max_seq = committed_seq.load(std::memory_order_relaxed);
                for (uint32_t i = 0; i < 1000; ++i)
                {
                   uint64_t s = rand_from_seq(local_ops + salt) % max_seq;
                   to_key(rand_from_seq(s), key);

                   bool        found = false;
                   std::string val_buf;

                   if (dwal_reader)
                   {
                      auto result =
                          dwal_reader->get(0, std::string_view(key.data(), key.size()),
                                          dwal::read_mode::trie);
                      found   = result.found;
                      val_buf = std::move(result.value);
                   }
                   else
                   {
                      found = tri_cur.get(key_view(key.data(), key.size()), &val_buf) >= 0;
                   }

                   if (found)
                   {
                      ++local_found;
                      if (cfg.validate && !verify_value(key.data(), key.size(), val_buf))
                         ++local_val_err;
                   }
                   ++local_ops;
                }
                counters[t]->ops.store(local_ops, std::memory_order_relaxed);
                counters[t]->found.store(local_found, std::memory_order_relaxed);
                counters[t]->validation_errors.store(local_val_err, std::memory_order_relaxed);
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
   auto sum_reader_found = [&]() -> int64_t
   {
      int64_t total = 0;
      for (uint32_t i = 0; i < cfg.readers; ++i)
         total += counters[i]->found.load(std::memory_order_relaxed);
      return total;
   };
   auto sum_val_errors = [&]() -> uint64_t
   {
      uint64_t total = 0;
      for (uint32_t i = 0; i < cfg.readers; ++i)
         total += counters[i]->validation_errors.load(std::memory_order_relaxed);
      return total;
   };

   // ── Writer loop ──────────────────────────────────────────────
   std::vector<char> key;
   int64_t           prev_ops   = 0;
   int64_t           prev_found = 0;
   auto              phase_start = std::chrono::steady_clock::now();

   for (uint32_t r = 0; r < cfg.rounds && !bench::interrupted(); ++r)
   {
      g_watchdog_round.store(r, std::memory_order_relaxed);
      reshuffle_random_buf();
      auto     start    = std::chrono::steady_clock::now();
      uint32_t inserted = 0;

      if (use_dwal)
      {
         while (inserted < cfg.items && !bench::interrupted())
         {
            auto     tx    = dwal_db->start_write_transaction(0);
            uint32_t batch = std::min(cfg.batch_size, cfg.items - inserted);
            for (uint32_t i = 0; i < batch; ++i)
            {
               rand_key(seq, key);
               auto val = cfg.validate
                              ? std::string_view(validation_value(key.data(), key.size(), cfg.value_size).data(), cfg.value_size)
                              : std::string_view(random_value(seq, cfg.value_size).data(), cfg.value_size);
               tx.upsert(std::string_view(key.data(), key.size()), val);
               ++seq;
               ++inserted;
            }
            tx.commit();
            committed_seq.store(seq, std::memory_order_relaxed);
            g_watchdog_ops.store(seq, std::memory_order_relaxed);
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
               auto val = cfg.validate ? validation_value(key.data(), key.size(), cfg.value_size)
                                       : random_value(seq, cfg.value_size);
               tx.upsert(key_view(key.data(), key.size()), val);
               ++seq;
               ++inserted;
            }
            tx.commit();
            committed_seq.store(seq, std::memory_order_relaxed);
            g_watchdog_ops.store(seq, std::memory_order_relaxed);
         }
      }

      auto     end       = std::chrono::steady_clock::now();
      double   secs      = std::chrono::duration<double>(end - start).count();
      uint64_t ips       = uint64_t(inserted / secs);

      auto     cur_ops   = sum_reader_ops();
      auto     cur_found = sum_reader_found();
      auto     delta_ops = cur_ops - prev_ops;
      uint64_t rps       = uint64_t(delta_ops / secs);
      double   found_pct = (delta_ops > 0)
                               ? 100.0 * (cur_found - prev_found) / delta_ops
                               : 0.0;
      prev_ops   = cur_ops;
      prev_found = cur_found;

      auto     db_bytes  = dir_size_bytes(db_dir);
      auto     stats     = db->get_stats();
      uint64_t val_err   = sum_val_errors();

      std::cout << std::setw(4) << std::left << r << " " << std::setw(12) << std::right
                << format_comma(ips) << "  writes/sec  " << std::setw(12)
                << format_comma(rps) << "  reads/sec  found=" << std::fixed
                << std::setprecision(1) << found_pct << "%"
                << "  db=" << format_size(db_bytes)
                << "  rq=" << stats.recycled_queue_depth << "/" << stats.recycled_queue_capacity;
      if (cfg.validate)
         std::cout << "  val_err=" << val_err;
      std::cout << std::endl;

      csv.log_round(phase_name, r, seq, ips, rps, found_pct, val_err, db_bytes,
                    stats.total_free_bytes, ws->get_total_allocated_objects(),
                    ws->get_pending_release_count(), stats.pinned_bytes,
                    stats.pinned_segments, stats.recycled_queue_depth);

      check_safety_limits(db_dir, db_bytes, stats.total_free_bytes, initial_alloc + seq, cfg.value_size);

      // ── Reopen database if configured ──────────────────────────────
      if (cfg.reopen_every > 0 && (r + 1) % cfg.reopen_every == 0 && r + 1 < cfg.rounds)
      {
         // Stop readers
         done.store(true, std::memory_order_relaxed);
         for (auto& t : readers)
            t.join();
         readers.clear();

         // Stop watchdog
         watchdog_stop.store(true, std::memory_order_relaxed);
         watchdog_thread.join();

         // Destroy DWAL, write session, database (order matters)
         auto reopen_start = std::chrono::steady_clock::now();
         g_active_dwal.store(nullptr, std::memory_order_relaxed);
         dwal_db.reset();
         print_db_stats("close (reopen)", db, ws);
         ws.reset();
         db.reset();

         // Reopen
         db = database::open(db_dir, psitri::open_mode::create_or_open, rcfg);
         ws = db->start_write_session();
         initial_alloc = ws->get_total_allocated_objects();

         if (use_dwal)
         {
            dwal::dwal_config dcfg;
            dcfg.merge_threads  = cfg.merge_threads;
            dcfg.max_rw_entries = cfg.max_rw_entries;
            dwal_db = std::make_unique<dwal::dwal_database>(db, db_dir / "wal", dcfg);
            g_active_dwal.store(dwal_db.get(), std::memory_order_relaxed);
         }

         auto reopen_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - reopen_start)
                              .count();
         print_db_stats("reopen", db, ws);
         std::cout << "── reopen at round " << (r + 1) << " (" << reopen_ms << " ms) ──\n";

         // Reset counters and restart readers
         done.store(false, std::memory_order_relaxed);
         for (uint32_t t = 0; t < cfg.readers; ++t)
         {
            counters[t]->ops.store(0, std::memory_order_relaxed);
            counters[t]->found.store(0, std::memory_order_relaxed);
            counters[t]->validation_errors.store(0, std::memory_order_relaxed);
         }
         prev_ops   = 0;
         prev_found = 0;

         for (uint32_t t = 0; t < cfg.readers; ++t)
         {
            readers.emplace_back(
                [&, t]()
                {
                   sal::set_current_thread_name("reader");
                   std::vector<char> lkey;
                   int64_t           local_ops     = 0;
                   int64_t           local_found   = 0;
                   uint64_t          local_val_err = 0;
                   const uint64_t    salt          = rand_from_seq(t * 999983ULL + r);

                   std::optional<dwal::dwal_read_session> dwal_reader;
                   std::shared_ptr<read_session>          tri_rs;
                   psitri::cursor                         tri_cur{sal::smart_ptr<sal::alloc_header>{}};

                   if (dwal_db)
                      dwal_reader.emplace(*dwal_db);
                   else
                   {
                      tri_rs  = db->start_read_session();
                      tri_cur = tri_rs->create_cursor(0);
                   }

                   while (!done.load(std::memory_order_relaxed) && !bench::interrupted())
                   {
                      if (!dwal_db && (local_ops % 10000) == 0 && tri_rs)
                         tri_cur.refresh(0);

                      uint64_t max_seq = committed_seq.load(std::memory_order_relaxed);
                      for (uint32_t i = 0; i < 1000; ++i)
                      {
                         uint64_t s = rand_from_seq(local_ops + salt) % max_seq;
                         to_key(rand_from_seq(s), lkey);

                         bool        found = false;
                         std::string val_buf;

                         if (dwal_reader)
                         {
                            auto result = dwal_reader->get(
                                0, std::string_view(lkey.data(), lkey.size()),
                                dwal::read_mode::trie);
                            found   = result.found;
                            val_buf = std::move(result.value);
                         }
                         else
                            found = tri_cur.get(key_view(lkey.data(), lkey.size()), &val_buf) >= 0;

                         if (found)
                         {
                            ++local_found;
                            if (cfg.validate && !verify_value(lkey.data(), lkey.size(), val_buf))
                               ++local_val_err;
                         }
                         ++local_ops;
                      }
                      counters[t]->ops.store(local_ops, std::memory_order_relaxed);
                      counters[t]->found.store(local_found, std::memory_order_relaxed);
                      counters[t]->validation_errors.store(local_val_err, std::memory_order_relaxed);
                   }
                });
         }

         // Restart watchdog
         g_watchdog_phase = phase_name;
         g_watchdog_ops.store(seq, std::memory_order_relaxed);
         g_watchdog_round.store(r + 1, std::memory_order_relaxed);
         watchdog_stop.store(false, std::memory_order_relaxed);
         watchdog_thread = std::thread(watchdog_loop, std::ref(watchdog_stop),
                                       std::cref(db), std::cref(ws));
      }
   }

   done.store(true, std::memory_order_relaxed);
   if (dwal_db)
      dwal_db->request_shutdown();
   watchdog_stop.store(true, std::memory_order_relaxed);
   watchdog_thread.join();
   for (auto& t : readers)
      t.join();

   auto     phase_end  = std::chrono::steady_clock::now();
   double   phase_secs = std::chrono::duration<double>(phase_end - phase_start).count();
   int64_t  total_ops  = sum_reader_ops();
   int64_t  total_found = sum_reader_found();

   std::cout << "───────────────────────────────────────────────────────────────\n"
             << "total: " << format_comma(seq) << " writes, "
             << format_comma(total_ops) << " reads in " << std::fixed << std::setprecision(3)
             << phase_secs << " sec\n"
             << "  writes: " << format_comma(uint64_t(seq / phase_secs)) << "/sec\n"
             << "  reads:  " << format_comma(uint64_t(total_ops / phase_secs)) << "/sec  ("
             << format_comma(total_found) << " found / " << format_comma(total_ops) << " = "
             << std::fixed << std::setprecision(1)
             << (total_ops > 0 ? 100.0 * total_found / total_ops : 0.0) << "%)\n";
   if (cfg.validate)
      std::cout << "  validation errors: " << sum_val_errors() << "\n";
   g_active_dwal.store(nullptr, std::memory_order_relaxed);
   print_db_stats("close", db, ws);
}

// ── Crash handler ───────────────────────────────────────────────

static void crash_handler(int sig, siginfo_t* info, void* /*ctx*/)
{
   const char* name = (sig == SIGBUS) ? "SIGBUS" : (sig == SIGSEGV) ? "SIGSEGV" : "SIGILL";
   fprintf(stderr, "\n=== %s at addr %p ===\n", name, info->si_addr);
   _exit(128 + sig);
}

// ── Help ────────────────────────────────────────────────────────

static void print_help()
{
   std::cout << R"(dwal-bench — PsiTri DWAL / direct COW stress test

USAGE:
  dwal-bench [MODE...] [OPTIONS]

MODES (default: --dwal --rw):
  --write-only        Write-only throughput (no readers)
  --rw                Write + concurrent trie readers
  --direct            Use direct COW backend (no DWAL buffering)
  --dwal              Use DWAL buffered backend
  --all               All combinations (write-only + rw, direct + dwal)

OPTIONS:
  -r, --rounds N          Rounds per invocation       (default: 10)
  -i, --items N           Items per round              (default: 1,000,000)
  -b, --batch N           Batch size per transaction   (default: 10)
  -s, --value-size N      Value size in bytes          (default: 32)
  -t, --readers N         Reader threads               (default: 6)
  --merge-threads N       DWAL merge pool threads      (default: 2)
  --max-rw N              DWAL RW btree swap threshold (default: 100,000)
  -d, --db-dir PATH       Database directory prefix    (default: ./dwal_bench_db)
  --csv-log PATH          CSV log file                 (default: ./dwal_bench_results.csv)
  --reset                 Wipe DB directories before starting
  --no-mlock              Disable pinned-segment mlock
  --pinned-cache-mb N     Pinned cache budget in MB    (default: 8192)
  --validate              Verify read values (xxhash of key)
  --reopen-every N        Close and reopen DB every N rounds (default: 0 = disabled)
  -h, --help              Show this help

CSV LOG:
  Results are appended to --csv-log.  To pretty-print in the terminal:
    column -t -s, dwal_bench_results.csv | less -S

EXAMPLES:
  # DWAL read-write stress test (default), reset DB, with validation:
  dwal-bench --reset --validate

  # Direct COW write-only, 100 rounds:
  dwal-bench --direct --write-only -r 100

  # All 4 combinations, large cache:
  dwal-bench --all --pinned-cache-mb 61440 --reset
)";
}

// ── Main ────────────────────────────────────────────────────────

int main(int argc, char** argv)
{
   struct sigaction sa{};
   sa.sa_sigaction = crash_handler;
   sa.sa_flags     = SA_SIGINFO;
   sigaction(SIGBUS, &sa, nullptr);
   sigaction(SIGSEGV, &sa, nullptr);
   // Custom signal handler: sets interrupted flag AND wakes any blocked
   // merge backpressure wait so the writer can exit promptly.
   {
      struct sigaction isa{};
      isa.sa_handler = [](int sig)
      {
         bench::signal_handler(sig);
         auto* dwal = g_active_dwal.load(std::memory_order_relaxed);
         if (dwal)
            dwal->request_shutdown();
      };
      isa.sa_flags = 0;
      sigemptyset(&isa.sa_mask);
      sigaction(SIGINT, &isa, nullptr);
      sigaction(SIGTERM, &isa, nullptr);
   }

   sal::set_current_thread_name("main");
   reshuffle_random_buf();

   bench_config cfg;

   for (int i = 1; i < argc; ++i)
   {
      std::string arg = argv[i];
      auto        next = [&]() -> std::string { return (i + 1 < argc) ? argv[++i] : ""; };

      if (arg == "--rounds" || arg == "-r")
         cfg.rounds = std::stoi(next());
      else if (arg == "--items" || arg == "-i")
         cfg.items = std::stoi(next());
      else if (arg == "--batch" || arg == "-b")
         cfg.batch_size = std::stoi(next());
      else if (arg == "--value-size" || arg == "-s")
         cfg.value_size = std::stoi(next());
      else if (arg == "--readers" || arg == "-t")
         cfg.readers = std::stoi(next());
      else if (arg == "--merge-threads")
         cfg.merge_threads = std::stoi(next());
      else if (arg == "--max-rw")
         cfg.max_rw_entries = std::stoi(next());
      else if (arg == "--db-dir" || arg == "-d")
         cfg.db_dir = next();
      else if (arg == "--csv-log")
         cfg.csv_path = next();
      else if (arg == "--pinned-cache-mb")
         cfg.pinned_cache_mb = std::stoull(next());
      else if (arg == "--reopen-every")
         cfg.reopen_every = std::stoi(next());
      else if (arg == "--reset")
         cfg.reset_db = true;
      else if (arg == "--no-mlock")
         cfg.no_mlock = true;
      else if (arg == "--validate")
         cfg.validate = true;
      else if (arg == "--write-only")
         cfg.run_write_only = true;
      else if (arg == "--rw")
         cfg.run_rw = true;
      else if (arg == "--direct")
         cfg.run_direct = true;
      else if (arg == "--dwal")
         cfg.run_dwal = true;
      else if (arg == "--all")
      {
         cfg.run_write_only = true;
         cfg.run_rw         = true;
         cfg.run_direct     = true;
         cfg.run_dwal       = true;
      }
      else if (arg == "--help" || arg == "-h")
      {
         print_help();
         return 0;
      }
      else
      {
         fprintf(stderr, "unknown option: %s\n", arg.c_str());
         return 1;
      }
   }

   // Default mode: --dwal --rw
   if (!cfg.run_write_only && !cfg.run_rw)
      cfg.run_rw = true;
   if (!cfg.run_direct && !cfg.run_dwal)
      cfg.run_dwal = true;

   // Print banner.
   std::cout << "\n╔═══════════════════════════════════════════════════════════════╗\n"
             << "║              PsiTri Stress Benchmark                         ║\n"
             << "╚═══════════════════════════════════════════════════════════════╝\n\n";

   std::cout << "config:\n"
             << "  rounds=" << cfg.rounds << " items=" << format_comma(cfg.items)
             << " batch=" << cfg.batch_size << " value_size=" << cfg.value_size << "\n"
             << "  readers=" << cfg.readers << " merge_threads=" << cfg.merge_threads
             << " max_rw=" << format_comma(cfg.max_rw_entries) << "\n"
             << "  pinned_cache=" << format_size(cfg.pinned_cache_mb * 1024ULL * 1024ULL)
             << " mlock=" << (cfg.no_mlock ? "off" : "on")
             << " validate=" << (cfg.validate ? "yes" : "no") << "\n"
             << "  db_dir=" << cfg.db_dir << " csv=" << cfg.csv_path << "\n"
             << "  modes:";
   if (cfg.run_write_only)
      std::cout << " write-only";
   if (cfg.run_rw)
      std::cout << " rw";
   std::cout << " | backends:";
   if (cfg.run_direct)
      std::cout << " direct";
   if (cfg.run_dwal)
      std::cout << " dwal";
   std::cout << "\n\n";

   // Init CSV logger.
   csv_logger csv;
   csv.init(cfg.csv_path, cfg);

   // Build the list of (use_dwal, write_only) phases to run.
   struct phase
   {
      bool use_dwal;
      bool write_only;
   };
   std::vector<phase> phases;

   if (cfg.run_direct && cfg.run_write_only)
      phases.push_back({false, true});
   if (cfg.run_dwal && cfg.run_write_only)
      phases.push_back({true, true});
   if (cfg.run_direct && cfg.run_rw)
      phases.push_back({false, false});
   if (cfg.run_dwal && cfg.run_rw)
      phases.push_back({true, false});

   for (auto& p : phases)
   {
      if (bench::interrupted())
         break;

      std::string suffix = std::string(p.use_dwal ? "_dwal" : "_direct") +
                           (p.write_only ? "_w" : "_rw");
      auto dir = std::filesystem::path(cfg.db_dir + suffix);

      if (cfg.reset_db)
      {
         std::error_code ec;
         std::filesystem::remove_all(dir, ec);
      }

      if (p.write_only)
         write_only_bench(cfg, p.use_dwal, csv, dir);
      else
         rw_bench(cfg, p.use_dwal, csv, dir);

      std::cout << "\n";
   }

   csv.log_marker("run_end");
   std::cout << "done.\n";
   return 0;
}
