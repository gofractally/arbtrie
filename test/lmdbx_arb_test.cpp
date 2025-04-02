#include <limits.h>  // For ULONG_MAX if needed by mdbx.h internals
#include <atomic>
#include <boost/program_options.hpp>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>   // For sprintf with MDBX_val
#include <cstring>  // For memcpy with MDBX_val
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>  // For concurrent test
#include <random>
#include <string>
#include <thread>
#include <vector>

// Include mdbx header
#include <mdbx.h>

// --- Utility Functions (Copied/Adapted from arb.cpp) ---

// Thread-safe way to name threads (platform specific)
#ifdef __APPLE__
#include <pthread.h>
void thread_name(const std::string& name)
{
   pthread_setname_np(name.c_str());
}
#elif defined(__linux__)
#include <pthread.h>
void thread_name(const std::string& name)
{
   pthread_setname_np(pthread_self(), name.c_str());
}
#else
void thread_name(const std::string& name)
{ /* No-op */
}
#endif

int64_t rand64()
{
   thread_local static std::mt19937 gen(
       std::random_device{}());  // Use random_device for better seeding per thread
   return (uint64_t(gen()) << 32) | gen();
}

uint64_t bswap(uint64_t x)
{
   x = (x & 0x00000000FFFFFFFF) << 32 | (x & 0xFFFFFFFF00000000) >> 32;
   x = (x & 0x0000FFFF0000FFFF) << 16 | (x & 0xFFFF0000FFFF0000) >> 16;
   x = (x & 0x00FF00FF00FF00FF) << 8 | (x & 0xFF00FF00FF00FF00) >> 8;
   return x;
}

// Simple comma formatting for readability
std::string add_comma(uint64_t n)
{
   std::string s          = std::to_string(n);
   int         insert_pos = s.length() - 3;
   while (insert_pos > 0)
   {
      s.insert(insert_pos, ",");
      insert_pos -= 3;
   }
   return s;
}

void print_hex(const MDBX_val* val)
{
   if (!val || !val->iov_base || val->iov_len == 0)
   {
      std::cout << "<empty>";
      return;
   }
   const unsigned char* data = static_cast<const unsigned char*>(val->iov_base);
   std::cout << std::hex << std::setfill('0');
   for (size_t i = 0; i < val->iov_len; ++i)
   {
      std::cout << std::setw(2) << static_cast<unsigned int>(data[i]);
   }
   std::cout << std::dec << std::setfill(' ');  // Reset to decimal
}

// --- MDBX Helper ---
// Function to check MDBX return code and print error
inline void check_mdbx_rc(int rc, const std::string& msg)
{
   if (rc != MDBX_SUCCESS)
   {
      std::cerr << "MDBX Error: " << msg << " (" << rc << ": " << mdbx_strerror(rc) << ")"
                << std::endl;
      // Decide on error handling: throw, exit, or return false? For a test, exit might be okay.
      exit(EXIT_FAILURE);
   }
}

// --- Main Test Function ---

int main(int argc, char** argv)
{
   thread_name("main");  // Name the main thread

   namespace po        = boost::program_options;
   std::string db_path = "lmdbx-test-db";  // Database directory path

   // --- Command Line Options ---
   int      count              = 1000000;
   int      batch_size         = 100;
   int      rounds             = 3;
   int      multithread_rounds = 20;
   int      num_read_threads   = 15;  // Number of concurrent reader threads
   uint64_t map_size_mb =
       1024 * 4;  // Default initial map size (e.g., 4GB) - MDBX grows automatically
   bool run_dense_rand        = true;
   bool run_little_endian_seq = true;
   bool run_big_endian_seq    = true;
   bool run_big_endian_rev    = true;
   bool run_rand_string       = true;
   bool run_get_known_le_seq  = true;
   bool run_get_known_le_rand = true;
   bool run_get_known_be_seq  = true;
   bool run_lower_bound_rand  = true;
   bool run_concurrent_rw     = true;

   unsigned mdbx_sync_flags =
       MDBX_SAFE_NOSYNC | MDBX_NOMETASYNC;  // Default to fast but safe sync mode
   unsigned mdbx_env_flags = MDBX_LIFORECLAIM | MDBX_NOSUBDIR;  // Flags for mdbx_env_open

   // clang-format off
    po::options_description desc("MDBX Benchmark Options");
    desc.add_options()
        ("help,h", "Print help message")
        ("db-path", po::value<std::string>(&db_path)->default_value("lmdbx-test-db"), "Database directory path")
        ("dense-rand", po::bool_switch(&run_dense_rand)->default_value(true), "Run dense random insert test")
        ("little-endian-seq", po::bool_switch(&run_little_endian_seq)->default_value(true), "Run little endian sequential insert test")
        ("big-endian-seq", po::bool_switch(&run_big_endian_seq)->default_value(true), "Run big endian sequential insert test")
        ("big-endian-rev", po::bool_switch(&run_big_endian_rev)->default_value(true), "Run big endian reverse sequential insert test")
        ("rand-string", po::bool_switch(&run_rand_string)->default_value(true), "Run random string insert test")
        ("get-known-le-seq", po::bool_switch(&run_get_known_le_seq)->default_value(true), "Run get known key little endian seq test")
        ("get-known-le-rand", po::bool_switch(&run_get_known_le_rand)->default_value(true), "Run get known key little endian rand test")
        ("get-known-be-seq", po::bool_switch(&run_get_known_be_seq)->default_value(true), "Run get known key big endian seq test")
        ("lower-bound-rand", po::bool_switch(&run_lower_bound_rand)->default_value(true), "Run lower bound random i64 test")
        ("concurrent-rw", po::bool_switch(&run_concurrent_rw)->default_value(true), "Run concurrent read/write test")
        ("sync-mode", po::value<std::string>()->default_value("safe"), "Sync mode: safe (SAFE_NOSYNC), none (UTTERLY_NOSYNC), full (SYNC_DURABLE)")
        ("writemap", po::bool_switch()->default_value(false), "Use MDBX_WRITEMAP mode")
        ("map-size-mb", po::value<uint64_t>(&map_size_mb)->default_value(4096), "Initial/Upper geometry limit in MB")
        ("count", po::value<int>(&count)->default_value(1000000), "Number of items per round")
        ("batch-size", po::value<int>(&batch_size)->default_value(100), "Number of items per transaction batch")
        ("rounds", po::value<int>(&rounds)->default_value(3), "Number of rounds for single-thread tests")
        ("multithread-rounds", po::value<int>(&multithread_rounds)->default_value(20), "Number of rounds for multi-thread test")
        ("read-threads", po::value<int>(&num_read_threads)->default_value(15), "Number of reader threads for concurrent test");
   // clang-format on

   po::variables_map vm;
   try
   {
      po::store(po::parse_command_line(argc, argv, desc), vm);
      po::notify(vm);
   }
   catch (const std::exception& e)
   {
      std::cerr << "Error parsing options: " << e.what() << std::endl;
      std::cout << desc << std::endl;
      return 1;
   }

   if (vm.count("help"))
   {
      std::cout << desc << std::endl;
      return 0;
   }

   std::string sync_mode_str = vm["sync-mode"].as<std::string>();
   if (sync_mode_str == "safe")
      mdbx_sync_flags = MDBX_SAFE_NOSYNC;
   else if (sync_mode_str == "none")
      mdbx_sync_flags = MDBX_UTTERLY_NOSYNC;
   else if (sync_mode_str == "full")
      mdbx_sync_flags = MDBX_SYNC_DURABLE;
   else
   {
      std::cerr << "Invalid sync-mode: " << sync_mode_str << ". Use 'safe', 'none', or 'full'."
                << std::endl;
      return 1;
   }

   if (vm["writemap"].as<bool>())
   {
      mdbx_env_flags |= MDBX_WRITEMAP;
      std::cout << "Using MDBX_WRITEMAP mode." << std::endl;
   }
   mdbx_env_flags |= mdbx_sync_flags;  // Combine sync flags

   // --- Print Configuration ---
   std::cout << "MDBX Benchmark Configuration:" << std::endl;
   std::cout << "  Database Path: " << db_path << std::endl;
   std::cout << "  Items per round: " << add_comma(count) << std::endl;
   std::cout << "  Batch Size: " << add_comma(batch_size) << std::endl;
   std::cout << "  Single-thread Rounds: " << rounds << std::endl;
   std::cout << "  Multi-thread Rounds: " << multithread_rounds << std::endl;
   std::cout << "  Concurrent Reader Threads: " << num_read_threads << std::endl;
   std::cout << "  Map Size (MB): " << add_comma(map_size_mb) << std::endl;
   std::cout << "  Sync Mode: " << sync_mode_str << " (Flags: " << mdbx_sync_flags << ")"
             << std::endl;
   std::cout << "  Env Flags: " << mdbx_env_flags << std::endl;
   std::cout << "  Tests Enabled:" << std::endl;
   if (run_dense_rand)
      std::cout << "    - Dense Random Inserts\n";
   if (run_little_endian_seq)
      std::cout << "    - Little Endian Sequential Inserts\n";
   if (run_big_endian_seq)
      std::cout << "    - Big Endian Sequential Inserts\n";
   if (run_big_endian_rev)
      std::cout << "    - Big Endian Reverse Sequential Inserts\n";
   if (run_rand_string)
      std::cout << "    - Random String Inserts\n";
   if (run_get_known_le_seq)
      std::cout << "    - Get Known LE Sequential\n";
   if (run_get_known_le_rand)
      std::cout << "    - Get Known LE Random\n";
   if (run_get_known_be_seq)
      std::cout << "    - Get Known BE Sequential\n";
   if (run_lower_bound_rand)
      std::cout << "    - Lower Bound Random\n";
   if (run_concurrent_rw)
      std::cout << "    - Concurrent Read/Write\n";
   std::cout << "-----\n";

   // --- Database Setup ---
   std::cout << "Resetting database directory: " << db_path << std::endl;
   try
   {
      // Create the directory if it doesn't exist
      if (!std::filesystem::exists(db_path))
      {
         std::filesystem::create_directories(db_path);
      }
      else
      {
         // Remove contents if it exists
         for (const auto& entry : std::filesystem::directory_iterator(db_path))
         {
            std::filesystem::remove_all(entry.path());
         }
      }
   }
   catch (const std::filesystem::filesystem_error& e)
   {
      std::cerr << "Filesystem error: " << e.what() << std::endl;
      return 1;
   }

   MDBX_env* env = nullptr;
   MDBX_dbi  dbi = 0;
   int       rc;

   rc = mdbx_env_create(&env);
   check_mdbx_rc(rc, "mdbx_env_create");

   // Set geometry before opening: lower=1MB, initial=1MB, upper=map_size_mb, growth_step=-1 (default), shrink_threshold=-1 (default)
   rc = mdbx_env_set_geometry(env, 1024 * 1024, 1024 * 1024, map_size_mb * 1024 * 1024, -1, -1, -1);
   check_mdbx_rc(rc, "mdbx_env_set_geometry");

   // Set max readers based on command line arg
   rc = mdbx_env_set_maxreaders(env, num_read_threads + 2);  // +1 for main thread writer, +1 extra
   check_mdbx_rc(rc, "mdbx_env_set_maxreaders");

   // MDBX uses 0 mode bits when creating dirs/files internally if needed, rely on umask
   // Provide the *directory* path here, MDBX will handle data.mdbx and lock.mdbx
   // MDBX_NOSUBDIR flag means db_path *is* the database file, not a directory
   rc = mdbx_env_open(env, db_path.c_str(), (MDBX_env_flags_t)mdbx_env_flags, 0664);
   check_mdbx_rc(rc, "mdbx_env_open");

   // --- Global State for Tests ---
   uint64_t total_items_inserted = 0;
   uint64_t seq_le               = 0;   // For little endian sequential inserts/gets
   uint64_t seq_be               = 0;   // For big endian sequential inserts/gets
   uint64_t seq_rev              = -1;  // For big endian reverse inserts
   uint64_t start_big_end        = 0;   // To remember start for BE get test

   // Lambda for iteration test
   auto iterate_all = [&](MDBX_env* current_env, MDBX_dbi current_dbi)
   {
      uint64_t     item_count = 0;
      MDBX_txn*    txn        = nullptr;
      MDBX_cursor* cursor     = nullptr;
      MDBX_val     key, data;

      auto start = std::chrono::steady_clock::now();
      try
      {
         rc = mdbx_txn_begin(current_env, nullptr, MDBX_TXN_RDONLY, &txn);
         check_mdbx_rc(rc, "iterate_all: mdbx_txn_begin");

         rc = mdbx_cursor_open(txn, current_dbi, &cursor);
         check_mdbx_rc(rc, "iterate_all: mdbx_cursor_open");

         rc = mdbx_cursor_get(cursor, &key, &data, MDBX_FIRST);
         while (rc == MDBX_SUCCESS)
         {
            item_count++;
            // Can optionally access key.iov_base, key.iov_len, data.iov_base, data.iov_len
            rc = mdbx_cursor_get(cursor, &key, &data, MDBX_NEXT);
         }

         if (rc != MDBX_NOTFOUND)
         {  // Should end with NOTFOUND
            check_mdbx_rc(rc, "iterate_all: mdbx_cursor_get (NEXT)");
         }

         mdbx_cursor_close(cursor);
         cursor = nullptr;
         // Get actual count from DB stat for comparison before aborting txn
         MDBX_stat db_stat;
         rc                  = mdbx_dbi_stat(txn, current_dbi, &db_stat, sizeof(db_stat));
         uint64_t db_entries = (rc == MDBX_SUCCESS) ? db_stat.ms_entries : 0;

         mdbx_txn_abort(txn);  // Use abort for read-only transaction
         txn = nullptr;

         auto end   = std::chrono::steady_clock::now();
         auto delta = std::chrono::duration<double, std::milli>(end - start).count();

         std::cout << "  iterated " << std::fixed << std::setprecision(0) << std::setw(12)
                   << add_comma(uint64_t(item_count / (delta / 1000.0)))
                   << " items/sec  total items iterated: " << add_comma(item_count)
                   << " (DB count: " << add_comma(db_entries) << ")"
                   << " (took " << std::fixed << std::setprecision(2) << delta << " ms)"
                   << std::endl;
      }
      catch (const std::exception& e)
      {
         std::cerr << "Exception during iteration: " << e.what() << std::endl;
         if (cursor)
            mdbx_cursor_close(cursor);
         if (txn)
            mdbx_txn_abort(txn);
      }
   };

   // --- Benchmarking Sections ---
   try
   {  // Wrap tests in try-catch for MDBX exceptions

      MDBX_txn* setup_txn = nullptr;
      rc                  = mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)0, &setup_txn);
      check_mdbx_rc(rc, "mdbx_txn_begin (for dbi_open)");
      rc = mdbx_dbi_open(setup_txn, nullptr, MDBX_CREATE, &dbi);  // Use default DB (dbi=0)
      check_mdbx_rc(rc, "mdbx_dbi_open (main)");
      rc = mdbx_txn_commit(setup_txn);
      check_mdbx_rc(rc, "mdbx_txn_commit (for dbi_open)");
      setup_txn = nullptr;

      // --- Dense Random Insert Loop ---
      if (run_dense_rand)
      {
         std::cout << "--- insert dense rand ---\n";
         MDBX_txn* txn = nullptr;
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            rc         = mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)0, &txn);
            check_mdbx_rc(rc, "mdbx_txn_begin round " + std::to_string(ro));

            for (int i = 0; i < count; ++i)
            {
               uint64_t val = rand64();
               MDBX_val key, data;
               key.iov_base = &val;
               key.iov_len  = sizeof(val);
               data         = key;
               rc           = mdbx_put(txn, dbi, &key, &data, (MDBX_put_flags_t)0);
               check_mdbx_rc(rc, "mdbx_put " + std::to_string(i));

               total_items_inserted++;  // Increment global counter

               if ((i + 1) % batch_size == 0)
               {
                  rc = mdbx_txn_commit(txn);
                  check_mdbx_rc(rc, "mdbx_txn_commit batch " + std::to_string(i / batch_size));
                  txn = nullptr;
                  rc  = mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)0, &txn);
                  check_mdbx_rc(rc, "mdbx_txn_begin next batch");
               }
            }
            if (txn)
            {
               rc = mdbx_txn_commit(txn);
               check_mdbx_rc(rc, "mdbx_txn_commit final");
               txn = nullptr;
            }

            auto end   = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration<double, std::milli>(end - start).count();

            std::cout << ro << "] " << std::fixed << std::setprecision(0) << std::setw(12)
                      << add_comma(uint64_t(count / (delta / 1000.0)))
                      << " dense rand insert/sec  total items: " << add_comma(total_items_inserted)
                      << " (took " << std::fixed << std::setprecision(2) << delta << " ms)"
                      << std::endl;
            iterate_all(env, dbi);  // Run iteration check
         }
      }

      // --- Little Endian Sequential Insert Loop ---
      if (run_little_endian_seq)
      {
         std::cout << "--- insert little endian seq ---\n";
         MDBX_txn* txn                     = nullptr;
         uint64_t  current_batch_start_seq = seq_le;
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            rc         = mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)0, &txn);
            check_mdbx_rc(rc, "mdbx_txn_begin round " + std::to_string(ro));
            current_batch_start_seq = seq_le;  // Track seq for this round

            for (int i = 0; i < count; ++i)
            {
               uint64_t val = ++seq_le;  // Use global LE counter
               MDBX_val key, data;
               key.iov_base = &val;
               key.iov_len  = sizeof(val);
               data         = key;
               rc           = mdbx_put(txn, dbi, &key, &data, (MDBX_put_flags_t)0);
               check_mdbx_rc(rc, "mdbx_put LE " + std::to_string(i));
               total_items_inserted++;

               if ((i + 1) % batch_size == 0)
               {
                  rc = mdbx_txn_commit(txn);
                  check_mdbx_rc(rc, "mdbx_txn_commit LE batch " + std::to_string(i / batch_size));
                  txn = nullptr;
                  rc  = mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)0, &txn);
                  check_mdbx_rc(rc, "mdbx_txn_begin LE next batch");
               }
            }
            if (txn)
            {
               rc = mdbx_txn_commit(txn);
               check_mdbx_rc(rc, "mdbx_txn_commit LE final");
               txn = nullptr;
            }

            auto end   = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration<double, std::milli>(end - start).count();

            std::cout << ro << "] " << std::fixed << std::setprecision(0) << std::setw(12)
                      << add_comma(uint64_t(count / (delta / 1000.0)))
                      << " LE seq insert/sec      total items: " << add_comma(total_items_inserted)
                      << " (took " << std::fixed << std::setprecision(2) << delta << " ms)"
                      << std::endl;
            iterate_all(env, dbi);
         }
      }

      // --- Big Endian Sequential Insert Loop ---
      if (run_big_endian_seq)
      {
         start_big_end = seq_be;  // Remember the starting point for the GET test later
         std::cout << "--- insert big endian seq starting with: " << start_big_end << " ---\n";
         MDBX_txn* txn = nullptr;
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            rc         = mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)0, &txn);
            check_mdbx_rc(rc, "mdbx_txn_begin BE round " + std::to_string(ro));

            for (int i = 0; i < count; ++i)
            {
               uint64_t val_orig = seq_be++;         // Use global BE counter
               uint64_t val_be   = bswap(val_orig);  // Swap bytes
               MDBX_val key, data;
               key.iov_base = &val_be;
               key.iov_len  = sizeof(val_be);
               // Store the original (LE) value for potential verification later?
               // For now, store BE key like arb.cpp
               data = key;
               rc   = mdbx_put(txn, dbi, &key, &data, (MDBX_put_flags_t)0);
               check_mdbx_rc(rc, "mdbx_put BE " + std::to_string(i));
               total_items_inserted++;

               if ((i + 1) % batch_size == 0)
               {
                  rc = mdbx_txn_commit(txn);
                  check_mdbx_rc(rc, "mdbx_txn_commit BE batch " + std::to_string(i / batch_size));
                  txn = nullptr;
                  rc  = mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)0, &txn);
                  check_mdbx_rc(rc, "mdbx_txn_begin BE next batch");
               }
            }
            if (txn)
            {
               rc = mdbx_txn_commit(txn);
               check_mdbx_rc(rc, "mdbx_txn_commit BE final");
               txn = nullptr;
            }

            auto end   = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration<double, std::milli>(end - start).count();

            std::cout << ro << "] " << std::fixed << std::setprecision(0) << std::setw(12)
                      << add_comma(uint64_t(count / (delta / 1000.0)))
                      << " BE seq insert/sec      total items: " << add_comma(total_items_inserted)
                      << " (took " << std::fixed << std::setprecision(2) << delta << " ms)"
                      << std::endl;
            iterate_all(env, dbi);
         }
      }

      // --- Big Endian Reverse Sequential Insert Loop ---
      if (run_big_endian_rev)
      {
         std::cout << "--- insert big endian rev seq ---\n";
         MDBX_txn* txn = nullptr;
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            rc         = mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)0, &txn);
            check_mdbx_rc(rc, "mdbx_txn_begin BE Rev round " + std::to_string(ro));

            for (int i = 0; i < count; ++i)
            {
               uint64_t val_orig = seq_rev--;        // Use global reverse counter
               uint64_t val_be   = bswap(val_orig);  // Swap bytes
               MDBX_val key, data;
               key.iov_base = &val_be;
               key.iov_len  = sizeof(val_be);
               data         = key;
               rc           = mdbx_put(txn, dbi, &key, &data, (MDBX_put_flags_t)0);
               check_mdbx_rc(rc, "mdbx_put BE Rev " + std::to_string(i));
               total_items_inserted++;

               if ((i + 1) % batch_size == 0)
               {
                  rc = mdbx_txn_commit(txn);
                  check_mdbx_rc(rc,
                                "mdbx_txn_commit BE Rev batch " + std::to_string(i / batch_size));
                  txn = nullptr;
                  rc  = mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)0, &txn);
                  check_mdbx_rc(rc, "mdbx_txn_begin BE Rev next batch");
               }
            }
            if (txn)
            {
               rc = mdbx_txn_commit(txn);
               check_mdbx_rc(rc, "mdbx_txn_commit BE Rev final");
               txn = nullptr;
            }

            auto end   = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration<double, std::milli>(end - start).count();

            std::cout << ro << "] " << std::fixed << std::setprecision(0) << std::setw(12)
                      << add_comma(uint64_t(count / (delta / 1000.0)))
                      << " BE rev insert/sec      total items: " << add_comma(total_items_inserted)
                      << " (took " << std::fixed << std::setprecision(2) << delta << " ms)"
                      << std::endl;
            // iterate_all(env, dbi); // Iteration can be slow with many items
         }
      }

      // --- Random String Insert Loop ---
      if (run_rand_string)
      {
         std::cout << "--- insert random string ---\n";
         MDBX_txn* txn = nullptr;
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            rc         = mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)0, &txn);
            check_mdbx_rc(rc, "mdbx_txn_begin RandStr round " + std::to_string(ro));

            for (int i = 0; i < count; ++i)
            {
               std::string kstr = std::to_string(rand64());  // Convert rand64 to string
               MDBX_val    key, data;
               key.iov_base = (void*)kstr.data();  // Be careful with lifetime if string is modified
               key.iov_len  = kstr.size();
               data         = key;  // Store string as value too
               rc           = mdbx_put(txn, dbi, &key, &data, (MDBX_put_flags_t)0);
               check_mdbx_rc(rc, "mdbx_put RandStr " + std::to_string(i));
               total_items_inserted++;

               if ((i + 1) % batch_size == 0)
               {
                  rc = mdbx_txn_commit(txn);
                  check_mdbx_rc(rc,
                                "mdbx_txn_commit RandStr batch " + std::to_string(i / batch_size));
                  txn = nullptr;
                  rc  = mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)0, &txn);
                  check_mdbx_rc(rc, "mdbx_txn_begin RandStr next batch");
               }
            }
            if (txn)
            {
               rc = mdbx_txn_commit(txn);
               check_mdbx_rc(rc, "mdbx_txn_commit RandStr final");
               txn = nullptr;
            }

            auto end   = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration<double, std::milli>(end - start).count();

            std::cout << ro << "] " << std::fixed << std::setprecision(0) << std::setw(12)
                      << add_comma(uint64_t(count / (delta / 1000.0)))
                      << " rand str insert/sec    total items: " << add_comma(total_items_inserted)
                      << " (took " << std::fixed << std::setprecision(2) << delta << " ms)"
                      << std::endl;
            // iterate_all(env, dbi); // Iteration can be slow
         }
      }

      // --- Get Known Key - Little Endian Sequential ---
      if (run_get_known_le_seq && seq_le > 0)  // Only run if LE inserts happened
      {
         std::cout << "--- get known key little endian seq ---\n";
         MDBX_txn* txn             = nullptr;
         uint64_t  seq_get_counter = 0;  // Use a separate counter for getting
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            rc         = mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn);  // Use Read-Only txn
            check_mdbx_rc(rc, "mdbx_txn_begin GetLESeq round " + std::to_string(ro));
            seq_get_counter = 0;  // Reset get counter each round

            // Assume 'count' items were inserted in the corresponding insert round
            int items_to_get = count;
            if (items_to_get > seq_le)
               items_to_get = seq_le;  // Don't try to get more than inserted

            for (int i = 0; i < items_to_get; ++i)
            {
               uint64_t val = ++seq_get_counter;
               MDBX_val key, data;
               key.iov_base = &val;
               key.iov_len  = sizeof(val);

               rc = mdbx_get(txn, dbi, &key, &data);
               if (rc != MDBX_SUCCESS)
               {  // Should always find the key
                  std::cerr << "ERROR: Failed to get LE Seq key " << val << std::endl;
                  check_mdbx_rc(rc, "mdbx_get LE Seq " + std::to_string(val));
               }
               // Optional: verify data content if needed
            }

            mdbx_txn_abort(txn);  // Abort read-only transaction
            txn = nullptr;

            auto end   = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration<double, std::milli>(end - start).count();

            std::cout << ro << "] " << std::fixed << std::setprecision(0) << std::setw(12)
                      << add_comma(uint64_t(items_to_get / (delta / 1000.0)))
                      << " LE seq get/sec         total items: " << add_comma(total_items_inserted)
                      << " (took " << std::fixed << std::setprecision(2) << delta << " ms)"
                      << std::endl;
         }
      }

      // --- Get Known Key - Little Endian Random ---
      if (run_get_known_le_rand && seq_le > 0)  // Only run if LE inserts happened
      {
         std::cout << "--- get known key little endian rand ---\n";
         MDBX_txn* txn = nullptr;
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            rc         = mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn);
            check_mdbx_rc(rc, "mdbx_txn_begin GetLERand round " + std::to_string(ro));

            int items_to_get = count;  // Try to get 'count' random items

            for (int i = 0; i < items_to_get; ++i)
            {
               uint64_t rnd = rand64();
               // Get a random key known to be inserted (1 to seq_le)
               uint64_t val = (seq_le > 0) ? (rnd % seq_le) + 1 : 0;
               if (val == 0)
                  continue;  // Skip if no items inserted

               MDBX_val key, data;
               key.iov_base = &val;
               key.iov_len  = sizeof(val);

               rc = mdbx_get(txn, dbi, &key, &data);
               if (rc != MDBX_SUCCESS)
               {  // Should always find the key
                  std::cerr << "ERROR: Failed to get LE Rand key " << val << std::endl;
                  check_mdbx_rc(rc, "mdbx_get LE Rand " + std::to_string(val));
               }
            }

            mdbx_txn_abort(txn);
            txn = nullptr;

            auto end   = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration<double, std::milli>(end - start).count();

            std::cout << ro << "] " << std::fixed << std::setprecision(0) << std::setw(12)
                      << add_comma(uint64_t(items_to_get / (delta / 1000.0)))
                      << " LE rand get/sec        total items: " << add_comma(total_items_inserted)
                      << " (took " << std::fixed << std::setprecision(2) << delta << " ms)"
                      << std::endl;
         }
      }

      // --- Get Known Key - Big Endian Sequential ---
      if (run_get_known_be_seq && seq_be > start_big_end)  // Only run if BE inserts happened
      {
         std::cout << "--- get known key big endian seq ---\n";
         MDBX_txn* txn             = nullptr;
         uint64_t  seq_get_counter = start_big_end;  // Start from where BE insert started
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            rc         = mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn);
            check_mdbx_rc(rc, "mdbx_txn_begin GetBESeq round " + std::to_string(ro));
            seq_get_counter = start_big_end;  // Reset get counter each round

            // Assume 'count' items were inserted per round * rounds
            uint64_t items_inserted_be = seq_be - start_big_end;
            int      items_to_get      = count;  // Get 'count' items
            if (items_to_get > items_inserted_be)
               items_to_get = items_inserted_be;

            for (int i = 0; i < items_to_get; ++i)
            {
               uint64_t val_orig = seq_get_counter++;
               uint64_t val_be   = bswap(val_orig);
               MDBX_val key, data;
               key.iov_base = &val_be;
               key.iov_len  = sizeof(val_be);

               rc = mdbx_get(txn, dbi, &key, &data);
               if (rc != MDBX_SUCCESS)
               {  // Should always find the key
                  std::cerr << "ERROR: Failed to get BE Seq key " << val_orig << " (BE: ";
                  print_hex(&key);
                  std::cerr << ")" << std::endl;
                  check_mdbx_rc(rc, "mdbx_get BE Seq " + std::to_string(val_orig));
               }
            }

            mdbx_txn_abort(txn);
            txn = nullptr;

            auto end   = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration<double, std::milli>(end - start).count();

            std::cout << ro << "] " << std::fixed << std::setprecision(0) << std::setw(12)
                      << add_comma(uint64_t(items_to_get / (delta / 1000.0)))
                      << " BE seq get/sec         total items: " << add_comma(total_items_inserted)
                      << " (took " << std::fixed << std::setprecision(2) << delta << " ms)"
                      << std::endl;
         }
      }

      // --- Lower Bound Random Keys ---
      if (run_lower_bound_rand)
      {
         std::cout << "--- lower bound random i64 ---\n";
         MDBX_txn*    txn    = nullptr;
         MDBX_cursor* cursor = nullptr;

         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            rc         = mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn);
            check_mdbx_rc(rc, "mdbx_txn_begin LBRand round " + std::to_string(ro));
            rc = mdbx_cursor_open(txn, dbi, &cursor);
            check_mdbx_rc(rc, "mdbx_cursor_open LBRand round " + std::to_string(ro));

            for (int i = 0; i < count; ++i)
            {
               uint64_t val = rand64();
               MDBX_val key, data;
               key.iov_base = &val;
               key.iov_len  = sizeof(val);

               // MDBX_SET_RANGE finds key >= requested key
               rc = mdbx_cursor_get(cursor, &key, &data, MDBX_SET_RANGE);

               if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND)
               {
                  // It's okay if NOTFOUND (key > max key)
                  check_mdbx_rc(rc, "mdbx_cursor_get LBRand " + std::to_string(i));
               }
               // Key/data will contain the found key/value if rc == MDBX_SUCCESS
            }

            mdbx_cursor_close(cursor);
            cursor = nullptr;
            mdbx_txn_abort(txn);
            txn = nullptr;

            auto end   = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration<double, std::milli>(end - start).count();

            std::cout << ro << "] " << std::fixed << std::setprecision(0) << std::setw(12)
                      << add_comma(uint64_t(count / (delta / 1000.0)))
                      << " rand lowerbound/sec    total items: " << add_comma(total_items_inserted)
                      << " (took " << std::fixed << std::setprecision(2) << delta << " ms)"
                      << std::endl;
         }
      }

      // --- Concurrent Read/Write Test ---
      if (run_concurrent_rw && num_read_threads > 0)
      {
         std::vector<std::thread> rthreads;
         rthreads.reserve(num_read_threads);
         std::atomic<bool>    done = false;
         std::atomic<int64_t> read_ops_count(0);  // Use a more descriptive name

         std::cout << "--- insert dense rand while reading " << num_read_threads
                   << " threads  batch size: " << batch_size << " for " << multithread_rounds
                   << " rounds ---\n";

         // Reader thread function
         auto read_loop = [&]()
         {
            std::string tname = "read_" + std::to_string(rand64());
            thread_name(tname);  // Name the thread

            MDBX_txn*    rtxn            = nullptr;
            MDBX_cursor* rcursor         = nullptr;
            int          rc_read         = MDBX_SUCCESS;
            int          ops_since_renew = 0;
            const int    renew_interval  = 1000;  // Renew txn/cursor periodically

            try
            {
               rc_read = mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &rtxn);
               check_mdbx_rc(rc_read, tname + ": mdbx_txn_begin");
               rc_read = mdbx_cursor_open(rtxn, dbi, &rcursor);
               check_mdbx_rc(rc_read, tname + ": mdbx_cursor_open");

               while (!done.load(std::memory_order_relaxed))
               {
                  // Periodically renew transaction and cursor to avoid holding old snapshot
                  if (++ops_since_renew >= renew_interval)
                  {
                     // DONT close the cursor here
                     rc_read = mdbx_txn_renew(rtxn);
                     if (rc_read == MDBX_SUCCESS)
                     {
                        // Try to renew the existing cursor
                        rc_read = mdbx_cursor_renew(rtxn, rcursor);
                        if (rc_read != MDBX_SUCCESS)
                        {
                           // Cursor renewal failed - maybe structure changed too much?
                           // Close the old cursor and open a new one in the renewed txn
                           std::cerr << "Warning (" << tname << "): mdbx_cursor_renew failed ("
                                     << rc_read << "), reopening cursor." << std::endl;
                           mdbx_cursor_close(rcursor);  // Close the invalid cursor handle
                           rc_read = mdbx_cursor_open(rtxn, dbi,
                                                      &rcursor);  // Open a new cursor
                           check_mdbx_rc(rc_read,
                                         tname + ": mdbx_cursor_open (after failed renew)");
                        }
                        // else: Cursor renewal succeeded
                     }
                     else
                     {
                        // Transaction renewal failed, maybe the env/dbi was closed?
                        // Abort the old transaction and try starting a fresh one.
                        std::cerr << "Warning (" << tname << "): mdbx_txn_renew failed (" << rc_read
                                  << "), reopening txn and cursor." << std::endl;
                        mdbx_cursor_close(rcursor);  // Close the cursor associated with the old txn
                        mdbx_txn_abort(rtxn);        // Abort the failed txn
                        rtxn = nullptr;              // Mark as inactive

                        // Re-create txn
                        rc_read = mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &rtxn);
                        if (rc_read != MDBX_SUCCESS)
                        {
                           std::cerr << "FATAL (" << tname
                                     << "): Failed to re-begin transaction after renew failure. "
                                        "Exiting thread."
                                     << std::endl;
                           // Can't recover, break the loop or throw
                           break;  // Exit the while loop
                        }

                        // Re-create cursor
                        rc_read = mdbx_cursor_open(rtxn, dbi, &rcursor);
                        if (rc_read != MDBX_SUCCESS)
                        {
                           std::cerr << "FATAL (" << tname
                                     << "): Failed to re-open cursor after renew failure. Exiting "
                                        "thread."
                                     << std::endl;
                           // Abort the new txn and exit
                           mdbx_txn_abort(rtxn);
                           break;  // Exit the while loop
                        }
                     }
                     ops_since_renew = 0;
                  }

                  uint64_t val = rand64();
                  MDBX_val key, data;
                  key.iov_base = &val;
                  key.iov_len  = sizeof(val);

                  // MDBX_SET_RANGE finds key >= requested key
                  rc_read = mdbx_cursor_get(rcursor, &key, &data, MDBX_SET_RANGE);

                  if (rc_read != MDBX_SUCCESS && rc_read != MDBX_NOTFOUND)
                  {
                     check_mdbx_rc(rc_read, tname + ": mdbx_cursor_get LB");
                  }
                  read_ops_count.fetch_add(1, std::memory_order_relaxed);
               }

               if (rcursor)
                  mdbx_cursor_close(rcursor);
               if (rtxn)
                  mdbx_txn_abort(rtxn);
            }
            catch (const std::exception& e)
            {
               std::cerr << "Exception in reader thread " << tname << ": " << e.what() << std::endl;
               if (rcursor)
                  mdbx_cursor_close(rcursor);
               if (rtxn)
                  mdbx_txn_abort(rtxn);
            }
         };

         // Launch reader threads
         for (int i = 0; i < num_read_threads; ++i)
         {
            rthreads.emplace_back(read_loop);
         }

         // Main thread acts as writer
         MDBX_txn* txn = nullptr;
         for (int ro = 0; ro < multithread_rounds; ++ro)
         {
            auto    start            = std::chrono::steady_clock::now();
            int64_t start_read_count = read_ops_count.load(std::memory_order_relaxed);

            rc = mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)0, &txn);
            check_mdbx_rc(rc, "mdbx_txn_begin ConcurrentWrite round " + std::to_string(ro));

            for (int i = 0; i < count; ++i)
            {
               uint64_t val = rand64();  // arb.cpp uses rand64 here too
               MDBX_val key, data;
               key.iov_base = &val;
               key.iov_len  = sizeof(val);
               data         = key;
               rc           = mdbx_put(txn, dbi, &key, &data, (MDBX_put_flags_t)0);
               check_mdbx_rc(rc, "mdbx_put ConcurrentWrite " + std::to_string(i));
               total_items_inserted++;

               if ((i + 1) % batch_size == 0)
               {
                  rc = mdbx_txn_commit(txn);
                  check_mdbx_rc(rc, "mdbx_txn_commit ConcurrentWrite batch " +
                                        std::to_string(i / batch_size));
                  txn = nullptr;
                  rc  = mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)0, &txn);
                  check_mdbx_rc(rc, "mdbx_txn_begin ConcurrentWrite next batch");
               }
            }
            if (txn)
            {
               rc = mdbx_txn_commit(txn);
               check_mdbx_rc(rc, "mdbx_txn_commit ConcurrentWrite final");
               txn = nullptr;
            }

            auto    end            = std::chrono::steady_clock::now();
            auto    delta_ms       = std::chrono::duration<double, std::milli>(end - start).count();
            int64_t end_read_count = read_ops_count.load(std::memory_order_relaxed);
            int64_t reads_this_round = end_read_count - start_read_count;

            std::cout << ro << "] Write: " << std::fixed << std::setprecision(0) << std::setw(12)
                      << add_comma(uint64_t(count / (delta_ms / 1000.0)))
                      << " insert/sec. Read: " << std::fixed << std::setprecision(0)
                      << std::setw(12)
                      << add_comma(uint64_t(reads_this_round / (delta_ms / 1000.0)))
                      << " lowerbound/sec. Total Items: " << add_comma(total_items_inserted)
                      << " (took " << std::fixed << std::setprecision(2) << delta_ms << " ms)"
                      << std::endl;
         }

         // Signal threads to stop and wait for them
         done.store(true, std::memory_order_relaxed);
         std::cout << "Waiting for reader threads to finish..." << std::endl;
         for (auto& t : rthreads)
         {
            if (t.joinable())
            {
               t.join();
            }
         }
         std::cout << "Reader threads finished." << std::endl;
      }
   }
   catch (const std::exception& e)
   {
      std::cerr << "Caught Exception during benchmark: " << e.what() << std::endl;
      // Clean up MDBX resources if possible
      if (dbi)
         mdbx_dbi_close(env, dbi);
      if (env)
         mdbx_env_close(env);  // Use single-argument close
      return 1;
   }

   // --- Cleanup ---
   std::cout << "-----\nBenchmark finished." << std::endl;
   // MDBX_dbi handles are usually closed when the env is closed,
   // but explicit closing is good practice if you opened many.
   if (dbi)
   {  // Check if dbi was ever opened successfully
      // No need to check RC here, as env_close handles it
      mdbx_dbi_close(env, dbi);
      dbi = 0;  // Mark as closed
   }

   if (env)
   {
      // Optionally print MDBX stats and info before closing
      MDBX_envinfo env_info;
      rc = mdbx_env_info_ex(env, nullptr, &env_info, sizeof(env_info));
      if (rc == MDBX_SUCCESS)
      {
         std::cout << "MDBX Env Info:\n";
         std::cout << "  Map Size: " << add_comma(env_info.mi_mapsize) << " bytes\n";
         std::cout << "  Last Used Page No: " << add_comma(env_info.mi_last_pgno) << "\n";
         std::cout << "  Last Txn ID: " << add_comma(env_info.mi_recent_txnid) << "\n";
         std::cout << "  DB Page Size (from info): " << env_info.mi_dxb_pagesize << "\n";
      }
      else
      {
         std::cerr << "Warning: Failed to get env info (" << rc << ": " << mdbx_strerror(rc) << ")"
                   << std::endl;
      }

      MDBX_stat env_stat;
      rc = mdbx_env_stat_ex(env, nullptr, &env_stat, sizeof(env_stat));
      if (rc == MDBX_SUCCESS)
      {
         std::cout << "MDBX Env Stat:\n";
         std::cout << "  DB Page Size (from stat): " << env_stat.ms_psize << "\n";
         std::cout << "  Tree Depth: " << env_stat.ms_depth << "\n";
         std::cout << "  Branch Pages: " << add_comma(env_stat.ms_branch_pages) << "\n";
         std::cout << "  Leaf Pages: " << add_comma(env_stat.ms_leaf_pages) << "\n";
         std::cout << "  Overflow Pages: " << add_comma(env_stat.ms_overflow_pages) << "\n";
         std::cout << "  Entries: " << add_comma(env_stat.ms_entries) << "\n";
      }
      else
      {
         std::cerr << "Warning: Failed to get env stat (" << rc << ": " << mdbx_strerror(rc) << ")"
                   << std::endl;
      }

      // Close the environment
      // Set abort=true if an error occurred earlier to avoid flushing potentially corrupt data - mdbx_env_close_ex handles this
      mdbx_env_close(env);  // Use single-argument close
      env = nullptr;        // Mark as closed
   }

   std::cout << "Database closed." << std::endl;

   return 0;
}