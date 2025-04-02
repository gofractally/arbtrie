#include <atomic>
#include <boost/program_options.hpp>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>  // For memcpy
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>  // Required by boost/program_options indirectly?
#include <mutex>   // For concurrent test
#include <random>
#include <string>
#include <thread>
#include <vector>

// Include SQLite3 header
#include <sqlite3.h>

// --- Utility Functions (Copied/Adapted) ---

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
   thread_local static std::mt19937 gen(std::random_device{}());
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

void print_hex(const void* data, size_t len)
{
   if (!data || len == 0)
   {
      std::cout << "<empty>";
      return;
   }
   const unsigned char* bytes = static_cast<const unsigned char*>(data);
   std::cout << std::hex << std::setfill('0');
   for (size_t i = 0; i < len; ++i)
   {
      std::cout << std::setw(2) << static_cast<unsigned int>(bytes[i]);
   }
   std::cout << std::dec << std::setfill(' ');  // Reset to decimal
}

// --- SQLite Helper ---
// Function to check SQLite return code and print error
inline void check_sqlite_rc(int rc, sqlite3* db, const std::string& msg)
{
   if (rc != SQLITE_OK && rc != SQLITE_ROW && rc != SQLITE_DONE)
   {
      std::cerr << "SQLite Error: " << msg << " (" << rc << ": " << sqlite3_errmsg(db) << ")"
                << std::endl;
      sqlite3_close(db);  // Attempt to close DB before exiting
      exit(EXIT_FAILURE);
   }
}
inline void check_sqlite_rc(int rc, const std::string& msg)
{
   if (rc != SQLITE_OK && rc != SQLITE_ROW && rc != SQLITE_DONE)
   {
      std::cerr << "SQLite Error: " << msg << " (" << rc << ")" << std::endl;
      exit(EXIT_FAILURE);
   }
}

// Helper to execute simple SQL statements
void exec_sql(sqlite3* db, const std::string& sql)
{
   char* err_msg = nullptr;
   int   rc      = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err_msg);
   if (rc != SQLITE_OK)
   {
      std::cerr << "Failed to execute SQL: " << sql << " (" << rc << ": " << err_msg << ")"
                << std::endl;
      sqlite3_free(err_msg);
      sqlite3_close(db);
      exit(EXIT_FAILURE);
   }
}

// --- Main Test Function ---

int main(int argc, char** argv)
{
   thread_name("main");  // Name the main thread

   namespace po        = boost::program_options;
   std::string db_path = "sqlite-test.db";  // Database file path

   // --- Command Line Options ---
   int  count                 = 1000000;
   int  batch_size            = 100;
   int  rounds                = 3;
   int  multithread_rounds    = 20;
   int  num_read_threads      = 15;
   int  cache_size_mb         = 2048;  // Default SQLite cache size in MB
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

   std::string sync_mode_str    = "normal";  // Default SQLite sync mode
   std::string journal_mode_str = "wal";     // Default SQLite journal mode

   // clang-format off
    po::options_description desc("SQLite Benchmark Options");
    desc.add_options()
        ("help,h", "Print help message")
        ("db-path", po::value<std::string>(&db_path)->default_value("sqlite-test.db"), "Database file path")
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
        ("sync-mode", po::value<std::string>(&sync_mode_str)->default_value("normal"), "Sync mode: full, normal, off")
        ("journal-mode", po::value<std::string>(&journal_mode_str)->default_value("wal"), "Journal mode: wal, delete, memory")
        ("cache-size-mb", po::value<int>(&cache_size_mb)->default_value(2048), "SQLite page cache size in MB")
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

   // Validate sync mode
   std::string sync_pragma;
   if (sync_mode_str == "full")
      sync_pragma = "PRAGMA synchronous = FULL;";
   else if (sync_mode_str == "normal")
      sync_pragma = "PRAGMA synchronous = NORMAL;";
   else if (sync_mode_str == "off")
      sync_pragma = "PRAGMA synchronous = OFF;";
   else
   {
      std::cerr << "Invalid sync-mode: " << sync_mode_str << ". Use 'full', 'normal', or 'off'."
                << std::endl;
      return 1;
   }

   // Validate journal mode
   std::string journal_pragma;
   if (journal_mode_str == "wal")
      journal_pragma = "PRAGMA journal_mode = WAL;";
   else if (journal_mode_str == "delete")
      journal_pragma = "PRAGMA journal_mode = DELETE;";
   else if (journal_mode_str == "memory")
      journal_pragma = "PRAGMA journal_mode = MEMORY;";
   else
   {
      std::cerr << "Invalid journal-mode: " << journal_mode_str
                << ". Use 'wal', 'delete', or 'memory'." << std::endl;
      return 1;
   }

   std::string cache_pragma = "PRAGMA cache_size = -" + std::to_string(cache_size_mb * 1024) +
                              ";";  // Negative value for KiB
   std::string mmap_pragma =
       "PRAGMA mmap_size = " + std::to_string(static_cast<long long>(cache_size_mb) * 1024 * 1024) +
       ";";  // Set mmap size generously

   // --- Print Configuration ---
   std::cout << "SQLite Benchmark Configuration:" << std::endl;
   std::cout << "  Database Path: " << db_path << std::endl;
   std::cout << "  Items per round: " << add_comma(count) << std::endl;
   std::cout << "  Batch Size: " << add_comma(batch_size) << std::endl;
   std::cout << "  Single-thread Rounds: " << rounds << std::endl;
   std::cout << "  Multi-thread Rounds: " << multithread_rounds << std::endl;
   std::cout << "  Concurrent Reader Threads: " << num_read_threads << std::endl;
   std::cout << "  Cache Size (MB): " << add_comma(cache_size_mb) << std::endl;
   std::cout << "  Journal Mode: " << journal_mode_str << std::endl;
   std::cout << "  Sync Mode: " << sync_mode_str << std::endl;
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
   std::cout << "Resetting database file: " << db_path << std::endl;
   std::filesystem::remove(db_path);           // Delete existing DB file if it exists
   std::filesystem::remove(db_path + "-shm");  // Delete WAL shared-memory file
   std::filesystem::remove(db_path + "-wal");  // Delete WAL file

   sqlite3* db = nullptr;
   int      rc =
       sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
   check_sqlite_rc(rc, db, "sqlite3_open_v2");

   // Apply PRAGMAs
   exec_sql(db, journal_pragma);
   exec_sql(db, sync_pragma);
   exec_sql(db, cache_pragma);
   exec_sql(db, mmap_pragma);  // Attempt to enable memory mapping
   exec_sql(db, "PRAGMA temp_store = MEMORY;");
   exec_sql(db, "PRAGMA secure_delete = OFF;");  // Minor optimization

   // Create Key-Value Table
   exec_sql(db, "CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB) WITHOUT ROWID;");
   // WITHOUT ROWID can be slightly faster if we don't need SQLite's implicit rowid

   // --- Prepare Statements (Optimization) ---
   sqlite3_stmt* insert_stmt      = nullptr;
   sqlite3_stmt* get_stmt         = nullptr;
   sqlite3_stmt* lower_bound_stmt = nullptr;

   rc = sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?);", -1,
                           &insert_stmt, nullptr);
   check_sqlite_rc(rc, db, "prepare insert");
   rc = sqlite3_prepare_v2(db, "SELECT value FROM kv WHERE key = ?;", -1, &get_stmt, nullptr);
   check_sqlite_rc(rc, db, "prepare get");
   rc = sqlite3_prepare_v2(db, "SELECT key, value FROM kv WHERE key >= ? ORDER BY key ASC LIMIT 1;",
                           -1, &lower_bound_stmt, nullptr);
   check_sqlite_rc(rc, db, "prepare lower_bound");

   // --- Global State for Tests ---
   uint64_t total_items_inserted = 0;
   uint64_t seq_le               = 0;   // For little endian sequential inserts/gets
   uint64_t seq_be               = 0;   // For big endian sequential inserts/gets
   uint64_t seq_rev              = -1;  // For big endian reverse inserts
   uint64_t start_big_end        = 0;   // To remember start for BE get test

   // Lambda for iteration test
   auto iterate_all = [&](sqlite3* current_db)
   {
      uint64_t      item_count = 0;
      sqlite3_stmt* stmt       = nullptr;
      const char*   tail       = nullptr;
      auto          start_iter = std::chrono::steady_clock::now();

      // Prepare statement for iteration
      rc = sqlite3_prepare_v2(current_db, "SELECT COUNT(*) FROM kv;", -1, &stmt, &tail);
      check_sqlite_rc(rc, current_db, "iterate_all: prepare count");
      rc = sqlite3_step(stmt);
      if (rc == SQLITE_ROW)
      {
         item_count = sqlite3_column_int64(stmt, 0);
      }
      else
      {
         check_sqlite_rc(rc, current_db, "iterate_all: step count");
      }
      sqlite3_finalize(stmt);
      stmt = nullptr;  // Important to reset

      auto end_iter   = std::chrono::steady_clock::now();
      auto delta_iter = std::chrono::duration<double, std::milli>(end_iter - start_iter).count();

      // Get table size on disk for info (Approximate)
      uint64_t db_size = 0;
      try
      {
         if (std::filesystem::exists(db_path))
         {
            db_size = std::filesystem::file_size(db_path);
         }
      }
      catch (...)
      { /* ignore */
      }

      // Calculate count items per sec
      double count_items_per_sec = (delta_iter > 0) ? (item_count / (delta_iter / 1000.0)) : 0.0;

      std::cout << "  DB count: " << add_comma(item_count) << " (" << std::fixed
                << std::setprecision(0) << add_comma(uint64_t(count_items_per_sec)) << " items/sec)"
                << " (count took " << std::fixed << std::setprecision(2) << delta_iter << " ms)"
                << " DB size: " << add_comma(db_size / (1024 * 1024)) << " MB" << std::endl;

      // Full iteration (optional, can be very slow)
      rc = sqlite3_prepare_v2(current_db, "SELECT key, value FROM kv ORDER BY key ASC;", -1, &stmt,
                              &tail);
      check_sqlite_rc(rc, current_db, "iterate_all: prepare select");

      auto     start      = std::chrono::steady_clock::now();
      uint64_t iter_count = 0;
      while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
      {
         iter_count++;
         // Optionally access data:
         // const void* key_data = sqlite3_column_blob(stmt, 0);
         // int key_len = sqlite3_column_bytes(stmt, 0);
         // const void* val_data = sqlite3_column_blob(stmt, 1);
         // int val_len = sqlite3_column_bytes(stmt, 1);
      }
      check_sqlite_rc(rc, current_db, "iterate_all: step select");  // Check for errors after loop
      sqlite3_finalize(stmt);

      auto end   = std::chrono::steady_clock::now();
      auto delta = std::chrono::duration<double, std::milli>(end - start).count();

      std::cout << "  iterated " << std::fixed << std::setprecision(0) << std::setw(12)
                << add_comma(uint64_t(iter_count / (delta / 1000.0)))
                << " items/sec  total items iterated: " << add_comma(iter_count)
                << " (DB count: " << add_comma(item_count) << ")"  // Added DB count for comparison
                << " (took " << std::fixed << std::setprecision(2) << delta << " ms)" << std::endl;
   };

   // --- Benchmarking Sections ---
   try
   {  // Wrap tests in try-catch for potential errors

      // --- Dense Random Insert Loop ---
      if (run_dense_rand)
      {
         std::cout << "--- insert dense rand ---\n";
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            exec_sql(db, "BEGIN IMMEDIATE TRANSACTION;");

            for (int i = 0; i < count; ++i)
            {
               uint64_t val = rand64();
               rc           = sqlite3_bind_blob(insert_stmt, 1, &val, sizeof(val), SQLITE_STATIC);
               check_sqlite_rc(rc, db, "bind key dense rand");
               rc = sqlite3_bind_blob(insert_stmt, 2, &val, sizeof(val), SQLITE_STATIC);
               check_sqlite_rc(rc, db, "bind value dense rand");

               rc = sqlite3_step(insert_stmt);
               check_sqlite_rc(rc, db, "step insert dense rand");
               sqlite3_reset(insert_stmt);  // Reset for next iteration

               total_items_inserted++;

               if ((i + 1) % batch_size == 0)
               {
                  exec_sql(db, "COMMIT;");
                  exec_sql(db, "BEGIN IMMEDIATE TRANSACTION;");
               }
            }
            exec_sql(db, "COMMIT;");  // Commit remaining items

            auto end   = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration<double, std::milli>(end - start).count();

            std::cout << ro << "] " << std::fixed << std::setprecision(0) << std::setw(12)
                      << add_comma(uint64_t(count / (delta / 1000.0)))
                      << " dense rand insert/sec  total items: " << add_comma(total_items_inserted)
                      << " (took " << std::fixed << std::setprecision(2) << delta << " ms)"
                      << std::endl;
            iterate_all(db);  // Run iteration check
         }
      }

      // --- Little Endian Sequential Insert Loop ---
      if (run_little_endian_seq)
      {
         std::cout << "--- insert little endian seq ---\n";
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            exec_sql(db, "BEGIN IMMEDIATE TRANSACTION;");

            for (int i = 0; i < count; ++i)
            {
               uint64_t val = ++seq_le;
               rc           = sqlite3_bind_blob(insert_stmt, 1, &val, sizeof(val), SQLITE_STATIC);
               check_sqlite_rc(rc, db, "bind key le seq");
               rc = sqlite3_bind_blob(insert_stmt, 2, &val, sizeof(val), SQLITE_STATIC);
               check_sqlite_rc(rc, db, "bind value le seq");

               rc = sqlite3_step(insert_stmt);
               check_sqlite_rc(rc, db, "step insert le seq");
               sqlite3_reset(insert_stmt);
               total_items_inserted++;

               if ((i + 1) % batch_size == 0)
               {
                  exec_sql(db, "COMMIT;");
                  exec_sql(db, "BEGIN IMMEDIATE TRANSACTION;");
               }
            }
            exec_sql(db, "COMMIT;");

            auto end   = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration<double, std::milli>(end - start).count();

            std::cout << ro << "] " << std::fixed << std::setprecision(0) << std::setw(12)
                      << add_comma(uint64_t(count / (delta / 1000.0)))
                      << " LE seq insert/sec      total items: " << add_comma(total_items_inserted)
                      << " (took " << std::fixed << std::setprecision(2) << delta << " ms)"
                      << std::endl;
            iterate_all(db);
         }
      }

      // --- Big Endian Sequential Insert Loop ---
      if (run_big_endian_seq)
      {
         start_big_end = seq_be;
         std::cout << "--- insert big endian seq starting with: " << start_big_end << " ---\n";
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            exec_sql(db, "BEGIN IMMEDIATE TRANSACTION;");

            for (int i = 0; i < count; ++i)
            {
               uint64_t val_orig = seq_be++;
               uint64_t val_be   = bswap(val_orig);
               rc = sqlite3_bind_blob(insert_stmt, 1, &val_be, sizeof(val_be), SQLITE_STATIC);
               check_sqlite_rc(rc, db, "bind key be seq");
               // Store BE key as value too for consistency with LMDBX test
               rc = sqlite3_bind_blob(insert_stmt, 2, &val_be, sizeof(val_be), SQLITE_STATIC);
               check_sqlite_rc(rc, db, "bind value be seq");

               rc = sqlite3_step(insert_stmt);
               check_sqlite_rc(rc, db, "step insert be seq");
               sqlite3_reset(insert_stmt);
               total_items_inserted++;

               if ((i + 1) % batch_size == 0)
               {
                  exec_sql(db, "COMMIT;");
                  exec_sql(db, "BEGIN IMMEDIATE TRANSACTION;");
               }
            }
            exec_sql(db, "COMMIT;");

            auto end   = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration<double, std::milli>(end - start).count();

            std::cout << ro << "] " << std::fixed << std::setprecision(0) << std::setw(12)
                      << add_comma(uint64_t(count / (delta / 1000.0)))
                      << " BE seq insert/sec      total items: " << add_comma(total_items_inserted)
                      << " (took " << std::fixed << std::setprecision(2) << delta << " ms)"
                      << std::endl;
            iterate_all(db);
         }
      }

      // --- Big Endian Reverse Sequential Insert Loop ---
      if (run_big_endian_rev)
      {
         std::cout << "--- insert big endian rev seq ---\n";
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            exec_sql(db, "BEGIN IMMEDIATE TRANSACTION;");

            for (int i = 0; i < count; ++i)
            {
               uint64_t val_orig = seq_rev--;
               uint64_t val_be   = bswap(val_orig);
               rc = sqlite3_bind_blob(insert_stmt, 1, &val_be, sizeof(val_be), SQLITE_STATIC);
               check_sqlite_rc(rc, db, "bind key be rev");
               rc = sqlite3_bind_blob(insert_stmt, 2, &val_be, sizeof(val_be), SQLITE_STATIC);
               check_sqlite_rc(rc, db, "bind value be rev");

               rc = sqlite3_step(insert_stmt);
               check_sqlite_rc(rc, db, "step insert be rev");
               sqlite3_reset(insert_stmt);
               total_items_inserted++;

               if ((i + 1) % batch_size == 0)
               {
                  exec_sql(db, "COMMIT;");
                  exec_sql(db, "BEGIN IMMEDIATE TRANSACTION;");
               }
            }
            exec_sql(db, "COMMIT;");

            auto end   = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration<double, std::milli>(end - start).count();

            std::cout << ro << "] " << std::fixed << std::setprecision(0) << std::setw(12)
                      << add_comma(uint64_t(count / (delta / 1000.0)))
                      << " BE rev insert/sec      total items: " << add_comma(total_items_inserted)
                      << " (took " << std::fixed << std::setprecision(2) << delta << " ms)"
                      << std::endl;
            // iterate_all(db); // Can be slow
         }
      }

      // --- Random String Insert Loop ---
      if (run_rand_string)
      {
         std::cout << "--- insert random string ---\n";
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            exec_sql(db, "BEGIN IMMEDIATE TRANSACTION;");

            for (int i = 0; i < count; ++i)
            {
               std::string kstr = std::to_string(rand64());
               // Use SQLITE_TRANSIENT because kstr goes out of scope
               rc = sqlite3_bind_blob(insert_stmt, 1, kstr.data(), kstr.size(), SQLITE_TRANSIENT);
               check_sqlite_rc(rc, db, "bind key rand str");
               rc = sqlite3_bind_blob(insert_stmt, 2, kstr.data(), kstr.size(), SQLITE_TRANSIENT);
               check_sqlite_rc(rc, db, "bind value rand str");

               rc = sqlite3_step(insert_stmt);
               check_sqlite_rc(rc, db, "step insert rand str");
               sqlite3_reset(insert_stmt);
               total_items_inserted++;

               if ((i + 1) % batch_size == 0)
               {
                  exec_sql(db, "COMMIT;");
                  exec_sql(db, "BEGIN IMMEDIATE TRANSACTION;");
               }
            }
            exec_sql(db, "COMMIT;");

            auto end   = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration<double, std::milli>(end - start).count();

            std::cout << ro << "] " << std::fixed << std::setprecision(0) << std::setw(12)
                      << add_comma(uint64_t(count / (delta / 1000.0)))
                      << " rand str insert/sec    total items: " << add_comma(total_items_inserted)
                      << " (took " << std::fixed << std::setprecision(2) << delta << " ms)"
                      << std::endl;
            // iterate_all(db); // Can be slow
         }
      }

      // --- Get Known Key - Little Endian Sequential ---
      if (run_get_known_le_seq && seq_le > 0)
      {
         std::cout << "--- get known key little endian seq ---\n";
         uint64_t seq_get_counter = 0;
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            exec_sql(db, "BEGIN TRANSACTION;");  // Read transaction sufficient
            seq_get_counter = 0;

            int items_to_get = count;
            if (items_to_get > seq_le)
               items_to_get = seq_le;

            for (int i = 0; i < items_to_get; ++i)
            {
               uint64_t val = ++seq_get_counter;
               rc           = sqlite3_bind_blob(get_stmt, 1, &val, sizeof(val), SQLITE_STATIC);
               check_sqlite_rc(rc, db, "bind get le seq");

               rc = sqlite3_step(get_stmt);
               if (rc != SQLITE_ROW)
               {  // Should always find the key
                  std::cerr << "ERROR: Failed to get LE Seq key " << val << " (rc: " << rc << ")"
                            << std::endl;
                  check_sqlite_rc(rc, db, "step get le seq");
               }
               // Optional: verify data content if needed
               // const void* data = sqlite3_column_blob(get_stmt, 0);
               // int len = sqlite3_column_bytes(get_stmt, 0);

               sqlite3_reset(get_stmt);
            }
            exec_sql(db, "COMMIT;");  // End read transaction

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
      if (run_get_known_le_rand && seq_le > 0)
      {
         std::cout << "--- get known key little endian rand ---\n";
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            exec_sql(db, "BEGIN TRANSACTION;");
            int items_to_get = count;

            for (int i = 0; i < items_to_get; ++i)
            {
               uint64_t rnd = rand64();
               uint64_t val = (seq_le > 0) ? (rnd % seq_le) + 1 : 0;
               if (val == 0)
                  continue;

               rc = sqlite3_bind_blob(get_stmt, 1, &val, sizeof(val), SQLITE_STATIC);
               check_sqlite_rc(rc, db, "bind get le rand");

               rc = sqlite3_step(get_stmt);
               if (rc != SQLITE_ROW)
               {
                  std::cerr << "ERROR: Failed to get LE Rand key " << val << " (rc: " << rc << ")"
                            << std::endl;
                  check_sqlite_rc(rc, db, "step get le rand");
               }
               sqlite3_reset(get_stmt);
            }
            exec_sql(db, "COMMIT;");

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
      if (run_get_known_be_seq && seq_be > start_big_end)
      {
         std::cout << "--- get known key big endian seq ---\n";
         uint64_t seq_get_counter = start_big_end;
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            exec_sql(db, "BEGIN TRANSACTION;");
            seq_get_counter = start_big_end;

            uint64_t items_inserted_be = seq_be - start_big_end;
            int      items_to_get      = count;
            if (items_to_get > items_inserted_be)
               items_to_get = items_inserted_be;

            for (int i = 0; i < items_to_get; ++i)
            {
               uint64_t val_orig = seq_get_counter++;
               uint64_t val_be   = bswap(val_orig);
               rc = sqlite3_bind_blob(get_stmt, 1, &val_be, sizeof(val_be), SQLITE_STATIC);
               check_sqlite_rc(rc, db, "bind get be seq");

               rc = sqlite3_step(get_stmt);
               if (rc != SQLITE_ROW)
               {
                  std::cerr << "ERROR: Failed to get BE Seq key " << val_orig << " (BE: ";
                  print_hex(&val_be, sizeof(val_be));
                  std::cerr << ") (rc: " << rc << ")" << std::endl;
                  check_sqlite_rc(rc, db, "step get be seq");
               }
               sqlite3_reset(get_stmt);
            }
            exec_sql(db, "COMMIT;");

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
         for (int ro = 0; ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            exec_sql(db, "BEGIN TRANSACTION;");  // Read transaction

            for (int i = 0; i < count; ++i)
            {
               uint64_t val = rand64();
               rc = sqlite3_bind_blob(lower_bound_stmt, 1, &val, sizeof(val), SQLITE_STATIC);
               check_sqlite_rc(rc, db, "bind lower bound rand");

               rc = sqlite3_step(lower_bound_stmt);  // Expect SQLITE_ROW or SQLITE_DONE

               if (rc != SQLITE_ROW && rc != SQLITE_DONE)
               {  // Error occurred
                  check_sqlite_rc(rc, db, "step lower bound rand");
               }
               // If rc == SQLITE_ROW, data is available via sqlite3_column_*
               sqlite3_reset(lower_bound_stmt);
            }
            exec_sql(db, "COMMIT;");  // End read transaction

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
      if (run_concurrent_rw && num_read_threads > 0 && journal_mode_str == "wal")
      {
         std::vector<std::thread> rthreads;
         rthreads.reserve(num_read_threads);
         std::atomic<bool>    done = false;
         std::atomic<int64_t> read_ops_count(0);

         std::cout << "--- insert dense rand while reading " << num_read_threads
                   << " threads (WAL mode) batch size: " << batch_size << " for "
                   << multithread_rounds << " rounds ---\n";

         // Reader thread function
         auto read_loop = [&]()
         {
            std::string tname = "read_" + std::to_string(rand64());
            thread_name(tname);

            sqlite3*      reader_db      = nullptr;
            sqlite3_stmt* reader_lb_stmt = nullptr;
            int           rc_read        = SQLITE_OK;

            try
            {
               // Each reader needs its own connection in WAL mode
               rc_read = sqlite3_open_v2(db_path.c_str(), &reader_db,
                                         SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, nullptr);
               if (rc_read != SQLITE_OK)
               {
                  std::cerr << "FATAL (" << tname << "): Failed to open reader DB connection ("
                            << rc_read << ": " << sqlite3_errmsg(reader_db) << "). Exiting thread."
                            << std::endl;
                  if (reader_db)
                     sqlite3_close(reader_db);
                  return;  // Exit thread if DB can't be opened
               }

               // Prepare statement for this thread's connection
               rc_read = sqlite3_prepare_v2(
                   reader_db, "SELECT key, value FROM kv WHERE key >= ? ORDER BY key ASC LIMIT 1;",
                   -1, &reader_lb_stmt, nullptr);
               if (rc_read != SQLITE_OK)
               {
                  std::cerr << "FATAL (" << tname << "): Failed to prepare lower bound statement ("
                            << rc_read << ": " << sqlite3_errmsg(reader_db) << "). Exiting thread."
                            << std::endl;
                  sqlite3_close(reader_db);
                  return;
               }

               while (!done.load(std::memory_order_relaxed))
               {
                  uint64_t val = rand64();
                  rc_read = sqlite3_bind_blob(reader_lb_stmt, 1, &val, sizeof(val), SQLITE_STATIC);
                  if (rc_read != SQLITE_OK)
                  {
                     std::cerr << "Warning (" << tname << "): bind lower bound failed (" << rc_read
                               << ": " << sqlite3_errmsg(reader_db) << ")" << std::endl;
                     // Potential issue, maybe retry or log differently? For now, continue.
                     sqlite3_reset(reader_lb_stmt);  // Reset on error too
                     continue;
                  }

                  rc_read = sqlite3_step(reader_lb_stmt);
                  if (rc_read != SQLITE_ROW && rc_read != SQLITE_DONE)
                  {
                     // SQLITE_BUSY or SQLITE_LOCKED might happen occasionally in WAL, even for readers, though less common.
                     if (rc_read == SQLITE_BUSY || rc_read == SQLITE_LOCKED)
                     {
                        std::this_thread::sleep_for(
                            std::chrono::microseconds(100));  // Small sleep and retry
                        sqlite3_reset(reader_lb_stmt);
                        continue;
                     }
                     else
                     {
                        std::cerr << "Warning (" << tname << "): step lower bound failed ("
                                  << rc_read << ": " << sqlite3_errmsg(reader_db) << ")"
                                  << std::endl;
                        // Log or handle other errors if needed
                     }
                  }
                  sqlite3_reset(reader_lb_stmt);
                  read_ops_count.fetch_add(1, std::memory_order_relaxed);
               }

               if (reader_lb_stmt)
                  sqlite3_finalize(reader_lb_stmt);
               if (reader_db)
                  sqlite3_close(reader_db);
            }
            catch (const std::exception& e)
            {
               std::cerr << "Exception in reader thread " << tname << ": " << e.what() << std::endl;
               if (reader_lb_stmt)
                  sqlite3_finalize(reader_lb_stmt);
               if (reader_db)
                  sqlite3_close(reader_db);
            }
            catch (...)
            {
               std::cerr << "Unknown exception in reader thread " << tname << std::endl;
               if (reader_lb_stmt)
                  sqlite3_finalize(reader_lb_stmt);
               if (reader_db)
                  sqlite3_close(reader_db);
            }
         };

         // Launch reader threads
         for (int i = 0; i < num_read_threads; ++i)
         {
            rthreads.emplace_back(read_loop);
         }

         // Main thread acts as writer
         for (int ro = 0; ro < multithread_rounds; ++ro)
         {
            auto    start            = std::chrono::steady_clock::now();
            int64_t start_read_count = read_ops_count.load(std::memory_order_relaxed);

            exec_sql(db, "BEGIN IMMEDIATE TRANSACTION;");

            for (int i = 0; i < count; ++i)
            {
               uint64_t val = rand64();
               rc           = sqlite3_bind_blob(insert_stmt, 1, &val, sizeof(val), SQLITE_STATIC);
               check_sqlite_rc(rc, db, "bind key concurrent write");
               rc = sqlite3_bind_blob(insert_stmt, 2, &val, sizeof(val), SQLITE_STATIC);
               check_sqlite_rc(rc, db, "bind value concurrent write");

               rc = sqlite3_step(insert_stmt);
               if (rc != SQLITE_DONE)
               {
                  // Handle potential busy/locked errors if writer gets blocked
                  if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED)
                  {
                     exec_sql(db, "COMMIT;");  // Commit what we have
                     std::this_thread::sleep_for(
                         std::chrono::milliseconds(1));  // Wait and retry txn
                     exec_sql(db, "BEGIN IMMEDIATE TRANSACTION;");
                     // Re-bind and re-step (or adjust loop logic)
                     i--;  // Decrement to retry this item
                     continue;
                  }
                  else
                  {
                     check_sqlite_rc(rc, db, "step insert concurrent write");
                  }
               }
               sqlite3_reset(insert_stmt);
               total_items_inserted++;

               if ((i + 1) % batch_size == 0)
               {
                  exec_sql(db, "COMMIT;");
                  exec_sql(db, "BEGIN IMMEDIATE TRANSACTION;");
               }
            }
            exec_sql(db, "COMMIT;");

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
      else if (run_concurrent_rw && num_read_threads > 0 && journal_mode_str != "wal")
      {
         std::cout << "--- Concurrent Read/Write test requires WAL journal mode. Skipping. ---\n";
      }
   }
   catch (const std::exception& e)
   {
      std::cerr << "Caught Exception during benchmark: " << e.what() << std::endl;
      // Clean up SQLite resources if possible
      if (insert_stmt)
         sqlite3_finalize(insert_stmt);
      if (get_stmt)
         sqlite3_finalize(get_stmt);
      if (lower_bound_stmt)
         sqlite3_finalize(lower_bound_stmt);
      if (db)
         sqlite3_close(db);
      return 1;
   }

   // --- Cleanup ---
   std::cout << "-----\nBenchmark finished." << std::endl;
   if (insert_stmt)
      sqlite3_finalize(insert_stmt);
   if (get_stmt)
      sqlite3_finalize(get_stmt);
   if (lower_bound_stmt)
      sqlite3_finalize(lower_bound_stmt);

   if (db)
   {
      // Optionally print final stats before closing
      iterate_all(db);

      // Close the database connection
      rc = sqlite3_close(db);
      if (rc != SQLITE_OK)
      {
         // Handle close error (e.g., unfinalized statements)
         std::cerr << "Error closing database: (" << rc << ": " << sqlite3_errmsg(db) << ")"
                   << std::endl;
         // The 'db' pointer is invalid after sqlite3_close returns an error.
      }
      else
      {
         db = nullptr;  // Mark as closed
         std::cout << "Database closed." << std::endl;
      }
   }

   return 0;
}