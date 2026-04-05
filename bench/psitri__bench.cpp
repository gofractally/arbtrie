/**
 * psitri benchmark — mirrors tidesdb__bench.c workload and output format
 * for direct comparison.
 *
 * Runs all three key patterns (sequential, random, zipfian) in a single
 * invocation for complete coverage.
 *
 * Covers: PUT (insert), GET (cold + warm cache), DELETE, iterator forward scan,
 *         iterator backward scan, iterator seek, iterator seek-for-prev.
 *
 * Key differences from TidesDB:
 *   - psitri is single-writer: PUT/DELETE run sequentially (batched transactions)
 *   - GET and iterator operations use concurrent read sessions
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <numeric>
#include <thread>
#include <vector>

#include <psitri/database.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>

using namespace psitri;

// ---------------------------------------------------------------------------
// ANSI color macros (match TidesDB bench output)
// ---------------------------------------------------------------------------
#define RESET "\033[0m"
#define BOLDGREEN "\033[1;32m"
#define BOLDRED "\033[1;31m"
#define BOLDCYAN "\033[1;36m"
#define BOLDWHITE "\033[1;37m"
#define BOLDMAGENTA "\033[1;35m"
#define BOLDYELLOW "\033[1;33m"
#define MAGENTA "\033[0;35m"

// ---------------------------------------------------------------------------
// Benchmark parameters — defaults match TidesDB's CMake defaults
// ---------------------------------------------------------------------------
#ifndef BENCH_NUM_OPERATIONS
#define BENCH_NUM_OPERATIONS 100000000
#endif
#ifndef BENCH_NUM_SEEK_OPS
#define BENCH_NUM_SEEK_OPS 1000
#endif
#ifndef BENCH_KEY_SIZE
#define BENCH_KEY_SIZE 16
#endif
#ifndef BENCH_VALUE_SIZE
#define BENCH_VALUE_SIZE 100
#endif
#ifndef BENCH_NUM_THREADS
#define BENCH_NUM_THREADS 8
#endif
#ifndef BENCH_DB_PATH
#define BENCH_DB_PATH "psitri_benchmark_db"
#endif
#ifndef BENCH_BATCH_SIZE
#define BENCH_BATCH_SIZE 1000
#endif

// ---------------------------------------------------------------------------
// Key/value generation — matches TidesDB bench exactly
// ---------------------------------------------------------------------------

static void generate_sequential_key(uint8_t* buffer, size_t size, int index)
{
   snprintf((char*)buffer, size, "k%06d", index);
}

static void generate_random_key(uint8_t* buffer, size_t size)
{
   static const char charset[] =
       "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
   for (size_t i = 0; i < size - 1; i++)
      buffer[i] = (uint8_t)charset[rand() % (int)(sizeof(charset) - 1)];
   buffer[size - 1] = '\0';
}

// Zipfian distribution — Hörmann-Derflinger rejection-inversion sampling
static double calc_continuous_approximation(double offset, double zipf_exponent, double x)
{
   return pow(offset + x, 1 - zipf_exponent) / (1 - zipf_exponent);
}

static double hinv(double zipf_exponent, double c_area)
{
   return floor(exp((log((1 - zipf_exponent) * c_area)) / (1 - zipf_exponent)));
}

static uint8_t zipf_next(double zipf_exponent, double off, double imax)
{
   double xmin     = 0.5;
   double xmax     = imax + 0.5;
   double hlow     = calc_continuous_approximation(off, zipf_exponent, xmin);
   double hupp     = calc_continuous_approximation(off, zipf_exponent, xmax);
   double tot_area = hupp - hlow;

   while (true)
   {
      double u      = (double)rand() / (double)RAND_MAX;
      double c_area = hlow + (u * tot_area);
      double k      = hinv(zipf_exponent, c_area);
      double l      = hlow - pow(off + k, -zipf_exponent);
      if (c_area >= l)
         return (uint8_t)k;
   }
}

static void generate_zipfian_key(uint8_t* buffer, size_t size, double max_operations)
{
   uint8_t key_num = zipf_next(1.3, 0.99, max_operations);
   snprintf((char*)buffer, size, "k%010d", key_num);
}

static void generate_deterministic_value(uint8_t* buffer, size_t size, int index)
{
   snprintf((char*)buffer, size, "val_%010d", index);
}

static void generate_zipfian_value(uint8_t* buffer, size_t size, uint8_t* key)
{
   snprintf((char*)buffer, size, "val_%s", (char*)(key + 1));
}

// ---------------------------------------------------------------------------
// Timing helper
// ---------------------------------------------------------------------------

static double get_time_ms()
{
   auto now = std::chrono::high_resolution_clock::now();
   return std::chrono::duration<double, std::milli>(now.time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// Thread data for concurrent reads
// ---------------------------------------------------------------------------

struct thread_data_t
{
   database*         db;
   uint8_t**         keys;
   uint8_t**         values;
   size_t*           key_sizes;
   size_t*           value_sizes;
   int               start;
   int               end;
   int               thread_id;
   int               count;
   int               num_ops;      // total operations (for seek key selection)
   int               num_seek_ops; // total seek ops across all threads
   std::atomic<int>* errors;
};

// ---------------------------------------------------------------------------
// GET worker — concurrent read sessions
// ---------------------------------------------------------------------------

static void thread_get(thread_data_t* data)
{
   auto rs  = data->db->start_read_session();
   auto cur = rs->create_cursor(0);

   for (int i = data->start; i < data->end;)
   {
      int batch_end = std::min(i + BENCH_BATCH_SIZE, data->end);

      for (int j = i; j < batch_end; j++)
      {
         key_view    kv((char*)data->keys[j], data->key_sizes[j]);
         std::string buf;
         int32_t     rc = cur.get(kv, &buf);

         if (rc >= 0)
         {
            if (buf.size() != data->value_sizes[j] ||
                memcmp(buf.data(), data->values[j], buf.size()) != 0)
            {
               data->errors->fetch_add(1);
               printf(BOLDRED "[Thread %d] GET verification failed for key %d\n" RESET,
                      data->thread_id, j);
            }
         }
         else
         {
            data->errors->fetch_add(1);
            printf(BOLDRED "[Thread %d] GET failed: key %d not found\n" RESET, data->thread_id, j);
         }
      }

      i = batch_end;
   }
}

// ---------------------------------------------------------------------------
// Iterator forward scan worker
// ---------------------------------------------------------------------------

static void thread_iter_forward(thread_data_t* data)
{
   auto rs  = data->db->start_read_session();
   auto cur = rs->create_cursor(0);

   int         count = 0;
   std::string prev_key;

   if (cur.seek_begin())
   {
      do
      {
         auto k = cur.key();

         if (!prev_key.empty())
         {
            std::string cur_key(k.data(), k.size());
            if (prev_key > cur_key)
            {
               data->errors->fetch_add(1);
               printf(BOLDRED
                      "[Thread %d] Forward iterator: keys out of order at position %d\n" RESET,
                      data->thread_id, count);
            }
            prev_key = std::move(cur_key);
         }
         else
         {
            prev_key.assign(k.data(), k.size());
         }

         count++;
      } while (cur.next());
   }

   data->count = count;
}

// ---------------------------------------------------------------------------
// Iterator backward scan worker
// ---------------------------------------------------------------------------

static void thread_iter_backward(thread_data_t* data)
{
   auto rs  = data->db->start_read_session();
   auto cur = rs->create_cursor(0);

   int         count = 0;
   std::string prev_key;

   if (cur.seek_last())
   {
      do
      {
         auto k = cur.key();

         if (!prev_key.empty())
         {
            std::string cur_key(k.data(), k.size());
            if (prev_key < cur_key)
            {
               data->errors->fetch_add(1);
               printf(BOLDRED
                      "[Thread %d] Backward iterator: keys out of order at position %d\n" RESET,
                      data->thread_id, count);
            }
            prev_key = std::move(cur_key);
         }
         else
         {
            prev_key.assign(k.data(), k.size());
         }

         count++;
      } while (cur.prev());
   }

   data->count = count;
}

// ---------------------------------------------------------------------------
// Iterator seek worker (lower_bound)
// ---------------------------------------------------------------------------

static void thread_iter_seek(thread_data_t* data)
{
   auto rs  = data->db->start_read_session();
   auto cur = rs->create_cursor(0);

   srand(time(nullptr) + data->thread_id);

   int num_seeks = data->num_seek_ops / BENCH_NUM_THREADS;
   for (int i = 0; i < num_seeks; i++)
   {
      int      key_idx = rand() % data->num_ops;
      key_view kv((char*)data->keys[key_idx], data->key_sizes[key_idx]);
      cur.lower_bound(kv);
   }
}

// ---------------------------------------------------------------------------
// Iterator seek-for-prev worker (lower_bound + prev)
// ---------------------------------------------------------------------------

static void thread_iter_seek_for_prev(thread_data_t* data)
{
   auto rs  = data->db->start_read_session();
   auto cur = rs->create_cursor(0);

   srand(time(nullptr) + data->thread_id + 1000);

   int num_seeks = data->num_seek_ops / BENCH_NUM_THREADS;
   for (int i = 0; i < num_seeks; i++)
   {
      int      key_idx = rand() % data->num_ops;
      key_view kv((char*)data->keys[key_idx], data->key_sizes[key_idx]);
      if (cur.lower_bound(kv))
         cur.prev();  // step back to key <= target
   }
}

// ---------------------------------------------------------------------------
// Run all benchmarks for a given key pattern
// ---------------------------------------------------------------------------

static void run_pattern(const char*                                              pattern_name,
                        std::function<void(uint8_t*, size_t, int)>               gen_key,
                        std::function<void(uint8_t*, size_t, int, uint8_t*)>     gen_value,
                        int                                                      num_ops,
                        int                                                      num_seek_ops)
{
   printf(BOLDYELLOW "\n========== Key Pattern: %s ==========\n" RESET, pattern_name);

   std::string db_path = std::string(BENCH_DB_PATH) + "_" + pattern_name;
   std::filesystem::remove_all(db_path);
   std::filesystem::create_directories(db_path + "/data");

   double start_time, end_time;

   // -- Allocate keys and values --
   auto keys        = std::vector<std::vector<uint8_t>>(num_ops);
   auto values      = std::vector<std::vector<uint8_t>>(num_ops);
   auto key_ptrs    = std::vector<uint8_t*>(num_ops);
   auto value_ptrs  = std::vector<uint8_t*>(num_ops);
   auto key_sizes   = std::vector<size_t>(num_ops);
   auto value_sizes = std::vector<size_t>(num_ops);

   for (int i = 0; i < num_ops; i++)
   {
      keys[i].resize(BENCH_KEY_SIZE);
      values[i].resize(BENCH_VALUE_SIZE);

      gen_key(keys[i].data(), BENCH_KEY_SIZE, i);
      key_sizes[i] = strlen((char*)keys[i].data());
      key_ptrs[i]  = keys[i].data();

      gen_value(values[i].data(), BENCH_VALUE_SIZE, i, keys[i].data());
      value_sizes[i] = strlen((char*)values[i].data());
      value_ptrs[i]  = values[i].data();
   }

   // -- Open database --
   auto db = database::open(db_path);

   // -- PUT --
   {
      printf(BOLDGREEN "\nBenchmarking Put operations...\n" RESET);
      auto ses = db->start_write_session();
      start_time = get_time_ms();

      for (int i = 0; i < num_ops;)
      {
         auto tx        = ses->start_transaction(0);
         int  batch_end = std::min(i + BENCH_BATCH_SIZE, num_ops);

         for (int j = i; j < batch_end; j++)
         {
            key_view   kv((char*)key_ptrs[j], key_sizes[j]);
            value_view vv((char*)value_ptrs[j], value_sizes[j]);
            tx.upsert(kv, vv);
         }

         tx.commit();
         i = batch_end;
      }

      end_time = get_time_ms();
      printf(BOLDGREEN "Put: %d operations in %.2f ms (%.2f ops/sec)\n" RESET, num_ops,
             end_time - start_time, (num_ops / (end_time - start_time)) * 1000);
   }

   // -- Close and reopen for cold cache --
   printf(BOLDGREEN "\nClosing and reopening database to clear caches...\n" RESET);
   db.reset();
   db = database::open(db_path, open_mode::open_existing);

   // -- Concurrent read tests --
   {
      std::atomic<int>               verification_errors{0};
      std::vector<thread_data_t>     tdata(BENCH_NUM_THREADS);
      std::vector<std::thread>       threads;

      // Count distinct keys for zipfian (duplicates mean fewer unique keys)
      int expected_keys = num_ops;
      if (strcmp(pattern_name, "zipfian") == 0)
      {
         // zipfian produces many duplicates — upsert means tree has fewer unique keys
         // count them by doing a forward scan
         auto rs  = db->start_read_session();
         auto cur = rs->create_cursor(0);
         expected_keys = 0;
         if (cur.seek_begin())
         {
            do { expected_keys++; } while (cur.next());
         }
         printf("  (zipfian: %d distinct keys in tree)\n", expected_keys);
      }

      for (int i = 0; i < BENCH_NUM_THREADS; i++)
      {
         tdata[i].db           = db.get();
         tdata[i].keys         = key_ptrs.data();
         tdata[i].values       = value_ptrs.data();
         tdata[i].key_sizes    = key_sizes.data();
         tdata[i].value_sizes  = value_sizes.data();
         tdata[i].start        = i * (num_ops / BENCH_NUM_THREADS);
         tdata[i].end          = (i == BENCH_NUM_THREADS - 1)
                                     ? num_ops
                                     : (i + 1) * (num_ops / BENCH_NUM_THREADS);
         tdata[i].thread_id    = i;
         tdata[i].count        = 0;
         tdata[i].num_ops      = num_ops;
         tdata[i].num_seek_ops = num_seek_ops;
         tdata[i].errors       = &verification_errors;
      }

      // -- GET (cold cache) --
      printf(BOLDGREEN "\nBenchmarking Get operations (cold cache)...\n" RESET);
      start_time = get_time_ms();

      for (int i = 0; i < BENCH_NUM_THREADS; i++)
         threads.emplace_back(thread_get, &tdata[i]);
      for (auto& t : threads)
         t.join();

      end_time = get_time_ms();
      printf(BOLDGREEN "Get: %d operations in %.2f ms (%.2f ops/sec)\n" RESET, num_ops,
             end_time - start_time, (num_ops / (end_time - start_time)) * 1000);

      if (verification_errors == 0)
         printf(BOLDGREEN "  ✓ All GET operations verified successfully\n" RESET);
      else
         printf(BOLDRED "  ✗ GET verification failed: %d errors\n" RESET,
                verification_errors.load());

      // -- Iterator Seek --
      printf(BOLDGREEN "\nBenchmarking Iterator Seek operations...\n" RESET);
      for (int i = 0; i < BENCH_NUM_THREADS; i++)
      {
         tdata[i].start = 0;
         tdata[i].end   = num_ops;
         tdata[i].count = 0;
      }

      threads.clear();
      start_time = get_time_ms();

      for (int i = 0; i < BENCH_NUM_THREADS; i++)
         threads.emplace_back(thread_iter_seek, &tdata[i]);
      for (auto& t : threads)
         t.join();

      end_time = get_time_ms();
      printf(BOLDGREEN "Iterator Seek: %d operations in %.2f ms (%.2f ops/sec)\n" RESET,
             num_seek_ops, end_time - start_time,
             (num_seek_ops / (end_time - start_time)) * 1000);

      // -- Iterator Seek For Prev --
      printf(BOLDGREEN "\nBenchmarking Iterator Seek For Prev operations...\n" RESET);

      threads.clear();
      start_time = get_time_ms();

      for (int i = 0; i < BENCH_NUM_THREADS; i++)
         threads.emplace_back(thread_iter_seek_for_prev, &tdata[i]);
      for (auto& t : threads)
         t.join();

      end_time = get_time_ms();
      printf(BOLDGREEN "Iterator Seek For Prev: %d operations in %.2f ms (%.2f ops/sec)\n" RESET,
             num_seek_ops, end_time - start_time,
             (num_seek_ops / (end_time - start_time)) * 1000);

      // -- Forward Iterator --
      printf(BOLDGREEN "\nBenchmarking Forward Iterator (full scan)...\n" RESET);
      verification_errors = 0;

      threads.clear();
      start_time = get_time_ms();

      for (int i = 0; i < BENCH_NUM_THREADS; i++)
         threads.emplace_back(thread_iter_forward, &tdata[i]);
      for (auto& t : threads)
         t.join();

      end_time = get_time_ms();

      int keys_per_thread = tdata[0].count;
      printf(BOLDGREEN "Forward Iterator: %d threads in %.2f ms (%.2f ops/sec)\n" RESET,
             BENCH_NUM_THREADS, end_time - start_time,
             (double(expected_keys) / (end_time - start_time)) * 1000);

      if (keys_per_thread == expected_keys)
         printf(BOLDGREEN "  ✓ Each thread iterated all %d keys successfully\n" RESET,
                keys_per_thread);
      else
         printf(BOLDRED "  ✗ Iterator count mismatch: expected %d, got %d keys per thread\n" RESET,
                expected_keys, keys_per_thread);

      if (verification_errors > 0)
         printf(BOLDRED "  ✗ Iterator verification failed: %d errors\n" RESET,
                verification_errors.load());

      // -- Backward Iterator --
      printf(BOLDGREEN "\nBenchmarking Backward Iterator (full scan)...\n" RESET);
      verification_errors = 0;

      for (int i = 0; i < BENCH_NUM_THREADS; i++)
         tdata[i].count = 0;

      threads.clear();
      start_time = get_time_ms();

      for (int i = 0; i < BENCH_NUM_THREADS; i++)
         threads.emplace_back(thread_iter_backward, &tdata[i]);
      for (auto& t : threads)
         t.join();

      end_time = get_time_ms();

      int backward_keys_per_thread = tdata[0].count;
      printf(BOLDGREEN "Backward Iterator: %d threads in %.2f ms (%.2f ops/sec)\n" RESET,
             BENCH_NUM_THREADS, end_time - start_time,
             (double(expected_keys) / (end_time - start_time)) * 1000);

      if (backward_keys_per_thread == expected_keys)
         printf(BOLDGREEN "  ✓ Each thread iterated all %d keys successfully\n" RESET,
                backward_keys_per_thread);
      else
         printf(BOLDRED "  ✗ Iterator count mismatch: expected %d, got %d keys per thread\n" RESET,
                expected_keys, backward_keys_per_thread);

      if (verification_errors > 0)
         printf(BOLDRED "  ✗ Iterator verification failed: %d errors\n" RESET,
                verification_errors.load());
   }

   // Report size before deletes (this is the live-data size)
   {
      auto stats = db->dump();
      uint64_t file_size = stats.total_segments * sal::segment_size;
      uint64_t free_space = stats.total_free_space;
      uint64_t used_space = file_size > free_space ? file_size - free_space : 0;
      printf(BOLDCYAN "\nDatabase size (before delete):\n" RESET);
      printf(BOLDCYAN "  File size:    %.2f MB (%.2f GB)\n" RESET,
             file_size / (1024.0 * 1024.0), file_size / (1024.0 * 1024.0 * 1024.0));
      printf(BOLDCYAN "  Logical used: %.2f MB (%.2f GB)\n" RESET,
             used_space / (1024.0 * 1024.0), used_space / (1024.0 * 1024.0 * 1024.0));
   }

   // -- DELETE --
   {
      printf(BOLDGREEN "\nBenchmarking Delete operations...\n" RESET);

      // Build deduplicated key list (zipfian has many duplicates)
      std::vector<int> delete_indices(num_ops);
      std::iota(delete_indices.begin(), delete_indices.end(), 0);
      std::sort(delete_indices.begin(), delete_indices.end(),
                [&](int a, int b)
                {
                   auto ka = key_view((char*)key_ptrs[a], key_sizes[a]);
                   auto kb = key_view((char*)key_ptrs[b], key_sizes[b]);
                   return ka < kb;
                });
      delete_indices.erase(
          std::unique(delete_indices.begin(), delete_indices.end(),
                      [&](int a, int b)
                      {
                         return key_sizes[a] == key_sizes[b] &&
                                memcmp(key_ptrs[a], key_ptrs[b], key_sizes[a]) == 0;
                      }),
          delete_indices.end());
      int num_deletes = (int)delete_indices.size();

      auto ses = db->start_write_session();
      start_time = get_time_ms();

      for (int i = 0; i < num_deletes;)
      {
         auto tx        = ses->start_transaction(0);
         int  batch_end = std::min(i + BENCH_BATCH_SIZE, num_deletes);

         for (int j = i; j < batch_end; j++)
         {
            int idx = delete_indices[j];
            key_view kv((char*)key_ptrs[idx], key_sizes[idx]);
            tx.remove(kv);
         }

         tx.commit();
         i = batch_end;
      }

      end_time = get_time_ms();
      printf(BOLDGREEN "Delete: %d operations in %.2f ms (%.2f ops/sec)\n" RESET, num_deletes,
             end_time - start_time, (num_deletes / (end_time - start_time)) * 1000);
   }

   // Wait for compactor to drain before reporting stats
   db->wait_for_compactor();

   // Report allocator stats after compactor has drained
   {
      auto stats = db->dump();
      uint64_t file_size = stats.total_segments * sal::segment_size;
      uint64_t free_space = stats.total_free_space;
      uint64_t used_space = file_size > free_space ? file_size - free_space : 0;
      printf(BOLDCYAN "File size:    %.2f MB (%.2f GB)  [high-water mark]\n" RESET,
             file_size / (1024.0 * 1024.0), file_size / (1024.0 * 1024.0 * 1024.0));
      printf(BOLDCYAN "Logical used: %.2f MB (%.2f GB)  [file - freed]\n" RESET,
             used_space / (1024.0 * 1024.0), used_space / (1024.0 * 1024.0 * 1024.0));
      printf(BOLDCYAN "Free space:   %.2f MB (%.2f GB)  [%d%% reclaimable]\n" RESET,
             free_space / (1024.0 * 1024.0), free_space / (1024.0 * 1024.0 * 1024.0),
             file_size > 0 ? (int)(100.0 * free_space / file_size) : 0);
      stats.print();
   }

   db.reset();
   std::filesystem::remove_all(db_path);
}

// ---------------------------------------------------------------------------
// main — runs all three key patterns
// ---------------------------------------------------------------------------

int main()
{
   srand((unsigned int)time(nullptr));

   printf(BOLDCYAN "\n*=== psitri Benchmark Configuration ===*\n" RESET);
   printf(BOLDWHITE "Workload Settings:\n" RESET);
   printf("  Operations: %d\n", BENCH_NUM_OPERATIONS);
   printf("  Seek Operations: %d\n", BENCH_NUM_SEEK_OPS);
   printf("  Key Size: %d bytes\n", BENCH_KEY_SIZE);
   printf("  Value Size: %d bytes\n", BENCH_VALUE_SIZE);
   printf("  Threads: %d (reads only — writes are single-writer)\n", BENCH_NUM_THREADS);
   printf("  Batch Size: %d\n", BENCH_BATCH_SIZE);
   printf("  Key Patterns: sequential, random, zipfian\n");
   printf("*======================================*\n" RESET);

   // -- Sequential --
   run_pattern(
       "sequential",
       [](uint8_t* buf, size_t sz, int i) { generate_sequential_key(buf, sz, i); },
       [](uint8_t* buf, size_t sz, int i, uint8_t*) { generate_deterministic_value(buf, sz, i); },
       BENCH_NUM_OPERATIONS, BENCH_NUM_SEEK_OPS);

   // -- Random --
   run_pattern(
       "random",
       [](uint8_t* buf, size_t sz, int) { generate_random_key(buf, sz); },
       [](uint8_t* buf, size_t sz, int i, uint8_t*) { generate_deterministic_value(buf, sz, i); },
       BENCH_NUM_OPERATIONS, BENCH_NUM_SEEK_OPS);

   // -- Zipfian --
   run_pattern(
       "zipfian",
       [](uint8_t* buf, size_t sz, int) {
          generate_zipfian_key(buf, sz, BENCH_NUM_OPERATIONS);
       },
       [](uint8_t* buf, size_t sz, int i, uint8_t* key) {
          generate_zipfian_value(buf, sz, key);
       },
       BENCH_NUM_OPERATIONS, BENCH_NUM_SEEK_OPS);

   printf(MAGENTA "\nAll benchmarks completed.\n" RESET);
   return 0;
}
