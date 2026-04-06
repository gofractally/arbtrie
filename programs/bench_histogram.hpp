#pragma once
#include <cstdint>
#include <cstdio>
#include <bit>
#include <time.h>

/**
 * Fixed-size latency histogram with log2 microsecond buckets.
 *
 * 25 buckets covering <1us to 8s+.
 *
 * Usage:
 *   bench::latency_histogram h;
 *   auto t0 = bench::now_us();
 *   do_work();
 *   h.record(bench::now_us() - t0);
 *
 * Per-thread, no atomics. Merge at end:
 *   bench::latency_histogram merged;
 *   for (auto& th : thread_histos) merged.merge(th);
 *   merged.print("commit");
 */

namespace bench
{
   /// Microsecond timestamp via clock_gettime CLOCK_MONOTONIC (~23ns on
   /// Linux x86 vDSO, ~20ns on macOS/ARM).  Portable across x86 and ARM.
   inline uint64_t now_us() noexcept
   {
      struct timespec ts;
      clock_gettime(CLOCK_MONOTONIC, &ts);
      return (uint64_t)ts.tv_sec * 1'000'000 + (uint64_t)ts.tv_nsec / 1000;
   }

   struct latency_histogram
   {
      static constexpr uint32_t num_buckets = 25;

      // Log2 microsecond buckets covering sub-microsecond to ~16 seconds:
      //  0: [0, 1)        5: [16, 32)       10: [512us, 1ms)   15: [16, 32ms)    20: [512ms, 1s)
      //  1: [1, 2)        6: [32, 64)       11: [1, 2ms)       16: [32, 64ms)    21: [1, 2s)
      //  2: [2, 4)        7: [64, 128)      12: [2, 4ms)       17: [64, 128ms)   22: [2, 4s)
      //  3: [4, 8)        8: [128, 256)     13: [4, 8ms)       18: [128, 256ms)  23: [4, 8s)
      //  4: [8, 16)       9: [256, 512)     14: [8, 16ms)      19: [256, 512ms)  24: [8s+)

      uint64_t counts[num_buckets] = {};
      uint64_t total_us = 0;
      uint64_t count    = 0;
      uint64_t max_us   = 0;

      void record(uint64_t us) noexcept
      {
         uint32_t bucket;
         if (us == 0)
            bucket = 0;
         else
         {
            // log2(us) + 1, clamped to num_buckets - 1
            bucket = (uint32_t)std::bit_width(us);  // = floor(log2(us)) + 1
            if (bucket >= num_buckets)
               bucket = num_buckets - 1;
         }
         counts[bucket]++;
         total_us += us;
         count++;
         if (us > max_us)
            max_us = us;
      }

      void merge(const latency_histogram& other) noexcept
      {
         for (uint32_t i = 0; i < num_buckets; i++)
            counts[i] += other.counts[i];
         total_us += other.total_us;
         count += other.count;
         if (other.max_us > max_us)
            max_us = other.max_us;
      }

      /// Returns the latency at the given percentile (0.0 - 1.0).
      /// Returns the upper bound of the bucket containing that percentile.
      uint64_t percentile(double p) const noexcept
      {
         if (count == 0)
            return 0;
         uint64_t target = (uint64_t)(count * p);
         uint64_t cumulative = 0;
         for (uint32_t i = 0; i < num_buckets; i++)
         {
            cumulative += counts[i];
            if (cumulative > target)
            {
               if (i == 0)
                  return 1;
               if (i >= num_buckets - 1)
                  return max_us;
               return 1ULL << i;  // upper bound of bucket i
            }
         }
         return max_us;
      }

      uint64_t avg_us() const noexcept { return count ? total_us / count : 0; }
      uint64_t p50() const noexcept { return percentile(0.50); }
      uint64_t p99() const noexcept { return percentile(0.99); }
      uint64_t p999() const noexcept { return percentile(0.999); }

      void print(const char* label) const
      {
         printf("  %-20s  avg=%4lu us  p50=%4lu us  p99=%4lu us  p99.9=%5lu us  max=%5lu us  (n=%lu)\n",
                label, avg_us(), p50(), p99(), p999(), max_us, count);
      }

      /// Print full bucket distribution.
      void print_detail(const char* label) const
      {
         printf("  %s latency distribution:\n", label);
         static const char* labels[num_buckets] = {
            "     <1 us", "    1-2 us", "    2-4 us", "    4-8 us",
            "   8-16 us", "  16-32 us", "  32-64 us", " 64-128 us",
            "128-256 us", "256-512 us", " 512us-1ms", "   1-2 ms",
            "   2-4 ms",  "   4-8 ms",  "  8-16 ms",  " 16-32 ms",
            " 32-64 ms",  "64-128 ms",  "128-256ms",  "256-512ms",
            "  512ms-1s", "    1-2 s",  "    2-4 s",  "    4-8 s",
            "     8+ s"
         };
         for (uint32_t i = 0; i < num_buckets; i++)
         {
            if (counts[i] == 0)
               continue;
            double pct = 100.0 * counts[i] / count;
            printf("    %s: %8lu  (%5.1f%%)\n", labels[i], counts[i], pct);
         }
      }

      /// Emit CSV header.
      static void csv_header(FILE* f)
      {
         fprintf(f, "avg_us,p50_us,p99_us,p999_us,max_us,count\n");
      }

      /// Emit CSV row.
      void csv_row(FILE* f) const
      {
         fprintf(f, "%lu,%lu,%lu,%lu,%lu,%lu\n",
                 avg_us(), p50(), p99(), p999(), max_us, count);
      }
   };

}  // namespace bench
