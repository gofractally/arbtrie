#pragma once
#include <atomic>
#include <chrono>
#include <limits>
#include <sal/config.hpp>
#include <sal/debug.hpp>

namespace sal
{
   /**
    * Types in this namespace must be able to function when allocated
    * in memory mapped files.
    */
   namespace mapped_memory
   {
      /**
       * Encapsulates the state and logic for adjusting the cache difficulty 
       * based upon the rate of data being promoted to the cache relative to
       * the cache size. Ideally the rate of promotion would fill the cache
       * in the time the cache is averaging frequency of access.
       */
      struct cache_difficulty_state
      {
         using time_point = std::chrono::time_point<std::chrono::system_clock>;
         using duration   = std::chrono::milliseconds;

         cache_difficulty_state()
             : _total_cache_size(
                   32 *
                   segment_size),  // TODO: configure this / sync with segment_provider::max_mlocked_segments
               _bytes_promoted_since_last_difficulty_update(0),
               _last_update(std::chrono::system_clock::now())
         {
         }
         /**
             * named after compactor, because only the compactor thread should
             * call this function, indirectly via compactor_promote_bytes()
             */
         void compactor_update_difficulty(time_point current_time)
         {
            // Calculate elapsed time in milliseconds
            auto elapsed_time =
                std::chrono::duration_cast<std::chrono::milliseconds>(current_time - _last_update);
            int64_t elapsed_ms = elapsed_time.count();

            int64_t target_ms = _cache_frequency_window.count();

            if (elapsed_ms <= 0 || target_ms <= 0)
               return;  // No change if invalid

            // The cache target is expressed as bytes/window, but the lottery
            // is a probability gap below uint64_t::max(). Observed policy bytes
            // are approximately proportional to that gap, so adjust the gap by
            // the expected/actual byte ratio instead of creeping by a fixed
            // amount every slice.
            uint64_t target_bps = target_promoted_bytes_per_sec();
            if (target_bps == 0)
               return;

            int64_t update_ms = target_ms / 64;
            if (update_ms < 1000)
               update_ms = 1000;
            if (update_ms > 5000)
               update_ms = 5000;

            uint64_t target_bytes_for_update =
                uint64_t((__uint128_t(target_bps) * uint64_t(update_ms)) / 1000);
            if (target_bytes_for_update == 0)
               target_bytes_for_update = 1;

            const uint64_t actual_bytes = _bytes_promoted_since_last_difficulty_update;
            if (elapsed_ms < update_ms && actual_bytes < target_bytes_for_update)
               return;

            uint64_t expected_bytes =
                uint64_t((__uint128_t(target_bps) * uint64_t(elapsed_ms)) / 1000);
            if (expected_bytes == 0)
               expected_bytes = 1;

            // Current difficulty value
            uint64_t max_uint64         = std::numeric_limits<uint64_t>::max();
            uint64_t current_difficulty = _cache_difficulty.load(std::memory_order_relaxed);
            uint64_t current_gap        = max_uint64 - current_difficulty;
            if (current_gap == 0)
               current_gap = 1;

            uint64_t new_gap;
            if (actual_bytes == 0)
            {
               // No useful bytes arrived in this interval; relax quickly.
               new_gap = current_gap > max_uint64 / 8 ? max_uint64 : current_gap * 8;
            }
            else
            {
               __uint128_t scaled_gap = __uint128_t(current_gap) * expected_bytes;
               scaled_gap /= actual_bytes;
               if (scaled_gap == 0)
                  scaled_gap = 1;
               if (scaled_gap > max_uint64)
                  scaled_gap = max_uint64;
               new_gap = uint64_t(scaled_gap);
            }

            // Clamp movement per control interval. Undershoot should converge
            // fast enough to matter in a running benchmark; overshoot backs off
            // more gently to avoid oscillating the read-side lottery.
            uint64_t min_gap = current_gap / 4;
            if (min_gap == 0)
               min_gap = 1;
            uint64_t max_gap = current_gap > max_uint64 / 8 ? max_uint64 : current_gap * 8;
            if (new_gap < min_gap)
               new_gap = min_gap;
            if (new_gap > max_gap)
               new_gap = max_gap;

            _cache_difficulty.store(max_uint64 - new_gap, std::memory_order_relaxed);

            // Reset the bytes counter and timestamp
            _bytes_promoted_since_last_difficulty_update = 0;
            _last_update                                 = current_time;
         }

         bool should_cache(uint64_t random, uint32_t size_bytes) const noexcept
         {
            if (size_bytes > max_cacheable_object_size)
               return false;

            uint64_t clines = (uint64_t(size_bytes) + cacheline_size - 1) / cacheline_size;
            if (clines == 0)
               clines = 1;

            const uint64_t max_uint64 = uint64_t(-1);
            const uint64_t difficulty =
                _cache_difficulty.load(std::memory_order_relaxed);
            const uint64_t gap = max_uint64 - difficulty;
            if (gap == 0)
               return random == max_uint64;

            uint64_t adjusted_gap = gap / clines;
            if (adjusted_gap == 0)
               adjusted_gap = 1;

            return random >= max_uint64 - adjusted_gap;
         }

         uint64_t get_cache_difficulty() const
         {
            return _cache_difficulty.load(std::memory_order_relaxed);
         }

         void configure_cache(uint64_t cache_bytes, uint64_t frequency_window_sec) noexcept
         {
            _total_cache_size      = cache_bytes;
            _cache_frequency_window =
                std::chrono::milliseconds(frequency_window_sec * 1000);
         }

         uint64_t target_promoted_bytes_per_sec() const noexcept
         {
            const auto window_ms = _cache_frequency_window.count();
            if (window_ms <= 0)
               return 0;
            return (_total_cache_size * 1000) / uint64_t(window_ms);
         }

         void compactor_policy_satisfied_bytes(
             uint64_t   bytes,
             time_point current_time = std::chrono::system_clock::now())
         {
            _bytes_promoted_since_last_difficulty_update += bytes;
            if (bytes != 0)
               total_cache_policy_satisfied_bytes.fetch_add(bytes,
                                                            std::memory_order_relaxed);
            compactor_update_difficulty(current_time);
         }

         // only the compactor thread should call this
         void compactor_promote_bytes(uint64_t   bytes,
                                      bool       source_was_pinned,
                                      bool       destination_is_pinned,
                                      uint32_t   source_demote_pressure_ppm = 0,
                                      time_point current_time = std::chrono::system_clock::now())
         {
            compactor_policy_satisfied_bytes(bytes, current_time);
            if (bytes != 0)
            {
               total_promoted_bytes.fetch_add(bytes, std::memory_order_relaxed);
               if (destination_is_pinned)
               {
                  if (source_was_pinned)
                  {
                     total_hot_to_hot_promotions.fetch_add(1, std::memory_order_relaxed);
                     total_hot_to_hot_promoted_bytes.fetch_add(bytes,
                                                               std::memory_order_relaxed);
                     total_hot_to_hot_demote_pressure_ppm.fetch_add(
                         source_demote_pressure_ppm, std::memory_order_relaxed);
                     total_hot_to_hot_byte_demote_pressure_ppm.fetch_add(
                         uint64_t(source_demote_pressure_ppm) * bytes,
                         std::memory_order_relaxed);
                  }
                  else
                  {
                     total_cold_to_hot_promotions.fetch_add(1, std::memory_order_relaxed);
                     total_cold_to_hot_promoted_bytes.fetch_add(bytes,
                                                                std::memory_order_relaxed);
                  }
               }
               else
               {
                  total_promoted_to_cold_promotions.fetch_add(1, std::memory_order_relaxed);
                  total_promoted_to_cold_bytes.fetch_add(bytes, std::memory_order_relaxed);
               }
            }
         }

         void compactor_promote_bytes(
             uint64_t   bytes,
             time_point current_time = std::chrono::system_clock::now())
         {
            compactor_policy_satisfied_bytes(bytes, current_time);
            if (bytes != 0)
               total_promoted_bytes.fetch_add(bytes, std::memory_order_relaxed);
         }

         void compactor_skip_young_hot_bytes(
             uint64_t   bytes,
             uint32_t   source_demote_pressure_ppm,
             time_point current_time = std::chrono::system_clock::now())
         {
            compactor_policy_satisfied_bytes(bytes, current_time);
            if (bytes != 0)
            {
               total_young_hot_skips.fetch_add(1, std::memory_order_relaxed);
               total_young_hot_skipped_bytes.fetch_add(bytes, std::memory_order_relaxed);
               total_young_hot_skip_demote_pressure_ppm.fetch_add(
                   source_demote_pressure_ppm, std::memory_order_relaxed);
               total_young_hot_skip_byte_demote_pressure_ppm.fetch_add(
                   uint64_t(source_demote_pressure_ppm) * bytes, std::memory_order_relaxed);
            }
         }

         /// TODO: should sync with segment_provider::max_mlocked_segments
         uint64_t _total_cache_size;

         /// updated by compactor processing rcache_queue
         //uint64_t   _bytes_promoted_since_last_update;
         uint64_t   _bytes_promoted_since_last_difficulty_update;
         time_point _last_update;

         /**
           * This is the amount of time that we expect to cycle the cache, shorter windows
           * adjust to higher frequency changes in probability of access but will cause more
           * SSD wear and extra copying. Longer windows will cause the cache to be less responsive
           * to changes in probability of access.
           */
         std::chrono::milliseconds _cache_frequency_window{60000};

         std::atomic<uint64_t>                              total_cache_policy_satisfied_bytes{0};
         std::atomic<uint64_t>                              total_promoted_bytes{0};
         std::atomic<uint64_t>                              total_hot_to_hot_promotions{0};
         std::atomic<uint64_t>                              total_hot_to_hot_promoted_bytes{0};
         std::atomic<uint64_t>                              total_hot_to_hot_demote_pressure_ppm{0};
         std::atomic<uint64_t>                              total_hot_to_hot_byte_demote_pressure_ppm{0};
         std::atomic<uint64_t>                              total_young_hot_skips{0};
         std::atomic<uint64_t>                              total_young_hot_skipped_bytes{0};
         std::atomic<uint64_t>                              total_young_hot_skip_demote_pressure_ppm{0};
         std::atomic<uint64_t>                              total_young_hot_skip_byte_demote_pressure_ppm{0};
         std::atomic<uint64_t>                              total_cold_to_hot_promotions{0};
         std::atomic<uint64_t>                              total_cold_to_hot_promoted_bytes{0};
         std::atomic<uint64_t>                              total_promoted_to_cold_promotions{0};
         std::atomic<uint64_t>                              total_promoted_to_cold_bytes{0};
         std::chrono::time_point<std::chrono::system_clock> _last_difficulty_update;
         std::atomic<uint64_t>                              _cache_difficulty{uint64_t(-1) -
                                                 (uint64_t(-1) / 1024)};  // 1 in 1024 probability
      };
   }  // namespace mapped_memory
}  // namespace sal
