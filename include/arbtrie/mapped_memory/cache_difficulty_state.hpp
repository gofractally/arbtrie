#pragma once
#include <arbtrie/config.hpp>
#include <arbtrie/debug.hpp>
#include <arbtrie/util.hpp>
#include <atomic>
#include <chrono>

namespace arbtrie
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

            // Define targets
            uint64_t target_bytes         = _total_cache_size / 16;  // 1/16th of cache
            int64_t  target_time_interval = target_ms / 16;          // 1/16th of total time in ms
            // Determine which trigger occurred
            bool bytes_trigger = (_bytes_promoted_since_last_difficulty_update >= target_bytes);
            bool time_trigger  = (elapsed_ms >= target_time_interval);

            // If neither trigger hit, no change
            if (!bytes_trigger && !time_trigger)
               return;

            /*
            if (bytes_trigger)
               ARBTRIE_WARN("compactor_update_difficulty: ", elapsed_ms, " target_ms: ", target_ms,
                            " target_bytes: ", target_bytes, " time_trigger: ", time_trigger,
                            " bytes_trigger: ", bytes_trigger, " _recent_bytes_promoted: ",
                            _bytes_promoted_since_last_difficulty_update,
                            " elapsed_ms: ", elapsed_ms);
                            */

            // Current difficulty value
            uint64_t max_uint32         = uint32_t(-1);
            uint64_t current_difficulty = _cache_difficulty.load(std::memory_order_relaxed);
            uint32_t new_difficulty;

            // Simplified adjustment logic:
            if (bytes_trigger && (!time_trigger || elapsed_ms < target_time_interval))
            {
               // Bytes target hit first: increase difficulty by decreasing the gap from max by 20%
               // Calculate the gap between current difficulty and max
               uint64_t gap = max_uint32 - current_difficulty;

               // Reduce the gap by 20% (multiply by 0.8 = 4/5) using integer math
               uint64_t new_gap = (gap * 7) / 8;

               // Ensure a minimum gap of 1 to prevent reaching max_uint32
               new_gap = (new_gap < 1) ? 1 : new_gap;

               // Calculate new difficulty by subtracting the new gap from max
               new_difficulty = static_cast<uint32_t>(max_uint32 - new_gap);

               //     ARBTRIE_WARN("increasing difficulty from ", current_difficulty, " to ",
               //                  new_difficulty, " delta: ", new_difficulty - current_difficulty);
            }
            else
            {
               uint64_t gap     = max_uint32 - current_difficulty;
               auto     new_gap = (gap * 9) / 8;
               new_difficulty   = static_cast<uint32_t>(max_uint32 - new_gap);
               /**/
               // Time target hit first: decrease difficulty by multiplying by 0.75
               // Using integer math: multiply by 3 and divide by 4
               //new_difficulty = static_cast<uint32_t>((double(current_difficulty) * .99));
               //    new_difficulty = current_difficulty;
               //    ARBTRIE_ERROR("decreasing difficulty from ", current_difficulty, " to ",
               //                  new_difficulty, " delta: ", current_difficulty - new_difficulty);
            }

            // Calculate probability as "1 in N attempts"
            double   probability = 1.0 - (static_cast<double>(new_difficulty) / max_uint32);
            uint64_t attempts_per_hit =
                probability > 0 ? std::round(1.0 / probability) : max_uint32;

            // Print warning with the new difficulty expressed as 1 in N attempts

            //            ARBTRIE_WARN("Cache difficulty updated: ", new_difficulty, " (1 in ", attempts_per_hit,
            //                        " attempts)",
            //                         " bytes_promoted: ", _bytes_promoted_since_last_difficulty_update,
            //                         " elapsed_ms: ", elapsed_ms, " probability: ", probability);

            // Update the internal member directly
            _cache_difficulty.store(new_difficulty, std::memory_order_relaxed);

            // Reset the bytes counter and timestamp
            _bytes_promoted_since_last_difficulty_update = 0;
            _last_update                                 = current_time;
         }

         bool should_cache(uint32_t random, uint32_t size_bytes) const
         {
            if (size_bytes > max_cacheable_object_size)
               return false;
            // random = xxh32::hash((char*)&random, sizeof(random), 0);
            // convert size to a muultiple of cache size, rounding up
            // ensures that it is at least 1 so we don't divide by 0
            uint64_t clines = round_up_multiple<64>(size_bytes + 1) / 64;

            uint64_t adjusted_difficulty =
                _cache_difficulty.load(std::memory_order_relaxed) * clines;

            //ARBTRIE_WARN("should_cache: ", random, " > ", adjusted_difficulty, " = ",
            //              random > (uint32_t)adjusted_difficulty);
            return random >= (uint32_t)adjusted_difficulty;
         }

         uint64_t get_cache_difficulty() const
         {
            return _cache_difficulty.load(std::memory_order_relaxed);
         }

         // only the compactor thread should call this
         void compactor_promote_bytes(uint64_t   bytes,
                                      time_point current_time = std::chrono::system_clock::now())
         {
            _bytes_promoted_since_last_difficulty_update += bytes;
            total_promoted_bytes.fetch_add(bytes, std::memory_order_relaxed);
            compactor_update_difficulty(current_time);
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

         std::atomic<uint64_t>                              total_promoted_bytes{0};
         std::chrono::time_point<std::chrono::system_clock> _last_difficulty_update;
         std::atomic<uint32_t>                              _cache_difficulty{uint32_t(-1) -
                                                 (uint32_t(-1) / 1024)};  // 1 in 1024 probability
      };
   }  // namespace mapped_memory
}  // namespace arbtrie