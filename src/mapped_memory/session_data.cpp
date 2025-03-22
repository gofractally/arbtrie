#include <arbtrie/mapped_memory/session_data.hpp>

namespace arbtrie
{
   namespace mapped_memory
   {
      /// only one thread may call this a time, will block until it is
      /// safe to sync the segment, the caller is only allowed to sync
      /// the segment up to @ref segment::_first_writable_page
      void session_data::start_sync_segment(segment_number segment_num)
      {
         // only one thread can do syncing at a time
         _active_mask.store(-1ull);
         _sync_request.store(segment_num, std::memory_order_release);

         // Build mask of sessions modifying this segment - fully branchless
         uint64_t active_mods = 0;

         // Process in blocks of 4 with manual prefetching
         constexpr int BLOCK_SIZE       = 4;
         constexpr int LAST_BLOCK_INDEX = 64 - BLOCK_SIZE;

         // Main loop - process all complete blocks except the last one,
         // note that each _modify_lock is on its own cache line to
         // avoid false sharing, so there are 64 cache lines to load
         for (int i = 0; i < LAST_BLOCK_INDEX; i += BLOCK_SIZE)
         {
            // Calculate offsets once
            const int idx0 = i + 0;
            const int idx1 = i + 1;
            const int idx2 = i + 2;
            const int idx3 = i + 3;

            // Process current block with direct indexing
            uint64_t is_match0 =
                (_modify_lock[idx0].load(std::memory_order_acquire) == segment_num);
            uint64_t is_match1 =
                (_modify_lock[idx1].load(std::memory_order_acquire) == segment_num);
            uint64_t is_match2 =
                (_modify_lock[idx2].load(std::memory_order_acquire) == segment_num);
            uint64_t is_match3 =
                (_modify_lock[idx3].load(std::memory_order_acquire) == segment_num);

            // Reuse the same indices for the bit shifts
            active_mods |= (is_match0 << idx0) | (is_match1 << idx1) | (is_match2 << idx2) |
                           (is_match3 << idx3);
         }

         // Process the final block directly with compile-time constant expressions
         uint64_t is_match0 =
             (_modify_lock[LAST_BLOCK_INDEX + 0].load(std::memory_order_acquire) == segment_num);
         uint64_t is_match1 =
             (_modify_lock[LAST_BLOCK_INDEX + 1].load(std::memory_order_acquire) == segment_num);
         uint64_t is_match2 =
             (_modify_lock[LAST_BLOCK_INDEX + 2].load(std::memory_order_acquire) == segment_num);
         uint64_t is_match3 =
             (_modify_lock[LAST_BLOCK_INDEX + 3].load(std::memory_order_acquire) == segment_num);

         active_mods |=
             (is_match0 << (LAST_BLOCK_INDEX + 0)) | (is_match1 << (LAST_BLOCK_INDEX + 1)) |
             (is_match2 << (LAST_BLOCK_INDEX + 2)) | (is_match3 << (LAST_BLOCK_INDEX + 3));

         // Fast path: if no active mods, skip waiting
         if (active_mods == 0)
            return;

         // Wait for all active modifications to complete
         uint64_t current_mask;
         while ((current_mask = _active_mask.load(std::memory_order_acquire)) & active_mods)
            _active_mask.wait(current_mask);
      }

      /**
       * This is the slow path for when sync() has contention, not much code but
       * getting it out of the hot path and preventing it from being inlined
       * may make a difference in performance of the hot path. Who knows how much
       * code is in notify_all()
       */
      __attribute__((noinline)) void session_data::notify_sync_thread(uint32_t session_num)
      {
         _active_mask.fetch_sub(1ULL << session_num, std::memory_order_release);
         _active_mask.notify_all();
      }
   }  // namespace mapped_memory

}  // namespace arbtrie
