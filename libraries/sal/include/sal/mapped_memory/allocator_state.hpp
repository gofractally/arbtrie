#pragma once
#include <atomic>
#include <sal/mapped_memory/cache_difficulty_state.hpp>
#include <sal/mapped_memory/read_lock_queue.hpp>
#include <sal/mapped_memory/segment_provider.hpp>
#include <sal/mapped_memory/segment_thread_state.hpp>
#include <sal/mapped_memory/session_data.hpp>

namespace sal
{
   namespace mapped_memory
   {
      /**
       * The data stored in the allocator_state is not written to disk on sync
       * and may be in a corrupt state after a hard crash. All values contained
       * within the allocator_state must be reconstructed from the segments
       */
      struct allocator_state
      {
         // set to 0 just before exit, set to 1 when opening database
         std::atomic<bool> clean_exit_flag;
         runtime_config    _config;

         // Thread state for the read bit decay thread
         segment_thread_state  read_bit_decay_thread_state;
         std::atomic<uint32_t> next_clear_read_bit_position{0};

         // Thread state for the segment provider thread
         segment_thread_state segment_provider_thread_state;
         segment_provider     _segment_provider;

         // Thread state for the compactor thread
         segment_thread_state   compact_thread_state;
         cache_difficulty_state _cache_difficulty_state;

         segment_thread_state pinned_compact_thread_state;

         // read lock queue, compactor pushes provider pops
         read_lock_queue _read_lock_queue;

         // data per session
         session_data _session_data;

         // data per segment
         segment_data _segment_data;
      };
   }  // namespace mapped_memory
}  // namespace sal