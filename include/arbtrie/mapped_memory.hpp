#pragma once
#include <arbtrie/hash/lehmer64.h>
#include <assert.h>
#include <arbtrie/address.hpp>
#include <arbtrie/circular_buffer.hpp>
#include <arbtrie/config.hpp>
#include <arbtrie/hash/xxh32.hpp>
#include <arbtrie/mapped_memory/cache_difficulty_state.hpp>
#include <arbtrie/mapped_memory/read_lock_queue.hpp>
#include <arbtrie/mapped_memory/segment.hpp>
#include <arbtrie/mapped_memory/segment_provider.hpp>
#include <arbtrie/mapped_memory/segment_thread_state.hpp>
#include <arbtrie/mapped_memory/session_data.hpp>
#include <arbtrie/mapping.hpp>
#include <arbtrie/padded_atomic.hpp>
#include <arbtrie/rdtsc.hpp>
#include <arbtrie/spmc_buffer.hpp>
#include <arbtrie/util.hpp>
#include <cstdint>

namespace arbtrie
{

   /// index into meta[free_segment_index]._free_segment_number
   using free_segment_index = uint64_t;

   // types that are memory mapped
   namespace mapped_memory
   {
      static constexpr segment_number invalid_segment_num = segment_number(-1);

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
         std::atomic<uint16_t> next_clear_read_bit_region{0};

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

}  // namespace arbtrie
