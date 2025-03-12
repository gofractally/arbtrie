#pragma once
#include <arbtrie/circular_buffer.hpp>
#include <arbtrie/hierarchical_bitmap.hpp>
#include <arbtrie/spmc_buffer.hpp>
#include <atomic>

namespace arbtrie
{
   namespace mapped_memory
   {
      /**
       * Data that belongs to the Segment Provider Thread
       */
      struct segment_provider
      {
         uint32_t max_mlocked_segments = 32;

         /** 
          * Segment Provider thread attempts to keep this buffer full of segments
          * so that the allocator never has to wait on IO to get a new segment.
          *
          * Segment Provider attempts to keep this buffer full of segments,
          * while Session Threads take prepared segments when they need to
          * write to the database.
          */
         spmc_buffer<small_segment_number> ready_segments;

         /** 
          * bitmap of segments that are free to be recycled pushed into
          * the ready_segments queue, only the Segment Provider thread
          * reads and writes from this queue. It pops from the read_lock_queue,
          * and sets the bits in the free_segments bitmap so that it can
          * quickly find free segments by position in the file.
          * 
          * The alternative to this data structure is a fixed size array
          * that uses insertion sort to keep the segments in order. This
          * data structure is about 30kb and an array able to hold a free
          * list of max_segment_count would be 1 MB. A free list of 8k
          * segments would be of equal size. This data structure is more
          * effecient to insert into and read from.  
          * 
          * 0 means segment is unavailable for recycling
          * 1 means segment is available for recycling
          */
         hierarchical_bitmap<max_segment_count> free_segments;

         /**
          * When a segment is popped from the read_segments queue,
          * the Segment Provider notices and then calls mlock on the
          * segment.
          * 
          * When the total number of mlocked segments is greater than
          * the runtime configured limit, the Segment Provider will 
          * look for the mlocked segment with the oldest virtual age 
          * within the set of mlocked segments.
          * 
          * On startup the database will mlock the segments in this
          * list for faster warm-up speed.
          * 
          * 0 means segment is not mlocked
          * 1 means segment is mlocked
          */
         hierarchical_bitmap<max_segment_count> mlock_segments;

         /**
          * The next sequence number for the segment provider to allocate
          */
         std::atomic<uint32_t> _next_alloc_seq = 0;
      };
   }  // namespace mapped_memory
}  // namespace arbtrie
