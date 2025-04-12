#pragma once
#include <sal/numbers.hpp>
#include <ucc/circular_buffer.hpp>

namespace sal
{
   namespace mapped_memory
   {
      /**
       * Encapsulates the read locking behavior for a session
       */
      class session_rlock
      {
        public:
         session_rlock() = default;

         /**
          * Lock the session by copying the last broadcasted end pointer to
          * the session's lock pointer (copy high bits to low bits)
          */
         void lock()
         {
            uint32_t high_bits = _lock_ptr.load(std::memory_order_relaxed) >> 32;
            ucc::set_low_bits(_lock_ptr, high_bits);
         }

         /**
          * Unlock the session by setting the low bits to maximum value
          */
         void unlock() { ucc::set_low_bits(_lock_ptr, uint32_t(-1)); }

         /**
          * Update the high bits with the new end pointer value
          */
         void update(uint32_t end) { ucc::set_high_bits(_lock_ptr, end); }

         /**
          * Get the current value of the lock pointer
          */
         uint64_t load(std::memory_order order = std::memory_order_relaxed) const
         {
            return _lock_ptr.load(order);
         }

        private:
         ucc::padded_atomic<uint64_t> _lock_ptr{uint64_t(-1)};
      };

      /**
       *  Read-locked segments
       * 
       *  After the compactor has emptied a segment, the segment cannot
       * be recycled until all read-locked sessions have finished reading
       * from the segment. Before any reads start the session read_lock 
       * records the push_pos() of the segment and when they are done reading
       * they set their recorded position to infinity(something greater than queue size)
       * 
       *  |-------A----R1--R2---E-------------| queue.size()
       *  
       *  A, R1, R2, E are all 64 bit numbers that count to infinity, but
       *  their index wraps % queue.size()
       * 
       *  E = push_pos() 
       *  A = pop_pos() 
       *  r = min(R1,R2,...)
       *  Read Locked = range [r, E)
       *  Reusable = range [A,r) 
       * 
       *  Compactor will stop compacting when E-A = queue.size()
       */
      struct read_lock_queue
      {
         /// @group compactor Compactor Methods
         /// compactor compacts segments when available_to_push() > 0
         /// and pushes segments after their contents are no longer needed
         ///@{
         uint32_t available_to_push() const;
         void     push_recycled_segment(segment_number seg_num);
         ///@}

         /// @group session_thread Session Thread Methods
         /// session threads lock/unlock when the start and stop reading
         ///@{
         //void           read_lock_session(allocator_session_number session_idx);
         //void           read_unlock_session(allocator_session_number session_idx);
         session_rlock& get_session_lock(allocator_session_number session_idx);
         ///@}

         /// @group segment_provider Segment Provider Methods
         /// segment provider thread pops in batches and
         /// moves the segments into the hierarchical bitmap of free segments
         ///@{
         uint32_t available_to_pop() const;
         uint32_t pop_recycled_segments(segment_number* seg_nums, int size_seg_num);
         ///@}

         read_lock_queue();

        private:
         void broadcast_end_ptr(uint32_t ep);

         // big enough for 32 GB of read-locked segments, nothing should
         // ever hold the read lock long enough to fill this buffer,
         // pushed by Compactor popped by Segment Provider
         ucc::circular_buffer<segment_number, 1024> recycled_segments;

         /**
             *  Each session_rlock contains a padded_atomic<int64_t> where:
             *  - Lower 32 bits represent R* above (session's view of recycling queue)
             *  - Upper 32 bits represent what compactor has pushed to the session, aka E
             *
             *  Allocator takes the min of the lower 32 bits to determine the lock position.
             *  These need to be in shared memory for inter-process coordination.
             * 
             * The idea is that we need to ensure consistency between the compactor,
             * the allocator, and the sessions locking data. Each session knows the
             * synchronizes with the compactor's end pointer and the find_min algorithm
             * to determine the correct lock position.
             */
         session_rlock _session_locks[64];
      };
      inline void read_lock_queue::push_recycled_segment(segment_number seg_num)
      {
         broadcast_end_ptr(recycled_segments.push(seg_num));
      }

      /**
       *  Broadcast the end pointer to all sessions.
       *  Aka, set the high bits of each session's lock pointer to the new end pointer
       */
      inline void read_lock_queue::broadcast_end_ptr(uint32_t new_end_ptr)
      {
         for (auto& lock_ptr : _session_locks)
            lock_ptr.update(new_end_ptr);
      }

      /**
       * Get a reference to the session_rlock for a specific session.
       * This allows the session to directly interact with its lock.
       * 
       * @param session_idx The index of the session
       * @return A reference to the session_rlock object for this session
       */
      inline session_rlock& read_lock_queue::get_session_lock(allocator_session_number session_idx)
      {
         return _session_locks[*session_idx];
      }

      inline uint32_t read_lock_queue::available_to_push() const
      {
         return recycled_segments.free_space();
      }

      /**
       *  The number of segments between the pop_pos and the minimum session read pointer
       * 
       *  @return The number of segments that can be popped from the read_lock_queue
       */
      inline uint32_t read_lock_queue::available_to_pop() const
      {
         // start with the maximuum possible value
         uint32_t min_read_pos = recycled_segments.get_push_pos();
         // go until the minimum possible value
         uint32_t pop_pos = recycled_segments.get_read_pos();

         // there is nothing possible
         if (min_read_pos == pop_pos)
            return 0;

         // something may be possible if all read pointers are greater than pop_pos
         for (auto& lock_ptr : _session_locks)
         {
            // read the low bits by casting to uint32_t
            uint32_t cur_read_pos = lock_ptr.load(std::memory_order_relaxed);
            min_read_pos          = std::min(min_read_pos, cur_read_pos);
            if (min_read_pos == uint32_t(-1))
               break;
         }
         // this works even when it wraps around because the maximum difference
         // between the two positions is max_segment_count which is less than
         // 2^31
         return min_read_pos - uint32_t(recycled_segments.get_read_pos());
         static_assert(max_segment_count < (1 << 31));
      }

      // pop_recycled_segments implementation
      inline uint32_t read_lock_queue::pop_recycled_segments(segment_number* seg_nums,
                                                             int             size_seg_num)
      {
         return recycled_segments.pop(seg_nums, size_seg_num);
      }
      // read_lock_queue constructor implementation
      inline read_lock_queue::read_lock_queue()
      {
         // Initialize all session lock pointers to infinity
         // TODO: some of these may have been initialized from
         // a previous run of the database and we should be careful
         // not to override them.
         // No need to explicitly initialize session_rlock objects as
         // they are initialized with -1ll in their constructor
      }

   }  // namespace mapped_memory
}  // namespace sal
