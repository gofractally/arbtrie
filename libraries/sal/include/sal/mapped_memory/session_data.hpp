#pragma once
#include <sal/config.hpp>
#include <sal/control_block_alloc.hpp>
#include <sal/debug.hpp>
#include <sal/mapped_memory/segment.hpp>
#include <sal/numbers.hpp>
#include <ucc/circular_buffer.hpp>
#include <ucc/padded_atomic.hpp>

namespace sal
{
   using rcache_queue_type  = ucc::circular_buffer<ptr_address, 1024 * 256>;
   using release_queue_type = ucc::circular_buffer<ptr_address, 1024 * 256>;

   namespace mapped_memory
   {
      /**
       * Each segment a transaction writes data to gets pushed to this queue,
       * when the transaction is complete (commit or abort), everything it touched
       * is marked read-only so that it can be cached / compacted.
       * 
       * This is sized for 16kb, which enables 4096 segments, each 32 MB, allowing
       * up to 128 GB of dirty memory before an exception is thrown. There would be
       * a lot to compact / recover so it just isn't practical to even consider
       * more.
       */
      struct dirty_segment_queue
      {
         void push(segment_number segment_num)
         {
            // ARBTRIE_WARN("dirty_segment_queue::push: segment_num: ", segment_num);
            if (_used >= 4096)
               throw std::runtime_error("dirty_segment_queue overflow");
            _segments[_used++] = segment_num;
         }
         segment_number pop() noexcept
         {
            return _used == 0 ? segment_number(-1) : _segments[--_used];
         }

        private:
         std::array<segment_number, 4096> _segments;
         uint32_t                         _used = 0;
      };

      struct session_data
      {
         /**
          * Capped at 64 because we use 64 bit atomics 
          * in many places 
          */
         static constexpr uint32_t session_cap = 64;

         // Constructor initializes all segment locks to -1
         session_data();

         allocator_session_number alloc_session_num();
         void                     release_session_num(allocator_session_number num) noexcept;
         rcache_queue_type&       rcache_queue(allocator_session_number session_num);
         release_queue_type&      release_queue(allocator_session_number session_num);
         uint32_t                 max_session_num() const;

         // the maximum number of sessions that can be supported
         constexpr uint32_t session_capacity() const;
         uint32_t           active_session_count() const;
         uint64_t           free_session_bitmap() const;
         uint32_t           session_segment_seq(allocator_session_number session_num) const;
         uint32_t           next_session_segment_seq(allocator_session_number session_num);
         void add_bytes_written(allocator_session_number session_num, uint64_t bytes) noexcept
         {
            _total_bytes_written[*session_num] += bytes;
         }
         dirty_segment_queue& dirty_segments(allocator_session_number session_num)
         {
            return _dirty_segments[*session_num];
         }

         /**
          * Gets the total bytes written by a specific session
          * @param session_num The session number to query
          * @return The total bytes written by the session
          */
         uint64_t total_bytes_written(allocator_session_number session_num) const
         {
            return _total_bytes_written[*session_num];
         }

        private:
         void notify_sync_thread(allocator_session_number session_num);

         // 1 bits mean free, 0 bits mean in use
         std::atomic<uint64_t> free_sessions{-1ull};

         // uses the 1/8th the space as tracking 1 bit per potential object id
         // but avoids the contention of using an atomic_hierarchical_bitmap
         // and allows the compactor to group data that is accessed together
         // next to each other in memory. session threads push to
         // their thread-local circular buffer and the compactor pops from
         // them and moves the referenced address to a pinned segment with
         // recent age.
         rcache_queue_type  _rcache_queue[session_cap];
         release_queue_type _release_queue[session_cap];

         /// each transaction
         dirty_segment_queue _dirty_segments[session_cap];

         // the sequence number of the next segment to be allocated by each session
         uint32_t _session_seg_seq[session_cap];

         /** tracks the number of bytes written by each session so we can
          * measure write amplification.
          */
         uint64_t _total_bytes_written[session_cap];
      };

      // Implementation of session_data methods

      inline session_data::session_data() {}

      inline rcache_queue_type& session_data::rcache_queue(allocator_session_number session_num)
      {
         return _rcache_queue[*session_num];
      }
      inline release_queue_type& session_data::release_queue(allocator_session_number session_num)
      {
         return _release_queue[*session_num];
      }

      inline uint32_t session_data::max_session_num() const
      {
         return std::countr_zero(free_sessions.load(std::memory_order_relaxed));
      }

      inline constexpr uint32_t session_data::session_capacity() const
      {
         return sizeof(_rcache_queue) / sizeof(_rcache_queue[0]);
      }

      inline uint32_t session_data::active_session_count() const
      {
         return std::popcount(free_sessions.load(std::memory_order_relaxed));
      }

      inline uint64_t session_data::free_session_bitmap() const
      {
         return free_sessions.load(std::memory_order_relaxed);
      }

      inline uint32_t session_data::session_segment_seq(allocator_session_number session_num) const
      {
         return _session_seg_seq[*session_num];
      }

      inline uint32_t session_data::next_session_segment_seq(allocator_session_number session_num)
      {
         return _session_seg_seq[*session_num] += 1;
      }

      inline allocator_session_number session_data::alloc_session_num()
      {
         auto     fs_bits = free_sessions.load(std::memory_order_relaxed);
         uint64_t new_fs_bits;
         uint32_t session_num;

         do
         {
            if (fs_bits == 0)
               throw std::runtime_error("max of 64 sessions can be in use");

            session_num = std::countr_zero(fs_bits);
            new_fs_bits = fs_bits & ~(1 << session_num);
            fs_bits     = free_sessions.load(std::memory_order_relaxed);
         } while (not free_sessions.compare_exchange_weak(fs_bits, new_fs_bits,
                                                          std::memory_order_relaxed));
         return allocator_session_number(session_num);
      }

      inline void session_data::release_session_num(allocator_session_number num) noexcept
      {
         // bit should be 0 (in use) when we attempt to release it
         assert(!(free_sessions.load(std::memory_order_relaxed) & (1 << *num)));

         // Set the bit to 1 to mark it as free
         free_sessions.fetch_add(uint64_t(1) << *num, std::memory_order_relaxed);
      }

   }  // namespace mapped_memory
}  // namespace sal