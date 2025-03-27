#pragma once
#include <arbtrie/address.hpp>
#include <arbtrie/circular_buffer.hpp>
#include <arbtrie/config.hpp>
#include <arbtrie/debug.hpp>
#include <arbtrie/padded_atomic.hpp>

namespace arbtrie
{
   using rcache_queue_type = circular_buffer<id_address, 1024 * 256>;

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
         segment_number pop()
         {
            //  if (_used > 0)
            //    ARBTRIE_WARN("dirty_segment_queue::pop: _used: ", _segments[_used - 1]);
            return _used == 0 ? -1 : _segments[--_used];
         }

        private:
         std::array<segment_number, 4097> _segments;
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

         uint32_t alloc_session_num();
         void     release_session_num(uint32_t num);
         auto&    rcache_queue(uint32_t session_num);
         uint32_t max_session_num() const;

         // the maximum number of sessions that can be supported
         constexpr uint32_t session_capacity() const;
         uint32_t           active_session_count() const;
         uint64_t           free_session_bitmap() const;
         uint32_t           session_segment_seq(uint32_t session_num) const;
         uint32_t           next_session_segment_seq(uint32_t session_num);
         void               add_bytes_written(uint32_t session_num, uint64_t bytes)
         {
            _total_bytes_written[session_num] += bytes;
         }

         /// only one thread may call this a time, will block until it is
         /// safe to sync the segment.
         // void start_sync_segment(segment_number segment_num);
         // /// call when done syncing the segment
         // void end_sync_segment();

         /// call this before attempting to modify a segment.
         /// @return true if you can modify items in place in the segment
         //inline bool try_modify_segment(uint32_t session_num, segment_number segment_num);

         /// call this when done modifying a segment
         //inline void end_modify(uint32_t session_num);
         dirty_segment_queue& dirty_segments(uint32_t session_num)
         {
            return _dirty_segments[session_num];
         }

         /**
          * Gets the total bytes written by a specific session
          * @param session_num The session number to query
          * @return The total bytes written by the session
          */
         uint64_t total_bytes_written(uint32_t session_num) const
         {
            return _total_bytes_written[session_num];
         }

        private:
         void notify_sync_thread(uint32_t session_num);

         // 1 bits mean free, 0 bits mean in use
         std::atomic<uint64_t> free_sessions{-1ull};

         // uses the 1/8th the space as tracking 1 bit per potential object id
         // but avoids the contention of using an atomic_hierarchical_bitmap
         // and allows the compactor to group data that is accessed together
         // next to each other in memory. (64 MB), session threads push to
         // their thread-local circular buffer and the compactor pops from
         // them and moves the referenced address to a pinned segment with
         // recent age.
         rcache_queue_type _rcache_queue[session_cap];

         // the sequence number of the next segment to be allocated by each session
         uint32_t _session_seg_seq[session_cap];

         /// each transaction
         dirty_segment_queue _dirty_segments[session_cap];

         /** tracks the number of bytes written by each session so we can
          * measure write amplification.
          */
         uint64_t _total_bytes_written[session_cap];

         /// the segments each session is currently modifying, or -1 if none
         // padded_atomic<uint64_t> _modify_lock[session_cap];
         /// the segment waiting to be synced, or -1 if none
         // padded_atomic<uint64_t> _sync_request;
         /// sessions clear bits as they exit modify when they see the sync request
         // padded_atomic<uint64_t> _active_mask;
      };

      // Implementation of session_data methods

      inline session_data::session_data() {}

      inline auto& session_data::rcache_queue(uint32_t session_num)
      {
         return _rcache_queue[session_num];
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

      inline uint32_t session_data::session_segment_seq(uint32_t session_num) const
      {
         return _session_seg_seq[session_num];
      }

      inline uint32_t session_data::next_session_segment_seq(uint32_t session_num)
      {
         return _session_seg_seq[session_num] += 1;
      }

      inline uint32_t session_data::alloc_session_num()
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

         // Debug output - shows session allocation details
         ARBTRIE_DEBUG("allocating session ", session_num, " - previous free_sessions=", std::hex,
                       fs_bits, " new free_sessions=", new_fs_bits, std::dec);

         return session_num;
      }

      inline void session_data::release_session_num(uint32_t num)
      {
         // Bit should be 0 (in use) when we attempt to release it
         uint64_t current_bits = free_sessions.load(std::memory_order_relaxed);
         bool     bit_is_set   = (current_bits & (1 << num)) != 0;

         // Debug output - shows session release details and potential issues
         ARBTRIE_DEBUG("releasing session ", num, " - current free_sessions=", std::hex,
                       current_bits, " bit value=", bit_is_set ? "1 (ALREADY FREE!)" : "0 (in use)",
                       std::dec);

         assert(!(free_sessions.load(std::memory_order_relaxed) & (1 << num)));

         // Set the bit to 1 to mark it as free
         free_sessions.fetch_add(uint64_t(1) << num, std::memory_order_relaxed);

         // Show the new state after release
         ARBTRIE_DEBUG("after release of session ", num, " - free_sessions=", std::hex,
                       free_sessions.load(std::memory_order_relaxed), std::dec);
      }

   }  // namespace mapped_memory
}  // namespace arbtrie