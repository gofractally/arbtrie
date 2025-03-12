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

         /// only one thread may call this a time, will block until it is
         /// safe to sync the segment.
         void start_sync_segment(segment_number segment_num);
         /// call when done syncing the segment
         void end_sync_segment();

         /// call this before attempting to modify a segment.
         /// @return true if you can modify items in place in the segment
         inline bool try_modify_segment(uint32_t session_num, segment_number segment_num);

         /// call this when done modifying a segment
         inline void end_modify(uint32_t session_num);

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

         /// the segments each session is currently modifying, or -1 if none
         padded_atomic<uint64_t> _modify_lock[session_cap];
         /// the segment waiting to be synced, or -1 if none
         padded_atomic<uint64_t> _sync_request;
         /// sessions clear bits as they exit modify when they see the sync request
         padded_atomic<uint64_t> _active_mask;
      };

      // Implementation of session_data methods

      inline session_data::session_data()
      {
         for (auto& lock : _modify_lock)
            lock.store(-1);
         _sync_request.store(-1);
         _active_mask.store(-1);
      }

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

      /// call this before attempting to modify a segment.
      /// @return true if you can modify items in place in the segment
      inline bool session_data::try_modify_segment(uint32_t session_num, segment_number segment_num)
      {
         ARBTRIE_DEBUG("try_modify_segment: session_num=", session_num,
                       " segment_num=", segment_num);
         // only one segment can be modified at a time per session
         assert(_modify_lock[session_num].load(std::memory_order_relaxed) == -1);

         auto sync_lock_request = _sync_request.load(std::memory_order_acquire);
         if (sync_lock_request == segment_num) [[unlikely]]
            return false;

         auto& lock = _modify_lock[session_num];

         // Notify the world we are about to modify this segment
         lock.store(segment_num, std::memory_order_release);

         // Double check the lock.. before modifying
         if (_sync_request.load(std::memory_order_acquire) == segment_num) [[unlikely]]
         {
            // Reset this session's mod_indicator to -1
            lock.store(-1, std::memory_order_release);
            notify_sync_thread(session_num);
            return false;
         }
         // it is ok to modify the segment here
         return true;
      }

      /// call this when done modifying a segment
      inline void session_data::end_modify(uint32_t session_num)
      {
         ARBTRIE_DEBUG("end_modify: session_num=", session_num);
         // TODO: if the caller kept track of the expected value, we could use store() instead
         // of exchange() to avoid the atomic load.
         uint32_t segment_num = _modify_lock[session_num].exchange(-1, std::memory_order_release);
         assert(segment_num != -1);
         if (_sync_request.load(std::memory_order_acquire) == segment_num) [[unlikely]]
            notify_sync_thread(session_num);
      }

      /// call when done syncing the segment
      inline void session_data::end_sync_segment()
      {
         _sync_request.store(-1, std::memory_order_release);
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