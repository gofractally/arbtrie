#pragma once
#include <arbtrie/hash/lehmer64.h>
#include <assert.h>
#include <arbtrie/address.hpp>
#include <arbtrie/circular_buffer.hpp>
#include <arbtrie/config.hpp>
#include <arbtrie/hash/xxh32.hpp>
#include <arbtrie/hierarchical_bitmap.hpp>
#include <arbtrie/padded_atomic.hpp>
#include <arbtrie/rdtsc.hpp>
#include <arbtrie/size_weighted_age.hpp>
#include <arbtrie/spmc_buffer.hpp>
#include <arbtrie/util.hpp>
#include <bit>
#include <cstdint>

namespace arbtrie
{

   /// index into meta[free_segment_index]._free_segment_number
   using free_segment_index = uint64_t;

   using rcache_queue_type = circular_buffer<id_address, 1024 * 256>;

   // types that are memory mapped
   namespace mapped_memory
   {

      static constexpr segment_number invalid_segment_num = segment_number(-1);

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
            _lock_ptr.set_low_bits(high_bits);
         }

         /**
          * Unlock the session by setting the low bits to maximum value
          */
         void unlock() { _lock_ptr.set_low_bits(uint32_t(-1)); }

         /**
          * Update the high bits with the new end pointer value
          */
         void update(uint32_t end) { _lock_ptr.set_high_bits(end); }

         /**
          * Get the current value of the lock pointer
          */
         uint64_t load(std::memory_order order = std::memory_order_relaxed) const
         {
            return _lock_ptr.load(order);
         }

        private:
         padded_atomic<uint64_t> _lock_ptr{uint64_t(-1)};
      };

      /**
       * Shared state for segment threads that is stored in mapped memory
       * for inter-process coordination
       * 
       * used by @ref segment_thread to track thread state
       */
      struct segment_thread_state
      {
         // Flag indicating if the thread is currently running
         // Used to prevent multiple processes from running duplicate threads
         // and to detect unclean shutdowns
         std::atomic<bool> running{false};

         // Process ID of the process running the thread
         // Helps with debugging and determining if the process crashed
         std::atomic<int> pid{0};

         // When the thread was started
         std::atomic<int64_t> start_time_ms{0};

         // Last time the thread reported being alive (heartbeat)
         std::atomic<int64_t> last_alive_time_ms{0};
      };

      // meta data about each segment,
      // stored in an array in allocator_state indexed by segment number
      // data is reconstructed on crash recovery and not synced
      struct segment_meta
      {
         /**
          *  When the database is synced, the last_sync_page is advanced and everything from
          * the start of the segment up to the last_sync_page is mprotected as read only. The
          * segment_header::alloc_pos is then moved to the end of the last_sync_page and any 
          * left over space on the last_sync_page is marked as free. This is because the OS
          * can only sync and write protect at the page boundary
          * 
          * rel_virtual_age is a 20bit floating point number that represents the virtual age
          * relative to a base virtual age. The base virtual age is updated from time to time
          * so that the floating point number has greatest precision near the current time and
          * least precision near the oldest segments.
          * 
          * 1. virtual age changes during allocation (when compaction ignores it)
          * 2. virtual age is then used to prioritize unlocking and compaction
          */
         struct state_data
         {
            uint64_t free_space : 26;       // able to store segment_size
            uint64_t unused : 20;           // store the relative virtual age
            uint64_t last_sync_page : 14;   //  segment_size / 4096 byte pages
            uint64_t is_alloc : 1     = 0;  // the segment is a session's private alloc area
            uint64_t is_pinned : 1    = 0;  // indicates that the segment is mlocked
            uint64_t is_read_only : 1 = 0;  // indicates that the segment is write protected

            static_assert((1 << 26) > segment_size);
            static_assert((1 << 20) >= segment_size / cacheline_size);
            static_assert((1 << 14) > segment_size / os_page_size);

            uint64_t to_int() const { return std::bit_cast<uint64_t>(*this); }
            explicit state_data(uint64_t x) { *this = std::bit_cast<state_data>(x); }

            state_data& set_last_sync_page(uint32_t page);
            state_data& free(uint32_t size);
            state_data& free_object(uint32_t size);
            state_data& set_alloc(bool s);
            state_data& set_pinned(bool s);
            state_data& set_read_only(bool s);

            // Set free_space to a specific value
            state_data& set_free_space(uint32_t size)
            {
               free_space = size;
               return *this;
            }
         };
         static_assert(sizeof(state_data) == sizeof(uint64_t));
         static_assert(std::is_trivially_copyable_v<state_data>);

         state_data get_free_state() const;
         state_data data() const { return get_free_state(); }
         void       free_object(uint32_t size);
         void       free(uint32_t size);
         void       finalize_segment(uint32_t size, uint64_t vage_value);
         void       clear() { _state_data.store(0, std::memory_order_relaxed); }
         bool       is_alloc() { return get_free_state().is_alloc; }
         bool       is_pinned() { return get_free_state().is_pinned; }
         void       set_pinned(bool s)
         {
            auto expected = _state_data.load(std::memory_order_relaxed);
            auto updated  = state_data(expected).set_pinned(s).to_int();
            while (
                not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
               updated = state_data(expected).set_pinned(s).to_int();
         }
         void finalize_compaction()
         {
            auto expected = _state_data.load(std::memory_order_relaxed);

            // Safety check: is_alloc should be false when finalizing compaction
            assert(not state_data(expected).is_alloc);

            // Create updated state with reset free_space and rel_virtual_age
            // but preserve other flags like is_pinned, is_read_only, and last_sync_page
            auto updated = state_data(expected).set_free_space(0).to_int();

            while (!_state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
               updated = state_data(expected).set_free_space(0).to_int();
            vage.store(0, std::memory_order_relaxed);
         }
         void     set_alloc_state(bool a);
         uint64_t get_last_sync_pos() const;
         void     start_alloc_segment();
         void     set_last_sync_pos(uint64_t pos);
         uint64_t get_vage() const { return vage.load(std::memory_order_relaxed); }
         void     set_vage(uint64_t vage) { this->vage.store(vage, std::memory_order_relaxed); }
         /// the total number of bytes freed by swap
         /// or by being moved to other segments.
         std::atomic<uint64_t> _state_data;

         /// virtual age of the segment, initialized as 1024x the segment_header::_age
         /// and updated with weighted average as data is allocated
         std::atomic<uint64_t> vage;
      };

      /**
       *  The segment header is actually stored in the last 64 bytes of the segment,
       *  and is used to track data about the segment that changes as it is being allocated.
       * 
       *  Segments are designed to be marked as read only as data is committed; therefore,
       *  the header is the last part to be marked as read only.
       * 
       *  TODO: add a char[] buffer to the header to make it the size of a segment with
       * the last 64 bytes containing the header information. 
       */
      struct segment_header
      {
         uint32_t get_alloc_pos() const { return _alloc_pos.load(std::memory_order_relaxed); }

         // the next position to allocate data, only
         // used by the thread that owns this segment and
         // set to uint64_t max when this segment is ready
         // to be marked read only to the seg_allocator
         std::atomic<uint32_t> _alloc_pos = 64;  // aka sizeof(segment_header)

         uint32_t _session_id;       ///< the session id that allocated this segment
         uint32_t _seg_sequence;     ///< the sequence number of this sessions segment alloc
         uint64_t _open_time_usec;   ///< unix time in microseconds this segment started writing
         uint64_t _close_time_usec;  ///< unix time in microseconds this segment was closed

         // every time a segment is allocated it is assigned an age
         // which aids in reconstruction, newer values take priority over older values
         uint32_t _age;

         // used to calculate object density of segment header,
         // to establish madvise
         uint32_t _checksum = 0;
         uint64_t unused[1];

         // Tracks accumulated virtual age during allocation
         size_weighted_age _vage_accumulator;
      };
      static_assert(sizeof(segment_header) == 64);

      /**
       * A full segment is a segment that contains the segment header and the data
       * that is allocated in the segment. The "header" is moved to the last 64 bytes
       * so that the segment can be marked read-only as sync() is called while still
       * being able to modify the header data until the segment is finalized.
       */
      struct full_segment
      {
         char           data[segment_size - sizeof(segment_header)];
         segment_header header;
      };
      static_assert(sizeof(full_segment) == segment_size);
      /**
       * The data stored in the allocator_state is not written to disk on sync
       * and may be in a corrupt state after a hard crash. All values contained
       * within the allocator_state must be reconstructed from the segments
       */
      struct allocator_state
      {
         // set to 0 just before exit, set to 1 when opening database
         std::atomic<bool>     clean_exit_flag;
         std::atomic<uint32_t> next_alloc_age = 0;

         // Thread state for the read bit decay thread
         segment_thread_state  read_bit_decay_thread_state;
         std::atomic<uint16_t> next_clear_read_bit_region{0};

         // Thread state for the segment provider thread
         segment_thread_state segment_provider_thread_state;

         // Thread state for the compactor thread
         segment_thread_state compact_thread_state;

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
               auto elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                   current_time - _last_update);
               int64_t elapsed_ms = elapsed_time.count();

               int64_t target_ms = _cache_frequency_window.count();

               if (elapsed_ms <= 0 || target_ms <= 0)
                  return;  // No change if invalid

               // Define targets
               uint64_t target_bytes         = _total_cache_size / 16;  // 1/16th of cache
               int64_t  target_time_interval = target_ms / 16;  // 1/16th of total time in ms

               // Determine which trigger occurred
               bool bytes_trigger = (_bytes_promoted_since_last_difficulty_update >= target_bytes);
               bool time_trigger  = (elapsed_ms >= target_time_interval);

               // If neither trigger hit, no change
               if (!bytes_trigger && !time_trigger)
                  return;

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
               ARBTRIE_WARN("Cache difficulty updated: ", new_difficulty, " (1 in ",
                            attempts_per_hit, " attempts)",
                            " bytes_promoted: ", _bytes_promoted_since_last_difficulty_update,
                            " elapsed_ms: ", elapsed_ms, " probability: ", probability);

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
               //    _bytes_promoted_since_last_update += bytes;
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
            std::atomic<uint32_t>                              _cache_difficulty{
                uint32_t(-1) - (uint32_t(-1) / 1024)};  // 1 in 1024 probability
         };
         cache_difficulty_state _cache_difficulty_state;
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
         };
         segment_provider _segment_provider;

         /**
          * Segment metadata is organized by column rather than row to make
          * more effecient scanning when we only care about a single column of
          * data.  
          * 
          * @group segment_metadata Segment Meta Data 
          */
         struct segment_data
         {
            segment_meta meta[max_segment_count];

            /**
             * Get the last synced position for a segment
             */
            inline uint64_t get_last_sync_pos(segment_number segment) const;
         };
         segment_data _segment_data;

         struct session_data
         {
            uint32_t alloc_session_num();
            void     release_session_num(uint32_t num);
            auto&    rcache_queue(uint32_t session_num) { return _rcache_queue[session_num]; }
            uint32_t max_session_num() const
            {
               return std::countr_zero(free_sessions.load(std::memory_order_relaxed));
            }
            constexpr uint32_t session_capacity() const
            {
               return sizeof(_rcache_queue) / sizeof(_rcache_queue[0]);
            }
            uint32_t active_session_count() const
            {
               return std::popcount(free_sessions.load(std::memory_order_relaxed));
            }

            uint64_t free_session_bitmap() const
            {
               return free_sessions.load(std::memory_order_relaxed);
            }

            uint32_t session_segment_seq(uint32_t session_num) const
            {
               return _session_seg_seq[session_num];
            }
            uint32_t next_session_segment_seq(uint32_t session_num)
            {
               return _session_seg_seq[session_num] += 1;
            }

           private:
            // 1 bits mean free, 0 bits mean in use
            std::atomic<uint64_t> free_sessions{-1ull};

            // uses the 1/8th the space as tracking 1 bit per potential object id
            // but avoids the contention of using an atomic_hierarchical_bitmap
            // and allows the compactor to group data that is accessed together
            // next to each other in memory. (64 MB), session threads push to
            // their thread-local circular buffer and the compactor pops from
            // them and moves the referenced address to a pinned segment with
            // recent age.
            rcache_queue_type _rcache_queue[64];
            uint32_t          _session_seg_seq[64];
         };
         session_data _session_data;

         // meta data associated with each segment, indexed by segment number
         ///@}

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

            /// @group segment_provider Segment Provider Methods
            /// segment provider thread pops in batches and
            /// moves the segments into the hierarchical bitmap of free segments
            ///@{
            uint32_t available_to_pop() const;
            uint32_t pop_recycled_segments(segment_number* seg_nums, int size_seg_num);
            ///@}

            /// @group session_thread Session Thread Methods
            /// session threads lock/unlock when the start and stop reading
            ///@{
            void           read_lock_session(uint32_t session_idx);
            void           read_unlock_session(uint32_t session_idx);
            session_rlock& get_session_lock(uint32_t session_idx);
            ///@}

            read_lock_queue();

           private:
            void broadcast_end_ptr(uint32_t ep);

            // big enough for 32 GB of read-locked segments, nothing should
            // ever hold the read lock long enough to fill this buffer,
            // pushed by Compactor popped by Segment Provider
            circular_buffer<segment_number, 1024> recycled_segments;

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
         read_lock_queue _read_lock_queue;
      };

      inline void allocator_state::read_lock_queue::push_recycled_segment(segment_number seg_num)
      {
         broadcast_end_ptr(recycled_segments.push(seg_num));
      }

      /**
       *  Broadcast the end pointer to all sessions.
       *  Aka, set the high bits of each session's lock pointer to the new end pointer
       */
      inline void allocator_state::read_lock_queue::broadcast_end_ptr(uint32_t new_end_ptr)
      {
         for (auto& lock_ptr : _session_locks)
            lock_ptr.update(new_end_ptr);
      }

      /**
       *  Lock the read session by copying the last broadcasted end pointer to
       *  the session's lock pointer.  Aka, copy the high bits to the low bits.
      inline void allocator_state::read_lock_queue::read_lock_session(uint32_t session_idx)
      {
         _session_locks[session_idx].lock();
      }
       */

      /**
       * Unlocks a session by setting the sessions's lock pointer to the maximum value (uint32_t(-1)).
       * 
       * This effectively removes the session from consideration when calculating the minimum
       * read position across all sessions. When a session is unlocked, it no longer prevents
       * segments from being recycled or compacted, as its lock position is set to the maximum
       * value, which won't be the minimum in any calculation.
       * 
       * The method preserves the lower 32 bits of the lock_ptr (which represent the session's
       * view of the recycling queue) while only modifying the upper 32 bits (the end pointer).
       * 
       * It uses a fetch_add with a delta from the current value because on x86 fetch_add is
       * a single atomic operation that can be faster than a compare_exchange which is what
       * fetch_or is implemented as.
       * 
       * @param session_idx The index of the session to unlock
      inline void allocator_state::read_lock_queue::read_unlock_session(uint32_t session_idx)
      {
         _session_locks[session_idx].unlock();
      }
       */

      /**
       * Get a reference to the session_rlock for a specific session.
       * This allows the session to directly interact with its lock.
       * 
       * @param session_idx The index of the session
       * @return A reference to the session_rlock object for this session
       */
      inline session_rlock& allocator_state::read_lock_queue::get_session_lock(uint32_t session_idx)
      {
         return _session_locks[session_idx];
      }

      inline uint32_t allocator_state::read_lock_queue::available_to_push() const
      {
         return recycled_segments.free_space();
      }

      /**
       *  The number of segments between the pop_pos and the minimum session read pointer
       * 
       *  @return The number of segments that can be popped from the read_lock_queue
       */
      inline uint32_t allocator_state::read_lock_queue::available_to_pop() const
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

      // State data implementations
      inline segment_meta::state_data& segment_meta::state_data::set_last_sync_page(uint32_t page)
      {
         assert(page <= segment_size / os_page_size);
         last_sync_page = page;
         return *this;
      }

      inline segment_meta::state_data& segment_meta::state_data::free(uint32_t size)
      {
         assert(size > 0);
         assert(free_space + size <= segment_size);
         free_space += size;
         return *this;
      }

      inline segment_meta::state_data& segment_meta::state_data::free_object(uint32_t size)
      {
         assert(size > 0);
         assert(free_space + size <= segment_size);

         free_space += size;
         // ++free_objects;
         return *this;
      }

      inline segment_meta::state_data& segment_meta::state_data::set_alloc(bool s)
      {
         is_alloc = s;
         return *this;
      }

      inline segment_meta::state_data& segment_meta::state_data::set_pinned(bool s)
      {
         is_pinned = s;
         return *this;
      }

      inline segment_meta::state_data& segment_meta::state_data::set_read_only(bool s)
      {
         is_read_only = s;
         return *this;
      }

      // Segment meta implementations
      inline segment_meta::state_data segment_meta::get_free_state() const
      {
         return state_data(_state_data.load(std::memory_order_relaxed));
      }

      inline void segment_meta::free_object(uint32_t size)
      {
         auto expected = _state_data.load(std::memory_order_relaxed);
         auto updated  = state_data(expected).free_object(size).to_int();
         while (not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
            updated = state_data(expected).free_object(size).to_int();
      }

      inline void segment_meta::free(uint32_t size)
      {
         auto expected = _state_data.load(std::memory_order_relaxed);
         auto updated  = state_data(expected).free(size).to_int();
         while (not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
            updated = state_data(expected).free(size).to_int();
      }

      inline void segment_meta::finalize_segment(uint32_t size, uint64_t vage_value)
      {
         auto expected = _state_data.load(std::memory_order_relaxed);

         assert(state_data(expected).is_alloc);
         vage = vage_value;

         auto updated = state_data(expected).free(size).set_alloc(false).to_int();
         while (not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
            updated = state_data(expected).free(size).set_alloc(false).to_int();
      }

      inline void segment_meta::set_alloc_state(bool a)
      {
         auto expected = _state_data.load(std::memory_order_relaxed);
         auto updated  = state_data(expected).set_alloc(a).to_int();
         while (not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
            updated = state_data(expected).set_alloc(a).to_int();
      }

      inline uint64_t segment_meta::get_last_sync_pos() const
      {
         return state_data(_state_data.load(std::memory_order_relaxed)).last_sync_page *
                os_page_size;
      }

      inline void segment_meta::start_alloc_segment()
      {
         auto expected = _state_data.load(std::memory_order_relaxed);
         assert(not state_data(expected).is_alloc);
         auto updated = state_data(expected).set_last_sync_page(0).set_alloc(true).to_int();
         assert(state_data(updated).is_alloc);

         while (not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
            updated = state_data(expected).set_last_sync_page(0).set_alloc(true).to_int();
         assert(state_data(updated).is_alloc);
      }

      inline void segment_meta::set_last_sync_pos(uint64_t pos)
      {
         auto page_num = round_down_multiple<4096>(pos) / 4096;
         auto expected = _state_data.load(std::memory_order_relaxed);
         auto updated  = state_data(expected).set_last_sync_page(page_num).to_int();
         while (not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
            updated = state_data(expected).set_last_sync_page(page_num).to_int();
      }

      // Session data implementations
      inline uint32_t allocator_state::session_data::alloc_session_num()
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

      inline void allocator_state::session_data::release_session_num(uint32_t num)
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

      // pop_recycled_segments implementation
      inline uint32_t allocator_state::read_lock_queue::pop_recycled_segments(
          segment_number* seg_nums,
          int             size_seg_num)
      {
         return recycled_segments.pop(seg_nums, size_seg_num);
      }

      /**
       * Get the last synced position for a segment
       * 
       * @param segment The segment number
       * @return The last synced position in the segment
       */
      inline uint64_t allocator_state::segment_data::get_last_sync_pos(segment_number segment) const
      {
         return meta[segment].get_last_sync_pos();
      }

   }  // namespace mapped_memory

}  // namespace arbtrie
