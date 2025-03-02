#pragma once
#include <assert.h>
#include <arbtrie/config.hpp>
#include <arbtrie/hierarchical_bitmap.hpp>
#include <arbtrie/padded_atomic.hpp>
#include <arbtrie/rdtsc.hpp>
#include <arbtrie/size_weighted_age.hpp>
#include <arbtrie/spmc_circular_buffer.hpp>
#include <arbtrie/util.hpp>

namespace arbtrie
{

   /// index into meta[free_segment_index]._free_segment_number
   using free_segment_index = uint64_t;

   // types that are memory mapped
   namespace mapped_memory
   {

      static constexpr segment_number invalid_segment_num = segment_number(-1);

      // meta data about each segment,
      // stored in an array in allocator_header indexed by segment number
      // data is reconstructed on crash recovery and not synced
      struct segment_meta
      {
         struct state_data
         {
            uint64_t free_space : 26;      // Max 128 MB in bytes
            uint64_t free_objects : 21;    // 128MB / 64 byte cacheline
            uint64_t last_sync_page : 15;  // 128MB / 4096 byte pages
            uint64_t is_alloc : 1  = 0;
            uint64_t is_pinned : 1 = 0;

            static_assert((1 << 26) > segment_size);
            static_assert((1 << 21) > segment_size / cacheline_size);
            static_assert((1 << 15) > segment_size / os_page_size);

            uint64_t to_int() const { return std::bit_cast<uint64_t>(*this); }
            explicit state_data(uint64_t x) { *this = std::bit_cast<state_data>(x); }

            state_data& set_last_sync_page(uint32_t page)
            {
               assert(page <= segment_size / os_page_size);
               last_sync_page = page;
               return *this;
            }

            state_data& free(uint32_t size)
            {
               assert(size > 0);
               assert(free_space + size <= segment_size);
               free_space += size;
               return *this;
            }
            state_data& free_object(uint32_t size)
            {
               assert(size > 0);
               assert(free_space + size <= segment_size);

               free_space += size;
               ++free_objects;
               return *this;
            }
            state_data& set_alloc(bool s)
            {
               is_alloc = s;
               return *this;
            }
            state_data& set_pinned(bool s)
            {
               is_pinned = s;
               return *this;
            }
         };
         static_assert(sizeof(state_data) == sizeof(uint64_t));
         static_assert(std::is_trivially_copyable_v<state_data>);

         auto get_free_state() const
         {
            return state_data(_state_data.load(std::memory_order_relaxed));
         }

         // returns the free space in bytes, and number of objects freed
         auto get_free_space_and_objs() const
         {
            return get_free_state();
            //  uint64_t v = _free_space_and_obj.load(std::memory_order_relaxed);
            //  return std::make_pair(v >> 32, v & 0xffffffff);
         }

         // notes that an object of size was freed
         void free_object(uint32_t size)
         {
            auto expected = _state_data.load(std::memory_order_relaxed);
            auto updated  = state_data(expected).free_object(size).to_int();
            while (
                not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
               updated = state_data(expected).free_object(size).to_int();
         }

         // doesn't increment object count
         void free(uint32_t size)
         {
            auto expected = _state_data.load(std::memory_order_relaxed);
            auto updated  = state_data(expected).free(size).to_int();
            while (
                not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
               updated = state_data(expected).free(size).to_int();
         }

         // after allocating
         void finalize_segment(uint32_t size)
         {
            auto expected = _state_data.load(std::memory_order_relaxed);

            assert(state_data(expected).is_alloc);

            auto updated = state_data(expected).free(size).set_alloc(false).to_int();
            while (
                not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
               updated = state_data(expected).free(size).set_alloc(false).to_int();
         }

         void clear()
         {
            _state_data.store(0, std::memory_order_relaxed);
            //_last_sync_pos.store(segment_size, std::memory_order_relaxed);
            _base_time = size_weighted_age();
         }

         bool is_alloc() { return get_free_state().is_alloc; }
         void set_alloc_state(bool a)
         {
            auto expected = _state_data.load(std::memory_order_relaxed);
            auto updated  = state_data(expected).set_alloc(a).to_int();
            while (
                not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
               updated = state_data(expected).set_alloc(a).to_int();
         }

         uint64_t get_last_sync_pos() const
         {
            return state_data(_state_data.load(std::memory_order_relaxed)).last_sync_page *
                   os_page_size;
         }

         void start_alloc_segment()
         {
            auto expected = _state_data.load(std::memory_order_relaxed);
            assert(not state_data(expected).is_alloc);
            auto updated = state_data(expected).set_last_sync_page(0).set_alloc(true).to_int();
            assert(state_data(updated).is_alloc);

            while (
                not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
               updated = state_data(expected).set_last_sync_page(0).set_alloc(true).to_int();
            assert(state_data(updated).is_alloc);
         }

         void set_last_sync_pos(uint64_t pos)
         {
            auto page_num = round_down_multiple<4096>(pos) / 4096;
            auto expected = _state_data.load(std::memory_order_relaxed);
            auto updated  = state_data(expected).set_last_sync_page(page_num).to_int();
            while (
                not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
               updated = state_data(expected).set_last_sync_page(page_num).to_int();
         }

         // std::atomic<uint64_t> _last_sync_pos;
         //  position of alloc pointer when last synced

         /**
          *   As data is written, this tracks the data-weighted
          *   average of time since data without read-bit set
          *   was written. By default this is the average allocation
          *   time, but when compacting data the incoming data may
          *   provide an alternative time to be averaged int.
          *
          *   - written by allocator thread
          *   - read by compactor after allocator thread is done with segment
          *
          *   - sharing cacheline with _free_space_and_obj which is
          *     written when data is freed which could be any number of
          *     threads.
          *
          *   - not stored on the segment_header because compactor
          *   iterates over all segment meta and we don't want to
          *   page in from disk segments just to read this time.
          *
          *   TODO: must this be atomic?
          */
         size_weighted_age _base_time;

         /// the total number of bytes freed by swap
         /// or by being moved to other segments.
         std::atomic<uint64_t> _state_data;

         // the avg time in ms between reads
         uint64_t read_frequency(uint64_t now = size_weighted_age::now())
         {
            return now - _base_time.time_ms;
         }
      };

      /// should align on a page boundary
      struct segment_header
      {
         uint32_t get_alloc_pos() const { return _alloc_pos.load(std::memory_order_relaxed); }

         // the next position to allocate data, only
         // used by the thread that owns this segment and
         // set to uint64_t max when this segment is ready
         // to be marked read only to the seg_allocator
         std::atomic<uint32_t> _alloc_pos = 64;  // aka sizeof(segment_header)

         // every time a segment is allocated it is assigned an age
         // which aids in reconstruction, newer values take priority over older ones
         uint32_t _age;

         // used to calculate object density of segment header,
         // to establish madvise
         // uint32_t _num_objects = 0;  // inc on alloc
         uint32_t _checksum = 0;
      };
      static_assert(sizeof(segment_header) <= 64);

      /**
       * The data stored in the alloc header is not written to disk on sync
       * and may be in a corrupt state after a hard crash. All values contained
       * within the allocator_header must be reconstructed from the segments
       */
      struct allocator_header
      {
         // set to 0 just before exit, set to 1 when opening database
         std::atomic<bool>     clean_exit_flag;
         std::atomic<uint32_t> next_alloc_age = 0;

         // Bitmap of available session slots, shared across processes
         std::atomic<uint64_t> free_sessions{-1ull};

         // bitmap of segments that are free to be recycled pushed into
         // the pending allocation queue.
         hierarchical_bitmap<max_segment_count> free_segments;

         // a background thread attempts to keep this buffer full of segments
         // so that the allocator never has to wait on IO to get a new segment.
         spmc_circular_buffer<small_segment_number> recycled_segments;

         // meta data associated with each segment, indexed by segment number
         segment_meta seg_meta[max_segment_count];

         // circular buffer described, big enough to hold every
         // potentially allocated segment which is subseuently freed.
         //
         // |-------A----R1--R2---E-------------| max_segment_count
         //
         // A = alloc_ptr where recycled segments are used
         // R* = session_ptrs last known recycled segment by each session
         // E = end_ptr where the next freed segment is posted to be recycled
         // Initial condition A = R* = E = 0
         // Invariant A <= R* <= E unless R* == -1
         //
         // If A == min(R*) then we must ask block_alloc to create a new segment
         //
         // A, R*, and E are 64 bit numbers that count to infinity, the
         // index in the buffer is A % max_segment_count which should be
         // a simple bitwise & operation if max_segment_count is a power of 2.
         // The values between [A-E) point to recyclable segments assuming no R*
         // is present. Values before A or E and after point to no valid segments
         segment_number free_seg_buffer[max_segment_count];

         /**
          *  Lower 32 bits represent R* above (session's view of recycling queue)
          *  Upper 32 bits represent what compactor has pushed to the session, aka E
          *
          *  Allocator takes the min of the lower 32 bits to determine the lock position.
          *  These need to be in shared memory for inter-process coordination.
          * 
          * The idea is that we need to ensure consistency between the compactor,
          * the allocator, and the sessions locking data. Each session knows the
          * synchronizes with the compactor's end pointer and the find_min algorithm
          * to determine the correct lock position.
          */
         padded_atomic<uint64_t> session_lock_ptrs[64];

         // these methods ensure proper wrapping of the free segment index when
         // addressing into the free_seg_buffer
         segment_number&       get_free_seg_slot(free_segment_index index);
         const segment_number& get_free_seg_slot(free_segment_index index) const;

         void                          push_recycled_segment(segment_number seg_num);
         void                          broadcast_end_ptr(uint32_t new_end_ptr);
         std::optional<segment_number> pop_recycled_segment(uint32_t min_rstar);

         /**
          * These functions are used to manage the free segment queue and ensure
          * that to the fullest extent possible data is allocated toward the
          * beginning of the database file and that we can reclaim unused space
          * on exit.
          */
         ///@{
         std::pair<segment_number, segment_number> sort_queue(segment_number end_segment);
         std::vector<segment_number> collect_segments(free_segment_index start_idx, size_t count);
         void push_and_broadcast_batch(const segment_number* segments, size_t count);
         std::pair<free_segment_index, size_t> claim_all();
         ///@}

         /**
          *  Flag to indicate that the compactor is sorting the free segment queue.
          *  When set, the allocator will wait for the compactor to finish sorting
          *  before attempting to recycle a segment.
          */
         std::atomic<bool> _queue_sort_flag;

         // when no segments are available for reuse, advance by segment_size
         padded_atomic<segment_offset>     alloc_ptr;  // A below
         padded_atomic<free_segment_index> end_ptr;    // E below
      };

      /// crash recovery:
      /// 1. scan all segments to find those that were mid-allocation:
      ///    if a lot of free space, then swap them and push to free seg buffer
      /// 2. Update reference counts on all objects in database
      /// 3. ? pray ?

      // Inline implementations
      inline segment_number& allocator_header::get_free_seg_slot(free_segment_index index)
      {
         return free_seg_buffer[index & (max_segment_count - 1)];
      }

      inline const segment_number& allocator_header::get_free_seg_slot(
          free_segment_index index) const
      {
         return free_seg_buffer[index & (max_segment_count - 1)];
      }

      inline void allocator_header::push_recycled_segment(segment_number seg_num)
      {
         assert(seg_num != invalid_segment_num && "Cannot recycle invalid segment");
         auto  ep   = end_ptr.load(std::memory_order_relaxed);
         auto& slot = get_free_seg_slot(ep);
         //         assert(slot == invalid_segment_num && "Slot must be empty before recycling");
         slot = seg_num;

         uint32_t new_end_ptr = end_ptr.fetch_add(1, std::memory_order_relaxed);

         // Update session_lock_ptrs for each active session
         broadcast_end_ptr(new_end_ptr);
      }

      inline void allocator_header::broadcast_end_ptr(uint32_t new_end_ptr)
      {
         auto fs = ~free_sessions.load(std::memory_order_relaxed);
         while (fs)
         {
            auto session_idx = std::countr_zero(fs);

            uint64_t cur         = session_lock_ptrs[session_idx].load(std::memory_order_relaxed);
            uint32_t cur_end_ptr = cur >> 32;
            int64_t  diff = static_cast<int64_t>(new_end_ptr) - static_cast<int64_t>(cur_end_ptr);
            uint64_t adjustment = static_cast<uint64_t>(diff) << 32;

            // because the adjustment has 0's in the lower 32 bits,
            // the fetch_add will not overwrite the lower 32 bits managed by the session thread
            session_lock_ptrs[session_idx].fetch_add(adjustment, std::memory_order_relaxed);

            fs &= fs - 1;  // Clear lowest set bit
         }
      }

      inline std::optional<segment_number> allocator_header::pop_recycled_segment(
          uint32_t min_rstar)
      {
         do
         {
            auto ap = alloc_ptr.load(std::memory_order_relaxed);
            while (min_rstar - ap > 1)
            {
               if (alloc_ptr.compare_exchange_weak(ap, ap + 1))
               {
                  auto& slot    = get_free_seg_slot(ap);
                  auto  seg_num = slot;

                  assert(seg_num != invalid_segment_num && "Found invalid segment in free buffer");

                  slot = invalid_segment_num;
                  return seg_num;
               }
            }
            // wait for the compactor to finish sorting the queue
            if (_queue_sort_flag.load(std::memory_order_relaxed))
            {
               _queue_sort_flag.wait(true, std::memory_order_relaxed);
               continue;
            }
            return std::nullopt;
         } while (true);
      }

      inline std::vector<segment_number> allocator_header::collect_segments(
          free_segment_index start_idx,
          size_t             count)
      {
         std::vector<segment_number> result;
         result.reserve(count);
         for (size_t i = 0; i < count; i++)
         {
            auto& slot = get_free_seg_slot(start_idx + i);
            if (slot != invalid_segment_num)
            {
               assert(slot != invalid_segment_num && "Found invalid segment in free buffer");
               result.push_back(slot);
               slot = invalid_segment_num;
            }
         }
         return result;
      }

      inline void allocator_header::push_and_broadcast_batch(const segment_number* segments,
                                                             size_t                count)
      {
         auto ep = end_ptr.load(std::memory_order_relaxed);
         // Push segments back
         for (size_t i = 0; i < count; i++)
         {
            auto& slot = get_free_seg_slot(ep + i);
            assert(slot == invalid_segment_num && "Slot must be empty before recycling");
            slot = segments[i];
            assert(slot != invalid_segment_num && "Cannot store invalid segment");
         }

         // Update end_ptr and broadcast
         uint32_t new_end_ptr = end_ptr.fetch_add(count, std::memory_order_relaxed);
         broadcast_end_ptr(new_end_ptr);
      }

      /**
       * Atomically claims all segments between alloc_ptr and end_ptr.
       * 
       * @return A pair containing:
       *         - first: The starting index where segments were claimed (old alloc_ptr)
       *         - second: Number of segments claimed (end_ptr - old alloc_ptr), or 0 if no segments available
       */
      inline std::pair<free_segment_index, size_t> allocator_header::claim_all()
      {
         do
         {
            auto ap = alloc_ptr.load(std::memory_order_relaxed);
            auto ep = end_ptr.load(std::memory_order_relaxed);

            if (ep <= ap)
               return {ap, 0};

            size_t claim_size = ep - ap;
            auto   old_ap     = ap;

            if (alloc_ptr.compare_exchange_weak(ap, ep, std::memory_order_relaxed))
               return {old_ap, claim_size};

         } while (true);
      }

      inline std::pair<segment_number, segment_number> allocator_header::sort_queue(
          segment_number end_segment)
      {
         // Set flag and ensure it's cleared on exit
         _queue_sort_flag.store(true, std::memory_order_release);
         auto guard = scoped_exit(
             [this]
             {
                _queue_sort_flag.store(false, std::memory_order_release);
                _queue_sort_flag.notify_all();
             });

         // Try to claim all segments in the queue
         auto [start_idx, count] = claim_all();
         if (count == 0)
            return {invalid_segment_num, end_segment};

         // Collect and sort segments
         auto batch = collect_segments(start_idx, count);
         std::sort(batch.begin(), batch.end());

         // Find segments contiguous with end_segment
         segment_number first_contiguous = invalid_segment_num;
         segment_number last_contiguous  = end_segment;

         // Search backwards through batch
         for (size_t i = batch.size(); i > 0; --i)
         {
            auto seg = batch[i - 1];
            if (seg != segment_number(last_contiguous - 1))
            {
               // Push non-contiguous segments back
               push_and_broadcast_batch(batch.data(), i);
               return {first_contiguous, end_segment};
            }

            if (first_contiguous == invalid_segment_num)
               first_contiguous = seg;
            last_contiguous = seg;
         }

         return {first_contiguous, end_segment};
      }

   }  // namespace mapped_memory

}  // namespace arbtrie
