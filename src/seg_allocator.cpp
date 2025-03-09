#include <errno.h>  // For errno and ESRCH
#include <sys/mman.h>
#include <algorithm>
#include <arbtrie/binary_node.hpp>
#include <arbtrie/file_fwd.hpp>
#include <arbtrie/seg_alloc_dump.hpp>
#include <arbtrie/seg_allocator.hpp>
#include <array>
#include <bit>
#include <cassert>
#include <cstring>
#include <new>
#include <string>
#include <thread>  // For std::this_thread::sleep_for

static const uint64_t page_size      = getpagesize();
static const uint64_t page_size_mask = ~(page_size - 1);
static const uint64_t segment_size   = 1ull << 30;  // 1GB
static const bool     debug_segments = false;

namespace arbtrie
{
   /** 
    * Insert a new element into a sorted array of pairs, maintaining sort order.
    * Uses binary search to find insertion point and rotation for efficient insertion.
    * 
    * @tparam N Size of the array
    * @tparam T Type of the first element in pair (segment number type)
    * @param arr The array to insert into
    * @param current_size Current number of elements in the array
    * @param new_value Pair to be inserted
    * @return Updated size after insertion
    */
   template <size_t N, typename T>
   int insert_sorted_pair(std::array<std::pair<T, int64_t>, N>& arr,
                          size_t                                current_size,
                          std::pair<T, int64_t>                 new_value)
   {
      // Check if this value should be included
      if (current_size < N ||
          new_value.second > arr[std::min<size_t>(N - 1, current_size - 1)].second)
      {
         // Determine the range to search within
         auto begin = arr.begin();
         auto end   = arr.begin() + std::min(N, static_cast<size_t>(current_size));

         // Find insertion position using binary search (for descending order by age)
         auto insertion_point =
             std::upper_bound(begin, end, new_value,
                              [](const auto& a, const auto& b) { return a.second > b.second; });

         // If we're at capacity, rotate out the lowest element
         if (current_size >= N)
         {
            std::rotate(insertion_point, arr.end() - 1, arr.end());
         }
         else
         {
            // We're still adding new elements
            std::rotate(insertion_point, arr.begin() + current_size,
                        arr.begin() + current_size + 1);
            current_size++;
         }

         // Set the value at the insertion point
         *insertion_point = new_value;
      }

      return current_size;
   }

   namespace mapped_memory
   {
      // Forward declarations
      class segment_header;

      // read_lock_queue constructor implementation
      allocator_state::read_lock_queue::read_lock_queue()
      {
         // Initialize all session lock pointers to infinity
         // TODO: some of these may have been initialized from
         // a previous run of the database and we should be careful
         // not to override them.
         // No need to explicitly initialize session_rlock objects as
         // they are initialized with -1ll in their constructor
      }
   }  // namespace mapped_memory

   seg_allocator::seg_allocator(std::filesystem::path dir)
       : _id_alloc(dir / "ids"),
         _block_alloc(dir / "segs", segment_size, max_segment_count),
         _seg_alloc_state_file(dir / "header", access_mode::read_write, true),
         _seg_sync_locks(max_segment_count),
         _dirty_segs(max_segment_count)
   {
      if (_seg_alloc_state_file.size() == 0)
      {
         _seg_alloc_state_file.resize(round_to_page(sizeof(mapped_memory::allocator_state)));
         new (_seg_alloc_state_file.data()) mapped_memory::allocator_state();
      }
      _mapped_state =
          reinterpret_cast<mapped_memory::allocator_state*>(_seg_alloc_state_file.data());

      mlock_pinned_segments();

      // Initialize and start all threads after _mapped_state is set
      _compactor_thread.emplace(&_mapped_state->compact_thread_state, "compactor",
                                [this](segment_thread& thread) { compactor_loop(thread); });

      _read_bit_decay_thread.emplace(&_mapped_state->read_bit_decay_thread_state, "read_bit_decay",
                                     [this](segment_thread& thread)
                                     { clear_read_bits_loop(thread); });

      _segment_provider_thread.emplace(&_mapped_state->segment_provider_thread_state,
                                       "segment_provider",
                                       [this](segment_thread& thread) { provider_loop(thread); });

      // Start all threads immediately
      _compactor_thread->start();
      _read_bit_decay_thread->start();
      _segment_provider_thread->start();
   }

   /**
    *  On startup, mlock all segments that are marked as pinned at last shutdown.
    */
   void seg_allocator::mlock_pinned_segments()
   {
      for (auto seg : _mapped_state->_segment_provider.mlock_segments)
      {
         auto* segment_ptr = get_segment(seg);
         if (mlock(segment_ptr, segment_size) != 0)
         {
            ARBTRIE_WARN("mlock failed for segment: ", seg, " error: ", strerror(errno));

            // Clear both the bitmap and the meta bit using the helper
            update_segment_pinned_state(seg, false);
         }
         else
         {
            // Ensure the meta bit is set correctly
            auto& meta  = _mapped_state->_segment_data.meta[seg];
            auto  state = meta.get_free_state();
            if (!state.is_pinned)
            {
               // Update only the meta bit since the bitmap already has this segment
               meta._state_data.store(state.set_pinned(true).to_int(), std::memory_order_relaxed);
            }
         }
      }
   }

   seg_allocator::~seg_allocator()
   {
      // Stop all threads
      if (_read_bit_decay_thread)
      {
         _read_bit_decay_thread->stop();
         _read_bit_decay_thread.reset();
      }

      if (_compactor_thread)
      {
         _compactor_thread->stop();
         _compactor_thread.reset();
      }

      if (_segment_provider_thread)
      {
         // Wake up any threads that might be waiting in the ready_segments buffer
         _mapped_state->_segment_provider.ready_segments.wake_blocked();

         // Stop the thread
         _segment_provider_thread->stop();
         _segment_provider_thread.reset();
      }
   }

   uint32_t seg_allocator::alloc_session_num()
   {
      return _mapped_state->_session_data.alloc_session_num();
   }

   void seg_allocator::release_session_num(uint32_t sn)
   {
      _mapped_state->_session_data.release_session_num(sn);
   }

   /**
    * Decays the read bits over time to enable effective
    * estimation of the frequency of reads on nodes, processing
    * regions at a rate that ensures all regions are processed within
    * the cache frequency window.
    *
    * @param thread Reference to the segment_thread running this function
    */
   void seg_allocator::clear_read_bits_loop(segment_thread& thread)
   {
      using namespace std::chrono;
      using namespace std::chrono_literals;

      // Initialize current_region from the persisted state
      uint16_t current_region =
          _mapped_state->next_clear_read_bit_region.load(std::memory_order_relaxed);

      // Number of total iterations needed to cover all regions (processing at least 1 region per iteration)
      const auto total_iterations_needed = id_alloc::max_regions;

      // Initial sleep time - will be recalculated in the loop
      milliseconds sleep_duration(0);

      // Loop until thread should exit (yield returns false)
      while (thread.yield(sleep_duration))
      {
         auto start_time = high_resolution_clock::now();
         _id_alloc.clear_some_read_bits(current_region, 1);

         // save and update the next region to process, wrapping around because uint16_t
         _mapped_state->next_clear_read_bit_region.store(++current_region,
                                                         std::memory_order_relaxed);

         // Dynamically calculate sleep time based on window size
         // Formula: window_time / total_iterations_needed = time_per_iteration
         auto time_per_iteration = std::chrono::duration_cast<std::chrono::milliseconds>(
             _mapped_state->_cache_frequency_window / total_iterations_needed);

         // Ensure a minimum time_per_iteration of 10ms to prevent excessive CPU usage
         time_per_iteration = std::max(time_per_iteration, 10ms);

         // Calculate actual processing time
         auto processing_time =
             duration_cast<milliseconds>(high_resolution_clock::now() - start_time);

         // Calculate actual sleep time by subtracting processing time
         sleep_duration = time_per_iteration > processing_time
                              ? time_per_iteration - processing_time
                              : milliseconds(0);
      }
   }
   /**
    * Responsible for promotingread data to hot cache and
    * reclaiming space from segments with a priority on reclaiming space
    * in the hot cache.
    * 
    * The virtual age of pages in the spmc_buffer needs to be based upon
    * the position of the segment in the buffer so that it loses mlock status
    * in the right order (low priority segments lose mlock first).
    * 
    * - we only munlock if mlocking increased beyond user limit; therefore,
    *   a mlocked segment in the queue will never lose its mlock status. We only
    *   mlock when processing an ack() from the consumer and only for segments that
    *   are not already mlocked... I suppose a consumer could grab an unlocked segment
    * before the provider pushes a locked segment, then when acking we would want to
    *   unlock the segment in the queue and there could be multiple, so giving them
    * a virtual age based upon the buffer position would work well. 
    */
   void seg_allocator::compactor_loop(segment_thread& thread)
   {
      auto ses = start_session();

      /** 
       *  The compactor always prioritizes cache, then ram, then
       *  all other segments each loop. It needs to keep data of similar
       *  age together to minimize movement of virtual age of objects.
       */
      while (thread.yield())
      {
         compactor_promote_rcache_data(ses);
         compact_pinned_segment(ses);
         compactor_promote_rcache_data(ses);
         compact_unpinned_segment(ses);
      }

      // prioritize compacting pinned segments that are not being used by any session
      // 1. dead space in RAM is a waste of memory and hurts performance
      // 2. data pinned in RAM don't cause SSD wear to move around...
      // 3. process data in a way that minimizes the difference between the
      //    virtual age of the source segment and the active writing segment.
      // 4. process hot data first (read cache), then pinned, then old data.

      // when processing data from unpinned segments, we don't want to move it to a
      // pinned segment because that will cause wear on the SSD... what we really want
      // is a way to claim an unpinned ready segment... by default, the pending segments
      // queue will be filled with pinned segments (because they go to the front of the line),
      // so we would want to allocate from the unpinned segments first.

      // This could happen by having a pop_back() method on the spmc circular buffer...
      // these are the ready segments closest to the head of the file.

      //     TODO: we need a way to capture bump the recylcled pinned segments
      //           to the front of the allocation list (after going rlock pergatory)
      //           because if we remove the mlock for even 1 second, let alone mark
      //           the pages as MADV_DONTNEED, we will cause a lot of wear on the SSD.
      //           - therefore, MADV_DONTNEED and munlock should only be called by the
      //             Segment Provider Thread, if it is not able to immediately push them
      //             to the front of the line. In fact, the Segment Provider Thread should
      //             swap any segments that are in line with mlocked segments.
      //
      //           - the Segment Provider Thread should be the only thread that can
      //             call munlock, and it should only unlock segments that are not in
      //             use by any session.
      //
      //           - the Segment Provider Thread should also be the only thread that can
      //             call MADV_DONTNEED.

      /**
       *  0. process the read cache for each session and move to a segment with current age
       *  1. find the set of pinned segments that are below the configuration threshold for
       *     maximum empty space of a pinned segment, then sort them by virtual age, then
       *     compact them in order of virtual age (to minimize movement of virtual age of 
       *     objects)... starting with the most recent virtual age first (closest to the read_cache age)
       *  2. find the set of segments that are above the configuration threshold for
       *     maximum empty space of a unpinned segment, sort by virtual age, then compact 
       *     them to a new segment remembering to keep the virtual age with new allocations. 
       *  After doing all this work, mark the rest of the active segment as free and start
       *  from the top. We don't want to mix old virtual age data with the read cache data.
       *        - it may leave a segment mostly empty, but it will have an old virtual age and
       *          will be compacted near the end of the next pass.
       */
   }
#if 0
   void seg_allocator::compact_loop_old()
   {
      ARBTRIE_WARN("compact_loop");
      using namespace std::chrono_literals;
      if (not _compactor_session)
         _compactor_session.emplace(start_session());

      ARBTRIE_WARN("compact_loop start: ", _done.load());
      while (not _done.load())
      {
         if (not compact_next_segment())
         {
            /// don't let the alloc threads starve for want of a free segment
            /// if the free segment queue is getting low, top it up... but
            /// don't top it up just because read threads have things blocked
            /// because they could "block" for a long time...

            auto min = get_min_read_ptr();
            auto ap  = _mapped_state->alloc_ptr.load(std::memory_order_relaxed);
            auto ep  = _mapped_state->end_ptr.load(std::memory_order_relaxed);
            if (min - ap <= 1 and (ep - ap) < 3)
            {
               auto seg = get_new_segment();
               munlock(seg.second, segment_size);
               madvise(seg.second, segment_size, MADV_RANDOM);
               seg.second->_alloc_pos.store(0, std::memory_order_relaxed);
               seg.second->_age = -1;

               _mapped_state->seg_meta[seg.first].clear();
               _mapped_state->free_seg_buffer[_mapped_state->end_ptr.load(std::memory_order_relaxed) &
                                        (max_segment_count - 1)] = seg.first;
               auto prev = _mapped_state->end_ptr.fetch_add(1, std::memory_order_release);
               set_session_end_ptrs(prev);
            }
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(100ms);
         }
         promote_rcache_data();
      }
      ARBTRIE_WARN("compact_loop end: ", _done.load());
      ARBTRIE_WARN("compact_loop done");
      _compactor_session.reset();
   }
#endif

   /**
    * For each session, pop up to 1024 objects from the read cache 
    * and promote them to the active segment
    * 
    * All queues are empty when this method returns false.
    * 
    * @return true if there may be more work to do, false otherwise
    */
   bool seg_allocator::compactor_promote_rcache_data(seg_alloc_session& ses)
   {
      bool       more_work = false;
      id_address read_ids[1024];

      // iterate over all sessions and not just the allocated ones
      // to avoid the race condition where a session is deallocated
      // before the queue is processed. The queue belongs to the
      // seg_allocator, not the session.
      for (uint32_t snum = 0; snum < _mapped_state->_session_data.session_capacity(); ++snum)
      {
         auto& rcache = _mapped_state->_session_data.rcache_queue(snum);
         // only process up to 1024 objects so that we can yield to
         // other tasks the compactor is doing without too much delay
         auto num_loaded = rcache.pop(read_ids, 1024);

         more_work |= rcache.usage() > 0;

         for (uint32_t i = 0; i < num_loaded; ++i)
         {
            auto state   = ses.lock();
            auto addr    = read_ids[i];
            auto obj_ref = state.get(addr);
            if (auto [header, loc] = obj_ref.try_move_header(); header)
            {
               auto [new_loc, new_header] = ses.alloc_data(header->size(), header->address_seq());
               memcpy(new_header, header, header->size());

               if constexpr (update_checksum_on_compact)
               {
                  if (not new_header->has_checksum())
                     new_header->update_checksum();
               }

               if (node_meta_type::success == obj_ref.try_move(loc, new_loc))
               {
                  _mapped_state->total_promoted_bytes.fetch_add(header->size(),
                                                                std::memory_order_relaxed);
                  _mapped_state->_segment_data.meta[loc.segment()].free_object(header->size());
               }
            }
            obj_ref.meta().end_pending_cache();
         }
      }
      return more_work;
   }

   /**
    * We want to compact pinned segments that have enough free space that
    * it is worth the cost of copying the non-empty data to a new segment. 
    * 
    * This data is in RAM and therefore not causing wear on the SSD. The
    * copy will cause contention on the node_meta objects as they move, and
    * will consume some memory bandwidth. 
    * 
    * Overall performance is based upon the % of data in memory and limiting
    * the number of SSD IO operations.  
    */
   bool seg_allocator::compact_pinned_segment(seg_alloc_session& ses)
   {
      auto total_segs = _block_alloc.num_blocks();

      int total_qualifying = 0;
      // Define N as the maximum number of top segments to track
      constexpr int                                     N = 16;  // Number of top segments to track
      std::array<std::pair<segment_number, int64_t>, N> qualifying_segments;

      for (int i = 0; i < total_segs; ++i)
      {
         auto& sd = _mapped_state->_segment_data.meta[i];
         auto  sm = sd.data();
         if (not sm.is_pinned or sm.is_alloc)
            continue;
         if (sm.free_space < segment_size / 8)
            continue;

         int64_t vage = sd.get_vage();

         // Check if this segment should be included (if array isn't full yet or if the vage is
         // higher than the lowest one we have)
         total_qualifying = insert_sorted_pair(qualifying_segments, total_qualifying, {i, vage});
      }
      if (total_qualifying < 8)
         return false;

      //      ARBTRIE_ERROR("compact_pinned_segments: ", total_qualifying);
      for (int i = 0; i < total_qualifying; ++i)
         compact_segment(ses, qualifying_segments[i].first);
      return total_qualifying != N;
   }

   bool seg_allocator::compact_unpinned_segment(seg_alloc_session& ses)
   {
      auto total_segs       = _block_alloc.num_blocks();
      int  total_qualifying = 0;

      // Define N as the maximum number of top segments to track
      constexpr int                                     N = 4;  // Number of top segments to track
      std::array<std::pair<segment_number, int64_t>, N> qualifying_segments;

      for (int i = 0; i < total_segs; ++i)
      {
         auto& sd = _mapped_state->_segment_data.meta[i];
         auto  sm = sd.data();
         if (sm.is_pinned or sm.is_alloc or sm.free_space < segment_size / 2)
            continue;

         int64_t vage = sd.get_vage();

         // Check if this segment should be included (if array isn't full yet or if the vage is
         // higher than the lowest one we have)
         total_qualifying = insert_sorted_pair(qualifying_segments, total_qualifying, {i, vage});
      }

      if (total_qualifying < 2)
         return false;

      ARBTRIE_WARN("compact_unpinned_segments: ", total_qualifying);

      // compact them in order into a new session
      auto unpinned_session = start_session();
      unpinned_session.set_alloc_to_pinned(false);

      // configure the session to not allocate from the pinned segments
      for (int i = 0; i < total_qualifying; ++i)
         compact_segment(ses, qualifying_segments[i].first);

      return total_qualifying != N;
   }

   void seg_allocator::compact_segment(seg_alloc_session& ses, uint64_t seg_num)
   {
      //      ARBTRIE_WARN("compact_segment: ", seg_num);
      auto        state = ses.lock();
      const auto* s     = get_segment(seg_num);
      //  if( not s->_write_lock.try_lock() ) {
      //     ARBTRIE_WARN( "unable to get write lock while compacting!" );
      //     abort();
      //  }
      const auto* shead = (const mapped_memory::segment_header*)s;
      const auto* send  = (const node_header*)((char*)s + segment_size);
      const char* foc =
          (const char*)s + round_up_multiple<64>(sizeof(mapped_memory::segment_header));
      const object_header* foo = (const object_header*)(foc);

      auto& smeta    = _mapped_state->_segment_data.meta[seg_num];
      auto  src_vage = smeta.get_vage();

      // Track destination segment info for logging
      uint64_t       dest_initial_vage = 0;
      segment_number current_dest_seg  = -1ull;

      if (ses._alloc_seg_ptr)
      {
         dest_initial_vage = ses._alloc_seg_ptr->_vage_accumulator.average_age();
         current_dest_seg  = ses._alloc_seg_num;
      }

      if (debug_memory)
      {
         if (not ses._alloc_seg_ptr)
            ARBTRIE_WARN("compact_segment: no destination segment allocated yet");
         else
         {
            auto alloc_pos = ses._alloc_seg_ptr->_alloc_pos.load(std::memory_order_relaxed);
            ARBTRIE_DEBUG("compacting segment: ", seg_num, " into ", ses._alloc_seg_num, " ",
                          _mapped_state->_segment_data.meta[seg_num].data().free_space);
            ARBTRIE_DEBUG("calloc: ", alloc_pos,
                          " cfree: ", _mapped_state->_segment_data.meta[seg_num].data().free_space);
         }
         if (s->_alloc_pos != segment_offset(-1))
         {
            ARBTRIE_WARN("compact_segment: segment ", seg_num,
                         " has alloc_pos: ", s->_alloc_pos.load());
         }
         assert(s->_alloc_pos == segment_offset(-1));
      }

      //   std::cerr << "seg " << seg_num <<" alloc pos: " << s->_alloc_pos <<"\n";

      auto seg_state = seg_num * segment_size;

      auto start_seg_ptr = ses._alloc_seg_ptr;
      auto start_seg_num = ses._alloc_seg_num;

      std::vector<std::pair<const object_header*, temp_meta_type>> skipped;
      std::vector<const object_header*>                            skipped_ref;
      std::vector<const object_header*>                            skipped_try_start;

      while (foo < send and foo->address())
      {
         assert(intptr_t(foo) % 64 == 0);

         if constexpr (update_checksum_on_modify)
            assert(foo->validate_checksum());

         auto foo_address = foo->address();
         // skip anything that has been freed
         // note the ref can go to 0 before foo->check is set to -1
         auto obj_ref = state.get(foo_address);
         if (obj_ref.ref() == 0)
         {
            if constexpr (debug_memory)
               skipped_ref.push_back(foo);
            foo = foo->next();
            continue;
         }

         // skip anything that isn't pointing
         // to foo, it may have been moved *or*
         // it may have been freed and the id reallocated to
         // another object. We cannot replace this with obj_ref.obj() == foo
         // because obj_ref could be pointing to an ID in the free list
         auto foo_idx     = (char*)foo - (char*)s;
         auto current_loc = obj_ref.loc();
         if (current_loc.to_abs() != seg_num * segment_size + foo_idx)
         {
            if constexpr (debug_memory)
               skipped.push_back({foo, obj_ref.meta_data()});
            foo = foo->next();
            continue;
         }

         auto obj_size    = foo->size();
         auto [loc, head] = ses.alloc_data(obj_size, foo->address_seq(), src_vage);
         if constexpr (debug_memory)
         {
            if (start_seg_num != ses._alloc_seg_num)
            {
               auto alloc_pos = ses._alloc_seg_ptr->_alloc_pos.load(std::memory_order_relaxed);
               ARBTRIE_DEBUG("compacting segment: ", seg_num, " into ", ses._alloc_seg_num, " ",
                             _mapped_state->_segment_data.meta[seg_num].data().free_space);
               ARBTRIE_DEBUG("calloc: ", alloc_pos, " cfree: ",
                             _mapped_state->_segment_data.meta[seg_num].data().free_space);
            }
         }

         if (obj_ref.try_start_move(obj_ref.loc())) [[likely]]
         {
            if (obj_ref.type() == node_type::binary)
            {
               copy_binary_node((binary_node*)head, (const binary_node*)foo);
            }
            else
            {
               memcpy(head, foo, obj_size);
            }
            if constexpr (update_checksum_on_compact)
            {
               if (not head->has_checksum())
                  head->update_checksum();
            }
            if constexpr (validate_checksum_on_compact)
            {
               if constexpr (update_checksum_on_modify)
               {
                  if (not head->has_checksum())
                     ARBTRIE_WARN("missing checksum detected: ", foo_address,
                                  " type: ", node_type_names[head->_ntype]);
               }
               if (not head->validate_checksum())
               {
                  ARBTRIE_WARN("invalid checksum detected: ", foo_address,
                               " checksum: ", to_hex(foo->checksum),
                               " != ", to_hex(foo->calculate_checksum()),
                               " type: ", node_type_names[head->_ntype]);
               }
            }
            if (node_meta_type::success != obj_ref.try_move(obj_ref.loc(), loc))
               ses.unalloc(obj_size);
         }
         else
         {
            if constexpr (debug_memory)
               skipped_try_start.push_back(foo);
         }

         // if ses.alloc_data() was forced to make space in a new segment
         // then we need to sync() the old write segment before moving forward
         if (not start_seg_ptr)
         {
            start_seg_ptr = ses._alloc_seg_ptr;
            start_seg_num = ses._alloc_seg_num;
         }
         else if (start_seg_ptr != ses._alloc_seg_ptr)
         {
            // Log summary when write segment fills up
            uint64_t dest_final_vage = start_seg_ptr->_vage_accumulator.average_age();
            int32_t  vage_delta      = dest_final_vage - src_vage;
            ARBTRIE_INFO("Compaction segment filled: src=", seg_num, " src_vage=", src_vage,
                         " dest=", start_seg_num, " dest_initial_vage=", dest_initial_vage,
                         " dest_final_vage=", dest_final_vage, " delta=", vage_delta);

            sync_segment(start_seg_num, sync_type::sync);
            start_seg_ptr = ses._alloc_seg_ptr;
            start_seg_num = ses._alloc_seg_num;

            // Update tracking for new destination segment
            dest_initial_vage = start_seg_ptr->_vage_accumulator.average_age();
            current_dest_seg  = start_seg_num;
         }
         foo = foo->next();
      }  // segment object iteration loop

      if constexpr (debug_memory)
      {
         if ((char*)send - (char*)foo > 4096)
         {
            ARBTRIE_WARN("existing compact loop earlier than expected: ", (char*)send - (char*)foo);
         }

         foo = (node_header*)(foc);
         while (foo < send and foo->address())
         {
            auto obj_ref     = state.get(foo->address());
            auto foo_idx     = (char*)foo - (char*)s;
            auto current_loc = obj_ref.loc();
            if (current_loc.to_abs() == seg_num * segment_size + foo_idx)
            {
               for (auto s : skipped)
               {
                  if (s.first == foo)
                  {
                     ARBTRIE_WARN("obj_ref: ", obj_ref.ref());
                     ARBTRIE_WARN("obj_type: ", node_type_names[obj_ref.type()]);
                     ARBTRIE_WARN("obj_loc: ", current_loc.abs_index(),
                                  " seg: ", current_loc.segment());
                     ARBTRIE_WARN("ptr: ", (void*)foo);
                     ARBTRIE_WARN("pos in segment: ", segment_size - ((char*)send - (char*)foo));

                     ARBTRIE_WARN("SKIPPED BECAUSE POS DIDN'T MATCH");
                     ARBTRIE_DEBUG("  old meta: ", s.second.to_int());
                     ARBTRIE_DEBUG("  null_node: ", null_node.abs_index(),
                                   " seg: ", null_node.segment());
                     ARBTRIE_DEBUG("  old loc: ", s.second.loc().abs_index(),
                                   " seg: ", s.second.loc().segment());
                     ARBTRIE_DEBUG("  old ref: ", s.second.ref());
                     ARBTRIE_DEBUG("  old type: ", node_type_names[s.second.type()]);
                     ARBTRIE_DEBUG("  old is_con: ", s.second.is_const());
                     ARBTRIE_DEBUG("  old is_ch: ", s.second.is_copying());
                     assert(current_loc.to_abs() != seg_num * segment_size + foo_idx);
                  }
               }
               for (auto s : skipped_ref)
               {
                  if (s == foo)
                  {
                     ARBTRIE_WARN("SKIPPED BECAUSE REF 0");
                  }
               }
               for (auto s : skipped_try_start)
               {
                  if (s == foo)
                  {
                     ARBTRIE_WARN("SKIPPED BECAUSE TRY START");
                  }
               }
            }
            foo = foo->next();
         }
      }

      // in order to maintain the invariant that the segment we just cleared
      // can be reused, we must make sure that the data we moved out has persisted to
      // disk.
      if (start_seg_ptr)
      {
         // Log summary when segment finishes compacting
         uint64_t dest_final_vage = start_seg_ptr->_vage_accumulator.average_age();
         int32_t  vage_delta      = dest_final_vage - src_vage;
         ARBTRIE_INFO("Compaction complete: src=", seg_num, " src_vage=", src_vage,
                      " dest=", start_seg_num, " dest_initial_vage=", dest_initial_vage,
                      " dest_final_vage=", dest_final_vage, " delta=", vage_delta);

         // TODO: don't hardcode MS_SYNC here, this will cause unnessary SSD wear on
         //       systems that opt not to flush
         //
         //       In theory, this should only be done with segments that were
         //       previously msync.
         if (-1 == msync(start_seg_ptr, start_seg_ptr->_alloc_pos, MS_SYNC))
         {
            ARBTRIE_WARN("msync error: ", strerror(errno));
         }
         /**
          * before any sync can occur we must grab the sync lock which will
          * block until all modifications on the segment have completed and
          * then prevent new modifications until after sync is complete.
          *
          * There is no need for a global sync lock if each segment has its
          * own sync lock!
          */
         _mapped_state->_segment_data.meta[seg_num].set_last_sync_pos(
             start_seg_ptr->get_alloc_pos());
      }

      //   s->_write_lock.unlock();
      //   s->_num_objects = 0;
      //
      // TODO: this segment is read only, don't modify it!
      const_cast<mapped_memory::segment_header*>(s)->_alloc_pos.store(0, std::memory_order_relaxed);
      const_cast<mapped_memory::segment_header*>(s)->_age = -1;
      // the segment we just cleared, so its free space and objects get reset to 0
      // and its last_sync pos gets put to the end because there is no need to sync it
      // because its data has already been synced by the compactor

      // Reset appropriate state fields while preserving pinned state and other flags
      _mapped_state->_segment_data.meta[seg_num].finalize_compaction();

      // only one thread can move the end_ptr or this will break
      // std::cerr<<"done freeing end_ptr: " << _mapped_state->end_ptr.load() <<" <== " << seg_num <<"\n";

      assert(seg_num != segment_number(-1));
      //   ARBTRIE_DEBUG("pushing recycled segment: ", seg_num);
      _mapped_state->_read_lock_queue.push_recycled_segment(seg_num);
   }

   void seg_allocator::sync_segment(int s, sync_type st) noexcept
   {
      auto seg = get_segment(s);
      // TODO BUG: when syncing we must sync to the end of a page,
      // but start the next sync from the beginning of the page
      // since we only store the last paged synced... we no longer
      // know if the alloc_pos was in the middle of that page and
      // therefore it may be dirty again. We may need to
      // subtract 1 page from the last sync pos (assuming it doesn't go neg)
      // and sync the page before.
      //
      // If we store last_sync_pos as the rounded down position, then
      // getting this will work!
      auto last_sync  = _mapped_state->_segment_data.meta[s].get_last_sync_pos();
      auto last_alloc = seg->get_alloc_pos();

      if (last_alloc > segment_size)
         last_alloc = segment_size;

      if (last_alloc > last_sync)
      {
         auto sync_bytes   = last_alloc - (last_sync & page_size_mask);
         auto seg_sync_ptr = (((intptr_t)seg + last_sync) & page_size_mask);

         static uint64_t total_synced = 0;
         if (-1 == msync((char*)seg_sync_ptr, sync_bytes, msync_flag(st)))
         {
            ARBTRIE_WARN("msync error: ", strerror(errno), " ps: ", getpagesize(),
                         " len: ", sync_bytes);
         }
         else
         {
            total_synced += sync_bytes;
            //           ARBTRIE_DEBUG( "total synced: ", add_comma(total_synced), " flag: ", msync_flag(st), " MS_SYNC: ", MS_SYNC );
         }
         _mapped_state->_segment_data.meta[s].set_last_sync_pos(last_alloc);
      }
   }
   void seg_allocator::sync(sync_type st)
   {
      if (st == sync_type::none)
         return;

      std::unique_lock lock(_sync_mutex);

      auto ndsi = get_last_dirty_seg_idx();
      while (_last_synced_index < ndsi)
      {
         auto lsi = _last_synced_index % max_segment_count;
         _seg_sync_locks[lsi].start_sync();
         sync_segment(_dirty_segs[ndsi % max_segment_count], st);
         _seg_sync_locks[lsi].end_sync();
         ++_last_synced_index;
      }
   }

   seg_alloc_dump seg_allocator::dump()
   {
      seg_alloc_dump result;

      auto total_segs       = _block_alloc.num_blocks();
      result.total_segments = total_segs;

      // Get count of segments in the mlock_segments bitmap
      result.mlocked_segments_count = _mapped_state->_segment_provider.mlock_segments.count();

      // Gather segment information
      for (uint32_t i = 0; i < total_segs; ++i)
      {
         auto  seg        = get_segment(i);
         auto& meta       = _mapped_state->_segment_data.meta[i];
         auto  space_objs = meta.data();

         // Get stats directly as a stats_result struct
         auto stats = calculate_segment_read_stats(i);

         seg_alloc_dump::segment_info seg_info;
         seg_info.segment_num   = i;
         seg_info.freed_percent = int(100 * double(space_objs.free_space) / segment_size);
         seg_info.freed_bytes   = space_objs.free_space;
         seg_info.freed_objects = 0;
         seg_info.alloc_pos     = (seg->_alloc_pos == -1 ? -1 : seg->get_alloc_pos());
         seg_info.is_alloc      = space_objs.is_alloc;
         seg_info.is_pinned     = space_objs.is_pinned;
         seg_info.bitmap_pinned = _mapped_state->_segment_provider.mlock_segments.test(i);
         seg_info.age           = seg->_age;
         seg_info.read_nodes    = stats.nodes_with_read_bit;
         seg_info.read_bytes    = stats.total_bytes;
         seg_info.vage          = meta.vage;
         seg_info.total_objects = stats.total_objects;

         result.segments.push_back(seg_info);
         result.total_free_space += space_objs.free_space;
         result.total_read_nodes += stats.nodes_with_read_bit;
         result.total_read_bytes += stats.total_bytes;
      }

      // Gather session information
      result.active_sessions = _mapped_state->_session_data.active_session_count();

      /*
      auto fs = _mapped_state->_session_data.free_session_bitmap();
      for (uint32_t i = 0; i < max_session_count; ++i)
      {
         if (fs & (1ull << i))
         {
            seg_alloc_dump::session_info session;
            session.session_num = i;
            auto p              = _mapped_state->_read_lock_queue.session_lock_ptr(i);
            session.is_locked   = (uint32_t(p) != uint32_t(-1));
            session.read_ptr    = uint32_t(p);
            result.sessions.push_back(session);
         }
      }
      */

      // Gather pending segments information
      /*
      for (auto x = _mapped_state->alloc_ptr.load(); x < _mapped_state->end_ptr.load(); ++x)
      {
         seg_alloc_dump::pending_segment pending;
         pending.index       = x;
         pending.segment_num = _mapped_state->free_seg_buffer[x & (max_segment_count - 1)];
         result.pending_segments.push_back(pending);
      }

      result.alloc_ptr          = _mapped_state->alloc_ptr.load();
      result.end_ptr            = _mapped_state->end_ptr.load();
      result.free_release_count = _id_alloc.free_release_count();
      */

      return result;
   }

   seg_allocator::stats_result seg_allocator::calculate_segment_read_stats(segment_number seg_num)
   {
      uint32_t nodes_with_read_bit = 0;
      uint64_t total_bytes         = 0;
      uint32_t total_objects       = 0;  // Added counter for all objects

      auto seg  = get_segment(seg_num);
      auto send = (const node_header*)((char*)seg + segment_size);

      if (seg->get_alloc_pos() != -1)
         send = (const node_header*)(((char*)seg) + seg->get_alloc_pos());

      const char* foc =
          (const char*)seg + round_up_multiple<64>(sizeof(mapped_memory::segment_header));
      const node_header* foo = (const node_header*)(foc);

      while (foo < send && foo->address())
      {
         // Get the object reference for this node
         auto  foo_address = foo->address();
         auto& obj_ref     = _id_alloc.get(foo_address);
         auto  current_loc = obj_ref.loc();
         auto  foo_idx     = (char*)foo - (char*)seg;

         // Only count if the object reference is pointing to this exact node
         if (current_loc.to_abs() != seg_num * segment_size + foo_idx)
         {
            foo = foo->next();
            continue;
         }
         // Count all objects
         total_objects++;

         // Check if the read bit is set and if the location matches
         if (obj_ref.is_read())
         {
            nodes_with_read_bit++;
            total_bytes += foo->size();
         }

         foo = foo->next();
      }

      return {nodes_with_read_bit, total_bytes, total_objects};
   }

   //-----------------------------------------------------------------------------
   // Segment Provider Thread Methods
   //-----------------------------------------------------------------------------

   /**
    * Process segments that have been recycled and are available to be reused
    * This is called before allocating a new segment in the provider loop
    */
   void seg_allocator::provider_process_recycled_segments()
   {
      auto& provider_state = _mapped_state->_segment_provider;

      // Process all available recycled segments
      while (auto available = _mapped_state->_read_lock_queue.available_to_pop())
      {
         segment_number segs[available];
         int popped = _mapped_state->_read_lock_queue.pop_recycled_segments(segs, available);
         // Set the recycled segment in the free_segments bitset
         for (int i = 0; i < popped; ++i)
            provider_state.free_segments.set(segs[i]);

         ARBTRIE_DEBUG("segment_provider: Recycled segments: ", popped,
                       " added to free_segments, count: ", provider_state.free_segments.count());
      }
   }

   std::optional<segment_number> seg_allocator::find_first_free_and_pinned_segment()
   {
      auto& provider_state = _mapped_state->_segment_provider;
      for (auto seg : provider_state.free_segments)
      {
         if (provider_state.mlock_segments.test(seg))
            return seg;
      }
      return std::nullopt;
   }

   void seg_allocator::provider_loop(segment_thread& thread)
   {
      // Thread name should already be set by segment_thread

      auto& provider_state = _mapped_state->_segment_provider;

      ARBTRIE_DEBUG("segment_provider: Starting provider thread");

      // Clear any interrupt flag from previous runs to ensure we start clean
      // This is important because the flag is stored in shared memory
      // and might be left set from a previous crash or restart
      provider_state.ready_segments.clear_interrupt();

      // Set a reasonable timeout that allows for regular heartbeat checks
      const std::chrono::milliseconds wait_timeout(50);

      int iteration_count = 0;
      while (thread.yield(std::chrono::milliseconds(0)))
      {
         iteration_count++;

         // Handle excess mlocked segments
         provider_munlock_excess_segments();

         // Process segments acknowledged by consumers
         provider_process_acknowledged_segments();

         // Process segments that have been recycled and are available to be reused
         provider_process_recycled_segments();

         // If we can't push, sleep briefly and continue to next iteration
         if (!provider_state.ready_segments.can_push())
            continue;

         // prioritize segments that are free and pinned
         std::optional<segment_number> free_pinned_seg = find_first_free_and_pinned_segment();

         segment_number seg_num;
         // Get a segment ready
         if (not free_pinned_seg)
            seg_num = provider_state.free_segments.unset_first_set();
         else
         {
            seg_num = *free_pinned_seg;
            provider_state.free_segments.reset(seg_num);
         }

         // Handle segment source appropriately
         if (seg_num == provider_state.free_segments.invalid_index)
         {
            seg_num = provider_allocate_new_segment();
            ARBTRIE_ERROR("segment_provider: Allocated new segment: ", seg_num);
         }
         else
         {
            ARBTRIE_INFO("segment_provider: Reused segment: ", seg_num);
         }

         // Prepare the segment for use
         provider_prepare_segment(seg_num);

         // Push the segment to the ready queue (this will always succeed)
         int64_t result;
         if (free_pinned_seg)
            result = provider_state.ready_segments.push_front(seg_num);
         else
            result = provider_state.ready_segments.push(seg_num);

         ARBTRIE_DEBUG("segment_provider: Pushed segment ", seg_num,
                       " to ready_segments, result: ", result,
                       ", new usage: ", provider_state.ready_segments.usage(), "/",
                       provider_state.ready_segments.capacity());

         // This should never happen since we checked can_push() first
         assert(result >= 0);
      }
      ARBTRIE_WARN("segment_provider: Exiting provider loop");
   }

   // called when a segment is being prepared for use by the provider thread
   void seg_allocator::disable_segment_write_protection(segment_number seg_num)
   {
      auto seg = get_segment(seg_num);
      // Make the segment read-write accessible
      if (mprotect(seg, segment_size, PROT_READ | PROT_WRITE) != 0) [[unlikely]]
         ARBTRIE_WARN("mprotect error(", errno, ") ", strerror(errno));
   }

   // called on msync to protect the memory ranges that have been synced to disk
   // called when a segment is demoted from mlock
   void seg_allocator::enable_segment_write_protection(segment_number seg_num)
   {
      auto seg = get_segment(seg_num);
      // Make the segment read-write accessible
      if (mprotect(seg, segment_size, PROT_READ) != 0) [[unlikely]]
         ARBTRIE_WARN("mprotect error(", errno, ") ", strerror(errno));
   }

   segment_number seg_allocator::provider_allocate_new_segment()
   {
      segment_number result = _block_alloc.alloc();
      return result;
   }

   void seg_allocator::provider_prepare_segment(segment_number seg_num)
   {
      auto sp   = _block_alloc.get(seg_num);
      auto shp  = new (sp) mapped_memory::segment_header();
      shp->_age = _mapped_state->next_alloc_age.fetch_add(1, std::memory_order_relaxed);

      // utilized by compactor to propagate the relative age of data in a segment
      // for the purposes of munlock the oldest data first and avoiding promoting data
      // just because we compacted a segment to save space.
      uint64_t now_ms = arbtrie::get_current_time_ms();
      shp->_vage_accumulator.reset(now_ms);

      // Update the virtual age in segment metadata to match the segment header's initial value
      _mapped_state->_segment_data.meta[seg_num].set_vage(now_ms);

      disable_segment_write_protection(seg_num);
   }

   /**
    * Unlocks excess segments from memory when we exceed the maximum mlocked limit
    */
   void seg_allocator::provider_munlock_excess_segments()
   {
      auto& provider_state = _mapped_state->_segment_provider;

      int mlocked_count = provider_state.mlock_segments.count();
      int max_count     = provider_state.max_mlocked_segments;

      if (mlocked_count <= max_count)
         return;

      // most recent are higher numbers, so we want the lowest number
      // as it will be the oldest segment
      uint64_t       oldest_age = -1;
      segment_number oldest_seg = -1;
      for (auto seg : provider_state.mlock_segments)
      {
         uint64_t vage = _mapped_state->_segment_data.meta[seg].get_vage();
         if (vage < oldest_age and vage != 0)
         {
            oldest_age = vage;
            oldest_seg = seg;
         }
      }

      if (oldest_age == -1)
         return;

      void* seg_ptr = get_segment(oldest_seg);
      if (munlock(seg_ptr, segment_size) != 0) [[unlikely]]
      {
         ARBTRIE_ERROR("munlock error(", errno, ") ", strerror(errno));
      }
      else
      {
         // Clear both the bitmap and the meta bit using the helper
         update_segment_pinned_state(oldest_seg, false);
         ARBTRIE_WARN("munlocked segment: ", oldest_seg,
                      " count: ", provider_state.mlock_segments.count(),
                      " vage abs: ", _mapped_state->_segment_data.meta[oldest_seg].get_vage(),
                      "     rel age ms: ",
                      arbtrie::get_current_time_ms() -
                          _mapped_state->_segment_data.meta[oldest_seg].get_vage());
      }

      // now that is it is no longer pinned, we are unlikely to
      // access it with high degree of locality because we jump from
      // node to node as we traverse the tree and each node fits on
      // a page.
      // TODO: make some effort to prevent nodes from spanning pages
      // as they are allocated. This will waste some space, but will
      // improve disk cache performance.
      if (madvise(seg_ptr, segment_size, MADV_RANDOM) != 0) [[unlikely]]
         ARBTRIE_WARN("madvise error(", errno, ") ", strerror(errno));
   }

   /**
    * Process segments that have been acknowledged by consumers via pop_ack()
    * and lock them in memory if under the maximum limit
    */
   void seg_allocator::provider_process_acknowledged_segments()
   {
      auto& provider_state = _mapped_state->_segment_provider;

      int processed_count = 0;
      while (auto seg_ack = provider_state.ready_segments.pop_ack())
      {
         processed_count++;

         ARBTRIE_INFO("mlock segment: ", *seg_ack,
                      " current mlock state: ", provider_state.mlock_segments.test(*seg_ack));
         if (provider_state.mlock_segments.test(*seg_ack))
         {
            continue;
         }
         void* seg_ptr = get_segment(*seg_ack);
         if (mlock(seg_ptr, segment_size) == 0)
         {
            ARBTRIE_WARN("mlocked segment: ", *seg_ack,
                         " count: ", provider_state.mlock_segments.count(),
                         " vage abs: ", _mapped_state->_segment_data.meta[*seg_ack].get_vage(),
                         "     rel age ms: ",
                         arbtrie::get_current_time_ms() -
                             _mapped_state->_segment_data.meta[*seg_ack].get_vage());
            // Set both the bitmap and the meta bit using the helper
            update_segment_pinned_state(*seg_ack, true);

            // munlock the oldest mlocked segment(s) if needed
            provider_munlock_excess_segments();
         }
         else
         {
            ARBTRIE_WARN("mlock error(", errno, ") ", strerror(errno));
            if (madvise(seg_ptr, segment_size, MADV_RANDOM) != 0)
            {
               ARBTRIE_WARN("madvice error(", errno, ") ", strerror(errno));
            }
         }
      }
      // if (processed_count > 0)
      //    ARBTRIE_WARN("processed ", processed_count, " acknowledged segments");
   }

   bool seg_allocator::stop_background_threads()
   {
      bool any_stopped = false;

      if (_read_bit_decay_thread && _read_bit_decay_thread->is_running())
      {
         _read_bit_decay_thread->stop();
         any_stopped = true;
      }

      if (_compactor_thread && _compactor_thread->is_running())
      {
         _compactor_thread->stop();
         any_stopped = true;
      }

      if (_segment_provider_thread && _segment_provider_thread->is_running())
      {
         // Wake up any threads that might be waiting in the ready_segments buffer
         _mapped_state->_segment_provider.ready_segments.wake_blocked();
         _segment_provider_thread->stop();
         any_stopped = true;
      }

      return any_stopped;
   }

   bool seg_allocator::start_background_threads(bool force_start)
   {
      bool any_started = false;

      // Only start threads if force_start is true or threads were running before
      if (!force_start && !_compactor_thread && !_read_bit_decay_thread &&
          !_segment_provider_thread)
         return false;

      // Start all background threads that were created
      if (_read_bit_decay_thread)
      {
         if (_read_bit_decay_thread->start())
            any_started = true;
      }

      if (_compactor_thread)
      {
         if (_compactor_thread->start())
            any_started = true;
      }

      if (_segment_provider_thread)
      {
         if (_segment_provider_thread->start())
            any_started = true;
      }

      return any_started;
   }

   /**
    * Helper function to update the pinned state of a segment, ensuring both
    * the bitmap and the metadata are synchronized.
    *
    * @param seg_num The segment number to update
    * @param is_pinned Whether to set (true) or clear (false) the pinned state
    */
   void seg_allocator::update_segment_pinned_state(segment_number seg_num, bool is_pinned)
   {
      auto& provider_state = _mapped_state->_segment_provider;

      // Update the bitmap
      if (is_pinned)
         provider_state.mlock_segments.set(seg_num);
      else
         provider_state.mlock_segments.reset(seg_num);

      // Update the segment metadata
      _mapped_state->_segment_data.meta[seg_num].set_pinned(is_pinned);
   }
}  // namespace arbtrie
