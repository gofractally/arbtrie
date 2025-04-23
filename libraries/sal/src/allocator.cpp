#include <sys/mman.h>
#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <sal/allocator.hpp>
#include <sal/allocator_impl.hpp>
#include <sal/debug.hpp>
#include <sal/seg_alloc_dump.hpp>
#include <sal/smart_ptr.hpp>

namespace sal
{
   /**
       * These methods assign a unique number to each instance of allocator so
       * that the thread-local allocator_session can be associated with a specific
       * allocator instance.
       *  
       * @group allocator_index Allocator Index Methods
       */
   ///@{
   static std::atomic<uint64_t>& get_allocator_index()
   {
      static std::atomic<uint64_t> ai = 0;
      return ai;
   }
   static uint32_t alloc_allocator_index()
   {
      uint64_t prev = get_allocator_index().load();
      while (prev != ~0ull)
      {
         uint64_t idx = __builtin_ctzll(~prev);
         if (get_allocator_index().compare_exchange_weak(prev, prev | (1ull << idx)))
            return idx;
      }
      SAL_ERROR("too many sal::allocator instances, limit is 64");
      abort();
      return 64;
   }
   static void free_allocator_index(uint32_t idx)
   {
      uint64_t prev = get_allocator_index().load();
      assert(prev & (1ull << idx));
      (void)prev;  // release warning suppression
      get_allocator_index().fetch_sub(1ull << idx);
   }

   ///@}
   /** 
    * Insert a new element into a sorted array of pairs, maintaining sort order.
    * Uses binary search to find insertion point and rotation for efficient insertion.
    * 
    * @tparam N Size of the array
    * @tparam T Type of the first element in pair (segment number type)
    * @param arr The array to insert into
    * @param current_size Current number of elements in the array
    * @param new_value Pair to be inserted
    * @return true if the value was inserted, false if the array is full
    */
   template <size_t N, typename T>
   bool insert_sorted_pair(std::array<std::pair<T, int64_t>, N>& arr,
                           size_t&                               current_size,
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
         return true;
      }
      return false;
   }

   namespace mapped_memory
   {
      // Forward declarations
      class segment_header;

   }  // namespace mapped_memory
   static_assert(__cplusplus >= 202002L, "C++20 or later required");

   allocator::allocator(std::filesystem::path dir, runtime_config cfg)
       : _ptr_alloc(dir / "ptrs"),
         _block_alloc(dir / "segs", segment_size, max_segment_count),
         _seg_alloc_state_file(dir / "header", access_mode::read_write, true),
         _root_object_file(dir / "roots", access_mode::read_write, true)
   {
      _allocator_index = alloc_allocator_index();
      if (_seg_alloc_state_file.size() == 0)
      {
         _seg_alloc_state_file.resize(
             system_config::round_to_page(sizeof(mapped_memory::allocator_state)));
         new (_seg_alloc_state_file.data()) mapped_memory::allocator_state();
      }
      if (_root_object_file.size() == 0)
      {
         _root_object_file.resize(system_config::round_to_page(sizeof(root_object_array)));
         auto arr = new (_root_object_file.data()) root_object_array();
         for (auto& ro : *arr)
            ro = null_ptr_address;
      }
      _mapped_state =
          reinterpret_cast<mapped_memory::allocator_state*>(_seg_alloc_state_file.data());
      _root_objects = reinterpret_cast<root_object_array*>(_root_object_file.data());
      assert(_root_objects);
      _mapped_state->_config = cfg;

      mlock_pinned_segments();

      provider_populate_pinned_segments();
      provider_populate_unpinned_segments();
      start_background_threads();
   }

   /**
    *  On startup, mlock all segments that are marked as pinned at last shutdown.
    */
   void allocator::mlock_pinned_segments()
   {
      for (auto seg : _mapped_state->_segment_provider.mlock_segments)
      {
         segment_number seg_num(seg);
         auto*          segment_ptr = get_segment(seg_num);
         if (mlock(segment_ptr, segment_size) != 0)
         {
            SAL_WARN("mlock failed for segment: ", seg, " error: ", strerror(errno));

            // Clear both the bitmap and the meta bit using the helper
            update_segment_pinned_state(seg_num, false);
            return;
         }
         _mapped_state->_segment_data.set_pinned(seg_num, true);
      }
   }

   allocator::~allocator()
   {
      stop_background_threads();
      free_allocator_index(_allocator_index);
   }

   allocator_session_number allocator::alloc_session_num() noexcept
   {
      return _mapped_state->_session_data.alloc_session_num();
   }

   /**
    * Decays the read bits over time to enable effective
    * estimation of the frequency of reads on nodes, processing
    * regions at a rate that ensures all regions are processed within
    * the cache frequency window.
    *
    * @param thread Reference to the segment_thread running this function
    */
   void allocator::clear_read_bits_loop(segment_thread& thread)
   {
      // Set thread name for sal debug system
      sal::set_current_thread_name("read_bit_decay");

      using namespace std::chrono;
      using namespace std::chrono_literals;

      time_point start_time = high_resolution_clock::now();

      uint64_t last_index =
          _mapped_state->next_clear_read_bit_position.load(std::memory_order_relaxed);

      // Loop until thread should exit (yield returns false)
      while (thread.yield(milliseconds(10)))
      {
         time_point current_time      = high_resolution_clock::now();
         uint32_t   max_address_count = _ptr_alloc.current_max_address_count();
         uint32_t   window_time_sec   = _mapped_state->_config.read_cache_window_sec;
         double elapsed_seconds = std::chrono::duration<double>(current_time - start_time).count();

         double elapsed_round     = uint64_t(elapsed_seconds / window_time_sec);
         auto   left_over         = elapsed_seconds - (elapsed_round * window_time_sec);
         auto   left_over_percent = left_over / window_time_sec;

         // tells me where we should be in the address space.
         uint64_t index_percent = left_over_percent * max_address_count;

         // if we should be before the last index percent, then time wrapped around.
         // process to the end of the address space first.
         if (index_percent < last_index)
         {
            for (uint64_t i = last_index; i < max_address_count; ++i)
               _ptr_alloc.get(ptr_address(i % max_address_count)).clear_pending_cache();
            last_index = 0;
         }
         for (uint64_t i = last_index; i < index_percent; ++i)
            _ptr_alloc.get(ptr_address(i % max_address_count)).clear_pending_cache();

         last_index = index_percent;
         _mapped_state->next_clear_read_bit_position.store(last_index, std::memory_order_relaxed);
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
   void allocator::compactor_loop(segment_thread& thread)
   {
      // Set thread name for sal debug system
      sal::set_current_thread_name("compactor");

      auto ses = get_session();
      // compact them in order into a new session

      // TODO: move compaction of unpinned segments to a separate thread,
      // and then use a mutex to synchronize access to the recycle queue.
      //auto  unpinned_session  = create_session();
      auto& sesr = *ses;
      //auto& unpinned_sessionr = *unpinned_session;
      //unpinned_session->set_alloc_to_pinned(false);

      /** 
       *  The compactor always prioritizes cache, then ram, then
       *  all other segments each loop. It needs to keep data of similar
       *  age together to minimize movement of virtual age of objects.
       */
      while (thread.yield())
      {
         compactor_promote_rcache_data(sesr);
         compact_pinned_segment(sesr);
         compactor_promote_rcache_data(sesr);
         compact_unpinned_segment(sesr);
      }
   }

   /**
    * For each session, pop up to 1024 objects from the read cache 
    * and promote them to the active segment
    * 
    * All queues are empty when this method returns false.
    * 
    * @return true if there may be more work to do, false otherwise
    */
   bool allocator::compactor_promote_rcache_data(allocator_session& ses)
   {
      bool        more_work = false;
      ptr_address read_ids[1024];

      // iterate over all sessions and not just the allocated ones
      // to avoid the race condition where a session is deallocated
      // before the queue is processed. The queue belongs to the
      // allocator, not the session.
      for (uint32_t snum = 0; snum < _mapped_state->_session_data.session_capacity(); ++snum)
      {
         auto& rcache = _mapped_state->_session_data.rcache_queue(allocator_session_number(snum));
         // only process up to 1024 objects so that we can yield to
         // other tasks the compactor is doing without too much delay
         auto num_loaded = rcache.pop(read_ids, 1024);

         more_work |= rcache.usage() > 0;

         for (uint32_t i = 0; i < num_loaded; ++i)
         {
            auto state   = ses.lock();  // read only lock
            auto addr    = read_ids[i];
            auto obj_ref = ses.get_ref<alloc_header>(addr);
            // reads the relaxed cached load of the location
            auto start_loc = obj_ref.loc();

            // when the object is freed and reallocated, the pending cache
            // bit is cleared, so we need to make sure the bit is set before
            // we assume we are still referencing the same object.
            if (not obj_ref.control().try_end_pending_cache())
               continue;

            // before we even consider reading this pointer, we need to make
            // sure that the location is read-only....
            if (not is_read_only(start_loc))
               continue;

            assert(obj_ref->address() == addr);

            auto compact_size = vcall::compact_size(obj_ref.obj());

            // TODO: return a scoped lock with new_loc and new_header
            //the ses modify lock is held while modifying while allocating
            auto [new_loc, new_header] =
                ses.alloc_data<alloc_header>(compact_size, obj_ref->type(), obj_ref->address_seq());

            vcall::compact_to(obj_ref.obj(), new_header);

            if (config_update_checksum_on_compact())
               if (not vcall::has_checksum(new_header))
                  vcall::update_checksum(new_header);

            /// if the location hasn't changed then we are good to go
            ///     - and the objeect is still valid ref count
            if (obj_ref.control().cas_move(start_loc, new_loc))
            {
               _mapped_state->_cache_difficulty_state.compactor_promote_bytes(obj_ref->size());
               ses.record_freed_space(obj_ref.obj());
            }
            else
            {
               if (not ses.unalloc(compact_size))
                  ses.record_freed_space(new_header);
            }
         }
      }
      _mapped_state->_cache_difficulty_state.compactor_promote_bytes(0);
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
   bool allocator::compact_pinned_segment(allocator_session& ses)
   {
      auto total_segs = _block_alloc.num_blocks();

      size_t total_qualifying = 0;
      // Define N as the maximum number of top segments to track
      constexpr int                                     N = 16;  // Number of top segments to track
      std::array<std::pair<segment_number, int64_t>, N> qualifying_segments;

      uint32_t    potential_free_space = 0;
      const auto& seg_data             = _mapped_state->_segment_data;
      for (segment_number i{0}; i < total_segs; ++i)
      {
         if (not seg_data.may_compact(i) or not seg_data.is_pinned(i))
            continue;
         const auto freed_space = seg_data.get_freed_space(i);
         if (freed_space < _mapped_state->_config.compact_pinned_unused_threshold_mb * 1024 * 1024)
            continue;

         int64_t vage = seg_data.get_vage(i);

         // Check if this segment should be included (if array isn't full yet or if the vage is
         // higher than the lowest one we have)
         if (insert_sorted_pair(qualifying_segments, total_qualifying, {i, vage}))
            potential_free_space += freed_space;
         //     ARBTRIE_INFO("compact_pinned_segment: ", i, " freed_space: ", freed_space,
         //                  " potential_free_space: ", potential_free_space);
      }
      if (potential_free_space < segment_size)
      {
         usleep(1000);
         return false;
      }

      for (uint32_t i = 0; i < total_qualifying; ++i)
         compact_segment(ses, qualifying_segments[i].first);
      return total_qualifying != N;
   }

   bool allocator::compact_unpinned_segment(allocator_session& ses)
   {
      auto   total_segs       = _block_alloc.num_blocks();
      size_t total_qualifying = 0;

      // Define N as the maximum number of top segments to track
      constexpr int                                     N = 8;  // Number of top segments to track
      std::array<std::pair<segment_number, int64_t>, N> qualifying_segments;

      uint32_t    potential_free_space = 0;
      const auto& seg_data             = _mapped_state->_segment_data;
      for (segment_number i{0}; i < total_segs; ++i)
      {
         const auto freed_space = seg_data.get_freed_space(i);
         if (not seg_data.may_compact(i))
            continue;
         if (seg_data.is_pinned(i))
            continue;
         if (freed_space <
             _mapped_state->_config.compact_unpinned_unused_threshold_mb * 1024 * 1024)
            continue;

         int64_t vage = seg_data.get_vage(i);

         // Check if this segment should be included (if array isn't full yet or if the vage is
         // higher than the lowest one we have)
         if (insert_sorted_pair(qualifying_segments, total_qualifying, {i, vage}))
            potential_free_space += freed_space;
         //     ARBTRIE_INFO("compact_unpinned_segment: ", i, " freed_space: ", freed_space,
         //                  " potential_free_space: ", potential_free_space);
      }
      /*
      ARBTRIE_WARN("compact_unpinned_segment: ", potential_free_space,
                   " total_qualifying: ", total_qualifying, " not_read_only: ", not_read_only,
                   " pinned: ", pinned, " insufficient_space: ", insufficient_space);
                   */
      if (potential_free_space < segment_size)
      {
         usleep(1000);
         return false;
      }

      // configure the session to not allocate from the pinned segments
      for (uint32_t i = 0; i < total_qualifying; ++i)
         compact_segment(ses, qualifying_segments[i].first);

      return total_qualifying != N;
   }

   void allocator::compact_segment(allocator_session& ses, segment_number seg_num)
   {
      //      ARBTRIE_ERROR("compact_segment: ", seg_num);
      auto        state = ses.lock();
      const auto* s     = get_segment(seg_num);

      // if it is read only, then we don't have to worry about locking!
      assert(s->is_read_only() && "segment must be read only before compacting");

      // collect the range we will be iterating over
      const auto* shead = (const mapped_memory::segment*)s;
      const auto* send  = (const alloc_header*)shead->end();

      // cast the start to first object_header
      const alloc_header* foo = (const alloc_header*)(shead);

      // define a lambda to help copy nodes and facilitate early exit
      auto try_copy_node = [&](const alloc_header* nh, msec_timestamp vage)
      {
         // skip anything that has been freed
         auto obj_ref = ses.get_ref<alloc_header>(nh->address());
         if (obj_ref.ref() == 0)
            return;  // object was freed
         if (obj_ref.obj() != nh)
            return;  // object was moved;
         // assert / throw exception if the checksum is invalid,
         // stop the world, we may need to do data recovery.
         if (config_validate_checksum_on_compact())
         {
            if (vcall::has_checksum(nh) and not vcall::verify_checksum(nh))
               throw std::runtime_error("checksum is invalid");
         }

         /// acquires the modify lock on the segment, we must release it
         auto [loc, head] = ses.alloc_data_vage<alloc_header>(vcall::compact_size(nh), vage,
                                                              nh->type(), nh->address_seq());
         // update checksum if needed, because the user may
         // have used config::update_checksum_on_upsert = false
         // to get better user performance and offload the checksum
         // work to the compaction thread.
         if (config_update_checksum_on_compact())
            if (not vcall::has_checksum(head))
               vcall::update_checksum(head);

         if (not obj_ref.control().cas_move(obj_ref.loc(), loc))
            if (not ses.unalloc(head->size()))
               ses.record_freed_space(head);
      };  /// end try_copy_node lambda

      msec_timestamp src_vage(s->age_accumulator.average());
      while (foo < send)
      {
         if (foo->type() == header_type::sync_head)
            src_vage =
                msec_timestamp(*reinterpret_cast<const sync_header*>(foo)->timestamp() / 1000);
         else  // foo is a node_header
            try_copy_node(foo, src_vage);
         assert(foo != foo->next());
         foo = foo->next();
      }

      // TODO: now we need to mark everything thus far in the compacted segment as read only,
      // to prevent any accidential modifications. The source was read only, so the
      // destination should be as well.

      // calling sync here may be overkill and introduce extra padding in a segment,
      // but perhaps if we hold off until there are no more segments to compact and
      // then call sync and then push to the recycle queue we can save syncs and space.

      // a truly paranoid approach would be to write protect the copy, then validate,
      // and then sync... but not sure what that gains us.
      ses.sync(_mapped_state->_config.sync_mode, _mapped_state->_config);

      // ensures that the segment will not get selected for compaction again until
      // after it is reused by the provider thread.
      _mapped_state->_segment_data.added_to_read_lock_queue(seg_num);

      // only one thread can move the end_ptr or this will break
      // std::cerr<<"done freeing end_ptr: " << _mapped_state->end_ptr.load() <<" <== " << seg_num <<"\n";

      //   ARBTRIE_DEBUG("pushing recycled segment: ", seg_num);
      _mapped_state->_read_lock_queue.push_recycled_segment(seg_num);
   }

   // called when a segment is being prepared for use by the provider thread
   void allocator::disable_segment_write_protection(segment_number seg_num)
   {
      auto seg = get_segment(seg_num);
      // Make the segment read-write accessible
      if (mprotect(seg, segment_size, PROT_READ | PROT_WRITE) != 0) [[unlikely]]
         SAL_WARN("mprotect error(", errno, ") ", strerror(errno));

      // reset the first writable page to 0, so everyone knows it is safe
      seg->_first_writable_page = 0;
   }

   seg_alloc_dump allocator::dump() const
   {
      seg_alloc_dump result;

      auto total_segs       = _block_alloc.num_blocks();
      result.total_segments = total_segs;

      // Get count of segments in the mlock_segments bitmap
      result.mlocked_segments_count = _mapped_state->_segment_provider.mlock_segments.count();

      // Get cache difficulty and total promoted bytes
      result.cache_difficulty = _mapped_state->_cache_difficulty_state.get_cache_difficulty();
      result.total_promoted_bytes =
          _mapped_state->_cache_difficulty_state.total_promoted_bytes.load(
              std::memory_order_relaxed);

      // Initialize total non-value nodes counter
      result.total_non_value_nodes = 0;

      // Gather segment information
      for (segment_number i{0}; i < total_segs; ++i)
      {
         auto seg = get_segment(i);

         const auto&                  seg_data    = _mapped_state->_segment_data;
         const auto                   freed_space = seg_data.get_freed_space(i);
         seg_alloc_dump::segment_info seg_info;
         seg_info.segment_num   = i;
         seg_info.freed_percent = int(100 * double(freed_space) / segment_size);
         seg_info.freed_bytes   = freed_space;
         seg_info.freed_objects = 0;
         seg_info.alloc_pos     = seg->get_alloc_pos();
         seg_info.is_pinned     = seg_data.is_pinned(i);
         seg_info.is_read_only  = seg_data.is_read_only(i);
         seg_info.is_free       = _mapped_state->_segment_provider.free_segments.test(*i);
         seg_info.bitmap_pinned = _mapped_state->_segment_provider.mlock_segments.test(*i);
         seg_info.age =
             seg->_provider_sequence;  // TODO: rename seg_info.age to seg_info.provider_sequence
                                       /*
         seg_info.read_nodes    = stats.nodes_with_read_bit;
         seg_info.read_bytes    = stats.total_bytes;
         seg_info.vage          = seg_data.get_vage(i);
         seg_info.total_objects = stats.total_objects;
         */

         result.segments.push_back(seg_info);
         result.total_free_space += freed_space;
         //result.total_read_nodes += stats.nodes_with_read_bit;
         //result.total_read_bytes += stats.total_bytes;
      }

      // Gather session information
      result.active_sessions = _mapped_state->_session_data.active_session_count();

      //auto fs = _mapped_state->_session_data.free_session_bitmap();
      for (allocator_session_number i{0}; i < _mapped_state->_session_data.session_capacity(); ++i)
      {
         // Check if this bit is not free (0 bit means in use)
         //if ((fs & (1ull << i)) == 0)
         {
            seg_alloc_dump::session_info session;
            session.session_num   = i;
            auto&    session_lock = _mapped_state->_read_lock_queue.get_session_lock(i);
            uint64_t lock_value   = session_lock.load();
            session.is_locked     = (uint32_t(lock_value) != uint32_t(-1));
            session.read_ptr      = uint32_t(lock_value);

            // Get the total bytes written by this session
            session.total_bytes_written = _mapped_state->_session_data.total_bytes_written(i);

            // Only add sessions with more than 0 bytes written
            if (session.total_bytes_written > 0)
               result.sessions.push_back(session);
         }
      }

      /*
      // Gather pending segments information
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

   //-----------------------------------------------------------------------------
   // Segment Provider Thread Methods
   //-----------------------------------------------------------------------------

   /**
    * Process segments that have been recycled and are available to be reused
    * This is called before allocating a new segment in the provider loop
    */
   void allocator::provider_process_recycled_segments()
   {
      auto& provider_state = _mapped_state->_segment_provider;

      // Process all available recycled segments
      while (auto available = _mapped_state->_read_lock_queue.available_to_pop())
      {
         segment_number segs[available];
         int popped = _mapped_state->_read_lock_queue.pop_recycled_segments(segs, available);
         // Set the recycled segment in the free_segments bitset
         for (int i = 0; i < popped; ++i)
         {
            provider_state.free_segments.set(*segs[i]);
            _mapped_state->_segment_data.added_to_free_segments(segs[i]);
         }
      }
   }

   std::optional<segment_number> allocator::find_first_free_and_pinned_segment()
   {
      auto& provider_state = _mapped_state->_segment_provider;
      for (auto seg : provider_state.free_segments)
      {
         if (provider_state.mlock_segments.test(seg))
            return segment_number(seg);
      }
      return std::nullopt;
   }

   void allocator::provider_loop(segment_thread& thread)
   {
      // Set thread name for sal debug system
      sal::set_current_thread_name("segment_provider");

      while (thread.yield(std::chrono::milliseconds(1)))
      {
         // Handle excess mlocked segments
         provider_munlock_excess_segments();

         // Process segments that have been recycled and are available to be reused
         provider_process_recycled_segments();

         provider_populate_pinned_segments();
         provider_populate_unpinned_segments();
      }
   }
   void allocator::provider_populate_pinned_segments()
   {
      auto& provider_state = _mapped_state->_segment_provider;
      if (provider_state.ready_pinned_segments.usage() > 4)
         return;
      // prioritize segments that are free and pinned
      std::optional<segment_number> free_pinned_seg = find_first_free_and_pinned_segment();

      segment_number seg_num;
      // Get a segment ready
      if (not free_pinned_seg)
         seg_num = segment_number(provider_state.free_segments.unset_first_set());
      else
      {
         seg_num = *free_pinned_seg;
         provider_state.free_segments.reset(*seg_num);
      }
      if (seg_num == provider_state.free_segments.invalid_index)
      {
         // grab a segment from the unpinned queue
         auto maybe_seg = provider_state.ready_unpinned_segments.try_pop();
         if (maybe_seg)
            seg_num = *maybe_seg;
         else
            seg_num = provider_allocate_new_segment();
      }

      provider_prepare_segment(seg_num, true /* pin it*/);
      provider_state.ready_pinned_segments.push(seg_num);
      _mapped_state->_segment_data.added_to_provider_queue(seg_num);
   }
   void allocator::provider_populate_unpinned_segments()
   {
      auto& provider_state = _mapped_state->_segment_provider;
      if (provider_state.ready_unpinned_segments.usage() > 2)
         return;

      segment_number seg_num{provider_state.free_segments.unset_first_set()};
      if (seg_num == provider_state.free_segments.invalid_index)
         seg_num = provider_allocate_new_segment();
      provider_prepare_segment(seg_num, false /* don't pin it*/);
      _mapped_state->_segment_data.added_to_provider_queue(seg_num);
      provider_state.ready_unpinned_segments.push(seg_num);
   }

   segment_number allocator::provider_allocate_new_segment()
   {
      auto [block_num, offset] = _block_alloc.alloc();
      return segment_number(*block_num);
   }

   void allocator::provider_prepare_segment(segment_number seg_num, bool pin)
   {
      auto& provider_state = _mapped_state->_segment_provider;

      // utilized by compactor to propagate the relative age of data in a segment
      // for the purposes of munlock the oldest data first and avoiding promoting data
      // just because we compacted a segment to save space.
      msec_timestamp now_ms = sal::get_current_time_msec();

      // Update the virtual age in segment metadata to match the segment header's initial value
      disable_segment_write_protection(seg_num);

      auto sp = _block_alloc.get<mapped_memory::segment>(block_allocator::block_number(*seg_num));
      if (pin)
      {
         // if it is not already mlocked, mlock it
         if (not provider_state.mlock_segments.test(*seg_num))
         {
            if (mlock(sp, segment_size) != 0) [[unlikely]]
            {
               SAL_ERROR("mlock error(", errno, ") ", strerror(errno));
               update_segment_pinned_state(seg_num, false);
            }
            else
            {
               update_segment_pinned_state(seg_num, true);
               // munlock the oldest mlocked segment(s) if needed
               provider_munlock_excess_segments();
            }
         }
      }
      else
      {
         if (provider_state.mlock_segments.test(*seg_num))
         {
            if (munlock(sp, segment_size) != 0) [[unlikely]]
            {
               SAL_ERROR("munlock error(", errno, ") ", strerror(errno));
            }
            update_segment_pinned_state(seg_num, false);
         }
      }
      auto shp = new (sp) mapped_memory::segment();
      shp->set_alloc_pos(0);
      shp->_first_writable_page = 0;
      shp->_session_id          = allocator_session_number(-1);
      shp->_open_time_usec      = msec_timestamp(0);
      shp->_close_time_usec     = msec_timestamp(0);
      shp->age_accumulator.reset(*now_ms);

      //ARBTRIE_WARN("segment_provider: Prepared segment ", seg_num,
      //             " freed space: ", meta.get_free_state().free_space);
   }

   /**
    * Unlocks excess segments from memory when we exceed the maximum mlocked limit
    */
   void allocator::provider_munlock_excess_segments()
   {
      auto& provider_state = _mapped_state->_segment_provider;

      int mlocked_count = provider_state.mlock_segments.count();
      int max_count =
          _mapped_state->_config.max_pinned_cache_size_mb / (segment_size / 1024 / 1024);

      if (mlocked_count <= max_count)
         return;

      // most recent are higher numbers, so we want the lowest number
      // as it will be the oldest segment
      uint64_t oldest_age = -1;
      uint32_t oldest_seg = -1;
      for (auto seg : provider_state.mlock_segments)
      {
         uint64_t vage = _mapped_state->_segment_data.get_vage(segment_number(seg));
         if (vage < oldest_age and vage != 0)
         {
            oldest_age = vage;
            oldest_seg = seg;
         }
      }

      if (oldest_age == uint32_t(-1))
         return;

      void* seg_ptr = get_segment(segment_number(oldest_seg));
      if (munlock(seg_ptr, segment_size) != 0) [[unlikely]]
      {
         SAL_ERROR("munlock error(", errno, ") ", strerror(errno));
      }
      else
      {
         // Clear both the bitmap and the meta bit using the helper
         update_segment_pinned_state(segment_number(oldest_seg), false);
      }

      // now that is it is no longer pinned, we are unlikely to
      // access it with high degree of locality because we jump from
      // node to node as we traverse the tree and each node fits on
      // a page.
      // TODO: make some effort to prevent nodes from spanning pages
      // as they are allocated. This will waste some space, but will
      // improve disk cache performance.
      if (madvise(seg_ptr, segment_size, MADV_RANDOM) != 0) [[unlikely]]
         SAL_ERROR("madvise error(", errno, ") ", strerror(errno));
   }

   void allocator::start_background_threads()
   {
      stop_background_threads();

      // Initialize and start all threads after _mapped_state is set
      _compactor_thread.emplace(&_mapped_state->compact_thread_state, "compactor",
                                [this](segment_thread& thread) { compactor_loop(thread); });

      _read_bit_decay_thread.emplace(&_mapped_state->read_bit_decay_thread_state, "read_bit_decay",
                                     [this](segment_thread& thread)
                                     { clear_read_bits_loop(thread); });

      _segment_provider_thread.emplace(&_mapped_state->segment_provider_thread_state,
                                       "segment_provider",
                                       [this](segment_thread& thread) { provider_loop(thread); });

      _read_bit_decay_thread->start();
      _compactor_thread->start();
      _segment_provider_thread->start();
   }

   void allocator::stop_background_threads()
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
         // Stop the thread
         _segment_provider_thread->stop();
         _segment_provider_thread.reset();
      }
   }
   void allocator::set_runtime_config(const runtime_config& cfg)
   {
      _mapped_state->_config = cfg;
   }

   /**
    * Helper function to update the pinned state of a segment, ensuring both
    * the bitmap and the metadata are synchronized.
    *
    * @param seg_num The segment number to update
    * @param is_pinned Whether to set (true) or clear (false) the pinned state
    */
   void allocator::update_segment_pinned_state(segment_number seg_num, bool is_pinned)
   {
      auto& provider_state = _mapped_state->_segment_provider;

      // Update the bitmap
      if (is_pinned)
         provider_state.mlock_segments.set(*seg_num);
      else
         provider_state.mlock_segments.reset(*seg_num);

      // Update the segment metadata
      _mapped_state->_segment_data.set_pinned(seg_num, is_pinned);
   }

   /// each thread has an array of allocator_session pointers, indexed by allocator_index
   static thread_local std::array<allocator_session*, 64>* current_session = nullptr;
   void allocator::end_session(allocator_session* ses)
   {
      assert(current_session and "end_session called with no current session");
      auto cs = current_session->at(_allocator_index);
      assert(cs == ses and "end_session called with a different thread's session");
      (void)ses;  // release warning suppression
      delete cs;
      current_session->at(_allocator_index) = nullptr;
   }
   allocator_session_ptr allocator::get_session()
   {
      if (not current_session)
      {
         SAL_WARN("get_session: creating new session array {} ", _allocator_index);
         current_session = new std::array<allocator_session*, 64>();
         for (auto& s : *current_session)
            s = nullptr;
      }

      if (not current_session->at(_allocator_index))
      {
         SAL_WARN("get_session: creating new session {} ", _allocator_index);
         return allocator_session_ptr((*current_session)[_allocator_index] =
                                          new allocator_session(*this, alloc_session_num()));
      }
      SAL_WARN("get_session: returning existing session");
      auto cs = current_session->at(_allocator_index);
      cs->retain_session();
      SAL_WARN("get_session: returning existing session {}  allocidx: {}  sessptr: {} ",
               cs->get_session_num(), _allocator_index, current_session->at(_allocator_index));
      return allocator_session_ptr(cs);
   }
   void allocator::release_session_num(allocator_session_number sn) noexcept
   {
      SAL_WARN("release_session_num: {}  allocidx: {}  sessptr: {} ", sn, _allocator_index,
               current_session->at(_allocator_index));
      assert(current_session);
      assert(current_session->at(_allocator_index));
      (*current_session)[_allocator_index] = nullptr;
      _mapped_state->_session_data.release_session_num(sn);
   }
}  // namespace sal
