#include <errno.h>  // For errno and ESRCH
#include <sys/mman.h>
#include <algorithm>
#include <arbtrie/arbtrie.hpp>
#include <arbtrie/binary_node.hpp>
#include <arbtrie/file_fwd.hpp>
#include <arbtrie/seg_alloc_dump.hpp>
#include <arbtrie/seg_allocator.hpp>
#include <array>
#include <bit>
#include <cassert>
#include <cstring>
#include <new>
#include <sal/debug.hpp>
#include <string>
#include <thread>         // For std::this_thread::sleep_for
#include <unordered_set>  // For std::unordered_set

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

   seg_allocator::seg_allocator(std::filesystem::path dir)
       : _id_alloc(dir / "ids"),
         _block_alloc(dir / "segs", segment_size, max_segment_count),
         _seg_alloc_state_file(dir / "header", access_mode::read_write, true)
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
            return;
         }
         _mapped_state->_segment_data.set_pinned(seg, true);
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
      // Set thread name for sal debug system
      sal::set_current_thread_name("read_bit_decay");

      using namespace std::chrono;
      using namespace std::chrono_literals;

      // Initialize current_region from the persisted state
      auto current_region =
          id_region(_mapped_state->next_clear_read_bit_region.load(std::memory_order_relaxed));

      // Number of total iterations needed to cover all regions (processing at least 1 region per iteration)
      const auto total_iterations_needed = id_alloc::max_regions;

      // Initial sleep time - will be recalculated in the loop
      milliseconds sleep_duration(0);

      // Loop until thread should exit (yield returns false)
      while (thread.yield(sleep_duration))
      {
         auto start_time = high_resolution_clock::now();
         _id_alloc.clear_active_bits(current_region, 1);

         // save and update the next region to process, wrapping around because uint16_t
         _mapped_state->next_clear_read_bit_region.store(*(++current_region),
                                                         std::memory_order_relaxed);

         // Dynamically calculate sleep time based on window size
         // Formula: window_time / total_iterations_needed = time_per_iteration
         auto time_per_iteration = std::chrono::duration_cast<std::chrono::milliseconds>(
             _mapped_state->_cache_difficulty_state._cache_frequency_window /
             total_iterations_needed);

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
      // Set thread name for sal debug system
      sal::set_current_thread_name("compactor");

      auto ses = start_session();
      // compact them in order into a new session
      auto unpinned_session = start_session();
      unpinned_session.set_alloc_to_pinned(false);

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
         compact_unpinned_segment(unpinned_session);
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
            auto state   = ses.lock();  // read only lock
            auto addr    = read_ids[i];
            auto obj_ref = state.get(addr);
            // reads the relaxed cached load of the location
            auto start_loc = obj_ref.loc();

            // when the object is freed and reallocated, the pending cache
            // bit is cleared, so we need to make sure the bit is set before
            // we assume we are still referencing the same object.
            if (not obj_ref.meta().try_end_pending_cache())
               continue;

            // before we even consider reading this pointer, we need to make
            // sure that the location is read-only....
            if (not state.is_read_only(start_loc))
               continue;

            // obj_ref.header() will fail here if object was released after
            // checking ref() above...
            auto header = state.get_node_pointer(start_loc);

            if (header->address() != addr)
               continue;

            // TODO: return a scoped lock with new_loc and new_header
            //the ses modify lock is held while modifying while allocating
            auto [new_loc, new_header] = ses.alloc_data(header->size(), header->address_seq());
            memcpy(new_header, header, header->size());

            if constexpr (update_checksum_on_compact)
            {
               if (not new_header->has_checksum())
                  new_header->update_checksum();
            }

            /// if the location hasn't changed then we are good to go
            ///     - and the objeect is still valid ref count
            if (obj_ref.compare_exchange_location(start_loc, new_loc))
            {
               _mapped_state->_cache_difficulty_state.compactor_promote_bytes(header->size());
               ses.record_freed_space(get_segment_num(start_loc), header);
            }
            else
            {
               if (not ses.unalloc(header->size()))
                  ses.record_freed_space(get_segment_num(new_loc), new_header);
            }
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

      size_t total_qualifying = 0;
      // Define N as the maximum number of top segments to track
      constexpr int                                     N = 16;  // Number of top segments to track
      std::array<std::pair<segment_number, int64_t>, N> qualifying_segments;

      uint32_t    potential_free_space = 0;
      const auto& seg_data             = _mapped_state->_segment_data;
      for (int i = 0; i < total_segs; ++i)
      {
         if (not seg_data.is_read_only(i) or not seg_data.is_pinned(i))
            continue;
         const auto freed_space = seg_data.get_freed_space(i);
         if (freed_space < segment_size / 2)
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

      for (int i = 0; i < total_qualifying; ++i)
         compact_segment(ses, qualifying_segments[i].first);
      return total_qualifying != N;
   }

   bool seg_allocator::compact_unpinned_segment(seg_alloc_session& ses)
   {
      auto   total_segs       = _block_alloc.num_blocks();
      size_t total_qualifying = 0;

      // Define N as the maximum number of top segments to track
      constexpr int                                     N = 8;  // Number of top segments to track
      std::array<std::pair<segment_number, int64_t>, N> qualifying_segments;

      uint32_t    potential_free_space = 0;
      const auto& seg_data             = _mapped_state->_segment_data;
      int         not_read_only        = 0;
      int         pinned               = 0;
      int         insufficient_space   = 0;
      for (int i = 0; i < total_segs; ++i)
      {
         const auto freed_space = seg_data.get_freed_space(i);
         if (not seg_data.is_read_only(i))
         {
            not_read_only++;
            continue;
         }
         if (seg_data.is_pinned(i))
         {
            pinned++;
            continue;
         }
         if (freed_space < segment_size / 2)
         {
            insufficient_space++;
            continue;
         }

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

      ARBTRIE_WARN("compact_unpinned_segments: ", total_qualifying);

      // configure the session to not allocate from the pinned segments
      for (int i = 0; i < total_qualifying; ++i)
         compact_segment(ses, qualifying_segments[i].first);

      return total_qualifying != N;
   }

   void seg_allocator::compact_segment(seg_alloc_session& ses, uint64_t seg_num)
   {
      ARBTRIE_DEBUG("compact_segment: ", seg_num);
      auto        state = ses.lock();
      const auto* s     = get_segment(seg_num);

      // if it is read only, then we don't have to worry about locking!
      assert(s->is_read_only() && "segment must be read only before compacting");

      // collect the range we will be iterating over
      const auto* shead = (const mapped_memory::segment*)s;
      const auto* send  = (const allocator_header*)shead->end();

      // cast the start to first object_header
      const allocator_header* foo = (const allocator_header*)(shead);

      // define a lambda to help copy nodes and facilitate early exit
      auto try_copy_node = [&](const node_header* nh, uint64_t vage)
      {
         if (not nh->address())
            return;

         // skip anything that has been freed
         auto obj_ref = state.get(nh->address());
         if (obj_ref.ref() == 0)
            return;  // object was freed

         const auto foo_idx    = (const char*)foo - (const char*)s;
         auto       expect_loc = obj_ref.loc();
         if (expect_loc.absolute_address() != seg_num * segment_size + foo_idx)
            return;  // object was moved

         // assert / throw exception if the checksum is invalid,
         // stop the world, we may need to do data recovery.
         if (config_validate_checksum_on_compact())
            nh->assert_checksum();

         /// TODO: acquire modification lock here

         /// acquires the modify lock on the segment, we must release it
         auto [loc, head] = ses.alloc_data(nh->size(), nh->address_seq(), vage);

         // binary nodes get optimized on copy
         if (nh->get_type() == node_type::binary)
            copy_binary_node((binary_node*)head, (const binary_node*)nh);
         else
         {
            memcpy_aligned_64byte(head, nh, nh->size());
            //memcpy(head, nh, nh->size());
         }

         // update checksum if needed, because the user may
         // have used config::update_checksum_on_upsert = false
         // to get better user performance and offload the checksum
         // work to the compaction thread.
         if (config_update_checksum_on_compact())
            if (not head->has_checksum())
               head->update_checksum();

         if (not obj_ref.compare_exchange_location(expect_loc, loc))
            if (not ses.unalloc(nh->size()))
               ses.record_freed_space(get_segment_num(loc), head);
      };  /// end try_copy_node lambda

      uint32_t src_vage = s->_vage_accumulator.average_age();
      while (foo < send)
      {
         if (foo->is_allocator_header())
            src_vage = foo->_source_age_ms;
         else  // foo is a node_header
            try_copy_node((const node_header*)(foo), src_vage);
         assert(foo != foo->next());
         foo = foo->next();
      }

      // TODO: now we need to mark everything thus far in the compacted segment as read only,
      // to prevent any accidential modifications. The source was read only, so the
      // destination should be as well.

      // Reset appropriate state fields while preserving pinned state and other flags
      // _mapped_state->_segment_data.meta[seg_num].finalize_compaction();

      ses.sync(sync_type::none, 0, id_address());

      // ensures that the segment will not get selected for compaction again until
      // after it is reused by the provider thread.
      _mapped_state->_segment_data.prepare_for_reuse(seg_num);

      // only one thread can move the end_ptr or this will break
      // std::cerr<<"done freeing end_ptr: " << _mapped_state->end_ptr.load() <<" <== " << seg_num <<"\n";

      //   ARBTRIE_DEBUG("pushing recycled segment: ", seg_num);
      _mapped_state->_read_lock_queue.push_recycled_segment(seg_num);
   }

   // called when a segment is being prepared for use by the provider thread
   void seg_allocator::disable_segment_write_protection(segment_number seg_num)
   {
      auto seg = get_segment(seg_num);
      // Make the segment read-write accessible
      if (mprotect(seg, segment_size, PROT_READ | PROT_WRITE) != 0) [[unlikely]]
         ARBTRIE_WARN("mprotect error(", errno, ") ", strerror(errno));

      // reset the first writable page to 0, so everyone knows it is safe
      seg->_first_writable_page = 0;
   }

   seg_alloc_dump seg_allocator::dump()
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
      for (uint32_t i = 0; i < total_segs; ++i)
      {
         auto seg = get_segment(i);

         // Get stats directly as a stats_result struct
         auto stats = calculate_segment_read_stats(i);

         // Copy cacheline statistics to the result
         for (int j = 0; j < 257; j++)
         {
            result.index_cline_counts[j] += stats.index_cline_counts[j];
            result.cline_delta_counts[j] += stats.cline_delta_counts[j];
         }

         // Track total non-value nodes for average calculation
         result.total_non_value_nodes += stats.non_value_nodes;

         const auto&                  seg_data    = _mapped_state->_segment_data;
         const auto                   freed_space = seg_data.get_freed_space(i);
         seg_alloc_dump::segment_info seg_info;
         seg_info.segment_num   = i;
         seg_info.freed_percent = int(100 * double(freed_space) / segment_size);
         seg_info.freed_bytes   = freed_space;
         seg_info.freed_objects = 0;
         seg_info.alloc_pos     = seg->get_alloc_pos();
         seg_info.is_pinned     = seg_data.is_pinned(i);
         seg_info.bitmap_pinned = _mapped_state->_segment_provider.mlock_segments.test(i);
         seg_info.age =
             seg->_provider_sequence;  // TODO: rename seg_info.age to seg_info.provider_sequence
         seg_info.read_nodes    = stats.nodes_with_read_bit;
         seg_info.read_bytes    = stats.total_bytes;
         seg_info.vage          = seg_data.get_vage(i);
         seg_info.total_objects = stats.total_objects;

         result.segments.push_back(seg_info);
         result.total_free_space += freed_space;
         result.total_read_nodes += stats.nodes_with_read_bit;
         result.total_read_bytes += stats.total_bytes;
      }

      // Gather session information
      result.active_sessions = _mapped_state->_session_data.active_session_count();

      auto fs = _mapped_state->_session_data.free_session_bitmap();
      for (uint32_t i = 0; i < _mapped_state->_session_data.session_capacity(); ++i)
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

   /** 
    * Count the number of unique cachelines and total branches for a node.
    * @param nh The node header to analyze
    * @return A tuple containing {number of unique cachelines, total number of branches, ideal cachelines}
    */
   std::tuple<uint32_t, uint32_t, uint32_t> count_cacheline_hits(const node_header* nh)
   {
      // Visit the address of each branch, divide it by 64 to get the cacheline,
      // keep track of unique cachelines as well as total branches
      uint32_t                     total_branches = 0;
      std::unordered_set<uint32_t> unique_cachelines;

      // Lambda to count each branch's cacheline
      auto count_branch = [&](id_address branch_addr)
      {
         if (branch_addr)
         {
            total_branches++;

            // Calculate cacheline from the address index - divide by 8 objects  8 bytes each per cacheline
            uint32_t cacheline = *branch_addr.index / 8;
            unique_cachelines.insert(cacheline);
         }
      };

      // Skip if null or is an allocator header
      if (!nh || nh->is_allocator_header())
      {
         return {0, 0, 0};
      }

      // Use cast_and_call to handle different node types
      cast_and_call(nh, [&](const auto* typed_node) { typed_node->visit_branches(count_branch); });

      // Calculate ideal number of cachelines: (branches+7)/8
      uint32_t ideal_cachelines = (total_branches + 7) / 8;

      return {static_cast<uint32_t>(unique_cachelines.size()), total_branches, ideal_cachelines};
   }

   seg_allocator::stats_result seg_allocator::calculate_segment_read_stats(segment_number seg_num)
   {
      seg_allocator::stats_result result;
      uint32_t                    nodes_with_read_bit = 0;
      uint64_t                    total_bytes         = 0;
      uint32_t                    total_objects       = 0;  // All objects
      uint32_t non_value_nodes = 0;  // Count of non-value nodes for average calculation

      auto seg  = get_segment(seg_num);
      auto send = (const node_header*)((char*)seg + segment_size);

      if (seg->get_alloc_pos() != -1)
         send = (const node_header*)(((char*)seg) + seg->get_alloc_pos());

      const node_header* foo = (const node_header*)(seg);

      while (foo < send && foo->address())
      {
         if (foo->is_allocator_header())
         {
            foo = foo->next();
            continue;
         }

         // Skip value_node types for cacheline counting
         if (foo->get_type() != node_type::value)
         {
            // Count cacheline hits for this node
            auto [unique_cachelines, branch_count, ideal_cachelines] = count_cacheline_hits(foo);

            // Skip nodes with 0 cachelines
            if (unique_cachelines > 0)
            {
               if (unique_cachelines < 257)
               {
                  result.index_cline_counts[unique_cachelines]++;
               }

               // Calculate and store delta between actual and ideal
               uint32_t delta = unique_cachelines > ideal_cachelines
                                    ? unique_cachelines - ideal_cachelines
                                    : ideal_cachelines - unique_cachelines;

               if (delta < 257)
               {
                  result.cline_delta_counts[delta]++;
               }

               non_value_nodes++;  // Count non-value nodes for average calculation
            }
         }

         // Get the object reference for this node
         auto foo_address = foo->address();

         auto& obj_ref     = _id_alloc.get(foo_address);
         auto  current_loc = obj_ref.loc();
         auto  foo_idx     = (char*)foo - (char*)seg;

         // Only count if the object reference is pointing to this exact node
         if (current_loc.absolute_address() != seg_num * segment_size + foo_idx)
         {
            foo = foo->next();
            continue;
         }
         // Count all objects
         total_objects++;

         // Check if the read bit is set and if the location matches
         if (obj_ref.active())
         {
            nodes_with_read_bit++;
            total_bytes += foo->size();
         }

         foo = foo->next();
      }

      result.nodes_with_read_bit = nodes_with_read_bit;
      result.total_bytes         = total_bytes;
      result.total_objects       = total_objects;
      result.non_value_nodes     = non_value_nodes;  // Store non-value node count

      return result;
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
      // Set thread name for sal debug system
      sal::set_current_thread_name("segment_provider");

      // Thread name should already be set by segment_thread

      auto& provider_state = _mapped_state->_segment_provider;

      ARBTRIE_DEBUG("segment_provider: Starting provider thread");

      // Set a reasonable timeout that allows for regular heartbeat checks
      const std::chrono::milliseconds wait_timeout(50);

      int iteration_count = 0;
      while (thread.yield(std::chrono::milliseconds(1)))
      {
         iteration_count++;

         // Handle excess mlocked segments
         provider_munlock_excess_segments();

         // Process segments that have been recycled and are available to be reused
         provider_process_recycled_segments();

         provider_populate_pinned_segments();
         provider_populate_unpinned_segments();
      }
      ARBTRIE_WARN("segment_provider: Exiting provider loop");
   }
   void seg_allocator::provider_populate_pinned_segments()
   {
      auto& provider_state = _mapped_state->_segment_provider;
      if (provider_state.ready_pinned_segments.usage() > 4)
         return;
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
   }
   void seg_allocator::provider_populate_unpinned_segments()
   {
      auto& provider_state = _mapped_state->_segment_provider;
      if (provider_state.ready_unpinned_segments.usage() > 2)
         return;

      segment_number seg_num = provider_state.free_segments.unset_first_set();
      if (seg_num == provider_state.free_segments.invalid_index)
         seg_num = provider_allocate_new_segment();
      provider_prepare_segment(seg_num, false /* don't pin it*/);
      provider_state.ready_unpinned_segments.push(seg_num);
   }

   segment_number seg_allocator::provider_allocate_new_segment()
   {
      auto [block_num, offset] = _block_alloc.alloc();
      return *block_num;
   }

   void seg_allocator::provider_prepare_segment(segment_number seg_num, bool pin)
   {
      auto& provider_state = _mapped_state->_segment_provider;

      // utilized by compactor to propagate the relative age of data in a segment
      // for the purposes of munlock the oldest data first and avoiding promoting data
      // just because we compacted a segment to save space.
      uint64_t now_ms = arbtrie::get_current_time_ms();

      // Update the virtual age in segment metadata to match the segment header's initial value
      _mapped_state->_segment_data.prepare_for_reuse(seg_num);
      disable_segment_write_protection(seg_num);

      auto sp = _block_alloc.get<mapped_memory::segment>(block_number(seg_num));
      if (pin)
      {
         // if it is not already mlocked, mlock it
         if (not provider_state.mlock_segments.test(seg_num))
         {
            if (mlock(sp, segment_size) != 0) [[unlikely]]
            {
               ARBTRIE_ERROR("mlock error(", errno, ") ", strerror(errno));
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
         if (provider_state.mlock_segments.test(seg_num))
         {
            if (munlock(sp, segment_size) != 0) [[unlikely]]
            {
               ARBTRIE_ERROR("munlock error(", errno, ") ", strerror(errno));
            }
            update_segment_pinned_state(seg_num, false);
         }
      }
      auto shp = new (sp) mapped_memory::segment();
      shp->_provider_sequence =
          _mapped_state->_segment_provider._next_alloc_seq.fetch_add(1, std::memory_order_relaxed);
      shp->set_alloc_pos(0);
      shp->_first_writable_page = 0;
      shp->_session_id          = -1;
      shp->_open_time_usec      = 0;
      shp->_close_time_usec     = 0;
      shp->_vage_accumulator.reset(now_ms);

      //ARBTRIE_WARN("segment_provider: Prepared segment ", seg_num,
      //             " freed space: ", meta.get_free_state().free_space);
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
         uint64_t vage = _mapped_state->_segment_data.get_vage(seg);
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
         /*
         ARBTRIE_WARN("munlocked segment: ", oldest_seg,
                      " count: ", provider_state.mlock_segments.count(),
                      " vage abs: ", _mapped_state->_segment_data.meta[oldest_seg].get_vage(),
                      "     rel age ms: ",
                      arbtrie::get_current_time_ms() -
                          _mapped_state->_segment_data.meta[oldest_seg].get_vage());
                          */
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
      _mapped_state->_segment_data.set_pinned(seg_num, is_pinned);
   }
}  // namespace arbtrie
