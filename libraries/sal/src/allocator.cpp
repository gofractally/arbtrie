#include <sys/mman.h>
#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <unordered_set>
#include <sal/alloc_header.hpp>
#include <sal/allocator.hpp>
#include <sal/allocator_impl.hpp>
#include <sal/debug.hpp>
#include <sal/seg_alloc_dump.hpp>
#include <sal/smart_ptr.hpp>

namespace sal
{
   /// Set to true to enable compactor timing and unpinned-segment diagnostics.
   /// When false, all instrumentation is compiled out by the optimizer.
   static constexpr bool debug_compactor = false;

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
         _block_alloc(dir / "segs", segment_size,
                      static_cast<uint32_t>(
                          std::min<int64_t>(cfg.max_database_size, sal::max_database_size) /
                          segment_size)),
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
      // Cap pinned cache budget to what RLIMIT_MEMLOCK will allow, using the value
      // already cached by block_allocator (no extra syscall needed).
      {
         uint64_t mlock_limit = _block_alloc.mlock_limit_bytes();
         if (mlock_limit != RLIM_INFINITY)
         {
            uint64_t limit_mb = mlock_limit / (1024ULL * 1024ULL);
            if (cfg.max_pinned_cache_size_mb > limit_mb)
            {
#ifdef __linux__
               SAL_WARN(
                   "RLIMIT_MEMLOCK is {} MB — capping pinned cache from {} MB to {} MB. "
                   "To allow more pinned RAM: `ulimit -l unlimited` (current shell) or add "
                   "`* hard memlock unlimited` to /etc/security/limits.conf (persistent).",
                   limit_mb, cfg.max_pinned_cache_size_mb, limit_mb);
#else
               SAL_WARN(
                   "RLIMIT_MEMLOCK is {} MB — capping pinned cache from {} MB to {} MB. "
                   "To allow more pinned RAM: `ulimit -l unlimited` (current shell).",
                   limit_mb, cfg.max_pinned_cache_size_mb, limit_mb);
#endif
               cfg.max_pinned_cache_size_mb = limit_mb;
            }
         }
      }

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
      truncate_free_tail_stopped();
      free_allocator_index(_allocator_index);
   }

   void allocator::truncate_free_tail_stopped()
   {
      auto& provider_state = _mapped_state->_segment_provider;

      // Drain the provider's ready queues back into free_segments bitmap.
      // These segments were popped from free_segments (or freshly allocated)
      // and prepared for reuse but never claimed by a session.
      while (auto seg = provider_state.ready_pinned_segments.try_pop())
         provider_state.free_segments.set(*(*seg));
      while (auto seg = provider_state.ready_unpinned_segments.try_pop())
         provider_state.free_segments.set(*(*seg));

      uint32_t num_segs  = _block_alloc.num_blocks();
      uint32_t new_count = num_segs;

      // Scan from the end to find the highest non-free segment
      while (new_count > 0 && provider_state.free_segments.test(new_count - 1))
         --new_count;

      if (new_count >= num_segs)
         return;  // nothing to truncate

      // munlock and clean up state for segments we're about to truncate
      for (uint32_t i = new_count; i < num_segs; ++i)
      {
         if (provider_state.mlock_segments.test(i))
         {
            auto* seg_ptr = get_segment(segment_number(i));
            ::munlock(seg_ptr, segment_size);
            update_segment_pinned_state(segment_number(i), false);
         }
         provider_state.free_segments.reset(i);
      }

      SAL_INFO("truncate_free_tail: {} -> {} segments (freeing {} MB)",
               num_segs, new_count,
               (num_segs - new_count) * (segment_size / (1024 * 1024)));

      _block_alloc.truncate(new_count);
   }

   void allocator::truncate_free_tail()
   {
      // Stop all background threads to get exclusive access to provider queues,
      // free_segments bitmap, and mlock_segments bitmap.
      stop_background_threads();

      truncate_free_tail_stopped();

      // Re-populate provider queues from remaining free segments and restart threads
      provider_populate_pinned_segments();
      provider_populate_unpinned_segments();
      start_background_threads();
   }

   void allocator::truncate_free_tail_final()
   {
      stop_background_threads();
      truncate_free_tail_stopped();
      // Intentionally do NOT restart threads or repopulate provider queues.
   }

   void allocator::copy_live_objects_to(allocator& dest)
   {
      uint64_t objects_copied = 0;
      uint64_t bytes_copied   = 0;

      {  // Scope the dest session so it's released before truncation
         auto  dst_ses  = dest.get_session();
         auto& dst_sesr = *dst_ses;

         uint32_t num_segs = _block_alloc.num_blocks();

         for (uint32_t seg_idx = 0; seg_idx < num_segs; ++seg_idx)
         {
            auto* seg = get_segment(segment_number(seg_idx));
            if (seg->get_alloc_pos() == 0)
               continue;

            auto* obj = reinterpret_cast<const alloc_header*>(seg->data);
            auto* end = reinterpret_cast<const alloc_header*>(
                seg->data +
                std::min<uint32_t>(segment_size - mapped_memory::segment_footer_size,
                                   seg->get_alloc_pos()));

            while (obj < end && obj->address() != null_ptr_address)
            {
               if (obj->size() == 0)
                  break;

               // Skip sync headers
               if (obj->type() == header_type::sync_head)
               {
                  obj = obj->next();
                  continue;
               }

               // Check if this object is live: control block points here with ref > 0
               auto* cb = _ptr_alloc.try_get(obj->address());
               if (cb)
               {
                  auto cb_data = cb->load(std::memory_order_relaxed);
                  if (cb_data.ref > 0)
                  {
                     // Verify the control block's location matches this object
                     uint64_t obj_abs =
                         uint64_t(seg_idx) * segment_size +
                         (reinterpret_cast<const char*>(obj) - seg->data);
                     auto obj_loc = location::from_absolute_address(obj_abs);

                     if (cb_data.loc() == obj_loc)
                     {
                        // Live object — allocate in dest and copy
                        auto [new_loc, new_ptr] = dst_sesr.alloc_data<alloc_header>(
                            obj->size(), obj->type(), obj->address_seq());
                        memcpy(new_ptr, obj, obj->size());

                        // Set up control block in dest
                        auto& dst_cb = dest._ptr_alloc.get_or_alloc(obj->address());
                        dst_cb.reset(new_loc, cb_data.ref);

                        ++objects_copied;
                        bytes_copied += obj->size();
                     }
                  }
               }

               obj = obj->next();
            }
         }

         // Copy root addresses
         for (uint32_t i = 0; i < _root_objects->size(); ++i)
         {
            auto addr = (*_root_objects)[i].load(std::memory_order_relaxed);
            (*dest._root_objects)[i].store(addr, std::memory_order_relaxed);
         }
      }  // dest session released here, active segment finalized

      SAL_WARN("defrag: copied {} objects ({} bytes) to new database",
               objects_copied, bytes_copied);
   }

   void allocator::recursive_retain_all(ptr_address addr)
   {
      if (addr == null_ptr_address)
         return;

      auto* cb = _ptr_alloc.try_get(addr);
      if (!cb || cb->ref() == 0)
         return;

      cb->retain();

      auto  loc    = cb->loc();
      auto  seg    = loc.segment();
      auto* segptr = get_segment(seg);
      auto* obj    = reinterpret_cast<const alloc_header*>(segptr->data + loc.segment_offset());

      if (obj->address() != addr)
         return;

      auto& vtables  = get_type_vtables();
      auto  type_idx = uint8_t(obj->type());
      if (type_idx < vtables.size() && vtables[type_idx].visit_children)
      {
         vtables[type_idx].visit_children(
             obj, [this](ptr_address child) { recursive_retain_all(child); });
      }
   }

   void allocator::recursive_sum_size(ptr_address addr, uint64_t& total, void* visited_ptr)
   {
      if (addr == null_ptr_address)
         return;

      auto& visited = *static_cast<std::unordered_set<uint64_t>*>(visited_ptr);
      if (!visited.insert(*addr).second)
         return;  // already counted

      auto* cb = _ptr_alloc.try_get(addr);
      if (!cb || cb->ref() == 0)
         return;

      auto  loc    = cb->loc();
      auto  seg    = loc.segment();
      auto* segptr = get_segment(seg);
      auto* obj    = reinterpret_cast<const alloc_header*>(segptr->data + loc.segment_offset());

      if (obj->address() != addr)
         return;

      total += obj->size();

      auto& vtables  = get_type_vtables();
      auto  type_idx = uint8_t(obj->type());
      if (type_idx < vtables.size() && vtables[type_idx].visit_children)
      {
         vtables[type_idx].visit_children(
             obj, [this, &total, visited_ptr](ptr_address child) {
                recursive_sum_size(child, total, visited_ptr);
             });
      }
   }

   uint64_t allocator::reachable_size()
   {
      uint64_t                      total = 0;
      std::unordered_set<uint64_t>  visited;
      visited.reserve(1 << 20);  // pre-allocate for ~1M objects

      for (uint32_t i = 0; i < _root_objects->size(); ++i)
      {
         auto addr = _root_objects->at(i).load(std::memory_order_relaxed);
         if (addr != null_ptr_address)
            recursive_sum_size(addr, total, &visited);
      }
      return total;
   }

   std::pair<const alloc_header*, location> allocator::resolve(ptr_address addr)
   {
      if (addr == null_ptr_address)
         return {nullptr, {}};

      auto* cb = _ptr_alloc.try_get(addr);
      if (!cb || cb->ref() == 0)
         return {nullptr, {}};

      auto  loc    = cb->loc();
      auto  seg    = loc.segment();
      auto* segptr = get_segment(seg);
      auto* obj    = reinterpret_cast<const alloc_header*>(segptr->data + loc.segment_offset());

      if (obj->address() != addr)
         return {nullptr, {}};

      return {obj, loc};
   }

   void allocator::verify_segments(verify_result& result)
   {
      uint32_t num_segs = _block_alloc.num_blocks();

      for (uint32_t seg_idx = 0; seg_idx < num_segs; ++seg_idx)
      {
         auto* seg = get_segment(segment_number(seg_idx));
         if (seg->get_alloc_pos() == 0)
            continue;

         // Walk the sync_header chain from the last sync header backward
         uint32_t pos = seg->_last_aheader_pos;
         while (pos > 0)
         {
            auto* ah = reinterpret_cast<const alloc_header*>(seg->data + pos);
            if (ah->type() != header_type::sync_head)
               break;

            auto* sh = reinterpret_cast<const sync_header*>(ah);

            if (sh->sync_checksum() != 0)
            {
               auto checksum_size =
                   pos + sh->checksum_offset() - sh->start_checksum_pos();
               auto computed =
                   XXH3_64bits(seg->data + sh->start_checksum_pos(), checksum_size);

               if (computed == sh->sync_checksum())
               {
                  ++result.segment_checksums.passed;
               }
               else
               {
                  ++result.segment_checksums.failed;
                  result.segment_failures.push_back(
                      {seg_idx, pos, sh->start_checksum_pos(),
                       pos + static_cast<uint32_t>(sh->checksum_offset())});
               }
            }
            else
            {
               ++result.segment_checksums.unknown;
            }

            if (pos == sh->prev_aheader_pos())
               break;  // prevent infinite loop
            pos = sh->prev_aheader_pos();
         }
      }
   }

   std::vector<allocator::segment_freed_audit> allocator::audit_freed_space()
   {
      std::vector<segment_freed_audit> results;
      auto                             total_segs = _block_alloc.num_blocks();
      const auto&                      seg_data   = _mapped_state->_segment_data;

      for (segment_number i{0}; i < total_segs; ++i)
      {
         const auto* s = get_segment(i);
         // Only audit read-only segments that have data
         if (!s->is_read_only())
            continue;

         segment_freed_audit audit;
         audit.seg             = i;
         audit.estimated_freed = seg_data.get_freed_space(i);

         const auto* shead = (const mapped_memory::segment*)s;
         const auto* send  = (const alloc_header*)shead->end();
         const alloc_header* obj = (const alloc_header*)(shead);

         while (obj < send)
         {
            if (obj->type() == header_type::sync_head)
            {
               audit.sync_headers += obj->size();
            }
            else
            {
               audit.total_objects++;
               auto* cb = _ptr_alloc.try_get(obj->address());
               if (!cb || cb->ref() == 0)
               {
                  audit.actual_dead += obj->size();
               }
               else
               {
                  auto  cdata = cb->load(std::memory_order_relaxed);
                  auto  loc   = cdata.loc();
                  auto* live_ptr =
                      reinterpret_cast<const alloc_header*>(get_segment(loc.segment())->data +
                                                            loc.segment_offset());
                  if (live_ptr != obj)
                     audit.actual_dead += obj->size();
                  else
                     audit.actual_live += obj->size();
               }
            }
            assert(obj != obj->next());
            obj = obj->next();
         }
         if (audit.total_objects > 0)
            results.push_back(audit);
      }
      return results;
   }

   void allocator::recover()
   {
      SAL_WARN("Recovering... rebuilding control blocks from segments!");

      stop_background_threads();

      // Phase 1: Clear all control blocks
      _ptr_alloc.clear_all();

      // Phase 2: Sort segments newest-to-oldest by _provider_sequence
      auto                  num_segs = _block_alloc.num_blocks();
      std::vector<uint32_t> age_index(num_segs);
      for (uint32_t i = 0; i < num_segs; ++i)
         age_index[i] = i;

      std::sort(age_index.begin(), age_index.end(),
                [this](uint32_t a, uint32_t b)
                {
                   return get_segment(segment_number(a))->_provider_sequence >
                          get_segment(segment_number(b))->_provider_sequence;
                });

      // Reset segment provider state
      _mapped_state->_segment_provider.free_segments.reset();
      _mapped_state->_segment_provider.mlock_segments.reset();

      for (uint32_t i = 0; i < num_segs; ++i)
         _mapped_state->_segment_data.set_pinned(segment_number(i), false);

      _mapped_state->_segment_provider.ready_pinned_segments.clear();
      _mapped_state->_segment_provider.ready_unpinned_segments.clear();

      // Phase 3: Scan each segment, rebuilding object ID -> location mapping
      uint32_t max_provider_seq = 0;
      for (auto seg_idx : age_index)
      {
         auto* seg = get_segment(segment_number(seg_idx));

         if (seg->_provider_sequence == 0 && seg->get_alloc_pos() == 0)
         {
            _mapped_state->_segment_provider.free_segments.set(seg_idx);
            continue;
         }

         if (seg->_provider_sequence > max_provider_seq)
            max_provider_seq = seg->_provider_sequence;

         auto* obj_ptr = reinterpret_cast<const alloc_header*>(seg->data);
         auto* seg_end = reinterpret_cast<const alloc_header*>(
             seg->data +
             std::min<uint32_t>(segment_size - mapped_memory::segment_footer_size,
                                seg->get_alloc_pos()));

         while (obj_ptr < seg_end && obj_ptr->address() != null_ptr_address)
         {
            if (obj_ptr->size() == 0)
               break;

            if (obj_ptr->type() == header_type::sync_head)
            {
               obj_ptr = obj_ptr->next();
               continue;
            }

            auto  addr = obj_ptr->address();
            auto& cb   = _ptr_alloc.get_or_alloc(addr);
            auto  cur_loc = cb.loc();

            uint64_t abs_addr =
                uint64_t(seg_idx) * segment_size +
                (reinterpret_cast<const char*>(obj_ptr) - seg->data);
            auto new_loc = location::from_absolute_address(abs_addr);

            // Update if unseen, or if in the same segment (later offset = newer)
            auto cur_seg = cur_loc.segment();
            if (!cur_loc.cacheline() || *cur_seg == seg_idx)
            {
               cb.store(control_block_data().set_loc(new_loc).set_ref(1),
                        std::memory_order_relaxed);
            }

            obj_ptr = obj_ptr->next();
         }
      }

      // Phase 4: Walk all root objects, recursively retain reachable nodes
      for (uint32_t i = 0; i < _root_objects->size(); ++i)
      {
         auto addr = _root_objects->at(i).load(std::memory_order_relaxed);
         if (addr != null_ptr_address)
            recursive_retain_all(addr);
      }

      // Phase 5: Free leaked objects
      _ptr_alloc.release_unreachable();

      // Reset segment provider sequence counter
      _mapped_state->_segment_provider._next_alloc_seq.store(max_provider_seq + 1,
                                                              std::memory_order_relaxed);
      _mapped_state->clean_exit_flag.store(false);

      provider_populate_pinned_segments();
      provider_populate_unpinned_segments();
      start_background_threads();

      SAL_WARN("Recovery complete.");
   }

   void allocator::reset_reference_counts()
   {
      SAL_WARN("Resetting reference counts...");

      stop_background_threads();

      _ptr_alloc.reset_all_refs();

      for (uint32_t i = 0; i < _root_objects->size(); ++i)
      {
         auto addr = _root_objects->at(i).load(std::memory_order_relaxed);
         if (addr != null_ptr_address)
            recursive_retain_all(addr);
      }

      _ptr_alloc.release_unreachable();

      start_background_threads();
   }

   /**
    * Find the last valid sync header in a segment by walking the chain backward
    * and verifying checksums. Returns the position of the last valid sync header,
    * or 0 if none found.
    */
   static uint32_t find_last_valid_sync(const mapped_memory::segment* seg)
   {
      uint32_t pos = seg->_last_aheader_pos;
      while (pos > 0)
      {
         auto* ah = reinterpret_cast<const alloc_header*>(seg->data + pos);
         if (ah->type() != header_type::sync_head)
            break;

         auto* sh = reinterpret_cast<const sync_header*>(ah);

         // Verify the sync checksum if present
         if (sh->sync_checksum() != 0)
         {
            auto checksum_size = pos + sh->checksum_offset() - sh->start_checksum_pos();
            auto computed = XXH3_64bits(seg->data + sh->start_checksum_pos(), checksum_size);
            if (computed == sh->sync_checksum())
               return pos;  // valid!

            SAL_WARN("sync checksum mismatch at pos {} in segment, walking backward", pos);
         }
         else
         {
            // No checksum — trust it (checksums were disabled when this was written)
            return pos;
         }

         // Walk backward
         pos = sh->prev_aheader_pos();
      }
      return 0;  // no valid sync header found
   }

   void allocator::recover_from_power_loss()
   {
      SAL_WARN("Power-loss recovery: validating segments and rebuilding state");

      stop_background_threads();

      // Phase 1: Clear all control blocks
      _ptr_alloc.clear_all();

      // Phase 2: Sort segments newest-to-oldest by _provider_sequence
      auto                  num_segs = _block_alloc.num_blocks();
      std::vector<uint32_t> age_index(num_segs);
      for (uint32_t i = 0; i < num_segs; ++i)
         age_index[i] = i;

      std::sort(age_index.begin(), age_index.end(),
                [this](uint32_t a, uint32_t b)
                {
                   return get_segment(segment_number(a))->_provider_sequence >
                          get_segment(segment_number(b))->_provider_sequence;
                });

      // Reset segment provider state
      _mapped_state->_segment_provider.free_segments.reset();
      _mapped_state->_segment_provider.mlock_segments.reset();

      for (uint32_t i = 0; i < num_segs; ++i)
         _mapped_state->_segment_data.set_pinned(segment_number(i), false);

      _mapped_state->_segment_provider.ready_pinned_segments.clear();
      _mapped_state->_segment_provider.ready_unpinned_segments.clear();

      // Phase 3: Validate sync headers, truncate torn tails, collect root info
      // We collect (timestamp, root_index, root_address) from valid sync headers
      struct root_recovery_entry
      {
         usec_timestamp timestamp;
         uint32_t       root_index;
         uint32_t       root_address;
      };
      std::vector<root_recovery_entry> recovered_roots;

      uint32_t max_provider_seq = 0;
      for (auto seg_idx : age_index)
      {
         auto* seg = get_segment(segment_number(seg_idx));

         if (seg->_provider_sequence == 0 && seg->get_alloc_pos() == 0)
         {
            _mapped_state->_segment_provider.free_segments.set(seg_idx);
            continue;
         }

         if (seg->_provider_sequence > max_provider_seq)
            max_provider_seq = seg->_provider_sequence;

         // Find last valid sync boundary
         uint32_t valid_sync_pos = find_last_valid_sync(seg);

         // Determine valid data end.
         // For segments with valid sync headers, we know data up to the sync
         // boundary is consistent. For segments without sync headers (e.g. the
         // active segment that was never synced), we scan up to alloc_pos and
         // rely on individual object checksums to detect corruption.
         uint32_t valid_end = seg->get_alloc_pos();

         // Collect root info from all valid sync headers in this segment
         if (valid_sync_pos > 0)
         {
            uint32_t scan_pos = valid_sync_pos;
            while (scan_pos > 0)
            {
               auto* ah = reinterpret_cast<const alloc_header*>(seg->data + scan_pos);
               if (ah->type() != header_type::sync_head)
                  break;
               auto* scan_sh = reinterpret_cast<const sync_header*>(ah);
               auto  info    = scan_sh->get_root_info();
               if (info)
               {
                  SAL_WARN("  found root_info in sync header at pos {}: root[{}] = {} ts={}",
                           scan_pos, info->root_index, info->root_address, *scan_sh->timestamp());
                  recovered_roots.push_back(
                      {scan_sh->timestamp(), info->root_index, info->root_address});
               }
               if (scan_pos == scan_sh->prev_aheader_pos())
                  break;  // prevent infinite loop
               scan_pos = scan_sh->prev_aheader_pos();
            }
         }

         SAL_WARN("segment {}: alloc_pos={} valid_end={} valid_sync_pos={} first_write_pos={}",
                  seg_idx, seg->get_alloc_pos(), valid_end, valid_sync_pos,
                  seg->get_first_write_pos());

         // Truncate: zero data beyond valid_end, update alloc_pos
         if (valid_end < seg->get_alloc_pos())
         {
            SAL_WARN("segment {}: truncating from {} to {} (torn tail)",
                     seg_idx, seg->get_alloc_pos(), valid_end);
            memset(seg->data + valid_end, 0,
                   seg->get_alloc_pos() - valid_end);
            seg->set_alloc_pos(valid_end);
         }

         // Phase 4: Scan validated segment data, rebuild object ID -> location
         auto* obj_ptr = reinterpret_cast<const alloc_header*>(seg->data);
         auto* seg_end = reinterpret_cast<const alloc_header*>(
             seg->data + std::min<uint32_t>(segment_size - mapped_memory::segment_footer_size,
                                            valid_end));

         while (obj_ptr < seg_end && obj_ptr->address() != null_ptr_address)
         {
            if (obj_ptr->size() == 0)
               break;

            if (obj_ptr->type() == header_type::sync_head)
            {
               obj_ptr = obj_ptr->next();
               continue;
            }

            auto  addr = obj_ptr->address();
            auto& cb   = _ptr_alloc.get_or_alloc(addr);
            auto  cur_loc = cb.loc();

            uint64_t abs_addr =
                uint64_t(seg_idx) * segment_size +
                (reinterpret_cast<const char*>(obj_ptr) - seg->data);
            auto new_loc = location::from_absolute_address(abs_addr);

            // Prefer newer segments (already sorted newest-first), or later offset in same segment
            auto cur_seg = cur_loc.segment();
            if (!cur_loc.cacheline() || *cur_seg == seg_idx)
            {
               cb.store(control_block_data().set_loc(new_loc).set_ref(1),
                        std::memory_order_relaxed);
            }

            obj_ptr = obj_ptr->next();
         }
      }

      // Phase 5: Rebuild roots from sync headers (newest timestamp wins)
      // Sort by timestamp descending so we encounter newest first
      std::sort(recovered_roots.begin(), recovered_roots.end(),
                [](const auto& a, const auto& b) { return a.timestamp > b.timestamp; });

      // Track which roots we've already set (first occurrence = newest)
      std::vector<bool> root_set(_root_objects->size(), false);
      uint32_t          roots_recovered = 0;
      for (auto& entry : recovered_roots)
      {
         if (entry.root_index < _root_objects->size() && !root_set[entry.root_index])
         {
            // Verify the ptr_address actually has a valid control block
            auto* cb = _ptr_alloc.try_get(ptr_address(entry.root_address));
            if (cb && cb->loc().cacheline())
            {
               _root_objects->at(entry.root_index)
                   .store(ptr_address(entry.root_address), std::memory_order_relaxed);
               root_set[entry.root_index] = true;
               ++roots_recovered;
               SAL_WARN("recovered root[{}] = {} from sync header (ts={})",
                        entry.root_index, entry.root_address, *entry.timestamp);
            }
         }
      }

      // Validate roots from the roots file — these were synced to disk by
      // allocator::sync() during commit, so they are the primary source.
      // Sync headers are a secondary source for when the roots file is corrupt.
      for (uint32_t i = 0; i < _root_objects->size(); ++i)
      {
         if (!root_set[i])
         {
            auto old = _root_objects->at(i).load(std::memory_order_relaxed);
            if (old != null_ptr_address)
            {
               // Verify the existing root's control block was rebuilt by the segment scan
               auto* cb = _ptr_alloc.try_get(old);
               if (!cb || !cb->loc().cacheline())
               {
                  SAL_WARN("root[{}] = {} from roots file is invalid (no control block), clearing", i, *old);
                  _root_objects->at(i).store(null_ptr_address, std::memory_order_relaxed);
               }
               else
               {
                  root_set[i] = true;  // existing root is valid, keep it
               }
            }
         }
      }

      // Phase 6: Walk all valid roots, recursively retain reachable nodes
      for (uint32_t i = 0; i < _root_objects->size(); ++i)
      {
         auto addr = _root_objects->at(i).load(std::memory_order_relaxed);
         if (addr != null_ptr_address)
            recursive_retain_all(addr);
      }

      // Phase 7: Free leaked objects
      _ptr_alloc.release_unreachable();

      // Reset segment provider sequence counter
      _mapped_state->_segment_provider._next_alloc_seq.store(max_provider_seq + 1,
                                                              std::memory_order_relaxed);
      _mapped_state->clean_exit_flag.store(false);

      provider_populate_pinned_segments();
      provider_populate_unpinned_segments();
      start_background_threads();

      uint32_t roots_from_file = 0;
      for (uint32_t i = 0; i < _root_objects->size(); ++i)
         if (root_set[i] && i >= roots_recovered)
            ++roots_from_file;
      SAL_WARN("Power-loss recovery complete. {} roots from sync headers, {} validated from roots file.",
               roots_recovered, roots_from_file);
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
      auto& sesr = *ses;

      /**
       *  The compactor always prioritizes cache, then ram, then
       *  all other segments each loop. It needs to keep data of similar
       *  age together to minimize movement of virtual age of objects.
       */
      if constexpr (debug_compactor)
      {
         using clock = std::chrono::steady_clock;
         uint64_t total_promote_ns = 0, total_pinned_ns = 0, total_unpinned_ns = 0;
         auto     last_report = clock::now();
         uint64_t iterations  = 0;

         while (thread.yield())
         {
            auto t0 = clock::now();
            compactor_promote_rcache_data(sesr);
            auto t1 = clock::now();
            compact_pinned_segment(sesr);
            auto t2 = clock::now();
            compactor_promote_rcache_data(sesr);
            compact_unpinned_segment(sesr);
            auto t3 = clock::now();

            total_promote_ns += (t1 - t0).count() + (t3 - t2).count();
            total_pinned_ns += (t2 - t1).count();
            total_unpinned_ns += (t3 - t2).count();
            ++iterations;

            auto elapsed = clock::now() - last_report;
            if (elapsed >= std::chrono::seconds(5))
            {
               auto to_ms = [](uint64_t ns) { return ns / 1'000'000; };
               SAL_WARN("compactor: iters={} promote={}ms pinned={}ms unpinned={}ms", iterations,
                        to_ms(total_promote_ns), to_ms(total_pinned_ns), to_ms(total_unpinned_ns));
               total_promote_ns = total_pinned_ns = total_unpinned_ns = 0;
               iterations                                              = 0;
               last_report                                             = clock::now();
            }
         }
      }
      else
      {
         while (thread.yield())
         {
            compactor_promote_rcache_data(sesr);
            compact_pinned_segment(sesr);
            compactor_promote_rcache_data(sesr);
            compact_unpinned_segment(sesr);
         }
      }
   }
   bool allocator::compactor_release_objects(allocator_session& ses)
   {
      bool        more_work = false;
      ptr_address read_ids[1024];
      for (uint32_t snum = 0; snum < _mapped_state->_session_data.session_capacity(); ++snum)
      {
         auto& rcache = _mapped_state->_session_data.release_queue(allocator_session_number(snum));
         // only process up to 1024 objects so that we can yield to
         // other tasks the compactor is doing without too much delay
         auto num_loaded = rcache.pop(read_ids, 1024);
         for (uint32_t i = 0; i < num_loaded; ++i)
         {
            auto addr    = read_ids[i];
            auto obj_ref = ses.get_ref<alloc_header>(addr);
            ses.final_release(addr);
         }
      }
      return more_work;
   }

   /**
    * Dedicated release thread loop — processes deferred object releases from all
    * session release queues.  Uses fine-grained read locks so the compactor can
    * continue recycling segments between individual releases.
    *
    * For each object: lock → dereference → vcall::destroy (cascades children
    * back to queue or processes inline if full) → unlock → record_freed_space
    * → free control block.
    */
   void allocator::release_loop(segment_thread& thread)
   {
      sal::set_current_thread_name("release");

      auto  ses  = get_session();
      auto& sesr = *ses;

      ptr_address read_ids[1024];
      while (thread.yield())
      {
         for (uint32_t snum = 0; snum < _mapped_state->_session_data.session_capacity(); ++snum)
         {
            auto& rqueue =
                _mapped_state->_session_data.release_queue(allocator_session_number(snum));
            auto num_loaded = rqueue.pop(read_ids, 1024);
            for (uint32_t i = 0; i < num_loaded; ++i)
            {
               // Fine-grained read lock prevents the compactor from recycling
               // the segment we're reading from during dereference + destroy.
               // The lock is nested, so recursive final_release calls through
               // vcall::destroy (when the queue is full) stay protected.
               sesr.retain_read_lock();
               sesr.final_release(read_ids[i]);
               sesr.release_read_lock();
            }
         }
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

      // Debug counters — compiled out unless debug_compactor is set
      uint32_t not_compactable = 0, pinned_count = 0, below_threshold = 0;
      uint32_t nc_active = 0, nc_pending = 0, nc_free = 0, nc_queued = 0, nc_zero = 0;

      for (segment_number i{0}; i < total_segs; ++i)
      {
         const auto freed_space = seg_data.get_freed_space(i);
         if (not seg_data.may_compact(i))
         {
            if constexpr (debug_compactor)
            {
               ++not_compactable;
               auto f = seg_data.get_flags(i);
               if (f & 0x04) ++nc_active;
               else if (f & 0x08) ++nc_pending;
               else if (f & 0x10) ++nc_free;
               else if (f & 0x20) ++nc_queued;
               else if (f == 0) ++nc_zero;
            }
            continue;
         }
         if (seg_data.is_pinned(i))
         {
            if constexpr (debug_compactor)
               ++pinned_count;
            continue;
         }
         if (freed_space <
             _mapped_state->_config.compact_unpinned_unused_threshold_mb * 1024 * 1024)
         {
            if constexpr (debug_compactor)
               ++below_threshold;
            continue;
         }

         int64_t vage = seg_data.get_vage(i);

         // Check if this segment should be included (if array isn't full yet or if the vage is
         // higher than the lowest one we have)
         if (insert_sorted_pair(qualifying_segments, total_qualifying, {i, vage}))
            potential_free_space += freed_space;
      }
      if (potential_free_space < segment_size)
      {
         if constexpr (debug_compactor)
         {
            static auto last_log = std::chrono::steady_clock::now();
            auto        now      = std::chrono::steady_clock::now();
            if (now - last_log >= std::chrono::seconds(5))
            {
               SAL_WARN(
                   "compact_unpinned: no work — segs={} nc={} (active={} pending={} free={} queued={} zero={}) pinned={} below_thresh={} qualifying={} potential_free={}KB",
                   total_segs, not_compactable, nc_active, nc_pending, nc_free, nc_queued, nc_zero,
                   pinned_count, below_threshold, total_qualifying, potential_free_space / 1024);
               last_log = now;
            }
         }
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
      //      SAL_ERROR("compact_segment: {:L}", seg_num);
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
         // Verify checksum — on failure, halt writes rather than crashing
         if (config_validate_checksum_on_compact())
         {
            if (vcall::has_checksum(nh) and not vcall::verify_checksum(nh))
            {
               SAL_ERROR("corruption detected: obj {} size: {} checksum: {:x} expected: {:x}",
                         nh->address(), nh->size(), nh->checksum(), nh->calculate_checksum());
               signal_corruption();
               return;  // stop compacting, don't touch corrupt data
            }
         }

         /// acquires the modify lock on the segment, we must release it
         auto [loc, head] = ses.alloc_data_vage<alloc_header>(vcall::compact_size(nh), vage,
                                                              nh->type(), nh->address_seq());
         vcall::compact_to(nh, head);
         // update checksum if needed, because the user may
         // have used config::update_checksum_on_upsert = false
         // to get better user performance and offload the checksum
         // work to the compaction thread.
         if (config_update_checksum_on_compact())
            if (not vcall::has_checksum(head))
               vcall::update_checksum(head);

         if (obj_ref.control().cas_move(obj_ref.loc(), loc))
         {
            ses.record_freed_space(obj_ref.obj());
         }
         else
         {
            if (not ses.unalloc(head->size()))
               ses.record_freed_space(head);
         }
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
         seg_info.hdr_read_only = seg->is_read_only();
         seg_info.is_finalized  = seg->is_finalized();
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

      // Control block stats
      result.control_block_zones    = _ptr_alloc.num_allocated_zones();
      result.control_block_capacity = _ptr_alloc.current_max_address_count();

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
      // Resolve effective max_count from the (already RLIMIT-capped) config.
      // If 0, mlock would never succeed — skip the attempt entirely.
      const int max_count =
          _mapped_state->_config.max_pinned_cache_size_mb / (segment_size / 1024 / 1024);
      if (pin && max_count > 0)
      {
         // if it is not already mlocked, mlock it
         if (not provider_state.mlock_segments.test(*seg_num))
         {
            if (mlock(sp, segment_size) != 0) [[unlikely]]
            {
               SAL_ERROR("mlock error({}) {}", errno, strerror(errno));
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

      if (oldest_age == uint64_t(-1))
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

      _release_thread.emplace(&_mapped_state->release_thread_state, "release",
                              [this](segment_thread& thread) { release_loop(thread); });

      _read_bit_decay_thread.emplace(&_mapped_state->read_bit_decay_thread_state, "read_bit_decay",
                                     [this](segment_thread& thread)
                                     { clear_read_bits_loop(thread); });

      _segment_provider_thread.emplace(&_mapped_state->segment_provider_thread_state,
                                       "segment_provider",
                                       [this](segment_thread& thread) { provider_loop(thread); });

      _read_bit_decay_thread->start();
      _release_thread->start();
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

      // Stop release thread before compactor so pending releases drain first
      if (_release_thread)
      {
         _release_thread->stop();
         _release_thread.reset();
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
         SAL_INFO("get_session: creating new session array {} ", _allocator_index);
         current_session = new std::array<allocator_session*, 64>();
         for (auto& s : *current_session)
            s = nullptr;
      }

      if (not current_session->at(_allocator_index))
      {
         SAL_INFO("get_session: creating new session {} ", _allocator_index);
         return allocator_session_ptr((*current_session)[_allocator_index] =
                                          new allocator_session(*this, alloc_session_num()));
      }
      SAL_INFO("get_session: returning existing session");
      auto cs = current_session->at(_allocator_index);
      cs->retain_session();
      SAL_INFO("get_session: returning existing session {}  allocidx: {}  sessptr: {} ",
               cs->get_session_num(), _allocator_index, current_session->at(_allocator_index));
      return allocator_session_ptr(cs);
   }
   void allocator::release_session_num(allocator_session_number sn) noexcept
   {
      SAL_INFO("release_session_num: {}  allocidx: {}  sessptr: {} ", sn, _allocator_index,
               current_session->at(_allocator_index));
      assert(current_session);
      assert(current_session->at(_allocator_index));
      (*current_session)[_allocator_index] = nullptr;
      _mapped_state->_session_data.release_session_num(sn);
   }
   std::array<vtable_pointers, 128>& get_type_vtables()
   {
      static std::array<vtable_pointers, 128> _type_vtables = []()
      {
         std::array<vtable_pointers, 128> vtables;
         for (uint8_t i = 0; i < vtables.size(); ++i)
            vtables[i] = vtable_pointers::create<vtable<alloc_header>>();
         return vtables;
      }();
      return _type_vtables;
   }

}  // namespace sal
