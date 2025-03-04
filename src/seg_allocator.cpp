#include <arbtrie/binary_node.hpp>
#include <arbtrie/file_fwd.hpp>
#include <arbtrie/seg_alloc_dump.hpp>
#include <arbtrie/seg_allocator.hpp>
#include <bit>
#include <cassert>
#include <new>

static const uint64_t page_size      = getpagesize();
static const uint64_t page_size_mask = ~(page_size - 1);

namespace arbtrie
{
   seg_allocator::seg_allocator(std::filesystem::path dir)
       : _id_alloc(dir / "ids"),
         _block_alloc(dir / "segs", segment_size, max_segment_count),
         _header_file(dir / "header", access_mode::read_write, true),
         _seg_sync_locks(max_segment_count),
         _dirty_segs(max_segment_count)
   {
      if (_header_file.size() == 0)
      {
         _header_file.resize(round_to_page(sizeof(mapped_memory::allocator_header)));
         new (_header_file.data()) mapped_memory::allocator_header();
      }
      _header = reinterpret_cast<mapped_memory::allocator_header*>(_header_file.data());

      for (auto& sptr : _header->session_lock_ptrs)
         sptr.store(uint32_t(-1ull));
      _done.store(false);

      for (auto& i : _mlocked)
         i.store(-1, std::memory_order_relaxed);
   }

   seg_allocator::~seg_allocator()
   {
      stop_compact_thread();
      _compactor_session.reset();

      // Clean up _rcache_queues
      for (auto& queue : _rcache_queues)
      {
         auto* ptr = queue.exchange(nullptr);
         delete ptr;
      }
   }
   uint32_t seg_allocator::alloc_session_num()
   {
      auto fs_bits = _header->free_sessions.load(std::memory_order_relaxed);
      if (fs_bits == 0)
         throw std::runtime_error("max of 64 sessions can be in use");

      auto fs          = std::countr_zero(fs_bits);
      auto new_fs_bits = fs_bits & ~(1 << fs);

      while (not _header->free_sessions.compare_exchange_strong(fs_bits, new_fs_bits))
      {
         if (fs_bits == 0)
            throw std::runtime_error("max of 64 sessions can be in use");

         fs          = std::countr_zero(fs_bits);
         new_fs_bits = fs_bits & ~(1 << fs);
      }
      if (not _rcache_queues[fs].load(std::memory_order_acquire))
      {
         auto* new_queue = new circular_buffer<uint32_t, 1024 * 1024>();
         circular_buffer<uint32_t, 1024 * 1024>* expected = nullptr;
         if (!_rcache_queues[fs].compare_exchange_strong(
                 expected, new_queue, std::memory_order_release, std::memory_order_acquire))
         {
            // Another thread beat us to creating the queue
            delete new_queue;
         }
      }
      return fs;
   }

   void seg_allocator::release_session_num(uint32_t sn)
   {
      _header->free_sessions.fetch_or(uint64_t(1) << sn);
   }

   void seg_allocator::start_compact_thread()
   {
      if (not _compact_thread.joinable())
      {
         _compact_thread = std::thread(
             [this]()
             {
                thread_name("compactor");
                set_current_thread_name("compactor");
                compact_loop();
             });

         // Start read bit clearer thread
         _read_bit_clearer = std::thread(
             [this]()
             {
                thread_name("read_bit_clearer");
                set_current_thread_name("read_bit_clearer");
                clear_read_bits_loop();
             });
      }
   }
   void seg_allocator::stop_compact_thread()
   {
      _done.store(true);
      if (_compact_thread.joinable())
         _compact_thread.join();
      if (_read_bit_clearer.joinable())
         _read_bit_clearer.join();
   }

   void seg_allocator::clear_read_bits_loop()
   {
      using namespace std::chrono;
      using namespace std::chrono_literals;

      uint16_t current_region = 0;

      while (!_done)
      {
         // Calculate target regions per iteration to finish in time
         const auto target_regions_per_iteration = std::max<uint32_t>(
             1u, id_alloc::max_regions /
                     (_cache_frequency_window.count() / 100));  // Based on 100ms sleep

         // Process regions
         _id_alloc.clear_some_read_bits(current_region, target_regions_per_iteration);

         // Update current_region for next iteration, wrapping around because uint16_t
         current_region += target_regions_per_iteration;

         // Sleep for a fixed interval
         std::this_thread::sleep_for(100ms);
      }
   }

   /**
    *  1. aggregate read stats from per-thread counters
    */
   void seg_allocator::compact_loop2()
   {
      auto ses = start_session();
      while (not _done.load(std::memory_order_relaxed))
      {
         if (compact_next_segment())
         {
            promote_rcache_data();
         }
         std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
   }
   void seg_allocator::compact_loop()
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
            auto ap  = _header->alloc_ptr.load(std::memory_order_relaxed);
            auto ep  = _header->end_ptr.load(std::memory_order_relaxed);
            if (min - ap <= 1 and (ep - ap) < 3)
            {
               auto seg = get_new_segment();
               munlock(seg.second, segment_size);
               madvise(seg.second, segment_size, MADV_RANDOM);
               seg.second->_alloc_pos.store(0, std::memory_order_relaxed);
               seg.second->_age = -1;

               _header->seg_meta[seg.first].clear();
               _header->free_seg_buffer[_header->end_ptr.load(std::memory_order_relaxed) &
                                        (max_segment_count - 1)] = seg.first;
               auto prev = _header->end_ptr.fetch_add(1, std::memory_order_release);
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

   void seg_allocator::promote_rcache_data()
   {
      uint32_t read_ids[1024];
      for (auto& rcache : _rcache_queues)
      {
         auto* queue = rcache.load(std::memory_order_acquire);
         if (not queue)
            break;
         auto state      = _compactor_session->lock();
         auto num_loaded = queue->pop(read_ids, 1024);

         //if (num_loaded > 0)
         //   ARBTRIE_DEBUG("num_loaded: ", num_loaded);
         for (uint32_t i = 0; i < num_loaded; ++i)
         {
            auto addr    = id_address::from_int(read_ids[i]);
            auto obj_ref = state.get(addr);
            if (auto [header, loc] = obj_ref.try_move_header(); header)
            {
               auto [new_loc, new_header] = _compactor_session->alloc_data(header->size(), addr);
               memcpy(new_header, header, header->size());

               if constexpr (update_checksum_on_compact)
               {
                  if (not new_header->has_checksum())
                     new_header->update_checksum();
               }

               if (node_meta_type::success == obj_ref.try_move(loc, new_loc))
               {
                  _total_promoted_bytes += header->size();
               }
               else
               {
                  // ARBTRIE_DEBUG("failed to move header");
               }
            }
            else
            {
               // ARBTRIE_DEBUG("failed to try_move_header");
            }
            obj_ref.meta().end_pending_cache();
         }
      }
   }

   bool seg_allocator::compact_next_segment()
   {
      if (not _compactor_session)
         _compactor_session.emplace(start_session());

      uint64_t most_empty_seg_num  = -1ll;
      uint64_t most_empty_seg_free = 0;
      auto     total_segs          = _block_alloc.num_blocks();
      auto     oldest              = -1ul;
      for (uint32_t s = 0; s < total_segs; ++s)
      {
         auto fso = _header->seg_meta[s].get_free_space_and_objs();
         if (fso.free_space > most_empty_seg_free)
            if (fso.free_space > segment_size / 8)  // most_empty_seg_free)
            {
               auto seg = get_segment(s);
               // only consider segs that are not actively allocing
               // or that haven't already been processed
               if (seg->_alloc_pos.load(std::memory_order_relaxed) == uint32_t(-1))
               {
                  //      if (seg->_age <= oldest)
                  {
                     most_empty_seg_num  = s;
                     most_empty_seg_free = fso.free_space;
                     oldest              = seg->_age;
                  }
               }
            }
      }

      // segments must be at least 25% empty before compaction is considered
      if (most_empty_seg_num == -1ull or most_empty_seg_free < segment_empty_threshold)
      {
         return false;
      }

      compact_segment(*_compactor_session, most_empty_seg_num);
      return true;
   }

   void seg_allocator::compact_segment(seg_alloc_session& ses, uint64_t seg_num)
   {
      auto state = ses.lock();
      auto s     = get_segment(seg_num);
      //  if( not s->_write_lock.try_lock() ) {
      //     ARBTRIE_WARN( "unable to get write lock while compacting!" );
      //     abort();
      //  }
      auto*        shead = (mapped_memory::segment_header*)s;
      auto         send  = (node_header*)((char*)s + segment_size);
      char*        foc   = (char*)s + round_up_multiple<64>(sizeof(mapped_memory::segment_header));
      node_header* foo   = (node_header*)(foc);

      if (debug_memory)
      {
         auto alloc_pos = ses._alloc_seg_ptr->_alloc_pos.load(std::memory_order_relaxed);
         ARBTRIE_DEBUG("compacting segment: ", seg_num, " into ", ses._alloc_seg_num, " ",
                       _header->seg_meta[ses._alloc_seg_num].get_free_space_and_objs().free_space);
         ARBTRIE_DEBUG("calloc: ", alloc_pos, " cfree: ",
                       _header->seg_meta[ses._alloc_seg_num].get_free_space_and_objs().free_space);
      }

      assert(s->_alloc_pos == segment_offset(-1));
      //   std::cerr << "seg " << seg_num <<" alloc pos: " << s->_alloc_pos <<"\n";

      auto seg_state = seg_num * segment_size;

      auto start_seg_ptr = ses._alloc_seg_ptr;
      auto start_seg_num = ses._alloc_seg_num;

      std::vector<std::pair<node_header*, temp_meta_type>> skipped;
      std::vector<node_header*>                            skipped_ref;
      std::vector<node_header*>                            skipped_try_start;

      auto& smeta    = _header->seg_meta[seg_num];
      auto  src_time = smeta._base_time.time_ms;

      madvise(s, segment_size, MADV_SEQUENTIAL);
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

         auto obj_size   = foo->size();
         auto [loc, ptr] = ses.alloc_data(obj_size, foo_address, src_time);

         if (obj_ref.try_start_move(obj_ref.loc())) [[likely]]
         {
            if (obj_ref.type() == node_type::binary)
            {
               copy_binary_node((binary_node*)ptr, (const binary_node*)foo);
            }
            else
            {
               memcpy(ptr, foo, obj_size);
            }
            if constexpr (update_checksum_on_compact)
            {
               if (not ptr->has_checksum())
                  ptr->update_checksum();
            }
            if constexpr (validate_checksum_on_compact)
            {
               if constexpr (update_checksum_on_modify)
               {
                  if (not ptr->has_checksum())
                     ARBTRIE_WARN("missing checksum detected: ", foo_address,
                                  " type: ", node_type_names[ptr->_ntype]);
               }
               if (not ptr->validate_checksum())
               {
                  ARBTRIE_WARN("invalid checksum detected: ", foo_address,
                               " checksum: ", foo->checksum, " != ", foo->calculate_checksum(),
                               " type: ", node_type_names[ptr->_ntype]);
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
            sync_segment(start_seg_num, sync_type::sync);
            start_seg_ptr = ses._alloc_seg_ptr;
            start_seg_num = ses._alloc_seg_num;
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
         _header->seg_meta[seg_num].set_last_sync_pos(start_seg_ptr->get_alloc_pos());
      }

      //   s->_write_lock.unlock();
      //   s->_num_objects = 0;
      s->_alloc_pos.store(0, std::memory_order_relaxed);
      s->_age = -1;
      // the segment we just cleared, so its free space and objects get reset to 0
      // and its last_sync pos gets put to the end because there is no need to sync it
      // because its data has already been synced by the compactor
      _header->seg_meta[seg_num].clear();

      // TODO: if I store the index in _mlocked then I don't have to search for it
      for (auto& ml : _mlocked)
         if (ml.load(std::memory_order_relaxed) == seg_num)
         {
            ml.store(-1, std::memory_order_relaxed);
            break;
         }

      munlock(s, segment_size);
      // it is unlikely to be accessed, and if it is don't pre-fetch
      madvise(s, segment_size, MADV_RANDOM);
      //madvise(s, segment_size, MADV_DONTNEED);

      // only one thread can move the end_ptr or this will break
      // std::cerr<<"done freeing end_ptr: " << _header->end_ptr.load() <<" <== " << seg_num <<"\n";

      assert(seg_num != segment_number(-1));
      _header->push_recycled_segment(seg_num);
   }

   /**
    * The min read pointer, aka min(R*), must be A <= R* <= E.
    * A, R, and E only ever increase
    * The last value of this function is stored in _min_read_ptr
    *
    * So long as the last value is greater than A, A can advance without
    * updating _min_read_ptr; however, if A >= _min_read_ptr then 
    * we want to check all active R* to find the min. If all sessions
    * are idle, the the min becomes E.
    *
    * Min automatically advances every time compactor pushes a new segment
    * to the end, but sometimes the compactor did its work while a read
    * lock was in place and once the read lock was released the min could
    * be updated.
    */
   uint64_t seg_allocator::get_min_read_ptr()
   {
      // alloc_ptr is safe to be relaxed because:
      // 1. It only ever increases monotonically
      // 2. We only use it as a lower bound check (ap >= min)
      // 3. If we read a stale value, it can only be lower than current,
      //    which means we might do an unnecessary min(R*) recalculation
      //    but will never miss a required one, and the data to
      // be read at the alloc position will be synchronized by the
      // acquire load of end_ptr below.
      auto ap = _header->alloc_ptr.load(std::memory_order_relaxed);

      // end_ptr protects the memory written when the compactor publishes
      // a new segment to the end of the queue, we must us acquire here to
      // synchronize with the compactor's view of the end of the queue.
      auto ep = _header->end_ptr.load(std::memory_order_acquire);

      // we can use relaxed ordering here - any stale read will be <= current value
      // any any memory synchronization required is handled by the acquire load
      // of end_ptr above
      auto min = _min_read_ptr.load(std::memory_order_relaxed);

      if (ap >= min and ep > min)  // then check to see if there is more
      {
         min = ep;
         // find new last min
         // TODO: only iterate over active sessions instead of all sessions
         // this is so infrequent it probably doesn't matter.
         auto fs      = ~_header->free_sessions.load();
         auto num_ses = std::popcount(fs);
         for (uint32_t i = 0; fs and i < max_session_count; ++i)
         {
            if (fs & (1ull << i))
            {
               if (uint32_t p = _header->session_lock_ptrs[i].load(std::memory_order_acquire);
                   p < min)
                  min = p;

               // we can't find anything lower than this
               if (min == ap)
               {
                  _min_read_ptr.store(min, std::memory_order_release);
                  return min;
               }
            }
         }
      }

      // when everything is unlocked, min is max uint64
      if (min > ep)
         min = ep;
      _min_read_ptr.store(min, std::memory_order_release);
      return min;
   }

   void seg_allocator::set_session_end_ptrs(uint32_t e)
   {
      auto     fs      = ~_header->free_sessions.load();
      auto     num_ses = std::popcount(fs);
      uint32_t min     = -1;
      for (uint32_t i = 0; fs and i < max_session_count; ++i)
      {
         if (fs & (1ull << i))
         {
            uint64_t p = _header->session_lock_ptrs[i].load(std::memory_order_relaxed);

            if (uint32_t(p) < min)
               min = uint32_t(p);

            p &= ~uint64_t(uint32_t(-1));  // clear the lower bits, to get accurate diff
            auto delta = (uint64_t(e) << 32) - p;
            assert((delta << 32) == 0);
            auto ep = _header->session_lock_ptrs[i].fetch_add(delta, std::memory_order_release);
         }
      }
      if (e > (1 << 20))
      {
         ARBTRIE_WARN(
             "TODO: looks like ALLOC P and END P need to be renormalized, they have wrapped the "
             "buffer too many times.");
      }

      if (min > e)  // only possible
         min = e;
      _min_read_ptr.store(min, std::memory_order_release);
   }

   /**
    *  reads allocator_header::reuse_ptr and if it is less than
    *  allocator_header::min_read_ptr then attempts increment the
    *  reuse pointer by exactly 1, if so then it uses the segment
    *  at _free_segments[reuse_ptr.old] 
    *
    *  If reuse_ptr == min_read_ptr then advance the alloc_ptr by
    *  segment_size to claim a new segment.
    */
   std::pair<segment_number, mapped_memory::segment_header*> seg_allocator::get_new_segment()
   {
      // ARBTRIE_DEBUG( " get new seg session min ptr: ", min );
      // ARBTRIE_WARN( "end ptr: ", _header->end_ptr.load(), " _header: ", _header );
      auto prepare_segment = [&](segment_number sn)
      {
         auto sp   = _block_alloc.get(sn);
         auto shp  = new (sp) mapped_memory::segment_header();
         shp->_age = _header->next_alloc_age.fetch_add(1, std::memory_order_relaxed);
         return std::pair<segment_number, mapped_memory::segment_header*>(sn, shp);
      };  // end prepare_segment

      if (auto seg = _header->pop_recycled_segment(get_min_read_ptr()))
         return prepare_segment(*seg);
      return prepare_segment(_block_alloc.alloc());

      // TODO... if min-ap == 0, but the compactor has set a flag indicating
      // that it is "sorting" a large queue of empty segments, then we wait for the
      // compactor to complete the sorting (should be quick compared to alloc() )
      // while the compactor is sorting, if it discovers the last segment(s) in the
      // file it can truncate the file and return space. This is useful for workloads
      // where the database grows and then shrinks.
   }

   /**
    when the compactor notices that empty free segment queue 
    has grown past a configured amount, it pops 80% of the queue,
    leaving some for quick allocs, it sorts the segments by
    their position in the file with the earliest in the file
    getting priority. It then pushes the lower 50% on to the queue,
    then pops 80% of the queue again 
        - this will grab the 20% left the first time + some of
          the items just pushed back... which represent the
          half of the queue that was earliest in the file.
        - we then sort what remains again.
        - at this point we should have identified the free
          segments that are at the end of the file, we can
          then truncate the file.
   */
   void seg_allocator::attempt_truncate_empty()
   {
      // Get the current size of the free segment queue and min read pointer
      auto ap         = _header->alloc_ptr.load(std::memory_order_relaxed);
      auto ep         = _header->end_ptr.load(std::memory_order_acquire);
      auto min_read   = get_min_read_ptr();
      auto queue_size = min_read - ap;  // Only consider segments up to min_read

      // Only proceed if queue has grown past configured threshold
      // TODO: Make this threshold configurable
      const uint32_t QUEUE_THRESHOLD = max_segment_count / 4;  // 25% of max segments
      if (queue_size <= QUEUE_THRESHOLD)
      {
         return;
      }

      // Calculate how many segments to process in this batch (80% of processable queue)
      auto                        batch_size = (queue_size * 8) / 10;
      std::vector<segment_number> segments_to_sort;
      segments_to_sort.reserve(batch_size);

      // Pop 80% of segments from queue, but only up to min_read
      for (uint32_t i = 0; i < batch_size && (ap + i) < min_read; ++i)
      {
         auto seg_num = _header->free_seg_buffer[(ap + i) & (max_segment_count - 1)];
         if (seg_num != segment_number(-1))
         {
            segments_to_sort.push_back(seg_num);
         }
      }

      // Sort segments by their position in the file (lower segment numbers first)
      std::sort(segments_to_sort.begin(), segments_to_sort.end());

      // Push back lower 50% to the queue
      auto segments_to_keep = segments_to_sort.size() / 2;
      for (size_t i = 0; i < segments_to_keep; ++i)
      {
         _header->free_seg_buffer[(_header->alloc_ptr.load(std::memory_order_relaxed) + i) &
                                  (max_segment_count - 1)] = segments_to_sort[i];
      }
      _header->alloc_ptr.fetch_add(segments_to_keep, std::memory_order_release);

      // Pop 80% again to get earliest segments in file
      auto remaining_size = min_read - _header->alloc_ptr.load(std::memory_order_relaxed);
      batch_size          = (remaining_size * 8) / 10;
      segments_to_sort.clear();
      segments_to_sort.reserve(batch_size);

      ap = _header->alloc_ptr.load(std::memory_order_relaxed);
      for (uint32_t i = 0; i < batch_size && (ap + i) < min_read; ++i)
      {
         auto seg_num = _header->free_seg_buffer[(ap + i) & (max_segment_count - 1)];
         if (seg_num != segment_number(-1))
         {
            segments_to_sort.push_back(seg_num);
         }
      }

      // Sort again to identify segments at end of file
      std::sort(segments_to_sort.begin(), segments_to_sort.end());

      // If we have contiguous segments at the end of the file, we can truncate
      if (!segments_to_sort.empty())
      {
         auto highest_seg = segments_to_sort.back();
         auto total_segs  = _block_alloc.num_blocks();

         // Check if highest segment is at the end of the file
         if (highest_seg == total_segs - 1)
         {
            // Count how many contiguous segments we have from the end
            size_t contiguous_count = 1;
            for (int i = segments_to_sort.size() - 2; i >= 0; --i)
            {
               if (segments_to_sort[i] == highest_seg - contiguous_count)
               {
                  contiguous_count++;
               }
               else
               {
                  break;
               }
            }

            // Truncate the file by the number of contiguous segments found
            if (contiguous_count > 0)
            {
               // TODO: _block_alloc.truncate(total_segs - contiguous_count);
            }
         }
      }
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
      auto last_sync  = _header->seg_meta[s].get_last_sync_pos();
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
         _header->seg_meta[s].set_last_sync_pos(last_alloc);
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

      // Gather segment information
      for (uint32_t i = 0; i < total_segs; ++i)
      {
         auto  seg                     = get_segment(i);
         auto& meta                    = _header->seg_meta[i];
         auto  space_objs              = meta.get_free_space_and_objs();
         auto [read_nodes, read_bytes] = calculate_segment_read_stats(i);

         seg_alloc_dump::segment_info seg_info;
         seg_info.segment_num   = i;
         seg_info.freed_percent = int(100 * double(space_objs.free_space) / segment_size);
         seg_info.freed_bytes   = space_objs.free_space;
         seg_info.freed_objects = space_objs.free_objects;
         seg_info.alloc_pos     = (seg->_alloc_pos == -1 ? -1 : seg->get_alloc_pos());
         seg_info.is_alloc      = space_objs.is_alloc;
         seg_info.is_pinned     = space_objs.is_pinned;
         seg_info.age           = seg->_age;
         seg_info.read_nodes    = read_nodes;
         seg_info.read_bytes    = read_bytes;

         result.segments.push_back(seg_info);
         result.total_free_space += space_objs.free_space;
         result.total_read_nodes += read_nodes;
         result.total_read_bytes += read_bytes;
      }

      // Gather session information
      auto fs                = ~_header->free_sessions.load();
      result.active_sessions = std::popcount(fs);

      for (uint32_t i = 0; i < max_session_count; ++i)
      {
         if (fs & (1ull << i))
         {
            seg_alloc_dump::session_info session;
            session.session_num = i;
            auto p              = _header->session_lock_ptrs[i].load();
            session.is_locked   = (uint32_t(p) != uint32_t(-1));
            session.read_ptr    = uint32_t(p);
            result.sessions.push_back(session);
         }
      }

      // Gather pending segments information
      for (auto x = _header->alloc_ptr.load(); x < _header->end_ptr.load(); ++x)
      {
         seg_alloc_dump::pending_segment pending;
         pending.index       = x;
         pending.segment_num = _header->free_seg_buffer[x & (max_segment_count - 1)];
         result.pending_segments.push_back(pending);
      }

      result.alloc_ptr          = _header->alloc_ptr.load();
      result.end_ptr            = _header->end_ptr.load();
      result.free_release_count = _id_alloc.free_release_count();

      return result;
   }

   std::pair<uint32_t, uint64_t> seg_allocator::calculate_segment_read_stats(segment_number seg_num)
   {
      uint32_t nodes_with_read_bit = 0;
      uint64_t total_bytes         = 0;

      auto        seg  = get_segment(seg_num);
      auto        send = (node_header*)((char*)seg + segment_size);
      const char* foc =
          (const char*)seg + round_up_multiple<64>(sizeof(mapped_memory::segment_header));
      node_header* foo = (node_header*)(foc);

      while (foo < send && foo->address())
      {
         // Get the object reference for this node
         auto  foo_address = foo->address();
         auto& obj_ref     = _id_alloc.get(foo_address);

         // Check if the read bit is set and if the location matches
         if (obj_ref.is_read())
         {
            auto foo_idx     = (char*)foo - (char*)seg;
            auto current_loc = obj_ref.loc();

            // Only count if the object reference is pointing to this exact node
            if (current_loc.to_abs() == seg_num * segment_size + foo_idx)
            {
               nodes_with_read_bit++;
               total_bytes += foo->size();
            }
         }

         foo = foo->next();
      }

      return {nodes_with_read_bit, total_bytes};
   }

};  // namespace arbtrie
