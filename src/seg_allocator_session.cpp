#include <arbtrie/seg_allocator.hpp>
#include <cassert>

namespace arbtrie
{
   seg_alloc_session::seg_alloc_session(seg_alloc_session&& mv)
       : _session_num(mv._session_num),
         _alloc_seg_num(mv._alloc_seg_num),
         _alloc_seg_ptr(mv._alloc_seg_ptr),
         _alloc_seg_meta(mv._alloc_seg_meta),
         _in_alloc(mv._in_alloc),
         _session_rlock(mv._session_rlock),
         _sega(mv._sega),
         _nested_read_lock(mv._nested_read_lock),
         _rcache_queue(mv._rcache_queue),
         _session_rng(mv._session_rng)
   {
      mv._alloc_seg_ptr  = nullptr;
      mv._alloc_seg_num  = -1ull;
      mv._alloc_seg_meta = nullptr;
      mv._session_num    = -1;
   }

   seg_alloc_session& seg_alloc_session::operator=(seg_alloc_session&& mv)
   {
      // We can't really move-assign this class correctly because it contains references
      // that cannot be reassigned. This implementation will just cleanup and mark invalid.

      if (this != &mv)
      {
         // Clean up current resources if we have a valid session
         if (_session_num != -1)
         {
            // Handle allocation segment cleanup
            if (_alloc_seg_ptr)
            {
               if (segment_size - _alloc_seg_ptr->_alloc_pos >= sizeof(node_header))
               {
                  memset(((char*)_alloc_seg_ptr) + _alloc_seg_ptr->_alloc_pos, 0,
                         sizeof(node_header));  // mark last object
               }
               if (_alloc_seg_meta)
                  _alloc_seg_meta->free(segment_size - _alloc_seg_ptr->_alloc_pos);
               _alloc_seg_ptr->_alloc_pos = uint32_t(-1);
            }

            // Release the session number back to the allocator
            _sega.release_session_num(_session_num);
         }

         // Take over the transferable non-reference members
         _session_num      = mv._session_num;
         _alloc_seg_num    = mv._alloc_seg_num;
         _alloc_seg_ptr    = mv._alloc_seg_ptr;
         _alloc_seg_meta   = mv._alloc_seg_meta;
         _in_alloc         = mv._in_alloc;
         _nested_read_lock = mv._nested_read_lock;
         _session_rng      = mv._session_rng;
         // Reference members can't be reassigned, so just making sure we're
         // consistent with expectations, but not moving them
         // (_session_rlock and _rcache_queue remain unchanged)

         // Clear resources in the moved-from object
         mv._alloc_seg_ptr  = nullptr;
         mv._alloc_seg_num  = -1ull;
         mv._alloc_seg_meta = nullptr;
         // Set session_num to -1 to mark as moved-from and prevent double-release
         mv._session_num = -1;
      }
      return *this;
   }

   seg_alloc_session::seg_alloc_session(seg_allocator& a, uint32_t ses_num)
       : _session_num(ses_num),
         _alloc_seg_num(-1ull),
         _alloc_seg_ptr(nullptr),
         _alloc_seg_meta(nullptr),
         _in_alloc(false),
         _session_rng(0xABBA7777 ^ ses_num),
         _session_rlock(a.get_session_rlock(ses_num)),
         _sega(a),
         _nested_read_lock(0),
         _rcache_queue(a.get_rcache_queue(ses_num))
   {
   }

   seg_alloc_session::~seg_alloc_session()
   {
      if (_session_num == -1)
         return;
      finalize_active_segment();
      _sega.release_session_num(_session_num);
      _session_num = -1;
   }

   void seg_alloc_session::finalize_active_segment()
   {
      if (_alloc_seg_ptr)
      {
         auto cur_apos = _alloc_seg_ptr->_alloc_pos.load(std::memory_order_relaxed);
         ARBTRIE_INFO("finalize_active_segment: ", _alloc_seg_num, " cur_apos: ", cur_apos,
                      " free_space: ", _alloc_seg_ptr->free_space());
         // update the segment meta data so that it knows the free space and
         // age of the segment data for use by compactor
         _alloc_seg_meta->finalize_segment(
             _alloc_seg_ptr->free_space(),
             _alloc_seg_ptr->_vage_accumulator.average_age());  // not alloc

         // Set the segment close time before finalization
         // ensure the alloc_pos is moved to end
         _alloc_seg_ptr->finalize();
         assert(_alloc_seg_ptr->is_finalized());

         _sega.push_dirty_segment(_alloc_seg_num);
         _alloc_seg_ptr  = nullptr;
         _alloc_seg_num  = -1ull;
         _alloc_seg_meta = nullptr;
      }
   }

   void seg_alloc_session::init_active_segment()
   {
      auto [num, ptr] = _sega.get_new_segment(_alloc_to_pinned);
      _alloc_seg_num  = num;
      _alloc_seg_ptr  = ptr;
      _alloc_seg_meta = &_sega.get_segment_meta(_alloc_seg_num);
      _alloc_seg_meta->start_alloc_segment();

      // Initialize segment header with session information
      _alloc_seg_ptr->_session_id = _session_num;
      _alloc_seg_ptr->_seg_sequence =
          _sega._mapped_state->_session_data.next_session_segment_seq(_session_num);
      _alloc_seg_ptr->_open_time_usec  = arbtrie::get_current_time_ms();
      _alloc_seg_ptr->_close_time_usec = 0;  // Will be set when segment is closed
      // TODO: release the modify lock and get a new segment
   }

   /**
    * Gets the modify lock when failrue is not an option 
    */
   void seg_alloc_session::assert_modify_segment(segment_number seg_num)
   {
      bool got_modify_lock = try_modify_segment(seg_num);
      assert(got_modify_lock);
      if constexpr (debug_memory)
      {
         if (not got_modify_lock)
         {
            ARBTRIE_WARN(" SEGMENT LOCKED BY SYNC THREAD");
            abort();
         }
      }
   }
   /**
    * Most of the time locks are wait free, but the allocator occasionally
    * conflicts with a syncing thread and we must wait for the sync to 
    * complete before we can allocate again, note that the sync lock period
    * is very short.
    */
   void seg_alloc_session::lock_alloc_segment()
   {
      if (not _alloc_seg_ptr or _alloc_seg_ptr->is_finalized()) [[unlikely]]
      {
         init_active_segment();
         assert_modify_segment(_alloc_seg_num);
         return;
      }
      // wait for the sync thread to release the modify lock,
      // this is a rare case, but it can happen if the sync thread
      // is trying to sync this alloc segment while another thread
      // is actively allocation to it. The logic for finalizing the
      // segment is too complex for atomics, and adding mutex per
      // segment felt too heavy because it would be another "lock"
      // primitive protecting something that is already protected
      // by another means.
      while (not try_modify_segment(_alloc_seg_num)) [[unlikely]]
         std::this_thread::sleep_for(std::chrono::microseconds(1));

      // below this point we have the modify lock on the segment

      // has the segment been finalized since we last wrote to it?
      if (_alloc_seg_ptr->is_finalized()) [[unlikely]]
      {
         // if so, we can't modify it and must get a new segment
         end_modify();
         init_active_segment();
         assert_modify_segment(_alloc_seg_num);
         return;
      }
      auto first_write_pos = _alloc_seg_ptr->get_first_write_pos();
      auto alloc_pos       = _alloc_seg_ptr->get_alloc_pos();

      // advance to the first writable position if we are not already there
      if (alloc_pos < first_write_pos)
         _alloc_seg_ptr->set_alloc_pos(first_write_pos);
   }

   /**
    * Allocates a node in the active segment and returns a pointer to the node, the
    * modify lock must be released after all writes are done on the allocated node.
    * 
    * The caller does not know what segment this will end up written to, so it cannot
    * write lock in advance. 
    * 
    * @param size, must be a multiple of 64
    */
   std::pair<node_location, node_header*> seg_alloc_session::alloc_data(uint32_t       size,
                                                                        id_address_seq adr_seq,
                                                                        uint64_t       vage)
   {
      assert(size < sizeof(mapped_memory::segment::data));
      assert(size == round_up_multiple<64>(size));
      const auto rounded_size = size;

      // ensure that a segment exists and the sync thread knows we are modifying it
      lock_alloc_segment();

      // find out where we are allocating from
      char* cur_pos = _alloc_seg_ptr->alloc_ptr();

      // B - if there isn't enough space, notify compactor go to A
      if (cur_pos + rounded_size > _alloc_seg_ptr->end()) [[unlikely]]
      {
         /// TODO: this could be a problem if the last page has
         /// already been locked... we could easily get locked out
         /// of our ability to clean up if we allow sync() threads
         /// to lock us down without a way to finalize... unless,
         /// the threads doing the mprotect() have the responsibility
         /// to finalize the segment *after* they have acquired the
         /// modify lock.
         finalize_active_segment();              // final bookkeeping before getting a new segment
         end_modify();                           // release the modify lock on the old segment
         init_active_segment();                  // get a new segment
         assert_modify_segment(_alloc_seg_num);  // this should always succeed
         cur_pos = _alloc_seg_ptr->alloc_ptr();
      }

      // at this point we have the modify lock on the segment

      // Update the vage_accumulator with the current allocation
      /// TODO: make the caller provide the vage value rather than force the condition on every call
      _alloc_seg_ptr->_vage_accumulator.add(size, vage ? vage : arbtrie::get_current_time_ms());

      auto head = (node_header*)cur_pos;
      // Create node_header with sequence number passed to constructor
      head = new (head) node_header(size, adr_seq, node_type::freelist);

      auto next_alloc_pos = cur_pos + rounded_size;
      auto alloc_idx      = _alloc_seg_ptr->set_alloc_ptr(next_alloc_pos);

      auto loc = _alloc_seg_num * segment_size + (cur_pos - _alloc_seg_ptr->data);

      // TODO: save tombstone incase we have to advance the alloc_ptr due to
      // a failed attempt to get the modify lock on the page we were partially
      // through writing to.
      return {node_location::from_absolute(loc), head};
   }

   void seg_alloc_session::unalloc(uint32_t size)
   {
      ARBTRIE_INFO("unalloc: size=", size);
      auto rounded_size = round_up_multiple<64>(size);
      if (_alloc_seg_ptr) [[likely]]
      {
         auto cap = _alloc_seg_ptr->_alloc_pos.load(std::memory_order_relaxed);
         if (cap and cap < segment_size) [[likely]]
         {
            auto cur_apos =
                _alloc_seg_ptr->_alloc_pos.fetch_sub(rounded_size, std::memory_order_relaxed);
            cur_apos -= rounded_size;
            /// TODO: replace this with the proper signal in event write protection
            ///       is enforced on this segment.
            memset(((char*)_alloc_seg_ptr) + cur_apos, 0, sizeof(node_header));
         }
         //_alloc_seg_ptr->_num_objects--;
      }
   }

   void seg_alloc_session::sync(sync_type st)
   {
      _sega.push_dirty_segment(_alloc_seg_num);
      _sega.sync(st);
   }

   uint64_t seg_alloc_session::count_ids_with_refs()
   {
      return _sega.count_ids_with_refs();
   }
}  // namespace arbtrie
