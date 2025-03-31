#include <arbtrie/seg_allocator.hpp>
#include <cassert>

namespace arbtrie
{
   seg_alloc_session::seg_alloc_session(seg_alloc_session&& mv)
       : _session_num(mv._session_num),
         _alloc_seg_num(mv._alloc_seg_num),
         _alloc_seg_ptr(mv._alloc_seg_ptr),
         //_alloc_seg_meta(mv._alloc_seg_meta),
         _dirty_segments(mv._dirty_segments),
         _in_alloc(mv._in_alloc),
         _session_rlock(mv._session_rlock),
         _sega(mv._sega),
         _nested_read_lock(mv._nested_read_lock),
         _rcache_queue(mv._rcache_queue),
         _session_rng(mv._session_rng)
   {
      mv._alloc_seg_ptr = nullptr;
      mv._alloc_seg_num = -1ull;
      //   mv._alloc_seg_meta = nullptr;
      mv._session_num = -1;
   }

   seg_alloc_session::seg_alloc_session(seg_allocator& a, uint32_t ses_num)
       : _session_num(ses_num),
         _alloc_seg_num(-1ull),
         _alloc_seg_ptr(nullptr),
         // _alloc_seg_meta(nullptr),
         _dirty_segments(a._mapped_state->_session_data.dirty_segments(ses_num)),
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
      if (_alloc_seg_ptr and not _alloc_seg_ptr->is_finalized())
      {
         auto cur_apos = _alloc_seg_ptr->get_alloc_pos();
         // ARBTRIE_INFO("finalize_active_segment: ", _alloc_seg_num, " cur_apos: ", cur_apos,
         //              " free_space: ", _alloc_seg_ptr->free_space());
         // update the segment meta data so that it knows the free space and
         // age of the segment data for use by compactor
         // _alloc_seg_meta->finalize_segment(
         //    _alloc_seg_ptr->free_space(),
         //   _alloc_seg_ptr->_vage_accumulator.average_age());  // not alloc

         // Set the segment close time before finalization
         // ensure the alloc_pos is moved to end
         _alloc_seg_ptr->finalize();
         assert(_alloc_seg_ptr->is_finalized());

         // ARBTRIE_WARN("finalize_active_segment: ", _alloc_seg_num, " cur_apos: ", cur_apos,
         //              " free_space: ", _alloc_seg_ptr->free_space());
         _dirty_segments.push(_alloc_seg_num);
      }
      _alloc_seg_ptr = nullptr;
      _alloc_seg_num = -1ull;
      // _alloc_seg_meta = nullptr;
   }

   void seg_alloc_session::init_active_segment()
   {
      auto [num, ptr] = _sega.get_new_segment(_alloc_to_pinned);
      _alloc_seg_num  = num;
      _alloc_seg_ptr  = ptr;
      //    _alloc_seg_meta = &_sega.get_segment_meta(_alloc_seg_num);
      //    _alloc_seg_meta->start_alloc_segment();

      // Initialize segment header with session information
      _alloc_seg_ptr->_session_id = _session_num;
      _alloc_seg_ptr->_seg_sequence =
          _sega._mapped_state->_session_data.next_session_segment_seq(_session_num);
      _alloc_seg_ptr->_open_time_usec  = arbtrie::get_current_time_ms();
      _alloc_seg_ptr->_close_time_usec = 0;  // Will be set when segment is closed
   }

   /**
    * Gets the modify lock when failrue is not an option 
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
    */

   /**
    * Most of the time locks are wait free, but the allocator occasionally
    * conflicts with a syncing thread and we must wait for the sync to 
    * complete before we can allocate again, note that the sync lock period
    * is very short.
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
    */

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

      if (not _alloc_seg_ptr) [[unlikely]]
         init_active_segment();

      if (not _alloc_seg_ptr->can_alloc(size)) [[unlikely]]
      {
         finalize_active_segment();  // final bookkeeping before getting a new segment
         init_active_segment();      // get a new segment
      }

      // Update the vage_accumulator with the current allocation
      /// TODO: make the caller provide the vage value rather than force the condition on every call
      _alloc_seg_ptr->_vage_accumulator.add(size, vage ? vage : arbtrie::get_current_time_ms());

      auto head = _alloc_seg_ptr->alloc<node_header>(size, adr_seq, node_type::freelist);

      auto loc = _alloc_seg_num * segment_size + ((char*)head - _alloc_seg_ptr->data);

      return {node_location::from_absolute_address(loc), head};
   }

   /**
    * Reclaims the most recently allocated size bytes
    * 
    * @param size The size of the object to reclaim
    * @return true if the object was reclaimed, false otherwise
    */
   bool seg_alloc_session::unalloc(uint32_t size)
   {
      if (not _alloc_seg_ptr) [[unlikely]]
         return false;

      assert(reinterpret_cast<node_header*>(_alloc_seg_ptr->data + _alloc_seg_ptr->get_alloc_pos() -
                                            size)
                 ->_nsize == size);

      _alloc_seg_ptr->unalloc(size);
      return true;
   }

   void seg_alloc_session::sync(int top_root_index, id_address top_root)
   {
      auto st      = _sega._mapped_state->_config.sync_mode;
      int  seg_num = _alloc_seg_num;
      if (seg_num != -1)
         seg_num = _dirty_segments.pop();
      //ARBTRIE_WARN("sync: pop() => seg_num: ", seg_num);
      while (seg_num != -1)
      {
         auto seg = _sega.get_segment(seg_num);
         //        ARBTRIE_WARN("sync: seg_num: ", seg_num, " is_finalized: ", seg->is_finalized());

         // sync() will write an allocation_header, and this header is space that
         // can be reclaimed if compacted, so we need to record it in the meta data
         // for easy access by the compactor
         _sega.record_session_write(_session_num, seg->sync(st, top_root_index, top_root));
         auto ahead = seg->get_last_aheader();
         //   ARBTRIE_INFO("sync: segment: ", seg_num, " free_space: ", seg->free_space(),
         //                " ahead: ", ahead->_nsize);

         _sega.record_freed_space(_session_num, seg_num, ahead);

         /// if the segment is entirely read only (because sync() just moved _last_writable_pos
         /// to the end of the segment) then we can set the segment meta data to indicate that
         /// the entire segment is read only
         assert(seg->is_finalized() ? seg->is_read_only() : true);
         if (seg->is_read_only())  //or seg->is_finalized())
            _sega._mapped_state->_segment_data.prepare_for_compaction(
                seg_num, seg->_vage_accumulator.average_age());
         seg_num = _dirty_segments.pop();
      }
      if (st == sync_type::fsync)
         _sega.fsync();
   }

   uint64_t seg_alloc_session::count_ids_with_refs()
   {
      return _sega.count_ids_with_refs();
   }
}  // namespace arbtrie
