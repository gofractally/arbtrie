#include <sal/allocator.hpp>
#include <sal/allocator_session.hpp>
#include <sal/mapped_memory/read_lock_queue.hpp>
#include <sal/time.hpp>
#include <ucc/round.hpp>

namespace sal
{

   allocator_session::allocator_session(allocator& a, allocator_session_number ses_num)
       : _block_base_ptr(a._block_alloc.get(offset_ptr(0))),
         _control_block_base_ptr(a._ptr_alloc._ptr_base),
         _session_rlock(a.get_session_rlock(ses_num)),
         _nested_read_lock(0),
         _rcache_queue(a.get_rcache_queue(ses_num)),
         _sega(a),
         _session_num(ses_num),
         _session_rng(0xABBA7777 ^ *ses_num),
         _ptr_alloc(a._ptr_alloc),
         _alloc_seg_num(-1),
         _alloc_seg_ptr(nullptr),
         _dirty_segments(a._mapped_state->_session_data.dirty_segments(ses_num)),
   {
      _block_base_ptr = a._block_alloc.get(offset_ptr(0));
   }

   allocator_session::~allocator_session()
   {
      if (_session_num == allocator_session_number(-1))
         return;
      finalize_active_segment();
      _sega.release_session_num(_session_num);
      _session_num = allocator_session_number(-1);
   }

   void allocator_session::finalize_active_segment()
   {
      if (_alloc_seg_ptr and not _alloc_seg_ptr->is_finalized())
      {
         _alloc_seg_ptr->finalize();
         _dirty_segments.push(_alloc_seg_num);
      }
      _alloc_seg_ptr = nullptr;
      _alloc_seg_num = segment_number(-1);
   }

   void allocator_session::init_active_segment()
   {
      auto [num, ptr] = _sega.get_new_segment(_alloc_to_pinned);
      _alloc_seg_num  = num;
      _alloc_seg_ptr  = ptr;

      // Initialize segment header with session information
      _alloc_seg_ptr->_session_id = _session_num;
      _alloc_seg_ptr->_seg_sequence =
          _sega._mapped_state->_session_data.next_session_segment_seq(_session_num);
      _alloc_seg_ptr->_open_time_usec  = sal::get_current_time_msec();
      _alloc_seg_ptr->_close_time_usec = 0;  // Will be set when segment is closed
   }

   /**
    * Reclaims the most recently allocated size bytes
    * 
    * @param size The size of the object to reclaim
    * @return true if the object was reclaimed, false otherwise
    */
   bool allocator_session::unalloc(uint32_t size)
   {
      if (not _alloc_seg_ptr) [[unlikely]]
         return false;

      assert(reinterpret_cast<alloc_header*>(_alloc_seg_ptr->data +
                                             _alloc_seg_ptr->get_alloc_pos() - size)
                 ->size() == size);

      _alloc_seg_ptr->unalloc(size);
      return true;
   }

   uint64_t allocator_session::count_ids_with_refs()
   {
      return _sega.count_ids_with_refs();
   }

   void allocator_session::sync(sync_type st, const runtime_config& cfg, std::span<char> user_data)
   {
      //auto           st      = _sega._mapped_state->_config.sync_mode;
      segment_number seg_num = _alloc_seg_num;
      if (seg_num != segment_number(-1))
         seg_num = _dirty_segments.pop();
      //ARBTRIE_WARN("sync: pop() => seg_num: ", seg_num);
      while (seg_num != segment_number(-1))
      {
         auto seg = _sega.get_segment(seg_num);
         //        ARBTRIE_WARN("sync: seg_num: ", seg_num, " is_finalized: ", seg->is_finalized());

         // sync() will write an allocation_header, and this header is space that
         // can be reclaimed if compacted, so we need to record it in the meta data
         // for easy access by the compactor
         _sega.record_session_write(_session_num, seg->sync(st, cfg, user_data));
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
}  // namespace sal
