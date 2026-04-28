#pragma once
#include <sal/allocator_session_impl.hpp>

namespace sal
{
   inline void allocator::release(ptr_address adr) noexcept
   {
      get_session()->release(adr);
   }
   /**
        * When an object is moved its space is freed and we need to record the freed space
        * so the compactor has the metadata it needs to efficiently identify segments that
        * can be compacted.
        * 
        * @param obj The object on the segment being freed
        * @param seg The segment number containing the object
        */
   template <typename T>
   inline void allocator::record_freed_space(allocator_session_number /*ses_num*/, T* obj,
                                              const char* tag)
   {
      auto seg_num = get_segment_for_object(obj);
      auto* seg    = get_segment(seg_num);
      // alloc_pos is read INSIDE add_freed_space under SAL_TRACK_LOCK to
      // avoid a race with concurrent main-thread allocation.
      _mapped_state->_segment_data.add_freed_space(seg_num, seg, obj, tag);
   }
}  // namespace sal