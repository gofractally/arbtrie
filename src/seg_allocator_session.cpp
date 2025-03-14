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
         auto cur_apos   = _alloc_seg_ptr->_alloc_pos.load(std::memory_order_relaxed);
         auto free_space = segment_size - cur_apos;

         if (free_space >= sizeof(node_header))
         {
            assert(cur_apos + sizeof(uint64_t) <= segment_size);
            memset(((char*)_alloc_seg_ptr) + cur_apos, 0, sizeof(node_header));
         }

         // Set the segment close time before finalization
         _alloc_seg_ptr->_close_time_usec = arbtrie::get_current_time_ms();

         _alloc_seg_meta->finalize_segment(
             segment_size - _alloc_seg_ptr->_alloc_pos,
             _alloc_seg_ptr->_vage_accumulator.average_age());  // not alloc

         _alloc_seg_ptr->_alloc_pos.store(uint32_t(-1), std::memory_order_release);
         _sega.push_dirty_segment(_alloc_seg_num);
         _alloc_seg_ptr  = nullptr;
         _alloc_seg_num  = -1ull;
         _alloc_seg_meta = nullptr;
      }
   }

   std::pair<node_location, node_header*> seg_alloc_session::alloc_data(uint32_t       size,
                                                                        id_address_seq adr_seq,
                                                                        uint64_t       vage)
   {
      assert(size < segment_size - round_up_multiple<64>(sizeof(mapped_memory::segment_header)));
      // A - if no segment get a new segment
      if (not _alloc_seg_ptr or
          _alloc_seg_ptr->_alloc_pos.load(std::memory_order_relaxed) > segment_size) [[unlikely]]
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
      }
      if (not vage)
      {
         vage = arbtrie::get_current_time_ms();
      }

      auto* sh           = _alloc_seg_ptr;
      auto  rounded_size = round_up_multiple<64>(size);

      auto cur_apos = sh->_alloc_pos.load(std::memory_order_relaxed);

      auto spec_pos   = uint64_t(cur_apos) + rounded_size;
      auto free_space = segment_size - cur_apos;

      // B - if there isn't enough space, notify compactor go to A
      if (spec_pos > (segment_size - sizeof(node_header))) [[unlikely]]
      {
         finalize_active_segment();
         return alloc_data(size, adr_seq, vage);  // recurse
      }

      // Update the vage_accumulator with the current allocation
      _alloc_seg_ptr->_vage_accumulator.add(size, vage);

      auto obj  = ((char*)sh) + sh->_alloc_pos.load(std::memory_order_relaxed);
      auto head = (node_header*)obj;
      // Create node_header with sequence number passed to constructor
      head = new (head) node_header(size, adr_seq, node_type::freelist);

      auto new_alloc_pos =
          rounded_size + sh->_alloc_pos.fetch_add(rounded_size, std::memory_order_relaxed);

      auto loc = _alloc_seg_num * segment_size + cur_apos;

      return {node_location::from_absolute(loc), head};
   }

   void seg_alloc_session::unalloc(uint32_t size)
   {
      auto rounded_size = round_up_multiple<64>(size);
      if (_alloc_seg_ptr) [[likely]]
      {
         auto cap = _alloc_seg_ptr->_alloc_pos.load(std::memory_order_relaxed);
         if (cap and cap < segment_size) [[likely]]
         {
            auto cur_apos =
                _alloc_seg_ptr->_alloc_pos.fetch_sub(rounded_size, std::memory_order_relaxed);
            cur_apos -= rounded_size;
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
