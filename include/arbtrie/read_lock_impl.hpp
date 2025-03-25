#pragma once
#include <arbtrie/fast_memcpy.hpp>
#include <arbtrie/read_lock.hpp>
#include <arbtrie/seg_alloc_session.hpp>

namespace arbtrie
{

   /// @pre refcount of id is 1
   inline object_ref read_lock::realloc(object_ref& oref, uint32_t size, node_type type, auto init)
   {
      auto adr = oref.address();
      auto l   = oref.loc();

      auto obj_ptr = _session._sega._block_alloc.get<node_header>(l.offset());

      _session.record_freed_space(get_segment_num(l), obj_ptr);

      //   std::cerr << "realloc " << id <<" size: " << size <<" \n";
      // TODO: mark the free space associated with the current location of id
      assert(size >= sizeof(node_header));
      assert(type != node_type::undefined);
      assert(obj_ptr->address() == adr);

      // alloc_data() starts a modify lock on the allocation segment, which
      // which must be released by calling end_modify() after all writes are done
      auto [loc, node_ptr] = _session.alloc_data(size, obj_ptr->address_seq());

      init(node_ptr);
      if (_session._sega.config_update_checksum_on_modify())
         node_ptr->update_checksum();

      assert(type == node_type::value or bool(node_ptr->_branch_id_region));
      assert(node_ptr->_nsize == size);
      assert(node_ptr->_ntype == type);
      assert(node_ptr->_node_id == adr);

      oref.move(loc, std::memory_order_release);
      return oref;
   }

   inline id_allocation read_lock::get_new_meta_node(id_region reg)
   {
      return _session._sega._id_alloc.alloc(reg);
   }

   inline object_ref read_lock::alloc(id_region reg, uint32_t size, node_type type, auto init)
   {
      assert(size >= sizeof(node_header));
      assert(type != node_type::undefined);

      auto allocation = _session._sega._id_alloc.alloc(reg);

      // alloc_data() starts a modify lock on the allocation segment, which
      // which must be released by calling end_modify() after all writes are done
      auto [loc, node_ptr] =
          _session.alloc_data(size, id_address_seq(allocation.address, allocation.sequence));
      //ARBTRIE_WARN( "alloc id: ", id, " type: " , node_type_names[type], " loc: ", loc._offset, " size: ", size);

      init(node_ptr);
      if (_session._sega.config_update_checksum_on_modify())
         node_ptr->update_checksum();

      assert(type == node_type::value or bool(node_ptr->_branch_id_region));

      allocation.ptr->store(temp_meta_type().set_loc(loc).set_ref(1), std::memory_order_release);

      assert(node_ptr->_nsize == size);
      assert(node_ptr->_ntype == type);
      assert(node_ptr->_node_id == allocation.address);

      return object_ref(*this, allocation.address, *allocation.ptr);
   }

   /**
   inline bool read_lock::is_synced(node_location loc)
   {
      return _session.is_synced(loc);
   }
   */

   inline node_header* read_lock::get_node_pointer(node_location loc)
   {
      if constexpr (debug_memory)
      {
         // if alloc_pos > loc.index() then we haven't overwriten this object yet, we are accessing
         // data behind the alloc pointer which should be safe
         // to access data we had to get the location from obj id database and we should read
         // with memory_order_acquire, when updating an object_info we need to write with
         // memory_order_release otherwise the data written may not be visible yet to the reader coming
         // along behind
         auto ptr = _session._sega._block_alloc.get<mapped_memory::segment>(
             block_number(get_segment_num(loc)));
         auto ap = ptr->get_alloc_pos();
         if (ap <= get_segment_offset(loc))
         {
            ARBTRIE_WARN("segment: ", get_segment_num(loc), " ap: ", ap,
                         "  loc: ", get_segment_offset(loc));
            abort();
         }
      }
      return _session._sega._block_alloc.get<node_header>(loc.offset());
   }

   inline void read_lock::free_meta_node(id_address a)
   {
      _session._sega._id_alloc.free(a);
   }

   inline id_region read_lock::get_new_region()
   {
      return _session._sega._id_alloc.get_new_region();
   }
   inline object_ref read_lock::get(id_address adr)
   {
      return object_ref(*this, adr, _session._sega._id_alloc.get(adr));
   }

   // returned mutable T is only valid while modify lock is in scope
   // TODO: compile time optimziation or a state variable can avoid sync_lock
   // the sync locks and atomic operations if we know for certain that
   // the process will not want to msync or is willing to risk
   // data not making it to disk.

   /**
    *  To modify in place we must know the following:
    * 1. the node is not being copied by the compactor
    *     - we know this because the compactor will set the copy bit
    * 2. the node is not in read-only memory 
    *    the node has not been synced to disk, which presuposes read-only
    *     - we know this from the segment footer which lets us know how much
    *       of the segment is read-only
    * 3. no one else is modifying or reading the node
    *     - we prove this by knowing we have a unique reference to the node
    * 
    *  If we determine that we might be able to modify in place we need to
    *  lock out any other threads that might try to mark the memory as read-only
    *  or subseqently attempt to copy the node. 
    * 
    * 1. we set the modify bit on the node meta, informing the compactor that
    *    a modification is in progress. 
    * 2. we attempt to acquire the sync lock on the segment,
    *     - this is a shared lock, meaning that many threads can acquire it
    *       and that the thread attempting to mark the node as read-only 
    *       must wait for all other threads to release it.
    */
   template <typename T>
   T* modify_lock::as()
   {
      // cached copy of last read that is still locked
      // unlikely, because the code is pretty consistent about
      // doing everything with the return value from as() once
      // or using as(lambda), so this is mostly a robustness check
      if (_observed_ptr) [[unlikely]]
         return (T*)_observed_ptr;

      // one way or another we will end up with a segment locked from
      // syncing either the current segment or the COW segment.
      _released = false;

      auto val = _meta.load(std::memory_order_acquire);
      auto loc = val.loc();

      // we can only modify in place if it isnt read-only and
      // the segment is owned by the current session
      if (_rlock.can_modify(loc))
         return (T*)(_observed_ptr = _rlock.get_node_pointer(loc));

      return (T*)(_observed_ptr = copy_on_write(val));
   }

   inline node_header* modify_lock::copy_on_write(temp_meta_type meta)
   {
      auto loc = meta.loc();

      auto cur_ptr  = _rlock.get_node_pointer(loc);
      auto old_oref = _rlock.get(cur_ptr->address());
      assert(cur_ptr->address() == old_oref.address());

      auto oref = _rlock.realloc(old_oref, cur_ptr->_nsize, cur_ptr->get_type(), [&](auto ptr)
                                 { memcpy_aligned_64byte(ptr, cur_ptr, cur_ptr->_nsize); });
      return _rlock.get_node_pointer(oref.meta_data().loc());
   }

   template <typename T>
   void modify_lock::as(std::invocable<T*> auto&& call_with_tptr)
   {
      call_with_tptr(as<T>());
   }

   inline void modify_lock::release()
   {
      _released = true;
      unlock();
   }

   inline void modify_lock::unlock()
   {
      if (_observed_ptr)
      {
         if constexpr (update_checksum_on_modify)
            _observed_ptr->update_checksum();
         else
            _observed_ptr->checksum = 0;
         //  _rlock.end_modify();
      }
      _released = true;
   }

   inline modify_lock::modify_lock(node_meta_type& m, read_lock& rl)
       : _meta(m), _rlock(rl), _released(true)
   {
   }

   inline modify_lock::~modify_lock()
   {
      if (not _released)
         unlock();
   }

   inline uint64_t read_lock::cache_difficulty() const
   {
      return _session.get_cache_difficulty();
   }

   inline bool read_lock::should_cache(uint32_t size) const
   {
      return _session.should_cache(size);
   }

   /**
    * Records when an object has been freed to update segment metadata
    * 
    * @param segment The segment number where the object is located
    * @param obj_ptr Pointer to the object being freed
    */
   inline void read_lock::freed_object(segment_number segment, const node_header* obj_ptr)
   {
      _session.record_freed_space(segment, obj_ptr);
   }

   inline bool read_lock::is_read_only(node_location loc) const
   {
      return _session.is_read_only(loc);
   }

   inline bool read_lock::can_modify(node_location loc) const
   {
      return _session.can_modify(loc);
   }

}  // namespace arbtrie
