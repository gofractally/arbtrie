#pragma once
#include <arbtrie/read_lock.hpp>
#include <arbtrie/seg_alloc_session.hpp>

namespace arbtrie
{

   /// @pre refcount of id is 1
   inline object_ref read_lock::realloc(object_ref& oref, uint32_t size, node_type type, auto init)
   {
      auto adr = oref.address();

      auto l   = oref.loc();
      auto seg = l.segment();

      // TODO: replace this with function call
      auto obj_ptr = (node_header*)((char*)_session._sega._block_alloc.get(seg) + l.abs_index());

      _session.free_object(seg, obj_ptr->object_capacity());

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

      _session.end_modify();

      assert(type == node_type::value or bool(node_ptr->_branch_id_region));
      assert(node_ptr->_nsize == size);
      assert(node_ptr->_ntype == type);
      assert(node_ptr->_node_id == adr);

      /// TODO: eliminate the redundant load with refresh
      oref.meta().set_location_and_type(loc, type, std::memory_order_release);
      oref.refresh(std::memory_order_relaxed);
      return oref;
   }

   inline id_allocation read_lock::get_new_meta_node(id_region reg)
   {
      return _session._sega._id_alloc.get_new_id(reg);
   }

   inline object_ref read_lock::alloc(id_region reg, uint32_t size, node_type type, auto init)
   {
      assert(size >= sizeof(node_header));
      assert(type != node_type::undefined);

      auto allocation = _session._sega._id_alloc.get_new_id(reg);

      // alloc_data() starts a modify lock on the allocation segment, which
      // which must be released by calling end_modify() after all writes are done
      auto [loc, node_ptr] =
          _session.alloc_data(size, id_address_seq(allocation.address, allocation.sequence));
      //ARBTRIE_WARN( "alloc id: ", id, " type: " , node_type_names[type], " loc: ", loc._offset, " size: ", size);

      init(node_ptr);
      if (_session._sega.config_update_checksum_on_modify())
         node_ptr->update_checksum();

      _session.end_modify();

      assert(type == node_type::value or bool(node_ptr->_branch_id_region));

      allocation.meta.store(temp_meta_type().set_type(type).set_location(loc).set_ref(1),
                            std::memory_order_release);

      assert(node_ptr->_nsize == size);
      assert(node_ptr->_ntype == type);
      assert(node_ptr->_node_id == allocation.address);
      assert(object_ref(*this, allocation.address, allocation.meta).type() != node_type::undefined);

      return object_ref(*this, allocation.address, allocation.meta);
   }

   inline bool read_lock::is_synced(node_location loc)
   {
      return _session.is_synced(loc);
   }

   inline sync_lock& read_lock::get_sync_lock(int seg)
   {
      return _session._sega._seg_sync_locks[seg];
   }
   inline node_header* read_lock::get_node_pointer(node_location loc)
   {
      auto segment = (mapped_memory::segment*)_session._sega._block_alloc.get(loc.segment());
      // 0 means we are accessing a swapped object on a segment that hasn't started new allocs
      // if alloc_pos > loc.index() then we haven't overwriten this object yet, we are accessing
      // data behind the alloc pointer which should be safe
      // to access data we had to get the location from obj id database and we should read
      // with memory_order_acquire, when updating an object_info we need to write with
      // memory_order_release otherwise the data written may not be visible yet to the reader coming
      // along behind

      // only check this in release if this flag is set
      if constexpr (debug_memory)
      {
         auto ap = segment->_alloc_pos.load(std::memory_order_relaxed);
         if (ap <= loc.abs_index())
         {
            ARBTRIE_WARN("segment: ", loc.segment(), " ap: ", ap, "  loc: ", loc.aligned_index(),
                         " abs: ", loc.abs_index());
            abort();
         }
      }
      else  // always check in debug builds
         assert(segment->_alloc_pos > loc.abs_index());

      return (node_header*)((char*)_session._sega._block_alloc.get(loc.segment()) +
                            loc.abs_index());
   }

   inline void read_lock::free_meta_node(id_address a)
   {
      _session._sega._id_alloc.free_id(a);
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

      auto val  = _meta.load(std::memory_order_acquire);
      auto loc  = val.loc();
      auto lseg = loc.segment();

      // if it is in read-only memory, then we need to copy the node
      if (_rlock.is_read_only(loc) or not _rlock.try_modify_segment(lseg))
         return (T*)(_observed_ptr = copy_on_write(val));

      return (T*)(_observed_ptr = _rlock.get_node_pointer(loc));
   }

   inline node_header* modify_lock::copy_on_write(node_meta_type::temp_type meta)
   {
      auto loc  = meta.loc();
      auto lseg = loc.segment();

      auto cur_ptr  = _rlock.get_node_pointer(loc);
      auto old_oref = _rlock.get(cur_ptr->address());
      assert(cur_ptr->address() == old_oref.address());

      /// TODO: realloc needs to obtain the lock on the segment and return responsibilty for
      /// releasing it to this modify lock, we need to add an assert that recursive calls don't
      /// try to acquire the same segment lock or one will unblock the others too soon.
      auto oref = _rlock.realloc(old_oref, cur_ptr->_nsize, cur_ptr->get_type(),
                                 [&](auto ptr) { memcpy(ptr, cur_ptr, cur_ptr->_nsize); });
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
         _rlock.end_modify();
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

   inline bool read_lock::try_modify_segment(segment_number segment_num)
   {
      return _session.try_modify_segment(segment_num);
   }

   inline void read_lock::end_modify()
   {
      _session.end_modify();
   }

   inline void read_lock::free_object(node_location loc, uint32_t size)
   {
      _session.free_object(loc.segment(), size);
   }

   inline bool read_lock::is_read_only(node_location loc) const
   {
      return _session.is_read_only(loc);
   }

}  // namespace arbtrie
