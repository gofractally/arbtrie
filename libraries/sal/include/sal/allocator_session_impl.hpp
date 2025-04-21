#pragma once
#include <sal/alloc_header.hpp>
#include <sal/allocator.hpp>
#include <sal/allocator_session.hpp>
#include <sal/transaction.hpp>

namespace sal
{

   [[nodiscard]] inline read_lock allocator_session::lock() noexcept
   {
      return read_lock(*this);
   }

   template <typename T>
   T* allocator_session::get(location loc) noexcept
   {
      if constexpr (debug_memory)
      {
         /// TODO: add sanity checks here
         // verify that the location isn't beyond the end of allocated memory
         // verify that the location is not in an unallocated segment or
         // unallocated space within a segment
         // verify that the type can be converted to T
         // verify that if the location is not read only, then the session owns the segment
      }
      return reinterpret_cast<T*>(_block_base_ptr + *loc.offset());
   }
   /**
    * Allocates a node in the active segment and returns a pointer to the node, the
    * modify lock must be released after all writes are done on the allocated node.
    * 
    * The caller does not know what segment this will end up written to, so it cannot
    * write lock in advance. 

    * constructs T(size, std::forward<decltype(args)>(args)...)
    * 
    * @param size, must be a multiple of 64
    * @param vage, the virtual age of the object, if 0, the current time will be used
    * @param args, arguments to forward to the constructor of the object after size.
    */

   template <typename T>
   std::pair<location, T*> allocator_session::alloc_data(uint32_t size, auto&&... args)
   {
      prepare_alloc(size, get_current_time_msec());
      auto head = _alloc_seg_ptr->alloc<T>(size, std::forward<decltype(args)>(args)...);
      auto loc  = location::from_absolute_address((char*)head - _block_base_ptr);
      return {loc, head};
   }  // namespace std::pair
   template <typename T>
   std::pair<location, T*> allocator_session::alloc_data_vage(uint32_t       size,
                                                              msec_timestamp vage,
                                                              auto&&... args)
   {
      prepare_alloc(size, vage);
      auto head = _alloc_seg_ptr->alloc<T>(size, std::forward<decltype(args)>(args)...);
      auto loc  = location::from_absolute_address((char*)head - _block_base_ptr);
      return {loc, head};
   }
   inline void allocator_session::prepare_alloc(uint32_t size, msec_timestamp vage)
   {
      assert(size < sizeof(mapped_memory::segment::data));
      assert(size == ucc::round_up_multiple<64>(size));

      if (not _alloc_seg_ptr) [[unlikely]]
         init_active_segment();

      if (not _alloc_seg_ptr->can_alloc(size)) [[unlikely]]
      {
         finalize_active_segment();  // final bookkeeping before getting a new segment
         init_active_segment();      // get a new segment
      }
      // Update the vage_accumulator with the current allocation
      /// TODO: make the caller provide the vage value rather than force the condition on every call
      _alloc_seg_ptr->age_accumulator.add(size, *vage);
   }

   /**
    * Generate a random number for cache decisions
    * 
    * @return A random 32-bit number
    */
   inline uint64_t allocator_session::get_random() noexcept
   {
      return _session_rng.next();
   }

   template <typename T>
   [[nodiscard]] ptr_address allocator_session::alloc(uint32_t   size,
                                                      alloc_hint hint,
                                                      auto&&... args) noexcept
   {
      assert(size >= sizeof(T));
      assert(size % 64 == 0);
      static_assert(std::is_base_of_v<alloc_header, T>, "T must derive from alloc_header");

      allocation palloc = _ptr_alloc.alloc(hint);

      auto [loc, node_ptr] =
          alloc_data<T>(size, palloc.addr_seq, std::forward<decltype(args)>(args)...);

      palloc.ptr->store(control_block_data().set_loc(loc).set_ref(1), std::memory_order_release);

      return palloc.addr_seq.address;
   }

   template <typename T>
   [[nodiscard]] smart_ptr<T> allocator_session::smart_alloc(uint32_t   size,
                                                             alloc_hint hint,
                                                             auto&&... args) noexcept
   {
      return smart_ptr<T>(allocator_session_ptr(this, true),
                          alloc<T>(size, hint, std::forward<decltype(args)>(args)...));
   }

   /**
    *  Allocates new memory but reuses the control block from an existing object whose
    * reference count is 1, the space from the existing object will be freed because the
    * control block will be pointing to a new location.
    */
   template <typename To, typename From>
   [[nodiscard]] smart_ref<To> allocator_session::realloc(const smart_ref<From>& from,
                                                          uint32_t               size,
                                                          auto&&... args)
   {
      assert(size >= sizeof(To));
      assert(size % 64 == 0);
      static_assert(std::is_base_of_v<alloc_header, To>, "To must derive from alloc_header");
      static_assert(std::is_base_of_v<alloc_header, From>, "From must derive from alloc_header");

      assert(from.ref() == 1);

      auto l = from.loc();

      auto [loc, node_ptr] =
          alloc_data<To>(size, from->address_seq(), std::forward<decltype(args)>(args)...);

      control_block_data old_control = from.control().move(loc);
      assert(old_control.ref == 1);

      // we have to refetch the pointer just incase the compactor moved the object
      // between when from read it and when we exchanged it.
      record_freed_space(get<From>(old_control.loc()));

      return smart_ref<To>(get_session_ptr(), node_ptr, from.control(),
                           from.control.load(std::memory_order_relaxed));
   }
   template <typename T>
   [[nodiscard]] T* allocator_session::copy_on_write(smart_ref<T>& ptr)
   {
      assert(can_read(ptr.loc()));
      if (can_modify(ptr.loc()))
         return ptr.get();
      uint32_t asize;

      if constexpr (std::is_same_v<T, alloc_header>)
         asize = vcall::cow_size(ptr.obj());
      else
         asize = ptr->cow_size();

      auto [loc, node_ptr] = alloc_data<T>(asize, ptr.address_seq());

      if constexpr (std::is_same_v<T, alloc_header>)
         vcall::copy_to(ptr, node_ptr);
      else
         ptr->copy_to(node_ptr);

      ptr._cached = ptr._control.move(loc);
      ptr._obj    = node_ptr;
      return node_ptr;
   }

   template <typename T>
   [[nodiscard]] smart_ref<T> allocator_session::get_ref(ptr_address adr) noexcept
   {
      assert(adr != null_ptr_address);
      auto& cb    = _ptr_alloc.get(adr);
      auto  cread = cb.load(std::memory_order_acquire);
      auto  ptr   = reinterpret_cast<T*>(_block_base_ptr + *cread.loc().offset());
      assert(int(T::type_id) == int(ptr->type()));
      return smart_ref<T>(get_session_ptr(), ptr, cb, cread);
   }

   inline bool allocator_session::is_read_only(ptr_address adr) const noexcept
   {
      assert(adr != null_ptr_address);
      auto& cb    = _ptr_alloc.get(adr);
      auto  cread = cb.load(std::memory_order_acquire);
      return is_read_only(cread.loc());
   }
   /**
    * Free an object in a segment
    * 
    * @param segment The segment number containing the object
    * @param object_size The size of the object to free
    */
   inline void allocator_session::record_freed_space(const alloc_header* obj) noexcept
   {
      _sega.record_freed_space(_session_num, obj);
   }

   /**
    * Check if a node location is read-only
    * 
    * @param loc The node location to check
    * @return true if the location is read-only, false otherwise
    */
   inline bool allocator_session::is_read_only(location loc) const noexcept
   {
      return _sega.is_read_only(loc);
   }

   /**
    * 
    */
   inline bool allocator_session::can_modify(location loc) const noexcept
   {
      return _sega.can_modify(_session_num, loc);
   }

   /**
    * Get the cache difficulty value which is used for determining read bit updates
    * 
    * @return The current cache difficulty value
    */
   inline uint64_t allocator_session::get_cache_difficulty() const noexcept
   {
      return _sega.get_cache_difficulty();
   }

   /**
    * Check if an object should be cached based on its size and difficulty threshold
    * 
    * @param size The size of the object in bytes
    * @return true if the object should be cached, false otherwise
    */
   inline bool allocator_session::should_cache(uint32_t size) noexcept
   {
      return _sega._mapped_state->_cache_difficulty_state.should_cache(get_random(), size);
   }
   inline void allocator_session::retain(ptr_address adr) noexcept
   {
      get(adr).retain();
   }
   inline void allocator_session::release(ptr_address adr) noexcept
   {
      auto prev = get(adr).release();
      if (prev.ref > 1)
         return;

      location loc = prev.loc();
      if (loc != location::null()) [[likely]]
      {
         this->retain_session();
         allocator_session_ptr ptr(this);

         const alloc_header* nptr = get<alloc_header>(loc);
         vcall::destroy(nptr, ptr);
         record_freed_space(nptr);
      }
      _ptr_alloc.free(adr);
   }

   /// called by the allocator_session_ptr destructor to release the session,
   /// notifies the allocator that the session is no longer in use when counter reaches 0
   inline void allocator_session::end_session()
   {
      SAL_INFO("allocator_session: end_session: {} {} ref: {}", this, _session_num, _ref_count);
      if (--_ref_count == 0)
      {
         SAL_ERROR("allocator_session: end_session: {} {} ref: {}", this, _session_num, _ref_count);
         _sega.end_session(this);
      }
   }

   template <typename T>
   smart_ptr<T> allocator_session::get_root(root_object_number ro) noexcept
   {
      return smart_ptr<T>(_sega.get(ro), this);
   }
   template <typename T>
   inline smart_ptr<T> allocator_session::set_root(root_object_number ro,
                                                   smart_ptr<T>       ptr,
                                                   sync_type          st) noexcept
   {
      sync(st);
      return smart_ptr<T>(_sega.set(ro, ptr.take(), st), this);
   }

   template <typename T, typename U>
   inline smart_ptr<T> allocator_session::cas_root(root_object_number ro,
                                                   smart_ptr<T>       expect,
                                                   smart_ptr<U>       desired,
                                                   sync_type          st) noexcept
   {
      sync(st);
      if (_sega.cas_root(ro, expect.address(), desired.address(), st))
      {
         desired.take();  // _sega took it, so don't release it
         // let the caller determine how and when to release prior value
         return smart_ptr<T>(expect.address(), this);
      }
      return smart_ptr<T>();
   }

   inline transaction_ptr allocator_session::start_transaction(root_object_number ro) noexcept
   {
      return std::make_unique<transaction>(allocator_session_ptr(this, true), ro);
   }

   inline allocator_session_ptr allocator_session::get_session_ptr() noexcept
   {
      return allocator_session_ptr(this, true);
   }

   inline smart_ptr<alloc_header> allocator_session::transaction_commit(
       root_object_number      ro,
       smart_ptr<alloc_header> desired,
       sync_type               st) noexcept
   {
      sync(st);
      return smart_ptr<alloc_header>(get_session_ptr(),
                                     _sega.transaction_commit(ro, desired.take(), st));
   }
   inline void allocator_session::transaction_abort(root_object_number ro) noexcept
   {
      _sega.transaction_abort(ro);
   }

   inline void allocator_session::retain_read_lock() noexcept
   {
      if (_nested_read_lock++)
         return;
      _session_rlock.lock();
   }
   inline void allocator_session::release_read_lock() noexcept
   {
      assert(_nested_read_lock > 0);
      if (--_nested_read_lock)
         return;
      _session_rlock.unlock();
   }

}  // namespace sal
