#pragma once
#include <sal/allocator_session.hpp>
#include <sal/object_ref.hpp>
#include <sal/read_lock.hpp>

namespace sal
{
   inline object_ref::object_ref(read_lock& rlock, ptr_address adr, shared_ptr& ptr)
       : _rlock(rlock), _ptr(ptr), _cached(_ptr.load(std::memory_order_relaxed)), _address(adr)
   {
   }

   inline object_ref::object_ref(const object_ref& p)
       : _rlock(p._rlock), _ptr(p._ptr), _cached(p._cached), _address(p._address)
   {
   }

   template <typename Type, bool SetReadBit>
   const Type* object_ref::as() const
   {
      assert(header()->verify_checksum());
      assert((Type::type == header<Type, SetReadBit>()->get_type()));
      return reinterpret_cast<const Type*>(header());
   }

   template <typename T, bool SetReadBit>
   inline const T* object_ref::header() const
   {
      assert(_ptr.load(std::memory_order_relaxed).ref);
      auto m = _ptr.load(std::memory_order_acquire);
      auto r = (const T*)_rlock.get_node_pointer(m.loc());
      if constexpr (debug_memory)
      {
         if (not r->verify_checksum())
         {
            SAL_ERROR("checksum: {:x}", r->get_checksum());
            abort();
         }
         assert(r->verify_checksum());
      }
      if constexpr (SetReadBit)
         maybe_update_read_stats(r->size());
      return r;
   }

   inline void object_ref::maybe_update_read_stats(uint32_t size) const
   {
      if (_rlock._session._rcache_queue.is_full())
      {
         SAL_WARN("rcache_queue is full, skipping cache");
         return;
      }
      if (_rlock.should_cache(size) and _rlock.is_read_only(_cached.loc()) and
          _ptr.try_inc_activity())
      {
         _rlock._session._rcache_queue.push(address());
      }
   }

   inline void object_ref::release() noexcept
   {
      auto prior = _ptr.release();
      if (prior.ref > 1)
         return;

      // TODO: _rlock.destroy( _address, prior.loc() );
      // this can enable rlock to choose whether to destory
      // immediately or queue it for compactor to process later
      auto ploc = prior.loc();
      auto nptr = _rlock.get_node_pointer(ploc);
      _rlock.free_shared_ptr(_address);
      _rlock.freed_object(ploc.segment(), nptr);
      vcall::destroy(nptr, _rlock);
   }

}  // namespace sal
