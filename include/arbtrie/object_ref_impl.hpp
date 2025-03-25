#pragma once
#include <assert.h>
#include <arbtrie/config.hpp>
#include <arbtrie/node_header.hpp>
#include <arbtrie/object_ref.hpp>
#include <arbtrie/read_lock.hpp>

namespace arbtrie
{
   inline object_ref::object_ref(read_lock& rlock, id_address adr, node_meta_type& met)
       : _rlock(rlock), _meta(met), _cached(_meta.load(std::memory_order_relaxed)), _address(adr)
   {
   }

   inline object_ref::object_ref(const object_ref& p)
       : _rlock(p._rlock), _address(p._address), _meta(p._meta), _cached(p._cached)
   {
   }

   template <typename Type, bool SetReadBit>
   const Type* object_ref::as() const
   {
      assert(header()->validate_checksum());
      assert((Type::type == header<Type, SetReadBit>()->get_type()));
      return reinterpret_cast<const Type*>(header());
   }

   template <typename T, bool SetReadBit>
   inline const T* object_ref::header() const
   {
      assert(_meta.load(std::memory_order_relaxed).ref);
      auto m = _meta.load(std::memory_order_acquire);
      auto r = (const T*)_rlock.get_node_pointer(m.loc());
      if constexpr (debug_memory)
      {
         if (not r->validate_checksum())
         {
            ARBTRIE_WARN("checksum: ", r->checksum);
            abort();
         }
         assert(r->validate_checksum());
      }
      if constexpr (SetReadBit)
         maybe_update_read_stats(r->size());
      return r;
   }

   /*
   template <typename T>
   std::pair<const T*, node_location> object_ref::try_move_header()
   {
      if (auto opt_loc = _meta.try_move_location())
      {
         return {_rlock.get_node_pointer(*opt_loc), *opt_loc};
      }
      return {nullptr, node_location::from_absolute(0)};
   }
   */

   inline void object_ref::maybe_update_read_stats(uint32_t size) const
   {
      if (_rlock._session._rcache_queue.is_full())
      {
         ARBTRIE_WARN("rcache_queue is full, skipping cache");
         return;
      }
      if (_rlock.should_cache(size) and _rlock.is_read_only(_cached.loc()) and
          _meta.try_inc_activity())
         _rlock._session._rcache_queue.push(address());
   }

   inline const node_header* object_ref::release()
   {
      auto prior = _meta.release();
      if (prior.ref > 1)
         return nullptr;

      auto ploc = prior.loc();
      auto nptr = _rlock.get_node_pointer(ploc);
      _rlock.free_meta_node(_address);
      _rlock.freed_object(get_segment_num(ploc), nptr);
      return nptr;
   }

}  // namespace arbtrie