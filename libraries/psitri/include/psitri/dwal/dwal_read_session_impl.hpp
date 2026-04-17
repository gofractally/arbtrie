#pragma once
#include <psitri/database_impl.hpp>
#include <psitri/dwal/dwal_database.hpp>
#include <psitri/dwal/dwal_read_session.hpp>
#include <psitri/read_session_impl.hpp>

#include <new>

namespace psitri::dwal
{
   template <class LockPolicy>
   basic_dwal_read_session<LockPolicy>::basic_dwal_read_session(database_type& db)
       : _db(db), _tri_session(db.underlying_db()->start_read_session())
   {
      init_cache();
   }

   template <class LockPolicy>
   basic_dwal_read_session<LockPolicy>::~basic_dwal_read_session()
   {
      if (_cache)
      {
         for (uint32_t i = 0; i < max_roots; ++i)
            _cache[i].~root_cache();
         ::operator delete(static_cast<void*>(_cache));
         _cache = nullptr;
      }
   }

   template <class LockPolicy>
   void basic_dwal_read_session<LockPolicy>::init_cache()
   {
      void* mem = ::operator new(sizeof(root_cache) * max_roots);
      _cache    = static_cast<root_cache*>(mem);

      auto session = _tri_session->allocator_session();
      for (uint32_t i = 0; i < max_roots; ++i)
         new (&_cache[i]) root_cache(session);
   }

   template <class LockPolicy>
   void basic_dwal_read_session<LockPolicy>::refresh(uint32_t root_index)
   {
      auto& root  = _db.root(root_index);
      auto& cache = _cache[root_index];

      uint32_t cur_gen = root.generation.load(std::memory_order_acquire);
      if (cache.initialized && cur_gen == cache.gen)
         return;

      {
         std::lock_guard lk(root.buffered_mutex);
         cache.snapshot = root.buffered_ptr;
      }

      cache.tri_cursor.refresh(root_index);

      cache.gen         = cur_gen;
      cache.initialized = true;
   }

   template <class LockPolicy>
   typename basic_dwal_read_session<LockPolicy>::lookup_result
   basic_dwal_read_session<LockPolicy>::get(uint32_t         root_index,
                                            std::string_view key,
                                            read_mode        mode)
   {
      auto& cache = _cache[root_index];

      uint32_t cur_gen = _db.root(root_index).generation.load(std::memory_order_acquire);
      if (!cache.initialized || cur_gen != cache.gen)
         refresh(root_index);

      if (mode == read_mode::trie)
      {
         std::string buf;
         if (cache.tri_cursor.get(key_view(key.data(), key.size()), &buf) >= 0)
            return {true, std::move(buf)};
         return {false, {}};
      }

      if (mode == read_mode::latest)
      {
         auto&         root = _db.root(root_index);
         art::offset_t head = root.cow.begin_read_latest();

         if (head != art::null_offset && root.rw_layer)
         {
            auto& arena = root.rw_layer->map.get_arena();
            auto* v     = art::get<btree_value>(arena, head, key);
            if (v)
            {
               root.cow.end_read_latest();
               if (v->is_tombstone())
                  return {false, {}};
               return {true, std::string(v->data)};
            }
            if (root.rw_layer->tombstones.is_deleted(key))
            {
               root.cow.end_read_latest();
               return {false, {}};
            }
         }

         root.cow.end_read_latest();
      }

      if (mode == read_mode::fresh)
      {
         auto&         root = _db.root(root_index);
         art::offset_t prev = root.cow.read_prev();
         if (prev != art::null_offset && root.rw_layer)
         {
            auto& arena = root.rw_layer->map.get_arena();
            auto* v     = art::get<btree_value>(arena, prev, key);
            if (v)
            {
               if (v->is_tombstone())
                  return {false, {}};
               return {true, std::string(v->data)};
            }
            if (root.rw_layer->tombstones.is_deleted(key))
               return {false, {}};
         }
      }

      if (cache.snapshot)
      {
         auto* v = cache.snapshot->map.get(key);
         if (v)
         {
            if (v->is_tombstone())
               return {false, {}};
            return {true, std::string(v->data)};
         }
         if (cache.snapshot->tombstones.is_deleted(key))
            return {false, {}};
      }

      std::string buf;
      if (cache.tri_cursor.get(key_view(key.data(), key.size()), &buf) >= 0)
         return {true, std::move(buf)};

      return {false, {}};
   }

}  // namespace psitri::dwal
