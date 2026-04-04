#include <psitri/dwal/dwal_read_session.hpp>
#include <psitri/dwal/dwal_database.hpp>
#include <psitri/database.hpp>

#include <new>

namespace psitri::dwal
{
   dwal_read_session::dwal_read_session(dwal_database& db)
       : _db(db), _tri_session(db.underlying_db()->start_read_session())
   {
      init_cache();
   }

   dwal_read_session::~dwal_read_session()
   {
      if (_cache)
      {
         for (uint32_t i = 0; i < max_roots; ++i)
            _cache[i].~root_cache();
         ::operator delete(static_cast<void*>(_cache));
         _cache = nullptr;
      }
   }

   void dwal_read_session::init_cache()
   {
      // Allocate raw storage for max_roots root_cache objects.
      void* mem = ::operator new(sizeof(root_cache) * max_roots);
      _cache    = static_cast<root_cache*>(mem);

      // Construct each with the allocator session from _tri_session.
      // This gives every tri_cursor a valid session so cursor::refresh() works.
      auto session = _tri_session->allocator_session();
      for (uint32_t i = 0; i < max_roots; ++i)
         new (&_cache[i]) root_cache(session);
   }

   void dwal_read_session::refresh(uint32_t root_index)
   {
      auto& root  = _db.root(root_index);
      auto& cache = _cache[root_index];

      // Double-check generation — another path may have already refreshed.
      uint32_t cur_gen = root.generation.load(std::memory_order_acquire);
      if (cache.initialized && cur_gen == cache.gen)
         return;

      // Grab the latest DWAL snapshot under a shared lock (readers don't contend).
      {
         std::shared_lock lk(root.buffered_mutex);
         cache.snapshot = root.buffered_ptr;
      }

      // Refresh the PsiTri cursor to pick up newly merged data.
      cache.tri_cursor.refresh(root_index);

      cache.gen         = cur_gen;
      cache.initialized = true;
   }

   dwal_read_session::lookup_result dwal_read_session::get(uint32_t         root_index,
                                                           std::string_view key,
                                                           read_mode        mode)
   {
      auto& cache = _cache[root_index];

      // Check if we need to refresh (gen changed or first access).
      uint32_t cur_gen = _db.root(root_index).generation.load(std::memory_order_acquire);
      if (!cache.initialized || cur_gen != cache.gen)
         refresh(root_index);

      // Persistent: PsiTri only.
      if (mode == read_mode::persistent)
      {
         std::string buf;
         if (cache.tri_cursor.get(key_view(key.data(), key.size()), &buf) >= 0)
            return {true, std::move(buf)};
         return {false, {}};
      }

      // Latest/buffered: search cached DWAL snapshot first.
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

      // Fall through to PsiTri.
      std::string buf;
      if (cache.tri_cursor.get(key_view(key.data(), key.size()), &buf) >= 0)
         return {true, std::move(buf)};

      return {false, {}};
   }

}  // namespace psitri::dwal
