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

      // Trie mode: PsiTri only.
      if (mode == read_mode::trie)
      {
         std::string buf;
         if (cache.tri_cursor.get(key_view(key.data(), key.size()), &buf) >= 0)
            return {true, std::move(buf)};
         return {false, {}};
      }

      // Latest mode: wait for writer_active==false, read head directly.
      // Increment reader_count so the writer knows to COW if it starts.
      if (mode == read_mode::latest)
      {
         auto& root = _db.root(root_index);
         auto& cow  = root.cow;

         // Increment reader_count (atomic, no CAS needed)
         cow.root_and_flags.fetch_add(cowart_flags::reader_count_one,
                                       std::memory_order_acquire);

         // Check if writer is active — if so, set reader_waiting and wait
         uint64_t flags = cow.root_and_flags.load(std::memory_order_acquire);
         if (cowart_flags{flags}.writer_active())
         {
            // Set reader_waiting bit via CAS (preserve reader_count)
            uint64_t expected = flags;
            uint64_t desired  = expected | cowart_flags::reader_waiting_bit;
            cow.root_and_flags.compare_exchange_strong(
                expected, desired, std::memory_order_acq_rel);

            // Wait for writer to finish
            std::unique_lock lk(cow.notify_mutex);
            cow.writer_done_cv.wait(lk, [&]() {
               return !cowart_flags{cow.root_and_flags.load(
                   std::memory_order_acquire)}.writer_active();
            });
         }

         // Read head root (writer_active is now false, head is committed)
         flags = cow.root_and_flags.load(std::memory_order_acquire);
         art::offset_t head = cowart_flags{flags}.root_offset();

         // Lookup in the RW arena using head root
         if (head != art::null_offset && root.rw_layer)
         {
            auto& arena = root.rw_layer->map.get_arena();
            auto* v = art::get<btree_value>(arena, head, key);
            if (v)
            {
               // Decrement reader_count before returning
               cow.root_and_flags.fetch_sub(cowart_flags::reader_count_one,
                                             std::memory_order_release);
               if (v->is_tombstone())
                  return {false, {}};
               return {true, std::string(v->data)};
            }
            if (root.rw_layer->tombstones.is_deleted(key))
            {
               cow.root_and_flags.fetch_sub(cowart_flags::reader_count_one,
                                             std::memory_order_release);
               return {false, {}};
            }
         }

         // Decrement reader_count — not found in RW, fall through to RO + Tri
         cow.root_and_flags.fetch_sub(cowart_flags::reader_count_one,
                                       std::memory_order_release);
      }

      // Fresh mode: read prev_root (always safe, zero coordination).
      if (mode == read_mode::fresh)
      {
         auto& root = _db.root(root_index);
         auto& cow  = root.cow;
         art::offset_t prev = cow.prev_root.load(std::memory_order_acquire);
         if (prev != art::null_offset && root.rw_layer)
         {
            auto& arena = root.rw_layer->map.get_arena();
            auto* v = art::get<btree_value>(arena, prev, key);
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

      // Buffered/fresh/latest: search cached RO snapshot (frozen arena).
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
