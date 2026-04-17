#pragma once
#include <psitri/cursor.hpp>
#include <psitri/dwal/btree_layer.hpp>
#include <psitri/dwal/dwal_root.hpp>
#include <psitri/fwd.hpp>
#include <psitri/lock_policy.hpp>

#include <cstdint>
#include <memory>
#include <string_view>

namespace psitri::dwal
{
   template <class LockPolicy>
   class basic_dwal_database;

   /// A read session for the DWAL layer.
   ///
   /// Caches per-root DWAL snapshots and PsiTri cursors, refreshing them
   /// only when the generation counter changes (i.e., after a swap).
   /// In the common case, get() checks an atomic generation counter and
   /// searches the cached snapshot — zero locks, zero contention with writers.
   ///
   /// Thread affinity: one session per reader thread (same as PsiTri read_session).
   ///
   /// Usage:
   /// @code
   ///   auto reader = dwal_db.start_read_session();
   ///   auto result = reader.get(0, "mykey", read_mode::buffered);
   ///   if (!result.found)
   ///       // not in any layer
   /// @endcode
   template <class LockPolicy = std_lock_policy>
   class basic_dwal_read_session
   {
     public:
      using database_type     = basic_dwal_database<LockPolicy>;
      using read_session_type = basic_read_session<LockPolicy>;

      struct lookup_result
      {
         bool        found = false;
         std::string value;
      };

      basic_dwal_read_session(database_type& db);
      ~basic_dwal_read_session();

      basic_dwal_read_session(basic_dwal_read_session&& other) noexcept
          : _db(other._db),
            _tri_session(std::move(other._tri_session)),
            _cache(other._cache)
      {
         other._cache = nullptr;
      }
      basic_dwal_read_session& operator=(basic_dwal_read_session&&) = delete;
      basic_dwal_read_session(const basic_dwal_read_session&)       = delete;
      basic_dwal_read_session& operator=(const basic_dwal_read_session&) = delete;

      /// Layered lookup with automatic snapshot caching.
      ///
      /// - trie: PsiTri cursor only (refreshed on gen change)
      /// - buffered/latest: cached DWAL snapshot + PsiTri fallback
      ///
      /// Fast path (gen unchanged): atomic load + cached btree search. No locks.
      lookup_result get(uint32_t root_index, std::string_view key,
                        read_mode mode = read_mode::buffered);

     private:
      void refresh(uint32_t root_index);

      struct root_cache
      {
         std::shared_ptr<btree_layer> snapshot;
         psitri::cursor               tri_cursor;
         uint32_t                     gen         = 0;
         bool                         initialized = false;

         root_cache(sal::allocator_session_ptr session)
             : tri_cursor{sal::smart_ptr<sal::alloc_header>(session, sal::null_ptr_address)}
         {
         }
      };

      database_type&                    _db;
      std::shared_ptr<read_session_type> _tri_session;

      void init_cache();

      static constexpr uint32_t max_roots = 512;
      root_cache*               _cache    = nullptr;
   };

   using dwal_read_session = basic_dwal_read_session<std_lock_policy>;

}  // namespace psitri::dwal
