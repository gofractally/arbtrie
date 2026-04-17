#pragma once
#include <psitri/dwal/dwal_transaction.hpp>
#include <psitri/dwal/merge_cursor.hpp>
#include <psitri/lock_policy.hpp>

#include <cassert>
#include <cstdint>
#include <initializer_list>
#include <map>
#include <vector>

namespace psitri::dwal
{
   template <class LockPolicy>
   class basic_dwal_database;

   /// Multi-root DWAL transaction.
   ///
   /// Coordinates one dwal_transaction per participating root. Roots are
   /// declared upfront (write set + read set) and locked in sorted index
   /// order to prevent deadlocks. Write roots take exclusive locks; read
   /// roots take shared locks. Commit is atomic across all write roots via
   /// linked WAL entries with a shared transaction ID.
   ///
   /// Usage:
   /// @code
   ///   auto tx = db.start_transaction({0, 1}, {3});
   ///   tx.root(0).upsert("key", "val");
   ///   auto r = tx.root(3).get("cfg");
   ///   tx.commit();
   /// @endcode
   template <class LockPolicy = std_lock_policy>
   class basic_transaction
   {
     public:
      using database_type         = basic_dwal_database<LockPolicy>;
      using dwal_transaction_type = basic_dwal_transaction<LockPolicy>;
      using remove_result_type    = basic_remove_result<LockPolicy>;
      using lookup_result         = typename dwal_transaction_type::lookup_result;

      /// Per-root handle. Exposes mutations (write roots only) and reads.
      class root_handle
      {
        public:
         // ── Mutations (assert writable) ────────────────────────────
         void               upsert(std::string_view key, std::string_view value);
         void               upsert_subtree(std::string_view key, sal::ptr_address addr);
         remove_result_type remove(std::string_view key);
         void               remove_range(std::string_view low, std::string_view high);

         // ── Reads (always allowed) ─────────────────────────────────
         lookup_result get(std::string_view key) const;

         bool     writable() const noexcept { return _writable; }
         uint32_t index() const noexcept { return _root_index; }

        private:
         friend class basic_transaction<LockPolicy>;
         root_handle(dwal_transaction_type* inner, uint32_t root_index, bool writable)
             : _inner(inner), _root_index(root_index), _writable(writable)
         {
         }

         dwal_transaction_type* _inner;
         uint32_t               _root_index;
         bool                   _writable;
      };

      basic_transaction(database_type&                  db,
                        std::initializer_list<uint32_t> write_roots,
                        std::initializer_list<uint32_t> read_roots = {});

      basic_transaction(database_type&        db,
                        std::vector<uint32_t> write_roots,
                        std::vector<uint32_t> read_roots = {});

      ~basic_transaction();

      basic_transaction(basic_transaction&& other) noexcept;
      basic_transaction(const basic_transaction&)            = delete;
      basic_transaction& operator=(const basic_transaction&) = delete;
      basic_transaction& operator=(basic_transaction&&)      = delete;

      /// Access a root that was declared at construction time.
      root_handle& root(uint32_t root_index);

      // ── Convenience methods (delegate to root_handle) ─────────────

      void               upsert(uint32_t root_index, std::string_view key, std::string_view value);
      remove_result_type remove(uint32_t root_index, std::string_view key);
      void               remove_range(uint32_t root_index, std::string_view low,
                                      std::string_view high);
      lookup_result      get(uint32_t root_index, std::string_view key);

      // ── Transaction control ───────────────────────────────────────

      void commit();
      void abort();

      bool is_committed() const noexcept { return _committed; }
      bool is_aborted() const noexcept { return _aborted; }

     private:
      void init(std::vector<uint32_t> write_roots, std::vector<uint32_t> read_roots);
      void release_locks();

      struct lock_entry
      {
         uint32_t index;
         bool     exclusive;
      };

      database_type*                               _db = nullptr;
      std::vector<lock_entry>                      _locks;    // sorted by index
      std::map<uint32_t, dwal_transaction_type>    _txns;     // per-root transactions
      std::map<uint32_t, root_handle>              _handles;  // per-root handles
      std::vector<uint32_t>                        _write_roots;
      bool                                         _committed = false;
      bool                                         _aborted   = false;
   };

   using transaction = basic_transaction<std_lock_policy>;

}  // namespace psitri::dwal
