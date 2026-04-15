#pragma once
#include <psitri/dwal/dwal_transaction.hpp>
#include <psitri/dwal/merge_cursor.hpp>

#include <cassert>
#include <cstdint>
#include <initializer_list>
#include <map>
#include <vector>

namespace psitri::dwal
{
   class dwal_database;

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
   class transaction
   {
     public:
      /// Per-root handle. Exposes mutations (write roots only) and reads.
      class root_handle
      {
        public:
         // ── Mutations (assert writable) ────────────────────────────
         void upsert(std::string_view key, std::string_view value);
         void upsert_subtree(std::string_view key, sal::tree_id tid);
         remove_result remove(std::string_view key);
         void remove_range(std::string_view low, std::string_view high);

         // ── Reads (always allowed) ─────────────────────────────────
         dwal_transaction::lookup_result get(std::string_view key) const;

         bool     writable() const noexcept { return _writable; }
         uint32_t index() const noexcept { return _root_index; }

        private:
         friend class transaction;
         root_handle(dwal_transaction* inner, uint32_t root_index, bool writable)
             : _inner(inner), _root_index(root_index), _writable(writable)
         {
         }

         dwal_transaction* _inner;
         uint32_t          _root_index;
         bool              _writable;
      };

      /// Construct a multi-root transaction. Locks are acquired in sorted order.
      /// write_roots get exclusive locks; read_roots get shared locks.
      transaction(dwal_database&                  db,
                  std::initializer_list<uint32_t> write_roots,
                  std::initializer_list<uint32_t> read_roots = {});

      /// Construct a multi-root transaction from vectors (for programmatic use).
      transaction(dwal_database&             db,
                  std::vector<uint32_t>      write_roots,
                  std::vector<uint32_t>      read_roots = {});

      ~transaction();

      transaction(transaction&& other) noexcept;
      transaction(const transaction&)            = delete;
      transaction& operator=(const transaction&) = delete;
      transaction& operator=(transaction&&)      = delete;

      /// Access a root that was declared at construction time.
      /// Asserts if root_index was not in the initial write or read set.
      root_handle& root(uint32_t root_index);

      // ── Convenience methods (delegate to root_handle) ─────────────

      void upsert(uint32_t root_index, std::string_view key, std::string_view value);
      remove_result remove(uint32_t root_index, std::string_view key);
      void remove_range(uint32_t root_index, std::string_view low, std::string_view high);
      dwal_transaction::lookup_result get(uint32_t root_index, std::string_view key);

      // ── Transaction control ───────────────────────────────────────

      /// Atomic commit across all write roots. Read roots just release locks.
      void commit();

      /// Rollback all write roots and release all locks.
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

      dwal_database*                        _db = nullptr;
      std::vector<lock_entry>               _locks;    // sorted by index
      std::map<uint32_t, dwal_transaction>  _txns;     // per-root transactions
      std::map<uint32_t, root_handle>       _handles;  // per-root handles
      std::vector<uint32_t>                 _write_roots;
      bool _committed = false;
      bool _aborted   = false;
   };

}  // namespace psitri::dwal
