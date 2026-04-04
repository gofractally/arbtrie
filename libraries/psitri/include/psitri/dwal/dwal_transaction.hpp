#pragma once
#include <psitri/dwal/btree_layer.hpp>
#include <psitri/dwal/btree_value.hpp>
#include <psitri/dwal/dwal_root.hpp>
#include <psitri/dwal/undo_log.hpp>
#include <psitri/dwal/wal_writer.hpp>

#include <cassert>
#include <optional>
#include <string>
#include <string_view>

namespace psitri::dwal
{
   class dwal_database;

   /// A buffered write transaction on a single DWAL root.
   ///
   /// Single writer per root — no lock held. Mutations go to the RW btree +
   /// undo log. On commit, a WAL entry is written. On abort (or destruction),
   /// the undo log is replayed to restore the btree.
   ///
   /// For direct-mode transactions (large tx fallback), the caller uses
   /// the underlying PsiTri transaction directly — this class is not used.
   class dwal_transaction
   {
     public:
      dwal_transaction(dwal_root&     root,
                       wal_writer*    wal,
                       uint32_t       root_index,
                       dwal_database* db     = nullptr,
                       bool           nested = false);

      ~dwal_transaction();

      dwal_transaction(const dwal_transaction&)            = delete;
      dwal_transaction& operator=(const dwal_transaction&) = delete;
      dwal_transaction(dwal_transaction&& other) noexcept;
      dwal_transaction& operator=(dwal_transaction&&) = delete;

      // ── Mutations ──────────────────────────────────────────────────

      /// Insert or update a data key-value pair.
      void upsert(std::string_view key, std::string_view value);

      /// Insert or update a subtree reference.
      void upsert_subtree(std::string_view key, sal::ptr_address addr);

      /// Remove a single key.
      /// Returns true if the key existed in any layer.
      bool remove(std::string_view key);

      /// Remove all keys in [low, high).
      void remove_range(std::string_view low, std::string_view high);

      // ── Point Reads ────────────────────────────────────────────────

      /// Look up a key across all active layers (RW → RO → Tri).
      /// Returns the btree_value if found, or nullopt if not found or tombstoned.
      /// For Tri-layer lookups, the caller must provide a tri_get callback.
      struct lookup_result
      {
         bool        found = false;
         btree_value value;
      };

      lookup_result get(std::string_view key) const;

      // ── Transaction Control ────────────────────────────────────────

      /// Commit: write WAL entry, release undo log old subtrees, discard undo.
      void commit();

      /// Abort: replay undo log, release new subtrees displaced during replay.
      void abort();

      /// Start a nested (sub) transaction. Returns a new transaction that
      /// commits back into this one's btree.
      dwal_transaction sub_transaction();

      // ── Accessors ──────────────────────────────────────────────────

      bool     is_committed() const noexcept { return _committed; }
      bool     is_aborted() const noexcept { return _aborted; }
      uint32_t root_index() const noexcept { return _root_index; }

     private:
      /// Check if a key exists in the RO btree. Returns the value if found.
      lookup_result ro_get(std::string_view key) const;

      /// Record the appropriate undo entry before modifying a key.
      void record_undo_for_upsert(std::string_view key);

      /// Record the appropriate undo entry before removing a key.
      void record_undo_for_remove(std::string_view key);

      dwal_root*     _root       = nullptr;
      wal_writer*    _wal        = nullptr;
      dwal_database* _db         = nullptr;
      uint32_t       _root_index = 0;
      bool        _committed  = false;
      bool        _aborted    = false;
      bool        _nested     = false;
      bool        _owns_lock  = false;

      undo_log    _undo;
   };

}  // namespace psitri::dwal
