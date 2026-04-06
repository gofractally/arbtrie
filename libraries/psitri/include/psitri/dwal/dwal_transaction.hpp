#pragma once
#include <psitri/dwal/btree_layer.hpp>
#include <psitri/dwal/btree_value.hpp>
#include <psitri/dwal/dwal_root.hpp>
#include <psitri/dwal/merge_cursor.hpp>
#include <psitri/dwal/undo_log.hpp>
#include <psitri/dwal/wal_writer.hpp>

#include <cassert>
#include <optional>
#include <string>
#include <string_view>

namespace psitri::dwal
{
   class dwal_database;
   class dwal_transaction;

   /// Lazy result from dwal_transaction::remove().
   ///
   /// If the key was found in the RW layer, the answer is known immediately.
   /// If not, converting to bool triggers a lookup in lower layers (RO + Tri).
   /// Callers that discard the result pay no lookup cost.
   class remove_result
   {
     public:
      /// Implicit conversion — resolves the lazy check if needed.
      operator bool()
      {
         if (!_resolved)
            resolve();
         return _existed;
      }

     private:
      friend class dwal_transaction;

      /// Already known (key was in RW layer).
      explicit remove_result(bool existed)
          : _existed(existed), _resolved(true) {}

      /// Deferred — needs RO + Tri check.
      remove_result(const dwal_transaction* tx, std::string key)
          : _tx(tx), _key(std::move(key)), _resolved(false) {}

      void resolve();

      const dwal_transaction* _tx = nullptr;
      std::string             _key;
      bool                    _existed  = false;
      bool                    _resolved = true;
   };

   /// Mode for a root within a transaction.
   enum class root_mode : uint8_t
   {
      read_write,  // Full access: RW btree mutations, undo log, WAL recording
      read_only,   // Reads only: 3-layer lookup, no undo/WAL, no mutations
   };

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
                       bool           nested = false,
                       root_mode      mode   = root_mode::read_write);

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
      /// Returns a remove_result that lazily checks whether the key existed.
      /// If the key was in the RW layer, the result is immediate.
      /// If not, converting to bool checks RO + Tri layers.
      /// Discarding the result skips the lower-layer lookup entirely.
      remove_result remove(std::string_view key);

      /// Remove all keys in [low, high).
      void remove_range(std::string_view low, std::string_view high);

      // ── Point Reads ────────────────────────────────────────────────

      /// Look up a key across all active layers (RW → RO → Tri).
      /// Returns the btree_value if found, or nullopt if not found or tombstoned.
      struct lookup_result
      {
         bool        found = false;
         btree_value value;

         /// Backing storage for Tri-layer results where the data is copied
         /// rather than referenced from pool-backed memory.
         std::string owned_data;

         /// Create a result that owns its data (for Tri-layer lookups).
         static lookup_result make_owned(std::string data)
         {
            lookup_result r;
            r.found      = true;
            r.owned_data = std::move(data);
            r.value      = btree_value::make_data(r.owned_data);
            return r;
         }
      };

      lookup_result get(std::string_view key) const;

      // ── Cursor ─────────────────────────────────────────────────────

      /// Create a merge cursor that sees the transaction's uncommitted writes.
      ///
      /// The returned cursor merges the live RW layer (including uncommitted
      /// mutations from this transaction), the frozen RO snapshot, and the
      /// PsiTri COW trie.  This is the DWAL equivalent of the native
      /// transaction::read_cursor().
      ///
      /// The cursor is only valid on the writer thread (same thread that
      /// owns this transaction).  Invalidated by subsequent mutations --
      /// recreate after upsert/remove if you need an updated view.
      owned_merge_cursor create_cursor() const;

      // ── Transaction Control ────────────────────────────────────────

      /// Commit: write WAL entry, release undo log old subtrees, discard undo.
      void commit();

      /// Commit as part of a multi-root transaction. Tags the WAL entry with
      /// the shared tx_id and participant count. The last participant sets
      /// is_commit = true, which writes the multi_tx_commit flag.
      void commit_multi(uint64_t tx_id, uint16_t participants, bool is_commit);

      /// Abort: replay undo log, release new subtrees displaced during replay.
      void abort();

      /// Start a nested (sub) transaction. Returns a new transaction that
      /// commits back into this one's btree.
      dwal_transaction sub_transaction();

      // ── Accessors ──────────────────────────────────────────────────

      bool     is_committed() const noexcept { return _committed; }
      bool     is_aborted() const noexcept { return _aborted; }
      bool     is_read_only() const noexcept { return _mode == root_mode::read_only; }
      uint32_t root_index() const noexcept { return _root_index; }

     private:
      friend class remove_result;

      /// Check if a key exists in the RO btree. Returns the value if found.
      lookup_result ro_get(std::string_view key) const;

      /// Check if a key exists in lower layers (RO + Tri). Used by remove_result.
      bool exists_in_lower_layers(std::string_view key) const;

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
      root_mode   _mode       = root_mode::read_write;

      undo_log    _undo;
   };

}  // namespace psitri::dwal
