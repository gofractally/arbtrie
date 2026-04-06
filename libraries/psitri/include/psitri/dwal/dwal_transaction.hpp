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

   /**
    * @brief Lazy result from dwal_transaction::remove().
    *
    * If the key was found in the RW layer, the answer is known immediately.
    * If not, converting to bool triggers a lookup in lower layers (RO + Tri).
    * Callers that discard the result pay no lookup cost.
    */
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

      explicit remove_result(bool existed)
          : _existed(existed), _resolved(true) {}

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
      read_write,  ///< Full access: RW btree mutations, undo log, WAL recording
      read_only,   ///< Reads only: 3-layer lookup, no undo/WAL, no mutations
   };

   /**
    * @brief A buffered write transaction on a single DWAL root.
    *
    * Provides ACID semantics over PsiTri's DWAL write path.  Mutations
    * accumulate in the RW ART map with WAL durability; reads see uncommitted
    * writes via layered lookup (RW → RO → Tri).
    *
    * ## Threading
    *
    * Single writer per root.  The transaction must be created, used, and
    * destroyed on the same thread.  No concurrent access — the RW layer
    * is writer-private.
    *
    * ## Lifecycle
    *
    *   auto tx = dwal_db.start_write_transaction(root_index);
    *   tx.upsert("key", "value");       // buffered in RW ART map
    *   auto val = tx.get("key");         // sees uncommitted write
    *   auto cur = tx.create_cursor();    // iterate over uncommitted + committed
    *   tx.commit();                      // WAL entry written, undo discarded
    *   // — or —
    *   tx.abort();                       // undo log replayed, RW map restored
    *
    * If neither commit() nor abort() is called, the destructor aborts.
    *
    * ## Nested Transactions
    *
    * sub_transaction() creates a child that commits back into the parent's
    * RW map.  Only the outermost commit writes a WAL entry.  Inner abort
    * undoes only the child's mutations.
    *
    * ## Direct Mode
    *
    * For large batch operations that exceed the ART buffer capacity, use
    * transaction_mode::direct when starting the transaction.  This flushes
    * the DWAL buffer and writes directly to the PsiTri COW trie.
    */
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

      /**
       * @brief Insert or update a data key-value pair.
       *
       * The write goes immediately to the RW ART map and is recorded
       * in the undo log (for rollback) and WAL buffer (for durability).
       *
       * @pre  !is_committed() && !is_aborted() && !is_read_only()
       * @param key    Key bytes (copied into ART arena)
       * @param value  Value bytes (copied into PMR pool)
       */
      void upsert(std::string_view key, std::string_view value);

      /**
       * @brief Insert or update a subtree reference.
       *
       * Stores a PsiTri subtree root address as the value.  The caller
       * must have obtained the address via smart_ptr::take() — the
       * transaction assumes ownership of one reference count.
       *
       * @pre  !is_committed() && !is_aborted() && !is_read_only()
       * @param key   Key bytes
       * @param addr  PsiTri subtree root address (one ref consumed)
       */
      void upsert_subtree(std::string_view key, sal::ptr_address addr);

      /**
       * @brief Remove a single key.
       *
       * Inserts a tombstone into the RW ART map, shadowing the key in
       * lower layers.  Returns a lazy remove_result:
       *   - If the key was in the RW map, the result resolves immediately.
       *   - If not, converting to bool checks RO + Tri layers.
       *   - Discarding the result skips the lower-layer lookup entirely.
       *
       * @pre  !is_committed() && !is_aborted() && !is_read_only()
       */
      remove_result remove(std::string_view key);

      /**
       * @brief Remove all keys in [low, high).
       *
       * Adds a range tombstone to the RW layer.  Keys in the range that
       * exist in lower layers are shadowed without per-key cost.
       *
       * @pre  !is_committed() && !is_aborted() && !is_read_only()
       */
      void remove_range(std::string_view low, std::string_view high);

      // ── Reads ──────────────────────────────────────────────────────

      /**
       * @brief Result of a point lookup or cursor value read.
       *
       * For RW/RO layer results, value.data points into pool-backed memory
       * valid for the layer's lifetime.  For Tri layer results, owned_data
       * holds a copy and value.data points into it.
       */
      struct lookup_result
      {
         bool        found = false;
         btree_value value;
         std::string owned_data;

         static lookup_result make_owned(std::string data)
         {
            lookup_result r;
            r.found      = true;
            r.owned_data = std::move(data);
            r.value      = btree_value::make_data(r.owned_data);
            return r;
         }
      };

      /**
       * @brief Point lookup across all layers (RW → RO → Tri).
       *
       * Sees uncommitted writes from this transaction.  Returns immediately
       * on first hit or tombstone.
       *
       * @pre  !is_committed() && !is_aborted()
       * @param key  Key to look up
       * @return     lookup_result with found=true if key exists and is not
       *             tombstoned, found=false otherwise.
       */
      lookup_result get(std::string_view key) const;

      /**
       * @brief Create a merge cursor that sees uncommitted writes.
       *
       * Returns an owned cursor that merges three layers:
       *   1. RW ART map — live, includes uncommitted mutations from this tx
       *   2. RO ART map — frozen snapshot from last swap (if any)
       *   3. PsiTri COW trie — persistent on-disk data
       *
       * The cursor supports full iteration: seek_begin(), next(), prev(),
       * lower_bound(), upper_bound(), count_keys().  Higher layers shadow
       * lower layers; tombstones filter deleted keys automatically.
       *
       * @pre  !is_committed() && !is_aborted()
       *
       * @note Writer-thread only.  The cursor accesses the live RW ART map
       *       without locking (writer-private).  Do not share across threads.
       *
       * @note Invalidated by subsequent mutations.  After calling upsert(),
       *       remove(), or remove_range(), discard the cursor and create a
       *       new one to see the updated state.
       */
      owned_merge_cursor create_cursor() const;

      // ── Transaction Control ────────────────────────────────────────

      /**
       * @brief Commit the transaction.
       *
       * Writes a WAL entry containing all mutations, releases old subtree
       * refs from the undo log, and discards the undo log.  After commit,
       * the mutations are durable (subject to WAL flush policy) and visible
       * to subsequent transactions.
       *
       * @pre  !is_committed() && !is_aborted()
       * @post is_committed() == true
       */
      void commit();

      /**
       * @brief Commit as part of a multi-root transaction.
       *
       * Tags the WAL entry with a shared transaction ID and participant
       * count.  The last participant sets is_commit=true, which marks the
       * group as atomically committed.  Used by dwal::transaction for
       * cross-root atomicity.
       *
       * @pre  !is_committed() && !is_aborted()
       */
      void commit_multi(uint64_t tx_id, uint16_t participants, bool is_commit);

      /**
       * @brief Abort the transaction.
       *
       * Replays the undo log in reverse to restore the RW ART map to its
       * pre-transaction state.  Releases new subtree refs that were
       * displaced during replay.
       *
       * @pre  !is_committed() && !is_aborted()
       * @post is_aborted() == true
       */
      void abort();

      /**
       * @brief Start a nested (sub) transaction.
       *
       * Returns a child transaction that operates on the same RW ART map.
       * The child's commit merges its undo entries into the parent.  The
       * child's abort undoes only its own mutations.  Only the outermost
       * transaction writes a WAL entry on commit.
       *
       * @pre  !is_committed() && !is_aborted() && !is_read_only()
       */
      dwal_transaction sub_transaction();

      // ── Accessors ──────────────────────────────────────────────────

      bool     is_committed() const noexcept { return _committed; }
      bool     is_aborted() const noexcept { return _aborted; }
      bool     is_read_only() const noexcept { return _mode == root_mode::read_only; }
      uint32_t root_index() const noexcept { return _root_index; }

     private:
      friend class remove_result;

      lookup_result ro_get(std::string_view key) const;
      bool exists_in_lower_layers(std::string_view key) const;
      void record_undo_for_upsert(std::string_view key);
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
