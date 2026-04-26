#pragma once
#include <psitri/lock_policy.hpp>
#include <psitri/read_session.hpp>
#include <psitri/tx_mode.hpp>
#include <psitri/write_cursor.hpp>

namespace psitri
{
   class transaction;
   struct read_set;
   namespace detail { class write_buffer; }

   template <class LockPolicy>
   class basic_write_session;

   /**
    * @brief A read-write view of the database for one thread.
    *
    * basic_write_session extends basic_read_session with mutation capabilities.
    * It provides two APIs for modifying data:
    *
    * - **Transactions** (recommended): start_transaction() acquires a per-root
    *   mutex, loads the current root, and returns a transaction object.
    *   commit() atomically publishes the new root; abort() (or destructor)
    *   releases the lock without publishing. Only one transaction per root
    *   can be active at a time across all sessions.
    *
    * - **Write cursors** (low-level): create_write_cursor() gives direct access
    *   to the tree without locking. The caller is responsible for atomically
    *   publishing the root via set_root(). Useful for building trees that will
    *   be stored as subtree values.
    *
    * @tparam LockPolicy  Must match the policy used by the owning database.
    *
    * @note **Thread affinity:** Like read_session, a write_session is backed by
    *       a thread-local allocator_session. Create one session per writer thread
    *       — do not create sessions on a coordinator and distribute them.
    *
    * @note **Sync mode:** Controls what durability guarantee commit() provides.
    *       Default is sync_type::none (fastest, data persists at OS discretion).
    *       Set via set_sync() before starting transactions. See sal::sync_type
    *       for the full durability hierarchy from none through full fsync.
    *
    * @note **Memory lifecycle:** Deleted nodes are not immediately freed. They
    *       are pushed to a release queue that the background compactor drains
    *       asynchronously. Use get_pending_release_count() to monitor the queue
    *       depth, or database::wait_for_compactor() to block until drained.
    */
   template <class LockPolicy = std_lock_policy>
   class basic_write_session : public basic_read_session<LockPolicy>
   {
     public:
      using base_type        = basic_read_session<LockPolicy>;
      using lock_policy_type = LockPolicy;
      using database_type    = basic_database<LockPolicy>;
      using ptr              = std::shared_ptr<basic_write_session>;

      basic_write_session(database_type& db);
      ~basic_write_session() = default;

      /** @name Transactions */
      ///@{

      /**
       * @brief Start an atomic transaction on a top-level root.
       *
       * Acquires the per-root mutex for root_index, loads the current root,
       * and returns a transaction. The mutex is held until commit() or abort()
       * is called (or the transaction is destroyed).
       *
       * @param root_index Index into the 512 top-level roots (0-511).
       * @param mode Transaction mode: batch (default, direct COW) or micro
       *        (buffered writes, O(1) sub-transaction abort).
       * @return A transaction that provides insert/update/upsert/remove/get.
       *
       * @warning The returned transaction captures a pointer to this session.
       *          The session must outlive the transaction.
       */
      transaction start_transaction(uint32_t root_index,
                                    tx_mode  mode = tx_mode::batch);

      /**
       * @brief Immediate-mode MVCC upsert on a single key.
       *
       * Two locking paths:
       * - **Fast path** (key has value_node): acquires a stripe lock on the
       *   value_node's ptr_address. Only the value_node is modified via CB
       *   relocation — no leaf/inner cascade. Multiple writers on different
       *   value_nodes can execute concurrently on the same root.
       * - **Slow path** (inline value or new key): acquires the per-root
       *   modify_lock because structural changes (inline→value_node promotion,
       *   leaf insert, or COW fallback) may cascade.
       *
       * Both paths allocate a global version number and update the root slot's
       * version CB for reclamation tracking.
       *
       * @param root_index Index into the 512 top-level roots (0-511).
       * @param key        The key to insert or update.
       * @param value      The new value.
       * @return The version number allocated for this write.
       */
      uint64_t mvcc_upsert(uint32_t root_index, key_view key, value_view value);

      /**
       * @brief Immediate-mode MVCC remove (tombstone) on a single key.
       *
       * Same locking semantics as mvcc_upsert: stripe lock on value_node
       * for keys that already have one, per-root mutex otherwise. Appends
       * a tombstone entry to the key's value_node.
       *
       * @param root_index Index into the 512 top-level roots (0-511).
       * @param key        The key to tombstone.
       * @return The version number allocated for this write.
       */
      uint64_t mvcc_remove(uint32_t root_index, key_view key);

      /**
       * @brief Background defrag pass on a single root.
       *
       * Walks the tree and strips dead version entries from value_nodes.
       * Dead entries are those whose version falls within a dead range
       * (all snapshots referencing that version have been dropped).
       *
       * Acquires the per-root mutex for the duration of the pass.
       *
       * @param root_index Index into the 512 top-level roots (0-511).
       * @return The number of value_nodes that were cleaned up.
       */
      uint64_t defrag_tree(uint32_t root_index);

      ///@}

      /** @name Write Cursors (Low-Level) */
      ///@{

      /**
       * @brief Create a write cursor on a new, empty tree.
       *
       * The cursor is not associated with any root — the caller must
       * explicitly publish the result via set_root() or store it as a
       * subtree value. Useful for building subtrees.
       */
      write_cursor_ptr create_write_cursor();

      /**
       * @brief Create a write cursor on an existing tree root.
       *
       * Mutations create copy-on-write clones of nodes along the path.
       * The original root is unaffected (it may be shared by snapshots
       * or other sessions). Call root() on the cursor to get the new
       * root after mutations.
       */
      write_cursor_ptr create_write_cursor(sal::smart_ptr<sal::alloc_header> root);

      ///@}

      /** @name Root Management */
      ///@{

      sal::smart_ptr<sal::alloc_header> get_root(uint32_t root_index);

      void set_root(uint32_t                          root_index,
                    sal::smart_ptr<sal::alloc_header> root,
                    sal::sync_type                    sync = sal::sync_type::none);

      /// Create a reference-counted smart_ptr from a raw ptr_address.
      sal::smart_ptr<sal::alloc_header> make_ptr(sal::ptr_address addr, bool retain = false)
      {
         return sal::smart_ptr<sal::alloc_header>(this->_allocator_session, addr, retain);
      }

      /// Create a reference-counted smart_ptr from a tree_id (root + ver).
      sal::smart_ptr<sal::alloc_header> make_ptr(sal::tree_id tid, bool retain = false)
      {
         return sal::smart_ptr<sal::alloc_header>(this->_allocator_session, tid, retain);
      }

      ///@}

      /** @name Durability */
      ///@{

      /**
       * @brief Set the sync mode used when transactions commit.
       */
      void set_sync(sal::sync_type sync) { _sync = sync; }

      sal::sync_type get_sync() const { return _sync; }

      ///@}

      /** @name Diagnostics */
      ///@{

      uint64_t get_total_allocated_objects() const;

      uint64_t get_pending_release_count() const;

      void dump_live_objects() const;

      template <typename Visitor>
      void for_each_live_object(Visitor&& visitor) const
      {
         this->_allocator_session->for_each_live_object(std::forward<Visitor>(visitor));
      }

      ///@}

     private:
      friend class transaction;

      /// Fast-path version-CB swap under lightweight per-root ver lock.
      void swap_root_ver(uint32_t root_index, uint64_t ver_num);

      /// Return a reference to the per-root modify_lock for multi-root locking.
      typename LockPolicy::mutex_type& root_modify_lock(uint32_t root_index);

      /// Allocate a global version CB and publish a new root at root_index.
      void publish_root(uint32_t root_index, sal::smart_ptr<sal::alloc_header> new_root);

      /// Create a tree_context for a root with dead-version and epoch info set.
      tree_context make_tree_context(uint32_t root_index);

      sal::sync_type _sync = sal::sync_type::none;
   };

   /// Default-policy alias preserved for existing consumers.
   using write_session     = basic_write_session<std_lock_policy>;
   using write_session_ptr = std::shared_ptr<write_session>;

}  // namespace psitri
