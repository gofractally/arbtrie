#pragma once
#include <cassert>
#include <functional>
#include <optional>
#include <psitri/detail/write_buffer.hpp>
#include <psitri/fwd.hpp>
#include <psitri/tree.hpp>
#include <psitri/tx_mode.hpp>
#include <psitri/value_pin.hpp>
#include <psitri/write_cursor.hpp>
#include <stdexcept>
#include <utility>
#include <vector>

namespace psitri
{
   class transaction;
   class tree_handle;

   // ═════════════════════════════════════════════════════════════════════
   // Per-tree change set
   // ═════════════════════════════════════════════════════════════════════

   /// Tracks modifications for a single tree within a transaction.
   /// Each tree opened during a transaction gets its own change_set, indexed
   /// by position in the transaction's _change_sets vector.
   struct change_set
   {
      std::optional<write_cursor>         cursor;
      std::optional<detail::write_buffer> buffer;

      /// If this tree was obtained from a parent tree, records how to write
      /// the subtree root back into the parent on commit.
      struct parent_link
      {
         uint32_t    parent_cs_index;  ///< index into transaction::_change_sets
         std::string key;              ///< key in parent where this subtree lives
      };
      std::optional<parent_link> parent;
      std::optional<uint32_t>    root_index;  ///< set for top-level roots only
   };

   // ═════════════════════════════════════════════════════════════════════
   // tree_handle — lightweight targeting scope into a transaction
   // ═════════════════════════════════════════════════════════════════════

   /// A non-owning reference to a specific tree within a transaction.
   /// Directs mutations and reads to that tree's change_set. Not a
   /// sub-transaction — has no commit/abort. The owning transaction
   /// manages all lifecycle and write-back.
   ///
   /// Obtained via transaction::primary(), tree_handle::open_subtree(),
   /// or tree_handle::create_subtree().
   class tree_handle
   {
     public:
      tree_handle(tree_handle&&) noexcept            = default;
      tree_handle& operator=(tree_handle&&) noexcept = default;
      tree_handle(const tree_handle&)                = delete;
      tree_handle& operator=(const tree_handle&)     = delete;

      // ── Mutations ─────────────────────────────────────────────────────
      void     insert(key_view key, value_view value);
      void     update(key_view key, value_view value);
      void     upsert(key_view key, value_view value);
      void     upsert_sorted(key_view key, value_view value);
      void     upsert_subtree(key_view key, tree subtree);
      void     upsert_subtree_sorted(key_view key, tree subtree);
      int      remove(key_view key);
      uint64_t remove_range(key_view lower, key_view upper);

      // ── Reads ─────────────────────────────────────────────────────────
      cursor read_cursor() const;
      cursor snapshot_cursor() const;

      template <ConstructibleBuffer T>
      std::optional<T> get(key_view key) const;

      int32_t get(key_view key, Buffer auto* buffer) const;
      bool    get(key_view key, std::invocable<value_view> auto&& lambda) const;

      cursor lower_bound(key_view key) const;
      cursor upper_bound(key_view key) const;
      bool   is_subtree(key_view key) const;

      // ── Subtree navigation ────────────────────────────────────────────
      tree_handle open_subtree(key_view key);
      tree_handle create_subtree(key_view key);

      // ── Diagnostics ───────────────────────────────────────────────────
      tree_context::stats get_stats();

     private:
      friend class transaction;
      tree_handle(transaction& tx, uint32_t cs_index) : _tx(&tx), _cs_index(cs_index) {}

      transaction* _tx;
      uint32_t     _cs_index;
   };

   // ═════════════════════════════════════════════════════════════════════
   // transaction_frame_ref — RAII sub-transaction guard
   // ═════════════════════════════════════════════════════════════════════

   /// RAII guard for a sub-transaction frame. Movable, not copyable.
   /// Delegates all operations to the owning transaction via dot notation.
   /// Destructor aborts if not explicitly committed or aborted.
   class transaction_frame_ref
   {
     public:
      transaction_frame_ref(const transaction_frame_ref&)            = delete;
      transaction_frame_ref& operator=(const transaction_frame_ref&) = delete;

      transaction_frame_ref(transaction_frame_ref&& o) noexcept : _tx(o._tx), _ended(o._ended)
      {
         o._ended = true;
      }

      transaction_frame_ref& operator=(transaction_frame_ref&&) = delete;

      ~transaction_frame_ref();

      // ── Mutations ─────────────────────────────────────────────────────

      void     insert(key_view key, value_view value);
      void     update(key_view key, value_view value);
      void     upsert(key_view key, value_view value);
      void     upsert_sorted(key_view key, value_view value);
      void     upsert(key_view key, sal::smart_ptr<sal::alloc_header> subtree_root);
      void     upsert_sorted(key_view key, sal::smart_ptr<sal::alloc_header> subtree_root);
      void     upsert_subtree(key_view key, tree subtree);
      void     upsert_subtree_sorted(key_view key, tree subtree);
      int      remove(key_view key);
      uint64_t remove_range(key_view lower, key_view upper);

      // ── Read access ───────────────────────────────────────────────────

      cursor read_cursor() const;
      cursor snapshot_cursor() const;

      template <ConstructibleBuffer T>
      std::optional<T> get(key_view key) const;

      int32_t get(key_view key, Buffer auto* buffer) const;
      bool    get(key_view key, std::invocable<value_view> auto&& lambda) const;

      /// Position a cursor at the first key >= query.
      cursor lower_bound(key_view key) const;

      /// Position a cursor at the first key > query.
      cursor upper_bound(key_view key) const;

      bool         is_subtree(key_view key) const;
      tree         get_subtree(key_view key) const;

      // ── Subtree navigation ────────────────────────────────────────────

      tree_handle open_subtree(key_view key);
      tree_handle create_subtree(key_view key);

      // ── Transaction control ───────────────────────────────────────────

      void                                commit() noexcept;
      void                                abort() noexcept;
      [[nodiscard]] transaction_frame_ref sub_transaction() noexcept;

      // ── Diagnostics ───────────────────────────────────────────────────

      tree_context::stats get_stats();

     private:
      friend class transaction;
      explicit transaction_frame_ref(transaction& tx) : _tx(&tx) {}

      transaction* _tx;
      bool         _ended = false;
   };

   // ═════════════════════════════════════════════════════════════════════
   // transaction
   // ═════════════════════════════════════════════════════════════════════

   class transaction
   {
     public:
      transaction(sal::allocator_session_ptr                             session,
	                  sal::smart_ptr<sal::alloc_header>                      root,
	                  std::function<void(sal::smart_ptr<sal::alloc_header>)> commit_func,
	                  std::function<void()>                                  rollback_func,
	                  tx_mode                                                mode = tx_mode::expect_success,
	                  uint64_t                                               epoch_base = 0)
	          : _session(std::move(session)),
	            _commit_func(std::move(commit_func)),
	            _rollback_func(std::move(rollback_func)),
	            _mode(mode),
	            _epoch_base(epoch_base)
      {
         init_primary_cs(std::move(root));
      }

      transaction(const transaction&)            = delete;
      transaction& operator=(const transaction&) = delete;
      transaction(transaction&&) noexcept        = default;
      transaction& operator=(transaction&&)      = delete;

      ~transaction() { abort(); }

      // ── Primary handle ────────────────────────────────────────────────

      tree_handle primary() { return tree_handle(*this, _primary_index); }

      /// Open an additional top-level root for multi-root transactions.
      /// Batch mode only. Roots must be opened in ascending index order.
      tree_handle open_root(uint32_t root_index);

      // ── Primary tree mutations (backward-compatible API) ──────────────

      void insert(key_view key, value_view value)
      {
         do_write(_primary_index, key, value, &write_cursor::insert);
      }

      void update(key_view key, value_view value)
      {
         do_write(_primary_index, key, value, &write_cursor::update);
      }

      void upsert(key_view key, value_view value)
      {
         do_write(_primary_index, key, value, &write_cursor::upsert);
      }

      void upsert_sorted(key_view key, value_view value)
      {
         do_write(_primary_index, key, value, &write_cursor::upsert_sorted);
      }

      void upsert(key_view key, sal::smart_ptr<sal::alloc_header> subtree_root)
      {
         auto& cs = cs_at(_primary_index);
         assert(_mode == tx_mode::expect_success && "subtree upsert not supported in buffered mode");
         cs.cursor->upsert(key, std::move(subtree_root));
      }

      void upsert_subtree(key_view key, tree subtree)
      {
         upsert(key, std::move(subtree).take_root());
      }

      void upsert_sorted(key_view key, sal::smart_ptr<sal::alloc_header> subtree_root)
      {
         auto& cs = cs_at(_primary_index);
         assert(_mode == tx_mode::expect_success && "subtree upsert not supported in buffered mode");
         cs.cursor->upsert_sorted(key, std::move(subtree_root));
      }

      void upsert_subtree_sorted(key_view key, tree subtree)
      {
         upsert_sorted(key, std::move(subtree).take_root());
      }

      int remove(key_view key)
      {
         return do_remove(_primary_index, key);
      }

      uint64_t remove_range(key_view lower, key_view upper)
      {
         return do_remove_range(_primary_index, lower, upper);
      }

      // ── Primary tree read access (backward-compatible) ────────────────

      cursor read_cursor() const { return cs_at(_primary_index).cursor->read_cursor(); }
      cursor snapshot_cursor() const { return read_cursor(); }
      value_pin pin_values() const { return value_pin(_session); }

      template <ConstructibleBuffer T>
      std::optional<T> get(key_view key) const
      {
         return do_get<T>(_primary_index, key);
      }

      int32_t get(key_view key, Buffer auto* buffer) const
      {
         return do_get(_primary_index, key, buffer);
      }

      bool get(key_view key, std::invocable<value_view> auto&& lambda) const
      {
         return do_get(_primary_index, key, std::forward<decltype(lambda)>(lambda));
      }

      cursor lower_bound(key_view key) const { return do_bound(_primary_index, key, false); }

      cursor upper_bound(key_view key) const { return do_bound(_primary_index, key, true); }

      bool is_subtree(key_view key) const
      {
         return do_is_subtree(_primary_index, key);
      }

      tree get_subtree(key_view key) const
      {
         auto& cs = cs_at(_primary_index);
         if (cs.buffer)
         {
            const auto* entry = cs.buffer->get(key);
            if (entry)
               return tree(cs.cursor->root().session());
         }
         return tree(cs.cursor->get_subtree(key));
      }

      // ── Transaction control ───────────────────────────────────────────

      void commit()
      {
         if (_commit_func)
         {
            // Batch/micro: commit subtrees bottom-up, then commit primary
            commit_subtrees_bottom_up();

            auto& pcs = cs_at(_primary_index);
            if (pcs.buffer && !pcs.buffer->empty())
               merge_buffer_to_persistent(pcs);

            _commit_func(pcs.cursor->take_root());
            _commit_func   = nullptr;
            _rollback_func = nullptr;

            commit_additional_roots();
         }
      }

      tree get_tree()
      {
         commit_subtrees_bottom_up();

         auto& pcs = cs_at(_primary_index);
         if (pcs.buffer && !pcs.buffer->empty())
            merge_buffer_to_persistent(pcs);

         return tree(pcs.cursor->root());
      }

      void abort() noexcept
      {
         while (!_frames.empty())
            abort_frame();

         if (_rollback_func)
         {
            _rollback_func();
            _rollback_func = nullptr;
            _commit_func   = nullptr;
         }
         abort_additional_roots();
      }

      [[nodiscard]] transaction_frame_ref sub_transaction() noexcept
      {
         push_frame();
         return transaction_frame_ref(*this);
      }

      // ── Diagnostics ───────────────────────────────────────────────────

      tree_context::stats get_stats() { return cs_at(_primary_index).cursor->get_stats(); }

     private:
      friend class tree_handle;
      friend class transaction_frame_ref;
      template <class> friend class basic_write_session;

      // ── Change set storage ────────────────────────────────────────────

      sal::allocator_session_ptr _session;
      std::vector<change_set>   _change_sets;
      uint32_t                  _primary_index = 0;

      change_set&       cs_at(uint32_t idx) { return _change_sets[idx]; }
      const change_set& cs_at(uint32_t idx) const { return _change_sets[idx]; }

      // ── Change set initialization ───────────────────────────────────

      void init_primary_cs(sal::smart_ptr<sal::alloc_header> root)
      {
         change_set cs;
         // Always emplace with the root smart_ptr so the txn's _ver
         // (attached by start_transaction's make_unique_root) travels into
         // the write_cursor even when _adr is null on a fresh database.
         // For modes that didn't allocate a ver, the smart_ptr is empty
         // either way — no harm.
	         cs.cursor.emplace(std::move(root), _epoch_base);
         if (uses_buffer())
            cs.buffer.emplace();
         _change_sets.push_back(std::move(cs));
      }

      // ── Subtree management ────────────────────────────────────────────

      /// Find an existing subtree change_set opened from (parent_idx, key).
      std::optional<uint32_t> find_subtree_cs(uint32_t parent_idx, key_view key) const
      {
         for (uint32_t i = 0; i < _change_sets.size(); ++i)
         {
            auto& cs = _change_sets[i];
            if (cs.parent && cs.parent->parent_cs_index == parent_idx &&
                cs.parent->key == key)
               return i;
         }
         return std::nullopt;
      }

      /// Add a subtree change_set for (parent_idx, key), reusing existing if found.
      /// If root is provided, uses it; otherwise creates an empty tree.
      uint32_t add_subtree_cs(uint32_t                                         parent_idx,
                              key_view                                         key,
                              std::optional<sal::smart_ptr<sal::alloc_header>> root)
      {
         auto existing = find_subtree_cs(parent_idx, key);
         if (existing)
            return *existing;

	         change_set cs;
	         if (root)
	            cs.cursor.emplace(std::move(*root), _epoch_base);
	         else
	            cs.cursor.emplace(_session, _epoch_base);
         if (uses_buffer())
            cs.buffer.emplace();
         cs.parent = change_set::parent_link{parent_idx, std::string(key)};

         uint32_t idx = static_cast<uint32_t>(_change_sets.size());
         _change_sets.push_back(std::move(cs));
         return idx;
      }

      uint32_t open_subtree_impl(uint32_t parent_idx, key_view key)
      {
         auto sub_ptr = cs_at(parent_idx).cursor->get_subtree(key);
         assert(sub_ptr && "key is not a subtree");
         return add_subtree_cs(parent_idx, key, std::move(sub_ptr));
      }

      uint32_t create_subtree_impl(uint32_t parent_idx, key_view key)
      {
         return add_subtree_cs(parent_idx, key, std::nullopt);
      }

      // ── Subtree commit (bottom-up) ────────────────────────────────────

      /// Commit all subtree change sets bottom-up: children before parents.
      /// Each committed subtree produces a new root that is upserted into its
      /// parent's change set under the recorded key.
      void commit_subtrees_bottom_up()
      {
         std::vector<bool> committed(_change_sets.size(), false);

         // Mark top-level entries (no parent) as not-a-subtree
         for (size_t i = 0; i < _change_sets.size(); ++i)
         {
            if (!_change_sets[i].parent)
               committed[i] = true;
         }

         bool progress = true;
         while (progress)
         {
            progress = false;
            for (size_t i = 0; i < _change_sets.size(); ++i)
            {
               if (committed[i])
                  continue;

               auto& cs = _change_sets[i];

               // Check if all children of this subtree are committed
               bool children_done = true;
               for (size_t j = 0; j < _change_sets.size(); ++j)
               {
                  if (committed[j])
                     continue;
                  auto& child = _change_sets[j];
                  if (child.parent &&
                      child.parent->parent_cs_index == static_cast<uint32_t>(i))
                  {
                     children_done = false;
                     break;
                  }
               }

               if (!children_done)
                  continue;

               // Commit this subtree: merge buffer, get new root
               if (cs.buffer && !cs.buffer->empty())
                  merge_buffer_to_persistent(cs);

               auto new_root = cs.cursor->root();

               // Write the new subtree root back into the parent's tree
               if (cs.parent)
               {
                  auto& parent_cs = cs_at(cs.parent->parent_cs_index);
                  parent_cs.cursor->upsert(cs.parent->key, std::move(new_root));
               }

               committed[i] = true;
               progress     = true;
            }
         }
      }

      // ── Internal mutation methods ─────────────────────────────────────

      using cursor_write_fn = void (write_cursor::*)(key_view, value_view);

      void do_write(uint32_t idx, key_view key, value_view value, cursor_write_fn op)
      {
         auto& cs = cs_at(idx);
         if (cs.buffer)
            micro_put(cs, key, value);
         else
            ((*cs.cursor).*op)(key, value);
      }

      int do_remove(uint32_t idx, key_view key)
      {
         auto& cs = cs_at(idx);
         if (cs.buffer)
            return micro_remove(cs, key);
         return cs.cursor->remove(key);
      }

      uint64_t do_remove_range(uint32_t idx, key_view lower, key_view upper)
      {
         auto& cs = cs_at(idx);
         if (cs.buffer)
            return micro_remove_range(cs, lower, upper);
         return cs.cursor->remove_range(lower, upper);
      }

      // ── Internal read methods ─────────────────────────────────────────

      template <ConstructibleBuffer T>
      std::optional<T> do_get(uint32_t idx, key_view key) const
      {
         auto& cs = cs_at(idx);
         if (cs.buffer)
         {
            const auto* entry = cs.buffer->get(key);
            if (entry)
            {
               if (entry->is_tombstone())
                  return std::nullopt;
               auto val = entry->value();
               T    result;
               result.resize(val.size());
               std::memcpy(result.data(), val.data(), val.size());
               return result;
            }
         }
         auto result = cs.cursor->get<T>(key);
         return result;
      }

      int32_t do_get(uint32_t idx, key_view key, Buffer auto* buffer) const
      {
         auto& cs = cs_at(idx);
         if (cs.buffer)
         {
            const auto* entry = cs.buffer->get(key);
            if (entry)
            {
               if (entry->is_tombstone())
                  return cursor::value_not_found;
               auto val = entry->value();
               buffer->resize(val.size());
               std::memcpy(buffer->data(), val.data(), val.size());
               return static_cast<int32_t>(val.size());
            }
         }
         auto result = cs.cursor->get(key, buffer);
         return result;
      }

      bool do_get(uint32_t idx, key_view key, std::invocable<value_view> auto&& lambda) const
      {
         auto& cs = cs_at(idx);
         if (cs.buffer)
         {
            const auto* entry = cs.buffer->get(key);
            if (entry)
            {
               if (entry->is_tombstone())
                  return false;
               auto val = entry->value();
               lambda(val);
               return true;
            }
         }
         return cs.cursor->get(key, std::forward<decltype(lambda)>(lambda));
      }

      cursor do_bound(uint32_t idx, key_view key, bool is_upper) const
      {
         auto& cs = cs_at(idx);
         cursor c(cs.cursor->root());
         if (is_upper)
            c.upper_bound(key);
         else
            c.lower_bound(key);
         return c;
      }

      bool do_is_subtree(uint32_t idx, key_view key) const
      {
         auto& cs = cs_at(idx);
         if (cs.buffer)
         {
            const auto* entry = cs.buffer->get(key);
            if (entry)
               return false;
         }
         return cs.cursor->is_subtree(key);
      }

      // ── Frame stack for sub-transactions ──────────────────────────────

      struct frame
      {
         detail::write_buffer::saved_state              buf_state;
         // Number of change_sets at push time; entries beyond this are dropped on abort.
         uint32_t                                       change_sets_size = 0;
         // Cursor roots of all change_sets at push time, indexed in parallel.
         std::vector<sal::smart_ptr<sal::alloc_header>> cs_roots;
      };

      bool uses_buffer() const noexcept { return _mode != tx_mode::expect_success; }

      void push_frame() noexcept
      {
         frame f;
         if (uses_buffer())
         {
            auto& cs    = cs_at(_primary_index);
            f.buf_state = cs.buffer->save();
            cs.buffer->bump_generation();
         }
         f.change_sets_size = static_cast<uint32_t>(_change_sets.size());
         f.cs_roots.reserve(_change_sets.size());
         for (auto& cs : _change_sets)
            f.cs_roots.push_back(cs.cursor->root());
         _frames.push_back(std::move(f));
      }

      void commit_frame() noexcept
      {
         assert(!_frames.empty());
         _frames.pop_back();
      }

      void abort_frame() noexcept
      {
         assert(!_frames.empty());
         auto f = std::move(_frames.back());
         _frames.pop_back();

         // Drop subtree change_sets opened within this frame.
         _change_sets.resize(f.change_sets_size);

         // Restore every cursor root to its pre-frame snapshot.
         for (uint32_t i = 0; i < f.change_sets_size; ++i)
            _change_sets[i].cursor.emplace(std::move(f.cs_roots[i]));

         // Restore primary write buffer in buffered mode.
         if (uses_buffer())
            cs_at(_primary_index).buffer->restore(f.buf_state);
      }

      // ── Micro mode helpers ────────────────────────────────────────────

      void micro_put(change_set& cs, key_view key, value_view value)
      {
         assert(cs.buffer);
         const auto* existing = cs.buffer->get(key);
         if (existing)
         {
            cs.buffer->put(key, value, false);
         }
         else
         {
            bool existed = cs.cursor->get<std::string>(key).has_value();
            cs.buffer->put(key, value, existed);
         }
      }

      int micro_remove(change_set& cs, key_view key)
      {
         assert(cs.buffer);
         const auto* existing = cs.buffer->get(key);
         if (existing)
         {
            if (existing->is_tombstone())
               return -1;
            int result = existing->is_data() ? static_cast<int>(existing->data_len) : 0;
            cs.buffer->erase(key, false);
            return result;
         }
         else
         {
            auto val = cs.cursor->get<std::string>(key);
            if (val)
            {
               int result = static_cast<int>(val->size());
               cs.buffer->erase(key, true);
               return result;
            }
            else
            {
               cs.buffer->erase(key, false);
               return -1;
            }
         }
      }

      static constexpr uint64_t tombstone_threshold = 256;

      uint64_t micro_remove_range(change_set& cs, key_view lower, key_view upper)
      {
         assert(cs.buffer);

         cursor   rc(cs.cursor->root());
         uint64_t persistent_count = rc.count_keys(lower, upper);

         if (persistent_count <= tombstone_threshold)
         {
            uint64_t removed = 0;

            cursor pc(cs.cursor->root());
            pc.lower_bound(lower);
            while (!pc.is_end())
            {
               auto k = pc.key();
               if (!upper.empty() && k >= upper)
                  break;

               const auto* entry = cs.buffer->get(k);
               if (!entry || !entry->is_tombstone())
               {
                  cs.buffer->erase(k, true);
                  ++removed;
               }
               pc.next();
            }

            std::vector<std::string> buf_insert_keys;
            auto                     it  = cs.buffer->lower_bound(lower);
            auto                     end = cs.buffer->end();
            while (it != end)
            {
               auto k = it.key();
               if (!upper.empty() && k >= upper)
                  break;
               auto& e = it.value();
               if (e.type == detail::buffer_entry::insert)
                  buf_insert_keys.emplace_back(k);
               ++it;
            }
            for (auto& k : buf_insert_keys)
            {
               cs.buffer->erase(k, false);
               ++removed;
            }

            return removed;
         }
         else
         {
            // Forced flush path: cursor->remove_range is a tree-touching
            // op. Materialize a per-txn ver even when the buffer
            // happens to be empty (merge_buffer_to_persistent only fires
            // when there's something to flush).
            if (!cs.buffer->empty())
               merge_buffer_to_persistent(cs);
            else
               materialize_txn_version();
            return cs.cursor->remove_range(lower, upper);
         }
      }

      void merge_buffer_to_persistent(change_set& cs)
      {
         // First tree-touching action for an expect_failure txn — materialize
         // a per-txn version on the working root before any
         // COW write fires. No-op if a ver was already allocated (either
         // expect_success at start_transaction, or an earlier flush in
         // this same expect_failure txn).
         materialize_txn_version();

         auto it  = cs.buffer->begin();
         auto end = cs.buffer->end();
         for (; it != end; ++it)
         {
            auto& entry = it.value();
            auto  key   = it.key();

            if (entry.is_data())
            {
               auto val = entry.value();
               cs.cursor->upsert_sorted(key, val);
            }
            else if (entry.type == detail::buffer_entry::tombstone)
            {
               cs.cursor->remove(key);
            }
         }

         if (_frames.empty())
            cs.buffer->clear();
         else
            cs.buffer->soft_clear();
      }

	      std::function<void(sal::smart_ptr<sal::alloc_header>)>   _commit_func;
	      std::function<void()>                                    _rollback_func;
	      tx_mode                                                  _mode = tx_mode::expect_success;
	      uint64_t                                                 _epoch_base = 0;
	      std::vector<frame>                                       _frames;

      // ── Multi-root support ────────────────────────────────────────────

      write_session* _ws = nullptr;

      // Type-erased lock handle. The fill site (basic_write_session::open_root)
      // is templated on LockPolicy, so it knows the concrete mutex type and
      // captures the unlock function pointer at that point. Storage is
      // type-agnostic so the transaction class itself doesn't need to be a
      // template — preserving the public `psitri::transaction` name without
      // forcing every consumer to specify a LockPolicy.
      struct held_lock
      {
         uint32_t root_index;
         uint32_t cs_index;
         void*    lock;
         void   (*unlock_fn)(void*);
      };
      std::vector<held_lock> _held_locks;
      uint32_t               _max_held_root = 0;

      // Has this txn allocated its own per-txn version yet? Set true by
      // start_transaction(expect_success) (which calls make_unique_root
      // up front) and by the lazy materialize_txn_version path. Distinguishes
      // "the working root carries my ver" from "the working root carries
      // an inherited published ver from get_root."
      bool _has_txn_version = false;

      // Defined in write_session_impl.hpp (needs write_session access)
      void commit_additional_roots();
      void abort_additional_roots() noexcept;

      /// Lazy per-txn version materialization for expect_failure mode. If
      /// the primary working root doesn't yet carry a txn-owned ver (txn
      /// has done no tree-touching work), allocate one via make_unique_root
      /// and attach it. Idempotent — no-op when a ver is already attached.
      /// Plugged into merge_buffer_to_persistent so the first tree-touch
      /// path in expect_failure ensures the version exists before any
      /// COW write fires.
      void materialize_txn_version();
   };

   // ═════════════════════════════════════════════════════════════════════
   // tree_handle inline implementations
   // ═════════════════════════════════════════════════════════════════════

   inline void tree_handle::insert(key_view key, value_view value)
   {
      _tx->do_write(_cs_index, key, value, &write_cursor::insert);
   }

   inline void tree_handle::update(key_view key, value_view value)
   {
      _tx->do_write(_cs_index, key, value, &write_cursor::update);
   }

   inline void tree_handle::upsert(key_view key, value_view value)
   {
      _tx->do_write(_cs_index, key, value, &write_cursor::upsert);
   }

   inline void tree_handle::upsert_sorted(key_view key, value_view value)
   {
      _tx->do_write(_cs_index, key, value, &write_cursor::upsert_sorted);
   }

   inline void tree_handle::upsert_subtree(key_view key, tree subtree)
   {
      _tx->cs_at(_cs_index).cursor->upsert(key, std::move(subtree).take_root());
   }

   inline void tree_handle::upsert_subtree_sorted(key_view key, tree subtree)
   {
      _tx->cs_at(_cs_index).cursor->upsert_sorted(key, std::move(subtree).take_root());
   }

   inline int tree_handle::remove(key_view key)
   {
      return _tx->do_remove(_cs_index, key);
   }

   inline uint64_t tree_handle::remove_range(key_view lower, key_view upper)
   {
      return _tx->do_remove_range(_cs_index, lower, upper);
   }

   inline cursor tree_handle::read_cursor() const
   {
      return _tx->cs_at(_cs_index).cursor->read_cursor();
   }

   inline cursor tree_handle::snapshot_cursor() const { return read_cursor(); }

   template <ConstructibleBuffer T>
   std::optional<T> tree_handle::get(key_view key) const
   {
      return _tx->do_get<T>(_cs_index, key);
   }

   inline int32_t tree_handle::get(key_view key, Buffer auto* buffer) const
   {
      return _tx->do_get(_cs_index, key, buffer);
   }

   inline bool tree_handle::get(key_view key,
                                std::invocable<value_view> auto&& lambda) const
   {
      return _tx->do_get(_cs_index, key, std::forward<decltype(lambda)>(lambda));
   }

   inline cursor tree_handle::lower_bound(key_view key) const
   {
      return _tx->do_bound(_cs_index, key, false);
   }

   inline cursor tree_handle::upper_bound(key_view key) const
   {
      return _tx->do_bound(_cs_index, key, true);
   }

   inline bool tree_handle::is_subtree(key_view key) const
   {
      return _tx->do_is_subtree(_cs_index, key);
   }

   inline tree_handle tree_handle::open_subtree(key_view key)
   {
      return tree_handle(*_tx, _tx->open_subtree_impl(_cs_index, key));
   }

   inline tree_handle tree_handle::create_subtree(key_view key)
   {
      return tree_handle(*_tx, _tx->create_subtree_impl(_cs_index, key));
   }

   inline tree_context::stats tree_handle::get_stats()
   {
      return _tx->cs_at(_cs_index).cursor->get_stats();
   }

   // ═════════════════════════════════════════════════════════════════════
   // transaction_frame_ref inline implementations
   // ═════════════════════════════════════════════════════════════════════

   inline transaction_frame_ref::~transaction_frame_ref()
   {
      if (!_ended)
         abort();
   }

   inline void transaction_frame_ref::insert(key_view key, value_view value)
   {
      _tx->insert(key, value);
   }
   inline void transaction_frame_ref::update(key_view key, value_view value)
   {
      _tx->update(key, value);
   }
   inline void transaction_frame_ref::upsert(key_view key, value_view value)
   {
      _tx->upsert(key, value);
   }
   inline void transaction_frame_ref::upsert_sorted(key_view key, value_view value)
   {
      _tx->upsert_sorted(key, value);
   }
   inline void transaction_frame_ref::upsert(key_view key,
                                             sal::smart_ptr<sal::alloc_header> subtree_root)
   {
      _tx->upsert(key, std::move(subtree_root));
   }
   inline void transaction_frame_ref::upsert_sorted(key_view key,
                                                    sal::smart_ptr<sal::alloc_header> subtree_root)
   {
      _tx->upsert_sorted(key, std::move(subtree_root));
   }
   inline void transaction_frame_ref::upsert_subtree(key_view key, tree subtree)
   {
      _tx->upsert_subtree(key, std::move(subtree));
   }
   inline void transaction_frame_ref::upsert_subtree_sorted(key_view key, tree subtree)
   {
      _tx->upsert_subtree_sorted(key, std::move(subtree));
   }
   inline int transaction_frame_ref::remove(key_view key) { return _tx->remove(key); }
   inline uint64_t transaction_frame_ref::remove_range(key_view lower, key_view upper)
   {
      return _tx->remove_range(lower, upper);
   }
   inline cursor transaction_frame_ref::read_cursor() const { return _tx->read_cursor(); }

   inline cursor transaction_frame_ref::snapshot_cursor() const { return read_cursor(); }

   inline cursor transaction_frame_ref::lower_bound(key_view key) const
   {
      return _tx->lower_bound(key);
   }
   inline cursor transaction_frame_ref::upper_bound(key_view key) const
   {
      return _tx->upper_bound(key);
   }

   template <ConstructibleBuffer T>
   std::optional<T> transaction_frame_ref::get(key_view key) const
   {
      return _tx->get<T>(key);
   }

   inline int32_t transaction_frame_ref::get(key_view key, Buffer auto* buffer) const
   {
      return _tx->get(key, buffer);
   }

   inline bool transaction_frame_ref::get(key_view key,
                                          std::invocable<value_view> auto&& lambda) const
   {
      return _tx->get(key, std::forward<decltype(lambda)>(lambda));
   }

   inline bool transaction_frame_ref::is_subtree(key_view key) const
   {
      return _tx->is_subtree(key);
   }
   inline tree transaction_frame_ref::get_subtree(key_view key) const
   {
      return _tx->get_subtree(key);
   }
   inline tree_handle transaction_frame_ref::open_subtree(key_view key)
   {
      return _tx->primary().open_subtree(key);
   }

   inline tree_handle transaction_frame_ref::create_subtree(key_view key)
   {
      return _tx->primary().create_subtree(key);
   }

   inline void transaction_frame_ref::commit() noexcept
   {
      if (!_ended)
      {
         _ended = true;
         _tx->commit_frame();
      }
   }

   inline void transaction_frame_ref::abort() noexcept
   {
      if (!_ended)
      {
         _ended = true;
         _tx->abort_frame();
      }
   }

   inline transaction_frame_ref transaction_frame_ref::sub_transaction() noexcept
   {
      return _tx->sub_transaction();
   }

   inline tree_context::stats transaction_frame_ref::get_stats() { return _tx->get_stats(); }

   class write_transaction
   {
     public:
      write_transaction(const write_transaction&)            = delete;
      write_transaction& operator=(const write_transaction&) = delete;
      write_transaction(write_transaction&&) noexcept        = default;
      write_transaction& operator=(write_transaction&&)      = delete;

      void insert(key_view key, value_view value) { _tx.insert(key, value); }
      void update(key_view key, value_view value) { _tx.update(key, value); }
      void upsert(key_view key, value_view value) { _tx.upsert(key, value); }
      void upsert_sorted(key_view key, value_view value) { _tx.upsert_sorted(key, value); }
      void upsert_subtree(key_view key, tree subtree)
      {
         _tx.upsert_subtree(key, std::move(subtree));
      }
      void upsert_subtree_sorted(key_view key, tree subtree)
      {
         _tx.upsert_subtree_sorted(key, std::move(subtree));
      }
      int remove(key_view key) { return _tx.remove(key); }
      uint64_t remove_range(key_view lower, key_view upper)
      {
         return _tx.remove_range(lower, upper);
      }

      bool get(key_view key, std::invocable<value_view> auto&& lambda) const
      {
         return _tx.get(key, std::forward<decltype(lambda)>(lambda));
      }
      template <ConstructibleBuffer T>
      std::optional<T> get(key_view key) const
      {
         return _tx.get<T>(key);
      }
      int32_t get(key_view key, Buffer auto* buffer) const { return _tx.get(key, buffer); }

      psitri::cursor cursor() const { return _tx.read_cursor(); }
      psitri::cursor snapshot_cursor() const { return _tx.snapshot_cursor(); }
      value_pin      pin_values() const { return _tx.pin_values(); }
      bool           is_subtree(key_view key) const { return _tx.is_subtree(key); }
      tree           get_subtree(key_view key) const { return _tx.get_subtree(key); }
      tree           get_tree() { return _tx.get_tree(); }
      void           abort() noexcept { _tx.abort(); }

     private:
      template <class>
      friend class basic_write_session;

      explicit write_transaction(transaction&& tx) : _tx(std::move(tx)) {}

      transaction _tx;
   };

}  // namespace psitri
