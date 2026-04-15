#pragma once
#include <cassert>
#include <functional>
#include <memory>
#include <optional>
#include <psitri/detail/write_buffer.hpp>
#include <psitri/tx_mode.hpp>
#include <psitri/write_cursor.hpp>
#include <vector>

namespace psitri
{
   class write_session;

   class transaction;

   // ═════════════════════════════════════════════════════════════════════
   // OCC read-set tracking
   // ═════════════════════════════════════════════════════════════════════

   /// A single point-read observation for OCC validation.
   struct read_entry
   {
      std::string      key;
      uint64_t         version;    ///< 0=inline, latest_version()=VN, UINT64_MAX=missing
      sal::ptr_address leaf_addr;  ///< leaf containing the key
   };

   /// A lower/upper-bound predicate observation for OCC phantom detection.
   /// Records the query key and the key it resolved to, so validation
   /// can re-execute the bound and detect inserted phantoms.
   struct lb_entry
   {
      std::string query_key;    ///< the key we searched for
      std::string found_key;    ///< the key we landed on (empty if at_end)
      bool        at_end;       ///< whether the bound hit end
      bool        is_upper;     ///< true = upper_bound, false = lower_bound
   };

   /// Accumulated read observations for an OCC transaction.
   struct read_set
   {
      std::vector<read_entry> entries;
      std::vector<lb_entry>   lower_bounds;

      void record(key_view key, uint64_t version, sal::ptr_address leaf_addr)
      {
         entries.push_back({std::string(key), version, leaf_addr});
      }

      void record_bound(key_view query, key_view found, bool at_end, bool is_upper)
      {
         lower_bounds.push_back({std::string(query), std::string(found), at_end, is_upper});
      }

      bool empty() const noexcept { return entries.empty() && lower_bounds.empty(); }
   };

   // ═════════════════════════════════════════════════════════════════════
   // Per-tree change set
   // ═════════════════════════════════════════════════════════════════════

   /// Tracks modifications and reads for a single tree within a transaction.
   /// Each tree_id opened during a transaction gets its own change_set.
   struct change_set
   {
      std::optional<write_cursor>       cursor;
      std::optional<detail::write_buffer> buffer;
      read_set                          reads;

      /// If this tree was obtained from a parent, records how to write
      /// the new tree_id back on commit.
      struct parent_link
      {
         sal::tree_id parent_tid;  ///< tree_id of the parent tree
         std::string  key;         ///< key in parent where this subtree lives
      };
      std::optional<parent_link> parent;
   };

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
      int      remove(key_view key);
      uint64_t remove_range(key_view lower, key_view upper);

      // ── Read access ───────────────────────────────────────────────────

      cursor read_cursor() const;

      template <ConstructibleBuffer T>
      std::optional<T> get(key_view key) const;

      int32_t get(key_view key, Buffer auto* buffer) const;

      /// Position a cursor at the first key >= query and record the
      /// predicate for OCC phantom detection.
      cursor lower_bound(key_view key) const;

      /// Position a cursor at the first key > query and record the
      /// predicate for OCC phantom detection.
      cursor upper_bound(key_view key) const;

      bool                              is_subtree(key_view key) const;
      sal::smart_ptr<sal::alloc_header> get_subtree(key_view key) const;
      write_cursor                      get_subtree_cursor(key_view key) const;

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

   /// OCC commit function type: receives the write buffer and read set,
   /// validates per-key, applies writes to the current tree, publishes.
   using occ_commit_fn = std::function<void(const detail::write_buffer*, const read_set&)>;

   class transaction
   {
     public:
      transaction(sal::allocator_session_ptr                             session,
                  sal::smart_ptr<sal::alloc_header>                      root,
                  std::function<void(sal::smart_ptr<sal::alloc_header>)> commit_func,
                  std::function<void()>                                  rollback_func,
                  tx_mode                                                mode = tx_mode::batch)
          : _commit_func(std::move(commit_func)),
            _rollback_func(std::move(rollback_func)),
            _mode(mode)
      {
         _primary_tid = root.get_tree_id();

         auto& cs = get_or_create_change_set(_primary_tid);
         if (root)
            cs.cursor.emplace(std::move(root));
         else
            cs.cursor.emplace(std::move(session));

         if (uses_buffer())
            cs.buffer.emplace();
      }

      /// OCC-specific constructor: provides an MVCC-aware commit function
      /// that validates the read set per-key and applies writes to the current tree.
      transaction(sal::allocator_session_ptr          session,
                  sal::smart_ptr<sal::alloc_header>   root,
                  occ_commit_fn                       occ_commit,
                  std::function<void()>               rollback_func)
          : _rollback_func(std::move(rollback_func)),
            _mode(tx_mode::occ)
      {
         _occ_commit_func = std::move(occ_commit);
         _primary_tid = root.get_tree_id();

         auto& cs = get_or_create_change_set(_primary_tid);
         if (root)
            cs.cursor.emplace(std::move(root));
         else
            cs.cursor.emplace(std::move(session));

         cs.buffer.emplace();
      }

      transaction(const transaction&)            = delete;
      transaction& operator=(const transaction&) = delete;
      transaction(transaction&&) noexcept        = default;
      transaction& operator=(transaction&&)      = delete;

      ~transaction() { abort(); }

      // ── Primary tree mutations (backward-compatible API) ──────────────

      void insert(key_view key, value_view value)
      {
         insert(_primary_tid, key, value);
      }

      void update(key_view key, value_view value)
      {
         update(_primary_tid, key, value);
      }

      void upsert(key_view key, value_view value)
      {
         upsert(_primary_tid, key, value);
      }

      void upsert_sorted(key_view key, value_view value)
      {
         upsert_sorted(_primary_tid, key, value);
      }

      void upsert(key_view key, sal::smart_ptr<sal::alloc_header> subtree_root)
      {
         assert_no_child_frame();
         auto& cs = primary();
         assert(_mode == tx_mode::batch && "subtree upsert not supported in buffered mode");
         cs.cursor->upsert(key, std::move(subtree_root));
      }

      void upsert_sorted(key_view key, sal::smart_ptr<sal::alloc_header> subtree_root)
      {
         assert_no_child_frame();
         auto& cs = primary();
         assert(_mode == tx_mode::batch && "subtree upsert not supported in buffered mode");
         cs.cursor->upsert_sorted(key, std::move(subtree_root));
      }

      int remove(key_view key)
      {
         return remove(_primary_tid, key);
      }

      uint64_t remove_range(key_view lower, key_view upper)
      {
         return remove_range(_primary_tid, lower, upper);
      }

      // ── Multi-tree mutations (tree_id-parameterized) ──────────────────

      void insert(sal::tree_id tid, key_view key, value_view value)
      {
         assert_no_child_frame();
         auto& cs = get_change_set(tid);
         if (cs.buffer)
            micro_put(cs, key, value);
         else
            cs.cursor->insert(key, value);
      }

      void update(sal::tree_id tid, key_view key, value_view value)
      {
         assert_no_child_frame();
         auto& cs = get_change_set(tid);
         if (cs.buffer)
            micro_put(cs, key, value);
         else
            cs.cursor->update(key, value);
      }

      void upsert(sal::tree_id tid, key_view key, value_view value)
      {
         assert_no_child_frame();
         auto& cs = get_change_set(tid);
         if (cs.buffer)
            micro_put(cs, key, value);
         else
            cs.cursor->upsert(key, value);
      }

      void upsert_sorted(sal::tree_id tid, key_view key, value_view value)
      {
         assert_no_child_frame();
         auto& cs = get_change_set(tid);
         if (cs.buffer)
            micro_put(cs, key, value);
         else
            cs.cursor->upsert_sorted(key, value);
      }

      int remove(sal::tree_id tid, key_view key)
      {
         assert_no_child_frame();
         auto& cs = get_change_set(tid);
         if (cs.buffer)
            return micro_remove(cs, key);
         return cs.cursor->remove(key);
      }

      uint64_t remove_range(sal::tree_id tid, key_view lower, key_view upper)
      {
         assert_no_child_frame();
         auto& cs = get_change_set(tid);
         if (cs.buffer)
            return micro_remove_range(cs, lower, upper);
         return cs.cursor->remove_range(lower, upper);
      }

      // ── Primary tree read access (backward-compatible) ────────────────

      cursor read_cursor() const { return primary().cursor->read_cursor(); }

      template <ConstructibleBuffer T>
      std::optional<T> get(key_view key) const
      {
         return get<T>(_primary_tid, key);
      }

      int32_t get(key_view key, Buffer auto* buffer) const
      {
         return get(_primary_tid, key, buffer);
      }

      cursor lower_bound(key_view key) const
      {
         return lower_bound(_primary_tid, key);
      }

      cursor upper_bound(key_view key) const
      {
         return upper_bound(_primary_tid, key);
      }

      bool is_subtree(key_view key) const
      {
         return is_subtree(_primary_tid, key);
      }

      sal::smart_ptr<sal::alloc_header> get_subtree(key_view key) const
      {
         return get_subtree(_primary_tid, key);
      }

      write_cursor get_subtree_cursor(key_view key) const
      {
         return primary().cursor->get_subtree_cursor(key);
      }

      // ── Multi-tree read access ────────────────────────────────────────

      template <ConstructibleBuffer T>
      std::optional<T> get(sal::tree_id tid, key_view key) const
      {
         auto& cs = get_change_set(tid);
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
         track_read(cs, key);
         return result;
      }

      int32_t get(sal::tree_id tid, key_view key, Buffer auto* buffer) const
      {
         auto& cs = get_change_set(tid);
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
         track_read(cs, key);
         return result;
      }

      bool is_subtree(sal::tree_id tid, key_view key) const
      {
         auto& cs = get_change_set(tid);
         if (cs.buffer)
         {
            const auto* entry = cs.buffer->get(key);
            if (entry)
               return false;
         }
         return cs.cursor->is_subtree(key);
      }

      sal::smart_ptr<sal::alloc_header> get_subtree(sal::tree_id tid, key_view key) const
      {
         auto& cs = get_change_set(tid);
         if (cs.buffer)
         {
            const auto* entry = cs.buffer->get(key);
            if (entry)
               return sal::smart_ptr<sal::alloc_header>(
                   cs.cursor->root().session(), sal::null_ptr_address);
         }
         return cs.cursor->get_subtree(key);
      }

      /// Position a cursor at lower_bound on the persistent tree and
      /// record the predicate for OCC phantom detection.
      cursor lower_bound(sal::tree_id tid, key_view key) const
      {
         auto& cs = get_change_set(tid);
         cursor c(cs.cursor->root());
         c.lower_bound(key);
         track_bound(cs, key, c, false);
         return c;
      }

      /// Position a cursor at upper_bound on the persistent tree and
      /// record the predicate for OCC phantom detection.
      cursor upper_bound(sal::tree_id tid, key_view key) const
      {
         auto& cs = get_change_set(tid);
         cursor c(cs.cursor->root());
         c.upper_bound(key);
         track_bound(cs, key, c, true);
         return c;
      }

      /// Open a subtree for modification within this transaction.
      /// Returns the subtree's tree_id. The subtree gets its own change_set
      /// with a parent link back to (tid, key) so commit can propagate.
      sal::tree_id open_subtree(sal::tree_id parent_tid, key_view key)
      {
         auto& pcs = get_change_set(parent_tid);

         // Read the subtree tree_id from the parent tree
         auto sub_ptr = pcs.cursor->get_subtree(key);
         if (!sub_ptr)
            return sal::null_tree_id;

         auto sub_tid = sub_ptr.get_tree_id();

         // Create change_set for the subtree if it doesn't exist
         auto it = find_change_set(sub_tid);
         if (it == _change_sets.end())
         {
            change_set cs;
            cs.cursor.emplace(std::move(sub_ptr));
            if (uses_buffer())
               cs.buffer.emplace();
            cs.parent = change_set::parent_link{parent_tid, std::string(key)};
            _change_sets.push_back({sub_tid, std::move(cs)});
         }

         return sub_tid;
      }

      /// Open a subtree by tree_id directly (no parent link).
      /// Use when the caller manages the tree_id independently.
      sal::tree_id open_tree(sal::smart_ptr<sal::alloc_header> root)
      {
         auto tid = root.get_tree_id();
         auto it  = find_change_set(tid);
         if (it == _change_sets.end())
         {
            change_set cs;
            cs.cursor.emplace(std::move(root));
            if (uses_buffer())
               cs.buffer.emplace();
            _change_sets.push_back({tid, std::move(cs)});
         }
         return tid;
      }

      // ── Transaction control ───────────────────────────────────────────

      void commit()
      {
         if (_occ_commit_func)
         {
            // OCC: commit subtrees bottom-up, then validate+apply primary
            commit_subtrees_bottom_up();

            auto& pcs = primary();
            _occ_commit_func(pcs.buffer ? &*pcs.buffer : nullptr, pcs.reads);
            _occ_commit_func = nullptr;
            _commit_func     = nullptr;
            _rollback_func   = nullptr;
            return;
         }
         if (_commit_func)
         {
            // Batch/micro: commit subtrees bottom-up, then commit primary
            commit_subtrees_bottom_up();

            auto& pcs = primary();
            if (pcs.buffer && !pcs.buffer->empty())
               merge_buffer_to_persistent(pcs);

            _commit_func(pcs.cursor->root());
            _commit_func   = nullptr;
            _rollback_func = nullptr;
         }
      }

      void abort() noexcept
      {
         while (!_frames.empty())
            abort_frame();

         if (_rollback_func)
         {
            _rollback_func();
            _rollback_func   = nullptr;
            _commit_func     = nullptr;
            _occ_commit_func = nullptr;
         }
      }

      [[nodiscard]] transaction_frame_ref sub_transaction() noexcept
      {
         push_frame();
         return transaction_frame_ref(*this);
      }

      // ── Diagnostics ───────────────────────────────────────────────────

      tree_context::stats get_stats() { return primary().cursor->get_stats(); }

     private:
      friend class transaction_frame_ref;

      // ── Change set storage ────────────────────────────────────────────

      /// Change sets stored as a vector of pairs (tree_id → change_set).
      /// Small number expected (primary + a few subtrees), so linear scan is fine.
      using cs_entry = std::pair<sal::tree_id, change_set>;
      std::vector<cs_entry> _change_sets;
      sal::tree_id          _primary_tid;

      change_set& primary() { return get_change_set(_primary_tid); }
      const change_set& primary() const { return get_change_set(_primary_tid); }

      typename std::vector<cs_entry>::iterator find_change_set(sal::tree_id tid)
      {
         return std::find_if(_change_sets.begin(), _change_sets.end(),
                             [&](const cs_entry& e) { return e.first == tid; });
      }

      typename std::vector<cs_entry>::const_iterator find_change_set(sal::tree_id tid) const
      {
         return std::find_if(_change_sets.begin(), _change_sets.end(),
                             [&](const cs_entry& e) { return e.first == tid; });
      }

      change_set& get_change_set(sal::tree_id tid)
      {
         auto it = find_change_set(tid);
         assert(it != _change_sets.end() && "change_set not found for tree_id");
         return it->second;
      }

      const change_set& get_change_set(sal::tree_id tid) const
      {
         auto it = find_change_set(tid);
         assert(it != _change_sets.end() && "change_set not found for tree_id");
         return it->second;
      }

      change_set& get_or_create_change_set(sal::tree_id tid)
      {
         auto it = find_change_set(tid);
         if (it != _change_sets.end())
            return it->second;
         _change_sets.push_back({tid, change_set{}});
         return _change_sets.back().second;
      }

      // ── Subtree commit (bottom-up) ────────────────────────────────────

      /// Commit all non-primary change sets bottom-up: children before parents.
      /// Each committed subtree produces a new root that is upserted into its
      /// parent's change set under the recorded key.
      void commit_subtrees_bottom_up()
      {
         // Simple approach: iterate until all subtrees are committed.
         // A subtree is ready to commit when none of its children are uncommitted.
         // With typically shallow nesting, this converges quickly.

         std::vector<bool> committed(_change_sets.size(), false);

         // Mark primary as not-a-subtree (committed last, separately)
         for (size_t i = 0; i < _change_sets.size(); ++i)
         {
            if (_change_sets[i].first == _primary_tid)
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

               auto& cs = _change_sets[i].second;

               // Check if all children of this subtree are committed
               bool children_done = true;
               for (size_t j = 0; j < _change_sets.size(); ++j)
               {
                  if (committed[j])
                     continue;
                  auto& child = _change_sets[j].second;
                  if (child.parent && child.parent->parent_tid == _change_sets[i].first)
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
                  auto& parent_cs = get_change_set(cs.parent->parent_tid);
                  parent_cs.cursor->upsert(cs.parent->key, std::move(new_root));
               }

               committed[i] = true;
               progress = true;
            }
         }
      }

      // ── Frame stack for sub-transactions ──────────────────────────────

      struct frame
      {
         sal::smart_ptr<sal::alloc_header> saved_root;
         detail::write_buffer::saved_state buf_state;
         bool micro_merged = false;
      };

      bool uses_buffer() const noexcept { return _mode != tx_mode::batch; }

      void push_frame() noexcept
      {
         auto& cs = primary();
         frame f;
         if (uses_buffer())
         {
            f.buf_state  = cs.buffer->save();
            f.saved_root = cs.cursor->root();
            cs.buffer->bump_generation();
         }
         else
         {
            f.saved_root = cs.cursor->root();
         }
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
         auto& cs = primary();
         auto f = std::move(_frames.back());
         _frames.pop_back();

         if (uses_buffer())
         {
            cs.buffer->restore(f.buf_state);
            if (f.micro_merged)
               cs.cursor.emplace(std::move(f.saved_root));
         }
         else
         {
            cs.cursor.emplace(std::move(f.saved_root));
         }
      }

      void assert_no_child_frame() const {}

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
            if (!cs.buffer->empty())
            {
               merge_buffer_to_persistent(cs);
               mark_frames_merged();
            }
            return cs.cursor->remove_range(lower, upper);
         }
      }

      void mark_frames_merged() noexcept
      {
         for (auto& f : _frames)
            f.micro_merged = true;
      }

      void merge_buffer_to_persistent(change_set& cs)
      {
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

      /// Track a persistent-tree read for OCC validation.
      void track_read(const change_set& cs, key_view key) const
      {
         if (_occ_commit_func)
         {
            auto info = cs.cursor->read_cursor().get_key_info(key);
            const_cast<change_set&>(cs).reads.record(key, info.version, info.leaf_addr);
         }
      }

      /// Track a lower_bound/upper_bound predicate for OCC phantom detection.
      void track_bound(const change_set& cs, key_view query, const cursor& c, bool is_upper) const
      {
         if (_occ_commit_func)
         {
            const_cast<change_set&>(cs).reads.record_bound(
                query, c.is_end() ? key_view{} : c.key(), c.is_end(), is_upper);
         }
      }

      std::function<void(sal::smart_ptr<sal::alloc_header>)>   _commit_func;
      occ_commit_fn                                            _occ_commit_func;
      std::function<void()>                                    _rollback_func;
      tx_mode                                                  _mode = tx_mode::batch;
      std::vector<frame>                                       _frames;
   };

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
   inline int transaction_frame_ref::remove(key_view key) { return _tx->remove(key); }
   inline uint64_t transaction_frame_ref::remove_range(key_view lower, key_view upper)
   {
      return _tx->remove_range(lower, upper);
   }
   inline cursor transaction_frame_ref::read_cursor() const { return _tx->read_cursor(); }

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

   inline bool transaction_frame_ref::is_subtree(key_view key) const
   {
      return _tx->is_subtree(key);
   }
   inline sal::smart_ptr<sal::alloc_header> transaction_frame_ref::get_subtree(key_view key) const
   {
      return _tx->get_subtree(key);
   }
   inline write_cursor transaction_frame_ref::get_subtree_cursor(key_view key) const
   {
      return _tx->get_subtree_cursor(key);
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

}  // namespace psitri
