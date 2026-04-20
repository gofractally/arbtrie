#pragma once
#include <cassert>
#include <functional>
#include <optional>
#include <psitri/detail/write_buffer.hpp>
#include <psitri/tx_mode.hpp>
#include <psitri/write_cursor.hpp>
#include <vector>

namespace psitri
{
   class transaction;

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

      bool                              is_subtree(key_view key) const;
      sal::smart_ptr<sal::alloc_header> get_subtree(key_view key) const;
      write_cursor                      get_subtree_cursor(key_view key) const;

      // Apply fn(write_cursor&) to the tracked subtree at key. Creates the
      // entry on first call; subsequent calls continue from the prior state.
      // Flushed to the main tree atomically on commit, discarded on abort.
      template <typename Fn>
      void with_subtree(key_view key, Fn&& fn);

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
                  tx_mode                                                mode = tx_mode::batch)
          : _commit_func(std::move(commit_func)),
            _rollback_func(std::move(rollback_func)),
            _mode(mode)
      {
         if (root)
            _cursor.emplace(std::move(root));
         else
            _cursor.emplace(std::move(session));

         if (_mode == tx_mode::micro)
            _buffer.emplace();
      }

      transaction(const transaction&)            = delete;
      transaction& operator=(const transaction&) = delete;
      transaction(transaction&&) noexcept        = default;
      transaction& operator=(transaction&&)      = delete;

      ~transaction() { abort(); }

      // ── Mutations ─────────────────────────────────────────────────────

      void insert(key_view key, value_view value)
      {
         assert_no_child_frame();
         if (_mode == tx_mode::micro)
            micro_put(key, value);
         else
            _cursor->insert(key, value);
      }

      void update(key_view key, value_view value)
      {
         assert_no_child_frame();
         if (_mode == tx_mode::micro)
            micro_put(key, value);
         else
            _cursor->update(key, value);
      }

      void upsert(key_view key, value_view value)
      {
         assert_no_child_frame();
         if (_mode == tx_mode::micro)
            micro_put(key, value);
         else
            _cursor->upsert(key, value);
      }

      void upsert_sorted(key_view key, value_view value)
      {
         assert_no_child_frame();
         if (_mode == tx_mode::micro)
            micro_put(key, value);
         else
            _cursor->upsert_sorted(key, value);
      }

      void upsert(key_view key, sal::smart_ptr<sal::alloc_header> subtree_root)
      {
         assert_no_child_frame();
         assert(_mode == tx_mode::batch && "subtree upsert not supported in micro mode");
         _cursor->upsert(key, std::move(subtree_root));
      }

      void upsert_sorted(key_view key, sal::smart_ptr<sal::alloc_header> subtree_root)
      {
         assert_no_child_frame();
         assert(_mode == tx_mode::batch && "subtree upsert not supported in micro mode");
         _cursor->upsert_sorted(key, std::move(subtree_root));
      }

      int remove(key_view key)
      {
         assert_no_child_frame();
         if (_mode == tx_mode::micro)
            return micro_remove(key);
         return _cursor->remove(key);
      }

      uint64_t remove_range(key_view lower, key_view upper)
      {
         assert_no_child_frame();
         if (_mode == tx_mode::micro)
            return micro_remove_range(lower, upper);
         return _cursor->remove_range(lower, upper);
      }

      // ── Read access ───────────────────────────────────────────────────

      cursor read_cursor() const { return _cursor->read_cursor(); }

      template <ConstructibleBuffer T>
      std::optional<T> get(key_view key) const
      {
         if (_mode == tx_mode::micro && _buffer)
         {
            const auto* entry = _buffer->get(key);
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
         return _cursor->get<T>(key);
      }

      int32_t get(key_view key, Buffer auto* buffer) const
      {
         if (_mode == tx_mode::micro && _buffer)
         {
            const auto* entry = _buffer->get(key);
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
         return _cursor->get(key, buffer);
      }

      bool is_subtree(key_view key) const
      {
         if (_mode == tx_mode::micro && _buffer)
         {
            const auto* entry = _buffer->get(key);
            if (entry)
               return false;  // buffered entries are never subtrees
         }
         return _cursor->is_subtree(key);
      }

      sal::smart_ptr<sal::alloc_header> get_subtree(key_view key) const
      {
         if (_mode == tx_mode::micro && _buffer)
         {
            const auto* entry = _buffer->get(key);
            if (entry)
               return sal::smart_ptr<sal::alloc_header>(
                   _cursor->root().session(), sal::null_ptr_address);
         }
         return _cursor->get_subtree(key);
      }

      write_cursor get_subtree_cursor(key_view key) const
      {
         return _cursor->get_subtree_cursor(key);
      }

      template <typename Fn>
      void with_subtree(key_view key, Fn&& fn)
      {
         with_subtree_impl(key, std::forward<Fn>(fn));
      }

      // ── Transaction control ───────────────────────────────────────────

      void commit() noexcept
      {
         if (_commit_func)
         {
            flush_open_subtrees();

            if (_mode == tx_mode::micro && _buffer && !_buffer->empty())
               merge_buffer_to_persistent();

            _commit_func(std::move(_cursor->root()));
            _commit_func   = nullptr;
            _rollback_func = nullptr;
         }
      }

      void abort() noexcept
      {
         while (!_frames.empty())
            abort_frame();

         _open_subtrees.clear();

         if (_rollback_func)
         {
            _rollback_func();
            _rollback_func = nullptr;
            _commit_func   = nullptr;
         }
      }

      /// Create a sub-transaction. Returns a transaction_frame_ref that
      /// delegates operations to this transaction. The parent must not
      /// be modified directly while the sub-transaction is active.
      [[nodiscard]] transaction_frame_ref sub_transaction() noexcept
      {
         push_frame();
         return transaction_frame_ref(*this);
      }

      // ── Diagnostics ───────────────────────────────────────────────────

      tree_context::stats get_stats() { return _cursor->get_stats(); }

     private:
      friend class transaction_frame_ref;

      // ── Frame stack for sub-transactions ──────────────────────────────

      struct frame
      {
         sal::smart_ptr<sal::alloc_header> saved_root;
         detail::write_buffer::saved_state buf_state;
         bool                              micro_merged = false;
         // Number of open subtrees at push_frame time; entries beyond are new.
         size_t subtrees_at_push = 0;
         // Roots of the first subtrees_at_push entries at push_frame time.
         std::vector<sal::smart_ptr<sal::alloc_header>> subtree_roots;
      };

      void push_frame() noexcept
      {
         frame f;
         if (_mode == tx_mode::micro)
         {
            f.buf_state  = _buffer->save();
            f.saved_root = _cursor->root();
            _buffer->bump_generation();
         }
         else
         {
            f.saved_root = _cursor->root();
         }
         f.subtrees_at_push = _open_subtrees.size();
         f.subtree_roots.reserve(_open_subtrees.size());
         for (auto& e : _open_subtrees)
            f.subtree_roots.push_back(e.root);
         _frames.push_back(std::move(f));
      }

      void commit_frame() noexcept
      {
         assert(!_frames.empty());
         _frames.pop_back();  // Data stays (buffer or cursor unchanged)
      }

      void abort_frame() noexcept
      {
         assert(!_frames.empty());
         auto f = std::move(_frames.back());
         _frames.pop_back();

         // Drop subtree entries opened within this frame.
         _open_subtrees.resize(f.subtrees_at_push);
         // Restore pre-frame roots by index.
         for (size_t i = 0; i < f.subtrees_at_push; ++i)
            _open_subtrees[i].root = std::move(f.subtree_roots[i]);

         if (_mode == tx_mode::micro)
         {
            _buffer->restore(f.buf_state);
            if (f.micro_merged)
               _cursor.emplace(std::move(f.saved_root));
         }
         else
         {
            _cursor.emplace(std::move(f.saved_root));
         }
      }

      void assert_no_child_frame() const
      {
         // If frames exist, the topmost frame_ref should be used, not the parent.
         // This is a debug assertion — not enforced at the type level.
      }

      // ── Tracked subtree cursors ───────────────────────────────────────

      struct subtree_entry
      {
         std::string                       key;
         sal::smart_ptr<sal::alloc_header> root;
      };

      // Linear scan — fast for the small N (< 20) typical of transactions.
      size_t find_subtree(key_view key) const noexcept
      {
         for (size_t i = 0; i < _open_subtrees.size(); ++i)
            if (_open_subtrees[i].key == key)
               return i;
         return _open_subtrees.size();
      }

      template <typename Fn>
      void with_subtree_impl(key_view key, Fn&& fn)
      {
         size_t idx = find_subtree(key);
         if (idx == _open_subtrees.size())
         {
            if (_mode == tx_mode::micro && _buffer && !_buffer->empty())
            {
               merge_buffer_to_persistent();
               mark_frames_merged();
            }
            _open_subtrees.push_back({std::string(key), _cursor->get_subtree(key)});
            idx = _open_subtrees.size() - 1;
         }

         auto& entry = _open_subtrees[idx];
         {
            write_cursor cur(entry.root ? entry.root
                                        : sal::smart_ptr<sal::alloc_header>(
                                              _cursor->root().session(), sal::null_ptr_address));
            std::forward<Fn>(fn)(cur);
            entry.root = cur.root();
         }
      }

      void flush_open_subtrees()
      {
         for (auto& e : _open_subtrees)
            if (e.root)
               _cursor->upsert(key_view(e.key), std::move(e.root));
         _open_subtrees.clear();
      }

      // ── Micro mode helpers ────────────────────────────────────────────

      void micro_put(key_view key, value_view value)
      {
         assert(_buffer);
         const auto* existing = _buffer->get(key);
         if (existing)
         {
            // Buffer already has this key — existed_in_persistent doesn't matter
            _buffer->put(key, value, false);
         }
         else
         {
            bool existed = _cursor->get<std::string>(key).has_value();
            _buffer->put(key, value, existed);
         }
      }

      int micro_remove(key_view key)
      {
         assert(_buffer);
         const auto* existing = _buffer->get(key);
         if (existing)
         {
            if (existing->is_tombstone())
               return -1;  // already deleted

            // Get value size before erasing (for return value compatibility)
            int result = existing->is_data() ? static_cast<int>(existing->data_len) : 0;
            _buffer->erase(key, false);  // existed_in_persistent ignored when in buffer
            return result;
         }
         else
         {
            // Check persistent tree
            auto val = _cursor->get<std::string>(key);
            if (val)
            {
               int result = static_cast<int>(val->size());
               _buffer->erase(key, true);
               return result;
            }
            else
            {
               _buffer->erase(key, false);
               return -1;
            }
         }
      }

      /// Threshold: if persistent key count in range is at or below this,
      /// use individual tombstones. Above: merge buffer and delegate.
      static constexpr uint64_t tombstone_threshold = 256;

      uint64_t micro_remove_range(key_view lower, key_view upper)
      {
         assert(_buffer);

         // Count persistent keys in range
         cursor   rc(_cursor->root());
         uint64_t persistent_count = rc.count_keys(lower, upper);

         if (persistent_count <= tombstone_threshold)
         {
            // Small range: tombstone individual keys
            uint64_t removed = 0;

            // Tombstone persistent keys in range
            cursor pc(_cursor->root());
            pc.lower_bound(lower);
            while (!pc.is_end())
            {
               auto k = pc.key();
               if (!upper.empty() && k >= upper)
                  break;

               const auto* entry = _buffer->get(k);
               if (!entry || !entry->is_tombstone())
               {
                  _buffer->erase(k, true);  // exists in persistent tree
                  ++removed;
               }
               pc.next();
            }

            // Also remove buffer-only inserts in range
            std::vector<std::string> buf_insert_keys;
            auto                     it  = _buffer->lower_bound(lower);
            auto                     end = _buffer->end();
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
               _buffer->erase(k, false);
               ++removed;
            }

            return removed;
         }
         else
         {
            // Large range: merge buffer to persistent, then range remove on cursor
            if (!_buffer->empty())
            {
               merge_buffer_to_persistent();
               mark_frames_merged();
            }
            return _cursor->remove_range(lower, upper);
         }
      }

      /// Mark all existing frames as merged so abort restores the cursor root
      /// rather than (now-invalid) buffer save states.
      void mark_frames_merged() noexcept
      {
         for (auto& f : _frames)
            f.micro_merged = true;
      }

      void merge_buffer_to_persistent()
      {
         auto it  = _buffer->begin();
         auto end = _buffer->end();
         for (; it != end; ++it)
         {
            auto& entry = it.value();
            auto  key   = it.key();

            if (entry.is_data())
            {
               auto val = entry.value();
               _cursor->upsert_sorted(key, val);
            }
            else if (entry.type == detail::buffer_entry::tombstone)
            {
               _cursor->remove(key);
            }
            // tombstone_noop: key doesn't exist in persistent tree — skip
         }

         if (_frames.empty())
            _buffer->clear();      // No sub-tx frames — full clear is safe
         else
            _buffer->soft_clear();  // Preserve arena for sub-tx save/restore
      }

      std::optional<write_cursor>                              _cursor;
      std::function<void(sal::smart_ptr<sal::alloc_header>)>   _commit_func;
      std::function<void()>                                    _rollback_func;
      tx_mode                                                  _mode = tx_mode::batch;
      std::optional<detail::write_buffer>                      _buffer;
      std::vector<frame>                                       _frames;
      std::vector<subtree_entry>                               _open_subtrees;
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

   template <typename Fn>
   void transaction_frame_ref::with_subtree(key_view key, Fn&& fn)
   {
      _tx->with_subtree_impl(key, std::forward<Fn>(fn));
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
