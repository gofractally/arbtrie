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
   class write_session;

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

      // ── Transaction control ───────────────────────────────────────────

      void commit() noexcept
      {
         if (_commit_func)
         {
            if (_mode == tx_mode::micro && _buffer && !_buffer->empty())
               merge_buffer_to_persistent();

            _commit_func(std::move(_cursor->root()));
            _commit_func   = nullptr;
            _rollback_func = nullptr;
         }
      }

      void abort() noexcept
      {
         // Unwind any outstanding frames
         while (!_frames.empty())
            abort_frame();

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
         // Saved cursor root — used for batch mode abort and micro mode
         // abort after a merge-then-delegate has flushed the buffer.
         sal::smart_ptr<sal::alloc_header> saved_root;
         // Saved buffer state — used for micro mode abort when no merge occurred.
         detail::write_buffer::saved_state buf_state;
         // Set true when a merge-then-delegate occurs within this frame's lifetime.
         // After merge, buf_state is invalid — abort must restore saved_root instead.
         bool micro_merged = false;
      };

      void push_frame() noexcept
      {
         frame f;
         if (_mode == tx_mode::micro)
         {
            f.buf_state  = _buffer->save();
            f.saved_root = _cursor->root();  // saved in case merge happens later
            _buffer->bump_generation();
         }
         else
         {
            f.saved_root = _cursor->root();
         }
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

         if (_mode == tx_mode::micro)
         {
            // Restore buffer state — works even after soft_clear because
            // the arena data is preserved (soft_clear doesn't free memory).
            _buffer->restore(f.buf_state);

            if (f.micro_merged)
            {
               // Merge modified the cursor — restore the pre-frame root too.
               _cursor.emplace(std::move(f.saved_root));
            }
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
