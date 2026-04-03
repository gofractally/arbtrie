#pragma once
#include <psitri/cursor.hpp>
#include <psitri/tree_ops.hpp>

namespace psitri
{
   class write_session;

   class write_cursor
   {
     public:
      using ptr = std::shared_ptr<write_cursor>;

      /// Create a write cursor on an empty (transient) tree
      write_cursor(sal::allocator_session_ptr session)
          : _ctx(sal::smart_ptr<sal::alloc_header>(session, sal::null_ptr_address))
      {
      }

      /// Create a write cursor on an existing root
      write_cursor(sal::smart_ptr<sal::alloc_header> root) : _ctx(std::move(root)) {}

      // -- Mutations --

      /// Insert key/value pair. Caller must ensure key does not exist.
      /// Precondition violation (duplicate key) asserts — it is not recoverable.
      void insert(key_view key, value_view value)
      {
         _ctx.upsert<upsert_mode::unique_insert>(key, value_type(value));
      }

      /// Update existing key. Caller must ensure key exists.
      /// Precondition violation (missing key) asserts — it is not recoverable.
      void update(key_view key, value_view value)
      {
         _ctx.upsert<upsert_mode::unique_update>(key, value_type(value));
      }

      /// Insert or update. Always succeeds.
      void upsert(key_view key, value_view value)
      {
         _ctx.upsert<upsert_mode::unique_upsert>(key, value_type(value));
      }

      /// Store a subtree as the value for a key.
      /// Takes ownership of the smart_ptr's reference count (the smart_ptr is consumed).
      void upsert(key_view key, sal::smart_ptr<sal::alloc_header> subtree_root)
      {
         auto addr = subtree_root.take();  // extract address without releasing ref
         _ctx.upsert<upsert_mode::unique_upsert>(key, value_type::make_subtree(addr));
      }

      /// Remove key. Returns size of removed value, or -1 if not found.
      int remove(key_view key) { return _ctx.remove(key); }

      /// Remove all keys in range [lower, upper). Returns number of keys removed.
      uint64_t remove_range(key_view lower, key_view upper)
      {
         return _ctx.remove_range(lower, upper);
      }

      // -- Read access --

      /// Get a read cursor for the current tree state
      cursor read_cursor() const { return cursor(_ctx.get_root()); }

      /// Count keys in range [lower, upper). Empty bounds mean unbounded.
      uint64_t count_keys(key_view lower = {}, key_view upper = {}) const
      {
         return cursor(_ctx.get_root()).count_keys(lower, upper);
      }

      /// Point lookup. Returns nullopt if key not found.
      template <ConstructibleBuffer T>
      std::optional<T> get(key_view key) const
      {
         cursor c(_ctx.get_root());
         return c.get<T>(key);
      }

      /// Point lookup into a buffer. Returns bytes read, or cursor::value_not_found.
      int32_t get(key_view key, Buffer auto* buffer) const
      {
         cursor c(_ctx.get_root());
         return c.get(key, buffer);
      }

      /// Check if the value at key is a subtree
      bool is_subtree(key_view key) const
      {
         cursor c(_ctx.get_root());
         return c.seek(key) && c.is_subtree();
      }

      /// Get a subtree root as a smart_ptr.
      /// Returns null smart_ptr if key not found or value is not a subtree.
      sal::smart_ptr<sal::alloc_header> get_subtree(key_view key) const
      {
         cursor c(_ctx.get_root());
         if (c.seek(key) && c.is_subtree())
            return c.subtree();
         return sal::smart_ptr<sal::alloc_header>(c.get_root().session(), sal::null_ptr_address);
      }

      /// Get a write_cursor rooted at the subtree stored at the given key.
      /// The returned cursor operates on a COW copy — changes are not reflected
      /// back to the parent tree until the caller stores the modified root.
      /// Returns an empty write_cursor if key not found or value is not a subtree.
      write_cursor get_subtree_cursor(key_view key) const
      {
         auto sub = get_subtree(key);
         if (sub)
            return write_cursor(std::move(sub));
         return write_cursor(_ctx.get_root().session());
      }

      // -- Root access --

      /// Get the current root (for saving or sharing)
      sal::smart_ptr<sal::alloc_header> root() const { return _ctx.get_root(); }

      /// Move root out — caller takes ownership, cursor is left empty
      sal::smart_ptr<sal::alloc_header> take_root() { return _ctx.take_root(); }

      /// Check if the tree is empty
      explicit operator bool() const { return static_cast<bool>(_ctx.get_root()); }

      // -- Diagnostics --

      tree_context::stats get_stats() { return _ctx.get_stats(); }
      void                print() { _ctx.print(); }
      void                validate() { _ctx.validate(); }

     private:
      friend class transaction;
      tree_context _ctx;
   };

   using write_cursor_ptr = std::shared_ptr<write_cursor>;

}  // namespace psitri
