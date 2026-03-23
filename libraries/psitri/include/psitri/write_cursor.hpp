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

      /// Insert key/value pair. Returns true if inserted, false if key already exists.
      bool insert(key_view key, value_view value)
      {
         // Use get to check existence first, since tree_context::upsert
         // with unique_insert corrupts root state on throw
         std::string tmp;
         if (get(key, &tmp) >= 0)
            return false;  // key exists
         _ctx.upsert<upsert_mode::unique_insert>(key, value_type(value));
         return true;
      }

      /// Update existing key. Returns true if updated, false if key not found.
      bool update(key_view key, value_view value)
      {
         if (!_ctx.get_root())
            return false;  // empty tree, nothing to update
         try
         {
            _ctx.upsert<upsert_mode::unique_update>(key, value_type(value));
            return true;
         }
         catch (const std::runtime_error&)
         {
            return false;
         }
      }

      /// Insert or update. Always succeeds.
      void upsert(key_view key, value_view value)
      {
         _ctx.upsert<upsert_mode::unique_upsert>(key, value_type(value));
      }

      /// Remove key. Returns size of removed value, or -1 if not found.
      int remove(key_view key) { return _ctx.remove(key); }

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
