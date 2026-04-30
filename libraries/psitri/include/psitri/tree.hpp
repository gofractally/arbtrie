#pragma once
#include <concepts>
#include <optional>
#include <psitri/cursor.hpp>
#include <utility>

namespace psitri
{
   /**
    * Copyable retained tree identity.
    *
    * A tree may be detached, a top-root snapshot, or a subtree snapshot. It is
    * intentionally a handle to an existing tree identity, not a mutable editing
    * object. Start a write_transaction to edit it.
    */
   class tree
   {
     public:
      tree() noexcept = default;
      explicit tree(sal::allocator_session_ptr session) noexcept
          : _root(std::move(session), sal::null_ptr_address)
      {
      }
      tree(sal::smart_ptr<sal::alloc_header> root) noexcept : _root(std::move(root)) {}

      tree(const tree&) noexcept            = default;
      tree(tree&&) noexcept                 = default;
      tree& operator=(const tree&) noexcept = default;
      tree& operator=(tree&&) noexcept      = default;

      explicit operator bool() const noexcept { return static_cast<bool>(_root); }

      operator sal::smart_ptr<sal::alloc_header>&() & noexcept { return _root; }
      operator const sal::smart_ptr<sal::alloc_header>&() const& noexcept { return _root; }
      operator sal::smart_ptr<sal::alloc_header>() && noexcept { return std::move(_root); }

      const sal::smart_ptr<sal::alloc_header>& raw_root() const noexcept { return _root; }
      sal::smart_ptr<sal::alloc_header>        copy_root() const noexcept { return _root; }
      sal::smart_ptr<sal::alloc_header>        take_root() noexcept { return std::move(_root); }

      const sal::allocator_session_ptr& session() const noexcept { return _root.session(); }
      value_pin                         pin_values() const { return value_pin(_root.session()); }
      sal::ptr_address                  address() const noexcept { return _root.address(); }
      sal::ptr_address                  ver() const noexcept { return _root.ver(); }
      void                              set_ver(sal::ptr_address v) noexcept { _root.set_ver(v); }
      sal::tree_id                      get_tree_id() const noexcept { return _root.get_tree_id(); }
      bool                              is_read_only() const noexcept { return _root.is_read_only(); }
      void                              release() noexcept { _root.release(); }
      sal::ptr_address                  take() noexcept { return _root.take(); }
      sal::tree_id                      take_tree_id() noexcept { return _root.take_tree_id(); }
      tree&                             give(sal::ptr_address adr) noexcept
      {
         _root.give(adr);
         return *this;
      }

      bool get(key_view key, std::invocable<value_view> auto&& lambda) const
      {
         psitri::cursor c(_root);
         if (!c.find(key) || c.is_subtree())
            return false;
         c.get_value(std::forward<decltype(lambda)>(lambda));
         return true;
      }

      template <ConstructibleBuffer T>
      std::optional<T> get(key_view key) const
      {
         psitri::cursor c(_root);
         return c.get<T>(key);
      }

      int32_t get(key_view key, Buffer auto* buffer) const
      {
         psitri::cursor c(_root);
         return c.get(key, buffer);
      }

      psitri::cursor cursor() const { return psitri::cursor(_root); }
      psitri::cursor snapshot_cursor() const { return psitri::cursor(_root); }

      tree get_subtree(key_view key) const
      {
         psitri::cursor c(_root);
         if (c.find(key) && c.is_subtree())
            return tree(c.subtree());
         return tree(_root.session());
      }

     private:
      sal::smart_ptr<sal::alloc_header> _root;
   };

}  // namespace psitri
