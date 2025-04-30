#pragma once
#include <psitri/node/inner.hpp>
#include <psitri/node/leaf.hpp>
#include <psitri/node/node.hpp>
#include <psitri/node/value_node.hpp>
#include <psitri/value_type.hpp>
#include <sal/smart_ptr.hpp>

namespace psitri
{

   // clang-format off
   /**
    * Concept for buffer types that can be used with iterator value functions.
    * Requires the type to be resizable and provide contiguous memory access.
    */
   template <typename T>
   concept Buffer = requires(T b) {
      { b.resize(std::size_t{}) } -> std::same_as<void>;
      { b.data() } -> std::convertible_to<void*>;
   };

   /**
    * Extended Buffer concept that also requires default constructibility.
    * Used for value() function that constructs a new buffer.
    */
   template <typename T>
   concept ConstructibleBuffer = Buffer<T> && requires { T(); };


   /**
    * Concept for types that can be implicitly converted to a std::string_view
    */
   template <typename T>
   concept ImplicitValueViewConvertible = requires(const T& t) {
      { std::string_view{t} } -> std::same_as<std::string_view>;
   };

   /**
    * Concept for types that provide data() and size() methods compatible with std::string_view construction
    */
   template <typename T>
   concept DataSizeValueViewConvertible = requires(const T& t) {
      { t.data() } -> std::convertible_to<const char*>;
      { t.size() } -> std::convertible_to<std::size_t>;
   };

   /**
    * Concept for types that can be converted to a std::string_view through either method
    */
   template <typename T>
   concept ValueViewConvertible = ImplicitValueViewConvertible<T> || DataSizeValueViewConvertible<T>;

   /**
    * Concept for types that can be used as values in the trie (either convertible to value_view or a smart_ptr<>)
    */
   template <typename T>
   concept ValueViewConvertibleOrNode = ValueViewConvertible<T> || std::same_as<T, sal::smart_ptr<sal::alloc_header>>;
   // clang-format on

   class cursor
   {
     public:
      cursor(sal::smart_ptr<sal::alloc_header> n);
      static constexpr int32_t value_not_found = -1;
      static constexpr int32_t value_subtree   = -2;

      bool seek_rend() noexcept;   /// position before first key
      bool seek_begin() noexcept;  /// position at first key
      bool seek_last() noexcept;   /// position at last key
      bool seek_end() noexcept;    /// position after last key

      /// checks if the position is before first key
      bool is_rend() const noexcept { return _path[0].branch == branch_number(-1); }
      /// checks if the position is after last key
      bool is_end() const noexcept { return depth() == 0 and _path[0].branch == _root_end_branch; }

      /// seek to the first key that is greater than or equal to the given key
      /// @return is_end()
      bool lower_bound(key_view key) noexcept;
      bool upper_bound(key_view key) noexcept;

      /// goes to exactly the given key, @return is_end() if not found
      bool seek(key_view key) noexcept;

      bool first(key_view prefix = {}) noexcept;
      bool last(key_view prefix = {}) noexcept;

      /// seek to the next key
      /// @return is_end()
      bool next() noexcept;

      /// seek to the previous key
      /// @return is_rend()
      bool prev() noexcept;

      key_view key() const noexcept { return key_view(_key_buf.data(), _key_len); }

      /// @return the value associated with the current key as a view that is valid for the
      /// the lifetime of the read_lock passed in.
      value_view value(sal::read_lock& rl) const noexcept;

      /// calls lambda with a view of the value for the current key, view is only valid for the lifetime of the lambda
      void get_value(std::invocable<value_view> auto&& lambda) const noexcept;

      /// constructs a new buffer of type ConstructibleBufferType and fills it with the value of the
      /// current key, returns nullopt if not a valid key or the value is a subtree
      template <ConstructibleBuffer ConstructibleBufferType>
      std::optional<ConstructibleBufferType> value() const noexcept;

      /// @return the number of bytes in the value of the current key,
      /// or cursor::value_not_found if not a valid key, or cursor::value_subtree if subtree
      int32_t value_size() const noexcept;

      template <ConstructibleBuffer ConstructibleBufferType>
      std::optional<ConstructibleBufferType> get(key_view key) const;

      /**
       * Get the value at the specified key into a buffer
       * @tparam Buffer Type that supports resize() and data() for contiguous memory access
       * @param key The key to get the value for
       * @param buffer Pointer to the buffer to read the value into
       * @return 
       *   >= 0: Number of bytes read into buffer on success
       *   iterator::value_nothing: Value not found
       *   iterator::value_subtree: Found subtree value (use subtree() or subtree_cursor() instead)
       */
      int32_t get(key_view key, Buffer auto* buffer) const;

      bool is_subtree() const noexcept;

      /// if the current value is a subtree, return it as a smart pointer
      /// @return a null smart_ptr if the current value is not a subtree
      sal::smart_ptr<sal::alloc_header> subtree() const noexcept;

      /// if the current value is a subtree, return it as a cursor
      /// @return a null cursor if the current value is not a subtree
      cursor subtree_cursor() const noexcept;

      /// get the root of the tree
      const sal::smart_ptr<sal::alloc_header>& get_root() const noexcept { return _node; }
      void                                     set_root(sal::smart_ptr<> r) noexcept
      {
         _node = std::move(r);
         seek_rend();
         _path_back->adr = _node.address();
      }

     private:
      int32_t get_impl(key_view key, Buffer auto* buffer) noexcept;
      bool    next_impl() noexcept;
      bool    prev_impl() noexcept;
      bool    lower_bound_impl(key_view key) noexcept;

      auto visit(ptr_address adr, auto&& lambda);

      struct path_entry
      {
         ptr_address   adr;
         branch_number branch;  // 0 to num_branches
         uint16_t      prefix_len;
      };
      int depth() const noexcept { return _path_back - _path.data(); }
      static_assert(sizeof(path_entry) == 8, "path_entry must be 8 bytes");

      sal::smart_ptr<sal::alloc_header> _node;
      std::array<char, 1024>            _key_buf;
      std::array<path_entry, 128>       _path;
      path_entry*                       _path_back;
      uint32_t                          _key_len;
      branch_number                     _root_end_branch;

      void append_key(key_view key) noexcept;
      bool pop() noexcept;
      bool reverse_pop() noexcept;
      void push(ptr_address adr) noexcept;
      void push_end(ptr_address adr) noexcept;
      void next_branch(key_view key) noexcept;
   };
   inline auto cursor::visit(ptr_address adr, auto&& lambda)
   {
      auto ref = _node.session()->get_ref(adr);
      switch (node_type(ref->type()))
      {
         case node_type::leaf:
            return lambda(*ref.as<leaf_node>());
         case node_type::inner:
            return lambda(*ref.as<inner_node>());
         case node_type::inner_prefix:
            return lambda(*ref.as<inner_prefix_node>());
         case node_type::value:
            return lambda(*ref.as<value_node>());
         default:
            std::unreachable();
      }
   }
   inline cursor::cursor(sal::smart_ptr<sal::alloc_header> n) : _node(std::move(n))
   {
      _path_back         = _path.data();
      _path_back->branch = branch_number(-1);
      _key_len           = 0;
      _path_back->adr    = _node.address();
      if (_node.address() == sal::null_ptr_address)
      {
         _path_back->prefix_len = 0;
         _root_end_branch       = branch_number(0);
         return;
      }
      auto read_lock = _node.session()->lock();
      _root_end_branch =
          visit(_node.address(), [](auto&& node) { return branch_number(node.num_branches()); });
   }

   inline bool cursor::lower_bound(key_view key) noexcept
   {
      auto read_lock = _node.session()->lock();
      return lower_bound_impl(key);
   }
   inline bool cursor::lower_bound_impl(key_view key) noexcept
   {
      if (sal::null_ptr_address == _node.address()) [[unlikely]]
         return false;
      seek_rend();
      while (true)
      {
         const node* n = _node.session()->get_ref<node>(_path_back->adr).obj();
         switch (n->type())
         {
            [[unlikely]] case node_type::leaf:
            {
               auto*         l      = static_cast<const leaf_node*>(n);
               branch_number branch = l->lower_bound(key);
               //     SAL_INFO("{} leaf branch: {} key: {}", _path_back->adr, branch, key);
               if (branch == l->num_branches())
               {
                  //   SAL_INFO("{} leaf branch == num_branches", _path_back->adr);
                  return pop(), next_impl();
               }
               //   SAL_INFO("{}  l->get_key({}): {} = {}", _path_back->adr, branch, l->get_key(branch),
               //           l->get_value(branch));
               _path_back->branch = branch;
               append_key(l->get_key(branch));
               return true;
            }
            [[likely]] case node_type::inner:
            {
               auto*         i      = static_cast<const inner_node*>(n);
               branch_number branch = i->lower_bound(key);
               //SAL_INFO("{} inner branch: {} divs: {}", _path_back->adr, branch, i->divs());
               _path_back->branch = branch;
               push(i->get_branch(branch));
               continue;
            }
            [[likely]] case node_type::inner_prefix:
            {
               auto* ip   = static_cast<const inner_prefix_node*>(n);
               auto  cpre = ucc::common_prefix(key, ip->prefix());
               //     SAL_INFO("{} cpre: '{}' curkey: '{}' argkey: '{}' divs: {}", _path_back->adr, cpre,
               //            this->key(), key, ip->divs());
               if (cpre.size() == ip->prefix().size())
               {
                  append_key(ip->prefix());
                  _path_back->branch = ip->lower_bound(key = key.substr(cpre.size()));
                  //     SAL_INFO("{} ip->lower_bound({}): {}", _path_back->adr, key, _path_back->branch);
                  push(ip->get_branch(_path_back->branch));
                  continue;
               }
               if (ip->prefix() > key)
               {
                  //    SAL_INFO("{} prefix {} > key {}", _path_back->adr, ip->prefix(), key);
                  _path_back->branch = branch_number(0);
                  append_key(ip->prefix());
                  push(ip->get_branch(_path_back->branch));
                  return next_impl();
               }
               // SAL_INFO("{} prefix{} < key{}", _path_back->adr, ip->prefix(), key);
               pop();
               return next_impl();
            }
            [[unlikely]] case node_type::value:
               [[fallthrough]];
            default:
               std::unreachable();
         }
      }
   }
   int32_t cursor::get(key_view key, Buffer auto* buffer) const
   {
      if (sal::null_ptr_address == _node.address()) [[unlikely]]
         return false;
      auto read_lock = _node.session()->lock();
      return get_impl(key, buffer);
   }
   int32_t cursor::get_impl(key_view key, Buffer auto* buffer) noexcept
   {
      seek_rend();
      while (true)
      {
         const node* n = _node.session()->get_ref<node>(_path_back->adr).obj();
         switch (n->type())
         {
            [[likely]] case node_type::inner:
            {
               const auto*   i      = static_cast<const inner_node*>(n);
               branch_number branch = i->lower_bound(key);
               _path_back->branch   = branch;
               push(i->get_branch(branch));
               continue;
            }
            [[likely]] case node_type::inner_prefix:
            {
               const auto* ip   = static_cast<const inner_prefix_node*>(n);
               auto        cpre = ucc::common_prefix(key, ip->prefix());
               if (cpre.size() != ip->prefix().size())
                  return seek_end(), cursor::value_not_found;

               append_key(ip->prefix());
               _path_back->branch = ip->lower_bound(key = key.substr(cpre.size()));
               push(ip->get_branch(_path_back->branch));
               continue;
            }
            [[unlikely]] case node_type::leaf:
            {
               const auto* l      = static_cast<const leaf_node*>(n);
               _path_back->branch = l->get(key);
               if (_path_back->branch == l->num_branches())
                  return seek_end(), cursor::value_not_found;
               append_key(key);
               switch (l->get_value_type(_path_back->branch))
               {
                  case leaf_node::value_type_flag::null:
                     buffer->resize(0);
                     return 0;
                  case leaf_node::value_type_flag::inline_data:
                  {
                     value_view vv = l->get_value_view(_path_back->branch);
                     buffer->resize(vv.size());
                     std::memcpy(buffer->data(), vv.data(), buffer->size());
                     return buffer->size();
                  }
                  case leaf_node::value_type_flag::value_node:
                  {
                     auto ref = _node.session()->get_ref<value_node>(
                         l->get_value_address(_path_back->branch));
                     auto vv = ref->get_data();
                     buffer->resize(vv.size());
                     std::memcpy(buffer->data(), vv.data(), vv.size());
                     return buffer->size();
                  }
                  case leaf_node::value_type_flag::subtree:
                     return value_subtree;
                  default:
                     std::unreachable();
               }
            }
            [[unlikely]] case node_type::value:
               [[fallthrough]];
            default:
               std::unreachable();
         }
      }
   }

   inline void cursor::append_key(key_view key) noexcept
   {
      // TODO: it is always possible to read 7 bytes past the end of the key stored in nodes; therefore,
      // this copy could be done 8 bytes at a time instead of 1 byte at a time, so long as we ensure that
      // it is safe to write 7 bytes past the end of _key_buf
      std::copy(key.begin(), key.end(), _key_buf.begin() + _key_len);
      _key_len += key.size();
      _path_back->prefix_len = key.size();
      assert(_key_len <= 1024);
   }
   inline bool cursor::pop() noexcept
   {
      _key_len -= _path_back->prefix_len;
      bool not_end = _path_back > _path.data();
      _path_back -= not_end;
      _path_back->branch = not_end ? _path_back->branch : _root_end_branch;
      return not_end;
   }
   inline bool cursor::reverse_pop() noexcept
   {
      _key_len -= _path_back->prefix_len;
      bool not_end = _path_back > _path.data();
      _path_back -= not_end;
      _path_back->branch |= branch_number(-!not_end);
      return not_end;
   }
   inline bool cursor::seek_end() noexcept
   {
      _path_back             = _path.data();
      _path_back->branch     = _root_end_branch;
      _key_len               = 0;
      _path_back->prefix_len = 0;
      return false;
   }
   inline bool cursor::seek_rend() noexcept
   {
      _path_back             = _path.data();
      _path_back->branch     = branch_number(-1);
      _key_len               = 0;
      _path_back->prefix_len = 0;
      return true;
   }
   inline bool cursor::seek_begin() noexcept
   {
      seek_rend();
      return next();
   }
   inline bool cursor::seek_last() noexcept
   {
      seek_end();
      return prev();
   }
   inline void cursor::push(ptr_address adr) noexcept
   {
      _path_back++;
      _path_back->adr        = adr;
      _path_back->branch     = branch_number(-1);
      _path_back->prefix_len = 0;
   }
   inline void cursor::push_end(ptr_address adr) noexcept
   {
      _path_back++;
      _path_back->adr        = adr;
      _path_back->branch     = branch_number(-2);
      _path_back->prefix_len = 0;
   }
   inline void cursor::next_branch(key_view key) noexcept
   {
      _key_len -= _path_back->prefix_len;
      std::copy(key.begin(), key.end(), _key_buf.begin() + _key_len);
      _key_len += (_path_back->prefix_len = key.size());
      assert(_key_len <= 32);
   }
   inline bool cursor::next() noexcept
   {
      assert(not is_end());
      auto read_lock = _node.session()->lock();
      return next_impl();
   }
   inline bool cursor::next_impl() noexcept
   {
      while (true)
      {
         const node* n = _node.session()->get_ref<node>(_path_back->adr).obj();
         switch (n->type())
         {
            [[likely]] case node_type::leaf:
            {
               auto* l = static_cast<const leaf_node*>(n);
               if (++_path_back->branch == l->num_branches()) [[unlikely]]
                  break;
               return next_branch(l->get_key(_path_back->branch)), true;
            }
            [[unlikely]] case node_type::inner:
            {
               auto* i = static_cast<const inner_node*>(n);
               if (++_path_back->branch == i->num_branches()) [[unlikely]]
                  break;
               push(i->get_branch(_path_back->branch));
               continue;
            }
            [[unlikely]] case node_type::inner_prefix:
            {
               auto* ip = static_cast<const inner_prefix_node*>(n);
               if (_path_back->branch == branch_number(-1)) [[unlikely]]
                  append_key(ip->prefix());
               if (++_path_back->branch == ip->num_branches()) [[unlikely]]
                  break;
               push(ip->get_branch(_path_back->branch));
               continue;
            }
            [[unlikely]] case node_type::value:
               [[fallthrough]];
            default:
               std::unreachable();
         }
         if (not pop()) [[unlikely]]
            return false;
      }  // while true
      std::unreachable();
   }
   inline bool cursor::prev() noexcept
   {
      assert(not is_rend());
      auto read_lock = _node.session()->lock();
      return prev_impl();
   }
   inline bool cursor::prev_impl() noexcept
   {
      while (true)
      {
         const node* n = _node.session()->get_ref<node>(_path_back->adr).obj();
         switch (n->type())
         {
            [[likely]] case node_type::leaf:
            {
               auto* l = static_cast<const leaf_node*>(n);
               if (branch_number(-2) == _path_back->branch) [[unlikely]]
                  _path_back->branch = branch_number(l->num_branches());
               if (--_path_back->branch == branch_number(-1)) [[unlikely]]
                  break;
               return next_branch(l->get_key(_path_back->branch)), true;
            }
            [[unlikely]] case node_type::inner:
            {
               auto* i = static_cast<const inner_node*>(n);
               if (branch_number(-2) == _path_back->branch) [[unlikely]]
                  _path_back->branch = branch_number(i->num_branches());
               if (--_path_back->branch == branch_number(-1)) [[unlikely]]
                  break;
               push_end(i->get_branch(_path_back->branch));
               continue;
            }
            [[unlikely]] case node_type::inner_prefix:
            {
               auto* ip = static_cast<const inner_prefix_node*>(n);
               if (branch_number(-2) == _path_back->branch) [[unlikely]]
               {
                  _path_back->branch = branch_number(ip->num_branches());
                  append_key(ip->prefix());
               }
               if (--_path_back->branch == branch_number(-1)) [[unlikely]]
                  break;
               push_end(ip->get_branch(_path_back->branch));
               continue;
            }
            [[unlikely]] case node_type::value:
               [[fallthrough]];
            default:
               std::unreachable();
         }
         if (not reverse_pop()) [[unlikely]]
            return false;
      }  // while true
      std::unreachable();
   }

}  // namespace psitri
