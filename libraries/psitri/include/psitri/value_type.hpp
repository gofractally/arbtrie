#pragma once
#include <cstring>
#include <psitri/node/node.hpp>
#include <psitri/util.hpp>
#include <string>
#include <variant>

namespace psitri
{
   inline key_view to_key(const char* str)
   {
      return key_view(str, strlen(str));
   }
   inline value_view to_value(const char* str)
   {
      return value_view(str, strlen(str));
   }
   inline key_view to_key_view(const std::string& str)
   {
      return key_view((const char*)str.data(), str.size());
   }
   inline value_view to_value_view(const std::string& str)
   {
      return value_view((const char*)str.data(), str.size());
   }
   /**
    *  Variant Wrapper to pass different types of values through update/insert
    *  operations.
    */
   struct value_type
   {
      enum class types
      {
         data,        // binary data
         value_node,  // contains an address of a value_node containing user data
         remove,      // empty state
         subtree      // contains a address as a user value
      };

      friend std::ostream& operator<<(std::ostream& os, types t)
      {
         switch (t)
         {
            case types::data:
               return os << "data";
            case types::value_node:
               return os << "value_node";
            case types::remove:
               return os << "remove";
            case types::subtree:
               return os << "subtree";
         }
         return os;
      }

      value_type(const char* str) noexcept : data(to_key(str)) {}
      explicit value_type(const std::string& vv) noexcept : data(to_key_view(vv)) {}
      value_type(value_view vv) noexcept : data(vv) {}
      value_type() noexcept : data(std::monostate{}) {}  // Default constructs to empty state

      // Static helper to construct value_type from ptr_address with explicit type
      template <types Type>
      static value_type make(ptr_address i) noexcept
      {
         static_assert(Type == types::subtree || Type == types::value_node,
                       "ptr_address can only be used with subtree or value_node types");
         value_type v;
         v.data.emplace<static_cast<size_t>(Type)>(i);
         return v;
      }
      static value_type make_subtree(ptr_address i) noexcept { return make<types::subtree>(i); }
      static value_type make_value_node(ptr_address i) noexcept
      {
         return make<types::value_node>(i);
      }

      uint32_t size() const noexcept
      {
         switch (type())
         {
            case types::data:
               return std::get<value_view>(data).size();
            case types::subtree:
            case types::value_node:
               return sizeof(ptr_address);
            case types::remove:
               return -1;
         }
      }

      const value_view& view() const noexcept { return std::get<value_view>(data); }
      ptr_address       subtree_address() const noexcept
      {
         return std::get<static_cast<size_t>(types::subtree)>(data);
      }
      ptr_address value_address() const noexcept
      {
         return std::get<static_cast<size_t>(types::value_node)>(data);
      }
      bool        is_address() const noexcept { return is_subtree() or is_value_node(); }
      ptr_address address() const noexcept
      {
         return is_subtree() ? subtree_address() : value_address();
      }
      bool is_view() const noexcept { return data.index() == static_cast<size_t>(types::data); }
      bool is_subtree() const noexcept
      {
         return data.index() == static_cast<size_t>(types::subtree);
      }
      bool is_value_node() const noexcept
      {
         return data.index() == static_cast<size_t>(types::value_node);
      }
      bool is_remove() const noexcept { return data.index() == static_cast<size_t>(types::remove); }

      types type() const noexcept { return static_cast<types>(data.index()); }

      template <typename Visitor>
      decltype(auto) visit(Visitor&& visitor) const
      {
         return std::visit(std::forward<Visitor>(visitor), data);
      }

      void place_into(uint8_t* buffer, uint16_t size) const noexcept
      {
         if (const value_view* vv = std::get_if<value_view>(&data))
         {
            memcpy(buffer, vv->data(), vv->size());
         }
         else
         {
            assert(size == sizeof(ptr_address));
            ptr_address id = address();
            memcpy(buffer, &id, sizeof(ptr_address));
         }
      }
      friend std::ostream& operator<<(std::ostream& out, const value_type& v)
      {
         if (v.is_subtree() or v.is_value_node())
            return out << v.address();
         return out << v.view();
      }

      friend bool operator==(const value_type& lhs, const value_type& rhs) noexcept
      {
         return lhs.data == rhs.data;
      }

     private:
      // Order must match types enum
      std::variant<
          value_view,      // data
          ptr_address,     // subtree
          std::monostate,  // remove
          ptr_address      // value_node - TODO: consider using a different type to avoid ambiguity
          >
          data;

      // Allow construction with in_place_index
      template <std::size_t I>
      value_type(std::in_place_index_t<I>, ptr_address i) noexcept : data(std::in_place_index<I>, i)
      {
      }
   };

}  // namespace psitri
