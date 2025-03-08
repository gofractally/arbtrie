#pragma once
#include <arbtrie/address.hpp>
#include <arbtrie/debug.hpp>
#include <arbtrie/node_meta.hpp>
#include <arbtrie/util.hpp>
#include <string_view>

#define XXH_INLINE_ALL
#include <arbtrie/hash/xxhash.h>

namespace arbtrie
{
   struct node_header;
   uint32_t                   calculate_checksum(const node_header* h);
   static constexpr const int max_branch_count = 257;

   using branch_index_type = int_fast16_t;
   inline constexpr branch_index_type char_to_branch(uint8_t c)
   {
      return branch_index_type(c) + 1;
   }
   inline constexpr uint8_t branch_to_char(branch_index_type b)
   {
      return uint8_t(b - 1);
   }

   /**
    * Base class for all objects that can be addressed and stored in the database.
    * Contains the core identity and type information, but doesn't include branch
    * region or number of branches which are specific to node types.
    */
   struct object_header
   {
      static constexpr uint32_t checksum_size = 1;  // Size in bytes of the checksum field
      uint32_t                  checksum : 8;       // Leading byte for checksum
      uint32_t   sequence : 24;  // 24-bit sequence number, set only during construction
      id_address _node_id;       // the ID of this object

      uint32_t _ntype : 3;    // node_type
      uint32_t _nsize : 25;   // bytes allocated for this object
      uint32_t _unused2 : 4;  // truly unused bits, should never be written to

      // Constructor
      inline object_header(uint32_t size, id_address_seq nid, node_type type = node_type::freelist)
          : checksum(0), sequence(nid.sequence), _node_id(nid.address), _ntype(type), _nsize(size)
      {
         assert(_node_id == nid.address);
      }

      void set_address(id_address a) { _node_id = a; }
      void set_type(node_type t) { _ntype = (int)t; }
      void set_id(id_address i) { _node_id = i; }

      uint32_t   size() const { return _nsize; }
      id_address address() const { return _node_id; }
      node_type  get_type() const { return (node_type)_ntype; }

      // size rounded up to the nearest 16 bytes
      inline uint32_t object_capacity() const { return (_nsize + 15) & -16; }

      uint32_t       get_sequence() const { return sequence; }
      id_address_seq address_seq() const { return id_address_seq(_node_id, sequence); }

      template <typename T>
      T* as()
      {
         assert(T::type == (node_type)_ntype);
         return static_cast<T*>(this);
      }

      template <typename T>
      const T* as() const
      {
         assert(T::type == (node_type)_ntype);
         return static_cast<const T*>(this);
      }

      char*          body() { return (char*)(this + 1); }
      const char*    body() const { return (const char*)(this + 1); }
      char*          tail() { return ((char*)this) + _nsize; }
      const uint8_t* tail() const { return ((const uint8_t*)this) + _nsize; }

      // Checksum calculation and validation methods
      // These are defined in node_header but accessible from object_header
      uint8_t calculate_checksum() const;
      void    update_checksum() { checksum = calculate_checksum(); }
      bool    has_checksum() const { return checksum != 0; }
      bool    validate_checksum() const
      {
         if (checksum)
            return (checksum == calculate_checksum());
         return true;
      }

      // Return next object in memory
      inline node_header* next() const { return (node_header*)(((char*)this) + object_capacity()); }
   } __attribute((packed));

   /**
    *  keysize limit of 1024 requires 10 bits, longer keys would 
    *  require either:
    *    1. less inlining values into binary_node
    *       a. 30 bytes inline limit for 2048 key length
    *    2. an extra byte per value in binary_node +
    *       a. increasing the _prefix_capacity bit width
    *       b. increasing the _prefix_trunc bit width
    *
    *  RocksDB keysize limit = 8MB
    *  LMDB keysize limit    = 512b
    */
   struct node_header : public object_header
   {
      id_region _branch_id_region;  // the ID region branches from this node are allocated to

      uint16_t _num_branches : 9;  // number of branches that are set
      uint16_t _unused : 7;        // unused bits

      inline node_header(uint32_t       size,
                         id_address_seq nid,
                         node_type      type       = node_type::freelist,
                         uint16_t       num_branch = 0)
          : object_header(size, nid, type), _num_branches(num_branch), _branch_id_region(0)
      {
         assert(intptr_t(this) % 64 == 0);
      }

      id_region branch_region() const { return id_region(_branch_id_region); }
      void      set_branch_region(id_region r) { _branch_id_region = r.to_int(); }

      uint16_t num_branches() const { return _num_branches; }
   } __attribute((packed));
   static_assert(sizeof(node_header) == 16);

   struct full_node;
   struct setlist_node;
   struct binary_node;
   struct value_node;
   struct index_node;

   template <typename T>
   concept is_node_header = std::is_same_v<std::remove_cv_t<T>, node_header>;

   template <typename T>
   concept is_value_type = std::is_same_v<std::remove_cv_t<T>, value_view> or
                           std::is_same_v<std::remove_cv_t<T>, id_address>;

   struct clone_config
   {
      int                     branch_cap = 0;  // inner nodes other than full
      int                     data_cap   = 0;  // value nodes, binary nodes
      int                     prefix_cap = 0;  //
      std::optional<key_view> set_prefix;
      friend auto             operator<=>(const clone_config&, const clone_config&) = default;

      int_fast16_t prefix_capacity() const
      {
         if (set_prefix)
            return std::max<int>(prefix_cap, set_prefix->size());
         return prefix_cap;
         ;
      }
   };

}  // namespace arbtrie
