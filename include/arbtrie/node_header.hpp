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

   enum class header_type : uint32_t
   {
      node      = 0,
      allocator = 1
   };

   /**
    *  Designed to overlap with the object_header data structure and enable
    *  discriminating between node_header and allocator_header types at 
    *  runtime using the _header_type flag.
    * 
    *  From time to time the seg_allocator will write to the header to record
    *  information about the segment and mark the ending and beginning of 
    *  transactions, compactions, etc.
    *  
    *  Because the allocator works on 64 byte cachelines, the allocator_header
    *  is allowed to be the same size with little penalty. Therefore it is used
    *  track useful statistics and error recovery information. Furthermore, when
    *  protecting or msyncing data the OS requires page aligned addresses which
    *  means that in most cases the allocator_header will occupy the free space
    *  in the left over bytes at the end of the last writable page. 
    * 
    *  A segment is a sequence of allocator_headers and node_headers.
    * 
    * [ n n n n a n n a...]
    * 
    * The allocation_header stores the checksum of all data from the
    * end of the last allocator_header to the start of this allocation header.
    * 
    * In most cases this is synced at the same time as the other node headers, 
    * unless the allocator_header is the first data in the start of a new
    * page. In that case it won't get protected or synced until the next page is 
    * protected or synced.
    * 
    * The last record in a segment is always an allocator_header.
    */
   struct allocator_header
   {
      enum types : uint8_t
      {
         end_of_segment   = 0,
         start_of_segment = 1,
         free_zone        = 2
      };
      bool is_allocator_header() const
      {
         return header_type(_header_type) == header_type::allocator;
      }
      bool is_end_of_segment() const { return _ntype == end_of_segment; }
      /**
       *  The checksum of the region [this-_start_checksum_offset, _start_checksum_offset + _checksum_bytes)
       * 
       *  Typically this would align with the start of the prior allocator_header, but there is not always
       *  a prior allocator_header.
       */
      uint64_t _checksum;
      uint32_t _ntype : 3;   ///< deepnds on _header_type
      uint32_t _nsize : 25;  ///< bytes allocated for this object
      uint32_t _unused : 3;  ///< truly unused bits, should never be written to
      /// used by segment allocator for bookkeeping, changes the meaning of _ntype
      uint32_t _header_type : 1 = 1;

      /// time this header was written, implies everything before this record
      /// was written before this time.
      uint64_t _time_stamp_ms;

      /// the position in the current segment where the checksumed data starts
      uint32_t _start_checksum_pos;
      /// the number of bytes in the checksumed data after _start_checksum_pos,
      uint32_t _checksum_bytes;

      /// the previous allocator_header in the current segment, used to
      /// form a linked list of allocator_headers.
      uint32_t _prev_aheader_pos;

      /**
       * When compacting data from another segment, this field tells us the original
       * age of the source data, the compactor will use this age for all nodes it compacts
       * until it comes across an updated age.
       */
      uint64_t _source_age_ms;

      /**
       * Documents the source of the segment the data came from, which can facilitate establishing
       * a total ordering of nodes during recovery, may not be needed, but we have 64 bytes to
       * play with
       */
      uint32_t _source_seg;

      /// regardless of what _nsize is, the allocations should always be 64 byte cacheline aligned
      uint32_t          capacity() const { return (_nsize + 63) & -64; }
      allocator_header* next() const { return (allocator_header*)(((char*)this) + capacity()); }
      allocator_header* prev(char* segment_base) const
      {
         return (allocator_header*)(segment_base + _prev_aheader_pos);
      }
   };
   static_assert(sizeof(allocator_header) <= 64);

   /**
    * Base class for all objects that can be addressed and stored in the database.
    * Contains the core identity and type information, but doesn't include branch
    * region or number of branches which are specific to node types.
    * 
    * @note the object_header must align with the mapped_memory::allocation_header such that
    * _header_type bit is in the same position in both types. It cannot be the
    * first byte of the object because of checksum requirements and we cannot
    * use other types of inheritance to enforce this alignment due to the use of
    * bitfields. This invariant is checked in the unit tests.
    */
   struct object_header
   {
      static constexpr uint32_t checksum_size = 1;  // Size in bytes of the checksum field
      uint32_t                  checksum : 8;       // Leading byte for checksum
      uint32_t   sequence : 24;  // 24-bit sequence number, set only during construction
      id_address _node_id;       // the ID of this object

      uint32_t _ntype : 3;   ///< node_type
      uint32_t _nsize : 25;  ///< bytes allocated for this object
      uint32_t _unused : 3;  ///< truly unused bits, should never be written to

      /// used by segment allocator for bookkeeping, changes the meaning of _ntype
      uint32_t _header_type : 1 = 0;  /// = 0 for node_header, = 1 for allocator_header

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
      void assert_checksum() const
      {
         if (not validate_checksum())
            throw std::runtime_error("checksum validation failed");
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
