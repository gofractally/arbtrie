#pragma once
#include <arbtrie/concepts.hpp>
#include <arbtrie/debug.hpp>
#include <arbtrie/find_byte.hpp>
#include <arbtrie/inner_node.hpp>
#include <concepts>

namespace arbtrie
{

   /**
    *  Setlist Node - A node implementation that stores branches in a sorted list.
    *  This type must satisfy inner_node_concept to ensure it provides all required
    *  functionality for inner nodes in the tree.
    *
    *  - break even storage with full node is 206 elements
    *  - break even storage with a (hypothetical) bitset node is 32 elements
    *  - always more effecient with storage than (hypothetical) index node
    *  - can hold up to 257 elements in a less effecient manner than
    *  full node
    *
    *  - O(n/2) average time for get/update
    *  - O(n/2) average time for lower bound 
    * 
    *  Node is always allocated as a multiple of the page size
    *
    *  notional data layout
    *  --------------------
    *  // object_header (base of node_header) - 12 bytes
    *  uint32_t checksum:8;        // First byte for checksum
    *  uint32_t sequence:24;       // 24-bit sequence number
    *  id_address _node_id;        // 8 bytes for node address
    *  uint32_t _ntype:3;          // node type
    *  uint32_t _nsize:25;         // allocated bytes 
    *  uint32_t _unused:3;         // unused bits
    *  uint32_t _header_type:1;    // 0 for node, 1 for allocator
    *  
    *  // node_header fields - 4 additional bytes
    *  id_region _branch_id_region; // 2 bytes for branch region
    *  uint16_t _num_branches:9;    // number of branches
    *  uint16_t _binary_node_opt:1; // binary node optimization flag
    *  uint16_t _unused:6;          // unused bits
    *  
    *  // inner_node<setlist_node> fields - 12 bytes
    *  uint32_t _descendants;         // 4 bytes for descendant count
    *  id_address _eof_value;         // 8 bytes for EOF value pointer
    *  uint32_t _eof_subtree:1;       // whether EOF is a subtree
    *  uint32_t _prefix_capacity:10;  // size of prefix buffer
    *  uint32_t _prefix_size:10;      // actual prefix length
    *  uint32_t _unused:11;           // unused bits
    *  
    *  // setlist_node has no additional fixed fields
    *  
    *  // Variable-sized data (layout in memory)
    *  uint8_t prefix[_prefix_capacity];      // Variable size prefix data
    *  uint8_t setlist[_num_branches];        // Character values for branches
    *  // potentially unused space in the middle
    *  // id_index pointers grow backward from the end of allocated space
    *  id_index branches[_num_branches];      // Branch pointers (at end of node)
    * 
    * | Size | Available | Required | Max     | Leftover | Optimal    |
    * |      | Space     | Prefix s | Branches| Bytes    | Prefix Cap |
    * |------|-----------|----------|---------|----------|------------|
    * |  64  |    32     |    0     |   10    |    2     |     2      |
    * | 128  |    96     |    0     |   32    |    0     |     0      |
    * | 192  |   160     |    0     |   53    |    1     |     1      |
    * | 256  |   224     |    0     |   74    |    2     |     2      |
    * | 320  |   288     |    0     |   96    |    0     |     0      |
    * | 384  |   352     |    0     |  117    |    1     |     1      |
    * | 448  |   416     |    0     |  138    |    2     |     2      |
    * | 512  |   480     |    0     |  160    |    0     |     0      |
    * 
    * 
    */
   class setlist_node : public inner_node<setlist_node>
   {
     public:
      static const node_type type = node_type::setlist;
      using node_header::get_type;

      // the data between tail() and end of prefix capacity
      uint16_t branch_data_cap() const
      {
         return (_nsize - sizeof(setlist_node) - prefix_capacity());
      }
      // the max number of branches
      uint8_t        branch_capacity() const { return _setlist_branch_capacity; }
      uint8_t*       get_setlist_ptr() { return end_prefix(); }
      const uint8_t* get_setlist_ptr() const { return end_prefix(); }
      int            get_setlist_size() const { return num_branches(); }
      key_view       get_setlist() const { return to_key(get_setlist_ptr(), get_setlist_size()); }

      id_index*       get_branch_ptr() { return ((id_index*)tail()) - branch_capacity(); }
      const id_index* get_branch_ptr() const
      {
         return ((const id_index*)tail()) - branch_capacity();
      }
      const id_index* get_branch_end_ptr() const
      {
         return ((id_index*)tail()) - branch_capacity() + num_branches();
      }

      sal::alloc_hint get_branch_alloc_hint() const
      {
         return sal::alloc_hint(branch_region(), get_branch_ptr(), num_branches());
      }

      constexpr local_index begin_index() const { return local_index(-1); }
      constexpr local_index end_index() const
      {
         return local_index(num_branches() + has_eof_value());
      }
      // Returns the index of the the branch matching k or end_index() if no match
      local_index get_index(key_view k) const
      {
         if (k.empty())
            return has_eof_value() ? local_index(0) : end_index();

         auto pos = get_setlist().find(k.front());

         if (pos == key_view::npos)
            return end_index();
         return local_index(pos + has_eof_value());
      }
      id_address get_branch(local_index idx) const
      {
         return id_address(branch_region(), get_branch_ptr()[idx.to_int() - has_eof_value()]);
      }
      local_index lower_bound_index(key_view k) const
      {
         if (k.empty())
            return local_index(-1 + has_eof_value());

         auto lb = arbtrie::lower_bound((const uint8_t*)get_setlist_ptr(), num_branches(),
                                        uint8_t(k.front()));
         return local_index(lb + has_eof_value());
      }
      local_index upper_bound_index(key_view k) const
      {
         if (k.empty())
            return local_index(-1 + has_eof_value());

         auto sl  = get_setlist_ptr();
         auto slp = sl;
         auto sle = slp + num_branches();

         while (slp != sle && uint8_t(*slp) <= uint8_t(k.front()))
            ++slp;
         return local_index(slp - sl + has_eof_value());
      }

      // Required functions for is_node_header_derived concept
      local_index next_index(local_index index) const
      {
         assert(index >= begin_index() or index < end_index());
         return ++index;
      }

      local_index prev_index(local_index index) const
      {
         assert(index <= end_index() or index > begin_index());
         if (index == local_end_index) [[unlikely]]
            index = end_index();
         return --index;
      }

      key_view get_branch_key(local_index index) const
      {
         assert(index <= end_index() or index > begin_index());

         // key_size is 1 when:
         // 1. index > 0 (not the EOF value)
         // 2. OR when index == 0 but there is no EOF value
         // In other words, key_size is 1 for all branch indices except EOF value
         const bool key_size = !(bool(index.to_int() == 0) and has_eof_value());
         return key_view((const char*)get_setlist_ptr() + index.to_int() - has_eof_value(),
                         key_size);
      }

      local_index get_branch_index(key_view k) const
      {
         if (k.empty())
         {
            if (has_eof_value())
               return local_index(0);
            return end_index();
         }
         auto pos = get_setlist().find(k.front());
         if (pos == key_view::npos)
            return end_index();
         return local_index(pos + has_eof_value());
      }

      // TODO impliment thse on inner_node instead of setlist and full
      bool              has_value() const { return has_eof_value(); }
      value_type        value() const { return get_eof_value(); }
      value_type::types get_value_type() const
      {
         if (is_eof_subtree())
            return value_type::types::subtree;
         return value_type::types::value_node;
      }
      value_type::types get_type(local_index index) const
      {
         if (has_eof_value() and index == local_index(0))
            return get_value_type();
         return value_type::types::value_node;
      }
      value_type get_value(local_index index) const
      {
         //std::cerr << "setlist_node::get_value: index: " << index.to_int() << std::endl;
         if (has_eof_value() and index == local_index(0)) [[unlikely]]
            return value();
         auto branch_idx = get_branch_ptr()[index.to_int() - has_eof_value()];
         return value_type::make_value_node(id_address(branch_region(), branch_idx));
      }

      /**
       * Returns the value at the given key and modifies the key to contain only the trailing portion.
       * If no value is found, returns a remove value_type.
       * @param key - The key to look up, will be modified to contain only the trailing portion if a match is found
       * @return value_type - The value if found, or remove type if not found
       */
      value_type get_value_and_trailing_key(key_view& key) const
      {
         // First check if key matches the common prefix
         key_view prefix = get_prefix();
         if (key.size() < prefix.size() || memcmp(key.data(), prefix.data(), prefix.size()) != 0)
            return value_type();  // Returns remove type

         // Advance past the prefix
         key = key.substr(prefix.size());

         // If we've consumed the entire key, check for EOF value
         if (key.empty())
         {
            if (has_eof_value())
               return get_eof_value();
            return value_type();  // Returns remove type
         }

         // Look up the branch in the setlist
         auto pos = get_setlist().find(key.front());
         if (pos == key_view::npos)
            return value_type();  // Returns remove type

         // Advance past the matched character
         key = key.substr(1);
         return get_value(local_index(pos + has_eof_value()));
      }

      ///@}

      // uint8_t             prefix[prefix_capacity()]
      // uint8_t             setlist[num_branches() - has_eof_value() ]
      // uint8_t             setlist[branch_capacity() - num_branches() - has_eof_value()]
      // uint8_t             unused[varries]
      // id_index            branches[num_branches()-1]
      // id_index            branches[branch_capacity() - num_branches() - 1 ]

      uint8_t calculate_checksum() const
      {
         // Skip only the first byte which contains the checksum
         auto hash = XXH3_64bits(((const char*)this) + checksum_size, _nsize - checksum_size);
         return uint8_t(hash);  // Take lowest byte as 8-bit checksum
      }

      bool          can_add_branch() const { return num_branches() < branch_capacity(); }
      setlist_node& add_branch(branch_index_type br, id_address b);

      void set_index(int idx, uint8_t byte, id_address adr)
      {
         assert(idx < _num_branches);
         assert(adr.region == branch_region());
         assert((char*)(get_branch_ptr() + idx) < tail());
         get_branch_ptr()[idx]  = adr.index;  //id_index(adr.index.to_int());
         get_setlist_ptr()[idx] = byte;
      }

      bool validate() const
      {
         auto sl = get_setlist();
         if (sl.size())
         {
            uint8_t last = sl.front();
            for (int i = 1; i < sl.size(); ++i)
            {
               //ARBTRIE_DEBUG( "last: ", int(last), " < ", int(uint8_t(sl[i])) );
               if (uint8_t(sl[i]) <= last)
               {
                  assert(!"order invalid");
                  throw std::runtime_error("order invalid");
               }
               last = sl[i];
            }
         }
         return true;
      }

      // find the position of the first branch >= br
      int_fast16_t lower_bound_idx(uint_fast16_t br) const
      {
         assert(br > 0 and br <= 256);
         uint8_t br2 = br - 1;
         auto    sl  = get_setlist_ptr();
         auto    slp = sl;
         auto    sle = slp + num_branches();

         while (slp != sle)
         {
            if (uint8_t(*slp) >= uint8_t(br2))
               return slp - sl;
            ++slp;
         }
         return slp - sl;
      }
      int_fast16_t upper_bound_idx(uint_fast16_t br) const
      {
         assert(br > 0 and br <= 256);
         uint8_t br2 = br - 1;
         auto    sl  = get_setlist_ptr();
         auto    slp = sl;
         auto    sle = slp + num_branches();

         while (slp != sle)
         {
            if (uint8_t(*slp) > uint8_t(br2))
               return slp - sl;
            ++slp;
         }
         return slp - sl;
      }

      // find the position of the first branch <= br
      int_fast16_t reverse_lower_bound_idx(uint_fast16_t br) const
      {
         assert(br > 0 and br <= 256);
         uint8_t    br2 = br - 1;
         const auto sl  = get_setlist_ptr();
         const auto sle = sl + num_branches();
         auto       slp = sle - 1;

         while (slp >= sl)
         {
            if (uint8_t(*slp) <= uint8_t(br2))
            {
               return slp - sl;
            }
            --slp;
         }
         return slp - sl;
      }

      std::pair<branch_index_type, id_address> lower_bound(branch_index_type br) const
      {
         if (br >= max_branch_count) [[unlikely]]
            return std::pair<int_fast16_t, id_address>(max_branch_count, {});
         if (br == 0)
         {
            if (_eof_value)
               return std::pair<int_fast16_t, id_address>(0, _eof_value);
            ++br;
         }

         uint8_t        b = br - 1;
         const uint8_t* s = (uint8_t*)get_setlist_ptr();
         const auto*    e = s + get_setlist_size();

         const uint8_t* p = s;
         while (p < e and b > *p)
            ++p;
         if (p == e)
            return std::pair<branch_index_type, id_address>(max_branch_count, {});
         return std::pair<branch_index_type, id_address>(
             char_to_branch(*p), {branch_region(), get_branch_ptr()[(p - s)]});
      }

      std::pair<branch_index_type, id_address> reverse_lower_bound(branch_index_type br) const
      {
         if (br == 0)
         {
            if (_eof_value)
               return std::pair<int_fast16_t, id_address>(0, _eof_value);
            return std::pair<int_fast16_t, id_address>(-1, {});
         }

         uint8_t        b = br - 1;
         const uint8_t* s = (uint8_t*)get_setlist_ptr();
         const auto*    e = s + get_setlist_size() - 1;

         const uint8_t* p = e;
         while (p >= s and b < *p)
         {
            --p;
         }
         if (p < s)
         {
            if (_eof_value)
               return std::pair<int_fast16_t, id_address>(0, _eof_value);
            else
               return std::pair<branch_index_type, id_address>(-1, {});
         }
         return std::pair<branch_index_type, id_address>(
             char_to_branch(*p), {branch_region(), get_branch_ptr()[(p - s)]});
      }

      auto& set_branch(branch_index_type br, id_address b)
      {
         assert(br < 257);
         assert(br > 0);
         assert(b);
         assert(b.region == branch_region());

         auto pos = get_setlist().find(br - 1);
         assert(pos != key_view::npos);
         get_branch_ptr()[pos] = b.index;  //id_index(b.index().to_int());
         return *this;
      }

      id_address get_branch(uint_fast16_t br) const
      {
         assert(br < 257);
         assert(br > 0);

         auto pos = get_setlist().find(br - 1);
         if (pos == key_view::npos)
            return id_address();

         return {branch_region(), get_branch_ptr()[pos]};
      }

      id_address find_branch(uint_fast16_t br) const
      {
         assert(br < 257);
         assert(br > 0);

         auto pos = get_setlist().find(br - 1);
         if (pos == key_view::npos)
            return id_address();

         return {branch_region(), get_branch_ptr()[pos]};
      }

      template <typename Visitor>
         requires requires(Visitor v, id_address addr) { v(addr); }
      inline void visit_branches(Visitor&& visitor) const
      {
         if (has_eof_value())
            visitor(id_address(_eof_value));

         auto*             ptr = get_branch_ptr();
         const auto* const end = ptr + num_branches();
         while (ptr != end)
         {
            visitor(id_address{branch_region(), *ptr});
            ++ptr;
         }
      }

      template <typename Visitor>
         requires requires(Visitor v, branch_index_type br, id_address addr) { v(br, addr); }
      inline void visit_branches_with_br(Visitor&& visitor) const
      {
         const auto*       slp   = get_setlist_ptr();
         const auto*       start = get_branch_ptr();
         const auto* const end   = start + num_branches();
         const auto*       ptr   = start;

         if (has_eof_value())
            visitor(0, id_address(_eof_value));

         while (ptr != end)
         {
            visitor(int(*slp) + 1, id_address(branch_region(), *ptr));
            ++ptr;
            ++slp;
         }
      }

      // @pre has_eof_value() == true
      // branch exists and is set
      auto& remove_branch(int_fast16_t br)
      {
         assert(br > 0);
         assert(num_branches() > 0);
         assert(br < max_branch_count);

         uint8_t ch  = uint8_t(br - 1);
         auto    sl  = get_setlist();
         auto    pos = sl.find(ch);

         assert(pos != key_view::npos);

         auto slp    = get_setlist_ptr();
         auto bptr   = get_branch_ptr();
         auto remain = sl.size() - pos - 1;
         auto pos1   = pos + 1;
         memmove(slp + pos, slp + pos1, remain);
         memmove(bptr + pos, bptr + pos1, remain * sizeof(id_index));
         --_num_branches;
         return *this;
      }

      inline static int_fast16_t alloc_size(const clone_config& cfg)
      {
         auto min_size = sizeof(setlist_node) + cfg.prefix_capacity() +
                         (cfg.branch_cap) * (sizeof(id_index) + 1);
         return round_up_multiple<64>(min_size);
      }

      inline static int_fast16_t alloc_size(const setlist_node* src, const clone_config& cfg)
      {
         assert(cfg.data_cap == 0);
         assert(src != nullptr);
         assert(cfg.branch_cap < 192);

         auto pcap     = cfg.set_prefix ? cfg.set_prefix->size()
                                        : std::max<int>(cfg.prefix_capacity(), src->prefix_size());
         auto bcap     = std::max<int>(cfg.branch_cap, src->num_branches());
         auto min_size = sizeof(setlist_node) + pcap + (bcap) * (sizeof(id_index) + 1);

         auto asize = round_up_multiple<64>(min_size);

         assert(src->num_branches() <=
                (asize - sizeof(setlist_node) - cfg.prefix_cap) / (1 + sizeof(id_index)));

         return asize;
      }

      setlist_node(int_fast16_t asize, id_address_seq nid, const clone_config& cfg)
          : inner_node(asize, nid, cfg, 0)
      {
      }

      setlist_node(int_fast16_t        asize,
                   id_address_seq      nid,
                   const setlist_node* src,
                   const clone_config& cfg)
          : inner_node(asize, nid, src, cfg)
      {
         assert(src->num_branches() <= branch_capacity());
         assert((char*)get_branch_ptr() + src->num_branches() * sizeof(id_index) <= tail());
         assert((char*)get_setlist_ptr() + src->get_setlist_size() <= tail());

         memcpy(get_setlist_ptr(), src->get_setlist_ptr(), src->get_setlist_size());
         memcpy(get_branch_ptr(), src->get_branch_ptr(), src->num_branches() * sizeof(id_index));

         assert(validate());
      }
   } __attribute((packed));  // end setlist_node

   static_assert(sizeof(setlist_node) ==
                 sizeof(node_header) + sizeof(uint64_t) + sizeof(id_address));

   inline setlist_node& setlist_node::add_branch(branch_index_type br, id_address b)
   {
      assert(br < max_branch_count);
      assert(br > 0);
      assert(b.region == branch_region());

      id_index* branches = get_branch_ptr();
      assert(_num_branches <= branch_capacity());

      auto pos = lower_bound_idx(br);

      auto slp = get_setlist_ptr();
      auto blp = get_branch_ptr();

      auto nbranch  = num_branches();
      auto sl_found = slp + pos;
      auto sl_end   = slp + nbranch;

      assert((char*)sl_found + 1 + (sl_end - sl_found) <= tail());
      memmove(sl_found + 1, sl_found, sl_end - sl_found);

      *sl_found = br - 1;

      auto b_found = blp + pos;
      auto b_end   = blp + nbranch;

      assert((char*)b_found + 1 + ((char*)b_end - (char*)b_found) <= tail());
      memmove(b_found + 1, b_found, (char*)b_end - (char*)b_found);

      *b_found = b.index;  //id_index(b.index().to_int());

      ++_num_branches;
      return *this;
   }

}  // namespace arbtrie
