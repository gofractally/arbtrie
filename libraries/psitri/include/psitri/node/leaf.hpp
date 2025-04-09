#pragma once
#include <hash/xxhash.h>
#include <psitri/node/node.hpp>
#include <psitri/util.hpp>
#include <psitri/value_type.hpp>

namespace psitri
{

   /**
   * Stores up to 4096 bytes 
   * fast insert doesn't wory about memory layout, but
   * compactor will organize it so that keys are laid out in
   * optimal binary search order and separately from values
   * key hashes make quick lookups possible if key is known
   * separates keys from values (uses 
   *
   * Overhead per Key:
   *   1 keyhash
   *   2 keyoffset
   *   2 keysize
   *   2 valueoffset
   *   2 value size+checksum... 9 bytes per key (if inline), 7.25 bytes per key for nodes
   *
   * On COW expand to 4096 bytes to maximize alloc space without having to copy it
   * On compact... node gets optimized to smallest size...
   */
   class leaf_node : public node
   {
     public:
      /// default constructor, contains one key, value pair
      leaf_node(size_t alloc_size, ptr_address_seq seq, key_view key, const value_type& value);
      /// clone and optimize
      leaf_node(size_t alloc_size, ptr_address_seq seq, const leaf_node* clone);

      /// clone and optimize subset and truncate keys by cprefix
      leaf_node(size_t           alloc_size,
                ptr_address_seq  seq,
                const leaf_node* clone,
                key_view         cprefix,
                branch_number    start,
                branch_number    end);

      /// clone and insert key, value
      leaf_node(size_t            alloc_size,
                ptr_address_seq   seq,
                const leaf_node*  clone,
                key_view          ins,
                const value_type& value);
      /// clone and remove bn
      leaf_node(size_t alloc_size, ptr_address_seq seq, const leaf_node* clone, branch_number bn);

      void     dump() const;
      uint16_t alloc_pos() const noexcept { return _alloc_pos; }
      uint16_t dead_space() const noexcept { return _dead_space; }
      uint32_t clines_capacity() const noexcept { return _cline_cap; }
      bool     is_optimal_layout() const noexcept { return _optimal_layout; }

      struct split_pos
      {
         key_view cprefix;           ///< common prefix of all keys
         uint8_t  divider;           ///< byte to split on
         uint32_t less_than_count;   ///< number of keys less than divider
         uint32_t greater_eq_count;  ///< number of keys greater than or equal to divider
      };
      split_pos get_split_pos() const noexcept;

      key_view get_key(branch_number bn) const noexcept
      {
         assert(bn < num_branches());
         return get_key_ptr(keys_offsets()[*bn])->get();
      }
      value_type get_value(branch_number bn) const noexcept
      {
         assert(bn < num_branches());
         value_branch vb = value_offsets()[*bn];
         switch (vb.type())
         {
            case value_branch::value_type::subtree:
               return value_type::make_subtree(get_address(vb));
            case value_branch::value_type::value_node:
               return value_type::make_value_node(get_address(vb));
            case value_branch::value_type::inline_data:
               return get_value_ptr(vb.offset())->get();
            case value_branch::value_type::null:
               return value_type(value_view());
         }
         std::unreachable();
      }

      /// @return size of old value
      size_t update_value(branch_number bn, const value_type& value) noexcept;

      /// determines whether there is enough space to insert the key
      /// @return the amount of free space after inserting the key,
      /// this will be negative if there is not enough space.
      int can_insert(key_view key, const value_type& value) const noexcept;

      int free_space() const noexcept { return alloc_head() - clines_end(); }

      /// @return the branch number of the inserted key
      /// @pre key is not already in the node
      /// @pre can_insert(key) must be true
      /// @pre bn  == lower_bound(key)
      branch_number insert(branch_number bn, key_view key, const value_type& value) noexcept;

      /// @pre bn != num_branches()
      void remove(branch_number bn) noexcept;

      uint8_t calc_key_hash(key_view key) const noexcept
      {
         return XXH3_64bits(key.data(), key.size());
      }
      /// uses hash to find key
      branch_number get(key_view key) const noexcept
      {
         uint8_t        khash     = calc_key_hash(key);
         const uint8_t* khashes   = key_hashs().data();
         uint32_t       reamining = num_branches();
         do
         {
            auto idx = find_byte(khashes, reamining, khash);
            if (idx == reamining)
               return branch_number(num_branches());
            if (get_key(branch_number(idx)) == key)
               return branch_number(idx);
            ++idx;
            khashes += idx;
            reamining -= idx;
         } while (true);
      }
      /// uses binary search to find key
      branch_number lower_bound(key_view key) const noexcept
      {
         int                  pos[2];
         static constexpr int left_pos  = 0;
         static constexpr int right_pos = 1;
         pos[left_pos]                  = -1;
         pos[right_pos]                 = num_branches();
         while (pos[right_pos] - pos[left_pos] > 1)
         {
            int  middle = (pos[left_pos] + pos[right_pos]) >> 1;
            bool geq    = get_key(branch_number(middle)) >= key;
            pos[geq]    = middle;
         }
         return branch_number(pos[right_pos]);
      }

      /// visit all branches that are ptr_address
      void visit_branches(std::invocable<ptr_address> auto&& lam) const noexcept
      {
         const value_branch* cvb = value_offsets();
         const uint32_t      n   = num_branches();
         for (int i = 0; i < n; ++i)
         {
            if (cvb[i].type() != value_branch::value_type::inline_data)
               lam(get_address(cvb[i]));
         }
      }

      std::span<const ptr_address> clines() const noexcept
      {
         return std::span<const ptr_address>(
             reinterpret_cast<const ptr_address*>(value_offsets() + num_branches()), _cline_cap);
      }

      uint16_t num_branches() const noexcept { return _num_branches; }

     private:
      void set_num_branches(uint16_t n) noexcept { _num_branches = n; }

      uint16_t _alloc_pos;
      uint16_t _dead_space;  // tracks freed data in alloc space
      uint32_t _cline_cap : 9;
      uint32_t _optimal_layout : 1;  ///< a bit that gets set when the node is optimized,
                                     /// cleared when optimized invariants broken
      uint64_t _num_branches : 9;
      uint64_t _unused : 13;
      uint8_t  _key_hashs[/*num_branches()*/];
      // the following fields are dynamic, but follow sequentially after clines
      //   uint8_t      key_hash[num_branch]  // align on 16 byte boundary and 64 byte boundary
      //   uint16_t     keys_offsets[num_branch]
      //   value_branch value_offsets[num_branch];
      //   ptr_address _clines[/*_cline_cap*/];
      //     ... alloc area..., indexed from tail()
      ///  tail() at end of object defined by ((char*)this) + size())
      ///
      ///  _clines is after value_offsets to make it easier to expand when updating
      ///       value nodes, and because this is the last data needed by a query and may not
      ///       be needed for many queries; therefore, get it out of the way of the more critical
      ///       data such as key_hash and keys_offsets which are needed in every query.

      /// offset into the alloc area from tail pointing to a key structure
      using key_offset = ucc::typed_int<uint16_t, struct key_offset_tag>;
      /// offset into the alloc area from tail pointing to a value structure
      using value_offset = ucc::typed_int<uint16_t, struct value_offset_tag>;
      /// offset into clines() array
      using cline_offset = ucc::typed_int<uint16_t, struct cline_offset_tag>;
      using cline_index  = ucc::typed_int<uint8_t, struct cline_index_tag>;

      std::span<uint8_t> key_hashs() noexcept
      {
         return std::span<uint8_t>(_key_hashs, num_branches());
      }
      std::span<const uint8_t> key_hashs() const noexcept
      {
         return std::span<const uint8_t>(_key_hashs, num_branches());
      }
      std::span<key_offset> keys_offsets() noexcept
      {
         return std::span<key_offset>(
             reinterpret_cast<key_offset*>(key_hashs().data() + num_branches()), num_branches());
      }
      std::span<const key_offset> keys_offsets() const noexcept
      {
         return std::span<const key_offset>(
             reinterpret_cast<const key_offset*>(key_hashs().data() + num_branches()),
             num_branches());
      }

      class value_branch
      {
        public:
         enum value_type : uint16_t
         {
            null,
            inline_data,
            value_node,
            subtree
         };
         value_branch() noexcept : _type(null), _offset(0) {}
         value_branch(value_offset offset) noexcept : _type(inline_data), _offset(*offset) {}
         value_branch(value_type t, cline_offset cl, cline_index idx) noexcept
             : _type(t), _offset(*cl << 4 | *idx & 0xF)
         {
            assert(t == value_type::subtree or t == value_type::value_node);
         }

         void clear() noexcept
         {
            _type   = value_type::null;
            _offset = 0;
         }

         bool         is_null() const noexcept { return _type == null; }
         bool         is_inline() const noexcept { return _type == inline_data; }
         bool         is_address() const noexcept { return _type >= value_node; }
         value_type   type() const noexcept { return (value_type)_type; }
         value_offset offset() const noexcept
         {
            assert(type() == value_type::inline_data);
            return value_offset(_offset);
         }
         void set_offset(value_offset off) noexcept
         {
            _type   = value_type::inline_data;
            _offset = *off;
         }
         cline_offset cline() const noexcept
         {
            assert(type() == value_type::subtree or type() == value_type::value_node);
            return cline_offset(_offset >> 4);  // Get upper 10 bits
         }
         void set_cline_and_idx(cline_offset cl,
                                cline_index  idx,
                                value_type   t = value_type::subtree) noexcept
         {
            assert(t == value_type::subtree or t == value_type::value_node);
            _type   = t;
            _offset = (*cl << 4) | (*idx & 0xF);
         }
         cline_index cline_idx() const noexcept
         {
            assert(type() == value_type::subtree or type() == value_type::value_node);
            return cline_index(_offset & 0xF);  // Get lower 4 bits
         }

        private:
         uint16_t _type : 2;     // subtree, value_node, inline, null
         uint16_t _offset : 14;  // if type is inline, bytes from tail
         // alt interp of offset:
         // uint16_t cline : 10;     // for subtree or value node
         // uint16_t cline_idx : 4;  // for subtree or value node
      };  // class value_branch
      static_assert(sizeof(value_branch) == 2);

      struct meta_arrays
      {
         char* khash;
         char* khash_end;
         char* koffs;
         char* koffs_end;
         char* voffs;
         char* voffs_end;
         char* clines;
         char* clines_end;
      };

      meta_arrays get_meta_arrays() noexcept
      {
         return meta_arrays{(char*)_key_hashs,
                            (char*)_key_hashs + num_branches(),
                            (char*)_key_hashs + num_branches(),
                            (char*)value_offsets(),
                            (char*)value_offsets(),
                            (char*)clines().data(),
                            (char*)clines().data(),
                            (char*)clines().data() + _cline_cap * sizeof(ptr_address)};
      }

      ptr_address get_address(value_branch vb) const noexcept
      {
         return ptr_address(*(clines()[*vb.cline()]) + *vb.cline_idx());
      }

      value_branch* value_offsets() noexcept
      {
         return reinterpret_cast<value_branch*>(keys_offsets().data() + num_branches());
      }
      const value_branch* value_offsets() const noexcept
      {
         return reinterpret_cast<const value_branch*>(keys_offsets().data() + num_branches());
      }
      const value_branch* value_offsets_end() const noexcept
      {
         return reinterpret_cast<const value_branch*>(value_offsets() + num_branches());
      }
      std::span<ptr_address> clines() noexcept
      {
         return std::span<ptr_address>(
             reinterpret_cast<ptr_address*>(value_offsets() + num_branches()), _cline_cap);
      }
      const char* clines_end() const noexcept
      {
         return (const char*)(value_offsets() + num_branches()) + _cline_cap * sizeof(ptr_address);
      }

      /// determine if addr is on an existing cline, or allocate a new one and
      /// return the value_branch for the new cline
      value_branch add_address_ptr(value_branch::value_type t, ptr_address addr) noexcept;

      /// remove the address ptr from the cline index, if there are no references
      void remove_address_ptr(cline_offset cl_off) noexcept;
      /// calculate the number of references to the cline
      int calc_cline_refs(cline_offset cl_off) const noexcept;

      class key
      {
         uint16_t size;
         uint8_t  data[];

        public:
         void set(key_view key)
         {
            size = key.size();
            std::memcpy(data, key.data(), size);
         }
         key_view get() const noexcept { return key_view((const char*)data, size); }
         uint8_t  hash() const noexcept { return XXH3_64bits(this, sizeof(key) + size); }
      };
      class value_data
      {
         uint8_t _checksum;
         uint8_t _size;
         uint8_t _data[];

        public:
         void set(value_view value)
         {
            _size = value.size();
            std::memcpy(_data, value.data(), _size);
            _checksum = XXH3_64bits((char*)&_size, sizeof(_size) + _size);
         }
         value_view get() const noexcept { return value_view((const char*)_data, _size); }
         bool       is_valid() const noexcept
         {
            return _checksum == XXH3_64bits((char*)&_size, sizeof(_size) + _size);
         }
         uint8_t checksum() const noexcept { return _checksum; }
      };
      static_assert(sizeof(key) == 2);
      static_assert(sizeof(value_data) == 2);

      key* get_key_ptr(key_offset off) noexcept
      {
         return reinterpret_cast<key*>(((char*)tail()) - *off);
      }
      const key* get_key_ptr(key_offset off) const noexcept
      {
         return reinterpret_cast<const key*>(((const char*)tail()) - *off);
      }
      key_offset alloc_key(key_view key) noexcept
      {
         SAL_INFO("alloc pos: {} alloc_key: {}", _alloc_pos, key);
         _alloc_pos += key.size() + sizeof(key);
         key_offset off = key_offset(_alloc_pos);
         SAL_INFO("off: {} offhead: {}  val_off_end: {} size: {}", off, size() - *off,
                  (uint8_t*)value_offsets_end() - (uint8_t*)this, size());
         assert((uint8_t*)get_key_ptr(off) >= (uint8_t*)value_offsets_end() &&
                "Allocation would overlap with value offsets");
         get_key_ptr(off)->set(key);
         return off;
      }
      value_data* get_value_ptr(value_offset off) noexcept
      {
         return reinterpret_cast<value_data*>(((char*)tail()) - *off);
      }
      const value_data* get_value_ptr(value_offset off) const noexcept
      {
         auto ptr = reinterpret_cast<const value_data*>(((const char*)tail()) - *off);
         return ptr;
      }
      value_offset alloc_value(value_view value) noexcept
      {
         _alloc_pos += value.size() + sizeof(value_data);
         value_offset off = value_offset(_alloc_pos);
         assert((uint8_t*)get_value_ptr(off) >= (uint8_t*)value_offsets_end() &&
                "Allocation would overlap with value offsets");
         get_value_ptr(off)->set(value);
         return off;
      }
      bool can_alloc_key(key_view key) const noexcept
      {
         return (const uint8_t*)get_key_ptr(key_offset(_alloc_pos + key.size() + sizeof(key))) <
                (const uint8_t*)value_offsets_end();
      }
      bool can_alloc_value(value_view value) const noexcept
      {
         return (const uint8_t*)get_value_ptr(
                    value_offset(_alloc_pos + value.size() + sizeof(value_data))) <
                (const uint8_t*)value_offsets_end();
      }
      void free_key(key_offset off) noexcept
      {
         _dead_space += sizeof(key) + get_key_ptr(off)->get().size();
      }
      void free_value(value_offset off) noexcept
      {
         _dead_space += sizeof(value_data) + get_value_ptr(off)->get().size();
      }
      const char* alloc_head() const noexcept { return (const char*)tail() - _alloc_pos; }
      char*       alloc_head() noexcept { return (char*)tail() - _alloc_pos; }

   } __attribute__((packed));
   static_assert(sizeof(leaf_node) == 20);

}  // namespace psitri
