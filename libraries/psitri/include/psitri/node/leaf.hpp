#pragma once
#include <hash/xxhash.h>
#include <psitri/node/node.hpp>
#include <psitri/util.hpp>
#include <psitri/value_type.hpp>
#include <sal/allocator_session.hpp>

namespace psitri
{

   class leaf_node;
   namespace op
   {
      struct leaf_insert
      {
         const leaf_node& src;
         branch_number    lb;
         key_view         key;
         value_type       value;
         uint8_t          cline_idx = 0xff;
      };
      struct leaf_update
      {
         const leaf_node& src;
         branch_number    lb;
         key_view         key;
         value_type       value;
      };
      struct leaf_remove
      {
         const leaf_node& src;
         branch_number    bn;
      };
      struct leaf_remove_range
      {
         const leaf_node& src;
         branch_number    lo;  ///< first branch to remove (inclusive)
         branch_number    hi;  ///< last branch to remove (exclusive)
      };
      struct leaf_prepend_prefix
      {
         const leaf_node& src;
         key_view         prefix;
      };
      struct leaf_from_visitor;
   }  // namespace op

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
      enum value_type_flag : uint16_t
      {
         null,
         inline_data,
         value_node,
         subtree
      };

      class entry_inserter
      {
         leaf_node& _leaf;
         uint16_t   _idx = 0;
        public:
         entry_inserter(leaf_node& l) : _leaf(l) {}
         void add(key_view key, value_type val);
         friend class leaf_node;
      };

      static constexpr uint32_t  max_leaf_size = 4096 / 2;
      static constexpr node_type type_id       = node_type::leaf;
      inline static uint32_t     alloc_size(key_view key, const value_type& value) noexcept;
      /// clone and optimize
      inline static uint32_t alloc_size(const leaf_node* clone) noexcept;
      inline static uint32_t alloc_size(const leaf_node* clone,
                                        key_view         cprefix,
                                        branch_number    start,
                                        branch_number    end)
      {
         assert(cprefix.size() <= 1024);
         return max_leaf_size;
      }
      inline static uint32_t alloc_size(const leaf_node* clone,
                                        branch_number    start,
                                        branch_number    end)
      {
         return max_leaf_size;
      }
      inline static uint32_t alloc_size(const leaf_node* src, const op::leaf_insert& ins)
      {
         return max_leaf_size;
      }
      inline static uint32_t alloc_size(const op::leaf_update& upd) { return max_leaf_size; }
      inline static uint32_t alloc_size(const op::leaf_remove& rm)
      {
         // no point in growing the node when we are removing a value
         return rm.src.size();
      }
      inline static uint32_t alloc_size(const op::leaf_remove_range& rm)
      {
         return rm.src.size();
      }
      inline static uint32_t alloc_size(const op::leaf_prepend_prefix&)
      {
         return max_leaf_size;
      }
      inline static uint32_t alloc_size(const struct op::leaf_from_visitor&) { return max_leaf_size; }

      leaf_node(size_t alloc_size, ptr_address_seq seq, const op::leaf_update& upd);
      leaf_node(size_t alloc_size, ptr_address_seq seq, const op::leaf_remove& rm);
      leaf_node(size_t alloc_size, ptr_address_seq seq, const op::leaf_remove_range& rm);
      leaf_node(size_t alloc_size, ptr_address_seq seq, const op::leaf_prepend_prefix& pp);
      leaf_node(size_t alloc_size, ptr_address_seq seq, const struct op::leaf_from_visitor& vis);

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
      leaf_node(size_t                 alloc_size,
                ptr_address_seq        seq,
                const leaf_node*       clone,
                const op::leaf_insert& ins);

      /// clone and remove bn
      leaf_node(size_t alloc_size, ptr_address_seq seq, const leaf_node* clone, branch_number bn);

      key_view get_common_prefix() const noexcept;

      /**
       *  A leaf can only have children on 16 unique cachelines, therefore,
       * we need to check if we have less than 16 or if addr is already on
       * an existing cacheline.
       */
      bool can_insert_address(ptr_address addr) const noexcept;

      void     dump() const;
      [[nodiscard]] bool validate_invariants() const noexcept;
      uint16_t alloc_pos() const noexcept { return _alloc_pos; }
      uint16_t dead_space() const noexcept { return _dead_space; }
      uint32_t clines_capacity() const noexcept { return _cline_cap; }
      bool     is_optimal_layout() const noexcept { return _optimal_layout; }

      uint32_t compact_size() const noexcept;
      void     compact_to(alloc_header* compact_dst) const noexcept;
      uint32_t cow_size() const noexcept { return max_leaf_size; }  // TODO: make this config const

      /// Release value_node and subtree addresses held by this leaf.
      void destroy(const sal::allocator_session_ptr& session) const noexcept
      {
         // TODO: restructure to avoid branch prediction miss per iteration
         // (e.g. gather address indices first, then release in a second pass)
         const value_branch* vb = value_offsets();
         for (uint32_t i = 0; i < num_branches(); ++i)
            if (vb[i].is_address())
               session->release(get_address(vb[i]));
      }

      /// Visit all child addresses (value_nodes and subtrees) held by this leaf.
      void visit_children(const std::function<void(sal::ptr_address)>& visitor) const noexcept
      {
         // TODO: restructure to avoid branch prediction miss per iteration
         const value_branch* vb = value_offsets();
         for (uint32_t i = 0; i < num_branches(); ++i)
            if (vb[i].is_address())
               visitor(get_address(vb[i]));
      }

      struct split_pos
      {
         key_view cprefix;           ///< common prefix of all keys
         uint8_t  divider;           ///< byte to split on
         uint32_t less_than_count;   ///< number of keys less than divider
         uint32_t greater_eq_count;  ///< number of keys greater than or equal to divider
         key_view divider_key() const noexcept { return key_view((const char*)&divider, 1); }
      };
      split_pos get_split_pos() const noexcept;

      PSITRI_NO_SANITIZE_ALIGNMENT key_view get_key(branch_number bn) const noexcept
      {
         assert(bn < num_branches());
         return get_key_ptr(keys_offsets()[*bn])->get();
      }
      PSITRI_NO_SANITIZE_ALIGNMENT value_type get_value(branch_number bn) const noexcept
      {
         assert(bn < num_branches());
         value_branch vb = value_offsets()[*bn];
         switch (vb.type())
         {
            case value_type_flag::subtree:
               return value_type::make_subtree(get_address(vb));
            case value_type_flag::value_node:
               return value_type::make_value_node(get_address(vb));
            case value_type_flag::inline_data:
               return get_value_ptr(vb.offset())->get();
            case value_type_flag::null:
               return value_type(value_view());
         }
         std::unreachable();
      }
      PSITRI_NO_SANITIZE_ALIGNMENT value_view get_value_view(branch_number bn) const noexcept
      {
         assert(value_offsets()[*bn].type() == value_type_flag::inline_data);
         return get_value_ptr(value_offsets()[*bn].offset())->get();
      }
      PSITRI_NO_SANITIZE_ALIGNMENT ptr_address get_value_address(branch_number bn) const noexcept
      {
         assert(value_offsets()[*bn].type() == value_type_flag::value_node);
         return get_address(value_offsets()[*bn]);
      }
      value_type_flag get_value_type(branch_number bn) const noexcept
      {
         return value_offsets()[*bn].type();
      }

      /// @return size of old value
      size_t update_value(branch_number bn, const value_type& value) noexcept;

      enum class can_apply_mode
      {
         none   = 0,
         modify = 1,
         defrag = 2
      };
      can_apply_mode can_apply(const op::leaf_insert& ins) const noexcept;
      can_apply_mode can_apply(const op::leaf_update& ins) const noexcept;
      //      can_apply_mode can_apply(const op::leaf_remove& ins) const noexcept;

      /// determines whether there is enough space to insert the key
      /// @return the amount of free space after inserting the key,
      /// this will be negative if there is not enough space.
      //int  can_insert(key_view key, const value_type& value) const noexcept;
      //bool can_insert_with_defrag(key_view key, const value_type& value) const noexcept;

      int free_space() const noexcept { return alloc_head() - meta_end(); }

      /// @return the branch number of the inserted key
      /// @pre key is not already in the node
      /// @pre can_insert(key) must be true
      /// @pre bn  == lower_bound(key)
      branch_number apply(const op::leaf_insert& ins) noexcept;
      void          apply(const op::leaf_remove& rm) noexcept;

      /// @pre bn != num_branches()
      void remove(branch_number bn) noexcept;

      /// Remove branches in range [lo, hi) in-place (unique mode).
      /// @pre lo < hi && hi <= num_branches()
      void remove_range(branch_number lo, branch_number hi) noexcept;

      uint8_t calc_key_hash(key_view key) const noexcept
      {
         return XXH3_64bits(key.data(), key.size());
      }

      /// Verify the stored key hash for branch bn matches computed hash.
      bool verify_key_hash(branch_number bn) const noexcept
      {
         auto key = get_key(bn);
         return key_hashs()[*bn] == calc_key_hash(key);
      }

      /// Value-level checksums have been removed (rely on node-level checksum).
      /// Always returns true for backward compat with verification code.
      bool verify_value_checksum(branch_number) const noexcept { return true; }

      /// Number of shared version table entries (0-31).
      uint8_t num_versions() const noexcept { return _num_versions; }

      /// Get the version index for a branch (0xFF = no version assigned).
      /// Returns 0xFF when _num_versions == 0 (ver_indices not allocated).
      uint8_t get_ver_index(branch_number bn) const noexcept
      {
         assert(bn < num_branches());
         if (_num_versions == 0)
            return 0xFF;
         return ver_indices()[*bn];
      }

      /// Set the version index for a branch.
      /// @pre _num_versions > 0 (ver_indices must be allocated)
      void set_ver_index(branch_number bn, uint8_t idx) noexcept
      {
         assert(bn < num_branches());
         assert(_num_versions > 0);
         assert(idx < _num_versions || idx == 0xFF);
         ver_indices()[*bn] = idx;
      }

      /// Get the version number for a branch via ver_indices → version_table.
      /// Returns 0 if no version assigned (0xFF index or _num_versions == 0).
      uint64_t get_version(branch_number bn) const noexcept
      {
         uint8_t idx = get_ver_index(bn);
         if (idx == 0xFF)
            return 0;
         assert(idx < _num_versions);
         return version_table()[idx].get();
      }

      /// Add a version to the shared table if not already present.
      /// Returns the table index. Deduplicates: if version already exists, returns existing index.
      /// @pre _num_versions < 31 (or version already exists)
      /// @pre caller must have ensured free_space() >= sizeof(version48) + num_branches()
      ///      (for first version, which allocates ver_indices + 1 version_table entry)
      uint8_t add_version(uint64_t version) noexcept
      {
         if (_num_versions == 0)
         {
            // First version: allocate ver_indices (nb bytes) + first version_table entry.
            // Caller must have verified free_space() is sufficient.
            init_ver_indices();
         }
         auto* vt = version_table();
         for (uint8_t i = 0; i < _num_versions; ++i)
            if (vt[i].get() == version)
               return i;
         assert(_num_versions < 31);
         uint8_t idx = _num_versions++;
         version_table()[idx].set(version);
         return idx;
      }

      /// uses hash to find key
      branch_number get(key_view key) const noexcept
      {
         //         SAL_INFO("get: key: {}", key);
         uint8_t        khash     = calc_key_hash(key);
         const uint8_t* khashes   = key_hashs().data();
         uint32_t       reamining = num_branches();

         /*
         for (uint32_t i = 0; i < reamining; ++i)
         {
            SAL_INFO("get_key({}) : hash: {} found: {}", i, int(khashes[i]),
                     get_key(branch_number(i)));
         }
*/

         uint32_t total_idx = 0;
         do
         {
            auto idx = find_byte(khashes, reamining, khash);
            //          SAL_INFO("get: idx: {} reamining: {}", idx, reamining);
            if (idx == reamining)
            {
               ////            SAL_ERROR("not found");
               return branch_number(num_branches());
            }
            total_idx += idx;
            ////       SAL_INFO("rel idx: {} get_key({}) : found: {}", idx, total_idx,
            //                get_key(branch_number(total_idx)));
            if (get_key(branch_number(total_idx)) == key)
               return branch_number(total_idx);
            ++idx;
            ++total_idx;
            khashes += idx;
            reamining -= idx;
         } while (true);
      }
      /// uses binary search to find first key >= search key
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
      /// uses binary search to find first key > search key
      branch_number upper_bound(key_view key) const noexcept
      {
         int                  pos[2];
         static constexpr int left_pos  = 0;
         static constexpr int right_pos = 1;
         pos[left_pos]                  = -1;
         pos[right_pos]                 = num_branches();
         while (pos[right_pos] - pos[left_pos] > 1)
         {
            int  middle = (pos[left_pos] + pos[right_pos]) >> 1;
            bool gt     = get_key(branch_number(middle)) > key;
            pos[gt]     = middle;
         }
         return branch_number(pos[right_pos]);
      }

      /// visit all branches that are ptr_address (value_node or subtree)
      void visit_branches(std::invocable<ptr_address> auto&& lam) const noexcept
      {
         const value_branch* cvb = value_offsets();
         const uint32_t      n   = num_branches();
         for (int i = 0; i < n; ++i)
         {
            if (cvb[i].is_address())
               lam(get_address(cvb[i]));
         }
      }

      std::span<const ptr_address> hint() const noexcept { return clines(); }

      // TODO: standardize this across inner nodes which clines returns const ptr_address*
      std::span<const ptr_address> clines() const noexcept
      {
         return std::span<const ptr_address>(
             reinterpret_cast<const ptr_address*>(value_offsets() + num_branches()), _cline_cap);
      }

      uint16_t num_branches() const noexcept { return _num_branches; }

      std::span<ptr_address> clines() noexcept
      {
         return std::span<ptr_address>(
             reinterpret_cast<ptr_address*>(value_offsets() + num_branches()), _cline_cap);
      }

      uint8_t find_cline_index(ptr_address addr) const noexcept
      {
         const ptr_address* clines    = this->clines().data();
         uint32_t           adr_cline = *addr & ~0x0ful;
         return ucc::find_u32x16((const uint32_t*)clines, _cline_cap, adr_cline);
      }

     private:
      void set_num_branches(uint16_t n) noexcept { _num_branches = n; }
      void clone_from(const leaf_node* clone);

      uint16_t _alloc_pos;
      uint16_t _dead_space;  // tracks freed data in alloc space
      uint32_t _cline_cap : 9;
      uint32_t _optimal_layout : 1;  ///< a bit that gets set when the node is optimized,
                                     /// cleared when optimized invariants broken
      uint32_t _num_branches : 9;
      uint32_t _num_versions : 5;    ///< number of shared version table entries (0-31)
      uint32_t _unused : 8;
      uint8_t  _key_hashs[/*num_branches()*/];
      // Dynamic arrays follow sequentially after _key_hashs:
      //   uint8_t      key_hash[num_branch]
      //   uint16_t     keys_offsets[num_branch]
      //   value_branch value_offsets[num_branch]
      //   ptr_address  _clines[_cline_cap]
      //   uint8_t      ver_indices[num_branch]   (0xFF = no version)
      //   version48    version_table[_num_versions]
      //   [free space]
      //   ← _alloc_pos (from tail)
      //   [alloc area: keys + values]

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
      PSITRI_NO_SANITIZE_ALIGNMENT std::span<key_offset> keys_offsets() noexcept
      {
         return std::span<key_offset>(
             reinterpret_cast<key_offset*>(key_hashs().data() + num_branches()), num_branches());
      }
      PSITRI_NO_SANITIZE_ALIGNMENT std::span<const key_offset> keys_offsets() const noexcept
      {
         return std::span<const key_offset>(
             reinterpret_cast<const key_offset*>(key_hashs().data() + num_branches()),
             num_branches());
      }

#if PSITRI_PLATFORM_OPTIMIZATIONS
      class __attribute__((packed)) value_branch
#else
      class value_branch
#endif
      {
        public:
         value_branch() noexcept : _type(null), _offset(0) {}
         value_branch(value_offset offset) noexcept : _type(inline_data), _offset(*offset) {}
         value_branch(value_type_flag t, cline_offset cl, cline_index idx) noexcept
             : _type(t), _offset(*cl << 4 | *idx & 0xF)
         {
            assert(t == value_type_flag::subtree or t == value_type_flag::value_node);
         }

         void clear() noexcept
         {
            _type   = value_type_flag::null;
            _offset = 0;
         }

         bool            is_null() const noexcept { return _type == null; }
         bool            is_inline() const noexcept { return _type == inline_data; }
         bool            is_address() const noexcept { return _type >= value_node; }
         value_type_flag type() const noexcept { return (value_type_flag)_type; }
         value_offset    offset() const noexcept
         {
            assert(type() == value_type_flag::inline_data);
            return value_offset(_offset);
         }
         void set_offset(value_offset off) noexcept
         {
            _type   = value_type_flag::inline_data;
            _offset = *off;
         }
         cline_offset cline() const noexcept
         {
            assert(type() == value_type_flag::subtree || type() == value_type_flag::value_node);
            return cline_offset(_offset >> 4);  // Get upper 10 bits
         }
         void set_cline_and_idx(cline_offset    cl,
                                cline_index     idx,
                                value_type_flag t = value_type_flag::subtree) noexcept
         {
            assert(t == value_type_flag::subtree or t == value_type_flag::value_node);
            _type   = t;
            _offset = (*cl << 4) | (*idx & 0xF);
         }
         cline_index cline_idx() const noexcept
         {
            assert(type() == value_type_flag::subtree or type() == value_type_flag::value_node);
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
                            const_cast<char*>(meta_end())};
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
      const char* clines_end() const noexcept
      {
         return (const char*)(value_offsets() + num_branches()) + _cline_cap * sizeof(ptr_address);
      }

      /// Per-branch version index — maps branch to version_table entry.
      /// 0xFF means no version assigned (default for COW-only mode).
      uint8_t* ver_indices() noexcept
      {
         return reinterpret_cast<uint8_t*>(
             const_cast<char*>(clines_end()));
      }
      const uint8_t* ver_indices() const noexcept
      {
         return reinterpret_cast<const uint8_t*>(clines_end());
      }

      /// Shared version table — up to 31 unique version48 entries.
      version48* version_table() noexcept
      {
         return reinterpret_cast<version48*>(ver_indices() + num_branches());
      }
      const version48* version_table() const noexcept
      {
         return reinterpret_cast<const version48*>(ver_indices() + num_branches());
      }

      /// End of all metadata (past version_table).
      /// When _num_versions == 0, ver_indices and version_table are not allocated
      /// — meta_end() == clines_end().  This avoids consuming nb bytes of free space
      /// in the common (COW-only) case.
      const char* meta_end() const noexcept
      {
         if (_num_versions == 0)
            return clines_end();
         return reinterpret_cast<const char*>(version_table() + _num_versions);
      }

      /// Initialize ver_indices to 0xFF for all branches.
      void init_ver_indices() noexcept
      {
         std::memset(ver_indices(), 0xFF, num_branches());
      }

      /// determine if addr is on an existing cline, or allocate a new one and
      /// return the value_branch for the new cline
      value_branch add_address_ptr(value_type_flag t, ptr_address addr) noexcept;

      /// remove the address ptr from the cline index, if there are no references
      void remove_address_ptr(cline_offset cl_off) noexcept;
      /// calculate the number of references to the cline
      int calc_cline_refs(cline_offset cl_off) const noexcept;

#if PSITRI_PLATFORM_OPTIMIZATIONS
      class __attribute__((packed)) key
#else
      class key
#endif
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
         uint16_t _size;
         uint8_t  _data[];

        public:
         void set(value_view value)
         {
            _size = value.size();
            std::memcpy(_data, value.data(), _size);
         }
         value_view get() const noexcept { return value_view((const char*)_data, _size); }
      };
      static_assert(sizeof(key) == 2);
      static_assert(sizeof(value_data) == 2);

      PSITRI_NO_SANITIZE_ALIGNMENT key* get_key_ptr(key_offset off) noexcept
      {
         return reinterpret_cast<key*>(((char*)tail()) - *off);
      }
      PSITRI_NO_SANITIZE_ALIGNMENT const key* get_key_ptr(key_offset off) const noexcept
      {
         return reinterpret_cast<const key*>(((const char*)tail()) - *off);
      }
      PSITRI_NO_SANITIZE_ALIGNMENT key_offset alloc_key(key_view key) noexcept
      {
         //       SAL_INFO("alloc pos: {} alloc_key: {}", _alloc_pos, key);
         _alloc_pos += key.size() + sizeof(leaf_node::key);
         key_offset off = key_offset(_alloc_pos);
         //       SAL_INFO("off: {} offhead: {}  val_off_end: {} size: {}", off, size() - *off,
         //               (uint8_t*)value_offsets_end() - (uint8_t*)this, size());
         //        SAL_WARN("space: {}", (uint8_t*)get_key_ptr(off) - (uint8_t*)value_offsets_end());
         assert((uint8_t*)get_key_ptr(off) >= (uint8_t*)value_offsets_end() &&
                "Allocation would overlap with value offsets");
         get_key_ptr(off)->set(key);
         return off;
      }
      PSITRI_NO_SANITIZE_ALIGNMENT value_data* get_value_ptr(value_offset off) noexcept
      {
         return reinterpret_cast<value_data*>(((char*)tail()) - *off);
      }
      PSITRI_NO_SANITIZE_ALIGNMENT const value_data* get_value_ptr(value_offset off) const noexcept
      {
         auto ptr = reinterpret_cast<const value_data*>(((const char*)tail()) - *off);
         return ptr;
      }
      PSITRI_NO_SANITIZE_ALIGNMENT value_offset alloc_value(value_view value) noexcept
      {
         _alloc_pos += value.size() + sizeof(value_data);
         value_offset off = value_offset(_alloc_pos);
         //         SAL_WARN("space: {}", (uint8_t*)get_value_ptr(off) - (uint8_t*)value_offsets_end());
         assert((uint8_t*)get_value_ptr(off) >= (uint8_t*)value_offsets_end() &&
                "Allocation would overlap with value offsets");
         get_value_ptr(off)->set(value);
         return off;
      }
      PSITRI_NO_SANITIZE_ALIGNMENT bool can_alloc_key(key_view key) const noexcept
      {
         return (const uint8_t*)get_key_ptr(key_offset(_alloc_pos + key.size() + sizeof(key))) <
                (const uint8_t*)value_offsets_end();
      }
      PSITRI_NO_SANITIZE_ALIGNMENT bool can_alloc_value(value_view value) const noexcept
      {
         return (const uint8_t*)get_value_ptr(
                    value_offset(_alloc_pos + value.size() + sizeof(value_data))) <
                (const uint8_t*)value_offsets_end();
      }
      PSITRI_NO_SANITIZE_ALIGNMENT void free_key(key_offset off) noexcept
      {
         _dead_space += sizeof(key) + get_key_ptr(off)->get().size();
      }
      PSITRI_NO_SANITIZE_ALIGNMENT void free_value(value_offset off) noexcept
      {
         _dead_space += sizeof(value_data) + get_value_ptr(off)->get().size();
      }
      const char* alloc_head() const noexcept { return (const char*)tail() - _alloc_pos; }
      char*       alloc_head() noexcept { return (char*)tail() - _alloc_pos; }

   } __attribute__((packed));
   static_assert(sizeof(leaf_node) == 20);

   inline uint32_t leaf_node::alloc_size(key_view key, const value_type& value) noexcept
   {
      return max_leaf_size;  //ucc::round_up_multiple<64>(sizeof(leaf_node) + key.size() + value.size());
   }
   /// clone and optimize
   inline uint32_t leaf_node::alloc_size(const leaf_node* clone) noexcept
   {
      return clone->size();
   }

   namespace op
   {
      struct leaf_from_visitor
      {
         using init_fn = void (*)(leaf_node::entry_inserter&, void*);
         init_fn  init;
         void*    ctx;
         uint16_t count;
      };
   }  // namespace op

   template <typename T>
   concept is_leaf_node = std::same_as<std::remove_cvref_t<std::remove_pointer_t<T>>, leaf_node>;
}  // namespace psitri