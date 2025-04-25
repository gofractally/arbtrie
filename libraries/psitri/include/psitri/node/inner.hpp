#pragma once
#include <cassert>
#include <cstdint>
#include <psitri/node/inner_base.hpp>
#include <psitri/util.hpp>
#include <sal/numbers.hpp>
#include <ucc/lower_bound.hpp>

namespace psitri
{

   class inner_prefix_node : public inner_node_base<inner_prefix_node>
   {
     public:
      static constexpr node_type type_id = node_type::inner_prefix;
      friend class inner_node_base<inner_prefix_node>;

      static inline uint32_t alloc_size(key_view                      prefix,
                                        const branch_set&             branches,
                                        int                           numcline,
                                        const std::array<uint8_t, 8>& cline_indices) noexcept
      {
         return ucc::round_up_multiple<64>(sizeof(inner_prefix_node) + prefix.size() +
                                           2 * branches.count() - 1 +
                                           numcline * sizeof(cline_data));
      }
      static inline uint32_t alloc_size(const any_inner_node_type auto* clone,
                                        key_view                        prefix) noexcept
      {
         return ucc::round_up_multiple<64>(clone->size() - clone->prefix_capacity() +
                                           prefix.size());
      }

      static inline uint32_t alloc_size(key_view                  prefix,
                                        const inner_prefix_node*  clone,
                                        const op::replace_branch& update) noexcept
      {
         auto new_branches = clone->_num_branches + update.sub_branches.count() - 1;
         return ucc::round_up_multiple<64>(sizeof(inner_prefix_node) + prefix.size() +
                                           2 * new_branches - 1 +
                                           update.needed_clines * sizeof(cline_data));
      }
      static inline uint32_t alloc_size(const any_inner_node_type auto* clone,
                                        key_view                        prefix,
                                        subrange                        range,
                                        const cline_freq_table&         ftab) noexcept
      {
         return ucc::round_up_multiple<64>(sizeof(inner_prefix_node) + prefix.size() +
                                           2 * (*range.end - *range.begin) - 1 +
                                           ftab.compressed_clines() * sizeof(cline_data));
      }

      inner_prefix_node(uint32_t                      asize,
                        ptr_address_seq               seq,
                        key_view                      prefix,
                        const branch_set&             branches,
                        int                           numcline,
                        const std::array<uint8_t, 8>& cline_indices) noexcept
          : inner_node_base(asize, node_type::inner_prefix, seq)
      {
         assert(asize == alloc_size(prefix, branches, numcline, cline_indices));
         // set cap once so offsets are valid
         _prefix_cap = prefix.size();
         init(branches, numcline, cline_indices);
         set_prefix(prefix);
      }

      inner_prefix_node(uint32_t                        asize,
                        ptr_address_seq                 seq,
                        const any_inner_node_type auto* clone,
                        key_view                        prefix,
                        subrange                        range,
                        const cline_freq_table&         ftab) noexcept
          : inner_node_base(asize, node_type::inner_prefix, seq)
      {
         _prefix_cap = prefix.size();
         init(asize, seq, clone, range, ftab);
         _prefix_cap = prefix.size();
         set_prefix(prefix);
      }

      inner_prefix_node(uint32_t                  asize,
                        ptr_address_seq           seq,
                        key_view                  prefix,
                        const inner_prefix_node*  clone,
                        const op::replace_branch& update) noexcept
          : inner_node_base(asize, node_type::inner_prefix, seq)
      {
         assert(asize == alloc_size(prefix, clone, update));
         // set cap once so offsets are valid
         _prefix_cap = prefix.size();
         init(clone, update);
         set_prefix(prefix);
      }
      inner_prefix_node(uint32_t                        asize,
                        ptr_address_seq                 seq,
                        const any_inner_node_type auto* clone,
                        key_view                        prefix) noexcept
          : inner_node_base(asize, node_type::inner_prefix, seq)
      {
         assert(asize == alloc_size(clone, prefix));
         _num_branches = clone->_num_branches;
         _num_cline    = clone->_num_cline;
         _prefix_cap   = prefix.size();
         set_prefix(prefix);
         memcpy(divisions(), clone->divisions(), clone->num_branches() - 1);
         memcpy(branches(), clone->const_branches(), clone->num_branches());
         memcpy(clines(), clone->clines(), clone->_num_cline * sizeof(ptr_address));
      }

      uint16_t       prefix_len() const noexcept { return _prefix_len; }
      uint16_t       prefix_capacity() const noexcept { return _prefix_cap; }
      key_view       prefix() const noexcept { return key_view((const char*)_prefix, _prefix_len); }
      uint8_t*       divisions() noexcept { return _prefix + _prefix_cap; }
      const uint8_t* divisions() const noexcept { return _prefix + _prefix_cap; }
      branch*        branches() noexcept { return (branch*)(divisions() + num_divisions()); }
      branch*        branches_end() noexcept { return branches() + num_branches(); }
      const branch*  branches_end() const noexcept
      {
         return const_cast<inner_prefix_node*>(this)->branches() + num_branches();
      }
      const branch* const_branches() const noexcept
      {
         return (const branch*)(divisions() + num_divisions());
      }
      uint16_t num_branches() const noexcept { return _num_branches; }

      void set_prefix(key_view pre) noexcept
      {
         assert(pre.size() <= prefix_capacity());
         _prefix_len = pre.size();
         memcpy(_prefix, pre.data(), pre.size());
      }
      branch_number lower_bound(key_view key) const noexcept
      {
         if (not key.size())
            return branch_number(0);
         return inner_node_base<inner_prefix_node>::lower_bound(key[0]);
      }
      [[nodiscard]] inline bool can_apply(const op::replace_branch& up) const noexcept
      {
         return alloc_size(prefix(), this, up) == size();
      }

      using inner_node_base<inner_prefix_node>::destroy;

     protected:
      static int reg_type;  // =sal::register_type_vtable<inner_prefix_node>();

      template <typename T>
      friend class inner_node_base;
      uint64_t _descendents : 39;  ///< 500 billion keys max
      uint64_t _num_branches : 9;  ///< a maximum of 256 branches per node (16 clines * 16 indices)
      uint64_t _num_cline : 5;     ///< only 16 cline are possible w/ 4 bit branch index
      uint64_t _prefix_len : 11;   ///< prefix length in bytes
      uint16_t _prefix_cap : 11;   ///< prefix capacity in bytes
      uint16_t _unused : 5;  ///< perhaps store used_cline (_num_cline-used_cline = free_clines)
      uint8_t  _prefix[/*prefix_len*/];  ///< prefix length in bytes
      /** 
          uint8_t  _divisions[ num_branches - 1 ];  
          branch branches[_numbranches]  - 
          -- -a-- - | ... spare space... 
          ptr_address clines[_numcline] - 64 byte aligned at end of object
          tail()
      */
   } __attribute__((packed));

   /**
    *  This node does not consume part of the key when traversing it,
    *  but instead operates like a b+tree, it only consumes the prefix
    *  and then subdivides the key space.
    *
    *  This has the space effeciency of a setlist node and can consider itself
    *  full when it gets to 16 clines... though it would get messy by forcing
    *  a refactor simply because a child node changed address after an update
    *  from shared state.
    *
    *  Unlike the ARBTRIE above... we only have 1 inner_node node type
    */
   class inner_node : public inner_node_base<inner_node>
   {
     public:
      template <typename T>
      friend class inner_node_base;

      static constexpr node_type type_id = node_type::inner;
      friend class inner_node_base<inner_node>;
      //      inner_node(uint32_t asize, ptr_address_seq seq, branch_set branches) noexcept;

      inner_node(uint32_t                      asize,
                 ptr_address_seq               seq,
                 const branch_set&             branches,
                 int                           numcline,
                 const std::array<uint8_t, 8>& cline_indices) noexcept;

      inner_node(uint32_t                        asize,
                 ptr_address_seq                 seq,
                 const any_inner_node_type auto* clone,
                 subrange                        range,
                 const cline_freq_table&         ftab) noexcept;

      inner_node(uint32_t                        asize,
                 ptr_address_seq                 seq,
                 const any_inner_node_type auto* clone,
                 const op::replace_branch&       update) noexcept;

      /**
       * Calculate the size of an inner_node object, the first param is always null and used for
       * type-based dispatch from sal::allocator_session::alloc and the remaining params match the
       * constructor parameters after the asize and ptr_address_seq which are always present. The
       * return value will be passed to the constructor and the constructor will assert that the size
       * passed matches the size returned by these methods.
       */
      ///@{
      inline static uint32_t alloc_size(const branch_set&             branches,
                                        int                           numcline,
                                        const std::array<uint8_t, 8>& cline_indices) noexcept;
      inline static uint32_t alloc_size(const any_inner_node_type auto* clone,
                                        subrange                        range,
                                        const cline_freq_table&         ftab) noexcept;
      inline static uint32_t alloc_size(const any_inner_node_type auto* clone,
                                        const op::replace_branch&       update) noexcept;
      ///@}

      branch_number lower_bound(key_view key) const noexcept
      {
         if (key.size() == 0) [[unlikely]]
            return branch_number(0);
         return inner_node_base<inner_node>::lower_bound(key[0]);
      }

      void remove_branch(branch_number)
      {
         // if this is the only branch ref a cline,
         // then set the cline to null.
         // shift all branches after bn left 1 byte
         // shift all dividers after bn-1 left 1 byte
      }
      uint16_t num_branches() const noexcept { return _num_branches; }

      const branch* const_branches() const noexcept
      {
         return (const branch*)(divisions() + num_divisions());
      }

      using inner_node_base<inner_node>::destroy;

     protected:
      branch* branches() noexcept { return (branch*)(divisions() + num_divisions()); }

      const uint8_t* divisions() const noexcept { return _divisions; }
      uint8_t*       divisions() noexcept { return _divisions; }
      const branch*  branches_end() const noexcept { return const_branches() + num_branches(); }
      branch*        branches_end() noexcept { return branches() + num_branches(); }

     private:
      uint64_t _descendents : 39;  // 500 billion keys max
      uint64_t _num_branches : 9;  // a maximum of 256 branches per node (16 clines * 16 indices)
      uint64_t _num_cline : 5;     /// only 16 cline are possible w/ 4 bit branch index
      uint64_t _unused : 11;       /// maybe store used_cline (_num_cline-used_cline = free_clines)
      uint8_t  _divisions[/* num_branches - 1 */];  //< offset 20 bytes, which is 4 byte algined
      /** 
          branch branches[_numbranches]  - 
          -- -a-- - | ... spare space... 
          ptr_address clines[_numcline] - 64 byte aligned at end of object
          tail()
      */
   } __attribute__((packed));  // class inner_node
   /// if this is not 20, then it will impact alignment of branches
   static constexpr uint32_t inner_node_size = 20;
   static_assert(sizeof(inner_node) == inner_node_size);

   inline uint32_t inner_node::alloc_size(const branch_set&             branches,
                                          int                           numcline,
                                          const std::array<uint8_t, 8>& cline_indices) noexcept
   {
      auto divider_capacity = ucc::round_up_multiple<1>(branches.count() - 1);
      return ucc::round_up_multiple<64>(inner_node_size + numcline * sizeof(ptr_address) +
                                        divider_capacity + branches.count());
   }
   inline uint32_t inner_node::alloc_size(const any_inner_node_type auto* clone,
                                          const op::replace_branch&       update) noexcept
   {
      auto new_branches_count = clone->num_branches() + update.sub_branches.count() - 1;
      auto divider_capacity   = new_branches_count - 1;
      return ucc::round_up_multiple<64>(inner_node_size +
                                        update.needed_clines * sizeof(ptr_address) +
                                        divider_capacity + new_branches_count);
   }

   inline inner_node::inner_node(uint32_t                      asize,
                                 ptr_address_seq               seq,
                                 const branch_set&             init_branches,
                                 int                           numcline,
                                 const std::array<uint8_t, 8>& cline_indices) noexcept
       : inner_node_base(asize, node_type::inner, seq)
   {
      assert(asize == alloc_size(init_branches, numcline, cline_indices));
      init(init_branches, numcline, cline_indices);
   }

   inline inner_node::inner_node(uint32_t                        asize,
                                 ptr_address_seq                 seq,
                                 const any_inner_node_type auto* clone,
                                 const op::replace_branch&       update) noexcept
       : inner_node_base(asize, node_type::inner, seq)
   {
      assert(asize == alloc_size(clone, update));
      init(clone, update);
   }

   /**
    * Calculate the size of a new inner_node that is a subrange of the existing node.
    * 
    * To determine the size of the new node we need to know how many clines are required
    * for the branches in the subrange. This is calculated by passing the branches to the
    * method create_cline_freq_table( begin, end );
    *
    * The freq table cannot be calculated within this method because the constructor also
    * needs the calculated data to initialize the new node and it is expensive to calculate
    * twice.
    * @param clone - the node to copy branches from.
    * @param ftab the result of create_cline_freq_table( begin, end )
    */
   inline uint32_t inner_node::alloc_size(const any_inner_node_type auto* clone,
                                          subrange                        range,
                                          const cline_freq_table&         ftab) noexcept
   {
      auto new_branches_count = *range.end - *range.begin;
      auto divider_capacity   = ucc::round_up_multiple<1>(new_branches_count - 1);
      auto needed_clines      = ftab.compressed_clines();
      return ucc::round_up_multiple<64>(inner_node_size + needed_clines * sizeof(ptr_address) +
                                        divider_capacity + new_branches_count);
   }

   /**
    * Presumably this is being called because @param clone has 16 clines and the
    * we need to split it into 2 nodes with the hope of reducing the number of clines.
    * 
    * Even if this produced exactly 8 clines, it is not gauranteed that they will be 
    * consecutive, and thus this node may need space for 16 clines even though only
    * 8 are used and only 1 happens to be in line 15 of the new node. 
    * 
    * To compress down the node and save up to 32 bytes we need to remap the branches
    * to the clines, but first we must identify the minimum set of clines for the 
    * new node. 
    */
   inline inner_node::inner_node(uint32_t                        asize,
                                 ptr_address_seq                 seq,
                                 const any_inner_node_type auto* clone,
                                 subrange                        range,
                                 const cline_freq_table&         ftab) noexcept
       : inner_node_base(asize, node_type::inner, seq)
   {
      assert(asize == alloc_size(clone, range, ftab));
      init(asize, seq, clone, range, ftab);
   }

}  // namespace psitri
