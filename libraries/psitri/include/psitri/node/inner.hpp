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
                                        const std::array<uint8_t, 8>& cline_indices,
                                        uint64_t                      = 0) noexcept
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
                                        const cline_freq_table&         ftab,
                                        uint64_t                        = 0) noexcept
      {
         return ucc::round_up_multiple<64>(sizeof(inner_prefix_node) + prefix.size() +
                                           2 * (*range.end - *range.begin) - 1 +
                                           ftab.compressed_clines() * sizeof(cline_data));
      }
      static inline uint32_t alloc_size(key_view                        prefix,
                                        const any_inner_node_type auto* clone,
                                        const op::inner_remove_branch&) noexcept
      {
         auto new_branches = clone->num_branches() - 1;
         return ucc::round_up_multiple<64>(sizeof(inner_prefix_node) + prefix.size() +
                                           2 * new_branches - 1 +
                                           clone->num_clines() * sizeof(cline_data));
      }
      static inline uint32_t alloc_size(key_view                        prefix,
                                        const any_inner_node_type auto* clone,
                                        const op::inner_remove_range& rm) noexcept
      {
         auto new_branches = clone->num_branches() - (*rm.hi - *rm.lo);
         return ucc::round_up_multiple<64>(sizeof(inner_prefix_node) + prefix.size() +
                                           2 * new_branches - 1 +
                                           clone->num_clines() * sizeof(cline_data));
      }

      inner_prefix_node(uint32_t                      asize,
                        ptr_address_seq               seq,
                        key_view                      prefix,
                        const branch_set&             branches,
                        int                           numcline,
                        const std::array<uint8_t, 8>& cline_indices,
                        uint64_t                      epoch) noexcept
          : inner_node_base(asize, node_type::inner_prefix, seq)
      {
         assert(asize == alloc_size(prefix, branches, numcline, cline_indices, epoch));
         // set cap once so offsets are valid
         _prefix_cap = prefix.size();
         init(branches, numcline, cline_indices, epoch);
         set_prefix(prefix);
      }

      inner_prefix_node(uint32_t                        asize,
                        ptr_address_seq                 seq,
                        const any_inner_node_type auto* clone,
                        key_view                        prefix,
                        subrange                        range,
                        const cline_freq_table&         ftab,
                        uint64_t                        epoch) noexcept
          : inner_node_base(asize, node_type::inner_prefix, seq)
      {
         _prefix_cap = prefix.size();
         init(asize, seq, clone, range, ftab, epoch);
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
                        key_view                        prefix,
                        const any_inner_node_type auto* clone,
                        const op::inner_remove_branch&  rm) noexcept
          : inner_node_base(asize, node_type::inner_prefix, seq)
      {
         assert(asize == alloc_size(prefix, clone, rm));
         _prefix_cap = prefix.size();
         init(clone, rm);
         set_prefix(prefix);
      }
      inner_prefix_node(uint32_t                        asize,
                        ptr_address_seq                 seq,
                        key_view                        prefix,
                        const any_inner_node_type auto* clone,
                        const op::inner_remove_range&   rm) noexcept
          : inner_node_base(asize, node_type::inner_prefix, seq)
      {
         assert(asize == alloc_size(prefix, clone, rm));
         _prefix_cap = prefix.size();
         init(clone, rm);
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
         _last_unique_version        = clone->_last_unique_version;
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
      uint64_t last_unique_version() const noexcept { return _last_unique_version; }
      void     set_last_unique_version(uint64_t e) noexcept
      {
         _last_unique_version = version_token(e, last_unique_version_bits);
      }

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
      static int reg_type;

      template <typename T>
      friend class inner_node_base;
      uint64_t _last_unique_version : 39;  ///< root version when this node was last refreshed
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
                 const std::array<uint8_t, 8>& cline_indices,
                 uint64_t                      epoch) noexcept;

      inner_node(uint32_t                        asize,
                 ptr_address_seq                 seq,
                 const any_inner_node_type auto* clone,
                 subrange                        range,
                 const cline_freq_table&         ftab,
                 uint64_t                        epoch) noexcept;

      inner_node(uint32_t                        asize,
                 ptr_address_seq                 seq,
                 const any_inner_node_type auto* clone,
                 const op::replace_branch&       update) noexcept;

      inner_node(uint32_t                        asize,
                 ptr_address_seq                 seq,
                 const any_inner_node_type auto* clone,
                 const op::inner_remove_branch&  rm) noexcept;

      inner_node(uint32_t                        asize,
                 ptr_address_seq                 seq,
                 const any_inner_node_type auto* clone,
                 const op::inner_remove_range&   rm) noexcept;

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
                                        const std::array<uint8_t, 8>& cline_indices,
                                        uint64_t                      epoch = 0) noexcept;
      inline static uint32_t alloc_size(const any_inner_node_type auto* clone,
                                        subrange                        range,
                                        const cline_freq_table&         ftab,
                                        uint64_t                        epoch = 0) noexcept;
      inline static uint32_t alloc_size(const any_inner_node_type auto* clone,
                                        const op::replace_branch&       update) noexcept;
      inline static uint32_t alloc_size(const any_inner_node_type auto* clone,
                                        const op::inner_remove_branch&  rm) noexcept;
      inline static uint32_t alloc_size(const any_inner_node_type auto* clone,
                                        const op::inner_remove_range&   rm) noexcept;
      ///@}

      branch_number lower_bound(key_view key) const noexcept
      {
         if (key.size() == 0) [[unlikely]]
            return branch_number(0);
         return inner_node_base<inner_node>::lower_bound(key[0]);
      }

      using inner_node_base<inner_node>::remove_branch;
      uint16_t num_branches() const noexcept { return _num_branches; }
      uint64_t last_unique_version() const noexcept { return _last_unique_version; }
      void     set_last_unique_version(uint64_t e) noexcept
      {
         _last_unique_version = version_token(e, last_unique_version_bits);
      }

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
      uint64_t _last_unique_version : 39;  // root version when this node was last refreshed
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

   class wide_inner_node : public node
   {
     public:
      static constexpr node_type type_id = node_type::wide_inner;

      static uint32_t alloc_size(const op::inner_build_plan& plan) noexcept
      {
         assert(plan.num_branches > 0);
         assert(plan.num_branches <= op::inner_build_plan::max_branches);
         assert(plan.num_clines <= plan.num_branches);
         return ucc::round_up_multiple<64>(
             sizeof(wide_inner_node) + plan.prefix.size() + (plan.num_branches - 1) +
             plan.num_branches * sizeof(wide_branch) + plan.num_clines * sizeof(cline_data));
      }
      static uint32_t alloc_size(const wide_inner_node* clone,
                                 const op::inner_remove_branch&) noexcept
      {
         assert(clone->num_branches() > 1);
         return ucc::round_up_multiple<64>(
             sizeof(wide_inner_node) + clone->prefix().size() + (clone->num_branches() - 2) +
             (clone->num_branches() - 1) * sizeof(wide_branch) +
             clone->num_clines() * sizeof(cline_data));
      }
      static uint32_t alloc_size(const wide_inner_node* clone,
                                 const op::inner_remove_range& rm) noexcept
      {
         const auto removed = *rm.hi - *rm.lo;
         assert(removed > 0 && removed < clone->num_branches());
         const auto new_branches = clone->num_branches() - removed;
         return ucc::round_up_multiple<64>(sizeof(wide_inner_node) + clone->prefix().size() +
                                           (new_branches - 1) +
                                           new_branches * sizeof(wide_branch) +
                                           clone->num_clines() * sizeof(cline_data));
      }

      wide_inner_node(uint32_t asize, ptr_address_seq seq,
                      const op::inner_build_plan& plan) noexcept
          : node(asize, type_id, seq)
      {
         init_from_plan(plan, 0);
      }

      wide_inner_node(uint32_t asize, ptr_address_seq seq, const wide_inner_node* clone,
                      const op::inner_remove_branch& rm) noexcept
          : node(asize, type_id, seq)
      {
         clone_without_range(clone, rm.br, branch_number(*rm.br + 1));
      }

      wide_inner_node(uint32_t asize, ptr_address_seq seq, const wide_inner_node* clone,
                      const op::inner_remove_range& rm) noexcept
          : node(asize, type_id, seq)
      {
         clone_without_range(clone, rm.lo, rm.hi);
      }

      uint16_t num_branches() const noexcept { return _num_branches; }
      uint16_t num_clines() const noexcept { return _num_cline; }
      uint64_t last_unique_version() const noexcept { return _last_unique_version; }
      void     set_last_unique_version(uint64_t e) noexcept
      {
         _last_unique_version = version_token(e, last_unique_version_bits);
      }

      uint16_t prefix_len() const noexcept { return _prefix_len; }
      uint16_t prefix_capacity() const noexcept { return _prefix_cap; }
      key_view prefix() const noexcept { return key_view((const char*)_prefix, _prefix_len); }

      uint8_t*       divisions() noexcept { return _prefix + _prefix_cap; }
      const uint8_t* divisions() const noexcept { return _prefix + _prefix_cap; }
      uint32_t       num_divisions() const noexcept { return _num_branches - 1; }
      key_view       divs() const noexcept
      {
         return key_view((const char*)divisions(), num_divisions());
      }

      wide_branch* branches() noexcept
      {
         return reinterpret_cast<wide_branch*>(divisions() + num_divisions());
      }
      const wide_branch* const_branches() const noexcept
      {
         return reinterpret_cast<const wide_branch*>(divisions() + num_divisions());
      }
      wide_branch* branches_end() noexcept { return branches() + num_branches(); }
      const wide_branch* branches_end() const noexcept { return const_branches() + num_branches(); }

      ptr_address* clines() noexcept
      {
         return reinterpret_cast<ptr_address*>(tail()) - num_clines();
      }
      const ptr_address* clines() const noexcept
      {
         return reinterpret_cast<const ptr_address*>(tail()) - num_clines();
      }
      cline_data* get_cline_data() noexcept { return reinterpret_cast<cline_data*>(clines()); }
      const cline_data* get_cline_data() const noexcept
      {
         return reinterpret_cast<const cline_data*>(clines());
      }

      std::span<const ptr_address> get_branch_clines() const noexcept
      {
         return {clines(), num_clines()};
      }
      sal::alloc_hint hint() const noexcept { return sal::alloc_hint(clines(), num_clines()); }

      branch_number lower_bound(key_view key) const noexcept
      {
         if (key.empty())
            return branch_number(0);
         return lower_bound(key[0]);
      }
      branch_number lower_bound(uint8_t byte) const noexcept
      {
         const uint16_t nd = num_divisions();
         if (nd == 0)
            return branch_number(0);
         uint16_t       lb = ucc::lower_bound_padded(divisions(), nd, byte);
         lb += (byte >= divisions()[lb]) * (lb < nd);
         return branch_number(lb);
      }

      ptr_address get_branch(branch_number n) const noexcept
      {
         auto br = const_branches()[*n];
         assert(br.line() < num_clines());
         auto cd = get_cline_data();
         return ptr_address(*cd[br.line()].base() + br.index());
      }

      void visit_branches(std::invocable<ptr_address> auto&& lam) const noexcept
      {
         for (uint16_t i = 0; i < num_branches(); ++i)
            lam(get_branch(branch_number(i)));
      }
      void visit_children(const std::function<void(sal::ptr_address)>& visitor) const noexcept
      {
         visit_branches([&](ptr_address addr) { visitor(addr); });
      }
      void destroy(const sal::allocator_session_ptr& session) const noexcept
      {
         visit_branches([&](ptr_address addr) { session->release(addr); });
      }

      [[nodiscard]] bool can_apply(const op::replace_branch&) const noexcept { return false; }
      void               apply(const op::replace_branch&) noexcept { assert(false); }

      void remove_branch(branch_number bn) noexcept
      {
         assert(*bn < _num_branches);
         assert(_num_branches > 1);
         get_cline_data()[branches()[*bn].line()].dec_ref();
         remove_divisions_and_branches(bn, branch_number(*bn + 1));
      }

      void remove_range(branch_number lo, branch_number hi) noexcept
      {
         assert(*lo < *hi && *hi <= _num_branches);
         assert((*hi - *lo) < _num_branches);
         for (uint16_t i = *lo; i < *hi; ++i)
            get_cline_data()[branches()[i].line()].dec_ref();
         remove_divisions_and_branches(lo, hi);
      }

      [[nodiscard]] bool validate_invariants() const noexcept
      {
         if (num_branches() == 0 || num_branches() > op::inner_build_plan::max_branches)
            return false;
         if (!std::is_sorted(divisions(), divisions() + num_divisions()))
            return false;
         if ((const uint8_t*)branches_end() > (const uint8_t*)clines())
            return false;
         std::array<uint8_t, 256> actual_refs{};
         for (uint16_t i = 0; i < num_branches(); ++i)
         {
            auto br = const_branches()[i];
            if (br.line() >= num_clines())
               return false;
            ++actual_refs[br.line()];
            if (get_branch(branch_number(i)) == sal::null_ptr_address)
               return false;
         }
         auto cd = get_cline_data();
         for (uint16_t i = 0; i < num_clines(); ++i)
         {
            if (cd[i].is_null())
            {
               if (actual_refs[i] != 0)
                  return false;
            }
            else if (cd[i].ref() != actual_refs[i])
               return false;
         }
         return true;
      }

     private:
      void init_from_plan(const op::inner_build_plan& plan, uint64_t epoch) noexcept
      {
         _last_unique_version = version_token(epoch, last_unique_version_bits);
         _num_branches        = plan.num_branches;
         _num_cline           = plan.num_clines;
         _prefix_len          = plan.prefix.size();
         _prefix_cap          = plan.prefix.size();
         std::memcpy(_prefix, plan.prefix.data(), plan.prefix.size());
         std::memcpy(divisions(), plan.dividers.data(), num_divisions());
         auto* brs = branches();
         for (uint16_t i = 0; i < num_branches(); ++i)
         {
            uint16_t line = plan.cline_index(plan.branches[i]);
            brs[i].set_line_index(line, *plan.branches[i] & 0x0f);
         }
         auto* cds = get_cline_data();
         for (uint16_t i = 0; i < num_clines(); ++i)
         {
            cds[i].data = *plan.clines[i];
            cds[i].set_ref(plan.cline_refs[i]);
         }
         assert(validate_invariants());
      }

      void clone_without_range(const wide_inner_node* clone,
                               branch_number          lo,
                               branch_number          hi) noexcept
      {
         _last_unique_version = clone->_last_unique_version;
         _prefix_len          = clone->_prefix_len;
         _prefix_cap          = clone->_prefix_len;
         std::memcpy(_prefix, clone->_prefix, _prefix_len);
         _num_cline = clone->_num_cline;
         std::memcpy(clines(), clone->clines(), _num_cline * sizeof(cline_data));
         _num_branches = clone->_num_branches - (*hi - *lo);

         uint16_t out = 0;
         for (uint16_t i = 0; i < clone->_num_branches; ++i)
         {
            if (i >= *lo && i < *hi)
               continue;
            if (out > 0)
            {
               uint16_t div_idx = (i == 0) ? 0 : i - 1;
               if (i == *hi && *lo > 0)
                  div_idx = *hi - 1;
               divisions()[out - 1] = clone->divisions()[div_idx];
            }
            branches()[out++] = clone->const_branches()[i];
         }
         assert(out == _num_branches);
         assert(validate_invariants());
      }

      void remove_divisions_and_branches(branch_number lo, branch_number hi) noexcept
      {
         const uint16_t old_nb = _num_branches;
         const uint16_t remove_count = *hi - *lo;
         const wide_branch* old_branches = const_branches();

         std::array<wide_branch, 256> kept{};
         uint16_t out = 0;
         for (uint16_t i = 0; i < old_nb; ++i)
            if (i < *lo || i >= *hi)
               kept[out++] = old_branches[i];

         if (*lo == 0)
            memmove(divisions(), divisions() + *hi, (old_nb - 1) - *hi);
         else
         {
            uint32_t head       = *lo - 1;
            uint32_t tail_start = *hi - 1;
            memmove(divisions() + head, divisions() + tail_start, (old_nb - 1) - tail_start);
         }

         _num_branches = old_nb - remove_count;
         std::memcpy(branches(), kept.data(), _num_branches * sizeof(wide_branch));
         assert(validate_invariants());
      }

      uint64_t _last_unique_version = 0;
      uint16_t _num_branches        = 0;
      uint16_t _num_cline           = 0;
      uint16_t _prefix_len          = 0;
      uint16_t _prefix_cap          = 0;
      uint8_t  _prefix[/*prefix_cap*/];
   } __attribute__((packed));

   class direct_inner_node : public node
   {
     public:
      static constexpr node_type type_id = node_type::direct_inner;

      static uint32_t alloc_size(const op::inner_build_plan& plan) noexcept
      {
         assert(plan.num_branches > 0);
         assert(plan.num_branches <= op::inner_build_plan::max_branches);
         return ucc::round_up_multiple<64>(sizeof(direct_inner_node) + plan.prefix.size() +
                                           (plan.num_branches - 1) +
                                           plan.num_branches * sizeof(ptr_address));
      }
      static uint32_t alloc_size(const direct_inner_node* clone,
                                 const op::inner_remove_branch&) noexcept
      {
         assert(clone->num_branches() > 1);
         return ucc::round_up_multiple<64>(sizeof(direct_inner_node) + clone->prefix().size() +
                                           (clone->num_branches() - 2) +
                                           (clone->num_branches() - 1) * sizeof(ptr_address));
      }
      static uint32_t alloc_size(const direct_inner_node* clone,
                                 const op::inner_remove_range& rm) noexcept
      {
         const auto removed = *rm.hi - *rm.lo;
         assert(removed > 0 && removed < clone->num_branches());
         const auto new_branches = clone->num_branches() - removed;
         return ucc::round_up_multiple<64>(sizeof(direct_inner_node) + clone->prefix().size() +
                                           (new_branches - 1) +
                                           new_branches * sizeof(ptr_address));
      }

      direct_inner_node(uint32_t asize, ptr_address_seq seq,
                        const op::inner_build_plan& plan) noexcept
          : node(asize, type_id, seq)
      {
         init_from_plan(plan, 0);
      }
      direct_inner_node(uint32_t asize, ptr_address_seq seq, const direct_inner_node* clone,
                        const op::inner_remove_branch& rm) noexcept
          : node(asize, type_id, seq)
      {
         clone_without_range(clone, rm.br, branch_number(*rm.br + 1));
      }
      direct_inner_node(uint32_t asize, ptr_address_seq seq, const direct_inner_node* clone,
                        const op::inner_remove_range& rm) noexcept
          : node(asize, type_id, seq)
      {
         clone_without_range(clone, rm.lo, rm.hi);
      }

      uint16_t num_branches() const noexcept { return _num_branches; }
      uint16_t num_clines() const noexcept { return _num_branches; }
      uint64_t last_unique_version() const noexcept { return _last_unique_version; }
      void     set_last_unique_version(uint64_t e) noexcept
      {
         _last_unique_version = version_token(e, last_unique_version_bits);
      }

      uint16_t prefix_len() const noexcept { return _prefix_len; }
      uint16_t prefix_capacity() const noexcept { return _prefix_cap; }
      key_view prefix() const noexcept { return key_view((const char*)_prefix, _prefix_len); }

      uint8_t*       divisions() noexcept { return _prefix + _prefix_cap; }
      const uint8_t* divisions() const noexcept { return _prefix + _prefix_cap; }
      uint32_t       num_divisions() const noexcept { return _num_branches - 1; }
      key_view       divs() const noexcept
      {
         return key_view((const char*)divisions(), num_divisions());
      }

      ptr_address* children() noexcept
      {
         return reinterpret_cast<ptr_address*>(tail()) - num_branches();
      }
      const ptr_address* children() const noexcept
      {
         return reinterpret_cast<const ptr_address*>(tail()) - num_branches();
      }
      const ptr_address* const_branches() const noexcept { return children(); }
      const ptr_address* branches_end() const noexcept { return children() + num_branches(); }

      std::span<const ptr_address> get_branch_clines() const noexcept
      {
         return {children(), num_branches()};
      }
      sal::alloc_hint hint() const noexcept { return sal::alloc_hint(children(), num_branches()); }

      branch_number lower_bound(key_view key) const noexcept
      {
         if (key.empty())
            return branch_number(0);
         return lower_bound(key[0]);
      }
      branch_number lower_bound(uint8_t byte) const noexcept
      {
         const uint16_t nd = num_divisions();
         if (nd == 0)
            return branch_number(0);
         uint16_t       lb = ucc::lower_bound_padded(divisions(), nd, byte);
         lb += (byte >= divisions()[lb]) * (lb < nd);
         return branch_number(lb);
      }

      ptr_address get_branch(branch_number n) const noexcept
      {
         assert(*n < num_branches());
         return children()[*n];
      }

      void visit_branches(std::invocable<ptr_address> auto&& lam) const noexcept
      {
         for (uint16_t i = 0; i < num_branches(); ++i)
            lam(children()[i]);
      }
      void visit_children(const std::function<void(sal::ptr_address)>& visitor) const noexcept
      {
         visit_branches([&](ptr_address addr) { visitor(addr); });
      }
      void destroy(const sal::allocator_session_ptr& session) const noexcept
      {
         visit_branches([&](ptr_address addr) { session->release(addr); });
      }

      [[nodiscard]] bool can_apply(const op::replace_branch&) const noexcept { return false; }
      void               apply(const op::replace_branch&) noexcept { assert(false); }

      void remove_branch(branch_number bn) noexcept
      {
         assert(*bn < _num_branches);
         assert(_num_branches > 1);
         remove_range(branch_number(*bn), branch_number(*bn + 1));
      }

      void remove_range(branch_number lo, branch_number hi) noexcept
      {
         assert(*lo < *hi && *hi <= _num_branches);
         assert((*hi - *lo) < _num_branches);
         const uint16_t old_nb       = _num_branches;
         const uint16_t remove_count = *hi - *lo;
         const auto*    old_children = children();

         std::array<ptr_address, 256> kept{};
         uint16_t out = 0;
         for (uint16_t i = 0; i < old_nb; ++i)
            if (i < *lo || i >= *hi)
               kept[out++] = old_children[i];

         if (*lo == 0)
            memmove(divisions(), divisions() + *hi, (old_nb - 1) - *hi);
         else
         {
            uint32_t head       = *lo - 1;
            uint32_t tail_start = *hi - 1;
            memmove(divisions() + head, divisions() + tail_start, (old_nb - 1) - tail_start);
         }

         _num_branches = old_nb - remove_count;
         std::memcpy(children(), kept.data(), _num_branches * sizeof(ptr_address));
         assert(validate_invariants());
      }

      [[nodiscard]] bool validate_invariants() const noexcept
      {
         if (num_branches() == 0 || num_branches() > op::inner_build_plan::max_branches)
            return false;
         if (!std::is_sorted(divisions(), divisions() + num_divisions()))
            return false;
         if ((const uint8_t*)(divisions() + num_divisions()) > (const uint8_t*)children())
            return false;
         for (uint16_t i = 0; i < num_branches(); ++i)
            if (children()[i] == sal::null_ptr_address)
               return false;
         return true;
      }

     private:
      void init_from_plan(const op::inner_build_plan& plan, uint64_t epoch) noexcept
      {
         _last_unique_version = version_token(epoch, last_unique_version_bits);
         _num_branches        = plan.num_branches;
         _prefix_len          = plan.prefix.size();
         _prefix_cap          = plan.prefix.size();
         std::memcpy(_prefix, plan.prefix.data(), plan.prefix.size());
         std::memcpy(divisions(), plan.dividers.data(), num_divisions());
         std::memcpy(children(), plan.branches.data(), num_branches() * sizeof(ptr_address));
         assert(validate_invariants());
      }

      void clone_without_range(const direct_inner_node* clone,
                               branch_number            lo,
                               branch_number            hi) noexcept
      {
         const uint16_t old_nb = clone->_num_branches;
         _last_unique_version  = clone->_last_unique_version;
         _prefix_len           = clone->_prefix_len;
         _prefix_cap           = clone->_prefix_len;
         std::memcpy(_prefix, clone->_prefix, _prefix_len);
         _num_branches = old_nb - (*hi - *lo);

         uint16_t out = 0;
         for (uint16_t i = 0; i < old_nb; ++i)
         {
            if (i >= *lo && i < *hi)
               continue;
            if (out > 0)
            {
               uint16_t div_idx = (i == 0) ? 0 : i - 1;
               if (i == *hi && *lo > 0)
                  div_idx = *hi - 1;
               divisions()[out - 1] = clone->divisions()[div_idx];
            }
            children()[out++] = clone->children()[i];
         }
         assert(out == _num_branches);
         assert(validate_invariants());
      }

      uint64_t _last_unique_version = 0;
      uint16_t _num_branches        = 0;
      uint16_t _prefix_len          = 0;
      uint16_t _prefix_cap          = 0;
      uint16_t _unused              = 0;
      uint8_t  _prefix[/*prefix_cap*/];
   } __attribute__((packed));

   class bplus_inner_node : public node
   {
     public:
      static constexpr node_type type_id = node_type::bplus_inner;
      static constexpr uint32_t  max_inner_size = 16 * 1024;

      static uint32_t alloc_size(const op::bplus_build_plan& plan) noexcept
      {
         assert(plan.num_branches > 0);
         assert(plan.num_branches <= op::bplus_build_plan::max_branches);
         uint32_t bytes = sizeof(bplus_inner_node) +
                          plan.num_branches * sizeof(ptr_address) +
                          (plan.num_branches - 1) * sizeof(uint16_t);
         for (uint16_t i = 0; i + 1 < plan.num_branches; ++i)
            bytes += sizeof(key_data) + plan.separators[i].size();
         return ucc::round_up_multiple<64>(bytes);
      }

      bplus_inner_node(uint32_t asize, ptr_address_seq seq,
                       const op::bplus_build_plan& plan) noexcept
          : node(asize, type_id, seq)
      {
         init_from_plan(plan, 0);
      }

      uint16_t num_branches() const noexcept { return _num_branches; }
      uint32_t num_divisions() const noexcept { return _num_branches - 1; }
      uint16_t num_clines() const noexcept { return _num_branches; }
      uint64_t last_unique_version() const noexcept { return _last_unique_version; }
      void     set_last_unique_version(uint64_t e) noexcept
      {
         _last_unique_version = version_token(e, last_unique_version_bits);
      }

      ptr_address* children() noexcept
      {
         return reinterpret_cast<ptr_address*>(_data);
      }
      const ptr_address* children() const noexcept
      {
         return reinterpret_cast<const ptr_address*>(_data);
      }
      const ptr_address* const_branches() const noexcept { return children(); }
      const ptr_address* branches_end() const noexcept { return children() + num_branches(); }

      uint16_t* separator_offsets() noexcept
      {
         return reinterpret_cast<uint16_t*>(children() + num_branches());
      }
      const uint16_t* separator_offsets() const noexcept
      {
         return reinterpret_cast<const uint16_t*>(children() + num_branches());
      }

      key_view separator(uint16_t n) const noexcept
      {
         assert(n < num_divisions());
         return get_key(separator_offsets()[n]);
      }

      sal::alloc_hint hint() const noexcept { return sal::alloc_hint(children(), num_branches()); }
      std::span<const ptr_address> get_branch_clines() const noexcept
      {
         return {children(), num_branches()};
      }

      branch_number lower_bound(key_view key) const noexcept
      {
         uint16_t lo = 0;
         uint16_t hi = num_divisions();
         while (lo < hi)
         {
            uint16_t mid = lo + (hi - lo) / 2;
            if (key < separator(mid))
               hi = mid;
            else
               lo = mid + 1;
         }
         return branch_number(lo);
      }

      ptr_address get_branch(branch_number n) const noexcept
      {
         assert(*n < num_branches());
         return children()[*n];
      }

      void visit_branches(std::invocable<ptr_address> auto&& lam) const noexcept
      {
         for (uint16_t i = 0; i < num_branches(); ++i)
            lam(children()[i]);
      }
      void visit_children(const std::function<void(sal::ptr_address)>& visitor) const noexcept
      {
         visit_branches([&](ptr_address addr) { visitor(addr); });
      }
      void destroy(const sal::allocator_session_ptr& session) const noexcept
      {
         visit_branches([&](ptr_address addr) { session->release(addr); });
      }

      [[nodiscard]] bool validate_invariants() const noexcept
      {
         if (num_branches() == 0 || num_branches() > op::bplus_build_plan::max_branches)
            return false;
         for (uint16_t i = 1; i < num_divisions(); ++i)
            if (!(separator(i - 1) < separator(i)))
               return false;
         for (uint16_t i = 0; i < num_branches(); ++i)
            if (children()[i] == sal::null_ptr_address)
               return false;
         return (const uint8_t*)meta_end() <= (const uint8_t*)alloc_head();
      }

     private:
      struct key_data
      {
         uint16_t size;
         uint8_t  data[];

         void set(key_view key) noexcept
         {
            size = key.size();
            std::memcpy(data, key.data(), size);
         }
         key_view get() const noexcept { return key_view((const char*)data, size); }
      } __attribute__((packed));

      void init_from_plan(const op::bplus_build_plan& plan, uint64_t epoch) noexcept
      {
         _last_unique_version = version_token(epoch, last_unique_version_bits);
         _num_branches        = plan.num_branches;
         _alloc_pos           = 0;
         std::memcpy(children(), plan.branches.data(), num_branches() * sizeof(ptr_address));
         for (uint16_t i = 0; i + 1 < num_branches(); ++i)
            separator_offsets()[i] = alloc_key(plan.separators[i]);
         assert(validate_invariants());
      }

      key_view get_key(uint16_t off) const noexcept
      {
         return reinterpret_cast<const key_data*>((const char*)tail() - off)->get();
      }
      uint16_t alloc_key(key_view key) noexcept
      {
         _alloc_pos += sizeof(key_data) + key.size();
         auto* ptr = reinterpret_cast<key_data*>((char*)tail() - _alloc_pos);
         assert((const uint8_t*)ptr >= (const uint8_t*)meta_end());
         ptr->set(key);
         return _alloc_pos;
      }
      const char* meta_end() const noexcept
      {
         return reinterpret_cast<const char*>(separator_offsets() + num_divisions());
      }
      const char* alloc_head() const noexcept { return (const char*)tail() - _alloc_pos; }

      uint64_t _last_unique_version = 0;
      uint16_t _num_branches        = 0;
      uint16_t _alloc_pos           = 0;
      uint32_t _unused              = 0;
      uint8_t  _data[/*children, separator offsets, key alloc*/];
   } __attribute__((packed));

   inline uint32_t inner_node::alloc_size(const branch_set&             branches,
                                          int                           numcline,
                                          const std::array<uint8_t, 8>& cline_indices,
                                          uint64_t) noexcept
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
                                 const std::array<uint8_t, 8>& cline_indices,
                                 uint64_t                      epoch) noexcept
       : inner_node_base(asize, node_type::inner, seq)
   {
      assert(asize == alloc_size(init_branches, numcline, cline_indices, epoch));
      init(init_branches, numcline, cline_indices, epoch);
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
                                          const cline_freq_table&         ftab,
                                          uint64_t) noexcept
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
                                 const cline_freq_table&         ftab,
                                 uint64_t                        epoch) noexcept
       : inner_node_base(asize, node_type::inner, seq)
   {
      assert(asize == alloc_size(clone, range, ftab, epoch));
      init(asize, seq, clone, range, ftab, epoch);
   }

   inline uint32_t inner_node::alloc_size(const any_inner_node_type auto* clone,
                                          const op::inner_remove_branch&) noexcept
   {
      auto new_branches_count = clone->num_branches() - 1;
      auto divider_capacity   = new_branches_count - 1;
      return ucc::round_up_multiple<64>(inner_node_size +
                                        clone->num_clines() * sizeof(ptr_address) +
                                        divider_capacity + new_branches_count);
   }

   inline inner_node::inner_node(uint32_t                        asize,
                                 ptr_address_seq                 seq,
                                 const any_inner_node_type auto* clone,
                                 const op::inner_remove_branch&  rm) noexcept
       : inner_node_base(asize, node_type::inner, seq)
   {
      assert(asize == alloc_size(clone, rm));
      init(clone, rm);
   }

   inline uint32_t inner_node::alloc_size(const any_inner_node_type auto* clone,
                                          const op::inner_remove_range&   rm) noexcept
   {
      auto new_branches_count = clone->num_branches() - (*rm.hi - *rm.lo);
      auto divider_capacity   = new_branches_count - 1;
      return ucc::round_up_multiple<64>(inner_node_size +
                                        clone->num_clines() * sizeof(ptr_address) +
                                        divider_capacity + new_branches_count);
   }

   inline inner_node::inner_node(uint32_t                        asize,
                                 ptr_address_seq                 seq,
                                 const any_inner_node_type auto* clone,
                                 const op::inner_remove_range&   rm) noexcept
       : inner_node_base(asize, node_type::inner, seq)
   {
      assert(asize == alloc_size(clone, rm));
      init(clone, rm);
   }

}  // namespace psitri
