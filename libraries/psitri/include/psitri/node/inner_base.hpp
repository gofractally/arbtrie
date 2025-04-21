#pragma once
#include <psitri/node/inner_node_util.hpp>
#include <psitri/node/node.hpp>

namespace psitri
{
   namespace op
   {
      struct replace_branch
      {
         branch_number     br;
         const branch_set& sub_branches;
         int               needed_clines;  ///< return value of find_clines
         uint8_t*          cline_indices;
      };
   };  // namespace op

   /**
    * This base class is used to abstract the common functionality between
    * inner_node and inner_prefix_node which are otherwise identical except
    * that inner_prefix_node also stores a prefix field.
    */
   template <typename Derived>
   class inner_node_base : public node
   {
     public:
      using node::node;

      ptr_address* clines() noexcept
      {
         return reinterpret_cast<ptr_address*>(tail()) - num_clines();
      }
      const ptr_address* clines() const noexcept
      {
         return reinterpret_cast<const ptr_address*>(tail()) - num_clines();
      }
      cline_data* get_cline_data() noexcept { return reinterpret_cast<cline_data*>(clines()); }

      ptr_address*       clines_tail() noexcept { return reinterpret_cast<ptr_address*>(tail()); }
      const ptr_address* clines_tail() const noexcept { return (const ptr_address*)(tail()); }

      /**
       * For each branch in sub_branches calculate the cacheline index it would be placed on 
       * assuming that the branch at br is being replaced. 
       * 
       * @return insufficient_clines if the node cannot accomodate the new branches, otherwise
       *         returns the number of clines needed for this node (including null clines in existing
       *         clines.
       */
      uint8_t find_clines(branch_number     br,
                          const branch_set& sub_branches,
                          uint8_t*          cline_indices) const noexcept;

      void destroy(const alloc_header* header, const sal::allocator_session_ptr& session) noexcept
      {
         const Derived& d   = static_cast<const Derived&>(*this);
         const branch*  pos = d.branches();
         auto           cd  = reinterpret_cast<const cline_data*>(d.clines());

         uint32_t nb = d.num_branches();
         assert(nb > 0 && "num branches should always be greater than 0");
         do
         {
            --nb;
            auto addr = ptr_address(*cd[pos->line()].base() + pos->index());
            session->release(addr);
            ++pos;
         } while (nb);
      }

     protected:
      inline void init(uint32_t                asize,
                       ptr_address_seq         seq,
                       const Derived*          clone,
                       subrange                range,
                       const cline_freq_table& ftab) noexcept
      {
         Derived& d      = static_cast<Derived&>(*this);
         d._num_branches = *range.end - *range.begin;
         d._num_cline    = ftab.compressed_clines();

         auto nth_set_bit_table = create_nth_set_bit_table_neon(ftab.freq_table);
         // note that this method is allowed to write to data up to 15 bytes before branches() as part of
         // the branchless SIMD implementation that processes 16 bytes at a time. This is fine, we just have to reset the
         // header information that may have been overwritten.
         copy_branches_and_update_cline_index(clone->const_branches() + *range.begin, d.branches(),
                                              d._num_branches, nth_set_bit_table);

         node::init(asize, Derived::type_id, seq);

         d._num_branches = *range.end - *range.begin;
         d._num_cline    = ftab.compressed_clines();
         d._descendents  = clone->_descendents;

         copy_masked_cline_data(ftab.clines_referenced, clone->clines(), d.clines());

         memcpy(d.divisions(), clone->divisions() + *range.begin, d._num_branches - 1);

         // update the ref counts for the new clines
         // the ref counts are stored in ftab for each index
         // we can utilize the nth_set_bit_table to convert the index in the freq table to the index
         // in the new clines array, and we can utilize the referenced_clines mask to determine which
         // indieces in the old index need to be updated.
         cline_data* clines_data = reinterpret_cast<cline_data*>(d.clines());
         uint32_t    bitmap      = ftab.clines_referenced;
         assert(bitmap != 0 && "clines_referenced should not be zero if node is valid");
         // Loop exactly 'compressed_clines' times
         uint32_t num_iterations = d._num_cline;
         do
         {
            --num_iterations;  // Decrement happens well before the check

            // Find the index of the least significant bit (original cline index)
            uint32_t i = __builtin_ctz(bitmap);
            assert(i < 16 && "Original cline index must be < 16");

            // Find the index of this cline in the new node's compacted cline array
            uint8_t new_idx = nth_set_bit_table[i];
            // Get the number of branches in this new node referencing this cline
            uint8_t new_ref_count = ftab.freq_table[i];

            assert(new_ref_count > 0 && "Reference count must be positive if cline is referenced");
            assert(new_ref_count <= 16 && "Reference count cannot exceed 16");
            assert(new_idx < d._num_cline && "New index must be within bounds");

            // Get reference to the target cline_data and set its reference count
            clines_data[new_idx].set_ref(new_ref_count);

            // Clear the least significant bit that we just processed
            bitmap &= (bitmap - 1);  // Ready for next potential ctz call

         } while (num_iterations);  // Check the decremented value
      }

      inline void init(const branch_set& branches, int numcline, uint8_t* cline_indices) noexcept
      {
         Derived& d      = static_cast<Derived&>(*this);
         d._num_branches = branches.count();
         d._num_cline    = numcline;
         d._descendents  = 0;

         memcpy(d.divisions(), branches.dividers().data(), branches.dividers().size());
         memset(d.clines(), 0xff, numcline * sizeof(ptr_address));

         auto cl_head = reinterpret_cast<cline_data*>(d.clines());

         auto    sub_addr = branches.addresses();
         branch* sub_brs  = d.branches();
         for (int i = 0; i < sub_addr.size(); ++i)
         {
            cline_data& cl = cl_head[cline_indices[i]];
            if (cl.is_null())
            {
               cl.set(sub_addr[i]);
               SAL_WARN("setting cline_data[{}] to {} ref: {}", int(cline_indices[i]), cl.base(),
                        cl.ref());
            }
            else
            {
               assert(cl.base() == (sub_addr[i] & ~ptr_address(0x0f)));
               cl.inc_ref();
               SAL_WARN("inc cline_data[{}] to {} ref: {}", int(cline_indices[i]), cl.base(),
                        cl.ref());
            }
            SAL_WARN("address {} => cline[{}] ", sub_addr[i], int(cline_indices[i]));
            sub_brs[i].set_line_index(cline_indices[i], *sub_addr[i] & uint32_t(0x0f));
            SAL_INFO("{} new branch {} {}", sub_brs, i, sub_brs[i]);
            assert(d.get_branch(branch_number(i)) == sub_addr[i]);
         }
      }
      inline void init(const Derived* clone, const op::replace_branch& update) noexcept
      {
         Derived& d      = static_cast<Derived&>(*this);
         d._num_branches = clone->_num_branches + update.sub_branches.count() - 1;
         d._num_cline    = update.needed_clines;
         d._descendents  = clone->_descendents;

         sal::ptr_address*       d_clines_data = reinterpret_cast<sal::ptr_address*>(d.clines());
         const uint16_t          d_num_clines  = d._num_cline;
         const sal::ptr_address* c_clines_data = clone->clines();
         const uint16_t          c_num_clines  = clone->_num_cline;
         const branch*           c_branches    = clone->const_branches();
         const uint8_t*          c_divisions   = clone->divisions();
         uint8_t*                d_divisions   = d.divisions();
         branch*                 d_branches    = d.branches();

         uint32_t new_clines = d_num_clines - c_num_clines;
         std::copy(c_clines_data, c_clines_data + c_num_clines, d_clines_data);
         std::fill(d_clines_data + c_num_clines, d_clines_data + d_num_clines,
                   sal::null_ptr_address);

         // release the cline data for the branch being replaced
         // calculate the branches from the addresses + cline indices
         auto cl_head = reinterpret_cast<cline_data*>(d_clines_data);
         cl_head[c_branches[*update.br].line()].dec_ref();

         int  in_pos   = *update.br;
         auto new_divs = update.sub_branches.dividers();
         memcpy(d_divisions, c_divisions, in_pos);
         memcpy(d_divisions + in_pos, new_divs.data(), new_divs.size());
         memcpy(d_divisions + in_pos + new_divs.size(), c_divisions + in_pos,
                clone->num_divisions() - in_pos);

         memcpy(d_branches, c_branches, in_pos);
         // copy the tail branches from the clone, skipping the branch being replaced
         memcpy(d_branches + in_pos + update.sub_branches.count(), c_branches + in_pos + 1,
                clone->_num_branches - in_pos - 1);

         branch* sub_brs = d_branches + in_pos;
         for (int i = 0; i < update.sub_branches.count(); ++i)
         {
            assert(update.cline_indices[i] < d._num_cline);
            cline_data& cl = cl_head[update.cline_indices[i]];
            if (cl.is_null())
               cl.set(update.sub_branches.addresses()[i]);
            else
            {
               assert(cl.base() == (update.sub_branches.addresses()[i] & ~ptr_address(0x0f)));
               cl.inc_ref();
            }
            sub_brs[i].set_line_index(update.cline_indices[i],
                                      *update.sub_branches.addresses()[i] & 0x0f);
         }
         assert(std::is_sorted(d.divisions(), d.divisions() + d.num_divisions()));
      }

     public:
      [[nodiscard]] inline bool can_apply(const op::replace_branch& up) const noexcept;
      inline void               apply(const op::replace_branch& up) noexcept;
      [[nodiscard]] inline bool validate_invariants() const noexcept;

      std::span<const ptr_address> get_branch_clines() const noexcept
      {
         const Derived& d = static_cast<const Derived&>(*this);
         return std::span<const ptr_address>(d.clines(), d.num_clines());
      }

      [[nodiscard]] inline ptr_address get_branch(branch_number n) const noexcept
      {
         const Derived& d  = static_cast<const Derived&>(*this);
         auto           br = d.const_branches()[*n];
         auto           cd = reinterpret_cast<const cline_data*>(d.clines());
         return ptr_address(*cd[br.line()].base() + br.index());
      }

      void visit_branches(auto&& lam) const noexcept(lam(ptr_address()))
      {
         const Derived&     d   = static_cast<const Derived&>(*this);
         const branch*      pos = d.branches();
         const branch*      end = d.branches_end();
         const ptr_address* cls = d.clines_tail();
         auto               cd  = reinterpret_cast<const cline_data*>(d.clines());
         while (pos != end)
         {
            lam(ptr_address(*cd[pos->line()].base() + pos->index()));
            ++pos;
         }
      }
      key_view divs() const noexcept
      {
         const Derived& d = static_cast<const Derived&>(*this);
         return key_view((const char*)d.divisions(), d.num_divisions());
      }
      uint32_t num_divisions() const noexcept
      {
         const Derived& d = static_cast<const Derived&>(*this);
         return d.num_branches() - 1;
      }

      uint32_t num_clines() const noexcept
      {
         const Derived& d = static_cast<const Derived&>(*this);
         return d._num_cline;
      }
      branch_number lower_bound(uint8_t byte) const noexcept
      {
         const Derived& d        = static_cast<const Derived&>(*this);
         const uint8_t* divs     = d.divisions();
         const uint16_t num_divs = d.num_divisions();
         // lower bound padded is 2x faster and we know it is always safe to read 16 bytes
         // past the end of dividers because it is followed
         // by branches free space, and clines. And even if it read past that the allocator always has
         // one cacheline at the end of the segment. All safe to read.
         uint16_t lbidx = ucc::lower_bound_padded(divs, num_divs, byte);

         SAL_WARN("lower_bound idx: {} divs: {} query: {}", lbidx, d.divs(), byte);

         // num_divisions() is always 1 less than num_branches()
         // lower_bound() calculates the number of divisions that are "less than" the byte, which
         // gives us the index of the first division that is greater, which means the result is
         // always a valid branch number.

         // if the firsts division is equal to the byte, then we get 0, but the first branch is
         // for things strictly less than the first division... likewise for all other divisions.
         // example:   Single Divider "b" with branches 0, and 1 with 0 for < b and 1 for >= b
         // lowerbound of "a" is also at index "0" in divisions and needs to be branch 0
         // lowerbound of "b" is at index "0" in divisions and needs to be branch 1
         // lowerbound of "c" is at index "1" in divisions and needs to be branch 1
         // "b">="b"  true  * (0 < num_div(1))  = 1
         // "c">="b"  true  * (1 < num_div(1))  = 0
         // this adds 1 to the lowerbound index exactly when needed.

         lbidx += (byte >= (divs[lbidx])) * (lbidx < num_divs);
         SAL_WARN("after fixup lower_bound idx: {} divs: {} query: {}", lbidx, d.divs(), byte);
         return branch_number(lbidx);
      }
      uint64_t descendents() const noexcept
      {
         const Derived& d = static_cast<const Derived&>(*this);
         return d._descendents;
      }
      uint32_t free_space() const noexcept
      {
         const Derived& d         = static_cast<const Derived&>(*this);
         uint32_t       head_size = (uint8_t*)d.branches_end() - ((uint8_t*)this);
         uint32_t       tail_size = d.num_clines() * sizeof(ptr_address);
         return size() - (head_size + tail_size);
      }
   };
   template <typename Derived>
   inline uint8_t inner_node_base<Derived>::find_clines(branch_number     br,
                                                        const branch_set& sub_branches,
                                                        uint8_t* cline_indices) const noexcept
   {
      const Derived& d = static_cast<const Derived&>(*this);
      return psitri::find_clines(d.get_branch_clines(), d.get_branch(br), sub_branches.addresses(),
                                 cline_indices);
   }
   template <typename Derived>
   [[nodiscard]] inline bool inner_node_base<Derived>::can_apply(
       const op::replace_branch& up) const noexcept
   {
      const Derived& d = static_cast<const Derived&>(*this);
      return Derived::alloc_size(&d, up) == d.size();
   }
   //  cur_divisions[0...br]
   //  new_sub_divisions[0...sub_divisions.count()]
   //  cur_divisions[br+1... num_divisions() )
   //  padding divisions[new_divisions_cap-new_divisions_count] = 0xff
   //  cur_branches[0...br)
   //  new_branches[0...sub_branches.count())
   //  cur_branches[br+1...num_branches())
   //  padding  []...
   //  clines[needed_clines]

   // Initial Condition... replace branch d
   // dividers:   a b c d l m n
   // div idx:    0 1 2 3 4 5 6  (7 dividers)
   // branches: 0 1 2 3 4 5 6 7  (8 branches = num_branches)
   // replace 'd' with 'def' where d = branch num 4, div idx 3
   //           [d]e f
   //           0 1 2 under
   template <typename Derived>
   inline void inner_node_base<Derived>::apply(const op::replace_branch& up) noexcept
   {
      Derived* d = static_cast<Derived*>(this);
      assert(std::is_sorted(up.sub_branches.dividers().begin(), up.sub_branches.dividers().end()));
      assert(d->can_apply(up));
      auto new_branch_count = d->_num_branches + up.sub_branches.count() - 1;
      SAL_WARN("replace branch #{} with {} branches, current num branches: {} new num branches: {}",
               up.br, up.sub_branches.count(), int(d->_num_branches), new_branch_count);
      auto     new_divisions_cap   = ucc::round_up_multiple<1>(new_branch_count - 1);
      uint32_t tail_branches_count = d->_num_branches - *up.br - 1;
      uint32_t head_branches_count = *up.br;

      // release the cline data for the branch being replaced
      d->get_cline_data()[d->branches()[*up.br].line()].dec_ref();

      branch* cur_branches_end      = d->branches_end();
      branch* cur_tail_branch_begin = cur_branches_end - tail_branches_count;
      branch* new_branches_begin    = (branch*)(d->divisions() + new_divisions_cap);
      branch* new_branches_end      = new_branches_begin + new_branch_count;
      branch* new_tail_branch_begin = new_branches_end - tail_branches_count;

      static_assert(sizeof(branch) == 1);
      memmove(new_tail_branch_begin, cur_tail_branch_begin, tail_branches_count);
      memmove(new_branches_begin, d->branches(), head_branches_count);
      // we have to calculate the branches from the addresses + cline indices

      int      new_divisions_count      = new_branch_count - 1;
      uint8_t* new_divisions_end        = d->divisions() + new_divisions_count;
      int      div_tail_len             = d->_num_branches - *up.br - 1;
      uint8_t* new_divisions_tail_begin = new_divisions_end - div_tail_len;
      uint8_t* cur_divisions_tail_begin = d->divisions() + d->_num_branches - 1 - div_tail_len;

      auto sub_div = up.sub_branches.dividers();
      memmove(new_divisions_tail_begin, cur_divisions_tail_begin, div_tail_len);
      memcpy(new_divisions_tail_begin - sub_div.size(), sub_div.data(), sub_div.size());

      assert(up.needed_clines >= d->_num_cline);
      // allocate space for the new clines, and move existing clintes.
      auto         new_clines_count = up.needed_clines;
      ptr_address* new_clines_head  = d->clines_tail() - new_clines_count;
      auto         delta_clines     = d->clines() - new_clines_head;
      memmove(new_clines_head, d->clines(), d->_num_cline * sizeof(cline_data));
      memset(d->clines_tail() - delta_clines, 0xff, delta_clines * sizeof(cline_data));

      auto cl_head = reinterpret_cast<cline_data*>(new_clines_head);

      auto    sub_addr = up.sub_branches.addresses();
      branch* sub_brs  = new_branches_begin + *up.br;
      for (int i = 0; i < sub_addr.size(); ++i)
      {
         assert(up.cline_indices[i] < up.needed_clines);
         cline_data& cl = cl_head[up.cline_indices[i]];
         if (cl.is_null())
            cl.set(sub_addr[i]);
         else
         {
            assert(cl.base() == (sub_addr[i] & ~ptr_address(0x0f)));
            cl.inc_ref();
         }
         sub_brs[i].set_line_index(up.cline_indices[i], *sub_addr[i] & 0x0f);
         //SAL_INFO("new branch {} {}", i, sub_brs[i]);
      }
      //   SAL_WARN("new branch count: {}  num_clines: {}", new_branch_count, needed_clines);
      d->_num_branches = new_branch_count;
      //_division_capacity = new_divisions_cap;
      d->_num_cline = up.needed_clines;
      assert(std::is_sorted(d->divisions(), d->divisions() + d->num_divisions()));
   }
   template <typename Derived>
   [[nodiscard]] inline bool inner_node_base<Derived>::validate_invariants() const noexcept
   {
      const Derived& d      = static_cast<const Derived&>(*this);
      bool divisions_sorted = std::is_sorted(d.divisions(), d.divisions() + d.num_divisions());
      // Assert in debug builds (standard assert does this automatically)
      assert(divisions_sorted && "Divisions are not sorted");
      if (!divisions_sorted)
         return false;  // Return false in all builds if check fails
                        // 2. Check internal layout
      bool ok = ((const uint8_t*)d.branches_end() <= (const uint8_t*)d.clines());
      assert(ok && "Branches array overlaps with clines array or exceeds node bounds");
      if (!ok)
         return false;

      // Verify that cline reference counts match actual branch usage
      for (uint32_t cline_idx = 0; cline_idx < d._num_cline; ++cline_idx)
      {
         // Use const version of clines() or reinterpret_cast if necessary
         const cline_data* cline_ptr = reinterpret_cast<const cline_data*>(d.clines() + cline_idx);
         uint32_t          actual_refs = 0;
         // Count branches referencing this cline
         for (uint32_t branch_idx = 0; branch_idx < d._num_branches; ++branch_idx)
         {
            if (d.const_branches()[branch_idx].line() == cline_idx)
               ++actual_refs;
         }

         bool ref_count_matches = (cline_ptr->ref() == actual_refs);
         // Assert in debug builds (standard assert does this automatically)
         assert(ref_count_matches && "Cline reference count mismatch");
         if (!ref_count_matches)
            return false;  // Return false in all builds if check fails
      }
      return true;
   }
}  // namespace psitri
