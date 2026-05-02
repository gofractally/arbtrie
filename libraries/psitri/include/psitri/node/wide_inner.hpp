#pragma once
#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <psitri/node/inner_base.hpp>
#include <psitri/node/wide_branch.hpp>
#include <sal/numbers.hpp>
#include <ucc/lower_bound.hpp>

namespace psitri
{

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

}  // namespace psitri
