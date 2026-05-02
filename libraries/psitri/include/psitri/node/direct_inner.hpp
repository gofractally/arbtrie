#pragma once
#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <psitri/node/inner_base.hpp>
#include <sal/numbers.hpp>
#include <ucc/lower_bound.hpp>

namespace psitri
{

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

}  // namespace psitri
