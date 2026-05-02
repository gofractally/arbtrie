#pragma once
#include <cassert>
#include <cstring>
#include <psitri/node/inner_base.hpp>
#include <sal/numbers.hpp>

namespace psitri
{

   class bplus_inner_node : public node
   {
     public:
      static constexpr node_type type_id = node_type::bplus_inner;
      static constexpr uint16_t  max_branches = op::bplus_build_plan::max_node_branches;
      static constexpr uint32_t  max_inner_size = 16 * 1024;

      static uint32_t alloc_size(const op::bplus_build_plan& plan) noexcept
      {
         assert(plan.num_branches > 0);
         assert(plan.num_branches <= max_branches);
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
         if (num_branches() == 0 || num_branches() > max_branches)
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

}  // namespace psitri
