#pragma once
#include <cstdint>
#include <sal/alloc_header.hpp>
#include <string_view>
#include <ucc/lower_bound.hpp>
#include <ucc/typed_int.hpp>
#include "sal/numbers.hpp"

namespace psitri
{
   using ptr_address     = sal::ptr_address;
   using ptr_address_seq = sal::ptr_address_seq;
   using key_view        = std::string_view;
   using value_view      = std::string_view;
   using branch_number   = ucc::typed_int<uint16_t, struct branch_number_tag>;

   static constexpr uint8_t       insufficient_clines = 0xff;
   static constexpr branch_number branch_zero         = branch_number(0);

   struct subrange
   {
      branch_number begin;
      branch_number end;
   };

   /**
    * The type of node, inner, leaf, or value, extends the types
    * sal::alloc_header::header_type enum.
    */
   enum class node_type : uint8_t
   {
      inner = (uint8_t)sal::header_type::start_user_type,
      inner_prefix,
      leaf,
      value
   };
   inline std::ostream& operator<<(std::ostream& os, node_type t)
   {
      static const char* names[] = {"inner", "inner_prefix", "leaf", "value", "unknown"};
      auto               idx     = static_cast<size_t>(t) - static_cast<size_t>(node_type::inner);
      os << (idx < 4 ? names[idx] : names[4]);
      return os;
   }

   /**
    * Each node maintains a list of ptr_address that point to the
    * cacheline, a brnach is an position(line) in this list plus an index
    * the cacheline (index). Each line is points to 128 bytes or
    * 16 sal::shared_ptr objects, in accordance to Intel and Apple fetching
    * 128 bytes of RAM at a time to L3 cache.  In this way we can reference
    * tri branches is as few as 1.25 bytes per branch if the allocator helps
    * us with good locality.
    */
   struct branch
   {
      uint8_t branch_data;
      void    set_line(uint8_t line) noexcept
      {
         assert(line < 16);
         branch_data = (branch_data & 0x0f) | (line << 4);
      }
      void set_index(uint8_t index) noexcept
      {
         assert(index < 16);
         branch_data = (branch_data & 0xf0) | index;
      }
      void set_line_index(uint8_t line, uint8_t index) noexcept
      {
         assert(line < 16);
         assert(index < 16);
         branch_data = (line << 4) | index;
      }
      uint8_t line() const noexcept { return branch_data >> 4; }
      uint8_t index() const noexcept { return branch_data & 0x0f; }

      friend std::ostream& operator<<(std::ostream& os, const branch& br)
      {
         os << "{line: " << int(br.line()) << " index: " << int(br.index()) << "}";
         return os;
      }
   } __attribute__((packed));
   static_assert(sizeof(branch) == 1);

   /**
    * Holds a temporary set of branches and dividers that exists
    * outside any node during the split process. There are at most
    * 6 branches and 5 dividers because that is the worst case amount
    * of node division required to make space for a new ptr_address in
    * a set of 16 ptr_address.
    */
   class branch_set
   {
     public:
      branch_set() { div[0] = 0; }
      uint16_t count() const noexcept { return div[0]; }

      branch_set(ptr_address branch)
      {
         branches[0] = branch;
         div[0]      = 1;
      }
      branch_set(uint8_t divider, ptr_address branch, ptr_address branch2)
      {
         branches[0] = branch;
         branches[1] = branch2;
         div[0]      = 2;
         div[1]      = divider;
      }
      /** the first branch does not have a divider */
      void set_front(ptr_address branch)
      {
         branches[0] = branch;
         div[0]      = 1;
      }
      ptr_address front() const noexcept { return branches[0]; }
      /** adds a 2nd+ branch which has a divider separating from
          the first branch.
         */
      void push_back(uint8_t d, ptr_address branch) noexcept
      {
         assert(count() > 0);
         assert(count() < 6);
         branches[count()] = branch;
         div[count()]      = d;
         div[0]++;
      }
      void push_front(ptr_address branch, uint8_t d)
      {
         assert(count() < 6);
         memmove(branches + 1, branches, count() * sizeof(ptr_address));
         memmove(div + 2, div + 1, count() * sizeof(uint8_t) - 1);
         branches[0] = branch;
         div[0]++;
         div[1] = d;
      }
      ptr_address                     get_first_branch() const noexcept { return branches[0]; }
      std::pair<uint8_t, ptr_address> get_div_branch(int b) const noexcept
      {
         assert(b > 0);
         assert(b < count());
         return {div[b], branches[b]};
      }
      // given one result with 2 branches and 1 divider
      // after you recusively upsert into the second branch you get
      // a replacement set of branches
      //
      // Given this:
      //  a1 a2
      //  2  d1
      // We have recursed into a2 and gotten a upsert_result back
      //  a3 a4 a5...
      //  3  d2 d3...
      // We need to replace a2 with a3-a5 and get
      // a1 a3 a4 a5...
      // 4  d1 d2 d3...
      void replace_back(const branch_set& other) noexcept
      {
         assert(count() + other.count() <= 7);
         // all of the branches from other are copied, and overwrite the last branch
         std::copy(other.branches, other.branches + other.count(), branches + count() - 1);
         // all of the dividers are copied, but don't overwrite the last divider
         std::copy(other.div + 1, other.div + other.count() - 1, div + count());
         div[0] += other.count() - 1;
      }
      std::span<const ptr_address> addresses() const noexcept { return {branches, count()}; }
      std::span<const uint8_t> dividers() const noexcept { return {div + 1, size_t(div[0]) - 1}; }

      friend std::ostream& operator<<(std::ostream& out, const branch_set& bs)
      {
         out << "branches: " << bs.count();
         out << "\n        ";
         for (int i = 1; i < bs.count(); ++i)
            out << std::setw(8) << bs.div[i] << " ";
         out << "\n";
         for (int i = 0; i < bs.count(); ++i)
            out << std::setw(8) << bs.branches[i] << " ";
         return out;
      }
      bool contains(ptr_address branch) const noexcept
      {
         // TODO: use SIMD
         for (int i = 0; i < count(); ++i)
            if (branches[i] == branch)
               return true;
         return false;
      }

     private:
      ptr_address branches[6];
      uint8_t     div[6];
   };

   /**
    * Base class for all nodes in the psitri tree, it mostly just wraps and redefines
    * methods from sal::alloc_header to use node_type instead of sal::header_type.
    */
   class node : public sal::alloc_header
   {
     public:
      constexpr node_type type() const noexcept { return (node_type)alloc_header::type(); };
      const node*         tail() const noexcept { return reinterpret_cast<const node*>(next()); }
      node* tail() noexcept { return reinterpret_cast<node*>(((char*)this) + size()); }

     protected:
      node(uint32_t asize, node_type t, ptr_address_seq seq)
          : sal::alloc_header(asize, sal::header_type(t), seq)
      {
      }
      void init(uint32_t asize, node_type t, ptr_address_seq seq) noexcept
      {
         alloc_header::init(asize, sal::header_type(t), seq);
      }
   } __attribute__((packed));
   static_assert(sizeof(node) == 12);

   /**
    * generic utility function that can be used with all nodes to find the new
    * cacheline indicies needed when replacing an old branch with a set of new
    * branches.
    * @param out_cline_indices - an array of size new_branches.size() that will
    *                           be populated with the cacheline indicies to be used
    *                           when allocating the new branches in later methods.
    */
   template <bool remove_old_branch = true>
   inline uint8_t find_clines(std::span<const ptr_address> current_clines,
                              ptr_address                  old_branch,
                              std::span<const ptr_address> new_branches,
                              std::array<uint8_t, 8>&      out_cline_indices) noexcept
   {
      //      SAL_INFO("find_clines({}, {}, {})", current_clines.size(), old_branch, new_branches.size());
      assert(current_clines.size() <= 16);
      assert(new_branches.size() <= 8);
      // current clines uses lower 4 bits to store the occupancy count, so
      // we have to shift them by 4 bits to get the actual cacheline address.
      alignas(64)
          ptr_address temp[16 + remove_old_branch];  // include an extra to prevent branch later
      alignas(16) ptr_address new_clines[8];
      ptr_address             old_branch_cline = old_branch >> 4;
      std::copy(current_clines.begin(), current_clines.end(), temp);
      std::copy(new_branches.begin(), new_branches.end(), new_clines);
      std::fill(temp + current_clines.size(), temp + 16, sal::null_ptr_address);

// Shift all temp values 4 bits right to align cacheline addresses
#if defined(__ARM_NEON)
      // Load 4 vectors of 4 uint32s each from temp array
      uint32x4_t vec[4];
      vec[0] = vld1q_u32((uint32_t*)(temp + 0));  // load_4_u32
      vec[1] = vld1q_u32((uint32_t*)(temp + 4));
      vec[2] = vld1q_u32((uint32_t*)(temp + 8));
      vec[3] = vld1q_u32((uint32_t*)(temp + 12));

      // Shift right by 4 bits to align cacheline addresses
      vec[0] = vshrq_n_u32(vec[0], 4);  // shift_right_4_u32
      vec[1] = vshrq_n_u32(vec[1], 4);
      vec[2] = vshrq_n_u32(vec[2], 4);
      vec[3] = vshrq_n_u32(vec[3], 4);

      // Store shifted vectors back to temp array
      vst1q_u32((uint32_t*)(temp + 0), vec[0]);  // store_4_u32
      vst1q_u32((uint32_t*)(temp + 4), vec[1]);
      vst1q_u32((uint32_t*)(temp + 8), vec[2]);
      vst1q_u32((uint32_t*)(temp + 12), vec[3]);

      // Load and shift new_clines array
      uint32x4_t new_vec_lo = vld1q_u32((uint32_t*)(new_clines + 0));  // load lower 4 values
      uint32x4_t new_vec_hi = vld1q_u32((uint32_t*)(new_clines + 4));  // load upper 4 values

      new_vec_lo = vshrq_n_u32(new_vec_lo, 4);  // shift right by 4
      new_vec_hi = vshrq_n_u32(new_vec_hi, 4);

      vst1q_u32((uint32_t*)(new_clines + 0), new_vec_lo);  // store back
      vst1q_u32((uint32_t*)(new_clines + 4), new_vec_hi);

      // Handle old branch removal if enabled
      if constexpr (remove_old_branch)
      {
         // Find index of old_branch_cline in temp array (returns 16 if not found)
         auto idx = ucc::find_u32x16_neon(vec[0], vec[1], vec[2], vec[3], *old_branch_cline);
         //         SAL_ERROR("find_clines: find_u32x16_neon: idx:   {} cur_cline: {}", idx,
         ////                  current_clines.size());
         //idx = std::min(idx, int(current_clines.size()));

         // Branchless update: Set temp[idx] to null_ptr_address if branch not shared
         // Only applies if idx is valid and branch count is 0 (single reference)
         temp[idx] |=
             ptr_address(-(idx < current_clines.size() and (*current_clines[idx] & 0x0f) == 0)) >>
             4;
      }
      //temp[idx] = sal::null_ptr_address;
      /// TODO: ensure that control_block_alloc never returns a ptr_address on cline 0
      /// we use the lower 4 bits to store the count on each branch, 0 means a count of
      /// 1 (becaus it is present at all), 0x0f means a count of 16 meaning all 16 slots
      /// in the cline are occupied by branches on this node. if the old_branch is the
      /// only reference it becomes a null_ptr_address because we can reuse it.
#else
      if constexpr (remove_old_branch)
      {
         for (size_t i = 0; i < current_clines.size(); i++)
         {
            if (temp[i] == old_branch_cline and (*current_clines[i] & 0x0f) == 0)
            {
               temp[i] = sal::null_ptr_address;
               break;
            }
         }
      }
      for (size_t i = 0; i < new_branches.size(); i++)
         new_clines[i] >>= 4;
      for (size_t i = 0; i < current_clines.size(); i++)
         temp[i] >>= 4;
#endif

      assert(current_clines.size() > 0);
      uint32_t    max_branch_index = 1 << (current_clines.size() - 1);
      ptr_address null_cline       = sal::null_ptr_address >> 4;
      for (int i = 0; i < new_branches.size(); ++i)
      {
         // TODO: provide scalar and SSE versions
         int cur_idx = ucc::find_u32x16_neon(vec[0], vec[1], vec[2], vec[3], *new_clines[i]);
         if (cur_idx < 16) [[likely]]
         {
            //            SAL_INFO("find_clines: found existing at {}", cur_idx);
            out_cline_indices[i] = cur_idx;
            continue;
         }
         cur_idx = ucc::find_u32x16_neon(vec[0], vec[1], vec[2], vec[3], *null_cline);
         if (cur_idx == 16) [[unlikely]]
         {
            //            SAL_ERROR("find_clines: insufficient clines");
            return insufficient_clines;
         }
         //        SAL_WARN("find_clines: found null at {}", cur_idx);

         temp[cur_idx] = new_clines[i];
         // Reload just the vector containing the modified index
         vec[cur_idx / 4]     = vld1q_u32((uint32_t*)(temp + (cur_idx & ~3)));
         out_cline_indices[i] = cur_idx;
         max_branch_index     = 1 << cur_idx;
      }
      return (32 - __builtin_clz(max_branch_index));
   }
   inline uint8_t find_clines(std::span<const ptr_address> current_clines,
                              std::span<const ptr_address> new_branches,
                              std::array<uint8_t, 8>&      out_cline_indices) noexcept
   {
      return find_clines<false>(current_clines, {}, new_branches, out_cline_indices);
   }
   inline uint8_t find_clines(std::span<const ptr_address> new_branches,
                              std::array<uint8_t, 8>&      out_cline_indices) noexcept
   {
      ptr_address cur = sal::null_ptr_address;
      return find_clines<false>({&cur, 1}, {}, new_branches, out_cline_indices);
   }
   inline uint8_t find_clines(const branch_set&       branches,
                              std::array<uint8_t, 8>& out_cline_indices) noexcept
   {
      ptr_address cur = sal::null_ptr_address;
      return find_clines<false>({&cur, 1}, {}, branches.addresses(), out_cline_indices);
   }

   /**
    * Interprets a cline as a base address + ref count so we can track
    * how many branches in a node are currently using the cline_data and
    * easily mark it free when the last reference goes away. Becaue every
    * valid count falls on the cline this does not corrupt the ability to
    * use this data as a hint to the allocator which ignores the lower 4
    * bits of every address in the hint. 
    * when there are no references the cline_data object == null_ptr_address
    * which is -1 (aka 0xffffffff)
    */
   struct cline_data
   {
      uint32_t    data;
      ptr_address base() const noexcept { return ptr_address(data & ~uint32_t(0x0f)); }
      uint32_t    ref() const noexcept { return uint32_t(data & 0x0f) + 1; }

      cline_data() : data(-1) {}

      /// automatically sets this to null_ptr_cline when last def is reached
      void dec_ref() noexcept
      {
         --data;
         data |= -uint32_t((data & 0x0f) == 0x0f);
      }
      void inc_ref() noexcept
      {
         assert((data & 0x0f) != 0x0f);
         ++data;
      }
      /// ref is the number of times referenced, sets to 0 because
      /// the non-null state is already ref count of 1 and 0xff is 16
      void set_ref(uint32_t ref) noexcept
      {
         assert(ref <= 16);
         assert(ref > 0);
         data = (data & ~uint32_t(0x0f)) | (ref - 1);
      }
      void set(ptr_address addr) noexcept
      {
         assert(is_null());
         data = *addr & ~uint32_t(0x0f);
         assert(ref() == 1);
      }
      bool is_null() const noexcept
      {
         return std::bit_cast<uint32_t>(*this) == *sal::null_ptr_address;
      }
   } __attribute__((packed));
   static_assert(sizeof(cline_data) == sizeof(ptr_address));
}  // namespace psitri
