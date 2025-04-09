#pragma once
#include <cassert>
#include <cstdint>
#include <optional>
#include <psitri/node/node.hpp>
#include <psitri/util.hpp>
#include <span>

namespace psitri
{

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
    *  Unlike the ARBTRIE above... we only have 1 inner node type
    */
   class inner : public node
   {
     public:
      /// construct a basic node with a prefix, two branches and a divider
      inner(uint32_t        asize,
            ptr_address_seq seq,
            key_view        prefix,
            ptr_address     self,
            ptr_address     a,
            uint8_t         div,
            ptr_address     b);
      /// clone and insert branch b after div
      inner(uint32_t        asize,
            ptr_address_seq seq,
            const inner*    clone,
            key_view        prefix,
            uint8_t         div,
            ptr_address     b);
      /// clone and update branch_num with ptr_address
      inner(uint32_t        asize,
            ptr_address_seq seq,
            const inner*    clone,
            key_view        prefix,
            branch_number   bn,
            ptr_address     b);

      static uint32_t alloc_size(uint32_t prefix_size, uint32_t ncline, uint32_t nbranch)
      {
         uint32_t size =
             sizeof(inner) + prefix_size + ncline * sizeof(ptr_address) + nbranch * 2 - 1;
         return round_up_multiple<64>(size);
      }

      /// query API

      uint64_t       descendents() const noexcept { return _descendents; }
      const key_view prefix() const noexcept
      {
         return key_view((const char*)_prefix, _prefix_size);
      }
      ptr_address get_branch(branch_number n) const noexcept
      {
         auto br = branches()[*n];
         return ptr_address(*clines_tail()[-br.line] + br.index);
      }

      /// branches are numbered 0 to num_branches()
      branch_number lower_bound(uint8_t b) const noexcept
      {
         return branch_number(psitri::lower_bound(divisions(), num_divisions(), b));
      }

      /// modification API

      bool can_set_branch(branch_number bn, ptr_address bp) noexcept { return false; }

      bool set_branch(branch_number bn, ptr_address bp) noexcept
      {
         return false;
         // does it share an existing cacheline?
         // have we exceeded 16 cl?
         //   can we add a new cacheline for this one w/out realloc
      }

      bool can_insert_branch() const noexcept { return false; }

      void insert_branch(uint8_t divider, branch bc) noexcept
      {
         assert(can_insert_branch());
         assert(bc.line < num_clines());
      }

      // given a ptr_address, calculate branch object it should fall on,
      // if it falls on an existing cacheline.
      std::optional<branch> calc_branch(ptr_address bp) const noexcept
      {
         const ptr_address  cl        = bp & ~ptr_address(0x0f);
         const ptr_address* lines     = clines_tail();
         auto               num_lines = num_clines();
         for (int i = 0; i < num_lines; ++i)
         {
            if (lines[-i] == cl)
               return branch(i, *bp & 0x0f);
         }
         return std::nullopt;
      }
      bool can_add_cline() const noexcept
      {
         if (num_clines() >= 16)
            return false;
         auto fs = free_space();
         if (fs >= sizeof(ptr_address))
            return true;
         /// perhaps we have a null cline we can use
         return calc_branch(ptr_address(0)).has_value();
      }

      uint32_t free_space() const noexcept
      {
         uint32_t head_size = (uint8_t*)branches_end() - ((uint8_t*)this);
         uint32_t tail_size = num_clines() * sizeof(ptr_address);
         return size() - (head_size + tail_size);
      }

      void visit_branches(auto&& lam) const noexcept(lam(ptr_address()))
      {
         const branch*      pos = branches();
         const branch*      end = branches_end();
         const ptr_address* cls = clines_tail();
         while (pos != end)
         {
            lam(ptr_address(*cls[-pos->line] + pos->index));
            ++pos;
         }
      }

      void remove_branch(branch_number)
      {
         // if this is the only branch ref a cline,
         // then set the cline to null.
         // shift all branches after bn left 1 byte
         // shift all dividers after bn-1 left 1 byte
      }
      std::span<const ptr_address> get_branch_clines() const noexcept
      {
         return std::span<const ptr_address>(clines(), num_clines());
      }
      uint8_t num_branches() const noexcept { return _num_branches; }

     protected:
      uint32_t     num_clines() const noexcept { return _num_cline; }
      ptr_address* clines() noexcept
      {
         return reinterpret_cast<ptr_address*>(tail()) - num_clines();
      }
      const ptr_address* clines() const noexcept
      {
         return reinterpret_cast<const ptr_address*>(tail()) - num_clines();
      }
      ptr_address*       clines_tail() noexcept { return reinterpret_cast<ptr_address*>(tail()); }
      const ptr_address* clines_tail() const noexcept { return (const ptr_address*)(tail()); }
      const uint8_t*     divisions() const noexcept { return _prefix + _prefix_cap; }
      uint8_t*           divisions() noexcept { return _prefix + _prefix_cap; }
      uint32_t           num_divisions() const noexcept { return num_branches() - 1; }
      const branch*      branches() const noexcept
      {
         return (const branch*)(divisions() + num_divisions());
      }
      branch*       branches() noexcept { return (branch*)(divisions() + num_divisions()); }
      const branch* branches_end() const noexcept { return branches() + num_branches(); }

     private:
      uint64_t _descendents : 39;  // 500 billion keys max
      uint64_t _prefix_cap : 10;   /// TODO: may be able to reuse this for num_branches
      uint64_t _prefix_size : 10;
      uint64_t _num_cline : 5;  /// only 16 cline are possible w/ 4 bit branch index
      uint8_t  _num_branches;
      uint8_t  _prefix[];

      /** 
          uint8_t divisions[_numbranches - 1]
          branch branches[_numbranches] |
          -- -a-- - | ... spare space... 
          ptr_address clines[_numcline]
          tail()
      */
   } __attribute__((packed));
   static_assert(sizeof(inner) == 21);
}  // namespace psitri
