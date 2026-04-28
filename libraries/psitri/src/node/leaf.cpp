#include <algorithm>  // Required for std::max, std::abs
#include <cassert>    // Required for assert
#include <cstdlib>    // Required for std::abs on int (safer than cmath ambiguity)
#include <psitri/node/leaf.hpp>
#include "ucc/fast_memcpy.hpp"

// All leaf_node methods work with intentionally unaligned packed data.
// When PSITRI_PLATFORM_OPTIMIZATIONS is on, suppress alignment sanitizer for the entire file.
#if PSITRI_PLATFORM_OPTIMIZATIONS && defined(__clang__)
#pragma clang attribute push(__attribute__((no_sanitize("alignment"))), apply_to = function)
#define PSITRI_LEAF_ALIGNMENT_PUSH 1
#endif

namespace psitri
{
   namespace
   {
      value_type source_value_for_leaf_copy(const leaf_node&                  src,
                                            branch_number                     bn,
                                            const op::leaf_value_rewrite* rewrite)
      {
         if (rewrite && rewrite->value)
            return rewrite->value(rewrite->ctx, src, bn);
         return src.get_value(bn);
      }
   }  // namespace

   /**
    * simulate lower_bound and track how often each position is accessed
    */
   constexpr void lower_bound_idx_pos(int n, int key, uint16_t* table)
   {
      int left  = -1;
      int right = n;
      while (right - left > 1)
      {
         int middle = (left + right) >> 1;
         table[middle] += 256;
         if (middle < key)
            left = middle;
         else
            right = middle;
      }
   }

   /**
    * create a table of the number of times each position is accessed
    * when simulating lower_bound for all the different index sizes,
    * this is used to determine the optimal order for the keys to be
    * laid out in the node so that they are accessed in a linear,
    * cache-friendly manner.
    */
   constexpr std::array<uint8_t, (256 * 255) / 2> create_table()
   {
      std::array<uint8_t, (256 * 255) / 2> sequence_table;
      uint16_t                             table[256];
      for (int i = 1; i < 256; ++i)
      {
         auto* tbl_pos = sequence_table.data() + (i * (i - 1)) / 2;
         for (int x = 0; x < i; ++x)
            table[x] = x;
         for (int x = 0; x < i; ++x)
            lower_bound_idx_pos(i, x, table);

         std::stable_sort(table, table + i, [](auto a, auto b) { return (a >> 8) > (b >> 8); });
         std::reverse(table, table + i);
         for (int x = 0; x < i; ++x)
            tbl_pos[x] = table[x] & 0xff;
      }
      return sequence_table;
   }

   static const auto search_seq_table = create_table();

   void leaf_node::set_branch_version(branch_number bn, uint64_t version) noexcept
   {
      if (version == 0)
         return;
      auto idx = add_version(version);
      set_ver_index(bn, idx);
      assert(free_space() >= 0);
   }

   void leaf_node::copy_branch_version_from(const leaf_node& src,
                                            branch_number    src_bn,
                                            branch_number    dst_bn) noexcept
   {
      if (src.get_ver_index(src_bn) == 0xFF)
         return;
      set_branch_version(dst_bn, src.get_version(src_bn));
   }

   leaf_node::leaf_node(size_t            alloc_size,
                        ptr_address_seq   seq,
                        key_view          key,
                        const value_type& value)
       : node(alloc_size, node_type::leaf, seq),
         _alloc_pos(0),
         _dead_space(0),
         _cline_cap(0),
         _optimal_layout(true)
   {
      _num_versions = 0;
      set_num_branches(0);  // Must initialize before lower_bound reads it
      apply(op::leaf_insert{*this, lower_bound(key), key, value});
   }
   uint32_t leaf_node::compact_size() const noexcept
   {
      // Only shrink when there's no dead space — dead space means the alloc area
      // contains gaps that can't be skipped with a simple memcpy. The next COW
      // will rebuild via clone_from to eliminate dead space, then compaction can shrink.
      if (_dead_space > 0)
         return size();
      uint32_t head_size = meta_end() - (const char*)this;
      uint32_t result =
          ucc::round_up_multiple<64>(head_size) + ucc::round_up_multiple<64>(_alloc_pos);
      // Floor: at least one cacheline for a valid node
      result = std::max<uint32_t>(result, 64u);
      return std::min<uint32_t>(result, size());
   }

   void leaf_node::compact_to(alloc_header* compact_dst) const noexcept
   {
      assert(compact_dst->size() == compact_size());
      if (compact_dst->size() == size())
      {
         // Same size: fast full copy
         ucc::memcpy_aligned_64byte(compact_dst, this, size());
         return;
      }
      // Different sizes: copy header+arrays forward, alloc area backward.
      // Save the destination's alloc_header (size, address_seq set by allocator)
      auto saved_header = *compact_dst;

      uint32_t head_bytes = meta_end() - (const char*)this;
      // Copy fixed header + dynamic arrays (everything from start to meta_end)
      memcpy(compact_dst, this, head_bytes);
      // Restore the alloc_header fields
      memcpy(compact_dst, &saved_header, sizeof(alloc_header));

      // Copy alloc area (grows backward from tail)
      auto* dst = reinterpret_cast<leaf_node*>(compact_dst);
      memcpy((char*)dst->tail() - _alloc_pos, (const char*)tail() - _alloc_pos, _alloc_pos);
   }

   void leaf_node::clone_from(const leaf_node* clone)
   {
      //    SAL_ERROR("cloning from {} {} to {} {}", clone->address(), clone, address(), this);
      PSITRI_ASSERT_INVARIANTS(clone->validate_invariants());
      set_num_branches(clone->num_branches());
      // Rebuild from scratch when there is dead space to reclaim, or when the
      // cline table is full — rebuilding compacts clines by only keeping those
      // actually referenced, which frees slots for subsequent update_value().
      if (clone->dead_space() || clone->_cline_cap >= 16)
      {
         // let the compactor do this work, it has a major slowdown of updates/inserts when
         // put in the critical path.
         //if (not clone->is_optimal_layout())
         {
            //         SAL_ERROR("cloning to optimal layout, num_branches: {}", num_branches());
            _alloc_pos      = 0;
            _dead_space     = 0;
            _cline_cap      = 0;
            _num_versions   = 0;
            _optimal_layout = true;

            const uint16_t nb = clone->num_branches();
            /// copy the key hashes
            memcpy(_key_hashs, clone->_key_hashs, nb * sizeof(uint8_t));

            /// copy the keys in the optimal layout order
            const uint8_t* seq = search_seq_table.data() + ((nb - 1) * nb) / 2;
            auto           kos = keys_offsets();

            /// allocate the keys in the optimal layout order
            for (uint16_t x = nb; x-- > 0;)
            {
               auto idx = seq[x];
               kos[idx] = alloc_key(clone->get_key(branch_number(idx)));
            }

            auto vos       = value_offsets();
            auto clone_vos = clone->value_offsets();

            // now copy the values in order, adding address pointers in order
            // this has to be forward order so that address_ptrs() are added in order
            for (uint16_t x = 0; x < nb; ++x)
            {
               value_type val = clone->get_value(branch_number(x));
               if (val.is_view())
               {
                  if (val.view().empty())
                     vos[x] = value_branch();
                  else
                     vos[x] = alloc_value(val.view());
               }
               else if (val.is_subtree())
                  vos[x] = add_address_ptr(value_type_flag::subtree, val.subtree_address());
               else if (val.is_value_node())
                  vos[x] = add_address_ptr(value_type_flag::value_node, val.value_address());
               else
                  vos[x] = value_branch();
            }
            for (uint16_t x = 0; x < nb; ++x)
               copy_branch_version_from(*clone, branch_number(x), branch_number(x));
            assert(is_optimal_layout());
            return;
         }
      }

      _alloc_pos      = clone->_alloc_pos;
      _cline_cap      = clone->_cline_cap;
      _dead_space     = clone->_dead_space;
      _num_versions   = clone->_num_versions;
      _optimal_layout = clone->_optimal_layout;

      assert(free_space() >= 0);

      // We cannot use the faster aligned memcpy unless we are copying from a leaf node
      // that has an optimal layout, otherwise we could end up copying data that is not
      // aligned to the cacheline size.
      // Copy everything from _key_hashs through meta_end (includes clines + ver_indices + version_table)
      auto head_size = clone->meta_end() - (const char*)clone->_key_hashs;

      memcpy(_key_hashs, clone->_key_hashs, head_size);

      auto  apos = ucc::round_up_multiple<64>(_alloc_pos);
      char* aptr = ((char*)tail()) - apos;
      //SAL_ERROR(" this.size:{} clone.size:{}", size(), clone->size());
      //SAL_ERROR(" memcpy_aligned_64byte( {}, {}, {})", aptr, ((char*)clone->tail()) - apos, apos);

      // in order to use the faster, aligned memcpy we need to handle the case where this would
      // overwrite the first 64 bytes of the node where the header info is stored. Using the slower
      // memcpy to copy exactly what is needed solves the problem for now.
      //ucc::memcpy_aligned_64byte(aptr, ((char*)clone->tail()) - apos, apos);

      memcpy(alloc_head(), clone->alloc_head(), clone->_alloc_pos);
      PSITRI_ASSERT_INVARIANTS(validate_invariants());
   }

   /// TODO: this could be made more effecient by copying and inserting in one pass
   /// and maintaining the optimal layout property, but for now it will just clone/opt
   /// then insert
   leaf_node::leaf_node(size_t                 alloc_size,
                        ptr_address_seq        seq,
                        const leaf_node*       clone,
                        const op::leaf_insert& ins)
       : node(alloc_size, node_type::leaf, seq),
         _alloc_pos(0),
         _dead_space(0),
         _cline_cap(0),
         _optimal_layout(true)
   {
      _num_versions = 0;
      if (clone->num_versions() || ins.created_at || ins.rewrite)
      {
         PSITRI_ASSERT_INVARIANTS(clone->validate_invariants());
         const uint16_t src_nb = clone->num_branches();
         const uint16_t dst_nb = src_nb + 1;
         const uint16_t ins_bn = *ins.lb;
         set_num_branches(dst_nb);

         const uint8_t* seq_table = search_seq_table.data() + ((dst_nb - 1) * dst_nb) / 2;
         auto           kos       = keys_offsets();
         auto           kh        = key_hashs();
         for (uint16_t x = dst_nb; x-- > 0;)
         {
            auto     dst_idx = seq_table[x];
            key_view dst_key = (dst_idx == ins_bn)
                                   ? ins.key
                                   : clone->get_key(branch_number(dst_idx - (dst_idx > ins_bn)));
            kos[dst_idx]     = alloc_key(dst_key);
            kh[dst_idx]      = calc_key_hash(dst_key);
         }

         auto vos = value_offsets();
         for (uint16_t dst_idx = 0; dst_idx < dst_nb; ++dst_idx)
         {
            value_type val =
                (dst_idx == ins_bn)
                    ? ins.value
                    : source_value_for_leaf_copy(
                          *clone, branch_number(dst_idx - (dst_idx > ins_bn)), ins.rewrite);
            if (val.is_view())
            {
               if (val.view().empty())
                  vos[dst_idx] = value_branch();
               else
                  vos[dst_idx] = alloc_value(val.view());
            }
            else if (val.is_subtree())
               vos[dst_idx] = add_address_ptr(value_type_flag::subtree, val.subtree_address());
            else if (val.is_value_node())
               vos[dst_idx] = add_address_ptr(value_type_flag::value_node, val.value_address());
            else
               vos[dst_idx] = value_branch();
         }

         for (uint16_t dst_idx = 0; dst_idx < dst_nb; ++dst_idx)
         {
            if (dst_idx == ins_bn)
               set_branch_version(branch_number(dst_idx), ins.created_at);
            else
               copy_branch_version_from(*clone, branch_number(dst_idx - (dst_idx > ins_bn)),
                                        branch_number(dst_idx));
         }
         assert(is_optimal_layout());
         PSITRI_ASSERT_INVARIANTS(validate_invariants());
         return;
      }
      clone_from(clone);
      apply(ins);
   }

   leaf_node::leaf_node(size_t alloc_size, ptr_address_seq seq, const op::leaf_update& upd)
       : node(alloc_size, node_type::leaf, seq),
         _alloc_pos(0),
         _dead_space(0),
         _cline_cap(0),
         _optimal_layout(true)
   {
      _num_versions = 0;
      // Rebuild from scratch, substituting the new value at upd.lb during the copy.
      // This avoids clone_from + update_value which would carry the old value as dead
      // space — causing overflow when the leaf is already near capacity.
      const leaf_node* clone = &upd.src;
      PSITRI_ASSERT_INVARIANTS(clone->validate_invariants());
      const uint16_t nb = clone->num_branches();
      set_num_branches(nb);

      memcpy(_key_hashs, clone->_key_hashs, nb * sizeof(uint8_t));

      const uint8_t* seq_table = search_seq_table.data() + ((nb - 1) * nb) / 2;
      auto           kos       = keys_offsets();
      for (uint16_t x = nb; x-- > 0;)
      {
         auto idx = seq_table[x];
         kos[idx] = alloc_key(clone->get_key(branch_number(idx)));
      }

      auto vos = value_offsets();
      for (uint16_t x = 0; x < nb; ++x)
      {
         // Substitute the new value at the updated branch
         value_type val = (x == *upd.lb)
                              ? upd.value
                              : source_value_for_leaf_copy(*clone, branch_number(x), upd.rewrite);
         if (val.is_view())
         {
            if (val.view().empty())
               vos[x] = value_branch();
            else
               vos[x] = alloc_value(val.view());
         }
         else if (val.is_subtree())
            vos[x] = add_address_ptr(value_type_flag::subtree, val.subtree_address());
         else if (val.is_value_node())
            vos[x] = add_address_ptr(value_type_flag::value_node, val.value_address());
         else
            vos[x] = value_branch();
      }
      for (uint16_t x = 0; x < nb; ++x)
         copy_branch_version_from(*clone, branch_number(x), branch_number(x));
      assert(is_optimal_layout());
      PSITRI_ASSERT_INVARIANTS(validate_invariants());
   }

   leaf_node::leaf_node(size_t alloc_size, ptr_address_seq seq, const op::leaf_remove& rm)
       : node(alloc_size, node_type::leaf, seq),
         _alloc_pos(0),
         _dead_space(0),
         _cline_cap(0),
         _optimal_layout(true)
   {
      _num_versions = 0;
      if (rm.src.num_versions() || rm.rewrite)
      {
         const leaf_node& src    = rm.src;
         const uint16_t   src_nb = src.num_branches();
         const uint16_t   dst_nb = src_nb - 1;
         set_num_branches(dst_nb);
         if (dst_nb == 0)
            return;

         const uint8_t* seq_table = search_seq_table.data() + ((dst_nb - 1) * dst_nb) / 2;
         auto           kos       = keys_offsets();
         auto           kh        = key_hashs();
         for (uint16_t x = dst_nb; x-- > 0;)
         {
            auto     dst_idx = seq_table[x];
            auto     src_idx = dst_idx + (dst_idx >= *rm.bn);
            key_view key     = src.get_key(branch_number(src_idx));
            kos[dst_idx]     = alloc_key(key);
            kh[dst_idx]      = calc_key_hash(key);
         }

         auto vos = value_offsets();
         for (uint16_t dst_idx = 0; dst_idx < dst_nb; ++dst_idx)
         {
            auto       src_idx = dst_idx + (dst_idx >= *rm.bn);
            value_type val =
                source_value_for_leaf_copy(src, branch_number(src_idx), rm.rewrite);
            if (val.is_view())
            {
               if (val.view().empty())
                  vos[dst_idx] = value_branch();
               else
                  vos[dst_idx] = alloc_value(val.view());
            }
            else if (val.is_subtree())
               vos[dst_idx] = add_address_ptr(value_type_flag::subtree, val.subtree_address());
            else if (val.is_value_node())
               vos[dst_idx] = add_address_ptr(value_type_flag::value_node, val.value_address());
            else
               vos[dst_idx] = value_branch();
            copy_branch_version_from(src, branch_number(src_idx), branch_number(dst_idx));
         }
         PSITRI_ASSERT_INVARIANTS(validate_invariants());
         return;
      }
      /// TODO: could be more effecient by copying node values in order and just skipping the
      /// removed branch, it would leave the leaf in a better layout without dead space
      /// but putting this work onto the compactor might make more sense.
      clone_from(&rm.src);
      apply(rm);
   }

   leaf_node::leaf_node(size_t alloc_size, ptr_address_seq seq, const op::leaf_remove_range& rm)
       : node(alloc_size, node_type::leaf, seq),
         _alloc_pos(0),
         _dead_space(0),
         _cline_cap(0),
         _optimal_layout(true)
   {
      _num_versions = 0;
      if (rm.src.num_versions() || rm.rewrite)
      {
         const leaf_node& src    = rm.src;
         const uint16_t   dst_nb = src.num_branches() - (*rm.hi - *rm.lo);
         set_num_branches(dst_nb);
         if (dst_nb == 0)
            return;

         const uint8_t* seq_table     = search_seq_table.data() + ((dst_nb - 1) * dst_nb) / 2;
         auto           kos           = keys_offsets();
         auto           kh            = key_hashs();
         auto           src_index_for = [&](uint16_t dst_idx)
         { return uint16_t(dst_idx + (dst_idx >= *rm.lo) * (*rm.hi - *rm.lo)); };
         for (uint16_t x = dst_nb; x-- > 0;)
         {
            auto     dst_idx = seq_table[x];
            auto     src_idx = src_index_for(dst_idx);
            key_view key     = src.get_key(branch_number(src_idx));
            kos[dst_idx]     = alloc_key(key);
            kh[dst_idx]      = calc_key_hash(key);
         }

         auto vos = value_offsets();
         for (uint16_t dst_idx = 0; dst_idx < dst_nb; ++dst_idx)
         {
            auto       src_idx = src_index_for(dst_idx);
            value_type val =
                source_value_for_leaf_copy(src, branch_number(src_idx), rm.rewrite);
            if (val.is_view())
            {
               if (val.view().empty())
                  vos[dst_idx] = value_branch();
               else
                  vos[dst_idx] = alloc_value(val.view());
            }
            else if (val.is_subtree())
               vos[dst_idx] = add_address_ptr(value_type_flag::subtree, val.subtree_address());
            else if (val.is_value_node())
               vos[dst_idx] = add_address_ptr(value_type_flag::value_node, val.value_address());
            else
               vos[dst_idx] = value_branch();
            copy_branch_version_from(src, branch_number(src_idx), branch_number(dst_idx));
         }
         PSITRI_ASSERT_INVARIANTS(validate_invariants());
         return;
      }
      // Clone the source then remove the range in-place.
      // This is simple and correct; the compactor can optimize layout later.
      clone_from(&rm.src);
      remove_range(rm.lo, rm.hi);
   }

   leaf_node::leaf_node(size_t alloc_size, ptr_address_seq seq, const op::leaf_prepend_prefix& pp)
       : node(alloc_size, node_type::leaf, seq),
         _alloc_pos(0),
         _dead_space(0),
         _cline_cap(0),
         _optimal_layout(true)
   {
      _num_versions        = 0;
      const leaf_node& src = pp.src;
      const uint16_t   nb  = src.num_branches();
      set_num_branches(nb);

      if (nb == 0)
         return;

      const uint8_t* aseq = search_seq_table.data() + ((nb - 1) * nb) / 2;
      auto           kos  = keys_offsets();
      auto           kh   = key_hashs();

      // Stack buffer for key with prepended prefix
      char key_buf[2048];
      assert(pp.prefix.size() < 1024);
      memcpy(key_buf, pp.prefix.data(), pp.prefix.size());

      // Allocate keys in optimal layout order (reverse for allocation)
      for (uint16_t x = nb; x-- > 0;)
      {
         auto idx      = aseq[x];
         auto orig_key = src.get_key(branch_number(idx));
         memcpy(key_buf + pp.prefix.size(), orig_key.data(), orig_key.size());
         key_view new_key(key_buf, pp.prefix.size() + orig_key.size());
         kos[idx] = alloc_key(new_key);
         kh[idx]  = calc_key_hash(new_key);
      }

      // Copy values in forward order
      auto vos = value_offsets();
      for (uint16_t x = 0; x < nb; ++x)
      {
         value_type val = source_value_for_leaf_copy(src, branch_number(x), pp.rewrite);
         if (val.is_view())
         {
            if (val.view().empty())
               vos[x] = value_branch();
            else
               vos[x] = alloc_value(val.view());
         }
         else if (val.is_subtree())
            vos[x] = add_address_ptr(value_type_flag::subtree, val.subtree_address());
         else if (val.is_value_node())
            vos[x] = add_address_ptr(value_type_flag::value_node, val.value_address());
         else
            vos[x] = value_branch();
      }
      for (uint16_t x = 0; x < nb; ++x)
         copy_branch_version_from(src, branch_number(x), branch_number(x));
      PSITRI_ASSERT_INVARIANTS(validate_invariants());
   }
   /*
    * Each node has an optimal layout that looks something like this:
    *   1. no dead space
    *   2. all keys are grouped together
    *   3. all keys are ordered by lower-bound search visit probability
    *   4. clines are sorted based upon order they appear in sorted keys,
    *      so the prefetcher can get a jump on the most important lines
    *      first. 
    *
    *  During normal updates these properties are not maintained because 
    *  doing so would be expensive for the insert/update/remove operations; however,
    *  when it comes time to copy a node we can choose to restore the optimal layout
    *  or not.  memcpy() is going to be much faster than the optimal layout code,
    *  but he delta cost vs memcpy() of the entire node is much less than the 
    *  delta cost of doing it at any other time. 
    */
   leaf_node::leaf_node(size_t alloc_size, ptr_address_seq seq, const leaf_node* clone)
       : node(alloc_size, node_type::leaf, seq)
   {
      clone_from(clone);
   }

   leaf_node::leaf_node(size_t           alloc_size,
                        ptr_address_seq  seq,
                        const leaf_node* clone,
                        const op::leaf_value_rewrite* rewrite)
       : leaf_node(alloc_size,
                   seq,
                   clone,
                   key_view(),
                   branch_zero,
                   branch_number(clone->num_branches()),
                   rewrite)
   {
   }

   /**
    *  Construct an optimized node by cloning the existing node and truncating keys
    *  by the common prefix and only including keys in the range [start, end).
    */
   leaf_node::leaf_node(size_t           alloc_size,
                        ptr_address_seq  seq,
                        const leaf_node* clone,
                        key_view         cprefix,
                        branch_number    start,
                        branch_number    end,
                        const op::leaf_value_rewrite* rewrite)
       : node(alloc_size, node_type::leaf, seq)
   {
      _alloc_pos      = 0;
      _dead_space     = 0;
      _cline_cap      = 0;
      _num_versions   = 0;
      _optimal_layout = true;
      set_num_branches(*end - *start);
      auto nb = num_branches();

      const uint8_t* aseq = search_seq_table.data() + ((nb - 1) * nb) / 2;
      auto           kos  = keys_offsets();
      auto           kh   = key_hashs();

      /// allocate the keys in the optimal layout order
      for (uint16_t x = *end; x-- > *start;)
      {
         auto idx = aseq[x - *start];  // dest index is relative to 0
         /// src idx is relative to start
         auto key = clone->get_key(branch_number(idx + *start)).substr(cprefix.size());
         kos[idx] = alloc_key(key);
         kh[idx]  = calc_key_hash(key);
      }

      auto vos       = value_offsets();
      auto clone_vos = clone->value_offsets();

      // now copy the values in order, adding address pointers in order
      // this has to be forward order so that address_ptrs() are added in order
      for (uint16_t x = *start; x < *end; ++x)
      {
         value_type val = source_value_for_leaf_copy(*clone, branch_number(x), rewrite);
         if (val.is_view())
         {
            if (val.view().empty())
               vos[x - *start] = value_branch();
            else
               vos[x - *start] = alloc_value(val.view());
         }
         else if (val.is_subtree())
            vos[x - *start] = add_address_ptr(value_type_flag::subtree, val.subtree_address());
         else if (val.is_value_node())
            vos[x - *start] = add_address_ptr(value_type_flag::value_node, val.value_address());
         else
            vos[x - *start] = value_branch();
      }
      for (uint16_t x = *start; x < *end; ++x)
         copy_branch_version_from(*clone, branch_number(x), branch_number(x - *start));
      PSITRI_ASSERT_INVARIANTS(validate_invariants());
   }

   /*
   leaf_node::leaf_node(size_t            alloc_size,
                        ptr_address_seq   seq,
                        const leaf_node*  clone,
                        key_view          ins,
                        const value_type& value)
       : node(alloc_size, node_type::leaf, seq)
   {
      // TODO: implement
   }
   */

   leaf_node::leaf_node(size_t           alloc_size,
                        ptr_address_seq  seq,
                        const leaf_node* clone,
                        branch_number    bn)
       : node(alloc_size, node_type::leaf, seq)
   {
      // TODO: implement
   }

   void leaf_node::entry_inserter::add(key_view key, value_type val)
   {
      auto kos = _leaf.keys_offsets();
      auto kh  = _leaf.key_hashs();
      auto vos = _leaf.value_offsets();

      kos[_idx] = _leaf.alloc_key(key);
      kh[_idx]  = _leaf.calc_key_hash(key);

      if (val.is_view())
      {
         if (val.view().empty())
            vos[_idx] = value_branch();
         else
            vos[_idx] = _leaf.alloc_value(val.view());
      }
      else if (val.is_subtree())
         vos[_idx] = _leaf.add_address_ptr(value_type_flag::subtree, val.subtree_address());
      else if (val.is_value_node())
         vos[_idx] = _leaf.add_address_ptr(value_type_flag::value_node, val.value_address());
      else
         vos[_idx] = value_branch();

      ++_idx;
   }

   leaf_node::leaf_node(size_t alloc_size, ptr_address_seq seq, const op::leaf_from_visitor& vis)
       : node(alloc_size, node_type::leaf, seq),
         _alloc_pos(0),
         _dead_space(0),
         _cline_cap(0),
         _optimal_layout(true)
   {
      _num_versions = 0;
      set_num_branches(vis.count);
      if (vis.count == 0)
         return;

      entry_inserter ins(*this);
      vis.init(ins, vis.ctx);

      assert(ins._idx == vis.count);
      PSITRI_ASSERT_INVARIANTS(validate_invariants());
   }

   leaf_node::can_apply_mode leaf_node::can_apply(const op::leaf_insert& ins) const noexcept
   {
      assert(ins.key.size() <= 1024);  // TODO: max key size constant
      assert(not ins.value.is_remove());
      // key hash(1), key_offset(2), value_branch(2)
      size_t size_required = sizeof(uint8_t) + sizeof(key_offset) + sizeof(value_branch);
      if (ins.value.is_view())
      {
         assert(ins.value.view().size() <= 0xffff);
         value_view data = ins.value.view();
         size_required += data.size() + sizeof(value_data);
      }
      size_required += ins.key.size() + 2;
      /// this over-estimates assuming worst case we must add a cline, but
      /// the calculating whether address() is on an existing cline requires
      /// scanning all clines to see if we can re-use one or have to add a new one.
      size_required += 4 * ins.value.is_address();
      if (_num_versions || ins.created_at)
      {
         size_required += 1;  // one ver_indices entry for the inserted branch
         if (_num_versions == 0)
            size_required += num_branches();
         bool existing_version = false;
         for (uint8_t i = 0; i < _num_versions; ++i)
            existing_version |= version_table()[i].get() == ins.created_at;
         if (ins.created_at && !existing_version)
            size_required += sizeof(version48);
      }
      int leftover = free_space() - size_required;
      // SAL_WARN("size_required: {}  free_space: {}  leftover: {}", size_required, free_space(),
      //          leftover);
      if (leftover >= 0 && _num_versions == 0 && ins.created_at == 0)
         return can_apply_mode::modify;
      if (leftover + int(dead_space()) + (int(leaf_node::cow_size()) - int(size())) >= 0)
      {
         // SAL_WARN("leftover: {}  dead_space: {}  cow_size: {}  size: {} == {}", leftover,
         //          dead_space(), leaf_node::cow_size(), size(),
         //          leftover + dead_space() + (leaf_node::cow_size() - size()));
         return can_apply_mode::defrag;
      }
      return can_apply_mode::none;
   }
   leaf_node::can_apply_mode leaf_node::can_apply(const op::leaf_update& upd) const noexcept
   {
      assert(upd.lb < num_branches());
      assert(not upd.value.is_remove());

      // Compute worst-case additional free_space consumed by update_value():
      //  - inline alloc: if the new value is a larger inline, alloc_value() grows _alloc_pos
      //  - cline growth: if the new value is address-typed and doesn't match an existing cline
      int extra = 0;

      value_branch old_vb     = value_offsets()[*upd.lb];
      bool         old_inline = old_vb.is_inline();
      size_t       old_size   = old_inline ? get_value_ptr(old_vb.offset())->get().size() : 0;

      if (upd.value.is_view())
      {
         auto v = upd.value.view();
         if (v.size() > 0 && !(old_inline && v.size() <= old_size))
            extra += v.size() + sizeof(value_data);
      }
      else if (upd.value.is_address())
      {
         // Conservative: assume a new cline slot is needed. We cannot use
         // can_insert_address() here because update_value() may remove the old
         // address's cline entry before calling add_address_ptr() for the new one,
         // invalidating any match can_insert_address() found in the pre-mutation state.
         if (_cline_cap >= 16)
            return can_apply_mode::none;  // force split; defrag rebuild can still overflow
         extra += sizeof(ptr_address);
      }

      int leftover = free_space() - extra;
      if (leftover >= 0)
         return can_apply_mode::modify;
      if (leftover + int(dead_space()) + (int(leaf_node::cow_size()) - int(size())) >= 0)
         return can_apply_mode::defrag;
      return can_apply_mode::none;
   }

   void leaf_node::apply(const op::leaf_remove& rm) noexcept
   {
      auto init_free_space = free_space();
      assert(rm.bn < num_branches());
      remove(rm.bn);
   }

   branch_number leaf_node::apply(const op::leaf_insert& ins) noexcept
   {
      auto init_free_space = free_space();
      assert(can_apply(ins) == can_apply_mode::modify);
      assert(_num_versions == 0);
      assert(ins.created_at == 0);
      assert(!ins.value.is_remove());
      assert(ins.lb == lower_bound(ins.key));
      assert(ins.lb == num_branches() or get_key(ins.lb) != ins.key);

      int      tail_len = num_branches() - *ins.lb;
      uint32_t bn       = *ins.lb;

      {
         constexpr const int move_size =
             sizeof(uint8_t) + sizeof(key_offset) + sizeof(value_branch);

         char*  vo_tail          = (char*)(value_offsets() + bn);
         size_t vo_tail_size     = tail_len * sizeof(value_branch);
         size_t clines_tail_size = _cline_cap * sizeof(ptr_address);
         memmove(vo_tail + move_size, vo_tail, vo_tail_size + clines_tail_size);

         char*  ko_tail      = (char*)(keys_offsets().data() + bn);
         size_t ko_tail_size = tail_len * sizeof(key_offset);
         size_t vo_head_size = bn * sizeof(value_branch);
         memmove(ko_tail + move_size - sizeof(value_branch), ko_tail, ko_tail_size + vo_head_size);

         char*  kh_tail      = (char*)(key_hashs().data() + bn);
         size_t kh_tail_size = tail_len * sizeof(uint8_t);
         size_t ko_head_size = bn * sizeof(key_offset);
         memmove(kh_tail + move_size - sizeof(key_offset) - sizeof(value_branch), kh_tail,
                 kh_tail_size + ko_head_size);

         set_num_branches(num_branches() + 1);
      }

      key_offset ko = alloc_key(ins.key);

      key_hashs()[bn]    = calc_key_hash(ins.key);
      keys_offsets()[bn] = ko;
      if (ins.value.is_view())
      {
         auto v = ins.value.view();
         if (v.size())
         {
            value_offset vo     = alloc_value(v);
            value_offsets()[bn] = value_branch(vo);
         }
         else
            value_offsets()[bn] = value_branch();
      }
      else if (ins.value.is_subtree())
      {
         value_offsets()[bn] =
             add_address_ptr(value_type_flag::subtree, ins.value.subtree_address());
      }
      else  //if (ins.value.is_value_node())
      {
         assert(ins.value.is_value_node());
         value_offsets()[bn] =
             add_address_ptr(value_type_flag::value_node, ins.value.value_address());
      }
      /// if there is only one branch then you cannot get more optimal, if
      /// there is more than one branch, then the optimal layout would be to
      /// have all the keys allocated together, separate from the values,
      /// and the keys should be sorted in a cache-friendly manner according to
      /// the traversal order of the tree.
      _optimal_layout = num_branches() == 1;

      assert(free_space() >= 0);
      assert(get_key(get(ins.key)) == ins.key);
      // kSAL_WARN("insert:{} = {}  get_value:{}", ins.key, ins.value, get_value(ins.lb));
      assert(get_value(ins.lb) == ins.value);
      PSITRI_ASSERT_INVARIANTS(validate_invariants());
      return ins.lb;
   }

   void leaf_node::remove(branch_number bn) noexcept
   {
      //SAL_ERROR("remove:{} key:{} size: {}", bn, get_key(bn), get_key(bn).size());
      assert(bn < num_branches());

      value_branch vb = value_offsets()[*bn];
      key_offset   ko = keys_offsets()[*bn];

      // 1. Free space accounting
      _dead_space += sizeof(key) + get_key_ptr(ko)->get().size();
      if (vb.is_inline())
      {
         value_offset vo = vb.offset();
         //SAL_ERROR("    value size: {}", get_value_ptr(vo)->get().size());
         _dead_space += sizeof(value_data) + get_value_ptr(vo)->get().size();
      }
      else if (vb.is_address())
      {
         // 2. Cline Management (if address type)
         cline_offset cl_off    = vb.cline();
         int          ref_count = 0;
         for (uint16_t i = 0; i < num_branches() and ref_count < 2; ++i)
         {
            value_branch other_vb = value_offsets()[i];
            ref_count += other_vb.is_address() && other_vb.cline() == cl_off;
         }

         if (ref_count == 1)  // the one being removed
         {
            // This was the last branch using this cline, mark it as free
            clines()[*cl_off] = sal::null_ptr_address;
            // if the last branch was removed, decrement the cline count
            _cline_cap -= (*cl_off == _cline_cap - 1);
         }
      }
      //
      // | khash[bn+1] -> koff[bn]  move back -1 byte
      // | koff[bn+1] -> voff[bn]  move back -3 byte
      // | voff[bn+1] -> clines[_cline_cap]  move back -5 byte

      auto meta = get_meta_arrays();
      auto bn1  = *bn + 1;
      // pointers to the start of the data to be moved
      auto khash_bn1  = meta.khash + bn1;
      auto koff_bn    = meta.koffs + *bn * sizeof(key_offset);
      auto koff_bn1   = meta.koffs + bn1 * sizeof(key_offset);
      auto voff_bn    = meta.voffs + *bn * sizeof(value_branch);
      auto voff_bn1   = meta.voffs + bn1 * sizeof(value_branch);
      auto clines_end = meta.clines_end;

      memmove(khash_bn1 - 1, khash_bn1, koff_bn - khash_bn1);
      memmove(koff_bn1 - 3, koff_bn1, voff_bn1 - koff_bn1);
      memmove(voff_bn1 - 5, voff_bn1, clines_end - voff_bn1);
      // 4. Decrement Branch Count
      set_num_branches(num_branches() - 1);

      // 5. Update Optimal Flag
      _optimal_layout = false;  // Removing always breaks optimal layout
      PSITRI_ASSERT_INVARIANTS(validate_invariants());
   }

   void leaf_node::remove_range(branch_number lo, branch_number hi) noexcept
   {
      assert(*lo < *hi && *hi <= num_branches());
      uint16_t count = *hi - *lo;

      // 1. Free space accounting and cline cleanup for removed branches
      for (uint16_t i = *lo; i < *hi; ++i)
      {
         key_offset ko = keys_offsets()[i];
         _dead_space += sizeof(key) + get_key_ptr(ko)->get().size();

         value_branch vb = value_offsets()[i];
         if (vb.is_inline())
         {
            value_offset vo = vb.offset();
            _dead_space += sizeof(value_data) + get_value_ptr(vo)->get().size();
         }
         else if (vb.is_address())
         {
            cline_offset cl_off = vb.cline();
            // Count references to this cline from branches NOT being removed
            int ref_count = 0;
            for (uint16_t j = 0; j < num_branches() && ref_count < 2; ++j)
            {
               if (j >= *lo && j < *hi)
                  continue;  // skip branches being removed
               value_branch other_vb = value_offsets()[j];
               ref_count += other_vb.is_address() && other_vb.cline() == cl_off;
            }
            if (ref_count == 0)
            {
               // Access via raw pointer to avoid span bounds issues: _cline_cap must not
               // be shrunk mid-loop because a later branch may share the same cline index.
               auto* cl_ptr    = reinterpret_cast<ptr_address*>(value_offsets() + num_branches());
               cl_ptr[*cl_off] = sal::null_ptr_address;
            }
         }
      }
      // Trim _cline_cap: remove trailing null clines freed above.
      {
         auto* cl_ptr = reinterpret_cast<ptr_address*>(value_offsets() + num_branches());
         while (_cline_cap > 0 && cl_ptr[_cline_cap - 1] == sal::null_ptr_address)
            --_cline_cap;
      }

      // 2. Shift metadata arrays to close the gap
      //    Layout: khash[0..nb) | koff[0..nb) | voff[0..nb) | clines[0.._cline_cap)
      //    Removing [lo, hi) shifts 3 sub-arrays by different amounts
      auto     meta = get_meta_arrays();
      uint16_t nb   = num_branches();

      // Pointers for the range being removed
      auto khash_hi   = meta.khash + *hi;
      auto koff_lo    = meta.koffs + *lo * sizeof(key_offset);
      auto koff_hi    = meta.koffs + *hi * sizeof(key_offset);
      auto voff_lo    = meta.voffs + *lo * sizeof(value_branch);
      auto voff_hi    = meta.voffs + *hi * sizeof(value_branch);
      auto clines_end = meta.clines_end;

      // khash: shift [hi..nb) back by count bytes
      memmove(khash_hi - count, khash_hi, koff_lo - khash_hi);
      // koff: shift [hi..nb) back by count*3 bytes (count in khash + count*sizeof(key_offset))
      uint16_t shift2 = count + count * sizeof(key_offset);
      memmove(koff_hi - shift2, koff_hi, voff_lo - koff_hi);
      // voff+clines: shift [hi..end) back by count*(1+2+2) = count*5 bytes
      uint16_t shift3 = count + count * sizeof(key_offset) + count * sizeof(value_branch);
      memmove(voff_hi - shift3, voff_hi, clines_end - voff_hi);

      set_num_branches(nb - count);
      _optimal_layout = false;
      PSITRI_ASSERT_INVARIANTS(validate_invariants());
   }

   size_t leaf_node::update_value(branch_number bn, const value_type& value) noexcept
   {
      assert(bn < num_branches());
      assert(not value.is_remove());
      //SAL_INFO("update: {} = '{}'", int(*bn), value);

      size_t        old_size = 0;
      value_branch& vb       = value_offsets()[*bn];
      if (vb.is_inline())
      {
         auto vp  = get_value_ptr(vb.offset());
         old_size = vp->get().size();
         if (value.is_view())
         {
            auto v = value.view();
            if (v.size() == 0)
            {
               vb = value_branch();
               _dead_space += old_size + sizeof(value_data);
               _optimal_layout = false;
            }
            else if (v.size() <= old_size)
            {
               vp->set(value.view());
               _dead_space += old_size - v.size();
               _optimal_layout &= v.size() == old_size;
            }
            else
            {
               vb = alloc_value(v);
               _dead_space += old_size + sizeof(value_data);
               _optimal_layout = false;
            }
         }
         else if (value.is_subtree())
         {
            vb = add_address_ptr(value_type_flag::subtree, value.subtree_address());
            _dead_space += old_size + sizeof(value_data);
            _optimal_layout = false;
         }
         else if (value.is_value_node())
         {
            vb = add_address_ptr(value_type_flag::value_node, value.value_address());
            _dead_space += old_size + sizeof(value_data);
            _optimal_layout = false;
         }
         return old_size;
      }
      else if (vb.is_address())
      {
         old_size            = sizeof(ptr_address);
         value_branch old_vb = vb;
         assert(old_vb.cline() == vb.cline());
         vb = value_branch();
         remove_address_ptr(old_vb.cline());
         _optimal_layout = false;
      }
      else  // vb.is_null()
      {
         old_size = 0;
         if (value.is_view())
         {
            auto v = value.view();
            if (v.size() == 0)
               return old_size;
         }
      }

      if (value.is_view())
      {
         auto v = value.view();
         if (v.size() == 0)
            vb = value_branch();
         else
            vb = alloc_value(v);
      }
      else if (value.is_subtree())
      {
         vb = add_address_ptr(value_type_flag::subtree, value.subtree_address());
      }
      else if (value.is_value_node())
      {
         vb = add_address_ptr(value_type_flag::value_node, value.value_address());
      }
      _optimal_layout = false;
      assert(get_value(bn) == value);
      PSITRI_ASSERT_INVARIANTS(validate_invariants());
      return old_size;
   }

   bool leaf_node::can_insert_address(ptr_address addr) const noexcept
   {
      ptr_address base_cline(*addr & ~0x0f);
      const auto  cls            = clines();
      bool        found_existing = false;
      // TODO: there is a scalar algo that can do this 2x at a time
      // there is a neon algo that can do this 4x at a time
      // there is a AVX2 algo that can do this 8x at a time
      for (int i = 0; i < cls.size(); ++i)
         found_existing |= (cls[i] == base_cline) | (sal::null_ptr_address == cls[i]);
      if (found_existing)
         return true;
      // No matching or empty slot — must grow _cline_cap.
      // Enforce both the 16-cline capacity limit and available free_space.
      return cls.size() < 16 && free_space() >= int(sizeof(ptr_address));
   }

   /**
    * 
    */
   leaf_node::value_branch leaf_node::add_address_ptr(value_type_flag t, ptr_address addr) noexcept
   {
      ptr_address            base_cline(*addr & ~0x0f);
      std::span<ptr_address> cls         = clines();
      int                    found_empty = -1;
      /// TODO: SIMD loop this bad boy, doing 2 compares and finding the index
      /// we can process 4 at a time for up to 4 iterations and the number of iterations
      /// can be known in advance and therefore predicted, just need to do one copy

      for (int i = 0; i < cls.size(); ++i)
      {
         if (cls[i] == base_cline)
            return value_branch(t, cline_offset(i), cline_index(*addr & 0x0f));
         if (sal::null_ptr_address == cls[i])
            found_empty = i;
      }
      if (found_empty >= 0)
      {
         cls[found_empty] = base_cline;
         return value_branch(t, cline_offset(found_empty), cline_index(*addr & 0x0f));
      }
      assert(free_space() >= 4);
      ++_cline_cap;
      /// cls[] will assert in debug if we address beyond its old size
      cls.data()[cls.size()] = base_cline;
      assert(free_space() >= 0);

      value_branch result(t, cline_offset(cls.size()), cline_index(*addr & 0x0f));
      return result;
   }
   int leaf_node::calc_cline_refs(cline_offset cl_off) const noexcept
   {
      int ref_count = 0;
      for (uint16_t i = 0; i < num_branches(); ++i)
      {
         value_branch vb = value_offsets()[i];
         ref_count += vb.is_address() && vb.cline() == cl_off;
      }
      return ref_count;
   }
   /// removes the address ptr if and only if no branches are found to reference it
   void leaf_node::remove_address_ptr(cline_offset cl_off) noexcept
   {
      //SAL_ERROR("remove_address_ptr:{}", cl_off);
      for (uint16_t i = 0; i < num_branches(); ++i)
      {
         value_branch vb = value_offsets()[i];
         if (vb.is_address())
         {
            if (vb.cline() == cl_off)
            {
               return;
            }
         }
      }
      clines()[*cl_off] = sal::null_ptr_address;
      //_cline_cap -= (cl_off == _cline_cap - 1);
      while (_cline_cap > 0 && clines()[_cline_cap - 1] == sal::null_ptr_address)
      {
         --_cline_cap;
      }
   }
   void leaf_node::dump() const
   {
      SAL_INFO("leaf_node::dump()");
      SAL_INFO("  num_branches: {}", num_branches());
      SAL_INFO("  clines_capacity: {}", clines_capacity());
      SAL_INFO("  dead_space: {}", dead_space());
      for (uint16_t i = 0; i < num_branches(); ++i)
      {
         SAL_ERROR("  [{}]  key: {} = '{}'", i, get_key(branch_number(i)),
                   get_value(branch_number(i)));
         value_branch vb = value_offsets()[i];
         if (vb.is_address())
            SAL_ERROR("       vb.cline():{}  type:{}", vb.cline(), vb.type());
      }
      SAL_ERROR("  clines: {}", clines().size());
   }

   bool leaf_node::validate_invariants() const noexcept
   {
      // 0a. Type must be leaf
      if (type() != node_type::leaf)
      {
         SAL_ERROR("leaf validate: type {} != leaf", (int)type());
         return false;
      }

      // 0b. Size must be valid (multiple of 64, <= max_leaf_size)
      if (size() > max_leaf_size || size() < sizeof(leaf_node) || size() % 64 != 0)
      {
         SAL_ERROR("leaf validate: size {} invalid (max:{} min:{})", size(), max_leaf_size,
                   sizeof(leaf_node));
         return false;
      }

      uint16_t nb = num_branches();

      // 0c. num_branches must fit within the node
      if (nb > (size() - sizeof(leaf_node)) / 5)
      {
         SAL_ERROR("leaf validate: num_branches {} too large for size {}", nb, size());
         return false;
      }

      // 1. Layout sanity: clines must not overlap with alloc area
      if (free_space() < 0)
      {
         SAL_ERROR("leaf validate: free_space {} < 0  cline_cap:{} nb:{}", free_space(), _cline_cap,
                   nb);
         return false;
      }

      // 2. Cline count should be reasonable (max 16 unique cachelines per leaf)
      if (_cline_cap > 16)
      {
         SAL_ERROR("leaf validate: _cline_cap {} > 16  nb:{}", _cline_cap, nb);
         return false;
      }

      // 3. Check that all keys are retrievable and offsets point within alloc area
      for (uint16_t i = 0; i < nb; ++i)
      {
         auto ko = keys_offsets()[i];
         if (*ko > _alloc_pos && _alloc_pos > 0)
         {
            SAL_ERROR("leaf validate: key_offset[{}]={} > alloc_pos={}", i, *ko, _alloc_pos);
            return false;
         }
      }

      // 4. Value type consistency: address-type values must reference valid cline indices
      for (uint16_t i = 0; i < nb; ++i)
      {
         value_branch vb = value_offsets()[i];
         if (vb.is_address())
         {
            auto cl = vb.cline();
            if (*cl >= _cline_cap)
            {
               SAL_ERROR("leaf validate: branch[{}] cline {} >= cline_cap {}", i, *cl, _cline_cap);
               return false;
            }
            // The cline entry should not be null
            if (clines()[*cl] == sal::null_ptr_address)
            {
               SAL_ERROR("leaf validate: branch[{}] references null cline {}", i, *cl);
               return false;
            }
         }
         else if (vb.is_inline())
         {
            auto off = vb.offset();
            if (*off > _alloc_pos && _alloc_pos > 0)
            {
               SAL_ERROR("leaf validate: branch[{}] value_offset {} > alloc_pos {}", i, *off,
                         _alloc_pos);
               return false;
            }
         }
      }

      // 5. Cline ref counts: each non-null cline should be referenced by at least one value_branch
      for (uint16_t c = 0; c < _cline_cap; ++c)
      {
         if (clines()[c] == sal::null_ptr_address)
            continue;
         int refs = 0;
         for (uint16_t i = 0; i < nb; ++i)
         {
            value_branch vb = value_offsets()[i];
            if (vb.is_address() && *vb.cline() == c)
               ++refs;
         }
         if (refs == 0)
         {
            SAL_ERROR("leaf validate: cline[{}] non-null but has 0 references", c);
            return false;
         }
      }

      // 6. No duplicate cline bases
      for (uint16_t i = 0; i < _cline_cap; ++i)
      {
         if (clines()[i] == sal::null_ptr_address)
            continue;
         ptr_address base_i = clines()[i] & ~ptr_address(0x0f);
         for (uint16_t j = i + 1; j < _cline_cap; ++j)
         {
            if (clines()[j] == sal::null_ptr_address)
               continue;
            ptr_address base_j = clines()[j] & ~ptr_address(0x0f);
            if (base_i == base_j)
            {
               SAL_ERROR("leaf validate: duplicate cline base at [{}] and [{}]: {}", i, j, base_i);
               return false;
            }
         }
      }

      return true;
   }

   key_view leaf_node::get_common_prefix() const noexcept
   {
      uint32_t nb = num_branches();
      assert(nb > 1);
      key_view front_key = get_key(branch_number(0));
      key_view back_key  = get_key(branch_number(nb - 1));
      return common_prefix(front_key, back_key);
   }

   /**
    * Find the best position to split the node by minimizing the size difference
    * between the two resulting nodes after the split.
    * Scans inward from the end, updating the best split found so far.
    *
    * @return A split_pos struct containing the common prefix, the dividing byte,
    *         and the counts of keys on either side of the split based on that byte.
    * @pre num_branches() > 1
    */
   leaf_node::split_pos leaf_node::get_split_pos() const noexcept
   {
      split_pos result;
      uint16_t  nb = num_branches();
      assert(nb > 1);

      // --- General case (nb >= 2) ---
      result.cprefix     = get_common_prefix();
      size_t cprefix_len = result.cprefix.size();

      uint8_t  bytes[nb];
      uint16_t last_idx  = nb - 1;
      uint8_t  last_byte = get_key(branch_number(1))[cprefix_len];
      uint16_t idx       = 1;
      uint8_t  byte      = last_byte;
      uint16_t delta     = nb;
      int      mid       = nb / 2;
      int      end       = nb;
      for (int x = 2; x < nb; ++x)
      {
         uint8_t next_byte = get_key(branch_number(x))[cprefix_len];
         if (next_byte != last_byte)
         {
            last_byte      = next_byte;
            auto cur_delta = abs(mid - x);
            if (cur_delta < delta)
            {
               idx   = x;
               byte  = next_byte;
               delta = cur_delta;
               assert(mid + delta <= nb);
               end = mid + delta;  //std::min<uint16_t>(nb, mid + delta);
            }
            else
               break;
         }
      }
      result.divider          = byte;
      result.less_than_count  = idx;
      result.greater_eq_count = nb - idx;
      //  SAL_ERROR("get_split_pos: cpre: '{}' divider: '{}' before: '{}' after: '{}'", result.cprefix,
      //            byte, idx, nb - idx);
      return result;
   }

}  // namespace psitri

#ifdef PSITRI_LEAF_ALIGNMENT_PUSH
#pragma clang attribute pop
#undef PSITRI_LEAF_ALIGNMENT_PUSH
#endif
