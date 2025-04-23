#include <algorithm>  // Required for std::max, std::abs
#include <cassert>    // Required for assert
#include <cstdlib>    // Required for std::abs on int (safer than cmath ambiguity)
#include <psitri/node/leaf.hpp>

namespace psitri
{
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
      apply(op::leaf_insert{*this, lower_bound(key), key, value});
   }

   void leaf_node::clone_from(const leaf_node* clone)
   {
      set_num_branches(clone->num_branches());
      if (not clone->is_optimal_layout())
      {
         SAL_INFO("cloning to optimal layout, num_branches: {}", num_branches());
         _alloc_pos      = 0;
         _dead_space     = 0;
         _cline_cap      = 0;
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
               vos[x] = add_address_ptr(value_branch::value_type::subtree, val.subtree_address());
            else if (val.is_value_node())
               vos[x] = add_address_ptr(value_branch::value_type::value_node, val.value_address());
            else
               vos[x] = value_branch();
         }
         assert(is_optimal_layout());
         return;
      }

      _alloc_pos      = clone->_alloc_pos;
      _cline_cap      = clone->_cline_cap;
      _dead_space     = clone->_dead_space;
      _optimal_layout = clone->_optimal_layout;

      assert(free_space() >= 0);

      auto ccline     = clone->clines();
      auto ccline_end = ccline.data() + ccline.size();
      auto head_size  = (char*)ccline_end - (char*)clone->_key_hashs;

      memcpy(_key_hashs, clone->_key_hashs, head_size);
      memcpy(alloc_head(), clone->alloc_head(), clone->_alloc_pos);
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
      clone_from(clone);
      apply(ins);
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
   /**
    *  Construct an optimized node by cloning the existing node and truncating keys
    *  by the common prefix and only including keys in the range [start, end).
    */
   leaf_node::leaf_node(size_t           alloc_size,
                        ptr_address_seq  seq,
                        const leaf_node* clone,
                        key_view         cprefix,
                        branch_number    start,
                        branch_number    end)
       : node(alloc_size, node_type::leaf, seq)
   {
      _alloc_pos      = 0;
      _dead_space     = 0;
      _cline_cap      = 0;
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
         value_type val = clone->get_value(branch_number(x));
         if (val.is_view())
         {
            if (val.view().empty())
               vos[x - *start] = value_branch();
            else
               vos[x - *start] = alloc_value(val.view());
         }
         else if (val.is_subtree())
            vos[x - *start] =
                add_address_ptr(value_branch::value_type::subtree, val.subtree_address());
         else if (val.is_value_node())
            vos[x - *start] =
                add_address_ptr(value_branch::value_type::value_node, val.value_address());
         else
            vos[x - *start] = value_branch();
      }
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

   leaf_node::can_apply_mode leaf_node::can_apply(const op::leaf_insert& ins) const noexcept
   {
      assert(ins.key.size() <= 1024);  // TODO: max key size constant
      assert(not ins.value.is_remove());
      // key hash(1), key_offset, value_branch(2),
      size_t size_required = sizeof(uint8_t) + sizeof(key_offset) + sizeof(value_branch);
      if (ins.value.is_view())
      {
         assert(ins.value.view().size() <= 0xff);
         value_view data = ins.value.view();
         size_required += data.size() + sizeof(value_data);
      }
      size_required += ins.key.size() + 2;
      /// this over-estimates assuming worst case we must add a cline, but
      /// the calculating whether address() is on an existing cline requires
      /// scanning all clines to see if we can re-use one or have to add a new one.
      size_required += 4 * ins.value.is_address();
      int leftover = free_space() - size_required;
      // SAL_WARN("size_required: {}  free_space: {}  leftover: {}", size_required, free_space(),
      //          leftover);
      if (leftover >= 0)
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
   branch_number leaf_node::apply(const op::leaf_insert& ins) noexcept
   {
      auto init_free_space = free_space();
      assert(can_apply(ins) == can_apply_mode::modify);
      assert(!ins.value.is_remove());
      assert(ins.lb == lower_bound(ins.key));
      assert(ins.lb == num_branches() or get_key(ins.lb) != ins.key);

      int      tail_len = num_branches() - *ins.lb;
      uint32_t bn       = *ins.lb;

      constexpr const int move_size = sizeof(uint8_t) + sizeof(key_offset) + sizeof(value_branch);

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
             add_address_ptr(value_branch::value_type::subtree, ins.value.subtree_address());
      }
      else  //if (ins.value.is_value_node())
      {
         assert(ins.value.is_value_node());
         value_offsets()[bn] =
             add_address_ptr(value_branch::value_type::value_node, ins.value.value_address());
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
      auto final_free_space = free_space();
      //      SAL_WARN("init free space: {} final free space: {}  delta: {}", init_free_space,
      //               final_free_space, final_free_space - init_free_space);
      return ins.lb;
   }

   void leaf_node::remove(branch_number bn) noexcept
   {
      SAL_ERROR("remove:{} key:{} size: {}", bn, get_key(bn), get_key(bn).size());
      assert(bn < num_branches());

      value_branch vb = value_offsets()[*bn];
      key_offset   ko = keys_offsets()[*bn];

      // 1. Free space accounting
      _dead_space += sizeof(key) + get_key_ptr(ko)->get().size();
      if (vb.is_inline())
      {
         value_offset vo = vb.offset();
         SAL_ERROR("    value size: {}", get_value_ptr(vo)->get().size());
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
            clines()[*cl_off] = ptr_address(0);
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
   }

   size_t leaf_node::update_value(branch_number bn, const value_type& value) noexcept
   {
      assert(bn < num_branches());
      assert(not value.is_remove());
      SAL_INFO("update: {} = '{}'", int(*bn), value);

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
            vb = add_address_ptr(value_branch::value_type::subtree, value.subtree_address());
            _dead_space += old_size + sizeof(value_data);
            _optimal_layout = false;
         }
         else if (value.is_value_node())
         {
            vb = add_address_ptr(value_branch::value_type::value_node, value.value_address());
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
         vb = add_address_ptr(value_branch::value_type::subtree, value.subtree_address());
      }
      else if (value.is_value_node())
      {
         vb = add_address_ptr(value_branch::value_type::value_node, value.value_address());
      }
      _optimal_layout = false;
      assert(get_value(bn) == value);
      return old_size;
   }

   bool leaf_node::can_insert_address(ptr_address addr) const noexcept
   {
      ptr_address base_cline(*addr & ~0x0f);
      if (clines().size() < 15)
         return true;
      bool       found_existing = false;
      const auto cls            = clines();
      // TODO: there is a scalar algo that can do this 2x at a time
      // there is a neon algo that can do this 4x at a time
      // there is a AVX2 algo that can do this 8x at a time
      for (int i = 0; i < cls.size(); ++i)
         found_existing |= cls[i] == base_cline;
      return found_existing;
   }

   /**
    * 
    */
   leaf_node::value_branch leaf_node::add_address_ptr(value_branch::value_type t,
                                                      ptr_address              addr) noexcept
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
      SAL_WARN("base_cline: {:x}  addr: {:x}  cls.size():{}", base_cline, addr, cls.size());
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
      SAL_ERROR("remove_address_ptr:{}", cl_off);
      for (uint16_t i = 0; i < num_branches(); ++i)
      {
         SAL_ERROR(" [{}]  key: {} = {}", i, get_key(branch_number(i)),
                   get_value(branch_number(i)));
         value_branch vb = value_offsets()[i];
         if (vb.is_address())
         {
            SAL_ERROR("       vb.cline():{}  cl_off:{}", vb.cline(), cl_off);
            if (vb.cline() == cl_off)
            {
               SAL_ERROR("        found");
               return;
            }
         }
      }
      clines()[*cl_off] = ptr_address(0);
      //_cline_cap -= (cl_off == _cline_cap - 1);
      while (_cline_cap > 0 && clines()[_cline_cap - 1] == ptr_address(0))
      {
         SAL_ERROR("   remove tail cline");
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

      // Initialize with the split just before the last element (index nb-2)
      int best_split_idx = nb - 1;

      // Calculate initial divider based on the key *after* the initial best_split_idx
      key_view initial_split_key = get_key(branch_number(best_split_idx));  // key[nb-1]
      assert(initial_split_key.size() > cprefix_len &&
             "Last key must differ from first key after common prefix if nb >= 2");
      result.divider = initial_split_key[cprefix_len];

      auto mid = nb / 2;
      for (int x = mid; x < best_split_idx; ++x)
      {
         key_view key_x  = get_key(branch_number(x));
         key_view key_x1 = get_key(branch_number(x + 1));
         if (key_x[cprefix_len] != key_x1[cprefix_len])
         {
            best_split_idx = x + 1;
            break;
         }
      }

      const int max_back = mid - (best_split_idx - mid) + 1;
      assert(max_back >= 1);  // so we don't have to check key sizes before key[cpefix_len]

      for (int x = mid - 1; x >= max_back; --x)
      {
         key_view key_x  = get_key(branch_number(x));
         key_view key_x1 = get_key(branch_number(x + 1));
         if (key_x[cprefix_len] != key_x1[cprefix_len])
         {
            best_split_idx = x + 1;
            break;
         }
      }
      result.less_than_count  = best_split_idx;
      result.greater_eq_count = nb - best_split_idx;
      result.divider          = get_key(branch_number(best_split_idx))[cprefix_len];

      return result;
   }

}  // namespace psitri
