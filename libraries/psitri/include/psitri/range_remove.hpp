#pragma once
// range_remove.hpp — tree_context member function definitions for range removal.
// This file is #included at the bottom of tree_ops.hpp so that these definitions
// are part of the same translation unit as tree_context.

namespace psitri
{

   inline uint64_t tree_context::remove_range(key_view lower, key_view upper)
   {
      if (!_root)
         return 0;
      if (lower >= upper && upper != max_key)
         return 0;  // empty range

      sal::read_lock lock = _session.lock();
      _delta_descendents = 0;

      // Map max_key -> empty for internal unbounded representation
      key_range range = {lower, upper == max_key ? key_view() : upper};

      auto rref     = *_root;
      auto old_addr = _root.take();

      branch_set result = range_remove<upsert_mode::unique>({}, rref, range);

      if (result.count() == 0)
         ;  // tree is now empty
      else if (result.count() == 1)
         _root.give(result.get_first_branch());
      else
         _root.give(make_inner(result));

      return static_cast<uint64_t>(-_delta_descendents);
   }

   template <upsert_mode mode>
   branch_set tree_context::range_remove(const sal::alloc_hint&   parent_hint,
                                         smart_ref<alloc_header>& ref,
                                         key_range                range)
   {
      if constexpr (mode.is_unique())
         if (ref.ref() > 1)
            return range_remove<mode.make_shared()>(parent_hint, ref, range);

      branch_set result;
      switch (node_type(ref->type()))
      {
         case node_type::inner:
            result = range_remove_inner<mode>(parent_hint, ref.as<inner_node>(), range);
            break;
         case node_type::inner_prefix:
            result = range_remove_inner<mode>(parent_hint, ref.as<inner_prefix_node>(), range);
            break;
         case node_type::leaf:
            result = range_remove_leaf<mode>(parent_hint, ref.as<leaf_node>(), range);
            break;
         case node_type::value:
         {
            // A value_node represents a single key with empty key ("")
            bool in_range = (range.lower_bound.empty() || key_view() >= range.lower_bound) &&
                            (range.upper_bound.empty() || key_view() < range.upper_bound);
            if (in_range)
            {
               _delta_descendents -= 1;
               result = {};  // remove this value
            }
            else
            {
               result = ref.address();
            }
            break;
         }
         default:
            std::unreachable();
      }

      if constexpr (mode.is_unique())
      {
         if (result.count() == 0)
            _session.release(ref.address());
      }
      else if (!result.contains(ref.address())) [[likely]]
         ref.release();

      return result;
   }

   template <upsert_mode mode>
   branch_set tree_context::range_remove_leaf(const sal::alloc_hint& parent_hint,
                                              smart_ref<leaf_node>&  leaf,
                                              key_range              range)
   {
      branch_number lo = range.lower_bound.empty() ? branch_number(0)
                                                    : leaf->lower_bound(range.lower_bound);
      branch_number hi = range.upper_bound.empty() ? branch_number(leaf->num_branches())
                                                    : leaf->lower_bound(range.upper_bound);

      if (*hi <= *lo)
         return leaf.address();  // nothing in range

      uint16_t count   = *hi - *lo;
      uint16_t total   = leaf->num_branches();

      if (count == total)
      {
         // All branches removed — release value_nodes in the range via destroy cascade
         _delta_descendents -= count;
         return {};
      }

      _delta_descendents -= count;

      if constexpr (mode.is_unique())
      {
         // Release value_nodes/subtrees for branches being removed
         for (uint16_t i = *lo; i < *hi; ++i)
         {
            if (leaf->get_value_type(branch_number(i)) >= leaf_node::value_type_flag::value_node)
               _session.release(leaf->get_value(branch_number(i)).address());
         }
         leaf.modify()->remove_range(lo, hi);
         return leaf.address();
      }
      else  // shared mode
      {
         retain_children(leaf);

         // Release value_nodes/subtrees for branches being removed
         for (uint16_t i = *lo; i < *hi; ++i)
         {
            if (leaf->get_value_type(branch_number(i)) >= leaf_node::value_type_flag::value_node)
               _session.release(leaf->get_value(branch_number(i)).address());
         }

         op::leaf_remove_range rm{.src = *leaf.obj(), .lo = lo, .hi = hi};
         return _session.alloc<leaf_node>(parent_hint, rm);
      }
   }

   template <upsert_mode mode, any_inner_node_type NodeT>
   branch_set tree_context::range_remove_inner(const sal::alloc_hint& parent_hint,
                                               smart_ref<NodeT>&      node,
                                               key_range              range)
   {
      // Handle prefix for inner_prefix_node — same logic as count_keys_inner
      if constexpr (is_inner_prefix_node<NodeT>)
      {
         key_view prefix    = node->prefix();
         key_view new_lower = range.lower_bound;
         key_view new_upper = range.upper_bound;

         // Narrow lower bound by prefix
         if (!range.lower_bound.empty())
         {
            auto cpre = common_prefix(prefix, range.lower_bound);
            if (cpre.size() == prefix.size())
            {
               new_lower = range.lower_bound.substr(prefix.size());
            }
            else if (cpre.size() < range.lower_bound.size())
            {
               if (static_cast<uint8_t>(prefix[cpre.size()]) >
                   static_cast<uint8_t>(range.lower_bound[cpre.size()]))
                  new_lower = key_view();  // all keys > lower
               else
                  return node.address();  // all keys < lower, nothing to remove
            }
            else
            {
               // lower_bound is a prefix of node prefix -> all keys > lower
               new_lower = key_view();
            }
         }

         // Narrow upper bound by prefix
         if (!range.upper_bound.empty())
         {
            auto cpre = common_prefix(prefix, range.upper_bound);
            if (cpre.size() == prefix.size())
            {
               if (range.upper_bound.size() == prefix.size())
                  return node.address();  // prefix == upper exactly, exclusive -> nothing to remove
               new_upper = range.upper_bound.substr(prefix.size());
            }
            else if (cpre.size() < range.upper_bound.size())
            {
               if (static_cast<uint8_t>(prefix[cpre.size()]) >=
                   static_cast<uint8_t>(range.upper_bound[cpre.size()]))
                  return node.address();  // all keys >= upper, nothing to remove
               else
                  new_upper = key_view();  // all keys < upper
            }
            else
            {
               // upper_bound is a prefix of node prefix -> all keys >= upper
               return node.address();
            }
         }

         range = {new_lower, new_upper};
      }

      // After prefix narrowing, check if unbounded (remove everything)
      if (range.is_unbounded())
      {
         _delta_descendents -= node->descendents();
         return {};  // remove entire subtree
      }

      if (range.is_empty_range())
         return node.address();

      // Route: find which branches the lower and upper bounds map to
      branch_number start = range.lower_bound.empty()
                                ? branch_number(0)
                                : node->lower_bound(range.lower_bound);

      if (*start >= node->num_branches())
         return node.address();  // nothing to remove

      branch_number end      = branch_number(node->num_branches());
      branch_number boundary = end;
      bool          has_boundary = false;

      if (!range.upper_bound.empty())
      {
         boundary = node->lower_bound(range.upper_bound);
         end      = boundary;
         if (*boundary < node->num_branches())
            has_boundary = true;
      }

      // If start == boundary, both bounds are in the same branch — recurse with full range
      if (*start == *boundary && has_boundary)
      {
         auto  badr = node->get_branch(start);
         auto  bref = _session.get_ref(badr);

         if constexpr (mode.is_shared())
            retain_children(node);

         branch_set sub = range_remove<mode>(node->get_branch_clines(), bref, range);

         if (sub.count() == 0)
         {
            // This branch became empty — treat like single-branch remove
            if (node->num_branches() == 1)
            {
               if constexpr (mode.is_unique())
                  _session.retain(badr);
               return {};
            }

            if constexpr (mode.is_unique())
            {
               node.modify(
                   [&](auto* n)
                   {
                      n->remove_branch(start);
                      n->add_descendents(_delta_descendents);
                   });
               return node.address();
            }
            else
            {
               op::inner_remove_branch rm{start, _delta_descendents};
               if constexpr (is_inner_node<NodeT>)
                  return _session.alloc<NodeT>(parent_hint, node.obj(), rm);
               else
                  return _session.alloc<NodeT>(parent_hint, node->prefix(), node.obj(), rm);
            }
         }

         // Branch still exists — update descendents
         if constexpr (mode.is_unique())
         {
            if (sub.count() == 1 && sub.get_first_branch() == badr)
            {
               if (_delta_descendents != 0)
                  node.modify()->add_descendents(_delta_descendents);
               return node.address();
            }
            // Branch address changed in unique mode — need merge_branches
            return merge_branches<mode.make_shared_or_unique_only()>(parent_hint, node, start, sub);
         }
         else
         {
            return merge_branches<mode.make_shared_or_unique_only()>(parent_hint, node, start, sub);
         }
      }

      // General case: start and boundary are in different branches
      // We need to:
      // 1. Recurse into start branch (partial removal, keys >= lower)
      // 2. Release fully-contained middle branches
      // 3. Recurse into boundary branch (partial removal, keys < upper)
      // 4. Rebuild node

      if constexpr (mode.is_shared())
         retain_children(node);

      int64_t saved_delta = _delta_descendents;

      // Count keys in middle branches (fully contained — will be removed entirely)
      int64_t middle_keys = 0;
      for (uint16_t i = *start + 1; i < *end; ++i)
         middle_keys += psitri::count_child_keys(_session, node->get_branch(branch_number(i)));

      // Recurse into start branch
      ptr_address start_addr     = node->get_branch(start);
      bool        start_empty    = false;
      bool        start_changed  = false;
      ptr_address new_start_addr = start_addr;

      if (!range.lower_bound.empty())
      {
         auto start_ref = _session.get_ref(start_addr);
         _delta_descendents = 0;
         branch_set start_result = range_remove<mode>(node->get_branch_clines(), start_ref, range);
         if (start_result.count() == 0)
            start_empty = true;
         else if (start_result.count() == 1)
         {
            new_start_addr = start_result.get_first_branch();
            start_changed  = (new_start_addr != start_addr);
         }
         else
         {
            // Start branch split — shouldn't happen in range remove (only shrinks)
            // but handle defensively by making a new inner node
            new_start_addr = make_inner(start_result);
            start_changed  = true;
         }
      }
      else
      {
         // lower is unbounded, so start branch is fully contained
         _delta_descendents = 0;
         _delta_descendents -= psitri::count_child_keys(_session, start_addr);
         start_empty = true;
      }
      int64_t start_delta = _delta_descendents;

      // Recurse into boundary branch
      ptr_address boundary_addr     = sal::null_ptr_address;
      bool        boundary_empty    = false;
      bool        boundary_changed  = false;
      ptr_address new_boundary_addr = sal::null_ptr_address;
      int64_t     boundary_delta    = 0;

      if (has_boundary)
      {
         boundary_addr     = node->get_branch(boundary);
         new_boundary_addr = boundary_addr;
         auto boundary_ref = _session.get_ref(boundary_addr);
         _delta_descendents = 0;
         branch_set boundary_result =
             range_remove<mode>(node->get_branch_clines(), boundary_ref, range);
         if (boundary_result.count() == 0)
            boundary_empty = true;
         else if (boundary_result.count() == 1)
         {
            new_boundary_addr = boundary_result.get_first_branch();
            boundary_changed  = (new_boundary_addr != boundary_addr);
         }
         else
         {
            new_boundary_addr = make_inner(boundary_result);
            boundary_changed  = true;
         }
         boundary_delta = _delta_descendents;
      }

      // Compute total delta
      int64_t total_delta = saved_delta + start_delta + boundary_delta - middle_keys;
      _delta_descendents  = total_delta;

      // Count surviving branches
      uint16_t before_count = *start;
      uint16_t after_start  = has_boundary ? *boundary + 1 : *end;
      uint16_t after_count  = node->num_branches() - after_start;
      uint16_t survivors    = before_count + (!start_empty ? 1 : 0) +
                              (!boundary_empty && has_boundary ? 1 : 0) + after_count;

      if (survivors == 0)
      {
         if constexpr (mode.is_unique())
         {
            // The caller will release this node, and its destroy() cascade releases
            // ALL children.  We must NOT double-release branches that the recursive
            // range_remove already released.  Instead, retain those branches so the
            // destroy cascade can properly release them (same pattern as the
            // same-branch num_branches==1 case).
            //
            // Middle branches and fully-contained start were NOT recursed into,
            // so they haven't been released — no action needed for them (the
            // destroy cascade handles them).
            //
            // Branches that WERE recursed into and came back empty were already
            // released by range_remove.  Retain them to counterbalance.
            if (start_empty && !range.lower_bound.empty())
               _session.retain(start_addr);
            if (boundary_empty && has_boundary)
               _session.retain(boundary_addr);
         }
         else
         {
            // In shared mode, retain_children was called (+1 to all children).
            // Release branches that were NOT recursed into to balance the retain.
            // Branches that were recursed into are balanced by ref.release() in
            // the shared-mode range_remove dispatch.
            for (uint16_t i = *start + 1; i < *end; ++i)
               _session.release(node->get_branch(branch_number(i)));
            if (start_empty && range.lower_bound.empty())
               _session.release(start_addr);
         }
         return {};
      }

      // Compute removal range for the inner node
      // Branches to remove: [start, boundary+1) or [start, end) minus the surviving start/boundary
      // We track which branches within the inner node are being removed/replaced.

      if constexpr (mode.is_unique())
      {
         // Release middle branches
         for (uint16_t i = *start + 1; i < *end; ++i)
            _session.release(node->get_branch(branch_number(i)));

         // Release fully-contained start if lower was unbounded
         if (start_empty && range.lower_bound.empty())
            _session.release(start_addr);

         // Now we need to remove branches from the inner node.
         // Determine the contiguous range to remove from [start..end+has_boundary-1]:
         // - If start is empty, it's part of the removal
         // - Middle branches [start+1, end) are always removed
         // - If boundary is empty, it's part of the removal

         uint16_t remove_lo = start_empty ? *start : *start + 1;
         uint16_t remove_hi = (boundary_empty || !has_boundary) ? (has_boundary ? *boundary + 1 : *end)
                                                                 : *end;

         if (remove_lo < remove_hi && remove_hi <= node->num_branches())
         {
            if (remove_hi - remove_lo == node->num_branches())
               return {};  // remove all — already handled above

            // If start survived but changed address, update it first
            if (!start_empty && start_changed)
            {
               // We need to replace the start branch. Since we're in unique mode and we've
               // already modified the start subtree in place, the address should still be valid
               // unless the child changed address (shared->new alloc).
               // This case is complex; let's use the merge path for simplicity.
            }

            node.modify(
                [&](auto* n)
                {
                   n->remove_range(branch_number(remove_lo), branch_number(remove_hi));
                   n->add_descendents(total_delta);
                });

            // Handle changed addresses after the remove_range.
            // add_descendents already applied total_delta, so merge_branches must use 0.
            // After remove_range, boundary's position shifted to remove_lo.
            if (!start_empty && start_changed && !boundary_empty && has_boundary && boundary_changed)
            {
               // Both changed — handle sequentially
               _delta_descendents = 0;
               branch_set ssub(new_start_addr);
               auto result = merge_branches<upsert_mode::unique>(parent_hint, node, start, ssub);
               if (result.count() == 1)
               {
                  auto result_ref = _session.template get_ref<NodeT>(result.get_first_branch());
                  auto boundary_new_pos = branch_number(remove_lo);
                  branch_set bsub(new_boundary_addr);
                  result = merge_branches<upsert_mode::unique>(parent_hint, result_ref, boundary_new_pos, bsub);
               }
               _delta_descendents = total_delta;
               return result;
            }
            if (!boundary_empty && has_boundary && boundary_changed)
            {
               auto boundary_new_pos = branch_number(remove_lo);
               _delta_descendents = 0;
               branch_set bsub(new_boundary_addr);
               auto result = merge_branches<upsert_mode::unique>(parent_hint, node, boundary_new_pos, bsub);
               _delta_descendents = total_delta;
               return result;
            }
            if (!start_empty && start_changed)
            {
               _delta_descendents = 0;
               branch_set ssub(new_start_addr);
               auto result = merge_branches<upsert_mode::unique>(parent_hint, node, start, ssub);
               _delta_descendents = total_delta;
               return result;
            }

            return node.address();
         }
         else
         {
            // Nothing to remove from inner node (all survived)
            if (_delta_descendents != 0)
               node.modify()->add_descendents(total_delta);

            // Handle changed addresses — add_descendents already applied total_delta,
            // so merge_branches must use 0 to avoid double-counting.
            if (!start_empty && start_changed)
            {
               _delta_descendents = 0;
               branch_set ssub(new_start_addr);
               auto result = merge_branches<upsert_mode::unique>(parent_hint, node, start, ssub);

               // If boundary also changed, apply it to the result node
               if (!boundary_empty && has_boundary && boundary_changed && result.count() == 1)
               {
                  auto result_ref = _session.template get_ref<NodeT>(result.get_first_branch());
                  branch_set bsub(new_boundary_addr);
                  result = merge_branches<upsert_mode::unique>(parent_hint, result_ref, boundary, bsub);
               }
               _delta_descendents = total_delta;
               return result;
            }
            if (!boundary_empty && has_boundary && boundary_changed)
            {
               _delta_descendents = 0;
               branch_set bsub(new_boundary_addr);
               auto result = merge_branches<upsert_mode::unique>(parent_hint, node, boundary, bsub);
               _delta_descendents = total_delta;
               return result;
            }

            return node.address();
         }
      }
      else  // shared mode
      {
         // In shared mode, retain_children was already called (+1 to all children).
         // Release middle branches (not recursed into — balance the retain)
         for (uint16_t i = *start + 1; i < *end; ++i)
            _session.release(node->get_branch(branch_number(i)));

         // Release start only if NOT recursed into (unbounded lower = fully contained).
         // If recursed into, the dispatch in range_remove() already released the old
         // ref (via ref.release()), which balances the retain.
         if (start_empty && range.lower_bound.empty())
            _session.release(start_addr);
         // Note: boundary is always recursed into when has_boundary, so dispatch handles it.
         // Do NOT explicitly release start/boundary that were recursed into — that would double-release.

         // Determine contiguous remove range
         uint16_t remove_lo = start_empty ? *start : *start + 1;
         uint16_t remove_hi = (boundary_empty || !has_boundary) ? (has_boundary ? *boundary + 1 : *end)
                                                                 : *end;

         if (remove_lo >= remove_hi)
         {
            // No branches to remove from the node, but addresses may have changed.
            // Need to allocate a new node copying all branches with updated addresses.
            // For simplicity, use subrange to build a new node:
            // Actually we just need to handle replaced start/boundary.
            // The easiest path: if no branches removed and no addresses changed,
            // just update descendents; but in shared mode we can't modify in place.
            // Build a new node that's a copy with updated descendents.

            if (!start_changed && !boundary_changed)
            {
               // Nothing actually changed — return original
               // But we've already retained children... undo by releasing them
               // Actually in shared mode, the caller will release `ref` if result doesn't contain it.
               // Since we're returning node.address(), the caller will NOT release it. Good.
               // But wait — retain_children was called but nothing changed.
               // In the shared upsert path, retain_children is called before recursion.
               // If recursion results in no change, the caller releases the old ref,
               // and the retained children balance with the new copy... but here we're
               // returning the original address, so the retain has no matching release.
               // We need to release children to undo the retain.

               // Actually, let me re-examine: in the shared path of upsert() (tree_ops.hpp:1017-1018),
               // retain_children is called and then sub_branches is computed. If the result is unchanged
               // (same address), the code at line 1306-1316 returns in.address(). The caller at line 767-768
               // checks `if (not result.contains(r.address())) r.release()`. Since result == node.address()
               // and r == ref, they match, so ref is NOT released. But retain_children gave +1 to all children.
               // Who decrements those? In the unchanged case, the old node still owns them.
               // The retain was speculative and now needs to be undone.
               // But in the existing code this is handled by... hmm, let me look more carefully.

               // Actually in the existing shared upsert code, retain_children is ALWAYS called before
               // recursion. If recursion doesn't change anything, the sub_branches still contains
               // the old address, and merge_branches is skipped. The code at line 1306-1316 returns
               // in.address(). Since the result contains in.address(), the old ref is NOT released.
               // So the old node survives with its children. The children were retained (+1),
               // and... this IS a bug in the existing code (over-retain). But actually it works out
               // because the shared path only runs when ref > 1, meaning someone else has a copy.
               // The retain ensures children survive when the caller eventually releases the ref.
               // Hmm, this is tricky.

               // For range_remove in shared mode, if nothing changed, we can avoid retain entirely.
               // But retain was already called. To compensate, release each child once.
               node->visit_branches(
                   [this](ptr_address br) { _session.release(br); });
               return node.address();
            }

            // Build replacement: use merge_branches for the changed branches.
            // When both start and boundary changed, we need two sequential merges.
            if (start_changed && boundary_changed)
            {
               // First merge: replace start branch
               branch_set ssub(new_start_addr);
               _delta_descendents = 0;  // merge_branches uses _delta_descendents
               auto after_start = merge_branches<mode.make_shared_or_unique_only()>(
                   parent_hint, node, start, ssub);
               // after_start is a new node. Replace boundary in it.
               // merge_branches in shared mode allocates a new node, so we get a smart_ref to it.
               if (after_start.count() == 1)
               {
                  auto after_ref = _session.template get_ref<NodeT>(after_start.get_first_branch());
                  branch_set bsub(new_boundary_addr);
                  _delta_descendents = total_delta;  // apply the full delta on the second merge
                  return merge_branches<upsert_mode::unique>(parent_hint, after_ref, boundary, bsub);
               }
               // If split occurred (unlikely for a replacement), just return what we have
               return after_start;
            }
            if (start_changed)
            {
               branch_set ssub(new_start_addr);
               return merge_branches<mode.make_shared_or_unique_only()>(parent_hint, node, start, ssub);
            }
            if (boundary_changed)
            {
               branch_set bsub(new_boundary_addr);
               return merge_branches<mode.make_shared_or_unique_only()>(parent_hint, node, boundary, bsub);
            }
            return node.address();
         }
         else
         {
            // Branches need to be removed. Build a new node.
            if (survivors == 1)
            {
               // Collapse to single branch
               ptr_address surviving;
               if (!start_empty)
                  surviving = new_start_addr;
               else if (!boundary_empty && has_boundary)
                  surviving = new_boundary_addr;
               else
               {
                  // Must be before or after range
                  if (before_count > 0)
                     surviving = node->get_branch(branch_number(0));
                  else
                     surviving = node->get_branch(branch_number(after_start));
               }
               if constexpr (is_inner_node<NodeT>)
                  return surviving;
               else
               {
                  // For inner_prefix_node, we can't just return the child—
                  // we need to prepend the prefix. But that's collapse logic.
                  // For simplicity, wrap in a new inner_prefix_node with 1 branch.
                  branch_set bs(surviving);
                  return make_inner_prefix(parent_hint, node->prefix(), bs);
               }
            }

            // Need to handle the case where start/boundary addresses changed.
            // The simplest correct approach: build using op::inner_remove_range for
            // the contiguous block, then handle replaced start/boundary separately.

            // First: if start or boundary changed address, we need to build the node
            // differently. For now, use the simpler approach of building from scratch
            // if addresses changed.

            if (start_changed || boundary_changed)
            {
               // Build a branch_set of all surviving branches
               // (Max survivors is total - removed, which could be up to 256...)
               // Since branch_set only holds 6, we'll use subrange-based alloc instead.

               // Fallback: allocate via inner_remove_range (which uses original addresses),
               // then handle changed addresses via merge_branches afterward.
               op::inner_remove_range rm{branch_number(remove_lo), branch_number(remove_hi),
                                         total_delta};
               ptr_address new_addr;
               if constexpr (is_inner_node<NodeT>)
                  new_addr = _session.alloc<NodeT>(parent_hint, node.obj(), rm);
               else
                  new_addr = _session.alloc<NodeT>(parent_hint, node->prefix(), node.obj(), rm);

               auto new_ref = _session.get_ref<NodeT>(new_addr);

               // Now replace changed branches
               if (!start_empty && start_changed)
               {
                  // Find position of start in new node
                  branch_number new_start_pos = start;  // same position (we removed after start)
                  _delta_descendents = 0;
                  branch_set ssub(new_start_addr);
                  auto result =
                      merge_branches<upsert_mode::unique>(parent_hint, new_ref, new_start_pos, ssub);
                  if (!boundary_empty && has_boundary && boundary_changed)
                  {
                     // boundary position shifted by number of removed branches
                     if (result.count() == 1)
                     {
                        auto result_ref = _session.get_ref<NodeT>(result.get_first_branch());
                        branch_number new_boundary_pos =
                            branch_number(*start + 1);  // right after start
                        _delta_descendents = 0;
                        branch_set bsub(new_boundary_addr);
                        auto final_result = merge_branches<upsert_mode::unique>(parent_hint, result_ref,
                                                                   new_boundary_pos, bsub);
                        _delta_descendents = total_delta;
                        return final_result;
                     }
                  }
                  _delta_descendents = total_delta;
                  return result;
               }
               if (!boundary_empty && has_boundary && boundary_changed)
               {
                  // Boundary position in new node: remove_lo (start of where removed block was)
                  branch_number new_boundary_pos = branch_number(remove_lo);
                  if (!start_empty)
                     new_boundary_pos = branch_number(remove_lo);  // already correct
                  _delta_descendents = 0;
                  branch_set bsub(new_boundary_addr);
                  auto final_result = merge_branches<upsert_mode::unique>(parent_hint, new_ref, new_boundary_pos,
                                                             bsub);
                  _delta_descendents = total_delta;
                  return final_result;
               }

               return new_addr;
            }

            // Simple case: no address changes, just remove the range
            op::inner_remove_range rm{branch_number(remove_lo), branch_number(remove_hi),
                                      total_delta};
            if constexpr (is_inner_node<NodeT>)
               return _session.alloc<NodeT>(parent_hint, node.obj(), rm);
            else
               return _session.alloc<NodeT>(parent_hint, node->prefix(), node.obj(), rm);
         }
      }
   }

}  // namespace psitri
