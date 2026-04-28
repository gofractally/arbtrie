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
      _delta_removed_keys = 0;

      // Map max_key -> empty for internal unbounded representation
      key_range range = {lower, upper == max_key ? key_view() : upper};

      auto rref     = *_root;
      auto old_addr = _root.take();
      // When ref > 1, range_remove delegates to shared mode, and the shared
      // dispatch releases the ref. Only release here when uniquely owned.
      bool uniquely_owned = (rref.ref() == 1);

      branch_set result = range_remove<upsert_mode::unique>({}, rref, range);

      if (result.count() == 0)
      {
         if (uniquely_owned)
            _session.release(old_addr);
         // else: shared dispatch already released via delegation
      }
      else if (result.count() == 1)
         _root.give(result.get_first_branch());
      else
         _root.give(make_inner(result));

      return static_cast<uint64_t>(-_delta_removed_keys);
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
               _delta_removed_keys -= 1;
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

      if constexpr (!mode.is_unique())
      {
         if (!result.contains(ref.address())) [[likely]]
            ref.release();
      }

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
         _delta_removed_keys -= count;
         return {};
      }

      _delta_removed_keys -= count;

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

         auto                  rewrite_plan = make_leaf_rewrite_plan_skipping(*leaf.obj(), lo, hi);
         auto                  rewrite_policy = leaf_rewrite_policy(rewrite_plan);
         op::leaf_remove_range rm{
             .src = *leaf.obj(), .lo = lo, .hi = hi, .rewrite = &rewrite_policy};
         if (!leaf->rebuilt_size_fits(rm))
         {
            release_leaf_rewrite_replacements(rewrite_plan);
            rm.rewrite = nullptr;
         }
         auto result = _session.alloc<leaf_node>(parent_hint, rm);
         if (rm.rewrite)
            release_leaf_rewrite_sources(rewrite_plan);
         return result;
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
         for (uint16_t i = 0; i < node->num_branches(); ++i)
            _delta_removed_keys -= psitri::count_child_keys(_session, node->get_branch(branch_number(i)));
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
         // Track if child ref>1: if so, range_remove<unique> will delegate to shared
         // and the shared dispatch will release badr when result is empty.
         [[maybe_unused]] bool badr_shared = (bref.ref() > 1);

         if constexpr (mode.is_shared())
            retain_children(node);

         branch_set sub = range_remove<mode>(node->get_branch_clines(), bref, range);

         if (sub.count() == 0)
         {
            // This branch became empty — treat like single-branch remove
            if (node->num_branches() == 1)
            {
               if constexpr (mode.is_unique())
               {
                  // Return {} so parent cascade releases this node + its children.
                  // If the child was shared (ref>1), the shared dispatch already released
                  // it; retain to prevent double-release from cascade.
                  if (badr_shared)
                     _session.retain(badr);
               }
               return {};
            }

            if constexpr (mode.is_unique())
            {
               // badr is no longer in the node after remove_branch; release it explicitly
               // UNLESS the shared dispatch already released it (child had ref > 1).
               if (!badr_shared)
                  _session.release(badr);
	               node.modify(
	                   [&](auto* n)
	                   {
	                      n->remove_branch(start);
	                      n->set_last_unique_version(_root_version);
	                   });
	               return node.address();
            }
            else
            {
	               op::inner_remove_branch rm{start};
	               if constexpr (is_inner_node<NodeT>)
	               {
	                  auto result = _session.alloc<NodeT>(parent_hint, node.obj(), rm);
	                  auto ref = _session.get_ref<NodeT>(result);
	                  ref.modify()->set_last_unique_version(_root_version);
	                  return result;
	               }
	               else
	               {
	                  auto result = _session.alloc<NodeT>(parent_hint, node->prefix(), node.obj(), rm);
	                  auto ref = _session.get_ref<NodeT>(result);
	                  ref.modify()->set_last_unique_version(_root_version);
	                  return result;
	               }
            }
         }

         // Branch still exists
         if constexpr (mode.is_unique())
         {
            if (sub.count() == 1 && sub.get_first_branch() == badr)
               return node.address();
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

      int64_t saved_delta = _delta_removed_keys;

      // Count keys in middle branches (fully contained — will be removed entirely)
      int64_t middle_keys = 0;
      for (uint16_t i = *start + 1; i < *end; ++i)
         middle_keys += psitri::count_child_keys(_session, node->get_branch(branch_number(i)));

      // Recurse into start branch
      ptr_address start_addr     = node->get_branch(start);
      bool        start_empty    = false;
      bool        start_changed  = false;
      ptr_address new_start_addr = start_addr;
      bool        start_recursed = false;
      [[maybe_unused]] bool start_shared = false;

      if (!range.lower_bound.empty())
      {
         start_recursed = true;
         auto start_ref = _session.get_ref(start_addr);
         start_shared   = (start_ref.ref() > 1);
         _delta_removed_keys = 0;
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
         _delta_removed_keys = 0;
         _delta_removed_keys -= psitri::count_child_keys(_session, start_addr);
         start_empty = true;
      }
      int64_t start_delta = _delta_removed_keys;

      // Recurse into boundary branch
      ptr_address boundary_addr     = sal::null_ptr_address;
      bool        boundary_empty    = false;
      bool        boundary_changed  = false;
      ptr_address new_boundary_addr = sal::null_ptr_address;
      int64_t     boundary_delta    = 0;
      [[maybe_unused]] bool boundary_shared = false;

      if (has_boundary)
      {
         boundary_addr     = node->get_branch(boundary);
         new_boundary_addr = boundary_addr;
         auto boundary_ref = _session.get_ref(boundary_addr);
         boundary_shared   = (boundary_ref.ref() > 1);
         _delta_removed_keys = 0;
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
         boundary_delta = _delta_removed_keys;
      }

      // Compute total delta
      int64_t total_delta = saved_delta + start_delta + boundary_delta - middle_keys;
      _delta_removed_keys  = total_delta;

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
            // Return {} so the parent cascade releases this node + all children.
            // Middle branches and non-recursed start/boundary are still in the branch
            // table — cascade handles them correctly.
            //
            // For recursed children that were shared (ref>1), the unique dispatch
            // delegated to shared mode which already released them. But they're still
            // in the branch table, so cascade would release again → retain to compensate.
            if (start_recursed && start_shared)
               _session.retain(start_addr);
            if (has_boundary && boundary_shared)
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
         // Release middle branches (not recursed into — always safe to release)
         for (uint16_t i = *start + 1; i < *end; ++i)
            _session.release(node->get_branch(branch_number(i)));

         // Release start if it was completely removed.
         // Skip if the shared dispatch already released it (recursed + shared).
         if (start_empty)
         {
            if (!(start_recursed && start_shared))
               _session.release(start_addr);
         }

         // Release boundary if it was completely removed by recursion.
         // Skip if the shared dispatch already released it (always recursed when has_boundary).
         if (boundary_empty && has_boundary)
         {
            if (!boundary_shared)
               _session.release(boundary_addr);
         }

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
                });

            // Handle changed addresses after the remove_range.
            // After remove_range, boundary's position shifted to remove_lo.
            if (!start_empty && start_changed && !boundary_empty && has_boundary && boundary_changed)
            {
               // Both changed — handle sequentially
               _delta_removed_keys = 0;
               branch_set ssub(new_start_addr);
               auto result = merge_branches<upsert_mode::unique>(parent_hint, node, start, ssub);
               if (result.count() == 1)
               {
                  auto result_ref = _session.template get_ref<NodeT>(result.get_first_branch());
                  auto boundary_new_pos = branch_number(remove_lo);
                  branch_set bsub(new_boundary_addr);
                  result = merge_branches<upsert_mode::unique>(parent_hint, result_ref, boundary_new_pos, bsub);
               }
               _delta_removed_keys = total_delta;
               return result;
            }
            if (!boundary_empty && has_boundary && boundary_changed)
            {
               auto boundary_new_pos = branch_number(remove_lo);
               _delta_removed_keys = 0;
               branch_set bsub(new_boundary_addr);
               auto result = merge_branches<upsert_mode::unique>(parent_hint, node, boundary_new_pos, bsub);
               _delta_removed_keys = total_delta;
               return result;
            }
            if (!start_empty && start_changed)
            {
               _delta_removed_keys = 0;
               branch_set ssub(new_start_addr);
               auto result = merge_branches<upsert_mode::unique>(parent_hint, node, start, ssub);
               _delta_removed_keys = total_delta;
               return result;
            }

            return node.address();
         }
         else
         {
            // Nothing to remove from inner node (all survived)
            // Handle changed addresses.
            if (!start_empty && start_changed)
            {
               _delta_removed_keys = 0;
               branch_set ssub(new_start_addr);
               auto result = merge_branches<upsert_mode::unique>(parent_hint, node, start, ssub);

               // If boundary also changed, apply it to the result node
               if (!boundary_empty && has_boundary && boundary_changed && result.count() == 1)
               {
                  auto result_ref = _session.template get_ref<NodeT>(result.get_first_branch());
                  branch_set bsub(new_boundary_addr);
                  result = merge_branches<upsert_mode::unique>(parent_hint, result_ref, boundary, bsub);
               }
               _delta_removed_keys = total_delta;
               return result;
            }
            if (!boundary_empty && has_boundary && boundary_changed)
            {
               _delta_removed_keys = 0;
               branch_set bsub(new_boundary_addr);
               auto result = merge_branches<upsert_mode::unique>(parent_hint, node, boundary, bsub);
               _delta_removed_keys = total_delta;
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
            // For simplicity, use merge_branches for the changed branches.

            if (!start_changed && !boundary_changed)
            {
               // Nothing actually changed — retain_children was speculative, undo it.
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
               _delta_removed_keys = 0;  // merge_branches uses _delta_removed_keys
               auto after_start = merge_branches<mode.make_shared_or_unique_only()>(
                   parent_hint, node, start, ssub);
               // after_start is a new node. Replace boundary in it.
               // merge_branches in shared mode allocates a new node, so we get a smart_ref to it.
               if (after_start.count() == 1)
               {
                  auto after_ref = _session.template get_ref<NodeT>(after_start.get_first_branch());
                  branch_set bsub(new_boundary_addr);
                  _delta_removed_keys = total_delta;  // apply the full delta on the second merge
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
               op::inner_remove_range rm{branch_number(remove_lo), branch_number(remove_hi)};
               ptr_address new_addr;
	               if constexpr (is_inner_node<NodeT>)
	                  new_addr = _session.alloc<NodeT>(parent_hint, node.obj(), rm);
	               else
	                  new_addr = _session.alloc<NodeT>(parent_hint, node->prefix(), node.obj(), rm);

	               auto new_ref = _session.get_ref<NodeT>(new_addr);
	               new_ref.modify()->set_last_unique_version(_root_version);

               // Now replace changed branches
               if (!start_empty && start_changed)
               {
                  // Find position of start in new node
                  branch_number new_start_pos = start;  // same position (we removed after start)
                  _delta_removed_keys = 0;
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
                        _delta_removed_keys = 0;
                        branch_set bsub(new_boundary_addr);
                        auto final_result = merge_branches<upsert_mode::unique>(parent_hint, result_ref,
                                                                   new_boundary_pos, bsub);
                        _delta_removed_keys = total_delta;
                        return final_result;
                     }
                  }
                  _delta_removed_keys = total_delta;
                  return result;
               }
               if (!boundary_empty && has_boundary && boundary_changed)
               {
                  // Boundary position in new node: remove_lo (start of where removed block was)
                  branch_number new_boundary_pos = branch_number(remove_lo);
                  if (!start_empty)
                     new_boundary_pos = branch_number(remove_lo);  // already correct
                  _delta_removed_keys = 0;
                  branch_set bsub(new_boundary_addr);
                  auto final_result = merge_branches<upsert_mode::unique>(parent_hint, new_ref, new_boundary_pos,
                                                             bsub);
                  _delta_removed_keys = total_delta;
                  return final_result;
               }

               return new_addr;
            }

            // Simple case: no address changes, just remove the range
	            op::inner_remove_range rm{branch_number(remove_lo), branch_number(remove_hi)};
	            if constexpr (is_inner_node<NodeT>)
	            {
	               auto result = _session.alloc<NodeT>(parent_hint, node.obj(), rm);
	               auto ref = _session.get_ref<NodeT>(result);
	               ref.modify()->set_last_unique_version(_root_version);
	               return result;
	            }
	            else
	            {
	               auto result = _session.alloc<NodeT>(parent_hint, node->prefix(), node.obj(), rm);
	               auto ref = _session.get_ref<NodeT>(result);
	               ref.modify()->set_last_unique_version(_root_version);
	               return result;
	            }
         }
      }
   }

}  // namespace psitri
