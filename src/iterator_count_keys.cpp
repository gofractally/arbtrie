#include <arbtrie/binary_node.hpp>
#include <arbtrie/concepts.hpp>
#include <arbtrie/full_node.hpp>
#include <arbtrie/inner_node.hpp>
#include <arbtrie/iterator.hpp>
#include <arbtrie/node_header.hpp>
#include <arbtrie/setlist_node.hpp>
#include <arbtrie/value_node.hpp>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace arbtrie
{
   // Debug level counter to track recursion depth
   thread_local int debug_level = 0;

   // Debug indent based on recursion level
   std::string debug_indent()
   {
      return std::string(debug_level * 2, ' ');
   }

   // Forward declaration for internal implementation
   size_t count_keys(read_lock&                     state,
                     const inner_node_concept auto* node,
                     id_address                     node_addr,
                     key_range                      range);

   // Specialized forward declarations for binary_node and value_node
   size_t count_keys(read_lock&         state,
                     const binary_node* node,
                     id_address         node_addr,
                     key_range          range);

   size_t count_keys(read_lock&        state,
                     const value_node* node,
                     id_address        node_addr,
                     key_range         range);

   /**
 * Find the local index corresponding to the end of a range on a node
 */
   local_index find_range_end_index(const inner_node_concept auto* node, const key_range& range)
   {
      if (range.upper_bound.empty())
         return node->end_index();

      // Get the local index of the first branch > end_byte
      return node->upper_bound_index(range.upper_bound);
   }

   /**
 * Count branches within a specific index range
 * 
 * @return The number of branches in the range
 */
   size_t count_branches_in_range(const inner_node_concept auto* node,
                                  local_index                    start_idx,
                                  local_index                    end_idx)
   {
      // For setlist_node which has contiguous indices, we can directly compute the difference
      if constexpr (is_setlist_node<decltype(node)>)
      {
         // Assert that end_idx is valid (will be checked in debug builds only)
         assert(end_idx <= node->end_index() && "end_idx should never exceed node->end_index()");

         // Simply compute the difference between indices for contiguous index types
         return end_idx - start_idx;
      }
      else  // full_node which may have gaps
      {
         // First, compare the index ranges to decide which is likely to have fewer branches
         size_t in_range_steps     = end_idx - start_idx;
         size_t out_of_range_steps = 256 - in_range_steps;

         // Choose the more efficient counting method based on range size
         if (in_range_steps <= out_of_range_steps)
         {
            // Directly count in-range branches
            size_t count = 0;
            for (local_index idx = start_idx; idx != node->end_index() && idx < end_idx;
                 idx             = node->next_index(idx))
               count++;
            return count;
         }
         else
         {
            // Count out-of-range branches and subtract from total
            size_t before_range_count = 0;
            for (local_index idx = node->begin_index(); idx != start_idx;
                 idx             = node->next_index(idx))
               before_range_count++;

            size_t after_range_count = 0;
            for (local_index idx = end_idx; idx != node->end_index(); idx = node->next_index(idx))
               after_range_count++;

            return node->num_branches() - (before_range_count + after_range_count);
         }
      }
   }

   uint32_t descendants(auto&& obj_ref)
   {
      // even though value nodes have a descendants() method,
      // we should avoid dereferencing the node and bringing it into cpu cache
      // or worse yet, a page fault if the node is not in cache
      if (obj_ref.type() == node_type::value)
         return 1;

      return cast_and_call(obj_ref.header(),
                           [&](const auto* typed_node) -> size_t
                           { return typed_node->descendants(); });
   }

   /**
    * Subtract keys outside range from the total count using the descendants() optimization
    */
   size_t count_by_exclusion(read_lock&                     state,
                             const inner_node_concept auto* node,
                             id_address                     node_addr,
                             local_index                    start_idx,
                             local_index                    end_idx,
                             const key_range&               range,
                             id_address                     boundary_branch_addr,
                             const node_header*             boundary_branch_node)
   {
      // Start with total descendants count
      size_t count = node->descendants();

      // Subtract the current node's value if it's not in range
      if (node->has_eof_value() && !range.contains_key(node->get_prefix()))
      {
         count--;
      }

      // Ensure idx is valid when we start - if begin_index returns -1, get the first valid index
      local_index idx = node->begin_index();
      if (idx.to_int() < 0)
      {
         idx = node->next_index(idx);
      }

      // Subtract all branches before start_idx
      for (; idx < start_idx; idx = node->next_index(idx))
      {
         // Get the branch and its descendants
         id_address branch_addr        = node->get_branch(idx);
         size_t     branch_descendants = descendants(state.get(branch_addr));

         count -= branch_descendants;
      }

      // there is a partial part of start_idx that we need to process
      // it is the part that is before the first branch that is in range
      // we need to process this partial part by counting the keys in it
      // and subtracting the count from the total
      // we need to do this because the branches before start_idx are not
      // in the range and we need to subtract their descendants from the total

      // Process the start_idx branch if it exists and contains the range boundary
      if (start_idx < node->end_index() && !range.lower_bound.empty())
      {
         // Get branch key for comparison
         key_view branch_key = node->get_branch_key(start_idx);

         // If this branch contains the lower bound byte exactly
         if (static_cast<unsigned char>(branch_key[0]) ==
             static_cast<unsigned char>(range.lower_bound[0]))
         {
            // Get the branch node
            id_address branch_addr = node->get_branch(start_idx);
            object_ref obj_ref     = state.get(branch_addr);

            // Cast and process the branch
            cast_and_call(obj_ref.header(),
                          [&](const auto* typed_start_node)
                          {
                             // Create an advanced range with the lower bound prefix removed
                             key_range advanced_range = range.with_advanced_from();

                             // Count keys in this branch that are in range
                             size_t keys_in_range =
                                 count_keys(state, typed_start_node, branch_addr, advanced_range);

                             // Get total keys in start boundary branch
                             size_t all_keys_in_branch = typed_start_node->descendants();

                             // Calculate how many keys are NOT in our range
                             size_t keys_outside_range = all_keys_in_branch - keys_in_range;

                             // Guard against calculation errors
                             if (keys_in_range > all_keys_in_branch)
                             {
                                keys_in_range      = all_keys_in_branch;
                                keys_outside_range = 0;
                             }

                             // If we somehow have more out-of-range keys than total, fix it
                             if (keys_outside_range > all_keys_in_branch)
                             {
                                keys_outside_range = all_keys_in_branch;
                             }

                             // Subtract the out-of-range keys from our total count
                             count -= keys_outside_range;
                          });
         }
      }

      // Process branches after end_idx
      // Ensure we handle the case of end_idx properly
      if (end_idx < node->end_index())  // Only if there are branches after end_idx
      {
         local_index after_idx = end_idx;

         // Iterate all branches at or after end_idx
         for (; after_idx != node->end_index(); after_idx = node->next_index(after_idx))
         {
            // Skip the special boundary branch if needed
            if (after_idx == end_idx && boundary_branch_node != nullptr)
            {
               continue;
            }

            // Get the branch and its descendants
            id_address branch_addr        = node->get_branch(after_idx);
            size_t     branch_descendants = descendants(state.get(branch_addr));

            count -= branch_descendants;
         }
      }

      // Process the boundary branch if it exists
      if (boundary_branch_node)
      {
         // Get the branch key for logging
         key_view branch_key = node->get_branch_key(end_idx);

         // Cast the boundary branch node once and use it for all operations
         cast_and_call(boundary_branch_node,
                       [&](const auto* typed_end_node)
                       {
                          // Prepare the advanced range for boundary branch
                          key_range advanced_range = range.with_advanced_to();

                          // Directly count keys in this branch that are in range
                          size_t keys_in_boundary = count_keys(
                              state, typed_end_node, boundary_branch_addr, advanced_range);

                          // Get total keys in boundary branch
                          size_t all_keys_in_boundary = typed_end_node->descendants();

                          // Calculate how many keys are NOT in our range
                          size_t keys_outside_range = all_keys_in_boundary - keys_in_boundary;

                          // Guard against boundary branch calculation errors
                          // If we somehow have more in-range keys than total, fix it
                          if (keys_in_boundary > all_keys_in_boundary)
                          {
                             keys_in_boundary   = all_keys_in_boundary;
                             keys_outside_range = 0;
                          }

                          // If we somehow have more out-of-range keys than total, fix it
                          if (keys_outside_range > all_keys_in_boundary)
                          {
                             keys_outside_range = all_keys_in_boundary;
                          }

                          // Final sanity check to ensure we're not counting in_range + out_of_range > total
                          if (keys_in_boundary + keys_outside_range > all_keys_in_boundary)
                          {
                             keys_outside_range = all_keys_in_boundary - keys_in_boundary;
                          }

                          // Subtract the out-of-range keys from our total count
                          count -= keys_outside_range;
                       });
      }

      // Verify the count is sensible (not negative)
      if (count > node->descendants())
      {
         count = node->descendants();
      }

      return count;
   }

   /**
 * Find the "end branch" that requires special processing during range traversal.
 * 
 * The "end branch" is the branch at the upper boundary of our range that may 
 * contain both in-range and out-of-range keys.
 */
   std::pair<id_address, const node_header*> find_range_boundary_branch(
       read_lock&                     state,
       const inner_node_concept auto* node,
       local_index                    end_idx,
       const key_range&               range)
   {
      // We need special processing if:
      // 1. end_idx is valid (within node bounds)
      // 2. We have a non-empty upper bound
      // 3. The branch at end_idx matches the upper bound byte exactly

      if (end_idx < node->end_index() && !range.upper_bound.empty())
      {
         // Get the branch key for comparison
         key_view branch_key = node->get_branch_key(end_idx);

         // Only process if the branch key matches the exact first byte of upper bound
         // Use unsigned char comparison to avoid sign extension issues
         if (static_cast<unsigned char>(branch_key[0]) ==
             static_cast<unsigned char>(range.get_end_byte()))
         {
            // Check if this is more than just a match on the last byte
            if (!range.is_last_byte_of_end())
            {
               // Get the branch value at this index and return it with its header
               id_address branch_addr = node->get_branch(end_idx);
               // We can assume branch_addr is valid based on tree invariants
               object_ref obj_ref = state.get(branch_addr);
               return {branch_addr, obj_ref.header()};
            }
         }
      }

      return {id_address{}, nullptr};  // No special branch found
   }

   /**
 * Count keys in the branches that fall within the specified range
 */
   size_t count_keys_in_branches(read_lock&                     state,
                                 const inner_node_concept auto* node,
                                 local_index                    start_idx,
                                 local_index                    end_idx,
                                 const key_range&               range)
   {
      size_t branch_count = 0;
      if (start_idx == node->begin_index())
         start_idx = node->next_index(start_idx);

      // Iterate through all branches in the range
      for (local_index idx = start_idx; idx < end_idx; idx = node->next_index(idx))
      {
         // Skip invalid branches
         if (idx == node->end_index())
            break;

         // Get branch key and create object reference
         key_view branch_key = node->get_branch_key(idx);
         auto     obj_ref    = state.get(node->get_branch(idx));

         // Skip branches with prefix greater than upper bound
         if (!range.upper_bound.empty() && static_cast<unsigned char>(branch_key[0]) >
                                               static_cast<unsigned char>(range.upper_bound[0]))
         {
            continue;
         }

         // Create appropriate child range
         key_range child_range;

         // If this branch matches the current lower bound exactly, adapt the range
         if (!range.lower_bound.empty() && idx == start_idx &&
             static_cast<unsigned char>(branch_key[0]) ==
                 static_cast<unsigned char>(range.lower_bound[0]))
         {
            key_view next_from = range.lower_bound.substr(1);
            key_view next_to =
                range.upper_bound.empty() ? key_view()
                : (static_cast<unsigned char>(range.upper_bound[0]) ==
                   static_cast<unsigned char>(range.lower_bound[0]))
                    ? range.upper_bound.substr(1)
                    : key_view();  // If first byte doesn't match, no upper bound in this branch

            child_range = {next_from, next_to};
         }
         else if (!range.upper_bound.empty())
         {
            // Past lower bound with upper bound
            if (static_cast<unsigned char>(branch_key[0]) <
                static_cast<unsigned char>(range.upper_bound[0]))
            {
               child_range = {key_view(), key_view()};  // Fully unbounded for upper limit
            }
            else if (static_cast<unsigned char>(branch_key[0]) ==
                     static_cast<unsigned char>(range.upper_bound[0]))
            {
               child_range = {key_view(), range.upper_bound.substr(1)};
            }
         }
         else
         {
            child_range = {key_view(), key_view()};  // Fully unbounded
         }

         // Count keys in this branch
         size_t branch_keys = cast_and_call(
             obj_ref.header(),
             [&](const auto* typed_node) -> size_t
             { return count_keys(state, typed_node, node->get_branch(idx), child_range); });

         branch_count += branch_keys;
      }

      return branch_count;
   }

   /**
 * Determine whether to use exclusion-based counting or traditional counting
 * based on which approach would require fewer operations.
 * 
 * @return true if exclusion-based counting should be used, false otherwise
 */
   bool should_count_by_exclusion(const inner_node_concept auto* node,
                                  local_index                    start_idx,
                                  local_index                    end_idx)
   {
      // Temporarily disable exclusion-based counting until it's fixed
      return false;

      // Count branches in the range (optimized to use the most efficient counting method)
      size_t in_range_branches     = count_branches_in_range(node, start_idx, end_idx);
      size_t total_branches        = node->num_branches();
      size_t out_of_range_branches = total_branches - in_range_branches;

      // Use exclusion-based counting if there are more in-range branches than out-of-range branches
      return in_range_branches > out_of_range_branches;
   }

   /**
    * Count keys within a given range in the trie, using a typed node.
    * This function handles a node that has already been retrieved and typed.
    */
   size_t count_keys(read_lock&                     state,
                     const inner_node_concept auto* node,
                     id_address                     node_addr,
                     key_range                      range)
   {
      // Get the node's prefix
      key_view node_prefix = node->get_prefix();

      // Try to narrow the range by the node's prefix, removing the common prefix
      if (!range.try_narrow_with_prefix(&node_prefix))
      {
         return 0;  // No intersection with range
      }

      // Check if the entire node (including its subtree) is in range
      if (range.is_unbounded())
      {
         // Use descendants() to get the total count directly for the entire subtree
         size_t count = node->descendants();
         return count;
      }

      // Start with count of node's value if in range
      size_t count = size_t(node->has_eof_value() && range.contains_key(node->get_prefix()));

      // Skip children processing if node's prefix exceeds upper bound
      if (range.key_exceeds_range(node_prefix))
      {
         return count;
      }

      // Find branch indices for traversal
      local_index start_idx = node->lower_bound_index(range.lower_bound);

      if (start_idx == node->end_index())
      {
         return count;  // No branches in range
      }

      local_index end_idx = find_range_end_index(node, range);

      // Find end branch for special processing
      auto [end_branch_addr, end_branch_node] =
          find_range_boundary_branch(state, node, end_idx, range);

      // Determine if exclusion-based counting is more efficient
      bool use_exclusion = should_count_by_exclusion(node, start_idx, end_idx);

      if (use_exclusion)
      {
         size_t exclusion_count = count_by_exclusion(state, node, node_addr, start_idx, end_idx,
                                                     range, end_branch_addr, end_branch_node);
         return exclusion_count;
      }

      // Traditional approach: count keys in all in-range branches
      size_t branch_count = count_keys_in_branches(state, node, start_idx, end_idx, range);
      count += branch_count;

      // Count keys in the boundary branch if needed
      if (end_branch_node)
      {
         size_t boundary_count = cast_and_call(
             end_branch_node,
             [&](const auto* typed_node) -> size_t
             { return count_keys(state, typed_node, end_branch_addr, range.with_advanced_to()); });

         count += boundary_count;
      }

      return count;
   }

   /**
 * Specialized implementation for binary_node
 * Binary nodes store a sorted set of complete keys with their values
 */
   size_t count_keys(read_lock&         state,
                     const binary_node* node,
                     id_address         node_addr,
                     key_range          range)
   {
      // Binary nodes already have efficient methods to find the indices for range bounds
      local_index lower_idx = node->lower_bound_index(range.lower_bound);
      local_index upper_idx = node->upper_bound_index(range.upper_bound);

      // Debug: log each key in the range
      size_t count = 0;
      for (local_index i = lower_idx; i != node->end_index() && i != upper_idx;
           i             = node->next_index(i))
      {
         count++;
      }

      // Calculate the count directly from indices
      size_t index_based_count = upper_idx - lower_idx;

      return index_based_count;
   }

   /**
    * Specialized implementation for value_node
    * Value nodes represent leaf nodes with a single key-value pair
    */
   size_t count_keys(read_lock&        state,
                     const value_node* node,
                     id_address        node_addr,
                     key_range         range)
   {
      // Standard range check
      return size_t(range.contains_key(node->get_prefix()));
   }

   /**
    * Count keys within a given range in the trie.
    * Internal helper function that counts keys starting from a given node.
    */
   size_t count_keys_in_range(read_lock& state, id_address node_addr, const key_range& range)
   {
      // Get the node reference and header
      object_ref obj_ref = state.get(node_addr);

      // Dispatch to the specialized version with the correct node type
      return cast_and_call(obj_ref.header(),
                           [&](const auto* typed_node) -> size_t
                           { return count_keys(state, typed_node, node_addr, range); });
   }

   /**
    * Count keys within a given range in the trie
    * 
    * @param state The read lock to access nodes
    * @param root The root node address
    * @param begin Lower bound of the range (inclusive)
    * @param end Upper bound of the range (exclusive)
    * @return Number of keys in the range [begin, end)
    */
   size_t count_keys_impl(read_lock& state,
                          id_address root,
                          key_view   lower_bound,
                          key_view   upper_bound)
   {
      if (!root)
         return 0;

      return count_keys_in_range(state, root, {lower_bound, upper_bound});
   }

}  // namespace arbtrie
