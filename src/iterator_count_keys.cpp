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
   // Forward declaration for internal implementation
   size_t count_keys(read_lock& state, const inner_node_concept auto* node, key_range range);

   // Specialized forward declarations for binary_node and value_node
   size_t count_keys(read_lock& state, const binary_node* node, key_range range);

   size_t count_keys(read_lock& state, const value_node* node, key_range range);

   /**
 * Find the local index corresponding to the end of a range on a node
 */
   local_index find_upper_bound_index(const inner_node_concept auto* node, const key_range& range)
   {
      // an empty upper bound means unbounded, but node's upper_bound_index() lacks the
      // context of the range() and therefore treates empty as a valid key less than any
      // other key.  Therefore, we use this helper function.
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
                             local_index                    start_idx,
                             local_index                    end_idx,
                             const key_range&               range,
                             const node_header*             boundary_branch_node)
   {
      // Get total descendants for this node
      size_t total_descendants = node->descendants();

      // Count keys outside our target range:
      // 1. Everything before the lower bound
      size_t before_count = 0;
      if (!range.lower_bound.empty())
      {
         // Create a range from empty to lower_bound (exclusive)
         key_range before_range{key_view(), range.lower_bound};

         // Count keys in branches before the start_idx
         local_index before_end_idx = node->lower_bound_index(range.lower_bound);
         before_count +=
             count_keys_in_branches(state, node, node->begin_index(), before_end_idx, before_range);

         // Count keys in the boundary branch if needed
         if (before_end_idx < node->end_index() && !range.lower_bound.empty() &&
             node->get_branch_key(before_end_idx)[0] == range.lower_bound[0])
         {
            id_address branch_addr = node->get_branch(before_end_idx);
            before_count += state.call_with_node(
                branch_addr,
                [&](const auto* typed_node) -> size_t
                {
                   key_range child_range{key_view(), range.lower_bound.substr(1)};
                   return count_keys(state, typed_node, child_range);
                });
         }
      }

      // 2. Everything at or after the upper bound
      size_t after_count = 0;
      if (!range.upper_bound.empty())
      {
         // Create a range from upper_bound (inclusive) to empty
         key_range after_range{range.upper_bound, key_view()};

         // Count keys in branches from end_idx to the end
         local_index after_start_idx = node->lower_bound_index(after_range.lower_bound);
         after_count +=
             count_keys_in_branches(state, node, after_start_idx, node->end_index(), after_range);

         // Count keys in the boundary branch if needed
         if (boundary_branch_node)
         {
            // The boundary branch requires special processing because it contains
            // the byte matching exactly the first byte of the upper bound
            after_count += cast_and_call(
                boundary_branch_node,
                [&](const auto* typed_node) -> size_t
                {
                   // Create a range that represents keys >= upper_bound
                   // This correctly uses the remaining part of upper_bound after the first byte
                   key_range boundary_range = range.with_advanced_to();

                   // Count keys in the branch that fall in the after range
                   return count_keys(state, typed_node, boundary_range);
                });
         }
      }

      // Subtract both from total
      size_t in_range_count = total_descendants - before_count - after_count;

      // Assert that we haven't counted more keys than possible
      assert(in_range_count <= total_descendants &&
             "Key count calculation error: counted more keys than exist");

      return in_range_count;
   }

   /**
    * Find the "end branch" that requires special processing during range traversal.
    * 
    * The "end branch" is the branch at the upper boundary of our range that may 
    * contain both in-range and out-of-range keys.
    */
   const node_header* find_range_boundary_branch(read_lock&                     state,
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
               return state.get(node->get_branch(end_idx)).header();
         }
      }
      return nullptr;  // No special branch found
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

         // Skip branches with prefix greater than upper bound
         if (!range.upper_bound.empty() && static_cast<unsigned char>(branch_key[0]) >
                                               static_cast<unsigned char>(range.upper_bound[0]))
            continue;

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
                : (range.upper_bound[0] == range.lower_bound[0])
                    ? range.upper_bound.substr(1)
                    : key_view();  // If first byte doesn't match, no upper bound in this branch

            child_range = {next_from, next_to};
         }
         else if (!range.upper_bound.empty())
         {
            // Past lower bound with upper bound
            if (static_cast<unsigned char>(branch_key[0]) <
                static_cast<unsigned char>(range.upper_bound[0]))
               child_range = {key_view(), key_view()};  // Fully unbounded for upper limit
            else if (branch_key[0] == range.upper_bound[0])
               child_range = {key_view(), range.upper_bound.substr(1)};
         }
         else
            child_range = {key_view(), key_view()};  // Fully unbounded

         branch_count += state.call_with_node(node->get_branch(idx),
                                              [&](const auto* typed_node) -> size_t {
                                                 return count_keys(state, typed_node, child_range);
                                              });
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
      // Count branches in the range (optimized to use the most efficient counting method)
      size_t in_range_branches     = count_branches_in_range(node, start_idx, end_idx);
      size_t total_branches        = node->num_branches();
      size_t out_of_range_branches = total_branches - in_range_branches;

      // Use exclusion-based counting if there are more in-range branches than out-of-range branches
      return in_range_branches > out_of_range_branches;
   }

   /**
    * Count keys within a given range in the trie, this is specialized for inner_node_concept
    */
   size_t count_keys(read_lock& state, const inner_node_concept auto* node, key_range range)
   {
      // Get the node's prefix
      key_view node_prefix = node->get_prefix();

      // Try to narrow the range by the node's prefix, removing the common prefix
      if (!range.try_narrow_with_prefix(&node_prefix))
         return 0;  // No intersection with range

      // at this point node_prefix has had the common prefix removed

      // Check if the entire node (including its subtree) is in range
      if (range.is_unbounded())
         return node->descendants();

      // Start with count of node's value if in range
      size_t count = size_t(node->has_eof_value() && range.contains_key(node->get_prefix()));

      // Skip children processing if node's prefix exceeds upper bound
      if (range.key_exceeds_range(node_prefix))
         return count;

      // Find branch indices for traversal
      local_index start_idx = node->lower_bound_index(range.lower_bound);

      if (start_idx == node->end_index())
         return count;  // No branches in range

      local_index end_idx = find_upper_bound_index(node, range);

      const auto* end_branch_node = find_range_boundary_branch(state, node, end_idx, range);

      // Determine if exclusion-based counting is more efficient
      if (should_count_by_exclusion(node, start_idx, end_idx))
         return count_by_exclusion(state, node, start_idx, end_idx, range, end_branch_node);

      // Traditional approach: count keys in all in-range branches
      count += count_keys_in_branches(state, node, start_idx, end_idx, range);

      // Count keys in the boundary branch if needed
      if (end_branch_node)
         count += cast_and_call(end_branch_node,
                                [&](const auto* typed_node) -> size_t {
                                   return count_keys(state, typed_node, range.with_advanced_to());
                                });

      return count;
   }

   /**
    * Specialized implementation for binary_node
    * Binary nodes store a sorted set of complete keys with their values
    */
   size_t count_keys(read_lock& state, const binary_node* node, key_range range)
   {
      // Binary nodes already have efficient methods to find the indices for range bounds
      local_index lower_idx = node->lower_bound_index(range.lower_bound);
      local_index upper_idx = node->upper_bound_index(range.upper_bound);

      // Calculate the count directly from indices
      return upper_idx - lower_idx;
   }

   /**
    * Specialized implementation for value_node
    * Value nodes represent leaf nodes with a single key-value pair
    */
   size_t count_keys(read_lock& state, const value_node* node, key_range range)
   {
      // Standard range check
      return size_t(range.contains_key(node->get_prefix()));
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

      return state.call_with_node(
          root,
          [&](const auto* typed_node) {
             return count_keys(state, typed_node, {lower_bound, upper_bound});
          });
   }

}  // namespace arbtrie
