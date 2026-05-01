#include <psitri/database.hpp>
#include <psitri/node/inner.hpp>
#include <psitri/node/leaf.hpp>
#include <psitri/node/value_node.hpp>
#include <sal/allocator_session_impl.hpp>
#include <algorithm>
#include <string_view>
#include <unordered_set>

namespace psitri::detail
{
   void register_node_types(sal::allocator& alloc)
   {
      alloc.register_type_ops<leaf_node>();
      alloc.register_type_ops<inner_node>();
      alloc.register_type_ops<inner_prefix_node>();
      alloc.register_type_ops<wide_inner_node>();
      alloc.register_type_ops<direct_inner_node>();
      alloc.register_type_ops<bplus_inner_node>();
      alloc.register_type_ops<value_node>();
   }

   // database method implementations now live in database_impl.hpp as
   // templated basic_database<LockPolicy>::... bodies. See that header.
   namespace
   {
      using vr = sal::verify_result;

      void verify_node(sal::allocator&                alloc,
                        sal::ptr_address               addr,
                        const std::string&             key_prefix,
                        uint32_t                       root_index,
                        vr&                            result,
                        std::unordered_set<uint64_t>&  visited)
      {
         if (addr == sal::null_ptr_address)
            return;

         if (!visited.insert(*addr).second)
            return;  // already verified (shared node in DAG)

         auto [obj, loc] = alloc.resolve(addr);
         if (!obj)
         {
            ++result.dangling_pointers;
            result.node_failures.push_back(
                {addr, {}, sal::header_type::undefined, root_index,
                 vr::to_hex(key_prefix), "dangling_pointer", false});
            return;
         }

         ++result.nodes_visited;
         result.reachable_bytes += obj->size();

         // Verify object checksum
         if (obj->checksum() == 0)
         {
            ++result.object_checksums.unknown;
         }
         else if (alloc.type_ops(obj).verify_checksum(obj))
         {
            ++result.object_checksums.passed;
         }
         else
         {
            ++result.object_checksums.failed;
            result.node_failures.push_back(
                {addr, loc, obj->type(), root_index,
                 vr::to_hex(key_prefix), "object_checksum", false});
         }

         // Dispatch by node type
         auto type = static_cast<node_type>(obj->type());

         switch (type)
         {
            case node_type::inner:
            {
               auto* n = static_cast<const inner_node*>(static_cast<const node*>(obj));
               auto  d = n->divs();
               for (uint32_t b = 0; b < n->num_branches(); ++b)
               {
                  std::string child_prefix = key_prefix;
                  if (b > 0 && b - 1 < d.size())
                     child_prefix.push_back(d[b - 1]);
                  else if (b == 0)
                     child_prefix.push_back('\0');  // first branch covers bytes starting at 0

                  verify_node(alloc, n->get_branch(branch_number(b)),
                              child_prefix, root_index, result, visited);
               }
               break;
            }
            case node_type::inner_prefix:
            {
               auto* n      = static_cast<const inner_prefix_node*>(static_cast<const node*>(obj));
               auto  prefix = n->prefix();
               auto  d      = n->divs();
               for (uint32_t b = 0; b < n->num_branches(); ++b)
               {
                  std::string child_prefix = key_prefix;
                  child_prefix.append(prefix.data(), prefix.size());
                  if (b > 0 && b - 1 < d.size())
                     child_prefix.push_back(d[b - 1]);
                  else if (b == 0)
                     child_prefix.push_back('\0');

                  verify_node(alloc, n->get_branch(branch_number(b)),
                              child_prefix, root_index, result, visited);
               }
               break;
            }
            case node_type::wide_inner:
            {
               auto* n = static_cast<const wide_inner_node*>(static_cast<const node*>(obj));
               auto  d = n->divs();
               for (uint32_t b = 0; b < n->num_branches(); ++b)
               {
                  std::string child_prefix = key_prefix;
                  if (b > 0 && b - 1 < d.size())
                     child_prefix.push_back(d[b - 1]);
                  else if (b == 0)
                     child_prefix.push_back('\0');

                  verify_node(alloc, n->get_branch(branch_number(b)),
                              child_prefix, root_index, result, visited);
               }
               break;
            }
            case node_type::direct_inner:
            {
               auto* n = static_cast<const direct_inner_node*>(static_cast<const node*>(obj));
               auto  d = n->divs();
               for (uint32_t b = 0; b < n->num_branches(); ++b)
               {
                  std::string child_prefix = key_prefix;
                  if (b > 0 && b - 1 < d.size())
                     child_prefix.push_back(d[b - 1]);
                  else if (b == 0)
                     child_prefix.push_back('\0');

                  verify_node(alloc, n->get_branch(branch_number(b)),
                              child_prefix, root_index, result, visited);
               }
               break;
            }
            case node_type::bplus_inner:
            {
               auto* n = static_cast<const bplus_inner_node*>(static_cast<const node*>(obj));
               for (uint32_t b = 0; b < n->num_branches(); ++b)
                  verify_node(alloc, n->get_branch(branch_number(b)), key_prefix, root_index,
                              result, visited);
               break;
            }
            case node_type::leaf:
            {
               auto* leaf = static_cast<const leaf_node*>(static_cast<const node*>(obj));

               for (uint32_t b = 0; b < leaf->num_branches(); ++b)
               {
                  auto bn  = branch_number(b);
                  auto key = leaf->get_key(bn);

                  // Verify key hash
                  if (leaf->verify_key_hash(bn))
                  {
                     ++result.key_checksums.passed;
                  }
                  else
                  {
                     ++result.key_checksums.failed;
                     result.key_failures.push_back(
                         {vr::to_hex(key), root_index, addr, loc,
                          static_cast<uint16_t>(b), "key_hash"});
                  }

                  // Verify value checksum or recurse into address values
                  auto vt = leaf->get_value_type(bn);
                  if (vt == leaf_node::value_type_flag::inline_data)
                  {
                     if (leaf->verify_value_checksum(bn))
                     {
                        ++result.value_checksums.passed;
                     }
                     else
                     {
                        ++result.value_checksums.failed;
                        result.key_failures.push_back(
                            {vr::to_hex(key), root_index, addr, loc,
                             static_cast<uint16_t>(b), "value_checksum"});
                     }
                  }
                  else if (vt == leaf_node::value_type_flag::value_node ||
                           vt == leaf_node::value_type_flag::subtree)
                  {
                     auto val = leaf->get_value(bn);
                     std::string child_prefix = key_prefix;
                     child_prefix.append(key.data(), key.size());
                     verify_node(alloc, val.address(),
                                 child_prefix, root_index, result, visited);
                  }
                  // null values have no checksum to verify
               }
               break;
            }
            case node_type::value:
            {
               auto* vn = static_cast<const value_node*>(static_cast<const node*>(obj));
               if (vn->is_subtree_container())
               {
                  for (uint8_t vi = 0; vi < vn->num_versions(); ++vi)
                  {
                     int16_t off = vn->get_entry_offset(vi);
                     if (off >= value_node::offset_data_start)
                     {
                        tree_id tid = vn->get_entry_tree_id(vi);
                        verify_node(alloc, tid.root, key_prefix, root_index, result, visited);
                     }
                  }
               }
               // Also visit child value_nodes
               for (uint8_t ni = 0; ni < vn->num_next(); ++ni)
                  verify_node(alloc, vn->next_ptrs()[ni].ptr, key_prefix, root_index, result, visited);
               break;
            }
            case node_type::value_index:
               // value_index nodes will be handled when implemented (Phase 3 overflow)
               break;
         }
      }
   }  // anonymous namespace

   sal::verify_result verify_all_roots(sal::allocator& alloc)
   {
      sal::verify_result result;

      // Pass 1: Segment sync checksums
      alloc.verify_segments(result);

      // Pass 2: Tree walk from all roots
      std::unordered_set<uint64_t> visited;
      visited.reserve(1 << 20);

      auto& roots     = alloc.root_objects();
      auto  num_roots = std::min<uint32_t>(roots.size(), num_top_roots);
      for (uint32_t i = 0; i < num_roots; ++i)
      {
         auto tid = roots[i].load(std::memory_order_relaxed);
         if (tid.root != sal::null_ptr_address)
         {
            ++result.roots_checked;
            verify_node(alloc, tid.root, {}, i, result, visited);
         }
      }

      return result;
   }

   namespace
   {
      void record_histogram(std::vector<uint64_t>& hist, uint64_t value)
      {
         if (hist.size() <= value)
            hist.resize(value + 1);
         ++hist[value];
      }

      void record_inner_stats(tree_stats_result& result,
                              tree_stats_depth_row& row,
                              uint64_t branches,
                              uint64_t bytes)
      {
         result.inner_branches += branches;
         row.inner_branches += branches;

         if (branches == 1)
         {
            ++result.single_branch_inners;
            ++row.single_branch_inners;
         }
         if (branches <= 2)
         {
            ++result.low_fanout_inners;
            ++row.low_fanout_inners;
         }

         result.total_inner_bytes += bytes;
         record_histogram(result.branches_per_inner_node, branches);
         row.fanout.record(branches);
      }

      void report_tree_stats_progress(const tree_stats_result& result,
                                      const tree_stats_options& options)
      {
         if (options.progress == nullptr || options.progress_interval_nodes == 0)
            return;
         if ((result.nodes_visited % options.progress_interval_nodes) == 0)
            options.progress(result, options.progress_user);
      }

      void record_node_visit(tree_stats_result& result,
                             const sal::alloc_header* obj,
                             const tree_stats_options& options)
      {
         ++result.nodes_visited;
         result.reachable_bytes += obj->size();
         report_tree_stats_progress(result, options);
         if (options.max_nodes != 0 && result.nodes_visited >= options.max_nodes)
            result.scan_truncated = true;
      }

      bool mark_tree_stats_node_seen(sal::ptr_address addr,
                                     tree_stats_result& result,
                                     std::unordered_set<uint64_t>& visited)
      {
         if (visited.insert(*addr).second)
            return true;
         ++result.shared_nodes_skipped;
         return false;
      }

      bool key_in_range(std::string_view key, const tree_stats_options& options) noexcept;

      void record_key_stats(tree_stats_result& result,
                            tree_stats_depth_row& row,
                            uint64_t key_size,
                            bool selected) noexcept
      {
         result.key_bytes += key_size;
         result.max_key_size = std::max(result.max_key_size, key_size);
         row.key_bytes += key_size;
         row.max_key_size = std::max(row.max_key_size, key_size);

         if (selected)
         {
            result.selected_key_bytes += key_size;
            result.max_selected_key_size =
                std::max(result.max_selected_key_size, key_size);
            row.selected_key_bytes += key_size;
            row.max_selected_key_size = std::max(row.max_selected_key_size, key_size);
         }
      }

      void record_data_value_stats(tree_stats_result& result,
                                   tree_stats_depth_row& row,
                                   uint64_t value_size) noexcept
      {
         ++result.data_value_count;
         result.data_value_bytes += value_size;
         result.max_data_value_size = std::max(result.max_data_value_size, value_size);

         ++row.data_value_count;
         row.data_value_bytes += value_size;
         row.max_data_value_size = std::max(row.max_data_value_size, value_size);
      }

      void maybe_record_value_node_data_size(sal::allocator& alloc,
                                             sal::ptr_address addr,
                                             tree_stats_result& result,
                                             tree_stats_depth_row& row)
      {
         auto resolved = alloc.resolve(addr);
         auto* obj     = resolved.first;
         if (!obj || static_cast<node_type>(obj->type()) != node_type::value)
            return;

         auto* value = static_cast<const value_node*>(static_cast<const node*>(obj));
         uint64_t value_size = 0;
         if (value->latest_data_size(value_size))
            record_data_value_stats(result, row, value_size);
      }

      uint64_t count_selected_leaf_keys(const leaf_node* leaf,
                                        std::string_view key_prefix,
                                        const tree_stats_options& options)
      {
         if (!options.has_key_range())
            return leaf->num_branches();

         uint64_t selected = 0;
         for (uint32_t b = 0; b < leaf->num_branches(); ++b)
         {
            auto key = leaf->get_key(branch_number(b));
            std::string full_key(key_prefix);
            full_key.append(key.data(), key.size());
            if (key_in_range(full_key, options))
               ++selected;
         }
         return selected;
      }

      void record_leaf_stats(sal::allocator& alloc,
                             tree_stats_result& result,
                             tree_stats_depth_row& row,
                             const leaf_node* leaf,
                             uint32_t depth,
                             std::string_view key_prefix,
                             const tree_stats_options& options)
      {
         const uint64_t selected_keys = count_selected_leaf_keys(leaf, key_prefix, options);
         ++result.leaf_nodes;
         result.leaf_keys += leaf->num_branches();
         result.selected_leaf_keys += selected_keys;
         result.max_depth = std::max<uint64_t>(result.max_depth, depth);
         result.leaf_depth_sum += depth;
         result.key_depth_sum += uint64_t(depth) * leaf->num_branches();
         result.total_leaf_alloc_bytes += leaf->size();
         result.total_leaf_used_bytes += leaf->alloc_pos();
         const uint64_t dead_space = leaf->dead_space();
         const uint64_t empty_space = leaf->size() > leaf->alloc_pos()
                                          ? uint64_t(leaf->size() - leaf->alloc_pos())
                                          : 0;
         result.total_leaf_dead_bytes += dead_space;
         result.total_leaf_empty_bytes += empty_space;

         const uint64_t leaf_clines = leaf->clines_capacity();
         result.leaf_clines += leaf_clines;
         result.max_leaf_clines = std::max(result.max_leaf_clines, leaf_clines);
         if (leaf_clines >= leaf_node::max_value_clines)
            ++result.cline_saturated_leaves;

         ++row.leaf_nodes;
         row.leaf_keys += leaf->num_branches();
         row.selected_leaf_keys += selected_keys;
         row.leaf_clines += leaf_clines;
         row.max_leaf_clines = std::max(row.max_leaf_clines, leaf_clines);
         if (leaf_clines >= leaf_node::max_value_clines)
            ++row.cline_saturated_leaves;
         row.leaf_alloc_bytes += leaf->size();
         row.leaf_used_bytes += leaf->alloc_pos();
         row.leaf_dead_bytes += dead_space;
         row.leaf_empty_bytes += empty_space;

         if (leaf->size() == leaf_node::max_leaf_size)
         {
            ++result.full_leaf_nodes;
            result.full_leaf_dead_bytes += dead_space;
            result.full_leaf_empty_bytes += empty_space;
            ++row.full_leaf_nodes;
            row.full_leaf_dead_bytes += dead_space;
            row.full_leaf_empty_bytes += empty_space;
         }
         record_histogram(result.keys_per_leaf, leaf->num_branches());
         record_histogram(result.leaf_clines_histogram, leaf_clines);
         record_histogram(result.leaf_depths, depth);

         uint64_t leaf_address_values = 0;
         for (uint32_t b = 0; b < leaf->num_branches(); ++b)
         {
            auto bn = branch_number(b);
            auto key = leaf->get_key(bn);
            auto vt = leaf->get_value_type(bn);
            if (vt == leaf_node::value_type_flag::value_node)
               ++leaf_address_values;

            const uint64_t full_key_size = key_prefix.size() + key.size();
            bool selected = true;
            if (options.has_key_range())
            {
               std::string full_key(key_prefix);
               full_key.append(key.data(), key.size());
               selected = key_in_range(full_key, options);
            }

            record_key_stats(result, row, full_key_size, selected);
            if (!selected)
               continue;

            if (vt == leaf_node::value_type_flag::inline_data)
               record_data_value_stats(result, row, leaf->get_value_view(bn).size());
            else if (vt == leaf_node::value_type_flag::value_node)
               maybe_record_value_node_data_size(alloc, leaf->get_value_address(bn),
                                                 result, row);
         }

         result.leaf_address_values += leaf_address_values;
         row.leaf_address_values += leaf_address_values;
         record_histogram(result.address_values_per_leaf, leaf_address_values);
      }

      void collect_tree_stats_node(sal::allocator&               alloc,
                                   sal::ptr_address              addr,
                                   uint32_t                      depth,
                                   std::string_view              key_prefix,
                                   tree_stats_result&            result,
                                   std::unordered_set<uint64_t>& visited,
                                   const tree_stats_options&     options)
      {
         if (result.scan_truncated)
            return;
         if (addr == sal::null_ptr_address)
            return;

         auto resolved = alloc.resolve(addr);
         auto* obj     = resolved.first;
         if (!obj)
         {
            ++result.dangling_pointers;
            return;
         }

         if (!mark_tree_stats_node_seen(addr, result, visited))
            return;
         record_node_visit(result, obj, options);

         switch (static_cast<node_type>(obj->type()))
         {
            case node_type::inner:
            {
               auto* n = static_cast<const inner_node*>(static_cast<const node*>(obj));
               auto& row = result.row_for_depth(depth);
               ++result.inner_nodes;
               ++row.inner_nodes;
               record_inner_stats(result, row, n->num_branches(), n->size());

               for (uint32_t b = 0; b < n->num_branches(); ++b)
                  collect_tree_stats_node(alloc, n->get_branch(branch_number(b)), depth + 1,
                                          key_prefix, result, visited, options);
               break;
            }
            case node_type::inner_prefix:
            {
               auto* n = static_cast<const inner_prefix_node*>(static_cast<const node*>(obj));
               std::string prefix(key_prefix);
               auto node_prefix = n->prefix();
               prefix.append(node_prefix.data(), node_prefix.size());
               auto& row = result.row_for_depth(depth);
               ++result.inner_prefix_nodes;
               ++row.inner_prefix_nodes;
               record_inner_stats(result, row, n->num_branches(), n->size());

               for (uint32_t b = 0; b < n->num_branches(); ++b)
                  collect_tree_stats_node(alloc, n->get_branch(branch_number(b)), depth + 1,
                                          prefix, result, visited, options);
               break;
            }
            case node_type::wide_inner:
            {
               auto* n = static_cast<const wide_inner_node*>(static_cast<const node*>(obj));
               auto& row = result.row_for_depth(depth);
               ++result.inner_nodes;
               ++row.inner_nodes;
               record_inner_stats(result, row, n->num_branches(), n->size());

               for (uint32_t b = 0; b < n->num_branches(); ++b)
                  collect_tree_stats_node(alloc, n->get_branch(branch_number(b)), depth + 1,
                                          key_prefix, result, visited, options);
               break;
            }
            case node_type::direct_inner:
            {
               auto* n = static_cast<const direct_inner_node*>(static_cast<const node*>(obj));
               auto& row = result.row_for_depth(depth);
               ++result.inner_nodes;
               ++row.inner_nodes;
               record_inner_stats(result, row, n->num_branches(), n->size());

               for (uint32_t b = 0; b < n->num_branches(); ++b)
                  collect_tree_stats_node(alloc, n->get_branch(branch_number(b)), depth + 1,
                                          key_prefix, result, visited, options);
               break;
            }
            case node_type::bplus_inner:
            {
               auto* n = static_cast<const bplus_inner_node*>(static_cast<const node*>(obj));
               auto& row = result.row_for_depth(depth);
               ++result.inner_nodes;
               ++row.inner_nodes;
               record_inner_stats(result, row, n->num_branches(), n->size());

               for (uint32_t b = 0; b < n->num_branches(); ++b)
                  collect_tree_stats_node(alloc, n->get_branch(branch_number(b)), depth + 1,
                                          key_prefix, result, visited, options);
               break;
            }
            case node_type::leaf:
            {
               auto* leaf = static_cast<const leaf_node*>(static_cast<const node*>(obj));
               auto& row = result.row_for_depth(depth);
               record_leaf_stats(alloc, result, row, leaf, depth, key_prefix, options);

               for (uint32_t b = 0; b < leaf->num_branches(); ++b)
               {
                  auto bn = branch_number(b);
                  auto vt = leaf->get_value_type(bn);
                  if (vt == leaf_node::value_type_flag::value_node)
                     collect_tree_stats_node(alloc, leaf->get_value_address(bn), depth,
                                             key_prefix, result, visited, options);
                  else if (vt == leaf_node::value_type_flag::subtree)
                     collect_tree_stats_node(alloc, leaf->get_value(bn).address(), 1,
                                             {}, result, visited, options);
               }
               break;
            }
            case node_type::value:
            {
               auto* value = static_cast<const value_node*>(static_cast<const node*>(obj));
               auto& row = result.row_for_depth(depth);
               ++result.value_nodes;
               ++row.value_nodes;
               result.total_value_bytes += value->size();
               if (value->is_flat())
               {
                  ++result.flat_value_nodes;
                  ++row.flat_value_nodes;
               }

               if (value->is_subtree_container())
               {
                  for (uint8_t i = 0; i < value->num_versions(); ++i)
                  {
                     if (value->get_entry_offset(i) < value_node::offset_data_start)
                        continue;
                     auto tid = value->get_entry_tree_id(i);
                     collect_tree_stats_node(alloc, tid.root, 1, {}, result, visited, options);
                  }
               }

               for (uint8_t i = 0; i < value->num_next(); ++i)
                  collect_tree_stats_node(alloc, value->next_ptrs()[i].ptr, depth,
                                          key_prefix, result, visited, options);
               break;
            }
            case node_type::value_index:
               break;
         }
      }

      bool key_less_equal(std::string_view lhs, std::string_view rhs) noexcept
      {
         return lhs.compare(rhs) <= 0;
      }

      bool key_in_range(std::string_view key, const tree_stats_options& options) noexcept
      {
         if (options.key_lower && key.compare(*options.key_lower) < 0)
            return false;
         if (options.key_upper && key.compare(*options.key_upper) >= 0)
            return false;
         return true;
      }

      std::optional<std::string> next_prefix_bound(std::string_view prefix)
      {
         std::string bound(prefix);
         for (size_t i = bound.size(); i > 0; --i)
         {
            auto byte = static_cast<unsigned char>(bound[i - 1]);
            if (byte != 0xff)
            {
               bound[i - 1] = static_cast<char>(byte + 1);
               bound.resize(i);
               return bound;
            }
         }
         return std::nullopt;
      }

      bool interval_intersects_range(std::string_view lower,
                                     const std::optional<std::string>& upper,
                                     const tree_stats_options& options) noexcept
      {
         if (options.key_upper && key_less_equal(*options.key_upper, lower))
            return false;
         if (options.key_lower && upper && key_less_equal(*upper, *options.key_lower))
            return false;
         return true;
      }

      std::string max_lower_bound(std::string_view lhs, std::string_view rhs)
      {
         return lhs.compare(rhs) >= 0 ? std::string(lhs) : std::string(rhs);
      }

      std::optional<std::string> min_upper_bound(const std::optional<std::string>& lhs,
                                                 const std::optional<std::string>& rhs)
      {
         if (!lhs)
            return rhs;
         if (!rhs)
            return lhs;
         return lhs->compare(*rhs) <= 0 ? lhs : rhs;
      }

      std::string append_byte(std::string_view prefix, uint8_t byte)
      {
         std::string out(prefix);
         out.push_back(static_cast<char>(byte));
         return out;
      }

      template <typename InnerNode>
      std::string branch_lower_bound(const InnerNode* node,
                                     std::string_view prefix,
                                     uint32_t branch)
      {
         if (branch == 0)
            return std::string(prefix);
         return append_byte(prefix, static_cast<uint8_t>(node->divs()[branch - 1]));
      }

      template <typename InnerNode>
      std::optional<std::string> branch_upper_bound(const InnerNode* node,
                                                    std::string_view prefix,
                                                    uint32_t branch)
      {
         if (branch + 1 < node->num_branches())
            return append_byte(prefix, static_cast<uint8_t>(node->divs()[branch]));
         return next_prefix_bound(prefix);
      }

      void collect_tree_stats_node_range(sal::allocator&               alloc,
                                         sal::ptr_address              addr,
                                         uint32_t                      depth,
                                         std::string_view              key_prefix,
                                         std::string_view              node_lower,
                                         const std::optional<std::string>& node_upper,
                                         tree_stats_result&            result,
                                         std::unordered_set<uint64_t>& visited,
                                         const tree_stats_options&     options)
      {
         if (result.scan_truncated)
            return;
         if (addr == sal::null_ptr_address)
            return;

         auto resolved = alloc.resolve(addr);
         auto* obj     = resolved.first;
         if (!obj)
         {
            ++result.dangling_pointers;
            return;
         }

         switch (static_cast<node_type>(obj->type()))
         {
            case node_type::inner:
            {
               auto* n = static_cast<const inner_node*>(static_cast<const node*>(obj));
               if (!mark_tree_stats_node_seen(addr, result, visited))
                  return;
               record_node_visit(result, obj, options);
               auto& row = result.row_for_depth(depth);
               ++result.inner_nodes;
               ++row.inner_nodes;
               record_inner_stats(result, row, n->num_branches(), n->size());

               for (uint32_t b = 0; b < n->num_branches(); ++b)
               {
                  auto lower = branch_lower_bound(n, key_prefix, b);
                  auto upper = branch_upper_bound(n, key_prefix, b);
                  lower = max_lower_bound(lower, node_lower);
                  upper = min_upper_bound(upper, node_upper);
                  if (!interval_intersects_range(lower, upper, options))
                     continue;
                  collect_tree_stats_node_range(alloc, n->get_branch(branch_number(b)),
                                                depth + 1, key_prefix, lower, upper,
                                                result, visited, options);
               }
               break;
            }
            case node_type::inner_prefix:
            {
               auto* n = static_cast<const inner_prefix_node*>(static_cast<const node*>(obj));
               std::string prefix(key_prefix);
               auto node_prefix = n->prefix();
               prefix.append(node_prefix.data(), node_prefix.size());
               auto prefix_upper = next_prefix_bound(prefix);
               auto prefix_lower = max_lower_bound(prefix, node_lower);
               prefix_upper = min_upper_bound(prefix_upper, node_upper);
               if (!interval_intersects_range(prefix_lower, prefix_upper, options))
                  return;

               if (!mark_tree_stats_node_seen(addr, result, visited))
                  return;
               record_node_visit(result, obj, options);
               auto& row = result.row_for_depth(depth);
               ++result.inner_prefix_nodes;
               ++row.inner_prefix_nodes;
               record_inner_stats(result, row, n->num_branches(), n->size());

               for (uint32_t b = 0; b < n->num_branches(); ++b)
               {
                  auto lower = branch_lower_bound(n, prefix, b);
                  auto upper = branch_upper_bound(n, prefix, b);
                  lower = max_lower_bound(lower, prefix_lower);
                  upper = min_upper_bound(upper, prefix_upper);
                  if (!interval_intersects_range(lower, upper, options))
                     continue;
                  collect_tree_stats_node_range(alloc, n->get_branch(branch_number(b)),
                                                depth + 1, prefix, lower, upper,
                                                result, visited, options);
               }
               break;
            }
            case node_type::wide_inner:
            {
               auto* n = static_cast<const wide_inner_node*>(static_cast<const node*>(obj));
               if (!mark_tree_stats_node_seen(addr, result, visited))
                  return;
               record_node_visit(result, obj, options);
               auto& row = result.row_for_depth(depth);
               ++result.inner_nodes;
               ++row.inner_nodes;
               record_inner_stats(result, row, n->num_branches(), n->size());

               for (uint32_t b = 0; b < n->num_branches(); ++b)
               {
                  auto lower = branch_lower_bound(n, key_prefix, b);
                  auto upper = branch_upper_bound(n, key_prefix, b);
                  lower = max_lower_bound(lower, node_lower);
                  upper = min_upper_bound(upper, node_upper);
                  if (!interval_intersects_range(lower, upper, options))
                     continue;
                  collect_tree_stats_node_range(alloc, n->get_branch(branch_number(b)),
                                                depth + 1, key_prefix, lower, upper,
                                                result, visited, options);
               }
               break;
            }
            case node_type::direct_inner:
            {
               auto* n = static_cast<const direct_inner_node*>(static_cast<const node*>(obj));
               if (!mark_tree_stats_node_seen(addr, result, visited))
                  return;
               record_node_visit(result, obj, options);
               auto& row = result.row_for_depth(depth);
               ++result.inner_nodes;
               ++row.inner_nodes;
               record_inner_stats(result, row, n->num_branches(), n->size());

               for (uint32_t b = 0; b < n->num_branches(); ++b)
               {
                  auto lower = branch_lower_bound(n, key_prefix, b);
                  auto upper = branch_upper_bound(n, key_prefix, b);
                  lower = max_lower_bound(lower, node_lower);
                  upper = min_upper_bound(upper, node_upper);
                  if (!interval_intersects_range(lower, upper, options))
                     continue;
                  collect_tree_stats_node_range(alloc, n->get_branch(branch_number(b)),
                                                depth + 1, key_prefix, lower, upper,
                                                result, visited, options);
               }
               break;
            }
            case node_type::bplus_inner:
            {
               auto* n = static_cast<const bplus_inner_node*>(static_cast<const node*>(obj));
               if (!mark_tree_stats_node_seen(addr, result, visited))
                  return;
               record_node_visit(result, obj, options);
               auto& row = result.row_for_depth(depth);
               ++result.inner_nodes;
               ++row.inner_nodes;
               record_inner_stats(result, row, n->num_branches(), n->size());

               for (uint32_t b = 0; b < n->num_branches(); ++b)
                  collect_tree_stats_node_range(alloc, n->get_branch(branch_number(b)),
                                                depth + 1, key_prefix, node_lower, node_upper,
                                                result, visited, options);
               break;
            }
            case node_type::leaf:
            {
               auto* leaf = static_cast<const leaf_node*>(static_cast<const node*>(obj));
               uint64_t selected = 0;
               for (uint32_t b = 0; b < leaf->num_branches(); ++b)
               {
                  auto key = leaf->get_key(branch_number(b));
                  std::string full_key(key_prefix);
                  full_key.append(key.data(), key.size());
                  if (key_in_range(full_key, options))
                     ++selected;
               }

               if (selected == 0)
                  return;

               if (!mark_tree_stats_node_seen(addr, result, visited))
                  return;
               record_node_visit(result, obj, options);
               auto& row = result.row_for_depth(depth);
               record_leaf_stats(alloc, result, row, leaf, depth, key_prefix, options);

               for (uint32_t b = 0; b < leaf->num_branches(); ++b)
               {
                  auto bn = branch_number(b);
                  auto key = leaf->get_key(bn);
                  std::string full_key(key_prefix);
                  full_key.append(key.data(), key.size());
                  if (!key_in_range(full_key, options))
                     continue;

                  auto vt = leaf->get_value_type(bn);
                  if (vt == leaf_node::value_type_flag::value_node)
                     collect_tree_stats_node(alloc, leaf->get_value_address(bn), depth,
                                             key_prefix, result, visited, options);
                  else if (vt == leaf_node::value_type_flag::subtree)
                     collect_tree_stats_node(alloc, leaf->get_value(bn).address(), 1,
                                             {}, result, visited, options);
               }
               break;
            }
            case node_type::value:
            {
               auto* value = static_cast<const value_node*>(static_cast<const node*>(obj));
               if (!mark_tree_stats_node_seen(addr, result, visited))
                  return;
               record_node_visit(result, obj, options);
               auto& row = result.row_for_depth(depth);
               ++result.value_nodes;
               ++row.value_nodes;
               result.total_value_bytes += value->size();
               if (value->is_flat())
               {
                  ++result.flat_value_nodes;
                  ++row.flat_value_nodes;
               }

               if (value->is_subtree_container())
               {
                  for (uint8_t i = 0; i < value->num_versions(); ++i)
                  {
                     if (value->get_entry_offset(i) < value_node::offset_data_start)
                        continue;
                     auto tid = value->get_entry_tree_id(i);
                     collect_tree_stats_node(alloc, tid.root, 1, {}, result, visited, options);
                  }
               }

               for (uint8_t i = 0; i < value->num_next(); ++i)
                  collect_tree_stats_node(alloc, value->next_ptrs()[i].ptr, depth,
                                          key_prefix, result, visited, options);
               break;
            }
            case node_type::value_index:
               break;
         }
      }
   }  // anonymous namespace

   tree_stats_result collect_tree_stats(sal::allocator& alloc,
                                        const tree_stats_options& options)
   {
      tree_stats_result result;
      result.key_range_enabled   = options.has_key_range();
      result.root_filter_enabled = options.root_index.has_value();
      result.root_filter_index   = options.root_index.value_or(0);
      result.max_nodes           = options.max_nodes;
      if (options.key_lower)
         result.key_range_lower = *options.key_lower;
      if (options.key_upper)
         result.key_range_upper = *options.key_upper;
      auto              session = alloc.get_session();
      auto              lock    = session->lock();

      std::unordered_set<uint64_t> visited;
      visited.reserve(1 << 20);

      auto& roots     = alloc.root_objects();
      auto  num_roots = std::min<uint32_t>(roots.size(), num_top_roots);
      uint32_t first_root = 0;
      uint32_t last_root  = num_roots;
      if (options.root_index)
      {
         if (*options.root_index >= num_roots)
            return result;
         first_root = *options.root_index;
         last_root  = first_root + 1;
      }

      for (uint32_t i = first_root; i < last_root; ++i)
      {
         auto tid = roots[i].load(std::memory_order_relaxed);
         if (tid.root == sal::null_ptr_address)
            continue;

         ++result.roots_checked;
         if (tid.ver == sal::null_ptr_address)
            ++result.roots_without_version;
         else
            ++result.roots_with_version;

         if (options.has_key_range())
            collect_tree_stats_node_range(alloc, tid.root, 1, {}, {}, std::nullopt,
                                          result, visited, options);
         else
            collect_tree_stats_node(alloc, tid.root, 1, {}, result, visited, options);
         if (result.scan_truncated)
            break;
      }

      return result;
   }

   namespace
   {
      uint64_t tree_version(const sal::allocator_session_ptr& session, sal::tree_id tid) noexcept
      {
         if (tid.ver == sal::null_ptr_address)
            return 0;
         auto version = session->try_read_custom_cb(tid.ver);
         return version ? *version : 0;
      }

      void record_version(std::unordered_set<uint64_t>& versions, uint64_t version)
      {
         versions.insert(version_token(version, value_version_bits));
      }

      void update_root_version_bounds(version_audit_result& result, uint64_t version)
      {
         if (result.roots_with_version == 0)
         {
            result.oldest_root_version = version;
            result.newest_root_version = version;
            return;
         }
         result.oldest_root_version = std::min(result.oldest_root_version, version);
         result.newest_root_version = std::max(result.newest_root_version, version);
      }

      void audit_value_pruning(const value_node& value,
                               uint64_t          prune_floor,
                               version_audit_result& result)
      {
         if (prune_floor == 0 || value.is_flat() || value.num_versions() == 0)
            return;

         const uint64_t base  = value.get_entry_version(0);
         const uint64_t floor = version_token(prune_floor, value_version_bits);
         const uint64_t floor_distance =
             version_distance(base, floor, value_version_bits);
         const uint64_t half_range = version_mask(value_version_bits) >> 1;
         if (floor_distance > half_range)
         {
            ++result.prune_floor_out_of_range_nodes;
            return;
         }

         uint8_t floor_idx = 0xFF;
         for (uint8_t i = 0; i < value.num_versions(); ++i)
         {
            uint64_t entry_distance =
                version_distance(base, value.get_entry_version(i), value_version_bits);
            if (entry_distance <= floor_distance)
               floor_idx = i;
            else
               break;
         }

         if (floor_idx == 0xFF)
         {
            ++result.prune_floor_unknown_nodes;
            return;
         }

         const uint64_t prunable = floor_idx;
         const bool     rewrite_floor_entry =
             value.get_entry_version(floor_idx) != floor;
         if (prunable || rewrite_floor_entry)
         {
            ++result.prunable_value_nodes;
            result.prunable_value_entries += prunable;
            if (rewrite_floor_entry)
               ++result.floor_rewrite_entries;
         }
      }

      void audit_progress(const version_audit_options& options,
                          const version_audit_result&  result) noexcept
      {
         if (!options.progress || options.progress_interval_nodes == 0)
            return;
         if (result.nodes_visited % options.progress_interval_nodes == 0)
            options.progress(result, options.progress_user);
      }

      void audit_node(sal::allocator&                alloc,
                      const sal::allocator_session_ptr& session,
                      sal::ptr_address               addr,
                      uint64_t                       prune_floor,
                      const version_audit_options&    options,
                      version_audit_result&          result,
                      std::unordered_set<uint64_t>&  visited,
                      std::unordered_set<uint64_t>&  versions)
      {
         if (addr == sal::null_ptr_address)
            return;

         if (!visited.insert(*addr).second)
         {
            ++result.shared_nodes_skipped;
            return;
         }

         auto resolved = alloc.resolve(addr);
         auto* obj = resolved.first;
         if (!obj)
         {
            ++result.dangling_pointers;
            return;
         }

         ++result.nodes_visited;
         audit_progress(options, result);
         auto type = static_cast<node_type>(obj->type());
         switch (type)
         {
            case node_type::inner:
            {
               ++result.inner_nodes;
               auto* n = static_cast<const inner_node*>(static_cast<const node*>(obj));
               for (uint32_t b = 0; b < n->num_branches(); ++b)
                  audit_node(alloc, session, n->get_branch(branch_number(b)), prune_floor,
                             options, result, visited, versions);
               break;
            }
            case node_type::inner_prefix:
            {
               ++result.inner_prefix_nodes;
               auto* n = static_cast<const inner_prefix_node*>(static_cast<const node*>(obj));
               for (uint32_t b = 0; b < n->num_branches(); ++b)
                  audit_node(alloc, session, n->get_branch(branch_number(b)), prune_floor,
                             options, result, visited, versions);
               break;
            }
            case node_type::wide_inner:
            {
               ++result.inner_nodes;
               auto* n = static_cast<const wide_inner_node*>(static_cast<const node*>(obj));
               for (uint32_t b = 0; b < n->num_branches(); ++b)
                  audit_node(alloc, session, n->get_branch(branch_number(b)), prune_floor,
                             options, result, visited, versions);
               break;
            }
            case node_type::direct_inner:
            {
               ++result.inner_nodes;
               auto* n = static_cast<const direct_inner_node*>(static_cast<const node*>(obj));
               for (uint32_t b = 0; b < n->num_branches(); ++b)
                  audit_node(alloc, session, n->get_branch(branch_number(b)), prune_floor,
                             options, result, visited, versions);
               break;
            }
            case node_type::bplus_inner:
            {
               ++result.inner_nodes;
               auto* n = static_cast<const bplus_inner_node*>(static_cast<const node*>(obj));
               for (uint32_t b = 0; b < n->num_branches(); ++b)
                  audit_node(alloc, session, n->get_branch(branch_number(b)), prune_floor,
                             options, result, visited, versions);
               break;
            }
            case node_type::leaf:
            {
               ++result.leaf_nodes;
               auto* leaf = static_cast<const leaf_node*>(static_cast<const node*>(obj));
               result.leaf_branches += leaf->num_branches();
               result.leaf_version_table_entries += leaf->num_versions();

               for (uint32_t b = 0; b < leaf->num_branches(); ++b)
               {
                  auto bn  = branch_number(b);
                  auto ver = leaf->get_version(bn);
                  if (ver != 0)
                  {
                     ++result.leaf_branch_versions;
                     ++result.leaf_versions_seen;
                     record_version(versions, ver);
                  }

                  auto vt = leaf->get_value_type(bn);
                  if (vt == leaf_node::value_type_flag::value_node ||
                      vt == leaf_node::value_type_flag::subtree)
                  {
                     auto val = leaf->get_value(bn);
                     if (vt == leaf_node::value_type_flag::subtree)
                     {
                        ++result.leaf_subtrees;
                        ++result.leaf_subtrees_without_ver;
                     }
                     audit_node(alloc, session, val.address(), prune_floor,
                                options, result, visited, versions);
                  }
               }
               break;
            }
            case node_type::value:
            {
               ++result.value_nodes;
               auto* value = static_cast<const value_node*>(static_cast<const node*>(obj));
               if (value->is_flat())
                  ++result.flat_value_nodes;
               if (value->num_versions() > 1)
                  ++result.value_nodes_with_history;
               if (value->num_next() > 0)
                  ++result.value_nodes_with_next;

               result.value_entries += value->num_versions();
               result.value_next_ptrs += value->num_next();
               result.max_value_entries =
                   std::max<uint64_t>(result.max_value_entries, value->num_versions());

               for (uint8_t i = 0; i < value->num_versions(); ++i)
               {
                  ++result.value_versions_seen;
                  record_version(versions, value->get_entry_version(i));
               }

               audit_value_pruning(*value, prune_floor, result);

               if (value->is_subtree_container())
               {
                  ++result.subtree_value_nodes;
                  for (uint8_t i = 0; i < value->num_versions(); ++i)
                  {
                     if (value->get_entry_offset(i) < value_node::offset_data_start)
                        continue;
                     ++result.subtree_value_entries;
                     auto tid = value->get_entry_tree_id(i);
                     if (auto ver = tree_version(session, tid); ver != 0)
                        record_version(versions, ver);
                     audit_node(alloc, session, tid.root, prune_floor,
                                options, result, visited, versions);
                  }
               }

               for (uint8_t i = 0; i < value->num_next(); ++i)
                  audit_node(alloc, session, value->next_ptrs()[i].ptr, prune_floor,
                             options, result, visited, versions);
               break;
            }
            case node_type::value_index:
               break;
         }
      }
   }  // anonymous namespace

   version_audit_result audit_all_roots(sal::allocator& alloc,
                                        const version_audit_options& options)
   {
      version_audit_result result;
      result.requested_prune_floor = options.prune_floor;

      auto session = alloc.get_session();
      auto& roots  = alloc.root_objects();
      auto  count  = std::min<uint32_t>(roots.size(), num_top_roots);

      for (uint32_t i = 0; i < count; ++i)
      {
         auto tid = roots[i].load(std::memory_order_relaxed);
         if (tid.root == sal::null_ptr_address)
            continue;

         ++result.roots_checked;
         uint64_t version = roots[i].version_for(tid);
         if (version == 0)
            version = tree_version(session, tid);

         if (version == 0)
         {
            ++result.roots_without_version;
         }
         else
         {
            update_root_version_bounds(result, version);
            ++result.roots_with_version;
         }
      }

      result.effective_prune_floor = options.prune_floor;
      if (result.effective_prune_floor == 0 && result.roots_with_version > 0)
         result.effective_prune_floor = result.oldest_root_version;

      std::unordered_set<uint64_t> visited;
      std::unordered_set<uint64_t> versions;
      visited.reserve(1 << 20);
      versions.reserve(1 << 16);

      for (uint32_t i = 0; i < count; ++i)
      {
         auto tid = roots[i].load(std::memory_order_relaxed);
         if (tid.root == sal::null_ptr_address)
            continue;

         uint64_t version = roots[i].version_for(tid);
         if (version == 0)
            version = tree_version(session, tid);
         if (version != 0)
            record_version(versions, version);

         audit_node(alloc, session, tid.root, result.effective_prune_floor,
                    options, result, visited, versions);
      }

      result.retained_versions = versions.size();
      return result;
   }

}  // namespace psitri::detail
