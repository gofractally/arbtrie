#include <psitri/database.hpp>
#include <psitri/node/inner.hpp>
#include <psitri/node/leaf.hpp>
#include <psitri/node/value_node.hpp>
#include <sal/allocator_session_impl.hpp>
#include <algorithm>
#include <unordered_set>

namespace psitri::detail
{
   void register_node_types(sal::allocator& alloc)
   {
      alloc.register_type_ops<leaf_node>();
      alloc.register_type_ops<inner_node>();
      alloc.register_type_ops<inner_prefix_node>();
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
