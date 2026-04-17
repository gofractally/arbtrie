#include <psitri/database.hpp>
#include <psitri/node/inner.hpp>
#include <psitri/node/leaf.hpp>
#include <psitri/node/value_node.hpp>
#include <unordered_set>

namespace psitri::detail
{
   void register_node_types()
   {
      static bool registered = false;
      if (registered)
         return;
      registered = true;
      sal::register_type_vtable<leaf_node>();
      sal::register_type_vtable<inner_node>();
      sal::register_type_vtable<inner_prefix_node>();
      sal::register_type_vtable<value_node>();
   }

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
         else if (sal::vcall::verify_checksum(obj))
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
               if (vn->is_subtree)
               {
                  auto child_addr = *reinterpret_cast<const sal::ptr_address*>(vn->data);
                  verify_node(alloc, child_addr, key_prefix, root_index, result, visited);
               }
               // value_node data integrity is covered by the object checksum
               break;
            }
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
         auto addr = roots[i].load(std::memory_order_relaxed);
         if (addr != sal::null_ptr_address)
         {
            ++result.roots_checked;
            verify_node(alloc, addr, {}, i, result, visited);
         }
      }

      return result;
   }

}  // namespace psitri::detail
