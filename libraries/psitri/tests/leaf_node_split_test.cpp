#include <catch2/catch_all.hpp>
#include <cstdlib>  // For std::aligned_alloc, std::free
#include <cstring>  // For memcpy
#include <map>
#include <memory>
#include <psitri/node/leaf.hpp>
#include <psitri/value_type.hpp>
#include <string>
#include <vector>

namespace
{
   // Deleter for memory allocated with std::aligned_alloc
   struct AlignedDeleter
   {
      void operator()(void* ptr) const { std::free(ptr); }
   };

   // Custom deleter for leaf_node created with placement new
   struct LeafNodeDeleter
   {
      void operator()(psitri::leaf_node* node) const
      {
         if (node)
         {
            node->~leaf_node();  // Explicitly call destructor
            std::free(node);     // Free the buffer allocated with aligned_alloc
         }
      }
   };

   using LeafNodePtr = std::unique_ptr<psitri::leaf_node, LeafNodeDeleter>;

   // Helper to create a leaf node unique_ptr in an aligned buffer
   LeafNodePtr create_leaf_node_ptr(
       size_t                    node_size,
       psitri::ptr_address_seq   seq,
       const psitri::leaf_node*  clone,
       psitri::key_view          cprefix                 = "",
       psitri::branch_number     start                   = psitri::branch_number(0),
       psitri::branch_number     end                     = psitri::branch_number(0),
       bool                      use_initial_constructor = false,
       psitri::key_view          initial_key             = "",
       const psitri::value_type& initial_value           = psitri::value_type(""))
   {
      constexpr size_t alignment = 64;
      void*            buffer    = std::aligned_alloc(alignment, node_size);
      if (!buffer)
      {
         throw std::bad_alloc();
      }
      std::memset(buffer, 0, node_size);

      psitri::leaf_node* raw_node = nullptr;
      try
      {
         if (use_initial_constructor)
         {
            raw_node = new (buffer) psitri::leaf_node(node_size, seq, initial_key, initial_value);
         }
         else if (*end > *start)
         {  // Use split constructor
            raw_node = new (buffer) psitri::leaf_node(node_size, seq, clone, cprefix, start, end);
         }
         else
         {  // Use regular clone constructor
            raw_node = new (buffer) psitri::leaf_node(node_size, seq, clone);
         }
      }
      catch (...)
      {
         std::free(buffer);
         throw;
      }
      return LeafNodePtr(raw_node);
   }

   // Overload for the initial key/value constructor
   LeafNodePtr create_leaf_node_ptr(psitri::key_view          initial_key,
                                    const psitri::value_type& initial_value,
                                    size_t                    node_size = 4096)
   {
      constexpr size_t alignment = 64;
      void*            buffer    = std::aligned_alloc(alignment, node_size);
      if (!buffer)
         throw std::bad_alloc();
      std::memset(buffer, 0, node_size);
      uintptr_t               buffer_addr_int = reinterpret_cast<uintptr_t>(buffer);
      psitri::ptr_address     buffer_addr(static_cast<unsigned int>(buffer_addr_int));
      psitri::ptr_address_seq seq = {buffer_addr, 0};
      return create_leaf_node_ptr(node_size, seq, nullptr, "", psitri::branch_number(0),
                                  psitri::branch_number(0), true, initial_key, initial_value);
   }

}  // namespace

TEST_CASE("LeafNode_Split", "[psitri][leaf_node][split]")
{
   using namespace psitri;

   constexpr size_t node_size = 4096;

   // 1. Create and populate source node
   LeafNodePtr source_node_ptr = create_leaf_node_ptr("prefix/common/key_a", value_type("value_a"));
   leaf_node&  source_node     = *source_node_ptr;

   std::map<std::string, value_type> test_data = {
       {"prefix/common/key_b", value_type("value_b")},
       {"prefix/common/key_c", value_type("value_c")},
       {"prefix/uncommon/key_d", value_type::make_subtree(ptr_address(1000))},
       {"prefix/uncommon/key_e", value_type("value_e")},
       {"prefix/zebra/key_f", value_type::make_value_node(ptr_address(2000))},
       {"prefix/zebra/key_g", value_type("value_g")}};

   // Insert data ensuring keys are sorted for predictable branch numbers
   for (const auto& pair : test_data)
   {
      op::leaf_insert ins{.src   = source_node,
                          .lb    = source_node.lower_bound(pair.first),
                          .key   = pair.first,
                          .value = pair.second};
      REQUIRE(source_node.can_apply(ins) != leaf_node::can_apply_mode::none);
      branch_number bn = source_node.lower_bound(pair.first);
      ins.lb           = bn;  // Ensure lower_bound is correct
      source_node.apply(ins);
   }
   REQUIRE(source_node.num_branches() == test_data.size() + 1);  // +1 for initial key

   // 2. Get split position
   leaf_node::split_pos sp = source_node.get_split_pos();
   INFO("Split Position: cprefix='" << sp.cprefix << "', divider=" << (int)sp.divider
                                    << ", less_count=" << sp.less_than_count
                                    << ", greater_eq_count=" << sp.greater_eq_count);

   REQUIRE(!sp.cprefix.empty());  // Expecting "prefix/" as common prefix
   REQUIRE(sp.cprefix == "prefix/");
   REQUIRE(sp.less_than_count > 0);
   REQUIRE(sp.greater_eq_count > 0);
   REQUIRE(sp.less_than_count + sp.greater_eq_count == source_node.num_branches());

   // 3. Create left node using split constructor
   uintptr_t left_buf_addr_int =
       reinterpret_cast<uintptr_t>(std::aligned_alloc(64, node_size));  // Dummy address
   ptr_address     left_addr(static_cast<unsigned int>(left_buf_addr_int));
   ptr_address_seq left_seq = {left_addr, 1};  // Different seq num
   LeafNodePtr     left_node_ptr =
       create_leaf_node_ptr(node_size, left_seq, &source_node, sp.cprefix, branch_number(0),
                            branch_number(sp.less_than_count));
   leaf_node& left_node = *left_node_ptr;
   std::free(reinterpret_cast<void*>(
       left_buf_addr_int));  // free dummy buffer used for address generation

   // 4. Verify left node
   REQUIRE(left_node.num_branches() == sp.less_than_count);
   REQUIRE(left_node.is_optimal_layout());
   for (uint32_t i = 0; i < sp.less_than_count; ++i)
   {
      branch_number bn_left(i);
      branch_number bn_source(i);
      key_view      key_left     = left_node.get_key(bn_left);
      key_view      key_source   = source_node.get_key(bn_source);
      value_type    value_left   = left_node.get_value(bn_left);
      value_type    value_source = source_node.get_value(bn_source);

      INFO("Verifying left node branch " << i << ": key_left='" << key_left << "', key_source='"
                                         << key_source << "'");
      REQUIRE(key_source.size() >= sp.cprefix.size());
      key_view expected_key_left = key_source.substr(sp.cprefix.size());
      REQUIRE(key_left == expected_key_left);
      REQUIRE(value_left == value_source);
   }

   // 5. Create right node using split constructor
   uintptr_t right_buf_addr_int =
       reinterpret_cast<uintptr_t>(std::aligned_alloc(64, node_size));  // Dummy address
   ptr_address     right_addr(static_cast<unsigned int>(right_buf_addr_int));
   ptr_address_seq right_seq  = {right_addr, 2};  // Different seq num
   LeafNodePtr right_node_ptr = create_leaf_node_ptr(node_size, right_seq, &source_node, sp.cprefix,
                                                     branch_number(sp.less_than_count),
                                                     branch_number(source_node.num_branches()));
   leaf_node&  right_node     = *right_node_ptr;
   std::free(reinterpret_cast<void*>(
       right_buf_addr_int));  // free dummy buffer used for address generation

   // 6. Verify right node
   REQUIRE(right_node.num_branches() == sp.greater_eq_count);
   REQUIRE(right_node.is_optimal_layout());
   for (uint32_t i = 0; i < sp.greater_eq_count; ++i)
   {
      branch_number bn_right(i);
      branch_number bn_source(sp.less_than_count + i);
      key_view      key_right    = right_node.get_key(bn_right);
      key_view      key_source   = source_node.get_key(bn_source);
      value_type    value_right  = right_node.get_value(bn_right);
      value_type    value_source = source_node.get_value(bn_source);

      INFO("Verifying right node branch " << i << ": key_right='" << key_right << "', key_source='"
                                          << key_source << "'");
      REQUIRE(key_source.size() >= sp.cprefix.size());
      key_view expected_key_right = key_source.substr(sp.cprefix.size());
      REQUIRE(key_right == expected_key_right);
      REQUIRE(value_right == value_source);
   }
   left_node.dump();
   right_node.dump();
}

TEST_CASE("LeafNode_SplitPrefixKey", "[psitri][leaf_node][split][edge_case]")
{
   using namespace psitri;

   constexpr size_t node_size = 4096;

   // 1. Create and populate source node with a key exactly matching the common prefix
   LeafNodePtr source_node_ptr = create_leaf_node_ptr("abc", value_type("value_abc"));
   leaf_node&  source_node     = *source_node_ptr;

   std::map<std::string, value_type> test_data = {
       {"abc/d", value_type("value_d")},
       {"abc/e", value_type::make_subtree(ptr_address(3000))},
       {"abc/f", value_type("value_f")},
       {"abc/x", value_type("value_x")},
       {"abc/y", value_type::make_value_node(ptr_address(4000))},
       {"abc/z", value_type("value_z")}};

   // Insert data
   for (const auto& pair : test_data)
   {
      op::leaf_insert ins{.src   = source_node,
                          .lb    = source_node.lower_bound(pair.first),
                          .key   = pair.first,
                          .value = pair.second};
      REQUIRE(source_node.can_apply(ins) != leaf_node::can_apply_mode::none);
      branch_number bn = source_node.lower_bound(pair.first);
      ins.lb           = bn;  // Ensure lower_bound is correct
      source_node.apply(ins);
   }
   REQUIRE(source_node.num_branches() == test_data.size() + 1);  // +1 for initial "abc" key

   // 2. Get split position
   leaf_node::split_pos sp = source_node.get_split_pos();
   INFO("Split Position (Edge Case): cprefix='" << sp.cprefix << "', divider=" << (int)sp.divider
                                                << ", less_count=" << sp.less_than_count
                                                << ", greater_eq_count=" << sp.greater_eq_count);

   REQUIRE(sp.cprefix == "abc");  // Verify common prefix
   REQUIRE(sp.less_than_count > 0);
   REQUIRE(sp.greater_eq_count > 0);
   REQUIRE(sp.less_than_count + sp.greater_eq_count == source_node.num_branches());
   // Determine which side the 'abc' key should fall on based on the divider
   // If 'abc' exists, it should be the first key (bn=0).
   // get_key(best_split_idx)[cprefix_len] determines the divider.
   // Keys strictly less than the divider's first differing char go left.
   // In this setup, 'abc' itself has no char at cprefix_len, it should be <= divider.
   // Let's assume the split happens correctly, 'abc' should be in the left node.
   REQUIRE(sp.less_than_count >= 1);  // Ensure 'abc' lands somewhere, likely left.

   // 3. Create left node
   uintptr_t   left_buf_addr_int = reinterpret_cast<uintptr_t>(std::aligned_alloc(64, node_size));
   ptr_address left_addr(static_cast<unsigned int>(left_buf_addr_int));
   ptr_address_seq left_seq = {left_addr, 3};  // Different seq num
   LeafNodePtr     left_node_ptr =
       create_leaf_node_ptr(node_size, left_seq, &source_node, sp.cprefix, branch_number(0),
                            branch_number(sp.less_than_count));
   leaf_node& left_node = *left_node_ptr;
   std::free(reinterpret_cast<void*>(left_buf_addr_int));

   // 4. Verify left node
   REQUIRE(left_node.num_branches() == sp.less_than_count);
   REQUIRE(left_node.is_optimal_layout());
   bool found_empty_key = false;
   for (uint32_t i = 0; i < sp.less_than_count; ++i)
   {
      branch_number bn_left(i);
      branch_number bn_source(i);
      key_view      key_left     = left_node.get_key(bn_left);
      key_view      key_source   = source_node.get_key(bn_source);
      value_type    value_left   = left_node.get_value(bn_left);
      value_type    value_source = source_node.get_value(bn_source);

      INFO("Verifying left node (Edge Case) branch " << i << ": key_left='" << key_left
                                                     << "', key_source='" << key_source << "'");
      REQUIRE(key_source.size() >= sp.cprefix.size());
      key_view expected_key_left = key_source.substr(sp.cprefix.size());
      REQUIRE(key_left == expected_key_left);
      REQUIRE(value_left == value_source);

      if (key_source == sp.cprefix)
      {
         REQUIRE(key_left.empty());  // Crucial check: key matching cprefix becomes empty
         found_empty_key = true;
      }
   }
   // Ensure the original 'abc' key was processed and resulted in an empty key
   // This assumes 'abc' ends up in the left node based on split logic.
   REQUIRE(found_empty_key);

   // 5. Create right node
   uintptr_t   right_buf_addr_int = reinterpret_cast<uintptr_t>(std::aligned_alloc(64, node_size));
   ptr_address right_addr(static_cast<unsigned int>(right_buf_addr_int));
   ptr_address_seq right_seq  = {right_addr, 4};  // Different seq num
   LeafNodePtr right_node_ptr = create_leaf_node_ptr(node_size, right_seq, &source_node, sp.cprefix,
                                                     branch_number(sp.less_than_count),
                                                     branch_number(source_node.num_branches()));
   leaf_node&  right_node     = *right_node_ptr;
   std::free(reinterpret_cast<void*>(right_buf_addr_int));

   // 6. Verify right node
   REQUIRE(right_node.num_branches() == sp.greater_eq_count);
   REQUIRE(right_node.is_optimal_layout());
   for (uint32_t i = 0; i < sp.greater_eq_count; ++i)
   {
      branch_number bn_right(i);
      branch_number bn_source(sp.less_than_count + i);
      key_view      key_right    = right_node.get_key(bn_right);
      key_view      key_source   = source_node.get_key(bn_source);
      value_type    value_right  = right_node.get_value(bn_right);
      value_type    value_source = source_node.get_value(bn_source);

      INFO("Verifying right node (Edge Case) branch " << i << ": key_right='" << key_right
                                                      << "', key_source='" << key_source << "'");
      REQUIRE(key_source.size() >= sp.cprefix.size());
      key_view expected_key_right = key_source.substr(sp.cprefix.size());
      REQUIRE(key_right == expected_key_right);
      REQUIRE(value_right == value_source);
      REQUIRE(key_right != "");  // No empty key expected in the right node in this setup
   }
}