#include <algorithm>
#include <catch2/catch_all.hpp>
#include <cstddef>  // For std::byte
#include <cstdlib>  // For std::aligned_alloc, std::free
#include <cstring>  // For memcpy
#include <map>
#include <memory>
#include <psitri/node/leaf.hpp>
#include <psitri/value_type.hpp>  // Include value_type
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
            // We got the buffer via aligned_alloc, so use free
            std::free(node);
         }
      }
   };

   using LeafNodePtr = std::unique_ptr<psitri::leaf_node, LeafNodeDeleter>;

   // Helper to create a leaf node unique_ptr
   LeafNodePtr create_leaf_node(psitri::key_view          initial_key,
                                const psitri::value_type& initial_value)
   {
      constexpr size_t alignment = 64;    // As per node.hpp _node_size definition basis
      constexpr size_t node_size = 4096;  // Standard node size

      void* buffer = std::aligned_alloc(alignment, node_size);
      if (!buffer)
      {
         throw std::bad_alloc();  // or handle allocation failure appropriately
      }
      // Zero initialize the buffer for predictability
      std::memset(buffer, 0, node_size);

      // Correctly create ptr_address from void*
      uintptr_t           buffer_addr_int = reinterpret_cast<uintptr_t>(buffer);
      psitri::ptr_address buffer_addr(
          static_cast<unsigned int>(buffer_addr_int));  // Assuming ptr_address holds unsigned int
      psitri::ptr_address_seq seq = {buffer_addr, 0};

      psitri::leaf_node* raw_node = nullptr;
      try
      {
         raw_node = new (buffer) psitri::leaf_node(node_size, seq, initial_key, initial_value);
      }
      catch (...)
      {
         std::free(buffer);  // Clean up buffer if constructor throws
         throw;
      }

      return LeafNodePtr(raw_node);
   }

   // Helper to calculate expected dead space increase based on public info
   size_t calculate_dead_space_increase(psitri::key_view key, const psitri::value_type& value)
   {
      // Size constants mimicking private leaf_node::key and leaf_node::value_data headers
      constexpr size_t key_header_size        = 2;
      constexpr size_t value_data_header_size = 2;

      size_t dead_increase = key_header_size + key.size();
      if (value.is_view())
      {  // Use is_view() instead of is_inline()
         dead_increase += value_data_header_size + value.view().size();
      }
      // No increase for address types as only the metadata entry is removed
      return dead_increase;
   }

}  // namespace

TEST_CASE("leaf_node basic insert and lookup", "[psitri][leaf_node]")
{
   using namespace psitri;

   key_view   initial_key   = "banana";
   value_type initial_value = value_type("yellow");

   LeafNodePtr node_ptr = create_leaf_node(initial_key, initial_value);
   leaf_node&  node     = *node_ptr;

   REQUIRE(node.num_branches() == 1);
   REQUIRE(node.type() == node_type::leaf);
   REQUIRE(node.size() == 4096);

   SECTION("Initial state verification")
   {
      branch_number bn0 = branch_number(0);
      REQUIRE(node.get(initial_key) == bn0);
      REQUIRE(node.lower_bound(initial_key) == bn0);
      REQUIRE(node.get_key(bn0) == initial_key);
      REQUIRE(node.get_value(bn0) == initial_value);
      REQUIRE(node.get_value(bn0).view() == "yellow");

      // Test keys before and after
      REQUIRE(node.lower_bound("apple") == bn0);
      REQUIRE(node.lower_bound("cantaloupe") == branch_number(1));
      REQUIRE(node.get("apple") ==
              branch_number(node.num_branches()));  // get should return num_branches if not found
   }

   SECTION("Insert multiple keys")
   {
      // Use std::map to keep track of expected order and values
      std::map<std::string, value_type> expected_data;
      expected_data[std::string(initial_key)] = initial_value;

      std::vector<std::pair<key_view, value_type>> data_to_insert = {
          {"apple", value_type("red")},
          {"date", value_type("brown")},
          {"cherry", value_type("dark red")},
          {"fig", value_type("purple")},
          {"grape", value_type("green")}};

      // Insert data
      for (const auto& pair : data_to_insert)
      {
         key_view          key   = pair.first;
         const value_type& value = pair.second;

         INFO("Checking space for key: " << key);
         REQUIRE(node.can_insert(key, value) >= 0);  // Check space before insert

         branch_number expected_bn = node.lower_bound(key);
         INFO("Attempting to insert key: " << key << " at expected_bn: " << *expected_bn);
         REQUIRE(node.get(key) == branch_number(node.num_branches()));  // Key shouldn't exist yet

         branch_number actual_bn = node.insert(expected_bn, key, value);

         REQUIRE(actual_bn == expected_bn);
         expected_data[std::string(key)] = value;  // Add to our expected map
         REQUIRE(node.num_branches() == expected_data.size());

         // Quick check immediately after insert
         INFO("Verifying key immediately after insert: " << key);
         REQUIRE(node.get(key) == actual_bn);
         REQUIRE(node.get_key(actual_bn) == key);
         REQUIRE(node.get_value(actual_bn) == value);
      }

      REQUIRE(node.num_branches() == expected_data.size());

      // Verify all data using the map order (map automatically sorts keys)
      size_t current_expected_bn_val = 0;
      for (const auto& pair : expected_data)
      {
         key_view          key   = pair.first;
         const value_type& value = pair.second;
         branch_number     expected_bn(static_cast<uint8_t>(current_expected_bn_val));

         INFO("Verifying key in final map check: " << key);
         REQUIRE(node.lower_bound(key) == expected_bn);
         REQUIRE(node.get(key) == expected_bn);
         REQUIRE(node.get_key(expected_bn) == key);
         REQUIRE(node.get_value(expected_bn) == value);

         // Check lower_bound for keys slightly before and after
         std::string before_key_str = std::string(key);
         if (!before_key_str.empty() && before_key_str != "apple")
         {  // Special case for first element
            char last_char = before_key_str.back();
            if (last_char > 0)
            {
               before_key_str.back() = last_char - 1;
               INFO("Checking lower_bound for key slightly before: "
                    << before_key_str << " (original: " << key << ")");
               REQUIRE(node.lower_bound(before_key_str) == expected_bn);
            }
         }
         std::string after_key_str = std::string(key) + "z";
         INFO("Checking lower_bound for key slightly after: " << after_key_str
                                                              << " (original: " << key << ")");
         REQUIRE(node.lower_bound(after_key_str) ==
                 branch_number(static_cast<uint8_t>(current_expected_bn_val + 1)));

         current_expected_bn_val++;
      }

      // Verify non-existent keys
      REQUIRE(node.get("aardvark") == branch_number(node.num_branches()));
      REQUIRE(node.get("mango") == branch_number(node.num_branches()));
      REQUIRE(node.get("zzz") == branch_number(node.num_branches()));

      REQUIRE(node.lower_bound("zzz") == branch_number(node.num_branches()));
      REQUIRE(node.lower_bound("aardvark") == branch_number(0));  // Should be before 'apple'

      // Explicitly verify sorted order by iterating through branches
      INFO("Explicitly checking sorted order of keys in the node");
      for (uint8_t i = 0; i < node.num_branches() - 1; ++i)
      {
         branch_number current_bn(i);
         branch_number next_bn(i + 1);
         key_view      current_key = node.get_key(current_bn);
         key_view      next_key    = node.get_key(next_bn);
         INFO("Comparing key[" << i << "] = " << current_key << " with key[" << (i + 1)
                               << "] = " << next_key);
         REQUIRE(current_key < next_key);
      }
   }

   SECTION("Insert and lookup address types (subtree, value_node)")
   {
      branch_number initial_num_branches = branch_number(node.num_branches());

      // Define some addresses and keys
      ptr_address subtree_addr(12345);
      ptr_address value_node_addr(67890);
      key_view    subtree_key    = "subtree_test";
      key_view    value_node_key = "value_node_test";

      value_type subtree_val    = value_type::make_subtree(subtree_addr);
      value_type value_node_val = value_type::make_value_node(value_node_addr);

      // Insert subtree
      branch_number expected_bn_sub = node.lower_bound(subtree_key);
      REQUIRE(node.can_insert(subtree_key, subtree_val) >= 0);
      branch_number actual_bn_sub = node.insert(expected_bn_sub, subtree_key, subtree_val);
      REQUIRE(actual_bn_sub == expected_bn_sub);
      REQUIRE(node.num_branches() == *initial_num_branches + 1);

      // Insert value_node
      branch_number expected_bn_val = node.lower_bound(value_node_key);
      REQUIRE(node.can_insert(value_node_key, value_node_val) >= 0);
      branch_number actual_bn_val = node.insert(expected_bn_val, value_node_key, value_node_val);
      REQUIRE(actual_bn_val == expected_bn_val);
      REQUIRE(node.num_branches() == *initial_num_branches + 2);

      // Verify subtree retrieval
      INFO("Verifying subtree key: " << subtree_key);
      REQUIRE(node.get(subtree_key) == actual_bn_sub);
      REQUIRE(node.lower_bound(subtree_key) == actual_bn_sub);
      REQUIRE(node.get_key(actual_bn_sub) == subtree_key);
      value_type retrieved_sub_val = node.get_value(actual_bn_sub);
      REQUIRE(retrieved_sub_val == subtree_val);
      REQUIRE(retrieved_sub_val.is_subtree());
      REQUIRE(!retrieved_sub_val.is_value_node());
      REQUIRE(!retrieved_sub_val.is_view());
      REQUIRE(!retrieved_sub_val.is_remove());
      REQUIRE(retrieved_sub_val.subtree_address() == subtree_addr);

      // Verify value_node retrieval
      INFO("Verifying value_node key: " << value_node_key);
      REQUIRE(node.get(value_node_key) == actual_bn_val);
      REQUIRE(node.lower_bound(value_node_key) == actual_bn_val);
      REQUIRE(node.get_key(actual_bn_val) == value_node_key);
      value_type retrieved_val_val = node.get_value(actual_bn_val);
      REQUIRE(retrieved_val_val == value_node_val);
      REQUIRE(retrieved_val_val.is_value_node());
      REQUIRE(!retrieved_val_val.is_subtree());
      REQUIRE(!retrieved_val_val.is_view());
      REQUIRE(!retrieved_val_val.is_remove());
      REQUIRE(retrieved_val_val.value_address() == value_node_addr);

      // Make sure original key is still present
      REQUIRE(node.get(initial_key) < branch_number(node.num_branches()));

      // Verify cline count after initial address insertions
      // (Assuming 12345 and 67890 have different upper bits / cacheline bases)
      REQUIRE(node.clines_capacity() == 2);

      // --- Test cline sharing ---
      INFO("Testing cline sharing");
      ptr_address sub_addr_base =
          ptr_address(*subtree_addr & ~0x0f);  // Get the base address (upper bits)

      // Create new addresses sharing the same base but different indices (lower 4 bits)
      ptr_address sub_addr_2(*sub_addr_base | 2);
      ptr_address sub_addr_3(*sub_addr_base | 3);
      REQUIRE((*sub_addr_2 & 0x0f) == 2);  // Double check index calculation
      REQUIRE((*sub_addr_3 & 0x0f) == 3);
      REQUIRE((*sub_addr_2 & ~0x0f) == (*subtree_addr & ~0x0f));  // Double check base preservation
      REQUIRE((*sub_addr_3 & ~0x0f) == (*subtree_addr & ~0x0f));

      key_view key_sub_2 = "subtree_test_2";
      key_view key_sub_3 = "subtree_test_3";

      value_type val_sub_2 = value_type::make_subtree(sub_addr_2);
      value_type val_sub_3 = value_type::make_subtree(sub_addr_3);

      branch_number branches_before_sharing = branch_number(node.num_branches());

      // Insert first shared cline address
      INFO("Inserting key with shared cline base: " << key_sub_2);
      branch_number expected_bn_sub_2 = node.lower_bound(key_sub_2);
      REQUIRE(node.can_insert(key_sub_2, val_sub_2) >= 0);
      branch_number actual_bn_sub_2 = node.insert(expected_bn_sub_2, key_sub_2, val_sub_2);
      REQUIRE(actual_bn_sub_2 == expected_bn_sub_2);
      REQUIRE(node.num_branches() == *branches_before_sharing + 1);
      REQUIRE(node.clines_capacity() == 2);  // Cline count should NOT increase

      // Insert second shared cline address
      INFO("Inserting second key with shared cline base: " << key_sub_3);
      branch_number expected_bn_sub_3 = node.lower_bound(key_sub_3);
      REQUIRE(node.can_insert(key_sub_3, val_sub_3) >= 0);
      branch_number actual_bn_sub_3 = node.insert(expected_bn_sub_3, key_sub_3, val_sub_3);
      REQUIRE(actual_bn_sub_3 == expected_bn_sub_3);
      REQUIRE(node.num_branches() == *branches_before_sharing + 2);
      REQUIRE(node.clines_capacity() == 2);  // Cline count should STILL not increase

      // Verify retrieval of shared cline addresses
      INFO("Verifying retrieval of key " << key_sub_2);
      value_type retrieved_sub_2 = node.get_value(actual_bn_sub_2);
      REQUIRE(node.get(key_sub_2) == actual_bn_sub_2);
      REQUIRE(node.get_key(actual_bn_sub_2) == key_sub_2);
      REQUIRE(retrieved_sub_2 == val_sub_2);
      REQUIRE(retrieved_sub_2.is_subtree());
      REQUIRE(retrieved_sub_2.subtree_address() == sub_addr_2);

      INFO("Verifying retrieval of key " << key_sub_3);
      value_type retrieved_sub_3 = node.get_value(actual_bn_sub_3);
      REQUIRE(node.get(key_sub_3) == actual_bn_sub_3);
      REQUIRE(node.get_key(actual_bn_sub_3) == key_sub_3);
      REQUIRE(retrieved_sub_3 == val_sub_3);
      REQUIRE(retrieved_sub_3.is_subtree());
      REQUIRE(retrieved_sub_3.subtree_address() == sub_addr_3);

      // Also verify the original shared-base address is still correct
      INFO("Re-verifying original key " << subtree_key << " that shares cline base");
      value_type retrieved_orig_sub = node.get_value(actual_bn_sub);
      REQUIRE(retrieved_orig_sub == subtree_val);
      REQUIRE(retrieved_orig_sub.is_subtree());
      REQUIRE(retrieved_orig_sub.subtree_address() == subtree_addr);
   }

   SECTION("Insert and retrieve empty value")
   {
      INFO("Testing empty value insertion and retrieval");
      key_view   empty_key = "empty_value_key";
      value_type empty_val = value_type("");  // Create value with empty view

      REQUIRE(empty_val.is_view());
      REQUIRE(empty_val.view().empty());

      branch_number expected_bn = node.lower_bound(empty_key);
      REQUIRE(node.can_insert(empty_key, empty_val) >= 0);
      branch_number actual_bn = node.insert(expected_bn, empty_key, empty_val);
      REQUIRE(actual_bn == expected_bn);

      INFO("Retrieving empty value for key: " << empty_key);
      value_type retrieved_val = node.get_value(actual_bn);

      REQUIRE(retrieved_val.is_view());
      REQUIRE(retrieved_val.view().empty());
      REQUIRE(retrieved_val.view().size() == 0);
      REQUIRE(retrieved_val == empty_val);  // Check equality

      // Ensure get() also finds the key
      REQUIRE(node.get(empty_key) == actual_bn);
   }

   SECTION("Visit branches")
   {
      INFO("Testing visit_branches");

      // Define addresses and values
      ptr_address addr1(1001), addr2(2002), addr3(3003);
      value_type  val_view = value_type("some_data");
      value_type  val_sub1 = value_type::make_subtree(addr1);
      value_type  val_val2 = value_type::make_value_node(addr2);
      value_type  val_sub3 = value_type::make_subtree(addr3);

      // Expected addresses to be visited
      std::vector<ptr_address> expected_addresses = {addr1, addr2, addr3};

      // Insert keys
      REQUIRE(node.can_insert("key_view", val_view) >= 0);
      node.insert(node.lower_bound("key_view"), "key_view", val_view);

      REQUIRE(node.can_insert("key_sub1", val_sub1) >= 0);
      node.insert(node.lower_bound("key_sub1"), "key_sub1", val_sub1);

      REQUIRE(node.can_insert("key_val2", val_val2) >= 0);
      node.insert(node.lower_bound("key_val2"), "key_val2", val_val2);

      REQUIRE(node.can_insert("key_sub3", val_sub3) >= 0);
      node.insert(node.lower_bound("key_sub3"), "key_sub3", val_sub3);

      // Collect visited addresses
      std::vector<ptr_address> visited_addresses;
      node.visit_branches([&](ptr_address addr) { visited_addresses.push_back(addr); });

      // Sort both vectors for comparison
      std::sort(expected_addresses.begin(), expected_addresses.end());
      std::sort(visited_addresses.begin(), visited_addresses.end());

      // Verify
      REQUIRE(visited_addresses.size() == expected_addresses.size());
      REQUIRE(visited_addresses == expected_addresses);

      // Test case with only view data (after re-creating the node)
      INFO("Testing visit_branches with only view data");
      LeafNodePtr node_view_only = create_leaf_node("view1", value_type("v1"));
      REQUIRE(node_view_only->can_insert("view2", value_type("v2")) >= 0);
      node_view_only->insert(node_view_only->lower_bound("view2"), "view2", value_type("v2"));

      std::vector<ptr_address> visited_view_only;
      node_view_only->visit_branches([&](ptr_address addr) { visited_view_only.push_back(addr); });
      REQUIRE(visited_view_only.empty());
   }

   SECTION("Clone constructor with tight fit")
   {
      INFO("Testing clone constructor with minimal free space");

      // 1. Create and populate a source node
      LeafNodePtr source_node_ptr = create_leaf_node("initial_clone_key", value_type("init_val"));
      leaf_node&  source_node     = *source_node_ptr;

      // Add varied data
      std::vector<std::pair<key_view, value_type>> data_to_insert = {
          {"clone_apple", value_type("red_clone")},
          {"clone_date", value_type::make_subtree(ptr_address(99001))},
          {"clone_cherry", value_type("dark_red_clone")},
          {"clone_fig", value_type::make_value_node(ptr_address(99002))},
          {"clone_grape", value_type("green_clone")}};

      for (const auto& pair : data_to_insert)
      {
         REQUIRE(source_node.can_insert(pair.first, pair.second) >= 0);
         branch_number bn = source_node.lower_bound(pair.first);
         source_node.insert(bn, pair.first, pair.second);
      }
      size_t source_branches = source_node.num_branches();
      REQUIRE(source_branches > 1);

      // 2. Calculate required size for clone (round up to alignment)
      constexpr size_t alignment         = 64;
      constexpr size_t source_total_size = 4096;
      int              source_free       = source_node.free_space();
      REQUIRE(source_free >= 0);
      size_t used_space    = source_total_size - source_free;
      size_t required_size = ((used_space + alignment - 1) / alignment) * alignment;
      INFO("Source node free space: " << source_free << ", Used space: " << used_space
                                      << ", Required clone size (aligned): " << required_size);
      REQUIRE(required_size <= source_total_size);

      // 3. Allocate destination buffer
      void* dest_buffer = std::aligned_alloc(alignment, required_size);
      REQUIRE(dest_buffer != nullptr);
      std::memset(dest_buffer, 0, required_size);  // Zero initialize

      // 4. Construct clone node
      uintptr_t       dest_addr_int = reinterpret_cast<uintptr_t>(dest_buffer);
      ptr_address     dest_addr(static_cast<unsigned int>(dest_addr_int));
      ptr_address_seq dest_seq       = {dest_addr, 0};
      leaf_node*      raw_clone_node = nullptr;
      LeafNodePtr     clone_node_ptr;  // Manages lifetime via custom deleter
      try
      {
         raw_clone_node = new (dest_buffer) leaf_node(required_size, dest_seq, &source_node);
         clone_node_ptr.reset(raw_clone_node);
      }
      catch (...)
      {
         std::free(dest_buffer);  // Clean up buffer if constructor throws
         INFO("Clone constructor threw an exception");
         REQUIRE(false);  // Force failure
         throw;
      }
      leaf_node& clone_node = *clone_node_ptr;

      // 5. Verify clone node properties
      INFO("Verifying clone node properties");
      REQUIRE(clone_node.size() == required_size);
      REQUIRE(clone_node.num_branches() == source_node.num_branches());
      REQUIRE(clone_node.alloc_pos() == source_node.alloc_pos());
      REQUIRE(clone_node.dead_space() ==
              0);  // Clone constructor currently doesn't compact dead space
      REQUIRE(clone_node.clines_capacity() == source_node.clines_capacity());
      REQUIRE(not source_node.is_optimal_layout());
      INFO("verifying cloning to optimal layout");
      REQUIRE(clone_node.is_optimal_layout());

      // Check free space - should be minimal due to rounding
      int clone_free = clone_node.free_space();
      INFO("Clone node free space: " << clone_free);
      REQUIRE(clone_free >= 0);
      REQUIRE(clone_free < alignment);  // Should have less than one alignment block free

      // 6. Verify data integrity by comparing all branches
      INFO("Verifying clone node data integrity");
      REQUIRE(source_node.num_branches() == clone_node.num_branches());
      for (uint8_t i = 0; i < source_node.num_branches(); ++i)
      {
         branch_number bn(i);
         INFO("Comparing branch number: " << i);
         REQUIRE(source_node.get_key(bn) == clone_node.get_key(bn));
         REQUIRE(source_node.get_value(bn) == clone_node.get_value(bn));
      }
   }

   SECTION("Remove elements")
   {
      INFO("Testing remove method");

      // 1. Setup a node with mixed data types and shared/unique clines
      ptr_address addr_u1(1000), addr_u2(2000);                         // Unique cline bases
      ptr_address addr_s1(3000), addr_s2(3000 | 1), addr_s3(3000 | 2);  // Shared cline base

      std::map<std::string, value_type> initial_data = {
          {"key_aa", value_type("view_aa")},
          {"key_bb", value_type::make_subtree(addr_u1)},  // Unique cline 1
          {"key_cc", value_type("view_cc")},
          {"key_dd", value_type::make_value_node(addr_s1)},  // Shared cline base 3000, idx 0
          {"key_ee", value_type("view_ee")},
          {"key_ff", value_type::make_subtree(addr_s2)},     // Shared cline base 3000, idx 1
          {"key_gg", value_type::make_value_node(addr_u2)},  // Unique cline 2
          {"key_hh", value_type::make_subtree(addr_s3)},     // Shared cline base 3000, idx 2
          {"key_ii", value_type("view_ii")}};

      LeafNodePtr node_ptr = create_leaf_node("placeholder", value_type("temp"));
      leaf_node&  node     = *node_ptr;
      node.remove(branch_number(0));  // Remove placeholder
      REQUIRE(node.num_branches() == 0);

      // Populate node
      for (const auto& pair : initial_data)
      {
         REQUIRE(node.can_insert(pair.first, pair.second) >= 0);
         branch_number bn = node.lower_bound(pair.first);
         node.insert(bn, pair.first, pair.second);
      }
      REQUIRE(node.num_branches() == initial_data.size());
      uint32_t expected_initial_clines = 3;  // u1(1k), u2(2k), s1(3k)
      REQUIRE(node.clines_capacity() == expected_initial_clines);

      // Verify helper
      auto verify_remaining_data =
          [&](const leaf_node& check_node, const std::map<std::string, value_type>& expected)
      {
         REQUIRE(check_node.num_branches() == expected.size());
         size_t current_expected_bn_val = 0;
         for (const auto& pair : expected)
         {
            branch_number expected_bn(static_cast<uint8_t>(current_expected_bn_val));
            key_view      key = pair.first;
            INFO("Verifying remaining key: " << key);
            REQUIRE(check_node.get(key) == expected_bn);
            REQUIRE(check_node.lower_bound(key) == expected_bn);
            REQUIRE(check_node.get_key(expected_bn) == key);
            REQUIRE(check_node.get_value(expected_bn) == pair.second);
            current_expected_bn_val++;
         }
         // Explicitly verify sorted order
         for (uint8_t i = 0; i < check_node.num_branches() - 1; ++i)
         {
            REQUIRE(check_node.get_key(branch_number(i)) <
                    check_node.get_key(branch_number(i + 1)));
         }
      };

      std::map<std::string, value_type> expected_after_remove = initial_data;

      // 2. Remove View Data (key_cc)
      INFO("Removing view data (key_cc)");
      uint16_t      initial_branches   = node.num_branches();     // Define before use
      uint32_t      initial_clines     = node.clines_capacity();  // Define before use
      key_view      key_to_remove_view = "key_cc";
      value_type    val_removed_view   = expected_after_remove.at(std::string(key_to_remove_view));
      branch_number bn_view            = node.get(key_to_remove_view);
      size_t        expected_dead_increase_view =
          calculate_dead_space_increase(key_to_remove_view, val_removed_view);
      uint16_t dead_space_before_remove_view = node.dead_space();  // Get dead space BEFORE remove

      node.remove(bn_view);
      expected_after_remove.erase(std::string(key_to_remove_view));

      REQUIRE(node.num_branches() == initial_branches - 1);
      // Check the INCREASE in dead space
      REQUIRE(node.dead_space() == dead_space_before_remove_view + expected_dead_increase_view);
      REQUIRE(!node.is_optimal_layout());
      REQUIRE(node.clines_capacity() == initial_clines);  // Removing view shouldn't affect clines
      verify_remaining_data(node, expected_after_remove);

      // 3. Remove Address Data (Shared Cline - key_ff)
      INFO("Removing address data from shared cline (key_ff)");
      initial_branches                = node.num_branches();     // Define before use
      initial_clines                  = node.clines_capacity();  // Define before use
      key_view   key_to_remove_shared = "key_ff";
      value_type val_removed_shared   = expected_after_remove.at(std::string(key_to_remove_shared));
      branch_number bn_shared         = node.get(key_to_remove_shared);
      size_t        expected_dead_increase_shared =
          calculate_dead_space_increase(key_to_remove_shared, val_removed_shared);
      uint16_t dead_space_before_remove_shared = node.dead_space();  // Get dead space BEFORE remove

      node.remove(bn_shared);
      expected_after_remove.erase(std::string(key_to_remove_shared));

      REQUIRE(node.num_branches() == initial_branches - 1);
      // Check the INCREASE in dead space
      REQUIRE(node.dead_space() == dead_space_before_remove_shared + expected_dead_increase_shared);
      REQUIRE(!node.is_optimal_layout());
      REQUIRE(node.clines_capacity() ==
              initial_clines);  // Cline count shouldn't change yet (key_dd and key_hh share it)
      verify_remaining_data(node, expected_after_remove);

      // 4. Remove Address Data (Last on Unique Cline - key_gg) - Check Non-Last Cline Freeing
      INFO("Removing address data last on unique cline (key_gg) - Non-Last Cline Slot");
      initial_branches                   = node.num_branches();     // Define before use
      initial_clines                     = node.clines_capacity();  // Define before use
      key_view   key_to_remove_unique_gg = "key_gg";
      value_type val_removed_unique_gg =
          expected_after_remove.at(std::string(key_to_remove_unique_gg));
      branch_number bn_unique_gg = node.get(key_to_remove_unique_gg);
      size_t        expected_dead_increase_unique_gg =
          calculate_dead_space_increase(key_to_remove_unique_gg, val_removed_unique_gg);
      uint16_t dead_space_before_remove_unique_gg =
          node.dead_space();  // Get dead space BEFORE remove

      node.remove(bn_unique_gg);
      expected_after_remove.erase(std::string(key_to_remove_unique_gg));

      REQUIRE(node.num_branches() == initial_branches - 1);
      // Check the INCREASE in dead space
      REQUIRE(node.dead_space() ==
              dead_space_before_remove_unique_gg + expected_dead_increase_unique_gg);
      REQUIRE(!node.is_optimal_layout());
      // Cline slot for addr_u2 (2000) is freed, but it might not be the *last* slot,
      // so capacity shouldn't necessarily decrease yet. Check it didn't increase.
      REQUIRE(node.clines_capacity() <= initial_clines);
      REQUIRE(node.get(key_to_remove_unique_gg) == branch_number(node.num_branches()));
      verify_remaining_data(node, expected_after_remove);

      // 5. Remove Address Data (Last on Shared Cline - key_hh)
      // Now remove key_hh, which shares base 3000 with key_dd. key_ff was already removed.
      // This should free up the cline slot for base 3000.
      INFO("Removing address data last on shared cline (key_hh)");
      initial_branches                     = node.num_branches();     // Define before use
      initial_clines                       = node.clines_capacity();  // Define before use
      key_view   key_to_remove_last_shared = "key_hh";
      value_type val_removed_last_shared =
          expected_after_remove.at(std::string(key_to_remove_last_shared));
      branch_number bn_last_shared = node.get(key_to_remove_last_shared);
      size_t        expected_dead_increase_last_shared =
          calculate_dead_space_increase(key_to_remove_last_shared, val_removed_last_shared);
      bool was_last_cline_slot = false;  // Determine if the cline being freed *was* the last one
      uint16_t dead_space_before_remove_last_shared =
          node.dead_space();  // Get dead space BEFORE remove

      // Need to find the actual cline offset used by key_hh before removing it
      // TODO: Add logic to determine if the cline slot being freed was the last one
      // For now, assume it might not be.
      node.remove(bn_last_shared);
      expected_after_remove.erase(std::string(key_to_remove_last_shared));

      REQUIRE(node.num_branches() == initial_branches - 1);
      // Check the INCREASE in dead space
      REQUIRE(node.dead_space() ==
              dead_space_before_remove_last_shared + expected_dead_increase_last_shared);
      REQUIRE(!node.is_optimal_layout());
      // We only decrement capacity if the *absolute last* slot is freed. Can't easily check that here.
      // If the freed cline wasn't the last one, capacity stays the same.
      // Let's check it didn't INCREASE. A decrease is possible but hard to predict without knowing the index.
      REQUIRE(node.clines_capacity() <= initial_clines);
      REQUIRE(node.get(key_to_remove_last_shared) == branch_number(node.num_branches()));
      verify_remaining_data(node, expected_after_remove);
   }

   SECTION("Update value")
   {
      INFO("Testing update_value method comprehensively");

      // --- Initial Node Setup ---
      ptr_address addr_u1(1000), addr_u2(2000);            // Initial Unique Clines
      ptr_address addr_s1a(3000 | 1), addr_s1b(3000 | 2);  // Initial Shared Cline (Base 3k)
      ptr_address addr_s2(4000 | 1);                       // Initial Shared Cline (Base 4k)

      // Keys for each starting state --- RE-ADDED ---
      key_view key_start_null       = "start_null";
      key_view key_start_inline     = "start_inline";
      key_view key_start_subtree    = "start_subtree";
      key_view key_start_valnode    = "start_valnode";
      key_view key_start_shared_sub = "start_shared_sub";  // Shares 3k base initially
                                                           // --- END RE-ADDED KEYS ---

      std::map<std::string, value_type> initial_data = {
          {std::string(key_start_null), value_type("")},                  // State 1: Null
          {std::string(key_start_inline), value_type("initial_inline")},  // State 2: Inline
          {std::string(key_start_subtree),
           value_type::make_subtree(addr_u1)},  // State 3: Subtree (Unique Cline 1k)
          {std::string(key_start_valnode),
           value_type::make_value_node(addr_u2)},  // State 4: ValueNode (Unique Cline 2k)
          {std::string(key_start_shared_sub),
           value_type::make_subtree(addr_s1a)},  // State 3: Subtree (Shared Cline 3k)
          // Add another entry for shared cline 3k to test shared -> X transitions
          {"shared_valnode_3k",
           value_type::make_value_node(addr_s1b)}  // State 4: ValueNode (Shared Cline 3k)
      };

      LeafNodePtr node_ptr = create_leaf_node("placeholder", value_type("temp"));
      leaf_node&  node     = *node_ptr;
      node.remove(branch_number(0));  // Remove placeholder
      REQUIRE(node.num_branches() == 0);

      // Populate node
      for (const auto& pair : initial_data)
      {
         REQUIRE(node.can_insert(pair.first, pair.second) >= 0);
         branch_number bn = node.lower_bound(pair.first);
         node.insert(bn, pair.first, pair.second);
      }
      REQUIRE(node.num_branches() == initial_data.size());
      uint32_t expected_initial_clines = 3;  // u1(1k), u2(2k), s1(3k)
      REQUIRE(node.clines_capacity() == expected_initial_clines);

      // --- Force optimal layout for testing the flag ---
      INFO("Cloning node to ensure optimal layout for update tests");
      size_t required_size = node.size();  // Use full size for simplicity
      void*  clone_buffer  = std::aligned_alloc(64, required_size);
      REQUIRE(clone_buffer != nullptr);
      std::memset(clone_buffer, 0, required_size);
      uintptr_t       clone_addr_int = reinterpret_cast<uintptr_t>(clone_buffer);
      ptr_address     clone_addr(static_cast<unsigned int>(clone_addr_int));
      ptr_address_seq clone_seq = {clone_addr, 0};
      LeafNodePtr optimal_node_ptr(new (clone_buffer) leaf_node(required_size, clone_seq, &node));
      leaf_node&  optimal_node = *optimal_node_ptr;
      REQUIRE(optimal_node.is_optimal_layout());  // Start optimally

      // Define addresses used for *new* values during updates
      ptr_address addr_new_u3(5000);       // New Unique Cline 5k
      ptr_address addr_new_u4(6000);       // New Unique Cline 6k
      ptr_address addr_new_s3a(7000 | 1);  // New Shared Cline 7k
      ptr_address addr_new_s3b(7000 | 2);  // New Shared Cline 7k

      // --- Test Matrix (using optimal_node) ---

      SECTION("Update From Null")
      {
         branch_number bn = optimal_node.get(key_start_null);
         REQUIRE(optimal_node.get_value(bn).view().empty());  // Verify initial state

         SECTION("To Null")
         {
            INFO("Null -> Null");
            value_type new_val           = value_type("");
            size_t     expected_old_size = 0;
            uint16_t   dead_space_before = optimal_node.dead_space();
            uint32_t   clines_before     = optimal_node.clines_capacity();
            size_t     returned_size     = optimal_node.update_value(bn, new_val);
            REQUIRE(returned_size == expected_old_size);
            REQUIRE(optimal_node.get_value(bn) == new_val);
            REQUIRE(optimal_node.dead_space() == dead_space_before);   // No change
            REQUIRE(optimal_node.clines_capacity() == clines_before);  // No change
            REQUIRE(optimal_node.is_optimal_layout());                 // Should remain optimal
         }
         SECTION("To Inline")
         {
            INFO("Null -> Inline");
            value_type new_val           = value_type("null_to_inline");
            size_t     expected_old_size = 0;
            uint16_t   dead_space_before = optimal_node.dead_space();
            uint32_t   clines_before     = optimal_node.clines_capacity();
            size_t     returned_size     = optimal_node.update_value(bn, new_val);
            REQUIRE(returned_size == expected_old_size);
            REQUIRE(optimal_node.get_value(bn) == new_val);
            REQUIRE(optimal_node.dead_space() == dead_space_before);
            REQUIRE(optimal_node.clines_capacity() == clines_before);
            REQUIRE(!optimal_node.is_optimal_layout());  // New alloc breaks optimal
         }
         SECTION("To Subtree")
         {
            INFO("Null -> Subtree (New Unique Cline)");
            value_type new_val           = value_type::make_subtree(addr_new_u3);
            size_t     expected_old_size = 0;
            uint16_t   dead_space_before = optimal_node.dead_space();
            uint32_t   clines_before     = optimal_node.clines_capacity();
            size_t     returned_size     = optimal_node.update_value(bn, new_val);
            REQUIRE(returned_size == expected_old_size);
            REQUIRE(optimal_node.get_value(bn) == new_val);
            REQUIRE(optimal_node.dead_space() == dead_space_before);
            REQUIRE(optimal_node.clines_capacity() == clines_before + 1);
            REQUIRE(!optimal_node.is_optimal_layout());  // Adding cline breaks optimal
         }
         SECTION("To ValueNode")
         {
            INFO("Null -> ValueNode (New Unique Cline)");
            value_type new_val           = value_type::make_value_node(addr_new_u4);
            size_t     expected_old_size = 0;
            uint16_t   dead_space_before = optimal_node.dead_space();
            uint32_t   clines_before     = optimal_node.clines_capacity();
            size_t     returned_size     = optimal_node.update_value(bn, new_val);
            REQUIRE(returned_size == expected_old_size);
            REQUIRE(optimal_node.get_value(bn) == new_val);
            REQUIRE(optimal_node.dead_space() == dead_space_before);
            REQUIRE(optimal_node.clines_capacity() == clines_before + 1);
            REQUIRE(!optimal_node.is_optimal_layout());  // Adding cline breaks optimal
         }
      }

      SECTION("Update From Inline")
      {
         branch_number bn          = optimal_node.get(key_start_inline);
         value_type    initial_val = optimal_node.get_value(bn);
         REQUIRE(initial_val.is_view());
         REQUIRE(!initial_val.view().empty());

         SECTION("To Null")
         {
            INFO("Inline -> Null");
            value_type new_val           = value_type("");
            size_t     expected_old_size = initial_val.view().size();
            uint16_t   dead_space_before = optimal_node.dead_space();
            uint32_t   clines_before     = optimal_node.clines_capacity();
            size_t     returned_size     = optimal_node.update_value(bn, new_val);
            REQUIRE(returned_size == expected_old_size);
            REQUIRE(optimal_node.get_value(bn) == new_val);
            REQUIRE(optimal_node.get_value(bn).view().empty());
            size_t expected_dead_increase = expected_old_size + 2 /*value_data header*/;
            REQUIRE(optimal_node.dead_space() == dead_space_before + expected_dead_increase);
            REQUIRE(optimal_node.clines_capacity() == clines_before);
            REQUIRE(!optimal_node.is_optimal_layout());  // Dead space breaks optimal
         }
         SECTION("To Inline (Same Size)")  // Added specific test for same size
         {
            INFO("Inline -> Inline (Same Size)");
            std::string s(initial_val.view().size(), 'X');  // Create string of same size
            value_type  new_val = value_type(s);
            REQUIRE(new_val.view().size() == initial_val.view().size());
            size_t   expected_old_size = initial_val.view().size();
            uint16_t dead_space_before = optimal_node.dead_space();
            uint32_t clines_before     = optimal_node.clines_capacity();
            size_t   returned_size     = optimal_node.update_value(bn, new_val);
            REQUIRE(returned_size == expected_old_size);
            REQUIRE(optimal_node.get_value(bn) == new_val);
            REQUIRE(optimal_node.dead_space() == dead_space_before);  // No change
            REQUIRE(optimal_node.clines_capacity() == clines_before);
            REQUIRE(optimal_node.is_optimal_layout());  // Should remain optimal
         }
         SECTION("To Inline (Smaller)")
         {
            INFO("Inline -> Inline (Smaller)");
            value_type new_val = value_type("small");
            REQUIRE(new_val.view().size() < initial_val.view().size());
            size_t   expected_old_size = initial_val.view().size();
            uint16_t dead_space_before = optimal_node.dead_space();
            uint32_t clines_before     = optimal_node.clines_capacity();
            size_t   returned_size     = optimal_node.update_value(bn, new_val);
            REQUIRE(returned_size == expected_old_size);
            REQUIRE(optimal_node.get_value(bn) == new_val);
            size_t expected_dead_increase = expected_old_size - new_val.view().size();
            REQUIRE(optimal_node.dead_space() == dead_space_before + expected_dead_increase);
            REQUIRE(optimal_node.clines_capacity() == clines_before);
            REQUIRE(!optimal_node.is_optimal_layout());  // Dead space breaks optimal
         }
         SECTION("To Inline (Larger)")
         {
            INFO("Inline -> Inline (Larger)");
            value_type new_val = value_type("much_larger_inline_value_than_before");
            REQUIRE(new_val.view().size() > initial_val.view().size());
            size_t   expected_old_size = initial_val.view().size();
            uint16_t dead_space_before = optimal_node.dead_space();
            uint32_t clines_before     = optimal_node.clines_capacity();
            size_t   returned_size     = optimal_node.update_value(bn, new_val);
            REQUIRE(returned_size == expected_old_size);
            REQUIRE(optimal_node.get_value(bn) == new_val);
            size_t expected_dead_increase = expected_old_size + 2 /*value_data header*/;
            REQUIRE(optimal_node.dead_space() == dead_space_before + expected_dead_increase);
            REQUIRE(optimal_node.clines_capacity() == clines_before);
            REQUIRE(!optimal_node.is_optimal_layout());  // New alloc + dead space breaks optimal
         }
         SECTION("To Subtree")
         {
            INFO("Inline -> Subtree (New Unique Cline)");
            value_type new_val = value_type::make_subtree(addr_new_u3);
            value_type initial_val =
                optimal_node.get_value(bn);  // Get initial value to get its size
            size_t   expected_old_size = initial_val.view().size();
            uint16_t dead_space_before = optimal_node.dead_space();
            uint32_t clines_before     = optimal_node.clines_capacity();
            size_t   returned_size     = optimal_node.update_value(bn, new_val);
            REQUIRE(returned_size == expected_old_size);
            REQUIRE(optimal_node.get_value(bn) == new_val);
            // Old inline value (data + header) becomes dead space
            size_t expected_dead_increase = expected_old_size + 2 /*value_data header*/;
            REQUIRE(optimal_node.dead_space() == dead_space_before + expected_dead_increase);
            REQUIRE(optimal_node.clines_capacity() == clines_before + 1);
            REQUIRE(!optimal_node.is_optimal_layout());  // Dead space + cline change breaks optimal
         }
         SECTION("To ValueNode")
         {
            INFO("Inline -> ValueNode (New Unique Cline)");
            value_type new_val = value_type::make_value_node(addr_new_u4);
            value_type initial_val =
                optimal_node.get_value(bn);  // Get initial value to get its size
            size_t   expected_old_size = initial_val.view().size();
            uint16_t dead_space_before = optimal_node.dead_space();
            uint32_t clines_before     = optimal_node.clines_capacity();
            size_t   returned_size     = optimal_node.update_value(bn, new_val);
            REQUIRE(returned_size == expected_old_size);
            REQUIRE(optimal_node.get_value(bn) == new_val);
            // Old inline value (data + header) becomes dead space
            size_t expected_dead_increase = expected_old_size + 2 /*value_data header*/;
            REQUIRE(optimal_node.dead_space() == dead_space_before + expected_dead_increase);
            REQUIRE(optimal_node.clines_capacity() == clines_before + 1);
            REQUIRE(!optimal_node.is_optimal_layout());  // Dead space + cline change breaks optimal
         }
      }

      SECTION("Update From Subtree")
      {
         ptr_address addr_existing_vn_u2   = addr_u2;   // Existing unique ValueNode cline 2k
         ptr_address addr_existing_sub_s1a = addr_s1a;  // Existing shared Subtree cline 3k

         SECTION("From Unique Cline (1k)")
         {
            branch_number bn = optimal_node.get(key_start_subtree);
            REQUIRE(optimal_node.get_value(bn).is_subtree());
            REQUIRE(optimal_node.get_value(bn).subtree_address() == addr_u1);

            SECTION("To Null")
            {
               INFO("Subtree(Unique) -> Null");
               value_type new_val           = value_type("");
               size_t     expected_old_size = sizeof(ptr_address);
               uint16_t   dead_space_before = optimal_node.dead_space();
               uint32_t   clines_before     = optimal_node.clines_capacity();
               size_t     returned_size     = optimal_node.update_value(bn, new_val);
               REQUIRE(returned_size == expected_old_size);
               REQUIRE(optimal_node.get_value(bn) == new_val);
               REQUIRE(optimal_node.dead_space() == dead_space_before);
               // Unique cline 1k should be freed, but it wasn't the last, so capacity doesn't change
               REQUIRE(optimal_node.clines_capacity() ==
                       clines_before);                      // Changed from clines_before - 1
               REQUIRE(!optimal_node.is_optimal_layout());  // Freeing cline breaks optimal
            }
            SECTION("To Inline")
            {
               INFO("Subtree(Unique) -> Inline");
               value_type new_val           = value_type("sub_to_inline");
               size_t     expected_old_size = sizeof(ptr_address);
               uint16_t   dead_space_before = optimal_node.dead_space();
               uint32_t   clines_before     = optimal_node.clines_capacity();
               size_t     returned_size     = optimal_node.update_value(bn, new_val);
               REQUIRE(returned_size == expected_old_size);
               REQUIRE(optimal_node.get_value(bn) == new_val);
               REQUIRE(optimal_node.dead_space() == dead_space_before);
               // Unique cline 1k should be freed, but it wasn't the last, so capacity doesn't change
               REQUIRE(optimal_node.clines_capacity() ==
                       clines_before);  // Changed from clines_before - 1
               REQUIRE(
                   !optimal_node.is_optimal_layout());  // New alloc + freeing cline breaks optimal
            }
            SECTION("To Subtree (New Unique)")
            {
               INFO("Subtree(Shared Cline) -> Subtree (New Unique)");
               value_type new_val           = value_type::make_subtree(addr_new_u3);
               size_t     expected_old_size = sizeof(ptr_address);
               uint16_t   dead_space_before = optimal_node.dead_space();
               uint32_t   clines_before     = optimal_node.clines_capacity();

               // the main focus of the test
               for (int i = 0; i < optimal_node.num_branches(); ++i)
               {
                  SAL_INFO("branch {} key: {} = {}", i, optimal_node.get_key(branch_number(i)),
                           optimal_node.get_value(branch_number(i)));
               }
               size_t returned_size = optimal_node.update_value(bn, new_val);

               REQUIRE(returned_size == expected_old_size);
               REQUIRE(optimal_node.get_value(bn) == new_val);
               REQUIRE(optimal_node.dead_space() == dead_space_before);
               // Cline 1k freed, existing cline u2 reused. Cline 2k wasn't last, capacity doesn't change.
               REQUIRE(optimal_node.clines_capacity() ==
                       clines_before + 1);  // Changed from clines_before + 1
               // REQUIRE(optimal_node.is_optimal_layout()); // Implementation currently breaks this
               REQUIRE(!optimal_node.is_optimal_layout());
            }
            SECTION("To ValueNode (Existing Unique)")
            {
               INFO("Subtree(Unique) -> ValueNode (Existing Unique)");
               value_type new_val           = value_type::make_value_node(addr_existing_vn_u2);
               size_t     expected_old_size = sizeof(ptr_address);
               uint16_t   dead_space_before = optimal_node.dead_space();
               uint32_t   clines_before     = optimal_node.clines_capacity();
               size_t     returned_size     = optimal_node.update_value(bn, new_val);
               REQUIRE(returned_size == expected_old_size);
               REQUIRE(optimal_node.get_value(bn) == new_val);
               REQUIRE(optimal_node.dead_space() == dead_space_before);
               // Cline 1k freed, cline 2k (existing) reused. Capacity doesn't change as cline 1 wasn't the last one.
               REQUIRE(optimal_node.clines_capacity() == clines_before);
               // REQUIRE(optimal_node.is_optimal_layout()); // Implementation currently breaks this
               REQUIRE(!optimal_node.is_optimal_layout());
            }
         }
         SECTION("From Shared Cline (3k)")
         {
            branch_number bn = optimal_node.get(key_start_shared_sub);
            REQUIRE(optimal_node.get_value(bn).is_subtree());
            ptr_address local_addr_s1a =
                optimal_node.get_value(bn).subtree_address();  // Use the actual retrieved address
            ptr_address local_addr_s1b =
                optimal_node.get_value(optimal_node.get("shared_valnode_3k")).value_address();
            REQUIRE(optimal_node.get_value(optimal_node.get("shared_valnode_3k")).value_address() ==
                    local_addr_s1b);

            SECTION("To Null")
            {
               INFO("Subtree(Shared) -> Null");
               value_type new_val           = value_type("");
               size_t     expected_old_size = sizeof(ptr_address);
               uint16_t   dead_space_before = optimal_node.dead_space();
               uint32_t   clines_before     = optimal_node.clines_capacity();
               size_t     returned_size     = optimal_node.update_value(bn, new_val);
               REQUIRE(returned_size == expected_old_size);
               REQUIRE(optimal_node.get_value(bn) == new_val);
               REQUIRE(optimal_node.dead_space() == dead_space_before);
               REQUIRE(optimal_node.clines_capacity() <= clines_before);
               REQUIRE(optimal_node.clines_capacity() >= clines_before - 1);
               REQUIRE(!optimal_node.is_optimal_layout());  // Changing value breaks optimal
            }
            SECTION("To Inline")
            {
               INFO("Subtree(Shared) -> Inline");
               value_type new_val           = value_type("shared_sub_to_inline");
               size_t     expected_old_size = sizeof(ptr_address);
               uint16_t   dead_space_before = optimal_node.dead_space();
               uint32_t   clines_before     = optimal_node.clines_capacity();
               size_t     returned_size     = optimal_node.update_value(bn, new_val);
               REQUIRE(returned_size == expected_old_size);
               REQUIRE(optimal_node.get_value(bn) == new_val);
               REQUIRE(optimal_node.dead_space() == dead_space_before);
               REQUIRE(optimal_node.clines_capacity() <= clines_before);
               REQUIRE(optimal_node.clines_capacity() >= clines_before - 1);
               REQUIRE(!optimal_node.is_optimal_layout());  // New alloc breaks optimal
            }
            SECTION("To Subtree (New Unique)")
            {
               INFO("Subtree(Shared) -> Subtree (New Unique)");
               value_type new_val           = value_type::make_subtree(addr_new_u3);
               size_t     expected_old_size = sizeof(ptr_address);
               uint16_t   dead_space_before = optimal_node.dead_space();
               uint32_t   clines_before     = optimal_node.clines_capacity();
               size_t     returned_size     = optimal_node.update_value(bn, new_val);
               REQUIRE(returned_size == expected_old_size);
               REQUIRE(optimal_node.get_value(bn) == new_val);
               REQUIRE(optimal_node.dead_space() == dead_space_before);
               REQUIRE(optimal_node.clines_capacity() <= clines_before + 1);
               REQUIRE(optimal_node.clines_capacity() >= clines_before);
               REQUIRE(!optimal_node.is_optimal_layout());  // Adding cline breaks optimal
            }
            SECTION("To ValueNode (Existing Shared 3k - Different Index)")
            {
               INFO("Subtree(Shared) -> ValueNode (Existing Shared 3k)");
               value_type new_val = value_type::make_value_node(local_addr_s1b);  // Use local copy
               size_t     expected_old_size = sizeof(ptr_address);
               uint16_t   dead_space_before = optimal_node.dead_space();
               uint32_t   clines_before     = optimal_node.clines_capacity();
               size_t     returned_size     = optimal_node.update_value(bn, new_val);
               REQUIRE(returned_size == expected_old_size);
               REQUIRE(optimal_node.get_value(bn) == new_val);
               REQUIRE(optimal_node.dead_space() == dead_space_before);
               REQUIRE(optimal_node.clines_capacity() == clines_before);
               REQUIRE(
                   optimal_node.get_value(optimal_node.get("shared_valnode_3k")).value_address() ==
                   local_addr_s1b);  // Check other key
               // REQUIRE(optimal_node.is_optimal_layout()); // Implementation currently breaks this
               REQUIRE(!optimal_node.is_optimal_layout());
            }
         }
      }

      SECTION("Update From ValueNode")
      {
         ptr_address addr_existing_sub_u1  = addr_u1;   // Existing unique Subtree cline 1k
         ptr_address addr_existing_sub_s1a = addr_s1a;  // Existing shared Subtree cline 3k

         SECTION("From Unique Cline (2k)")
         {
            branch_number bn = optimal_node.get(key_start_valnode);
            REQUIRE(optimal_node.get_value(bn).is_value_node());
            REQUIRE(optimal_node.get_value(bn).value_address() == addr_u2);

            SECTION("To Null")
            {
               INFO("ValueNode(Unique) -> Null");
               value_type new_val           = value_type("");
               size_t     expected_old_size = sizeof(ptr_address);
               uint16_t   dead_space_before = optimal_node.dead_space();
               uint32_t   clines_before     = optimal_node.clines_capacity();
               size_t     returned_size     = optimal_node.update_value(bn, new_val);
               REQUIRE(returned_size == expected_old_size);
               REQUIRE(optimal_node.get_value(bn) == new_val);
               REQUIRE(optimal_node.dead_space() == dead_space_before);
               // Unique cline 2k was the last one, freeing it should decrease capacity.
               REQUIRE(optimal_node.clines_capacity() == clines_before - 1);
               REQUIRE(!optimal_node.is_optimal_layout());  // Freeing cline breaks optimal
            }
            SECTION("To Inline")
            {
               INFO("ValueNode(Unique) -> Inline");
               value_type new_val           = value_type("val_to_inline");
               size_t     expected_old_size = sizeof(ptr_address);
               uint16_t   dead_space_before = optimal_node.dead_space();
               uint32_t   clines_before     = optimal_node.clines_capacity();
               size_t     returned_size     = optimal_node.update_value(bn, new_val);
               REQUIRE(returned_size == expected_old_size);
               REQUIRE(optimal_node.get_value(bn) == new_val);
               REQUIRE(optimal_node.dead_space() == dead_space_before);
               // Unique cline 2k was the last one, freeing it should decrease capacity.
               REQUIRE(optimal_node.clines_capacity() == clines_before - 1);
               REQUIRE(
                   !optimal_node.is_optimal_layout());  // New alloc + freeing cline breaks optimal
            }
            SECTION("To Subtree (Existing Unique)")
            {
               INFO("ValueNode(Unique) -> Subtree (Existing Unique)");
               value_type new_val           = value_type::make_subtree(addr_new_u3);
               size_t     expected_old_size = sizeof(ptr_address);
               uint16_t   dead_space_before = optimal_node.dead_space();
               uint32_t   clines_before     = optimal_node.clines_capacity();
               size_t     returned_size     = optimal_node.update_value(bn, new_val);
               REQUIRE(returned_size == expected_old_size);
               REQUIRE(optimal_node.get_value(bn) == new_val);
               REQUIRE(optimal_node.dead_space() == dead_space_before);
               REQUIRE(optimal_node.clines_capacity() == clines_before);
               // REQUIRE(optimal_node.is_optimal_layout()); // Implementation currently breaks this
               REQUIRE(!optimal_node.is_optimal_layout());
            }
            SECTION("To ValueNode (New Unique)")
            {
               INFO("ValueNode(Unique) -> ValueNode (New Unique)");
               value_type new_val           = value_type::make_value_node(addr_new_u4);
               size_t     expected_old_size = sizeof(ptr_address);
               uint16_t   dead_space_before = optimal_node.dead_space();
               uint32_t   clines_before     = optimal_node.clines_capacity();
               size_t     returned_size     = optimal_node.update_value(bn, new_val);
               REQUIRE(returned_size == expected_old_size);
               REQUIRE(optimal_node.get_value(bn) == new_val);
               REQUIRE(optimal_node.dead_space() == dead_space_before);
               REQUIRE(optimal_node.clines_capacity() == clines_before);
               // REQUIRE(optimal_node.is_optimal_layout()); // Implementation currently breaks this
               REQUIRE(!optimal_node.is_optimal_layout());
            }
         }
         SECTION("From Shared Cline (3k)")
         {
            branch_number bn = optimal_node.get("shared_valnode_3k");
            REQUIRE(optimal_node.get_value(bn).is_value_node());
            ptr_address local_addr_s1b_start =
                optimal_node.get_value(bn).value_address();  // Get actual starting address
            // Verify the other key using cline 3k is potentially still there

            SECTION("To Null")
            {
               INFO("ValueNode(Shared) -> Null");
               value_type new_val           = value_type("");
               size_t     expected_old_size = sizeof(ptr_address);
               uint16_t   dead_space_before = optimal_node.dead_space();
               uint32_t   clines_before     = optimal_node.clines_capacity();
               size_t     returned_size     = optimal_node.update_value(bn, new_val);
               REQUIRE(returned_size == expected_old_size);
               REQUIRE(optimal_node.get_value(bn) == new_val);
               REQUIRE(optimal_node.dead_space() == dead_space_before);
               REQUIRE(optimal_node.clines_capacity() <= clines_before);
               REQUIRE(optimal_node.clines_capacity() >= clines_before - 1);
               REQUIRE(!optimal_node.is_optimal_layout());  // Changing value breaks optimal
            }
            SECTION("To Inline")
            {
               INFO("ValueNode(Shared) -> Inline");
               value_type new_val           = value_type("shared_val_to_inline");
               size_t     expected_old_size = sizeof(ptr_address);
               uint16_t   dead_space_before = optimal_node.dead_space();
               uint32_t   clines_before     = optimal_node.clines_capacity();
               size_t     returned_size     = optimal_node.update_value(bn, new_val);
               REQUIRE(returned_size == expected_old_size);
               REQUIRE(optimal_node.get_value(bn) == new_val);
               REQUIRE(optimal_node.dead_space() == dead_space_before);
               REQUIRE(optimal_node.clines_capacity() <= clines_before);
               REQUIRE(optimal_node.clines_capacity() >= clines_before - 1);
               REQUIRE(!optimal_node.is_optimal_layout());  // New alloc breaks optimal
            }
            SECTION("To Subtree (New Unique)")
            {
               INFO("ValueNode(Shared) -> Subtree (New Unique)");
               value_type new_val           = value_type::make_subtree(addr_new_u3);
               size_t     expected_old_size = sizeof(ptr_address);
               uint16_t   dead_space_before = optimal_node.dead_space();
               uint32_t   clines_before     = optimal_node.clines_capacity();
               size_t     returned_size     = optimal_node.update_value(bn, new_val);
               REQUIRE(returned_size == expected_old_size);
               REQUIRE(optimal_node.get_value(bn) == new_val);
               REQUIRE(optimal_node.dead_space() == dead_space_before);
               REQUIRE(optimal_node.clines_capacity() <= clines_before + 1);
               REQUIRE(optimal_node.clines_capacity() >= clines_before);
               REQUIRE(!optimal_node.is_optimal_layout());  // Adding cline breaks optimal
            }
            SECTION("To ValueNode (New Shared 7k)")
            {
               INFO("ValueNode(Shared 3k) -> ValueNode (New Shared 7k)");
               value_type new_val           = value_type::make_value_node(addr_new_s3a);
               size_t     expected_old_size = sizeof(ptr_address);
               uint16_t   dead_space_before = optimal_node.dead_space();
               uint32_t   clines_before     = optimal_node.clines_capacity();
               size_t     returned_size     = optimal_node.update_value(bn, new_val);
               REQUIRE(returned_size == expected_old_size);
               REQUIRE(optimal_node.get_value(bn) == new_val);
               REQUIRE(optimal_node.dead_space() == dead_space_before);
               REQUIRE(optimal_node.clines_capacity() <= clines_before + 1);
               REQUIRE(optimal_node.clines_capacity() >= clines_before);
               // REQUIRE(optimal_node.is_optimal_layout()); // Implementation currently breaks this
               REQUIRE(!optimal_node.is_optimal_layout());
            }
         }
      }
   }
}
