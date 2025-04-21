#include <catch2/catch_all.hpp>
#include <psitri/node/inner.hpp>
#include <psitri/node/inner_node_util.hpp>  // Needed for split logic
#include <string>                           // Needed for prefix generation
#include <vector>                           // Needed for dynamic buffer

using namespace psitri;

TEST_CASE("ClineData")
{
   cline_data d;
   REQUIRE(d.is_null());
   d.set(ptr_address(10001));
   REQUIRE(d.ref() == 1);
   REQUIRE(d.base() == ptr_address(10000));
   d.inc_ref();
   REQUIRE(d.ref() == 2);
   REQUIRE(d.base() == ptr_address(10000));
   d.dec_ref();
   REQUIRE(d.ref() == 1);
   REQUIRE(d.base() == ptr_address(10000));
   d.dec_ref();
   REQUIRE(d.is_null());
}

TEST_CASE("InnerNode", "[inner_node]")
{
   SECTION("two branches, one cacheline")
   {
      branch_set bs;
      bs.set_front(ptr_address(10001));
      bs.push_back('M', ptr_address(10002));

      SAL_INFO("creat inner from branch set: {}", bs);

      uint8_t out_clines[8];
      auto    req_cline = find_clines(bs.addresses(), out_clines);
      REQUIRE(req_cline == 1);

      auto asize = inner_node::alloc_size(bs, req_cline, out_clines);
      REQUIRE(asize == 64);

      char buffer[asize];
      auto inode = new (buffer) inner_node(asize, ptr_address_seq(), bs, req_cline, out_clines);
      REQUIRE(inode->get_branch(branch_number(0)) == ptr_address(10001));
      REQUIRE(inode->get_branch(branch_number(1)) == ptr_address(10002));
      REQUIRE(inode->num_branches() == 2);

      //SECTION("replace branch 1 with two branches")
      {
         branch_set bs2;
         bs2.set_front(ptr_address(10003));
         bs2.push_back('P', ptr_address(10004));
         uint8_t cline_indices[8];
         auto    req_cline = inode->find_clines(branch_number(1), bs2, cline_indices);
         REQUIRE(req_cline == 1);
         op::replace_branch update_op{branch_number(1), bs2, req_cline, cline_indices};
         REQUIRE(inode->can_apply(update_op));
         inode->apply(update_op);
         REQUIRE(inode->get_branch(branch_number(0)) == ptr_address(10001));
         REQUIRE(inode->get_branch(branch_number(1)) == ptr_address(10003));
         REQUIRE(inode->get_branch(branch_number(2)) == ptr_address(10004));
         REQUIRE(inode->num_branches() == 3);
         REQUIRE(inode->num_divisions() == 2);
         REQUIRE(inode->divs() == key_view("MP"));
      }
      //SECTION("replace branch 1 with 3 branches")
      {
         branch_set bs2;
         bs2.set_front(ptr_address(10006));
         bs2.push_back('N', ptr_address(10007));
         bs2.push_back('O', ptr_address(10008));
         uint8_t cline_indices[8];
         auto    req_cline = inode->find_clines(branch_number(1), bs2, cline_indices);
         REQUIRE(req_cline == 1);
         op::replace_branch update_op{branch_number(1), bs2, req_cline, cline_indices};
         REQUIRE(inode->can_apply(update_op));
         inode->apply(update_op);
         REQUIRE(inode->get_branch(branch_number(0)) == ptr_address(10001));
         REQUIRE(inode->get_branch(branch_number(1)) == ptr_address(10006));
         REQUIRE(inode->get_branch(branch_number(2)) == ptr_address(10007));
         REQUIRE(inode->get_branch(branch_number(3)) == ptr_address(10008));
         REQUIRE(inode->get_branch(branch_number(4)) == ptr_address(10004));
         REQUIRE(inode->num_branches() == 5);
         REQUIRE(inode->num_divisions() == 4);
         REQUIRE(inode->divs() == key_view("MNOP"));
      }
      //SECTION("replace branch last branch with 3 branches")
      {
         branch_set bs2;
         bs2.set_front(ptr_address(10009));
         bs2.push_back('X', ptr_address(10010));
         bs2.push_back('Y', ptr_address(10011));
         uint8_t cline_indices[8];
         auto    req_cline = inode->find_clines(branch_number(4), bs2, cline_indices);
         REQUIRE(req_cline == 1);
         op::replace_branch update_op{branch_number(4), bs2, req_cline, cline_indices};
         REQUIRE(inode->can_apply(update_op));
         inode->apply(update_op);
         REQUIRE(inode->get_branch(branch_number(0)) == ptr_address(10001));
         REQUIRE(inode->get_branch(branch_number(1)) == ptr_address(10006));
         REQUIRE(inode->get_branch(branch_number(2)) == ptr_address(10007));
         REQUIRE(inode->get_branch(branch_number(3)) == ptr_address(10008));
         REQUIRE(inode->get_branch(branch_number(4)) == ptr_address(10009));
         REQUIRE(inode->get_branch(branch_number(5)) == ptr_address(10010));
         REQUIRE(inode->get_branch(branch_number(6)) == ptr_address(10011));
         REQUIRE(inode->num_branches() == 7);
         REQUIRE(inode->num_divisions() == 6);
         REQUIRE(inode->divs() == key_view("MNOPXY"));
         SAL_WARN(" free space: {}", inode->free_space());

         // SECTION: Test lower_bound
         {
            SAL_INFO("Testing lower_bound with dividers: MNOPXY");
            // Keys before first divider ('M')
            REQUIRE(inode->lower_bound(key_view("A")) == branch_number(0));
            REQUIRE(inode->lower_bound(key_view("L")) == branch_number(0));

            // Keys matching dividers
            REQUIRE(inode->lower_bound(key_view("M")) ==
                    branch_number(1));  // Branch after 'M' divider
            REQUIRE(inode->lower_bound(key_view("N")) ==
                    branch_number(2));  // Branch after 'N' divider
            REQUIRE(inode->lower_bound(key_view("O")) ==
                    branch_number(3));  // Branch after 'O' divider
            REQUIRE(inode->lower_bound(key_view("P")) ==
                    branch_number(4));  // Branch after 'P' divider
            REQUIRE(inode->lower_bound(key_view("X")) ==
                    branch_number(5));  // Branch after 'X' divider
            REQUIRE(inode->lower_bound(key_view("Y")) ==
                    branch_number(6));  // Branch after 'Y' divider

            // Lowercase keys are > 'Y'
            REQUIRE(inode->lower_bound(key_view("m")) == branch_number(6));
            REQUIRE(inode->lower_bound(key_view("n")) == branch_number(6));
            REQUIRE(inode->lower_bound(key_view("o")) == branch_number(6));
            REQUIRE(inode->lower_bound(key_view("p")) == branch_number(6));
            REQUIRE(inode->lower_bound(key_view("x")) == branch_number(6));

            // Keys after last divider ('Y')
            REQUIRE(inode->lower_bound(key_view("y")) == branch_number(6));
            REQUIRE(inode->lower_bound(key_view("Z")) == branch_number(6));
            REQUIRE(inode->lower_bound(key_view("z")) == branch_number(6));
         }
      }
      // REPLACE FIRST BRANCH WITH 6 BRANCHES
      {
         branch_set bs2;
         bs2.set_front(ptr_address(10017));
         bs2.push_back('0', ptr_address(10012));
         bs2.push_back('1', ptr_address(10013));
         bs2.push_back('2', ptr_address(10014));
         bs2.push_back('3', ptr_address(10015));
         bs2.push_back('4', ptr_address(10016));

         uint8_t cline_indices[8];
         auto    req_cline = inode->find_clines(branch_number(0), bs2, cline_indices);
         REQUIRE(req_cline == 2);
         op::replace_branch update_op{branch_number(0), bs2, req_cline, cline_indices};
         REQUIRE(inode->can_apply(update_op));
         inode->apply(update_op);
         REQUIRE(inode->divs() == key_view("01234MNOPXY"));
         REQUIRE(inode->get_branch(branch_number(0)) == ptr_address(10017));
         REQUIRE(inode->get_branch(branch_number(1)) == ptr_address(10012));
         REQUIRE(inode->get_branch(branch_number(2)) == ptr_address(10013));
         REQUIRE(inode->get_branch(branch_number(3)) == ptr_address(10014));
         REQUIRE(inode->get_branch(branch_number(4)) == ptr_address(10015));
         REQUIRE(inode->get_branch(branch_number(5)) == ptr_address(10016));
         REQUIRE(inode->get_branch(branch_number(6)) == ptr_address(10006));
         REQUIRE(inode->get_branch(branch_number(7)) == ptr_address(10007));
         REQUIRE(inode->get_branch(branch_number(8)) == ptr_address(10008));
         REQUIRE(inode->get_branch(branch_number(9)) == ptr_address(10009));
         REQUIRE(inode->get_branch(branch_number(10)) == ptr_address(10010));
         REQUIRE(inode->get_branch(branch_number(11)) == ptr_address(10011));
         REQUIRE(inode->num_branches() == 12);
         REQUIRE(inode->num_divisions() == 11);
         SAL_WARN(" free space: {}", inode->free_space());
      }
      // REPLACE LAST BRANCH WITH 6 BRANCHES, cloning to a larger size
      {
         branch_set bs2;
         bs2.set_front(ptr_address(20011));
         bs2.push_back('a', ptr_address(20012));
         bs2.push_back('b', ptr_address(20013));
         bs2.push_back('c', ptr_address(20014));
         bs2.push_back('d', ptr_address(20015));
         bs2.push_back('e', ptr_address(20010));

         uint8_t cline_indices[8];
         auto    req_cline = inode->find_clines(branch_number(12), bs2, cline_indices);
         REQUIRE(req_cline == 3);

         op::replace_branch update_op{branch_number(11), bs2, req_cline, cline_indices};

         // not enough space to add these new branches..
         REQUIRE(not inode->can_apply(update_op));

         // new buffer with more space
         auto asize2 = inner_node::alloc_size(inode, update_op);
         assert(asize2 == 128);
         char buffer[asize2];
         auto inode2 = new (buffer) inner_node(128, ptr_address_seq(), inode, update_op);

         REQUIRE(inode2->divs() == key_view("01234MNOPXYabcde"));
         REQUIRE(inode2->get_branch(branch_number(0)) == ptr_address(10017));
         REQUIRE(inode2->get_branch(branch_number(1)) == ptr_address(10012));
         REQUIRE(inode2->get_branch(branch_number(2)) == ptr_address(10013));
         REQUIRE(inode2->get_branch(branch_number(3)) == ptr_address(10014));
         REQUIRE(inode2->get_branch(branch_number(4)) == ptr_address(10015));
         REQUIRE(inode2->get_branch(branch_number(5)) == ptr_address(10016));
         REQUIRE(inode2->get_branch(branch_number(6)) == ptr_address(10006));
         REQUIRE(inode2->get_branch(branch_number(7)) == ptr_address(10007));
         REQUIRE(inode2->get_branch(branch_number(8)) == ptr_address(10008));
         REQUIRE(inode2->get_branch(branch_number(9)) == ptr_address(10009));
         REQUIRE(inode2->get_branch(branch_number(10)) == ptr_address(10010));
         REQUIRE(inode2->get_branch(branch_number(11)) == ptr_address(20011));
         REQUIRE(inode2->get_branch(branch_number(12)) == ptr_address(20012));
         REQUIRE(inode2->get_branch(branch_number(13)) == ptr_address(20013));
         REQUIRE(inode2->get_branch(branch_number(14)) == ptr_address(20014));
         REQUIRE(inode2->get_branch(branch_number(15)) == ptr_address(20015));
         REQUIRE(inode2->get_branch(branch_number(16)) == ptr_address(20010));
         REQUIRE(inode2->num_branches() == 17);
         REQUIRE(inode2->num_divisions() == 16);
         SAL_WARN(" free space: {}", inode2->free_space());
      }
   }
   SECTION("two branches, two cacheline")
   {
      branch_set bs;
      bs.set_front(ptr_address(10001));
      bs.push_back('m', ptr_address(20002));

      uint8_t out_clines[8];
      auto    req_cline = find_clines(bs.addresses(), out_clines);
      REQUIRE(req_cline == 2);

      auto asize = inner_node::alloc_size(bs, req_cline, out_clines);
      REQUIRE(asize == 64);

      char buffer[asize];
      auto inode = new (buffer) inner_node(asize, ptr_address_seq(), bs, req_cline, out_clines);
      REQUIRE(inode->get_branch(branch_number(0)) == ptr_address(10001));
      REQUIRE(inode->get_branch(branch_number(1)) == ptr_address(20002));
   }
}

TEST_CASE("InnerPrefixNode", "[inner_prefix_node]")
{
   // Helper function to create a prefix string of a given size
   auto create_prefix = [](size_t size) -> std::string
   {
      if (size == 0)
         return "";
      std::string prefix(size, 'P');  // Fill with 'P' for simplicity
      // Make first and last chars unique if size > 1
      if (size > 0)
         prefix[0] = 'A';
      if (size > 1)
         prefix[size - 1] = 'Z';
      return prefix;
   };

   // Test with various prefix sizes
   for (size_t prefix_size : {0, 10, 100, 512, 1024})
   {
      std::string prefix_str = create_prefix(prefix_size);
      key_view    prefix_kv(prefix_str);

      SECTION("Prefix size: " + std::to_string(prefix_size))
      {
         // --- Mirrored tests from InnerNode ---

         SECTION("two branches, one cacheline")
         {
            branch_set bs;
            bs.set_front(ptr_address(10001));
            bs.push_back('M', ptr_address(10002));

            uint8_t out_clines[8];
            auto    req_cline = find_clines(bs.addresses(), out_clines);
            REQUIRE(req_cline == 1);

            auto asize = inner_prefix_node::alloc_size(prefix_kv, bs, req_cline, out_clines);
            REQUIRE(asize >= sizeof(inner_prefix_node) + prefix_size + 2 * bs.count() - 1 +
                                 req_cline * sizeof(cline_data));
            // Ensure allocation is cacheline aligned
            REQUIRE(asize % 64 == 0);

            char buffer[asize];
            auto inode = new (buffer)
                inner_prefix_node(asize, ptr_address_seq(), prefix_kv, bs, req_cline, out_clines);

            REQUIRE(inode->type() == node_type::inner_prefix);
            REQUIRE(inode->prefix_len() == prefix_size);
            REQUIRE(inode->prefix() == prefix_kv);
            REQUIRE(inode->get_branch(branch_number(0)) == ptr_address(10001));
            REQUIRE(inode->get_branch(branch_number(1)) == ptr_address(10002));
            REQUIRE(inode->num_branches() == 2);

            // --- Replace branch tests adapted for inner_prefix_node ---
            //SECTION("replace branch 1 with two branches")
            {
               branch_set bs2;
               bs2.set_front(ptr_address(10003));
               bs2.push_back('P', ptr_address(10004));
               uint8_t cline_indices[8];
               auto    req_cline = inode->find_clines(branch_number(1), bs2, cline_indices);
               REQUIRE(req_cline == 1);
               op::replace_branch update_op{branch_number(1), bs2, req_cline, cline_indices};

               // Check if reallocation is needed
               if (!inode->can_apply(update_op))
               {
                  auto asize2 = inner_prefix_node::alloc_size(prefix_kv, inode, update_op);
                  char buffer2[asize2];
                  // Manually call destructor on the object in the old buffer if needed
                  // inode->~inner_prefix_node(); // Consider if resources need explicit release
                  auto inode2 = new (buffer2)
                      inner_prefix_node(asize2, ptr_address_seq(), prefix_kv, inode, update_op);
                  // Update inode pointer for subsequent tests within this scope
                  inode = inode2;
                  // buffer = buffer2; // REMOVED: Cannot assign stack arrays

                  REQUIRE(inode->prefix() == prefix_kv);  // Verify prefix preserved after clone
                  REQUIRE(inode->get_branch(branch_number(0)) == ptr_address(10001));
                  REQUIRE(inode->get_branch(branch_number(1)) == ptr_address(10003));
                  REQUIRE(inode->get_branch(branch_number(2)) == ptr_address(10004));
                  REQUIRE(inode->num_branches() == 3);
                  REQUIRE(inode->num_divisions() == 2);
                  REQUIRE(inode->divs() == key_view("MP"));
               }
               else
               {
                  inode->apply(update_op);
                  REQUIRE(inode->prefix() ==
                          prefix_kv);  // Verify prefix preserved after in-place update
                  REQUIRE(inode->get_branch(branch_number(0)) == ptr_address(10001));
                  REQUIRE(inode->get_branch(branch_number(1)) == ptr_address(10003));
                  REQUIRE(inode->get_branch(branch_number(2)) == ptr_address(10004));
                  REQUIRE(inode->num_branches() == 3);
                  REQUIRE(inode->num_divisions() == 2);
                  REQUIRE(inode->divs() == key_view("MP"));
               }
            }
            // Add more replacement tests here, adapting allocation/cloning as needed...
            // (Example: replace branch 1 with 3 branches, replace last branch, replace first branch)
            // Remember to check prefix preservation and handle potential reallocations.

            //SECTION("replace branch 1 with 3 branches") - Adapted Example
            {
               branch_set bs2;
               bs2.set_front(ptr_address(10006));
               bs2.push_back('N', ptr_address(10007));
               bs2.push_back('O', ptr_address(10008));
               uint8_t cline_indices[8];
               // Use current inode state (might have 3 branches now)
               branch_number target_bn = branch_number(1);
               auto          req_cline = inode->find_clines(target_bn, bs2, cline_indices);
               REQUIRE(req_cline != insufficient_clines);  // Ensure clines are found

               op::replace_branch update_op{target_bn, bs2, req_cline, cline_indices};

               if (!inode->can_apply(update_op))
               {
                  auto asize2 = inner_prefix_node::alloc_size(prefix_kv, inode, update_op);
                  char buffer2[asize2];
                  // Manually call destructor on the object in the old buffer if needed
                  // inode->~inner_prefix_node(); // Consider if resources need explicit release
                  auto inode2 = new (buffer2)
                      inner_prefix_node(asize2, ptr_address_seq(), prefix_kv, inode, update_op);
                  inode = inode2;
                  // buffer = buffer2; // REMOVED
               }
               else
               {
                  inode->apply(update_op);
               }

               REQUIRE(inode->prefix() == prefix_kv);
               REQUIRE(inode->get_branch(branch_number(0)) == ptr_address(10001));
               REQUIRE(inode->get_branch(branch_number(1)) == ptr_address(10006));
               REQUIRE(inode->get_branch(branch_number(2)) == ptr_address(10007));
               REQUIRE(inode->get_branch(branch_number(3)) == ptr_address(10008));
               REQUIRE(inode->get_branch(branch_number(4)) ==
                       ptr_address(10004));  // From previous test
               REQUIRE(inode->num_branches() == 5);
               REQUIRE(inode->num_divisions() == 4);
               REQUIRE(inode->divs() == key_view("MNOP"));
            }

            // If reallocation happened, inode now points into buffer2. buffer holds the old node.
            // Stack unwinding handles buffer and buffer2 lifetimes.
            // Best practice would be std::vector<char> or unique_ptr to manage buffer lifetimes.

         }  // end section two branches one cacheline

         SECTION("two branches, two cacheline")
         {
            branch_set bs;
            bs.set_front(ptr_address(10001));       // Cline 1000x
            bs.push_back('m', ptr_address(20002));  // Cline 2000x

            uint8_t out_clines[8];
            auto    req_cline = find_clines(bs.addresses(), out_clines);
            REQUIRE(req_cline == 2);  // Expecting two clines

            auto asize = inner_prefix_node::alloc_size(prefix_kv, bs, req_cline, out_clines);
            REQUIRE(asize >= sizeof(inner_prefix_node) + prefix_size + 2 * bs.count() - 1 +
                                 req_cline * sizeof(cline_data));
            REQUIRE(asize % 64 == 0);

            char buffer[asize];
            auto inode = new (buffer)
                inner_prefix_node(asize, ptr_address_seq(), prefix_kv, bs, req_cline, out_clines);

            REQUIRE(inode->type() == node_type::inner_prefix);
            REQUIRE(inode->prefix_len() == prefix_size);
            REQUIRE(inode->prefix() == prefix_kv);
            REQUIRE(inode->num_clines() == 2);  // Check number of clines stored
            REQUIRE(inode->get_branch(branch_number(0)) == ptr_address(10001));
            REQUIRE(inode->get_branch(branch_number(1)) == ptr_address(20002));
            REQUIRE(inode->num_branches() == 2);
            REQUIRE(inode->num_divisions() == 1);
            REQUIRE(inode->divs() == key_view("m"));
            SAL_WARN("divs: {}", inode->divs());

            // Add lower_bound tests considering the prefix
            if (prefix_size > 0)
            {
               // Key shorter than prefix
               REQUIRE(inode->lower_bound(prefix_kv.substr(0, prefix_kv.length() - 1)) ==
                       branch_number(0));
               // Key equal to prefix
               REQUIRE(inode->lower_bound(prefix_kv) == branch_number(0));
               // Key longer, starting with prefix, check based on first char after prefix
               std::string key_after_prefix_a = prefix_str + "a";  // Before 'm'
               REQUIRE(inode->lower_bound(key_after_prefix_a) == branch_number(0));
               std::string key_after_prefix_m = prefix_str + "m";  // Equal to divider 'm'
               REQUIRE(inode->lower_bound(key_after_prefix_m) == branch_number(1));
               std::string key_after_prefix_z = prefix_str + "z";  // After 'm'
               REQUIRE(inode->lower_bound(key_after_prefix_z) == branch_number(1));
            }
            else
            {
               // No prefix, behaves like inner_node
               REQUIRE(inode->lower_bound("a") == branch_number(0));
               REQUIRE(inode->lower_bound("m") == branch_number(1));
               REQUIRE(inode->lower_bound("z") == branch_number(1));
            }
         }  // end section two branches two cacheline

      }  // end section Prefix size
   }  // end loop over prefix sizes
}  // end TEST_CASE InnerPrefixNode

TEST_CASE("InnerNodeSplit", "[inner_node]")
{
   // Use vector for dynamic buffer management
   std::vector<char> buffer;
   inner_node*       inode_ptr = nullptr;

   // Addresses for branches A-Z, a-z
   constexpr uint32_t base_addr1_val = 10000;
   constexpr uint32_t base_addr2_val = 20000;
   // Placeholder address for the very first branch (before 'A')
   const ptr_address placeholder_addr = ptr_address(9999);

   auto get_addr = [&](char c) -> ptr_address
   {
      if (c >= 'A' && c <= 'Z')
         return ptr_address(base_addr1_val + (c - 'A'));
      else  // a-z (placeholder is handled separately)
         return ptr_address(base_addr2_val + (c - 'a'));
   };

   // Characters that will become the DIVIDERS
   const std::string divider_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
   REQUIRE(divider_chars.length() == 52);

   // 1. Start with Placeholder and 'A'. Divider is 'A'.
   {
      branch_set bs_init;
      bs_init.set_front(placeholder_addr);    // Zeroth branch
      bs_init.push_back('A', get_addr('A'));  // First divider 'A', first real branch 'A'

      uint8_t clines_init[16];
      // Need clines for placeholder_addr and get_addr('A')
      auto req_cline_init = find_clines(bs_init.addresses(), clines_init);
      REQUIRE(req_cline_init > 0);
      REQUIRE(req_cline_init <= 16);

      auto asize_init = inner_node::alloc_size(bs_init, req_cline_init, clines_init);
      buffer.resize(asize_init);
      inode_ptr = new (buffer.data())
          inner_node(asize_init, ptr_address_seq(), bs_init, req_cline_init, clines_init);

      REQUIRE(inode_ptr->num_branches() == 2);
      REQUIRE(inode_ptr->num_divisions() == 1);
      REQUIRE(inode_ptr->divs() == key_view("A"));
      REQUIRE(inode_ptr->get_branch(branch_number(0)) == placeholder_addr);
      REQUIRE(inode_ptr->get_branch(branch_number(1)) == get_addr('A'));
   }

   // 2. Incrementally add branches B-Z, a-z, using A-Z, a-z as dividers
   for (size_t i = 1; i < divider_chars.length(); ++i)  // Start from B (index 1)
   {
      char prev_branch_char = divider_chars[i - 1];  // e.g., 'A' when i=1
      char new_divider_char = divider_chars[i];      // e.g., 'B' when i=1
      char new_branch_char  = new_divider_char;      // Branch char is same as divider

      // The branch we replace is the one associated with the previous divider char
      // which is at index `i` (since branch 0 is placeholder)
      branch_number target_branch_bn   = branch_number(i);
      ptr_address   target_branch_addr = inode_ptr->get_branch(target_branch_bn);

      REQUIRE(target_branch_addr ==
              get_addr(prev_branch_char));  // Ensure we're replacing the correct branch

      branch_set bs_replace;
      bs_replace.set_front(target_branch_addr);  // Keep the previous branch address
      bs_replace.push_back(new_divider_char, get_addr(new_branch_char));  // Add the new branch

      uint8_t clines_replace[16];
      auto req_cline_replace = inode_ptr->find_clines(target_branch_bn, bs_replace, clines_replace);

      REQUIRE(req_cline_replace != insufficient_clines);  // Must be able to find clines
      REQUIRE(req_cline_replace <= 16);

      op::replace_branch update_op{target_branch_bn, bs_replace, req_cline_replace, clines_replace};

      if (!inode_ptr->can_apply(update_op))
      {
         // Reallocate and clone
         auto new_asize = inner_node::alloc_size(inode_ptr, update_op);
         SAL_ERROR("reallocating from {} to {} bytes", buffer.size(), new_asize);
         REQUIRE(new_asize >= buffer.size());  // Expecting growth or same size

         std::vector<char> new_buffer(new_asize);
         inner_node*       new_inode_ptr =
             new (new_buffer.data()) inner_node(new_asize, ptr_address_seq(), inode_ptr, update_op);

         // Swap buffers and update pointer
         buffer.swap(new_buffer);
         inode_ptr = new_inode_ptr;
      }
      else
      {
         // Replace in-place
         inode_ptr->apply(update_op);
      }

      // After adding divider `i`, we have `i+2` branches (placeholder + 0..i)
      REQUIRE(inode_ptr->num_branches() == (i + 2));
      REQUIRE(inode_ptr->get_branch(branch_number(i + 1)) == get_addr(new_branch_char));
      REQUIRE(inode_ptr->divs().length() == (i + 1));
      REQUIRE(inode_ptr->divs().back() == new_divider_char);
      REQUIRE(inode_ptr->get_branch(branch_number(0)) == placeholder_addr);
   }

   // 3. Final Assertions
   REQUIRE(inode_ptr->num_branches() == 53);               // Placeholder + 52 real branches
   REQUIRE(inode_ptr->num_divisions() == 52);              // Dividers A-Z, a-z
   REQUIRE(inode_ptr->divs() == key_view(divider_chars));  // Check dividers are correct

   // Check branches
   REQUIRE(inode_ptr->get_branch(branch_number(0)) == placeholder_addr);  // Placeholder
   REQUIRE(inode_ptr->get_branch(branch_number(1)) == get_addr('A'));
   REQUIRE(inode_ptr->get_branch(branch_number(2)) == get_addr('B'));
   REQUIRE(inode_ptr->get_branch(branch_number(26)) == get_addr('Z'));
   REQUIRE(inode_ptr->get_branch(branch_number(27)) == get_addr('a'));
   REQUIRE(inode_ptr->get_branch(branch_number(52)) == get_addr('z'));  // Last real branch

   // Check some intermediate branches based on dividers
   REQUIRE(inode_ptr->get_branch(branch_number(13)) ==
           get_addr('M'));  // Divider 'M' leads to branch M (index 13)
   REQUIRE(inode_ptr->get_branch(branch_number(36)) ==
           get_addr('j'));  // Divider 'j' leads to branch j (index 36)

   // Ensure invariants hold
   REQUIRE(inode_ptr->validate_invariants());

   // --- Split the node ---

   // 1. Define subranges
   subrange range1 = {branch_number(0), branch_number(27)};   // Branches 0-26 (Placeholder, A-Z)
   subrange range2 = {branch_number(27), branch_number(53)};  // Branches 27-52 (a-z)
   REQUIRE((*range1.end - *range1.begin) == 27);
   REQUIRE((*range2.end - *range2.begin) == 26);

   // 2. Calculate frequency tables for each range (using public const branches())
   auto ftab1 = create_cline_freq_table(inode_ptr->const_branches() + *range1.begin,
                                        inode_ptr->const_branches() + *range1.end);
   auto ftab2 = create_cline_freq_table(inode_ptr->const_branches() + *range2.begin,
                                        inode_ptr->const_branches() + *range2.end);

   // 3. Calculate allocation sizes
   auto asize1 = inner_node::alloc_size(inode_ptr, range1, ftab1);
   auto asize2 = inner_node::alloc_size(inode_ptr, range2, ftab2);
   REQUIRE(asize1 > 0);
   REQUIRE(asize2 > 0);

   // 4. Allocate buffers and construct new nodes
   std::vector<char> buffer1(asize1);
   std::vector<char> buffer2(asize2);

   inner_node* node1 =
       new (buffer1.data()) inner_node(asize1, ptr_address_seq(), inode_ptr, range1, ftab1);
   inner_node* node2 =
       new (buffer2.data()) inner_node(asize2, ptr_address_seq(), inode_ptr, range2, ftab2);

   // 5. Assertions for Node 1 (Placeholder, A-Z)
   REQUIRE(node1->num_branches() == 27);
   REQUIRE(node1->num_divisions() == 26);
   const std::string node1_divs = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
   REQUIRE(node1->divs() == key_view(node1_divs));
   REQUIRE(node1->get_branch(branch_number(0)) == placeholder_addr);  // Check placeholder
   for (uint16_t i = 1; i <= 26; ++i)  // Branches 1 to 26 should be A to Z
   {
      char expected_char = 'A' + (i - 1);
      REQUIRE(node1->get_branch(branch_number(i)) == get_addr(expected_char));
   }
   REQUIRE(node1->validate_invariants());

   // 6. Assertions for Node 2 (a-z)
   REQUIRE(node2->num_branches() == 26);
   REQUIRE(node2->num_divisions() == 25);
   // the div for "a" is everything before "b" and thus "a" is not included
   const std::string node2_divs = "bcdefghijklmnopqrstuvwxyz";
   REQUIRE(node2->divs() == key_view(node2_divs));
   for (uint16_t i = 0; i <= 25; ++i)  // Branches 0 to 25 should be a to z
   {
      // Note: Branches in node2 are indexed starting from 0
      // Original branch index = 27 + i
      char expected_char = 'a' + i;
      REQUIRE(node2->get_branch(branch_number(i)) == get_addr(expected_char));
   }
   REQUIRE(node2->validate_invariants());

   SAL_INFO(" left node size: {} clines: {} branches: {} free: {}", node1->size(),
            node1->num_clines(), (int)node1->num_branches(), node1->free_space());
   SAL_INFO(" right node size: {} clines: {} branches: {} free: {}", node2->size(),
            node2->num_clines(), (int)node2->num_branches(), node2->free_space());
}