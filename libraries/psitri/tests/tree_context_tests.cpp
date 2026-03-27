#include <catch2/catch_all.hpp>
#include <fstream>
#include <psitri/cursor.hpp>
#include <psitri/tree_ops.hpp>
#include <random>
#include <sal/sal.hpp>
#include "sal/numbers.hpp"
using namespace psitri;

constexpr int SCALE = 1;

int64_t rand64()
{
   thread_local static std::mt19937 gen(rand());
   return (uint64_t(gen()) << 32) | gen();
}

std::vector<std::string> load_words(uint32_t limit = -1)
{
   std::vector<std::string> words;
   words.reserve(300000);
   std::ifstream file("/usr/share/dict/words");
   std::string   word;
   while (file >> word && words.size() < limit)
   {
      words.push_back(word);
   }
   return words;
}

TEST_CASE("cursor-prev-next", "[cursor]")
{
   sal::set_current_thread_name("main");
   std::filesystem::remove_all("db");
   sal::register_type_vtable<leaf_node>();
   sal::register_type_vtable<inner_prefix_node>();
   sal::register_type_vtable<inner_node>();
   sal::register_type_vtable<value_node>();

   sal::allocator salloc("db", sal::runtime_config());
   auto           ses  = salloc.get_session();
   auto           root = ses->get_root<>(sal::root_object_number(0));

   SAL_WARN("root: {} {}", &root, root.address());
   tree_context ctx(root);

   auto words   = load_words();
   auto start   = std::chrono::high_resolution_clock::now();
   int  inspect = words.size();  //5333;
   for (int i = 0; i < words.size(); i++)
   {
      //   if (i == words.size() - 1)
      //     SAL_INFO("inserting word[{}]: {}", i, words[i]);
      ctx.insert(to_key_view(words[i]), to_value_view(words[i]));
   }
   auto end = std::chrono::high_resolution_clock::now();
   SAL_ERROR("inserted {:L} words in {:L} ms, {:L} words/sec", words.size(),
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
             (words.size() * 1000.0) /
                 std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());

   std::sort(words.begin(), words.end());
   auto cur = cursor(ctx.get_root());

   start = std::chrono::high_resolution_clock::now();
   for (int i = 0; i < words.size(); ++i)
   {
      //   SAL_WARN("lower bound {}", words[i]);
      cur.lower_bound(to_key_view(words[i]));
      auto itr = std::lower_bound(words.begin(), words.end(), words[i]);
      REQUIRE(not cur.is_end());
      if (cur.key() != key_view(*itr))
         ctx.print();
      assert(cur.key() == key_view(*itr));
      REQUIRE(cur.key() == key_view(*itr));
   }
   end = std::chrono::high_resolution_clock::now();
   SAL_WARN("lower bound: {:L} ms {:L} words/sec",
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
            (words.size() * 1000.0) /
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());

   uint32_t count = 0;
   start          = std::chrono::high_resolution_clock::now();
   cur.seek_rend();
   int i = 0;
   while (cur.next())
   {
      REQUIRE(cur.key() == key_view(words[i++]));
      count++;
   }
   end = std::chrono::high_resolution_clock::now();
   SAL_WARN("count: {} {:L} ms {:L} words/sec", count,
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
            uint64_t((words.size() * 1000.0) /
                     std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()));
   REQUIRE(count == words.size());
   REQUIRE(cur.is_end());
   count = 0;
   start = std::chrono::high_resolution_clock::now();
   while (cur.prev())
   {
      count++;
   }
   end = std::chrono::high_resolution_clock::now();
   SAL_WARN("count: {} {:L} ms {:L} words/sec", count,
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
            uint64_t((words.size() * 1000.0) /
                     std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()));
   REQUIRE(count == words.size());
   REQUIRE(cur.is_rend());

   SAL_WARN("lower bound hello");
   cur.lower_bound(to_key_view("hello"));
   REQUIRE(not cur.is_end());
   REQUIRE(cur.key() == key_view("hello"));

   auto itr = std::lower_bound(words.begin(), words.end(), "boyz");
   SAL_WARN("lower bound boyz");
   cur.lower_bound(to_key_view("boyz"));
   REQUIRE(not cur.is_end());
   REQUIRE(cur.key() == key_view(*itr));
   auto itr3 = std::lower_bound(words.begin(), words.end(), "Ancerata");
   SAL_WARN("lower bound Ancerata");
   cur.lower_bound(to_key_view("Ancerata"));
   REQUIRE(not cur.is_end());
   REQUIRE(cur.key() == key_view(*itr3));
}

TEST_CASE("cursor-lowerbound", "[cursor]")
{
   sal::set_current_thread_name("main");
   std::filesystem::remove_all("db");
   sal::allocator salloc("db", sal::runtime_config());
   auto           ses = salloc.get_session();
}

TEST_CASE("tree_context-insert-remove", "[tree_context][remove]")
{
   sal::set_current_thread_name("main");
   std::filesystem::remove_all("db");
   sal::register_type_vtable<leaf_node>();
   sal::register_type_vtable<inner_prefix_node>();
   sal::register_type_vtable<inner_node>();
   sal::register_type_vtable<value_node>();

   sal::allocator salloc("db", sal::runtime_config());
   auto           ses  = salloc.get_session();
   auto           root = ses->get_root<>(sal::root_object_number(0));

   tree_context ctx(root);

   auto words = load_words();
   SAL_INFO("loaded {} words for insert-remove test", words.size());

   // Insert all words
   auto start = std::chrono::high_resolution_clock::now();
   for (const auto& word : words)
   {
      ctx.insert(to_key_view(word), to_value_view(word));
   }
   auto end = std::chrono::high_resolution_clock::now();
   SAL_ERROR("inserted {:L} words in {:L} ms, {:L} words/sec", words.size(),
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
             (words.size() * 1000.0) /
                 std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());

   // Validate descendant counts after insert
   ctx.validate();

   // Sort words for ordered removal
   std::sort(words.begin(), words.end());

   // Remove all words in order
   start             = std::chrono::high_resolution_clock::now();
   int removed_count = 0;
   for (const auto& word : words)
   {
      // First verify the key exists before removing it
      auto cur   = cursor(ctx.get_root());
      bool found = cur.lower_bound(to_key_view(word));
      REQUIRE(not cur.is_end());             // Should not be at end if key exists
      REQUIRE(cur.key() == key_view(word));  // Verify we found the correct key

      // Now remove the key
      int result = ctx.remove(to_key_view(word));
      REQUIRE(result > 0);  // Should return size of removed value (word length)
      removed_count++;

      // Validate invariants periodically (every 10K removals)
      if (ctx.get_root() && (removed_count % 10000 == 0))
         ctx.validate();
   }
   end = std::chrono::high_resolution_clock::now();
   SAL_ERROR("removed {:L} words in {:L} ms, {:L} words/sec", removed_count,
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
             (removed_count * 1000.0) /
                 std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());

   REQUIRE(removed_count == words.size());

   // Verify tree is empty by trying to remove a known word
   int result = ctx.remove(to_key_view("hello"));
   REQUIRE(result == -1);  // Should return -1 for non-existent key

   // Verify tree is empty by checking root is null
   REQUIRE_FALSE(ctx.get_root());

   SAL_INFO("Successfully inserted and removed {} words", words.size());
}

TEST_CASE("tree_context-single-branch-collapse", "[tree_context][remove][collapse]")
{
   sal::set_current_thread_name("main");
   std::filesystem::remove_all("db");
   sal::register_type_vtable<leaf_node>();
   sal::register_type_vtable<inner_prefix_node>();
   sal::register_type_vtable<inner_node>();
   sal::register_type_vtable<value_node>();

   sal::allocator salloc("db", sal::runtime_config());
   auto           ses  = salloc.get_session();
   auto           root = ses->get_root<>(sal::root_object_number(0));

   tree_context ctx(root);

   // Phase 1: Test inner_node collapse
   // Insert 50 keys each for groups "a", "b", "c"
   std::vector<std::string> a_keys, b_keys, c_keys;
   for (int i = 0; i < 50; ++i)
   {
      std::string ak = "a" + std::to_string(i);
      std::string bk = "b" + std::to_string(i);
      std::string ck = "c" + std::to_string(i);
      a_keys.push_back(ak);
      b_keys.push_back(bk);
      c_keys.push_back(ck);
      ctx.insert(to_key_view(ak), to_value_view(ak));
      ctx.insert(to_key_view(bk), to_value_view(bk));
      ctx.insert(to_key_view(ck), to_value_view(ck));
   }
   ctx.validate();

   // Remove all "b" keys
   for (const auto& k : b_keys)
   {
      int result = ctx.remove(to_key_view(k));
      REQUIRE(result > 0);
   }
   ctx.validate();

   // Remove all "c" keys -> should trigger collapse
   for (const auto& k : c_keys)
   {
      int result = ctx.remove(to_key_view(k));
      REQUIRE(result > 0);
   }
   ctx.validate();

   // Check no single-branch inner nodes remain
   auto stats = ctx.get_stats();
   REQUIRE(stats.single_branch_inners == 0);

   // Verify all "a" keys still accessible via cursor
   auto cur = cursor(ctx.get_root());
   std::sort(a_keys.begin(), a_keys.end());
   cur.seek_rend();
   for (const auto& k : a_keys)
   {
      REQUIRE(cur.next());
      REQUIRE(cur.key() == key_view(k));
   }
   REQUIRE_FALSE(cur.next());

   // Phase 2: Test inner_prefix_node collapse
   // Insert keys with shared prefix
   std::vector<std::string> px_keys, py_keys;
   for (int i = 0; i < 50; ++i)
   {
      std::string xk = "prefix_x" + std::to_string(i);
      std::string yk = "prefix_y" + std::to_string(i);
      px_keys.push_back(xk);
      py_keys.push_back(yk);
      ctx.insert(to_key_view(xk), to_value_view(xk));
      ctx.insert(to_key_view(yk), to_value_view(yk));
   }
   ctx.validate();

   // Remove all "prefix_y" keys -> should collapse inner_prefix_node
   for (const auto& k : py_keys)
   {
      int result = ctx.remove(to_key_view(k));
      REQUIRE(result > 0);
   }
   ctx.validate();

   stats = ctx.get_stats();
   REQUIRE(stats.single_branch_inners == 0);

   // Verify all remaining keys accessible via cursor
   std::vector<std::string> all_remaining;
   all_remaining.insert(all_remaining.end(), a_keys.begin(), a_keys.end());
   all_remaining.insert(all_remaining.end(), px_keys.begin(), px_keys.end());
   std::sort(all_remaining.begin(), all_remaining.end());

   cur = cursor(ctx.get_root());
   cur.seek_rend();
   int count = 0;
   while (cur.next())
   {
      REQUIRE(count < all_remaining.size());
      REQUIRE(cur.key() == key_view(all_remaining[count]));
      count++;
   }
   REQUIRE(count == all_remaining.size());
}

TEST_CASE("tree_context-subtree-collapse", "[tree_context][remove][collapse]")
{
   sal::set_current_thread_name("main");
   std::filesystem::remove_all("db");
   sal::register_type_vtable<leaf_node>();
   sal::register_type_vtable<inner_prefix_node>();
   sal::register_type_vtable<inner_node>();
   sal::register_type_vtable<value_node>();

   sal::allocator salloc("db", sal::runtime_config());
   auto           ses  = salloc.get_session();
   auto           root = ses->get_root<>(sal::root_object_number(0));

   tree_context ctx(root);

   // Insert 4 groups of 6 keys with shared prefixes to create multi-level trie
   // Total = 24 keys. After removing 2 groups (12 keys), 12 remain < threshold (24)
   std::vector<std::string> g1_keys, g2_keys, g3_keys, g4_keys;
   for (int i = 0; i < 6; ++i)
   {
      char buf[32];
      snprintf(buf, sizeof(buf), "group1_%03d", i);
      g1_keys.push_back(buf);
      snprintf(buf, sizeof(buf), "group2_%03d", i);
      g2_keys.push_back(buf);
      snprintf(buf, sizeof(buf), "group3_%03d", i);
      g3_keys.push_back(buf);
      snprintf(buf, sizeof(buf), "group4_%03d", i);
      g4_keys.push_back(buf);
   }

   for (const auto& k : g1_keys) ctx.insert(to_key_view(k), to_value_view(k));
   for (const auto& k : g2_keys) ctx.insert(to_key_view(k), to_value_view(k));
   for (const auto& k : g3_keys) ctx.insert(to_key_view(k), to_value_view(k));
   for (const auto& k : g4_keys) ctx.insert(to_key_view(k), to_value_view(k));
   ctx.validate();

   auto stats_before = ctx.get_stats();
   REQUIRE(stats_before.total_keys == 24);

   // Remove groups 3 and 4 under "group3_" and "group4_" prefixes
   // This should bring descendants below threshold and trigger collapse
   for (const auto& k : g3_keys)
   {
      int result = ctx.remove(to_key_view(k));
      REQUIRE(result > 0);
   }
   ctx.validate();

   for (const auto& k : g4_keys)
   {
      int result = ctx.remove(to_key_view(k));
      REQUIRE(result > 0);
   }
   ctx.validate();

   // Verify remaining keys still accessible via cursor
   std::vector<std::string> remaining;
   remaining.insert(remaining.end(), g1_keys.begin(), g1_keys.end());
   remaining.insert(remaining.end(), g2_keys.begin(), g2_keys.end());
   std::sort(remaining.begin(), remaining.end());

   auto cur = cursor(ctx.get_root());
   cur.seek_rend();
   int count = 0;
   while (cur.next())
   {
      REQUIRE(count < remaining.size());
      REQUIRE(cur.key() == key_view(remaining[count]));
      count++;
   }
   REQUIRE(count == remaining.size());

   auto stats_after = ctx.get_stats();
   REQUIRE(stats_after.total_keys == 12);
   REQUIRE(stats_after.sparse_subtree_inners == 0);

   // Re-insert removed keys — verify leaf splits correctly
   for (const auto& k : g3_keys) ctx.insert(to_key_view(k), to_value_view(k));
   for (const auto& k : g4_keys) ctx.insert(to_key_view(k), to_value_view(k));
   ctx.validate();

   auto stats_reinsert = ctx.get_stats();
   REQUIRE(stats_reinsert.total_keys == 24);

   // Verify all keys accessible
   std::vector<std::string> all_keys;
   all_keys.insert(all_keys.end(), g1_keys.begin(), g1_keys.end());
   all_keys.insert(all_keys.end(), g2_keys.begin(), g2_keys.end());
   all_keys.insert(all_keys.end(), g3_keys.begin(), g3_keys.end());
   all_keys.insert(all_keys.end(), g4_keys.begin(), g4_keys.end());
   std::sort(all_keys.begin(), all_keys.end());

   cur = cursor(ctx.get_root());
   cur.seek_rend();
   count = 0;
   while (cur.next())
   {
      REQUIRE(count < all_keys.size());
      REQUIRE(cur.key() == key_view(all_keys[count]));
      count++;
   }
   REQUIRE(count == all_keys.size());

   // Edge case: large keys should skip collapse (wouldn't fit in a single leaf)
   tree_context ctx2(ses->get_root<>(sal::root_object_number(1)));
   std::string large_prefix(500, 'x');
   std::vector<std::string> large_keys;
   for (int i = 0; i < 5; ++i)
   {
      std::string k = large_prefix + std::to_string(i);
      large_keys.push_back(k);
      ctx2.insert(to_key_view(k), to_value_view(k));
   }
   // Add keys on a different branch to create inner nodes
   for (int i = 0; i < 5; ++i)
   {
      std::string k = "y" + std::to_string(i);
      ctx2.insert(to_key_view(k), to_value_view(k));
   }
   ctx2.validate();

   // Remove all "y" keys - with large prefix keys, collapse should be skipped
   for (int i = 0; i < 5; ++i)
   {
      std::string k = "y" + std::to_string(i);
      ctx2.remove(to_key_view(k));
   }
   ctx2.validate();

   // Verify large-key entries still intact
   cur = cursor(ctx2.get_root());
   cur.seek_rend();
   std::sort(large_keys.begin(), large_keys.end());
   count = 0;
   while (cur.next())
   {
      REQUIRE(count < large_keys.size());
      REQUIRE(cur.key() == key_view(large_keys[count]));
      count++;
   }
   REQUIRE(count == large_keys.size());
}

TEST_CASE("tree_context", "[tree_context]")
{
   sal::set_current_thread_name("main");
   std::filesystem::remove_all("db");
   sal::allocator salloc("db", sal::runtime_config());
   sal::register_type_vtable<leaf_node>();
   sal::register_type_vtable<inner_prefix_node>();
   sal::register_type_vtable<inner_node>();
   sal::register_type_vtable<value_node>();
   auto ses  = salloc.get_session();
   auto root = ses->get_root<>(sal::root_object_number(0));

   //std::locale::global(std::locale("en_US.UTF-8"));

   auto words = load_words();  //18200);
   SAL_INFO("loaded {} words: {}", words.size());

   {
      tree_context ctx(root);
      // make unique from dictionary
      ctx.insert("hellohello", "world");

      smart_ptr<alloc_header> last_version;
      auto                    print_stats = [&]()
      {
         auto stats = ctx.get_stats();
         SAL_ERROR(
             "Stats:\n"
             "  Inner nodes:        {:L}\n"
             "  Inner prefix nodes: {:L}\n"
             "  Leaf nodes:         {:L}\n"
             "  Value nodes:        {:L}\n"
             "  Branches:           {:L}\n"
             "  Clines:             {:L}\n"
             "  Max depth:          {:L}\n"
             "  Total keys:         {:L}\n"
             "  Total inner node size: {:L}\n"
             "  Average inner node size: {:L}\n"
             "  Average clines per inner node: {:L}\n"
             "  Average branch per inner node: {:L}",
             stats.inner_nodes, stats.inner_prefix_nodes, stats.leaf_nodes, stats.value_nodes,
             stats.branches, stats.clines, stats.max_depth, stats.total_keys,
             stats.total_inner_node_size, stats.average_inner_node_size(),
             stats.average_clines_per_inner_node(), stats.average_branch_per_inner_node());
      };
      //print_stats();

      uint32_t             batch             = 100000 / SCALE;
      uint32_t             round_size        = 1000000 / SCALE;
      uint32_t             batches_per_round = round_size / batch;
      uint64_t             key               = 0;
      std::array<char, 63> big_value;
      for (int r = 0; r < 30 / SCALE; ++r)
      {
         auto start = std::chrono::high_resolution_clock::now();
         for (uint32_t b = 0; b < batches_per_round; b++)
         {
            for (uint32_t i = 0; i < batch; i++)
            {
               ++key;
               key                      = rand64();
               uint64_t   bigendian_key = __builtin_bswap64(key);
               key_view   kstr((char*)&bigendian_key, sizeof(bigendian_key));
               value_view vstr(big_value.data(), big_value.size());
               ctx.insert(kstr, kstr);
            }
            ses->set_root(sal::root_object_number(0), ctx.get_root(), sal::sync_type::mprotect);
         }
         auto end = std::chrono::high_resolution_clock::now();
         auto duration_ms =
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
         auto inserts_per_sec = (batch * batches_per_round * 1000.) / duration_ms;
         SAL_ERROR("[{:L}] Dense Random {:L} inserts/sec batch size: {:L}", r,
                   uint64_t(inserts_per_sec), batch);
         SAL_WARN("total nodes visited: {:L} total allocated: {:L}", ctx.get_stats().total_nodes(),
                  ses->get_total_allocated_objects());
      }
      print_stats();
      cursor cur(ctx.get_root());
      for (int r = 0; r < 3; ++r)
      {
         auto start = std::chrono::high_resolution_clock::now();
         for (int i = 0; i < round_size; i++)
         {
            key                      = rand64();
            uint64_t   bigendian_key = __builtin_bswap64(key);
            key_view   kstr((char*)&bigendian_key, sizeof(bigendian_key));
            value_view vstr(big_value.data(), big_value.size());
            cur.lower_bound(kstr);
         }
         auto end = std::chrono::high_resolution_clock::now();
         auto duration_ms =
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
         auto lower_bound_per_sec = (round_size * 1000.) / duration_ms;
         SAL_ERROR("lower bound: {:L} ms {:L} words/sec", duration_ms, lower_bound_per_sec);
      }
   }
   SAL_ERROR("total allocated after context exit : {:L}", ses->get_total_allocated_objects());
   ses->set_root(sal::root_object_number(0), sal::smart_ptr<alloc_header>(),
                 sal::sync_type::mprotect);
   SAL_ERROR("total allocated after set root null: {:L}", ses->get_total_allocated_objects());
   /*
   auto stats = ctx.get_stats();
   SAL_ERROR(
       "Stats:\n"
       "  Inner nodes:        {:L}\n"
       "  Inner prefix nodes: {:L}\n"
       "  Leaf nodes:         {:L}\n"
       "  Value nodes:        {:L}\n"
       "  Branches:           {:L}\n"
       "  Clines:             {:L}\n"
       "  Max depth:          {:L}\n"
       "  Total keys:         {:L}\n"
       "  Total inner node size: {:L}\n"
       "  Average inner node size: {:L}\n"
       "  Average clines per inner node: {:L}\n"
       "  Average branch per inner node: {:L}",
       stats.inner_nodes, stats.inner_prefix_nodes, stats.leaf_nodes, stats.value_nodes,
       stats.branches, stats.clines, stats.max_depth, stats.total_keys, stats.total_inner_node_size,
       stats.average_inner_node_size(), stats.average_clines_per_inner_node(),
       stats.average_branch_per_inner_node());
       */
   //ctx.print();
}
