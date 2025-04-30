#include <catch2/catch_all.hpp>
#include <fstream>
#include <psitri/cursor.hpp>
#include <psitri/tree_ops.hpp>
#include <random>
#include <sal/sal.hpp>
#include "sal/numbers.hpp"
using namespace psitri;

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
   SAL_WARN("lower bound Ancerata");
   cur.lower_bound(to_key_view("Ancerata"));
   assert(cur.key() == key_view("Ancerata"));
}

TEST_CASE("cursor-lowerbound", "[cursor]")
{
   sal::set_current_thread_name("main");
   std::filesystem::remove_all("db");
   sal::allocator salloc("db", sal::runtime_config());
   auto           ses = salloc.get_session();
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

      uint32_t             batch             = 100000;
      uint32_t             round_size        = 1000000;
      uint32_t             batches_per_round = round_size / batch;
      uint64_t             key               = 0;
      std::array<char, 63> big_value;
      for (int r = 0; r < 30; ++r)
      {
         //  SAL_WARN("round: {}", r);
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
               //ctx.validate(last_version);
               ctx.insert(kstr, kstr);
               //ctx.validate();
               //ctx.validate(last_version);
            }
            //  SAL_WARN("total nodes visited: {:L} total allocated: {:L}", ctx.get_stats().total_nodes(),
            //           ses->get_total_allocated_objects());
            ses->set_root(sal::root_object_number(0), ctx.get_root(), sal::sync_type::mprotect);
            //   SAL_WARN("after set root total nodes visited: {:L} total allocated: {:L}",
            //           ctx.get_stats().total_nodes(), ses->get_total_allocated_objects());
            // ctx.print();
            //last_version = ctx.get_root();
         }
         auto end = std::chrono::high_resolution_clock::now();
         auto duration_ms =
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
         auto inserts_per_sec = (batch * batches_per_round * 1000.) / duration_ms;
         SAL_ERROR("[{:L}] Dense Random {:L} inserts/sec batch size: {:L}", r,
                   uint64_t(inserts_per_sec), batch);
         // last_version = ctx.get_root();
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
