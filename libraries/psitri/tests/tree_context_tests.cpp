#include <catch2/catch_all.hpp>
#include <fstream>
#include <locale>
#include <psitri/tree_ops.hpp>
#include <random>
#include <sal/sal.hpp>
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

TEST_CASE("tree_context", "[tree_context]")
{
   std::filesystem::remove_all("db");
   sal::allocator salloc("db", sal::runtime_config());
   auto           ses  = salloc.get_session();
   auto           root = ses->get_root<>(sal::root_object_number(0));

   std::locale::global(std::locale("en_US.UTF-8"));

   auto words = load_words();  //18200);
   SAL_INFO("loaded {} words: {}", words.size());

   tree_context ctx(root);
   // make unique from dictionary
   /*
   ctx.insert("hellohello", "world");

   auto start = std::chrono::high_resolution_clock::now();
   for (int i = 0; i < words.size(); i++)
   {
      //      SAL_INFO("inserting word: {}", word);
      ctx.insert(to_key_view(words[i]), to_value_view(words[i]));
   }
   auto end = std::chrono::high_resolution_clock::now();
   SAL_ERROR("loaded {:L} words in {:L} ms", words.size(),
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
   auto duration_ms   = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   auto words_per_sec = (words.size() * 1000.0) / duration_ms;

   SAL_ERROR("Performance: {:L.10f} words/sec", uint64_t(words_per_sec));
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

   for (int i = 0; i < 50; ++i)
   {
      uint64_t key;
      auto     start = std::chrono::high_resolution_clock::now();
      for (uint32_t i = 0; i < 1000000; i++)
      {
         key = rand64();
         key_view kstr((char*)&key, sizeof(key));
         ctx.insert(kstr, kstr);
      }
      auto end         = std::chrono::high_resolution_clock::now();
      auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      auto inserts_per_sec = (1000000 * 1000) / duration_ms;
      SAL_ERROR("Performance: Dense Random {:L} inserts/sec", uint64_t(inserts_per_sec));
   }
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
   //ctx.print();
}
