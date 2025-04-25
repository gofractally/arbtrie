#include <catch2/catch_all.hpp>
#include <fstream>
#include <locale>
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
      /*
   auto                    start = std::chrono::high_resolution_clock::now();
   for (int i = 0; i < words.size(); i++)
   {
      if (i == 163)
      {
         SAL_ERROR("break point");
      }
      //      SAL_INFO("inserting word[{}]: {}", i, words[i]);
      ctx.insert(to_key_view(words[i]), to_value_view(words[i]));
     
      SAL_INFO("----------new version-----------");
      ctx.print();
      if (last_version)
      {
         SAL_INFO("----------last version------");
         ctx.print(*last_version);
         SAL_INFO("----------end last version------");
      }
   // ses->set_root(sal::root_object_number(1), ctx.get_root(), sal::sync_type::mprotect);
   last_version = ctx.get_root();
}
   auto end = std::chrono::high_resolution_clock::now();
   ctx.print();
   SAL_ERROR("loaded {:L} words in {:L} ms", words.size(),
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
   auto duration_ms   = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   auto words_per_sec = (words.size() * 1000.0) / duration_ms;

   SAL_ERROR("Performance: {:L.10f} words/sec", uint64_t(words_per_sec));
   */

      auto print_stats = [&]()
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
