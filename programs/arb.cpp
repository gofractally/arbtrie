#include <stdlib.h>
#include <boost/program_options.hpp>
#include <format>
#include <fstream>
#include <iostream>
#include <string>

#include <arbtrie/binary_node.hpp>
#include <arbtrie/database.hpp>
#include <random>
#include <sstream>
#include <vector>

using namespace arbtrie;

void test_binary_node_layout() {}

int64_t rand64()
{
   thread_local static std::mt19937 gen(rand());
   return (uint64_t(gen()) << 32) | gen();
}
uint64_t bswap(uint64_t x)
{
   x = (x & 0x00000000FFFFFFFF) << 32 | (x & 0xFFFFFFFF00000000) >> 32;
   x = (x & 0x0000FFFF0000FFFF) << 16 | (x & 0xFFFF0000FFFF0000) >> 16;
   x = (x & 0x00FF00FF00FF00FF) << 8 | (x & 0xFF00FF00FF00FF00) >> 8;
   return x;
}
void indent(int depth)
{
   std::cout << depth << "|";
   for (int i = 0; i < depth; ++i)
      std::cout << "    ";
}
namespace arbtrie
{
   void toupper(std::string& s)
   {
      for (auto& c : s)
         c = std::toupper(c);
   }
   std::string to_upper(std::string_view sv)
   {
      std::string str(sv);
      toupper(str);
      return str;
   }

   void print_hex(std::string_view v)
   {
      std::cout << std::setfill('0');
      for (auto c : v)
         std::cout << std::hex << std::setw(2) << uint16_t(uint8_t(c));
   }
}  // namespace arbtrie

/*
std::string add_comma(uint64_t s)
{
   if (s < 1000)
      return std::to_string(s);
   if (s < 1000000)
   {
      return std::to_string(s / 1000) + ',' + std::to_string((s % 1000) + 1000).substr(1);
   }
   if (s < 1000000000)
   {
      return std::to_string(s / 1000000) + ',' +
             std::to_string(((s % 1000000) / 1000) + 1000).substr(1) + "," +
             std::to_string((s % 1000) + 1000).substr(1);
   }
   return std::to_string(s);
};
*/

void validate_invariant(session_rlock& state, id_address i);
void validate_invariant(session_rlock& state, id_address i, auto* in)
{
   in->visit_branches_with_br(
       [&](int br, id_address adr)
       {
          if (in->branch_region() != adr.region)
             throw std::runtime_error("region invariant violated");
          validate_invariant(state, adr);
       });
}
void validate_invariant(session_rlock& state, id_address i, const binary_node* inner) {}
void validate_invariant(session_rlock& state, id_address i, const value_node* inner) {}
void validate_invariant(session_rlock& state, id_address i)
{
   if (i)
   {
      auto ref = state.get(i);
      cast_and_call(ref.header(), [&](const auto* ptr) { validate_invariant(state, i, ptr); });
   }
}

void print(session_rlock& state, id_address i, int depth = 1);
void print_pre(session_rlock&           state,
               id_address               i,
               std::string              prefix,
               std::vector<std::string> path  = {},
               int                      depth = 1);
/*
void print(session_rlock& state, const arbtrie::index_node* in, int depth )
{
   indent(depth);
   std::cout << "index node: "<< in->id() << " with " << in->num_branches() <<" branches\n";
   for( uint16_t i = 0; i < 257; ++i ) {
      indent(depth+1);
      auto b = in->get_branch(i);

      if( b ) {
      std::cout << i << "]  id: " <<  (b ? *b : object_id()) <<"  ";
      std::cout << "ref: " << state.get(*b).ref() <<"\n";
      } else {
      std::cout << i << "]  id: null \n";
      }

   }
}
*/

void print_pre(session_rlock&           state,
               auto*                    in,
               std::string              prefix,
               std::vector<std::string> path,
               int                      depth = 1)
{
   prefix += to_str(in->get_prefix());
   path.push_back(to_hex(in->get_prefix()));

   in->visit_branches_with_br(
       [&](int br, id_address bid)
       {
          if (0 == br)
          {
             std::cout << depth << " |" << node_type_names[in->get_type()][0] << "  ";
             //std::cout << prefix;
             print_hex(prefix);
             std::cout << "   " << bid << "  ";
             auto va = state.get(bid);
             std::cout << node_type_names[va.header()->get_type()] << "    ";
             // std::cout << va->value();
             // assert(to_upper(prefix) == va->value());
             //print_hex(to_str(va->value()));
             std::cout << "\n";
             return;
          }
          auto c = branch_to_char(br);
          path.push_back("-" + to_hex(key_view((char*)&c, 1)));
          print_pre(state, bid, prefix + char(branch_to_char(br)), path, depth + 1);
          path.pop_back();
       });
   path.pop_back();
}
void print_pre(session_rlock&           state,
               const binary_node*       bn,
               std::string              prefix,
               std::vector<std::string> path,
               int                      depth = 1)
{
   for (int i = 0; i < bn->num_branches(); ++i)
   {
      //auto k = bn->get_key(i);
      std::cout << depth << " |B  ";
      auto kvp = bn->get_key_val_ptr(i);
      print_hex(prefix);
      std::cout << "-";
      print_hex(std::string(to_str(kvp->key())));

      std::cout << "     ";
      for (auto s : path)
         std::cout << s << " ";
      std::cout << to_hex(kvp->key());
      //std::cout << (prefix + std::string(kvp->key()));
      std::cout << "\n";
      /*
      if (prefix.size() + kvp->key().size() > 8)
      {
         ARBTRIE_WARN("    ERROR   ");
      }
      */
      /*
      indent(depth);
      auto v = kvp->value();
      if (bn->is_obj_id(i))  //kvp->is_value_node())
      {
         std::cout << " id: " << kvp->value_id() << "  ";
         auto vr = state.get(kvp->value_id());
         v       = vr.as<value_node>()->value();
         std::cout << "ref: " << vr.ref() << " ";
      }
      //   std::cout << "koff: " << bn->key_offset(i) << " ksize: ";
      //   std::cout << k.size() <<" ";
      std::cout << "'";
      std::cout << kvp->key() << "' = '" << v << "'\n";
      */
   }
}

void print_pre(session_rlock&           state,
               id_address               i,
               std::string              prefix,
               std::vector<std::string> path,
               int                      depth)
{
   auto obj = state.get(i);
   switch (obj.header()->get_type())
   {
      case node_type::binary:
         return print_pre(state, obj.as<binary_node>(), prefix, path, depth);
      case node_type::setlist:
         return print_pre(state, obj.as<setlist_node>(), prefix, path, depth);
      case node_type::full:
         return print_pre(state, obj.as<full_node>(), prefix, path, depth);
      case node_type::value:
         std::cout << "VALUE: id: " << i << "\n";
         return;
      default:
         std::cout << "UNKNOWN!: id: " << i << "  " << obj.header()->get_type() << "\n";
         return;
   }
}

void print(session_rlock& state, const binary_node* bn, int depth = 0)
{
   assert(depth < 6);
   assert(bn->get_type() == node_type::binary);
   //indent(depth);
   std::cout << "BN   r" << state.get(bn->address()).ref() << "    binary node " << bn->address()
             << " with " << std::dec << bn->num_branches()
             << " branches and ref : " << state.get(bn->address()).ref() << " size: " << bn->size()
             << "  spare: " << bn->spare_capacity() << "  "
             << " free_slots: " << int(bn->_branch_cap - bn->_num_branches)
             << " kvsize: " << bn->key_val_section_size() << "\n";

   return;
   /*
   for (int i = 0; i < bn->num_branches(); ++i)
   {
      //auto k = bn->get_key(i);
      auto kvp = bn->get_key_val_ptr(i);
      indent(depth);
      auto v = kvp->value();
      if (bn->is_obj_id(i))  //kvp->is_value_node())
      {
         std::cout << " id: " << kvp->value_id() << "  ";
         auto vr = state.get(kvp->value_id());
         v       = vr.as<value_node>()->value();
         std::cout << "ref: " << vr.ref() << " ";
      }
      //   std::cout << "koff: " << bn->key_offset(i) << " ksize: ";
      //   std::cout << k.size() <<" ";
      std::cout << "'";
      //    print_hex(kvp->key());
      std::cout << to_str(kvp->key()) << "' = '" << to_str(v) << "'\n";
   }
   */
}

void print(session_rlock& state, const full_node* sl, int depth = 0)
{
   std::cout << "FULL r" << state.get(sl->address()).ref() << "   cpre\""
             << to_str(sl->get_prefix()) << "\" cps: " << sl->get_prefix().size()
             << " id: " << sl->address() << " ";
   /**             
   if (sl->has_eof_value())
   {
      std::cout << " = '" << to_str(state.get(sl->get_branch(0)).as<value_node>()->value())
                << "' branches: " << std::dec << sl->num_branches() << " \n";
   }
   else
   {
      std::cout << "\n";
   }
   sl->visit_branches_with_br(
       [&](int br, id_address bid)
       {
          if (not br)
             return;
          indent(depth);
          //std::cout << "'"<<char(br-1)<<"' -> ";
          std::cout << "'" << br << "' -> ";
          print(state, bid, depth + 1);
       });
       */
}
void print(session_rlock& state, const value_node* sl, int depth = 0) {}
void print(session_rlock& state, const setlist_node* sl, int depth = 0)
{
   //  auto gsl = sl->get_setlist();
   //   for( auto b : gsl ) {
   //     std::cout << int(b) <<"\n";
   // }
   sl->visit_branches_with_br(
       [&](int br, id_address bid)
       {
          if (not br)
             return;
          indent(depth);
          //std::cout << "'"<<char(br-1)<<"' -> ";
          std::cout << "'" << br << "' -> ";
          print(state, bid, depth + 1);
       });
   assert(sl->validate());
   /*
   const auto count = sl->num_branches() - sl->has_eof_value();
   for (int i = 0; i < count; ++i)
   {
      indent(depth);
      auto byte_id = sl->get_by_index(i);
      std::cout << "'" << std::dec << uint16_t(byte_id.first) << "' -> ";
      std::cout << "  ref: " << std::dec << state.get(byte_id.second).ref() << " id: " << std::dec
                << byte_id.second;
      print(state, byte_id.second, depth + 1);
   }
   */
}
/*
void find_refs(session_rlock& state, object_id i, int depth);
void find_refs(session_rlock& state, const binary_node* bn, int depth = 0) {}
void find_refs(session_rlock& state, const setlist_node* sl, int depth = 0)
{
   const auto count = sl->num_branches() - sl->has_eof_value();
   for (int i = 0; i < count; ++i)
   {
      auto byte_id = sl->get_by_index(i);
      find_refs(state, byte_id.second, depth + 1);
   }
}
void find_refs(session_rlock& state, object_id i, int depth)
{
   auto obj = state.get(i);
   //if( obj.ref() != 1 ) throw std::runtime_error("unexpected ref" );
   assert(obj.ref() == 1);
   switch (obj.type())
   {
      case node_type::setlist:
         return find_refs(state, obj.as<setlist_node>(), depth);
      case node_type::binary:
         return find_refs(state, obj.as<binary_node>(), depth);
      case node_type::value:
      default:
         return;
   }
}

*/
void print(session_rlock& state, id_address i, int depth)
{
   auto obj = state.get(i);
   switch (obj.header()->get_type())
   {
      case node_type::binary:
         return print(state, obj.as<binary_node>(), depth);
      case node_type::setlist:
         return print(state, obj.as<setlist_node>(), depth);
      case node_type::full:
         return print(state, obj.as<full_node>(), depth);
      case node_type::value:
         return print(state, obj.as<value_node>(), depth);
         std::cout << "VALUE: id: " << i << "\n";
         return;
      default:
         std::cout << "UNKNOWN!: id: " << i << " " << obj.header()->get_type() << " -\n";
         return;
   }
}

void test_binary_node();
void test_refactor();
int  main(int argc, char** argv)
{
   arbtrie::thread_name("main");
   //test_binary_node();
   //   test_refactor();
   //   return 0;
   //
   // This variable previously controlled explicit compaction
   // Now it's only used for logging purposes since compaction is automatic
   bool sync_compact = false;

   std::cout << "resetting database\n";
   std::filesystem::remove_all("arbtriedb");
   arbtrie::database::create("arbtriedb");

   //const char* filename = "/Users/dlarimer/all_files.txt";
   auto          filename = "/usr/share/dict/words";
   std::ifstream file(filename);

   std::vector<std::string> v;
   std::string              str;

   int64_t batch_size = 10000;

   // Read the next line from File until it reaches the
   // end.
   while (file >> str)
   {
      // Now keep reading next line
      // and push it in vector function until end of file
      v.push_back(str);
   }
   std::cout << "loaded " << v.size() << " keys from " << filename << "\n";

   arbtrie::runtime_config cfg;

   uint64_t seq = 0;
   try
   {
      ARBTRIE_WARN("starting arbtrie...");

      do
      {
         std::optional<node_handle> last_root;
         std::optional<node_handle> last_root2;
         int                        rounds             = 3;
         int                        multithread_rounds = 20;
         int                        data_size          = 8;
         namespace po                                  = boost::program_options;
         // clang-format off
         po::options_description desc("Test options");
         desc.add_options()
            ("help,h", "Print help message")
            ("dense-rand", po::bool_switch()->default_value(true), "Run dense random insert test")
            ("little-endian-seq", po::bool_switch()->default_value(true), "Run little endian sequential insert test")
            ("big-endian-seq", po::bool_switch()->default_value(true), "Run big endian sequential insert test")
            ("big-endian-rev", po::bool_switch()->default_value(true), "Run big endian reverse sequential insert test")
            ("rand-string", po::bool_switch()->default_value(true), "Run random string insert test")
            ("sync", po::value<sync_type>()->default_value(sync_type::mprotect), "none, mprotect, msync_async, msync_sync, fsync, full")
            ("enable-read-cache", po::bool_switch()->default_value(true), "Read threads will promote data to pinned memory")
            ("count", po::value<int>()->default_value(1000000), "Number of items to insert")
            ("batch-size", po::value<int>()->default_value(100), "Number of items to insert per batch")
            ("compacted-pinned-threshold-mb", po::value<int>()->default_value(16), 
                      "How much unused space is tolerated before compacting pinned segments, "
                      "increases SSD wear if in sync mode and this is low, but boosts performance "
                      " if you can keep more pinned memory doing useful stuff, max 32MB")
            ("compacted-unpinned-threshold-mb", po::value<int>()->default_value(16), 
                      "How much unused space is tolerated before compacting unpinned segments, "
                      "increases SSD wear, but reduces space used if low, if high it will save your "
                      " SSD from wear but consume more storage, max 32MB")
            ("rounds", po::value<int>()->default_value(3), "Number of rounds to run")
            ("datasize", po::value<int>()->default_value(8), "Number of bytes in the key")
            ("multithread-rounds", po::value<int>()->default_value(20), "Number of multi-thread rounds to run")
            ("max-pinned-cache-size-mb", po::value<int>()->default_value(1024), "Amount of RAM to pin in memory, multiple of 32 MB");
         // clang-format on

         po::variables_map vm;
         po::store(po::parse_command_line(argc, argv, desc), vm);
         po::notify(vm);

         if (vm.count("help"))
         {
            std::cout << desc << "\n";
            return 0;
         }

         const int count                          = vm["count"].as<int>();
         batch_size                               = vm["batch-size"].as<int>();
         rounds                                   = vm["rounds"].as<int>();
         multithread_rounds                       = vm["multithread-rounds"].as<int>();
         cfg.max_pinned_cache_size_mb             = vm["max-pinned-cache-size-mb"].as<int>();
         cfg.compact_pinned_unused_threshold_mb   = vm["compacted-pinned-threshold-mb"].as<int>();
         cfg.compact_unpinned_unused_threshold_mb = vm["compacted-unpinned-threshold-mb"].as<int>();
         cfg.sync_mode                            = vm["sync"].as<sync_type>();
         cfg.enable_read_cache                    = vm["enable-read-cache"].as<bool>();
         data_size                                = vm["datasize"].as<int>();

         ARBTRIE_WARN("count: ", count);
         ARBTRIE_WARN("batch size: ", batch_size);
         ARBTRIE_WARN("rounds: ", rounds);
         ARBTRIE_WARN("multithread rounds: ", multithread_rounds);
         ARBTRIE_WARN("max pinned cache size: ", cfg.max_pinned_cache_size_mb);
         ARBTRIE_WARN("compact pinned unused threshold: ", cfg.compact_pinned_unused_threshold_mb);
         ARBTRIE_WARN("compact unpinned unused threshold: ",
                      cfg.compact_unpinned_unused_threshold_mb);
         ARBTRIE_WARN("sync mode: ", cfg.sync_mode);

         database db("arbtriedb", cfg);
         auto     ws = db.start_write_session();

         auto tx = ws->start_write_transaction(0);

         auto iterate_all = [&]()
         {
            try
            {
               uint64_t item_count = 0;

               std::vector<char> data;
               auto              start = std::chrono::steady_clock::now();
               tx->start();
               while (tx->next())
               {
                  tx->key();
                  //  tx->value([](auto&&) { return 0; });
                  ++item_count;
               }
               auto end   = std::chrono::steady_clock::now();
               auto delta = end - start;
               auto kc    = tx->count_keys();
               std::cout << "iterated " << std::setw(12)
                         << add_comma(
                                int64_t(item_count) /
                                (std::chrono::duration<double, std::milli>(delta).count() / 1000))
                         << " items/sec  total items: " << add_comma(item_count) << " count: " << kc
                         << "\n";
            }
            catch (const std::exception& e)
            {
               ARBTRIE_WARN("Caught Exception: ", e.what());
               throw;
            }
         };

         uint64_t seq3  = 0;
         auto     ttest = temp_meta_type(5);

         if (vm["dense-rand"].as<bool>())
         {
            std::cout << "insert dense rand \n";
            for (int ro = 0; ro < rounds; ++ro)
            {
               auto start = std::chrono::steady_clock::now();
               for (int i = 0; i < count * 3; i += 3)
               {
                  uint64_t val = rand64();
                  ++seq;
                  key_view kstr((char*)&val, sizeof(val));
                  tx->insert(kstr, kstr);
                  assert(tx->valid());

                  if ((seq % batch_size) == (batch_size - 1))
                  {
                     tx->commit_and_continue();
                     assert(tx->valid());
                  }
               }
               tx->commit_and_continue();
               assert(tx->valid());

               auto end   = std::chrono::steady_clock::now();
               auto delta = end - start;
               if (sync_compact)
               {
                  // NOTE: compact_next_segment() was removed from the database API
                  // Compaction now happens automatically in the background
               }

               std::cout << ro << "] " << std::setw(12)
                         << add_comma(int64_t(
                                (count) /
                                (std::chrono::duration<double, std::milli>(delta).count() / 1000)))
                         << " dense rand insert/sec  total items: " << add_comma(seq) << "\n";
               iterate_all();
            }
         }

         if (vm["little-endian-seq"].as<bool>())
         {
            std::cout << "insert little endian seq\n";
            for (int ro = 0; ro < rounds; ++ro)
            {
               auto start = std::chrono::steady_clock::now();
               for (int i = 0; i < count; ++i)
               {
                  uint64_t val = ++seq3;
                  seq++;
                  key_view kstr((char*)&val, sizeof(val));
                  tx->insert(kstr, kstr);
                  if ((i % batch_size) == (batch_size - 1))
                  {
                     assert(tx->valid());
                     tx->commit_and_continue();
                     assert(tx->valid());
                  }
               }
               tx->commit_and_continue();
               auto end   = std::chrono::steady_clock::now();
               auto delta = end - start;

               std::cout << ro << "] " << std::setw(12)
                         << add_comma(int64_t(
                                (count) /
                                (std::chrono::duration<double, std::milli>(delta).count() / 1000)))
                         << " insert/sec  total items: " << add_comma(seq) << "\n";
            }
         }

         auto start_big_end = seq3;
         if (vm["big-endian-seq"].as<bool>())
         {
            std::cout << "insert big endian seq starting with: " << seq3 << "\n";
            for (int ro = 0; ro < rounds; ++ro)
            {
               auto start = std::chrono::steady_clock::now();
               for (int i = 0; i < count; ++i)
               {
                  uint64_t val = bswap(seq3++);
                  ++seq;
                  key_view kstr((char*)&val, sizeof(val));
                  tx->insert(kstr, kstr);
                  if ((i % batch_size) == (batch_size - 1))
                  {
                     tx->commit_and_continue();
                  }
               }
               tx->commit_and_continue();
               auto end   = std::chrono::steady_clock::now();
               auto delta = end - start;

               std::cout << ro << "] " << std::setw(12)
                         << add_comma(int64_t(
                                (count) /
                                (std::chrono::duration<double, std::milli>(delta).count() / 1000)))
                         << " insert/sec  total items: " << add_comma(seq) << "\n";
               iterate_all();
            }
         }

         uint64_t seq4 = -1;
         if (vm["big-endian-rev"].as<bool>())
         {
            std::cout << "insert big endian rev seq\n";
            for (int ro = 0; ro < rounds; ++ro)
            {
               auto start = std::chrono::steady_clock::now();
               for (int i = 0; i < count; ++i)
               {
                  uint64_t val = bswap(seq4--);
                  ++seq;
                  key_view kstr((char*)&val, sizeof(val));
                  tx->insert(kstr, kstr);
                  if ((i % batch_size) == 0)
                  {
                     tx->commit_and_continue();
                  }
               }
               tx->commit_and_continue();
               auto end   = std::chrono::steady_clock::now();
               auto delta = end - start;

               std::cout << ro << "] " << std::setw(12)
                         << add_comma(int64_t(
                                (count) /
                                (std::chrono::duration<double, std::milli>(delta).count() / 1000)))
                         << " insert/sec  total items: " << add_comma(seq) << "\n";
            }
         }

         if (vm["rand-string"].as<bool>())
         {
            std::cout << "insert to_string(rand) \n";
            for (int ro = 0; ro < rounds; ++ro)
            {
               auto start = std::chrono::steady_clock::now();
               for (int i = 0; i < count; ++i)
               {
                  ++seq;
                  auto kstr = std::to_string(rand64());
                  tx->insert(to_key_view(kstr), to_value_view(kstr));
                  if ((i % batch_size) == 0)
                  {
                     tx->commit_and_continue();
                  }
               }
               tx->commit_and_continue();
               auto end   = std::chrono::steady_clock::now();
               auto delta = end - start;

               std::cout << ro << "] " << std::setw(12)
                         << add_comma(int64_t(
                                (count) /
                                (std::chrono::duration<double, std::milli>(delta).count() / 1000)))
                         << " rand str insert/sec  total items: " << add_comma(seq) << "\n";
            }
         }
         iterate_all();

         if (vm["little-endian-seq"].as<bool>())
         {
            std::cout << "get known key little endian seq\n";
            uint64_t seq2 = 0;
            for (int ro = 0; true and ro < rounds; ++ro)
            {
               auto start = std::chrono::steady_clock::now();
               for (int i = 0; i < count; ++i)
               {
                  uint64_t val = ++seq2;
                  key_view kstr((char*)&val, sizeof(val));
                  auto     s = tx->get_size(kstr);
                  assert(s > 0);
               }
               auto end   = std::chrono::steady_clock::now();
               auto delta = end - start;

               std::cout << ro << "] " << std::setw(12)
                         << add_comma(int64_t(
                                (count) /
                                (std::chrono::duration<double, std::milli>(delta).count() / 1000)))
                         << "  seq get/sec  total items: " << add_comma(seq) << "\n";
            }

            std::cout << "get known key little endian rand\n";
            for (int ro = 0; true and ro < rounds; ++ro)
            {
               auto start = std::chrono::steady_clock::now();
               for (int i = 0; i < count; ++i)
               {
                  uint64_t rnd  = rand64();
                  uint64_t val  = (rnd % (seq2 - 1)) + 1;
                  uint64_t val2 = val;
                  key_view kstr((char*)&val, sizeof(val));
                  auto     s = tx->get_size(kstr);
                  assert(s > 0);
               }
               auto end   = std::chrono::steady_clock::now();
               auto delta = end - start;

               std::cout << ro << "] " << std::setw(12)
                         << add_comma(int64_t(
                                (count) /
                                (std::chrono::duration<double, std::milli>(delta).count() / 1000)))
                         << "  rand get/sec  total items: " << add_comma(seq) << "\n";
            }
         }
         std::cout << "get known key big endian seq\n";
         for (int ro = 0; true and ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            for (int i = 0; i < count; ++i)
            {
               uint64_t val = bswap(start_big_end++);
               key_view kstr((char*)&val, sizeof(val));
               auto     s = tx->get_size(kstr);
               assert(s > 0);
            }
            auto end   = std::chrono::steady_clock::now();
            auto delta = end - start;

            std::cout << ro << "] " << std::setw(12)
                      << add_comma(int64_t(
                             (count) /
                             (std::chrono::duration<double, std::milli>(delta).count() / 1000)))
                      << "  seq get/sec  total items: " << add_comma(seq) << "\n";
         }

         std::cout << "lower bound random i64\n";
         for (int ro = 0; true and ro < rounds; ++ro)
         {
            auto start = std::chrono::steady_clock::now();
            for (int i = 0; i < count; ++i)
            {
               uint64_t val = rand64();
               key_view kstr((char*)&val, sizeof(val));
               tx->lower_bound(kstr);
            }
            auto end   = std::chrono::steady_clock::now();
            auto delta = end - start;

            std::cout << ro << "] " << std::setw(12)
                      << add_comma(int64_t(
                             (count) /
                             (std::chrono::duration<double, std::milli>(delta).count() / 1000)))
                      << "  rand lowerbound/sec  total items: " << add_comma(seq) << "\n";
         }

         auto read_thread = [&]() {};

         std::vector<std::unique_ptr<std::thread>> rthreads;
         rthreads.reserve(15);
         std::atomic<bool>     done = false;
         std::atomic<int64_t>  read_count;
         std::mutex            _lr_mutex;
         std::vector<uint64_t> inserted_numbers;
         //  inserted_numbers.resize(multithread_rounds * count);
         std::atomic<uint64_t> inserted_numbers_round(1);

         char data[data_size];

         for (uint32_t i = 0; i < rthreads.capacity(); ++i)
         {
            auto read_loop = [&]()
            {
               std::vector<char> buf;
               buf.resize(8);
               arbtrie::thread_name("read_thread");
               auto     rs    = db.start_read_session();
               int64_t  tc    = 0;
               int      cn    = 0;
               uint64_t round = inserted_numbers_round.load(std::memory_order_relaxed) * count;
               while (not done.load(std::memory_order_relaxed))
               {
                  int  roundc = 100000;
                  int  added  = 0;
                  auto rtx    = rs.start_caching_read_transaction(0);
                  for (int i = 0; i < batch_size; ++i)
                  {
                     ++added;
                     // uint64_t val = XXH64(&i, sizeof(i), 0);
                     uint64_t val = rand64();
                     //val          =numptrs inserted_numbers[val % round];
                     key_view kstr((char*)&val, sizeof(val));
                     if (rtx->valid())
                     {
                        rtx->lower_bound(kstr);
                        //                rtx->get(kstr, &buf);
                     }
                     if ((i & 0x4ff) == 0)
                     {
                        read_count.fetch_add(added, std::memory_order_relaxed);
                        added = 0;
                     }
                  }
                  read_count.fetch_add(added, std::memory_order_relaxed);
                  added = 0;
               }
            };
            rthreads.emplace_back(new std::thread(read_loop));
         }

         try
         {
            std::cout << "insert dense rand while reading " << rthreads.size()
                      << " threads  batch size: " << batch_size << " for " << multithread_rounds
                      << " rounds\n";
            for (int ro = 0; ro < multithread_rounds; ++ro)
            {
               auto start = std::chrono::steady_clock::now();
               for (int i = 0; i < count; ++i)
               {
                  uint64_t val                     = rand64();
                  inserted_numbers[ro * count + i] = val;
                  // auto     str = std::to_string(val);
                  ++seq;
                  key_view kstr((char*)&val, sizeof(val));
                  key_view data_key(data, data_size);
                  tx->insert(kstr, data_key);
                  if (i % batch_size == 0)
                     tx->commit_and_continue();
               }
               auto end   = std::chrono::steady_clock::now();
               auto delta = end - start;
               inserted_numbers_round.fetch_add(1, std::memory_order_relaxed);
               tx->commit_and_continue();

               std::cout << ro << "] " << std::setw(12)
                         << add_comma(int64_t(
                                (count) /
                                (std::chrono::duration<double, std::milli>(delta).count() / 1000)))
                         << " dense rand insert/sec  total items: " << add_comma(seq) << "    "
                         << add_comma(int64_t(
                                (read_count.exchange(0, std::memory_order_relaxed)) /
                                (std::chrono::duration<double, std::milli>(delta).count() / 1000)))
                         << "  lowerbound/sec \n";
            }
         }
         catch (const std::exception& e)
         {
            ARBTRIE_WARN("Caught Exception: ", e.what());
            done = true;
            for (auto& r : rthreads)
               r->join();
            throw;
         }

         done = true;
         for (auto& r : rthreads)
            r->join();

         auto find_all = [&](const int64_t start_idx)
         {
            ARBTRIE_WARN("find all from ", start_idx, " ", multithread_rounds,
                         " start_idx/multithread_rounds: ", start_idx / count);
            for (int ro = start_idx / count; ro < multithread_rounds; ++ro)
            {
               ARBTRIE_WARN(" find all from ", start_idx);
               auto start = std::chrono::steady_clock::now();
               for (int i = start_idx % count; i < count; ++i)
               {
                  uint64_t val = inserted_numbers[ro * multithread_rounds + i];
                  //auto     str = std::to_string(val);
                  key_view kstr((char*)&val, sizeof(val));
                  if (not tx->find(kstr))
                  {
                     ARBTRIE_ERROR("something broke: ", val, " ro: ", ro, " i: ", i,
                                   " start_idx: ", start_idx);
                     abort();
                     ro = multithread_rounds;
                     break;
                  }
                  /*
               auto result = tx->remove(kstr);
               if (result != 8)
               {
                  ARBTRIE_ERROR("something broke: ", result);
                  ro = multithread_rounds;
                  break;
               }
                  if (i % batch_size == 0)
                     tx->commit_and_continue();
               */
               }
               auto end   = std::chrono::steady_clock::now();
               auto delta = end - start;

               std::cout << ro << "] " << std::setw(12)
                         << add_comma(int64_t(
                                (count) /
                                (std::chrono::duration<double, std::milli>(delta).count() / 1000)))
                         << " dense rand find/sec  total found items: " << add_comma(count) << "\n";
            }
         };
         if (false)
         {
            find_all(0);
            for (int ro = 0; ro < multithread_rounds; ++ro)
            {
               ARBTRIE_WARN("init count: ", tx->count_keys());
               auto init_count = tx->count_keys();
               auto start      = std::chrono::steady_clock::now();
               for (int i = 0; i < count; ++i)
               {
                  //find_all(ro * count + i);
                  uint64_t val = inserted_numbers[ro * count + i];
                  //    ARBTRIE_WARN("removing ", val, " ro: ", ro, " i: ", i);
                  //auto     str = std::to_string(val);
                  key_view kstr((char*)&val, sizeof(val));
                  /*
               if (not tx->find(kstr))
               {
                  ARBTRIE_ERROR("something broke: ", val, " ro: ", ro, " i: ", i);
                  ro = multithread_rounds;
                  break;
               }
               if (not tx->find(kstr))
               {
                  ARBTRIE_ERROR("unable to find before remove: ", val, " ro: ", ro, " i: ", i);
                  abort();
                  ro = multithread_rounds;
                  break;
               }
               */
                  auto result = tx->remove(kstr);
                  if (result != 8)
                  {
                     ARBTRIE_ERROR("something broke: ", result, " val: ", val, " ro: ", ro,
                                   " i: ", i);
                     abort();
                     ro = multithread_rounds;
                     break;
                  }
               }
               auto end   = std::chrono::steady_clock::now();
               auto delta = end - start;
               if (tx->count_keys() != init_count - count)
               {
                  ARBTRIE_ERROR("something broke: ", tx->count_keys(), " init_count: ", init_count);
                  abort();
               }

               tx->commit_and_continue();

               std::cout << ro << "] " << std::setw(12)
                         << add_comma(int64_t(
                                (count) /
                                (std::chrono::duration<double, std::milli>(delta).count() / 1000)))
                         << " dense rand remove/sec  total items: " << add_comma(init_count - count)
                         << "\n";
            }
         }

         ARBTRIE_WARN("sleeping for 1 seconds");
         std::this_thread::sleep_for(std::chrono::seconds(1));
         db.print_stats(std::cout);

         if (sync_compact)
         {
            // NOTE: compact_next_segment() was removed from the database API
            // Compaction now happens automatically in the background
         }
      } while (false);
      /*
      if (false)
      {
         auto l = ws._segas->lock();
         print(l, r.address());
      }
      */

      //      auto l = ws._segas->lock();
      //      print(l, r.address());
      //
      std::cout << "wait for cleanup...\n";
      usleep(1000000 * 2);
      if (sync_compact)
      {
         // NOTE: compact_next_segment() was removed from the database API
         // Compaction now happens automatically in the background
      }
      // NOTE: stop_compact_thread() was removed from the database API
      // Compaction thread lifecycle is now managed internally by the database
   }
   catch (const std::exception& e)
   {
      ARBTRIE_WARN("Caught Exception: ", e.what());
   }
   return 0;
}

struct environ
{
   environ()
   {
      std::cout << "resetting database\n";
      std::filesystem::remove_all("arbtriedb");
      arbtrie::database::create("arbtriedb");
      db = new database("arbtriedb");
   }
   ~environ() { delete db; }
   arbtrie::database* db;
};

void load_words(write_session& ws, node_handle& root, uint64_t limit = -1)
{
   auto filename = "/usr/share/dict/words";

   {
      auto          start = std::chrono::steady_clock::now();
      std::ifstream file(filename);

      std::string key;
      std::string val;

      int  count    = 0;
      bool inserted = false;
      // Read the next line from File until it reaches the
      // end.
      while (file >> key)
      {
         val = key;
         toupper(val);
         ws.upsert(root, to_key_view(key), to_value_view(val));
         ws.get(root, to_key_view(key),
                [&](bool found, const value_type& r)
                {
                   if (key == "psych")
                   {
                      ARBTRIE_WARN("get ", key, " =  ", to_str(r.view()));
                      inserted = true;
                   }
                   assert(found);
                   assert(r.view() == to_value_view(val));
                });

         ++count;
         if (count > limit)
            break;
      }

      auto end   = std::chrono::steady_clock::now();
      auto delta = end - start;

      std::cout << "db loaded " << std::setw(12)
                << add_comma(int64_t(
                       (count) / (std::chrono::duration<double, std::milli>(delta).count() / 1000)))
                << " words/sec  total items: " << add_comma(count) << " from " << filename << "\n";
      usleep(1000000 * 3);
   }
}

void test_binary_node()
{
   environ env;
   {
      auto                       ws = env.db->start_write_session();
      std::optional<node_handle> last_root;
      auto                       cur_root = ws->create_root();
      //     auto                       state    = ws._segas->lock();
      ARBTRIE_DEBUG("upsert hello = world");

      ws->upsert(cur_root, to_key_view("hello"), to_value_view("world"));
      //     print(state, cur_root.address(), 1);
      ws->upsert(
          cur_root, to_key_view("long"),
          to_key_view("message                                                          ends"));

      //     print(state, cur_root.address(), 1);

      last_root = cur_root;

      std::cout << "root.........\n";
      //    print(state, cur_root.address(), 1);
      std::cout << "last_root.........\n";
      //     print(state, last_root->address(), 1);

      std::cout << "\n ========== inserting 'update' = 'world' ==========\n";
      ws->upsert(cur_root, to_key_view("update"),
                 to_value_view("long                                                      world"));

      std::cout << "root.........\n";
      //   print(state, cur_root.address(), 1);
      std::cout << "last_root.........\n";
      //   print(state, last_root->address(), 1);

      std::cout << "\n ========== releasing last_root ==========\n";
      last_root.reset();

      std::cout << "\n ========== inserting 'mayday' = 'help me, somebody' ==========\n";
      ws->upsert(cur_root, to_key_view("mayday"), to_value_view("help me, somebody"));
      //    print(state, cur_root.address(), 1);

      std::cout << "root.........\n";
      //  print(state, cur_root.address(), 1);
   }
   usleep(1000000);
   env.db->print_stats(std::cout);
}

void test_refactor()
{
   environ env;
   auto    ws = env.db->start_write_session();
   //   auto state = ws._segas->lock();

   {
      std::optional<node_handle> last_root;
      {
         auto cur_root = ws->create_root();

         for (int i = 0; i < 1000000; ++i)
         {
            //     std::cout << "==================   start upsert  " << i << "=====================\n";
            std::string v            = std::to_string(rand64());
            std::string value_buffer = v + "==============123456790=======" + v;
            ws->upsert(cur_root, to_key_view(v), to_value_view(value_buffer));
            // std::cout << " after upsert and set backup\n";
            last_root = cur_root;
            if (i >= 179)
            {
               //  print(state, cur_root.id(), 1);
            }
            //   std::cout << " free last root and save cur root\n";
            //   print(state, cur_root.id(), 1);
            /*
         std::cout << "======= set last root = cur_root  =================\n";
         std::cout << "before  cr.refs: " << cur_root.references() << "  id: " << cur_root.id()
                   << " \n";
         if (last_root)
            std::cout << "before  lr.refs: " << last_root->references()
                      << "  lr id: " << last_root->id() << "\n";
         last_root = cur_root;
         std::cout << "after  cr.refs: " << cur_root.references() << "  id: " << cur_root.id()
                   << " \n";
         std::cout << "after  lr.refs: " << last_root->references() << "  lr id: " << last_root->id()
                   << "\n";
         std::cout << "==================   post " << i << "=====================\n";
         print(state, cur_root.id(), 1);
         */
         }
         std::cout << "before release cur_root\n";
         //      print(state, cur_root.id(), 1);
         //      env.db->print_stats(std::cout);
      }
      std::cout << "before last root\n";
      if (last_root)
      {
         //      print(state, last_root->id(), 1);
         //      env.db->print_stats(std::cout);
      }
   }
   std::cout << "before exit after release all roots\n";
   // NOTE: compact_next_segment() was removed from the database API
   // Compaction now happens automatically in the background
   env.db->print_stats(std::cout);
}
