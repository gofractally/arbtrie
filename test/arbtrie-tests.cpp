#include <arbtrie/hash/xxhash.h>
#include <algorithm>
#include <arbtrie/binary_node.hpp>
#include <arbtrie/database.hpp>
#include <arbtrie/inner_node.hpp>
#include <arbtrie/iterator.hpp>
#include <arbtrie/mapping.hpp>
#include <arbtrie/node_handle.hpp>
#include <arbtrie/node_meta.hpp>
#include <arbtrie/rdtsc.hpp>
#include <arbtrie/value_node.hpp>
#include <cctype>      // for std::toupper
#include <filesystem>  // Add this for std::filesystem
#include <fstream>     // Add this for std::ifstream
#include <random>
#include <string>
#include <vector>

template <typename S, typename T>
S& operator<<(S& stream, const std::optional<T>& obj)
{
   if (obj)
      return stream << *obj;
   else
      return stream << "<nullopt>";
}

#include <catch2/catch_all.hpp>  // Use catch_all.hpp for v3

using namespace arbtrie;
using namespace std::literals::string_literals;

void toupper(std::string& s)
{
   for (auto& c : s)
      c = std::toupper(static_cast<unsigned char>(c));
}

struct environ
{
   environ()
   {
      std::cerr << "resetting database\n";
      std::filesystem::remove_all("arbtriedb");
      arbtrie::database::create("arbtriedb");
      db = new database("arbtriedb");
   }
   ~environ() { delete db; }
   arbtrie::database* db;
};

std::vector<std::string> load_words(write_transaction& ws, uint32_t limit = -1)
{
   auto                     filename = "/usr/share/dict/words";
   std::vector<std::string> result;
   auto                     start = std::chrono::steady_clock::now();
   std::ifstream            file(filename);

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
      val.resize(64);
      //    if (result.size() != ws.count_keys())
      //      ARBTRIE_WARN(key, " count_keys: ", ws.count_keys());
      REQUIRE(result.size() == ws.count_keys());

      result.push_back(key);
      ws.upsert(to_key_view(key), to_value_view(val));

      /*
         ws.get(root, key,
                [&](bool found, const value_type& r)
                {
                   assert(found);
                   assert(r.view() == val);
                });
                */

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
   usleep(1000000 * 2);
   return result;
}
void validate_refcount(session_rlock& state, id_address i, int c = 1);
void validate_refcount(session_rlock& state, id_address i, const auto* in, int c)
{
   in->visit_branches_with_br(
       [&](int br, id_address adr)
       {
          if (in->branch_region().to_int() != adr.region().to_int())
             throw std::runtime_error("region refcount violated");
          validate_refcount(state, adr, c);
       });
}
void validate_refcount(session_rlock& state, id_address i, const binary_node* inner, int) {}
void validate_refcount(session_rlock& state, id_address i, const value_node* inner, int) {}
void validate_refcount(session_rlock& state, id_address i, int c)
{
   if (i)
   {
      auto ref = state.get(i);
      REQUIRE(ref.ref() > 0);
      REQUIRE(ref.ref() <= c);
      cast_and_call(ref.header(), [&](const auto* ptr) { validate_refcount(state, i, ptr, c); });
   }
}

TEST_CASE("binary-node")
{
   alignas(64) char node_buffer[64 * 16];
   auto bn = new (node_buffer) binary_node(sizeof(node_buffer), id_address{}, clone_config{});
   ARBTRIE_DEBUG("capacity: ", bn->data_capacity());
   ARBTRIE_DEBUG("spare capacity: ", bn->spare_capacity());
   ARBTRIE_DEBUG("branch capacity: ", bn->_branch_cap);
   ARBTRIE_DEBUG("branches: ", bn->num_branches());
   ARBTRIE_WARN("reserving 8 branches");
   bn->reserve_branch_cap(8);
   ARBTRIE_DEBUG("capacity: ", bn->data_capacity());
   ARBTRIE_DEBUG("spare capacity: ", bn->spare_capacity());
   ARBTRIE_DEBUG("branch capacity: ", bn->_branch_cap);
   ARBTRIE_DEBUG("branches: ", bn->num_branches());

   auto idx = bn->lower_bound_idx(to_key_view("hello"));
   bn->insert(kv_index(idx), to_key_view("hello"), to_value_view("world"));

   ARBTRIE_DEBUG("capacity: ", bn->data_capacity());
   ARBTRIE_DEBUG("spare capacity: ", bn->spare_capacity());
   ARBTRIE_DEBUG("branch capacity: ", bn->_branch_cap);
   ARBTRIE_DEBUG("branches: ", bn->num_branches());
}

TEST_CASE("update-size")
{
   environ env;
   {
      auto ws = env.db->start_write_session();
      auto tx = ws->start_transaction();

      std::string big_value;

      auto old_key_size = tx.upsert(to_key_view("hello"), to_value_view("world"));
      REQUIRE(old_key_size == -1);
      old_key_size = tx.upsert(to_key_view("hello"), to_value_view("new world"));
      REQUIRE(old_key_size == 5);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view("the old world"));
      REQUIRE(old_key_size == -1);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view("world"));
      REQUIRE(old_key_size == 13);
      old_key_size = tx.remove(to_key_view("goodbye"));
      REQUIRE(old_key_size == 5);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      old_key_size = tx.remove(to_key_view("goodbye"));
      REQUIRE(old_key_size == 0);
      big_value.resize(10);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      big_value.resize(0);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == 10);
      big_value.resize(1000);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == 0);
      big_value.resize(500);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == 1000);
      big_value.resize(50);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == 500);
      big_value.resize(300);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == 50);
      old_key_size = tx.remove(to_key_view("goodbye"));
      REQUIRE(old_key_size == 300);

      big_value.resize(60);
      old_key_size = tx.upsert(to_key_view("afill"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      old_key_size = tx.upsert(to_key_view("bfill"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      old_key_size = tx.upsert(to_key_view("cfill"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      old_key_size = tx.upsert(to_key_view("dfill"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      old_key_size = tx.upsert(to_key_view("efill"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      old_key_size = tx.upsert(to_key_view("ffill"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      std::string key = "fill";
      for (int i = 0; i < 22; ++i)
      {
         old_key_size = tx.upsert(to_key_view(key), to_value_view(big_value));
         key += 'a';
      }

      big_value.resize(500);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      big_value.resize(50);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == 500);
      big_value.resize(300);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == 50);
      big_value.resize(50);
      /// this will should change a key that is currely a 4 byte ptr to an inline 50 bytes
      /// but the existing binary node is unable to accomodate the extra space
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == 300);

      env.db->print_stats(std::cerr);
   }
   env.db->print_stats(std::cerr);
}
TEST_CASE("update-size-shared")
{
   environ env;
   {
      auto ws = env.db->start_write_session();
      auto tx = ws->start_transaction();

      std::optional<node_handle> tmp;
      std::string                big_value;

      auto old_key_size = tx.upsert(to_key_view("hello"), to_value_view("world"));
      REQUIRE(old_key_size == -1);
      tmp          = tx.get_root();
      old_key_size = tx.upsert(to_key_view("hello"), to_value_view("new world"));
      REQUIRE(old_key_size == 5);
      tmp          = tx.get_root();
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view("the old world"));
      REQUIRE(old_key_size == -1);
      tmp          = tx.get_root();
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view("world"));
      REQUIRE(old_key_size == 13);
      tmp          = tx.get_root();
      old_key_size = tx.remove(to_key_view("goodbye"));
      REQUIRE(old_key_size == 5);
      tmp          = tx.get_root();
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      tmp          = tx.get_root();
      old_key_size = tx.remove(to_key_view("goodbye"));
      REQUIRE(old_key_size == 0);
      tmp = tx.get_root();
      big_value.resize(10);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      tmp = tx.get_root();
      big_value.resize(0);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == 10);
      tmp = tx.get_root();
      big_value.resize(1000);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == 0);
      tmp = tx.get_root();
      big_value.resize(500);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == 1000);
      tmp = tx.get_root();
      big_value.resize(50);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == 500);
      tmp = tx.get_root();
      big_value.resize(300);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == 50);
      tmp          = tx.get_root();
      old_key_size = tx.remove(to_key_view("goodbye"));
      REQUIRE(old_key_size == 300);
      tmp = tx.get_root();

      big_value.resize(60);
      old_key_size = tx.upsert(to_key_view("afill"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      old_key_size = tx.upsert(to_key_view("bfill"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      old_key_size = tx.upsert(to_key_view("cfill"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      old_key_size = tx.upsert(to_key_view("dfill"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      old_key_size = tx.upsert(to_key_view("efill"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      old_key_size = tx.upsert(to_key_view("ffill"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      std::string key = "fill";
      for (int i = 0; i < 22; ++i)
      {
         old_key_size = tx.upsert(to_key_view(key), to_value_view(big_value));
         key += 'a';
         tmp = tx.get_root();
      }

      big_value.resize(500);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == -1);
      tmp = tx.get_root();
      big_value.resize(50);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == 500);
      tmp = tx.get_root();
      big_value.resize(300);
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == 50);
      tmp = tx.get_root();
      big_value.resize(50);
      /// this will should change a key that is currely a 4 byte ptr to an inline 50 bytes
      /// but the existing binary node is unable to accomodate the extra space
      old_key_size = tx.upsert(to_key_view("goodbye"), to_value_view(big_value));
      REQUIRE(old_key_size == 300);
      tmp = tx.get_root();

      env.db->print_stats(std::cerr);
      ARBTRIE_WARN("resetting temp");
      tmp.reset();
      env.db->print_stats(std::cerr);
   }
   env.db->print_stats(std::cerr);
}

TEST_CASE("insert-words")
{
   auto          filename = "/usr/share/dict/words";
   std::ifstream file(filename);

   std::string              key;
   std::vector<std::string> keys;
   std::vector<std::string> values;
   keys.reserve(400'000);
   values.reserve(400'000);
   while (file >> key)
   {
      keys.push_back(key);
      //   values.push_back(key);
      //   toupper(values.back());
   }
   std::sort(keys.begin(), keys.end());
   values = keys;

   auto test_words = [&](bool shared)
   {
      environ env;
      auto    ws    = env.db->start_write_session();
      auto    tx    = ws->start_transaction();
      auto    start = std::chrono::steady_clock::now();

      int  count    = 0;
      bool inserted = false;
      for (int i = 0; i < keys.size(); ++i)
      {
         if (i == 2560)
            std::cerr << "break\n";

         REQUIRE(tx.count_keys() == i);
         tx.upsert(to_key_view(keys[i]), to_value_view(values[i]));
         auto buf = tx.get<std::string>(to_key_view(keys[i]));
         REQUIRE(buf);
         REQUIRE(*buf == values[i]);
      }
      for (int i = 0; i < keys.size(); ++i)
      {
         auto buf = tx.get<std::string>(to_key_view(keys[i]));
         REQUIRE(buf);
         REQUIRE(*buf == values[i]);
      }

      auto end   = std::chrono::steady_clock::now();
      auto delta = end - start;

      std::cout << "db loaded " << std::setw(12)
                << add_comma(
                       int64_t((keys.size()) /
                               (std::chrono::duration<double, std::milli>(delta).count() / 1000)))
                << " words/sec  total items: " << add_comma(keys.size()) << " from " << filename
                << "\n";

      auto iterate_all = [&]()
      {
         {
            uint64_t item_count = 0;

            std::vector<char> data;
            auto              start = std::chrono::steady_clock::now();
            auto              fkeys = keys.begin();
            tx.start();
            while (tx.next())
            {
               tx.key();
               assert(tx.key().size() < 1024);
               tx.value(data);
               assert(tx.key().size() == data.size());

               /*
               if( fkeys->size() != data.size() or
                   0 != memcmp( fkeys->data(), data.data(), data.size() ) ) {
                  ARBTRIE_WARN( "expected '", *fkeys, " got ", std::string(data.data(),data.size()) );
               }
               REQUIRE( fkeys->size() == data.size() );
               REQUIRE( 0 == memcmp( fkeys->data(), data.data(), data.size() ) );
               */

               ++item_count;
               ++fkeys;
            }
            auto end   = std::chrono::steady_clock::now();
            auto delta = end - start;
            std::cout << "iterated " << std::setw(12)
                      << add_comma(
                             int64_t(item_count) /
                             (std::chrono::duration<double, std::milli>(delta).count() / 1000))
                      << " items/sec  total items: " << add_comma(item_count) << "\n";
            REQUIRE(item_count == keys.size());
            start = std::chrono::steady_clock::now();

            int rcount = 0;
            tx.reverse_lower_bound();
            auto rkeys = keys.rbegin();
            while (not tx.is_rend())
            {
               assert(rkeys != keys.rend());
               REQUIRE(rkeys != keys.rend());
               //      ARBTRIE_WARN( "checking ", *rkeys );
               tx.value(data);
               /*
               if( rkeys->size() != data.size() or
                   0 != memcmp( rkeys->data(), data.data(), data.size() ) ) {
                  ARBTRIE_WARN( "count: ", rcount, " expected '", *rkeys, " got ", std::string(data.data(),data.size()) );
               }
               REQUIRE( rkeys->size() == data.size() );
               REQUIRE( 0 == memcmp( rkeys->data(), data.data(), data.size() ) );
               */

               //              ARBTRIE_DEBUG( rcount, "] itr.key: ", to_str(itr.key()), " = ", std::string_view(data.data(),data.size()) );
               REQUIRE(tx.key().size() == data.size());
               if (*rkeys == "zuccarino")
               {
                  ARBTRIE_WARN("break");
               }
               tx.prev();
               ++rcount;
               ++rkeys;
            }
            REQUIRE(rcount == keys.size());
            end   = std::chrono::steady_clock::now();
            delta = end - start;
            std::cout << "reverse iterated " << std::setw(12)
                      << add_comma(
                             int64_t(item_count) /
                             (std::chrono::duration<double, std::milli>(delta).count() / 1000))
                      << " items/sec  total items: " << add_comma(item_count) << "\n";
         }
      };
      iterate_all();
      std::optional<node_handle> shared_handle;
      if (shared)
         shared_handle = tx.get_root();
      ARBTRIE_WARN("removing for keys in order, shared: ", shared);
      auto cnt = tx.count_keys();
      REQUIRE(cnt == keys.size());
      for (int i = 0; i < keys.size(); ++i)
      {
         // ARBTRIE_DEBUG( "check before remove: ", keys[i], " i: ", i, " shared: ", shared );
         // ARBTRIE_DEBUG( "ws.count: ", ws->count_keys(root), " i: ", i );
         REQUIRE(cnt - i == tx.count_keys());
         auto buf = tx.get<std::string>(to_key_view(keys[i]));
         REQUIRE(buf);
         REQUIRE(*buf == values[i]);
         if (not buf)
         {
            ARBTRIE_WARN("looking before remove: ", keys[i]);
            abort();
         }

         //ARBTRIE_DEBUG( "before remove: ", keys[i] );
         tx.remove(to_key_view(keys[i]));
         //ARBTRIE_DEBUG( "after remove: ", keys[i] );
         /*{
         auto l = ws->_segas->lock();
         validate_refcount( l, root.address(), int(shared+1) );
         }
         */
         buf = tx.get<std::string>(to_key_view(keys[i]));
         REQUIRE(not buf);
         //ARBTRIE_DEBUG("checking remove: ", keys[i]);
      }
      REQUIRE(tx.count_keys() == 0);
      env.db->print_stats(std::cerr);
   };  // test_words

   // ARBTRIE_DEBUG( "load in file order" );
   ARBTRIE_DEBUG("forward file order unique");
   test_words(false);
   ARBTRIE_DEBUG("forward file order shared");
   test_words(true);
   ARBTRIE_DEBUG("load in reverse file order");
   std::reverse(keys.begin(), keys.end());
   std::reverse(values.begin(), values.end());
   ARBTRIE_DEBUG("remove reverse file order shared");
   test_words(true);
   ARBTRIE_DEBUG("remove reverse file order unique");
   test_words(false);
   ARBTRIE_DEBUG("load in random order shared");
   {
      auto rng = std::default_random_engine{};
      std::shuffle(keys.begin(), keys.end(), rng);
   }
   {
      auto rng = std::default_random_engine{};
      std::shuffle(values.begin(), values.end(), rng);
   }
   test_words(true);
   ARBTRIE_DEBUG("load in random order unique");
   test_words(false);
}

TEST_CASE("update")
{
   environ env;
   auto    ws = env.db->start_write_session();
   auto    tx = ws->start_transaction();

   tx.upsert(to_key_view("hello"), to_value_view("world"));
   tx.update(to_key_view("hello"), to_value_view("heaven"));
   auto val = tx.get<std::string>(to_key_view("hello"));
   REQUIRE(val);
   REQUIRE(*val == "heaven");

   tx.update(to_key_view("hello"), to_value_view("small"));
   val = tx.get<std::string>(to_key_view("hello"));
   REQUIRE(val);
   REQUIRE(*val == "small");

   tx.update(to_key_view("hello"), to_value_view("medium"));
   val = tx.get<std::string>(to_key_view("hello"));
   REQUIRE(val);
   REQUIRE(*val == "medium");

   tx.update(to_key_view("hello"),
             to_value_view(
                 "heaven is a great place to go! Let's get out of here. This line must be long."));
   val = tx.get<std::string>(to_key_view("hello"));
   REQUIRE(val);
   REQUIRE(*val == "heaven is a great place to go! Let's get out of here. This line must be long.");

   INFO("setting a short (inline) value over an existing non-inline value");
   tx.update(to_key_view("hello"), to_value_view("short"));

   SECTION("updating an inline value that is smaller than object id to big value")
   {
      tx.upsert(to_key_view("a"), to_value_view("a"));
      tx.update(to_key_view("a"),
                to_value_view("object_id is larger than 'a'.. what do we do here? This must be "
                              "longer than 63 bytes"));
   }

   env.db->print_stats(std::cerr);
   val = tx.get<std::string>(to_key_view("hello"));
   REQUIRE(val);
   REQUIRE(*val == "short");

   tx.abort();
   env.db->print_stats(std::cerr);
}

TEST_CASE("random-size-updates-shared")
{
   environ env;
   {
      auto ws = env.db->start_write_session();
      {
         auto tx    = ws->start_transaction();
         auto words = load_words(tx);

         std::optional<node_handle> tmp;
         std::string                data;
         std::vector<char>          result;
         auto                       rng = std::default_random_engine{};
         for (int i = 0; i < 910; ++i)
         {
            if (i == 909)
            {
               std::cerr << "break;\n";
            }
            auto idx = rng() % (words.size());
            data.resize(rng() % 250);

            auto initsize = tx.get_size(to_key_view(words[idx]));
            auto prevsize = tx.upsert(to_key_view(words[idx]), to_value_view(data));
            assert(initsize == prevsize);
            REQUIRE(initsize == prevsize);
            auto postsize = tx.get_size(to_key_view(words[idx]));
            REQUIRE(postsize == data.size());
            tmp = tx.get_root();
            //  if( i % 1000 == 0 ) {
            //     ARBTRIE_DEBUG( "i: ", i, " ", ws->count_ids_with_refs() );
            //  }
         }
         env.db->print_stats(std::cerr);
         ARBTRIE_DEBUG("references before release: ", ws->count_ids_with_refs());
      }
      ARBTRIE_DEBUG("references after release: ", ws->count_ids_with_refs());
      env.db->print_stats(std::cerr);
      REQUIRE(0 == ws->count_ids_with_refs());
   }
   // let the compactor catch up
   usleep(1000000 * 2);
   env.db->print_stats(std::cerr);
}

TEST_CASE("remove")
{
   environ env;
   auto    ws = env.db->start_write_session();
   ARBTRIE_DEBUG("references before start: ", ws->count_ids_with_refs());
   {
      write_transaction tx    = ws->start_transaction();
      auto              words = load_words(tx);

      // remove key that does not exist
      REQUIRE(tx.get_size(to_key_view("xcvbn")) == -1);
      auto r = tx.remove(to_key_view("xcvbn"));
      REQUIRE(r == -1);
      auto share = tx.get_root();
      r          = tx.remove(to_key_view("xcvbn"));
      REQUIRE(r == -1);
      ARBTRIE_DEBUG("references before release: ", ws->count_ids_with_refs());
   }
   ARBTRIE_DEBUG("references after release: ", ws->count_ids_with_refs());
   REQUIRE(ws->count_ids_with_refs() == 0);
}

TEST_CASE("subtree2")
{
   environ env;
   {
      auto ws = env.db->start_write_session();
      {
         auto tx = ws->start_transaction();

         // create test tree
         std::string big_value;
         tx.upsert(to_key_view("hello"), to_value_view("world"));
         tx.upsert(to_key_view("goodbye"), to_value_view("darkness"));
         auto& root = tx.root_handle();

         // insert subtree into empty tree
         auto empty = ws->start_transaction(-1);

         empty.upsert(to_key_view("subtree"), tx.get_root());
         REQUIRE(tx.root_handle().ref() == 2);  // tx, and value of subtree key
         auto r1 = empty.get_subtree(to_key_view("subtree"));
         REQUIRE(bool(r1));
         REQUIRE(tx.root_handle().ref() == 3);  // r1, root, and value of subtree key
         empty.remove(to_key_view("subtree"));
         REQUIRE(tx.root_handle().ref() == 2);  // r1 and root

         // insert subtree into tree with 1 value node,
         // this should split value node into a binary node with the root stored
         empty.upsert(to_key_view("one"), to_value_view("value"));
         empty.upsert(to_key_view("subtree"), tx.get_root());
         REQUIRE(root.ref() == 3);  // r1 and root, and value of subtree key
         auto r2 = empty.get_subtree(to_key_view("subtree"));
         REQUIRE(root.ref() == 4);  // r1, r2, and root, and value of subtree key
         empty.remove(to_key_view("subtree"));
         REQUIRE(root.ref() == 3);  // r1 r2 and root

         // insert subtree into tree with binary node
         big_value.resize(100);
         empty.upsert(to_key_view("big"), to_value_view(big_value));
         empty.upsert(to_key_view("big2"), to_value_view(big_value));
         empty.upsert(to_key_view("subtree"), node_handle(root));
         auto r3 = empty.get_subtree(to_key_view("subtree"));
         REQUIRE(root.ref() == 5);  // r1, r2, r3 and root, and value of subtree key
         empty.remove(to_key_view("subtree"));
         REQUIRE(root.ref() == 4);  // r1 r2 and root

         // refactor binary tree with subtree into radix node
         empty.upsert(to_key_view("subtree"), node_handle(root));
         big_value.resize(60);
         std::string key = "Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
         for (int i = 0; i < 50; ++i)
         {
            empty.upsert(to_key_view(key), to_value_view(big_value));
            key[0]++;
         }
         auto r4 = empty.get_subtree(to_key_view("subtree"));
         REQUIRE(root.ref() == 6);  // r1, r2, r3, r4 and root, and value of subtree key

         // split value node into binary tree
         empty.upsert(to_key_view("S"), node_handle(root));
         REQUIRE(root.ref() == 7);  // r1, r2, r3, r4, and root, and value of "subtree" and "S" key
         auto r5 = empty.get_subtree(to_key_view("S"));
         REQUIRE(root.ref() ==
                 8);  // r1, r2, r3, r4, r5 and root, and value of "subtree" and "S" key

         // insert into inner eof value
         empty.upsert(to_key_view(""), node_handle(root));
         REQUIRE(root.ref() ==
                 9);  // r1, r2, r3, r4, and root, and value of "subtree", "", and "S" key
         auto r6 = empty.get_subtree(to_key_view(""));
         REQUIRE(root.ref() ==
                 10);  // r1, r2, r3, r4, r5, r6 and root, and value of "subtree", "", and "S" key

         empty.upsert(to_key_view("start-with-data"), to_value_view("data"));
         empty.upsert(to_key_view("start-with-data"), node_handle(root));

         REQUIRE(
             root.ref() ==
             11);  // r1, r2, r3, r4, r5, r6 and root, and value of "subtree", "", "start-with-data", and "S" key
         empty.upsert(to_key_view("start-with-data"), to_value_view("release test"));
         REQUIRE(root.ref() ==
                 10);  // r1, r2, r3, r4, r5, r6 and root, and value of "subtree", ", and "S" key
         empty.upsert(to_key_view("start-with-data"), node_handle(root));
         empty.upsert(to_key_view("start-with-data"), node_handle(root));
         empty.upsert(to_key_view("start-with-data"), node_handle(root));
         REQUIRE(root.ref() ==
                 11);  // r1, r2, r3, r4, r5, r6 and root, and value of "subtree", ", and "S" key

         {
            std::vector<char> buf;
            empty.lower_bound();
            while (not empty.is_end())
            {
               if (empty.key() == "big")
                  std::cerr << "break big\n";

               std::cerr << '"' << to_str(empty.key()) << " = " << empty.subtree().is_valid()
                         << "\n";
               if (auto sitr = empty.subtree_iterator(); sitr.valid())
               {
                  sitr.start();
                  while (sitr.next())
                     std::cerr << "\t\t" << to_str(sitr.key()) << "\n";
               }
               empty.next();
            }
         }

         empty.abort();
         REQUIRE(root.ref() == 7);  // r1, r2, r3, r4, r5, r6 and root

         auto old_subtree = tx.upsert(to_key_view("version1"), node_handle(root));
         tx.upsert(to_key_view("goodbye"), to_value_view("evil"));
         auto v1 = tx.get_subtree(to_key_view("version1"));
         REQUIRE(bool(v1));
         std::vector<char> value;
         v1->get(to_key_view("goodbye"), &value);
         REQUIRE(tx.lower_bound(to_key_view("version1")));
         REQUIRE(tx.subtree().is_valid());

         ARBTRIE_DEBUG("output: ", std::string(value.data(), value.size()));
         // auto size    = ws->get( root, to_key_view("version1"), v1 );

         env.db->print_stats(std::cerr);
      }
      REQUIRE(ws->count_ids_with_refs() == 0);
   }
}

/**
 * Utilizing CPU ticks as a fast source of randomness
 * to determine whether to record a read or not... 
 */
TEST_CASE("rdtsc")
{
   int64_t counts[16];
   memset(counts, 0, sizeof(counts));
   for (int i = 0; i < 1000000; ++i)
   {
      counts[rdtsc() % 16]++;
   }
   /*
   for( int i = 0; i < 16; ++i ) {
      ARBTRIE_WARN( "counts[",i,"] = ", counts[i] );
   }
   auto x = rdtsc();
   auto h = XXH3_64bits(&x,sizeof(x));
   auto y = rdtsc();
   ARBTRIE_DEBUG( x );
   ARBTRIE_DEBUG( y );
   ARBTRIE_DEBUG( y - x );
   */
}

TEST_CASE("random-size-updates")
{
   environ env;
   {
      auto ws = env.db->start_write_session();
      {
         auto tx    = ws->start_transaction();
         auto words = load_words(tx);

         std::string       data;
         std::vector<char> result;
         auto              rng = std::default_random_engine{};
         for (int i = 0; i < 1000000; ++i)
         {
            auto idx = rng() % words.size();
            data.resize(rng() % 250);

            auto initsize = tx.get_size(to_key_view(words[idx]));
            auto prevsize = tx.upsert(to_key_view(words[idx]), to_value_view(data));
            assert(initsize == prevsize);
            REQUIRE(initsize == prevsize);
            auto postsize = tx.get_size(to_key_view(words[idx]));
            REQUIRE(postsize == data.size());
         }
         env.db->print_stats(std::cerr);
         ws->count_ids_with_refs();
      }
      REQUIRE(ws->count_ids_with_refs() == 0);
   }
   // let the compactor catch up
   usleep(1000000 * 2);
   env.db->print_stats(std::cerr);
}

TEST_CASE("recover")
{
   node_stats v1;
   node_stats v2;
   node_stats v3;
   node_stats v4;
   node_stats v5;
   environ    env;
   {
      auto ws = env.db->start_write_session();
      auto tx = ws->start_transaction();
      // No need for create_root() with transaction API
      load_words(tx);
      tx.commit_and_continue();
      // get_node_stats is on the session, not the transaction
      auto stats = v1 = ws->get_node_stats(tx.get_root());
      ARBTRIE_DEBUG("total nodes: ", stats.total_nodes());
      ARBTRIE_DEBUG("max-depth: ", stats.max_depth);
      ARBTRIE_DEBUG("avg-depth: ", stats.average_depth());
      ARBTRIE_DEBUG("total_size: ", stats.total_size() / double(MB), " MB");
   }

   ARBTRIE_WARN("RELOADING");
   delete env.db;
   env.db = new database("arbtriedb");
   {
      auto ws    = env.db->start_read_session();
      auto rt    = ws.start_transaction();
      auto stats = v2 = ws.get_node_stats(rt.get_root());
      REQUIRE(v2 == v1);
      ARBTRIE_DEBUG("total nodes: ", stats.total_nodes());
      ARBTRIE_DEBUG("max-depth: ", stats.max_depth);
      ARBTRIE_DEBUG("avg-depth: ", stats.average_depth());
      ARBTRIE_DEBUG("total_size: ", stats.total_size() / double(MB), " MB");
      for (int i = 0; i < num_types; ++i)
         ARBTRIE_DEBUG(node_type_names[i], " = ", stats.node_counts[i]);
   }
   env.db->recover();
   ARBTRIE_WARN("AFTER RECOVER");
   {
      auto ws    = env.db->start_write_session();
      auto tx    = ws->start_transaction();
      auto stats = v3 = ws->get_node_stats(tx.get_root());
      ARBTRIE_DEBUG("total nodes: ", stats.total_nodes());
      ARBTRIE_DEBUG("max-depth: ", stats.max_depth);
      ARBTRIE_DEBUG("avg-depth: ", stats.average_depth());
      ARBTRIE_DEBUG("total_size: ", stats.total_size() / double(MB), " MB");
      for (int i = 0; i < num_types; ++i)
         ARBTRIE_DEBUG(node_type_names[i], " = ", stats.node_counts[i]);
      REQUIRE(v3 == v1);
   }
   {
      ARBTRIE_WARN("INSERT 1 Million Rows");
      auto ws = env.db->start_write_session();
      auto tx = ws->start_transaction();
      for (uint64_t i = 0; i < 1000'000; ++i)
      {
         key_view kstr((char*)&i, sizeof(i));
         tx.insert(kstr, kstr);
      }
      tx.commit_and_continue();
      // get_node_stats is on the session, not the transaction
      auto stats = v4 = ws->get_node_stats(tx.get_root());
      ARBTRIE_DEBUG("total nodes: ", stats.total_nodes());
      ARBTRIE_DEBUG("max-depth: ", stats.max_depth);
      ARBTRIE_DEBUG("avg-depth: ", stats.average_depth());
      ARBTRIE_DEBUG("total_size: ", stats.total_size() / double(MB), " MB");
   }
   delete env.db;
   env.db = new database("arbtriedb");
   env.db->recover();
   ARBTRIE_WARN("AFTER RECOVER 2");
   {
      auto ws    = env.db->start_write_session();
      auto tx    = ws->start_transaction();
      auto stats = v5 = ws->get_node_stats(tx.get_root());
      ARBTRIE_DEBUG("total nodes: ", stats.total_nodes());
      ARBTRIE_DEBUG("max-depth: ", stats.max_depth);
      ARBTRIE_DEBUG("avg-depth: ", stats.average_depth());
      ARBTRIE_DEBUG("total_size: ", stats.total_size() / double(MB), " MB");
      for (int i = 0; i < num_types; ++i)
         ARBTRIE_DEBUG(node_type_names[i], " = ", stats.node_counts[i]);
      REQUIRE(v5 == v4);
   }
}

int64_t rand64()
{
   thread_local static std::mt19937 gen(rand());
   return (uint64_t(gen()) << 32) | gen();
}

TEST_CASE("dense-rand-insert")
{
   environ env;
   auto    ws = env.db->start_write_session();
   {
      auto tx = ws->start_transaction();

      for (int i = 0; i < 100000; i++)
      {
         if (i == 60)
         {
            ARBTRIE_WARN("i: ", i);
         }
         REQUIRE(tx.count_keys() == i);

         uint64_t val = rand64();
         key_view kstr((char*)&val, sizeof(val));
         tx.insert(kstr, kstr);

         auto value = tx.get<std::string>(kstr);
         if (!value)
         {
            ARBTRIE_WARN("unable to find key: ", val, " i:", i);
            assert(!"should have found key!");
         }
         REQUIRE(value);
      }
      tx.abort();
   }
   REQUIRE(ws->count_ids_with_refs() == 0);
}
