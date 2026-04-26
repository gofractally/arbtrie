#include <catch2/catch_all.hpp>
#include <map>
#include <random>
#include <string>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/tree_ops.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/value_type.hpp>

using namespace psitri;

constexpr int SCALE = 1;

namespace
{
   struct update_test_db
   {
      std::string                    dir;
      std::shared_ptr<database>      db;
      std::shared_ptr<write_session> ses;

      update_test_db(const std::string& name = "update_value_testdb")
          : dir(name)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir + "/data");
         db  = database::open(dir);
         ses = db->start_write_session();
      }
      ~update_test_db() { std::filesystem::remove_all(dir); }
   };

   struct temp_tree_edit
   {
      explicit temp_tree_edit(write_session& ses)
          : tx(ses.start_write_transaction(ses.create_temporary_tree()))
      {
      }

      void upsert(key_view key, value_view value) { tx.upsert(key, value); }
      int  remove(key_view key) { return tx.remove(key); }
      uint64_t remove_range(key_view lower, key_view upper)
      {
         return tx.remove_range(lower, upper);
      }
      int32_t get(key_view key, Buffer auto* buffer) const { return tx.get(key, buffer); }

      write_transaction tx;
   };

   temp_tree_edit start_temp_edit(update_test_db& t) { return temp_tree_edit(*t.ses); }
}  // namespace

// Reproduces crash: dense 3-byte keys pack a leaf to capacity, then
// updating a small inline value to a >64-byte value (stored as value_node)
// requires a new cline slot. Without the fix, add_address_ptr() asserts
// free_space() >= 4 on the full leaf.
TEST_CASE("update_value: inline to value_node on full leaf triggers split",
          "[update_value][split]")
{
   update_test_db t;
   auto           cur = start_temp_edit(t);

   std::map<std::string, std::string> oracle;
   std::mt19937                       rng(98765);

   const int OPS       = 5000 / SCALE;
   const int KEY_RANGE = 500;

   for (int op = 0; op < OPS; ++op)
   {
      int i      = rng() % KEY_RANGE;
      int action = rng() % 100;

      char key_buf[4];
      snprintf(key_buf, sizeof(key_buf), "%03d", i);
      std::string key(key_buf);

      if (action < 50)
      {
         // Value size sometimes crosses the 64-byte value_node threshold
         int         val_size = (rng() % 3 == 0) ? 100 + (rng() % 200) : 5 + (rng() % 20);
         std::string val(val_size, static_cast<char>('A' + (op % 26)));
         cur.upsert(to_key_view(key), to_value_view(val));
         oracle[key] = val;
      }
      else if (action < 85)
      {
         cur.remove(to_key_view(key));
         oracle.erase(key);
      }
      else
      {
         int j = std::min(i + 5, KEY_RANGE);
         char lo_buf[4], hi_buf[4];
         snprintf(lo_buf, sizeof(lo_buf), "%03d", i);
         snprintf(hi_buf, sizeof(hi_buf), "%03d", j);
         cur.remove_range(to_key_view(std::string(lo_buf)),
                          to_key_view(std::string(hi_buf)));
         for (int k = i; k < j; ++k)
         {
            char kbuf[4];
            snprintf(kbuf, sizeof(kbuf), "%03d", k);
            oracle.erase(std::string(kbuf));
         }
      }
   }

   // Verify oracle matches trie
   for (auto& [k, v] : oracle)
   {
      std::vector<uint8_t> buf;
      int32_t              r = cur.get(to_key_view(k), &buf);
      REQUIRE(r >= 0);
      REQUIRE(std::string(buf.begin(), buf.end()) == v);
   }
}
