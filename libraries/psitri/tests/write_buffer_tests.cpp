#include <psitri/detail/write_buffer.hpp>
#include <catch2/catch_test_macros.hpp>
#include <map>
#include <random>
#include <string>
#include <vector>

using namespace psitri::detail;

// ── Basic operations ────────────────────────────────────────────────────────

TEST_CASE("write_buffer basic put and get", "[write_buffer]")
{
   write_buffer buf;

   buf.put("hello", "world", false);  // new key
   auto* e = buf.get("hello");
   REQUIRE(e != nullptr);
   REQUIRE(e->type == buffer_entry::insert);
   REQUIRE(e->value() == "world");
   REQUIRE(buf.delta_count() == 1);
   REQUIRE(buf.size() == 1);
}

TEST_CASE("write_buffer put existing key in persistent", "[write_buffer]")
{
   write_buffer buf;

   buf.put("key", "val", true);  // key exists in persistent tree
   auto* e = buf.get("key");
   REQUIRE(e != nullptr);
   REQUIRE(e->type == buffer_entry::update);
   REQUIRE(e->value() == "val");
   REQUIRE(buf.delta_count() == 0);  // update doesn't change count
}

TEST_CASE("write_buffer overwrite insert with new value", "[write_buffer]")
{
   write_buffer buf;

   buf.put("key", "val1", false);
   REQUIRE(buf.delta_count() == 1);

   buf.put("key", "val2", false);  // existed_in_persistent ignored — already in buffer
   auto* e = buf.get("key");
   REQUIRE(e->type == buffer_entry::insert);  // stays insert
   REQUIRE(e->value() == "val2");
   REQUIRE(buf.delta_count() == 1);  // unchanged
}

TEST_CASE("write_buffer overwrite update with new value", "[write_buffer]")
{
   write_buffer buf;

   buf.put("key", "val1", true);
   buf.put("key", "val2", true);
   auto* e = buf.get("key");
   REQUIRE(e->type == buffer_entry::update);  // stays update
   REQUIRE(e->value() == "val2");
   REQUIRE(buf.delta_count() == 0);  // unchanged
}

// ── Tombstone operations ────────────────────────────────────────────────────

TEST_CASE("write_buffer erase non-existent key not in persistent", "[write_buffer]")
{
   write_buffer buf;

   buf.erase("key", false);
   auto* e = buf.get("key");
   REQUIRE(e != nullptr);
   REQUIRE(e->type == buffer_entry::tombstone_noop);
   REQUIRE(e->is_tombstone());
   REQUIRE(buf.delta_count() == 0);  // no effect on count
}

TEST_CASE("write_buffer erase key in persistent", "[write_buffer]")
{
   write_buffer buf;

   buf.erase("key", true);
   auto* e = buf.get("key");
   REQUIRE(e != nullptr);
   REQUIRE(e->type == buffer_entry::tombstone);
   REQUIRE(e->is_tombstone());
   REQUIRE(buf.delta_count() == -1);
}

TEST_CASE("write_buffer erase insert -> tombstone_noop", "[write_buffer]")
{
   write_buffer buf;

   buf.put("key", "val", false);
   REQUIRE(buf.delta_count() == 1);

   buf.erase("key", false);
   auto* e = buf.get("key");
   REQUIRE(e->type == buffer_entry::tombstone_noop);
   REQUIRE(buf.delta_count() == 0);  // +1 - 1 = 0
}

TEST_CASE("write_buffer erase update -> tombstone", "[write_buffer]")
{
   write_buffer buf;

   buf.put("key", "val", true);
   REQUIRE(buf.delta_count() == 0);

   buf.erase("key", true);
   auto* e = buf.get("key");
   REQUIRE(e->type == buffer_entry::tombstone);
   REQUIRE(buf.delta_count() == -1);
}

TEST_CASE("write_buffer erase tombstone is no-op", "[write_buffer]")
{
   write_buffer buf;

   buf.erase("key", true);
   REQUIRE(buf.delta_count() == -1);

   buf.erase("key", true);  // double erase
   REQUIRE(buf.delta_count() == -1);  // unchanged
   REQUIRE(buf.size() == 1);
}

TEST_CASE("write_buffer erase tombstone_noop is no-op", "[write_buffer]")
{
   write_buffer buf;

   buf.erase("key", false);
   REQUIRE(buf.delta_count() == 0);

   buf.erase("key", false);
   REQUIRE(buf.delta_count() == 0);
   REQUIRE(buf.size() == 1);
}

// ── Tombstone -> data transitions ───────────────────────────────────────────

TEST_CASE("write_buffer put over tombstone -> update", "[write_buffer]")
{
   write_buffer buf;

   buf.erase("key", true);  // tombstone
   REQUIRE(buf.delta_count() == -1);

   buf.put("key", "restored", true);
   auto* e = buf.get("key");
   REQUIRE(e->type == buffer_entry::update);
   REQUIRE(e->value() == "restored");
   REQUIRE(buf.delta_count() == 0);  // -1 + 1 = 0
}

TEST_CASE("write_buffer put over tombstone_noop -> insert", "[write_buffer]")
{
   write_buffer buf;

   buf.erase("key", false);  // tombstone_noop
   REQUIRE(buf.delta_count() == 0);

   buf.put("key", "new", false);
   auto* e = buf.get("key");
   REQUIRE(e->type == buffer_entry::insert);
   REQUIRE(e->value() == "new");
   REQUIRE(buf.delta_count() == 1);
}

// ── Multiple keys ───────────────────────────────────────────────────────────

TEST_CASE("write_buffer multiple keys with mixed operations", "[write_buffer]")
{
   write_buffer buf;

   buf.put("aaa", "v1", false);   // insert: +1
   buf.put("bbb", "v2", true);    // update: 0
   buf.put("ccc", "v3", false);   // insert: +1
   buf.erase("ddd", true);        // tombstone: -1
   REQUIRE(buf.delta_count() == 1);
   REQUIRE(buf.size() == 4);

   REQUIRE(buf.get("aaa")->type == buffer_entry::insert);
   REQUIRE(buf.get("bbb")->type == buffer_entry::update);
   REQUIRE(buf.get("ccc")->type == buffer_entry::insert);
   REQUIRE(buf.get("ddd")->type == buffer_entry::tombstone);
   REQUIRE(buf.get("zzz") == nullptr);  // not in buffer
}

// ── Save / restore ──────────────────────────────────────────────────────────

TEST_CASE("write_buffer save/restore basic", "[write_buffer]")
{
   write_buffer buf;

   buf.put("key1", "val1", false);
   buf.put("key2", "val2", false);
   REQUIRE(buf.delta_count() == 2);

   auto saved = buf.save();
   buf.bump_generation();

   buf.put("key3", "val3", false);
   buf.erase("key1", false);
   REQUIRE(buf.delta_count() == 2);  // +3 inserts -1 erase = +2
   REQUIRE(buf.size() == 3);

   buf.restore(saved);
   REQUIRE(buf.delta_count() == 2);
   REQUIRE(buf.size() == 2);
   REQUIRE(buf.get("key1") != nullptr);
   REQUIRE(buf.get("key1")->value() == "val1");
   REQUIRE(buf.get("key2") != nullptr);
   REQUIRE(buf.get("key3") == nullptr);  // rolled back
}

TEST_CASE("write_buffer save/restore with tombstones", "[write_buffer]")
{
   write_buffer buf;

   buf.put("a", "1", false);
   buf.erase("b", true);
   REQUIRE(buf.delta_count() == 0);  // +1 - 1 = 0

   auto saved = buf.save();
   buf.bump_generation();

   buf.put("b", "restored", true);  // tombstone -> update
   buf.put("c", "new", false);
   REQUIRE(buf.delta_count() == 2);

   buf.restore(saved);
   REQUIRE(buf.delta_count() == 0);
   REQUIRE(buf.get("b")->is_tombstone());
   REQUIRE(buf.get("c") == nullptr);
}

TEST_CASE("write_buffer nested save/restore (2 levels)", "[write_buffer]")
{
   write_buffer buf;

   buf.put("base", "v0", false);
   auto s1 = buf.save();
   buf.bump_generation();

   buf.put("level1", "v1", false);
   auto s2 = buf.save();
   buf.bump_generation();

   buf.put("level2", "v2", false);
   REQUIRE(buf.delta_count() == 3);
   REQUIRE(buf.size() == 3);

   // Abort level 2
   buf.restore(s2);
   REQUIRE(buf.delta_count() == 2);
   REQUIRE(buf.get("level2") == nullptr);
   REQUIRE(buf.get("level1") != nullptr);

   // Continue at level 1 — write again after restore
   buf.put("level1b", "v1b", false);
   REQUIRE(buf.delta_count() == 3);

   // Abort level 1
   buf.restore(s1);
   REQUIRE(buf.delta_count() == 1);
   REQUIRE(buf.get("level1") == nullptr);
   REQUIRE(buf.get("level1b") == nullptr);
   REQUIRE(buf.get("base") != nullptr);
   REQUIRE(buf.get("base")->value() == "v0");
}

TEST_CASE("write_buffer save/restore preserves data integrity", "[write_buffer]")
{
   write_buffer buf;

   // Build up some state
   for (int i = 0; i < 100; ++i)
   {
      std::string key = "key" + std::to_string(i);
      std::string val = "val" + std::to_string(i);
      buf.put(key, val, false);
   }
   REQUIRE(buf.delta_count() == 100);

   auto saved = buf.save();
   buf.bump_generation();

   // Modify half the keys
   for (int i = 0; i < 50; ++i)
   {
      std::string key = "key" + std::to_string(i);
      buf.erase(key, false);
   }
   for (int i = 100; i < 150; ++i)
   {
      std::string key = "key" + std::to_string(i);
      std::string val = "newval" + std::to_string(i);
      buf.put(key, val, false);
   }
   REQUIRE(buf.delta_count() == 100);  // -50 + 50 = net 0 change

   // Restore
   buf.restore(saved);
   REQUIRE(buf.delta_count() == 100);

   // Verify all original keys are intact
   for (int i = 0; i < 100; ++i)
   {
      std::string key = "key" + std::to_string(i);
      std::string val = "val" + std::to_string(i);
      auto* e = buf.get(key);
      REQUIRE(e != nullptr);
      REQUIRE(e->type == buffer_entry::insert);
      REQUIRE(e->value() == val);
   }
   // New keys rolled back
   for (int i = 100; i < 150; ++i)
   {
      std::string key = "key" + std::to_string(i);
      REQUIRE(buf.get(key) == nullptr);
   }
}

// ── Sorted iteration ────────────────────────────────────────────────────────

TEST_CASE("write_buffer sorted iteration", "[write_buffer]")
{
   write_buffer buf;

   buf.put("cherry", "3", false);
   buf.put("apple", "1", false);
   buf.put("banana", "2", false);
   buf.erase("dragonfruit", true);

   std::vector<std::string> keys;
   for (auto it = buf.begin(); it != buf.end(); ++it)
      keys.push_back(std::string(it.key()));

   REQUIRE(keys.size() == 4);
   REQUIRE(keys[0] == "apple");
   REQUIRE(keys[1] == "banana");
   REQUIRE(keys[2] == "cherry");
   REQUIRE(keys[3] == "dragonfruit");
}

// ── Clear ───────────────────────────────────────────────────────────────────

TEST_CASE("write_buffer clear resets everything", "[write_buffer]")
{
   write_buffer buf;

   buf.put("a", "1", false);
   buf.put("b", "2", true);
   buf.erase("c", true);
   REQUIRE(buf.size() == 3);

   buf.clear();
   REQUIRE(buf.size() == 0);
   REQUIRE(buf.delta_count() == 0);
   REQUIRE(buf.empty());
   REQUIRE(buf.get("a") == nullptr);
}

// ── Fuzz test ───────────────────────────────────────────────────────────────

TEST_CASE("write_buffer fuzz test vs std::map oracle", "[write_buffer][fuzz]")
{
   // Oracle: std::map tracks expected state
   struct oracle_entry
   {
      buffer_entry::kind type;
      std::string        value;
   };
   std::map<std::string, oracle_entry> oracle;

   write_buffer buf;
   std::mt19937 rng(42);
   int32_t      expected_delta = 0;

   auto random_key = [&]()
   {
      int         n = rng() % 100;
      std::string k = "k";
      k += std::to_string(n);
      return k;
   };

   auto random_value = [&]()
   {
      int         n = rng() % 1000;
      std::string v = "v";
      v += std::to_string(n);
      return v;
   };

   auto validate = [&]()
   {
      REQUIRE(buf.delta_count() == expected_delta);
      REQUIRE(buf.size() == oracle.size());
      for (auto& [key, entry] : oracle)
      {
         auto* e = buf.get(key);
         REQUIRE(e != nullptr);
         REQUIRE(e->type == entry.type);
         if (e->is_data())
         {
            INFO("key=" << key << " expected=" << entry.value);
            REQUIRE(std::string(e->value()) == entry.value);
         }
      }
   };

   for (int iter = 0; iter < 5000; ++iter)
   {
      std::string key   = random_key();
      bool        is_put = (rng() % 3 != 0);  // 2/3 puts, 1/3 erases
      bool        existed_in_persistent = (rng() % 2 == 0);

      auto oracle_it = oracle.find(key);

      if (is_put)
      {
         std::string val = random_value();

         if (oracle_it != oracle.end())
         {
            auto old_kind = oracle_it->second.type;
            buffer_entry::kind new_kind;
            if (old_kind == buffer_entry::tombstone)
            {
               new_kind = buffer_entry::update;
               ++expected_delta;
            }
            else if (old_kind == buffer_entry::tombstone_noop)
            {
               new_kind = buffer_entry::insert;
               ++expected_delta;
            }
            else
               new_kind = old_kind;

            oracle_it->second = {new_kind, val};
         }
         else
         {
            auto kind = existed_in_persistent ? buffer_entry::update : buffer_entry::insert;
            if (kind == buffer_entry::insert)
               ++expected_delta;
            oracle[key] = {kind, val};
         }

         buf.put(key, val, existed_in_persistent);
      }
      else
      {
         if (oracle_it != oracle.end())
         {
            if (oracle_it->second.type != buffer_entry::tombstone &&
                oracle_it->second.type != buffer_entry::tombstone_noop)
            {
               if (oracle_it->second.type == buffer_entry::insert)
               {
                  oracle_it->second = {buffer_entry::tombstone_noop, ""};
                  --expected_delta;
               }
               else
               {
                  oracle_it->second = {buffer_entry::tombstone, ""};
                  --expected_delta;
               }
            }
         }
         else
         {
            auto kind =
                existed_in_persistent ? buffer_entry::tombstone : buffer_entry::tombstone_noop;
            if (kind == buffer_entry::tombstone)
               --expected_delta;
            oracle[key] = {kind, ""};
         }

         buf.erase(key, existed_in_persistent);
      }

      // Validate the key that was just modified
      auto* e = buf.get(key);
      auto& expected = oracle[key];
      INFO("iter=" << iter << " key=" << key << " is_put=" << is_put);
      REQUIRE(e != nullptr);
      REQUIRE(e->type == expected.type);
      if (e->is_data())
         REQUIRE(std::string(e->value()) == expected.value);
   }

   // Final full validation
   validate();
}

// ── art_map<buffer_entry, heap_arena> direct test ───────────────────────────

TEST_CASE("heap_arena art_map upsert_inline overwrite", "[write_buffer]")
{
   art::art_map<buffer_entry, art::heap_arena> m;

   // Insert with inline data
   buffer_entry e1{buffer_entry::insert, 4};
   m.upsert_inline("k6", e1, "v730");

   auto* got = m.get("k6");
   REQUIRE(got != nullptr);
   REQUIRE(got->data_len == 4);
   REQUIRE(got->value() == "v730");

   // Overwrite with tombstone (no inline data)
   buffer_entry e2{buffer_entry::tombstone, 0};
   m.upsert("k6", e2);

   got = m.get("k6");
   REQUIRE(got != nullptr);
   REQUIRE(got->data_len == 0);
   REQUIRE(got->value() == "");

   // Overwrite with new inline data
   buffer_entry e3{buffer_entry::update, 4};
   m.upsert_inline("k6", e3, "v828");

   got = m.get("k6");
   REQUIRE(got != nullptr);
   REQUIRE(got->data_len == 4);
   REQUIRE(std::string(got->value()) == "v828");
}

TEST_CASE("heap_arena art_map many inserts then overwrite", "[write_buffer]")
{
   art::art_map<buffer_entry, art::heap_arena> m;

   // Insert many keys to build up tree
   for (int i = 0; i < 100; ++i)
   {
      std::string key = "k" + std::to_string(i);
      std::string val = "v" + std::to_string(i * 10);
      buffer_entry e{buffer_entry::insert, static_cast<uint32_t>(val.size())};
      m.upsert_inline(key, e, val);
   }

   // Overwrite k6 with new value
   buffer_entry e{buffer_entry::update, 4};
   m.upsert_inline("k6", e, "XXXX");

   auto* got = m.get("k6");
   REQUIRE(got != nullptr);
   REQUIRE(got->data_len == 4);
   REQUIRE(std::string(got->value()) == "XXXX");
}

TEST_CASE("heap_arena free functions upsert_inline overwrite", "[write_buffer]")
{
   art::heap_arena ha;
   art::offset_t   root = art::null_offset;
   uint32_t         size = 0;

   // Insert many keys to build up tree with inner nodes
   for (int i = 0; i < 100; ++i)
   {
      std::string key = "k" + std::to_string(i);
      std::string val = "v" + std::to_string(i * 10);
      buffer_entry e{buffer_entry::insert, static_cast<uint32_t>(val.size())};
      auto [ptr, inserted] = art::upsert_inline<buffer_entry>(ha, root, key, e, val, 0);
      if (inserted)
         ++size;
   }

   REQUIRE(size == 100);

   // Overwrite k6 with new inline data
   buffer_entry e{buffer_entry::update, 4};
   auto [ptr, inserted] = art::upsert_inline<buffer_entry>(ha, root, "k6", e, "XXXX", 0);
   REQUIRE_FALSE(inserted);  // overwrite, not insert

   auto* got = art::get<buffer_entry>(ha, root, "k6");
   REQUIRE(got != nullptr);
   REQUIRE(got->data_len == 4);
   REQUIRE(std::string(got->value()) == "XXXX");
}

// ── Large save/restore ──────────────────────────────────────────────────────

TEST_CASE("write_buffer large save/restore (10K entries)", "[write_buffer]")
{
   write_buffer buf;

   for (int i = 0; i < 10000; ++i)
   {
      std::string key = "key" + std::to_string(i);
      std::string val = "val" + std::to_string(i);
      buf.put(key, val, (i % 3 == 0));
   }

   int32_t initial_delta = buf.delta_count();
   auto    saved         = buf.save();
   buf.bump_generation();

   // Mutate heavily
   for (int i = 0; i < 5000; ++i)
   {
      std::string key = "key" + std::to_string(i);
      buf.erase(key, (i % 3 == 0));
   }
   for (int i = 10000; i < 15000; ++i)
   {
      std::string key = "key" + std::to_string(i);
      std::string val = "newval" + std::to_string(i);
      buf.put(key, val, false);
   }

   // delta_count may or may not differ — check restored state instead

   buf.restore(saved);
   REQUIRE(buf.delta_count() == initial_delta);
   REQUIRE(buf.size() == 10000);

   // Spot check
   REQUIRE(buf.get("key0")->value() == "val0");
   REQUIRE(buf.get("key9999")->value() == "val9999");
   REQUIRE(buf.get("key10000") == nullptr);
}
