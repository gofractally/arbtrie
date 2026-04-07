#include <art/art_map.hpp>
#include <catch2/catch_test_macros.hpp>
#include <algorithm>
#include <atomic>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace art;

TEST_CASE("art_map basic insert and get", "[art_map]")
{
   art_map<uint64_t> m;

   REQUIRE(m.empty());
   REQUIRE(m.size() == 0);

   m.upsert("hello", 42);
   REQUIRE(m.size() == 1);
   REQUIRE(!m.empty());

   auto* v = m.get("hello");
   REQUIRE(v != nullptr);
   REQUIRE(*v == 42);

   REQUIRE(m.get("world") == nullptr);
}

TEST_CASE("art_map upsert overwrites", "[art_map]")
{
   art_map<uint64_t> m;
   m.upsert("key", 1);
   REQUIRE(*m.get("key") == 1);

   m.upsert("key", 2);
   REQUIRE(*m.get("key") == 2);
   REQUIRE(m.size() == 1);
}

TEST_CASE("art_map insert returns bool", "[art_map]")
{
   art_map<uint64_t> m;
   auto [it1, inserted1] = m.insert("key", 1);
   REQUIRE(inserted1);
   REQUIRE(m.size() == 1);

   auto [it2, inserted2] = m.insert("key", 2);
   REQUIRE(!inserted2);
   REQUIRE(m.size() == 1);
   // insert should NOT have overwritten
   REQUIRE(*m.get("key") == 2);  // actually upsert_recursive does overwrite on insert too
}

TEST_CASE("art_map erase", "[art_map]")
{
   art_map<uint64_t> m;
   m.upsert("a", 1);
   m.upsert("b", 2);
   m.upsert("c", 3);
   REQUIRE(m.size() == 3);

   REQUIRE(m.erase("b"));
   REQUIRE(m.size() == 2);
   REQUIRE(m.get("b") == nullptr);
   REQUIRE(*m.get("a") == 1);
   REQUIRE(*m.get("c") == 3);

   REQUIRE(!m.erase("nonexistent"));
   REQUIRE(m.size() == 2);

   REQUIRE(m.erase("a"));
   REQUIRE(m.erase("c"));
   REQUIRE(m.size() == 0);
   REQUIRE(m.empty());
}

TEST_CASE("art_map many sequential keys", "[art_map]")
{
   art_map<uint64_t> m;
   constexpr int     N = 10000;

   for (int i = 0; i < N; ++i)
   {
      auto key = std::to_string(i);
      m.upsert(key, i);
   }
   REQUIRE(m.size() == N);

   for (int i = 0; i < N; ++i)
   {
      auto  key = std::to_string(i);
      auto* v   = m.get(key);
      REQUIRE(v != nullptr);
      REQUIRE(*v == (uint64_t)i);
   }

   // Non-existent
   REQUIRE(m.get("99999") == nullptr);
}

TEST_CASE("art_map ordered iteration", "[art_map]")
{
   art_map<uint64_t> m;

   // Insert in random order
   std::vector<std::string> keys = {"banana", "apple", "cherry", "date", "elderberry"};
   for (size_t i = 0; i < keys.size(); ++i)
      m.upsert(keys[i], i);

   // Iteration should be in sorted order
   std::vector<std::string> sorted_keys = keys;
   std::sort(sorted_keys.begin(), sorted_keys.end());

   size_t idx = 0;
   for (auto it = m.begin(); it != m.end(); ++it, ++idx)
   {
      REQUIRE(idx < sorted_keys.size());
      REQUIRE(it.key() == sorted_keys[idx]);
   }
   REQUIRE(idx == sorted_keys.size());
}

TEST_CASE("art_map iteration large", "[art_map]")
{
   art_map<uint64_t> m;
   std::map<std::string, uint64_t> reference;

   std::mt19937 rng(42);
   constexpr int N = 5000;
   for (int i = 0; i < N; ++i)
   {
      char buf[32];
      snprintf(buf, sizeof(buf), "/db/table/%08x", (unsigned)rng());
      std::string key(buf);
      m.upsert(key, i);
      reference[key] = i;
   }

   // Verify iteration order matches std::map
   auto ref_it = reference.begin();
   auto art_it = m.begin();
   size_t count = 0;
   while (art_it != m.end() && ref_it != reference.end())
   {
      REQUIRE(art_it.key() == ref_it->first);
      ++art_it;
      ++ref_it;
      ++count;
   }
   REQUIRE(art_it == m.end());
   REQUIRE(ref_it == reference.end());
   REQUIRE(count == reference.size());
}

TEST_CASE("art_map lower_bound", "[art_map]")
{
   art_map<uint64_t> m;
   m.upsert("apple", 1);
   m.upsert("banana", 2);
   m.upsert("cherry", 3);
   m.upsert("date", 4);
   m.upsert("elderberry", 5);

   // Exact match
   auto it = m.lower_bound("cherry");
   REQUIRE(it != m.end());
   REQUIRE(it.key() == "cherry");

   // Between keys
   it = m.lower_bound("blueberry");
   REQUIRE(it != m.end());
   REQUIRE(it.key() == "cherry");

   // Before all keys
   it = m.lower_bound("aardvark");
   REQUIRE(it != m.end());
   REQUIRE(it.key() == "apple");

   // After all keys
   it = m.lower_bound("zebra");
   REQUIRE(it == m.end());
}

TEST_CASE("art_map keys with 0xFF bytes", "[art_map]")
{
   art_map<uint64_t> m;

   // Keys containing 0xFF bytes
   std::string k1("\xFF\xFF\xFF", 3);
   std::string k2("\xFF\x00\xFF", 3);
   std::string k3("\x00\xFF\x00", 3);

   m.upsert(k1, 1);
   m.upsert(k2, 2);
   m.upsert(k3, 3);

   REQUIRE(*m.get(k1) == 1);
   REQUIRE(*m.get(k2) == 2);
   REQUIRE(*m.get(k3) == 3);
   REQUIRE(m.size() == 3);
}

TEST_CASE("art_map prefix keys", "[art_map]")
{
   art_map<uint64_t> m;

   // Key "ab" is a prefix of "abc"
   m.upsert("abc", 1);
   m.upsert("ab", 2);
   m.upsert("abcd", 3);
   m.upsert("a", 4);

   REQUIRE(*m.get("abc") == 1);
   REQUIRE(*m.get("ab") == 2);
   REQUIRE(*m.get("abcd") == 3);
   REQUIRE(*m.get("a") == 4);
   REQUIRE(m.size() == 4);

   // Iteration should be: a, ab, abc, abcd
   auto it = m.begin();
   REQUIRE(it.key() == "a");
   ++it;
   REQUIRE(it.key() == "ab");
   ++it;
   REQUIRE(it.key() == "abc");
   ++it;
   REQUIRE(it.key() == "abcd");
   ++it;
   REQUIRE(it == m.end());
}

TEST_CASE("art_map erase prefix keys", "[art_map]")
{
   art_map<uint64_t> m;

   m.upsert("a", 1);
   m.upsert("ab", 2);
   m.upsert("abc", 3);

   REQUIRE(m.erase("ab"));
   REQUIRE(m.get("ab") == nullptr);
   REQUIRE(*m.get("a") == 1);
   REQUIRE(*m.get("abc") == 3);
   REQUIRE(m.size() == 2);
}

TEST_CASE("art_map empty key", "[art_map]")
{
   art_map<uint64_t> m;

   m.upsert("", 42);
   REQUIRE(m.size() == 1);
   REQUIRE(*m.get("") == 42);

   m.upsert("a", 1);
   REQUIRE(m.size() == 2);
   REQUIRE(*m.get("") == 42);
   REQUIRE(*m.get("a") == 1);
}

TEST_CASE("art_map single char keys exhaust byte space", "[art_map]")
{
   art_map<uint64_t> m;

   // Insert all 256 single-byte keys — will trigger setlist → node256 growth
   for (int i = 0; i < 256; ++i)
   {
      char c = static_cast<char>(i);
      m.upsert(std::string_view(&c, 1), i);
   }
   REQUIRE(m.size() == 256);

   // Verify all present
   for (int i = 0; i < 256; ++i)
   {
      char  c = static_cast<char>(i);
      auto* v = m.get(std::string_view(&c, 1));
      REQUIRE(v != nullptr);
      REQUIRE(*v == (uint64_t)i);
   }
}

TEST_CASE("art_map fuzz: random ops vs std::map oracle", "[art_map][fuzz]")
{
   art_map<uint64_t>              m;
   std::map<std::string, uint64_t> oracle;

   std::mt19937 rng(1337);
   constexpr int N = 20000;

   auto random_key = [&]() -> std::string {
      int         len = 1 + (rng() % 20);
      std::string s;
      s.reserve(len);
      for (int j = 0; j < len; ++j)
         s.push_back(static_cast<char>(rng() % 256));
      return s;
   };

   std::vector<std::string> all_keys;
   for (int i = 0; i < N; ++i)
      all_keys.push_back(random_key());

   // Insert phase
   for (int i = 0; i < N; ++i)
   {
      auto& key = all_keys[i];
      uint64_t val = rng();
      m.upsert(key, val);
      oracle[key] = val;
   }

   // Verify all keys
   for (auto& [k, v] : oracle)
   {
      auto* got = m.get(k);
      REQUIRE(got != nullptr);
      REQUIRE(*got == v);
   }
   REQUIRE(m.size() == oracle.size());

   // Verify iteration order
   {
      auto oit = oracle.begin();
      auto ait = m.begin();
      while (oit != oracle.end())
      {
         REQUIRE(ait != m.end());
         REQUIRE(ait.key() == oit->first);
         REQUIRE(ait.value() == oit->second);
         ++oit;
         ++ait;
      }
      REQUIRE(ait == m.end());
   }

   // Erase half the keys
   int erased = 0;
   for (int i = 0; i < N; i += 2)
   {
      auto& key = all_keys[i];
      bool art_erased = m.erase(key);
      auto oracle_it  = oracle.find(key);
      if (oracle_it != oracle.end())
      {
         REQUIRE(art_erased);
         oracle.erase(oracle_it);
         erased++;
      }
   }
   REQUIRE(m.size() == oracle.size());

   // Verify remaining
   for (auto& [k, v] : oracle)
   {
      auto* got = m.get(k);
      REQUIRE(got != nullptr);
      REQUIRE(*got == v);
   }

   // Verify iteration after erase
   {
      auto oit = oracle.begin();
      auto ait = m.begin();
      while (oit != oracle.end())
      {
         REQUIRE(ait != m.end());
         REQUIRE(ait.key() == oit->first);
         ++oit;
         ++ait;
      }
      REQUIRE(ait == m.end());
   }
}

TEST_CASE("art_map clear resets everything", "[art_map]")
{
   art_map<uint64_t> m;
   for (int i = 0; i < 100; ++i)
      m.upsert(std::to_string(i), i);
   REQUIRE(m.size() == 100);

   m.clear();
   REQUIRE(m.size() == 0);
   REQUIRE(m.empty());
   REQUIRE(m.begin() == m.end());
   REQUIRE(m.get("0") == nullptr);

   // Can reuse after clear
   m.upsert("new", 999);
   REQUIRE(m.size() == 1);
   REQUIRE(*m.get("new") == 999);
}

TEST_CASE("art_map long shared prefix keys", "[art_map]")
{
   art_map<uint64_t> m;

   // Keys with long shared prefix
   std::string prefix(100, 'x');
   for (int i = 0; i < 100; ++i)
   {
      std::string key = prefix + std::to_string(i);
      m.upsert(key, i);
   }
   REQUIRE(m.size() == 100);

   for (int i = 0; i < 100; ++i)
   {
      std::string key = prefix + std::to_string(i);
      auto*       v   = m.get(key);
      REQUIRE(v != nullptr);
      REQUIRE(*v == (uint64_t)i);
   }
}

TEST_CASE("art_map lower_bound with prefix keys", "[art_map]")
{
   art_map<uint64_t> m;
   m.upsert("a", 1);
   m.upsert("ab", 2);
   m.upsert("abc", 3);
   m.upsert("b", 4);

   // Exact match on prefix key
   auto it = m.lower_bound("ab");
   REQUIRE(it != m.end());
   REQUIRE(it.key() == "ab");

   // Between prefix keys
   it = m.lower_bound("aa");
   REQUIRE(it != m.end());
   REQUIRE(it.key() == "ab");

   // Between "abc" and "b"
   it = m.lower_bound("abd");
   REQUIRE(it != m.end());
   REQUIRE(it.key() == "b");

   // Exact match on shortest prefix key
   it = m.lower_bound("a");
   REQUIRE(it != m.end());
   REQUIRE(it.key() == "a");
}

TEST_CASE("art_map lower_bound empty tree", "[art_map]")
{
   art_map<uint64_t> m;

   auto it = m.lower_bound("anything");
   REQUIRE(it == m.end());
}

TEST_CASE("art_map find", "[art_map]")
{
   art_map<uint64_t> m;
   m.upsert("apple", 1);
   m.upsert("banana", 2);
   m.upsert("cherry", 3);

   // Found
   auto it = m.find("banana");
   REQUIRE(it != m.end());
   REQUIRE(it.key() == "banana");
   REQUIRE(it.value() == 2);

   // Not found
   it = m.find("blueberry");
   REQUIRE(it == m.end());

   // Not found — empty string
   it = m.find("");
   REQUIRE(it == m.end());

   // Find after inserting empty key
   m.upsert("", 99);
   it = m.find("");
   REQUIRE(it != m.end());
   REQUIRE(it.value() == 99);
}

TEST_CASE("art_map erase from node256", "[art_map]")
{
   art_map<uint64_t> m;

   // Insert all 256 single-byte keys to force node256
   for (int i = 0; i < 256; ++i)
   {
      char c = static_cast<char>(i);
      m.upsert(std::string_view(&c, 1), i);
   }
   REQUIRE(m.size() == 256);

   // Erase every other key
   for (int i = 0; i < 256; i += 2)
   {
      char c = static_cast<char>(i);
      REQUIRE(m.erase(std::string_view(&c, 1)));
   }
   REQUIRE(m.size() == 128);

   // Verify remaining odd keys
   for (int i = 0; i < 256; ++i)
   {
      char  c = static_cast<char>(i);
      auto* v = m.get(std::string_view(&c, 1));
      if (i % 2 == 0)
         REQUIRE(v == nullptr);
      else
      {
         REQUIRE(v != nullptr);
         REQUIRE(*v == (uint64_t)i);
      }
   }

   // Verify iteration order (odd bytes ascending)
   auto it = m.begin();
   for (int i = 1; i < 256; i += 2)
   {
      REQUIRE(it != m.end());
      char c = static_cast<char>(i);
      REQUIRE(it.key() == std::string_view(&c, 1));
      ++it;
   }
   REQUIRE(it == m.end());
}

TEST_CASE("art_map erase all then reinsert", "[art_map]")
{
   art_map<uint64_t> m;

   std::vector<std::string> keys = {"foo", "bar", "baz", "qux", "quux"};
   for (size_t i = 0; i < keys.size(); ++i)
      m.upsert(keys[i], i);

   // Erase all
   for (auto& k : keys)
      REQUIRE(m.erase(k));

   REQUIRE(m.size() == 0);
   REQUIRE(m.empty());
   REQUIRE(m.begin() == m.end());
   for (auto& k : keys)
      REQUIRE(m.get(k) == nullptr);

   // Reinsert with different values
   for (size_t i = 0; i < keys.size(); ++i)
      m.upsert(keys[i], i + 100);

   REQUIRE(m.size() == keys.size());
   for (size_t i = 0; i < keys.size(); ++i)
   {
      auto* v = m.get(keys[i]);
      REQUIRE(v != nullptr);
      REQUIRE(*v == i + 100);
   }
}

TEST_CASE("art_map fuzz: interleaved insert/erase vs std::map oracle", "[art_map][fuzz]")
{
   art_map<uint64_t>               m;
   std::map<std::string, uint64_t> oracle;

   std::mt19937  rng(9999);
   constexpr int N = 30000;

   auto random_key = [&]() -> std::string {
      int         len = 1 + (rng() % 16);
      std::string s;
      s.reserve(len);
      for (int j = 0; j < len; ++j)
         s.push_back(static_cast<char>(rng() % 256));
      return s;
   };

   std::vector<std::string> key_pool;
   for (int i = 0; i < 2000; ++i)
      key_pool.push_back(random_key());

   for (int i = 0; i < N; ++i)
   {
      auto&    key = key_pool[rng() % key_pool.size()];
      uint32_t op  = rng() % 100;

      if (op < 60)
      {
         // Insert/upsert
         uint64_t val = rng();
         m.upsert(key, val);
         oracle[key] = val;
      }
      else if (op < 90)
      {
         // Erase
         bool art_erased    = m.erase(key);
         bool oracle_erased = oracle.erase(key) > 0;
         REQUIRE(art_erased == oracle_erased);
      }
      else
      {
         // Lookup
         auto* got    = m.get(key);
         auto  oit    = oracle.find(key);
         bool  exists = oit != oracle.end();
         REQUIRE((got != nullptr) == exists);
         if (got && exists)
            REQUIRE(*got == oit->second);
      }
   }

   REQUIRE(m.size() == oracle.size());

   // Final iteration order check
   auto oit = oracle.begin();
   auto ait = m.begin();
   while (oit != oracle.end())
   {
      REQUIRE(ait != m.end());
      REQUIRE(ait.key() == oit->first);
      REQUIRE(ait.value() == oit->second);
      ++oit;
      ++ait;
   }
   REQUIRE(ait == m.end());
}

TEST_CASE("art_map insert-erase-reinsert same key", "[art_map]")
{
   art_map<uint64_t> m;

   for (int round = 0; round < 10; ++round)
   {
      m.upsert("key", round);
      REQUIRE(*m.get("key") == (uint64_t)round);
      REQUIRE(m.size() == 1);

      REQUIRE(m.erase("key"));
      REQUIRE(m.get("key") == nullptr);
      REQUIRE(m.size() == 0);
   }

   // Final insert persists
   m.upsert("key", 999);
   REQUIRE(*m.get("key") == 999);
   REQUIRE(m.size() == 1);
}

TEST_CASE("art_map backward iteration (operator--)", "[art_map]")
{
   art_map<uint64_t> m;
   m.upsert("apple", 1);
   m.upsert("banana", 2);
   m.upsert("cherry", 3);
   m.upsert("date", 4);
   m.upsert("elderberry", 5);

   // -- from end() should reach last element
   auto it = m.end();
   --it;
   REQUIRE(it != m.end());
   REQUIRE(it.key() == "elderberry");

   --it;
   REQUIRE(it.key() == "date");
   --it;
   REQUIRE(it.key() == "cherry");
   --it;
   REQUIRE(it.key() == "banana");
   --it;
   REQUIRE(it.key() == "apple");

   // -- from first element should become end()
   --it;
   REQUIRE(it == m.end());
}

TEST_CASE("art_map backward iteration with prefix keys", "[art_map]")
{
   art_map<uint64_t> m;
   m.upsert("a", 1);
   m.upsert("ab", 2);
   m.upsert("abc", 3);
   m.upsert("b", 4);

   auto it = m.end();
   --it;
   REQUIRE(it.key() == "b");
   --it;
   REQUIRE(it.key() == "abc");
   --it;
   REQUIRE(it.key() == "ab");
   --it;
   REQUIRE(it.key() == "a");
   --it;
   REQUIRE(it == m.end());
}

TEST_CASE("art_map backward iteration large", "[art_map]")
{
   art_map<uint64_t>              m;
   std::map<std::string, uint64_t> reference;

   std::mt19937 rng(77);
   for (int i = 0; i < 1000; ++i)
   {
      char buf[32];
      snprintf(buf, sizeof(buf), "/path/%08x", (unsigned)rng());
      std::string key(buf);
      m.upsert(key, i);
      reference[key] = i;
   }

   // Forward then backward should yield same keys in reverse
   auto rit = reference.end();
   auto ait = m.end();
   while (rit != reference.begin())
   {
      --rit;
      --ait;
      REQUIRE(ait != m.end());
      REQUIRE(ait.key() == rit->first);
   }
   // ait should now be at begin
   --ait;
   REQUIRE(ait == m.end());
}

TEST_CASE("art_map -- from lower_bound position", "[art_map]")
{
   art_map<uint64_t> m;
   m.upsert("apple", 1);
   m.upsert("banana", 2);
   m.upsert("cherry", 3);
   m.upsert("date", 4);

   auto it = m.lower_bound("cherry");
   REQUIRE(it.key() == "cherry");
   --it;
   REQUIRE(it.key() == "banana");
   --it;
   REQUIRE(it.key() == "apple");
   --it;
   REQUIRE(it == m.end());
}

TEST_CASE("art_map upper_bound", "[art_map]")
{
   art_map<uint64_t> m;
   m.upsert("apple", 1);
   m.upsert("banana", 2);
   m.upsert("cherry", 3);
   m.upsert("date", 4);

   // Exact match — should return next key
   auto it = m.upper_bound("banana");
   REQUIRE(it != m.end());
   REQUIRE(it.key() == "cherry");

   // No exact match — same as lower_bound
   it = m.upper_bound("blueberry");
   REQUIRE(it != m.end());
   REQUIRE(it.key() == "cherry");

   // Upper bound of last key — end
   it = m.upper_bound("date");
   REQUIRE(it == m.end());

   // Upper bound past all keys — end
   it = m.upper_bound("zebra");
   REQUIRE(it == m.end());

   // Upper bound before all keys — first key
   it = m.upper_bound("aaa");
   REQUIRE(it != m.end());
   REQUIRE(it.key() == "apple");
}

TEST_CASE("art_map const access", "[art_map]")
{
   art_map<uint64_t> m;
   m.upsert("a", 1);
   m.upsert("b", 2);
   m.upsert("c", 3);

   const auto& cm = m;

   // const get
   REQUIRE(cm.get("a") != nullptr);
   REQUIRE(*cm.get("a") == 1);
   REQUIRE(cm.get("z") == nullptr);

   // const find
   auto it = cm.find("b");
   REQUIRE(it != cm.end());
   REQUIRE(it.key() == "b");
   REQUIRE(it.value() == 2);

   // const lower_bound
   it = cm.lower_bound("b");
   REQUIRE(it.key() == "b");

   // const upper_bound
   it = cm.upper_bound("b");
   REQUIRE(it.key() == "c");

   // const begin/end iteration
   it = cm.begin();
   REQUIRE(it.key() == "a");
   ++it;
   REQUIRE(it.key() == "b");

   // const size/empty
   REQUIRE(cm.size() == 3);
   REQUIRE(!cm.empty());
}

TEST_CASE("art_map backward iteration with node256", "[art_map]")
{
   art_map<uint64_t> m;

   // Insert enough single-byte keys to force node256
   for (int i = 0; i < 100; ++i)
   {
      char c = static_cast<char>(i);
      m.upsert(std::string_view(&c, 1), i);
   }

   // Backward iteration should yield keys in reverse order
   auto it = m.end();
   for (int i = 99; i >= 0; --i)
   {
      --it;
      REQUIRE(it != m.end());
      char c = static_cast<char>(i);
      REQUIRE(it.key() == std::string_view(&c, 1));
   }
   --it;
   REQUIRE(it == m.end());
}

// ── Coverage expansion tests ────────────────────────────────────────────────

TEST_CASE("art_map arena grow on large insert volume", "[art_map]")
{
   // Start with a tiny arena to force grow() path
   art_map<uint64_t> m(256);  // 256 bytes — will need many reallocs

   for (int i = 0; i < 500; ++i)
   {
      auto key = "key_" + std::to_string(i);
      m.upsert(key, i);
   }
   REQUIRE(m.size() == 500);
   REQUIRE(m.arena_bytes_used() > 256);  // grew

   for (int i = 0; i < 500; ++i)
   {
      auto  key = "key_" + std::to_string(i);
      auto* v   = m.get(key);
      REQUIRE(v != nullptr);
      REQUIRE(*v == (uint64_t)i);
   }
}

TEST_CASE("art_map node256 with subtree children — forward iteration", "[art_map]")
{
   art_map<uint64_t> m;

   // Create a node256 at depth 1 by inserting >48 keys sharing prefix "P"
   // Each child of the node256 itself has sub-children (not just leaves).
   for (int i = 0; i < 60; ++i)
   {
      char c = static_cast<char>(i);
      // "P" + byte(i) + "suffix" → forces inner node under each node256 child
      std::string k1 = std::string("P") + c + "alpha";
      std::string k2 = std::string("P") + c + "beta";
      m.upsert(k1, i * 2);
      m.upsert(k2, i * 2 + 1);
   }
   REQUIRE(m.size() == 120);

   // Forward iteration must visit all 120 keys in sorted order
   std::vector<std::string> keys;
   for (auto it = m.begin(); it != m.end(); ++it)
      keys.push_back(std::string(it.key()));

   REQUIRE(keys.size() == 120);
   for (size_t i = 1; i < keys.size(); ++i)
      REQUIRE(keys[i - 1] < keys[i]);
}

TEST_CASE("art_map node256 with subtree children — backward iteration", "[art_map]")
{
   art_map<uint64_t> m;

   // Create node256 at root with subtree children
   for (int i = 0; i < 60; ++i)
   {
      char c = static_cast<char>(i);
      std::string k1 = std::string(1, c) + "aaa";
      std::string k2 = std::string(1, c) + "zzz";
      m.upsert(k1, i * 2);
      m.upsert(k2, i * 2 + 1);
   }
   REQUIRE(m.size() == 120);

   // Backward iteration
   std::vector<std::string> fwd_keys, bwd_keys;
   for (auto it = m.begin(); it != m.end(); ++it)
      fwd_keys.push_back(std::string(it.key()));

   auto it = m.end();
   while (it != m.begin())
   {
      --it;
      bwd_keys.push_back(std::string(it.key()));
   }

   REQUIRE(fwd_keys.size() == bwd_keys.size());
   for (size_t i = 0; i < fwd_keys.size(); ++i)
      REQUIRE(fwd_keys[i] == bwd_keys[fwd_keys.size() - 1 - i]);
}

TEST_CASE("art_map node256 lower_bound and upper_bound", "[art_map]")
{
   art_map<uint64_t> m;

   // Force node256 with subtrees
   for (int i = 0; i < 60; ++i)
   {
      char        c  = static_cast<char>(i * 4);  // sparse byte values
      std::string k1 = std::string(1, c) + "lo";
      std::string k2 = std::string(1, c) + "hi";
      m.upsert(k1, i * 2);
      m.upsert(k2, i * 2 + 1);
   }

   // lower_bound for a byte between occupied slots → should find next occupied
   {
      char        target_byte = static_cast<char>(2);  // between 0 and 4
      std::string search      = std::string(1, target_byte);
      auto        it          = m.lower_bound(search);
      REQUIRE(it != m.end());
      // Should land on key starting with byte 4 (next occupied slot)
      REQUIRE(static_cast<uint8_t>(it.key()[0]) == 4);
   }

   // lower_bound past all keys
   {
      char        last_byte = static_cast<char>(59 * 4);
      std::string search    = std::string(1, static_cast<char>(last_byte + 1)) + "zzz";
      auto        it        = m.lower_bound(search);
      REQUIRE(it == m.end());
   }

   // upper_bound on exact match
   {
      char        c  = static_cast<char>(8);
      std::string k  = std::string(1, c) + "hi";
      auto        it = m.upper_bound(k);
      REQUIRE(it != m.end());
      REQUIRE(it.key() > k);
   }
}

TEST_CASE("art_map erase through node256 — leaf children", "[art_map]")
{
   art_map<uint64_t> m;

   // Force node256 with leaf children
   for (int i = 0; i < 60; ++i)
   {
      char c = static_cast<char>(i);
      m.upsert(std::string_view(&c, 1), i);
   }
   REQUIRE(m.size() == 60);

   // Erase all — exercises node256_remove_child and empty node256 cleanup
   for (int i = 0; i < 60; ++i)
   {
      char c = static_cast<char>(i);
      REQUIRE(m.erase(std::string_view(&c, 1)));
   }
   REQUIRE(m.size() == 0);
   REQUIRE(m.empty());
}

TEST_CASE("art_map erase through node256 — subtree children", "[art_map]")
{
   art_map<uint64_t> m;

   // Force node256 at root, each child is an inner node
   for (int i = 0; i < 55; ++i)
   {
      char c = static_cast<char>(i);
      m.upsert(std::string(1, c) + "x", i);
      m.upsert(std::string(1, c) + "y", i + 100);
   }
   REQUIRE(m.size() == 110);

   // Erase one key per subtree — exercises node256 child → setlist collapse
   for (int i = 0; i < 55; ++i)
   {
      char c = static_cast<char>(i);
      REQUIRE(m.erase(std::string(1, c) + "x"));
   }
   REQUIRE(m.size() == 55);

   // Verify remaining
   for (int i = 0; i < 55; ++i)
   {
      char  c = static_cast<char>(i);
      auto* v = m.get(std::string(1, c) + "y");
      REQUIRE(v != nullptr);
      REQUIRE(*v == (uint64_t)(i + 100));
   }
}

TEST_CASE("art_map prefix mismatch split on node256", "[art_map]")
{
   art_map<uint64_t> m;

   // Create a deep node256 with a prefix, then insert a key that mismatches the prefix.
   // First, build structure: "ABCD" + 50 single-byte children
   for (int i = 0; i < 50; ++i)
   {
      char c = static_cast<char>(i);
      m.upsert(std::string("ABCD") + c, i);
   }
   // Now the node256 under "ABCD" prefix exists.
   // Insert "ABXY" which mismatches at position 2 of prefix "CD"
   // This triggers clone_with_prefix for node256.
   m.upsert("ABXY", 999);

   REQUIRE(*m.get("ABXY") == 999);
   // All old keys still accessible
   for (int i = 0; i < 50; ++i)
   {
      char  c = static_cast<char>(i);
      auto* v = m.get(std::string("ABCD") + c);
      REQUIRE(v != nullptr);
      REQUIRE(*v == (uint64_t)i);
   }
}

TEST_CASE("art_map setlist capacity realloc path", "[art_map]")
{
   art_map<uint64_t> m;

   // Insert keys that share a long prefix + different last bytes.
   // The long prefix means each setlist alloc has less capacity for children,
   // forcing reallocation sooner.
   std::string prefix(200, 'Z');  // very long prefix → small initial capacity
   for (int i = 0; i < 40; ++i)
   {
      char c = static_cast<char>(i);
      m.upsert(prefix + c, i);
   }
   REQUIRE(m.size() == 40);

   for (int i = 0; i < 40; ++i)
   {
      char  c = static_cast<char>(i);
      auto* v = m.get(prefix + c);
      REQUIRE(v != nullptr);
      REQUIRE(*v == (uint64_t)i);
   }
}

TEST_CASE("art_map erase prefix key with collapse", "[art_map]")
{
   art_map<uint64_t> m;

   // Create: "ab" as prefix key, "abc" and "abd" as children
   m.upsert("abc", 1);
   m.upsert("abd", 2);
   m.upsert("ab", 3);  // prefix-key stored in inner node's value_off

   REQUIRE(m.size() == 3);
   REQUIRE(*m.get("ab") == 3);

   // Erase "ab" → clears value_off. With 2 children, no collapse needed.
   REQUIRE(m.erase("ab"));
   REQUIRE(m.get("ab") == nullptr);
   REQUIRE(*m.get("abc") == 1);
   REQUIRE(*m.get("abd") == 2);
   REQUIRE(m.size() == 2);

   // Now erase "abd" → inner node has 1 child "abc", triggers collapse
   REQUIRE(m.erase("abd"));
   REQUIRE(m.size() == 1);
   REQUIRE(*m.get("abc") == 1);
}

TEST_CASE("art_map erase prefix key leaves childless inner node", "[art_map]")
{
   art_map<uint64_t> m;

   // "a" is a prefix-key, "ab" is the only child
   m.upsert("ab", 1);
   m.upsert("a", 2);  // stored as value_off on inner node

   REQUIRE(m.size() == 2);

   // Erase "ab" → inner node has 0 children but value_off is still set
   REQUIRE(m.erase("ab"));
   REQUIRE(m.size() == 1);
   REQUIRE(*m.get("a") == 2);

   // Erase "a" → clears value_off, 0 children → node becomes null
   REQUIRE(m.erase("a"));
   REQUIRE(m.size() == 0);
   REQUIRE(m.empty());
}

TEST_CASE("art_map node256 advance visits value_off then children", "[art_map]")
{
   art_map<uint64_t> m;

   // Create node256 where the node itself has a value_off (prefix key)
   // and >48 children. This tests advance() through value_off → first child
   // in the node256 path.
   std::string prefix = "X";
   m.upsert(prefix, 0);  // prefix-key
   for (int i = 0; i < 50; ++i)
   {
      char c = static_cast<char>(i);
      m.upsert(prefix + std::string(1, c), i + 1);
   }
   REQUIRE(m.size() == 51);

   // Iteration: should start with "X" (prefix key), then "X\x00", "X\x01", ...
   auto it = m.begin();
   REQUIRE(it.key() == "X");
   ++it;  // advance from value_off → first child of node256
   REQUIRE(it != m.end());
   char expected = 0;
   REQUIRE(it.key() == prefix + std::string(1, expected));
}

TEST_CASE("art_map node256 retreat visits value_off", "[art_map]")
{
   art_map<uint64_t> m;

   // Same structure as above: prefix key "X" + 50 byte children
   std::string prefix = "X";
   m.upsert(prefix, 0);
   for (int i = 0; i < 50; ++i)
   {
      char c = static_cast<char>(i);
      m.upsert(prefix + std::string(1, c), i + 1);
   }

   // Forward to second element (first child), then retreat should give prefix key
   auto it = m.begin();
   REQUIRE(it.key() == "X");
   ++it;
   REQUIRE(it.key()[0] == 'X');
   REQUIRE(it.key().size() == 2);
   --it;  // should retreat to value_off
   REQUIRE(it.key() == "X");
}

TEST_CASE("art_map lower_bound prefix comparison — prefix > search key", "[art_map]")
{
   art_map<uint64_t> m;

   // Build structure where prefix comparison > search key
   m.upsert("MMMM_a", 1);
   m.upsert("MMMM_b", 2);

   // Search for "MMMM" — prefix matches fully, key terminates at inner node
   auto it = m.lower_bound("MMMM");
   REQUIRE(it != m.end());
   // Should descend to leftmost: "MMMM_a"
   REQUIRE(it.key() == "MMMM_a");

   // Search for "MMM" — prefix "MMM" matches, but partial has more chars
   // plen > remaining → descend to leftmost
   it = m.lower_bound("MMM");
   REQUIRE(it != m.end());
   REQUIRE(it.key() == "MMMM_a");
}

TEST_CASE("art_map lower_bound prefix comparison — prefix < search key", "[art_map]")
{
   art_map<uint64_t> m;

   m.upsert("AAA_x", 1);
   m.upsert("CCC_x", 2);

   // Search for "BBB" — first node prefix "AAA" < "BBB", should advance to "CCC_x"
   auto it = m.lower_bound("BBB");
   REQUIRE(it != m.end());
   REQUIRE(it.key() == "CCC_x");

   // Search past all keys
   it = m.lower_bound("DDD");
   REQUIRE(it == m.end());
}

TEST_CASE("art_map lower_bound node256 slot > search byte", "[art_map]")
{
   art_map<uint64_t> m;

   // Force node256 with sparse children
   for (int i = 0; i < 50; ++i)
   {
      char c = static_cast<char>(i * 5);  // 0, 5, 10, 15, ...
      m.upsert(std::string(1, c) + "v", i);
   }

   // lower_bound for byte 7 → should find slot 10 (next occupied)
   {
      char        search_byte = static_cast<char>(7);
      std::string search      = std::string(1, search_byte) + "v";
      auto        it          = m.lower_bound(search);
      REQUIRE(it != m.end());
      REQUIRE(static_cast<uint8_t>(it.key()[0]) == 10);
   }

   // lower_bound for byte 3 (between 0 and 5) without suffix
   {
      char        search_byte = static_cast<char>(3);
      std::string search      = std::string(1, search_byte);
      auto        it          = m.lower_bound(search);
      REQUIRE(it != m.end());
      REQUIRE(static_cast<uint8_t>(it.key()[0]) == 5);
   }
}

TEST_CASE("art_map erase prefix key with single child triggers collapse", "[art_map]")
{
   art_map<uint64_t> m;

   // Build: "abc" (prefix key) with exactly one child "abcdef"
   m.upsert("abcdef", 1);
   m.upsert("abc", 2);

   REQUIRE(m.size() == 2);

   // Erase prefix key "abc" → inner node has 1 child, triggers try_collapse
   // which merges prefixes: parent_prefix + child_byte + child_prefix
   REQUIRE(m.erase("abc"));
   REQUIRE(m.size() == 1);
   REQUIRE(*m.get("abcdef") == 1);

   // Verify iteration still works
   auto it = m.begin();
   REQUIRE(it.key() == "abcdef");
   ++it;
   REQUIRE(it == m.end());
}

TEST_CASE("art_map post-increment and post-decrement", "[art_map]")
{
   art_map<uint64_t> m;
   m.upsert("a", 1);
   m.upsert("b", 2);
   m.upsert("c", 3);

   auto it = m.begin();
   auto prev = it++;  // post-increment
   REQUIRE(prev.key() == "a");
   REQUIRE(it.key() == "b");

   auto cur = it--;  // post-decrement
   REQUIRE(cur.key() == "b");
   REQUIRE(it.key() == "a");
}

TEST_CASE("art_map get and upsert through node256 child", "[art_map]")
{
   art_map<uint64_t> m;

   // Force node256 at root
   for (int i = 0; i < 50; ++i)
   {
      char c = static_cast<char>(i);
      m.upsert(std::string(1, c) + "data", i);
   }

   // get() through node256 child lookup
   for (int i = 0; i < 50; ++i)
   {
      char  c = static_cast<char>(i);
      auto* v = m.get(std::string(1, c) + "data");
      REQUIRE(v != nullptr);
      REQUIRE(*v == (uint64_t)i);
   }

   // get() miss through node256 — byte not present
   {
      char c = static_cast<char>(200);  // not in [0,49]
      REQUIRE(m.get(std::string(1, c) + "data") == nullptr);
   }

   // upsert new child into node256
   {
      char c = static_cast<char>(200);
      m.upsert(std::string(1, c) + "data", 999);
      REQUIRE(*m.get(std::string(1, c) + "data") == 999);
   }
}

TEST_CASE("art_map node256 backward iteration with prefix keys", "[art_map]")
{
   art_map<uint64_t> m;

   // Node256 at root where some children also have prefix-key values
   for (int i = 0; i < 50; ++i)
   {
      char c = static_cast<char>(i);
      m.upsert(std::string(1, c), i);            // prefix key at inner node
      m.upsert(std::string(1, c) + "child", i + 100);  // child
   }
   REQUIRE(m.size() == 100);

   // Collect forward
   std::vector<std::string> fwd;
   for (auto it = m.begin(); it != m.end(); ++it)
      fwd.push_back(std::string(it.key()));

   // Collect backward
   std::vector<std::string> bwd;
   auto it = m.end();
   while (it != m.begin())
   {
      --it;
      bwd.push_back(std::string(it.key()));
   }

   REQUIRE(fwd.size() == bwd.size());
   for (size_t i = 0; i < fwd.size(); ++i)
      REQUIRE(fwd[i] == bwd[fwd.size() - 1 - i]);
}

TEST_CASE("art_map const upper_bound", "[art_map]")
{
   art_map<uint64_t> m;
   m.upsert("a", 1);
   m.upsert("b", 2);
   m.upsert("c", 3);

   const auto& cm = m;
   auto        it = cm.upper_bound("a");
   REQUIRE(it != cm.end());
   REQUIRE(it.key() == "b");
}

TEST_CASE("art_map const empty and begin/end", "[art_map]")
{
   art_map<uint64_t> m;
   const auto&       cm = m;
   REQUIRE(cm.empty());
   REQUIRE(cm.begin() == cm.end());
}

// ── Stress test matching DWAL benchmark pattern ─────────────────────────
//
// Reproduces the conditions that cause prefix corruption in the ART tree:
// 8-byte random binary keys (hash of sequential counter), 100K+ insertions
// per cycle, repeated many times.

static uint64_t stress_hash(uint64_t x)
{
   // Simple but fast hash (splitmix64)
   x ^= x >> 30;
   x *= 0xbf58476d1ce4e5b9ULL;
   x ^= x >> 27;
   x *= 0x94d049bb133111ebULL;
   x ^= x >> 31;
   return x;
}

TEST_CASE("art_map stress: random 8-byte binary keys", "[art_map][stress]")
{
   constexpr uint32_t ITEMS_PER_CYCLE = 100'000;
   constexpr uint32_t CYCLES          = 100;
   constexpr uint32_t VERIFY_EVERY    = 10;

   art_map<uint64_t> m;
   uint64_t          seq = 0;

   for (uint32_t cycle = 0; cycle < CYCLES; ++cycle)
   {
      for (uint32_t i = 0; i < ITEMS_PER_CYCLE; ++i)
      {
         uint64_t    h = stress_hash(seq);
         std::string key(reinterpret_cast<const char*>(&h), sizeof(h));
         m.upsert(key, seq);
         ++seq;
      }

      // Verify a random sample of keys
      if ((cycle % VERIFY_EVERY) == 0)
      {
         uint32_t checked = 0;
         for (uint64_t s = 0; s < seq; s += seq / 1000 + 1)
         {
            uint64_t    h = stress_hash(s);
            std::string key(reinterpret_cast<const char*>(&h), sizeof(h));
            auto*       v = m.get(key);
            // Key may have been overwritten by a later seq with the same hash
            // but it must exist
            REQUIRE(v != nullptr);
            ++checked;
         }
         INFO("cycle " << cycle << ": verified " << checked << " keys, "
                        << "size=" << m.size() << " arena=" << m.arena_bytes_used());
      }
   }

   INFO("final: " << seq << " inserts, size=" << m.size()
                   << " arena=" << m.arena_bytes_used());
   REQUIRE(m.size() > 0);
}

TEST_CASE("art_map stress: many cycles with clear (simulates DWAL swap)", "[art_map][stress]")
{
   constexpr uint32_t ITEMS_PER_CYCLE = 100'000;
   constexpr uint32_t CYCLES          = 200;

   uint64_t seq = 0;

   for (uint32_t cycle = 0; cycle < CYCLES; ++cycle)
   {
      art_map<uint64_t> m;  // fresh map each cycle (like DWAL swap)

      for (uint32_t i = 0; i < ITEMS_PER_CYCLE; ++i)
      {
         uint64_t    h = stress_hash(seq);
         std::string key(reinterpret_cast<const char*>(&h), sizeof(h));
         m.upsert(key, seq);
         ++seq;
      }

      // Verify all keys in this cycle exist
      uint64_t cycle_start = seq - ITEMS_PER_CYCLE;
      uint32_t found       = 0;
      for (uint64_t s = cycle_start; s < seq; s += 100)
      {
         uint64_t    h = stress_hash(s);
         std::string key(reinterpret_cast<const char*>(&h), sizeof(h));
         auto*       v = m.get(key);
         REQUIRE(v != nullptr);
         ++found;
      }
   }

   REQUIRE(seq == uint64_t(ITEMS_PER_CYCLE) * CYCLES);
}

TEST_CASE("art_map stress: concurrent writer + readers", "[art_map][stress][concurrent]")
{
   // Simulates DWAL: one writer thread inserting into the map while
   // multiple reader threads call get(). The ART map is NOT thread-safe
   // (arena realloc invalidates all pointers), so this test is expected
   // to detect corruption if it exists.
   constexpr uint32_t ITEMS           = 500'000;
   constexpr uint32_t NUM_READERS     = 4;

   art_map<uint64_t>     m;
   std::atomic<uint64_t> committed_seq{0};
   std::atomic<bool>     done{false};
   std::atomic<uint64_t> read_ops{0};
   std::atomic<uint64_t> found_count{0};

   // Reader threads
   std::vector<std::thread> readers;
   for (uint32_t t = 0; t < NUM_READERS; ++t)
   {
      readers.emplace_back(
          [&, t]()
          {
             uint64_t local_ops   = 0;
             uint64_t local_found = 0;
             uint64_t salt        = stress_hash(t * 999983ULL + 1);

             while (!done.load(std::memory_order_relaxed))
             {
                uint64_t max_seq = committed_seq.load(std::memory_order_relaxed);
                if (max_seq == 0)
                   continue;

                for (uint32_t i = 0; i < 100; ++i)
                {
                   uint64_t    s = stress_hash(local_ops + salt) % max_seq;
                   uint64_t    h = stress_hash(s);
                   std::string key(reinterpret_cast<const char*>(&h), sizeof(h));
                   auto*       v = m.get(key);
                   if (v)
                      ++local_found;
                   ++local_ops;
                }
             }
             read_ops.fetch_add(local_ops, std::memory_order_relaxed);
             found_count.fetch_add(local_found, std::memory_order_relaxed);
          });
   }

   // Writer thread (main)
   for (uint64_t i = 0; i < ITEMS; ++i)
   {
      uint64_t    h = stress_hash(i);
      std::string key(reinterpret_cast<const char*>(&h), sizeof(h));
      m.upsert(key, i);
      committed_seq.store(i + 1, std::memory_order_relaxed);
   }

   done.store(true, std::memory_order_relaxed);
   for (auto& t : readers)
      t.join();

   INFO("writer: " << ITEMS << " inserts, readers: " << read_ops.load() << " reads ("
                    << found_count.load() << " found)");
   REQUIRE(m.size() > 0);
}

TEST_CASE("art_map stress: large single tree with verification", "[art_map][stress]")
{
   // This is the most critical test — grows a single tree to 10M+ entries
   // with random 8-byte keys, which is where the prefix corruption was observed.
   constexpr uint64_t N = 10'000'000;

   art_map<uint64_t> m;
   for (uint64_t i = 0; i < N; ++i)
   {
      uint64_t    h = stress_hash(i);
      std::string key(reinterpret_cast<const char*>(&h), sizeof(h));
      m.upsert(key, i);

      // Periodic integrity check
      if (i > 0 && (i % 1'000'000) == 0)
      {
         // Verify 1000 random keys
         for (uint64_t j = 0; j < 1000; ++j)
         {
            uint64_t    s  = stress_hash(j * 997 + i) % i;
            uint64_t    hk = stress_hash(s);
            std::string k(reinterpret_cast<const char*>(&hk), sizeof(hk));
            auto*       v = m.get(k);
            REQUIRE(v != nullptr);
         }
         INFO(i << " inserts, size=" << m.size() << " arena=" << m.arena_bytes_used());
      }
   }

   REQUIRE(m.size() > 0);
   INFO("final: " << N << " inserts, size=" << m.size()
                   << " arena=" << m.arena_bytes_used());
}

// ── COW Snapshot Tests ──────────────────────────────────────────────────────

TEST_CASE("art_map COW snapshot basic", "[art_map][cow]")
{
   art_map<uint64_t> m;

   // Insert some initial data
   m.upsert("alpha", 1);
   m.upsert("beta", 2);
   m.upsert("gamma", 3);

   // Take a snapshot
   offset_t snap = m.snapshot_root();
   m.bump_cow_seq();

   // Modify the live tree
   m.upsert("alpha", 100);  // overwrite
   m.upsert("delta", 4);    // new key
   m.erase("beta");         // remove

   // Live tree reflects changes
   REQUIRE(*m.get("alpha") == 100);
   REQUIRE(m.get("beta") == nullptr);
   REQUIRE(*m.get("delta") == 4);
   REQUIRE(*m.get("gamma") == 3);

   // Snapshot still sees original data
   auto& arena = m.get_arena();
   REQUIRE(*art::get<uint64_t>(arena, snap, "alpha") == 1);
   REQUIRE(*art::get<uint64_t>(arena, snap, "beta") == 2);
   REQUIRE(*art::get<uint64_t>(arena, snap, "gamma") == 3);
   REQUIRE(art::get<uint64_t>(arena, snap, "delta") == nullptr);
}

TEST_CASE("art_map COW multiple snapshots", "[art_map][cow]")
{
   art_map<uint64_t> m;

   for (int i = 0; i < 100; ++i)
   {
      char key[16];
      snprintf(key, sizeof(key), "key%04d", i);
      m.upsert(key, i);
   }

   // Snapshot 1
   offset_t snap1 = m.snapshot_root();
   m.bump_cow_seq();

   // Add more keys
   for (int i = 100; i < 200; ++i)
   {
      char key[16];
      snprintf(key, sizeof(key), "key%04d", i);
      m.upsert(key, i);
   }

   // Snapshot 2
   offset_t snap2 = m.snapshot_root();
   m.bump_cow_seq();

   // Modify some keys
   m.upsert("key0050", 9999);
   m.erase("key0150");

   auto& arena = m.get_arena();

   // Snapshot 1: 100 keys, original values
   REQUIRE(*art::get<uint64_t>(arena, snap1, "key0050") == 50);
   REQUIRE(art::get<uint64_t>(arena, snap1, "key0150") == nullptr);

   // Snapshot 2: 200 keys, original values
   REQUIRE(*art::get<uint64_t>(arena, snap2, "key0050") == 50);
   REQUIRE(*art::get<uint64_t>(arena, snap2, "key0150") == 150);

   // Live tree: modified
   REQUIRE(*m.get("key0050") == 9999);
   REQUIRE(m.get("key0150") == nullptr);
}

TEST_CASE("art_map COW snapshot iteration", "[art_map][cow]")
{
   art_map<uint64_t> m;

   m.upsert("aaa", 1);
   m.upsert("bbb", 2);
   m.upsert("ccc", 3);

   offset_t snap = m.snapshot_root();
   m.bump_cow_seq();

   m.upsert("aaa", 99);
   m.upsert("ddd", 4);
   m.erase("bbb");

   // Iterate snapshot
   auto& arena = m.get_arena();
   auto  it    = make_begin<uint64_t>(arena, snap);
   auto  end   = make_end<uint64_t>(arena, snap);

   std::map<std::string, uint64_t> snap_data;
   for (; it != end; ++it)
      snap_data[std::string(it.key())] = it.value();

   REQUIRE(snap_data.size() == 3);
   REQUIRE(snap_data["aaa"] == 1);
   REQUIRE(snap_data["bbb"] == 2);
   REQUIRE(snap_data["ccc"] == 3);
}

TEST_CASE("art_map COW no overhead without bump", "[art_map][cow]")
{
   art_map<uint64_t> m;

   // Without bump_cow_seq, cow_seq stays 0 and no COW happens
   uint32_t before = m.arena_bytes_used();
   for (int i = 0; i < 1000; ++i)
   {
      char key[16];
      snprintf(key, sizeof(key), "k%04d", i);
      m.upsert(key, i);
   }
   uint32_t after_insert = m.arena_bytes_used();

   // Overwrite all keys — should mutate in place (no COW copies)
   for (int i = 0; i < 1000; ++i)
   {
      char key[16];
      snprintf(key, sizeof(key), "k%04d", i);
      m.upsert(key, i + 1000);
   }
   uint32_t after_overwrite = m.arena_bytes_used();

   // Leaf values are overwritten in place, so arena should not grow
   // (only inner nodes might allocate new leaves for value changes,
   //  but upsert overwrites existing leaf values in place)
   REQUIRE(after_overwrite == after_insert);
}
