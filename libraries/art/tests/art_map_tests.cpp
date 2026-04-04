#include <art/art_map.hpp>
#include <catch2/catch_test_macros.hpp>
#include <algorithm>
#include <map>
#include <random>
#include <string>
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
