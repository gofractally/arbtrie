#include <catch2/catch_all.hpp>
#include <map>
#include <random>
#include <string>
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/value_type.hpp>

using namespace psitri;

namespace
{
   struct edge_db
   {
      std::string                    dir;
      std::shared_ptr<database>      db;
      std::shared_ptr<write_session> ses;

      edge_db(const std::string& name = "edge_case_testdb")
          : dir(name)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir + "/data");
         db  = database::open(dir);
         ses = db->start_write_session();
      }

      ~edge_db() { std::filesystem::remove_all(dir); }
   };
}  // namespace

// ============================================================
// Value node threshold boundary (64 bytes)
// ============================================================
// Values >64 bytes are allocated as value_node objects (tree_ops.hpp:46).
// Values <=64 bytes are stored inline in the leaf.
// Test the exact boundary.

TEST_CASE("edge: value at inline/value_node boundary (64 bytes)", "[edge_case][structural]")
{
   edge_db t;
   auto    cur = t.ses->create_write_cursor();

   // Exactly 64 bytes: should be inline
   std::string val_64(64, 'X');
   cur->upsert(to_key("k64"), to_value_view(val_64));

   // 65 bytes: should be value_node
   std::string val_65(65, 'Y');
   cur->upsert(to_key("k65"), to_value_view(val_65));

   // 63 bytes: definitely inline
   std::string val_63(63, 'Z');
   cur->upsert(to_key("k63"), to_value_view(val_63));

   // Verify all retrieve correctly
   REQUIRE(*cur->get<std::string>(to_key("k64")) == val_64);
   REQUIRE(*cur->get<std::string>(to_key("k65")) == val_65);
   REQUIRE(*cur->get<std::string>(to_key("k63")) == val_63);

   // Transition: update inline → value_node
   std::string val_big(200, 'A');
   cur->upsert(to_key("k64"), to_value_view(val_big));
   REQUIRE(*cur->get<std::string>(to_key("k64")) == val_big);

   // Transition: update value_node → inline
   cur->upsert(to_key("k65"), to_value("tiny"));
   REQUIRE(*cur->get<std::string>(to_key("k65")) == "tiny");

   // Transition: update value_node → null
   cur->upsert(to_key("k64"), value_view(nullptr, 0));
   auto result = cur->get<std::string>(to_key("k64"));
   REQUIRE(result.has_value());
   REQUIRE(result->empty());
}

// ============================================================
// Leaf capacity exhaustion with many small keys
// ============================================================
// max_leaf_size = 2048 bytes, ~5 bytes overhead per entry.
// With 3-byte keys and no values, we can pack ~300 entries
// before hitting the split boundary.

TEST_CASE("edge: pack leaf to split point with minimal keys", "[edge_case][structural]")
{
   edge_db t;
   auto    cur = t.ses->create_write_cursor();

   // Insert keys until we definitely exceed a single leaf's capacity
   // 3-byte keys: "000" to "999" (1000 keys, ~8 bytes each = ~8000 bytes)
   // This will force multiple leaf splits
   std::map<std::string, std::string> oracle;
   for (int i = 0; i < 1000; ++i)
   {
      char key[4];
      snprintf(key, sizeof(key), "%03d", i);
      std::string k(key), v(key);
      cur->upsert(to_key_view(k), to_value_view(v));
      oracle[k] = v;
   }

   REQUIRE(cur->count_keys() == 1000);

   // Verify every key survives the splits
   for (auto& [k, v] : oracle)
   {
      auto result = cur->get<std::string>(to_key_view(k));
      INFO("key: " << k);
      REQUIRE(result.has_value());
      REQUIRE(*result == v);
   }
}

// ============================================================
// Leaf capacity with large values forcing value_node allocation
// ============================================================

TEST_CASE("edge: leaf with mix of inline and value_node near capacity", "[edge_case][structural]")
{
   edge_db t;
   auto    cur = t.ses->create_write_cursor();

   // Insert keys where every other one has a large value (value_node)
   // The leaf still needs to track the address, and the value_node
   // reference counts must be correct through splits
   std::map<std::string, std::string> oracle;
   for (int i = 0; i < 200; ++i)
   {
      auto k = "k" + std::to_string(i);
      std::string val;
      if (i % 2 == 0)
         val = std::string(10, 'a');  // inline
      else
         val = std::string(500, static_cast<char>('A' + (i % 26)));  // value_node

      cur->upsert(to_key_view(k), to_value_view(val));
      oracle[k] = val;
   }

   // Verify all values survive splits correctly
   for (auto& [k, v] : oracle)
   {
      auto result = cur->get<std::string>(to_key_view(k));
      INFO("key: " << k);
      REQUIRE(result.has_value());
      REQUIRE(*result == v);
   }
}

// ============================================================
// Binary keys with null bytes (exercises key comparison)
// ============================================================

TEST_CASE("edge: binary keys with embedded null bytes", "[edge_case]")
{
   edge_db t;
   auto    cur = t.ses->create_write_cursor();

   // Keys containing null bytes - tests that comparisons use size, not strlen
   std::string k1("a\0b", 3);
   std::string k2("a\0c", 3);
   std::string k3("a\0\0", 3);
   std::string k4("a", 1);     // shorter prefix
   std::string k5("a\0b\0", 4);  // k1 with extra null

   cur->insert(key_view(k1.data(), k1.size()), to_value("v1"));
   cur->insert(key_view(k2.data(), k2.size()), to_value("v2"));
   cur->insert(key_view(k3.data(), k3.size()), to_value("v3"));
   cur->insert(key_view(k4.data(), k4.size()), to_value("v4"));
   cur->insert(key_view(k5.data(), k5.size()), to_value("v5"));

   REQUIRE(cur->count_keys() == 5);

   // Each key must be independently retrievable
   std::string buf;
   REQUIRE(cur->get(key_view(k1.data(), k1.size()), &buf) >= 0);
   REQUIRE(buf == "v1");
   REQUIRE(cur->get(key_view(k2.data(), k2.size()), &buf) >= 0);
   REQUIRE(buf == "v2");
   REQUIRE(cur->get(key_view(k3.data(), k3.size()), &buf) >= 0);
   REQUIRE(buf == "v3");
   REQUIRE(cur->get(key_view(k4.data(), k4.size()), &buf) >= 0);
   REQUIRE(buf == "v4");
   REQUIRE(cur->get(key_view(k5.data(), k5.size()), &buf) >= 0);
   REQUIRE(buf == "v5");

   // Iteration must produce sorted byte order
   auto rc = cur->read_cursor();
   rc.seek_begin();
   std::string prev;
   int count = 0;
   while (!rc.is_end())
   {
      std::string k(rc.key().data(), rc.key().size());
      if (!prev.empty())
         REQUIRE(prev < k);
      prev = k;
      ++count;
      rc.next();
   }
   REQUIRE(count == 5);
}

// ============================================================
// Prefix keys: structural transitions during insert/remove
// ============================================================
// Keys "a", "ab", "abc", ... where each is a prefix of the next.
// This creates specific inner_prefix_node structures.

TEST_CASE("edge: prefix chain insert/remove structural integrity", "[edge_case][structural]")
{
   edge_db t;
   auto    cur = t.ses->create_write_cursor();

   // Build a chain where each key is a prefix of the next
   std::vector<std::string> chain;
   std::string growing;
   for (int i = 0; i < 30; ++i)
   {
      growing += static_cast<char>('a' + (i % 26));
      chain.push_back(growing);
   }

   for (auto& k : chain)
      cur->insert(to_key_view(k), to_value_view(k));

   REQUIRE(cur->count_keys() == chain.size());

   // Remove from the middle - this forces prefix node restructuring
   // because the prefix shared between shorter and longer keys changes
   for (int i = 10; i < 20; ++i)
   {
      cur->remove(to_key_view(chain[i]));
   }

   // Verify surviving keys
   for (int i = 0; i < 10; ++i)
   {
      auto result = cur->get<std::string>(to_key_view(chain[i]));
      REQUIRE(result.has_value());
      REQUIRE(*result == chain[i]);
   }
   for (int i = 10; i < 20; ++i)
   {
      REQUIRE_FALSE(cur->get<std::string>(to_key_view(chain[i])).has_value());
   }
   for (size_t i = 20; i < chain.size(); ++i)
   {
      auto result = cur->get<std::string>(to_key_view(chain[i]));
      REQUIRE(result.has_value());
      REQUIRE(*result == chain[i]);
   }
}

// ============================================================
// Keys with long shared prefix forcing deep prefix nodes
// ============================================================

TEST_CASE("edge: very long shared prefix forces deep inner_prefix_node", "[edge_case][structural]")
{
   edge_db t;
   auto    cur = t.ses->create_write_cursor();

   // 500-byte common prefix (tests prefix storage limits)
   std::string prefix(500, 'P');
   std::map<std::string, std::string> oracle;

   for (int i = 0; i < 100; ++i)
   {
      // Short unique suffix
      char suffix[8];
      snprintf(suffix, sizeof(suffix), "%04d", i);
      std::string key = prefix + suffix;
      std::string val = std::to_string(i);
      cur->upsert(to_key_view(key), to_value_view(val));
      oracle[key] = val;
   }

   // Add a key that shares only part of the prefix
   // This forces the prefix node to split its stored prefix
   std::string partial = prefix.substr(0, 250) + "DIFFERENT/key";
   cur->upsert(to_key_view(partial), to_value("split"));
   oracle[partial] = "split";

   // Verify all keys
   for (auto& [k, v] : oracle)
   {
      auto result = cur->get<std::string>(to_key_view(k));
      INFO("key length: " << k.size());
      REQUIRE(result.has_value());
      REQUIRE(*result == v);
   }
}

// ============================================================
// Rapid insert-remove churn on overlapping key ranges
// ============================================================
// This exercises the cline reference counting code in leaf nodes.
// When a key is inserted, it may create a value_node (cline ref).
// When removed, the cline ref must be decremented. Rapid cycling
// stresses this ref counting at boundaries.

TEST_CASE("edge: rapid insert-remove cycling stresses cline ref counting", "[edge_case][structural]")
{
   edge_db t;
   auto    cur = t.ses->create_write_cursor();

   std::map<std::string, std::string> oracle;
   std::mt19937 rng(42424);

   // Use a small key space so the same leaf nodes get modified repeatedly
   const int KEY_RANGE = 20;
   const int CYCLES    = 1000;

   for (int c = 0; c < CYCLES; ++c)
   {
      int i = rng() % KEY_RANGE;
      std::string key = "x" + std::to_string(i);

      if (rng() % 2 == 0)
      {
         // Insert with large value (creates value_node → cline ref)
         std::string val(100, static_cast<char>('A' + (c % 26)));
         cur->upsert(to_key_view(key), to_value_view(val));
         oracle[key] = val;
      }
      else
      {
         cur->remove(to_key_view(key));
         oracle.erase(key);
      }
   }

   // Verify final state
   REQUIRE(cur->count_keys() == oracle.size());
   for (auto& [k, v] : oracle)
   {
      auto result = cur->get<std::string>(to_key_view(k));
      INFO("key: " << k);
      REQUIRE(result.has_value());
      REQUIRE(*result == v);
   }
}

// ============================================================
// Multiple distinct first bytes with per-branch heavy mutation
// ============================================================
// This exercises inner node cline management: each first byte creates
// a branch, and heavy mutation within each branch stresses the cline
// ref counting during replace_branch operations (known bug area).

TEST_CASE("edge: per-branch heavy mutation stresses replace_branch", "[edge_case][structural]")
{
   edge_db t;
   auto    cur = t.ses->create_write_cursor();

   std::map<std::string, std::string> oracle;

   // Create 20 branches (distinct first bytes)
   for (int branch = 0; branch < 20; ++branch)
   {
      // Within each branch, rapidly insert and remove keys
      // This causes the child node to be repeatedly replaced (replace_branch)
      for (int pass = 0; pass < 5; ++pass)
      {
         for (int i = 0; i < 10; ++i)
         {
            std::string key(1, static_cast<char>('A' + branch));
            key += std::to_string(pass * 100 + i);
            std::string val = "v" + std::to_string(pass * 100 + i);
            cur->upsert(to_key_view(key), to_value_view(val));
            oracle[key] = val;
         }
         // Remove half of what we just added
         for (int i = 0; i < 5; ++i)
         {
            std::string key(1, static_cast<char>('A' + branch));
            key += std::to_string(pass * 100 + i);
            cur->remove(to_key_view(key));
            oracle.erase(key);
         }
      }
   }

   // Verify oracle match
   REQUIRE(cur->count_keys() == oracle.size());
   for (auto& [k, v] : oracle)
   {
      auto result = cur->get<std::string>(to_key_view(k));
      INFO("key: " << k);
      REQUIRE(result.has_value());
      REQUIRE(*result == v);
   }
}

// ============================================================
// Empty value interactions with structural operations
// ============================================================

TEST_CASE("edge: empty values through split and collapse", "[edge_case][structural]")
{
   edge_db t;
   auto    cur = t.ses->create_write_cursor();

   // Insert enough empty-value keys to force leaf splits
   std::set<std::string> keys;
   for (int i = 0; i < 500; ++i)
   {
      char buf[8];
      snprintf(buf, sizeof(buf), "%04d", i);
      std::string k(buf);
      cur->upsert(to_key_view(k), value_view(nullptr, 0));
      keys.insert(k);
   }

   REQUIRE(cur->count_keys() == 500);

   // Remove most keys (force collapses)
   for (int i = 0; i < 480; ++i)
   {
      char buf[8];
      snprintf(buf, sizeof(buf), "%04d", i);
      std::string k(buf);
      cur->remove(to_key_view(k));
      keys.erase(k);
   }

   // Verify surviving empty-value keys are readable
   REQUIRE(cur->count_keys() == keys.size());
   for (auto& k : keys)
   {
      auto result = cur->get<std::string>(to_key_view(k));
      REQUIRE(result.has_value());
      REQUIRE(result->empty());
   }
}

// ============================================================
// Subtree values through structural transitions
// ============================================================

TEST_CASE("edge: subtree values survive leaf splits", "[edge_case][structural]")
{
   edge_db t;

   // Create a subtree
   auto sub_cursor = t.ses->create_write_cursor();
   for (int i = 0; i < 10; ++i)
      sub_cursor->upsert(to_key_view("sub-" + std::to_string(i)),
                         to_value_view("sub-val-" + std::to_string(i)));
   auto subtree_root = sub_cursor->root();

   // Insert the subtree along with many regular keys to force leaf splits
   auto cur = t.ses->create_write_cursor();
   cur->upsert(to_key("subtree-holder"), std::move(subtree_root));

   // Add enough keys to force the leaf containing the subtree to split
   for (int i = 0; i < 500; ++i)
      cur->upsert(to_key_view("data-" + std::to_string(i)),
                  to_value_view("val-" + std::to_string(i)));

   // Verify the subtree is still accessible and valid
   REQUIRE(cur->is_subtree(to_key("subtree-holder")));
   auto recovered = cur->get_subtree_cursor(to_key("subtree-holder"));
   for (int i = 0; i < 10; ++i)
   {
      auto result = recovered.get<std::string>(to_key_view("sub-" + std::to_string(i)));
      REQUIRE(result.has_value());
      REQUIRE(*result == "sub-val-" + std::to_string(i));
   }

   // Regular keys also intact
   for (int i = 0; i < 500; ++i)
   {
      auto result = cur->get<std::string>(to_key_view("data-" + std::to_string(i)));
      REQUIRE(result.has_value());
   }
}
