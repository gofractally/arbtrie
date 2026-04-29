#include <catch2/catch_all.hpp>
#include <algorithm>
#include <random>
#include <string>
#include <vector>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/value_type.hpp>

using namespace psitri;

constexpr int CURSOR_SCALE = 1;

namespace
{
   struct cursor_test_db
   {
      std::string                    dir;
      std::shared_ptr<database>      db;
      std::shared_ptr<write_session> ses;

      cursor_test_db(const std::string& name = "cursor_testdb")
          : dir(name)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir + "/data");
         db  = database::open(dir);
         ses = db->start_write_session();
      }

      ~cursor_test_db() { std::filesystem::remove_all(dir); }
   };

   struct temp_tree_edit
   {
      explicit temp_tree_edit(write_session& ses)
          : tx(ses.start_write_transaction(ses.create_temporary_tree()))
      {
      }

      void insert(key_view key, value_view value) { tx.insert(key, value); }
      void upsert(key_view key, value_view value) { tx.upsert(key, value); }
      int  remove(key_view key) { return tx.remove(key); }

      cursor snapshot_cursor() const { return tx.snapshot_cursor(); }

      write_transaction tx;
   };

   temp_tree_edit start_temp_edit(cursor_test_db& t) { return temp_tree_edit(*t.ses); }

   std::string make_key(int i)
   {
      char buf[32];
      snprintf(buf, sizeof(buf), "key-%08d", i);
      return buf;
   }

   std::string make_value(int i)
   {
      char buf[64];
      snprintf(buf, sizeof(buf), "value-%08d", i);
      return buf;
   }

   /// Collect all keys via forward iteration
   std::vector<std::string> collect_keys_forward(cursor& rc)
   {
      std::vector<std::string> keys;
      rc.seek_begin();
      while (!rc.is_end())
      {
         keys.emplace_back(rc.key().data(), rc.key().size());
         rc.next();
      }
      return keys;
   }

   /// Collect all keys via backward iteration
   std::vector<std::string> collect_keys_backward(cursor& rc)
   {
      std::vector<std::string> keys;
      rc.seek_last();
      while (!rc.is_rend())
      {
         keys.emplace_back(rc.key().data(), rc.key().size());
         rc.prev();
      }
      std::reverse(keys.begin(), keys.end());
      return keys;
   }
}  // namespace

// ============================================================
// Cursor over trees that have undergone leaf splits
// ============================================================
// max_leaf_size = 2048 bytes. With ~5 bytes overhead per entry,
// a leaf holds roughly ~200 short keys before splitting.
// Inserting 500+ keys with short keys guarantees multiple splits.

TEST_CASE("cursor: iteration after multiple leaf splits", "[cursor][structural]")
{
   cursor_test_db t;
   auto           cur = start_temp_edit(t);

   // Insert enough keys to force several leaf splits (max_leaf_size=2048)
   // Each entry ~5 bytes overhead + key + value data, so ~200 entries per leaf
   const int N = 1000 / CURSOR_SCALE;

   std::vector<std::string> expected;
   for (int i = 0; i < N; ++i)
   {
      auto k = make_key(i);
      expected.push_back(k);
      cur.upsert(to_key_view(k), to_value_view(make_value(i)));
   }
   std::sort(expected.begin(), expected.end());

   auto rc = cur.snapshot_cursor();

   // Forward and backward must agree across split boundaries
   auto forward  = collect_keys_forward(rc);
   auto backward = collect_keys_backward(rc);

   REQUIRE(forward.size() == expected.size());
   REQUIRE(forward == expected);
   REQUIRE(backward == expected);

   // lower_bound must work across split boundaries
   // Pick keys near likely split points (every ~200 keys)
   for (int i = 0; i < N; i += (N / 10))
   {
      REQUIRE(rc.lower_bound(to_key_view(expected[i])));
      REQUIRE(std::string(rc.key().data(), rc.key().size()) == expected[i]);
   }
}

// ============================================================
// Cursor over tree with 256+ distinct first-byte fan-out
// ============================================================
// Inner nodes hold max 256 branches (16 clines x 16 per cline).
// Keys with all 256 possible first bytes force inner node cline exhaustion.

TEST_CASE("cursor: iteration over 256-way fan-out at root", "[cursor][structural]")
{
   cursor_test_db t;
   auto           cur = start_temp_edit(t);

   // Create keys with every possible first byte (0x00..0xFF)
   std::vector<std::string> expected;
   for (int b = 0; b < 256; ++b)
   {
      std::string key(1, static_cast<char>(b));
      key += "suffix";
      expected.push_back(key);
      cur.insert(key_view(key.data(), key.size()), to_value_view(std::to_string(b)));
   }
   std::sort(expected.begin(), expected.end());

   auto rc = cur.snapshot_cursor();
   auto keys = collect_keys_forward(rc);

   REQUIRE(keys.size() == 256);
   REQUIRE(keys == expected);

   // Backward iteration must also traverse all branches
   auto back_keys = collect_keys_backward(rc);
   REQUIRE(back_keys == expected);

   // Verify point lookups across all branches
   for (int b = 0; b < 256; ++b)
   {
      std::string key(1, static_cast<char>(b));
      key += "suffix";
      std::string buf;
      REQUIRE(rc.get(key_view(key.data(), key.size()), &buf) >= 0);
      REQUIRE(buf == std::to_string(b));
   }
}

TEST_CASE("cursor: find is exact point positioning", "[cursor][find]")
{
   cursor_test_db t;
   auto           cur = start_temp_edit(t);

   cur.upsert(to_key_view("aaa"), to_value_view("one"));
   cur.upsert(to_key_view("ccc"), to_value_view("three"));
   cur.upsert(to_key_view("eee"), to_value_view("five"));

   auto rc = cur.snapshot_cursor();

   REQUIRE(rc.find(to_key_view("ccc")));
   CHECK(rc.key() == to_key_view("ccc"));
   auto value = rc.value<std::string>();
   REQUIRE(value.has_value());
   CHECK(*value == "three");

   CHECK_FALSE(rc.find(to_key_view("bbb")));
   CHECK(rc.is_end());

   REQUIRE(rc.lower_bound(to_key_view("bbb")));
   CHECK(rc.key() == to_key_view("ccc"));

   CHECK_FALSE(rc.seek(to_key_view("bbb")));
   CHECK(rc.key() == to_key_view("ccc"));
}

// ============================================================
// Cursor near collapse threshold (24 keys)
// ============================================================
// When an inner node's descendents <= 24, it collapses to a leaf.
// Test iteration correctness across this structural transition.

TEST_CASE("cursor: correctness across collapse boundary", "[cursor][structural]")
{
   cursor_test_db t;
   auto           cur = start_temp_edit(t);

   // Insert 30 keys with distinct first bytes to create inner node branches
   const int START = 30;
   std::vector<std::string> all_keys;
   for (int i = 0; i < START; ++i)
   {
      // Distinct first byte forces separate inner branches
      std::string key(1, static_cast<char>(i));
      key += "-data";
      all_keys.push_back(key);
      cur.upsert(key_view(key.data(), key.size()), to_value("val"));
   }
   std::sort(all_keys.begin(), all_keys.end());

   // Verify iteration with >24 descendants (no collapse)
   auto rc = cur.snapshot_cursor();
   REQUIRE(collect_keys_forward(rc).size() == START);

   // Remove keys one at a time from 30 down through the collapse threshold (24)
   // At each step verify all remaining keys are still accessible
   for (int remove_count = 0; remove_count < 10; ++remove_count)
   {
      auto& key_to_remove = all_keys.back();
      cur.remove(key_view(key_to_remove.data(), key_to_remove.size()));
      all_keys.pop_back();

      rc = cur.snapshot_cursor();
      auto keys = collect_keys_forward(rc);

      INFO("after removing " << (remove_count + 1) << " keys, remaining=" << all_keys.size());
      REQUIRE(keys.size() == all_keys.size());

      // Verify each remaining key is retrievable
      for (auto& k : all_keys)
      {
         std::string buf;
         REQUIRE(rc.get(key_view(k.data(), k.size()), &buf) >= 0);
      }
   }
}

// ============================================================
// Cursor over tree with inner_prefix_node transitions
// ============================================================
// When many keys share a common prefix, inner_prefix_node is created.
// Inserting keys that break the prefix forces structural changes.

TEST_CASE("cursor: inner_prefix_node creation and traversal", "[cursor][structural]")
{
   cursor_test_db t;
   auto           cur = start_temp_edit(t);

   // Phase 1: Insert many keys with long shared prefix
   // This creates inner_prefix_nodes with the common prefix extracted
   std::vector<std::string> keys;
   std::string prefix = "very/long/shared/path/segment/";
   for (int i = 0; i < 200 / CURSOR_SCALE; ++i)
   {
      std::string key = prefix + make_key(i);
      keys.push_back(key);
      cur.upsert(to_key_view(key), to_value_view(make_value(i)));
   }

   // Phase 2: Insert keys that DON'T share the prefix
   // This forces the root to change structure, potentially splitting prefix nodes
   for (int i = 0; i < 50 / CURSOR_SCALE; ++i)
   {
      std::string key = "different/prefix/" + make_key(i);
      keys.push_back(key);
      cur.upsert(to_key_view(key), to_value_view(make_value(i + 1000)));
   }

   // Phase 3: Insert keys that partially match the prefix
   for (int i = 0; i < 50 / CURSOR_SCALE; ++i)
   {
      std::string key = "very/long/different/" + make_key(i);
      keys.push_back(key);
      cur.upsert(to_key_view(key), to_value_view(make_value(i + 2000)));
   }

   std::sort(keys.begin(), keys.end());

   auto rc         = cur.snapshot_cursor();
   auto forward    = collect_keys_forward(rc);
   auto backward   = collect_keys_backward(rc);

   REQUIRE(forward == keys);
   REQUIRE(backward == keys);

   // Verify lower_bound works across prefix node boundaries
   // The transition from "different/" to "very/long/different/" to "very/long/shared/"
   // exercises the inner_prefix_node common_prefix comparison
   for (auto& k : keys)
   {
      REQUIRE(rc.lower_bound(to_key_view(k)));
      REQUIRE(std::string(rc.key().data(), rc.key().size()) == k);
   }
}

// ============================================================
// Cursor interleaved next/prev near leaf boundaries
// ============================================================
// When iterating near the boundary between two leaf nodes,
// next() must cross into the next leaf and prev() must cross back.

TEST_CASE("cursor: interleaved next/prev across leaf boundaries", "[cursor][structural]")
{
   cursor_test_db t;
   auto           cur = start_temp_edit(t);

   // Force multiple leaf splits with sequential keys
   const int N = 500 / CURSOR_SCALE;
   for (int i = 0; i < N; ++i)
      cur.upsert(to_key_view(make_key(i)), to_value_view(make_value(i)));

   auto rc = cur.snapshot_cursor();

   // Navigate to middle of the tree (likely near a leaf boundary)
   rc.lower_bound(to_key_view(make_key(N / 2)));
   std::string mid(rc.key().data(), rc.key().size());

   // Rapidly alternate next/prev - this exercises the leaf boundary crossing code
   for (int i = 0; i < 20; ++i)
   {
      rc.next();
      std::string after(rc.key().data(), rc.key().size());
      REQUIRE(after > mid);

      rc.prev();
      std::string back(rc.key().data(), rc.key().size());
      REQUIRE(back == mid);
   }

   // Walk forward 5 steps, then backward 5 steps, verify we return to start
   std::string start(rc.key().data(), rc.key().size());
   for (int i = 0; i < 5; ++i)
      rc.next();
   for (int i = 0; i < 5; ++i)
      rc.prev();

   REQUIRE(std::string(rc.key().data(), rc.key().size()) == start);
}

// ============================================================
// Cursor after bulk remove that triggers structural changes
// ============================================================

TEST_CASE("cursor: iteration after bulk remove triggers collapse", "[cursor][structural]")
{
   cursor_test_db t;
   auto           cur = start_temp_edit(t);

   const int N = 500 / CURSOR_SCALE;
   for (int i = 0; i < N; ++i)
      cur.upsert(to_key_view(make_key(i)), to_value_view(make_value(i)));

   // Remove 90% of keys - this will trigger many collapses
   std::mt19937 rng(42);
   std::vector<int> indices(N);
   std::iota(indices.begin(), indices.end(), 0);
   std::shuffle(indices.begin(), indices.end(), rng);

   std::set<std::string> remaining;
   for (int i = 0; i < N; ++i)
      remaining.insert(make_key(i));

   int to_remove = N * 9 / 10;
   for (int i = 0; i < to_remove; ++i)
   {
      auto k = make_key(indices[i]);
      cur.remove(to_key_view(k));
      remaining.erase(k);
   }

   // Verify cursor correctly iterates the sparse remaining keys
   auto rc   = cur.snapshot_cursor();
   auto keys = collect_keys_forward(rc);

   REQUIRE(keys.size() == remaining.size());
   std::vector<std::string> expected(remaining.begin(), remaining.end());
   REQUIRE(keys == expected);

   // Verify each remaining key is accessible via point lookup
   for (auto& k : remaining)
   {
      std::string buf;
      REQUIRE(rc.get(to_key_view(k), &buf) >= 0);
   }
}

// ============================================================
// Cursor snapshot isolation: writer mutates during read
// ============================================================
// COW semantics: a cursor holding a root smart_ptr sees the tree
// as of the snapshot, even after the tree is mutated.

TEST_CASE("cursor: COW snapshot isolation during heavy mutation", "[cursor][structural]")
{
   cursor_test_db t;

   const int N = 200 / CURSOR_SCALE;

   // Phase 1: populate and commit
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
         tx.upsert(to_key_view(make_key(i)), to_value_view(make_value(i)));
      tx.commit();
   }

   // Take a snapshot
   auto root = t.ses->get_root(0);
   auto snapshot = root.snapshot_cursor();
   auto snapshot_keys = collect_keys_forward(snapshot);
   REQUIRE(snapshot_keys.size() == static_cast<size_t>(N));

   // Phase 2: Heavily mutate the tree - overwrite all, add more, remove some
   {
      auto tx = t.ses->start_transaction(0);
      // Overwrite all existing keys with different values
      for (int i = 0; i < N; ++i)
         tx.upsert(to_key_view(make_key(i)), to_value_view(make_value(i + 5000)));
      // Add new keys
      for (int i = N; i < N * 2; ++i)
         tx.upsert(to_key_view(make_key(i)), to_value_view(make_value(i)));
      // Remove some originals
      for (int i = 0; i < N / 4; ++i)
         tx.remove(to_key_view(make_key(i)));
      tx.commit();
   }

   // Snapshot must still see original N keys with original values
   auto snapshot_keys2 = collect_keys_forward(snapshot);
   REQUIRE(snapshot_keys2 == snapshot_keys);

   for (int i = 0; i < N; ++i)
   {
      std::string buf;
      REQUIRE(snapshot.get(to_key_view(make_key(i)), &buf) >= 0);
      REQUIRE(buf == make_value(i));  // original values, not mutated
   }
}

// ============================================================
// Cursor with value_node transitions
// ============================================================
// Values >64 bytes get stored as separate value_node objects.
// Cursor must correctly dereference these during iteration.

TEST_CASE("cursor: iteration with mixed inline and value_node values", "[cursor][structural]")
{
   cursor_test_db t;
   auto           cur = start_temp_edit(t);

   // Create entries that alternate between inline (<=64 bytes) and value_node (>64 bytes)
   const int N = 100 / CURSOR_SCALE;
   std::map<std::string, std::string> expected;

   for (int i = 0; i < N; ++i)
   {
      auto k = make_key(i);
      std::string val;
      if (i % 3 == 0)
         val = "";  // null value
      else if (i % 3 == 1)
         val = make_value(i);  // small inline (~20 bytes)
      else
         val = std::string(200 + i, static_cast<char>('A' + (i % 26)));  // large value_node

      expected[k] = val;
      cur.upsert(to_key_view(k), to_value_view(val));
   }

   auto rc = cur.snapshot_cursor();

   // Forward iteration - verify each value matches
   rc.seek_begin();
   auto it = expected.begin();
   while (!rc.is_end())
   {
      REQUIRE(it != expected.end());
      std::string k(rc.key().data(), rc.key().size());
      REQUIRE(k == it->first);

      std::string buf;
      auto        result = rc.get(to_key_view(k), &buf);
      REQUIRE(result >= 0);
      REQUIRE(buf == it->second);

      ++it;
      rc.next();
   }
   REQUIRE(it == expected.end());
}

// ============================================================
// Cursor after database reopen with structural transitions
// ============================================================
// Write a complex tree, close DB, reopen, verify cursor
// traverses the serialized/deserialized structure correctly.

TEST_CASE("cursor: complex tree survives reopen", "[cursor][persistence]")
{
   const std::string dir = "cursor_complex_reopen";
   const int N = 300 / CURSOR_SCALE;

   std::map<std::string, std::string> expected;

   // Build a complex tree with diverse structure
   {
      std::filesystem::remove_all(dir);
      std::filesystem::create_directories(dir + "/data");
      auto db  = database::open(dir);
      auto ses = db->start_write_session();
      auto tx  = ses->start_transaction(0);

      // Keys that create prefix nodes
      for (int i = 0; i < N / 3; ++i)
      {
         auto k = "prefix/shared/" + make_key(i);
         auto v = make_value(i);
         expected[k] = v;
         tx.upsert(to_key_view(k), to_value_view(v));
      }

      // Keys with diverse first bytes (fan-out)
      for (int i = 0; i < N / 3; ++i)
      {
         std::string k(1, static_cast<char>(i));
         k += "/fanout";
         auto v = make_value(i + 1000);
         expected[k] = v;
         tx.upsert(key_view(k.data(), k.size()), to_value_view(v));
      }

      // Large values that create value_nodes
      for (int i = 0; i < N / 3; ++i)
      {
         auto k = "large/" + make_key(i);
         auto v = std::string(500, static_cast<char>('A' + (i % 26)));
         expected[k] = v;
         tx.upsert(to_key_view(k), to_value_view(v));
      }

      tx.commit();
   }

   // Reopen and verify everything
   {
      auto db  = database::open(dir);
      auto ses = db->start_write_session();
      auto root = ses->get_root(0);
      REQUIRE(root);

      auto rc = root.snapshot_cursor();

      // Verify count
      REQUIRE(rc.count_keys() == expected.size());

      // Verify forward iteration
      auto keys = collect_keys_forward(rc);
      std::vector<std::string> expected_keys;
      for (auto& [k, v] : expected)
         expected_keys.push_back(k);
      REQUIRE(keys == expected_keys);

      // Verify all values correct
      for (auto& [k, v] : expected)
      {
         std::string buf;
         auto result = rc.get(key_view(k.data(), k.size()), &buf);
         INFO("key: " << k);
         REQUIRE(result >= 0);
         REQUIRE(buf == v);
      }
   }

   std::filesystem::remove_all(dir);
}

// ============================================================
// count_keys cross-validation with random ranges
// ============================================================

TEST_CASE("cursor: count_keys matches manual iteration for random ranges", "[cursor]")
{
   cursor_test_db t;
   auto           cur = start_temp_edit(t);

   const int N = 500 / CURSOR_SCALE;
   for (int i = 0; i < N; ++i)
      cur.upsert(to_key_view(make_key(i)), to_value_view(make_value(i)));

   auto rc = cur.snapshot_cursor();

   std::mt19937 rng(42);
   for (int trial = 0; trial < 50; ++trial)
   {
      int lo = rng() % N;
      int hi = rng() % N;
      if (lo > hi) std::swap(lo, hi);

      auto lower = make_key(lo);
      auto upper = make_key(hi);

      uint64_t api_count = rc.count_keys(to_key_view(lower), to_key_view(upper));

      uint64_t manual_count = 0;
      rc.lower_bound(to_key_view(lower));
      while (!rc.is_end())
      {
         std::string k(rc.key().data(), rc.key().size());
         if (k >= upper) break;
         ++manual_count;
         rc.next();
      }

      INFO("range [" << lower << ", " << upper << ")");
      REQUIRE(api_count == manual_count);
   }
}
