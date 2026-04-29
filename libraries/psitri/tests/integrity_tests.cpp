#include <catch2/catch_all.hpp>
#include <map>
#include <random>
#include <set>
#include <string>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/value_type.hpp>

using namespace psitri;

constexpr int INTEG_SCALE = 1;

namespace
{
   struct integrity_db
   {
      std::string                    dir;
      std::shared_ptr<database>      db;
      std::shared_ptr<write_session> ses;

      integrity_db(const std::string& name = "integrity_testdb")
          : dir(name)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir + "/data");
         db  = database::open(dir);
         ses = db->start_write_session();
      }

      ~integrity_db() { std::filesystem::remove_all(dir); }
   };

   struct temp_tree_edit
   {
      explicit temp_tree_edit(write_session& ses)
          : tx(ses.start_write_transaction(ses.create_temporary_tree()))
      {
      }

      void upsert(key_view key, value_view value) { tx.upsert(key, value); }
      int  remove(key_view key) { return tx.remove(key); }
      bool remove_range_any(key_view lower, key_view upper)
      {
         return tx.remove_range_any(lower, upper);
      }

      uint64_t remove_range_counted(key_view lower, key_view upper)
      {
         return tx.remove_range_counted(lower, upper);
      }

      template <ConstructibleBuffer T>
      std::optional<T> get(key_view key) const
      {
         return tx.get<T>(key);
      }

      int32_t get(key_view key, Buffer auto* buffer) const { return tx.get(key, buffer); }

      cursor snapshot_cursor() const { return tx.snapshot_cursor(); }
      uint64_t count_keys() const
      {
         auto c = snapshot_cursor();
         return c.count_keys();
      }

      write_transaction tx;
   };

   temp_tree_edit start_temp_edit(integrity_db& t) { return temp_tree_edit(*t.ses); }

   std::string ikey(int i)
   {
      char buf[32];
      snprintf(buf, sizeof(buf), "ikey-%08d", i);
      return buf;
   }

   std::string ival(int i)
   {
      char buf[64];
      snprintf(buf, sizeof(buf), "ival-%08d", i);
      return buf;
   }

   /// Verify that tree contents exactly match oracle map
   void verify_oracle(temp_tree_edit& cur, const std::map<std::string, std::string>& oracle)
   {
      // Check each expected key has correct value
      for (auto& [key, val] : oracle)
      {
         auto result = cur.get<std::string>(to_key_view(key));
         INFO("key: " << key);
         REQUIRE(result.has_value());
         REQUIRE(*result == val);
      }

      // Check iteration produces exactly the right keys (no extras)
      auto     rc    = cur.snapshot_cursor();
      uint64_t count = 0;
      rc.seek_begin();
      while (!rc.is_end())
      {
         std::string k(rc.key().data(), rc.key().size());
         INFO("iterated key: " << k);
         REQUIRE(oracle.count(k) == 1);
         ++count;
         rc.next();
      }
      REQUIRE(count == oracle.size());
   }
}  // namespace

// ============================================================
// Oracle comparison: random mixed ops on a tree that forces
// leaf splits, inner node growth, and collapses.
// ============================================================
// The oracle (std::map) is the ground truth. Every operation is
// applied to both the map and the trie, then we verify they match.

// Regression test for leaf node free_space going negative during update_value.
// Originally triggered by mixed upsert/remove/range_remove with 3-byte keys
// and variable-size values crossing value_node threshold. Now fixed.
TEST_CASE("integrity: random ops oracle comparison forcing structural changes", "[integrity][oracle]")
{
   integrity_db t;
   auto         cur = start_temp_edit(t);

   std::map<std::string, std::string> oracle;
   std::mt19937                       rng(98765);

   // Use short keys to maximize entries per leaf and force more splits.
   // Key range of 500 with random insert/remove means the tree oscillates
   // between growth (splits) and shrinkage (collapses).
   const int OPS       = 5000 / INTEG_SCALE;
   const int KEY_RANGE = 500;

   for (int op = 0; op < OPS; ++op)
   {
      int i      = rng() % KEY_RANGE;
      int action = rng() % 100;

      // 3-byte key forces dense packing → more entries per leaf → splits
      char key_buf[4];
      snprintf(key_buf, sizeof(key_buf), "%03d", i);
      std::string key(key_buf);

      if (action < 50)
      {
         // upsert with value size that sometimes crosses the 64-byte value_node threshold
         int val_size = (rng() % 3 == 0) ? 100 + (rng() % 200) : 5 + (rng() % 20);
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
         // range_remove: remove a small range
         int j = std::min(i + 5, KEY_RANGE);
         char lo_buf[4], hi_buf[4];
         snprintf(lo_buf, sizeof(lo_buf), "%03d", i);
         snprintf(hi_buf, sizeof(hi_buf), "%03d", j);
         std::string lo(lo_buf), hi(hi_buf);
         cur.remove_range_counted(to_key_view(lo), to_key_view(hi));
         auto it = oracle.lower_bound(lo);
         while (it != oracle.end() && it->first < hi)
            it = oracle.erase(it);
      }

      // Periodic verification (every 500 ops) to catch corruption early
      if ((op + 1) % (500 / INTEG_SCALE) == 0)
      {
         INFO("verification at op " << (op + 1));
         REQUIRE(cur.count_keys() == oracle.size());
      }
   }

   // Final full verification
   verify_oracle(cur, oracle);
}

// ============================================================
// Oracle comparison with keys designed to stress prefix nodes
// ============================================================

TEST_CASE("integrity: oracle comparison with prefix-heavy keys", "[integrity][oracle]")
{
   integrity_db t;
   auto         cur = start_temp_edit(t);

   std::map<std::string, std::string> oracle;
   std::mt19937                       rng(11111);

   // Keys with various prefix groups: forces inner_prefix_node creation
   // and structural changes when keys are added/removed across groups
   const char* prefixes[] = {
      "users/active/",
      "users/inactive/",
      "users/",         // shorter prefix overlapping with above
      "data/metrics/",
      "data/logs/",
      "config/",
      "",               // root-level keys
   };
   const int NUM_PREFIXES = sizeof(prefixes) / sizeof(prefixes[0]);
   const int OPS          = 3000 / INTEG_SCALE;

   for (int op = 0; op < OPS; ++op)
   {
      int prefix_idx = rng() % NUM_PREFIXES;
      int suffix     = rng() % 100;
      std::string key = std::string(prefixes[prefix_idx]) + std::to_string(suffix);
      int action = rng() % 3;

      if (action < 2)
      {
         std::string val = ival(op);
         cur.upsert(to_key_view(key), to_value_view(val));
         oracle[key] = val;
      }
      else
      {
         cur.remove(to_key_view(key));
         oracle.erase(key);
      }
   }

   verify_oracle(cur, oracle);
}

// ============================================================
// Oracle comparison with high-fan-out keys (diverse first bytes)
// ============================================================
// Keys with all 256 possible first bytes stress inner node cline limits.

TEST_CASE("integrity: oracle with 256-way fan-out insert/remove cycles", "[integrity][oracle]")
{
   integrity_db t;
   auto         cur = start_temp_edit(t);

   std::map<std::string, std::string> oracle;

   // Phase 1: Insert keys covering all 256 first bytes
   for (int b = 0; b < 256; ++b)
   {
      std::string key(1, static_cast<char>(b));
      key += "/data";
      std::string val = ival(b);
      cur.upsert(key_view(key.data(), key.size()), to_value_view(val));
      oracle[key] = val;
   }
   REQUIRE(cur.count_keys() == 256);

   // Phase 2: Remove half (every other first byte)
   for (int b = 0; b < 256; b += 2)
   {
      std::string key(1, static_cast<char>(b));
      key += "/data";
      cur.remove(key_view(key.data(), key.size()));
      oracle.erase(key);
   }
   REQUIRE(cur.count_keys() == 128);

   // Phase 3: Re-insert removed keys with different values
   for (int b = 0; b < 256; b += 2)
   {
      std::string key(1, static_cast<char>(b));
      key += "/data";
      std::string val = ival(b + 1000);
      cur.upsert(key_view(key.data(), key.size()), to_value_view(val));
      oracle[key] = val;
   }
   REQUIRE(cur.count_keys() == 256);

   verify_oracle(cur, oracle);
}

// ============================================================
// Transaction isolation: concurrent snapshots see consistent state
// ============================================================

TEST_CASE("integrity: transaction snapshot consistency", "[integrity][transaction]")
{
   integrity_db t;
   const int    N = 200 / INTEG_SCALE;

   // Commit initial state
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
         tx.upsert(to_key_view(ikey(i)), to_value_view(ival(i)));
      tx.commit();
   }

   // Take snapshot of version 1
   auto root_v1 = t.ses->get_root(0);

   // Commit version 2: overwrite all + add new keys
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
         tx.upsert(to_key_view(ikey(i)), to_value_view(ival(i + 5000)));
      for (int i = N; i < N * 2; ++i)
         tx.upsert(to_key_view(ikey(i)), to_value_view(ival(i)));
      tx.commit();
   }

   auto root_v2 = t.ses->get_root(0);

   // Commit version 3: remove first quarter
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N / 4; ++i)
         tx.remove(to_key_view(ikey(i)));
      tx.commit();
   }

   auto root_v3 = t.ses->get_root(0);

   // Verify all three snapshots independently
   auto c1 = root_v1.snapshot_cursor();
   REQUIRE(c1.count_keys() == static_cast<uint64_t>(N));
   for (int i = 0; i < N; ++i)
   {
      std::string buf;
      REQUIRE(c1.get(to_key_view(ikey(i)), &buf) >= 0);
      REQUIRE(buf == ival(i));  // v1 values
   }

   auto c2 = root_v2.snapshot_cursor();
   REQUIRE(c2.count_keys() == static_cast<uint64_t>(N * 2));
   for (int i = 0; i < N; ++i)
   {
      std::string buf;
      REQUIRE(c2.get(to_key_view(ikey(i)), &buf) >= 0);
      REQUIRE(buf == ival(i + 5000));  // v2 overwritten values
   }

   auto c3 = root_v3.snapshot_cursor();
   REQUIRE(c3.count_keys() == static_cast<uint64_t>(N * 2 - N / 4));
   for (int i = 0; i < N / 4; ++i)
   {
      std::string buf;
      REQUIRE(c3.get(to_key_view(ikey(i)), &buf) == cursor::value_not_found);
   }
}

// ============================================================
// Persistence: modify/close/reopen/verify with structural transitions
// ============================================================

TEST_CASE("integrity: 3-phase persist with structural changes", "[integrity][persistence]")
{
   const std::string dir = "integrity_3phase";
   const int         N   = 200 / INTEG_SCALE;
   std::map<std::string, std::string> oracle;

   // Phase 1: Bulk insert, creating deep tree with splits
   {
      std::filesystem::remove_all(dir);
      std::filesystem::create_directories(dir + "/data");
      auto db  = database::open(dir);
      auto ses = db->start_write_session();
      auto tx  = ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
      {
         oracle[ikey(i)] = ival(i);
         tx.upsert(to_key_view(ikey(i)), to_value_view(ival(i)));
      }
      tx.commit();
   }

   // Phase 2: Reopen, bulk remove (forces collapses), add new keys
   {
      auto db  = database::open(dir);
      auto ses = db->start_write_session();
      auto tx  = ses->start_transaction(0);

      // Remove 80% of keys — this forces repeated collapse of inner nodes
      for (int i = 0; i < N * 4 / 5; ++i)
      {
         tx.remove(to_key_view(ikey(i)));
         oracle.erase(ikey(i));
      }

      // Add keys with large values (value_node allocation)
      for (int i = N; i < N + N / 5; ++i)
      {
         std::string val(256, static_cast<char>('X' + (i % 3)));
         oracle[ikey(i)] = val;
         tx.upsert(to_key_view(ikey(i)), to_value_view(val));
      }
      tx.commit();
   }

   // Phase 3: Reopen and verify final state matches oracle
   {
      auto db  = database::open(dir);
      auto ses = db->start_write_session();
      auto root = ses->get_root(0);
      REQUIRE(root);

      auto rc = root.snapshot_cursor();
      REQUIRE(rc.count_keys() == oracle.size());

      for (auto& [key, val] : oracle)
      {
         std::string buf;
         INFO("key: " << key);
         REQUIRE(rc.get(to_key_view(key), &buf) >= 0);
         REQUIRE(buf == val);
      }
   }

   std::filesystem::remove_all(dir);
}

// ============================================================
// Sub-transaction rollback preserves structural integrity
// ============================================================

TEST_CASE("integrity: nested sub-transaction rollback under structural pressure", "[integrity][transaction]")
{
   integrity_db t;

   // Build a non-trivial base tree
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < 100; ++i)
         tx.upsert(to_key_view(ikey(i)), to_value_view(ival(i)));
      tx.commit();
   }

   // Start parent tx, make changes, then sub-tx makes more, sub-tx aborts
   {
      auto tx = t.ses->start_transaction(0);
      // Parent modifies some keys
      for (int i = 0; i < 50; ++i)
         tx.upsert(to_key_view(ikey(i)), to_value_view(ival(i + 9000)));

      {
         auto sub = tx.sub_transaction();
         // Sub-tx makes heavy structural changes: add many new keys
         for (int i = 100; i < 300; ++i)
            sub.upsert(to_key_view(ikey(i)), to_value_view(ival(i)));
         // Sub-tx removes some of parent's keys
         for (int i = 0; i < 25; ++i)
            sub.remove(to_key_view(ikey(i)));

         // Verify sub-tx sees its own changes
         REQUIRE(sub.get<std::string>(to_key_view(ikey(200))).has_value());

         sub.abort();  // discard all sub-tx changes
      }

      // Parent should see its own modifications but NOT sub-tx changes
      for (int i = 0; i < 50; ++i)
      {
         auto result = tx.get<std::string>(to_key_view(ikey(i)));
         REQUIRE(result.has_value());
         REQUIRE(*result == ival(i + 9000));  // parent's modification
      }

      // Sub-tx's additions should not be visible
      REQUIRE_FALSE(tx.get<std::string>(to_key_view(ikey(200))).has_value());

      tx.commit();
   }

   // Final verification
   auto root = t.ses->get_root(0);
   auto rc = root.snapshot_cursor();
   REQUIRE(rc.count_keys() == 100);
   for (int i = 0; i < 50; ++i)
   {
      std::string buf;
      REQUIRE(rc.get(to_key_view(ikey(i)), &buf) >= 0);
      REQUIRE(buf == ival(i + 9000));  // modified by parent tx
   }
   for (int i = 50; i < 100; ++i)
   {
      std::string buf;
      REQUIRE(rc.get(to_key_view(ikey(i)), &buf) >= 0);
      REQUIRE(buf == ival(i));  // untouched originals
   }
}

// ============================================================
// range_remove that crosses structural boundaries
// ============================================================

TEST_CASE("integrity: range_remove across leaf/inner boundaries with oracle", "[integrity][oracle]")
{
   integrity_db t;
   auto         cur = start_temp_edit(t);

   std::map<std::string, std::string> oracle;
   const int N = 500 / INTEG_SCALE;

   // Build tree large enough for multiple levels
   for (int i = 0; i < N; ++i)
   {
      oracle[ikey(i)] = ival(i);
      cur.upsert(to_key_view(ikey(i)), to_value_view(ival(i)));
   }

   // Do several range removes that cross leaf boundaries
   std::mt19937 rng(77777);
   for (int trial = 0; trial < 10; ++trial)
   {
      int lo = rng() % N;
      int hi = lo + 10 + (rng() % 40);  // ranges of 10-50 keys
      if (hi > N) hi = N;

      auto lower = ikey(lo);
      auto upper = ikey(hi);
      cur.remove_range_counted(to_key_view(lower), to_key_view(upper));

      auto it = oracle.lower_bound(lower);
      while (it != oracle.end() && it->first < upper)
         it = oracle.erase(it);
   }

   // Verify final state
   verify_oracle(cur, oracle);
}

// ============================================================
// Insert/remove interleaving that oscillates around collapse threshold
// ============================================================

TEST_CASE("integrity: insert/remove oscillation around collapse threshold", "[integrity][structural]")
{
   integrity_db t;
   auto         cur = start_temp_edit(t);

   std::set<std::string> present;

   // Insert 30 keys with distinct first bytes (above collapse threshold of 24)
   for (int i = 0; i < 30; ++i)
   {
      std::string key(1, static_cast<char>(i + 'A'));
      key += "-val";
      cur.upsert(key_view(key.data(), key.size()), to_value("x"));
      present.insert(key);
   }

   // Repeatedly remove down to 20, then re-insert up to 30.
   // Each cycle crosses the collapse threshold (24) in both directions.
   for (int cycle = 0; cycle < 5; ++cycle)
   {
      // Remove 10 (30 → 20, crossing collapse threshold at 24)
      auto it = present.end();
      for (int j = 0; j < 10; ++j)
      {
         --it;
         cur.remove(key_view(it->data(), it->size()));
      }
      // Actually erase from set
      auto erase_it = present.end();
      for (int j = 0; j < 10; ++j)
         --erase_it;
      present.erase(erase_it, present.end());

      REQUIRE(cur.count_keys() == present.size());

      // Verify all remaining keys
      for (auto& k : present)
      {
         std::string buf;
         INFO("cycle " << cycle << " checking " << k);
         REQUIRE(cur.get(key_view(k.data(), k.size()), &buf) >= 0);
      }

      // Re-insert 10 different keys (20 → 30)
      for (int j = 0; j < 10; ++j)
      {
         std::string key = "new" + std::to_string(cycle * 10 + j);
         cur.upsert(to_key_view(key), to_value("y"));
         present.insert(key);
      }

      REQUIRE(cur.count_keys() == present.size());
   }
}
