#include <catch2/catch_all.hpp>
#include <algorithm>
#include <chrono>
#include <numeric>
#include <random>
#include <thread>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/tree_ops.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/value_type.hpp>

using namespace psitri;

constexpr int SCALE = 1;

namespace
{
   struct test_db
   {
      std::string                   dir;
      std::shared_ptr<database>     db;
      std::shared_ptr<write_session> ses;

      test_db(const std::string& name = "public_api_testdb")
          : dir(name)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir + "/data");
         db  = database::open(dir);
         ses = db->start_write_session();
      }

      ~test_db() { std::filesystem::remove_all(dir); }

      /// Validate that every node reachable from root has refcount == 1.
      /// Only valid when no concurrent readers or snapshots exist.
      void validate_unique_refs(uint32_t root_index = 0)
      {
         auto root = ses->get_root(root_index);
         if (!root) return;
         tree_context ctx(root);
         auto ref = root.session()->get_ref(root.address());
         ctx.validate_unique_refs(ref);
      }
   };
}  // namespace

// ============================================================
// write_cursor tests
// ============================================================

TEST_CASE("write_cursor basic CRUD", "[public-api][write-cursor]")
{
   test_db t;

   auto cur = t.ses->create_write_cursor();

   SECTION("insert and get")
   {
      cur->insert(to_key("hello"), to_value("world"));
      auto val = cur->get<std::string>(to_key("hello"));
      REQUIRE(val.has_value());
      REQUIRE(*val == "world");
   }

   SECTION("update existing key")
   {
      cur->insert(to_key("key"), to_value("v1"));
      cur->update(to_key("key"), to_value("v2"));
      auto val = cur->get<std::string>(to_key("key"));
      REQUIRE(*val == "v2");
   }

   SECTION("upsert inserts new and updates existing")
   {
      cur->upsert(to_key("key"), to_value("v1"));
      auto val = cur->get<std::string>(to_key("key"));
      REQUIRE(*val == "v1");

      cur->upsert(to_key("key"), to_value("v2"));
      val = cur->get<std::string>(to_key("key"));
      REQUIRE(*val == "v2");
   }

   SECTION("remove existing key")
   {
      cur->insert(to_key("key"), to_value("value"));
      int removed = cur->remove(to_key("key"));
      REQUIRE(removed >= 0);
      auto val = cur->get<std::string>(to_key("key"));
      REQUIRE_FALSE(val.has_value());
   }

   SECTION("remove nonexistent returns -1")
   {
      REQUIRE(cur->remove(to_key("missing")) == -1);
   }

   SECTION("get nonexistent returns nullopt")
   {
      auto val = cur->get<std::string>(to_key("missing"));
      REQUIRE_FALSE(val.has_value());
   }
}

TEST_CASE("write_cursor read_cursor iteration", "[public-api][write-cursor]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();

   // Insert keys in random order
   cur->insert(to_key("cherry"), to_value("3"));
   cur->insert(to_key("apple"), to_value("1"));
   cur->insert(to_key("banana"), to_value("2"));
   cur->insert(to_key("date"), to_value("4"));

   // Iterate and verify sorted order
   auto rc = cur->read_cursor();
   rc.seek_begin();

   std::vector<std::string> keys;
   while (!rc.is_end())
   {
      keys.emplace_back(rc.key().data(), rc.key().size());
      rc.next();
   }

   REQUIRE(keys.size() == 4);
   REQUIRE(keys[0] == "apple");
   REQUIRE(keys[1] == "banana");
   REQUIRE(keys[2] == "cherry");
   REQUIRE(keys[3] == "date");
}

TEST_CASE("write_cursor get into buffer", "[public-api][write-cursor]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();

   cur->insert(to_key("key"), to_value("hello world"));

   std::string buf;
   int32_t     result = cur->get(to_key("key"), &buf);
   REQUIRE(result >= 0);
   REQUIRE(buf == "hello world");

   result = cur->get(to_key("missing"), &buf);
   REQUIRE(result < 0);
}

// ============================================================
// transaction tests
// ============================================================

TEST_CASE("transaction commit persists root", "[public-api][transaction]")
{
   test_db t;

   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_key("persisted"), to_value("yes"));
      tx.commit();
   }

   // Verify root was saved — read it back
   auto root = t.ses->get_root(0);
   REQUIRE(root);

   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("persisted"), &buf) >= 0);
   REQUIRE(buf == "yes");
}

TEST_CASE("transaction abort discards changes", "[public-api][transaction]")
{
   test_db t;

   // First commit something
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_key("base"), to_value("original"));
      tx.commit();
   }

   // Now start a transaction, add data, then abort
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_key("discarded"), to_value("gone"));
      tx.abort();
   }

   // Verify "discarded" is not present, "base" is still there
   auto root = t.ses->get_root(0);
   REQUIRE(root);

   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("base"), &buf) >= 0);
   REQUIRE(buf == "original");
   REQUIRE(c.get(to_key("discarded"), &buf) < 0);
}

TEST_CASE("transaction destructor aborts", "[public-api][transaction]")
{
   test_db t;

   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_key("base"), to_value("saved"));
      tx.commit();
   }

   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_key("lost"), to_value("gone"));
      // destructor should abort
   }

   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("base"), &buf) >= 0);
   REQUIRE(c.get(to_key("lost"), &buf) < 0);
}

TEST_CASE("transaction sub_transaction commit", "[public-api][transaction]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0);
   tx.upsert(to_key("outer"), to_value("yes"));

   {
      auto sub = tx.sub_transaction();
      sub.upsert(to_key("inner"), to_value("yes"));
      sub.commit();  // commits back to parent tx
   }

   // Both keys visible before outer commit
   std::string buf;
   REQUIRE(tx.get(to_key("outer"), &buf) >= 0);
   REQUIRE(tx.get(to_key("inner"), &buf) >= 0);

   tx.commit();

   // Both persisted
   auto root = t.ses->get_root(0);
   cursor c(root);
   REQUIRE(c.get(to_key("outer"), &buf) >= 0);
   REQUIRE(c.get(to_key("inner"), &buf) >= 0);
}

TEST_CASE("transaction sub_transaction abort", "[public-api][transaction]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0);
   tx.upsert(to_key("outer"), to_value("yes"));

   {
      auto sub = tx.sub_transaction();
      sub.upsert(to_key("inner_lost"), to_value("gone"));
      sub.abort();
   }

   // "inner_lost" should not be visible in parent
   std::string buf;
   REQUIRE(tx.get(to_key("outer"), &buf) >= 0);
   REQUIRE(tx.get(to_key("inner_lost"), &buf) < 0);

   tx.commit();
}

TEST_CASE("transaction read_cursor snapshot", "[public-api][transaction]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0);
   tx.upsert(to_key("a"), to_value("1"));
   tx.upsert(to_key("b"), to_value("2"));
   tx.upsert(to_key("c"), to_value("3"));

   auto rc = tx.read_cursor();
   rc.seek_begin();

   int count = 0;
   while (!rc.is_end())
   {
      ++count;
      rc.next();
   }
   REQUIRE(count == 3);

   tx.commit();
}

// ============================================================
// session tests
// ============================================================

TEST_CASE("session get_root empty returns null", "[public-api][session]")
{
   test_db t;
   auto    root = t.ses->get_root(0);
   REQUIRE_FALSE(root);
}

TEST_CASE("session set_root and get_root round-trip", "[public-api][session]")
{
   test_db t;

   // Build a tree via write_cursor
   auto cur = t.ses->create_write_cursor();
   cur->insert(to_key("key"), to_value("value"));
   auto root = cur->root();

   // Save and reload
   t.ses->set_root(0, root, sal::sync_type::none);
   auto loaded = t.ses->get_root(0);
   REQUIRE(loaded);

   cursor c(loaded);
   std::string buf;
   REQUIRE(c.get(to_key("key"), &buf) >= 0);
   REQUIRE(buf == "value");
}

TEST_CASE("multiple independent roots", "[public-api][session]")
{
   test_db t;

   // Use two different root indices
   {
      auto tx0 = t.ses->start_transaction(0);
      tx0.upsert(to_key("root0_key"), to_value("root0_val"));
      tx0.commit();
   }
   {
      auto tx1 = t.ses->start_transaction(1);
      tx1.upsert(to_key("root1_key"), to_value("root1_val"));
      tx1.commit();
   }

   // Each root has only its own data
   std::string buf;

   auto r0 = t.ses->get_root(0);
   cursor c0(r0);
   REQUIRE(c0.get(to_key("root0_key"), &buf) >= 0);
   REQUIRE(buf == "root0_val");
   REQUIRE(c0.get(to_key("root1_key"), &buf) < 0);

   auto r1 = t.ses->get_root(1);
   cursor c1(r1);
   REQUIRE(c1.get(to_key("root1_key"), &buf) >= 0);
   REQUIRE(buf == "root1_val");
   REQUIRE(c1.get(to_key("root0_key"), &buf) < 0);
}

// ============================================================
// database reopen test
// ============================================================

TEST_CASE("database reopen preserves data", "[public-api][database]")
{
   const std::string dir = "reopen_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   // Write data and close
   {
      auto db  = database::open(dir);
      auto ses = db->start_write_session();
      ses->set_sync(sal::sync_type::full);
      auto tx  = ses->start_transaction(0);
      tx.upsert(to_key("survive"), to_value("reopen"));
      tx.commit();
   }

   // Reopen and verify
   {
      auto db  = database::open(dir);
      auto ses = db->start_write_session();
      auto root = ses->get_root(0);
      REQUIRE(root);

      cursor c(root);
      std::string buf;
      REQUIRE(c.get(to_key("survive"), &buf) >= 0);
      REQUIRE(buf == "reopen");
   }

   std::filesystem::remove_all(dir);
}

// ============================================================
// bulk operations test
// ============================================================

TEST_CASE("bulk insert and verify", "[public-api][bulk]")
{
   test_db t;

   const int N = 10000 / SCALE;

   auto tx = t.ses->start_transaction(0);
   for (int i = 0; i < N; ++i)
   {
      std::string key = "key_" + std::to_string(i);
      std::string val = "val_" + std::to_string(i);
      tx.insert(to_key_view(key), to_value_view(val));
   }
   tx.commit();

   // Verify all via get
   auto root = t.ses->get_root(0);
   cursor c(root);
   for (int i = 0; i < N; ++i)
   {
      std::string key = "key_" + std::to_string(i);
      std::string expected = "val_" + std::to_string(i);
      std::string buf;
      REQUIRE(c.get(to_key_view(key), &buf) >= 0);
      REQUIRE(buf == expected);
   }

   // Verify count via iteration
   c.seek_begin();
   int count = 0;
   while (!c.is_end())
   {
      ++count;
      c.next();
   }
   REQUIRE(count == N);
}

TEST_CASE("bulk insert then remove all", "[public-api][bulk]")
{
   test_db t;

   const int N = 1000 / SCALE;

   // Insert
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
      {
         std::string key = "k" + std::to_string(i);
         std::string val = "v" + std::to_string(i);
         tx.upsert(to_key_view(key), to_value_view(val));
      }
      tx.commit();
   }
   t.validate_unique_refs();

   // Remove all
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
      {
         std::string key = "k" + std::to_string(i);
         REQUIRE(tx.remove(to_key_view(key)) >= 0);
      }
      tx.commit();
   }

   // Verify empty
   auto root = t.ses->get_root(0);
   if (root)
   {
      cursor c(root);
      c.seek_begin();
      REQUIRE(c.is_end());
   }
}

// ============================================================
// Remove reference count tests
// ============================================================

TEST_CASE("remove does not leak references - repeated insert/remove cycles", "[public-api][remove][refcount]")
{
   test_db t;

   const int N = 500 / SCALE;  // keys per cycle
   const int CYCLES = 50 / SCALE;

   for (int cycle = 0; cycle < CYCLES; ++cycle)
   {
      // Insert N keys
      {
         auto tx = t.ses->start_transaction(0);
         for (int i = 0; i < N; ++i)
         {
            char key[32];
            snprintf(key, sizeof(key), "key%06d", i);
            std::string val(100, 'A' + (cycle % 26));
            tx.upsert(to_key_view(std::string(key)), to_value_view(val));
         }
         tx.commit();
      }
      t.validate_unique_refs();

      // Remove all N keys
      {
         auto tx = t.ses->start_transaction(0);
         for (int i = 0; i < N; ++i)
         {
            char key[32];
            snprintf(key, sizeof(key), "key%06d", i);
            tx.remove(to_key_view(std::string(key)));
         }
         tx.commit();
      }
   }
   // If we get here without "reference count exceeded limits", references are clean
}

TEST_CASE("remove nonexistent keys does not leak references", "[public-api][remove][refcount]")
{
   test_db t;

   const int N = 100;

   // Insert some keys
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
      {
         char key[32];
         snprintf(key, sizeof(key), "key%06d", i);
         tx.upsert(to_key_view(std::string(key)), to_value("value"));
      }
      tx.commit();
   }

   // Repeatedly remove nonexistent keys (should be no-ops)
   for (int cycle = 0; cycle < 100; ++cycle)
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
      {
         char key[32];
         snprintf(key, sizeof(key), "missing%06d_%03d", i, cycle);
         tx.remove(to_key_view(std::string(key)));
      }
      tx.commit();
   }

   // Verify original keys still present and refs are clean
   t.validate_unique_refs();
   {
      cursor c(t.ses->get_root(0));
      c.seek_begin();
      int count = 0;
      if (!c.is_end())
      {
         do { ++count; } while (c.next());
      }
      REQUIRE(count == N);
   }
}

TEST_CASE("batched remove across multiple transactions", "[public-api][remove][refcount]")
{
   test_db t;

   const int N = 5000 / SCALE;
   const int BATCH = 100;

   // Insert all keys in one transaction
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
      {
         char key[32];
         snprintf(key, sizeof(key), "k%08d", i);
         std::string val(50, 'x');
         tx.upsert(to_key_view(std::string(key)), to_value_view(val));
      }
      tx.commit();
   }
   t.validate_unique_refs();

   // Remove in batches (simulates benchmark pattern)
   for (int i = 0; i < N; i += BATCH)
   {
      auto tx  = t.ses->start_transaction(0);
      int  end = std::min(i + BATCH, N);
      for (int j = i; j < end; ++j)
      {
         char key[32];
         snprintf(key, sizeof(key), "k%08d", j);
         REQUIRE(tx.remove(to_key_view(std::string(key))) >= 0);
      }
      tx.commit();
   }

   // Verify empty
   auto root = t.ses->get_root(0);
   if (root)
   {
      cursor c(root);
      c.seek_begin();
      REQUIRE(c.is_end());
   }
}

TEST_CASE("remove same key repeatedly does not leak references", "[public-api][remove][refcount]")
{
   test_db t;

   // Insert one key
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_key_view("thekey"), to_value("thevalue"));
      tx.commit();
   }

   // Remove the same key many times across many transactions
   // (simulates zipfian pattern where same key is deleted repeatedly)
   for (int i = 0; i < 1000 / SCALE; ++i)
   {
      auto tx = t.ses->start_transaction(0);
      tx.remove(to_key_view("thekey"));
      tx.commit();
   }

   // Insert it back and verify it works
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_key_view("thekey"), to_value("restored"));
      tx.commit();
   }

   cursor c(t.ses->get_root(0));
   auto val = c.get<std::string>(to_key_view("thekey"));
   REQUIRE(val.has_value());
   REQUIRE(*val == "restored");
}

TEST_CASE("high-frequency insert/remove on overlapping keys", "[public-api][remove][refcount]")
{
   test_db t;

   // Simulate zipfian-like access: small key space, many operations
   const int KEY_SPACE = 50;
   const int OPS = 10000 / SCALE;

   for (int op = 0; op < OPS; ++op)
   {
      auto tx = t.ses->start_transaction(0);
      int  k  = op % KEY_SPACE;
      char key[32];
      snprintf(key, sizeof(key), "zk%04d", k);

      if (op % 3 == 0)
      {
         // Remove
         tx.remove(to_key_view(std::string(key)));
      }
      else
      {
         // Upsert
         char val[32];
         snprintf(val, sizeof(val), "v%d", op);
         tx.upsert(to_key_view(std::string(key)), to_value_view(std::string(val)));
      }
      tx.commit();
   }

   // Verify tree is consistent: can iterate without crash
   cursor c(t.ses->get_root(0));
   c.seek_begin();
   int count = 0;
   if (!c.is_end())
   {
      do { ++count; } while (c.next());
   }
   // Some keys should remain (2/3 of ops are upserts, 1/3 removes)
   REQUIRE(count > 0);
   REQUIRE(count <= KEY_SPACE);
}

// Helper: insert N keys in one transaction
static void insert_keys(test_db& t, int N, const char* prefix = "key", const char* vprefix = "val")
{
   auto tx = t.ses->start_transaction(0);
   for (int i = 0; i < N; ++i)
   {
      char key[64], val[64];
      snprintf(key, sizeof(key), "%s%06d", prefix, i);
      snprintf(val, sizeof(val), "%s%06d", vprefix, i);
      tx.upsert(to_key_view(std::string(key)), to_value_view(std::string(val)));
   }
   tx.commit();
}

// Helper: remove N keys in one transaction
static void remove_keys(test_db& t, int N, const char* prefix = "key")
{
   auto tx = t.ses->start_transaction(0);
   for (int i = 0; i < N; ++i)
   {
      char key[64];
      snprintf(key, sizeof(key), "%s%06d", prefix, i);
      tx.remove(to_key_view(std::string(key)));
   }
   tx.commit();
}

// Helper: count keys via iteration
static int count_keys(test_db& t)
{
   auto root = t.ses->get_root(0);
   if (!root) return 0;
   cursor c(root);
   c.seek_begin();
   int count = 0;
   if (!c.is_end())
      do { ++count; } while (c.next());
   return count;
}

TEST_CASE("insert-removeall-reinsert 3 keys", "[public-api][remove][refcount]")
{
   test_db t;
   insert_keys(t, 3);
   t.validate_unique_refs();
   remove_keys(t, 3);
   REQUIRE(count_keys(t) == 0);
   insert_keys(t, 3, "key", "new");
   t.validate_unique_refs();
   REQUIRE(count_keys(t) == 3);
}

TEST_CASE("insert-removeall-reinsert 50 keys", "[public-api][remove][refcount]")
{
   test_db t;
   insert_keys(t, 50);
   t.validate_unique_refs();
   remove_keys(t, 50);
   REQUIRE(count_keys(t) == 0);
   insert_keys(t, 50, "key", "new");
   t.validate_unique_refs();
   REQUIRE(count_keys(t) == 50);
}

TEST_CASE("insert-removeall-reinsert 500 keys", "[public-api][remove][refcount]")
{
   test_db t;
   insert_keys(t, 500);
   t.validate_unique_refs();
   remove_keys(t, 500);
   REQUIRE(count_keys(t) == 0);
   insert_keys(t, 500, "key", "new");
   t.validate_unique_refs();
   REQUIRE(count_keys(t) == 500);
}

TEST_CASE("insert-removeall-reinsert 5000 keys", "[public-api][remove][refcount]")
{
   test_db t;
   const int N = 5000 / SCALE;
   insert_keys(t, N);
   t.validate_unique_refs();
   remove_keys(t, N);
   REQUIRE(count_keys(t) == 0);
   insert_keys(t, N, "key", "new");
   t.validate_unique_refs();
   REQUIRE(count_keys(t) == N);
}

TEST_CASE("50 cycles of insert-removeall 500 keys", "[public-api][remove][refcount]")
{
   test_db t;
   const int N = 500 / SCALE;
   for (int cycle = 0; cycle < 50 / SCALE; ++cycle)
   {
      INFO("cycle " << cycle);
      insert_keys(t, N);
      t.validate_unique_refs();
      remove_keys(t, N);
   }
}

TEST_CASE("batched remove 100 at a time from 5000", "[public-api][remove][refcount]")
{
   test_db t;
   const int N = 5000 / SCALE;
   insert_keys(t, N);
   t.validate_unique_refs();
   for (int i = 0; i < N; i += 100)
   {
      auto tx  = t.ses->start_transaction(0);
      for (int j = i; j < i + 100 && j < N; ++j)
      {
         char key[64];
         snprintf(key, sizeof(key), "key%06d", j);
         tx.remove(to_key_view(std::string(key)));
      }
      tx.commit();
   }
   REQUIRE(count_keys(t) == 0);
}

TEST_CASE("remove half then reinsert", "[public-api][remove][refcount]")
{
   test_db t;
   const int N = 1000 / SCALE;
   insert_keys(t, N);
   t.validate_unique_refs();
   // Remove even keys
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N; i += 2)
      {
         char key[64];
         snprintf(key, sizeof(key), "key%06d", i);
         tx.remove(to_key_view(std::string(key)));
      }
      tx.commit();
   }
   t.validate_unique_refs();
   REQUIRE(count_keys(t) == N / 2);
   // Reinsert even keys
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N; i += 2)
      {
         char key[64];
         snprintf(key, sizeof(key), "key%06d", i);
         tx.upsert(to_key_view(std::string(key)), to_value("reinserted"));
      }
      tx.commit();
   }
   t.validate_unique_refs();
   REQUIRE(count_keys(t) == N);
}

TEST_CASE("insert-removeall-reinsert 500 keys with large values", "[public-api][remove][refcount]")
{
   test_db t;
   const int N = 500 / SCALE;
   std::string big_val(200, 'X');  // large enough to force value_node storage
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
      {
         char key[64];
         snprintf(key, sizeof(key), "key%06d", i);
         tx.upsert(to_key_view(std::string(key)), to_value_view(big_val));
      }
      tx.commit();
   }
   t.validate_unique_refs();
   remove_keys(t, N);
   REQUIRE(count_keys(t) == 0);
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
      {
         char key[64];
         snprintf(key, sizeof(key), "key%06d", i);
         tx.upsert(to_key_view(std::string(key)), to_value_view(big_val));
      }
      tx.commit();
   }
   t.validate_unique_refs();
   REQUIRE(count_keys(t) == N);
}

TEST_CASE("5 cycles insert-removeall 500 keys with large values", "[public-api][remove][refcount]")
{
   test_db t;
   const int N = 500 / SCALE;
   std::string big_val(200, 'X');
   for (int cycle = 0; cycle < 5; ++cycle)
   {
      INFO("cycle " << cycle);
      {
         auto tx = t.ses->start_transaction(0);
         for (int i = 0; i < N; ++i)
         {
            char key[64];
            snprintf(key, sizeof(key), "key%06d", i);
            tx.upsert(to_key_view(std::string(key)), to_value_view(big_val));
         }
         tx.commit();
      }
      t.validate_unique_refs();
      remove_keys(t, N);
   }
}

TEST_CASE("interleaved insert and remove in same transaction", "[public-api][remove][refcount]")
{
   test_db t;
   insert_keys(t, 100);
   // In one tx: remove some, insert others
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < 50; ++i)
      {
         char key[64];
         snprintf(key, sizeof(key), "key%06d", i);
         tx.remove(to_key_view(std::string(key)));
      }
      for (int i = 100; i < 150; ++i)
      {
         char key[64], val[64];
         snprintf(key, sizeof(key), "key%06d", i);
         snprintf(val, sizeof(val), "val%06d", i);
         tx.upsert(to_key_view(std::string(key)), to_value_view(std::string(val)));
      }
      tx.commit();
   }
   t.validate_unique_refs();
   REQUIRE(count_keys(t) == 100);  // removed 50, added 50
}

// ============================================================
// Orphan / leak detection tests
//
// These tests verify that after removing keys, no unreachable
// (orphaned) nodes remain allocated.  The key invariant:
//   reachable nodes (via tree walk) == total allocated objects
// When the tree is empty, both should be 0.
//
// Important: the SAL allocator uses an async release queue
// drained by a compactor thread.  After releasing refs we must
// wait for the queue to drain before comparing counts.
// ============================================================

/// Wait for the compactor to drain the release queue and for
/// get_total_allocated_objects() to stabilize.
static void wait_for_compactor(test_db& t, int max_ms = 5000)
{
   if (!t.db->wait_for_compactor(std::chrono::milliseconds(max_ms)))
   {
      WARN("compactor did not drain within " << max_ms << "ms, pending="
           << t.ses->get_pending_release_count());
   }
}

/// Helper: count reachable nodes by walking the tree from root
static uint64_t reachable_nodes(test_db& t)
{
   auto root = t.ses->get_root(0);
   if (!root)
      return 0;
   tree_context ctx(root);
   uint64_t count = ctx.get_stats().total_nodes();
   // Count the version CB attached to the root slot (not a tree node)
   if (root.ver() != sal::null_ptr_address)
      ++count;
   return count;
}

/// Helper: assert no orphaned nodes exist.
/// reachable (tree walk) must equal total allocated objects.
static void require_no_orphans(test_db& t, const char* context = "")
{
   wait_for_compactor(t);
   auto root      = t.ses->get_root(0);
   uint64_t reachable = 0;
   if (root)
   {
      tree_context ctx(root);
      reachable = ctx.get_stats().total_nodes();
      // Count the version CB attached to the root slot (not a tree node)
      if (root.ver() != sal::null_ptr_address)
         ++reachable;
   }
   uint64_t allocated = t.ses->get_total_allocated_objects();
   INFO(context << " reachable=" << reachable << " allocated=" << allocated);
   REQUIRE(reachable == allocated);
}

/// Helper: assert tree is completely empty and nothing is allocated
static void require_empty_no_leaks(test_db& t, const char* context = "")
{
   wait_for_compactor(t);
   REQUIRE(count_keys(t) == 0);
   auto root = t.ses->get_root(0);
   INFO(context << " root should be null after removing all keys");
   REQUIRE_FALSE(root);
   uint64_t allocated = t.ses->get_total_allocated_objects();
   INFO(context << " allocated=" << allocated << " (expected 0)");
   REQUIRE(allocated == 0);
}

TEST_CASE("leak: insert then release root leaves zero allocated objects",
          "[public-api][leak]")
{
   test_db t;
   const int N = 2000 / SCALE;

   insert_keys(t, N);
   require_no_orphans(t, "after insert");

   // Release root — should cascade-release all nodes
   t.ses->set_root(0, {}, sal::sync_type::none);

   // Wait for compactor to drain any queued releases
   wait_for_compactor(t);

   uint64_t allocated = t.ses->get_total_allocated_objects();
   INFO("allocated after root release=" << allocated << " (expected 0)");
   REQUIRE(allocated == 0);
}

TEST_CASE("leak: insert large values then release root leaves zero allocated",
          "[public-api][leak]")
{
   test_db t;
   const int N = 500 / SCALE;
   std::string big_val(300, 'V');  // forces value_node allocation

   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
      {
         char key[64];
         snprintf(key, sizeof(key), "key%06d", i);
         tx.upsert(to_key_view(std::string(key)), to_value_view(big_val));
      }
      tx.commit();
   }
   require_no_orphans(t, "after insert large values");

   // Release root
   t.ses->set_root(0, {}, sal::sync_type::none);
   wait_for_compactor(t);

   uint64_t allocated = t.ses->get_total_allocated_objects();
   INFO("allocated after root release=" << allocated << " (expected 0)");
   REQUIRE(allocated == 0);
}

TEST_CASE("leak: insert-release-reinsert cycles via root release",
          "[public-api][leak]")
{
   test_db t;
   const int N      = 500;
   const int CYCLES = 5;

   for (int cycle = 0; cycle < CYCLES; ++cycle)
   {
      INFO("cycle " << cycle);
      insert_keys(t, N);
      require_no_orphans(t, "after insert");

      // Release root
      t.ses->set_root(0, {}, sal::sync_type::none);
      wait_for_compactor(t);

      uint64_t allocated = t.ses->get_total_allocated_objects();
      INFO("cycle " << cycle << " allocated after root release=" << allocated);
      REQUIRE(allocated == 0);
   }
}

TEST_CASE("leak: remove-all sequential leaves zero allocated objects",
          "[public-api][remove][leak]")
{
   test_db t;
   const int N = 2000 / SCALE;

   insert_keys(t, N);
   require_no_orphans(t, "after insert");

   remove_keys(t, N);
   require_empty_no_leaks(t, "after sequential remove-all");
}

TEST_CASE("leak: diagnose remove leak scaling",
          "[public-api][remove][leak][diag]")
{
   for (int N : {10, 50, 100, 200 / SCALE, 500 / SCALE, 1000 / SCALE, 2000 / SCALE})
   {
      test_db t("diag_testdb");
      insert_keys(t, N);
      wait_for_compactor(t);
      uint64_t after_insert = t.ses->get_total_allocated_objects();
      uint64_t reachable    = reachable_nodes(t);
      WARN("N=" << N << " after_insert: reachable=" << reachable
                << " allocated=" << after_insert);

      remove_keys(t, N);
      wait_for_compactor(t);
      uint64_t after_remove = t.ses->get_total_allocated_objects();
      WARN("N=" << N << " after_remove: allocated=" << after_remove
                << " leaked=" << after_remove);

      // Also try releasing root to see if that reclaims more
      t.ses->set_root(0, {}, sal::sync_type::none);
      wait_for_compactor(t);
      uint64_t after_release = t.ses->get_total_allocated_objects();
      WARN("N=" << N << " after_root_release: allocated=" << after_release);
   }
}

TEST_CASE("leak: remove-all random order leaves zero allocated objects",
          "[public-api][remove][leak]")
{
   test_db t;
   const int N = 2000 / SCALE;

   insert_keys(t, N);
   require_no_orphans(t, "after insert");

   // Build shuffled index list
   std::vector<int> indices(N);
   std::iota(indices.begin(), indices.end(), 0);
   std::mt19937 rng(42);  // deterministic seed
   std::shuffle(indices.begin(), indices.end(), rng);

   // Remove in random order
   {
      auto tx = t.ses->start_transaction(0);
      for (int idx : indices)
      {
         char key[64];
         snprintf(key, sizeof(key), "key%06d", idx);
         REQUIRE(tx.remove(to_key_view(std::string(key))) >= 0);
      }
      tx.commit();
   }

   require_empty_no_leaks(t, "after random-order remove-all");
}

TEST_CASE("leak: remove-all reverse order leaves zero allocated objects",
          "[public-api][remove][leak]")
{
   test_db t;
   const int N = 2000 / SCALE;

   insert_keys(t, N);

   // Remove in reverse order (exercises different collapse paths)
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = N - 1; i >= 0; --i)
      {
         char key[64];
         snprintf(key, sizeof(key), "key%06d", i);
         REQUIRE(tx.remove(to_key_view(std::string(key))) >= 0);
      }
      tx.commit();
   }

   require_empty_no_leaks(t, "after reverse remove-all");
}

TEST_CASE("leak: remove half - reachable equals allocated",
          "[public-api][remove][leak]")
{
   test_db t;
   const int N = 2000 / SCALE;

   insert_keys(t, N);
   require_no_orphans(t, "after insert");

   // Remove odd-indexed keys
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 1; i < N; i += 2)
      {
         char key[64];
         snprintf(key, sizeof(key), "key%06d", i);
         tx.remove(to_key_view(std::string(key)));
      }
      tx.commit();
   }

   REQUIRE(count_keys(t) == N / 2);
   require_no_orphans(t, "after removing odd keys");
   t.validate_unique_refs();
}

TEST_CASE("leak: insert-remove-reinsert cycles have no cumulative leaks",
          "[public-api][remove][leak]")
{
   test_db t;
   const int N      = 500;
   const int CYCLES = 10;

   for (int cycle = 0; cycle < CYCLES; ++cycle)
   {
      INFO("cycle " << cycle);
      insert_keys(t, N);
      require_no_orphans(t, "after insert");
      remove_keys(t, N);
      require_empty_no_leaks(t, "after remove-all");
   }
}

TEST_CASE("leak: large values - remove-all leaves zero allocated",
          "[public-api][remove][leak]")
{
   test_db t;
   const int N = 500 / SCALE;
   std::string big_val(300, 'V');  // forces value_node allocation

   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
      {
         char key[64];
         snprintf(key, sizeof(key), "key%06d", i);
         tx.upsert(to_key_view(std::string(key)), to_value_view(big_val));
      }
      tx.commit();
   }
   require_no_orphans(t, "after insert large values");

   remove_keys(t, N);
   require_empty_no_leaks(t, "after remove-all large values");
}

TEST_CASE("leak: mixed key sizes - remove-all leaves zero allocated",
          "[public-api][remove][leak]")
{
   test_db t;

   // Insert short and medium keys to trigger inner_prefix_node creation
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < 500 / SCALE; ++i)
      {
         char key[64];
         snprintf(key, sizeof(key), "s%d", i);
         tx.upsert(to_key_view(std::string(key)), to_value("v"));
      }
      for (int i = 0; i < 200 / SCALE; ++i)
      {
         std::string key = "medium_prefix_" + std::to_string(i);
         tx.upsert(to_key_view(key), to_value("val"));
      }
      for (int i = 0; i < 100 / SCALE; ++i)
      {
         std::string key(100, 'k');
         key += std::to_string(i);
         tx.upsert(to_key_view(key), to_value("longval"));
      }
      tx.commit();
   }
   require_no_orphans(t, "after mixed insert");

   // Remove all in one transaction
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < 500 / SCALE; ++i)
      {
         char key[64];
         snprintf(key, sizeof(key), "s%d", i);
         tx.remove(to_key_view(std::string(key)));
      }
      for (int i = 0; i < 200 / SCALE; ++i)
      {
         std::string key = "medium_prefix_" + std::to_string(i);
         tx.remove(to_key_view(key));
      }
      for (int i = 0; i < 100 / SCALE; ++i)
      {
         std::string key(100, 'k');
         key += std::to_string(i);
         tx.remove(to_key_view(key));
      }
      tx.commit();
   }
   require_empty_no_leaks(t, "after mixed remove-all");
}

TEST_CASE("leak: batched remove across transactions - no orphans",
          "[public-api][remove][leak]")
{
   test_db t;
   const int N     = 3000;
   const int BATCH = 100;

   insert_keys(t, N);
   require_no_orphans(t, "after insert");

   // Remove in batches, each batch in its own transaction
   for (int i = 0; i < N; i += BATCH)
   {
      auto tx  = t.ses->start_transaction(0);
      int  end = std::min(i + BATCH, N);
      for (int j = i; j < end; ++j)
      {
         char key[64];
         snprintf(key, sizeof(key), "key%06d", j);
         tx.remove(to_key_view(std::string(key)));
      }
      tx.commit();

      // Check no orphans after each batch
      require_no_orphans(t, "after batch remove");
   }

   require_empty_no_leaks(t, "after all batches");
}

TEST_CASE("leak: snapshot held during remove - no orphans after release",
          "[public-api][remove][leak]")
{
   test_db t;
   const int N = 1000 / SCALE;

   insert_keys(t, N);

   // Take a snapshot (retain the root)
   auto snapshot = t.ses->get_root(0);
   REQUIRE(snapshot);

   // Remove all keys while snapshot is held
   remove_keys(t, N);

   // Tree should be empty from the current root's perspective
   REQUIRE(count_keys(t) == 0);

   // But allocated objects may be non-zero because snapshot holds refs
   // The snapshot should still be readable
   {
      cursor c(snapshot);
      c.seek_begin();
      int snap_count = 0;
      if (!c.is_end())
         do { ++snap_count; } while (c.next());
      REQUIRE(snap_count == N);
   }

   // Release the snapshot
   snapshot = {};

   // Now set root to null to release all references
   t.ses->set_root(0, {}, sal::sync_type::none);

   // Wait for compactor to drain release queue
   wait_for_compactor(t);

   // After releasing snapshot, everything should be freed
   uint64_t allocated = t.ses->get_total_allocated_objects();
   INFO("allocated after snapshot release=" << allocated << " (expected 0)");
   REQUIRE(allocated == 0);
}

TEST_CASE("leak: interleaved insert/remove in same tx - no orphans",
          "[public-api][remove][leak]")
{
   test_db t;
   const int N = 500 / SCALE;

   insert_keys(t, N);
   require_no_orphans(t, "after initial insert");

   // In one tx: remove first half, insert new keys
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N / 2; ++i)
      {
         char key[64];
         snprintf(key, sizeof(key), "key%06d", i);
         tx.remove(to_key_view(std::string(key)));
      }
      for (int i = N; i < N + N / 2; ++i)
      {
         char key[64], val[64];
         snprintf(key, sizeof(key), "key%06d", i);
         snprintf(val, sizeof(val), "val%06d", i);
         tx.upsert(to_key_view(std::string(key)), to_value_view(std::string(val)));
      }
      tx.commit();
   }

   REQUIRE(count_keys(t) == N);
   require_no_orphans(t, "after interleaved insert/remove");

   // Now remove everything
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = N / 2; i < N + N / 2; ++i)
      {
         char key[64];
         snprintf(key, sizeof(key), "key%06d", i);
         tx.remove(to_key_view(std::string(key)));
      }
      tx.commit();
   }
   require_empty_no_leaks(t, "after final remove-all");
}

// ============================================================
// Range-remove leak detection tests
// ============================================================

TEST_CASE("leak: range_remove all keys leaves zero allocated",
          "[public-api][range_remove][leak]")
{
   test_db t;
   const int N = 2000 / SCALE;

   insert_keys(t, N);
   require_no_orphans(t, "after insert");

   {
      auto tx = t.ses->start_transaction(0);
      uint64_t removed = tx.remove_range("", max_key);
      REQUIRE(removed == N);
      tx.commit();
   }
   require_empty_no_leaks(t, "after range_remove all");
}

TEST_CASE("leak: range_remove subset leaves no orphans",
          "[public-api][range_remove][leak]")
{
   test_db t;
   const int N = 2000 / SCALE;

   insert_keys(t, N);
   require_no_orphans(t, "after insert");

   // Remove middle third
   char lo_buf[64], hi_buf[64];
   snprintf(lo_buf, sizeof(lo_buf), "key%06d", N / 3);
   snprintf(hi_buf, sizeof(hi_buf), "key%06d", 2 * N / 3);

   {
      auto tx = t.ses->start_transaction(0);
      uint64_t removed = tx.remove_range(lo_buf, hi_buf);
      REQUIRE(removed > 0);
      tx.commit();
   }
   require_no_orphans(t, "after range_remove middle third");

   // Remove the rest
   {
      auto tx = t.ses->start_transaction(0);
      tx.remove_range("", max_key);
      tx.commit();
   }
   require_empty_no_leaks(t, "after range_remove everything remaining");
}

TEST_CASE("leak: range_remove subset scaling diagnosis",
          "[public-api][range_remove][leak][diag_rr]")
{
   for (int N : {10, 50, 100, 200, 300, 400, 500, 1000 / SCALE})
   {
      test_db t("diag_rr_testdb");
      insert_keys(t, N);
      wait_for_compactor(t);

      uint64_t reachable_before = reachable_nodes(t);
      uint64_t allocated_before = t.ses->get_total_allocated_objects();
      WARN("N=" << N << " before: reachable=" << reachable_before
                << " allocated=" << allocated_before);

      // Remove middle third via transaction (forces shared mode)
      char lo_buf[64], hi_buf[64];
      snprintf(lo_buf, sizeof(lo_buf), "key%06d", N / 3);
      snprintf(hi_buf, sizeof(hi_buf), "key%06d", 2 * N / 3);

      {
         auto tx = t.ses->start_transaction(0);
         uint64_t removed = tx.remove_range(lo_buf, hi_buf);
         WARN("N=" << N << " removed=" << removed);
         tx.commit();
      }
      wait_for_compactor(t);

      uint64_t reachable_after = reachable_nodes(t);
      uint64_t allocated_after = t.ses->get_total_allocated_objects();
      WARN("N=" << N << " after: reachable=" << reachable_after
                << " allocated=" << allocated_after
                << " diff=" << (int64_t)(reachable_after - allocated_after));

      CHECK(reachable_after == allocated_after);
   }
}

TEST_CASE("leak: range_remove with large values leaves no orphans",
          "[public-api][range_remove][leak]")
{
   test_db t;
   const int N = 500 / SCALE;
   std::string big_val(300, 'X');  // forces value_node allocation

   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
      {
         char key[64];
         snprintf(key, sizeof(key), "key%06d", i);
         tx.upsert(to_key_view(std::string(key)), to_value_view(big_val));
      }
      tx.commit();
   }
   require_no_orphans(t, "after insert large values");

   // Range-remove half
   char hi_buf[64];
   snprintf(hi_buf, sizeof(hi_buf), "key%06d", N / 2);

   {
      auto tx = t.ses->start_transaction(0);
      tx.remove_range("", hi_buf);
      tx.commit();
   }
   require_no_orphans(t, "after range_remove half with large values");

   // Remove the rest
   {
      auto tx = t.ses->start_transaction(0);
      tx.remove_range("", max_key);
      tx.commit();
   }
   require_empty_no_leaks(t, "after range_remove all with large values");
}

TEST_CASE("leak: repeated range_remove cycles leave zero allocated",
          "[public-api][range_remove][leak]")
{
   test_db t;
   const int N = 1000 / SCALE;

   for (int cycle = 0; cycle < 3; ++cycle)
   {
      INFO("cycle " << cycle);
      insert_keys(t, N);
      require_no_orphans(t, "after insert");

      // Remove everything via range
      {
         auto tx = t.ses->start_transaction(0);
         tx.remove_range("", max_key);
         tx.commit();
      }
      require_empty_no_leaks(t, "after range_remove all");
   }
}

TEST_CASE("leak: interleaved insert and range_remove - no orphans",
          "[public-api][range_remove][leak]")
{
   test_db t;
   const int N = 1000 / SCALE;

   insert_keys(t, N);
   require_no_orphans(t, "after initial insert");

   // Range-remove first half
   char mid_buf[64];
   snprintf(mid_buf, sizeof(mid_buf), "key%06d", N / 2);
   {
      auto tx = t.ses->start_transaction(0);
      tx.remove_range("", mid_buf);
      tx.commit();
   }
   require_no_orphans(t, "after range_remove first half");

   // Insert new keys in the removed range
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < N / 2; ++i)
      {
         char key[64], val[64];
         snprintf(key, sizeof(key), "new_%06d", i);
         snprintf(val, sizeof(val), "val_%06d", i);
         tx.upsert(to_key_view(std::string(key)), to_value_view(std::string(val)));
      }
      tx.commit();
   }
   require_no_orphans(t, "after re-insert into removed range");

   // Remove everything
   {
      auto tx = t.ses->start_transaction(0);
      tx.remove_range("", max_key);
      tx.commit();
   }
   require_empty_no_leaks(t, "after final range_remove all");
}

// ============================================================
// micro mode transaction tests
// ============================================================

TEST_CASE("micro transaction basic upsert and get", "[public-api][transaction][micro]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
   tx.upsert(to_key("hello"), to_value("world"));
   tx.upsert(to_key("foo"), to_value("bar"));

   // Visible within the transaction
   auto v1 = tx.get<std::string>(to_key("hello"));
   REQUIRE(v1.has_value());
   REQUIRE(*v1 == "world");

   auto v2 = tx.get<std::string>(to_key("foo"));
   REQUIRE(v2.has_value());
   REQUIRE(*v2 == "bar");

   // Not-found key
   REQUIRE_FALSE(tx.get<std::string>(to_key("missing")).has_value());

   tx.commit();

   // Persisted
   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("hello"), &buf) >= 0);
   REQUIRE(buf == "world");
   REQUIRE(c.get(to_key("foo"), &buf) >= 0);
   REQUIRE(buf == "bar");
}

TEST_CASE("micro transaction get with Buffer overload", "[public-api][transaction][micro]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
   tx.upsert(to_key("key"), to_value("value123"));

   std::string buf;
   int32_t result = tx.get(to_key("key"), &buf);
   REQUIRE(result >= 0);
   REQUIRE(buf == "value123");

   result = tx.get(to_key("nope"), &buf);
   REQUIRE(result == cursor::value_not_found);

   tx.commit();
}

TEST_CASE("micro transaction overwrite value", "[public-api][transaction][micro]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
   tx.upsert(to_key("key"), to_value("first"));

   auto v = tx.get<std::string>(to_key("key"));
   REQUIRE(v.has_value());
   REQUIRE(*v == "first");

   tx.upsert(to_key("key"), to_value("second"));
   v = tx.get<std::string>(to_key("key"));
   REQUIRE(*v == "second");

   tx.commit();

   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("key"), &buf) >= 0);
   REQUIRE(buf == "second");
}

TEST_CASE("micro transaction remove", "[public-api][transaction][micro]")
{
   test_db t;

   // Pre-populate persistent tree
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_key("a"), to_value("1"));
      tx.upsert(to_key("b"), to_value("2"));
      tx.upsert(to_key("c"), to_value("3"));
      tx.commit();
   }

   // Micro mode: remove "b"
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);

      REQUIRE(tx.get<std::string>(to_key("b")).has_value());
      int removed_size = tx.remove(to_key("b"));
      REQUIRE(removed_size >= 0);

      // Tombstone shadows the persistent key
      REQUIRE_FALSE(tx.get<std::string>(to_key("b")).has_value());

      // Other keys still visible
      REQUIRE(tx.get<std::string>(to_key("a")).has_value());
      REQUIRE(tx.get<std::string>(to_key("c")).has_value());

      tx.commit();
   }

   // Verify persistent tree
   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("a"), &buf) >= 0);
   REQUIRE(c.get(to_key("b"), &buf) < 0);  // removed
   REQUIRE(c.get(to_key("c"), &buf) >= 0);
}

TEST_CASE("micro transaction remove nonexistent key returns -1", "[public-api][transaction][micro]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
   int result = tx.remove(to_key("nonexistent"));
   REQUIRE(result == -1);
   tx.commit();
}

TEST_CASE("micro transaction remove then re-insert", "[public-api][transaction][micro]")
{
   test_db t;

   // Pre-populate
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_key("key"), to_value("original"));
      tx.commit();
   }

   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
      tx.remove(to_key("key"));
      REQUIRE_FALSE(tx.get<std::string>(to_key("key")).has_value());

      tx.upsert(to_key("key"), to_value("resurrected"));
      auto v = tx.get<std::string>(to_key("key"));
      REQUIRE(v.has_value());
      REQUIRE(*v == "resurrected");

      tx.commit();
   }

   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("key"), &buf) >= 0);
   REQUIRE(buf == "resurrected");
}

TEST_CASE("micro transaction abort discards all changes", "[public-api][transaction][micro]")
{
   test_db t;

   // Pre-populate
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_key("existing"), to_value("keep"));
      tx.commit();
   }

   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
      tx.upsert(to_key("new_key"), to_value("gone"));
      tx.remove(to_key("existing"));
      tx.abort();
   }

   // Persistent tree unchanged
   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("existing"), &buf) >= 0);
   REQUIRE(buf == "keep");
   REQUIRE(c.get(to_key("new_key"), &buf) < 0);
}

TEST_CASE("micro transaction sub_transaction commit", "[public-api][transaction][micro]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
   tx.upsert(to_key("outer"), to_value("yes"));

   {
      auto sub = tx.sub_transaction();
      sub.upsert(to_key("inner"), to_value("yes"));
      sub.commit();
   }

   // Both visible
   REQUIRE(tx.get<std::string>(to_key("outer")).has_value());
   REQUIRE(tx.get<std::string>(to_key("inner")).has_value());

   tx.commit();

   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("outer"), &buf) >= 0);
   REQUIRE(c.get(to_key("inner"), &buf) >= 0);
}

TEST_CASE("micro transaction sub_transaction abort", "[public-api][transaction][micro]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
   tx.upsert(to_key("outer"), to_value("yes"));

   {
      auto sub = tx.sub_transaction();
      sub.upsert(to_key("inner_lost"), to_value("gone"));
      sub.abort();
   }

   REQUIRE(tx.get<std::string>(to_key("outer")).has_value());
   REQUIRE_FALSE(tx.get<std::string>(to_key("inner_lost")).has_value());

   tx.commit();

   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("outer"), &buf) >= 0);
   REQUIRE(c.get(to_key("inner_lost"), &buf) < 0);
}

TEST_CASE("micro transaction sub_transaction destructor aborts", "[public-api][transaction][micro]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
   tx.upsert(to_key("kept"), to_value("yes"));

   {
      auto sub = tx.sub_transaction();
      sub.upsert(to_key("temp"), to_value("gone"));
      // ~sub fires, auto-aborts
   }

   REQUIRE(tx.get<std::string>(to_key("kept")).has_value());
   REQUIRE_FALSE(tx.get<std::string>(to_key("temp")).has_value());

   tx.commit();
}

TEST_CASE("micro transaction nested sub_transactions two levels", "[public-api][transaction][micro]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
   tx.upsert(to_key("L0"), to_value("zero"));

   {
      auto sub1 = tx.sub_transaction();
      sub1.upsert(to_key("L1"), to_value("one"));

      {
         auto sub2 = sub1.sub_transaction();
         sub2.upsert(to_key("L2"), to_value("two"));
         sub2.commit();
      }

      // L2 visible after sub2 commit
      REQUIRE(sub1.get<std::string>(to_key("L2")).has_value());

      sub1.commit();
   }

   REQUIRE(tx.get<std::string>(to_key("L0")).has_value());
   REQUIRE(tx.get<std::string>(to_key("L1")).has_value());
   REQUIRE(tx.get<std::string>(to_key("L2")).has_value());

   tx.commit();

   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("L0"), &buf) >= 0);
   REQUIRE(c.get(to_key("L1"), &buf) >= 0);
   REQUIRE(c.get(to_key("L2"), &buf) >= 0);
}

TEST_CASE("micro transaction nested abort inner preserves outer", "[public-api][transaction][micro]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
   tx.upsert(to_key("L0"), to_value("zero"));

   {
      auto sub1 = tx.sub_transaction();
      sub1.upsert(to_key("L1"), to_value("one"));

      {
         auto sub2 = sub1.sub_transaction();
         sub2.upsert(to_key("L2_lost"), to_value("gone"));
         sub2.abort();
      }

      // L2_lost not visible
      REQUIRE_FALSE(sub1.get<std::string>(to_key("L2_lost")).has_value());
      // L1 still visible
      REQUIRE(sub1.get<std::string>(to_key("L1")).has_value());

      sub1.commit();
   }

   REQUIRE(tx.get<std::string>(to_key("L0")).has_value());
   REQUIRE(tx.get<std::string>(to_key("L1")).has_value());
   REQUIRE_FALSE(tx.get<std::string>(to_key("L2_lost")).has_value());

   tx.commit();
}

TEST_CASE("micro transaction sub_transaction abort after remove restores", "[public-api][transaction][micro]")
{
   test_db t;

   // Pre-populate
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_key("k1"), to_value("v1"));
      tx.upsert(to_key("k2"), to_value("v2"));
      tx.commit();
   }

   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);

      {
         auto sub = tx.sub_transaction();
         sub.remove(to_key("k1"));
         REQUIRE_FALSE(sub.get<std::string>(to_key("k1")).has_value());
         sub.abort();
      }

      // k1 restored after sub abort
      auto v = tx.get<std::string>(to_key("k1"));
      REQUIRE(v.has_value());
      REQUIRE(*v == "v1");

      tx.commit();
   }
}

TEST_CASE("micro transaction many keys merge to persistent tree", "[public-api][transaction][micro]")
{
   test_db t;

   constexpr int N = 200;

   auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
   for (int i = 0; i < N; ++i)
   {
      auto k = "key_" + std::to_string(i);
      auto v = "val_" + std::to_string(i);
      tx.upsert(to_key_view(k), to_value_view(v));
   }
   tx.commit();

   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;
   for (int i = 0; i < N; ++i)
   {
      auto k = "key_" + std::to_string(i);
      auto v = "val_" + std::to_string(i);
      REQUIRE(c.get(to_key_view(k), &buf) >= 0);
      REQUIRE(buf == v);
   }
}

TEST_CASE("micro transaction with pre-existing persistent data", "[public-api][transaction][micro]")
{
   test_db t;

   // Pre-populate
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < 50; ++i)
      {
         auto k = "p_" + std::to_string(i);
         auto v = "persistent_" + std::to_string(i);
         tx.upsert(to_key_view(k), to_value_view(v));
      }
      tx.commit();
   }

   // Micro mode: add new, update existing, remove some
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);

      // Add new keys
      for (int i = 50; i < 75; ++i)
      {
         auto k = "p_" + std::to_string(i);
         auto v = "new_" + std::to_string(i);
         tx.upsert(to_key_view(k), to_value_view(v));
      }

      // Update existing keys
      for (int i = 0; i < 10; ++i)
      {
         auto k = "p_" + std::to_string(i);
         auto v = "updated_" + std::to_string(i);
         tx.upsert(to_key_view(k), to_value_view(v));
      }

      // Remove some keys
      for (int i = 40; i < 50; ++i)
      {
         auto k = "p_" + std::to_string(i);
         tx.remove(to_key_view(k));
      }

      // Verify within transaction
      for (int i = 0; i < 10; ++i)
      {
         auto k = "p_" + std::to_string(i);
         auto v = tx.get<std::string>(to_key_view(k));
         REQUIRE(v.has_value());
         REQUIRE(*v == "updated_" + std::to_string(i));
      }

      for (int i = 40; i < 50; ++i)
      {
         auto k = "p_" + std::to_string(i);
         REQUIRE_FALSE(tx.get<std::string>(to_key_view(k)).has_value());
      }

      tx.commit();
   }

   // Verify persistent tree
   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;

   // Updated keys
   for (int i = 0; i < 10; ++i)
   {
      auto k = "p_" + std::to_string(i);
      REQUIRE(c.get(to_key_view(k), &buf) >= 0);
      REQUIRE(buf == "updated_" + std::to_string(i));
   }

   // Untouched keys
   for (int i = 10; i < 40; ++i)
   {
      auto k = "p_" + std::to_string(i);
      REQUIRE(c.get(to_key_view(k), &buf) >= 0);
      REQUIRE(buf == "persistent_" + std::to_string(i));
   }

   // Removed keys
   for (int i = 40; i < 50; ++i)
   {
      auto k = "p_" + std::to_string(i);
      REQUIRE(c.get(to_key_view(k), &buf) < 0);
   }

   // New keys
   for (int i = 50; i < 75; ++i)
   {
      auto k = "p_" + std::to_string(i);
      REQUIRE(c.get(to_key_view(k), &buf) >= 0);
      REQUIRE(buf == "new_" + std::to_string(i));
   }
}

TEST_CASE("batch sub_transaction still works via frame_ref", "[public-api][transaction]")
{
   test_db t;

   // Batch mode (default) sub_transaction now returns frame_ref — verify it works
   auto tx = t.ses->start_transaction(0);
   tx.upsert(to_key("batch_outer"), to_value("yes"));

   {
      auto sub = tx.sub_transaction();
      sub.upsert(to_key("batch_inner"), to_value("yes"));

      {
         auto sub2 = sub.sub_transaction();
         sub2.upsert(to_key("batch_L2"), to_value("yes"));
         sub2.commit();
      }

      sub.commit();
   }

   REQUIRE(tx.get<std::string>(to_key("batch_outer")).has_value());
   REQUIRE(tx.get<std::string>(to_key("batch_inner")).has_value());
   REQUIRE(tx.get<std::string>(to_key("batch_L2")).has_value());

   tx.commit();

   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("batch_outer"), &buf) >= 0);
   REQUIRE(c.get(to_key("batch_inner"), &buf) >= 0);
   REQUIRE(c.get(to_key("batch_L2"), &buf) >= 0);
}

TEST_CASE("batch sub_transaction abort via frame_ref", "[public-api][transaction]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0);
   tx.upsert(to_key("kept"), to_value("yes"));

   {
      auto sub = tx.sub_transaction();
      sub.upsert(to_key("discarded"), to_value("gone"));
      sub.abort();
   }

   REQUIRE(tx.get<std::string>(to_key("kept")).has_value());
   REQUIRE_FALSE(tx.get<std::string>(to_key("discarded")).has_value());

   tx.commit();
}

TEST_CASE("micro remove_range small uses tombstones", "[public-api][transaction][micro]")
{
   test_db t;

   // Pre-populate with keys a..z
   {
      auto tx = t.ses->start_transaction(0);
      for (char c = 'a'; c <= 'z'; ++c)
      {
         std::string k(1, c);
         tx.upsert(to_key_view(k), to_value_view(k));
      }
      tx.commit();
   }

   // Micro mode: remove range [e, u) — 16 keys, well under tombstone threshold
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
      tx.upsert(to_key("new1"), to_value("added"));

      uint64_t removed = tx.remove_range(to_key("e"), to_key("u"));
      REQUIRE(removed == 17);  // e,f,g,...,t (16 persistent) + "new1" (1 buffer insert)

      // Removed keys not visible
      REQUIRE_FALSE(tx.get<std::string>(to_key("e")).has_value());
      REQUIRE_FALSE(tx.get<std::string>(to_key("t")).has_value());

      // Keys outside range still visible
      REQUIRE(tx.get<std::string>(to_key("a")).has_value());
      REQUIRE(tx.get<std::string>(to_key("d")).has_value());
      REQUIRE(tx.get<std::string>(to_key("u")).has_value());
      REQUIRE(tx.get<std::string>(to_key("z")).has_value());

      // "new1" was in range [e,u) — also removed
      REQUIRE_FALSE(tx.get<std::string>(to_key("new1")).has_value());

      tx.commit();
   }

   // Verify persistent tree
   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("a"), &buf) >= 0);
   REQUIRE(c.get(to_key("d"), &buf) >= 0);
   REQUIRE(c.get(to_key("e"), &buf) < 0);
   REQUIRE(c.get(to_key("t"), &buf) < 0);
   REQUIRE(c.get(to_key("u"), &buf) >= 0);
   REQUIRE(c.get(to_key("z"), &buf) >= 0);
   REQUIRE(c.get(to_key("new1"), &buf) < 0);  // was in range [e,u)
}

TEST_CASE("micro remove_range also removes buffer inserts in range",
          "[public-api][transaction][micro]")
{
   test_db t;

   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);

      // Insert keys into buffer (no persistent data)
      for (char c = 'a'; c <= 'z'; ++c)
      {
         std::string k(1, c);
         tx.upsert(to_key_view(k), to_value_view(k));
      }

      // Remove range of buffer-only inserts
      uint64_t removed = tx.remove_range(to_key("f"), to_key("n"));
      REQUIRE(removed == 8);  // f,g,h,i,j,k,l,m

      REQUIRE_FALSE(tx.get<std::string>(to_key("f")).has_value());
      REQUIRE(tx.get<std::string>(to_key("e")).has_value());
      REQUIRE(tx.get<std::string>(to_key("n")).has_value());

      tx.commit();
   }

   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("e"), &buf) >= 0);
   REQUIRE(c.get(to_key("f"), &buf) < 0);
   REQUIRE(c.get(to_key("m"), &buf) < 0);
   REQUIRE(c.get(to_key("n"), &buf) >= 0);
}

TEST_CASE("micro remove_range large triggers merge-then-delegate",
          "[public-api][transaction][micro]")
{
   test_db t;

   // Build a dictionary-like dataset: 2000 words
   std::vector<std::string> words;
   words.reserve(2000);
   for (int i = 0; i < 2000; ++i)
   {
      // Generate words like "word_0000" .. "word_1999" (sorted)
      char buf[16];
      snprintf(buf, sizeof(buf), "word_%04d", i);
      words.emplace_back(buf);
   }

   // Pre-populate persistent tree
   {
      auto tx = t.ses->start_transaction(0);
      for (auto& w : words)
         tx.upsert(to_key_view(w), to_value_view(w));
      tx.commit();
   }

   // Micro mode: some writes, then large range deletion
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);

      // Buffer some writes
      tx.upsert(to_key("aaa_before_range"), to_value("kept"));
      tx.upsert(to_key("zzz_after_range"), to_value("kept"));
      tx.upsert(to_key("word_0500"), to_value("updated_500"));  // update within range

      // Remove a large range — over tombstone_threshold (256)
      // word_0300 .. word_0899 = 600 words
      uint64_t removed = tx.remove_range(to_key("word_0300"), to_key("word_0900"));

      // Should have triggered merge-then-delegate
      REQUIRE(removed >= 599);  // 600 persistent keys minus 1 that was updated to buffer

      // Keys outside range still visible
      REQUIRE(tx.get<std::string>(to_key("word_0000")).has_value());
      REQUIRE(tx.get<std::string>(to_key("word_0299")).has_value());
      REQUIRE(tx.get<std::string>(to_key("word_0900")).has_value());
      REQUIRE(tx.get<std::string>(to_key("word_1999")).has_value());

      // Buffered keys outside range survived the merge
      REQUIRE(tx.get<std::string>(to_key("aaa_before_range")).has_value());
      REQUIRE(tx.get<std::string>(to_key("zzz_after_range")).has_value());

      // Keys inside range removed
      REQUIRE_FALSE(tx.get<std::string>(to_key("word_0300")).has_value());
      REQUIRE_FALSE(tx.get<std::string>(to_key("word_0500")).has_value());
      REQUIRE_FALSE(tx.get<std::string>(to_key("word_0899")).has_value());

      // Can still write after merge
      tx.upsert(to_key("word_0500"), to_value("re-added"));
      REQUIRE(tx.get<std::string>(to_key("word_0500")).has_value());

      tx.commit();
   }

   // Verify persistent tree
   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("word_0000"), &buf) >= 0);
   REQUIRE(c.get(to_key("word_0299"), &buf) >= 0);
   REQUIRE(c.get(to_key("word_0300"), &buf) < 0);
   REQUIRE(c.get(to_key("word_0500"), &buf) >= 0);
   REQUIRE(buf == "re-added");
   REQUIRE(c.get(to_key("word_0899"), &buf) < 0);
   REQUIRE(c.get(to_key("word_0900"), &buf) >= 0);
   REQUIRE(c.get(to_key("aaa_before_range"), &buf) >= 0);
   REQUIRE(c.get(to_key("zzz_after_range"), &buf) >= 0);
}

TEST_CASE("micro sub_transaction with large range remove then abort",
          "[public-api][transaction][micro]")
{
   test_db t;

   // Pre-populate with 500 keys
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < 500; ++i)
      {
         char buf[16];
         snprintf(buf, sizeof(buf), "k_%04d", i);
         tx.upsert(to_key_view(std::string(buf)), to_value_view(std::string(buf)));
      }
      tx.commit();
   }

   // Micro mode: writes, then sub-tx with large range remove, then abort sub-tx
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);

      // Parent writes
      tx.upsert(to_key("parent_key"), to_value("parent_val"));

      {
         auto sub = tx.sub_transaction();

         // Sub-tx writes
         sub.upsert(to_key("sub_key"), to_value("sub_val"));

         // Large range remove triggers merge-then-delegate within sub-tx
         uint64_t removed = sub.remove_range(to_key("k_0100"), to_key("k_0400"));
         REQUIRE(removed >= 299);

         // Sub-tx sees the removal
         REQUIRE_FALSE(sub.get<std::string>(to_key("k_0200")).has_value());

         // Abort the sub-transaction
         sub.abort();
      }

      // After abort: parent should see original persistent data restored
      // k_0200 should be visible again
      REQUIRE(tx.get<std::string>(to_key("k_0200")).has_value());

      // Parent's own write is still there
      REQUIRE(tx.get<std::string>(to_key("parent_key")).has_value());

      // Sub-tx's write should be gone
      REQUIRE_FALSE(tx.get<std::string>(to_key("sub_key")).has_value());

      tx.commit();
   }

   // Verify: all original keys intact, plus parent_key
   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("k_0200"), &buf) >= 0);
   REQUIRE(c.get(to_key("parent_key"), &buf) >= 0);
   REQUIRE(c.get(to_key("sub_key"), &buf) < 0);
}

TEST_CASE("micro nested sub_transaction: inner merge, outer abort",
          "[public-api][transaction][micro]")
{
   test_db t;

   // Pre-populate with 500 keys
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < 500; ++i)
      {
         char buf[16];
         snprintf(buf, sizeof(buf), "n_%04d", i);
         tx.upsert(to_key_view(std::string(buf)), to_value_view(std::string(buf)));
      }
      tx.commit();
   }

   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
      tx.upsert(to_key("L0_key"), to_value("L0_val"));

      {
         auto sub1 = tx.sub_transaction();
         sub1.upsert(to_key("L1_key"), to_value("L1_val"));

         {
            auto sub2 = sub1.sub_transaction();
            sub2.upsert(to_key("L2_key"), to_value("L2_val"));

            // Large range remove inside L2 triggers merge
            sub2.remove_range(to_key("n_0100"), to_key("n_0400"));

            sub2.commit();  // L2 commits into L1
         }

         // L1 sees L2's changes
         REQUIRE_FALSE(sub1.get<std::string>(to_key("n_0200")).has_value());
         REQUIRE(sub1.get<std::string>(to_key("L2_key")).has_value());

         sub1.abort();  // Abort L1 — should restore pre-L1 state
      }

      // After L1 abort: persistent data restored, L0_key still buffered
      REQUIRE(tx.get<std::string>(to_key("n_0200")).has_value());
      REQUIRE(tx.get<std::string>(to_key("L0_key")).has_value());
      REQUIRE_FALSE(tx.get<std::string>(to_key("L1_key")).has_value());
      REQUIRE_FALSE(tx.get<std::string>(to_key("L2_key")).has_value());

      tx.commit();
   }

   auto root = t.ses->get_root(0);
   cursor c(root);
   std::string buf;
   REQUIRE(c.get(to_key("n_0200"), &buf) >= 0);
   REQUIRE(c.get(to_key("L0_key"), &buf) >= 0);
   REQUIRE(c.get(to_key("L1_key"), &buf) < 0);
}
