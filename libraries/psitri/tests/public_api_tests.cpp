#include <catch2/catch_all.hpp>
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/value_type.hpp>

using namespace psitri;

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
         db  = std::make_shared<database>(dir, runtime_config());
         ses = db->start_write_session();
      }

      ~test_db() { std::filesystem::remove_all(dir); }
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
      REQUIRE(cur->insert(to_key("hello"), to_value("world")));
      auto val = cur->get<std::string>(to_key("hello"));
      REQUIRE(val.has_value());
      REQUIRE(*val == "world");
   }

   SECTION("insert duplicate returns false")
   {
      REQUIRE(cur->insert(to_key("key"), to_value("v1")));
      REQUIRE_FALSE(cur->insert(to_key("key"), to_value("v2")));
      // original value preserved
      auto val = cur->get<std::string>(to_key("key"));
      REQUIRE(*val == "v1");
   }

   SECTION("update existing key")
   {
      cur->insert(to_key("key"), to_value("v1"));
      REQUIRE(cur->update(to_key("key"), to_value("v2")));
      auto val = cur->get<std::string>(to_key("key"));
      REQUIRE(*val == "v2");
   }

   SECTION("update nonexistent returns false")
   {
      REQUIRE_FALSE(cur->update(to_key("missing"), to_value("v1")));
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
      auto db  = std::make_shared<database>(dir, runtime_config());
      auto ses = db->start_write_session();
      auto tx  = ses->start_transaction(0);
      tx.upsert(to_key("survive"), to_value("reopen"));
      tx.commit(sal::sync_type::full);
   }

   // Reopen and verify
   {
      auto db  = std::make_shared<database>(dir, runtime_config());
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

   const int N = 10000;

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

   const int N = 1000;

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
