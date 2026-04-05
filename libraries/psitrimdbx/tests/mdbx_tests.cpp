#include <catch2/catch_test_macros.hpp>

#include <mdbx.h>
#include <mdbx.h++>

#include <cstring>
#include <filesystem>
#include <string>

namespace fs = std::filesystem;

static fs::path make_temp_dir(const char* prefix)
{
   auto p = fs::temp_directory_path() / (std::string(prefix) + "_mdbx_test");
   fs::remove_all(p);  // Clean any leftover from prior run
   fs::create_directories(p);
   return p;
}

// ════════════════════════════════════════════════════════════════════
// C API tests
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: env create, open, close", "[mdbx][c-api]")
{
   auto dir = make_temp_dir("c_env");

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(env != nullptr);

   REQUIRE(mdbx_env_set_maxdbs(env, 8) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   REQUIRE(mdbx_env_close(env) == MDBX_SUCCESS);
   // Cleanup handled by make_temp_dir() at start of next run
}

TEST_CASE("C API: basic put/get/del", "[mdbx][c-api]")
{
   auto dir = make_temp_dir("c_crud");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   // Start RW transaction
   MDBX_txn* txn = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

   // Open default DBI
   MDBX_dbi dbi = 0;
   REQUIRE(mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi) == MDBX_SUCCESS);
   REQUIRE(dbi == 1); // unnamed default = DBI 1

   // Put
   const char* k = "hello";
   const char* v = "world";
   MDBX_val key  = {const_cast<char*>(k), 5};
   MDBX_val data = {const_cast<char*>(v), 5};
   REQUIRE(mdbx_put(txn, dbi, &key, &data, MDBX_UPSERT) == MDBX_SUCCESS);

   // Get within same txn
   MDBX_val got = {};
   REQUIRE(mdbx_get(txn, dbi, &key, &got) == MDBX_SUCCESS);
   REQUIRE(got.iov_len == 5);
   REQUIRE(std::memcmp(got.iov_base, "world", 5) == 0);

   // Commit
   REQUIRE(mdbx_txn_commit(txn) == MDBX_SUCCESS);

   // Read in a new RO txn
   MDBX_txn* ro = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro) == MDBX_SUCCESS);
   got = {};
   REQUIRE(mdbx_get(ro, dbi, &key, &got) == MDBX_SUCCESS);
   REQUIRE(std::string(static_cast<char*>(got.iov_base), got.iov_len) == "world");
   mdbx_txn_abort(ro);

   // Delete
   MDBX_txn* txn2 = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn2);
   REQUIRE(mdbx_del(txn2, dbi, &key, nullptr) == MDBX_SUCCESS);
   got = {};
   REQUIRE(mdbx_get(txn2, dbi, &key, &got) == MDBX_NOTFOUND);
   mdbx_txn_commit(txn2);

   mdbx_env_close(env);
   // Cleanup handled by make_temp_dir() at start of next run
}

TEST_CASE("C API: named databases", "[mdbx][c-api]")
{
   auto dir = make_temp_dir("c_named");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 16);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);

   // Open named DB
   MDBX_dbi users = 0, orders = 0;
   REQUIRE(mdbx_dbi_open(txn, "users", (MDBX_db_flags_t)(MDBX_CREATE), &users) == MDBX_SUCCESS);
   REQUIRE(mdbx_dbi_open(txn, "orders", (MDBX_db_flags_t)(MDBX_CREATE), &orders) == MDBX_SUCCESS);
   REQUIRE(users != orders);

   // Put in users
   MDBX_val k = {const_cast<char*>("alice"), 5};
   MDBX_val v = {const_cast<char*>("admin"), 5};
   mdbx_put(txn, users, &k, &v, MDBX_UPSERT);

   // Put in orders
   MDBX_val k2 = {const_cast<char*>("order1"), 6};
   MDBX_val v2 = {const_cast<char*>("pending"), 7};
   mdbx_put(txn, orders, &k2, &v2, MDBX_UPSERT);

   // Cross-check: key not in wrong table
   MDBX_val got = {};
   REQUIRE(mdbx_get(txn, users, &k2, &got) == MDBX_NOTFOUND);
   REQUIRE(mdbx_get(txn, orders, &k, &got) == MDBX_NOTFOUND);

   // Correct table finds key
   REQUIRE(mdbx_get(txn, users, &k, &got) == MDBX_SUCCESS);
   REQUIRE(mdbx_get(txn, orders, &k2, &got) == MDBX_SUCCESS);

   mdbx_txn_commit(txn);

   // Re-open same named DB
   MDBX_txn* txn2 = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn2);
   MDBX_dbi users2 = 0;
   REQUIRE(mdbx_dbi_open(txn2, "users", MDBX_DB_DEFAULTS, &users2) == MDBX_SUCCESS);
   REQUIRE(users2 == users); // Same DBI handle
   mdbx_txn_abort(txn2);

   mdbx_env_close(env);
   // Cleanup handled by make_temp_dir() at start of next run
}

TEST_CASE("C API: cursor iteration", "[mdbx][c-api]")
{
   auto dir = make_temp_dir("c_cursor");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);

   MDBX_dbi dbi = 0;
   mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);

   // Insert sorted data
   const char* keys[] = {"apple", "banana", "cherry", "date", "elderberry"};
   for (auto* k : keys)
   {
      MDBX_val key  = {const_cast<char*>(k), std::strlen(k)};
      MDBX_val data = {const_cast<char*>("v"), 1};
      mdbx_put(txn, dbi, &key, &data, MDBX_UPSERT);
   }
   mdbx_txn_commit(txn);

   // Iterate forward
   MDBX_txn* ro = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro);

   MDBX_cursor* cur = nullptr;
   REQUIRE(mdbx_cursor_open(ro, dbi, &cur) == MDBX_SUCCESS);

   MDBX_val k, d;
   int      count = 0;
   std::string prev_key;

   int rc = mdbx_cursor_get(cur, &k, &d, MDBX_FIRST);
   while (rc == MDBX_SUCCESS)
   {
      std::string key(static_cast<char*>(k.iov_base), k.iov_len);
      REQUIRE(key > prev_key); // Sorted order
      prev_key = key;
      count++;
      rc = mdbx_cursor_get(cur, &k, &d, MDBX_NEXT);
   }
   REQUIRE(count == 5);

   // Seek with SET_RANGE
   MDBX_val seek_key = {const_cast<char*>("c"), 1};
   REQUIRE(mdbx_cursor_get(cur, &seek_key, &d, MDBX_SET_RANGE) == MDBX_SUCCESS);
   std::string found(static_cast<char*>(seek_key.iov_base), seek_key.iov_len);
   REQUIRE(found == "cherry");

   mdbx_cursor_close(cur);
   mdbx_txn_abort(ro);
   mdbx_env_close(env);
   // Cleanup handled by make_temp_dir() at start of next run
}

TEST_CASE("C API: NOOVERWRITE returns KEYEXIST", "[mdbx][c-api]")
{
   auto dir = make_temp_dir("c_nooverwrite");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);

   MDBX_dbi dbi = 0;
   mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);

   MDBX_val k = {const_cast<char*>("key"), 3};
   MDBX_val v = {const_cast<char*>("val1"), 4};
   REQUIRE(mdbx_put(txn, dbi, &k, &v, MDBX_NOOVERWRITE) == MDBX_SUCCESS);

   MDBX_val v2 = {const_cast<char*>("val2"), 4};
   REQUIRE(mdbx_put(txn, dbi, &k, &v2, MDBX_NOOVERWRITE) == MDBX_KEYEXIST);

   mdbx_txn_abort(txn);
   mdbx_env_close(env);
   // Cleanup handled by make_temp_dir() at start of next run
}

TEST_CASE("C API: abort rolls back", "[mdbx][c-api]")
{
   auto dir = make_temp_dir("c_abort");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   // Write and commit
   MDBX_txn* txn = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
   MDBX_dbi dbi = 0;
   mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);
   MDBX_val k = {const_cast<char*>("key"), 3};
   MDBX_val v = {const_cast<char*>("committed"), 9};
   mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT);
   mdbx_txn_commit(txn);

   // Write and abort
   MDBX_txn* txn2 = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn2);
   MDBX_val v2 = {const_cast<char*>("aborted"), 7};
   mdbx_put(txn2, dbi, &k, &v2, MDBX_UPSERT);
   mdbx_txn_abort(txn2);

   // Verify original value persists
   MDBX_txn* ro = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro);
   MDBX_val got = {};
   REQUIRE(mdbx_get(ro, dbi, &k, &got) == MDBX_SUCCESS);
   REQUIRE(std::string(static_cast<char*>(got.iov_base), got.iov_len) == "committed");
   mdbx_txn_abort(ro);

   mdbx_env_close(env);
   // Cleanup handled by make_temp_dir() at start of next run
}

TEST_CASE("C API: error strings", "[mdbx][c-api]")
{
   REQUIRE(std::string(mdbx_strerror(MDBX_SUCCESS)).find("Successful") != std::string::npos);
   REQUIRE(std::string(mdbx_strerror(MDBX_NOTFOUND)).find("MDBX_NOTFOUND") != std::string::npos);
   REQUIRE(std::string(mdbx_strerror(MDBX_KEYEXIST)).find("exists") != std::string::npos);
}

// ════════════════════════════════════════════════════════════════════
// C++ API tests
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C++ API: env_managed open/close", "[mdbx][cpp-api]")
{
   auto dir = make_temp_dir("cpp_env");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;

   {
      mdbx::env_managed db(dir.c_str(), cp, op);
      REQUIRE(static_cast<bool>(db));
   }
   // Destructor should close cleanly

   // Cleanup handled by make_temp_dir() at start of next run
}

TEST_CASE("C++ API: basic CRUD via txn", "[mdbx][cpp-api]")
{
   auto dir = make_temp_dir("cpp_crud");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;

   mdbx::env_managed db(dir.c_str(), cp, op);

   // Write
   {
      auto txn = db.start_write();
      auto map = txn.create_map("test", mdbx::key_mode::usual, mdbx::value_mode::single);

      txn.upsert(map, mdbx::slice("key1"), mdbx::slice("value1"));
      txn.upsert(map, mdbx::slice("key2"), mdbx::slice("value2"));
      txn.upsert(map, mdbx::slice("key3"), mdbx::slice("value3"));

      txn.commit();
   }

   // Read
   {
      auto txn = db.start_read();
      auto map = txn.open_map("test");

      auto v = txn.get(map, mdbx::slice("key2"));
      REQUIRE(v.string_view() == "value2");

      // Not found
      REQUIRE_THROWS_AS(txn.get(map, mdbx::slice("missing")), mdbx::not_found);

      // Get with default
      auto v2 = txn.get(map, mdbx::slice("missing"), mdbx::slice("default"));
      REQUIRE(v2.string_view() == "default");
   }

   // Erase
   {
      auto txn = db.start_write();
      auto map = txn.create_map("test");

      REQUIRE(txn.erase(map, mdbx::slice("key2")));
      REQUIRE(!txn.erase(map, mdbx::slice("nonexistent")));

      txn.commit();
   }

   // Verify erase persisted
   {
      auto txn = db.start_read();
      auto map = txn.open_map("test");

      REQUIRE_THROWS_AS(txn.get(map, mdbx::slice("key2")), mdbx::not_found);
      REQUIRE(txn.get(map, mdbx::slice("key1")).string_view() == "value1");
      REQUIRE(txn.get(map, mdbx::slice("key3")).string_view() == "value3");
   }

   // Cleanup handled by make_temp_dir() at start of next run
}

TEST_CASE("C++ API: cursor navigation", "[mdbx][cpp-api]")
{
   auto dir = make_temp_dir("cpp_cursor");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;
   mdbx::env_managed db(dir.c_str(), cp, op);

   // Populate
   {
      auto txn = db.start_write();
      auto map = txn.create_map("data");
      for (int i = 0; i < 100; i++)
      {
         auto k = "key_" + std::to_string(1000 + i);
         auto v = "val_" + std::to_string(i);
         txn.upsert(map, mdbx::slice(k), mdbx::slice(v));
      }
      txn.commit();
   }

   // Iterate
   {
      auto txn = db.start_read();
      auto map = txn.open_map("data");
      auto cur = txn.open_cursor(map);

      auto r = cur.to_first(false);
      REQUIRE(r.done);
      REQUIRE(r.key.string_view().substr(0, 4) == "key_");

      int count = 1;
      while (cur.to_next(false).done)
         count++;
      REQUIRE(count == 100);

      // lower_bound
      r = cur.lower_bound(mdbx::slice("key_1050"));
      REQUIRE(r.done);
      REQUIRE(r.key.string_view() == "key_1050");

      r = cur.lower_bound(mdbx::slice("key_1050x"));
      REQUIRE(r.done);
      REQUIRE(r.key.string_view() == "key_1051");
   }

   // Cleanup handled by make_temp_dir() at start of next run
}

TEST_CASE("C++ API: try_insert returns existing value", "[mdbx][cpp-api]")
{
   auto dir = make_temp_dir("cpp_tryinsert");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;
   mdbx::env_managed db(dir.c_str(), cp, op);

   auto txn = db.start_write();
   auto map = txn.create_map("m");

   auto r1 = txn.try_insert(map, mdbx::slice("k"), mdbx::slice("v1"));
   REQUIRE(r1.done);

   auto r2 = txn.try_insert(map, mdbx::slice("k"), mdbx::slice("v2"));
   REQUIRE(!r2.done);

   txn.commit();
   // Cleanup handled by make_temp_dir() at start of next run
}

TEST_CASE("C++ API: abort rolls back writes", "[mdbx][cpp-api]")
{
   auto dir = make_temp_dir("cpp_abort");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;
   mdbx::env_managed db(dir.c_str(), cp, op);

   // Commit initial
   {
      auto txn = db.start_write();
      auto map = txn.create_map("m");
      txn.upsert(map, mdbx::slice("key"), mdbx::slice("original"));
      txn.commit();
   }

   // Write + abort
   {
      auto txn = db.start_write();
      auto map = txn.create_map("m");
      txn.upsert(map, mdbx::slice("key"), mdbx::slice("modified"));
      txn.abort();
   }

   // Verify original persists
   {
      auto txn = db.start_read();
      auto map = txn.open_map("m");
      auto v   = txn.get(map, mdbx::slice("key"));
      REQUIRE(v.string_view() == "original");
   }

   // Cleanup handled by make_temp_dir() at start of next run
}

TEST_CASE("C++ API: slice comparison", "[mdbx][cpp-api]")
{
   mdbx::slice a("abc");
   mdbx::slice b("abd");
   mdbx::slice c("abc");
   mdbx::slice d("ab");

   REQUIRE(a == c);
   REQUIRE(a != b);
   REQUIRE(a < b);
   REQUIRE(b > a);
   REQUIRE(d < a);
   REQUIRE(a.starts_with(d));
}

TEST_CASE("C++ API: multiple maps isolation", "[mdbx][cpp-api]")
{
   auto dir = make_temp_dir("cpp_multimaps");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;
   mdbx::env_managed db(dir.c_str(), cp, op);

   {
      auto txn = db.start_write();
      auto m1  = txn.create_map("map1");
      auto m2  = txn.create_map("map2");

      txn.upsert(m1, mdbx::slice("shared_key"), mdbx::slice("from_map1"));
      txn.upsert(m2, mdbx::slice("shared_key"), mdbx::slice("from_map2"));

      // Each map has its own namespace
      REQUIRE(txn.get(m1, mdbx::slice("shared_key")).string_view() == "from_map1");
      REQUIRE(txn.get(m2, mdbx::slice("shared_key")).string_view() == "from_map2");

      txn.commit();
   }

   // Re-read in a new transaction
   {
      auto txn = db.start_read();
      auto m1  = txn.open_map("map1");
      auto m2  = txn.open_map("map2");

      REQUIRE(txn.get(m1, mdbx::slice("shared_key")).string_view() == "from_map1");
      REQUIRE(txn.get(m2, mdbx::slice("shared_key")).string_view() == "from_map2");
   }

   // Cleanup handled by make_temp_dir() at start of next run
}
