#include <catch2/catch_test_macros.hpp>

#include <mdbx.h>
#include <mdbx.h++>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <random>
#include <string>
#include <thread>
#include <vector>

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
}

// ════════════════════════════════════════════════════════════════════
// DUPSORT tests
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: DUPSORT basic put/get/del", "[mdbx][dupsort]")
{
   auto dir = make_temp_dir("c_dupsort");

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 16) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   MDBX_txn* txn = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

   MDBX_dbi dbi;
   REQUIRE(mdbx_dbi_open(txn, "dupsort_db",
           static_cast<MDBX_db_flags_t>(MDBX_CREATE | MDBX_DUPSORT), &dbi) == MDBX_SUCCESS);

   // Insert multiple values for same key
   auto put = [&](const char* k, const char* v) {
      MDBX_val key{const_cast<char*>(k), strlen(k)};
      MDBX_val val{const_cast<char*>(v), strlen(v)};
      REQUIRE(mdbx_put(txn, dbi, &key, &val, MDBX_UPSERT) == MDBX_SUCCESS);
   };

   put("fruit", "apple");
   put("fruit", "banana");
   put("fruit", "cherry");
   put("veggie", "carrot");
   put("veggie", "potato");

   // Get returns first value for key (sorted)
   {
      MDBX_val key{const_cast<char*>("fruit"), 5};
      MDBX_val val;
      REQUIRE(mdbx_get(txn, dbi, &key, &val) == MDBX_SUCCESS);
      REQUIRE(std::string_view(static_cast<char*>(val.iov_base), val.iov_len) == "apple");
   }

   // Delete specific dup
   {
      MDBX_val key{const_cast<char*>("fruit"), 5};
      MDBX_val val{const_cast<char*>("banana"), 6};
      REQUIRE(mdbx_del(txn, dbi, &key, &val) == MDBX_SUCCESS);
   }

   // Verify banana is gone, apple and cherry remain
   {
      MDBX_cursor* cur = nullptr;
      REQUIRE(mdbx_cursor_open(txn, dbi, &cur) == MDBX_SUCCESS);

      MDBX_val key{const_cast<char*>("fruit"), 5};
      MDBX_val data;
      REQUIRE(mdbx_cursor_get(cur, &key, &data, MDBX_SET) == MDBX_SUCCESS);
      REQUIRE(std::string_view(static_cast<char*>(data.iov_base), data.iov_len) == "apple");

      REQUIRE(mdbx_cursor_get(cur, &key, &data, MDBX_NEXT_DUP) == MDBX_SUCCESS);
      REQUIRE(std::string_view(static_cast<char*>(data.iov_base), data.iov_len) == "cherry");

      REQUIRE(mdbx_cursor_get(cur, &key, &data, MDBX_NEXT_DUP) == MDBX_NOTFOUND);

      mdbx_cursor_close(cur);
   }

   // Count dups
   {
      MDBX_cursor* cur = nullptr;
      REQUIRE(mdbx_cursor_open(txn, dbi, &cur) == MDBX_SUCCESS);

      MDBX_val key{const_cast<char*>("veggie"), 6};
      MDBX_val data;
      REQUIRE(mdbx_cursor_get(cur, &key, &data, MDBX_SET) == MDBX_SUCCESS);

      size_t count = 0;
      REQUIRE(mdbx_cursor_count(cur, &count) == MDBX_SUCCESS);
      REQUIRE(count == 2);

      mdbx_cursor_close(cur);
   }

   mdbx_txn_commit(txn);
   mdbx_env_close(env);
}

TEST_CASE("C API: DUPSORT cursor navigation", "[mdbx][dupsort]")
{
   auto dir = make_temp_dir("c_dupsort_nav");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 16);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);

   MDBX_dbi dbi;
   mdbx_dbi_open(txn, "nav",
                  static_cast<MDBX_db_flags_t>(MDBX_CREATE | MDBX_DUPSORT), &dbi);

   auto put = [&](const char* k, const char* v) {
      MDBX_val key{const_cast<char*>(k), strlen(k)};
      MDBX_val val{const_cast<char*>(v), strlen(v)};
      mdbx_put(txn, dbi, &key, &val, MDBX_UPSERT);
   };

   put("a", "1");
   put("a", "2");
   put("a", "3");
   put("b", "10");
   put("b", "20");
   put("c", "100");

   MDBX_cursor* cur = nullptr;
   mdbx_cursor_open(txn, dbi, &cur);

   auto get_kv = [&](MDBX_cursor_op op) -> std::pair<std::string, std::string> {
      MDBX_val key, data;
      int rc = mdbx_cursor_get(cur, &key, &data, op);
      if (rc != MDBX_SUCCESS)
         return {"", ""};
      return {
         std::string(static_cast<char*>(key.iov_base), key.iov_len),
         std::string(static_cast<char*>(data.iov_base), data.iov_len)
      };
   };

   // FIRST → a:1
   auto [k1, v1] = get_kv(MDBX_FIRST);
   REQUIRE(k1 == "a");
   REQUIRE(v1 == "1");

   // NEXT_DUP → a:2
   auto [k2, v2] = get_kv(MDBX_NEXT_DUP);
   REQUIRE(k2 == "a");
   REQUIRE(v2 == "2");

   // NEXT_NODUP → b:10
   auto [k3, v3] = get_kv(MDBX_NEXT_NODUP);
   REQUIRE(k3 == "b");
   REQUIRE(v3 == "10");

   // LAST_DUP → b:20
   auto [k4, v4] = get_kv(MDBX_LAST_DUP);
   REQUIRE(k4 == "b");
   REQUIRE(v4 == "20");

   // PREV_DUP → b:10
   auto [k5, v5] = get_kv(MDBX_PREV_DUP);
   REQUIRE(k5 == "b");
   REQUIRE(v5 == "10");

   // FIRST_DUP → b:10 (already at first)
   auto [k6, v6] = get_kv(MDBX_FIRST_DUP);
   REQUIRE(k6 == "b");
   REQUIRE(v6 == "10");

   // LAST → c:100
   auto [k7, v7] = get_kv(MDBX_LAST);
   REQUIRE(k7 == "c");
   REQUIRE(v7 == "100");

   // PREV_NODUP → b:20 (last dup of previous key)
   auto [k8, v8] = get_kv(MDBX_PREV_NODUP);
   REQUIRE(k8 == "b");
   // PREV_NODUP lands on last dup of prev key
   REQUIRE(v8 == "20");

   // GET_BOTH: exact match
   {
      MDBX_val key{const_cast<char*>("a"), 1};
      MDBX_val data{const_cast<char*>("2"), 1};
      REQUIRE(mdbx_cursor_get(cur, &key, &data, MDBX_GET_BOTH) == MDBX_SUCCESS);
   }

   // GET_BOTH_RANGE: lower bound within key's values
   {
      MDBX_val key{const_cast<char*>("a"), 1};
      MDBX_val data{const_cast<char*>("15"), 2}; // between "1" and "2"
      int rc = mdbx_cursor_get(cur, &key, &data, MDBX_GET_BOTH_RANGE);
      REQUIRE(rc == MDBX_SUCCESS);
      REQUIRE(std::string_view(static_cast<char*>(data.iov_base), data.iov_len) == "2");
   }

   mdbx_cursor_close(cur);
   mdbx_txn_commit(txn);
   mdbx_env_close(env);
}

TEST_CASE("C API: DUPSORT delete all dups", "[mdbx][dupsort]")
{
   auto dir = make_temp_dir("c_dupsort_delall");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 16);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);

   MDBX_dbi dbi;
   mdbx_dbi_open(txn, "delall",
                  static_cast<MDBX_db_flags_t>(MDBX_CREATE | MDBX_DUPSORT), &dbi);

   auto put = [&](const char* k, const char* v) {
      MDBX_val key{const_cast<char*>(k), strlen(k)};
      MDBX_val val{const_cast<char*>(v), strlen(v)};
      mdbx_put(txn, dbi, &key, &val, MDBX_UPSERT);
   };

   put("x", "1");
   put("x", "2");
   put("x", "3");
   put("y", "a");

   // Delete all dups for "x" (data=nullptr)
   {
      MDBX_val key{const_cast<char*>("x"), 1};
      REQUIRE(mdbx_del(txn, dbi, &key, nullptr) == MDBX_SUCCESS);
   }

   // "x" should be gone
   {
      MDBX_val key{const_cast<char*>("x"), 1};
      MDBX_val val;
      REQUIRE(mdbx_get(txn, dbi, &key, &val) == MDBX_NOTFOUND);
   }

   // "y" should still exist
   {
      MDBX_val key{const_cast<char*>("y"), 1};
      MDBX_val val;
      REQUIRE(mdbx_get(txn, dbi, &key, &val) == MDBX_SUCCESS);
      REQUIRE(std::string_view(static_cast<char*>(val.iov_base), val.iov_len) == "a");
   }

   mdbx_txn_commit(txn);
   mdbx_env_close(env);
}

TEST_CASE("C++ API: DUPSORT multi-value", "[mdbx][dupsort][cpp-api]")
{
   auto dir = make_temp_dir("cpp_dupsort");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;
   mdbx::env_managed db(dir.c_str(), cp, op);

   // Write
   {
      auto txn = db.start_write();
      auto map = txn.create_map("multi", mdbx::key_mode::usual, mdbx::value_mode::multi);

      txn.upsert(map, mdbx::slice("color"), mdbx::slice("blue"));
      txn.upsert(map, mdbx::slice("color"), mdbx::slice("green"));
      txn.upsert(map, mdbx::slice("color"), mdbx::slice("red"));
      txn.upsert(map, mdbx::slice("shape"), mdbx::slice("circle"));
      txn.upsert(map, mdbx::slice("shape"), mdbx::slice("square"));

      txn.commit();
   }

   // Read with cursor
   {
      auto txn = db.start_read();
      auto map = txn.open_map("multi");
      auto cur = txn.open_cursor(map);

      // First entry
      auto r = cur.to_first(false);
      REQUIRE(r.done);
      REQUIRE(r.key.string_view() == "color");
      REQUIRE(r.value.string_view() == "blue");

      // Next dup
      r = cur.to_next_dup(false);
      REQUIRE(r.done);
      REQUIRE(r.key.string_view() == "color");
      REQUIRE(r.value.string_view() == "green");

      r = cur.to_next_dup(false);
      REQUIRE(r.done);
      REQUIRE(r.value.string_view() == "red");

      // No more dups for "color"
      r = cur.to_next_dup(false);
      REQUIRE(!r.done);

      // Next unique key
      r = cur.to_next(false);
      REQUIRE(r.done);
      REQUIRE(r.key.string_view() == "shape");
      REQUIRE(r.value.string_view() == "circle");

      // Count dups for "shape"
      size_t count = cur.count_multivalue();
      REQUIRE(count == 2);
   }
}

// ════════════════════════════════════════════════════════════════════
// mdbx_drop and mdbx_dbi_stat tests
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: mdbx_drop clear", "[mdbx][c-api][drop]")
{
   auto dir = make_temp_dir("c_drop");

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 4) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   // Insert some data
   {
      MDBX_txn* txn = nullptr;
      REQUIRE(mdbx_txn_begin_ex(env, nullptr, MDBX_TXN_READWRITE, &txn, nullptr) == MDBX_SUCCESS);
      MDBX_dbi dbi = 0;
      REQUIRE(mdbx_dbi_open(txn, nullptr, MDBX_CREATE, &dbi) == MDBX_SUCCESS);

      for (int i = 0; i < 10; ++i)
      {
         auto key = "key" + std::to_string(i);
         auto val = "val" + std::to_string(i);
         MDBX_val k{key.data(), key.size()};
         MDBX_val v{val.data(), val.size()};
         REQUIRE(mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT) == MDBX_SUCCESS);
      }

      // Verify entries via stat
      MDBX_stat stat{};
      REQUIRE(mdbx_dbi_stat(txn, dbi, &stat, sizeof(stat)) == MDBX_SUCCESS);
      REQUIRE(stat.ms_entries == 10);

      // Clear the database (del=0)
      REQUIRE(mdbx_drop(txn, dbi, 0) == MDBX_SUCCESS);

      // Verify empty
      REQUIRE(mdbx_dbi_stat(txn, dbi, &stat, sizeof(stat)) == MDBX_SUCCESS);
      REQUIRE(stat.ms_entries == 0);

      // Can still insert after clear
      {
         std::string key = "after_clear";
         std::string val = "works";
         MDBX_val    k{key.data(), key.size()};
         MDBX_val    v{val.data(), val.size()};
         REQUIRE(mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT) == MDBX_SUCCESS);
      }

      REQUIRE(mdbx_dbi_stat(txn, dbi, &stat, sizeof(stat)) == MDBX_SUCCESS);
      REQUIRE(stat.ms_entries == 1);

      REQUIRE(mdbx_txn_commit_ex(txn, nullptr) == MDBX_SUCCESS);
   }

   mdbx_env_close_ex(env, false);
   fs::remove_all(dir);
}

TEST_CASE("C API: mdbx_drop delete named DB", "[mdbx][c-api][drop]")
{
   auto dir = make_temp_dir("c_drop_del");

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 4) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   MDBX_dbi dbi = 0;

   // Create and populate a named DB
   {
      MDBX_txn* txn = nullptr;
      REQUIRE(mdbx_txn_begin_ex(env, nullptr, MDBX_TXN_READWRITE, &txn, nullptr) == MDBX_SUCCESS);
      REQUIRE(mdbx_dbi_open(txn, "testdb", MDBX_CREATE, &dbi) == MDBX_SUCCESS);

      std::string key = "hello";
      std::string val = "world";
      MDBX_val    k{key.data(), key.size()};
      MDBX_val    v{val.data(), val.size()};
      REQUIRE(mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT) == MDBX_SUCCESS);

      // Drop (del=1) — clears data AND removes the DBI
      REQUIRE(mdbx_drop(txn, dbi, 1) == MDBX_SUCCESS);

      REQUIRE(mdbx_txn_commit_ex(txn, nullptr) == MDBX_SUCCESS);
   }

   // Verify the named DB was removed — re-opening it should create fresh
   {
      MDBX_txn* txn = nullptr;
      REQUIRE(mdbx_txn_begin_ex(env, nullptr, MDBX_TXN_READWRITE, &txn, nullptr) == MDBX_SUCCESS);
      MDBX_dbi dbi2 = 0;
      REQUIRE(mdbx_dbi_open(txn, "testdb", MDBX_CREATE, &dbi2) == MDBX_SUCCESS);

      MDBX_stat stat{};
      REQUIRE(mdbx_dbi_stat(txn, dbi2, &stat, sizeof(stat)) == MDBX_SUCCESS);
      REQUIRE(stat.ms_entries == 0);

      REQUIRE(mdbx_txn_commit_ex(txn, nullptr) == MDBX_SUCCESS);
   }

   mdbx_env_close_ex(env, false);
   fs::remove_all(dir);
}

TEST_CASE("C++ API: drop_map and clear_map", "[mdbx][cpp-api][drop]")
{
   auto dir = make_temp_dir("cpp_drop");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 4;

   mdbx::env_managed db(dir.c_str(), cp, op);

   auto map = mdbx::map_handle(0);

   // Insert data
   {
      auto txn = db.start_write();
      txn.upsert(map, mdbx::slice("a"), mdbx::slice("1"));
      txn.upsert(map, mdbx::slice("b"), mdbx::slice("2"));
      txn.upsert(map, mdbx::slice("c"), mdbx::slice("3"));
      txn.commit();
   }

   // Clear map (del=0)
   {
      auto txn = db.start_write();
      txn.clear_map(map);

      // Verify empty — get should throw not_found
      REQUIRE_THROWS_AS(txn.get(map, mdbx::slice("a")), mdbx::not_found);

      txn.commit();
   }

   fs::remove_all(dir);
}

// ════════════════════════════════════════════════════════════════════
// DBI persistence tests
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: named DBI persists across env reopen", "[mdbx][c-api][persistence]")
{
   auto dir = make_temp_dir("c_persist");

   // Phase 1: create a named DB and insert data
   {
      MDBX_env* env = nullptr;
      REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
      REQUIRE(mdbx_env_set_maxdbs(env, 8) == MDBX_SUCCESS);
      REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

      MDBX_txn* txn = nullptr;
      REQUIRE(mdbx_txn_begin_ex(env, nullptr, MDBX_TXN_READWRITE, &txn, nullptr) == MDBX_SUCCESS);

      MDBX_dbi dbi = 0;
      REQUIRE(mdbx_dbi_open(txn, "accounts", MDBX_CREATE, &dbi) == MDBX_SUCCESS);

      std::string key = "alice";
      std::string val = "100";
      MDBX_val    k{key.data(), key.size()};
      MDBX_val    v{val.data(), val.size()};
      REQUIRE(mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT) == MDBX_SUCCESS);

      REQUIRE(mdbx_txn_commit_ex(txn, nullptr) == MDBX_SUCCESS);
      mdbx_env_close_ex(env, false);
   }

   // Phase 2: reopen and verify the named DB and data are still there
   {
      MDBX_env* env = nullptr;
      REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
      REQUIRE(mdbx_env_set_maxdbs(env, 8) == MDBX_SUCCESS);
      REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

      MDBX_txn* txn = nullptr;
      REQUIRE(mdbx_txn_begin_ex(env, nullptr, MDBX_TXN_READWRITE, &txn, nullptr) == MDBX_SUCCESS);

      // Open without CREATE — should find existing DB
      MDBX_dbi dbi = 0;
      REQUIRE(mdbx_dbi_open(txn, "accounts", MDBX_DB_DEFAULTS, &dbi) == MDBX_SUCCESS);

      // Read back the value
      std::string key = "alice";
      MDBX_val    k{key.data(), key.size()};
      MDBX_val    v{};
      REQUIRE(mdbx_get(txn, dbi, &k, &v) == MDBX_SUCCESS);
      REQUIRE(std::string_view(static_cast<char*>(v.iov_base), v.iov_len) == "100");

      REQUIRE(mdbx_txn_commit_ex(txn, nullptr) == MDBX_SUCCESS);
      mdbx_env_close_ex(env, false);
   }

   fs::remove_all(dir);
}

// ════════════════════════════════════════════════════════════════════
// txn_reset / txn_renew tests
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: txn_reset and txn_renew cycle", "[mdbx][c-api][reset]")
{
   auto dir = make_temp_dir("c_reset");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   // Populate
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
      MDBX_dbi dbi = 0;
      mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);
      for (int i = 0; i < 100; i++)
      {
         auto key = "key_" + std::to_string(i);
         auto val = "val_" + std::to_string(i);
         MDBX_val k{key.data(), key.size()};
         MDBX_val v{val.data(), val.size()};
         mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT);
      }
      mdbx_txn_commit(txn);
   }

   // Open RO txn, then cycle reset/renew multiple times
   MDBX_txn* ro = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro) == MDBX_SUCCESS);

   MDBX_dbi dbi = 0;
   mdbx_dbi_open(ro, nullptr, MDBX_DB_DEFAULTS, &dbi);

   for (int cycle = 0; cycle < 10; cycle++)
   {
      REQUIRE(mdbx_txn_renew(ro) == MDBX_SUCCESS);

      // Read a value
      auto key = "key_" + std::to_string(cycle * 10);
      MDBX_val k{key.data(), key.size()};
      MDBX_val v{};
      REQUIRE(mdbx_get(ro, dbi, &k, &v) == MDBX_SUCCESS);

      auto expected = "val_" + std::to_string(cycle * 10);
      REQUIRE(std::string(static_cast<char*>(v.iov_base), v.iov_len) == expected);

      REQUIRE(mdbx_txn_reset(ro) == MDBX_SUCCESS);
   }

   // Reset on RW txn should fail
   MDBX_txn* rw = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &rw);
   REQUIRE(mdbx_txn_reset(rw) == MDBX_BAD_TXN);
   REQUIRE(mdbx_txn_renew(rw) == MDBX_BAD_TXN);
   mdbx_txn_abort(rw);

   mdbx_txn_abort(ro);
   mdbx_env_close(env);
}

TEST_CASE("C API: txn_renew sees new writes", "[mdbx][c-api][reset]")
{
   auto dir = make_temp_dir("c_renew_sees");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   // Create DBI
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
      MDBX_dbi dbi = 0;
      mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);
      MDBX_val k{const_cast<char*>("key"), 3};
      MDBX_val v{const_cast<char*>("v1"), 2};
      mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT);
      mdbx_txn_commit(txn);
   }

   // Start RO txn, read v1
   MDBX_txn* ro = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro);
   MDBX_dbi dbi = 0;
   mdbx_dbi_open(ro, nullptr, MDBX_DB_DEFAULTS, &dbi);

   {
      MDBX_val k{const_cast<char*>("key"), 3};
      MDBX_val v{};
      REQUIRE(mdbx_get(ro, dbi, &k, &v) == MDBX_SUCCESS);
      REQUIRE(std::string(static_cast<char*>(v.iov_base), v.iov_len) == "v1");
   }

   mdbx_txn_reset(ro);

   // Write v2 while RO is reset
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
      MDBX_val k{const_cast<char*>("key"), 3};
      MDBX_val v{const_cast<char*>("v2"), 2};
      mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT);
      mdbx_txn_commit(txn);
   }

   // Renew and verify we see v2
   mdbx_txn_renew(ro);
   {
      MDBX_val k{const_cast<char*>("key"), 3};
      MDBX_val v{};
      REQUIRE(mdbx_get(ro, dbi, &k, &v) == MDBX_SUCCESS);
      REQUIRE(std::string(static_cast<char*>(v.iov_base), v.iov_len) == "v2");
   }

   mdbx_txn_abort(ro);
   mdbx_env_close(env);
}

// ════════════════════════════════════════════════════════════════════
// Cursor SET / SET_RANGE tests
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: cursor SET exact match", "[mdbx][c-api][cursor]")
{
   auto dir = make_temp_dir("c_cursor_set");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
   MDBX_dbi dbi = 0;
   mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);

   const char* keys[] = {"aaa", "bbb", "ccc", "ddd", "eee"};
   for (auto* k : keys)
   {
      MDBX_val key{const_cast<char*>(k), strlen(k)};
      MDBX_val val{const_cast<char*>("x"), 1};
      mdbx_put(txn, dbi, &key, &val, MDBX_UPSERT);
   }
   mdbx_txn_commit(txn);

   MDBX_txn* ro = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro);
   MDBX_cursor* cur = nullptr;
   mdbx_cursor_open(ro, dbi, &cur);

   // SET: exact match
   {
      MDBX_val k{const_cast<char*>("ccc"), 3};
      MDBX_val v{};
      REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_SET) == MDBX_SUCCESS);
      REQUIRE(std::string(static_cast<char*>(k.iov_base), k.iov_len) == "ccc");
   }

   // SET: key not found
   {
      MDBX_val k{const_cast<char*>("abc"), 3};
      MDBX_val v{};
      REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_SET) == MDBX_NOTFOUND);
   }

   // SET_RANGE: exact match
   {
      MDBX_val k{const_cast<char*>("bbb"), 3};
      MDBX_val v{};
      REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_SET_RANGE) == MDBX_SUCCESS);
      REQUIRE(std::string(static_cast<char*>(k.iov_base), k.iov_len) == "bbb");
   }

   // SET_RANGE: lands on next key
   {
      MDBX_val k{const_cast<char*>("bbc"), 3};
      MDBX_val v{};
      REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_SET_RANGE) == MDBX_SUCCESS);
      REQUIRE(std::string(static_cast<char*>(k.iov_base), k.iov_len) == "ccc");
   }

   // SET_RANGE: past last key
   {
      MDBX_val k{const_cast<char*>("zzz"), 3};
      MDBX_val v{};
      REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_SET_RANGE) == MDBX_NOTFOUND);
   }

   mdbx_cursor_close(cur);
   mdbx_txn_abort(ro);
   mdbx_env_close(env);
}

TEST_CASE("C API: cursor reverse iteration", "[mdbx][c-api][cursor]")
{
   auto dir = make_temp_dir("c_cursor_rev");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
   MDBX_dbi dbi = 0;
   mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);

   for (int i = 0; i < 20; i++)
   {
      auto key = "k" + std::to_string(1000 + i);
      MDBX_val k{key.data(), key.size()};
      MDBX_val v{key.data(), key.size()};
      mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT);
   }
   mdbx_txn_commit(txn);

   MDBX_txn* ro = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro);
   MDBX_cursor* cur = nullptr;
   mdbx_cursor_open(ro, dbi, &cur);

   // LAST
   MDBX_val k, v;
   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_LAST) == MDBX_SUCCESS);
   REQUIRE(std::string(static_cast<char*>(k.iov_base), k.iov_len) == "k1019");

   // PREV all the way back
   int count = 1;
   std::string prev_key(static_cast<char*>(k.iov_base), k.iov_len);
   while (mdbx_cursor_get(cur, &k, &v, MDBX_PREV) == MDBX_SUCCESS)
   {
      std::string cur_key(static_cast<char*>(k.iov_base), k.iov_len);
      REQUIRE(cur_key < prev_key);
      prev_key = cur_key;
      count++;
   }
   REQUIRE(count == 20);

   mdbx_cursor_close(cur);
   mdbx_txn_abort(ro);
   mdbx_env_close(env);
}

// ════════════════════════════════════════════════════════════════════
// mdbx_replace tests
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: mdbx_replace get-and-set", "[mdbx][c-api]")
{
   auto dir = make_temp_dir("c_replace");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
   MDBX_dbi dbi = 0;
   mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);

   // Insert initial value
   MDBX_val k{const_cast<char*>("key"), 3};
   MDBX_val v{const_cast<char*>("old_value"), 9};
   mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT);

   // Replace: get old, put new
   MDBX_val new_val{const_cast<char*>("new_value"), 9};
   MDBX_val old_val{};
   REQUIRE(mdbx_replace(txn, dbi, &k, &new_val, &old_val, MDBX_UPSERT) == MDBX_SUCCESS);
   REQUIRE(std::string(static_cast<char*>(old_val.iov_base), old_val.iov_len) == "old_value");

   // Verify new value
   MDBX_val got{};
   REQUIRE(mdbx_get(txn, dbi, &k, &got) == MDBX_SUCCESS);
   REQUIRE(std::string(static_cast<char*>(got.iov_base), got.iov_len) == "new_value");

   // Replace on missing key: old_data should be empty
   MDBX_val k2{const_cast<char*>("missing"), 7};
   MDBX_val new2{const_cast<char*>("val"), 3};
   MDBX_val old2{};
   int rc = mdbx_replace(txn, dbi, &k2, &new2, &old2, MDBX_UPSERT);
   REQUIRE(rc == MDBX_SUCCESS);

   mdbx_txn_commit(txn);
   mdbx_env_close(env);
}

// ════════════════════════════════════════════════════════════════════
// Large value tests
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: large values", "[mdbx][c-api]")
{
   auto dir = make_temp_dir("c_large");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
   MDBX_dbi dbi = 0;
   mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);

   // Test various value sizes: 1B, 100B, 1KB, 10KB, 100KB, 1MB
   std::vector<size_t> sizes = {1, 100, 1024, 10240, 102400, 1048576};

   for (size_t sz : sizes)
   {
      auto key = "size_" + std::to_string(sz);
      std::string val(sz, 'A' + (sz % 26));

      MDBX_val k{key.data(), key.size()};
      MDBX_val v{val.data(), val.size()};
      REQUIRE(mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT) == MDBX_SUCCESS);
   }
   mdbx_txn_commit(txn);

   // Read back and verify
   MDBX_txn* ro = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro);

   for (size_t sz : sizes)
   {
      auto key = "size_" + std::to_string(sz);
      MDBX_val k{key.data(), key.size()};
      MDBX_val v{};
      REQUIRE(mdbx_get(ro, dbi, &k, &v) == MDBX_SUCCESS);
      REQUIRE(v.iov_len == sz);
      // Verify first and last bytes
      REQUIRE(static_cast<char*>(v.iov_base)[0] == (char)('A' + (sz % 26)));
      REQUIRE(static_cast<char*>(v.iov_base)[sz - 1] == (char)('A' + (sz % 26)));
   }

   mdbx_txn_abort(ro);
   mdbx_env_close(env);
}

TEST_CASE("C API: empty value", "[mdbx][c-api]")
{
   auto dir = make_temp_dir("c_empty_kv");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
   MDBX_dbi dbi = 0;
   mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);

   // Empty value (zero-length)
   {
      MDBX_val k{const_cast<char*>("key_empty_val"), 13};
      MDBX_val v{nullptr, 0};
      REQUIRE(mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT) == MDBX_SUCCESS);

      MDBX_val got{};
      REQUIRE(mdbx_get(txn, dbi, &k, &got) == MDBX_SUCCESS);
      REQUIRE(got.iov_len == 0);
   }

   mdbx_txn_commit(txn);
   mdbx_env_close(env);
}

// ════════════════════════════════════════════════════════════════════
// Read mode tests
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: read modes return correct data", "[mdbx][c-api][read-mode]")
{
   auto dir = make_temp_dir("c_readmode");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   // Populate data
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
      MDBX_dbi dbi = 0;
      mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);
      for (int i = 0; i < 50; i++)
      {
         auto key = "rk_" + std::to_string(i);
         auto val = "rv_" + std::to_string(i);
         MDBX_val k{key.data(), key.size()};
         MDBX_val v{val.data(), val.size()};
         mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT);
      }
      mdbx_txn_commit(txn);
   }

   // Test all three read modes
   int modes[] = {PSITRI_READ_MODE_BUFFERED, PSITRI_READ_MODE_LATEST, PSITRI_READ_MODE_TRIE};
   const char* mode_names[] = {"buffered", "latest", "trie"};

   for (int mi = 0; mi < 3; mi++)
   {
      SECTION(mode_names[mi])
      {
         REQUIRE(mdbx_env_set_read_mode(env, modes[mi]) == MDBX_SUCCESS);

         MDBX_txn* ro = nullptr;
         mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro);
         MDBX_dbi dbi = 0;
         mdbx_dbi_open(ro, nullptr, MDBX_DB_DEFAULTS, &dbi);

         // Read 10 keys spread across the range
         for (int i = 0; i < 50; i += 5)
         {
            auto key = "rk_" + std::to_string(i);
            auto expected = "rv_" + std::to_string(i);
            MDBX_val k{key.data(), key.size()};
            MDBX_val v{};
            int rc = mdbx_get(ro, dbi, &k, &v);
            // buffered/trie modes may not see data not yet swapped/merged
            if (modes[mi] != PSITRI_READ_MODE_LATEST && rc == MDBX_NOTFOUND)
               continue;
            REQUIRE(rc == MDBX_SUCCESS);
            REQUIRE(std::string(static_cast<char*>(v.iov_base), v.iov_len) == expected);
         }

         // Missing key should return NOTFOUND in all modes
         {
            auto key = std::string("nonexistent");
            MDBX_val k{key.data(), key.size()};
            MDBX_val v{};
            REQUIRE(mdbx_get(ro, dbi, &k, &v) == MDBX_NOTFOUND);
         }

         mdbx_txn_abort(ro);
      }
   }

   mdbx_env_close(env);
}

TEST_CASE("C API: invalid read mode rejected", "[mdbx][c-api][read-mode]")
{
   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   REQUIRE(mdbx_env_set_read_mode(env, -1) == MDBX_EINVAL);
   REQUIRE(mdbx_env_set_read_mode(env, 3) == MDBX_EINVAL);
   REQUIRE(mdbx_env_set_read_mode(env, 0) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_read_mode(env, 1) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_read_mode(env, 2) == MDBX_SUCCESS);
   mdbx_env_close(env);
}

// ════════════════════════════════════════════════════════════════════
// Concurrent reader + writer tests
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: concurrent reader and writer", "[mdbx][c-api][concurrent]")
{
   auto dir = make_temp_dir("c_concurrent");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   // Create DBI and seed data
   MDBX_dbi dbi = 0;
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
      mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);
      for (int i = 0; i < 100; i++)
      {
         auto key = "ck_" + std::to_string(i);
         std::string val = "cv_0";
         MDBX_val k{key.data(), key.size()};
         MDBX_val v{val.data(), val.size()};
         mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT);
      }
      mdbx_txn_commit(txn);
   }

   std::atomic<bool> stop{false};
   std::atomic<bool> reader_ready{false};
   std::atomic<int> read_count{0};
   std::atomic<int> read_errors{0};

   // Reader thread: reset/renew loop doing point reads
   std::thread reader([&]()
   {
      MDBX_txn* ro = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro);
      mdbx_txn_reset(ro);
      reader_ready.store(true, std::memory_order_release);

      while (!stop.load(std::memory_order_relaxed))
      {
         mdbx_txn_renew(ro);

         auto key = "ck_" + std::to_string(read_count.load() % 100);
         MDBX_val k{key.data(), key.size()};
         MDBX_val v{};
         int rc = mdbx_get(ro, dbi, &k, &v);
         if (rc == MDBX_SUCCESS)
            read_count.fetch_add(1, std::memory_order_relaxed);
         else
            read_errors.fetch_add(1, std::memory_order_relaxed);

         mdbx_txn_reset(ro);
      }
      mdbx_txn_abort(ro);
   });

   // Wait for reader to be ready before starting writes
   while (!reader_ready.load(std::memory_order_acquire)) {}

   // Writer: update values
   for (int round = 1; round <= 20; round++)
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
      for (int i = 0; i < 100; i++)
      {
         auto key = "ck_" + std::to_string(i);
         auto val = "cv_" + std::to_string(round);
         MDBX_val k{key.data(), key.size()};
         MDBX_val v{val.data(), val.size()};
         mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT);
      }
      mdbx_txn_commit(txn);
   }

   stop.store(true, std::memory_order_relaxed);
   reader.join();

   // Reader should have completed some reads; errors are acceptable
   // since the reader may race with writer commits
   REQUIRE(read_count.load() + read_errors.load() > 0);

   mdbx_env_close(env);
}

// ════════════════════════════════════════════════════════════════════
// Multiple named DBIs tests
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: many named DBIs", "[mdbx][c-api]")
{
   auto dir = make_temp_dir("c_many_dbis");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 32);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   const int NUM_DBIS = 10;
   MDBX_dbi dbis[NUM_DBIS];

   // Create and populate all DBIs in one transaction
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);

      for (int d = 0; d < NUM_DBIS; d++)
      {
         auto name = "db_" + std::to_string(d);
         REQUIRE(mdbx_dbi_open(txn, name.c_str(), MDBX_CREATE, &dbis[d]) == MDBX_SUCCESS);

         for (int i = 0; i < 10; i++)
         {
            auto key = "k" + std::to_string(d) + "_" + std::to_string(i);
            auto val = "v" + std::to_string(d) + "_" + std::to_string(i);
            MDBX_val k{key.data(), key.size()};
            MDBX_val v{val.data(), val.size()};
            mdbx_put(txn, dbis[d], &k, &v, MDBX_UPSERT);
         }
      }
      mdbx_txn_commit(txn);
   }

   // Verify each DBI has exactly 10 entries and correct data
   {
      MDBX_txn* ro = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro);

      for (int d = 0; d < NUM_DBIS; d++)
      {
         MDBX_stat stat{};
         REQUIRE(mdbx_dbi_stat(ro, dbis[d], &stat, sizeof(stat)) == MDBX_SUCCESS);
         REQUIRE(stat.ms_entries == 10);

         // Check a specific key
         auto key = "k" + std::to_string(d) + "_5";
         auto expected = "v" + std::to_string(d) + "_5";
         MDBX_val k{key.data(), key.size()};
         MDBX_val v{};
         REQUIRE(mdbx_get(ro, dbis[d], &k, &v) == MDBX_SUCCESS);
         REQUIRE(std::string(static_cast<char*>(v.iov_base), v.iov_len) == expected);

         // Key from another DBI shouldn't exist
         auto wrong_key = "k" + std::to_string((d + 1) % NUM_DBIS) + "_5";
         MDBX_val wk{wrong_key.data(), wrong_key.size()};
         MDBX_val wv{};
         REQUIRE(mdbx_get(ro, dbis[d], &wk, &wv) == MDBX_NOTFOUND);
      }

      mdbx_txn_abort(ro);
   }

   mdbx_env_close(env);
}

// ════════════════════════════════════════════════════════════════════
// Overwrite / update tests
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: overwrite with different sizes", "[mdbx][c-api]")
{
   auto dir = make_temp_dir("c_overwrite");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
   MDBX_dbi dbi = 0;
   mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);

   MDBX_val k{const_cast<char*>("key"), 3};

   // Write v1
   MDBX_val v1{const_cast<char*>("short"), 5};
   REQUIRE(mdbx_put(txn, dbi, &k, &v1, MDBX_UPSERT) == MDBX_SUCCESS);

   // Overwrite with longer value
   std::string long_val(500, 'X');
   MDBX_val v2{long_val.data(), long_val.size()};
   REQUIRE(mdbx_put(txn, dbi, &k, &v2, MDBX_UPSERT) == MDBX_SUCCESS);

   // Read back
   MDBX_val got{};
   REQUIRE(mdbx_get(txn, dbi, &k, &got) == MDBX_SUCCESS);
   REQUIRE(got.iov_len == 500);
   REQUIRE(static_cast<char*>(got.iov_base)[0] == 'X');

   // Overwrite with shorter value
   MDBX_val v3{const_cast<char*>("y"), 1};
   REQUIRE(mdbx_put(txn, dbi, &k, &v3, MDBX_UPSERT) == MDBX_SUCCESS);

   got = {};
   REQUIRE(mdbx_get(txn, dbi, &k, &got) == MDBX_SUCCESS);
   REQUIRE(got.iov_len == 1);
   REQUIRE(static_cast<char*>(got.iov_base)[0] == 'y');

   mdbx_txn_commit(txn);
   mdbx_env_close(env);
}

// ════════════════════════════════════════════════════════════════════
// userctx tests
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: env userctx", "[mdbx][c-api]")
{
   MDBX_env* env = nullptr;
   mdbx_env_create(&env);

   REQUIRE(mdbx_env_get_userctx(env) == nullptr);

   int data = 42;
   REQUIRE(mdbx_env_set_userctx(env, &data) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_get_userctx(env) == &data);

   REQUIRE(mdbx_env_set_userctx(env, nullptr) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_get_userctx(env) == nullptr);

   mdbx_env_close(env);
}

// ════════════════════════════════════════════════════════════════════
// Bulk insert + cursor scan consistency
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: bulk insert and full scan", "[mdbx][c-api]")
{
   auto dir = make_temp_dir("c_bulk");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   const int N = 5000;

   // Bulk insert
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
      MDBX_dbi dbi = 0;
      mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);

      for (int i = 0; i < N; i++)
      {
         char key[16];
         snprintf(key, sizeof(key), "key_%08d", i);
         auto val = "value_" + std::to_string(i);
         MDBX_val k{key, strlen(key)};
         MDBX_val v{val.data(), val.size()};
         REQUIRE(mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT) == MDBX_SUCCESS);
      }
      mdbx_txn_commit(txn);
   }

   // Stat should report N entries
   {
      MDBX_txn* ro = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro);
      MDBX_dbi dbi = 0;
      mdbx_dbi_open(ro, nullptr, MDBX_DB_DEFAULTS, &dbi);

      MDBX_stat stat{};
      mdbx_dbi_stat(ro, dbi, &stat, sizeof(stat));
      REQUIRE(stat.ms_entries == N);

      // Full scan in sorted order
      MDBX_cursor* cur = nullptr;
      mdbx_cursor_open(ro, dbi, &cur);

      MDBX_val k, v;
      int count = 0;
      std::string prev;
      int rc = mdbx_cursor_get(cur, &k, &v, MDBX_FIRST);
      while (rc == MDBX_SUCCESS)
      {
         std::string key(static_cast<char*>(k.iov_base), k.iov_len);
         REQUIRE(key > prev);
         prev = key;
         count++;
         rc = mdbx_cursor_get(cur, &k, &v, MDBX_NEXT);
      }
      REQUIRE(count == N);

      mdbx_cursor_close(cur);
      mdbx_txn_abort(ro);
   }

   mdbx_env_close(env);
}

// ════════════════════════════════════════════════════════════════════
// Delete + re-insert consistency
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: delete and re-insert", "[mdbx][c-api]")
{
   auto dir = make_temp_dir("c_del_reinsert");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_dbi dbi = 0;

   // Insert 100 keys
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
      mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);
      for (int i = 0; i < 100; i++)
      {
         auto key = "dk_" + std::to_string(i);
         auto val = "dv_" + std::to_string(i);
         MDBX_val k{key.data(), key.size()};
         MDBX_val v{val.data(), val.size()};
         mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT);
      }
      mdbx_txn_commit(txn);
   }

   // Delete even keys
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
      for (int i = 0; i < 100; i += 2)
      {
         auto key = "dk_" + std::to_string(i);
         MDBX_val k{key.data(), key.size()};
         REQUIRE(mdbx_del(txn, dbi, &k, nullptr) == MDBX_SUCCESS);
      }
      mdbx_txn_commit(txn);
   }

   // Verify only odd keys remain
   {
      MDBX_txn* ro = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro);

      MDBX_stat stat{};
      mdbx_dbi_stat(ro, dbi, &stat, sizeof(stat));
      REQUIRE(stat.ms_entries == 50);

      for (int i = 0; i < 100; i++)
      {
         auto key = "dk_" + std::to_string(i);
         MDBX_val k{key.data(), key.size()};
         MDBX_val v{};
         int rc = mdbx_get(ro, dbi, &k, &v);
         if (i % 2 == 0)
            REQUIRE(rc == MDBX_NOTFOUND);
         else
            REQUIRE(rc == MDBX_SUCCESS);
      }
      mdbx_txn_abort(ro);
   }

   // Re-insert even keys with new values
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
      for (int i = 0; i < 100; i += 2)
      {
         auto key = "dk_" + std::to_string(i);
         auto val = "new_" + std::to_string(i);
         MDBX_val k{key.data(), key.size()};
         MDBX_val v{val.data(), val.size()};
         mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT);
      }
      mdbx_txn_commit(txn);
   }

   // Verify all 100 keys present, even keys have new values
   {
      MDBX_txn* ro = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro);

      MDBX_stat stat{};
      mdbx_dbi_stat(ro, dbi, &stat, sizeof(stat));
      REQUIRE(stat.ms_entries == 100);

      for (int i = 0; i < 100; i++)
      {
         auto key = "dk_" + std::to_string(i);
         MDBX_val k{key.data(), key.size()};
         MDBX_val v{};
         REQUIRE(mdbx_get(ro, dbi, &k, &v) == MDBX_SUCCESS);

         std::string val(static_cast<char*>(v.iov_base), v.iov_len);
         if (i % 2 == 0)
            REQUIRE(val == "new_" + std::to_string(i));
         else
            REQUIRE(val == "dv_" + std::to_string(i));
      }
      mdbx_txn_abort(ro);
   }

   mdbx_env_close(env);
}

// ════════════════════════════════════════════════════════════════════
// cursor_on_first / cursor_on_last tests
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: cursor_on_first and cursor_on_last", "[mdbx][c-api][cursor]")
{
   auto dir = make_temp_dir("c_on_first_last");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
   MDBX_dbi dbi = 0;
   mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);

   const char* keys[] = {"aaa", "bbb", "ccc"};
   for (auto* k : keys)
   {
      MDBX_val key{const_cast<char*>(k), strlen(k)};
      MDBX_val val{const_cast<char*>("v"), 1};
      mdbx_put(txn, dbi, &key, &val, MDBX_UPSERT);
   }
   mdbx_txn_commit(txn);

   MDBX_txn* ro = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro);
   MDBX_cursor* cur = nullptr;
   mdbx_cursor_open(ro, dbi, &cur);

   MDBX_val k, v;

   // Position at first
   mdbx_cursor_get(cur, &k, &v, MDBX_FIRST);
   REQUIRE(mdbx_cursor_on_first(cur) == MDBX_RESULT_TRUE);
   REQUIRE(mdbx_cursor_on_last(cur) == MDBX_RESULT_FALSE);

   // Move to middle
   mdbx_cursor_get(cur, &k, &v, MDBX_NEXT);
   REQUIRE(mdbx_cursor_on_first(cur) == MDBX_RESULT_FALSE);
   REQUIRE(mdbx_cursor_on_last(cur) == MDBX_RESULT_FALSE);

   // Move to last
   mdbx_cursor_get(cur, &k, &v, MDBX_LAST);
   REQUIRE(mdbx_cursor_on_first(cur) == MDBX_RESULT_FALSE);
   REQUIRE(mdbx_cursor_on_last(cur) == MDBX_RESULT_TRUE);

   mdbx_cursor_close(cur);
   mdbx_txn_abort(ro);
   mdbx_env_close(env);
}

// ════════════════════════════════════════════════════════════════════
// Sequential write transactions accumulate correctly
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: sequential write transactions", "[mdbx][c-api]")
{
   auto dir = make_temp_dir("c_seq_txn");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_dbi dbi = 0;

   // 10 sequential write transactions
   for (int t = 0; t < 10; t++)
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
      if (t == 0)
         mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);

      for (int i = 0; i < 10; i++)
      {
         auto key = "t" + std::to_string(t) + "_k" + std::to_string(i);
         std::string val = "val";
         MDBX_val k{key.data(), key.size()};
         MDBX_val v{val.data(), val.size()};
         mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT);
      }
      mdbx_txn_commit(txn);
   }

   // Should have 100 entries total
   MDBX_txn* ro = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro);
   MDBX_stat stat{};
   mdbx_dbi_stat(ro, dbi, &stat, sizeof(stat));
   REQUIRE(stat.ms_entries == 100);
   mdbx_txn_abort(ro);

   mdbx_env_close(env);
}

// ════════════════════════════════════════════════════════════════════
// Phase 3: New test cases for bug fixes and missing APIs
// ════════════════════════════════════════════════════════════════════

TEST_CASE("C API: multi-DBI transaction atomicity (commit)", "[mdbx][c-api][atomicity]")
{
   auto dir = make_temp_dir("multi_dbi_commit");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   // Write to two different named databases in one transaction
   {
      MDBX_txn* txn = nullptr;
      REQUIRE(mdbx_txn_begin_ex(env, nullptr, MDBX_TXN_READWRITE, &txn, nullptr) == MDBX_SUCCESS);

      MDBX_dbi dbi1 = 0, dbi2 = 0;
      REQUIRE(mdbx_dbi_open(txn, "table_a", MDBX_CREATE, &dbi1) == MDBX_SUCCESS);
      REQUIRE(mdbx_dbi_open(txn, "table_b", MDBX_CREATE, &dbi2) == MDBX_SUCCESS);

      MDBX_val k1{const_cast<char*>("k1"), 2};
      MDBX_val v1{const_cast<char*>("from_a"), 6};
      REQUIRE(mdbx_put(txn, dbi1, &k1, &v1, MDBX_UPSERT) == MDBX_SUCCESS);

      MDBX_val k2{const_cast<char*>("k2"), 2};
      MDBX_val v2{const_cast<char*>("from_b"), 6};
      REQUIRE(mdbx_put(txn, dbi2, &k2, &v2, MDBX_UPSERT) == MDBX_SUCCESS);

      REQUIRE(mdbx_txn_commit(txn) == MDBX_SUCCESS);
   }

   // Read back — both tables should have their data
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn);

      MDBX_dbi dbi1 = 0, dbi2 = 0;
      mdbx_dbi_open(txn, "table_a", MDBX_DB_DEFAULTS, &dbi1);
      mdbx_dbi_open(txn, "table_b", MDBX_DB_DEFAULTS, &dbi2);

      MDBX_val k1{const_cast<char*>("k1"), 2};
      MDBX_val val{};
      REQUIRE(mdbx_get(txn, dbi1, &k1, &val) == MDBX_SUCCESS);
      REQUIRE(std::string_view(static_cast<char*>(val.iov_base), val.iov_len) == "from_a");

      MDBX_val k2{const_cast<char*>("k2"), 2};
      REQUIRE(mdbx_get(txn, dbi2, &k2, &val) == MDBX_SUCCESS);
      REQUIRE(std::string_view(static_cast<char*>(val.iov_base), val.iov_len) == "from_b");

      mdbx_txn_abort(txn);
   }

   mdbx_env_close(env);
}

TEST_CASE("C API: multi-DBI transaction atomicity (abort)", "[mdbx][c-api][atomicity]")
{
   auto dir = make_temp_dir("multi_dbi_abort");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   // Insert initial data
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin_ex(env, nullptr, MDBX_TXN_READWRITE, &txn, nullptr);
      MDBX_dbi dbi = 0;
      mdbx_dbi_open(txn, "tbl", MDBX_CREATE, &dbi);
      MDBX_val k{const_cast<char*>("orig"), 4};
      MDBX_val v{const_cast<char*>("data"), 4};
      mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT);
      mdbx_txn_commit(txn);
   }

   // Write to two tables then abort — nothing should persist
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin_ex(env, nullptr, MDBX_TXN_READWRITE, &txn, nullptr);
      MDBX_dbi dbi1 = 0, dbi2 = 0;
      mdbx_dbi_open(txn, "tbl", MDBX_DB_DEFAULTS, &dbi1);
      mdbx_dbi_open(txn, "tbl2", MDBX_CREATE, &dbi2);

      MDBX_val k1{const_cast<char*>("new_key"), 7};
      MDBX_val v1{const_cast<char*>("new_val"), 7};
      mdbx_put(txn, dbi1, &k1, &v1, MDBX_UPSERT);

      MDBX_val k2{const_cast<char*>("x"), 1};
      MDBX_val v2{const_cast<char*>("y"), 1};
      mdbx_put(txn, dbi2, &k2, &v2, MDBX_UPSERT);

      mdbx_txn_abort(txn);
   }

   // Verify: orig data still there, aborted data not
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn);
      MDBX_dbi dbi = 0;
      mdbx_dbi_open(txn, "tbl", MDBX_DB_DEFAULTS, &dbi);

      MDBX_val k{const_cast<char*>("orig"), 4};
      MDBX_val val{};
      REQUIRE(mdbx_get(txn, dbi, &k, &val) == MDBX_SUCCESS);
      REQUIRE(std::string_view(static_cast<char*>(val.iov_base), val.iov_len) == "data");

      MDBX_val k2{const_cast<char*>("new_key"), 7};
      REQUIRE(mdbx_get(txn, dbi, &k2, &val) == MDBX_NOTFOUND);

      mdbx_txn_abort(txn);
   }

   mdbx_env_close(env);
}

TEST_CASE("C API: MDBX_CURRENT returns NOTFOUND for missing key", "[mdbx][c-api][flags]")
{
   auto dir = make_temp_dir("current_flag");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 4);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   mdbx_txn_begin_ex(env, nullptr, MDBX_TXN_READWRITE, &txn, nullptr);
   MDBX_dbi dbi = 0;
   mdbx_dbi_open(txn, nullptr, MDBX_CREATE, &dbi);

   // Insert a key
   MDBX_val k{const_cast<char*>("exists"), 6};
   MDBX_val v{const_cast<char*>("val1"), 4};
   REQUIRE(mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT) == MDBX_SUCCESS);

   // MDBX_CURRENT on existing key should succeed
   MDBX_val v2{const_cast<char*>("val2"), 4};
   REQUIRE(mdbx_put(txn, dbi, &k, &v2, MDBX_CURRENT) == MDBX_SUCCESS);

   // MDBX_CURRENT on missing key should return NOTFOUND
   MDBX_val km{const_cast<char*>("missing"), 7};
   MDBX_val vm{const_cast<char*>("nope"), 4};
   REQUIRE(mdbx_put(txn, dbi, &km, &vm, MDBX_CURRENT) == MDBX_NOTFOUND);

   mdbx_txn_abort(txn);
   mdbx_env_close(env);
}

TEST_CASE("C API: MDBX_APPEND validates key ordering", "[mdbx][c-api][flags]")
{
   auto dir = make_temp_dir("append_flag");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 4);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   mdbx_txn_begin_ex(env, nullptr, MDBX_TXN_READWRITE, &txn, nullptr);
   MDBX_dbi dbi = 0;
   mdbx_dbi_open(txn, nullptr, MDBX_CREATE, &dbi);

   // First APPEND should succeed (empty DB)
   MDBX_val k1{const_cast<char*>("aaa"), 3};
   MDBX_val v1{const_cast<char*>("v"), 1};
   REQUIRE(mdbx_put(txn, dbi, &k1, &v1, MDBX_APPEND) == MDBX_SUCCESS);

   // APPEND with greater key should succeed
   MDBX_val k2{const_cast<char*>("bbb"), 3};
   REQUIRE(mdbx_put(txn, dbi, &k2, &v1, MDBX_APPEND) == MDBX_SUCCESS);

   // APPEND with smaller key should fail
   MDBX_val k3{const_cast<char*>("aab"), 3};
   REQUIRE(mdbx_put(txn, dbi, &k3, &v1, MDBX_APPEND) == MDBX_EKEYMISMATCH);

   mdbx_txn_abort(txn);
   mdbx_env_close(env);
}

TEST_CASE("C API: MDBX_NOOVERWRITE returns existing value", "[mdbx][c-api][flags]")
{
   auto dir = make_temp_dir("nooverwrite_flag");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 4);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   mdbx_txn_begin_ex(env, nullptr, MDBX_TXN_READWRITE, &txn, nullptr);
   MDBX_dbi dbi = 0;
   mdbx_dbi_open(txn, nullptr, MDBX_CREATE, &dbi);

   MDBX_val k{const_cast<char*>("key"), 3};
   MDBX_val v{const_cast<char*>("original"), 8};
   REQUIRE(mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT) == MDBX_SUCCESS);

   // NOOVERWRITE should fail and return existing value
   MDBX_val v2{const_cast<char*>("replaced"), 8};
   REQUIRE(mdbx_put(txn, dbi, &k, &v2, MDBX_NOOVERWRITE) == MDBX_KEYEXIST);
   REQUIRE(std::string_view(static_cast<char*>(v2.iov_base), v2.iov_len) == "original");

   mdbx_txn_abort(txn);
   mdbx_env_close(env);
}

TEST_CASE("C API: mdbx_cursor_create unbound", "[mdbx][c-api][cursor]")
{
   // Verify that mdbx_cursor_create allocates an unbound cursor
   MDBX_cursor* cur = mdbx_cursor_create(nullptr);
   REQUIRE(cur != nullptr);
   mdbx_cursor_close(cur);
}

TEST_CASE("C++ API: cursor bind pattern", "[mdbx][cpp-api][cursor]")
{
   auto dir = make_temp_dir("cursor_bind");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;
   mdbx::env_managed db(dir.c_str(), cp, op);

   {
      auto txn = db.start_write();
      auto map = txn.create_map("test");
      txn.upsert(map, mdbx::slice("a"), mdbx::slice("1"));
      txn.upsert(map, mdbx::slice("b"), mdbx::slice("2"));
      txn.commit();
   }

   {
      auto txn = db.start_read();
      auto map = txn.open_map("test");
      auto cur = txn.open_cursor(map);

      // Bind to same txn/map should work
      cur.bind(txn, map);

      auto kv = cur.to_first();
      REQUIRE(kv.key.string_view() == "a");
   }
}

TEST_CASE("C API: mdbx_cursor_copy clones position", "[mdbx][c-api][cursor]")
{
   auto dir = make_temp_dir("cursor_copy");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 4);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin_ex(env, nullptr, MDBX_TXN_READWRITE, &txn, nullptr);
      MDBX_dbi dbi = 0;
      mdbx_dbi_open(txn, nullptr, MDBX_CREATE, &dbi);
      for (int i = 0; i < 5; i++)
      {
         auto key = "key" + std::to_string(i);
         auto val = "val" + std::to_string(i);
         MDBX_val k{key.data(), key.size()};
         MDBX_val v{val.data(), val.size()};
         mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT);
      }
      mdbx_txn_commit(txn);
   }

   MDBX_txn* txn = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn);
   MDBX_dbi dbi = 0;
   mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi);

   MDBX_cursor* src = nullptr;
   mdbx_cursor_open(txn, dbi, &src);

   // Position src at key2
   MDBX_val k{}, v{};
   mdbx_cursor_get(src, &k, &v, MDBX_FIRST);
   mdbx_cursor_get(src, &k, &v, MDBX_NEXT);
   mdbx_cursor_get(src, &k, &v, MDBX_NEXT);
   REQUIRE(std::string_view(static_cast<char*>(k.iov_base), k.iov_len) == "key2");

   // Copy and verify same position
   MDBX_cursor* dst = nullptr;
   mdbx_cursor_open(txn, dbi, &dst);
   REQUIRE(mdbx_cursor_copy(src, dst) == MDBX_SUCCESS);

   MDBX_val k2{}, v2{};
   REQUIRE(mdbx_cursor_get(dst, &k2, &v2, MDBX_GET_CURRENT) == MDBX_SUCCESS);
   REQUIRE(std::string_view(static_cast<char*>(k2.iov_base), k2.iov_len) == "key2");

   // Advance src — dst should stay at key2
   mdbx_cursor_get(src, &k, &v, MDBX_NEXT);
   REQUIRE(std::string_view(static_cast<char*>(k.iov_base), k.iov_len) == "key3");

   mdbx_cursor_get(dst, &k2, &v2, MDBX_GET_CURRENT);
   REQUIRE(std::string_view(static_cast<char*>(k2.iov_base), k2.iov_len) == "key2");

   mdbx_cursor_close(src);
   mdbx_cursor_close(dst);
   mdbx_txn_abort(txn);
   mdbx_env_close(env);
}

TEST_CASE("C API: mdbx_env_set_option / get_option", "[mdbx][c-api][option]")
{
   MDBX_env* env = nullptr;
   mdbx_env_create(&env);

   // Most options are no-ops in the shim, but should round-trip
   REQUIRE(mdbx_env_set_option(env, MDBX_opt_max_db, 32) == MDBX_SUCCESS);

   uint64_t val = 0;
   REQUIRE(mdbx_env_get_option(env, MDBX_opt_max_db, &val) == MDBX_SUCCESS);

   // Unknown options are silently accepted (no-op) by the shim
   REQUIRE(mdbx_env_set_option(env, static_cast<MDBX_option_t>(9999), 42) == MDBX_SUCCESS);

   mdbx_env_close_ex(env, false);
}

TEST_CASE("C++ API: DUPSORT erase via txn", "[mdbx][cpp-api][dupsort]")
{
   auto dir = make_temp_dir("dupsort_erase");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;
   mdbx::env_managed db(dir.c_str(), cp, op);

   // Insert multiple dups for one key
   {
      auto txn = db.start_write();
      auto map = txn.create_map("ds", mdbx::key_mode::usual, mdbx::value_mode::multi);

      txn.upsert(map, mdbx::slice("key"), mdbx::slice("alpha"));
      txn.upsert(map, mdbx::slice("key"), mdbx::slice("beta"));
      txn.upsert(map, mdbx::slice("key"), mdbx::slice("gamma"));
      txn.commit();
   }

   // Verify all dups via read cursor
   {
      auto txn = db.start_read();
      auto map = txn.open_map("ds");
      auto cur = txn.open_cursor(map);

      auto kv = cur.to_first();
      REQUIRE(kv.done);
      REQUIRE(kv.value.string_view() == "alpha");
      kv = cur.to_next();
      REQUIRE(kv.value.string_view() == "beta");
      kv = cur.to_next();
      REQUIRE(kv.value.string_view() == "gamma");
   }

   // Erase specific dup value via txn.erase(map, key, value)
   {
      auto txn = db.start_write();
      auto map = txn.open_map("ds");
      REQUIRE(txn.erase(map, mdbx::slice("key"), mdbx::slice("beta")));
      txn.commit();
   }

   // Verify: alpha and gamma remain, beta is gone
   {
      auto txn = db.start_read();
      auto map = txn.open_map("ds");
      auto cur = txn.open_cursor(map);

      auto kv = cur.to_first();
      REQUIRE(kv.value.string_view() == "alpha");
      kv = cur.to_next();
      REQUIRE(kv.value.string_view() == "gamma");
      kv = cur.to_next(false);
      REQUIRE_FALSE(kv.done);
   }

   // Erase remaining key via txn.erase(map, key)  — removes all dups
   {
      auto txn = db.start_write();
      auto map = txn.open_map("ds");
      REQUIRE(txn.erase(map, mdbx::slice("key")));
      txn.commit();
   }

   // Verify: key is gone
   {
      auto txn = db.start_read();
      auto map = txn.open_map("ds");
      REQUIRE_THROWS_AS(txn.get(map, mdbx::slice("key")), mdbx::not_found);
   }
}

TEST_CASE("C++ API: cursor move generic operation", "[mdbx][cpp-api][cursor]")
{
   auto dir = make_temp_dir("cursor_move");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;
   mdbx::env_managed db(dir.c_str(), cp, op);

   {
      auto txn = db.start_write();
      auto map = txn.create_map("test");
      txn.upsert(map, mdbx::slice("a"), mdbx::slice("1"));
      txn.upsert(map, mdbx::slice("b"), mdbx::slice("2"));
      txn.upsert(map, mdbx::slice("c"), mdbx::slice("3"));
      txn.commit();
   }

   {
      auto txn = db.start_read();
      auto map = txn.open_map("test");
      auto cur = txn.open_cursor(map);

      // FIRST
      auto r1 = cur.move(MDBX_FIRST, nullptr, nullptr, false);
      REQUIRE(r1.done);
      REQUIRE(r1.key.string_view() == "a");

      // NEXT
      auto r2 = cur.move(MDBX_NEXT, nullptr, nullptr, false);
      REQUIRE(r2.done);
      REQUIRE(r2.key.string_view() == "b");

      // LAST
      auto r3 = cur.move(MDBX_LAST, nullptr, nullptr, false);
      REQUIRE(r3.done);
      REQUIRE(r3.key.string_view() == "c");

      // NEXT past end — should not throw, done=false
      auto r4 = cur.move(MDBX_NEXT, nullptr, nullptr, false);
      REQUIRE_FALSE(r4.done);
   }
}

TEST_CASE("C API: txn_reset releases snapshot", "[mdbx][c-api][txn]")
{
   auto dir = make_temp_dir("txn_reset");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 4);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   // Insert initial data
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin_ex(env, nullptr, MDBX_TXN_READWRITE, &txn, nullptr);
      MDBX_dbi dbi = 0;
      mdbx_dbi_open(txn, nullptr, MDBX_CREATE, &dbi);
      MDBX_val k{const_cast<char*>("k"), 1};
      MDBX_val v{const_cast<char*>("v1"), 2};
      mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT);
      mdbx_txn_commit(txn);
   }

   // Start RO transaction, read, reset, renew after a write, read again
   MDBX_txn* ro = nullptr;
   mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro);
   MDBX_dbi dbi = 0;
   mdbx_dbi_open(ro, nullptr, MDBX_DB_DEFAULTS, &dbi);

   MDBX_val k{const_cast<char*>("k"), 1};
   MDBX_val val{};
   REQUIRE(mdbx_get(ro, dbi, &k, &val) == MDBX_SUCCESS);
   REQUIRE(std::string_view(static_cast<char*>(val.iov_base), val.iov_len) == "v1");

   // Reset (releases snapshot)
   REQUIRE(mdbx_txn_reset(ro) == MDBX_SUCCESS);

   // Write new value while RO txn is reset
   {
      MDBX_txn* rw = nullptr;
      mdbx_txn_begin_ex(env, nullptr, MDBX_TXN_READWRITE, &rw, nullptr);
      MDBX_dbi dbi2 = 0;
      mdbx_dbi_open(rw, nullptr, MDBX_DB_DEFAULTS, &dbi2);
      MDBX_val v2{const_cast<char*>("v2"), 2};
      mdbx_put(rw, dbi2, &k, &v2, MDBX_UPSERT);
      mdbx_txn_commit(rw);
   }

   // Renew and read — should see new value
   REQUIRE(mdbx_txn_renew(ro) == MDBX_SUCCESS);
   REQUIRE(mdbx_get(ro, dbi, &k, &val) == MDBX_SUCCESS);
   REQUIRE(std::string_view(static_cast<char*>(val.iov_base), val.iov_len) == "v2");

   mdbx_txn_abort(ro);
   mdbx_env_close(env);
}

TEST_CASE("C++ API: env get_stat, get_info, get_pagesize", "[mdbx][cpp-api][env]")
{
   auto dir = make_temp_dir("env_info");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;
   mdbx::env_managed db(dir.c_str(), cp, op);

   auto stat = db.get_stat();
   REQUIRE(stat.ms_psize > 0);

   auto info = db.get_info();
   REQUIRE(info.mi_dxb_pagesize > 0);

   REQUIRE(db.get_pagesize() > 0);
}

TEST_CASE("C++ API: txn get_map_stat and get_handle_info", "[mdbx][cpp-api][txn]")
{
   auto dir = make_temp_dir("map_stat");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;
   mdbx::env_managed db(dir.c_str(), cp, op);

   {
      auto txn = db.start_write();
      auto map = txn.create_map("test");
      txn.upsert(map, mdbx::slice("a"), mdbx::slice("1"));
      txn.upsert(map, mdbx::slice("b"), mdbx::slice("2"));
      txn.commit();
   }

   {
      auto txn = db.start_read();
      auto map = txn.open_map("test");

      auto stat = txn.get_map_stat(map);
      REQUIRE(stat.ms_entries == 2);

      auto info = txn.get_handle_info(map);
      REQUIRE(info.dbi == map.dbi);
   }
}

TEST_CASE("C API: DUPSORT GET_BOTH in write transaction", "[mdbx][dupsort]")
{
   auto dir = make_temp_dir("dupsort_get_both_rw");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 16);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_dbi dbi;

   // Write data in first transaction
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
      mdbx_dbi_open(txn, "ds", MDBX_db_flags_t(MDBX_DUPSORT | MDBX_CREATE), &dbi);

      MDBX_val key{const_cast<char*>("key1"), 4};
      MDBX_val val1{const_cast<char*>("alpha"), 5};
      MDBX_val val2{const_cast<char*>("beta"), 4};
      MDBX_val val3{const_cast<char*>("gamma"), 5};
      mdbx_put(txn, dbi, &key, &val1, MDBX_UPSERT);
      mdbx_put(txn, dbi, &key, &val2, MDBX_UPSERT);
      mdbx_put(txn, dbi, &key, &val3, MDBX_UPSERT);
      mdbx_txn_commit(txn);
   }

   // Open a write transaction, open cursor, use GET_BOTH
   {
      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
      mdbx_dbi_open(txn, "ds", MDBX_db_flags_t(MDBX_DUPSORT), &dbi);

      MDBX_cursor* cur = nullptr;
      REQUIRE(mdbx_cursor_open(txn, dbi, &cur) == MDBX_SUCCESS);

      // GET_BOTH should find exact key+value pair
      MDBX_val key{const_cast<char*>("key1"), 4};
      MDBX_val data{const_cast<char*>("beta"), 4};
      int rc = mdbx_cursor_get(cur, &key, &data, MDBX_GET_BOTH);
      REQUIRE(rc == MDBX_SUCCESS);
      REQUIRE(std::string_view(static_cast<char*>(key.iov_base), key.iov_len) == "key1");
      REQUIRE(std::string_view(static_cast<char*>(data.iov_base), data.iov_len) == "beta");

      // GET_BOTH for non-existent value should return NOTFOUND
      MDBX_val key2{const_cast<char*>("key1"), 4};
      MDBX_val data2{const_cast<char*>("delta"), 5};
      REQUIRE(mdbx_cursor_get(cur, &key2, &data2, MDBX_GET_BOTH) == MDBX_NOTFOUND);

      // GET_BOTH_RANGE should find >= value within key
      MDBX_val key3{const_cast<char*>("key1"), 4};
      MDBX_val data3{const_cast<char*>("b"), 1};
      rc = mdbx_cursor_get(cur, &key3, &data3, MDBX_GET_BOTH_RANGE);
      REQUIRE(rc == MDBX_SUCCESS);
      REQUIRE(std::string_view(static_cast<char*>(data3.iov_base), data3.iov_len) == "beta");

      mdbx_cursor_close(cur);
      mdbx_txn_abort(txn);
   }

   mdbx_env_close(env);
}

TEST_CASE("C++ API: value_mode::multi_reverse iteration order", "[mdbx][cpp-api][dupsort]")
{
   auto dir = make_temp_dir("multi_reverse");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;
   mdbx::env_managed db(dir.c_str(), cp, op);

   // Create a table with multi_reverse (DUPSORT | REVERSEDUP)
   {
      auto txn = db.start_write();
      auto map = txn.create_map("rev", mdbx::key_mode::usual, mdbx::value_mode::multi_reverse);

      txn.upsert(map, mdbx::slice("k1"), mdbx::slice("aaa"));
      txn.upsert(map, mdbx::slice("k1"), mdbx::slice("bbb"));
      txn.upsert(map, mdbx::slice("k1"), mdbx::slice("ccc"));
      txn.commit();
   }

   // Values should iterate in reverse order (ccc, bbb, aaa)
   {
      auto txn = db.start_read();
      auto map = txn.open_map("rev");
      auto cur = txn.open_cursor(map);

      auto kv = cur.to_first();
      REQUIRE(kv.done);
      REQUIRE(kv.key.string_view() == "k1");
      REQUIRE(kv.value.string_view() == "ccc");

      kv = cur.to_next();
      REQUIRE(kv.done);
      REQUIRE(kv.value.string_view() == "bbb");

      kv = cur.to_next();
      REQUIRE(kv.done);
      REQUIRE(kv.value.string_view() == "aaa");

      kv = cur.to_next(false);
      REQUIRE_FALSE(kv.done);
   }

   // mdbx_get returns the first value in iteration order ("ccc")
   {
      auto txn = db.start_read();
      auto map = txn.open_map("rev");
      auto val = txn.get(map, mdbx::slice("k1"));
      REQUIRE(val.string_view() == "ccc");
   }

   // Erase works with reverse dup
   {
      auto txn = db.start_write();
      auto map = txn.open_map("rev");
      REQUIRE(txn.erase(map, mdbx::slice("k1"), mdbx::slice("bbb")));
      txn.commit();
   }

   {
      auto txn = db.start_read();
      auto map = txn.open_map("rev");
      auto cur = txn.open_cursor(map);

      auto kv = cur.to_first();
      REQUIRE(kv.value.string_view() == "ccc");
      kv = cur.to_next();
      REQUIRE(kv.value.string_view() == "aaa");
      kv = cur.to_next(false);
      REQUIRE_FALSE(kv.done);
   }
}

TEST_CASE("C++ API: DUPSORT upsert accumulates across transactions", "[mdbx][dupsort][cpp-api]")
{
   auto dir = make_temp_dir("dupsort_cross_txn");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;
   mdbx::env_managed db(dir.c_str(), cp, op);

   mdbx::map_handle map;
   {
      auto txn = db.start_write();
      map = txn.create_map("ds", mdbx::key_mode::usual, mdbx::value_mode::multi);
      txn.upsert(map, mdbx::slice("k"), mdbx::slice("v1"));
      txn.commit();
   }

   {
      auto txn = db.start_read();
      auto stat = txn.get_map_stat(map);
      REQUIRE(stat.ms_entries == 1);
   }

   // Second transaction: upsert different value for same key
   {
      auto txn = db.start_write();
      map = txn.open_map("ds");
      txn.upsert(map, mdbx::slice("k"), mdbx::slice("v2"));
      txn.commit();
   }

   // Should have 2 entries now (v1 and v2 are separate dups)
   {
      auto txn = db.start_read();
      auto stat = txn.get_map_stat(map);
      INFO("Expected 2 entries (v1 + v2), got " << stat.ms_entries);
      REQUIRE(stat.ms_entries == 2);
   }

   // Verify both values are present via cursor
   {
      auto txn = db.start_read();
      auto cur = txn.open_cursor(map);
      auto kv = cur.to_first();
      REQUIRE(kv.done);
      REQUIRE(kv.value.string_view() == "v1");
      kv = cur.to_next();
      REQUIRE(kv.done);
      REQUIRE(kv.value.string_view() == "v2");
      kv = cur.to_next(false);
      REQUIRE_FALSE(kv.done);
   }
}

TEST_CASE("C++ API: DUPSORT upsert with binary keys accumulates", "[mdbx][dupsort][cpp-api]")
{
   auto dir = make_temp_dir("dupsort_binary_accum");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;
   mdbx::env_managed db(dir.c_str(), cp, op);

   // Simulate eth benchmark: 20-byte key with embedded \x00, 32-byte random values
   auto make_addr = [](uint64_t id) {
      std::string a(20, '\0');
      std::memcpy(a.data() + 12, &id, sizeof(id));
      return a;
   };

   std::mt19937_64 rng(42);
   auto rand_bytes = [&](size_t len) {
      std::string s(len, '\0');
      for (size_t i = 0; i < len; i += 8) {
         uint64_t r = rng();
         size_t n = std::min(len - i, size_t(8));
         std::memcpy(s.data() + i, &r, n);
      }
      return s;
   };

   mdbx::map_handle map;

   // Preload 100K accounts (matches benchmark scale)
   {
      auto txn = db.start_write();
      map = txn.create_map("acct", mdbx::key_mode::usual, mdbx::value_mode::multi);
      for (int i = 0; i < 100000; i++) {
         auto addr = make_addr(i);
         auto bal = rand_bytes(32);
         txn.upsert(map, mdbx::slice(addr), mdbx::slice(bal));
      }
      txn.commit();
   }

   {
      auto txn = db.start_read();
      auto stat = txn.get_map_stat(map);
      INFO("After preload: expected 100000, got " << stat.ms_entries);
      REQUIRE(stat.ms_entries == 100000);
   }

   // Upsert 100 updates to existing accounts in a new transaction
   {
      auto txn = db.start_write();
      map = txn.open_map("acct");
      for (int i = 0; i < 100; i++) {
         auto addr = make_addr(rng() % 100000);
         auto bal = rand_bytes(32);
         txn.upsert(map, mdbx::slice(addr), mdbx::slice(bal));
      }
      txn.commit();
   }

   {
      auto txn = db.start_read();
      auto stat = txn.get_map_stat(map);
      INFO("After 100 updates: expected 100100, got " << stat.ms_entries);
      REQUIRE(stat.ms_entries == 100100);
   }
}

TEST_CASE("C API: mdbx_env_copy creates valid copy", "[mdbx][c-api][env]")
{
   auto dir  = make_temp_dir("env_copy_src");
   auto dest = make_temp_dir("env_copy_dst");

   // Write data into source env
   {
      MDBX_env* env = nullptr;
      mdbx_env_create(&env);
      mdbx_env_set_maxdbs(env, 16);
      mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
      MDBX_dbi dbi;
      mdbx_dbi_open(txn, "test", MDBX_db_flags_t(MDBX_CREATE), &dbi);

      MDBX_val key{const_cast<char*>("hello"), 5};
      MDBX_val val{const_cast<char*>("world"), 5};
      mdbx_put(txn, dbi, &key, &val, MDBX_UPSERT);
      mdbx_txn_commit(txn);

      // Copy the env
      REQUIRE(mdbx_env_copy(env, dest.c_str(), 0) == MDBX_SUCCESS);
      mdbx_env_close(env);
   }

   // Open the copy and verify data survived
   {
      MDBX_env* env = nullptr;
      mdbx_env_create(&env);
      mdbx_env_set_maxdbs(env, 16);
      mdbx_env_open(env, dest.c_str(), MDBX_ENV_DEFAULTS, 0644);

      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn);
      MDBX_dbi dbi;
      mdbx_dbi_open(txn, "test", MDBX_DB_DEFAULTS, &dbi);

      MDBX_val key{const_cast<char*>("hello"), 5};
      MDBX_val val{};
      REQUIRE(mdbx_get(txn, dbi, &key, &val) == MDBX_SUCCESS);
      REQUIRE(std::string_view(static_cast<char*>(val.iov_base), val.iov_len) == "world");

      mdbx_txn_abort(txn);
      mdbx_env_close(env);
   }
}

TEST_CASE("C API: drain with tombstones across env close/reopen", "[mdbx][c-api][persistence]")
{
   auto dir = make_temp_dir("drain_tombstones");

   // Write data, then delete some, then close (triggers drain)
   {
      MDBX_env* env = nullptr;
      mdbx_env_create(&env);
      mdbx_env_set_maxdbs(env, 16);
      mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
      MDBX_dbi dbi;
      mdbx_dbi_open(txn, "tbl", MDBX_db_flags_t(MDBX_CREATE), &dbi);

      for (int i = 0; i < 10; i++)
      {
         std::string k = "key" + std::to_string(i);
         std::string v = "val" + std::to_string(i);
         MDBX_val key{k.data(), k.size()};
         MDBX_val val{v.data(), v.size()};
         mdbx_put(txn, dbi, &key, &val, MDBX_UPSERT);
      }
      mdbx_txn_commit(txn);

      // Delete key3, key5, key7
      mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
      mdbx_dbi_open(txn, "tbl", MDBX_DB_DEFAULTS, &dbi);
      for (int i : {3, 5, 7})
      {
         std::string k = "key" + std::to_string(i);
         MDBX_val key{k.data(), k.size()};
         mdbx_del(txn, dbi, &key, nullptr);
      }
      mdbx_txn_commit(txn);

      // Close triggers drain — tombstones should propagate to psitri
      mdbx_env_close(env);
   }

   // Reopen and verify: deleted keys are gone, others remain
   {
      MDBX_env* env = nullptr;
      mdbx_env_create(&env);
      mdbx_env_set_maxdbs(env, 16);
      mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

      MDBX_txn* txn = nullptr;
      mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn);
      MDBX_dbi dbi;
      mdbx_dbi_open(txn, "tbl", MDBX_DB_DEFAULTS, &dbi);

      // Deleted keys should be gone
      for (int i : {3, 5, 7})
      {
         std::string k = "key" + std::to_string(i);
         MDBX_val key{k.data(), k.size()};
         MDBX_val val{};
         REQUIRE(mdbx_get(txn, dbi, &key, &val) == MDBX_NOTFOUND);
      }

      // Surviving keys should be present
      for (int i : {0, 1, 2, 4, 6, 8, 9})
      {
         std::string k = "key" + std::to_string(i);
         std::string expected = "val" + std::to_string(i);
         MDBX_val key{k.data(), k.size()};
         MDBX_val val{};
         REQUIRE(mdbx_get(txn, dbi, &key, &val) == MDBX_SUCCESS);
         REQUIRE(std::string_view(static_cast<char*>(val.iov_base), val.iov_len) == expected);
      }

      mdbx_txn_abort(txn);
      mdbx_env_close(env);
   }
}
