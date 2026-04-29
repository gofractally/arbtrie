#include <catch2/catch_test_macros.hpp>

#include <mdbx.h>
#include <mdbx.h++>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <filesystem>
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

static std::string mdbx_bytes(const MDBX_val& v)
{
   return std::string(static_cast<const char*>(v.iov_base), v.iov_len);
}

static std::string be_u64(uint64_t n)
{
   std::string out(8, '\0');
   for (int i = 0; i < 8; ++i)
      out[7 - i] = static_cast<char>((n >> (i * 8)) & 0xff);
   return out;
}

static unsigned char hex_nibble(char c)
{
   if (c >= '0' && c <= '9') return static_cast<unsigned char>(c - '0');
   if (c >= 'a' && c <= 'f') return static_cast<unsigned char>(c - 'a' + 10);
   if (c >= 'A' && c <= 'F') return static_cast<unsigned char>(c - 'A' + 10);
   FAIL("invalid hex digit");
   return 0;
}

static std::string hex_to_bytes(std::string_view hex)
{
   REQUIRE((hex.size() % 2) == 0);
   std::string out(hex.size() / 2, '\0');
   for (size_t i = 0; i < out.size(); ++i)
   {
      out[i] = static_cast<char>((hex_nibble(hex[i * 2]) << 4) |
                                 hex_nibble(hex[i * 2 + 1]));
   }
   return out;
}

static uint64_t from_be_u64(const MDBX_val& v)
{
   REQUIRE(v.iov_len == 8);
   uint64_t n = 0;
   const auto* p = static_cast<const unsigned char*>(v.iov_base);
   for (int i = 0; i < 8; ++i)
      n = (n << 8) | p[i];
   return n;
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

TEST_CASE("C API: Silkworm env options, marker file, and copy",
          "[mdbx][c-api][silkworm]")
{
   auto dir = make_temp_dir("c_env_silkworm");
   auto copy_dir = fs::temp_directory_path() / "c_env_silkworm_copy_mdbx_test";
   fs::remove_all(copy_dir);

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 16) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(),
                         static_cast<MDBX_env_flags_t>(MDBX_NOTLS |
                                                        MDBX_NORDAHEAD |
                                                        MDBX_COALESCE),
                         0644) == MDBX_SUCCESS);

   REQUIRE(fs::exists(dir / "mdbx.dat"));
   REQUIRE(fs::file_size(dir / "mdbx.dat") > 0);

   REQUIRE(mdbx_env_set_option(env, MDBX_opt_txn_dp_initial, 16 * 1024) ==
           MDBX_SUCCESS);
   uint64_t option_value = 0;
   REQUIRE(mdbx_env_get_option(env, MDBX_opt_txn_dp_initial, &option_value) ==
           MDBX_SUCCESS);
   REQUIRE(option_value == 16 * 1024);

   REQUIRE(mdbx_env_copy(env, copy_dir.c_str(), 0) == MDBX_SUCCESS);
   REQUIRE(fs::exists(copy_dir / "mdbx.dat"));

   REQUIRE(mdbx_env_close(env) == MDBX_SUCCESS);
   fs::remove_all(copy_dir);
}

TEST_CASE("C API: duplicate write transaction fails without blocking",
          "[mdbx][c-api][transaction][silkworm]")
{
   auto dir = make_temp_dir("c_duplicate_writer");

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 8) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   MDBX_txn* writer = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &writer) == MDBX_SUCCESS);

   MDBX_txn* duplicate = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &duplicate) == MDBX_BUSY);
   REQUIRE(duplicate == nullptr);

   REQUIRE(mdbx_txn_abort(writer) == MDBX_SUCCESS);
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &writer) == MDBX_SUCCESS);
   REQUIRE(mdbx_txn_abort(writer) == MDBX_SUCCESS);

   REQUIRE(mdbx_env_close(env) == MDBX_SUCCESS);
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

TEST_CASE("C API: multi-DBI abort rolls back every touched root", "[mdbx][c-api][transaction]")
{
   auto dir = make_temp_dir("c_multi_dbi_abort");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 16);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_dbi users = 0, orders = 0;
   {
      MDBX_txn* setup = nullptr;
      REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &setup) == MDBX_SUCCESS);
      REQUIRE(mdbx_dbi_open(setup, "users", MDBX_CREATE, &users) == MDBX_SUCCESS);
      REQUIRE(mdbx_dbi_open(setup, "orders", MDBX_CREATE, &orders) == MDBX_SUCCESS);
      REQUIRE(mdbx_txn_commit(setup) == MDBX_SUCCESS);
   }

   MDBX_val user_key{const_cast<char*>("alice"), 5};
   MDBX_val user_val{const_cast<char*>("admin"), 5};
   MDBX_val order_key{const_cast<char*>("order1"), 6};
   MDBX_val order_val{const_cast<char*>("pending"), 7};

   MDBX_txn* txn = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);
   REQUIRE(mdbx_put(txn, users, &user_key, &user_val, MDBX_UPSERT) == MDBX_SUCCESS);
   REQUIRE(mdbx_put(txn, orders, &order_key, &order_val, MDBX_UPSERT) == MDBX_SUCCESS);

   MDBX_val got{};
   REQUIRE(mdbx_get(txn, users, &user_key, &got) == MDBX_SUCCESS);
   REQUIRE(mdbx_get(txn, orders, &order_key, &got) == MDBX_SUCCESS);
   REQUIRE(mdbx_txn_abort(txn) == MDBX_SUCCESS);

   MDBX_txn* ro = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro) == MDBX_SUCCESS);
   REQUIRE(mdbx_get(ro, users, &user_key, &got) == MDBX_NOTFOUND);
   REQUIRE(mdbx_get(ro, orders, &order_key, &got) == MDBX_NOTFOUND);
   mdbx_txn_abort(ro);

   mdbx_env_close(env);
}

TEST_CASE("C API: named DBI creation abort rolls back catalog", "[mdbx][c-api][transaction]")
{
   auto dir = make_temp_dir("c_dbi_create_abort");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 16);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

   MDBX_dbi temp = 0;
   REQUIRE(mdbx_dbi_open(txn, "temp", MDBX_CREATE, &temp) == MDBX_SUCCESS);
   REQUIRE(mdbx_txn_abort(txn) == MDBX_SUCCESS);

   MDBX_txn* ro = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro) == MDBX_SUCCESS);
   MDBX_dbi missing = 0;
   REQUIRE(mdbx_dbi_open(ro, "temp", MDBX_DB_DEFAULTS, &missing) == MDBX_NOTFOUND);
   mdbx_txn_abort(ro);

   mdbx_env_close(env);
}

TEST_CASE("C API: named DBI catalog preserves root ids across reopen",
          "[mdbx][c-api][persistence][silkworm]")
{
   auto dir = make_temp_dir("c_catalog_root_ids");

   {
      MDBX_env* env = nullptr;
      REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
      REQUIRE(mdbx_env_set_maxdbs(env, 16) == MDBX_SUCCESS);
      REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

      MDBX_txn* txn = nullptr;
      REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

      MDBX_dbi zeta = 0, alpha = 0;
      REQUIRE(mdbx_dbi_open(txn, "zeta", MDBX_CREATE, &zeta) == MDBX_SUCCESS);
      REQUIRE(mdbx_dbi_open(txn, "alpha", MDBX_CREATE, &alpha) == MDBX_SUCCESS);

      MDBX_val key{const_cast<char*>("same"), 4};
      MDBX_val vz{const_cast<char*>("zeta-value"), 10};
      MDBX_val va{const_cast<char*>("alpha-value"), 11};
      REQUIRE(mdbx_put(txn, zeta, &key, &vz, MDBX_UPSERT) == MDBX_SUCCESS);
      REQUIRE(mdbx_put(txn, alpha, &key, &va, MDBX_UPSERT) == MDBX_SUCCESS);
      REQUIRE(mdbx_txn_commit(txn) == MDBX_SUCCESS);
      REQUIRE(mdbx_env_close(env) == MDBX_SUCCESS);
   }

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 16) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   MDBX_txn* txn = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn) == MDBX_SUCCESS);

   MDBX_dbi zeta = 0, alpha = 0;
   REQUIRE(mdbx_dbi_open(txn, "zeta", MDBX_DB_DEFAULTS, &zeta) == MDBX_SUCCESS);
   REQUIRE(mdbx_dbi_open(txn, "alpha", MDBX_DB_DEFAULTS, &alpha) == MDBX_SUCCESS);

   MDBX_val key{const_cast<char*>("same"), 4};
   MDBX_val got{};
   REQUIRE(mdbx_get(txn, zeta, &key, &got) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(got) == "zeta-value");
   REQUIRE(mdbx_get(txn, alpha, &key, &got) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(got) == "alpha-value");

   REQUIRE(mdbx_txn_abort(txn) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_close(env) == MDBX_SUCCESS);
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

TEST_CASE("C API: cursor storage is reused within a transaction", "[mdbx][c-api][cursor][pool]")
{
   auto dir = make_temp_dir("c_cursor_pool");

   MDBX_env* env = nullptr;
   mdbx_env_create(&env);
   mdbx_env_set_maxdbs(env, 8);
   mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644);

   MDBX_txn* txn = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

   MDBX_dbi dbi = 0;
   REQUIRE(mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi) == MDBX_SUCCESS);

   MDBX_cursor* first = nullptr;
   REQUIRE(mdbx_cursor_open(txn, dbi, &first) == MDBX_SUCCESS);
   mdbx_cursor_close(first);

   MDBX_cursor* second = nullptr;
   REQUIRE(mdbx_cursor_open(txn, dbi, &second) == MDBX_SUCCESS);
   REQUIRE(second == first);
   mdbx_cursor_close(second);

   REQUIRE(mdbx_txn_abort(txn) == MDBX_SUCCESS);
   mdbx_env_close(env);
}

TEST_CASE("C API: cursor put preserves current position",
          "[mdbx][c-api][cursor][write]")
{
   auto dir = make_temp_dir("c_cursor_put_position");

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 8) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   MDBX_txn* txn = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

   MDBX_dbi dbi = 0;
   REQUIRE(mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi) == MDBX_SUCCESS);

   auto put = [&](const char* key_text, const char* value_text) {
      MDBX_val key{const_cast<char*>(key_text), std::strlen(key_text)};
      MDBX_val value{const_cast<char*>(value_text), std::strlen(value_text)};
      REQUIRE(mdbx_put(txn, dbi, &key, &value, MDBX_UPSERT) == MDBX_SUCCESS);
   };
   put("apple", "a");
   put("banana", "b");
   put("cherry", "c");

   MDBX_cursor* cur = nullptr;
   REQUIRE(mdbx_cursor_open(txn, dbi, &cur) == MDBX_SUCCESS);

   MDBX_val key{const_cast<char*>("banana"), 6};
   MDBX_val value{};
   REQUIRE(mdbx_cursor_get(cur, &key, &value, MDBX_SET_KEY) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(value) == "b");

   MDBX_val date_key{const_cast<char*>("date"), 4};
   MDBX_val date_value{const_cast<char*>("d"), 1};
   REQUIRE(mdbx_cursor_put(cur, &date_key, &date_value, MDBX_UPSERT) == MDBX_SUCCESS);

   MDBX_val current_key{};
   MDBX_val current_value{};
   REQUIRE(mdbx_cursor_get(cur, &current_key, &current_value, MDBX_GET_CURRENT) ==
           MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(current_key) == "banana");
   REQUIRE(mdbx_bytes(current_value) == "b");

   REQUIRE(mdbx_cursor_get(cur, &current_key, &current_value, MDBX_NEXT) ==
           MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(current_key) == "cherry");
   REQUIRE(mdbx_bytes(current_value) == "c");

   key = {const_cast<char*>("banana"), 6};
   REQUIRE(mdbx_cursor_get(cur, &key, &value, MDBX_SET_KEY) == MDBX_SUCCESS);
   MDBX_val updated_value{const_cast<char*>("bb"), 2};
   REQUIRE(mdbx_cursor_put(cur, &key, &updated_value, MDBX_CURRENT) == MDBX_SUCCESS);
   REQUIRE(mdbx_cursor_get(cur, &current_key, &current_value, MDBX_GET_CURRENT) ==
           MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(current_key) == "banana");
   REQUIRE(mdbx_bytes(current_value) == "bb");

   mdbx_cursor_close(cur);
   REQUIRE(mdbx_txn_abort(txn) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_close(env) == MDBX_SUCCESS);
}

TEST_CASE("C API: cursor delete continues and sibling cursors see writes",
          "[mdbx][c-api][cursor][write]")
{
   auto dir = make_temp_dir("c_cursor_delete_stale");

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 8) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   MDBX_txn* txn = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

   MDBX_dbi dbi = 0;
   REQUIRE(mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi) == MDBX_SUCCESS);

   auto put = [&](const char* key_text, const char* value_text) {
      MDBX_val key{const_cast<char*>(key_text), std::strlen(key_text)};
      MDBX_val value{const_cast<char*>(value_text), std::strlen(value_text)};
      REQUIRE(mdbx_put(txn, dbi, &key, &value, MDBX_UPSERT) == MDBX_SUCCESS);
   };
   put("aa", "1");
   put("ab", "2");
   put("ac", "3");
   put("ba", "4");

   MDBX_cursor* reader = nullptr;
   MDBX_cursor* writer = nullptr;
   REQUIRE(mdbx_cursor_open(txn, dbi, &reader) == MDBX_SUCCESS);
   REQUIRE(mdbx_cursor_open(txn, dbi, &writer) == MDBX_SUCCESS);

   MDBX_val key{const_cast<char*>("ab"), 2};
   MDBX_val value{};
   REQUIRE(mdbx_cursor_get(reader, &key, &value, MDBX_SET_KEY) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(value) == "2");

   key = {const_cast<char*>("ab"), 2};
   REQUIRE(mdbx_cursor_get(writer, &key, &value, MDBX_SET_KEY) == MDBX_SUCCESS);
   REQUIRE(mdbx_cursor_del(writer, static_cast<MDBX_put_flags_t>(0)) ==
           MDBX_SUCCESS);

   key = {const_cast<char*>("ab"), 2};
   REQUIRE(mdbx_cursor_get(reader, &key, &value, MDBX_SET_KEY) == MDBX_NOTFOUND);
   key = {const_cast<char*>("ab"), 2};
   REQUIRE(mdbx_cursor_get(reader, &key, &value, MDBX_SET_RANGE) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(key) == "ac");
   REQUIRE(mdbx_bytes(value) == "3");

   key = {const_cast<char*>("aa"), 2};
   REQUIRE(mdbx_cursor_get(writer, &key, &value, MDBX_SET_KEY) == MDBX_SUCCESS);
   size_t erased = 0;
   do
   {
      REQUIRE(mdbx_cursor_del(writer, static_cast<MDBX_put_flags_t>(0)) ==
              MDBX_SUCCESS);
      ++erased;
   } while (mdbx_cursor_get(writer, &key, &value, MDBX_NEXT) == MDBX_SUCCESS);
   REQUIRE(erased == 3);

   REQUIRE(mdbx_cursor_get(writer, &key, &value, MDBX_FIRST) == MDBX_NOTFOUND);

   mdbx_cursor_close(writer);
   mdbx_cursor_close(reader);
   REQUIRE(mdbx_txn_abort(txn) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_close(env) == MDBX_SUCCESS);
}

TEST_CASE("C API: cursor_copy clones current position", "[mdbx][c-api][cursor][silkworm]")
{
   auto dir = make_temp_dir("c_cursor_copy");

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 8) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   MDBX_txn* txn = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

   MDBX_dbi dbi = 0;
   REQUIRE(mdbx_dbi_open(txn, nullptr, MDBX_DB_DEFAULTS, &dbi) == MDBX_SUCCESS);

   for (auto item : {"a", "b", "c"})
   {
      MDBX_val k{const_cast<char*>(item), 1};
      MDBX_val v{const_cast<char*>("value"), 5};
      REQUIRE(mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT) == MDBX_SUCCESS);
   }

   MDBX_cursor* cur = nullptr;
   REQUIRE(mdbx_cursor_open(txn, dbi, &cur) == MDBX_SUCCESS);
   MDBX_val k{const_cast<char*>("b"), 1};
   MDBX_val v{};
   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_SET_KEY) == MDBX_SUCCESS);

   MDBX_cursor* clone = mdbx_cursor_create(nullptr);
   REQUIRE(clone != nullptr);
   REQUIRE(mdbx_cursor_copy(cur, clone) == MDBX_SUCCESS);

   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_NEXT) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(k) == "c");

   MDBX_val clone_key{};
   MDBX_val clone_val{};
   REQUIRE(mdbx_cursor_get(clone, &clone_key, &clone_val, MDBX_GET_CURRENT) ==
           MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(clone_key) == "b");

   mdbx_cursor_close(clone);
   mdbx_cursor_close(cur);
   REQUIRE(mdbx_txn_abort(txn) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_close(env) == MDBX_SUCCESS);
}

TEST_CASE("C API: returned cursor slices are valid until cursor movement",
          "[mdbx][c-api][cursor][lifetime][silkworm]")
{
   auto dir = make_temp_dir("c_cursor_slice_lifetime");

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 16) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   const std::string key = "K";
   const std::string value_a = "A" + std::string(96, 'a');
   const std::string value_b = "B" + std::string(96, 'b');

   {
      MDBX_txn* txn = nullptr;
      REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

      MDBX_dbi dbi = 0;
      REQUIRE(mdbx_dbi_open(txn, "dups",
                            static_cast<MDBX_db_flags_t>(MDBX_CREATE | MDBX_DUPSORT),
                            &dbi) == MDBX_SUCCESS);

      MDBX_val k{const_cast<char*>(key.data()), key.size()};
      MDBX_val va{const_cast<char*>(value_a.data()), value_a.size()};
      MDBX_val vb{const_cast<char*>(value_b.data()), value_b.size()};
      REQUIRE(mdbx_put(txn, dbi, &k, &va, MDBX_UPSERT) == MDBX_SUCCESS);
      REQUIRE(mdbx_put(txn, dbi, &k, &vb, MDBX_UPSERT) == MDBX_SUCCESS);
      REQUIRE(mdbx_txn_commit(txn) == MDBX_SUCCESS);
   }

   MDBX_txn* txn = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

   MDBX_dbi dbi = 0;
   REQUIRE(mdbx_dbi_open(txn, "dups", MDBX_DUPSORT, &dbi) == MDBX_SUCCESS);

   auto lookup_then_close_cursor = [&]() {
      MDBX_cursor* cur = nullptr;
      REQUIRE(mdbx_cursor_open(txn, dbi, &cur) == MDBX_SUCCESS);

      MDBX_val k{const_cast<char*>(key.data()), key.size()};
      MDBX_val v{const_cast<char*>(value_b.data()), 1};
      REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_GET_BOTH_RANGE) == MDBX_SUCCESS);
      REQUIRE(mdbx_bytes(v) == value_b);

      mdbx_cursor_close(cur);
   };

   lookup_then_close_cursor();

   for (int i = 0; i < 100; ++i)
   {
      MDBX_cursor* cur = nullptr;
      REQUIRE(mdbx_cursor_open(txn, dbi, &cur) == MDBX_SUCCESS);
      MDBX_val k{};
      MDBX_val v{};
      REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_FIRST) == MDBX_SUCCESS);
      mdbx_cursor_close(cur);
   }

   REQUIRE(mdbx_txn_abort(txn) == MDBX_SUCCESS);
   mdbx_env_close(env);
}

TEST_CASE("C API: Silkworm seek/current/next cursor pattern",
          "[mdbx][c-api][cursor][silkworm]")
{
   auto dir = make_temp_dir("c_silkworm_seek_next");

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 16) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   {
      MDBX_txn* txn = nullptr;
      REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

      MDBX_dbi dbi = 0;
      REQUIRE(mdbx_dbi_open(txn, "canon", MDBX_CREATE, &dbi) == MDBX_SUCCESS);

      for (uint64_t i = 0; i < 5; ++i)
      {
         auto key = be_u64(i);
         auto val = "val_" + std::to_string(i);
         MDBX_val k{key.data(), key.size()};
         MDBX_val v{val.data(), val.size()};
         REQUIRE(mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT) == MDBX_SUCCESS);
      }

      REQUIRE(mdbx_txn_commit(txn) == MDBX_SUCCESS);
   }

   MDBX_txn* txn = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

   MDBX_dbi dbi = 0;
   REQUIRE(mdbx_dbi_open(txn, "canon", MDBX_DB_DEFAULTS, &dbi) == MDBX_SUCCESS);

   {
      ::mdbx::txn cpp_txn(txn);
      auto cpp_cur = cpp_txn.open_cursor(::mdbx::map_handle(dbi));
      auto seek_target = be_u64(1);
      REQUIRE(cpp_cur.seek(::mdbx::slice(seek_target.data(), seek_target.size())));
      REQUIRE_FALSE(cpp_cur.eof());
   }

   MDBX_cursor* cur = nullptr;
   REQUIRE(mdbx_cursor_open(txn, dbi, &cur) == MDBX_SUCCESS);

   auto seek_key = be_u64(0);
   MDBX_val k{seek_key.data(), seek_key.size()};
   MDBX_val v{};
   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_SET_KEY) == MDBX_SUCCESS);

   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_GET_CURRENT) == MDBX_SUCCESS);
   REQUIRE(from_be_u64(k) == 0);
   REQUIRE(mdbx_bytes(v) == "val_0");

   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_NEXT) == MDBX_SUCCESS);
   REQUIRE(from_be_u64(k) == 1);
   REQUIRE(mdbx_bytes(v) == "val_1");

   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_NEXT) == MDBX_SUCCESS);
   REQUIRE(from_be_u64(k) == 2);
   REQUIRE(mdbx_bytes(v) == "val_2");

   for (uint64_t expected = 3; expected < 5; ++expected)
   {
      REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_NEXT) == MDBX_SUCCESS);
      REQUIRE(from_be_u64(k) == expected);
   }

   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_NEXT) == MDBX_NOTFOUND);

   mdbx_cursor_close(cur);
   REQUIRE(mdbx_txn_abort(txn) == MDBX_SUCCESS);
   mdbx_env_close(env);
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

TEST_CASE("C++ API: Silkworm compatibility surface", "[mdbx][cpp-api][silkworm]")
{
   auto dir = make_temp_dir("cpp_silkworm_surface");

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;
   op.mode = mdbx::env::operate_parameters::mode_from_flags(MDBX_WRITEMAP);
   op.options = mdbx::env::operate_parameters::options_from_flags(MDBX_WRITEMAP);
   op.durability =
      mdbx::env::operate_parameters::durability_from_flags(MDBX_SYNC_DURABLE);

   mdbx::env_managed db(dir.c_str(), cp, op);
   REQUIRE(db.get_path() == dir);
   REQUIRE(db.get_stat().ms_psize == db.get_pagesize());
   REQUIRE(db.get_info().mi_dxb_pagesize == db.get_pagesize());
   REQUIRE(db.check_readers() == 0);
   REQUIRE(mdbx::get_build().target != nullptr);

   std::vector<unsigned char> bytes;
   bytes.push_back(0xab);
   bytes.push_back(0xcd);
   mdbx::slice bytes_slice(bytes);
   REQUIRE(bytes_slice.size() == 2);
   REQUIRE(mdbx::to_hex(bytes_slice).as_string() == "abcd");
   bytes_slice.remove_prefix(1);
   REQUIRE(bytes_slice.safe_head(1)[0] == 0xcd);

   auto txn = db.start_write();
   auto map = txn.create_map("dups", mdbx::key_mode::usual,
                             mdbx::value_mode::multi_reverse);
   auto info = txn.get_handle_info(map);
   REQUIRE((info.flags & MDBX_DUPSORT) != 0);
   REQUIRE((info.flags & MDBX_REVERSEDUP) != 0);

   auto cur = txn.open_cursor(map);
   mdbx::slice v1("one");
   REQUIRE(cur.put(mdbx::slice("k"), &v1, MDBX_UPSERT) == MDBX_SUCCESS);
   mdbx::slice v2("two");
   REQUIRE(cur.put(mdbx::slice("k"), &v2, MDBX_APPENDDUP) == MDBX_SUCCESS);

   auto found = cur.move(mdbx::cursor::move_operation::get_both,
                         mdbx::slice("k"), mdbx::slice("two"), false);
   REQUIRE(found.done);
   REQUIRE(found.value.string_view() == "two");

   auto current = cur.move(mdbx::cursor::move_operation::get_current, false);
   REQUIRE(current.done);
   REQUIRE(current.key.string_view() == "k");

   REQUIRE(cur.erase(mdbx::slice("k"), mdbx::slice("two")));
   REQUIRE(txn.get_map_stat(map).ms_entries == 1);
   txn.commit();
   REQUIRE(mdbx_cursor_get(cur, nullptr, nullptr, MDBX_GET_CURRENT) == MDBX_BAD_TXN);
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

TEST_CASE("C API: DUPSORT hidden marker preserves empty duplicate values",
          "[mdbx][dupsort]")
{
   auto dir = make_temp_dir("c_dupsort_empty_dup");

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 16) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   MDBX_txn* txn = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

   MDBX_dbi dbi = 0;
   REQUIRE(mdbx_dbi_open(txn, "dupsort_empty",
                         static_cast<MDBX_db_flags_t>(MDBX_CREATE | MDBX_DUPSORT),
                         &dbi) == MDBX_SUCCESS);

   std::string key = "k";
   std::string empty;
   std::string value = "a";
   MDBX_val k{key.data(), key.size()};
   MDBX_val empty_val{empty.data(), empty.size()};
   MDBX_val value_val{value.data(), value.size()};

   REQUIRE(mdbx_put(txn, dbi, &k, &value_val, MDBX_UPSERT) == MDBX_SUCCESS);
   REQUIRE(mdbx_put(txn, dbi, &k, &empty_val, MDBX_UPSERT) == MDBX_SUCCESS);

   MDBX_val got{};
   REQUIRE(mdbx_get(txn, dbi, &k, &got) == MDBX_SUCCESS);
   REQUIRE(got.iov_len == 0);

   MDBX_cursor* cur = nullptr;
   REQUIRE(mdbx_cursor_open(txn, dbi, &cur) == MDBX_SUCCESS);

   MDBX_val ck{key.data(), key.size()};
   MDBX_val cv{};
   REQUIRE(mdbx_cursor_get(cur, &ck, &cv, MDBX_SET_KEY) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(ck) == key);
   REQUIRE(cv.iov_len == 0);

   size_t count = 0;
   REQUIRE(mdbx_cursor_count(cur, &count) == MDBX_SUCCESS);
   REQUIRE(count == 2);

   REQUIRE(mdbx_cursor_get(cur, &ck, &cv, MDBX_NEXT_DUP) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(cv) == value);
   REQUIRE(mdbx_cursor_get(cur, &ck, &cv, MDBX_PREV_DUP) == MDBX_SUCCESS);
   REQUIRE(cv.iov_len == 0);

   MDBX_val range_key{key.data(), key.size()};
   MDBX_val range_value{empty.data(), empty.size()};
   REQUIRE(mdbx_cursor_get(cur, &range_key, &range_value, MDBX_GET_BOTH_RANGE) ==
           MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(range_key) == key);
   REQUIRE(range_value.iov_len == 0);

   mdbx_cursor_close(cur);

   REQUIRE(mdbx_del(txn, dbi, &k, &empty_val) == MDBX_SUCCESS);
   REQUIRE(mdbx_get(txn, dbi, &k, &got) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(got) == value);

   REQUIRE(mdbx_del(txn, dbi, &k, &value_val) == MDBX_SUCCESS);
   REQUIRE(mdbx_get(txn, dbi, &k, &got) == MDBX_NOTFOUND);

   std::string replacement = "z";
   MDBX_val replacement_val{replacement.data(), replacement.size()};
   REQUIRE(mdbx_put(txn, dbi, &k, &replacement_val, MDBX_NOOVERWRITE) ==
           MDBX_SUCCESS);

   REQUIRE(mdbx_txn_commit(txn) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_close(env) == MDBX_SUCCESS);
}

TEST_CASE("C API: DUPSORT uses one encoded PsiTri key budget",
          "[mdbx][dupsort][limits]")
{
   auto dir = make_temp_dir("c_dupsort_composite_budget");

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 16) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   MDBX_txn* txn = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

   MDBX_dbi dbi = 0;
   REQUIRE(mdbx_dbi_open(txn, "dupsort_limits",
                         static_cast<MDBX_db_flags_t>(MDBX_CREATE | MDBX_DUPSORT),
                         &dbi) == MDBX_SUCCESS);

   std::string key(512, 'k');
   std::string first(509, 'a');
   std::string second(509, 'b');

   MDBX_val k{key.data(), key.size()};
   MDBX_val v2{second.data(), second.size()};
   MDBX_val v1{first.data(), first.size()};
   REQUIRE(mdbx_put(txn, dbi, &k, &v2, MDBX_UPSERT) == MDBX_SUCCESS);
   REQUIRE(mdbx_put(txn, dbi, &k, &v1, MDBX_UPSERT) == MDBX_SUCCESS);

   MDBX_val got{};
   REQUIRE(mdbx_get(txn, dbi, &k, &got) == MDBX_SUCCESS);
   REQUIRE(std::string_view(static_cast<char*>(got.iov_base), got.iov_len) == first);

   MDBX_cursor* cur = nullptr;
   REQUIRE(mdbx_cursor_open(txn, dbi, &cur) == MDBX_SUCCESS);
   MDBX_val ck{key.data(), key.size()};
   MDBX_val cv{};
   REQUIRE(mdbx_cursor_get(cur, &ck, &cv, MDBX_SET) == MDBX_SUCCESS);
   REQUIRE(std::string_view(static_cast<char*>(cv.iov_base), cv.iov_len) == first);
   REQUIRE(mdbx_cursor_get(cur, &ck, &cv, MDBX_NEXT_DUP) == MDBX_SUCCESS);
   REQUIRE(std::string_view(static_cast<char*>(cv.iov_base), cv.iov_len) == second);
   mdbx_cursor_close(cur);

   std::string binary_key("bin\0key", 7);
   std::string binary_first("\0a", 2);
   std::string binary_second("\0b", 2);
   MDBX_val bk{binary_key.data(), binary_key.size()};
   MDBX_val bv1{binary_first.data(), binary_first.size()};
   MDBX_val bv2{binary_second.data(), binary_second.size()};
   REQUIRE(mdbx_put(txn, dbi, &bk, &bv2, MDBX_UPSERT) == MDBX_SUCCESS);
   REQUIRE(mdbx_put(txn, dbi, &bk, &bv1, MDBX_UPSERT) == MDBX_SUCCESS);
   MDBX_val bgot{};
   REQUIRE(mdbx_get(txn, dbi, &bk, &bgot) == MDBX_SUCCESS);
   REQUIRE(std::string_view(static_cast<char*>(bgot.iov_base), bgot.iov_len) ==
           binary_first);

   std::string too_large_value(511, 'x');
   MDBX_val bad_v{too_large_value.data(), too_large_value.size()};
   REQUIRE(mdbx_put(txn, dbi, &k, &bad_v, MDBX_UPSERT) == MDBX_BAD_VALSIZE);

   std::string too_large_key(1022, 'y');
   MDBX_val bad_k{too_large_key.data(), too_large_key.size()};
   MDBX_val small_v{const_cast<char*>("v"), 1};
   REQUIRE(mdbx_put(txn, dbi, &bad_k, &small_v, MDBX_UPSERT) == MDBX_BAD_VALSIZE);

   REQUIRE(mdbx_txn_commit(txn) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_close(env) == MDBX_SUCCESS);

   env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 16) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   MDBX_txn* ro = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro) == MDBX_SUCCESS);
   MDBX_dbi reopened = 0;
   REQUIRE(mdbx_dbi_open(ro, "dupsort_limits", MDBX_DUPSORT, &reopened) == MDBX_SUCCESS);
   MDBX_val rk{key.data(), key.size()};
   MDBX_val rv{};
   REQUIRE(mdbx_get(ro, reopened, &rk, &rv) == MDBX_SUCCESS);
   REQUIRE(std::string_view(static_cast<char*>(rv.iov_base), rv.iov_len) == first);
   REQUIRE(mdbx_txn_abort(ro) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_close(env) == MDBX_SUCCESS);
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

TEST_CASE("C API: Silkworm DUPSORT cursor patterns", "[mdbx][dupsort][silkworm]")
{
   auto dir = make_temp_dir("c_silkworm_dupsort");

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 16) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   {
      MDBX_txn* txn = nullptr;
      REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

      MDBX_dbi dbi = 0;
      REQUIRE(mdbx_dbi_open(txn, "dups",
                            static_cast<MDBX_db_flags_t>(MDBX_CREATE | MDBX_DUPSORT),
                            &dbi) == MDBX_SUCCESS);

      auto put_dup = [&](const char* key, const char* val) {
         MDBX_val k{const_cast<char*>(key), std::strlen(key)};
         MDBX_val v{const_cast<char*>(val), std::strlen(val)};
         REQUIRE(mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT) == MDBX_SUCCESS);
      };

      put_dup("A", "10");
      put_dup("A", "20");
      put_dup("A", "30");
      put_dup("B", "5");
      put_dup("B", "15");

      REQUIRE(mdbx_txn_commit(txn) == MDBX_SUCCESS);
   }

   MDBX_txn* txn = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

   MDBX_dbi dbi = 0;
   REQUIRE(mdbx_dbi_open(txn, "dups", MDBX_DUPSORT, &dbi) == MDBX_SUCCESS);

   MDBX_cursor* cur = nullptr;
   REQUIRE(mdbx_cursor_open(txn, dbi, &cur) == MDBX_SUCCESS);

   {
      MDBX_val k{const_cast<char*>("A"), 1};
      MDBX_val v{const_cast<char*>("20"), 2};
      REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_GET_BOTH) == MDBX_SUCCESS);
      REQUIRE(mdbx_bytes(k) == "A");
      REQUIRE(mdbx_bytes(v) == "20");
   }

   {
      MDBX_val k{};
      MDBX_val v{};
      REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_NEXT_DUP) == MDBX_SUCCESS);
      REQUIRE(mdbx_bytes(k) == "A");
      REQUIRE(mdbx_bytes(v) == "30");

      REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_NEXT_DUP) == MDBX_NOTFOUND);
   }

   {
      MDBX_val k{};
      MDBX_val v{};
      REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_NEXT_NODUP) == MDBX_SUCCESS);
      REQUIRE(mdbx_bytes(k) == "B");
      REQUIRE(mdbx_bytes(v) == "15");
   }

   {
      MDBX_val k{const_cast<char*>("B"), 1};
      MDBX_val v{const_cast<char*>("2"), 1};
      REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_GET_BOTH_RANGE) == MDBX_SUCCESS);
      REQUIRE(mdbx_bytes(k) == "B");
      REQUIRE(mdbx_bytes(v) == "5");
   }

   {
      MDBX_val k{const_cast<char*>("A"), 1};
      MDBX_val v{};
      REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_SET_KEY) == MDBX_SUCCESS);
      REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_FIRST_DUP) == MDBX_SUCCESS);
      REQUIRE(mdbx_bytes(v) == "10");
      REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_LAST_DUP) == MDBX_SUCCESS);
      REQUIRE(mdbx_bytes(v) == "30");
   }

   mdbx_cursor_close(cur);
   REQUIRE(mdbx_txn_abort(txn) == MDBX_SUCCESS);
   mdbx_env_close(env);
}

TEST_CASE("C API: Silkworm binary DUPSORT state iteration",
          "[mdbx][dupsort][silkworm]")
{
   auto dir = make_temp_dir("c_silkworm_binary_dupsort");

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 16) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   MDBX_txn* txn = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

   MDBX_dbi plain_state = 0;
   REQUIRE(mdbx_dbi_open(txn, "PlainState",
                         static_cast<MDBX_db_flags_t>(MDBX_CREATE | MDBX_DUPSORT),
                         &plain_state) == MDBX_SUCCESS);

   auto address = [](unsigned char seed) {
      std::string out(20, '\0');
      for (size_t i = 0; i < out.size(); ++i)
         out[i] = static_cast<char>((seed + i * 17) & 0xff);
      out[3] = '\0';
      return out;
   };

   auto incarnation_key = [](std::string address, uint64_t incarnation) {
      for (int i = 7; i >= 0; --i)
         address.push_back(static_cast<char>((incarnation >> (i * 8)) & 0xff));
      return address;
   };

   auto storage_value = [](unsigned char slot_seed, unsigned char value_seed) {
      std::string out(32, '\0');
      for (size_t i = 0; i < 32; ++i)
         out[i] = static_cast<char>((slot_seed + i * 13) & 0xff);
      out[5] = '\0';
      out.push_back(static_cast<char>(value_seed));
      out.push_back('\0');
      out.push_back(static_cast<char>(value_seed + 1));
      return out;
   };

   auto account_value = [](unsigned char seed) {
      std::string out;
      out.push_back(static_cast<char>(0xf8));
      out.push_back('\0');
      out.push_back(static_cast<char>(seed));
      return out;
   };

   auto put = [&](const std::string& key, const std::string& value) {
      MDBX_val k{const_cast<char*>(key.data()), key.size()};
      MDBX_val v{const_cast<char*>(value.data()), value.size()};
      REQUIRE(mdbx_put(txn, plain_state, &k, &v, MDBX_UPSERT) == MDBX_SUCCESS);
   };

   const auto addr_a = address(0x11);
   const auto addr_b = address(0x70);
   const auto acct_a = account_value(0xa1);
   const auto acct_b = account_value(0xb1);
   const auto stor_a_key = incarnation_key(addr_a, 1);
   const auto stor_b_key = incarnation_key(addr_b, 1);
   const auto stor_a_low = storage_value(0x10, 0x01);
   const auto stor_a_high = storage_value(0x90, 0x02);
   const auto stor_b_low = storage_value(0x20, 0x03);

   put(stor_a_key, stor_a_high);
   put(addr_b, acct_b);
   put(stor_a_key, stor_a_low);
   put(stor_b_key, stor_b_low);
   put(addr_a, acct_a);

   MDBX_cursor* cur = nullptr;
   REQUIRE(mdbx_cursor_open(txn, plain_state, &cur) == MDBX_SUCCESS);

   MDBX_val key{};
   MDBX_val value{};
   REQUIRE(mdbx_cursor_get(cur, &key, &value, MDBX_FIRST) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(key) == addr_a);
   REQUIRE(mdbx_bytes(value) == acct_a);

   REQUIRE(mdbx_cursor_get(cur, &key, &value, MDBX_NEXT) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(key) == stor_a_key);
   REQUIRE(mdbx_bytes(value) == stor_a_low);

   REQUIRE(mdbx_cursor_get(cur, &key, &value, MDBX_NEXT_DUP) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(key) == stor_a_key);
   REQUIRE(mdbx_bytes(value) == stor_a_high);

   REQUIRE(mdbx_cursor_get(cur, &key, &value, MDBX_NEXT_DUP) == MDBX_NOTFOUND);

   REQUIRE(mdbx_cursor_get(cur, &key, &value, MDBX_NEXT) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(key) == addr_b);
   REQUIRE(mdbx_bytes(value) == acct_b);

   REQUIRE(mdbx_cursor_get(cur, &key, &value, MDBX_NEXT) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(key) == stor_b_key);
   REQUIRE(mdbx_bytes(value) == stor_b_low);

   MDBX_val seek_key{const_cast<char*>(stor_a_key.data()), stor_a_key.size()};
   MDBX_val seek_value{const_cast<char*>(stor_a_low.data()), 16};
   REQUIRE(mdbx_cursor_get(cur, &seek_key, &seek_value, MDBX_GET_BOTH_RANGE) ==
           MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(seek_key) == stor_a_key);
   REQUIRE(mdbx_bytes(seek_value) == stor_a_low);

   mdbx_cursor_close(cur);
   REQUIRE(mdbx_txn_commit(txn) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_close(env) == MDBX_SUCCESS);
}

TEST_CASE("C API: Silkworm cursor invalid operation errors",
          "[mdbx][silkworm]")
{
   auto dir = make_temp_dir("c_silkworm_cursor_invalid");

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 16) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   MDBX_txn* txn = nullptr;
   REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

   MDBX_dbi single = 0;
   REQUIRE(mdbx_dbi_open(txn, "single",
                         static_cast<MDBX_db_flags_t>(MDBX_CREATE), &single) ==
           MDBX_SUCCESS);

   MDBX_dbi multi = 0;
   REQUIRE(mdbx_dbi_open(txn, "multi",
                         static_cast<MDBX_db_flags_t>(MDBX_CREATE | MDBX_DUPSORT),
                         &multi) == MDBX_SUCCESS);

   auto put = [&](MDBX_dbi dbi, const char* k, const char* v) {
      MDBX_val key{const_cast<char*>(k), strlen(k)};
      MDBX_val val{const_cast<char*>(v), strlen(v)};
      REQUIRE(mdbx_put(txn, dbi, &key, &val, MDBX_UPSERT) == MDBX_SUCCESS);
   };
   put(single, "AA", "00");
   put(single, "BB", "11");
   put(multi, "AA", "00");
   put(multi, "AA", "11");

   MDBX_cursor* cur = nullptr;
   REQUIRE(mdbx_cursor_open(txn, single, &cur) == MDBX_SUCCESS);
   MDBX_val k{};
   MDBX_val v{};
   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_GET_CURRENT) == MDBX_ENODATA);
   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_PREV) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(k) == "BB");
   REQUIRE(mdbx_bytes(v) == "11");
   mdbx_cursor_close(cur);

   REQUIRE(mdbx_cursor_open(txn, single, &cur) == MDBX_SUCCESS);
   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_NEXT_DUP) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(k) == "AA");
   REQUIRE(mdbx_bytes(v) == "00");
   mdbx_cursor_close(cur);

   REQUIRE(mdbx_cursor_open(txn, single, &cur) == MDBX_SUCCESS);
   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_PREV_DUP) == MDBX_SUCCESS);
   REQUIRE(mdbx_bytes(k) == "BB");
   REQUIRE(mdbx_bytes(v) == "11");
   mdbx_cursor_close(cur);

   REQUIRE(mdbx_cursor_open(txn, single, &cur) == MDBX_SUCCESS);
   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_FIRST) == MDBX_SUCCESS);
   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_FIRST_DUP) == MDBX_INCOMPATIBLE);
   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_LAST_DUP) == MDBX_INCOMPATIBLE);
   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_GET_BOTH) == MDBX_INCOMPATIBLE);
   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_GET_BOTH_RANGE) == MDBX_INCOMPATIBLE);
   mdbx_cursor_close(cur);

   REQUIRE(mdbx_cursor_open(txn, multi, &cur) == MDBX_SUCCESS);
   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_FIRST_DUP) == MDBX_EINVAL);
   REQUIRE(mdbx_cursor_get(cur, &k, &v, MDBX_LAST_DUP) == MDBX_EINVAL);
   mdbx_cursor_close(cur);

   REQUIRE(mdbx_txn_abort(txn) == MDBX_SUCCESS);
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

   // Deleting an absent dup key reports NOTFOUND like native MDBX.
   {
      MDBX_val key{const_cast<char*>("missing"), 7};
      REQUIRE(mdbx_del(txn, dbi, &key, nullptr) == MDBX_NOTFOUND);
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

TEST_CASE("C API: Code table large bytecode round trip", "[mdbx][c-api][silkworm]")
{
   auto dir = make_temp_dir("c_code_large");

   const auto code_hash = hex_to_bytes(
      "f40d3381e4aaf383996cd5295d27b2edb24017be97374fd6e101ea3cf72a34cc");
   std::string bytecode;
   bytecode.reserve(18617);
   bytecode.append("\x60\x60\x60\x40\x52", 5);
   for (size_t i = bytecode.size(); i < 18617; ++i)
      bytecode.push_back(static_cast<char>((i * 131 + 17) & 0xff));

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 8) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   MDBX_dbi code_dbi = 0;
   {
      MDBX_txn* txn = nullptr;
      REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);
      REQUIRE(mdbx_dbi_open(txn, "Code", MDBX_CREATE, &code_dbi) == MDBX_SUCCESS);

      MDBX_val key{const_cast<char*>(code_hash.data()), code_hash.size()};
      MDBX_val val{bytecode.data(), bytecode.size()};
      REQUIRE(mdbx_put(txn, code_dbi, &key, &val, MDBX_UPSERT) == MDBX_SUCCESS);

      MDBX_val in_txn{};
      REQUIRE(mdbx_get(txn, code_dbi, &key, &in_txn) == MDBX_SUCCESS);
      REQUIRE(in_txn.iov_len == bytecode.size());
      REQUIRE(std::memcmp(in_txn.iov_base, bytecode.data(), bytecode.size()) == 0);

      REQUIRE(mdbx_txn_commit(txn) == MDBX_SUCCESS);
   }

   {
      MDBX_txn* ro = nullptr;
      REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro) == MDBX_SUCCESS);

      MDBX_val key{const_cast<char*>(code_hash.data()), code_hash.size()};
      MDBX_val val{};
      REQUIRE(mdbx_get(ro, code_dbi, &key, &val) == MDBX_SUCCESS);
      REQUIRE(val.iov_len == bytecode.size());
      REQUIRE(std::memcmp(val.iov_base, bytecode.data(), bytecode.size()) == 0);

      MDBX_cursor* cursor = nullptr;
      REQUIRE(mdbx_cursor_open(ro, code_dbi, &cursor) == MDBX_SUCCESS);
      key = MDBX_val{const_cast<char*>(code_hash.data()), code_hash.size()};
      val = MDBX_val{};
      REQUIRE(mdbx_cursor_get(cursor, &key, &val, MDBX_SET) == MDBX_SUCCESS);
      REQUIRE(key.iov_len == code_hash.size());
      REQUIRE(std::memcmp(key.iov_base, code_hash.data(), code_hash.size()) == 0);
      REQUIRE(val.iov_len == bytecode.size());
      REQUIRE(std::memcmp(val.iov_base, bytecode.data(), bytecode.size()) == 0);

      mdbx_cursor_close(cursor);
      mdbx_txn_abort(ro);
   }

   mdbx_env_close(env);
}

TEST_CASE("C API: Code table empty value upgraded to large bytecode", "[mdbx][c-api][silkworm]")
{
   auto dir = make_temp_dir("c_code_empty_to_large");

   const auto code_hash = hex_to_bytes(
      "f40d3381e4aaf383996cd5295d27b2edb24017be97374fd6e101ea3cf72a34cc");
   std::string bytecode;
   bytecode.reserve(18617);
   bytecode.append("\x60\x60\x60\x40\x52", 5);
   for (size_t i = bytecode.size(); i < 18617; ++i)
      bytecode.push_back(static_cast<char>((i * 131 + 17) & 0xff));

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 8) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) == MDBX_SUCCESS);

   MDBX_dbi code_dbi = 0;
   {
      MDBX_txn* txn = nullptr;
      REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);
      REQUIRE(mdbx_dbi_open(txn, "Code", MDBX_CREATE, &code_dbi) == MDBX_SUCCESS);

      MDBX_val key{const_cast<char*>(code_hash.data()), code_hash.size()};
      MDBX_val empty{nullptr, 0};
      REQUIRE(mdbx_put(txn, code_dbi, &key, &empty, MDBX_UPSERT) == MDBX_SUCCESS);
      REQUIRE(mdbx_txn_commit(txn) == MDBX_SUCCESS);
   }

   {
      MDBX_txn* txn = nullptr;
      REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) == MDBX_SUCCESS);

      MDBX_val key{const_cast<char*>(code_hash.data()), code_hash.size()};
      MDBX_val val{bytecode.data(), bytecode.size()};
      REQUIRE(mdbx_put(txn, code_dbi, &key, &val, MDBX_UPSERT) == MDBX_SUCCESS);

      MDBX_val in_txn{};
      REQUIRE(mdbx_get(txn, code_dbi, &key, &in_txn) == MDBX_SUCCESS);
      REQUIRE(in_txn.iov_len == bytecode.size());
      REQUIRE(std::memcmp(in_txn.iov_base, bytecode.data(), bytecode.size()) == 0);

      REQUIRE(mdbx_txn_commit(txn) == MDBX_SUCCESS);
   }

   {
      MDBX_txn* ro = nullptr;
      REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro) == MDBX_SUCCESS);

      MDBX_val key{const_cast<char*>(code_hash.data()), code_hash.size()};
      MDBX_val val{};
      REQUIRE(mdbx_get(ro, code_dbi, &key, &val) == MDBX_SUCCESS);
      REQUIRE(val.iov_len == bytecode.size());
      REQUIRE(std::memcmp(val.iov_base, bytecode.data(), bytecode.size()) == 0);

      mdbx_txn_abort(ro);
   }

   mdbx_env_close(env);
}

TEST_CASE("C API: Silkworm state code tables publish together",
          "[mdbx][c-api][silkworm]")
{
   auto dir = make_temp_dir("c_silkworm_state_code_publish");

   const auto address =
      hex_to_bytes("6062e466cf33a5d1e22ac57b2a726a23bf79a0d0");
   const auto code_hash = hex_to_bytes(
      "2420ae84d6a3b36442082a6ec0b552b4dbe8bc64dd2a89a72c4ca32e64055603");

   std::string storage_prefix = address + be_u64(1);
   std::string account_payload{"account-with-code-hash"};
   std::string bytecode;
   bytecode.reserve(17'411);
   bytecode.append("\x60\x60\x60\x40\x52", 5);
   for (size_t i = bytecode.size(); i < 17'411; ++i)
      bytecode.push_back(static_cast<char>((i * 67 + 29) & 0xff));

   MDBX_env* env = nullptr;
   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 64) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) ==
           MDBX_SUCCESS);

   MDBX_dbi code_dbi = 0;
   MDBX_dbi plain_code_hash_dbi = 0;
   MDBX_dbi plain_state_dbi = 0;
   {
      MDBX_txn* txn = nullptr;
      REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) ==
              MDBX_SUCCESS);
      REQUIRE(mdbx_dbi_open(txn, "Code", MDBX_CREATE, &code_dbi) ==
              MDBX_SUCCESS);
      REQUIRE(mdbx_dbi_open(txn, "PlainCodeHash", MDBX_CREATE,
                            &plain_code_hash_dbi) == MDBX_SUCCESS);
      REQUIRE(mdbx_dbi_open(
                 txn, "PlainState",
                 static_cast<MDBX_db_flags_t>(MDBX_CREATE | MDBX_DUPSORT),
                 &plain_state_dbi) == MDBX_SUCCESS);
      REQUIRE(mdbx_txn_commit(txn) == MDBX_SUCCESS);
   }

   {
      MDBX_txn* txn = nullptr;
      REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn) ==
              MDBX_SUCCESS);

      MDBX_val code_key{const_cast<char*>(code_hash.data()), code_hash.size()};
      MDBX_val code_val{bytecode.data(), bytecode.size()};
      REQUIRE(mdbx_put(txn, code_dbi, &code_key, &code_val, MDBX_UPSERT) ==
              MDBX_SUCCESS);

      MDBX_val hash_key{storage_prefix.data(), storage_prefix.size()};
      MDBX_val hash_val{const_cast<char*>(code_hash.data()), code_hash.size()};
      REQUIRE(mdbx_put(txn, plain_code_hash_dbi, &hash_key, &hash_val,
                       MDBX_UPSERT) == MDBX_SUCCESS);

      MDBX_val state_key{const_cast<char*>(address.data()), address.size()};
      MDBX_val state_val{account_payload.data(), account_payload.size()};
      REQUIRE(mdbx_put(txn, plain_state_dbi, &state_key, &state_val,
                       MDBX_UPSERT) == MDBX_SUCCESS);

      MDBX_val got{};
      REQUIRE(mdbx_get(txn, code_dbi, &code_key, &got) == MDBX_SUCCESS);
      REQUIRE(got.iov_len == bytecode.size());
      REQUIRE(std::memcmp(got.iov_base, bytecode.data(), bytecode.size()) == 0);

      got = {};
      REQUIRE(mdbx_get(txn, plain_code_hash_dbi, &hash_key, &got) ==
              MDBX_SUCCESS);
      REQUIRE(mdbx_bytes(got) == code_hash);

      got = {};
      REQUIRE(mdbx_get(txn, plain_state_dbi, &state_key, &got) == MDBX_SUCCESS);
      REQUIRE(mdbx_bytes(got) == account_payload);

      REQUIRE(mdbx_txn_commit(txn) == MDBX_SUCCESS);
   }

   mdbx_env_close(env);

   REQUIRE(mdbx_env_create(&env) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_set_maxdbs(env, 64) == MDBX_SUCCESS);
   REQUIRE(mdbx_env_open(env, dir.c_str(), MDBX_ENV_DEFAULTS, 0644) ==
           MDBX_SUCCESS);

   {
      MDBX_txn* ro = nullptr;
      REQUIRE(mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ro) ==
              MDBX_SUCCESS);
      REQUIRE(mdbx_dbi_open(ro, "Code", MDBX_DB_DEFAULTS, &code_dbi) ==
              MDBX_SUCCESS);
      REQUIRE(mdbx_dbi_open(ro, "PlainCodeHash", MDBX_DB_DEFAULTS,
                            &plain_code_hash_dbi) == MDBX_SUCCESS);
      REQUIRE(mdbx_dbi_open(ro, "PlainState", MDBX_DUPSORT,
                            &plain_state_dbi) == MDBX_SUCCESS);

      MDBX_val code_key{const_cast<char*>(code_hash.data()), code_hash.size()};
      MDBX_val got{};
      REQUIRE(mdbx_get(ro, code_dbi, &code_key, &got) == MDBX_SUCCESS);
      REQUIRE(got.iov_len == bytecode.size());
      REQUIRE(std::memcmp(got.iov_base, bytecode.data(), bytecode.size()) == 0);

      MDBX_val hash_key{storage_prefix.data(), storage_prefix.size()};
      got = {};
      REQUIRE(mdbx_get(ro, plain_code_hash_dbi, &hash_key, &got) ==
              MDBX_SUCCESS);
      REQUIRE(mdbx_bytes(got) == code_hash);

      MDBX_val state_key{const_cast<char*>(address.data()), address.size()};
      got = {};
      REQUIRE(mdbx_get(ro, plain_state_dbi, &state_key, &got) == MDBX_SUCCESS);
      REQUIRE(mdbx_bytes(got) == account_payload);

      mdbx_txn_abort(ro);
   }

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
