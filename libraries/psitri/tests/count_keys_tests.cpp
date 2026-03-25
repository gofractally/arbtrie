#include <catch2/catch_all.hpp>
#include <algorithm>
#include <random>
#include <vector>
#include <psitri/database.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/value_type.hpp>

using namespace psitri;

constexpr int SCALE = 1;

namespace
{
   struct test_db
   {
      std::string                    dir;
      std::shared_ptr<database>      db;
      std::shared_ptr<write_session> ses;

      test_db(const std::string& name = "count_keys_testdb")
          : dir(name)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir + "/data");
         db  = std::make_shared<database>(dir, runtime_config());
         ses = db->start_write_session();
      }
      ~test_db() { std::filesystem::remove_all(dir); }
   };

   /// Brute-force count via cursor iteration for validation
   uint64_t count_by_iteration(cursor& c, key_view lower, key_view upper)
   {
      uint64_t count = 0;
      if (lower.empty())
         c.seek_begin();
      else
         c.lower_bound(lower);

      while (!c.is_end())
      {
         if (!upper.empty() && c.key() >= upper)
            break;
         ++count;
         c.next();
      }
      return count;
   }
   // Helper: insert keys with a given prefix
   void insert_prefix_group(write_cursor_ptr& cur,
                            const std::string& prefix,
                            int                count)
   {
      for (int i = 0; i < count; ++i)
      {
         char key[128], val[128];
         snprintf(key, sizeof(key), "%s%03d", prefix.c_str(), i);
         snprintf(val, sizeof(val), "v%03d", i);
         cur->upsert(to_key(key), to_value(val));
      }
   }
}  // namespace

TEST_CASE("count_keys empty tree", "[count_keys]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();
   REQUIRE(cur->count_keys() == 0);
   REQUIRE(cur->count_keys("a", "z") == 0);
}

TEST_CASE("count_keys single leaf", "[count_keys]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();

   cur->insert(to_key("apple"), to_value("1"));
   cur->insert(to_key("banana"), to_value("2"));
   cur->insert(to_key("cherry"), to_value("3"));
   cur->insert(to_key("date"), to_value("4"));
   cur->insert(to_key("elderberry"), to_value("5"));

   SECTION("unbounded returns total")
   {
      REQUIRE(cur->count_keys() == 5);
   }
   SECTION("lower bound only")
   {
      REQUIRE(cur->count_keys("cherry") == 3);  // cherry, date, elderberry
   }
   SECTION("upper bound only")
   {
      REQUIRE(cur->count_keys("", "cherry") == 2);  // apple, banana
   }
   SECTION("both bounds")
   {
      REQUIRE(cur->count_keys("banana", "elderberry") == 3);  // banana, cherry, date
   }
   SECTION("empty range")
   {
      REQUIRE(cur->count_keys("z", "a") == 0);
   }
   SECTION("range containing no keys")
   {
      REQUIRE(cur->count_keys("f", "g") == 0);
   }
   SECTION("range containing all keys")
   {
      REQUIRE(cur->count_keys("a", "z") == 5);
   }
   SECTION("exact key as lower bound")
   {
      REQUIRE(cur->count_keys("banana", "date") == 2);  // banana, cherry
   }
   SECTION("exact key as upper bound is exclusive")
   {
      REQUIRE(cur->count_keys("apple", "cherry") == 2);  // apple, banana
   }
}

TEST_CASE("count_keys multi-level tree", "[count_keys]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();

   // Insert enough keys to force inner nodes
   const int              N = 500 / SCALE;
   std::vector<std::string> keys;
   keys.reserve(N);
   for (int i = 0; i < N; ++i)
   {
      char buf[32];
      snprintf(buf, sizeof(buf), "key_%05d", i);
      keys.emplace_back(buf);
   }
   // Insert in random order to get varied tree structure
   std::mt19937 rng(42);
   auto shuffled = keys;
   std::shuffle(shuffled.begin(), shuffled.end(), rng);
   for (auto& k : shuffled)
      cur->insert(to_key_view(k), to_value("v"));

   // Sort for reference
   std::sort(keys.begin(), keys.end());

   SECTION("unbounded equals total")
   {
      REQUIRE(cur->count_keys() == N);
   }

   SECTION("validate against iteration for random ranges")
   {
      auto                             rc = cur->read_cursor();
      std::uniform_int_distribution<int> dist(0, N - 1);
      const int                         num_checks = 50 / SCALE;

      for (int trial = 0; trial < num_checks; ++trial)
      {
         int a = dist(rng);
         int b = dist(rng);
         if (a > b)
            std::swap(a, b);

         key_view lower = keys[a];
         key_view upper = (b < N - 1) ? key_view(keys[b + 1]) : key_view();

         uint64_t counted  = cur->count_keys(lower, upper.empty() ? key_view() : upper);
         uint64_t iterated = count_by_iteration(rc, lower, upper);

         INFO("lower=" << lower << " upper=" << (upper.empty() ? "(unbounded)" : upper)
                        << " trial=" << trial);
         REQUIRE(counted == iterated);
      }
   }
}

TEST_CASE("count_keys prefix coverage", "[count_keys]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();

   // Keys with shared prefixes to force inner_prefix_node creation
   std::vector<std::string> keys;
   for (const char* prefix : {"alpha_", "beta_", "gamma_"})
      for (int i = 0; i < 50 / SCALE; ++i)
      {
         char buf[64];
         snprintf(buf, sizeof(buf), "%s%03d", prefix, i);
         keys.emplace_back(buf);
      }

   for (auto& k : keys)
      cur->insert(to_key_view(k), to_value("v"));

   std::sort(keys.begin(), keys.end());
   int per_group = 50 / SCALE;

   SECTION("range within one prefix group")
   {
      auto rc       = cur->read_cursor();
      auto counted  = cur->count_keys("beta_", "beta_~");
      auto iterated = count_by_iteration(rc, "beta_", "beta_~");
      REQUIRE(counted == iterated);
      REQUIRE(counted == per_group);
   }

   SECTION("range spanning prefix groups")
   {
      auto rc       = cur->read_cursor();
      auto counted  = cur->count_keys("alpha_", "gamma_");
      auto iterated = count_by_iteration(rc, "alpha_", "gamma_");
      REQUIRE(counted == iterated);
   }

   SECTION("range excluding all groups")
   {
      REQUIRE(cur->count_keys("z", "zz") == 0);
   }
}

TEST_CASE("count_keys edge cases", "[count_keys]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();

   SECTION("single-character keys")
   {
      for (char c = 'a'; c <= 'z'; ++c)
      {
         std::string s(1, c);
         cur->insert(to_key_view(s), to_value("v"));
      }

      REQUIRE(cur->count_keys() == 26);
      REQUIRE(cur->count_keys("d", "h") == 4);  // d, e, f, g
      REQUIRE(cur->count_keys("a", "b") == 1);  // just a
   }

   SECTION("keys that are prefixes of each other")
   {
      cur->insert(to_key("a"), to_value("1"));
      cur->insert(to_key("ab"), to_value("2"));
      cur->insert(to_key("abc"), to_value("3"));
      cur->insert(to_key("abcd"), to_value("4"));

      REQUIRE(cur->count_keys() == 4);
      REQUIRE(cur->count_keys("a", "abc") == 2);   // a, ab
      REQUIRE(cur->count_keys("ab", "abcd") == 2);  // ab, abc
   }
}

TEST_CASE("count_keys brute-force validation", "[count_keys]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();

   const int                N = 2000 / SCALE;
   std::vector<std::string> keys;
   std::mt19937             rng(12345);

   // Generate random keys of varying length
   for (int i = 0; i < N; ++i)
   {
      int         len = 3 + (rng() % 20);
      std::string key;
      key.reserve(len);
      for (int j = 0; j < len; ++j)
         key.push_back('a' + (rng() % 26));
      keys.push_back(key);
   }

   // Remove duplicates
   std::sort(keys.begin(), keys.end());
   keys.erase(std::unique(keys.begin(), keys.end()), keys.end());

   for (auto& k : keys)
      cur->insert(to_key_view(k), to_value("v"));

   REQUIRE(cur->count_keys() == keys.size());

   // Validate many random ranges
   auto                                rc = cur->read_cursor();
   std::uniform_int_distribution<int>  dist(0, keys.size() - 1);
   const int                           M = 200 / SCALE;

   for (int trial = 0; trial < M; ++trial)
   {
      int a = dist(rng);
      int b = dist(rng);
      if (a > b)
         std::swap(a, b);

      key_view lower = keys[a];
      key_view upper = (b < (int)keys.size() - 1) ? key_view(keys[b + 1]) : key_view();

      uint64_t counted  = cur->count_keys(lower, upper.empty() ? key_view() : upper);
      uint64_t iterated = count_by_iteration(rc, lower, upper);

      INFO("lower=" << lower << " upper=" << (upper.empty() ? "(unbounded)" : upper)
                     << " trial=" << trial);
      REQUIRE(counted == iterated);
   }
}

// ============================================================================
// Coverage-focused tests: exercise uncovered code paths in count_keys.hpp
// ============================================================================

TEST_CASE("coverage: count_keys prefix narrowing edge cases", "[count_keys][coverage]")
{
   // Create 3 prefix groups to force inner_prefix_nodes with prefixes "aaa_", "bbb_", "ccc_"
   test_db t("cov_ck_prefix");
   auto    cur = t.ses->create_write_cursor();
   insert_prefix_group(cur, "aaa_", 20);
   insert_prefix_group(cur, "bbb_", 20);
   insert_prefix_group(cur, "ccc_", 20);
   REQUIRE(cur->count_keys() == 60);

   auto rc = cur->read_cursor();

   SECTION("lower diverges after prefix — all keys < lower (count_keys line 88)")
   {
      // prefix="bbb_", lower="bbc" → prefix[2]='b' < lower[2]='c' → count=0 for bbb group
      uint64_t counted  = cur->count_keys("bbc", "z");
      uint64_t iterated = count_by_iteration(rc, "bbc", "z");
      REQUIRE(counted == iterated);
      REQUIRE(counted == 20);  // only ccc group
   }

   SECTION("lower is prefix of node prefix (count_keys line 93)")
   {
      // prefix="bbb_", lower="bb" → lower is prefix of prefix → all keys > lower
      uint64_t counted  = cur->count_keys("bb", "z");
      uint64_t iterated = count_by_iteration(rc, "bb", "z");
      REQUIRE(counted == iterated);
      REQUIRE(counted == 40);  // bbb + ccc
   }

   SECTION("upper equals prefix exactly (count_keys line 104)")
   {
      // prefix="bbb_", upper="bbb_" → exclusive, 0 keys from bbb group
      uint64_t counted  = cur->count_keys("a", "bbb_");
      uint64_t iterated = count_by_iteration(rc, "a", "bbb_");
      REQUIRE(counted == iterated);
      REQUIRE(counted == 20);  // only aaa group
   }

   SECTION("upper diverges before prefix — all keys >= upper (count_keys line 111)")
   {
      // prefix="bbb_", upper="bba" → prefix[2]='b' >= upper[2]='a' → count=0 for bbb
      uint64_t counted  = cur->count_keys("a", "bba");
      uint64_t iterated = count_by_iteration(rc, "a", "bba");
      REQUIRE(counted == iterated);
      REQUIRE(counted == 20);  // only aaa group
   }

   SECTION("upper is prefix of node prefix (count_keys line 118)")
   {
      // prefix="bbb_", upper="bbb" → upper is prefix of prefix → 0 keys from bbb
      uint64_t counted  = cur->count_keys("a", "bbb");
      uint64_t iterated = count_by_iteration(rc, "a", "bbb");
      REQUIRE(counted == iterated);
      REQUIRE(counted == 20);  // only aaa group
   }

   SECTION("empty range after prefix narrowing (count_keys line 129)")
   {
      // Both bounds narrow into the same prefix group but create inverted range
      // lower="bbb_z" → narrows to "z", upper="bbb_a" → narrows to "a"
      // Since "z" > "a", the range is empty
      uint64_t counted  = cur->count_keys("bbb_z", "bbb_a");
      uint64_t iterated = count_by_iteration(rc, "bbb_z", "bbb_a");
      REQUIRE(counted == 0);
      REQUIRE(iterated == 0);
   }

   SECTION("start >= num_branches after narrowing (count_keys line 136)")
   {
      // All keys in bbb group are "bbb_000"-"bbb_019".
      // lower="bbb_z" → after prefix narrowing "z", lower_bound('z') >= num_branches
      uint64_t counted  = cur->count_keys("bbb_z", "ccc_");
      uint64_t iterated = count_by_iteration(rc, "bbb_z", "ccc_");
      REQUIRE(counted == iterated);
      REQUIRE(counted == 0);
   }
}

TEST_CASE("coverage: count_keys on value_node children", "[count_keys][coverage]")
{
   // Keys that are prefixes of each other create value_nodes at branch points.
   test_db t("cov_ck_value_node");
   auto    cur = t.ses->create_write_cursor();

   cur->upsert(to_key("x"), to_value("val_x"));
   cur->upsert(to_key("xa"), to_value("val_xa"));
   cur->upsert(to_key("xab"), to_value("val_xab"));
   cur->upsert(to_key("xb"), to_value("val_xb"));
   REQUIRE(cur->count_keys() == 4);

   auto rc = cur->read_cursor();

   SECTION("range includes value_node key")
   {
      // "x" is at empty key after prefix "x" → value_node
      // Range [x, xa) should count just "x"
      uint64_t counted  = cur->count_keys("x", "xa");
      uint64_t iterated = count_by_iteration(rc, "x", "xa");
      REQUIRE(counted == iterated);
      REQUIRE(counted == 1);
   }

   SECTION("range excludes value_node key")
   {
      // Range [xa, xz) should count xa, xab, xb but NOT "x"
      uint64_t counted  = cur->count_keys("xa", "xz");
      uint64_t iterated = count_by_iteration(rc, "xa", "xz");
      REQUIRE(counted == iterated);
      REQUIRE(counted == 3);
   }

   SECTION("range before all keys")
   {
      uint64_t counted = cur->count_keys("a", "b");
      REQUIRE(counted == 0);
   }

   SECTION("unbounded count")
   {
      uint64_t counted  = cur->count_keys();
      uint64_t iterated = count_by_iteration(rc, {}, {});
      REQUIRE(counted == 4);
      REQUIRE(iterated == 4);
   }
}
