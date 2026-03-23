#include <catch2/catch_all.hpp>
#include <algorithm>
#include <random>
#include <vector>
#include <psitri/database.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/value_type.hpp>

using namespace psitri;

#ifdef NDEBUG
constexpr int SCALE = 1;
#else
constexpr int SCALE = 5;
#endif

namespace
{
   struct test_db
   {
      std::string                    dir;
      std::shared_ptr<database>      db;
      std::shared_ptr<write_session> ses;

      test_db(const std::string& name = "range_remove_testdb")
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
   uint64_t count_by_iteration(cursor& c, key_view lower = {}, key_view upper = {})
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

   /// Collect all keys via cursor iteration
   std::vector<std::string> collect_keys(cursor& c)
   {
      std::vector<std::string> keys;
      c.seek_begin();
      while (!c.is_end())
      {
         keys.emplace_back(c.key());
         c.next();
      }
      return keys;
   }
}  // namespace

TEST_CASE("range_remove empty tree", "[range_remove]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();
   REQUIRE(cur->remove_range("a", "z") == 0);
   REQUIRE(cur->remove_range("", max_key) == 0);
}

TEST_CASE("range_remove single leaf remove all", "[range_remove]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();

   cur->insert(to_key("apple"), to_value("1"));
   cur->insert(to_key("banana"), to_value("2"));
   cur->insert(to_key("cherry"), to_value("3"));

   REQUIRE(cur->count_keys() == 3);
   uint64_t removed = cur->remove_range("", max_key);
   REQUIRE(removed == 3);
   REQUIRE(cur->count_keys() == 0);
}

TEST_CASE("range_remove single leaf remove subset", "[range_remove]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();

   cur->insert(to_key("apple"), to_value("1"));
   cur->insert(to_key("banana"), to_value("2"));
   cur->insert(to_key("cherry"), to_value("3"));
   cur->insert(to_key("date"), to_value("4"));
   cur->insert(to_key("elderberry"), to_value("5"));

   SECTION("remove middle range")
   {
      uint64_t removed = cur->remove_range("banana", "elderberry");
      REQUIRE(removed == 3);  // banana, cherry, date
      REQUIRE(cur->count_keys() == 2);

      auto rc   = cur->read_cursor();
      auto keys = collect_keys(rc);
      REQUIRE(keys.size() == 2);
      REQUIRE(keys[0] == "apple");
      REQUIRE(keys[1] == "elderberry");
   }

   SECTION("remove from beginning")
   {
      uint64_t removed = cur->remove_range("", "cherry");
      REQUIRE(removed == 2);  // apple, banana
      REQUIRE(cur->count_keys() == 3);
   }

   SECTION("remove from end")
   {
      uint64_t removed = cur->remove_range("date", max_key);
      REQUIRE(removed == 2);  // date, elderberry
      REQUIRE(cur->count_keys() == 3);
   }
}

TEST_CASE("range_remove single key via range", "[range_remove]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();

   cur->insert(to_key("apple"), to_value("1"));
   cur->insert(to_key("banana"), to_value("2"));
   cur->insert(to_key("cherry"), to_value("3"));

   // Remove just "banana" by using [banana, cherry) range
   uint64_t removed = cur->remove_range("banana", "cherry");
   REQUIRE(removed == 1);
   REQUIRE(cur->count_keys() == 2);

   auto rc   = cur->read_cursor();
   auto keys = collect_keys(rc);
   REQUIRE(keys[0] == "apple");
   REQUIRE(keys[1] == "cherry");
}

TEST_CASE("range_remove empty range", "[range_remove]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();

   cur->insert(to_key("apple"), to_value("1"));
   cur->insert(to_key("banana"), to_value("2"));

   REQUIRE(cur->remove_range("z", "a") == 0);
   REQUIRE(cur->count_keys() == 2);
}

TEST_CASE("range_remove multi-level tree", "[range_remove]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();

   const int                N = 500 / SCALE;
   std::vector<std::string> keys;
   keys.reserve(N);
   for (int i = 0; i < N; ++i)
   {
      char buf[32];
      snprintf(buf, sizeof(buf), "key_%05d", i);
      keys.emplace_back(buf);
   }

   std::mt19937 rng(42);
   auto         shuffled = keys;
   std::shuffle(shuffled.begin(), shuffled.end(), rng);
   for (auto& k : shuffled)
      cur->insert(to_key_view(k), to_value("v"));

   std::sort(keys.begin(), keys.end());

   SECTION("remove a range from the middle")
   {
      int lo_idx = N / 4;
      int hi_idx = 3 * N / 4;

      auto     lo       = keys[lo_idx];
      auto     hi       = keys[hi_idx];
      uint64_t expected = hi_idx - lo_idx;

      uint64_t before_count = cur->count_keys(lo, hi);
      REQUIRE(before_count == expected);

      uint64_t removed = cur->remove_range(lo, hi);
      REQUIRE(removed == expected);
      REQUIRE(cur->count_keys() == N - expected);

      // Verify remaining keys are correct
      auto rc   = cur->read_cursor();
      auto remaining = collect_keys(rc);
      REQUIRE(remaining.size() == N - expected);

      // All remaining keys should be < lo or >= hi
      for (auto& k : remaining)
      {
         INFO("remaining key: " << k);
         REQUIRE((k < lo || k >= hi));
      }

      // Validate tree structure
      cur->validate();
   }

   SECTION("remove all via range")
   {
      uint64_t removed = cur->remove_range("", max_key);
      REQUIRE(removed == N);
      REQUIRE(cur->count_keys() == 0);
   }
}

TEST_CASE("range_remove prefix coverage", "[range_remove]")
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

   SECTION("remove one prefix group")
   {
      uint64_t removed = cur->remove_range("beta_", "beta_~");
      REQUIRE(removed == per_group);
      REQUIRE(cur->count_keys() == 2 * per_group);
      cur->validate();
   }

   SECTION("remove spanning groups")
   {
      uint64_t removed = cur->remove_range("alpha_", "gamma_");
      REQUIRE(removed == 2 * per_group);
      REQUIRE(cur->count_keys() == per_group);
      cur->validate();
   }

   SECTION("remove nothing")
   {
      REQUIRE(cur->remove_range("z", "zz") == 0);
      REQUIRE(cur->count_keys() == 3 * per_group);
   }
}

TEST_CASE("range_remove count_keys consistency", "[range_remove]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();

   const int                N = 200 / SCALE;
   std::vector<std::string> keys;
   std::mt19937             rng(99);

   for (int i = 0; i < N; ++i)
   {
      int         len = 3 + (rng() % 15);
      std::string key;
      key.reserve(len);
      for (int j = 0; j < len; ++j)
         key.push_back('a' + (rng() % 26));
      keys.push_back(key);
   }

   std::sort(keys.begin(), keys.end());
   keys.erase(std::unique(keys.begin(), keys.end()), keys.end());

   for (auto& k : keys)
      cur->insert(to_key_view(k), to_value("v"));

   // count_keys before remove should equal remove_range return value
   int a = keys.size() / 3;
   int b = 2 * keys.size() / 3;

   key_view lower = keys[a];
   key_view upper = keys[b];

   uint64_t counted = cur->count_keys(lower, upper);
   uint64_t removed = cur->remove_range(lower, upper);
   REQUIRE(counted == removed);
   cur->validate();
}

TEST_CASE("range_remove interleaved insert/remove", "[range_remove]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();

   const int N = 100 / SCALE;

   // Insert some keys
   for (int i = 0; i < N; ++i)
   {
      char buf[32];
      snprintf(buf, sizeof(buf), "key_%05d", i);
      cur->insert(to_key(buf), to_value("v1"));
   }

   // Remove a range from the middle (indices scaled to actual count)
   int lo_idx = N / 5;
   int hi_idx = 3 * N / 5;
   char lo_buf[32], hi_buf[32];
   snprintf(lo_buf, sizeof(lo_buf), "key_%05d", lo_idx);
   snprintf(hi_buf, sizeof(hi_buf), "key_%05d", hi_idx);

   uint64_t removed1 = cur->remove_range(lo_buf, hi_buf);
   REQUIRE(removed1 == uint64_t(hi_idx - lo_idx));
   cur->validate();

   // Insert some keys back into the removed range
   for (int i = lo_idx; i < hi_idx; i += 2)
   {
      char buf[32];
      snprintf(buf, sizeof(buf), "key_%05d", i);
      cur->insert(to_key(buf), to_value("v2"));
   }
   cur->validate();

   // Remove from midpoint to end
   int mid_idx = N / 2;
   char mid_buf[32];
   snprintf(mid_buf, sizeof(mid_buf), "key_%05d", mid_idx);

   uint64_t removed2 = cur->remove_range(mid_buf, max_key);
   REQUIRE(removed2 > 0);
   cur->validate();

   // All remaining keys should be < mid_buf
   auto rc   = cur->read_cursor();
   auto keys = collect_keys(rc);
   for (auto& k : keys)
   {
      INFO("remaining: " << k);
      REQUIRE(k < std::string(mid_buf));
   }
}

TEST_CASE("range_remove brute-force validation", "[range_remove]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();

   const int                N = 300 / SCALE;
   std::vector<std::string> keys;
   std::mt19937             rng(54321);

   for (int i = 0; i < N; ++i)
   {
      int         len = 3 + (rng() % 20);
      std::string key;
      key.reserve(len);
      for (int j = 0; j < len; ++j)
         key.push_back('a' + (rng() % 26));
      keys.push_back(key);
   }

   std::sort(keys.begin(), keys.end());
   keys.erase(std::unique(keys.begin(), keys.end()), keys.end());

   for (auto& k : keys)
      cur->insert(to_key_view(k), to_value("v"));

   uint64_t initial_count = cur->count_keys();
   REQUIRE(initial_count == keys.size());

   // Perform several random range removes and validate after each
   const int                     num_trials = 10 / SCALE;
   std::uniform_int_distribution<int> dist(0, keys.size() - 1);

   std::vector<std::string> remaining_keys = keys;

   for (int trial = 0; trial < num_trials && remaining_keys.size() > 1; ++trial)
   {
      int a = dist(rng) % remaining_keys.size();
      int b = dist(rng) % remaining_keys.size();
      if (a > b)
         std::swap(a, b);
      if (a == b)
         continue;

      std::string lower_str = remaining_keys[a];
      std::string upper_str = remaining_keys[b];
      if (lower_str >= upper_str)
         continue;  // empty range, skip

      // Compute expected removals
      uint64_t expected = 0;
      auto     it       = remaining_keys.begin();
      while (it != remaining_keys.end())
      {
         if (*it >= lower_str && *it < upper_str)
         {
            it = remaining_keys.erase(it);
            ++expected;
         }
         else
            ++it;
      }

      uint64_t removed = cur->remove_range(lower_str, upper_str);
      INFO("trial=" << trial << " lower=" << lower_str << " upper=" << upper_str);
      REQUIRE(removed == expected);
      REQUIRE(cur->count_keys() == remaining_keys.size());

      cur->validate();

      // Verify remaining keys match
      auto rc     = cur->read_cursor();
      auto actual = collect_keys(rc);
      REQUIRE(actual.size() == remaining_keys.size());
      for (size_t i = 0; i < actual.size(); ++i)
      {
         INFO("index=" << i << " expected=" << remaining_keys[i] << " actual=" << actual[i]);
         REQUIRE(actual[i] == remaining_keys[i]);
      }
   }
}

TEST_CASE("range_remove large dataset with big ranges", "[range_remove]")
{
   test_db t;
   auto    cur = t.ses->create_write_cursor();

   // Load a large dataset — synthetic dictionary-like words
   const int                N = 10000 / SCALE;
   std::vector<std::string> keys;
   keys.reserve(N);
   std::mt19937 rng(77777);

   // Generate realistic variable-length keys with shared prefixes
   const char* prefixes[] = {"account_", "balance_", "config_", "data_",  "event_",
                             "file_",    "group_",   "hash_",   "index_", "job_",
                             "key_",     "log_",     "meta_",   "node_",  "order_",
                             "payment_", "query_",   "record_", "state_", "token_",
                             "user_",    "value_",   "work_",   "xact_",  "zone_"};
   constexpr int num_prefixes = sizeof(prefixes) / sizeof(prefixes[0]);

   for (int i = 0; i < N; ++i)
   {
      std::string key = prefixes[rng() % num_prefixes];
      int         suffix_len = 4 + (rng() % 12);
      for (int j = 0; j < suffix_len; ++j)
         key.push_back('a' + (rng() % 26));
      keys.push_back(key);
   }

   // Deduplicate and sort
   std::sort(keys.begin(), keys.end());
   keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
   int total = keys.size();

   // Shuffle for insertion to get varied tree structure
   auto shuffled = keys;
   std::shuffle(shuffled.begin(), shuffled.end(), rng);
   for (auto& k : shuffled)
      cur->insert(to_key_view(k), to_value("v"));

   REQUIRE(cur->count_keys() == total);

   SECTION("remove large contiguous range from middle")
   {
      // Remove ~50% of keys from the middle
      int lo_idx = total / 4;
      int hi_idx = 3 * total / 4;

      auto     lo       = keys[lo_idx];
      auto     hi       = keys[hi_idx];
      uint64_t expected = cur->count_keys(lo, hi);
      REQUIRE(expected > 500 / SCALE);  // sanity: should be a big range

      uint64_t removed = cur->remove_range(lo, hi);
      REQUIRE(removed == expected);
      REQUIRE(cur->count_keys() == total - expected);

      // Validate remaining keys
      auto                    rc     = cur->read_cursor();
      auto                    actual = collect_keys(rc);
      std::vector<std::string> expected_remaining;
      for (auto& k : keys)
         if (k < lo || k >= hi)
            expected_remaining.push_back(k);

      REQUIRE(actual.size() == expected_remaining.size());
      for (size_t i = 0; i < actual.size(); ++i)
      {
         INFO("i=" << i);
         REQUIRE(actual[i] == expected_remaining[i]);
      }
      cur->validate();
   }

   SECTION("multiple large range removes")
   {
      std::vector<std::string> remaining = keys;
      const int                num_removes = 5;

      for (int trial = 0; trial < num_removes && remaining.size() > 100; ++trial)
      {
         // Pick a random range spanning ~20% of remaining keys
         int range_size = remaining.size() / 5;
         int lo_idx     = rng() % (remaining.size() - range_size);
         int hi_idx     = lo_idx + range_size;

         std::string lo = remaining[lo_idx];
         std::string hi = remaining[hi_idx];

         uint64_t counted = cur->count_keys(lo, hi);
         uint64_t removed = cur->remove_range(lo, hi);
         INFO("trial=" << trial << " lo=" << lo << " hi=" << hi);
         REQUIRE(removed == counted);

         // Update expected remaining
         remaining.erase(
             std::remove_if(remaining.begin(), remaining.end(),
                            [&](const std::string& k) { return k >= lo && k < hi; }),
             remaining.end());

         REQUIRE(cur->count_keys() == remaining.size());
         cur->validate();
      }

      // Final full validation
      auto rc     = cur->read_cursor();
      auto actual = collect_keys(rc);
      REQUIRE(actual.size() == remaining.size());
      for (size_t i = 0; i < actual.size(); ++i)
      {
         INFO("i=" << i);
         REQUIRE(actual[i] == remaining[i]);
      }
   }

   SECTION("remove entire prefix group")
   {
      // Remove all keys starting with "user_"
      uint64_t counted = cur->count_keys("user_", "user_~");
      uint64_t removed = cur->remove_range("user_", "user_~");
      REQUIRE(removed == counted);
      REQUIRE(removed > 0);

      // Verify no "user_" keys remain
      auto rc     = cur->read_cursor();
      auto actual = collect_keys(rc);
      for (auto& k : actual)
      {
         INFO("key=" << k);
         REQUIRE(k.substr(0, 5) != "user_");
      }
      REQUIRE(cur->count_keys() == total - removed);
      cur->validate();
   }

   SECTION("remove everything")
   {
      uint64_t removed = cur->remove_range("", max_key);
      REQUIRE(removed == total);
      REQUIRE(cur->count_keys() == 0);
   }

   SECTION("remove from beginning")
   {
      // Remove first ~30% of keys
      int      hi_idx  = total * 3 / 10;
      auto     hi      = keys[hi_idx];
      uint64_t removed = cur->remove_range("", hi);
      REQUIRE(removed == (uint64_t)hi_idx);
      REQUIRE(cur->count_keys() == total - hi_idx);
      cur->validate();
   }

   SECTION("remove from end")
   {
      // Remove last ~30% of keys
      int      lo_idx  = total * 7 / 10;
      auto     lo      = keys[lo_idx];
      uint64_t removed = cur->remove_range(lo, max_key);
      REQUIRE(removed == (uint64_t)(total - lo_idx));
      REQUIRE(cur->count_keys() == lo_idx);
      cur->validate();
   }
}
