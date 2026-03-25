/**
 * Zip Tests — insert/remove interleaving that exercises every boundary condition.
 *
 * The "zip" pattern:
 *   Growing phase:  insert 2, remove last 1, insert 2, remove last 1, ...
 *   Shrinking phase: remove 2, reinsert last 1, remove 2, reinsert last 1, ...
 *
 * This ensures every node size (1, 2, 3, ..., N) is hit during both growth
 * and shrinkage, catching off-by-one errors in split/merge/defrag paths.
 * Tree invariants are validated after every mutation step.
 *
 * Key types exercise different trie structures:
 *   - dictionary words   (variable-length, prefix-heavy)
 *   - big-endian seq     (fixed 8-byte, high byte changes slowly)
 *   - little-endian seq  (fixed 8-byte, low byte changes fast)
 *   - short 3-byte keys  (dense, many collisions per inner node)
 *   - long prefix keys   (forces inner_prefix_node paths)
 *
 * Value sizes are varied to cross the 64-byte value_node threshold.
 */

#include <catch2/catch_all.hpp>
#include <algorithm>
#include <fstream>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <psitri/database.hpp>
#include <psitri/transaction.hpp>
#include <psitri/tree_ops.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/value_type.hpp>

using namespace psitri;

constexpr int ZIP_SCALE = 1;

namespace
{
   struct zip_db
   {
      std::string                    dir;
      std::shared_ptr<database>      db;
      std::shared_ptr<write_session> ses;

      zip_db(const std::string& name = "zip_testdb")
          : dir(name)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir + "/data");
         db  = std::make_shared<database>(dir, runtime_config());
         ses = db->start_write_session();
      }
      ~zip_db() { std::filesystem::remove_all(dir); }
   };

   // ── Key generators ──────────────────────────────────────────

   /// Big-endian sequential: 8 bytes, MSB varies slowly
   std::string big_endian_key(uint64_t i)
   {
      std::string k(8, '\0');
      for (int b = 7; b >= 0; --b)
      {
         k[7 - b] = static_cast<char>(i >> (b * 8));
      }
      return k;
   }

   /// Little-endian sequential: 8 bytes, LSB varies fast
   std::string little_endian_key(uint64_t i)
   {
      std::string k(8, '\0');
      for (int b = 0; b < 8; ++b)
         k[b] = static_cast<char>(i >> (b * 8));
      return k;
   }

   /// Short 3-byte keys: dense, exercises leaf packing tightly
   std::string short_key(uint64_t i)
   {
      std::string k(3, '\0');
      k[0] = static_cast<char>((i >> 16) & 0xff);
      k[1] = static_cast<char>((i >> 8) & 0xff);
      k[2] = static_cast<char>(i & 0xff);
      return k;
   }

   /// Long common-prefix keys: forces inner_prefix_node creation
   std::string long_prefix_key(uint64_t i)
   {
      // 20-byte shared prefix + 4-byte discriminator
      std::string k = "shared/prefix/path__";
      k += static_cast<char>((i >> 24) & 0xff);
      k += static_cast<char>((i >> 16) & 0xff);
      k += static_cast<char>((i >> 8) & 0xff);
      k += static_cast<char>(i & 0xff);
      return k;
   }

   /// Dictionary words from /usr/share/dict/words
   std::vector<std::string> load_dict_keys(uint32_t limit = 2000)
   {
      std::vector<std::string> words;
      words.reserve(limit);
      std::ifstream file("/usr/share/dict/words");
      std::string   word;
      while (file >> word && words.size() < limit)
         words.push_back(word);
      return words;
   }

   // ── Value generators ────────────────────────────────────────

   /// Generates values of varying sizes. Cycles through:
   ///   empty, tiny (3B), medium (30B), near-threshold (60B),
   ///   over-threshold (100B, forces value_node), large (200B)
   std::string make_value(uint64_t i)
   {
      switch (i % 6)
      {
         case 0:
            return "";  // empty
         case 1:
            return std::string(3, 'a' + (i % 26));  // 3 bytes
         case 2:
            return std::string(30, 'b' + (i % 26));  // 30 bytes
         case 3:
            return std::string(60, 'c' + (i % 26));  // 60 bytes (near threshold)
         case 4:
            return std::string(100, 'd' + (i % 26));  // 100 bytes (value_node)
         case 5:
            return std::string(200, 'e' + (i % 26));  // 200 bytes (large value_node)
         default:
            return "";
      }
   }

   // ── The zip test engine ─────────────────────────────────────

   /**
    * Run the full zip pattern for a given key sequence.
    *
    * @param keys  Ordered sequence of unique keys to use
    * @param N     Number of keys to exercise (clamped to keys.size())
    */
   void run_zip(zip_db& t, const std::vector<std::string>& keys, int N)
   {
      N = std::min(N, (int)keys.size());
      INFO("zip test with " << N << " keys, first key len=" << keys[0].size());

      auto cur = t.ses->create_write_cursor();

      // Track which keys are currently in the tree
      std::set<std::string> live;
      int                   seq = 0;  // value counter for varied sizes

      // ── Growing phase: net +1 per iteration ──────────────────
      // Insert keys[0..1], remove keys[1], insert keys[1..2], remove keys[2], ...
      // After step i: tree contains keys[0..i]
      for (int i = 0; i < N; ++i)
      {
         // Insert key[i]
         {
            auto v = make_value(seq++);
            cur->upsert(to_key_view(keys[i]), to_value_view(v));
            live.insert(keys[i]);
            cur->validate();
            REQUIRE(cur->count_keys() == live.size());
         }

         // If not the last key in this phase, insert key[i+1] then remove it
         if (i + 1 < N)
         {
            // Insert next
            {
               auto v = make_value(seq++);
               cur->upsert(to_key_view(keys[i + 1]), to_value_view(v));
               live.insert(keys[i + 1]);
               cur->validate();
               REQUIRE(cur->count_keys() == live.size());
            }
            // Remove it
            {
               int removed = cur->remove(to_key_view(keys[i + 1]));
               REQUIRE(removed >= 0);
               live.erase(keys[i + 1]);
               cur->validate();
               REQUIRE(cur->count_keys() == live.size());
            }
         }
      }

      // All N keys should now be in the tree
      REQUIRE(cur->count_keys() == (uint64_t)N);

      // ── Update phase: change every value (exercises update_value) ──
      for (int i = 0; i < N; ++i)
      {
         auto v = make_value(seq++);
         cur->upsert(to_key_view(keys[i]), to_value_view(v));
         cur->validate();
      }
      REQUIRE(cur->count_keys() == (uint64_t)N);

      // ── Shrinking phase: net -1 per iteration ────────────────
      // Remove keys[N-1] and keys[N-2], reinsert keys[N-2], remove keys[N-3]&[N-2], reinsert [N-3]...
      for (int i = N - 1; i >= 0; --i)
      {
         // Remove key[i]
         {
            int removed = cur->remove(to_key_view(keys[i]));
            REQUIRE(removed >= 0);
            live.erase(keys[i]);
            cur->validate();
            REQUIRE(cur->count_keys() == live.size());
         }

         // If not the last removal, also remove key[i-1] then reinsert it
         if (i > 0)
         {
            // Remove previous
            {
               int removed = cur->remove(to_key_view(keys[i - 1]));
               REQUIRE(removed >= 0);
               live.erase(keys[i - 1]);
               cur->validate();
               REQUIRE(cur->count_keys() == live.size());
            }
            // Reinsert it
            {
               auto v = make_value(seq++);
               cur->upsert(to_key_view(keys[i - 1]), to_value_view(v));
               live.insert(keys[i - 1]);
               cur->validate();
               REQUIRE(cur->count_keys() == live.size());
            }
         }
      }

      // Tree should be empty
      REQUIRE(cur->count_keys() == 0);
      REQUIRE(live.empty());
   }

}  // namespace

// ════════════════════════════════════════════════════════════════
// Test cases
// ════════════════════════════════════════════════════════════════

TEST_CASE("zip: big-endian sequential keys", "[zip]")
{
   zip_db t("zip_be_testdb");
   int    N = 300 / ZIP_SCALE;

   std::vector<std::string> keys;
   keys.reserve(N);
   for (int i = 0; i < N; ++i)
      keys.push_back(big_endian_key(i));

   run_zip(t, keys, N);
}

TEST_CASE("zip: little-endian sequential keys", "[zip]")
{
   zip_db t("zip_le_testdb");
   int    N = 300 / ZIP_SCALE;

   std::vector<std::string> keys;
   keys.reserve(N);
   for (int i = 0; i < N; ++i)
      keys.push_back(little_endian_key(i));

   run_zip(t, keys, N);
}

TEST_CASE("zip: short 3-byte keys", "[zip]")
{
   zip_db t("zip_short_testdb");
   int    N = 300 / ZIP_SCALE;

   std::vector<std::string> keys;
   keys.reserve(N);
   for (int i = 0; i < N; ++i)
      keys.push_back(short_key(i));

   run_zip(t, keys, N);
}

TEST_CASE("zip: long common-prefix keys", "[zip]")
{
   zip_db t("zip_prefix_testdb");
   int    N = 300 / ZIP_SCALE;

   std::vector<std::string> keys;
   keys.reserve(N);
   for (int i = 0; i < N; ++i)
      keys.push_back(long_prefix_key(i));

   run_zip(t, keys, N);
}

TEST_CASE("zip: dictionary words", "[zip]")
{
   auto words = load_dict_keys(2000);
   if (words.size() < 50)
   {
      WARN("Skipping dictionary test — /usr/share/dict/words not available");
      return;
   }

   zip_db t("zip_dict_testdb");
   int    N = std::min((int)words.size(), 500 / ZIP_SCALE);

   // Dictionary words are already unique and sorted
   run_zip(t, words, N);
}

TEST_CASE("zip: short 3-byte keys with large values only", "[zip]")
{
   // This specifically targets the bug: 3-byte keys + values crossing the
   // 64-byte value_node threshold, exercising update_value transitions
   // between inline and value_node storage.
   zip_db t("zip_short_large_testdb");
   int    N = 200 / ZIP_SCALE;

   std::vector<std::string> keys;
   keys.reserve(N);
   for (int i = 0; i < N; ++i)
      keys.push_back(short_key(i));

   // Override: all values are 100 bytes (above 64-byte threshold → value_node)
   auto cur = t.ses->create_write_cursor();

   std::set<std::string> live;
   int                   seq = 0;

   auto large_val = [&]() -> std::string
   {
      return std::string(100, 'A' + (seq++ % 26));
   };

   // Growing phase
   for (int i = 0; i < N; ++i)
   {
      cur->upsert(to_key_view(keys[i]), to_value_view(large_val()));
      live.insert(keys[i]);
      cur->validate();

      if (i + 1 < N)
      {
         cur->upsert(to_key_view(keys[i + 1]), to_value_view(large_val()));
         live.insert(keys[i + 1]);
         cur->validate();

         cur->remove(to_key_view(keys[i + 1]));
         live.erase(keys[i + 1]);
         cur->validate();
      }
   }
   REQUIRE(cur->count_keys() == (uint64_t)N);

   // Update phase: switch values between small (inline) and large (value_node)
   for (int i = 0; i < N; ++i)
   {
      // Alternate: even → small inline, odd → large value_node
      std::string v = (i % 2 == 0) ? std::string(10, 'x') : std::string(100, 'Y');
      cur->upsert(to_key_view(keys[i]), to_value_view(v));
      cur->validate();
   }
   // Flip them back
   for (int i = 0; i < N; ++i)
   {
      std::string v = (i % 2 == 0) ? std::string(100, 'Z') : std::string(5, 'w');
      cur->upsert(to_key_view(keys[i]), to_value_view(v));
      cur->validate();
   }

   REQUIRE(cur->count_keys() == (uint64_t)N);

   // Shrinking phase
   for (int i = N - 1; i >= 0; --i)
   {
      cur->remove(to_key_view(keys[i]));
      live.erase(keys[i]);
      cur->validate();

      if (i > 0)
      {
         cur->remove(to_key_view(keys[i - 1]));
         live.erase(keys[i - 1]);
         cur->validate();

         cur->upsert(to_key_view(keys[i - 1]), to_value_view(large_val()));
         live.insert(keys[i - 1]);
         cur->validate();
      }
   }
   REQUIRE(cur->count_keys() == 0);
}

TEST_CASE("zip: shuffled keys stress boundary ordering", "[zip]")
{
   // Same keys but in shuffled order — different insertion/removal patterns
   // hit different inner node split/merge paths
   zip_db t("zip_shuffled_testdb");
   int    N = 300 / ZIP_SCALE;

   std::vector<std::string> keys;
   keys.reserve(N);
   for (int i = 0; i < N; ++i)
      keys.push_back(big_endian_key(i));

   // Deterministic shuffle
   std::mt19937 rng(42);
   std::shuffle(keys.begin(), keys.end(), rng);

   run_zip(t, keys, N);
}
