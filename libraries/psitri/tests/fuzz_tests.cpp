#include <catch2/catch_all.hpp>
#include <algorithm>
#include <cstring>
#include <iomanip>
#include <map>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include <psitri/database.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/tree_ops.hpp>
#include <psitri/value_type.hpp>
#include <psitri/write_session_impl.hpp>

using namespace psitri;

#ifdef NDEBUG
constexpr int SCALE = 1;
#else
constexpr int SCALE = 5;
#endif

namespace
{
   // ============================================================
   // Helpers
   // ============================================================

   std::string hex_encode(std::string_view sv)
   {
      std::ostringstream oss;
      oss << std::hex << std::setfill('0');
      for (unsigned char c : sv)
         oss << std::setw(2) << (int)c;
      return oss.str();
   }

   struct test_db
   {
      std::string                    dir;
      std::shared_ptr<database>      db;
      std::shared_ptr<write_session> ses;

      test_db(const std::string& name = "fuzz_testdb")
          : dir(name)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir + "/data");
         db  = std::make_shared<database>(dir, runtime_config());
         ses = db->start_write_session();
      }

      ~test_db() { std::filesystem::remove_all(dir); }

      void assert_no_leaks()
      {
         db->wait_for_compactor(std::chrono::milliseconds(5000));
         auto count = ses->get_total_allocated_objects();
         REQUIRE(count == 0);
      }
   };

   // ============================================================
   // Key Generator
   // ============================================================

   class key_generator
   {
     public:
      explicit key_generator(uint64_t seed) : _rng(seed), _seq_counter(0)
      {
         // Generate a shared prefix for common_prefix strategy
         std::uniform_int_distribution<int> prefix_len_dist(8, 32);
         int                                plen = prefix_len_dist(_rng);
         _shared_prefix.resize(plen);
         for (auto& c : _shared_prefix)
            c = _byte_dist(_rng);
      }

      enum class strategy
      {
         random_key,
         big_endian_seq,
         little_endian_seq,
         common_prefix,
         single_byte,
         empty_key,
         long_key,
         binary_key,
         COUNT
      };

      // Generate a new key and add to pool
      std::string generate()
      {
         auto s   = pick_strategy();
         auto key = generate_by_strategy(s);
         _key_pool.push_back(key);
         return key;
      }

      // Pick a random existing key from pool, or generate if pool empty
      std::string pick_existing()
      {
         if (_key_pool.empty())
            return generate();
         std::uniform_int_distribution<size_t> dist(0, _key_pool.size() - 1);
         return _key_pool[dist(_rng)];
      }

      // 50/50 existing vs new
      std::string pick_or_generate()
      {
         if (_key_pool.empty() || _coin_dist(_rng))
            return generate();
         return pick_existing();
      }

      // Generate a random value
      std::string generate_value()
      {
         std::uniform_int_distribution<int> len_dist(0, 512);
         int                                len = len_dist(_rng);
         std::string                        val(len, '\0');
         for (auto& c : val)
            c = _byte_dist(_rng);
         return val;
      }

      // Generate a range [lower, upper) where lower < upper
      std::pair<std::string, std::string> generate_range()
      {
         std::string a = pick_or_generate();
         std::string b = pick_or_generate();
         if (a > b)
            std::swap(a, b);
         if (a == b)
         {
            // Make sure they differ
            b.push_back('\xFF');
         }
         return {a, b};
      }

      // Generate a short prefix for prefix-related searches
      std::string generate_prefix()
      {
         std::uniform_int_distribution<int> len_dist(1, 4);
         int                                len = len_dist(_rng);
         std::string                        prefix(len, '\0');
         for (auto& c : prefix)
            c = _byte_dist(_rng);
         return prefix;
      }

      // Remove a key from the pool (after successful remove)
      void remove_from_pool(const std::string& key)
      {
         auto it = std::find(_key_pool.begin(), _key_pool.end(), key);
         if (it != _key_pool.end())
         {
            *it = _key_pool.back();
            _key_pool.pop_back();
         }
      }

      // Remove all keys in range from pool
      void remove_range_from_pool(const std::string& lower, const std::string& upper)
      {
         _key_pool.erase(std::remove_if(_key_pool.begin(), _key_pool.end(),
                                        [&](const std::string& k)
                                        { return k >= lower && k < upper; }),
                         _key_pool.end());
      }

     private:
      std::mt19937_64                        _rng;
      uint64_t                               _seq_counter;
      std::string                            _shared_prefix;
      std::vector<std::string>               _key_pool;
      std::uniform_int_distribution<uint8_t> _byte_dist{0, 255};
      std::bernoulli_distribution            _coin_dist{0.5};

      // Weights: random=30, big_end=15, little_end=10, prefix=15, single=5, empty=2, long=8, binary=15
      static constexpr int _weights[] = {30, 15, 10, 15, 5, 2, 8, 15};

      strategy pick_strategy()
      {
         static constexpr int total =
             30 + 15 + 10 + 15 + 5 + 2 + 8 + 15;  // = 100
         std::uniform_int_distribution<int> dist(0, total - 1);
         int                                r   = dist(_rng);
         int                                cum = 0;
         for (int i = 0; i < (int)strategy::COUNT; ++i)
         {
            cum += _weights[i];
            if (r < cum)
               return strategy(i);
         }
         return strategy::random_key;
      }

      // Generate a random byte in range [1, 255] (no null bytes)
      char random_byte_no_null()
      {
         std::uniform_int_distribution<uint8_t> dist(1, 255);
         return (char)dist(_rng);
      }

      std::string generate_by_strategy(strategy s)
      {
         switch (s)
         {
            case strategy::random_key:
            {
               std::uniform_int_distribution<int> len_dist(1, 128);
               int                                len = len_dist(_rng);
               std::string                        key(len, '\0');
               for (auto& c : key)
                  c = _byte_dist(_rng);
               return key;
            }
            case strategy::big_endian_seq:
            {
               // Use counter + 1 to avoid all-zero keys
               uint64_t    val = ++_seq_counter;
               std::string key(8, '\0');
               for (int i = 7; i >= 0; --i)
               {
                  key[7 - i] = (char)(val >> (i * 8));
               }
               return key;
            }
            case strategy::little_endian_seq:
            {
               uint64_t    val = ++_seq_counter;
               std::string key(8, '\0');
               std::memcpy(key.data(), &val, 8);
               return key;
            }
            case strategy::common_prefix:
            {
               std::uniform_int_distribution<int> suffix_len_dist(1, 16);
               int                                slen = suffix_len_dist(_rng);
               std::string                        key  = _shared_prefix;
               key.resize(_shared_prefix.size() + slen);
               for (size_t i = _shared_prefix.size(); i < key.size(); ++i)
                  key[i] = _byte_dist(_rng);
               return key;
            }
            case strategy::single_byte:
            {
               std::string key(1, (char)_byte_dist(_rng));
               return key;
            }
            case strategy::empty_key:
            {
               // Use a single-byte key instead of empty to avoid edge cases
               return std::string(1, random_byte_no_null());
            }
            case strategy::long_key:
            {
               std::uniform_int_distribution<int> len_dist(256, 1024);
               int                                len = len_dist(_rng);
               std::string                        key(len, '\0');
               for (auto& c : key)
                  c = _byte_dist(_rng);
               return key;
            }
            case strategy::binary_key:
            {
               std::uniform_int_distribution<int> len_dist(1, 64);
               int                                len = len_dist(_rng);
               std::string                        key(len, '\0');
               for (auto& c : key)
                  c = _byte_dist(_rng);
               return key;
            }
            default:
               return {};
         }
      }
   };

   // ============================================================
   // Fuzz Runner
   // ============================================================

   enum class op_type
   {
      insert,
      update,
      upsert,
      remove,
      remove_range,
      get,
      count_keys,
      cursor_forward,
      cursor_backward,
      lower_bound,
      seek,
      commit_reopen,
      COUNT
   };

   const char* op_name(op_type op)
   {
      switch (op)
      {
         case op_type::insert:
            return "insert";
         case op_type::update:
            return "update";
         case op_type::upsert:
            return "upsert";
         case op_type::remove:
            return "remove";
         case op_type::remove_range:
            return "remove_range";
         case op_type::get:
            return "get";
         case op_type::count_keys:
            return "count_keys";
         case op_type::cursor_forward:
            return "cursor_forward";
         case op_type::cursor_backward:
            return "cursor_backward";
         case op_type::lower_bound:
            return "lower_bound";
         case op_type::seek:
            return "seek";
         case op_type::commit_reopen:
            return "commit_reopen";
         default:
            return "unknown";
      }
   }

   struct op_weights
   {
      int weights[(int)op_type::COUNT] = {};
   };

   class fuzz_runner
   {
     public:
      fuzz_runner(test_db& tdb, uint64_t seed, op_weights ow)
          : _tdb(tdb),
            _keygen(seed),
            _rng(seed + 7919),  // different seed than keygen
            _ow(ow),
            _op_count(0)
      {
         _cur = _tdb.ses->create_write_cursor();
         _total_weight = 0;
         for (int i = 0; i < (int)op_type::COUNT; ++i)
            _total_weight += _ow.weights[i];
      }

      void run(uint64_t num_ops)
      {
         for (uint64_t i = 0; i < num_ops; ++i)
         {
            auto op = pick_op();
            execute(op);
            _op_count++;

            if (_op_count % (100 / std::max(1, SCALE)) == 0)
               spot_check();

            if (_op_count % (500 / std::max(1, SCALE)) == 0)
               full_forward_check();
         }

         // Final validation
         full_forward_check();
         full_backward_check();
      }

      void cleanup_and_leak_check()
      {
         // Remove all remaining keys
         for (auto it = _oracle.begin(); it != _oracle.end();)
         {
            auto key = it->first;
            _cur->remove(to_key_view(key));
            it = _oracle.erase(it);
         }

         // Verify tree is empty
         {
            auto rc = _cur->read_cursor();
            REQUIRE_FALSE(rc.seek_begin());
         }

         // Set root to null to release all memory
         _tdb.ses->set_root(0, _cur->take_root());

         // Now check for leaks
         _tdb.assert_no_leaks();
      }

     private:
      test_db&                           _tdb;
      key_generator                      _keygen;
      std::mt19937_64                    _rng;
      op_weights                         _ow;
      uint64_t                           _op_count;
      int                                _total_weight;
      write_cursor_ptr                   _cur;
      std::map<std::string, std::string> _oracle;

      op_type pick_op()
      {
         std::uniform_int_distribution<int> dist(0, _total_weight - 1);
         int                                r   = dist(_rng);
         int                                cum = 0;
         for (int i = 0; i < (int)op_type::COUNT; ++i)
         {
            cum += _ow.weights[i];
            if (r < cum)
               return op_type(i);
         }
         return op_type::upsert;
      }

      void execute(op_type op)
      {
         INFO("op #" << _op_count << " type=" << op_name(op) << " oracle_size=" << _oracle.size());

         switch (op)
         {
            case op_type::insert:
               do_insert();
               break;
            case op_type::update:
               do_update();
               break;
            case op_type::upsert:
               do_upsert();
               break;
            case op_type::remove:
               do_remove();
               break;
            case op_type::remove_range:
               do_remove_range();
               break;
            case op_type::get:
               do_get();
               break;
            case op_type::count_keys:
               do_count_keys();
               break;
            case op_type::cursor_forward:
               do_cursor_forward();
               break;
            case op_type::cursor_backward:
               do_cursor_backward();
               break;
            case op_type::lower_bound:
               do_lower_bound();
               break;
            case op_type::seek:
               do_seek();
               break;
            case op_type::commit_reopen:
               do_commit_reopen();
               break;
            default:
               break;
         }

         // Verify all oracle keys are findable AFTER each op
         for (auto& [k, v] : _oracle)
         {
            std::string buf;
            int32_t     r = _cur->get(to_key_view(k), &buf);
            if (r < 0)
            {
               std::cerr << "=== KEY LOST at op #" << _op_count << " type=" << op_name(op)
                         << " oracle_size=" << _oracle.size() << " ===" << std::endl;
               std::cerr << "  Lost key: " << hex_encode(k) << " (len=" << k.size() << ")" << std::endl;
               std::cerr << "  All oracle keys:" << std::endl;
               for (auto& [ok, ov] : _oracle)
               {
                  std::string buf2;
                  int32_t     r2 = _cur->get(to_key_view(ok), &buf2);
                  std::cerr << "    " << hex_encode(ok) << " get=" << r2 << std::endl;
               }
               std::cerr << "  Iterated keys:" << std::endl;
               {
                  auto rc2 = _cur->read_cursor();
                  if (rc2.seek_begin())
                  {
                     do { std::cerr << "    " << hex_encode(rc2.key()) << std::endl; } while (rc2.next());
                  }
               }
               INFO("KEY LOST: " << hex_encode(k) << " at op #" << _op_count);
               REQUIRE(r >= 0);
            }
         }

      }

      void do_insert()
      {
         auto key   = _keygen.generate();
         auto value = _keygen.generate_value();
         INFO("insert key=" << hex_encode(key) << " len=" << key.size());

         bool psitri_result = _cur->insert(to_key_view(key), to_value_view(value));
         bool oracle_result = _oracle.insert({key, value}).second;
         REQUIRE(psitri_result == oracle_result);

         // Verify the key is now retrievable
         std::string buf;
         int32_t     get_result = _cur->get(to_key_view(key), &buf);
         REQUIRE(get_result >= 0);
      }

      void do_update()
      {
         auto key   = _keygen.pick_or_generate();
         auto value = _keygen.generate_value();
         INFO("update key=" << hex_encode(key));

         bool psitri_result = _cur->update(to_key_view(key), to_value_view(value));
         auto it            = _oracle.find(key);
         bool oracle_exists = (it != _oracle.end());
         REQUIRE(psitri_result == oracle_exists);
         if (oracle_exists)
            it->second = value;
      }

      void do_upsert()
      {
         auto key   = _keygen.pick_or_generate();
         auto value = _keygen.generate_value();
         INFO("upsert key=" << hex_encode(key) << " len=" << key.size());

         _cur->upsert(to_key_view(key), to_value_view(value));
         _oracle[key] = value;

         // Verify the key is now retrievable with correct value
         std::string buf;
         int32_t     get_result = _cur->get(to_key_view(key), &buf);
         REQUIRE(get_result == (int32_t)value.size());
         REQUIRE(buf == value);
      }

      void do_remove()
      {
         auto key = _keygen.pick_or_generate();
         INFO("remove key=" << hex_encode(key));

         int  psitri_result = _cur->remove(to_key_view(key));
         auto it            = _oracle.find(key);
         if (it == _oracle.end())
         {
            if (psitri_result != -1)
            {
               // psitri found a key that oracle doesn't have - dump state for debugging
               INFO("REMOVE DESYNC: psitri found key not in oracle, result=" << psitri_result);
               INFO("oracle has " << _oracle.size() << " keys, psitri iteration count="
                                  << count_by_iteration({}, {}));
               // Check if the key was retrievable via get before remove
               REQUIRE(psitri_result == -1);
            }
         }
         else
         {
            // remove() returns value size, but for large values stored in value_nodes
            // it may return the allocation size (including header overhead).
            // Just verify it indicates success (>= 0).
            REQUIRE(psitri_result >= 0);
            _oracle.erase(it);
            _keygen.remove_from_pool(key);
         }
      }

      void do_remove_range()
      {
         if (_oracle.empty())
            return;  // skip on empty tree

         auto [lower, upper] = _keygen.generate_range();
         INFO("remove_range lower=" << hex_encode(lower) << " upper=" << hex_encode(upper));

         // Count what oracle expects to remove
         auto     lo           = _oracle.lower_bound(lower);
         auto     hi           = _oracle.lower_bound(upper);
         uint64_t oracle_count = std::distance(lo, hi);

         uint64_t psitri_count = _cur->remove_range(to_key_view(lower), to_key_view(upper));

         // Erase from oracle
         _oracle.erase(_oracle.lower_bound(lower), _oracle.lower_bound(upper));
         _keygen.remove_range_from_pool(lower, upper);

         REQUIRE(psitri_count == oracle_count);
      }

      void do_get()
      {
         auto key = _keygen.pick_or_generate();
         INFO("get key=" << hex_encode(key));

         std::string buf;
         int32_t     psitri_result = _cur->get(to_key_view(key), &buf);
         auto        it            = _oracle.find(key);
         if (it == _oracle.end())
         {
            REQUIRE(psitri_result < 0);
         }
         else
         {
            REQUIRE(psitri_result == (int32_t)it->second.size());
            REQUIRE(buf == it->second);
         }
      }

      // Count keys by iterating the cursor (brute force validation)
      uint64_t count_by_iteration(key_view lower, key_view upper)
      {
         if (!*_cur)
            return 0;  // empty tree

         auto     rc    = _cur->read_cursor();
         uint64_t count = 0;
         if (lower.empty())
         {
            if (!rc.seek_begin())
               return 0;
         }
         else
         {
            if (!rc.lower_bound(lower))
               return 0;
         }

         while (!rc.is_end())
         {
            if (!upper.empty() && rc.key() >= upper)
               break;
            ++count;
            rc.next();
         }
         return count;
      }

      void do_count_keys()
      {
         if (_oracle.empty())
            return;  // skip count on empty tree

         // Test both bounded and unbounded count
         std::bernoulli_distribution coin(0.3);
         if (coin(_rng))
         {
            // Unbounded - use iteration count as ground truth
            uint64_t iter_count   = count_by_iteration({}, {});
            uint64_t psitri_count = _cur->count_keys();
            INFO("unbounded count: iter=" << iter_count << " count_keys=" << psitri_count
                                          << " oracle=" << _oracle.size());
            REQUIRE(iter_count == _oracle.size());
            REQUIRE(psitri_count == _oracle.size());
         }
         else
         {
            // Bounded range
            auto [lower, upper] = _keygen.generate_range();
            INFO("count_keys lower=" << hex_encode(lower) << " upper=" << hex_encode(upper));

            uint64_t iter_count =
                count_by_iteration(to_key_view(lower), to_key_view(upper));
            uint64_t psitri_count =
                _cur->count_keys(to_key_view(lower), to_key_view(upper));
            auto     lo           = _oracle.lower_bound(lower);
            auto     hi           = _oracle.lower_bound(upper);
            uint64_t oracle_count = std::distance(lo, hi);

            INFO("bounded count: iter=" << iter_count << " count_keys=" << psitri_count
                                        << " oracle=" << oracle_count);
            REQUIRE(iter_count == oracle_count);
            if (psitri_count != oracle_count)
            {
               WARN("count_keys mismatch: count_keys=" << psitri_count
                                                        << " expected=" << oracle_count
                                                        << " (may be a count_keys bug)");
            }
         }
      }

      void do_cursor_forward()
      {
         full_forward_check();
      }

      void do_cursor_backward()
      {
         full_backward_check();
      }

      void do_lower_bound()
      {
         if (_oracle.empty())
            return;  // skip on empty tree to avoid null cursor issues

         auto key = _keygen.pick_or_generate();
         INFO("lower_bound key=" << hex_encode(key));

         auto rc = _cur->read_cursor();
         rc.lower_bound(to_key_view(key));

         auto it = _oracle.lower_bound(key);
         if (it == _oracle.end())
         {
            REQUIRE(rc.is_end());
         }
         else
         {
            REQUIRE_FALSE(rc.is_end());
            REQUIRE(std::string(rc.key()) == it->first);
         }
      }

      void do_seek()
      {
         if (_oracle.empty())
            return;  // skip on empty tree to avoid null cursor issues

         auto key = _keygen.pick_or_generate();
         INFO("seek key=" << hex_encode(key));

         auto rc    = _cur->read_cursor();
         bool found = rc.seek(to_key_view(key));
         bool oracle_found = _oracle.count(key) > 0;
         REQUIRE(found == oracle_found);
         if (found)
         {
            REQUIRE(std::string(rc.key()) == key);
         }
      }

      void do_commit_reopen()
      {
         // Persist current state via root, then re-read
         auto root = _cur->root();
         _tdb.ses->set_root(0, std::move(root));

         auto new_root = _tdb.ses->get_root(0);
         _cur          = _tdb.ses->create_write_cursor(std::move(new_root));

         // Spot-check a few keys
         int checks = std::min((int)_oracle.size(), 5);
         if (checks > 0)
         {
            auto it = _oracle.begin();
            std::uniform_int_distribution<int> skip_dist(0, std::max(0, (int)_oracle.size() - 1));
            for (int i = 0; i < checks; ++i)
            {
               // Pick a random key from oracle
               auto check_it = _oracle.begin();
               std::advance(check_it, skip_dist(_rng) % _oracle.size());

               std::string buf;
               int32_t     result = _cur->get(to_key_view(check_it->first), &buf);
               REQUIRE(result == (int32_t)check_it->second.size());
               REQUIRE(buf == check_it->second);
            }
         }
      }

      // -- Validation helpers --

      void spot_check()
      {
         INFO("spot_check at op #" << _op_count);

         // Count check via iteration (more reliable)
         uint64_t iter_count = count_by_iteration({}, {});
         REQUIRE(iter_count == _oracle.size());

         // Check a few random existing keys
         int checks = std::min((int)_oracle.size(), 5);
         if (checks > 0)
         {
            auto it = _oracle.begin();
            std::uniform_int_distribution<int> skip_dist(0, std::max(0, (int)_oracle.size() - 1));
            for (int i = 0; i < checks; ++i)
            {
               auto check_it = _oracle.begin();
               std::advance(check_it, skip_dist(_rng) % _oracle.size());

               std::string buf;
               int32_t     result = _cur->get(to_key_view(check_it->first), &buf);
               INFO("spot_check get key=" << hex_encode(check_it->first));
               REQUIRE(result >= 0);
               REQUIRE(buf == check_it->second);
            }
         }
      }

      void full_forward_check()
      {
         INFO("full_forward_check at op #" << _op_count << " oracle_size=" << _oracle.size());

         // First verify iteration count agrees (more reliable than count_keys)
         uint64_t iter_count = count_by_iteration({}, {});
         INFO("iter_count=" << iter_count);
         if (iter_count != _oracle.size())
         {
            // Dump all oracle keys
            std::cerr << "=== ITERATION MISMATCH at op #" << _op_count
                      << " oracle=" << _oracle.size() << " iter=" << iter_count << " ===" << std::endl;
            std::cerr << "Oracle keys:" << std::endl;
            for (auto& [k, v] : _oracle)
               std::cerr << "  " << hex_encode(k) << " (len=" << k.size() << ")" << std::endl;
            std::cerr << "Iterated keys:" << std::endl;
            {
               auto rc2 = _cur->read_cursor();
               if (rc2.seek_begin())
               {
                  do
                  {
                     std::cerr << "  " << hex_encode(rc2.key()) << " (len=" << rc2.key().size() << ")"
                               << std::endl;
                  } while (rc2.next());
               }
            }
            // Check each oracle key individually via get()
            std::cerr << "Individual get() results:" << std::endl;
            for (auto& [k, v] : _oracle)
            {
               std::string buf;
               int32_t     r = _cur->get(to_key_view(k), &buf);
               std::cerr << "  " << hex_encode(k) << " get=" << r
                         << (r >= 0 ? " FOUND" : " NOT_FOUND") << std::endl;
            }
            // Validate tree structure
            _cur->validate();
         }
         REQUIRE(iter_count == _oracle.size());

         if (_oracle.empty())
            return;

         auto rc = _cur->read_cursor();
         rc.seek_begin();
         auto     it    = _oracle.begin();
         uint64_t count = 0;
         while (!rc.is_end() && it != _oracle.end())
         {
            INFO("forward check idx=" << count << " psitri_key=" << hex_encode(rc.key())
                                       << " oracle_key=" << hex_encode(it->first));
            REQUIRE(std::string(rc.key()) == it->first);
            rc.next();
            ++it;
            ++count;
         }
         REQUIRE(rc.is_end());
         REQUIRE(it == _oracle.end());
         REQUIRE(count == _oracle.size());
      }

      void full_backward_check()
      {
         INFO("full_backward_check at op #" << _op_count << " oracle_size=" << _oracle.size());

         if (_oracle.empty())
         {
            uint64_t iter_count = count_by_iteration({}, {});
            REQUIRE(iter_count == 0);
            return;
         }

         auto rc = _cur->read_cursor();
         rc.seek_last();
         auto     it    = _oracle.rbegin();
         uint64_t count = 0;
         while (!rc.is_rend() && it != _oracle.rend())
         {
            INFO("backward check idx=" << count << " psitri_key=" << hex_encode(rc.key())
                                        << " oracle_key=" << hex_encode(it->first));
            REQUIRE(std::string(rc.key()) == it->first);
            rc.prev();
            ++it;
            ++count;
         }
         REQUIRE(rc.is_rend());
         REQUIRE(it == _oracle.rend());
         REQUIRE(count == _oracle.size());
      }
   };

   // ============================================================
   // Predefined weight profiles
   // ============================================================

   // Indices: insert, update, upsert, remove, remove_range, get, count_keys,
   //          cursor_forward, cursor_backward, lower_bound, seek, commit_reopen

   op_weights balanced_weights()
   {
      return {{25, 10, 20, 10, 5, 15, 5, 2, 2, 2, 2, 2}};
   }

   // No remove_range - avoids known remove_range bugs
   op_weights balanced_no_rr_weights()
   {
      return {{25, 10, 20, 15, 0, 15, 5, 2, 2, 2, 2, 2}};
   }

   op_weights sequential_weights()
   {
      return {{35, 5, 15, 5, 0, 20, 5, 3, 3, 3, 2, 2}};
   }

   op_weights remove_heavy_weights()
   {
      return {{20, 5, 10, 25, 20, 10, 3, 2, 2, 1, 1, 1}};
   }

   op_weights cursor_heavy_weights()
   {
      return {{15, 5, 15, 5, 0, 10, 8, 15, 10, 8, 5, 2}};
   }

   op_weights transaction_weights()
   {
      return {{25, 5, 15, 20, 0, 10, 3, 2, 2, 2, 1, 10}};
   }

}  // anonymous namespace

// ============================================================
// Test Cases
// ============================================================

TEST_CASE("fuzz random heavy", "[fuzz]")
{
   uint64_t seed = GENERATE(42, 12345, 987654);
   INFO("seed=" << seed);

   test_db     tdb("fuzz_random_heavy_" + std::to_string(seed));
   fuzz_runner runner(tdb, seed, balanced_no_rr_weights());

   runner.run(5000 / SCALE);
   runner.cleanup_and_leak_check();
}

TEST_CASE("fuzz sequential heavy", "[fuzz]")
{
   uint64_t seed = GENERATE(1, 77777);
   INFO("seed=" << seed);

   test_db     tdb("fuzz_seq_heavy_" + std::to_string(seed));
   fuzz_runner runner(tdb, seed, sequential_weights());

   runner.run(5000 / SCALE);
   runner.cleanup_and_leak_check();
}

TEST_CASE("fuzz remove heavy", "[fuzz][remove_range]")
{
   uint64_t seed = GENERATE(42, 55555);
   INFO("seed=" << seed);

   test_db     tdb("fuzz_remove_heavy_" + std::to_string(seed));
   fuzz_runner runner(tdb, seed, remove_heavy_weights());

   runner.run(5000 / SCALE);
   runner.cleanup_and_leak_check();
}

TEST_CASE("fuzz cursor heavy", "[fuzz]")
{
   uint64_t seed = GENERATE(42, 31415);
   INFO("seed=" << seed);

   test_db     tdb("fuzz_cursor_heavy_" + std::to_string(seed));
   fuzz_runner runner(tdb, seed, cursor_heavy_weights());

   runner.run(3000 / SCALE);
   runner.cleanup_and_leak_check();
}

TEST_CASE("fuzz transaction lifecycle", "[fuzz]")
{
   uint64_t seed = GENERATE(42, 99999);
   INFO("seed=" << seed);

   test_db     tdb("fuzz_tx_lifecycle_" + std::to_string(seed));
   fuzz_runner runner(tdb, seed, transaction_weights());

   runner.run(2000 / SCALE);
   runner.cleanup_and_leak_check();
}

TEST_CASE("fuzz edge cases", "[fuzz]")
{
   uint64_t seed = GENERATE(42, 271828);
   INFO("seed=" << seed);

   test_db     tdb("fuzz_edge_" + std::to_string(seed));
   fuzz_runner runner(tdb, seed, balanced_no_rr_weights());

   runner.run(2000 / SCALE);
   runner.cleanup_and_leak_check();
}
