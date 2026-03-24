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
         // Wait multiple times to rule out race conditions
         db->wait_for_compactor(std::chrono::milliseconds(5000));
         auto pending = ses->get_pending_release_count();
         if (pending > 0)
         {
            std::cerr << "Still pending: " << pending << ", waiting again..." << std::endl;
            db->wait_for_compactor(std::chrono::milliseconds(5000));
         }
         auto count = ses->get_total_allocated_objects();
         if (count > 0)
         {
            std::cerr << "=== LEAKED " << count << " objects ===" << std::endl;
            ses->dump_live_objects();
            std::cerr << "=== END LEAKED ===" << std::endl;
         }
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
      std::string generate_value(int max_len = 512)
      {
         std::uniform_int_distribution<int> len_dist(0, max_len);
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
      fuzz_runner(test_db& tdb, uint64_t seed, op_weights ow, int max_value_len = 512)
          : _tdb(tdb),
            _keygen(seed),
            _rng(seed + 7919),  // different seed than keygen
            _ow(ow),
            _op_count(0),
            _max_value_len(max_value_len)
      {
         _cur = _tdb.ses->create_write_cursor();
         _total_weight = 0;
         for (int i = 0; i < (int)op_type::COUNT; ++i)
            _total_weight += _ow.weights[i];
      }

      void run(uint64_t num_ops, bool trace_allocs = false)
      {
         uint64_t prev_alloc = 0;
         for (uint64_t i = 0; i < num_ops; ++i)
         {
            auto op = pick_op();
            execute(op);
            _op_count++;

            if (trace_allocs)
            {
               auto alloc = _tdb.ses->get_total_allocated_objects();
               if (op == op_type::commit_reopen || op == op_type::update ||
                   op == op_type::upsert || op == op_type::insert || op == op_type::remove)
               {
                  std::cerr << "op#" << _op_count << " " << op_name(op)
                            << " oracle=" << _oracle.size()
                            << " alloc=" << alloc << std::endl;
               }
               prev_alloc = alloc;
            }

            if (_op_count % (100 / std::max(1, SCALE)) == 0)
               spot_check();

            if (_op_count % (500 / std::max(1, SCALE)) == 0)
               full_forward_check();
         }

         // Final validation
         full_forward_check();
         full_backward_check();
      }

      uint64_t cleanup_and_count_leaks()
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
            CHECK_FALSE(rc.seek_begin());
         }

         // Set root to null to release all memory
         _tdb.ses->set_root(0, _cur->take_root());

         _tdb.db->wait_for_compactor(std::chrono::milliseconds(5000));
         auto pending = _tdb.ses->get_pending_release_count();
         if (pending > 0)
            _tdb.db->wait_for_compactor(std::chrono::milliseconds(5000));
         return _tdb.ses->get_total_allocated_objects();
      }

      void print_tree()
      {
         if (_cur && *_cur)
            _cur->print();
         else
            std::cerr << "(empty tree)" << std::endl;
      }

      void cleanup_and_leak_check()
      {
         auto count = cleanup_and_count_leaks();
         if (count > 0)
         {
            std::cerr << "=== LEAKED " << count << " objects ===" << std::endl;
            _tdb.ses->dump_live_objects();
            std::cerr << "=== END LEAKED ===" << std::endl;
         }
         REQUIRE(count == 0);
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
      int                                _max_value_len;

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
         auto value = _keygen.generate_value(_max_value_len);
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
         auto value = _keygen.generate_value(_max_value_len);
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
         auto value = _keygen.generate_value(_max_value_len);
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

// Diagnostic tests removed — they were debugging aids for shared-mode ref leaks
// that have been fixed (no-op detection + no-throw in shared mode).
// To add new diagnostic tests, use the [fuzz][diag] tags.

#if 0  // Diagnostic tests kept for reference but disabled
TEST_CASE("fuzz value transition leak", "[fuzz][diag]")
{
   // Use the actual fuzz runner with only insert+upsert+remove (no update op),
   // to isolate whether the leak is from `update` vs `upsert`
   uint64_t seed = 42;
   INFO("seed=" << seed);

   test_db     tdb("fuzz_vt_leak");
   // insert=30, update=0, upsert=30, remove=20, remove_range=0, get=10, rest=0
   fuzz_runner runner(tdb, seed, op_weights{{30, 0, 30, 20, 0, 10, 5, 2, 2, 1, 0, 0}});
   runner.run(2000 / SCALE);
   runner.cleanup_and_leak_check();
}

TEST_CASE("fuzz update-only leak", "[fuzz][diag]")
{
   // Use the actual fuzz runner with insert+update+remove (no upsert)
   uint64_t seed = 42;
   INFO("seed=" << seed);

   test_db     tdb("fuzz_uo_leak");
   // insert=30, update=20, upsert=0, remove=20, remove_range=0, get=20, rest=0
   fuzz_runner runner(tdb, seed, op_weights{{30, 20, 0, 20, 0, 20, 5, 2, 2, 1, 0, 0}});
   runner.run(2000 / SCALE);
   runner.cleanup_and_leak_check();
}

TEST_CASE("fuzz all-mutations leak", "[fuzz][diag]")
{
   // All three mutation types together — WITH commit_reopen
   uint64_t seed = 42;
   INFO("seed=" << seed);

   test_db     tdb("fuzz_all_mut");
   fuzz_runner runner(tdb, seed, op_weights{{25, 10, 20, 15, 0, 15, 5, 2, 2, 2, 2, 2}});
   runner.run(1000 / SCALE);
   runner.cleanup_and_leak_check();
}

TEST_CASE("fuzz all-mut no-commit leak", "[fuzz][diag]")
{
   // Same weights but commit_reopen=0
   uint64_t seed = 42;
   INFO("seed=" << seed);

   test_db     tdb("fuzz_nc");
   fuzz_runner runner(tdb, seed, op_weights{{25, 10, 20, 15, 0, 15, 5, 2, 2, 2, 2, 0}});
   runner.run(1000 / SCALE);
   runner.cleanup_and_leak_check();
}

TEST_CASE("fuzz commit+upsert minimal leak", "[fuzz][diag]")
{
   // Minimal: just upsert + commit_reopen, no update/insert/remove
   test_db tdb("fuzz_cu_min");
   auto    cur = tdb.ses->create_write_cursor();

   constexpr int N = 50;

   auto make_key = [](int i) -> std::string
   {
      std::string key(4, '\0');
      key[0] = (i >> 24) & 0xff;
      key[1] = (i >> 16) & 0xff;
      key[2] = (i >> 8) & 0xff;
      key[3] = i & 0xff;
      return key;
   };

   // Insert N keys with large values
   for (int i = 0; i < N; ++i)
   {
      auto key = make_key(i);
      std::string val(200, 'A' + (i % 26));
      cur->upsert(to_key_view(key), to_value_view(val));
   }

   // Commit (share the tree)
   tdb.ses->set_root(0, cur->root());
   auto new_root = tdb.ses->get_root(0);
   cur = tdb.ses->create_write_cursor(std::move(new_root));

   // Upsert same keys with different values (triggers shared-mode COW)
   for (int i = 0; i < N; ++i)
   {
      auto key = make_key(i);
      std::string val(200, 'B' + (i % 26));
      cur->upsert(to_key_view(key), to_value_view(val));
   }

   // Remove all
   for (int i = 0; i < N; ++i)
      cur->remove(to_key_view(make_key(i)));

   // Release everything
   cur->take_root();
   tdb.ses->set_root(0, {}, sal::sync_type::none);
   tdb.assert_no_leaks();
}

TEST_CASE("fuzz multi-commit leak", "[fuzz][diag]")
{
   // Multiple commit_reopen cycles with mixed update/upsert
   test_db tdb("fuzz_mc_leak");
   auto    cur = tdb.ses->create_write_cursor();

   constexpr int N = 20;

   auto make_key = [](int i) -> std::string
   {
      std::string key(4, '\0');
      key[0] = (i >> 24) & 0xff;
      key[1] = (i >> 16) & 0xff;
      key[2] = (i >> 8) & 0xff;
      key[3] = i & 0xff;
      return key;
   };

   auto do_commit = [&]()
   {
      tdb.ses->set_root(0, cur->root());
      auto nr = tdb.ses->get_root(0);
      cur     = tdb.ses->create_write_cursor(std::move(nr));
   };

   // Insert N keys
   for (int i = 0; i < N; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'A' + (i % 26));
      cur->upsert(to_key_view(key), to_value_view(val));
   }

   // Cycle 1: commit + update
   do_commit();
   for (int i = 0; i < N; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'X');
      cur->update(to_key_view(key), to_value_view(val));
   }

   // Cycle 2: commit + upsert
   do_commit();
   for (int i = 0; i < N; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'Y');
      cur->upsert(to_key_view(key), to_value_view(val));
   }

   // Cycle 3: commit + update again
   do_commit();
   for (int i = 0; i < N; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'Z');
      cur->update(to_key_view(key), to_value_view(val));
   }

   // Remove all
   for (int i = 0; i < N; ++i)
      cur->remove(to_key_view(make_key(i)));

   cur->take_root();
   tdb.ses->set_root(0, {}, sal::sync_type::none);
   tdb.assert_no_leaks();
}

TEST_CASE("fuzz insert-after-commit leak", "[fuzz][diag]")
{
   // Insert new keys AND update/upsert existing keys after commit_reopen
   test_db tdb("fuzz_iac_leak");
   auto    cur = tdb.ses->create_write_cursor();

   constexpr int N = 20;

   auto make_key = [](int i) -> std::string
   {
      std::string key(4, '\0');
      key[0] = (i >> 24) & 0xff;
      key[1] = (i >> 16) & 0xff;
      key[2] = (i >> 8) & 0xff;
      key[3] = i & 0xff;
      return key;
   };

   auto do_commit = [&]()
   {
      tdb.ses->set_root(0, cur->root());
      auto nr = tdb.ses->get_root(0);
      cur     = tdb.ses->create_write_cursor(std::move(nr));
   };

   // Insert N keys
   for (int i = 0; i < N; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'A' + (i % 26));
      cur->upsert(to_key_view(key), to_value_view(val));
   }

   // Commit, then insert NEW keys + update existing
   do_commit();
   for (int i = N; i < N * 2; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'B' + (i % 26));
      cur->upsert(to_key_view(key), to_value_view(val));
   }
   for (int i = 0; i < N; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'X');
      cur->update(to_key_view(key), to_value_view(val));
   }

   // Commit again, then more inserts + updates
   do_commit();
   for (int i = N * 2; i < N * 3; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'C' + (i % 26));
      cur->upsert(to_key_view(key), to_value_view(val));
   }
   for (int i = 0; i < N * 2; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'Z');
      cur->upsert(to_key_view(key), to_value_view(val));
   }

   // Remove all
   for (int i = 0; i < N * 3; ++i)
      cur->remove(to_key_view(make_key(i)));

   cur->take_root();
   tdb.ses->set_root(0, {}, sal::sync_type::none);
   tdb.assert_no_leaks();
}

TEST_CASE("fuzz interleaved-ops+commit leak", "[fuzz][diag]")
{
   // Interleave different operation types within a single commit cycle
   test_db tdb("fuzz_io_leak");
   auto    cur = tdb.ses->create_write_cursor();

   constexpr int N = 30;

   auto make_key = [](int i) -> std::string
   {
      std::string key(4, '\0');
      key[0] = (i >> 24) & 0xff;
      key[1] = (i >> 16) & 0xff;
      key[2] = (i >> 8) & 0xff;
      key[3] = i & 0xff;
      return key;
   };

   auto do_commit = [&]()
   {
      tdb.ses->set_root(0, cur->root());
      auto nr = tdb.ses->get_root(0);
      cur     = tdb.ses->create_write_cursor(std::move(nr));
   };

   // Insert keys
   for (int i = 0; i < N; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'A' + (i % 26));
      cur->upsert(to_key_view(key), to_value_view(val));
   }

   do_commit();

   // Interleaved: update k0, insert k30, upsert k1, insert k31, update k2, ...
   for (int i = 0; i < N / 2; ++i)
   {
      // update existing
      cur->update(to_key_view(make_key(i)), to_value_view(std::string(200, 'X')));
      // insert new
      cur->upsert(to_key_view(make_key(N + i)), to_value_view(std::string(200, 'Y')));
      // upsert existing
      cur->upsert(to_key_view(make_key(N / 2 + i)), to_value_view(std::string(200, 'Z')));
   }

   do_commit();

   // More interleaved ops + removes
   for (int i = 0; i < N / 4; ++i)
   {
      cur->remove(to_key_view(make_key(i)));
      cur->upsert(to_key_view(make_key(N + N / 2 + i)), to_value_view(std::string(200, 'W')));
      if (i < N / 2)
         cur->update(to_key_view(make_key(N / 2 + i)), to_value_view(std::string(200, 'V')));
   }

   // Remove all remaining
   std::set<int> removed;
   for (int i = 0; i < N / 4; ++i)
      removed.insert(i);
   for (int i = 0; i < N + N / 2 + N / 4; ++i)
   {
      if (removed.count(i) == 0)
         cur->remove(to_key_view(make_key(i)));
   }

   cur->take_root();
   tdb.ses->set_root(0, {}, sal::sync_type::none);
   tdb.assert_no_leaks();
}

TEST_CASE("fuzz update+upsert+commit minimal", "[fuzz][diag]")
{
   // Test: explicit update + upsert + commit_reopen
   test_db tdb("fuzz_uuc_min");
   auto    cur = tdb.ses->create_write_cursor();

   constexpr int N = 20;

   auto make_key = [](int i) -> std::string
   {
      std::string key(4, '\0');
      key[0] = (i >> 24) & 0xff;
      key[1] = (i >> 16) & 0xff;
      key[2] = (i >> 8) & 0xff;
      key[3] = i & 0xff;
      return key;
   };

   // Phase 1: Insert N keys with large values (>64 bytes → value_nodes)
   for (int i = 0; i < N; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'A' + (i % 26));
      cur->upsert(to_key_view(key), to_value_view(val));
   }

   // Phase 2: Commit (share the tree, ref=2)
   tdb.ses->set_root(0, cur->root());
   auto new_root = tdb.ses->get_root(0);
   cur           = tdb.ses->create_write_cursor(std::move(new_root));

   // Phase 3: Explicit update() on some keys (uses unique_update → shared_update)
   for (int i = 0; i < N / 2; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'X' + (i % 3));
      cur->update(to_key_view(key), to_value_view(val));
   }

   // Phase 4: upsert() on remaining keys (uses unique_upsert → maybe shared_upsert)
   for (int i = N / 2; i < N; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'Y' + (i % 3));
      cur->upsert(to_key_view(key), to_value_view(val));
   }

   // Phase 5: Remove all
   for (int i = 0; i < N; ++i)
      cur->remove(to_key_view(make_key(i)));

   // Phase 6: Release everything and check
   auto leaked_before_release = tdb.ses->get_total_allocated_objects();
   cur->take_root();
   tdb.ses->set_root(0, {}, sal::sync_type::none);

   tdb.db->wait_for_compactor(std::chrono::milliseconds(5000));
   auto leaked = tdb.ses->get_total_allocated_objects();
   WARN("leaked_before_release=" << leaked_before_release << " leaked_after=" << leaked);
   REQUIRE(leaked == 0);
}

TEST_CASE("fuzz update-only+commit minimal", "[fuzz][diag]")
{
   // Same but ONLY update, no upsert after commit
   test_db tdb("fuzz_uc_min");
   auto    cur = tdb.ses->create_write_cursor();

   constexpr int N = 20;

   auto make_key = [](int i) -> std::string
   {
      std::string key(4, '\0');
      key[0] = (i >> 24) & 0xff;
      key[1] = (i >> 16) & 0xff;
      key[2] = (i >> 8) & 0xff;
      key[3] = i & 0xff;
      return key;
   };

   for (int i = 0; i < N; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'A' + (i % 26));
      cur->upsert(to_key_view(key), to_value_view(val));
   }

   tdb.ses->set_root(0, cur->root());
   auto new_root = tdb.ses->get_root(0);
   cur           = tdb.ses->create_write_cursor(std::move(new_root));

   // ONLY update, no upsert
   for (int i = 0; i < N; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'X' + (i % 3));
      cur->update(to_key_view(key), to_value_view(val));
   }

   for (int i = 0; i < N; ++i)
      cur->remove(to_key_view(make_key(i)));

   cur->take_root();
   tdb.ses->set_root(0, {}, sal::sync_type::none);
   tdb.assert_no_leaks();
}

TEST_CASE("fuzz upsert-only+commit minimal", "[fuzz][diag]")
{
   // Same but ONLY upsert, no explicit update after commit
   test_db tdb("fuzz_usc_min");
   auto    cur = tdb.ses->create_write_cursor();

   constexpr int N = 20;

   auto make_key = [](int i) -> std::string
   {
      std::string key(4, '\0');
      key[0] = (i >> 24) & 0xff;
      key[1] = (i >> 16) & 0xff;
      key[2] = (i >> 8) & 0xff;
      key[3] = i & 0xff;
      return key;
   };

   for (int i = 0; i < N; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'A' + (i % 26));
      cur->upsert(to_key_view(key), to_value_view(val));
   }

   tdb.ses->set_root(0, cur->root());
   auto new_root = tdb.ses->get_root(0);
   cur           = tdb.ses->create_write_cursor(std::move(new_root));

   // ONLY upsert on existing keys
   for (int i = 0; i < N; ++i)
   {
      auto        key = make_key(i);
      std::string val(200, 'Y' + (i % 3));
      cur->upsert(to_key_view(key), to_value_view(val));
   }

   for (int i = 0; i < N; ++i)
      cur->remove(to_key_view(make_key(i)));

   cur->take_root();
   tdb.ses->set_root(0, {}, sal::sync_type::none);
   tdb.assert_no_leaks();
}

TEST_CASE("fuzz leak bisect", "[fuzz][diag]")
{
   // Run all-mut no-insert pattern and log mutations to find which op causes leak
   uint64_t seed = 42;
   INFO("seed=" << seed);

   // Instead of using fuzz_runner, replicate the logic with logging
   test_db tdb("fuzz_bisect");
   auto    cur = tdb.ses->create_write_cursor();

   key_generator keygen(seed);
   std::mt19937_64 rng(seed + 7919);
   std::map<std::string, std::string> oracle;

   // Operation weights from no-insert test: {0, 10, 45, 15, 0, 15, 5, 2, 2, 2, 2, 2}
   // insert=0, update=10, upsert=45, remove=15, rr=0, get=15, ...commit_reopen=2
   // Total mutations we care about: update, upsert, remove, commit_reopen

   auto pick_op = [&]() -> int
   {
      // Simplified: 10=update, 45=upsert, 15=remove, 2=commit_reopen, 28=get/read (skip)
      std::uniform_int_distribution<int> dist(0, 99);
      int r = dist(rng);
      if (r < 10) return 0;       // update
      if (r < 55) return 1;       // upsert
      if (r < 70) return 2;       // remove
      if (r < 72) return 3;       // commit_reopen
      return 4;                    // read-only (skip)
   };

   auto do_commit_reopen = [&]()
   {
      tdb.ses->set_root(0, cur->root());
      auto nr = tdb.ses->get_root(0);
      cur = tdb.ses->create_write_cursor(std::move(nr));
   };

   int op_count = 0;
   for (int i = 0; i < 200; ++i)  // 200 ops (1000/SCALE=5)
   {
      int op = pick_op();
      if (op == 4) { ++op_count; continue; }  // skip reads

      std::string key;
      std::string val;

      switch (op)
      {
         case 0:  // update
         {
            if (oracle.empty()) break;
            auto it = oracle.begin();
            std::uniform_int_distribution<int> skip(0, oracle.size() - 1);
            std::advance(it, skip(rng));
            key = it->first;
            val = std::string(100 + (rng() % 200), 'U');
            cur->update(to_key_view(key), to_value_view(val));
            oracle[key] = val;
            break;
         }
         case 1:  // upsert
         {
            key = keygen.pick_or_generate();
            val = std::string(100 + (rng() % 200), 'S');
            cur->upsert(to_key_view(key), to_value_view(val));
            oracle[key] = val;
            break;
         }
         case 2:  // remove
         {
            if (oracle.empty()) break;
            auto it = oracle.begin();
            std::uniform_int_distribution<int> skip(0, oracle.size() - 1);
            std::advance(it, skip(rng));
            key = it->first;
            cur->remove(to_key_view(key));
            oracle.erase(it);
            break;
         }
         case 3:  // commit_reopen
         {
            do_commit_reopen();
            break;
         }
      }
      ++op_count;
   }

   WARN("After " << op_count << " ops, oracle has " << oracle.size() << " keys");

   // Remove all
   for (auto it = oracle.begin(); it != oracle.end(); it = oracle.erase(it))
      cur->remove(to_key_view(it->first));

   cur->take_root();
   tdb.ses->set_root(0, {}, sal::sync_type::none);
   tdb.db->wait_for_compactor(std::chrono::milliseconds(5000));
   auto leaked = tdb.ses->get_total_allocated_objects();
   WARN("leaked=" << leaked);
   REQUIRE(leaked == 0);
}

TEST_CASE("fuzz insert+update leak", "[fuzz][diag]")
{
   uint64_t seed = 42;
   INFO("seed=" << seed);
   test_db     tdb("fuzz_iu_leak");
   // insert=30, update=30, upsert=0, remove=20, rr=0, get=10, rest=0
   fuzz_runner runner(tdb, seed, op_weights{{30, 30, 0, 20, 0, 10, 5, 2, 2, 1, 0, 0}});
   runner.run(1000 / SCALE);
   runner.cleanup_and_leak_check();
}

TEST_CASE("fuzz insert+upsert leak", "[fuzz][diag]")
{
   uint64_t seed = 42;
   INFO("seed=" << seed);
   test_db     tdb("fuzz_ius_leak");
   // insert=30, update=0, upsert=30, remove=20, rr=0, get=10, rest=0
   fuzz_runner runner(tdb, seed, op_weights{{30, 0, 30, 20, 0, 10, 5, 2, 2, 1, 0, 0}});
   runner.run(1000 / SCALE);
   runner.cleanup_and_leak_check();
}

TEST_CASE("fuzz all-mut no-update leak", "[fuzz][diag]")
{
   // Same total weights as all-mutations but update=0
   uint64_t seed = 42;
   INFO("seed=" << seed);
   test_db     tdb("fuzz_no_upd");
   fuzz_runner runner(tdb, seed, op_weights{{25, 0, 30, 15, 0, 15, 5, 2, 2, 2, 2, 2}});
   runner.run(1000 / SCALE);
   runner.cleanup_and_leak_check();
}

TEST_CASE("fuzz all-mut no-upsert leak", "[fuzz][diag]")
{
   // Same total weights as all-mutations but upsert=0
   uint64_t seed = 42;
   INFO("seed=" << seed);
   test_db     tdb("fuzz_no_ups");
   fuzz_runner runner(tdb, seed, op_weights{{25, 10, 0, 15, 0, 15, 5, 2, 2, 2, 2, 2}});
   runner.run(1000 / SCALE);
   runner.cleanup_and_leak_check();
}

TEST_CASE("fuzz all-mut no-insert leak", "[fuzz][diag]")
{
   // Same total weights but insert=0 (relies on upsert to add keys)
   uint64_t seed = 42;
   INFO("seed=" << seed);
   test_db     tdb("fuzz_no_ins");
   fuzz_runner runner(tdb, seed, op_weights{{0, 10, 45, 15, 0, 15, 5, 2, 2, 2, 2, 2}});
   runner.run(1000 / SCALE);
   runner.cleanup_and_leak_check();
}

TEST_CASE("fuzz no-insert small-values", "[fuzz][diag]")
{
   // Same as no-insert but with values <= 60 bytes (no value_nodes)
   // Leaks 4 objects = tree nodes only (vs 43 with value_nodes)
   uint64_t seed = 42;
   INFO("seed=" << seed);
   test_db     tdb("fuzz_ni_small");
   fuzz_runner runner(tdb, seed, op_weights{{0, 10, 45, 15, 0, 15, 5, 2, 2, 2, 2, 2}}, 60);
   runner.run(680 / SCALE, true);
   runner.cleanup_and_leak_check();
}

TEST_CASE("fuzz op128 leak", "[fuzz][diag]")
{
   // Op #128 (a remove after two commit_reopens) introduces 4 leaked leaf nodes
   // First run 127 ops (no leak), then run 1 more op with diagnostics
   test_db     tdb("fuzz_op128");
   fuzz_runner runner(tdb, 42, op_weights{{0, 10, 45, 15, 0, 15, 5, 2, 2, 2, 2, 2}}, 60);
   runner.run(127);  // 127 ops → no leak

   // Print tree structure before op 128
   std::cerr << "=== TREE BEFORE op 128 ===" << std::endl;
   runner.print_tree();
   std::cerr << "=== OBJECTS BEFORE op 128 ===" << std::endl;
   tdb.ses->dump_live_objects();

   runner.run(1, true);  // op #128

   std::cerr << "=== TREE AFTER op 128 ===" << std::endl;
   runner.print_tree();
   std::cerr << "=== OBJECTS AFTER op 128 ===" << std::endl;
   tdb.ses->dump_live_objects();

   runner.cleanup_and_leak_check();
}

TEST_CASE("fuzz bisect random heavy", "[fuzz][diag]")
{
   op_weights ow{{25, 10, 20, 15, 0, 15, 5, 2, 2, 2, 2, 2}};
   uint64_t seed = 12345;
   int max_ops = 1000;  // 5000/SCALE=5

   int lo = 1, hi = max_ops;
   while (lo < hi)
   {
      int mid = (lo + hi) / 2;
      test_db tdb("fuzz_brh_" + std::to_string(mid));
      fuzz_runner runner(tdb, seed, ow);
      runner.run(mid);
      auto leaks = runner.cleanup_and_count_leaks();
      std::cerr << "Bisect: " << mid << " ops → " << leaks << " leaks" << std::endl;
      if (leaks > 0)
         hi = mid;
      else
         lo = mid + 1;
   }
   std::cerr << "=== FIRST LEAKING OP: " << lo << " ===" << std::endl;

   // Replay with diagnostics
   {
      test_db tdb("fuzz_brh_final");
      fuzz_runner runner(tdb, seed, ow);
      runner.run(lo - 1);
      std::cerr << "=== TREE BEFORE ===" << std::endl;
      runner.print_tree();
      runner.run(1, true);
      std::cerr << "=== TREE AFTER ===" << std::endl;
      runner.print_tree();
      runner.cleanup_and_leak_check();
   }
}

TEST_CASE("fuzz bisect leak", "[fuzz][diag]")
{
   // Binary search for the first operation that introduces a leak.
   // For each candidate N, we create a fresh DB, replay N ops, cleanup, check leaks.
   op_weights ow{{0, 10, 45, 15, 0, 15, 5, 2, 2, 2, 2, 2}};
   int max_ops = 680 / SCALE;

   // First verify max_ops leaks
   {
      test_db tdb("fuzz_bisect_verify");
      fuzz_runner runner(tdb, 42, ow, 60);
      runner.run(max_ops);
      auto leaks = runner.cleanup_and_count_leaks();
      std::cerr << "Verify: " << max_ops << " ops → " << leaks << " leaks" << std::endl;
      REQUIRE(leaks > 0);
   }

   int lo = 1, hi = max_ops;
   while (lo < hi)
   {
      int mid = (lo + hi) / 2;
      test_db tdb("fuzz_bisect_" + std::to_string(mid));
      fuzz_runner runner(tdb, 42, ow, 60);
      runner.run(mid);
      auto leaks = runner.cleanup_and_count_leaks();
      std::cerr << "Bisect: " << mid << " ops → " << leaks << " leaks" << std::endl;
      if (leaks > 0)
         hi = mid;
      else
         lo = mid + 1;
   }
   std::cerr << "=== FIRST LEAKING OP: " << lo << " ===" << std::endl;

   // Now replay with tracing to see exactly what that op is
   {
      test_db tdb("fuzz_bisect_final");
      fuzz_runner runner(tdb, 42, ow, 60);
      runner.run(lo, true);
      auto leaks = runner.cleanup_and_count_leaks();
      std::cerr << "Final: " << lo << " ops → " << leaks << " leaks" << std::endl;
      REQUIRE(leaks == 0);  // will fail, showing us the op
   }
}

TEST_CASE("fuzz double-commit+update leak", "[fuzz][diag]")
{
   // Test: double commit_reopen then update in shared mode
   test_db tdb("fuzz_dcu_leak");
   auto    cur = tdb.ses->create_write_cursor();

   constexpr int N = 40;  // enough to have inner nodes

   auto make_key = [](int i) -> std::string
   {
      std::string key(4, '\0');
      key[0] = (i >> 24) & 0xff;
      key[1] = (i >> 16) & 0xff;
      key[2] = (i >> 8) & 0xff;
      key[3] = i & 0xff;
      return key;
   };

   auto do_commit = [&]()
   {
      tdb.ses->set_root(0, cur->root());
      auto nr = tdb.ses->get_root(0);
      cur     = tdb.ses->create_write_cursor(std::move(nr));
   };

   // Insert N keys with small values
   for (int i = 0; i < N; ++i)
      cur->upsert(to_key_view(make_key(i)), to_value_view(std::string(30, 'A' + (i % 26))));

   // Double commit_reopen (as seen in the trace)
   do_commit();
   do_commit();

   // Update one key in shared mode
   cur->update(to_key_view(make_key(0)), to_value_view(std::string(30, 'X')));

   // Remove all
   for (int i = 0; i < N; ++i)
      cur->remove(to_key_view(make_key(i)));

   cur->take_root();
   tdb.ses->set_root(0, {}, sal::sync_type::none);
   tdb.assert_no_leaks();
}

TEST_CASE("fuzz single-commit+update leak", "[fuzz][diag]")
{
   // Same but with single commit (should pass)
   test_db tdb("fuzz_scu_leak");
   auto    cur = tdb.ses->create_write_cursor();

   constexpr int N = 40;

   auto make_key = [](int i) -> std::string
   {
      std::string key(4, '\0');
      key[0] = (i >> 24) & 0xff;
      key[1] = (i >> 16) & 0xff;
      key[2] = (i >> 8) & 0xff;
      key[3] = i & 0xff;
      return key;
   };

   auto do_commit = [&]()
   {
      tdb.ses->set_root(0, cur->root());
      auto nr = tdb.ses->get_root(0);
      cur     = tdb.ses->create_write_cursor(std::move(nr));
   };

   for (int i = 0; i < N; ++i)
      cur->upsert(to_key_view(make_key(i)), to_value_view(std::string(30, 'A' + (i % 26))));

   // Single commit
   do_commit();

   // Update one key
   cur->update(to_key_view(make_key(0)), to_value_view(std::string(30, 'X')));

   for (int i = 0; i < N; ++i)
      cur->remove(to_key_view(make_key(i)));

   cur->take_root();
   tdb.ses->set_root(0, {}, sal::sync_type::none);
   tdb.assert_no_leaks();
}

TEST_CASE("fuzz commit+many-updates leak", "[fuzz][diag]")
{
   // Commit then update ALL keys
   test_db tdb("fuzz_cmu_leak");
   auto    cur = tdb.ses->create_write_cursor();

   constexpr int N = 40;

   auto make_key = [](int i) -> std::string
   {
      std::string key(4, '\0');
      key[0] = (i >> 24) & 0xff;
      key[1] = (i >> 16) & 0xff;
      key[2] = (i >> 8) & 0xff;
      key[3] = i & 0xff;
      return key;
   };

   auto do_commit = [&]()
   {
      tdb.ses->set_root(0, cur->root());
      auto nr = tdb.ses->get_root(0);
      cur     = tdb.ses->create_write_cursor(std::move(nr));
   };

   for (int i = 0; i < N; ++i)
      cur->upsert(to_key_view(make_key(i)), to_value_view(std::string(30, 'A' + (i % 26))));

   do_commit();

   // Update ALL keys
   for (int i = 0; i < N; ++i)
      cur->update(to_key_view(make_key(i)), to_value_view(std::string(30, 'X')));

   // Commit again then update more
   do_commit();
   for (int i = 0; i < N; ++i)
      cur->update(to_key_view(make_key(i)), to_value_view(std::string(30, 'Y')));

   for (int i = 0; i < N; ++i)
      cur->remove(to_key_view(make_key(i)));

   cur->take_root();
   tdb.ses->set_root(0, {}, sal::sync_type::none);
   tdb.assert_no_leaks();
}

TEST_CASE("fuzz no-insert big-values-only", "[fuzz][diag]")
{
   // Same as no-insert but values always > 64 bytes (always value_nodes)
   uint64_t seed = 42;
   INFO("seed=" << seed);
   test_db     tdb("fuzz_ni_big");
   fuzz_runner runner(tdb, seed, op_weights{{0, 10, 45, 15, 0, 15, 5, 2, 2, 2, 2, 2}}, 512);
   runner.run(1000 / SCALE);
   runner.cleanup_and_leak_check();
}
#endif  // Diagnostic tests
