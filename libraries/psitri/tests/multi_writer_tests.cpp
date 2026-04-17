#include <catch2/catch_all.hpp>
#include <atomic>
#include <thread>
#include <vector>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/value_type.hpp>

using namespace psitri;

constexpr int SCALE = 1;

namespace
{
   struct multi_writer_db
   {
      std::string               dir;
      std::shared_ptr<database> db;

      multi_writer_db(const std::string& name = "multi_writer_testdb")
          : dir(name)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir + "/data");
         db = database::open(dir);
      }

      ~multi_writer_db() { std::filesystem::remove_all(dir); }
   };

   /// Generate an 8-byte big-endian key from a uint64_t
   inline key_view to_be_key(uint64_t val, std::vector<char>& buf)
   {
      buf.resize(8);
      for (int i = 7; i >= 0; --i)
      {
         buf[i] = char(val & 0xFF);
         val >>= 8;
      }
      return key_view(buf.data(), buf.size());
   }

   /// Release all roots and verify no objects are leaked.
   void require_no_leaks(multi_writer_db& t, uint32_t num_roots)
   {
      auto ses = t.db->start_write_session();
      for (uint32_t i = 1; i <= num_roots; ++i)
         ses->set_root(i, {});

      // Cascaded destroy() calls generate new release queue entries,
      // so we must loop until everything settles.
      auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
      while (std::chrono::steady_clock::now() < deadline)
      {
         t.db->wait_for_compactor();
         if (ses->get_total_allocated_objects() == 0)
            break;
         std::this_thread::sleep_for(std::chrono::milliseconds(50));
      }
      uint64_t allocated = ses->get_total_allocated_objects();
      INFO("allocated after releasing all roots: " << allocated << " (expected 0)");
      REQUIRE(allocated == 0);
   }

   /// Count keys in a tree by iterating a cursor
   uint64_t count_keys(std::shared_ptr<write_session>& ses, uint32_t root_index)
   {
      auto root = ses->get_root(root_index);
      if (!root)
         return 0;
      auto     rc = cursor(std::move(root));
      uint64_t count = 0;
      rc.seek_begin();
      while (!rc.is_end())
      {
         ++count;
         rc.next();
      }
      return count;
   }
}  // namespace

// ============================================================
// Multi-writer insert tests
// ============================================================

TEST_CASE("multi-writer concurrent inserts", "[multi-writer][insert]")
{
   multi_writer_db t("mw_insert_test");
   const uint32_t  num_writers     = 4;
   const uint32_t  items_per_round = 10000 / SCALE;
   const uint32_t  num_rounds      = 3;

   std::atomic<bool> start_flag{false};

   std::vector<std::thread> writers;
   writers.reserve(num_writers);

   for (uint32_t w = 0; w < num_writers; ++w)
   {
      writers.emplace_back(
          [&, w]()
          {
             auto              ws         = t.db->start_write_session();
             uint32_t          root_index = w + 1;
             std::vector<char> key_buf;
             std::vector<char> val(64, 'v');
             value_view        vv(val.data(), val.size());

             while (!start_flag.load(std::memory_order_relaxed))
                ;

             for (uint32_t r = 0; r < num_rounds; ++r)
             {
                auto tx = ws->start_transaction(root_index);
                for (uint32_t i = 0; i < items_per_round; ++i)
                {
                   uint64_t seq = uint64_t(r) * items_per_round + i;
                   seq          = seq * num_writers + w;
                   tx.insert(to_be_key(seq, key_buf), vv);
                }
                tx.commit();
             }
          });
   }

   start_flag.store(true, std::memory_order_relaxed);
   for (auto& thr : writers)
      thr.join();

   t.db->wait_for_compactor();

   auto ses = t.db->start_write_session();
   for (uint32_t w = 0; w < num_writers; ++w)
   {
      REQUIRE(count_keys(ses, w + 1) == uint64_t(items_per_round) * num_rounds);
   }
   ses.reset();
   require_no_leaks(t, num_writers);
}

TEST_CASE("multi-writer concurrent insert and remove", "[multi-writer][remove]")
{
   multi_writer_db t("mw_insert_remove_test");
   const uint32_t  num_writers     = 4;
   const uint32_t  items_per_round = 5000 / SCALE;

   std::atomic<bool> start_flag{false};

   std::vector<std::thread> writers;
   writers.reserve(num_writers);

   for (uint32_t w = 0; w < num_writers; ++w)
   {
      writers.emplace_back(
          [&, w]()
          {
             auto              ws         = t.db->start_write_session();
             uint32_t          root_index = w + 1;
             std::vector<char> key_buf;
             std::vector<char> val(32, 'x');
             value_view        vv(val.data(), val.size());

             while (!start_flag.load(std::memory_order_relaxed))
                ;

             // Round 1: Insert items
             {
                auto tx = ws->start_transaction(root_index);
                for (uint32_t i = 0; i < items_per_round; ++i)
                {
                   uint64_t seq = uint64_t(i) * num_writers + w;
                   tx.insert(to_be_key(seq, key_buf), vv);
                }
                tx.commit();
             }

             // Round 2: Remove half the items
             {
                auto tx = ws->start_transaction(root_index);
                for (uint32_t i = 0; i < items_per_round; i += 2)
                {
                   uint64_t seq = uint64_t(i) * num_writers + w;
                   tx.remove(to_be_key(seq, key_buf));
                }
                tx.commit();
             }

             // Round 3: Insert more items
             {
                auto tx = ws->start_transaction(root_index);
                for (uint32_t i = 0; i < items_per_round; ++i)
                {
                   uint64_t seq = uint64_t(items_per_round + i) * num_writers + w;
                   tx.insert(to_be_key(seq, key_buf), vv);
                }
                tx.commit();
             }
          });
   }

   start_flag.store(true, std::memory_order_relaxed);
   for (auto& thr : writers)
      thr.join();

   t.db->wait_for_compactor();

   auto ses = t.db->start_write_session();
   for (uint32_t w = 0; w < num_writers; ++w)
   {
      uint64_t expected = items_per_round / 2 + items_per_round;
      REQUIRE(count_keys(ses, w + 1) == expected);
   }
   ses.reset();
   require_no_leaks(t, num_writers);
}

TEST_CASE("multi-writer shared tree inserts", "[multi-writer][shared-tree]")
{
   multi_writer_db t("mw_shared_tree_test");
   const uint32_t  num_writers      = 4;
   const uint32_t  items_per_writer = 2000 / SCALE;
   const uint32_t  root_index       = 1;

   std::atomic<bool> start_flag{false};

   std::vector<std::thread> writers;
   writers.reserve(num_writers);

   for (uint32_t w = 0; w < num_writers; ++w)
   {
      writers.emplace_back(
          [&, w]()
          {
             auto              ws = t.db->start_write_session();
             std::vector<char> key_buf;
             std::vector<char> val(16, char('a' + w));
             value_view        vv(val.data(), val.size());

             while (!start_flag.load(std::memory_order_relaxed))
                ;

             auto tx = ws->start_transaction(root_index);
             for (uint32_t i = 0; i < items_per_writer; ++i)
             {
                uint64_t seq = uint64_t(i) * num_writers + w;
                tx.insert(to_be_key(seq, key_buf), vv);
             }
             tx.commit();
          });
   }

   start_flag.store(true, std::memory_order_relaxed);
   for (auto& thr : writers)
      thr.join();

   t.db->wait_for_compactor();

   // Last committer wins, but tree should be readable without crashing
   auto     ses   = t.db->start_write_session();
   uint64_t count = count_keys(ses, root_index);
   REQUIRE(count >= items_per_writer);
   ses.reset();
   require_no_leaks(t, 1);
}

TEST_CASE("multi-writer with concurrent readers", "[multi-writer][reader]")
{
   multi_writer_db t("mw_reader_test");
   const uint32_t  num_writers     = 2;
   const uint32_t  num_readers     = 2;
   const uint32_t  items_per_round = 5000 / SCALE;
   const uint32_t  num_rounds      = 3;

   std::atomic<bool> start_flag{false};
   std::atomic<bool> writers_done{false};

   std::vector<std::thread> threads;

   // Writers: each writer has its own tree
   for (uint32_t w = 0; w < num_writers; ++w)
   {
      threads.emplace_back(
          [&, w]()
          {
             auto              ws         = t.db->start_write_session();
             uint32_t          root_index = w + 1;
             std::vector<char> key_buf;
             std::vector<char> val(64, 'v');
             value_view        vv(val.data(), val.size());

             while (!start_flag.load(std::memory_order_relaxed))
                ;

             for (uint32_t r = 0; r < num_rounds; ++r)
             {
                auto tx = ws->start_transaction(root_index);
                for (uint32_t i = 0; i < items_per_round; ++i)
                {
                   uint64_t seq = uint64_t(r) * items_per_round + i;
                   seq          = seq * num_writers + w;
                   tx.insert(to_be_key(seq, key_buf), vv);
                }
                tx.commit();
             }
          });
   }

   // Readers: iterate trees while writes are in progress
   for (uint32_t r = 0; r < num_readers; ++r)
   {
      threads.emplace_back(
          [&, r]()
          {
             auto     rs         = t.db->start_read_session();
             uint32_t root_index = (r % num_writers) + 1;

             while (!start_flag.load(std::memory_order_relaxed))
                ;

             uint32_t iterations = 0;
             while (!writers_done.load(std::memory_order_relaxed) || iterations < 10)
             {
                auto cur = rs->create_cursor(root_index);
                cur.seek_begin();
                while (!cur.is_end())
                   cur.next();
                ++iterations;
                std::this_thread::yield();
             }
          });
   }

   start_flag.store(true, std::memory_order_relaxed);

   for (uint32_t w = 0; w < num_writers; ++w)
      threads[w].join();
   writers_done.store(true, std::memory_order_relaxed);

   for (uint32_t i = num_writers; i < threads.size(); ++i)
      threads[i].join();

   t.db->wait_for_compactor();
   require_no_leaks(t, num_writers);
}

TEST_CASE("multi-writer stress insert", "[multi-writer][stress]")
{
   multi_writer_db t("mw_stress_test");
   const uint32_t  num_writers     = 4;
   const uint32_t  items_per_round = 50000 / SCALE;
   const uint32_t  num_rounds      = 5;

   std::atomic<bool> start_flag{false};

   std::vector<std::thread> writers;
   writers.reserve(num_writers);

   for (uint32_t w = 0; w < num_writers; ++w)
   {
      writers.emplace_back(
          [&, w]()
          {
             auto              ws         = t.db->start_write_session();
             uint32_t          root_index = w + 1;
             std::vector<char> key_buf;
             std::vector<char> val(128, char('A' + w));
             value_view        vv(val.data(), val.size());

             while (!start_flag.load(std::memory_order_relaxed))
                ;

             for (uint32_t r = 0; r < num_rounds; ++r)
             {
                auto tx = ws->start_transaction(root_index);
                for (uint32_t i = 0; i < items_per_round; ++i)
                {
                   uint64_t seq = uint64_t(r) * items_per_round + i;
                   seq          = seq * num_writers + w;
                   tx.insert(to_be_key(seq, key_buf), vv);
                }
                tx.commit();
             }
          });
   }

   start_flag.store(true, std::memory_order_relaxed);
   for (auto& thr : writers)
      thr.join();

   t.db->wait_for_compactor();

   auto ses = t.db->start_write_session();
   for (uint32_t w = 0; w < num_writers; ++w)
   {
      REQUIRE(count_keys(ses, w + 1) == uint64_t(items_per_round) * num_rounds);
   }
   ses.reset();
   require_no_leaks(t, num_writers);
}

TEST_CASE("multi-writer insert then remove all", "[multi-writer][remove-all]")
{
   multi_writer_db t("mw_remove_all_test");
   const uint32_t  num_writers = 4;
   const uint32_t  items       = 5000 / SCALE;

   std::atomic<bool> start_flag{false};

   std::vector<std::thread> writers;
   writers.reserve(num_writers);

   for (uint32_t w = 0; w < num_writers; ++w)
   {
      writers.emplace_back(
          [&, w]()
          {
             auto              ws         = t.db->start_write_session();
             uint32_t          root_index = w + 1;
             std::vector<char> key_buf;
             std::vector<char> val(32, 'r');
             value_view        vv(val.data(), val.size());

             while (!start_flag.load(std::memory_order_relaxed))
                ;

             // Insert
             {
                auto tx = ws->start_transaction(root_index);
                for (uint32_t i = 0; i < items; ++i)
                {
                   uint64_t seq = uint64_t(i) * num_writers + w;
                   tx.insert(to_be_key(seq, key_buf), vv);
                }
                tx.commit();
             }

             // Remove all
             {
                auto tx = ws->start_transaction(root_index);
                for (uint32_t i = 0; i < items; ++i)
                {
                   uint64_t seq = uint64_t(i) * num_writers + w;
                   tx.remove(to_be_key(seq, key_buf));
                }
                tx.commit();
             }
          });
   }

   start_flag.store(true, std::memory_order_relaxed);
   for (auto& thr : writers)
      thr.join();

   t.db->wait_for_compactor();

   auto ses = t.db->start_write_session();
   for (uint32_t w = 0; w < num_writers; ++w)
   {
      REQUIRE(count_keys(ses, w + 1) == 0);
   }
   ses.reset();
   require_no_leaks(t, num_writers);
}
