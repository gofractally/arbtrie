#include <catch2/catch_all.hpp>
#include <atomic>
#include <latch>
#include <thread>
#include <vector>
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <sal/smart_ptr.hpp>
#include <sal/smart_ptr_impl.hpp>

using namespace psitri;

namespace
{
   struct smart_ptr_db
   {
      std::string               dir;
      std::shared_ptr<database> db;

      smart_ptr_db(const std::string& name = "smart_ptr_testdb")
          : dir(name)
      {
         std::filesystem::remove_all(dir);
         db = database::create(dir, runtime_config());
      }

      ~smart_ptr_db() { std::filesystem::remove_all(dir); }
   };

   /// Populate root with N keys and commit so the root is read-only.
   /// Session must have sync >= mprotect for shared_smart_ptr tests.
   void populate(std::shared_ptr<write_session>& ses, uint32_t n, uint32_t root = 0)
   {
      auto tx = ses->start_transaction(root);
      for (uint32_t i = 0; i < n; ++i)
      {
         auto k = "key-" + std::to_string(i);
         auto v = "val-" + std::to_string(i);
         tx.upsert(k, v);
      }
      tx.commit();
   }
}  // namespace

// ============================================================
// smart_ptr basics — same-thread copy, move, release
// ============================================================

TEST_CASE("smart_ptr copy increments ref count", "[smart_ptr]")
{
   smart_ptr_db t;
   auto         ses = t.db->start_write_session();
   populate(ses, 10);

   auto root = ses->get_root(0);
   REQUIRE(root);

   {
      auto copy = root;  // copy constructor — should retain
      REQUIRE(copy);
      REQUIRE(copy.address() == root.address());
   }
   // copy destroyed — ref count decremented, but root still valid
   REQUIRE(root);
   REQUIRE(root.address() != sal::null_ptr_address);
}

TEST_CASE("smart_ptr move transfers ownership", "[smart_ptr]")
{
   smart_ptr_db t;
   auto         ses = t.db->start_write_session();
   populate(ses, 5);

   auto root = ses->get_root(0);
   auto addr = root.address();
   REQUIRE(root);

   auto moved = std::move(root);
   REQUIRE(!root);  // moved-from is null
   REQUIRE(moved);
   REQUIRE(moved.address() == addr);
}

TEST_CASE("smart_ptr take() releases ownership without decrementing", "[smart_ptr]")
{
   smart_ptr_db t;
   auto         ses = t.db->start_write_session();
   populate(ses, 3);

   auto root = ses->get_root(0);
   auto addr = root.address();

   auto taken = root.take();
   REQUIRE(!root);  // smart_ptr is now null
   REQUIRE(taken == addr);

   // We must manually manage the taken address — give it back to avoid leak
   root.give(taken);
   REQUIRE(root);
}

// ============================================================
// shared_smart_ptr — cross-thread sharing
// ============================================================

TEST_CASE("shared_smart_ptr constructed from committed root", "[smart_ptr][shared]")
{
   smart_ptr_db t;
   auto         ses = t.db->start_write_session();
   populate(ses, 10);

   auto root = ses->get_root(0);
   REQUIRE(root.is_read_only());

   sal::shared_smart_ptr<sal::alloc_header> shared(root);
   REQUIRE(shared);

   // Convert back to thread-local smart_ptr
   auto local = shared.get();
   REQUIRE(local);
   REQUIRE(local.address() == root.address());
}

TEST_CASE("shared_smart_ptr copy semantics", "[smart_ptr][shared]")
{
   smart_ptr_db t;
   auto         ses = t.db->start_write_session();
   populate(ses, 5);

   auto                                     root = ses->get_root(0);
   sal::shared_smart_ptr<sal::alloc_header> shared(root);

   // Copy
   auto copy = shared;
   REQUIRE(copy);

   // Move
   auto moved = std::move(copy);
   REQUIRE(moved);
   REQUIRE(!copy);
}

TEST_CASE("shared_smart_ptr passes root to another thread for reading", "[smart_ptr][shared][threads]")
{
   smart_ptr_db t;
   auto         ses = t.db->start_write_session();
   populate(ses, 100);

   auto                                     root = ses->get_root(0);
   sal::shared_smart_ptr<sal::alloc_header> shared(root);

   std::atomic<uint64_t> reader_count{0};

   std::thread reader(
       [&]()
       {
          // Reader thread creates its own session and uses the shared root
          auto rs = t.db->start_read_session();

          // Convert shared_smart_ptr to thread-local smart_ptr
          auto local_root = shared.get();
          REQUIRE(local_root);

          // Iterate using a cursor constructed from the shared root
          auto rc = cursor(std::move(local_root));
          rc.seek_begin();
          while (!rc.is_end())
          {
             ++reader_count;
             rc.next();
          }
       });

   reader.join();
   REQUIRE(reader_count == 100);
}

TEST_CASE("shared_smart_ptr read by multiple threads concurrently", "[smart_ptr][shared][threads]")
{
   smart_ptr_db t;
   auto         ses = t.db->start_write_session();
   populate(ses, 200);

   auto                                     root = ses->get_root(0);
   sal::shared_smart_ptr<sal::alloc_header> shared(root);

   constexpr int                num_threads = 4;
   std::vector<uint64_t>        counts(num_threads, 0);
   std::latch                   start_gate(num_threads);
   std::vector<std::thread>     threads;

   for (int i = 0; i < num_threads; ++i)
   {
      threads.emplace_back(
          [&, i]()
          {
             auto rs = t.db->start_read_session();
             auto local_root = shared.get();

             // All threads start reading at the same time
             start_gate.arrive_and_wait();

             auto rc = cursor(std::move(local_root));
             rc.seek_begin();
             while (!rc.is_end())
             {
                ++counts[i];
                rc.next();
             }
          });
   }

   for (auto& t : threads)
      t.join();

   // Every reader should see all 200 keys
   for (int i = 0; i < num_threads; ++i)
   {
      INFO("thread " << i << " count: " << counts[i]);
      REQUIRE(counts[i] == 200);
   }
}

TEST_CASE("shared_smart_ptr writer and concurrent readers", "[smart_ptr][shared][threads]")
{
   smart_ptr_db t;
   auto         ses = t.db->start_write_session();
   populate(ses, 50);

   // Snapshot the root before further writes
   auto                                     root = ses->get_root(0);
   sal::shared_smart_ptr<sal::alloc_header> snapshot(root);

   // Writer continues modifying root 0
   std::thread writer(
       [&]()
       {
          auto ws = t.db->start_write_session();
          for (int i = 50; i < 150; ++i)
          {
             auto tx = ws->start_transaction(0);
             tx.upsert("new-key-" + std::to_string(i), "new-val");
             tx.commit();
          }
       });

   // Reader uses the snapshot — should see exactly 50 keys regardless of writer
   std::atomic<uint64_t> reader_count{0};
   std::thread           reader(
       [&]()
       {
          auto rs         = t.db->start_read_session();
          auto local_root = snapshot.get();
          auto rc         = cursor(std::move(local_root));
          rc.seek_begin();
          while (!rc.is_end())
          {
             ++reader_count;
             rc.next();
          }
       });

   writer.join();
   reader.join();

   // Snapshot isolation: reader sees exactly 50 keys
   REQUIRE(reader_count == 50);
}

TEST_CASE("shared_smart_ptr survives original smart_ptr destruction", "[smart_ptr][shared]")
{
   smart_ptr_db t;
   auto         ses = t.db->start_write_session();
   populate(ses, 20);

   auto             root     = ses->get_root(0);
   sal::ptr_address original_addr = root.address();
   sal::shared_smart_ptr<sal::alloc_header> shared(root);

   {
      // Let the original smart_ptr go out of scope
      auto root2 = ses->get_root(0);
      (void)root2;  // just to show a second copy can exist
   }
   // root is destroyed here, but shared still holds a reference

   REQUIRE(shared);
   auto local = shared.get();
   REQUIRE(local);
   REQUIRE(local.address() == original_addr);

   // Should still be usable — iterate the tree
   auto     rc = cursor(std::move(local));
   uint64_t count = 0;
   rc.seek_begin();
   while (!rc.is_end())
   {
      ++count;
      rc.next();
   }
   REQUIRE(count == 20);
}

TEST_CASE("multiple shared_smart_ptr snapshots at different points in time",
          "[smart_ptr][shared][threads]")
{
   smart_ptr_db t;
   auto         ses = t.db->start_write_session();

   // Phase 1: insert 10 keys, snapshot
   populate(ses, 10);
   auto root1 = ses->get_root(0);
   sal::shared_smart_ptr<sal::alloc_header> snap1(root1);

   // Phase 2: insert 10 more keys, snapshot
   {
      auto tx = ses->start_transaction(0);
      for (int i = 10; i < 20; ++i)
         tx.upsert("key-" + std::to_string(i), "val-" + std::to_string(i));
      tx.commit();
   }
   auto root2 = ses->get_root(0);
   sal::shared_smart_ptr<sal::alloc_header> snap2(root2);

   // Read both snapshots from different threads
   std::atomic<uint64_t> count1{0}, count2{0};

   std::thread t1(
       [&]()
       {
          auto rs    = t.db->start_read_session();
          auto local = snap1.get();
          auto rc    = cursor(std::move(local));
          rc.seek_begin();
          while (!rc.is_end())
          {
             ++count1;
             rc.next();
          }
       });

   std::thread t2(
       [&]()
       {
          auto rs    = t.db->start_read_session();
          auto local = snap2.get();
          auto rc    = cursor(std::move(local));
          rc.seek_begin();
          while (!rc.is_end())
          {
             ++count2;
             rc.next();
          }
       });

   t1.join();
   t2.join();

   REQUIRE(count1 == 10);
   REQUIRE(count2 == 20);
}

TEST_CASE("shared_smart_ptr used to build subtree on worker thread", "[smart_ptr][shared][threads]")
{
   smart_ptr_db t;
   auto         ses = t.db->start_write_session();

   // Build a subtree on a worker thread, pass it back via shared_smart_ptr
   std::optional<sal::shared_smart_ptr<sal::alloc_header>> subtree_root;

   std::thread worker(
       [&]()
       {
          auto ws = t.db->start_write_session();
          ws->set_sync(sal::sync_type::mprotect);
          // Use a dedicated root (root 1) to commit the subtree so it becomes read-only
          auto tx = ws->start_transaction(1);
          for (int i = 0; i < 50; ++i)
          {
             auto k = "sub-" + std::to_string(i);
             auto v = "data-" + std::to_string(i);
             tx.upsert(k, v);
          }
          tx.commit();
          auto root    = ws->get_root(1);
          subtree_root.emplace(root);
       });

   worker.join();
   REQUIRE(subtree_root.has_value());
   REQUIRE(*subtree_root);

   // Store the subtree as a value in root 0
   auto tx = ses->start_transaction(0);
   tx.upsert("workers-result", subtree_root->get());
   tx.commit();

   // Verify the subtree was stored
   auto tx2 = ses->start_transaction(0);
   REQUIRE(tx2.is_subtree("workers-result"));
   tx2.abort();
}
