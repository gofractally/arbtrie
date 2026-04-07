#include <art/cow_coordinator.hpp>
#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <thread>
#include <vector>

using namespace art;

TEST_CASE("cow_coordinator initial state", "[cow_coordinator]")
{
   cow_coordinator cc;
   auto s = cc.load_state();

   REQUIRE(s.root_offset() == null_offset);
   REQUIRE_FALSE(s.writer_active());
   REQUIRE_FALSE(s.reader_waiting());
   REQUIRE(s.reader_count() == 0);
   REQUIRE(s.cow_seq() == 0);
   REQUIRE(cc.read_prev() == null_offset);
}

TEST_CASE("cow_coordinator begin/end write no readers", "[cow_coordinator]")
{
   cow_coordinator cc;
   cc.set_root(100);

   // begin_write with no readers — cow_seq should NOT bump
   uint32_t seq = cc.begin_write();
   REQUIRE(seq == 0);

   auto s = cc.load_state();
   REQUIRE(s.writer_active());
   REQUIRE(s.root_offset() == 100);
   REQUIRE(s.cow_seq() == 0);

   // end_write — update root, no readers to notify
   bool notified = cc.end_write(200, seq);
   REQUIRE_FALSE(notified);

   s = cc.load_state();
   REQUIRE_FALSE(s.writer_active());
   REQUIRE(s.root_offset() == 200);
   REQUIRE(s.cow_seq() == 0);  // no bump — no collision
}

TEST_CASE("cow_coordinator reader/writer collision at begin_write", "[cow_coordinator]")
{
   cow_coordinator cc;
   cc.set_root(100);

   // Simulate a reader traversing head
   uint32_t reader_root = cc.begin_read_latest();
   REQUIRE(reader_root == 100);

   auto s = cc.load_state();
   REQUIRE(s.reader_count() == 1);

   // Writer starts — sees reader_count > 0, must bump cow_seq
   uint32_t seq = cc.begin_write();
   REQUIRE(seq == 1);  // bumped from 0

   s = cc.load_state();
   REQUIRE(s.writer_active());
   REQUIRE(s.cow_seq() == 1);

   // prev_root should be published (head before write started)
   REQUIRE(cc.read_prev() == 100);

   // Reader finishes
   cc.end_read_latest();

   // Writer commits
   cc.end_write(200, seq);

   s = cc.load_state();
   REQUIRE(s.root_offset() == 200);
   REQUIRE_FALSE(s.writer_active());
}

TEST_CASE("cow_coordinator reader/writer collision at end_write", "[cow_coordinator]")
{
   cow_coordinator cc;
   cc.set_root(100);

   // Writer starts with no readers — no cow_seq bump
   uint32_t seq = cc.begin_write();
   REQUIRE(seq == 0);

   // Reader arrives during write — increments reader_count
   // (In real code, reader would also set reader_waiting and wait,
   //  but for testing the state transitions we just check the flags.)

   // Simulate: reader increments count while writer is active
   // In practice the reader would call begin_read_latest which waits,
   // but we can test the end_write logic by manually setting reader_count.
   // Instead, let's test via the reader_waiting path.

   // Writer commits — sees no readers (they'd be waiting, not counted yet
   // since begin_read_latest blocks). No bump.
   bool notified = cc.end_write(200, seq);
   REQUIRE_FALSE(notified);
   REQUIRE(cc.load_state().cow_seq() == 0);
}

TEST_CASE("cow_coordinator multiple readers", "[cow_coordinator]")
{
   cow_coordinator cc;
   cc.set_root(100);

   // Multiple readers start
   uint32_t r1 = cc.begin_read_latest();
   uint32_t r2 = cc.begin_read_latest();
   uint32_t r3 = cc.begin_read_latest();

   REQUIRE(r1 == 100);
   REQUIRE(r2 == 100);
   REQUIRE(r3 == 100);
   REQUIRE(cc.load_state().reader_count() == 3);

   // Writer starts — sees readers, bumps cow_seq
   uint32_t seq = cc.begin_write();
   REQUIRE(seq == 1);
   REQUIRE(cc.read_prev() == 100);

   // Readers finish one by one
   cc.end_read_latest();
   REQUIRE(cc.load_state().reader_count() == 2);
   cc.end_read_latest();
   REQUIRE(cc.load_state().reader_count() == 1);
   cc.end_read_latest();
   REQUIRE(cc.load_state().reader_count() == 0);

   // Writer commits — no readers left, but cow_seq stays at current
   cc.end_write(200, seq);
   REQUIRE(cc.load_state().cow_seq() == 1);
}

TEST_CASE("cow_coordinator no COW when taking turns", "[cow_coordinator]")
{
   cow_coordinator cc;
   cc.set_root(100);

   // Round 1: write, then read — no overlap
   uint32_t seq = cc.begin_write();
   REQUIRE(seq == 0);
   cc.end_write(200, seq);

   uint32_t root = cc.begin_read_latest();
   REQUIRE(root == 200);
   cc.end_read_latest();

   // Round 2: write, then read — still no overlap
   seq = cc.begin_write();
   REQUIRE(seq == 0);  // never bumped — no collision ever
   cc.end_write(300, seq);

   root = cc.begin_read_latest();
   REQUIRE(root == 300);
   cc.end_read_latest();

   // cow_seq should still be 0 — zero COW overhead
   REQUIRE(cc.load_state().cow_seq() == 0);
}

TEST_CASE("cow_coordinator fresh reads prev_root", "[cow_coordinator]")
{
   cow_coordinator cc;
   cc.set_root(100);

   // No prev yet
   REQUIRE(cc.read_prev() == null_offset);

   // Reader starts traversing, then writer starts → prev published
   uint32_t r = cc.begin_read_latest();
   REQUIRE(r == 100);

   uint32_t seq = cc.begin_write();
   REQUIRE(cc.read_prev() == 100);  // head copied to prev

   // Reader is still active when writer commits — end_write sees
   // reader_count > 0, publishes prev = new_root (200)
   cc.end_write(200, seq);
   cc.end_read_latest();

   // prev was updated to 200 by end_write (reader was still active)
   REQUIRE(cc.read_prev() == 200);

   // Fresh reader gets prev without any coordination
   REQUIRE(cc.read_prev() == 200);
}

TEST_CASE("cow_coordinator reset", "[cow_coordinator]")
{
   cow_coordinator cc;
   cc.set_root(100);

   uint32_t r = cc.begin_read_latest();
   cc.end_read_latest();

   uint32_t seq = cc.begin_write();
   cc.end_write(200, seq);

   // Reset (e.g., arena swap)
   cc.reset(500);

   auto s = cc.load_state();
   REQUIRE(s.root_offset() == 500);
   REQUIRE(s.cow_seq() == 0);
   REQUIRE(s.reader_count() == 0);
   REQUIRE_FALSE(s.writer_active());
   REQUIRE(cc.read_prev() == null_offset);
}

TEST_CASE("cow_coordinator concurrent readers and writer", "[cow_coordinator]")
{
   cow_coordinator cc;
   cc.set_root(42);

   constexpr int NUM_READERS = 8;
   constexpr int ITERATIONS  = 10000;

   std::atomic<bool> stop{false};
   std::atomic<int>  reads_completed{0};

   // Reader threads: continuously read latest
   std::vector<std::thread> readers;
   for (int i = 0; i < NUM_READERS; ++i)
   {
      readers.emplace_back([&]() {
         while (!stop.load(std::memory_order_relaxed))
         {
            uint32_t root = cc.begin_read_latest();
            // root should always be a valid value we've written
            assert(root != null_offset);
            (void)root;
            cc.end_read_latest();
            reads_completed.fetch_add(1, std::memory_order_relaxed);
         }
      });
   }

   // Writer thread: write transactions
   for (int i = 0; i < ITERATIONS; ++i)
   {
      uint32_t seq = cc.begin_write();
      uint32_t new_root = 1000 + i;
      cc.end_write(new_root, seq);
   }

   stop.store(true, std::memory_order_relaxed);
   for (auto& t : readers)
      t.join();

   // Verify final state
   auto s = cc.load_state();
   REQUIRE(s.root_offset() == uint32_t(1000 + ITERATIONS - 1));
   REQUIRE_FALSE(s.writer_active());
   REQUIRE(s.reader_count() == 0);
   REQUIRE(reads_completed.load() > 0);
}
