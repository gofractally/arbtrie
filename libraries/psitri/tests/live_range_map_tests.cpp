#include <catch2/catch_all.hpp>
#include <psitri/live_range_map.hpp>

using namespace psitri;

TEST_CASE("live_range_map: single version", "[live_range_map]")
{
   live_range_map map;
   map.add_dead_version(5);
   map.flush_pending();

   CHECK(map.is_dead(5));
   CHECK_FALSE(map.is_dead(4));
   CHECK_FALSE(map.is_dead(6));
   CHECK(map.num_ranges() == 1);
}

TEST_CASE("live_range_map: adjacent versions coalesce", "[live_range_map]")
{
   live_range_map map;
   map.add_dead_version(5);
   map.add_dead_version(6);
   map.add_dead_version(7);
   map.flush_pending();

   CHECK(map.is_dead(5));
   CHECK(map.is_dead(6));
   CHECK(map.is_dead(7));
   CHECK_FALSE(map.is_dead(4));
   CHECK_FALSE(map.is_dead(8));
   CHECK(map.num_ranges() == 1);  // coalesced into [5,7]
}

TEST_CASE("live_range_map: non-adjacent ranges stay separate", "[live_range_map]")
{
   live_range_map map;
   map.add_dead_version(2);
   map.add_dead_version(5);
   map.add_dead_version(10);
   map.flush_pending();

   CHECK(map.is_dead(2));
   CHECK(map.is_dead(5));
   CHECK(map.is_dead(10));
   CHECK_FALSE(map.is_dead(3));
   CHECK_FALSE(map.is_dead(7));
   CHECK(map.num_ranges() == 3);
}

TEST_CASE("live_range_map: gap-filling merges ranges", "[live_range_map]")
{
   live_range_map map;
   // Create ranges [2,2] and [4,4]
   map.add_dead_version(2);
   map.add_dead_version(4);
   map.flush_pending();
   CHECK(map.num_ranges() == 2);

   // Add 3 — fills the gap, should coalesce into [2,4]
   map.add_dead_version(3);
   map.flush_pending();
   CHECK(map.num_ranges() == 1);
   CHECK(map.is_dead(2));
   CHECK(map.is_dead(3));
   CHECK(map.is_dead(4));
}

TEST_CASE("live_range_map: out-of-order insertion", "[live_range_map]")
{
   live_range_map map;
   map.add_dead_version(10);
   map.add_dead_version(3);
   map.add_dead_version(7);
   map.add_dead_version(4);
   map.add_dead_version(8);
   map.flush_pending();

   CHECK(map.is_dead(3));
   CHECK(map.is_dead(4));
   CHECK(map.is_dead(7));
   CHECK(map.is_dead(8));
   CHECK(map.is_dead(10));
   CHECK_FALSE(map.is_dead(5));
   CHECK_FALSE(map.is_dead(9));
   CHECK(map.num_ranges() == 3);  // [3,4], [7,8], [10,10]
}

TEST_CASE("live_range_map: auto-merge at pending capacity", "[live_range_map]")
{
   live_range_map map;

   // Add exactly pending_capacity versions — should auto-merge
   for (uint32_t i = 0; i < live_range_map::pending_capacity; ++i)
      map.add_dead_version(i + 1);

   // Pending should have been flushed
   CHECK(map.pending_count() == 0);
   CHECK(map.num_ranges() == 1);  // [1, 16]

   for (uint32_t i = 1; i <= live_range_map::pending_capacity; ++i)
      CHECK(map.is_dead(i));
   CHECK_FALSE(map.is_dead(0));
   CHECK_FALSE(map.is_dead(live_range_map::pending_capacity + 1));
}

TEST_CASE("live_range_map: large range coalescing", "[live_range_map]")
{
   live_range_map map;

   // Add versions 1..100 in random-ish order
   for (uint64_t i = 1; i <= 100; ++i)
      map.add_dead_version((i * 37) % 100 + 1);  // permutation of 1..100

   map.flush_pending();

   // Should coalesce into a single range [1, 100]
   CHECK(map.num_ranges() == 1);
   for (uint64_t i = 1; i <= 100; ++i)
      CHECK(map.is_dead(i));
   CHECK_FALSE(map.is_dead(0));
   CHECK_FALSE(map.is_dead(101));
}

TEST_CASE("live_range_map: snapshot publish and query", "[live_range_map]")
{
   live_range_map map;
   map.add_dead_version(5);
   map.add_dead_version(6);
   map.add_dead_version(10);
   map.flush_pending();

   // No snapshot published yet
   CHECK(map.load_snapshot() == nullptr);

   map.publish_snapshot();
   auto* snap = map.load_snapshot();
   REQUIRE(snap != nullptr);
   CHECK(snap->num_ranges() == 2);  // [5,6], [10,10]
   CHECK(snap->is_dead(5));
   CHECK(snap->is_dead(6));
   CHECK(snap->is_dead(10));
   CHECK_FALSE(snap->is_dead(7));
   CHECK(snap->oldest_retained_floor() == 0);

   // Add more dead versions and publish again
   map.add_dead_version(7);
   map.add_dead_version(8);
   map.add_dead_version(9);
   map.publish_snapshot();

   auto* snap2 = map.load_snapshot();
   REQUIRE(snap2 != nullptr);
   CHECK(snap2->num_ranges() == 1);  // [5,10]
   CHECK(snap2->is_dead(5));
   CHECK(snap2->is_dead(9));
   CHECK(snap2->is_dead(10));
   CHECK(snap2->oldest_retained_floor() == 0);
}

TEST_CASE("live_range_map: retained floor follows contiguous dead prefix",
          "[live_range_map][floor]")
{
   live_range_map map;

   CHECK(map.oldest_retained_floor() == 0);

   map.add_dead_version(1);
   map.add_dead_version(2);
   map.flush_pending();
   CHECK(map.oldest_retained_floor() == 0);

   map.add_dead_version(0);
   map.flush_pending();
   CHECK(map.oldest_retained_floor() == 3);

   map.add_dead_version(4);
   map.flush_pending();
   CHECK(map.oldest_retained_floor() == 3);

   map.add_dead_version(3);
   map.publish_snapshot();

   auto* snap = map.load_snapshot();
   REQUIRE(snap != nullptr);
   CHECK(snap->oldest_retained_floor() == 5);
   CHECK(snap->is_dead(0));
   CHECK(snap->is_dead(4));
   CHECK_FALSE(snap->is_dead(5));
}

TEST_CASE("live_range_map: empty map queries", "[live_range_map]")
{
   live_range_map map;
   CHECK_FALSE(map.is_dead(0));
   CHECK_FALSE(map.is_dead(1));
   CHECK_FALSE(map.is_dead(UINT64_MAX));
   CHECK(map.num_ranges() == 0);
}

TEST_CASE("live_range_map: duplicate version insertion", "[live_range_map]")
{
   live_range_map map;
   map.add_dead_version(5);
   map.add_dead_version(5);
   map.add_dead_version(5);
   map.flush_pending();

   CHECK(map.is_dead(5));
   CHECK(map.num_ranges() == 1);
}

TEST_CASE("live_range_map: batch add_dead_versions", "[live_range_map]")
{
   live_range_map map;
   uint64_t       versions[] = {3, 1, 4, 1, 5, 9, 2, 6};
   map.add_dead_versions(versions, 8);
   map.flush_pending();

   CHECK(map.is_dead(1));
   CHECK(map.is_dead(6));
   CHECK(map.is_dead(9));
   CHECK_FALSE(map.is_dead(7));
   CHECK_FALSE(map.is_dead(8));
   CHECK(map.num_ranges() == 2);  // [1,6], [9,9]
}
