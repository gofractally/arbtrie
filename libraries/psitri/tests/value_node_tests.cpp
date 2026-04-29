#include <catch2/catch_all.hpp>
#include <cstdlib>
#include <cstring>
#include <psitri/database.hpp>
#include <psitri/node/value_node.hpp>
#include <psitri/version_compare.hpp>
#include <string>
#include <vector>

namespace
{
   struct AlignedDeleter
   {
      void operator()(void* ptr) const { std::free(ptr); }
   };

   struct ValueNodeDeleter
   {
      void operator()(psitri::value_node* node) const
      {
         if (node)
         {
            node->~value_node();
            std::free(node);
         }
      }
   };

   using ValueNodePtr = std::unique_ptr<psitri::value_node, ValueNodeDeleter>;

   ValueNodePtr create_value_node(psitri::value_view v)
   {
      constexpr size_t alignment = 64;
      uint32_t         asize     = psitri::value_node::alloc_size(v);

      void* buffer = std::aligned_alloc(alignment, asize);
      REQUIRE(buffer != nullptr);
      std::memset(buffer, 0, asize);

      psitri::ptr_address_seq seq = {psitri::ptr_address(0), 0};
      auto* node = new (buffer) psitri::value_node(asize, seq, v);
      return ValueNodePtr(node);
   }

   ValueNodePtr create_value_node(const psitri::value_type& v)
   {
      constexpr size_t alignment = 64;
      uint32_t         asize     = psitri::value_node::alloc_size(v);

      void* buffer = std::aligned_alloc(alignment, asize);
      REQUIRE(buffer != nullptr);
      std::memset(buffer, 0, asize);

      psitri::ptr_address_seq seq = {psitri::ptr_address(0), 0};
      auto* node = new (buffer) psitri::value_node(asize, seq, v);
      return ValueNodePtr(node);
   }
}  // namespace

TEST_CASE("version comparison helpers are ring-aware", "[mvcc][wrap]")
{
   constexpr uint64_t mask39 = (uint64_t(1) << psitri::last_unique_version_bits) - 1;

   CHECK(psitri::version_token(mask39 + 1, psitri::last_unique_version_bits) == 0);
   CHECK(psitri::version_distance(mask39 - 1, 1, psitri::last_unique_version_bits) == 3);
   CHECK(psitri::needs_unique_refresh(mask39 - 1, 0, 1));
   CHECK_FALSE(psitri::needs_unique_refresh(0, 0, 1));
   CHECK(psitri::version_newer_than(1, mask39 - 1, psitri::last_unique_version_bits));
   CHECK_FALSE(psitri::version_newer_than(mask39 - 1, 1, psitri::last_unique_version_bits));
}

TEST_CASE("value_node single data value", "[value_node]")
{
   std::string data = "hello world, this is a test value";
   auto        node = create_value_node(psitri::value_view(data.data(), data.size()));

   SECTION("basic properties")
   {
      CHECK(node->num_versions() == 1);
      CHECK(node->num_next() == 0);
      CHECK(node->is_subtree_container() == false);
      CHECK(node->latest_version() == 0);
   }

   SECTION("get_data returns correct value")
   {
      auto vv = node->get_data();
      CHECK(vv.size() == data.size());
      CHECK(std::string(vv.data(), vv.size()) == data);
   }

   SECTION("get_value_at_version returns correct value")
   {
      auto vv = node->get_value_at_version(0);
      CHECK(vv.size() == data.size());
      CHECK(std::string(vv.data(), vv.size()) == data);

      // Any version >= 0 should return the same value (latest <= requested)
      auto vv2 = node->get_value_at_version(100);
      CHECK(vv2.size() == data.size());
   }

   SECTION("entry packing")
   {
      CHECK(node->get_entry_version(0) == 0);
      CHECK(node->get_entry_offset(0) >= psitri::value_node::offset_data_start);
   }
}

TEST_CASE("value_node empty value", "[value_node]")
{
   auto node = create_value_node(psitri::value_view());

   CHECK(node->num_versions() == 1);
   CHECK(node->get_entry_offset(0) == psitri::value_node::offset_zero_length);
   CHECK(node->get_value_at_version(0).empty());
}

TEST_CASE("value_node large value", "[value_node]")
{
   std::string large(500, 'X');
   auto node = create_value_node(psitri::value_view(large.data(), large.size()));

   CHECK(node->num_versions() == 1);
   auto vv = node->get_data();
   CHECK(vv.size() == 500);
   CHECK(std::string(vv.data(), vv.size()) == large);
}

TEST_CASE("value_node subtree value", "[value_node]")
{
   sal::tree_id tid{psitri::ptr_address(42), psitri::ptr_address(99)};
   auto         vt = psitri::value_type::make_subtree(tid);
   auto         node = create_value_node(vt);

   SECTION("basic properties")
   {
      CHECK(node->num_versions() == 1);
      CHECK(node->is_subtree_container() == true);
      CHECK(node->num_next() == 0);
   }

   SECTION("get_tree_id returns correct value")
   {
      auto result = node->get_tree_id();
      CHECK(result.root == psitri::ptr_address(42));
      CHECK(result.ver == psitri::ptr_address(99));
   }

   SECTION("visit_branches yields tree_id addresses")
   {
      std::vector<psitri::ptr_address> visited;
      node->visit_branches([&](psitri::ptr_address addr) { visited.push_back(addr); });
      REQUIRE(visited.size() == 2);
      CHECK(visited[0] == psitri::ptr_address(42));  // root
      CHECK(visited[1] == psitri::ptr_address(99));  // ver
   }
}

TEST_CASE("value_node subtree with null ver", "[value_node]")
{
   sal::tree_id tid{psitri::ptr_address(42), sal::null_ptr_address};
   auto         vt = psitri::value_type::make_subtree(tid);
   auto         node = create_value_node(vt);

   std::vector<psitri::ptr_address> visited;
   node->visit_branches([&](psitri::ptr_address addr) { visited.push_back(addr); });
   REQUIRE(visited.size() == 1);
   CHECK(visited[0] == psitri::ptr_address(42));  // only root, ver is null
}

TEST_CASE("value_node data value has no branches", "[value_node]")
{
   auto node = create_value_node(psitri::value_view("test", 4));

   std::vector<psitri::ptr_address> visited;
   node->visit_branches([&](psitri::ptr_address addr) { visited.push_back(addr); });
   CHECK(visited.empty());
}

TEST_CASE("value_node entry pack/unpack roundtrip", "[value_node]")
{
   SECTION("normal version and offset")
   {
      uint64_t entry = psitri::value_node::pack_entry(12345, 100);
      CHECK(psitri::value_node::entry_version(entry) == 12345);
      CHECK(psitri::value_node::entry_offset(entry) == 100);
   }

   SECTION("large version")
   {
      uint64_t ver   = (1ULL << 48) - 1;  // max 48-bit version
      uint64_t entry = psitri::value_node::pack_entry(ver, 500);
      CHECK(psitri::value_node::entry_version(entry) == ver);
      CHECK(psitri::value_node::entry_offset(entry) == 500);
   }

   SECTION("negative offsets (tombstone, null)")
   {
      uint64_t entry_tomb = psitri::value_node::pack_entry(42, psitri::value_node::offset_tombstone);
      CHECK(psitri::value_node::entry_version(entry_tomb) == 42);
      CHECK(psitri::value_node::entry_offset(entry_tomb) == psitri::value_node::offset_tombstone);

      uint64_t entry_null = psitri::value_node::pack_entry(42, psitri::value_node::offset_null);
      CHECK(psitri::value_node::entry_version(entry_null) == 42);
      CHECK(psitri::value_node::entry_offset(entry_null) == psitri::value_node::offset_null);
   }

   SECTION("sort order preserved")
   {
      // version dominates upper bits, so entries with higher version sort higher
      uint64_t e1 = psitri::value_node::pack_entry(1, 100);
      uint64_t e2 = psitri::value_node::pack_entry(2, 50);
      uint64_t e3 = psitri::value_node::pack_entry(3, 200);
      CHECK(e1 < e2);
      CHECK(e2 < e3);

      // Same version, different offsets — still preserves order
      uint64_t e4 = psitri::value_node::pack_entry(10, 3);
      uint64_t e5 = psitri::value_node::pack_entry(10, 100);
      CHECK(e4 < e5);
   }
}

TEST_CASE("value_node alloc_size", "[value_node]")
{
   SECTION("small value rounds to 64")
   {
      auto asize = psitri::value_node::alloc_size(psitri::value_view("hi", 2));
      CHECK(asize == 64);
      CHECK(asize % 64 == 0);
   }

   SECTION("large value")
   {
      std::string large(500, 'X');
      auto asize = psitri::value_node::alloc_size(psitri::value_view(large.data(), large.size()));
      CHECK(asize >= 16 + 8 + 2 + 500);  // header + entry + size_field + data
      CHECK(asize % 64 == 0);
   }

   SECTION("subtree value")
   {
      auto vt    = psitri::value_type::make_subtree(sal::tree_id{psitri::ptr_address(1), psitri::ptr_address(2)});
      auto asize = psitri::value_node::alloc_size(vt);
      CHECK(asize >= 16 + 8 + 2 + sizeof(sal::tree_id));
      CHECK(asize % 64 == 0);
   }
}

TEST_CASE("version48 pack/unpack", "[value_node]")
{
   psitri::version48 v;

   SECTION("zero")
   {
      v.set(0);
      CHECK(v.get() == 0);
   }

   SECTION("small value")
   {
      v.set(12345);
      CHECK(v.get() == 12345);
   }

   SECTION("max 48-bit value")
   {
      uint64_t max48 = (1ULL << 48) - 1;
      v.set(max48);
      CHECK(v.get() == max48);
   }

   SECTION("mid-range value")
   {
      uint64_t val = 0xDEADBEEF42ULL;
      v.set(val);
      CHECK(v.get() == val);
   }
}

TEST_CASE("value_next_ptr layout", "[value_node]")
{
   CHECK(sizeof(psitri::value_next_ptr) == 16);
   CHECK(sizeof(psitri::version48) == 6);

   psitri::value_next_ptr np;
   np.ptr = psitri::ptr_address(100);
   np.low_ver.set(10);
   np.high_ver.set(20);

   CHECK(np.ptr == psitri::ptr_address(100));
   CHECK(np.low_ver.get() == 10);
   CHECK(np.high_ver.get() == 20);
}

TEST_CASE("value_node struct size", "[value_node]")
{
   CHECK(sizeof(psitri::value_node) == 16);
}

namespace
{
   // Helpers to construct value_node variants on a heap-allocated, aligned
   // buffer. Lifetime is managed via ValueNodeDeleter.
   ValueNodePtr append_version(const psitri::value_node* src, uint64_t version,
                               psitri::value_view new_val)
   {
      uint32_t asize  = psitri::value_node::alloc_size(src, version, new_val);
      void*    buffer = std::aligned_alloc(64, asize);
      REQUIRE(buffer != nullptr);
      std::memset(buffer, 0, asize);
      psitri::ptr_address_seq seq = {psitri::ptr_address(0), 0};
      auto* node =
          new (buffer) psitri::value_node(asize, seq, src, version, new_val);
      return ValueNodePtr(node);
   }

   ValueNodePtr replace_last_version(const psitri::value_node* src, uint64_t version,
                                     psitri::value_view new_val)
   {
      // Same alloc_size as append (over-allocates by ~one entry slot;
      // not worth a separate overload). Construction uses replace_last_tag.
      uint32_t asize  = psitri::value_node::alloc_size(src, version, new_val);
      void*    buffer = std::aligned_alloc(64, asize);
      REQUIRE(buffer != nullptr);
      std::memset(buffer, 0, asize);
      psitri::ptr_address_seq seq = {psitri::ptr_address(0), 0};
      auto* node = new (buffer) psitri::value_node(
          asize, seq, src, version, new_val,
          psitri::value_node::replace_last_tag{});
      return ValueNodePtr(node);
   }

   ValueNodePtr append_tombstone(const psitri::value_node* src, uint64_t version)
   {
      uint32_t asize  = psitri::value_node::alloc_size(src, version, nullptr);
      void*    buffer = std::aligned_alloc(64, asize);
      REQUIRE(buffer != nullptr);
      std::memset(buffer, 0, asize);
      psitri::ptr_address_seq seq = {psitri::ptr_address(0), 0};
      auto* node = new (buffer) psitri::value_node(asize, seq, src, version, nullptr);
      return ValueNodePtr(node);
   }

   ValueNodePtr replace_last_with_tombstone(const psitri::value_node* src, uint64_t version)
   {
      uint32_t asize  = psitri::value_node::alloc_size(src, version, nullptr,
                                                       psitri::value_node::replace_last_tag{});
      void*    buffer = std::aligned_alloc(64, asize);
      REQUIRE(buffer != nullptr);
      std::memset(buffer, 0, asize);
      psitri::ptr_address_seq seq = {psitri::ptr_address(0), 0};
      auto* node = new (buffer)
          psitri::value_node(asize, seq, src, version, nullptr,
                             psitri::value_node::replace_last_tag{});
      return ValueNodePtr(node);
   }

   ValueNodePtr copy_with_dead_snapshot(const psitri::value_node*     src,
                                        const psitri::live_range_map::snapshot* dead)
   {
      uint32_t asize  = psitri::value_node::alloc_size(src, dead);
      void*    buffer = std::aligned_alloc(64, asize);
      REQUIRE(buffer != nullptr);
      std::memset(buffer, 0, asize);
      psitri::ptr_address_seq seq = {psitri::ptr_address(0), 0};
      auto* node = new (buffer) psitri::value_node(asize, seq, src, dead);
      return ValueNodePtr(node);
   }

   ValueNodePtr copy_with_prune_floor(const psitri::value_node* src, uint64_t floor)
   {
      psitri::value_node::prune_floor_policy prune{floor};
      uint32_t asize  = psitri::value_node::alloc_size(src, prune);
      void*    buffer = std::aligned_alloc(64, asize);
      REQUIRE(buffer != nullptr);
      std::memset(buffer, 0, asize);
      psitri::ptr_address_seq seq = {psitri::ptr_address(0), 0};
      auto* node = new (buffer) psitri::value_node(asize, seq, src, prune);
      return ValueNodePtr(node);
   }

   ValueNodePtr append_with_prune_floor(const psitri::value_node* src,
                                        uint64_t                  floor,
                                        uint64_t                  version,
                                        psitri::value_view        new_val)
   {
      psitri::value_node::prune_floor_policy prune{floor};
      uint32_t asize  = psitri::value_node::alloc_size(src, version, new_val, prune);
      void*    buffer = std::aligned_alloc(64, asize);
      REQUIRE(buffer != nullptr);
      std::memset(buffer, 0, asize);
      psitri::ptr_address_seq seq = {psitri::ptr_address(0), 0};
      auto* node = new (buffer) psitri::value_node(asize, seq, src, version, new_val, prune);
      return ValueNodePtr(node);
   }

   sal::tree_id entry_tree_id(const psitri::value_node* node, uint8_t idx)
   {
      auto view = node->get_entry_value(idx);
      REQUIRE(view.size() == sizeof(sal::tree_id));
      sal::tree_id tid{};
      std::memcpy(&tid, view.data(), sizeof(tid));
      return tid;
   }

   void check_entry_tree_id(const psitri::value_node* node, uint8_t idx, sal::tree_id expected)
   {
      sal::tree_id actual = entry_tree_id(node, idx);
      CHECK(actual.root == expected.root);
      CHECK(actual.ver == expected.ver);
   }

}

TEST_CASE("value_node copy_to larger allocation preserves tail-relative values",
          "[value_node][cow]")
{
   auto v0 = create_value_node(psitri::value_view("v0", 2));
   auto v5 = append_version(v0.get(), 5, psitri::value_view("value-five", 10));

   const uint32_t asize = v5->cow_size();
   REQUIRE(asize > v5->size());

   void* buffer = std::aligned_alloc(64, asize);
   REQUIRE(buffer != nullptr);
   std::memset(buffer, 0, asize);

   psitri::ptr_address_seq seq = {psitri::ptr_address(123), 7};
   auto* header = new (buffer)
       sal::alloc_header(asize, static_cast<sal::header_type>(psitri::value_node::type_id), seq);

   v5->copy_to(header);
   ValueNodePtr copy(reinterpret_cast<psitri::value_node*>(header));

   CHECK(copy->size() == asize);
   CHECK(copy->address_seq().address == seq.address);
   CHECK(copy->address_seq().sequence == seq.sequence);
   REQUIRE(copy->num_versions() == 2);
   CHECK(copy->get_entry_version(0) == 0);
   CHECK(copy->get_entry_version(1) == 5);
   CHECK(std::string(copy->get_entry_value(0)) == "v0");
   CHECK(std::string(copy->get_entry_value(1)) == "value-five");
}

TEST_CASE("value_node coalesce: append vs replace_last", "[value_node][coalesce]")
{
   // Build a value_node with version 1 — single entry chain
   auto v0 = create_value_node(psitri::value_view("v1", 2));
   REQUIRE(v0->num_versions() == 1);
   CHECK(v0->latest_version() == 0);  // initial version is 0

   // Append v=42 (regular append) — chain grows to 2 entries
   auto v1 = append_version(v0.get(), 42, psitri::value_view("v42", 3));
   REQUIRE(v1->num_versions() == 2);
   CHECK(v1->latest_version() == 42);

   // Now replace-last from v1 with v=42 (same txn, second update)
   auto v2 = replace_last_version(v1.get(), 42, psitri::value_view("v42b", 4));
   REQUIRE(v2->num_versions() == 2);  // still 2, not 3
   CHECK(v2->latest_version() == 42);
   // Top entry should be the new value, not the prior v=42 value
   psitri::value_view latest = v2->get_data();
   REQUIRE(latest.size() == 4);
   CHECK(std::string(latest.data(), latest.size()) == "v42b");

   // The entry below the top should still be the original (version 0)
   CHECK(v2->get_entry_version(0) == 0);

   // Replacing again — chain still 2 entries
   auto v3 = replace_last_version(v2.get(), 42, psitri::value_view("v42c", 4));
   REQUIRE(v3->num_versions() == 2);
   CHECK(v3->latest_version() == 42);
   psitri::value_view final = v3->get_data();
   CHECK(std::string(final.data(), final.size()) == "v42c");
}

TEST_CASE("value_node coalesce: replace_last on single-version source", "[value_node][coalesce]")
{
   // Edge case: source has exactly one entry. replace_last drops it,
   // append adds the new one. Result should still be 1 entry.
   auto v0 = create_value_node(psitri::value_view("orig", 4));
   REQUIRE(v0->num_versions() == 1);

   auto v1 = replace_last_version(v0.get(), 5, psitri::value_view("new", 3));
   REQUIRE(v1->num_versions() == 1);
   CHECK(v1->latest_version() == 5);
   psitri::value_view final = v1->get_data();
   CHECK(std::string(final.data(), final.size()) == "new");
}

TEST_CASE("value_node lookup uses 48-bit circular version order", "[value_node][mvcc][wrap]")
{
   constexpr uint64_t max48 = (uint64_t(1) << 48) - 1;

   auto v0 = create_value_node(psitri::value_view("seed", 4));
   auto v1 = replace_last_version(v0.get(), max48 - 1, psitri::value_view("pre", 3));
   auto v2 = append_version(v1.get(), max48, psitri::value_view("max", 3));
   auto v3 = append_version(v2.get(), max48 + 1, psitri::value_view("zero", 4));
   auto v4 = append_version(v3.get(), max48 + 2, psitri::value_view("one", 3));

   CHECK(v4->get_entry_version(0) == max48 - 1);
   CHECK(v4->get_entry_version(1) == max48);
   CHECK(v4->get_entry_version(2) == 0);
   CHECK(v4->get_entry_version(3) == 1);
   CHECK(v4->latest_version() == 1);

   auto at_pre = v4->get_value_at_version(max48 - 1);
   CHECK(std::string(at_pre.data(), at_pre.size()) == "pre");

   CHECK(v4->get_value_at_version(max48 - 2).empty());

   auto at_max = v4->get_value_at_version(max48);
   CHECK(std::string(at_max.data(), at_max.size()) == "max");

   auto at_zero = v4->get_value_at_version(0);
   CHECK(std::string(at_zero.data(), at_zero.size()) == "zero");

   auto at_one = v4->get_value_at_version(1);
   CHECK(std::string(at_one.data(), at_one.size()) == "one");

   auto future = v4->get_value_at_version(2);
   CHECK(std::string(future.data(), future.size()) == "one");
}

TEST_CASE("value_node prune floor preserves floor-visible state", "[value_node][mvcc][prune]")
{
   auto v0  = create_value_node(psitri::value_view("seed", 4));
   auto v10 = replace_last_version(v0.get(), 10, psitri::value_view("A", 1));
   auto v20 = append_version(v10.get(), 20, psitri::value_view("B", 1));
   auto v30 = append_version(v20.get(), 30, psitri::value_view("C", 1));
   auto v40 = append_version(v30.get(), 40, psitri::value_view("D", 1));

   auto pruned = copy_with_prune_floor(v40.get(), 25);

   REQUIRE(pruned->num_versions() == 3);
   CHECK(pruned->get_entry_version(0) == 25);
   CHECK(std::string(pruned->get_entry_value(0).data(), pruned->get_entry_value(0).size()) == "B");
   CHECK(pruned->get_entry_version(1) == 30);
   CHECK(std::string(pruned->get_entry_value(1).data(), pruned->get_entry_value(1).size()) == "C");
   CHECK(pruned->get_entry_version(2) == 40);
   CHECK(std::string(pruned->get_entry_value(2).data(), pruned->get_entry_value(2).size()) == "D");

   CHECK(pruned->get_value_at_version(24).empty());
   auto at_floor = pruned->get_value_at_version(25);
   CHECK(std::string(at_floor.data(), at_floor.size()) == "B");
   auto at_29 = pruned->get_value_at_version(29);
   CHECK(std::string(at_29.data(), at_29.size()) == "B");
}

TEST_CASE("value_node prune floor preserves exact floor entry", "[value_node][mvcc][prune]")
{
   auto v0  = create_value_node(psitri::value_view("seed", 4));
   auto v10 = replace_last_version(v0.get(), 10, psitri::value_view("A", 1));
   auto v20 = append_version(v10.get(), 20, psitri::value_view("B", 1));
   auto v30 = append_version(v20.get(), 30, psitri::value_view("C", 1));

   auto pruned = copy_with_prune_floor(v30.get(), 20);

   REQUIRE(pruned->num_versions() == 2);
   CHECK(pruned->get_entry_version(0) == 20);
   CHECK(std::string(pruned->get_entry_value(0).data(), pruned->get_entry_value(0).size()) == "B");
   CHECK(pruned->get_entry_version(1) == 30);
   CHECK(std::string(pruned->get_entry_value(1).data(), pruned->get_entry_value(1).size()) == "C");
}

TEST_CASE("value_node prune floor preserves absent-at-floor state", "[value_node][mvcc][prune]")
{
   auto v0  = create_value_node(psitri::value_view("seed", 4));
   auto v10 = replace_last_version(v0.get(), 10, psitri::value_view("A", 1));
   auto v20 = append_version(v10.get(), 20, psitri::value_view("B", 1));

   auto pruned = copy_with_prune_floor(v20.get(), 5);

   REQUIRE(pruned->num_versions() == 2);
   CHECK(pruned->get_entry_version(0) == 10);
   CHECK(std::string(pruned->get_entry_value(0).data(), pruned->get_entry_value(0).size()) == "A");
   CHECK(pruned->get_entry_version(1) == 20);
   CHECK(std::string(pruned->get_entry_value(1).data(), pruned->get_entry_value(1).size()) == "B");
   CHECK(pruned->get_value_at_version(5).empty());
}

TEST_CASE("value_node prune floor preserves tombstones", "[value_node][mvcc][prune]")
{
   auto v0  = create_value_node(psitri::value_view("seed", 4));
   auto v10 = replace_last_version(v0.get(), 10, psitri::value_view("A", 1));
   auto v20 = append_tombstone(v10.get(), 20);
   auto v40 = append_version(v20.get(), 40, psitri::value_view("B", 1));

   auto pruned = copy_with_prune_floor(v40.get(), 25);

   REQUIRE(pruned->num_versions() == 2);
   CHECK(pruned->get_entry_version(0) == 25);
   CHECK(pruned->get_entry_offset(0) == psitri::value_node::offset_tombstone);
   CHECK(pruned->get_entry_version(1) == 40);
   CHECK(std::string(pruned->get_entry_value(1).data(), pruned->get_entry_value(1).size()) == "B");

   auto [floor_offset, floor_idx] = pruned->find_version(25);
   CHECK(floor_idx == 0);
   CHECK(floor_offset == psitri::value_node::offset_tombstone);
}

TEST_CASE("value_node prune floor is ring-aware", "[value_node][mvcc][prune][wrap]")
{
   constexpr uint64_t max48 = (uint64_t(1) << 48) - 1;

   auto v0 = create_value_node(psitri::value_view("seed", 4));
   auto v1 = replace_last_version(v0.get(), max48 - 1, psitri::value_view("pre", 3));
   auto v2 = append_version(v1.get(), max48, psitri::value_view("max", 3));
   auto v3 = append_version(v2.get(), max48 + 1, psitri::value_view("zero", 4));
   auto v4 = append_version(v3.get(), max48 + 2, psitri::value_view("one", 3));

   auto pruned = copy_with_prune_floor(v4.get(), max48);

   REQUIRE(pruned->num_versions() == 3);
   CHECK(pruned->get_entry_version(0) == max48);
   CHECK(std::string(pruned->get_entry_value(0).data(), pruned->get_entry_value(0).size()) == "max");
   CHECK(pruned->get_entry_version(1) == 0);
   CHECK(std::string(pruned->get_entry_value(1).data(), pruned->get_entry_value(1).size()) == "zero");
   CHECK(pruned->get_entry_version(2) == 1);
   CHECK(std::string(pruned->get_entry_value(2).data(), pruned->get_entry_value(2).size()) == "one");
}

TEST_CASE("value_node prune append replaces floor entry at same version",
          "[value_node][mvcc][prune]")
{
   auto v0  = create_value_node(psitri::value_view("seed", 4));
   auto v10 = replace_last_version(v0.get(), 10, psitri::value_view("A", 1));
   auto v20 = append_version(v10.get(), 20, psitri::value_view("B", 1));

   auto v30 = append_with_prune_floor(v20.get(), 30, 30, psitri::value_view("C", 1));

   REQUIRE(v30->num_versions() == 1);
   CHECK(v30->get_entry_version(0) == 30);
   CHECK(std::string(v30->get_entry_value(0).data(), v30->get_entry_value(0).size()) == "C");
}

TEST_CASE("passive value_node relocation prunes data-only history without releases",
          "[value_node][mvcc][prune][passive]")
{
   auto v0 = create_value_node(psitri::value_view("A", 1));
   auto v1 = append_version(v0.get(), 1, psitri::value_view("B", 1));
   auto v2 = append_version(v1.get(), 2, psitri::value_view("C", 1));

   psitri::live_range_map dead;
   dead.add_dead_version(0);
   dead.publish_snapshot();

   psitri::detail::psitri_object_context ctx{dead};
   psitri::detail::psitri_value_node_ops ops(ctx);

   const uint32_t asize  = ops.compact_size(v2.get());
   void*          buffer = std::aligned_alloc(64, asize);
   REQUIRE(buffer != nullptr);
   std::memset(buffer, 0, asize);

   psitri::ptr_address_seq seq = {psitri::ptr_address(77), 3};
   auto* header = new (buffer)
       sal::alloc_header(asize, static_cast<sal::header_type>(psitri::value_node::type_id), seq);

   std::vector<sal::ptr_address> pending_storage;
   sal::pending_release_list pending_releases(pending_storage);
   ops.passive_compact_to(v2.get(), header, pending_releases);

   ValueNodePtr pruned(reinterpret_cast<psitri::value_node*>(header));
   CHECK(pending_releases.empty());
   REQUIRE(pruned->num_versions() == 2);
   CHECK(pruned->get_entry_version(0) == 1);
   CHECK(std::string(pruned->get_entry_value(0).data(), pruned->get_entry_value(0).size()) == "B");
   CHECK(pruned->get_entry_version(1) == 2);
   CHECK(std::string(pruned->get_entry_value(1).data(), pruned->get_entry_value(1).size()) == "C");
}

TEST_CASE("value_node subtree prune reports dropped tree refs",
          "[value_node][mvcc][prune][subtree]")
{
   sal::tree_id tid{psitri::ptr_address(42), psitri::ptr_address(99)};
   auto         sub = create_value_node(psitri::value_type::make_subtree(tid));
   auto         tombstone = append_tombstone(sub.get(), 1);

   psitri::value_node::prune_floor_policy prune{1};
   std::vector<sal::ptr_address> pending_storage;
   sal::pending_release_list pending(pending_storage);

   CHECK(tombstone->collect_pruned_references(prune, pending));
   REQUIRE(pending.size() == 2);
   CHECK(pending[0] == tid.root);
   CHECK(pending[1] == tid.ver);

   auto pruned = copy_with_prune_floor(tombstone.get(), 1);
   REQUIRE(pruned->num_versions() == 1);
   CHECK(pruned->get_entry_version(0) == 1);
   CHECK(pruned->get_entry_offset(0) == psitri::value_node::offset_tombstone);
}

TEST_CASE("value_node subtree prune keeps the retained predecessor ref",
          "[value_node][mvcc][prune][subtree]")
{
   sal::tree_id tid{psitri::ptr_address(42), psitri::ptr_address(99)};
   auto         sub = create_value_node(psitri::value_type::make_subtree(tid));
   auto         tombstone = append_tombstone(sub.get(), 10);

   SECTION("exact floor keeps the subtree entry without releasing it")
   {
      psitri::value_node::prune_floor_policy prune{0};
      std::vector<sal::ptr_address> pending_storage;
      sal::pending_release_list pending(pending_storage);

      CHECK(tombstone->collect_pruned_references(prune, pending));
      CHECK(pending.empty());

      auto pruned = copy_with_prune_floor(tombstone.get(), 0);
      REQUIRE(pruned->num_versions() == 2);
      CHECK(pruned->get_entry_version(0) == 0);
      check_entry_tree_id(pruned.get(), 0, tid);
      CHECK(pruned->get_entry_version(1) == 10);
      CHECK(pruned->get_entry_offset(1) == psitri::value_node::offset_tombstone);
   }

   SECTION("floor between subtree and tombstone rewrites but keeps the same ref")
   {
      psitri::value_node::prune_floor_policy prune{5};
      std::vector<sal::ptr_address> pending_storage;
      sal::pending_release_list pending(pending_storage);

      CHECK(tombstone->collect_pruned_references(prune, pending));
      CHECK(pending.empty());

      auto pruned = copy_with_prune_floor(tombstone.get(), 5);
      REQUIRE(pruned->num_versions() == 2);
      CHECK(pruned->get_entry_version(0) == 5);
      check_entry_tree_id(pruned.get(), 0, tid);
      CHECK(pruned->get_entry_version(1) == 10);
      CHECK(pruned->get_entry_offset(1) == psitri::value_node::offset_tombstone);
   }
}

TEST_CASE("value_node subtree prune handles null version refs and overflow",
          "[value_node][mvcc][prune][subtree]")
{
   sal::tree_id tid{psitri::ptr_address(42), sal::null_ptr_address};
   auto         sub = create_value_node(psitri::value_type::make_subtree(tid));
   auto         tombstone = append_tombstone(sub.get(), 1);

   SECTION("null ver only queues the subtree root")
   {
      psitri::value_node::prune_floor_policy prune{1};
      std::vector<sal::ptr_address> pending_storage;
      sal::pending_release_list pending(pending_storage);

      CHECK(tombstone->collect_pruned_references(prune, pending));
      REQUIRE(pending.size() == 1);
      CHECK(pending[0] == tid.root);

      auto pruned = copy_with_prune_floor(tombstone.get(), 1);
      REQUIRE(pruned->num_versions() == 1);
      CHECK(pruned->get_entry_version(0) == 1);
      CHECK(pruned->get_entry_offset(0) == psitri::value_node::offset_tombstone);
   }

   SECTION("release storage grows for passive collection")
   {
      sal::tree_id two_refs{psitri::ptr_address(42), psitri::ptr_address(99)};
      auto         two_ref_sub = create_value_node(psitri::value_type::make_subtree(two_refs));
      auto         two_ref_tombstone = append_tombstone(two_ref_sub.get(), 1);

      psitri::value_node::prune_floor_policy prune{1};
      std::vector<sal::ptr_address> pending_storage;
      pending_storage.reserve(1);
      sal::pending_release_list pending(pending_storage);

      CHECK(two_ref_tombstone->collect_pruned_references(prune, pending));
      CHECK_FALSE(pending.failed());
      REQUIRE(pending.size() == 2);
      CHECK(pending[0] == two_refs.root);
      CHECK(pending[1] == two_refs.ver);
   }
}

TEST_CASE("value_node subtree dead-entry cleanup does not release the latest value",
          "[value_node][mvcc][subtree]")
{
   sal::tree_id tid{psitri::ptr_address(70), psitri::ptr_address(71)};

   SECTION("dead latest entry is preserved")
   {
      auto sub = create_value_node(psitri::value_type::make_subtree(tid));

      psitri::live_range_map dead;
      dead.add_dead_version(0);
      dead.publish_snapshot();

      std::vector<sal::ptr_address> pending_storage;
      sal::pending_release_list pending(pending_storage);

      CHECK(sub->collect_dead_references(dead.load_snapshot(), pending));
      CHECK(pending.empty());

      auto copied = copy_with_dead_snapshot(sub.get(), dead.load_snapshot());
      REQUIRE(copied->num_versions() == 1);
      CHECK(copied->get_entry_version(0) == 0);
      check_entry_tree_id(copied.get(), 0, tid);
   }

   SECTION("dead non-latest subtree entry is released")
   {
      auto sub = create_value_node(psitri::value_type::make_subtree(tid));
      auto tombstone = append_tombstone(sub.get(), 1);

      psitri::live_range_map dead;
      dead.add_dead_version(0);
      dead.publish_snapshot();

      std::vector<sal::ptr_address> pending_storage;
      sal::pending_release_list pending(pending_storage);

      CHECK(tombstone->collect_dead_references(dead.load_snapshot(), pending));
      REQUIRE(pending.size() == 2);
      CHECK(pending[0] == tid.root);
      CHECK(pending[1] == tid.ver);

      auto copied = copy_with_dead_snapshot(tombstone.get(), dead.load_snapshot());
      REQUIRE(copied->num_versions() == 1);
      CHECK(copied->get_entry_version(0) == 1);
      CHECK(copied->get_entry_offset(0) == psitri::value_node::offset_tombstone);
   }
}

TEST_CASE("value_node subtree replace-last reports replaced tree refs",
          "[value_node][mvcc][subtree]")
{
   sal::tree_id tid{psitri::ptr_address(7), psitri::ptr_address(8)};
   auto         sub = create_value_node(psitri::value_type::make_subtree(tid));

   std::vector<sal::ptr_address> pending_storage;
   sal::pending_release_list pending(pending_storage);

   CHECK(sub->collect_replace_last_references(pending));
   REQUIRE(pending.size() == 2);
   CHECK(pending[0] == tid.root);
   CHECK(pending[1] == tid.ver);

   auto replaced = replace_last_with_tombstone(sub.get(), 0);
   REQUIRE(replaced->num_versions() == 1);
   CHECK(replaced->get_entry_version(0) == 0);
   CHECK(replaced->get_entry_offset(0) == psitri::value_node::offset_tombstone);
}

TEST_CASE("value_node subtree top replacement helper only releases matching top version",
          "[value_node][mvcc][prune][subtree]")
{
   sal::tree_id tid{psitri::ptr_address(7), psitri::ptr_address(8)};
   auto         sub = create_value_node(psitri::value_type::make_subtree(tid));

   std::vector<sal::ptr_address> pending_storage;
   sal::pending_release_list pending(pending_storage);
   CHECK(sub->collect_replaced_top_references(0, pending));
   REQUIRE(pending.size() == 2);
   CHECK(pending[0] == tid.root);
   CHECK(pending[1] == tid.ver);

   std::vector<sal::ptr_address> untouched_storage;
   sal::pending_release_list untouched(untouched_storage);
   CHECK(sub->collect_replaced_top_references(1, untouched));
   CHECK(untouched.empty());
}

TEST_CASE("passive value_node relocation prunes subtree history with pending releases",
          "[value_node][mvcc][prune][passive][subtree]")
{
   sal::tree_id tid{psitri::ptr_address(100), psitri::ptr_address(101)};
   auto         sub = create_value_node(psitri::value_type::make_subtree(tid));
   auto         tombstone = append_tombstone(sub.get(), 1);

   psitri::live_range_map dead;
   dead.add_dead_version(0);
   dead.publish_snapshot();

   psitri::detail::psitri_object_context ctx{dead};
   psitri::detail::psitri_value_node_ops ops(ctx);

   const uint32_t asize  = ops.compact_size(tombstone.get());
   void*          buffer = std::aligned_alloc(64, asize);
   REQUIRE(buffer != nullptr);
   std::memset(buffer, 0, asize);

   psitri::ptr_address_seq seq = {psitri::ptr_address(78), 4};
   auto* header = new (buffer)
       sal::alloc_header(asize, static_cast<sal::header_type>(psitri::value_node::type_id), seq);

   std::vector<sal::ptr_address> pending_storage;
   sal::pending_release_list pending(pending_storage);
   ops.passive_compact_to(tombstone.get(), header, pending);

   ValueNodePtr pruned(reinterpret_cast<psitri::value_node*>(header));
   REQUIRE(pending.size() == 2);
   CHECK(pending[0] == tid.root);
   CHECK(pending[1] == tid.ver);
   REQUIRE(pruned->num_versions() == 1);
   CHECK(pruned->get_entry_version(0) == 1);
   CHECK(pruned->get_entry_offset(0) == psitri::value_node::offset_tombstone);
}
