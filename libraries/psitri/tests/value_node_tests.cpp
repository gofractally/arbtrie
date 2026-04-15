#include <catch2/catch_all.hpp>
#include <cstdlib>
#include <cstring>
#include <psitri/node/value_node.hpp>
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
