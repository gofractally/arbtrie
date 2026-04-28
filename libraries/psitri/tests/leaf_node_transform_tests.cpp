#include <catch2/catch_all.hpp>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <psitri/node/leaf.hpp>
#include <psitri/value_type.hpp>

using namespace psitri;

namespace
{
   struct leaf_deleter
   {
      void operator()(leaf_node* ptr) const noexcept
      {
         if (!ptr)
            return;
         ptr->~leaf_node();
         std::free(ptr);
      }
   };

   using leaf_ptr = std::unique_ptr<leaf_node, leaf_deleter>;

   void* alloc_leaf_buffer()
   {
      void* ptr = std::aligned_alloc(64, leaf_node::max_leaf_size);
      REQUIRE(ptr != nullptr);
      std::memset(ptr, 0, leaf_node::max_leaf_size);
      return ptr;
   }

   ptr_address_seq next_seq(void* ptr)
   {
      static uint32_t seq = 1;
      return {ptr_address(static_cast<uint32_t>(reinterpret_cast<uintptr_t>(ptr))), seq++};
   }

   leaf_ptr make_leaf(key_view key, value_type value)
   {
      void* buf = alloc_leaf_buffer();
      return leaf_ptr(new (buf) leaf_node(leaf_node::max_leaf_size, next_seq(buf), key, value));
   }

   leaf_ptr clone_leaf(const leaf_node& src)
   {
      void* buf = alloc_leaf_buffer();
      return leaf_ptr(new (buf) leaf_node(leaf_node::max_leaf_size, next_seq(buf), &src));
   }

   leaf_ptr insert_leaf(const leaf_node& src,
                        key_view         key,
                        value_type       value,
                        uint64_t         created_at = 0)
   {
      op::leaf_insert op{.src        = src,
                         .lb         = src.lower_bound(key),
                         .key        = key,
                         .value      = value,
                         .created_at = created_at};
      REQUIRE(src.can_apply(op) != leaf_node::can_apply_mode::none);

      void* buf = alloc_leaf_buffer();
      return leaf_ptr(new (buf) leaf_node(leaf_node::max_leaf_size, next_seq(buf), &src, op));
   }

   leaf_ptr update_leaf(const leaf_node& src, key_view key, value_type value)
   {
      auto bn = src.get(key);
      REQUIRE(bn != src.num_branches());

      op::leaf_update op{.src = src, .lb = bn, .key = key, .value = value};
      REQUIRE(src.can_apply(op) != leaf_node::can_apply_mode::none);

      void* buf = alloc_leaf_buffer();
      return leaf_ptr(new (buf) leaf_node(leaf_node::max_leaf_size, next_seq(buf), op));
   }

   leaf_ptr remove_leaf(const leaf_node& src, key_view key)
   {
      auto bn = src.get(key);
      REQUIRE(bn != src.num_branches());

      op::leaf_remove op{.src = src, .bn = bn};
      void*           buf = alloc_leaf_buffer();
      return leaf_ptr(new (buf) leaf_node(leaf_node::max_leaf_size, next_seq(buf), op));
   }

   leaf_ptr remove_range_leaf(const leaf_node& src, branch_number lo, branch_number hi)
   {
      op::leaf_remove_range op{.src = src, .lo = lo, .hi = hi};
      void*                 buf = alloc_leaf_buffer();
      return leaf_ptr(new (buf) leaf_node(leaf_node::max_leaf_size, next_seq(buf), op));
   }

   leaf_ptr split_leaf(const leaf_node& src, branch_number start, branch_number end)
   {
      void* buf = alloc_leaf_buffer();
      return leaf_ptr(new (buf) leaf_node(leaf_node::max_leaf_size, next_seq(buf), &src, key_view(),
                                          start, end));
   }

   leaf_ptr prepend_leaf(const leaf_node& src, key_view prefix)
   {
      op::leaf_prepend_prefix op{.src = src, .prefix = prefix};
      void*                   buf = alloc_leaf_buffer();
      return leaf_ptr(new (buf) leaf_node(leaf_node::max_leaf_size, next_seq(buf), op));
   }

   branch_number require_key(const leaf_node& leaf, key_view key)
   {
      auto bn = leaf.get(key);
      REQUIRE(bn != leaf.num_branches());
      return bn;
   }

   void require_value(const leaf_node& leaf, key_view key, value_type expected)
   {
      auto bn = require_key(leaf, key);
      CHECK(leaf.get_value(bn) == expected);
   }
}  // namespace

TEST_CASE("leaf transform: versioned insert is COW-pure", "[leaf_node][transform][mvcc]")
{
   auto src = make_leaf("base", value_type("seed"));

   auto dst = insert_leaf(*src, "new", value_type("fresh"), 7);

   REQUIRE(src->validate_invariants());
   REQUIRE(dst->validate_invariants());

   CHECK(src->num_branches() == 1);
   CHECK(src->get("new") == branch_number(src->num_branches()));
   CHECK(src->num_versions() == 0);
   require_value(*src, "base", value_type("seed"));

   CHECK(dst->num_branches() == 2);
   require_value(*dst, "base", value_type("seed"));
   require_value(*dst, "new", value_type("fresh"));
   CHECK(dst->get_version(require_key(*dst, "base")) == 0);
   CHECK(dst->get_version(require_key(*dst, "new")) == 7);
}

TEST_CASE("leaf transform: hot rewrite expands to max leaf size",
          "[leaf_node][transform][mvcc]")
{
   auto src = make_leaf("base", value_type("seed"));

   auto dst = insert_leaf(*src, "new", value_type("fresh"), 7);

   REQUIRE(dst->validate_invariants());
   CHECK(dst->size() == leaf_node::max_leaf_size);
   CHECK(dst->free_space() > 0);
}

TEST_CASE("leaf transform: branch creation version survives update promotion",
          "[leaf_node][transform][mvcc]")
{
   auto src      = make_leaf("base", value_type("seed"));
   auto inserted = insert_leaf(*src, "new", value_type("inline-v1"), 11);

   auto promoted = update_leaf(*inserted, "new", value_type::make_value_node(ptr_address(0x4000)));

   REQUIRE(inserted->validate_invariants());
   REQUIRE(promoted->validate_invariants());

   auto inserted_bn = require_key(*inserted, "new");
   CHECK(inserted->get_version(inserted_bn) == 11);
   CHECK(inserted->get_value(inserted_bn) == value_type("inline-v1"));

   auto promoted_bn = require_key(*promoted, "new");
   CHECK(promoted->get_version(promoted_bn) == 11);
   CHECK(promoted->get_value(promoted_bn).is_value_node());
   CHECK(promoted->get_value(promoted_bn).value_address() == ptr_address(0x4000));
}

TEST_CASE("leaf transform: clone and rebuild preserve branch versions",
          "[leaf_node][transform][mvcc]")
{
   auto base = make_leaf("a", value_type("va"));
   auto b    = insert_leaf(*base, "b", value_type("vb"), 5);
   auto c    = insert_leaf(*b, "c", value_type("vc"), 9);

   auto cloned = clone_leaf(*c);

   REQUIRE(cloned->validate_invariants());
   CHECK(cloned->num_branches() == c->num_branches());
   CHECK(cloned->get_version(require_key(*cloned, "a")) == 0);
   CHECK(cloned->get_version(require_key(*cloned, "b")) == 5);
   CHECK(cloned->get_version(require_key(*cloned, "c")) == 9);
   require_value(*cloned, "a", value_type("va"));
   require_value(*cloned, "b", value_type("vb"));
   require_value(*cloned, "c", value_type("vc"));
}

TEST_CASE("leaf transform: remove drops only the removed branch version",
          "[leaf_node][transform][mvcc]")
{
   auto base = make_leaf("a", value_type("va"));
   auto b    = insert_leaf(*base, "b", value_type("vb"), 5);
   auto c    = insert_leaf(*b, "c", value_type("vc"), 9);

   auto removed = remove_leaf(*c, "b");

   REQUIRE(removed->validate_invariants());
   CHECK(removed->num_branches() == 2);
   CHECK(removed->get("b") == branch_number(removed->num_branches()));
   CHECK(removed->get_version(require_key(*removed, "a")) == 0);
   CHECK(removed->get_version(require_key(*removed, "c")) == 9);

   CHECK(c->num_branches() == 3);
   CHECK(c->get_version(require_key(*c, "b")) == 5);
}

TEST_CASE("leaf transform: remove_range and split preserve survivor versions",
          "[leaf_node][transform][mvcc]")
{
   auto base = make_leaf("a", value_type("va"));
   auto b    = insert_leaf(*base, "b", value_type("vb"), 5);
   auto c    = insert_leaf(*b, "c", value_type("vc"), 9);
   auto d    = insert_leaf(*c, "d", value_type("vd"), 13);

   auto ranged = remove_range_leaf(*d, branch_number(1), branch_number(3));
   REQUIRE(ranged->validate_invariants());
   CHECK(ranged->num_branches() == 2);
   CHECK(ranged->get("b") == branch_number(ranged->num_branches()));
   CHECK(ranged->get("c") == branch_number(ranged->num_branches()));
   CHECK(ranged->get_version(require_key(*ranged, "a")) == 0);
   CHECK(ranged->get_version(require_key(*ranged, "d")) == 13);

   auto split = split_leaf(*d, branch_number(1), branch_number(4));
   REQUIRE(split->validate_invariants());
   CHECK(split->num_branches() == 3);
   CHECK(split->get_version(require_key(*split, "b")) == 5);
   CHECK(split->get_version(require_key(*split, "c")) == 9);
   CHECK(split->get_version(require_key(*split, "d")) == 13);
}

TEST_CASE("leaf transform: prepend_prefix preserves branch versions",
          "[leaf_node][transform][mvcc]")
{
   auto base = make_leaf("a", value_type("va"));
   auto b    = insert_leaf(*base, "b", value_type("vb"), 5);
   auto c    = insert_leaf(*b, "c", value_type("vc"), 9);

   auto prefixed = prepend_leaf(*c, "p/");

   REQUIRE(prefixed->validate_invariants());
   CHECK(prefixed->num_branches() == 3);
   CHECK(prefixed->get_version(require_key(*prefixed, "p/a")) == 0);
   CHECK(prefixed->get_version(require_key(*prefixed, "p/b")) == 5);
   CHECK(prefixed->get_version(require_key(*prefixed, "p/c")) == 9);
   require_value(*prefixed, "p/b", value_type("vb"));
}
