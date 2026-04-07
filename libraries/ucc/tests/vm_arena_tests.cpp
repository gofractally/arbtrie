#include <catch2/catch_test_macros.hpp>
#include <ucc/vm_arena.hpp>

#include <cstring>
#include <thread>
#include <vector>

TEST_CASE("vm_arena basic allocation", "[vm_arena]")
{
   ucc::vm_arena arena;
   REQUIRE(arena.bytes_used() == 0);
   REQUIRE(arena.base() != nullptr);

   auto off = arena.allocate(100);
   REQUIRE(off == 0);
   REQUIRE(arena.bytes_used() > 0);

   // Write and read back
   auto* ptr = arena.as<char>(off);
   std::memset(ptr, 0xAB, 100);
   REQUIRE(static_cast<unsigned char>(ptr[0]) == 0xAB);
   REQUIRE(static_cast<unsigned char>(ptr[99]) == 0xAB);
}

TEST_CASE("vm_arena alignment", "[vm_arena]")
{
   ucc::vm_arena arena;

   auto off1 = arena.allocate(1);
   REQUIRE(off1 == 0);
   REQUIRE(arena.bytes_used() == ucc::vm_arena::alignment);

   auto off2 = arena.allocate(1);
   REQUIRE(off2 == ucc::vm_arena::alignment);
   REQUIRE(off2 % ucc::vm_arena::alignment == 0);
}

TEST_CASE("vm_arena base pointer stability", "[vm_arena]")
{
   ucc::vm_arena arena;
   const char*   base = arena.base();

   // Allocate across multiple commit chunks
   for (int i = 0; i < 100; ++i)
      arena.allocate(64 * 1024);  // 64 KB each = 6.4 MB total

   REQUIRE(arena.base() == base);
   REQUIRE(arena.bytes_used() >= 100 * 64 * 1024);
}

TEST_CASE("vm_arena clear and reuse", "[vm_arena]")
{
   ucc::vm_arena arena;
   const char*   base = arena.base();

   auto off = arena.allocate(1024);
   auto* ptr = arena.as<int>(off);
   *ptr = 42;

   REQUIRE(arena.bytes_used() > 0);
   REQUIRE(arena.committed() > 0);

   arena.clear();
   REQUIRE(arena.bytes_used() == 0);
   REQUIRE(arena.committed() == 0);
   REQUIRE(arena.base() == base);  // base pointer unchanged

   // Can allocate again after clear
   auto off2 = arena.allocate(1024);
   REQUIRE(off2 == 0);  // cursor reset
   auto* ptr2 = arena.as<int>(off2);
   *ptr2 = 99;
   REQUIRE(*ptr2 == 99);
}

TEST_CASE("vm_arena large allocation", "[vm_arena]")
{
   ucc::vm_arena arena;

   // Allocate 16 MB in one shot
   auto off = arena.allocate(16u << 20);
   REQUIRE(off == 0);

   auto* ptr = arena.as<char>(off);
   // Write first and last byte to confirm pages are committed
   ptr[0] = 'A';
   ptr[(16u << 20) - 1] = 'Z';
   REQUIRE(ptr[0] == 'A');
   REQUIRE(ptr[(16u << 20) - 1] == 'Z');
}

TEST_CASE("vm_arena many small allocations", "[vm_arena]")
{
   ucc::vm_arena arena;

   // Allocate 10,000 small objects
   std::vector<uint32_t> offsets;
   for (uint32_t i = 0; i < 10000; ++i)
   {
      auto off = arena.allocate(sizeof(uint32_t));
      *arena.as<uint32_t>(off) = i;
      offsets.push_back(off);
   }

   // Verify all values
   for (uint32_t i = 0; i < 10000; ++i)
      REQUIRE(*arena.as<uint32_t>(offsets[i]) == i);
}

TEST_CASE("vm_arena move semantics", "[vm_arena]")
{
   ucc::vm_arena arena1;
   auto off = arena1.allocate(64);
   *arena1.as<uint64_t>(off) = 0xDEADBEEF;
   const char* base = arena1.base();

   // Move construct
   ucc::vm_arena arena2(std::move(arena1));
   REQUIRE(arena2.base() == base);
   REQUIRE(*arena2.as<uint64_t>(off) == 0xDEADBEEF);
   REQUIRE(arena1.base() == nullptr);
   REQUIRE(arena1.bytes_used() == 0);

   // Move assign
   ucc::vm_arena arena3;
   arena3 = std::move(arena2);
   REQUIRE(arena3.base() == base);
   REQUIRE(*arena3.as<uint64_t>(off) == 0xDEADBEEF);
   REQUIRE(arena2.base() == nullptr);
}

TEST_CASE("vm_arena concurrent read safety", "[vm_arena]")
{
   ucc::vm_arena arena;

   // Pre-allocate some data
   constexpr uint32_t N = 1000;
   std::vector<uint32_t> offsets;
   for (uint32_t i = 0; i < N; ++i)
   {
      auto off = arena.allocate(sizeof(uint64_t));
      *arena.as<uint64_t>(off) = i * 100;
      offsets.push_back(off);
   }

   // Read from another thread while writer allocates more
   std::atomic<bool> done{false};
   std::thread reader([&]() {
      while (!done.load(std::memory_order_relaxed))
      {
         for (uint32_t i = 0; i < N; ++i)
         {
            uint64_t val = *arena.as<uint64_t>(offsets[i]);
            // Values should never change — writer only appends
            assert(val == i * 100);
         }
      }
   });

   // Writer allocates more data (doesn't touch old data)
   for (uint32_t i = 0; i < 5000; ++i)
   {
      auto off = arena.allocate(128);
      std::memset(arena.as<char>(off), 0, 128);
   }

   done.store(true, std::memory_order_relaxed);
   reader.join();
}
