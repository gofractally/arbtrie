#include <catch2/catch_test_macros.hpp>
#include <sal/block_allocator.hpp>
#include <sal/debug.hpp>

#include <filesystem>
#include <random>
#include <vector>

namespace fs = std::filesystem;

// Size constants
constexpr uint64_t MB         = 1024 * 1024;
constexpr uint64_t BLOCK_SIZE = 16 * MB;  // 16 MB blocks (must be power of 2)
// Max blocks can be any positive integer, not necessarily a power of 2
constexpr uint32_t MAX_BLOCKS = 5;  // Arbitrary non-power-of-2 value

// Helper function to format size in human-readable format (for debugging output)
std::string format_size(uint64_t size_bytes)
{
   const char* units[]    = {"B", "KB", "MB", "GB", "TB", "PB"};
   int         unit_index = 0;
   double      size       = static_cast<double>(size_bytes);

   while (size >= 1024.0 && unit_index < 5)
   {
      size /= 1024.0;
      unit_index++;
   }

   char buffer[64];
   std::snprintf(buffer, sizeof(buffer), "%.2f %s", size, units[unit_index]);
   return std::string(buffer);
}

TEST_CASE("Power of 2 validation", "[block_allocator]")
{
   SECTION("is_power_of_2 helper function")
   {
      // Powers of 2 should return true
      REQUIRE(sal::block_allocator::is_power_of_2(1));
      REQUIRE(sal::block_allocator::is_power_of_2(2));
      REQUIRE(sal::block_allocator::is_power_of_2(4));
      REQUIRE(sal::block_allocator::is_power_of_2(8));
      REQUIRE(sal::block_allocator::is_power_of_2(16));
      REQUIRE(sal::block_allocator::is_power_of_2(32));
      REQUIRE(sal::block_allocator::is_power_of_2(64));
      REQUIRE(sal::block_allocator::is_power_of_2(128));
      REQUIRE(sal::block_allocator::is_power_of_2(256));
      REQUIRE(sal::block_allocator::is_power_of_2(MB));
      REQUIRE(sal::block_allocator::is_power_of_2(BLOCK_SIZE));

      // Non-powers of 2 should return false
      REQUIRE_FALSE(sal::block_allocator::is_power_of_2(0));
      REQUIRE_FALSE(sal::block_allocator::is_power_of_2(3));
      REQUIRE_FALSE(sal::block_allocator::is_power_of_2(5));
      REQUIRE_FALSE(sal::block_allocator::is_power_of_2(6));
      REQUIRE_FALSE(sal::block_allocator::is_power_of_2(7));
      REQUIRE_FALSE(sal::block_allocator::is_power_of_2(9));
      REQUIRE_FALSE(sal::block_allocator::is_power_of_2(15));
      REQUIRE_FALSE(sal::block_allocator::is_power_of_2(10 * MB));  // 10 MB is not a power of 2
   }

   SECTION("Constructor validation")
   {
      // Create a temporary file path
      fs::path temp_path = fs::temp_directory_path() / "sal_test_block_file.dat";
      fs::remove(temp_path);

      // Valid block size (power of 2) should work with any max_blocks
      REQUIRE_NOTHROW(
          sal::block_allocator(temp_path, BLOCK_SIZE, MAX_BLOCKS));     // Non-power-of-2 max_blocks
      REQUIRE_NOTHROW(sal::block_allocator(temp_path, BLOCK_SIZE, 8));  // Power-of-2 max_blocks
      REQUIRE_NOTHROW(
          sal::block_allocator(temp_path, BLOCK_SIZE, 3));  // Another non-power-of-2 max_blocks

      // Invalid block sizes (not powers of 2) should throw, regardless of max_blocks
      REQUIRE_THROWS_AS(sal::block_allocator(temp_path, 3 * MB, MAX_BLOCKS), std::invalid_argument);
      REQUIRE_THROWS_AS(sal::block_allocator(temp_path, 10 * MB, 8), std::invalid_argument);
      REQUIRE_THROWS_AS(sal::block_allocator(temp_path, 15 * MB, 3), std::invalid_argument);

      // Block size of 0 should throw
      REQUIRE_THROWS(sal::block_allocator(temp_path, 0, MAX_BLOCKS));

      fs::remove(temp_path);
   }

   SECTION("find_max_reservation_size validation")
   {
      // Valid block sizes (powers of 2) should work
      REQUIRE_NOTHROW(sal::block_allocator::find_max_reservation_size(MB));
      REQUIRE_NOTHROW(sal::block_allocator::find_max_reservation_size(2 * MB));
      REQUIRE_NOTHROW(sal::block_allocator::find_max_reservation_size(4 * MB));
      REQUIRE_NOTHROW(sal::block_allocator::find_max_reservation_size(BLOCK_SIZE));

      // Invalid block sizes (not powers of 2) should throw
      REQUIRE_THROWS_AS(sal::block_allocator::find_max_reservation_size(3 * MB),
                        std::invalid_argument);
      REQUIRE_THROWS_AS(sal::block_allocator::find_max_reservation_size(10 * MB),
                        std::invalid_argument);
      REQUIRE_THROWS_AS(sal::block_allocator::find_max_reservation_size(15 * MB),
                        std::invalid_argument);

      // Block size of 0 should return 0 (special case)
      REQUIRE(sal::block_allocator::find_max_reservation_size(0) == 0);
   }
}

TEST_CASE("Find maximum reservation size", "[block_allocator][virtual_memory]")
{
   // Test with a few different block sizes to ensure the method works with various sizes
   SECTION("Find max reservation size with default block size")
   {
      uint64_t max_size = sal::block_allocator::find_max_reservation_size(BLOCK_SIZE);

      // The maximum should be at least 1 GB (very conservative minimum)
      constexpr uint64_t MIN_EXPECTED = 1ULL << 30;  // 1 GB

      REQUIRE(max_size >= MIN_EXPECTED);
      REQUIRE(max_size % BLOCK_SIZE == 0);  // Should be a multiple of block size

      INFO("Max reservation size: " << format_size(max_size) << " (" << (max_size / BLOCK_SIZE)
                                    << " blocks of " << format_size(BLOCK_SIZE) << ")");
   }

   SECTION("Find max reservation size with small block size")
   {
      constexpr uint64_t SMALL_BLOCK = 1 * MB;  // 1 MB blocks (power of 2)
      uint64_t           max_size    = sal::block_allocator::find_max_reservation_size(SMALL_BLOCK);

      REQUIRE(max_size >= SMALL_BLOCK);      // At least one block
      REQUIRE(max_size % SMALL_BLOCK == 0);  // Should be a multiple of block size

      INFO("Max reservation size with 1MB blocks: " << format_size(max_size) << " ("
                                                    << (max_size / SMALL_BLOCK) << " blocks of "
                                                    << format_size(SMALL_BLOCK) << ")");
   }

   SECTION("Find max reservation size with large block size")
   {
      constexpr uint64_t LARGE_BLOCK = 1 * 1024 * MB;  // 1 GB blocks (power of 2)
      uint64_t           max_size    = sal::block_allocator::find_max_reservation_size(LARGE_BLOCK);

      REQUIRE(max_size >= LARGE_BLOCK);      // At least one block
      REQUIRE(max_size % LARGE_BLOCK == 0);  // Should be a multiple of block size

      INFO("Max reservation size with 1GB blocks: " << format_size(max_size) << " ("
                                                    << (max_size / LARGE_BLOCK) << " blocks of "
                                                    << format_size(LARGE_BLOCK) << ")");
   }
}

TEST_CASE("Block allocator basic operations", "[block_allocator]")
{
   // Create a temporary file path
   fs::path temp_path = fs::temp_directory_path() / "sal_test_block_file.dat";

   // Make sure we start clean
   fs::remove(temp_path);

   SECTION("Construction and basic operations")
   {
      // Create a block allocator
      sal::block_allocator allocator(temp_path, BLOCK_SIZE, MAX_BLOCKS);

      // Check initial state
      REQUIRE(allocator.block_size() == BLOCK_SIZE);
      REQUIRE(allocator.num_blocks() == 0);

      // Allocate a block
      auto offset = allocator.alloc();
      REQUIRE(offset == 0);  // First block should be at offset 0
      REQUIRE(allocator.num_blocks() == 1);

      // Get a pointer to the block
      void* block_ptr = allocator.get(offset);
      REQUIRE(block_ptr != nullptr);

      // Test we can write to the block (just the first MB to avoid excess memory usage in test)
      std::memset(block_ptr, 0xFF, MB);

      // Verify the data was written correctly (sample check)
      auto data = static_cast<const unsigned char*>(block_ptr);
      for (size_t i = 0; i < 1024; i++)
      {
         REQUIRE(data[i] == 0xFF);
      }

      // Allocate another block
      auto offset2 = allocator.alloc();
      REQUIRE(offset2 == BLOCK_SIZE);  // Second block should be at offset BLOCK_SIZE
      REQUIRE(allocator.num_blocks() == 2);

      // Get pointer to second block
      void* block_ptr2 = allocator.get(offset2);
      REQUIRE(block_ptr2 != nullptr);

      // Verify the offset arithmetic is correct
      REQUIRE(static_cast<char*>(block_ptr2) - static_cast<char*>(block_ptr) ==
              static_cast<ptrdiff_t>(BLOCK_SIZE));

      // Sync blocks to disk
      allocator.sync(sal::sync_type::async);
   }

   SECTION("Reserve blocks with non-power-of-2 count")
   {
      sal::block_allocator allocator(temp_path, BLOCK_SIZE, MAX_BLOCKS);

      // Reserve several blocks with a non-power-of-2 count
      uint32_t num_reserved = allocator.reserve(3);  // 3 is not a power of 2
      REQUIRE(num_reserved == 3);
      REQUIRE(allocator.num_blocks() == 3);

      // Write different data to each block to test they're distinct
      // Use offset_ptr to get each block
      for (uint32_t i = 0; i < 3; i++)
      {
         sal::block_allocator::offset_ptr offset = allocator.block_to_offset(i);
         auto block_ptr = static_cast<unsigned char*>(allocator.get(offset));
         // Set first byte to a unique value
         block_ptr[0] = static_cast<unsigned char>(0xA0 + i);
      }

      // Verify data is distinct and correctly set
      for (uint32_t i = 0; i < 3; i++)
      {
         sal::block_allocator::offset_ptr offset = allocator.block_to_offset(i);
         auto block_ptr = static_cast<const unsigned char*>(allocator.get(offset));
         REQUIRE(block_ptr[0] == static_cast<unsigned char>(0xA0 + i));
      }

      // Try to reserve beyond max (should throw)
      REQUIRE_THROWS_AS(allocator.reserve(MAX_BLOCKS + 1), std::runtime_error);
   }

   SECTION("Max blocks non-power-of-2 validation")
   {
      // Create allocators with different non-power-of-2 max_blocks values
      sal::block_allocator allocator1(temp_path, BLOCK_SIZE, 3);
      REQUIRE(allocator1.alloc() == 0);               // First block at 0
      REQUIRE(allocator1.alloc() == BLOCK_SIZE);      // Second block
      REQUIRE(allocator1.alloc() == 2 * BLOCK_SIZE);  // Third block
      // Should throw when trying to allocate beyond max
      REQUIRE_THROWS_AS(allocator1.alloc(), std::runtime_error);

      // Clean up
      fs::remove(temp_path);

      // Test with another non-power-of-2 value
      sal::block_allocator allocator2(temp_path, BLOCK_SIZE, 5);
      for (int i = 0; i < 5; i++)
      {
         REQUIRE(allocator2.alloc() == i * BLOCK_SIZE);
      }
      // Should throw when trying to allocate beyond max
      REQUIRE_THROWS_AS(allocator2.alloc(), std::runtime_error);
   }

   SECTION("Offset-block conversion methods")
   {
      sal::block_allocator allocator(temp_path, BLOCK_SIZE, MAX_BLOCKS);

      // Allocate a few blocks
      auto offset0 = allocator.alloc();
      auto offset1 = allocator.alloc();
      auto offset2 = allocator.alloc();
      auto offset3 = allocator.alloc();  // Add one more block

      // Test offset_to_block
      REQUIRE(allocator.offset_to_block(offset0) == 0);
      REQUIRE(allocator.offset_to_block(offset1) == 1);
      REQUIRE(allocator.offset_to_block(offset2) == 2);
      REQUIRE(allocator.offset_to_block(offset3) == 3);  // Test the new block

      // Test block_to_offset - these should use bit shift operations internally
      REQUIRE(allocator.block_to_offset(0) == offset0);
      REQUIRE(allocator.block_to_offset(1) == offset1);
      REQUIRE(allocator.block_to_offset(2) == offset2);
      REQUIRE(allocator.block_to_offset(3) == offset3);  // Test the new block

      // Verify that block_to_offset is using the bit shift optimization
      // For a 16MB block size, _log2_block_size should be 24 (2^24 = 16*2^20)
      REQUIRE(allocator.block_to_offset(1) == (1 << 24));
      REQUIRE(allocator.block_to_offset(2) == (2 << 24));
      REQUIRE(allocator.block_to_offset(3) == (3 << 24));

      // Test block alignment checks
      REQUIRE(allocator.is_block_aligned(0));
      REQUIRE(allocator.is_block_aligned(BLOCK_SIZE));
      REQUIRE(allocator.is_block_aligned(2 * BLOCK_SIZE));
      REQUIRE_FALSE(allocator.is_block_aligned(1));
      REQUIRE_FALSE(allocator.is_block_aligned(BLOCK_SIZE - 1));
      REQUIRE_FALSE(allocator.is_block_aligned(BLOCK_SIZE + 1));
      REQUIRE_FALSE(allocator.is_block_aligned(BLOCK_SIZE / 2));

      // Test round-trip conversions
      REQUIRE(allocator.block_to_offset(allocator.offset_to_block(offset0)) == offset0);
      REQUIRE(allocator.block_to_offset(allocator.offset_to_block(offset1)) == offset1);
      REQUIRE(allocator.block_to_offset(allocator.offset_to_block(offset2)) == offset2);
      REQUIRE(allocator.block_to_offset(allocator.offset_to_block(offset3)) ==
              offset3);  // Test the new block

      REQUIRE(allocator.offset_to_block(allocator.block_to_offset(0)) == 0);
      REQUIRE(allocator.offset_to_block(allocator.block_to_offset(1)) == 1);
      REQUIRE(allocator.offset_to_block(allocator.block_to_offset(2)) == 2);
      REQUIRE(allocator.offset_to_block(allocator.block_to_offset(3)) == 3);  // Test the new block
   }

   // Clean up
   fs::remove(temp_path);
}