#include <catch2/catch_test_macros.hpp>
#include <sal/block_allocator.hpp>
#include <sal/debug.hpp>

#include <chrono>
#include <filesystem>
#include <random>
#include <vector>

namespace fs = std::filesystem;

// Size constants
constexpr uint64_t MB         = 1024 * 1024;
constexpr uint64_t BLOCK_SIZE = 16 * MB;  // 16 MB blocks (must be multiple of os_page_size)
// Max blocks can be any positive integer
constexpr uint32_t MAX_BLOCKS = 5;  // Arbitrary value

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

TEST_CASE("Block size validation", "[block_allocator]")
{
   SECTION("Constructor validation")
   {
      // Create a temporary file path
      fs::path temp_path = fs::temp_directory_path() / "sal_test_block_file.dat";
      fs::remove(temp_path);

      // Valid block sizes (multiples of os_page_size) should work with any max_blocks
      REQUIRE_NOTHROW(sal::block_allocator(temp_path, BLOCK_SIZE, MAX_BLOCKS));  // Large block size
      REQUIRE_NOTHROW(sal::block_allocator(temp_path, 4096, 8));  // Minimum block size (1 page)
      REQUIRE_NOTHROW(sal::block_allocator(temp_path, 8192, 3));  // 2 pages

      // Invalid block sizes (not multiples of os_page_size) should throw
      REQUIRE_THROWS_AS(sal::block_allocator(temp_path, 4095, MAX_BLOCKS), std::invalid_argument);
      REQUIRE_THROWS_AS(sal::block_allocator(temp_path, 4097, 8), std::invalid_argument);
      REQUIRE_THROWS_AS(sal::block_allocator(temp_path, 8191, 3), std::invalid_argument);

      // Block size of 0 should throw
      REQUIRE_THROWS(sal::block_allocator(temp_path, 0, MAX_BLOCKS));

      fs::remove(temp_path);
   }

   SECTION("find_max_reservation_size validation")
   {
      // Valid block sizes (multiples of os_page_size) should work
      REQUIRE_NOTHROW(sal::block_allocator::find_max_reservation_size(4096));        // 1 page
      REQUIRE_NOTHROW(sal::block_allocator::find_max_reservation_size(8192));        // 2 pages
      REQUIRE_NOTHROW(sal::block_allocator::find_max_reservation_size(16384));       // 4 pages
      REQUIRE_NOTHROW(sal::block_allocator::find_max_reservation_size(BLOCK_SIZE));  // Many pages

      // Invalid block sizes (not multiples of os_page_size) should throw
      REQUIRE_THROWS_AS(sal::block_allocator::find_max_reservation_size(4095),
                        std::invalid_argument);
      REQUIRE_THROWS_AS(sal::block_allocator::find_max_reservation_size(4097),
                        std::invalid_argument);
      REQUIRE_THROWS_AS(sal::block_allocator::find_max_reservation_size(8191),
                        std::invalid_argument);
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
      auto [block_num1, offset1] = allocator.alloc();
      REQUIRE(*offset1 == 0);     // First block should be at offset 0
      REQUIRE(*block_num1 == 0);  // First block should be index 0
      REQUIRE(allocator.num_blocks() == 1);

      // Get a pointer to the block
      void* block_ptr = allocator.get(offset1);
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
      auto [block_num2, offset2] = allocator.alloc();
      REQUIRE(*offset2 == BLOCK_SIZE);  // Second block should be at offset BLOCK_SIZE
      REQUIRE(*block_num2 == 1);        // Second block should be index 1
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

      // With the updated behavior, reserve() only pre-maps the space but doesn't increment num_blocks
      // So we need to allocate the blocks explicitly
      REQUIRE(allocator.num_blocks() == 0);

      // Allocate blocks and verify they use pre-reserved space
      std::vector<std::pair<sal::block_allocator::block_number, sal::block_allocator::offset_ptr>>
          blocks;
      for (uint32_t i = 0; i < 3; i++)
      {
         blocks.push_back(allocator.alloc());
         REQUIRE(*blocks.back().first == i);                // Block number should match index
         REQUIRE(*blocks.back().second == i * BLOCK_SIZE);  // Offset should be index * block_size
      }
      REQUIRE(allocator.num_blocks() == 3);

      // Write different data to each block to test they're distinct
      for (uint32_t i = 0; i < 3; i++)
      {
         auto block_ptr = reinterpret_cast<unsigned char*>(allocator.get(blocks[i].second));
         // Set first byte to a unique value
         block_ptr[0] = static_cast<unsigned char>(0xA0 + i);
      }

      // Verify data is distinct and correctly set
      for (uint32_t i = 0; i < 3; i++)
      {
         auto block_ptr = reinterpret_cast<const unsigned char*>(allocator.get(blocks[i].second));
         REQUIRE(block_ptr[0] == static_cast<unsigned char>(0xA0 + i));
      }

      // Try to reserve beyond max (should throw)
      REQUIRE_THROWS_AS(allocator.reserve(MAX_BLOCKS + 1), std::runtime_error);
   }

   SECTION("Max blocks non-power-of-2 validation")
   {
      // Create allocators with different non-power-of-2 max_blocks values
      sal::block_allocator allocator1(temp_path, BLOCK_SIZE, 3);
      auto [block1_0, offset1_0] = allocator1.alloc();
      REQUIRE(*offset1_0 == 0);  // First block at 0
      auto [block1_1, offset1_1] = allocator1.alloc();
      REQUIRE(*offset1_1 == BLOCK_SIZE);  // Second block
      auto [block1_2, offset1_2] = allocator1.alloc();
      REQUIRE(*offset1_2 == 2 * BLOCK_SIZE);  // Third block
      // Should throw when trying to allocate beyond max
      REQUIRE_THROWS_AS(allocator1.alloc(), std::runtime_error);

      // Clean up
      fs::remove(temp_path);

      // Test with another non-power-of-2 value
      sal::block_allocator allocator2(temp_path, BLOCK_SIZE, 5);
      for (int i = 0; i < 5; i++)
      {
         auto [block_num, offset] = allocator2.alloc();
         REQUIRE(*offset == i * BLOCK_SIZE);
         REQUIRE(*block_num == i);
      }
      // Should throw when trying to allocate beyond max
      REQUIRE_THROWS_AS(allocator2.alloc(), std::runtime_error);
   }

   SECTION("Pre-reserving blocks for efficient allocation")
   {
      sal::block_allocator allocator(temp_path, BLOCK_SIZE, MAX_BLOCKS);

      // Reserve space for 4 blocks
      allocator.reserve(4);

      // Verify num_blocks is still 0 (reserve only maps space without incrementing _num_blocks)
      REQUIRE(allocator.num_blocks() == 0);

      // Measure time to allocate pre-reserved blocks (should be fast path)
      auto start_time = std::chrono::high_resolution_clock::now();

      // Allocate blocks using the fast path (pre-reserved space)
      std::vector<std::pair<sal::block_allocator::block_number, sal::block_allocator::offset_ptr>>
          blocks;
      for (int i = 0; i < 4; i++)
      {
         blocks.push_back(allocator.alloc());

         // Verify block number and offset are correct
         REQUIRE(*blocks[i].first == i);
         REQUIRE(*blocks[i].second == i * BLOCK_SIZE);
      }

      auto end_time = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

      // Output timing for information (not a strict test requirement)
      INFO("Time to allocate 4 pre-reserved blocks: " << duration.count() << " microseconds");

      // Verify we have the expected number of blocks
      REQUIRE(allocator.num_blocks() == 4);

      // Allocate one more block that requires a slow path (not pre-reserved)
      start_time               = std::chrono::high_resolution_clock::now();
      auto [block_num, offset] = allocator.alloc();
      end_time                 = std::chrono::high_resolution_clock::now();
      duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

      INFO("Time to allocate 1 non-reserved block: " << duration.count() << " microseconds");

      // Verify the new allocation is correct
      REQUIRE(*block_num == 4);
      REQUIRE(*offset == 4 * BLOCK_SIZE);
      REQUIRE(allocator.num_blocks() == 5);
   }

   SECTION("Offset-block conversion methods")
   {
      sal::block_allocator allocator(temp_path, BLOCK_SIZE, MAX_BLOCKS);

      // Allocate a few blocks
      auto [block_num0, offset0] = allocator.alloc();
      auto [block_num1, offset1] = allocator.alloc();
      auto [block_num2, offset2] = allocator.alloc();
      auto [block_num3, offset3] = allocator.alloc();  // Add one more block

      // Test offset_to_block
      REQUIRE(allocator.offset_to_block(offset0) == sal::block_allocator::block_number(0));
      REQUIRE(allocator.offset_to_block(offset1) == sal::block_allocator::block_number(1));
      REQUIRE(allocator.offset_to_block(offset2) == sal::block_allocator::block_number(2));
      REQUIRE(allocator.offset_to_block(offset3) ==
              sal::block_allocator::block_number(3));  // Test the new block

      // Test block_to_offset - these should use bit shift operations internally
      REQUIRE(allocator.block_to_offset(sal::block_allocator::block_number(0)) == offset0);
      REQUIRE(allocator.block_to_offset(sal::block_allocator::block_number(1)) == offset1);
      REQUIRE(allocator.block_to_offset(sal::block_allocator::block_number(2)) == offset2);
      REQUIRE(allocator.block_to_offset(sal::block_allocator::block_number(3)) ==
              offset3);  // Test the new block

      // Verify that block_to_offset is using the bit shift optimization
      // For a 16MB block size, _log2_block_size should be 24 (2^24 = 16*2^20)
      REQUIRE(allocator.block_to_offset(sal::block_allocator::block_number(1)) == (1 << 24));
      REQUIRE(allocator.block_to_offset(sal::block_allocator::block_number(2)) == (2 << 24));
      REQUIRE(allocator.block_to_offset(sal::block_allocator::block_number(3)) == (3 << 24));

      // Test block alignment checks
      REQUIRE(allocator.is_block_aligned(sal::block_allocator::offset_ptr{0}));
      REQUIRE(allocator.is_block_aligned(sal::block_allocator::offset_ptr{BLOCK_SIZE}));
      REQUIRE(allocator.is_block_aligned(sal::block_allocator::offset_ptr{2 * BLOCK_SIZE}));
      REQUIRE_FALSE(allocator.is_block_aligned(sal::block_allocator::offset_ptr{1}));
      REQUIRE_FALSE(allocator.is_block_aligned(sal::block_allocator::offset_ptr{BLOCK_SIZE - 1}));
      REQUIRE_FALSE(allocator.is_block_aligned(sal::block_allocator::offset_ptr{BLOCK_SIZE + 1}));
      REQUIRE_FALSE(allocator.is_block_aligned(sal::block_allocator::offset_ptr{BLOCK_SIZE / 2}));

      // Test round-trip conversions
      REQUIRE(allocator.block_to_offset(allocator.offset_to_block(offset0)) == offset0);
      REQUIRE(allocator.block_to_offset(allocator.offset_to_block(offset1)) == offset1);
      REQUIRE(allocator.block_to_offset(allocator.offset_to_block(offset2)) == offset2);
      REQUIRE(allocator.block_to_offset(allocator.offset_to_block(offset3)) ==
              offset3);  // Test the new block

      REQUIRE(allocator.offset_to_block(allocator.block_to_offset(
                  sal::block_allocator::block_number(0))) == sal::block_allocator::block_number(0));
      REQUIRE(allocator.offset_to_block(allocator.block_to_offset(
                  sal::block_allocator::block_number(1))) == sal::block_allocator::block_number(1));
      REQUIRE(allocator.offset_to_block(allocator.block_to_offset(
                  sal::block_allocator::block_number(2))) == sal::block_allocator::block_number(2));
      REQUIRE(allocator.offset_to_block(allocator.block_to_offset(
                  sal::block_allocator::block_number(3))) == sal::block_allocator::block_number(3));
   }

   // Clean up
   fs::remove(temp_path);
}

TEST_CASE("Block allocator truncate operations", "[block_allocator]")
{
   // Create a temporary file path
   fs::path temp_path = fs::temp_directory_path() / "sal_test_block_file_truncate.dat";

   // Make sure we start clean
   fs::remove(temp_path);

   SECTION("Truncate to smaller size")
   {
      sal::block_allocator allocator(temp_path, BLOCK_SIZE, MAX_BLOCKS);

      // First reserve some blocks and allocate them
      allocator.reserve(4);

      // Allocate 4 blocks
      std::vector<std::pair<sal::block_allocator::block_number, sal::block_allocator::offset_ptr>>
          blocks;
      for (int i = 0; i < 4; i++)
      {
         blocks.push_back(allocator.alloc());

         // Write some identifiable data to each block
         auto* data = reinterpret_cast<unsigned char*>(allocator.get(blocks[i].second));
         data[0]    = static_cast<unsigned char>(0xA0 + i);
      }

      REQUIRE(allocator.num_blocks() == 4);

      // Now truncate to 2 blocks
      allocator.truncate(2);

      // Verify the size was reduced
      REQUIRE(allocator.num_blocks() == 2);

      // Verify the first two blocks still have their data
      for (int i = 0; i < 2; i++)
      {
         auto* data = reinterpret_cast<const unsigned char*>(allocator.get(blocks[i].second));
         REQUIRE(data[0] == static_cast<unsigned char>(0xA0 + i));
      }

      // Allocating should now start from block 2
      auto [new_block, new_offset] = allocator.alloc();
      REQUIRE(*new_block == 2);
      REQUIRE(*new_offset == 2 * BLOCK_SIZE);
      REQUIRE(allocator.num_blocks() == 3);
   }

   SECTION("Truncate to same size")
   {
      sal::block_allocator allocator(temp_path, BLOCK_SIZE, MAX_BLOCKS);

      // Allocate 2 blocks
      allocator.alloc();
      allocator.alloc();
      REQUIRE(allocator.num_blocks() == 2);

      // Truncate to same size
      allocator.truncate(2);

      // Size should remain the same
      REQUIRE(allocator.num_blocks() == 2);

      // Allocating should now create block 2
      auto [new_block, new_offset] = allocator.alloc();
      REQUIRE(*new_block == 2);
   }

   SECTION("Truncate to larger size (should call reserve)")
   {
      sal::block_allocator allocator(temp_path, BLOCK_SIZE, MAX_BLOCKS);

      // Allocate 1 block
      allocator.alloc();
      REQUIRE(allocator.num_blocks() == 1);

      // Truncate to larger size
      allocator.truncate(3);

      // Size should be updated, but blocks aren't allocated until used
      REQUIRE(allocator.num_blocks() == 1);

      // We should now be able to allocate up to block 2 without resizing
      auto [block1, offset1] = allocator.alloc();
      REQUIRE(*block1 == 1);

      auto [block2, offset2] = allocator.alloc();
      REQUIRE(*block2 == 2);

      REQUIRE(allocator.num_blocks() == 3);

      // Should be able to allocate beyond original truncate size
      auto [block3, offset3] = allocator.alloc();
      REQUIRE(*block3 == 3);
      REQUIRE(allocator.num_blocks() == 4);
   }

   SECTION("Truncate beyond max blocks (should throw)")
   {
      sal::block_allocator allocator(temp_path, BLOCK_SIZE, MAX_BLOCKS);

      // Truncate beyond max_blocks should throw
      REQUIRE_THROWS_AS(allocator.truncate(MAX_BLOCKS + 1), std::runtime_error);
   }

   // Clean up
   fs::remove(temp_path);
}