#include <catch2/catch_test_macros.hpp>
#include <sal/debug.hpp>
#include <sal/mapping.hpp>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

// Size constants
constexpr size_t MB          = 1024 * 1024;
constexpr size_t LARGE_SIZE  = 16 * MB;  // 16 MB
constexpr size_t MEDIUM_SIZE = 4 * MB;   // 4 MB for initial content

TEST_CASE("Memory mapping basic operations", "[mapping]")
{
   // Create a temporary file path
   fs::path temp_path = fs::temp_directory_path() / "sal_test_mapping.dat";

   // Make sure we start clean
   fs::remove(temp_path);

   SECTION("Empty file mapping")
   {
      // Create an empty file
      std::ofstream file(temp_path);
      file.close();

      // Map the empty file
      sal::mapping map(temp_path, sal::access_mode::read_write);

      // Check initial state
      REQUIRE(map.size() == 0);
      REQUIRE(map.data() == nullptr);

      // Resize the mapping to 16 MB
      auto old_data = map.resize(LARGE_SIZE);

      // Old data should be null since file was empty
      REQUIRE(old_data == nullptr);

      // New size and data should be valid
      REQUIRE(map.size() == LARGE_SIZE);
      REQUIRE(map.data() != nullptr);

      // Check we can write to it (just first 1 MB to save memory)
      auto data_ptr = static_cast<char*>(map.data());
      std::memset(data_ptr, 0xFF, MB);

      // Verify data
      for (size_t i = 0; i < 1024; i++)
      {
         REQUIRE(data_ptr[i] == static_cast<char>(0xFF));
      }
   }

   SECTION("Non-empty file mapping and resize")
   {
      // Create a file with some content (4 MB)
      std::vector<char> init_data(MEDIUM_SIZE, 0xAA);

      {
         std::ofstream file(temp_path, std::ios::binary);
         file.write(init_data.data(), init_data.size());
      }

      // Map the file
      sal::mapping map(temp_path, sal::access_mode::read_write);

      // Check initial state
      REQUIRE(map.size() == MEDIUM_SIZE);
      REQUIRE(map.data() != nullptr);

      // Check content is what we wrote (sampling a few points)
      auto data_ptr = static_cast<const char*>(map.data());
      for (size_t i = 0; i < 1024; i++)
      {
         REQUIRE(data_ptr[i] == static_cast<char>(0xAA));
      }
      REQUIRE(data_ptr[MEDIUM_SIZE - 1] == static_cast<char>(0xAA));

      // Resize the mapping to 16 MB
      auto old_data = map.resize(LARGE_SIZE);

      // Old data should not be null
      REQUIRE(old_data != nullptr);

      // New size and data should be valid
      REQUIRE(map.size() == LARGE_SIZE);
      REQUIRE(map.data() != nullptr);

      // Verify content is preserved in first part (sampling)
      data_ptr = static_cast<const char*>(map.data());
      for (size_t i = 0; i < 1024; i++)
      {
         REQUIRE(data_ptr[i] == static_cast<char>(0xAA));
      }

      // Check a few points throughout the file
      REQUIRE(data_ptr[MEDIUM_SIZE / 2] == static_cast<char>(0xAA));
      REQUIRE(data_ptr[MEDIUM_SIZE - 1] == static_cast<char>(0xAA));

      // Write to the newly extended part
      auto write_ptr = static_cast<char*>(map.data()) + MEDIUM_SIZE;
      std::memset(write_ptr, 0xBB, 1024);

      // Verify this section was written correctly
      for (size_t i = 0; i < 1024; i++)
      {
         REQUIRE(write_ptr[i] == static_cast<char>(0xBB));
      }
   }

   // Clean up
   fs::remove(temp_path);
}