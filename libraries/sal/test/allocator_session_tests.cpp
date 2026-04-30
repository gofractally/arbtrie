#include <catch2/catch_test_macros.hpp>
#include <filesystem>

#include <sal/allocator_session_impl.hpp>
#include <sal/mapping.hpp>
#include <sal/mapped_memory/allocator_state.hpp>

namespace fs = std::filesystem;

namespace
{
   fs::path temp_dir(std::string name)
   {
      auto path = fs::temp_directory_path() / name;
      fs::remove_all(path);
      fs::create_directories(path);
      return path;
   }
}  // namespace

TEST_CASE("allocator open resets stale session bitmap", "[sal][session][recovery]")
{
   auto path = temp_dir("sal_session_bitmap_recovery_test");

   {
      sal::allocator alloc(path, sal::runtime_config{}, false);
      auto           session = alloc.get_session();
      REQUIRE(session.get() != nullptr);
   }

   {
      sal::mapping header(path / "header", sal::access_mode::read_write);
      auto* state = reinterpret_cast<sal::mapped_memory::allocator_state*>(header.data());
      for (uint32_t i = 0; i < sal::mapped_memory::session_data::session_cap; ++i)
         (void)state->_session_data.alloc_session_num();
      REQUIRE(state->_session_data.free_session_bitmap() == 0);
      header.sync(sal::sync_type::full);
   }

   {
      sal::allocator alloc(path, sal::runtime_config{}, false);
      CHECK(alloc.dump().active_sessions == 0);
      auto session = alloc.get_session();
      REQUIRE(session.get() != nullptr);
      CHECK(alloc.dump().active_sessions == 1);
   }

   fs::remove_all(path);
}
