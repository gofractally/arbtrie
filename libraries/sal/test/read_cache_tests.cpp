#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>
#include <string>

#define private public
#include <sal/allocator_session_impl.hpp>
#include <sal/mapped_memory/cache_difficulty_state.hpp>
#undef private

namespace fs = std::filesystem;

namespace
{
   struct cache_test_node : sal::alloc_header
   {
      static constexpr sal::header_type type_id = sal::header_type::start_user_type;

      cache_test_node(uint32_t asize, sal::ptr_address_seq seq) noexcept
          : sal::alloc_header(asize, type_id, seq)
      {
      }
   };

   fs::path temp_dir(std::string name)
   {
      auto path = fs::temp_directory_path() / name;
      fs::remove_all(path);
      fs::create_directories(path);
      return path;
   }
}  // namespace

TEST_CASE("cache difficulty stays in 64-bit space", "[sal][read_cache]")
{
   sal::mapped_memory::cache_difficulty_state state;

   state.compactor_promote_bytes(0, std::chrono::system_clock::now() + std::chrono::seconds(10));

   CHECK(state.get_cache_difficulty() > uint64_t{1} << 63);
}

TEST_CASE("cache policy follows runtime config", "[sal][read_cache]")
{
   sal::mapped_memory::cache_difficulty_state state;
   state.configure_cache(8ull * 1024 * 1024 * 1024, 18000);

   CHECK(state._total_cache_size == 8ull * 1024 * 1024 * 1024);
   CHECK(state._cache_frequency_window == std::chrono::milliseconds(18000 * 1000));
   CHECK(state.target_promoted_bytes_per_sec() ==
         (8ull * 1024 * 1024 * 1024) / 18000);
}

TEST_CASE("cache difficulty relaxes quickly when policy undershoots", "[sal][read_cache]")
{
   sal::mapped_memory::cache_difficulty_state state;
   state.configure_cache(8ull * 1024 * 1024 * 1024, 600);

   const uint64_t max_uint64 = ~uint64_t{0};
   const uint64_t initial_gap = max_uint64 / 1024;
   const auto     start = std::chrono::system_clock::now();
   state._last_update = start;
   state._cache_difficulty.store(max_uint64 - initial_gap, std::memory_order_relaxed);

   state.compactor_policy_satisfied_bytes(0, start + std::chrono::seconds(5));
   auto first_gap = max_uint64 - state.get_cache_difficulty();
   CHECK(first_gap == initial_gap * 8);

   state.compactor_policy_satisfied_bytes(0, start + std::chrono::seconds(10));
   auto second_gap = max_uint64 - state.get_cache_difficulty();
   CHECK(second_gap == initial_gap * 64);
}

TEST_CASE("cache difficulty tightens when policy overshoots", "[sal][read_cache]")
{
   sal::mapped_memory::cache_difficulty_state state;
   state.configure_cache(8ull * 1024 * 1024 * 1024, 600);

   const uint64_t max_uint64 = ~uint64_t{0};
   const uint64_t initial_gap = max_uint64 / 64;
   const auto     start = std::chrono::system_clock::now();
   state._last_update = start;
   state._cache_difficulty.store(max_uint64 - initial_gap, std::memory_order_relaxed);

   const uint64_t five_second_target = state.target_promoted_bytes_per_sec() * 5;
   state.compactor_policy_satisfied_bytes(five_second_target * 16,
                                          start + std::chrono::seconds(5));

   auto tightened_gap = max_uint64 - state.get_cache_difficulty();
   CHECK(tightened_gap == initial_gap / 4);
}

TEST_CASE("cache policy cost-normalizes larger objects", "[sal][read_cache]")
{
   sal::mapped_memory::cache_difficulty_state state;
   const uint64_t max_uint64 = ~uint64_t{0};
   const uint64_t gap        = max_uint64 / 8;
   state._cache_difficulty.store(max_uint64 - gap, std::memory_order_relaxed);

   const uint64_t between_one_and_two_cacheline_thresholds = max_uint64 - ((gap * 3) / 4);

   CHECK(state.should_cache(between_one_and_two_cacheline_thresholds, 64));
   CHECK_FALSE(state.should_cache(between_one_and_two_cacheline_thresholds, 128));
}

TEST_CASE("cache promoter tracks source residency", "[sal][read_cache]")
{
   sal::mapped_memory::cache_difficulty_state state;

   state.compactor_promote_bytes(64, false, true);
   state.compactor_promote_bytes(128, true, true, 750000);
   state.compactor_skip_young_hot_bytes(512, 125000);
   state.compactor_promote_bytes(256, false, false);

   CHECK(state.total_cache_policy_satisfied_bytes.load(std::memory_order_relaxed) == 960);
   CHECK(state.total_promoted_bytes.load(std::memory_order_relaxed) == 448);
   CHECK(state.total_cold_to_hot_promotions.load(std::memory_order_relaxed) == 1);
   CHECK(state.total_cold_to_hot_promoted_bytes.load(std::memory_order_relaxed) == 64);
   CHECK(state.total_hot_to_hot_promotions.load(std::memory_order_relaxed) == 1);
   CHECK(state.total_hot_to_hot_promoted_bytes.load(std::memory_order_relaxed) == 128);
   CHECK(state.total_hot_to_hot_demote_pressure_ppm.load(std::memory_order_relaxed) ==
         750000);
   CHECK(state.total_hot_to_hot_byte_demote_pressure_ppm.load(std::memory_order_relaxed) ==
         96000000);
   CHECK(state.total_young_hot_skips.load(std::memory_order_relaxed) == 1);
   CHECK(state.total_young_hot_skipped_bytes.load(std::memory_order_relaxed) == 512);
   CHECK(state.total_young_hot_skip_demote_pressure_ppm.load(std::memory_order_relaxed) ==
         125000);
   CHECK(state.total_young_hot_skip_byte_demote_pressure_ppm.load(std::memory_order_relaxed) ==
         64000000);
   CHECK(state.total_promoted_to_cold_promotions.load(std::memory_order_relaxed) == 1);
   CHECK(state.total_promoted_to_cold_bytes.load(std::memory_order_relaxed) == 256);
}

TEST_CASE("read-only sampled reads enqueue MFU cache promotion", "[sal][read_cache]")
{
   auto path = temp_dir("sal_read_cache_enqueue_test");

   sal::runtime_config cfg;
   cfg.enable_read_cache        = true;
   cfg.max_pinned_cache_size_mb = 32;
   cfg.sync_mode                = sal::sync_type::mprotect;

   {
      sal::allocator alloc(path, cfg, false);
      auto           ses = alloc.get_session();
      auto           adr = ses->alloc<cache_test_node>(uint32_t{64}, sal::alloc_hint{});

      ses->sync(sal::sync_type::mprotect, cfg);
      alloc._mapped_state->_cache_difficulty_state._cache_difficulty.store(
          0, std::memory_order_relaxed);

      auto& queue = alloc._mapped_state->_session_data.rcache_queue(ses->get_session_num());

      {
         auto lock = ses->lock();
         auto ref  = ses->get_ref<cache_test_node>(adr);
         (void)ref;
      }
      CHECK(queue.usage() == 0);

      {
         auto lock = ses->lock();
         auto ref  = ses->get_ref<cache_test_node>(adr);
         (void)ref;
      }
      CHECK(queue.usage() == 1);
   }

   fs::remove_all(path);
}

TEST_CASE("promoter skips young HOT advisories before copying", "[sal][read_cache]")
{
   auto path = temp_dir("sal_read_cache_young_hot_skip_test");

   sal::runtime_config cfg;
   cfg.enable_read_cache        = true;
   cfg.max_pinned_cache_size_mb = 32;
   cfg.sync_mode                = sal::sync_type::mprotect;

   {
      sal::allocator alloc(path, cfg, false);
      auto           ses = alloc.get_session();
      auto           adr = ses->alloc<cache_test_node>(uint32_t{64}, sal::alloc_hint{});

      ses->sync(sal::sync_type::mprotect, cfg);
      alloc._mapped_state->_cache_difficulty_state._cache_difficulty.store(
          0, std::memory_order_relaxed);

      auto& queue = alloc._mapped_state->_session_data.rcache_queue(ses->get_session_num());
      const auto loc_before = ses->get(adr).loc();

      {
         auto lock = ses->lock();
         auto ref  = ses->get_ref<cache_test_node>(adr);
         (void)ref;
      }
      {
         auto lock = ses->lock();
         auto ref  = ses->get_ref<cache_test_node>(adr);
         (void)ref;
      }
      REQUIRE(queue.usage() == 1);

      alloc.compactor_promote_rcache_data(*ses);

      CHECK(queue.usage() == 0);
      CHECK(ses->get(adr).loc() == loc_before);
      CHECK(alloc._mapped_state->_cache_difficulty_state.total_young_hot_skips.load(
                std::memory_order_relaxed) == 1);
      CHECK(alloc._mapped_state->_cache_difficulty_state.total_young_hot_skipped_bytes.load(
                std::memory_order_relaxed) == 64);
      CHECK(alloc._mapped_state->_cache_difficulty_state.total_promoted_bytes.load(
                std::memory_order_relaxed) == 0);
      CHECK(alloc._mapped_state->_cache_difficulty_state
                .total_cache_policy_satisfied_bytes.load(std::memory_order_relaxed) == 64);
   }

   fs::remove_all(path);
}

TEST_CASE("disabled read cache does not enqueue sampled reads", "[sal][read_cache]")
{
   auto path = temp_dir("sal_read_cache_disabled_test");

   sal::runtime_config cfg;
   cfg.enable_read_cache        = false;
   cfg.max_pinned_cache_size_mb = 32;
   cfg.sync_mode                = sal::sync_type::mprotect;

   {
      sal::allocator alloc(path, cfg, false);
      auto           ses = alloc.get_session();
      auto           adr = ses->alloc<cache_test_node>(uint32_t{64}, sal::alloc_hint{});

      ses->sync(sal::sync_type::mprotect, cfg);
      alloc._mapped_state->_cache_difficulty_state._cache_difficulty.store(
          0, std::memory_order_relaxed);

      auto& queue = alloc._mapped_state->_session_data.rcache_queue(ses->get_session_num());

      for (int i = 0; i < 4; ++i)
      {
         auto lock = ses->lock();
         auto ref  = ses->get_ref<cache_test_node>(adr);
         (void)ref;
      }
      CHECK(queue.usage() == 0);
   }

   fs::remove_all(path);
}
