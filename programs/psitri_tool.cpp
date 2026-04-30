#include <filesystem>
#include <iomanip>
#include <iostream>
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cctype>
#include <cstring>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/variables_map.hpp>

#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/dwal/wal_status.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/write_cursor.hpp>
#include <sal/block_allocator.hpp>
#include <sal/config.hpp>
#include <sal/control_block_alloc.hpp>
#include <sal/mapped_memory/allocator_state.hpp>
#include <sal/mapped_memory/session_op_stats.hpp>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef PSITRI_TRICORDER_HAS_PSIO_JSON
#include <psio/to_json.hpp>
#endif

namespace po = boost::program_options;
namespace fs = std::filesystem;
using namespace psitri;

struct tricorder_fanout_buckets
{
   uint64_t fanout_1         = 0;
   uint64_t fanout_2         = 0;
   uint64_t fanout_3_to_4    = 0;
   uint64_t fanout_5_to_8    = 0;
   uint64_t fanout_9_to_16   = 0;
   uint64_t fanout_17_to_32  = 0;
   uint64_t fanout_33_to_64  = 0;
   uint64_t fanout_65_to_128 = 0;
   uint64_t fanout_129_plus  = 0;
};

struct tricorder_depth_stats
{
   uint32_t depth = 0;

   uint64_t inner_nodes          = 0;
   uint64_t inner_prefix_nodes   = 0;
   uint64_t value_nodes          = 0;
   uint64_t flat_value_nodes     = 0;
   uint64_t inner_branches       = 0;
   uint64_t single_branch_inners = 0;
   uint64_t low_fanout_inners    = 0;
   tricorder_fanout_buckets fanout;

   uint64_t leaf_nodes            = 0;
   uint64_t leaf_keys             = 0;
   uint64_t selected_leaf_keys    = 0;
   uint64_t key_bytes             = 0;
   uint64_t selected_key_bytes    = 0;
   uint64_t max_key_size          = 0;
   uint64_t max_selected_key_size = 0;
   uint64_t data_value_count      = 0;
   uint64_t data_value_bytes      = 0;
   uint64_t max_data_value_size   = 0;
   uint64_t leaf_clines           = 0;
   uint64_t max_leaf_clines       = 0;
   uint64_t cline_saturated_leaves = 0;
   uint64_t leaf_address_values   = 0;
   uint64_t leaf_alloc_bytes      = 0;
   uint64_t leaf_used_bytes       = 0;
   uint64_t leaf_dead_bytes       = 0;
   uint64_t leaf_empty_bytes      = 0;
   uint64_t full_leaf_nodes       = 0;
   uint64_t full_leaf_dead_bytes  = 0;
   uint64_t full_leaf_empty_bytes = 0;
};

struct tricorder_tree_stats_report
{
   uint32_t schema_version = 1;

   bool key_range_enabled   = false;
   std::string range_lower_hex;
   std::string range_upper_hex;
   bool root_filter_enabled = false;
   uint32_t root_filter_index = 0;
   bool scan_truncated = false;
   uint64_t max_nodes  = 0;

   uint32_t roots_checked         = 0;
   uint32_t roots_with_version    = 0;
   uint32_t roots_without_version = 0;

   uint64_t latest_version        = 0;
   uint64_t retained_versions     = 0;
   uint64_t dead_versions         = 0;
   uint64_t dead_version_ranges   = 0;
   uint64_t pending_dead_versions = 0;

   uint64_t nodes_visited        = 0;
   uint64_t shared_nodes_skipped = 0;
   uint64_t dangling_pointers    = 0;
   uint64_t reachable_bytes      = 0;

   uint64_t inner_nodes        = 0;
   uint64_t inner_prefix_nodes = 0;
   uint64_t leaf_nodes         = 0;
   uint64_t value_nodes        = 0;
   uint64_t flat_value_nodes   = 0;

   uint64_t inner_branches       = 0;
   uint64_t single_branch_inners = 0;
   uint64_t low_fanout_inners    = 0;
   uint64_t leaf_keys            = 0;
   uint64_t selected_leaf_keys    = 0;
   uint64_t key_bytes             = 0;
   uint64_t selected_key_bytes    = 0;
   uint64_t max_key_size          = 0;
   uint64_t max_selected_key_size = 0;
   uint64_t data_value_count      = 0;
   uint64_t data_value_bytes      = 0;
   uint64_t max_data_value_size   = 0;
   uint64_t leaf_clines           = 0;
   uint64_t max_leaf_clines       = 0;
   uint64_t cline_saturated_leaves = 0;
   uint64_t leaf_address_values   = 0;
   uint64_t max_depth            = 0;
   uint64_t leaf_depth_sum       = 0;
   uint64_t key_depth_sum        = 0;

   uint64_t total_inner_bytes      = 0;
   uint64_t total_leaf_alloc_bytes = 0;
   uint64_t total_leaf_used_bytes  = 0;
   uint64_t total_leaf_dead_bytes  = 0;
   uint64_t total_leaf_empty_bytes = 0;
   uint64_t full_leaf_nodes        = 0;
   uint64_t full_leaf_dead_bytes   = 0;
   uint64_t full_leaf_empty_bytes  = 0;
   uint64_t total_value_bytes      = 0;

   std::vector<uint64_t> branches_per_inner_node;
   std::vector<uint64_t> keys_per_leaf;
   std::vector<uint64_t> leaf_clines_histogram;
   std::vector<uint64_t> address_values_per_leaf;
   std::vector<uint64_t> leaf_depths;
   std::vector<tricorder_depth_stats> depth_stats;
};

struct tree_stats_shard_result
{
   uint32_t           shard_index = 0;
   tree_stats_options options;
   tree_stats_result  stats;
};

struct tree_stats_partitioned_summary
{
   uint32_t shard_count = 0;
   uint32_t jobs        = 0;

   std::string range_lower_hex;
   std::string range_upper_hex;
   bool root_filter_enabled = false;
   uint32_t root_filter_index = 0;
   bool scan_truncated = false;
   uint64_t max_nodes_per_shard = 0;

   uint64_t selected_leaf_keys    = 0;
   uint64_t nodes_visited         = 0;
   uint64_t leaf_nodes            = 0;
   uint64_t reachable_bytes       = 0;
   uint64_t dangling_pointers     = 0;
   uint64_t shared_nodes_skipped  = 0;
   uint64_t total_leaf_dead_bytes = 0;
   uint64_t total_leaf_empty_bytes = 0;

   std::vector<tree_stats_shard_result> shards;
};

#ifdef PSITRI_TRICORDER_HAS_PSIO_JSON
PSIO_REFLECT(tricorder_fanout_buckets,
             fanout_1,
             fanout_2,
             fanout_3_to_4,
             fanout_5_to_8,
             fanout_9_to_16,
             fanout_17_to_32,
             fanout_33_to_64,
             fanout_65_to_128,
             fanout_129_plus)

PSIO_REFLECT(tricorder_depth_stats,
             depth,
             inner_nodes,
             inner_prefix_nodes,
             value_nodes,
             flat_value_nodes,
             inner_branches,
             single_branch_inners,
             low_fanout_inners,
             fanout,
             leaf_nodes,
             leaf_keys,
             selected_leaf_keys,
             key_bytes,
             selected_key_bytes,
             max_key_size,
             max_selected_key_size,
             data_value_count,
             data_value_bytes,
             max_data_value_size,
             leaf_clines,
             max_leaf_clines,
             cline_saturated_leaves,
             leaf_address_values,
             leaf_alloc_bytes,
             leaf_used_bytes,
             leaf_dead_bytes,
             leaf_empty_bytes,
             full_leaf_nodes,
             full_leaf_dead_bytes,
             full_leaf_empty_bytes)

PSIO_REFLECT(tricorder_tree_stats_report,
             schema_version,
             key_range_enabled,
             range_lower_hex,
             range_upper_hex,
             root_filter_enabled,
             root_filter_index,
             scan_truncated,
             max_nodes,
             roots_checked,
             roots_with_version,
             roots_without_version,
             latest_version,
             retained_versions,
             dead_versions,
             dead_version_ranges,
             pending_dead_versions,
             nodes_visited,
             shared_nodes_skipped,
             dangling_pointers,
             reachable_bytes,
             inner_nodes,
             inner_prefix_nodes,
             leaf_nodes,
             value_nodes,
             flat_value_nodes,
             inner_branches,
             single_branch_inners,
             low_fanout_inners,
             leaf_keys,
             selected_leaf_keys,
             key_bytes,
             selected_key_bytes,
             max_key_size,
             max_selected_key_size,
             data_value_count,
             data_value_bytes,
             max_data_value_size,
             leaf_clines,
             max_leaf_clines,
             cline_saturated_leaves,
             leaf_address_values,
             max_depth,
             leaf_depth_sum,
             key_depth_sum,
             total_inner_bytes,
             total_leaf_alloc_bytes,
             total_leaf_used_bytes,
             total_leaf_dead_bytes,
             total_leaf_empty_bytes,
             full_leaf_nodes,
             full_leaf_dead_bytes,
             full_leaf_empty_bytes,
             total_value_bytes,
             branches_per_inner_node,
             keys_per_leaf,
             leaf_clines_histogram,
             address_values_per_leaf,
             leaf_depths,
             depth_stats)
#endif

// ── helpers ──────────────────────────────────────────────────────────────────

constexpr const char* stale_refcount_warning =
    "\n  Warning: unclean shutdown was detected earlier; no rebuild was forced.\n"
    "           Reference counts may be stale, so leaked disk space may remain\n"
    "           until `psitricorder audit-refcounts` or another full refcount\n"
    "           scan repairs the accounting.\n";

uint64_t file_size_or_zero(const fs::path& p)
{
   std::error_code ec;
   auto            sz = fs::file_size(p, ec);
   return ec ? 0 : sz;
}

uint64_t dir_size(const fs::path& dir)
{
   uint64_t        total = 0;
   std::error_code ec;
   for (auto& entry : fs::recursive_directory_iterator(dir, ec))
      if (entry.is_regular_file())
         total += entry.file_size();
   return total;
}

std::string format_bytes(uint64_t bytes)
{
   return sal::seg_alloc_dump::format_bytes(bytes);
}

std::string format_number(uint64_t n);

std::string format_rate(double bytes_per_sec)
{
   if (bytes_per_sec <= 0)
      return "0 B/s";
   return format_bytes(static_cast<uint64_t>(bytes_per_sec)) + "/s";
}

std::string format_signed_rate(double bytes_per_sec)
{
   if (bytes_per_sec < 0)
      return "-" + format_rate(-bytes_per_sec);
   return format_rate(bytes_per_sec);
}

std::string format_ratio(double numerator, double denominator)
{
   if (denominator <= 0)
      return "0.0%";
   std::ostringstream out;
   out << std::fixed << std::setprecision(1) << (100.0 * numerator / denominator) << "%";
   return out.str();
}

uint64_t difficulty_attempts(uint64_t difficulty)
{
   const uint64_t max_difficulty = ~uint64_t{0};
   const uint64_t gap            = max_difficulty - difficulty;
   if (gap == 0)
      return max_difficulty;
   return max_difficulty / gap;
}

struct passive_header_mapping
{
   int    fd   = -1;
   void*  data = MAP_FAILED;
   size_t size = 0;

   explicit passive_header_mapping(const fs::path& header_path)
   {
      fd = ::open(header_path.native().c_str(), O_RDONLY | O_CLOEXEC);
      if (fd < 0)
         throw std::runtime_error("open(" + header_path.string() + "): " +
                                  std::strerror(errno));

      struct stat st;
      if (::fstat(fd, &st) != 0)
         throw std::runtime_error("fstat(" + header_path.string() + "): " +
                                  std::strerror(errno));
      if (st.st_size < static_cast<off_t>(sizeof(sal::mapped_memory::allocator_state)))
         throw std::runtime_error("header is smaller than allocator_state");

      size = static_cast<size_t>(st.st_size);
      data = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
      if (data == MAP_FAILED)
         throw std::runtime_error("mmap(" + header_path.string() + "): " +
                                  std::strerror(errno));
   }

   passive_header_mapping(const passive_header_mapping&)            = delete;
   passive_header_mapping& operator=(const passive_header_mapping&) = delete;

   ~passive_header_mapping()
   {
      if (data != MAP_FAILED)
         ::munmap(data, size);
      if (fd >= 0)
         ::close(fd);
   }

   const sal::mapped_memory::allocator_state* state() const
   {
      return static_cast<const sal::mapped_memory::allocator_state*>(data);
   }
};

struct passive_ptr_header_mapping
{
   int    fd   = -1;
   void*  data = MAP_FAILED;
   size_t size = 0;

   explicit passive_ptr_header_mapping(const fs::path& header_path)
   {
      fd = ::open(header_path.native().c_str(), O_RDONLY | O_CLOEXEC);
      if (fd < 0)
         throw std::runtime_error("open(" + header_path.string() + "): " +
                                  std::strerror(errno));

      struct stat st;
      if (::fstat(fd, &st) != 0)
         throw std::runtime_error("fstat(" + header_path.string() + "): " +
                                  std::strerror(errno));
      if (st.st_size < static_cast<off_t>(sizeof(sal::detail::ptr_alloc_header)))
         throw std::runtime_error("ptrs/header.bin is smaller than ptr_alloc_header");

      size = static_cast<size_t>(st.st_size);
      data = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
      if (data == MAP_FAILED)
         throw std::runtime_error("mmap(" + header_path.string() + "): " +
                                  std::strerror(errno));
   }

   passive_ptr_header_mapping(const passive_ptr_header_mapping&)            = delete;
   passive_ptr_header_mapping& operator=(const passive_ptr_header_mapping&) = delete;

   ~passive_ptr_header_mapping()
   {
      if (data != MAP_FAILED)
         ::munmap(data, size);
      if (fd >= 0)
         ::close(fd);
   }

   const sal::detail::ptr_alloc_header* header() const
   {
      return static_cast<const sal::detail::ptr_alloc_header*>(data);
   }
};

struct passive_session_ops_mapping
{
   int    fd   = -1;
   void*  data = MAP_FAILED;
   size_t size = 0;

   explicit passive_session_ops_mapping(const fs::path& path)
   {
      fd = ::open(path.native().c_str(), O_RDONLY | O_CLOEXEC);
      if (fd < 0)
         throw std::runtime_error("open(" + path.string() + "): " + std::strerror(errno));

      struct stat st;
      if (::fstat(fd, &st) != 0)
         throw std::runtime_error("fstat(" + path.string() + "): " + std::strerror(errno));
      if (st.st_size < static_cast<off_t>(sizeof(sal::mapped_memory::session_operation_stats)))
         throw std::runtime_error("session_ops.bin is smaller than session_operation_stats");

      size = static_cast<size_t>(st.st_size);
      data = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
      if (data == MAP_FAILED)
         throw std::runtime_error("mmap(" + path.string() + "): " + std::strerror(errno));
   }

   passive_session_ops_mapping(const passive_session_ops_mapping&)            = delete;
   passive_session_ops_mapping& operator=(const passive_session_ops_mapping&) = delete;

   ~passive_session_ops_mapping()
   {
      if (data != MAP_FAILED)
         ::munmap(data, size);
      if (fd >= 0)
         ::close(fd);
   }

   const sal::mapped_memory::session_operation_stats* stats() const
   {
      return static_cast<const sal::mapped_memory::session_operation_stats*>(data);
   }
};

struct passive_database_mapping
{
   int    fd   = -1;
   void*  data = MAP_FAILED;
   size_t size = 0;

   explicit passive_database_mapping(const fs::path& dbfile_path)
   {
      fd = ::open(dbfile_path.native().c_str(), O_RDONLY | O_CLOEXEC);
      if (fd < 0)
         throw std::runtime_error("open(" + dbfile_path.string() + "): " +
                                  std::strerror(errno));

      struct stat st;
      if (::fstat(fd, &st) != 0)
         throw std::runtime_error("fstat(" + dbfile_path.string() + "): " +
                                  std::strerror(errno));
      if (st.st_size < static_cast<off_t>(sizeof(psitri::detail::database_state)))
         throw std::runtime_error("dbfile.bin is smaller than database_state");

      size = static_cast<size_t>(st.st_size);
      data = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
      if (data == MAP_FAILED)
         throw std::runtime_error("mmap(" + dbfile_path.string() + "): " +
                                  std::strerror(errno));
   }

   passive_database_mapping(const passive_database_mapping&)            = delete;
   passive_database_mapping& operator=(const passive_database_mapping&) = delete;

   ~passive_database_mapping()
   {
      if (data != MAP_FAILED)
         ::munmap(data, size);
      if (fd >= 0)
         ::close(fd);
   }

   const psitri::detail::database_state* state() const
   {
      return static_cast<const psitri::detail::database_state*>(data);
   }
};

struct passive_wal_status_mapping
{
   int    fd   = -1;
   void*  data = MAP_FAILED;
   size_t size = 0;

   explicit passive_wal_status_mapping(const fs::path& path)
   {
      fd = ::open(path.native().c_str(), O_RDONLY | O_CLOEXEC);
      if (fd < 0)
         throw std::runtime_error("open(" + path.string() + "): " + std::strerror(errno));

      struct stat st;
      if (::fstat(fd, &st) != 0)
         throw std::runtime_error("fstat(" + path.string() + "): " + std::strerror(errno));
      if (st.st_size < static_cast<off_t>(sizeof(psitri::dwal::wal_status_file)))
         throw std::runtime_error("wal/status.bin is smaller than wal_status_file");

      size = static_cast<size_t>(st.st_size);
      data = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
      if (data == MAP_FAILED)
         throw std::runtime_error("mmap(" + path.string() + "): " + std::strerror(errno));

      const auto* mapped_file = static_cast<const psitri::dwal::wal_status_file*>(data);
      const auto* hdr = &mapped_file->roots_header;
      if (hdr->magic != psitri::dwal::wal_status_magic ||
          hdr->version != psitri::dwal::wal_status_version)
         throw std::runtime_error("wal/status.bin has incompatible header");
   }

   passive_wal_status_mapping(const passive_wal_status_mapping&)            = delete;
   passive_wal_status_mapping& operator=(const passive_wal_status_mapping&) = delete;

   ~passive_wal_status_mapping()
   {
      if (data != MAP_FAILED)
         ::munmap(data, size);
      if (fd >= 0)
         ::close(fd);
   }

   const psitri::dwal::wal_status_file* file() const
   {
      return static_cast<const psitri::dwal::wal_status_file*>(data);
   }
};

struct passive_root_object_record
{
   std::atomic<uint64_t> tree;
   std::atomic<uint64_t> version;
   std::atomic<uint64_t> check;
};

static_assert(sizeof(passive_root_object_record) == 24);

struct passive_roots_mapping
{
   int    fd   = -1;
   void*  data = MAP_FAILED;
   size_t size = 0;

   explicit passive_roots_mapping(const fs::path& roots_path)
   {
      fd = ::open(roots_path.native().c_str(), O_RDONLY | O_CLOEXEC);
      if (fd < 0)
         throw std::runtime_error("open(" + roots_path.string() + "): " +
                                  std::strerror(errno));

      struct stat st;
      if (::fstat(fd, &st) != 0)
         throw std::runtime_error("fstat(" + roots_path.string() + "): " +
                                  std::strerror(errno));
      const size_t min_size = sizeof(passive_root_object_record) * num_top_roots;
      if (st.st_size < static_cast<off_t>(min_size))
         throw std::runtime_error("roots is smaller than top-root records");

      size = static_cast<size_t>(st.st_size);
      data = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
      if (data == MAP_FAILED)
         throw std::runtime_error("mmap(" + roots_path.string() + "): " +
                                  std::strerror(errno));
   }

   passive_roots_mapping(const passive_roots_mapping&)            = delete;
   passive_roots_mapping& operator=(const passive_roots_mapping&) = delete;

   ~passive_roots_mapping()
   {
      if (data != MAP_FAILED)
         ::munmap(data, size);
      if (fd >= 0)
         ::close(fd);
   }

   const passive_root_object_record* records() const
   {
      return static_cast<const passive_root_object_record*>(data);
   }
};

uint64_t passive_root_object_check(uint64_t packed_tree, uint64_t root_version) noexcept
{
   uint64_t x = packed_tree ^ (root_version + 0x9e3779b97f4a7c15ull +
                               (packed_tree << 6) + (packed_tree >> 2));
   x ^= x >> 30;
   x *= 0xbf58476d1ce4e5b9ull;
   x ^= x >> 27;
   x *= 0x94d049bb133111ebull;
   x ^= x >> 31;
   return x ? x : 1;
}

struct mfu_snapshot
{
   std::chrono::steady_clock::time_point t;
   uint64_t difficulty              = 0;
   uint64_t policy_satisfied        = 0;
   uint64_t promoted                = 0;
   uint64_t cold_to_hot_promotions  = 0;
   uint64_t cold_to_hot_bytes       = 0;
   uint64_t hot_to_hot_promotions   = 0;
   uint64_t hot_to_hot_bytes        = 0;
   uint64_t hot_to_hot_pressure_ppm = 0;
   uint64_t hot_to_hot_byte_pressure_ppm = 0;
   uint64_t young_hot_skips         = 0;
   uint64_t young_hot_skipped_bytes = 0;
   uint64_t young_hot_skip_pressure_ppm = 0;
   uint64_t young_hot_skip_byte_pressure_ppm = 0;
   uint64_t promoted_to_cold_count  = 0;
   uint64_t promoted_to_cold_bytes  = 0;
   uint64_t target_bps              = 0;
   uint64_t cache_bytes             = 0;
   uint64_t window_sec              = 0;
   uint64_t mlocked_regions         = 0;
   uint64_t successful_mlock_regions = 0;
   uint64_t failed_mlock_regions     = 0;
   uint64_t successful_munlock_regions = 0;
   uint64_t failed_munlock_regions     = 0;
   uint64_t cb_header_pinned          = 0;
   uint64_t cb_mlock_success_regions  = 0;
   uint64_t cb_mlock_failed_regions   = 0;
   uint64_t cb_mlock_skipped_regions  = 0;
   uint64_t cb_mlock_success_bytes    = 0;
};

mfu_snapshot read_mfu_snapshot(const sal::mapped_memory::allocator_state& state,
                               const sal::detail::ptr_alloc_header*      ptr_header)
{
   mfu_snapshot s;
   s.t          = std::chrono::steady_clock::now();
   s.difficulty =
       state._cache_difficulty_state.get_cache_difficulty();
   s.policy_satisfied =
       state._cache_difficulty_state.total_cache_policy_satisfied_bytes.load(
           std::memory_order_relaxed);
   s.promoted =
       state._cache_difficulty_state.total_promoted_bytes.load(std::memory_order_relaxed);
   s.cold_to_hot_promotions =
       state._cache_difficulty_state.total_cold_to_hot_promotions.load(
           std::memory_order_relaxed);
   s.cold_to_hot_bytes =
       state._cache_difficulty_state.total_cold_to_hot_promoted_bytes.load(
           std::memory_order_relaxed);
   s.hot_to_hot_promotions =
       state._cache_difficulty_state.total_hot_to_hot_promotions.load(
           std::memory_order_relaxed);
   s.hot_to_hot_bytes =
       state._cache_difficulty_state.total_hot_to_hot_promoted_bytes.load(
           std::memory_order_relaxed);
   s.hot_to_hot_pressure_ppm =
       state._cache_difficulty_state.total_hot_to_hot_demote_pressure_ppm.load(
           std::memory_order_relaxed);
   s.hot_to_hot_byte_pressure_ppm =
       state._cache_difficulty_state.total_hot_to_hot_byte_demote_pressure_ppm.load(
           std::memory_order_relaxed);
   s.young_hot_skips =
       state._cache_difficulty_state.total_young_hot_skips.load(std::memory_order_relaxed);
   s.young_hot_skipped_bytes =
       state._cache_difficulty_state.total_young_hot_skipped_bytes.load(
           std::memory_order_relaxed);
   s.young_hot_skip_pressure_ppm =
       state._cache_difficulty_state.total_young_hot_skip_demote_pressure_ppm.load(
           std::memory_order_relaxed);
   s.young_hot_skip_byte_pressure_ppm =
       state._cache_difficulty_state.total_young_hot_skip_byte_demote_pressure_ppm.load(
           std::memory_order_relaxed);
   s.promoted_to_cold_count =
       state._cache_difficulty_state.total_promoted_to_cold_promotions.load(
           std::memory_order_relaxed);
   s.promoted_to_cold_bytes =
       state._cache_difficulty_state.total_promoted_to_cold_bytes.load(
           std::memory_order_relaxed);
   s.cache_bytes = state._cache_difficulty_state._total_cache_size;
   s.window_sec =
       uint64_t(state._cache_difficulty_state._cache_frequency_window.count() / 1000);
   s.target_bps = state._cache_difficulty_state.target_promoted_bytes_per_sec();
   s.mlocked_regions = state._segment_provider.mlock_segments.count();
   s.successful_mlock_regions =
       state._segment_provider.successful_mlock_regions.load(std::memory_order_relaxed);
   s.failed_mlock_regions =
       state._segment_provider.failed_mlock_regions.load(std::memory_order_relaxed);
   s.successful_munlock_regions =
       state._segment_provider.successful_munlock_regions.load(std::memory_order_relaxed);
   s.failed_munlock_regions =
       state._segment_provider.failed_munlock_regions.load(std::memory_order_relaxed);
   if (ptr_header)
   {
      s.cb_header_pinned =
          ptr_header->control_block_header_mlock_pinned.load(std::memory_order_relaxed);
      s.cb_mlock_success_regions =
          ptr_header->control_block_zone_mlock_success_regions.load(
              std::memory_order_relaxed) +
          ptr_header->control_block_freelist_mlock_success_regions.load(
              std::memory_order_relaxed);
      s.cb_mlock_failed_regions =
          ptr_header->control_block_zone_mlock_failed_regions.load(
              std::memory_order_relaxed) +
          ptr_header->control_block_freelist_mlock_failed_regions.load(
              std::memory_order_relaxed);
      s.cb_mlock_skipped_regions =
          ptr_header->control_block_zone_mlock_skipped_regions.load(
              std::memory_order_relaxed) +
          ptr_header->control_block_freelist_mlock_skipped_regions.load(
              std::memory_order_relaxed);
      s.cb_mlock_success_bytes =
          ptr_header->control_block_zone_mlock_success_bytes.load(std::memory_order_relaxed) +
          ptr_header->control_block_freelist_mlock_success_bytes.load(
              std::memory_order_relaxed);
   }
   return s;
}

int cmd_mfu_watch(const fs::path& dir, uint64_t interval_ms, uint64_t samples)
{
   if (interval_ms == 0)
      interval_ms = 1000;

   fs::path db_dir = dir;
   if (!fs::exists(db_dir / "header") && fs::exists(db_dir / "chaindata" / "header"))
      db_dir = db_dir / "chaindata";

   passive_header_mapping mapping(db_dir / "header");
   const auto*            state = mapping.state();
   std::unique_ptr<passive_ptr_header_mapping> ptr_mapping;
   if (fs::exists(db_dir / "ptrs" / "header.bin"))
      ptr_mapping = std::make_unique<passive_ptr_header_mapping>(db_dir / "ptrs" / "header.bin");
   const auto* ptr_header = ptr_mapping ? ptr_mapping->header() : nullptr;

   auto first = read_mfu_snapshot(*state, ptr_header);
   auto prev  = first;

   std::cout << "Passive MFU watch: " << fs::canonical(db_dir) << "\n";
   std::cout << "Reads only the mmap'd header; it does not open a database session.\n\n";
	   std::cout << std::setw(8) << std::left << "sample"
	             << std::setw(16) << std::left << "promote/s"
	             << std::setw(16) << std::left << "cold->hot/s"
	             << std::setw(16) << std::left << "refresh/s"
	             << std::setw(16) << std::left << "skip/s"
	             << std::setw(16) << std::left << "policy/s"
	             << std::setw(16) << std::left << "avg/s"
	             << std::setw(16) << std::left << "target/s"
	             << std::setw(18) << std::left << "promoted"
	             << std::setw(18) << std::left << "policy"
	             << std::setw(18) << std::left << "c2hBytes"
	             << std::setw(18) << std::left << "refBytes"
	             << std::setw(18) << std::left << "skipBytes"
	             << std::setw(18) << std::left << "toColdBytes"
	             << std::setw(14) << std::left << "c2h"
	             << std::setw(14) << std::left << "refresh"
	             << std::setw(14) << std::left << "skip"
	             << std::setw(14) << std::left << "toCold"
	             << std::setw(10) << std::left << "mlock"
	             << std::setw(12) << std::left << "mlockOK"
	             << std::setw(12) << std::left << "mlockFail"
             << std::setw(8) << std::left << "cbHdr"
             << std::setw(10) << std::left << "cbOK"
             << std::setw(10) << std::left << "cbFail"
             << std::setw(10) << std::left << "cbSkip"
             << std::setw(12) << std::left << "cbBytes"
             << std::setw(10) << std::left << "ref%"
             << std::setw(10) << std::left << "skip%"
             << std::setw(14) << std::left << "difficulty"
             << "odds\n";

   for (uint64_t i = 0; samples == 0 || i < samples; ++i)
   {
      if (i != 0)
         std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));

      auto cur = read_mfu_snapshot(*state, ptr_header);
      auto interval =
          std::chrono::duration<double>(cur.t - prev.t).count();
      auto total_interval =
          std::chrono::duration<double>(cur.t - first.t).count();

      double interval_bps = 0;
      if (i != 0 && interval > 0 && cur.promoted >= prev.promoted)
         interval_bps = double(cur.promoted - prev.promoted) / interval;

      double cold_to_hot_bps = 0;
      if (i != 0 && interval > 0 && cur.cold_to_hot_bytes >= prev.cold_to_hot_bytes)
         cold_to_hot_bps = double(cur.cold_to_hot_bytes - prev.cold_to_hot_bytes) / interval;

      double hot_to_hot_bps = 0;
      if (i != 0 && interval > 0 && cur.hot_to_hot_bytes >= prev.hot_to_hot_bytes)
         hot_to_hot_bps = double(cur.hot_to_hot_bytes - prev.hot_to_hot_bytes) / interval;

      double young_hot_skip_bps = 0;
      if (i != 0 && interval > 0 &&
          cur.young_hot_skipped_bytes >= prev.young_hot_skipped_bytes)
         young_hot_skip_bps =
             double(cur.young_hot_skipped_bytes - prev.young_hot_skipped_bytes) / interval;

      double policy_bps = 0;
      if (i != 0 && interval > 0 && cur.policy_satisfied >= prev.policy_satisfied)
         policy_bps = double(cur.policy_satisfied - prev.policy_satisfied) / interval;

      double refresh_pressure = 0;
      if (cur.hot_to_hot_bytes > prev.hot_to_hot_bytes &&
          cur.hot_to_hot_byte_pressure_ppm >= prev.hot_to_hot_byte_pressure_ppm)
      {
         refresh_pressure =
             double(cur.hot_to_hot_byte_pressure_ppm - prev.hot_to_hot_byte_pressure_ppm) /
             double(cur.hot_to_hot_bytes - prev.hot_to_hot_bytes) / 10000.0;
      }

      double skip_pressure = 0;
      if (cur.young_hot_skipped_bytes > prev.young_hot_skipped_bytes &&
          cur.young_hot_skip_byte_pressure_ppm >= prev.young_hot_skip_byte_pressure_ppm)
      {
         skip_pressure =
             double(cur.young_hot_skip_byte_pressure_ppm -
                    prev.young_hot_skip_byte_pressure_ppm) /
             double(cur.young_hot_skipped_bytes - prev.young_hot_skipped_bytes) / 10000.0;
      }

      double avg_bps = 0;
      if (total_interval > 0 && cur.promoted >= first.promoted)
         avg_bps = double(cur.promoted - first.promoted) / total_interval;

      std::cout << std::setw(8) << std::left << i
                << std::setw(16) << std::left << format_rate(interval_bps)
                << std::setw(16) << std::left << format_rate(cold_to_hot_bps)
                << std::setw(16) << std::left << format_rate(hot_to_hot_bps)
                << std::setw(16) << std::left << format_rate(young_hot_skip_bps)
	                << std::setw(16) << std::left << format_rate(policy_bps)
	                << std::setw(16) << std::left << format_rate(avg_bps)
	                << std::setw(16) << std::left << format_rate(double(cur.target_bps))
	                << std::setw(18) << std::left << format_bytes(cur.promoted)
	                << std::setw(18) << std::left << format_bytes(cur.policy_satisfied)
	                << std::setw(18) << std::left << format_bytes(cur.cold_to_hot_bytes)
	                << std::setw(18) << std::left << format_bytes(cur.hot_to_hot_bytes)
	                << std::setw(18) << std::left << format_bytes(cur.young_hot_skipped_bytes)
	                << std::setw(18) << std::left << format_bytes(cur.promoted_to_cold_bytes)
	                << std::setw(14) << std::left << cur.cold_to_hot_promotions
	                << std::setw(14) << std::left << cur.hot_to_hot_promotions
	                << std::setw(14) << std::left << cur.young_hot_skips
	                << std::setw(14) << std::left << cur.promoted_to_cold_count
	                << std::setw(10) << std::left << cur.mlocked_regions
	                << std::setw(12) << std::left << cur.successful_mlock_regions
	                << std::setw(12) << std::left << cur.failed_mlock_regions
                << std::setw(8) << std::left << (cur.cb_header_pinned ? "yes" : "no")
                << std::setw(10) << std::left << cur.cb_mlock_success_regions
                << std::setw(10) << std::left << cur.cb_mlock_failed_regions
                << std::setw(10) << std::left << cur.cb_mlock_skipped_regions
                << std::setw(12) << std::left << format_bytes(cur.cb_mlock_success_bytes)
                << std::setw(10) << std::left << std::fixed << std::setprecision(1)
                << refresh_pressure
                << std::setw(10) << std::left << std::fixed << std::setprecision(1)
                << skip_pressure
                << std::setw(14) << std::left << cur.difficulty
                << "1/" << std::setw(12) << std::left
                << difficulty_attempts(cur.difficulty) << "\n";
      std::cout.flush();

      prev = cur;
   }

   return 0;
}

struct dashboard_snapshot
{
   std::chrono::steady_clock::time_point t;
   mfu_snapshot cache;
   std::array<uint64_t, sal::mapped_memory::session_operation_count> session_ops{};
   bool session_ops_available = false;
   bool wal_status_available = false;
   uint64_t wal_active_roots = 0;
   uint64_t wal_merging_roots = 0;
   uint64_t wal_rw_entries = 0;
   uint64_t wal_rw_arena_bytes = 0;
   uint64_t wal_ro_entries = 0;
   uint64_t wal_ro_arena_bytes = 0;
   uint64_t wal_rw_file_bytes = 0;
   uint64_t wal_rw_buffered_bytes = 0;
   uint64_t wal_rw_logical_bytes = 0;
   uint64_t wal_ro_file_bytes = 0;
   uint64_t wal_entries = 0;
   uint64_t wal_ops = 0;
   uint64_t wal_upsert_data_ops = 0;
   uint64_t wal_upsert_subtree_ops = 0;
   uint64_t wal_remove_ops = 0;
   uint64_t wal_remove_range_ops = 0;
   uint64_t wal_multi_entries = 0;
   uint64_t wal_committed_entry_bytes = 0;
   uint64_t wal_key_bytes = 0;
   uint64_t wal_value_bytes = 0;
   uint64_t wal_write_calls = 0;
   uint64_t wal_write_bytes = 0;
   uint64_t wal_flush_calls = 0;
   uint64_t wal_fsync_calls = 0;
   uint64_t wal_fullsync_calls = 0;
   uint64_t wal_clean_closes = 0;
   uint64_t wal_discarded_entries = 0;
   uint64_t wal_swaps = 0;
   uint64_t wal_merge_requests = 0;
   uint64_t wal_merge_completions = 0;
   uint64_t wal_merge_aborts = 0;
   uint64_t wal_merge_entries = 0;
   uint64_t wal_merge_range_tombstones = 0;
   uint64_t wal_merge_wall_ns = 0;
   uint64_t wal_merge_commit_ns = 0;
   uint64_t wal_merge_cpu_ns = 0;
   uint64_t wal_max_throttle_sleep_ns = 0;

   bool        dbfile_metadata_available = false;
   bool        root_metadata_available   = false;
   bool        clean_shutdown            = false;
   bool        ref_counts_stale          = false;
   uint32_t    db_flags                  = 0;
   uint64_t    top_version               = 0;
   uint64_t    epoch_interval            = 0;
   uint64_t    epoch_base                = 0;
   uint64_t    populated_roots           = 0;
   uint64_t    roots_with_version        = 0;
   uint64_t    roots_without_version     = 0;
   uint64_t    active_root_versions      = 0;
   uint64_t    oldest_root_version       = 0;
   uint64_t    newest_root_version       = 0;
   bool        active_root_address_available = false;
   uint32_t    active_root_index             = 0;
   uint32_t    active_root_address           = *sal::null_ptr_address;
   uint32_t    active_root_version_address   = *sal::null_ptr_address;
   uint64_t    active_root_record_version    = 0;
   std::string version_error;

   uint64_t compact_pinned_threshold_mb   = 0;
   uint64_t compact_unpinned_threshold_mb = 0;

   uint64_t seg_file_bytes = 0;
   uint64_t total_segments = 0;

   uint64_t active_segments = 0;
   uint64_t read_only_segments = 0;
   uint64_t pinned_segments = 0;
   uint64_t pending_segments = 0;
   uint64_t free_segments = 0;
   uint64_t queued_segments = 0;
   uint64_t zero_flag_segments = 0;

   uint64_t may_compact_segments = 0;
   uint64_t may_compact_pinned_segments = 0;
   uint64_t may_compact_unpinned_segments = 0;
   uint64_t reclaimable_bytes = 0;
   uint64_t eligible_reclaimable_bytes = 0;
   uint64_t eligible_pinned_reclaimable_bytes = 0;
   uint64_t eligible_unpinned_reclaimable_bytes = 0;
   uint64_t blocked_reclaimable_bytes = 0;
   uint64_t active_reclaimable_bytes = 0;
   uint64_t pending_reclaimable_bytes = 0;
   uint64_t free_segment_bytes = 0;

   uint64_t ready_pinned_depth = 0;
   uint64_t ready_unpinned_depth = 0;
   uint64_t recycled_depth = 0;
   uint64_t recycled_capacity = 0;
   uint64_t recycled_available_to_pop = 0;
   uint64_t recycled_available_to_push = 0;
   uint64_t recycle_readlock_blocked = 0;

   uint64_t active_sessions = 0;
   uint64_t session_bytes_written = 0;
   uint64_t rcache_depth = 0;
   uint64_t release_queue_depth = 0;
   uint64_t mlock_success_regions = 0;
   uint64_t mlock_fail_regions = 0;
   uint64_t munlock_success_regions = 0;
   uint64_t munlock_fail_regions = 0;
   uint64_t cb_mlock_success_bytes = 0;
};

constexpr uint32_t seg_flag_read_only = 1u << 0;
constexpr uint32_t seg_flag_pinned    = 1u << 1;
constexpr uint32_t seg_flag_active    = 1u << 2;
constexpr uint32_t seg_flag_pending   = 1u << 3;
constexpr uint32_t seg_flag_free      = 1u << 4;
constexpr uint32_t seg_flag_queued    = 1u << 5;

void read_version_metadata(const fs::path& db_dir, dashboard_snapshot& s)
{
   try
   {
      if (fs::exists(db_dir / "dbfile.bin"))
      {
         passive_database_mapping db_mapping(db_dir / "dbfile.bin");
         const auto*              db_state = db_mapping.state();
         s.dbfile_metadata_available = true;
         s.clean_shutdown =
             db_state->clean_shutdown.load(std::memory_order_relaxed);
         s.db_flags         = db_state->flags;
         s.ref_counts_stale = (db_state->flags & psitri::detail::flag_ref_counts_stale) != 0;
         s.top_version =
             db_state->global_version.load(std::memory_order_relaxed);
         s.epoch_interval = db_state->epoch_interval;
         if (s.epoch_interval != 0)
            s.epoch_base = (s.top_version / s.epoch_interval) * s.epoch_interval;
      }

      if (fs::exists(db_dir / "roots"))
      {
         passive_roots_mapping roots_mapping(db_dir / "roots");
         const auto*           roots = roots_mapping.records();
         std::vector<uint64_t> versions;
         versions.reserve(num_top_roots);

         for (uint32_t i = 0; i < num_top_roots; ++i)
         {
            const uint64_t packed_tree =
                roots[i].tree.load(std::memory_order_acquire);
            auto tid = sal::tree_id::unpack(packed_tree);
            if (tid.root == sal::null_ptr_address)
               continue;

            ++s.populated_roots;
            const uint64_t root_version =
                roots[i].version.load(std::memory_order_relaxed);
            if (!s.active_root_address_available)
            {
               s.active_root_address_available = true;
               s.active_root_index             = i;
               s.active_root_address           = *tid.root;
               s.active_root_version_address   = *tid.ver;
               s.active_root_record_version    = root_version;
            }
            const uint64_t root_check =
                roots[i].check.load(std::memory_order_relaxed);
            const bool valid_version_record =
                root_version != 0 &&
                root_check == passive_root_object_check(packed_tree, root_version);

            if (valid_version_record)
            {
               ++s.roots_with_version;
               versions.push_back(root_version);
            }
            else
            {
               ++s.roots_without_version;
            }
         }

         std::sort(versions.begin(), versions.end());
         versions.erase(std::unique(versions.begin(), versions.end()), versions.end());
         s.active_root_versions = versions.size();
         if (!versions.empty())
         {
            s.oldest_root_version = versions.front();
            s.newest_root_version = versions.back();
         }
         s.root_metadata_available = true;
      }
   }
   catch (const std::exception& e)
   {
      s.version_error = e.what();
   }
}

dashboard_snapshot read_dashboard_snapshot(const fs::path& db_dir,
                                           const sal::mapped_memory::allocator_state& state,
                                           const sal::detail::ptr_alloc_header* ptr_header,
                                           const sal::mapped_memory::session_operation_stats*
                                               op_stats,
                                           const psitri::dwal::wal_status_file* wal_status)
{
   dashboard_snapshot s;
   s.t = std::chrono::steady_clock::now();
   s.cache = read_mfu_snapshot(state, ptr_header);
   read_version_metadata(db_dir, s);
   if (wal_status && wal_status->roots_header.magic == psitri::dwal::wal_status_magic &&
       wal_status->roots_header.version == psitri::dwal::wal_status_version)
   {
      s.wal_status_available = true;
      const uint32_t root_count =
         std::min<uint32_t>(wal_status->roots_header.root_count,
                            psitri::dwal::wal_status_max_roots);
      for (uint32_t i = 0; i < root_count; ++i)
      {
         const auto& r = wal_status->roots[i];
         if (r.active.load(std::memory_order_relaxed) == 0)
            continue;
         ++s.wal_active_roots;
         if (r.merge_complete.load(std::memory_order_relaxed) == 0)
            ++s.wal_merging_roots;
         s.wal_rw_entries += r.rw_layer_entries.load(std::memory_order_relaxed);
         s.wal_rw_arena_bytes += r.rw_arena_bytes.load(std::memory_order_relaxed);
         s.wal_ro_entries += r.ro_layer_entries.load(std::memory_order_relaxed);
         s.wal_ro_arena_bytes += r.ro_arena_bytes.load(std::memory_order_relaxed);
         s.wal_rw_file_bytes += r.rw_wal_file_bytes.load(std::memory_order_relaxed);
         s.wal_rw_buffered_bytes += r.rw_wal_buffered_bytes.load(std::memory_order_relaxed);
         s.wal_rw_logical_bytes += r.rw_wal_logical_bytes.load(std::memory_order_relaxed);
         s.wal_ro_file_bytes += r.ro_wal_file_bytes.load(std::memory_order_relaxed);
         s.wal_entries += r.wal_entries.load(std::memory_order_relaxed);
         s.wal_ops += r.wal_ops.load(std::memory_order_relaxed);
         s.wal_upsert_data_ops += r.wal_upsert_data_ops.load(std::memory_order_relaxed);
         s.wal_upsert_subtree_ops += r.wal_upsert_subtree_ops.load(std::memory_order_relaxed);
         s.wal_remove_ops += r.wal_remove_ops.load(std::memory_order_relaxed);
         s.wal_remove_range_ops += r.wal_remove_range_ops.load(std::memory_order_relaxed);
         s.wal_multi_entries += r.wal_multi_entries.load(std::memory_order_relaxed);
         s.wal_committed_entry_bytes +=
            r.wal_committed_entry_bytes.load(std::memory_order_relaxed);
         s.wal_key_bytes += r.wal_key_bytes.load(std::memory_order_relaxed);
         s.wal_value_bytes += r.wal_value_bytes.load(std::memory_order_relaxed);
         s.wal_write_calls += r.wal_write_calls.load(std::memory_order_relaxed);
         s.wal_write_bytes += r.wal_write_bytes.load(std::memory_order_relaxed);
         s.wal_flush_calls += r.wal_flush_calls.load(std::memory_order_relaxed);
         s.wal_fsync_calls += r.wal_fsync_calls.load(std::memory_order_relaxed);
         s.wal_fullsync_calls += r.wal_fullsync_calls.load(std::memory_order_relaxed);
         s.wal_clean_closes += r.wal_clean_closes.load(std::memory_order_relaxed);
         s.wal_discarded_entries += r.wal_discarded_entries.load(std::memory_order_relaxed);
         s.wal_swaps += r.swaps.load(std::memory_order_relaxed);
         s.wal_merge_requests += r.merge_requests.load(std::memory_order_relaxed);
         s.wal_merge_completions += r.merge_completions.load(std::memory_order_relaxed);
         s.wal_merge_aborts += r.merge_aborts.load(std::memory_order_relaxed);
         s.wal_merge_entries += r.merge_entries.load(std::memory_order_relaxed);
         s.wal_merge_range_tombstones +=
            r.merge_range_tombstones.load(std::memory_order_relaxed);
         s.wal_merge_wall_ns += r.merge_wall_ns.load(std::memory_order_relaxed);
         s.wal_merge_commit_ns += r.merge_commit_ns.load(std::memory_order_relaxed);
         s.wal_merge_cpu_ns += r.merge_cpu_ns.load(std::memory_order_relaxed);
         s.wal_max_throttle_sleep_ns = std::max<uint64_t>(
            s.wal_max_throttle_sleep_ns,
            r.throttle_sleep_ns.load(std::memory_order_relaxed));
      }
   }
   s.compact_pinned_threshold_mb =
       state._config.compact_pinned_unused_threshold_mb;
   s.compact_unpinned_threshold_mb =
       state._config.compact_unpinned_unused_threshold_mb;

   s.seg_file_bytes = file_size_or_zero(db_dir / "segs");
   s.total_segments = s.seg_file_bytes / sal::segment_size;

   const auto& seg_data = state._segment_data;
   for (uint64_t i = 0; i < s.total_segments; ++i)
   {
      sal::segment_number seg{static_cast<uint32_t>(i)};
      const uint32_t flags = seg_data.get_flags(seg);
      const uint64_t freed = seg_data.get_freed_space(seg);

      s.reclaimable_bytes += freed;
      if (flags == 0)
         ++s.zero_flag_segments;
      if (flags & seg_flag_active)
      {
         ++s.active_segments;
         s.active_reclaimable_bytes += freed;
      }
      if (flags & seg_flag_read_only)
         ++s.read_only_segments;
      if (flags & seg_flag_pinned)
         ++s.pinned_segments;
      if (flags & seg_flag_pending)
      {
         ++s.pending_segments;
         s.pending_reclaimable_bytes += freed;
      }
      if (flags & seg_flag_free)
      {
         ++s.free_segments;
         s.free_segment_bytes += sal::segment_size;
      }
      if (flags & seg_flag_queued)
         ++s.queued_segments;

      if (seg_data.may_compact(seg))
      {
         ++s.may_compact_segments;
         s.eligible_reclaimable_bytes += freed;
         if (seg_data.is_pinned(seg))
         {
            ++s.may_compact_pinned_segments;
            s.eligible_pinned_reclaimable_bytes += freed;
         }
         else
         {
            ++s.may_compact_unpinned_segments;
            s.eligible_unpinned_reclaimable_bytes += freed;
         }
      }
      else
      {
         s.blocked_reclaimable_bytes += freed;
      }
   }

   s.ready_pinned_depth = state._segment_provider.ready_pinned_segments.usage();
   s.ready_unpinned_depth = state._segment_provider.ready_unpinned_segments.usage();
   s.recycled_depth = state._read_lock_queue.recycled_queue_depth();
   s.recycled_capacity = state._read_lock_queue.recycled_queue_capacity();
   s.recycled_available_to_pop = state._read_lock_queue.available_to_pop();
   s.recycled_available_to_push = state._read_lock_queue.available_to_push();
   if (s.recycled_depth > s.recycled_available_to_pop)
      s.recycle_readlock_blocked = s.recycled_depth - s.recycled_available_to_pop;

   s.active_sessions = state._session_data.active_session_count();
   const uint32_t max_session = state._session_data.session_capacity();
   for (uint32_t i = 0; i < max_session; ++i)
   {
      sal::allocator_session_number sn{i};
      s.session_bytes_written += state._session_data.total_bytes_written(sn);
      s.rcache_depth += state._session_data.rcache_queue(sn).usage();
      s.release_queue_depth += state._session_data.release_queue(sn).usage();
   }
   if (op_stats && op_stats->compatible())
   {
      s.session_ops_available = true;
      for (uint32_t op = 0; op < sal::mapped_memory::session_operation_count; ++op)
      {
         s.session_ops[op] = op_stats->total(
             static_cast<sal::mapped_memory::session_operation>(op));
      }
   }

   s.mlock_success_regions =
       state._segment_provider.successful_mlock_regions.load(std::memory_order_relaxed);
   s.mlock_fail_regions =
       state._segment_provider.failed_mlock_regions.load(std::memory_order_relaxed);
   s.munlock_success_regions =
       state._segment_provider.successful_munlock_regions.load(std::memory_order_relaxed);
   s.munlock_fail_regions =
       state._segment_provider.failed_munlock_regions.load(std::memory_order_relaxed);
   s.cb_mlock_success_bytes = ptr_header
                                  ? ptr_header->control_block_zone_mlock_success_bytes.load(
                                        std::memory_order_relaxed) +
                                        ptr_header->control_block_freelist_mlock_success_bytes.load(
                                            std::memory_order_relaxed)
                                  : 0;
   return s;
}

double byte_rate(uint64_t now, uint64_t prev, double seconds)
{
   if (seconds <= 0)
      return 0;
   if (now >= prev)
      return double(now - prev) / seconds;
   return -double(prev - now) / seconds;
}

uint64_t counter_delta(uint64_t now, uint64_t prev)
{
   return now >= prev ? now - prev : 0;
}

struct mfu_rate_sample
{
   double   seconds = 0;
   uint64_t promoted = 0;
   uint64_t policy_satisfied = 0;
   uint64_t cold_to_hot_bytes = 0;
   uint64_t hot_to_hot_bytes = 0;
   uint64_t young_hot_skipped_bytes = 0;
   uint64_t promoted_to_cold_bytes = 0;
   uint64_t pinned_copy_bytes = 0;
   uint64_t pinned_effective_bytes = 0;
};

struct mfu_rolling_rates
{
   uint64_t                    max_samples = 10;
   std::deque<mfu_rate_sample> samples;
   mfu_rate_sample             total;

   explicit mfu_rolling_rates(uint64_t window_samples = 10) : max_samples(window_samples) {}

   void push(const dashboard_snapshot& cur, const dashboard_snapshot& prev)
   {
      if (max_samples == 0)
         return;

      mfu_rate_sample s;
      s.seconds =
          std::chrono::duration<double>(cur.t - prev.t).count();
      if (s.seconds <= 0)
         return;

      s.promoted =
          counter_delta(cur.cache.promoted, prev.cache.promoted);
      s.policy_satisfied =
          counter_delta(cur.cache.policy_satisfied, prev.cache.policy_satisfied);
      s.cold_to_hot_bytes =
          counter_delta(cur.cache.cold_to_hot_bytes, prev.cache.cold_to_hot_bytes);
      s.hot_to_hot_bytes =
          counter_delta(cur.cache.hot_to_hot_bytes, prev.cache.hot_to_hot_bytes);
      s.young_hot_skipped_bytes =
          counter_delta(cur.cache.young_hot_skipped_bytes, prev.cache.young_hot_skipped_bytes);
      s.promoted_to_cold_bytes =
          counter_delta(cur.cache.promoted_to_cold_bytes, prev.cache.promoted_to_cold_bytes);
      s.pinned_copy_bytes = s.cold_to_hot_bytes + s.hot_to_hot_bytes;
      s.pinned_effective_bytes = s.pinned_copy_bytes + s.young_hot_skipped_bytes;

      add(s);
      samples.push_back(s);
      while (samples.size() > max_samples)
      {
         subtract(samples.front());
         samples.pop_front();
      }
   }

   double rate(uint64_t mfu_rate_sample::*field) const
   {
      if (total.seconds <= 0)
         return 0;
      return double(total.*field) / total.seconds;
   }

   uint64_t size() const { return samples.size(); }

 private:
   void add(const mfu_rate_sample& s)
   {
      total.seconds += s.seconds;
      total.promoted += s.promoted;
      total.policy_satisfied += s.policy_satisfied;
      total.cold_to_hot_bytes += s.cold_to_hot_bytes;
      total.hot_to_hot_bytes += s.hot_to_hot_bytes;
      total.young_hot_skipped_bytes += s.young_hot_skipped_bytes;
      total.promoted_to_cold_bytes += s.promoted_to_cold_bytes;
      total.pinned_copy_bytes += s.pinned_copy_bytes;
      total.pinned_effective_bytes += s.pinned_effective_bytes;
   }

   void subtract(const mfu_rate_sample& s)
   {
      total.seconds -= s.seconds;
      total.promoted -= s.promoted;
      total.policy_satisfied -= s.policy_satisfied;
      total.cold_to_hot_bytes -= s.cold_to_hot_bytes;
      total.hot_to_hot_bytes -= s.hot_to_hot_bytes;
      total.young_hot_skipped_bytes -= s.young_hot_skipped_bytes;
      total.promoted_to_cold_bytes -= s.promoted_to_cold_bytes;
      total.pinned_copy_bytes -= s.pinned_copy_bytes;
      total.pinned_effective_bytes -= s.pinned_effective_bytes;
   }
};

struct session_op_rate_sample
{
   double seconds = 0;
   std::array<uint64_t, sal::mapped_memory::session_operation_count> ops{};
};

struct session_op_rolling_rates
{
   uint64_t                    max_samples = 10;
   std::deque<session_op_rate_sample> samples;
   session_op_rate_sample      total;

   explicit session_op_rolling_rates(uint64_t window_samples = 10)
       : max_samples(window_samples)
   {
   }

   void push(const dashboard_snapshot& cur, const dashboard_snapshot& prev)
   {
      if (max_samples == 0 || !cur.session_ops_available || !prev.session_ops_available)
         return;

      session_op_rate_sample s;
      s.seconds = std::chrono::duration<double>(cur.t - prev.t).count();
      if (s.seconds <= 0)
         return;

      for (uint32_t op = 0; op < sal::mapped_memory::session_operation_count; ++op)
         s.ops[op] = counter_delta(cur.session_ops[op], prev.session_ops[op]);

      add(s);
      samples.push_back(s);
      while (samples.size() > max_samples)
      {
         subtract(samples.front());
         samples.pop_front();
      }
   }

   double rate(uint32_t op) const
   {
      if (total.seconds <= 0)
         return 0;
      return double(total.ops[op]) / total.seconds;
   }

   uint64_t size() const { return samples.size(); }

 private:
   void add(const session_op_rate_sample& s)
   {
      total.seconds += s.seconds;
      for (uint32_t op = 0; op < sal::mapped_memory::session_operation_count; ++op)
         total.ops[op] += s.ops[op];
   }

   void subtract(const session_op_rate_sample& s)
   {
      total.seconds -= s.seconds;
      for (uint32_t op = 0; op < sal::mapped_memory::session_operation_count; ++op)
         total.ops[op] -= s.ops[op];
   }
};

void print_dashboard_row(std::string_view label,
                         const std::string& value,
                         const std::string& detail = {})
{
   std::cout << "  " << std::setw(27) << std::left << label
             << std::setw(17) << std::left << value
             << detail << "\n";
}

std::string rolling_rate_detail(const mfu_rolling_rates& rates,
                                uint64_t mfu_rate_sample::*field,
                                double instant_rate)
{
   if (rates.max_samples == 0)
      return format_rate(instant_rate);
   if (rates.size() == 0)
      return "warming";
   return "avg " + format_rate(rates.rate(field)) + "  inst " + format_rate(instant_rate);
}

std::string format_ops_rate(double ops_per_sec)
{
   if (ops_per_sec <= 0)
      return "0/s";
   if (ops_per_sec >= 100)
      return format_number(static_cast<uint64_t>(ops_per_sec + 0.5)) + "/s";
   std::ostringstream out;
   out << std::fixed << std::setprecision(1) << ops_per_sec << "/s";
   return out.str();
}

uint64_t total_session_ops(const dashboard_snapshot& s)
{
   if (!s.session_ops_available)
      return 0;
   uint64_t total = 0;
   for (uint32_t op = 0; op < sal::mapped_memory::session_operation_count; ++op)
      total += s.session_ops[op];
   return total;
}

double total_session_op_rate(const session_op_rolling_rates& rates)
{
   double total = 0;
   for (uint32_t op = 0; op < sal::mapped_memory::session_operation_count; ++op)
      total += rates.rate(op);
   return total;
}

std::string rolling_op_rate_detail(const session_op_rolling_rates& rates,
                                   uint32_t                        op,
                                   double                          instant_rate)
{
   if (rates.max_samples == 0)
      return format_ops_rate(instant_rate);
   if (rates.size() == 0)
      return "warming";
   return "avg " + format_ops_rate(rates.rate(op)) + "  inst " +
          format_ops_rate(instant_rate);
}

void print_session_op_header()
{
   std::cout << "  " << std::setw(27) << std::left << "operation"
             << std::setw(17) << std::right << "total"
             << std::setw(9) << std::right << "% ops"
             << std::setw(16) << std::right << "avg/s"
             << std::setw(16) << std::right << "inst/s"
             << "\n";
}

void print_session_op_row(std::string_view label,
                          uint64_t         total,
                          double           percent,
                          double           avg_rate,
                          double           instant_rate)
{
   std::cout << "  " << std::setw(27) << std::left << label
             << std::setw(17) << std::right << format_number(total)
             << std::setw(9) << std::right << format_ratio(percent, 100.0)
             << std::setw(16) << std::right << format_ops_rate(avg_rate)
             << std::setw(16) << std::right << format_ops_rate(instant_rate)
             << "\n";
}

std::string format_range(uint64_t low, uint64_t high)
{
   if (low == 0 && high == 0)
      return "n/a";
   if (low == high)
      return format_number(low);
   return format_number(low) + ".." + format_number(high);
}

std::string format_ptr_address(uint32_t address)
{
   if (address == *sal::null_ptr_address)
      return "null";

   std::ostringstream out;
   out << "0x" << std::hex << std::setw(8) << std::setfill('0') << address;
   return out.str();
}

void print_psitricorder_terms()
{
   std::cout
       << "psitricorder dashboard terms\n\n"
       << "Usage:\n"
       << "  psitricorder dashboard <db-dir> --interval-ms 1000\n"
       << "  psitricorder dashboard <db-dir> --rate-samples 60\n"
       << "  psitricorder --dashboard --db-dir <db-dir>\n"
       << "  psitricorder --explain\n\n"
       << "Cache policy:\n"
       << "  difficulty        The MFU lottery threshold. Higher difficulty means fewer\n"
       << "                    read traversals mark objects for promotion. Odds are shown\n"
       << "                    as roughly 1/N successful lottery hits.\n"
       << "  cache window      Time horizon for the MFU controller. The target promotion\n"
       << "                    rate is cache-size / window. This is a write-amplification\n"
       << "                    budget, not a guarantee; if difficulty reaches 1/1 and\n"
       << "                    policy/s stays lower, the workload is not producing enough\n"
       << "                    cost-normalized repeated reads to spend the budget.\n"
       << "  policy bytes      Bytes that satisfied cache policy: actual promoted bytes\n"
       << "                    plus young-HOT bytes intentionally skipped. This lets the\n"
       << "                    controller avoid needless HOT->HOT churn.\n"
       << "  size policy       Larger objects spend more cache budget, so the read lottery\n"
       << "                    is cost-normalized by cacheline count. A large object must\n"
       << "                    be proportionally hotter to displace several small ones.\n"
       << "  displayed rates   Dashboard rates are viewer-side rolling averages by default.\n"
       << "                    The default is 60 samples, roughly one minute at the\n"
       << "                    default 1s interval. Use --rate-samples to tune it.\n"
       << "  COLD -> HOT       Unpinned objects copied into pinned memory.\n"
       << "  HOT refresh       Pinned objects copied forward to remain in the hot region.\n"
       << "  young HOT skipped Pinned objects seen by the promoter but young enough to\n"
       << "                    leave in place.\n"
       << "  promoted to cold  Promotion requests that were copied to unpinned memory,\n"
       << "                    expected to stay near zero in a healthy hot allocation path.\n"
       << "  pinned copy result\n"
       << "                    COLD->HOT plus HOT refresh bytes physically copied into\n"
       << "                    pinned memory.\n"
       << "  pinned effective  Pinned copy result plus young-HOT skipped bytes, showing\n"
       << "                    copied-or-retained pinned-cache benefit.\n\n"
       << "Session operation counters:\n"
       << "  Session Ops       Per-write-session counters stored in session_ops.bin.\n"
       << "                    Each session has its own hardware-cacheline-aligned block\n"
       << "                    (128 bytes on Apple ARM64), and the dashboard shows the\n"
       << "                    aggregate total plus rolling op/sec by operation type.\n"
       << "                    Live writers from older builds show this as unavailable\n"
       << "                    until restarted.\n\n"
       << "Compaction policy:\n"
       << "  hot threshold     Free-space threshold for pinned segments. It is lower\n"
       << "                    because dead bytes inside mlock'd memory waste scarce RAM.\n"
       << "  cold threshold    Free-space threshold for unpinned segments. It is higher\n"
       << "                    because cold disk space is cheaper than write amplification.\n"
       << "  reclaimable       Dead/free bytes known inside segments.\n"
       << "  eligible          Reclaimable bytes in segments that currently meet the\n"
       << "                    configured compaction threshold.\n"
       << "  blocked           Reclaimable bytes not currently compactable, commonly\n"
       << "                    because the segment is active or still in read-lock delay.\n\n"
       << "Version policy:\n"
       << "  top version       Current global MVCC commit/version counter from dbfile.bin.\n"
       << "  epoch base        Current version maintenance floor. Paths older than this\n"
       << "                    floor are candidates for COW maintenance on write.\n"
       << "  active root versions\n"
       << "                    Distinct committed top-root versions visible from the root\n"
       << "                    table. This is a passive root-slot view, not a full subtree\n"
       << "                    or refcount audit.\n"
       << "  active root address\n"
       << "                    Current top-root node address from the roots mmap. In the\n"
       << "                    single-root LMDBX layout this should remain stable unless\n"
       << "                    root replacement occurs, such as root split/merge,\n"
       << "                    epoch-forced uniqueness, or unintended COW-to-root.\n";
}

void render_dashboard(const fs::path& db_dir,
                      const dashboard_snapshot& cur,
                      const dashboard_snapshot& prev,
                      const mfu_rolling_rates& rates,
                      const session_op_rolling_rates& op_rates,
                      uint64_t sample_index)
{
   const double interval =
       std::chrono::duration<double>(cur.t - prev.t).count();
   const uint64_t pinned_useful =
       cur.cache.cold_to_hot_bytes + cur.cache.hot_to_hot_bytes;
   const uint64_t pinned_effective =
       pinned_useful + cur.cache.young_hot_skipped_bytes;
   const uint64_t recovered_full_segment_bytes =
       (cur.free_segments + cur.ready_pinned_depth + cur.ready_unpinned_depth +
        cur.recycled_depth) * sal::segment_size;
   const uint64_t recycle_capacity_bytes = cur.recycled_capacity * sal::segment_size;
   const double promoted_rate =
       byte_rate(cur.cache.promoted, prev.cache.promoted, interval);
   const double policy_rate =
       byte_rate(cur.cache.policy_satisfied, prev.cache.policy_satisfied, interval);
   const double cold_to_hot_rate =
       byte_rate(cur.cache.cold_to_hot_bytes, prev.cache.cold_to_hot_bytes, interval);
   const double hot_to_hot_rate =
       byte_rate(cur.cache.hot_to_hot_bytes, prev.cache.hot_to_hot_bytes, interval);
   const double young_hot_skip_rate =
       byte_rate(cur.cache.young_hot_skipped_bytes,
                 prev.cache.young_hot_skipped_bytes,
                 interval);
   const double promoted_to_cold_rate =
       byte_rate(cur.cache.promoted_to_cold_bytes,
                 prev.cache.promoted_to_cold_bytes,
                 interval);

   std::cout << "\033[2J\033[H";
   std::cout << "psitricorder dashboard  sample=" << sample_index
             << "  db=" << fs::canonical(db_dir) << "\n";
   std::cout << "passive mmap view; safe while the DB is live. Run --explain for terms.\n\n";

   std::cout << "Policy / Versions\n";
   if (cur.dbfile_metadata_available)
   {
      print_dashboard_row("top version",
                          format_number(cur.top_version),
                          "epoch " + format_number(cur.epoch_base) +
                              " / interval " + format_number(cur.epoch_interval));
      print_dashboard_row("shutdown/refcounts",
                          cur.clean_shutdown ? "clean" : "live/unclean",
                          cur.ref_counts_stale ? "refcounts stale" : "refcounts ok");
   }
   else
   {
      print_dashboard_row("top version", "n/a", "dbfile.bin unavailable");
   }
   if (cur.root_metadata_available)
   {
      print_dashboard_row("active root versions",
                          format_number(cur.active_root_versions),
                          "range " +
                              format_range(cur.oldest_root_version,
                                           cur.newest_root_version));
      print_dashboard_row("root version records",
                          format_number(cur.roots_with_version) + " ok",
                          format_number(cur.roots_without_version) + " missing, " +
                              format_number(cur.populated_roots) + " populated roots");
      if (cur.active_root_address_available)
      {
         std::string detail = "root " + format_number(cur.active_root_index) +
                              ", ver-cb " +
                              format_ptr_address(cur.active_root_version_address) +
                              ", version " +
                              format_number(cur.active_root_record_version);
         if (prev.active_root_address_available &&
             cur.active_root_index == prev.active_root_index)
         {
            if (cur.active_root_address == prev.active_root_address &&
                cur.active_root_version_address == prev.active_root_version_address)
               detail += ", stable";
            else
               detail += ", changed from " +
                         format_ptr_address(prev.active_root_address) + "/" +
                         format_ptr_address(prev.active_root_version_address);
         }
         if (cur.populated_roots != 1)
            detail += ", first of " + format_number(cur.populated_roots) +
                      " populated roots";
         print_dashboard_row("active root address",
                             format_ptr_address(cur.active_root_address),
                             detail);
      }
   }
   else
   {
      print_dashboard_row("active root versions", "n/a", "roots unavailable");
   }
   if (!cur.version_error.empty())
      print_dashboard_row("version metadata", "partial", cur.version_error);
   print_dashboard_row("difficulty",
                       "1/" + format_number(difficulty_attempts(cur.cache.difficulty)),
                       "raw " + format_number(cur.cache.difficulty));
   print_dashboard_row("cache budget",
                       format_bytes(cur.cache.cache_bytes),
                       "window " + std::to_string(cur.cache.window_sec) +
                           "s, target " + format_rate(double(cur.cache.target_bps)));
   print_dashboard_row("compact thresholds",
                       "hot " + format_number(cur.compact_pinned_threshold_mb) + " MB",
                       "cold " + format_number(cur.compact_unpinned_threshold_mb) + " MB");

   std::cout << "\nMFU Cache";
   if (rates.max_samples != 0)
      std::cout << " (rate avg " << rates.size() << "/" << rates.max_samples << " samples)";
   std::cout << "\n";
   print_dashboard_row("cache window",
                       std::to_string(cur.cache.window_sec) + "s",
                       "target " + format_rate(double(cur.cache.target_bps)));
   print_dashboard_row("promoted total",
                       format_bytes(cur.cache.promoted),
                       rolling_rate_detail(rates,
                                           &mfu_rate_sample::promoted,
                                           promoted_rate));
   print_dashboard_row("policy bytes",
                       format_bytes(cur.cache.policy_satisfied),
                       rolling_rate_detail(rates,
                                           &mfu_rate_sample::policy_satisfied,
                                           policy_rate));
   print_dashboard_row("COLD -> HOT",
                       format_bytes(cur.cache.cold_to_hot_bytes),
                       rolling_rate_detail(rates,
                                           &mfu_rate_sample::cold_to_hot_bytes,
                                           cold_to_hot_rate) +
                           "  " + format_number(cur.cache.cold_to_hot_promotions) + " objs");
   print_dashboard_row("HOT refresh",
                       format_bytes(cur.cache.hot_to_hot_bytes),
                       rolling_rate_detail(rates,
                                           &mfu_rate_sample::hot_to_hot_bytes,
                                           hot_to_hot_rate) +
                           "  " + format_number(cur.cache.hot_to_hot_promotions) + " objs");
   print_dashboard_row("young HOT skipped",
                       format_bytes(cur.cache.young_hot_skipped_bytes),
                       rolling_rate_detail(rates,
                                           &mfu_rate_sample::young_hot_skipped_bytes,
                                           young_hot_skip_rate) +
                           "  " + format_number(cur.cache.young_hot_skips) + " objs");
   print_dashboard_row("promoted to cold",
                       format_bytes(cur.cache.promoted_to_cold_bytes),
                       rolling_rate_detail(rates,
                                           &mfu_rate_sample::promoted_to_cold_bytes,
                                           promoted_to_cold_rate) +
                           "  " +
                           format_ratio(double(cur.cache.promoted_to_cold_bytes),
                                        double(cur.cache.promoted)));
   print_dashboard_row("pinned copy result",
                       format_bytes(pinned_useful),
                       rolling_rate_detail(rates,
                                           &mfu_rate_sample::pinned_copy_bytes,
                                           cold_to_hot_rate + hot_to_hot_rate) +
                           "  " + format_ratio(double(pinned_useful),
                                               double(cur.cache.promoted)));
   print_dashboard_row("pinned effective",
                       format_bytes(pinned_effective),
                       rolling_rate_detail(rates,
                                           &mfu_rate_sample::pinned_effective_bytes,
                                           cold_to_hot_rate + hot_to_hot_rate +
                                               young_hot_skip_rate));

   std::cout << "\nDWAL / WAL\n";
   if (!cur.wal_status_available)
   {
      print_dashboard_row("wal status", "n/a", "wal/status.bin unavailable");
   }
   else
   {
      print_dashboard_row("active roots",
                          format_number(cur.wal_active_roots),
                          format_number(cur.wal_merging_roots) + " merging");
      print_dashboard_row("RW layer",
                          format_number(cur.wal_rw_entries) + " entries",
                          format_bytes(cur.wal_rw_arena_bytes) + " arena");
      print_dashboard_row("RO layer",
                          format_number(cur.wal_ro_entries) + " entries",
                          format_bytes(cur.wal_ro_arena_bytes) + " arena");
      print_dashboard_row("WAL rw bytes",
                          format_bytes(cur.wal_rw_logical_bytes),
                          "file " + format_bytes(cur.wal_rw_file_bytes) +
                              ", buffered " + format_bytes(cur.wal_rw_buffered_bytes) +
                              ", " +
                              format_signed_rate(byte_rate(cur.wal_rw_logical_bytes,
                                                           prev.wal_rw_logical_bytes,
                                                           interval)));
      print_dashboard_row("WAL ro bytes",
                          format_bytes(cur.wal_ro_file_bytes),
                          "frozen WAL waiting for merge");
      print_dashboard_row("WAL entries/ops",
                          format_number(cur.wal_entries) + " / " +
                              format_number(cur.wal_ops),
                          format_ops_rate(interval > 0 ? double(counter_delta(cur.wal_ops,
                                                                               prev.wal_ops)) /
                                                           interval
                                                       : 0));
      print_dashboard_row("WAL op mix",
                          "up " + format_number(cur.wal_upsert_data_ops),
                          "subtree " + format_number(cur.wal_upsert_subtree_ops) +
                              ", rm " + format_number(cur.wal_remove_ops) +
                              ", range " + format_number(cur.wal_remove_range_ops));
      print_dashboard_row("WAL payload bytes",
                          format_bytes(cur.wal_committed_entry_bytes),
                          "keys " + format_bytes(cur.wal_key_bytes) +
                              ", values " + format_bytes(cur.wal_value_bytes));
      print_dashboard_row("WAL writes",
                          format_bytes(cur.wal_write_bytes),
                          format_number(cur.wal_write_calls) + " pwrite batches, " +
                              format_number(cur.wal_flush_calls) + " flushes");
      print_dashboard_row("WAL fsync/fullsync",
                          format_number(cur.wal_fsync_calls) + " / " +
                              format_number(cur.wal_fullsync_calls),
                          "clean closes " + format_number(cur.wal_clean_closes) +
                              ", discards " + format_number(cur.wal_discarded_entries));
      print_dashboard_row("WAL swaps/merges",
                          format_number(cur.wal_swaps) + " / " +
                              format_number(cur.wal_merge_completions),
                          "requests " + format_number(cur.wal_merge_requests) +
                              ", aborts " + format_number(cur.wal_merge_aborts));
      print_dashboard_row("merge entries",
                          format_number(cur.wal_merge_entries),
                          "ranges " + format_number(cur.wal_merge_range_tombstones));
      print_dashboard_row("merge time",
                          format_rate(byte_rate(cur.wal_merge_entries,
                                                prev.wal_merge_entries,
                                                interval)) + " entries",
                          "wall " + format_signed_rate(byte_rate(cur.wal_merge_wall_ns,
                                                                  prev.wal_merge_wall_ns,
                                                                  interval)) +
                              "ns/s, commit " +
                              format_signed_rate(byte_rate(cur.wal_merge_commit_ns,
                                                           prev.wal_merge_commit_ns,
                                                           interval)) +
                              "ns/s");
      print_dashboard_row("max throttle sleep",
                          format_number(cur.wal_max_throttle_sleep_ns) + " ns");
   }

   std::cout << "\nCompactor / Reclamation\n";
   print_dashboard_row("reclaimable bytes",
                       format_bytes(cur.reclaimable_bytes),
                       format_signed_rate(byte_rate(cur.reclaimable_bytes,
                                                    prev.reclaimable_bytes,
                                                    interval)));
   print_dashboard_row("eligible reclaimable",
                       format_bytes(cur.eligible_reclaimable_bytes),
                       format_signed_rate(byte_rate(cur.eligible_reclaimable_bytes,
                                                    prev.eligible_reclaimable_bytes,
                                                    interval)) +
                           "  ~" +
                           std::to_string(cur.eligible_reclaimable_bytes / sal::segment_size) +
                           " segments");
   print_dashboard_row("eligible pinned",
                       format_bytes(cur.eligible_pinned_reclaimable_bytes),
                       format_number(cur.may_compact_pinned_segments) + " segs");
   print_dashboard_row("eligible unpinned",
                       format_bytes(cur.eligible_unpinned_reclaimable_bytes),
                       format_number(cur.may_compact_unpinned_segments) + " segs");
   print_dashboard_row("blocked reclaimable",
                       format_bytes(cur.blocked_reclaimable_bytes),
                       "active " + format_bytes(cur.active_reclaimable_bytes) +
                           ", pending " + format_bytes(cur.pending_reclaimable_bytes));
   print_dashboard_row("full segments recovered",
                       format_bytes(recovered_full_segment_bytes),
                       "free/ready/recycled");
   print_dashboard_row("recycled queue",
                       std::to_string(cur.recycled_depth) + " / " +
                           std::to_string(cur.recycled_capacity),
                       format_bytes(cur.recycled_depth * sal::segment_size) + " of " +
                           format_bytes(recycle_capacity_bytes));
   print_dashboard_row("read-lock blocked",
                       std::to_string(cur.recycle_readlock_blocked),
                       "available-to-pop " + std::to_string(cur.recycled_available_to_pop));
   print_dashboard_row("ready pinned/unpinned",
                       std::to_string(cur.ready_pinned_depth) + " / " +
                           std::to_string(cur.ready_unpinned_depth),
                       "provider queues");

   std::cout << "\nSegments / Sessions\n";
   print_dashboard_row("segment file",
                       format_bytes(cur.seg_file_bytes),
                       format_number(cur.total_segments) + " segments");
   print_dashboard_row("states active/ro/pending",
                       format_number(cur.active_segments) + " / " +
                           format_number(cur.read_only_segments) + " / " +
                           format_number(cur.pending_segments));
   print_dashboard_row("states free/queued/zero",
                       format_number(cur.free_segments) + " / " +
                           format_number(cur.queued_segments) + " / " +
                           format_number(cur.zero_flag_segments));
   print_dashboard_row("pinned segments",
                       format_number(cur.pinned_segments),
                       format_bytes(cur.pinned_segments * sal::segment_size));
   print_dashboard_row("active sessions",
                       format_number(cur.active_sessions),
                       "rcache " + format_number(cur.rcache_depth) +
                           ", release " + format_number(cur.release_queue_depth));
   print_dashboard_row("session synced bytes",
                       format_bytes(cur.session_bytes_written),
                       format_signed_rate(byte_rate(cur.session_bytes_written,
                                                    prev.session_bytes_written,
                                                    interval)));
   print_dashboard_row("cache copy / synced",
                       format_ratio(double(cur.cache.promoted),
                                    double(cur.session_bytes_written)),
                       "rough cache write amp");
   print_dashboard_row("mlock ok/fail",
                       format_number(cur.mlock_success_regions) + " / " +
                           format_number(cur.mlock_fail_regions),
                       "munlock " + format_number(cur.munlock_success_regions) + " / " +
                           format_number(cur.munlock_fail_regions));
   print_dashboard_row("control-block pinned",
                       format_bytes(cur.cb_mlock_success_bytes));

   std::cout << "\nSession Ops";
   if (op_rates.max_samples != 0)
      std::cout << " (rate avg " << op_rates.size() << "/" << op_rates.max_samples
                << " samples)";
   std::cout << "\n";
   if (!cur.session_ops_available)
   {
      print_dashboard_row("op counters", "n/a", "session_ops.bin unavailable");
   }
   else
   {
      bool any_ops = false;
      const uint64_t total_ops      = total_session_ops(cur);
      const uint64_t prev_total_ops = total_session_ops(prev);
      const double instant_total =
          interval > 0 ? double(counter_delta(total_ops, prev_total_ops)) / interval : 0;
      const double avg_total = total_session_op_rate(op_rates);

      print_session_op_header();
      print_session_op_row("total ops",
                           total_ops,
                           total_ops > 0 ? 100.0 : 0.0,
                           avg_total,
                           instant_total);
      for (uint32_t op = 0; op < sal::mapped_memory::session_operation_count; ++op)
      {
         const uint64_t total = cur.session_ops[op];
         const double instant =
             interval > 0 ? double(counter_delta(cur.session_ops[op], prev.session_ops[op])) /
                                interval
                          : 0;
         const double avg = op_rates.rate(op);
         if (total == 0 && instant == 0 && avg == 0)
            continue;

         any_ops = true;
         print_session_op_row(sal::mapped_memory::session_operation_names[op],
                              total,
                              total_ops > 0 ? 100.0 * double(total) / double(total_ops) : 0.0,
                              avg,
                              instant);
      }
      if (!any_ops)
         print_dashboard_row("op counters", "0", "no observed operation types yet");
   }

   std::cout << "\nNotes: compactor lifetime bytes are derived from current shared state. "
             << "True cumulative segment-compact source/live/recovered bytes need SAL counters "
             << "in the writer process.\n";
   std::cout.flush();
}

int cmd_dashboard(const fs::path& dir,
                  uint64_t       interval_ms,
                  uint64_t       samples,
                  uint64_t       rate_samples)
{
   if (interval_ms == 0)
      interval_ms = 1000;

   fs::path db_dir = dir;
   if (!fs::exists(db_dir / "header") && fs::exists(db_dir / "chaindata" / "header"))
      db_dir = db_dir / "chaindata";

   passive_header_mapping mapping(db_dir / "header");
   const auto*            state = mapping.state();
   std::unique_ptr<passive_ptr_header_mapping> ptr_mapping;
   if (fs::exists(db_dir / "ptrs" / "header.bin"))
      ptr_mapping = std::make_unique<passive_ptr_header_mapping>(db_dir / "ptrs" / "header.bin");
   const auto* ptr_header = ptr_mapping ? ptr_mapping->header() : nullptr;
   std::unique_ptr<passive_session_ops_mapping> op_mapping;
   if (fs::exists(db_dir / "session_ops.bin"))
      op_mapping = std::make_unique<passive_session_ops_mapping>(db_dir / "session_ops.bin");
   const auto* op_stats = op_mapping ? op_mapping->stats() : nullptr;
   std::unique_ptr<passive_wal_status_mapping> wal_mapping;
   if (fs::exists(db_dir / "wal" / "status.bin"))
      wal_mapping = std::make_unique<passive_wal_status_mapping>(db_dir / "wal" / "status.bin");
   const auto* wal_status = wal_mapping ? wal_mapping->file() : nullptr;

   auto prev = read_dashboard_snapshot(db_dir, *state, ptr_header, op_stats, wal_status);
   mfu_rolling_rates rates(rate_samples);
   session_op_rolling_rates op_rates(rate_samples);
   for (uint64_t i = 0; samples == 0 || i < samples; ++i)
   {
      if (i != 0)
         std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
      auto cur = read_dashboard_snapshot(db_dir, *state, ptr_header, op_stats, wal_status);
      if (i != 0)
      {
         rates.push(cur, prev);
         op_rates.push(cur, prev);
      }
      render_dashboard(db_dir, cur, prev, rates, op_rates, i);
      prev = cur;
   }
   return 0;
}

int hex_digit(char c)
{
   if (c >= '0' && c <= '9')
      return c - '0';
   if (c >= 'a' && c <= 'f')
      return c - 'a' + 10;
   if (c >= 'A' && c <= 'F')
      return c - 'A' + 10;
   return -1;
}

std::string parse_hex_bytes(std::string_view input)
{
   std::string clean;
   clean.reserve(input.size());
   for (char c : input)
   {
      if (std::isspace(static_cast<unsigned char>(c)) || c == '_' || c == '-')
         continue;
      clean.push_back(c);
   }

   if ((clean.size() % 2) != 0)
      throw std::runtime_error("hex key must contain an even number of digits");

   std::string out;
   out.reserve(clean.size() / 2);
   for (size_t i = 0; i < clean.size(); i += 2)
   {
      int hi = hex_digit(clean[i]);
      int lo = hex_digit(clean[i + 1]);
      if (hi < 0 || lo < 0)
         throw std::runtime_error("hex key contains a non-hex character");
      out.push_back(static_cast<char>((hi << 4) | lo));
   }
   return out;
}

std::string bytes_to_hex(std::string_view bytes)
{
   static constexpr char digits[] = "0123456789abcdef";
   std::string out;
   out.reserve(bytes.size() * 2);
   for (unsigned char c : bytes)
   {
      out.push_back(digits[c >> 4]);
      out.push_back(digits[c & 0x0f]);
   }
   return out;
}

std::string range_bound_hex(const std::optional<std::string>& bound)
{
   return bound ? bytes_to_hex(*bound) : std::string{};
}

std::optional<std::string> first_byte_boundary(uint32_t boundary)
{
   if (boundary == 0 || boundary >= 256)
      return std::nullopt;
   return std::string(1, static_cast<char>(boundary));
}

std::optional<std::string> max_lower_bound(const std::optional<std::string>& lhs,
                                           const std::optional<std::string>& rhs)
{
   if (!lhs)
      return rhs;
   if (!rhs)
      return lhs;
   return lhs->compare(*rhs) >= 0 ? lhs : rhs;
}

std::optional<std::string> min_upper_bound(const std::optional<std::string>& lhs,
                                           const std::optional<std::string>& rhs)
{
   if (!lhs)
      return rhs;
   if (!rhs)
      return lhs;
   return lhs->compare(*rhs) <= 0 ? lhs : rhs;
}

bool empty_key_range(const std::optional<std::string>& lower,
                     const std::optional<std::string>& upper)
{
   return lower && upper && upper->compare(*lower) <= 0;
}

recovery_mode parse_recovery(const std::string& s)
{
   if (s == "none")
      return recovery_mode::none;
   if (s == "deferred")
      return recovery_mode::deferred_cleanup;
   if (s == "app_crash")
      return recovery_mode::app_crash;
   if (s == "power_loss")
      return recovery_mode::power_loss;
   if (s == "full_verify")
      return recovery_mode::full_verify;
   throw std::runtime_error("unknown recovery mode: " + s +
                            " (valid: none, deferred, app_crash, power_loss, full_verify)");
}

// ── commands ─────────────────────────────────────────────────────────────────

void cmd_info(database& db, const fs::path& dir)
{
   std::cout << "Database: " << fs::canonical(dir) << "\n\n";

   // File sizes
   std::cout << "── Files ──\n";
   auto segs_size   = file_size_or_zero(dir / "segs");
   auto header_size = file_size_or_zero(dir / "header");
   auto roots_size  = file_size_or_zero(dir / "roots");
   auto ptrs_size   = dir_size(dir / "ptrs");
   auto dbfile_size = file_size_or_zero(dir / "dbfile.bin");
   auto total_size  = segs_size + header_size + roots_size + ptrs_size + dbfile_size;

   std::cout << std::left;
   std::cout << "  " << std::setw(20) << "segs" << format_bytes(segs_size) << "\n";
   std::cout << "  " << std::setw(20) << "header" << format_bytes(header_size) << "\n";
   std::cout << "  " << std::setw(20) << "roots" << format_bytes(roots_size) << "\n";
   std::cout << "  " << std::setw(20) << "ptrs/" << format_bytes(ptrs_size) << "\n";
   std::cout << "  " << std::setw(20) << "dbfile.bin" << format_bytes(dbfile_size) << "\n";
   std::cout << "  " << std::setw(20) << "TOTAL" << format_bytes(total_size) << "\n";

   // Segment summary from dump
   auto dump = db.dump();
   std::cout << "\n── Segments ──\n";

   uint64_t used_segments = 0;
   uint64_t total_alloc   = 0;
   uint64_t total_freed   = 0;
   for (auto& seg : dump.segments)
   {
      if (!seg.is_free && seg.alloc_pos > 0)
      {
         ++used_segments;
         total_alloc += seg.alloc_pos;
         total_freed += seg.freed_bytes;
      }
   }

   std::cout << "  " << std::setw(20) << "Total segments" << dump.total_segments << "\n";
   std::cout << "  " << std::setw(20) << "Used segments" << used_segments << "\n";
   std::cout << "  " << std::setw(20) << "Allocated" << format_bytes(total_alloc) << "\n";
   std::cout << "  " << std::setw(20) << "Freed" << format_bytes(total_freed) << "\n";
   std::cout << "  " << std::setw(20) << "Live (est.)" << format_bytes(total_alloc - total_freed)
             << "\n";
   std::cout << "  " << std::setw(20) << "Valid objects" << dump.total_read_nodes << "\n";
   std::cout << "  " << std::setw(20) << "Valid bytes" << format_bytes(dump.total_read_bytes)
             << "\n";

   std::cout << "\n── MFU Cache ──\n";
   const uint64_t max_difficulty = ~uint64_t{0};
   const uint64_t difficulty_gap = max_difficulty - dump.cache_difficulty;
   std::cout << "  " << std::setw(20) << "Pinned segments" << dump.mlocked_segments_count
             << "\n";
   std::cout << "  " << std::setw(20) << "Pinned capacity"
             << format_bytes(uint64_t(dump.mlocked_segments_count) * sal::segment_size) << "\n";
   std::cout << "  " << std::setw(20) << "mlock success"
             << dump.successful_mlock_regions << " regions, "
             << format_bytes(dump.successful_mlock_regions * sal::segment_size) << "\n";
   if (dump.failed_mlock_regions != 0)
      std::cout << "  " << std::setw(20) << "mlock failed"
                << dump.failed_mlock_regions << " regions\n";
   std::cout << "  " << std::setw(20) << "munlock success"
             << dump.successful_munlock_regions << " regions\n";
   if (dump.failed_munlock_regions != 0)
      std::cout << "  " << std::setw(20) << "munlock failed"
                << dump.failed_munlock_regions << " regions\n";
   std::cout << "  " << std::setw(20) << "Difficulty" << dump.cache_difficulty;
   if (difficulty_gap > 0)
      std::cout << " (about 1 in " << (max_difficulty / difficulty_gap) << ")";
   std::cout << "\n";
   std::cout << "  " << std::setw(20) << "Policy bytes"
             << format_bytes(dump.cache_policy_satisfied_bytes) << "\n";
   std::cout << "  " << std::setw(20) << "Promoted total"
             << format_bytes(dump.total_promoted_bytes) << "\n";
   std::cout << "  " << std::setw(20) << "COLD->HOT"
             << dump.cache_cold_to_hot_promotions << " objects, "
             << format_bytes(dump.cache_cold_to_hot_promoted_bytes) << "\n";
   std::cout << "  " << std::setw(20) << "HOT refresh"
             << dump.cache_hot_to_hot_promotions << " objects, "
             << format_bytes(dump.cache_hot_to_hot_promoted_bytes) << "\n";
   std::cout << "  " << std::setw(20) << "Young HOT skip"
             << dump.cache_young_hot_skips << " objects, "
             << format_bytes(dump.cache_young_hot_skipped_bytes) << "\n";
   if (dump.cache_hot_to_hot_promotions != 0)
   {
      const double avg_pressure =
          double(dump.cache_hot_to_hot_demote_pressure_ppm) /
          double(dump.cache_hot_to_hot_promotions) / 10000.0;
      const double byte_avg_pressure =
          dump.cache_hot_to_hot_promoted_bytes == 0
              ? 0.0
              : double(dump.cache_hot_to_hot_byte_demote_pressure_ppm) /
                    double(dump.cache_hot_to_hot_promoted_bytes) / 10000.0;
      std::cout << "  " << std::setw(20) << "Refresh pressure"
                << std::fixed << std::setprecision(1) << avg_pressure
                << "% avg, " << byte_avg_pressure << "% byte-avg\n";
   }
   if (dump.cache_young_hot_skips != 0)
   {
      const double avg_pressure =
          double(dump.cache_young_hot_skip_demote_pressure_ppm) /
          double(dump.cache_young_hot_skips) / 10000.0;
      const double byte_avg_pressure =
          dump.cache_young_hot_skipped_bytes == 0
              ? 0.0
              : double(dump.cache_young_hot_skip_byte_demote_pressure_ppm) /
                    double(dump.cache_young_hot_skipped_bytes) / 10000.0;
      std::cout << "  " << std::setw(20) << "Skip pressure"
                << std::fixed << std::setprecision(1) << avg_pressure
                << "% avg, " << byte_avg_pressure << "% byte-avg\n";
   }
   if (dump.cache_promoted_to_cold_promotions != 0)
      std::cout << "  " << std::setw(20) << "Promoted to cold"
                << dump.cache_promoted_to_cold_promotions << " objects, "
                << format_bytes(dump.cache_promoted_to_cold_bytes) << "\n";
   std::cout << "  " << std::setw(20) << "Target rate"
             << format_bytes(dump.cache_target_promoted_bytes_per_s) << "/s\n";
   std::cout << "  " << std::setw(20) << "Recycled queue"
             << dump.recycled_queue_depth << " / " << dump.recycled_queue_capacity
             << " segments\n";

   std::cout << "\n── Control Blocks ──\n";
   std::cout << "  " << std::setw(20) << "Zones" << dump.control_block_zones << "\n";
   std::cout << "  " << std::setw(20) << "Capacity" << dump.control_block_capacity << "\n";
   std::cout << "  " << std::setw(20) << "Header mlock"
             << (dump.control_block_header_pinned ? "yes" : "no") << "\n";
   std::cout << "  " << std::setw(20) << "Zone mlock"
             << dump.control_block_zone_mlock_success_regions << " ok, "
             << dump.control_block_zone_mlock_failed_regions << " failed, "
             << dump.control_block_zone_mlock_skipped_regions << " skipped, "
             << format_bytes(dump.control_block_zone_mlock_success_bytes) << " pinned\n";
   std::cout << "  " << std::setw(20) << "Free-list mlock"
             << dump.control_block_freelist_mlock_success_regions << " ok, "
             << dump.control_block_freelist_mlock_failed_regions << " failed, "
             << dump.control_block_freelist_mlock_skipped_regions << " skipped, "
             << format_bytes(dump.control_block_freelist_mlock_success_bytes) << " pinned\n";

   // Root count
   auto ses        = db.start_read_session();
   int  root_count = 0;
   int  first_root = -1;
   int  last_root  = -1;
   for (uint32_t i = 0; i < num_top_roots; ++i)
   {
      auto root = ses->get_root(i);
      if (root)
      {
         ++root_count;
         if (first_root < 0)
            first_root = i;
         last_root = i;
      }
   }

   std::cout << "\n── Roots ──\n";
   std::cout << "  " << std::setw(20) << "Populated roots" << root_count << " / " << num_top_roots
             << "\n";
   if (root_count > 0)
      std::cout << "  " << std::setw(20) << "Range" << first_root << ".." << last_root << "\n";

   if (db.ref_counts_stale())
      std::cout << stale_refcount_warning;
}

void cmd_defrag(database& db, const fs::path& dir)
{
   auto total_before = dir_size(dir);

   std::cout << "Before: " << format_bytes(total_before) << "\n";
   std::cout << "Copying live objects to new database...\n";

   db.defrag();

   // After defrag, dir contains the new database and dir.old has the original
   auto total_after = dir_size(dir);
   auto backup_dir  = fs::path(dir.string() + ".old");

   std::cout << "After:  " << format_bytes(total_after) << "\n";

   if (total_before > total_after)
      std::cout << "Saved:  " << format_bytes(total_before - total_after) << "\n";
   else
      std::cout << "No space reclaimed.\n";

   std::cout << "Old database preserved at: " << backup_dir << "\n";
   std::cout << "Remove it manually after verifying the new database.\n";
}

std::string format_number(uint64_t n)
{
   auto s = std::to_string(n);
   int  pos = s.size() - 3;
   while (pos > 0)
   {
      s.insert(pos, ",");
      pos -= 3;
   }
   return s;
}

void print_audit_progress(const version_audit_result& r, void*)
{
   std::cerr << "  progress: nodes=" << format_number(r.nodes_visited)
             << " leaves=" << format_number(r.leaf_nodes)
             << " values=" << format_number(r.value_nodes)
             << " versions=" << format_number(r.value_versions_seen + r.leaf_versions_seen)
             << " dangling=" << format_number(r.dangling_pointers) << "\n";
}

int cmd_audit_versions(database& db)
{
   std::cout << "Auditing MVCC version index...\n\n";

   auto r = db.audit_version_index();

   std::cout << "-- Version Index --\n";
   std::cout << "  " << std::setw(32) << std::left << "Latest version"
             << format_number(r.latest_version) << "\n";
   std::cout << "  " << std::setw(32) << std::left << "Retained from index"
             << format_number(r.retained_versions_from_index) << "\n";
   std::cout << "  " << std::setw(32) << std::left << "Dead from index"
             << format_number(r.dead_versions_from_index) << "\n";
   std::cout << "  " << std::setw(32) << std::left << "Dead version ranges"
             << format_number(r.dead_version_ranges) << "\n";
   std::cout << "  " << std::setw(32) << std::left << "Pending dead versions"
             << format_number(r.pending_dead_versions) << "\n";

   std::cout << "\n-- Control Blocks --\n";
   std::cout << "  " << std::setw(32) << std::left << "Version control blocks"
             << format_number(r.version_control_blocks) << "\n";
	   std::cout << "  " << std::setw(32) << std::left << "Live version blocks"
	             << format_number(r.live_version_control_blocks) << "\n";
	   std::cout << "  " << std::setw(32) << std::left << "Unknown live version blocks"
	             << format_number(r.unknown_live_version_blocks) << "\n";
	   std::cout << "  " << std::setw(32) << std::left << "Zero-ref version blocks"
	             << format_number(r.zero_ref_version_blocks) << "\n";
   std::cout << "  " << std::setw(32) << std::left << "Oldest live version"
             << format_number(r.min_live_version) << "\n";
   std::cout << "  " << std::setw(32) << std::left << "Newest live version"
             << format_number(r.max_live_version) << "\n";

   std::cout << "\n-- Validation --\n";
   std::cout << "  " << std::setw(32) << std::left << "Index matches CB scan"
             << (r.index_matches_control_blocks() ? "yes" : "no") << "\n";

   return r.index_matches_control_blocks() ? 0 : 1;
}

int cmd_audit_refcounts(database& db, uint64_t prune_floor, uint64_t progress_nodes)
{
   version_audit_options options;
   options.prune_floor             = prune_floor;
   options.progress_interval_nodes = progress_nodes;
   if (progress_nodes != 0)
      options.progress = print_audit_progress;

   std::cout << "Auditing reachable version references...\n\n";
   std::cout.flush();

   auto r = db.audit_versions(options);

   std::cout << "-- Roots --\n";
   std::cout << "  " << std::setw(30) << std::left << "Roots checked"
             << r.roots_checked << " / " << num_top_roots << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Roots with version"
             << r.roots_with_version << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Roots without version"
             << r.roots_without_version << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Oldest root version"
             << r.oldest_root_version << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Newest root version"
             << r.newest_root_version << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Prune floor"
             << r.effective_prune_floor;
   if (r.requested_prune_floor == 0)
      std::cout << " (from oldest root)";
   std::cout << "\n";

   std::cout << "\n-- Reachable Nodes --\n";
   std::cout << "  " << std::setw(30) << std::left << "Nodes visited"
             << format_number(r.nodes_visited) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Shared nodes skipped"
             << format_number(r.shared_nodes_skipped) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Dangling pointers"
             << format_number(r.dangling_pointers) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Inner nodes"
             << format_number(r.inner_nodes) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Inner prefix nodes"
             << format_number(r.inner_prefix_nodes) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Leaf nodes"
             << format_number(r.leaf_nodes) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Value nodes"
             << format_number(r.value_nodes) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Flat value nodes"
             << format_number(r.flat_value_nodes) << "\n";

   std::cout << "\n-- Leaf Versions --\n";
   std::cout << "  " << std::setw(30) << std::left << "Leaf branches"
             << format_number(r.leaf_branches) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Version table entries"
             << format_number(r.leaf_version_table_entries) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Branch version refs"
             << format_number(r.leaf_branch_versions) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Leaf subtrees"
             << format_number(r.leaf_subtrees) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Leaf subtrees no ver"
             << format_number(r.leaf_subtrees_without_ver) << "\n";

   std::cout << "\n-- Value Versions --\n";
   std::cout << "  " << std::setw(30) << std::left << "Value entries"
             << format_number(r.value_entries) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Value nodes with history"
             << format_number(r.value_nodes_with_history) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Value nodes with next"
             << format_number(r.value_nodes_with_next) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Value next ptrs"
             << format_number(r.value_next_ptrs) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Subtree value nodes"
             << format_number(r.subtree_value_nodes) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Subtree value entries"
             << format_number(r.subtree_value_entries) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Max entries/node"
             << format_number(r.max_value_entries) << "\n";

   std::cout << "\n-- Retention Debt --\n";
   std::cout << "  " << std::setw(30) << std::left << "Distinct versions retained"
             << format_number(r.retained_versions) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Value version refs"
             << format_number(r.value_versions_seen) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Leaf version refs"
             << format_number(r.leaf_versions_seen) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Prunable value entries"
             << format_number(r.prunable_value_entries) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Nodes with prune work"
             << format_number(r.prunable_value_nodes) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Floor rewrite entries"
             << format_number(r.floor_rewrite_entries) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Unknown floor nodes"
             << format_number(r.prune_floor_unknown_nodes) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Out-of-range floor nodes"
             << format_number(r.prune_floor_out_of_range_nodes) << "\n";

   if (r.dangling_pointers != 0)
      return 1;
   return 0;
}

uint64_t histogram_total(const std::vector<uint64_t>& hist)
{
   uint64_t total = 0;
   for (auto count : hist)
      total += count;
   return total;
}

void print_histogram(const char* title, const std::vector<uint64_t>& hist, const char* bucket)
{
   std::cout << "\n-- " << title << " --\n";

   auto total = histogram_total(hist);
   if (total == 0)
   {
      std::cout << "  (empty)\n";
      return;
   }

   auto flags     = std::cout.flags();
   auto precision = std::cout.precision();

   std::cout << "  " << std::setw(12) << std::left << bucket
             << std::setw(18) << std::left << "Count"
             << "Percent\n";
   for (size_t i = 0; i < hist.size(); ++i)
   {
      if (hist[i] == 0)
         continue;
      double pct = 100.0 * double(hist[i]) / double(total);
      std::cout << "  " << std::setw(12) << std::left << i
                << std::setw(18) << std::left << format_number(hist[i])
                << std::fixed << std::setprecision(2) << pct << "%\n";
   }

   std::cout.flags(flags);
   std::cout.precision(precision);
}

tricorder_fanout_buckets to_tricorder_buckets(const tree_stats_fanout_buckets& b)
{
   return {
       b.fanout_1,
       b.fanout_2,
       b.fanout_3_to_4,
       b.fanout_5_to_8,
       b.fanout_9_to_16,
       b.fanout_17_to_32,
       b.fanout_33_to_64,
       b.fanout_65_to_128,
       b.fanout_129_plus,
   };
}

bool has_depth_data(const tree_stats_depth_row& row)
{
   return row.inner_nodes != 0 || row.inner_prefix_nodes != 0 || row.value_nodes != 0 ||
          row.leaf_nodes != 0;
}

tricorder_depth_stats to_tricorder_depth_stats(const tree_stats_depth_row& row)
{
   tricorder_depth_stats out;
   out.depth                 = row.depth;
   out.inner_nodes           = row.inner_nodes;
   out.inner_prefix_nodes    = row.inner_prefix_nodes;
   out.value_nodes           = row.value_nodes;
   out.flat_value_nodes      = row.flat_value_nodes;
   out.inner_branches        = row.inner_branches;
   out.single_branch_inners  = row.single_branch_inners;
   out.low_fanout_inners     = row.low_fanout_inners;
   out.fanout                = to_tricorder_buckets(row.fanout);
   out.leaf_nodes            = row.leaf_nodes;
   out.leaf_keys             = row.leaf_keys;
   out.selected_leaf_keys    = row.selected_leaf_keys;
   out.key_bytes             = row.key_bytes;
   out.selected_key_bytes    = row.selected_key_bytes;
   out.max_key_size          = row.max_key_size;
   out.max_selected_key_size = row.max_selected_key_size;
   out.data_value_count      = row.data_value_count;
   out.data_value_bytes      = row.data_value_bytes;
   out.max_data_value_size   = row.max_data_value_size;
   out.leaf_clines           = row.leaf_clines;
   out.max_leaf_clines       = row.max_leaf_clines;
   out.cline_saturated_leaves = row.cline_saturated_leaves;
   out.leaf_address_values   = row.leaf_address_values;
   out.leaf_alloc_bytes      = row.leaf_alloc_bytes;
   out.leaf_used_bytes       = row.leaf_used_bytes;
   out.leaf_dead_bytes       = row.leaf_dead_bytes;
   out.leaf_empty_bytes      = row.leaf_empty_bytes;
   out.full_leaf_nodes       = row.full_leaf_nodes;
   out.full_leaf_dead_bytes  = row.full_leaf_dead_bytes;
   out.full_leaf_empty_bytes = row.full_leaf_empty_bytes;
   return out;
}

tricorder_tree_stats_report make_tree_stats_report(const tree_stats_result& s)
{
   tricorder_tree_stats_report out;
   out.key_range_enabled     = s.key_range_enabled;
   out.range_lower_hex       = bytes_to_hex(s.key_range_lower);
   out.range_upper_hex       = bytes_to_hex(s.key_range_upper);
   out.root_filter_enabled   = s.root_filter_enabled;
   out.root_filter_index     = s.root_filter_index;
   out.scan_truncated        = s.scan_truncated;
   out.max_nodes             = s.max_nodes;
   out.roots_checked          = s.roots_checked;
   out.roots_with_version     = s.roots_with_version;
   out.roots_without_version  = s.roots_without_version;
   out.latest_version         = s.latest_version;
   out.retained_versions      = s.retained_versions;
   out.dead_versions          = s.dead_versions;
   out.dead_version_ranges    = s.dead_version_ranges;
   out.pending_dead_versions  = s.pending_dead_versions;
   out.nodes_visited         = s.nodes_visited;
   out.shared_nodes_skipped   = s.shared_nodes_skipped;
   out.dangling_pointers      = s.dangling_pointers;
   out.reachable_bytes        = s.reachable_bytes;
   out.inner_nodes            = s.inner_nodes;
   out.inner_prefix_nodes     = s.inner_prefix_nodes;
   out.leaf_nodes             = s.leaf_nodes;
   out.value_nodes            = s.value_nodes;
   out.flat_value_nodes       = s.flat_value_nodes;
   out.inner_branches         = s.inner_branches;
   out.single_branch_inners   = s.single_branch_inners;
   out.low_fanout_inners      = s.low_fanout_inners;
   out.leaf_keys              = s.leaf_keys;
   out.selected_leaf_keys     = s.selected_leaf_keys;
   out.key_bytes              = s.key_bytes;
   out.selected_key_bytes     = s.selected_key_bytes;
   out.max_key_size           = s.max_key_size;
   out.max_selected_key_size  = s.max_selected_key_size;
   out.data_value_count       = s.data_value_count;
   out.data_value_bytes       = s.data_value_bytes;
   out.max_data_value_size    = s.max_data_value_size;
   out.leaf_clines            = s.leaf_clines;
   out.max_leaf_clines        = s.max_leaf_clines;
   out.cline_saturated_leaves = s.cline_saturated_leaves;
   out.leaf_address_values    = s.leaf_address_values;
   out.max_depth              = s.max_depth;
   out.leaf_depth_sum         = s.leaf_depth_sum;
   out.key_depth_sum          = s.key_depth_sum;
   out.total_inner_bytes      = s.total_inner_bytes;
   out.total_leaf_alloc_bytes = s.total_leaf_alloc_bytes;
   out.total_leaf_used_bytes  = s.total_leaf_used_bytes;
   out.total_leaf_dead_bytes  = s.total_leaf_dead_bytes;
   out.total_leaf_empty_bytes = s.total_leaf_empty_bytes;
   out.full_leaf_nodes        = s.full_leaf_nodes;
   out.full_leaf_dead_bytes   = s.full_leaf_dead_bytes;
   out.full_leaf_empty_bytes  = s.full_leaf_empty_bytes;
   out.total_value_bytes      = s.total_value_bytes;
   out.branches_per_inner_node = s.branches_per_inner_node;
   out.keys_per_leaf          = s.keys_per_leaf;
   out.leaf_clines_histogram  = s.leaf_clines_histogram;
   out.address_values_per_leaf = s.address_values_per_leaf;
   out.leaf_depths            = s.leaf_depths;

   out.depth_stats.reserve(s.depth_stats.size());
   for (const auto& row : s.depth_stats)
      if (has_depth_data(row))
         out.depth_stats.push_back(to_tricorder_depth_stats(row));
   return out;
}

#ifndef PSITRI_TRICORDER_HAS_PSIO_JSON
void write_json_key(std::ostream& os, bool& first, std::string_view key)
{
   if (!first)
      os << ',';
   first = false;
   os << '"';
   for (char c : key)
   {
      if (c == '"' || c == '\\')
         os << '\\';
      os << c;
   }
   os << "\":";
}

void write_json_uint_field(std::ostream& os, bool& first, std::string_view key, uint64_t value)
{
   write_json_key(os, first, key);
   os << value;
}

void write_json_bool_field(std::ostream& os, bool& first, std::string_view key, bool value)
{
   write_json_key(os, first, key);
   os << (value ? "true" : "false");
}

void write_json_string_field(std::ostream& os,
                             bool& first,
                             std::string_view key,
                             std::string_view value)
{
   write_json_key(os, first, key);
   os << '"';
   for (char c : value)
   {
      if (c == '"' || c == '\\')
         os << '\\';
      os << c;
   }
   os << '"';
}

void write_json_vector_field(std::ostream&              os,
                             bool&                      first,
                             std::string_view           key,
                             const std::vector<uint64_t>& values)
{
   write_json_key(os, first, key);
   os << '[';
   for (size_t i = 0; i < values.size(); ++i)
   {
      if (i != 0)
         os << ',';
      os << values[i];
   }
   os << ']';
}

void write_json(std::ostream& os, const tricorder_fanout_buckets& b)
{
   bool first = true;
   os << '{';
   write_json_uint_field(os, first, "fanout_1", b.fanout_1);
   write_json_uint_field(os, first, "fanout_2", b.fanout_2);
   write_json_uint_field(os, first, "fanout_3_to_4", b.fanout_3_to_4);
   write_json_uint_field(os, first, "fanout_5_to_8", b.fanout_5_to_8);
   write_json_uint_field(os, first, "fanout_9_to_16", b.fanout_9_to_16);
   write_json_uint_field(os, first, "fanout_17_to_32", b.fanout_17_to_32);
   write_json_uint_field(os, first, "fanout_33_to_64", b.fanout_33_to_64);
   write_json_uint_field(os, first, "fanout_65_to_128", b.fanout_65_to_128);
   write_json_uint_field(os, first, "fanout_129_plus", b.fanout_129_plus);
   os << '}';
}

void write_json(std::ostream& os, const tricorder_depth_stats& row)
{
   bool first = true;
   os << '{';
   write_json_uint_field(os, first, "depth", row.depth);
   write_json_uint_field(os, first, "inner_nodes", row.inner_nodes);
   write_json_uint_field(os, first, "inner_prefix_nodes", row.inner_prefix_nodes);
   write_json_uint_field(os, first, "value_nodes", row.value_nodes);
   write_json_uint_field(os, first, "flat_value_nodes", row.flat_value_nodes);
   write_json_uint_field(os, first, "inner_branches", row.inner_branches);
   write_json_uint_field(os, first, "single_branch_inners", row.single_branch_inners);
   write_json_uint_field(os, first, "low_fanout_inners", row.low_fanout_inners);
   write_json_key(os, first, "fanout");
   write_json(os, row.fanout);
   write_json_uint_field(os, first, "leaf_nodes", row.leaf_nodes);
   write_json_uint_field(os, first, "leaf_keys", row.leaf_keys);
   write_json_uint_field(os, first, "selected_leaf_keys", row.selected_leaf_keys);
   write_json_uint_field(os, first, "key_bytes", row.key_bytes);
   write_json_uint_field(os, first, "selected_key_bytes", row.selected_key_bytes);
   write_json_uint_field(os, first, "max_key_size", row.max_key_size);
   write_json_uint_field(os, first, "max_selected_key_size", row.max_selected_key_size);
   write_json_uint_field(os, first, "data_value_count", row.data_value_count);
   write_json_uint_field(os, first, "data_value_bytes", row.data_value_bytes);
   write_json_uint_field(os, first, "max_data_value_size", row.max_data_value_size);
   write_json_uint_field(os, first, "leaf_clines", row.leaf_clines);
   write_json_uint_field(os, first, "max_leaf_clines", row.max_leaf_clines);
   write_json_uint_field(os, first, "cline_saturated_leaves",
                         row.cline_saturated_leaves);
   write_json_uint_field(os, first, "leaf_address_values", row.leaf_address_values);
   write_json_uint_field(os, first, "leaf_alloc_bytes", row.leaf_alloc_bytes);
   write_json_uint_field(os, first, "leaf_used_bytes", row.leaf_used_bytes);
   write_json_uint_field(os, first, "leaf_dead_bytes", row.leaf_dead_bytes);
   write_json_uint_field(os, first, "leaf_empty_bytes", row.leaf_empty_bytes);
   write_json_uint_field(os, first, "full_leaf_nodes", row.full_leaf_nodes);
   write_json_uint_field(os, first, "full_leaf_dead_bytes", row.full_leaf_dead_bytes);
   write_json_uint_field(os, first, "full_leaf_empty_bytes", row.full_leaf_empty_bytes);
   os << '}';
}

std::string to_json(const tricorder_tree_stats_report& report)
{
   std::ostringstream os;
   bool               first = true;
   os << '{';
   write_json_uint_field(os, first, "schema_version", report.schema_version);
   write_json_bool_field(os, first, "key_range_enabled", report.key_range_enabled);
   write_json_string_field(os, first, "range_lower_hex", report.range_lower_hex);
   write_json_string_field(os, first, "range_upper_hex", report.range_upper_hex);
   write_json_bool_field(os, first, "root_filter_enabled", report.root_filter_enabled);
   write_json_uint_field(os, first, "root_filter_index", report.root_filter_index);
   write_json_bool_field(os, first, "scan_truncated", report.scan_truncated);
   write_json_uint_field(os, first, "max_nodes", report.max_nodes);
   write_json_uint_field(os, first, "roots_checked", report.roots_checked);
   write_json_uint_field(os, first, "roots_with_version", report.roots_with_version);
   write_json_uint_field(os, first, "roots_without_version", report.roots_without_version);
   write_json_uint_field(os, first, "latest_version", report.latest_version);
   write_json_uint_field(os, first, "retained_versions", report.retained_versions);
   write_json_uint_field(os, first, "dead_versions", report.dead_versions);
   write_json_uint_field(os, first, "dead_version_ranges", report.dead_version_ranges);
   write_json_uint_field(os, first, "pending_dead_versions", report.pending_dead_versions);
   write_json_uint_field(os, first, "nodes_visited", report.nodes_visited);
   write_json_uint_field(os, first, "shared_nodes_skipped", report.shared_nodes_skipped);
   write_json_uint_field(os, first, "dangling_pointers", report.dangling_pointers);
   write_json_uint_field(os, first, "reachable_bytes", report.reachable_bytes);
   write_json_uint_field(os, first, "inner_nodes", report.inner_nodes);
   write_json_uint_field(os, first, "inner_prefix_nodes", report.inner_prefix_nodes);
   write_json_uint_field(os, first, "leaf_nodes", report.leaf_nodes);
   write_json_uint_field(os, first, "value_nodes", report.value_nodes);
   write_json_uint_field(os, first, "flat_value_nodes", report.flat_value_nodes);
   write_json_uint_field(os, first, "inner_branches", report.inner_branches);
   write_json_uint_field(os, first, "single_branch_inners", report.single_branch_inners);
   write_json_uint_field(os, first, "low_fanout_inners", report.low_fanout_inners);
   write_json_uint_field(os, first, "leaf_keys", report.leaf_keys);
   write_json_uint_field(os, first, "selected_leaf_keys", report.selected_leaf_keys);
   write_json_uint_field(os, first, "key_bytes", report.key_bytes);
   write_json_uint_field(os, first, "selected_key_bytes", report.selected_key_bytes);
   write_json_uint_field(os, first, "max_key_size", report.max_key_size);
   write_json_uint_field(os, first, "max_selected_key_size", report.max_selected_key_size);
   write_json_uint_field(os, first, "data_value_count", report.data_value_count);
   write_json_uint_field(os, first, "data_value_bytes", report.data_value_bytes);
   write_json_uint_field(os, first, "max_data_value_size", report.max_data_value_size);
   write_json_uint_field(os, first, "leaf_clines", report.leaf_clines);
   write_json_uint_field(os, first, "max_leaf_clines", report.max_leaf_clines);
   write_json_uint_field(os, first, "cline_saturated_leaves",
                         report.cline_saturated_leaves);
   write_json_uint_field(os, first, "leaf_address_values",
                         report.leaf_address_values);
   write_json_uint_field(os, first, "max_depth", report.max_depth);
   write_json_uint_field(os, first, "leaf_depth_sum", report.leaf_depth_sum);
   write_json_uint_field(os, first, "key_depth_sum", report.key_depth_sum);
   write_json_uint_field(os, first, "total_inner_bytes", report.total_inner_bytes);
   write_json_uint_field(os, first, "total_leaf_alloc_bytes", report.total_leaf_alloc_bytes);
   write_json_uint_field(os, first, "total_leaf_used_bytes", report.total_leaf_used_bytes);
   write_json_uint_field(os, first, "total_leaf_dead_bytes", report.total_leaf_dead_bytes);
   write_json_uint_field(os, first, "total_leaf_empty_bytes", report.total_leaf_empty_bytes);
   write_json_uint_field(os, first, "full_leaf_nodes", report.full_leaf_nodes);
   write_json_uint_field(os, first, "full_leaf_dead_bytes", report.full_leaf_dead_bytes);
   write_json_uint_field(os, first, "full_leaf_empty_bytes", report.full_leaf_empty_bytes);
   write_json_uint_field(os, first, "total_value_bytes", report.total_value_bytes);
   write_json_vector_field(os, first, "branches_per_inner_node",
                           report.branches_per_inner_node);
   write_json_vector_field(os, first, "keys_per_leaf", report.keys_per_leaf);
   write_json_vector_field(os, first, "leaf_clines_histogram",
                           report.leaf_clines_histogram);
   write_json_vector_field(os, first, "address_values_per_leaf",
                           report.address_values_per_leaf);
   write_json_vector_field(os, first, "leaf_depths", report.leaf_depths);

   write_json_key(os, first, "depth_stats");
   os << '[';
   for (size_t i = 0; i < report.depth_stats.size(); ++i)
   {
      if (i != 0)
         os << ',';
      write_json(os, report.depth_stats[i]);
   }
   os << "]}";
   return os.str();
}
#endif

std::string tree_stats_to_json(const tree_stats_result& s)
{
   auto report = make_tree_stats_report(s);
#ifdef PSITRI_TRICORDER_HAS_PSIO_JSON
   return psio::convert_to_json(report);
#else
   return to_json(report);
#endif
}

void json_quote(std::ostream& os, std::string_view value)
{
   os << '"';
   for (unsigned char c : value)
   {
      switch (c)
      {
         case '"':
         case '\\':
            os << '\\' << static_cast<char>(c);
            break;
         case '\n':
            os << "\\n";
            break;
         case '\r':
            os << "\\r";
            break;
         case '\t':
            os << "\\t";
            break;
         default:
            if (c < 0x20)
            {
               static constexpr char digits[] = "0123456789abcdef";
               os << "\\u00" << digits[c >> 4] << digits[c & 0x0f];
            }
            else
            {
               os << static_cast<char>(c);
            }
      }
   }
   os << '"';
}

std::string tree_stats_partitioned_to_json(const tree_stats_partitioned_summary& summary)
{
   std::ostringstream os;
   bool               first = true;
   auto key = [&](std::string_view name) {
      if (!first)
         os << ',';
      first = false;
      json_quote(os, name);
      os << ':';
   };
   auto field_uint = [&](std::string_view name, uint64_t value) {
      key(name);
      os << value;
   };
   auto field_bool = [&](std::string_view name, bool value) {
      key(name);
      os << (value ? "true" : "false");
   };
   auto field_string = [&](std::string_view name, std::string_view value) {
      key(name);
      json_quote(os, value);
   };

   os << '{';
   field_uint("schema_version", 1);
   field_string("mode", "range-shards");
   field_bool("partitioned_shape_totals_are_exact", false);
   field_uint("shard_count", summary.shard_count);
   field_uint("jobs", summary.jobs);
   field_string("range_lower_hex", summary.range_lower_hex);
   field_string("range_upper_hex", summary.range_upper_hex);
   field_bool("root_filter_enabled", summary.root_filter_enabled);
   field_uint("root_filter_index", summary.root_filter_index);
   field_bool("scan_truncated", summary.scan_truncated);
   field_uint("max_nodes_per_shard", summary.max_nodes_per_shard);
   field_uint("selected_leaf_keys", summary.selected_leaf_keys);
   field_uint("nodes_visited", summary.nodes_visited);
   field_uint("leaf_nodes", summary.leaf_nodes);
   field_uint("reachable_bytes", summary.reachable_bytes);
   field_uint("dangling_pointers", summary.dangling_pointers);
   field_uint("shared_nodes_skipped", summary.shared_nodes_skipped);
   field_uint("total_leaf_dead_bytes", summary.total_leaf_dead_bytes);
   field_uint("total_leaf_empty_bytes", summary.total_leaf_empty_bytes);
   field_string("note",
                "Shard sums are for sampling and can double-count shared ancestors, "
                "boundary leaves, and shared subtrees. Use unsharded tree-stats for "
                "an exact global audit.");

   key("shards");
   os << '[';
   for (size_t i = 0; i < summary.shards.size(); ++i)
   {
      if (i != 0)
         os << ',';

      const auto& shard = summary.shards[i];
      bool        shard_first = true;
      auto shard_key = [&](std::string_view name) {
         if (!shard_first)
            os << ',';
         shard_first = false;
         json_quote(os, name);
         os << ':';
      };

      os << '{';
      shard_key("shard_index");
      os << shard.shard_index;
      shard_key("range_lower_hex");
      json_quote(os, range_bound_hex(shard.options.key_lower));
      shard_key("range_upper_hex");
      json_quote(os, range_bound_hex(shard.options.key_upper));
      shard_key("stats");
      os << tree_stats_to_json(shard.stats);
      os << '}';
   }
   os << "]}";
   return os.str();
}

void print_fanout_by_depth(const tree_stats_result& s)
{
   std::cout << "\n-- Fanout By Depth --\n";
   std::cout << "  " << std::setw(7) << std::left << "Depth"
             << std::setw(14) << std::left << "Inner"
             << std::setw(14) << std::left << "Prefix"
             << std::setw(12) << std::left << "Avg"
             << std::setw(10) << std::left << "1"
             << std::setw(10) << std::left << "2"
             << std::setw(10) << std::left << "3-4"
             << std::setw(10) << std::left << "5-8"
             << std::setw(10) << std::left << "9-16"
             << std::setw(10) << std::left << "17-32"
             << std::setw(10) << std::left << "33-64"
             << std::setw(10) << std::left << "65-128"
             << "129+\n";

   bool any = false;
   for (const auto& row : s.depth_stats)
   {
      if (row.total_inner_nodes() == 0)
         continue;

      any = true;
      std::cout << "  " << std::setw(7) << std::left << row.depth
                << std::setw(14) << std::left << format_number(row.inner_nodes)
                << std::setw(14) << std::left << format_number(row.inner_prefix_nodes)
                << std::setw(12) << std::left << std::fixed << std::setprecision(2)
                << row.average_branches_per_inner()
                << std::setw(10) << std::left << format_number(row.fanout.fanout_1)
                << std::setw(10) << std::left << format_number(row.fanout.fanout_2)
                << std::setw(10) << std::left << format_number(row.fanout.fanout_3_to_4)
                << std::setw(10) << std::left << format_number(row.fanout.fanout_5_to_8)
                << std::setw(10) << std::left << format_number(row.fanout.fanout_9_to_16)
                << std::setw(10) << std::left << format_number(row.fanout.fanout_17_to_32)
                << std::setw(10) << std::left << format_number(row.fanout.fanout_33_to_64)
                << std::setw(10) << std::left << format_number(row.fanout.fanout_65_to_128)
                << format_number(row.fanout.fanout_129_plus) << "\n";
   }

   if (!any)
      std::cout << "  (empty)\n";
}

void print_leaf_density_by_depth(const tree_stats_result& s)
{
   std::cout << "\n-- Leaf Density By Depth --\n";
   std::cout << "  " << std::setw(7) << std::left << "Depth"
             << std::setw(14) << std::left << "Leaves"
             << std::setw(16) << std::left << "Keys"
             << std::setw(16) << std::left << "Selected"
             << std::setw(12) << std::left << "Keys/Leaf"
             << std::setw(14) << std::left << "Alloc"
             << std::setw(14) << std::left << "Used"
             << std::setw(12) << std::left << "Dead%"
             << "Empty%\n";

   bool any = false;
   for (const auto& row : s.depth_stats)
   {
      if (row.leaf_nodes == 0)
         continue;

      any = true;
      std::cout << "  " << std::setw(7) << std::left << row.depth
                << std::setw(14) << std::left << format_number(row.leaf_nodes)
                << std::setw(16) << std::left << format_number(row.leaf_keys)
                << std::setw(16) << std::left << format_number(row.selected_leaf_keys)
                << std::setw(12) << std::left << std::fixed << std::setprecision(2)
                << row.average_keys_per_leaf()
                << std::setw(14) << std::left << format_bytes(row.leaf_alloc_bytes)
                << std::setw(14) << std::left << format_bytes(row.leaf_used_bytes)
                << std::setw(12) << std::left << row.leaf_dead_space_percent()
                << row.leaf_empty_space_percent() << "\n";
   }

   if (!any)
      std::cout << "  (empty)\n";
}

void print_leaf_clines_by_depth(const tree_stats_result& s)
{
   std::cout << "\n-- Leaf Clines By Depth --\n";
   std::cout << "  " << std::setw(7) << std::left << "Depth"
             << std::setw(14) << std::left << "Leaves"
             << std::setw(14) << std::left << "Clines"
             << std::setw(14) << std::left << "AddrVals"
             << std::setw(12) << std::left << "C/Leaf"
             << std::setw(12) << std::left << "A/Leaf"
             << std::setw(12) << std::left << "A/Cline"
             << std::setw(8) << std::left << "Max"
             << "Sat\n";

   bool any = false;
   for (const auto& row : s.depth_stats)
   {
      if (row.leaf_nodes == 0)
         continue;

      any = true;
      std::cout << "  " << std::setw(7) << std::left << row.depth
                << std::setw(14) << std::left << format_number(row.leaf_nodes)
                << std::setw(14) << std::left << format_number(row.leaf_clines)
                << std::setw(14) << std::left << format_number(row.leaf_address_values)
                << std::setw(12) << std::left << std::fixed << std::setprecision(2)
                << row.average_clines_per_leaf()
                << std::setw(12) << std::left
                << row.average_address_values_per_leaf()
                << std::setw(12) << std::left
                << row.average_address_values_per_cline()
                << std::setw(8) << std::left << row.max_leaf_clines
                << format_number(row.cline_saturated_leaves) << "\n";
   }

   if (!any)
      std::cout << "  (empty)\n";
}

int cmd_tree_stats(database& db, bool json_output, const tree_stats_options& options)
{
   if (json_output)
   {
      std::cerr << "Collecting tree shape stats...\n";
      if (options.has_key_range())
      {
         std::cerr << "  range: ["
                   << (options.key_lower ? bytes_to_hex(*options.key_lower) : std::string{})
                   << ", "
                   << (options.key_upper ? bytes_to_hex(*options.key_upper) : std::string{"inf"})
                   << ")\n";
      }
      if (options.root_index)
         std::cerr << "  root-index: " << *options.root_index << "\n";
      if (options.max_nodes != 0)
         std::cerr << "  max-nodes: " << options.max_nodes << "\n";
      std::cerr.flush();
   }
   else
   {
      std::cout << "Collecting tree shape stats...\n\n";
      if (options.has_key_range())
      {
         std::cout << "Range: ["
                   << (options.key_lower ? bytes_to_hex(*options.key_lower) : std::string{})
                   << ", "
                   << (options.key_upper ? bytes_to_hex(*options.key_upper) : std::string{"inf"})
                   << ")\n\n";
      }
      if (options.root_index)
         std::cout << "Root index: " << *options.root_index << "\n\n";
      if (options.max_nodes != 0)
         std::cout << "Max nodes: " << format_number(options.max_nodes) << "\n\n";
      std::cout.flush();
   }

   auto s = db.tree_stats(options);

   if (json_output)
   {
      std::cout << tree_stats_to_json(s) << "\n";
      if (db.ref_counts_stale())
         std::cerr << stale_refcount_warning;
      return s.dangling_pointers != 0 ? 1 : 0;
   }

   auto flags     = std::cout.flags();
   auto precision = std::cout.precision();

   std::cout << "-- Versions --\n";
   std::cout << "  " << std::setw(30) << std::left << "Latest version"
             << format_number(s.latest_version) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Retained versions"
             << format_number(s.retained_versions) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Dead versions"
             << format_number(s.dead_versions) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Dead version ranges"
             << format_number(s.dead_version_ranges) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Pending dead versions"
             << format_number(s.pending_dead_versions) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Scan truncated"
             << (s.scan_truncated ? "yes" : "no") << "\n";
   if (s.max_nodes != 0)
      std::cout << "  " << std::setw(30) << std::left << "Max nodes"
                << format_number(s.max_nodes) << "\n";

   std::cout << "\n-- Roots --\n";
   std::cout << "  " << std::setw(30) << std::left << "Roots checked"
             << s.roots_checked << " / " << num_top_roots << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Roots with version"
             << s.roots_with_version << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Roots without version"
             << s.roots_without_version << "\n";

   std::cout << "\n-- Shape --\n";
   std::cout << "  " << std::setw(30) << std::left << "Nodes visited"
             << format_number(s.nodes_visited) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Shared nodes skipped"
             << format_number(s.shared_nodes_skipped) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Dangling pointers"
             << format_number(s.dangling_pointers) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Reachable bytes"
             << format_bytes(s.reachable_bytes) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Inner nodes"
             << format_number(s.inner_nodes) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Inner prefix nodes"
             << format_number(s.inner_prefix_nodes) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Leaf nodes"
             << format_number(s.leaf_nodes) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Value nodes"
             << format_number(s.value_nodes) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Flat value nodes"
             << format_number(s.flat_value_nodes) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Keys"
             << format_number(s.leaf_keys) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Selected keys"
             << format_number(s.selected_leaf_keys) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Single-branch inners"
             << format_number(s.single_branch_inners) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Inners with <=2 branches"
             << format_number(s.low_fanout_inners) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Max depth"
             << s.max_depth << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Avg depth/key"
             << std::fixed << std::setprecision(2) << s.average_depth() << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Avg depth/leaf"
             << std::fixed << std::setprecision(2) << s.average_leaf_depth() << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Avg branches/inner"
             << std::fixed << std::setprecision(2) << s.average_branches_per_inner() << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Avg keys/leaf"
             << std::fixed << std::setprecision(2) << s.average_keys_per_leaf() << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Avg key size"
             << std::fixed << std::setprecision(2) << s.average_key_size() << " bytes\n";
   std::cout << "  " << std::setw(30) << std::left << "Avg selected key size"
             << std::fixed << std::setprecision(2) << s.average_selected_key_size()
             << " bytes\n";
   std::cout << "  " << std::setw(30) << std::left << "Max key size"
             << format_bytes(s.max_key_size) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Max selected key size"
             << format_bytes(s.max_selected_key_size) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Data values"
             << format_number(s.data_value_count) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Avg data value size"
             << format_bytes(uint64_t(s.average_data_value_size())) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Max data value size"
             << format_bytes(s.max_data_value_size) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Leaf clines"
             << format_number(s.leaf_clines) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Avg clines/leaf"
             << std::fixed << std::setprecision(2) << s.average_clines_per_leaf()
             << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Max clines/leaf"
             << s.max_leaf_clines << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Cline-saturated leaves"
             << format_number(s.cline_saturated_leaves) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Leaf address values"
             << format_number(s.leaf_address_values) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Addr values/leaf"
             << std::fixed << std::setprecision(2)
             << s.average_address_values_per_leaf() << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Addr values/cline"
             << std::fixed << std::setprecision(2)
             << s.average_address_values_per_cline() << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Avg leaf alloc size"
             << format_bytes(uint64_t(s.average_leaf_alloc_size())) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Avg leaf used size"
             << format_bytes(uint64_t(s.average_leaf_used_size())) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Leaf dead space"
             << format_bytes(s.total_leaf_dead_bytes) << " ("
             << std::fixed << std::setprecision(2) << s.leaf_dead_space_percent()
             << "%)\n";
   std::cout << "  " << std::setw(30) << std::left << "Avg leaf dead space"
             << format_bytes(uint64_t(s.average_leaf_dead_space())) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Leaf empty space"
             << format_bytes(s.total_leaf_empty_bytes) << " ("
             << std::fixed << std::setprecision(2) << s.leaf_empty_space_percent()
             << "%)\n";
   std::cout << "  " << std::setw(30) << std::left << "Avg leaf empty space"
             << format_bytes(uint64_t(s.average_leaf_empty_space())) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Full leaf nodes"
             << format_number(s.full_leaf_nodes) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Full leaf dead space"
             << format_bytes(s.full_leaf_dead_bytes) << " ("
             << std::fixed << std::setprecision(2) << s.full_leaf_dead_space_percent()
             << "%)\n";
   std::cout << "  " << std::setw(30) << std::left << "Avg full leaf dead"
             << format_bytes(uint64_t(s.average_full_leaf_dead_space())) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Full leaf empty space"
             << format_bytes(s.full_leaf_empty_bytes) << " ("
             << std::fixed << std::setprecision(2) << s.full_leaf_empty_space_percent()
             << "%)\n";
   std::cout << "  " << std::setw(30) << std::left << "Avg full leaf empty"
             << format_bytes(uint64_t(s.average_full_leaf_empty_space())) << "\n";

   std::cout.flags(flags);
   std::cout.precision(precision);

   print_fanout_by_depth(s);
   print_leaf_density_by_depth(s);
   print_leaf_clines_by_depth(s);
   print_histogram("Branches Per Inner Node", s.branches_per_inner_node, "Branches");
   print_histogram("Keys Per Leaf", s.keys_per_leaf, "Keys");
   print_histogram("Leaf Cline Histogram", s.leaf_clines_histogram, "Clines");
   print_histogram("Address Values Per Leaf", s.address_values_per_leaf,
                   "AddressValues");
   print_histogram("Leaf Depths", s.leaf_depths, "Depth");

   if (db.ref_counts_stale())
      std::cout << stale_refcount_warning;

   if (s.dangling_pointers != 0)
      return 1;
   return 0;
}

int cmd_verify(database& db)
{
   std::cout << "Verifying database integrity...\n\n";

   auto r = db.verify();

   // Segment sync checksums
   std::cout << "-- Segment Sync Checksums --\n";
   std::cout << "  " << std::setw(22) << std::left << "Checked"
             << format_number(r.segment_checksums.total()) << "\n";
   std::cout << "  " << std::setw(22) << std::left << "Passed"
             << format_number(r.segment_checksums.passed) << "\n";
   std::cout << "  " << std::setw(22) << std::left << "Failed"
             << format_number(r.segment_checksums.failed) << "\n";
   std::cout << "  " << std::setw(22) << std::left << "Not checksummed"
             << format_number(r.segment_checksums.unknown) << "\n";

   // Object checksums
   std::cout << "\n-- Object Checksums --\n";
   std::cout << "  " << std::setw(22) << std::left << "Reachable objects"
             << format_number(r.object_checksums.total()) << "\n";
   std::cout << "  " << std::setw(22) << std::left << "Passed"
             << format_number(r.object_checksums.passed) << "\n";
   std::cout << "  " << std::setw(22) << std::left << "Failed"
             << format_number(r.object_checksums.failed) << "\n";
   std::cout << "  " << std::setw(22) << std::left << "Not checksummed"
             << format_number(r.object_checksums.unknown) << "\n";

   // Key hashes
   std::cout << "\n-- Key Hashes --\n";
   std::cout << "  " << std::setw(22) << std::left << "Keys checked"
             << format_number(r.key_checksums.total()) << "\n";
   std::cout << "  " << std::setw(22) << std::left << "Passed"
             << format_number(r.key_checksums.passed) << "\n";
   std::cout << "  " << std::setw(22) << std::left << "Failed"
             << format_number(r.key_checksums.failed) << "\n";

   // Value checksums
   std::cout << "\n-- Value Checksums --\n";
   std::cout << "  " << std::setw(22) << std::left << "Values checked"
             << format_number(r.value_checksums.total()) << "\n";
   std::cout << "  " << std::setw(22) << std::left << "Passed"
             << format_number(r.value_checksums.passed) << "\n";
   std::cout << "  " << std::setw(22) << std::left << "Failed"
             << format_number(r.value_checksums.failed) << "\n";

   // Tree structure
   std::cout << "\n-- Tree Structure --\n";
   std::cout << "  " << std::setw(22) << std::left << "Roots checked"
             << r.roots_checked << " / " << num_top_roots << "\n";
   std::cout << "  " << std::setw(22) << std::left << "Nodes visited"
             << format_number(r.nodes_visited) << "\n";
   std::cout << "  " << std::setw(22) << std::left << "Reachable size"
             << format_bytes(r.reachable_bytes) << "\n";
   std::cout << "  " << std::setw(22) << std::left << "Dangling pointers"
             << format_number(r.dangling_pointers) << "\n";

   if (db.ref_counts_stale())
      std::cout << stale_refcount_warning;

   // Failure details
   if (!r.ok())
   {
      std::cout << "\n-- Failures --\n";

      for (auto& f : r.segment_failures)
      {
         std::cout << "  FAIL: segment " << f.segment << " sync checksum at pos "
                   << f.range_start << "-" << f.range_end << "\n";
      }

      for (auto& f : r.node_failures)
      {
         std::cout << "  FAIL: " << f.failure_type << " at address 0x" << std::hex << *f.address
                   << std::dec;
         if (f.node_type != sal::header_type::undefined)
            std::cout << " (" << static_cast<psitri::node_type>(f.node_type) << ")";
         std::cout << "\n";
         if (!f.key_prefix_hex.empty())
            std::cout << "        Affected keys: prefix 0x" << f.key_prefix_hex << "...\n";
         std::cout << "        Root: " << f.root_index << "\n";
      }

      for (auto& f : r.key_failures)
      {
         std::cout << "  FAIL: " << f.failure_type << " at key 0x" << f.key_hex << "\n";
         std::cout << "        Root: " << f.root_index
                   << "  Leaf: 0x" << std::hex << *f.leaf_address << std::dec
                   << "  Branch: " << f.branch_index << "\n";
      }

      std::cout << "\n  " << format_number(r.total_failures()) << " integrity failure(s) found.\n";
      return 1;
   }

   std::cout << "\n  Database integrity verified.\n";
   return 0;
}

// ── vas-info command ─────────────────────────────────────────────────────────

void cmd_vas_info()
{
   std::cout <<
      "Virtual Address Space (VAS) Report\n"
      "====================================\n"
      "\n"
      "VAS = Virtual Address Space — the range of memory addresses a process\n"
      "can use. On 64-bit Linux each process has up to ~128 TiB of VAS.\n"
      "\n"
      "At startup psitri reserves a large contiguous VAS region (PROT_NONE,\n"
      "no physical RAM used) for each internal file. This ensures that all\n"
      "future MAP_FIXED growth stays within the reserved range and never\n"
      "silently overwrites unrelated mappings. The reservation itself is\n"
      "virtual-only: it does not consume RAM or swap until data is written.\n"
      "\n"
      "Impact of insufficient VAS:\n"
      "  - The OS cannot satisfy the full reservation and a smaller region is\n"
      "    used, capping the maximum database size for that process lifetime.\n"
      "  - Running many databases in one process (e.g. a test suite) fragments\n"
      "    VAS progressively, so later databases receive smaller reservations.\n"
      "  - Use --max-db-size to match the reservation to your actual workload\n"
      "    and avoid wasting VAS on space you will never use.\n"
      "\n";

   // Probe the three block_allocator types used by a psitri database.
   struct probe
   {
      const char* name;
      uint64_t    block_size;
      uint64_t    theoretical_bytes;
   };

   const uint64_t ptrs_per_zone   = 1u << 22;  // 4 M, matches control_block_alloc
   const uint64_t zone_size_bytes = ptrs_per_zone * 8;  // sizeof(control_block) == 8
   const uint64_t free_list_block = ptrs_per_zone / 8;
   const uint32_t max_zones       = static_cast<uint32_t>((1ull << 32) / ptrs_per_zone);

   probe probes[] = {
       {"segment allocator (data)", sal::segment_size, sal::max_database_size},
       {"control blocks (zone.bin)", zone_size_bytes,
        static_cast<uint64_t>(max_zones) * zone_size_bytes},
       {"free list  (free_list.bin)", free_list_block,
        static_cast<uint64_t>(max_zones) * free_list_block},
   };

   std::cout << std::left
             << std::setw(30) << "allocator"
             << std::setw(12) << "block size"
             << std::setw(14) << "theoretical"
             << std::setw(14) << "available"
             << "headroom\n";
   std::cout << std::string(72, '-') << "\n";

   for (auto& p : probes)
   {
      uint64_t avail = sal::block_allocator::find_max_reservation_size(p.block_size);
      double   pct   = p.theoretical_bytes > 0
                           ? 100.0 * avail / p.theoretical_bytes
                           : 0.0;
      std::cout << std::left
                << std::setw(30) << p.name
                << std::setw(12) << format_bytes(p.block_size)
                << std::setw(14) << format_bytes(p.theoretical_bytes)
                << std::setw(14) << format_bytes(avail)
                << std::fixed << std::setprecision(1) << pct << "%\n";
   }

   std::cout << "\n"
             << "Recommended --max-db-size: "
             << format_bytes(sal::block_allocator::find_max_reservation_size(sal::segment_size))
             << "  (largest contiguous segment VAS currently available)\n";
}

void tree_stats_progress_callback(const tree_stats_result& stats, void* user)
{
   static std::mutex progress_mutex;
   std::lock_guard   lock(progress_mutex);

   std::cerr << "tree-stats progress: nodes=" << format_number(stats.nodes_visited)
             << " leaves=" << format_number(stats.leaf_nodes)
             << " selected_keys=" << format_number(stats.selected_leaf_keys)
             << " reachable=" << format_bytes(stats.reachable_bytes);
   if (user)
      std::cerr << " shard=" << *static_cast<const uint32_t*>(user);
   if (stats.max_nodes != 0)
      std::cerr << " max_nodes=" << format_number(stats.max_nodes);
   if (stats.scan_truncated)
      std::cerr << " truncated=1";
   std::cerr << "\n";
}

tree_stats_options make_tree_stats_options(std::string_view range_hex,
                                           const std::string& begin_hex,
                                           const std::string& end_hex,
                                           int64_t root_index,
                                           uint64_t progress_nodes,
                                           uint64_t max_nodes)
{
   tree_stats_options options;

   if (!range_hex.empty())
   {
      auto colon = range_hex.find(':');
      if (colon == std::string_view::npos)
         throw std::runtime_error("--range-hex must be formatted as BEGIN:END");
      auto begin = range_hex.substr(0, colon);
      auto end   = range_hex.substr(colon + 1);
      if (!begin.empty())
         options.key_lower = parse_hex_bytes(begin);
      if (!end.empty())
         options.key_upper = parse_hex_bytes(end);
   }

   if (!begin_hex.empty())
      options.key_lower = parse_hex_bytes(begin_hex);
   if (!end_hex.empty())
      options.key_upper = parse_hex_bytes(end_hex);

   if (options.key_lower && options.key_upper &&
       std::string_view(*options.key_upper).compare(*options.key_lower) <= 0)
      throw std::runtime_error("tree-stats key range upper bound must be greater than lower bound");

   if (root_index >= 0)
   {
      if (root_index >= num_top_roots)
         throw std::runtime_error("--root-index is outside the top-root table");
      options.root_index = static_cast<uint32_t>(root_index);
   }

   options.progress_interval_nodes = progress_nodes;
   options.progress = progress_nodes ? tree_stats_progress_callback : nullptr;
   options.max_nodes = max_nodes;
   return options;
}

std::vector<tree_stats_options> make_tree_stats_shard_options(const tree_stats_options& base,
                                                              uint32_t shard_count)
{
   if (shard_count < 2)
      throw std::runtime_error("--range-shards must be at least 2");
   if (shard_count > 256)
      throw std::runtime_error("--range-shards is capped at 256 first-byte partitions");

   std::vector<tree_stats_options> shards;
   shards.reserve(shard_count);
   for (uint32_t i = 0; i < shard_count; ++i)
   {
      const uint32_t start = (i * 256u) / shard_count;
      const uint32_t end   = ((i + 1u) * 256u) / shard_count;
      if (start == end)
         continue;

      auto shard = base;
      shard.key_lower = max_lower_bound(base.key_lower, first_byte_boundary(start));
      shard.key_upper = min_upper_bound(base.key_upper, first_byte_boundary(end));
      if (empty_key_range(shard.key_lower, shard.key_upper))
         continue;
      shards.push_back(std::move(shard));
   }

   if (shards.empty())
      throw std::runtime_error("--range-shards produced no non-empty key ranges");
   return shards;
}

uint32_t resolve_tree_stats_jobs(uint32_t requested_jobs, size_t shard_count)
{
   if (shard_count == 0)
      return 0;
   uint32_t jobs = requested_jobs;
   if (jobs == 0)
   {
      jobs = std::thread::hardware_concurrency();
      if (jobs == 0)
         jobs = 1;
   }
   jobs = std::max<uint32_t>(jobs, 1);
   return std::min<uint32_t>(jobs, static_cast<uint32_t>(shard_count));
}

tree_stats_partitioned_summary run_tree_stats_shards(database& db,
                                                     const tree_stats_options& base_options,
                                                     uint32_t shard_count,
                                                     uint32_t requested_jobs)
{
   auto shard_options = make_tree_stats_shard_options(base_options, shard_count);
   auto jobs          = resolve_tree_stats_jobs(requested_jobs, shard_options.size());

   tree_stats_partitioned_summary summary;
   summary.shard_count = static_cast<uint32_t>(shard_options.size());
   summary.jobs        = jobs;
   summary.range_lower_hex = range_bound_hex(base_options.key_lower);
   summary.range_upper_hex = range_bound_hex(base_options.key_upper);
   summary.root_filter_enabled = base_options.root_index.has_value();
   summary.root_filter_index   = base_options.root_index.value_or(0);
   summary.max_nodes_per_shard = base_options.max_nodes;

   summary.shards.resize(shard_options.size());
   for (size_t i = 0; i < shard_options.size(); ++i)
   {
      summary.shards[i].shard_index = static_cast<uint32_t>(i);
      summary.shards[i].options     = std::move(shard_options[i]);
      if (summary.shards[i].options.progress)
         summary.shards[i].options.progress_user = &summary.shards[i].shard_index;
   }

   std::atomic<size_t> next_shard{0};
   std::vector<std::exception_ptr> errors(jobs);
   std::vector<std::thread> workers;
   workers.reserve(jobs);

   for (uint32_t worker_index = 0; worker_index < jobs; ++worker_index)
   {
      workers.emplace_back([&db, &summary, &next_shard, &errors, worker_index]() {
         try
         {
            for (;;)
            {
               auto shard_index = next_shard.fetch_add(1, std::memory_order_relaxed);
               if (shard_index >= summary.shards.size())
                  break;
               auto& shard = summary.shards[shard_index];
               shard.stats = db.tree_stats(shard.options);
            }
         }
         catch (...)
         {
            errors[worker_index] = std::current_exception();
         }
      });
   }

   for (auto& worker : workers)
      worker.join();
   for (auto& error : errors)
      if (error)
         std::rethrow_exception(error);

   for (const auto& shard : summary.shards)
   {
      summary.selected_leaf_keys += shard.stats.selected_leaf_keys;
      summary.nodes_visited += shard.stats.nodes_visited;
      summary.leaf_nodes += shard.stats.leaf_nodes;
      summary.reachable_bytes += shard.stats.reachable_bytes;
      summary.dangling_pointers += shard.stats.dangling_pointers;
      summary.shared_nodes_skipped += shard.stats.shared_nodes_skipped;
      summary.total_leaf_dead_bytes += shard.stats.total_leaf_dead_bytes;
      summary.total_leaf_empty_bytes += shard.stats.total_leaf_empty_bytes;
      summary.scan_truncated = summary.scan_truncated || shard.stats.scan_truncated;
   }

   return summary;
}

int cmd_tree_stats_shards(database& db,
                          bool json_output,
                          const tree_stats_options& base_options,
                          uint32_t shard_count,
                          uint32_t requested_jobs)
{
   if (json_output)
   {
      std::cerr << "Collecting partitioned tree shape stats...\n"
                << "  requested-shards: " << shard_count << "\n";
      if (base_options.has_key_range())
      {
         std::cerr << "  base-range: [" << range_bound_hex(base_options.key_lower)
                   << ", "
                   << (base_options.key_upper ? range_bound_hex(base_options.key_upper)
                                              : std::string{"inf"})
                   << ")\n";
      }
      if (base_options.root_index)
         std::cerr << "  root-index: " << *base_options.root_index << "\n";
      if (base_options.max_nodes != 0)
         std::cerr << "  max-nodes/shard: " << base_options.max_nodes << "\n";
      std::cerr.flush();
   }
   else
   {
      std::cout << "Collecting partitioned tree shape stats...\n\n"
                << "Requested shards: " << shard_count << "\n";
      if (base_options.has_key_range())
      {
         std::cout << "Base range: [" << range_bound_hex(base_options.key_lower)
                   << ", "
                   << (base_options.key_upper ? range_bound_hex(base_options.key_upper)
                                              : std::string{"inf"})
                   << ")\n";
      }
      if (base_options.root_index)
         std::cout << "Root index: " << *base_options.root_index << "\n";
      if (base_options.max_nodes != 0)
         std::cout << "Max nodes/shard: " << format_number(base_options.max_nodes) << "\n";
      std::cout << "\n";
      std::cout.flush();
   }

   auto summary = run_tree_stats_shards(db, base_options, shard_count, requested_jobs);

   if (json_output)
   {
      std::cout << tree_stats_partitioned_to_json(summary) << "\n";
      if (db.ref_counts_stale())
         std::cerr << stale_refcount_warning;
      return summary.dangling_pointers != 0 ? 1 : 0;
   }

   std::cout << "-- Partitioned Summary --\n";
   std::cout << "  " << std::setw(30) << std::left << "Active shards"
             << summary.shard_count << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Worker threads"
             << summary.jobs << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Scan truncated"
             << (summary.scan_truncated ? "yes" : "no") << "\n";
   if (summary.max_nodes_per_shard != 0)
      std::cout << "  " << std::setw(30) << std::left << "Max nodes/shard"
                << format_number(summary.max_nodes_per_shard) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Selected keys"
             << format_number(summary.selected_leaf_keys) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Nodes visited (sum)"
             << format_number(summary.nodes_visited) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Leaf nodes (sum)"
             << format_number(summary.leaf_nodes) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Reachable bytes (sum)"
             << format_bytes(summary.reachable_bytes) << "\n";
   std::cout << "  " << std::setw(30) << std::left << "Dangling pointers"
             << format_number(summary.dangling_pointers) << "\n";
   std::cout << "\n  Note: summed shape fields can double-count shared ancestors, "
             << "boundary leaves, and shared subtrees.\n";

   std::cout << "\n-- Shards --\n";
   std::cout << "  " << std::setw(7) << std::left << "Shard"
             << std::setw(12) << std::left << "Begin"
             << std::setw(12) << std::left << "End"
             << std::setw(16) << std::left << "Selected"
             << std::setw(16) << std::left << "Leaves"
             << std::setw(16) << std::left << "Nodes"
             << std::setw(14) << std::left << "Reachable"
             << "Dangling\n";
   for (const auto& shard : summary.shards)
   {
      std::cout << "  " << std::setw(7) << std::left << shard.shard_index
                << std::setw(12) << std::left << range_bound_hex(shard.options.key_lower)
                << std::setw(12) << std::left
                << (shard.options.key_upper ? range_bound_hex(shard.options.key_upper)
                                            : std::string{"inf"})
                << std::setw(16) << std::left
                << format_number(shard.stats.selected_leaf_keys)
                << std::setw(16) << std::left << format_number(shard.stats.leaf_nodes)
                << std::setw(16) << std::left << format_number(shard.stats.nodes_visited)
                << std::setw(14) << std::left << format_bytes(shard.stats.reachable_bytes)
                << format_number(shard.stats.dangling_pointers) << "\n";
   }

   if (db.ref_counts_stale())
      std::cout << stale_refcount_warning;
   return summary.dangling_pointers != 0 ? 1 : 0;
}

// ── main ─────────────────────────────────────────────────────────────────────

int main(int argc, char** argv)
{
   std::string db_dir;
   std::string command;
   std::string recovery_str;
   int64_t     max_db_size_gb = -1;
   uint64_t    prune_floor = 0;
   uint64_t    progress_nodes = 10000000;
   uint64_t    max_nodes = 0;
   std::string range_hex;
   std::string begin_hex;
   std::string end_hex;
   int64_t     root_index = -1;
   uint32_t    range_shards = 1;
   uint32_t    jobs = 0;
	   uint64_t    watch_interval_ms = 1000;
	   uint64_t    watch_samples = 0;
	   uint64_t    rate_samples = 60;
	   bool        json_output = false;
	   bool        dashboard_flag = false;
	   bool        top_flag = false;
	   bool        explain_terms = false;
	   bool        legend_terms = false;

   po::options_description desc("psitricorder — database inspection & repair utility");
   auto                    opt = desc.add_options();
   opt("help,h", "show this help message");
   opt("db-dir,d", po::value<std::string>(&db_dir), "database directory");
	   opt("command", po::value<std::string>(&command)->default_value("info"),
	       "command: info, terms, mfu-watch, dashboard, top, tree-stats, verify, audit-versions, audit-refcounts, defrag, vas-info");
   opt("recovery,r", po::value<std::string>(&recovery_str)->default_value("none"),
       "recovery mode: none, deferred, app_crash, power_loss, full_verify");
   opt("max-db-size", po::value<int64_t>(&max_db_size_gb),
       "cap the virtual address reservation in GiB (e.g. 512 for 512 GiB); "
       "default is the compile-time max (32768 GiB = 32 TiB)");
   opt("prune-floor", po::value<uint64_t>(&prune_floor)->default_value(0),
       "audit-refcounts: oldest version that must remain readable; default is oldest root");
   opt("progress-nodes", po::value<uint64_t>(&progress_nodes)->default_value(10000000),
       "audit-refcounts/tree-stats: print progress every N visited nodes; 0 disables");
   opt("max-nodes", po::value<uint64_t>(&max_nodes)->default_value(0),
       "tree-stats: stop after N visited nodes; in --range-shards mode this is per shard");
   opt("range-hex", po::value<std::string>(&range_hex),
       "tree-stats: scan only keys in hex range BEGIN:END; either side may be empty");
   opt("begin-hex", po::value<std::string>(&begin_hex),
       "tree-stats: inclusive lower key bound as hex bytes");
   opt("end-hex", po::value<std::string>(&end_hex),
       "tree-stats: exclusive upper key bound as hex bytes");
   opt("root-index", po::value<int64_t>(&root_index)->default_value(-1),
       "tree-stats: scan only one top-root index");
   opt("range-shards", po::value<uint32_t>(&range_shards)->default_value(1),
       "tree-stats: split the selected key range into N first-byte shards; "
       "use for parallel sampling, not exact global audits");
   opt("jobs", po::value<uint32_t>(&jobs)->default_value(0),
       "tree-stats --range-shards: worker thread count; default is hardware concurrency");
	   opt("interval-ms", po::value<uint64_t>(&watch_interval_ms)->default_value(1000),
	       "mfu-watch/dashboard: polling interval in milliseconds");
	   opt("samples", po::value<uint64_t>(&watch_samples)->default_value(0),
	       "mfu-watch/dashboard: number of samples to print; 0 means run until interrupted");
	   opt("rate-samples", po::value<uint64_t>(&rate_samples)->default_value(60),
	       "dashboard: rolling samples used for displayed rates; 60 is about one minute at the default interval, 1 is near-instant, 0 disables");
	   opt("dashboard", po::bool_switch(&dashboard_flag),
	       "run the passive real-time dashboard; use with --db-dir");
	   opt("top", po::bool_switch(&top_flag),
	       "alias for --dashboard");
	   opt("explain", po::bool_switch(&explain_terms),
	       "print dashboard/MFU term definitions and exit");
	   opt("legend", po::bool_switch(&legend_terms),
	       "alias for --explain");
	   opt("json", po::bool_switch(&json_output),
	       "tree-stats: emit machine-readable JSON on stdout");

   po::positional_options_description pos;
   pos.add("command", 1);
   pos.add("db-dir", 1);

   po::variables_map vm;
   try
   {
      po::store(po::command_line_parser(argc, argv).options(desc).positional(pos).run(), vm);
	      po::notify(vm);
	   }
   catch (const std::exception& e)
   {
      std::cerr << "Error: " << e.what() << "\n\n" << desc << "\n";
	      return 1;
	   }

	   if ((dashboard_flag || top_flag) && db_dir.empty() && !command.empty() &&
	       command != "info" && command != "dashboard" && command != "top")
	      db_dir = command;
	   if (dashboard_flag || top_flag)
	      command = "dashboard";

   if (explain_terms || legend_terms || command == "terms" || command == "legend")
   {
      print_psitricorder_terms();
      return 0;
   }

   // vas-info doesn't need a db-dir
   if (command == "vas-info" && db_dir.empty())
   {
      cmd_vas_info();
      return 0;
   }

   if (vm.count("help") || db_dir.empty())
   {
      std::cout << "Usage: psitricorder [command] <db-dir> [options]\n\n"
                << "Commands:\n"
	                << "  info       Show database size summary (default)\n"
	                << "  terms      Define dashboard and MFU monitoring terms\n"
	                << "  mfu-watch  Passively watch MFU difficulty and promotion rate from mmap'd header\n"
	                << "  dashboard  Real-time passive cache/compactor dashboard; top is an alias\n"
	                << "  tree-stats Report tree shape, density histograms, and retained version count\n"
                << "  verify     Full integrity verification (offline)\n"
                << "  audit-versions\n"
                << "             Scan version control blocks and validate the dead-version index\n"
                << "  audit-refcounts\n"
                << "             Full reachable tree scan for version references and retention debt\n"
                << "  defrag     Compact and truncate to minimum size\n"
                << "  vas-info   Query available virtual address space (no db-dir needed)\n"
                << "\n"
                << desc << "\n";
      return db_dir.empty() ? 1 : 0;
   }

   if (!fs::exists(db_dir))
   {
      std::cerr << "Error: database directory does not exist: " << db_dir << "\n";
      return 1;
   }

   runtime_config cfg;
   if (max_db_size_gb > 0)
      cfg.max_database_size = max_db_size_gb * 1024LL * 1024 * 1024;

   try
   {
	      if (command == "mfu-watch")
	         return cmd_mfu_watch(db_dir, watch_interval_ms, watch_samples);
	      if (command == "dashboard" || command == "top")
	         return cmd_dashboard(db_dir, watch_interval_ms, watch_samples, rate_samples);

      auto mode = parse_recovery(recovery_str);
      auto db   = database::open(db_dir, open_mode::open_existing, cfg, mode);

      if (command == "info")
         cmd_info(*db, db_dir);
      else if (command == "tree-stats")
      {
         auto options = make_tree_stats_options(range_hex, begin_hex, end_hex,
                                                root_index, progress_nodes, max_nodes);
         if (range_shards > 1)
            return cmd_tree_stats_shards(*db, json_output, options, range_shards, jobs);
         return cmd_tree_stats(*db, json_output, options);
      }
      else if (command == "verify")
         return cmd_verify(*db);
      else if (command == "audit-versions")
         return cmd_audit_versions(*db);
      else if (command == "audit-refcounts")
         return cmd_audit_refcounts(*db, prune_floor, progress_nodes);
      else if (command == "defrag")
         cmd_defrag(*db, db_dir);
	      else if (command == "vas-info")
	         cmd_vas_info();  // also works with a db-dir positional arg
	      else
	      {
	         std::cerr << "Unknown command: " << command << "\n";
	         std::cerr << "Valid commands: info, terms, mfu-watch, dashboard, top, tree-stats, verify, audit-versions, audit-refcounts, defrag, vas-info\n";
	         return 1;
	      }
   }
   catch (const std::exception& e)
   {
      std::cerr << "Error: " << e.what() << "\n";
      return 1;
   }

   return 0;
}
