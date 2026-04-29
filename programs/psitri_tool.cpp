#include <filesystem>
#include <iomanip>
#include <iostream>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <exception>
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
#include <psitri/read_session_impl.hpp>
#include <psitri/write_cursor.hpp>
#include <sal/block_allocator.hpp>
#include <sal/config.hpp>

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
   print_histogram("Branches Per Inner Node", s.branches_per_inner_node, "Branches");
   print_histogram("Keys Per Leaf", s.keys_per_leaf, "Keys");
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
   bool        json_output = false;

   po::options_description desc("psitricorder — database inspection & repair utility");
   auto                    opt = desc.add_options();
   opt("help,h", "show this help message");
   opt("db-dir,d", po::value<std::string>(&db_dir), "database directory");
   opt("command", po::value<std::string>(&command)->default_value("info"),
       "command: info, tree-stats, verify, audit-versions, audit-refcounts, defrag, vas-info");
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
         std::cerr << "Valid commands: info, tree-stats, verify, audit-versions, audit-refcounts, defrag, vas-info\n";
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
