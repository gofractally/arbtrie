#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>

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

namespace po = boost::program_options;
namespace fs = std::filesystem;
using namespace psitri;

// ── helpers ──────────────────────────────────────────────────────────────────

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
      std::cout << "\n  Warning: ref counts stale (deferred_cleanup recovery pending)\n";
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
      std::cout << "\n  Warning: ref counts stale (deferred_cleanup recovery pending)\n";

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

// ── main ─────────────────────────────────────────────────────────────────────

int main(int argc, char** argv)
{
   std::string db_dir;
   std::string command;
   std::string recovery_str;
   int64_t     max_db_size_gb = -1;

   po::options_description desc("psitri-tool — database inspection & defrag utility");
   auto                    opt = desc.add_options();
   opt("help,h", "show this help message");
   opt("db-dir,d", po::value<std::string>(&db_dir), "database directory");
   opt("command", po::value<std::string>(&command)->default_value("info"),
       "command: info, verify, defrag, vas-info");
   opt("recovery,r", po::value<std::string>(&recovery_str)->default_value("none"),
       "recovery mode: none, deferred, app_crash, power_loss, full_verify");
   opt("max-db-size", po::value<int64_t>(&max_db_size_gb),
       "cap the virtual address reservation in GiB (e.g. 512 for 512 GiB); "
       "default is the compile-time max (32768 GiB = 32 TiB)");

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
      std::cout << "Usage: psitri-tool [command] <db-dir> [options]\n\n"
                << "Commands:\n"
                << "  info       Show database size summary (default)\n"
                << "  verify     Full integrity verification (offline)\n"
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
      auto db   = std::make_shared<database>(db_dir, cfg, mode);

      if (command == "info")
         cmd_info(*db, db_dir);
      else if (command == "verify")
         return cmd_verify(*db);
      else if (command == "defrag")
         cmd_defrag(*db, db_dir);
      else if (command == "vas-info")
         cmd_vas_info();  // also works with a db-dir positional arg
      else
      {
         std::cerr << "Unknown command: " << command << "\n";
         std::cerr << "Valid commands: info, verify, defrag, vas-info\n";
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
