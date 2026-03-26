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

// ── main ─────────────────────────────────────────────────────────────────────

int main(int argc, char** argv)
{
   std::string db_dir;
   std::string command;
   std::string recovery_str;

   po::options_description desc("psitri-tool — database inspection & defrag utility");
   auto                    opt = desc.add_options();
   opt("help,h", "show this help message");
   opt("db-dir,d", po::value<std::string>(&db_dir), "database directory");
   opt("command", po::value<std::string>(&command)->default_value("info"),
       "command: info, defrag");
   opt("recovery,r", po::value<std::string>(&recovery_str)->default_value("none"),
       "recovery mode: none, deferred, app_crash, power_loss, full_verify");

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

   if (vm.count("help") || db_dir.empty())
   {
      std::cout << "Usage: psitri-tool [command] <db-dir> [options]\n\n"
                << "Commands:\n"
                << "  info       Show database size summary (default)\n"
                << "  defrag     Compact and truncate to minimum size\n"
                << "\n"
                << desc << "\n";
      return db_dir.empty() ? 1 : 0;
   }

   if (!fs::exists(db_dir))
   {
      std::cerr << "Error: database directory does not exist: " << db_dir << "\n";
      return 1;
   }

   try
   {
      auto mode = parse_recovery(recovery_str);
      auto db   = std::make_shared<database>(db_dir, runtime_config{}, mode);

      if (command == "info")
         cmd_info(*db, db_dir);
      else if (command == "defrag")
         cmd_defrag(*db, db_dir);
      else
      {
         std::cerr << "Unknown command: " << command << "\n";
         std::cerr << "Valid commands: info, defrag\n";
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
