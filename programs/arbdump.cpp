#include <algorithm>
#include <arbtrie/database.hpp>
#include <boost/program_options.hpp>
#include <iomanip>
#include <iostream>
#include <sal/shared_ptr_alloc.hpp>
#include <vector>

// Helper function to print top N regions by usage
void print_top_regions(const std::string& ptr_dir, int top_n)
{
   try
   {
      sal::shared_ptr_alloc allocator(ptr_dir);
      auto                  stats = allocator.region_stats();

      // Sort stats by usage count in descending order
      std::sort(stats.begin(), stats.end(),
                [](const auto& a, const auto& b) { return a.use > b.use; });

      // Determine how many regions to print
      int num_to_print = std::min((int)stats.size(), top_n);

      std::cout << "Top " << num_to_print << " Regions by Shared Pointer Usage (from " << ptr_dir
                << "):\n";
      std::cout << "-------------------------\n";
      std::cout << std::left << std::setw(10) << "Region" << std::right << std::setw(10) << "Usage"
                << "\n";
      std::cout << "-------------------------\n";

      for (int i = 0; i < num_to_print; ++i)
      {
         if (stats[i].use > 0)
         {
            std::cout << std::left << std::setw(10) << *stats[i].region << std::right
                      << std::setw(10) << stats[i].use << "\n";
         }
         else
         {
            // If we hit zero usage, stop printing as they are sorted
            break;
         }
      }
      std::cout << "-------------------------\n";

      // Get and print the region usage summary
      auto summary = allocator.get_region_usage_summary();
      std::cout << "\nRegion Usage Summary:\n";
      std::cout << "  Non-empty Regions: " << summary.count << "\n";
      if (summary.count > 0)
      {
         std::cout << "  Min Usage:         " << summary.min << "\n";
         std::cout << "  Max Usage:         " << summary.max << "\n";
         std::cout << "  Mean Usage:        " << std::fixed << std::setprecision(2) << summary.mean
                   << "\n";
         std::cout << "  Std Dev Usage:     " << std::fixed << std::setprecision(2)
                   << summary.stddev << "\n";
      }
      std::cout << "  Total Usage:       " << summary.total_usage << "\n";
      std::cout << "-------------------------\n";
   }
   catch (const std::exception& e)
   {
      std::cerr << "Error processing shared_ptr_alloc data: " << e.what() << "\n";
      // Consider if exiting here is the desired behavior or if main should handle it.
      // For now, we just print the error like before.
   }
}

int main(int argc, char** argv)
{
   namespace po = boost::program_options;
   std::string             dir;
   std::string             ptr_dir;
   int                     top_n;
   po::options_description desc("arbdump options");
   desc.add_options()("help,h", "Print help message")(
       "dir", po::value<std::string>()->default_value("arbtriedb"),
       "directory of database (used if --ptr-dir is not specified)")(
       "ptr-dir", po::value<std::string>(&ptr_dir),
       "directory of shared_ptr_alloc (if specified, prints region stats instead of DB stats)")(
       "top-regions", po::value<int>(&top_n)->default_value(20),
       "Number of top regions to display by usage");

   po::variables_map vm;
   po::store(po::parse_command_line(argc, argv, desc), vm);
   po::notify(vm);

   if (vm.count("help"))
   {
      std::cout << desc << "\n";
      return 0;
   }

   if (vm.count("ptr-dir"))
   {
      print_top_regions(ptr_dir, top_n);  // Call helper function
   }
   else
   {
      try
      {
         arbtrie::database db(vm["dir"].as<std::string>(), arbtrie::runtime_config());
         db.print_stats(std::cout);
      }
      catch (const std::exception& e)
      {
         std::cerr << "Error opening or processing database: " << e.what() << "\n";
         return 1;
      }
   }

   return 0;
}
