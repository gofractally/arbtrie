#include <errno.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

// Utility function to format size in human-readable format
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

// Test a specific reservation size
bool test_reservation(uint64_t size)
{
   void* ptr = ::mmap(nullptr,                      // Let the system choose the address
                      size,                         // Size to reserve
                      PROT_NONE,                    // No access permissions initially
                      MAP_PRIVATE | MAP_ANONYMOUS,  // Private anonymous mapping
                      -1,                           // No file descriptor for anonymous mapping
                      0                             // No offset
   );

   bool success = (ptr != MAP_FAILED);

   if (success)
   {
      std::cout << "✅ Successfully reserved " << format_size(size) << " at address " << ptr
                << std::endl;

      // Free the mapping immediately to avoid running out of address space
      ::munmap(ptr, size);
   }
   else
   {
      std::cout << "❌ Failed to reserve " << format_size(size) << ": " << strerror(errno)
                << std::endl;
   }

   return success;
}

int main()
{
   // Get system page size
   long page_size = sysconf(_SC_PAGESIZE);
   std::cout << "System page size: " << format_size(page_size) << std::endl;

   // Get system info
   std::cout << "Testing maximum virtual memory reservation..." << std::endl;
   std::cout << "----------------------------------------" << std::endl;

   // Start with 1 GB and work up to large sizes
   uint64_t sizes[] = {
       1ULL << 30,    // 1 GB
       10ULL << 30,   // 10 GB
       100ULL << 30,  // 100 GB
       1ULL << 40,    // 1 TB
       2ULL << 40,    // 2 TB
       4ULL << 40,    // 4 TB
       8ULL << 40,    // 8 TB
       16ULL << 40,   // 16 TB
       32ULL << 40,   // 32 TB
       64ULL << 40,   // 64 TB
       128ULL << 40,  // 128 TB
       256ULL << 40,  // 256 TB
   };

   // Test each predefined size
   std::cout << "Testing predefined sizes:" << std::endl;
   for (uint64_t size : sizes)
   {
      test_reservation(size);
   }

   // Binary search to find the exact maximum reservation size
   std::cout << "\nFinding exact maximum reservation size using binary search:" << std::endl;
   uint64_t low            = 1ULL << 30;    // Start at 1 GB
   uint64_t high           = 512ULL << 40;  // Go up to 512 TB
   uint64_t max_successful = 0;

   while (low <= high)
   {
      uint64_t mid = low + (high - low) / 2;

      // Round to page size
      mid = (mid / page_size) * page_size;

      if (test_reservation(mid))
      {
         max_successful = mid;
         low            = mid + page_size;
      }
      else
      {
         high = mid - page_size;
      }

      // If we're close enough, stop the search
      if (high - low < page_size * 2)
         break;
   }

   if (max_successful > 0)
   {
      std::cout << "\nMaximum successful reservation: " << format_size(max_successful) << std::endl;
   }
   else
   {
      std::cout << "\nCould not determine maximum reservation size." << std::endl;
   }

   return 0;
}