#include <bit>  // Add this for std::countl_zero and std::popcount
#include <sal/block_allocator.hpp>
#include <sal/debug.hpp>

namespace sal
{
   uint64_t block_allocator::find_max_reservation_size(uint64_t block_size)
   {
      if (block_size == 0)
      {
         SAL_WARN("find_max_reservation_size called with block_size 0");
         return 0;
      }

      if (!is_power_of_2(block_size))
      {
         SAL_ERROR("find_max_reservation_size called with non-power-of-2 block_size: {}",
                   block_size);
         throw std::invalid_argument("block_size must be a power of 2");
      }

      // Set maximum search limit (will be adjusted down to a multiple of block_size)
      // Start with a reasonable upper limit based on current hardware capabilities
      /// (48 bit address space)
      constexpr uint64_t max_limit = 256ULL << 40;  // 256 TB
      uint64_t           high      = (max_limit / block_size) * block_size;

      // Set minimum search limit
      uint64_t low = block_size;  // At least one block

      // Track the largest successful reservation
      uint64_t max_successful = 0;

      sal_debug("Starting binary search for max reservation size with block_size={}", block_size);

      // Binary search for maximum reservation size
      while (low <= high)
      {
         uint64_t mid = low + ((high - low) / 2);

         // Adjust to a multiple of block_size
         mid = (mid / block_size) * block_size;
         if (mid < low)
            mid = low;

         sal_debug("Trying reservation of size {}", mid);

         // Try to reserve this amount of memory
         void* ptr = ::mmap(nullptr,                      // Let the system choose the address
                            mid,                          // Size to reserve
                            PROT_NONE,                    // No access permissions initially
                            MAP_PRIVATE | MAP_ANONYMOUS,  // Private anonymous mapping
                            -1,  // No file descriptor for anonymous mapping
                            0    // No offset
         );

         if (ptr != MAP_FAILED)
         {
            // Success - this size works
            max_successful = mid;
            sal_debug("Successfully reserved {} bytes", mid);

            // Release the mapping before trying a larger size
            ::munmap(ptr, mid);

            // Try a larger size
            low = mid + block_size;
         }
         else
         {
            // Failed - try a smaller size
            sal_debug("Failed to reserve {} bytes: {}", mid, strerror(errno));
            high = mid - block_size;
         }
      }

      if (max_successful > 0)
      {
         sal_debug("Maximum reservation size found: {} bytes ({} blocks)", max_successful,
                   max_successful / block_size);
         return max_successful;
      }

      // If all failed, return a minimal safe value
      SAL_WARN("Could not find any working reservation size, returning minimal size");
      return block_size;  // At least one block
   }

   block_allocator::block_allocator(std::filesystem::path file,
                                    uint64_t              block_size,
                                    uint32_t              max_blocks,
                                    bool                  read_write)
       : _filename(file), _block_size(block_size)
   {
      // Validate that block_size is a power of 2
      if (!is_power_of_2(block_size))
      {
         SAL_ERROR("block_allocator constructor {} called with non-power-of-2 block_size: {}",
                   file.native(), block_size);
         throw std::invalid_argument("block_size must be a power of 2");
      }

      // Calculate log2 of block_size for efficient addressing using countl_zero
      // For a power of 2 number, log2 is (bit width - leading zeros - 1)
      _log2_block_size = sizeof(block_size) * 8 - std::countl_zero(block_size) - 1;

      // Verify our calculation is correct
      assert((1ULL << _log2_block_size) == block_size);

      _max_blocks       = max_blocks;
      _reserved_base    = nullptr;
      _reservation_size = 0;
      _mapped_base      = nullptr;

      // Reserve the maximum virtual address space early to ensure contiguity
      uint64_t max_potential_size = static_cast<uint64_t>(max_blocks) * block_size;

      if (max_potential_size > 0)
      {
         // Try to reserve the entire potential address space without mapping files
         _reserved_base =
             ::mmap(nullptr,                      // Let the system choose the address
                    max_potential_size,           // Reserve enough space for all potential blocks
                    PROT_NONE,                    // No access permissions initially
                    MAP_PRIVATE | MAP_ANONYMOUS,  // Private anonymous mapping (not backed by file)
                    -1,                           // No file descriptor for anonymous mapping
                    0                             // No offset
             );

         if (_reserved_base != MAP_FAILED)
         {
            _reservation_size = max_potential_size;
            sal_debug("Reserved contiguous address space: base={:#x}, size={}",
                      reinterpret_cast<uintptr_t>(_reserved_base), _reservation_size);
         }
         else
         {
            // Reservation failed - we require this for proper operation
            SAL_ERROR("Failed to reserve address space of size {}: {}", max_potential_size,
                      strerror(errno));
            throw std::runtime_error("Failed to reserve contiguous address space");
         }
      }

      int flags = O_CLOEXEC;
      int flock_operation;
      if (read_write)
      {
         flags |= O_RDWR;
         flags |= O_CREAT;
         flock_operation = LOCK_EX;
      }
      else
      {
         flags |= O_RDONLY;
         flock_operation = LOCK_SH;
      }

      _fd = ::open(file.native().c_str(), flags, 0644);
      if (_fd == -1)
      {
         // Clean up the reservation before throwing
         if (_reserved_base && _reserved_base != MAP_FAILED)
         {
            ::munmap(_reserved_base, _reservation_size);
         }

         sal_debug("Opening file: {}", file.native());
         throw std::runtime_error("unable to open block file");
      }

      if (::flock(_fd, flock_operation | LOCK_NB) != 0)
      {
         // Clean up the reservation before throwing
         if (_reserved_base && _reserved_base != MAP_FAILED)
         {
            ::munmap(_reserved_base, _reservation_size);
         }

         ::close(_fd);
         throw std::system_error{errno, std::generic_category()};
      }

      struct stat statbuf[1];
      if (::fstat(_fd, statbuf) != 0)
      {
         // Clean up the reservation before throwing
         if (_reserved_base && _reserved_base != MAP_FAILED)
         {
            ::munmap(_reserved_base, _reservation_size);
         }

         ::close(_fd);
         throw std::system_error{errno, std::generic_category()};
      }

      _file_size = statbuf->st_size;
      if (_file_size % block_size != 0)
      {
         // Clean up the reservation before throwing
         if (_reserved_base && _reserved_base != MAP_FAILED)
         {
            ::munmap(_reserved_base, _reservation_size);
         }

         ::close(_fd);
         throw std::runtime_error("block file isn't a multiple of block size");
      }

      if (_file_size)
      {
         auto prot = PROT_READ | PROT_WRITE;

         // Map the file at the beginning of our reserved space
         // Release the protection on the portion we need
         if (::munmap(_reserved_base, _file_size) != 0)
         {
            SAL_WARN("Failed to release protection on reserved space: {}", strerror(errno));
            // Continue anyway, trying to map
         }

         // Map the file at the exact address of our reserved space
         _mapped_base = ::mmap(_reserved_base, _file_size, prot, MAP_SHARED | MAP_FIXED, _fd, 0);

         if (_mapped_base != MAP_FAILED)
         {
            // Successfully mapped the file at the beginning of our reserved space
            sal_debug("Successfully mapped existing file at reserved space: base={:#x}, size={}",
                      reinterpret_cast<uintptr_t>(_mapped_base), _file_size);

            // Calculate how many blocks we have
            uint32_t block_count = _file_size / _block_size;
            _num_blocks.store(block_count, std::memory_order_release);
         }
         else
         {
            // Mapping failed - clean up and throw
            SAL_ERROR("Failed to map file at reserved space: {}", strerror(errno));

            if (_reserved_base && _reserved_base != MAP_FAILED)
            {
               ::munmap(_reserved_base, _reservation_size);
            }

            ::close(_fd);
            throw std::runtime_error("Failed to map file at reserved address space");
         }
      }
   }

   block_allocator::~block_allocator()
   {
      if (_fd)
      {
         // Unmap the entire file at once if we have a mapped base
         if (_mapped_base && _mapped_base != MAP_FAILED && _file_size > 0)
         {
            ::munmap(_mapped_base, _file_size);
            _mapped_base = nullptr;
         }

         ::close(_fd);
      }

      // Release the reserved address space if we had one
      if (_reserved_base && _reserved_base != MAP_FAILED && _reservation_size > 0)
      {
         ::munmap(_reserved_base, _reservation_size);
         _reserved_base    = nullptr;
         _reservation_size = 0;
      }
   }

   void block_allocator::sync(sync_type st)
   {
      if (_fd and sync_type::none != st)
      {
         // We can sync the entire mapped region at once
         if (_mapped_base && _mapped_base != MAP_FAILED && _file_size > 0)
         {
            ::msync(_mapped_base, _file_size, msync_flag(st));
         }
      }
   }

   uint32_t block_allocator::reserve(uint32_t desired_num_blocks, bool memlock)
   {
      if (desired_num_blocks > _max_blocks)
         throw std::runtime_error("unable to reserve, maximum block would be reached");

      std::lock_guard l{_resize_mutex};
      if (num_blocks() >= desired_num_blocks)
         return desired_num_blocks;

      auto cur_num   = num_blocks();
      auto add_count = desired_num_blocks - cur_num;
      auto new_size  = _file_size + _block_size * add_count;

      // Resize the file
      if (::ftruncate(_fd, new_size) < 0)
      {
         throw std::system_error(errno, std::generic_category());
      }

      auto prot = PROT_READ | PROT_WRITE;

      // If this is the first allocation, map the file at the reserved space
      if (cur_num == 0)
      {
         // Release the protection on the portion we need
         if (::munmap(_reserved_base, new_size) != 0)
         {
            SAL_WARN("Failed to release protection on reserved space: {}", strerror(errno));
            // Continue anyway, trying to map
         }

         // Map the file at the exact address of our reserved space
         void* addr = ::mmap(_reserved_base, new_size, prot, MAP_SHARED | MAP_FIXED, _fd, 0);

         if (addr != MAP_FAILED)
         {
            // Successfully mapped the file at the beginning of our reserved space
            _mapped_base = addr;

            if (memlock)
            {
               if (::mlock(addr, new_size))
               {
                  SAL_WARN("Unable to mlock memory region");
                  ::madvise(addr, new_size, MADV_RANDOM);
               }
            }

            sal_debug("Reserved {} blocks contiguously at base={:#x}", desired_num_blocks,
                      reinterpret_cast<uintptr_t>(addr));

            _file_size = new_size;
            return _num_blocks.fetch_add(add_count, std::memory_order_release) + add_count;
         }
         else
         {
            SAL_ERROR("Failed to map file at reserved space: {}", strerror(errno));

            // Revert file size and throw
            if (::ftruncate(_fd, _file_size) < 0)
            {
               SAL_ERROR("Additionally failed to restore file size");
            }
            throw std::runtime_error("unable to map file into reserved space");
         }
      }
      else
      {
         // For cases where we already have a mapped region, we need to map just the new blocks
         // DO NOT unmap the existing region to avoid invalidating existing pointers

         // Calculate address and size of the new region to map
         void*  new_region_addr = static_cast<char*>(_reserved_base) + _file_size;
         size_t new_region_size = new_size - _file_size;

         // Release protection on just the new portion we need
         if (::munmap(new_region_addr, new_region_size) != 0)
         {
            SAL_WARN("Failed to release protection on reserved space for extension: {}",
                     strerror(errno));
            // Continue anyway, trying to map
         }

         // Map just the new portion of the file at the correct address to maintain contiguity
         void* addr = ::mmap(new_region_addr, new_region_size, prot, MAP_SHARED | MAP_FIXED, _fd,
                             _file_size);

         if (addr != MAP_FAILED)
         {
            // Successfully extended the mapping contiguously
            if (memlock)
            {
               if (::mlock(addr, new_region_size))
               {
                  SAL_WARN("Unable to mlock extended memory region");
                  ::madvise(addr, new_region_size, MADV_RANDOM);
               }
            }

            sal_debug("Mapped {} additional blocks contiguously at base={:#x}", add_count,
                      reinterpret_cast<uintptr_t>(addr));

            _file_size = new_size;
            return _num_blocks.fetch_add(add_count, std::memory_order_release) + add_count;
         }
         else
         {
            SAL_ERROR("Failed to map extension at expected address: {}", strerror(errno));

            // Revert file size and throw
            if (::ftruncate(_fd, _file_size) < 0)
            {
               SAL_ERROR("Failed to restore file size after mapping failure");
            }

            throw std::runtime_error("Failed to extend mapping at expected address");
         }
      }
   }

   block_allocator::offset_ptr block_allocator::alloc()
   {
      std::lock_guard l{_resize_mutex};
      auto            nb = _num_blocks.load(std::memory_order_relaxed);
      if (nb == _max_blocks)
         throw std::runtime_error("maximum block number reached");

      auto new_size = _file_size + _block_size;

      // Resize the file
      if (::ftruncate(_fd, new_size) < 0)
      {
         throw std::system_error(errno, std::generic_category());
      }

      auto prot = PROT_READ | PROT_WRITE;

      // For cases where we already have a mapped region, we need to map just the new block
      if (_mapped_base && _mapped_base != MAP_FAILED)
      {
         // DO NOT unmap the existing region to avoid invalidating existing pointers

         // Calculate address of the new block to map
         void* new_block_addr = static_cast<char*>(_reserved_base) + _file_size;

         // Release protection on just the new block we need
         if (::munmap(new_block_addr, _block_size) != 0)
         {
            SAL_WARN("Failed to release protection on reserved space for alloc: {}",
                     strerror(errno));
            // Continue anyway, trying to map
         }

         // Map only the new block at the expected address
         void* addr =
             ::mmap(new_block_addr, _block_size, prot, MAP_SHARED | MAP_FIXED, _fd, _file_size);

         if (addr != MAP_FAILED)
         {
            offset_ptr offset = _file_size;  // Offset to the newly allocated block
            _file_size        = new_size;

            sal_debug("Alloc: Mapped new block contiguously at base={:#x}",
                      reinterpret_cast<uintptr_t>(addr));

            _num_blocks.fetch_add(1, std::memory_order_release);
            return offset;
         }
         else
         {
            SAL_ERROR("Alloc: Failed to map new block at expected address: {}", strerror(errno));

            // Revert file size and throw
            if (::ftruncate(_fd, _file_size) < 0)
            {
               SAL_ERROR("Failed to restore file size after mapping failure");
            }

            throw std::runtime_error("Failed to map new block at expected address");
         }
      }
      else  // First mapping
      {
         // Release the protection on the portion we need
         if (::munmap(_reserved_base, new_size) != 0)
         {
            SAL_WARN("Failed to release protection on reserved space for first alloc: {}",
                     strerror(errno));
            // Continue anyway, trying to map
         }

         // Map the file at the exact address of our reserved space
         void* addr = ::mmap(_reserved_base, new_size, prot, MAP_SHARED | MAP_FIXED, _fd, 0);

         if (addr != MAP_FAILED)
         {
            // Successfully mapped the file at the beginning of our reserved space
            _mapped_base = addr;
            _file_size   = new_size;

            sal_debug("Alloc: First block mapped contiguously at base={:#x}",
                      reinterpret_cast<uintptr_t>(addr));

            // For the first block, offset is 0
            _num_blocks.fetch_add(1, std::memory_order_release);
            return 0;
         }
         else
         {
            SAL_ERROR("Alloc: Failed to map file at reserved space for first alloc: {}",
                      strerror(errno));

            // Revert file size
            if (::ftruncate(_fd, _file_size) < 0)
            {
               SAL_ERROR("Failed to restore file size after mapping failure");
            }

            throw std::runtime_error("Failed to map first block at reserved address space");
         }
      }
   }
}  // namespace sal