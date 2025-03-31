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

      while (max_potential_size > 0)
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
            SAL_INFO("Reserved contiguous address space:  size={}", _reservation_size);
            break;
         }
         else
         {
            // Reservation failed - we require this for proper operation
            SAL_ERROR("Failed to reserve address space of size {}: {}, trying lower max",
                      max_potential_size, strerror(errno));
            max_potential_size /= 2;
            //throw std::runtime_error("Failed to reserve contiguous address space");
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

      _file_size.store(statbuf->st_size, std::memory_order_relaxed);
      if (_file_size.load(std::memory_order_relaxed) % block_size != 0)
      {
         // Clean up the reservation before throwing
         if (_reserved_base && _reserved_base != MAP_FAILED)
         {
            ::munmap(_reserved_base, _reservation_size);
         }

         ::close(_fd);
         throw std::runtime_error("block file isn't a multiple of block size");
      }

      if (_file_size.load(std::memory_order_relaxed) > 0)
      {
         auto prot = PROT_READ | PROT_WRITE;

         // Map the file at the beginning of our reserved space
         // Release the protection on the portion we need
         if (::munmap(_reserved_base, _file_size.load(std::memory_order_relaxed)) != 0)
         {
            SAL_WARN("Failed to release protection on reserved space: {}", strerror(errno));
            // Continue anyway, trying to map
         }

         // Map the file at the exact address of our reserved space
         _mapped_base = ::mmap(_reserved_base, _file_size.load(std::memory_order_relaxed), prot,
                               MAP_SHARED | MAP_FIXED, _fd, 0);

         if (_mapped_base != MAP_FAILED)
         {
            // Successfully mapped the file at the beginning of our reserved space
            sal_debug("Successfully mapped existing file at reserved space: base={:#x}, size={}",
                      reinterpret_cast<uintptr_t>(_mapped_base),
                      _file_size.load(std::memory_order_relaxed));

            // Calculate how many blocks we have
            uint32_t block_count = _file_size.load(std::memory_order_relaxed) / _block_size;
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
         if (_mapped_base && _mapped_base != MAP_FAILED &&
             _file_size.load(std::memory_order_relaxed) > 0)
         {
            ::munmap(_mapped_base, _file_size.load());
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

   void block_allocator::fsync(bool full)
   {
      if (_fd)
      {
#ifdef __APPLE__
         if (full)
         {
            if (fcntl(_fd, F_FULLFSYNC) < 0)
            {
               SAL_ERROR("Failed to fsync file: {}", strerror(errno));
               throw std::system_error(errno, std::generic_category());
            }
            return;
         }
#endif
         if (::fsync(_fd) < 0)
         {
            SAL_ERROR("Failed to fsync file: {}", strerror(errno));
            throw std::system_error(errno, std::generic_category());
         }
      }
   }

   uint32_t block_allocator::reserve(uint32_t desired_num_blocks, bool mlock)
   {
      if (desired_num_blocks > _max_blocks)
         throw std::runtime_error("unable to reserve, maximum block would be reached");

      std::lock_guard l{_resize_mutex};

      // Calculate current capacity in blocks (how many blocks the file can hold)
      uint32_t capacity = _file_size.load(std::memory_order_relaxed) / _block_size;

      // Check if we already have enough space reserved in the file
      if (capacity >= desired_num_blocks)
         return capacity;

      auto add_count = desired_num_blocks - capacity;
      auto new_size  = _file_size.load(std::memory_order_relaxed) + _block_size * add_count;

      // Resize the file
      if (::ftruncate(_fd, new_size) < 0)
      {
         throw std::system_error(errno, std::generic_category());
      }

      auto prot = PROT_READ | PROT_WRITE;

      // Calculate where to map
      void*  map_addr   = static_cast<char*>(_reserved_base) + _file_size;
      size_t map_size   = new_size - _file_size;
      off_t  map_offset = _file_size;

      // Release the protection on the portion we need
      if (::munmap(map_addr, map_size) != 0)
      {
         SAL_WARN("Failed to release protection on reserved space: {}", strerror(errno));
         // Continue anyway, trying to map
      }

      // Map the file at the specified address
      void* addr = ::mmap(map_addr, map_size, prot, MAP_SHARED | MAP_FIXED, _fd, map_offset);

      if (addr != MAP_FAILED)
      {
         if (mlock)
         {
            if (::mlock(addr, map_size) != 0)
            {
               SAL_ERROR("Failed to mlock file: {}", strerror(errno));
            }
         }
         // Successfully mapped the file
         if (capacity == 0)
         {
            // First allocation: save the base pointer
            _mapped_base = addr;
         }

         // Update file size (increases capacity)
         _file_size = new_size;

         // Return the new capacity (not num_blocks)
         return desired_num_blocks;
      }
      else
      {
         SAL_ERROR("Failed to map file at expected address: {}", strerror(errno));

         // Revert file size and throw
         if (::ftruncate(_fd, _file_size) < 0)
         {
            SAL_ERROR("Additionally failed to restore file size");
         }
         throw std::runtime_error("unable to map file at expected address");
      }
   }

   std::pair<block_allocator::block_number, block_allocator::offset_ptr> block_allocator::alloc()
   {
      // Atomically claim the next block index without a lock
      uint32_t block_index = _num_blocks.fetch_add(1, std::memory_order_acquire);

      // Check if we hit the max blocks limit
      if (block_index >= _max_blocks)
      {
         // Undo the fetch_add to restore consistent state
         _num_blocks.fetch_sub(1, std::memory_order_release);
         sal_debug("ALLOC: Max blocks reached - _max_blocks={}", _max_blocks);
         throw std::runtime_error("maximum block number reached");
      }

      // Check if we already have this block mapped
      if (block_index < (_file_size / _block_size))
      {
         // Fast path: block already mapped, return the block number and offset
         offset_ptr offset(block_index * _block_size);
         return {block_number(block_index), offset};
      }

      // Slow path: need to map a new block
      try
      {
         // Call reserve() to map more space (with the lock)
         reserve(block_index + 1);

         // After reserve succeeds, our previously claimed block_index is valid
         offset_ptr offset(block_index * _block_size);

         return {block_number(block_index), offset};
      }
      catch (const std::exception& e)
      {
         // If reserve fails, restore consistent state
         _num_blocks.fetch_sub(1, std::memory_order_release);
         sal_debug("ALLOC: Slow path failed - {}", e.what());
         throw;  // Re-throw the original exception
      }
   }

   uint32_t block_allocator::resize(uint32_t desired_num_blocks)
   {
      if (desired_num_blocks > _max_blocks)
         throw std::runtime_error("unable to resize, maximum block limit would be exceeded");

      // First ensure we have enough capacity (reserve handles its own locking)
      reserve(desired_num_blocks);

      // Directly set the number of blocks
      // NOTE: This method is not thread safe with simultaneous use of alloc()
      _num_blocks.store(desired_num_blocks, std::memory_order_release);

      return desired_num_blocks;
   }

   void block_allocator::truncate(uint32_t nblocks)
   {
      std::lock_guard l{_resize_mutex};

      // Check if the requested size is greater than max_blocks
      if (nblocks > _max_blocks)
         throw std::runtime_error("cannot truncate to size larger than max_blocks");

      // Calculate the new file size
      uint64_t new_size = static_cast<uint64_t>(nblocks) * _block_size;

      // If the new size is larger than current, use reserve instead
      if (new_size > _file_size)
      {
         // Release the lock and call reserve
         l.~lock_guard();
         reserve(nblocks);
         return;
      }

      // If new size is the same as current, just update num_blocks if needed
      if (new_size == _file_size)
      {
         uint32_t current_blocks = _num_blocks.load(std::memory_order_acquire);
         if (current_blocks != nblocks)
         {
            _num_blocks.store(nblocks, std::memory_order_release);
         }
         return;
      }

      // For shrinking, we need to:
      // 1. Unmap the portion we're going to remove
      // 2. Truncate the file
      // 3. Update our state variables

      // Unmap the part we're removing
      void*  unmap_start = static_cast<char*>(_mapped_base) + new_size;
      size_t unmap_size  = _file_size - new_size;

      if (::munmap(unmap_start, unmap_size) != 0)
      {
         SAL_WARN("Failed to unmap portion of file during truncate: {}", strerror(errno));
         // Continue anyway - the file will still be truncated
      }

      // Truncate the file to the new size
      if (::ftruncate(_fd, new_size) < 0)
      {
         // If truncation fails, we need to remap the unmapped portion
         int   prot = PROT_READ | PROT_WRITE;
         void* remapped =
             ::mmap(unmap_start, unmap_size, prot, MAP_SHARED | MAP_FIXED, _fd, new_size);

         if (remapped == MAP_FAILED)
         {
            SAL_ERROR("Failed to remap file after truncate failure: {}", strerror(errno));
            // We're in an inconsistent state at this point
            throw std::runtime_error("failed to remap file after truncate failure");
         }

         throw std::system_error(errno, std::generic_category(), "failed to truncate file");
      }

      // Update our state variables
      _file_size = new_size;
      _num_blocks.store(nblocks, std::memory_order_release);

      // Restore the protection on the released virtual address space
      if (_reserved_base && _reservation_size > 0)
      {
         void*  prot_start = static_cast<char*>(_reserved_base) + new_size;
         size_t prot_size  = _reservation_size - new_size;

         if (prot_size > 0)
         {
            // Remap as protected memory to maintain our reservation
            void* protected_area = ::mmap(prot_start, prot_size, PROT_NONE,
                                          MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0);

            if (protected_area == MAP_FAILED)
            {
               SAL_WARN("Failed to restore protection on address space after truncate: {}",
                        strerror(errno));
               // Not a fatal error, we can continue without this
            }
         }
      }

      sal_debug("Truncated file to {} blocks ({} bytes)", nblocks, new_size);
   }
}  // namespace sal
