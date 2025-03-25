#pragma once
#include <filesystem>
#include <memory>
#include <mutex>
#include <utility>

#include <cassert>
#include <system_error>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <vector>

#include <sal/mapping.hpp>  // sync_type
#include <sal/typed_int.hpp>

namespace sal
{
   /**
    * Responsible for maintaining a memory mapped file on disk that grows as needed.
    * Uses a contiguous address space reservation to ensure blocks are mapped contiguously.
    */
   class block_allocator
   {
     public:
      // 64-bit offset from the base pointer
      using offset_ptr                        = typed_int<uint64_t, struct offset_ptr_tag>;
      using block_number                      = typed_int<uint64_t, struct block_num_tag>;
      static constexpr offset_ptr null_offset = offset_ptr(-1);

      /**
       * Constructor for block_allocator.
       * 
       * @param file The path to the file to use for block allocation
       * @param block_size The size of each block in bytes (MUST be a power of 2)
       * @param max_blocks The maximum number of blocks that can be allocated
       * @param read_write Whether the file should be opened in read-write mode
       * 
       * @throws std::runtime_error If the reservation of virtual address space fails
       * @throws std::invalid_argument If block_size is not a power of 2
       */
      block_allocator(std::filesystem::path file,
                      uint64_t              block_size,
                      uint32_t              max_blocks,
                      bool                  read_write = true);
      ~block_allocator();

      uint64_t block_size() const { return _block_size; }
      uint64_t num_blocks() const { return _num_blocks.load(std::memory_order_relaxed); }

      /**
       * Resizes the file and num_blocks() count to nblocks.
       */
      void truncate(uint32_t nblocks);

      /**
       * This method syncs all mapped memory to disk.
       */
      void sync(sync_type st);

      /**
       * Return a pointer to the block at the specified offset
       * 
       * @param offset The offset pointer (returned by alloc)
       * @return A pointer to the block at the specified offset
       */
      template <typename T = void>
      inline T* get(offset_ptr offset) noexcept
      {
         assert(*offset < _file_size);
         return reinterpret_cast<T*>(((char*)_mapped_base) + *offset);
      }
      /**
       * Return a const pointer to the block at the specified offset
       * 
       * @param offset The offset pointer (returned by alloc)
       * @return A const pointer to the block at the specified offset
       */
      template <typename T = void>
      inline const T* get(offset_ptr offset) const noexcept
      {
         assert(*offset < _file_size);
         return reinterpret_cast<const T*>(((const char*)_mapped_base) + *offset);
      }
      /**
       * Return a const pointer to the block at the specified block number
       * 
       * @param block_num The block number (index)
       * @return A const pointer to the block at the specified block number
       */
      template <typename T = void>
      inline T* get(block_number block_num) noexcept
      {
         assert(*block_num < _num_blocks.load(std::memory_order_relaxed));
         return get<T>(block_to_offset(block_num));
      }

      /**
       * Return a const pointer to the block at the specified offset
       * 
       * @param offset The offset pointer (returned by alloc)
       * @return A const pointer to the block at the specified offset
       */
      template <typename T = void>
      inline const T* get(block_number block_num) const noexcept
      {
         assert(*block_num < _num_blocks.load(std::memory_order_relaxed));
         return get<T>(block_to_offset(block_num));
      }

      /**
       * Convenience method to convert a block number (index) to an offset pointer
       * Uses efficient bit shifting since block_size is guaranteed to be a power of 2
       * 
       * @param block_num The block number (index)
       * @return The offset pointer to the start of the block
       */
      inline offset_ptr block_to_offset(block_number block_num) const noexcept
      {
         assert(*block_num < _num_blocks.load(std::memory_order_relaxed));
         return offset_ptr(*block_num << _log2_block_size);  // Fast multiplication by power of 2
      }

      /**
       * Convenience method to convert an offset pointer to a block number (index)
       * Uses efficient bit shifting since block_size is guaranteed to be a power of 2
       * 
       * @param offset The offset pointer
       * @return The block number (index)
       */
      inline block_number offset_to_block(offset_ptr offset) const noexcept
      {
         assert(*offset < _file_size);
         // Fast division by power of 2
         return block_number(*offset >> _log2_block_size);
      }

      /**
       * Check if an address is aligned to a block boundary
       * 
       * @param offset The offset to check
       * @return True if the offset is at a block boundary, false otherwise
       */
      inline bool is_block_aligned(offset_ptr offset) const noexcept
      {
         // Fast modulo using bit mask since block_size is a power of 2
         return (*offset & (_block_size - 1)) == 0;
      }

      // ensures that at least the desired number of blocks are present
      uint32_t reserve(uint32_t desired_num_blocks);

      /**
       * Allocate a new block and return both the block number and offset pointer to it
       * 
       * @return A pair containing the block number and offset pointer to the newly allocated block
       * @throws std::runtime_error If the maximum number of blocks has been reached
       */
      std::pair<block_number, offset_ptr> alloc();

      /**
       * Resizes the block allocator to the specified number of blocks.
       * Similar to std::vector::resize(), this ensures _num_blocks equals the desired size.
       * 
       * NOTE: This method is not thread-safe when used simultaneously with alloc().
       * 
       * @param desired_num_blocks The desired number of allocated blocks
       * @return The actual number of blocks after resizing
       * @throws std::runtime_error If the maximum number of blocks would be exceeded
       */
      uint32_t resize(uint32_t desired_num_blocks);

      /**
       * Finds the maximum possible reservation size as a multiple of the specified block size.
       * This method performs a binary search to identify the largest number of blocks that can
       * be reserved contiguously in the virtual address space of the current system.
       *
       * @param block_size The size of each block in bytes (MUST be a power of 2)
       * @return The maximum reservation size in bytes (will be a multiple of block_size)
       * @throws std::invalid_argument If block_size is not a power of 2
       */
      static uint64_t find_max_reservation_size(uint64_t block_size);

      /**
       * Helper method to determine if a value is a power of 2
       *
       * @param x The value to check
       * @return True if x is a power of 2, false otherwise
       */
      static bool is_power_of_2(uint64_t x) noexcept
      {
         // Zero and negative numbers are not powers of 2
         return x > 0 && (x & (x - 1)) == 0;
      }

     private:
      std::filesystem::path _filename;
      uint64_t              _block_size;
      uint8_t               _log2_block_size;  // log2 of block_size for fast bit shifting
      uint64_t              _max_blocks;
      uint64_t              _file_size;
      int                   _fd;
      std::atomic<uint64_t> _num_blocks;
      mutable std::mutex    _resize_mutex;

      // Address space reservation
      void*    _reserved_base;     // Base address of the reserved virtual memory region
      uint64_t _reservation_size;  // Size of the reserved region
      void*    _mapped_base;       // Base address of the mapped file region
   };
}  // namespace sal
