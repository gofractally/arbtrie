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

#include <arbtrie/debug.hpp>
#include <arbtrie/mapping.hpp>

namespace arbtrie
{

   /**
    * Responsible for maintaining a memory mapped file on disk that grows as needed.
    */
   class block_allocator
   {
     public:
      using block_number = uint32_t;

      block_allocator(std::filesystem::path file,
                      uint64_t              block_size,
                      uint32_t              max_blocks,
                      bool                  read_write = true);
      ~block_allocator();

      uint64_t block_size() const { return _block_size; }
      uint64_t num_blocks() const { return _num_blocks.load(std::memory_order_relaxed); }

      /**
       * This method brute forces syncing all blocks which likely
       * flushes more than needed.
       */
      void sync(sync_type st);

      // return the base pointer for the mapped segment
      inline void* get(block_number i) noexcept
      {
         assert(i < _num_blocks.load(std::memory_order_relaxed));
         return _block_mapping[i];
      }

      // ensures that at least the desired number of blocks are present
      uint32_t reserve(uint32_t desired_num_blocks, bool memlock = false);

      block_number alloc();

     private:
      using char_ptr = char*;

      std::filesystem::path _filename;
      uint64_t              _block_size;
      uint64_t              _max_blocks;
      uint64_t              _file_size;
      int                   _fd;
      std::atomic<uint64_t> _num_blocks;
      char_ptr*             _block_mapping;
      mutable std::mutex    _resize_mutex;
   };
}  // namespace arbtrie
