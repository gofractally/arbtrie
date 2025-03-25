#pragma once
#include <cstdint>
#include <sal/block_allocator.hpp>

namespace sal
{
   using offset_ptr = block_allocator::offset_ptr;

   /**
    * References a location in shared memory, addressed by cacheline
    * as used in shared_ptr_data::cacheline_offset. Its purpose is to
    * keep track of whether the location is addressed in bytes (absolute),
    * on by cacheline index.  It assumes a 64 byte cacheline.  
    */
   struct location
   {
     public:
      static constexpr uint64_t location_shift = 6;
      uint64_t                  absolute_address() const { return _cacheline_offset * 64; }
      uint64_t                  cacheline() const { return _cacheline_offset; }
      offset_ptr                offset() const { return offset_ptr(absolute_address()); }

      static constexpr location from_absolute_address(uint64_t address)
      {
         return location(address / 64);
      }
      static constexpr location from_cacheline(uint64_t cacheline) { return location(cacheline); }

      constexpr bool operator==(const location& other) const
      {
         return _cacheline_offset == other._cacheline_offset;
      }
      constexpr bool operator!=(const location& other) const
      {
         return _cacheline_offset != other._cacheline_offset;
      }

     private:
      explicit constexpr location(uint64_t cacheline_offset) : _cacheline_offset(cacheline_offset)
      {
      }
      uint64_t _cacheline_offset : 41;
   };

}  // namespace sal