#pragma once
#include <cstdint>
#include <sal/block_allocator.hpp>
#include <sal/numbers.hpp>

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
      uint64_t                  absolute_address() const noexcept { return _cacheline_offset * 64; }
      uint64_t                  cacheline() const noexcept { return _cacheline_offset; }
      offset_ptr                offset() const noexcept { return offset_ptr(absolute_address()); }

      location() noexcept : _cacheline_offset(0) {}

      static constexpr location from_absolute_address(uint64_t address) noexcept
      {
         return location(address / 64);
      }
      static constexpr location from_cacheline(uint64_t cacheline) noexcept
      {
         return location(cacheline);
      }
      static constexpr location null() noexcept { return location::from_absolute_address(-1ull); }

      constexpr bool operator==(const location& other) const noexcept
      {
         return _cacheline_offset == other._cacheline_offset;
      }
      constexpr bool operator!=(const location& other) const noexcept
      {
         return _cacheline_offset != other._cacheline_offset;
      }

      segment_number segment() const noexcept
      {
         return segment_number(absolute_address() / segment_size);
      }

      uint64_t segment_offset() const noexcept { return absolute_address() % segment_size; }

     private:
      explicit constexpr location(uint64_t cacheline_offset) noexcept
          : _cacheline_offset(cacheline_offset)
      {
      }
      uint64_t _cacheline_offset : 41;
   };

}  // namespace sal