#pragma once
#include <cstdint>

namespace sal
{
   /**
    * References a location in shared memory, addressed by cacheline
    * as used in shared_ptr_data::cacheline_offset. Its purpose is to
    * keep track of whether the location is addressed in bytes (absolute),
    * on by cacheline index.  It assumes a 64 byte cacheline.  
    */
   struct location
   {
     public:
      uint64_t absolute_address() const { return _cacheline_offset * 64; }
      uint64_t cacheline() const { return _cacheline_offset; }

      static location from_absolute_address(uint64_t address) { return location(address / 64); }
      static location from_cacheline(uint64_t cacheline) { return location(cacheline); }

      bool operator==(const location& other) const
      {
         return _cacheline_offset == other._cacheline_offset;
      }
      bool operator!=(const location& other) const
      {
         return _cacheline_offset != other._cacheline_offset;
      }

     private:
      explicit location(uint64_t cacheline_offset) : _cacheline_offset(cacheline_offset) {}
      uint64_t _cacheline_offset;
   };

}  // namespace sal