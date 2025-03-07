#pragma once
#include <arbtrie/rdtsc.hpp>
#include <arbtrie/util.hpp>

namespace arbtrie
{

   /**
    * Facilitatesthe accumulation of a weighted average age in a single
    * uint64_t
    */
   struct size_weighted_age
   {
      static constexpr const uint64_t read_cl_mask = make_mask<0, 21>();
      static constexpr const uint64_t age_mask     = make_mask<21, 43>();

      size_weighted_age() : read_cl(0), age(0) {}
      size_weighted_age(uint64_t data) { *this = std::bit_cast<size_weighted_age>(data); }

      uint64_t to_int() const { return std::bit_cast<uint64_t>(*this); }

      /**
       * @param bytes the number of bytes to accumulate, with the vage
       * associated with the bytes.
       * @param vage the virtual age of the bytes being added to the average
       */
      size_weighted_age& add(uint32_t bytes, uint64_t vage)
      {
         auto clines = round_up_multiple<64>(bytes) / 64;
         // data-weighted average age
         auto tmp = (age * read_cl + vage * clines) / (read_cl + clines);

         // Ensure tmp can be stored in 43 bits (2^43 - 1 = 8796093022207)
         assert(tmp <= 0x7FFFFFFFFFF);
         // Ensure read_cl + clines can fit in 21 bits (2^21 - 1 = 2097151)
         assert(read_cl + clines <= 0x1FFFFF);

         age = tmp;
         read_cl += clines;
         return *this;
      }
      size_weighted_age& reset()
      {
         read_cl = 0;
         age     = 0;
         return *this;
      }

      auto operator<=>(const size_weighted_age& other) const { return age <=> other.age; }

      uint64_t read_cl : 21;  // read cachelines, 21 bits supports up to 128MB segments
      uint64_t age : 43;      // 1024x the age of the segment
   };

}  // namespace arbtrie
