#pragma once
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

      size_weighted_age() : sum_age_times_size(0), sum_size(1) {}

      /**
       * @param bytes the number of bytes to accumulate, with the vage
       * associated with the bytes.
       * @param vage the virtual age of the bytes being added to the average
       */
      size_weighted_age& add(uint32_t bytes, uint64_t vage)
      {
         bytes = round_up_multiple<64>(bytes) / 64;
         sum_size += bytes;
         sum_age_times_size += vage * uint64_t(bytes);
         return *this;
      }
      size_weighted_age& reset(uint64_t vage)
      {
         sum_age_times_size = vage;
         sum_size           = 1;  // prevent divide by zero
         return *this;
      }

      auto operator<=>(const size_weighted_age& other) const
      {
         return average_age() <=> other.average_age();
      }

      uint64_t sum_age_times_size;
      uint32_t sum_size;

      uint64_t average_age() const { return sum_age_times_size / sum_size; }
   };

}  // namespace arbtrie
