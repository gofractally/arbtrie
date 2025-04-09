#pragma once
#include <compare>  // for operator<=> std::strong_ordering
#include <cstdint>
#include <ucc/make_mask.hpp>

namespace ucc
{
   /**
    * Facilitatesthe accumulation of a weighted average age in a single
    * uint64_t
    */
   class weighted_average
   {
     public:
      weighted_average() : sum_age_times_size(0), sum_size(1) {}

      /**
       * @param bytes the number of bytes to accumulate, with the vage
       * associated with the bytes.
       * @param vage the virtual age of the bytes being added to the average
       */
      weighted_average& add(uint32_t bytes, uint64_t vage)
      {
         sum_size += bytes;
         sum_age_times_size += vage * uint64_t(bytes);
         return *this;
      }
      weighted_average& reset(uint64_t vage)
      {
         sum_age_times_size = vage;
         sum_size           = 1;  // prevent divide by zero
         return *this;
      }

      auto operator<=>(const weighted_average& other) const
      {
         return average() <=> other.average();
      }
      uint64_t average() const { return sum_age_times_size / sum_size; }

     private:
      uint64_t sum_age_times_size;
      uint32_t sum_size;
   };

}  // namespace ucc