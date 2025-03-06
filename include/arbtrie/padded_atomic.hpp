#pragma once
#include <atomic>
#include <cassert>
#include <chrono>
#include <new>
#include <thread>

#ifdef __cpp_lib_hardware_interference_size
static constexpr size_t ARBTRIE_CACHE_LINE_SIZE = std::hardware_destructive_interference_size;
#else
static constexpr size_t ARBTRIE_CACHE_LINE_SIZE = 64;
#endif

namespace arbtrie
{

   template <typename T>
   struct padded_atomic : public std::atomic<T>
   {
      char padding[ARBTRIE_CACHE_LINE_SIZE - sizeof(std::atomic<T>)];

      using std::atomic<T>::load;
      using std::atomic<T>::store;
      using std::atomic<T>::exchange;
      using std::atomic<T>::compare_exchange_weak;
      using std::atomic<T>::compare_exchange_strong;
      using std::atomic<T>::fetch_add;
      using std::atomic<T>::fetch_sub;
      using std::atomic<T>::wait;
      using std::atomic<T>::notify_one;
      using std::atomic<T>::notify_all;

      /**
       * These bit manipulation functions are designed to allow ONE thread to modify the high 32 bits
       * and ONE thread to modify the low 32 bits concurrently. They are NOT designed to support
       * multiple threads modifying the same portion (high or low) simultaneously.
       * 
       * @group bit_manipulation Bit Manipulation Functions
       */
      ///@{
      inline void set_high_bits(uint32_t value)
      {
         // These bit manipulation methods must only be used with uint64_t type
         // to avoid sign extension issues with signed types
         static_assert(std::is_same<T, uint64_t>::value,
                       "set_high_bits is only safe to use with uint64_t atomic type");

         uint64_t cur        = load(std::memory_order_acquire);
         uint64_t diff       = value - uint32_t(cur >> 32);
         uint64_t adjustment = diff << 32;

         // because the adjustment has 0's in the lower 32 bits,
         // the fetch_add will not overwrite the lower 32 bits managed by the session thread
         auto result = fetch_add(adjustment, std::memory_order_release);

#ifndef NDEBUG
         // The result of fetch_add is the value before the add, so we can calculate the new value
         uint64_t new_value = result + adjustment;

         // Assert that the lower bits remain unchanged
         assert((new_value & 0xFFFFFFFF) == (result & 0xFFFFFFFF));

         // Assert that the high bits are set to the requested value
         assert((new_value >> 32) == value);
#else
         (void)result;  // hush compiler warning
#endif
      }

      inline void set_low_bits(uint32_t value)
      {
         // These bit manipulation methods must only be used with uint64_t type
         // to avoid sign extension issues with signed types
         static_assert(std::is_same<T, uint64_t>::value,
                       "set_low_bits is only safe to use with uint64_t atomic type");

         uint64_t cur         = load(std::memory_order_acquire);
         uint32_t current_low = static_cast<uint32_t>(cur & 0xFFFFFFFF);

         // Calculate the signed difference as int64_t to handle negative differences
         int64_t diff = static_cast<int64_t>(value) - static_cast<int64_t>(current_low);

         // We can now add this difference, which could be positive or negative
         // Only the low 32 bits will be affected since the high 32 bits of diff are sign extension
         auto result = fetch_add(diff, std::memory_order_release);

#ifndef NDEBUG
         // The result of fetch_add is the value before the add, so we can calculate the new value
         uint64_t new_value = result + diff;

         // Assert that the high bits remain unchanged
         assert((new_value >> 32) == (result >> 32));
         // Assert that the low bits are set to the requested value
         assert((new_value & 0xFFFFFFFF) == value);
#else
         (void)result;  // hush compiler warning
#endif
      }
   } __attribute__((__aligned__(ARBTRIE_CACHE_LINE_SIZE)));

   static_assert(sizeof(padded_atomic<uint64_t>) == ARBTRIE_CACHE_LINE_SIZE);

   /// @}

}  // namespace arbtrie