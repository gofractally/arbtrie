#pragma once
#include <atomic>
#include <new>

namespace ucc
{
   /**
    * @brief A padded atomic type that is aligned to the hardware destructive interference size.
    * 
    * This type is designed to provide thread-safe operations on a single variable across multiple
    * threads, while also ensuring that the variable is aligned to the hardware's cache line size.
    * 
    * prevents false sharing of cache lines by padding the atomic variable to the cache line size.
    */
   template <typename T>
   struct padded_atomic : public std::atomic<T>
   {
      using std::atomic<T>::atomic;

      char padding[std::hardware_destructive_interference_size - sizeof(std::atomic<T>)];

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

   } __attribute__((__aligned__(std::hardware_destructive_interference_size)));

   static_assert(sizeof(padded_atomic<uint64_t>) == std::hardware_destructive_interference_size);

   /**
   * These bit manipulation functions are designed to allow ONE thread to modify the high 32 bits
   * and ONE thread to modify the low 32 bits concurrently. They are NOT designed to support
   * multiple threads modifying the same portion (high or low) simultaneously.
   * 
   * @group atomc_bit_manipulation Atomic Bit Manipulation Functions
   */
   ///@{
   inline void set_high_bits(std::atomic<uint64_t>& atomic, uint32_t value)
   {
      uint64_t cur        = atomic.load(std::memory_order_acquire);
      uint64_t diff       = value - uint32_t(cur >> 32);
      uint64_t adjustment = diff << 32;

      // because the adjustment has 0's in the lower 32 bits,
      // the fetch_add will not overwrite the lower 32 bits managed by the session thread
      atomic.fetch_add(adjustment, std::memory_order_release);
   }

   inline void set_low_bits(std::atomic<uint64_t>& atomic, uint32_t value)
   {
      uint64_t cur         = atomic.load(std::memory_order_acquire);
      uint32_t current_low = static_cast<uint32_t>(cur & 0xFFFFFFFF);

      // Calculate the signed difference as int64_t to handle negative differences
      int64_t diff = static_cast<int64_t>(value) - static_cast<int64_t>(current_low);

      // We can now add this difference, which could be positive or negative
      // Only the low 32 bits will be affected since the high 32 bits of diff are sign extension
      atomic.fetch_add(diff, std::memory_order_release);
   }
   /// @} atomic bit manipulation functions

}  // namespace ucc