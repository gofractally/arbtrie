#pragma once

#include <arbtrie/debug.hpp>
#include <arbtrie/padded_atomic.hpp>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace arbtrie
{

   /**
 * A fixed-size single-producer multiple-consumer buffer optimized for shared memory use.
 * 
 * Features:
 * - Lock-free operations using atomic bitmap for slot tracking
 * - 32 slots with efficient bit operations
 * - Single producer, multiple consumer design
 * - Shared memory compatible (no mutex/cv)
 * 
 * Synchronization Protocol with split bitmap (single 64-bit atomic):
 * - Bits 0-31: Set by producer to mark slots with data (available for consumption)
 * - Bits 32-63: Set by consumers to claim slots they're reading
 * - Consumer first sets bit [X+32] to claim slot X
 * - Consumer then reads the data
 * - Consumer atomically clears BOTH bits [X] and [X+32] in one operation
 * - This guarantees no race conditions between consumers or with producer
 * 
 * @tparam T The type of elements stored in the buffer
 */
   template <typename T>
   class poly_buffer
   {
     public:
      // Constructor and core methods
      poly_buffer() = default;

      // Producer methods
      bool try_push(T value);
      void push(T value);  // Waits until space available

      // Consumer methods
      std::optional<T> try_pop();
      T                pop();  // Waits until data available

      // Utility methods
      uint64_t                  usage() const;
      static constexpr uint64_t capacity() { return buffer_size; }

      // Reset the buffer to initial state (empty)
      void clear();

     private:
      static constexpr uint64_t buffer_size = 32;  // 32 slots (half of 64-bit bitmap)
      static constexpr uint64_t mask        = buffer_size - 1;

      // Bit layout in bitmap:
      // [0-31]: Producer bits (1 = slot has data available)
      // [32-63]: Consumer bits (1 = slot is being consumed)
      static constexpr uint64_t producer_shift = 0;
      static constexpr uint64_t consumer_shift = 32;
      static constexpr uint64_t producer_mask  = 0x00000000FFFFFFFFULL;
      static constexpr uint64_t consumer_mask  = 0xFFFFFFFF00000000ULL;

      std::array<T, buffer_size> buf;
      std::atomic<uint64_t>      bitmap{0};         /// tracks slots with data
      std::atomic<uint32_t> next_producer_slot{0};  /// next slot for the producer (wraps around)
      std::atomic<uint32_t> next_consumer_slot{0};  /// next slot for the consumer (wraps around)

      // Helper to find slot index from bit position in bitmap
      static constexpr uint64_t bit_to_slot(uint64_t bit_pos) { return bit_pos & mask; }

      // Helper to create producer bit mask for a slot
      static constexpr uint64_t producer_bit(uint64_t slot)
      {
         return 1ULL << (slot + producer_shift);
      }

      // Helper to create consumer bit mask for a slot
      static constexpr uint64_t consumer_bit(uint64_t slot)
      {
         return 1ULL << (slot + consumer_shift);
      }

      // Helper to create combined mask for both producer and consumer bits
      static constexpr uint64_t combined_bits(uint64_t slot)
      {
         return producer_bit(slot) | consumer_bit(slot);
      }

      // Helper to find first available slot (bit set in producer portion)
      uint64_t find_available_slot(uint64_t bitmap_val) const
      {
         // Extract just producer bits and find first set bit
         uint64_t producer_bits = bitmap_val & producer_mask;
         return __builtin_ctzll(producer_bits);
      }

      // Helper to find first free slot for producer
      uint64_t find_free_slot(uint64_t bitmap_val) const
      {
         // Slot is free if BOTH producer and consumer bits are clear
         // Combine both halves to find any set bit (occupied slot)
         uint64_t producer_bits = bitmap_val & producer_mask;
         uint64_t consumer_bits = (bitmap_val & consumer_mask) >> consumer_shift;
         uint64_t occupied      = producer_bits | consumer_bits;

         // If all bits are set, buffer is full
         if (occupied == 0xFFFFFFFFULL)
            return buffer_size;  // Return invalid slot index

         // Find first unset bit in occupied bitmap
         return __builtin_ctzll(~occupied);
      }
   };

   // Implementation

   template <typename T>
   inline bool poly_buffer<T>::try_push(T value)
   {
      // Get current bitmap state
      uint64_t current = bitmap.load(std::memory_order_acquire);

      // Calculate free and occupied slots
      uint64_t producer_bits = current & producer_mask;
      uint64_t consumer_bits = (current & consumer_mask) >> consumer_shift;
      uint64_t occupied      = producer_bits | consumer_bits;

      // If all bits are set, buffer is full
      if (occupied == 0xFFFFFFFFULL)
         return false;

      // Get the next slot to write to (stored in next_producer_slot)
      uint32_t slot = next_producer_slot.load(std::memory_order_relaxed);

      // Find a valid slot (starting from the intended next slot)
      for (uint32_t i = 0; i < buffer_size; ++i)
      {
         uint32_t try_slot = (slot + i) & mask;

         // Check if this slot is free
         if (!(occupied & (1ULL << try_slot)))
         {
            slot = try_slot;
            break;
         }
      }

      // Write data
      buf[slot] = std::move(value);

      // Set the producer bit
      uint64_t bit  = producer_bit(slot);
      uint64_t prev = bitmap.fetch_or(bit, std::memory_order_release);

      // Update next_producer_slot to point to the next slot
      next_producer_slot.store((slot + 1) & mask, std::memory_order_relaxed);

      // Notify if buffer was empty
      if ((prev & producer_mask) == 0)
         bitmap.notify_all();

      return true;
   }

   template <typename T>
   inline void poly_buffer<T>::push(T value)
   {
      while (!try_push(std::move(value)))
      {
         // Load current bitmap to wait on
         uint64_t current = bitmap.load(std::memory_order_acquire);

         // Calculate how many slots are used
         uint64_t producer_bits = current & producer_mask;
         uint64_t consumer_bits = (current & consumer_mask) >> consumer_shift;
         uint64_t occupied      = producer_bits | consumer_bits;

         // Only wait if the buffer is truly full
         if (occupied == 0xFFFFFFFFULL)
            bitmap.wait(current, std::memory_order_acquire);

         // Try again immediately after waking up or if slots freed up between checks
      }
   }

   template <typename T>
   inline std::optional<T> poly_buffer<T>::try_pop()
   {
      // Load current bitmap state
      uint64_t current = bitmap.load(std::memory_order_acquire);

      // Check if any producer bits are set (data available)
      uint64_t producer_bits = current & producer_mask;
      if (producer_bits == 0)
         return std::nullopt;

      // Get the next slot to read from (stored in next_consumer_slot)
      uint32_t slot = next_consumer_slot.load(std::memory_order_relaxed);

      // Find a valid slot with data (starting from the intended next slot)
      bool found = false;
      for (uint32_t i = 0; i < buffer_size; ++i)
      {
         uint32_t try_slot = (slot + i) & mask;

         // Check if this slot has data
         if (producer_bits & (1ULL << try_slot))
         {
            slot  = try_slot;
            found = true;
            break;
         }
      }

      if (!found)
         return std::nullopt;

      // Set the consumer bit to claim this slot
      uint64_t consumer_mask = consumer_bit(slot);
      uint64_t prev          = bitmap.fetch_or(consumer_mask, std::memory_order_acq_rel);

      // Check if already claimed
      if (prev & consumer_mask)
         return std::nullopt;

      // Verify producer bit is still set
      if (!(prev & producer_bit(slot)))
      {
         bitmap.fetch_and(~consumer_mask, std::memory_order_release);
         return std::nullopt;
      }

      // Read the data
      T data = buf[slot];

      // Clear both bits
      uint64_t combined_mask = combined_bits(slot);
      bitmap.fetch_and(~combined_mask, std::memory_order_release);

      // Update next_consumer_slot to point to the next slot
      next_consumer_slot.store((slot + 1) & mask, std::memory_order_relaxed);

      // Notify if buffer was full
      if ((prev & producer_mask) == producer_mask)
         bitmap.notify_all();

      return data;
   }

   template <typename T>
   inline T poly_buffer<T>::pop()
   {
      std::optional<T> result;
      while (!(result = try_pop()))
      {
         // Wait for new data to become available
         uint64_t current = bitmap.load(std::memory_order_acquire);

         // Only wait if still no data
         if ((current & producer_mask) == 0)
            bitmap.wait(current, std::memory_order_acquire);

         // Try again immediately after waking up (no wait if data appeared)
      }
      return *result;
   }

   template <typename T>
   inline uint64_t poly_buffer<T>::usage() const
   {
      // Load current bitmap
      uint64_t bits = bitmap.load(std::memory_order_acquire);

      // OR with consumer_mask to ensure the consumer bits are all set
      // This ensures we always have 32 bits set in the upper portion
      bits |= consumer_mask;

      // Count all set bits and subtract the 32 bits from consumer portion
      return __builtin_popcountll(bits) - 32;
   }

   template <typename T>
   inline void poly_buffer<T>::clear()
   {
      // Atomically clear the entire bitmap to reset all state
      bitmap.store(0, std::memory_order_release);

      // Notify any waiting consumers/producers
      bitmap.notify_all();
   }
}  // namespace arbtrie