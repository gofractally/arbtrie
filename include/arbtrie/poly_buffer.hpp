#pragma once

#include <arbtrie/debug.hpp>
#include <arbtrie/padded_atomic.hpp>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <thread>

namespace arbtrie
{

   /**
 * A fixed-size single-producer multiple-consumer buffer optimized for shared memory use.
 * 
 * Features:
 * - Lock-free operations using atomic counters and bitmap
 * - 32 slots with efficient bit operations
 * - Single producer, multiple consumer design
 * - Shared memory compatible
 * 
 * Synchronization Protocol:
 * - Producer increments its counter after adding data, masked for slot access
 * - Consumers compete to increment the consumer counter via CAS
 * - Producers and consumers use bitmap to track slot state
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
      static constexpr uint64_t buffer_size = 32;  // 32 slots
      static constexpr uint64_t mask        = buffer_size - 1;

      std::array<T, buffer_size> buf;
      std::atomic<uint64_t>      bitmap{0};  // 1 = slot has data, 0 = slot is empty

      // Counters that grow to infinity, masked for actual slot access
      std::atomic<uint64_t> producer_count{0};  // Next position to produce
      std::atomic<uint64_t> consumer_count{0};  // Next position to consume
   };

   // Implementation

   template <typename T>
   inline bool poly_buffer<T>::try_push(T value)
   {
      // Get current counts
      uint64_t prod_count = producer_count.load(std::memory_order_relaxed);
      uint64_t cons_count = consumer_count.load(std::memory_order_acquire);

      // Check if buffer is full (producer has lapped consumer completely)
      if (prod_count - cons_count >= buffer_size)
         return false;

      // Determine the actual slot in the buffer
      uint64_t slot = prod_count & mask;

      // Check if slot bit is already set (shouldn't happen, but check for safety)
      uint64_t bit_mask = 1ULL << slot;
      uint64_t current  = bitmap.load(std::memory_order_acquire);
      if (current & bit_mask)
         return false;

      // Write data to slot
      buf[slot] = std::move(value);

      // Set the bit to mark slot as containing data
      current = bitmap.fetch_or(bit_mask, std::memory_order_release);

      // Increment producer count
      producer_count.store(prod_count + 1, std::memory_order_release);

      // Only notify waiting consumers if the queue was empty before this push
      if (current == 0)
         bitmap.notify_all();

      return true;
   }

   template <typename T>
   inline void poly_buffer<T>::push(T value)
   {
      while (true)
      {
         // Try push first
         if (try_push(std::move(value)))
            return;

         // Get current counts to determine which slot to wait on
         uint64_t prod_count = producer_count.load(std::memory_order_relaxed);
         uint64_t cons_count = consumer_count.load(std::memory_order_acquire);

         // Check if buffer is actually full
         if (prod_count - cons_count >= buffer_size)
         {
            // Determine which slot we're waiting for
            uint64_t slot     = prod_count & mask;
            uint64_t bit_mask = 1ULL << slot;

            // Load bitmap to check if slot bit is set
            uint64_t current = bitmap.load(std::memory_order_acquire);

            // Only wait if the bit is still set (slot is occupied)
            if (current & bit_mask)
            {
               // Wait for this specific bitmap value to change
               bitmap.wait(current, std::memory_order_acquire);
            }
         }

         // Try again immediately
      }
   }

   template <typename T>
   inline std::optional<T> poly_buffer<T>::try_pop()
   {
      // Get current counts
      uint64_t cons_count = consumer_count.load(std::memory_order_relaxed);
      uint64_t prod_count = producer_count.load(std::memory_order_acquire);

      // Check if buffer is empty
      if (cons_count >= prod_count)
         return std::nullopt;

      // Try to claim this position with CAS
      if (!consumer_count.compare_exchange_strong(cons_count, cons_count + 1,
                                                  std::memory_order_acq_rel))
         return std::nullopt;  // Another consumer got it first

      // Determine the actual slot in the buffer
      uint64_t slot = cons_count & mask;

      // Verify the bit is set (data is available)
      uint64_t bit_mask = 1ULL << slot;
      uint64_t current  = bitmap.load(std::memory_order_acquire);
      if (!(current & bit_mask))
      {
         // This should never happen unless there's a bug
         // Still, handle it gracefully by retrying
         return std::nullopt;
      }

      // Read the data
      T data = buf[slot];

      // Clear the bit to mark slot as empty
      bitmap.fetch_and(~bit_mask, std::memory_order_release);

      // Notify waiting producers
      bitmap.notify_all();

      return data;
   }

   template <typename T>
   inline T poly_buffer<T>::pop()
   {
      while (true)
      {
         // Try pop first
         auto result = try_pop();
         if (result)
            return *result;

         // Get current counts to determine which slot to wait on
         uint64_t cons_count = consumer_count.load(std::memory_order_relaxed);
         uint64_t prod_count = producer_count.load(std::memory_order_acquire);

         // Check if buffer is actually empty
         if (cons_count >= prod_count)
         {
            // Determine which slot we're waiting for
            uint64_t slot     = cons_count & mask;
            uint64_t bit_mask = 1ULL << slot;

            // Load bitmap to check if the bit is clear
            uint64_t current = bitmap.load(std::memory_order_acquire);

            // Only wait if the bit is still clear (no data available)
            if (!(current & bit_mask))
            {
               // Wait for this specific bitmap value to change
               bitmap.wait(current, std::memory_order_acquire);
            }
         }

         // Try again immediately
      }
   }

   template <typename T>
   inline uint64_t poly_buffer<T>::usage() const
   {
      uint64_t prod_count = producer_count.load(std::memory_order_acquire);
      uint64_t cons_count = consumer_count.load(std::memory_order_acquire);

      return (prod_count > cons_count) ? (prod_count - cons_count) : 0;
   }

   template <typename T>
   inline void poly_buffer<T>::clear()
   {
      // Reset both counters and bitmap
      consumer_count.store(0, std::memory_order_relaxed);
      producer_count.store(0, std::memory_order_relaxed);
      bitmap.store(0, std::memory_order_release);

      // Notify any waiting threads
      bitmap.notify_all();
   }
}  // namespace arbtrie