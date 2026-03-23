#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ucc/padded_atomic.hpp>

namespace ucc
{

   /**
    * A fixed-size single-producer multiple-consumer (SPMC) circular buffer optimized for
    * lock-free operations, suitable for shared memory contexts.
    *
    * Uses atomic operations on counters and a bitmap for thread-safe access to 32 slots.
    *
    * Synchronization Algorithm Details:
    * ---------------------------------
    * - Counters:
    *   - `producer_count`: Atomically tracked by the *single* producer. It represents the total
    *     number of successful push operations initiated. Incremented *after* data is placed
    *     and the corresponding bitmap bit is set.
    *   - `consumer_count`: Atomically tracked by *multiple* consumers. It represents the total
    *     number of pop operations *claimed* (via CAS). Incremented *before* data is read.
    *   - Both counters grow indefinitely. The difference `producer_count - consumer_count`
    *     yields the current number of items in the buffer. The actual circular buffer slot
    *     index for a given count is `count & mask` (where `mask = buffer_size - 1`).
    * - Bitmap (`bitmap`):
    *   - A 64-bit atomic integer where each bit (0-31) corresponds to a buffer slot.
    *   - A bit is SET (1) by the producer *after* writing data to the slot, marking it ready for consumption.
    *   - A bit is CLEARED (0) by a consumer *after* successfully reading data, marking the slot available for production.
    *   - Serves a dual purpose: tracking slot readiness/availability and acting as the atomic variable for `wait`/`notify` operations.
    * - Full/Empty Conditions (checked using counter snapshots):
    *   - Full: `producer_count - consumer_count >= buffer_size`. The buffer has no available slots.
    *   - Empty: `consumer_count >= producer_count`. The buffer has no items ready for consumption.
    * - Producer Logic (`try_push`/`push`):
    *   1. Read counters to check the Full condition.
    *   2. Calculate the target `slot = producer_count & mask`.
    *   3. (Optional check: ensure `bitmap` bit for `slot` is 0).
    *   4. Write `value` to `buf[slot]`.
    *   5. Atomically set the `slot` bit in `bitmap` using `fetch_or` (release semantics ensure the write to `buf` is visible before the bit is set).
    *   6. Update `producer_count` (relaxed memory order suffices as the `bitmap` change provides necessary synchronization).
    *   7. If the buffer might have been empty before this push, call `bitmap.notify_all()` to wake potential waiting consumers.
    *   8. `push` loops, calling `try_push`. If full, it waits (`bitmap.wait()`) for the specific target slot's bit to be cleared by a consumer before retrying.
    * - Consumer Logic (`try_pop`/`pop`):
    *   1. Read counters to check the Empty condition (acquire semantics on `producer_count` ensure visibility of producer writes).
    *   2. Attempt to claim the current `consumer_count` slot by atomically incrementing `consumer_count` using `compare_exchange_strong` (acq_rel semantics). If CAS fails, another consumer claimed it; return/retry.
    *   3. Calculate the claimed `slot = original_consumer_count & mask`.
    *   4. Check if the `slot` bit in `bitmap` is set (acquire semantics).
    *   5. Read data ` T data = buf[slot]`.
    *   6. Atomically clear the `slot` bit in `bitmap` using `fetch_and` (release semantics ensure the read from `buf` happens before the bit is cleared).
    *   7. Call `bitmap.notify_all()` to wake potential waiting producers.
    *   8. `pop` loops, calling `try_pop`. If empty, it waits (`bitmap.wait()`) for the specific target slot's bit to be set by the producer before retrying.
    * - Memory Ordering:
    *   - Acquire semantics are used when reading state (e.g., checking `producer_count` in consumer, checking `bitmap` before read) to ensure prior writes by other threads are visible.
    *   - Release semantics are used when publishing state changes (e.g., setting `bitmap` bit in producer, clearing `bitmap` bit in consumer) to ensure local writes are visible to other threads before the atomic operation completes.
    *   - Acquire-Release semantics are used in the consumer's CAS on `consumer_count` to combine the acquire (for reading `producer_count`) and release (for publishing the incremented `consumer_count`).
    *
    * @tparam T The type of elements stored in the buffer. Must be default constructible and movable.
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
      static constexpr uint64_t buffer_size = 64;  // Changed from 32
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
}  // namespace ucc