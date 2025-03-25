#pragma once

#include <algorithm>  // for std::min
#include <arbtrie/debug.hpp>
#include <arbtrie/padded_atomic.hpp>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <type_traits>

namespace arbtrie
{
   /**
    * Exception thrown when a blocking operation is interrupted
    */
   class thread_interrupted : public std::runtime_error
   {
     public:
      thread_interrupted() : std::runtime_error("Thread operation interrupted") {}
   };

   /**
    * Single-producer, multiple-consumer buffer implementation with exactly 32 slots
    * 
    * This class implements a fixed-size buffer that allows concurrent access from one producer 
    * and multiple consumer threads without requiring explicit locks. It is a double-ended queue
    * that supports both FIFO (queue) and LIFO (stack) operations from both ends which allows
    * a producer to push priority items to the front of the queue and a consumer to chose whether
    * they want priority or low priority items.  
    * 
    * On top of this it supports a dynamic target capacity designed to minimize consumers having
    * to wait for the producer to "top off" the buffer. This is balanced against the producer 
    * having to wait for consumers.  In optimal flow, neither the producer nor consumers end
    * up waiting for the other and minimimal memory is wasted in the buffer.
    * 
    * There is two-way communication allowing the producer to take action after consumption and
    * before pushing something new. The consumers can decide if they want the producer to take
    * this extra action or not. (eg. mlock once in use, but only if the consumer requests it)
    * 
    * Target Use Case:
    *   - A producer has two types of data to push, priority and non-priority.
    *   - Multiple consumers want the priority data, but some consumers want the non-priority
    *     data as well.
    *   - Graceful fall back to for consumers to get different priority data than requested
    *   - One buffer that optimizes size to minimize delay on consumption
    *   - example: prioritize mlocked memory segments, but allow consumer to choose 
    *     segments that are not mlocked if they are not available.
    * 
    * Key Features:
    * - Supports both FIFO (queue) and LIFO (stack) operations from both ends
    * - Zero-copy, lock-free synchronization between producer and consumers
    * - Configurable watermarks for flow control
    * - Adaptive buffering based on consumer/producer speeds
    * - Bitmap-based slot tracking for efficient operations
    * - Value swapping capability for in-place updates
    * 
    * Interface:
    * - Producer methods:
    *   - push(data): Non-blocking FIFO push, returns index or -1 if full
    *   - push_wait(data): Blocking version of push, waits at high water mark
    *   - push_front(data): Non-blocking LIFO push (newest item comes out first)
    *   - push_front_wait(data): Blocking version of push_front
    *   - pop_ack(): Acknowledges consumed items, clearing their ack bits
    *   - try_swap(index, new_value): Atomically swaps a value at a specific index
    * 
    * - Consumer methods:
    *   - pop(): Non-blocking FIFO consume (take oldest item first)
    *   - pop_wait(): Blocking version of pop
    *   - pop_back(): Non-blocking LIFO consume (take newest item first)
    *   - pop_back_wait(): Blocking version of pop_back
    * 
    * - Status & monitoring methods:
    *   - usage(): Get total slots in use (available + pending ack)
    *   - free_space(): Get slots available for pushing
    *   - pending_ack_count(): Get count of consumed items awaiting acknowledgment
    *   - get_pushable_bitmap(): Get bitmap of slots available for pushing
    *   - get_swappable_bitmap(): Get bitmap of slots eligible for swapping
    *   - values(index): Direct read-only access to values by index
    * 
    * - Configuration methods:
    *   - get/set_min_water_gap(): Control buffer flow
    *   - get_high/low_water_mark(): Get current thresholds
    * 
    * Data Flow & Lifecycle:
    * 1. Producer pushes data to empty slots, making them available to consumers
    * 2. Consumers pop data, marking slots as pending acknowledgment
    * 3. Producer acknowledges consumed items with pop_ack(), freeing slots
    * 4. Slots cycle between three states: free, available, and pending ack
    * 
    * Naming convention:
    * - Any method that may block has 'wait' in its name (push_wait, pop_wait)
    * - Methods without 'wait' are guaranteed non-blocking
    * 
    * Advanced Usage:
    * - Symmetric FIFO: push() + pop() - Queue behavior (first in, first out)
    * - Symmetric LIFO: push_front() + pop() - Stack behavior (last in, first out)
    * - Priority FIFO: push() + pop_back() - Stack behavior (last in, first out)
    * - Alternative LIFO: push_front() + pop_back() - Queue behavior (first in, first out)
    * - Mixed Mode: Use different operations as needed for advanced use cases
    * - In-place Update: try_swap() to atomically modify values without changing order
    * 
    * Algorithm:
    * The buffer uses a 64-bit atomic bitmap to track slot availability, where:
    * - Bit 1 in available_bits = slot contains data and is available for consumers
    * - Bit 0 in available_bits = slot is either empty or pending acknowledgment
    * - Bit 1 in ack_bits = slot has been consumed and needs acknowledgment
    * - Bit 0 in ack_bits = slot is either available or empty
    * 
    * Slot States:
    * 1. Free: available_bits=0, ack_bits=0 - Available for producer to push data
    * 2. Available: available_bits=1, ack_bits=0 - Contains data for consumers
    * 3. Pending Ack: available_bits=0, ack_bits=1 - Consumed, waiting for producer acknowledgment
    * 
    * Producer push algorithm:
    * 1. Load available_bits and ack_bits bitmaps
    * 2. Find slots that are free (available_bits=0 AND ack_bits=0)
    * 3. Choose appropriate slot (first available for FIFO, rightmost for LIFO)
    * 4. Write data to that slot
    * 5. Set available_bits to 1 and ack_bits to 1 to track the publish
    * 6. Return the index where the data was stored
    * 7. If usage exceeds high water mark:
    *    - Wait until usage drops below low water mark
    * 
    * Consumer pop algorithm:
    * 1. Load available_bits bitmap (acquire)
    * 2. Find appropriate set bit (rightmost for FIFO, leftmost for LIFO)
    * 3. Speculatively read data from that slot
    * 4. Try to atomically clear the bit to 0
    * 5. If successful:
    *    - Check if usage dropped below low water mark
    *    - Notify producer if it's waiting
    *    - Return the consumed data
    * 6. If unsuccessful (another consumer claimed it), retry from step 1
    *
    * Acknowledgment flow:
    * 1. Producer calls pop_ack() to get consumed items
    * 2. For each consumed item, the ack bit is cleared
    * 3. This frees up the slot for future push operations
    * 
    * Swapping algorithm:
    * 1. Check if slot is available (bit set) and not pending ack
    * 2. Atomically clear the availability bit to claim it
    * 3. Replace the data in the slot
    * 4. Set the availability bit again to re-publish
    * 5. Return the original value if successful
    *
    * Bitmap helpers:
    * - get_pushable_bitmap(): Returns slots available for pushing (available_bits=0 AND ack_bits=0)
    * - get_swappable_bitmap(): Returns slots eligible for swapping (available_bits=1 AND ack_bits=0)
    * 
    * Buffering behavior:
    * - High water mark (default 75% full): When reached, producer waits
    * - Low water mark (default 25% full): When reached, producer resumes
    * - Minimum gap between marks ensures smooth flow control
    * - Water marks adapt based on consumer/producer speeds:
    *   - Increases when consumers wait (more buffering)
    *   - Decreases when producer waits (earlier back-pressure)
    * 
    * Waiting and notification:
    * - Producer waits using atomic wait when buffer is too full
    * - Consumers wait using atomic wait when buffer is empty
    * - Buffer adjusts capacity automatically based on wait patterns
    */
   template <typename T>
   class spmc_buffer
   {
     private:
      static constexpr uint64_t buffer_size = 32;               // Reduced to 32 slots
      static constexpr uint64_t mask        = buffer_size - 1;  // Mask for slot indices

      // Default thresholds are adjusted for 32 slots
      static constexpr uint64_t default_high_water = 2;
      static constexpr uint64_t default_low_water  = 1;
      // Constants for bit manipulation in combined bitmap
      static constexpr uint64_t avail_shift = 32;                     // High 32 bits for available
      static constexpr uint64_t avail_mask  = 0xFFFFFFFF00000000ULL;  // Mask for available bits
      static constexpr uint64_t ack_mask    = 0x00000000FFFFFFFFULL;  // Mask for ack bits

      // Convert from flat index to bit positions
      static constexpr uint64_t avail_bit(uint64_t index) { return 1ULL << (index + avail_shift); }
      static constexpr uint64_t ack_bit(uint64_t index) { return 1ULL << index; }
      static constexpr uint64_t avail_and_ack_bits(uint64_t index)
      {
         return avail_bit(index) | ack_bit(index);
      }

      // Data storage
      std::array<T, buffer_size> buf;  // The actual data storage
      padded_atomic<uint64_t>    bitmap{
          0};  // Combined bitmap: high 32 bits = available, low 32 bits = ack
      padded_atomic<uint64_t> priority_bits{
          0};  // Bitmap tracking priority items (1 = high priority)

      // Water levels for back-pressure
      std::atomic<uint64_t> high_water_mark{default_high_water};  // Adapts based on wait patterns
      std::atomic<uint64_t> low_water_mark{default_low_water};    // Always maintains gap below high
      std::atomic<uint64_t> min_water_gap{8};                     // Minimum required gap

      // Wait coordination
      std::atomic<bool> producer_waiting{false};     // Set when producer is waiting for low water
      std::atomic<int>  waiting_consumers{0};        // Count of consumers waiting for data
      std::atomic<bool> interrupt_requested{false};  // Set to true to wake blocked threads

      /**
       * Helper to write a value to a slot, mark it as available, and track it for acknowledgement
       * 
       * @param slot_index The index of the slot to write to
       * @param value The value to write to the slot
       */
      void write_and_publish(uint64_t slot_index, T value)
      {
         // Mask to ensure valid index
         slot_index &= mask;

         // Write data to slot
         buf[slot_index] = value;

         // Publish atomically - set both available and ack bits
         // IMPLEMENTATION NOTE: We use fetch_add instead of fetch_or for better performance.
         // This is safe because:
         // 1. Only the producer ever sets these bits, so there is no thread contention on setting
         // 2. The bits being set will always be 0 beforehand (we only publish to slots that are free)
         // 3. For bits that are already 0, adding the bit value is identical to OR-ing it
         // 4. The atomic fetch_add is typically implemented with a single instruction on x86 (LOCK XADD),
         //    while fetch_or might require multiple instructions
         bitmap.fetch_add(avail_and_ack_bits(slot_index), std::memory_order_release);

         // Notify any waiting consumers
         if (waiting_consumers.load(std::memory_order_acquire) > 0)
            bitmap.notify_one();
      }

      /**
        * Direction for water mark adjustments to control buffering behavior
        */
      enum class buffer_adjustment
      {
         more_buffering,  // Move water marks up to allow more buffering
         less_buffering   // Move water marks down to apply back pressure earlier
      };

      /**
        * Adjusts both high and low water marks together while maintaining their configured gap.
        * When moving up (more_buffering), increases both marks if possible.
        * When moving down (less_buffering), decreases both marks if possible.
        * Always maintains the minimum gap between marks.
        */
      void adjust_water_marks(buffer_adjustment direction)
      {
         uint64_t current_high = high_water_mark.load(std::memory_order_relaxed);
         uint64_t current_low  = low_water_mark.load(std::memory_order_relaxed);
         uint64_t current_gap  = min_water_gap.load(std::memory_order_relaxed);

         if (direction == buffer_adjustment::more_buffering)
         {
            // Move both marks up if we won't exceed buffer size
            if (current_high < buffer_size - 1)
            {
               high_water_mark.fetch_add(1, std::memory_order_relaxed);
               // Ensure low mark doesn't get too close to buffer size
               if (current_low < (buffer_size - 1 - current_gap))
                  low_water_mark.fetch_add(1, std::memory_order_relaxed);
            }
         }
         else  // less_buffering
         {
            // Move both marks down if we maintain minimum gap from zero
            if (current_low > current_gap)
            {
               low_water_mark.fetch_sub(1, std::memory_order_relaxed);
               // High mark follows to maintain gap
               if (current_high > (current_gap * 2))  // Ensure we don't go too low
                  high_water_mark.fetch_sub(1, std::memory_order_relaxed);
            }
         }
      }

      /**
       * Set the available bit for a slot.
       * 
       * @param index The slot index to set
       */
      void set_avail_bit(uint64_t index)
      {
         bitmap.fetch_or(avail_bit(index), std::memory_order_release);
      }

      /**
       * Clear the available bit for a slot.
       * 
       * @param index The slot index to clear
       */
      void clear_avail_bit(uint64_t index)
      {
         bitmap.fetch_and(~avail_bit(index), std::memory_order_release);
      }

      /**
       * Clear the ack bit for a slot.
       * 
       * IMPLEMENTATION NOTE: We use fetch_sub instead of fetch_and for better performance.
       * This is safe because:
       * 1. Only the producer ever clears the ack bit (in normal mode)
       * 2. The producer will only clear bits that it knows are set (after consumer pop)
       * 3. When clearing a bit that we know is set, subtracting the bit value is 
       *    mathematically equivalent to AND with complement
       * 4. The atomic fetch_sub is typically implemented with a single instruction on x86 (LOCK XADD),
       *    while fetch_and might require multiple instructions
       * 
       * @param index The slot index to clear the ack bit for
       */
      void clear_ack_bit(uint64_t index)
      {
         bitmap.fetch_sub(ack_bit(index),
                          std::memory_order_release);  // More efficient than fetch_and(~bit)
      }

      /**
       * Clear available and ack bits for a slot atomically.
       * Used in skip_ack mode.
       * 
       * @param index The slot index to clear bits for
       */
      void clear_avail_and_ack_bits(uint64_t index)
      {
         bitmap.fetch_and(~avail_and_ack_bits(index), std::memory_order_release);
      }

      /**
       * Clear available bit but leave ack bit set for a slot atomically.
       * Used in require_ack mode.
       * 
       * @param index The slot index to update
       * @return true if successful, false if the bits weren't in the expected state
       */
      bool clear_avail_keep_ack(uint64_t index)
      {
         uint64_t expected = avail_and_ack_bits(index);
         uint64_t desired  = ack_bit(index);  // Clear avail, keep ack

         // Try to atomically transition from (avail=1,ack=1) to (avail=0,ack=1)
         uint64_t current = bitmap.load(std::memory_order_acquire);
         while ((current & avail_and_ack_bits(index)) == avail_and_ack_bits(index))
         {
            if (bitmap.compare_exchange_weak(current,
                                             (current & ~avail_bit(index)),  // Clear only avail bit
                                             std::memory_order_release, std::memory_order_relaxed))
            {
               return true;
            }
         }
         return false;  // Bits were not in expected state
      }

      /**
        * Acknowledge that a consumed item at the specified index has been processed
        * by the producer. This clears the ack bit, allowing the slot to be reused.
        * 
        * @param idx The index position to acknowledge
        */
      void ack(uint64_t idx) { clear_ack_bit(idx & mask); }

      // Helper to count used slots
      uint64_t used_slots() const
      {
         uint64_t bits = (bitmap.load(std::memory_order_acquire) & avail_mask) >> avail_shift;
         return bits == 0 ? 0 : __builtin_popcountll(bits);
      }

     public:
      spmc_buffer() {}

      /**
       * Check if a specific item at the given index has been consumed
       * 
       * This allows the producer to track consumption of previously pushed items.
       * Once an item is consumed, its bit in the bitmap will be cleared (set to 0).
       * 
       * @param idx The index position to check
       * @return true if the item at position idx has been consumed, false otherwise
       */
      bool check_consumption(uint64_t idx) const
      {
         uint64_t bit = avail_bit(idx & mask);
         return (bitmap.load(std::memory_order_acquire) & bit) ==
                0;  // Consumed if avail bit is cleared
      }

      /**
       * Get bitmap of slots that have been consumed from the specified positions
       * 
       * Useful for batch checking which slots have been consumed.
       * 
       * @param positions_bitmap Bitmap where 1 indicates positions to check
       * @return Bitmap where 1 means the position has been consumed
       */
      uint64_t get_consumed_bitmap(uint64_t positions_bitmap) const
      {
         // Shift positions_bitmap to match available bit positions
         uint64_t avail_positions = positions_bitmap << avail_shift;
         uint64_t current         = bitmap.load(std::memory_order_acquire);

         // Return positions where available bit is 0 (consumed)
         return (~current & avail_positions) >> avail_shift;
      }

      /**
       * Get bitmap of slots that are available for pushing data to
       * 
       * These are slots that:
       * 1. Are NOT available to consumers (inverted bit = 1)
       * 2. Do NOT have their ack bit set (not pending acknowledgment)
       * 
       * @return Bitmap where each set bit represents a slot available for pushing
       */
      uint64_t get_pushable_bitmap() const
      {
         uint64_t bm    = bitmap.load(std::memory_order_acquire);
         uint64_t avail = (bm & avail_mask) >> avail_shift;
         uint64_t acks  = bm & ack_mask;

         // Pushable if both avail and ack bits are 0
         uint64_t pushable = ~avail & ~acks;
         return pushable;
      }

      /**
       * Enum defining the acknowledgment behavior for pop operations
       */
      enum class ack_mode
      {
         require_ack,  // Normal mode: requires producer acknowledgment after consumption
         skip_ack      // Skip acknowledgment: item is immediately made available for reuse
      };

      // Static constants for more convenient access to ack_mode values
      static constexpr ack_mode require_ack = ack_mode::require_ack;
      static constexpr ack_mode skip_ack    = ack_mode::skip_ack;

      /**
       * Wake up any blocked threads with interruption
       * 
       * This method should be called when the buffer is no longer needed or
       * when a clean shutdown is required.
       */
      void wake_blocked()
      {
         bool was_interrupted = interrupt_requested.load(std::memory_order_acquire);
         ARBTRIE_DEBUG("spmc_buffer::wake_blocked() - Setting interrupt flag, previous value=",
                       was_interrupted ? "true" : "false");

         // Set the interrupt flag
         interrupt_requested.store(true, std::memory_order_release);

         // Wake up any waiting threads
         bitmap.notify_all();

         ARBTRIE_DEBUG("spmc_buffer::wake_blocked() - Notified all waiters");
      }

      /**
       * Check if there's space to push more data
       * 
       * @return True if there's space available below the high water mark
       */
      bool can_push() const
      {
         uint64_t avail_bits = (bitmap.load(std::memory_order_acquire) & avail_mask) >> avail_shift;
         uint64_t used_slots_count = __builtin_popcountll(avail_bits);
         uint64_t high             = high_water_mark.load(std::memory_order_relaxed);

         return used_slots_count < high;
      }

      /**
       * Push data to the buffer (standard FIFO operation).
       * Only one thread can push at a time (the producer).
       * This is a non-blocking version that will return -1 if the buffer is full.
       * 
       * @param data The item to push to the buffer
       * @return The index where the item was stored, or -1 if buffer is full
       */
      int64_t push(T data)
      {
         // Look for a free slot by finding an unset bit in the available bitmap
         uint64_t avail_bits = (bitmap.load(std::memory_order_acquire) & avail_mask) >> avail_shift;
         uint64_t used_slots_count = __builtin_popcountll(avail_bits);
         uint64_t high             = high_water_mark.load(std::memory_order_relaxed);

         // If we've reached the high water mark, signal that the buffer is full
         // and adjust water marks to apply back pressure
         if (used_slots_count >= high)
         {
            // Mark producer as waiting - this is used for flow control adaptation
            producer_waiting.store(true, std::memory_order_release);

            // Adjust water marks to apply back pressure earlier
            adjust_water_marks(buffer_adjustment::less_buffering);
            return -1;
         }

         // Clear producer waiting flag if it was set
         producer_waiting.store(false, std::memory_order_release);

         // Look for a free slot (bit = 0 in the available bitmap)
         uint64_t free_slot = __builtin_ctzll(~avail_bits & ((1ULL << buffer_size) - 1));
         if (free_slot >= buffer_size)
            return -1;  // No free slots found

         // Write the data and make it available for consumers
         write_and_publish(free_slot, std::move(data));

         // If consumers are waiting, wake one up
         if (waiting_consumers.load(std::memory_order_acquire) > 0)
            bitmap.notify_one();

         return free_slot;
      }

      /**
       * Push data to the front of the buffer so it's the next item to be popped (LIFO behavior).
       * Only one thread can push at a time (the producer).
       * This is a non-blocking version that will return -1 if the buffer is full.
       * 
       * @param data The item to push to the front of the buffer
       * @return The index where the item was stored, or -1 if buffer is full
       */
      int64_t push_front(T data)
      {
         // Look for a free slot by finding an unset bit in the available bitmap
         uint64_t avail_bits = (bitmap.load(std::memory_order_acquire) & avail_mask) >> avail_shift;
         uint64_t used_slots_count = __builtin_popcountll(avail_bits);
         uint64_t high             = high_water_mark.load(std::memory_order_relaxed);

         // If we've reached the high water mark, signal that the buffer is full
         // and adjust water marks to apply back pressure
         if (used_slots_count >= high)
         {
            // Mark producer as waiting - this is used for flow control adaptation
            producer_waiting.store(true, std::memory_order_release);

            // Adjust water marks to apply back pressure earlier
            adjust_water_marks(buffer_adjustment::less_buffering);
            return -1;
         }

         // Clear producer waiting flag if it was set
         producer_waiting.store(false, std::memory_order_release);

         // Look for a free slot (bit = 0 in the available bitmap)
         uint64_t free_slot = __builtin_ctzll(~avail_bits & ((1ULL << buffer_size) - 1));
         if (free_slot >= buffer_size)
            return -1;  // No free slots found

         // Write the data and make it available for consumers
         write_and_publish(free_slot, std::move(data));

         // Mark the item as high priority
         priority_bits.fetch_or(1ULL << free_slot, std::memory_order_release);

         // If consumers are waiting, wake one up
         if (waiting_consumers.load(std::memory_order_acquire) > 0)
            bitmap.notify_one();

         return free_slot;
      }

      /**
       * Pop an item from the buffer in FIFO order
       * Prioritizes high priority items (pushed via push_front) when available.
       * 
       * @tparam AckModeT Whether to track the item for acknowledgment
       * @return Optional containing the consumed data if successful, empty optional if buffer was empty
       */
      template <ack_mode AckModeT = ack_mode::require_ack>
      std::optional<T> pop()
      {
         do
         {
            uint64_t current_bitmap = bitmap.load(std::memory_order_acquire);
            uint64_t available      = (current_bitmap & avail_mask) >> avail_shift;

            if (available == 0)
               return std::nullopt;  // No data available

            // Apply priority filtering - prefer high priority items
            uint64_t priorities    = priority_bits.load(std::memory_order_acquire);
            uint64_t filtered_bits = available;

            // If there are high priority items available, use only those
            if (available & priorities)
               filtered_bits &= priorities;

            // Find lowest available slot
            uint64_t bit_pos = __builtin_ctzll(filtered_bits);
            // Determine which bits to clear based on ack mode
            uint64_t bits_to_clear;
            if constexpr (AckModeT == ack_mode::require_ack)
            {
               // In require_ack mode, clear only the avail bit, leaving ack bit set
               bits_to_clear = avail_bit(bit_pos);
            }
            else
            {
               // In skip_ack mode, clear both avail and ack bits
               bits_to_clear = avail_and_ack_bits(bit_pos);
            }

            // Try to atomically clear the appropriate bits
            if (bitmap.fetch_and(~bits_to_clear, std::memory_order_release) & avail_bit(bit_pos))
            {
               // Only proceed if the avail bit was successfully cleared (it was set)

               // Check if we crossed the low water mark and producer is waiting
               uint64_t used = __builtin_popcountll(
                   (bitmap.load(std::memory_order_acquire) & avail_mask) >> avail_shift);
               uint64_t low = low_water_mark.load(std::memory_order_relaxed);

               auto data = buf[bit_pos];

               if (used <= low && producer_waiting.load(std::memory_order_acquire))
               {
                  bitmap.notify_one();
               }

               return data;
            }

            // If we get here, another thread claimed the slot before us
            // Try again with updated bitmap
         } while (true);
      }

      /**
       * Pop an item from the back of the buffer (LIFO order)
       * Prioritizes low priority items (pushed via regular push) when available.
       * 
       * @tparam AckModeT Whether to track the item for acknowledgment
       * @return Optional containing the consumed data if successful, empty optional if buffer was empty
       */
      template <ack_mode AckModeT = ack_mode::require_ack>
      std::optional<T> pop_back()
      {
         do
         {
            uint64_t current_bitmap = bitmap.load(std::memory_order_acquire);
            uint64_t available      = (current_bitmap & avail_mask) >> avail_shift;

            if (available == 0)
               return std::nullopt;  // No data available

            // Apply priority filtering - prefer low priority items
            uint64_t priorities    = priority_bits.load(std::memory_order_acquire);
            uint64_t filtered_bits = available;

            // If there are low priority items available, use only those
            if (available & ~priorities)
               filtered_bits &= ~priorities;

            // Find highest available slot (leftmost bit instead of rightmost)
            uint64_t bit_pos = 31 - __builtin_clzll(filtered_bits);  // Find leftmost 1 bit
            T        data    = buf[bit_pos];

            // Determine which bits to clear based on ack mode
            uint64_t bits_to_clear;
            if constexpr (AckModeT == ack_mode::require_ack)
            {
               // In require_ack mode, clear only the avail bit, leaving ack bit set
               bits_to_clear = avail_bit(bit_pos);
            }
            else
            {
               // In skip_ack mode, clear both avail and ack bits
               bits_to_clear = avail_and_ack_bits(bit_pos);
            }

            // Try to atomically clear the appropriate bits
            if (bitmap.fetch_and(~bits_to_clear, std::memory_order_release) & avail_bit(bit_pos))
            {
               // Only proceed if the avail bit was successfully cleared (it was set)

               // Check if we crossed the low water mark and producer is waiting
               uint64_t used = __builtin_popcountll(
                   (bitmap.load(std::memory_order_acquire) & avail_mask) >> avail_shift);
               uint64_t low = low_water_mark.load(std::memory_order_relaxed);

               if (used <= low && producer_waiting.load(std::memory_order_acquire))
               {
                  bitmap.notify_one();
               }

               return data;
            }

            // If we get here, another thread claimed the slot before us
            // Try again with updated bitmap
         } while (true);
      }

      /**
       * Blocking pop operation from the back (LIFO) that waits until data is available
       * 
       * @tparam AckModeT Whether to track the item for acknowledgment
       * @return The consumed data item
       * @throws thread_interrupted if the operation is interrupted
       */
      template <ack_mode AckModeT = ack_mode::require_ack>
      T pop_back_wait()
      {
         std::optional<T> result;
         while (!(result = pop_back<AckModeT>()))
         {
            // Check for interruption
            if (interrupt_requested.load(std::memory_order_acquire))
               throw thread_interrupted();

            // Increment waiting count before waiting
            waiting_consumers.fetch_add(1, std::memory_order_release);
            adjust_water_marks(
                buffer_adjustment::
                    more_buffering);  // Increase high water mark since consumer had to wait

            // The bitmap value is currently 0 in the available bits (no data)
            // Wait until any bits change - producer will update the bitmap and notify
            // We shift back since bitmap.wait expects the full value, not just the bits we're interested in
            uint64_t current_bits = (bitmap.load(std::memory_order_acquire) & avail_mask);
            bitmap.wait(current_bits);

            // Decrement waiting count after waking
            waiting_consumers.fetch_sub(1, std::memory_order_release);

            // Check for interruption after waking
            if (interrupt_requested.load(std::memory_order_acquire))
               throw thread_interrupted();
         }
         return *result;
      }

      /**
       * Blocking pop operation that waits until data is available
       * Prioritizes high priority items (pushed via push_front) when available.
       * 
       * @tparam AckModeT Whether to track the item for acknowledgment
       * @return The consumed data item
       * @throws thread_interrupted if the operation is interrupted
       */
      template <ack_mode AckModeT = ack_mode::require_ack>
      T pop_wait()
      {
         std::optional<T> result;
         while (!(result = pop<AckModeT>()))
         {
            // Check for interruption
            bool is_interrupted = interrupt_requested.load(std::memory_order_acquire);
            if (is_interrupted)
            {
               ARBTRIE_DEBUG("spmc_buffer::pop_wait() - Thread interrupted, throwing exception");
               throw thread_interrupted();
            }

            // Increment waiting count before waiting
            waiting_consumers.fetch_add(1, std::memory_order_release);

            // Try once more before waiting
            if ((result = pop<AckModeT>()))
            {
               waiting_consumers.fetch_sub(1, std::memory_order_release);
               return *result;
            }

            // Wait for a bit to become set
            bitmap.wait(bitmap.load(std::memory_order_acquire), std::memory_order_acquire);

            // Check again for interruption after wake up
            is_interrupted = interrupt_requested.load(std::memory_order_acquire);
            if (is_interrupted)
            {
               ARBTRIE_DEBUG(
                   "spmc_buffer::pop_wait() - Thread interrupted after waking, throwing exception");
               waiting_consumers.fetch_sub(1, std::memory_order_release);
               throw thread_interrupted();
            }

            // Decrement waiting count
            waiting_consumers.fetch_sub(1, std::memory_order_release);

            // Let the producer know if we've been waiting too long
            if (usage() < low_water_mark)
            {
               producer_waiting.store(true, std::memory_order_release);
               adjust_water_marks(buffer_adjustment::more_buffering);
            }
         }

         return *result;
      }

      /**
       * Get and acknowledge the next consumed but unacknowledged item from the buffer.
       * 
       * This method should only be called by the producer. It finds the next slot that 
       * has been consumed (bit in available_bits is 0) but not yet acknowledged 
       * (bit in ack_bits is 1). It returns the value at that location and clears the ack bit.
       * 
       * @return Optional containing the value if one was found, empty optional otherwise
       */
      std::optional<T> pop_ack()
      {
         uint64_t current_bitmap = bitmap.load(std::memory_order_acquire);
         uint64_t avail          = (current_bitmap & avail_mask) >> avail_shift;
         uint64_t acks           = current_bitmap & ack_mask;

         // Find positions that are both consumed (available bit = 0) and acked (ack bit = 1)
         uint64_t consumed_positions = ~avail & acks;

         if (consumed_positions == 0)
            return std::nullopt;  // No consumed items waiting for acknowledgement

         // Find the first consumed position
         uint64_t pos = __builtin_ctzll(consumed_positions);

         // Clear the ack bit
         clear_ack_bit(pos);

         // Return the value at this position
         return buf[pos];
      }

      // Utility and configuration methods

      /**
       * Get the number of free slots in the buffer from the producer's perspective
       * 
       * A slot is considered free only if:
       * 1. It is NOT available to consumers (not set in available_bits)
       * 2. It is NOT pending acknowledgment (not set in ack_bits)
       * 
       * This accurately reflects slots that can be used for new pushes.
       */
      uint64_t free_space() const
      {
         // Slots are occupied if they are either available to consumers OR pending acknowledgment
         uint64_t avail = (bitmap.load(std::memory_order_acquire) & avail_mask) >> avail_shift;
         uint64_t occupied_slots = avail | (bitmap.load(std::memory_order_acquire) & ack_mask);

         // Count the number of unoccupied slots
         uint64_t occupied_count = occupied_slots == 0 ? 0 : __builtin_popcountll(occupied_slots);
         return buffer_size - occupied_count;
      }

      /**
       * Get the number of slots currently in use from the producer's perspective
       * 
       * A slot is considered in use if:
       * 1. It is available to consumers (set in available_bits), OR
       * 2. It is pending acknowledgment (set in ack_bits)
       * 
       * @return Number of slots in use
       */
      uint64_t usage() const
      {
         // Slots are occupied if they are either available to consumers OR pending acknowledgment
         uint64_t avail = (bitmap.load(std::memory_order_acquire) & avail_mask) >> avail_shift;
         uint64_t occupied_slots = avail | (bitmap.load(std::memory_order_acquire) & ack_mask);

         // Count the number of occupied slots
         return occupied_slots == 0 ? 0 : __builtin_popcountll(occupied_slots);
      }

      /**
       * Get the number of consumed items pending acknowledgment
       * 
       * This indicates how many times pop_ack() can be called before it will return nullopt.
       * These are slots where:
       * 1. The bit is NOT set in available_bits (already consumed)
       * 2. The bit IS set in ack_bits (waiting for acknowledgment)
       * 
       * @return Number of consumed items pending acknowledgment
       */
      uint64_t pending_ack_count() const
      {
         uint64_t bm    = bitmap.load(std::memory_order_acquire);
         uint64_t avail = (bm & avail_mask) >> avail_shift;
         uint64_t acks  = bm & ack_mask;

         // Count slots that have ack bit set but avail bit cleared
         return __builtin_popcountll(acks & ~avail);
      }

      /**
       * Get the count of bits set in the ack bitmap
       * 
       * @return The number of bits set in the ack bitmap
       */
      uint64_t ack_count() const
      {
         uint64_t bm   = bitmap.load(std::memory_order_acquire);
         uint64_t acks = bm & ack_mask;

         // Count set bits in the ack portion of the bitmap
         return __builtin_popcountll(acks);
      }

      /**
       * Get total buffer capacity
       */
      static constexpr uint64_t capacity() { return buffer_size; }

      /**
       * Get the buffer's high water mark
       * 
       * @return The current high water mark value
       */
      uint64_t get_high_water_mark() const
      {
         return high_water_mark.load(std::memory_order_relaxed);
      }

      /**
       * Get the buffer's low water mark
       * 
       * @return The current low water mark value
       */
      uint64_t get_low_water_mark() const { return low_water_mark.load(std::memory_order_relaxed); }

      /**
       * Set the minimum gap between low and high water marks
       * 
       * @param new_gap The new minimum gap value (must be less than buffer_size)
       * @return True if the gap was set, false if the value was invalid
       */
      bool set_min_water_gap(uint64_t new_gap)
      {
         // Validate the new gap is within bounds
         if (new_gap == 0 || new_gap >= buffer_size)
            return false;

         // Set the new gap
         min_water_gap.store(new_gap, std::memory_order_relaxed);

         // Adjust high water mark if needed to maintain the gap
         uint64_t current_low  = low_water_mark.load(std::memory_order_relaxed);
         uint64_t current_high = high_water_mark.load(std::memory_order_relaxed);

         if (current_high < current_low + new_gap)
         {
            // High water mark too low, adjust it
            uint64_t new_high = std::min(current_low + new_gap, buffer_size - 1);
            high_water_mark.store(new_high, std::memory_order_relaxed);

            // Notify any waiting producer since thresholds have changed and they might
            // be able to proceed now with the new higher threshold
            bitmap.notify_one();
         }

         return true;
      }

      /**
       * Get the minimum gap between water marks
       */
      uint64_t get_min_water_gap() const { return min_water_gap.load(std::memory_order_relaxed); }

      /**
       * Get bitmap of available slots for debugging/testing
       */
      uint64_t get_available_bitmap() const
      {
         return (bitmap.load(std::memory_order_acquire) & avail_mask) >> avail_shift;
      }

      /**
       * Get bitmap of slots eligible for swapping by the producer
       * 
       * These are slots that:
       * 1. Are available (bit set in available_bits) - they contain data that consumers can read
       * 2. Do NOT have their ack bit set - they are NOT pending acknowledgment
       * 
       * @return Bitmap where each set bit represents a slot eligible for swapping
       */
      uint64_t get_swappable_bitmap() const
      {
         uint64_t bm    = bitmap.load(std::memory_order_acquire);
         uint64_t avail = (bm & avail_mask) >> avail_shift;
         uint64_t acks  = bm & ack_mask;

         // Slots eligible for swapping are those that are available AND NOT acked
         uint64_t swappable = avail & ~acks;
         return swappable;
      }

      /**
       * Access the value at a specific index
       * 
       * This method provides direct read-only access to values in the buffer.
       * Note: This should only be used by the producer thread.
       * 
       * @param index The index to access (must be less than buffer_size)
       * @return Reference to the value at the specified index
       */
      const T& values(uint64_t index) const { return buf[index & mask]; }

      /**
       * Attempt to atomically swap a value at a specific index
       * 
       * This method:
       * 1. Tries to consume the value at the specified index (if it's available and not acked)
       * 2. If successful, replaces it with new_value and makes it available again
       * 3. Returns the original value if successful, or nullopt if the slot was not swappable
       * 
       * Note: This should only be used by the producer thread.
       * 
       * @param index The index to swap (must be less than buffer_size)
       * @param new_value The value to store at this index if swap is successful
       * @return Optional containing the old value if swap succeeded, or nullopt if slot was not swappable
       */
      std::optional<T> try_swap(uint64_t index, T new_value)
      {
         // Mask index to buffer size
         index &= mask;

         // Get bitmap of slots eligible for swapping
         uint64_t swappable = get_swappable_bitmap();

         // Check if the specified slot is eligible for swapping (available and not acked)
         if (!(swappable & (1ULL << index)))
         {
            // Slot is not swappable - set the value and mark as available
            write_and_publish(index, new_value);

            // Return nullopt to indicate there was nothing swappable
            return std::nullopt;
         }

         // Read the current value (to return if successful)
         T old_value = buf[index];

         // Load the current availability bitmap
         uint64_t current = (bitmap.load(std::memory_order_acquire) & avail_mask) >> avail_shift;

         // Try to claim the slot by clearing its bit
         uint64_t new_bits = current & ~(1ULL << index);
         if (!bitmap.compare_exchange_strong(current, new_bits, std::memory_order_acquire))
         {
            // Another thread consumed this slot before we could
            // Try again with the original approach - set the value
            write_and_publish(index, new_value);

            // Return nullopt since we couldn't swap
            return std::nullopt;
         }

         // We've successfully claimed the slot, now replace the value and republish
         write_and_publish(index, new_value);

         return old_value;
      }

      /**
       * Convenience method that pops an item without requiring acknowledgment.
       * This provides a cleaner way to use the skip_ack mode.
       * 
       * @return The popped item
       */
      std::optional<T> pop_without_ack() { return pop<skip_ack>(); }

      /**
       * Convenience method that pops an item from the back without requiring acknowledgment.
       * This provides a cleaner way to use the skip_ack mode.
       * 
       * @return The popped item
       */
      std::optional<T> pop_back_without_ack() { return pop_back<skip_ack>(); }

      /**
       * Convenience method that waits for an item and pops it without requiring acknowledgment.
       * 
       * @return The popped item
       * @throws thread_interrupted if the operation is interrupted
       */
      T pop_wait_without_ack() { return pop_wait<skip_ack>(); }

      /**
       * Wait-and-pop the item from the back of the queue without checking for interruption
       * 
       * @return The popped item
       */
      T pop_back_wait_ignore_interrupt() { return pop_back_wait().first; }

      /**
       * Convenience method that pops an item, ignoring interruption status.
       * This method is not suitable for situations where interruptions need to be handled.
       * 
       * @return The popped item
       */
      T pop_wait_ignore_interrupt() { return pop_wait().first; }

      /**
       * Clear the interrupt flag for the buffer
       * 
       * This should be called after handling an interruption if you want
       * to continue using the buffer for new operations.
       */
      void clear_interrupt() { interrupt_requested.store(false, std::memory_order_release); }

      /**
       * Convenience method that waits for an item and pops it from the back without requiring acknowledgment.
       * 
       * @return The popped item
       * @throws thread_interrupted if the operation is interrupted
       */
      T pop_back_wait_without_ack() { return pop_back_wait<skip_ack>(); }

      /**
       * Reset the buffer to its initial state
       * 
       * This method clears all items from the buffer, resets water marks to their default values,
       * and wakes any blocked threads. After calling this method, the buffer will be in the same
       * state as if it was newly constructed.
       */
      void reset()
      {
         // Clear bitmap (both available and ack bits)
         bitmap.store(0, std::memory_order_relaxed);

         // Clear priority bits
         priority_bits.store(0, std::memory_order_relaxed);

         // Reset water marks to default values
         high_water_mark.store(default_high_water, std::memory_order_relaxed);
         low_water_mark.store(default_low_water, std::memory_order_relaxed);

         // Wake up any blocked threads
         wake_blocked();

         // Reset waiting state
         producer_waiting.store(false, std::memory_order_relaxed);

         // Important: Clear the interrupt flag after wake_blocked() sets it
         // This ensures we don't leave the buffer in an interrupted state
         clear_interrupt();

         // Note: we don't need to clear the data array (buf) as items
         // are only accessible when their corresponding bits are set in the bitmap
      }
   };
}  // namespace arbtrie