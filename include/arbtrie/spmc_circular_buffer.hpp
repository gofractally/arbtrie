#pragma once

#include <algorithm>  // for std::min
#include <arbtrie/padded_atomic.hpp>
#include <array>
#include <atomic>

namespace arbtrie
{
   /**
    * Single-producer, multiple-consumer circular buffer implementation with exactly 64 slots
    * 
    * This class implements a fixed-size circular buffer that allows concurrent access from one producer 
    * and multiple consumer threads without requiring explicit locks. It uses atomic operations and cache line 
    * padding to provide efficient thread-safe communication.
    * 
    * Algorithm:
    * The buffer uses a 64-bit atomic bitmap to track slot availability, where:
    * - Bit 0 = slot is empty and available for producer
    * - Bit 1 = slot contains data and is available for consumers
    * 
    * Producer algorithm:
    * 1. Load bitmap
    * 2. Find first free slot (rightmost 0 bit) using count trailing zeros
    * 3. Write data to that slot
    * 4. Set bit to 1 (release) to indicate data is available
    * 5. If usage exceeds high water mark:
    *    - Decrease high water mark to apply back-pressure
    *    - Wait until usage drops below low water mark
    * 
    * Consumer algorithm:
    * 1. Load bitmap (acquire)
    * 2. Find rightmost 1 bit using count trailing zeros
    * 3. Speculatively read data from that slot
    * 4. Try to atomically clear bit to 0
    * 5. If successful:
    *    - Check if usage dropped below low water mark
    *    - Notify producer if it's waiting
    * 6. If unsuccessful (another consumer claimed it), retry from step 1
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
    * - Consumers can wait using atomic wait when buffer is empty
    * - Notifications wake waiting threads when conditions change
    * - Wait counts track number of waiting threads for efficient notification
    * 
    * Memory ordering:
    * - Producer uses release when setting bits
    * - Consumer uses acquire when loading bitmap and clearing bits
    * - This ensures consumers see data written by producer
    * - Water marks use relaxed ordering as they only affect performance
    * 
    * The buffer size is fixed at 64 slots to match the bitmap size, allowing:
    * - Efficient bit operations for slot management
    * - No need for separate read/write positions
    * - Lock-free operation for all threads
    * - FIFO ordering for better predictability
    * 
    * Memory layout:
    * - Bitmap is cache-line aligned to prevent false sharing as it's modified by all threads
    * - Data slots are packed normally since only the producer writes to them
    * 
    * Contention reduction:
    * - Producer fills from low bits (0) upward
    * - Consumers claim from low bits (0) upward
    * - This aligned direction ensures FIFO ordering
    * - Natural flow maintains data ordering and fairness
    */
   template <typename T>
   class spmc_circular_buffer
   {
     private:
      static constexpr uint64_t buffer_size = 64;
      static constexpr uint64_t mask        = buffer_size - 1;

      // Default thresholds and constraints
      static constexpr uint64_t default_high_water = buffer_size * 3 / 4;  // 75% full
      static constexpr uint64_t default_low_water  = buffer_size / 4;      // 25% full
      static constexpr uint64_t default_min_gap    = 8;  // Default minimum items between marks
      static constexpr uint64_t max_high_water     = buffer_size - 1;  // Never completely full

      std::array<T, buffer_size> buf;             // The actual data storage
      padded_atomic<uint64_t> available_bits{0};  // Bitmap tracking available slots (1 = available)

      // Adaptive water marks and control
      std::atomic<uint64_t> high_water_mark{default_high_water};  // Adapts based on wait patterns
      std::atomic<uint64_t> low_water_mark{default_low_water};    // Always maintains gap below high
      std::atomic<uint64_t> min_water_gap{default_min_gap};       // Minimum required gap

      // Synchronization flags
      std::atomic<bool> producer_waiting{false};  // Set when producer is waiting for low water
      std::atomic<int>  waiting_consumers{0};     // Count of consumers waiting for data

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
            if (current_high < max_high_water)
            {
               high_water_mark.fetch_add(1, std::memory_order_relaxed);
               // Ensure low mark doesn't get too close to buffer size
               if (current_low < (max_high_water - current_gap))
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

      // Helper function to set a bit in the bitmap
      void set_bit(uint64_t index)
      {
         uint64_t bit = 1ULL << index;
         available_bits.fetch_add(bit, std::memory_order_release);
      }

      // Helper to count used slots
      uint64_t used_slots() const
      {
         uint64_t bits = available_bits.load(std::memory_order_acquire);
         return bits == 0 ? 0 : __builtin_popcountll(bits);
      }

     public:
      spmc_circular_buffer()
      {
         // Initial assertions to validate configuration
         static_assert(default_min_gap > 0, "Default minimum water gap must be positive");
         static_assert(default_high_water > (default_low_water + default_min_gap),
                       "Default water marks must respect minimum gap");
         static_assert(max_high_water < buffer_size,
                       "Maximum high water must be less than buffer size");
      }

      // Push data into the buffer, only one thread can push at a time
      // Returns false if buffer is full
      bool push(T data)
      {
         uint64_t bits     = available_bits.load(std::memory_order_acquire);
         uint64_t inverted = ~bits;
         if (inverted == 0)
         {
            return false;  // No free slots (all bits are 1)
         }

         uint64_t free_slot = __builtin_ctzll(inverted);  // Find first free slot from low bits

         // Write the data
         buf[free_slot] = data;

         // Mark as available
         set_bit(free_slot);

         // Notify any waiting consumers after the slot is fully ready
         if (waiting_consumers.load(std::memory_order_acquire) > 0)
            available_bits.notify_one();

         // After pushing, check if we need to wait for low water mark
         bits          = available_bits.load(std::memory_order_acquire);
         uint64_t used = bits == 0 ? 0 : __builtin_popcountll(bits);
         uint64_t high = high_water_mark.load(std::memory_order_relaxed);

         if (used >= high)
         {
            // Producer forced to wait - decrease high water mark
            adjust_water_marks(buffer_adjustment::less_buffering);

            while (true)
            {
               bits         = available_bits.load(std::memory_order_acquire);
               used         = bits == 0 ? 0 : __builtin_popcountll(bits);
               uint64_t low = low_water_mark.load(std::memory_order_relaxed);

               if (used < low)
               {
                  break;  // Exit when usage drops below low water mark
               }

               // Set waiting flag only when we know we will actually wait
               producer_waiting.store(true, std::memory_order_release);
               available_bits.wait(bits);  // Wait on the exact value we checked
               producer_waiting.store(false, std::memory_order_release);
            }
         }

         return true;
      }

      // Try to consume a single element from the buffer
      // Returns true if successful, false if no data available
      bool try_consume(T& out_data)
      {
         do
         {
            uint64_t current = available_bits.load(std::memory_order_acquire);
            if (current == 0)
            {
               return false;  // No data available
            }

            // Find lowest available slot and read speculatively
            uint64_t bit_pos = __builtin_ctzll(current);  // Find rightmost 1 bit
            out_data         = buf[bit_pos];              // Speculative read

            // Try to claim it by clearing the bit
            uint64_t new_bits = current & ~(1ULL << bit_pos);
            if (available_bits.compare_exchange_weak(current, new_bits, std::memory_order_acquire))
            {
               // Check if we crossed the low water mark and producer is waiting
               uint64_t used = new_bits == 0 ? 0 : __builtin_popcountll(new_bits);
               uint64_t low  = low_water_mark.load(std::memory_order_relaxed);

               if (used <= low && producer_waiting.load(std::memory_order_acquire))
               {
                  available_bits.notify_one();
               }
               return true;
            }
         } while (true);
      }

      // Blocking consume that waits until data is available using atomic wait
      void consume(T& out_data)
      {
         while (!try_consume(out_data))
         {
            // Increment waiting count before waiting
            waiting_consumers.fetch_add(1, std::memory_order_release);
            adjust_water_marks(
                buffer_adjustment::
                    more_buffering);  // Increase high water mark since consumer had to wait
            available_bits.wait(0);   // We know bits is 0 since try_consume returned false
            // Decrement waiting count after waking
            waiting_consumers.fetch_sub(1, std::memory_order_release);
         }
      }

      // Get the number of free slots in the buffer
      uint64_t free_space() const
      {
         uint64_t bits = available_bits.load(std::memory_order_acquire);
         return bits == 0 ? buffer_size : buffer_size - __builtin_popcountll(bits);
      }

      // Get bitmap of available slots for debugging/testing
      uint64_t get_available_bitmap() const
      {
         return available_bits.load(std::memory_order_acquire);
      }

      // Get current usage level
      uint64_t usage() const { return used_slots(); }

      // Get current water marks
      uint64_t get_high_water_mark() const
      {
         return high_water_mark.load(std::memory_order_relaxed);
      }
      uint64_t get_low_water_mark() const { return low_water_mark.load(std::memory_order_relaxed); }

      // Set new minimum gap between water marks
      // Adjusts high water mark if needed to maintain the gap
      bool set_min_water_gap(uint64_t new_gap)
      {
         // Validate the new gap is within bounds
         if (new_gap == 0 || new_gap >= buffer_size)
            return false;

         uint64_t current_high = high_water_mark.load(std::memory_order_relaxed);
         uint64_t current_low  = low_water_mark.load(std::memory_order_relaxed);

         // Store the new gap first
         min_water_gap.store(new_gap, std::memory_order_relaxed);

         // If current gap is too small, increase high water mark
         if ((current_high - current_low) < new_gap)
         {
            uint64_t target_high = current_low + new_gap;
            // Don't exceed maximum allowed high water
            target_high = std::min(target_high, max_high_water);
            high_water_mark.store(target_high, std::memory_order_relaxed);

            // Since we increased the high water mark, notify the producer
            // who might be waiting on the previous lower threshold
            available_bits.notify_one();
         }

         return true;
      }

      // Get current minimum gap setting
      uint64_t get_min_water_gap() const { return min_water_gap.load(std::memory_order_relaxed); }
   };
}  // namespace arbtrie