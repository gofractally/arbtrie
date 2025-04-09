#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <optional>
#include <ucc/padded_atomic.hpp>

namespace ucc
{
   /**
    * @brief A lock-free single-producer single-consumer circular buffer implementation
    * 
    * This class implements a fixed-size circular buffer that allows concurrent access from one producer 
    * and one consumer thread without requiring explicit locks. It uses atomic operations and cache line 
    * padding to provide efficient thread-safe communication.
    *
    * The buffer size must be a power of 2 to allow efficient wrapping using bitwise operations.
    * By default, the buffer stores 32-bit unsigned integers, but can store any type T.
    *
    * Key features:
    * - Lock-free implementation using atomic operations
    * - Cache line padding to prevent false sharing
    * - Fixed size buffer with power-of-2 size requirement
    * - Single producer / single consumer design
    * - Non-blocking push and pop operations
    *
    * Usage:
    * The producer thread calls push() to add data while the consumer thread calls pop() to retrieve data.
    * If the buffer is full, push() will return false. If the buffer is empty, pop() will return 0.
    *
    * This buffer is used within the arbtrie library for efficient inter-thread communication, particularly
    * for passing read node IDs from read threads to the compact thread so that they can be moved to
    * the pinned RAM cache.
    *
    * @tparam T The type of elements stored in the buffer, defaults to uint32_t
    * @tparam buffer_size The size of the circular buffer, must be a power of 2
    */
   template <typename T, uint64_t buffer_size>
   class circular_buffer
   {
     private:
      static_assert((buffer_size & (buffer_size - 1)) == 0, "buffer_size must be a power of 2");
      static constexpr uint64_t mask = buffer_size - 1;

      std::array<T, buffer_size> buf;          // The actual data storage
      padded_atomic<uint64_t>    push_pos{0};  // Push position, on its own cache line
      padded_atomic<uint64_t>    read_pos{0};  // Read position

     public:
      circular_buffer()                                  = default;
      circular_buffer(const circular_buffer&)            = delete;
      circular_buffer& operator=(const circular_buffer&) = delete;

      static constexpr uint64_t npos = uint64_t(-1);
      bool                      is_full() const { return usage() >= buffer_size; }

      // Push data into the buffer, only one thread can push at a time
      // @return the virtual index after the push, or throw if the buffer is full
      uint64_t push(T data)
      {
         uint64_t current_push = push_pos.load(std::memory_order_relaxed);
         uint64_t current_read = read_pos.load(std::memory_order_acquire);

         // Check if we are more than buffer_size ahead of the read position
         if (current_push - current_read >= buffer_size)
            throw std::runtime_error("circular_buffer overflow");

         buf[current_push & mask] = data;
         current_push++;
         push_pos.store(current_push, std::memory_order_release);
         return current_push;
      }

      // Pop a single element from the buffer, only one thread can call it
      std::optional<T> try_pop()
      {
         uint64_t current_read = read_pos.load(std::memory_order_relaxed);
         uint64_t current_push = push_pos.load(std::memory_order_acquire);

         if (current_read == current_push)
            return std::nullopt;  // No data available

         T out_data = buf[current_read & mask];
         read_pos.store(current_read + 1, std::memory_order_relaxed);
         return out_data;
      }

      // Read data from the buffer into provided buffer
      std::size_t pop(T* out_buffer, std::size_t max_size)
      {
         uint64_t current_read = read_pos.load(std::memory_order_relaxed);
         uint64_t current_push = push_pos.load(std::memory_order_acquire);

         if (current_read == current_push)
            return 0;  // No new data to read

         uint64_t available = current_push - current_read;  // Safe due to only incrementing push
         uint64_t to_read   = std::min(available, static_cast<uint64_t>(max_size));

         // Calculate how many items we can read in one contiguous block
         uint64_t first_block_size = std::min(to_read, buffer_size - (current_read & mask));

         // Copy the first contiguous block
         std::memcpy(out_buffer, &buf[current_read & mask], first_block_size * sizeof(T));

         // If there's more to read after wrapping around
         if (first_block_size < to_read)
         {
            // Copy the wrapping part
            std::memcpy(out_buffer + first_block_size, &buf[0],
                        (to_read - first_block_size) * sizeof(T));
         }

         // Atomically update read position
         read_pos.store(current_read + to_read, std::memory_order_relaxed);
         return to_read;
      }

      /**
       * Access an element at a specific position in the buffer,
       * beware that this is not thread safe, and should only be used if
       * there is not concurrent access to the buffer.
       * 
       * @param pos The absolute position to access
       * @return Reference to the element at the specified position
       */
      T at(uint64_t pos) const { return buf[pos & mask]; }

      /**
       * Get the current push position (write position)
       * 
       * @return The current push position as a uint64_t
       */
      uint64_t get_push_pos() const { return push_pos.load(std::memory_order_acquire); }

      /**
       * Get the current read position
       * 
       * @return The current read position as a uint64_t
       */
      uint64_t get_read_pos() const { return read_pos.load(std::memory_order_acquire); }

      /**
       * Get the number of available elements in the buffer
       * 
       * @return Number of elements available for reading
       */
      uint64_t usage() const
      {
         uint64_t current_push = push_pos.load(std::memory_order_acquire);
         uint64_t current_read = read_pos.load(std::memory_order_acquire);
         return current_push - current_read;
      }

      uint64_t capacity() const { return buffer_size; }
      uint64_t free_space() const { return capacity() - usage(); }
   };

}  // namespace ucc