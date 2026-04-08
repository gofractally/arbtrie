#pragma once
#include <art/arena.hpp>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace art
{
   /// Heap-backed bump allocator with the same interface as `arena`.
   ///
   /// Uses `std::vector<char>` instead of a 4 GB mmap reservation.
   /// Suitable for writer-private buffers where no concurrent readers
   /// need a stable base pointer. Grows by doubling on demand.
   ///
   /// IMPORTANT: `base()` may change after `allocate()` if the vector
   /// grows. All access must go through `as<T>(offset)` — do not cache
   /// raw pointers across allocations.
   class heap_arena
   {
     public:
      explicit heap_arena(uint32_t initial_capacity = 1u << 20)
      {
         _data.reserve(initial_capacity);
      }

      ~heap_arena() = default;

      heap_arena(heap_arena&&) noexcept            = default;
      heap_arena& operator=(heap_arena&&) noexcept = default;
      heap_arena(const heap_arena&)                = delete;
      heap_arena& operator=(const heap_arena&)     = delete;

      /// Allocate `size` bytes, cacheline-rounded and cacheline-aligned.
      /// Returns a byte offset.
      offset_t allocate(uint32_t size)
      {
         uint32_t rounded = (size + cacheline_size - 1) & ~(cacheline_size - 1);
         uint32_t off     = _cursor;
         uint32_t end     = off + rounded;
         if (end < off) [[unlikely]]
            throw std::length_error("heap_arena: cursor + size wraps uint32_t");
         if (end > _data.size())
            _data.resize(end);
         _cursor = end;
         return off;
      }

      template <typename T>
      T* as(offset_t off) noexcept
      {
         return reinterpret_cast<T*>(_data.data() + off);
      }

      template <typename T>
      const T* as(offset_t off) const noexcept
      {
         return reinterpret_cast<const T*>(_data.data() + off);
      }

      char*       base() noexcept { return _data.data(); }
      const char* base() const noexcept { return _data.data(); }

      /// Reset cursor. Keeps allocated capacity for reuse.
      void clear() noexcept
      {
         _cursor = 0;
      }

      uint32_t bytes_used() const noexcept { return _cursor; }

     private:
      std::vector<char> _data;
      uint32_t          _cursor = 0;
   };

}  // namespace art
