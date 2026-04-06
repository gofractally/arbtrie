#pragma once
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <new>
#include <stdexcept>
#include <utility>

namespace art
{
#if defined(__APPLE__) && defined(__aarch64__)
   static constexpr uint32_t cacheline_size = 128;
#else
   static constexpr uint32_t cacheline_size = 64;
#endif

   // ── 32-bit byte offset ───────────────────────────────────────────────────
   // Direct byte offset into the arena. All allocations are cacheline-aligned.
   // Index 0xFFFFFFFF is reserved as null.

   using offset_t = uint32_t;
   static constexpr offset_t null_offset = 0xFFFFFFFF;

   // ── Tagged pointer helpers ────────────────────────────────────────────────
   // Bit 0 distinguishes leaf (tagged) from inner node (untagged).
   // All allocations are cacheline-aligned so bit 0 is always free.

   inline bool is_leaf(offset_t off) noexcept { return off & 1; }
   inline offset_t tag_leaf(offset_t off) noexcept { return off | 1; }
   inline offset_t untag_leaf(offset_t off) noexcept { return off & ~offset_t(1); }

   // ── Bump arena — all allocations cacheline-aligned ────────────────────────

   class arena
   {
     public:
      explicit arena(uint32_t initial_capacity = 1u << 20) : _capacity(initial_capacity)
      {
         _base = static_cast<char*>(std::malloc(_capacity));
         assert(_base);
         // Start at cacheline boundary; offset 0 is valid (not null)
         _cursor = 0;
      }

      ~arena() { std::free(_base); }

      arena(arena&& o) noexcept
          : _base(o._base), _cursor(o._cursor), _capacity(o._capacity)
      {
         o._base     = nullptr;
         o._cursor   = 0;
         o._capacity = 0;
      }

      arena& operator=(arena&& o) noexcept
      {
         if (this != &o)
         {
            std::free(_base);
            _base       = o._base;
            _cursor     = o._cursor;
            _capacity   = o._capacity;
            o._base     = nullptr;
            o._cursor   = 0;
            o._capacity = 0;
         }
         return *this;
      }

      arena(const arena&)            = delete;
      arena& operator=(const arena&) = delete;

      /// Allocate `size` bytes, cacheline-rounded and cacheline-aligned.
      /// Returns a byte offset.
      /// @throws std::length_error if the arena would exceed its uint32_t address space.
      offset_t allocate(uint32_t size)
      {
         uint32_t rounded = (size + cacheline_size - 1) & ~(cacheline_size - 1);
         uint32_t off     = _cursor;
         uint32_t end     = off + rounded;
         if (end < off) [[unlikely]]
            throw std::length_error("ART arena: cursor + size wraps uint32_t");
         if (end > _capacity) [[unlikely]]
            grow(end);
         _cursor = end;
         return off;
      }

      template <typename T>
      T* as(offset_t off) noexcept
      {
         return reinterpret_cast<T*>(_base + off);
      }

      template <typename T>
      const T* as(offset_t off) const noexcept
      {
         return reinterpret_cast<const T*>(_base + off);
      }

      char*       base() noexcept { return _base; }
      const char* base() const noexcept { return _base; }

      /// Reset arena cursor, reuse memory without freeing.
      void clear() noexcept { _cursor = 0; }

      uint32_t bytes_used() const noexcept { return _cursor; }
      uint32_t capacity() const noexcept { return _capacity; }

     private:
      char*    _base;
      uint32_t _cursor;
      uint32_t _capacity;

      /// @throws std::length_error if the arena would exceed uint32_t max.
      void grow(uint32_t needed)
      {
         uint64_t new_cap = _capacity;
         if (new_cap == 0)
            new_cap = 1u << 20;
         while (new_cap < needed)
            new_cap *= 2;
         if (new_cap > UINT32_MAX)
            throw std::length_error("ART arena: capacity would exceed 4 GB");
         _capacity = static_cast<uint32_t>(new_cap);
         _base     = static_cast<char*>(std::realloc(_base, _capacity));
         if (!_base)
            throw std::bad_alloc();
      }
   };

}  // namespace art
