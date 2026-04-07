#pragma once
#include <ucc/vm_arena.hpp>
#include <cstdint>

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

   // ── Virtual-memory-backed bump arena ─────────────────────────────────────
   // Thin wrapper around ucc::vm_arena that enforces cacheline-aligned
   // allocations. The base pointer is stable for the arena's lifetime
   // (no realloc), making it safe for concurrent readers.

   class arena
   {
     public:
      explicit arena(uint32_t /*initial_capacity*/ = 1u << 20) {}

      ~arena() = default;

      arena(arena&&) noexcept            = default;
      arena& operator=(arena&&) noexcept = default;
      arena(const arena&)                = delete;
      arena& operator=(const arena&)     = delete;

      /// Allocate `size` bytes, cacheline-rounded and cacheline-aligned.
      /// Returns a byte offset.
      /// @throws std::length_error if the arena would exceed its 4 GB address space.
      offset_t allocate(uint32_t size)
      {
         uint32_t rounded = (size + cacheline_size - 1) & ~(cacheline_size - 1);
         return _vm.allocate(rounded);
      }

      template <typename T>
      T* as(offset_t off) noexcept
      {
         return _vm.as<T>(off);
      }

      template <typename T>
      const T* as(offset_t off) const noexcept
      {
         return _vm.as<T>(off);
      }

      char*       base() noexcept { return _vm.base(); }
      const char* base() const noexcept { return _vm.base(); }

      /// Reset arena cursor, reuse memory without unmapping.
      void clear() noexcept { _vm.clear(); }

      uint32_t bytes_used() const noexcept { return _vm.bytes_used(); }

     private:
      ucc::vm_arena _vm;
   };

}  // namespace art
