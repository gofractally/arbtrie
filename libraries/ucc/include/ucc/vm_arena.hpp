#pragma once
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <new>
#include <stdexcept>

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace ucc
{
   /// Virtual-memory-backed bump allocator with a stable base pointer.
   ///
   /// Reserves 4 GB of virtual address space upfront (no physical memory cost).
   /// Commits pages on demand in 2 MB chunks. The base pointer never moves,
   /// making it safe for concurrent readers to dereference previously-allocated
   /// offsets while the writer allocates new ones.
   ///
   /// Allocations are aligned to `alignment` bytes (default 64, cache-line).
   /// Individual allocations cannot be freed; memory is reclaimed in bulk
   /// via clear(), which releases physical pages back to the OS.
   class vm_arena
   {
     public:
      static constexpr uint64_t reservation_size = 4ULL << 30;   // 4 GB
      static constexpr uint32_t commit_granularity = 2u << 20;   // 2 MB

#if defined(__APPLE__) && defined(__aarch64__)
      static constexpr uint32_t alignment = 128;
#else
      static constexpr uint32_t alignment = 64;
#endif

      using offset_t = uint32_t;

      vm_arena()
      {
#ifdef _WIN32
         _base = static_cast<char*>(
             VirtualAlloc(nullptr, reservation_size, MEM_RESERVE, PAGE_NOACCESS));
         if (!_base)
            throw std::bad_alloc();
#else
         _base = static_cast<char*>(mmap(nullptr, reservation_size, PROT_NONE,
                                         MAP_ANONYMOUS | MAP_PRIVATE, -1, 0));
         if (_base == MAP_FAILED)
         {
            _base = nullptr;
            throw std::bad_alloc();
         }
#endif
         _cursor    = 0;
         _committed = 0;
      }

      ~vm_arena()
      {
         if (_base)
         {
#ifdef _WIN32
            VirtualFree(_base, 0, MEM_RELEASE);
#else
            munmap(_base, reservation_size);
#endif
         }
      }

      vm_arena(vm_arena&& o) noexcept
          : _base(o._base), _cursor(o._cursor), _committed(o._committed)
      {
         o._base      = nullptr;
         o._cursor    = 0;
         o._committed = 0;
      }

      vm_arena& operator=(vm_arena&& o) noexcept
      {
         if (this != &o)
         {
            if (_base)
            {
#ifdef _WIN32
               VirtualFree(_base, 0, MEM_RELEASE);
#else
               munmap(_base, reservation_size);
#endif
            }
            _base        = o._base;
            _cursor      = o._cursor;
            _committed   = o._committed;
            o._base      = nullptr;
            o._cursor    = 0;
            o._committed = 0;
         }
         return *this;
      }

      vm_arena(const vm_arena&)            = delete;
      vm_arena& operator=(const vm_arena&) = delete;

      /// Allocate `size` bytes, rounded up to alignment boundary.
      /// Returns a byte offset into the arena.
      /// @throws std::length_error if the arena would exceed 4 GB.
      offset_t allocate(uint32_t size)
      {
         uint32_t rounded = (size + alignment - 1) & ~(alignment - 1);
         uint32_t off     = _cursor;
         uint32_t end     = off + rounded;
         if (end < off) [[unlikely]]
            throw std::length_error("vm_arena: cursor + size wraps uint32_t");
         if (end > _committed) [[unlikely]]
            grow_committed(end);
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

      /// Reset the arena. Releases all physical pages back to the OS.
      /// The virtual reservation and base pointer remain valid.
      void clear() noexcept
      {
         if (_committed > 0)
         {
#ifdef _WIN32
            VirtualFree(_base, _committed, MEM_DECOMMIT);
#elif defined(__linux__)
            madvise(_base, _committed, MADV_DONTNEED);
            mprotect(_base, _committed, PROT_NONE);
#else
            // macOS and other POSIX
            madvise(_base, _committed, MADV_FREE);
            mprotect(_base, _committed, PROT_NONE);
#endif
         }
         _cursor    = 0;
         _committed = 0;
      }

      uint32_t bytes_used() const noexcept { return _cursor; }
      uint32_t committed() const noexcept { return _committed; }

     private:
      char*    _base      = nullptr;
      uint32_t _cursor    = 0;
      uint32_t _committed = 0;

      void grow_committed(uint32_t needed)
      {
         // Round up to commit granularity (2 MB)
         uint64_t new_committed =
             (static_cast<uint64_t>(needed) + commit_granularity - 1) &
             ~(static_cast<uint64_t>(commit_granularity) - 1);
         if (new_committed > reservation_size)
            throw std::length_error("vm_arena: would exceed 4 GB reservation");
         uint32_t new_committed32 = static_cast<uint32_t>(
             std::min(new_committed, static_cast<uint64_t>(UINT32_MAX)));

#ifdef _WIN32
         void* result = VirtualAlloc(_base + _committed,
                                     new_committed32 - _committed,
                                     MEM_COMMIT, PAGE_READWRITE);
         if (!result)
            throw std::bad_alloc();
#else
         int ret = mprotect(_base + _committed,
                            new_committed32 - _committed,
                            PROT_READ | PROT_WRITE);
         if (ret != 0)
            throw std::bad_alloc();
#endif
         _committed = new_committed32;
      }
   };

}  // namespace ucc
