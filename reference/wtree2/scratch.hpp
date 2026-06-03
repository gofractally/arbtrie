#pragma once
// wtree2/scratch.hpp — thread-local bump arena for transient scratch.
//
// HARD RULE: no heap allocations in the data-structure hot paths. Where a routine
// needs a dynamically-sized temporary (e.g. the divider/child arrays in
// bulk_build, or a gather buffer in a rare insert path), it bumps this static
// per-thread arena instead of std::vector. Each routine takes a `scratch_mark` at
// entry; the destructor rewinds the bump pointer, so nested/recursive use is a
// stack discipline with zero allocation after the one-time static reservation.

#include <cstddef>
#include <cstdint>
#include <new>

namespace psitri::wtree2 {

// 64 MB per thread, static storage (not a heap allocation per op). Sized to hold
// the widest transient: a top-level bulk_build over ~10M keys has ~100K dividers
// (+children) ≈ <1 MB; recursion is shallow. 64 MB is comfortable headroom.
inline constexpr std::size_t kScratchBytes = 64ull << 20;

struct scratch_arena
{
    alignas(64) uint8_t buf[kScratchBytes];
    std::size_t off = 0;
};
inline thread_local scratch_arena g_scratch;

// RAII rewind: take at routine entry, frees everything bumped during the routine.
struct scratch_mark
{
    std::size_t saved;
    scratch_mark() noexcept : saved(g_scratch.off) {}
    ~scratch_mark() noexcept { g_scratch.off = saved; }
    scratch_mark(const scratch_mark&)            = delete;
    scratch_mark& operator=(const scratch_mark&) = delete;
};

// Bump `n` T's (aligned). Returns nullptr only if the arena is exhausted (a bug —
// the arena is sized to never need that in practice).
template <typename T>
inline T* scratch_alloc(std::size_t n) noexcept
{
    constexpr std::size_t a   = alignof(T);
    std::size_t           cur = (g_scratch.off + (a - 1)) & ~(a - 1);
    const std::size_t     end = cur + n * sizeof(T);
    if (end > kScratchBytes) [[unlikely]] return nullptr;
    g_scratch.off = end;
    return reinterpret_cast<T*>(g_scratch.buf + cur);
}

// A minimal arena-backed push-only vector for the few spots that grow by push.
template <typename T>
struct scratch_vec
{
    T*          data_ = nullptr;
    std::size_t size_ = 0;
    std::size_t cap_  = 0;

    explicit scratch_vec(std::size_t reserve) noexcept
        : data_(scratch_alloc<T>(reserve)), cap_(reserve)
    {
    }
    void   push_back(const T& v) noexcept { data_[size_++] = v; }   // caller guarantees cap
    T&     operator[](std::size_t i) noexcept { return data_[i]; }
    const T& operator[](std::size_t i) const noexcept { return data_[i]; }
    std::size_t size() const noexcept { return size_; }
    T*     data() noexcept { return data_; }
    const T* data() const noexcept { return data_; }
};

}  // namespace psitri::wtree2
