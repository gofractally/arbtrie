#pragma once
#include <atomic>
#include <new>

#ifdef __cpp_lib_hardware_interference_size
static constexpr size_t ARBTRIE_CACHE_LINE_SIZE = std::hardware_destructive_interference_size;
#else
static constexpr size_t ARBTRIE_CACHE_LINE_SIZE = 64;
#endif

namespace arbtrie
{

   template <typename T>
   struct padded_atomic : public std::atomic<T>
   {
      char padding[ARBTRIE_CACHE_LINE_SIZE - sizeof(std::atomic<T>)];
   } __attribute__((__aligned__(ARBTRIE_CACHE_LINE_SIZE)));

   static_assert(sizeof(padded_atomic<uint64_t>) == ARBTRIE_CACHE_LINE_SIZE);

}  // namespace arbtrie