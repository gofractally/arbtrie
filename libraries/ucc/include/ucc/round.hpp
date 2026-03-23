#pragma once
#include <bit>
#include <cassert>
namespace ucc
{
   template <unsigned int N, typename T>
   constexpr T round_up_multiple(T v)
   {
      static_assert(std::popcount(N) == 1, "N must be power of 2");
      return (v + (T(N) - 1)) & -T(N);
   }
   template <typename T>
   constexpr T round_up_multiple(T v, T N)
   {
      assert(std::popcount(N) == 1 && "N must be power of 2");
      return (v + (N - 1)) & -N;
   }

   template <unsigned int N, typename T>
   constexpr T round_down_multiple(T v)
   {
      static_assert(std::popcount(N) == 1, "N must be power of 2");
      return v & ~(N - 1);
   }
}  // namespace ucc