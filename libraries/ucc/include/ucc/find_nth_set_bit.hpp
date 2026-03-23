#pragma once

#include <assert.h>
#include <stdint.h>

/// general case for all CPUs

namespace ucc
{

/**
   What is _pdep_u64?
   _pdep_u64 is an intrinsic from the BMI2 (Bit Manipulation Instruction Set 2) extension, available on modern x86 processors (e.g., Intel Haswell or later, AMD Zen or later).

   It stands for "parallel deposit." It takes two 64-bit arguments:
   The first argument (1ULL << n) provides the bits to deposit.

   The second argument (x) acts as a mask, specifying where those bits should be placed.

   Specifically, _pdep_u64 scatters the bits of the first argument into the positions of the set bits (1s) in the second argument, from least significant to most significant.

   Breaking Down the Operation:
   1ULL << n creates a 64-bit value with a single bit set at position n (counting from the least significant bit, zero-based).
   For n = 0, this is 0b0001 (bit 0 set).

   For n = 1, this is 0b0010 (bit 1 set).

   For n = 3, this is 0b1000 (bit 3 set).

   _pdep_u64(1ULL << n, x) then deposits that single 1 into the nth set bit position of x. The "nth set bit" refers to the nth 1 in x when counting set bits from the least significant end.

   The function returns a 64-bit integer with a single 1 at the position of the nth set bit in x. If n is equal to or greater than the number of set bits in x, the result is 0.

   _tzcnt_u64 (trailing zero count) is another BMI1 intrinsic that counts the number of trailing zeros in a 64-bit integer, effectively giving the position of the least significant set bit.

Since _pdep_u64 outputs a value with exactly one set bit (or zero), _tzcnt_u64 returns the index of that bit.

If the input to _tzcnt_u64 is 0 (i.e., n exceeds the number of set bits), it returns 64.

*/
#ifdef __x86_64__
#include <immintrin.h>
#endif

/// highly optimized 2 instruction for x86
#ifdef __x86_64__
   inline unsigned find_nth_set_bit(uint64_t x, unsigned n)
   {
      assert(x != 0);
      return _tzcnt_u64(_pdep_u64(1ULL << n, x));
   }
#else
   inline int find_nth_set_bit(uint64_t x, unsigned n)
   {
      assert(x != 0);  /// popcount us undefined behavior for 0
      uint32_t total_set_bits = __builtin_popcountll(x);
      if (n >= total_set_bits)
         return 64;

      // Iterate to the nth set bit
      for (int i = 0; i < n; i++)
      {
         x &= (x - 1);  // Clear the least significant set bit
      }
      return __builtin_ctzll(x);  // Return position of the next set bit
   }
#endif

}  // namespace ucc