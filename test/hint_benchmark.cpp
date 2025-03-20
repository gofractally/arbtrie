#include <arm_neon.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

// ANSI color codes for terminal output
#define RESET_COLOR "\033[0m"
#define ORANGE_COLOR "\033[38;5;208m"
#define GREEN_COLOR "\033[32m"

// Aligned memory allocation helpers
template <typename T>
T* aligned_alloc(size_t count, size_t alignment = 16)
{
   void* ptr = nullptr;
#if defined(_WIN32)
   ptr = _aligned_malloc(count * sizeof(T), alignment);
#else
   if (posix_memalign(&ptr, alignment, count * sizeof(T)) != 0)
   {
      ptr = nullptr;
   }
#endif
   return static_cast<T*>(ptr);
}

template <typename T>
void aligned_free(T* ptr)
{
#if defined(_WIN32)
   _aligned_free(ptr);
#else
   free(ptr);
#endif
}

// Hint structure and function to benchmark
struct hint
{
   hint() { memset((char*)this, 0, sizeof(*this)); }
   uint64_t pages[2];
   uint64_t cachelines[128];
};

// Compact version of hint with a single uint64_t for all cachelines
struct compact_hint
{
   compact_hint() : pages{0, 0}, cacheline_bitmap(0) {}
   uint64_t pages[2];
   uint64_t cacheline_bitmap;  // Single bitmap representing all cachelines
};

// Minimized hint structure with 4 cacheline bitmaps
struct min_hint
{
   min_hint() { memset((char*)this, 0, sizeof(*this)); }
   uint64_t pages[2];
   uint64_t cachelines[4];
};

// Minimized hint structure with 6 cacheline bitmaps for V7
struct min_hint_v7
{
   min_hint_v7() { memset((char*)this, 0, sizeof(*this)); }
   uint64_t pages[2];
   uint64_t cachelines[6];
};

// Minimized hint structure with 8 cacheline bitmaps for V8
struct min_hint_v8
{
   min_hint_v8() { memset((char*)this, 0, sizeof(*this)); }
   uint64_t pages[2];
   uint64_t cachelines[8];
};

// Minimized hint structure with 8 cacheline bitmaps for V9 (same as V8 but with unrolled processing)
struct min_hint_v9
{
   min_hint_v9() { memset((char*)this, 0, sizeof(*this)); }
   uint64_t pages[2];
   uint64_t cachelines[8];
};

// Minimized hint structure with 8 cacheline bitmaps for V10 (same as V8 but with 4-way unrolled processing)
struct min_hint_v10
{
   min_hint_v10() { memset((char*)this, 0, sizeof(*this)); }
   uint64_t pages[2];
   uint64_t cachelines[8];
};
// Minimized hint structure with 8 cacheline bitmaps for V11 (same as V8 but with 4-way unrolled processing)
struct min_hint_v11
{
   min_hint_v11() { memset((char*)this, 0, sizeof(*this)); }
   uint64_t pages[2];
   uint64_t cachelines[8];
};

/// @brief  works with any multiple of 8, aigned on 16 byte boundary
/// TODO: doesn't support ignoring zeros, but could be added with a different variatin
/// @param indices
/// @param hint_count
void calculate_hint_v11(min_hint_v11& h, uint16_t* indices, uint16_t hint_count)
{
   //uint16x8x4_t chunks[4] = {vld1q_u16_x4(indices), vld1q_u16_x4(indices + 32),
   //                          vld1q_u16_x4(indices + 64), vld1q_u16_x4(indices + 96)};

   auto      mask63 = vdupq_n_u16(63);
   uint16_t* end    = indices + hint_count;
   while (indices < end)
   {
      uint16x8_t hints              = vld1q_u16(indices);
      uint16x8_t pages              = vshrq_n_u16(hints, 9);
      uint16x8_t indicies           = vshrq_n_u16(hints, 15);
      uint16x8_t bit_positions      = vandq_u16(pages, mask63);
      uint16x8_t cacheline_indicies = vandq_u16(vshrq_n_u16(hints, 3), mask63);

      h.pages[vgetq_lane_u16(pages, 0)] |= (1ULL << vgetq_lane_u16(bit_positions, 0));
      h.pages[vgetq_lane_u16(pages, 1)] |= (1ULL << vgetq_lane_u16(bit_positions, 1));
      h.pages[vgetq_lane_u16(pages, 2)] |= (1ULL << vgetq_lane_u16(bit_positions, 2));
      h.pages[vgetq_lane_u16(pages, 3)] |= (1ULL << vgetq_lane_u16(bit_positions, 3));
      h.pages[vgetq_lane_u16(pages, 4)] |= (1ULL << vgetq_lane_u16(bit_positions, 4));
      h.pages[vgetq_lane_u16(pages, 5)] |= (1ULL << vgetq_lane_u16(bit_positions, 5));
      h.pages[vgetq_lane_u16(pages, 6)] |= (1ULL << vgetq_lane_u16(bit_positions, 6));
      h.pages[vgetq_lane_u16(pages, 7)] |= (1ULL << vgetq_lane_u16(bit_positions, 7));

      h.cachelines[vgetq_lane_u16(cacheline_indicies, 0)] |=
          (1ULL << vgetq_lane_u16(bit_positions, 0));
      h.cachelines[vgetq_lane_u16(cacheline_indicies, 1)] |=
          (1ULL << vgetq_lane_u16(bit_positions, 1));
      h.cachelines[vgetq_lane_u16(cacheline_indicies, 2)] |=
          (1ULL << vgetq_lane_u16(bit_positions, 2));
      h.cachelines[vgetq_lane_u16(cacheline_indicies, 3)] |=
          (1ULL << vgetq_lane_u16(bit_positions, 3));
      h.cachelines[vgetq_lane_u16(cacheline_indicies, 4)] |=
          (1ULL << vgetq_lane_u16(bit_positions, 4));
      h.cachelines[vgetq_lane_u16(cacheline_indicies, 5)] |=
          (1ULL << vgetq_lane_u16(bit_positions, 5));
      h.cachelines[vgetq_lane_u16(cacheline_indicies, 6)] |=
          (1ULL << vgetq_lane_u16(bit_positions, 6));
      h.cachelines[vgetq_lane_u16(cacheline_indicies, 7)] |=
          (1ULL << vgetq_lane_u16(bit_positions, 7));
      indices += 8;
   }
}

// Forward declaration for V6 implementation
void calculate_hint_v6(min_hint& h, uint16_t* indices, uint16_t hint_count);

// Forward declaration for V7 implementation
void calculate_hint_v7(min_hint_v7& h, uint16_t* indices, uint16_t hint_count);

// Forward declaration for V8 implementation
void calculate_hint_v8(min_hint_v8& h, uint16_t* indices, uint16_t hint_count);

// Forward declaration for V9 implementation
void calculate_hint_v9(min_hint_v9& h, uint16_t* indices, uint16_t hint_count);

// Forward declaration for V10 implementation
void calculate_hint_v10(min_hint_v10& h, uint16_t* indices, uint16_t hint_count);

// Original implementation (v1)
template <bool indicies_contain_zero = false>
void calculate_hint_v1(hint& h, uint16_t* indices, uint16_t hint_count)
{
   for (int i = 0; i < hint_count; i++)
   {
      uint16_t       value = indices[i];
      const uint64_t ignore_zero =
          -(uint64_t)(!indicies_contain_zero || value != 0);  // All 1s if value != 0, else 0

      // Page computation
      uint16_t page         = value >> 9;  // Page number (bits 9-15)
      int      index        = page >> 6;   // 0 or 1 (for page_bits array)
      uint16_t bit_position = page & 63;   // Bit 0-63 within page_bits[index]
      h.pages[index] |= (1ULL << bit_position) & ignore_zero;
      // Cacheline computation
      uint16_t cacheline_index = (value >> 3) & 63;  // Bits 3-8
      h.cachelines[page] |= (1ULL << cacheline_index) & ignore_zero;
   }
}

// Version with explicit handling of zeros (v2)
void calculate_hint_v2(hint& h, uint16_t* indices, uint16_t hint_count)
{
   for (int i = 0; i < hint_count; i++)
   {
      uint16_t value = indices[i];
      if (value == 0)
         continue;  // Skip zeros explicitly

      // Page computation
      uint16_t page         = value >> 9;  // Page number (bits 9-15)
      int      index        = page >> 6;   // 0 or 1 (for page_bits array)
      uint16_t bit_position = page & 63;   // Bit 0-63 within page_bits[index]
      h.pages[index] |= (1ULL << bit_position);

      // Cacheline computation
      uint16_t cacheline_index = (value >> 3) & 63;  // Bits 3-8
      h.cachelines[page] |= (1ULL << cacheline_index);
   }
}

// Version with loop unrolling (v3)
void calculate_hint_v3(hint& h, uint16_t* indices, uint16_t hint_count)
{
   int i = 0;

   // Process 4 indices at a time
   for (; i + 3 < hint_count; i += 4)
   {
      uint16_t value0 = indices[i];
      uint16_t value1 = indices[i + 1];
      uint16_t value2 = indices[i + 2];
      uint16_t value3 = indices[i + 3];

      if (value0 != 0)
      {
         uint16_t page0         = value0 >> 9;
         int      index0        = page0 >> 6;
         uint16_t bit_position0 = page0 & 63;
         h.pages[index0] |= (1ULL << bit_position0);

         uint16_t cacheline_index0 = (value0 >> 3) & 63;
         h.cachelines[page0] |= (1ULL << cacheline_index0);
      }

      if (value1 != 0)
      {
         uint16_t page1         = value1 >> 9;
         int      index1        = page1 >> 6;
         uint16_t bit_position1 = page1 & 63;
         h.pages[index1] |= (1ULL << bit_position1);

         uint16_t cacheline_index1 = (value1 >> 3) & 63;
         h.cachelines[page1] |= (1ULL << cacheline_index1);
      }

      if (value2 != 0)
      {
         uint16_t page2         = value2 >> 9;
         int      index2        = page2 >> 6;
         uint16_t bit_position2 = page2 & 63;
         h.pages[index2] |= (1ULL << bit_position2);

         uint16_t cacheline_index2 = (value2 >> 3) & 63;
         h.cachelines[page2] |= (1ULL << cacheline_index2);
      }

      if (value3 != 0)
      {
         uint16_t page3         = value3 >> 9;
         int      index3        = page3 >> 6;
         uint16_t bit_position3 = page3 & 63;
         h.pages[index3] |= (1ULL << bit_position3);

         uint16_t cacheline_index3 = (value3 >> 3) & 63;
         h.cachelines[page3] |= (1ULL << cacheline_index3);
      }
   }

   // Handle remaining elements
   for (; i < hint_count; i++)
   {
      uint16_t value = indices[i];
      if (value == 0)
         continue;

      uint16_t page         = value >> 9;
      int      index        = page >> 6;
      uint16_t bit_position = page & 63;
      h.pages[index] |= (1ULL << bit_position);

      uint16_t cacheline_index = (value >> 3) & 63;
      h.cachelines[page] |= (1ULL << cacheline_index);
   }
}

// Version with prefetching (v4)
void calculate_hint_v4(hint& h, uint16_t* indices, uint16_t hint_count)
{
   if (hint_count == 0)
      return;

   // Pre-fetch the first item
   uint16_t value = indices[0];

   for (int i = 0; i < hint_count - 1; i++)
   {
      // Pre-fetch the next item
      uint16_t next_value = indices[i + 1];
      __builtin_prefetch(&indices[i + 2], 0, 3);  // Prefetch for read, high temporal locality

      if (value != 0)
      {
         // Process the current value
         uint16_t page         = value >> 9;
         int      index        = page >> 6;
         uint16_t bit_position = page & 63;
         h.pages[index] |= (1ULL << bit_position);

         uint16_t cacheline_index = (value >> 3) & 63;
         h.cachelines[page] |= (1ULL << cacheline_index);
      }

      // Update for next iteration
      value = next_value;
   }

   // Process the last element
   if (value != 0)
   {
      uint16_t page         = value >> 9;
      int      index        = page >> 6;
      uint16_t bit_position = page & 63;
      h.pages[index] |= (1ULL << bit_position);

      uint16_t cacheline_index = (value >> 3) & 63;
      h.cachelines[page] |= (1ULL << cacheline_index);
   }
}

// Compact version with a single bitmap for all cachelines (v5)
void calculate_hint_v5(compact_hint& h, uint16_t* indices, uint16_t hint_count)
{
   for (int i = 0; i < hint_count; i++)
   {
      uint16_t value = indices[i];
      if (value == 0)
         continue;

      // Page computation
      uint8_t page = (value >> 15) & 0x1;
      h.pages[page] |= 1ULL << ((value >> 9) & 0x3F);

      // Calculate cacheline (0-63) and set corresponding bit in bitmap
      uint8_t cacheline = (value >> 6) & 0x3F;
      h.cacheline_bitmap |= 1ULL << cacheline;
   }
}

// Function for min_hint with interleaved page mapping (v6)
void calculate_hint_v6(min_hint& h, uint16_t* indices, uint16_t hint_count)
{
   for (int i = 0; i < hint_count; i++)
   {
      uint16_t value = indices[i];
      if (value == 0)
         continue;

      // Page computation - same as other versions
      uint16_t page         = value >> 9;  // Page number (bits 9-15)
      int      index        = page >> 6;   // 0 or 1 (for page_bits array)
      uint16_t bit_position = page & 63;   // Bit 0-63 within page_bits[index]
      h.pages[index] |= (1ULL << bit_position);

      // Cacheline computation with interleaved mapping
      uint16_t cacheline = value >> 6;      // Cacheline number (bits 6-15)
      uint16_t cache_idx = cacheline & 3;   // Which of the 4 cachelines to use (0-3)
      uint16_t bit_pos   = cacheline >> 2;  // Which bit within that cacheline

      h.cachelines[cache_idx] |= (1ULL << bit_pos);
   }
}

// Function for min_hint_v7 with interleaved page mapping using 6 cachelines (v7)
void calculate_hint_v7(min_hint_v7& h, uint16_t* indices, uint16_t hint_count)
{
   for (int i = 0; i < hint_count; i++)
   {
      uint16_t value = indices[i];
      if (value == 0)
         continue;

      // Page computation - same as other versions
      uint16_t page         = value >> 9;  // Page number (bits 9-15)
      int      index        = page >> 6;   // 0 or 1 (for page_bits array)
      uint16_t bit_position = page & 63;   // Bit 0-63 within page_bits[index]
      h.pages[index] |= (1ULL << bit_position);

      // Cacheline computation with interleaved mapping using 6 cachelines
      uint16_t cacheline = value >> 6;     // Cacheline number (bits 6-15)
      uint16_t cache_idx = cacheline % 6;  // Which of the 6 cachelines to use (0-5)
      uint16_t bit_pos   = cacheline / 6;  // Which bit within that cacheline

      h.cachelines[cache_idx] |= (1ULL << bit_pos);
   }
}

// Function for min_hint_v8 with interleaved page mapping using 8 cachelines (v8)
void calculate_hint_v8(min_hint_v8& h, uint16_t* indices, uint16_t hint_count)
{
   for (int i = 0; i < hint_count; i++)
   {
      uint16_t value = indices[i];
      if (value == 0)
         continue;

      // Page computation - same as other versions
      uint16_t page         = value >> 9;  // Page number (bits 9-15)
      int      index        = page >> 6;   // 0 or 1 (for page_bits array)
      uint16_t bit_position = page & 63;   // Bit 0-63 within page_bits[index]
      h.pages[index] |= (1ULL << bit_position);

      // Cacheline computation with interleaved mapping using 8 cachelines
      uint16_t cacheline = value >> 6;     // Cacheline number (bits 6-15)
      uint16_t cache_idx = cacheline % 8;  // Which of the 8 cachelines to use (0-7)
      uint16_t bit_pos   = cacheline / 8;  // Which bit within that cacheline

      h.cachelines[cache_idx] |= (1ULL << bit_pos);
   }
}

// Function for min_hint_v9 with 8-way interleaved mapping and loop unrolling by 2 (v9)
void calculate_hint_v9(min_hint_v9& h, uint16_t* indices, uint16_t hint_count)
{
   int i = 0;

   // Process two indices at a time
   for (; i + 1 < hint_count; i += 2)
   {
      // Process first value
      uint16_t value1 = indices[i];
      if (value1 != 0)
      {
         // Page computation for first value
         uint16_t page1         = value1 >> 9;
         int      index1        = page1 >> 6;
         uint16_t bit_position1 = page1 & 63;
         h.pages[index1] |= (1ULL << bit_position1);

         // Cacheline computation for first value
         uint16_t cacheline1 = value1 >> 6;
         uint16_t cache_idx1 = cacheline1 % 8;
         uint16_t bit_pos1   = cacheline1 / 8;
         h.cachelines[cache_idx1] |= (1ULL << bit_pos1);
      }

      // Process second value
      uint16_t value2 = indices[i + 1];
      if (value2 != 0)
      {
         // Page computation for second value
         uint16_t page2         = value2 >> 9;
         int      index2        = page2 >> 6;
         uint16_t bit_position2 = page2 & 63;
         h.pages[index2] |= (1ULL << bit_position2);

         // Cacheline computation for second value
         uint16_t cacheline2 = value2 >> 6;
         uint16_t cache_idx2 = cacheline2 % 8;
         uint16_t bit_pos2   = cacheline2 / 8;
         h.cachelines[cache_idx2] |= (1ULL << bit_pos2);
      }
   }

   // Handle remaining element if count is odd
   if (i < hint_count)
   {
      uint16_t value = indices[i];
      if (value != 0)
      {
         // Page computation
         uint16_t page         = value >> 9;
         int      index        = page >> 6;
         uint16_t bit_position = page & 63;
         h.pages[index] |= (1ULL << bit_position);

         // Cacheline computation
         uint16_t cacheline = value >> 6;
         uint16_t cache_idx = cacheline % 8;
         uint16_t bit_pos   = cacheline / 8;
         h.cachelines[cache_idx] |= (1ULL << bit_pos);
      }
   }
}

// Function for min_hint_v10 with 8-way interleaved mapping and loop unrolling by 4 (v10)
void calculate_hint_v10(min_hint_v10& h, uint16_t* indices, uint16_t hint_count)
{
   int i = 0;

   // Process four indices at a time
   for (; i + 3 < hint_count; i += 4)
   {
      // Process first value
      uint16_t value1 = indices[i];
      if (value1 != 0)
      {
         // Page computation for first value
         uint16_t page1         = value1 >> 9;
         int      index1        = page1 >> 6;
         uint16_t bit_position1 = page1 & 63;
         h.pages[index1] |= (1ULL << bit_position1);

         // Cacheline computation for first value
         uint16_t cacheline1 = value1 >> 6;
         uint16_t cache_idx1 = cacheline1 % 8;
         uint16_t bit_pos1   = cacheline1 / 8;
         h.cachelines[cache_idx1] |= (1ULL << bit_pos1);
      }

      // Process second value
      uint16_t value2 = indices[i + 1];
      if (value2 != 0)
      {
         // Page computation for second value
         uint16_t page2         = value2 >> 9;
         int      index2        = page2 >> 6;
         uint16_t bit_position2 = page2 & 63;
         h.pages[index2] |= (1ULL << bit_position2);

         // Cacheline computation for second value
         uint16_t cacheline2 = value2 >> 6;
         uint16_t cache_idx2 = cacheline2 % 8;
         uint16_t bit_pos2   = cacheline2 / 8;
         h.cachelines[cache_idx2] |= (1ULL << bit_pos2);
      }

      // Process third value
      uint16_t value3 = indices[i + 2];
      if (value3 != 0)
      {
         // Page computation for third value
         uint16_t page3         = value3 >> 9;
         int      index3        = page3 >> 6;
         uint16_t bit_position3 = page3 & 63;
         h.pages[index3] |= (1ULL << bit_position3);

         // Cacheline computation for third value
         uint16_t cacheline3 = value3 >> 6;
         uint16_t cache_idx3 = cacheline3 % 8;
         uint16_t bit_pos3   = cacheline3 / 8;
         h.cachelines[cache_idx3] |= (1ULL << bit_pos3);
      }

      // Process fourth value
      uint16_t value4 = indices[i + 3];
      if (value4 != 0)
      {
         // Page computation for fourth value
         uint16_t page4         = value4 >> 9;
         int      index4        = page4 >> 6;
         uint16_t bit_position4 = page4 & 63;
         h.pages[index4] |= (1ULL << bit_position4);

         // Cacheline computation for fourth value
         uint16_t cacheline4 = value4 >> 6;
         uint16_t cache_idx4 = cacheline4 % 8;
         uint16_t bit_pos4   = cacheline4 / 8;
         h.cachelines[cache_idx4] |= (1ULL << bit_pos4);
      }
   }

   // Handle remaining elements
   for (; i < hint_count; i++)
   {
      uint16_t value = indices[i];
      if (value != 0)
      {
         // Page computation
         uint16_t page         = value >> 9;
         int      index        = page >> 6;
         uint16_t bit_position = page & 63;
         h.pages[index] |= (1ULL << bit_position);

         // Cacheline computation
         uint16_t cacheline = value >> 6;
         uint16_t cache_idx = cacheline % 8;
         uint16_t bit_pos   = cacheline / 8;
         h.cachelines[cache_idx] |= (1ULL << bit_pos);
      }
   }
}

// Benchmark function for calculate_hint
void benchmark_calculate_hint(int num_iterations)
{
   using namespace std::chrono;

   std::random_device              rd;
   std::mt19937                    gen(rd());
   std::uniform_int_distribution<> dist_no_zeros(1, UINT16_MAX - 1);  // Avoid 0 and max values
   std::uniform_int_distribution<> dist_with_zeros(0,
                                                   UINT16_MAX - 1);  // Include 0, avoid max value

   // Allocate buffer for maximum number of indices (256)
   uint16_t* indices_no_zeros   = aligned_alloc<uint16_t>(256, 16);
   uint16_t* indices_with_zeros = aligned_alloc<uint16_t>(256, 16);

   // Fill with random data
   for (int i = 0; i < 256; i++)
   {
      indices_no_zeros[i]   = dist_no_zeros(gen);
      indices_with_zeros[i] = dist_with_zeros(gen);
   }

   // Ensure some zeros are actually present
   for (int i = 0; i < 256; i += 8)
   {
      indices_with_zeros[i] = 0;
   }

   struct HintBenchmarkResult
   {
      std::string name;
      int         count;  // Number of indices processed
      double      time_us;
      double      time_per_index_ns;
      double      speedup;  // Relative to V1 with same count
   };

   auto benchmark_impl = [&](const char* name, auto func, uint16_t* data,
                             int count) -> HintBenchmarkResult
   {
      std::vector<hint> hints(num_iterations);

      auto start = high_resolution_clock::now();

      for (int i = 0; i < num_iterations; i++)
      {
         func(hints[i], data, count);
      }

      auto end      = high_resolution_clock::now();
      auto duration = duration_cast<microseconds>(end - start);

      // Clean up to prevent any compiler optimizations
      double _clean = 0;
      for (auto& h : hints)
      {
         _clean += h.pages[0] + h.pages[1];
         for (int i = 0; i < 128; i++)
            _clean += h.cachelines[i];
      }
      if (_clean == 0.1234)
         std::cout << "Unlikely: " << _clean << std::endl;

      return {name, count, static_cast<double>(duration.count()),
              static_cast<double>(duration.count() * 1000.0 / (count * num_iterations)),
              1.0};  // Relative speedup will be calculated later
   };

   auto benchmark_compact_impl = [&](const char* name, auto func, uint16_t* data,
                                     int count) -> HintBenchmarkResult
   {
      std::vector<compact_hint> hints(num_iterations);

      auto start = high_resolution_clock::now();

      for (int i = 0; i < num_iterations; i++)
      {
         func(hints[i], data, count);
      }

      auto end      = high_resolution_clock::now();
      auto duration = duration_cast<microseconds>(end - start);

      // Clean up to prevent any compiler optimizations
      double _clean = 0;
      for (auto& h : hints)
      {
         _clean += h.pages[0] + h.pages[1] + h.cacheline_bitmap;
      }
      if (_clean == 0.1234)
         std::cout << "Unlikely: " << _clean << std::endl;

      return {name, count, static_cast<double>(duration.count()),
              static_cast<double>(duration.count() * 1000.0 / (count * num_iterations)),
              1.0};  // Relative speedup will be calculated later
   };

   auto benchmark_min_impl = [&](const char* name, auto func, uint16_t* data,
                                 int count) -> HintBenchmarkResult
   {
      std::vector<min_hint> hints(num_iterations);

      auto start = std::chrono::high_resolution_clock::now();

      for (int i = 0; i < num_iterations; i++)
      {
         func(hints[i], data, count);
      }

      auto end      = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

      // Clean up to prevent any compiler optimizations
      double _clean = 0;
      for (auto& h : hints)
      {
         _clean += h.pages[0] + h.pages[1] + h.cachelines[0] + h.cachelines[1] + h.cachelines[2] +
                   h.cachelines[3];
      }
      if (_clean == 0.1234)
         std::cout << "Unlikely: " << _clean << std::endl;

      return {name, count, static_cast<double>(duration.count()),
              static_cast<double>(duration.count() * 1000.0 / (count * num_iterations)),
              1.0};  // Relative speedup will be calculated later
   };

   // Add new benchmark implementation for min_hint_v7
   auto benchmark_min_v7_impl = [&](const char* name, auto func, uint16_t* data,
                                    int count) -> HintBenchmarkResult
   {
      std::vector<min_hint_v7> hints(num_iterations);

      auto start = std::chrono::high_resolution_clock::now();

      for (int i = 0; i < num_iterations; i++)
      {
         func(hints[i], data, count);
      }

      auto end      = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

      // Clean up to prevent any compiler optimizations
      double _clean = 0;
      for (auto& h : hints)
      {
         _clean += h.pages[0] + h.pages[1];
         for (int i = 0; i < 6; i++)
            _clean += h.cachelines[i];
      }
      if (_clean == 0.1234)
         std::cout << "Unlikely: " << _clean << std::endl;

      return {name, count, static_cast<double>(duration.count()),
              static_cast<double>(duration.count() * 1000.0 / (count * num_iterations)),
              1.0};  // Relative speedup will be calculated later
   };

   // Add new benchmark implementation for min_hint_v8
   auto benchmark_min_v8_impl = [&](const char* name, auto func, uint16_t* data,
                                    int count) -> HintBenchmarkResult
   {
      std::vector<min_hint_v8> hints(num_iterations);

      auto start = std::chrono::high_resolution_clock::now();

      for (int i = 0; i < num_iterations; i++)
      {
         func(hints[i], data, count);
      }

      auto end      = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

      // Clean up to prevent any compiler optimizations
      double _clean = 0;
      for (auto& h : hints)
      {
         _clean += h.pages[0] + h.pages[1];
         for (int i = 0; i < 8; i++)
            _clean += h.cachelines[i];
      }
      if (_clean == 0.1234)
         std::cout << "Unlikely: " << _clean << std::endl;

      return {name, count, static_cast<double>(duration.count()),
              static_cast<double>(duration.count() * 1000.0 / (count * num_iterations)),
              1.0};  // Relative speedup will be calculated later
   };

   // Add new benchmark implementation for min_hint_v9
   auto benchmark_min_v9_impl = [&](const char* name, auto func, uint16_t* data,
                                    int count) -> HintBenchmarkResult
   {
      std::vector<min_hint_v9> hints(num_iterations);

      auto start = std::chrono::high_resolution_clock::now();

      for (int i = 0; i < num_iterations; i++)
      {
         func(hints[i], data, count);
      }

      auto end      = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

      // Clean up to prevent any compiler optimizations
      double _clean = 0;
      for (auto& h : hints)
      {
         _clean += h.pages[0] + h.pages[1];
         for (int i = 0; i < 8; i++)
            _clean += h.cachelines[i];
      }
      if (_clean == 0.1234)
         std::cout << "Unlikely: " << _clean << std::endl;

      return {name, count, static_cast<double>(duration.count()),
              static_cast<double>(duration.count() * 1000.0 / (count * num_iterations)),
              1.0};  // Relative speedup will be calculated later
   };

   // Add new benchmark implementation for min_hint_v10
   auto benchmark_min_v10_impl = [&](const char* name, auto func, uint16_t* data,
                                     int count) -> HintBenchmarkResult
   {
      std::vector<min_hint_v10> hints(num_iterations);

      auto start = std::chrono::high_resolution_clock::now();

      for (int i = 0; i < num_iterations; i++)
      {
         func(hints[i], data, count);
      }

      auto end      = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

      // Clean up to prevent any compiler optimizations
      double _clean = 0;
      for (auto& h : hints)
      {
         _clean += h.pages[0] + h.pages[1];
         for (int i = 0; i < 8; i++)
            _clean += h.cachelines[i];
      }
      if (_clean == 0.1234)
         std::cout << "Unlikely: " << _clean << std::endl;

      return {name, count, static_cast<double>(duration.count()),
              static_cast<double>(duration.count() * 1000.0 / (count * num_iterations)),
              1.0};  // Relative speedup will be calculated later
   };

   // Add new benchmark implementation for min_hint_v11
   auto benchmark_min_v11_impl = [&](const char* name, auto func, uint16_t* data,
                                     int count) -> HintBenchmarkResult
   {
      std::vector<min_hint_v11> hints(num_iterations);

      auto start = std::chrono::high_resolution_clock::now();

      for (int i = 0; i < num_iterations; i++)
      {
         func(hints[i], data, count);
      }

      auto end      = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

      // Clean up to prevent any compiler optimizations
      double _clean = 0;
      for (auto& h : hints)
      {
         _clean += h.pages[0] + h.pages[1];
         for (int i = 0; i < 8; i++)
            _clean += h.cachelines[i];
      }
      if (_clean == 0.1234)
         std::cout << "Unlikely: " << _clean << std::endl;

      return {name, count, static_cast<double>(duration.count()),
              static_cast<double>(duration.count() * 1000.0 / (count * num_iterations)),
              1.0};  // Relative speedup will be calculated later
   };

   // Header
   std::cout << "\n";
   std::cout << "+----------------------------+-----+------------------+------------------+-------"
                "-------+\n";
   std::cout << "| Algorithm                  |  N  |    Time (μs/it)  |  Time/idx (ns)   |   "
                "Speedup    |\n";
   std::cout << "+----------------------------+-----+------------------+------------------+-------"
                "-------+\n";

   // Run benchmarks with varying numbers of indices for data without zeros
   std::cout << "Benchmarking with indices that don't contain zeros:\n";

   // Only use specified sizes: 4, 16, 64, 128, 256
   for (int count : {4, 16, 64, 128, 256})
   {
      // Get baseline for calculating speedup
      auto baseline = benchmark_impl(
          "V1: Baseline", [](hint& h, uint16_t* i, uint16_t c)
          { calculate_hint_v1<false>(h, i, c); }, indices_no_zeros, count);

      // Compare with other implementations
      auto v3_result = benchmark_impl(
          "V3: Loop Unrolling", [](hint& h, uint16_t* i, uint16_t c)
          { calculate_hint_v3(h, i, c); }, indices_no_zeros, count);

      auto v4_result = benchmark_impl(
          "V4: Prefetching", [](hint& h, uint16_t* i, uint16_t c) { calculate_hint_v4(h, i, c); },
          indices_no_zeros, count);

      auto v5_result = benchmark_compact_impl(
          "V5: Compact Bitmap", [](compact_hint& h, uint16_t* i, uint16_t c)
          { calculate_hint_v5(h, i, c); }, indices_no_zeros, count);

      auto v6_result = benchmark_min_impl(
          "V6: 4-way", [](min_hint& h, uint16_t* i, uint16_t c) { calculate_hint_v6(h, i, c); },
          indices_no_zeros, count);

      auto v7_result = benchmark_min_v7_impl(
          "V7: 6-way", [](min_hint_v7& h, uint16_t* i, uint16_t c) { calculate_hint_v7(h, i, c); },
          indices_no_zeros, count);

      auto v8_result = benchmark_min_v8_impl(
          "V8: 8-way", [](min_hint_v8& h, uint16_t* i, uint16_t c) { calculate_hint_v8(h, i, c); },
          indices_no_zeros, count);

      auto v9_result = benchmark_min_v9_impl(
          "V9: 8-way+Unroll2", [](min_hint_v9& h, uint16_t* i, uint16_t c)
          { calculate_hint_v9(h, i, c); }, indices_no_zeros, count);

      auto v10_result = benchmark_min_v10_impl(
          "V10: 8-way+Unroll4", [](min_hint_v10& h, uint16_t* i, uint16_t c)
          { calculate_hint_v10(h, i, c); }, indices_no_zeros, count);

      // Benchmark V11 for all counts, not just 128
      auto v11_result = benchmark_min_v11_impl(
          "V11: NEON Vectorized", [](min_hint_v11& h, uint16_t* i, uint16_t c)
          { calculate_hint_v11(h, i, c); }, indices_no_zeros, count);

      // Calculate speedups relative to baseline
      v3_result.speedup  = baseline.time_us / v3_result.time_us;
      v4_result.speedup  = baseline.time_us / v4_result.time_us;
      v5_result.speedup  = baseline.time_us / v5_result.time_us;
      v6_result.speedup  = baseline.time_us / v6_result.time_us;
      v7_result.speedup  = baseline.time_us / v7_result.time_us;
      v8_result.speedup  = baseline.time_us / v8_result.time_us;
      v9_result.speedup  = baseline.time_us / v9_result.time_us;
      v10_result.speedup = baseline.time_us / v10_result.time_us;
      v11_result.speedup = baseline.time_us / v11_result.time_us;

      // Output results
      std::vector<HintBenchmarkResult> results = {baseline,   v3_result, v4_result, v5_result,
                                                  v6_result,  v7_result, v8_result, v9_result,
                                                  v10_result, v11_result};

      for (const auto& result : results)
      {
         std::cout << "| " << std::setw(28) << std::left << result.name << "| " << std::setw(3)
                   << std::right << result.count << " | " << std::setw(16) << std::fixed
                   << std::setprecision(3) << (result.time_us / num_iterations) << " | "
                   << std::setw(16) << std::fixed << std::setprecision(3)
                   << result.time_per_index_ns << " | " << std::setw(13) << std::fixed
                   << std::setprecision(2) << result.speedup << " |\n";
      }

      std::cout
          << "+----------------------------+-----+------------------+------------------+-------"
             "-------+\n";
   }

   // Only run the test with zeros for count=256
   if (true)
   {
      int count = 256;
      std::cout << "\nBenchmarking with indices that contain zeros (N=256):\n";
      std::cout
          << "+----------------------------+-----+------------------+------------------+-------"
             "-------+\n";
      std::cout << "| Algorithm                  |  N  |    Time (μs/it)  |  Time/idx (ns)   |   "
                   "Speedup    |\n";
      std::cout
          << "+----------------------------+-----+------------------+------------------+-------"
             "-------+\n";

      // Get baseline for calculating speedup
      auto baseline = benchmark_impl(
          "V1: Baseline", [](hint& h, uint16_t* i, uint16_t c)
          { calculate_hint_v1<false>(h, i, c); }, indices_with_zeros, count);

      // Compare with other implementations
      auto v2_result = benchmark_impl(
          "V2: Zeros Support", [](hint& h, uint16_t* i, uint16_t c) { calculate_hint_v2(h, i, c); },
          indices_with_zeros, count);

      auto v3_result = benchmark_impl(
          "V3: Loop Unrolling", [](hint& h, uint16_t* i, uint16_t c)
          { calculate_hint_v3(h, i, c); }, indices_with_zeros, count);

      auto v4_result = benchmark_impl(
          "V4: Prefetching", [](hint& h, uint16_t* i, uint16_t c) { calculate_hint_v4(h, i, c); },
          indices_with_zeros, count);

      auto v5_result = benchmark_compact_impl(
          "V5: Compact Bitmap", [](compact_hint& h, uint16_t* i, uint16_t c)
          { calculate_hint_v5(h, i, c); }, indices_with_zeros, count);

      auto v6_result = benchmark_min_impl(
          "V6: 4-way", [](min_hint& h, uint16_t* i, uint16_t c) { calculate_hint_v6(h, i, c); },
          indices_with_zeros, count);

      auto v7_result = benchmark_min_v7_impl(
          "V7: 6-way", [](min_hint_v7& h, uint16_t* i, uint16_t c) { calculate_hint_v7(h, i, c); },
          indices_with_zeros, count);

      auto v8_result = benchmark_min_v8_impl(
          "V8: 8-way", [](min_hint_v8& h, uint16_t* i, uint16_t c) { calculate_hint_v8(h, i, c); },
          indices_with_zeros, count);

      auto v9_result = benchmark_min_v9_impl(
          "V9: 8-way+Unroll2", [](min_hint_v9& h, uint16_t* i, uint16_t c)
          { calculate_hint_v9(h, i, c); }, indices_with_zeros, count);

      auto v10_result = benchmark_min_v10_impl(
          "V10: 8-way+Unroll4", [](min_hint_v10& h, uint16_t* i, uint16_t c)
          { calculate_hint_v10(h, i, c); }, indices_with_zeros, count);

      // Add benchmark for V11 with zeros too
      auto v11_result = benchmark_min_v11_impl(
          "V11: NEON Vectorized", [](min_hint_v11& h, uint16_t* i, uint16_t c)
          { calculate_hint_v11(h, i, c); }, indices_with_zeros, count);

      // Calculate speedups relative to baseline
      v2_result.speedup  = baseline.time_us / v2_result.time_us;
      v3_result.speedup  = baseline.time_us / v3_result.time_us;
      v4_result.speedup  = baseline.time_us / v4_result.time_us;
      v5_result.speedup  = baseline.time_us / v5_result.time_us;
      v6_result.speedup  = baseline.time_us / v6_result.time_us;
      v7_result.speedup  = baseline.time_us / v7_result.time_us;
      v8_result.speedup  = baseline.time_us / v8_result.time_us;
      v9_result.speedup  = baseline.time_us / v9_result.time_us;
      v10_result.speedup = baseline.time_us / v10_result.time_us;
      v11_result.speedup = baseline.time_us / v11_result.time_us;

      // Output results
      std::vector<HintBenchmarkResult> results = {baseline,  v2_result,  v3_result, v4_result,
                                                  v5_result, v6_result,  v7_result, v8_result,
                                                  v9_result, v10_result, v11_result};

      for (const auto& result : results)
      {
         std::cout << "| " << std::setw(28) << std::left << result.name << "| " << std::setw(3)
                   << std::right << result.count << " | " << std::setw(16) << std::fixed
                   << std::setprecision(3) << (result.time_us / num_iterations) << " | "
                   << std::setw(16) << std::fixed << std::setprecision(3)
                   << result.time_per_index_ns << " | " << std::setw(13) << std::fixed
                   << std::setprecision(2) << result.speedup << " |\n";
      }

      std::cout
          << "+----------------------------+-----+------------------+------------------+-------"
             "-------+\n";
   }

   // Clean up
   aligned_free(indices_no_zeros);
   aligned_free(indices_with_zeros);
}

int main()
{
   // Run calculate_hint benchmarks
   std::cout << "Running calculate_hint benchmarks...\n";
   benchmark_calculate_hint(500000);

   return 0;
}