#pragma once

#include <cstdint>

// Include implementation headers
#include "scalar.hpp"
#include "tournament.hpp"

#if defined(__ARM_NEON)
#include "neon_v11.hpp"
#include "neon_v11a.hpp"
#include "neon_v13.hpp"
#include "neon_v13a.hpp"
#include "neon_v14.hpp"
#include "neon_v15.hpp"
#endif

// Forward declarations of min_index functions
int find_approx_min_index_scalar_32(uint16_t* counters, int start);
int find_approx_min_index_scalar_64(uint16_t* counters, int start);
int find_approx_min_index_tournament_32(uint16_t* counters, int start);
int find_approx_min_index_tournament_64(uint16_t* counters, int start);

#if defined(__ARM_NEON)
int find_approx_min_index_neon_v11_32(uint16_t* counters, int start);
int find_approx_min_index_neon_v11_64(uint16_t* counters, int start);
int find_approx_min_index_neon_v11a_32(uint16_t* counters, int start);
int find_approx_min_index_neon_v11b_32(uint16_t* counters, int start);
int find_approx_min_index_neon_v13_32(uint16_t* counters, int start);
int find_approx_min_index_neon_v13_64(uint16_t* counters, int start);
int find_approx_min_index_neon_v13a_32(uint16_t* counters, int start);
int find_approx_min_index_neon_v14_32(uint16_t* counters, int start);
int find_approx_min_index_neon_v14_64(uint16_t* counters, int start);
int find_approx_min_index_neon_v15_32(uint16_t* counters, int start);
int find_approx_min_index_neon_v15_64(uint16_t* counters, int start);
#endif

// Global function that selects best implementation based on available hardware
inline int find_approx_min_index_32(uint16_t* counters, int start)
{
#if defined(__ARM_NEON)
   return find_approx_min_index_neon_v15_32(counters, start);  // Use v15 as default
#else
   return find_approx_min_index_tournament_32(counters, start);
#endif
}

inline int find_approx_min_index_64(uint16_t* counters, int start)
{
#if defined(__ARM_NEON)
   return find_approx_min_index_neon_v15_64(counters, start);  // Use v15 as default
#else
   return find_approx_min_index_tournament_64(counters, start);
#endif
}

// For backward compatibility - will be deprecated
inline int find_approx_min_index(uint16_t* __attribute__((aligned(128))) original_counters,
                                 int                                     start1,
                                 int                                     start2)
{
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);
   return find_approx_min_index_64(original_counters, start1);
}
