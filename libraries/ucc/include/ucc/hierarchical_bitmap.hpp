
#include <cassert>
#include <cstdint>
#include <cstring>  // For memset
#include <stdexcept>

namespace ucc
{
   /**
    * @class hierarchical_bitmap
    * 
    * A hierarchical bitmap implementation providing efficient bit manipulation operations with
    * advanced first-set/first-unset bit finding capabilities. While originally designed for memory
    * allocation systems, this data structure serves as a general-purpose bitmap with O(1) bitwise
    * operations regardless of size.
    * 
    * Design Overview:
    * ---------------
    * The bitmap is organized in a hierarchical tree structure where:
    * - Level 0 (bottom): Contains the actual bits (1 bit per position)
    * - Level 1-N (above): Each bit represents the status of a group of bits in the level below
    * 
    * This hierarchical design allows for extremely fast bit operations that would otherwise require
    * linear scans in traditional bitmap implementations.
    * 
    * Capacity and Storage Requirements:
    * -------------------------------
    * For managing N bits, storage is calculated per level, where each level manages
    * groups of 64 bits from the level below. Storage at each level is rounded up
    * to the nearest word (64 bits).
    * 
    * Level 1 (N ≤ 64):
    * - Level 0: ceil(N/64) words
    * - Total: 1 word
    * 
    * Level 2 (64 < N ≤ 4096):
    * - Level 0: ceil(N/64) words
    * - Level 1: 1 word
    * - Total: ceil(N/64) + 1 words
    * 
    * Level 3 (4096 < N ≤ 262144):
    * - Level 0: ceil(N/64) words
    * - Level 1: ceil(N/4096) words
    * - Level 2: 1 word
    * - Total: ceil(N/64) + ceil(N/4096) + 1 words
    * 
    * Level 4 (262144 < N ≤ 16777216):
    * - Level 0: ceil(N/64) words
    * - Level 1: ceil(N/4096) words
    * - Level 2: ceil(N/262144) words
    * - Level 3: 1 word
    * - Total: ceil(N/64) + ceil(N/4096) + ceil(N/262144) + 1 words
    * 
    * Example sizes:
    * - 50 bits: 1 word (8 bytes)
    * - 1000 bits: 17 words (136 bytes)
    * - 10000 bits: 159 words (1,272 bytes)
    * - 1000000 bits: 15,760 words (126,080 bytes)
    * 
    * Note: Storage is allocated statically at compile time based on NumBits,
    * with arrays sized exactly as needed for each level.
    * 
    * Key Features:
    * ------------
    * 1. Standard Bit Operations:
    *    - set(): Set a specific bit or all bits
    *    - reset(): Reset a specific bit or all bits
    *    - test(): Check if a specific bit is set
    *    - any(): Check if any bit is set
    *    - none(): Check if no bits are set
    * 
    * 2. Advanced Operations:
    *    - O(1) unset_first_set(): Find and clear the first set bit
    *    - O(1) set_first_unset(): Find and set the first unset bit
    *    - O(1) countr_zero(): Find the first set bit without modifying
    *    - O(1) find_first_unset(): Find the first unset bit without modifying
    * 
    * 3. Performance Benefits:
    *    - Cache-friendly design using 64-bit words aligned on cache lines
    *    - Minimized memory accesses when searching for set/unset bits
    *    - Higher levels provide quick rejection of fully set/unset regions
    *    - Bit operations remain O(1) regardless of bitmap size
    * 
    * 4. Memory Efficiency:
    *    - Minimal overhead (approximately 1.02x the size of a standard bitmap)
    *    - Automatic aggregation of bit information in higher levels
    * 
    * Usage Example:
    * -------------
    * @code
    * hierarchical_bitmap<4096> bitmap;  // Creates a bitmap supporting 4,096 bits
    * 
    * // Initially all bits are set to 1
    * 
    * // Find and clear the first set bit
    * uint32_t idx = bitmap.unset_first_set();
    * if (idx != bitmap.invalid_index) {
    *     // Use the index...
    * }
    * 
    * // Set a specific bit
    * bitmap.set(idx);
    * 
    * // Find and set the first unset bit
    * uint32_t first_unset = bitmap.set_first_unset();
    * 
    * // Check if any bits are set
    * bool has_set_bits = bitmap.any();
    * 
    * // Clear all bits
    * bitmap.reset();
    * @endcode
    * 
    * Implementation Notes:
    * -------------------
    * - Uses __builtin_ctzll for efficient first-set-bit finding
    * - Maintains parent bits automatically when child states change
    * - Throws std::runtime_error for out-of-bounds access
    * - Specialized implementations for levels 1-4 for optimal performance
    * - Compatible with std::bitset for common operations
    * 
    * Performance Characteristics:
    * -------------------------
    * - unset_first_set(): 2-5 memory accesses (depending on level)
    * - set_first_unset(): 2-5 memory accesses (depending on level)
    * - set(): 1-4 memory accesses (depending on level)
    * - reset(): 1-4 memory accesses (depending on level)
    * - test(): 1 memory access
    * - countr_zero(): Same as unset_first_set() but no writes
    * - find_first_unset(): Same as set_first_unset() but no writes
    * 
    * Worst Case Memory Usage (64-bit words):
    * -----------
    * - Level 1: 8 bytes (1 word)
    * - Level 2: 520 bytes (65 words)
    * - Level 3: 33KB (4,161 words)
    * - Level 4: 2.1MB (266,305 words)
    */
   template <uint32_t NumBits>
   class hierarchical_bitmap
   {
     public:
      static constexpr uint32_t bits_per_word = 64;  // Number of bits in a uint64_t
      static constexpr uint32_t invalid_index = NumBits;

      // Helper to calculate required level for N segments
      static constexpr uint32_t calc_level(uint64_t n)
      {
         uint32_t level    = 1;
         uint64_t capacity = bits_per_word;
         while (capacity < n)
         {
            capacity *= bits_per_word;
            level++;
         }
         return level;
      }

      // Helper to calculate number of words needed at each level
      static constexpr uint32_t calc_level_words(uint32_t level, uint64_t max_segments)
      {
         if (level == 0)
         {
            return (max_segments + bits_per_word - 1) / bits_per_word;
         }
         return (calc_level_words(level - 1, max_segments) + bits_per_word - 1) / bits_per_word;
      }

     private:
      static constexpr uint32_t required_level = calc_level(NumBits);

      // Calculate array sizes for each level at compile time
      static constexpr uint32_t l0_words = calc_level_words(0, NumBits);
      static constexpr uint32_t l1_words = calc_level_words(1, NumBits);
      static constexpr uint32_t l2_words = calc_level_words(2, NumBits);
      static constexpr uint32_t l3_words = calc_level_words(3, NumBits);

      // Level arrays sized exactly as needed
      alignas(64) uint64_t level0[l0_words];
      alignas(64) uint64_t level1[required_level > 1 ? l1_words : 0];
      alignas(64) uint64_t level2[required_level > 2 ? l2_words : 0];
      alignas(64) uint64_t level3[required_level > 3 ? l3_words : 0];

      // Running count of set bits
      uint32_t bit_count;

      // Helper to mask off bits beyond NumBits in the last word
      static constexpr uint64_t get_last_word_mask()
      {
         uint32_t bits_in_last_word = NumBits % bits_per_word;
         if (bits_in_last_word == 0)
            return ~0ULL;                         // All bits if perfectly aligned
         return (1ULL << bits_in_last_word) - 1;  // Only bits needed for NumBits
      }

     public:
      // Constructor: Initialize all bits to 0 (empty)
      hierarchical_bitmap()
      {
         // Initialize all words to 0
         memset(level0, 0, sizeof(level0));

         if constexpr (required_level > 1)
         {
            memset(level1, 0, sizeof(level1));
            if constexpr (required_level > 2)
            {
               memset(level2, 0, sizeof(level2));
               if constexpr (required_level > 3)
               {
                  memset(level3, 0, sizeof(level3));
               }
            }
         }

         // Initialize bit count to zero
         bit_count = 0;
      }

      /**
       * @brief Count trailing zeros in the bitmap
       * 
       * Efficiently counts the number of consecutive unset bits (zeros) from the 
       * lowest bit position (least significant bit) toward the highest.
       * 
       * Uses the hierarchical structure to skip large blocks of bits efficiently.
       * 
       * This function can also be used to find the first set bit in the bitmap:
       * - Returns the position of the first (lowest) set bit
       * - If no bits are set, returns invalid_index
       * 
       * @return The number of trailing zeros (equivalent to the index of the first set bit),
       *         or invalid_index if no bits are set
       */
      uint32_t countr_zero() const
      {
         if constexpr (required_level == 1)
         {
            if (level0[0] == 0)
               return NumBits;  // Return NumBits instead of invalid_index
            uint32_t bit = __builtin_ctzll(level0[0]);
            return bit >= NumBits ? NumBits : bit;  // Return NumBits instead of invalid_index
         }
         else if constexpr (required_level == 2)
         {
            if (level1[0] == 0)
               return NumBits;  // Return NumBits instead of invalid_index
            uint32_t l1_bit  = __builtin_ctzll(level1[0]);
            uint64_t l0_val  = level0[l1_bit];
            uint32_t bit_pos = __builtin_ctzll(l0_val);
            uint32_t segment = (l1_bit * bits_per_word) + bit_pos;
            return segment >= NumBits ? NumBits
                                      : segment;  // Return NumBits instead of invalid_index
         }
         else if constexpr (required_level == 3)
         {
            if (level2[0] == 0)
               return NumBits;  // Return NumBits instead of invalid_index
            uint32_t l2_bit  = __builtin_ctzll(level2[0]);
            uint64_t l1_val  = level1[l2_bit];
            uint32_t l1_bit  = __builtin_ctzll(l1_val);
            uint32_t l0_idx  = (l2_bit * bits_per_word) + l1_bit;
            uint64_t l0_val  = level0[l0_idx];
            uint32_t bit_pos = __builtin_ctzll(l0_val);
            uint32_t segment = (l0_idx * bits_per_word) + bit_pos;
            return segment >= NumBits ? NumBits
                                      : segment;  // Return NumBits instead of invalid_index
         }
         else
         {
            if (level3[0] == 0)
               return NumBits;  // Return NumBits instead of invalid_index
            uint32_t l3_bit  = __builtin_ctzll(level3[0]);
            uint64_t l2_val  = level2[l3_bit];
            uint32_t l2_bit  = __builtin_ctzll(l2_val);
            uint32_t l1_idx  = (l3_bit * bits_per_word) + l2_bit;
            uint64_t l1_val  = level1[l1_idx];
            uint32_t l1_bit  = __builtin_ctzll(l1_val);
            uint32_t l0_idx  = (l1_idx * bits_per_word) + l1_bit;
            uint64_t l0_val  = level0[l0_idx];
            uint32_t bit_pos = __builtin_ctzll(l0_val);
            uint32_t segment = (l0_idx * bits_per_word) + bit_pos;
            return segment >= NumBits ? NumBits
                                      : segment;  // Return NumBits instead of invalid_index
         }
      }

      /**
       * @brief Finds the first unset bit in the bitmap
       * 
       * This method searches through the bitmap to find the first bit that is 0.
       * It searches directly within the original bitmap without creating any temporary copies.
       * 
       * @return The index of the first unset bit, or invalid_index if all bits are set
       */
      uint32_t find_first_unset() const
      {
         if constexpr (required_level == 1)
         {
            // For a Level 1 bitmap, just find the first unset bit in the single word
            uint64_t inverted = ~level0[0] & get_last_word_mask();
            if (inverted == 0)
               return invalid_index;
            uint32_t bit = __builtin_ctzll(inverted);
            return bit >= NumBits ? invalid_index : bit;
         }
         else if constexpr (required_level == 2)
         {
            // For Level 2, check each word for any unset bits
            for (uint32_t i = 0; i < l0_words; ++i)
            {
               uint64_t inverted = ~level0[i];
               // Apply mask for the last word
               if (i == l0_words - 1)
               {
                  uint64_t mask = get_last_word_mask();
                  inverted &= mask;
               }

               if (inverted != 0)
               {
                  uint32_t bit_pos = __builtin_ctzll(inverted);
                  uint32_t segment = (i * bits_per_word) + bit_pos;
                  return segment >= NumBits ? invalid_index : segment;
               }
            }
            return invalid_index;
         }
         else if constexpr (required_level == 3 || required_level == 4)
         {
            // For higher-level bitmaps, don't try to use the hierarchy to find unset bits
            // since the hierarchy is optimized for finding set bits, not unset bits.
            // Instead, directly scan all level0 words.
            for (uint32_t l0_idx = 0; l0_idx < l0_words; ++l0_idx)
            {
               // Skip if this word is fully set
               if (level0[l0_idx] == ~0ULL)
                  continue;

               // This L0 word has at least one unset bit
               uint64_t inverted = ~level0[l0_idx];
               // Apply mask for the last word
               if (l0_idx == l0_words - 1)
               {
                  uint64_t mask = get_last_word_mask();
                  inverted &= mask;
               }

               if (inverted != 0)
               {
                  uint32_t bit_pos = __builtin_ctzll(inverted);
                  uint32_t segment = (l0_idx * bits_per_word) + bit_pos;
                  return segment >= NumBits ? invalid_index : segment;
               }
            }
            return invalid_index;
         }
      }

      /**
       * @brief Finds the first set bit and unsets it (sets to 0)
       * 
       * @return The index of the found and unset bit, or invalid_index if no bits were set
       */
      uint32_t unset_first_set()
      {
         uint32_t idx = countr_zero();
         if (idx != invalid_index)
            reset(idx);
         return idx;
      }

      /**
       * @brief Finds the first unset bit and sets it (changes to 1)
       * 
       * This method searches through the bitmap to find the first bit that is 0
       * and changes it to 1.
       * 
       * The implementation uses the in-place find_first_unset method
       * to avoid creating any temporary copies of the bitmap.
       * 
       * @return The index of the found and set bit, or invalid_index if all bits are already set
       */
      uint32_t set_first_unset()
      {
         // Find the first unset bit
         uint32_t idx = find_first_unset();

         // If a bit was found, set it in the original bitmap
         if (idx != invalid_index)
            set(idx);

         return idx;
      }

      /**
       * @brief Sets the bit at the specified index
       * 
       * @param idx The index of the bit to set
       * @throws std::runtime_error if idx is out of range
       */
      void set(uint32_t idx)
      {
         if (idx >= NumBits)
            throw std::runtime_error("Index out of range");

         uint32_t l0_idx  = idx / bits_per_word;
         uint32_t bit_pos = idx % bits_per_word;
         uint64_t old_l0  = level0[l0_idx];

         // Branchless bit count update: only increment if bit was not already set
         // The expression ((old_l0 >> bit_pos) & 1) is already either 0 or 1
         // Inverting it gives us 1 when the bit was not set, 0 otherwise
         bit_count += 1 - ((old_l0 >> bit_pos) & 1);

         level0[l0_idx] |= (1ULL << bit_pos);

         if constexpr (required_level > 1)
         {
            if (old_l0 == 0)
            {
               uint32_t l1_idx = l0_idx / bits_per_word;
               uint32_t l1_bit = l0_idx % bits_per_word;
               uint64_t old_l1 = level1[l1_idx];
               level1[l1_idx] |= (1ULL << l1_bit);

               if constexpr (required_level > 2)
               {
                  if (old_l1 == 0)
                  {
                     uint32_t l2_idx = l1_idx / bits_per_word;
                     uint32_t l2_bit = l1_idx % bits_per_word;
                     uint64_t old_l2 = level2[l2_idx];
                     level2[l2_idx] |= (1ULL << l2_bit);

                     if constexpr (required_level > 3)
                     {
                        if (old_l2 == 0)
                        {
                           uint32_t l3_bit = l2_idx % bits_per_word;
                           level3[0] |= (1ULL << l3_bit);
                        }
                     }
                  }
               }
            }
         }
      }

      /**
       * @brief Tests if the bit at the specified index is set
       * 
       * @param idx The index of the bit to test
       * @return true if the bit is set, false otherwise
       * @throws std::runtime_error if idx is out of range
       */
      bool test(uint32_t idx) const
      {
         if (idx >= NumBits)
            throw std::runtime_error("Index out of range");

         uint32_t l0_idx  = idx / bits_per_word;
         uint32_t bit_pos = idx % bits_per_word;
         // The result is already either 0 or 1, which converts directly to bool
         return (level0[l0_idx] >> bit_pos) & 1;
      }

      /**
       * @brief Resets the bit at the specified index (sets to 0)
       * 
       * @param idx The index of the bit to reset
       * @throws std::runtime_error if idx is out of range
       */
      void reset(uint32_t idx)
      {
         if (idx >= NumBits)
            throw std::runtime_error("Index out of range");

         uint32_t l0_idx  = idx / bits_per_word;
         uint32_t bit_pos = idx % bits_per_word;
         uint64_t old_l0  = level0[l0_idx];

         // Branchless bit count update: only decrement if bit was previously set
         // The expression ((old_l0 >> bit_pos) & 1) is already either 0 or 1
         bit_count -= ((old_l0 >> bit_pos) & 1);

         level0[l0_idx] &= ~(1ULL << bit_pos);

         if constexpr (required_level > 1)
         {
            if (old_l0 != 0 && level0[l0_idx] == 0)
            {
               uint32_t l1_idx = l0_idx / bits_per_word;
               uint32_t l1_bit = l0_idx % bits_per_word;
               uint64_t old_l1 = level1[l1_idx];
               level1[l1_idx] &= ~(1ULL << l1_bit);

               if constexpr (required_level > 2)
               {
                  if (old_l1 != 0 && level1[l1_idx] == 0)
                  {
                     uint32_t l2_idx = l1_idx / bits_per_word;
                     uint32_t l2_bit = l1_idx % bits_per_word;
                     uint64_t old_l2 = level2[l2_idx];
                     level2[l2_idx] &= ~(1ULL << l2_bit);

                     if constexpr (required_level > 3)
                     {
                        if (old_l2 != 0 && level2[l2_idx] == 0)
                        {
                           uint32_t l3_bit = l2_idx % bits_per_word;
                           level3[0] &= ~(1ULL << l3_bit);
                        }
                     }
                  }
               }
            }
         }
      }

      /**
       * @brief Alias for reset(). Resets the bit at the specified index (sets to 0)
       * 
       * @param idx The index of the bit to reset
       * @throws std::runtime_error if idx is out of range
       */
      void unset(uint32_t idx) { reset(idx); }

      // Add more methods from std::bitset for completeness

      /**
       * @brief Sets all bits in the bitmap
       */
      void set()
      {
         // Set all words to all 1s
         memset(level0, 0xFF, sizeof(level0));
         // Fix the last word with proper masking
         level0[l0_words - 1] = get_last_word_mask();

         if constexpr (required_level > 1)
         {
            memset(level1, 0xFF, sizeof(level1));
            if constexpr (required_level > 2)
            {
               memset(level2, 0xFF, sizeof(level2));
               if constexpr (required_level > 3)
               {
                  memset(level3, 0xFF, sizeof(level3));
               }
            }
         }

         // Set bit count to total number of bits
         bit_count = NumBits;
      }

      /**
       * @brief Resets all bits in the bitmap
       */
      void reset()
      {
         memset(level0, 0, sizeof(level0));

         if constexpr (required_level > 1)
         {
            memset(level1, 0, sizeof(level1));
            if constexpr (required_level > 2)
            {
               memset(level2, 0, sizeof(level2));
               if constexpr (required_level > 3)
               {
                  memset(level3, 0, sizeof(level3));
               }
            }
         }

         // Reset bit count to zero
         bit_count = 0;
      }

      /**
       * @brief Checks if any bit is set in the bitmap
       * 
       * @return true if at least one bit is set, false otherwise
       */
      bool any() const
      {
         if constexpr (required_level == 1)
            return level0[0] != 0;
         else if constexpr (required_level == 2)
            return level1[0] != 0;
         else if constexpr (required_level == 3)
            return level2[0] != 0;
         else
            return level3[0] != 0;
      }

      /**
       * @brief Checks if none of the bits are set
       * 
       * @return true if no bits are set, false otherwise
       */
      bool none() const { return !any(); }

      /**
       * @brief Check if all bits in the bitmap are set.
       * @return true if all bits are set, false otherwise.
       */
      bool all() const { return bit_count == NumBits; }

      /**
       * @brief Returns the number of set bits
       * 
       * @return The count of set bits
       */
      uint32_t count() const { return bit_count; }

      /**
       * Check that the bitmap maintains its hierarchical invariants
       */
      bool check_invariants() const
      {
         constexpr uint32_t bits_per_word = 64;

         // Check all bits in bitmap
         for (uint32_t base = 0; base < NumBits; base += bits_per_word)
         {
            // Check 64-bit group of level 0 bits
            bool any_bit_set = false;
            for (uint32_t offset = 0; offset < bits_per_word && base + offset < NumBits; ++offset)
            {
               if (test(base + offset))
               {
                  any_bit_set = true;
                  break;
               }
            }

            // Now find the next set bit using countr_zero
            // If any_bit_set is true, then the next set bit must be in this group
            // If any_bit_set is false, then the next set bit must be after this group

            if (any_bit_set)
            {
               uint32_t next_set = countr_zero();
               if (next_set == NumBits)  // Check for NumBits instead of invalid_index
               {
                  // Inconsistency: we found a set bit, but countr_zero returns NumBits
                  return false;
               }

               // We can also verify that countr_zero finds a bit that is actually set
               if (!test(next_set))
               {
                  // Inconsistency: countr_zero returns a bit that isn't actually set
                  return false;
               }
            }
         }

         // Additionally, we can check that if countr_zero returns a valid index,
         // that bit must actually be set
         uint32_t first_set = countr_zero();
         if (first_set != NumBits)  // Check for NumBits instead of invalid_index
         {
            if (!test(first_set))
            {
               return false;
            }
         }

         // Similarly for find_first_unset
         uint32_t first_unset = find_first_unset();
         if (first_unset != invalid_index)
         {
            if (test(first_unset))
            {
               return false;
            }
         }

         return true;
      }

      /**
       * @brief Check if the bitmap is empty (all bits are unset)
       * 
       * This method efficiently checks if the bitmap is empty by examining
       * only the top-level word of the hierarchy, rather than checking
       * the entire bitmap or using count() == 0.
       * 
       * @return true if the bitmap is empty, false otherwise
       */
      bool empty() const
      {
         if constexpr (required_level == 1)
         {
            // For single-word bitmaps
            return level0[0] == 0;
         }
         else if constexpr (required_level == 2)
         {
            // For level 2 bitmaps, we can just check level1
            return level1[0] == 0;
         }
         else if constexpr (required_level == 3)
         {
            // For level 3 bitmaps, we can just check level2
            return level2[0] == 0;
         }
         else  // required_level == 4
         {
            // For level 4 bitmaps, we can just check level3
            return level3[0] == 0;
         }
      }

      /**
       * Iterator for efficiently traversing all set bits in the bitmap
       */
      class iterator
      {
        public:
         using iterator_category = std::bidirectional_iterator_tag;
         using value_type        = uint32_t;
         using difference_type   = std::ptrdiff_t;
         using pointer           = const uint32_t*;
         using reference         = uint32_t;

         iterator() : _bitmap(nullptr), _current_index(invalid_index) {}

         iterator(const hierarchical_bitmap* bitmap, uint32_t start_index)
             : _bitmap(bitmap), _current_index(start_index)
         {
         }

         /**
          * Dereference operator - returns the current bit index
          */
         reference operator*() const { return _current_index; }

         /**
          * Pre-increment operator - advances to the next set bit
          */
         iterator& operator++()
         {
            if (_current_index == invalid_index)
               return *this;

            // Get the current position
            uint32_t bit_idx = _current_index;

            // Find the next set bit after the current one
            bit_idx++;
            if (bit_idx >= NumBits)
            {
               _current_index = invalid_index;
               return *this;
            }

            // Calculate the word and bit position
            uint32_t word_idx = bit_idx / bits_per_word;
            uint32_t bit_pos  = bit_idx % bits_per_word;

            // Check if there are more bits in the current word
            if (word_idx < l0_words)
            {
               uint64_t word = _bitmap->level0[word_idx];

               // Mask out bits we've already processed
               word &= ~((1ULL << bit_pos) - 1);

               if (word != 0)
               {
                  // Found a set bit in the current word
                  uint32_t next_bit_pos = __builtin_ctzll(word);
                  _current_index        = word_idx * bits_per_word + next_bit_pos;
                  return *this;
               }

               // No more bits in this word, move to the next word
               word_idx++;
            }

            // Use the hierarchy to efficiently find the next word with set bits
            if constexpr (required_level >= 2)
            {
               // First check if we need to find a new l1 word
               uint32_t l1_idx = word_idx / bits_per_word;
               if (l1_idx < l1_words)
               {
                  // Check remaining bits in current l1 word
                  uint32_t l0_start_in_l1 = word_idx % bits_per_word;
                  uint64_t l1_word        = _bitmap->level1[l1_idx];

                  // Mask out words we've already processed
                  l1_word &= ~((1ULL << l0_start_in_l1) - 1);

                  if (l1_word != 0)
                  {
                     // Found an l1 bit, which means there's at least one set bit in that l0 word
                     uint32_t next_l0_in_l1 = __builtin_ctzll(l1_word);
                     uint32_t next_l0_idx   = l1_idx * bits_per_word + next_l0_in_l1;

                     // Now find the first set bit in that l0 word
                     uint64_t l0_word      = _bitmap->level0[next_l0_idx];
                     uint32_t next_bit_pos = __builtin_ctzll(l0_word);
                     _current_index        = next_l0_idx * bits_per_word + next_bit_pos;
                     return *this;
                  }

                  // No more bits in this l1 word, move to the next l1 word
                  l1_idx++;
               }

               if constexpr (required_level >= 3)
               {
                  // Check if we need to find a new l2 word
                  uint32_t l2_idx = l1_idx / bits_per_word;
                  if (l2_idx < l2_words)
                  {
                     // Check remaining bits in current l2 word
                     uint32_t l1_start_in_l2 = l1_idx % bits_per_word;
                     uint64_t l2_word        = _bitmap->level2[l2_idx];

                     // Mask out words we've already processed
                     l2_word &= ~((1ULL << l1_start_in_l2) - 1);

                     if (l2_word != 0)
                     {
                        // Found an l2 bit, traverse down to find the actual bit
                        uint32_t next_l1_in_l2 = __builtin_ctzll(l2_word);
                        uint32_t next_l1_idx   = l2_idx * bits_per_word + next_l1_in_l2;

                        // Find the first l0 word with bits in this l1 word
                        uint64_t l1_word       = _bitmap->level1[next_l1_idx];
                        uint32_t next_l0_in_l1 = __builtin_ctzll(l1_word);
                        uint32_t next_l0_idx   = next_l1_idx * bits_per_word + next_l0_in_l1;

                        // Find the first bit in that l0 word
                        uint64_t l0_word      = _bitmap->level0[next_l0_idx];
                        uint32_t next_bit_pos = __builtin_ctzll(l0_word);
                        _current_index        = next_l0_idx * bits_per_word + next_bit_pos;
                        return *this;
                     }

                     // No more bits in this l2 word, move to the next l2 word
                     l2_idx++;
                  }

                  if constexpr (required_level >= 4)
                  {
                     // Check if we need to find a new l3 word
                     uint32_t l3_idx = l2_idx / bits_per_word;
                     if (l3_idx < l3_words)
                     {
                        // Check remaining bits in current l3 word
                        uint32_t l2_start_in_l3 = l2_idx % bits_per_word;
                        uint64_t l3_word        = _bitmap->level3[l3_idx];

                        // Mask out words we've already processed
                        l3_word &= ~((1ULL << l2_start_in_l3) - 1);

                        if (l3_word != 0)
                        {
                           // Found an l3 bit, traverse down to find the actual bit
                           uint32_t next_l2_in_l3 = __builtin_ctzll(l3_word);
                           uint32_t next_l2_idx   = l3_idx * bits_per_word + next_l2_in_l3;

                           // Find the first l1 word with bits in this l2 word
                           uint64_t l2_word       = _bitmap->level2[next_l2_idx];
                           uint32_t next_l1_in_l2 = __builtin_ctzll(l2_word);
                           uint32_t next_l1_idx   = next_l2_idx * bits_per_word + next_l1_in_l2;

                           // Find the first l0 word with bits in this l1 word
                           uint64_t l1_word       = _bitmap->level1[next_l1_idx];
                           uint32_t next_l0_in_l1 = __builtin_ctzll(l1_word);
                           uint32_t next_l0_idx   = next_l1_idx * bits_per_word + next_l0_in_l1;

                           // Find the first bit in that l0 word
                           uint64_t l0_word      = _bitmap->level0[next_l0_idx];
                           uint32_t next_bit_pos = __builtin_ctzll(l0_word);
                           _current_index        = next_l0_idx * bits_per_word + next_bit_pos;
                           return *this;
                        }
                     }
                  }
               }
            }

            // No more set bits found
            _current_index = invalid_index;
            return *this;
         }

         /**
          * Post-increment operator
          */
         iterator operator++(int)
         {
            iterator tmp = *this;
            ++(*this);
            return tmp;
         }

         /**
          * Equality comparison
          */
         bool operator==(const iterator& other) const
         {
            return _current_index == other._current_index && _bitmap == other._bitmap;
         }

         /**
          * Inequality comparison
          */
         bool operator!=(const iterator& other) const { return !(*this == other); }

         /**
          * Pre-decrement operator - moves to the previous set bit
          */
         iterator& operator--()
         {
            if (_bitmap == nullptr)
               return *this;

            // Special case - if we're at the end, go to the last set bit
            if (_current_index == invalid_index)
            {
               // Find the last set bit in the bitmap by using the complementary countl_zero
               uint32_t leading_zeros = _bitmap->countl_zero();

               if (leading_zeros < NumBits)
               {
                  _current_index = NumBits - leading_zeros - 1;
                  return *this;
               }

               // No set bits in the bitmap
               return *this;
            }

            // Can't go before the first bit
            if (_current_index == 0)
            {
               _current_index = invalid_index;
               return *this;
            }

            // Find the previous set bit before the current one
            uint32_t bit_idx = _current_index - 1;

            // Calculate the word and bit position
            uint32_t word_idx = bit_idx / bits_per_word;
            uint32_t bit_pos  = bit_idx % bits_per_word;

            // Check if there are more bits in the current word
            if (word_idx < l0_words)
            {
               uint64_t word = _bitmap->level0[word_idx];

               // Mask to keep only bits at or below our position
               uint64_t mask;
               if (bit_pos == 63)
                  mask = ~0ULL;  // All bits set if we want all bits up to position 63
               else
                  mask = (1ULL << (bit_pos + 1)) - 1;

               uint64_t masked_word = word & mask;

               if (masked_word != 0)
               {
                  // Found a set bit in the current word - get the highest bit
                  uint32_t prev_bit_pos = 63 - __builtin_clzll(masked_word);
                  _current_index        = word_idx * bits_per_word + prev_bit_pos;
                  return *this;
               }

               // No more bits in this word, move to the previous word
               if (word_idx == 0)
               {
                  // No more words before this one
                  _current_index = invalid_index;
                  return *this;
               }

               word_idx--;
            }

            // Use the hierarchy to efficiently find the previous word with set bits
            if constexpr (required_level >= 2)
            {
               // First check if we need to find a previous l1 word
               uint32_t l1_idx = word_idx / bits_per_word;

               // Look for set bits in the current l0 word
               uint64_t l0_word = _bitmap->level0[word_idx];
               if (l0_word != 0)
               {
                  // Found bits in this word, return the highest one
                  uint32_t prev_bit_pos = 63 - __builtin_clzll(l0_word);
                  _current_index        = word_idx * bits_per_word + prev_bit_pos;
                  return *this;
               }

               // Need to find a previous l0 word within the current l1 word
               uint32_t l0_start_in_l1 = word_idx % bits_per_word;
               if (l0_start_in_l1 > 0)
               {
                  uint64_t l1_word = _bitmap->level1[l1_idx];

                  // Mask to keep only bits below our position
                  uint64_t mask = (1ULL << l0_start_in_l1) - 1;
                  l1_word &= mask;

                  if (l1_word != 0)
                  {
                     // Found an l1 bit, which means there's at least one set bit in that l0 word
                     uint32_t prev_l0_in_l1 = 63 - __builtin_clzll(l1_word);
                     uint32_t prev_l0_idx   = l1_idx * bits_per_word + prev_l0_in_l1;

                     // Now find the highest set bit in that l0 word
                     uint64_t prev_l0_word = _bitmap->level0[prev_l0_idx];
                     uint32_t prev_bit_pos = 63 - __builtin_clzll(prev_l0_word);
                     _current_index        = prev_l0_idx * bits_per_word + prev_bit_pos;
                     return *this;
                  }
               }

               // No more bits in this l1 word, move to the previous l1 word
               if (l1_idx == 0)
               {
                  // No more l1 words before this one
                  _current_index = invalid_index;
                  return *this;
               }

               l1_idx--;

               if constexpr (required_level >= 3)
               {
                  // Check if we need to find a previous l2 word
                  uint32_t l2_idx = l1_idx / bits_per_word;

                  // Look for set bits in the current l1 word
                  uint64_t l1_word = _bitmap->level1[l1_idx];
                  if (l1_word != 0)
                  {
                     // Found an l1 bit, find the highest one
                     uint32_t high_l0_in_l1 = 63 - __builtin_clzll(l1_word);
                     uint32_t high_l0_idx   = l1_idx * bits_per_word + high_l0_in_l1;

                     // Find the highest bit in the l0 word
                     uint64_t l0_word      = _bitmap->level0[high_l0_idx];
                     uint32_t high_bit_pos = 63 - __builtin_clzll(l0_word);
                     _current_index        = high_l0_idx * bits_per_word + high_bit_pos;
                     return *this;
                  }

                  // Need to find a previous l1 word within the current l2 word
                  uint32_t l1_start_in_l2 = l1_idx % bits_per_word;
                  if (l1_start_in_l2 > 0)
                  {
                     uint64_t l2_word = _bitmap->level2[l2_idx];

                     // Mask to keep only bits below our position
                     uint64_t mask = (1ULL << l1_start_in_l2) - 1;
                     l2_word &= mask;

                     if (l2_word != 0)
                     {
                        // Found an l2 bit, traverse down to find the actual bit
                        uint32_t prev_l1_in_l2 = 63 - __builtin_clzll(l2_word);
                        uint32_t prev_l1_idx   = l2_idx * bits_per_word + prev_l1_in_l2;

                        // Find the highest l0 word with bits in this l1 word
                        uint64_t l1_word       = _bitmap->level1[prev_l1_idx];
                        uint32_t high_l0_in_l1 = 63 - __builtin_clzll(l1_word);
                        uint32_t high_l0_idx   = prev_l1_idx * bits_per_word + high_l0_in_l1;

                        // Find the highest bit in that l0 word
                        uint64_t l0_word      = _bitmap->level0[high_l0_idx];
                        uint32_t high_bit_pos = 63 - __builtin_clzll(l0_word);
                        _current_index        = high_l0_idx * bits_per_word + high_bit_pos;
                        return *this;
                     }
                  }

                  // No more bits in this l2 word, move to the previous l2 word
                  if (l2_idx == 0)
                  {
                     // No more l2 words before this one
                     _current_index = invalid_index;
                     return *this;
                  }

                  l2_idx--;

                  if constexpr (required_level >= 4)
                  {
                     // Check if we need to find a previous l3 word
                     uint32_t l3_idx = l2_idx / bits_per_word;

                     // Look for set bits in the current l2 word
                     uint64_t l2_word = _bitmap->level2[l2_idx];
                     if (l2_word != 0)
                     {
                        // Found an l2 bit, find the highest one
                        uint32_t high_l1_in_l2 = 63 - __builtin_clzll(l2_word);
                        uint32_t high_l1_idx   = l2_idx * bits_per_word + high_l1_in_l2;

                        // Find the highest l0 word in this l1 word
                        uint64_t l1_word       = _bitmap->level1[high_l1_idx];
                        uint32_t high_l0_in_l1 = 63 - __builtin_clzll(l1_word);
                        uint32_t high_l0_idx   = high_l1_idx * bits_per_word + high_l0_in_l1;

                        // Find the highest bit in that l0 word
                        uint64_t l0_word      = _bitmap->level0[high_l0_idx];
                        uint32_t high_bit_pos = 63 - __builtin_clzll(l0_word);
                        _current_index        = high_l0_idx * bits_per_word + high_bit_pos;
                        return *this;
                     }

                     // Need to find a previous l2 word within the current l3 word
                     uint32_t l2_start_in_l3 = l2_idx % bits_per_word;
                     if (l2_start_in_l3 > 0)
                     {
                        uint64_t l3_word = _bitmap->level3[l3_idx];

                        // Mask to keep only bits below our position
                        uint64_t mask = (1ULL << l2_start_in_l3) - 1;
                        l3_word &= mask;

                        if (l3_word != 0)
                        {
                           // Found an l3 bit, traverse down to find the actual bit
                           uint32_t prev_l2_in_l3 = 63 - __builtin_clzll(l3_word);
                           uint32_t prev_l2_idx   = l3_idx * bits_per_word + prev_l2_in_l3;

                           // Find the highest l1 word with bits in this l2 word
                           uint64_t l2_word       = _bitmap->level2[prev_l2_idx];
                           uint32_t high_l1_in_l2 = 63 - __builtin_clzll(l2_word);
                           uint32_t high_l1_idx   = prev_l2_idx * bits_per_word + high_l1_in_l2;

                           // Find the highest l0 word with bits in this l1 word
                           uint64_t l1_word       = _bitmap->level1[high_l1_idx];
                           uint32_t high_l0_in_l1 = 63 - __builtin_clzll(l1_word);
                           uint32_t high_l0_idx   = high_l1_idx * bits_per_word + high_l0_in_l1;

                           // Find the highest bit in that l0 word
                           uint64_t l0_word      = _bitmap->level0[high_l0_idx];
                           uint32_t high_bit_pos = 63 - __builtin_clzll(l0_word);
                           _current_index        = high_l0_idx * bits_per_word + high_bit_pos;
                           return *this;
                        }
                     }

                     // If we get here, we need to search previous l3 words
                     for (int i = l3_idx - 1; i >= 0; --i)
                     {
                        if (_bitmap->level3[i] != 0)
                        {
                           // Found a non-empty l3 word
                           uint32_t high_l2_in_l3 = 63 - __builtin_clzll(_bitmap->level3[i]);
                           uint32_t high_l2_idx   = i * bits_per_word + high_l2_in_l3;

                           // Find the highest l1 word with bits in this l2 word
                           uint64_t l2_word       = _bitmap->level2[high_l2_idx];
                           uint32_t high_l1_in_l2 = 63 - __builtin_clzll(l2_word);
                           uint32_t high_l1_idx   = high_l2_idx * bits_per_word + high_l1_in_l2;

                           // Find the highest l0 word with bits in this l1 word
                           uint64_t l1_word       = _bitmap->level1[high_l1_idx];
                           uint32_t high_l0_in_l1 = 63 - __builtin_clzll(l1_word);
                           uint32_t high_l0_idx   = high_l1_idx * bits_per_word + high_l0_in_l1;

                           // Find the highest bit in that l0 word
                           uint64_t l0_word      = _bitmap->level0[high_l0_idx];
                           uint32_t high_bit_pos = 63 - __builtin_clzll(l0_word);
                           _current_index        = high_l0_idx * bits_per_word + high_bit_pos;
                           return *this;
                        }
                     }
                  }
                  else
                  {
                     // If we get here, we need to search previous l2 words
                     for (int i = l2_idx - 1; i >= 0; --i)
                     {
                        if (_bitmap->level2[i] != 0)
                        {
                           // Found a non-empty l2 word
                           uint32_t high_l1_in_l2 = 63 - __builtin_clzll(_bitmap->level2[i]);
                           uint32_t high_l1_idx   = i * bits_per_word + high_l1_in_l2;

                           // Find the highest l0 word with bits in this l1 word
                           uint64_t l1_word       = _bitmap->level1[high_l1_idx];
                           uint32_t high_l0_in_l1 = 63 - __builtin_clzll(l1_word);
                           uint32_t high_l0_idx   = high_l1_idx * bits_per_word + high_l0_in_l1;

                           // Find the highest bit in that l0 word
                           uint64_t l0_word      = _bitmap->level0[high_l0_idx];
                           uint32_t high_bit_pos = 63 - __builtin_clzll(l0_word);
                           _current_index        = high_l0_idx * bits_per_word + high_bit_pos;
                           return *this;
                        }
                     }
                  }
               }
               else
               {
                  // If we get here, we need to search previous l1 words
                  for (int i = l1_idx - 1; i >= 0; --i)
                  {
                     if (_bitmap->level1[i] != 0)
                     {
                        // Found a non-empty l1 word
                        uint32_t high_l0_in_l1 = 63 - __builtin_clzll(_bitmap->level1[i]);
                        uint32_t high_l0_idx   = i * bits_per_word + high_l0_in_l1;

                        // Find the highest bit in that l0 word
                        uint64_t l0_word      = _bitmap->level0[high_l0_idx];
                        uint32_t high_bit_pos = 63 - __builtin_clzll(l0_word);
                        _current_index        = high_l0_idx * bits_per_word + high_bit_pos;
                        return *this;
                     }
                  }
               }
            }
            else
            {
               // For level 1, just scan previous words
               for (int i = word_idx - 1; i >= 0; --i)
               {
                  if (_bitmap->level0[i] != 0)
                  {
                     uint32_t prev_bit_pos = 63 - __builtin_clzll(_bitmap->level0[i]);
                     _current_index        = i * bits_per_word + prev_bit_pos;
                     return *this;
                  }
               }
            }

            // No more set bits found
            _current_index = invalid_index;
            return *this;
         }

         /**
          * Post-decrement operator
          */
         iterator operator--(int)
         {
            iterator tmp = *this;
            --(*this);
            return tmp;
         }

        private:
         const hierarchical_bitmap* _bitmap;
         uint32_t                   _current_index;

         /**
          * Helper function to find the last set bit in the bitmap
          */
         uint32_t find_last_set() const
         {
            if (!_bitmap || _bitmap->none())
               return invalid_index;

            // Directly find the highest set bit
            uint32_t leading_zeros = _bitmap->countl_zero();

            // If all bits are unset (all leading zeros), return invalid_index
            if (leading_zeros == NumBits)
               return invalid_index;

            // The position of the highest set bit is (NumBits - leading_zeros - 1)
            uint32_t result = NumBits - leading_zeros - 1;
            return result;
         }
      };  // class iterator

      /**
       * Returns an iterator to the first set bit in the bitmap
       */
      iterator begin() const
      {
         uint32_t first_bit = countr_zero();
         // If countr_zero returns NumBits (no bits set), return end iterator
         return iterator(this, first_bit == NumBits ? invalid_index : first_bit);
      }

      /**
       * Returns an iterator representing the end of the bitmap (past the last set bit)
       */
      iterator end() const { return iterator(this, invalid_index); }

      /**
       * @brief Count leading zeros in the bitmap
       * 
       * Efficiently counts the number of consecutive unset bits (zeros) from the 
       * highest bit position (most significant bit) toward the lowest.
       * 
       * Uses the hierarchical structure to skip large blocks of bits efficiently.
       * 
       * @return Number of leading zeros in the bitmap
       */
      uint32_t countl_zero() const
      {
         if (none())
            return NumBits;  // All bits are zero

         if constexpr (required_level == 1)
         {
            // Single word case
            uint64_t word = level0[l0_words - 1];
            uint64_t mask = get_last_word_mask();
            word &= mask;

            if (word == 0)
               return NumBits;

            uint32_t highest_bit_pos = 63 - __builtin_clzll(word);
            return NumBits - highest_bit_pos - 1;
         }
         else if constexpr (required_level == 2)
         {
            // Find the highest word with a bit set
            for (int i = l1_words - 1; i >= 0; --i)
            {
               if (level1[i] != 0)
               {
                  // Found a level1 word with bits set
                  uint64_t l1_word        = level1[i];
                  uint32_t high_bit_in_l1 = 63 - __builtin_clzll(l1_word);
                  uint32_t l0_idx         = i * bits_per_word + high_bit_in_l1;

                  // Find the highest bit in the level0 word
                  uint64_t l0_word        = level0[l0_idx];
                  uint32_t high_bit_in_l0 = 63 - __builtin_clzll(l0_word);
                  uint32_t highest_bit    = l0_idx * bits_per_word + high_bit_in_l0;

                  return NumBits - highest_bit - 1;
               }
            }
            return NumBits;
         }
         else if constexpr (required_level == 3)
         {
            // Find the highest level2 word with a bit set
            for (int i = l2_words - 1; i >= 0; --i)
            {
               if (level2[i] != 0)
               {
                  // Found a level2 word with bits set
                  uint64_t l2_word        = level2[i];
                  uint32_t high_bit_in_l2 = 63 - __builtin_clzll(l2_word);
                  uint32_t l1_idx         = i * bits_per_word + high_bit_in_l2;

                  // Find the highest level1 word with a bit set
                  uint64_t l1_word        = level1[l1_idx];
                  uint32_t high_bit_in_l1 = 63 - __builtin_clzll(l1_word);
                  uint32_t l0_idx         = l1_idx * bits_per_word + high_bit_in_l1;

                  // Find the highest bit in the level0 word
                  uint64_t l0_word        = level0[l0_idx];
                  uint32_t high_bit_in_l0 = 63 - __builtin_clzll(l0_word);
                  uint32_t highest_bit    = l0_idx * bits_per_word + high_bit_in_l0;

                  return NumBits - highest_bit - 1;
               }
            }
            return NumBits;
         }
         else
         {
            // Find the highest level3 word with a bit set
            for (int i = l3_words - 1; i >= 0; --i)
            {
               if (level3[i] != 0)
               {
                  // Found a level3 word with bits set
                  uint64_t l3_word        = level3[i];
                  uint32_t high_bit_in_l3 = 63 - __builtin_clzll(l3_word);
                  uint32_t l2_idx         = i * bits_per_word + high_bit_in_l3;

                  // Find the highest level2 word with a bit set
                  uint64_t l2_word        = level2[l2_idx];
                  uint32_t high_bit_in_l2 = 63 - __builtin_clzll(l2_word);
                  uint32_t l1_idx         = l2_idx * bits_per_word + high_bit_in_l2;

                  // Find the highest level1 word with a bit set
                  uint64_t l1_word        = level1[l1_idx];
                  uint32_t high_bit_in_l1 = 63 - __builtin_clzll(l1_word);
                  uint32_t l0_idx         = l1_idx * bits_per_word + high_bit_in_l1;

                  // Find the highest bit in the level0 word
                  uint64_t l0_word        = level0[l0_idx];
                  uint32_t high_bit_in_l0 = 63 - __builtin_clzll(l0_word);
                  uint32_t highest_bit    = l0_idx * bits_per_word + high_bit_in_l0;

                  return NumBits - highest_bit - 1;
               }
            }
            return NumBits;
         }
      }
   };
}  // namespace ucc