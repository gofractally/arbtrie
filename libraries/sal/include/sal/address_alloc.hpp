#pragma once
#include <atomic>
#include <bit>  // For std::countr_zero, std::popcount
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <sstream>

#include <sal/block_allocator.hpp>
#include <sal/config.hpp>
#include <sal/debug.hpp>
#include <sal/mapping.hpp>

namespace sal
{
   // Constants for address allocation

   /// Derived constants for page/region layout
   /// @group constants Address Allocation Constants
   ///@{

   constexpr uint32_t address_slot_size = sizeof(uint64_t);
   /**
       * Each region grows by 1 page at a time.
       */
   // clang-format off
   constexpr static uint32_t region_page_size = os_page_size;  // 4096 bytes
   constexpr static uint16_t region_page_capacity = region_page_size / address_slot_size;  // 512 slots per page
   constexpr static uint16_t cachelines_per_region_page = region_page_capacity / cacheline_size;  // 64 cachelines per page
   constexpr static uint16_t addresses_per_cacheline = cacheline_size / address_slot_size;    // 8 slots per cacheline
   constexpr static uint64_t max_regions = 1 << 16;  // 2^16 regions is the max constexpr uint64_t max_addresses_per_region =
   constexpr static uint64_t max_address_index = 1 << 16;  // 2^16 address::index is uint16_t
   constexpr static uint64_t region_block_size = region_page_size * max_regions;  // 2^32 addresses
   constexpr static uint64_t max_region_blocks = max_address_index / region_page_capacity;  // 128 blocks per region
   // clang-format on

   // Type alias for region ID
   using region_id = uint16_t;

   // slots per region page
   constexpr static uint32_t address_region_page_capacity = 512;
   struct address
   {
      uint16_t region;  /// region id
      uint16_t index;   /// index into the region
      uint16_t region_page() const { return index / address_region_page_capacity; }
      /// index 0 to 512
      uint16_t region_page_slot() const { return index % address_region_page_capacity; }
      /// 0 to 8 cacheline index (64 bit bitmap)
      uint16_t cacheline_idx() const { return region_page_slot() / 64; }
      /// the cacheline index is always %64 the base index because it is a contuous address space
      uint16_t index_in_cacheline() const { return index % 64; }

      friend std::ostream& operator<<(std::ostream& os, const address& addr)
      {
         return os << "r" << addr.region << ".i" << addr.index;
      }
   };

   struct address_allocation
   {
      address                addr;  /// allocated address
      std::atomic<uint16_t>& slot;  /// reference to the allocated slot
   };

   /**
    * @brief Try to clear a bit in the bitmap
    * @param bitmap the bitmap to clear the bit in
    * @param bit_mask the bit to clear
    * @param prev_bitmap the previous bitmap value, updated to the value before the clear
    * @return true if the bit was cleared, false if it was already cleared
    */
   inline bool try_clear_bit(std::atomic<uint64_t>& bitmap,
                             uint64_t               bit_mask,
                             uint64_t&              prev_bitmap)
   {
      do
      {
         if (!(prev_bitmap & bit_mask))
            return false;
      } while (!bitmap.compare_exchange_weak(prev_bitmap, prev_bitmap & ~bit_mask,
                                             std::memory_order_relaxed));
      return true;
   }

   /**
    * This class manages the allocation of 64 bit atomic slots according to
    * a region / page / cacheline hierarchy with a hierarchical bitmap 
    * to identify free slots. 
    * 
    * It is designed to be thread safe and lock free, except for the
    * lock in the block allocator everytime major growth is needed.
    */
   class address_alloc
   {
     public:
      /** Constructs an address allocator using the specified file path as the root for memory-mapped files.
       * Creates three files:
       * - file/address_blocks: Contains the actual address slots
       * - file/page_headers: Contains the page headers with free slot bitmaps
       * - file/alloc_header: Contains the root header with region tracking
       */
      explicit address_alloc(const std::filesystem::path& file);
      ~address_alloc();

      // Delete copy operations
      address_alloc(const address_alloc&)            = delete;
      address_alloc& operator=(const address_alloc&) = delete;

      /** Get a new region ID for allocating addresses.
       * Thread-safe.
       * @return A new unique region ID
       */
      region_id get_new_region();

      /** Get a new address in the specified region.
       * Thread-safe.
       * @param id The region ID to allocate from
       * @return A new address and reference to its slot
       * @throws std::runtime_error if no free slots are available
       */
      address_allocation get_new_address(region_id id);

      /** Free a previously allocated address.
       * Thread-safe.
       * @param addr The address to free
       * @throws std::runtime_error if the address is invalid
       */
      void free_address(address addr);

      /** Count the total number of allocated addresses across all regions.
       * Thread-safe.
       * @return The number of allocated addresses
       */
      uint64_t count() const;

      std::atomic<uint16_t>& get_slot(address a)
      {
         auto* block = get_address_block(a.region_page());
         return block->pages[a.region].slot[a.region_page_slot()];
      }
      const std::atomic<uint16_t>& get_slot(address a) const
      {
         auto* block = get_address_block(a.region_page());
         return block->pages[a.region].slot[a.region_page_slot()];
      }

      /**
       * Validate the consistency of the bitmap hierarchy at all levels.
       * This checks that the bitmaps at each level (region, page, cacheline, slot)
       * are correctly synchronized with each other.
       *
       * @return A string describing any inconsistencies found, or an empty string if none
       */
      std::string validate_invariant() const
      {
         return {};
         std::stringstream errors;
         uint16_t          num_blocks = get_num_blocks();

         // Check all active regions
         for (uint32_t region = 0;
              region < get_alloc_header()->next_region.load(std::memory_order_relaxed); ++region)
         {
            const auto& region_header = get_alloc_header()->region_headers[region];

            // Check each potential page block
            for (uint16_t page_idx = 0; page_idx < num_blocks; ++page_idx)
            {
               // Check if region_header says this page has free slots
               bool     region_says_page_has_free_slots = false;
               uint64_t page_block_mask                 = 1ULL << (page_idx % 64);
               uint64_t page_blocks_bitmap =
                   region_header._page_blocks_with_free_slots[page_idx >= 64].load(
                       std::memory_order_relaxed);

               region_says_page_has_free_slots = (page_blocks_bitmap & page_block_mask) != 0;

               // Get the page header and check its cachelines bitmap
               auto*    page_header_block = get_region_page_header_block(page_idx);
               auto&    page_header       = page_header_block->page_headers[region];
               uint64_t cachelines_bitmap =
                   page_header.cachelines_with_free_slots.load(std::memory_order_relaxed);
               bool page_has_any_free_cachelines = cachelines_bitmap != 0;

               // Region and page level inconsistency check
               if (region_says_page_has_free_slots != page_has_any_free_cachelines)
               {
                  errors << "Inconsistency for region " << region << ", page " << page_idx
                         << ": region header says "
                         << (region_says_page_has_free_slots ? "has" : "doesn't have")
                         << " free slots, but page header says "
                         << (page_has_any_free_cachelines ? "has" : "doesn't have")
                         << " free cachelines. Region bitmap: 0x" << std::hex << page_blocks_bitmap
                         << ", page bitmap: 0x" << cachelines_bitmap << std::dec << "\n";
               }

               // Track if any cacheline in this page actually has free slots
               bool actual_page_has_free_slots = false;

               // Check each cacheline in this page
               for (uint16_t cacheline_idx = 0; cacheline_idx < cachelines_per_region_page;
                    ++cacheline_idx)
               {
                  bool page_says_cacheline_has_free_slots =
                      (cachelines_bitmap & (1ULL << cacheline_idx)) != 0;

                  // Compute the absolute slot index range for this cacheline
                  uint16_t first_slot_idx = page_idx * address_region_page_capacity +
                                            cacheline_idx * addresses_per_cacheline;
                  uint16_t last_slot_idx = first_slot_idx + addresses_per_cacheline - 1;

                  // Check if any slots in this cacheline are free
                  bool cacheline_has_free_slots = false;

                  // Calculate slot indices for this cacheline and check bitmap
                  for (uint16_t slot_idx = first_slot_idx; slot_idx <= last_slot_idx; ++slot_idx)
                  {
                     int      free_slot_idx = slot_idx / 64;
                     uint64_t slot_bit      = 1ULL << (slot_idx % 64);

                     uint64_t free_slot_bitmap =
                         page_header.free_slots[free_slot_idx].load(std::memory_order_relaxed);

                     if (free_slot_bitmap & slot_bit)
                     {
                        // Found a free slot in this cacheline
                        cacheline_has_free_slots = true;
                        actual_page_has_free_slots =
                            true;  // Track that page has at least one free slot
                        break;
                     }
                  }

                  // Cacheline and free slots inconsistency check
                  if (page_says_cacheline_has_free_slots != cacheline_has_free_slots)
                  {
                     errors << "Inconsistency for region " << region << ", page " << page_idx
                            << ", cacheline " << cacheline_idx << ": page header says "
                            << (page_says_cacheline_has_free_slots ? "has" : "doesn't have")
                            << " free slots, but slot bitmaps indicate it "
                            << (cacheline_has_free_slots ? "has" : "doesn't have")
                            << " free slots. Cachelines bitmap: 0x" << std::hex << cachelines_bitmap
                            << std::dec << ", slots " << first_slot_idx << "-" << last_slot_idx
                            << "\n";
                  }
               }

               // Final consistency check: if any slots are actually free, the region bitmap should indicate this
               if (actual_page_has_free_slots != region_says_page_has_free_slots)
               {
                  errors << "Inconsistency for region " << region << ", page " << page_idx
                         << ": actual slot state indicates page "
                         << (actual_page_has_free_slots ? "has" : "doesn't have")
                         << " free slots, but region bitmap says it "
                         << (region_says_page_has_free_slots ? "has" : "doesn't have")
                         << " free slots.\n";
               }
            }
         }

         return errors.str();
      }

     private:
      /**
       * per region, we have a header that tracks
       * the pages with free slots in them, a max of 128 pages (2^16 slots per region)
       * with 2^16 regions matches one slot for all 2^32 address possible. 
       */
      struct region_header
      {
         /// _region_page_blocks has max 128 address blocks (256 MB each)
         std::atomic<uint64_t> _page_blocks_with_free_slots[2];  /// 1 bit per _region_page_blocks
      };

      /**
       * per region, per page in the region we have a header that tracks
       * that free slots in that page for rapid allocation.
       */
      struct region_page_header
      {
         /// 1 bit per cacheline, total 64 cachelines per page
         /// I put this here because it gets read every time, and
         /// we might as well bring 7 of 8 free_slots with us because
         /// the majority of the time thats the data we will need next
         std::atomic<uint64_t> cachelines_with_free_slots;  // 8 bytes

         /// this is a higher resolution view of cachelines with free slots,
         /// that gives 8 bits per cacheline to represen the slots
         /// 1 bit per region_page::slot, aka one uint64_t per cacheline
         std::atomic<uint64_t> free_slots[8];  // 64 bytes

         /// TODO future meta data... because we need to keep cacheline aligned
         std::atomic<uint64_t> cachelines_with_4plus_slots;  // 8 bytes
         std::atomic<uint64_t> cachelines_with_read_bits;    // TODO
         uint64_t              padding[5];                   // Pad to 128 bytes (power of 2)

         /// @return a cacheline index from 0 to 64, with 64 meaning inconsitency detected (aka not found)
         uint16_t first_free_cacheline() const
         {
            uint64_t cachelines_bitmap = cachelines_with_free_slots.load(std::memory_order_acquire);
            if (cachelines_bitmap == 0)
               return 64;
            return std::countr_zero(cachelines_bitmap);
         }

         /// @param cline_idx a cline known to have free slots (see first_free_cacheline)
         /// @return a slot index from 0 to 512, with 512 meaning inconsitency detected (aka not found)
         uint16_t first_free_slot(uint16_t cline_idx) const
         {
            int      free_slot_idx    = cline_idx * 8 / 64;
            uint64_t free_slot_bitmap = free_slots[free_slot_idx].load(std::memory_order_acquire);
            if (free_slot_bitmap == 0)
               return 512;  // inconsitency detected
            return free_slot_idx * 64 + std::countr_zero(free_slot_bitmap);
         }

         /// @param cline_idx a cline from 0 to 63
         /// @param slot_idx a slot index from 0 to 511 (compatible with cline_idx)
         /// @return 0 if the slot was already taken or inconsitency detected
         /// @return 1 if the slot was allocated
         /// @return 2 if the last slot in the cacheline index was allocated
         int try_alloc_slot(uint16_t cline_idx, uint16_t slot_idx)
         {
            int      free_slot_idx    = slot_idx / 64;
            uint64_t free_slot_bitmap = free_slots[free_slot_idx].load(std::memory_order_acquire);
            if (free_slot_bitmap == 0)
               return 0;
            uint64_t slot_bit = 1ULL << (slot_idx % 64);

            if (not try_clear_bit(free_slots[free_slot_idx], slot_bit, free_slot_bitmap))
               return 0;

            // we need to determine if we cleared the last bit in the cacheline
            // if we did, we need to clear the bit in the cachelines_with_free_slots
            // std::bitset<64> bitmap(free_slot_bitmap);

            free_slot_bitmap &= ~slot_bit;
            if (not((free_slot_bitmap >> (cline_idx * 8)) & 0xff))
            {
               uint64_t cline_bit = 1ULL << cline_idx;
               // we need to clear the bit in the cachelines_with_free_slots
               auto prev =
                   cachelines_with_free_slots.fetch_xor(cline_bit, std::memory_order_relaxed);
               if (prev == cline_bit)  // it was the last bit..
               {
                  /// then we cleared the entire cline and need to report back
                  return 2;
               }
            }
            return 1;
         }
      };

      /**
       * Parallel structure to address_block, contains the header for
       * each address_block which has the bitmaps that track the free slots
       * in the address_block[region].slots
       */
      struct region_page_header_block
      {
         region_page_header page_headers[max_regions];
      };

      struct region_page
      {
         /// index this by address_idx
         std::atomic<uint16_t> slot[address_region_page_capacity];
      };

      /**
       * Each address block contains 1 page per region
       */
      struct address_block
      {
         /// index this by address.region_page()
         region_page pages[max_regions];
      };

      /**
       * Root header for the address allocator that tracks global state
       * including block allocation and region management.
       * 
       * When any region fills up its currently allocated blocks,
       * we allocate a new block for all regions to maintain alignment
       * and update max_reserved_address_blocks.
       */
      struct address_alloc_header
      {
         region_header region_headers[max_regions];  // Per-region allocation state

         std::atomic<uint16_t> next_region;  // Next region ID to allocate

         /// @brief  aka the number of 256MB blocks
         std::atomic<uint16_t> pages_per_region;  // pages allocated per each region
      };

      /// Block allocator for address blocks
      std::unique_ptr<block_allocator> _region_page_blocks;

      /// Block allocator for page headers
      constexpr static uint64_t region_page_header_block_size =
          max_regions * sizeof(region_page_header);
      std::unique_ptr<block_allocator> _region_pages_headers;

      /// Memory mapped allocator header
      std::unique_ptr<mapping> _alloc_header;
      /// Mutex to protect block allocation
      std::mutex _block_mutex;

      // Helper to get allocator header pointer
      address_alloc_header* get_alloc_header()
      {
         return static_cast<address_alloc_header*>(_alloc_header->data());
      }

      const address_alloc_header* get_alloc_header() const
      {
         return static_cast<const address_alloc_header*>(_alloc_header->data());
      }

      // Helper to get address block pointer
      address_block* get_address_block(uint16_t page_idx)
      {
         return static_cast<address_block*>(
             _region_page_blocks->get(_region_page_blocks->block_to_offset(page_idx)));
      }

      const address_block* get_address_block(uint16_t page_idx) const
      {
         return static_cast<const address_block*>(
             _region_page_blocks->get(_region_page_blocks->block_to_offset(page_idx)));
      }

      // Helper to get page header block pointer
      region_page_header_block* get_region_page_header_block(uint16_t block_idx)
      {
         return static_cast<region_page_header_block*>(
             _region_pages_headers->get(_region_pages_headers->block_to_offset(block_idx)));
      }

      const region_page_header_block* get_region_page_header_block(uint16_t block_idx) const
      {
         return static_cast<const region_page_header_block*>(
             _region_pages_headers->get(_region_pages_headers->block_to_offset(block_idx)));
      }

      // Helper to get number of allocated blocks
      uint16_t get_num_blocks() const
      {
         return get_alloc_header()->pages_per_region.load(std::memory_order_acquire);
      }

      /**
       * A region page is full when all cachelines on the page are allocated,
       * this means we need to update the region_header to reflect that the page is full
       */
      void region_page_full(region_id id, uint16_t page_block_idx)
      {
         assert(page_block_idx < 128);

         auto& region_header = get_alloc_header()->region_headers[id];
         auto  idx           = page_block_idx >= 64;
         auto& bitmap        = region_header._page_blocks_with_free_slots[idx];

         bitmap.fetch_xor(1ULL << page_block_idx % 64, std::memory_order_relaxed);
      }

      /**
       *  Given a region id, ids are grouped into pages with 512 slots each,
       *  this will tell us which of those pages have free slots or grow the
       *  pages if needed.
       */
      int first_free_page_block(region_id id)
      {
         auto& region_header = get_alloc_header()->region_headers[id];

         do
         {
            uint64_t page_blocks_bitmap =
                region_header._page_blocks_with_free_slots[0].load(std::memory_order_acquire);
            int page_blocks_bitmap_idx = 0;
            if (page_blocks_bitmap == 0)
            {
               page_blocks_bitmap =
                   region_header._page_blocks_with_free_slots[1].load(std::memory_order_acquire);
               page_blocks_bitmap_idx = 1;
            }
            if (page_blocks_bitmap == 0)
            {
               grow_page_blocks(1);
               continue;
            }
            uint16_t page_block_idx = std::countr_zero(page_blocks_bitmap);
            return page_blocks_bitmap_idx * 64 + page_block_idx;
         } while (true);
      }
      void grow_page_blocks(uint16_t num_blocks)
      {
         ensure_blocks_available(_region_page_blocks->num_blocks() + num_blocks);
      }

      // Helper to ensure enough blocks are allocated
      void ensure_blocks_available(uint16_t required_blocks);
   };  // class address_alloc
}  // namespace sal
