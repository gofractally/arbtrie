#include <sal/hash/lehmer64.h>
#include <bit>
#include <cmath>
#include <numeric>
#include <optional>
#include <sal/debug.hpp>
#include <sal/hint.hpp>
#include <sal/mapping.hpp>
#include <sal/min_index.hpp>
#include <sal/shared_ptr_alloc.hpp>

namespace sal
{
   shared_ptr_alloc::shared_ptr_alloc(const std::filesystem::path& dir) : _dir(dir)
   {
      std::filesystem::create_directories(dir);
      _page_allocator = std::make_unique<block_allocator>(
          dir / "page_allocator.bin", alloc_block_size, 2048 /* 8*2^32 / 16MB */);
      _page_table = std::make_unique<mapping>(dir / "page_table.bin", access_mode::read_write);
      if (_page_table->size() != sizeof(detail::page_table))
      {
         _page_table->resize(sizeof(detail::page_table));
         new (_page_table->data()) detail::page_table();
      }

      // Mark region 0 as maximally used so it's never selected
      // First 16 bits of the first region_use_counts element correspond to region 0
      uint64_t current = get_page_table().region_use_counts[0].load(std::memory_order_relaxed);
      uint64_t new_value =
          (current & ~0xFFFFULL) | 0xFFFFULL;  // Set region 0 count to max (0xFFFF)
      get_page_table().region_use_counts[0].store(new_value, std::memory_order_relaxed);
   }

   shared_ptr_alloc::~shared_ptr_alloc() {}

   detail::page& shared_ptr_alloc::get_or_alloc_page(detail::region&     reg,
                                                     detail::page_number pg_num)
   {
      detail::page_offset pg_idx = reg.pages[*pg_num].load(std::memory_order_acquire);
      if (pg_idx == detail::null_page)
      {
         // Need to allocate a new page - use mutex to prevent race conditions
         std::lock_guard<std::mutex> lock(_page_alloc_mutex);

         // Double-check that the page is still null after acquiring mutex
         // (another thread might have allocated it while we were waiting)
         pg_idx = reg.pages[*pg_num].load(std::memory_order_acquire);
         if (pg_idx == detail::null_page)
         {
            // Still null, we're the first thread to allocate this page
            // Let alloc_page handle all allocation logic
            pg_idx = alloc_page(pg_num);
            reg.pages[*pg_num].store(pg_idx, std::memory_order_release);
         }
      }
      return get_page(pg_idx);
   }
   detail::page_offset shared_ptr_alloc::alloc_page(detail::page_number pg_num)
   {
      // get the next page sequence
      auto np      = get_page_table()._pages_alloced.fetch_add(1, std::memory_order_acquire);
      auto nblocks = _page_allocator->num_blocks();
      if (nblocks * _page_allocator->block_size() <= np * sizeof(detail::page))
      {
         _page_allocator->reserve(
             1 + ((np + 1) * sizeof(detail::page) / _page_allocator->block_size()));
      }
      detail::page_offset offset(sizeof(detail::page) * np);
      auto                pg = new (&get_page(offset)) detail::page();
      if (not pg_num)
      {
         // Mark the first pointer as used (pointer 0)
         pg->free_ptrs[0].store(~1ULL, std::memory_order_release);

         // We also need to update the free_cachelines bitmap if this was the only pointer in that cacheline
         // First check if there are still other free pointers in the first cacheline
         // The first 8 pointers (0-7) are in cacheline 0, bit 0 of free_cachelines
         uint64_t first_cacheline_bits = pg->free_ptrs[0].load(std::memory_order_relaxed) & 0xFF;

         // If there are no other free pointers in this cacheline, update free_cachelines
         if (first_cacheline_bits == 0)
         {
            // Clear bit 0 in free_cachelines (marking cacheline 0 as having no free pointers)
            uint64_t free_cl = pg->free_cachelines.load(std::memory_order_relaxed);
            pg->free_cachelines.store(free_cl & ~1ULL, std::memory_order_release);
         }
      }
      return offset;
   }

   /**
    * This class is designed to be accessed by multiple threads, but because it is
    * lock free, it is possible the indices that direct us to free ptrs are read in
    * an inconsistent state. If this happens we will arrive at the end of the page
    * expecting to find an available ptr but not find one (another thread claimed it),
    * this isn't a problem, just try again in a loop like CAS. 
    * 
    * @throw std::runtime_error if no ptrs are available
    * @return an allocation if successful, std::nullopt if transient inconsitency detected
    */
   std::optional<allocation> shared_ptr_alloc::try_alloc(ptr_address::region_type region,
                                                         const hint&              h)
   {
      (void)h;
      auto& reg = get_page_table().regions[*region];

      uint64_t free_pages = reg.free_pages[0].load(std::memory_order_acquire);
      // SAL_INFO("free_pages[0]: {}", std::bitset<64>(free_pages));
      int free_pages_idx = 0;
      if (not free_pages)
      {
         free_pages = reg.free_pages[1].load(std::memory_order_acquire);
         // SAL_WARN("free_pages[1]: {}", std::bitset<64>(free_pages));
         free_pages_idx = 1;
         if (not free_pages)
            throw std::runtime_error("shared_ptr_alloc: no pointers available");
      }

      uint64_t first_free_page = std::countr_zero(free_pages) + 64 * free_pages_idx;
      // SAL_INFO("first_free_page: {}", first_free_page);

      // Get the page from the page table
      detail::page& pg = get_or_alloc_page(reg, detail::page_number(first_free_page));

      // Find first free cacheline by checking free_cachelines bitmap
      uint64_t free_cachelines = pg.free_cachelines.load(std::memory_order_acquire);
      if (!free_cachelines) [[unlikely]]
      {
         //         SAL_ERROR("inconsitency detected: free_cachelines: {}", std::bitset<64>(free_cachelines));
         return std::nullopt;  // inconsitency detected
      }
      // SAL_INFO("init free_cachelines: {}", std::bitset<64>(free_cachelines));
      // the cacheline claims to have free ptrs, so we need to find the first free ptr
      uint64_t first_free_cacheline = std::countr_zero(free_cachelines);

      // cacheline to address.index
      uint64_t index_in_free_ptrs = first_free_cacheline * 8 / 64;
      auto&    free_ptrs          = pg.free_ptrs[index_in_free_ptrs];
      uint64_t free_ptrs_bitmap   = free_ptrs.load(std::memory_order_acquire);

      // Keep trying until we successfully clear a set bit
      while (free_ptrs_bitmap) [[likely]]
      {
         uint64_t first_free_ptr = std::countr_zero(free_ptrs_bitmap);
         uint64_t cleared_bit    = 1ULL << first_free_ptr;

         uint64_t expected = free_ptrs_bitmap;
         uint64_t desired  = expected & ~cleared_bit;

         if (not free_ptrs.compare_exchange_strong(free_ptrs_bitmap, desired,
                                                   std::memory_order_acq_rel))
         {
            //   SAL_WARN("contention detected: free_ptrs_bitmap: {}",
            //            std::bitset<64>(free_ptrs_bitmap));
            continue;
         }
         // SAL_INFO("before free_ptrs_bitmap: {}", std::bitset<64>(free_ptrs_bitmap));
         // SAL_INFO("after free_ptrs_bitmap:  {}", std::bitset<64>(desired));

         // pointer claimed, now check to see if it was the last ptr in the cacheline
         auto cacheline_bit_idx = first_free_ptr / 8;
         auto cacheline_mask    = uint64_t(0xff) << (cacheline_bit_idx * 8);

         auto ptr_index = ptr_address::index_type(index_in_free_ptrs * 64 + first_free_ptr);
         // was this the last bit in the cacheline?  1 in 8 chance
         if ((free_ptrs_bitmap & cacheline_mask) == cleared_bit) [[unlikely]]
         {
            auto cacheline_bit = 1ULL << (*ptr_index / 8);
            // we claimed the last ptr in the cacheline, clear the bit in the cacheline
            auto prev = pg.free_cachelines.fetch_xor(cacheline_bit, std::memory_order_release);
            if (prev == cacheline_bit) [[unlikely]]  // 1 in 64
               // we allocated the last pointer in the page.
               reg.free_pages[first_free_page >= 64].fetch_xor(1ULL << (first_free_page % 64),
                                                               std::memory_order_release);
         }
         auto& ptr = pg.get_ptr(ptr_index);
         // SAL_INFO("index_in_free_ptrs: {}", index_in_free_ptrs);
         // SAL_INFO("first_free_ptr: {}", first_free_ptr);
         // SAL_INFO("ptr_index: {}", ptr_index);
         auto alloc_index = first_free_page * ptrs_per_page + *ptr_index;
         // SAL_WARN("alloc: {}", ptr_address{region, ptr_address::index_type(alloc_index)});

         // Increment the region use count
         get_page_table().inc_region(region);

         // SAL_INFO("alloc_index: {}", alloc_index);
         return allocation{ptr_address{region, ptr_address::index_type(alloc_index)}, &ptr,
                           get_page_table()._sequence.fetch_add(1, std::memory_order_relaxed)};
      }
      /// inconsitency detected, or someone else claimed the last ptr in the cacheline first
      //     SAL_ERROR("contention detected: free_ptrs_bitmap: {}", std::bitset<64>(free_ptrs_bitmap));
      return std::nullopt;
   }

   allocation shared_ptr_alloc::alloc(ptr_address::region_type region, const alloc_hint& /*ahint*/)
   {
      hint                      h     = hint::any();
      std::optional<allocation> alloc = try_alloc(region, h);
      int                       count = 0;
      while (!alloc)
      {
         if (count % 64 == 63)
            std::this_thread::yield();
         if (count % 1024 == 1023)
         {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
         }
         if (count > 1024 * 1024)
            throw std::runtime_error(
                "shared_ptr_alloc: *contention* no pointers available after "
                "1 mega attempts");
         alloc = try_alloc(region, h);
         count++;
      }
      if (count > 1024)
         SAL_WARN("shared_ptr_alloc: *contention* took {} attempts", count);
      // Sequence number is now assigned in try_alloc
      return *alloc;
   }

   /// @pre address is a valid pointer address
   void shared_ptr_alloc::free(ptr_address address)
   {
      //SAL_WARN("free: {}", address);
      auto& reg      = get_page_table().regions[*address.region];
      auto  page_idx = address_index_to_page(address.index);
      auto& pg       = get_page(reg.get_page_offset(page_idx));
      auto& ptr      = pg.get_ptr(address.index);

      auto index_on_page = *address.index % ptrs_per_page;
      auto free_slot_idx = index_on_page / 64;
      // SAL_WARN("free_slot_idx: {}", free_slot_idx);
      auto free_slot_bit = 1ULL << (index_on_page % 64);
      // SAL_WARN("free_slot_bit:  {}", std::bitset<64>(free_slot_bit));

      auto free_slot_cacheline = index_on_page / 8;
      // SAL_WARN("free_slot_cacheline: {}", free_slot_cacheline);
      auto cacheline_in_slot_idx = free_slot_cacheline % 8;
      // SAL_WARN("cacheline_in_slot_idx: {}", cacheline_in_slot_idx);
      auto cacheline_mask = uint64_t(0xff) << (cacheline_in_slot_idx * 8);
      // SAL_WARN("cacheline_mask: {}", std::bitset<64>(cacheline_mask));

      // assert the ptr is not already marked as free
      // SAL_WARN("free_slot_idx: {}", free_slot_idx);
      // SAL_WARN("free_slot_bit: {}", std::bitset<64>(free_slot_bit));
      // SAL_WARN("free_ptrs[{}]:   {}", free_slot_idx, std::bitset<64>(pg.free_ptrs[free_slot_idx]));

      // Check if the bit is already marked as free
      uint64_t current_bits = pg.free_ptrs[free_slot_idx].load(std::memory_order_acquire);
      if (current_bits & free_slot_bit)
      {
         // Print detailed error with new lines for better readability
         SAL_ERROR(
             "DOUBLE FREE DETECTED: {}\n"
             "  - Bit already marked as free!\n"
             "  - free_slot_idx: {}\n"
             "  - free_slot_bit: {}\n"
             "  - current_bitmap: {}\n"
             "  - bits_position: {}\n"
             "  - region: {}, page: {}, index_on_page: {}",
             address, free_slot_idx, std::bitset<64>(free_slot_bit), std::bitset<64>(current_bits),
             index_on_page % 64, *address.region, *page_idx, index_on_page);

         abort();
      }

      assert(not(current_bits & free_slot_bit));

      ptr.~shared_ptr();  /// reset it, make it ready to be allocated again

      // Decrement the region use count
      get_page_table().dec_region(address.region);

      // mark the ptr as free, we are the sole owner of this bit so fetch_add is safe
      auto prev = pg.free_ptrs[free_slot_idx].fetch_add(free_slot_bit, std::memory_order_release);

      // SAL_WARN("prev:              {}", std::bitset<64>(prev));
      // SAL_WARN("free_slot_bit: +   {}", std::bitset<64>(free_slot_bit));
      // SAL_WARN("pg.free_ptrs[{}]:   {}", free_slot_idx,
      //          std::bitset<64>(pg.free_ptrs[free_slot_idx]));
      // assert the bit was not already set, race condition of two people freeing the same ptr
      assert(not(prev & free_slot_bit));

      // SAL_WARN("cacheline_mask:   {}", std::bitset<64>(cacheline_mask));
      if (prev & cacheline_mask) [[likely]]  // 7 in 8
         return;                             // we were not the first to be freed in cacheline

      // we are the first ptr in the page to be freed, set the bit in the page
      uint64_t free_slot_cl_bit = 1ULL << free_slot_cacheline;  // 63 in 64 chance
      // SAL_WARN("free_slot_cl_bit: {}", std::bitset<64>(free_slot_cl_bit));
      // SAL_WARN("free_cl before  : {}", std::bitset<64>(pg.free_cachelines.load()));

      if (pg.free_cachelines.fetch_xor(free_slot_cl_bit, std::memory_order_release)) [[likely]]
      {
         // SAL_WARN("free_cl after   : {}", std::bitset<64>(pg.free_cachelines.load()));
         return;  // we are not the first cacheline with free bit
      }
      // SAL_WARN("free_cl after   : {}", std::bitset<64>(pg.free_cachelines.load()));

      int   reg_free_pages_idx    = *page_idx >= 64;
      auto& fp_idx                = reg.free_pages[reg_free_pages_idx];
      int   bit_in_free_pages_idx = *page_idx % 64;

      // SAL_WARN("fp_idx before: {}", std::bitset<64>(fp_idx.load()));
      fp_idx.fetch_xor(1ULL << bit_in_free_pages_idx, std::memory_order_release);
      // SAL_WARN("fp_idx after: {}", std::bitset<64>(fp_idx.load()));
   }

   /**
    * A suggestion for a region when you don't care which region 
    * you are allocated in, attempts provide a region that isn't
    * already over crowded by utilizing a random sample of 32
    * regions and selecting the least used one.
    */
   ptr_address::region_type shared_ptr_alloc::get_new_region()
   {
      // Get a pointer to region_use_counts aligned at 128 bytes
      const uint16_t* region_counts =
          reinterpret_cast<const uint16_t*>(get_page_table().region_use_counts.data());

      // Get random index aligned to 32 bytes using thread-local RNG
      // by using thread-local rng we avoid contention on the global RNG and ensure
      // that different threads will tend to be looking at different regions and thereby
      // reducing contention.
      static thread_local lehmer64_rng rng(
          std::chrono::steady_clock::now().time_since_epoch().count());
      uint64_t aligned_index     = (rng.next() & 0xFFFF) & ~31ULL;  // Mask to 2^16 and align to 32
      const uint16_t* region_ptr = region_counts + aligned_index;

      int min_index = find_min_index_32(region_ptr);

      auto result = ptr_address::region_type(aligned_index + min_index);

      // In debug mode, throw if we somehow got region 0 - should never happen
      if constexpr (debug_memory)
      {
         if (*result == 0)
            throw std::runtime_error("get_new_region returned region 0, which is reserved");
      }

      return result;
   }

   void shared_ptr_alloc::clear_all()
   {
      // Reset free_pages bitmaps
      for (auto& region : get_page_table().regions)
         region.free_pages[0].store(~0ULL, std::memory_order_relaxed);
      for (auto& region : get_page_table().regions)
         region.free_pages[1].store(~0ULL, std::memory_order_relaxed);

      // Reset all page table entries to null_page
      for (auto& region : get_page_table().regions)
      {
         for (uint32_t page_idx = 0; page_idx < pages_per_region; ++page_idx)
         {
            region.pages[page_idx].store(detail::null_page, std::memory_order_relaxed);
         }
      }

      // Reset page table to initial state
      get_page_table()._pages_alloced.store(0, std::memory_order_relaxed);

      // Reset region use counts to 0, except region 0 which stays at max
      for (size_t i = 1; i < get_page_table().region_use_counts.size(); ++i)
         get_page_table().region_use_counts[i].store(0, std::memory_order_relaxed);

      // Ensure region 0 is at max count
      uint64_t current   = get_page_table().region_use_counts[0].load(std::memory_order_relaxed);
      uint64_t new_value = (current & ~0xFFFFULL) | 0xFFFFULL;
      get_page_table().region_use_counts[0].store(new_value, std::memory_order_relaxed);

      // Reset block allocator to start fresh
      _page_allocator->truncate(0);
   }

   /**
    * Resets all reference counters to 1 if they are currently in use (ref count >= 1),
    * and frees any pointers with a reference count of 0.
    * This is useful during recovery to ensure all shared_ptrs start with a clean state.
    */
   void shared_ptr_alloc::reset_all_refs()
   {
      // Add debug counters
      int total_pointers_processed = 0;
      int pointers_reset           = 0;
      int pointers_freed           = 0;

      // Iterate through all regions
      for (uint32_t region_idx = 0; region_idx < (1 << 16); ++region_idx)
      {
         auto& region = get_page_table().regions[region_idx];

         // For each region, check all pages - don't skip any
         for (uint32_t page_offset = 0; page_offset < pages_per_region; ++page_offset)
         {
            // Get the page offset from the region's page table
            auto page_idx  = detail::page_number(page_offset);
            auto pg_offset = region.pages[*page_idx].load(std::memory_order_relaxed);

            // Skip if page hasn't been allocated yet
            if (pg_offset == detail::null_page)
               continue;

            // Get the page and process it
            auto& page = get_page(pg_offset);

            // Process each slot in the page
            for (uint32_t slot_idx = 0; slot_idx < ptrs_per_page / 64; ++slot_idx)
            {
               // Get the bitmap of free pointers in this slot
               uint64_t free_ptrs_bitmap = page.free_ptrs[slot_idx].load(std::memory_order_relaxed);

               // Start from bit 1 for slot 0, bit 0 for other slots
               // slot_idx == 0 evaluates to true (1) for the first slot
               uint32_t start_bit = (slot_idx == 0);

               // Iterate through all bits in the bitmap
               for (uint32_t bit_idx = start_bit; bit_idx < 64; ++bit_idx)
               {
                  // If the bit is NOT set, the pointer is allocated
                  if (!(free_ptrs_bitmap & (1ULL << bit_idx)))
                  {
                     // Calculate the pointer index
                     auto ptr_idx = ptr_address::index_type(slot_idx * 64 + bit_idx);

                     // Get reference to the shared_ptr
                     auto& ptr = page.get_ptr(ptr_idx);

                     // Check its reference count
                     auto ref_count = ptr.use_count();

                     total_pointers_processed++;

                     // Since we start at bit 1 for slot 0, we never encounter null pointers
                     // (where loc.cacheline() would be 0)
                     if (ref_count > 1)
                     {
                        // Reset reference count to 1 while preserving location
                        ptr.set_ref(1, std::memory_order_relaxed);
                        pointers_reset++;
                     }
                     else if (ref_count == 0)
                     {
                        // For pointers with ref count 0, free them
                        ptr_address addr(ptr_address::region_type(region_idx), ptr_idx);
                        free(addr);
                        pointers_freed++;
                     }
                  }
               }
            }
         }
      }

      SAL_INFO(
          "Reset all shared_ptr reference counts to 1 for used pointers, freed unused pointers. "
          "Processed: {}, Reset: {}, Freed: {}",
          total_pointers_processed, pointers_reset, pointers_freed);
   }

   /**
    * Frees pointers with reference count of 0 or 1 (unreachable pointers).
    * Called after reset_all_refs() and recursive retain to free any pointers
    * that weren't reachable from the root.
    */
   void shared_ptr_alloc::release_unreachable()
   {
      // First, reset region use counts - start at 1 to preserve region 0
      for (uint32_t region_idx = 1; region_idx < (1 << 16); ++region_idx)
      {
         // Reset region use count to 0
         auto idx = region_idx / 4;
         auto bit = region_idx % 4;

         // Clear only this region's bits, preserving others
         uint64_t mask    = ~(0xFFFFULL << (bit * 16));
         uint64_t current = get_page_table().region_use_counts[idx].load(std::memory_order_relaxed);
         get_page_table().region_use_counts[idx].store(current & mask, std::memory_order_relaxed);
      }

      // Make sure region 0 is set to max
      uint64_t current   = get_page_table().region_use_counts[0].load(std::memory_order_relaxed);
      uint64_t new_value = (current & ~0xFFFFULL) | 0xFFFFULL;
      get_page_table().region_use_counts[0].store(new_value, std::memory_order_relaxed);

      // Track count of freed pointers
      int pointers_freed = 0;

      // Iterate through existing pages to minimize overhead - skip region 0
      for (uint32_t region_idx = 1; region_idx < (1 << 16); ++region_idx)
      {
         auto& region = get_page_table().regions[region_idx];

         // Process each page in this region
         for (uint32_t page_offset = 0; page_offset < pages_per_region; ++page_offset)
         {
            // Get the page offset
            auto page_idx  = detail::page_number(page_offset);
            auto pg_offset = region.pages[*page_idx].load(std::memory_order_relaxed);

            // Skip unallocated pages
            if (pg_offset == detail::null_page)
               continue;

            // Process each used pointer in this page
            auto& page = get_page(pg_offset);

            // Process slots in the page
            for (uint32_t slot_idx = 0; slot_idx < ptrs_per_page / 64; ++slot_idx)
            {
               // Load free bitmap once for the entire slot
               uint64_t free_ptrs_bitmap = page.free_ptrs[slot_idx].load(std::memory_order_relaxed);

               // Set the start bit (skip bit 0 of slot 0)
               uint32_t start_bit = (slot_idx == 0) ? 1 : 0;

               // Examine each allocated pointer
               for (uint32_t bit_idx = start_bit; bit_idx < 64; ++bit_idx)
               {
                  // Skip free pointers (bit is set)
                  if (free_ptrs_bitmap & (1ULL << bit_idx))
                     continue;

                  // Calculate the pointer index
                  auto ptr_idx = ptr_address::index_type(slot_idx * 64 + bit_idx);

                  // Get the shared_ptr reference
                  auto& ptr = page.get_ptr(ptr_idx);

                  // Get the ref count
                  auto ref_count = ptr.use_count();

                  // If ref count is 0 or 1, free it
                  if (ref_count <= 1)
                  {
                     ptr_address addr(ptr_address::region_type(region_idx), ptr_idx);
                     free(addr);
                     pointers_freed++;
                  }
                  else
                  {
                     // If ref count > 1, decrement it by 1
                     ptr.release();

                     // Increment region use count for this pointer since we're keeping it
                     // This is needed to restore the region use counts we reset at the beginning
                     get_page_table().inc_region(ptr_address::region_type(region_idx));
                  }
               }
            }
         }
      }

      SAL_INFO("Released unreachable shared_ptr objects. Freed: {}", pointers_freed);
   }

   /**
    * Returns the total number of used pointers across all regions by summing
    * the region use counts. Each entry in the region_use_counts array contains
    * 4 separate 16-bit counters packed into a 64-bit value.
    */
   uint64_t shared_ptr_alloc::used() const
   {
      uint64_t total_used = 0;

      // Iterate through all regions use count entries
      for (size_t i = 0; i < get_page_table().region_use_counts.size(); ++i)
      {
         uint64_t packed_counts =
             get_page_table().region_use_counts[i].load(std::memory_order_relaxed);

         // Extract the 4 separate 16-bit counts from the packed 64-bit value
         uint16_t count0 = packed_counts & 0xFFFF;
         uint16_t count1 = (packed_counts >> 16) & 0xFFFF;
         uint16_t count2 = (packed_counts >> 32) & 0xFFFF;
         uint16_t count3 = (packed_counts >> 48) & 0xFFFF;

         // Add all 4 counts to the total
         total_used += count0 + count1 + count2 + count3;
      }

      // Subtract region 0's count (which is always set to maximum)
      total_used -= 0xFFFF;

      return total_used;
   }

   shared_ptr_alloc::region_stats_t shared_ptr_alloc::region_stats() const
   {
      region_stats_t        stats = {UINT16_MAX, 0, 0.0, 0.0, 0};
      std::vector<uint16_t> counts;

      // Reserve space for all possible regions (2^16)
      // Each region is a 16-bit value, so total memory is only 128KB
      counts.reserve(1ULL << 16);

      // First pass: collect all non-zero region counts
      for (size_t i = 0; i < get_page_table().region_use_counts.size(); ++i)
      {
         uint64_t packed_counts =
             get_page_table().region_use_counts[i].load(std::memory_order_relaxed);

         // Process each of the 4 regions in this packed value
         uint16_t counts_array[4] = {static_cast<uint16_t>(packed_counts & 0xFFFF),
                                     static_cast<uint16_t>((packed_counts >> 16) & 0xFFFF),
                                     static_cast<uint16_t>((packed_counts >> 32) & 0xFFFF),
                                     static_cast<uint16_t>((packed_counts >> 48) & 0xFFFF)};

         for (int j = 0; j < 4; ++j)
         {
            if (counts_array[j] > 0)
            {
               // Update min and max
               stats.min = std::min(stats.min, counts_array[j]);
               stats.max = std::max(stats.max, counts_array[j]);

               // Add to our collection for mean and stddev calculation
               counts.push_back(counts_array[j]);
            }
         }
      }

      // Update count of non-empty regions
      stats.count = counts.size();

      // If no regions have pointers, return early with defaults
      if (stats.count == 0)
      {
         stats.min = 0;
         return stats;
      }

      // Calculate mean using std::accumulate
      uint64_t sum = std::accumulate(counts.begin(), counts.end(), 0ULL);
      stats.mean   = static_cast<double>(sum) / stats.count;

      // Calculate standard deviation
      double sum_of_squares = 0.0;
      for (auto count : counts)
      {
         double diff = count - stats.mean;
         sum_of_squares += diff * diff;
      }
      stats.stddev = sqrt(sum_of_squares / stats.count);

      return stats;
   }

   /**
    * @brief Clears active bits for pointers in the specified range of regions
    * @param start_region The region to start clearing from
    * @param num_regions The number of regions to process
    */
   void shared_ptr_alloc::clear_active_bits(ptr_address::region_type start_region,
                                            uint32_t                 num_regions)
   {
      // Calculate the end region, bounded by max_regions
      const auto end_region = std::min<uint32_t>(*start_region + num_regions, max_regions);

      // Iterate through the specified regions
      for (uint32_t region_idx = *start_region; region_idx < end_region; ++region_idx)
      {
         auto& region = get_page_table().regions[region_idx];

         // For each region, iterate through all its pages
         for (uint32_t page_offset = 0; page_offset < pages_per_region; ++page_offset)
         {
            // Get the page offset from the region's page table
            auto page_idx  = detail::page_number(page_offset);
            auto pg_offset = region.pages[*page_idx].load(std::memory_order_relaxed);

            // Skip if page hasn't been allocated yet
            if (pg_offset == detail::null_page)
               continue;

            // Get the page and process it
            auto& page = get_page(pg_offset);

            // Process each slot in the page
            for (uint32_t slot_idx = 0; slot_idx < ptrs_per_page / 64; ++slot_idx)
            {
               // Get the bitmap of free pointers in this slot
               uint64_t free_ptrs_bitmap = page.free_ptrs[slot_idx].load(std::memory_order_relaxed);

               // Start from bit 1 for slot 0, bit 0 for other slots (skip null pointer)
               uint32_t start_bit = (slot_idx == 0);

               // Iterate through all bits in the bitmap
               for (uint32_t bit_idx = start_bit; bit_idx < 64; ++bit_idx)
               {
                  // If the bit is NOT set, the pointer is allocated
                  if (!(free_ptrs_bitmap & (1ULL << bit_idx)))
                  {
                     // Calculate the pointer index
                     auto ptr_idx = ptr_address::index_type(slot_idx * 64 + bit_idx);

                     // Get reference to the shared_ptr
                     auto& ptr = page.get_ptr(ptr_idx);

                     // Clear active bit by clearing pending_cache flag
                     if (ptr.pending_cache())
                     {
                        // Use the existing clear_pending_cache() method
                        ptr.clear_pending_cache();
                     }
                  }
               }
            }
         }
      }
   }

   /**
    * Returns a reference to a shared_ptr at the specified address, allocating it if needed.
    * This is primarily used for recovery operations where we need to ensure pointers
    * exist at specific addresses.
    */
   shared_ptr& shared_ptr_alloc::get_or_alloc(ptr_address address)
   {
      // First check if the pointer already exists
      shared_ptr* existing_ptr = try_get(address);
      if (existing_ptr != nullptr)
      {
         return *existing_ptr;
      }

      // If we need to allocate, first check if we need to create the page
      auto& region = get_page_table().get_region(address.region);

      // Calculate page number from the address index
      detail::page_number page_num = address_index_to_page(address.index);

      // Get or allocate the page
      detail::page& page = get_or_alloc_page(region, page_num);

      // Calculate slot and bit indices
      uint32_t slot_idx = *address.index / 64;
      uint32_t bit_idx  = *address.index % 64;

      // Attempt to mark the bit as allocated (clear the bit in the free bitmap)
      uint64_t expected_bitmap = page.free_ptrs[slot_idx].load(std::memory_order_relaxed);
      uint64_t desired_bitmap;

      do
      {
         // If the bit is already cleared, the pointer is already allocated
         if (!(expected_bitmap & (1ULL << bit_idx)))
         {
            // Pointer is already allocated, just return it
            return page.get_ptr(address.index);
         }

         // Clear the bit to mark it as allocated
         desired_bitmap = expected_bitmap & ~(1ULL << bit_idx);

      } while (!page.free_ptrs[slot_idx].compare_exchange_weak(
          expected_bitmap, desired_bitmap, std::memory_order_release, std::memory_order_relaxed));

      // Update half-free and free cachelines bits as needed
      // Similar to the logic in the try_alloc method

      // For simplicity, we also update the bitmap of free cachelines
      if (std::popcount(desired_bitmap) <= 4)
      {
         // This cacheline is now half-free or less
         page.half_free_cachelines.fetch_or(1ULL << slot_idx, std::memory_order_relaxed);
      }

      if (desired_bitmap == 0)
      {
         // No more free pointers in this cacheline
         page.free_cachelines.fetch_and(~(1ULL << slot_idx), std::memory_order_relaxed);
      }

      // Increment the region use count
      get_page_table().inc_region(address.region);

      // Return a reference to the now allocated pointer
      shared_ptr& result = page.get_ptr(address.index);

      // Initialize the shared_ptr to have a zero reference count
      result.reset(sal::location(), 0);

      return result;
   }
}  // namespace sal
