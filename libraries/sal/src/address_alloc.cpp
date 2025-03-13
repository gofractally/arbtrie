#include <cstring>
#include <sal/address_alloc.hpp>
#include <sal/debug.hpp>

namespace sal
{
   address_alloc::address_alloc(const std::filesystem::path& file)
   {
      // Create directories first
      std::filesystem::create_directories(file);
      SAL_WARN("address_alloc constructor {} called, address_block_size: {}", file.native(),
               region_block_size);

      // Initialize block allocators after directories are created
      _region_page_blocks =
          std::make_unique<block_allocator>(file / "address_blocks", region_block_size /*256MB*/,
                                            max_region_blocks /*128*/, true);  // read_write=true

      SAL_WARN("address_alloc constructor {} called, page_header_block_size: {}", file.native(),
               region_page_header_block_size);

      _region_pages_headers = std::make_unique<block_allocator>(
          file / "page_headers", region_page_header_block_size /*1MB*/, max_region_blocks /*128*/,
          true);  // read_write=true

      _alloc_header = std::make_unique<mapping>(file / "alloc_header", access_mode::read_write);

      // Initialize header if this is a newly mapped file
      if (_alloc_header->size() != sizeof(address_alloc_header))
      {
         // Ensure allocator header is properly sized
         _alloc_header->resize(sizeof(address_alloc_header));

         auto* header = get_alloc_header();
         new (header) address_alloc_header();

         // Ensure we have at least one block allocated
         ensure_blocks_available(1);
      }
   }

   address_alloc::~address_alloc() = default;

   region_id address_alloc::get_new_region()
   {
      return get_alloc_header()->next_region.fetch_add(1, std::memory_order_relaxed);
   }

   uint64_t address_alloc::count() const
   {
      uint64_t total = 0;

      // Get number of blocks allocated
      uint16_t num_blocks = get_num_blocks();

      for (uint16_t block = 0; block < num_blocks; ++block)
      {
         const auto* page_header_block = get_region_page_header_block(block);
         for (uint32_t region = 0; region < max_regions; ++region)
         {
            const auto& page_header = page_header_block->page_headers[region];
            for (uint16_t cacheline = 0; cacheline < cachelines_per_region_page; ++cacheline)
            {
               uint64_t free_bitmap =
                   page_header.free_slots[cacheline].load(std::memory_order_relaxed);
               uint64_t allocated_count = std::popcount(~free_bitmap);
               total += allocated_count;
            }
         }
      }
      return total;
   }

   address_allocation address_alloc::get_new_address(region_id id)
   {
      constexpr uint16_t max_retries = 100;  // Prevent infinite loops

      for (uint16_t retry_count = 0; retry_count < max_retries; ++retry_count)
      {
         // Get region header to check page blocks with free slots
         //auto& region_header = get_alloc_header()->region_headers[id];

         // Find the first page block with free slots (bit = 1)
         uint16_t page_block_idx = first_free_page_block(id);

         // Get the page header block for this region
         auto* page_header_block  = get_region_page_header_block(page_block_idx);
         auto& page_header        = page_header_block->page_headers[id];
         auto  first_free_cline64 = page_header.first_free_cacheline();
         if (first_free_cline64 == 64)
            continue;

         int first_free_slot512 = page_header.first_free_slot(first_free_cline64);

         if (first_free_slot512 == 512)
            continue;

         switch (page_header.try_alloc_slot(first_free_cline64, first_free_slot512))
         {
            case 0:  // slot was already taken or inconsitency detected
               SAL_WARN("slot was already taken or inconsitency detected: {} page_block_idx: {}",
                        id, page_block_idx);
               continue;
            case 2:  // last slot in the cacheline was allocated
               region_page_full(id, page_block_idx);
            case 1:  // slot was allocated
               break;
         }

         // Create the address - page_block_idx selects the 512-slot page,
         // abs_slot_idx selects the specific slot within that page
         address new_addr;
         new_addr.region = id;
         new_addr.index  = page_block_idx * 512 + first_free_slot512;

         // Get reference to the slot only after successful bitmap allocation
         auto& slot_ref = get_slot(new_addr);

         return {new_addr, slot_ref};
      }

      // If we reach here, we've exceeded the maximum number of retries
      throw std::runtime_error("Failed to allocate address after maximum retry attempts");
   }

   void address_alloc::free_address(address addr)
   {
      // Calculate all indices directly
      const uint16_t page_idx = addr.region_page();

      // Cache references to each level of the tree structure
      auto* page_header_block = get_region_page_header_block(page_idx);
      auto& page_header       = page_header_block->page_headers[addr.region];
      auto& slot_ref          = get_slot(addr);
      auto& region_header     = get_alloc_header()->region_headers[addr.region];

      // Read the original allocation location from the slot value
      uint16_t cacheline_idx = addr.cacheline_idx();

      // Mark the slot value as 0 (indicating it's not in use)
      slot_ref.store(0, std::memory_order_relaxed);

      uint64_t slot_bit = 1ULL << addr.index_in_cacheline();
      // This is what actually makes the slot available for allocation:
      // Set the corresponding bit to 1 (free) in the free_slots bitmap using fetch_add
      uint64_t prev_slots =
          page_header.free_slots[cacheline_idx].fetch_add(slot_bit, std::memory_order_release);

      // Assert that the bit was not already set (slot was not already free)
      if (prev_slots & slot_bit)
      {
         SAL_ERROR("Double free detected: free_address({})", addr);
         abort();
      }

      // Check if the cacheline was empty and is now non-empty
      // Use same bit-checking logic as in try_alloc_slot,
      // 1 means free slots, if it was 0 before then we went from 0 to 1
      bool cacheline_now_has_free_slots = ((prev_slots >> (cacheline_idx * 8)) & 0xff) == 0;

      if (cacheline_now_has_free_slots)
      {
         uint64_t prev_cachelines_bitmap = 0;
         // we need to xor the cacheline_mask to the cachelines_with_free_slots
         prev_cachelines_bitmap = page_header.cachelines_with_free_slots.fetch_xor(
             1ULL << cacheline_idx, std::memory_order_relaxed);
         if (not prev_cachelines_bitmap)
         {
            // we need to clear the bit in the region_header
            region_header._page_blocks_with_free_slots[page_idx >= 64].fetch_xor(
                1ULL << (page_idx % 64), std::memory_order_release);
         }
      }
   }

   void address_alloc::ensure_blocks_available(uint16_t required_blocks)
   {
      std::lock_guard lock(_block_mutex);  // Lock the mutex for the duration of this method

      // TEMP: Limit block growth during debugging
      assert(required_blocks <= 6 && "Temporary limit: max 4 blocks (1GB) during debugging");

      auto* header = get_alloc_header();

      // Use CAS loop to update max_reserved_address_blocks since other threads
      // may be reading from old regions during growth
      uint16_t current_blocks;
      do
      {
         current_blocks = header->pages_per_region.load(std::memory_order_acquire);
         if (current_blocks >= required_blocks)
         {
            sal_debug("Already have enough blocks ({} >= {})", current_blocks, required_blocks);
            return;  // Early return if we already have enough blocks
         }

         // Log warning about growth - each block is 256MB
         SAL_WARN("Growing address blocks from {} to {} ({} MB -> {} MB)", current_blocks,
                  required_blocks, current_blocks * 256, required_blocks * 256);

      } while (!header->pages_per_region.compare_exchange_weak(
          current_blocks, required_blocks, std::memory_order_acq_rel, std::memory_order_acquire));

      sal_debug("Successfully updated pages_per_region to {}", required_blocks);

      // Successfully updated, now actually allocate the blocks
      // Call reserve and use its return value to ensure we have the correct number of blocks
      uint32_t actual_blocks = _region_page_blocks->reserve(required_blocks);
      _region_pages_headers->reserve(required_blocks);
      sal_debug("Reserved {} blocks in block allocators", actual_blocks);

      // Initialize the new blocks and their corresponding headers
      for (uint16_t block = current_blocks; block < actual_blocks; ++block)
      {
         sal_debug("Initializing block {}", block);

         // Get and initialize the new page block
         void* page_block = _region_page_blocks->get(_region_page_blocks->block_to_offset(block));
         if (!page_block)
            throw std::runtime_error("Failed to get page block");
         std::memset(page_block, 0, region_block_size);

         // Get and initialize the new header block
         void* header_block =
             _region_pages_headers->get(_region_pages_headers->block_to_offset(block));
         if (!header_block)
            throw std::runtime_error("Failed to get header block");

         // Initialize the header block
         auto* page_header_block = static_cast<region_page_header_block*>(header_block);
         for (uint32_t region = 0; region < max_regions; ++region)
         {
            auto& page_header = page_header_block->page_headers[region];

            // Initialize all slots as free
            for (uint16_t cacheline = 0; cacheline < cachelines_per_region_page; ++cacheline)
               page_header.free_slots[cacheline].store(~0ULL, std::memory_order_relaxed);

            // Mark all cachelines as having free slots
            page_header.cachelines_with_free_slots.store(~0ULL, std::memory_order_relaxed);
         }

         // Update region headers to indicate this block has free slots
         for (uint32_t region = 0; region < max_regions; ++region)
         {
            const uint64_t page_block_mask = 1ULL << (block % 64);
            header->region_headers[region]._page_blocks_with_free_slots[block >= 64].fetch_or(
                page_block_mask, std::memory_order_release);
         }
         sal_debug("Block {} fully initialized and marked as free in all regions", block);
      }

      // Update max_reserved_address_blocks to match actual allocation
      header->pages_per_region.store(actual_blocks, std::memory_order_release);
      sal_debug("Final pages_per_region = {}", actual_blocks);
   }
}  // namespace sal