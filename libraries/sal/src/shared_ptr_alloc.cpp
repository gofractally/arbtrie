#include <sal/hash/lehmer64.h>
#include <bit>
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
            pg_idx = alloc_page(pg_num);
            reg.pages[*pg_num].store(pg_idx, std::memory_order_release);
            SAL_INFO("Allocated new page {} at offset {}", *pg_num, pg_idx);
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
         _page_allocator->reserve(
             1 + ((nblocks + 1) * sizeof(detail::page) / _page_allocator->block_size()));
      detail::page_offset offset(sizeof(detail::page) * np);
      auto                pg = new (&get_page(offset)) detail::page();
      if (not pg_num)
         pg->free_ptrs[0].store(~1ULL, std::memory_order_release);
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
         return allocation{ptr_address{region, ptr_address::index_type(alloc_index)}, &ptr};
      }
      /// inconsitency detected, or someone else claimed the last ptr in the cacheline first
      //     SAL_ERROR("contention detected: free_ptrs_bitmap: {}", std::bitset<64>(free_ptrs_bitmap));
      return std::nullopt;
   }

   allocation shared_ptr_alloc::alloc(ptr_address::region_type region,
                                      ptr_address::index_type* /*index_hints*/,
                                      uint16_t /*hint_count*/)
   {
      hint                      h     = hint::any();
      std::optional<allocation> alloc = try_alloc(region, h);
      int                       count = 0;
      while (!alloc)
      {
         if (count % 64 == 63)
            std::this_thread::yield();
         if (count % 1024 == 1023)
            SAL_WARN("shared_ptr_alloc: *contention* no pointers available after {} attempts",
                     count);
         if (count > 1024 * 1024)
            throw std::runtime_error(
                "shared_ptr_alloc: *contention* no pointers available after "
                "1 mega attempt");
         alloc = try_alloc(region, h);
         count++;
      }
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
   ptr_address::region_type shared_ptr_alloc::next_region()
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

      return ptr_address::region_type(aligned_index + min_index);
   }
}  // namespace sal
