#include <sal/shared_ptr_alloc.hpp>

namespace sal
{
   shared_ptr_alloc::shared_ptr_alloc(const std::filesystem::path& dir) : _dir(dir)
   {
      std::filesystem::create_directories(dir);
      _page_allocator = std::make_unique<block_allocator>(
          dir / "page_allocator.bin", alloc_block_size, 2048 /* 8*2^32 / 16MB */);
      _page_table = std::make_unique<mapping>(dir / "page_table.bin");
      if (_page_table->size() != sizeof(page_table))
      {
         _page_table->resize(sizeof(page_table));
         new (_page_table->data()) page_table();
      }
   }

   shared_ptr_alloc::~shared_ptr_alloc() {}

   page& shared_ptr_alloc::get_page(page_index pidx)
   {
      return *_page_allocator->get<page>(page_index);
   }
   page& shared_ptr_alloc::get_or_alloc_page(region& reg, int pg_num)
   {
      auto pg_idx = reg.pages[pg_num].load(std::memory_order_acquire);
      if (pg_idx == null_page)
      {
         pg_idx = alloc_page();
         reg.pages[pg_num].store(pg_idx, std::memory_order_release);
      }
      return get_page(pg_idx);
   }
   page_index shared_ptr_alloc::alloc_page()
   {
      // get the next page sequence
      auto np      = get_page_table()._pages_alloced.fetch_add(1, std::memory_order_relaxed);
      auto nblocks = _page_allocator->num_blocks();
      if (nblocks * <= np)
         _page_allocator->grow(nblocks + 1);
      return sizeof(page) * np;
   }

   struct hint
   {
      hint() { memset((char*)this, 0, sizeof(*this)); }
      uint64_t pages[2];
      uint64_t cachelines[128];
   };

   template <bool indicies_contain_zero = false>
   void calculate_hint(hint& h, address_index* indices, uint16_t hint_count)
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
   std::optional<allocation> shared_ptr_alloc::try_alloc(uint16_t region, const hint& h)
   {
      auto& reg = get_page_table().regions[region];

      int      idx        = reg.free_pages[0].load(std::memory_order_acquire) == 0;
      uint64_t free_pages = reg.free_pages[idx].load(std::memory_order_acquire);

      if (not free_pages) [[unlikely]]
         throw std::runtime_error("shared_ptr_alloc: no pointers available");

      uint64_t first_free_page = std::countr_zero(free_pages) + 64 * idx;

      // Get the page from the page table
      page& pg = get_or_alloc_page(reg, first_free_page);

      // Find first free cacheline by checking free_cachelines bitmap
      uint64_t free_cachelines = pg.free_cachelines.load(std::memory_order_acquire);
      if (!free_cachelines) [[unlikely]]
         return std::nullopt;  // inconsitency detected

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
            continue;  /// look for next bit

         // pointer claimed, now check to see if it was the last ptr in the cacheline
         auto cacheline_bit_idx = first_free_ptr / 8;
         auto cacheline_mask    = uint64_t(0xff) << cacheline_bit_idx;

         // was this the last bit in the cacheline?  1 in 8 chance
         if ((free_ptrs_bitmap & cacheline_mask) == cleared_bit) [[unlikely]]
         {
            auto cacheline_bit = 1ULL << cacheline_bit_idx;
            // we claimed the last ptr in the cacheline, clear the bit in the cacheline
            auto prev = page.free_cachelines.fetch_xor(cacheline_bit, std::memory_order_release);
            if (prev == ~cacheline_bit) [[unlikely]]  // 1 in 64
               // we allocated the last pointer in the page.
               reg.free_pages[first_free_page >= 64].fetch_xor(1ULL << (first_free_page % 64),
                                                               std::memory_order_release);
         }

         auto& ptr = page.ptrs[first_free_ptr];
         return allocation{ptr_address{region, first_free_page}, &ptr};
      }
      /// inconsitency detected, or someone else claimed the last ptr in the cacheline first
      return std::nullopt;
   }

   allocation shared_ptr_alloc::alloc(uint16_t region, address_index* hint, uint16_t hint_count)
   {
      hint h;
      if (hint)
         calculate_hint(h, hint, hint_count);
      else
         memset((char*)&h, 0xff, sizeof(h));

      std::optional<allocation> alloc = try_alloc(region, h);
      while (!alloc)
         alloc = try_alloc(region, h);
      return *alloc;
   }

   /// @pre address is a valid pointer address
   void shared_ptr_alloc::free(ptr_address address)
   {
      auto& reg      = get_page_table().regions[address.region];
      auto  page_idx = address_index_to_page(address.index);
      auto& pg       = get_page(reg.pages[page_idx]);
      auto& ptr      = pg.ptrs[ptr_index_on_page(address.index)];

      auto free_slot_idx         = address.index / 64;
      auto free_slot_bit         = 1ULL << (address.index % 64);
      auto free_slot_cacheline   = address.index / 8;  // 8 ptrs per cacheline
      auto cacheline_in_slot_idx = free_slot_cacheline % 8;
      auto cacheline_mask        = uint64_t(0xff) << cacheline_in_slot_idx;

      // assert the ptr is not already marked as free
      assert(not(pg.free_ptrs[free_slot_idx].load(std::memory_order_relaxed) & free_slot_bit));

      ptr.~shared_ptr();  /// reset it, make it ready to be allocated again

      // mark the ptr as free, we are the sole owner of this bit so fetch_add is safe
      auto prev = pg.free_ptrs[free_slot_idx].fetch_add(free_slot_bit, std::memory_order_release);

      // assert the bit was not already set, race condition of two people freeing the same ptr
      assert(prev & free_slot_bit);

      if (prev & cacheline_mask) [[likely]]  // 7 in 8
         return;                             // we were not the first to be freed in cacheline

      // we are the first ptr in the page to be freed, set the bit in the page
      auto free_slot_cl_bit = 1ULL << free_slot_cacheline;  // 63 in 64 chance
      if (pg.free_cachelines.fetch_xor(free_slot_cl_bit, std::memory_order_release)) [[likely]]
         return;  // we are not the first cacheline with free bit

      int   reg_free_pages_idx    = page_idx >= 64;
      auto& fp_idx                = reg.free_pages[reg_free_pages_idx];
      int   bit_in_free_pages_idx = page_idx % 64;

      fp_idx.fetch_xor(1ULL << bit_in_free_pages_idx, std::memory_order_release);
   }
}  // namespace sal
