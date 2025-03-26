#pragma once
#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <filesystem>
#include <ostream>
#include <sal/block_allocator.hpp>
#include <sal/mapping.hpp>
#include <sal/shared_ptr.hpp>
#include <sal/typed_int.hpp>

namespace sal
{

   /**
    * 
    */
   struct ptr_address
   {
      using index_type  = typed_int<uint16_t, struct index_tag>;
      using region_type = typed_int<uint16_t, struct region_tag>;
      ptr_address() {}
      ptr_address(region_type region, index_type index) : index(index), region(region) {}
      explicit ptr_address(uint32_t addr)
          : index(index_type(addr & 0xffff)), region(region_type(addr >> 16))
      {
      }

      index_type  index;
      region_type region;

      uint32_t                     to_int() const { return uint32_t(*region) << 16 | *index; }
      static constexpr ptr_address from_int(uint32_t addr)
      {
         return ptr_address(region_type(addr >> 16), index_type(addr & 0xffff));
      }
      explicit operator bool() const { return bool(index); }

      friend bool operator<=>(const ptr_address& a, const ptr_address& b) = default;

      friend std::ostream& operator<<(std::ostream& os, const ptr_address& addr)
      {
         return os << "r" << addr.region << "." << addr.index;
      }
   };
   static_assert(sizeof(ptr_address) == 4, "ptr_address must be 4 bytes");

   /**
    * operator to combine a region and index into an address
    */
   inline ptr_address operator+(ptr_address::region_type r, ptr_address::index_type i)
   {
      return ptr_address(r, i);
   }

   /**
    * When address are allocated they are assigned a sequence number
    * which is used to track the order of allocation across threads and
    * facilitate recovery when multiple segments have the same node with
    * the same address. 
    */
   struct ptr_address_seq
   {
      ptr_address address;
      uint32_t    sequence;

      constexpr ptr_address_seq(ptr_address addr, uint32_t seq) : address(addr), sequence(seq) {}

      constexpr      operator ptr_address() const { return address; }
      constexpr bool operator==(const ptr_address_seq& other) const = default;
   };

   /// forward declared internal hint which is the aggregate of alloc_hints
   struct hint;

   /**
    * The allocator should make its best effort to allocate a new ptr on
    * the same cacheline as one of the hints. If the allocator can't find
    * a cacheline with a free slot, it will allocate a new ptr in a new
    * cacheline that is mostly empty.
    */
   struct alloc_hint
   {
      alloc_hint(const ptr_address::index_type* hints, uint16_t count = 1)
          : hints(hints), count(count)
      {
      }
      static constexpr alloc_hint    any() { return alloc_hint{nullptr, 0}; }
      const ptr_address::index_type* hints = nullptr;
      uint16_t                       count = 0;
   };

   namespace detail
   {
      static constexpr uint32_t ptrs_per_page    = 512;
      static constexpr uint32_t pages_per_region = (1 << 16) / ptrs_per_page;
      using page_number                          = typed_int<uint16_t, struct page_number_tag>;

      /**
       * Stores 512 pointers in a region along with
       * index information to help identify free slots.
       */
      struct alignas(std::hardware_destructive_interference_size) page  // 4176 bytes
      {
         /// 1 bit for each ptr in page::ptrs, 64 bytes 1 cacheline
         std::array<std::atomic<uint64_t>, ptrs_per_page / 64> free_ptrs;
         /// 1 bit for each 64 bit cacheline in page::ptrs that has at least 1 free ptr
         std::atomic<uint64_t> free_cachelines = uint64_t(-1);
         /// 1 bit for each 64 bit cacheline in page::ptrs with <= 4 free ptr
         std::atomic<uint64_t> half_free_cachelines = 0;

         auto& get_ptr(ptr_address::index_type index) { return ptrs[ptr_index_on_page(index)]; }

         page()
         {
            free_cachelines.store(~0ULL, std::memory_order_relaxed);
            half_free_cachelines.store(0, std::memory_order_relaxed);

            // the first ptr is always taken, it is the null ptr, and
            // should never be allocated.
            for (uint32_t i = 0; i < free_ptrs.size(); ++i)
               free_ptrs[i].store(~0ULL, std::memory_order_relaxed);
         }

        private:
         int ptr_index_on_page(ptr_address::index_type address_index)
         {
            return *address_index % ptrs_per_page;
         }

         // 64 cachelines, 512 ptrs
         alignas(64) std::array<shared_ptr, ptrs_per_page> ptrs;
      };

      using page_offset                      = block_allocator::offset_ptr;
      static constexpr page_offset null_page = block_allocator::null_offset;

      struct region
      {
         /// 1 bit for each page in the region with at least 1 free ptr,
         /// there are at most 128 pages in a region with 512 ptrs per
         /// page giving 2^16 addresses per region.
         std::array<std::atomic<uint64_t>, 2> free_pages;

         /// page_index of -1 means the page is not allocated,
         /// otherwise it is an offset_ptr into _page_allocator
         std::array<std::atomic<page_offset>, pages_per_region> pages;

         page_offset get_page_offset(ptr_address::index_type index) const
         {
            return pages[*index / ptrs_per_page].load(std::memory_order_relaxed);
         }
         page_offset get_page_offset(page_number page) const
         {
            return pages[*page].load(std::memory_order_relaxed);
         }

         static_assert(std::atomic<detail::page_offset>::is_always_lock_free,
                       "std::atomic<page_offset> must be lock free");
         region()
         {
            for (auto& p : pages)
               p.store(null_page, std::memory_order_relaxed);
            free_pages[0].store(~0ULL, std::memory_order_relaxed);
            free_pages[1].store(~0ULL, std::memory_order_relaxed);
         }
      };

      struct page_table
      {
         /// next region to allocate in for those who don't care which region
         std::atomic<uint16_t> _next_region = 0;
         std::atomic<uint32_t> _sequence    = 0;

         /// protected by _page_alloc_mutex
         std::atomic<uint64_t> _pages_alloced = 0;
         /// 1 bit for each region with at least 1 free page
         std::array<region, 1 << 16> regions;

         region& get_region(ptr_address::region_type reg) { return regions[*reg]; }

         void inc_region(ptr_address::region_type reg)
         {
            auto idx = *reg / 4;
            auto bit = *reg % 4;
            region_use_counts[idx].fetch_add(1ULL << (bit * 16), std::memory_order_relaxed);
         }
         void dec_region(ptr_address::region_type reg)
         {
            auto idx = *reg / 4;
            auto bit = *reg % 4;
            region_use_counts[idx].fetch_sub(1ULL << (bit * 16), std::memory_order_relaxed);
         }

         /// there are 4, 16 bit, counters per uint64_t.
         alignas(128) std::array<std::atomic<uint64_t>, (1 << 16) / 4> region_use_counts;
      };
   }  // namespace detail

   struct allocation
   {
      ptr_address address;
      shared_ptr* ptr;
      uint32_t    sequence;
   };

   /**
    * This allocator is used to manage the storage of shared pointers to
    * objects in shared memory. Traditionally there are two places where
    * std::shared_ptr's state is stored, with the object it points to (eg. make_shared)
    * or as its own heap allocation as in (std::shared_ptr<T>(new T)).
    * 
    * When building data structures, (e.g a Copy on Write (COW) Trie), each node needs to
    * store up to 257 shared pointers. Each COW requires copying these shared pointers,
    * which means accessing 257 locations in memory just to increment a reference count.
    * 
    * Ideally we would only need to access 32 cachelines to update 256 reference counts,
    * but with traditional allocation strategies we usually have to load 256 cachelines.
    * 
    * This allocator uses a memory mapped file to store the shared pointers tightly packed
    * and provides a 32 bit ptr_address to each shared pointer. Furthermore, the allocator
    * gives the caller the power to control up to 256 regions in which to allocate the
    * shared pointers and even enables the caller to provide hints about cachelines it
    * would prefer to use. 
    * 
    * In this way data structures such as COW tries can build nodes that store all
    * of their children in a single region and address them with just 16 bits of address
    * while also greatly increasing the probability of many children sharing cachelines.
    * 
    * The pointers fit in a single atomic uint64_t and contain a reference to an offset
    * into the memory mapped file provided by the data allocator. The data allocator only
    * produces cacheline aligned data, so the offset can save bits and make space to store
    * additional metadata with the pointer. This enables atomic moves of the data the
    * pointer references among other things.
    * 
    * Given a ptr_address, first load page_table to get the idx of page
    *        then load the page + offset to get the shared ptr.location
    * 
    * This means 2 cacheline loads per ptr access if the page_table is cold.
    * The page_table when using 4 billion pointers is about 32 MB, but the
    * first 6144 pointers in each region can be accessed with just the 
    * 1st cacheline. Assuming well distributed data across regions,
    * you have a database with 3 GB worth of pointers pointing to at 
    * least 25 GB of memory. All access for one node are likely to go 
    * through the same cacheline of the page table.  
    */
   class shared_ptr_alloc
   {
     public:
      /**
       *  @param dir the directory to store the page table and pages
       */
      shared_ptr_alloc(const std::filesystem::path& dir);
      ~shared_ptr_alloc();

      /**
       * A suggestion for a region when you don't care which region 
       * you are allocated in, attempts provide a region that isn't
       * already over crowded.
       */
      ptr_address::region_type get_new_region();

      /**
       *  @throw std::runtime_error if no ptrs are available
       *  @return an the address of the allocated ptr and a pointer to the shared_ptr
       */
      allocation alloc(ptr_address::region_type region, const alloc_hint& hint = alloc_hint::any());

      /// @pre address is a valid pointer address
      void free(ptr_address address);

      /// @pre address is a valid pointer address returned from alloc()
      shared_ptr& get(ptr_address address)
      {
         detail::page_offset poff =
             get_page_table().get_region(address.region).get_page_offset(address.index);
         return get_page(poff).get_ptr(address.index);
      }

      /// @brief Try to get a pointer, returning nullptr if the address is invalid or freed
      /// @param address The address to look up
      /// @return A pointer to the shared_ptr if it exists and is allocated, nullptr otherwise
      shared_ptr* try_get(ptr_address address)
      {
         auto& region = get_page_table().get_region(address.region);

         // Check if the page exists
         detail::page_offset poff = region.get_page_offset(address.index);
         if (poff == detail::null_page)
            return nullptr;

         // Get the page
         auto& page = get_page(poff);

         // Check if the pointer is allocated (bit is NOT set in the free bitmap)
         uint32_t slot_idx = *address.index / 64;
         uint32_t bit_idx  = *address.index % 64;

         // If the bit is set, the pointer is free
         uint64_t free_ptrs_bitmap = page.free_ptrs[slot_idx].load(std::memory_order_relaxed);
         if (free_ptrs_bitmap & (1ULL << bit_idx))
            return nullptr;

         // Pointer exists and is allocated
         return &page.get_ptr(address.index);
      }

      /**
       * @brief Get a shared_ptr by address, allocating it if it doesn't exist
       * 
       * This method is used in recovery scenarios where we need to ensure a pointer
       * exists at a specific address. If the pointer already exists, it returns a
       * reference to the existing pointer. If it doesn't exist, it allocates a new
       * pointer at that address.
       * 
       * @param address The address to retrieve or allocate at
       * @return A reference to the shared_ptr at the specified address
       * @throws std::runtime_error if allocation fails
       */
      shared_ptr& get_or_alloc(ptr_address address);

      /**
       * This API is utilized to rebuild the allocator in the event of a 
       * a crash that corrupted the state.
       * @group recovery_api
       */
      /// @{
      // set all meta nodes to 0
      void clear_all();

      // release all refs, if prior was <= 1 move to free list
      void release_unreachable();

      // set all refs > 1 to 1, leave 0 alone
      void reset_all_refs();

      /// @brief Returns the total number of used pointers across all regions
      /// @return The sum of all region use counts
      uint64_t used() const;

      /**
       * @brief Calculate statistics on region usage
       * @return A struct containing min, max, mean, and standard deviation of region usage
       */
      struct region_stats_t
      {
         uint16_t min;     ///< Minimum number of used pointers in any non-empty region
         uint16_t max;     ///< Maximum number of used pointers in any region
         double   mean;    ///< Average number of used pointers across non-empty regions
         double   stddev;  ///< Standard deviation of used pointers across non-empty regions
         uint32_t count;   ///< Number of non-empty regions
      };
      region_stats_t region_stats() const;
      /// @}

      static constexpr uint32_t max_regions = 1 << 16;
      void clear_active_bits(ptr_address::region_type start_region, uint32_t num_regions);

     private:
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
      std::optional<allocation> try_alloc(ptr_address::region_type region, const hint& h);

      detail::page_number address_index_to_page(ptr_address::index_type address_index)
      {
         return detail::page_number(*address_index / ptrs_per_page);
      }
      int ptr_index_on_page(ptr_address::index_type address_index)
      {
         return *address_index % ptrs_per_page;
      }
      detail::page_offset alloc_page(detail::page_number pg_num);

      detail::page& get_or_alloc_page(detail::region& reg, detail::page_number pg_num);

      /// @pre page_index is a valid page index
      detail::page& get_page(block_allocator::offset_ptr pg)
      {
         return *_page_allocator->get<detail::page>(pg);
      }
      detail::page_table& get_page_table() { return *_page_table->as<detail::page_table>(); }

      // Const versions for read-only access
      const detail::page_table& get_page_table() const
      {
         return *_page_table->as<detail::page_table>();
      }

      static constexpr uint32_t ptrs_per_page    = 512;
      static constexpr uint32_t pages_per_region = (1 << 16) / ptrs_per_page;
      static constexpr uint32_t alloc_block_size = 16 * 1024 * 1024;

      /**
       * used for allocating blocks of pages 16 MB at a time,
       * and then subdividing them into page objects and issuing
       * them as needed.
       */
      std::unique_ptr<block_allocator> _page_allocator;
      std::unique_ptr<mapping>         _page_table;
      std::filesystem::path            _dir;

      /// only one thread at a time should attempt to allocate
      /// pages when they come across a null_page in the page_table
      std::mutex _page_alloc_mutex;
   };
}  // namespace sal
