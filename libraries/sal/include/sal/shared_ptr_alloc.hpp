#pragma once
#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <sal/block_allocator.hpp>
#include <sal/mapping.hpp>

namespace sal
{
   /**
    * References a location in shared memory, addressed by cacheline
    * as used in shared_ptr_data::cacheline_offset. Its purpose is to
    * keep track of whether the location is addressed in bytes (absolute),
    * on by cacheline index.  It assumes a 64 byte cacheline.  
    */
   struct location
   {
     public:
      uint64_t absolute_address() const { return _cacheline_offset * 64; }
      uint64_t cacheline() const { return _cacheline_offset; }

      static location from_absolute_address(uint64_t address) { return location(address / 64); }
      static location from_cacheline(uint64_t cacheline) { return location(cacheline); }

     private:
      explicit location(uint64_t cacheline_offset) : _cacheline_offset(cacheline_offset) {}
      uint64_t _cacheline_offset;
   };

   /**
    * A shared pointer to a location in shared memory
    */
   struct shared_ptr
   {
     public:
      int  use_count() const { return shared_ptr_data(_data.load(std::memory_order_relaxed)).ref; }
      bool unique() const { return use_count() == 1; }
      void reset(){};
      bool retain(){};
      bool release(){};

     private:
      std::atomic<uint64_t> _data;

      struct shared_ptr_data
      {
         /// reference count, up to 16k shared references
         uint64_t ref : 19;
         /// index to the cacheline of up to 128 TB of memory with 64 bytes per cacheline
         /// this is the maximum addressable by mapped memory on modern systems.
         uint64_t cacheline_offset : 41;

         /// indicates this object doesn't have any shared_ptr members,
         /// nor destructor calls needed
         uint64_t is_pod : 1;

         /// 0 for small objects, 1 for large objects
         uint64_t zone : 1;

         /// used to track the activity of object for caching purposes
         /// saturated integer, 0 to 3
         uint64_t activity : 2;

         uint64_t to_int() const { return std::bit_cast<uint64_t>(*this); }
         void     from_int(uint64_t value) { *this = std::bit_cast<shared_ptr_data>(value); }

         shared_ptr_data& set_ref(uint64_t r)
         {
            ref = r;
            return *this;
         }
         shared_ptr_data& set_cacheline_offset(uint64_t o)
         {
            cacheline_offset = o;
            return *this;
         }
         shared_ptr_data& set_pod(bool p)
         {
            is_pod = p;
            return *this;
         }
         shared_ptr_data& set_activity(uint64_t a)
         {
            activity = a;
            return *this;
         }
      };

      /**
       * updates the cacheline_offset to the desired value 
       * if the current value is equal to the expected value 
       * and the ref count is not 0
       */
      bool cas_move(location expected, location desired);
      static_assert(sizeof(shared_ptr_data) == 8, "shared_ptr_data must be 8 bytes");
   };
   static_assert(sizeof(shared_ptr) == 8, "shared_ptr must be 8 bytes");

   /**
    * 
    */
   struct ptr_address
   {
      uint16_t region;
      uint16_t index;
   };

   struct hint;

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

      struct allocation
      {
         ptr_address address;
         shared_ptr* ptr;
      };

      /**
       * A suggestion for a region when you don't care which region 
       * you are allocated in, attempts provide a region that isn't
       * already over crowded.
       */
      uint16_t get_region() { return _next_region.fetch_add(1, std::memory_order_relaxed); }

      /**
       *  @throw std::runtime_error if no ptrs are available
       *  @return an the address of the allocated ptr and a pointer to the shared_ptr
       */
      allocation alloc(uint16_t region, address_index* hint = 0, uint16_t hint_count = 0);

      /// @pre address is a valid pointer address
      void free(ptr_address address);

      /// @pre address is a valid pointer address returned from alloc()
      shared_ptr& get(ptr_address address)
      {
         auto pg =
             get_page_table().regions[address.region].pages[address_index_to_page(address.index)];
         return get_page(pg).ptrs[ptr_index_on_page(address.index)];
      }

     private:
      static constexpr uint16_t null_page = std::numeric_limits<uint16_t>::max();
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
      std::optional<allocation> try_alloc(uint16_t region, const hint& h);

      int address_index_to_page(uint16_t address_index) { return address_index / ptrs_per_page; }
      int ptr_index_on_page(uint16_t address_index) { return address_index % ptrs_per_page; }

      /// @pre page_index is a valid page index
      page& get_page(page_index pg)
      {
         assert(pg != null_page);
         return *_page_allocator->get<page>(pg);
      }
      page_table& get_page_table() { return *_page_table->as<page_table>(); }

      static constexpr uint32_t ptrs_per_page    = 512;
      static constexpr uint32_t pages_per_region = (1 << 16) / ptrs_per_page;
      static constexpr uint32_t alloc_block_size = 16 * 1024 * 1024;

      struct page  // 4176 bytes
      {
         /// 1 bit for each ptr in page::ptrs, 64 bytes 1 cacheline
         std::array<std::atomic<uint64_t>, ptrs_per_page / 64> free_ptrs;
         /// 1 bit for each 64 bit cacheline in page::ptrs that has at least 1 free ptr
         std::atomic<uint64_t> free_cachelines = uint64_t(-1);
         /// 1 bit for each 64 bit cacheline in page::ptrs with <= 4 free ptr
         std::atomic<uint64_t> half_free_cachelines = 0;

         // 64 cachelines, 512 ptrs
         std::array<shared_ptr, ptrs_per_page> ptrs;

         page()
         {
            for (auto& fp : free_ptrs)
               fp.store(uint64_t(-1), std::memory_order_relaxed);
         }
      };

      using page_index = uint32_t;
      struct region
      {
         /// 1 bit for each page in the region with at least 1 free ptr,
         /// there are at most 128 pages in a region with 512 ptrs per
         /// page giving 2^16 addresses per region.
         std::array<std::atomic<uint64_t>, 2> free_pages;

         /// page_index of -1 means the page is not allocated,
         /// otherwise it is an offset_ptr into _page_allocator
         std::array<std::atomic<page_index>, pages_per_region> pages;

         region() : free_pages(uint64_t(-1), uint64_t(-1))
         {
            std::fill(pages.begin(), pages.end(), null_page);
         }
      };

      struct page_table
      {
         /// next region to allocate in for those who don't care which region
         std::atomic<uint16_t> _next_region = 0;

         /// protected by _page_alloc_mutex
         uint64_t _pages_alloced = 0;
         /// 1 bit for each region with at least 1 free page
         std::array<region, 1 << 16> regions;

         void inc_region(uint16_t reg)
         {
            auto idx = reg / 4;
            auto bit = reg % 4;
            region_use_counts[idx].fetch_add(1ULL << (bit * 16), std::memory_order_relaxed);
         }
         void dec_region(uint16_t reg)
         {
            auto idx = reg / 4;
            auto bit = reg % 4;
            region_use_counts[idx].fetch_sub(1ULL << (bit * 16), std::memory_order_relaxed);
         }

         /// there are 4, 16 bit, counters per uint64_t.
         std::array<std::atomic<uint64_t>, (1 << 16) / 4> region_use_counts;
      };

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
