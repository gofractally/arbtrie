#pragma once
#include <hash/lehmer64.h>
#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <filesystem>
#include <sal/block_allocator.hpp>
#include <sal/mapping.hpp>
#include <sal/shared_ptr.hpp>
#include <sal/simd_utils.hpp>
#include <span>
#include <ucc/typed_int.hpp>
namespace sal
{

   using ptr_address = ucc::typed_int<uint32_t, struct ptr_address_tag>;

   /**
    * When address are allocated they are assigned a sequence number
    * which is used to track the order of allocation across threads and
    * facilitate recovery when multiple segments have the same node with
    * the same address. 
    */
   class ptr_address_seq
   {
     public:
      uint16_t    sequence;
      ptr_address address;

      ptr_address_seq() : sequence(0), address(0) {}
      constexpr ptr_address_seq(ptr_address addr, uint32_t seq) : sequence(seq), address(addr) {}

      constexpr      operator ptr_address() const { return address; }
      constexpr bool operator==(const ptr_address_seq& other) const = default;
   } __attribute__((packed));

   using alloc_hint = std::span<const ptr_address>;

   namespace detail
   {
      static constexpr uint32_t ptrs_per_zone       = 1 << 22;  // 4 million
      static constexpr uint32_t zone_size_bytes     = ptrs_per_zone * sizeof(shared_ptr);  // 32
      static constexpr uint32_t max_allocated_zones = (1ull << 32) / ptrs_per_zone;
      using zone_number = ucc::typed_int<uint16_t, struct zone_number_tag>;

      struct zone_free_list
      {
         std::array<std::atomic<uint64_t>, ptrs_per_zone / 64> free_ptrs;
      };
      struct ptr_alloc_header
      {
         /// zone number with most free ptrs
         std::atomic<uint16_t> min_alloc_zone;

         /// total allocations in this zone
         std::atomic<uint32_t> alloc_seq;
         std::atomic<uint64_t> total_allocations;

         /// @return the sequence number of the allocation
         uint32_t inc_alloc_count(ptr_address ptr)
         {
            total_allocations.fetch_add(1, std::memory_order_relaxed);
            const auto zone       = *ptr / ptrs_per_zone;
            auto       prior_used = zone_alloc_count[zone].fetch_add(1, std::memory_order_relaxed);

            /**
             * When adding to a zone we might no longer be the valid "min zone", 
             * so if we are considered the "min zone" we may need to update the min
             * zone to the lowest zone. We don't know what the true lowest is, but
             * we can quickly calculate the average per zone, so if our zone is above
             * average then we know, with certainty, that we are no longer the min zone.
             */
            auto min_zone = min_alloc_zone.load(std::memory_order_relaxed);
            if (min_zone == zone and prior_used >= average_allocations())
               update_min_zone();

            return alloc_seq.fetch_add(1, std::memory_order_relaxed);
         }
         void update_min_zone()
         {
            uint32_t min_zone  = 0;
            uint32_t min_count = zone_alloc_count[0].load(std::memory_order_relaxed);
            uint32_t num_zones = allocated_zones.load(std::memory_order_relaxed);
            for (uint32_t i = 1; i < num_zones; ++i)
            {
               auto zmin = zone_alloc_count[i].load(std::memory_order_relaxed);
               if (zmin < min_count)
               {
                  min_zone  = i;
                  min_count = zmin;
               }
            }
            min_alloc_zone.store(min_zone, std::memory_order_release);
         }

         void dec_alloc_count(ptr_address ptr)
         {
            const auto zone  = *ptr / ptrs_per_zone;
            auto       prior = zone_alloc_count[zone].fetch_sub(1, std::memory_order_relaxed);

            /**
             * When removing from a zone we might become the new "min zone", or at least
             * be lower than what is considered the current "min zone" which may be anywhere
             * between the abs min and the average. 
             */
            auto maz      = min_alloc_zone.load(std::memory_order_acquire);
            auto maz_used = zone_alloc_count[maz].load(std::memory_order_relaxed);

            if (prior - 1 < maz_used)
               min_alloc_zone.store(zone, std::memory_order_release);

            total_allocations.fetch_sub(1, std::memory_order_relaxed);
         }
         uint32_t average_allocations() const
         {
            return total_allocations.load(std::memory_order_relaxed) /
                   allocated_zones.load(std::memory_order_relaxed);
         }

         /// the number of zones allocated
         std::atomic<uint32_t> allocated_zones;

         /// for each zone, the number of allocated pointers in the zone, max 1024 zones
         /// for 4 billion pointers (2^32)
         std::array<std::atomic<uint32_t>, (1ull << 32) / ptrs_per_zone> zone_alloc_count;
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
    * gives the caller the power to provide hints about cachelines it would prefer to use.
    * 
    * In this way data structures such as COW tries can build nodes that easily compress
    * the number of bytes needed to store pointers while also minimizing cache misses when
    * retaining/releasing/visiting all children.
    * 
    * The allocator grows in blocks (zones) of 32 mb, of 4 million pointers, and will utilize 
    * the allocation zone until it is 50% full, at which point it will allocate a new zone. 
    * From this point on, the allocator will switch zones whenever a zone becomes the least
    * filled or when it goes above the average filled zone. 
    * 
    * Data is stored in 3 files, a header file contains meta information that helps identify
    * the lest filled, a bitmap file that tracks which pointers are free, and a data file
    * that contains the shared pointers.
    * 
    * Given the contiguous nature of the memory mapping, a pointer's address is a direct
    * offset into the datafile and the bitmap file which makes alloc/free/get operations
    * very fast.
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
       *  @throw std::runtime_error if no ptrs are available
       *  @return an the address of the allocated ptr and a pointer to the shared_ptr
       * @brief this will attempt to allocate on one of the least filled cachelines
       * within the current zone. 
       */
      allocation alloc()
      {
         // each thread will look to a different cacheline in the zone minimizing contention
         static std::atomic<uint64_t>     seed(0);
         static thread_local lehmer64_rng rnd(seed.fetch_add(1, std::memory_order_relaxed));
         do
         {
            // if average allocations is > 50% then we need to add a new zone
            if (_header_ptr->average_allocations() > detail::ptrs_per_zone / 2) [[unlikely]]
               ensure_capacity(num_allocated_zones() + 1);

            auto  min_zone       = _header_ptr->min_alloc_zone.load(std::memory_order_relaxed);
            auto* zone_free_base = _free_list_base + (((detail::ptrs_per_zone * min_zone) / 64));

            /// pick 1 cacheline ( 8 uint64_t ) from zone_free_base at random..
            static constexpr uint32_t n64_per_zone = detail::ptrs_per_zone / 64;
            static constexpr uint32_t cl_per_zone  = n64_per_zone / 8;

            auto cl   = rnd.next() % cl_per_zone;
            auto base = zone_free_base + (cl * 8);

            alignas(128) uint64_t free_bits[8];
            for (int i = 0; i < 8; ++i)
               free_bits[i] = base[i].load(std::memory_order_relaxed);

            // get an index of the byte with the most set (free) bits
            auto most_free_byte = max_pop_cnt8_index64((uint8_t*)free_bits);

            ptr_address ptr_hint(min_zone * detail::ptrs_per_zone + cl * 8 + (most_free_byte * 8));

            // it is entirely possible that all 64 bytes are already taken and/or another
            // thread randomly chose the same cacheline and took the last pointer in the cacheline.
            // so we need to try again... given a 50% capacity target, most of the time there should
            // be at least 1 free pointer out of the 512 pointers checked by max_pop_cnt8_index64.
            auto optalloc = try_alloc(alloc_hint{&ptr_hint, 1});
            if (not optalloc) [[unlikely]]
               continue;

            return *optalloc;
         } while (_header_ptr->total_allocations.load(std::memory_order_acquire) < (1ull << 32));
         [[likely]] throw std::runtime_error("failed to allocate");
      }

      void ensure_capacity(uint32_t req_zones);

      /**
       * @brief first attempt to allocate with one of the hints, and if that fails,
       * then allocate on one of the least filled cachelines
       * within the current zone to reduce the likelihood of using a spot a future
       * alloc may want via a hint. 
       */
      allocation alloc(const alloc_hint& hint)
      {
         if (auto alloc = try_alloc(hint))
            return *alloc;
         return alloc();
      }

      /**
       * This will attempt to allocate in one of the cachelines provided by the hint.
       */
      std::optional<allocation> try_alloc(const alloc_hint& hint)
      {
         for (auto addr : hint)
         {
            // round down to the nearest 16 element boundary
            uint32_t cl = *addr & ~uint32_t(0x0f);

            std::atomic<uint64_t>& free_list = _free_list_base[cl / 64];

            // in a 128 byte cacheline, there are 16 8 byte ptrs
            uint64_t base_clinebits = 0xffffull << (cl % 64);

            uint64_t masked_free_bits = free_list.load(std::memory_order_relaxed) & base_clinebits;

            while (masked_free_bits)
            {
               int         index = std::countr_zero(masked_free_bits);
               ptr_address ptr(cl + index);
               shared_ptr& p = _ptr_base[*ptr];
               if (claim_address(ptr)) [[likely]]
                  return allocation{ptr, &p, _header_ptr->inc_alloc_count(ptr)};
               masked_free_bits = free_list.load(std::memory_order_acquire) & base_clinebits;
            }
         }
         return std::nullopt;
      }

      /// @pre address is a valid pointer address
      void free(ptr_address address) noexcept
      {
         assert((*address / detail::ptrs_per_zone) <
                _header_ptr->allocated_zones.load(std::memory_order_relaxed));
         assert(_ptr_base[*address].ref() == 0);

         release_address(address);
         _header_ptr->dec_alloc_count(address);
      }

      /// @pre address is a valid pointer address returned from alloc()
      shared_ptr& get(ptr_address address) noexcept
      {
         assert((*address / detail::ptrs_per_zone) <
                _header_ptr->allocated_zones.load(std::memory_order_relaxed));

         return _ptr_base[*address];
      }

      /// @brief Try to get a pointer, returning nullptr if the address is invalid or freed
      /// @param address The address to look up
      /// @return A pointer to the shared_ptr if it exists and is allocated, nullptr otherwise
      shared_ptr* try_get(ptr_address address) noexcept
      {
         if ((*address / detail::ptrs_per_zone) >=
             _header_ptr->allocated_zones.load(std::memory_order_relaxed))
            return nullptr;

         auto& ptr = _ptr_base[*address];
         if (ptr.load(std::memory_order_relaxed).cacheline_offset ==
             shared_ptr::max_cacheline_offset)
            return nullptr;

         return &ptr;
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
      shared_ptr& get_or_alloc(ptr_address address)
      {
         if (auto ptr = try_get(address))
            return *ptr;

         ensure_capacity(*address / detail::ptrs_per_zone + 1);

         if (not claim_address(address)) [[unlikely]]
            throw std::runtime_error("failed to allocate");
         return _ptr_base[*address];
      }

      /**
       * This API is utilized to rebuild the allocator in the event of a 
       * a crash that corrupted the state.
       * @group recovery_api
       */
      /// @{
      // set all meta nodes to empty state
      void clear_all();

      // release all refs, if prior was <= 1 move to free list
      void release_unreachable();

      // set all refs > 1 to 1, leave 0 alone
      void reset_all_refs();

      /// @brief Returns the total number of used pointers across all regions
      /// @return The sum of all region use counts
      uint64_t used() const
      {
         return _header_ptr->total_allocations.load(std::memory_order_relaxed);
      }

      void clear_active_bits(ptr_address start, uint32_t num)
      {
         auto end = std::min<uint64_t>(*start + num, detail::ptrs_per_zone * num_allocated_zones());
         for (auto i = start; i < end; ++i)
            get(i).clear_pending_cache();
      }

      uint32_t num_allocated_zones() const
      {
         return _header_ptr->allocated_zones.load(std::memory_order_relaxed);
      }

      /// the maximum address that could be allocated without
      /// resizing the file.
      uint32_t current_max_address_count() const
      {
         return detail::ptrs_per_zone * num_allocated_zones();
      }

      //private:
      bool claim_address(ptr_address address)
      {
         assert((*address / detail::ptrs_per_zone) <
                _header_ptr->allocated_zones.load(std::memory_order_relaxed));

         auto& p = _ptr_base[*address];
         if (p.claim())
         {
            /// clear the bit in the bitmap to indicate the pointer is allocated
            auto prior = _free_list_base[*address / 64].fetch_sub(1ull << (*address % 64),
                                                                  std::memory_order_relaxed);
            assert(prior & (1ull << (*address % 64)));
            (void)prior;  // suppress unused variable warning in release builds
         }
         return true;
      }
      void release_address(ptr_address address)
      {
         assert((*address / detail::ptrs_per_zone) <
                _header_ptr->allocated_zones.load(std::memory_order_relaxed));

         assert(!(_free_list_base[*address / 64].load(std::memory_order_relaxed) &
                  (1ull << (*address % 64))));

         _ptr_base[*address].move({}, std::memory_order_relaxed);

         auto prior = _free_list_base[*address / 64].fetch_add(1ull << (*address % 64),
                                                               std::memory_order_release);
         assert(prior & (1ull << (*address % 64)));
         (void)prior;  // suppress unused variable warning in release builds
      }
      /**
       * used for allocating blocks of pages 16 MB at a time,
       * and then subdividing them into page objects and issuing
       * them as needed.
       */
      std::unique_ptr<block_allocator> _zone_allocator;
      std::unique_ptr<block_allocator> _zone_free_list;
      std::unique_ptr<mapping>         _header;
      std::filesystem::path            _dir;
      detail::ptr_alloc_header*        _header_ptr;
      shared_ptr*                      _ptr_base;
      std::atomic<uint64_t>*           _free_list_base;
      std::mutex                       _grow_mutex;
   };
}  // namespace sal
