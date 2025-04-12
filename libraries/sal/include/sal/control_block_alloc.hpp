#pragma once
#include <hash/lehmer64.h>
#include <hash/xxhash.h>
#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <sal/block_allocator.hpp>
#include <sal/control_block.hpp>
#include <sal/mapping.hpp>
#include <sal/numbers.hpp>
#include <sal/simd_utils.hpp>
#include <span>
#include <ucc/typed_int.hpp>
namespace sal
{
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
      static constexpr uint32_t zone_size_bytes     = ptrs_per_zone * sizeof(control_block);  // 32
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
            //            SAL_WARN("min_zone: {} min_count: {}", min_zone, min_count);
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
            {
               //              SAL_INFO("min_zone: {} prior: {} maz_used: {}", zone, prior, maz_used);
               min_alloc_zone.store(zone, std::memory_order_release);
            }

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
      ptr_address_seq addr_seq;
      control_block*  ptr;
   };

   /**
    * This allocator is used to manage the storage of shared pointers to
    * objects in shared memory. Traditionally there are two places where
    * std::control_block's state (its control block) is stored, with the object
    * it points to (eg. make_shared) or as its own heap allocation as in
    * (std::control_block<T>(new T)).
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
    * The allocator grows in blocks (zones) of 32 mb, of 4 million control_blocks, and will utilize 
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
   class control_block_alloc
   {
     public:
      /**
       *  @param dir the directory to store the page table and pages
       */
      control_block_alloc(const std::filesystem::path& dir);
      ~control_block_alloc();

      /**
       *  @throw std::runtime_error if no ptrs are available
       *  @return an the address of the allocated control_block and a pointer to the control_block 
       * @brief this will attempt to allocate on one of the least filled cachelines
       * within the current zone. 
       */
      allocation alloc()
      {
         // each thread will look to a different cacheline in the zone minimizing contention
         static std::atomic<uint64_t>     seed(0);
         static thread_local lehmer64_rng rnd(seed.fetch_add(1, std::memory_order_relaxed));
         int                              attempts = 0;
         do
         {
            // if average allocations is > 50% then we need to add a new zone
            if (_header_ptr->average_allocations() > detail::ptrs_per_zone / 2) [[unlikely]]
               ensure_capacity(num_allocated_zones() + 1);

            auto  min_zone       = _header_ptr->min_alloc_zone.load(std::memory_order_relaxed);
            auto* zone_free_base = _free_list_base + ((detail::ptrs_per_zone / 64) * min_zone);

            /// pick 1 64 byte cacheline ( 8 uint64_t ) to scan free bits from
            static constexpr uint32_t n64_per_zone = detail::ptrs_per_zone / 64;
            uint64_t                  start_index  = rnd.next() % n64_per_zone;
            start_index &= ~7ull;  // round down to nearest multiple of 8

            // atomic load 8 uint64_t from the zone free list
            //alignas(128) uint64_t free_bits[8];
            //for (int i = 0; i < 8; ++i)
            //   free_bits[i] = zone_free_base[start_index + i].load(std::memory_order_relaxed);

            uint8_t* free_bytes = (uint8_t*)(zone_free_base + start_index);

            // get an index of the byte with the most set (free) bits
            int most_free_byte = max_pop_cnt8_index64(free_bytes);

            if (free_bytes[most_free_byte] == 0) [[unlikely]]
            {
               // it is entirely possible that all 64 bytes are already taken and/or another
               // thread randomly chose the same cacheline and took the last pointer in the cacheline.
               // so we need to try again... given a 50% capacity target, most of the time there should
               // be at least 1 free pointer out of the 512 pointers checked by max_pop_cnt8_index64.
               // this is most likely to happen once you approach max capacity, but could happen
               // if due to heavy locality in one area of memory that happens to get randomly
               // chosen.  If 99% of all bits in a zone are allocated, then there is a 0.5% chance that
               // 512 bits will all be taken... assuming independences; however, since we are
               // also allowing hints, this would undermine complete independence, in any event,
               // we can try multiple times across many different zones and are likely to find
               // a free slot within a few attempts even with 99% of capacity. This is because
               // we are able to check 512 bits at a time.
               if (++attempts == 1024 * 1024)
               {
                  throw std::runtime_error("failed to allocate control block after 1M attempts ");
               }
               /*
               SAL_WARN("all 512 slots in zone {} starting at {} are taken", min_zone,
                        start_index * 512);
               SAL_WARN("most free byte: free_bits[{}] = {:x}", most_free_byte,
                        uint16_t(((uint8_t*)free_bits)[most_free_byte]));
               for (int i = 0; i < 64; ++i)
                  assert(free_bytes[i] == 0);
               auto most_free_byte = max_pop_cnt8_index64((uint8_t*)free_bits);
               (void)most_free_byte;
               SAL_WARN("total alloc: {}",
                        _header_ptr->total_allocations.load(std::memory_order_relaxed));
               SAL_WARN("ptrs-per-zone: {}", detail::ptrs_per_zone);
               SAL_WARN("zone_size_bytes: {}", detail::zone_size_bytes);
               assert(attempts < 10);
               */
               continue;
            }

            auto optalloc = try_alloc(ptr_address(min_zone * detail::ptrs_per_zone +
                                                  start_index * 64 + most_free_byte * 8));
            if (not optalloc) [[unlikely]]
            {
               SAL_WARN("failed to allocate from hint: {} with cl claiming {} free",
                        ptr_address(min_zone * detail::ptrs_per_zone + start_index * 64 +
                                    most_free_byte * 8),
                        std::popcount(free_bytes[most_free_byte]));
               continue;
            }

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

      std::optional<allocation> try_alloc(ptr_address addr)
      {
         // round down to the nearest 16 element boundary
         uint32_t cl      = *addr & ~uint32_t(0x0f);
         uint32_t flblock = cl / 64;

         // the cacheline falls into these 64 bytes.
         std::atomic<uint64_t>& free_list = _free_list_base[flblock];

         // in a 128 byte cacheline, there are 16 8 byte ptrs ( aka 64 items)
         // cl has already been rounded down, so if we %64 we will get 0,
         // 16, 32, or 48, which is exaclty how many bytes we need to shift
         // the mask that identifies potential spots on same cacheline as hint
         const uint32_t base_offset    = cl % 64;
         uint64_t       base_clinebits = 0xffffull << base_offset;

         // now get the intersection of the mask and actual free bits to see if
         // we have anything.
         uint64_t flist            = free_list.load();
         uint64_t masked_free_bits = flist & base_clinebits;

         while (masked_free_bits)
         {
            // while there are free bits in the cacheline of the hint
            int  index = std::countr_zero(masked_free_bits);
            auto bit   = uint64_t(1ull) << index;
            if (not free_list.compare_exchange_strong(flist, flist ^ bit,
                                                      std::memory_order_acquire)) [[unlikely]]
            {
               masked_free_bits = flist & base_clinebits;
               //               SAL_WARN("*contention* attempting to claim address: {} ", flblock * 64 + index);
               continue;
            }
            ptr_address    ptr(flblock * 64 + index);
            control_block& p = _ptr_base[*ptr];
            return allocation{{ptr, _header_ptr->inc_alloc_count(ptr)}, &p};
         }
         return std::nullopt;
      }

      /**
       * This will attempt to allocate in one of the cachelines provided by the hint.
       */
      std::optional<allocation> try_alloc(const alloc_hint& hint)
      {
         for (auto addr : hint)
         {
            if (auto alloc = try_alloc(addr))
               return alloc;
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
      control_block& get(ptr_address address) noexcept
      {
         assert((*address / detail::ptrs_per_zone) <
                _header_ptr->allocated_zones.load(std::memory_order_relaxed));

         return _ptr_base[*address];
      }

      /// @brief Try to get a pointer, returning nullptr if the address is invalid or freed
      /// @param address The address to look up
      /// @return A pointer to the control_block if it exists and is allocated, nullptr otherwise
      control_block* try_get(ptr_address address) noexcept
      {
         if ((*address / detail::ptrs_per_zone) >=
             _header_ptr->allocated_zones.load(std::memory_order_relaxed))
            return nullptr;

         auto& ptr = _ptr_base[*address];
         if (ptr.load(std::memory_order_relaxed).cacheline_offset ==
             control_block::max_cacheline_offset)
            return nullptr;

         return &ptr;
      }

      /**
       * @brief Get a control_block by address, allocating it if it doesn't exist
       * 
       * This method is used in recovery scenarios where we need to ensure a pointer
       * exists at a specific address. If the pointer already exists, it returns a
       * reference to the existing pointer. If it doesn't exist, it allocates a new
       * pointer at that address.
       * 
       * @param address The address to retrieve or allocate at
       * @return A reference to the control_block at the specified address
       * @throws std::runtime_error if allocation fails
       */
      control_block& get_or_alloc(ptr_address address)
      {
         SAL_WARN("get_or_alloc: {}", address);
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
      inline bool claim_address(ptr_address address)
      {
         assert(is_free(address));
         //    SAL_WARN("claim_address: {}", address);
         assert((*address / detail::ptrs_per_zone) <
                _header_ptr->allocated_zones.load(std::memory_order_relaxed));

         auto& flb     = _free_list_base[*address / 64];
         auto  bit     = (1ull << (*address % 64));
         auto  current = flb.load(std::memory_order_relaxed);
         do
         {
            if (not(current & bit))
               return false;
         } while (not flb.compare_exchange_weak(current, current ^ bit, std::memory_order_acquire));

         return true;
      }

      bool is_free(ptr_address address)
      {
         return (_free_list_base[*address / 64].load(std::memory_order_relaxed) &
                 (1ull << (*address % 64)));
      }
      void release_address(ptr_address address)
      {
         //         SAL_WARN("release_address: {}", address);
         assert((*address / detail::ptrs_per_zone) <
                _header_ptr->allocated_zones.load(std::memory_order_relaxed));

         assert(not is_free(address));

         auto prior = _free_list_base[*address / 64].fetch_add(1ull << (*address % 64),
                                                               std::memory_order_release);
         assert(not(prior & (1ull << (*address % 64))));
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
      control_block*                   _ptr_base;
      std::atomic<uint64_t>*           _free_list_base;
      std::mutex                       _grow_mutex;
   };
}  // namespace sal
