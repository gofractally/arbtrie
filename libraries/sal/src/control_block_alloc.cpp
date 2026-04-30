#include <hash/lehmer64.h>
#include <sal/control_block_alloc.hpp>
#include <sal/debug.hpp>
#include <sal/mapping.hpp>
#include <sal/min_index.hpp>

namespace sal
{
   control_block_alloc::control_block_alloc(const std::filesystem::path& dir) : _dir(dir)
   {
      std::filesystem::create_directories(dir);
      _dir    = dir;
      _header = std::make_unique<mapping>(dir / "header.bin", sal::access_mode::read_write, true);
      if (_header->size() != sizeof(detail::ptr_alloc_header))
      {
         if (_header->size() != 0)
            throw std::runtime_error("control_block_alloc header file is wrong size");
         _header->resize(sizeof(detail::ptr_alloc_header));
         _header_ptr = new (_header->data()) detail::ptr_alloc_header();
      }
      _header_ptr = reinterpret_cast<detail::ptr_alloc_header*>(_header->data());
      if (not _header_ptr)
         throw std::runtime_error("failed to create control_block_alloc header");
      _zone_allocator = std::make_unique<block_allocator>(dir / "zone.bin", detail::zone_size_bytes,
                                                          detail::max_allocated_zones);
      _zone_free_list = std::make_unique<block_allocator>(
          dir / "free_list.bin", detail::ptrs_per_zone / 8, detail::max_allocated_zones);
      _zone_allocator->mlock_mapped_blocks();
      _zone_free_list->mlock_mapped_blocks();

      SAL_ERROR("free_list.bit blocksize: {}", detail::ptrs_per_zone / 8);
      //   _zone_free_list->reserve(1, true);
      //   _zone_allocator->reserve(1, true);
      ensure_capacity(1);
      publish_mlock_stats();
      _ptr_base       = _zone_allocator->get<control_block>(offset_ptr(0));
      _free_list_base = _zone_free_list->get<std::atomic<uint64_t>>(offset_ptr(0));
   }

   control_block_alloc::~control_block_alloc() {}

   void control_block_alloc::publish_mlock_stats() noexcept
   {
      auto stats = get_mlock_stats();
      _header_ptr->control_block_header_mlock_pinned.store(stats.header_pinned ? 1 : 0,
                                                           std::memory_order_relaxed);
      _header_ptr->control_block_zone_mlock_success_regions.store(
          stats.zones.successful_regions, std::memory_order_relaxed);
      _header_ptr->control_block_zone_mlock_failed_regions.store(
          stats.zones.failed_regions, std::memory_order_relaxed);
      _header_ptr->control_block_zone_mlock_skipped_regions.store(
          stats.zones.skipped_regions, std::memory_order_relaxed);
      _header_ptr->control_block_zone_mlock_success_bytes.store(
          stats.zones.successful_bytes, std::memory_order_relaxed);
      _header_ptr->control_block_zone_mlock_failed_bytes.store(stats.zones.failed_bytes,
                                                               std::memory_order_relaxed);
      _header_ptr->control_block_zone_mlock_skipped_bytes.store(
          stats.zones.skipped_bytes, std::memory_order_relaxed);
      _header_ptr->control_block_freelist_mlock_success_regions.store(
          stats.free_list.successful_regions, std::memory_order_relaxed);
      _header_ptr->control_block_freelist_mlock_failed_regions.store(
          stats.free_list.failed_regions, std::memory_order_relaxed);
      _header_ptr->control_block_freelist_mlock_skipped_regions.store(
          stats.free_list.skipped_regions, std::memory_order_relaxed);
      _header_ptr->control_block_freelist_mlock_success_bytes.store(
          stats.free_list.successful_bytes, std::memory_order_relaxed);
      _header_ptr->control_block_freelist_mlock_failed_bytes.store(
          stats.free_list.failed_bytes, std::memory_order_relaxed);
      _header_ptr->control_block_freelist_mlock_skipped_bytes.store(
          stats.free_list.skipped_bytes, std::memory_order_relaxed);
   }

   void control_block_alloc::ensure_capacity(uint32_t req_zones)
   {
      // if average allocations is > 50% then we need to add a new zone
      // if the min zone is > 50% then we need to add a new zone
      auto azone = _header_ptr->allocated_zones.load(std::memory_order_relaxed);
      if (azone == detail::max_allocated_zones)
         return;

      std::lock_guard<std::mutex> lock(_grow_mutex);

      azone = _header_ptr->allocated_zones.load(std::memory_order_acquire);
      if (azone == detail::max_allocated_zones)
         return;

      auto num_zones = _zone_allocator->reserve(req_zones, true);
      _zone_free_list->reserve(req_zones, true);
      _zone_free_list->resize(num_zones);
      _zone_allocator->resize(num_zones);

      // Verify base addresses are stable after resize. Skip on first call (when _ptr_base is
      // still null — it's assigned after ensure_capacity(1) returns in the constructor).
      // If the underlying reservation was too small and a remap occurred, these would change,
      // causing every existing pointer into the control block array to become dangling.
      if (_ptr_base != nullptr)
      {
         auto* new_ptr_base = _zone_allocator->get<control_block>(block_allocator::offset_ptr(0));
         if (new_ptr_base != _ptr_base)
            throw std::runtime_error(
                "control_block_alloc: zone allocator base address moved after resize — "
                "reservation was insufficient. All control block pointers are now invalid.");
      }

      if (_free_list_base != nullptr)
      {
         auto* new_free_list_base =
             _zone_free_list->get<std::atomic<uint64_t>>(block_allocator::offset_ptr(0));
         if (new_free_list_base != _free_list_base)
            throw std::runtime_error(
                "control_block_alloc: free list base address moved after resize — "
                "reservation was insufficient.");
      }

      while (azone < num_zones)
      {
         SAL_WARN("growing control_block capacity: {} ", azone);
         char* c = _zone_allocator->get(block_allocator::block_number(azone));
         char* f = _zone_free_list->get(block_allocator::block_number(azone));
         std::memset(c, 0xff, _zone_allocator->block_size());
         std::memset(f, 0xff, _zone_free_list->block_size());
         ++azone;
      }

      azone = _header_ptr->allocated_zones.load(std::memory_order_relaxed);
      while (azone < num_zones)
         _header_ptr->allocated_zones.compare_exchange_strong(azone, num_zones,
                                                              std::memory_order_release);
      _header_ptr->min_alloc_zone.store(azone - 1, std::memory_order_relaxed);
      publish_mlock_stats();
   }

   void control_block_alloc::clear_all()
   {
      auto num_zones = _header_ptr->allocated_zones.load(std::memory_order_relaxed);

      for (uint32_t z = 0; z < num_zones; ++z)
      {
         char* zone_data = _zone_allocator->get(block_allocator::block_number(z));
         char* free_data = _zone_free_list->get(block_allocator::block_number(z));
         std::memset(zone_data, 0, _zone_allocator->block_size());
         std::memset(free_data, 0xff, _zone_free_list->block_size());
      }

      for (uint32_t z = 0; z < num_zones; ++z)
         _header_ptr->zone_alloc_count[z].store(0, std::memory_order_relaxed);
      _header_ptr->total_allocations.store(0, std::memory_order_relaxed);
      _header_ptr->min_alloc_zone.store(0, std::memory_order_relaxed);
      _header_ptr->alloc_seq.store(0, std::memory_order_relaxed);
   }

   void control_block_alloc::release_unreachable()
   {
      auto     num_zones       = _header_ptr->allocated_zones.load(std::memory_order_relaxed);
      uint64_t total_freed     = 0;
      uint64_t total_surviving = 0;

      for (uint32_t z = 0; z < num_zones; ++z)
         _header_ptr->zone_alloc_count[z].store(0, std::memory_order_relaxed);
      _header_ptr->total_allocations.store(0, std::memory_order_relaxed);

      for (uint32_t z = 0; z < num_zones; ++z)
      {
         uint32_t zone_base = z * detail::ptrs_per_zone;

         for (uint32_t i = 0; i < detail::ptrs_per_zone / 64; ++i)
         {
            auto&    flb       = _free_list_base[zone_base / 64 + i];
            uint64_t free_bits = flb.load(std::memory_order_relaxed);
            uint64_t new_free  = free_bits;
            bool     changed   = false;

            for (uint32_t bit = 0; bit < 64; ++bit)
            {
               if (free_bits & (1ULL << bit))
                  continue;

               ptr_address addr(zone_base + i * 64 + bit);
               auto&       cb  = _ptr_base[*addr];
               auto        ref = cb.ref();

               if (ref <= 1)
               {
                  if (ref == 1)
                     cb.release();
                  cb.reset();
                  new_free |= (1ULL << bit);
                  changed = true;
                  total_freed++;
               }
               else
               {
                  cb.release();
                  _header_ptr->zone_alloc_count[z].fetch_add(1, std::memory_order_relaxed);
                  _header_ptr->total_allocations.fetch_add(1, std::memory_order_relaxed);
                  total_surviving++;
               }
            }
            if (changed)
               flb.store(new_free, std::memory_order_relaxed);
         }
      }

      _header_ptr->update_min_zone();
      SAL_WARN("release_unreachable: freed {} unreachable, {} surviving", total_freed,
               total_surviving);
   }

   void control_block_alloc::reset_all_refs()
   {
      auto num_zones = _header_ptr->allocated_zones.load(std::memory_order_relaxed);

      for (uint32_t z = 0; z < num_zones; ++z)
      {
         uint32_t zone_base = z * detail::ptrs_per_zone;
         for (uint32_t i = 0; i < detail::ptrs_per_zone / 64; ++i)
         {
            uint64_t free_bits =
                _free_list_base[zone_base / 64 + i].load(std::memory_order_relaxed);

            for (uint32_t bit = 0; bit < 64; ++bit)
            {
               if (free_bits & (1ULL << bit))
                  continue;

               ptr_address addr(zone_base + i * 64 + bit);
               auto&       cb = _ptr_base[*addr];

               if (cb.ref() > 1)
                  cb.set_ref(1, std::memory_order_relaxed);
            }
         }
      }
   }

}  // namespace sal
