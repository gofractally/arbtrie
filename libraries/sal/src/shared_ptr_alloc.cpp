#include <hash/lehmer64.h>
#include <sal/debug.hpp>
#include <sal/mapping.hpp>
#include <sal/min_index.hpp>
#include <sal/shared_ptr_alloc.hpp>

namespace sal
{
   shared_ptr_alloc::shared_ptr_alloc(const std::filesystem::path& dir) : _dir(dir)
   {
      std::filesystem::create_directories(dir);
      _dir            = dir;
      _header         = std::make_unique<mapping>(dir / "header.bin", sal::access_mode::read_write,
                                                  sizeof(detail::ptr_alloc_header));
      _header_ptr     = reinterpret_cast<detail::ptr_alloc_header*>(_header->data());
      _zone_allocator = std::make_unique<block_allocator>(dir / "zone.bin", detail::zone_size_bytes,
                                                          detail::max_allocated_zones);
      _zone_free_list = std::make_unique<block_allocator>(
          dir / "free_list.bin", 1 << 19 /* 32M /8 byte per ptr/ 8 bit per byte*/,
          detail::max_allocated_zones);
      _zone_free_list->reserve(1, true);
      _zone_allocator->reserve(1, true);
      _ptr_base       = _zone_allocator->get<shared_ptr>(offset_ptr(0));
      _free_list_base = _zone_free_list->get<std::atomic<uint64_t>>(offset_ptr(0));
   }

   shared_ptr_alloc::~shared_ptr_alloc() {}

   void shared_ptr_alloc::ensure_capacity(uint32_t req_zones)
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
      while (azone < num_zones)
      {
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
   }

}  // namespace sal
