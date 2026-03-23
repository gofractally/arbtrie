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
      _header = std::make_unique<mapping>(dir / "header.bin", sal::access_mode::read_write,
                                          sizeof(detail::ptr_alloc_header));
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

      SAL_ERROR("free_list.bit blocksize: {}", detail::ptrs_per_zone / 8);
      //   _zone_free_list->reserve(1, true);
      //   _zone_allocator->reserve(1, true);
      ensure_capacity(1);
      _ptr_base       = _zone_allocator->get<control_block>(offset_ptr(0));
      _free_list_base = _zone_free_list->get<std::atomic<uint64_t>>(offset_ptr(0));
   }

   control_block_alloc::~control_block_alloc() {}

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
   }

}  // namespace sal
