#pragma once
#include <sal/alloc_header.hpp>
#include <sal/mapped_memory/segment.hpp>

namespace sal
{
   namespace mapped_memory
   {
      /// @return the total bytes synced/written by this session
      /// @tparam UserData the type of the user data to be stored in the sync header, must be
      /// POD and <= 44 bytes
      template <typename UserData>
      uint64_t segment::sync(sync_type st, const runtime_config& cfg, UserData user_data)
      {
         auto  alloc_pos = get_alloc_pos();
         char* alloc_ptr = data + alloc_pos;

         uint32_t next_page_pos =
             ucc::round_up_multiple<uint32_t>(alloc_pos + 64, system_config::os_page_size());

         if (is_finalized())
            next_page_pos = segment_size;
         else if (next_page_pos >= end_pos())
            finalize();

         auto new_alloc_pos = std::min<uint32_t>(next_page_pos, end_pos());

         // Set size to reach page boundary
         auto asize = new_alloc_pos - alloc_pos;

         auto ahead = new (alloc_ptr) sync_header(asize);
         ahead->set_timestamp(sal::get_current_time_usec());
         ahead->set_prev_aheader_pos(_last_aheader_pos);
         memcpy(ahead->user_data(), &user_data, sizeof(UserData));
         ahead->set_user_data_size(sizeof(UserData));
         auto lah = get_last_aheader();

         if (lah->type() == header_type::sync_head)
            ahead->set_start_checksum_pos(_last_aheader_pos + lah->size());

         auto cheksum_size = alloc_pos + ahead->checksum_offset() - ahead->start_checksum_pos();

         assert(alloc_pos <= segment_size - 64);
         _last_aheader_pos = alloc_pos;

         // TODO: maybe restore this once we are validating it in recovery
         if (cfg.checksum_commits)
            ahead->set_sync_checksum(XXH3_64bits(data + ahead->start_checksum_pos(), cheksum_size));

         auto old_first_writable_page_pos =
             uint32_t(_first_writable_page.load(std::memory_order_relaxed))
             << system_config::os_page_size_log2;

         _first_writable_page.store(next_page_pos >> system_config::os_page_size_log2,
                                    std::memory_order_relaxed);
         uint64_t protect_size = next_page_pos - old_first_writable_page_pos;
         assert(protect_size > 0);
         set_alloc_pos(new_alloc_pos);

         if (st == sync_type::none)
            return protect_size;

         if (mprotect(data + old_first_writable_page_pos, protect_size, PROT_READ))
         {
            SAL_ERROR("mprotect failed: {}", strerror(errno));
            throw std::runtime_error("mprotect failed");
         }
         if (st >= sync_type::msync_async)
         {
            int mode = st == sync_type::msync_async ? MS_ASYNC : MS_SYNC;
            if (msync(data + old_first_writable_page_pos, protect_size, mode))
            {
               SAL_ERROR("msync ({}) failed: {}", st, strerror(errno));
               throw std::runtime_error("msync failed");
            }
         }
         assert(is_finalized() ? is_read_only() : true);
         return protect_size;
      }
   }  // namespace mapped_memory
}  // namespace sal
