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
      uint64_t segment::sync(sync_type st, const runtime_config& cfg, std::span<char> user_data)
      {
         auto alloc_pos = get_alloc_pos();

         // Nothing-to-sync guard. If alloc_pos hasn't advanced past the
         // current _first_writable_page, the caller is asking us to sync
         // a segment that's already fully synced (typically: a finalized
         // segment popped from the dirty queue whose tail was already
         // mprotect'd to PROT_READ on a prior sync). The placement-new
         // below would write a sync_header at `data + alloc_pos` — which
         // is now in a read-only page → EXC_BAD_ACCESS / SIGBUS.
         //
         // The active-segment caller (`allocator_session::sync`) checks
         // this externally via `not is_finalized() and alloc_pos >
         // first_write_pos`, but the dirty-queue drain loop does not.
         // Guard here so any caller is safe.
         uint32_t cur_first_writable_page_pos =
             uint32_t(_first_writable_page.load(std::memory_order_relaxed))
             << system_config::os_page_size_log2;
         if (alloc_pos <= cur_first_writable_page_pos)
         {
            // No new data since the previous sync. There are three sub-cases
            // and only one of them needs work:
            //
            //  1. Not finalized: caller is asking us to sync something that
            //     is already fully synced through `_first_writable_page`.
            //     The tail is still writable and may receive more
            //     allocations later. Nothing to do.
            //
            //  2. Finalized AND already read-only (alloc_pos < fwp_pos,
            //     fwp == pages_per_segment): terminal state, the previous
            //     sync wrote the terminal sync_header and mprotected the
            //     tail. Nothing to do.
            //
            //  3. Finalized but NOT read-only (alloc_pos == fwp_pos,
            //     fwp < pages_per_segment): allocator_session::
            //     finalize_active_segment() finalized this segment without
            //     running it through sync(), so it never got its terminal
            //     sync_header and its tail is still writable. Fall through
            //     to the normal finalized-sync path, which will:
            //       - write the sync_header at data + alloc_pos (safe:
            //         alloc_pos is the start of the first writable page),
            //       - advance _first_writable_page to pages_per_segment,
            //       - advance _alloc_pos to end_pos(),
            //       - mprotect/msync the tail.
            //     Without this fall-through, the segment stays in
            //     finalized && !read_only forever and the next session
            //     drain trips `allocator_session::sync`'s post-condition.
            //     This also matters for compaction safety: compaction
            //     scans read-only segments up to shead->end(), so the
            //     terminal sync_header must actually be on disk before
            //     read-only is allowed to be true.
            if (not is_finalized() or is_read_only())
               return 0;
            // else: fall through to the normal finalized-sync path below.
         }

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
         memcpy(ahead->user_data(), user_data.data(), user_data.size());
         ahead->set_user_data_size(user_data.size());
         auto lah = get_last_aheader();

         if (lah->type() == header_type::sync_head)
            ahead->set_start_checksum_pos(_last_aheader_pos + lah->size());

         auto cheksum_size = alloc_pos + ahead->checksum_offset() - ahead->start_checksum_pos();

         assert(alloc_pos <= segment_size - 64);
         _last_aheader_pos = alloc_pos;

         // TODO: maybe restore this once we are validating it in recovery
         if (cfg.checksum_on_commit)
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
