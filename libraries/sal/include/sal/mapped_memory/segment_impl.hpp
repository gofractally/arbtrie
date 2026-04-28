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

         uint32_t cur_first_writable_page_pos =
             uint32_t(_first_writable_page.load(std::memory_order_relaxed))
             << system_config::os_page_size_log2;

         // Detect the fall-through case: a segment was finalized via
         // allocator_session::finalize_active_segment() (which doesn't run
         // sync()), so it sits in `finalized && !read_only` with
         // alloc_pos == _first_writable_page_pos. We must complete the
         // transition to read-only here, but with a small terminal
         // sync_header — NOT one that claims the entire unused tail as its
         // "size", because:
         //   - record_freed_space(seg, last_aheader) in the caller adds
         //     last_aheader->size() to the segment's freed_space counter.
         //   - If we made that header span the unused tail (~28 MB), we'd
         //     be claiming "this many bytes are reclaimable here" all in
         //     one shot. The unused tail IS reclaimable, but the freed_space
         //     accounting is per-segment and capped by segment_size; mixing
         //     "padding bytes within [0, alloc_pos)" already counted during
         //     the active-segment per-page sync path with "the tail" breaks
         //     bounds and trips the assertion in add_freed_space.
         // So this fall-through writes a 64-byte terminal header (just a
         // marker for compaction's alloc_header chain walk), advances
         // _first_writable_page directly to pages_per_segment, mprotects/
         // msyncs the rest. The caller is responsible for accounting the
         // unused-tail bytes in freed_space exactly once.
         if (alloc_pos <= cur_first_writable_page_pos)
         {
            if (!user_data.empty() && not is_finalized() && not is_read_only())
            {
               constexpr uint32_t marker_size = 64;
               assert(alloc_pos + marker_size <= segment_size - segment_footer_size);
               assert(user_data.size() <= sync_header_user_data_capacity);

               SAL_TRACK_LOCK();
               char* alloc_ptr = data + alloc_pos;
               auto* ahead     = new (alloc_ptr) sync_header(marker_size);
               ahead->set_timestamp(sal::get_current_time_usec());
               ahead->set_prev_aheader_pos(_last_aheader_pos);
               memcpy(ahead->user_data(), user_data.data(), user_data.size());
               ahead->set_user_data_size(user_data.size());
               {
                  auto lah = get_last_aheader();
                  if (lah->type() == header_type::sync_head)
                     ahead->set_start_checksum_pos(_last_aheader_pos + lah->size());
                  auto cksum = alloc_pos + ahead->checksum_offset() -
                               ahead->start_checksum_pos();
                  if (cfg.checksum_on_commit)
                     ahead->set_sync_checksum(
                         XXH3_64bits(data + ahead->start_checksum_pos(), cksum));
               }
               _last_aheader_pos = alloc_pos;
               set_alloc_pos(alloc_pos + marker_size);
               SAL_TRACK_ALLOC(ahead, marker_size, "metadata_sync_header");
               if (st >= sync_type::msync_async)
               {
                  const uint32_t page_size = system_config::os_page_size();
                  const uint32_t page_pos  = alloc_pos & ~(page_size - 1);
                  int mode = st == sync_type::msync_async ? MS_ASYNC : MS_SYNC;
                  if (msync(data + page_pos, page_size, mode))
                  {
                     SAL_ERROR("msync ({}) failed: {}", st, strerror(errno));
                     throw std::runtime_error("msync failed");
                  }
               }
               return marker_size;
            }

            //  1. Not finalized: still being written. Tail is writable. NOOP.
            //  2. Finalized && read_only: terminal state, no work.
            //  3. Finalized && !read_only: write small terminal header,
            //     mprotect + msync the rest, advance fwp to end.
            if (not is_finalized() or is_read_only())
               return 0;

            constexpr uint32_t terminal_size = 64;
            assert(alloc_pos + terminal_size <= segment_size - segment_footer_size);

            // Lock the tracker for the entire sync_header alloc bookkeeping
            // (alloc_pos bump + SAL_TRACK_ALLOC) so concurrent ops can't
            // observe a torn invariant.
            SAL_TRACK_LOCK();
            char* alloc_ptr = data + alloc_pos;
            auto* ahead     = new (alloc_ptr) sync_header(terminal_size);
            ahead->set_timestamp(sal::get_current_time_usec());
            ahead->set_prev_aheader_pos(_last_aheader_pos);
            memcpy(ahead->user_data(), user_data.data(), user_data.size());
            ahead->set_user_data_size(user_data.size());
            {
               auto lah = get_last_aheader();
               if (lah->type() == header_type::sync_head)
                  ahead->set_start_checksum_pos(_last_aheader_pos + lah->size());
               auto cksum = alloc_pos + ahead->checksum_offset() - ahead->start_checksum_pos();
               if (cfg.checksum_on_commit)
                  ahead->set_sync_checksum(
                      XXH3_64bits(data + ahead->start_checksum_pos(), cksum));
            }
            _last_aheader_pos = alloc_pos;
            set_alloc_pos(alloc_pos + terminal_size);
            // Track AFTER alloc_pos bumped so the invariant holds.
            SAL_TRACK_ALLOC(ahead, terminal_size, "fall_through_terminal_header");

            // Make the entire remainder of the segment read-only.
            uint32_t old_fwp_pos = cur_first_writable_page_pos;
            _first_writable_page.store(pages_per_segment, std::memory_order_relaxed);
            uint64_t protect_size = segment_size - old_fwp_pos;

            if (st == sync_type::none)
               return protect_size;

            if (mprotect(data + old_fwp_pos, protect_size, PROT_READ))
            {
               SAL_ERROR("mprotect failed: {}", strerror(errno));
               throw std::runtime_error("mprotect failed");
            }
            if (st >= sync_type::msync_async)
            {
               int mode = st == sync_type::msync_async ? MS_ASYNC : MS_SYNC;
               if (msync(data + old_fwp_pos, protect_size, mode))
               {
                  SAL_ERROR("msync ({}) failed: {}", st, strerror(errno));
                  throw std::runtime_error("msync failed");
               }
            }
            assert(is_finalized() ? is_read_only() : true);
            return protect_size;
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
         {
            // Atomic {alloc_pos bump + mark_alloc} for the verifier.
            SAL_TRACK_LOCK();
            set_alloc_pos(new_alloc_pos);
            SAL_TRACK_ALLOC(ahead, asize, "active_sync_header");
         }

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
