#pragma once
#include <arbtrie/config.hpp>
#include <arbtrie/debug.hpp>
#include <arbtrie/mapping.hpp>
#include <arbtrie/node_header.hpp>
#include <arbtrie/padded_atomic.hpp>
#include <arbtrie/size_weighted_age.hpp>
#include <cassert>
#include <cstdint>

namespace arbtrie
{
   namespace mapped_memory
   {
      /// meta data about each segment used by the compactor to
      /// quickly determine which segments are eligible for compaction and
      /// to track data about the segments once segments are read-only.
      ///
      /// - stored in an array in allocator_state indexed by segment number
      /// - data is reconstructed on crash recovery and not synced
      struct segment_meta
      {
        private:
         /// tracks the virtual age of the segment, impacting its priority for
         /// pinning and compaction, updated only by the session thread that
         /// owns the segment when the segment becomes entirely read only and
         /// therefore eligible for compaction. From this point on it is only
         /// read by the compaction thread.
         std::atomic<uint64_t> vage;

         /// tracks the space that could be reclaimed if compacted
         /// written to by any thread objects are released or moved enabling
         /// the space to be reclaimed if compacted.
         std::atomic<uint32_t> freed_space;

         enum segment_flags : uint32_t
         {
            read_only = 1 << 0,  // segment entirely read only (eligable for compaction)
            pinned    = 1 << 1,  // segment pinned in RAM
            active =
                1 << 2,  // segment active and being used by a session (at least partially writable)
            pending = 1 << 3,  // segment compacted, waiting on read lock release
            free    = 1 << 4,  // segment free and ready for reuse
            queued  = 1 << 5,  // segment in provider queue waiting for session to claim
         };

         /// tracks if the segment is read only, set by the session thread when the
         /// sync() method has moved the _first_writable_page to the end of the segment
         /// cleared by the provider thread when the segment is prepared for reuse.
         std::atomic<uint32_t> flags;

        public:
         void added_to_provider_queue()
         {
            assert(not(flags.load(std::memory_order_relaxed) & read_only));
            uint32_t new_flags = (flags.load(std::memory_order_relaxed) & pinned) | queued;
            flags.store(new_flags, std::memory_order_relaxed);
         }
         void added_to_read_lock_queue()
         {
            assert(flags.load(std::memory_order_relaxed) & read_only);
            // don't change pinned or read_only flags
            uint32_t new_flags =
                (flags.load(std::memory_order_relaxed) & pinned | read_only) | pending;
            flags.store(new_flags, std::memory_order_relaxed);
         }
         void added_to_free_list()
         {
            assert(not(flags.load(std::memory_order_relaxed) & (active | queued)));
            assert(flags.load(std::memory_order_relaxed) & read_only);
            uint32_t new_flags = (flags.load(std::memory_order_relaxed) & pinned) | free;
            flags.store(new_flags, std::memory_order_relaxed);
         }
         /// transition to active from queued
         void allocated_by_session()
         {
            assert(not(flags.load(std::memory_order_relaxed) & read_only));
            assert(flags.load(std::memory_order_relaxed) & queued);
            uint32_t new_flags = (flags.load(std::memory_order_relaxed) & pinned) | active;
            flags.store(new_flags, std::memory_order_relaxed);
            freed_space.store(0, std::memory_order_relaxed);
         }

         void add_freed_space(uint32_t size)
         {
            assert(size + freed_space.load(std::memory_order_relaxed) <= segment_size);
            freed_space.fetch_add(size, std::memory_order_relaxed);
         }

         /// stores the age and marks it as ready only
         void prepare_for_compaction(uint64_t vage_value)
         {
            vage.store(vage_value, std::memory_order_relaxed);
            flags.store((flags.load(std::memory_order_relaxed) & pinned) | read_only,
                        std::memory_order_relaxed);
         }
         void set_pinned(bool is_pinned)
         {
            if (is_pinned)
               flags.fetch_or(pinned, std::memory_order_relaxed);
            else
               flags.fetch_and(~pinned, std::memory_order_relaxed);
         }
         // compactor may only consider segments that are read only and not in one of the other states
         bool may_compact() const
         {
            return (flags.load(std::memory_order_relaxed) & ~pinned) == read_only;
         }
         bool     is_read_only() const { return flags.load(std::memory_order_relaxed) & read_only; }
         bool     is_pinned() const { return flags.load(std::memory_order_relaxed) & pinned; }
         uint32_t get_freed_space() const { return freed_space.load(std::memory_order_relaxed); }
         uint64_t get_vage() const { return vage.load(std::memory_order_relaxed); }
      };  // segment_meta

      /**
       * Meta data about every possible segment.
       */
      struct segment_data
      {
         bool may_compact(segment_number segment) const { return meta[segment].may_compact(); }
         void added_to_free_segments(segment_number segment)
         {
            //            ARBTRIE_INFO("free: ", segment);
            meta[segment].added_to_free_list();
         }
         void added_to_provider_queue(segment_number segment)
         {
            //           ARBTRIE_INFO("provider q: ", segment);
            meta[segment].added_to_provider_queue();
         }
         template <typename T>
         void add_freed_space(segment_number segment, const T* obj)
         {
            meta[segment].add_freed_space(obj->_nsize);
         }

         // initial condition of a new segment, given a starting age
         void added_to_read_lock_queue(segment_number segment)
         {
            //          ARBTRIE_INFO("read lock q: ", segment);
            meta[segment].added_to_read_lock_queue();
         }
         void prepare_for_compaction(segment_number segment, uint64_t vage)
         {
            //         ARBTRIE_INFO("may compact: ", segment);
            meta[segment].prepare_for_compaction(vage);
            assert(meta[segment].may_compact());
         }
         void allocated_by_session(segment_number segment)
         {
            //        ARBTRIE_INFO("allocated: ", segment);
            meta[segment].allocated_by_session();
         }
         uint32_t get_freed_space(segment_number segment) const
         {
            return meta[segment].get_freed_space();
         }
         uint64_t get_vage(segment_number segment) const { return meta[segment].get_vage(); }
         bool is_read_only(segment_number segment) const { return meta[segment].is_read_only(); }
         bool is_pinned(segment_number segment) const { return meta[segment].is_pinned(); }
         void set_pinned(segment_number segment, bool pinned) { meta[segment].set_pinned(pinned); }

        private:
         segment_meta meta[max_segment_count];
      };

      static constexpr size_t segment_footer_size = 64;

      /**
       * The main unit of memory allocation, can be thought of as a "super page" because
       * it is at this resolution that memory is mlocked, madvised, and it determines the
       * largest size that can be allocated. 
       * 
       * Data is written in append only fashion, and once a transaction is committed,
       * everything that has been written becomes mprotected as read only. At this point
       * the user can also call sync() to flush the data to disk.
       * 
       * It is an invariant that _first_unsynced_page <= _first_writable_page <= _alloc_pos / os_page_size
       * because we never want to modify data that has already been synced to disk.
       * 
       * The segment is designed to hold a sequence of object_header derived objects where
       * each object is aligned on cpu cache line boundaries. Each object_header contains a
       * type and _nsize field which allows us to navigate the objects in order through the
       * segment. 
       * 
       * # Life Cycle
       * 
       * ```mermaid
       * graph LR
       *   A[new/free_list] --> B[provider_queues]
       *   B --> C[session_alloc]
       *   C --> D[read_only]
       *   D --> E[compacting]
       *   E --> F[pending_recycle]
       *   F --> A
       * ```
       */
      struct segment
      {
         uint32_t get_alloc_pos() const { return _alloc_pos; }

         /// @brief  the amount of space available for allocation
         uint32_t free_space() const { return end() - alloc_ptr(); }

         char*       alloc_ptr() { return data + get_alloc_pos(); }
         const char* alloc_ptr() const { return data + get_alloc_pos(); }
         uint32_t    end_pos() const { return segment_size - segment_footer_size; }
         const char* end() const { return data + end_pos(); }

         /// @brief  records the time allocation was completed
         void finalize()
         {
            _close_time_usec = arbtrie::get_current_time_ms();
            //             ARBTRIE_WARN("segment::finalize: _close_time_usec: ", _close_time_usec,
            //                         " _alloc_pos (seg_size-footer_size): ",
            //                         _alloc_pos.load(std::memory_order_relaxed), " free_space: ", free_space());
            assert(is_finalized());
         }

         /// @brief  returns true if finalize() has been called setting the _close_time_usec
         bool is_finalized() const { return _close_time_usec != 0; }

         void set_alloc_pos(uint32_t pos)
         {
            assert(pos <= end_pos());
            _alloc_pos = pos;  //.store(pos, std::memory_order_relaxed);
         }

         /// @brief  helper to convert ptr to pos
         uint32_t set_alloc_ptr(char* ptr)
         {
            auto idx = ptr - data;
            set_alloc_pos(idx);
            return idx;
         }

         segment()
         {
            _alloc_pos           = 0;
            _first_writable_page = 0;
            _session_id          = -1;
            _seg_sequence        = -1;
         }
         bool can_alloc(uint32_t size) const
         {
            assert(size == round_up_multiple<64>(size));
            // leave enough room for the ending allocator header
            return get_alloc_pos() + size <= sizeof(data) - 64;
         }
         template <typename T>
         T* alloc(uint32_t size, auto&&... args)
         {
            assert(can_alloc(size));
            auto result = new (data + _alloc_pos) T(size, std::forward<decltype(args)>(args)...);
            _alloc_pos += size;
            return result;
         }
         void unalloc(uint32_t size)
         {
            assert(size == round_up_multiple<64>(size));
            assert(size <= get_alloc_pos());

            assert(_alloc_pos >= size);
            _alloc_pos -= size;
         }

         /**
          * We can only modify data in the range [_first_writable_page*os_page_size, _alloc_pos)
          */
         bool can_modify(uint32_t pos) const
         {
            if (pos >= get_alloc_pos())
               return false;
            //const auto page = pos / system_config::os_page_size();
            assert(pos / system_config::os_page_size() == pos >> system_config::os_page_size_log2);
            const auto page = pos >> system_config::os_page_size_log2;
            if (page < _first_writable_page.load(std::memory_order_relaxed))
               return false;
            return pos < (segment_size - segment_footer_size);
         }

         inline uint32_t get_first_write_pos() const
         {
            return _first_writable_page.load(std::memory_order_relaxed) *
                   system_config::os_page_size();
         }

         /**
          * Returns true if the entire segment is read only
          */
         bool is_read_only() const
         {
            return _first_writable_page.load(std::memory_order_relaxed) == pages_per_segment;
         }

         /// @return the total bytes synced/written by this session
         uint64_t sync(sync_type             st,
                       int                   top_root_index,
                       id_address            top_root,
                       const runtime_config& cfg)
         {
            auto  alloc_pos          = get_alloc_pos();
            char* alloc_ptr          = data + alloc_pos;
            auto  ahead              = new (alloc_ptr) allocator_header;
            ahead->_time_stamp_ms    = arbtrie::get_current_time_ms();
            ahead->_top_node_update  = top_root_index;
            ahead->_top_node_id      = top_root;
            ahead->_prev_aheader_pos = _last_aheader_pos;
            auto lah                 = get_last_aheader();

            if (lah->is_allocator_header())
               ahead->_start_checksum_pos = _last_aheader_pos + lah->_nsize;

            auto cheksum_size =
                alloc_pos + offsetof(allocator_header, _checksum) - ahead->_start_checksum_pos;

            assert(alloc_pos <= segment_size - 64);
            _last_aheader_pos = alloc_pos;

            uint32_t next_page_pos =
                round_up_multiple<uint32_t>(alloc_pos + 64, system_config::os_page_size());

            if (is_finalized())
               next_page_pos = segment_size;
            else if (next_page_pos >= end_pos())
               finalize();

            auto new_alloc_pos = std::min<uint32_t>(next_page_pos, end_pos());

            // Set size to reach page boundary
            ahead->_nsize = new_alloc_pos - alloc_pos;
            if (cfg.checksum_commits)
               ahead->_checksum = XXH3_64bits(data + ahead->_start_checksum_pos, cheksum_size);

            auto old_first_writable_page_pos =
                uint32_t(_first_writable_page.load(std::memory_order_relaxed))
                << system_config::os_page_size_log2;

            _first_writable_page.store(next_page_pos >> system_config::os_page_size_log2,
                                       std::memory_order_relaxed);
            uint64_t protect_size = next_page_pos - old_first_writable_page_pos;
            assert(protect_size > 0);
            set_alloc_pos(new_alloc_pos);
            /*   ARBTRIE_INFO(
                "sync: protect_size: ", double(protect_size) / system_config::os_page_size(),
                " old_first_writable_page_pos: ", old_first_writable_page_pos,
                " next_page_pos: ", next_page_pos);
                */
            if (st == sync_type::none)
               return protect_size;

            if (mprotect(data + old_first_writable_page_pos, protect_size, PROT_READ))
            {
               ARBTRIE_ERROR("mprotect failed: ", strerror(errno));
               throw std::runtime_error("mprotect failed");
            }
            if (st >= sync_type::msync_async)
            {
               int mode = st == sync_type::msync_async ? MS_ASYNC : MS_SYNC;
               if (msync(data + old_first_writable_page_pos, protect_size, mode))
               {
                  ARBTRIE_ERROR("msync (", st, ") failed: ", strerror(errno));
                  throw std::runtime_error("msync failed");
               }
            }
            assert(is_finalized() ? is_read_only() : true);
            return protect_size;
         }

         char data[segment_size - segment_footer_size];
         // the next position to allocate data, only
         // modified by the thread that owns this segment and
         // set to uint64_t max when this segment is ready
         // to be marked read only to the seg_allocator, allocator
         // thread must check _first_writable_page before before using
         // _alloc_pos.
        private:
         uint32_t _alloc_pos = 0;  /// number of bytes allocated from data
        public:
         /// The os_page number of the first page that can be written to
         /// advanced by the sync() thread... sync thread waits until all
         /// modifying threads are done before enforcing the write protection
         std::atomic<uint16_t> _first_writable_page = 0;
         static_assert(std::atomic<uint16_t>::is_always_lock_free,
                       "std::atomic<uint16_t> must be lock free");
         static_assert(sizeof(std::atomic<uint16_t>) == sizeof(uint16_t),
                       "std::atomic<uint16_t> must not add overhead");
         uint16_t _session_id      = -1;  ///< the session id that allocated this segment
         uint32_t _seg_sequence    = 0;   ///< the sequence number of this sessions segment alloc
         uint64_t _open_time_usec  = 0;  ///< unix time in microseconds this segment started writing
         uint64_t _close_time_usec = 0;  ///< unix time in microseconds this segment was closed

         // the provider thread assigns sequence numbers to segments as they are
         // prepared, -1 means the segment is in the free list and not used
         uint32_t _provider_sequence = 0;
         uint32_t _last_aheader_pos  = 0;
         uint64_t _unused;

         const allocator_header* get_last_aheader() const
         {
            return (const allocator_header*)(data + _last_aheader_pos);
         }
         // Tracks accumulated virtual age during allocation
         size_weighted_age _vage_accumulator;
      };  // __attribute((packed));
      static_assert(sizeof(segment) == segment_size);

   }  // namespace mapped_memory
}  // namespace arbtrie