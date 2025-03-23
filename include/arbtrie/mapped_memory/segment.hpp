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
      // meta data about each segment,
      // stored in an array in allocator_state indexed by segment number
      // data is reconstructed on crash recovery and not synced
      struct segment_meta
      {
         /**
          *  When the database is synced, the last_sync_page is advanced and everything from
          * the start of the segment up to the last_sync_page is mprotected as read only. The
          * segment_header::alloc_pos is then moved to the end of the last_sync_page and any 
          * left over space on the last_sync_page is marked as free. This is because the OS
          * can only sync and write protect at the page boundary
          * 
          * rel_virtual_age is a 20bit floating point number that represents the virtual age
          * relative to a base virtual age. The base virtual age is updated from time to time
          * so that the floating point number has greatest precision near the current time and
          * least precision near the oldest segments.
          * 
          * 1. virtual age changes during allocation (when compaction ignores it)
          * 2. virtual age is then used to prioritize unlocking and compaction
          */
         struct state_data
         {
            uint64_t free_space : 26;       // able to store segment_size
            uint64_t unused : 21;           // store the relative virtual age
            uint64_t last_sync_page : 14;   //  segment_size / 4096 byte pages
            uint64_t is_pinned : 1    = 0;  // indicates that the segment is mlocked
            uint64_t is_read_only : 1 = 0;  // indicates that the entire segment is write protected

            static_assert((1 << 26) > segment_size);
            static_assert((1 << 20) >= segment_size / cacheline_size);
            static_assert((1 << 14) > segment_size / 4096);

            uint64_t to_int() const { return std::bit_cast<uint64_t>(*this); }
            explicit state_data(uint64_t x) { *this = std::bit_cast<state_data>(x); }

            state_data& set_last_sync_page(uint32_t page);
            state_data& free(uint32_t size);
            state_data& free_object(uint32_t size);
            state_data& set_pinned(bool s);
            state_data& set_read_only(bool s);

            // Set free_space to a specific value
            state_data& set_free_space(uint32_t size)
            {
               free_space = size;
               return *this;
            }
         };
         static_assert(sizeof(state_data) == sizeof(uint64_t));
         static_assert(std::is_trivially_copyable_v<state_data>);

         state_data get_free_state() const;
         state_data data() const { return get_free_state(); }
         void       free_object(uint32_t size);
         void       free(uint32_t size);
         void       finalize_segment(uint32_t size, uint64_t vage_value);
         void       clear()
         {
            ARBTRIE_INFO("segment_meta::clear");
            _state_data.store(0, std::memory_order_relaxed);
         }
         bool is_pinned() { return get_free_state().is_pinned; }
         void set_read_only(bool s)
         {
            auto expected = _state_data.load(std::memory_order_relaxed);
            auto updated  = state_data(expected).set_read_only(s).to_int();
            while (
                not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
               updated = state_data(expected).set_read_only(s).to_int();
         }
         void set_pinned(bool s)
         {
            auto expected = _state_data.load(std::memory_order_relaxed);
            auto updated  = state_data(expected).set_pinned(s).to_int();
            while (
                not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
               updated = state_data(expected).set_pinned(s).to_int();
         }
         void finalize_compaction()
         {
            auto expected = _state_data.load(std::memory_order_relaxed);

            // Create updated state with reset free_space and rel_virtual_age
            // but preserve other flags like is_pinned, is_read_only, and last_sync_page
            auto updated = state_data(expected).set_free_space(0).to_int();

            while (!_state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
               updated = state_data(expected).set_free_space(0).to_int();
            vage.store(0, std::memory_order_relaxed);
         }
         uint64_t get_last_sync_pos() const;
         void     start_alloc_segment();
         void     set_last_sync_pos(uint64_t pos);
         uint64_t get_vage() const { return vage.load(std::memory_order_relaxed); }
         void     set_vage(uint64_t vage) { this->vage.store(vage, std::memory_order_relaxed); }

         /// the total number of bytes freed by swap
         /// or by being moved to other segments.
         std::atomic<uint64_t> _state_data;

         /// virtual age of the segment, initialized as 1024x the segment_header::_age
         /// and updated with weighted average as data is allocated
         std::atomic<uint64_t> vage;
      };  // segment_meta

      /**
          * Segment metadata is organized by column rather than row to make
          * more effecient scanning when we only care about a single column of
          * data.  
          * 
          * @group segment_metadata Segment Meta Data 
          */
      ///@{
      struct segment_data
      {
         segment_meta meta[max_segment_count];
         /**
             * Get the last synced position for a segment
             */
         inline uint64_t get_last_sync_pos(segment_number segment) const;
         //   inline uint16_t get_first_write_pos(segment_number segment) const;
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
       */
      struct segment
      {
         uint32_t get_alloc_pos() const { return _alloc_pos.load(std::memory_order_relaxed); }
         uint32_t free_space() const { return end() - alloc_ptr(); }

         char*       alloc_ptr() { return data + get_alloc_pos(); }
         const char* alloc_ptr() const { return data + get_alloc_pos(); }
         uint32_t    end_pos() const { return segment_size - segment_footer_size; }
         const char* end() const { return data + end_pos(); }

         void finalize()
         {
            _close_time_usec = arbtrie::get_current_time_ms();
            //             ARBTRIE_WARN("segment::finalize: _close_time_usec: ", _close_time_usec,
            //                         " _alloc_pos (seg_size-footer_size): ",
            //                         _alloc_pos.load(std::memory_order_relaxed), " free_space: ", free_space());
            assert(is_finalized());
         }
         bool is_finalized() const { return _close_time_usec != 0; }

         void set_alloc_pos(uint32_t pos)
         {
            assert(pos <= end_pos());
            _alloc_pos.store(pos, std::memory_order_relaxed);
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
            auto prev = _alloc_pos.fetch_add(size, std::memory_order_relaxed);
            return new (data + prev) T(size, std::forward<decltype(args)>(args)...);
         }
         void unalloc(uint32_t size)
         {
            assert(size == round_up_multiple<64>(size));
            assert(size <= get_alloc_pos());

            auto prev = _alloc_pos.fetch_sub(size, std::memory_order_relaxed);
            assert(prev >= size);
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
            if (page < _first_writable_page)
               return false;
            return pos < (segment_size - segment_footer_size);
         }

         inline uint32_t get_first_write_pos() const
         {
            return _first_writable_page * system_config::os_page_size();
         }

         /**
          * Returns true if the entire segment is read only
          */
         bool is_read_only() const { return _first_writable_page == pages_per_segment; }

         void sync(sync_type st, int top_root_index, id_address top_root)
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

            _last_aheader_pos = alloc_pos;

            uint32_t next_page_pos =
                round_up_multiple<uint32_t>(alloc_pos + 64, system_config::os_page_size());

            if (is_finalized())
               next_page_pos = segment_size;
            else if (next_page_pos >= end_pos())
               finalize();

            // Set size to reach page boundary
            ahead->_nsize    = next_page_pos - alloc_pos;
            ahead->_checksum = XXH3_64bits(data + ahead->_start_checksum_pos, cheksum_size);

            auto old_first_writable_page_pos = uint32_t(_first_writable_page)
                                               << system_config::os_page_size_log2;

            _first_writable_page = next_page_pos >> system_config::os_page_size_log2;
            auto protect_size    = next_page_pos - old_first_writable_page_pos;
            assert(protect_size > 0);
            set_alloc_pos(std::min<uint32_t>(next_page_pos, end_pos()));
            /*   ARBTRIE_INFO(
                "sync: protect_size: ", double(protect_size) / system_config::os_page_size(),
                " old_first_writable_page_pos: ", old_first_writable_page_pos,
                " next_page_pos: ", next_page_pos);
                */
            if (mprotect(data + old_first_writable_page_pos, protect_size, PROT_READ))
            {
               ARBTRIE_ERROR("mprotect failed: ", strerror(errno));
               throw std::runtime_error("mprotect failed");
            }
            assert(is_finalized() ? is_read_only() : true);
         }

         char data[segment_size - segment_footer_size];
         // the next position to allocate data, only
         // modified by the thread that owns this segment and
         // set to uint64_t max when this segment is ready
         // to be marked read only to the seg_allocator, allocator
         // thread must check _first_writable_page before before using
         // _alloc_pos.
        private:
         std::atomic<uint32_t> _alloc_pos = 0;  /// number of bytes allocated from data
        public:
         /// The os_page number of the first page that can be written to
         /// advanced by the sync() thread... sync thread waits until all
         /// modifying threads are done before enforcing the write protection
         uint16_t _first_writable_page = 0;
         uint16_t _session_id          = -1;  ///< the session id that allocated this segment
         uint32_t _seg_sequence        = 0;  ///< the sequence number of this sessions segment alloc
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

      // State data implementations
      inline segment_meta::state_data& segment_meta::state_data::set_last_sync_page(uint32_t page)
      {
         assert(page <= segment_size / system_config::os_page_size());
         last_sync_page = page;
         return *this;
      }

      inline segment_meta::state_data& segment_meta::state_data::free(uint32_t size)
      {
         assert(free_space + size <= segment_size);
         free_space += size;
         assert(free_space >= size);
         return *this;
      }

      inline segment_meta::state_data& segment_meta::state_data::free_object(uint32_t size)
      {
         assert(size > 0);
         assert(free_space + size <= sizeof(mapped_memory::segment));

         free_space += size;
         // ++free_objects;
         return *this;
      }

      inline segment_meta::state_data& segment_meta::state_data::set_pinned(bool s)
      {
         is_pinned = s;
         return *this;
      }

      inline segment_meta::state_data& segment_meta::state_data::set_read_only(bool s)
      {
         is_read_only = s;
         return *this;
      }

      // Segment meta implementations
      inline segment_meta::state_data segment_meta::get_free_state() const
      {
         return state_data(_state_data.load(std::memory_order_relaxed));
      }

      inline void segment_meta::free_object(uint32_t size)
      {
         //if (size == 2496)
         auto expected = _state_data.load(std::memory_order_relaxed);
         //         ARBTRIE_INFO("free_object: size=", size,
         //                      " existing free_space=", segment_meta::state_data(expected).free_space);
         auto updated = state_data(expected).free_object(size).to_int();
         while (not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
            updated = state_data(expected).free_object(size).to_int();
      }

      inline void segment_meta::free(uint32_t size)
      {
         auto expected = _state_data.load(std::memory_order_relaxed);
         auto updated  = state_data(expected).free(size).to_int();
         while (not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
            updated = state_data(expected).free(size).to_int();
      }

      inline void segment_meta::finalize_segment(uint32_t size, uint64_t vage_value)
      {
         auto expected = _state_data.load(std::memory_order_relaxed);

         vage = vage_value;

         auto updated = state_data(expected).free(size).to_int();
         while (not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
            updated = state_data(expected).free(size).to_int();
      }

      inline uint64_t segment_meta::get_last_sync_pos() const
      {
         return state_data(_state_data.load(std::memory_order_relaxed)).last_sync_page *
                system_config::os_page_size();
      }

      inline void segment_meta::start_alloc_segment()
      {
         auto expected = _state_data.load(std::memory_order_relaxed);
         auto updated  = state_data(expected).set_last_sync_page(0).to_int();

         while (not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
            updated = state_data(expected).set_last_sync_page(0).to_int();
      }

      inline void segment_meta::set_last_sync_pos(uint64_t pos)
      {
         auto page_num = round_down_multiple<4096>(pos) / 4096;
         auto expected = _state_data.load(std::memory_order_relaxed);
         auto updated  = state_data(expected).set_last_sync_page(page_num).to_int();
         while (not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
            updated = state_data(expected).set_last_sync_page(page_num).to_int();
      }

      /**
       * Get the last synced position for a segment
       * 
       * @param segment The segment number
       * @return The last synced position in the segment
       */
      inline uint64_t segment_data::get_last_sync_pos(segment_number segment) const
      {
         return meta[segment].get_last_sync_pos();
      }

   }  // namespace mapped_memory
}  // namespace arbtrie