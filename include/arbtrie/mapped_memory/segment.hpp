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
            uint64_t unused : 20;           // store the relative virtual age
            uint64_t last_sync_page : 14;   //  segment_size / 4096 byte pages
            uint64_t is_alloc : 1     = 0;  // the segment is a session's private alloc area
            uint64_t is_pinned : 1    = 0;  // indicates that the segment is mlocked
            uint64_t is_read_only : 1 = 0;  // indicates that the entire segment is write protected

            static_assert((1 << 26) > segment_size);
            static_assert((1 << 20) >= segment_size / cacheline_size);
            static_assert((1 << 14) > segment_size / os_page_size);

            uint64_t to_int() const { return std::bit_cast<uint64_t>(*this); }
            explicit state_data(uint64_t x) { *this = std::bit_cast<state_data>(x); }

            state_data& set_last_sync_page(uint32_t page);
            state_data& free(uint32_t size);
            state_data& free_object(uint32_t size);
            state_data& set_alloc(bool s);
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
         void       clear() { _state_data.store(0, std::memory_order_relaxed); }
         bool       is_alloc() { return get_free_state().is_alloc; }
         bool       is_pinned() { return get_free_state().is_pinned; }
         void       set_pinned(bool s)
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

            // Safety check: is_alloc should be false when finalizing compaction
            assert(not state_data(expected).is_alloc);

            // Create updated state with reset free_space and rel_virtual_age
            // but preserve other flags like is_pinned, is_read_only, and last_sync_page
            auto updated = state_data(expected).set_free_space(0).to_int();

            while (!_state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
               updated = state_data(expected).set_free_space(0).to_int();
            vage.store(0, std::memory_order_relaxed);
         }
         void     set_alloc_state(bool a);
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
         inline uint16_t get_first_write_pos(segment_number segment) const;
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
         char data[segment_size - segment_footer_size];

         uint32_t get_alloc_pos() const { return _alloc_pos.load(std::memory_order_relaxed); }
         uint32_t free_space() const { return end() - alloc_ptr(); }

         char*       alloc_ptr() { return data + get_alloc_pos(); }
         const char* alloc_ptr() const { return data + get_alloc_pos(); }
         uint32_t    end_pos() const { return segment_size - segment_footer_size; }
         const char* end() const { return data + end_pos(); }

         void finalize()
         {
            _close_time_usec = arbtrie::get_current_time_ms();
            _alloc_pos.store(segment_size - segment_footer_size, std::memory_order_relaxed);
         }
         bool is_finalized() const { return _close_time_usec != 0; }

         void set_alloc_pos(uint32_t pos) { _alloc_pos.store(pos, std::memory_order_relaxed); }

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
            _first_unsynced_page = 0;
            _session_id          = -1;
            _seg_sequence        = -1;
         }

         /**
          * Calls mprotect() with PROT_READ on the set of pages
          * [_first_unsynced_page, _alloc_pos / os_page_size] and will advance the
          * alloc position to the start of the next page if necessary and write
          */
         void protect();

         /**
          * Calls protect() first to maintain the invariant, then 
          * calls msync() with the given flags on the set of pages
          * [_first_unsynced_page, _alloc_pos / os_page_size] and will advance the
          * alloc position to the start of the next page if necessary.
          */
         void sync(sync_type sync = sync_type::async);

         /**
          * We can only modify data in the range [_first_writable_page*os_page_size, _alloc_pos)
          */
         bool can_modify(uint32_t pos) const
         {
            if (pos >= get_alloc_pos())
               return false;
            const auto page = pos / os_page_size;
            if (page < _first_writable_page.load(std::memory_order_relaxed))
               return false;
            return pos < (segment_size - segment_footer_size);
         }

         inline uint32_t get_first_write_pos() const
         {
            return _first_writable_page.load(std::memory_order_acquire) * os_page_size;
         }

         bool is_read_only() const
         {
            return _first_writable_page.load(std::memory_order_relaxed) ==
                   segment_size / os_page_size;
         }

         // the next position to allocate data, only
         // modified by the thread that owns this segment and
         // set to uint64_t max when this segment is ready
         // to be marked read only to the seg_allocator, allocator
         // thread must check _first_writable_page before before using
         // _alloc_pos.
         std::atomic<uint32_t> _alloc_pos = 0;  /// number of bytes allocated from data

         /// The os_page number of the first page that can be written to
         /// advanced by the sync() thread... sync thread waits until all
         /// modifying threads are done before enforcing the write protection
         std::atomic<uint16_t> _first_writable_page = 0;
         /// The os page number of the first page that is not synced to disk
         std::atomic<uint16_t> _first_unsynced_page = 0;

         uint32_t _session_id;       ///< the session id that allocated this segment
         uint32_t _seg_sequence;     ///< the sequence number of this sessions segment alloc
         uint64_t _open_time_usec;   ///< unix time in microseconds this segment started writing
         uint64_t _close_time_usec;  ///< unix time in microseconds this segment was closed

         // the provider thread assigns sequence numbers to segments as they are
         // prepared, -1 means the segment is in the free list and not used
         uint32_t _provider_sequence;

         // tracks how much of the segment has been checksumed and
         // recorded in the allocator_header::checksum fields formed by the
         // linked list of _last_aheader_pos
         std::atomic<uint32_t> _checksum_pos     = 0;
         std::atomic<uint32_t> _last_aheader_pos = 0;

         // Tracks accumulated virtual age during allocation
         size_weighted_age _vage_accumulator;
      };
      static_assert(sizeof(segment) == segment_size);

      // State data implementations
      inline segment_meta::state_data& segment_meta::state_data::set_last_sync_page(uint32_t page)
      {
         assert(page <= segment_size / os_page_size);
         last_sync_page = page;
         return *this;
      }

      inline segment_meta::state_data& segment_meta::state_data::free(uint32_t size)
      {
         assert(free_space + size <= segment_size);
         ARBTRIE_INFO("free: size=", size);
         free_space += size;
         assert(free_space >= size);
         return *this;
      }

      inline segment_meta::state_data& segment_meta::state_data::free_object(uint32_t size)
      {
         assert(size > 0);
         assert(free_space + size <= sizeof(mapped_memory::segment::data));

         free_space += size;
         // ++free_objects;
         return *this;
      }

      inline segment_meta::state_data& segment_meta::state_data::set_alloc(bool s)
      {
         is_alloc = s;
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
         ARBTRIE_INFO("free_object: size=", size,
                      " existing free_space=", segment_meta::state_data(expected).free_space);
         auto updated = state_data(expected).free_object(size).to_int();
         while (not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
            updated = state_data(expected).free_object(size).to_int();
      }

      inline void segment_meta::free(uint32_t size)
      {
         ARBTRIE_INFO("free: size=", size);
         auto expected = _state_data.load(std::memory_order_relaxed);
         auto updated  = state_data(expected).free(size).to_int();
         while (not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
            updated = state_data(expected).free(size).to_int();
      }

      inline void segment_meta::finalize_segment(uint32_t size, uint64_t vage_value)
      {
         auto expected = _state_data.load(std::memory_order_relaxed);

         assert(state_data(expected).is_alloc);
         vage = vage_value;

         auto updated = state_data(expected).free(size).set_alloc(false).to_int();
         while (not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
            updated = state_data(expected).free(size).set_alloc(false).to_int();
      }

      inline void segment_meta::set_alloc_state(bool a)
      {
         auto expected = _state_data.load(std::memory_order_relaxed);
         auto updated  = state_data(expected).set_alloc(a).to_int();
         while (not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
            updated = state_data(expected).set_alloc(a).to_int();
      }

      inline uint64_t segment_meta::get_last_sync_pos() const
      {
         return state_data(_state_data.load(std::memory_order_relaxed)).last_sync_page *
                os_page_size;
      }

      inline void segment_meta::start_alloc_segment()
      {
         auto expected = _state_data.load(std::memory_order_relaxed);
         assert(not state_data(expected).is_alloc);
         auto updated = state_data(expected).set_last_sync_page(0).set_alloc(true).to_int();
         assert(state_data(updated).is_alloc);

         while (not _state_data.compare_exchange_weak(expected, updated, std::memory_order_relaxed))
            updated = state_data(expected).set_last_sync_page(0).set_alloc(true).to_int();
         assert(state_data(updated).is_alloc);
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