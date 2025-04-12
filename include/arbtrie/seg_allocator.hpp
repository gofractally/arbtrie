#pragma once
#include <arbtrie/circular_buffer.hpp>
#include <arbtrie/config.hpp>
#include <arbtrie/debug.hpp>
#include <arbtrie/id_alloc.hpp>
#include <arbtrie/mapped_memory.hpp>
#include <arbtrie/mapping.hpp>
#include <arbtrie/node_header.hpp>
#include <arbtrie/node_meta.hpp>
#include <arbtrie/padded_atomic.hpp>
#include <arbtrie/rdtsc.hpp>
#include <arbtrie/seg_alloc_dump.hpp>
#include <arbtrie/seg_alloc_session.hpp>
#include <arbtrie/segment_thread.hpp>
#include <arbtrie/sync_lock.hpp>
#include <sal/block_allocator.hpp>

/**
 *  @file seg_allocator.hpp
 */
namespace arbtrie
{
   class seg_alloc_session;

   /**
    * @brief Responsible for multi-threaded memory managment 
    * 
    * 1. Grows the database in large 32MB blocks, called segments.
    * 2. Multiple threads can allocate memory using @ref seg_alloc_session
    *    - each thread has an independent allocation segment so there is no
    *      contention for the global allocator
    * 3. The allocator moves objects around to minimize fragmentation 
    *    and optimize the location objects are stored based upon
    *    the frequency of access. 
    * 4. The allocator mlocks() a configured number of segments to minimize swapping. 
    * 
    * Internally the seg_allocator utilizes the id_alloc to assigne object ids, which
    * are used to enable relocatable pointers. 
    */
   class seg_allocator
   {
     public:
      // only 64 bits in bitfield used to allocate sessions
      // only really require 1 per thread
      static const uint32_t max_session_count = 64;

      seg_allocator(std::filesystem::path dir, runtime_config cfg);
      ~seg_allocator();

      void stop_threads();
      void start_threads();

      /**
       * Debugging methods
       * @group Debugging
       */
      ///@{
      uint64_t       count_ids_with_refs() { return _id_alloc.used(); }
      seg_alloc_dump dump();
      ///@}

      /**
       * @group Configuration Methods
       */
      ///@{
      bool config_validate_checksum_on_compact() const;
      bool config_update_checksum_on_compact() const;
      bool config_update_checksum_on_modify() const;
      ///@}

      void fsync(bool full = false) { _block_alloc.fsync(full); }

      seg_alloc_session start_session() { return seg_alloc_session(*this, alloc_session_num()); }

      /**
       * Stops all background threads (compactor, read_bit_decay, segment_provider)
       * @return true if any threads were running and stopped
       */
      bool stop_background_threads();

      /**
       * Starts all background threads that were previously running
       * @param force_start if true, starts all threads regardless of previous state
       * @return true if any threads were started
       */
      bool start_background_threads(bool force_start = false);

      // Return struct for segment read statistics
      struct stats_result
      {
         stats_result() { memset(this, 0, sizeof(stats_result)); }
         uint32_t nodes_with_read_bit;
         uint64_t total_bytes;
         uint32_t total_objects;
         uint32_t non_value_nodes;          // Count of non-value nodes for average calculation
         uint32_t index_cline_counts[257];  // Histogram of actual cacheline hits [0-256+]
         uint32_t cline_delta_counts[257];  // Histogram of delta between actual and ideal [0-256+]
      };

     private:
      friend class database;
      void release_unreachable();
      void reset_meta_nodes(recover_args args);
      void reset_reference_counts();

      friend class seg_alloc_session;
      friend class read_lock;
      friend class modify_lock;
      friend class object_ref;

      void mlock_pinned_segments();

      /**
       * Utilized by seg_alloc_session 
       */
      /// @{
      mapped_memory::segment* get_segment(segment_number seg) noexcept
      {
         return reinterpret_cast<mapped_memory::segment*>(_block_alloc.get(block_number(seg)));
      }
      const mapped_memory::segment* get_segment(segment_number seg) const noexcept
      {
         return reinterpret_cast<const mapped_memory::segment*>(
             _block_alloc.get(block_number(seg)));
      }

      uint32_t alloc_session_num();
      void     release_session_num(uint32_t sn);

      /**
       * Read bit decay thread methods
       */
      //@{
      /**
       * Decays the read bits over time to provide a least-recently-read approximation
       *
       * @param thread Reference to the segment_thread running this function
       */
      void clear_read_bits_loop(segment_thread& thread);

      std::optional<segment_thread> _read_bit_decay_thread;
      //@}

      /**
       * Compactor Thread Methods
       */
      //@{
      /**
       * Main loop for the compactor thread that processes and compacts segments
       * 
       * @param thread Reference to the segment_thread running this function
       */
      void compactor_loop(segment_thread& thread);

      void compact_segment(seg_alloc_session& ses, uint64_t seg_num);
      bool compact_pinned_segment(seg_alloc_session& ses);
      bool compact_unpinned_segment(seg_alloc_session& ses);
      bool compactor_promote_rcache_data(seg_alloc_session& ses);

      // segment_thread implementation for the compactor
      std::optional<segment_thread> _compactor_thread;
      std::atomic<bool>             _compactor_done = false;
      //@}

      /**
       * Methods for the segment provider thread, this thread is responsible for ensuring
       * that session threads always have access to new segments without unexpected delays
       * caused by waiting on the operating system to grow files, or lock memory.
       * 
       * @group segment_provider_thread Segment Provider Thread Methods
       */
      ///@{
      void                          provider_munlock_excess_segments();
      void                          provider_prepare_segment(segment_number seg_num, bool pin_it);
      void                          provider_process_recycled_segments();
      void                          provider_populate_pinned_segments();
      void                          provider_populate_unpinned_segments();
      std::optional<segment_number> find_first_free_and_pinned_segment();
      segment_number                provider_allocate_new_segment();

      /**
       * Main loop for the segment provider thread
       * 
       * @param thread Reference to the segment_thread running this function
       */
      void provider_loop(segment_thread& thread);

      std::optional<segment_thread> _segment_provider_thread;
      ///@}

      /**
        * @name Segment Write Protection
        * Methods to enable/disable write protection on segments
        */
      void disable_segment_write_protection(segment_number seg_num);

      /**
        * Calculate statistics about read bits in a segment
        * @param seg_num The segment number to analyze
        * @return A pair containing {number of node headers with read bit set, total bytes of those nodes}
        */
      stats_result calculate_segment_read_stats(segment_number seg_num);

      // maps ids to locations
      id_alloc _id_alloc;

      // allocates new segments
      sal::block_allocator _block_alloc;

      mapping                         _seg_alloc_state_file;
      mapped_memory::allocator_state* _mapped_state;
      std::mutex                      _sync_mutex;

      // Thread state tracking for stop/start_background_threads is handled in mapped_memory

      segment_number get_segment_for_object(const void* obj) const
      {
         auto base   = (const char*)_block_alloc.get(offset_ptr(0));
         auto offset = (const char*)obj - base;
         return segment_number(offset / segment_size);
      }

      /**
        * When an object is moved its space is freed and we need to record the freed space
        * so the compactor has the metadata it needs to efficiently identify segments that
        * can be compacted.
        * 
        * @param obj The object on the segment being freed
        * @param seg The segment number containing the object
        */
      template <typename T>
      inline void record_freed_space(const uint32_t ses_num, segment_number seg, T* obj)
      {
         assert(get_segment_for_object(obj) == seg && "object not in segment");
         /*
         auto base   = _block_alloc.get(offset_ptr());
         auto offset = (const char*)obj - base;
         if (can_modify(ses_num, node_location::from_absolute_address(offset)))
         {
            if (not obj->is_allocator_header())
            {
               record_reusable(ses_num, obj->size());
            }
         }
         */
         _mapped_state->_segment_data.add_freed_space(seg, obj);
      }

      inline void record_session_write(uint32_t session_num, uint64_t bytes) noexcept
      {
         _mapped_state->_session_data.add_bytes_written(session_num, bytes);
      }

      /**
        * Check if a node location has been synced to disk.
        * 
        * @param loc The node location to check
        * @return true if the location is synced, false otherwise
        */
      inline bool is_read_only(node_location loc) const
      {
         int64_t seg = get_segment_num(loc);
         assert(seg < max_segment_count && "invalid segment passed to is_read_only");
         return get_segment(seg)->get_first_write_pos() > get_segment_offset(loc);
      }
      inline bool can_modify(int ses_num, node_location loc) const
      {
         auto seg = get_segment(get_segment_num(loc));
         return seg->_session_id == ses_num && seg->get_first_write_pos() < get_segment_offset(loc);
      }

      /**
       * Get a reference to the session_rlock for a given session number
       * 
       * @param session_num The session number
       * @return Reference to the session_rlock
       */
      inline mapped_memory::session_rlock& get_session_rlock(uint32_t session_num)
      {
         return _mapped_state->_read_lock_queue.get_session_lock(session_num);
      }

      /**
       * Get a reference to the segment metadata for a given segment number
       * 
       * @param segment The segment number
       * @return Reference to the segment_meta
      inline mapped_memory::segment_meta& get_segment_meta(segment_number segment)
      {
         return _mapped_state->_segment_data.meta[segment];
      }
       */

      /**
        * Get the cache difficulty value which is used for determining read bit updates
        * 
        * @return The current cache difficulty value
        */
      inline uint32_t get_cache_difficulty() const
      {
         return _mapped_state->_cache_difficulty_state.get_cache_difficulty();
      }

      /**
        * Get the cache queue for a given session number
        * 
        * @param session_num The session number
        * @return Reference to the cache queue
        */
      inline auto& get_rcache_queue(uint32_t session_num) const
      {
         return _mapped_state->_session_data.rcache_queue(session_num);
      }

      /**
       * Get a new segment from the block allocator
       * 
       * @return A pair containing the segment number and the segment header
       */
      std::pair<segment_number, mapped_memory::segment*> get_new_segment(
          bool alloc_to_pinned = true)
      {
         segment_number segnum;
         if (alloc_to_pinned)
         {
            // takes the highest priority pinned segment available, and if not pinned
            // then it will ack the segment provider who will get it pinned right-quick
            segnum = _mapped_state->_segment_provider.ready_pinned_segments.pop();
            //            ARBTRIE_WARN("get_new_segment pinned: ", segnum);
         }
         else
         {
            // back takes the lowest priority segment, without ack means it will
            // not send a signal to the provider to mlock the segment
            segnum = _mapped_state->_segment_provider.ready_unpinned_segments.pop();
         }
         auto shp = get_segment(segnum);
         shp->_vage_accumulator.reset(arbtrie::get_current_time_ms());
         shp->_provider_sequence = _mapped_state->_segment_provider._next_alloc_seq.fetch_add(
             1, std::memory_order_relaxed);
         _mapped_state->_segment_data.allocated_by_session(segnum);
         return {segnum, shp};
      }

      // Helper to synchronize segment pinned state between bitmap and metadata
      void update_segment_pinned_state(segment_number seg_num, bool is_pinned);
   };  // seg_allocator

   inline bool seg_allocator::config_validate_checksum_on_compact() const
   {
      return _mapped_state->_config.validate_checksum_on_compact;
   }

   inline bool seg_allocator::config_update_checksum_on_compact() const
   {
      return _mapped_state->_config.update_checksum_on_compact;
   }

   inline bool seg_allocator::config_update_checksum_on_modify() const
   {
      return _mapped_state->_config.update_checksum_on_modify;
   }

}  // namespace arbtrie
#include <arbtrie/read_lock_impl.hpp>
#include <arbtrie/seg_alloc_session_impl.hpp>
