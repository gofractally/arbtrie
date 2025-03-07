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
#include <condition_variable>
#include <thread>
#include <vector>

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

      seg_allocator(std::filesystem::path dir);
      ~seg_allocator();

      /**
       * Debugging methods
       * @group Debugging
       */
      ///@{
      void           print_region_stats() { _id_alloc.print_region_stats(); }
      uint64_t       count_ids_with_refs() { return _id_alloc.count_ids_with_refs(); }
      seg_alloc_dump dump();
      ///@}

      void sync(sync_type st = sync_type::sync);

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

     private:
      friend class database;
      void    release_unreachable();
      void    reset_meta_nodes(recover_args args);
      void    reset_reference_counts();
      int64_t clear_lock_bits();
      void    sync_segment(int seg, sync_type st) noexcept;

      friend class seg_alloc_session;
      friend class read_lock;
      friend class modify_lock;
      friend class object_ref;

      void mlock_pinned_segments();

      /**
       * Utilized by seg_alloc_session 
       */
      /// @{
      mapped_memory::segment_header* get_segment(segment_number seg) noexcept
      {
         return static_cast<mapped_memory::segment_header*>(_block_alloc.get(seg));
      }
      uint32_t alloc_session_num();
      void     release_session_num(uint32_t sn);
      /**
       * This must be called via a session because the session is responsible
       * for documenting what regions could be read
       *
       * All objects are const because they cannot be modified after being
       * written, unless accessed via a session's mutation lock
       */
      const node_header* get_object(node_location loc) const;
      ///@}

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

      void attempt_truncate_empty();

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
      void           provider_munlock_excess_segments();
      void           provider_process_acknowledged_segments();
      void           provider_prepare_segment(segment_number seg_num);
      void           provider_process_recycled_segments();
      segment_number provider_allocate_new_segment();

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
      void enable_segment_write_protection(segment_number seg_num);

      /**
        * Calculate statistics about read bits in a segment
        * @param seg_num The segment number to analyze
        * @return A pair containing {number of node headers with read bit set, total bytes of those nodes}
        */
      std::pair<uint32_t, uint64_t> calculate_segment_read_stats(segment_number seg_num);

      /**
        *  After all writes are complete, and there is not enough space
        *  to allocate the next object the alloc_ptr gets set to MAX and
        *  the page gets 
        */
      void finalize_segment(segment_number);

      /**
        *  After all data has been removed from a segment
        * - madvise free/don't need 
        * - add the segment number to the free segments at allocator_header::end_ptr
        * - increment allocator_header::end_ptr
        */
      void release_segment(segment_number);

      void push_dirty_segment(int seg_num)
      {
         std::unique_lock lock(_dirty_segs_mutex);
         _dirty_segs[_next_dirt_seg_index % max_segment_count] = seg_num;
         ++_next_dirt_seg_index;
      }
      int get_last_dirty_seg_idx()
      {
         std::unique_lock lock(_dirty_segs_mutex);
         return _next_dirt_seg_index;
      }

      // maps ids to locations
      id_alloc _id_alloc;

      // allocates new segments
      block_allocator _block_alloc;

      mapping                         _seg_alloc_state_file;
      mapped_memory::allocator_state* _mapped_state;
      std::mutex                      _sync_mutex;

      std::vector<sync_lock> _seg_sync_locks;
      std::vector<int>       _dirty_segs;
      std::mutex             _dirty_segs_mutex;
      uint64_t               _next_dirt_seg_index = 0;
      uint64_t               _last_synced_index   = 0;

      // Thread state tracking for stop/start_background_threads is handled in mapped_memory

      /**
        * Free an object in a segment
        * 
        * @param segment The segment number containing the object
        * @param object_size The size of the object to free
        */
      inline void free_object(segment_number segment, uint32_t object_size)
      {
         assert(segment < max_segment_count && "Segment number out of bounds");
         _mapped_state->_segment_data.meta[segment].free_object(object_size);
      }

      /**
        * Get the last synced position for a segment.
        * 
        * @param segment The segment number
        * @return The last synced position in the segment
        */
      inline size_t get_last_sync_position(segment_number segment) const
      {
         assert(segment < max_segment_count && "invalid segment passed to get_last_sync_position");
         return _mapped_state->_segment_data.get_last_sync_pos(segment);
      }

      /**
        * Check if a node location has been synced to disk.
        * 
        * @param loc The node location to check
        * @return true if the location is synced, false otherwise
        */
      inline bool is_synced(node_location loc) const
      {
         int64_t seg = loc.segment();
         assert(seg < max_segment_count && "invalid segment passed to is_synced");
         return _mapped_state->_segment_data.get_last_sync_pos(seg) > loc.abs_index();
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
       */
      inline mapped_memory::segment_meta& get_segment_meta(segment_number segment)
      {
         return _mapped_state->_segment_data.meta[segment];
      }

      /**
        * Get the cache difficulty value which is used for determining read bit updates
        * 
        * @return The current cache difficulty value
        */
      inline uint32_t get_cache_difficulty() const
      {
         return _mapped_state->_cache_difficulty.load(std::memory_order_relaxed);
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
      std::pair<segment_number, mapped_memory::segment_header*> get_new_segment()
      {
         auto segnum = _mapped_state->_segment_provider.ready_segments.pop_wait();
         return {segnum, get_segment(segnum)};
      }
   };  // seg_allocator

}  // namespace arbtrie
#include <arbtrie/read_lock_impl.hpp>
#include <arbtrie/seg_alloc_session_impl.hpp>
