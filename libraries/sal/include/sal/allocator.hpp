#pragma once
#include <cstdint>
#include <memory>
#include <sal/block_allocator.hpp>
#include <sal/control_block_alloc.hpp>
#include <sal/mapped_memory/allocator_state.hpp>
#include <sal/seg_alloc_dump.hpp>
#include <sal/segment_thread.hpp>

namespace sal
{
   class allocator_session;
   class allocator_session_ptr;
   class read_lock;

   /**
    * A thread-safe smart allocator that manages objects derived from
    * sal::alloc_header.  Objects returned are reference counted and
    * persistent on disk and when sync() is called they become write-locked
    * but users can continue to copy-on-write without blocking. 
    */
   class allocator
   {
     public:
      /// 64 bits for session id
      static constexpr uint32_t max_session_count = 64;

      allocator(std::filesystem::path dir, runtime_config cfg);
      ~allocator();

      void start_background_threads();
      void stop_background_threads();

      void set_runtime_config(const runtime_config& cfg);

      void fsync(bool full = false);

      // gets the current thread's session, increments its ref count
      // becomes get_thread_session() returns a non-atomic smart pointer that will
      // release the session when it goes out of scope
      allocator_session_ptr get_session();

      seg_alloc_dump dump() const;

      /**
       * 
       * Forwards to the thread-local allocator_session::lock() method, it is faster
       * and more efficient to keep a cached copy of your thread's session than to
       * use this method.
       */
      [[nodiscard]] read_lock lock();

      // release smart_ref.take() if you are doing manual memory management
      void release(ptr_address adr) noexcept;

     private:
      allocator_session_ptr get_session() const;
      void                  end_session(allocator_session* ses);

      friend class allocator_session;
      friend class read_lock;

      mapped_memory::allocator_state* _mapped_state;
      control_block_alloc             _ptr_alloc;
      sal::block_allocator            _block_alloc;
      uint32_t                        _allocator_index;
      mapping                         _seg_alloc_state_file;
      std::mutex                      _sync_mutex;

      inline bool config_validate_checksum_on_compact() const;
      inline bool config_update_checksum_on_compact() const;
      inline bool config_update_checksum_on_modify() const;

      void mlock_pinned_segments();

      /**
       * Utilized by allocator_session 
       */
      /// @{
      mapped_memory::segment* get_segment(segment_number seg) noexcept
      {
         return reinterpret_cast<mapped_memory::segment*>(
             _block_alloc.get(block_allocator::block_number(*seg)));
      }
      const mapped_memory::segment* get_segment(segment_number seg) const noexcept
      {
         return reinterpret_cast<const mapped_memory::segment*>(
             _block_alloc.get(block_allocator::block_number(*seg)));
      }

      allocator_session_number alloc_session_num() noexcept;
      void                     release_session_num(allocator_session_number sn) noexcept;

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

      void compact_segment(allocator_session& ses, segment_number seg_num);
      bool compact_pinned_segment(allocator_session& ses);
      bool compact_unpinned_segment(allocator_session& ses);
      bool compactor_promote_rcache_data(allocator_session& ses);

      // segment_thread implementation for the compactor
      std::optional<segment_thread> _compactor_thread;
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
      inline void record_freed_space(allocator_session_number /*ses_num*/, T* obj)
      {
         _mapped_state->_segment_data.add_freed_space(get_segment_for_object(obj), obj);
      }

      inline void record_session_write(allocator_session_number session_num,
                                       uint64_t                 bytes) noexcept
      {
         _mapped_state->_session_data.add_bytes_written(session_num, bytes);
      }

      /**
        * Check if a node location has been synced to disk.
        * 
        * @param loc The node location to check
        * @return true if the location is synced, false otherwise
        */
      inline bool is_read_only(location loc) const
      {
         segment_number seg = loc.segment();
         assert(seg < max_segment_count && "invalid segment passed to is_read_only");
         return get_segment(seg)->get_first_write_pos() > loc.segment_offset();
      }
      inline bool can_modify(allocator_session_number ses_num, location loc) const
      {
         auto seg = get_segment(loc.segment());
         return seg->_session_id == ses_num && seg->get_first_write_pos() < loc.segment_offset();
      }

      /**
       * Get a reference to the session_rlock for a given session number
       * 
       * @param session_num The session number
       * @return Reference to the session_rlock
       */
      inline mapped_memory::session_rlock& get_session_rlock(allocator_session_number session_num)
      {
         return _mapped_state->_read_lock_queue.get_session_lock(session_num);
      }

      /**
        * Get the cache difficulty value which is used for determining read bit updates
        * 
        * @return The current cache difficulty value
        */
      inline uint64_t get_cache_difficulty() const
      {
         return _mapped_state->_cache_difficulty_state.get_cache_difficulty();
      }

      /**
        * Get the cache queue for a given session number
        * 
        * @param session_num The session number
        * @return Reference to the cache queue
        */
      inline auto& get_rcache_queue(allocator_session_number session_num) const
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
            // back takes the lowest priority segment
            segnum = _mapped_state->_segment_provider.ready_unpinned_segments.pop();
         }
         auto shp = get_segment(segnum);
         shp->age_accumulator.reset(*sal::get_current_time_msec());
         shp->_provider_sequence = _mapped_state->_segment_provider._next_alloc_seq.fetch_add(
             1, std::memory_order_relaxed);
         _mapped_state->_segment_data.allocated_by_session(segnum);
         return {segnum, shp};
      }

      // Helper to synchronize segment pinned state between bitmap and metadata
      void update_segment_pinned_state(segment_number seg_num, bool is_pinned);
   };  // seg_allocator

   inline bool allocator::config_validate_checksum_on_compact() const
   {
      return _mapped_state->_config.validate_checksum_on_compact;
   }

   inline bool allocator::config_update_checksum_on_compact() const
   {
      return _mapped_state->_config.update_checksum_on_compact;
   }

   inline bool allocator::config_update_checksum_on_modify() const
   {
      return _mapped_state->_config.update_checksum_on_modify;
   }

};  // namespace sal

#include <sal/allocator_session_impl.hpp>