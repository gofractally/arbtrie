#pragma once
#include <cstdint>
#include <memory>
#include <new>
#include <ucc/padded_atomic.hpp>
#include <stdexcept>
#include <sal/block_allocator.hpp>
#include <sal/control_block_alloc.hpp>
#include <sal/mapped_memory/allocator_state.hpp>
#include <sal/seg_alloc_dump.hpp>
#include <sal/segment_thread.hpp>
#include <sal/verify.hpp>
#include <shared_mutex>
#include "sal/config.hpp"

namespace sal
{
   class allocator_session;
   class allocator_session_ptr;
   class read_lock;

   /**
    * Indicates the nature of an unclean shutdown to guide recovery strategy.
    */
   enum class recovery_mode
   {
      none,              ///< clean shutdown, no recovery needed
      deferred_cleanup,  ///< mark ref counts stale, defer leak reclamation — O(1)
      app_crash,         ///< OS was fine, just fix ref counts — O(live objects)
      power_loss,        ///< validate sync checksums, truncate torn tails, rebuild
      full_verify        ///< deep per-object checksum verification (maintenance tool)
   };

   /**
    * Exception thrown when a write is attempted on a corruption-halted database.
    */
   class corruption_error : public std::runtime_error
   {
     public:
      using std::runtime_error::runtime_error;
   };

   /**
    * A thread-safe smart allocator that manages objects derived from
    * sal::alloc_header.  Objects returned are reference counted and
    * persistent on disk and when sync() is called they become write-locked
    * but users can continue to copy-on-write without blocking. 
    * 
    * Example Usage:
    * ```
    *  allocator a( "db")
    *  alloc_session_ptr s = db.get_session();
    *  smart_ptr<node> n = s->alloc<node>( size, alloc_hint(), constructor args...);
    *  /// use n, then save it to a global #0 
    *  s.set_root( root_object_number(0), std::move(n) );
    *
    *  /// any thread that wishes to modify root 0 without conflict 
    *  transaction_ptr t = s->start_transaction(root_object_number(0));
    *  smart_ptr<node>& r0 = t.root(); // modify this to update temp state
    *  t->commit(sync_type::fsync); // to commit changes to r0 
    *  t->abort(); // to abandon changes... or simply let t go out of scope.
    *
    * ```
    */
   class allocator
   {
     public:
      /// 64 bits for session id
      static constexpr uint32_t max_session_count = 64;

      allocator(std::filesystem::path dir, runtime_config cfg = runtime_config());
      ~allocator();

      /**
       * @brief Initialize shared ownership for cross-thread shared_smart_ptr use.
       *
       * The allocator is typically a direct member of database, not managed
       * by shared_ptr. This stores an aliasing shared_ptr that shares
       * ownership with the parent, so shared_from_this() works correctly.
       *
       * Must be called after the parent object is managed by shared_ptr.
       */
      template <typename Parent>
      void init_shared_ownership(std::shared_ptr<Parent> parent)
      {
         _self = std::shared_ptr<allocator>(std::move(parent), this);
      }

      std::shared_ptr<allocator> shared_from_this() { return _self; }

      void start_background_threads();
      void stop_background_threads();

      void set_runtime_config(const runtime_config& cfg);

      /// Returns the allocator_session for the calling thread, creating one if
      /// none exists.  Sessions are stored in thread_local storage and are
      /// 1:1 with (thread, allocator) pairs.
      ///
      /// IMPORTANT: The returned session is bound to the calling thread.
      /// Do not create a session on one thread and use it from another.
      /// In multi-threaded code, each thread must call get_session()
      /// (or start_write_session / start_read_session) from its own
      /// context.
      allocator_session_ptr get_session();

      seg_alloc_dump dump() const;

      /// Walk all objects reachable from root objects and sum their allocated sizes.
      /// Returns total bytes occupied by live, reachable objects.
      uint64_t reachable_size();

      /// Linear scan of all segments comparing actual dead space to estimated freed space.
      /// For each read-only segment, walks all objects and checks whether the control block
      /// still points at that location. Returns per-segment data for diagnostics.
      struct segment_freed_audit
      {
         segment_number seg;
         uint64_t       estimated_freed = 0;  // from get_freed_space()
         uint64_t       actual_dead     = 0;  // objects whose CB points elsewhere or ref==0
         uint64_t       actual_live     = 0;  // objects whose CB still points here with ref>0
         uint64_t       total_objects   = 0;
         uint64_t       sync_headers    = 0;  // sync header bytes
      };
      std::vector<segment_freed_audit> audit_freed_space();

      /// Total pending releases across all session queues
      inline uint64_t total_pending_releases() const;

      /// Truncate trailing free segments from the segment file to reclaim disk space.
      /// Must be called after background threads are stopped and compaction is complete.
      void truncate_free_tail();

      /// Stop background threads and truncate trailing free segments without
      /// restarting.  Use when the allocator is about to be destroyed.
      void truncate_free_tail_final();

      /// Copy all live objects from this allocator into dest, using dest's session
      /// for allocation.  Both allocators must have background threads stopped.
      /// After this call, dest contains a packed copy of every live object and
      /// its control blocks are set up with the correct locations and ref counts.
      /// Root addresses are also copied.
      void copy_live_objects_to(allocator& dest);

      /// Verify all segment sync checksums. Walks each segment's sync_header chain
      /// and verifies the XXH3 checksum covering each sync range.
      /// Populates segment_checksums counters and segment_failures in the result.
      void verify_segments(verify_result& result);

      /// Resolve a ptr_address to its alloc_header and location.
      /// Returns {nullptr, {}} if address is invalid (no control block, ref==0,
      /// or address mismatch).
      std::pair<const alloc_header*, location> resolve(ptr_address addr);

      /// Access root objects for iteration
      auto& root_objects() const { return *_root_objects; }

      /// Check if corruption has been detected (e.g. by compactor).
      /// Writers should call this before starting transactions.
      bool corruption_detected() const noexcept
      {
         return _corruption_detected.load(std::memory_order_relaxed);
      }

      /// Throws corruption_error if corruption has been detected.
      /// Called at the start of write transactions.
      void check_corruption() const
      {
         if (__builtin_expect(_corruption_detected.load(std::memory_order_relaxed), 0)) [[unlikely]]
            throw corruption_error("database corruption detected, writes halted");
      }

      /// Signal that corruption has been detected (called by compactor).
      void signal_corruption() noexcept
      {
         _corruption_detected.store(true, std::memory_order_relaxed);
      }

      /**
       * Full recovery: clear all control blocks, scan segments newest-to-oldest
       * to rebuild object locations, walk roots to retain reachable objects,
       * then free anything unreachable. Used after an unclean shutdown.
       */
      void recover();

      /**
       * Lightweight recovery: reset all ref counts > 1 to 1, walk roots to
       * retain reachable objects, then free anything unreachable.
       * Used to reclaim leaked memory without a full segment scan.
       */
      void reset_reference_counts();

      /**
       * Power-loss recovery: validate sync header chains in each segment,
       * truncate torn tails, rebuild roots from sync_root_info embedded in
       * sync headers, then rebuild control blocks and ref counts.
       */
      void recover_from_power_loss();

      /**
       * 
       * Forwards to the thread-local allocator_session::lock() method, it is faster
       * and more efficient to keep a cached copy of your thread's session than to
       * use this method.
       */
      [[nodiscard]] read_lock lock();

      void retain(ptr_address adr) noexcept { _ptr_alloc.get(adr).retain(); }

      /// forwards to get_session()->release(adr)
      void release(ptr_address adr) noexcept;
      /**
       * Syncs the root object to disk.
       * 
       * @param st The sync type to use.
       */
      void sync(sync_type st) noexcept
      {
         if (st < sync_type::msync_sync)
            return;

         // we don't msync() the _block_alloc because that is done
         // on a session-by-session basis, but we still need to fsync() it
         // because that applies to the entire file. But we don't want to
         // fsync(full=true) because _root_object_file also need to be synced
         // and it will do a full (computer-wide) sync if needed and implicitly
         // grab data synced by _block_alloc.
         if (st >= sync_type::fsync)
            _block_alloc.fsync(false);
         _root_object_file.sync(st);

         // we don't sync _ptr_alloc because that data can be recovered from data
         // that is being synced. We also don't sync _mapped_state because it also
         // can be recovered from data that is being synced.
      }

     private:
      std::shared_ptr<allocator> _self;  ///< aliasing shared_ptr for shared_from_this()

      /// Core truncation logic — assumes background threads are already stopped.
      /// Does not restart threads; caller is responsible for that.
      void truncate_free_tail_stopped();

      friend class transaction;
      /**
       * @name Root Object Methods
       * These methods set and get "global" root objects that can be looked up by
       * a number, there are at most 1024 root objects that can be used and these
       * objects are updated atomically and synced to disk when transactions are
       * committed.
       *
       * Any number of readers can operate at the same time, and there are two ways
       * of doing updates: compare and swap (CAS) that asserts the current value is
       * the same as the initial value (thereby proving no one else wrote), or
       * by starting a transaction which will block any others from attempting to
       * start a transaction or compare and swap until it is committed. Note:
       * compare and swap is essentially starting a transaction, setting the root,
       * and committing the transaction. 
       * These methods are private to be used by friend classes of allocator_session
       * and transaction only.
       *
       * @group root_object_methods Root Object Methods
       */
      ///@{

      /**
       * Caller is responsible for releasing the returned address.
       */
      [[nodiscard]] ptr_address get(root_object_number ro) noexcept
      {
         assert(ro < _root_objects->size() && "invalid root object number");
         std::shared_lock<std::shared_mutex> lock(_root_object_mutex[*ro]);
         auto adr = _root_objects->at(*ro).load(std::memory_order_acquire);
         SAL_TRACE("get root object: {} adr: {}", ro, adr);
         if (adr != null_ptr_address)
            retain(adr);
         return adr;
      }

      /**
       * Caller is responsible for *giving* a valid reference, and releasing the returned address.
       */
      [[nodiscard]] ptr_address set(root_object_number ro, ptr_address adr, sync_type st) noexcept
      {
         std::lock_guard<std::mutex>        wlock(_write_mutex[*ro]);
         std::lock_guard<std::shared_mutex> rlock(_root_object_mutex[*ro]);
         auto result = _root_objects->at(*ro).exchange(adr, std::memory_order_release);
         sync(st);
         return result;
      }

      /**
       * Caller is responsible for *giving* a valid desire reference, and releasing the 
       * expected reference if successful.  On failure the caller remains responsible for
       * the reference to the desired outcome.
       */
      bool cas_root(root_object_number ro,
                    ptr_address        expect,
                    ptr_address        desire,
                    sync_type          st) noexcept
      {
         std::lock_guard<std::mutex>        wlock(_write_mutex[*ro]);
         std::lock_guard<std::shared_mutex> rlock(_root_object_mutex[*ro]);
         if (_root_objects->at(*ro).compare_exchange_strong(expect, desire,
                                                            std::memory_order_release))
         {
            sync(st);
            return true;
         }
         return false;
      }

      /**
       * Grabs the write mutex for root object, which will ensure that no other 
       * threads will be working on a update to this root object until this transaction
       * is committed or aborted. 
       */
      [[nodiscard]] ptr_address start_transaction(root_object_number ro)
      {
         check_corruption();
         SAL_WARN("{}", _root_objects);
         SAL_WARN("start_transaction: {} size: {}", ro, _root_objects->size());
         assert(ro < _root_objects->size() && "invalid root object number");
         _write_mutex[*ro].lock();
         return get(ro);
      }

      /**
       * Commits the transaction, and updates the root object with the desired reference.
       * Caller is responsible for releasing the returned address.
       */
      [[nodiscard]] ptr_address transaction_commit(root_object_number ro,
                                                   ptr_address        desired,
                                                   sync_type          st) noexcept
      {
         assert(ro < _root_objects->size() && "invalid root object number");
         auto result = _root_objects->at(*ro).exchange(desired, std::memory_order_release);
         sync(st);
         _write_mutex[*ro].unlock();
         return result;
      }

      /**
       * Aborts the transaction, and releases the write mutex.
       */
      void transaction_abort(root_object_number ro) noexcept
      {
         assert(ro < _root_objects->size() && "invalid root object number");
         _write_mutex[*ro].unlock();
      }
      ///@}

      /// @see get_session() — same thread-affinity constraints apply.
      allocator_session_ptr get_session() const;
      void                  end_session(allocator_session* ses);

      friend class allocator_session;
      friend class read_lock;
      using root_object_array = std::array<std::atomic<ptr_address>, 1024>;

      mapped_memory::allocator_state* _mapped_state;
      control_block_alloc             _ptr_alloc;
      sal::block_allocator            _block_alloc;
      uint32_t                        _allocator_index;
      mapping                         _seg_alloc_state_file;
      mapping                         _root_object_file;
      root_object_array*              _root_objects;
      std::mutex                      _sync_mutex;
      /// used by readers/writers to grab/update a root object
      std::array<std::shared_mutex, 1024> _root_object_mutex;
      /// mutexes used by transactions to ensure that only one writer per root object
      std::array<std::mutex, 1024> _write_mutex;

      /// Corruption flag — set by compactor on checksum failure, checked by writers.
      /// Own cacheline to avoid false sharing (128 bytes for Apple M-series).
      alignas(ucc::hardware_cacheline_size) std::atomic<bool> _corruption_detected{false};

      inline bool config_validate_checksum_on_compact() const;
      inline bool config_update_checksum_on_compact() const;
      inline bool config_update_checksum_on_modify() const;

      void mlock_pinned_segments();
      void recursive_retain_all(ptr_address addr);
      // Implementation helper for reachable_size(); defined in allocator.cpp
      void recursive_sum_size(ptr_address addr, uint64_t& total, void* visited);
      bool compactor_release_objects(allocator_session& ses);

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
      inline void record_freed_space(allocator_session_number /*ses_num*/, T* obj);
      /*
      {
         //static std::vector<T*> ptrs;
         //ptrs.push_back(obj);
         if (*obj->address() == 873472)
         {
            SAL_WARN("record_freed_space: {} adr: {} ptr: {}", obj->size(), obj->address(),
                     int64_t(obj));
            auto nref = get_session()->get_ref<leaf_node>(obj->address());
            SAL_WARN("record_freed_space: {} adr: {} ptr: {}", obj->size(), obj->address(),
                     int64_t(nref.obj()));
         }
         _mapped_state->_segment_data.add_freed_space(get_segment_for_object(obj), obj);
      }  
      */

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
         //         SAL_INFO("can_modify: ses_num: {} == ses_ses: {}  loc-seg-off: {} >= seg-first-write: {}",
         ////                 ses_num, seg->_session_id, loc.segment_offset(), seg->get_first_write_pos());
         return seg->_session_id == ses_num && seg->get_first_write_pos() <= loc.segment_offset();
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
      inline auto& get_release_queue(allocator_session_number session_num) const
      {
         return _mapped_state->_session_data.release_queue(session_num);
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

   inline uint64_t allocator::total_pending_releases() const
   {
      uint64_t total = 0;
      for (uint32_t i = 0; i < max_session_count; ++i)
         total += get_release_queue(allocator_session_number(i)).usage();
      return total;
   }

};  // namespace sal
