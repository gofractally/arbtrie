#pragma once
#include <hash/lehmer64.h>
#include <cassert>
#include <cstdint>
#include <optional>
#include <sal/config.hpp>
#include <sal/control_block.hpp>
#include <sal/control_block_alloc.hpp>
#include <sal/debug.hpp>
#include <sal/location.hpp>
#include <sal/mapped_memory/read_lock_queue.hpp>
#include <sal/mapped_memory/segment.hpp>
#include <sal/mapped_memory/session_data.hpp>
#include <sal/mapped_memory/session_op_stats.hpp>
#include <sal/numbers.hpp>
#include <sal/time.hpp>
#include "ucc/round.hpp"
#include <thread>

namespace sal
{
   namespace detail
   {
      inline thread_local uint32_t session_operation_depth = 0;
   }

   class allocator;
   class read_lock;

   template <typename T>
   class smart_ptr;

   template <typename T>
   class smart_ref;

   class allocator_session;
   class allocator_session_ptr;
   class transaction;
   using transaction_ptr = std::unique_ptr<transaction>;

   class read_lock;
   class alloc_header;

   class allocator_session
   {
     public:
      class operation_scope
      {
        public:
         explicit operation_scope(bool entered = false) noexcept : _entered(entered) {}
         operation_scope(const operation_scope&)            = delete;
         operation_scope& operator=(const operation_scope&) = delete;
         operation_scope(operation_scope&& other) noexcept : _entered(other._entered)
         {
            other._entered = false;
         }
         operation_scope& operator=(operation_scope&& other) noexcept
         {
            if (this != &other)
            {
               release();
               _entered       = other._entered;
               other._entered = false;
            }
            return *this;
         }
         ~operation_scope() { release(); }

        private:
         void release() noexcept
         {
            if (!_entered)
               return;
            assert(detail::session_operation_depth > 0);
            --detail::session_operation_depth;
            _entered = false;
         }

         bool _entered = false;
      };

      /**
       * Returns an object that prevents the compactor from overwriting data 
       * that has been moved but for which this thread may still be reading the
       * old location. This is a wait-free atomic load/store operation. If any
       * thread attempts to dereference data without this lock debug builds will
       * assert, but release builds will not because holding the lock is an
       * invariant. This method is reentrant so the same thread can call it
       * multiple times.
       */
      [[nodiscard]] read_lock lock() noexcept;

      uint64_t get_total_allocated_objects() const noexcept;
      uint64_t get_pending_release_count() const noexcept;

      /// Segment allocation stats (for profiling).
      uint64_t seg_alloc_count() const noexcept { return _seg_alloc_count; }
      uint64_t seg_alloc_ns() const noexcept { return _seg_alloc_ns; }


      /// DEBUG: Call visitor(ptr_address, ref_count, alloc_header*) for each live object
      template <typename Visitor>
      void for_each_live_object(Visitor&& visitor) const noexcept;

      template <typename T>
      [[nodiscard]] ptr_address alloc(auto&&... args) noexcept
      {
         return this->alloc<T>(T::alloc_size(std::forward<decltype(args)>(args)...), alloc_hint{},
                               std::forward<decltype(args)>(args)...);
      }
      template <typename T>
      [[nodiscard]] smart_ptr<T> smart_alloc(auto&&... args) noexcept
      {
         return this->smart_alloc<T>(T::alloc_size(std::forward<decltype(args)>(args)...),
                                     alloc_hint{}, std::forward<decltype(args)>(args)...);
      }

      template <typename T>
      [[nodiscard]] ptr_address alloc(alloc_hint hint, auto&&... args) noexcept
      {
         return this->alloc<T>(T::alloc_size(std::forward<decltype(args)>(args)...), hint,
                               std::forward<decltype(args)>(args)...);
      }
      template <typename T>
      [[nodiscard]] smart_ptr<T> smart_alloc(alloc_hint hint, auto&&... args) noexcept
      {
         return this->smart_alloc<T>(T::alloc_size(std::forward<decltype(args)>(args)...), hint,
                                     std::forward<decltype(args)>(args)...);
      }

      template <typename T>
      [[nodiscard]] ptr_address alloc(uint32_t size, alloc_hint hint, auto&&... args) noexcept;

      template <typename T>
      [[nodiscard]] smart_ptr<T> smart_alloc(uint32_t   size,
                                             alloc_hint hint,
                                             auto&&... args) noexcept;

      template <typename To, typename From>
      [[nodiscard]] smart_ref<To> realloc(const smart_ref<From>& ptr, auto&&... args) noexcept
      {
         return this->realloc<To>(ptr, To::alloc_size(std::forward<decltype(args)>(args)...),
                                  std::forward<decltype(args)>(args)...);
      }
      template <typename To, typename From>
      [[nodiscard]] smart_ref<To> realloc(const smart_ref<From>& ptr,
                                          uint32_t               size,
                                          auto&&... args);

      /// Like realloc but allows ref > 1 (MVCC: atomically redirect shared CB to new data).
      template <typename To, typename From>
      [[nodiscard]] smart_ref<To> mvcc_realloc(const smart_ref<From>& ptr, auto&&... args) noexcept
      {
         return this->mvcc_realloc<To>(ptr, To::alloc_size(std::forward<decltype(args)>(args)...),
                                       std::forward<decltype(args)>(args)...);
      }
      template <typename To, typename From>
      [[nodiscard]] smart_ref<To> mvcc_realloc(const smart_ref<From>& ptr,
                                               uint32_t               size,
                                               auto&&... args);

      template <typename T>
      [[nodiscard]] T* copy_on_write(smart_ref<T>& ptr);

      template <typename T = alloc_header>
      [[nodiscard]] smart_ptr<T> get_root(root_object_number ro) noexcept;

      /**
       * Sets the root object for the session.
       * 
       * @param ro The root object number to set.
       * @param ptr The smart pointer to set the root object to.
       * @return The previous root object.
       */
      template <typename T>
      smart_ptr<T> set_root(root_object_number ro, smart_ptr<T> ptr, sync_type) noexcept;

      template <typename T, typename U>
      smart_ptr<T> cas_root(root_object_number ro,
                            smart_ptr<T>       expect,
                            smart_ptr<U>       desired,
                            sync_type) noexcept;

      /**
       * Returns a transaction object that can be used to modify the root object.
       * The transaction object will be released when the transaction is committed or
       * aborted.
       */
      transaction_ptr       start_transaction(root_object_number ro) noexcept;
      allocator_session_ptr get_session_ptr() noexcept;

      /// Allocate a custom control block with no segment-backed data.
      /// Stores a user-defined value (up to 41 bits) in the location field.
      /// Returns a ptr_address with ref count = 1.
      [[nodiscard]] ptr_address alloc_custom_cb(uint64_t user_value) noexcept;

      /// Read the user-defined value from a custom control block.
      /// Precondition: adr was allocated via alloc_custom_cb().
      uint64_t read_custom_cb(ptr_address adr) const noexcept;

      /// Read a custom control block only if the address is live and has the
      /// custom marker. Crash recovery must not trust root metadata blindly.
      std::optional<uint64_t> try_read_custom_cb(ptr_address adr) const noexcept;

      /// Check if a control block is custom (no segment data).
      /// The {active=0, pending_cache=1} bit pattern marks custom CBs.
      static bool is_custom_cb(control_block_data cbd) noexcept;

      /**
       * This is to be used if and only if the user has taken ownership of the
       * ptr_address via smart_ptr<T>::take() and is now using manual memory
       * management.
       */
      void release(ptr_address adr) noexcept;

      /**
       * This is to be used if and only if the address is the last reference to the
       * object.
       */
      void final_release(ptr_address adr) noexcept;

      /**
       * This is to be used if and only if the user has taken ownership of the
       * ptr_address via smart_ptr<T>::take() and is now using manual memory
       * management.
       */
      void retain(ptr_address adr);

      template <typename T = alloc_header>
      [[nodiscard]] smart_ref<T> get_ref(ptr_address adr) noexcept;

      inline void prefetch(ptr_address adr) const noexcept;
      inline bool is_read_only(ptr_address adr) const noexcept;

      template <typename UserData>
      void sync(sync_type st, const runtime_config& cfg, UserData user_data = 0)
      {
         static_assert(std::is_trivially_copyable_v<UserData>, "UserData must be POD");
         sync(st, cfg, std::span<char>(reinterpret_cast<char*>(&user_data), sizeof(UserData)));
      }
      // everything allocated and modified by this session since the last
      // call to sync will be saved to disk
      void sync(sync_type st, const runtime_config& cfg, std::span<char> user_data = {});
      void sync(sync_type st);

      ~allocator_session();
      allocator&               get_allocator() const noexcept { return _sega; }
      allocator_session_number get_session_num() const noexcept { return _session_num; }
      [[nodiscard]] operation_scope record_operation(mapped_memory::session_operation op,
                                                     uint64_t count = 1) noexcept;

      bool config_update_checksum_on_modify() const noexcept
      {
         // TODO: cache reference to runtime_config and return _update_checksum_on_modify;
         return false;
      }  //_update_checksum_on_modify; }

     private:
      friend class transaction;
      smart_ptr<alloc_header> transaction_commit(root_object_number      ro,
                                                 smart_ptr<alloc_header> desired,
                                                 sync_type               st) noexcept;
      void                    transaction_abort(root_object_number ro) noexcept;

      friend class read_lock;
      friend class allocator_session_ptr;
      friend class allocator;
      template <typename T>
      friend class smart_ptr;
      template <typename T>
      friend class smart_ref;
      template <typename T>
      friend class modify_guard;

      template <typename T>
      T*             get(location loc) noexcept;
      control_block& get(ptr_address adr) noexcept { return _ptr_alloc.get(adr); }

      void retain_read_lock() noexcept;
      void release_read_lock() noexcept;

      void init_active_segment();
      void finalize_active_segment();

      //   void end_modify();
      //   bool try_modify_segment(segment_number segment_num);

      allocator_session(allocator& a, allocator_session_number ses_num);
      allocator_session()                                    = delete;
      allocator_session(const allocator_session&)            = delete;
      allocator_session& operator=(const allocator_session&) = delete;

      inline void record_freed_space(const alloc_header* obj, const char* tag) noexcept;

      /**
       * Check if a node location has been synced to disk.
      inline bool is_synced(location loc) const;
       */
      inline bool is_read_only(location loc) const noexcept;

      /// requires segment be owned by this session and loc not on read-only page
      inline bool can_modify(location loc) const noexcept;

      /**
       * Get the cache difficulty value which is used for determining read bit updates
       */
      inline uint64_t get_cache_difficulty() const noexcept;

      /**
       * Check if an object should be cached based on its size and difficulty threshold
       * @param adr The control block address being dereferenced
       * @param size The size of the object in bytes
       * @return true if the object should be cached, false otherwise
       */
      inline bool should_cache(ptr_address adr, uint32_t size) noexcept;

      /**
       * Generate a random number for cache decisions
       * @return A random 64-bit number
       */
      inline uint64_t get_random() noexcept;

      /**
       * Reclaims the most recently allocated size bytes
       */
      bool unalloc(uint32_t size);

      template <typename T>
      std::pair<location, T*> alloc_data(uint32_t size, auto&&... args);
      template <typename T>
      std::pair<location, T*> alloc_data_vage(uint32_t size, msec_timestamp vage, auto&&... args);

      /**
       * Set the allocation policy for the session
       * 
       * @param alloc_to_pinned true if the session should allocate to pinned segments when it needs
       * to allocate more memory.
       */
      void set_alloc_to_pinned(bool alloc_to_pinned) { _alloc_to_pinned = alloc_to_pinned; }

      /// Position the most frequently used members at the beginning of the class
      /// to reduce cache misses since the session is queiried for every dereference
      /// of an address or locaiton.

      // cache these pointers from sega._block_allocator()... and sega._control_block_alloc...
      // so that users of the session can have faster indexing with less indirection.
      char*                        _block_base_ptr;
      control_block*               _control_block_base_ptr;
      int                          _nested_read_lock = 0;
      allocator_session_number     _session_num;  // index into _sega's active sessions list
      rcache_queue_type&           _rcache_queue;
      release_queue_type&          _release_queue;
      allocator&                   _sega;
      control_block_alloc&         _ptr_alloc;
      mapped_memory::segment*      _alloc_seg_ptr  = nullptr;
      mapped_memory::segment_meta* _alloc_seg_meta = nullptr;

      // separate cacheline starts here...
      // RNG for cache decisions - initialized with session number for reproducibility
      lehmer64_rng _session_rng;  // 32 bytes..

      mapped_memory::dirty_segment_queue& _dirty_segments;
      segment_number                      _alloc_seg_num   = segment_number(-1);
      bool                                _alloc_to_pinned = true;

      // ── Segment allocation stats (for profiling merge drain) ───────
      uint64_t _seg_alloc_count   = 0;  ///< Number of init_active_segment() calls.
      uint64_t _seg_alloc_ns      = 0;  ///< Cumulative nanoseconds in init_active_segment().
      // Reference to the session read lock from read_lock_queue
      mapped_memory::session_rlock& _session_rlock;

      // Thread-ownership validation (enabled via SAL_THREAD_CHECKS or Debug builds).
      // Returns true on success; on violation prints diagnostic and aborts.
      // Usage: assert(check_thread_ownership()) — compiled out in Release
      // unless SAL_THREAD_CHECKS is defined.
#if !defined(NDEBUG) || defined(SAL_THREAD_CHECKS)
      std::thread::id _owning_thread{std::this_thread::get_id()};

      bool check_thread_ownership() const
      {
         if (_owning_thread != std::this_thread::get_id()) [[unlikely]]
         {
            fprintf(stderr,
                    "\n!!! SESSION THREAD VIOLATION: session %u accessed from wrong thread\n",
                    *_session_num);
            return false;
         }
         return true;
      }
#else
      bool check_thread_ownership() const noexcept { return true; }
#endif

      void prepare_alloc(uint32_t size, msec_timestamp vage);

      friend class allocator_session_ptr;
      friend class allocator;
      void end_session();
      void retain_session() { ++_ref_count; }
      int  _ref_count = 1;

   };  // end of class allocator_session

   /**
    * A non-atomic reference counted smart pointer that will release the allocator_session 
    * when it goes out of scope, do not pass this pointer to another thread.
    */
   class allocator_session_ptr
   {
     public:
      explicit allocator_session_ptr(allocator_session* session_ptr) : _session_ptr(session_ptr) {}
      allocator_session_ptr(allocator_session* session_ptr, bool retain) : _session_ptr(session_ptr)
      {
         if (retain) [[likely]]
            _session_ptr->retain_session();
      }
      ~allocator_session_ptr()
      {
         if (_session_ptr)
            _session_ptr->end_session();
      }

      allocator_session_ptr(const allocator_session_ptr& other) : _session_ptr(other._session_ptr)
      {
         if (_session_ptr)
            _session_ptr->retain_session();
      }
      allocator_session_ptr(allocator_session_ptr&& other) : _session_ptr(other._session_ptr)
      {
         other._session_ptr = nullptr;
      }
      allocator_session_ptr& operator=(const allocator_session_ptr& other)
      {
         if (this == &other)
            return *this;
         if (other._session_ptr)
            other._session_ptr->retain_session();
         if (_session_ptr)
            _session_ptr->end_session();
         _session_ptr = other._session_ptr;
         return *this;
      }
      allocator_session_ptr& operator=(allocator_session_ptr&& other)
      {
         std::swap(_session_ptr, other._session_ptr);
         return *this;
      }

      allocator_session* operator->() const { return _session_ptr; }
      allocator_session& operator*() const { return *_session_ptr; }
      allocator_session* get() const { return _session_ptr; }

     private:
      allocator_session* _session_ptr;
   };

}  // namespace sal
#include <sal/read_lock.hpp>
