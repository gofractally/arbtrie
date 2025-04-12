#pragma once
#include <hash/lehmer64.h>
#include <cstdint>
#include <sal/control_block.hpp>
#include <sal/control_block_alloc.hpp>
#include <sal/location.hpp>
#include <sal/mapped_memory/read_lock_queue.hpp>
#include <sal/mapped_memory/segment.hpp>
#include <sal/mapped_memory/session_data.hpp>
#include <sal/numbers.hpp>
#include <sal/time.hpp>

namespace sal
{
   class allocator;
   class read_lock;

   template <typename T>
   class smart_ptr;

   template <typename T>
   class smart_ref;

   class allocator_session;
   class allocator_session_ptr;

   class read_lock;
   class alloc_header;

   class allocator_session
   {
     public:
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

      template <typename T>
      [[nodiscard]] smart_ptr<T> alloc(uint32_t size, alloc_hint hint, auto&&... args);

      template <typename To, typename From>
      [[nodiscard]] smart_ptr<To> realloc(smart_ref<From>& ptr, uint32_t size, auto&&... args);

      template <typename T>
      [[nodiscard]] T* copy_on_write(smart_ref<T>& ptr);

      /**
       * This is to be used if and only if the user has taken ownership of the
       * ptr_address via smart_ptr<T>::take() and is now using manual memory
       * management.
       */
      void release(ptr_address adr) noexcept;

      /**
       * This is to be used if and only if the user has taken ownership of the
       * ptr_address via smart_ptr<T>::take() and is now using manual memory
       * management.
       */
      void retain(ptr_address adr) noexcept;

      template <typename T = alloc_header>
      [[nodiscard]] smart_ref<T> get(ptr_address adr) noexcept;

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

     private:
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

      void retain_read_lock();
      void release_read_lock();

      void init_active_segment();
      void finalize_active_segment();

      //   void end_modify();
      //   bool try_modify_segment(segment_number segment_num);

      allocator_session(allocator& a, allocator_session_number ses_num);
      allocator_session()                                    = delete;
      allocator_session(const allocator_session&)            = delete;
      allocator_session& operator=(const allocator_session&) = delete;

      inline void record_freed_space(const alloc_header* obj) noexcept;

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
       * @param size The size of the object in bytes
       * @return true if the object should be cached, false otherwise
       */
      inline bool should_cache(uint32_t size) noexcept;

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
      // Reference to the session read lock from read_lock_queue
      mapped_memory::session_rlock& _session_rlock;

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
      allocator_session_ptr(allocator_session* session_ptr) : _session_ptr(session_ptr) {}
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
