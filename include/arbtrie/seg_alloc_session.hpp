#pragma once
#include <arbtrie/circular_buffer.hpp>
#include <arbtrie/config.hpp>
#include <arbtrie/id_alloc.hpp>
#include <arbtrie/mapped_memory.hpp>
#include <arbtrie/mapping.hpp>
#include <arbtrie/node_handle.hpp>
#include <arbtrie/node_header.hpp>
#include <arbtrie/node_meta.hpp>
#include <arbtrie/sync_lock.hpp>
#include <atomic>
#include <memory>

namespace arbtrie
{
   class seg_allocator;
   class read_lock;

   class seg_alloc_session
   {
     public:
      // before any objects can be read, the session must note the
      // current state of the free segment queue so that no segments that
      // could be read while the return value of this method is in scope can
      // be reused (overwritten)
      read_lock lock();
      void      sync(sync_type st);

      ~seg_alloc_session();
      seg_alloc_session(seg_alloc_session&& mv);
      seg_alloc_session& operator=(seg_alloc_session&& mv);

      uint64_t count_ids_with_refs();

     private:
      friend class read_lock;
      friend class modify_lock;
      friend class seg_allocator;
      friend class object_ref;

      void retain_read_lock();
      void release_read_lock();
      void finalize_active_segment();

      seg_alloc_session(seg_allocator& a, uint32_t ses_num);
      seg_alloc_session()                                    = delete;
      seg_alloc_session(const seg_alloc_session&)            = delete;
      seg_alloc_session& operator=(const seg_alloc_session&) = delete;

      inline void free_object(segment_number segment, uint32_t object_size);

      inline size_t get_last_sync_position(segment_number segment) const;

      /**
       * Check if a node location has been synced to disk.
       */
      inline bool is_synced(node_location loc) const;

      /**
       * Get the cache difficulty value which is used for determining read bit updates
       */
      inline uint32_t get_cache_difficulty() const;

      void                                   unalloc(uint32_t size);
      std::pair<node_location, node_header*> alloc_data(uint32_t       size,
                                                        id_address_seq adr_seq,
                                                        uint64_t       vage = 0);

      /**
       * Set the allocation policy for the session
       * 
       * @param alloc_to_pinned true if the session should allocate to pinned segments, false otherwise
       */
      void set_alloc_to_pinned(bool alloc_to_pinned) { _alloc_to_pinned = alloc_to_pinned; }

     private:
      seg_allocator& _sega;
      uint32_t       _session_num;  // index into _sega's active sessions list
      bool           _alloc_to_pinned = true;

      segment_number                 _alloc_seg_num  = -1ull;
      mapped_memory::segment_header* _alloc_seg_ptr  = nullptr;
      mapped_memory::segment_meta*   _alloc_seg_meta = nullptr;
      bool                           _in_alloc       = false;

      // Reference to the session read lock from read_lock_queue
      mapped_memory::session_rlock& _session_rlock;
      int                           _nested_read_lock = 0;

      // Reference to the read cache queue from seg_allocator
      rcache_queue_type& _rcache_queue;
   };
}  // namespace arbtrie
