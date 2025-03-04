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
#include <arbtrie/sync_lock.hpp>
#include <thread>
#include <vector>

/**
 *  @file seg_allocator.hpp
 *
 *  Responsible for allocating large segments of memory (multiple MB power of 2), each
 *  segment in turn stores objects pointed to from the id_allocator.
 *
 *  1. Each thread has its own session and allocates to its own
 *     segment in an append-only manner.
 *  2. Once an object has been written to a segment and its location
 *     exposed to the id_allocator it is considered immutible by
 *     the segment allocator, apps may still mutate it if they independently
 *     verify that only one thread is reading it and they lock the id while
 *     modifying it so that the gc thread doesn't try to compact it.
 *          - this should be unlikely because all modify in place operations
 *          occur with temporary, uncommited data which will likely be in
 *          the active allocation segment where it won't be moved anyway
 *  3. Once a segment is full, it is marked as read-only to the seg_allocator until
 *     its data is no longer referenced and the segment can be 
 *     recycled. E.g. no new allocation will write over it.
 *  4. When data is read, it is copied to the current alloc segment unless
 *     another thread locked it first. Once copied the
 *     item's location in the object db is updated and it is unlocked.
 *
 *     No threads need to wait for the copy because the data in the old location and new location
 *     are identical and the reader already has a "lock" in the form of object reference counts
 *     that force it to clone to modify anything with 2+ references. 
 *
 *  5. A garbage-collector (GC) thread finds the most empty segment and moves 
 *     all of the objects that remain to its own segment, then makes the
 *     segment available for reuse by other threads (once all threads have
 *     released the implied write lock)
 *
 *
 *    Segment Recycle Buffer 
 *
 *      [==N=R1---R2--A]
 *
 *        N = position of next segment to be reused on alloc
 *        R1..RN = the last position of A observed by each thread
 *        A = position the next segment to be recycled gets pushed to
 *
 *          1. new segment is allocated (mmap) 
 *          2. data is written until it is full (seg.alloc_p reaches end)
 *          3. nodes are released marking potential free space
 *          4. compactor copies remaining data to another new segment 
 *          5. compactor pushes segment into queue for reallocation and increments A ptr
 *          6. read threads 'lock' the reallocation queue by observing A the
 *              position of the last segment pushed to the queue and copying to
 *              a thread-local variable
 *          7. when a new segment needs to be allocated it can only recycle
 *              segments if all threads have observed the updated recycle 
 *              queue position; therefore, it pulls from N and advances N
 *              until the last one it pulls is N == R1 and N = R1+1. 
 *       
 *       A is really N copies of A, one per thread and shares the same
 *       64 bit value as NX & RX. The compactor reads RXAX and writes
 *       RX(AX+1) with memory order release. 
 *
 *       Nodes lock by reading RXAX and storing AXAX
 *
 *         
 *
 *
 *        
 *  6. the Object ID allocation system can be made thread safe by giving
 *     each "writing session" a "segment" of the object id space. Writers would
 *     only have to synchronize id allocation requests when their segments are full.
 *
 *  Theory: 
 *      a. data will be organized in the order it tends to be accessed in
 *      b. infrequently modified data will be grouped together by GC 
 *          - because as things are modified they are moved away 
 *      c. the most-recent N segments can have their memory pinned
 *      d. madvise can be effeciently used to mark alloc segmentsfor
 *           SEQ and/or pin them to memory. It can also mark segments as
 *           RANDOM or UNNEEDED to improve OS cache managment.
 *
 *   Future Work:
 *      0. Large objects should not get moved around and could utilize their own allocation
 *         system... or at the very least get grouped together because they are less likely to
 *         be modified than smaller objects.
 *      1. separating binary nodes and value nodes from other inner node types reduces probability of
 *         loading unecessary data from disk.
 *      2. as nodes are being read set a "read bit", then when compacting, move read nodes to different
 *         segments from unread ones. 
 *              - take a bit from the reference count in node_meta
 *                   a. this would not result in an extra cacheline miss
 *                   b. this could be lazy non-looping compare & swap to prevent
 *                      readers/writers from stomping on eachother. 
 *                   c. the read bit only needs to modified if not already set
 *                   d. the read bit only needs to be set on segments not mmapped
 *                   e. the read bit gets automatically cleared when node is moved
 *                   f. generate a random number and only update the read bit some of the time.
 *                             - the probability of updating the read bit depends upon how "cold"
 *                             the object is (age of segment). Colder segments are less likely to
 *                             mark an object read because of one access. But if an object "becomes hot"
 *                             then it is more likely to get its read bit set.
 *              - add meta data to the segment that tracks how many times the objects
 *                in the segment have been moved due to lack of access. When compacting
 *                a segment you add one to that source segments "access age" for the segment
 *                that is receiving the idol segments. 
 *              - when selecting a segment to compact there are several metrics to consider:
 *                   a. what percentage of the segment has been freed and therefore net recaptured space
 *                   b. what percentage of remaining data has been read at least once
 *                          - a segment that is 50% read has greater sorting effeciency than
 *                            a segment that is 10% or 90% read. 
 *                   c. the number of times the objects in the segment have been moved because
 *                      of lack of access. 
 *
 */
namespace arbtrie
{
   class seg_alloc_session;

   class seg_allocator
   {
     protected:
      friend class database;
      void    release_unreachable();
      void    reset_meta_nodes(recover_args args);
      void    reset_reference_counts();
      int64_t clear_lock_bits();
      void    sync_segment(int seg, sync_type st) noexcept;

     public:
      // only 64 bits in bitfield used to allocate sessions
      // only really require 1 per thread
      static const uint32_t max_session_count = 64;
      // using session                           = seg_alloc_session;

      seg_allocator(std::filesystem::path dir);
      ~seg_allocator();

      void           print_region_stats() { _id_alloc.print_region_stats(); }
      uint64_t       count_ids_with_refs() { return _id_alloc.count_ids_with_refs(); }
      seg_alloc_dump dump();
      void           sync(sync_type st = sync_type::sync);
      void           start_compact_thread();
      void           stop_compact_thread();
      bool           compact_next_segment();

      seg_alloc_session start_session() { return seg_alloc_session(*this, alloc_session_num()); }

     private:
      friend class seg_alloc_session;
      friend class read_lock;
      friend class modify_lock;
      friend class object_ref;

      std::optional<seg_alloc_session> _compactor_session;

      mapped_memory::segment_header* get_segment(segment_number seg) noexcept
      {
         return static_cast<mapped_memory::segment_header*>(_block_alloc.get(seg));
      }

      uint32_t alloc_session_num();
      void     release_session_num(uint32_t sn);

      std::pair<segment_number, mapped_memory::segment_header*> get_new_segment();

      void compact_loop2();
      void compact_loop();
      void compact_segment(seg_alloc_session& ses, uint64_t seg_num);
      void promote_rcache_data();
      void clear_read_bits_loop();
      void attempt_truncate_empty();

      /**
       * Calculate statistics about read bits in a segment
       * @param seg_num The segment number to analyze
       * @return A pair containing {number of node headers with read bit set, total bytes of those nodes}
       */
      std::pair<uint32_t, uint64_t> calculate_segment_read_stats(segment_number seg_num);

      /**
       * This must be called via a session because the session is responsible
       * for documenting what regions could be read
       *
       * All objects are const because they cannot be modified after being
       * written.
       */
      const node_header* get_object(node_location loc) const;

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

      /**
       * finds the most empty segment that is at least 25% empty
       * - marks it for sequential access
       * - scans it for remaining objects, moving them to a new region
       * - releases segment
       * - marks it as unneeded 
       *
       * and moves its contents to
       * a new segment owned by the gc thread th
       */
      std::thread _compact_thread;

      // maps ids to locations
      id_alloc _id_alloc;

      // allocates new segments
      block_allocator _block_alloc;

      // Thread for clearing read bits
      std::thread _read_bit_clearer;

      // Duration for cache frequency window (default 1 minute)
      std::chrono::milliseconds _cache_frequency_window{60000};

      // keeps track of the segments that are mlocked....
      // TODO: this needs to get refactored
      alignas(ARBTRIE_CACHE_LINE_SIZE) std::atomic<uint32_t> _total_mlocked = 0;
      alignas(ARBTRIE_CACHE_LINE_SIZE) std::array<std::atomic<int32_t>, 256> _mlocked;

      /**
       *  This is the highest the alloc_ptr is allowed to
       *  advance and equal to min value of thread_ptrs.
       *
       *  Do not read directly, read via get_min_read_ptr()
       */
      std::atomic<uint64_t> _min_read_ptr = -1ull;  // min(R*)
      uint64_t              get_min_read_ptr();
      void                  set_session_end_ptrs(uint32_t e);

      // Track total bytes copied during promote_rcache_data operations
      uint64_t _total_promoted_bytes{0};

      // Difficulty threshold for read bit updates (0-4294967295)
      std::atomic<uint32_t> _cache_difficulty{uint32_t(-1) -
                                              (uint32_t(-1) / 16)};  // 1 in 16 probability

      /**
       * Each session has its own read cache queue to track read operations
       * for promoting data during compaction.
       */
      std::array<std::atomic<circular_buffer<uint32_t, 1024 * 1024>*>, 64> _rcache_queues{};

      std::atomic<bool> _done = false;

      mapping                          _header_file;
      mapped_memory::allocator_header* _header;
      std::mutex                       _sync_mutex;

      std::vector<sync_lock> _seg_sync_locks;
      std::vector<int>       _dirty_segs;
      std::mutex             _dirty_segs_mutex;
      uint64_t               _next_dirt_seg_index = 0;
      uint64_t               _last_synced_index   = 0;

   };  // seg_allocator

}  // namespace arbtrie
#include <arbtrie/read_lock_impl.hpp>
#include <arbtrie/seg_alloc_session_impl.hpp>
