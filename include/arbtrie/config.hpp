#pragma once
#include <unistd.h>
#include <arbtrie/hash/xxh32.hpp>
#include <bit>
#include <cstddef>
#include <iostream>
#include <string_view>

namespace arbtrie
{

   namespace system_config
   {
      static const size_t page_size = []() -> size_t
      {
#ifdef _WIN32
         SYSTEM_INFO si;
         GetSystemInfo(&si);
         return si.dwPageSize;
#else
         return sysconf(_SC_PAGESIZE);
#endif
      }();

      const uint32_t os_page_size_log2 = []() -> uint32_t { return std::countr_zero(page_size); }();

      inline size_t os_page_size()
      {
         return page_size;
      }
   }  // namespace system_config

   /**
    * For ACID **Durablity** requriments this configures
    * how agressive arbtrie will be in flushing data to disk 
    * and protecting data from corruption.
    * 
    * 0. none - fastests (no system calls) but least protection,
    *    you must be sure your program will not write to the database's
    *    mapped memory except during a commit() call, mprotect() is probably
    *    worth doing as it doesn't have much overhead. 
    * 1. mprotect - mprotect() will be used to write protect the
    *    data in memory once committed. This will prevent application
    *    code from modifying the data and corrupting the database. This is
    *    the level that assumes the OS will not crash or power cut. Even if
    *    your app crashes, your data is safe.
    * 2. msync_async - msync(MS_ASYNC) will be used which will tell
    *    the OS to write as soon as possible without blocking
    *    the caller. This only flushes to the OS disk-cache and
    *    does not gaurantee that the data is on disk.
    * 3. msync_sync - msync(MS_SYNC) will be used to block caller
    *    until the OS has finished its msync() to disk cache.
    * 4. fsync - in addition to msync(MS_SYNC) tells the OS to
    *    sync the data to the physical disk. Note that while the
    *    OS will have sent all data to the drive, this does not
    *    gaurantee that the drive hasn't cached the data and it
    *    may not be on the drive yet. 
    * 5. full - F_FULLSYNC (Mac OS X), in addition to fsync() asks the 
    *    drive to flush all data to the physical media, this will
    *    sync all data from all processes on the system not just
    *    the current process.
    */
   enum class sync_type
   {
      none        = 0,  // on program close or as OS chooses
      mprotect    = 1,  // mprotect() will be used to write protect the data
      msync_async = 2,  // nonblocking, but write soon
      msync_sync  = 3,  // block until changes are committed to disk
      fsync       = 4,  // in addition to msync(MS_SYNC) tells the OS to
                        // sync the data to the physical disk. Note that while the
                        // OS will have sent all data to the drive, this does not
                        // gaurantee that the drive hasn't cached the data and it
                        // may not be on the drive yet.
      full = 5          // F_FULLSYNC (Mac OS X), in addition to fsync() asks the
   };
   inline std::ostream& operator<<(std::ostream& os, sync_type st)
   {
      switch (st)
      {
         case sync_type::none:
            return os << "none";
         case sync_type::mprotect:
            return os << "mprotect";
         case sync_type::msync_async:
            return os << "msync_async";
         case sync_type::msync_sync:
            return os << "msync_sync";
         case sync_type::fsync:
            return os << "fsync";
         case sync_type::full:
            return os << "full";
         default:
            return os << "unknown";
      }
   }

   inline std::istream& operator>>(std::istream& is, sync_type& st)
   {
      std::string str;
      is >> str;
      if (str == "none")
         st = sync_type::none;
      else if (str == "mprotect")
         st = sync_type::mprotect;
      else if (str == "msync_async")
         st = sync_type::msync_async;
      else if (str == "msync_sync")
         st = sync_type::msync_sync;
      else if (str == "fsync")
         st = sync_type::fsync;
      else if (str == "full")
         st = sync_type::full;
      else
         is.setstate(std::ios::failbit);
      return is;
   }

   enum access_mode
   {
      read_only  = 0,
      read_write = 1
   };

   /**
    * Parameters that can be changed at runtime.
    */
   struct runtime_config
   {
      /**
       * The point at which a binary node will get converted into
       * a full node or setlist node (depending on keys).
       */
      uint32_t binary_refactor_threshold      = 4096;
      uint32_t binary_node_max_size           = 4096;
      uint32_t binary_node_max_keys           = 254;
      uint32_t binary_node_initial_size       = 4096;
      uint32_t binary_node_initial_branch_cap = 64;

      /**
       * The default is 1 GB, this gives 32 segments,
       * if you have a lot of write threads you may want to 
       * increase this to 64 MB per thread or more. The
       * more the better, but this should be less than
       * the system memory or you will start seeing errors
       * in the logs about mlock() failing.
       * 
       * This should be a multiple of the segment size
       */
      uint64_t max_pinned_cache_size_mb = 1024;

      /**
       * The default is 1 hour, and this impacts the
       * rate of cache eviction and the amount of SSD
       * wear.  Longer windows are slower to adapt to
       * changing access patterns, but are more effecient
       * with respect to CPU and SSD wear. 
       */
      uint64_t read_cache_window_sec = 60 * 60;

      /**
       * When true, read operations will promote
       * the most frequently accessed data to pinned
       * cache. This has minimal overhead for readers,
       * because the work is offloaded to background thread,
       * but may cause additional SSD wear and consume
       * some memory bandwidth. Having a large 
       * max_pinned_cache_size_mb will minimize the
       * SSD wear when used in conjunection with 
       * sync_mode::none
       */
      bool enable_read_cache = true;

      /**
       * When true, the database will write protect the
       * data that has been committed even if it is not
       * being actively msync() to disk. This prevents stray
       * writes from other parts of the process from corrupting
       * the database memory, but it comes at the cost of 
       * increasing the amount of Copy on Write utilized and
       * there is a small amount of overhead in system calls
       * updating the memory protection.
       * 
       * This only has effect when sync_mode is "none", because
       * we have to ensure that once data is synced that we
       * don't modify it again.
       */
      bool write_protect_on_commit = true;

      /**
       * 0 = none, 
       *    fastest, least SSD wear,
       *    enables write_protect_on_commit option
       *    data may not persist until program exit.
       *    safe as long as OS doesn't crash or power loss
       * 1 = async, background msync(), most data gets to disk
       *    the OS gets the data to disk ASAP, but without blocking
       *    the database will be slower, more SSD wear, but likely
       *    most data will be recoverable even after a power loss
       * 2 = sync, block until data is on disk
       *    the database will be slower, more SSD wear, but the in
       *    theory the most durable, but most OS's will not even
       *    fully gaurantee that the data is on the physical disk 
       *    limits according to the msync(MS_SYNC) documentation and
       *    each OS and hardware configuration is different.
       */
      sync_type sync_mode = sync_type::none;

      /**
       * Calculating the checksum is expensive and mostly
       * used to detect corruption of data at rest, generally
       * we can rely upon background processes to keep the checksums
       * up to date to minimize latency for the user.
       */
      bool update_checksum_on_upsert = false;

      /**
       * This is a perfect opportunity to discover corruption
       * early and will halt the process when corruption is detected
       * and give the user a chance to recover.
       */
      bool validate_checksum_on_compact = true;

      /**
       * This uses more CPU, but it is in the background so it
       * is worth having accurate checksums.
       */
      bool update_checksum_on_compact = true;
      bool update_checksum_on_modify  = false;

      /**
       * This determines the tolerance of freed data in the
       * mlock() pages before the compactor will move the
       * remaining unpinned data to a new segment. 
       * 
       * If this is set too high, a lot of RAM will be wasted
       * and not helping with performance.
       * 
       * If this is set too low, the compactor will be agressive
       * and may move data around more than necessary, consuming
       * memory bandwidth and may also cause more SSD wear, if you
       * are using anything other than sync_type::none because the
       * OS will have to flush the moved data to disk, even though
       * it is mlock() for read performance. 
       * 
       * The default is 4MB, which means the compactor will not
       * compact a segment unless it can convert 8 segments into
       * 7 or fewer segments.
       * 
       * TODO: redefine the algorithm for the compactor such that
       * it will always compact when it can produce at least 1
       * recycled segment as a result. 
       */
      uint8_t compact_pinned_unused_threshold_mb = 4;

      /**
       * Unpinned data is not mlocked() and is therefore subject to
       * the OS page cache eviction policies which operate on
       * a 4096 page level. This threshold should be high enough
       * that the compactor will not move data around too often 
       * causing SSD wear. By default this is set to 50% of the
       * segment size, meaning that the compactor will not compact
       * unless it can combine 2 segments into 1.
       */
      uint8_t compact_unpinned_unused_threshold_mb = 16;
   };

   // designed to fit within 4096 bytes with other header information
   // so msync the page doesn't waste data.
   static constexpr const uint32_t num_top_roots = 1024;

   /**
    * This will slow down performance, but ensures the checksum should
    * be accurate at all times. If this is not set, the checksum will
    * be zeroed on modify until a later point (eg. compaction, or setroot)
    * chooses to update it.
    */
   static constexpr const bool update_checksum_on_modify = false;

   static constexpr const bool use_binary_nodes = true;
   /**
    *  Checksum's are deferred until just before msync so that data
    *  at rest always has a checksum. The idea is that until the user
    *  chooses to flush to disk there is no gaurantee that the data
    *  will survivie a hardware crash.
    */
   static constexpr const bool update_checksum_on_msync   = false and not update_checksum_on_modify;
   static constexpr const bool update_checksum_on_compact = true and not update_checksum_on_modify;
   static constexpr const bool validate_checksum_on_compact = true;

   static_assert(not(update_checksum_on_msync and update_checksum_on_modify));
   static_assert(not(update_checksum_on_compact and update_checksum_on_modify));

   static constexpr const uint64_t MB = 1024ull * 1024ull;
   static constexpr const uint64_t GB = 1024ull * MB;
   static constexpr const uint64_t TB = 1024ull * GB;

   // On M2+ macs this is 128, use std::hardware_destructive_interference_size
   // if you need the real cacheline size, we assume 64 for most x86 architectures
   static constexpr const uint32_t cacheline_size = 64;

   // the largest object that will attempt to be promoted to pinned cache,
   // the goal of the cache is to avoid disk cache misses. This would ideally
   // be the largest node size (full binary node) which really contains up to
   // 256 keys. Larger user values represent a single key/value pair and result
   // in at most 1 cache miss for the large object followed by sequential reads.
   static constexpr const uint32_t max_cacheable_object_size = 4096;

   /**
    *  Certain parameters depend upon reserving space for eventual growth
    *  of the database. 
    */
   static constexpr const uint64_t max_database_size = 8 * TB;

   // must be a power of 2
   // size of the data segments file grows
   //
   // the segment size impacts the largest possible value
   // to be stored, which can be no larger than 50% of the
   // segment size.
   //
   // the smaller this value, the more overhead there is in
   // searching for segments to compact and the pending
   // free queue.
   //
   // the larger this value the longer the stall when things
   // need to grow, but stalls happen less frequently. Larger
   // values also mean it takes longer to reclaim free space because
   // free space in an allocating segment cannot be reclaimed until
   // the allocations reach the end.  TODO: should the allocator
   // consider abandoing a segment early if much of what has been
   // allocated has already been freed?
   //
   // each thread will have a segment this size, so larger values
   // may use more memory than necessary for idle threads
   // max value: 4 GB due to type of segment_offset
   static constexpr const uint64_t segment_size      = 32 * MB;
   static const uint32_t           pages_per_segment = segment_size / system_config::os_page_size();

   /// object pointers can only address 48 bits
   /// 128 TB limit on database size with 47 bits, this saves us
   /// 8MB of memory relative to 48 bits in cases with less than 128 TB
   static constexpr const uint64_t max_segment_count = max_database_size / segment_size;

   /**
    * This impacts the number of reference count bits that are reserved in case
    * all threads attempt to increment one atomic variable at the same time and
    * overshoot.  This would mean 32 cores all increment the same atomic at
    * the same instant before any core can realize the overshoot and subtract out.
    *
    * The session allocation algo uses a 64 bit atomic to alloc session numbers,
    * so going beyond 64 would require a refactor of that code.
    */
   static constexpr const uint32_t max_threads = 64;

   //static constexpr const uint32_t os_page_size = 4096;
   /**
    * Each ID region can store 512 IDs before the ID
    * file needs to grow becuase each ID takes 8 bytes, making
    * this larger grows the minimum meta_index size by
    * 2^16 * id_page_size.  
    *
    * id_page_size of 4096 means 256MB minimum node_meta index size
    * that grows in increments of 256MB
    *
    * Each node must allocate all of their children into
    * the same region, so that means up to 256 children or
    * 2 full nodes will fill one page.  
    *
    * For this reason it is critical that nodes be evenly
    * spread across all regions to prevent premature growth to
    * 512MB or more just because one region is too dense.
    */
   static constexpr const uint32_t id_page_size = 4096;

   static_assert(segment_size < 4 * GB, "size must be less than 4GB");
   static_assert(std::popcount(segment_size) == 1, "size must be power of 2");
   // compacted
   static constexpr const uint64_t segment_empty_threshold = segment_size / 2;
   static_assert(segment_empty_threshold < segment_size);

   // the maximum value a node may have
   static constexpr const uint64_t max_value_size = segment_size / 2;
   static_assert(max_value_size <= segment_size / 2);

   // more than 1024 and the bit fields in nodes need adjustment
   static constexpr const uint16_t max_key_length = 1024;
   static_assert(max_key_length <= 1024);

   // the number of branches at which an inner node is automatically
   // upgraded to a full node, a full node has 2 bytes per branch,
   // and a setlist node has 1 byte over head per branch present.
   //
   // At 128 branches, a full node would be 128 bytes larger, but
   // a full node is able to dispatch faster by not having to
   // scane the setlist. 128 represents 2 cachelines in the setlist
   // that must be scanned. Since cacheline loading is one of the
   // major bottlenecks, this number should be a multiple of the
   // cacheline size so no loaded memory is wasted.
   //
   // In practice 128 was found to be a good speed/space tradeoff.
   static constexpr const int full_node_threshold = 128;

   static constexpr const uint64_t binary_refactor_threshold = 4096;
   static constexpr const uint64_t binary_node_max_size      = 4096;  // 1 page
   static constexpr const int      binary_node_max_keys      = 254;   /// must be less than 255
   static_assert(binary_refactor_threshold <= binary_node_max_size);

   // initial space reserved for growth in place, larger values
   // support faster insertion, but have much wasted space if your
   // keys are not dense.
   static constexpr const int binary_node_initial_size = 3072;

   // extra space reserved for growth in place
   static constexpr const int binary_node_initial_branch_cap = 64;

   static_assert(binary_node_max_keys < 255);

   using byte_type            = char;
   using key_view             = std::string_view;
   using value_view           = std::string_view;
   using segment_offset       = uint32_t;
   using segment_number       = uint64_t;
   using small_segment_number = uint32_t;

   struct recover_args
   {
      bool validate_checksum = false;
      bool recover_unsync    = false;
   };

   struct config_state
   {
      int64_t  max_database_size = arbtrie::max_database_size;
      uint32_t max_threads       = arbtrie::max_threads;
      uint32_t cacheline_size    = arbtrie::cacheline_size;
      uint32_t id_page_size      = arbtrie::id_page_size;
      uint32_t segment_size      = arbtrie::segment_size;
      uint32_t max_key_length    = arbtrie::max_key_length;
   };
   inline const std::uint32_t file_magic = ([](){
      static constexpr const config_state state;
      char buffer[sizeof(state)];
      std::memcpy( buffer, &state, sizeof(state) );
      return xxh32::hash( buffer, sizeof(state), 0 );
   })();
}  // namespace arbtrie
