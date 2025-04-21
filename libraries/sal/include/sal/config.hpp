#pragma once
#include <unistd.h>
#include <cstdint>
#include <cstring>
#include <hash/xxh32.hpp>
#include <iostream>
#ifdef _WIN32
#include <sys/sysinfo.h>
#endif

namespace sal
{
   // On M2+ macs this is 128, use std::hardware_destructive_interference_size
   // if you need the real cacheline size, we assume 64 for most x86 architectures
   // even though Intel also fetches 128 bytes to L3 cache.
   constexpr uint32_t cacheline_size = 64;

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

      /**
       * on Apple M2+ this is 16384, but most other systems are 4096
       */
      inline size_t os_page_size()
      {
         return page_size;
      }
      inline constexpr std::size_t round_to_page(std::size_t arg)
      {
         return ((arg + os_page_size() - 1) / os_page_size()) * os_page_size();
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
      none        = 0,        // on program close or as OS chooses
      mprotect    = 1,        // mprotect() will be used to write protect the data
      msync_async = 2,        // nonblocking, but write soon
      msync_sync  = 3,        // block until changes are committed to disk
      fsync       = 4,        // in addition to msync(MS_SYNC) tells the OS to
                              // sync the data to the physical disk. Note that while the
                              // OS will have sent all data to the drive, this does not
                              // gaurantee that the drive hasn't cached the data and it
                              // may not be on the drive yet.
      full              = 5,  // F_FULLSYNC (Mac OS X), in addition to fsync() asks the
      default_sync_type = msync_sync
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

   enum class access_mode
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
       * The default is 1 GB, this gives 32 segments,
       * if you have a lot of write threads you may want to 
       * increase this to 64 MB per thread or more. The
       * more the better, but this should be less than
       * the system memory or you will start seeing errors
       * in the logs about mlock() failing.
       * 
       * This should be a multiple of the segment size
       */
      uint64_t max_pinned_cache_size_mb = 1024 * 8;

      /**
       * The default is 1 hour, and this impacts the
       * rate of cache eviction and the amount of SSD
       * wear.  Longer windows are slower to adapt to
       * changing access patterns, but are more effecient
       * with respect to CPU and SSD wear. 
       */
      uint64_t read_cache_window_sec = 60 * 60 * 5;

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
       * Every commit advances the write-protected region of
       * memory, at this time there is an opportunity to calculate
       * the checksum of the segment(s) that are being frozen; however
       * this information is only useful for detecting corruption, and
       * not recovering from corruption. 
       * 
       * Indepdnent of this checksum there is also a 1 byte checksum
       * on every key/value pair that is stored in binary nodes, and
       * each node also has a 1 byte checksum which is updated on
       * commit. 
       * 
       * This is more expensive, but it will detect corruption
       * of data at rest. This is about a 10% performance hit.
       */
      bool checksum_commits = false;

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

   /**
    * This will slow down performance, but ensures the checksum should
    * be accurate at all times. If this is not set, the checksum will
    * be zeroed on modify until a later point (eg. compaction)
    * chooses to update it.
    */
   static constexpr const bool update_checksum_on_modify = false;

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
   static constexpr const uint64_t max_database_size = 32 * TB;

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

   // the maximum object size that can be allocated in a segment
   // generally limited to half the segment size (16MB)
   static constexpr const uint64_t max_object_size = segment_size / 2;
   static_assert(max_object_size <= segment_size / 2);

   struct config_state
   {
      int64_t  max_database_size = sal::max_database_size;
      uint32_t max_threads       = sal::max_threads;
      uint32_t cacheline_size    = sal::cacheline_size;
      uint32_t segment_size      = sal::segment_size;
   };
   inline const std::uint32_t file_magic = ([](){
      static constexpr const config_state state;
      char buffer[sizeof(state)];
      std::memcpy( buffer, &state, sizeof(state) );
      return xxh32::hash( buffer, sizeof(state), 0 );
   })();
}  // namespace sal
