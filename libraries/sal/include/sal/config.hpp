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
    * @brief Controls the ACID durability guarantee for committed data.
    *
    * Each level adds progressively stronger guarantees at the cost of
    * write latency and SSD wear. Choose based on your failure model:
    *
    * | Level        | App crash | OS crash  | Power loss | Write latency     |
    * |--------------|-----------|-----------|------------|-------------------|
    * | none         | No        | No        | No         | Zero (no syscall) |
    * | mprotect     | Yes       | No        | No         | ~microseconds     |
    * | msync_async  | Yes       | Probably  | Probably   | ~microseconds     |
    * | msync_sync   | Yes       | Mostly    | Mostly     | ~milliseconds     |
    * | fsync        | Yes       | Yes       | Mostly*    | ~milliseconds     |
    * | full         | Yes       | Yes       | Yes        | ~10s of ms        |
    *
    * *fsync flushes to the drive controller but the drive may still cache
    * data in its volatile write buffer. full (F_FULLFSYNC on macOS) asks
    * the drive to flush its cache to physical media.
    */
   enum class sync_type
   {
      none        = 0,  ///< No sync. Data persists when the OS flushes dirty pages (process exit, memory pressure).
      mprotect    = 1,  ///< Write-protect committed pages via mprotect(PROT_READ). Stray writes cause SIGSEGV.
      msync_async = 2,  ///< msync(MS_ASYNC): hint to OS to flush soon, non-blocking. No hard guarantee.
      msync_sync  = 3,  ///< msync(MS_SYNC): block until OS has written pages to its disk cache.
      fsync       = 4,  ///< fsync(): block until OS has sent data to the drive controller.
      full        = 5,  ///< F_FULLFSYNC (macOS) / fsync + drive cache flush: data on physical media.
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
    * @brief Runtime-tunable parameters for database behavior.
    *
    * These can be changed after database creation via database::set_runtime_config().
    * All fields have sensible defaults for general-purpose workloads.
    */
   struct runtime_config
   {
      /** @name Cache & Memory */
      ///@{

      /**
       * @brief Maximum RAM reserved for pinned (mlock'd) cache segments, in MB.
       *
       * Hot objects are physically relocated into mlock'd segments that are
       * guaranteed to stay in RAM. This budget controls how much RAM the MFU
       * cache can consume. Each 32 MB segment within this budget is mlock'd.
       *
       * - Must be a multiple of the segment size (32 MB).
       * - Should be less than total system RAM to avoid mlock() failures.
       * - More pinned cache = more of the hot working set guaranteed in RAM.
       * - With many writer threads, budget at least 64 MB per thread for
       *   write segments plus headroom for promoted hot data.
       *
       * Default: 8192 MB (8 GB = 256 pinned segments).
       */
      uint64_t max_pinned_cache_size_mb = 1024 * 8;

      /**
       * @brief Duration of the MFU cache decay window, in seconds.
       *
       * A background thread clears the access-tracking bits (active and
       * pending_cache) on a rolling schedule. Objects must be accessed
       * frequently enough within this window to be promoted to pinned cache.
       *
       * - Longer window: slower to adapt to changing access patterns, but
       *   less CPU overhead and less SSD wear from unnecessary promotions.
       * - Shorter window: faster adaptation, but more background work and
       *   potential for thrashing if the hot set is larger than the cache.
       *
       * Default: 18000 seconds (5 hours).
       */
      uint64_t read_cache_window_sec = 60 * 60 * 5;

      /**
       * @brief Enable MFU-based read cache promotion.
       *
       * When true, read operations probabilistically mark frequently-accessed
       * objects for promotion to pinned (mlock'd) segments. The actual
       * relocation is done by the background compactor, so reader overhead
       * is minimal (a probabilistic check + queue push).
       *
       * Disable this if the entire dataset fits in RAM and you want to
       * eliminate all background promotion activity.
       *
       * Default: true.
       */
      bool enable_read_cache = true;

      ///@}

      /** @name Durability & Write Protection */
      ///@{

      /**
       * @brief The sync level applied when transactions commit.
       *
       * Controls the durability guarantee for committed data. See sync_type
       * for the full hierarchy from none (fastest, no crash safety) through
       * full (slowest, survives power loss).
       *
       * This is the default for the database; individual write sessions can
       * override it via write_session::set_sync().
       *
       * Default: sync_type::none (data persists at OS discretion).
       */
      sync_type sync_mode = sync_type::none;

      /**
       * @brief Write-protect committed pages even when sync_mode is none.
       *
       * When true, committed segments are marked PROT_READ via mprotect().
       * Any stray write to committed data (e.g. from a bug) triggers SIGSEGV
       * immediately rather than silently corrupting the database.
       *
       * This also forces copy-on-write for all mutations to committed data,
       * which is already the normal path — the overhead is the mprotect()
       * syscall itself (~microseconds per segment transition).
       *
       * Only has effect when sync_mode is none. Higher sync modes already
       * write-protect as part of the sync process.
       *
       * Default: true.
       */
      bool write_protect_on_commit = true;

      ///@}

      /** @name Checksums & Integrity */
      ///@{

      /**
       * When true, every user commit checksums the newly-frozen
       * region of each dirty segment (XXH3-64). This detects
       * corruption of data at rest but costs ~3-4% throughput
       * at typical batch sizes. Disable for maximum write speed
       * when the compactor's checksums provide sufficient coverage.
       */
      bool checksum_on_commit = false;

      /**
       * When true, the compactor checksums each segment it writes
       * after copying live objects into it. Because compaction runs
       * in the background, this is essentially free from the user's
       * perspective and ensures all data at rest is checksummed
       * once compaction catches up.
       */
      bool checksum_on_compact = true;

      /**
       * @brief Update object checksums on every upsert (not just commit).
       *
       * When true, checksums are recomputed immediately on every mutation.
       * This catches corruption sooner but adds overhead to the hot write
       * path. Usually unnecessary — background compaction and commit-time
       * checksumming cover most cases.
       *
       * Default: false.
       */
      bool update_checksum_on_upsert = false;

      /**
       * @brief Validate object checksums during background compaction.
       *
       * When the compactor relocates an object, it verifies the checksum
       * before copying. If corruption is detected, the compactor halts and
       * the database throws corruption_error on the next write, giving the
       * application a chance to recover.
       *
       * Default: true.
       */
      bool validate_checksum_on_compact = true;

      /**
       * @brief Recompute object checksums during background compaction.
       *
       * When the compactor copies an object to a new location, it can
       * recompute the checksum to ensure accuracy. Since compaction runs
       * in a background thread, the CPU cost does not affect foreground
       * latency.
       *
       * Default: true.
       */
      bool update_checksum_on_compact = true;

      /**
       * @brief Recompute checksums on every in-place modification.
       *
       * Only applies when a node has ref_count == 1 (unique ownership)
       * and is modified in place rather than copied. Usually false —
       * commit-time or compaction-time checksumming is sufficient.
       *
       * Default: false.
       */
      bool update_checksum_on_modify = false;

      ///@}

      /** @name Compaction Thresholds */
      ///@{

      /**
       * @brief Free-space threshold before compacting pinned segments, in MB.
       *
       * Pinned (mlock'd) segments hold the hot working set in RAM. Free
       * space in pinned segments wastes precious cache budget, so the
       * compactor is aggressive about defragmenting them.
       *
       * When a pinned segment has at least this much free space, it becomes
       * eligible for compaction (live objects are copied to a new segment
       * and the old segment is recycled).
       *
       * - Too high: RAM wasted on free space within pinned segments.
       * - Too low: excessive compaction of pinned data, consuming memory
       *   bandwidth. If sync_mode > none, also causes SSD wear.
       *
       * Default: 4 MB (~12.5% of a 32 MB segment).
       */
      uint8_t compact_pinned_unused_threshold_mb = 4;

      /**
       * @brief Free-space threshold before compacting unpinned segments, in MB.
       *
       * Unpinned segments are subject to OS page-cache eviction (4 KB pages).
       * Disk space is cheaper than RAM, so the compactor is lazier here to
       * minimize SSD wear from unnecessary data movement.
       *
       * When an unpinned segment has at least this much free space, it
       * becomes eligible for compaction.
       *
       * Default: 16 MB (50% of a 32 MB segment — only compact when you can
       * combine 2 segments into 1).
       */
      uint8_t compact_unpinned_unused_threshold_mb = 16;

      ///@}

      /** @name Virtual Address Space */
      ///@{

      /**
       * @brief Maximum database size, which determines how much virtual address
       * space is reserved up front for the segment file.
       *
       * The segment allocator reserves this much contiguous virtual address
       * space (PROT_NONE) at startup so that every future MAP_FIXED call lands
       * within the reserved region. If the OS cannot satisfy the full request,
       * it retries with half the size until it succeeds, then caps the usable
       * database size accordingly.
       *
       * On machines with limited overcommit or when running many databases in
       * one process (e.g. tests), a smaller value reduces VAS pressure and
       * prevents fragmentation.
       *
       * Default: sal::max_database_size (32 TiB).
       */
      int64_t max_database_size = 32LL * 1024 * 1024 * 1024 * 1024;  // 32 TiB

      ///@}
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
