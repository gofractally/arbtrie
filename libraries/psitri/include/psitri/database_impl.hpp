#pragma once
#include <psitri/database.hpp>

namespace psitri
{
   namespace detail
   {
      /// Bit flags for database_state::flags
      enum db_flags : uint32_t
      {
         flag_ref_counts_stale = 1u << 0,  ///< ref counts need cleanup (deferred_cleanup)
      };

      struct database_state
      {
         uint32_t          magic          = sal::file_magic;
         uint32_t          flags          = 0;
         std::atomic<bool> clean_shutdown = true;
         runtime_config    config;

         /// Global monotonic MVCC version counter, incremented on every commit.
         /// The version number is stored in a custom control block (via alloc_custom_cb)
         /// and the ver_adr is packed into the root slot alongside root_adr.
         std::atomic<uint64_t> global_version{0};

         /// Epoch = global_version / epoch_interval.  Inner nodes store the epoch
         /// at which they were last COW'd.  The first write after an epoch boundary
         /// to a path with stale inner nodes triggers a full COW cascade, providing
         /// an opportunity for structural maintenance (merge, collapse, rebalance,
         /// pruning multi-version value_nodes).
         ///
         /// Trade-off: smaller interval → more frequent COW maintenance (less bloat,
         /// lower MVCC throughput).  Larger → more MVCC fast-path hits (higher
         /// throughput, more temporary version accumulation).
         ///
         /// Rule of thumb: set to >= the number of unique keys in the hot set.
         /// Default 1M provides a maintenance pass roughly every 1M writes.
         uint64_t epoch_interval = 1'000'000;

         uint64_t current_epoch() const noexcept
         {
            return global_version.load(std::memory_order_relaxed) / epoch_interval;
         }
      };
   }  // namespace detail

   inline void database::set_epoch_interval(uint64_t interval)
   {
      _dbm->epoch_interval = interval;
   }

   inline uint64_t database::current_epoch() const
   {
      return _dbm->current_epoch();
   }
}  // namespace psitri