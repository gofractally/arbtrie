#pragma once
#include <psitri/dwal/btree_layer.hpp>
#include <psitri/dwal/undo_log.hpp>
#include <psitri/dwal/wal_writer.hpp>
#include <sal/numbers.hpp>

#include <atomic>
#include <cstdint>
#include <memory>
#include <shared_mutex>

namespace psitri::dwal
{
   /// Read mode: which layers to include when reading.
   enum class read_mode
   {
      latest,      // RW + RO + Tri — shared lock on RW
      buffered,    // RO + Tri — no lock
      persistent,  // Tri only — zero DWAL overhead
   };

   /// Transaction mode: buffered writes vs direct COW.
   enum class transaction_mode
   {
      buffered,  // Normal: writes go to RW btree + WAL
      direct,    // Large tx: flush RW, write directly to PsiTri COW
   };

   /// Per-root DWAL state.
   ///
   /// Each of the 512 roots has its own independent DWAL state:
   /// - RW btree (guarded by shared_mutex)
   /// - RO btree (immutable after swap)
   /// - WAL writer
   /// - Undo log (per active transaction)
   /// - Atomic pointers for lock-free reader access
   struct dwal_root
   {
      /// Reader/writer lock for the RW btree.
      /// Writers hold exclusive for full transaction duration.
      /// Readers (read_mode::latest) hold shared for full read transaction.
      std::shared_mutex rw_mutex;

      /// The hot RW btree — actively being written to.
      std::unique_ptr<btree_layer> rw_layer;

      /// The frozen RO btree — being merged to PsiTri.
      /// Atomic pointer for lock-free reader access.
      /// Only non-null between swap and merge completion + reader drain.
      std::atomic<btree_layer*> ro_ptr{nullptr};

      /// The PsiTri root address — updated atomically when merge completes.
      /// Stored as raw uint32_t for atomic operations.
      std::atomic<uint32_t> tri_root{0};

      /// Base root of the current RO btree — the PsiTri root at swap time.
      /// Used by readers for staleness detection.
      std::atomic<uint32_t> ro_base_root{0};

      /// WAL writer for the current RW btree.
      std::unique_ptr<wal_writer> wal;

      /// Generation counter — incremented on each swap.
      std::atomic<uint32_t> generation{0};

      /// Monotonically increasing sequence number for WAL entries.
      uint64_t next_wal_seq = 0;

      /// Whether a merge is in progress for this root.
      std::atomic<bool> merge_active{false};

      /// Condition variable for backpressure — writer blocks if RO slot occupied.
      std::mutex              merge_mutex;
      std::condition_variable merge_cv;

      dwal_root() : rw_layer(std::make_unique<btree_layer>()) {}
   };

}  // namespace psitri::dwal
