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
      latest,      // frozen RW snapshot + Tri (one swap behind writer)
      buffered,    // same as latest (frozen RW snapshot + Tri)
      trie,        // Tri only — zero DWAL overhead
   };

   /// Transaction mode: buffered writes vs direct COW.
   enum class transaction_mode
   {
      buffered,  // Normal: writes go to RW btree + WAL
      direct,    // Large tx: flush RW, write directly to PsiTri COW
   };

   /// Per-root DWAL state.
   ///
   /// Single writer owns the RW btree exclusively (unique_ptr, no sharing).
   /// On swap, the RW btree is frozen and published as a shared_ptr for readers.
   /// Readers check an atomic generation counter to avoid locking when nothing
   /// has changed — the mutex is only acquired once per swap cycle to copy the
   /// shared_ptr.
   ///
   /// Cache-line aligned to prevent false sharing between writer and reader paths.
   struct dwal_root
   {
      // ── Writer-private section (only writer touches these) ─────────

      /// The hot RW btree. Protected by rw_mutex: writer takes exclusive,
      /// readers (get_latest, latest-mode cursors) take shared.
      /// Created via make_shared so the control block is co-allocated;
      /// on swap it moves directly to buffered_ptr with no extra allocation.
      std::shared_ptr<btree_layer> rw_layer;

      /// Protects rw_layer for concurrent reader access.
      /// Writer takes exclusive lock for mutations; readers take shared lock
      /// for point lookups and cursor snapshot creation.
      /// Only used when enable_rw_locking is true (opt-in for MDBX-style readers).
      mutable std::shared_mutex rw_mutex;

      /// When true, writer acquires rw_mutex exclusively around transactions
      /// and readers acquire it shared. When false (default), no locking —
      /// the single-writer model assumes no concurrent RW-layer readers.
      bool enable_rw_locking{false};

      /// WAL writer for the current RW btree.
      std::unique_ptr<wal_writer> wal;

      /// Monotonically increasing sequence number for WAL entries.
      uint64_t next_wal_seq = 0;

      /// Set true by merge thread after it finishes draining and nulls buffered_ptr.
      /// Checked by writer on commit — if true, writer swaps RW→RO.
      std::atomic<bool> merge_complete{true};

      /// Per-root transaction-level read/write exclusion.
      /// Writers take exclusive, readers take shared.
      /// Acquired in sorted root-index order to prevent deadlocks.
      std::shared_mutex tx_mutex;

      // ── Reader section (separated to avoid false sharing) ──────────
      alignas(128) std::shared_mutex buffered_mutex;

      /// The frozen RO btree — published on swap, read by latest/buffered readers.
      /// Protected by buffered_mutex. Readers copy the shared_ptr then release.
      std::shared_ptr<btree_layer> buffered_ptr;

      /// Generation counter — incremented on each swap.
      /// Readers compare against their cached gen to skip the mutex when unchanged.
      std::atomic<uint32_t> generation{0};

      /// The PsiTri root address — updated atomically when merge completes.
      std::atomic<uint32_t> tri_root{0};

      /// Base root of the current RO btree — the PsiTri root at swap time.
      std::atomic<uint32_t> ro_base_root{0};

      dwal_root() : rw_layer(std::make_shared<btree_layer>()) {}
   };

}  // namespace psitri::dwal
