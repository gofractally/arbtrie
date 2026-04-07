#pragma once
#include <art/cow_coordinator.hpp>
#include <psitri/dwal/btree_layer.hpp>
#include <psitri/dwal/undo_log.hpp>
#include <psitri/dwal/wal_writer.hpp>
#include <sal/numbers.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <shared_mutex>

namespace psitri::dwal
{
   /**
    * @brief Read mode: freshness vs cost tradeoff for DWAL readers.
    *
    *     Mode       Layers              Reader cost            Writer impact
    *     ---------  ------------------  ---------------------  ─────────────────
    *     trie       Tri only            none                   none
    *     buffered   frozen RO + Tri     atomic load            none
    *     fresh      prev_root + RO+Tri  atomic load            none
    *     latest     head + RO + Tri     wait ≤1 tx if active   COW if collision
    */
   enum class read_mode
   {
      trie,        ///< Tri only — zero DWAL overhead, most stale
      buffered,    ///< Frozen RO + Tri — no writer interaction
      fresh,       ///< prev_root + RO + Tri — zero coordination, slightly stale
      latest,      ///< Head root + RO + Tri — waits if writer active, forces COW on collision
   };

   /// Transaction mode: buffered writes vs direct COW.
   enum class transaction_mode
   {
      buffered,  // Normal: writes go to RW btree + WAL
      direct,    // Large tx: flush RW, write directly to PsiTri COW
   };

   /// Per-root DWAL state.
   ///
   /// Reader/writer coordination is handled lock-free by cow_coordinator.
   /// The only mutex is tx_mutex for per-root transaction serialization.
   struct dwal_root
   {
      // ── Writer-private section ────────────────────────────────────

      /// The hot RW btree. Writer-private during transactions.
      /// Readers access the committed root via cow_coordinator (no mutex).
      std::shared_ptr<btree_layer> rw_layer;

      /// WAL writer for the current RW btree.
      std::unique_ptr<wal_writer> wal;

      /// Monotonically increasing sequence number for WAL entries.
      uint64_t next_wal_seq = 0;

      /// Set true by merge thread after it finishes draining the RO btree.
      /// Checked by writer on commit to decide if a swap is needed.
      std::atomic<bool> merge_complete{true};

      /// Per-root transaction-level read/write exclusion.
      /// Writers take exclusive, readers take shared.
      /// Acquired in sorted root-index order to prevent deadlocks.
      std::shared_mutex tx_mutex;

      // ── Reader section (cache-line separated) ─────────────────────

      /// The frozen RO btree — published on arena swap, read by buffered readers.
      /// Brief mutex for shared_ptr copy (only held for the copy, not during read).
      alignas(128) std::mutex buffered_mutex;
      std::shared_ptr<btree_layer> buffered_ptr;

      /// Generation counter — incremented on each arena swap.
      /// Readers compare against their cached gen to detect new RO snapshots.
      std::atomic<uint32_t> generation{0};

      /// The PsiTri root address — updated atomically when merge completes.
      std::atomic<uint32_t> tri_root{0};

      /// Base root of the current RO btree — the PsiTri root at swap time.
      std::atomic<uint32_t> ro_base_root{0};

      // ── Adaptive write throttle ───────────────────────────────────

      /// Current per-commit sleep in nanoseconds.  Starts at 0 (no throttle).
      std::atomic<uint32_t> throttle_sleep_ns{0};

      /// Arena capacity recorded when the merge thread finishes.
      std::atomic<uint32_t> arena_at_merge_complete{0};

      /// Time of the last snapshot publication or arena swap.
      std::chrono::steady_clock::time_point last_snapshot_time{std::chrono::steady_clock::now()};

      // ── COWART coordination ───────────────────────────────────────

      /// Lock-free reader/writer coordination.
      /// Manages head root, prev_root, reader_count, writer_active, cow_seq.
      art::cow_coordinator cow;

      dwal_root() : rw_layer(std::make_shared<btree_layer>()) {}
   };

}  // namespace psitri::dwal
