#pragma once
#include <psitri/dwal/btree_layer.hpp>
#include <psitri/dwal/undo_log.hpp>
#include <psitri/dwal/wal_writer.hpp>
#include <sal/numbers.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <shared_mutex>

namespace psitri::dwal
{
   /**
    * @brief Read mode: freshness vs cost tradeoff for DWAL readers.
    *
    * All modes see only committed data.  The difference is how recent
    * that data is and what cost the reader imposes on the system.
    *
    *     Mode       Layers          Reader waits for   Writer impact
    *     ---------  --------------  -----------------  ------------------
    *     trie       Tri only        nothing            none
    *     buffered   RO + Tri        nothing            none
    *     fresh      RO + Tri        merge + swap       none (self-imposed)
    *     latest     RW + RO + Tri   current tx finish  blocks next tx start
    *
    * **Choosing a mode:**
    *
    * - `trie`: high-throughput reads tolerating seconds of staleness.
    * - `buffered`: default for external readers.  Staleness bounded by
    *   `max_flush_delay` or the writer's natural swap frequency.
    * - `fresh`: reader needs all committed data but can wait.  Sets an
    *   atomic flag; the writer (on next commit) or the merge thread
    *   (if writer is idle) swaps RW→RO and wakes waiting readers.
    *   Reader blocks only itself — zero writer impact.
    * - `latest`: reader needs the absolute freshest view.  Acquires a
    *   shared lock on the RW layer, blocking until the writer finishes
    *   its current transaction.  While held, the writer cannot start a
    *   new transaction.  Use sparingly — this is the only mode that
    *   taxes the writer.
    */
   enum class read_mode
   {
      trie,        ///< Tri only — zero DWAL overhead, most stale
      buffered,    ///< RO + Tri — no writer interaction, bounded staleness
      fresh,       ///< RO + Tri after forced swap — reader waits, writer unaffected
      latest,      ///< RW + RO + Tri — shared lock, blocks writer's next tx
   };

   /// Transaction mode: buffered writes vs direct COW.
   enum class transaction_mode
   {
      buffered,  // Normal: writes go to RW btree + WAL
      direct,    // Large tx: flush RW, write directly to PsiTri COW
   };

   // ── COWART shared state ────────────────────────────────────────────────
   //
   // Packed 64-bit atomic for lock-free reader/writer coordination.
   // Bits 63-32: root_offset (current RW root in arena)
   // Bit  31:    reader_waiting (a reader wants a snapshot)
   // Bit  30:    writer_active (a write transaction is in progress)
   // Bits 29-0:  cow_seq (COW generation counter, ~1 billion generations)

   struct cowart_flags
   {
      uint64_t packed;

      static constexpr uint64_t reader_waiting_bit = uint64_t(1) << 31;
      static constexpr uint64_t writer_active_bit  = uint64_t(1) << 30;
      static constexpr uint64_t cow_seq_mask       = (uint64_t(1) << 30) - 1;
      static constexpr uint64_t root_shift         = 32;

      uint32_t root_offset() const noexcept { return static_cast<uint32_t>(packed >> root_shift); }
      bool     reader_waiting() const noexcept { return packed & reader_waiting_bit; }
      bool     writer_active() const noexcept { return packed & writer_active_bit; }
      uint32_t cow_seq() const noexcept { return static_cast<uint32_t>(packed & cow_seq_mask); }

      static uint64_t make(uint32_t root, bool rw, bool wa, uint32_t seq) noexcept
      {
         return (uint64_t(root) << root_shift) |
                (rw ? reader_waiting_bit : 0) |
                (wa ? writer_active_bit : 0) |
                (seq & cow_seq_mask);
      }
   };

   /// COWART coordination state — lives alongside legacy dwal_root fields.
   /// Used when the COW-based snapshot protocol is active.
   struct cowart_state
   {
      /// Packed root offset + flags for lock-free reader/writer handoff.
      std::atomic<uint64_t> root_and_flags{0};

      /// Last published snapshot root (for buffered/fresh readers).
      std::atomic<uint32_t> last_root{art::null_offset};

      /// Readers waiting for a fresh snapshot.
      std::mutex              notify_mutex;
      std::condition_variable writer_done_cv;

      /// Reader epoch tracking for arena reclamation.
      static constexpr uint32_t max_reader_slots = 64;
      struct reader_slot
      {
         std::atomic<uint32_t> held_cow_seq{0};  // 0 = idle
      };
      reader_slot reader_slots[max_reader_slots];
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

      // ── Adaptive write throttle ───────────────────────────────────
      //
      // Self-tuning sleep that keeps the RW arena from growing past the
      // target by the time merge completes.  Adjusted once per merge cycle:
      //   - merge finished early (arena < target) → reduce sleep
      //   - merge finished late  (arena > target) → increase sleep
      //
      // Writer checks throttle_sleep_ns on every commit and sleeps if
      // the arena is above the low-water mark (25% of target).

      /// Current per-commit sleep in nanoseconds.  Starts at 0 (no throttle).
      /// Adjusted by the merge thread when it finishes draining.
      std::atomic<uint32_t> throttle_sleep_ns{0};

      /// Arena capacity recorded when the merge thread finishes.
      /// Used by the adjustment algorithm to decide if sleep should increase.
      std::atomic<uint32_t> arena_at_merge_complete{0};

      /// Time of the last RW→RO swap.  Used for time-based flush.
      std::chrono::steady_clock::time_point last_swap_time{std::chrono::steady_clock::now()};

      // ── Swap coordination for fresh-mode readers ──────────────────

      /// Set by readers that want the latest committed data (fresh mode).
      /// Checked by the writer on commit and by the merge thread after
      /// drain.  Cleared after swap + notify.
      std::atomic<bool> readers_want_swap{false};

      /// Condition variable notified after RW→RO swap completes.
      /// Fresh-mode readers wait on this.
      std::condition_variable_any swap_cv;

      /// Mutex for swap_cv wait.  Only fresh-mode readers lock this
      /// (shared) while waiting.  The swapper notifies without holding it.
      mutable std::shared_mutex swap_mutex;

      // ── COWART state (coexists with legacy fields during migration) ──
      cowart_state cow;

      dwal_root() : rw_layer(std::make_shared<btree_layer>()) {}
   };

}  // namespace psitri::dwal
