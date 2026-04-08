#pragma once
#include <art/arena.hpp>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>

namespace art
{
   /// Lock-free reader/writer coordinator for COW ART snapshots.
   ///
   /// Manages a single 64-bit atomic that packs:
   ///   - root_offset (32 bits): head root in the arena
   ///   - writer_active (1 bit): a write transaction is in progress
   ///   - reader_waiting (1 bit): a blocked latest reader is waiting for notify
   ///   - reader_count (6 bits): readers actively traversing head (0–63)
   ///   - cow_seq (24 bits): COW generation counter (up to 16M)
   ///
   /// Readers use fetch_add/fetch_sub for reader_count (no CAS).
   /// Writers use CAS to avoid clobbering concurrent reader_count changes.
   ///
   /// COW is only triggered when readers and writer collide:
   ///   - begin_write sees reader_count > 0 → bumps cow_seq, publishes prev
   ///   - end_write sees reader_waiting or reader_count > 0 → bumps cow_seq
   ///   - If they take turns, cow_seq never bumps — zero overhead.
   ///
   /// Testable independently — no dependency on ART nodes or btree_layer.
   class cow_coordinator
   {
     public:
      // ── Bit layout ────────────────────────────────────────────────────

      static constexpr uint64_t root_shift         = 32;
      static constexpr uint64_t writer_active_bit  = uint64_t(1) << 31;
      static constexpr uint64_t reader_waiting_bit = uint64_t(1) << 30;
      static constexpr uint64_t reader_count_shift = 24;
      static constexpr uint64_t reader_count_one   = uint64_t(1) << reader_count_shift;
      static constexpr uint64_t reader_count_mask  = uint64_t(0x3F) << reader_count_shift;
      static constexpr uint64_t cow_seq_mask       = (uint64_t(1) << 24) - 1;

      // ── Accessors for packed state ────────────────────────────────────

      struct state
      {
         uint64_t packed;

         uint32_t root_offset() const noexcept { return uint32_t(packed >> root_shift); }
         bool     writer_active() const noexcept { return packed & writer_active_bit; }
         bool     reader_waiting() const noexcept { return packed & reader_waiting_bit; }
         uint8_t  reader_count() const noexcept
         {
            return uint8_t((packed & reader_count_mask) >> reader_count_shift);
         }
         uint32_t cow_seq() const noexcept { return uint32_t(packed & cow_seq_mask); }
      };

      static uint64_t pack(uint32_t root, bool wa, bool rw,
                           uint8_t rc, uint32_t seq) noexcept
      {
         return (uint64_t(root) << root_shift) |
                (wa ? writer_active_bit : 0) |
                (rw ? reader_waiting_bit : 0) |
                (uint64_t(rc & 0x3F) << reader_count_shift) |
                (seq & cow_seq_mask);
      }

      // ── Construction ──────────────────────────────────────────────────

      cow_coordinator()
          : _flags{pack(null_offset, false, false, 0, 0)},
            _prev{null_offset}
      {
      }

      // ── Writer interface ──────────────────────────────────────────────

      /// Called when a write transaction begins.
      ///
      /// Sets writer_active. If readers are traversing head (reader_count > 0),
      /// bumps cow_seq and copies head → prev so:
      ///   - Fresh readers can use prev (always safe)
      ///   - The writer will COW nodes shared with active readers
      ///
      /// Returns the cow_seq the writer should use for COW checks.
      uint32_t begin_write() noexcept
      {
         uint64_t expected = _flags.load(std::memory_order_acquire);
         for (;;)
         {
            auto s = state{expected};

            // If a reader is waiting (blocked on CV), yield to let it
            // wake up and read the committed root before we set writer_active
            // again. Without this, the writer can starve waiting readers
            // by immediately re-acquiring writer_active.
            if (s.reader_waiting())
            {
               std::this_thread::yield();
               expected = _flags.load(std::memory_order_acquire);
               continue;
            }

            uint32_t new_seq = s.cow_seq();

            if (s.reader_count() > 0)
            {
               // Readers traversing head — must COW to protect them.
               new_seq = (s.cow_seq() + 1) & cow_seq_mask;
               _prev.store(s.root_offset(), std::memory_order_release);
            }

            uint64_t desired = pack(
                s.root_offset(), /*wa=*/true, /*rw=*/false,
                s.reader_count(), new_seq);

            if (_flags.compare_exchange_weak(
                    expected, desired, std::memory_order_acq_rel))
               return new_seq;
         }
      }

      /// Called when a write transaction commits or aborts.
      ///
      /// Updates head root, clears writer_active. If readers are waiting
      /// or active, bumps cow_seq and publishes prev so the next transaction
      /// will COW shared nodes.
      ///
      /// Returns true if readers were notified (caller should call notify()).
      bool end_write(uint32_t new_root, uint32_t current_cow_seq) noexcept
      {
         uint64_t expected = _flags.load(std::memory_order_acquire);
         bool     has_readers = false;

         for (;;)
         {
            auto s = state{expected};
            has_readers = s.reader_waiting() || s.reader_count() > 0;
            uint32_t new_seq = current_cow_seq;

            if (has_readers)
            {
               new_seq = (current_cow_seq + 1) & cow_seq_mask;
               _prev.store(new_root, std::memory_order_release);
            }

            uint64_t desired = pack(
                new_root, /*wa=*/false, /*rw=*/false,
                s.reader_count(), new_seq);

            if (_flags.compare_exchange_weak(
                    expected, desired, std::memory_order_acq_rel))
               break;
         }

         if (has_readers)
            notify_readers();

         return has_readers;
      }

      // ── Latest reader interface ───────────────────────────────────────

      /// Begin a latest read. Increments reader_count.
      /// If writer is active, sets reader_waiting and blocks until writer finishes.
      /// Returns the head root offset (committed, safe to traverse).
      uint32_t begin_read_latest() noexcept
      {
         // Increment reader_count (atomic, no CAS)
         _flags.fetch_add(reader_count_one, std::memory_order_acquire);

         // Snapshot cow_seq before checking writer_active.
         // If cow_seq changes, a transaction committed (even if writer_active
         // is immediately set again by the next transaction).
         uint64_t flags = _flags.load(std::memory_order_acquire);
         auto     s     = state{flags};

         if (s.writer_active())
         {
            uint32_t seen_seq = s.cow_seq();

            // Set reader_waiting via CAS (preserve reader_count)
            uint64_t expected = flags;
            uint64_t desired  = expected | reader_waiting_bit;
            _flags.compare_exchange_strong(
                expected, desired, std::memory_order_acq_rel);

            // Wait until cow_seq changes (a transaction committed).
            // This avoids starvation: even if the writer immediately
            // starts a new tx, the cow_seq bump proves a commit happened
            // and our reader_count forced COW, so the root is safe.
            std::unique_lock lk(_notify_mutex);
            _notify_cv.wait(lk, [this, seen_seq]() {
               auto cur = state{_flags.load(std::memory_order_acquire)};
               return cur.cow_seq() != seen_seq || !cur.writer_active();
            });
         }

         // Head is committed and safe.
         return state{_flags.load(std::memory_order_acquire)}.root_offset();
      }

      /// End a latest read. Decrements reader_count.
      void end_read_latest() noexcept
      {
         _flags.fetch_sub(reader_count_one, std::memory_order_release);
      }

      // ── Fresh reader interface ────────────────────────────────────────

      /// Read prev root (always safe, zero coordination).
      /// Returns null_offset if no prev has been published yet.
      uint32_t read_prev() const noexcept
      {
         return _prev.load(std::memory_order_acquire);
      }

      // ── Freshness timer support ─────────────────────────────────────

      /// Force a snapshot publication (prev_root + cow_seq bump) during
      /// a write transaction, regardless of reader state. Used by the
      /// freshness timer to bound staleness for fresh-mode readers.
      /// Must be called while writer_active is set (during a transaction).
      void force_publish(uint32_t root, uint32_t current_cow_seq) noexcept
      {
         uint32_t new_seq = (current_cow_seq + 1) & cow_seq_mask;
         _prev.store(root, std::memory_order_release);

         // Update cow_seq in flags (writer_active stays set)
         uint64_t expected = _flags.load(std::memory_order_acquire);
         for (;;)
         {
            auto s = state{expected};
            uint64_t desired = pack(
                s.root_offset(), /*wa=*/true, s.reader_waiting(),
                s.reader_count(), new_seq);
            if (_flags.compare_exchange_weak(
                    expected, desired, std::memory_order_acq_rel))
               break;
         }
      }

      // ── State queries ─────────────────────────────────────────────────

      /// Load the current packed state (for diagnostics/testing).
      state load_state() const noexcept
      {
         return state{_flags.load(std::memory_order_acquire)};
      }

      /// Get the current head root without incrementing reader_count.
      /// Only safe when no writer can be active (e.g., single-threaded setup).
      uint32_t head_root() const noexcept
      {
         return state{_flags.load(std::memory_order_acquire)}.root_offset();
      }

      /// Reset state (e.g., after arena swap). Sets head to new_root,
      /// clears all flags and counters, resets prev.
      void reset(uint32_t new_root = null_offset) noexcept
      {
         _flags.store(pack(new_root, false, false, 0, 0), std::memory_order_release);
         _prev.store(null_offset, std::memory_order_release);
      }

      /// Set head root directly (e.g., after recovery).
      /// Only safe when no concurrent readers or writers.
      void set_root(uint32_t root) noexcept
      {
         auto s = load_state();
         _flags.store(pack(root, false, false, 0, s.cow_seq()), std::memory_order_release);
      }

     private:
      void notify_readers()
      {
         _notify_cv.notify_all();
      }

      std::atomic<uint64_t>   _flags;
      std::atomic<uint32_t>   _prev;
      std::mutex              _notify_mutex;
      std::condition_variable _notify_cv;
   };

}  // namespace art
