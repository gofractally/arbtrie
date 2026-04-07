#include <psitri/dwal/merge_pool.hpp>

#include <psitri/database.hpp>
#include <psitri/dwal/btree_layer.hpp>
#include <psitri/dwal/dwal_root.hpp>
#include <psitri/write_session_impl.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <ctime>

namespace psitri::dwal
{
   merge_pool::merge_pool(std::shared_ptr<psitri::database> db,
                          uint32_t                           num_threads,
                          epoch_registry&                    epochs,
                          std::filesystem::path              wal_dir,
                          uint64_t                           target_arena_bytes)
       : _db(std::move(db)), _epochs(epochs), _wal_dir(std::move(wal_dir)),
         _target_arena_bytes(target_arena_bytes)
   {
      // Sessions are created lazily on each worker thread (not here) because
      // allocator_sessions are thread-local — a session created on the main
      // thread cannot be used on a worker thread.
      _sessions.resize(num_threads);

      _threads.reserve(num_threads);
      for (uint32_t i = 0; i < num_threads; ++i)
         _threads.emplace_back(&merge_pool::worker_loop, this, i);
   }

   merge_pool::~merge_pool() { shutdown(); }

   void merge_pool::signal(uint32_t root_index, dwal_root& root)
   {
      {
         std::lock_guard lk(_queue_mu);
         _queue.push({root_index, &root});
      }
      _queue_cv.notify_one();
   }

   void merge_pool::request_stop()
   {
      _shutdown.store(true, std::memory_order_relaxed);
      _queue_cv.notify_all();
   }

   void merge_pool::shutdown()
   {
      _shutdown.store(true, std::memory_order_relaxed);
      _queue_cv.notify_all();
      for (auto& t : _threads)
      {
         if (t.joinable())
            t.join();
      }
      _threads.clear();
      _sessions.clear();
   }

   void merge_pool::worker_loop(uint32_t thread_index)
   {
      // Create the write session on this worker thread so the allocator_session
      // is bound to the correct thread-local storage.  The session must also be
      // destroyed on this thread (allocator_session has thread affinity).
      _sessions[thread_index] = _db->start_write_session();

      while (!_shutdown.load(std::memory_order_relaxed))
      {
         merge_request req;
         {
            std::unique_lock lk(_queue_mu);
            _queue_cv.wait(lk, [this]
                           { return !_queue.empty() || _shutdown.load(std::memory_order_relaxed); });
            if (_shutdown.load(std::memory_order_relaxed) && _queue.empty())
               break;
            req = _queue.front();
            _queue.pop();
         }

         drain_ro_btree(thread_index, req.root_index, *req.root);
         try_reclaim();
      }

      // Destroy session on this thread to respect allocator_session thread affinity.
      _sessions[thread_index].reset();
   }

   void merge_pool::drain_ro_btree(uint32_t thread_index, uint32_t root_index, dwal_root& root)
   {
      // Grab a shared_ptr to the RO btree — keeps it alive during merge.
      std::shared_ptr<btree_layer> ro;
      {
         std::lock_guard lk(root.buffered_mutex);
         ro = root.buffered_ptr;
      }
      if (!ro)
         return;

      auto wall_start = std::chrono::steady_clock::now();
      struct timespec cpu_start_ts;
      clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_start_ts);

      auto& ws  = *_sessions[thread_index];
      auto  as  = ws.allocator_session();
      auto  seg_count_before = as->seg_alloc_count();
      auto  seg_ns_before    = as->seg_alloc_ns();
      auto  tx = ws.start_transaction(root_index);

      // Drain all entries from the RO btree into PsiTri.
      // Keys arrive in sorted order — use upsert_sorted to enable sibling
      // prefetch, warming pages the next key will need.
      //
      // Per-entry timing: bucket each upsert into latency ranges to identify
      // whether stalls come from a few very slow ops (segment alloc) or many
      // moderately slow ones (page fault reads).
      uint64_t entry_count = 0;
      uint64_t bucket_lt1us  = 0;  // < 1 us
      uint64_t bucket_lt10us = 0;  // 1-10 us
      uint64_t bucket_lt100us = 0; // 10-100 us
      uint64_t bucket_lt1ms  = 0;  // 100us-1ms
      uint64_t bucket_lt10ms = 0;  // 1-10 ms
      uint64_t bucket_ge10ms = 0;  // >= 10 ms
      double   max_entry_us  = 0;
      double   sum_top_us    = 0;  // sum of entries >= 1ms
      uint64_t count_top     = 0;

      bool aborted = false;
      for (auto it = ro->map.begin(); it != ro->map.end(); ++it)
      {
         if (_shutdown.load(std::memory_order_relaxed)) [[unlikely]]
         {
            tx.abort();
            aborted = true;
            fprintf(stderr, "[MERGE] shutdown requested — aborting after %llu entries\n",
                    (unsigned long long)entry_count);
            break;
         }

         auto entry_start = std::chrono::steady_clock::now();

         auto  key = it.key();
         auto& val = it.value();
         if (val.is_tombstone())
         {
            tx.remove(key_view(key.data(), key.size()));
         }
         else if (val.is_subtree())
         {
            auto subtree = ws.make_ptr(val.subtree_root, /*retain=*/true);
            if (subtree)
               tx.upsert_sorted(key, std::move(subtree));
         }
         else
         {
            tx.upsert_sorted(key, val.data);
         }
         ++entry_count;
         double us = std::chrono::duration<double, std::micro>(
                         std::chrono::steady_clock::now() - entry_start).count();
         if (us < 1)        ++bucket_lt1us;
         else if (us < 10)  ++bucket_lt10us;
         else if (us < 100) ++bucket_lt100us;
         else if (us < 1000) ++bucket_lt1ms;
         else if (us < 10000) ++bucket_lt10ms;
         else                ++bucket_ge10ms;
         if (us > max_entry_us) max_entry_us = us;
         if (us >= 1000) { sum_top_us += us; ++count_top; }
      }
      if (aborted)
      {
         root.merge_complete.store(true, std::memory_order_release);
         root.merge_complete.notify_all();
         return;
      }

      // Apply range tombstones.
      for (const auto& range : ro->tombstones.ranges())
         tx.remove_range(range.low, range.high);

      auto wall_pre_commit = std::chrono::steady_clock::now();
      struct timespec cpu_pre_commit_ts;
      clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_pre_commit_ts);

      tx.commit();

      auto wall_end = std::chrono::steady_clock::now();
      struct timespec cpu_end_ts;
      clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_end_ts);

      auto to_ms = [](struct timespec& a, struct timespec& b) -> double {
         return (b.tv_sec - a.tv_sec) * 1000.0 + (b.tv_nsec - a.tv_nsec) / 1e6;
      };

      double wall_drain_ms  = std::chrono::duration<double, std::milli>(wall_pre_commit - wall_start).count();
      double wall_commit_ms = std::chrono::duration<double, std::milli>(wall_end - wall_pre_commit).count();
      double wall_total_ms  = wall_drain_ms + wall_commit_ms;
      double cpu_drain_ms   = to_ms(cpu_start_ts, cpu_pre_commit_ts);
      double cpu_commit_ms  = to_ms(cpu_pre_commit_ts, cpu_end_ts);
      double cpu_total_ms   = cpu_drain_ms + cpu_commit_ms;
      double syscall_pct    = wall_total_ms > 0 ? 100.0 * (1.0 - cpu_total_ms / wall_total_ms) : 0;

      double entries_per_sec = wall_total_ms > 0 ? entry_count / (wall_total_ms / 1000.0) : 0;
      double us_per_entry   = entry_count > 0 ? (wall_total_ms * 1000.0) / entry_count : 0;
      double top_total_ms   = sum_top_us / 1000.0;

      uint64_t seg_count = as->seg_alloc_count() - seg_count_before;
      double   seg_ms    = (as->seg_alloc_ns() - seg_ns_before) / 1e6;

      fprintf(stderr,
              "[MERGE] root=%u entries=%llu  %.0f ms wall / %.0f ms cpu  "
              "syscall=%.0f%%  %.0f entries/sec  %.2f us/entry  max=%.1f ms\n"
              "        latency: <1us=%llu  <10us=%llu  <100us=%llu  <1ms=%llu  "
              "<10ms=%llu  >=10ms=%llu  |  stalls(>=1ms): %llu entries, %.0f ms total (%.0f%% of wall)\n"
              "        segments: %llu new segs, %.0f ms total (%.0f%% of wall)  "
              "%.1f ms/seg\n",
              root_index, (unsigned long long)entry_count,
              wall_total_ms, cpu_total_ms, syscall_pct,
              entries_per_sec, us_per_entry, max_entry_us / 1000.0,
              (unsigned long long)bucket_lt1us, (unsigned long long)bucket_lt10us,
              (unsigned long long)bucket_lt100us, (unsigned long long)bucket_lt1ms,
              (unsigned long long)bucket_lt10ms, (unsigned long long)bucket_ge10ms,
              (unsigned long long)count_top, top_total_ms,
              wall_total_ms > 0 ? 100.0 * top_total_ms / wall_total_ms : 0,
              (unsigned long long)seg_count, seg_ms,
              wall_total_ms > 0 ? 100.0 * seg_ms / wall_total_ms : 0,
              seg_count > 0 ? seg_ms / seg_count : 0);

      // Update the DWAL root's tri_root to reflect the new PsiTri root.
      auto new_root = ws.get_root(root_index);
      if (new_root)
         root.tri_root.store(static_cast<uint32_t>(new_root.address()), std::memory_order_release);

      // Null the buffered shared_ptr — last reader holding a copy will free it.
      {
         std::lock_guard lk(root.buffered_mutex);
         root.buffered_ptr.reset();
      }

      // Drop our local reference.
      uint32_t gen = ro->generation;
      ro.reset();

      // Queue for epoch-based reclamation (in case readers still hold copies).
      // Note: with shared_ptr this is handled automatically, but we keep the
      // epoch tracking for generation ordering.

      // Delete the RO WAL file — its data is now in PsiTri.
      if (!_wal_dir.empty())
      {
         auto ro_wal = _wal_dir / ("root-" + std::to_string(root_index)) / "wal-ro.dwal";
         std::error_code ec;
         std::filesystem::remove(ro_wal, ec);
      }

      // ── Adaptive throttle adjustment ──────────────────────────────
      // Record the current RW arena capacity so the writer can see how
      // far the arena grew while this merge was running.  Then adjust
      // the per-commit sleep to target the merge finishing before the
      // arena reaches max_rw_arena_bytes.
      {
         uint32_t arena_cap = root.rw_layer ? root.rw_layer->map.arena_capacity() : 0;
         root.arena_at_merge_complete.store(arena_cap, std::memory_order_relaxed);

         uint32_t cur_sleep = root.throttle_sleep_ns.load(std::memory_order_relaxed);
         uint64_t target    = _target_arena_bytes;

         if (target > 0 && arena_cap > 0)
         {
            if (arena_cap > target)
            {
               // Merge was too slow — increase sleep.
               // Scale proportionally: 2x over target → 2x sleep increase.
               uint32_t new_sleep = cur_sleep < 100 ? 100 : cur_sleep;
               double   ratio     = double(arena_cap) / double(target);
               new_sleep = uint32_t(std::min(double(new_sleep) * ratio, 100000.0));
               root.throttle_sleep_ns.store(new_sleep, std::memory_order_relaxed);
            }
            else
            {
               // Merge kept up — reduce sleep by 10%.
               uint32_t new_sleep = uint32_t(cur_sleep * 0.9);
               root.throttle_sleep_ns.store(new_sleep, std::memory_order_relaxed);
            }
         }
      }

      // Signal the writer that it can swap again.
      root.merge_complete.store(true, std::memory_order_release);
      root.merge_complete.notify_all();

      // Under COWART, fresh readers use prev_root (zero coordination).
      // No need for merge-thread-initiated swaps.
   }

   void merge_pool::try_reclaim()
   {
      // With shared_ptr-based RO pools, reclamation is automatic.
      // The last shared_ptr holder (reader or merge thread) frees the btree_layer.
      // We keep this method for future epoch-based optimizations.
   }

}  // namespace psitri::dwal
