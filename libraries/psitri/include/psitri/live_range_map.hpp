#pragma once
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace psitri
{

   /**
    * Tracks ranges of dead MVCC versions for opportunistic cleanup.
    *
    * Dead versions are those whose snapshot reference count has reached zero —
    * no cursor will ever read them again.  Entries in value_nodes whose version
    * falls in a dead range can be stripped during copy-on-write.
    *
    * **Working copy** (writer-side, mutex-protected):
    *   - Sorted parallel arrays `_lows[]` / `_highs[]` describing disjoint,
    *     non-adjacent ranges `[low, high]` (inclusive).
    *   - A small `_pending[]` buffer that accumulates individual dead versions
    *     before being merged in batch.
    *
    * **Published snapshot** (reader-side, lock-free):
    *   - An atomically-swapped shared_ptr to a read-only `snapshot` containing
    *     a copy of the sorted ranges.  Readers load the pointer once per COW
    *     operation and probe it per value_node entry.
    *
    * Invariants:
    *   - Ranges are sorted ascending by low.
    *   - Ranges do not overlap and are not adjacent (they are coalesced).
    *   - `_lows[i] <= _highs[i]` for all i.
    *   - `_highs[i] + 1 < _lows[i+1]` for all i (gap between ranges).
    */
   class live_range_map
   {
     public:
      /// Read-only snapshot of dead version ranges.
      struct snapshot
      {
         std::vector<uint64_t> lows;
         std::vector<uint64_t> highs;

         /// Returns true if version V falls in any dead range.
         bool is_dead(uint64_t v) const noexcept
         {
            if (lows.empty())
               return false;
            // Binary search: find the last range whose low <= v.
            auto it = std::upper_bound(lows.begin(), lows.end(), v);
            if (it == lows.begin())
               return false;  // v < all lows
            --it;
            auto idx = it - lows.begin();
            return v <= highs[idx];
         }

         uint64_t num_ranges() const noexcept { return lows.size(); }
      };

      static constexpr uint32_t pending_capacity = 16;

      live_range_map() = default;

      /// Add a single dead version to the pending buffer.
      /// When the buffer is full, it is automatically merged.
      void add_dead_version(uint64_t version) noexcept
      {
         std::lock_guard lock(_mutex);
         _pending[_pending_count++] = version;
         if (_pending_count == pending_capacity)
            merge_pending_locked();
      }

      /// Add multiple dead versions in batch.
      void add_dead_versions(const uint64_t* versions, uint32_t count) noexcept
      {
         std::lock_guard lock(_mutex);
         for (uint32_t i = 0; i < count; ++i)
         {
            _pending[_pending_count++] = versions[i];
            if (_pending_count == pending_capacity)
               merge_pending_locked();
         }
      }

      /// Force-merge any pending versions into the sorted range arrays.
      void flush_pending() noexcept
      {
         std::lock_guard lock(_mutex);
         if (_pending_count > 0)
            merge_pending_locked();
      }

      /// Build and publish a new read-only snapshot.
      /// Readers that load the snapshot after this call will see all
      /// versions added before this call.
      void publish_snapshot() noexcept
      {
         std::lock_guard lock(_mutex);
         if (_pending_count > 0)
            merge_pending_locked();

         auto snap   = std::make_shared<snapshot>();
         snap->lows  = _lows;
         snap->highs = _highs;
         _published_owner = snap;
         _published_ptr.store(snap.get(), std::memory_order_release);
      }

      /// Load the latest published snapshot (lock-free for readers).
      /// Returns nullptr if no snapshot has been published yet.
      const snapshot* load_snapshot() const noexcept
      {
         return _published_ptr.load(std::memory_order_acquire);
      }

      /// Direct is_dead query on the working copy (mutex-protected).
      bool is_dead(uint64_t version) noexcept
      {
         std::lock_guard lock(_mutex);
         if (_pending_count > 0)
            merge_pending_locked();
         return is_dead_in_ranges(version);
      }

      /// Number of disjoint ranges in the working copy.
      uint64_t num_ranges() const noexcept
      {
         // No lock needed for diagnostic use
         return _lows.size();
      }

      /// Number of pending versions not yet merged.
      uint32_t pending_count() const noexcept { return _pending_count; }

     private:
      mutable std::mutex    _mutex;
      std::vector<uint64_t> _lows;
      std::vector<uint64_t> _highs;

      uint64_t _pending[pending_capacity];
      uint32_t _pending_count = 0;

      // Published snapshot: readers load atomically, writers swap under mutex.
      // Managed with shared_ptr — published via atomic raw pointer swap.
      std::shared_ptr<const snapshot> _published_owner;     // keeps the current snapshot alive
      std::atomic<const snapshot*>    _published_ptr{nullptr};  // lock-free read pointer

      /// Check if version falls in any range (caller holds mutex, no pending).
      bool is_dead_in_ranges(uint64_t v) const noexcept
      {
         if (_lows.empty())
            return false;
         auto it = std::upper_bound(_lows.begin(), _lows.end(), v);
         if (it == _lows.begin())
            return false;
         --it;
         auto idx = it - _lows.begin();
         return v <= _highs[idx];
      }

      /// Merge pending buffer into sorted range arrays.
      /// Caller holds _mutex.
      void merge_pending_locked() noexcept
      {
         if (_pending_count == 0)
            return;

         // Sort pending
         std::sort(_pending, _pending + _pending_count);

         // Insert each version as a single-element range [v, v]
         for (uint32_t i = 0; i < _pending_count; ++i)
            insert_range(_pending[i], _pending[i]);

         _pending_count = 0;
      }

      /// Insert a range [low, high] into the sorted range arrays,
      /// coalescing overlapping or adjacent ranges.
      void insert_range(uint64_t low, uint64_t high) noexcept
      {
         if (_lows.empty())
         {
            _lows.push_back(low);
            _highs.push_back(high);
            return;
         }

         // Find the first range whose low > high+1 (can't be merged with [low,high])
         // and the last range whose high+1 >= low (can be merged with [low,high])
         auto it_start = std::lower_bound(_lows.begin(), _lows.end(), low);
         // Back up to find ranges that might overlap/adjoin from the left
         size_t start_idx = (it_start == _lows.begin()) ? 0 : (it_start - _lows.begin() - 1);
         // Check if the range at start_idx can merge (its high+1 >= low)
         if (_highs[start_idx] + 1 < low)
            ++start_idx;

         // Find the last range that overlaps or adjoins from the right
         size_t end_idx = start_idx;
         while (end_idx < _lows.size() && _lows[end_idx] <= high + 1)
            ++end_idx;

         if (start_idx == end_idx)
         {
            // No overlap — insert a new range
            _lows.insert(_lows.begin() + start_idx, low);
            _highs.insert(_highs.begin() + start_idx, high);
         }
         else
         {
            // Merge: coalesce [start_idx, end_idx) into one range
            uint64_t merged_low  = std::min(low, _lows[start_idx]);
            uint64_t merged_high = std::max(high, _highs[end_idx - 1]);
            _lows[start_idx]  = merged_low;
            _highs[start_idx] = merged_high;
            // Erase the consumed ranges
            if (end_idx - start_idx > 1)
            {
               _lows.erase(_lows.begin() + start_idx + 1, _lows.begin() + end_idx);
               _highs.erase(_highs.begin() + start_idx + 1, _highs.begin() + end_idx);
            }
         }
      }
   };

}  // namespace psitri
