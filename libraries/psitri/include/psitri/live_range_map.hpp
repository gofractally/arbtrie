#pragma once
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstring>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <sal/mapping.hpp>
#include <stdexcept>
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
         uint64_t              retained_floor = 0;

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

	         /// Conservative oldest retained version implied by contiguous dead
	         /// ranges starting at version 1. Version 0 is not a retained root
	         /// version and must not prevent the floor from advancing.
	         uint64_t oldest_retained_floor() const noexcept { return retained_floor; }
      };

	      static constexpr uint32_t pending_capacity = 16;

	      live_range_map() = default;

	      explicit live_range_map(const std::filesystem::path& path)
	      {
	         open(path);
	      }

	      void open(const std::filesystem::path& path)
	      {
	         std::lock_guard lock(_mutex);
	         std::filesystem::create_directories(path.parent_path());
	         _mapping = std::make_unique<sal::mapping>(path, sal::access_mode::read_write);
	         _created_storage = _mapping->size() == 0;
	         if (_created_storage)
	         {
	            _mapping->resize(storage_size(initial_mapped_capacity));
	            refresh_mapping();
	            new (_mapped_header) mapped_header();
	            _mapped_header->magic    = mapped_magic;
	            _mapped_header->format   = mapped_format;
	            _mapped_header->capacity = initial_mapped_capacity;
	         }
	         else
	         {
	            refresh_mapping();
	            validate_mapping();
	         }
	         if (pending_count_locked() > 0)
	            merge_pending_locked();
	         publish_snapshot_locked();
	      }

	      bool created_storage() const noexcept { return _created_storage; }

      /// Add a single dead version to the pending buffer.
      /// When the buffer is full, it is automatically merged.
	      void add_dead_version(uint64_t version) noexcept
	      {
	         std::lock_guard lock(_mutex);
	         auto* pending = pending_data();
	         auto  count   = pending_count_locked();
	         pending[count++] = version;
	         set_pending_count(count);
	         if (count == pending_capacity)
	            merge_pending_locked();
	      }

	      /// Add multiple dead versions in batch.
	      void add_dead_versions(const uint64_t* versions, uint32_t count) noexcept
	      {
	         std::lock_guard lock(_mutex);
	         for (uint32_t i = 0; i < count; ++i)
	         {
	            auto* pending = pending_data();
	            auto  pending_count = pending_count_locked();
	            pending[pending_count++] = versions[i];
	            set_pending_count(pending_count);
	            if (pending_count == pending_capacity)
	               merge_pending_locked();
	         }
	      }

	      void add_dead_range(uint64_t low, uint64_t high) noexcept
	      {
	         if (high < low)
	            return;
	         std::lock_guard lock(_mutex);
	         if (pending_count_locked() > 0)
	            merge_pending_locked();
	         insert_range(low, high);
	      }

      /// Force-merge any pending versions into the sorted range arrays.
	      void flush_pending() noexcept
	      {
	         std::lock_guard lock(_mutex);
	         if (pending_count_locked() > 0)
	            merge_pending_locked();
	      }

      /// Build and publish a new read-only snapshot.
      /// Readers that load the snapshot after this call will see all
      /// versions added before this call.
	      void publish_snapshot() noexcept
	      {
	         std::lock_guard lock(_mutex);
	         if (pending_count_locked() > 0)
	            merge_pending_locked();
	         publish_snapshot_locked();
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
	         if (pending_count_locked() > 0)
	            merge_pending_locked();
	         return is_dead_in_ranges(version);
	      }

      /// Number of disjoint ranges in the working copy.
	      uint64_t num_ranges() const noexcept
	      {
	         // No lock needed for diagnostic use.
	         return range_count_locked();
	      }

      /// Count dead versions in the inclusive range [low, high].
      uint64_t count_dead_versions(uint64_t low, uint64_t high) noexcept
      {
         if (high < low)
            return 0;

	         std::lock_guard lock(_mutex);
	         if (pending_count_locked() > 0)
	            merge_pending_locked();

	         uint64_t count = 0;
	         size_t   idx   = lower_bound_high(low);
	         size_t   n     = range_count_locked();

	         while (idx < n && range_at(idx).low <= high)
	         {
	            auto     r          = range_at(idx);
	            uint64_t range_low  = std::max(r.low, low);
	            uint64_t range_high = std::min(r.high, high);
	            if (range_low <= range_high)
	               count += range_high - range_low + 1;
	            ++idx;
         }
         return count;
      }

	      /// Conservative oldest retained version implied by contiguous dead
	      /// ranges starting at version 1 in the working copy.
	      uint64_t oldest_retained_floor() noexcept
	      {
	         std::lock_guard lock(_mutex);
	         if (pending_count_locked() > 0)
	            merge_pending_locked();
	         return retained_floor_locked();
	      }

	      /// Number of pending versions not yet merged.
	      uint32_t pending_count() const noexcept { return pending_count_locked(); }

	      void sync(sal::sync_type type = sal::sync_type::full) noexcept
	      {
	         if (_mapping)
	            _mapping->sync(type);
	      }

	     private:
	      struct range_record
	      {
	         uint64_t low  = 0;
	         uint64_t high = 0;
	      };

	      struct mapped_header
	      {
	         uint64_t magic         = 0;
	         uint32_t format        = 0;
	         uint32_t capacity      = 0;
	         uint32_t count         = 0;
	         uint32_t pending_count = 0;
	         uint64_t pending[pending_capacity] = {};
	      };

	      static constexpr uint64_t mapped_magic = 0x7073697472696476ULL;  // "psitridv"
	      static constexpr uint32_t mapped_format = 1;
	      static constexpr uint32_t initial_mapped_capacity = 1024;

	      mutable std::mutex _mutex;
	      std::vector<range_record> _ranges;
	      uint64_t _pending[pending_capacity] = {};
	      uint32_t _pending_count = 0;
	      std::unique_ptr<sal::mapping> _mapping;
	      mapped_header* _mapped_header = nullptr;
	      range_record*  _mapped_ranges = nullptr;
	      bool           _created_storage = false;

	      // Published snapshot: readers load atomically, writers swap under mutex.
	      // Managed with shared_ptr — published via atomic raw pointer swap.
	      std::shared_ptr<const snapshot> _published_owner;     // keeps the current snapshot alive
	      std::atomic<const snapshot*>    _published_ptr{nullptr};  // lock-free read pointer

	      static constexpr std::size_t storage_size(uint32_t capacity) noexcept
	      {
	         return sizeof(mapped_header) + std::size_t(capacity) * sizeof(range_record);
	      }

	      bool mapped() const noexcept { return _mapped_header != nullptr; }

	      void refresh_mapping() noexcept
	      {
	         _mapped_header = static_cast<mapped_header*>(_mapping->data());
	         _mapped_ranges = reinterpret_cast<range_record*>(
	             reinterpret_cast<char*>(_mapped_header) + sizeof(mapped_header));
	      }

	      void validate_mapping()
	      {
	         if (_mapping->size() < sizeof(mapped_header))
	            throw std::runtime_error("dead-version index is too small");
	         if (_mapped_header->magic != mapped_magic)
	            throw std::runtime_error("dead-version index has wrong magic");
	         if (_mapped_header->format != mapped_format)
	            throw std::runtime_error("dead-version index has unsupported format");
	         if (_mapped_header->capacity == 0 ||
	             _mapping->size() < storage_size(_mapped_header->capacity))
	            throw std::runtime_error("dead-version index has invalid capacity");
	         if (_mapped_header->count > _mapped_header->capacity)
	            throw std::runtime_error("dead-version index has invalid range count");
	         if (_mapped_header->pending_count > pending_capacity)
	            throw std::runtime_error("dead-version index has invalid pending count");
	      }

	      uint32_t pending_count_locked() const noexcept
	      {
	         return mapped() ? _mapped_header->pending_count : _pending_count;
	      }

	      void set_pending_count(uint32_t count) noexcept
	      {
	         if (mapped())
	            _mapped_header->pending_count = count;
	         else
	            _pending_count = count;
	      }

	      uint64_t* pending_data() noexcept
	      {
	         return mapped() ? _mapped_header->pending : _pending;
	      }

	      const uint64_t* pending_data() const noexcept
	      {
	         return mapped() ? _mapped_header->pending : _pending;
	      }

	      uint32_t range_count_locked() const noexcept
	      {
	         return mapped() ? _mapped_header->count : uint32_t(_ranges.size());
	      }

	      void set_range_count(uint32_t count) noexcept
	      {
	         if (mapped())
	            _mapped_header->count = count;
	         else
	            _ranges.resize(count);
	      }

	      const range_record* range_data() const noexcept
	      {
	         return mapped() ? _mapped_ranges : _ranges.data();
	      }

	      range_record* range_data() noexcept
	      {
	         return mapped() ? _mapped_ranges : _ranges.data();
	      }

	      range_record range_at(size_t idx) const noexcept
	      {
	         return range_data()[idx];
	      }

	      void set_range(size_t idx, range_record r) noexcept
	      {
	         range_data()[idx] = r;
	      }

	      void ensure_range_capacity(uint32_t needed)
	      {
	         if (!mapped())
	         {
	            if (needed > _ranges.capacity())
	               _ranges.reserve(needed);
	            return;
	         }
	         if (needed <= _mapped_header->capacity)
	            return;

	         uint32_t new_capacity = std::max<uint32_t>(needed, _mapped_header->capacity * 2);
	         _mapping->resize(storage_size(new_capacity));
	         refresh_mapping();
	         _mapped_header->capacity = new_capacity;
	      }

	      void insert_slot(size_t idx, range_record value)
	      {
	         auto count = range_count_locked();
	         if (!mapped())
	         {
	            _ranges.insert(_ranges.begin() + idx, value);
	            return;
	         }
	         ensure_range_capacity(count + 1);
	         auto* data = range_data();
	         if (idx < count)
	            std::memmove(data + idx + 1, data + idx, (count - idx) * sizeof(range_record));
	         data[idx] = value;
	         if (mapped())
	            _mapped_header->count = count + 1;
	         else
	            _ranges.resize(count + 1);
	      }

	      void erase_slots(size_t first, size_t last)
	      {
	         if (first >= last)
	            return;
	         if (!mapped())
	         {
	            _ranges.erase(_ranges.begin() + first, _ranges.begin() + last);
	            return;
	         }
	         auto count = range_count_locked();
	         auto* data = range_data();
	         if (last < count)
	            std::memmove(data + first, data + last, (count - last) * sizeof(range_record));
	         set_range_count(uint32_t(count - (last - first)));
	      }

	      static bool range_strictly_before(range_record r, uint64_t low) noexcept
	      {
	         return r.high < low && low - r.high > 1;
	      }

	      static bool range_strictly_after(uint64_t high, range_record r) noexcept
	      {
	         return high < r.low && r.low - high > 1;
	      }

	      size_t lower_bound_high(uint64_t low) const noexcept
	      {
	         size_t left = 0;
	         size_t right = range_count_locked();
	         while (left < right)
	         {
	            size_t mid = left + (right - left) / 2;
	            if (range_at(mid).high < low)
	               left = mid + 1;
	            else
	               right = mid;
	         }
	         return left;
	      }

	      void publish_snapshot_locked() noexcept
	      {
	         auto snap = std::make_shared<snapshot>();
	         auto n    = range_count_locked();
	         snap->lows.reserve(n);
	         snap->highs.reserve(n);
	         for (uint32_t i = 0; i < n; ++i)
	         {
	            auto r = range_at(i);
	            snap->lows.push_back(r.low);
	            snap->highs.push_back(r.high);
	         }
	         snap->retained_floor = retained_floor_locked();
	         _published_owner = snap;
	         _published_ptr.store(snap.get(), std::memory_order_release);
	      }

	      /// Check if version falls in any range (caller holds mutex, no pending).
	      bool is_dead_in_ranges(uint64_t v) const noexcept
	      {
	         auto n = range_count_locked();
	         if (n == 0)
	            return false;

	         size_t left = 0;
	         size_t right = n;
	         while (left < right)
	         {
	            size_t mid = left + (right - left) / 2;
	            if (range_at(mid).low <= v)
	               left = mid + 1;
	            else
	               right = mid;
	         }
	         if (left == 0)
	            return false;
	         auto r = range_at(left - 1);
	         return v <= r.high;
	      }

	      uint64_t retained_floor_locked() const noexcept
	      {
	         if (range_count_locked() == 0)
	            return 0;
	         auto first = range_at(0);
	         if (first.low > 1)
	            return 0;
	         if (first.high == UINT64_MAX)
	            return UINT64_MAX;
	         return first.high + 1;
	      }

      /// Merge pending buffer into sorted range arrays.
      /// Caller holds _mutex.
	      void merge_pending_locked() noexcept
	      {
	         auto count = pending_count_locked();
	         if (count == 0)
	            return;

	         // Sort pending
	         auto* pending = pending_data();
	         std::sort(pending, pending + count);

	         // Insert each version as a single-element range [v, v]
	         for (uint32_t i = 0; i < count; ++i)
	            insert_range(pending[i], pending[i]);

	         set_pending_count(0);
	      }

      /// Insert a range [low, high] into the sorted range arrays,
      /// coalescing overlapping or adjacent ranges.
	      void insert_range(uint64_t low, uint64_t high) noexcept
	      {
	         auto count = range_count_locked();
	         if (count == 0)
	         {
	            insert_slot(0, {low, high});
	            return;
	         }

	         size_t idx = 0;
	         while (idx < count && range_strictly_before(range_at(idx), low))
	            ++idx;

	         if (idx == count || range_strictly_after(high, range_at(idx)))
	         {
	            insert_slot(idx, {low, high});
	            return;
	         }

	         uint64_t merged_low  = std::min(low, range_at(idx).low);
	         uint64_t merged_high = std::max(high, range_at(idx).high);
	         size_t   end_idx     = idx + 1;
	         while (end_idx < count && !range_strictly_after(merged_high, range_at(end_idx)))
	         {
	            auto r = range_at(end_idx);
	            merged_low  = std::min(merged_low, r.low);
	            merged_high = std::max(merged_high, r.high);
	            ++end_idx;
	         }
	         set_range(idx, {merged_low, merged_high});
	         erase_slots(idx + 1, end_idx);
	      }
	   };

}  // namespace psitri
