#pragma once
#include <algorithm>
#include <cstring>
#include <memory_resource>
#include <string_view>
#include <vector>

namespace psitri::dwal
{
   /// Non-overlapping, sorted list of deleted key ranges [low, high).
   /// Ranges are merged on insert. Used by each btree layer to track
   /// range deletions that shadow keys in lower layers.
   ///
   /// low/high are string_views into an external bump allocator pool.
   /// Since the pool only frees as a unit, "dead" range strings are simply
   /// abandoned — no per-entry deallocation needed.
   class range_tombstone_list
   {
     public:
      struct range
      {
         std::string_view low;   // inclusive, pool-backed
         std::string_view high;  // exclusive, pool-backed

         bool contains(std::string_view key) const noexcept
         {
            return key >= low && key < high;
         }
      };

      /// Provide a string copy function (e.g. btree_layer::store_string).
      /// Must be set before calling add() or split_at().
      using copy_fn_t = std::string_view (*)(void* ctx, std::string_view src);
      void set_copy_fn(copy_fn_t fn, void* ctx) noexcept
      {
         _copy_fn  = fn;
         _copy_ctx = ctx;
      }

      /// O(log R) — binary search for range containing key.
      bool is_deleted(std::string_view key) const noexcept
      {
         if (_ranges.empty())
            return false;
         auto it = std::upper_bound(
             _ranges.begin(), _ranges.end(), key,
             [](std::string_view k, const range& r) { return k < r.low; });
         if (it == _ranges.begin())
            return false;
         --it;
         return it->contains(key);
      }

      /// Add a range deletion [low, high). Merges with adjacent/overlapping ranges.
      /// low and high are copied into the pool via the copy function.
      void add(std::string_view low, std::string_view high)
      {
         if (low >= high)
            return;

         auto pool_low  = pool_copy(low);
         auto pool_high = pool_copy(high);

         // Find first range that could overlap (its high > low)
         auto first = std::lower_bound(
             _ranges.begin(), _ranges.end(), pool_low,
             [](const range& r, std::string_view l) { return r.high < l; });

         // Find last range that overlaps (its low < high)
         auto last = std::upper_bound(
             first, _ranges.end(), pool_high,
             [](std::string_view h, const range& r) { return h < r.low; });

         if (first == last)
         {
            // No overlap — insert new range
            _ranges.insert(first, range{pool_low, pool_high});
         }
         else
         {
            // Merge: extend first to cover everything, erase the rest
            if (pool_low < first->low)
               first->low = pool_low;
            auto& last_range = *(last - 1);
            if (pool_high > last_range.high)
               first->high = pool_high;
            else
               first->high = last_range.high;
            if (last - first > 1)
               _ranges.erase(first + 1, last);
         }
      }

      /// Remove a range tombstone [low, high). Used by undo replay on abort.
      void remove(std::string_view low, std::string_view high)
      {
         _ranges.erase(
             std::remove_if(_ranges.begin(), _ranges.end(),
                            [&](const range& r) { return r.low == low && r.high == high; }),
             _ranges.end());
      }

      /// Split a range when a key is inserted within it.
      /// [A, Z) + insert("M") → [A, M) + [M\0, Z)
      void split_at(std::string_view key)
      {
         for (auto it = _ranges.begin(); it != _ranges.end(); ++it)
         {
            if (it->contains(key))
            {
               // Successor of key: key + '\0'
               // We need to allocate key.size()+1 bytes in the pool
               auto successor = copy_with_null_suffix(key);

               if (key == it->low)
               {
                  it->low = successor;
                  if (it->low >= it->high)
                     _ranges.erase(it);
               }
               else if (successor >= it->high)
               {
                  it->high = pool_copy(key);
                  if (it->low >= it->high)
                     _ranges.erase(it);
               }
               else
               {
                  auto old_high = it->high;
                  it->high      = pool_copy(key);
                  _ranges.insert(it + 1, range{successor, old_high});
               }
               return;
            }
         }
      }

      const std::vector<range>& ranges() const noexcept { return _ranges; }
      bool                      empty() const noexcept { return _ranges.empty(); }
      size_t                    size() const noexcept { return _ranges.size(); }
      void                      clear() noexcept { _ranges.clear(); }

     private:
      std::string_view pool_copy(std::string_view src)
      {
         if (_copy_fn)
            return _copy_fn(_copy_ctx, src);
         // Fallback: use internal pool (for standalone usage / tests).
         if (src.empty())
            return {};
         auto* buf = static_cast<char*>(_own_pool.allocate(src.size(), 1));
         std::memcpy(buf, src.data(), src.size());
         return {buf, src.size()};
      }

      /// Copy key into pool with an appended '\0' byte (successor key).
      std::string_view copy_with_null_suffix(std::string_view key)
      {
         // Build key + '\0' on stack, then copy into pool.
         char buf[4096];
         if (key.size() + 1 <= sizeof(buf))
         {
            std::memcpy(buf, key.data(), key.size());
            buf[key.size()] = '\0';
            return pool_copy(std::string_view(buf, key.size() + 1));
         }
         std::string tmp(key);
         tmp.push_back('\0');
         return pool_copy(tmp);
      }

      std::vector<range>                    _ranges;  // sorted, non-overlapping
      copy_fn_t                             _copy_fn  = nullptr;
      void*                                 _copy_ctx = nullptr;
      std::pmr::monotonic_buffer_resource   _own_pool;  // fallback when no external pool
   };

}  // namespace psitri::dwal
