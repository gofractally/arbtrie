#pragma once
#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

namespace psitri::dwal
{
   /// Non-overlapping, sorted list of deleted key ranges [low, high).
   /// Ranges are merged on insert. Used by each btree layer to track
   /// range deletions that shadow keys in lower layers.
   class range_tombstone_list
   {
     public:
      struct range
      {
         std::string low;   // inclusive
         std::string high;  // exclusive

         bool contains(std::string_view key) const noexcept
         {
            return key >= low && key < high;
         }
      };

      /// O(log R) — binary search for range containing key.
      bool is_deleted(std::string_view key) const noexcept
      {
         if (_ranges.empty())
            return false;
         // Find the first range whose low > key, then check the one before it
         auto it = std::upper_bound(
             _ranges.begin(), _ranges.end(), key,
             [](std::string_view k, const range& r) { return k < r.low; });
         if (it == _ranges.begin())
            return false;
         --it;
         return it->contains(key);
      }

      /// Add a range deletion [low, high). Merges with adjacent/overlapping ranges.
      void add(std::string low, std::string high)
      {
         if (low >= high)
            return;

         // Find first range that could overlap (its high > low)
         auto first = std::lower_bound(
             _ranges.begin(), _ranges.end(), low,
             [](const range& r, const std::string& l) { return r.high < l; });

         // Find last range that overlaps (its low < high)
         auto last = std::upper_bound(
             first, _ranges.end(), high,
             [](const std::string& h, const range& r) { return h < r.low; });

         if (first == last)
         {
            // No overlap — insert new range
            _ranges.insert(first, range{std::move(low), std::move(high)});
         }
         else
         {
            // Merge: extend first to cover everything, erase the rest
            first->low  = std::min(first->low, std::move(low));
            auto& last_range = *(last - 1);
            first->high = std::max(last_range.high, std::move(high));
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
               std::string successor(key);
               successor.push_back('\0');

               if (key == it->low)
               {
                  // Key is at the start — just shrink from the left
                  it->low = std::move(successor);
                  if (it->low >= it->high)
                     _ranges.erase(it);
               }
               else if (successor >= it->high)
               {
                  // Key is at the end — just shrink from the right
                  it->high = std::string(key);
                  if (it->low >= it->high)
                     _ranges.erase(it);
               }
               else
               {
                  // Key is in the middle — split into two ranges
                  std::string old_high = std::move(it->high);
                  it->high = std::string(key);
                  _ranges.insert(it + 1, range{std::move(successor), std::move(old_high)});
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
      std::vector<range> _ranges;  // sorted, non-overlapping
   };

}  // namespace psitri::dwal
