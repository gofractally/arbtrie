#pragma once
#include <psitri/dwal/btree_layer.hpp>
#include <psitri/dwal/btree_value.hpp>

#include <cassert>
#include <cstring>
#include <memory>
#include <memory_resource>
#include <string_view>
#include <vector>

namespace psitri::dwal
{
   struct undo_entry
   {
      enum class kind : uint8_t
      {
         /// Key was inserted into btree_map (didn't exist before).
         /// Undo: erase from btree_map.
         insert,

         /// Key existed in btree_map, value was overwritten.
         /// Undo: restore old btree_value.
         overwrite_buffered,

         /// Key existed only in lower layers (RO/PsiTri), shadowed by new entry.
         /// Undo: erase from btree_map (reads fall through to lower layers).
         overwrite_cow,

         /// Key existed in btree_map, was removed/tombstoned.
         /// Undo: re-insert old btree_value.
         erase_buffered,

         /// Key existed only in lower layers, tombstone added to btree_map.
         /// Undo: erase tombstone from btree_map.
         erase_cow,

         /// Range was erased.
         erase_range,
      };

      kind             type;
      std::string_view key;  // always arena-backed

      /// Old value — only meaningful for overwrite_buffered and erase_buffered.
      /// For data values: old_value.data is arena-backed.
      /// For subtree values: old_value.subtree_root is the ptr_address.
      btree_value old_value;

      /// For erase_range only
      struct range_data
      {
         std::string_view low;   // arena-backed
         std::string_view high;  // arena-backed

         struct buffered_entry
         {
            std::string_view key;  // arena-backed
            btree_value      old_value;
         };
         std::vector<buffered_entry> buffered_keys;
      };
      std::unique_ptr<range_data> range;  // non-null only for erase_range
   };

   /// In-memory undo log with nested transaction support via frame stack.
   /// All string_views point into the arena, not the btree_map.
   class undo_log
   {
     public:
      undo_log() : _arena(std::make_unique<std::pmr::monotonic_buffer_resource>()) {}

      undo_log(undo_log&&) noexcept            = default;
      undo_log& operator=(undo_log&&) noexcept = default;
      undo_log(const undo_log&)                = delete;
      undo_log& operator=(const undo_log&)     = delete;

      /// Copy a string into the arena. The returned view is stable for the
      /// lifetime of this undo_log.
      std::string_view arena_copy(std::string_view src)
      {
         if (src.empty())
            return {};
         auto* buf = static_cast<char*>(_arena->allocate(src.size(), 1));
         std::memcpy(buf, src.data(), src.size());
         return {buf, src.size()};
      }

      /// Push a new nested transaction frame.
      void push_frame() { _frame_starts.push_back(static_cast<uint32_t>(_entries.size())); }

      /// Pop the current frame (inner commit — entries merge into parent).
      void pop_frame()
      {
         assert(!_frame_starts.empty());
         _frame_starts.pop_back();
      }

      /// Current frame start index.
      uint32_t current_frame_start() const
      {
         assert(!_frame_starts.empty());
         return _frame_starts.back();
      }

      /// Add an undo entry.
      void push(undo_entry entry) { _entries.push_back(std::move(entry)); }

      /// Record an insert. Undo = erase from btree_map.
      void record_insert(std::string_view key)
      {
         push({undo_entry::kind::insert, arena_copy(key), {}, nullptr});
      }

      /// Record overwriting a buffered value. Undo = restore old value.
      void record_overwrite_buffered(std::string_view key, btree_value old_val)
      {
         // If old value is data, copy it to arena
         if (old_val.is_data())
            old_val.data = arena_copy(old_val.data);
         push({undo_entry::kind::overwrite_buffered, arena_copy(key), old_val, nullptr});
      }

      /// Record overwriting a value from lower layers. Undo = erase from btree_map.
      void record_overwrite_cow(std::string_view key)
      {
         push({undo_entry::kind::overwrite_cow, arena_copy(key), {}, nullptr});
      }

      /// Record erasing a buffered value. Undo = re-insert old value.
      void record_erase_buffered(std::string_view key, btree_value old_val)
      {
         if (old_val.is_data())
            old_val.data = arena_copy(old_val.data);
         push({undo_entry::kind::erase_buffered, arena_copy(key), old_val, nullptr});
      }

      /// Record erasing a value from lower layers. Undo = erase tombstone.
      void record_erase_cow(std::string_view key)
      {
         push({undo_entry::kind::erase_cow, arena_copy(key), {}, nullptr});
      }

      /// Record a range erase.
      void record_erase_range(std::string_view                         low,
                              std::string_view                         high,
                              std::vector<undo_entry::range_data::buffered_entry> buffered_keys)
      {
         auto rd  = std::make_unique<undo_entry::range_data>();
         rd->low  = arena_copy(low);
         rd->high = arena_copy(high);
         // Arena-copy the keys and data values in buffered entries
         for (auto& entry : buffered_keys)
         {
            entry.key = arena_copy(entry.key);
            if (entry.old_value.is_data())
               entry.old_value.data = arena_copy(entry.old_value.data);
         }
         rd->buffered_keys = std::move(buffered_keys);
         push({undo_entry::kind::erase_range, {}, {}, std::move(rd)});
      }

      /// Replay entries from current frame in reverse and truncate.
      /// Caller provides a callback to apply each undo operation to the btree_map.
      /// Returns subtree addresses that need to be released (new values displaced
      /// from btree_map during replay).
      template <typename UndoApplyFn>
      void replay_current_frame(UndoApplyFn&& apply_fn)
      {
         uint32_t start = current_frame_start();
         for (auto i = static_cast<int64_t>(_entries.size()) - 1;
              i >= static_cast<int64_t>(start); --i)
         {
            apply_fn(_entries[static_cast<size_t>(i)]);
         }
         _entries.resize(start);
         pop_frame();
      }

      /// Replay ALL entries in reverse (outermost abort).
      template <typename UndoApplyFn>
      void replay_all(UndoApplyFn&& apply_fn)
      {
         for (auto i = static_cast<int64_t>(_entries.size()) - 1; i >= 0; --i)
         {
            apply_fn(_entries[static_cast<size_t>(i)]);
         }
         _entries.clear();
         _frame_starts.clear();
      }

      /// Collect subtree addresses from old values that need releasing on commit.
      template <typename ReleaseFn>
      void release_old_subtrees(ReleaseFn&& release_fn) const
      {
         for (const auto& entry : _entries)
         {
            if ((entry.type == undo_entry::kind::overwrite_buffered ||
                 entry.type == undo_entry::kind::erase_buffered) &&
                entry.old_value.is_subtree())
            {
               release_fn(entry.old_value.subtree_root);
            }
            if (entry.type == undo_entry::kind::erase_range && entry.range)
            {
               for (const auto& be : entry.range->buffered_keys)
               {
                  if (be.old_value.is_subtree())
                     release_fn(be.old_value.subtree_root);
               }
            }
         }
      }

      /// Discard the entire undo log (outermost commit).
      void discard()
      {
         _entries.clear();
         _frame_starts.clear();
         // Arena memory is freed when this undo_log is destroyed or reset
      }

      bool   empty() const noexcept { return _entries.empty(); }
      size_t depth() const noexcept { return _frame_starts.size(); }
      size_t entry_count() const noexcept { return _entries.size(); }

     private:
      std::vector<undo_entry>  _entries;
      std::vector<uint32_t>    _frame_starts;
      std::unique_ptr<std::pmr::monotonic_buffer_resource> _arena;
   };

}  // namespace psitri::dwal
