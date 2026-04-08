#pragma once
#include <art/art_map.hpp>
#include <art/heap_arena.hpp>
#include <cstdint>
#include <string_view>

namespace psitri::detail
{
   /// Entry stored in the write buffer's ART map.
   /// Tracks relationship to the persistent tree for delta_count support.
   /// Value bytes are stored inline in the leaf, after this struct.
   struct buffer_entry
   {
      enum kind : uint8_t
      {
         insert,        // new key, not in persistent tree
         update,        // overwrites existing persistent key
         tombstone,     // removes existing persistent key (affects count)
         tombstone_noop // key didn't exist in persistent tree (no count effect)
      };
      kind     type     = kind::insert;
      uint32_t data_len = 0;

      /// Access inline value data stored after this struct in the leaf.
      std::string_view value() const noexcept
      {
         if (data_len == 0)
            return {};
         return {reinterpret_cast<const char*>(this) + sizeof(buffer_entry), data_len};
      }

      bool is_tombstone() const noexcept
      {
         return type == tombstone || type == tombstone_noop;
      }
      bool is_data() const noexcept { return type == insert || type == update; }
   };

   /// Transaction write buffer — buffers mutations in a heap-backed ART map.
   ///
   /// Uses art_map<buffer_entry, heap_arena> internally. The heap_arena uses
   /// std::vector<char> — no 4 GB mmap, no concurrent reader concerns.
   ///
   /// Supports sub-transaction save/restore via generation-tagged COW.
   /// Aborted sub-transactions leave garbage nodes in the arena that are
   /// reclaimed on clear().
   class write_buffer
   {
     public:
      using map_type = art::art_map<buffer_entry, art::heap_arena>;
      using iterator = art::art_iterator<buffer_entry, art::heap_arena>;

      explicit write_buffer(uint32_t initial_arena_bytes = 1u << 20)
          : _map(initial_arena_bytes)
      {
      }

      // ── Mutations ─────────────────────────────────────────────────────

      /// Insert or overwrite a key-value pair.
      /// @param existed_in_persistent  whether the key exists in the persistent tree
      ///        (only used when key is not already in the buffer)
      void put(std::string_view key, std::string_view value, bool existed_in_persistent)
      {
         buffer_entry* existing = _map.get(key);
         buffer_entry::kind new_kind;

         if (existing)
         {
            auto old_kind = existing->type;
            if (old_kind == buffer_entry::tombstone || old_kind == buffer_entry::tombstone_noop)
            {
               // Restoring a deleted key
               new_kind = (old_kind == buffer_entry::tombstone) ? buffer_entry::update
                                                                : buffer_entry::insert;
               // tombstone → update: was -1, now 0, delta +1
               // tombstone_noop → insert: was 0, now +1, delta +1
               ++_delta_count;
            }
            else
            {
               // insert stays insert, update stays update
               new_kind = old_kind;
            }
         }
         else
         {
            new_kind = existed_in_persistent ? buffer_entry::update : buffer_entry::insert;
            if (new_kind == buffer_entry::insert)
               ++_delta_count;
         }

         buffer_entry entry{new_kind, static_cast<uint32_t>(value.size())};
         _map.upsert_inline(key, entry, value);
      }

      /// Mark a key as deleted.
      /// @param existed_in_persistent  whether the key exists in the persistent tree
      ///        (only used when key is not already in the buffer)
      void erase(std::string_view key, bool existed_in_persistent)
      {
         buffer_entry* existing = _map.get(key);

         if (existing)
         {
            if (existing->is_tombstone())
               return;  // already deleted — no-op

            auto old_kind = existing->type;
            buffer_entry::kind new_kind;

            if (old_kind == buffer_entry::insert)
            {
               new_kind = buffer_entry::tombstone_noop;
               --_delta_count;  // was +1, now 0
            }
            else  // update
            {
               new_kind = buffer_entry::tombstone;
               --_delta_count;  // was 0, now -1
            }

            buffer_entry entry{new_kind, 0};
            _map.upsert(key, entry);
         }
         else
         {
            buffer_entry::kind kind =
                existed_in_persistent ? buffer_entry::tombstone : buffer_entry::tombstone_noop;
            if (kind == buffer_entry::tombstone)
               --_delta_count;

            buffer_entry entry{kind, 0};
            _map.upsert(key, entry);
         }
      }

      // ── Lookup ────────────────────────────────────────────────────────

      /// Look up a key in the buffer.
      /// Returns nullptr if not in buffer. Caller checks is_tombstone().
      const buffer_entry* get(std::string_view key) const noexcept
      {
         return const_cast<map_type&>(_map).get(key);
      }

      // ── Sub-transaction save/restore ──────────────────────────────────

      struct saved_state
      {
         art::offset_t root;
         uint32_t      cow_seq;
         uint32_t      size;
         int32_t       delta_count;
      };

      saved_state save() const noexcept
      {
         return {_map.root(), _map.cow_seq(), _map.size(), _delta_count};
      }

      void restore(const saved_state& s) noexcept
      {
         _map.root()  = s.root;
         _map.set_size(s.size);
         _delta_count = s.delta_count;
         // cow_seq is NOT restored — it monotonically increases.
         // Nodes reachable from restored root have older cow_seq,
         // so subsequent writes will COW them correctly.
      }

      /// Increment COW generation. Subsequent mutations will COW shared nodes.
      void bump_generation() noexcept { _map.bump_cow_seq(); }

      // ── Iteration ─────────────────────────────────────────────────────

      iterator begin() const noexcept
      {
         return const_cast<map_type&>(_map).begin();
      }

      iterator end() const noexcept
      {
         return const_cast<map_type&>(_map).end();
      }

      iterator lower_bound(std::string_view key) const noexcept
      {
         return const_cast<map_type&>(_map).lower_bound(key);
      }

      // ── Count support ─────────────────────────────────────────────────

      /// Net key count change vs persistent tree (+inserts, -tombstones).
      int32_t delta_count() const noexcept { return _delta_count; }

      /// Total entries in the buffer (data + tombstones).
      uint32_t size() const noexcept { return _map.size(); }

      // ── Lifecycle ─────────────────────────────────────────────────────

      void clear() noexcept
      {
         _map.clear();
         _delta_count = 0;
      }

      /// Reset root and counters without clearing the arena.
      /// Used after merge-then-delegate: the arena data is preserved so that
      /// sub-transaction save/restore can still access pre-merge buffer entries.
      /// New writes allocate from the current arena cursor and won't overwrite old data.
      void soft_clear() noexcept
      {
         _map.root()  = art::null_offset;
         _map.set_size(0);
         _delta_count = 0;
         _map.bump_cow_seq();  // ensure subsequent writes COW correctly
      }

      bool empty() const noexcept { return _map.empty(); }

      /// Access the underlying arena (for frame bump allocation).
      art::heap_arena& arena() noexcept { return _map.get_arena(); }

     private:
      map_type _map;
      int32_t  _delta_count = 0;  // net: +1 per insert, -1 per tombstone
   };

}  // namespace psitri::detail
