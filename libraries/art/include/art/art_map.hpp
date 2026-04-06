#pragma once
#include <art/iterator.hpp>
#include <art/node_ops.hpp>

namespace art
{
   template <typename Value>
   class art_map
   {
     public:
      using iterator  = art_iterator<Value>;
      using size_type = uint32_t;

      explicit art_map(uint32_t initial_arena_bytes = 1u << 20)
          : _arena(initial_arena_bytes)
      {
      }

      ~art_map() = default;

      art_map(art_map&&) noexcept            = default;
      art_map& operator=(art_map&&) noexcept = default;
      art_map(const art_map&)                = delete;
      art_map& operator=(const art_map&)     = delete;

      // ── Modifiers ─────────────────────────────────────────────────────

      /// Insert key-value pair. Returns {iterator, true} on insert,
      /// {iterator_to_existing, false} if key already exists.
      std::pair<iterator, bool> insert(std::string_view key, const Value& value)
      {
         auto [vptr, inserted] = art::upsert<Value>(_arena, _root, key, value);
         if (inserted)
            ++_size;
         iterator it;
         it._arena    = &_arena;
         it._root     = _root;
         it._leaf_off = null_offset;  // lightweight — not positioned
         return {it, inserted};
      }

      /// Insert or overwrite. Always returns pointer to the value.
      Value* upsert(std::string_view key, const Value& value)
      {
         auto [vptr, inserted] = art::upsert<Value>(_arena, _root, key, value);
         if (inserted)
            ++_size;
         return vptr;
      }

      /// Erase a single key. Returns true if key was found and removed.
      bool erase(std::string_view key)
      {
         bool removed = art::erase<Value>(_arena, _root, key);
         if (removed)
            --_size;
         return removed;
      }

      // ── Lookup ────────────────────────────────────────────────────────

      /// Fastest lookup path — returns pointer to value or nullptr.
      Value* get(std::string_view key) noexcept
      {
         return art::get<Value>(_arena, _root, key);
      }

      const Value* get(std::string_view key) const noexcept
      {
         return art::get<Value>(const_cast<arena&>(_arena), _root, key);
      }

      /// Find exact key. Returns end() if not found.
      iterator find(std::string_view key) noexcept
      {
         Value* v = get(key);
         if (!v)
            return end();
         return make_lower_bound<Value>(_arena, _root, key);
      }

      iterator find(std::string_view key) const noexcept
      {
         const Value* v = get(key);
         if (!v)
            return end();
         return make_lower_bound<Value>(const_cast<arena&>(_arena), _root, key);
      }

      /// First entry with key >= given key.
      iterator lower_bound(std::string_view key) noexcept
      {
         return make_lower_bound<Value>(_arena, _root, key);
      }

      iterator lower_bound(std::string_view key) const noexcept
      {
         return make_lower_bound<Value>(const_cast<arena&>(_arena), _root, key);
      }

      /// First entry with key > given key.
      iterator upper_bound(std::string_view key) noexcept
      {
         auto it = lower_bound(key);
         if (it != end() && it.key() == key)
            ++it;
         return it;
      }

      iterator upper_bound(std::string_view key) const noexcept
      {
         auto it = lower_bound(key);
         if (it != end() && it.key() == key)
            ++it;
         return it;
      }

      // ── Iteration ─────────────────────────────────────────────────────

      iterator begin() noexcept { return make_begin<Value>(_arena, _root); }
      iterator begin() const noexcept
      {
         return make_begin<Value>(const_cast<arena&>(_arena), _root);
      }

      iterator end() noexcept { return make_end<Value>(_arena, _root); }
      iterator end() const noexcept
      {
         return make_end<Value>(const_cast<arena&>(_arena), _root);
      }

      // ── Capacity ──────────────────────────────────────────────────────

      size_type size() const noexcept { return _size; }
      bool      empty() const noexcept { return _size == 0; }

      /// Reset tree and arena. Reuses arena memory.
      void clear() noexcept
      {
         _arena.clear();
         _root = null_offset;
         _size = 0;
      }

      uint32_t arena_bytes_used() const noexcept { return _arena.bytes_used(); }
      uint32_t arena_capacity() const noexcept { return _arena.capacity(); }

     private:
      arena     _arena;
      offset_t  _root = null_offset;
      size_type _size = 0;
   };

}  // namespace art
