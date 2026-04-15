#pragma once
#include <psitri/live_range_map.hpp>
#include <psitri/node/node.hpp>
#include <psitri/value_type.hpp>

namespace psitri
{

   /**
    * Child pointer in a base value_node.  Points to another value_node
    * that holds older versions.  Range [low_ver, high_ver] describes the
    * version span reachable through this child.
    */
   struct value_next_ptr
   {
      ptr_address ptr;       // 4 bytes — child value_node address
      version48   low_ver;   // 6 bytes — lowest version in child
      version48   high_ver;  // 6 bytes — highest version in child
   } __attribute__((packed));
   static_assert(sizeof(value_next_ptr) == 16);

   /**
    * MVCC multi-version value node (base / layer 0).
    *
    * Stores multiple versioned values for a single key.  Each version is
    * recorded as a packed uint64_t entry:
    *
    *     entry = (version << 16) | (offset & 0xFFFF)
    *
    * Sort order is preserved (version dominates upper bits).  Entries are
    * sorted ascending — latest version is last.
    *
    * Offset encoding (int16_t, sign-extended in lower 16 bits):
    *   -2  = tombstone
    *   -1  = null value
    *    0  = reserved
    *    1  = tree_id (subtree) stored in alloc area
    *    2  = zero-length value
    *   >=3 = byte offset from tail() to stored value data
    *
    * Value data in the alloc area: { uint16_t size, uint8_t data[size] }.
    *
    * Layout:
    *   alloc_header  (12 bytes)
    *   _alloc_pos    (uint16_t)  — delta from end of node for alloc area
    *   _num_versions (uint8_t)   — number of entries in entries[]
    *   _num_next     (uint8_t)   — number of child pointers (0-8)
    *   next_ptrs[0.._num_next-1] (16 bytes each)
    *   entries[0.._num_versions-1] (8 bytes each, sorted ascending)
    *   [free space]
    *   ← _alloc_pos (from tail)
    *   value data area (grows from end backward)
    *
    * Max node size: 2048 bytes (unless a single value exceeds 2048).
    *
    * When the base node fills up, entries are sealed into a child
    * (next[i]).  When all 8 next slots are used, the base is pushed
    * to a value_index_node (Phase 3, later).
    */
   struct value_node : public node
   {
      static constexpr node_type type_id       = node_type::value;
      static constexpr uint32_t  max_node_size = 2048;

      /// Values larger than this are stored in "flat" mode — raw data
      /// after the node header, no entry/offset system.  MVCC operations
      /// fall back to COW for flat nodes (correctness, not speed).
      static constexpr uint32_t max_inline_entry_size = 30000;

      /// Returns true if a value can be stored using the packed entry/offset system.
      static bool can_inline(value_view v) noexcept { return v.size() <= max_inline_entry_size; }

      // ── Offset sentinel values ──────────────────────────────────
      static constexpr int16_t offset_tombstone  = -2;
      static constexpr int16_t offset_null        = -1;
      static constexpr int16_t offset_reserved    = 0;
      static constexpr int16_t offset_tree_id     = 1;
      static constexpr int16_t offset_zero_length = 2;
      static constexpr int16_t offset_data_start  = 3;

      // ── Entry packing helpers ───────────────────────────────────
      static uint64_t pack_entry(uint64_t version, int16_t offset) noexcept
      {
         return (version << 16) | (uint16_t(offset));
      }
      static uint64_t entry_version(uint64_t entry) noexcept { return entry >> 16; }
      static int16_t  entry_offset(uint64_t entry) noexcept { return int16_t(entry & 0xFFFF); }

      // ── Allocation sizing ───────────────────────────────────────

      /// Alloc size for a single value (backward-compat constructor).
      static uint32_t alloc_size(value_view v)
      {
         if (!can_inline(v))
         {
            // Flat mode: header + uint32_t size + raw data
            return ucc::round_up_multiple<64>(
                sizeof(value_node) + sizeof(uint32_t) + v.size());
         }
         // header(16) + 0 next_ptrs + 1 entry(8) + value data(2 + v.size())
         return ucc::round_up_multiple<64>(
             sizeof(value_node) + 8 + sizeof(uint16_t) + v.size());
      }

      static uint32_t alloc_size(const value_type& v)
      {
         if (v.is_view())
            return alloc_size(v.view());
         else  // subtree — store tree_id in alloc area
            return ucc::round_up_multiple<64>(
                sizeof(value_node) + 8 + sizeof(uint16_t) + sizeof(tree_id));
      }

      /// Alloc size for appending a version to an existing value_node (MVCC).
      static uint32_t alloc_size(const value_node* src, uint64_t version, value_view new_val)
      {
         uint32_t existing = sizeof(value_node) + src->_num_next * sizeof(value_next_ptr) +
                             src->_num_versions * 8 + src->_alloc_pos;
         uint32_t added = 8 + sizeof(uint16_t) + new_val.size();  // 1 entry + value data
         return std::max(ucc::round_up_multiple<64>(existing + added), src->size());
      }

      /// Alloc size for promoting inline value to 2-entry value_node (MVCC).
      static uint32_t alloc_size(uint64_t old_ver, value_view old_val,
                                 uint64_t new_ver, value_view new_val)
      {
         uint32_t data = sizeof(value_node) + 2 * 8 +  // 2 entries
                         2 * sizeof(uint16_t) + old_val.size() + new_val.size();
         return ucc::round_up_multiple<64>(data);
      }

      /// Alloc size for appending a tombstone to an existing value_node (MVCC).
      static uint32_t alloc_size(const value_node* src, uint64_t version, std::nullptr_t)
      {
         uint32_t existing = sizeof(value_node) + src->_num_next * sizeof(value_next_ptr) +
                             src->_num_versions * 8 + src->_alloc_pos;
         uint32_t added = 8;  // 1 entry only, no value data for tombstone
         return std::max(ucc::round_up_multiple<64>(existing + added), src->size());
      }

      // ── Overloads with dead-version snapshot (for opportunistic cleanup) ──
      // These over-allocate slightly (use source sizes) — stripped dead entries
      // just means less of the allocated space is used, which is fine.
      static uint32_t alloc_size(const value_node* src, uint64_t version, value_view new_val,
                                 const live_range_map::snapshot*)
      {
         return alloc_size(src, version, new_val);
      }
      static uint32_t alloc_size(const value_node* src, uint64_t version, std::nullptr_t,
                                 const live_range_map::snapshot*)
      {
         return alloc_size(src, version, nullptr);
      }

      /// Strip-only: alloc size for copying with dead entries removed (no new entry).
      static uint32_t alloc_size(const value_node* src, const live_range_map::snapshot*)
      {
         return src->size();
      }

      uint32_t num_branches() const { return _is_flat ? 1 : (_num_versions + _num_next); }

      /// True if this node stores a large value in flat mode (no entry/offset system).
      /// MVCC operations fall back to COW for flat nodes.
      bool is_flat() const noexcept { return _is_flat; }

      // ── Constructors ────────────────────────────────────────────

      /// Single data value, version 0.
      value_node(uint32_t asize, ptr_address_seq seq, value_view v)
          : node(asize, type_id, seq),
            _alloc_pos(0),
            _num_versions(0),
            _num_next(0),
            _is_subtree(0),
            _is_flat(0)
      {
         if (!can_inline(v))
         {
            // Flat mode: store raw data directly after node header.
            // No entries[], no offset indirection.
            _is_flat = 1;
            auto* p = reinterpret_cast<uint8_t*>(this) + sizeof(value_node);
            uint32_t sz = v.size();
            std::memcpy(p, &sz, sizeof(uint32_t));
            std::memcpy(p + sizeof(uint32_t), v.data(), v.size());
         }
         else
         {
            append_data_entry(0, v);
         }
      }

      /// Single value_type, version 0.
      value_node(uint32_t asize, ptr_address_seq seq, const value_type& v)
          : node(asize, type_id, seq),
            _alloc_pos(0),
            _num_versions(0),
            _num_next(0),
            _is_subtree(v.is_subtree() ? 1 : 0),
            _is_flat(0)
      {
         assert(v.is_view() or v.is_subtree());
         if (v.is_view())
         {
            if (!can_inline(v.view()))
            {
               _is_flat = 1;
               auto* p = reinterpret_cast<uint8_t*>(this) + sizeof(value_node);
               uint32_t sz = v.size();
               std::memcpy(p, &sz, sizeof(uint32_t));
               std::memcpy(p + sizeof(uint32_t), v.view().data(), v.size());
            }
            else
            {
               append_data_entry(0, v.view());
            }
         }
         else
         {
            tree_id tid = v.subtree_id();
            append_tree_id_entry(0, tid);
         }
      }

      /// MVCC: copy existing value_node and append a new data version.
      value_node(uint32_t asize, ptr_address_seq seq,
                 const value_node* src, uint64_t version, value_view new_val)
          : node(asize, type_id, seq),
            _alloc_pos(0),
            _num_versions(0),
            _num_next(src->_num_next),
            _is_subtree(0),
            _is_flat(0)
      {
         std::memcpy(next_ptrs(), src->next_ptrs(), _num_next * sizeof(value_next_ptr));
         copy_entries_from(src);
         append_data_entry(version, new_val);
      }

      /// MVCC: copy existing value_node, strip dead entries, append new data version.
      value_node(uint32_t asize, ptr_address_seq seq,
                 const value_node* src, uint64_t version, value_view new_val,
                 const live_range_map::snapshot* dead)
          : node(asize, type_id, seq),
            _alloc_pos(0),
            _num_versions(0),
            _num_next(src->_num_next),
            _is_subtree(0),
            _is_flat(0)
      {
         std::memcpy(next_ptrs(), src->next_ptrs(), _num_next * sizeof(value_next_ptr));
         copy_entries_from(src, dead);
         append_data_entry(version, new_val);
      }

      /// MVCC: promote inline value to 2-entry value_node.
      value_node(uint32_t asize, ptr_address_seq seq,
                 uint64_t old_ver, value_view old_val,
                 uint64_t new_ver, value_view new_val)
          : node(asize, type_id, seq),
            _alloc_pos(0),
            _num_versions(0),
            _num_next(0),
            _is_subtree(0),
            _is_flat(0)
      {
         append_data_entry(old_ver, old_val);
         append_data_entry(new_ver, new_val);
      }

      /// MVCC: copy existing value_node and append a tombstone.
      value_node(uint32_t asize, ptr_address_seq seq,
                 const value_node* src, uint64_t version, std::nullptr_t)
          : node(asize, type_id, seq),
            _alloc_pos(0),
            _num_versions(0),
            _num_next(src->_num_next),
            _is_subtree(src->_is_subtree),
            _is_flat(0)
      {
         std::memcpy(next_ptrs(), src->next_ptrs(), _num_next * sizeof(value_next_ptr));
         copy_entries_from(src);
         entries()[_num_versions++] = pack_entry(version, offset_tombstone);
      }

      /// MVCC: copy existing value_node, strip dead entries, append tombstone.
      value_node(uint32_t asize, ptr_address_seq seq,
                 const value_node* src, uint64_t version, std::nullptr_t,
                 const live_range_map::snapshot* dead)
          : node(asize, type_id, seq),
            _alloc_pos(0),
            _num_versions(0),
            _num_next(src->_num_next),
            _is_subtree(src->_is_subtree),
            _is_flat(0)
      {
         std::memcpy(next_ptrs(), src->next_ptrs(), _num_next * sizeof(value_next_ptr));
         copy_entries_from(src, dead);
         entries()[_num_versions++] = pack_entry(version, offset_tombstone);
      }

      /// Strip-only: copy existing value_node with dead entries removed.
      /// No new entry is appended — used by background defrag.
      value_node(uint32_t asize, ptr_address_seq seq,
                 const value_node* src, const live_range_map::snapshot* dead)
          : node(asize, type_id, seq),
            _alloc_pos(0),
            _num_versions(0),
            _num_next(src->_num_next),
            _is_subtree(src->_is_subtree),
            _is_flat(0)
      {
         std::memcpy(next_ptrs(), src->next_ptrs(), _num_next * sizeof(value_next_ptr));
         copy_entries_from(src, dead);
      }

      // ── Accessors ───────────────────────────────────────────────

      /// Get the latest (most recent) value data.
      /// For backward compat — returns the data for the last entry.
      value_view get_data() const
      {
         if (_is_flat)
         {
            auto* p = reinterpret_cast<const uint8_t*>(this) + sizeof(value_node);
            uint32_t sz;
            std::memcpy(&sz, p, sizeof(uint32_t));
            return value_view(reinterpret_cast<const char*>(p + sizeof(uint32_t)), sz);
         }
         assert(_num_versions > 0);
         uint64_t last   = entries()[_num_versions - 1];
         int16_t  offset = entry_offset(last);
         assert(offset >= offset_data_start);
         return get_stored_value(offset);
      }

      /// Get the tree_id from the latest entry.
      tree_id get_tree_id() const
      {
         assert(_is_subtree);
         assert(_num_versions > 0);
         uint64_t last   = entries()[_num_versions - 1];
         int16_t  offset = entry_offset(last);
         assert(offset >= offset_data_start);
         return get_stored_tree_id(offset);
      }

      bool is_subtree_container() const noexcept { return _is_subtree; }

      uint8_t num_versions() const noexcept { return _num_versions; }
      uint8_t num_next() const noexcept { return _num_next; }

      /// Get the version number from the i-th entry.
      uint64_t get_entry_version(uint8_t i) const noexcept
      {
         assert(i < _num_versions);
         return entry_version(entries()[i]);
      }

      /// Get the offset from the i-th entry.
      int16_t get_entry_offset(uint8_t i) const noexcept
      {
         assert(i < _num_versions);
         return entry_offset(entries()[i]);
      }

      /// Get value data for the i-th entry (must have offset >= 3).
      value_view get_entry_value(uint8_t i) const noexcept
      {
         int16_t off = get_entry_offset(i);
         assert(off >= offset_data_start);
         return get_stored_value(off);
      }

      /// Lookup value at a specific version (finds latest entry <= version).
      /// Returns {offset, entry_index} or {offset_null, 0xFF} if not found.
      std::pair<int16_t, uint8_t> find_version(uint64_t version) const noexcept
      {
         if (_is_flat)
            return {offset_data_start, 0};  // flat nodes have 1 implicit version at 0

         // Linear scan from end (latest first) — entries sorted ascending.
         // SIMD acceleration deferred to Phase 7.
         const uint64_t* e = entries();
         for (int i = _num_versions - 1; i >= 0; --i)
         {
            if (entry_version(e[i]) <= version)
               return {entry_offset(e[i]), uint8_t(i)};
         }
         // Not in this node — check children (next_ptrs).
         // For now, children not searched (Phase 6 will add traversal).
         return {offset_null, 0xFF};
      }

      /// Get the value at a specific version.  Returns empty view if not found
      /// or if the entry is a tombstone/null.
      value_view get_value_at_version(uint64_t version) const noexcept
      {
         if (_is_flat)
            return get_data();

         auto [offset, idx] = find_version(version);
         if (offset >= offset_data_start)
            return get_stored_value(offset);
         if (offset == offset_zero_length)
            return value_view();
         return value_view();  // tombstone, null, or not found
      }

      /// Latest version number stored in this node.
      uint64_t latest_version() const noexcept
      {
         if (_is_flat || _num_versions == 0)
            return 0;
         return entry_version(entries()[_num_versions - 1]);
      }

      /// Check if this node has any entries with dead versions.
      bool has_dead_entries(const live_range_map::snapshot* dead) const noexcept
      {
         if (!dead || _is_flat)
            return false;
         const uint64_t* e = entries();
         for (uint8_t i = 0; i < _num_versions; ++i)
         {
            if (dead->is_dead(entry_version(e[i])))
               return true;
         }
         return false;
      }

      /// Count the number of live (non-dead) entries.
      uint8_t count_live_entries(const live_range_map::snapshot* dead) const noexcept
      {
         if (!dead)
            return _num_versions;
         const uint64_t* e = entries();
         uint8_t count = 0;
         for (uint8_t i = 0; i < _num_versions; ++i)
         {
            if (!dead->is_dead(entry_version(e[i])))
               ++count;
         }
         return count;
      }

      // ── COW / compaction support ────────────────────────────────

      uint32_t cow_size() const noexcept { return max_node_size; }

      uint32_t compact_size() const noexcept { return size(); }

      void compact_to(alloc_header* dst) const noexcept
      {
         assert(dst->size() == size());
         ucc::memcpy_aligned_64byte(dst, this, size());
      }

      // ── Branch visitation (for retain/release) ──────────────────

      /// Visit all ptr_addresses that must be retained/released.
      /// This includes next_ptr children and tree_id subtrees in entries.
      void visit_branches(std::invocable<ptr_address> auto&& lam) const
      {
         if (_is_flat)
            return;  // flat nodes have no child pointers

         // Visit child pointers.
         const value_next_ptr* np = next_ptrs();
         for (uint8_t i = 0; i < _num_next; ++i)
            lam(np[i].ptr);

         // Visit tree_id entries (subtree values).
         if (_is_subtree)
         {
            const uint64_t* e = entries();
            for (uint8_t i = 0; i < _num_versions; ++i)
            {
               int16_t off = entry_offset(e[i]);
               if (off >= offset_data_start)
               {
                  tree_id tid = get_stored_tree_id(off);
                  lam(tid.root);
                  if (tid.ver != sal::null_ptr_address)
                     lam(tid.ver);
               }
            }
         }
      }

      void visit_children(const std::function<void(sal::ptr_address)>& visitor) const noexcept
      {
         if (_is_flat)
            return;  // flat nodes have no child pointers

         const value_next_ptr* np = next_ptrs();
         for (uint8_t i = 0; i < _num_next; ++i)
            visitor(np[i].ptr);

         if (_is_subtree)
         {
            const uint64_t* e = entries();
            for (uint8_t i = 0; i < _num_versions; ++i)
            {
               int16_t off = entry_offset(e[i]);
               if (off >= offset_data_start)
               {
                  tree_id tid = get_stored_tree_id(off);
                  visitor(tid.root);
                  if (tid.ver != sal::null_ptr_address)
                     visitor(tid.ver);
               }
            }
         }
      }

      /// Release all children (called by SAL vtable on final release).
      void destroy(const sal::allocator_session_ptr& session) const noexcept
      {
         if (_is_flat)
            return;  // flat nodes have no child pointers

         const value_next_ptr* np = next_ptrs();
         for (uint8_t i = 0; i < _num_next; ++i)
            session->release(np[i].ptr);

         if (_is_subtree)
         {
            const uint64_t* e = entries();
            for (uint8_t i = 0; i < _num_versions; ++i)
            {
               int16_t off = entry_offset(e[i]);
               if (off >= offset_data_start)
               {
                  tree_id tid = get_stored_tree_id(off);
                  session->release(tid.root);
                  if (tid.ver != sal::null_ptr_address)
                     session->release(tid.ver);
               }
            }
         }
      }

      // ── Internal layout accessors ───────────────────────────────

      const value_next_ptr* next_ptrs() const noexcept
      {
         return reinterpret_cast<const value_next_ptr*>(
             reinterpret_cast<const uint8_t*>(this) + sizeof(value_node));
      }
      value_next_ptr* next_ptrs() noexcept
      {
         return reinterpret_cast<value_next_ptr*>(
             reinterpret_cast<uint8_t*>(this) + sizeof(value_node));
      }

      const uint64_t* entries() const noexcept
      {
         return reinterpret_cast<const uint64_t*>(
             reinterpret_cast<const uint8_t*>(next_ptrs()) +
             _num_next * sizeof(value_next_ptr));
      }
      uint64_t* entries() noexcept
      {
         return reinterpret_cast<uint64_t*>(
             reinterpret_cast<uint8_t*>(next_ptrs()) +
             _num_next * sizeof(value_next_ptr));
      }

     private:
      uint16_t _alloc_pos;      // delta from tail() for alloc area
      uint8_t  _num_versions;   // number of entries
      uint8_t  _num_next : 4;   // number of child pointers (0-8)
      uint8_t  _is_subtree : 1; // all entries are tree_ids
      uint8_t  _is_flat : 1;    // flat mode: raw data after header, no entries
      uint8_t  _reserved : 2;

      // Dynamic data follows (accessed via next_ptrs() and entries()):
      //   value_next_ptr  next[_num_next]
      //   uint64_t        entries[_num_versions]
      //   [free space]
      //   ← _alloc_pos (from tail)
      //   stored value data: { uint16_t size, uint8_t data[] } ...

      /// Pointer to tail (end of allocated node memory).
      const uint8_t* tail() const noexcept
      {
         return reinterpret_cast<const uint8_t*>(this) + size();
      }
      uint8_t* tail() noexcept
      {
         return reinterpret_cast<uint8_t*>(this) + size();
      }

      /// Read stored value data at the given byte offset from tail.
      struct stored_value
      {
         uint16_t size;
         uint8_t  data[];
      } __attribute__((packed));

      PSITRI_NO_SANITIZE_ALIGNMENT
      const stored_value* get_stored_value_ptr(int16_t offset) const noexcept
      {
         return reinterpret_cast<const stored_value*>(tail() - offset);
      }

      value_view get_stored_value(int16_t offset) const noexcept
      {
         const stored_value* sv = get_stored_value_ptr(offset);
         return value_view(reinterpret_cast<const char*>(sv->data), sv->size);
      }

      tree_id get_stored_tree_id(int16_t offset) const noexcept
      {
         const stored_value* sv = get_stored_value_ptr(offset);
         assert(sv->size == sizeof(tree_id));
         tree_id tid;
         std::memcpy(&tid, sv->data, sizeof(tree_id));
         return tid;
      }

      /// Allocate space in the alloc area and store value data.
      /// Returns the byte offset from tail().
      PSITRI_NO_SANITIZE_ALIGNMENT
      int16_t alloc_value_data(value_view v) noexcept
      {
         if (v.empty())
            return offset_zero_length;
         _alloc_pos += sizeof(uint16_t) + v.size();
         int16_t offset = int16_t(_alloc_pos);
         assert(offset >= offset_data_start);
         auto* sv = reinterpret_cast<stored_value*>(tail() - offset);
         sv->size = v.size();
         std::memcpy(sv->data, v.data(), v.size());
         return offset;
      }

      /// Allocate space and store a tree_id in the alloc area.
      PSITRI_NO_SANITIZE_ALIGNMENT
      int16_t alloc_tree_id_data(tree_id tid) noexcept
      {
         _alloc_pos += sizeof(uint16_t) + sizeof(tree_id);
         int16_t offset = int16_t(_alloc_pos);
         assert(offset >= offset_data_start);
         auto* sv = reinterpret_cast<stored_value*>(tail() - offset);
         sv->size = sizeof(tree_id);
         std::memcpy(sv->data, &tid, sizeof(tree_id));
         return offset;
      }

      /// Copy all entries and value data from another value_node.
      /// The node must have the same _num_next (already set and child ptrs copied).
      PSITRI_NO_SANITIZE_ALIGNMENT
      void copy_entries_from(const value_node* src) noexcept
      {
         copy_entries_from(src, nullptr);
      }

      /// Copy entries from source, optionally stripping dead versions.
      /// When dead != nullptr, entries whose version is_dead() are skipped
      /// (their value data is not copied, saving space and I/O).
      /// The latest entry (highest version, last in sorted order) is always
      /// preserved regardless of dead status — it represents the current value.
      PSITRI_NO_SANITIZE_ALIGNMENT
      void copy_entries_from(const value_node* src,
                             const live_range_map::snapshot* dead) noexcept
      {
         const uint64_t* se        = src->entries();
         uint8_t         last_idx  = src->_num_versions > 0 ? src->_num_versions - 1 : 0;
         for (uint8_t i = 0; i < src->_num_versions; ++i)
         {
            // Skip dead versions during opportunistic cleanup,
            // but always preserve the latest entry (current value)
            if (dead && i != last_idx && dead->is_dead(entry_version(se[i])))
               continue;

            int16_t off = entry_offset(se[i]);
            if (off >= offset_data_start)
            {
               // Copy value data and get new offset
               const stored_value* sv = src->get_stored_value_ptr(off);
               if (src->_is_subtree)
               {
                  tree_id tid;
                  std::memcpy(&tid, sv->data, sizeof(tree_id));
                  int16_t new_off = alloc_tree_id_data(tid);
                  entries()[_num_versions++] = pack_entry(entry_version(se[i]), new_off);
               }
               else
               {
                  value_view v(reinterpret_cast<const char*>(sv->data), sv->size);
                  int16_t new_off = alloc_value_data(v);
                  entries()[_num_versions++] = pack_entry(entry_version(se[i]), new_off);
               }
            }
            else
            {
               // Tombstone, null, zero-length — no data to copy
               entries()[_num_versions++] = se[i];
            }
         }
      }

      /// Append an entry for a data value.
      void append_data_entry(uint64_t version, value_view v) noexcept
      {
         int16_t offset = alloc_value_data(v);
         entries()[_num_versions++] = pack_entry(version, offset);
      }

      /// Append an entry for a tree_id (subtree).
      void append_tree_id_entry(uint64_t version, tree_id tid) noexcept
      {
         int16_t offset = alloc_tree_id_data(tid);
         entries()[_num_versions++] = pack_entry(version, offset);
      }
   } __attribute__((packed));
   static_assert(sizeof(value_node) == 16);

   template <typename T>
   concept is_value_node = std::same_as<std::remove_cvref_t<std::remove_pointer_t<T>>, value_node>;
}  // namespace psitri
