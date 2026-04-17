#pragma once
#include <art/art_map.hpp>
#include <psitri/dwal/btree_value.hpp>
#include <psitri/dwal/range_tombstone_list.hpp>
#include <sal/allocator.hpp>

#include <cstring>
#include <string_view>

namespace psitri::dwal
{
   /// A btree layer: ART map + range tombstones.
   ///
   /// Keys are stored in the ART arena. Value data (string_view payloads)
   /// is also stored in the ART arena — no separate pool needed.
   /// The arena is freed as a unit when the layer is discarded.
   ///
   /// Ref-count contract for subtree values:
   /// While a subtree `sal::ptr_address` lives in this layer's map, the
   /// layer holds one reference on it. The reference is taken on insert
   /// (store_subtree), dropped when the entry is overwritten by data /
   /// tombstone / another subtree, dropped when the entry is erased, and
   /// dropped for any remaining subtree entries on destruction. This
   /// mirrors the retain-on-drain semantics in merge_pool_impl.hpp so
   /// callers can pass a bare `ptr_address` without having to manage a
   /// smart_ptr across the handoff.
   ///
   /// Ref-count operations go through the `sal::allocator` directly
   /// rather than through a thread-affine `allocator_session_ptr`:
   /// `retain()` is an atomic increment, and `release()` forwards to the
   /// calling thread's thread-local session, so either can be invoked
   /// safely from any thread that touches this layer (writer, merge
   /// thread, or the database-destructor flush thread).
   struct btree_layer
   {
      using map_type = art::art_map<btree_value>;
      using iterator = map_type::iterator;

      map_type             map;
      range_tombstone_list tombstones;
      uint32_t             generation = 0;

      /// Allocator used to retain/release subtree addresses. Null when
      /// the layer is constructed without one (e.g. standalone tests
      /// that stash fake addresses and never actually reference real
      /// SAL objects). In that mode store_subtree is a no-op for
      /// ref-counting.
      sal::allocator* alloc = nullptr;

      btree_layer() : map(1u << 20)
      {
         tombstones.set_copy_fn(
             [](void* ctx, std::string_view src) -> std::string_view
             {
                return static_cast<btree_layer*>(ctx)->store_string(src);
             },
             this);
      }

      // Non-copyable, non-movable (arena addresses must stay stable).
      btree_layer(const btree_layer&)            = delete;
      btree_layer& operator=(const btree_layer&) = delete;
      btree_layer(btree_layer&&)                 = delete;
      btree_layer& operator=(btree_layer&&)      = delete;

      ~btree_layer()
      {
         release_all_subtrees();
      }

      /// Bind or rebind the allocator used for subtree ref-counting.
      /// Typically called once, before the first store_subtree. Safe to
      /// call repeatedly; the pointer is not owning.
      void set_allocator(sal::allocator* a) noexcept { alloc = a; }

      /// Copy a string into the ART arena, returning a stable string_view.
      /// Used for range tombstone bounds (not for value data — that goes inline in leaves).
      std::string_view store_string(std::string_view src)
      {
         if (src.empty())
            return {};
         auto& arena = map.get_arena();
         auto  off   = arena.allocate(src.size());
         auto* buf   = arena.as<char>(off);
         std::memcpy(buf, src.data(), src.size());
         return {buf, src.size()};
      }

      /// Store a key/data-value pair into the map.
      /// Value data is stored inline in the ART leaf allocation — no separate alloc.
      void store_data(std::string_view key, std::string_view value)
      {
         // If we're overwriting a subtree entry, drop its refcount.
         release_prior_subtree_if_any(key);

         // upsert_inline stores value bytes right after the btree_value struct
         // in the leaf. We then fix up the data string_view to point at the
         // inline copy.
         auto* bv = map.upsert_inline(key, btree_value::make_data({}), value);
         if (bv && !value.empty())
         {
            // The inline data starts right after the btree_value struct
            auto* inline_ptr = reinterpret_cast<const char*>(bv) + sizeof(btree_value);
            bv->data = {inline_ptr, value.size()};
         }
      }

      /// Store a key/subtree pair. Retains one refcount on `addr` —
      /// the caller does NOT need to hold a smart_ptr across this call.
      void store_subtree(std::string_view key, sal::ptr_address addr)
      {
         // If we're replacing another subtree entry, drop the old ref first.
         // Note: if the prior subtree was this same address, we still release
         // and then retain — net zero, correct.
         release_prior_subtree_if_any(key);

         // Retain the new address so our map slot owns one refcount.
         if (alloc && addr != sal::null_ptr_address)
            alloc->retain(addr);

         map.upsert(key, btree_value::make_subtree(addr));
      }

      /// Store a tombstone for a key.
      void store_tombstone(std::string_view key)
      {
         // Tombstone also overwrites a subtree if one was at this key.
         release_prior_subtree_if_any(key);
         map.upsert(key, btree_value::make_tombstone());
      }

      /// Erase a single key. If the prior entry was a subtree, release its ref.
      void erase(std::string_view key)
      {
         release_prior_subtree_if_any(key);
         map.erase(key);
      }

      /// Number of entries (including tombstones).
      size_t size() const noexcept { return map.size(); }

      /// True if both the map and tombstone list are empty.
      bool empty() const noexcept { return map.empty() && tombstones.empty(); }

      /// Release refs for any subtree entries currently in the map.
      /// Safe to call multiple times; after the first call the subtree
      /// addresses are zeroed so subsequent calls are no-ops.
      void release_all_subtrees() noexcept
      {
         if (!alloc)
            return;
         for (auto it = map.begin(); it != map.end(); ++it)
         {
            auto& v = it.value();
            if (v.is_subtree() && v.subtree_root != sal::null_ptr_address)
            {
               alloc->release(v.subtree_root);
               v.subtree_root = sal::null_ptr_address;
            }
         }
      }

     private:
      /// If the entry at `key` is a subtree, release its ref and zero the address
      /// in place so a subsequent release cannot double-decrement.
      void release_prior_subtree_if_any(std::string_view key) noexcept
      {
         if (!alloc)
            return;
         auto* v = map.get(key);
         if (v && v->is_subtree() && v->subtree_root != sal::null_ptr_address)
         {
            alloc->release(v->subtree_root);
            v->subtree_root = sal::null_ptr_address;
         }
      }
   };

}  // namespace psitri::dwal
