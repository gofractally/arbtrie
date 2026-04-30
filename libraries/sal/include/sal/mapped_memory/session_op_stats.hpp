#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <string_view>

#include <sal/numbers.hpp>
#include <ucc/padded_atomic.hpp>

namespace sal::mapped_memory
{
   enum class session_operation : uint8_t
   {
      cursor_get = 0,
      cursor_find,
      cursor_lower_bound,
      cursor_upper_bound,
      cursor_seek,
      cursor_next,
      cursor_prev,
      cursor_value,
      cursor_value_size,
      cursor_count_keys,
      cursor_key_info,
      txn_get,
      txn_lower_bound,
      txn_upper_bound,
      txn_insert,
      txn_update,
      txn_upsert,
      txn_upsert_sorted,
      txn_remove,
      txn_remove_range_any,
      txn_remove_range_counted,
      tree_upsert,
      tree_remove,
      tree_remove_range_any,
      tree_remove_range_counted,
      count
   };

   inline constexpr uint32_t session_operation_count =
       static_cast<uint32_t>(session_operation::count);

   inline constexpr std::array<std::string_view, session_operation_count>
       session_operation_names = {
           "cursor.get",
           "cursor.find",
           "cursor.lower_bound",
           "cursor.upper_bound",
           "cursor.seek",
           "cursor.next",
           "cursor.prev",
           "cursor.value",
           "cursor.value_size",
           "cursor.count_keys",
           "cursor.key_info",
           "txn.get",
           "txn.lower_bound",
           "txn.upper_bound",
           "txn.insert",
           "txn.update",
           "txn.upsert",
           "txn.upsert_sorted",
           "txn.remove",
           "txn.remove_range_any",
           "txn.remove_range_counted",
           "tree.upsert",
           "tree.remove",
           "tree.remove_range_any",
           "tree.remove_range_counted",
       };

   inline constexpr std::size_t session_operation_cacheline_size =
       ucc::hardware_cacheline_size;

   struct alignas(session_operation_cacheline_size) session_operation_slot
   {
      std::array<std::atomic<uint64_t>, session_operation_count> counts{};

      void clear() noexcept
      {
         for (auto& count : counts)
            count.store(0, std::memory_order_relaxed);
      }

      void add(session_operation op, uint64_t delta = 1) noexcept
      {
         counts[static_cast<uint32_t>(op)].fetch_add(delta, std::memory_order_relaxed);
      }

      uint64_t get(session_operation op) const noexcept
      {
         return counts[static_cast<uint32_t>(op)].load(std::memory_order_relaxed);
      }
   };

   static_assert(sizeof(session_operation_slot) % session_operation_cacheline_size == 0,
                 "session op slots should not share cache lines");

   struct session_operation_stats
   {
      static constexpr uint64_t magic_value = 0x7073697472696f70ull;  // psitriop
      static constexpr uint32_t schema_version = 1;
      static constexpr uint32_t session_cap = 64;

      std::atomic<uint64_t> magic{magic_value};
      std::atomic<uint32_t> version{schema_version};
      std::atomic<uint32_t> session_capacity{session_cap};
      std::atomic<uint32_t> operation_capacity{session_operation_count};
      std::atomic<uint32_t> reserved{0};

      std::array<session_operation_slot, session_cap> sessions;

      void reset() noexcept
      {
         magic.store(magic_value, std::memory_order_relaxed);
         version.store(schema_version, std::memory_order_relaxed);
         session_capacity.store(session_cap, std::memory_order_relaxed);
         operation_capacity.store(session_operation_count, std::memory_order_relaxed);
         reserved.store(0, std::memory_order_relaxed);
         for (auto& session : sessions)
            session.clear();
      }

      bool compatible() const noexcept
      {
         return magic.load(std::memory_order_relaxed) == magic_value &&
                version.load(std::memory_order_relaxed) == schema_version &&
                session_capacity.load(std::memory_order_relaxed) == session_cap &&
                operation_capacity.load(std::memory_order_relaxed) == session_operation_count;
      }

      void add(allocator_session_number session,
               session_operation        op,
               uint64_t                 delta = 1) noexcept
      {
         if (*session >= session_cap)
            return;
         sessions[*session].add(op, delta);
      }

      uint64_t get(allocator_session_number session, session_operation op) const noexcept
      {
         if (*session >= session_cap)
            return 0;
         return sessions[*session].get(op);
      }

      uint64_t total(session_operation op) const noexcept
      {
         uint64_t result = 0;
         for (uint32_t i = 0; i < session_cap; ++i)
            result += sessions[i].get(op);
         return result;
      }
   };
}  // namespace sal::mapped_memory
