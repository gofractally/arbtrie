#pragma once
#include <psitri/database.hpp>

namespace psitri
{
   namespace detail
   {
      struct database_state
      {
         database_state()
         {
            for (auto& r : top_root)
               r.store(0, std::memory_order_relaxed);
         }
         uint32_t          magic          = sal::file_magic;
         uint32_t          flags          = 0x77777777;
         std::atomic<bool> clean_shutdown = true;
         runtime_config    config;
         // top_root is protected by _root_change_mutex to prevent race conditions
         // which involve loading or storing top_root, bumping refcounts, decrementing
         // refcounts, cloning, and cleaning up node children when the refcount hits 0.
         // Since it's protected by a mutex, it normally wouldn't need to be atomic.
         // However, making it atomic hopefully aids SIGKILL behavior, which is impacted
         // by instruction reordering and multi-instruction non-atomic writes.
         std::atomic<uint64_t> top_root[num_top_roots];
      };
   }  // namespace detail
}  // namespace psitri