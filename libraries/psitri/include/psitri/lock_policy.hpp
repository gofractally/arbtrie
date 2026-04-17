#pragma once
#include <mutex>
#include <shared_mutex>

namespace psitri
{
   /**
    * @brief Default lock policy using std::mutex / std::shared_mutex.
    *
    * A lock policy is a trivial traits struct that tells psitri which mutex
    * types to use for internal synchronization (per-root modify locks,
    * database-wide sync lock, DWAL per-root serialization). Consumers that
    * run psitri from a fiber scheduler can supply their own policy whose
    * mutexes yield the fiber on contention rather than blocking the OS
    * thread.
    *
    * A policy must provide:
    *   - `mutex_type` — any type satisfying C++ BasicLockable
    *     (`lock()` / `unlock()` members). Used with `std::lock_guard`
    *     and `std::unique_lock`.
    *   - `shared_mutex_type` — any type satisfying C++ SharedLockable
    *     (`lock()`, `lock_shared()`, `unlock()`, `unlock_shared()`).
    *     Used with `std::shared_lock` and `std::unique_lock` for
    *     reader/writer exclusion (DWAL per-root tx_mutex).
    *
    * The policy is fixed at database construction time via the template
    * parameter on `basic_database<LockPolicy>`; all sessions and cursors
    * obtained from that database carry the same policy. Mixing policies
    * on a single database instance is not supported.
    */
   struct std_lock_policy
   {
      using mutex_type        = std::mutex;
      using shared_mutex_type = std::shared_mutex;
   };

}  // namespace psitri
