#pragma once
#include <mutex>

namespace psitri
{
   /**
    * @brief Default lock policy using std::mutex.
    *
    * A lock policy is a trivial traits struct that tells psitri which mutex
    * type to use for internal synchronization (per-root modify locks,
    * database-wide sync lock). Consumers that run psitri from a fiber
    * scheduler can supply their own policy whose `mutex_type` is a
    * fiber-aware mutex — the fiber yields on contention rather than
    * blocking the underlying OS thread.
    *
    * A policy must provide:
    *   - `mutex_type` — any type satisfying C++ BasicLockable
    *     (`lock()` / `unlock()` members). It is used directly with
    *     `std::lock_guard` and `std::unique_lock`.
    *
    * The policy is fixed at database construction time via the template
    * parameter on `basic_database<LockPolicy>`; all sessions and cursors
    * obtained from that database carry the same policy. Mixing policies
    * on a single database instance is not supported.
    */
   struct std_lock_policy
   {
      using mutex_type = std::mutex;
   };

}  // namespace psitri
