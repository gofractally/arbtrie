#pragma once
#include <ucc/padded_atomic.hpp>

#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <vector>

namespace psitri::dwal
{
   /// Per-session generation lock for epoch-based RO pool reclamation.
   ///
   /// Each reader session has its own padded_atomic<uint64_t>:
   /// - Low 32 bits:  generation the reader is currently observing (or 0xFFFFFFFF if idle)
   /// - High 32 bits: latest generation (broadcast by writer on each swap)
   ///
   /// Readers pay exactly one atomic store to lock and one to unlock.
   /// No CAS, no contention between readers (each has its own cache line).
   struct dwal_session_lock
   {
      static constexpr uint32_t idle = std::numeric_limits<uint32_t>::max();

      ucc::padded_atomic<uint64_t> gen_ptr{uint64_t(-1)};

      /// Pin the current generation — copies high bits to low bits.
      void lock()
      {
         ucc::set_low_bits(gen_ptr,
                           static_cast<uint32_t>(gen_ptr.load(std::memory_order_relaxed) >> 32));
      }

      /// Release the pin — sets low bits to idle (0xFFFFFFFF).
      void unlock() { ucc::set_low_bits(gen_ptr, idle); }

      /// Read the pinned generation (low 32 bits).
      uint32_t pinned_generation() const
      {
         return static_cast<uint32_t>(gen_ptr.load(std::memory_order_acquire));
      }

      /// Broadcast a new generation to this session (sets high 32 bits).
      void broadcast(uint32_t gen) { ucc::set_high_bits(gen_ptr, gen); }
   };

   /// Registry of session locks for epoch-based pool reclamation.
   ///
   /// The merge thread checks `min_pinned()` to determine whether an old
   /// RO pool can be safely freed: safe when min_pinned > pool_generation.
   class epoch_registry
   {
     public:
      /// Allocate a new session lock. Returns its index.
      uint32_t allocate()
      {
         std::lock_guard lk(_mu);
         if (!_free_list.empty())
         {
            uint32_t idx = _free_list.back();
            _free_list.pop_back();
            _locks[idx]->gen_ptr.store(uint64_t(-1), std::memory_order_relaxed);
            return idx;
         }
         uint32_t idx = static_cast<uint32_t>(_locks.size());
         _locks.push_back(std::make_unique<dwal_session_lock>());
         return idx;
      }

      /// Release a session lock slot back to the free list.
      void release(uint32_t idx)
      {
         std::lock_guard lk(_mu);
         _locks[idx]->gen_ptr.store(uint64_t(-1), std::memory_order_relaxed);
         _free_list.push_back(idx);
      }

      /// Get the lock at an index.
      dwal_session_lock& operator[](uint32_t idx) { return *_locks[idx]; }

      /// Broadcast a new generation to all sessions.
      void broadcast_all(uint32_t gen)
      {
         std::lock_guard lk(_mu);
         for (auto& lock : _locks)
            lock->broadcast(gen);
      }

      /// Return the minimum pinned generation across all sessions.
      /// Returns 0xFFFFFFFF if no session is pinned (all idle).
      uint32_t min_pinned() const
      {
         uint32_t min_gen = dwal_session_lock::idle;
         // No lock needed — reading atomic values. Stale reads are safe
         // (conservative: may delay freeing, never premature).
         for (size_t i = 0; i < _locks.size(); ++i)
         {
            uint32_t g = _locks[i]->pinned_generation();
            if (g < min_gen)
               min_gen = g;
         }
         return min_gen;
      }

      size_t size() const
      {
         std::lock_guard lk(_mu);
         return _locks.size();
      }

     private:
      mutable std::mutex                                _mu;
      std::vector<std::unique_ptr<dwal_session_lock>>   _locks;
      std::vector<uint32_t>                             _free_list;
   };

}  // namespace psitri::dwal
