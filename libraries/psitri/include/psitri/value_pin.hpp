#pragma once
#include <sal/allocator_session.hpp>
#include <sal/read_lock.hpp>

namespace psitri
{
   /**
    * Explicit RAII pin for borrowed value views.
    *
    * Normal zero-copy reads should prefer callback APIs. Use value_pin only
    * when a borrowed value_view must remain valid across a small local scope.
    * A value_pin protects memory lifetime; it is not a snapshot and does not
    * make cursors valid across mutation.
    */
   class value_pin
   {
     public:
      explicit value_pin(const sal::allocator_session_ptr& session) : _lock(*session) {}

      value_pin(const value_pin&)            = delete;
      value_pin& operator=(const value_pin&) = delete;
      value_pin(value_pin&&)                 = delete;
      value_pin& operator=(value_pin&&)      = delete;

     private:
      friend class cursor;

      sal::read_lock& lock() noexcept { return _lock; }

      sal::read_lock _lock;
   };
}  // namespace psitri
