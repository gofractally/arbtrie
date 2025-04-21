#pragma once
#include <sal/allocator_session.hpp>

namespace sal
{
   class read_lock
   {
     public:
      read_lock(allocator_session& ses) : _ses(ses) { _ses.retain_read_lock(); }
      ~read_lock() { _ses.release_read_lock(); }
      read_lock(const read_lock& other) : _ses(other._ses) { _ses.retain_read_lock(); }

     private:
      allocator_session& _ses;
   };
}  // namespace sal
