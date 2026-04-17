#pragma once
#include <psitri/lock_policy.hpp>

namespace psitri
{
   template <class LockPolicy> class basic_database;
   template <class LockPolicy> class basic_read_session;
   template <class LockPolicy> class basic_write_session;

   using database      = basic_database<std_lock_policy>;
   using read_session  = basic_read_session<std_lock_policy>;
   using write_session = basic_write_session<std_lock_policy>;
}
