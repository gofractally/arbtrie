#include <psitri/dwal/dwal_read_session_impl.hpp>

namespace psitri::dwal
{
   // Explicit instantiation for the default lock policy.
   template class basic_dwal_read_session<std_lock_policy>;
}  // namespace psitri::dwal
