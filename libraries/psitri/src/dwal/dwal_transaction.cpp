#include <psitri/dwal/dwal_transaction_impl.hpp>

namespace psitri::dwal
{
   // Explicit instantiation for the default lock policy.
   template class basic_dwal_transaction<std_lock_policy>;
   template class basic_remove_result<std_lock_policy>;
}  // namespace psitri::dwal
