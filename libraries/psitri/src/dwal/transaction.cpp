#include <psitri/dwal/transaction_impl.hpp>

namespace psitri::dwal
{
   // Explicit instantiation for the default lock policy.
   template class basic_transaction<std_lock_policy>;
}  // namespace psitri::dwal
