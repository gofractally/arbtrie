#include <psitri/dwal/merge_pool_impl.hpp>

namespace psitri::dwal
{
   // Explicit instantiation for the default lock policy.
   // Consumers that use a different LockPolicy (e.g. fiber-aware) instantiate
   // basic_merge_pool in their own translation unit by including
   // <psitri/dwal/merge_pool_impl.hpp> and writing
   //   template class psitri::dwal::basic_merge_pool<my_fiber_policy>;
   template class basic_merge_pool<std_lock_policy>;
}  // namespace psitri::dwal
