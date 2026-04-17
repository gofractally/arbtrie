#include <psitri/dwal/dwal_database_impl.hpp>

namespace psitri::dwal::detail
{
   tl_cache_storage& thread_local_cache()
   {
      static thread_local tl_cache_storage s_tl_cache;
      return s_tl_cache;
   }
}  // namespace psitri::dwal::detail

namespace psitri::dwal
{
   template class basic_dwal_database<std_lock_policy>;
}  // namespace psitri::dwal
