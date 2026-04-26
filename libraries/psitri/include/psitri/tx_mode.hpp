#pragma once
#include <cstdint>

namespace psitri
{
   /// Transaction mode.
   enum class tx_mode : uint8_t
   {
      batch,  ///< Mutations go directly to the persistent tree via write_cursor (COW).
      micro,  ///< Mutations are buffered; merged to persistent tree on commit.
   };
}  // namespace psitri
