#pragma once
#include <cstdint>
#include <exception>

namespace psitri
{
   /// Transaction mode.
   enum class tx_mode : uint8_t
   {
      batch,  ///< Mutations go directly to the persistent tree via write_cursor (COW).
      micro,  ///< Mutations are buffered; merged to persistent tree on commit.
      occ     ///< Optimistic: buffered writes, root-version validation at commit.
   };

   /// Thrown by transaction::commit() when an OCC transaction detects a conflict.
   struct occ_conflict : std::exception
   {
      const char* what() const noexcept override { return "OCC transaction conflict"; }
   };
}  // namespace psitri
