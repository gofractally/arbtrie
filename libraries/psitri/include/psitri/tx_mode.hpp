#pragma once
#include <cstdint>

namespace psitri
{
   /// Transaction mode.
   ///
   /// Pick the mode that matches what the caller knows about commit
   /// likelihood. The two modes differ in *when* writes touch the tree:
   ///
   /// - `expect_success` — eager: writes go through the COW write path
   ///   immediately. Cheap commit (one root publish), aborts cost a
   ///   dead-version sweep. Best when the txn is almost certainly going
   ///   to commit (replay, import, schema migration, known-good logic).
   ///
   /// - `expect_failure` — deferred: writes accumulate in a RAM delta
   ///   buffer; the tree is only touched on commit (or on a forced flush
   ///   from a "big" op like remove_range). Aborts are free. Best when
   ///   the txn is speculative (block-building, validation, dry-run).
   enum class tx_mode : uint8_t
   {
      expect_success,
      expect_failure,
   };
}  // namespace psitri
