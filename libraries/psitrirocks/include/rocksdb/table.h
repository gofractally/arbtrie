#pragma once
#include <memory>
#include <rocksdb/cache.h>
#include <rocksdb/filter_policy.h>

namespace rocksdb
{

   class TableFactory;

   struct BlockBasedTableOptions
   {
      std::shared_ptr<Cache> block_cache;
      const FilterPolicy*    filter_policy          = nullptr;
      size_t                 block_size              = 4096;
      bool                   cache_index_and_filter_blocks = false;
      bool                   pin_l0_filter_and_index_blocks_in_cache = false;
      int                    format_version          = 5;
   };

   inline std::shared_ptr<TableFactory> NewBlockBasedTableFactory(const BlockBasedTableOptions& = {})
   {
      return nullptr;
   }

}  // namespace rocksdb
