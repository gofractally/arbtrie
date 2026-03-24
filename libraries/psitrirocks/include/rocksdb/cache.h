#pragma once
#include <cstddef>
#include <memory>

namespace rocksdb
{

   class Cache
   {
     public:
      virtual ~Cache() = default;
   };

   inline std::shared_ptr<Cache> NewLRUCache(size_t) { return nullptr; }

}  // namespace rocksdb
