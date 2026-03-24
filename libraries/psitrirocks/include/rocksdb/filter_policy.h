#pragma once
#include <memory>

namespace rocksdb
{

   class FilterPolicy
   {
     public:
      virtual ~FilterPolicy() = default;
   };

   inline const FilterPolicy* NewBloomFilterPolicy(int, bool = true) { return nullptr; }

}  // namespace rocksdb
