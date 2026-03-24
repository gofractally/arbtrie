#pragma once
#include <memory>

namespace rocksdb
{

   class Statistics
   {
     public:
      virtual ~Statistics() = default;
   };

   inline std::shared_ptr<Statistics> CreateDBStatistics() { return nullptr; }

}  // namespace rocksdb
