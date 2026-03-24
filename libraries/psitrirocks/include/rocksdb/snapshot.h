#pragma once
#include <rocksdb/types.h>

namespace rocksdb
{

   class Snapshot
   {
     public:
      virtual SequenceNumber GetSequenceNumber() const = 0;

     protected:
      virtual ~Snapshot() = default;
   };

}  // namespace rocksdb
