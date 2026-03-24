#pragma once
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

namespace rocksdb
{

   class Iterator
   {
     public:
      virtual ~Iterator() = default;

      virtual bool   Valid() const       = 0;
      virtual void   SeekToFirst()       = 0;
      virtual void   SeekToLast()        = 0;
      virtual void   Seek(const Slice& target) = 0;
      virtual void   SeekForPrev(const Slice& target) = 0;
      virtual void   Next()              = 0;
      virtual void   Prev()              = 0;
      virtual Slice  key() const         = 0;
      virtual Slice  value() const       = 0;
      virtual Status status() const      = 0;

      virtual Status Refresh() { return Status::NotSupported(); }
   };

}  // namespace rocksdb
