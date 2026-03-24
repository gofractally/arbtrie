#pragma once
#include <rocksdb/slice.h>

namespace rocksdb
{

   class Comparator
   {
     public:
      virtual ~Comparator() = default;

      virtual int         Compare(const Slice& a, const Slice& b) const = 0;
      virtual const char* Name() const                                  = 0;

      virtual void FindShortestSeparator(std::string*, const Slice&) const {}
      virtual void FindShortSuccessor(std::string*) const {}
   };

   inline const Comparator* BytewiseComparator()
   {
      struct Bytewise : public Comparator
      {
         int         Compare(const Slice& a, const Slice& b) const override { return a.compare(b); }
         const char* Name() const override { return "leveldb.BytewiseComparator"; }
      };
      static Bytewise instance;
      return &instance;
   }

}  // namespace rocksdb
