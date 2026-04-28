// LevelDB-compatible Comparator for psitrileveldb.
// Core never sets a custom comparator — default is bytewise. Declared for
// header completeness only.
#pragma once

#include <leveldb/slice.h>

#include <string>

namespace leveldb {

class Comparator {
 public:
    virtual ~Comparator() = default;
    virtual int Compare(const Slice& a, const Slice& b) const = 0;
    virtual const char* Name() const = 0;
    virtual void FindShortestSeparator(std::string* start, const Slice& limit) const = 0;
    virtual void FindShortSuccessor(std::string* key) const = 0;
};

const Comparator* BytewiseComparator();

}  // namespace leveldb
