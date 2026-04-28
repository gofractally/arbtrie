// LevelDB-compatible Iterator for psitrileveldb.
#pragma once

#include <leveldb/slice.h>
#include <leveldb/status.h>

namespace leveldb {

class Iterator {
 public:
    Iterator() = default;
    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;
    virtual ~Iterator() = default;

    virtual bool Valid() const = 0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual void Seek(const Slice& target) = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;
    virtual Slice key() const = 0;
    virtual Slice value() const = 0;
    virtual Status status() const = 0;
};

}  // namespace leveldb
