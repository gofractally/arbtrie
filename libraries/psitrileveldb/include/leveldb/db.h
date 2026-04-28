// LevelDB-compatible DB for psitrileveldb.
// MVP: Open / Get / Write / NewIterator / GetProperty / GetApproximateSizes
// / CompactRange / DestroyDB. Snapshot API declared (Core uses None) but
// implementations may return nullptr.
#pragma once

#include <leveldb/iterator.h>
#include <leveldb/options.h>
#include <leveldb/slice.h>
#include <leveldb/status.h>

#include <cstdint>
#include <string>

namespace leveldb {

extern const int kMajorVersion;
extern const int kMinorVersion;

class WriteBatch;
class Snapshot;

struct Range {
    Slice start;
    Slice limit;
    Range() = default;
    Range(const Slice& s, const Slice& l) : start(s), limit(l) {}
};

class DB {
 public:
    static Status Open(const Options& options, const std::string& name, DB** dbptr);

    DB() = default;
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;
    virtual ~DB() = default;

    virtual Status Put(const WriteOptions& opt, const Slice& key, const Slice& value) = 0;
    virtual Status Delete(const WriteOptions& opt, const Slice& key) = 0;
    virtual Status Write(const WriteOptions& opt, WriteBatch* updates) = 0;
    virtual Status Get(const ReadOptions& opt, const Slice& key, std::string* value) = 0;

    virtual Iterator* NewIterator(const ReadOptions& opt) = 0;

    virtual const Snapshot* GetSnapshot() = 0;
    virtual void ReleaseSnapshot(const Snapshot* s) = 0;

    virtual bool GetProperty(const Slice& property, std::string* value) = 0;
    virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) = 0;
    virtual void CompactRange(const Slice* begin, const Slice* end) = 0;
};

Status DestroyDB(const std::string& name, const Options& options);
Status RepairDB(const std::string& name, const Options& options);

}  // namespace leveldb
