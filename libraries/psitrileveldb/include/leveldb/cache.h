// LevelDB-compatible Cache for psitrileveldb.
// Cache is a no-op shell here; psitri manages its own cache. Core only ever
// constructs one via NewLRUCache, stashes it in Options, and `delete`s it on
// shutdown — it never calls any methods. We provide the minimum to satisfy
// that lifecycle.
#pragma once

#include <cstddef>

namespace leveldb {

class Cache {
 public:
    Cache() = default;
    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;
    virtual ~Cache() = default;
};

Cache* NewLRUCache(size_t capacity);

}  // namespace leveldb
