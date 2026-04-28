// LevelDB-compatible mem env factory for psitrileveldb.
// Core uses NewMemEnv(Env::Default()) for memory_only databases, then
// `delete`s the returned Env on shutdown.
#pragma once

#include <leveldb/env.h>

namespace leveldb {

// Returns a new in-memory Env. Caller takes ownership.
Env* NewMemEnv(Env* base);

}  // namespace leveldb
