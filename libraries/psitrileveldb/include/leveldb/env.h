// LevelDB-compatible Env/Logger for psitrileveldb.
// Core subclasses Logger to wire LevelDB messages into BCLog. Env::Default()
// is held but never invoked beyond construction; mem env is used when the
// caller asks for memory_only mode.
#pragma once

#include <cstdarg>

namespace leveldb {

class Logger {
 public:
    Logger() = default;
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;
    virtual ~Logger() = default;
    virtual void Logv(const char* format, va_list ap) = 0;
};

class Env {
 public:
    Env() = default;
    Env(const Env&) = delete;
    Env& operator=(const Env&) = delete;
    virtual ~Env() = default;

    // Process-wide default environment. Caller does NOT take ownership.
    static Env* Default();
};

}  // namespace leveldb
