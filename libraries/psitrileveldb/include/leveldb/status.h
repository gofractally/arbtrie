// LevelDB-compatible Status for psitrileveldb.
// Core uses ok(), IsNotFound(), ToString(); we ship the full predicate set
// for compatibility with any consumer that checks them.
#pragma once

#include <leveldb/slice.h>

#include <string>
#include <utility>

namespace leveldb {

class Status {
 public:
    Status() noexcept : code_(kOk) {}

    static Status OK() { return Status(); }
    static Status NotFound(const Slice& msg = {}, const Slice& msg2 = {}) {
        return Status(kNotFound, msg, msg2);
    }
    static Status Corruption(const Slice& msg = {}, const Slice& msg2 = {}) {
        return Status(kCorruption, msg, msg2);
    }
    static Status NotSupported(const Slice& msg = {}, const Slice& msg2 = {}) {
        return Status(kNotSupported, msg, msg2);
    }
    static Status InvalidArgument(const Slice& msg = {}, const Slice& msg2 = {}) {
        return Status(kInvalidArgument, msg, msg2);
    }
    static Status IOError(const Slice& msg = {}, const Slice& msg2 = {}) {
        return Status(kIOError, msg, msg2);
    }

    bool ok() const { return code_ == kOk; }
    bool IsNotFound() const { return code_ == kNotFound; }
    bool IsCorruption() const { return code_ == kCorruption; }
    bool IsIOError() const { return code_ == kIOError; }
    bool IsNotSupportedError() const { return code_ == kNotSupported; }
    bool IsInvalidArgument() const { return code_ == kInvalidArgument; }

    std::string ToString() const {
        if (code_ == kOk) return "OK";
        std::string s;
        switch (code_) {
            case kNotFound:        s = "NotFound: ";        break;
            case kCorruption:      s = "Corruption: ";      break;
            case kNotSupported:    s = "Not implemented: "; break;
            case kInvalidArgument: s = "Invalid argument: ";break;
            case kIOError:         s = "IO error: ";        break;
            default:               s = "Unknown: ";         break;
        }
        s += msg_;
        return s;
    }

 private:
    enum Code { kOk = 0, kNotFound, kCorruption, kNotSupported, kInvalidArgument, kIOError };

    Status(Code c, const Slice& m1, const Slice& m2) : code_(c) {
        msg_.assign(m1.data(), m1.size());
        if (m2.size() > 0) { msg_.append(": "); msg_.append(m2.data(), m2.size()); }
    }

    Code code_;
    std::string msg_;
};

}  // namespace leveldb
