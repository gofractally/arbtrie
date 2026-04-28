// LevelDB-compatible WriteBatch for psitrileveldb.
//
// Storage matches upstream LevelDB: ops are appended into a single flat
// std::string buffer, no per-Put heap allocation beyond amortized growth.
// DB::Write iterates the buffer once and replays into a psitri transaction.
#pragma once

#include <leveldb/slice.h>
#include <leveldb/status.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace leveldb {

class WriteBatch {
 public:
    enum class Tag : uint8_t { kDelete = 0x0, kPut = 0x1 };

    class Handler {
     public:
        virtual ~Handler() = default;
        virtual void Put(const Slice& key, const Slice& value) = 0;
        virtual void Delete(const Slice& key) = 0;
    };

    WriteBatch() = default;
    ~WriteBatch() = default;

    WriteBatch(const WriteBatch&) = default;
    WriteBatch& operator=(const WriteBatch&) = default;

    void Put(const Slice& key, const Slice& value) {
        rep_.push_back(static_cast<char>(Tag::kPut));
        AppendVarint64(rep_, key.size());
        rep_.append(key.data(), key.size());
        AppendVarint64(rep_, value.size());
        rep_.append(value.data(), value.size());
    }

    void Delete(const Slice& key) {
        rep_.push_back(static_cast<char>(Tag::kDelete));
        AppendVarint64(rep_, key.size());
        rep_.append(key.data(), key.size());
    }

    void Clear() { rep_.clear(); }

    // O(1) — caller (Bitcoin Core's CCoinsViewDB::BatchWrite) invokes this
    // on every iteration of its per-coin loop, so it must not scan ops.
    // Add a small constant for the header overhead LevelDB users expect.
    size_t ApproximateSize() const { return rep_.size() + 12; }

    Status Iterate(Handler* h) const {
        const char* p   = rep_.data();
        const char* end = p + rep_.size();
        while (p < end) {
            Tag tag = static_cast<Tag>(static_cast<uint8_t>(*p++));
            uint64_t klen = 0;
            p = ReadVarint64(p, end, &klen);
            assert(p && klen <= static_cast<uint64_t>(end - p));
            const char* kp = p;
            p += klen;
            if (tag == Tag::kPut) {
                uint64_t vlen = 0;
                p = ReadVarint64(p, end, &vlen);
                assert(p && vlen <= static_cast<uint64_t>(end - p));
                const char* vp = p;
                p += vlen;
                h->Put(Slice(kp, klen), Slice(vp, vlen));
            } else {
                h->Delete(Slice(kp, klen));
            }
        }
        return Status::OK();
    }

    void Append(const WriteBatch& src) { rep_.append(src.rep_); }

    bool Empty() const { return rep_.empty(); }

    // Internal: typed walk for the shim's own DB::Write path. Avoids vtable
    // dispatch from the Handler interface for the hot replay loop.
    template <typename PutFn, typename DeleteFn>
    void ForEach(PutFn put, DeleteFn del) const {
        const char* p   = rep_.data();
        const char* end = p + rep_.size();
        while (p < end) {
            Tag tag = static_cast<Tag>(static_cast<uint8_t>(*p++));
            uint64_t klen = 0;
            p = ReadVarint64(p, end, &klen);
            assert(p && klen <= static_cast<uint64_t>(end - p));
            const char* kp = p;
            p += klen;
            if (tag == Tag::kPut) {
                uint64_t vlen = 0;
                p = ReadVarint64(p, end, &vlen);
                assert(p && vlen <= static_cast<uint64_t>(end - p));
                const char* vp = p;
                p += vlen;
                put(Slice(kp, klen), Slice(vp, vlen));
            } else {
                del(Slice(kp, klen));
            }
        }
    }

 private:
    static void AppendVarint64(std::string& s, uint64_t v) {
        while (v >= 0x80) {
            s.push_back(static_cast<char>(static_cast<uint8_t>(v) | 0x80));
            v >>= 7;
        }
        s.push_back(static_cast<char>(v));
    }

    static const char* ReadVarint64(const char* p, const char* end, uint64_t* out) {
        uint64_t v     = 0;
        unsigned shift = 0;
        while (p < end) {
            uint8_t b = static_cast<uint8_t>(*p++);
            v |= static_cast<uint64_t>(b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                *out = v;
                return p;
            }
            shift += 7;
            if (shift > 63) return nullptr;  // overflow / corrupt
        }
        return nullptr;
    }

    std::string rep_;
};

}  // namespace leveldb
