// LevelDB-compatible Slice for psitrileveldb.
#pragma once

#include <cassert>
#include <cstddef>
#include <cstring>
#include <string>

namespace leveldb {

class Slice {
 public:
    Slice() : data_(""), size_(0) {}
    Slice(const char* d, size_t n) : data_(d), size_(n) {}
    Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}
    Slice(const char* s) : data_(s), size_(std::strlen(s)) {}

    const char* data() const { return data_; }
    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }

    // Iterator surface so `std::span{slice}` deduces a contiguous_range
    // (Bitcoin Core uses MakeByteSpan(slice) → std::as_bytes(std::span{slice})).
    const char* begin() const { return data_; }
    const char* end() const { return data_ + size_; }

    char operator[](size_t n) const { assert(n < size_); return data_[n]; }

    void clear() { data_ = ""; size_ = 0; }
    void remove_prefix(size_t n) { assert(n <= size_); data_ += n; size_ -= n; }

    std::string ToString() const { return std::string(data_, size_); }

    int compare(const Slice& b) const {
        const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
        int r = std::memcmp(data_, b.data_, min_len);
        if (r == 0) {
            if (size_ < b.size_) r = -1;
            else if (size_ > b.size_) r = 1;
        }
        return r;
    }

    bool starts_with(const Slice& x) const {
        return size_ >= x.size_ && std::memcmp(data_, x.data_, x.size_) == 0;
    }

 private:
    const char* data_;
    size_t size_;
};

inline bool operator==(const Slice& a, const Slice& b) {
    return a.size() == b.size() && std::memcmp(a.data(), b.data(), a.size()) == 0;
}
inline bool operator!=(const Slice& a, const Slice& b) { return !(a == b); }

}  // namespace leveldb
