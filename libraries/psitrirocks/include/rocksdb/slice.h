#pragma once
#include <cassert>
#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>

namespace rocksdb
{

   class Slice
   {
     public:
      Slice() : data_(""), size_(0) {}
      Slice(const char* d, size_t n) : data_(d), size_(n) {}
      Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}
      Slice(const char* s) : data_(s), size_(s ? strlen(s) : 0) {}
      Slice(std::string_view sv) : data_(sv.data()), size_(sv.size()) {}

      const char* data() const { return data_; }
      size_t      size() const { return size_; }
      bool        empty() const { return size_ == 0; }

      char operator[](size_t n) const
      {
         assert(n < size_);
         return data_[n];
      }

      void clear()
      {
         data_ = "";
         size_ = 0;
      }

      void remove_prefix(size_t n)
      {
         assert(n <= size_);
         data_ += n;
         size_ -= n;
      }

      void remove_suffix(size_t n)
      {
         assert(n <= size_);
         size_ -= n;
      }

      std::string ToString(bool hex = false) const
      {
         if (hex)
         {
            std::string result;
            result.reserve(size_ * 2);
            for (size_t i = 0; i < size_; i++)
            {
               static const char hexdigits[] = "0123456789ABCDEF";
               result.push_back(hexdigits[(unsigned char)data_[i] >> 4]);
               result.push_back(hexdigits[(unsigned char)data_[i] & 0xf]);
            }
            return result;
         }
         return std::string(data_, size_);
      }

      int compare(const Slice& b) const
      {
         const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
         int          r       = memcmp(data_, b.data_, min_len);
         if (r == 0)
         {
            if (size_ < b.size_)
               r = -1;
            else if (size_ > b.size_)
               r = +1;
         }
         return r;
      }

      bool starts_with(const Slice& x) const
      {
         return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
      }

      operator std::string_view() const { return std::string_view(data_, size_); }

     private:
      const char* data_;
      size_t      size_;
   };

   inline bool operator==(const Slice& a, const Slice& b)
   {
      return a.size() == b.size() && memcmp(a.data(), b.data(), a.size()) == 0;
   }
   inline bool operator!=(const Slice& a, const Slice& b) { return !(a == b); }

   class PinnableSlice : public Slice
   {
     public:
      PinnableSlice() = default;

      void PinSelf(const Slice& slice)
      {
         buf_.assign(slice.data(), slice.size());
         *static_cast<Slice*>(this) = Slice(buf_);
      }

      void PinSelf(std::string&& s)
      {
         buf_ = std::move(s);
         *static_cast<Slice*>(this) = Slice(buf_);
      }

      void Reset()
      {
         *static_cast<Slice*>(this) = Slice();
         buf_.clear();
      }

      std::string* GetSelf() { return &buf_; }

     private:
      std::string buf_;
   };

}  // namespace rocksdb
