#pragma once
#include <algorithm>
#include <arbtrie/time.hpp>
#include <atomic>
#include <chrono>
#include <cstdlib>  // For atexit
#include <format>
#include <mutex>
#include <thread>

namespace arbtrie
{

   inline uint64_t get_current_time_ms()
   {
      // Simply read from the TimeManager
      return time_manager::getCurrentTimeMs();
   }

   template <class Src, class Dst>
   using transcribe_const_t = std::conditional_t<std::is_const<Src>{}, const Dst, Dst>;
   template <class Src, class Dst>
   using transcribe_volatile_t = std::conditional_t<std::is_volatile<Src>{}, Dst volatile, Dst>;
   template <class Src, class Dst>
   using transcribe_cv_t = transcribe_const_t<Src, transcribe_volatile_t<Src, Dst> >;

   template <unsigned int N, typename T>
   constexpr T round_up_multiple(T v)
   {
      static_assert(std::popcount(N) == 1, "N must be power of 2");
      return (v + (T(N) - 1)) & -T(N);
   }
   template <typename T>
   constexpr T round_up_multiple(T v, T N)
   {
      assert(std::popcount(N) == 1 && "N must be power of 2");
      return (v + (N - 1)) & -N;
   }

   template <unsigned int N, typename T>
   constexpr T round_down_multiple(T v)
   {
      static_assert(std::popcount(N) == 1, "N must be power of 2");
      return v & ~(N - 1);
   }

   template <uint32_t offset, uint32_t width>
   constexpr uint64_t make_mask()
   {
      static_assert(offset + width <= 64);
      return (uint64_t(-1) >> (64 - width)) << offset;
   }

   // This always returns a view into the first argument
   inline key_view common_prefix(key_view a, key_view b)
   {
      return {a.begin(), std::mismatch(a.begin(), a.end(), b.begin(), b.end()).first};
   }

   inline std::string to_hex(key_view sv)
   {
      std::string out;
      out.reserve(sv.size() * 2);
      for (auto c : sv)
         out += std::format("{:02x}", uint8_t(c));
      return out;
   }
   inline std::string to_hex(char c)
   {
      return std::format("{:02x}", uint8_t(c));
   }
   inline std::string_view to_str(key_view k)
   {
      return std::string_view((const char*)k.data(), k.size());
   }
   inline key_view to_key_view(const std::string& str)
   {
      return key_view((const byte_type*)str.data(), str.size());
   }
   inline value_view to_value_view(const std::string& str)
   {
      return value_view((const byte_type*)str.data(), str.size());
   }

   inline std::string add_comma(uint64_t v)
   {
      auto s   = std::to_string(v);
      int  n   = s.length() - 3;
      int  end = (v >= 0) ? 0 : 1;  // Support for negative numbers
      while (n > end)
      {
         s.insert(n, ",");
         n -= 3;
      }
      return s;
   };

   inline constexpr key_view to_key(const char* c, size_t len)
   {
      return key_view((const byte_type*)c, len);
   }
   inline constexpr key_view to_key(const uint8_t* c, size_t len)
   {
      return key_view((const byte_type*)c, len);
   }
   inline constexpr key_view to_key(const char* c)
   {
      return key_view((const byte_type*)c, strnlen(c, max_key_length));
   }
   inline constexpr key_view to_value(const char* c, size_t len)
   {
      return key_view((const byte_type*)c, len);
   }
   inline constexpr key_view to_value(const uint8_t* c, size_t len)
   {
      return key_view((const byte_type*)c, len);
   }
   inline constexpr value_view to_value(const char* c)
   {
      return value_view((const byte_type*)c, strnlen(c, max_value_size));
   }

   /**
    * RAII utility that executes a cleanup function when going out of scope
    */
   template <typename F>
   class scoped_exit
   {
      F    _cleanup;
      bool _active = true;  // Flag to track if this instance owns the cleanup responsibility

     public:
      explicit scoped_exit(F&& cleanup) : _cleanup(std::move(cleanup)) {}
      ~scoped_exit()
      {
         if (_active)
            _cleanup();
      }

      // Prevent copying
      scoped_exit(const scoped_exit&)            = delete;
      scoped_exit& operator=(const scoped_exit&) = delete;

      // Allow moving
      scoped_exit(scoped_exit&& other) noexcept
          : _cleanup(std::move(other._cleanup)), _active(other._active)
      {
         other._active = false;  // Transfer ownership
      }

      scoped_exit& operator=(scoped_exit&& other) noexcept
      {
         if (this != &other)
         {
            if (_active)
               _cleanup();  // Call cleanup of current object before replacing it

            _cleanup      = std::move(other._cleanup);
            _active       = other._active;
            other._active = false;  // Transfer ownership
         }
         return *this;
      }
   };

   // Deduction guide
   template <typename F>
   scoped_exit(F&&) -> scoped_exit<F>;
}  // namespace arbtrie
