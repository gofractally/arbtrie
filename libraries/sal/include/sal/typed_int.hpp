#pragma once
#include <compare>
#include <iostream>
#include <limits>

namespace sal
{
   template <typename T, typename Tag>
   class typed_int
   {
      T value;

     public:
      // Constructors
      constexpr typed_int() : value(T{}) {}
      constexpr explicit typed_int(T v) : value(v) {}

      // Copy and move constructors
      constexpr typed_int(const typed_int& other)     = default;
      constexpr typed_int(typed_int&& other) noexcept = default;

      // Copy and move assignment operators
      constexpr typed_int& operator=(const typed_int& other)     = default;
      constexpr typed_int& operator=(typed_int&& other) noexcept = default;

      // Conversion operators
      constexpr explicit operator T() const { return value; }
      constexpr explicit operator bool() const { return value != 0; }
      constexpr bool     operator!() const { return value == 0; }

      // Safer way to access the underlying value
      constexpr T get_value() const { return value; }

      // Dereference operator for convenient access to the value
      constexpr T operator*() const { return value; }

      template <typename U>
      constexpr U as() const
      {
         return static_cast<U>(value);
      }

      friend constexpr bool operator==(const typed_int& lhs, const typed_int& rhs)
      {
         return lhs.value == rhs.value;
      }
      friend constexpr bool operator!=(const typed_int& lhs, const typed_int& rhs)
      {
         return lhs.value != rhs.value;
      }
      friend constexpr bool operator<(const typed_int& lhs, const typed_int& rhs)
      {
         return lhs.value < rhs.value;
      }
      friend constexpr bool operator<=(const typed_int& lhs, const typed_int& rhs)
      {
         return lhs.value <= rhs.value;
      }
      friend constexpr bool operator>(const typed_int& lhs, const typed_int& rhs)
      {
         return lhs.value > rhs.value;
      }
      friend constexpr bool operator>=(const typed_int& lhs, const typed_int& rhs)
      {
         return lhs.value >= rhs.value;
      }

      friend constexpr auto operator<=>(const typed_int& lhs, const typed_int& rhs)
      {
         return lhs.value <=> rhs.value;
      }

      // Comparison with raw value
      friend constexpr bool operator==(const typed_int& lhs, const T& rhs)
      {
         return lhs.value == rhs;
      }
      friend constexpr bool operator!=(const typed_int& lhs, const T& rhs)
      {
         return lhs.value != rhs;
      }
      friend constexpr bool operator<(const typed_int& lhs, const T& rhs)
      {
         return lhs.value < rhs;
      }
      friend constexpr bool operator<=(const typed_int& lhs, const T& rhs)
      {
         return lhs.value <= rhs;
      }
      friend constexpr bool operator>(const typed_int& lhs, const T& rhs)
      {
         return lhs.value > rhs;
      }
      friend constexpr bool operator>=(const typed_int& lhs, const T& rhs)
      {
         return lhs.value >= rhs;
      }

      // reversed for symmetry
      friend constexpr bool operator==(const T& lhs, const typed_int& rhs)
      {
         return lhs == rhs.value;
      }
      friend constexpr bool operator!=(const T& lhs, const typed_int& rhs)
      {
         return lhs != rhs.value;
      }
      friend constexpr bool operator<(const T& lhs, const typed_int& rhs)
      {
         return lhs < rhs.value;
      }
      friend constexpr bool operator<=(const T& lhs, const typed_int& rhs)
      {
         return lhs <= rhs.value;
      }
      friend constexpr bool operator>(const T& lhs, const typed_int& rhs)
      {
         return lhs > rhs.value;
      }
      friend constexpr bool operator>=(const T& lhs, const typed_int& rhs)
      {
         return lhs >= rhs.value;
      }

      // Arithmetic operators
      constexpr typed_int& operator+=(const typed_int& rhs)
      {
         value += rhs.value;
         return *this;
      }
      constexpr typed_int& operator-=(const typed_int& rhs)
      {
         value -= rhs.value;
         return *this;
      }
      constexpr typed_int& operator*=(const typed_int& rhs)
      {
         value *= rhs.value;
         return *this;
      }
      constexpr typed_int& operator/=(const typed_int& rhs)
      {
         value /= rhs.value;
         return *this;
      }
      constexpr typed_int& operator%=(const typed_int& rhs)
      {
         value %= rhs.value;
         return *this;
      }

      friend constexpr typed_int operator+(typed_int lhs, const typed_int& rhs)
      {
         lhs += rhs;
         return lhs;
      }
      friend constexpr typed_int operator-(typed_int lhs, const typed_int& rhs)
      {
         lhs -= rhs;
         return lhs;
      }
      friend constexpr typed_int operator*(typed_int lhs, const typed_int& rhs)
      {
         lhs *= rhs;
         return lhs;
      }
      friend constexpr typed_int operator/(typed_int lhs, const typed_int& rhs)
      {
         lhs /= rhs;
         return lhs;
      }
      friend constexpr typed_int operator%(typed_int lhs, const typed_int& rhs)
      {
         lhs %= rhs;
         return lhs;
      }

      // Unary operators
      constexpr typed_int operator+() const { return *this; }
      constexpr typed_int operator-() const { return typed_int(-value); }

      // Bitwise operators
      constexpr typed_int& operator&=(const typed_int& rhs)
      {
         value &= rhs.value;
         return *this;
      }
      constexpr typed_int& operator|=(const typed_int& rhs)
      {
         value |= rhs.value;
         return *this;
      }
      constexpr typed_int& operator^=(const typed_int& rhs)
      {
         value ^= rhs.value;
         return *this;
      }
      constexpr typed_int& operator<<=(const typed_int& rhs)
      {
         value <<= rhs.value;
         return *this;
      }
      constexpr typed_int& operator>>=(const typed_int& rhs)
      {
         value >>= rhs.value;
         return *this;
      }

      friend constexpr typed_int operator&(typed_int lhs, const typed_int& rhs)
      {
         lhs &= rhs;
         return lhs;
      }
      friend constexpr typed_int operator|(typed_int lhs, const typed_int& rhs)
      {
         lhs |= rhs;
         return lhs;
      }
      friend constexpr typed_int operator^(typed_int lhs, const typed_int& rhs)
      {
         lhs ^= rhs;
         return lhs;
      }
      friend constexpr typed_int operator<<(typed_int lhs, const typed_int& rhs)
      {
         lhs <<= rhs;
         return lhs;
      }
      friend constexpr typed_int operator>>(typed_int lhs, const typed_int& rhs)
      {
         lhs >>= rhs;
         return lhs;
      }
      friend constexpr typed_int operator~(const typed_int& val) { return typed_int(~val.value); }

      // Increment/decrement operators
      constexpr typed_int& operator++()
      {
         ++value;
         return *this;
      }

      constexpr typed_int operator++(int)
      {
         typed_int tmp(*this);
         ++value;
         return tmp;
      }

      constexpr typed_int& operator--()
      {
         --value;
         return *this;
      }

      constexpr typed_int operator--(int)
      {
         typed_int tmp(*this);
         --value;
         return tmp;
      }

      template <typename CharT, typename Traits>
      friend std::basic_ostream<CharT, Traits>& operator<<(std::basic_ostream<CharT, Traits>& os,
                                                           const typed_int&                   obj)
      {
         return os << obj.value;
      }

      template <typename CharT, typename Traits>
      friend std::basic_istream<CharT, Traits>& operator>>(std::basic_istream<CharT, Traits>& is,
                                                           typed_int&                         obj)
      {
         T val;
         if (is >> val)
            obj.value = val;
         return is;
      }
   };
}  // namespace sal

namespace std
{
   template <typename T, typename Tag>
   struct numeric_limits<sal::typed_int<T, Tag>>
   {
     private:
      using TypedInt = sal::typed_int<T, Tag>;

     public:
      static constexpr bool is_specialized = true;

      static constexpr TypedInt min() noexcept { return TypedInt(std::numeric_limits<T>::min()); }
      static constexpr TypedInt max() noexcept { return TypedInt(std::numeric_limits<T>::max()); }
      static constexpr TypedInt lowest() noexcept
      {
         return TypedInt(std::numeric_limits<T>::lowest());
      }

      static constexpr int  digits       = std::numeric_limits<T>::digits;
      static constexpr int  digits10     = std::numeric_limits<T>::digits10;
      static constexpr int  max_digits10 = std::numeric_limits<T>::max_digits10;
      static constexpr bool is_signed    = std::numeric_limits<T>::is_signed;
      static constexpr bool is_integer   = std::numeric_limits<T>::is_integer;
      static constexpr bool is_exact     = std::numeric_limits<T>::is_exact;
      static constexpr int  radix        = std::numeric_limits<T>::radix;

      static constexpr TypedInt epsilon() noexcept
      {
         return TypedInt(std::numeric_limits<T>::epsilon());
      }
      static constexpr TypedInt round_error() noexcept
      {
         return TypedInt(std::numeric_limits<T>::round_error());
      }

      static constexpr int  min_exponent      = std::numeric_limits<T>::min_exponent;
      static constexpr int  min_exponent10    = std::numeric_limits<T>::min_exponent10;
      static constexpr int  max_exponent      = std::numeric_limits<T>::max_exponent;
      static constexpr int  max_exponent10    = std::numeric_limits<T>::max_exponent10;
      static constexpr bool has_infinity      = std::numeric_limits<T>::has_infinity;
      static constexpr bool has_quiet_NaN     = std::numeric_limits<T>::has_quiet_NaN;
      static constexpr bool has_signaling_NaN = std::numeric_limits<T>::has_signaling_NaN;
      static constexpr bool has_denorm_loss   = std::numeric_limits<T>::has_denorm_loss;

      static constexpr TypedInt infinity() noexcept
      {
         return TypedInt(std::numeric_limits<T>::infinity());
      }
      static constexpr TypedInt quiet_NaN() noexcept
      {
         return TypedInt(std::numeric_limits<T>::quiet_NaN());
      }
      static constexpr TypedInt signaling_NaN() noexcept
      {
         return TypedInt(std::numeric_limits<T>::signaling_NaN());
      }
      static constexpr TypedInt denorm_min() noexcept
      {
         return TypedInt(std::numeric_limits<T>::denorm_min());
      }

      static constexpr bool is_iec559  = std::numeric_limits<T>::is_iec559;
      static constexpr bool is_bounded = std::numeric_limits<T>::is_bounded;
      static constexpr bool is_modulo  = std::numeric_limits<T>::is_modulo;

      static constexpr bool              traps           = std::numeric_limits<T>::traps;
      static constexpr bool              tinyness_before = std::numeric_limits<T>::tinyness_before;
      static constexpr float_round_style round_style     = std::numeric_limits<T>::round_style;
   };
}  // namespace std
