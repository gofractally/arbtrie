#pragma once
#include <cstdint>

namespace psitri
{
   inline constexpr uint8_t value_version_bits        = 48;
   inline constexpr uint8_t last_unique_version_bits  = 39;

   constexpr uint64_t version_mask(uint8_t bits) noexcept
   {
      return (uint64_t(1) << bits) - 1;
   }

   constexpr uint64_t version_token(uint64_t v, uint8_t bits) noexcept
   {
      return v & version_mask(bits);
   }

   constexpr uint64_t version_distance(uint64_t from, uint64_t to, uint8_t bits) noexcept
   {
      return (version_token(to, bits) - version_token(from, bits)) & version_mask(bits);
   }

   constexpr bool version_between(uint64_t base,
                                  uint64_t value,
                                  uint64_t newest,
                                  uint8_t  bits) noexcept
   {
      return version_distance(base, value, bits) <= version_distance(base, newest, bits);
   }

   constexpr bool version_visible_at(uint64_t history_base,
                                     uint64_t entry_version,
                                     uint64_t read_version,
                                     uint8_t  bits) noexcept
   {
      return version_distance(history_base, entry_version, bits) <=
             version_distance(history_base, read_version, bits);
   }

   constexpr bool version_older_than(uint64_t a,
                                     uint64_t b,
                                     uint64_t newest,
                                     uint8_t  bits) noexcept
   {
      return version_distance(a, newest, bits) > version_distance(b, newest, bits);
   }

   constexpr bool version_newer_than(uint64_t a,
                                     uint64_t b,
                                     uint8_t  bits) noexcept
   {
      uint64_t distance = version_distance(b, a, bits);
      return distance != 0 && distance < (version_mask(bits) >> 1);
   }

   constexpr bool needs_unique_refresh(uint64_t last_unique_version,
                                       uint64_t epoch_base,
                                       uint64_t root_version) noexcept
   {
      return version_older_than(last_unique_version,
                                epoch_base,
                                root_version,
                                last_unique_version_bits);
   }
}
