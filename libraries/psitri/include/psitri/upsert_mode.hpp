#pragma once

namespace psitri
{
   struct upsert_mode
   {
      enum type : uint32_t
      {
         unique             = 1,  // ref count of all parent nodes and this is 1
         insert             = 2,  // fail if key does exist
         update             = 4,  // fail if key doesn't exist
         same_region        = 8,
         remove             = 16,
         must_remove_f      = 32,
         upsert             = insert | update,
         unique_upsert      = unique | upsert,
         unique_insert      = unique | insert,
         unique_update      = unique | update,
         unique_remove      = unique | remove,
         unique_must_remove = unique | must_remove_f | remove,
         shared_upsert      = upsert,
         shared_insert      = insert,
         shared_update      = update,
         shared_remove      = remove
      };

      constexpr upsert_mode(upsert_mode::type t) : flags(t) {};

      constexpr bool        is_unique() const { return flags & unique; }
      constexpr bool        is_shared() const { return not is_unique(); }
      constexpr bool        is_same_region() const { return flags & same_region; }
      constexpr upsert_mode make_shared() const { return {flags & ~unique}; }
      constexpr upsert_mode make_unique() const { return {flags | unique}; }
      constexpr upsert_mode make_same_region() const { return {flags | same_region}; }
      constexpr bool        may_insert() const { return flags & insert; }
      constexpr bool        may_update() const { return flags & update; }
      constexpr bool        must_insert() const { return not(flags & (update | remove)); }
      constexpr bool        must_update() const { return not is_remove() and not(flags & insert); }
      constexpr bool        is_insert() const { return (flags & insert); }
      constexpr bool        is_upsert() const { return (flags & insert) and (flags & update); }
      constexpr bool        is_remove() const { return flags & remove; }
      constexpr bool        is_update() const { return flags & update; }
      constexpr bool        must_remove() const { return flags & must_remove_f; }

      // private: structural types cannot have private members,
      // but the flags field is not meant to be used directly
      constexpr upsert_mode(uint32_t f) : flags(f) {}
      uint32_t flags;
   };
}  // namespace psitri