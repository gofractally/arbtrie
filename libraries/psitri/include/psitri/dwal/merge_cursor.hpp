#pragma once
#include <psitri/cursor.hpp>
#include <psitri/dwal/btree_layer.hpp>
#include <psitri/dwal/btree_value.hpp>

#include <optional>
#include <string>
#include <string_view>

namespace psitri::dwal
{
   /// A merge cursor that presents a unified sorted view across the three
   /// DWAL layers: RW btree, RO btree, and PsiTri COW tree.
   ///
   /// At each position, the cursor holds the smallest live (non-tombstoned)
   /// key across all sources. Higher layers (RW > RO > Tri) shadow lower
   /// layers for the same key.
   ///
   /// Layer pointers may be null to exclude layers (e.g., read_mode::buffered
   /// omits RW, read_mode::persistent omits RW+RO).
   class merge_cursor
   {
     public:
      using btree_iter = btree_layer::iterator;

      /// Construct a merge cursor over up to three layers.
      /// Any layer pointer may be null to exclude that layer.
      /// The tri cursor is optional — pass a default-constructed optional to skip.
      merge_cursor(const btree_layer*            rw,
                   const btree_layer*            ro,
                   std::optional<psitri::cursor>  tri);

      // ── Positioning ────────────────────────────────────────────────

      bool seek_begin();
      bool seek_last();
      bool lower_bound(std::string_view key);
      bool upper_bound(std::string_view key);
      bool seek(std::string_view key);

      // ── Navigation ─────────────────────────────────────────────────

      bool next();
      bool prev();

      // ── State ──────────────────────────────────────────────────────

      bool is_end() const noexcept { return _at_end; }
      bool is_rend() const noexcept { return _at_rend; }

      /// Current key (valid only when !is_end() && !is_rend()).
      std::string_view key() const noexcept { return _current_key; }

      /// Current value. Only valid for data values from btree layers.
      /// For PsiTri values, the caller should use the tri cursor directly.
      const btree_value& current_value() const noexcept { return _current_value; }

      /// Which layer produced the current key.
      enum class source
      {
         rw,
         ro,
         tri,
         none
      };
      source current_source() const noexcept { return _source; }

      /// Whether the current value is a subtree.
      bool is_subtree() const noexcept;

      /// Access the underlying tri cursor (e.g., for value reads, subtree access).
      psitri::cursor*       tri_cursor() noexcept;
      const psitri::cursor* tri_cursor() const noexcept;

      // ── Counting ───────────────────────────────────────────────────

      /// Count live keys in [lower, upper). Uses iteration (exact).
      uint64_t count_keys(std::string_view lower = {}, std::string_view upper = {});

     private:
      /// Advance the merge state to find the next live key >= current positions.
      /// Returns false if all sources are exhausted (at end).
      bool advance_forward();

      /// Retreat the merge state to find the previous live key.
      bool advance_backward();

      /// Check if a key is shadowed by tombstones in higher layers.
      bool is_tombstoned_by_rw(std::string_view k) const;
      bool is_tombstoned_by_ro(std::string_view k) const;

      /// Skip the RO and/or Tri iterators past a key that was taken by a higher layer.
      void skip_ro_past(std::string_view k);
      void skip_tri_past(std::string_view k);

      // Layer pointers (null = excluded).
      const btree_layer* _rw = nullptr;
      const btree_layer* _ro = nullptr;

      // Btree iterators.
      btree_iter _rw_it, _rw_end;
      btree_iter _ro_it, _ro_end;

      // PsiTri cursor (optional).
      std::optional<psitri::cursor> _tri;

      // Current merged position.
      std::string  _current_key;
      btree_value  _current_value;
      source       _source  = source::none;
      bool         _at_end  = true;
      bool         _at_rend = true;
   };

}  // namespace psitri::dwal
