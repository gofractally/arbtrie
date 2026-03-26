#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <sal/alloc_header.hpp>
#include <sal/location.hpp>

namespace sal
{
   /// Result of a full database integrity verification.
   /// Captures enough information per failure to drive targeted repair.
   struct verify_result
   {
      // ── Counters ──────────────────────────────────────────────────────
      struct checksum_counts
      {
         uint64_t passed  = 0;
         uint64_t failed  = 0;
         uint64_t unknown = 0;  // checksum was 0 (not enabled)
         uint64_t total() const { return passed + failed + unknown; }
      };
      struct hash_counts
      {
         uint64_t passed = 0;
         uint64_t failed = 0;
         uint64_t total() const { return passed + failed; }
      };

      checksum_counts segment_checksums;
      checksum_counts object_checksums;
      hash_counts     key_checksums;
      hash_counts     value_checksums;

      uint64_t nodes_visited    = 0;
      uint64_t reachable_bytes  = 0;
      uint64_t dangling_pointers = 0;
      uint32_t roots_checked    = 0;

      // ── Failure details ───────────────────────────────────────────────

      struct segment_failure
      {
         uint32_t segment;       // segment number
         uint32_t sync_pos;      // position of sync_header in segment
         uint32_t range_start;   // start of checksummed byte range
         uint32_t range_end;     // end of checksummed byte range
      };

      struct node_failure
      {
         ptr_address address;           // node's ptr_address
         location    loc;               // segment + offset
         header_type node_type;         // type of the node
         uint32_t    root_index;        // which root tree
         std::string key_prefix_hex;    // hex-encoded key prefix to this node
         std::string failure_type;      // "object_checksum", "dangling_pointer"
         bool        children_reachable = false; // true if we still descended past this
      };

      struct key_failure
      {
         std::string key_hex;           // hex-encoded full key
         uint32_t    root_index;        // which root tree
         ptr_address leaf_address;      // the leaf containing this key
         location    leaf_loc;          // segment + offset of the leaf
         uint16_t    branch_index;      // position within the leaf
         std::string failure_type;      // "key_hash", "value_checksum"
      };

      std::vector<segment_failure> segment_failures;
      std::vector<node_failure>    node_failures;
      std::vector<key_failure>     key_failures;

      bool ok() const
      {
         return segment_checksums.failed == 0 && object_checksums.failed == 0 &&
                key_checksums.failed == 0 && value_checksums.failed == 0 &&
                dangling_pointers == 0;
      }

      uint64_t total_failures() const
      {
         return segment_checksums.failed + object_checksums.failed +
                key_checksums.failed + value_checksums.failed + dangling_pointers;
      }

      // ── Hex encoding helper ───────────────────────────────────────────
      static std::string to_hex(const char* data, size_t len)
      {
         static constexpr char hex[] = "0123456789abcdef";
         std::string           result;
         result.reserve(len * 2);
         for (size_t i = 0; i < len; ++i)
         {
            auto c = static_cast<uint8_t>(data[i]);
            result.push_back(hex[c >> 4]);
            result.push_back(hex[c & 0xf]);
         }
         return result;
      }
      static std::string to_hex(std::string_view sv) { return to_hex(sv.data(), sv.size()); }
   };

}  // namespace sal
