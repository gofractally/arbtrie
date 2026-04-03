#pragma once
#include <cstddef>
#include <cstdint>

namespace psitri::dwal
{
   /// WAL file magic: "DWL1" as little-endian uint32.
   static constexpr uint32_t wal_magic   = 0x44574C31;
   static constexpr uint32_t wal_version = 1;

   /// WAL file header — 64 bytes, written once at file creation.
   struct wal_header
   {
      uint32_t magic             = wal_magic;
      uint32_t version           = wal_version;
      uint64_t sequence_base     = 0;  // first sequence number in this file
      uint64_t created_timestamp = 0;  // nanoseconds since epoch (debug only)
      uint16_t root_index        = 0;  // which root this WAL belongs to
      uint16_t flags             = 0;  // bit 0 = clean close
      char     reserved[36]      = {}; // zero-filled
   };
   static_assert(sizeof(wal_header) == 64);

   /// Operation types within a WAL entry.
   enum class wal_op_type : uint8_t
   {
      upsert_data    = 0x01,
      remove         = 0x02,
      remove_range   = 0x03,
      upsert_subtree = 0x04,
   };

   /// WAL entry layout (serialized field-by-field, 14-byte header):
   ///   [0..4)   uint32  entry_size — total bytes including trailing hash
   ///   [4..12)  uint64  sequence   — monotonically increasing
   ///   [12..14) uint16  op_count   — number of operations
   ///   [14..N-8)        operations[]
   ///   [N-8..N) uint64  xxh3_64    — covers bytes [0, N-8)
   static constexpr size_t wal_entry_header_size = 14;
   static constexpr size_t wal_entry_hash_size   = 8;

   /// Flag bit for clean close in wal_header::flags.
   static constexpr uint16_t wal_flag_clean_close = 0x0001;

}  // namespace psitri::dwal
