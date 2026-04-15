#pragma once
#include <psitri/dwal/wal_format.hpp>
#include <sal/numbers.hpp>

#include <cstdint>
#include <filesystem>
#include <string_view>
#include <vector>

namespace psitri::dwal
{
   /// Represents a single decoded WAL operation for replay.
   struct wal_operation
   {
      wal_op_type      type;
      std::string_view key;       // points into reader's buffer
      std::string_view value;     // upsert_data only
      sal::tree_id     subtree;   // upsert_subtree only
      std::string_view range_low; // remove_range only
      std::string_view range_high;// remove_range only
   };

   /// Decoded WAL entry — one committed transaction.
   struct wal_entry
   {
      uint64_t                     sequence = 0;
      std::vector<wal_operation>   ops;

      /// Multi-root transaction fields (0 for single-root entries).
      uint8_t  entry_flags             = 0;
      uint64_t multi_tx_id             = 0;
      uint16_t multi_participant_count = 0;

      bool is_multi_tx() const noexcept { return multi_tx_id != 0; }
      bool is_multi_tx_commit() const noexcept
      {
         return (entry_flags & wal_entry_flag_multi_tx_commit) != 0;
      }
   };

   /// Read-only WAL file reader for crash recovery.
   ///
   /// Reads the file sequentially, validates each entry's hash, and yields
   /// decoded entries. Stops at the first invalid entry (torn write).
   class wal_reader
   {
     public:
      /// Open a WAL file for reading. Returns false if file doesn't exist
      /// or has an invalid header.
      bool open(const std::filesystem::path& path);

      /// Read the file header. Must be called after open().
      const wal_header& header() const noexcept { return _header; }

      /// Read the next valid entry. Returns false at EOF or first invalid entry.
      bool next(wal_entry& entry);

      /// Replay all valid entries, calling the visitor for each.
      /// Returns the number of entries replayed.
      template <typename Visitor>
      uint64_t replay_all(Visitor&& visitor)
      {
         wal_entry entry;
         uint64_t  count = 0;
         while (next(entry))
         {
            visitor(entry);
            ++count;
         }
         return count;
      }

      /// The sequence number one past the last valid entry read.
      uint64_t end_sequence() const noexcept { return _end_seq; }

      /// Whether the file was cleanly closed.
      bool was_clean_close() const noexcept
      {
         return (_header.flags & wal_flag_clean_close) != 0;
      }

     private:
      bool read_bytes(void* dst, size_t len);
      bool read_entry_raw(std::vector<char>& buf);
      bool decode_entry(const std::vector<char>& buf, wal_entry& entry);

      int                   _fd       = -1;
      uint64_t              _file_size = 0;
      uint64_t              _read_pos  = 0;
      uint64_t              _end_seq   = 0;
      wal_header            _header    = {};
      std::vector<char>     _raw_buf;   // reusable buffer for raw entry bytes
   };

}  // namespace psitri::dwal
