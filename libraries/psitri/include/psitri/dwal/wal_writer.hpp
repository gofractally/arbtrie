#pragma once
#include <psitri/dwal/wal_format.hpp>
#include <sal/numbers.hpp>

#include <cstdint>
#include <filesystem>
#include <string_view>
#include <vector>

namespace psitri::dwal
{
   /// Buffered, append-only WAL writer.
   ///
   /// Accumulates operations for the current transaction into an in-memory
   /// buffer, then writes a complete entry (with trailing xxh3_64 hash) on
   /// commit. No fsync per transaction — the caller invokes flush() when
   /// durability is needed (periodic app-level flush).
   ///
   /// Thread safety: NOT thread-safe. The caller holds the per-root exclusive
   /// lock during transaction commit, so only one thread writes at a time.
   class wal_writer
   {
     public:
      /// Open or create a WAL file at the given path.
      /// If the file exists and has a valid header, appends after the last
      /// valid entry. Otherwise creates a fresh file.
      explicit wal_writer(const std::filesystem::path& path,
                          uint16_t                     root_index,
                          uint64_t                     sequence_base = 0);

      ~wal_writer();

      wal_writer(const wal_writer&)            = delete;
      wal_writer& operator=(const wal_writer&) = delete;
      wal_writer(wal_writer&& other) noexcept;
      wal_writer& operator=(wal_writer&& other) noexcept;

      /// Begin accumulating operations for a new transaction.
      void begin_entry();

      /// Add an upsert (data) operation.
      void add_upsert_data(std::string_view key, std::string_view value);

      /// Add an upsert (subtree) operation.
      void add_upsert_subtree(std::string_view key, sal::ptr_address addr);

      /// Add a remove operation.
      void add_remove(std::string_view key);

      /// Add a range remove operation.
      void add_remove_range(std::string_view low, std::string_view high);

      /// Finalize the current entry: compute hash, write to file buffer.
      /// Returns the sequence number assigned to this entry.
      uint64_t commit_entry();

      /// Discard the current in-progress entry (transaction abort).
      void discard_entry();

      /// Flush the OS write buffer to disk (fsync).
      void flush();

      /// Mark the file as cleanly closed and flush.
      void close();

      /// Current write position (file size).
      uint64_t file_size() const noexcept { return _file_pos; }

      /// Next sequence number that will be assigned.
      uint64_t next_sequence() const noexcept { return _next_seq; }

      /// Whether an entry is currently being built.
      bool entry_in_progress() const noexcept { return _entry_active; }

     private:
      void write_bytes(const void* data, size_t len);
      void write_u8(uint8_t v);
      void write_u16(uint16_t v);
      void write_u32(uint32_t v);
      void write_u64(uint64_t v);
      void write_string(std::string_view sv);
      void flush_write_buffer();

      int                    _fd        = -1;
      uint64_t               _file_pos  = 0;
      uint64_t               _next_seq  = 0;
      uint16_t               _op_count  = 0;
      bool                   _entry_active = false;
      std::filesystem::path  _path;

      /// Per-entry buffer: accumulates a single transaction's operations.
      std::vector<char>      _entry_buf;

      /// File write buffer: accumulates completed entries before OS write.
      std::vector<char>      _write_buf;
   };

}  // namespace psitri::dwal
