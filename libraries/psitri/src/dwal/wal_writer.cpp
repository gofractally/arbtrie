#include <psitri/dwal/wal_writer.hpp>

#include <hash/xxhash.h>

#include <cassert>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace psitri::dwal
{
   static uint64_t now_nanos()
   {
      auto tp = std::chrono::system_clock::now().time_since_epoch();
      return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(tp).count());
   }

   wal_writer::wal_writer(const std::filesystem::path& path,
                          uint16_t                     root_index,
                          uint64_t                     sequence_base)
       : _next_seq(sequence_base), _path(path)
   {
      _fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
      if (_fd < 0)
         throw std::runtime_error("wal_writer: cannot open " + path.string());

      struct stat st;
      if (::fstat(_fd, &st) < 0)
      {
         ::close(_fd);
         _fd = -1;
         throw std::runtime_error("wal_writer: fstat failed");
      }

      if (st.st_size >= static_cast<off_t>(sizeof(wal_header)))
      {
         // Existing file — validate header and seek to end.
         wal_header hdr;
         if (::pread(_fd, &hdr, sizeof(hdr), 0) == sizeof(hdr) && hdr.magic == wal_magic &&
             hdr.version == wal_version)
         {
            _file_pos = static_cast<uint64_t>(st.st_size);
            _next_seq = hdr.sequence_base;
            // TODO: scan to find actual end sequence (for now, trust caller)
            if (sequence_base > _next_seq)
               _next_seq = sequence_base;
            return;
         }
         // Invalid header — truncate and rewrite.
         ::ftruncate(_fd, 0);
      }

      // Write fresh header.
      wal_header hdr;
      hdr.magic             = wal_magic;
      hdr.version           = wal_version;
      hdr.sequence_base     = sequence_base;
      hdr.created_timestamp = now_nanos();
      hdr.root_index        = root_index;
      hdr.flags             = 0;
      std::memset(hdr.reserved, 0, sizeof(hdr.reserved));

      auto written = ::pwrite(_fd, &hdr, sizeof(hdr), 0);
      if (written != sizeof(hdr))
      {
         ::close(_fd);
         _fd = -1;
         throw std::runtime_error("wal_writer: failed to write header");
      }
      _file_pos = sizeof(wal_header);
   }

   wal_writer::~wal_writer()
   {
      if (_fd >= 0)
         ::close(_fd);
   }

   wal_writer::wal_writer(wal_writer&& other) noexcept
       : _fd(std::exchange(other._fd, -1)),
         _file_pos(other._file_pos),
         _next_seq(other._next_seq),
         _op_count(other._op_count),
         _entry_active(other._entry_active),
         _path(std::move(other._path)),
         _entry_buf(std::move(other._entry_buf)),
         _write_buf(std::move(other._write_buf))
   {
   }

   wal_writer& wal_writer::operator=(wal_writer&& other) noexcept
   {
      if (this != &other)
      {
         if (_fd >= 0)
            ::close(_fd);
         _fd           = std::exchange(other._fd, -1);
         _file_pos     = other._file_pos;
         _next_seq     = other._next_seq;
         _op_count     = other._op_count;
         _entry_active = other._entry_active;
         _path         = std::move(other._path);
         _entry_buf    = std::move(other._entry_buf);
         _write_buf    = std::move(other._write_buf);
      }
      return *this;
   }

   // ── Entry building ──────────────────────────────────────────────────

   void wal_writer::begin_entry()
   {
      assert(!_entry_active);
      _entry_buf.clear();
      _op_count     = 0;
      _entry_active = true;

      // Reserve space for the entry header (written at commit time).
      // Uses the v2 header size (25 bytes) which includes multi-tx fields.
      _entry_buf.resize(wal_entry_header_size);
   }

   void wal_writer::add_upsert_data(std::string_view key, std::string_view value)
   {
      assert(_entry_active);
      write_u8(static_cast<uint8_t>(wal_op_type::upsert_data));
      write_string(key);
      write_u32(static_cast<uint32_t>(value.size()));
      write_bytes(value.data(), value.size());
      ++_op_count;
   }

   void wal_writer::add_upsert_subtree(std::string_view key, sal::ptr_address addr)
   {
      assert(_entry_active);
      write_u8(static_cast<uint8_t>(wal_op_type::upsert_subtree));
      write_string(key);
      uint32_t raw = addr.get_value();
      write_u32(raw);
      ++_op_count;
   }

   void wal_writer::add_remove(std::string_view key)
   {
      assert(_entry_active);
      write_u8(static_cast<uint8_t>(wal_op_type::remove));
      write_string(key);
      ++_op_count;
   }

   void wal_writer::add_remove_range(std::string_view low, std::string_view high)
   {
      assert(_entry_active);
      write_u8(static_cast<uint8_t>(wal_op_type::remove_range));
      write_string(low);
      write_string(high);
      ++_op_count;
   }

   uint64_t wal_writer::finalize_entry(uint8_t entry_flags, uint64_t multi_tx_id,
                                       uint16_t multi_participant_count)
   {
      assert(_entry_active);
      _entry_active = false;

      uint64_t seq        = _next_seq++;
      uint32_t entry_size = static_cast<uint32_t>(_entry_buf.size() + wal_entry_hash_size);

      // Patch the entry header at the beginning of the buffer.
      char* hdr = _entry_buf.data();
      std::memcpy(hdr + 0, &entry_size, 4);
      std::memcpy(hdr + 4, &seq, 8);
      std::memcpy(hdr + 12, &_op_count, 2);
      std::memcpy(hdr + 14, &entry_flags, 1);
      std::memcpy(hdr + 15, &multi_tx_id, 8);
      std::memcpy(hdr + 23, &multi_participant_count, 2);

      // Compute hash over everything except the hash itself.
      uint64_t hash = XXH3_64bits(_entry_buf.data(), _entry_buf.size());

      // Append the hash.
      size_t pos = _entry_buf.size();
      _entry_buf.resize(pos + wal_entry_hash_size);
      std::memcpy(_entry_buf.data() + pos, &hash, 8);

      // Append to the write buffer.
      _write_buf.insert(_write_buf.end(), _entry_buf.begin(), _entry_buf.end());

      return seq;
   }

   uint64_t wal_writer::commit_entry()
   {
      return finalize_entry(0, 0, 0);
   }

   uint64_t wal_writer::commit_entry_multi(uint64_t tx_id, uint16_t participants, bool is_commit)
   {
      uint8_t flags = is_commit ? wal_entry_flag_multi_tx_commit : 0;
      return finalize_entry(flags, tx_id, participants);
   }

   void wal_writer::discard_entry()
   {
      assert(_entry_active);
      _entry_active = false;
      _entry_buf.clear();
      _op_count = 0;
   }

   void wal_writer::flush()
   {
      flush(sal::sync_type::full);
   }

   void wal_writer::flush(sal::sync_type sync)
   {
      assert(_fd >= 0);
      flush_write_buffer();
      if (sync >= sal::sync_type::full)
      {
#ifdef __APPLE__
         ::fcntl(_fd, F_FULLFSYNC);
#else
         ::fsync(_fd);
#endif
      }
      else if (sync >= sal::sync_type::fsync)
      {
         ::fsync(_fd);
      }
      // For msync_async or less, write buffer is flushed but no sync call
   }

   void wal_writer::close()
   {
      if (_fd < 0)
         return;

      flush_write_buffer();

      // Set the clean-close flag in the header.
      uint16_t flags = wal_flag_clean_close;
      ::pwrite(_fd, &flags, sizeof(flags), offsetof(wal_header, flags));

#ifdef __APPLE__
      ::fcntl(_fd, F_FULLFSYNC);
#else
      ::fsync(_fd);
#endif

      ::close(_fd);
      _fd = -1;
   }

   // ── Internal helpers ────────────────────────────────────────────────

   void wal_writer::write_bytes(const void* data, size_t len)
   {
      auto* p = static_cast<const char*>(data);
      _entry_buf.insert(_entry_buf.end(), p, p + len);
   }

   void wal_writer::write_u8(uint8_t v)
   {
      _entry_buf.push_back(static_cast<char>(v));
   }

   void wal_writer::write_u16(uint16_t v)
   {
      write_bytes(&v, sizeof(v));
   }

   void wal_writer::write_u32(uint32_t v)
   {
      write_bytes(&v, sizeof(v));
   }

   void wal_writer::write_u64(uint64_t v)
   {
      write_bytes(&v, sizeof(v));
   }

   void wal_writer::write_string(std::string_view sv)
   {
      write_u16(static_cast<uint16_t>(sv.size()));
      write_bytes(sv.data(), sv.size());
   }

   void wal_writer::flush_write_buffer()
   {
      if (_write_buf.empty() || _fd < 0)
         return;

      const char* data     = _write_buf.data();
      size_t      remaining = _write_buf.size();

      while (remaining > 0)
      {
         auto n = ::pwrite(_fd, data, remaining, static_cast<off_t>(_file_pos));
         if (n <= 0)
            throw std::runtime_error("wal_writer: write failed");
         _file_pos += static_cast<uint64_t>(n);
         data      += n;
         remaining -= static_cast<size_t>(n);
      }

      _write_buf.clear();
   }

}  // namespace psitri::dwal
