#include <psitri/dwal/wal_reader.hpp>

#include <hash/xxhash.h>

#include <cstring>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace psitri::dwal
{
   bool wal_reader::open(const std::filesystem::path& path)
   {
      if (_fd >= 0)
         ::close(_fd);

      _fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
      if (_fd < 0)
         return false;

      struct stat st;
      if (::fstat(_fd, &st) < 0 || st.st_size < static_cast<off_t>(sizeof(wal_header)))
      {
         ::close(_fd);
         _fd = -1;
         return false;
      }

      _file_size = static_cast<uint64_t>(st.st_size);

      if (::pread(_fd, &_header, sizeof(_header), 0) != sizeof(_header))
      {
         ::close(_fd);
         _fd = -1;
         return false;
      }

      if (_header.magic != wal_magic || _header.version != wal_version)
      {
         ::close(_fd);
         _fd = -1;
         return false;
      }

      _read_pos = sizeof(wal_header);
      _end_seq  = _header.sequence_base;
      return true;
   }

   bool wal_reader::next(wal_entry& entry)
   {
      if (_fd < 0)
         return false;

      if (!read_entry_raw(_raw_buf))
         return false;

      if (!decode_entry(_raw_buf, entry))
         return false;

      _end_seq = entry.sequence + 1;
      return true;
   }

   bool wal_reader::read_bytes(void* dst, size_t len)
   {
      if (_read_pos + len > _file_size)
         return false;
      auto n = ::pread(_fd, dst, len, static_cast<off_t>(_read_pos));
      if (n != static_cast<ssize_t>(len))
         return false;
      _read_pos += len;
      return true;
   }

   bool wal_reader::read_entry_raw(std::vector<char>& buf)
   {
      // Read entry_size (first 4 bytes).
      uint32_t entry_size = 0;
      uint64_t saved_pos  = _read_pos;

      if (!read_bytes(&entry_size, 4))
         return false;

      // Sanity: minimum entry is header + hash, maximum bounded by file.
      if (entry_size < wal_entry_header_size + wal_entry_hash_size ||
          saved_pos + entry_size > _file_size)
      {
         _read_pos = saved_pos;
         return false;
      }

      // Read the full entry.
      buf.resize(entry_size);
      std::memcpy(buf.data(), &entry_size, 4);

      // Read remaining bytes after entry_size.
      if (!read_bytes(buf.data() + 4, entry_size - 4))
      {
         _read_pos = saved_pos;
         return false;
      }

      // Validate hash (last 8 bytes).
      uint64_t stored_hash = 0;
      std::memcpy(&stored_hash, buf.data() + entry_size - wal_entry_hash_size, 8);
      uint64_t computed_hash = XXH3_64bits(buf.data(), entry_size - wal_entry_hash_size);

      if (stored_hash != computed_hash)
      {
         _read_pos = saved_pos;
         return false;
      }

      return true;
   }

   bool wal_reader::decode_entry(const std::vector<char>& buf, wal_entry& entry)
   {
      // Accept both v1 (14-byte) and v2 (25-byte) headers.
      if (buf.size() < wal_entry_header_size_v1 + wal_entry_hash_size)
         return false;

      const char* p = buf.data();

      // Parse header fields.
      // uint32_t entry_size at [0..4) — already validated.
      std::memcpy(&entry.sequence, p + 4, 8);
      uint16_t op_count = 0;
      std::memcpy(&op_count, p + 12, 2);

      // Parse v2 multi-tx fields if present, otherwise zero them.
      size_t header_size;
      if (buf.size() >= wal_entry_header_size + wal_entry_hash_size)
      {
         header_size = wal_entry_header_size;
         std::memcpy(&entry.entry_flags, p + 14, 1);
         std::memcpy(&entry.multi_tx_id, p + 15, 8);
         std::memcpy(&entry.multi_participant_count, p + 23, 2);
      }
      else
      {
         header_size = wal_entry_header_size_v1;
         entry.entry_flags             = 0;
         entry.multi_tx_id             = 0;
         entry.multi_participant_count = 0;
      }

      entry.ops.clear();
      entry.ops.reserve(op_count);

      const char* cur = p + header_size;
      const char* end = p + buf.size() - wal_entry_hash_size;

      for (uint16_t i = 0; i < op_count; ++i)
      {
         if (cur >= end)
            return false;

         wal_operation op = {};
         op.type          = static_cast<wal_op_type>(static_cast<uint8_t>(*cur++));

         auto read_u16 = [&]() -> uint16_t
         {
            if (cur + 2 > end)
               return 0;
            uint16_t v;
            std::memcpy(&v, cur, 2);
            cur += 2;
            return v;
         };

         auto read_u32 = [&]() -> uint32_t
         {
            if (cur + 4 > end)
               return 0;
            uint32_t v;
            std::memcpy(&v, cur, 4);
            cur += 4;
            return v;
         };

         auto read_sv = [&]() -> std::string_view
         {
            uint16_t len = read_u16();
            if (cur + len > end)
               return {};
            std::string_view sv(cur, len);
            cur += len;
            return sv;
         };

         switch (op.type)
         {
            case wal_op_type::upsert_data:
            {
               op.key   = read_sv();
               uint32_t vlen = read_u32();
               if (cur + vlen > end)
                  return false;
               op.value = std::string_view(cur, vlen);
               cur += vlen;
               break;
            }
            case wal_op_type::remove:
            {
               op.key = read_sv();
               break;
            }
            case wal_op_type::remove_range:
            {
               op.range_low  = read_sv();
               op.range_high = read_sv();
               break;
            }
            case wal_op_type::upsert_subtree:
            {
               op.key = read_sv();
               auto read_u64 = [&]() -> uint64_t
               {
                  if (cur + 8 > end)
                     return 0;
                  uint64_t v;
                  std::memcpy(&v, cur, 8);
                  cur += 8;
                  return v;
               };
               op.subtree = sal::tree_id::unpack(read_u64());
               break;
            }
            default:
               return false;  // Unknown op type — corrupt entry.
         }

         entry.ops.push_back(op);
      }

      return cur == end;
   }

}  // namespace psitri::dwal
