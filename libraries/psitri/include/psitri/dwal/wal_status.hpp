#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>

namespace psitri::dwal
{
   static constexpr uint64_t wal_status_magic   = 0x3153544154534c57ull; // "WLSTATS1"
   static constexpr uint32_t wal_status_version = 1;
   static constexpr uint32_t wal_status_max_roots = 512;

   struct alignas(128) wal_root_status
   {
      std::atomic<uint64_t> active{0};
      std::atomic<uint64_t> generation{0};
      std::atomic<uint64_t> merge_complete{1};
      std::atomic<uint64_t> tri_root{0};
      std::atomic<uint64_t> ro_base_root{0};
      std::atomic<uint64_t> throttle_sleep_ns{0};
      std::atomic<uint64_t> arena_at_merge_complete{0};

      std::atomic<uint64_t> rw_layer_entries{0};
      std::atomic<uint64_t> rw_arena_bytes{0};
      std::atomic<uint64_t> ro_layer_entries{0};
      std::atomic<uint64_t> ro_arena_bytes{0};

      std::atomic<uint64_t> rw_wal_file_bytes{0};
      std::atomic<uint64_t> rw_wal_buffered_bytes{0};
      std::atomic<uint64_t> rw_wal_logical_bytes{0};
      std::atomic<uint64_t> ro_wal_file_bytes{0};
      std::atomic<uint64_t> wal_next_sequence{0};

      std::atomic<uint64_t> wal_entries{0};
      std::atomic<uint64_t> wal_ops{0};
      std::atomic<uint64_t> wal_upsert_data_ops{0};
      std::atomic<uint64_t> wal_upsert_subtree_ops{0};
      std::atomic<uint64_t> wal_remove_ops{0};
      std::atomic<uint64_t> wal_remove_range_ops{0};
      std::atomic<uint64_t> wal_multi_entries{0};
      std::atomic<uint64_t> wal_committed_entry_bytes{0};
      std::atomic<uint64_t> wal_key_bytes{0};
      std::atomic<uint64_t> wal_value_bytes{0};

      std::atomic<uint64_t> wal_write_calls{0};
      std::atomic<uint64_t> wal_write_bytes{0};
      std::atomic<uint64_t> wal_flush_calls{0};
      std::atomic<uint64_t> wal_fsync_calls{0};
      std::atomic<uint64_t> wal_fullsync_calls{0};
      std::atomic<uint64_t> wal_clean_closes{0};
      std::atomic<uint64_t> wal_discarded_entries{0};

      std::atomic<uint64_t> swaps{0};
      std::atomic<uint64_t> merge_requests{0};
      std::atomic<uint64_t> merge_completions{0};
      std::atomic<uint64_t> merge_aborts{0};
      std::atomic<uint64_t> merge_entries{0};
      std::atomic<uint64_t> merge_range_tombstones{0};
      std::atomic<uint64_t> merge_wall_ns{0};
      std::atomic<uint64_t> merge_commit_ns{0};
      std::atomic<uint64_t> merge_cpu_ns{0};
   };

   struct alignas(128) wal_status_header
   {
      uint64_t magic      = wal_status_magic;
      uint32_t version    = wal_status_version;
      uint32_t root_count = wal_status_max_roots;
      uint8_t  reserved[112]{};
   };

   static_assert(sizeof(wal_status_header) == 128);

   struct wal_status_file
   {
      wal_status_header roots_header;
      wal_root_status   roots[wal_status_max_roots];
   };

   class wal_status_mapping
   {
     public:
      explicit wal_status_mapping(const std::filesystem::path& wal_dir);
      ~wal_status_mapping();

      wal_status_mapping(const wal_status_mapping&)            = delete;
      wal_status_mapping& operator=(const wal_status_mapping&) = delete;

      wal_root_status* root(uint32_t index) noexcept;
      const wal_status_file* file() const noexcept { return _data; }

     private:
      int              _fd   = -1;
      wal_status_file* _data = nullptr;
   };

} // namespace psitri::dwal
