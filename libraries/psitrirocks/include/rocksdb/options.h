#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include <rocksdb/comparator.h>
#include <rocksdb/env.h>
#include <rocksdb/snapshot.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>

namespace rocksdb
{

   enum CompressionType
   {
      kNoCompression     = 0,
      kSnappyCompression = 1,
      kZlibCompression   = 2,
      kBZip2Compression  = 3,
      kLZ4Compression    = 4,
      kLZ4HCCompression  = 5,
      kXpressCompression = 6,
      kZSTD              = 7,
      kDisableCompressionOption = 0xff,
   };

   enum CompactionStyle
   {
      kCompactionStyleLevel  = 0,
      kCompactionStyleUniversal = 1,
      kCompactionStyleFIFO   = 2,
      kCompactionStyleNone   = 3,
   };

   struct ColumnFamilyOptions
   {
      const Comparator* comparator              = BytewiseComparator();
      CompressionType   compression             = kNoCompression;
      size_t            write_buffer_size        = 64 << 20;
      int               max_write_buffer_number  = 2;
      int               num_levels               = 7;
      uint64_t          target_file_size_base    = 64 << 20;
      uint64_t          max_bytes_for_level_base = 256 << 20;
      double            max_bytes_for_level_multiplier = 10;
      CompactionStyle   compaction_style         = kCompactionStyleLevel;
      bool              level_compaction_dynamic_level_bytes = false;
      int               bloom_locality           = 0;

      std::shared_ptr<TableFactory> table_factory;
   };

   struct DBOptions
   {
      bool                        create_if_missing               = false;
      bool                        create_missing_column_families  = false;
      bool                        error_if_exists                 = false;
      int                         max_open_files                  = -1;
      int                         max_background_jobs             = 2;
      int                         max_background_compactions      = -1;
      int                         max_background_flushes          = -1;
      bool                        allow_mmap_reads                = false;
      bool                        allow_mmap_writes               = false;
      bool                        use_direct_reads                = false;
      bool                        use_direct_io_for_flush_and_compaction = false;
      bool                        use_fsync                       = false;
      Env*                        env                             = Env::Default();
      std::shared_ptr<Statistics>  statistics;
      std::string                 wal_dir;
   };

   struct Options : public DBOptions, public ColumnFamilyOptions
   {
      Options() = default;
      Options(const DBOptions& db_opts, const ColumnFamilyOptions& cf_opts)
          : DBOptions(db_opts), ColumnFamilyOptions(cf_opts)
      {
      }
   };

   struct ReadOptions
   {
      const Snapshot* snapshot             = nullptr;
      bool            verify_checksums     = true;
      bool            fill_cache           = true;
      bool            total_order_seek     = false;
      bool            prefix_same_as_start = false;
      bool            tailing              = false;
      bool            async_io             = false;
      size_t          readahead_size       = 0;
      const Slice*    iterate_lower_bound  = nullptr;
      const Slice*    iterate_upper_bound  = nullptr;
   };

   struct WriteOptions
   {
      bool sync       = false;
      bool disableWAL = false;
      bool no_slowdown = false;
      bool low_pri     = false;
   };

   struct FlushOptions
   {
      bool wait                      = true;
      bool allow_write_stall         = false;
   };

   struct CompactRangeOptions
   {
      bool change_level                = false;
      int  target_level                = -1;
      bool exclusive_manual_compaction = false;
   };

   struct ColumnFamilyDescriptor
   {
      std::string          name;
      ColumnFamilyOptions  options;

      ColumnFamilyDescriptor()
          : name("default") {}
      ColumnFamilyDescriptor(const std::string& n, const ColumnFamilyOptions& opts)
          : name(n), options(opts) {}
   };

}  // namespace rocksdb
