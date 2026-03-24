#pragma once
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <string>
#include <utility>
#include <vector>

namespace rocksdb
{

   class ColumnFamilyHandle;

   class WriteBatch
   {
     public:
      WriteBatch() = default;
      explicit WriteBatch(size_t /*reserved_bytes*/, size_t /*max_bytes*/ = 0,
                          size_t /*protection_bytes_per_key*/ = 0, size_t /*ts_sz*/ = 0)
      {
      }

      Status Put(const Slice& key, const Slice& value)
      {
         ops_.push_back({Op::kPut, key.ToString(), value.ToString()});
         return Status::OK();
      }

      Status Put(ColumnFamilyHandle*, const Slice& key, const Slice& value)
      {
         return Put(key, value);
      }

      Status Delete(const Slice& key)
      {
         ops_.push_back({Op::kDelete, key.ToString(), {}});
         return Status::OK();
      }

      Status Delete(ColumnFamilyHandle*, const Slice& key) { return Delete(key); }

      Status SingleDelete(const Slice& key) { return Delete(key); }
      Status SingleDelete(ColumnFamilyHandle* cf, const Slice& key)
      {
         return Delete(cf, key);
      }

      Status DeleteRange(const Slice& begin_key, const Slice& end_key)
      {
         ops_.push_back({Op::kDeleteRange, begin_key.ToString(), end_key.ToString()});
         return Status::OK();
      }

      Status DeleteRange(ColumnFamilyHandle*, const Slice& begin_key, const Slice& end_key)
      {
         return DeleteRange(begin_key, end_key);
      }

      Status Merge(const Slice& key, const Slice& value)
      {
         // Merge not supported — treat as Put
         return Put(key, value);
      }

      Status Merge(ColumnFamilyHandle* cf, const Slice& key, const Slice& value)
      {
         return Put(cf, key, value);
      }

      void Clear() { ops_.clear(); }

      uint32_t Count() const { return static_cast<uint32_t>(ops_.size()); }

      size_t GetDataSize() const
      {
         size_t total = 0;
         for (auto& op : ops_)
            total += op.key.size() + op.value.size();
         return total;
      }

      // Internal: used by DB implementation to iterate operations
      enum class Op
      {
         kPut,
         kDelete,
         kDeleteRange,
      };

      struct Entry
      {
         Op          op;
         std::string key;
         std::string value;
      };

      const std::vector<Entry>& GetEntries() const { return ops_; }

     private:
      std::vector<Entry> ops_;
   };

}  // namespace rocksdb
