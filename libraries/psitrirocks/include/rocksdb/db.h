#pragma once
#include <memory>
#include <string>
#include <vector>

#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/snapshot.h>
#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>

namespace rocksdb
{

   class ColumnFamilyHandle
   {
     public:
      virtual ~ColumnFamilyHandle() = default;
      virtual const std::string&  GetName() const        = 0;
      virtual uint32_t            GetID() const           = 0;
      virtual const Comparator*   GetComparator() const   = 0;
   };

   struct Range
   {
      Slice start;
      Slice limit;
   };

   class DB
   {
     public:
      virtual ~DB() = default;

      // --- Open / Destroy ---

      static Status Open(const Options& options, const std::string& name, DB** dbptr);

      static Status Open(const DBOptions&                              db_options,
                         const std::string&                            name,
                         const std::vector<ColumnFamilyDescriptor>&    column_families,
                         std::vector<ColumnFamilyHandle*>*             handles,
                         DB**                                          dbptr);

      static Status OpenForReadOnly(const Options& options, const std::string& name,
                                    DB** dbptr, bool = false);

      // --- Write ---

      virtual Status Put(const WriteOptions& options, const Slice& key, const Slice& value);

      virtual Status Put(const WriteOptions& options, ColumnFamilyHandle* cf,
                         const Slice& key, const Slice& value);

      virtual Status Delete(const WriteOptions& options, const Slice& key);

      virtual Status Delete(const WriteOptions& options, ColumnFamilyHandle* cf,
                            const Slice& key);

      virtual Status SingleDelete(const WriteOptions& options, const Slice& key)
      {
         return Delete(options, key);
      }

      virtual Status SingleDelete(const WriteOptions& options, ColumnFamilyHandle* cf,
                                  const Slice& key)
      {
         return Delete(options, cf, key);
      }

      virtual Status DeleteRange(const WriteOptions& options, ColumnFamilyHandle* cf,
                                 const Slice& begin_key, const Slice& end_key);

      virtual Status Merge(const WriteOptions& options, const Slice& key, const Slice& value)
      {
         return Put(options, key, value);
      }

      virtual Status Write(const WriteOptions& options, WriteBatch* updates) = 0;

      // --- Read ---

      virtual Status Get(const ReadOptions& options, const Slice& key, std::string* value);

      virtual Status Get(const ReadOptions& options, ColumnFamilyHandle* cf,
                         const Slice& key, std::string* value);

      virtual Status Get(const ReadOptions& options, ColumnFamilyHandle* cf,
                         const Slice& key, PinnableSlice* value);

      virtual std::vector<Status> MultiGet(const ReadOptions& options,
                                           const std::vector<Slice>& keys,
                                           std::vector<std::string>* values);

      // --- Iterators ---

      virtual Iterator* NewIterator(const ReadOptions& options) = 0;

      virtual Iterator* NewIterator(const ReadOptions& options, ColumnFamilyHandle* cf);

      // --- Snapshots ---

      virtual const Snapshot* GetSnapshot()                         = 0;
      virtual void            ReleaseSnapshot(const Snapshot* snap) = 0;

      // --- Column Families ---

      virtual ColumnFamilyHandle* DefaultColumnFamily() const = 0;

      virtual Status CreateColumnFamily(const ColumnFamilyOptions& options,
                                        const std::string& column_family_name,
                                        ColumnFamilyHandle** handle);

      // --- Maintenance ---

      virtual Status Flush(const FlushOptions& options);
      virtual Status Flush(const FlushOptions& options, ColumnFamilyHandle* cf);

      virtual Status CompactRange(const CompactRangeOptions& options,
                                  const Slice* begin, const Slice* end);

      virtual bool GetProperty(const Slice& property, std::string* value);
      virtual bool GetProperty(ColumnFamilyHandle* cf, const Slice& property,
                               std::string* value);

      virtual Status Close() { return Status::OK(); }
   };

   Status DestroyDB(const std::string& name, const Options& options);

   // ManagedSnapshot: RAII wrapper for snapshots
   class ManagedSnapshot
   {
     public:
      explicit ManagedSnapshot(DB* db) : db_(db), snapshot_(db->GetSnapshot()) {}
      ~ManagedSnapshot()
      {
         if (db_ && snapshot_)
            db_->ReleaseSnapshot(snapshot_);
      }

      ManagedSnapshot(const ManagedSnapshot&)            = delete;
      ManagedSnapshot& operator=(const ManagedSnapshot&) = delete;

      const Snapshot* snapshot() const { return snapshot_; }

     private:
      DB*             db_;
      const Snapshot* snapshot_;
   };

}  // namespace rocksdb
