#include <rocksdb/db.h>

#include <mdbx.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <mutex>

namespace rocksdb
{

   // ── Internal types ──────────────────────────────────────────────────────

   namespace
   {

      // Snapshot backed by a long-lived read-only MDBX transaction
      class MdbxSnapshot : public Snapshot
      {
        public:
         MdbxSnapshot(MDBX_env* env) : seq_(next_seq())
         {
            mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn_);
         }

         ~MdbxSnapshot()
         {
            if (txn_)
               mdbx_txn_abort(txn_);
         }

         SequenceNumber GetSequenceNumber() const override { return seq_; }

         MDBX_txn* txn() const { return txn_; }

        private:
         static SequenceNumber next_seq()
         {
            static std::atomic<SequenceNumber> counter{0};
            return counter.fetch_add(1);
         }

         MDBX_txn*      txn_;
         SequenceNumber  seq_;
      };

      // Column family handle — maps to an MDBX DBI
      class MdbxCFHandle : public ColumnFamilyHandle
      {
        public:
         MdbxCFHandle(uint32_t id, std::string name, MDBX_dbi dbi)
             : id_(id), name_(std::move(name)), dbi_(dbi) {}

         const std::string& GetName() const override { return name_; }
         uint32_t           GetID() const override { return id_; }
         const Comparator*  GetComparator() const override { return BytewiseComparator(); }

         MDBX_dbi dbi() const { return dbi_; }

        private:
         uint32_t    id_;
         std::string name_;
         MDBX_dbi    dbi_;
      };

      // Iterator backed by an MDBX cursor
      class MdbxIterator : public Iterator
      {
        public:
         MdbxIterator(MDBX_env* env, MDBX_dbi dbi, MDBX_txn* snap_txn)
         {
            if (snap_txn)
            {
               // Use the snapshot transaction — don't own it
               txn_     = snap_txn;
               own_txn_ = false;
            }
            else
            {
               mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn_);
               own_txn_ = true;
            }
            mdbx_cursor_open(txn_, dbi, &cursor_);
            valid_ = false;
         }

         ~MdbxIterator()
         {
            if (cursor_)
               mdbx_cursor_close(cursor_);
            if (own_txn_ && txn_)
               mdbx_txn_abort(txn_);
         }

         bool Valid() const override { return valid_; }

         void SeekToFirst() override
         {
            MDBX_val k, v;
            valid_ = (mdbx_cursor_get(cursor_, &k, &v, MDBX_FIRST) == MDBX_SUCCESS);
            if (valid_)
               cache(k, v);
         }

         void SeekToLast() override
         {
            MDBX_val k, v;
            valid_ = (mdbx_cursor_get(cursor_, &k, &v, MDBX_LAST) == MDBX_SUCCESS);
            if (valid_)
               cache(k, v);
         }

         void Seek(const Slice& target) override
         {
            MDBX_val k = {const_cast<char*>(target.data()), target.size()};
            MDBX_val v;
            valid_ = (mdbx_cursor_get(cursor_, &k, &v, MDBX_SET_RANGE) == MDBX_SUCCESS);
            if (valid_)
               cache(k, v);
         }

         void SeekForPrev(const Slice& target) override
         {
            MDBX_val k = {const_cast<char*>(target.data()), target.size()};
            MDBX_val v;
            int rc = mdbx_cursor_get(cursor_, &k, &v, MDBX_SET_RANGE);
            if (rc == MDBX_SUCCESS)
            {
               // If we landed exactly on target, done
               if (k.iov_len == target.size() &&
                   std::memcmp(k.iov_base, target.data(), target.size()) == 0)
               {
                  valid_ = true;
                  cache(k, v);
               }
               else
               {
                  // Landed past target, go back one
                  rc = mdbx_cursor_get(cursor_, &k, &v, MDBX_PREV);
                  valid_ = (rc == MDBX_SUCCESS);
                  if (valid_)
                     cache(k, v);
               }
            }
            else
            {
               // Past end — seek to last
               SeekToLast();
            }
         }

         void Next() override
         {
            MDBX_val k, v;
            valid_ = (mdbx_cursor_get(cursor_, &k, &v, MDBX_NEXT) == MDBX_SUCCESS);
            if (valid_)
               cache(k, v);
         }

         void Prev() override
         {
            MDBX_val k, v;
            valid_ = (mdbx_cursor_get(cursor_, &k, &v, MDBX_PREV) == MDBX_SUCCESS);
            if (valid_)
               cache(k, v);
         }

         Slice key() const override { return Slice(cached_key_); }

         Slice value() const override { return Slice(cached_value_); }

         Status status() const override { return Status::OK(); }

        private:
         void cache(const MDBX_val& k, const MDBX_val& v)
         {
            cached_key_.assign(static_cast<const char*>(k.iov_base), k.iov_len);
            cached_value_.assign(static_cast<const char*>(v.iov_base), v.iov_len);
         }

         MDBX_txn*    txn_     = nullptr;
         MDBX_cursor* cursor_  = nullptr;
         bool         own_txn_ = false;
         bool         valid_   = false;
         std::string  cached_key_;
         std::string  cached_value_;
      };

   }  // anonymous namespace

   // ── DB implementation ───────────────────────────────────────────────────

   class MdbxDB : public DB
   {
     public:
      MdbxDB(MDBX_env* env, MDBX_dbi dbi, bool use_sync)
          : env_(env), dbi_(dbi),
            default_cf_(std::make_unique<MdbxCFHandle>(0, "default", dbi)),
            use_sync_(use_sync)
      {
      }

      ~MdbxDB() override { Close(); }

      // ── Write path ──
      // MDBX is single-writer (global write lock), so each Put/Delete
      // starts a write txn and commits. Same constraint as the RocksDB API.

      Status Put(const WriteOptions&, const Slice& key, const Slice& value) override
      {
         return PutImpl(dbi_, key, value);
      }

      Status Put(const WriteOptions&, ColumnFamilyHandle* cf,
                 const Slice& key, const Slice& value) override
      {
         return PutImpl(get_dbi(cf), key, value);
      }

      Status Delete(const WriteOptions&, const Slice& key) override
      {
         return DeleteImpl(dbi_, key);
      }

      Status Delete(const WriteOptions&, ColumnFamilyHandle* cf, const Slice& key) override
      {
         return DeleteImpl(get_dbi(cf), key);
      }

      Status DeleteRange(const WriteOptions&, ColumnFamilyHandle* cf,
                         const Slice& begin_key, const Slice& end_key) override
      {
         MDBX_txn* txn;
         int rc = mdbx_txn_begin(env_, nullptr, MDBX_TXN_READWRITE, &txn);
         if (rc != MDBX_SUCCESS)
            return Status::IOError("txn_begin failed");

         MDBX_cursor* cursor;
         mdbx_cursor_open(txn, get_dbi(cf), &cursor);

         MDBX_val k = {const_cast<char*>(begin_key.data()), begin_key.size()};
         MDBX_val v;
         rc = mdbx_cursor_get(cursor, &k, &v, MDBX_SET_RANGE);
         while (rc == MDBX_SUCCESS)
         {
            if (k.iov_len >= end_key.size() &&
                std::memcmp(k.iov_base, end_key.data(), end_key.size()) >= 0)
               break;
            mdbx_cursor_del(cursor, MDBX_CURRENT);
            rc = mdbx_cursor_get(cursor, &k, &v, MDBX_NEXT);
         }

         mdbx_cursor_close(cursor);
         mdbx_txn_commit(txn);
         return Status::OK();
      }

      Status Write(const WriteOptions&, WriteBatch* batch) override
      {
         if (!batch || batch->Count() == 0)
            return Status::OK();

         MDBX_txn* txn;
         int rc = mdbx_txn_begin(env_, nullptr, MDBX_TXN_READWRITE, &txn);
         if (rc != MDBX_SUCCESS)
            return Status::IOError("txn_begin failed");

         for (auto& entry : batch->GetEntries())
         {
            MDBX_val k = {const_cast<char*>(entry.key.data()), entry.key.size()};
            switch (entry.op)
            {
               case WriteBatch::Op::kPut:
               {
                  MDBX_val v = {const_cast<char*>(entry.value.data()), entry.value.size()};
                  mdbx_put(txn, dbi_, &k, &v, MDBX_UPSERT);
                  break;
               }
               case WriteBatch::Op::kDelete:
                  mdbx_del(txn, dbi_, &k, nullptr);
                  break;
               case WriteBatch::Op::kDeleteRange:
                  // entry.value is the end key for range deletes
                  break;
            }
         }

         mdbx_txn_commit(txn);
         return Status::OK();
      }

      // ── Read path ──

      Status Get(const ReadOptions& options, const Slice& key, std::string* value) override
      {
         return GetImpl(options, dbi_, key, value);
      }

      Status Get(const ReadOptions& options, ColumnFamilyHandle* cf,
                 const Slice& key, std::string* value) override
      {
         return GetImpl(options, get_dbi(cf), key, value);
      }

      Status Get(const ReadOptions& options, ColumnFamilyHandle* cf,
                 const Slice& key, PinnableSlice* value) override
      {
         std::string buf;
         Status      s = GetImpl(options, get_dbi(cf), key, &buf);
         if (s.ok())
            value->PinSelf(std::move(buf));
         return s;
      }

      std::vector<Status> MultiGet(const ReadOptions& options,
                                   const std::vector<Slice>& keys,
                                   std::vector<std::string>* values) override
      {
         std::vector<Status> statuses;
         statuses.reserve(keys.size());
         values->resize(keys.size());
         for (size_t i = 0; i < keys.size(); i++)
            statuses.push_back(GetImpl(options, dbi_, keys[i], &(*values)[i]));
         return statuses;
      }

      // ── Iterator ──

      Iterator* NewIterator(const ReadOptions& options) override
      {
         return NewIteratorImpl(options, dbi_);
      }

      Iterator* NewIterator(const ReadOptions& options, ColumnFamilyHandle* cf) override
      {
         return NewIteratorImpl(options, get_dbi(cf));
      }

      // ── Snapshots ──

      const Snapshot* GetSnapshot() override
      {
         return new MdbxSnapshot(env_);
      }

      void ReleaseSnapshot(const Snapshot* snap) override
      {
         delete static_cast<const MdbxSnapshot*>(snap);
      }

      // ── Column families ──

      ColumnFamilyHandle* DefaultColumnFamily() const override { return default_cf_.get(); }

      Status CreateColumnFamily(const ColumnFamilyOptions&,
                                const std::string& name,
                                ColumnFamilyHandle** handle) override
      {
         MDBX_txn* txn;
         int rc = mdbx_txn_begin(env_, nullptr, MDBX_TXN_READWRITE, &txn);
         if (rc != MDBX_SUCCESS)
            return Status::IOError("txn_begin failed");

         MDBX_dbi dbi;
         rc = mdbx_dbi_open(txn, name.c_str(), MDBX_CREATE, &dbi);
         mdbx_txn_commit(txn);

         if (rc != MDBX_SUCCESS)
            return Status::IOError("dbi_open failed");

         uint32_t id = next_cf_id_.fetch_add(1);
         *handle     = new MdbxCFHandle(id, name, dbi);
         return Status::OK();
      }

      // ── Maintenance ──

      Status Flush(const FlushOptions&) override
      {
         mdbx_env_sync_ex(env_, true, false);
         return Status::OK();
      }

      Status Flush(const FlushOptions& opts, ColumnFamilyHandle*) override
      {
         return Flush(opts);
      }

      Status CompactRange(const CompactRangeOptions&, const Slice*, const Slice*) override
      {
         return Status::OK();
      }

      bool GetProperty(const Slice&, std::string* value) override
      {
         if (value) *value = "";
         return false;
      }

      bool GetProperty(ColumnFamilyHandle*, const Slice& property, std::string* value) override
      {
         return GetProperty(property, value);
      }

      Status Close() override
      {
         if (env_)
         {
            mdbx_env_close(env_);
            env_ = nullptr;
         }
         return Status::OK();
      }

     private:
      MDBX_dbi get_dbi(ColumnFamilyHandle* cf) const
      {
         if (cf)
            return static_cast<MdbxCFHandle*>(cf)->dbi();
         return dbi_;
      }

      Status PutImpl(MDBX_dbi dbi, const Slice& key, const Slice& value)
      {
         MDBX_txn* txn;
         int rc = mdbx_txn_begin(env_, nullptr, MDBX_TXN_READWRITE, &txn);
         if (rc != MDBX_SUCCESS)
            return Status::IOError("txn_begin failed");

         MDBX_val k = {const_cast<char*>(key.data()), key.size()};
         MDBX_val v = {const_cast<char*>(value.data()), value.size()};
         rc = mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT);
         if (rc != MDBX_SUCCESS)
         {
            mdbx_txn_abort(txn);
            return Status::IOError("put failed");
         }

         rc = mdbx_txn_commit(txn);
         if (rc != MDBX_SUCCESS)
            return Status::IOError("commit failed");
         return Status::OK();
      }

      Status DeleteImpl(MDBX_dbi dbi, const Slice& key)
      {
         MDBX_txn* txn;
         int rc = mdbx_txn_begin(env_, nullptr, MDBX_TXN_READWRITE, &txn);
         if (rc != MDBX_SUCCESS)
            return Status::IOError("txn_begin failed");

         MDBX_val k = {const_cast<char*>(key.data()), key.size()};
         rc = mdbx_del(txn, dbi, &k, nullptr);
         // MDBX_NOTFOUND is fine — key didn't exist
         if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND)
         {
            mdbx_txn_abort(txn);
            return Status::IOError("del failed");
         }

         mdbx_txn_commit(txn);
         return Status::OK();
      }

      Status GetImpl(const ReadOptions& options, MDBX_dbi dbi,
                     const Slice& key, std::string* value)
      {
         MDBX_txn* txn;
         bool own_txn = false;

         if (options.snapshot)
         {
            txn = static_cast<const MdbxSnapshot*>(options.snapshot)->txn();
         }
         else
         {
            int rc = mdbx_txn_begin(env_, nullptr, MDBX_TXN_RDONLY, &txn);
            if (rc != MDBX_SUCCESS)
               return Status::IOError("txn_begin failed");
            own_txn = true;
         }

         MDBX_val k = {const_cast<char*>(key.data()), key.size()};
         MDBX_val v;
         int rc = mdbx_get(txn, dbi, &k, &v);

         if (own_txn)
            mdbx_txn_abort(txn);

         if (rc == MDBX_NOTFOUND)
            return Status::NotFound();
         if (rc != MDBX_SUCCESS)
            return Status::IOError("get failed");

         value->assign(static_cast<const char*>(v.iov_base), v.iov_len);
         return Status::OK();
      }

      Iterator* NewIteratorImpl(const ReadOptions& options, MDBX_dbi dbi)
      {
         MDBX_txn* snap_txn = nullptr;
         if (options.snapshot)
            snap_txn = static_cast<const MdbxSnapshot*>(options.snapshot)->txn();
         return new MdbxIterator(env_, dbi, snap_txn);
      }

      MDBX_env*                       env_;
      MDBX_dbi                        dbi_;
      std::unique_ptr<MdbxCFHandle>   default_cf_;
      bool                            use_sync_;
      std::atomic<uint32_t>           next_cf_id_{1};
   };

   // ── Static methods ──────────────────────────────────────────────────────

   Status DB::Open(const Options& options, const std::string& name, DB** dbptr)
   {
      try
      {
         auto path = std::filesystem::path(name);
         if (options.create_if_missing)
            std::filesystem::create_directories(path);

         MDBX_env* env;
         int rc = mdbx_env_create(&env);
         if (rc != MDBX_SUCCESS)
         {
            *dbptr = nullptr;
            return Status::IOError("env_create failed");
         }

         // Set map size large enough for benchmarks (64 GB)
         rc = mdbx_env_set_geometry(env,
             0,                          // size_lower
             0,                          // size_now
             64LL * 1024 * 1024 * 1024,  // size_upper (64GB)
             256LL * 1024 * 1024,        // growth_step (256MB)
             256LL * 1024 * 1024,        // shrink_threshold (256MB)
             4096);                      // pagesize
         if (rc != MDBX_SUCCESS)
            fprintf(stderr, "MDBX set_geometry: %d (%s)\n", rc, mdbx_strerror(rc));
         mdbx_env_set_maxdbs(env, 16);
         mdbx_env_set_maxreaders(env, 128);

         unsigned flags = MDBX_CREATE | MDBX_LIFORECLAIM;
         if (!options.use_fsync)
            flags |= MDBX_SAFE_NOSYNC;

         rc = mdbx_env_open(env, path.c_str(), static_cast<MDBX_env_flags_t>(flags), 0664);
         if (rc != MDBX_SUCCESS)
         {
            mdbx_env_close(env);
            *dbptr = nullptr;
            return Status::IOError("env_open failed: " + std::to_string(rc));
         }

         // Open the default unnamed database
         MDBX_txn* txn;
         rc = mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
         if (rc != MDBX_SUCCESS)
         {
            mdbx_env_close(env);
            *dbptr = nullptr;
            return Status::IOError("txn_begin failed");
         }

         MDBX_dbi dbi;
         rc = mdbx_dbi_open(txn, nullptr, MDBX_CREATE, &dbi);
         mdbx_txn_commit(txn);

         if (rc != MDBX_SUCCESS)
         {
            mdbx_env_close(env);
            *dbptr = nullptr;
            return Status::IOError("dbi_open failed");
         }

         *dbptr = new MdbxDB(env, dbi, options.use_fsync);
         return Status::OK();
      }
      catch (const std::exception& e)
      {
         *dbptr = nullptr;
         return Status::IOError(e.what());
      }
   }

   Status DB::Open(const DBOptions&                           db_options,
                   const std::string&                         name,
                   const std::vector<ColumnFamilyDescriptor>& column_families,
                   std::vector<ColumnFamilyHandle*>*          handles,
                   DB**                                       dbptr)
   {
      Options opts;
      static_cast<DBOptions&>(opts) = db_options;
      if (!column_families.empty())
         static_cast<ColumnFamilyOptions&>(opts) = column_families[0].options;

      Status s = Open(opts, name, dbptr);
      if (!s.ok())
         return s;

      handles->clear();
      auto* mdb = static_cast<MdbxDB*>(*dbptr);
      for (size_t i = 0; i < column_families.size(); i++)
      {
         handles->push_back(new MdbxCFHandle(static_cast<uint32_t>(i),
                                              column_families[i].name,
                                              0));  // default dbi
      }
      return Status::OK();
   }

   Status DB::OpenForReadOnly(const Options& options, const std::string& name,
                              DB** dbptr, bool)
   {
      return Open(options, name, dbptr);
   }

   // Default implementations for non-pure-virtual methods

   Status DB::Put(const WriteOptions& options, const Slice& key, const Slice& value)
   {
      WriteBatch batch;
      batch.Put(key, value);
      return Write(options, &batch);
   }

   Status DB::Put(const WriteOptions& options, ColumnFamilyHandle*,
                  const Slice& key, const Slice& value)
   {
      return Put(options, key, value);
   }

   Status DB::Delete(const WriteOptions& options, const Slice& key)
   {
      WriteBatch batch;
      batch.Delete(key);
      return Write(options, &batch);
   }

   Status DB::Delete(const WriteOptions& options, ColumnFamilyHandle*,
                     const Slice& key)
   {
      return Delete(options, key);
   }

   Status DB::DeleteRange(const WriteOptions&, ColumnFamilyHandle*,
                          const Slice&, const Slice&)
   {
      return Status::NotSupported("DeleteRange");
   }

   Status DB::Get(const ReadOptions& options, const Slice& key, std::string* value)
   {
      return Get(options, DefaultColumnFamily(), key, value);
   }

   Status DB::Get(const ReadOptions& options, ColumnFamilyHandle* cf,
                  const Slice& key, std::string* value)
   {
      return Status::NotSupported("Get not implemented in base");
   }

   Status DB::Get(const ReadOptions& options, ColumnFamilyHandle* cf,
                  const Slice& key, PinnableSlice* value)
   {
      std::string buf;
      Status      s = Get(options, cf, key, &buf);
      if (s.ok())
         value->PinSelf(std::move(buf));
      return s;
   }

   std::vector<Status> DB::MultiGet(const ReadOptions& options,
                                    const std::vector<Slice>& keys,
                                    std::vector<std::string>* values)
   {
      std::vector<Status> statuses;
      statuses.reserve(keys.size());
      values->resize(keys.size());
      for (size_t i = 0; i < keys.size(); i++)
         statuses.push_back(Get(options, keys[i], &(*values)[i]));
      return statuses;
   }

   Iterator* DB::NewIterator(const ReadOptions& options, ColumnFamilyHandle*)
   {
      return NewIterator(options);
   }

   Status DB::CreateColumnFamily(const ColumnFamilyOptions&, const std::string&,
                                 ColumnFamilyHandle**)
   {
      return Status::NotSupported("CreateColumnFamily");
   }

   Status DB::Flush(const FlushOptions&) { return Status::OK(); }
   Status DB::Flush(const FlushOptions&, ColumnFamilyHandle*) { return Status::OK(); }
   Status DB::CompactRange(const CompactRangeOptions&, const Slice*, const Slice*)
   {
      return Status::OK();
   }
   bool DB::GetProperty(const Slice&, std::string*) { return false; }
   bool DB::GetProperty(ColumnFamilyHandle*, const Slice&, std::string*) { return false; }

   Status DestroyDB(const std::string& name, const Options&)
   {
      try
      {
         std::filesystem::remove_all(name);
         return Status::OK();
      }
      catch (const std::exception& e)
      {
         return Status::IOError(e.what());
      }
   }

}  // namespace rocksdb
