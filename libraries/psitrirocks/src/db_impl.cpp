#include <rocksdb/db.h>

#include <psitri/database.hpp>
#include <sal/read_lock.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/transaction.hpp>

#include <filesystem>
#include <mutex>
#include <sstream>

namespace rocksdb
{

   // ── Internal types ──────────────────────────────────────────────────────

   namespace
   {

      // Snapshot backed by a psitri read_session + root snapshot
      class PsiTriSnapshot : public Snapshot
      {
        public:
         PsiTriSnapshot(std::shared_ptr<psitri::read_session> rs, uint32_t root_index)
             : rs_(std::move(rs)), root_(rs_->get_root(root_index)), seq_(next_seq())
         {
         }

         SequenceNumber GetSequenceNumber() const override { return seq_; }

         sal::smart_ptr<sal::alloc_header> root() const { return root_; }

        private:
         static SequenceNumber next_seq()
         {
            static std::atomic<SequenceNumber> counter{0};
            return counter.fetch_add(1);
         }

         std::shared_ptr<psitri::read_session> rs_;
         sal::smart_ptr<sal::alloc_header>     root_;
         SequenceNumber                        seq_;
      };

      // Column family handle — maps to a psitri root index
      class PsiTriCFHandle : public ColumnFamilyHandle
      {
        public:
         PsiTriCFHandle(uint32_t id, std::string name)
             : id_(id), name_(std::move(name)) {}

         const std::string& GetName() const override { return name_; }
         uint32_t           GetID() const override { return id_; }
         const Comparator*  GetComparator() const override { return BytewiseComparator(); }

        private:
         uint32_t    id_;
         std::string name_;
      };

      // Iterator backed by psitri::cursor
      class PsiTriIterator : public Iterator
      {
        public:
         PsiTriIterator(sal::smart_ptr<sal::alloc_header> root)
             : cursor_(std::move(root))
         {
            cursor_.seek_end();
         }

         bool Valid() const override { return !cursor_.is_end() && !cursor_.is_rend(); }

         void SeekToFirst() override { cursor_.seek_begin(); }

         void SeekToLast() override { cursor_.seek_last(); }

         void Seek(const Slice& target) override
         {
            cursor_.lower_bound(std::string_view(target.data(), target.size()));
         }

         void SeekForPrev(const Slice& target) override
         {
            std::string_view sv(target.data(), target.size());
            if (cursor_.lower_bound(sv))
            {
               if (cursor_.key() != sv)
                  cursor_.prev();
            }
            else
            {
               cursor_.seek_last();
            }
         }

         void Next() override { cursor_.next(); }
         void Prev() override { cursor_.prev(); }

         Slice key() const override
         {
            auto k = cursor_.key();
            return Slice(k.data(), k.size());
         }

         Slice value() const override
         {
            cursor_.get_value([this](psitri::value_view vv) {
               cached_value_.assign(vv.data(), vv.size());
            });
            return Slice(cached_value_);
         }

         Status status() const override { return Status::OK(); }

        private:
         mutable psitri::cursor cursor_;
         mutable std::string    cached_value_;
      };

   }  // anonymous namespace

   // ── DB implementation ───────────────────────────────────────────────────

   class PsiTriDB : public DB
   {
     public:
      PsiTriDB(std::shared_ptr<psitri::database> db, const Options& opts)
          : db_(std::move(db)),
            default_cf_(std::make_unique<PsiTriCFHandle>(0, "default")),
            sync_(opts.use_fsync ? sal::sync_type::fsync : sal::sync_type::none)
      {
      }

      ~PsiTriDB() override { Close(); }

      // ── Thread-local session management ──
      //
      // psitri sessions are per-thread: each thread gets its own segment for
      // allocation and its own read lock slot.  No external mutex needed —
      // psitri's per-root modify_lock serializes writers internally.

      struct ThreadState
      {
         std::shared_ptr<psitri::write_session> ws;
         std::shared_ptr<psitri::read_session>  rs;
      };

      ThreadState& get_thread_state()
      {
         thread_local std::unordered_map<PsiTriDB*, ThreadState> states;
         auto& s = states[this];
         if (!s.ws)
         {
            s.ws = db_->start_write_session();
            s.ws->set_sync(sync_);
         }
         return s;
      }

      std::shared_ptr<psitri::read_session>& get_read_session()
      {
         auto& ts = get_thread_state();
         if (!ts.rs)
            ts.rs = db_->start_read_session();
         return ts.rs;
      }

      // ── Write path ──
      // Each Put/Delete starts a transaction and commits immediately, matching
      // RocksDB semantics where each write is durable and visible on return.

      Status Put(const WriteOptions&, const Slice& key, const Slice& value) override
      {
         auto& ts = get_thread_state();
         auto  tx = ts.ws->start_transaction(0);
         tx.upsert(std::string_view(key.data(), key.size()),
                   std::string_view(value.data(), value.size()));
         tx.commit();
         return Status::OK();
      }

      Status Put(const WriteOptions&, ColumnFamilyHandle* cf,
                 const Slice& key, const Slice& value) override
      {
         auto& ts = get_thread_state();
         auto  tx = ts.ws->start_transaction(cf ? cf->GetID() : 0);
         tx.upsert(std::string_view(key.data(), key.size()),
                   std::string_view(value.data(), value.size()));
         tx.commit();
         return Status::OK();
      }

      Status Delete(const WriteOptions&, const Slice& key) override
      {
         auto& ts = get_thread_state();
         auto  tx = ts.ws->start_transaction(0);
         tx.remove(std::string_view(key.data(), key.size()));
         tx.commit();
         return Status::OK();
      }

      Status Delete(const WriteOptions&, ColumnFamilyHandle* cf, const Slice& key) override
      {
         auto& ts = get_thread_state();
         auto  tx = ts.ws->start_transaction(cf ? cf->GetID() : 0);
         tx.remove(std::string_view(key.data(), key.size()));
         tx.commit();
         return Status::OK();
      }

      Status DeleteRange(const WriteOptions&, ColumnFamilyHandle* cf,
                         const Slice& begin_key, const Slice& end_key) override
      {
         auto& ts = get_thread_state();
         auto  tx = ts.ws->start_transaction(cf ? cf->GetID() : 0);
         tx.remove_range(std::string_view(begin_key.data(), begin_key.size()),
                         std::string_view(end_key.data(), end_key.size()));
         tx.commit();
         return Status::OK();
      }

      Status Write(const WriteOptions&, WriteBatch* batch) override
      {
         if (!batch || batch->Count() == 0)
            return Status::OK();

         auto& ts = get_thread_state();
         auto  tx = ts.ws->start_transaction(0);

         for (auto& entry : batch->GetEntries())
         {
            switch (entry.op)
            {
               case WriteBatch::Op::kPut:
                  tx.upsert(entry.key, entry.value);
                  break;
               case WriteBatch::Op::kDelete:
                  tx.remove(entry.key);
                  break;
               case WriteBatch::Op::kDeleteRange:
                  tx.remove_range(entry.key, entry.value);
                  break;
            }
         }

         tx.commit();
         return Status::OK();
      }

      // ── Read path ──

      Status Get(const ReadOptions& options, const Slice& key, std::string* value) override
      {
         return GetImpl(options, 0, key, value);
      }

      Status Get(const ReadOptions& options, ColumnFamilyHandle* cf,
                 const Slice& key, std::string* value) override
      {
         return GetImpl(options, cf ? cf->GetID() : 0, key, value);
      }

      Status Get(const ReadOptions& options, ColumnFamilyHandle* cf,
                 const Slice& key, PinnableSlice* value) override
      {
         std::string buf;
         Status      s = GetImpl(options, cf ? cf->GetID() : 0, key, &buf);
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
            statuses.push_back(GetImpl(options, 0, keys[i], &(*values)[i]));
         return statuses;
      }

      // ── Iterator ──

      Iterator* NewIterator(const ReadOptions& options) override
      {
         return NewIteratorImpl(options, 0);
      }

      Iterator* NewIterator(const ReadOptions& options, ColumnFamilyHandle* cf) override
      {
         return NewIteratorImpl(options, cf ? cf->GetID() : 0);
      }

      // ── Snapshots ──

      const Snapshot* GetSnapshot() override
      {
         return new PsiTriSnapshot(get_read_session(), 0);
      }

      void ReleaseSnapshot(const Snapshot* snap) override
      {
         delete static_cast<const PsiTriSnapshot*>(snap);
      }

      // ── Column families ──

      ColumnFamilyHandle* DefaultColumnFamily() const override { return default_cf_.get(); }

      Status CreateColumnFamily(const ColumnFamilyOptions&,
                                const std::string& name,
                                ColumnFamilyHandle** handle) override
      {
         uint32_t id  = next_cf_id_.fetch_add(1);
         *handle      = new PsiTriCFHandle(id, name);
         return Status::OK();
      }

      // ── Maintenance ──

      Status Flush(const FlushOptions&) override { return Status::OK(); }

      Status Flush(const FlushOptions&, ColumnFamilyHandle*) override { return Status::OK(); }

      Status CompactRange(const CompactRangeOptions&, const Slice*, const Slice*) override
      {
         return Status::OK();
      }

      bool GetProperty(const Slice& property, std::string* value) override
      {
         if (property == "psitri.stats")
         {
            if (value)
            {
               db_->wait_for_compactor();
               std::ostringstream os;

               // Tree-level stats: walk the actual live trie
               {
                  auto ws = db_->start_write_session();
                  auto trx = ws->start_transaction(0);
                  auto ts = trx.get_stats();
                  uint64_t total_tree_bytes = ts.total_inner_node_size + ts.total_leaf_size + ts.total_value_size;
               os << "tree: " << ts.total_keys << " keys  "
                     << (total_tree_bytes / (1024.0 * 1024.0)) << " MB"
                     << " (inner: " << (ts.total_inner_node_size / 1024.0) << " KB"
                     << "  leaf: " << (ts.total_leaf_size / (1024.0 * 1024.0)) << " MB"
                     << "  value: " << (ts.total_value_size / (1024.0 * 1024.0)) << " MB)"
                     << "  depth: " << ts.max_depth;
               }

               // Segment-level stats
               auto d = db_->dump();
               uint64_t total_freed = 0;
               uint64_t total_allocated = 0;
               uint32_t free_segs = 0;
               for (auto& seg : d.segments)
               {
                  total_freed += seg.freed_bytes;
                  total_allocated += seg.alloc_pos > 0 ? seg.alloc_pos : 0;
                  if (seg.is_free) ++free_segs;
               }
               os << "\nsegs: " << d.total_segments << " (" << free_segs << " free)"
                  << "  allocated: " << (total_allocated / (1024.0 * 1024.0)) << " MB"
                  << "  freed: " << (total_freed / (1024.0 * 1024.0)) << " MB"
                  << "  retained: " << d.total_retained
                  << "  sessions: " << d.sessions.size();

               // Walk all live objects via control block allocator
               {
                  auto ws = db_->start_write_session();
                  uint64_t live_inner = 0, live_leaf = 0, live_value = 0, live_other = 0;
                  uint64_t bytes_inner = 0, bytes_leaf = 0, bytes_value = 0, bytes_other = 0;
                  uint64_t high_ref = 0;
                  ws->for_each_live_object(
                      [&](sal::ptr_address, uint32_t ref, const sal::alloc_header* obj)
                      {
                         uint64_t sz = obj->size();
                         switch ((int)obj->type())
                         {
                            case (int)psitri::node_type::inner:
                            case (int)psitri::node_type::inner_prefix:
                               ++live_inner; bytes_inner += sz; break;
                            case (int)psitri::node_type::leaf:
                               ++live_leaf; bytes_leaf += sz; break;
                            case (int)psitri::node_type::value:
                               ++live_value; bytes_value += sz; break;
                            default:
                               ++live_other; bytes_other += sz; break;
                         }
                         if (ref > 1) ++high_ref;
                      });
                  uint64_t total_live = bytes_inner + bytes_leaf + bytes_value + bytes_other;
                  os << "\nlive objects: "
                     << (total_live / (1024.0 * 1024.0)) << " MB"
                     << " (inner: " << live_inner << "/" << (bytes_inner / 1024.0) << " KB"
                     << "  leaf: " << live_leaf << "/" << (bytes_leaf / (1024.0 * 1024.0)) << " MB"
                     << "  value: " << live_value << "/" << (bytes_value / (1024.0 * 1024.0)) << " MB"
                     << "  other: " << live_other << "/" << (bytes_other / (1024.0 * 1024.0)) << " MB)"
                     << "  ref>1: " << high_ref;
               }

               // Pretty-printed segment dump
               os << "\n";
               d.print(os);

               *value = os.str();
            }
            return true;
         }
         if (property == "psitri.compact_and_truncate")
         {
            auto before = db_->dump().total_segments;
            db_->compact_and_truncate();
            auto after_dump = db_->dump();
            if (value)
            {
               std::ostringstream os;
               os << "truncated: " << before << " -> " << after_dump.total_segments << " segments";
               *value = os.str();
            }
            return true;
         }
         // RocksDB-compatible size properties
         if (property == "rocksdb.total-sst-files-size" ||
             property == "rocksdb.live-sst-files-size" ||
             property == "rocksdb.estimate-pending-compaction-bytes")
         {
            auto d = db_->dump();
            if (value)
            {
               if (property == "rocksdb.total-sst-files-size")
               {
                  *value = std::to_string(d.total_segments * sal::segment_size);
               }
               else if (property == "rocksdb.live-sst-files-size")
               {
                  // Live = total allocated minus freed (internal fragmentation)
                  uint64_t live = 0;
                  for (auto& seg : d.segments)
                     if (!seg.is_free && seg.alloc_pos > 0)
                        live += seg.alloc_pos - seg.freed_bytes;
                  *value = std::to_string(live);
               }
               else  // estimate-pending-compaction-bytes
               {
                  *value = std::to_string(d.total_free_space);
               }
            }
            return true;
         }
         if (value) *value = "";
         return false;
      }

      bool GetProperty(ColumnFamilyHandle*, const Slice& property, std::string* value) override
      {
         return GetProperty(property, value);
      }

      Status Close() override { return Status::OK(); }

     private:
      Status GetImpl(const ReadOptions& options, uint32_t root_idx,
                     const Slice& key, std::string* value)
      {
         sal::smart_ptr<sal::alloc_header> root;
         if (options.snapshot)
         {
            auto* snap = static_cast<const PsiTriSnapshot*>(options.snapshot);
            root       = snap->root();
         }
         else
         {
            root = get_read_session()->get_root(root_idx);
         }

         if (!root)
            return Status::NotFound();

         psitri::cursor c(std::move(root));
         auto           result = c.get(std::string_view(key.data(), key.size()), value);
         if (result < 0)
            return Status::NotFound();
         return Status::OK();
      }

      Iterator* NewIteratorImpl(const ReadOptions& options, uint32_t root_idx)
      {
         sal::smart_ptr<sal::alloc_header> root;
         if (options.snapshot)
         {
            auto* snap = static_cast<const PsiTriSnapshot*>(options.snapshot);
            root       = snap->root();
         }
         else
         {
            root = get_read_session()->get_root(root_idx);
         }

         if (!root)
            root = get_read_session()->get_root(root_idx);

         return new PsiTriIterator(std::move(root));
      }

      std::shared_ptr<psitri::database> db_;
      std::unique_ptr<PsiTriCFHandle>   default_cf_;
      sal::sync_type                    sync_;
      std::atomic<uint32_t>             next_cf_id_{1};
   };

   // ── Static methods ──────────────────────────────────────────────────────

   Status DB::Open(const Options& options, const std::string& name, DB** dbptr)
   {
      try
      {
         auto path = std::filesystem::path(name);
         if (options.create_if_missing)
         {
            std::filesystem::create_directories(path);
         }
         else if (!std::filesystem::exists(path))
         {
            return Status::IOError("Directory does not exist: " + name);
         }

         if (options.error_if_exists && std::filesystem::exists(path) &&
             !std::filesystem::is_empty(path))
         {
            return Status::InvalidArgument("Database already exists: " + name);
         }

         auto db = psitri::database::create(path);
         *dbptr  = new PsiTriDB(std::move(db), options);
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
      for (size_t i = 0; i < column_families.size(); i++)
      {
         handles->push_back(
             new PsiTriCFHandle(static_cast<uint32_t>(i), column_families[i].name));
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
