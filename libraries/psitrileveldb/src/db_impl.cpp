// psitrileveldb: LevelDB-compatible API backed by psitri direct API.
//
// Direct API only — no DWAL layer. Writes go through start_write_session() +
// start_transaction(0) + commit. Reads and iterators take their own
// read_session for snapshot isolation (Core's dbwrapper_tests assert that a
// new iterator sees the database state at iterator-construction time, not
// later writes).

#include <leveldb/cache.h>
#include <leveldb/comparator.h>
#include <leveldb/db.h>
#include <leveldb/env.h>
#include <leveldb/filter_policy.h>
#include <leveldb/helpers/memenv/memenv.h>
#include <leveldb/iterator.h>
#include <leveldb/options.h>
#include <leveldb/slice.h>
#include <leveldb/status.h>
#include <leveldb/write_batch.h>

#include <psitri/database.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>

#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>

namespace leveldb {

// Report ≥ 1.16 so consumers (Bitcoin Core) enable paranoid_checks.
const int kMajorVersion = 1;
const int kMinorVersion = 23;

namespace {

constexpr uint32_t kPrimaryRoot = 0;

inline std::string_view to_sv(const Slice& s) { return {s.data(), s.size()}; }

// ── Cache / FilterPolicy / Comparator: opaque shells ──────────────────────
// Core constructs these via the factory fns, stashes them in Options, and
// `delete`s them on shutdown. It never invokes their methods. The base class
// in the header is sufficient.

// ── Env / Logger / mem env ────────────────────────────────────────────────

class DefaultEnv : public Env {};
DefaultEnv g_default_env;

// Mem env is a marker only — we represent "memory_only" mode by routing
// open() to a fresh temp directory, since psitri is mmap-backed and has no
// pure in-memory mode. The temp dir is removed on Env destruction.
class MemEnv : public Env {
 public:
    explicit MemEnv(Env* /*base*/) {
        auto base = std::filesystem::temp_directory_path();
        for (int i = 0; i < 1000; ++i) {
            auto candidate = base / ("psitrileveldb-mem-" + std::to_string(i) + "-" +
                                     std::to_string(reinterpret_cast<uintptr_t>(this)));
            if (!std::filesystem::exists(candidate)) {
                std::filesystem::create_directories(candidate);
                tmp_root_ = std::move(candidate);
                return;
            }
        }
        // fall back: best effort
    }
    ~MemEnv() override {
        if (!tmp_root_.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(tmp_root_, ec);
        }
    }
    const std::filesystem::path& tmp_root() const { return tmp_root_; }
 private:
    std::filesystem::path tmp_root_;
};

// ── Iterator ──────────────────────────────────────────────────────────────

class PsitriIterator : public Iterator {
 public:
    PsitriIterator(std::shared_ptr<psitri::read_session> rs,
                   psitri::tree                          root)
        : rs_(std::move(rs)),
          tree_(std::move(root)),
          cursor_(tree_.copy_root())
    {
        cursor_.seek_end();  // start invalid; matches LevelDB's "must Seek* before reading"
    }

    bool Valid() const override { return !cursor_.is_end() && !cursor_.is_rend(); }

    void SeekToFirst() override { cursor_.seek_begin(); }
    void SeekToLast()  override { cursor_.seek_last();  }
    void Seek(const Slice& target) override { cursor_.lower_bound(to_sv(target)); }
    void Next() override { cursor_.next(); }
    void Prev() override { cursor_.prev(); }

    Slice key() const override {
        auto k = cursor_.key();
        return Slice(k.data(), k.size());
    }

    Slice value() const override {
        cursor_.get_value([this](psitri::value_view vv) {
            cached_value_.assign(vv.data(), vv.size());
        });
        return Slice(cached_value_.data(), cached_value_.size());
    }

    Status status() const override { return Status::OK(); }

 private:
    std::shared_ptr<psitri::read_session> rs_;
    psitri::tree                          tree_;
    mutable psitri::cursor                cursor_;
    mutable std::string                   cached_value_;
};

// ── DB ────────────────────────────────────────────────────────────────────

class PsitriDB : public DB {
 public:
    PsitriDB(std::shared_ptr<psitri::database> db, std::filesystem::path path)
        : db_(std::move(db)), path_(std::move(path)) {}

    ~PsitriDB() override = default;

    // ── Writes ──
    Status Put(const WriteOptions& opt, const Slice& key, const Slice& value) override {
        WriteBatch b;
        b.Put(key, value);
        return Write(opt, &b);
    }

    Status Delete(const WriteOptions& opt, const Slice& key) override {
        WriteBatch b;
        b.Delete(key);
        return Write(opt, &b);
    }

    Status Write(const WriteOptions& /*opt*/, WriteBatch* updates) override {
        // psitri sync mode is configured database-wide; WriteOptions::sync is
        // ignored. Bitcoin Core's chainstate flush still gets durability via
        // tx.commit() under the runtime config in effect.
        if (!updates || updates->Empty()) return Status::OK();
        try {
            auto& ws = write_session_for_thread();
            auto tx = ws->start_transaction(kPrimaryRoot);
            size_t n = 0;
            updates->ForEach(
                [&](const Slice& k, const Slice& v) {
                    tx.upsert(to_sv(k), to_sv(v));
                    ++n;
                },
                [&](const Slice& k) {
                    tx.remove(to_sv(k));
                    ++n;
                });
            tx.commit();
            // Distribution of batch sizes — see record_batch_size for why.
            record_batch_size(n);
            return Status::OK();
        } catch (const std::exception& e) {
            return Status::IOError(e.what());
        }
    }

    // ── Reads ──
    Status Get(const ReadOptions& /*opt*/, const Slice& key, std::string* value) override {
        try {
            auto& rs = read_session_for_thread();
            auto t = rs->get_root(kPrimaryRoot);
            psitri::cursor c(t);
            std::string buf;
            int32_t n = c.get(to_sv(key), &buf);
            if (n < 0) return Status::NotFound(key);
            if (value) *value = std::move(buf);
            return Status::OK();
        } catch (const std::exception& e) {
            return Status::IOError(e.what());
        }
    }

    Iterator* NewIterator(const ReadOptions& /*opt*/) override {
        // Snapshot at iterator-creation time via the calling thread's
        // cached read_session.
        auto& rs = read_session_for_thread();
        auto t = rs->get_root(kPrimaryRoot);
        return new PsitriIterator(rs, std::move(t));
    }

    // ── Snapshots: declared but unused by Core (returns nullptr) ──
    const Snapshot* GetSnapshot() override { return nullptr; }
    void ReleaseSnapshot(const Snapshot*) override {}

    // ── Properties ──
    bool GetProperty(const Slice& property, std::string* value) override {
        if (property == Slice("leveldb.approximate-memory-usage")) {
            // Core parses this as size_t. Best-effort: report 0; psitri's
            // memory accounting lives in get_stats() but doesn't map cleanly
            // to LevelDB's "memtable + cache" notion.
            if (value) *value = "0";
            return true;
        }
        if (value) value->clear();
        return false;
    }

    void GetApproximateSizes(const Range* /*range*/, int n, uint64_t* sizes) override {
        // Core only uses this for diagnostic logging in some paths; Bitcoin
        // Core's CDBWrapper::EstimateSizeImpl forwards but doesn't gate on
        // the result. Zero is acceptable.
        for (int i = 0; i < n; ++i) sizes[i] = 0;
    }

    void CompactRange(const Slice* /*begin*/, const Slice* /*end*/) override {
        // No-op: psitri's compactor runs in the background.
    }

 private:
    // Bucketed histogram of WriteBatch sizes seen by Write(). Buckets are
    // log2-ish: [1, 2..3, 4..7, 8..15, 16..31, ..., 1M..2M-1, 2M+]. Printed
    // every PSITRILEVELDB_HIST_INTERVAL writes so we can see the distribution
    // without a flood of per-call logs.
    static constexpr int kHistBuckets = 24;  // up to ~16M
    void record_batch_size(size_t n) {
        std::lock_guard<std::mutex> lock(hist_mutex_);
        int bucket = 0;
        size_t v = n;
        while (v > 1 && bucket < kHistBuckets - 1) { v >>= 1; ++bucket; }
        ++hist_[bucket];
        ++hist_total_;
        hist_sum_ += n;
        if (hist_total_ % 100000 == 0) dump_hist_locked();
    }
    void dump_hist_locked() {
        // Print non-empty buckets only, range [2^b, 2^(b+1)-1], plus mean.
        std::string s = "[psitrileveldb] writes=" + std::to_string(hist_total_)
                      + " avg_ops=" + std::to_string(hist_total_ ? hist_sum_/hist_total_ : 0)
                      + " hist:";
        for (int i = 0; i < kHistBuckets; ++i) {
            if (hist_[i] == 0) continue;
            size_t lo = (i == 0) ? 1 : (size_t{1} << i);
            s += " " + std::to_string(lo) + "+:" + std::to_string(hist_[i]);
        }
        std::fprintf(stderr, "%s\n", s.c_str());
    }
    std::mutex hist_mutex_;
    uint64_t   hist_[kHistBuckets] = {};
    uint64_t   hist_total_ = 0;
    uint64_t   hist_sum_ = 0;

    // Sessions are thread-affine: a session created on thread T must be
    // destroyed on thread T (psitri's allocator stores the session in TLS
    // and asserts on cross-thread end_session).
    //
    // We previously cached sessions in a per-DB map. That map's entries
    // were destroyed on the DB-destroying thread, not the session-creating
    // threads — tripping the SAL assertion under bitcoind's worker pool.
    //
    // Fix: hold the per-thread session in a thread_local map keyed by
    // PsitriDB*. The thread_local destructor runs on the OWNING thread at
    // thread exit, so each session destructs where it was created. The
    // tradeoff is that a thread holding a session keeps the database
    // alive — for replay/long-lived processes this is fine; only matters
    // if the same path needs to be re-opened mid-process.
    static auto& tls_read_sessions() {
        thread_local std::unordered_map<
            PsitriDB*, std::shared_ptr<psitri::read_session>> m;
        return m;
    }
    static auto& tls_write_sessions() {
        thread_local std::unordered_map<
            PsitriDB*, std::shared_ptr<psitri::write_session>> m;
        return m;
    }

    std::shared_ptr<psitri::read_session>& read_session_for_thread() {
        auto& m = tls_read_sessions();
        auto& rs = m[this];
        if (!rs) rs = db_->start_read_session();
        return rs;
    }

    std::shared_ptr<psitri::write_session>& write_session_for_thread() {
        auto& m = tls_write_sessions();
        auto& ws = m[this];
        if (!ws) ws = db_->start_write_session();
        return ws;
    }

    std::shared_ptr<psitri::database>  db_;
    std::filesystem::path              path_;
};

}  // namespace

// ── Factory functions ─────────────────────────────────────────────────────

Env* Env::Default() { return &g_default_env; }

Cache* NewLRUCache(size_t /*capacity*/) { return new Cache(); }
const FilterPolicy* NewBloomFilterPolicy(int /*bits_per_key*/) { return new FilterPolicy(); }
const Comparator* BytewiseComparator() { return nullptr; }
Env* NewMemEnv(Env* base) { return new MemEnv(base); }

// ── DB::Open / DestroyDB / RepairDB ───────────────────────────────────────

Status DB::Open(const Options& options, const std::string& name, DB** dbptr) {
    *dbptr = nullptr;
    try {
        std::filesystem::path path;
        if (auto* mem = dynamic_cast<MemEnv*>(options.env)) {
            path = mem->tmp_root() / "db";
            std::filesystem::create_directories(path);
        } else {
            path = name;
            std::filesystem::create_directories(path);
        }

        psitri::open_mode mode;
        if (options.error_if_exists) {
            mode = psitri::open_mode::create_only;
        } else if (options.create_if_missing) {
            mode = psitri::open_mode::create_or_open;
        } else {
            mode = psitri::open_mode::open_existing;
        }

        auto db = psitri::database::open(path, mode);

        // LevelDB convention: callers (e.g. Bitcoin Core's dbwrapper_tests)
        // expect a LOCK file at the DB root. psitri uses its own locking,
        // but a sentinel file keeps API-compat happy.
        std::ofstream{path / "LOCK"}.flush();

        *dbptr = new PsitriDB(std::move(db), std::move(path));
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError(name, e.what());
    }
}

Status DestroyDB(const std::string& name, const Options& /*options*/) {
    try {
        std::error_code ec;
        std::filesystem::remove_all(name, ec);
        if (ec) return Status::IOError(name, ec.message());
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError(name, e.what());
    }
}

Status RepairDB(const std::string& /*name*/, const Options& /*options*/) {
    return Status::NotSupported("RepairDB not implemented");
}

}  // namespace leveldb
