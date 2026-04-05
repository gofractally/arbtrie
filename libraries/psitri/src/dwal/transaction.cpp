#include <psitri/dwal/transaction.hpp>

#include <psitri/dwal/dwal_database.hpp>

#include <algorithm>
#include <cassert>
#include <set>
#include <stdexcept>

namespace psitri::dwal
{
   // ── root_handle ──────────────────────────────────────────────────

   void transaction::root_handle::upsert(std::string_view key, std::string_view value)
   {
      assert(_writable && "upsert called on read-only root");
      _inner->upsert(key, value);
   }

   void transaction::root_handle::upsert_subtree(std::string_view key, sal::ptr_address addr)
   {
      assert(_writable && "upsert_subtree called on read-only root");
      _inner->upsert_subtree(key, addr);
   }

   bool transaction::root_handle::remove(std::string_view key)
   {
      assert(_writable && "remove called on read-only root");
      return _inner->remove(key);
   }

   void transaction::root_handle::remove_range(std::string_view low, std::string_view high)
   {
      assert(_writable && "remove_range called on read-only root");
      _inner->remove_range(low, high);
   }

   dwal_transaction::lookup_result transaction::root_handle::get(std::string_view key) const
   {
      return _inner->get(key);
   }

   // ── transaction construction ─────────────────────────────────────

   transaction::transaction(dwal_database&                  db,
                            std::initializer_list<uint32_t> write_roots,
                            std::initializer_list<uint32_t> read_roots)
       : _db(&db)
   {
      init(std::vector<uint32_t>(write_roots), std::vector<uint32_t>(read_roots));
   }

   transaction::transaction(dwal_database&        db,
                            std::vector<uint32_t> write_roots,
                            std::vector<uint32_t> read_roots)
       : _db(&db)
   {
      init(std::move(write_roots), std::move(read_roots));
   }

   void transaction::init(std::vector<uint32_t> write_roots, std::vector<uint32_t> read_roots)
   {
      // Build sorted, deduplicated write set.
      std::set<uint32_t> write_set(write_roots.begin(), write_roots.end());

      // Remove any read roots that are already in the write set (write implies read).
      std::set<uint32_t> read_set;
      for (auto r : read_roots)
      {
         if (write_set.find(r) == write_set.end())
            read_set.insert(r);
      }

      _write_roots.assign(write_set.begin(), write_set.end());

      // Build lock list sorted by root index (total order → no deadlocks).
      for (auto idx : write_set)
         _locks.push_back({idx, true});
      for (auto idx : read_set)
         _locks.push_back({idx, false});
      std::sort(_locks.begin(), _locks.end(),
                [](const lock_entry& a, const lock_entry& b) { return a.index < b.index; });

      // Acquire locks in sorted order.
      for (auto& le : _locks)
      {
         auto& root = _db->ensure_root_public(le.index);
         if (le.exclusive)
            root.tx_mutex.lock();
         else
            root.tx_mutex.lock_shared();
      }

      // Create dwal_transactions: read_write for write roots, read_only for read roots.
      for (auto idx : write_set)
      {
         _db->ensure_wal_public(idx);
         auto& dr = _db->root(idx);
         auto  it = _txns.emplace(
             std::piecewise_construct, std::forward_as_tuple(idx),
             std::forward_as_tuple(dr, dr.wal.get(), idx, _db, false, root_mode::read_write));
         _handles.insert({idx, root_handle(&it.first->second, idx, true)});
      }
      for (auto idx : read_set)
      {
         auto& dr = _db->root(idx);
         auto  it = _txns.emplace(std::piecewise_construct, std::forward_as_tuple(idx),
                                  std::forward_as_tuple(dr, nullptr, idx, _db, false,
                                                       root_mode::read_only));
         _handles.insert({idx, root_handle(&it.first->second, idx, false)});
      }
   }

   transaction::~transaction()
   {
      if (!_committed && !_aborted && _db)
         abort();
   }

   transaction::transaction(transaction&& other) noexcept
       : _db(std::exchange(other._db, nullptr)),
         _locks(std::move(other._locks)),
         _txns(std::move(other._txns)),
         _handles(std::move(other._handles)),
         _write_roots(std::move(other._write_roots)),
         _committed(other._committed),
         _aborted(other._aborted)
   {
      // Fix up root_handle pointers after move.
      for (auto& [idx, handle] : _handles)
         handle._inner = &_txns.at(idx);
   }

   // ── root access ──────────────────────────────────────────────────

   transaction::root_handle& transaction::root(uint32_t root_index)
   {
      auto it = _handles.find(root_index);
      assert(it != _handles.end() && "root_index not declared in transaction");
      return it->second;
   }

   // ── convenience methods ──────────────────────────────────────────

   void transaction::upsert(uint32_t root_index, std::string_view key, std::string_view value)
   {
      root(root_index).upsert(key, value);
   }

   bool transaction::remove(uint32_t root_index, std::string_view key)
   {
      return root(root_index).remove(key);
   }

   dwal_transaction::lookup_result transaction::get(uint32_t root_index, std::string_view key)
   {
      return root(root_index).get(key);
   }

   // ── commit ───────────────────────────────────────────────────────

   void transaction::commit()
   {
      assert(!_committed && !_aborted);
      _committed = true;

      if (_write_roots.size() == 1)
      {
         // Single write root: fast path, no multi-tx overhead.
         _txns.at(_write_roots[0]).commit();
      }
      else if (_write_roots.size() > 1)
      {
         // Multi-root: tag WAL entries with shared tx_id.
         auto tx_id      = _db->next_multi_tx_id();
         auto last       = _write_roots.back();
         auto part_count = static_cast<uint16_t>(_write_roots.size());

         for (auto idx : _write_roots)
            _txns.at(idx).commit_multi(tx_id, part_count, idx == last);
      }

      // Read-only transactions: commit is a no-op (just marks as committed).
      for (auto& [idx, dtx] : _txns)
      {
         if (dtx.is_read_only() && !dtx.is_committed())
            dtx.commit();
      }

      // Per-root swap check (independent, same as today).
      for (auto idx : _write_roots)
      {
         auto& dr = _db->root(idx);
         if (dr.merge_complete.load(std::memory_order_acquire) && _db->should_swap(idx))
            _db->try_swap_rw_to_ro(idx);
      }

      release_locks();
   }

   // ── abort ────────────────────────────────────────────────────────

   void transaction::abort()
   {
      assert(!_committed && !_aborted);
      _aborted = true;

      // Abort write roots in reverse order.
      for (auto it = _write_roots.rbegin(); it != _write_roots.rend(); ++it)
      {
         auto& dtx = _txns.at(*it);
         if (!dtx.is_committed() && !dtx.is_aborted())
            dtx.abort();
      }

      // Abort read-only transactions (no-op undo, just marks aborted).
      for (auto& [idx, dtx] : _txns)
      {
         if (!dtx.is_committed() && !dtx.is_aborted())
            dtx.abort();
      }

      release_locks();
   }

   // ── lock management ──────────────────────────────────────────────

   void transaction::release_locks()
   {
      for (auto& le : _locks)
      {
         auto& dr = _db->root(le.index);
         if (le.exclusive)
            dr.tx_mutex.unlock();
         else
            dr.tx_mutex.unlock_shared();
      }
      _locks.clear();
   }

}  // namespace psitri::dwal
