#pragma once
#include <algorithm>
#include <psitri/database.hpp>
#include <psitri/read_session.hpp>
#include <psitri/write_session.hpp>
#include <sal/allocator_session_impl.hpp>

namespace psitri
{
   namespace detail
   {
      /// Bit flags for database_state::flags
      enum db_flags : uint32_t
      {
         flag_ref_counts_stale = 1u << 0,  ///< ref counts need cleanup (deferred_cleanup)
      };

      struct database_state
      {
         uint32_t          magic          = sal::file_magic;
         uint32_t          flags          = 0;
         std::atomic<bool> clean_shutdown = true;
         runtime_config    config;

         /// Global monotonic MVCC version counter, incremented on every commit.
         /// The version number is stored in a custom control block (via alloc_custom_cb)
         /// and the ver_adr is packed into the root slot alongside root_adr.
         std::atomic<uint64_t> global_version{0};

         /// Epoch base = (global_version / epoch_interval) * epoch_interval.
         /// Inner nodes store the root version at which they were last made unique.
         /// The first write after an epoch boundary to a path with stale inner
         /// nodes triggers a COW maintenance cascade.
         ///
         /// Trade-off: smaller interval → more frequent COW maintenance (less bloat,
         /// lower MVCC throughput).  Larger → more MVCC fast-path hits (higher
         /// throughput, more temporary version accumulation).
         ///
         /// Rule of thumb: set to >= the number of unique keys in the hot set.
         /// Default 1M provides a maintenance pass roughly every 1M writes.
         uint64_t epoch_interval = 1'000'000;

         uint64_t current_epoch_base() const noexcept
         {
            auto version = global_version.load(std::memory_order_relaxed);
            return (version / epoch_interval) * epoch_interval;
         }

         // top_root is an array of root object addresses, one per top-level root.
         // Reads/writes are routed through the SAL allocator's atomic primitives;
         // user-visible serialization is provided by basic_database::_modify_lock.
         // The atomic type also aids SIGKILL behavior by avoiding torn writes.
         std::atomic<uint64_t> top_root[num_top_roots];
      };

      inline bool database_exists(const std::filesystem::path& dir)
      {
         return std::filesystem::exists(std::filesystem::symlink_status(dir / "data"));
      }
   }  // namespace detail

   // ─── basic_database<LockPolicy> member definitions ──────────────────────

   template <class LockPolicy>
   basic_database<LockPolicy>::~basic_database()
   {
      _dbm->clean_shutdown = true;
      _dbfile.sync(sal::sync_type::full);
   }

   template <class LockPolicy>
   basic_database<LockPolicy>::basic_database(const std::filesystem::path& dir,
                                              const runtime_config&        cfg,
                                              recovery_mode                mode)
       : _dir(dir),
         _cfg(cfg),
         _object_registry(_dead_versions),
         _allocator(dir, cfg, false),
         _dbfile(dir / "dbfile.bin", sal::access_mode::read_write)
   {
      _object_registry.install(_allocator);

      // Register callback: when a version CB's refcount drops to 0,
      // record its version number in the dead-version map for MVCC
      // version reclamation.
      _allocator.set_on_custom_cb_released(
          [this](uint64_t version) { _dead_versions.add_dead_version(version); });

      if (_dbfile.size() == 0)
      {
         _dbfile.resize(sizeof(detail::database_state));
         new (_dbfile.data()) detail::database_state();
      }
      _dbm = reinterpret_cast<detail::database_state*>(_dbfile.data());

      if (_dbfile.size() != sizeof(detail::database_state))
         throw std::runtime_error("Wrong size for file: " + (dir / "db").native());

      /// TODO: add custom magic number of database file
      if (_dbm->magic != sal::file_magic)
         throw std::runtime_error("Not a arbtrie file: " + (dir / "db").native());

      // Determine effective recovery mode
      auto effective_mode = mode;
      if (not _dbm->clean_shutdown && effective_mode == recovery_mode::none)
         effective_mode = recovery_mode::deferred_cleanup;  // fast default for unclean shutdown

      switch (effective_mode)
      {
         case recovery_mode::none:
            break;
         case recovery_mode::deferred_cleanup:
            SAL_WARN("database was not shutdown cleanly, deferring leak reclamation");
            _dbm->flags |= detail::flag_ref_counts_stale;
            break;
         case recovery_mode::app_crash:
            SAL_WARN("database was not shutdown cleanly, resetting reference counts");
            _allocator.reset_reference_counts();
            _dbm->flags &= ~detail::flag_ref_counts_stale;
            break;
         case recovery_mode::power_loss:
            SAL_WARN("power loss recovery: validating segments and rebuilding state");
            _allocator.recover_from_power_loss();
            _dbm->flags &= ~detail::flag_ref_counts_stale;
            break;
         case recovery_mode::full_verify:
            SAL_WARN("full verification requested: rebuilding and verifying all checksums");
            _allocator.recover();  // TODO: add verify pass
            _dbm->flags &= ~detail::flag_ref_counts_stale;
            break;
      }
      recover_global_version_from_roots();
      _dbm->clean_shutdown = false;
      _allocator.start_background_threads();
   }

   template <class LockPolicy>
   void basic_database<LockPolicy>::recover_global_version_from_roots()
   {
      auto     session     = _allocator.get_session();
      uint64_t max_version = 0;
      for (uint32_t i = 0; i < num_top_roots; ++i)
      {
         auto root = session->template get_root<>(sal::root_object_number(i));
         auto ver  = root.ver();
         if (ver == sal::null_ptr_address)
            continue;

         auto version_opt = session->try_read_custom_cb(ver);
         if (!version_opt)
            continue;

         auto version = *version_opt;
         if (version >= sal::control_block::max_cacheline_offset - 1)
            continue;
         max_version = std::max(max_version, version);
      }

      auto current = _dbm->global_version.load(std::memory_order_relaxed);
      while (current < max_version &&
             !_dbm->global_version.compare_exchange_weak(
                 current, max_version, std::memory_order_relaxed,
                 std::memory_order_relaxed))
      {
      }
   }

   template <class LockPolicy>
   std::shared_ptr<basic_database<LockPolicy>>
   basic_database<LockPolicy>::open(std::filesystem::path dir,
                                    open_mode             mode,
                                    const runtime_config& cfg,
                                    recovery_mode         recovery)
   {
      bool exists = detail::database_exists(dir);

      switch (mode)
      {
         case open_mode::create_only:
            if (exists)
               throw std::runtime_error("database already exists: " + dir.generic_string());
            break;

         case open_mode::open_existing:
            if (!exists)
               throw std::runtime_error("database does not exist: " + dir.generic_string());
            break;

         case open_mode::create_or_open:
            break;

         case open_mode::read_only:
            throw std::runtime_error("read_only mode not yet implemented");
      }

      if (!exists)
         std::filesystem::create_directories(dir / "data");

      auto db = std::make_shared<basic_database<LockPolicy>>(dir, cfg, recovery);
      db->_allocator.init_shared_ownership(db);
      return db;
   }

   template <class LockPolicy>
   std::shared_ptr<basic_database<LockPolicy>>
   basic_database<LockPolicy>::create(std::filesystem::path dir, const runtime_config& cfg)
   {
      return open(std::move(dir), open_mode::create_only, cfg);
   }

   template <class LockPolicy>
   bool basic_database<LockPolicy>::ref_counts_stale() const
   {
      return _dbm->flags & detail::flag_ref_counts_stale;
   }

   template <class LockPolicy>
   void basic_database<LockPolicy>::reclaim_leaked_memory()
   {
      if (!(_dbm->flags & detail::flag_ref_counts_stale))
         return;
      SAL_WARN("reclaiming leaked memory (deferred cleanup)");
      _allocator.reset_reference_counts();
      _dbm->flags &= ~detail::flag_ref_counts_stale;
   }

   template <class LockPolicy>
   void basic_database<LockPolicy>::init_allocator_shared_ownership()
   {
      _allocator.init_shared_ownership(this->shared_from_this());
   }

   template <class LockPolicy>
   std::shared_ptr<basic_write_session<LockPolicy>>
   basic_database<LockPolicy>::start_write_session()
   {
      return std::make_shared<basic_write_session<LockPolicy>>(*this);
   }

   template <class LockPolicy>
   std::shared_ptr<basic_read_session<LockPolicy>>
   basic_database<LockPolicy>::start_read_session()
   {
      return std::make_shared<basic_read_session<LockPolicy>>(*this);
   }

   template <class LockPolicy>
   void basic_database<LockPolicy>::defrag()
   {
      namespace fs = std::filesystem;

      auto defrag_dir = fs::path(_dir.string() + ".defrag");
      auto backup_dir = fs::path(_dir.string() + ".old");

      // Clean up any leftover defrag directory from a prior interrupted run
      fs::remove_all(defrag_dir);

      // Create the destination database
      auto dest_db = basic_database::create(defrag_dir, _cfg);

      // Stop source background threads for consistent segment access
      _allocator.stop_background_threads();

      // Copy all live objects from source to dest
      _allocator.copy_live_objects_to(dest_db->_allocator);

      // Sync destination to disk
      dest_db->sync();

      // Verify: walk the entire tree structure in the new database.
      auto src_reachable = _allocator.reachable_size();
      auto dst_reachable = dest_db->_allocator.reachable_size();

      if (src_reachable != dst_reachable)
      {
         dest_db.reset();
         fs::remove_all(defrag_dir);
         throw std::runtime_error(
             "defrag verification failed: reachable size mismatch (src=" +
             std::to_string(src_reachable) + " dst=" + std::to_string(dst_reachable) + ")");
      }

      SAL_WARN("defrag: verified reachable_size = {} bytes in both databases",
               dst_reachable);

      dest_db->_allocator.truncate_free_tail_final();

      dest_db.reset();

      _allocator.start_background_threads();

      if (fs::exists(backup_dir))
         fs::remove_all(backup_dir);
      fs::rename(_dir, backup_dir);
      fs::rename(defrag_dir, _dir);

      SAL_WARN("defrag complete. Old database preserved at: {}", backup_dir.string());
      SAL_WARN("Verify the new database, then remove {} manually", backup_dir.string());
   }

   template <class LockPolicy>
   void basic_database<LockPolicy>::set_epoch_interval(uint64_t interval)
   {
      _dbm->epoch_interval = interval;
   }

   template <class LockPolicy>
   uint64_t basic_database<LockPolicy>::current_epoch_base() const
   {
      return _dbm->current_epoch_base();
   }

}  // namespace psitri
