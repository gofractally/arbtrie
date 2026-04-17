#pragma once
#include <psitri/database.hpp>
#include <psitri/read_session.hpp>
#include <psitri/write_session.hpp>

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
         database_state()
         {
            for (auto& r : top_root)
               r.store(0, std::memory_order_relaxed);
         }
         uint32_t          magic          = sal::file_magic;
         uint32_t          flags          = 0;
         std::atomic<bool> clean_shutdown = true;
         runtime_config    config;
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
         _allocator(dir, cfg),
         _dbfile(dir / "dbfile.bin", sal::access_mode::read_write)
   {
      detail::register_node_types();
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
      _dbm->clean_shutdown = false;
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

}  // namespace psitri
