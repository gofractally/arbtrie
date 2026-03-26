#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/node/inner.hpp>
#include <psitri/node/leaf.hpp>
#include <psitri/node/value_node.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/write_session_impl.hpp>

namespace psitri
{
   static void register_node_types()
   {
      static bool registered = false;
      if (registered)
         return;
      registered = true;
      sal::register_type_vtable<leaf_node>();
      sal::register_type_vtable<inner_node>();
      sal::register_type_vtable<inner_prefix_node>();
      sal::register_type_vtable<value_node>();
   }

   database::database(const std::filesystem::path& dir,
                      const runtime_config&       cfg,
                      recovery_mode               mode)
       : _dir(dir),
         _cfg(cfg),
         _allocator(dir, cfg),
         _dbfile(dir / "dbfile.bin", sal::access_mode::read_write)
   {
      register_node_types();
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

   database::~database()
   {
      _dbm->clean_shutdown = true;
      _dbfile.sync(sal::sync_type::full);
   }

   std::shared_ptr<database> database::create(std::filesystem::path dir, const runtime_config& cfg)
   {
      if (std::filesystem::exists(std::filesystem::symlink_status(dir / "db")) ||
          std::filesystem::exists(std::filesystem::symlink_status(dir / "data")))
         throw std::runtime_error("directory already exists: " + dir.generic_string());

      std::filesystem::create_directories(dir / "data");

      return std::make_shared<database>(dir, cfg);
   }

   bool database::ref_counts_stale() const
   {
      return _dbm->flags & detail::flag_ref_counts_stale;
   }

   void database::sync()
   {
      std::lock_guard lock(_sync_mutex);
      _allocator.sync(_cfg.sync_mode);
   }

   void database::reclaim_leaked_memory()
   {
      if (!(_dbm->flags & detail::flag_ref_counts_stale))
         return;
      SAL_WARN("reclaiming leaked memory (deferred cleanup)");
      _allocator.reset_reference_counts();
      _dbm->flags &= ~detail::flag_ref_counts_stale;
   }

   void database::set_runtime_config(const runtime_config& cfg)
   {
      _cfg = cfg;
      _allocator.set_runtime_config(cfg);
   }

   read_session::~read_session()  = default;
   write_session::~write_session() = default;

   std::shared_ptr<write_session> database::start_write_session()
   {
      return std::make_shared<write_session>(*this);
   }
   std::shared_ptr<read_session> database::start_read_session()
   {
      return std::make_shared<read_session>(*this);
   }

   void database::defrag()
   {
      namespace fs = std::filesystem;

      auto defrag_dir = fs::path(_dir.string() + ".defrag");
      auto backup_dir = fs::path(_dir.string() + ".old");

      // Clean up any leftover defrag directory from a prior interrupted run
      fs::remove_all(defrag_dir);

      // Create the destination database
      auto dest_db = database::create(defrag_dir, _cfg);

      // Stop source background threads for consistent segment access
      _allocator.stop_background_threads();

      // Copy all live objects from source to dest
      _allocator.copy_live_objects_to(dest_db->_allocator);

      // Sync destination to disk
      dest_db->sync();

      // Verify: walk the entire tree structure in the new database.
      // reachable_size() follows all child pointers from every root,
      // proving the tree is navigable and all control blocks resolve.
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

      // Truncate trailing free segments to minimize file size.
      // Use truncate_free_tail_final (not truncate_free_tail) to avoid
      // provider_populate re-allocating segments we just freed.
      dest_db->_allocator.truncate_free_tail_final();

      // Close destination database (destructor won't restart threads)
      dest_db.reset();

      // Restart source background threads (needed for clean destructor)
      _allocator.start_background_threads();

      // Swap: source → backup, defrag → source
      if (fs::exists(backup_dir))
         fs::remove_all(backup_dir);
      fs::rename(_dir, backup_dir);
      fs::rename(defrag_dir, _dir);

      SAL_WARN("defrag complete. Old database preserved at: {}", backup_dir.string());
      SAL_WARN("Verify the new database, then remove {} manually", backup_dir.string());
   }

}  // namespace psitri
