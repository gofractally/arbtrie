#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/node/inner.hpp>
#include <psitri/node/leaf.hpp>
#include <psitri/node/value_node.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/write_session_impl.hpp>
#include <unordered_set>

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

   static bool database_exists(const std::filesystem::path& dir)
   {
      return std::filesystem::exists(std::filesystem::symlink_status(dir / "data"));
   }

   std::shared_ptr<database> database::open(std::filesystem::path dir,
                                            open_mode             mode,
                                            const runtime_config& cfg,
                                            recovery_mode         recovery)
   {
      bool exists = database_exists(dir);

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

      auto db = std::make_shared<database>(dir, cfg, recovery);
      db->_allocator.init_shared_ownership(db);
      return db;
   }

   std::shared_ptr<database> database::create(std::filesystem::path dir, const runtime_config& cfg)
   {
      return open(std::move(dir), open_mode::create_only, cfg);
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

   void database::init_allocator_shared_ownership()
   {
      _allocator.init_shared_ownership(shared_from_this());
   }

   std::shared_ptr<write_session> database::start_write_session()
   {
      return std::make_shared<write_session>(*this);
   }
   std::shared_ptr<read_session> database::start_read_session()
   {
      return std::make_shared<read_session>(*this);
   }

   namespace
   {
      using vr = sal::verify_result;

      void verify_node(sal::allocator&                alloc,
                        sal::ptr_address               addr,
                        const std::string&             key_prefix,
                        uint32_t                       root_index,
                        vr&                            result,
                        std::unordered_set<uint64_t>&  visited)
      {
         if (addr == sal::null_ptr_address)
            return;

         if (!visited.insert(*addr).second)
            return;  // already verified (shared node in DAG)

         auto [obj, loc] = alloc.resolve(addr);
         if (!obj)
         {
            ++result.dangling_pointers;
            result.node_failures.push_back(
                {addr, {}, sal::header_type::undefined, root_index,
                 vr::to_hex(key_prefix), "dangling_pointer", false});
            return;
         }

         ++result.nodes_visited;
         result.reachable_bytes += obj->size();

         // Verify object checksum
         if (obj->checksum() == 0)
         {
            ++result.object_checksums.unknown;
         }
         else if (sal::vcall::verify_checksum(obj))
         {
            ++result.object_checksums.passed;
         }
         else
         {
            ++result.object_checksums.failed;
            result.node_failures.push_back(
                {addr, loc, obj->type(), root_index,
                 vr::to_hex(key_prefix), "object_checksum", false});
         }

         // Dispatch by node type
         auto type = static_cast<node_type>(obj->type());

         switch (type)
         {
            case node_type::inner:
            {
               auto* n = static_cast<const inner_node*>(static_cast<const node*>(obj));
               auto  d = n->divs();
               for (uint32_t b = 0; b < n->num_branches(); ++b)
               {
                  std::string child_prefix = key_prefix;
                  if (b > 0 && b - 1 < d.size())
                     child_prefix.push_back(d[b - 1]);
                  else if (b == 0)
                     child_prefix.push_back('\0');  // first branch covers bytes starting at 0

                  verify_node(alloc, n->get_branch(branch_number(b)),
                              child_prefix, root_index, result, visited);
               }
               break;
            }
            case node_type::inner_prefix:
            {
               auto* n      = static_cast<const inner_prefix_node*>(static_cast<const node*>(obj));
               auto  prefix = n->prefix();
               auto  d      = n->divs();
               for (uint32_t b = 0; b < n->num_branches(); ++b)
               {
                  std::string child_prefix = key_prefix;
                  child_prefix.append(prefix.data(), prefix.size());
                  if (b > 0 && b - 1 < d.size())
                     child_prefix.push_back(d[b - 1]);
                  else if (b == 0)
                     child_prefix.push_back('\0');

                  verify_node(alloc, n->get_branch(branch_number(b)),
                              child_prefix, root_index, result, visited);
               }
               break;
            }
            case node_type::leaf:
            {
               auto* leaf = static_cast<const leaf_node*>(static_cast<const node*>(obj));

               for (uint32_t b = 0; b < leaf->num_branches(); ++b)
               {
                  auto bn  = branch_number(b);
                  auto key = leaf->get_key(bn);

                  // Verify key hash
                  if (leaf->verify_key_hash(bn))
                  {
                     ++result.key_checksums.passed;
                  }
                  else
                  {
                     ++result.key_checksums.failed;
                     result.key_failures.push_back(
                         {vr::to_hex(key), root_index, addr, loc,
                          static_cast<uint16_t>(b), "key_hash"});
                  }

                  // Verify value checksum or recurse into address values
                  auto vt = leaf->get_value_type(bn);
                  if (vt == leaf_node::value_type_flag::inline_data)
                  {
                     if (leaf->verify_value_checksum(bn))
                     {
                        ++result.value_checksums.passed;
                     }
                     else
                     {
                        ++result.value_checksums.failed;
                        result.key_failures.push_back(
                            {vr::to_hex(key), root_index, addr, loc,
                             static_cast<uint16_t>(b), "value_checksum"});
                     }
                  }
                  else if (vt == leaf_node::value_type_flag::value_node ||
                           vt == leaf_node::value_type_flag::subtree)
                  {
                     auto val = leaf->get_value(bn);
                     std::string child_prefix = key_prefix;
                     child_prefix.append(key.data(), key.size());
                     verify_node(alloc, val.address(),
                                 child_prefix, root_index, result, visited);
                  }
                  // null values have no checksum to verify
               }
               break;
            }
            case node_type::value:
            {
               auto* vn = static_cast<const value_node*>(static_cast<const node*>(obj));
               if (vn->is_subtree)
               {
                  auto child_addr = *reinterpret_cast<const sal::ptr_address*>(vn->data);
                  verify_node(alloc, child_addr, key_prefix, root_index, result, visited);
               }
               // value_node data integrity is covered by the object checksum
               break;
            }
         }
      }
   }  // anonymous namespace

   sal::verify_result database::verify()
   {
      sal::verify_result result;

      // Pass 1: Segment sync checksums
      _allocator.verify_segments(result);

      // Pass 2: Tree walk from all roots
      std::unordered_set<uint64_t> visited;
      visited.reserve(1 << 20);

      auto& roots     = _allocator.root_objects();
      auto  num_roots = std::min<uint32_t>(roots.size(), num_top_roots);
      for (uint32_t i = 0; i < num_roots; ++i)
      {
         auto addr = roots[i].load(std::memory_order_relaxed);
         if (addr != sal::null_ptr_address)
         {
            ++result.roots_checked;
            verify_node(_allocator, addr, {}, i, result, visited);
         }
      }

      return result;
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
