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

   database::database(const std::filesystem::path& dir, const runtime_config& cfg)
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

      if (not _dbm->clean_shutdown)
         SAL_WARN("database was not shutdown cleanly, memory may have leaked");
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

}  // namespace psitri
