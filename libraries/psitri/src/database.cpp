#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>

namespace psitri
{
   database::database(const std::filesystem::path& dir, const runtime_config& cfg)
       : _dir(dir),
         _cfg(cfg),
         _allocator(dir, cfg),
         _dbfile(dir / "dbfile.bin", sal::access_mode::read_write)
   {
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

}  // namespace psitri
