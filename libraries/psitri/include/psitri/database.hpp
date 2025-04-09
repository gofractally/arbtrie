#pragma once
#include <filesystem>
#include <mutex>
#include <sal/allocator.hpp>
#include <sal/config.hpp>
#include <sal/mapping.hpp>

namespace psitri
{
   using runtime_config = sal::runtime_config;
   class write_session;
   class read_session;

   static constexpr uint32_t num_top_roots = 512;
   namespace detail
   {
      class database_state;
   }

   class database : public std::enable_shared_from_this<database>
   {
     public:
      database(const std::filesystem::path& dir, const runtime_config& cfg);
      ~database();

      static std::shared_ptr<database> create(std::filesystem::path dir,
                                              const runtime_config& = {});

      void sync();
      void set_runtime_config(const runtime_config& cfg);

      std::shared_ptr<write_session> start_write_session();
      std::shared_ptr<read_session>  start_read_session();

     private:
      std::filesystem::path _dir;
      runtime_config        _cfg;

      mutable std::mutex _sync_mutex;
      mutable std::mutex _root_change_mutex[num_top_roots];
      mutable std::mutex _modify_lock[num_top_roots];

      std::mutex& modify_lock(int index) { return _modify_lock[index]; }

      sal::allocator          _allocator;
      sal::mapping            _dbfile;
      detail::database_state* _dbm;
   };
}  // namespace psitri