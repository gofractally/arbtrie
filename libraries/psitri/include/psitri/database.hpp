#pragma once
#include <chrono>
#include <filesystem>
#include <mutex>
#include <psitri/write_session.hpp>
#include <sal/allocator.hpp>
#include <sal/config.hpp>
#include <sal/mapping.hpp>
#include <sal/seg_alloc_dump.hpp>
#include <thread>

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

      sal::seg_alloc_dump dump() const { return _allocator.dump(); }
      void print_stats(std::ostream& os = std::cout) const { dump().print(os); }

      /// Block until the compactor has drained all pending releases.
      /// Returns true if drained, false if timed out.
      bool wait_for_compactor(std::chrono::milliseconds timeout = std::chrono::milliseconds(10000))
      {
         auto ses      = start_write_session();
         auto deadline = std::chrono::steady_clock::now() + timeout;
         while (std::chrono::steady_clock::now() < deadline)
         {
            if (ses->get_pending_release_count() == 0)
            {
               std::this_thread::sleep_for(std::chrono::milliseconds(50));
               if (ses->get_pending_release_count() == 0)
                  return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
         }
         return false;
      }

      /// Full recovery: rebuild control blocks from segments and reclaim leaked memory
      void recover() { _allocator.recover(); }

      /// Lightweight recovery: reset reference counts and reclaim leaked memory
      void reset_reference_counts() { _allocator.reset_reference_counts(); }

      std::shared_ptr<write_session> start_write_session();
      std::shared_ptr<read_session>  start_read_session();

     private:
      friend class read_session;
      friend class write_session;

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
   using database_ptr = std::shared_ptr<database>;
}  // namespace psitri