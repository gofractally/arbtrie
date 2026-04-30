#include <psitri/dwal/wal_status.hpp>

#include <cerrno>
#include <cstring>
#include <stdexcept>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace psitri::dwal
{
   wal_status_mapping::wal_status_mapping(const std::filesystem::path& wal_dir)
   {
      std::filesystem::create_directories(wal_dir);
      auto path = wal_dir / "status.bin";

      _fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
      if (_fd < 0)
         throw std::runtime_error("dwal status: open(" + path.string() + "): " +
                                  std::strerror(errno));

      const size_t status_size = sizeof(wal_status_file);
      struct stat  st;
      if (::fstat(_fd, &st) != 0)
      {
         ::close(_fd);
         _fd = -1;
         throw std::runtime_error("dwal status: fstat(" + path.string() + "): " +
                                  std::strerror(errno));
      }

      if (st.st_size != static_cast<off_t>(status_size))
      {
         if (::ftruncate(_fd, static_cast<off_t>(status_size)) != 0)
         {
            ::close(_fd);
            _fd = -1;
            throw std::runtime_error("dwal status: ftruncate(" + path.string() + "): " +
                                     std::strerror(errno));
         }
      }

      void* mapped = ::mmap(nullptr, status_size, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
      if (mapped == MAP_FAILED)
      {
         ::close(_fd);
         _fd = -1;
         throw std::runtime_error("dwal status: mmap(" + path.string() + "): " +
                                  std::strerror(errno));
      }

      _data = static_cast<wal_status_file*>(mapped);
      if (_data->roots_header.magic != wal_status_magic ||
          _data->roots_header.version != wal_status_version ||
          _data->roots_header.root_count != wal_status_max_roots)
      {
         std::memset(_data, 0, status_size);
         _data->roots_header.magic      = wal_status_magic;
         _data->roots_header.version    = wal_status_version;
         _data->roots_header.root_count = wal_status_max_roots;
      }
   }

   wal_status_mapping::~wal_status_mapping()
   {
      if (_data)
         ::munmap(_data, sizeof(wal_status_file));
      if (_fd >= 0)
         ::close(_fd);
   }

   wal_root_status* wal_status_mapping::root(uint32_t index) noexcept
   {
      if (!_data || index >= wal_status_max_roots)
         return nullptr;
      return &_data->roots[index];
   }

} // namespace psitri::dwal
