#include <sal/debug.hpp>
#include <sal/mapping.hpp>

#include <cassert>
#include <system_error>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>

namespace sal
{
   namespace
   {
      int get_prot(access_mode mode)
      {
         if (mode == access_mode::read_write)
         {
            return PROT_READ | PROT_WRITE;
         }
         else
         {
            assert(mode == access_mode::read_only);
            return PROT_READ;
         }
      }

      void try_pin(bool* pinned, void* base, std::size_t size)
      {
         if (*pinned)
         {
            bool success = ::mlock(base, size) == 0;
            if (!success && debug_memory)
            {
               SAL_WARN("Failed to mlock memory region: addr={:#x}, size={}",
                        reinterpret_cast<uintptr_t>(base), size);
            }
            *pinned = success;
         }
      }
   }  // namespace

   mapping::mapping(const std::filesystem::path& file, access_mode mode, bool pin)
       : _mode(mode), _pinned(pin)
   {
      int flags = O_CLOEXEC;
      int flock_operation;
      if (mode == access_mode::read_write)
      {
         flags |= O_RDWR;
         flags |= O_CREAT;
         flock_operation = LOCK_EX;
      }
      else
      {
         flags |= O_RDONLY;
         flock_operation = LOCK_SH;
      }
      _fd = ::open(file.native().c_str(), flags, 0644);
      if (_fd == -1)
      {
         SAL_ERROR("Failed to open file: {}, error={}", file.native(), strerror(errno));
         throw std::system_error{errno, std::generic_category()};
      }
      if (::flock(_fd, flock_operation | LOCK_NB) != 0)
      {
         SAL_ERROR("Failed to lock file: {}, error={}", file.native(), strerror(errno));
         ::close(_fd);
         throw std::system_error{errno, std::generic_category()};
      }
      struct stat statbuf[1];
      if (::fstat(_fd, statbuf) != 0)
      {
         SAL_ERROR("Failed to stat file: {}, error={}", file.native(), strerror(errno));
         ::close(_fd);
         throw std::system_error{errno, std::generic_category()};
      }
      _size = statbuf->st_size;
      if (_size == 0)
      {
         _data = nullptr;
         SAL_WARN("Opened empty file: {}", file.native());
      }
      else
      {
         if (auto addr = ::mmap(nullptr, _size, get_prot(mode), MAP_SHARED, _fd, 0);
             addr != MAP_FAILED)
         {
            _data = addr;
            try_pin(&_pinned, addr, _size);
            SAL_WARN("Mapped file: {}, size={}, addr={:#x}", file.native(), _size,
                     reinterpret_cast<uintptr_t>(addr));
         }
         else
         {
            SAL_ERROR("Failed to mmap file: {}, size={}, error={}", file.native(), _size,
                      strerror(errno));
            ::close(_fd);
            throw std::system_error{errno, std::generic_category()};
         }
      }
   }

   mapping::~mapping()
   {
      if (auto p = _data.load())
      {
         if (debug_memory)
         {
            SAL_WARN("Unmapping memory: addr={:#x}, size={}", reinterpret_cast<uintptr_t>(p),
                     _size);
         }
         ::munmap(p, _size);
      }
      ::close(_fd);
   }

   std::shared_ptr<void> mapping::resize(std::size_t new_size)
   {
      if (new_size < _size)
      {
         SAL_WARN("Shrinking mapping not implemented: current={}, requested={}", _size, new_size);
         throw std::runtime_error("Not implemented: shrink mapping");
      }
      else if (new_size == _size)
      {
         return nullptr;
      }

      SAL_WARN("Resizing mapping: current={}, new={}", _size, new_size);

      struct munmapper
      {
         ~munmapper()
         {
            if (addr)
            {
               ::munmap(addr, size);
            }
         }
         void*       addr = nullptr;
         std::size_t size;
      };
      // Do this first, because it can throw
      auto result = std::make_shared<munmapper>();
      if (::ftruncate(_fd, new_size) < 0)
      {
         SAL_ERROR("Failed to resize file: current={}, new={}, error={}", _size, new_size,
                   strerror(errno));
         throw std::system_error{errno, std::generic_category()};
      }
      auto prot = get_prot(_mode);
#ifdef MAP_FIXED_NOREPLACE
      // Try to extend the existing mapping
      {
         auto end = (char*)_data.load() + _size;
         auto addr =
             ::mmap(end, new_size - _size, prot, MAP_SHARED | MAP_FIXED_NOREPLACE, _fd, _size);
         if (addr == end)
         {
            try_pin(&_pinned, end, new_size - _size);
            _size = new_size;
            SAL_WARN("Extended existing mapping: addr={:#x}, new_size={}",
                     reinterpret_cast<uintptr_t>(_data.load()), _size);
            return nullptr;
         }
         else
         {
            if (addr != MAP_FAILED)
            {
               // fail safe in case the kernel does not support MAP_FIXED_NOREPLACE
               ::munmap(addr, new_size - _size);
            }
         }
      }
#endif
      // Move the mapping to a new location
      if (auto addr = ::mmap(nullptr, new_size, prot, MAP_SHARED, _fd, 0); addr != MAP_FAILED)
      {
         result->addr = _data.load();
         result->size = _size;
         _data        = addr;
         _size        = new_size;
         try_pin(&_pinned, addr, _size);
         SAL_WARN("Remapped to new location: old_addr={:#x}, new_addr={:#x}, size={}",
                  reinterpret_cast<uintptr_t>(result->addr), reinterpret_cast<uintptr_t>(addr),
                  _size);
         return std::shared_ptr<void>(std::move(result), result->addr);
      }
      else
      {
         SAL_ERROR("Failed to remap file: size={}, error={}", new_size, strerror(errno));
         throw std::system_error{errno, std::generic_category()};
      }
   }

}  // namespace sal