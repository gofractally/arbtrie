#pragma once
#include <sys/mman.h>
#include <arbtrie/config.hpp>
#include <atomic>
#include <filesystem>
#include <memory>
#include <utility>

namespace arbtrie
{

   // none is implimented by specifiying MS_ASYNC and MS_SYNC which will
   // cause msync to fail if not checked.
   inline int msync_flag(sync_type st)
   {
      static int flags[] = {MS_ASYNC | MS_SYNC, MS_ASYNC, MS_SYNC};
      return flags[(int)st];
   };

   // Thread safety:
   //
   // The file must not be resized by another process
   //
   // resize and size may not be called concurrently
   // data may be called concurrently with itself, resize, or size
   // The pointer returned by resize must be retained until all accesses to the previous data complete.
   //
   // Formally,
   //
   // Given
   //   - R is a call to resize that returns a non-null pointer
   //   - X is the destruction of the last copy of the result of R
   //   - D is a call to data
   //   - A is a memory access to the region referenced by the result of D
   // then, the behavior is undefined unless A happens before X OR R happens before D
   //
   class mapping
   {
     public:
      mapping(const std::filesystem::path& file, access_mode mode, bool pin = false);
      ~mapping();
      // Sets the size of the file to new_size.
      //
      // If data is invalidated, returns a shared_ptr that owns the
      // previous data. Otherwise returns null.
      //
      // exception safety: strong
      std::shared_ptr<void> resize(std::size_t new_size);
      void*                 data() { return _data.load(); }
      const void*           data() const { return _data.load(); }
      std::size_t           size() const { return _size; }
      bool                  pinned() const { return _pinned; }
      access_mode           mode() const { return _mode; }
      void                  sync(sync_type st = sync_type::sync)
      {
         if (not msync_flag(st))
            return;
         if (msync(data(), size(), msync_flag(st)))
         {
            throw std::runtime_error("mapping.hpp: msync returned -1");
         }
      }

     private:
      std::atomic<void*> _data;
      std::size_t        _size;
      int                _fd;
      access_mode        _mode;
      bool               _pinned;
   };
}  // namespace arbtrie
