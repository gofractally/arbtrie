#include <arbtrie/block_allocator.hpp>

namespace arbtrie
{
   block_allocator::block_allocator(std::filesystem::path file,
                                    uint64_t              block_size,
                                    uint32_t              max_blocks,
                                    bool                  read_write)
       : _filename(file), _block_size(block_size)
   {
      _max_blocks    = max_blocks;
      _block_mapping = new char_ptr[max_blocks];

      int flags = O_CLOEXEC;
      int flock_operation;
      if (read_write)
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
         ARBTRIE_DEBUG("opening ", file.native());
         throw std::runtime_error("unable to open block file");
      }

      if (::flock(_fd, flock_operation | LOCK_NB) != 0)
      {
         ::close(_fd);
         throw std::system_error{errno, std::generic_category()};
      }
      struct stat statbuf[1];
      if (::fstat(_fd, statbuf) != 0)
      {
         ::close(_fd);
         throw std::system_error{errno, std::generic_category()};
      }
      _file_size = statbuf->st_size;
      if (_file_size % block_size != 0)
      {
         ::close(_fd);
         throw std::runtime_error("block file isn't a multiple of block size");
      }
      if (_file_size)
      {
         if (auto addr = ::mmap(nullptr, _file_size, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
             addr != MAP_FAILED)
         {
            char* data = (char*)addr;
            auto  end  = data + _file_size;
            while (data != end)
            {
               _block_mapping[_num_blocks.fetch_add(1)] = data;
               //_block_mapping.push_back(data);
               data += _block_size;
            }
         }
         else
         {
            ::close(_fd);
            throw std::system_error{errno, std::generic_category()};
         }
      }
   }

   block_allocator::~block_allocator()
   {
      if (_fd)
      {
         for (uint32_t i = 0; i < _num_blocks.load(); ++i)
            ::munmap(_block_mapping[i], _block_size);
         ::close(_fd);
      }
   }

   void block_allocator::sync(sync_type st)
   {
      if (_fd and sync_type::none != st)
      {
         uint64_t nb = num_blocks();
         for (uint32_t i = 0; i < nb; ++i)
            ::msync(_block_mapping[i], _block_size, msync_flag(st));
      }
   }

   uint32_t block_allocator::reserve(uint32_t desired_num_blocks, bool memlock)
   {
      if (desired_num_blocks > _max_blocks)
         throw std::runtime_error("unable to reserve, maximum block would be reached");

      std::lock_guard l{_resize_mutex};
      if (num_blocks() >= desired_num_blocks)
         return desired_num_blocks;

      auto cur_num   = num_blocks();
      auto add_count = desired_num_blocks - cur_num;

      auto new_size = _file_size + _block_size * add_count;
      if (::ftruncate(_fd, new_size) < 0)
      {
         throw std::system_error(errno, std::generic_category());
      }

      auto prot = PROT_READ | PROT_WRITE;
      if (auto addr = ::mmap(nullptr, _block_size, prot, MAP_SHARED, _fd, _file_size);
          addr != MAP_FAILED)
      {
         if (memlock)
         {
            if (::mlock(addr, new_size - _file_size))
            {
               ARBTRIE_WARN("unable to mlock ID lookups");
               ::madvise(addr, new_size - _file_size, MADV_RANDOM);
            }
         }
         char* ac = (char*)addr;
         while (cur_num < desired_num_blocks)
         {
            _block_mapping[cur_num] = ac;
            ++cur_num;
            ac += _block_size;
         }
         _file_size = new_size;
         return _num_blocks.fetch_add(add_count, std::memory_order_release) + add_count;
      }
      if (::ftruncate(_fd, _file_size) < 0)
      {
         throw std::system_error(errno, std::generic_category());
      }
      throw std::runtime_error("unable to mmap new block");
   }

   block_allocator::block_number block_allocator::alloc()
   {
      std::lock_guard l{_resize_mutex};
      auto            nb = _num_blocks.load(std::memory_order_relaxed);
      if (nb == _max_blocks)
         throw std::runtime_error("maximum block number reached");

      auto new_size = _file_size + _block_size;
      if (::ftruncate(_fd, new_size) < 0)
      {
         throw std::system_error(errno, std::generic_category());
      }

      auto prot = PROT_READ | PROT_WRITE;
      if (auto addr = ::mmap(nullptr, _block_size, prot, MAP_SHARED, _fd, _file_size);
          addr != MAP_FAILED)
      {
         _block_mapping[_num_blocks.load(std::memory_order_relaxed)] = (char*)addr;
         _file_size                                                  = new_size;
         return _num_blocks.fetch_add(1, std::memory_order_release);
      }
      ARBTRIE_ERROR("unable to mmap new block, file size: ", _file_size,
                    " block size: ", _block_size,
                    " num blocks: ", _num_blocks.load(std::memory_order_relaxed),
                    " max blocks: ", _max_blocks, " error: ", strerror(errno));
      throw std::runtime_error("unable to mmap new block");
   }
}  // namespace arbtrie