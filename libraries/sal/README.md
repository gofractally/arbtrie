# SAL - Storage Abstraction Library

SAL provides a modern C++20 interface for low-level storage operations such as memory mapping and block management. 

## Features

- **Memory Mapping**: Simple interface for memory-mapped files with resize operations
- **Block Allocation**: Efficient block-based storage with 16MB block size
- **Modern Formatting**: Uses C++20's `std::format` for intuitive, type-safe logging
- **Configurable Logging**: Level-based logging system that can be controlled via environment variables

## Building

SAL is built using CMake:

```bash
# Build from the arbtrie project root
mkdir -p build/debug
cd build/debug
cmake -DCMAKE_BUILD_TYPE=Debug -DSAL_DEBUG=ON -DSAL_BUILD_TESTS=ON ../..
make

# Run the tests
./libraries/sal/bin/sal-tests
```

## Usage

### Memory Mapping

```cpp
#include <sal/mapping.hpp>

// Create or open a memory-mapped file
sal::mapping map("data.bin", sal::access_mode::read_write);

// Resize the mapping (returns old data if resizing required a new mapping)
std::shared_ptr<void> old_data = map.resize(16 * 1024 * 1024); // 16 MB

// Access the mapped memory
void* data = map.data();
std::size_t size = map.size();

// Write to the memory
std::memset(data, 0, size);

// Sync changes to disk
map.sync(sal::sync_type::sync);
```

### Block Allocation

```cpp
#include <sal/block_allocator.hpp>

// Create a block allocator with 16 MB blocks
sal::block_allocator blocks("blocks.bin", 16 * 1024 * 1024, 100);

// Allocate a new block
auto block_num = blocks.alloc();

// Get a pointer to the block
void* block_ptr = blocks.get(block_num);

// Reserve multiple blocks at once
blocks.reserve(10);

// Synchronize all blocks to disk
blocks.sync(sal::sync_type::async);
```

### Logging

```cpp
#include <sal/debug.hpp>

// Set log level via environment variable
// export SAL_LOG_LEVEL=DEBUG

// Log messages at different levels with std::format style formatting
SAL_DEBUG("Initializing component: {}", component_name);
SAL_INFO("Loaded {} items in {:.2f} seconds", count, elapsed);
SAL_WARN("Disk space below {}%", percent);
SAL_ERROR("Failed to open file: {}", filename);
```

## License

SAL is part of the arbtrie project. See project root for license details. 