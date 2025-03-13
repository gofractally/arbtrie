# SAL (Systems Abstraction Layer) Library

The SAL library provides a collection of low-level system utilities for memory management, file operations, and performance-critical operations used by the arbtrie project.

## Key Components

### Block Allocator

The `block_allocator` class is responsible for efficient file-backed memory management with the following features:

- Memory-mapped file storage with contiguous virtual address space
- Efficient block allocation for data structures requiring direct memory access
- Support for block-aligned memory operations
- Atomically safe growth operations that maintain pointer stability

## Improvements over Original Implementation

The SAL library implements several critical improvements over the original arbtrie codebase:

### 1. Enhanced Memory Mapping Strategy

**Original Approach:**
- Memory regions were unmapped and remapped during growth
- Pointers to previously allocated blocks could become invalidated
- This caused potential memory corruption if pointers were in use

**SAL Improvements:**
- **Pointer Stability**: Once memory is allocated, its address never changes
- **Incremental Mapping**: Only new regions are mapped, leaving existing mappings untouched
- **Contiguous Allocation**: Maintains a contiguous virtual address space for all blocks

### 2. Optimized Power-of-2 Operations

**Original Approach:**
- Used loop-based calculations for determining block sizes and offsets
- Limited block size flexibility

**SAL Improvements:**
- **Efficient log2 Calculation**: Uses `std::countl_zero` for O(1) computation
- **Bitwise Operations**: Fast bit shifts for multiplication/division operations
- **Power-of-2 Validation**: Explicit validation with O(1) check: `x & (x-1) == 0`

### 3. Flexible Configuration

**Original Approach:**
- Fixed relationship between parameters

**SAL Improvements:**
- **Non-power-of-2 Max Blocks**: Allows arbitrary maximum block counts
- **Power-of-2 Block Sizes**: Enforces power-of-2 block sizes for efficiency
- **Dynamic Address Space Allocation**: Determines maximum reservation size based on system capabilities

### 4. Memory Safety Enhancements

**Original Approach:**
- Potential memory leaks during error conditions

**SAL Improvements:**
- **Clean Resource Management**: Properly handles cleanup in all error paths
- **Exception Safety**: Provides strong exception guarantee for allocation operations
- **Atomic Operations**: Thread-safe block count tracking

### 5. Performance Optimization

**Original Approach:**
- Less efficient memory usage patterns

**SAL Improvements:**
- **Optimized Block Addressing**: Fast bit-shift operations for address calculations
- **Block Alignment**: Efficient modulo checks with bitwise operations
- **Reduced System Calls**: Minimizes number of mmap/munmap operations

## Usage Examples

```cpp
// Create a block allocator with 16MB blocks (must be power of 2)
sal::block_allocator allocator("data.bin", 16*1024*1024, 10);

// Allocate a block and get a pointer to it
auto offset = allocator.alloc();
void* block_ptr = allocator.get(offset);

// Write data to the block
memcpy(block_ptr, data, data_size);

// Synchronize to disk
allocator.sync(sal::sync_type::async);

// Convert between block numbers and offsets
uint32_t block_num = allocator.offset_to_block(offset);
sal::block_allocator::offset_ptr same_offset = allocator.block_to_offset(block_num);
```

## Design Principles

The SAL library follows these core design principles:

1. **Performance-first**: Optimized for high-performance systems programming
2. **Memory safety**: Robust memory management without sacrificing performance
3. **Simplicity**: Clean, understandable APIs with comprehensive documentation
4. **Compatibility**: Works across different platforms and compilers

The library also provides extensive debugging support and error reporting to make development easier.

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