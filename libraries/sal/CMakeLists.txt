cmake_minimum_required(VERSION 3.16)
project(sal VERSION 0.1.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Option to build tests
option(SAL_BUILD_TESTS "Build the SAL test suite" OFF)
# Debug definitions
option(SAL_DEBUG "Enable debug logging" OFF)

# Create the library target
add_library(sal
    src/block_allocator.cpp
    src/allocator.cpp
    src/mapping.cpp
    src/control_block_alloc.cpp
    src/allocator_session.cpp
    src/segment_thread.cpp
)
target_compile_definitions(sal PUBLIC XXH_INLINE_ALL)

# Set include directories
target_include_directories(sal
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Set compile features and options
target_compile_features(sal PUBLIC cxx_std_23)
target_compile_options(sal 
    PRIVATE 
        -Wall 
        -Wextra
        -Werror
        -Wno-vla-cxx-extension
)

# Debug definitions
if(SAL_DEBUG)
    target_compile_definitions(sal PRIVATE SAL_DEBUG=1)
endif()

# Create list of header files
set(SAL_HEADERS
    include/sal/block_allocator.hpp
    include/sal/mapping.hpp
    include/sal/debug.hpp
)

# Tests
if(SAL_BUILD_TESTS)
    enable_testing()
    find_package(Catch2 3 REQUIRED)
    
    # Create test target
    add_executable(sal-tests
        test/block_allocator_tests.cpp
        test/mapping_tests.cpp
        test/control_block_alloc_tests.cpp
        test/min_index_tests.cpp
        test/allocator_session_tests.cpp
        test/read_cache_tests.cpp
    )
    
    
    # Link against the library and Catch2
    target_link_libraries(sal-tests
        PRIVATE
            sal
            Catch2::Catch2WithMain
    )
    
    
    # Set the output directory for the test executable
    set_target_properties(sal-tests
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
    )
    
    
    # Register tests with CTest
    include(CTest)
    include(Catch)
    catch_discover_tests(sal-tests)
    
    # Add reserve limit test executable
    add_executable(reserve-limit-test
        test/reserve_limit_test.cpp
    )
    
    # Set the output directory for the limit test executable
    set_target_properties(reserve-limit-test
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
    )
endif()

# Check if the parent project enabled sanitizers
if(ENABLE_SANITIZER)
    message(STATUS "SAL: Enabling Thread Sanitizer")
    target_compile_options(sal PUBLIC -fsanitize=thread -g)
    target_link_options(sal PUBLIC -fsanitize=thread -g)

    # Also apply to tests if they are built
    if(TARGET sal-tests)
        target_compile_options(sal-tests PUBLIC -fsanitize=thread -g)
        target_link_options(sal-tests PUBLIC -fsanitize=thread -g)
    endif()
    if(TARGET reserve-limit-test)
        target_compile_options(reserve-limit-test PUBLIC -fsanitize=thread -g)
        target_link_options(reserve-limit-test PUBLIC -fsanitize=thread -g)
    endif()
endif()

# Check if the parent project enabled coverage
if(ENABLE_COVERAGE)
    message(STATUS "SAL: Enabling Coverage Reporting")
    # Use -O1 and NDEBUG for faster coverage runs — asserts and -O0 are unnecessary overhead
    target_compile_options(sal PUBLIC -fprofile-arcs -ftest-coverage -g -O1 -DNDEBUG)
    target_link_options(sal PUBLIC -fprofile-arcs -ftest-coverage)

    # Also apply to tests if they are built
    if(TARGET sal-tests)
        target_compile_options(sal-tests PUBLIC -fprofile-arcs -ftest-coverage -g -O1 -DNDEBUG)
        target_link_options(sal-tests PUBLIC -fprofile-arcs -ftest-coverage)
    endif()
    if(TARGET reserve-limit-test)
        target_compile_options(reserve-limit-test PUBLIC -fprofile-arcs -ftest-coverage -g -O1 -DNDEBUG)
        target_link_options(reserve-limit-test PUBLIC -fprofile-arcs -ftest-coverage)
    endif()
endif()

# Link dependencies (hash and ucc)
target_link_libraries(sal PUBLIC hash ucc)

# Installation is handled by the top-level CMakeLists.txt psitri export set 
