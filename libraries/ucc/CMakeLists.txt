cmake_minimum_required(VERSION 3.16)

add_library(ucc INTERFACE)

target_include_directories(ucc INTERFACE 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include> # Assuming headers might be installed someday
)

# Installation is handled by the top-level CMakeLists.txt psitri export set

# Add tests subdirectory if BUILD_TESTING is enabled
if(BUILD_TESTS)
    add_subdirectory(tests)
endif() 