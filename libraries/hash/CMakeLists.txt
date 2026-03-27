cmake_minimum_required(VERSION 3.16)

add_library(hash INTERFACE)

target_include_directories(hash INTERFACE 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include> # Assuming headers might be installed someday
)

# Installation is handled by the top-level CMakeLists.txt psitri export set 