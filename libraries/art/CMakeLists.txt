cmake_minimum_required(VERSION 3.16)
add_library(art INTERFACE)
target_include_directories(art INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_link_libraries(art INTERFACE ucc)
