cmake_minimum_required(VERSION 3.16)

# ── psitrimdbx backend ──────────────────────────────────────────────
add_executable(eth-state-bench src/eth_state_bench.cpp)
target_link_libraries(eth-state-bench PRIVATE psitrimdbx psitri sal)
target_include_directories(eth-state-bench PRIVATE
   ${CMAKE_SOURCE_DIR}/libraries/psitrimdbx/include)
target_compile_definitions(eth-state-bench PRIVATE BACKEND_NAME="psitrimdbx")

# ── Native libmdbx backend (optional) ──────────────────────────────
# Set LIBMDBX_SOURCE_DIR to the path of a libmdbx source checkout to
# build a comparison binary.  Example:
#   cmake -DLIBMDBX_SOURCE_DIR=/path/to/libmdbx ...
if(LIBMDBX_SOURCE_DIR AND EXISTS "${LIBMDBX_SOURCE_DIR}/CMakeLists.txt")
   set(MDBX_BUILD_CXX ON CACHE BOOL "" FORCE)
   set(MDBX_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
   set(MDBX_BUILD_SHARED_LIBRARY OFF CACHE BOOL "" FORCE)
   set(MDBX_ENABLE_TESTS OFF CACHE BOOL "" FORCE)
   set(MDBX_INSTALL OFF CACHE BOOL "" FORCE)
   add_subdirectory(${LIBMDBX_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}/libmdbx EXCLUDE_FROM_ALL)

   add_executable(eth-state-bench-mdbx src/eth_state_bench.cpp)
   target_link_libraries(eth-state-bench-mdbx PRIVATE mdbx-static)
   target_compile_definitions(eth-state-bench-mdbx PRIVATE BACKEND_NAME="native-mdbx")
endif()
