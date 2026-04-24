cmake_minimum_required(VERSION 3.16)

add_executable(eth-state-bench src/eth_state_bench.cpp)
target_link_libraries(eth-state-bench PRIVATE psitrimdbx psitri sal)
target_include_directories(eth-state-bench PRIVATE
   ${CMAKE_SOURCE_DIR}/libraries/psitrimdbx/include)
