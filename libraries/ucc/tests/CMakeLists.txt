cmake_minimum_required(VERSION 3.16)
project(ucc-tests LANGUAGES CXX)

find_package(Catch2 3 REQUIRED)

add_executable(ucc-perf-test 
    find_u32x16_perf_test.cpp
    lower_bound_tests.cpp
)

# Ensure the test executable can find the ucc headers
target_include_directories(ucc-perf-test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include # Include directory within ucc
        ${PROJECT_SOURCE_DIR}/../.. # Include directory for other project libs if needed (e.g., from root)
)

# Link against Catch2
target_link_libraries(ucc-perf-test PRIVATE Catch2::Catch2WithMain)

# Enable CTest integration
include(CTest)
include(Catch)
catch_discover_tests(ucc-perf-test)

# Define output directory for the executable relative to build type
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)
set_target_properties(ucc-perf-test PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${EXECUTABLE_OUTPUT_PATH}
    RUNTIME_OUTPUT_DIRECTORY_DEBUG ${EXECUTABLE_OUTPUT_PATH}
    RUNTIME_OUTPUT_DIRECTORY_RELEASE ${EXECUTABLE_OUTPUT_PATH}
) 