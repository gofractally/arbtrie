# Min index benchmark helper components
# This CMakeLists.txt is for the header implementations, not for building 
# the benchmarks directly (those are built in the parent directory)

# Create a header-only library for the min_index implementations
add_library(min_index_components INTERFACE)
target_include_directories(min_index_components INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)

# No need to duplicate the build of binaries that are now in the parent directory 