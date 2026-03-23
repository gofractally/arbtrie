cmake_minimum_required(VERSION 3.16)

add_library(hash INTERFACE)

target_include_directories(hash INTERFACE 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include> # Assuming headers might be installed someday
)

# Installation and Export
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Install the interface target definition
install(TARGETS hash EXPORT hash-targets
    # No actual library/archive/runtime for INTERFACE libraries
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# Install the headers
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# Export the target
install(EXPORT hash-targets
    FILE hash-targets.cmake
    NAMESPACE hash::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hash
)

# Generate and install package configuration files (optional but good practice)
# write_basic_package_version_file(
#     ${CMAKE_CURRENT_BINARY_DIR}/hash-config-version.cmake
#     VERSION 1.0.0 # Specify a version
#     COMPATIBILITY SameMajorVersion
# )
# 
# configure_package_config_file(
#     ${CMAKE_CURRENT_SOURCE_DIR}/cmake/hash-config.cmake.in # You'd need to create this template
#     ${CMAKE_CURRENT_BINARY_DIR}/hash-config.cmake
#     INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hash
# )
# 
# install(FILES
#     ${CMAKE_CURRENT_BINARY_DIR}/hash-config.cmake
#     ${CMAKE_CURRENT_BINARY_DIR}/hash-config-version.cmake
#     DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hash
# ) 