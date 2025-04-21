cmake_minimum_required(VERSION 3.16)

add_library(ucc INTERFACE)

target_include_directories(ucc INTERFACE 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include> # Assuming headers might be installed someday
)

# Installation and Export
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Install the interface target definition
install(TARGETS ucc EXPORT ucc-targets
    # No actual library/archive/runtime for INTERFACE libraries
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# Install the headers
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# Export the target
install(EXPORT ucc-targets
    FILE ucc-targets.cmake
    NAMESPACE ucc::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ucc
)

# Generate and install package configuration files (optional but good practice)
# write_basic_package_version_file(
#     ${CMAKE_CURRENT_BINARY_DIR}/ucc-config-version.cmake
#     VERSION 1.0.0 # Specify a version
#     COMPATIBILITY SameMajorVersion
# )
# 
# configure_package_config_file(
#     ${CMAKE_CURRENT_SOURCE_DIR}/cmake/ucc-config.cmake.in # You'd need to create this template
#     ${CMAKE_CURRENT_BINARY_DIR}/ucc-config.cmake
#     INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ucc
# )
# 
# install(FILES
#     ${CMAKE_CURRENT_BINARY_DIR}/ucc-config.cmake
#     ${CMAKE_CURRENT_BINARY_DIR}/ucc-config-version.cmake
#     DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ucc
# )

# Add tests subdirectory if BUILD_TESTING is enabled
if(BUILD_TESTING)
    add_subdirectory(tests)
endif() 